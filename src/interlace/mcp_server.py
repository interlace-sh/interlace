"""A stdio MCP server over a project directory.

Speaks the Model Context Protocol (JSON-RPC 2.0, ``Content-Length`` framing) so an
agent can list models, preview rows, plan, and apply without inventing the CLI.
``apply`` refuses to run unless ``confirm`` is true — call ``plan`` first.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import traceback
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any, cast

from interlace import __version__
from interlace.exceptions import InterlaceError
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import CompiledProject
from interlace.inspect import failing_rows, preview_model
from interlace.plan.orchestrate import compute_plan, plan_and_apply, resolve_selection
from interlace.project import Project
from interlace.query import prepare_readonly
from interlace.streaming import ensure_stream_tables

_PROTOCOL = "2024-11-05"
_SUPPORTED = frozenset({_PROTOCOL, "2025-03-26", "2025-06-18"})

Tool = Callable[[Path, dict[str, Any]], Awaitable[Any]]


def _env(project: Project, args: dict[str, Any]) -> str:
    given = args.get("environment")
    if isinstance(given, str) and given:
        return given
    return os.environ.get("INTERLACE_ENV", "prod")


def _schema(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"type": "object", "properties": properties, "additionalProperties": False}
    if required:
        body["required"] = required
    return body


def _text(value: Any, *, error: bool = False) -> dict[str, Any]:
    if not isinstance(value, str):
        value = json.dumps(value, indent=2, default=str)
    result: dict[str, Any] = {"content": [{"type": "text", "text": value}]}
    if error:
        result["isError"] = True
    return result


async def _list_models(path: Path, _args: dict[str, Any]) -> Any:
    compiled = Project.load(path).compile()
    return [
        {
            "name": name,
            "materialise": compiled.models[name].materialise,
            "strategy": compiled.models[name].strategy,
            "engine": compiled.models[name].engine,
            "depends_on": list(compiled.models[name].dependencies),
        }
        for name in compiled.graph.topological_sort()
    ]


async def _get_model(path: Path, args: dict[str, Any]) -> Any:
    compiled = Project.load(path).compile()
    name = str(args["name"])
    if name not in compiled.models:
        raise InterlaceError(f"unknown model: {name}")
    model = compiled.models[name]
    return {
        "name": name,
        "materialise": model.materialise,
        "strategy": model.strategy,
        "engine": model.engine,
        "fingerprint": model.fingerprint,
        "depends_on": list(model.dependencies),
        "checks": [spec.name for spec in model.checks],
        "sql": model.definition_sql,
    }


async def _open(path: Path) -> tuple[Project, CompiledProject, Any, Any]:
    project = Project.load(path)
    compiled = project.compile()
    state = await project.open_state()
    try:
        engines = project.open_engines()
    except Exception:
        await state.close()
        raise
    return project, compiled, state, engines


async def _preview(path: Path, args: dict[str, Any]) -> Any:
    project, compiled, state, engines = await _open(path)
    try:
        name = str(args["name"])
        if name not in compiled.models:
            raise InterlaceError(f"unknown model: {name}")
        model = compiled.models[name]
        preview = await preview_model(
            compiled,
            state,
            engines.require(model.engine, model=name),
            name,
            _env(project, args),
            int(args.get("limit") or 25),
        )
    finally:
        await state.close()
        engines.close()
    return {
        "available": preview.available,
        "message": preview.message,
        "relation": preview.relation,
        "columns": preview.sample.columns,
        "rows": preview.sample.rows,
        "truncated": preview.sample.truncated,
        "profile": [
            {
                "column": column.column,
                "nulls": column.nulls,
                "distinct": column.distinct,
                "min": column.min,
                "max": column.max,
            }
            for column in preview.profile
        ],
        "last_build": (
            None
            if preview.last_build is None
            else {
                "status": preview.last_build.status,
                "message": preview.last_build.message,
                "statement": preview.last_build.statement,
                "seconds": preview.last_build.seconds,
                "rows": preview.last_build.rows,
            }
        ),
    }


async def _plan(path: Path, args: dict[str, Any]) -> Any:
    project, compiled, state, engines = await _open(path)
    try:
        environment = _env(project, args)
        selectors = [str(item) for item in args.get("selectors") or []]
        selected = await resolve_selection(compiled, state, environment, selectors)
        plan = await compute_plan(
            compiled, environment, state, engines, select=selected, forward_only=bool(args.get("forward_only"))
        )
    finally:
        await state.close()
        engines.close()
    return {
        "environment": environment,
        "empty": plan.is_empty,
        "breaking": plan.has_breaking_changes,
        "blocking": list(plan.blocking),
        "changes": [
            {
                "name": change.name,
                "change_type": change.change_type.value,
                "category": change.category.value if change.category else None,
            }
            for change in plan.changes
        ],
    }


async def _apply(path: Path, args: dict[str, Any]) -> Any:
    if args.get("confirm") is not True:
        raise InterlaceError("apply was not run — call plan, then call apply with confirm set to true")
    project, compiled, state, engines = await _open(path)
    try:
        environment = _env(project, args)

        async def prepare() -> None:
            if project.streams:
                await ensure_stream_tables(project.streams, engines.get())

        plan, result = await plan_and_apply(
            compiled,
            environment=environment,
            project=project,
            engines=engines,
            state=state,
            lock_owner=f"mcp:{os.getpid()}",
            selectors=[str(item) for item in args.get("selectors") or []],
            forward_only=bool(args.get("forward_only")),
            force=bool(args.get("force")),
            prepare=prepare,
        )
    finally:
        await state.close()
        engines.close()
    if result is None:
        return {"environment": environment, "built": [], "promoted": 0}
    return {
        "environment": environment,
        "built": result.built,
        "reused": result.reused,
        "promoted": result.promoted,
    }


async def _query(path: Path, args: dict[str, Any]) -> Any:
    project = Project.load(path)
    engines = project.open_engines()
    try:
        engine = engines.get()
        bounded, cap = prepare_readonly(str(args["sql"]), engine.dialect, int(args.get("limit") or 100))
        reader = await engine.fetch(bounded)
        columns = list(reader.schema.names)
        records = await asyncio.to_thread(lambda: reader.read_all().to_pylist())
    finally:
        engines.close()
    shown = records[:cap]
    return {
        "columns": columns,
        "rows": [[record[name] for name in columns] for record in shown],
        "truncated": len(records) > cap,
    }


async def _lineage(path: Path, _args: dict[str, Any]) -> Any:
    compiled = Project.load(path).compile()
    columns = column_lineage(compiled)
    return {
        "models": list(compiled.graph.topological_sort()),
        "edges": [[upstream, name] for name, model in compiled.models.items() for upstream in model.dependencies],
        "columns": {
            model: {column: [[ref[0], ref[1]] for ref in refs] for column, refs in mapping.items()}
            for model, mapping in columns.items()
        },
    }


async def _checks(path: Path, args: dict[str, Any]) -> Any:
    project = Project.load(path)
    state = await project.open_state()
    try:
        model = args.get("model")
        rows = await state.list_check_results(str(model) if model else None)
    finally:
        await state.close()
    return rows


async def _failing(path: Path, args: dict[str, Any]) -> Any:
    project, compiled, state, engines = await _open(path)
    try:
        name = str(args["model"])
        if name not in compiled.models:
            raise InterlaceError(f"unknown model: {name}")
        model = compiled.models[name]
        sample = await failing_rows(
            compiled,
            state,
            engines.require(model.engine, model=name),
            name,
            str(args["check"]),
            _env(project, args),
            int(args.get("limit") or 25),
        )
    finally:
        await state.close()
        engines.close()
    return {
        "available": sample.available,
        "message": sample.message,
        "columns": sample.sample.columns,
        "rows": sample.sample.rows,
        "truncated": sample.sample.truncated,
    }


async def _runs(path: Path, args: dict[str, Any]) -> Any:
    project = Project.load(path)
    state = await project.open_state()
    try:
        runs = await state.list_runs()
    finally:
        await state.close()
    limit = int(args.get("limit") or 20)
    return [
        {"id": run["id"], "state": run["state"], "models": run["flow_selector"], "error": run["error"]}
        for run in runs[:limit]
    ]


_TOOLS: list[tuple[str, str, dict[str, Any], Tool]] = [
    (
        "list_models",
        "List the project's models in dependency order.",
        _schema({}),
        _list_models,
    ),
    (
        "get_model",
        "Read one model's materialisation, fingerprint, checks, and SQL.",
        _schema({"name": {"type": "string"}}, ["name"]),
        _get_model,
    ),
    (
        "preview_model",
        "Sample rows and a column profile (nulls, distinct, min, max) from a built model, plus the last build's status and failed statement.",
        _schema(
            {
                "name": {"type": "string"},
                "environment": {"type": "string"},
                "limit": {"type": "integer"},
            },
            ["name"],
        ),
        _preview,
    ),
    (
        "plan",
        "Diff the project against an environment. Does not build. Call this before apply.",
        _schema(
            {
                "environment": {"type": "string"},
                "selectors": {"type": "array", "items": {"type": "string"}},
                "forward_only": {"type": "boolean"},
            }
        ),
        _plan,
    ),
    (
        "apply",
        "Build and promote. Refuses unless confirm is true. Pass force true only after plan reported breaking changes.",
        _schema(
            {
                "confirm": {"type": "boolean"},
                "force": {"type": "boolean"},
                "environment": {"type": "string"},
                "selectors": {"type": "array", "items": {"type": "string"}},
                "forward_only": {"type": "boolean"},
            },
            ["confirm"],
        ),
        _apply,
    ),
    (
        "query",
        "Run one read-only SELECT against the warehouse. The same fence as interlace query.",
        _schema({"sql": {"type": "string"}, "limit": {"type": "integer"}}, ["sql"]),
        _query,
    ),
    (
        "lineage",
        "The model DAG and column-level lineage.",
        _schema({}),
        _lineage,
    ),
    (
        "list_checks",
        "Recent check results, newest first. Optionally filter by model.",
        _schema({"model": {"type": "string"}}),
        _checks,
    ),
    (
        "failing_rows",
        "The rows a check rejected. Table-level and Python checks have no row set. Does not change the promotion gate.",
        _schema(
            {
                "model": {"type": "string"},
                "check": {"type": "string"},
                "environment": {"type": "string"},
                "limit": {"type": "integer"},
            },
            ["model", "check"],
        ),
        _failing,
    ),
    (
        "list_runs",
        "Recent queued and finished runs.",
        _schema({"limit": {"type": "integer"}}),
        _runs,
    ),
]


def _tool_map() -> dict[str, tuple[str, dict[str, Any], Tool]]:
    return {name: (description, schema, fn) for name, description, schema, fn in _TOOLS}


def handle(path: Path, message: dict[str, Any]) -> dict[str, Any] | None:
    """Answer one JSON-RPC message. Notifications return None (no response)."""
    method = str(message.get("method") or "")
    msg_id = message.get("id")
    raw_params = message.get("params")
    params: dict[str, Any] = raw_params if isinstance(raw_params, dict) else {}
    if method.startswith("notifications/"):
        return None
    if msg_id is None:
        return None

    def result(value: Any) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": msg_id, "result": value}

    def error(code: int, text: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": text}}

    if method == "initialize":
        version = params.get("protocolVersion")
        if version not in _SUPPORTED:
            version = _PROTOCOL
        return result(
            {
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "interlace", "version": __version__},
            }
        )
    if method == "ping":
        return result({})
    if method == "tools/list":
        return result(
            {
                "tools": [
                    {"name": name, "description": description, "inputSchema": schema}
                    for name, description, schema, _fn in _TOOLS
                ]
            }
        )
    if method == "tools/call":
        from interlace.state.store import event_actor

        token = event_actor.set("mcp")
        try:
            name = str(params.get("name") or "")
            raw_args = params.get("arguments")
            arguments: dict[str, Any] = raw_args if isinstance(raw_args, dict) else {}
            tool = _tool_map().get(name)
            if tool is None:
                return result(_text(f"unknown tool {name!r}", error=True))
            if name == "apply" and arguments.get("confirm") is not True:
                return result(
                    _text("apply was not run — call plan, then call apply with confirm set to true", error=True)
                )
            try:
                value: Any = asyncio.run(cast(Coroutine[Any, Any, Any], tool[2](path, arguments)))
            except InterlaceError as exc:
                return result(_text(exc.message, error=True))
            except Exception as exc:
                traceback.print_exc(file=sys.stderr)
                return result(_text(str(exc) or type(exc).__name__, error=True))
            return result(_text(value))
        finally:
            event_actor.reset(token)
    return error(-32601, f"method not found: {method}")


def _read_message(stdin: Any) -> dict[str, Any] | None:
    headers: dict[str, str] = {}
    while True:
        line = stdin.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        decoded = line.decode("utf-8", "replace")
        key, _, value = decoded.partition(":")
        headers[key.strip().lower()] = value.strip()
    length = int(headers.get("content-length") or "0")
    if length <= 0:
        return None
    body = stdin.read(length)
    if len(body) < length:
        return None
    parsed = json.loads(body)
    return parsed if isinstance(parsed, dict) else None


def _write_message(stdout: Any, payload: dict[str, Any]) -> None:
    data = json.dumps(payload).encode()
    stdout.write(f"Content-Length: {len(data)}\r\n\r\n".encode() + data)
    stdout.flush()


def serve_stdio(path: Path) -> None:
    """Read MCP messages from stdin and write responses to stdout until EOF."""
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    while True:
        message = _read_message(stdin)
        if message is None:
            return
        response = handle(path, message)
        if response is not None:
            _write_message(stdout, response)
