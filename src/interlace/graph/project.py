"""Compile declared models into a fingerprinted dependency graph.

Takes the registry's :class:`ModelDef`s and produces a :class:`CompiledProject`:
each model parsed (SQL) or source-hashed (Python), its dependencies resolved
(explicit ``depends_on`` plus implicit table references that match a known
model), topologically ordered, and fingerprinted upstream-first. The plan step
consumes this — diffing fingerprints against the state store to classify changes
and build the snapshots it persists.
"""

from __future__ import annotations

import inspect
import textwrap
from collections.abc import Iterable
from dataclasses import dataclass

from sqlglot import exp

from interlace.dsl.decorators import ModelDef
from interlace.exceptions import DefinitionError
from interlace.exports import ExportConfig
from interlace.graph.dag import DependencyGraph
from interlace.ir.canonicalize import parse, table_references
from interlace.ir.fingerprint import canonical_sql, data_fingerprint, metadata_fingerprint
from interlace.ir.relation import TableRef

_PHYSICAL_PREFIX = "interlace__"


@dataclass(frozen=True)
class CompiledModel:
    """A model resolved to a fingerprint, physical table, and dependency set."""

    name: str
    dialect: str
    dependencies: tuple[str, ...]
    fingerprint: str  # full: SQL + config + upstream fingerprints
    local_fingerprint: str  # SQL + config only — separates direct from indirect changes
    metadata_hash: str
    definition_sql: str | None  # canonical SQL, for change classification (None for Python models)
    physical_table: TableRef
    materialise: str
    strategy: str
    key: tuple[str, ...]  # business key for keyed strategies (merge_by_key)
    time_column: str | None  # partition column for incremental_by_time
    interval: str | None  # grain for incremental_by_time (e.g. "1d")
    tags: tuple[str, ...]  # for tag: selection
    schedule: dict[str, str] | None  # cron/interval schedule for the trigger engine
    columns: dict[str, str | None] | None  # output contract validated at apply time
    export: ExportConfig | None  # presence makes this a sink (no physical table/view)
    ast: exp.Expression | None  # parsed SQL, or None for Python models
    owner: str | None = None  # surfaced in the catalog/API (metadata, not fingerprinted into data)
    description: str | None = None


@dataclass
class CompiledProject:
    """All compiled models plus the dependency graph that orders them."""

    models: dict[str, CompiledModel]
    graph: DependencyGraph

    def ordered(self) -> list[CompiledModel]:
        return [self.models[name] for name in self.graph.topological_sort()]


def _split_name(name: str) -> tuple[str, str]:
    schema, _, base = name.rpartition(".")
    return (schema or "main"), base


def _physical_table(name: str, fingerprint: str, catalog: str | None) -> TableRef:
    schema, base = _split_name(name)
    return TableRef(schema=f"{_PHYSICAL_PREFIX}{schema}", name=f"{base}__{fingerprint}", catalog=catalog)


def _resolve_dependencies(
    model: ModelDef, names: set[str], default_dialect: str
) -> tuple[tuple[str, ...], exp.Expression | None, str]:
    dialect = model.dialect or default_dialect
    deps: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        if candidate != model.name and candidate in names and candidate not in seen:
            seen.add(candidate)
            deps.append(candidate)

    for explicit in model.depends_on:
        add(explicit)

    ast: exp.Expression | None = None
    if model.sql is not None:
        ast = parse(model.sql, dialect)
        for ref in table_references(ast):
            if ref in names:
                add(ref)
            else:
                add(ref.rsplit(".", 1)[-1])  # match a qualified ref to a model by its tail

    return tuple(deps), ast, dialect


def _fingerprint_query(model: ModelDef, ast: exp.Expression | None) -> str | exp.Expression:
    if ast is not None:
        return ast
    if model.fn is not None:
        return textwrap.dedent(inspect.getsource(model.fn))
    raise DefinitionError(f"model {model.name!r} has neither SQL nor a function body")


def compile_models(
    models: Iterable[ModelDef], *, default_dialect: str = "duckdb", catalog: str | None = None
) -> CompiledProject:
    """Compile models into a fingerprinted, topologically-ordered project."""
    definitions = {m.name: m for m in models}
    names = set(definitions)

    resolved: dict[str, tuple[tuple[str, ...], exp.Expression | None, str]] = {}
    graph = DependencyGraph()
    for name, definition in definitions.items():
        deps, ast, dialect = _resolve_dependencies(definition, names, default_dialect)
        resolved[name] = (deps, ast, dialect)
        graph.add_node(name)
        for dep in deps:
            graph.add_dependency(name, dep)

    compiled: dict[str, CompiledModel] = {}
    for name in graph.topological_sort():  # upstreams first; raises on cycle
        definition = definitions[name]
        deps, ast, dialect = resolved[name]
        strategy_config = {
            "materialise": definition.materialise,
            "strategy": definition.strategy,
            "key": list(definition.key),
            "kind": definition.kind,
            "interval": definition.interval,
            "time_column": definition.time_column,
            "export": {"to": definition.export.to, "path": definition.export.path} if definition.export else None,
            "dialect": dialect,
        }
        query = _fingerprint_query(definition, ast)
        local_fingerprint = data_fingerprint(query=query, strategy_config=strategy_config, upstream_fingerprints=[])
        fingerprint = data_fingerprint(
            query=query,
            strategy_config=strategy_config,
            upstream_fingerprints=[compiled[dep].fingerprint for dep in deps],
        )
        metadata_hash = metadata_fingerprint(
            {"owner": definition.owner, "tags": list(definition.tags), "description": definition.description}
        )
        compiled[name] = CompiledModel(
            name=name,
            dialect=dialect,
            dependencies=deps,
            fingerprint=fingerprint,
            local_fingerprint=local_fingerprint,
            metadata_hash=metadata_hash,
            definition_sql=canonical_sql(ast) if ast is not None else None,
            physical_table=_physical_table(name, fingerprint, catalog),
            materialise=definition.materialise,
            strategy=definition.strategy,
            key=definition.key,
            time_column=definition.time_column,
            interval=definition.interval,
            tags=definition.tags,
            schedule=definition.schedule,
            columns=definition.columns,
            export=definition.export,
            ast=ast,
            owner=definition.owner,
            description=definition.description,
        )

    return CompiledProject(models=compiled, graph=graph)
