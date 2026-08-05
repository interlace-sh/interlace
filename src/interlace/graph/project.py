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
from dataclasses import dataclass, field

from sqlglot import exp

from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import CheckDef, ModelDef, ModelFn
from interlace.exceptions import CompilationError, DefinitionError
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
    engine: str  # named engine that executes and stores this model
    dependencies: tuple[str, ...]
    fingerprint: str  # full: SQL + config + upstream fingerprints
    local_fingerprint: str  # SQL + config only — separates direct from indirect changes
    metadata_hash: str
    definition_sql: str | None  # canonical SQL, for change classification (None for Python models)
    physical_table: TableRef
    materialise: str
    strategy: str
    key: tuple[str, ...]  # business key for keyed strategies (merge)
    time_column: str | None  # partition column for incremental_by_time
    cursor: str | None  # column whose max is injected into a Python model's `cursor` param
    interval: str | None  # grain for incremental_by_time (e.g. "1d")
    tags: tuple[str, ...]  # for tag: selection
    schedule: dict[str, str] | None  # cron/interval schedule for the trigger engine
    columns: dict[str, str | None] | None  # output contract validated at apply time
    # Terminal materialisation (materialise: table/file) — a destination interlace
    # does not own; empty/None for the interlace-owned virtual/view/ephemeral planes.
    target: str | None  # <alias>.<schema>.<table> for materialise: table
    path: str | None  # output path for materialise: file
    format: str | None  # csv | parquet | json for materialise: file
    environments: tuple[str, ...]  # which environments actually deliver a terminal model
    ast: exp.Expression | None  # parsed SQL, or None for Python models
    owner: str | None = None  # surfaced in the catalog/API (metadata, not fingerprinted into data)
    description: str | None = None
    fn: ModelFn | None = None  # the Python model function (source is fingerprinted; None for SQL)
    checks: tuple[CheckSpec, ...] = ()  # metadata-fingerprinted: changing a check never rebuilds data
    backfill: str = "auto"  # incremental first-build window policy: auto | none | <ISO start>

    @property
    def is_terminal(self) -> bool:
        """A terminal model delivers into an external destination (table/file): no
        managed snapshot table, no environment view, environment-gated side effects."""
        return self.materialise in ("table", "file")


@dataclass
class CompiledProject:
    """All compiled models plus the dependency graph that orders them."""

    models: dict[str, CompiledModel]
    graph: DependencyGraph
    python_checks: dict[str, tuple[CheckDef, ...]] = field(default_factory=dict)  # @check fns by model

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
        try:
            ast = parse(model.sql, dialect)
        except CompilationError as exc:  # name the offending model so the error is actionable
            raise CompilationError(
                f"model {model.name!r}: {exc.message}", details={**exc.details, "model": model.name}
            ) from exc
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
    models: Iterable[ModelDef],
    *,
    default_dialect: str = "duckdb",
    default_engine: str = "default",
    engine_dialects: dict[str, str] | None = None,
    known_engines: set[str] | None = None,
    catalog: str | None = None,
    checks: Iterable[CheckDef] = (),
) -> CompiledProject:
    """Compile models into a fingerprinted, topologically-ordered project.

    ``checks`` are ``@check``-decorated Python functions, attached by model name.
    ``engine_dialects`` maps engine name → sqlglot dialect (used when a model
    omits ``dialect``). ``known_engines`` validates model ``engine`` pins.
    """
    definitions = {m.name: m for m in models}
    names = set(definitions)
    dialects_by_engine = engine_dialects or {}
    engines = known_engines

    resolved: dict[str, tuple[tuple[str, ...], exp.Expression | None, str, str]] = {}
    graph = DependencyGraph()
    for name, definition in definitions.items():
        engine = definition.engine or default_engine
        if engines is not None and engine not in engines:
            raise DefinitionError(
                f"model {name!r} references unknown engine {engine!r}",
                details={"engines": sorted(engines)},
            )
        if definition.materialise == "file" and definition.checks:
            raise DefinitionError(
                f"{name!r} materialises as a file, which has no queryable table to check — declare checks on "
                f"the model it selects from (a materialise: table can carry checks; they run against the "
                f"delivered external table)"
            )
        # Authoring dialect: explicit model dialect, else the engine's, else project default.
        model_default_dialect = dialects_by_engine.get(engine, default_dialect)
        deps, ast, dialect = _resolve_dependencies(definition, names, model_default_dialect)
        resolved[name] = (deps, ast, dialect, engine)
        graph.add_node(name)
        for dep in deps:
            graph.add_dependency(name, dep)

    compiled: dict[str, CompiledModel] = {}
    for name in graph.topological_sort():  # upstreams first; raises on cycle
        definition = definitions[name]
        deps, ast, dialect, engine = resolved[name]
        for dep in deps:  # topo order: deps already compiled
            if compiled[dep].materialise == "file":
                raise DefinitionError(
                    f"model {name!r} depends on {dep!r}, which materialises as a file — a file isn't a readable "
                    f"table; depend on the model it selects from instead"
                )
            if compiled[dep].materialise == "table" and compiled[dep].engine != engine:
                # a table model is read directly by its external target; the target's attach
                # alias lives on its own engine, so a cross-engine read can't reach it
                raise DefinitionError(
                    f"model {name!r} on engine {engine!r} depends on table {dep!r} on engine "
                    f"{compiled[dep].engine!r} — a reverse-ETL table is read by its external target, which isn't "
                    f"reachable cross-engine; put them on the same engine"
                )
            if compiled[dep].engine != engine and compiled[dep].materialise == "ephemeral":
                raise DefinitionError(
                    f"model {name!r} on engine {engine!r} inlines ephemeral {dep!r} declared on engine "
                    f"{compiled[dep].engine!r}; an ephemeral model must share its consumers' engine "
                    f"(see docs/architecture/MULTI_ENGINE.md)"
                )
        strategy_config = {
            "materialise": definition.materialise,
            "strategy": definition.strategy,
            "key": list(definition.key),
            "interval": definition.interval,
            "time_column": definition.time_column,
            "cursor": definition.cursor,
            "engine": engine,
            # Terminal params are behavioural and must fold into the fingerprint: a
            # retargeted delivery, or a widened `environments` gate, must re-plan or a
            # newly-allowed environment would classify UNCHANGED and never deliver.
            "target": definition.target,
            "path": definition.path,
            "format": definition.format,
            "environments": sorted(definition.environments),
            "dialect": dialect,
        }
        query = _fingerprint_query(definition, ast)
        # render the canonical SQL once: it feeds both fingerprints and definition_sql
        canonical = canonical_sql(query) if isinstance(query, exp.Expression) else query
        local_fingerprint = data_fingerprint(query=canonical, strategy_config=strategy_config, upstream_fingerprints=[])
        fingerprint = data_fingerprint(
            query=canonical,
            strategy_config=strategy_config,
            upstream_fingerprints=[compiled[dep].fingerprint for dep in deps],
        )
        metadata_hash = metadata_fingerprint(
            {
                "owner": definition.owner,
                "tags": list(definition.tags),
                "description": definition.description,
                "checks": [
                    {"type": c.type, "columns": list(c.columns), "severity": c.severity, "params": c.params}
                    for c in definition.checks
                ],
            }
        )
        compiled[name] = CompiledModel(
            name=name,
            dialect=dialect,
            engine=engine,
            dependencies=deps,
            fingerprint=fingerprint,
            local_fingerprint=local_fingerprint,
            metadata_hash=metadata_hash,
            definition_sql=canonical if ast is not None else None,
            physical_table=_physical_table(name, fingerprint, catalog),
            materialise=definition.materialise,
            strategy=definition.strategy,
            key=definition.key,
            time_column=definition.time_column,
            backfill=definition.backfill,
            cursor=definition.cursor,
            interval=definition.interval,
            tags=definition.tags,
            schedule=definition.schedule,
            columns=definition.columns,
            target=definition.target,
            path=definition.path,
            format=definition.format,
            environments=definition.environments,
            ast=ast,
            owner=definition.owner,
            description=definition.description,
            fn=definition.fn,
            checks=definition.checks,
        )

    python_checks: dict[str, tuple[CheckDef, ...]] = {}
    for check in checks:
        if check.model not in compiled:
            raise DefinitionError(f"@check {check.name!r} references unknown model {check.model!r}")
        python_checks[check.model] = (*python_checks.get(check.model, ()), check)

    return CompiledProject(models=compiled, graph=graph, python_checks=python_checks)
