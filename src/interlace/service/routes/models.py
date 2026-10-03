"""Model list, detail, preview, impact, and failing-check rows."""

from __future__ import annotations

from litestar import get
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException
from litestar.params import FromPath, FromQuery
from litestar.response import Redirect

from interlace import __version__
from interlace.graph.project import CompiledProject
from interlace.scheduler.daemon import (
    reload_if_stale,
)
from interlace.service.present import (
    _info,
    _output,
    _python_source,
    _sample,
)
from interlace.service.types import (
    ConstraintInfo,
    ImpactColumn,
    ImpactResponse,
    IndexInfo,
    ModelDetail,
    ModelInfo,
    SampleResponse,
    SchemaPolicyInfo,
)


@get("/health")
async def health(state: State) -> dict[str, str]:
    status = getattr(state, "startup_status", "ok")
    body = {"status": status, "version": __version__, "environment": state.environment}
    if status == "starting":
        body["detail"] = "startup apply is still running"
    elif status == "error":
        body["detail"] = getattr(state, "startup_error", "") or "startup apply failed"
    return body


@get("/", include_in_schema=False)
async def ui_redirect() -> Redirect:
    return Redirect(path="/ui/")


@get("/models")
async def get_models(state: State) -> list[ModelInfo]:
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    return [
        _info(compiled.models[name], bool(compiled.models[name].checks) or bool(compiled.python_checks.get(name)))
        for name in compiled.graph.topological_sort()
    ]


@get("/models/{name:str}")
async def get_model(name: FromPath[str], state: State) -> ModelDetail:
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    if name not in compiled.models:
        raise NotFoundException(detail=f"unknown model: {name}")
    model = compiled.models[name]
    cols = state.lineage.get(name, {})
    policy = model.schema_policy
    return ModelDetail(
        name=name,
        output=_output(model),
        materialise=model.materialise,
        strategy=model.strategy,
        is_terminal=model.is_terminal,
        fingerprint=model.fingerprint,
        depends_on=list(model.dependencies),
        upstream=sorted(compiled.graph.ancestors(name)),
        downstream=sorted(compiled.graph.descendants(name)),
        columns={col: [f"{t}.{c}" for t, c in refs] for col, refs in cols.items()},
        tags=list(model.tags),
        owner=model.owner,
        schedule=model.schedule,
        sql=model.definition_sql,
        language="python" if model.ast is None else "sql",
        source=_python_source(model),
        indexes=[
            IndexInfo(columns=list(spec.columns), name=spec.object_name(name), unique=spec.unique)
            for spec in model.indexes
        ],
        constraints=[
            ConstraintInfo(
                type=spec.type,
                name=spec.object_name(name),
                columns=list(spec.columns),
                expression=spec.expression,
                reference=spec.reference,
                fields=list(spec.fields),
            )
            for spec in model.constraints
        ],
        schema=SchemaPolicyInfo(columns=policy.columns, indexes=policy.indexes, constraints=policy.constraints),
    )


@get("/models/{name:str}/impact")
async def get_model_impact(name: FromPath[str], state: State, column: FromQuery[str]) -> ImpactResponse:
    """Column-level blast radius of ``{name}.{column}``: every downstream column
    transitively derived from it, plus opaque consumers (Python / ``*`` models)."""
    from interlace.graph.column_lineage import column_impact

    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    if name not in compiled.models:
        raise NotFoundException(detail=f"unknown model: {name}")
    result = column_impact(compiled, name, column)
    return ImpactResponse(
        source=result["source"],
        impacted=[ImpactColumn(**row) for row in result["impacted"]],
        opaque_consumers=result["opaque_consumers"],
    )


@get("/models/{name:str}/preview")
async def get_model_preview(
    name: FromPath[str],
    state: State,
    environment: FromQuery[str | None] = None,
    limit: FromQuery[int] = 25,
) -> SampleResponse:
    """A row sample and a column profile of the model as promoted in ``environment``.

    Ephemeral and file models, and anything not built yet, come back with
    ``available`` false and a ``message`` — the last build is still attached, so a
    failure can be read before a table exists."""
    from interlace.exceptions import DefinitionError
    from interlace.inspect import preview_model

    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    if name not in compiled.models:
        raise NotFoundException(detail=f"unknown model: {name}")
    model = compiled.models[name]
    try:
        preview = await preview_model(
            compiled,
            state.store,
            state.engines.require(model.engine, model=name),
            name,
            environment or state.environment,
            limit,
        )
    except DefinitionError as exc:
        raise ClientException(detail=exc.message) from exc
    return _sample(preview, profile=True)


@get("/models/{name:str}/checks/{check:str}/rows")
async def get_check_rows(
    name: FromPath[str],
    check: FromPath[str],
    state: State,
    environment: FromQuery[str | None] = None,
    limit: FromQuery[int] = 25,
) -> SampleResponse:
    """The rows a check rejected. Table-level checks and Python checks set
    ``available`` false — they have no row set. The promotion gate is unchanged."""
    from interlace.exceptions import DefinitionError
    from interlace.inspect import failing_rows

    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    if name not in compiled.models:
        raise NotFoundException(detail=f"unknown model: {name}")
    model = compiled.models[name]
    try:
        sample = await failing_rows(
            compiled,
            state.store,
            state.engines.require(model.engine, model=name),
            name,
            check,
            environment or state.environment,
            limit,
        )
    except DefinitionError as exc:
        if "unknown" in exc.message[:40].lower():
            raise NotFoundException(detail=exc.message) from exc
        raise ClientException(detail=exc.message) from exc
    return _sample(sample, profile=False)
