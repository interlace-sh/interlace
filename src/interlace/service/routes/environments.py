"""Environment list, drop, history, and rollback."""

from __future__ import annotations

from litestar import delete, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException
from litestar.params import FromPath, FromQuery

from interlace.graph.project import CompiledProject
from interlace.scheduler.daemon import (
    reload_if_stale,
)
from interlace.service.types import (
    EnvironmentInfo,
    RollbackRequest,
)
from interlace.state.locks import hold_apply_lock


@get("/environments")
async def get_environments(state: State) -> list[EnvironmentInfo]:
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    out: list[EnvironmentInfo] = []
    promoted_ats = await state.store.environment_promoted_at()
    for env in await state.store.list_environments():
        promoted = await state.store.get_environment(env)
        changed = sum(1 for model in compiled.models.values() if promoted.get(model.name) != model.fingerprint)
        out.append(EnvironmentInfo(name=env, models=len(promoted), changed=changed, promoted_at=promoted_ats.get(env)))
    return out


@delete("/environments/{name:str}", opt={"scope": "admin"}, status_code=200)
async def drop_environment_endpoint(name: FromPath[str], state: State, force: FromQuery[bool] = False) -> dict:
    """Drop an environment: views removed, snapshots released to gc. Production needs force=true."""
    from interlace.plan.plan import PRODUCTION_ENV
    from interlace.state.janitor import drop_environment

    if name == PRODUCTION_ENV and not force:
        raise ClientException(detail=f"{name!r} is the production environment; pass force=true to drop it")
    if not await state.store.get_environment(name):
        raise NotFoundException(detail=f"unknown environment: {name}")
    async with hold_apply_lock(state.store, owner=state.lock_owner):
        dropped = await drop_environment(state.store, engines=state.engines, environment=name)
    await state.store.append_event("environment.dropped", entity=name, payload={"views": dropped})
    return {"environment": name, "dropped_views": dropped}


@get("/environments/{name:str}/history")
async def get_environment_history(name: FromPath[str], state: State) -> list[dict]:
    """Promotion generations, newest first — the rollback targets."""
    return [dict(row) for row in await state.store.list_generations(name)]


@post("/environments/{name:str}/rollback", opt={"scope": "admin"}, status_code=200)
async def rollback_environment_endpoint(name: FromPath[str], state: State, data: RollbackRequest | None = None) -> dict:
    """Repoint the environment's views at an earlier promotion generation.
    Nothing rebuilds — the views move."""
    from interlace.exceptions import PlanError
    from interlace.state.janitor import rollback_environment

    request = data or RollbackRequest()
    try:
        async with hold_apply_lock(state.store, owner=state.lock_owner):
            result = await rollback_environment(
                state.store,
                engines=state.engines,
                environment=name,
                to_generation=request.generation,
            )
    except PlanError as exc:
        raise ClientException(detail=exc.message) from exc
    await state.store.append_event("environment.rolled_back", entity=name, payload=result)
    return result
