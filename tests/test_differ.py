"""Plan differ: classify added/modified/removed and breaking/non-breaking."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from interlace.dsl.decorators import ModelDef
from interlace.graph.project import CompiledProject, compile_models
from interlace.plan.differ import diff, snapshot_of
from interlace.plan.plan import ChangeType
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


async def promote(store: SqliteStateStore, environment: str, project: CompiledProject) -> None:
    """Simulate an apply: persist each model's snapshot and point the environment at it."""
    for model in project.ordered():
        await store.add_snapshot(snapshot_of(model, ChangeCategory.BREAKING))
    await store.promote(environment, {name: m.fingerprint for name, m in project.models.items()})


@pytest.fixture()
async def store(tmp_path: Path) -> AsyncIterator[SqliteStateStore]:
    s = await SqliteStateStore.open(tmp_path / "state.db")
    yield s
    await s.close()


async def test_all_added_against_empty_environment(store: SqliteStateStore) -> None:
    project = compile_models([sql_model("a", "SELECT 1 AS x"), sql_model("b", "SELECT * FROM a")])
    plan = await diff(project, "prod", store)

    assert {c.name: c.change_type for c in plan.changes} == {"a": ChangeType.ADDED, "b": ChangeType.ADDED}
    assert len(plan.backfills) == 2
    assert len(plan.virtual_updates) == 2


async def test_no_changes_when_project_unchanged(store: SqliteStateStore) -> None:
    await promote(store, "prod", compile_models([sql_model("a", "SELECT 1 AS x")]))
    plan = await diff(compile_models([sql_model("a", "SELECT 1 AS x")]), "prod", store)

    assert plan.is_empty
    assert plan.changes == []


async def test_added_column_is_non_breaking(store: SqliteStateStore) -> None:
    await promote(store, "prod", compile_models([sql_model("a", "SELECT 1 AS x")]))
    plan = await diff(compile_models([sql_model("a", "SELECT 1 AS x, 2 AS y")]), "prod", store)

    (change,) = plan.changes
    assert change.change_type is ChangeType.MODIFIED
    assert change.category is ChangeCategory.NON_BREAKING


async def test_changed_expression_is_breaking(store: SqliteStateStore) -> None:
    await promote(store, "prod", compile_models([sql_model("a", "SELECT 1 AS x")]))
    plan = await diff(compile_models([sql_model("a", "SELECT 2 AS x")]), "prod", store)

    assert plan.changes[0].category is ChangeCategory.BREAKING
    assert plan.has_breaking_changes


async def test_indirect_change_inherits_non_breaking(store: SqliteStateStore) -> None:
    await promote(
        store, "prod", compile_models([sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")])
    )
    v2 = compile_models([sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")])
    plan = await diff(v2, "prod", store)

    by_name = {c.name: c for c in plan.changes}
    assert by_name["up"].category is ChangeCategory.NON_BREAKING  # added column = direct non-breaking
    assert by_name["down"].change_type is ChangeType.MODIFIED  # fingerprint moved via upstream
    assert by_name["down"].category is ChangeCategory.NON_BREAKING  # indirect, inherits non-breaking


async def test_indirect_change_inherits_breaking(store: SqliteStateStore) -> None:
    await promote(
        store, "prod", compile_models([sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")])
    )
    v2 = compile_models([sql_model("up", "SELECT 9 AS x"), sql_model("down", "SELECT x FROM up")])
    plan = await diff(v2, "prod", store)

    by_name = {c.name: c for c in plan.changes}
    assert by_name["up"].category is ChangeCategory.BREAKING
    assert by_name["down"].category is ChangeCategory.BREAKING


async def test_removed_model_detected(store: SqliteStateStore) -> None:
    await promote(store, "prod", compile_models([sql_model("a", "SELECT 1 AS x"), sql_model("b", "SELECT 2 AS y")]))
    plan = await diff(compile_models([sql_model("a", "SELECT 1 AS x")]), "prod", store)

    removed = [c for c in plan.changes if c.change_type is ChangeType.REMOVED]
    assert [c.name for c in removed] == ["b"]
    assert removed[0].previous_fingerprint is not None
