"""Indirect non-breaking rebuild-skip: a downstream model whose output is
provably identical reuses its previous physical table instead of rebuilding.

The invariant under test: an indirectly-changed model's SQL is unchanged and was
previously valid, so it cannot reference newly-added upstream columns — only a
projection ``*`` (or a Python model, which sees whole tables) inherits them.
"""

from __future__ import annotations

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef], environment: str = "prod"):
    engine, store = env
    compiled = compile_models(models)
    plan = await diff(compiled, environment, store)
    return plan, await apply(plan, compiled=compiled, engine=engine, state=store)


async def test_clean_downstream_reuses_previous_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")]
    await _apply(env, v1)
    first = await store.get_environment("prod")

    v2 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")]
    plan, result = await _apply(env, v2)

    assert result.built == ["up"]  # only the directly-changed model rebuilt
    assert result.reused == ["down"]
    second = await store.get_environment("prod")
    assert second["down"] != first["down"]  # fingerprint still advanced

    # the new snapshot points at the ORIGINAL physical table
    old = await store.get_snapshot("down", first["down"])
    new = await store.get_snapshot("down", second["down"])
    assert new is not None and old is not None
    assert new.physical_table == old.physical_table
    assert await _rows(engine, "SELECT x FROM main.down") == [{"x": 1}]  # env view still resolves


async def test_star_downstream_rebuilds_and_inherits_columns(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT * FROM up")])
    _, result = await _apply(env, [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT * FROM up")])

    assert set(result.built) == {"up", "down"}  # star inherits the new column -> rebuild
    assert result.reused == []
    assert await _rows(engine, "SELECT x, y FROM main.down") == [{"x": 1, "y": 2}]


async def test_where_change_is_semantic_and_rebuilds_downstream(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A filter change keeps projections identical but changes data — never skippable."""
    engine, _ = env
    src = "SELECT * FROM (VALUES (1), (2), (3)) AS t (x)"
    await _apply(env, [sql_model("up", f"SELECT x FROM ({src}) q"), sql_model("down", "SELECT x FROM up")])

    v2 = [sql_model("up", f"SELECT x FROM ({src}) q WHERE x > 1"), sql_model("down", "SELECT x FROM up")]
    plan, result = await _apply(env, v2)

    by_name = {c.name: c for c in plan.changes}
    assert by_name["up"].category is ChangeCategory.BREAKING
    assert set(result.built) == {"up", "down"}
    assert result.reused == []
    assert len(await _rows(engine, "SELECT x FROM main.down")) == 2  # downstream sees filtered data


async def test_skip_propagates_down_a_clean_chain(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    v1 = [
        sql_model("a", "SELECT 1 AS x"),
        sql_model("b", "SELECT x FROM a"),
        sql_model("c", "SELECT x FROM b"),
    ]
    await _apply(env, v1)
    v2 = [
        sql_model("a", "SELECT 1 AS x, 2 AS y"),
        sql_model("b", "SELECT x FROM a"),
        sql_model("c", "SELECT x FROM b"),
    ]
    _, result = await _apply(env, v2)

    assert result.built == ["a"]
    assert set(result.reused) == {"b", "c"}  # b is clean, so c's inputs are identical too


async def test_count_star_is_not_a_projection_star(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    v1 = [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT count(*) AS n FROM up")]
    await _apply(env, v1)
    v2 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT count(*) AS n FROM up")]
    _, result = await _apply(env, v2)

    assert result.built == ["up"]
    assert result.reused == ["down"]  # count(*) counts rows; additive columns cannot change it


async def test_python_downstream_always_rebuilds(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    import pyarrow as pa

    def snap(up) -> pa.Table:  # a Python model sees every upstream column
        return up.table()

    v1 = [sql_model("up", "SELECT 1 AS x"), ModelDef(name="snap", fn=snap, depends_on=("up",))]
    await _apply(env, v1)
    v2 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), ModelDef(name="snap", fn=snap, depends_on=("up",))]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"up", "snap"}
    assert result.reused == []


async def test_rebuilt_model_resolves_reused_upstream_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """c reads clean-reused b and semantic d: c rebuilds and must find b's data
    at its OLD physical table (the fingerprint-derived name was never built)."""
    engine, _ = env
    v1 = [
        sql_model("a", "SELECT 1 AS x"),
        sql_model("b", "SELECT x FROM a"),
        sql_model("d", "SELECT 10 AS w"),
        sql_model("c", "SELECT b.x, d.w FROM b, d"),
    ]
    await _apply(env, v1)
    v2 = [
        sql_model("a", "SELECT 1 AS x, 2 AS y"),  # additive -> b clean-reused
        sql_model("b", "SELECT x FROM a"),
        sql_model("d", "SELECT 99 AS w"),  # semantic -> c rebuilds
        sql_model("c", "SELECT b.x, d.w FROM b, d"),
    ]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"a", "d", "c"}
    assert result.reused == ["b"]
    assert await _rows(engine, "SELECT x, w FROM main.c") == [{"x": 1, "w": 99}]


async def test_semantic_change_to_unconsumed_column_skips_downstream(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """Column pruning: y's expression changed, but down reads only x — clean skip."""
    engine, _ = env
    v1 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")]
    await _apply(env, v1)
    v2 = [sql_model("up", "SELECT 1 AS x, 20 AS y"), sql_model("down", "SELECT x FROM up")]
    plan, result = await _apply(env, v2)

    by_name = {c.name: c for c in plan.changes}
    assert by_name["up"].category is ChangeCategory.BREAKING  # the direct change itself is semantic
    assert result.built == ["up"]
    assert result.reused == ["down"]
    assert await _rows(engine, "SELECT x FROM main.down") == [{"x": 1}]


async def test_semantic_change_to_consumed_column_rebuilds_downstream(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, _ = env
    v1 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")]
    await _apply(env, v1)
    v2 = [sql_model("up", "SELECT 5 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"up", "down"}
    assert result.reused == []
    assert await _rows(engine, "SELECT x FROM main.down") == [{"x": 5}]


async def test_column_pruning_attributes_by_join_alias(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Qualified references attribute per-source: c reads u.x and o.w, so a change
    to up.y prunes cleanly even through the join."""
    v1 = [
        sql_model("up", "SELECT 1 AS x, 2 AS y"),
        sql_model("other", "SELECT 10 AS w"),
        sql_model("c", "SELECT u.x, o.w FROM up u, other o"),
    ]
    await _apply(env, v1)
    v2 = [
        sql_model("up", "SELECT 1 AS x, 20 AS y"),
        sql_model("other", "SELECT 10 AS w"),
        sql_model("c", "SELECT u.x, o.w FROM up u, other o"),
    ]
    _, result = await _apply(env, v2)

    assert result.built == ["up"]
    assert result.reused == ["c"]


async def test_unqualified_columns_in_join_disable_pruning(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """An unqualified reference in a multi-source query is unattributable — rebuild."""
    v1 = [
        sql_model("up", "SELECT 1 AS x, 2 AS y"),
        sql_model("other", "SELECT 10 AS w"),
        sql_model("c", "SELECT w FROM up u, other o"),
    ]
    await _apply(env, v1)
    v2 = [
        sql_model("up", "SELECT 1 AS x, 20 AS y"),
        sql_model("other", "SELECT 10 AS w"),
        sql_model("c", "SELECT w FROM up u, other o"),
    ]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"up", "c"}


async def test_changed_aggregate_prunes_but_grouped_column_consumers_survive(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """GROUP BY on plain columns keeps the row set stable: only the changed
    aggregate is touched, so a consumer of the group key skips."""
    engine, _ = env
    src = "SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30)) AS t (k, v)"
    v1 = [sql_model("up", f"SELECT k, sum(v) AS s FROM ({src}) q GROUP BY k"), sql_model("down", "SELECT k FROM up")]
    await _apply(env, v1)
    v2 = [
        sql_model("up", f"SELECT k, sum(v * 2) AS s FROM ({src}) q GROUP BY k"),
        sql_model("down", "SELECT k FROM up"),
    ]
    _, result = await _apply(env, v2)

    assert result.built == ["up"]
    assert result.reused == ["down"]
    assert len(await _rows(engine, "SELECT k FROM main.down")) == 2


async def test_distinct_upstream_disables_pruning(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """DISTINCT dedups over every column: changing y can change the row set of x too."""
    src = "SELECT * FROM (VALUES (1, 10), (1, 20)) AS t (a, b)"
    v1 = [sql_model("up", f"SELECT DISTINCT a AS x, b AS y FROM ({src}) q"), sql_model("down", "SELECT x FROM up")]
    await _apply(env, v1)
    v2 = [
        sql_model("up", f"SELECT DISTINCT a AS x, b % 2 AS y FROM ({src}) q"),
        sql_model("down", "SELECT x FROM up"),
    ]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"up", "down"}


def test_pruning_proofs_bail_on_unsound_constructs() -> None:
    """Each construct here lets a projection edit leak beyond its own column (or
    hides a consumption) — the proofs must fall back to the conservative answer.
    Regression tests for the 2026-07-30 soundness review."""
    from interlace.ir.canonicalize import parse
    from interlace.ir.fingerprint import canonical_sql
    from interlace.plan.differ import _consumed_columns, _direct_impact

    def impact(prev_raw: str, cur_raw: str) -> tuple[tuple[str, ...] | None, frozenset[str] | None]:
        return _direct_impact(canonical_sql(parse(prev_raw)), parse(cur_raw))

    # case folding: definition_sql is stored lowercased; consumers keep author case
    _, touched = impact("SELECT b AS Amount, c FROM t", "SELECT b2 AS Amount, c FROM t")
    consumed = _consumed_columns(parse("SELECT Amount FROM dep"), "dep")
    assert touched is not None and consumed is not None and touched & consumed

    # ordinal ORDER BY + LIMIT: the ordinal silently re-points at a changed projection
    assert impact("SELECT a, b AS x FROM t ORDER BY 2 LIMIT 10", "SELECT a, b * 2 AS x FROM t ORDER BY 2 LIMIT 10") == (
        None,
        None,
    )
    assert impact("SELECT a, b FROM t ORDER BY 2 LIMIT 5", "SELECT a, c, b FROM t ORDER BY 2 LIMIT 5") == (None, None)

    # GROUP BY ALL: the grouping key set tracks the projection list
    assert impact("SELECT k, v AS x FROM t GROUP BY ALL", "SELECT k, v2 AS x FROM t GROUP BY ALL") == (None, None)
    assert impact("SELECT k FROM t GROUP BY ALL", "SELECT k, v FROM t GROUP BY ALL") == (None, None)

    # DISTINCT: adding a column changes the dedup key set of existing columns
    assert impact("SELECT DISTINCT a FROM t", "SELECT DISTINCT a, b FROM t") == (None, None)

    # identical SQL: the change is strategy/materialise config — outside the proofs
    assert impact("SELECT a FROM t", "SELECT a FROM t") == (None, None)

    # USING keys are read from both sides; NATURAL keys are unknowable
    assert _consumed_columns(parse("SELECT dep.a FROM dep JOIN o USING (id)"), "dep") == {"a", "id"}
    assert _consumed_columns(parse("SELECT a FROM dep NATURAL JOIN o"), "dep") is None

    # struct access consumes its root column, qualified or not
    assert _consumed_columns(parse("SELECT rec.f1 AS f FROM dep"), "dep") == {"rec"}
    assert _consumed_columns(parse("SELECT u.rec.f1 FROM dep u"), "dep") == {"rec"}

    # DuckDB COLUMNS(regex) matches unknown (and future) columns
    assert _consumed_columns(parse("SELECT COLUMNS('a.*') FROM dep"), "dep") is None


async def test_strategy_change_with_same_sql_rebuilds_downstream(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """Identical SQL, different strategy: the proofs can't see config, so the
    downstream must rebuild."""
    sql = "SELECT * FROM (VALUES (1, 10)) AS t (id, v)"
    v1 = [sql_model("up", sql), sql_model("down", "SELECT id FROM up")]
    await _apply(env, v1)
    v2 = [sql_model("up", sql, strategy="merge_by_key", key=("id",)), sql_model("down", "SELECT id FROM up")]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"up", "down"}
    assert result.reused == []


async def test_select_subset_pulls_in_changed_ancestors(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Selecting only the downstream of a changed upstream must schedule the
    upstream too — otherwise the downstream builds against a fingerprint table
    that was never materialised."""
    from interlace.plan.differ import diff as diff_fn

    engine, store = env
    v1 = [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")]
    await _apply(env, v1)

    compiled = compile_models([sql_model("up", "SELECT 5 AS x"), sql_model("down", "SELECT x FROM up")])
    plan = await diff_fn(compiled, "prod", store, select={"down"})
    result = await apply(plan, compiled=compiled, engine=engine, state=store)

    assert set(result.built) == {"up", "down"}
    assert await _rows(engine, "SELECT x FROM main.down") == [{"x": 5}]


async def test_reuse_survives_plan_render_fields(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    await _apply(env, [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")])
    engine, store = env
    compiled = compile_models([sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")])
    plan = await diff(compiled, "prod", store)

    up = next(c for c in plan.changes if c.name == "up")
    assert up.impacted_columns == ("y",)  # additive columns surfaced for diff display
    assert {s.name for s in plan.reuses} == {"down"}
    assert {t.snapshot.name for t in plan.backfills} == {"up"}
