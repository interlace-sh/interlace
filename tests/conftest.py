"""Shared fixtures and helpers for the test suite.

``env`` is the standard throwaway warehouse: an in-memory DuckDB engine plus an
on-disk state store, both closed after the test. Files needing seeded data or an
engine registry define a local ``env`` that shadows this one.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.engines.base import EngineAdapter
from interlace.engines.duckdb import DuckDBAdapter
from interlace.state.store import SqliteStateStore


@pytest.fixture(autouse=True)
def _isolate_interlace_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI tests must not inherit the developer's INTERLACE_ENV."""
    monkeypatch.delenv("INTERLACE_ENV", raising=False)


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


async def fetch_rows(engine: EngineAdapter, sql: str) -> list[dict]:
    """Rows of ``sql`` as dicts — the standard assertion helper."""
    return (await engine.fetch(sqlglot.parse_one(sql))).read_all().to_pylist()
