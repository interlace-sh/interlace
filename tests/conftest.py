"""Shared fixtures and helpers for the test suite.

``env`` is the standard throwaway warehouse: an in-memory DuckDB engine plus an
on-disk state store, both closed after the test. Files needing seeded data or an
engine registry define a local ``env`` that shadows this one.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.engines.base import EngineAdapter
from interlace.engines.duckdb import DuckDBAdapter
from interlace.state.store import SqliteStateStore

# Rich styles its output when the environment tells it to, whatever the stream is —
# so a developer (or a task runner) exporting FORCE_COLOR / TTY_COMPATIBLE turned
# every CLI assertion into a coin flip: `assert "1/1 passed" in result.output` fails
# against "Checks: \x1b[1;36m1\x1b[0m/\x1b[1;36m1\x1b[0m passed". This has to run at
# conftest IMPORT, not in a fixture: `cli.main` builds its Console at module import,
# and latches the decision there. TERM=dumb as well as the pops, because NO_COLOR
# still emits bold, and because it holds even if something forces colour later.
for _forced_colour in ("FORCE_COLOR", "CLICOLOR_FORCE", "TTY_COMPATIBLE"):
    os.environ.pop(_forced_colour, None)
os.environ["TERM"] = "dumb"


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
