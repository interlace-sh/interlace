"""Snapshot fingerprinting determinism and sensitivity."""

from __future__ import annotations

import pytest
import sqlglot

from interlace.ir.fingerprint import data_fingerprint, metadata_fingerprint

pytestmark = pytest.mark.unit


def fp(query: str, *, strategy: dict | None = None, upstreams: list[str] | None = None) -> str:
    return data_fingerprint(
        query=sqlglot.parse_one(query),
        strategy_config=strategy or {"strategy": "full"},
        upstream_fingerprints=upstreams or [],
    )


def test_fingerprint_is_deterministic() -> None:
    assert fp("SELECT 1") == fp("SELECT 1")


def test_query_change_changes_fingerprint() -> None:
    assert fp("SELECT 1") != fp("SELECT 2")


def test_strategy_config_change_changes_fingerprint() -> None:
    assert fp("SELECT 1", strategy={"strategy": "full"}) != fp("SELECT 1", strategy={"strategy": "merge"})


def test_upstream_change_propagates() -> None:
    assert fp("SELECT 1", upstreams=["aaaa"]) != fp("SELECT 1", upstreams=["bbbb"])


def test_upstream_order_is_irrelevant() -> None:
    assert fp("SELECT 1", upstreams=["aaaa", "bbbb"]) == fp("SELECT 1", upstreams=["bbbb", "aaaa"])


def test_metadata_fingerprint_is_independent_of_data() -> None:
    assert metadata_fingerprint({"owner": "alice"}) != metadata_fingerprint({"owner": "bob"})
