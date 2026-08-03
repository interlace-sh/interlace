"""Snapshot fingerprinting.

A model's ``data`` fingerprint is a hash of its normalised SQL AST, its
materialisation/strategy config, and the sorted fingerprints of its upstreams —
so any change that affects results (here or upstream) yields a new fingerprint
and triggers a rebuild. A separate ``metadata`` fingerprint covers comments,
owner, and tags, which must never trigger a rebuild.

This mirrors sqlmesh's snapshot model. The hash is deliberately short (16 hex
chars) because it becomes part of physical table names.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlglot import exp

_FP_LEN = 16


def canonical_sql(ast: exp.Expression) -> str:
    """Render an AST to a stable, comment-free, normalised string for hashing."""
    return ast.sql(comments=False, normalize=True, pretty=False)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(*parts: str) -> str:
    h = hashlib.sha256("\x00".join(parts).encode("utf-8"))
    return h.hexdigest()[:_FP_LEN]


def data_fingerprint(
    *,
    query: str | exp.Expression,
    strategy_config: dict[str, Any],
    upstream_fingerprints: list[str],
) -> str:
    """Fingerprint that changes whenever the model's output could change.

    ``query`` is the canonical SQL for SQL models, or the dedented function
    source for Python models (produced by the caller via ``inspect.getsource``).
    """
    sql = canonical_sql(query) if isinstance(query, exp.Expression) else query
    return _digest(sql, _stable_json(strategy_config), *sorted(upstream_fingerprints))


def metadata_fingerprint(metadata: dict[str, Any]) -> str:
    """Fingerprint over non-semantic metadata (comments, owner, tags)."""
    return _digest(_stable_json(metadata))
