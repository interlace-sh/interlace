"""Per-model config for SQL files.

A SQL model may declare its materialisation, strategy, key, etc. via a leading
block comment whose YAML is namespaced under ``interlace`` — valid SQL, no Jinja,
ignored by the engine:

    /*
    interlace:
      materialise: view
      key: order_id
    */
    SELECT ...

Only the first block comment is considered, and only when it parses to a mapping
with a top-level ``interlace`` key; otherwise the SQL is left untouched.
"""

from __future__ import annotations

import re
from typing import Any

import yaml

from interlace.exceptions import ConfigurationError

_BLOCK_COMMENT = re.compile(r"/\*(.*?)\*/", re.DOTALL)


def extract_sql_config(content: str) -> tuple[dict[str, Any], str]:
    """Return ``(config, sql)`` — the parsed config and the SQL with the block removed."""
    match = _BLOCK_COMMENT.search(content)
    if match is None:
        return {}, content
    try:
        parsed = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return {}, content
    if not isinstance(parsed, dict) or "interlace" not in parsed:
        return {}, content

    config = parsed["interlace"] or {}
    if not isinstance(config, dict):
        raise ConfigurationError("SQL model config must be a mapping", details={"got": type(config).__name__})

    sql = (content[: match.start()] + content[match.end() :]).strip()
    return config, sql
