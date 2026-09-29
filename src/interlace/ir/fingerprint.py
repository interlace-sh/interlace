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
import inspect
import json
import re
import textwrap
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlglot import exp

_FP_LEN = 16


def canonical_sql(ast: exp.Expr) -> str:
    """Render an AST to a stable, comment-free, normalised string for hashing."""
    return ast.sql(comments=False, normalize=True, pretty=False)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(*parts: str) -> str:
    h = hashlib.sha256("\x00".join(parts).encode("utf-8"))
    return h.hexdigest()[:_FP_LEN]


def data_fingerprint(
    *,
    query: str | exp.Expr,
    strategy_config: dict[str, Any],
    upstream_fingerprints: list[str],
) -> str:
    """Fingerprint that changes whenever the model's output could change.

    ``query`` is the canonical SQL for SQL models, or :func:`python_source` for
    Python models.
    """
    sql = canonical_sql(query) if isinstance(query, exp.Expr) else query
    return _digest(sql, _stable_json(strategy_config), *sorted(upstream_fingerprints))


def python_source(fn: Callable[..., Any]) -> str:
    """Dedented function source, plus captured defaults and closure values.

    A literal written in the signature (``cursor=None``) is already in the
    source, so it is not repeated and existing fingerprints stay put. A factory
    default (``tenant=tenant``) and every closure cell are appended, so two
    models generated from one function body fingerprint differently and editing
    the captured value replans. Plain data is hashed in full. Anything else
    (a client, a connection) is hashed by type only — its ``repr`` can contain
    a memory address. Bytecode is not hashed.
    """
    return _python_source(fn, set())


def _python_source(fn: Callable[..., Any], seen: set[int]) -> str:
    if id(fn) in seen:
        return getattr(fn, "__qualname__", type(fn).__qualname__)
    seen.add(id(fn))
    source = textwrap.dedent(inspect.getsource(fn))
    captured = _captured(fn, source, seen)
    if not captured:
        return source
    return f"{source}\n{_stable_json(captured)}"


def _captured(fn: Callable[..., Any], source: str, seen: set[int]) -> dict[str, Any]:
    defaults = _bound_defaults(fn, _signature_header(source), seen)
    closures = _bound_closures(fn, seen)
    captured: dict[str, Any] = {}
    if defaults:
        captured["defaults"] = defaults
    if closures:
        captured["closures"] = closures
    return captured


def _bound_defaults(fn: Callable[..., Any], header: str, seen: set[int]) -> dict[str, Any]:
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return {}
    defaults: dict[str, Any] = {}
    for name, param in signature.parameters.items():
        if param.default is inspect.Parameter.empty or _written_as_literal(header, name, param.default):
            continue
        defaults[name] = _stable_value(param.default, seen)
    return defaults


def _bound_closures(fn: Callable[..., Any], seen: set[int]) -> dict[str, Any]:
    cells = fn.__closure__
    if not cells:
        return {}
    closures: dict[str, Any] = {}
    for name, cell in zip(fn.__code__.co_freevars, cells, strict=True):
        try:
            contents = cell.cell_contents
        except ValueError:
            continue  # the cell is empty; nothing was captured
        closures[name] = _stable_value(contents, seen)
    return closures


def _signature_header(source: str) -> str:
    """The ``def`` line only, so a mention of ``cursor=None`` in the body is not a literal."""
    start = source.find("def ")
    if start < 0:
        return source
    depth = 0
    seen_paren = False
    for index, char in enumerate(source[start:], start):
        if char == "(":
            depth += 1
            seen_paren = True
        elif char == ")":
            depth -= 1
        elif char == ":" and seen_paren and depth == 0:
            return source[start:index]
    return source[start:]


def _written_as_literal(header: str, name: str, value: object) -> bool:
    for form in _literal_forms(value):
        # An annotation may sit between the name and `=`: `cursor: object = None`.
        pattern = rf"(?<!\w){re.escape(name)}(?:\s*:\s*[^=]+?)?\s*=\s*{re.escape(form)}(?!\w)"
        if re.search(pattern, header):
            return True
    return False


def _literal_forms(value: object) -> tuple[str, ...]:
    if value is None:
        return ("None",)
    if isinstance(value, bool):
        return ("True",) if value else ("False",)
    if isinstance(value, int):
        return (repr(value),)
    if isinstance(value, float):
        return (repr(value),)
    if isinstance(value, str):
        return tuple(dict.fromkeys((repr(value), json.dumps(value))))
    return ()


def _stable_value(value: object, seen: set[int]) -> Any:
    if isinstance(value, (list, tuple, dict, set, frozenset)):
        return _stable_collection(value, seen)
    if inspect.isfunction(value) or inspect.ismethod(value):
        return _stable_callable(value, seen)
    return _stable_scalar(value)


def _stable_scalar(value: object) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return repr(value) if value != value or value in (float("inf"), float("-inf")) else value
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": bytes(value).hex()}
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}"}


def _stable_collection(
    value: list[Any] | tuple[Any, ...] | dict[Any, Any] | set[Any] | frozenset[Any], seen: set[int]
) -> Any:
    if id(value) in seen:
        return {"cycle": True}
    seen.add(id(value))
    if isinstance(value, (list, tuple)):
        return [_stable_value(item, seen) for item in value]
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda pair: str(pair[0]))
        return {str(key): _stable_value(item, seen) for key, item in items}
    stable = [_stable_value(item, seen) for item in value]
    return sorted(stable, key=lambda item: json.dumps(item, sort_keys=True, default=str))


def _stable_callable(value: Callable[..., Any], seen: set[int]) -> Any:
    if inspect.ismethod(value):
        body = _callable_source(value.__func__, seen, "method")
        return {"method": body, "self": type(value.__self__).__qualname__}
    return {"function": _callable_source(value, seen, "function")}


def _callable_source(fn: Callable[..., Any], seen: set[int], fallback: str) -> str:
    try:
        return _python_source(fn, seen)
    except (OSError, TypeError):
        return getattr(fn, "__qualname__", fallback)


def metadata_fingerprint(metadata: dict[str, Any]) -> str:
    """Fingerprint over non-semantic metadata (comments, owner, tags)."""
    return _digest(_stable_json(metadata))


def physical_fingerprint(spec: dict[str, Any]) -> str:
    """Fingerprint over indexes, constraints, and schema policy.

    Empty when the model declares none of them, so it matches snapshots written
    before this hash existed. It is deliberately not part of :func:`data_fingerprint`:
    adding an index must not rebuild the table or invalidate downstream models.
    """
    if not spec:
        return ""
    return _digest(_stable_json(spec))
