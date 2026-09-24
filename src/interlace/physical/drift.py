"""Report external-table drift without mutating it.

Column policy is enforced when a delivery is aligned (see ``plan.apply``). This
pass is what ``plan`` can say before any write: unexpected indexes, and — when
the model declares a ``columns`` contract — columns the live table is missing
or has at a type the policy will not alter.
"""

from __future__ import annotations

from interlace.plan.plan import DriftNote

_NUMERIC_WIDTH = {"TINYINT": 0, "SMALLINT": 1, "INTEGER": 2, "BIGINT": 3, "FLOAT": 4, "DOUBLE": 5}
_TYPE_ALIASES = {
    "INT": "INTEGER",
    "INT4": "INTEGER",
    "INT8": "BIGINT",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "FLOAT8": "DOUBLE",
    "FLOAT4": "FLOAT",
}


def _norm(dtype: str) -> str:
    upper = dtype.upper()
    return _TYPE_ALIASES.get(upper, upper)


def _widens(current: str, incoming: str) -> bool:
    current, incoming = _norm(current), _norm(incoming)
    return (
        current in _NUMERIC_WIDTH and incoming in _NUMERIC_WIDTH and _NUMERIC_WIDTH[incoming] > _NUMERIC_WIDTH[current]
    )


def column_drift(
    model: str,
    target: str,
    policy: str,
    live: dict[str, str],
    contract: dict[str, str | None],
) -> list[DriftNote]:
    """Compare a live external table to the model's column contract.

    Extra live columns are always a non-blocking note: nothing here drops them.
    ``reject`` blocks on a missing column or a type change that is not a numeric widen.
    """
    notes: list[DriftNote] = []
    live_by_fold = {name.casefold(): (name, dtype) for name, dtype in live.items()}
    contract_folds = {name.casefold() for name in contract}
    for name, dtype in contract.items():
        found = live_by_fold.get(name.casefold())
        if found is None:
            message = f"{model}: {target} has no column {name}"
            if policy == "reject":
                notes.append(DriftNote(model, message + "; schema.columns is reject", blocking=True))
            elif policy == "ignore":
                notes.append(DriftNote(model, message + "; schema.columns is ignore, so delivery will not add it"))
            else:
                notes.append(DriftNote(model, message + "; delivery will add it"))
            continue
        if dtype is None:
            continue
        _live_name, live_type = found
        if _norm(live_type) == _norm(dtype) or _widens(live_type, dtype):
            continue
        message = f"{model}: {target}.{name} is {live_type}, model contract says {dtype}"
        if policy == "reject":
            notes.append(DriftNote(model, message + "; schema.columns is reject", blocking=True))
        elif policy == "ignore":
            notes.append(DriftNote(model, message + "; schema.columns is ignore, so the type is left unchanged"))
        else:
            notes.append(DriftNote(model, message + "; delivery will cast into the existing type"))
    for name in live:
        if name.casefold() not in contract_folds:
            notes.append(
                DriftNote(model, f"{model}: {target} has column {name} the model does not declare; left in place")
            )
    return notes
