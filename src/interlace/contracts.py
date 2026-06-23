"""Model output contracts.

A model may declare its expected output columns (and optionally types) via
per-model config. After a model is built, ``apply`` introspects the physical
table and validates it against the contract, raising ``SchemaError`` on drift —
a missing contracted column, or a type mismatch — before the environment is
promoted. Extra columns beyond the contract are allowed (additive evolution).
"""

from __future__ import annotations

from interlace.exceptions import SchemaError

Contract = dict[str, str | None]


def validate_contract(model: str, actual: dict[str, str], contract: Contract) -> None:
    """Raise ``SchemaError`` if the built columns violate the contract."""
    missing = [column for column in contract if column not in actual]
    if missing:
        raise SchemaError(
            f"model {model!r} is missing contracted column(s): {', '.join(sorted(missing))}",
            details={"model": model, "missing": sorted(missing)},
        )

    mismatches = {
        column: {"expected": expected, "actual": actual[column]}
        for column, expected in contract.items()
        if expected is not None and actual[column].upper() != expected.upper()
    }
    if mismatches:
        raise SchemaError(
            f"model {model!r} has column type mismatch(es): {sorted(mismatches)}",
            details={"model": model, "mismatches": mismatches},
        )
