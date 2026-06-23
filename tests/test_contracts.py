"""Model output contract validation."""

from __future__ import annotations

import pytest

from interlace.contracts import validate_contract
from interlace.exceptions import SchemaError

pytestmark = pytest.mark.unit

_ACTUAL = {"id": "INTEGER", "name": "VARCHAR"}


def test_passes_when_columns_present() -> None:
    validate_contract("m", _ACTUAL, {"id": None, "name": None})  # no raise


def test_extra_actual_columns_are_allowed() -> None:
    validate_contract("m", _ACTUAL, {"id": None})  # name is extra; fine


def test_missing_column_raises() -> None:
    with pytest.raises(SchemaError) as exc:
        validate_contract("m", _ACTUAL, {"id": None, "missing": None})
    assert "missing" in exc.value.details["missing"]


def test_type_match_is_case_insensitive() -> None:
    validate_contract("m", _ACTUAL, {"id": "integer"})  # no raise


def test_type_mismatch_raises() -> None:
    with pytest.raises(SchemaError):
        validate_contract("m", _ACTUAL, {"id": "VARCHAR"})
