"""Typed project vars: var('name') becomes a literal before the fingerprint."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from interlace.config.config import ProjectConfig, VarConfig
from interlace.dsl.decorators import ModelDef
from interlace.exceptions import DefinitionError
from interlace.graph.project import compile_models
from interlace.ir.macros import parse_macros

pytestmark = pytest.mark.unit

REGION = VarConfig(type="string", value="eu")
MINIMUM = VarConfig(type="int", value=3)
ACTIVE = VarConfig(type="bool", value=True)
SINCE = VarConfig(type="date", value="2024-01-01")
RATE = VarConfig(type="float", value=0.05)
OPENED = VarConfig(type="timestamp", value="2024-01-01T00:00:00")


def _sql(sql: str, variables: dict[str, VarConfig] | None = None) -> str:
    compiled = compile_models(
        [ModelDef(name="orders", sql=sql)],
        variables=variables if variables is not None else {"region": REGION},
    )
    ast = compiled.models["orders"].ast
    assert ast is not None
    return ast.sql(dialect="duckdb")


def test_each_type_becomes_a_literal() -> None:
    rendered = _sql(
        "SELECT * FROM orders WHERE region = var('region') AND n >= var('minimum') "
        "AND flag = var('active') AND day >= var('since') AND rate > var('rate') "
        "AND opened >= var('opened')",
        {
            "region": REGION,
            "minimum": MINIMUM,
            "active": ACTIVE,
            "since": SINCE,
            "rate": RATE,
            "opened": OPENED,
        },
    )
    assert "region = 'eu'" in rendered
    assert "n >= 3" in rendered
    assert "flag = TRUE" in rendered
    assert "CAST('2024-01-01' AS DATE)" in rendered
    assert "rate > 0.05" in rendered
    assert "CAST('2024-01-01T00:00:00' AS TIMESTAMP)" in rendered
    assert "VAR(" not in rendered


def test_an_unknown_name_fails_at_compile() -> None:
    with pytest.raises(DefinitionError, match="unknown var 'missing'"):
        _sql("SELECT var('missing') AS v")


def test_a_non_literal_argument_is_left_as_a_function() -> None:
    assert _sql("SELECT var(region) AS v FROM orders") == "SELECT VAR(region) AS v FROM orders"


def test_editing_a_value_changes_the_fingerprint_of_models_that_read_it() -> None:
    before = compile_models(
        [ModelDef(name="orders", sql="SELECT var('region') AS region"), ModelDef(name="plain", sql="SELECT 1 AS n")],
        variables={"region": REGION},
    )
    after = compile_models(
        [ModelDef(name="orders", sql="SELECT var('region') AS region"), ModelDef(name="plain", sql="SELECT 1 AS n")],
        variables={"region": VarConfig(type="string", value="us")},
    )
    assert before.models["orders"].fingerprint != after.models["orders"].fingerprint
    assert before.models["plain"].fingerprint == after.models["plain"].fingerprint


def test_a_macro_body_can_read_a_var() -> None:
    macros = {m.name.casefold(): m for m in parse_macros("CREATE MACRO region_is() AS var('region');", "duckdb", "m")}
    compiled = compile_models(
        [ModelDef(name="orders", sql="SELECT region_is() AS region")],
        macros=macros,
        variables={"region": REGION},
    )
    ast = compiled.models["orders"].ast
    assert ast is not None
    assert ast.sql(dialect="duckdb") == "SELECT ('eu') AS region"


def test_a_string_rejects_a_number() -> None:
    with pytest.raises(ValidationError, match="string var value"):
        VarConfig(type="string", value=1)


def test_an_int_rejects_a_bool() -> None:
    with pytest.raises(ValidationError, match="int var value"):
        VarConfig(type="int", value=True)


def test_a_date_must_be_iso() -> None:
    with pytest.raises(ValidationError, match="YYYY-MM-DD"):
        VarConfig(type="date", value="yesterday")


def test_a_var_name_must_be_an_identifier() -> None:
    with pytest.raises(ValidationError, match="identifier"):
        ProjectConfig(vars={"not a name": {"type": "string", "value": "x"}})


def test_a_quoted_string_keeps_its_quote() -> None:
    rendered = _sql("SELECT var('region') AS region", {"region": VarConfig(type="string", value="o'brien")})
    assert "o''brien" in rendered or "o\\'brien" in rendered
