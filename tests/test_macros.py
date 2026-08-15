"""SQL macros: parsed from CREATE MACRO, expanded into the IR before fingerprinting."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.discovery import discover_macros
from interlace.exceptions import DefinitionError
from interlace.graph.project import compile_models
from interlace.ir.macros import expand_macros, parse_macros

pytestmark = pytest.mark.unit

CENTS = "CREATE MACRO cents_to_dollars(amount) AS (amount / 100)::numeric(16, 2);"


def _macros(sql: str) -> dict:
    return {m.name.casefold(): m for m in parse_macros(sql, "duckdb", "macros/test.sql")}


def _expand(sql: str, macro_sql: str = CENTS) -> str:
    ast = sqlglot.parse_one(sql, read="duckdb")
    return expand_macros(ast, _macros(macro_sql), "m").sql("duckdb")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_parses_name_params_and_body() -> None:
    [macro] = parse_macros(CENTS, "duckdb", "macros/test.sql")
    assert macro.name == "cents_to_dollars"
    assert macro.params == ("amount",)
    assert macro.body.sql("duckdb") == "CAST((amount / 100) AS DECIMAL(16, 2))"


def test_expands_a_call_with_its_argument() -> None:
    assert _expand("SELECT cents_to_dollars(subtotal) AS s FROM t") == (
        "SELECT (CAST((subtotal / 100) AS DECIMAL(16, 2))) AS s FROM t"
    )


def test_expands_every_call_site() -> None:
    expanded = _expand("SELECT cents_to_dollars(subtotal), cents_to_dollars(tax_paid) FROM t")
    assert expanded.count("CAST((") == 2
    assert "subtotal / 100" in expanded and "tax_paid / 100" in expanded


def test_argument_may_be_an_expression() -> None:
    assert "a + b" in _expand("SELECT cents_to_dollars(a + b) FROM t")


def test_leaves_ordinary_functions_alone() -> None:
    assert _expand("SELECT coalesce(a, b), some_udf(c) FROM t") == "SELECT COALESCE(a, b), SOME_UDF(c) FROM t"


def test_expansion_is_parenthesised_so_precedence_survives() -> None:
    # without the parens this would read as `100 * amount / 100`
    assert _expand("SELECT 100 * cents_to_dollars(amount) FROM t") == (
        "SELECT 100 * (CAST((amount / 100) AS DECIMAL(16, 2))) FROM t"
    )


def test_macros_may_call_macros() -> None:
    nested = CENTS + "\nCREATE MACRO doubled(x) AS cents_to_dollars(x) * 2;"
    assert "amount" not in _expand("SELECT doubled(total) FROM t", nested)
    assert "(total / 100)" in _expand("SELECT doubled(total) FROM t", nested)


def test_a_recursive_macro_is_an_error_not_a_hang() -> None:
    with pytest.raises(DefinitionError, match="did not settle"):
        _expand("SELECT loop(1) FROM t", "CREATE MACRO loop(x) AS loop(x) + 1;")


def test_wrong_arity_names_the_macro() -> None:
    with pytest.raises(DefinitionError, match="takes 1 argument"):
        _expand("SELECT cents_to_dollars(a, b) FROM t")


def test_a_table_macro_is_rejected_with_a_reason() -> None:
    # sqlglot cannot parse a table macro at all, so it arrives as an opaque Command
    with pytest.raises(DefinitionError, match="scalar expression"):
        parse_macros("CREATE MACRO t() AS TABLE SELECT 1 AS x;", "duckdb", "macros/test.sql")


def test_a_non_macro_statement_is_rejected() -> None:
    with pytest.raises(DefinitionError, match="expected only CREATE MACRO"):
        parse_macros("SELECT 1;", "duckdb", "macros/test.sql")


def test_duplicate_definitions_name_both_files(tmp_path: Path) -> None:
    _write(tmp_path / "macros" / "a.sql", CENTS)
    _write(tmp_path / "macros" / "b.sql", CENTS)
    with pytest.raises(DefinitionError, match="defined twice"):
        discover_macros(tmp_path, ["macros"], "duckdb")


def test_missing_macro_directory_is_ignored(tmp_path: Path) -> None:
    assert discover_macros(tmp_path, ["macros"], "duckdb") == {}


# --- the reason expansion happens at compile time --------------------------------


def _fingerprint(macro_sql: str) -> str:
    from interlace.dsl.decorators import ModelDef

    models = compile_models(
        [ModelDef(name="orders", sql="SELECT cents_to_dollars(subtotal) AS subtotal FROM raw")],
        macros=_macros(macro_sql),
    )
    return models.models["orders"].fingerprint


def test_editing_a_macro_changes_its_callers_fingerprint() -> None:
    """The whole reason this is not a warehouse-side CREATE MACRO: a macro created in
    the engine is invisible to the fingerprint, so editing one would leave every model
    that calls it stale with nothing to notice."""
    before = _fingerprint(CENTS)
    after = _fingerprint("CREATE MACRO cents_to_dollars(amount) AS (amount / 1000)::numeric(16, 2);")

    assert before != after


def test_one_definition_renders_per_dialect() -> None:
    """dbt writes default__/postgres__/bigquery__ variants because Jinja renders text.
    This is expanded into the AST, so the transpiler handles the dialect."""
    from interlace.dsl.decorators import ModelDef

    models = compile_models(
        [ModelDef(name="orders", sql="SELECT cents_to_dollars(subtotal) AS subtotal FROM raw")],
        macros=_macros(CENTS),
    )
    ast = models.models["orders"].ast
    assert ast is not None

    assert "subtotal / 100" in ast.sql("duckdb")
    # Postgres would do integer division on `/`, so sqlglot casts first — the exact
    # thing dbt's postgres__cents_to_dollars exists to hand-write
    assert "CAST(subtotal AS DOUBLE PRECISION) / NULLIF(100, 0)" in ast.sql("postgres")
    assert "AS NUMERIC" in ast.sql("bigquery")


def test_a_macro_body_may_reference_a_model_and_that_is_a_dependency() -> None:
    """Expansion happens before dependency resolution, so a table in a macro body is a
    real edge — not a reference to a model nobody knows about."""
    from interlace.dsl.decorators import ModelDef

    models = compile_models(
        [
            ModelDef(name="rates", sql="SELECT 1 AS rate"),
            ModelDef(name="orders", sql="SELECT in_gbp(total) AS total FROM raw"),
        ],
        macros=_macros("CREATE MACRO in_gbp(x) AS x * (SELECT rate FROM rates);"),
    )

    assert models.models["orders"].dependencies == ("rates",)
