"""Remote ADBC engine adapters — capabilities, dialects, and connect guards.

Postgres and Redshift share the PG wire and are exercised live in
``test_postgres_engine``. Snowflake and BigQuery are ALPHA: there is no local
account to run against, so they are covered here at the SQL-shape/config level
only (dialect-correct statement generation via sqlglot, right caps, and the
missing-driver guard)."""

from __future__ import annotations

import pyarrow as pa
import pytest
import sqlglot

from interlace.engines.adbc import arrow_type_name
from interlace.engines.base import EngineCaps
from interlace.engines.bigquery import BigQueryAdapter
from interlace.engines.redshift import RedshiftAdapter
from interlace.engines.snowflake import SnowflakeAdapter
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies.merge import Merge
from interlace.strategies.scd import Scd

pytestmark = pytest.mark.unit

_TARGET = TableRef(schema="s", name="t")


def _rel(sql: str) -> SqlRelation:
    return SqlRelation(ast=sqlglot.parse_one(sql))


def test_dialects_and_capabilities() -> None:
    # Redshift: PG wire, no CREATE OR REPLACE / star-EXCLUDE, but has MERGE.
    assert RedshiftAdapter.dialect == "redshift"
    assert RedshiftAdapter.caps.supports_merge
    assert not RedshiftAdapter.caps.supports_create_or_replace
    assert not RedshiftAdapter.caps.supports_star_exclude
    # Snowflake / BigQuery: the full set.
    for adapter in (SnowflakeAdapter, BigQueryAdapter):
        assert adapter.caps.supports_create_or_replace
        assert adapter.caps.supports_star_exclude
        assert adapter.caps.supports_merge
    assert SnowflakeAdapter.dialect == "snowflake"
    assert BigQueryAdapter.dialect == "bigquery"


def test_arrow_type_name_uses_planner_vocabulary() -> None:
    assert arrow_type_name(pa.int8()) == "TINYINT"
    assert arrow_type_name(pa.int32()) == "INTEGER"
    assert arrow_type_name(pa.int64()) == "BIGINT"
    assert arrow_type_name(pa.float32()) == "FLOAT"
    assert arrow_type_name(pa.float64()) == "DOUBLE"
    assert arrow_type_name(pa.string()) == "VARCHAR"
    assert arrow_type_name(pa.bool_()) == "BOOLEAN"
    assert arrow_type_name(pa.date32()) == "DATE"
    assert arrow_type_name(pa.timestamp("us")) == "TIMESTAMP"


@pytest.mark.parametrize("dialect", ["redshift", "snowflake", "bigquery"])
def test_merge_transpiles_to_each_dialect(dialect: str) -> None:
    caps = EngineCaps(supports_merge=True)
    statements = Merge(("id",)).plan_statements(_rel("SELECT id, v FROM src"), _TARGET, caps, None, columns=["id", "v"])
    sql = statements[0].sql(dialect=dialect)
    assert sql.startswith("MERGE INTO") and "WHEN MATCHED" in sql and "WHEN NOT MATCHED" in sql


@pytest.mark.parametrize(
    ("dialect", "star_exclude"),
    [("redshift", False), ("snowflake", True), ("bigquery", True)],
)
def test_scd_transpiles_to_each_dialect(dialect: str, star_exclude: bool) -> None:
    caps = EngineCaps(supports_star_exclude=star_exclude)
    statements = Scd(("id",)).plan_statements(_rel("SELECT id, tier FROM src"), _TARGET, caps)
    for statement in statements:  # every statement must render in the dialect without error
        assert statement.sql(dialect=dialect)


def test_snowflake_and_bigquery_connect_need_their_extra() -> None:
    # the optional ADBC drivers are not installed in the base/dev environment
    with pytest.raises(ConfigurationError, match="adbc-snowflake"):
        SnowflakeAdapter.connect("user:pw@account/db/schema")
    with pytest.raises(ConfigurationError, match="adbc-bigquery"):
        BigQueryAdapter.connect("my-gcp-project")
