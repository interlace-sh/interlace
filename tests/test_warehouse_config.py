"""Warehouse configuration: env interpolation, secret rendering, and the DuckLake
attach-options path (remote catalogs, data_path, metadata_schema).

The Postgres-catalog case is exercised at the parsing/wiring level (no Postgres in the
test environment); the options path itself runs for real against a local file catalog
with a custom data_path.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import sqlglot

from interlace.config.config import SecretConfig, load_config
from interlace.engines.duckdb import DuckDBAdapter
from interlace.project import Project, _secret_sql

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


# --- config: ${VAR} interpolation ---------------------------------------------


def test_env_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WH_DSN", "dbname=lakes host=db user=writer")
    monkeypatch.setenv("WH_KEY", "key123")
    (tmp_path / "interlace.yaml").write_text(
        "name: t\n"
        'database: "ducklake:postgres:${WH_DSN}"\n'
        "data_path: s3://core/staged/t/\n"
        "metadata_schema: t\n"
        "secrets:\n"
        "  lake_s3:\n"
        "    key_id: ${WH_KEY}\n"
        "    secret: ${WH_MISSING}\n"
    )
    config = load_config(tmp_path / "interlace.yaml")
    assert config.database == "ducklake:postgres:dbname=lakes host=db user=writer"
    assert config.secrets["lake_s3"].key_id == "key123"
    # Unset vars stay literal — loud, never a silent empty string.
    assert config.secrets["lake_s3"].secret == "${WH_MISSING}"


def test_open_engine_rejects_unresolved_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An unset ${VAR} must fail at engine open — left literal, DuckDB would create
    a directory actually named '${VAR}' before erroring somewhere less obvious."""
    from interlace.exceptions import ConfigurationError

    monkeypatch.delenv("WH_DATA", raising=False)
    (tmp_path / "interlace.yaml").write_text(
        'name: t\ndatabase: ".interlace/warehouse.ducklake"\ndata_path: "${WH_DATA}"\n'
    )
    project = Project.load(tmp_path)
    with pytest.raises(ConfigurationError) as excinfo:
        project.open_engine()
    assert excinfo.value.details["variables"] == ["WH_DATA"]
    assert not (tmp_path / "${WH_DATA}").exists()


# --- secret rendering -----------------------------------------------------------


def test_secret_sql_rendering() -> None:
    sql = _secret_sql(
        "lake_s3",
        SecretConfig(
            key_id="ak", secret="s'k", endpoint="localhost:9000", url_style="path", use_ssl=False, scope="s3://core"
        ),
    )
    assert sql.startswith("CREATE OR REPLACE SECRET lake_s3 (TYPE s3, ")
    assert "KEY_ID 'ak'" in sql
    assert "SECRET 's''k'" in sql  # quoting survives
    assert "ENDPOINT 'localhost:9000'" in sql
    assert "URL_STYLE 'path'" in sql
    assert "USE_SSL false" in sql
    assert "SCOPE 's3://core'" in sql


# --- open_engine wiring ----------------------------------------------------------


def test_remote_catalog_is_not_path_resolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A postgres: catalog DSN must reach ATTACH verbatim (the old code mangled it into
    a filesystem path), with the options and extensions wired through."""
    (tmp_path / "interlace.yaml").write_text(
        "name: ml\n"
        'database: "ducklake:postgres:dbname=lakes host=db"\n'
        "data_path: s3://core/staged/ml/\n"
        "metadata_schema: ml\n"
        "secrets:\n"
        "  lake_s3: {key_id: k, secret: s, endpoint: 'localhost:9000', url_style: path, use_ssl: false}\n"
    )
    captured: dict = {}

    def fake_connect_ducklake(catalog: str, **kwargs: object) -> str:
        captured["catalog"] = catalog
        captured.update(kwargs)
        return "engine"

    monkeypatch.setattr(DuckDBAdapter, "connect_ducklake", staticmethod(fake_connect_ducklake))
    project = Project.load(tmp_path)
    engine = project.open_engine()
    assert engine == "engine"
    assert captured["catalog"] == "ducklake:postgres:dbname=lakes host=db"
    assert captured["alias"] == "ml"
    assert captured["data_path"] == "s3://core/staged/ml/"
    assert captured["metadata_schema"] == "ml"
    assert list(captured["extensions"]) == ["ducklake", "postgres", "httpfs"]
    [secret] = captured["secrets"]
    assert secret.startswith("CREATE OR REPLACE SECRET lake_s3 ")
    # And no junk directory was created from the DSN.
    assert not any("postgres:" in p.name for p in tmp_path.iterdir())


def test_plain_file_ducklake_unchanged(tmp_path: Path) -> None:
    """No options => the original duckdb.connect('ducklake:<path>') fast path."""
    (tmp_path / "interlace.yaml").write_text("name: plain\n")
    project = Project.load(tmp_path)
    engine = project.open_engine()
    try:
        assert (tmp_path / ".interlace").exists()
    finally:
        engine.close()


@pytest.mark.integration
async def test_file_catalog_with_custom_data_path(tmp_path: Path) -> None:
    """The options path end-to-end (offline): file catalog + custom local data_path via
    explicit ATTACH — plan/apply lands Parquet under the configured directory."""
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))
    data_dir = tmp_path / "lake-data"
    config = (project_dir / "interlace.yaml").read_text()
    (project_dir / "interlace.yaml").write_text(
        config + f"\ndatabase: ducklake:{tmp_path}/catalog.ducklake\ndata_path: {data_dir}\n"
    )
    project = Project.load(project_dir)
    engine = project.open_engine()
    try:
        await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS raw")
        # Enough rows that DuckLake writes Parquet rather than inlining in the catalog.
        await engine.execute_sql("CREATE TABLE raw.t AS SELECT range AS n FROM range(1000)")
        reader = await engine.fetch(sqlglot.parse_one("SELECT count(*) AS n FROM raw.t"))
        assert reader.read_all().to_pylist() == [{"n": 1000}]
    finally:
        engine.close()
    assert (tmp_path / "catalog.ducklake").exists()
    assert any(data_dir.rglob("*.parquet"))  # data landed under the custom data_path


# --- secrets are instance-wide: applied once, never re-raced per cursor ----------


async def test_secrets_survive_concurrent_cursors(tmp_path: Path) -> None:
    """Regression: session-init used to re-run CREATE OR REPLACE SECRET on every new
    cursor; under parallel backfills concurrent cursors hit DuckDB's 'catalog
    write-write conflict on alter'. Secrets are instance-wide — once is enough."""
    import asyncio

    engine = DuckDBAdapter.connect_ducklake(
        f"{tmp_path}/wh.ducklake",
        secrets=["CREATE OR REPLACE SECRET core_s3 (TYPE S3, KEY_ID 'k', SECRET 's')"],
    )
    try:
        # every cursor still sees the secret (it was never per-session state)...
        reader = await engine.fetch_sql("SELECT name FROM duckdb_secrets()")
        assert [row["name"] for row in reader.read_all().to_pylist()] == ["core_s3"]
        # ...and a storm of concurrent cursor-opening statements never conflicts
        await asyncio.gather(*(engine.execute_sql(f"CREATE OR REPLACE TABLE t{i % 4} AS SELECT 1") for i in range(64)))
    finally:
        engine.close()


# --- postgres DSNs must name their host (no silent localhost:5432) ---------------


def test_postgres_engine_requires_explicit_host(tmp_path: Path) -> None:
    from interlace.exceptions import ConfigurationError

    (tmp_path / "interlace.yaml").write_text(
        "name: pg\nengines:\n  warehouse:\n    type: postgres\n    database: 'dbname=analytics user=writer'\n"
    )
    with pytest.raises(ConfigurationError, match="names no host"):
        Project.load(tmp_path).open_engines().require("warehouse")


def test_postgres_attach_requires_explicit_host(tmp_path: Path) -> None:
    from interlace.exceptions import ConfigurationError

    (tmp_path / "interlace.yaml").write_text("name: pg\ndatabase: ':memory:'\nattach:\n  crm: 'postgres:dbname=crm'\n")
    with pytest.raises(ConfigurationError, match="names no host"):
        Project.load(tmp_path).open_engine()


def test_explicit_pg_hosts_pass_the_guard() -> None:
    from interlace.project import _require_explicit_pg_host

    _require_explicit_pg_host("postgresql://writer@db.internal:5455/analytics", "t")
    _require_explicit_pg_host("dbname=analytics host=db.internal port=5455", "t")
    _require_explicit_pg_host("postgres:dbname=crm host=/var/run/postgresql", "t")  # deliberate unix socket
