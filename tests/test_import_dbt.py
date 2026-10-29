"""dbt import: the Jinja subset, a project built in the test, and the public examples."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.importers.dbt import import_dbt
from interlace.importers.jinja import UnsupportedJinja, render_template
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project

pytestmark = pytest.mark.unit

runner = CliRunner()

_ORDERS = """
{% set payment_methods = ['credit_card', 'coupon'] %}

select
    {% for payment_method in payment_methods -%}
    sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
    {% endfor -%}
from {{ ref('stg_payments') }}
"""


def test_list_loop_and_ref_expand() -> None:
    rendered = render_template(_ORDERS)
    assert "from stg_payments" in rendered.sql
    assert "credit_card_amount" in rendered.sql
    assert "coupon_amount" in rendered.sql
    assert "{{" not in rendered.sql


def test_macro_dispatch_uses_the_default_body() -> None:
    macros_sql = """
    {% macro cents_to_dollars(column_name) -%}
        {{ return(adapter.dispatch('cents_to_dollars')(column_name)) }}
    {%- endmacro %}
    {% macro default__cents_to_dollars(column_name) -%}
        ({{ column_name }} / 100)::numeric(16, 2)
    {%- endmacro %}
    """
    from interlace.importers.jinja import parse_macros

    rendered = render_template(
        "select {{ cents_to_dollars('subtotal') }} as subtotal from {{ source('ecom', 'raw_orders') }}",
        macros=parse_macros(macros_sql),
    )
    assert "(subtotal / 100)::numeric(16, 2)" in rendered.sql
    assert "raw_orders" in rendered.sql
    assert "ecom" not in rendered.sql


def test_is_incremental_keeps_the_full_refresh() -> None:
    rendered = render_template(
        "select * from {{ ref('orders') }} "
        "{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}"
    )
    assert "where id >" not in rendered.sql
    assert "from orders" in rendered.sql
    assert rendered.warnings


def test_unknown_jinja_is_refused() -> None:
    with pytest.raises(UnsupportedJinja):
        render_template("{% if target.name == 'prod' %} select 1 {% else %} select 2 {% endif %}")


def _project(root: Path) -> None:
    (root / "seeds").mkdir()
    (root / "seeds" / "raw_orders.csv").write_text("id,status\n1,placed\n2,shipped\n")
    (root / "models").mkdir()
    (root / "models" / "stg_orders.sql").write_text("select id as order_id, status from {{ ref('raw_orders') }}\n")
    (root / "models" / "schema.yml").write_text("""
version: 2
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('raw_orders')
              field: id
      - name: status
        tests:
          - accepted_values:
              values: [placed, shipped]
""")
    (root / "snapshots").mkdir()
    (root / "snapshots" / "orders_snapshot.sql").write_text("""
        {% snapshot orders_snapshot %}
        {{ config(unique_key='order_id', strategy='check') }}
        select * from {{ ref('stg_orders') }}
        {% endsnapshot %}
        """)
    (root / "dbt_project.yml").write_text(
        "name: shop\nmodel-paths: [models]\nseed-paths: [seeds]\nsnapshot-paths: [snapshots]\n"
        "models:\n  shop:\n    materialized: table\n"
    )


async def test_import_builds_and_checks_pass(tmp_path: Path) -> None:
    source = tmp_path / "dbt"
    source.mkdir()
    _project(source)
    report = import_dbt(source, tmp_path / "out")

    assert report.skipped == []
    assert set(report.seeds) == {"raw_orders"}
    assert {"stg_orders", "orders_snapshot"} <= set(report.models)
    assert report.checks >= 3

    project = Project.load(report.output)
    compiled = project.compile()
    engine = project.open_engine()
    store = await project.open_state()
    try:
        result = await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
        assert "stg_orders" in result.built
        assert result.checks
        assert all(check.status == "passed" for check in result.checks)
    finally:
        await store.close()
        engine.close()


def test_import_command(tmp_path: Path) -> None:
    source = tmp_path / "dbt"
    source.mkdir()
    _project(source)
    out = tmp_path / "out"
    result = runner.invoke(app, ["import", "dbt", str(source), "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert (out / "interlace.yaml").is_file()
    assert (out / "IMPORT.md").is_file()
    refused = runner.invoke(app, ["import", "dbt", str(source), "--out", str(out)])
    assert refused.exit_code == 1


_CORPUS = (
    ("jaffle-shop-classic", "https://github.com/dbt-labs/jaffle-shop-classic.git"),
    ("jaffle-shop", "https://github.com/dbt-labs/jaffle-shop.git"),
    ("jaffle_shop_duckdb", "https://github.com/dbt-labs/jaffle_shop_duckdb.git"),
    ("mrr-playbook", "https://github.com/dbt-labs/mrr-playbook.git"),
)


@pytest.mark.requires_network
@pytest.mark.parametrize("name,url", _CORPUS)
async def test_public_dbt_projects_compile(tmp_path: Path, name: str, url: str) -> None:
    """The projects a migration actually has to survive. Cloned, not vendored."""
    dest = tmp_path / name
    cloned = shutil.which("git")
    assert cloned
    import subprocess

    subprocess.run(["git", "clone", "--depth", "1", url, str(dest)], check=True, capture_output=True)
    report = import_dbt(dest, tmp_path / "out" / name)
    # A skipped model means the Jinja subset missed something these projects use.
    assert report.skipped == [], [(note.name, note.detail) for note in report.skipped]
    compiled = Project.load(report.output).compile()
    assert compiled.models
    if name == "jaffle-shop-classic":
        project = Project.load(report.output)
        compiled = project.compile()
        engine = project.open_engine()
        store = await project.open_state()
        try:
            result = await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
            failed = [check for check in result.checks if check.status != "passed"]
            assert not failed
            assert "customers" in result.built
        finally:
            await store.close()
            engine.close()
