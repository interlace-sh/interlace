"""Scaffold a new interlace project (used by ``interlace init``).

Writes a minimal, immediately-runnable project with no external data source: a
seed SQL model, a Python model over it (Arrow in and out), and a SQL rollup over
the Python model's output — a small SQL → Python → SQL chain that shows how the
two model kinds compose.
"""

from __future__ import annotations

from pathlib import Path

from interlace.config.config import CONFIG_FILE
from interlace.exceptions import ConfigurationError

_CONFIG = """\
name: {name}
default_dialect: duckdb
# The warehouse: DuckLake (Parquet + SQL catalog) by default. Also accepted:
# a plain .duckdb path, ":memory:", or quack:<host>:<port> for a warehouse
# served by `interlace serve --quack`.
database: ducklake:.interlace/warehouse.ducklake
"""

_RAW_EVENTS = """\
-- A seed model: inline rows, so the project runs with no external source. Other
-- models reference it by name (`raw_events`) and interlace infers the dependency,
-- rewriting it to the physical table at apply time.
SELECT event_id, user_id, kind, CAST(amount AS DOUBLE) AS amount, country, ts
FROM (
    VALUES
        (1,  101, 'view',     0.00,  'US', TIMESTAMP '2026-01-01 09:00:00'),
        (2,  101, 'click',    0.00,  'US', TIMESTAMP '2026-01-01 09:01:00'),
        (3,  101, 'purchase', 49.90, 'US', TIMESTAMP '2026-01-01 09:03:00'),
        (4,  102, 'view',     0.00,  'GB', TIMESTAMP '2026-01-01 10:15:00'),
        (5,  102, 'view',     0.00,  'GB', TIMESTAMP '2026-01-01 10:16:00'),
        (6,  103, 'click',    0.00,  'DE', TIMESTAMP '2026-01-01 11:20:00'),
        (7,  103, 'purchase', 129.00, 'DE', TIMESTAMP '2026-01-01 11:25:00'),
        (8,  104, 'view',     0.00,  'US', TIMESTAMP '2026-01-01 12:05:00'),
        (9,  104, 'click',    0.00,  'US', TIMESTAMP '2026-01-01 12:06:00'),
        (10, 104, 'purchase', 19.99, 'US', TIMESTAMP '2026-01-01 12:09:00'),
        (11, 105, 'view',     0.00,  'GB', TIMESTAMP '2026-01-01 13:30:00'),
        (12, 105, 'purchase', 74.50, 'GB', TIMESTAMP '2026-01-01 13:33:00')
) AS t (event_id, user_id, kind, amount, country, ts)
"""

_ENRICHED_EVENTS = '''\
"""A Python model in the middle of the pipeline.

Its parameter is named after the `raw_events` model, so interlace infers the
dependency — no `depends_on` needed. Data arrives and leaves as Arrow (never
pandas), a batch at a time, so memory stays bounded however large the source grows.
Here it derives a `revenue` column (the amount on a purchase, else 0) and an
`is_conversion` flag — the kind of row-wise logic that is clumsy in SQL.
"""

import pyarrow as pa
import pyarrow.compute as pc

from interlace import model


@model()  # materialise: virtual (default), strategy: replace (default)
def enriched_events(raw_events):
    for batch in raw_events.reader():
        is_conversion = pc.equal(batch.column("kind"), "purchase")
        revenue = pc.if_else(is_conversion, batch.column("amount"), pa.scalar(0.0))
        columns = [*batch.columns, revenue, is_conversion]
        names = [*batch.schema.names, "revenue", "is_conversion"]
        yield pa.RecordBatch.from_arrays(columns, names=names)
'''

_EVENT_SUMMARY = """\
/* interlace:
  checks:            # data-quality gates: an error-severity failure blocks promotion
    - not_null: country
    - row_count: {min: 1}
*/
-- Reads the Python model's output (`enriched_events`) — referenced by name, like
-- any model. Per-country conversions and revenue.
SELECT
    country,
    count(*) AS events,
    count(*) FILTER (WHERE is_conversion) AS conversions,
    round(sum(revenue), 2) AS revenue
FROM enriched_events
GROUP BY country
ORDER BY revenue DESC
"""

_README = """\
# {name}

An interlace project — a small **SQL → Python → SQL** pipeline that runs with no
external data:

```
raw_events (SQL seed, 12 rows)
  └─ enriched_events (Python: adds revenue + is_conversion, over Arrow)
       └─ event_summary (SQL: per-country conversions & revenue, with checks)
```

```bash
interlace plan               # preview changes (prod: unprefixed views)
interlace apply              # build models and promote production
interlace apply --env dev    # or a prefixed dev sandbox (dev__main.*)
```

The Python model's parameter is named after `raw_events`, so the dependency is
inferred — no `depends_on` required. Add `depends_on` only for names a parameter
can't spell (a schema-qualified upstream like `raw.accounts`, or a non-model source).
"""


def scaffold_project(root: Path, name: str | None = None) -> list[Path]:
    """Create a starter project under ``root``. Refuses to overwrite an existing one."""
    root = Path(root)
    project_name = name or root.resolve().name
    config = root / CONFIG_FILE
    if config.exists():
        raise ConfigurationError("project already initialised", details={"path": str(config)})

    models = root / "models"
    models.mkdir(parents=True, exist_ok=True)

    files = {
        config: _CONFIG.format(name=project_name),
        models / "raw_events.sql": _RAW_EVENTS,
        models / "enriched_events.py": _ENRICHED_EVENTS,
        models / "event_summary.sql": _EVENT_SUMMARY,
        root / "README.md": _README.format(name=project_name),
    }
    for path, content in files.items():
        path.write_text(content)
    return list(files)
