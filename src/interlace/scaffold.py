"""Scaffold a new interlace project (used by ``interlace init``).

Writes a minimal, immediately-runnable project: a config file and two SQL models
forming a small dependency chain that needs no external data source.
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
-- A seed model: inline rows, so the project runs with no external source.
SELECT *
FROM (
    VALUES
        (1, 'click', 100),
        (2, 'view', 50),
        (3, 'click', 75)
) AS t (event_id, kind, amount)
"""

_EVENT_TOTALS = """\
-- Aggregates raw_events. Reference upstreams by model name; interlace resolves
-- the dependency and rewrites it to the physical table at apply time.
SELECT
    kind,
    count(*) AS events,
    sum(amount) AS total_amount
FROM raw_events
GROUP BY kind
"""

_README = """\
# {name}

An interlace project.

```bash
interlace plan --env dev     # preview changes
interlace apply --env dev    # build models and promote the environment
```
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
        models / "event_totals.sql": _EVENT_TOTALS,
        root / "README.md": _README.format(name=project_name),
    }
    for path, content in files.items():
        path.write_text(content)
    return list(files)
