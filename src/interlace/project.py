"""A loaded project: config + discovered models, with engine/state factories.

This is the entry point the CLI builds on — ``Project.load(dir)`` reads the
config, discovers models, and can compile them and open the warehouse engine and
control-plane state store at the configured (root-relative) paths.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from interlace.config.config import CONFIG_FILE, ProjectConfig, load_config
from interlace.dsl.decorators import ModelDef
from interlace.dsl.discovery import discover_models
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import CompiledProject, compile_models
from interlace.state.store import SqliteStateStore


@dataclass
class Project:
    root: Path
    config: ProjectConfig
    models: list[ModelDef]

    @classmethod
    def load(cls, root: Path | str) -> Project:
        root = Path(root)
        config = load_config(root / CONFIG_FILE)
        models = discover_models(root, config.model_paths, config.default_dialect)
        return cls(root=root, config=config, models=models)

    def compile(self) -> CompiledProject:
        return compile_models(self.models, default_dialect=self.config.default_dialect)

    def open_engine(self) -> DuckDBAdapter:
        """Open the warehouse: DuckLake (default), a plain DuckDB file, ":memory:",
        or a remote warehouse served over the quack protocol."""
        database = self.config.database
        if database.startswith("quack:"):
            from interlace.engines.quack import QuackAdapter  # lazy: only quack clients need it

            token = self.config.quack_token or os.environ.get("INTERLACE_QUACK_TOKEN")
            return QuackAdapter.connect(database, token=token)
        if database.startswith("ducklake:"):
            catalog = database.removeprefix("ducklake:")
            if not Path(catalog).is_absolute():
                resolved = self.root / catalog
                resolved.parent.mkdir(parents=True, exist_ok=True)
                database = f"ducklake:{resolved}"
        elif database != ":memory:":
            path = self.root / database
            path.parent.mkdir(parents=True, exist_ok=True)
            database = str(path)
        return DuckDBAdapter.connect(database)

    async def open_state(self) -> SqliteStateStore:
        path = self.root / self.config.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return await SqliteStateStore.open(path)
