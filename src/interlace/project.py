"""A loaded project: config + discovered models, with engine/state factories.

This is the entry point the CLI builds on — ``Project.load(dir)`` reads the
config, discovers models, and can compile them and open the warehouse engine and
control-plane state store at the configured (root-relative) paths.
"""

from __future__ import annotations

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
        database = self.config.database
        if database != ":memory:":
            path = self.root / database
            path.parent.mkdir(parents=True, exist_ok=True)
            database = str(path)
        return DuckDBAdapter.connect(database)

    async def open_state(self) -> SqliteStateStore:
        path = self.root / self.config.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return await SqliteStateStore.open(path)
