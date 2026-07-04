"""Discover models in a project directory.

Walks the configured model paths: ``.sql`` files become SQL models named by
their path (``models/silver/orders.sql`` -> ``silver.orders``); ``.py`` files are
imported so their ``@model`` decorators register (Python models name themselves
via the decorator or function name). Both populate the global registry, which is
cleared first for a clean load.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from interlace.checks.spec import parse_checks
from interlace.dsl.decorators import _KINDS, _MATERIALISATIONS, REGISTRY, ModelDef, _as_columns, _as_export, _as_tuple
from interlace.dsl.sql_config import extract_sql_config
from interlace.exceptions import DefinitionError


def discover_models(root: Path, model_paths: list[str], default_dialect: str) -> list[ModelDef]:
    REGISTRY.clear()
    for relative in model_paths:
        base = root / relative
        if not base.is_dir():
            continue
        for sql_file in sorted(base.rglob("*.sql")):
            config, sql = extract_sql_config(sql_file.read_text())
            REGISTRY.register_model(_sql_model(_model_name(base, sql_file), sql, config, default_dialect))
        for py_file in sorted(base.rglob("*.py")):
            if py_file.name.startswith("_"):
                continue
            _import_module(base, py_file)
    return list(REGISTRY.models.values())


def _sql_model(default_name: str, sql: str, config: dict[str, Any], default_dialect: str) -> ModelDef:
    materialise = config.get("materialise", "table")
    if materialise not in _MATERIALISATIONS:
        raise DefinitionError(f"unknown materialise {materialise!r}", details={"model": default_name})
    kind = config.get("kind", "batch")
    if kind not in _KINDS:
        raise DefinitionError(f"unknown kind {kind!r}", details={"model": default_name})
    return ModelDef(
        name=config.get("name", default_name),
        sql=sql,
        materialise=materialise,
        strategy=config.get("strategy", "full"),
        key=_as_tuple(config.get("key") or ()),
        dialect=config.get("dialect") or default_dialect,
        depends_on=_as_tuple(config.get("depends_on") or ()),
        kind=kind,
        interval=config.get("interval"),
        time_column=config.get("time_column"),
        tags=_as_tuple(config.get("tags") or ()),
        owner=config.get("owner"),
        description=config.get("description"),
        columns=_as_columns(config.get("columns")),
        export=_as_export(config.get("export")),
        schedule=config.get("schedule"),
        checks=parse_checks(config.get("checks"), default_name),
    )


def _model_name(base: Path, file: Path) -> str:
    return ".".join(file.relative_to(base).with_suffix("").parts)


def _import_module(base: Path, file: Path) -> None:
    module_name = "interlace_model_" + "_".join(file.relative_to(base).with_suffix("").parts)
    spec = importlib.util.spec_from_file_location(module_name, file)
    if spec is None or spec.loader is None:
        raise DefinitionError("could not import model module", details={"path": str(file)})
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
