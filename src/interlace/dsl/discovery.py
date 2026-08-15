"""Discover models in a project directory.

Walks the configured model paths: ``.sql`` files become SQL models named by
their path (``models/silver/orders.sql`` -> ``silver.orders``); ``.py`` files are
imported so their ``@model`` decorators register (Python models name themselves
via the decorator or function name). Both populate the global registry, which is
cleared first for a clean load.
"""

from __future__ import annotations

import contextlib
import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from interlace.checks.spec import parse_checks
from interlace.dsl.decorators import REGISTRY, ModelDef, _as_columns, _as_tuple, validate_materialise
from interlace.dsl.sql_config import extract_sql_config
from interlace.exceptions import DefinitionError, InterlaceError


def discover_models(root: Path, model_paths: list[str], default_dialect: str) -> list[ModelDef]:
    REGISTRY.clear()
    imported = set(sys.modules)
    try:
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
    finally:
        _forget_project_modules(root, imported)
    return list(REGISTRY.models.values())


def _forget_project_modules(root: Path, before: set[str]) -> None:
    """Drop the project's own helper modules from the import cache.

    A helper is imported under whatever plain name it has (``_macros``), so leaving it
    cached would hand the *next* project — a reload under ``interlace serve``, another
    project in the same process — the first one's version of a file with the same name.
    Only modules loaded from inside this project during this pass are dropped."""
    for name in set(sys.modules) - before:
        module = sys.modules.get(name)
        origin = getattr(module, "__file__", None)
        if origin and Path(origin).is_relative_to(root):
            del sys.modules[name]


def _sql_model(default_name: str, sql: str, config: dict[str, Any], default_dialect: str) -> ModelDef:
    name = config.get("name", default_name)
    if "export" in config:
        raise DefinitionError(
            f"model {name!r}: export: was removed in 2.0 — use materialise: table (with target:) for reverse "
            f"ETL, or materialise: file (with path:/format:) for a file",
            details={"model": name},
        )
    materialise = config.get("materialise", "virtual")
    strategy = config.get("strategy", "replace")
    key = _as_tuple(config.get("key") or ())
    target = config.get("target")
    path = config.get("path")
    format = config.get("format")
    validate_materialise(
        name, materialise=materialise, strategy=strategy, target=target, path=path, format=format, key=key
    )
    return ModelDef(
        name=name,
        sql=sql,
        materialise=materialise,
        strategy=strategy,
        key=key,
        dialect=config.get("dialect"),  # None → compile fills from engine dialect
        engine=config.get("engine"),
        depends_on=_as_tuple(config.get("depends_on") or ()),
        interval=config.get("interval"),
        time_column=config.get("time_column"),
        backfill=config.get("backfill", "auto"),  # first-build window for incremental
        tags=_as_tuple(config.get("tags") or ()),
        owner=config.get("owner"),
        description=config.get("description"),
        columns=_as_columns(config.get("columns")),
        target=target,
        path=path,
        format=format,
        environments=_as_tuple(config.get("environments") or ("prod",)),
        schedule=config.get("schedule"),
        checks=parse_checks(config.get("checks"), default_name),
    )


def _model_name(base: Path, file: Path) -> str:
    return ".".join(file.relative_to(base).with_suffix("").parts)


def _relative_to_cwd(file: Path) -> str:
    try:
        return str(file.relative_to(Path.cwd()))
    except ValueError:
        return str(file)


@contextlib.contextmanager
def _importable(directory: Path) -> Iterator[None]:
    """Put ``directory`` at the front of ``sys.path``, and take it back off."""
    entry = str(directory)
    sys.path.insert(0, entry)
    try:
        yield
    finally:
        with contextlib.suppress(ValueError):
            sys.path.remove(entry)


def _import_module(base: Path, file: Path) -> None:
    module_name = "interlace_model_" + "_".join(file.relative_to(base).with_suffix("").parts)
    spec = importlib.util.spec_from_file_location(module_name, file)
    if spec is None or spec.loader is None:
        raise DefinitionError("could not import model module", details={"path": str(file)})
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        # The model's own directory goes on the path for the duration of the import, so a
        # model can `from _shared import ...` a helper sitting next to it — the closest
        # thing to a macro. Files starting with `_` are already skipped as models, which
        # is only useful if they can be imported. Removed again straight after: a project's
        # helpers must not leak into the import path of everything that follows.
        with _importable(file.parent):
            spec.loader.exec_module(module)
    except InterlaceError:
        raise  # a bad @model config is already a clean, actionable error — don't rewrap it
    except Exception as exc:
        # a typo/import error in user model code should read like a user error — one
        # line naming the file — not a dozen frames of interlace's import machinery
        sys.modules.pop(module_name, None)  # don't leave a half-initialised module behind
        raise DefinitionError(
            f"could not load {_relative_to_cwd(file)}: {type(exc).__name__}: {exc}",
            details={"path": str(file)},
        ) from exc
