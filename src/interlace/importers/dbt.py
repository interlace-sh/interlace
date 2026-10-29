"""Convert a dbt project into an interlace project.

``ref`` and ``source`` become table names, seeds become models that read their
CSV, schema tests become checks, and snapshots become ``scd``. Jinja that is
actually a loop over a list, or a macro whose body is a SQL fragment, is
expanded. Anything else is copied aside and listed in ``IMPORT.md`` — a model
is never half-converted into SQL that merely looks right.

Materialisations: dbt ``view`` stays a view, ``table`` becomes ``virtual`` (the
managed table behind an environment view — ``materialise: table`` is an external
delivery, which is not what dbt means), ``ephemeral`` stays ephemeral. An
incremental model without a time column is imported as a full replace and named
in the report, because interlace windows incrementals by ``time_column``.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from interlace.checks.spec import parse_checks
from interlace.dsl.decorators import ModelDef
from interlace.dsl.dynamic import render_model_sql
from interlace.exceptions import ConfigurationError, DefinitionError
from interlace.importers.jinja import Macro, UnsupportedJinja, parse_macros, render_template

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_REF = re.compile(r"""ref\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")
_SOURCE = re.compile(r"""source\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*,\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")
_DOC = re.compile(r"\{\{\s*doc\([^)]*\)\s*\}\}")
_SKIP_DIRS = frozenset({"target", "dbt_packages", "dbt_modules", "logs", ".git", "analysis", "analyses"})

# dbt materialized → (interlace materialise, strategy). ``table`` is virtual on
# purpose: dbt's table is a managed relation, not an external delivery.
_MATERIAL: dict[str, tuple[str, str]] = {
    "view": ("view", "replace"),
    "table": ("virtual", "replace"),
    "ephemeral": ("ephemeral", "replace"),
    "incremental": ("virtual", "incremental"),
}
_CONFIG_KEYS = frozenset(
    {"materialized", "schema", "tags", "enabled", "unique_key", "alias", "incremental_strategy", "updated_at"}
)


@dataclass(frozen=True)
class Note:
    """One thing the import did not do, or did with a caveat."""

    kind: str  # skipped | warning
    name: str
    detail: str


@dataclass
class ImportReport:
    """What ``import_dbt`` wrote and what it refused."""

    project: str
    output: Path
    models: list[str] = field(default_factory=list)
    seeds: list[str] = field(default_factory=list)
    checks: int = 0
    notes: list[Note] = field(default_factory=list)

    @property
    def skipped(self) -> list[Note]:
        return [note for note in self.notes if note.kind == "skipped"]

    def markdown(self) -> str:
        lines = [
            f"# Imported from dbt project `{self.project}`",
            "",
            f"{len(self.models)} models, {len(self.seeds)} seeds, {self.checks} checks.",
            "",
        ]
        if not self.notes:
            lines.append("Everything this importer understands converted.")
            return "\n".join(lines) + "\n"
        lines.append("## Needs a look")
        lines.append("")
        for note in self.notes:
            lines.append(f"- **{note.kind}** `{note.name}` — {note.detail}")
        lines.append("")
        return "\n".join(lines)


def import_dbt(source: Path, output: Path, *, force: bool = False) -> ImportReport:  # noqa: C901
    """Write an interlace project for the dbt project at ``source`` into ``output``."""
    source = source.resolve()
    output = output.resolve()
    project_file = source / "dbt_project.yml"
    if not project_file.is_file():
        raise ConfigurationError(
            f"no dbt_project.yml in {source}",
            details={"path": str(source)},
        )
    if output == source or source in output.parents:
        raise ConfigurationError(
            "the output directory must not be the dbt project or a folder inside it",
            details={"path": str(output)},
        )
    if output.exists() and any(output.iterdir()) and not force:
        raise ConfigurationError(
            f"{output} is not empty — pass --force to write into it",
            details={"path": str(output)},
        )

    project = _load_yaml(project_file) or {}
    if not isinstance(project, dict) or not project.get("name"):
        raise ConfigurationError("dbt_project.yml has no name", details={"path": str(project_file)})
    name = str(project["name"])
    report = ImportReport(project=name, output=output)
    paths = _paths(project)
    macros = _load_macros(source, paths["macro"])
    properties = _load_properties(source, paths, report)
    seeds = _find_seeds(source, paths["seed"])
    _note_packages(source, report)
    if properties.semantic:
        report.notes.append(
            Note(
                "warning", "semantic models", "MetricFlow semantic models have no interlace equivalent and were skipped"
            )
        )

    output.mkdir(parents=True, exist_ok=True)
    written: set[str] = set()
    for seed_name, seed_path in sorted(seeds.items()):
        if seed_name in written:
            continue
        copied = _copy_seed(source, output, seed_path)
        _write_model(
            output,
            Path("models") / "seeds" / f"{seed_name}.sql",
            _definition(
                seed_name,
                f"SELECT * FROM read_csv_auto('{_sql_path(copied)}')",
                materialise="virtual",
                description=properties.descriptions.get(seed_name),
                checks=properties.checks.get(seed_name, []),
            ),
        )
        written.add(seed_name)
        report.seeds.append(seed_name)
        report.checks += len(properties.checks.get(seed_name, []))

    for sql_file in _sql_files(source, paths["model"]):
        model_name = sql_file.stem
        if not _IDENT.fullmatch(model_name):
            report.notes.append(Note("skipped", model_name, "the file name is not an identifier"))
            continue
        if model_name in written:
            report.notes.append(
                Note("skipped", str(sql_file.relative_to(source)), f"duplicate model name {model_name}")
            )
            continue
        relative = _under(source, sql_file, paths["model"])
        folder = _folder_config(project.get("models") or {}, name, relative)
        _convert_sql(
            source,
            output,
            sql_file,
            model_name,
            folder,
            macros,
            properties,
            report,
            written,
            snapshot=False,
            target=Path("models") / relative,
        )

    for sql_file in _sql_files(source, paths["snapshot"]):
        text = sql_file.read_text()
        match = re.search(
            r"\{%-?\s*snapshot\s+([A-Za-z_][A-Za-z0-9_]*)\s*-?%\}(.*?)\{%-?\s*endsnapshot", text, re.DOTALL
        )
        if match is None:
            report.notes.append(Note("skipped", str(sql_file.relative_to(source)), "no {% snapshot %} block"))
            continue
        model_name, body = match.group(1), match.group(2)
        if model_name in written:
            report.notes.append(Note("skipped", model_name, "duplicate model name"))
            continue
        _convert_sql(
            source,
            output,
            sql_file,
            model_name,
            {},
            macros,
            properties,
            report,
            written,
            snapshot=True,
            text=body,
            target=Path("models") / "snapshots" / f"{model_name}.sql",
        )

    for source_name, table_name, relation in properties.sources:
        if table_name in written:
            if table_name in seeds:
                report.notes.append(
                    Note("warning", f"{source_name}.{table_name}", "source() reads the seed of the same name")
                )
            continue
        if not _IDENT.fullmatch(table_name):
            report.notes.append(Note("skipped", f"{source_name}.{table_name}", "the table name is not an identifier"))
            continue
        _write_model(
            output,
            Path("models") / "sources" / f"{table_name}.sql",
            _definition(
                table_name,
                f"SELECT * FROM {relation}",
                description=properties.descriptions.get(table_name)
                or f"dbt source {source_name}.{table_name}. Point this at the warehouse table or a file.",
                checks=properties.checks.get(table_name, []),
            ),
        )
        written.add(table_name)
        report.models.append(table_name)
        report.checks += len(properties.checks.get(table_name, []))
        report.notes.append(
            Note(
                "warning",
                f"{source_name}.{table_name}",
                f"imported as SELECT * FROM {relation} — there is no seed with this name",
            )
        )

    for test_file in _sql_files(source, paths["test"]):
        report.notes.append(
            Note(
                "skipped",
                str(test_file.relative_to(source)),
                "singular tests are not converted; rewrite as a sql check",
            )
        )

    (output / "interlace.yaml").write_text(
        yaml.safe_dump(
            {"name": name, "default_dialect": "duckdb"},
            sort_keys=False,
        )
    )
    (output / "IMPORT.md").write_text(report.markdown())
    return report


@dataclass
class _Properties:
    checks: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    descriptions: dict[str, str] = field(default_factory=dict)
    sources: list[tuple[str, str, str]] = field(default_factory=list)  # source, table, sql relation
    semantic: bool = False


def _convert_sql(
    source: Path,
    output: Path,
    sql_file: Path,
    model_name: str,
    folder: dict[str, Any],
    macros: dict[str, Macro],
    properties: _Properties,
    report: ImportReport,
    written: set[str],
    *,
    snapshot: bool,
    text: str | None = None,
    target: Path | None = None,
) -> None:
    raw = text if text is not None else sql_file.read_text()
    try:
        rendered = render_template(raw, macros=macros, model_name=model_name)
    except UnsupportedJinja as exc:
        _keep_original(output, source, sql_file, raw)
        report.notes.append(Note("skipped", model_name, exc.message))
        return
    for warning in rendered.warnings:
        report.notes.append(Note("warning", model_name, warning))
    inline = dict(rendered.config)
    if "enabled" in folder and not _enabled(folder.get("enabled")):
        report.notes.append(Note("skipped", model_name, "disabled in dbt_project.yml"))
        return
    if "enabled" in inline and not _enabled(inline.get("enabled")):
        report.notes.append(Note("skipped", model_name, "disabled in config()"))
        return
    alias = inline.get("alias")
    name = str(alias) if isinstance(alias, str) and _IDENT.fullmatch(alias) else model_name
    if name != model_name:
        report.notes.append(Note("warning", model_name, f"alias {name} is the model name"))
    if name in written:
        report.notes.append(Note("skipped", name, "duplicate model name"))
        return
    materialise, strategy, key, time_column, caveats = _materialisation(folder, inline, snapshot)
    if strategy == "scd" and not key:
        report.notes.append(Note("skipped", name, "snapshot has no unique_key, and scd requires one"))
        return
    for caveat in caveats:
        report.notes.append(Note("warning", name, caveat))
    checks = properties.checks.get(model_name, [])
    try:
        definition = _definition(
            name,
            rendered.sql.strip(),
            materialise=materialise,
            strategy=strategy,
            key=key,
            time_column=time_column,
            tags=_tags(folder, inline),
            description=properties.descriptions.get(model_name),
            checks=checks,
        )
    except DefinitionError as exc:
        report.notes.append(Note("skipped", name, exc.message))
        return
    # ``target`` keeps the dbt layout. The header name is the dbt name, not the path.
    _write_model(output, target or Path("models") / sql_file.name, definition)
    written.add(name)
    report.models.append(name)
    report.checks += len(checks)


def _materialisation(
    folder: dict[str, Any], inline: dict[str, Any], snapshot: bool
) -> tuple[str, str, tuple[str, ...], str | None, list[str]]:
    caveats: list[str] = []
    if snapshot:
        key = _key(inline.get("unique_key"))
        updated = inline.get("updated_at")
        time_column = str(updated) if isinstance(updated, str) and _IDENT.fullmatch(updated) else None
        kind = str(inline.get("strategy") or "check")
        if kind not in {"timestamp", "check"}:
            caveats.append(f"snapshot strategy {kind} was imported as scd")
        if kind == "check":
            caveats.append("a dbt check snapshot compares a hash; scd compares the whole row")
        return "virtual", "scd", key, time_column, caveats

    raw = inline.get("materialized", folder.get("materialized", "view"))
    materialized = str(raw)
    materialise, strategy = _MATERIAL.get(materialized, ("virtual", "replace"))
    if materialized not in _MATERIAL:
        caveats.append(f"materialized {materialized!r} was imported as a virtual replace")
    key = _key(inline.get("unique_key"))
    if strategy == "incremental":
        # interlace incremental reads one time window. dbt expresses that window as
        # a Jinja branch, which does not name the column, so this stays a full replace.
        materialise, strategy = "virtual", "replace"
        caveats.append(
            "dbt incremental was imported as a full replace. Set strategy: incremental "
            "with interval and time_column once the window column is known"
        )
        key = ()
    elif key and strategy == "replace":
        key = ()  # a dbt table can carry unique_key without upserting
    return materialise, strategy, key, None, caveats


def _definition(
    name: str,
    sql: str,
    *,
    materialise: str = "virtual",
    strategy: str = "replace",
    key: tuple[str, ...] = (),
    time_column: str | None = None,
    tags: tuple[str, ...] = (),
    description: str | None = None,
    checks: list[dict[str, Any]] | None = None,
) -> ModelDef:
    return ModelDef(
        name=name,
        sql=sql.strip() + "\n",
        materialise=materialise,
        strategy=strategy,
        key=key,
        time_column=time_column,
        tags=tags,
        description=description,
        checks=parse_checks(checks or [], name),
    )


def _write_model(output: Path, relative: Path, definition: ModelDef) -> None:
    target = output / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_model_sql(definition))


def _keep_original(output: Path, source: Path, sql_file: Path, text: str) -> None:
    target = output / "unconverted" / sql_file.relative_to(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)


def _load_properties(source: Path, paths: dict[str, list[str]], report: ImportReport) -> _Properties:  # noqa: C901
    props = _Properties()
    roots = [*(source / relative for relative in paths["model"]), *(source / relative for relative in paths["seed"])]
    roots += [source / relative for relative in paths["snapshot"]]
    for root in roots:
        if not root.is_dir():
            continue
        for yml in sorted(root.rglob("*.yml")) + sorted(root.rglob("*.yaml")):
            if any(part in _SKIP_DIRS for part in yml.parts):
                continue
            document = _load_yaml(yml)
            if not isinstance(document, dict):
                if document is not None:
                    report.notes.append(
                        Note("warning", str(yml.relative_to(source)), "YAML is not a mapping; tests were skipped")
                    )
                continue
            if "semantic_models" in document or _has_semantic(document):
                props.semantic = True
            if "unit_tests" in document:
                report.notes.append(
                    Note(
                        "warning",
                        str(yml.relative_to(source)),
                        "dbt unit tests have no interlace equivalent and were skipped",
                    )
                )
            for model in (
                _as_list(document.get("models")) + _as_list(document.get("seeds")) + _as_list(document.get("snapshots"))
            ):
                _take_model(props, model, report, str(yml.relative_to(source)))
            for entry in _as_list(document.get("sources")):
                _take_source(props, entry, report, str(yml.relative_to(source)))
    return props


def _take_model(props: _Properties, model: Any, report: ImportReport, origin: str) -> None:
    if not isinstance(model, dict) or not model.get("name"):
        return
    name = str(model["name"])
    description = _description(model.get("description"))
    if description:
        props.descriptions[name] = description
    checks, notes = _tests_for(model)
    props.checks.setdefault(name, []).extend(checks)
    for detail in notes:
        report.notes.append(Note("warning", f"{origin}:{name}", detail))


def _take_source(props: _Properties, entry: Any, report: ImportReport, origin: str) -> None:
    if not isinstance(entry, dict) or not entry.get("name"):
        return
    source_name = str(entry["name"])
    schema = entry.get("schema") or source_name
    database = entry.get("database")
    for table in _as_list(entry.get("tables")):
        if not isinstance(table, dict) or not table.get("name"):
            continue
        table_name = str(table["name"])
        identifier = str(table.get("identifier") or table_name)
        relation = _relation(database, schema, identifier)
        props.sources.append((source_name, table_name, relation))
        description = _description(table.get("description"))
        if description:
            props.descriptions[table_name] = description
        checks, notes = _tests_for(table)
        props.checks.setdefault(table_name, []).extend(checks)
        for detail in notes:
            report.notes.append(Note("warning", f"{origin}:{source_name}.{table_name}", detail))


def _tests_for(node: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    checks: list[dict[str, Any]] = []
    notes: list[str] = []
    for column in _as_list(node.get("columns")):
        if not isinstance(column, dict) or not column.get("name"):
            continue
        column_name = str(column["name"])
        for entry in _test_entries(column):
            check, note = _one_test(entry, column_name)
            if check is not None:
                checks.append(check)
            if note:
                notes.append(note)
    for entry in _test_entries(node):
        check, note = _one_test(entry, None)
        if check is not None:
            checks.append(check)
        if note:
            notes.append(note)
    return checks, notes


def _one_test(entry: Any, column: str | None) -> tuple[dict[str, Any] | None, str | None]:  # noqa: C901
    if isinstance(entry, str):
        kind, cfg = entry, {}
    elif isinstance(entry, dict) and len(entry) == 1:
        kind, raw = next(iter(entry.items()))
        cfg = dict(raw) if isinstance(raw, dict) else {}
    else:
        return None, f"unrecognised test {entry!r}"
    arguments = cfg.get("arguments")
    if isinstance(arguments, dict):
        cfg = {**arguments, **{key: value for key, value in cfg.items() if key != "arguments"}}
    severity = _severity(cfg)
    short = str(kind).split(".")[-1]
    raw_config = cfg.get("config")
    config: dict[str, Any] = raw_config if isinstance(raw_config, dict) else {}
    where = cfg.get("where") or config.get("where")
    note = f"{kind} was imported without its where filter ({where})" if where else None
    if short in {"not_null", "unique"}:
        if column is None and short == "not_null":
            return None, f"{kind} needs a column"
        columns = _combination(cfg) if short == "unique" and column is None else (column,)
        if not columns or columns == (None,):
            return None, f"{kind} needs a column"
        return _check(short, columns, severity), note
    if short == "unique_combination_of_columns":
        columns = _combination(cfg)
        if not columns:
            return None, f"{kind} needs combination_of_columns"
        return _check("unique", columns, severity), note
    if short == "accepted_values":
        values = cfg.get("values")
        if column is None or not isinstance(values, list):
            return None, f"{kind} needs a column and values"
        return _check("accepted_values", (column,), severity, values=values), note
    if short == "accepted_range":
        if column is None:
            return None, f"{kind} needs a column"
        params: dict[str, Any] = {}
        if "min_value" in cfg:
            params["min"] = cfg["min_value"]
        if "max_value" in cfg:
            params["max"] = cfg["max_value"]
        return _check("range", (column,), severity, **params), note
    if short == "relationships":
        target = _relation_ref(cfg.get("to"))
        field = cfg.get("field")
        if column is None or target is None or not isinstance(field, str):
            return None, f"{kind} needs column, to and field"
        return _check("relationships", (column,), severity, **{"to": target, "field": field}), note
    if short == "expression_is_true":
        expression = cfg.get("expression")
        if not isinstance(expression, str) or not expression.strip():
            return None, f"{kind} needs an expression"
        return _check("expression", (), severity, expression=expression.strip()), note
    if short == "not_null_proportion":
        if column and cfg.get("at_least") == 1:
            return _check("not_null", (column,), severity), note
        return None, f"{kind} is not a built-in check"
    return None, f"{kind} is not a built-in check"


def _check(kind: str, columns: tuple[Any, ...], severity: str, **params: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {"type": kind}
    names = tuple(str(column) for column in columns if column)
    if len(names) == 1:
        entry["column"] = names[0]
    elif names:
        entry["columns"] = list(names)
    if severity != "error":
        entry["severity"] = severity
    entry.update(params)
    return entry


def _relation_ref(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    ref = _REF.search(value)
    if ref:
        return ref.group(1)
    source = _SOURCE.search(value)
    if source:
        return source.group(2)
    if _IDENT.fullmatch(value):
        return value
    return None


def _combination(cfg: dict[str, Any]) -> tuple[str, ...]:
    raw = cfg.get("combination_of_columns") or cfg.get("column_names") or ()
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, list):
        return tuple(str(item) for item in raw)
    return ()


def _severity(cfg: dict[str, Any]) -> str:
    raw_config = cfg.get("config")
    config: dict[str, Any] = raw_config if isinstance(raw_config, dict) else {}
    raw = cfg.get("severity", config.get("severity", "error"))
    text = str(raw)
    if text == "warning":
        return "warn"
    return text if text in {"error", "warn", "info"} else "error"


def _test_entries(node: dict[str, Any]) -> list[Any]:
    raw = node.get("data_tests", node.get("tests", []))
    return raw if isinstance(raw, list) else []


def _folder_config(models: Any, project: str, relative: Path) -> dict[str, Any]:
    """dbt's folder config, child keys overriding parents. ``+materialized`` counts."""
    if not isinstance(models, dict):
        return {}
    node: Any = models.get(project, models)
    collected: dict[str, Any] = {}
    _absorb(collected, node)
    if not isinstance(node, dict):
        return collected
    for part in relative.parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            break
        node = child
        _absorb(collected, node)
    return collected


def _absorb(dest: dict[str, Any], node: Any) -> None:
    if not isinstance(node, dict):
        return
    for key, value in node.items():
        name = key[1:] if str(key).startswith("+") else str(key)
        if name in _CONFIG_KEYS and not isinstance(value, dict):
            dest[name] = value


def _paths(project: dict[str, Any]) -> dict[str, list[str]]:
    def pick(*keys: str, default: str) -> list[str]:
        for key in keys:
            value = project.get(key)
            if isinstance(value, list) and value:
                return [str(item) for item in value]
            if isinstance(value, str) and value:
                return [value]
        return [default]

    return {
        "model": pick("model-paths", "source-paths", default="models"),
        "seed": pick("seed-paths", "data-paths", default="seeds"),
        "snapshot": pick("snapshot-paths", default="snapshots"),
        "test": pick("test-paths", default="tests"),
        "macro": pick("macro-paths", default="macros"),
    }


def _load_macros(source: Path, relatives: list[str]) -> dict[str, Macro]:
    macros: dict[str, Macro] = {}
    for sql_file in _sql_files(source, relatives):
        try:
            macros.update(parse_macros(sql_file.read_text()))
        except UnsupportedJinja:
            continue
    return macros


def _find_seeds(source: Path, relatives: list[str]) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for root in relatives:
        base = source / root
        if not base.is_dir():
            continue
        for csv in sorted(base.rglob("*.csv")):
            if csv.stem not in found and _IDENT.fullmatch(csv.stem):
                found[csv.stem] = csv
    return found


def _copy_seed(source: Path, output: Path, csv: Path) -> str:
    relative = csv.relative_to(source).as_posix()
    target = output / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(csv, target)
    return relative


def _sql_files(source: Path, relatives: list[str]) -> list[Path]:
    files: list[Path] = []
    for root in relatives:
        base = source / root
        if not base.is_dir():
            continue
        for sql in sorted(base.rglob("*.sql")):
            if any(part in _SKIP_DIRS for part in sql.relative_to(source).parts):
                continue
            files.append(sql)
    return files


def _under(source: Path, sql_file: Path, relatives: list[str]) -> Path:
    """``sql_file`` relative to the configured directory that contains it."""
    for relative in relatives:
        root = source / relative
        if sql_file.is_relative_to(root):
            return sql_file.relative_to(root)
    return Path(sql_file.name)


def _key(value: Any) -> tuple[str, ...]:
    if isinstance(value, str) and _IDENT.fullmatch(value):
        return (value,)
    if isinstance(value, list) and all(isinstance(item, str) and _IDENT.fullmatch(item) for item in value):
        return tuple(value)
    return ()


def _tags(folder: dict[str, Any], inline: dict[str, Any]) -> tuple[str, ...]:
    raw = inline.get("tags", folder.get("tags", ()))
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, list):
        return tuple(str(item) for item in raw)
    return ()


def _enabled(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"false", "0", "no"}
    return bool(value)


def _description(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _DOC.sub("", value).strip()
    return text or None


def _relation(database: Any, schema: Any, identifier: str) -> str:
    parts = [str(part) for part in (database, schema, identifier) if part]
    if not all(_IDENT.fullmatch(part) for part in parts):
        return identifier if _IDENT.fullmatch(identifier) else "source_table"
    return ".".join(parts)


def _sql_path(relative: str) -> str:
    return relative.replace("'", "''")


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _has_semantic(document: dict[str, Any]) -> bool:
    for model in _as_list(document.get("models")):
        if isinstance(model, dict) and "semantic_model" in model:
            return True
    return False


def _note_packages(source: Path, report: ImportReport) -> None:
    packages = source / "packages.yml"
    if not packages.is_file():
        return
    document = _load_yaml(packages)
    names: list[str] = []
    if isinstance(document, dict):
        for entry in _as_list(document.get("packages")):
            if isinstance(entry, dict):
                names.append(str(entry.get("package") or entry.get("git") or "package"))
    if names:
        report.notes.append(
            Note(
                "warning",
                "packages",
                "not installed (" + ", ".join(names) + "). Built-in rewrites cover "
                "dbt_utils.surrogate_key, date_spine and expression_is_true; other package calls are skipped per model",
            )
        )


def _load_yaml(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text())
    except yaml.YAMLError:
        return None


# Re-exported so a caller can render a template the same way the importer does.
__all__ = ["ImportReport", "Note", "import_dbt"]
