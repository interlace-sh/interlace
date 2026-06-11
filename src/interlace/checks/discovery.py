"""
SQL check file discovery.

Discovers .sql check files from the project's checks/ directory
and converts them into SqlCheck instances.
"""

import re
from pathlib import Path
from typing import Any

from interlace.checks.base import CheckSeverity
from interlace.checks.decorator import PythonCheck, get_registered_checks
from interlace.checks.types.sql import SqlCheck
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.discovery")


def _parse_check_annotations(content: str) -> dict[str, str]:
    """
    Parse check annotations from SQL comments.

    Format: ``-- @key: value``

    Returns:
        Dictionary of annotation key-value pairs
    """
    annotations: dict[str, str] = {}
    pattern = r"--\s*@(\w+):\s*(.+)"
    for match in re.finditer(pattern, content):
        key = match.group(1).strip()
        value = match.group(2).strip()
        annotations[key] = value
    return annotations


def _extract_sql_body(content: str) -> str:
    """
    Extract the SQL body from a check file, stripping annotation comments.

    Returns the SQL query with annotation lines removed.
    """
    lines = content.split("\n")
    sql_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip annotation lines (-- @key: value)
        if re.match(r"--\s*@\w+:", stripped):
            continue
        sql_lines.append(line)

    return "\n".join(sql_lines).strip()


def discover_sql_checks(project_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """
    Discover SQL check files from the project's checks/ directory.

    Scans ``{project_dir}/checks/*.sql`` and ``{project_dir}/checks/**/*.sql``
    for check definitions.

    Args:
        project_dir: Root project directory

    Returns:
        Dictionary mapping model names to lists of check config dicts,
        where each dict contains a ``_check_instance`` key with a SqlCheck.
    """
    checks_dir = project_dir / "checks"
    if not checks_dir.is_dir():
        return {}

    checks_by_model: dict[str, list[dict[str, Any]]] = {}

    for sql_file in sorted(checks_dir.rglob("*.sql")):
        try:
            content = sql_file.read_text()
        except OSError as e:
            logger.warning(f"Could not read check file {sql_file}: {e}")
            continue

        annotations = _parse_check_annotations(content)
        sql_body = _extract_sql_body(content)

        if not sql_body:
            logger.warning(f"Check file {sql_file} has no SQL body, skipping")
            continue

        check_name = annotations.get("name")
        model_name = annotations.get("model")

        if not check_name:
            logger.warning(f"Check file {sql_file} missing @name annotation, skipping")
            continue
        if not model_name:
            logger.warning(f"Check file {sql_file} missing @model annotation, skipping")
            continue

        severity_str = annotations.get("severity", "error").lower()
        severity_map = {
            "error": CheckSeverity.ERROR,
            "warn": CheckSeverity.WARN,
            "warning": CheckSeverity.WARN,
            "info": CheckSeverity.INFO,
        }
        severity = severity_map.get(severity_str, CheckSeverity.ERROR)

        check_instance = SqlCheck(
            sql=sql_body,
            name=check_name,
            severity=severity,
            description=annotations.get("description"),
        )

        checks_by_model.setdefault(model_name, []).append({"_check_instance": check_instance})

        logger.debug(f"Discovered SQL check '{check_name}' for model '{model_name}' from {sql_file}")

    return checks_by_model


def discover_python_checks() -> dict[str, list[dict[str, Any]]]:
    """
    Collect registered @check-decorated functions into per-model check lists.

    Returns:
        Dictionary mapping model names to lists of check config dicts,
        where each dict contains a ``_check_instance`` key with a PythonCheck.
    """
    checks_by_model: dict[str, list[dict[str, Any]]] = {}

    severity_map = {
        "error": CheckSeverity.ERROR,
        "warn": CheckSeverity.WARN,
        "warning": CheckSeverity.WARN,
        "info": CheckSeverity.INFO,
    }

    for metadata in get_registered_checks():
        severity = severity_map.get(metadata["severity"], CheckSeverity.ERROR)

        check_instance = PythonCheck(
            func=metadata["function"],
            name=metadata["name"],
            severity=severity,
            description=metadata.get("description"),
        )

        for model_name in metadata["model"]:
            checks_by_model.setdefault(model_name, []).append({"_check_instance": check_instance})
            logger.debug(f"Discovered Python check '{metadata['name']}' for model '{model_name}'")

    return checks_by_model
