"""
Checks-only execution mode.

Runs data checks against already-materialised tables without
re-executing or re-materialising models.
"""

from typing import Any

from interlace.connections.manager import get_connection, init_connections
from interlace.utils.logging import get_logger

logger = get_logger("interlace.core.checks_runner")


async def run_checks_only(
    models: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """
    Run checks on existing tables without executing models.

    Args:
        models: Dictionary of model definitions (must have checks configured)
        config: Configuration dictionary

    Returns:
        Dictionary mapping model names to check results
    """
    from interlace.checks.runner import CheckRunner

    # Initialize connections
    init_connections(config)
    default_conn_name = config.get("connections", {}).get("default", {}).get("name", "default")
    connection = get_connection(default_conn_name)

    if connection is None:
        logger.error("No default connection available for checks")
        return {}

    runner = CheckRunner(connection=connection)
    results: dict[str, dict[str, Any]] = {}

    for model_name, model_info in models.items():
        checks = model_info.get("checks")
        if not checks:
            continue

        schema = model_info.get("schema", config.get("models", {}).get("default_schema", "public"))
        materialise = model_info.get("materialise", "table")

        # Skip ephemeral models (no table to check)
        if materialise == "ephemeral":
            continue

        # Use model's connection if specified
        model_conn_name = model_info.get("connection", default_conn_name)
        model_conn = get_connection(model_conn_name) or connection

        logger.info(f"Running checks for '{model_name}'...")
        try:
            summary = runner.run_model_checks(
                model_name=model_name,
                model_info=model_info,
                schema=schema,
                connection=model_conn,
            )
            if summary is None:
                continue

            status = "passed"
            if summary.has_failures:
                status = "failed"
            elif summary.has_warnings:
                status = "warn"

            results[model_name] = {
                "status": status,
                "total": summary.total_checks,
                "passed": summary.passed,
                "failed": summary.failed,
                "has_warnings": summary.has_warnings,
                "success_rate": summary.success_rate,
            }

            for check_result in summary.results:
                if check_result.status.name == "FAILED":
                    logger.error(f"  {check_result.check_name}: FAILED - {check_result.message}")
                else:
                    logger.info(f"  {check_result.check_name}: {check_result.status.name} - {check_result.message}")

        except Exception as e:
            logger.error(f"Checks failed for '{model_name}': {e}")
            results[model_name] = {"status": "error", "error": str(e)}

    return results
