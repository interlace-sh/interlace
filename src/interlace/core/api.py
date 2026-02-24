"""
Programmatic API for Interlace.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

from interlace.core.executor import Executor
from interlace.utils.async_utils import dual


@dual
async def run(
    models: list[str | Callable] | str | Callable | None = None,
    project_dir: Path | None = None,
    env: str | None = None,
    verbose: bool = False,
    since: str | None = None,
    until: str | None = None,
    force: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Run Interlace models programmatically, automatically works in both sync and async contexts.

    Args:
        models: Models to run. Can be:
            - None: Run all discovered models
            - str: Single model name (e.g., "users")
            - Callable: Single model function (e.g., users)
            - List[str]: List of model names (e.g., ["users", "orders"])
            - List[Callable]: List of model functions (e.g., [users, orders])
            - List[Union[str, Callable]]: Mixed list (e.g., ["users", orders])
        project_dir: Project directory (default: current directory)
        env: Environment name (default: from INTERLACE_ENV env var or "dev")
        verbose: Enable verbose output
        since: Backfill lower bound - overrides cursor start value (e.g. "2024-01-01")
        until: Backfill upper bound - adds cursor upper-bound filter
        **kwargs: Additional options

    Returns:
        Dictionary mapping model names to execution results

    Environment:
        Uses INTERLACE_ENV environment variable (default: "dev")

    Examples:
        # Sync usage (auto-detected)
        results = run()  # Blocks until complete

        # Async usage (auto-detected)
        results = await run()  # Non-blocking, returns coroutine

        # Backfill a time range
        results = run(since="2024-01-01", until="2024-06-30")
    """

    # Determine project directory
    if project_dir is None:
        project_dir = Path.cwd()
    else:
        project_dir = Path(project_dir)

    # Initialize Interlace (config, logging, connections, state store, models)
    from interlace.core.initialization import InitializationError, initialize

    try:
        config_obj, state_store, all_models, graph = initialize(project_dir, env=env, verbose=verbose)
        config = config_obj.data
    except InitializationError as e:
        # For programmatic API, we want clean error messages
        # Raise as RuntimeError so caller sees the message, not stack trace
        import sys

        # Print error to stderr and exit with non-zero code
        # This prevents stack trace display
        # Use logger if available, otherwise stderr
        try:
            from interlace.utils.logging import get_logger

            logger = get_logger("interlace.api")
            logger.error(f"Execution failed: {e}", exc_info=True)
        except Exception:
            import sys

            sys.stderr.write(f"ERROR: {e}\n")
        sys.exit(1)

    if not all_models:
        logger = get_logger("interlace.api")
        if verbose:
            logger.info("No models found!")
        return {}

    # Get display for header info
    from interlace.utils.display import get_display

    get_display()

    # Filter models if specific models requested
    if models is not None:
        # Normalize to list
        if not isinstance(models, list):
            models = [models]

        # Convert callables to names
        model_names = set()
        for m in models:
            if isinstance(m, str):
                model_names.add(m)
            elif callable(m):
                # Get model name from function attribute
                if hasattr(m, "_interlace_model"):
                    model_names.add(m._interlace_model["name"])
                else:
                    raise ValueError(f"Callable {m} is not an Interlace model (missing @model decorator)")
            else:
                raise ValueError(f"Invalid model type: {type(m)}")

        # Add dependencies
        models_to_run = set(model_names)
        for model_name in model_names:
            deps = graph.get_dependencies(model_name)
            models_to_run.update(deps)

        # Filter models
        all_models = {k: v for k, v in all_models.items() if k in models_to_run}
    # Inject backfill overrides into config
    if since is not None:
        config["_since"] = since
    if until is not None:
        config["_until"] = until
    force = force or (since is not None)  # Backfill implies forced re-execution

    # Execute models
    executor = Executor(config)
    results = await executor.execute_dynamic(all_models, graph, force=force)

    return results


def run_sync(
    models: list[str | Callable] | str | Callable | None = None,
    project_dir: Path | None = None,
    env: str | None = None,
    verbose: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Synchronous wrapper for run().

    This explicitly runs synchronously, even if called from an async context.
    Use this when you want to force synchronous execution.

    See run() for parameter documentation.
    """
    return asyncio.run(run(models=models, project_dir=project_dir, env=env, verbose=verbose, **kwargs))  # type: ignore[arg-type]
