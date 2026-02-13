"""
Interlace startup initialization.

Orchestrates initialization of all components in the correct order:
1. Config (with validation)
2. Logging
3. Connections (with validation, must be before state store)
4. State store
5. Models (discovery, dependency graph, cycle detection)
"""

import os
from pathlib import Path
from typing import Any

import yaml

from interlace.config.loader import Config, load_config
from interlace.connections.manager import init_connections
from interlace.core.dependencies import build_dependency_graph
from interlace.core.state import StateStore
from interlace.exceptions import (
    InitializationError,
    InterlaceError,
)
from interlace.utils.discovery import discover_models


class InterlaceInitializer:
    """Handles complete initialization of Interlace project."""

    def __init__(self, project_dir: Path, env: str | None = None, verbose: bool = False):
        self.project_dir = Path(project_dir)
        self.env = env or os.environ.get("INTERLACE_ENV", "dev")
        self.verbose = verbose

        # Initialized components
        self.config: Config | None = None
        self.connections_initialized: bool = False
        self.state_store: StateStore | None = None
        self.models: dict[str, dict[str, Any]] | None = None
        self.graph: Any | None = None

    def initialize_all(self) -> tuple[Config, StateStore | None, dict[str, dict[str, Any]], Any]:
        """
        Initialize all components in the correct order.

        Returns:
            Tuple of (config, state_store, models, dependency_graph)

        Raises:
            InitializationError: If any initialization step fails
        """
        # 1. Initialize config (with validation)
        self.config = self._initialize_config()

        # 2. Initialize logging (needs config)
        self._initialize_logging()

        # 3. Initialize connections (with validation, must be before state store)
        self._initialize_connections()

        # 4. Initialize state store (needs connections)
        self._initialize_state_store()

        # 5. Initialize models (discovery, dependency graph, cycle detection)
        self.models, self.graph = self._initialize_models()

        return self.config, self.state_store, self.models, self.graph

    def _initialize_config(self) -> Config:
        """Initialize and validate configuration."""
        try:
            config = load_config(self.project_dir, env=self.env)

            # Validate config structure
            config.validate()

            # Store environment and project_dir for later use
            config.data["_env"] = self.env
            config.data["_project_dir"] = self.project_dir

            return config
        except (FileNotFoundError, PermissionError, ValueError, yaml.YAMLError) as e:
            raise InitializationError(str(e)) from None
        except InterlaceError:
            raise
        except Exception as e:
            raise InitializationError(f"Unexpected error loading config: {e}") from None

    def _initialize_logging(self) -> None:
        """Initialize logging from config."""
        try:
            # Set global config for logging auto-setup
            from interlace.config.singleton import GlobalConfig

            GlobalConfig.set_config(self.config)

            # Store project_dir for auto-setup logging
            import interlace.utils.logging as logging_module

            logging_module._stored_project_dir = self.project_dir

            # Logging will be auto-setup by get_logger() when first accessed
            # No explicit setup needed here
        except (ImportError, AttributeError, TypeError) as e:
            raise InitializationError(f"Failed to initialize logging: {e}") from None

    def _initialize_connections(self) -> None:
        """Initialize and validate all connections."""
        try:
            # Initialize connection manager (validates connections)
            init_connections(self.config.data)
            self.connections_initialized = True
        except (ValueError, RuntimeError, OSError) as e:
            raise InitializationError(str(e)) from None
        except InterlaceError:
            raise
        except Exception as e:
            raise InitializationError(f"Failed to initialize connections: {e}") from None

    def _initialize_state_store(self) -> None:
        """Initialize state store (requires connections to be initialized)."""
        if not self.connections_initialized:
            raise InitializationError("Connections must be initialized before state store") from None

        try:
            state_config = self.config.data.get("state", {})
            if state_config.get("enabled", True):
                self.state_store = StateStore(self.config.data)
            else:
                self.state_store = None
        except (ValueError, RuntimeError, OSError) as e:
            raise InitializationError(f"Failed to initialize state store: {e}") from None
        except InterlaceError:
            raise
        except Exception as e:
            raise InitializationError(f"Failed to initialize state store: {e}") from None

    def _initialize_models(self) -> tuple[dict[str, dict[str, Any]], Any]:
        """Discover models, build dependency graph, and check for cycles."""
        models_dir = self.project_dir / "models"

        if not models_dir.exists():
            raise InitializationError(
                f"Models directory not found: {models_dir}\n"
                f"  Suggestion: Create a 'models' directory in your project root"
            )

        if not models_dir.is_dir():
            raise InitializationError(f"Models path exists but is not a directory: {models_dir}")

        # Discover models
        try:
            # Get connection for storing file hashes (optional, graceful if not available)
            connection = None
            if self.state_store:
                try:
                    connection = self.state_store._get_connection()
                except (RuntimeError, OSError):
                    pass  # Graceful degradation if connection not available
            models = discover_models(models_dir, connection)
        except (ImportError, SyntaxError, ValueError, OSError) as e:
            raise InitializationError(f"Failed to discover models: {e}") from None
        except InterlaceError:
            raise
        except Exception as e:
            raise InitializationError(f"Failed to discover models: {e}") from None

        if not models:
            if self.verbose:
                from interlace.utils.logging import get_logger

                logger = get_logger("interlace.initialization")
                logger.info("No models found in models directory")
            return {}, None

        # Build dependency graph
        try:
            graph = build_dependency_graph(models)
        except (ValueError, KeyError, TypeError) as e:
            raise InitializationError(f"Failed to build dependency graph: {e}") from None

        # Check for cycles
        cycles = graph.detect_cycles()
        if cycles:
            raise InitializationError(
                f"Circular dependencies detected: {cycles}\n"
                f"Suggestion: Review model dependencies to break the cycle"
            ) from None

        return models, graph


def initialize(
    project_dir: Path, env: str | None = None, verbose: bool = False
) -> tuple[Config, StateStore | None, dict[str, dict[str, Any]], Any]:
    """
    Initialize Interlace project.

    Args:
        project_dir: Project directory path
        env: Environment name (default: from INTERLACE_ENV env var or "dev")
        verbose: Enable verbose output

    Returns:
        Tuple of (config, state_store, models, dependency_graph)

    Raises:
        InitializationError: If initialization fails
    """
    initializer = InterlaceInitializer(project_dir, env, verbose)
    return initializer.initialize_all()
