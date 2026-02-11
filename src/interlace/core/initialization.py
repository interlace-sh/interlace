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
from typing import Dict, Any, Optional, Tuple
from interlace.config.loader import load_config, Config
from interlace.connections.manager import init_connections
from interlace.core.state import StateStore
from interlace.utils.discovery import discover_models
from interlace.core.dependencies import build_dependency_graph


class InitializationError(Exception):
    """
    Exception raised during initialization with detailed error information.
    
    This exception should not be chained to avoid exposing internal stack traces.
    Error messages should be informative and actionable.
    """
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
    
    def __str__(self) -> str:
        return self.message


class InterlaceInitializer:
    """Handles complete initialization of Interlace project."""
    
    def __init__(self, project_dir: Path, env: Optional[str] = None, verbose: bool = False):
        self.project_dir = Path(project_dir)
        self.env = env or os.environ.get("INTERLACE_ENV", "dev")
        self.verbose = verbose
        
        # Initialized components
        self.config: Optional[Config] = None
        self.connections_initialized: bool = False
        self.state_store: Optional[StateStore] = None
        self.models: Optional[Dict[str, Dict[str, Any]]] = None
        self.graph: Optional[Any] = None
    
    def initialize_all(self) -> Tuple[Config, Optional[StateStore], Dict[str, Dict[str, Any]], Any]:
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
        except Exception as e:
            # Extract clean error message
            error_msg = str(e)
            raise InitializationError(error_msg) from None
    
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
        except Exception as e:
            error_msg = str(e)
            raise InitializationError(f"Failed to initialize logging: {error_msg}") from None
    
    def _initialize_connections(self) -> None:
        """Initialize and validate all connections."""
        try:
            # Initialize connection manager (validates connections)
            init_connections(self.config.data)
            self.connections_initialized = True
        except Exception as e:
            # Extract clean error message - connection errors already have good messages
            error_msg = str(e)
            # Remove nested "Error: Error: ..." patterns
            if error_msg.startswith("Connection validation failed"):
                # Use the message as-is (it already has all the details)
                raise InitializationError(error_msg) from None
            # For other errors, pass through the message
            raise InitializationError(error_msg) from None
    
    def _initialize_state_store(self) -> None:
        """Initialize state store (requires connections to be initialized)."""
        if not self.connections_initialized:
            raise InitializationError(
                "Connections must be initialized before state store"
            ) from None
        
        try:
            state_config = self.config.data.get("state", {})
            if state_config.get("enabled", True):
                self.state_store = StateStore(self.config.data)
            else:
                self.state_store = None
        except Exception as e:
            error_msg = str(e)
            raise InitializationError(f"Failed to initialize state store: {error_msg}") from None
    
    def _initialize_models(self) -> Tuple[Dict[str, Dict[str, Any]], Any]:
        """Discover models, build dependency graph, and check for cycles."""
        models_dir = self.project_dir / "models"
        
        if not models_dir.exists():
            raise InitializationError(
                f"Models directory not found: {models_dir}\n"
                f"  Suggestion: Create a 'models' directory in your project root"
            )
        
        if not models_dir.is_dir():
            raise InitializationError(
                f"Models path exists but is not a directory: {models_dir}"
            )
        
        # Discover models
        try:
            # Get connection for storing file hashes (optional, graceful if not available)
            connection = None
            if self.state_store:
                try:
                    connection = self.state_store._get_connection()
                except Exception:
                    pass  # Graceful degradation if connection not available
            models = discover_models(models_dir, connection)
        except Exception as e:
            error_msg = str(e)
            raise InitializationError(f"Failed to discover models: {error_msg}") from None
        
        if not models:
            if self.verbose:
                from interlace.utils.logging import get_logger
                logger = get_logger("interlace.initialization")
                logger.info("No models found in models directory")
            return {}, None
        
        # Build dependency graph
        try:
            graph = build_dependency_graph(models)
        except Exception as e:
            error_msg = str(e)
            raise InitializationError(f"Failed to build dependency graph: {error_msg}") from None
        
        # Check for cycles
        cycles = graph.detect_cycles()
        if cycles:
            raise InitializationError(
                f"Circular dependencies detected: {cycles}\n"
                f"Suggestion: Review model dependencies to break the cycle"
            ) from None
        
        return models, graph


def initialize(project_dir: Path, env: Optional[str] = None, verbose: bool = False) -> Tuple[Config, Optional[StateStore], Dict[str, Dict[str, Any]], Any]:
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

