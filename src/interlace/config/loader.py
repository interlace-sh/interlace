"""
Configuration file loading.

Phase 0: Load and parse config.yaml files.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from interlace.config.resolver import resolve_config


class Config:
    """Interlace configuration container with dict-like access."""

    def __init__(self, data: dict[str, Any]):
        self.data = data
        # Convenience properties for common config sections
        self.connections = data.get("connections", {})
        self.scheduler = data.get("scheduler", {})
        self.models = data.get("models", {})

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation."""
        keys = key.split(".")
        value = self.data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value

    def __getitem__(self, key: str) -> Any:
        """Dict-like access: config['key'] or config['nested']['key']."""
        if isinstance(key, str) and "." in key:
            # Support dot notation: config['my_api.api_key']
            return self.get(key)
        # Direct key access
        if key in self.data:
            value = self.data[key]
            # Return nested dicts as Config objects for chaining
            if isinstance(value, dict):
                return Config(value)
            return value
        raise KeyError(f"Config key '{key}' not found")

    def __contains__(self, key: str) -> bool:
        """Check if key exists in config."""
        if isinstance(key, str) and "." in key:
            # For dot notation, check if path exists
            keys = key.split(".")
            value = self.data
            for k in keys:
                if not isinstance(value, dict) or k not in value:
                    return False
                value = value[k]
            return True
        return key in self.data

    def __iter__(self) -> "Iterator[str]":
        """Iterate over top-level keys."""
        return iter(self.data)

    def keys(self):
        """Get top-level keys."""
        return self.data.keys()

    def values(self):
        """Get top-level values."""
        return self.data.values()

    def items(self):
        """Get top-level items."""
        return self.data.items()

    def validate(self) -> None:
        """Validate configuration structure and content."""
        errors = []

        # Check if config is a dictionary
        if not isinstance(self.data, dict):
            errors.append(f"Configuration must be a dictionary/mapping, got {type(self.data).__name__}")
            raise ValueError("\n".join(errors))

        # Validate connections section (if present)
        connections = self.data.get("connections")
        if connections is not None and not isinstance(connections, dict):
            errors.append(f"Configuration 'connections' must be a dictionary, got {type(connections).__name__}")

        # Check for at least one connection (warning, not error)
        if not connections:
            # This is a warning, not an error - will be handled by connection validation
            pass

        if errors:
            raise ValueError("\n".join(errors))


def load_config(project_path: Path | None = None, env: str | None = None) -> Config:
    """
    Load Interlace configuration.

    Phase 0: Load config.yaml and config.{env}.yaml, merge with environment variables.

    Args:
        project_path: Path to project root (default: current directory)
        env: Environment name (dev, staging, prod)

    Returns:
        Config instance with merged configuration
    """
    if project_path is None:
        project_path = Path.cwd()

    # Load base config
    base_config_path = project_path / "config.yaml"
    if not base_config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {base_config_path}\n"
            f"  Suggestion: Create a config.yaml file in your project root"
        )

    if not base_config_path.is_file():
        raise FileNotFoundError(f"Configuration path is not a file: {base_config_path}")

    try:
        with open(base_config_path) as f:
            try:
                config_data = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                # Provide detailed YAML parsing error
                error_msg = str(e)
                if hasattr(e, "problem_mark"):
                    mark = e.problem_mark
                    raise ValueError(
                        f"Error parsing config.yaml at line {mark.line + 1}, column {mark.column + 1}:\n"
                        f"  {error_msg}\n"
                        f"  File: {base_config_path}\n"
                        f"  Suggestion: Check YAML syntax, ensure proper indentation and quotes"
                    ) from e
                else:
                    raise ValueError(f"Error parsing config.yaml: {error_msg}\n" f"  File: {base_config_path}") from e
    except PermissionError as e:
        raise PermissionError(
            f"Permission denied reading config.yaml: {base_config_path}\n"
            f"  Error: {e}\n"
            f"  Suggestion: Check file permissions"
        ) from e

    # Load environment-specific config
    if env:
        env_config_path = project_path / f"config.{env}.yaml"
        if env_config_path.exists():
            with open(env_config_path) as f:
                env_data = yaml.safe_load(f) or {}
                # Merge (env overrides base)
                _merge_dict(config_data, env_data)

    # Apply environment variable substitution and resolve placeholders
    env_name = env or "dev"
    config_data = resolve_config(config_data, env_name)

    return Config(config_data)


def _merge_dict(base: dict, override: dict):
    """Recursively merge override into base."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _merge_dict(base[key], value)
        else:
            base[key] = value


def _substitute_env_vars(data: Any) -> Any:
    """
    Substitute environment variables in config.

    Supports ${VAR_NAME} syntax and {env} placeholder.
    """
    if isinstance(data, dict):
        return {k: _substitute_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_substitute_env_vars(item) for item in data]
    elif isinstance(data, str):
        # Substitute ${VAR_NAME}
        import re

        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))

        result = re.sub(r"\${([^}]+)}", replace_var, data)
        # Note: {env} placeholder substitution is handled in resolve_config()
        # This function does basic env var substitution only
        return result
    else:
        return data
