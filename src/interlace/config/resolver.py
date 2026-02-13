"""
Configuration resolution and environment variable substitution.

Phase 0: Resolve configuration with environment variables and placeholders.
"""

import os
import re
from typing import Any


def resolve_config(config_data: dict[str, Any], env: str = "dev") -> dict[str, Any]:
    """
    Resolve configuration with environment variable substitution.

    Phase 0: Substitute ${VAR_NAME} and {env} placeholders.

    Args:
        config_data: Configuration dictionary
        env: Current environment name

    Returns:
        Resolved configuration
    """
    return _resolve_value(config_data, env)


def _resolve_value(value: Any, env: str) -> Any:
    """Recursively resolve values in configuration."""
    if isinstance(value, dict):
        return {k: _resolve_value(v, env) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_value(item, env) for item in value]
    elif isinstance(value, str):
        # Substitute ${VAR_NAME}
        result = re.sub(r"\${([^}]+)}", lambda m: os.getenv(m.group(1), m.group(0)), value)
        # Substitute {env} placeholder
        result = result.replace("{env}", env)
        return result
    else:
        return value
