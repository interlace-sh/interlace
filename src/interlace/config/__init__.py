"""
Configuration management.

Phase 0: Configuration file parsing, environment resolution, connection loading.
"""

from interlace.config.loader import load_config, Config
from interlace.config.resolver import resolve_config

__all__ = [
    "load_config",
    "Config",
    "resolve_config",
]
