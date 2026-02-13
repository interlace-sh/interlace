"""
Global configuration singleton.

Provides a global config instance that can be accessed from anywhere in the application.
"""

import threading
from collections.abc import Iterator
from typing import Any

from interlace.config.loader import Config


class GlobalConfig:
    """Global configuration singleton manager."""

    _instance: Config | None = None
    _lock = threading.Lock()

    @classmethod
    def set_config(cls, config: Config):
        """Set the global config instance."""
        with cls._lock:
            cls._instance = config

    @classmethod
    def get_config(cls) -> Config | None:
        """Get the global config instance."""
        return cls._instance

    @classmethod
    def reset_config(cls):
        """Reset the global config instance (for testing)."""
        with cls._lock:
            cls._instance = None


# Global config accessor
def get_config() -> Config | None:
    """
    Get the global Config instance.

    Returns:
        Config instance if set, None otherwise
    """
    return GlobalConfig.get_config()


# Create a proxy object that provides dict-like access
class ConfigProxy:
    """
    Proxy object that provides dict-like access to global config.

    Usage:
        from interlace import config
        api_key = config["my_api"]["api_key"]
        # or
        api_key = config.get("my_api.api_key")
    """

    def __getitem__(self, key: str):
        """Dict-like access: config['key']."""
        cfg = get_config()
        if cfg is None:
            raise RuntimeError("Config not initialized. Call interlace.run() first or set config manually.")
        return cfg[key]

    def __contains__(self, key: str) -> bool:
        """Check if key exists: 'key' in config."""
        cfg = get_config()
        if cfg is None:
            return False
        return key in cfg

    def get(self, key: str, default: Any = None):
        """Get config value with dot notation: config.get('my_api.api_key')."""
        cfg = get_config()
        if cfg is None:
            return default
        return cfg.get(key, default)

    def __iter__(self) -> Iterator[str]:
        """Iterate over config keys."""
        cfg = get_config()
        if cfg is None:
            return iter([])
        return iter(cfg)

    def keys(self):
        """Get config keys."""
        cfg = get_config()
        if cfg is None:
            return []
        return cfg.keys()

    def values(self):
        """Get config values."""
        cfg = get_config()
        if cfg is None:
            return []
        return cfg.values()

    def items(self):
        """Get config items."""
        cfg = get_config()
        if cfg is None:
            return []
        return cfg.items()


# Create the global config proxy instance
config = ConfigProxy()
