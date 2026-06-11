"""
Plugin registry for extensible materializers, strategies, and quality checks.

Provides lazy-loaded registries that defer imports until first use.
Built-in plugins are registered automatically; third-party plugins
can register via setuptools entry_points.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.registry")


class PluginRegistry:
    """
    Lazy-loaded plugin registry.

    Stores factory callables (classes or functions) keyed by name.
    Built-in factories are registered via load_builtins() on first access.
    Instances are cached after first creation.

    Example:
        >>> materializers = PluginRegistry("materializers")
        >>> materializers.register("table", TableMaterializer)
        >>> m = materializers.get("table")  # creates instance on first call
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._factories: dict[str, Callable[..., Any]] = {}
        self._instances: dict[str, Any] = {}
        self._builtins_loaded = False
        self._builtin_loader: Callable[[PluginRegistry], None] | None = None

    def set_builtin_loader(self, loader: Callable[[PluginRegistry], None]) -> None:
        """Set the function that registers built-in plugins."""
        self._builtin_loader = loader

    def _ensure_builtins(self) -> None:
        """Load built-in plugins on first access."""
        if not self._builtins_loaded:
            self._builtins_loaded = True
            if self._builtin_loader:
                self._builtin_loader(self)

    def register(self, key: str, factory: Callable[..., Any] | type) -> None:
        """Register a plugin factory by key."""
        self._factories[key] = factory

    def get(self, key: str) -> Any:
        """
        Get a plugin instance by key (lazy instantiation).

        Returns None if key is not registered.
        """
        self._ensure_builtins()

        if key in self._instances:
            return self._instances[key]

        factory = self._factories.get(key)
        if factory is None:
            return None

        instance = factory()
        self._instances[key] = instance
        return instance

    def get_all(self) -> dict[str, Any]:
        """Get all registered plugins as a dict (instantiates all)."""
        self._ensure_builtins()
        return {key: self.get(key) for key in self._factories}

    def keys(self) -> list[str]:
        """List all registered plugin keys."""
        self._ensure_builtins()
        return list(self._factories.keys())

    def __contains__(self, key: str) -> bool:
        self._ensure_builtins()
        return key in self._factories

    def __repr__(self) -> str:
        return f"PluginRegistry({self._name!r}, keys={list(self._factories.keys())})"


# ---------------------------------------------------------------------------
# Built-in loaders — deferred imports to avoid pulling in all modules at startup
# ---------------------------------------------------------------------------


def _load_materializers(registry: PluginRegistry) -> None:
    from interlace.materialization.ephemeral import EphemeralMaterializer
    from interlace.materialization.table import TableMaterializer
    from interlace.materialization.view import ViewMaterializer

    registry.register("table", TableMaterializer)
    registry.register("view", ViewMaterializer)
    registry.register("ephemeral", EphemeralMaterializer)


def _load_strategies(registry: PluginRegistry) -> None:
    from interlace.strategies.append import AppendStrategy
    from interlace.strategies.merge_by_key import MergeByKeyStrategy
    from interlace.strategies.none import NoneStrategy
    from interlace.strategies.replace import ReplaceStrategy
    from interlace.strategies.scd_type_2 import SCDType2Strategy

    registry.register("merge_by_key", MergeByKeyStrategy)
    registry.register("append", AppendStrategy)
    registry.register("replace", ReplaceStrategy)
    registry.register("scd_type_2", SCDType2Strategy)
    registry.register("none", NoneStrategy)


# ---------------------------------------------------------------------------
# Global registries — singleton instances
# ---------------------------------------------------------------------------

materializer_registry = PluginRegistry("materializers")
materializer_registry.set_builtin_loader(_load_materializers)

strategy_registry = PluginRegistry("strategies")
strategy_registry.set_builtin_loader(_load_strategies)
