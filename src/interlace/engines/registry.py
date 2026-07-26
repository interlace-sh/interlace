"""Named engine registry — multi-engine routing without a global singleton.

A project may declare several engines in ``interlace.yaml``. The registry opens
them lazily (so unused remote engines stay cold), resolves models by name, and
closes everything on teardown. Single-engine projects use one entry named
``default`` synthesised from the top-level warehouse fields.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import TypeVar

from interlace.engines.base import EngineAdapter
from interlace.exceptions import ConfigurationError, PlanError

_T = TypeVar("_T", bound=EngineAdapter)

Opener = Callable[[str], EngineAdapter]


class EngineRegistry(Mapping[str, EngineAdapter]):
    """Lazy map of engine name → open :class:`EngineAdapter`.

    ``get(name)`` opens on first use. Unknown names raise
    :class:`ConfigurationError`. ``default`` is the project's default engine name.
    """

    def __init__(self, names: Iterable[str], opener: Opener, *, default: str = "default") -> None:
        self._names = frozenset(names)
        self._opener = opener
        self.default = default
        self._cache: dict[str, EngineAdapter] = {}
        if default not in self._names:
            raise ConfigurationError(
                f"default_engine {default!r} is not a configured engine",
                details={"engines": sorted(self._names)},
            )

    def __getitem__(self, name: str) -> EngineAdapter:
        return self.get(name)

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(self._names))

    def __len__(self) -> int:
        return len(self._names)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self._names

    def get(self, name: str | None = None) -> EngineAdapter:  # type: ignore[override]
        """Return the adapter for ``name`` (or the default engine), opening if needed."""
        key = self.default if name is None else name
        if key not in self._names:
            raise ConfigurationError(
                f"unknown engine {key!r}",
                details={"engines": sorted(self._names)},
            )
        if key not in self._cache:
            self._cache[key] = self._opener(key)
        return self._cache[key]

    def require(self, name: str, *, model: str | None = None) -> EngineAdapter:
        """Like :meth:`get`, but raise :class:`PlanError` when the engine is missing."""
        if name not in self._names:
            where = f" for model {model!r}" if model else ""
            raise PlanError(
                f"engine {name!r}{where} is not configured",
                details={"engines": sorted(self._names)},
            )
        return self.get(name)

    def close(self) -> None:
        """Close every opened adapter (idempotent for already-closed adapters)."""
        for adapter in self._cache.values():
            close = getattr(adapter, "close", None)
            if callable(close):
                close()
        self._cache.clear()


def as_registry(
    engine: EngineAdapter | None = None,
    engines: Mapping[str, EngineAdapter] | EngineRegistry | None = None,
    *,
    default: str = "default",
) -> EngineRegistry:
    """Normalise the ``engine=`` / ``engines=`` apply kwargs into a registry.

    Existing call sites that pass a single ``engine=`` keep working: that adapter
    is registered under ``default``.
    """
    if isinstance(engines, EngineRegistry):
        if engine is not None:
            # Prefer the registry; a bare engine is only a fallback for default.
            pass
        return engines

    cache: dict[str, EngineAdapter] = dict(engines or {})
    if engine is not None:
        cache.setdefault(default, engine)
    if not cache:
        raise PlanError("apply requires engine= or engines=")

    def opener(name: str) -> EngineAdapter:
        try:
            return cache[name]
        except KeyError as exc:
            raise ConfigurationError(
                f"unknown engine {name!r}",
                details={"engines": sorted(cache)},
            ) from exc

    return EngineRegistry(cache.keys(), opener, default=default if default in cache else next(iter(cache)))
