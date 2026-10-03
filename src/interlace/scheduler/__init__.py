"""Orchestration: triggers, the trigger engine, and the worker (queue = state store).

Submodules import each other. This package does not load them up front: importing
``interlace.scheduler.completion`` must not pull in the worker, which imports the
apply path that imports completion.
"""

from __future__ import annotations

import importlib
from typing import Any

__all__ = [
    "CronTrigger",
    "IntervalTrigger",
    "RunRequest",
    "Trigger",
    "TriggerEngine",
    "build_triggers",
    "drain",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "CronTrigger": ("interlace.scheduler.triggers", "CronTrigger"),
    "IntervalTrigger": ("interlace.scheduler.triggers", "IntervalTrigger"),
    "RunRequest": ("interlace.scheduler.triggers", "RunRequest"),
    "Trigger": ("interlace.scheduler.triggers", "Trigger"),
    "TriggerEngine": ("interlace.scheduler.engine", "TriggerEngine"),
    "build_triggers": ("interlace.scheduler.engine", "build_triggers"),
    "drain": ("interlace.scheduler.worker", "drain"),
}


def __getattr__(name: str) -> Any:
    found = _EXPORTS.get(name)
    if found is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = found
    return getattr(importlib.import_module(module_name), attr)
