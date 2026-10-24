"""One parsed schedule per model.

``fresh: {column, within}`` and ``after: [a, b]`` are stored as the column, the
window, and the model names. Callers read those fields. They do not split the
wire string back apart. ``as_dict`` is the form the API and a written model
file use.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from interlace.exceptions import DefinitionError

ScheduleKind = Literal["cron", "every", "watch", "on_change", "fresh", "after", "webhook"]
SCHEDULE_KINDS: tuple[ScheduleKind, ...] = ("cron", "every", "watch", "on_change", "fresh", "after", "webhook")


@dataclass(frozen=True)
class Schedule:
    kind: ScheduleKind
    expression: str
    after: tuple[str, ...] = ()
    column: str = ""
    window: str = ""

    def as_dict(self) -> dict[str, str]:
        return {self.kind: self.expression}


def normalize_schedule(schedule: Mapping[str, Any] | None, *, model: str) -> Schedule | None:
    """Parse a schedule mapping once. Empty means the model is not scheduled."""
    if not schedule:
        return None
    if not isinstance(schedule, Mapping):
        raise DefinitionError(f"model {model!r}: schedule must be a mapping")
    kind = _only_kind(schedule, model=model)
    value = schedule[kind]
    if kind == "fresh":
        column, window = _fresh(model, value)
        return Schedule(kind="fresh", expression=f"{column} {window}", column=column, window=window)
    if kind == "after":
        names = _after(model, value)
        return Schedule(kind="after", expression=",".join(names), after=names)
    if not isinstance(value, str):
        raise DefinitionError(f"model {model!r}: schedule {kind!r} must be a string")
    return Schedule(kind=kind, expression=value)


def _only_kind(schedule: Mapping[str, Any], *, model: str) -> ScheduleKind:
    key = next(iter(schedule)) if len(schedule) == 1 else ""
    if key == "cron":
        return "cron"
    if key == "every":
        return "every"
    if key == "watch":
        return "watch"
    if key == "on_change":
        return "on_change"
    if key == "fresh":
        return "fresh"
    if key == "after":
        return "after"
    if key == "webhook":
        return "webhook"
    raise DefinitionError(
        f"model {model!r}: schedule needs exactly one of {', '.join(SCHEDULE_KINDS)}",
        details={"schedule": dict(schedule)},
    )


def _fresh(model: str, value: object) -> tuple[str, str]:
    if isinstance(value, Mapping):
        column = value.get("column")
        window = value.get("within")
        if not isinstance(column, str) or not column.strip():
            raise DefinitionError(f"model {model!r}: fresh.column must name a timestamp column")
        if not isinstance(window, str) or not window.strip():
            raise DefinitionError(f"model {model!r}: fresh.within must be a window like '2h'")
        return column.strip(), window.strip()
    if isinstance(value, str):
        column, _, window = value.strip().rpartition(" ")
        if not column.strip() or not window.strip():
            raise DefinitionError(f"model {model!r}: fresh {value!r} must look like 'updated_at 2h'")
        return column.strip(), window.strip()
    raise DefinitionError(f"model {model!r}: fresh must be a window string or {{column, within}}")


def _after(model: str, value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        names = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, (list, tuple)):
        names = [str(item).strip() for item in value]
        if any(not name for name in names):
            raise DefinitionError(f"model {model!r}: after must name a model, or a list of models")
    else:
        raise DefinitionError(f"model {model!r}: after must name a model, or a list of models")
    if not names:
        raise DefinitionError(f"model {model!r}: after must name a model, or a list of models")
    return tuple(dict.fromkeys(names))
