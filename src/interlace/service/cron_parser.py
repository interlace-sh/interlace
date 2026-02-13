"""
Cron expression parser for Interlace scheduling.

Parses standard 5-field crontab expressions and computes next fire times.
Used by the scheduler loop in server.py to determine when models should run.

Cron support:
- Standard 5 fields: minute hour day month day_of_week
- Supported tokens per field: '*', '*/n', 'a', 'a,b,c', 'a-b', 'a-b/n'
- Day-of-month vs day-of-week semantics: if both are restricted (not '*'),
  then match if (dom matches OR dow matches). This matches traditional cron.

No external dependencies -- uses stdlib ``zoneinfo`` for timezone handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


class CronParseError(ValueError):
    pass


@dataclass(frozen=True)
class CronSpec:
    minutes: set[int]
    hours: set[int]
    dom: set[int]  # 1-31
    months: set[int]  # 1-12
    dow: set[int]  # 0-6 (0=Sunday)
    dom_any: bool
    dow_any: bool
    tz: ZoneInfo


def next_fire_time_cron(expr: str, *, now: datetime, timezone: str | None = None) -> datetime:
    """
    Compute next fire time for a standard 5-field cron expression.

    Args:
        expr: Cron expression (e.g. "0 6 * * *" for daily at 6am)
        now: Current datetime (used as reference point)
        timezone: Optional timezone name (e.g. "UTC", "America/New_York")

    Returns:
        Next datetime when the cron expression matches

    Raises:
        CronParseError: If the expression is invalid or produces no match
    """
    tz = ZoneInfo(timezone) if timezone else (now.tzinfo or ZoneInfo("UTC"))
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)

    spec = _parse_cron(expr, tz=tz)  # type: ignore[arg-type]
    # Start at next minute boundary
    start = (now.replace(second=0, microsecond=0) + timedelta(minutes=1)).astimezone(tz)
    return _find_next_match(spec, start)


def _find_next_match(spec: CronSpec, cursor: datetime) -> datetime:
    # Bound search to prevent infinite loops on invalid specs.
    # 370 days covers leap year edge while remaining safe.
    limit = cursor + timedelta(days=370)
    cur = cursor

    while cur <= limit:
        if cur.month not in spec.months:
            cur = _ceil_month(cur, spec)
            continue
        if cur.day not in spec.dom and not spec.dom_any and not _dom_or_dow_match(spec, cur):
            cur = _ceil_day(cur, spec)
            continue

        # Evaluate dom/dow with cron OR semantics (when both restricted).
        if not _dom_or_dow_match(spec, cur):
            cur = _ceil_day(cur, spec)
            continue

        if cur.hour not in spec.hours:
            cur = _ceil_hour(cur, spec)
            continue
        if cur.minute not in spec.minutes:
            cur = _ceil_minute(cur, spec)
            continue
        return cur

    raise CronParseError("cron expression produced no next fire time within safety window")


def _dom_or_dow_match(spec: CronSpec, dt: datetime) -> bool:
    dom_match = dt.day in spec.dom
    # Python: Monday=0..Sunday=6; cron: Sunday=0..Saturday=6 (we normalize to 0=Sunday)
    py_dow = dt.weekday()  # 0=Mon..6=Sun
    cron_dow = (py_dow + 1) % 7  # 0=Sun..6=Sat
    dow_match = cron_dow in spec.dow

    if spec.dom_any and spec.dow_any:
        return True
    if spec.dom_any and not spec.dow_any:
        return dow_match
    if spec.dow_any and not spec.dom_any:
        return dom_match
    # Both restricted -> OR semantics
    return dom_match or dow_match


def _ceil_month(dt: datetime, spec: CronSpec) -> datetime:
    # Advance to next allowed month, set day/hour/minute minimal.
    for _ in range(24):  # at most 12 months, but safe
        if dt.month in spec.months:
            return dt
        dt = (dt.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
    return dt


def _ceil_day(dt: datetime, spec: CronSpec) -> datetime:
    return (dt + timedelta(days=1)).replace(hour=0, minute=0)


def _ceil_hour(dt: datetime, spec: CronSpec) -> datetime:
    return (dt + timedelta(hours=1)).replace(minute=0)


def _ceil_minute(dt: datetime, spec: CronSpec) -> datetime:
    return dt + timedelta(minutes=1)


def _parse_cron(expr: str, *, tz: ZoneInfo) -> CronSpec:
    parts = [p for p in expr.strip().split() if p]
    if len(parts) != 5:
        raise CronParseError(f"cron must have 5 fields, got {len(parts)}: {expr!r}")

    minutes = _parse_field(parts[0], min_v=0, max_v=59)
    hours = _parse_field(parts[1], min_v=0, max_v=23)
    dom_any = parts[2] == "*"
    dom = _parse_field(parts[2], min_v=1, max_v=31)
    months = _parse_field(parts[3], min_v=1, max_v=12)
    dow_any = parts[4] == "*"
    dow = _parse_field(parts[4], min_v=0, max_v=6, allow_7_as_0=True)

    return CronSpec(
        minutes=minutes,
        hours=hours,
        dom=dom,
        months=months,
        dow=dow,
        dom_any=dom_any,
        dow_any=dow_any,
        tz=tz,
    )


def _parse_field(
    token: str,
    *,
    min_v: int,
    max_v: int,
    allow_7_as_0: bool = False,
) -> set[int]:
    token = token.strip()
    if token == "*":
        return set(range(min_v, max_v + 1))

    values: set[int] = set()
    for part in token.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            base = base.strip()
            step_s = step_s.strip()
            if not step_s.isdigit():
                raise CronParseError(f"invalid step in field: {token!r}")
            step = int(step_s)
            part = base

        if part == "*":
            rng = range(min_v, max_v + 1, step)
            values.update(rng)
            continue

        if "-" in part:
            a_s, b_s = part.split("-", 1)
            if not (a_s.strip().isdigit() and b_s.strip().isdigit()):
                raise CronParseError(f"invalid range in field: {token!r}")
            a = int(a_s)
            b = int(b_s)
            if allow_7_as_0:
                a_orig, b_orig = a, b
                a = 0 if a == 7 else a
                b = 0 if b == 7 else b
                # Handle wrap-around: e.g. "5-7" → a=5, b=0 after 7→0
                # means "5, 6, 0" (Fri, Sat, Sun in DOW)
                if a > b and (a_orig <= b_orig):
                    # Range wrapped due to 7→0 conversion
                    values.update(range(a, max_v + 1, step))
                    values.add(0)
                    continue
            if a > b:
                raise CronParseError(f"range start > end in field: {token!r}")
            if a < min_v or b > max_v:
                raise CronParseError(f"range out of bounds in field: {token!r}")
            values.update(range(a, b + 1, step))
            continue

        if not part.isdigit():
            raise CronParseError(f"invalid value in field: {token!r}")
        v = int(part)
        if allow_7_as_0 and v == 7:
            v = 0
        if v < min_v or v > max_v:
            raise CronParseError(f"value out of bounds in field: {token!r}")
        values.add(v)

    if not values:
        raise CronParseError(f"empty field: {token!r}")
    return values
