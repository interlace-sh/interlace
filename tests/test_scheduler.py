"""
Tests for the cron parser and scheduler persistence.

Tests for cron parsing, next fire time calculation, compute_next_fire helper,
and scheduler persistence / missed-job handling.
"""

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest


class TestCronParsing:
    """Tests for cron expression parsing."""

    def test_parse_star(self):
        """Test parsing * (every value)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("*", min_v=0, max_v=59)
        assert result == set(range(0, 60))

    def test_parse_single_value(self):
        """Test parsing a single numeric value."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("30", min_v=0, max_v=59)
        assert result == {30}

    def test_parse_comma_separated(self):
        """Test parsing comma-separated values."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("0,15,30,45", min_v=0, max_v=59)
        assert result == {0, 15, 30, 45}

    def test_parse_range(self):
        """Test parsing a range (a-b)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("1-5", min_v=1, max_v=31)
        assert result == {1, 2, 3, 4, 5}

    def test_parse_step(self):
        """Test parsing step values (*/n)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("*/15", min_v=0, max_v=59)
        assert result == {0, 15, 30, 45}

    def test_parse_range_with_step(self):
        """Test parsing range with step (a-b/n)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("0-30/10", min_v=0, max_v=59)
        assert result == {0, 10, 20, 30}

    def test_parse_day_of_week_7_as_0(self):
        """Test that day 7 is treated as Sunday (0)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("7", min_v=0, max_v=6, allow_7_as_0=True)
        assert result == {0}

    def test_parse_invalid_value(self):
        """Test that invalid values raise CronParseError."""
        from interlace.service.cron_parser import CronParseError, _parse_field

        with pytest.raises(CronParseError):
            _parse_field("abc", min_v=0, max_v=59)

    def test_parse_out_of_bounds(self):
        """Test that out-of-bounds values raise CronParseError."""
        from interlace.service.cron_parser import CronParseError, _parse_field

        with pytest.raises(CronParseError):
            _parse_field("60", min_v=0, max_v=59)

    def test_parse_empty_field(self):
        """Test that empty field raises CronParseError."""
        from interlace.service.cron_parser import CronParseError, _parse_field

        with pytest.raises(CronParseError):
            _parse_field("", min_v=0, max_v=59)


class TestCronSpec:
    """Tests for CronSpec parsing."""

    def test_parse_cron_every_minute(self):
        """Test parsing '* * * * *' (every minute)."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import _parse_cron

        spec = _parse_cron("* * * * *", tz=ZoneInfo("UTC"))

        assert spec.minutes == set(range(0, 60))
        assert spec.hours == set(range(0, 24))
        assert spec.dom == set(range(1, 32))
        assert spec.months == set(range(1, 13))
        assert spec.dow == set(range(0, 7))
        assert spec.dom_any is True
        assert spec.dow_any is True

    def test_parse_cron_hourly(self):
        """Test parsing '0 * * * *' (every hour at minute 0)."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import _parse_cron

        spec = _parse_cron("0 * * * *", tz=ZoneInfo("UTC"))

        assert spec.minutes == {0}
        assert spec.hours == set(range(0, 24))

    def test_parse_cron_daily(self):
        """Test parsing '0 0 * * *' (daily at midnight)."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import _parse_cron

        spec = _parse_cron("0 0 * * *", tz=ZoneInfo("UTC"))

        assert spec.minutes == {0}
        assert spec.hours == {0}
        assert spec.dom_any is True
        assert spec.dow_any is True

    def test_parse_cron_weekday(self):
        """Test parsing '0 9 * * 1-5' (9am on weekdays)."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import _parse_cron

        spec = _parse_cron("0 9 * * 1-5", tz=ZoneInfo("UTC"))

        assert spec.minutes == {0}
        assert spec.hours == {9}
        assert spec.dow == {1, 2, 3, 4, 5}
        assert spec.dow_any is False

    def test_parse_cron_invalid_fields(self):
        """Test that wrong number of fields raises error."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import CronParseError, _parse_cron

        with pytest.raises(CronParseError):
            _parse_cron("* * *", tz=ZoneInfo("UTC"))

        with pytest.raises(CronParseError):
            _parse_cron("* * * * * *", tz=ZoneInfo("UTC"))


class TestNextFireTime:
    """Tests for next_fire_time_cron calculation."""

    def test_next_fire_every_minute(self):
        """Test next fire time for every minute."""
        from interlace.service.cron_parser import next_fire_time_cron

        now = datetime(2024, 1, 15, 10, 30, 45, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("* * * * *", now=now)

        # Should be next minute (10:31:00)
        assert next_fire.minute == 31
        assert next_fire.hour == 10
        assert next_fire.second == 0

    def test_next_fire_hourly(self):
        """Test next fire time for hourly at minute 0."""
        from interlace.service.cron_parser import next_fire_time_cron

        now = datetime(2024, 1, 15, 10, 30, 0, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("0 * * * *", now=now)

        # Should be 11:00:00
        assert next_fire.minute == 0
        assert next_fire.hour == 11

    def test_next_fire_specific_time(self):
        """Test next fire time for specific hour:minute."""
        from interlace.service.cron_parser import next_fire_time_cron

        now = datetime(2024, 1, 15, 10, 30, 0, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("0 12 * * *", now=now)

        # Should be 12:00:00 same day
        assert next_fire.minute == 0
        assert next_fire.hour == 12
        assert next_fire.day == 15

    def test_next_fire_wraps_to_next_day(self):
        """Test that fire time wraps to next day if needed."""
        from interlace.service.cron_parser import next_fire_time_cron

        now = datetime(2024, 1, 15, 23, 30, 0, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("0 9 * * *", now=now)

        # Should be 9:00:00 next day
        assert next_fire.minute == 0
        assert next_fire.hour == 9
        assert next_fire.day == 16

    def test_next_fire_with_timezone(self):
        """Test next fire time respects timezone."""
        from interlace.service.cron_parser import next_fire_time_cron

        now = datetime(2024, 1, 15, 10, 30, 0, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("0 12 * * *", now=now, timezone="America/New_York")

        # Result should be in the specified timezone
        assert next_fire.tzinfo == ZoneInfo("America/New_York")

    def test_next_fire_weekday_restriction(self):
        """Test next fire time with weekday restriction."""
        from interlace.service.cron_parser import next_fire_time_cron

        # January 15, 2024 is a Monday (weekday=0)
        now = datetime(2024, 1, 15, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("0 9 * * 1", now=now)  # Monday only

        # Should be Monday Jan 22 (next Monday at 9am)
        assert next_fire.weekday() == 0  # Monday
        assert next_fire.hour == 9

    def test_next_fire_monthly(self):
        """Test next fire time for specific day of month."""
        from interlace.service.cron_parser import next_fire_time_cron

        now = datetime(2024, 1, 20, 10, 0, 0, tzinfo=ZoneInfo("UTC"))
        next_fire = next_fire_time_cron("0 0 1 * *", now=now)  # 1st of month

        # Should be Feb 1
        assert next_fire.day == 1
        assert next_fire.month == 2


class TestDomDowSemantics:
    """Tests for day-of-month OR day-of-week cron semantics."""

    def test_dom_only(self):
        """Test when only day-of-month is restricted."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import CronSpec, _dom_or_dow_match

        # Day 15 only, any day of week
        spec = CronSpec(
            minutes={0},
            hours={0},
            dom={15},
            months=set(range(1, 13)),
            dow=set(range(0, 7)),
            dom_any=False,
            dow_any=True,
            tz=ZoneInfo("UTC"),
        )

        # Jan 15 should match
        dt = datetime(2024, 1, 15, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is True

        # Jan 16 should not match
        dt = datetime(2024, 1, 16, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is False

    def test_dow_only(self):
        """Test when only day-of-week is restricted."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import CronSpec, _dom_or_dow_match

        # Any day of month, Monday only (1)
        spec = CronSpec(
            minutes={0},
            hours={0},
            dom=set(range(1, 32)),
            months=set(range(1, 13)),
            dow={1},  # Monday
            dom_any=True,
            dow_any=False,
            tz=ZoneInfo("UTC"),
        )

        # Jan 15, 2024 is Monday - should match
        dt = datetime(2024, 1, 15, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is True

        # Jan 16, 2024 is Tuesday - should not match
        dt = datetime(2024, 1, 16, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is False

    def test_both_restricted_or_semantics(self):
        """Test OR semantics when both dom and dow are restricted."""
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import CronSpec, _dom_or_dow_match

        # Day 1 OR Monday - traditional cron OR semantics
        spec = CronSpec(
            minutes={0},
            hours={0},
            dom={1},
            months=set(range(1, 13)),
            dow={1},  # Monday
            dom_any=False,
            dow_any=False,
            tz=ZoneInfo("UTC"),
        )

        # Jan 1, 2024 is Monday - matches both, should be True
        dt = datetime(2024, 1, 1, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is True

        # Jan 15, 2024 is Monday - matches dow, should be True
        dt = datetime(2024, 1, 15, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is True

        # Feb 1, 2024 is Thursday - matches dom, should be True
        dt = datetime(2024, 2, 1, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is True

        # Jan 16, 2024 is Tuesday, not day 1 - should be False
        dt = datetime(2024, 1, 16, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert _dom_or_dow_match(spec, dt) is False


class TestComputeNextFire:
    """Tests for InterlaceService._compute_next_fire helper."""

    def _make_service(self):
        """Create a minimal InterlaceService instance for testing."""
        from pathlib import Path

        from interlace.service.server import InterlaceService

        svc = InterlaceService(
            project_dir=Path("/tmp/test-project"),
            env=None,
            verbose=False,
        )
        return svc

    def test_compute_next_fire_cron(self):
        """Test compute_next_fire with a cron schedule."""
        svc = self._make_service()
        sched = {"cron": "0 12 * * *", "timezone": "UTC"}
        # Reference: Jan 15 2024 10:00 UTC
        ref = datetime(2024, 1, 15, 10, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()
        result = svc._compute_next_fire(sched, ref)

        # Next fire should be Jan 15 12:00 UTC
        result_dt = datetime.fromtimestamp(result, tz=ZoneInfo("UTC"))
        assert result_dt.hour == 12
        assert result_dt.day == 15

    def test_compute_next_fire_interval(self):
        """Test compute_next_fire with an interval schedule."""
        svc = self._make_service()
        sched = {"every_s": 600}
        ref = time.time()
        result = svc._compute_next_fire(sched, ref)

        # Next fire should be ~600 seconds from ref
        assert abs(result - (ref + 600)) < 1.0

    def test_compute_next_fire_invalid_cron_fallback(self):
        """Test that invalid cron expression falls back to +60s."""
        svc = self._make_service()
        sched = {"cron": "invalid cron expression"}
        ref = time.time()
        result = svc._compute_next_fire(sched, ref)

        # Fallback should be ~60 seconds from ref
        assert abs(result - (ref + 60)) < 1.0

    def test_compute_next_fire_default_interval(self):
        """Test that missing every_s defaults to 3600s."""
        svc = self._make_service()
        sched = {}  # No cron, no every_s
        ref = time.time()
        result = svc._compute_next_fire(sched, ref)

        # Default: 3600s
        assert abs(result - (ref + 3600)) < 1.0

    def test_compute_next_fire_minimum_interval(self):
        """Test that every_s below 1 is clamped to 1."""
        svc = self._make_service()
        sched = {"every_s": 0.1}
        ref = time.time()
        result = svc._compute_next_fire(sched, ref)

        # Minimum 1 second
        assert abs(result - (ref + 1.0)) < 0.5


class TestSchedulerPersistence:
    """Tests for scheduler persistence via StateStore."""

    def test_missed_job_detection_cron(self):
        """Test that a missed cron job is detected (next_fire in the past)."""
        svc = self._make_service()
        sched = {"cron": "0 6 * * *", "timezone": "UTC"}  # daily at 6am

        # Simulate: last run was 2 days ago at 6am
        two_days_ago = datetime(2024, 1, 13, 6, 0, 0, tzinfo=ZoneInfo("UTC"))
        ref = two_days_ago.timestamp()

        # Compute next fire from that last run
        computed = svc._compute_next_fire(sched, ref)

        # It should be Jan 14 06:00 -- definitely in the past relative to "now"
        computed_dt = datetime.fromtimestamp(computed, tz=ZoneInfo("UTC"))
        assert computed_dt.day == 14
        assert computed_dt.hour == 6

        # The scheduler loop checks if computed <= now -- this would trigger misfire
        now = datetime(2024, 1, 15, 10, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()
        assert computed <= now, "Missed job should have next_fire in the past"

    def test_missed_job_detection_interval(self):
        """Test that a missed interval job is detected."""
        svc = self._make_service()
        sched = {"every_s": 3600}  # every hour

        # Last run was 5 hours ago
        five_hours_ago = time.time() - 5 * 3600
        computed = svc._compute_next_fire(sched, five_hours_ago)

        # Next fire should be 4 hours ago (5h ago + 1h) -- in the past
        assert computed < time.time(), "Missed interval job should be in the past"

    def test_no_missed_job_when_on_schedule(self):
        """Test that on-schedule models don't trigger misfire."""
        svc = self._make_service()
        sched = {"every_s": 3600}

        # Last run was 30 minutes ago
        thirty_min_ago = time.time() - 1800
        computed = svc._compute_next_fire(sched, thirty_min_ago)

        # Next fire should be 30 minutes in the future
        assert computed > time.time(), "On-schedule model should not trigger misfire"

    def _make_service(self):
        """Create a minimal InterlaceService instance for testing."""
        from pathlib import Path

        from interlace.service.server import InterlaceService

        return InterlaceService(
            project_dir=Path("/tmp/test-project"),
            env=None,
            verbose=False,
        )


class TestSchedulerStatus:
    """Tests for scheduler status observability."""

    def test_get_scheduler_status_no_scheduled_models(self):
        """Test status when no models are scheduled."""
        from pathlib import Path

        from interlace.service.server import InterlaceService

        svc = InterlaceService(
            project_dir=Path("/tmp/test-project"),
            env=None,
            verbose=False,
        )
        svc.models = {"my_model": {"name": "my_model"}}  # no schedule

        status = svc.get_scheduler_status()
        assert status["running"] is False
        assert status["scheduled_models"] == 0
        assert status["models"] == []

    def test_get_scheduler_status_with_scheduled_model(self):
        """Test status when a model has a schedule."""
        from pathlib import Path

        from interlace.service.server import InterlaceService

        svc = InterlaceService(
            project_dir=Path("/tmp/test-project"),
            env=None,
            verbose=False,
        )
        svc._scheduler_running = True
        svc.models = {
            "hourly_model": {
                "name": "hourly_model",
                "schema": "public",
                "schedule": {"cron": "0 * * * *"},
            },
        }
        # Simulate next_fire populated by scheduler loop
        next_ts = time.time() + 3600
        svc._scheduler_next_fire = {"hourly_model": next_ts}

        status = svc.get_scheduler_status()
        assert status["running"] is True
        assert status["scheduled_models"] == 1
        assert len(status["models"]) == 1
        assert status["models"][0]["model"] == "hourly_model"
        assert status["models"][0]["next_fire_at"] is not None
        assert status["models"][0]["schedule"] == {"cron": "0 * * * *"}
