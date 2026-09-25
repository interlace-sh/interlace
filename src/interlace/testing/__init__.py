"""Fixture tests: build selected models in an ephemeral DuckDB and diff goldens."""

from interlace.testing.golden import FixtureReport, run_fixture_tests

__all__ = ["FixtureReport", "run_fixture_tests"]
