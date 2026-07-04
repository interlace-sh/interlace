"""Data-quality checks. Results gate promotion: an ``error``-severity failure
aborts the apply before the environment is promoted.

Import :mod:`interlace.checks.runner` for execution — kept out of this package
init so declaring checks (spec) never drags in the runtime.
"""

from interlace.checks.spec import CheckSpec, parse_checks

__all__ = ["CheckSpec", "parse_checks"]
