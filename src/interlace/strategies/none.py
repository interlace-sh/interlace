"""
None strategy - no automatic loading, user handles manually.

Phase 0: Basic none strategy (placeholder).
"""

from interlace.strategies.base import Strategy


class NoneStrategy(Strategy):
    """None strategy - returns new data as-is (user handles persistence)."""
