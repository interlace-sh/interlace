"""
Data loading strategies.

Phase 0: Basic strategy implementations (merge_by_key, append, replace, none).
Phase 2: SCD Type 2 strategy for dimension history tracking.
"""

from interlace.strategies.base import Strategy
from interlace.strategies.merge_by_key import MergeByKeyStrategy
from interlace.strategies.append import AppendStrategy
from interlace.strategies.replace import ReplaceStrategy
from interlace.strategies.none import NoneStrategy
from interlace.strategies.scd_type_2 import SCDType2Strategy

__all__ = [
    "Strategy",
    "MergeByKeyStrategy",
    "AppendStrategy",
    "ReplaceStrategy",
    "NoneStrategy",
    "SCDType2Strategy",
]

# Strategy registry
STRATEGIES = {
    "merge_by_key": MergeByKeyStrategy,
    "append": AppendStrategy,
    "replace": ReplaceStrategy,
    "none": NoneStrategy,
    "scd_type_2": SCDType2Strategy,
}


def get_strategy(strategy_name: str) -> Strategy:
    """Get strategy by name."""
    if strategy_name not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {strategy_name}")
    return STRATEGIES[strategy_name]()
