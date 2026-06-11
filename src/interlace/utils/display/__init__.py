"""
Display package for Interlace execution.

This module re-exports all public names from the display module
for backward compatibility. Existing imports like:

    from interlace.utils.display import Display, get_display

continue to work unchanged.
"""

from interlace.utils.display.display import (
    ArrowColumn,
    DeletedColumn,
    DependenciesColumn,
    Display,
    ErrorColumn,
    ExecutionTimeColumn,
    FlowTimeColumn,
    InsertedColumn,
    LogHandler,
    MaterialisationColumn,
    NameColumn,
    RowsColumn,
    SchemaChangesColumn,
    StatusColumn,
    StepColumn,
    StrategyColumn,
    UpdatedColumn,
    get_display,
)

__all__ = [
    "ArrowColumn",
    "DeletedColumn",
    "DependenciesColumn",
    "Display",
    "ErrorColumn",
    "ExecutionTimeColumn",
    "FlowTimeColumn",
    "InsertedColumn",
    "LogHandler",
    "MaterialisationColumn",
    "NameColumn",
    "RowsColumn",
    "SchemaChangesColumn",
    "StatusColumn",
    "StepColumn",
    "StrategyColumn",
    "UpdatedColumn",
    "get_display",
]
