#!/usr/bin/env python3
"""Run the incremental pipeline programmatically with backfill support."""

from pathlib import Path

from interlace import run_sync

# Standard run — processes only new data via cursors
result = run_sync(project_dir=Path(__file__).parent)
print(f"Run completed: {result.get('status', 'unknown')}")

# Backfill — reprocess a specific date range
# result = run_sync(
#     project_dir=Path(__file__).parent,
#     since="2024-01-01",
#     until="2024-01-31",
#     force=True,
# )
# print(f"Backfill completed: {result.get('status', 'unknown')}")
