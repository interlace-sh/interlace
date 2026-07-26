"""Versioned model state — the unit the planner diffs and the scheduler fills.

A :class:`Snapshot` ties a model's identity to a content fingerprint, a physical
table holding that exact version's data, and the interval ledger recording which
ranges are filled. Virtual-environment views point at ``physical_table``; promote
and rollback are just repointing those views.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from interlace.ir.relation import TableRef
from interlace.state.interval import IntervalSet


class ChangeCategory(Enum):
    """How a model changed relative to its previous snapshot."""

    BREAKING = "breaking"  # output could differ -> backfill this model + impacted downstream
    NON_BREAKING = "non_breaking"  # additive (e.g. new column) -> no downstream rebuild
    METADATA = "metadata"  # comments/owner/tags only -> never rebuild
    FORWARD_ONLY = "forward_only"  # apply going forward without restating history


@dataclass
class Snapshot:
    """A specific, fingerprinted version of a model.

    Not frozen: ``intervals`` grows as the scheduler fills ranges. Identity is the
    ``(name, fingerprint)`` pair; the physical table name embeds the fingerprint.
    """

    name: str
    fingerprint: str
    metadata_hash: str
    physical_table: TableRef
    change_category: ChangeCategory
    intervals: IntervalSet = field(default_factory=IntervalSet)
    local_fingerprint: str = ""  # SQL + config only; lets the differ separate direct vs indirect changes
    definition_sql: str | None = None  # canonical SQL of the version, for change classification
    engine: str = "default"  # named engine that owns this snapshot's physical table

    @property
    def key(self) -> str:
        return f"{self.name}@{self.fingerprint}"
