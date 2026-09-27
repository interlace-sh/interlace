"""One plan document for the CLI, the HTTP API, and MCP.

The plan layer does not import service wire types. Callers map this structure
onto msgspec structs or JSON.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from interlace.graph.project import CompiledProject
from interlace.plan.plan import Plan
from interlace.state.snapshot import Snapshot


@dataclass(frozen=True)
class PlanChangeView:
    name: str
    change_type: str
    category: str | None
    previous_fingerprint: str | None
    new_fingerprint: str | None
    impacted_columns: tuple[str, ...]
    new_sql: str | None
    previous_sql: str | None
    reused: bool


@dataclass(frozen=True)
class PlanDocument:
    environment: str
    changes: tuple[PlanChangeView, ...]
    transfers: tuple[str, ...]
    physical: tuple[str, ...]
    drift: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["changes"] = [asdict(change) for change in self.changes]
        payload["transfers"] = list(self.transfers)
        payload["physical"] = list(self.physical)
        payload["drift"] = list(self.drift)
        return payload


def plan_document(
    plan: Plan,
    compiled: CompiledProject,
    previous: dict[tuple[str, str], Snapshot],
    environment: str,
) -> PlanDocument:
    """The preview every surface shows: changes, transfers, physical DDL, drift."""
    reused = {snapshot.name for snapshot in plan.reuses}
    changes: list[PlanChangeView] = []
    for change in plan.changes:
        model = compiled.models.get(change.name)
        prior = previous.get((change.name, change.previous_fingerprint or ""))
        changes.append(
            PlanChangeView(
                name=change.name,
                change_type=change.change_type.value,
                category=change.category.value if change.category else None,
                previous_fingerprint=change.previous_fingerprint,
                new_fingerprint=change.new_fingerprint,
                impacted_columns=change.impacted_columns,
                new_sql=model.definition_sql if model else None,
                previous_sql=prior.definition_sql if prior else None,
                reused=change.name in reused,
            )
        )
    transfers = tuple(
        f"{edge.model}: {edge.source.name} -> {edge.target.name} ({edge.via} -> {edge.table.schema}.{edge.table.name})"
        for edge in plan.transfers
    )
    physical = tuple(
        f"{'+' if change.op == 'add' else '-'} {change.kind} {change.name}"
        for action in plan.physical
        for change in action.changes
    )
    return PlanDocument(
        environment=environment,
        changes=tuple(changes),
        transfers=transfers,
        physical=physical,
        drift=tuple(note.message for note in plan.drift),
    )
