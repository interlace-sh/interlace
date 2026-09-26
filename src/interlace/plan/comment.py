"""Markdown rendering of a plan — the GitHub PR comment body."""

from __future__ import annotations

from interlace.plan.plan import ChangeType, Plan

_MARKER = "<!-- interlace-plan -->"


def plan_markdown(plan: Plan, environment: str) -> str:
    """GitHub-flavoured summary of ``plan`` for a PR comment.

    Starts with ``<!-- interlace-plan -->`` so the Action can replace an earlier
    comment on the same PR instead of stacking them.
    """
    reused = {snapshot.name for snapshot in plan.reuses}
    breaking = [
        change.name for change in plan.changes if change.category is not None and change.category.value == "breaking"
    ]
    physical = [
        f"{'+' if change.op == 'add' else '-'} {change.kind} `{change.name}` ({action.name})"
        for action in plan.physical
        for change in action.changes
    ]
    lines = [_MARKER, f"## interlace plan (`{environment}`)", ""]
    if not plan.changes and not plan.physical and not plan.transfers and not plan.drift:
        lines.append("No changes.")
        return "\n".join(lines) + "\n"
    if breaking:
        lines.append("**Breaking:** " + ", ".join(f"`{name}`" for name in breaking))
    if reused:
        lines.append(f"**Reuse (no rebuild):** {len(reused)} model(s)")
    if physical:
        lines.append("**Physical:** " + ", ".join(physical))
    if plan.transfers:
        lines.append("**Transfers:** " + ", ".join(f"`{t.model}`" for t in plan.transfers))
    if plan.drift:
        lines.append("**Drift:**")
        lines.extend(f"- {note.message}" for note in plan.drift)
    if breaking or reused or physical or plan.transfers or plan.drift:
        lines.append("")
    if plan.changes:
        lines.extend(
            [
                "| Model | Change | Category | Build |",
                "| --- | --- | --- | --- |",
            ]
        )
        for change in plan.changes:
            build = (
                "reuse" if change.name in reused else ("—" if change.change_type is ChangeType.REMOVED else "rebuild")
            )
            category = change.category.value if change.category else "—"
            lines.append(f"| `{change.name}` | {change.change_type.value} | {category} | {build} |")
        lines.append("")
    if plan.blocking:
        lines.append("This plan is **blocked** by schema drift (`force` will not bypass it).")
        lines.append("")
    elif breaking:
        lines.append("Apply will refuse until you pass `--force` / `force=true`.")
        lines.append("")
    return "\n".join(lines)
