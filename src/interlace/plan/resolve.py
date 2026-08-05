"""Resolve a model's query for execution.

Direct upstream references are rewritten to their physical tables; references to
*ephemeral* upstreams are instead inlined as CTEs (the ephemeral model produces
no table). Ephemeral chains inline recursively, in dependency order, so a model
that reads an ephemeral staging model gets its logic spliced in at compile time.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from sqlglot import exp

from interlace.exceptions import PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import resolve_references
from interlace.ir.relation import TableRef
from interlace.sinks import target_ref


def _cte_name(model_name: str) -> str:
    return "_eph_" + model_name.replace(".", "_")


def _ref_mapping(
    model: CompiledModel, project: CompiledProject, physical: Mapping[str, TableRef] | None
) -> dict[str, TableRef]:
    """Map each dependency to a physical table, or to a (schema-less) CTE name if ephemeral.

    ``physical`` overrides the fingerprint-derived table names — a reused snapshot
    lives at its *previous* physical table, and only the caller (apply) knows that.
    """
    mapping: dict[str, TableRef] = {}
    for dep in model.dependencies:
        upstream = project.models[dep]
        if upstream.materialise == "ephemeral":
            mapping[dep] = TableRef(schema="", name=_cte_name(dep))
        elif upstream.materialise == "table":
            # a table model is read by its delivered external target, not a snapshot
            mapping[dep] = target_ref(upstream.target or "")
        else:
            mapping[dep] = (physical or {}).get(dep, upstream.physical_table)
    return mapping


def _ephemeral_ancestors(model: CompiledModel, project: CompiledProject) -> list[str]:
    """Ephemeral models reachable through ephemeral chains, in dependency order."""
    order: list[str] = []
    seen: set[str] = set()

    def visit(name: str) -> None:
        for dep in project.models[name].dependencies:
            if project.models[dep].materialise == "ephemeral" and dep not in seen:
                seen.add(dep)
                visit(dep)  # inline an ephemeral's own ephemeral deps first
                order.append(dep)

    visit(model.name)
    return order


def resolve_model_query(
    model: CompiledModel, project: CompiledProject, physical: Mapping[str, TableRef] | None = None
) -> exp.Expression:
    """Rewrite a model's query for execution, inlining ephemeral upstreams as CTEs."""
    if model.ast is None:
        raise PlanError(f"cannot resolve a Python model query: {model.name!r}")

    body: exp.Expression = resolve_references(model.ast, _ref_mapping(model, project, physical))
    for ancestor_name in _ephemeral_ancestors(model, project):
        ancestor = project.models[ancestor_name]
        if ancestor.ast is None:
            raise PlanError(f"ephemeral model {ancestor_name!r} must be SQL (cannot inline a Python model)")
        cte_body = resolve_references(ancestor.ast, _ref_mapping(ancestor, project, physical))
        body = cast("exp.Query", body).with_(_cte_name(ancestor_name), as_=cte_body)
    return body
