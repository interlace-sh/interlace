"""Warehouse names shared by the planner and the control plane.

Environment views, the production schema, and cross-engine staging tables.
``state.janitor`` and ``plan`` both need these; neither should import the other
to get a schema name.
"""

from __future__ import annotations

from interlace.ir.relation import TableRef

PRODUCTION_ENV = "prod"
"""The production environment lives at the *unprefixed* schema (``main.orders``):
that's what BI tools and consumers connect to. Every other environment is a
prefixed sandbox (``dev__main.orders``) over the same physical snapshots."""

XFER_SCHEMA = "interlace__xfer"


def env_view(environment: str, model_name: str) -> TableRef:
    """The virtual-environment view for a model: ``<schema>.<model>`` in
    production, ``<env>__<schema>.<model>`` everywhere else."""
    schema, _, base = model_name.rpartition(".")
    prefix = "" if environment == PRODUCTION_ENV else f"{environment}__"
    return TableRef(schema=f"{prefix}{schema or 'main'}", name=base)


def staging_table(upstream: str) -> TableRef:
    """Where a transferred upstream lands on the consumer's engine (replaced on every transfer)."""
    return TableRef(schema=XFER_SCHEMA, name=upstream.replace(".", "__"))
