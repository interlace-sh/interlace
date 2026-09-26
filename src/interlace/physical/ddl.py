"""Render index and constraint DDL as sqlglot ASTs.

Engines that do not enforce a constraint get a non-unique index on its columns
(when that is meaningful) and a warning, instead of a constraint that would
silently do nothing. ``NOT NULL`` on DuckDB is ``ALTER COLUMN … SET NOT NULL``;
everywhere else it is a named ``CHECK`` so it can be dropped by the name we record.
"""

from __future__ import annotations

from sqlglot import exp, parse_one

from interlace.engines.base import EngineCaps
from interlace.exceptions import DefinitionError
from interlace.ir.relation import TableRef, drop
from interlace.physical.spec import ConstraintSpec, IndexSpec, PhysicalObject

_INDEX_FALLBACK = frozenset({"primary_key", "unique", "foreign_key"})


def desired_objects(
    model: str,
    indexes: tuple[IndexSpec, ...],
    constraints: tuple[ConstraintSpec, ...],
    *,
    manage_indexes: bool,
    manage_constraints: bool,
    caps: EngineCaps,
) -> tuple[tuple[PhysicalObject, ...], list[str]]:
    """Objects to create for this model, plus warnings for constraints the engine will not enforce."""
    objects: list[PhysicalObject] = []
    warnings: list[str] = []
    if manage_indexes:
        for index in indexes:
            objects.append(PhysicalObject(kind="index", name=index.object_name(model)))
    if not manage_constraints:
        return tuple(objects), warnings
    for constraint in constraints:
        if constraint.type in caps.enforced_constraints:
            kind = "not_null" if constraint.type == "not_null" and caps.not_null_as_column else "constraint"
            objects.append(
                PhysicalObject(
                    kind=kind,
                    name=constraint.object_name(model),
                    column=constraint.columns[0] if kind == "not_null" else None,
                )
            )
            continue
        if constraint.columns and constraint.type in _INDEX_FALLBACK and manage_indexes:
            name = constraint.object_name(model)
            objects.append(PhysicalObject(kind="index", name=name))
            warnings.append(
                f"{model}: {constraint.type} is not enforced on this engine; "
                f"created non-unique index {name} instead. A check is the portable gate."
            )
        else:
            warnings.append(
                f"{model}: {constraint.type} is not enforced on this engine and was not created. "
                f"A check is the portable gate."
            )
    return tuple(objects), warnings


def create_statements(
    table: TableRef,
    model: str,
    indexes: tuple[IndexSpec, ...],
    constraints: tuple[ConstraintSpec, ...],
    objects: tuple[PhysicalObject, ...],
    *,
    dialect: str,
) -> list[exp.Expr]:
    """CREATE/ALTER statements for ``objects``, in declaration order."""
    wanted = {obj.name: obj for obj in objects}
    statements: list[exp.Expr] = []
    for index in indexes:
        obj = wanted.get(index.object_name(model))
        if obj is not None and obj.kind == "index":
            statements.append(_create_index(table, obj.name, index.columns, index.unique))
    for constraint in constraints:
        obj = wanted.get(constraint.object_name(model))
        if obj is None:
            continue
        if obj.kind == "index":
            statements.append(_create_index(table, obj.name, constraint.columns, unique=False))
        elif obj.kind == "not_null":
            statements.append(_set_not_null(table, constraint.columns[0], drop=False))
        else:
            statements.append(_add_constraint(table, obj.name, constraint, dialect))
    return statements


def drop_statement(table: TableRef, obj: PhysicalObject, dialect: str) -> exp.Expr:
    """Drop one recorded object. DuckDB index names are catalog-qualified; Postgres indexes are not."""
    if obj.kind == "index":
        catalog = table.catalog if dialect == "duckdb" else None
        return drop(exp.table_(obj.name, db=table.schema, catalog=catalog), kind="INDEX")
    if obj.kind == "not_null" and obj.column:
        return _set_not_null(table, obj.column, drop=True)
    return exp.Alter(
        this=table.to_expr(),
        kind="TABLE",
        actions=[drop(exp.to_identifier(obj.name), kind="CONSTRAINT")],
    )


def _create_index(table: TableRef, name: str, columns: tuple[str, ...], unique: bool) -> exp.Expr:
    return exp.Create(
        this=exp.Index(
            this=exp.to_identifier(name),
            table=table.to_expr(),
            params=exp.IndexParameters(columns=[exp.Ordered(this=exp.column(column)) for column in columns]),
        ),
        kind="INDEX",
        unique=unique,
        exists=True,
    )


def _set_not_null(table: TableRef, column: str, *, drop: bool) -> exp.Expr:
    return exp.Alter(
        this=table.to_expr(),
        kind="TABLE",
        actions=[exp.AlterColumn(this=exp.to_identifier(column), drop=drop, allow_null=drop)],
    )


def _add_constraint(table: TableRef, name: str, spec: ConstraintSpec, dialect: str) -> exp.Expr:
    return exp.Alter(
        this=table.to_expr(),
        kind="TABLE",
        actions=[
            exp.AddConstraint(
                expressions=[
                    exp.Constraint(this=exp.to_identifier(name), expressions=[_constraint_body(spec, dialect)])
                ]
            )
        ],
    )


def _constraint_body(spec: ConstraintSpec, dialect: str) -> exp.Expr:
    if spec.type == "primary_key":
        return exp.PrimaryKey(expressions=[exp.to_identifier(column) for column in spec.columns])
    if spec.type == "unique":
        return exp.UniqueColumnConstraint(
            this=exp.Schema(expressions=[exp.to_identifier(column) for column in spec.columns])
        )
    if spec.type == "not_null":
        column = exp.column(spec.columns[0])
        return exp.CheckColumnConstraint(this=exp.Not(this=exp.Is(this=column, expression=exp.Null())))
    if spec.type == "check":
        try:
            predicate = parse_one(spec.expression or "", read=dialect)
        except Exception as exc:
            raise DefinitionError(f"check expression {spec.expression!r} is not valid SQL") from exc
        return exp.CheckColumnConstraint(this=predicate)
    return exp.ForeignKey(
        expressions=[exp.to_identifier(column) for column in spec.columns],
        reference=exp.Reference(
            this=exp.Schema(
                this=_reference_table(spec.reference or ""),
                expressions=[exp.to_identifier(column) for column in spec.fields],
            )
        ),
    )


def _reference_table(reference: str) -> exp.Table:
    parts = reference.split(".")
    if len(parts) == 1:
        return exp.table_(parts[0])
    if len(parts) == 2:
        return exp.table_(parts[1], db=parts[0])
    if len(parts) == 3:
        return exp.table_(parts[2], db=parts[1], catalog=parts[0])
    raise DefinitionError(f"foreign_key target {reference!r} must be table, schema.table, or catalog.schema.table")
