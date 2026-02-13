"""
Schema history endpoints.
"""

from collections import defaultdict
from typing import Any

from aiohttp import web

from interlace.connections.manager import get_connection as get_named_connection
from interlace.schema.tracking import get_current_schema_version, get_schema_history
from interlace.service.api.errors import ErrorCode, NotFoundError, ValidationError
from interlace.service.api.handlers import BaseHandler
from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.schema")


class SchemaHandler(BaseHandler):
    """Handler for schema-related endpoints."""

    def _get_connection(self) -> Any | None:
        """Get the default database connection from the connection manager."""
        try:
            # Determine default connection name from config
            executor_config = self.config.get("executor", {})
            conn_name = executor_config.get("default_connection", "default")

            # Try to get connection from manager
            try:
                conn_obj = get_named_connection(conn_name)
                return conn_obj.connection if hasattr(conn_obj, "connection") else conn_obj
            except (RuntimeError, ValueError):
                # Connection manager not initialized or connection not found
                # Try "default" as fallback
                if conn_name != "default":
                    try:
                        conn_obj = get_named_connection("default")
                        return conn_obj.connection if hasattr(conn_obj, "connection") else conn_obj
                    except Exception:
                        pass
                return None
        except Exception as e:
            logger.debug(f"Could not get connection: {e}")
            return None

    async def get_history(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/schema/history

        Get schema version history for a model.

        Returns list of schema versions with columns and change timestamps.
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        model = self.models[name]
        schema_name = model.get("schema", "public")

        # Get connection from connection manager
        connection = self._get_connection()
        if connection is None:
            return await self.json_response(
                {"versions": [], "total": 0, "message": "No database connection"},
                request=request,
            )

        # Get schema history
        history_records = get_schema_history(connection, name, schema_name)

        if not history_records:
            # Return current schema if no history
            current_schema = self._get_current_schema_from_model(name, model)
            if current_schema:
                return await self.json_response(
                    {
                        "versions": [
                            {
                                "version": 1,
                                "columns": current_schema,
                                "detected_at": None,
                            }
                        ],
                        "total": 1,
                    },
                    request=request,
                )
            return await self.json_response(
                {"versions": [], "total": 0},
                request=request,
            )

        # Group by version
        versions = self._group_by_version(history_records)

        return await self.json_response(
            {"versions": versions, "total": len(versions)},
            request=request,
        )

    async def get_current_version(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/schema/current

        Get current schema version number.
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        model = self.models[name]
        schema_name = model.get("schema", "public")

        connection = self._get_connection()
        if connection is None:
            return await self.json_response(
                {"version": 0, "message": "No database connection"},
                request=request,
            )

        version = get_current_schema_version(connection, name, schema_name)

        return await self.json_response(
            {"version": version},
            request=request,
        )

    async def compare_versions(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/schema/diff

        Compare two schema versions.

        Query params:
            from: Source version number
            to: Target version number
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        model = self.models[name]
        schema_name = model.get("schema", "public")

        # Parse and validate version parameters
        try:
            from_version = int(request.query.get("from", 1))
            to_version = int(request.query.get("to", 0))
        except (ValueError, TypeError) as e:
            raise ValidationError("'from' and 'to' must be valid integers") from e

        connection = self._get_connection()
        if connection is None:
            return await self.json_response(
                {"diff": [], "message": "No database connection"},
                request=request,
            )

        # Get target version (default to current if 0)
        if to_version == 0:
            to_version = get_current_schema_version(connection, name, schema_name)

        # Get both versions
        from_history = get_schema_history(connection, name, schema_name, from_version)
        to_history = get_schema_history(connection, name, schema_name, to_version)

        if not from_history or not to_history:
            return await self.json_response(
                {"diff": [], "from_version": from_version, "to_version": to_version},
                request=request,
            )

        # Compute diff
        diff = self._compute_diff(from_history, to_history)

        return await self.json_response(
            {
                "diff": diff,
                "from_version": from_version,
                "to_version": to_version,
            },
            request=request,
        )

    def _group_by_version(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Group history records by version."""
        by_version: dict[int, dict[str, Any]] = defaultdict(lambda: {"columns": [], "detected_at": None})

        for record in records:
            version = record.get("version", 1)
            version_data = by_version[version]
            version_data["version"] = version
            version_data["columns"].append(
                {
                    "name": record.get("column_name"),
                    "type": record.get("column_type"),
                    "nullable": record.get("is_nullable", True),
                    "is_primary_key": bool(record.get("is_primary_key", False)),
                }
            )
            # Use first detected_at for the version
            if version_data["detected_at"] is None:
                detected_at = record.get("detected_at")
                if detected_at is not None:
                    version_data["detected_at"] = (
                        detected_at.isoformat() if hasattr(detected_at, "isoformat") else str(detected_at)
                    )

        # Sort by version descending (newest first)
        versions = sorted(by_version.values(), key=lambda v: v["version"], reverse=True)
        return versions

    def _get_current_schema_from_model(self, name: str, model: dict[str, Any]) -> list[dict[str, Any]]:
        """Get current schema from model definition or database."""
        columns = []

        # Try from model fields
        fields = model.get("fields")
        if fields and hasattr(fields, "items"):
            for col_name, col_type in fields.items():
                columns.append(
                    {
                        "name": col_name,
                        "type": str(col_type),
                        "nullable": True,
                        "is_primary_key": False,
                    }
                )
            # Mark primary keys
            primary_key = model.get("primary_key")
            if primary_key:
                pk_set = {primary_key} if isinstance(primary_key, str) else set(primary_key)
                for col in columns:
                    if col["name"] in pk_set:
                        col["is_primary_key"] = True
            return columns

        # Try from database if connection available
        connection = self._get_connection()
        if connection is not None:
            schema_name = model.get("schema", "public")
            try:
                table = connection.table(name, database=schema_name)
                schema = table.schema()
                for col_name, col_type in schema.items():
                    columns.append(
                        {
                            "name": col_name,
                            "type": str(col_type),
                            "nullable": True,
                            "is_primary_key": False,
                        }
                    )
                return columns
            except Exception:
                pass

        return columns

    def _compute_diff(self, from_records: list[dict], to_records: list[dict]) -> list[dict[str, Any]]:
        """Compute diff between two versions."""
        from_cols = {r["column_name"]: r for r in from_records}
        to_cols = {r["column_name"]: r for r in to_records}

        diff = []

        # Check removed and modified
        for name, from_col in from_cols.items():
            to_col = to_cols.get(name)
            if not to_col:
                diff.append(
                    {
                        "name": name,
                        "change": "removed",
                        "old_type": from_col.get("column_type"),
                    }
                )
            elif from_col.get("column_type") != to_col.get("column_type"):
                diff.append(
                    {
                        "name": name,
                        "change": "modified",
                        "old_type": from_col.get("column_type"),
                        "new_type": to_col.get("column_type"),
                    }
                )
            else:
                diff.append(
                    {
                        "name": name,
                        "change": "unchanged",
                        "old_type": from_col.get("column_type"),
                        "new_type": to_col.get("column_type"),
                    }
                )

        # Check added
        for name, to_col in to_cols.items():
            if name not in from_cols:
                diff.append(
                    {
                        "name": name,
                        "change": "added",
                        "new_type": to_col.get("column_type"),
                    }
                )

        return diff
