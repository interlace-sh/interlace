"""
Flow and task history endpoints.
"""

from typing import Any, Dict, List, Optional

from aiohttp import web

from interlace.service.api.errors import NotFoundError, ErrorCode, ValidationError
from interlace.service.api.handlers import BaseHandler

# Pagination limits to prevent resource exhaustion
MAX_PAGE_SIZE = 1000
MAX_OFFSET = 100000


class FlowsHandler(BaseHandler):
    """Handler for flow history endpoints."""

    async def list(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/flows

        List execution flows with optional filtering.

        Query params:
            status: Filter by status (pending, running, completed, failed, cancelled)
            trigger_type: Filter by trigger type (cli, api, schedule, event, webhook)
            since: Filter flows started after timestamp
            until: Filter flows started before timestamp
            limit: Max results (default: 50)
            offset: Pagination offset
        """
        # Get filter params
        status_filter = request.query.get("status")
        trigger_filter = request.query.get("trigger_type")
        since = request.query.get("since")
        until = request.query.get("until")

        # Parse and validate pagination params
        try:
            limit = int(request.query.get("limit", 50))
            offset = int(request.query.get("offset", 0))
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid pagination parameter: {e}")

        # Enforce bounds to prevent resource exhaustion
        limit = max(1, min(limit, MAX_PAGE_SIZE))
        offset = max(0, min(offset, MAX_OFFSET))

        # Query from state store if available
        flows = []
        total = 0

        if self.state_store:
            try:
                flows, total = await self._query_flows(
                    status=status_filter,
                    trigger_type=trigger_filter,
                    since=since,
                    until=until,
                    limit=limit,
                    offset=offset,
                )
            except Exception:
                # State store query failed, return empty
                pass

        # Include current in-memory flow if active
        if hasattr(self.service, "flow") and self.service.flow:
            current = self.service.flow
            # Check if it matches filters
            if (not status_filter or current.status.value == status_filter) and \
               (not trigger_filter or current.trigger_type == trigger_filter):
                flow_dict = self._serialize_flow(current)
                # Prepend current flow if not in results
                if not any(f["flow_id"] == flow_dict["flow_id"] for f in flows):
                    flows.insert(0, flow_dict)
                    total += 1

        return await self.json_response(
            {
                "flows": flows[:limit],
                "total": total,
                "has_more": total > offset + limit,
            },
            request=request,
        )

    async def get(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/flows/{flow_id}

        Get details of a specific flow including all tasks.
        """
        flow_id = request.match_info["flow_id"]

        # Check current in-memory flow first
        if hasattr(self.service, "flow") and self.service.flow:
            if self.service.flow.flow_id == flow_id:
                flow = self._serialize_flow(self.service.flow, include_tasks=True)
                return await self.json_response(flow, request=request)

        # Query from state store
        if self.state_store:
            try:
                flow = await self._get_flow_from_store(flow_id)
                if flow:
                    return await self.json_response(flow, request=request)
            except Exception:
                pass

        raise NotFoundError("Flow", flow_id, ErrorCode.FLOW_NOT_FOUND)

    async def tasks(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/flows/{flow_id}/tasks

        Get all tasks for a specific flow.
        """
        flow_id = request.match_info["flow_id"]

        # Check current in-memory flow first
        if hasattr(self.service, "flow") and self.service.flow:
            if self.service.flow.flow_id == flow_id:
                tasks = [
                    self._serialize_task(task)
                    for task in self.service.flow.tasks.values()
                ]
                return await self.json_response({"tasks": tasks}, request=request)

        # Query from state store
        if self.state_store:
            try:
                tasks = await self._get_tasks_from_store(flow_id)
                if tasks is not None:
                    return await self.json_response({"tasks": tasks}, request=request)
            except Exception:
                pass

        raise NotFoundError("Flow", flow_id, ErrorCode.FLOW_NOT_FOUND)

    def _serialize_flow(
        self, flow, include_tasks: bool = False
    ) -> Dict[str, Any]:
        """Serialize Flow object to dict."""
        # Compute task counts
        total_tasks = len(flow.tasks) if hasattr(flow, "tasks") else 0
        completed_tasks = 0
        failed_tasks = 0

        total_rows = 0
        if hasattr(flow, "tasks"):
            for task in flow.tasks.values():
                if task.status.value == "completed":
                    completed_tasks += 1
                elif task.status.value == "failed":
                    failed_tasks += 1
                if task.rows_processed is not None:
                    total_rows += task.rows_processed

        result = {
            "flow_id": flow.flow_id,
            "status": flow.status.value,
            "started_at": flow.started_at,
            "completed_at": flow.completed_at,
            "duration_seconds": flow.get_duration() if hasattr(flow, "get_duration") else None,
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "total_rows": total_rows if total_rows > 0 else None,
            "trigger": flow.trigger_type,
            # Keep legacy fields for compatibility
            "trigger_type": flow.trigger_type,
            "trigger_id": getattr(flow, "trigger_id", None),
            "created_by": getattr(flow, "created_by", None),
            "metadata": getattr(flow, "metadata", {}),
            "summary": flow.get_summary() if hasattr(flow, "get_summary") else self._compute_summary(flow),
        }

        if include_tasks:
            result["tasks"] = [
                self._serialize_task(task)
                for task in flow.tasks.values()
            ]

        return result

    def _serialize_task(self, task) -> Dict[str, Any]:
        """Serialize Task object to dict."""
        return {
            "task_id": task.task_id,
            "flow_id": task.flow_id,
            "model_name": task.model_name,
            "schema_name": getattr(task, "schema_name", "public"),
            "status": task.status.value,
            "attempt": task.attempt,
            "max_attempts": task.max_attempts,
            "enqueued_at": task.enqueued_at,
            "started_at": task.started_at,
            "completed_at": task.completed_at,
            "duration_seconds": task.get_duration() if hasattr(task, "get_duration") else None,
            "wait_time_seconds": task.get_wait_time() if hasattr(task, "get_wait_time") else None,
            "materialise": task.materialise,
            "strategy": task.strategy,
            "dependencies": task.dependencies,
            "rows_processed": task.rows_processed,
            "rows_ingested": getattr(task, "rows_ingested", None),
            "rows_inserted": getattr(task, "rows_inserted", None),
            "rows_updated": getattr(task, "rows_updated", None),
            "rows_deleted": getattr(task, "rows_deleted", None),
            "schema_changes": getattr(task, "schema_changes", 0),
            "error_type": task.error_type,
            "error_message": task.error_message,
            "error_stacktrace": task.error_stacktrace,
        }

    def _compute_summary(self, flow) -> Dict[str, int]:
        """Compute task summary for a flow."""
        summary = {
            "total_tasks": 0,
            "pending": 0,
            "waiting": 0,
            "ready": 0,
            "running": 0,
            "materialising": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
        }

        for task in flow.tasks.values():
            summary["total_tasks"] += 1
            status = task.status.value.lower()
            if status in summary:
                summary[status] += 1

        return summary

    async def _query_flows(
        self,
        status: Optional[str],
        trigger_type: Optional[str],
        since: Optional[str],
        until: Optional[str],
        limit: int,
        offset: int,
    ) -> tuple:
        """Query flows from state store or in-memory history."""
        # Try state store first
        if self.state_store:
            try:
                conn = self.state_store._get_connection()
                if conn is not None:
                    # Build dynamic WHERE clause
                    conditions: List[str] = []
                    if status:
                        conditions.append(f"status = '{status}'")
                    if trigger_type:
                        conditions.append(f"trigger_type = '{trigger_type}'")
                    if since:
                        conditions.append(f"started_at >= TIMESTAMP '{since}'")
                    if until:
                        conditions.append(f"started_at <= TIMESTAMP '{until}'")

                    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

                    # Get total count
                    count_result = conn.sql(
                        f"SELECT COUNT(*) AS cnt FROM interlace.flows{where}"
                    ).execute()
                    total = int(count_result.iloc[0, 0]) if count_result is not None and len(count_result) > 0 else 0

                    # Get page of flows
                    result = conn.sql(
                        f"SELECT * FROM interlace.flows{where} "
                        f"ORDER BY started_at DESC LIMIT {limit} OFFSET {offset}"
                    ).execute()

                    if result is not None and len(result) > 0:
                        flows = result.to_dict("records")
                        # Normalise keys for API response
                        for f in flows:
                            f.setdefault("trigger", f.get("trigger_type"))
                            f.setdefault("summary", {})
                        return flows, total
            except Exception:
                pass  # Fall back to in-memory

        # Fall back to in-memory history
        flows = []
        for flow in getattr(self.service, "flow_history", []):
            # Apply filters
            if status and flow.status.value != status:
                continue
            if trigger_type and flow.trigger_type != trigger_type:
                continue
            if since and flow.started_at and flow.started_at < float(since):
                continue
            if until and flow.started_at and flow.started_at > float(until):
                continue

            flows.append(self._serialize_flow(flow))

        total = len(flows)

        # Apply pagination
        flows = flows[offset:offset + limit]

        return flows, total

    async def _get_flow_from_store(self, flow_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific flow from state store or in-memory history."""
        # First check in-memory history
        for flow in getattr(self.service, "flow_history", []):
            if flow.flow_id == flow_id:
                return self._serialize_flow(flow, include_tasks=True)

        # Query state store
        if self.state_store:
            try:
                conn = self.state_store._get_connection()
                if conn is not None:
                    from interlace.core.state import _escape_sql_string

                    safe_id = _escape_sql_string(flow_id)
                    result = conn.sql(
                        f"SELECT * FROM interlace.flows WHERE flow_id = '{safe_id}'"
                    ).execute()
                    if result is not None and len(result) > 0:
                        flow_dict = result.to_dict("records")[0]
                        flow_dict.setdefault("trigger", flow_dict.get("trigger_type"))
                        flow_dict.setdefault("summary", {})

                        # Fetch associated tasks
                        tasks_result = conn.sql(
                            f"SELECT * FROM interlace.tasks WHERE flow_id = '{safe_id}' "
                            f"ORDER BY started_at"
                        ).execute()
                        if tasks_result is not None and len(tasks_result) > 0:
                            flow_dict["tasks"] = tasks_result.to_dict("records")
                        else:
                            flow_dict["tasks"] = []
                        return flow_dict
            except Exception:
                pass

        return None

    async def _get_tasks_from_store(self, flow_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get tasks for a flow from state store or in-memory history."""
        # First check in-memory history
        for flow in getattr(self.service, "flow_history", []):
            if flow.flow_id == flow_id:
                return [self._serialize_task(task) for task in flow.tasks.values()]

        # Query state store
        if self.state_store:
            try:
                conn = self.state_store._get_connection()
                if conn is not None:
                    from interlace.core.state import _escape_sql_string

                    safe_id = _escape_sql_string(flow_id)
                    result = conn.sql(
                        f"SELECT * FROM interlace.tasks WHERE flow_id = '{safe_id}' "
                        f"ORDER BY started_at"
                    ).execute()
                    if result is not None and len(result) > 0:
                        return result.to_dict("records")
            except Exception:
                pass

        return None
