"""
Run execution endpoints.
"""

import asyncio
import json
import time
from typing import Any

from aiohttp import web

from interlace.service.api.errors import (
    ErrorCode,
    NotFoundError,
    ValidationError,
)
from interlace.service.api.handlers import BaseHandler


class RunsHandler(BaseHandler):
    """Handler for triggering and managing runs."""

    async def create(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/runs

        Trigger a new execution.

        Request body:
            models: List of model names to run (null = all)
            force: Force execution even if no changes detected
            trigger_metadata: Optional metadata for the run

        Returns 202 Accepted with run details.
        """
        try:
            body = await request.json()
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON in request body: {e.msg}") from e
        except Exception:
            # Empty body is valid for this endpoint (all models will run)
            body = {}

        model_names = body.get("models")
        force = body.get("force", False)
        trigger_metadata = body.get("trigger_metadata", {})
        since = body.get("since")
        until = body.get("until")

        # Validate force parameter is boolean
        if not isinstance(force, bool):
            raise ValidationError("'force' must be a boolean (true/false)")

        # Validate backfill parameters
        if since is not None and not isinstance(since, str):
            raise ValidationError("'since' must be a string (e.g. '2024-01-01')")
        if until is not None and not isinstance(until, str):
            raise ValidationError("'until' must be a string (e.g. '2024-06-30')")

        # Backfill implies forced re-execution
        if since is not None:
            force = True

        # Validate model names if provided
        if model_names:
            if not isinstance(model_names, list):
                raise ValidationError("'models' must be a list of model names")

            invalid_models = [m for m in model_names if m not in self.models]
            if invalid_models:
                raise ValidationError(
                    f"Unknown models: {invalid_models}",
                    details={"invalid_models": invalid_models},
                )

        # Resolve models with dependencies
        selected_models = self._resolve_models_with_deps(model_names)

        # Trigger execution in background
        run_id = await self._trigger_run(selected_models, force, trigger_metadata, since=since, until=until)

        return await self.json_response(
            {
                "run_id": run_id,
                "status": "accepted",
                "models": list(selected_models.keys()),
                "estimated_tasks": len(selected_models),
            },
            status=202,
            request=request,
        )

    async def cancel(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/runs/{run_id}/cancel

        Cancel a running execution.
        """
        run_id = request.match_info["run_id"]

        # Check if this is the current running flow
        if hasattr(self.service, "flow") and self.service.flow:
            if self.service.flow.flow_id == run_id:
                # Attempt to cancel
                if hasattr(self.service, "cancel_current_run"):
                    await self.service.cancel_current_run()
                    return await self.json_response(
                        {"status": "cancelled", "run_id": run_id},
                        request=request,
                    )

        # Check if already completed
        if self.state_store:
            # Could check state store for completed flows
            pass

        raise NotFoundError("Run", run_id, ErrorCode.RUN_NOT_FOUND)

    async def status(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/runs/{run_id}/status

        Get real-time status of a running execution.
        """
        run_id = request.match_info["run_id"]

        # Check current flow
        if hasattr(self.service, "flow") and self.service.flow:
            if self.service.flow.flow_id == run_id:
                flow = self.service.flow
                import time

                elapsed = None
                if flow.started_at:
                    elapsed = round(time.time() - flow.started_at, 2)

                # Compute progress with defensive checks
                tasks = flow.tasks if hasattr(flow, "tasks") and flow.tasks else {}
                total = len(tasks)

                def get_task_status(task: Any) -> str:
                    """Safely get task status value."""
                    if hasattr(task, "status") and hasattr(task.status, "value"):
                        return task.status.value  # type: ignore[no-any-return]
                    return "unknown"

                completed = sum(1 for t in tasks.values() if get_task_status(t) == "completed")
                failed = sum(1 for t in tasks.values() if get_task_status(t) == "failed")
                running = sum(1 for t in tasks.values() if get_task_status(t) == "running")
                pending = total - completed - failed - running

                # Build task list with defensive attribute access
                task_list = []
                for t in tasks.values():
                    task_info = {
                        "model_name": getattr(t, "model_name", "unknown"),
                        "status": get_task_status(t),
                        "rows_processed": getattr(t, "rows_processed", None),
                        "duration_seconds": (t.get_duration() if hasattr(t, "get_duration") else None),
                    }
                    task_list.append(task_info)

                return await self.json_response(
                    {
                        "run_id": run_id,
                        "status": (
                            flow.status.value
                            if hasattr(flow, "status") and hasattr(flow.status, "value")
                            else "unknown"
                        ),
                        "started_at": getattr(flow, "started_at", None),
                        "elapsed_seconds": elapsed,
                        "progress": {
                            "total_tasks": total,
                            "completed": completed,
                            "failed": failed,
                            "running": running,
                            "pending": pending,
                        },
                        "tasks": task_list,
                    },
                    request=request,
                )

        raise NotFoundError("Run", run_id, ErrorCode.RUN_NOT_FOUND)

    def _resolve_models_with_deps(self, model_names: list[str] | None) -> dict[str, dict[str, Any]]:
        """
        Resolve requested models with their dependencies (recursive).

        If model_names is None, returns all models.
        """
        if model_names is None:
            return dict(self.models)

        # Start with requested models
        all_models = set(model_names)

        # Recursively add dependencies
        if self.graph:

            def resolve_recursive(name: str, visited: set) -> None:
                """Recursively resolve all dependencies."""
                if name in visited:
                    return  # Avoid cycles
                visited.add(name)
                deps = self.graph.get_dependencies(name)
                if deps:
                    for dep in deps:
                        all_models.add(dep)
                        resolve_recursive(dep, visited)

            visited: set = set()
            for name in model_names:
                resolve_recursive(name, visited)

        return {k: v for k, v in self.models.items() if k in all_models}

    async def _trigger_run(
        self,
        models: dict[str, dict[str, Any]],
        force: bool,
        metadata: dict[str, Any],
        since: str | None = None,
        until: str | None = None,
    ) -> str:
        """Trigger execution in background and return run ID."""
        import uuid

        from interlace.core.executor import Executor
        from interlace.core.flow import Flow, FlowStatus, Task, TaskStatus

        run_id = f"run_{uuid.uuid4().hex[:12]}"

        # Include backfill info in flow metadata
        flow_metadata = {**metadata}
        if since is not None:
            flow_metadata["since"] = since
        if until is not None:
            flow_metadata["until"] = until

        # Create Flow object with tasks
        flow = Flow(
            flow_id=run_id,
            trigger_type="api",
            metadata=flow_metadata,
        )

        # Create tasks for each model
        for model_name, model_info in models.items():
            task = Task(
                task_id=f"task_{uuid.uuid4().hex[:8]}",
                flow_id=run_id,
                model_name=model_name,
                schema_name=model_info.get("schema", "public"),
                materialise=model_info.get("materialise", "table"),
                strategy=model_info.get("strategy"),
                dependencies=model_info.get("depends_on", []),
            )
            task.enqueue()
            flow.tasks[model_name] = task

        # Track in concurrent flows dictionary (thread-safe for multiple concurrent runs)
        self.service.flows_in_progress[run_id] = flow
        # Also set as current flow for backward compatibility
        self.service.flow = flow

        async def _execute() -> None:
            try:
                flow.start()

                # Inject backfill overrides into a config copy
                exec_config = dict(self.config)
                if since is not None:
                    exec_config["_since"] = since
                if until is not None:
                    exec_config["_until"] = until
                executor = Executor(exec_config)

                # Emit flow started event
                if self.event_bus:
                    await self.event_bus.emit(
                        "flow.started",
                        {"flow_id": run_id, "models": list(models.keys())},
                        flow_id=run_id,
                    )

                results = await executor.execute_dynamic(models, self.graph, force=force)

                # Update tasks from execution results
                has_failures = False
                for model_name, task in flow.tasks.items():
                    if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED]:
                        # Already finalized by executor
                        if task.status in [TaskStatus.FAILED, TaskStatus.SKIPPED]:
                            has_failures = True
                        continue

                    result = results.get(model_name, {})
                    result_status = result.get("status", "success")

                    if result_status == "error":
                        has_failures = True
                        task.complete(success=False)
                        task.error_message = result.get("error")
                        task.error_stacktrace = result.get("traceback") or result.get("error_stacktrace")
                    elif result_status == "skipped":
                        has_failures = True
                        task.skip()
                        task.error_message = result.get("error")
                    else:
                        task.complete(success=True)
                        task.rows_processed = result.get("rows")

                flow.complete(success=not has_failures)

                # Build enriched event data
                summary = flow.get_summary()
                total_rows = sum(t.rows_processed for t in flow.tasks.values() if t.rows_processed is not None)
                event_type = "flow.failed" if has_failures else "flow.completed"
                event_data = {
                    "flow_id": run_id,
                    "status": flow.status.value,
                    "completed_tasks": summary["completed"],
                    "failed_tasks": summary["failed"],
                    "total_tasks": summary["total_tasks"],
                    "duration_seconds": flow.get_duration(),
                    "total_rows": total_rows,
                }

                # Emit flow event
                if self.event_bus:
                    await self.event_bus.emit(
                        event_type,
                        event_data,
                        flow_id=run_id,
                    )
            except Exception as e:
                # Mark flow as failed
                flow.status = FlowStatus.FAILED
                flow.completed_at = time.time()

                # Emit flow failed event
                if self.event_bus:
                    await self.event_bus.emit(
                        "flow.failed",
                        {"flow_id": run_id, "error": str(e)},
                        flow_id=run_id,
                    )
            finally:
                # Save to history
                self.service.save_flow_to_history(flow)
                # Remove from in-progress flows
                self.service.flows_in_progress.pop(run_id, None)
                # Clear legacy flow attribute if it's this one
                if self.service.flow and self.service.flow.flow_id == run_id:
                    self.service.flow = None

        # Run in background
        asyncio.create_task(_execute())

        return run_id
