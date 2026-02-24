"""
Flow and Task tracking for Interlace execution.

A Flow represents a top-level execution request (equivalent to a Job).
A Task represents an individual model execution (equivalent to a Run).
"""

import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class FlowStatus(StrEnum):
    """Flow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(StrEnum):
    """Task execution status."""

    PENDING = "pending"
    WAITING = "waiting"  # Waiting for dependencies
    READY = "ready"  # Dependencies satisfied, ready to execute
    RUNNING = "running"  # Executing model code
    MATERIALISING = "materialising"  # Materializing results
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Flow:
    """
    Flow represents a top-level execution request.

    Equivalent to a Job - groups multiple task executions.
    """

    flow_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    trigger_type: str = "cli"  # 'schedule', 'cli', 'api', 'event', 'webhook'
    trigger_id: str | None = None
    status: FlowStatus = FlowStatus.PENDING
    started_at: float | None = None
    completed_at: float | None = None
    created_by: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    model_selection: str | None = None  # NULL = all models, otherwise the selection expression

    # Task tracking
    tasks: dict[str, "Task"] = field(default_factory=dict)  # model_name -> Task

    def start(self) -> None:
        """Mark flow as started."""
        self.status = FlowStatus.RUNNING
        self.started_at = time.time()

    def complete(self, success: bool = True) -> None:
        """Mark flow as completed."""
        self.status = FlowStatus.COMPLETED if success else FlowStatus.FAILED
        self.completed_at = time.time()

    def get_duration(self) -> float | None:
        """Get flow duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        elif self.started_at:
            return time.time() - self.started_at
        return None

    def get_summary(self) -> dict[str, Any]:
        """Get flow summary statistics."""
        total_tasks = len(self.tasks)
        completed = sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in self.tasks.values() if t.status == TaskStatus.FAILED)
        skipped = sum(1 for t in self.tasks.values() if t.status == TaskStatus.SKIPPED)
        running = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
        pending = sum(
            1 for t in self.tasks.values() if t.status in [TaskStatus.PENDING, TaskStatus.WAITING, TaskStatus.READY]
        )

        return {
            "flow_id": self.flow_id,
            "status": self.status.value,
            "trigger_type": self.trigger_type,
            "total_tasks": total_tasks,
            "completed": completed,
            "failed": failed,
            "skipped": skipped,
            "running": running,
            "pending": pending,
            "duration": self.get_duration(),
        }


@dataclass
class Task:
    """
    Task represents an individual model execution.

    Equivalent to a Run - tracks a single model execution within a flow.
    """

    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    flow_id: str = ""
    model_name: str = ""
    schema_name: str = ""
    parent_task_ids: list[str] = field(default_factory=list)  # Upstream tasks that triggered this

    status: TaskStatus = TaskStatus.PENDING
    attempt: int = 1
    max_attempts: int = 1

    # Timing
    enqueued_at: float | None = None
    started_at: float | None = None
    completed_at: float | None = None

    # Model configuration
    materialise: str = "table"
    strategy: str | None = None
    dependencies: list[str] = field(default_factory=list)

    # Results
    rows_processed: int | None = None
    rows_ingested: int | None = None  # Raw input count before strategy
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    schema_changes: int = 0

    # Errors
    error_type: str | None = None
    error_message: str | None = None
    error_stacktrace: str | None = None

    # Skip tracking
    skipped_reason: str | None = None  # NULL = ran normally, e.g. 'no_changes', 'cached', 'upstream_failed'

    # Metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    def enqueue(self) -> None:
        """Mark task as enqueued."""
        self.status = TaskStatus.PENDING
        self.enqueued_at = time.time()

    def mark_waiting(self) -> None:
        """Mark task as waiting for dependencies."""
        self.status = TaskStatus.WAITING

    def mark_ready(self) -> None:
        """Mark task as ready to execute (dependencies satisfied)."""
        self.status = TaskStatus.READY

    def start(self) -> None:
        """Mark task as started."""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

    def mark_materialising(self) -> None:
        """Mark task as materialising results."""
        self.status = TaskStatus.MATERIALISING

    def complete(self, success: bool = True) -> None:
        """Mark task as completed."""
        self.status = TaskStatus.COMPLETED if success else TaskStatus.FAILED
        self.completed_at = time.time()

    def skip(self) -> None:
        """Mark task as skipped."""
        self.status = TaskStatus.SKIPPED
        self.completed_at = time.time()

    def get_duration(self) -> float | None:
        """Get task duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        elif self.started_at:
            return time.time() - self.started_at
        return None

    def get_wait_time(self) -> float | None:
        """Get time spent waiting (enqueued to started)."""
        if self.enqueued_at and self.started_at:
            return self.started_at - self.enqueued_at
        return None

    def get_summary(self) -> dict[str, Any]:
        """Get task summary."""
        return {
            "task_id": self.task_id,
            "model_name": self.model_name,
            "status": self.status.value,
            "duration": self.get_duration(),
            "wait_time": self.get_wait_time(),
            "rows_processed": self.rows_processed,
            "schema_changes": self.schema_changes,
            "error": self.error_message if self.status == TaskStatus.FAILED else None,
        }
