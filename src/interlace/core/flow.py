"""
Flow and Task tracking for Interlace execution.

A Flow represents a top-level execution request (equivalent to a Job).
A Task represents an individual model execution (equivalent to a Run).
"""

import uuid
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum


class FlowStatus(str, Enum):
    """Flow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
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
    trigger_id: Optional[str] = None
    status: FlowStatus = FlowStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    created_by: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Task tracking
    tasks: Dict[str, "Task"] = field(default_factory=dict)  # model_name -> Task

    def start(self):
        """Mark flow as started."""
        self.status = FlowStatus.RUNNING
        self.started_at = time.time()

    def complete(self, success: bool = True):
        """Mark flow as completed."""
        self.status = FlowStatus.COMPLETED if success else FlowStatus.FAILED
        self.completed_at = time.time()

    def get_duration(self) -> Optional[float]:
        """Get flow duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        elif self.started_at:
            return time.time() - self.started_at
        return None

    def get_summary(self) -> Dict[str, Any]:
        """Get flow summary statistics."""
        total_tasks = len(self.tasks)
        completed = sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in self.tasks.values() if t.status == TaskStatus.FAILED)
        skipped = sum(1 for t in self.tasks.values() if t.status == TaskStatus.SKIPPED)
        running = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
        pending = sum(
            1
            for t in self.tasks.values()
            if t.status in [TaskStatus.PENDING, TaskStatus.WAITING, TaskStatus.READY]
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
    parent_task_ids: List[str] = field(default_factory=list)  # Upstream tasks that triggered this

    status: TaskStatus = TaskStatus.PENDING
    attempt: int = 1
    max_attempts: int = 1

    # Timing
    enqueued_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    # Model configuration
    materialise: str = "table"
    strategy: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)

    # Results
    rows_processed: Optional[int] = None
    rows_ingested: Optional[int] = None  # Raw input count before strategy
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    schema_changes: int = 0

    # Errors
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    error_stacktrace: Optional[str] = None

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def enqueue(self):
        """Mark task as enqueued."""
        self.status = TaskStatus.PENDING
        self.enqueued_at = time.time()

    def mark_waiting(self):
        """Mark task as waiting for dependencies."""
        self.status = TaskStatus.WAITING

    def mark_ready(self):
        """Mark task as ready to execute (dependencies satisfied)."""
        self.status = TaskStatus.READY

    def start(self):
        """Mark task as started."""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

    def mark_materialising(self):
        """Mark task as materialising results."""
        self.status = TaskStatus.MATERIALISING

    def complete(self, success: bool = True):
        """Mark task as completed."""
        self.status = TaskStatus.COMPLETED if success else TaskStatus.FAILED
        self.completed_at = time.time()

    def skip(self):
        """Mark task as skipped."""
        self.status = TaskStatus.SKIPPED
        self.completed_at = time.time()

    def get_duration(self) -> Optional[float]:
        """Get task duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        elif self.started_at:
            return time.time() - self.started_at
        return None

    def get_wait_time(self) -> Optional[float]:
        """Get time spent waiting (enqueued to started)."""
        if self.enqueued_at and self.started_at:
            return self.started_at - self.enqueued_at
        return None

    def get_summary(self) -> Dict[str, Any]:
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
