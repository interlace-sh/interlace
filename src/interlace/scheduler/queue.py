"""The durable work queue contract.

Tasks live in the state DB, not in memory, so a crash never loses queued work and
adding worker processes later needs no redesign. The single-node SQLite backend
claims with ``BEGIN IMMEDIATE``; the Postgres backend uses ``FOR UPDATE SKIP
LOCKED`` — identical semantics behind this Protocol, verified by one conformance
suite.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class TaskState(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Command(Enum):
    """Returned from a heartbeat to signal cooperative control."""

    CONTINUE = "continue"
    CANCEL = "cancel"


@dataclass(frozen=True)
class TaskSpec:
    """A unit of work to enqueue. ``concurrency_key`` serialises same-key tasks."""

    flow_id: str
    kind: str  # "model" | "stream_flush" | "check" | ...
    payload: dict[str, Any]
    priority: int = 0
    concurrency_key: str | None = None
    max_attempts: int = 3
    timeout_s: float | None = None


@dataclass
class ClaimedTask:
    """A task leased to a worker, carrying the fencing token used on finish/heartbeat."""

    task_id: str
    spec: TaskSpec
    lease_token: str
    attempt: int
    state: TaskState = TaskState.RUNNING


@dataclass
class TaskResult:
    succeeded: bool
    error: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


class WorkQueue(Protocol):
    """Durable, lease-based task queue. Backend-agnostic (SQLite or Postgres)."""

    async def enqueue(self, task: TaskSpec) -> str: ...

    async def claim(self, worker_id: str, slots: int) -> list[ClaimedTask]: ...

    async def heartbeat(self, task_id: str, lease_token: str) -> Command: ...

    async def finish(self, task_id: str, lease_token: str, result: TaskResult) -> None: ...
