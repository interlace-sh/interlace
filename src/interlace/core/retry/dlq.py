"""
Dead Letter Queue (DLQ) for storing failed model executions.

Phase 2: Track failures for manual inspection and debugging.
"""

import json
import time
from dataclasses import asdict, dataclass
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.retry.dlq")


@dataclass
class DLQEntry:
    """
    Entry in the Dead Letter Queue.

    Stores information about a failed model execution for debugging.
    """

    # Model name that failed
    model_name: str

    # Exception type and message
    exception_type: str
    exception_message: str
    exception_traceback: str | None = None

    # Retry metadata
    total_attempts: int = 1
    retry_history: list[dict[str, Any]] | None = None

    # Timing information
    first_attempt_time: float | None = None
    last_attempt_time: float | None = None
    total_duration: float = 0.0

    # Model parameters (for reproducing failure)
    model_config: dict[str, Any] | None = None
    model_dependencies: list[str] | None = None

    # Run context
    run_id: str | None = None
    environment: str | None = None

    # Entry metadata
    dlq_timestamp: float | None = None
    dlq_id: str | None = None

    def __post_init__(self) -> None:
        """Set default values."""
        if self.retry_history is None:
            self.retry_history = []
        if self.model_config is None:
            self.model_config = {}
        if self.model_dependencies is None:
            self.model_dependencies = []
        if self.dlq_timestamp is None:
            self.dlq_timestamp = time.time()
        if self.first_attempt_time is None:
            self.first_attempt_time = time.time()
        if self.last_attempt_time is None:
            self.last_attempt_time = time.time()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DLQEntry":
        """Create from dictionary."""
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "DLQEntry":
        """Create from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


class DeadLetterQueue:
    """
    Dead Letter Queue for tracking failed model executions.

    Stores failed runs in state database for manual inspection and retry.
    Similar to AWS SQS Dead Letter Queue or Airflow's zombie task detection.

    Examples:
        >>> dlq = DeadLetterQueue(state_store)
        >>>
        >>> # Record a failure
        >>> entry = DLQEntry(
        ...     model_name="api_ingest",
        ...     exception_type="ConnectionError",
        ...     exception_message="Failed to connect to API",
        ...     total_attempts=5
        ... )
        >>> dlq.add(entry)
        >>>
        >>> # Query failures
        >>> failures = dlq.get_recent(limit=10)
        >>> for failure in failures:
        ...     print(f"{failure.model_name}: {failure.exception_message}")
        >>>
        >>> # Retry a failed run
        >>> entry = dlq.get_by_id(dlq_id)
        >>> # ... retry logic ...
        >>> dlq.mark_resolved(dlq_id)
    """

    def __init__(self, state_store: Any | None = None):
        """
        Initialize Dead Letter Queue.

        Args:
            state_store: StateStore instance for persistence (optional)
        """
        self.state_store = state_store
        self._in_memory_queue: list[DLQEntry] = []

    def add(self, entry: DLQEntry) -> str:
        """
        Add an entry to the DLQ.

        Args:
            entry: DLQEntry to add

        Returns:
            DLQ entry ID
        """
        # Generate ID if not provided
        if entry.dlq_id is None:
            entry.dlq_id = self._generate_id(entry)

        logger.warning(
            f"Adding to DLQ: {entry.model_name} failed after {entry.total_attempts} attempts - "
            f"{entry.exception_type}: {entry.exception_message}"
        )

        # Store in database if available
        if self.state_store:
            self._persist_entry(entry)
        else:
            # Fallback to in-memory storage
            self._in_memory_queue.append(entry)

        return entry.dlq_id

    def get_by_id(self, dlq_id: str) -> DLQEntry | None:
        """
        Get a DLQ entry by ID.

        Args:
            dlq_id: DLQ entry ID

        Returns:
            DLQEntry if found, None otherwise
        """
        if self.state_store:
            return self._fetch_entry_by_id(dlq_id)
        else:
            # Search in-memory queue
            for entry in self._in_memory_queue:
                if entry.dlq_id == dlq_id:
                    return entry
            return None

    def get_by_model(self, model_name: str, limit: int = 10) -> list[DLQEntry]:
        """
        Get DLQ entries for a specific model.

        Args:
            model_name: Model name to filter by
            limit: Maximum number of entries to return

        Returns:
            List of DLQEntry objects
        """
        if self.state_store:
            return self._fetch_entries_by_model(model_name, limit)
        else:
            # Filter in-memory queue
            entries = [e for e in self._in_memory_queue if e.model_name == model_name]
            return sorted(entries, key=lambda e: e.dlq_timestamp or 0.0, reverse=True)[:limit]

    def get_recent(self, limit: int = 10) -> list[DLQEntry]:
        """
        Get recent DLQ entries.

        Args:
            limit: Maximum number of entries to return

        Returns:
            List of DLQEntry objects, most recent first
        """
        if self.state_store:
            return self._fetch_recent_entries(limit)
        else:
            # Sort in-memory queue by timestamp
            return sorted(self._in_memory_queue, key=lambda e: e.dlq_timestamp or 0.0, reverse=True)[:limit]

    def mark_resolved(self, dlq_id: str, resolution_note: str | None = None) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: DLQ entry ID
            resolution_note: Optional note about how issue was resolved
        """
        logger.info(f"Marking DLQ entry {dlq_id} as resolved")

        if self.state_store:
            self._mark_entry_resolved(dlq_id, resolution_note)
        else:
            # Remove from in-memory queue
            self._in_memory_queue = [e for e in self._in_memory_queue if e.dlq_id != dlq_id]

    def clear_model(self, model_name: str) -> None:
        """
        Clear all DLQ entries for a model.

        Args:
            model_name: Model name to clear
        """
        logger.info(f"Clearing DLQ entries for model: {model_name}")

        if self.state_store:
            self._clear_model_entries(model_name)
        else:
            self._in_memory_queue = [e for e in self._in_memory_queue if e.model_name != model_name]

    def clear_all(self) -> None:
        """Clear all DLQ entries."""
        logger.warning("Clearing all DLQ entries")

        if self.state_store:
            self._clear_all_entries()
        else:
            self._in_memory_queue.clear()

    def get_stats(self) -> dict[str, Any]:
        """
        Get DLQ statistics.

        Returns:
            Dictionary with DLQ metrics
        """
        if self.state_store:
            return self._fetch_stats()
        else:
            # Calculate stats from in-memory queue
            total = len(self._in_memory_queue)
            by_model: dict[str, int] = {}
            by_exception: dict[str, int] = {}

            for entry in self._in_memory_queue:
                by_model[entry.model_name] = by_model.get(entry.model_name, 0) + 1
                by_exception[entry.exception_type] = by_exception.get(entry.exception_type, 0) + 1

            return {
                "total_entries": total,
                "entries_by_model": by_model,
                "entries_by_exception": by_exception,
            }

    # Private methods for database interaction

    def _generate_id(self, entry: DLQEntry) -> str:
        """Generate unique ID for DLQ entry."""
        import hashlib

        content = f"{entry.model_name}_{entry.dlq_timestamp}"
        return f"dlq_{hashlib.sha256(content.encode()).hexdigest()[:16]}"

    def _persist_entry(self, entry: DLQEntry) -> None:
        """Persist entry to state database."""
        if not self.state_store:
            return

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                # Fallback to in-memory
                self._in_memory_queue.append(entry)
                return

            from interlace.core.context import _execute_sql_internal

            # Build insert SQL
            dlq_table = "interlace.dead_letter_queue"

            # Escape and format values
            def sql_value(val: Any) -> str:
                if val is None:
                    return "NULL"
                elif isinstance(val, str):
                    return f"'{val.replace(chr(39), chr(39)+chr(39))}'"
                elif isinstance(val, bool):
                    # bool must be checked before int/float because
                    # isinstance(True, int) is True (bool is a subclass of int)
                    return "TRUE" if val else "FALSE"
                elif isinstance(val, (int, float)):
                    return str(val)
                elif isinstance(val, (list, dict)):
                    import json

                    return f"'{json.dumps(val).replace(chr(39), chr(39)+chr(39))}'"
                else:
                    return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"

            # Insert the entry
            _execute_sql_internal(
                conn,
                f"""
                INSERT INTO {dlq_table} (
                    dlq_id, model_name, exception_type, exception_message,
                    exception_traceback, total_attempts, retry_history,
                    first_attempt_time, last_attempt_time, total_duration,
                    model_config, model_dependencies, run_id, environment,
                    dlq_timestamp, is_resolved
                ) VALUES (
                    {sql_value(entry.dlq_id)},
                    {sql_value(entry.model_name)},
                    {sql_value(entry.exception_type)},
                    {sql_value(entry.exception_message)},
                    {sql_value(entry.exception_traceback)},
                    {sql_value(entry.total_attempts)},
                    {sql_value(entry.retry_history)},
                    {sql_value(entry.first_attempt_time)},
                    {sql_value(entry.last_attempt_time)},
                    {sql_value(entry.total_duration)},
                    {sql_value(entry.model_config)},
                    {sql_value(entry.model_dependencies)},
                    {sql_value(entry.run_id)},
                    {sql_value(entry.environment)},
                    {sql_value(entry.dlq_timestamp)},
                    FALSE
                )
                """,
            )
            logger.debug(f"Persisted DLQ entry {entry.dlq_id} to state database")
        except Exception as e:
            logger.warning(f"Failed to persist DLQ entry to database: {e}. Using in-memory storage.")
            self._in_memory_queue.append(entry)

    def _fetch_entry_by_id(self, dlq_id: str) -> DLQEntry | None:
        """Fetch entry from state database by ID."""
        if not self.state_store:
            return None

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return None

            dlq_table = "interlace.dead_letter_queue"
            escaped_id = dlq_id.replace("'", "''")

            result = conn.sql(
                f"SELECT * FROM {dlq_table} WHERE dlq_id = '{escaped_id}' AND is_resolved = FALSE"
            ).execute()

            if len(result) == 0:
                return None

            row = result.iloc[0]
            return self._row_to_entry(row)
        except Exception as e:
            logger.debug(f"Failed to fetch DLQ entry by ID: {e}")
            return None

    def _fetch_entries_by_model(self, model_name: str, limit: int) -> list[DLQEntry]:
        """Fetch entries from state database by model."""
        if not self.state_store:
            return []

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return []

            dlq_table = "interlace.dead_letter_queue"
            escaped_name = model_name.replace("'", "''")

            result = conn.sql(f"""
                SELECT * FROM {dlq_table}
                WHERE model_name = '{escaped_name}' AND is_resolved = FALSE
                ORDER BY dlq_timestamp DESC
                LIMIT {limit}
                """).execute()

            return [self._row_to_entry(row) for _, row in result.iterrows()]
        except Exception as e:
            logger.debug(f"Failed to fetch DLQ entries by model: {e}")
            return []

    def _fetch_recent_entries(self, limit: int) -> list[DLQEntry]:
        """Fetch recent entries from state database."""
        if not self.state_store:
            return []

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return []

            dlq_table = "interlace.dead_letter_queue"

            result = conn.sql(f"""
                SELECT * FROM {dlq_table}
                WHERE is_resolved = FALSE
                ORDER BY dlq_timestamp DESC
                LIMIT {limit}
                """).execute()

            return [self._row_to_entry(row) for _, row in result.iterrows()]
        except Exception as e:
            logger.debug(f"Failed to fetch recent DLQ entries: {e}")
            return []

    def _mark_entry_resolved(self, dlq_id: str, resolution_note: str | None) -> None:
        """Mark entry as resolved in state database."""
        if not self.state_store:
            return

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return

            from interlace.core.context import _execute_sql_internal

            dlq_table = "interlace.dead_letter_queue"
            escaped_id = dlq_id.replace("'", "''")
            escaped_note = (resolution_note or "").replace("'", "''")

            _execute_sql_internal(
                conn,
                f"""
                UPDATE {dlq_table}
                SET is_resolved = TRUE,
                    resolved_at = CURRENT_TIMESTAMP,
                    resolution_note = '{escaped_note}'
                WHERE dlq_id = '{escaped_id}'
                """,
            )
            logger.debug(f"Marked DLQ entry {dlq_id} as resolved")
        except Exception as e:
            logger.warning(f"Failed to mark DLQ entry as resolved: {e}")

    def _clear_model_entries(self, model_name: str) -> None:
        """Clear model entries from state database."""
        if not self.state_store:
            return

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return

            from interlace.core.context import _execute_sql_internal

            dlq_table = "interlace.dead_letter_queue"
            escaped_name = model_name.replace("'", "''")

            _execute_sql_internal(
                conn,
                f"DELETE FROM {dlq_table} WHERE model_name = '{escaped_name}'",
            )
            logger.debug(f"Cleared DLQ entries for model {model_name}")
        except Exception as e:
            logger.warning(f"Failed to clear DLQ entries for model: {e}")

    def _clear_all_entries(self) -> None:
        """Clear all entries from state database."""
        if not self.state_store:
            return

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return

            from interlace.core.context import _execute_sql_internal

            dlq_table = "interlace.dead_letter_queue"

            _execute_sql_internal(conn, f"DELETE FROM {dlq_table}")
            logger.debug("Cleared all DLQ entries")
        except Exception as e:
            logger.warning(f"Failed to clear all DLQ entries: {e}")

    def _fetch_stats(self) -> dict[str, Any]:
        """Fetch stats from state database."""
        if not self.state_store:
            return {
                "total_entries": 0,
                "entries_by_model": {},
                "entries_by_exception": {},
            }

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return {
                    "total_entries": 0,
                    "entries_by_model": {},
                    "entries_by_exception": {},
                }

            dlq_table = "interlace.dead_letter_queue"

            # Get total count
            total_result = conn.sql(f"SELECT COUNT(*) as cnt FROM {dlq_table} WHERE is_resolved = FALSE").execute()
            total = int(total_result.iloc[0]["cnt"])

            # Get counts by model
            model_result = conn.sql(f"""
                SELECT model_name, COUNT(*) as cnt FROM {dlq_table}
                WHERE is_resolved = FALSE
                GROUP BY model_name
                """).execute()
            by_model = {row["model_name"]: int(row["cnt"]) for _, row in model_result.iterrows()}

            # Get counts by exception type
            exc_result = conn.sql(f"""
                SELECT exception_type, COUNT(*) as cnt FROM {dlq_table}
                WHERE is_resolved = FALSE
                GROUP BY exception_type
                """).execute()
            by_exception = {row["exception_type"]: int(row["cnt"]) for _, row in exc_result.iterrows()}

            return {
                "total_entries": total,
                "entries_by_model": by_model,
                "entries_by_exception": by_exception,
            }
        except Exception as e:
            logger.debug(f"Failed to fetch DLQ stats: {e}")
            return {
                "total_entries": 0,
                "entries_by_model": {},
                "entries_by_exception": {},
            }

    def _row_to_entry(self, row: Any) -> DLQEntry:
        """Convert a database row to DLQEntry."""
        import json

        # Parse JSON fields
        retry_history = row.get("retry_history")
        if isinstance(retry_history, str):
            try:
                retry_history = json.loads(retry_history)
            except (json.JSONDecodeError, TypeError):
                retry_history = []

        model_config = row.get("model_config")
        if isinstance(model_config, str):
            try:
                model_config = json.loads(model_config)
            except (json.JSONDecodeError, TypeError):
                model_config = {}

        model_dependencies = row.get("model_dependencies")
        if isinstance(model_dependencies, str):
            try:
                model_dependencies = json.loads(model_dependencies)
            except (json.JSONDecodeError, TypeError):
                model_dependencies = []

        # Convert timestamp fields
        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return float(val)
            # Handle pandas Timestamp
            if hasattr(val, "timestamp"):
                return float(val.timestamp())
            return None

        return DLQEntry(
            model_name=row.get("model_name", ""),
            exception_type=row.get("exception_type", ""),
            exception_message=row.get("exception_message"),
            exception_traceback=row.get("exception_traceback"),
            total_attempts=int(row.get("total_attempts", 1)),
            retry_history=retry_history or [],
            first_attempt_time=to_float(row.get("first_attempt_time")),
            last_attempt_time=to_float(row.get("last_attempt_time")),
            total_duration=float(row.get("total_duration", 0.0)),
            model_config=model_config or {},
            model_dependencies=model_dependencies or [],
            run_id=row.get("run_id"),
            environment=row.get("environment"),
            dlq_timestamp=to_float(row.get("dlq_timestamp")),
            dlq_id=row.get("dlq_id"),
        )
