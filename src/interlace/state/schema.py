"""Control-plane schema. One ordered migration list, applied under a lock."""

from __future__ import annotations

import sqlite3

_MIGRATIONS: list[str] = [
    # 0001 — snapshots, interval ledger, environment pointers
    """
    CREATE TABLE snapshots (
        name               TEXT NOT NULL,
        fingerprint        TEXT NOT NULL,
        local_fingerprint  TEXT NOT NULL,
        metadata_hash      TEXT NOT NULL,
        definition_sql     TEXT,
        physical_catalog   TEXT,
        physical_schema    TEXT NOT NULL,
        physical_name      TEXT NOT NULL,
        change_category    TEXT NOT NULL,
        created_at         TEXT NOT NULL,
        PRIMARY KEY (name, fingerprint)
    );

    CREATE TABLE intervals (
        name         TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        start_ts     TEXT NOT NULL,
        end_ts       TEXT NOT NULL,
        PRIMARY KEY (name, fingerprint, start_ts, end_ts)
    );

    CREATE TABLE environments (
        environment  TEXT NOT NULL,
        model_name   TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        promoted_at  TEXT NOT NULL,
        PRIMARY KEY (environment, model_name)
    );
    """,
    # 0002 — orchestration: durable run queue + per-trigger state
    """
    CREATE TABLE work_queue (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key  TEXT UNIQUE,
        flow_selector    TEXT NOT NULL,
        partition_start  TEXT,
        partition_end    TEXT,
        priority         INTEGER NOT NULL DEFAULT 0,
        state            TEXT NOT NULL DEFAULT 'queued',
        attempts         INTEGER NOT NULL DEFAULT 0,
        error            TEXT,
        enqueued_at      TEXT NOT NULL
    );

    CREATE TABLE trigger_state (
        trigger_id     TEXT PRIMARY KEY,
        last_fired_at  TEXT
    );
    """,
    # 0003 — durable event log (run/stream lifecycle; SSE replay spine)
    """
    CREATE TABLE event_log (
        seq      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts       TEXT NOT NULL,
        type     TEXT NOT NULL,
        entity   TEXT,
        payload  TEXT
    );
    """,
    # 0004 — API keys (scoped) for the HTTP service
    """
    CREATE TABLE api_keys (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL,
        key_hash    TEXT NOT NULL UNIQUE,
        scopes      TEXT NOT NULL,
        created_at  TEXT NOT NULL
    );
    """,
    # 0005 — data-quality check results (gate promotion; surfaced via API/UI)
    """
    CREATE TABLE check_results (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        environment  TEXT NOT NULL,
        model        TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        check_name   TEXT NOT NULL,
        check_type   TEXT NOT NULL,
        severity     TEXT NOT NULL,
        status       TEXT NOT NULL,
        failures     INTEGER NOT NULL DEFAULT 0,
        message      TEXT,
        executed_at  TEXT NOT NULL
    );
    CREATE INDEX idx_check_results_model ON check_results (model, id DESC);
    """,
    # 0006 — multi-engine: which named engine owns each snapshot's physical table
    """
    ALTER TABLE snapshots ADD COLUMN engine TEXT NOT NULL DEFAULT 'default';
    """,
    # 0007 — per-task worker: leases (crash reclaim), cooperative cancellation
    """
    ALTER TABLE work_queue ADD COLUMN lease_owner TEXT;
    ALTER TABLE work_queue ADD COLUMN lease_expires_at TEXT;
    ALTER TABLE work_queue ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0;
    """,
    # 0008 — indexes for the two hot growing tables (claim scans, run timelines)
    """
    CREATE INDEX idx_work_queue_state ON work_queue (state, priority DESC, id);
    CREATE INDEX idx_event_log_entity ON event_log (entity);
    """,
    # 0009 — restate runs: reprocess the window instead of catching up
    """
    ALTER TABLE work_queue ADD COLUMN restate INTEGER NOT NULL DEFAULT 0;
    """,
    # 0010 — promotion history: every promote snapshots the environment's FULL
    # mapping as one generation, so `rollback` can repoint views to any earlier
    # state (as long as gc hasn't reclaimed those snapshots)
    """
    CREATE TABLE promotion_history (
        environment  TEXT NOT NULL,
        generation   INTEGER NOT NULL,
        model_name   TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        promoted_at  TEXT NOT NULL,
        PRIMARY KEY (environment, generation, model_name)
    );
    CREATE INDEX idx_promotion_history_env ON promotion_history (environment, generation DESC);
    """,
    # 0011 — cross-process advisory locks (CLI apply vs daemon flusher/apply)
    """
    CREATE TABLE advisory_locks (
        name        TEXT PRIMARY KEY,
        owner       TEXT NOT NULL,
        expires_at  TEXT NOT NULL
    );
    """,
    # 0012 — physical DDL (indexes/constraints) tracked separately from the data fingerprint
    """
    ALTER TABLE snapshots ADD COLUMN physical_hash TEXT NOT NULL DEFAULT '';
    ALTER TABLE snapshots ADD COLUMN physical_objects TEXT NOT NULL DEFAULT '[]';
    """,
    # 0013 — Postgres CDC: confirmed LSN advances only after the stream watermark does
    """
    CREATE TABLE cdc_confirmed (
        stream  TEXT PRIMARY KEY,
        lsn     TEXT NOT NULL
    );

    CREATE TABLE cdc_pending (
        stream      TEXT NOT NULL,
        log_offset  INTEGER NOT NULL,
        lsn         TEXT NOT NULL,
        PRIMARY KEY (stream, log_offset)
    );
    """,
]


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply pending migrations, each in its own transaction with the version bump
    inside it — a crash can never commit DDL without advancing user_version, and
    two processes opening a fresh database serialise on BEGIN IMMEDIATE (the loser
    re-reads the version and skips)."""
    while True:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version >= len(_MIGRATIONS):
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            current = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if current != version:  # another process migrated while we waited
                conn.execute("ROLLBACK")
                continue
            for statement in _MIGRATIONS[version].split(";"):
                if statement.strip():
                    conn.execute(statement)
            conn.execute(f"PRAGMA user_version = {version + 1}")  # transactional in SQLite
            conn.commit()
        except Exception:
            conn.execute("ROLLBACK")
            raise
