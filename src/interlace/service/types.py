"""HTTP wire types (msgspec structs).

Litestar serialises these natively. Kept out of ``app.py`` so routes stay
handlers rather than a mixed type catalogue.
"""

from __future__ import annotations

import msgspec


class ModelInfo(msgspec.Struct):
    name: str
    output: str  # the materialisation value (virtual/view/ephemeral/table/file)
    materialise: str
    strategy: str
    is_terminal: bool  # materialise: table/file — delivered to an external destination
    fingerprint: str
    depends_on: list[str]
    tags: list[str]
    owner: str | None
    schedule: dict[str, str] | None
    engine: str = "default"
    language: str = "sql"  # "sql" | "python"
    has_checks: bool = False  # declares SQL or Python checks — the runs view flags per-model check status


class IndexInfo(msgspec.Struct):
    columns: list[str]
    name: str  # resolved object name (explicit, or il__<model>__…)
    unique: bool = False


class ConstraintInfo(msgspec.Struct):
    type: str
    name: str
    columns: list[str] = msgspec.field(default_factory=list)
    expression: str | None = None  # check
    reference: str | None = None  # foreign_key target, as written
    fields: list[str] = msgspec.field(default_factory=list)


class SchemaPolicyInfo(msgspec.Struct):
    """External-table drift policy. ``columns`` is unused on an owned snapshot."""

    columns: str = "additive"  # additive | reject | ignore
    indexes: str = "manage"  # manage | ignore
    constraints: str = "manage"


class ModelDetail(msgspec.Struct):
    name: str
    output: str
    materialise: str
    strategy: str
    is_terminal: bool
    fingerprint: str
    depends_on: list[str]
    upstream: list[str]
    downstream: list[str]
    columns: dict[str, list[str]]
    tags: list[str]
    owner: str | None
    schedule: dict[str, str] | None
    sql: str | None = None  # canonical SQL; None for Python models
    language: str = "sql"  # "sql" | "python"
    source: str | None = None  # dedented function source for Python models
    indexes: list[IndexInfo] = msgspec.field(default_factory=list)
    constraints: list[ConstraintInfo] = msgspec.field(default_factory=list)
    schema: SchemaPolicyInfo = msgspec.field(default_factory=SchemaPolicyInfo)


class Change(msgspec.Struct):
    name: str
    change_type: str
    category: str | None
    previous_fingerprint: str | None = None
    new_fingerprint: str | None = None
    impacted_columns: list[str] = msgspec.field(default_factory=list)
    new_sql: str | None = None
    previous_sql: str | None = None
    reused: bool = False  # output provably identical: recorded without a rebuild


class PlanResponse(msgspec.Struct):
    environment: str
    changes: list[Change]
    transfers: list[str] = msgspec.field(default_factory=list)  # explicit cross-engine movement
    physical: list[str] = msgspec.field(default_factory=list)  # "+ index il__orders__id"
    drift: list[str] = msgspec.field(default_factory=list)  # external-table drift; blocking drift is a 400


class RunInfo(msgspec.Struct):
    id: int
    flow_selector: list[str]
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None = None
    priority: int = 0
    partition: list[str] | None = None
    restate: bool = False
    # how the run came to be — the enqueue key's prefix names the trigger
    # (cron: / interval: / watch: / webhook: / api: / stream:)
    idempotency_key: str | None = None
    environment: str | None = None  # the env it built into (once it has succeeded)
    duration: float | None = None  # wall-clock seconds, run.started → terminal


class CreateRun(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    environment: str | None = None
    start: str | None = None  # ISO timestamp: backfill window start (incremental models)
    end: str | None = None  # ISO timestamp: backfill window end
    restate: bool = False  # reprocess the window instead of skipping filled intervals


class CreateRunResult(msgspec.Struct):
    enqueued: int
    models: list[str]


class EventInfo(msgspec.Struct):
    seq: int
    ts: str
    type: str
    entity: str | None
    payload: dict | None


class RunDetail(msgspec.Struct):
    id: int
    flow_selector: list[str]
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None
    priority: int
    partition: list[str] | None
    events: list[EventInfo]
    restate: bool = False
    idempotency_key: str | None = None


class EnvironmentInfo(msgspec.Struct):
    name: str
    models: int
    changed: int  # compiled models whose fingerprint differs from the one promoted here
    promoted_at: str | None = None  # when the environment last moved


class ApplyRequest(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    environment: str | None = None
    force: bool = False  # required to proceed when the plan has breaking changes
    forward_only: bool = False  # history-keeping models inherit their table; new logic applies ahead


class ApplyResponse(msgspec.Struct):
    environment: str
    built: list[str]
    promoted: int
    breaking: bool
    reused: list[str] = msgspec.field(default_factory=list)
    transfers: list[str] = msgspec.field(default_factory=list)
    # per-model row movement (inserted/updated/deleted) and build seconds
    rows: dict[str, dict[str, int]] = msgspec.field(default_factory=dict)
    timings: dict[str, float] = msgspec.field(default_factory=dict)
    gated: list[str] = msgspec.field(default_factory=list)  # terminals recorded but not delivered (env gate)
    checks: list[CheckOutcomeInfo] = msgspec.field(default_factory=list)  # so a UI apply can show check results


class CheckResultInfo(msgspec.Struct):
    id: int
    environment: str
    model: str
    fingerprint: str
    check_name: str
    check_type: str
    severity: str
    status: str
    failures: int
    message: str | None
    executed_at: str


class StreamInfo(msgspec.Struct):
    name: str
    schema: dict[str, str]
    table: str
    head: int  # highest offset accepted into the log
    watermark: int  # highest offset materialized into the warehouse
    pending: int  # head - watermark: durable events not yet in the warehouse
    on_schema_drift: str = "reject"
    retention: str | None = None  # age after which materialized events are swept


class StreamDetail(msgspec.Struct):
    name: str
    schema: dict[str, str]
    table: str
    head: int
    watermark: int
    pending: int
    idempotency_key: str | None
    recent: list[dict]  # latest payloads, newest last
    on_schema_drift: str = "reject"
    retention: str | None = None


class PublishResult(msgspec.Struct):
    """Ack for a durable append. Materialization is micro-batched: a flusher task
    coalesces publishes into one warehouse write moments later — poll the stream's
    ``watermark`` (GET /streams/{name}) to observe it land."""

    accepted: int
    deduplicated: int
    last_offset: int | None
    quarantined: int = 0  # events diverted to <stream>__quarantine (quarantine mode)


class StreamCommit(msgspec.Struct):
    """Advance a consumer group's committed offset. The token comes from the SSE lease frame."""

    group: str
    offset: int
    token: str


class StreamCommitResult(msgspec.Struct):
    group: str
    committed_offset: int


class QueryRequest(msgspec.Struct):
    sql: str
    limit: int = 500  # capped at 10_000; the console is for inspection, not extraction


class QueryResponse(msgspec.Struct):
    columns: list[str]
    types: list[str]
    rows: list[list]  # JSON-safe cells (non-scalar values stringified)
    row_count: int
    truncated: bool
    elapsed_ms: float


class ProfileColumn(msgspec.Struct):
    column: str
    type: str
    nulls: int
    distinct: int
    min: str | None = None
    max: str | None = None


class BuildInfo(msgspec.Struct):
    status: str  # done | failed | cancelled
    at: str
    seconds: float | None = None
    rows: dict[str, int] | None = None
    message: str | None = None
    statement: str | None = None


class SampleResponse(msgspec.Struct):
    """A bounded read of a model, or of the rows one check rejected."""

    available: bool
    message: str | None = None
    relation: str | None = None
    columns: list[str] = msgspec.field(default_factory=list)
    types: list[str] = msgspec.field(default_factory=list)
    rows: list[list] = msgspec.field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    profile: list[ProfileColumn] = msgspec.field(default_factory=list)
    last_build: BuildInfo | None = None


class EngineInfo(msgspec.Struct):
    name: str
    type: str
    dialect: str
    database: str  # credentials redacted
    default: bool


class ConnectionInfo(msgspec.Struct):
    name: str
    type: str  # "http" | "postgres"
    base_url: str | None = None
    headers: dict[str, str] | None = None  # secret-bearing values replaced with …
    dsn: str | None = None  # credentials redacted


class ScheduleInfo(msgspec.Struct):
    model: str
    kind: str  # "cron" | "every"
    expression: str
    next_fire: str | None
    last_fired: str | None


class ImpactColumn(msgspec.Struct):
    model: str
    column: str
    via: str  # the upstream column it was derived from, one hop up


class ImpactResponse(msgspec.Struct):
    source: str  # "model.column"
    impacted: list[ImpactColumn]  # downstream columns transitively derived from it
    opaque_consumers: list[str]  # models reading the source whole (Python / * projections)


class LineageModel(msgspec.Struct):
    name: str
    output: str
    strategy: str
    engine: str
    tags: list[str]
    columns: list[str]  # output columns: warehouse-described, else parsed from the AST
    types: dict[str, str] = msgspec.field(default_factory=dict)  # column -> engine type (when described)
    has_schedule: bool = False
    has_checks: bool = False


class LineageStream(msgspec.Struct):
    name: str  # keyed "streams.<name>" in edges/columns to match SQL table refs
    stream: str  # the bare stream name
    columns: list[str]
    types: dict[str, str] = msgspec.field(default_factory=dict)
    consumers: list[str] = msgspec.field(default_factory=list)  # models reading it directly


class LineageResponse(msgspec.Struct):
    models: list[LineageModel]
    edges: list[list[str]]  # [upstream, downstream]
    # model -> column -> [[upstream_model, upstream_column], ...]
    columns: dict[str, dict[str, list[list[str]]]]
    streams: list[LineageStream] = msgspec.field(default_factory=list)


class RunChecksRequest(msgspec.Struct):
    environment: str | None = None
    selectors: list[str] = msgspec.field(default_factory=list)


class CheckOutcomeInfo(msgspec.Struct):
    model: str
    name: str
    check_type: str
    severity: str
    status: str
    failures: int
    message: str | None = None


class RunChecksResponse(msgspec.Struct):
    environment: str
    outcomes: list[CheckOutcomeInfo]
    skipped: list[str]  # declared but not promoted in this environment
    passed: int
    blocking_failures: int


class ApiKeyInfo(msgspec.Struct):
    name: str
    scopes: list[str]
    created_at: str


class CreateApiKey(msgspec.Struct):
    name: str
    scopes: list[str] = msgspec.field(default_factory=lambda: ["read"])


class RollbackRequest(msgspec.Struct):
    generation: int | None = None  # default: the generation before the latest


class GcRequest(msgspec.Struct):
    grace: str = "7d"  # keep unreferenced snapshots younger than this
    dry_run: bool = False


class GcResponse(msgspec.Struct):
    removed_snapshots: int
    dropped_tables: list[str]
    kept_snapshots: int
    dry_run: bool


class ResetRequest(msgspec.Struct):
    confirm: bool = False  # required unless dry_run
    dry_run: bool = False


class ResetResponse(msgspec.Struct):
    dropped_views: list[str]
    dropped_schemas: list[str]
    cleared_snapshots: int
    kept_terminals: list[str]
    environments: list[str]
    stream_log_cleared: bool
    dry_run: bool


class HookResult(msgspec.Struct):
    model: str
    idempotency_key: str
    enqueued: bool


class FixtureTestRequest(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    update_golden: bool = False


class FixtureTestResponse(msgspec.Struct):
    ok: bool
    passed: list[str]
    messages: list[str]
