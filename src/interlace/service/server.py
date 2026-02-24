"""
Interlace long-running service (HTTP APIs + background runners).

Provides:
- REST API for admin UI and programmatic access
- Real-time updates via Server-Sent Events (SSE)
- Background scheduler for cron/interval model execution
- SFTP sync job triggers
"""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from typing import Any

import ibis
from aiohttp import web

from interlace.connections.manager import get_connection as get_named_connection
from interlace.core.context import _execute_sql_internal
from interlace.core.executor import Executor
from interlace.core.initialization import InitializationError, initialize
from interlace.service.api import EventBus, setup_routes
from interlace.service.api.middleware import error_middleware, setup_auth, setup_cors
from interlace.service.api.routes import setup_legacy_routes
from interlace.service.cron_parser import CronParseError, next_fire_time_cron
from interlace.sync.sftp_sync import run_sftp_sync_job
from interlace.sync.types import ProcessorSpec, SFTPSyncJob
from interlace.utils.logging import get_logger

logger = get_logger("interlace.service")


def _job_from_dict(job_dict: dict[str, Any]) -> SFTPSyncJob:
    processors = [
        ProcessorSpec(name=p["name"], config=p.get("config"))
        for p in (job_dict.get("processors") or [])
        if isinstance(p, dict) and "name" in p
    ]
    return SFTPSyncJob(
        job_id=job_dict["job_id"],
        sftp_connection=job_dict["sftp_connection"],
        remote_glob=job_dict["remote_glob"],
        destination_connection=job_dict["destination_connection"],
        destination_prefix=job_dict.get("destination_prefix", ""),
        processors=processors,
        max_files_per_run=int(job_dict.get("max_files_per_run", 200)),
        change_detection=str(job_dict.get("change_detection", "mtime_size")),
        work_dir=str(job_dict.get("work_dir", "/tmp/interlace-sync")),
    )


class InterlaceService:
    def __init__(self, project_dir: Path, env: str | None, verbose: bool):
        self.project_dir = Path(project_dir)
        self.env = env
        self.verbose = verbose

        self.config: dict[str, Any] = {}
        self.state_store: Any = None
        self.models: dict[str, dict[str, Any]] = {}
        self.graph: Any = None

        # Event bus for real-time updates
        self.event_bus: EventBus = EventBus()

        # Current flow for tracking execution state (deprecated, kept for backward compat)
        self.flow: Any = None

        # Concurrent flows tracking: keyed by run_id to prevent race conditions
        self.flows_in_progress: dict[str, Any] = {}

        # In-memory flow history (newest first, limited size)
        self.flow_history: list = []
        self.max_flow_history = 100

        # Scheduler state
        self._scheduler_running = False
        self._scheduler_next_fire: dict[str, float] = {}  # model_name -> next fire ts

        self._run_lock = asyncio.Lock()
        self._background_tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()

    def save_flow_to_history(self, flow: Any) -> None:
        """Save a completed flow to in-memory history."""
        # Add to front of list (newest first)
        self.flow_history.insert(0, flow)
        # Trim to max size
        if len(self.flow_history) > self.max_flow_history:
            self.flow_history = self.flow_history[: self.max_flow_history]

    def initialize(self) -> None:
        config_obj, state_store, all_models, graph = initialize(self.project_dir, env=self.env, verbose=self.verbose)
        self.config = config_obj.data
        self.state_store = state_store
        self.models = all_models
        self.graph = graph
        logger.info(f"Initialized with {len(all_models)} models")

    def start_background_tasks(self, *, enable_scheduler: bool = True) -> None:
        """
        Start background loops (sync jobs + scheduler).

        Phase B/C: scheduler is interval/cron-based and uses StateStore as durability layer.
        """
        self._stopping.clear()
        # Sync jobs loop (interval-based per job)
        self._background_tasks.append(asyncio.create_task(self._sync_loop()))
        if enable_scheduler:
            self._scheduler_running = True
            self._background_tasks.append(asyncio.create_task(self._model_schedule_loop()))

    async def stop_background_tasks(self) -> None:
        self._stopping.set()
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    def get_scheduler_status(self) -> dict[str, Any]:
        """Return current scheduler status for API / CLI observability.

        Returns a dict with ``running``, ``scheduled_models`` count, and per-model
        details including schedule config, next fire time, and last run time.
        """
        from datetime import datetime as dt
        from zoneinfo import ZoneInfo

        models_status = []
        for name, info in self.models.items():
            sched = info.get("schedule")
            if not isinstance(sched, dict):
                continue
            next_ts = self._scheduler_next_fire.get(name)
            entry: dict[str, Any] = {
                "model": name,
                "schedule": sched,
                "next_fire_at": (dt.fromtimestamp(next_ts, tz=ZoneInfo("UTC")).isoformat() if next_ts else None),
            }
            # Read persisted last_run_at
            if self.state_store:
                try:
                    schema_name = info.get("schema", "public")
                    last_run_dt = self.state_store.get_model_last_run_at(name, schema_name)
                    entry["last_run_at"] = last_run_dt.isoformat() if last_run_dt else None
                except Exception:
                    entry["last_run_at"] = None
            else:
                entry["last_run_at"] = None
            models_status.append(entry)

        return {
            "running": self._scheduler_running,
            "scheduled_models": len(models_status),
            "models": models_status,
        }

    async def _sync_loop(self) -> None:
        """
        Periodically run configured sync jobs.

        Config:
          sync:
            jobs:
              - ...
                schedule:
                  every_s: 60
                  # OR cron: \"*/5 * * * *\"
        """
        jobs = self.config.get("sync", {}).get("jobs", []) or []
        if not jobs:
            return

        next_run: dict[str, float] = {}
        while not self._stopping.is_set():
            now = time.time()
            for job_dict in jobs:
                try:
                    job_id = str(job_dict.get("job_id"))
                    sched = job_dict.get("schedule") or {"every_s": 300}
                    due = next_run.get(job_id, 0.0)
                    if now < due:
                        continue

                    job = _job_from_dict(job_dict)
                    # Run sync in a thread to avoid blocking event loop (paramiko is blocking)
                    await asyncio.to_thread(run_sftp_sync_job, job, config=self.config, state_store=self.state_store)

                    # Compute next run
                    if "cron" in sched:
                        try:
                            next_run[job_id] = next_fire_time_cron(
                                str(sched["cron"]),
                                now=datetime_from_ts(now, tz=sched.get("timezone")),
                                timezone=sched.get("timezone"),
                            ).timestamp()
                        except CronParseError:
                            next_run[job_id] = now + 60.0
                    else:
                        every_s = float(sched.get("every_s", 300))
                        next_run[job_id] = now + max(1.0, every_s)
                except Exception as e:
                    logger.error(f"sync loop error: {e}")
                    # backoff a little per job
                    next_run[str(job_dict.get("job_id"))] = now + 30.0

            await asyncio.sleep(0.5)

    async def _model_schedule_loop(self) -> None:
        """
        Periodically trigger scheduled model runs with persistence.

        On startup, reads ``last_run_at`` from the StateStore (model_metadata
        table) for each scheduled model and computes the next fire time.  This
        prevents duplicate runs after a service restart.

        Misfire policy: **run-once** -- if the computed next fire time is in the
        past (i.e. the service was down when the model was due), the model is
        executed once immediately rather than firing every missed occurrence.

        After each run, ``last_run_at`` is persisted back to the StateStore so
        state survives restarts.

        Config shape supported:
          schedule: { cron: "0 * * * *", timezone: "UTC", max_concurrency: 1 }
          schedule: { every_s: 600, max_concurrency: 1 }
        """
        from datetime import datetime as dt
        from zoneinfo import ZoneInfo

        # Build schedule plans for models that declare schedule
        scheduled_models: dict[str, dict[str, Any]] = {}
        for name, info in self.models.items():
            sched = info.get("schedule")
            if isinstance(sched, dict):
                scheduled_models[name] = sched

        if not scheduled_models:
            return

        # --- Restore persisted state and compute initial next_fire ---
        next_fire: dict[str, float] = {}
        now = time.time()

        for model_name, sched in scheduled_models.items():
            schema_name = self.models[model_name].get("schema", "public")
            last_run: float | None = None

            # Read last_run_at from StateStore (survives restarts)
            if self.state_store:
                try:
                    last_run_dt = self.state_store.get_model_last_run_at(model_name, schema_name)
                    if last_run_dt is not None:
                        if last_run_dt.tzinfo is None:
                            last_run_dt = last_run_dt.replace(tzinfo=ZoneInfo("UTC"))
                        last_run = last_run_dt.timestamp()
                except Exception as e:
                    logger.debug(f"Could not read last_run_at for {model_name}: {e}")

            if last_run is not None:
                # Compute next fire relative to persisted last run
                computed = self._compute_next_fire(sched, last_run)
                if computed <= now:
                    # Missed job -- misfire policy: run once immediately
                    logger.info(
                        f"Scheduler: missed job detected for {model_name} "
                        f"(was due at {dt.fromtimestamp(computed, tz=ZoneInfo('UTC')).isoformat()}), "
                        f"running once now"
                    )
                    next_fire[model_name] = 0.0  # run immediately on next tick
                else:
                    next_fire[model_name] = computed
                    logger.debug(
                        f"Scheduler: {model_name} next fire at "
                        f"{dt.fromtimestamp(computed, tz=ZoneInfo('UTC')).isoformat()}"
                    )
            else:
                # No prior run recorded -- run immediately
                next_fire[model_name] = 0.0

        # Expose schedule state for observability
        self._scheduler_next_fire = dict(next_fire)

        # --- Main loop ---
        while not self._stopping.is_set():
            now = time.time()
            for model_name, sched in scheduled_models.items():
                due = next_fire.get(model_name, 0.0)
                if now < due:
                    continue
                try:
                    # Trigger a run for this model (and its deps)
                    await self._enqueue_run([model_name], force=False)

                    # Persist last_run_at
                    run_ts = dt.fromtimestamp(now, tz=ZoneInfo("UTC"))
                    if self.state_store:
                        try:
                            schema_name = self.models[model_name].get("schema", "public")
                            self.state_store.set_model_last_run_at(model_name, schema_name, run_at=run_ts)
                        except Exception as e:
                            logger.warning(f"Could not persist last_run_at for {model_name}: {e}")

                    # Compute next fire time
                    next_fire[model_name] = self._compute_next_fire(sched, now)
                    self._scheduler_next_fire[model_name] = next_fire[model_name]
                except Exception as e:
                    logger.error(f"schedule loop error model={model_name}: {e}")
                    next_fire[model_name] = now + 60.0
                    self._scheduler_next_fire[model_name] = next_fire[model_name]

            await asyncio.sleep(0.5)

    def _compute_next_fire(self, sched: dict[str, Any], reference_ts: float) -> float:
        """Compute next fire timestamp from a schedule spec and a reference time.

        Args:
            sched: Schedule dict (``{cron: "...", timezone: "..."}`` or ``{every_s: N}``)
            reference_ts: Unix timestamp to compute "next" relative to

        Returns:
            Unix timestamp of next fire time
        """
        if "cron" in sched:
            try:
                return next_fire_time_cron(
                    str(sched["cron"]),
                    now=datetime_from_ts(reference_ts, tz=sched.get("timezone")),
                    timezone=sched.get("timezone"),
                ).timestamp()
            except CronParseError:
                return reference_ts + 60.0
        else:
            every_s = float(sched.get("every_s", 3600))
            return reference_ts + max(1.0, every_s)

    async def _enqueue_run(self, model_names: list[str] | None, force: bool) -> None:
        """
        Enqueue a run as a background task (serialized by _run_lock for now).

        Future: allow concurrency with per-run ids + DB-backed queue.
        """
        requested_set = set(model_names or [])
        if requested_set and self.graph:
            for m in list(requested_set):
                requested_set.update(self.graph.get_dependencies(m))
        selected_models = (
            self.models if not requested_set else {k: v for k, v in self.models.items() if k in requested_set}
        )

        async def _run_task() -> None:
            async with self._run_lock:
                ex = Executor(self.config)
                try:
                    await ex.execute_dynamic(selected_models, self.graph, force=force)
                finally:
                    ex.executor_pool.shutdown(wait=False)

        asyncio.create_task(_run_task())

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def handle_run(self, request: web.Request) -> web.Response:
        """
        POST /runs
        Body:
          { "models": ["a","b"], "force": false }
        """
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid or missing JSON body"}, status=400)
        requested = payload.get("models")
        force = bool(payload.get("force", False))

        if requested is None:
            selected_models = self.models
        else:
            if not isinstance(requested, list) or not all(isinstance(x, str) for x in requested):
                return web.json_response({"error": "models must be list[str]"}, status=400)
            requested_set = set(requested)
            # include dependencies
            if self.graph:
                for m in list(requested_set):
                    deps = self.graph.get_dependencies(m)
                    requested_set.update(deps)
            selected_models = {k: v for k, v in self.models.items() if k in requested_set}

        # Run in background and return a simple ack (HA-ready improvements later)
        await self._enqueue_run(list(selected_models.keys()), force=force)
        return web.json_response(
            {"status": "accepted", "models": list(selected_models.keys()), "force": force},
            status=202,
        )

    async def handle_sync_once(self, request: web.Request) -> web.Response:
        """
        POST /sync/run_once
        Body:
          { "job_id": "..." }  # optional, defaults to first job
        """
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid or missing JSON body"}, status=400)
        job_id = payload.get("job_id")

        jobs = self.config.get("sync", {}).get("jobs", [])
        if not jobs:
            return web.json_response({"error": "no sync.jobs configured"}, status=400)

        chosen = None
        if job_id:
            for j in jobs:
                if j.get("job_id") == job_id:
                    chosen = j
                    break
            if chosen is None:
                return web.json_response({"error": f"unknown job_id {job_id}"}, status=404)
        else:
            chosen = jobs[0]

        job = _job_from_dict(chosen)
        # Run sync inline (single tick is bounded by max_files_per_run)
        summary = run_sftp_sync_job(job, config=self.config, state_store=self.state_store)
        return web.json_response({"status": "ok", "summary": summary})

    async def handle_stream_publish(self, request: web.Request) -> web.Response:
        """
        POST /streams/{stream_name}
        Body: dict or list[dict]

        Phase C: Append event(s) into the stream table, then trigger downstream models
        that depend on this stream (impact-based execution).
        """
        stream_name = request.match_info.get("stream_name")
        if not stream_name:
            return web.json_response({"error": "missing stream_name"}, status=400)

        stream_info = self.models.get(stream_name)
        if not stream_info or stream_info.get("type") != "stream":
            return web.json_response({"error": f"unknown stream '{stream_name}'"}, status=404)

        payload = await request.json()
        if isinstance(payload, dict):
            rows = [payload]
        elif isinstance(payload, list) and all(isinstance(x, dict) for x in payload):
            rows = payload
        else:
            return web.json_response({"error": "body must be object or array of objects"}, status=400)

        schema = stream_info.get("schema", "events")
        conn_name = (
            stream_info.get("connection") or self.config.get("executor", {}).get("default_connection") or "default"
        )
        try:
            conn_wrapper = get_named_connection(conn_name)
        except Exception:
            # Fallback to default connection name selection used in Executor
            fallback_name = next(iter(self.config.get("connections", {}).keys()), None)
            if fallback_name is None:
                return web.json_response({"error": "No connections configured"}, status=500)
            conn_wrapper = get_named_connection(fallback_name)
        con = conn_wrapper.connection

        # Ensure schema exists
        try:
            if hasattr(con, "create_database"):
                con.create_database(schema, force=False)
        except Exception:
            pass

        # Ensure stream table exists (prefer declared schema if provided)
        table_exists = False
        try:
            table_exists = stream_name in con.list_tables(database=schema)
        except Exception:
            table_exists = False

        if not table_exists:
            fields = stream_info.get("fields")
            if fields:
                from interlace.utils.schema_utils import fields_to_ibis_schema

                ibis_schema = fields_to_ibis_schema(fields)
                if ibis_schema:
                    con.create_table(stream_name, schema=ibis_schema, database=schema, overwrite=False)
                    table_exists = True
            if not table_exists:
                # Create table from incoming rows (small event payloads). This may infer types.
                con.create_table(stream_name, obj=ibis.memtable(rows), database=schema, overwrite=False)
                table_exists = True

        # Insert rows via temp table to keep it set-based in DuckDB.
        tmp_name = f"_interlace_stream_{stream_name}_{uuid.uuid4().hex}"
        tmp_expr = ibis.memtable(rows)
        try:
            # Prefer creating temp table from ibis expr without executing
            try:
                con.create_table(tmp_name, obj=tmp_expr, temp=True)
            except Exception:
                df = tmp_expr.execute()
                con.create_table(tmp_name, obj=df, temp=True)

            _execute_sql_internal(con, f"INSERT INTO {schema}.{stream_name} SELECT * FROM {tmp_name}")
        finally:
            try:
                if hasattr(con, "drop_table"):
                    con.drop_table(tmp_name, force=True)
                else:
                    _execute_sql_internal(con, f"DROP TABLE IF EXISTS {tmp_name}")
            except Exception:
                pass

        # Trigger downstream impacted models
        dependents: list[str] = []
        if self.graph:
            dependents = self.graph.get_dependents(stream_name) or []

        if dependents:
            await self._enqueue_run(dependents, force=True)

        return web.json_response(
            {
                "status": "accepted",
                "stream": stream_name,
                "rows_received": len(rows),
                "triggered_models": dependents,
            },
            status=202,
        )


def run_service(
    *,
    project_dir: Path,
    env: str | None,
    host: str,
    port: int,
    verbose: bool = False,
    enable_scheduler: bool = True,
    run_on_startup: bool = False,
    enable_ui: bool = True,
    cors_origins: list[str] | None = None,
) -> None:
    """
    Run the Interlace service (blocking).

    Args:
        project_dir: Project directory path
        env: Environment name (dev, staging, prod)
        host: Host to bind to
        port: Port to bind to
        verbose: Enable verbose logging
        enable_scheduler: Enable background scheduler for scheduled models
        run_on_startup: Perform initial run of all models on startup
        enable_ui: Enable web UI serving (from embedded static files)
        cors_origins: List of allowed CORS origins
    """
    svc = InterlaceService(project_dir=project_dir, env=env, verbose=verbose)
    try:
        svc.initialize()
    except InitializationError as e:
        raise RuntimeError(f"Initialization failed: {e}") from None

    # Create app with error handling middleware
    app = web.Application(middlewares=[error_middleware])

    # Setup CORS
    if cors_origins is None:
        # Default origins for development
        cors_origins = [
            "http://localhost:5173",  # Vite dev server
            "http://localhost:8080",  # Same origin
            f"http://{host}:{port}",
        ]
    setup_cors(app, origins=cors_origins)

    # Setup API key authentication (if configured)
    auth_config = svc.config.get("service.auth", {}) if svc.config else {}
    if isinstance(auth_config, dict) and auth_config:
        setup_auth(app, auth_config)

    # Register new API routes
    setup_routes(app, svc)

    # Register legacy routes for backward compatibility
    setup_legacy_routes(app, svc)

    # Legacy routes (keep for backward compatibility during transition)
    app.add_routes(
        [
            web.post("/runs", svc.handle_run),
            web.post("/sync/run_once", svc.handle_sync_once),
            web.post("/streams/{stream_name}", svc.handle_stream_publish),
        ]
    )

    # Setup static file serving for embedded UI (SPA)
    if enable_ui:
        static_dir = Path(__file__).parent / "static"
        if static_dir.exists() and (static_dir / "index.html").exists():
            # Serve SvelteKit build output (_app/) - hashed filenames, long cache
            if (static_dir / "_app").exists():
                app.router.add_static("/_app", static_dir / "_app", name="sveltekit_app")

            # Serve static assets (logos, images, etc.)
            if (static_dir / "assets").exists():
                app.router.add_static("/assets", static_dir / "assets", name="static_assets")

            # Serve individual static files (favicon, icons, manifest, etc.)
            async def static_file_handler(request: web.Request) -> web.StreamResponse:
                filename = request.match_info["filename"]
                ext = request.match_info["ext"]
                filepath = static_dir / f"{filename}.{ext}"
                if filepath.exists() and filepath.is_file():
                    return web.FileResponse(filepath)
                raise web.HTTPNotFound()

            for ext in ("svg", "png", "ico", "webmanifest"):
                app.router.add_get("/{filename}.{ext:" + ext + "}", static_file_handler)

            # SPA fallback: serve index.html for all non-API routes
            async def spa_handler(request: web.Request) -> web.StreamResponse:
                # Don't serve index.html for API routes
                if request.path.startswith("/api/") or request.path.startswith("/health"):
                    raise web.HTTPNotFound()
                response = web.FileResponse(static_dir / "index.html")
                # Prevent caching index.html so new deploys are picked up
                response.headers["Cache-Control"] = "no-cache"
                return response

            # Serve index.html at root
            app.router.add_get("/", spa_handler)
            # Catch-all for SPA client-side routing (must be added last)
            app.router.add_get("/{path:.*}", spa_handler)

            logger.info(f"Serving UI from {static_dir}")
        else:
            logger.warning("UI static files not found. Run 'interlace ui build' to generate them.")

    async def on_startup(app: web.Application) -> None:
        svc.start_background_tasks(enable_scheduler=enable_scheduler)
        if run_on_startup:
            logger.info("Performing initial run on startup...")
            await svc._enqueue_run(None, force=False)
        logger.info(f"Interlace service started on http://{host}:{port}")
        logger.info(f"API available at http://{host}:{port}/api/v1/")

    async def on_cleanup(app: web.Application) -> None:
        # Drain SSE subscribers first so their handlers exit cleanly
        if svc.event_bus:
            await svc.event_bus.shutdown()
            await asyncio.sleep(0.1)  # let handler coroutines finish
        await svc.stop_background_tasks()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # Disable aiohttp's built-in access logger to avoid a known bug where
    # start_time can be None (TypeError: float - NoneType).  Interlace has
    # its own request logging via the error_middleware.
    web.run_app(app, host=host, port=port, access_log=None)


def datetime_from_ts(ts: float, tz: str | None = None) -> Any:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    zone = ZoneInfo(tz) if tz else ZoneInfo("UTC")
    return datetime.fromtimestamp(ts, tz=zone)
