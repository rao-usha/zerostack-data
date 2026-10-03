"""
One APScheduler across every process that shares the database (SPEC_160).

APScheduler's job store is a shared table (``apscheduler_jobs``), but each
process that starts a scheduler fires the jobs itself, so two API processes
(laptop + cloud, or two Cloud Run instances) ran every schedule twice. Two
guards:

- ``RUN_SCHEDULER=false`` keeps a process out entirely: no registration, no
  lock, no stale-job resolver.
- With it on, a process runs jobs only while it holds a session-level
  Postgres advisory lock on a dedicated long-lived connection. A standby
  retries every ``SCHEDULER_LEADER_RETRY_SECONDS``; the leader re-checks the
  lock every ``SCHEDULER_LEADER_CHECK_SECONDS`` and pauses its scheduler if
  the connection (and with it the lock) is gone.

Every process starts APScheduler *paused* so that schedule edits made through
the API still reach the shared job store; only the leader resumes it.

Which loops are leader-only and which run per process is documented in
``docs/specs/SPEC_160_scheduler_leader_runtime_split.md``.
"""

import asyncio
import inspect
import logging
from typing import Any, Callable, Dict, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logger = logging.getLogger(__name__)

# Arbitrary constant ("nexdata scheduler leader"), single-bigint form; next to
# MIGRATION_LOCK_KEY (804_261_117). The two-int namespaces (122, 124, 144) live
# in a different pg_locks key space (objsubid 2), so they cannot collide.
SCHEDULER_LEADER_LOCK_KEY = 804_261_160
LOCK_APPLICATION_NAME = "nexdata-scheduler-leader"

# The lock session must outlive any server-side idle timeout, and the server
# must notice a vanished leader host in about a minute (default is the OS
# keepalive, ~2 h) so the lock is released for a standby. Best effort: an
# unknown setting (older server) is skipped.
_LOCK_SESSION_SETTINGS = (
    "SET idle_session_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
    "SET tcp_keepalives_idle = 30",
    "SET tcp_keepalives_interval = 10",
    "SET tcp_keepalives_count = 3",
)

STALE_RUNNING_HOURS = 2
STALE_RESOLVED_MESSAGE = "Stale job auto-resolved on startup"

# A RUNNING ingestion job older than the threshold whose linked queue job is
# still live is being worked on by a worker (a 4 h CMS or mart load) and is
# left alone. The link rule is SPEC_121's (ingestion_job_sync).
_RESOLVE_STALE_SQL = """
    UPDATE ingestion_jobs ij
    SET status = 'failed',
        error_message = :msg,
        completed_at = NOW()
    WHERE ij.status = 'running'
      AND ij.started_at < NOW() - make_interval(hours => :hours)
      AND NOT EXISTS (
          SELECT 1 FROM job_queue jq
          WHERE (jq.payload ->> 'ingestion_job_id') = ij.id::text
            AND (jq.job_table_id = ij.id OR jq.job_table_id IS NULL)
            AND lower(jq.status::text) IN ('pending', 'blocked', 'claimed', 'running')
      )
"""


def _key_parts(key: int):
    """pg_locks shows a bigint advisory key as classid (high) / objid (low)."""
    return {"hi": (key >> 32) & 0xFFFFFFFF, "lo": key & 0xFFFFFFFF}


_OWN_LOCK_SQL = """
    SELECT count(*) FROM pg_locks
    WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
      AND objsubid = 1 AND classid::bigint = :hi AND objid::bigint = :lo
"""

_HOLDER_SQL = """
    SELECT a.pid, a.application_name, host(a.client_addr) AS client_addr, a.backend_start
    FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'advisory' AND l.granted
      AND l.objsubid = 1 AND l.classid::bigint = :hi AND l.objid::bigint = :lo
    LIMIT 1
"""


class LeaderLock:
    """A session-level advisory lock on its own AUTOCOMMIT connection.

    Synchronous; ``SchedulerLeadership`` calls it through ``asyncio.to_thread``.
    AUTOCOMMIT matters: a lock taken inside a transaction would leave the
    session "idle in transaction" for as long as it leads.
    """

    def __init__(self, url: str, key: int = SCHEDULER_LEADER_LOCK_KEY,
                 application_name: str = LOCK_APPLICATION_NAME):
        self.url = url
        self.key = key
        self.application_name = application_name
        self._engine = None
        self._conn = None
        self.backend_pid: Optional[int] = None

    def _get_engine(self):
        if self._engine is None:
            connect_args: Dict[str, Any] = {}
            if self.url.startswith("postgresql"):
                connect_args = {
                    "application_name": self.application_name,
                    "connect_timeout": 10,
                    # client-side keepalives: notice a dead server/proxy
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 3,
                }
            self._engine = create_engine(self.url, poolclass=NullPool, connect_args=connect_args)
        return self._engine

    def try_acquire(self) -> bool:
        """Take the lock if it is free. True if this object holds it now."""
        if self._conn is not None:
            if self.is_held():
                return True
        conn = self._get_engine().connect().execution_options(isolation_level="AUTOCOMMIT")
        try:
            for stmt in _LOCK_SESSION_SETTINGS:
                try:
                    conn.execute(text(stmt))
                except Exception as e:  # e.g. idle_session_timeout before PG14
                    logger.debug(f"Leader lock session setting skipped ({stmt}): {e}")
            got = bool(conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": self.key}).scalar())
            pid = conn.execute(text("SELECT pg_backend_pid()")).scalar() if got else None
        except Exception:
            conn.close()
            raise
        if not got:
            conn.close()
            return False
        self._conn = conn
        self.backend_pid = pid
        return True

    def is_held(self) -> bool:
        """True while our session is alive and still holds the lock."""
        if self._conn is None:
            return False
        try:
            held = bool(self._conn.execute(text(_OWN_LOCK_SQL), _key_parts(self.key)).scalar())
        except Exception as e:
            logger.warning(f"Scheduler leader lock connection lost: {type(e).__name__}: {e}")
            held = False
        if not held:
            self._close()
        return held

    def holder(self) -> Optional[Dict[str, Any]]:
        """Who holds the lock (for the standby log line). Best effort."""
        try:
            with self._get_engine().connect() as conn:
                row = conn.execute(text(_HOLDER_SQL), _key_parts(self.key)).mappings().first()
            return dict(row) if row else None
        except Exception:
            return None

    def release(self) -> None:
        """Unlock and close the lock connection; safe to call repeatedly."""
        if self._conn is not None:
            try:
                self._conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": self.key})
            except Exception:
                pass  # closing the session releases it anyway
        self._close()
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def _close(self) -> None:
        conn, self._conn, self.backend_pid = self._conn, None, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                try:
                    conn.invalidate()
                except Exception:
                    pass


async def _call(fn: Optional[Callable[[], Any]], what: str) -> None:
    if fn is None:
        return
    try:
        result = fn()
        if inspect.isawaitable(result):
            await result
    except Exception as e:
        logger.error(f"Scheduler leadership: {what} failed: {e}", exc_info=True)


class SchedulerLeadership:
    """Keeps trying to lead; runs ``on_elected`` / ``on_demoted`` on changes."""

    def __init__(self, lock, on_elected: Optional[Callable[[], Any]] = None,
                 on_demoted: Optional[Callable[[], Any]] = None,
                 retry_seconds: float = 120.0, check_seconds: float = 30.0):
        self.lock = lock
        self.on_elected = on_elected
        self.on_demoted = on_demoted
        self.retry_seconds = retry_seconds
        self.check_seconds = check_seconds
        self._leader = False
        self._task: Optional[asyncio.Task] = None
        self._stopping: Optional[asyncio.Event] = None
        self._last_holder: Any = object()
        if retry_seconds <= check_seconds:
            logger.warning(
                "SCHEDULER_LEADER_RETRY_SECONDS (%s) should exceed SCHEDULER_LEADER_CHECK_SECONDS "
                "(%s) so a leader that lost its lock pauses before a standby takes over",
                retry_seconds, check_seconds,
            )

    @property
    def is_leader(self) -> bool:
        return self._leader

    async def start(self) -> bool:
        """First attempt inline, then keep the loop running in the background."""
        self._stopping = asyncio.Event()
        await self._attempt()
        self._task = asyncio.create_task(self._run(), name="scheduler-leadership")
        return self._leader

    async def _attempt(self) -> bool:
        try:
            got = await asyncio.to_thread(self.lock.try_acquire)
        except Exception as e:
            logger.warning(f"Scheduler leader lock attempt failed: {type(e).__name__}: {e}")
            got = False
        if got:
            self._leader = True
            self._last_holder = object()
            logger.info(
                "Scheduler leader lock acquired (key=%s, backend pid=%s): this process runs the scheduler",
                getattr(self.lock, "key", "?"), getattr(self.lock, "backend_pid", "?"),
            )
            await _call(self.on_elected, "on_elected")
            return True
        holder = None
        try:
            holder = await asyncio.to_thread(self.lock.holder)
        except Exception:
            pass
        level = logging.INFO if holder != self._last_holder else logging.DEBUG
        self._last_holder = holder
        logger.log(
            level,
            "Scheduler leader lock is held by another process (%s); standing by, retrying every %ss",
            holder or "unknown", self.retry_seconds,
        )
        return False

    async def _check(self) -> None:
        try:
            held = await asyncio.to_thread(self.lock.is_held)
        except Exception as e:
            logger.warning(f"Scheduler leader lock check failed: {e}")
            held = False
        if not held:
            self._leader = False
            logger.error("Lost the scheduler leader lock; pausing the scheduler and standing by")
            await _call(self.on_demoted, "on_demoted")

    async def _run(self) -> None:
        while not self._stopping.is_set():
            wait = self.check_seconds if self._leader else self.retry_seconds
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=wait)
                break
            except asyncio.TimeoutError:
                pass
            try:
                if self._leader:
                    await self._check()
                else:
                    await self._attempt()
            except Exception as e:  # never let the loop die
                logger.error(f"Scheduler leadership loop error: {e}", exc_info=True)

    async def stop_loop(self) -> None:
        if self._stopping is not None:
            self._stopping.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                self._task.cancel()
            self._task = None

    async def release(self) -> None:
        self._leader = False
        try:
            await asyncio.to_thread(self.lock.release)
        except Exception as e:
            logger.warning(f"Releasing the scheduler leader lock failed: {e}")

    async def stop(self) -> None:
        await self.stop_loop()
        await self.release()


# ---------------------------------------------------------------------------
# Resolver for jobs left RUNNING by a dead process
# ---------------------------------------------------------------------------


def resolve_stale_running_jobs(engine, hours: int = STALE_RUNNING_HOURS) -> int:
    """Fail RUNNING ingestion jobs older than ``hours`` that nothing runs.

    Leader-only (an API restart or a second API process must not fail jobs a
    worker is in the middle of). Rows with a live linked queue job are left
    to the worker; rows whose queue job already ended are settled here or by
    the SPEC_121 orphan sweep, whichever comes first.
    """
    with engine.begin() as conn:
        n = conn.execute(text(_RESOLVE_STALE_SQL), {"msg": STALE_RESOLVED_MESSAGE, "hours": hours}).rowcount
    if n:
        logger.warning(f"Resolved {n} stale RUNNING ingestion job(s) older than {hours}h with no live worker job")
    return n or 0


# ---------------------------------------------------------------------------
# Process-wide runtime (what main.py calls)
# ---------------------------------------------------------------------------

_leadership: Optional[SchedulerLeadership] = None
_enabled: Optional[bool] = None


def _run_scheduler_setting() -> bool:
    try:
        from app.core.config import get_settings

        return bool(get_settings().run_scheduler)
    except Exception:
        return True


def leader_status() -> Optional[bool]:
    """True: this process runs the scheduler. False: standby. None: not taking part."""
    if _leadership is None:
        return None
    return _leadership.is_leader


def may_run_jobs() -> bool:
    """Whether this process may (un)pause APScheduler.

    Without a runtime (scripts, tests) RUN_SCHEDULER alone decides, as before.
    """
    if _leadership is not None:
        return _leadership.is_leader
    if _enabled is False:
        return False
    return _run_scheduler_setting()


async def start_scheduler_runtime(
    register_jobs: Callable[[], Any],
    *,
    enabled: Optional[bool] = None,
    lock=None,
    url: Optional[str] = None,
    engine=None,
    retry_seconds: Optional[float] = None,
    check_seconds: Optional[float] = None,
) -> Optional[bool]:
    """Start APScheduler paused, then lead it if allowed and elected.

    Returns the leader status (None when RUN_SCHEDULER is off).
    """
    global _leadership, _enabled
    from app.core import scheduler_service

    if enabled is None or (lock is None and url is None) or retry_seconds is None or check_seconds is None:
        from app.core.config import get_settings

        settings = get_settings()
        enabled = settings.run_scheduler if enabled is None else enabled
        url = url or settings.database_url
        retry_seconds = retry_seconds or settings.scheduler_leader_retry_seconds
        check_seconds = check_seconds or settings.scheduler_leader_check_seconds
    _enabled = bool(enabled)

    # Paused in every process: API schedule edits still reach the shared store.
    try:
        scheduler_service.start_scheduler(paused=True)
    except Exception as e:
        logger.warning(f"Failed to start the (paused) scheduler: {e}")

    if not _enabled:
        logger.info("RUN_SCHEDULER is off: this process registers and runs no scheduled jobs")
        return None

    async def on_elected():
        try:
            from app.core.database import get_engine

            await asyncio.to_thread(resolve_stale_running_jobs, engine or get_engine())
        except Exception as e:
            logger.warning(f"Stale running-job resolver skipped: {e}")
        try:
            register_jobs()
        except Exception as e:
            logger.warning(f"Scheduled job registration failed: {e}", exc_info=True)
        scheduler_service.start_scheduler()

    def on_demoted():
        scheduler_service.pause_scheduler()

    _leadership = SchedulerLeadership(
        lock or LeaderLock(url),
        on_elected=on_elected,
        on_demoted=on_demoted,
        retry_seconds=retry_seconds,
        check_seconds=check_seconds,
    )
    return await _leadership.start()


async def stop_scheduler_runtime() -> None:
    """Stop the loop, stop the scheduler, then release the lock (that order,
    so the next leader never overlaps with this one)."""
    global _leadership
    leadership = _leadership
    if leadership is not None:
        await leadership.stop_loop()
    try:
        from app.core import scheduler_service

        scheduler_service.stop_scheduler()
    except Exception as e:
        logger.warning(f"Error stopping scheduler: {e}")
    if leadership is not None:
        await leadership.release()
    _leadership = None
