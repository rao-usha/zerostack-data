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
  lock every ``SCHEDULER_LEADER_CHECK_SECONDS`` (bounded by a timeout: a hung
  check counts as a lost lock) and pauses its scheduler if the connection
  (and with it the lock) is gone. A process that takes the lock waits one
  check interval plus that timeout before it resumes the scheduler, so a
  previous leader has stepped down by then.

Every process starts APScheduler *paused* so that schedule edits made through
the API still reach the shared job store; only the leader resumes it.

Which loops are leader-only and which run per process is documented in
``docs/specs/SPEC_160_scheduler_leader_runtime_split.md``.
"""

import asyncio
import inspect
import logging
import threading
from datetime import datetime
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

LOCK_TCP_USER_TIMEOUT_MS = 30_000
LOCK_STATEMENT_TIMEOUT_MS = 10_000

# Upper bound on one lock check from the leader (a hung socket counts as a
# lost lock) and on one acquisition attempt (connect_timeout is 10 s).
DEFAULT_CHECK_TIMEOUT_SECONDS = 10.0
ACQUIRE_TIMEOUT_SECONDS = 30.0

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
        # Bumped by abandon(): a try_acquire still blocked in a worker thread
        # when its caller gave up must not install its connection afterwards.
        self._generation = 0

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
                    # Keepalives do nothing while a query waits on unacked
                    # data; this bounds that (libpq >= 12, TCP only).
                    "tcp_user_timeout": LOCK_TCP_USER_TIMEOUT_MS,
                    # Every lock query is instant; a stuck one is a dead link.
                    "options": f"-c statement_timeout={LOCK_STATEMENT_TIMEOUT_MS}",
                }
            self._engine = create_engine(self.url, poolclass=NullPool, connect_args=connect_args)
        return self._engine

    def try_acquire(self) -> bool:
        """Take the lock if it is free. True if this object holds it now."""
        if self._conn is not None:
            if self.is_held():
                return True
        generation = self._generation
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
            _close_quietly(conn)
            raise
        if not got or generation != self._generation:
            # Not free, or the caller timed out and abandoned this attempt:
            # closing the session drops a lock it may just have taken.
            _close_quietly(conn)
            return False
        self._conn = conn
        self.backend_pid = pid
        return True

    def is_held(self) -> bool:
        """True while our session is alive and still holds the lock."""
        conn = self._conn
        if conn is None:
            return False
        try:
            held = bool(conn.execute(text(_OWN_LOCK_SQL), _key_parts(self.key)).scalar())
        except Exception as e:
            logger.warning(f"Scheduler leader lock connection lost: {type(e).__name__}: {e}")
            held = False
        if not held:
            if self._conn is conn:
                self._close()
            else:  # abandoned meanwhile; only this (dead) connection is ours to close
                _close_quietly(conn)
        return held

    def abandon(self) -> None:
        """Forget the lock connection without waiting on it.

        For a check or attempt that hung (dead socket): the caller has already
        stepped down; the stuck worker thread closes the connection when its
        call finally fails, and the server frees the lock with the session.
        """
        self._generation += 1
        conn, self._conn, self.backend_pid = self._conn, None, None
        if conn is not None:
            threading.Thread(target=_close_quietly, args=(conn,), daemon=True,
                             name="leader-lock-abandon").start()

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
            _close_quietly(conn)


def _close_quietly(conn) -> None:
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
    """Keeps trying to lead; runs ``on_elected`` / ``on_demoted`` on changes.

    Three states: standby (lock not held), *fencing* (lock held, waiting
    ``fence_seconds`` before running anything) and leader. The fence closes
    the overlap window: a previous leader that lost its lock notices within
    one check interval plus the check timeout (a hung check counts as lost),
    so a new leader that waits that long before resuming the scheduler does
    not fire jobs alongside it. A process frozen whole (laptop sleep) is the
    exception: on wake its scheduler may fire due jobs before its next check.
    """

    def __init__(self, lock, on_elected: Optional[Callable[[], Any]] = None,
                 on_demoted: Optional[Callable[[], Any]] = None,
                 retry_seconds: float = 120.0, check_seconds: float = 30.0,
                 check_timeout: Optional[float] = None,
                 fence_seconds: Optional[float] = None,
                 acquire_timeout: float = ACQUIRE_TIMEOUT_SECONDS):
        self.lock = lock
        self.on_elected = on_elected
        self.on_demoted = on_demoted
        self.retry_seconds = retry_seconds
        self.check_seconds = check_seconds
        self.check_timeout = (min(DEFAULT_CHECK_TIMEOUT_SECONDS, check_seconds)
                              if check_timeout is None else check_timeout)
        self.fence_seconds = (check_seconds + self.check_timeout
                              if fence_seconds is None else fence_seconds)
        self.acquire_timeout = acquire_timeout
        self._held = False    # we hold the lock (fencing or leading)
        self._leader = False  # elected: on_elected ran, the scheduler may fire
        self.elections = 0
        self._task: Optional[asyncio.Task] = None
        self._stopping: Optional[asyncio.Event] = None
        self._last_holder: Any = object()
        # The fence, not retry > check, keeps leaders from overlapping, so any
        # retry interval is safe. It assumes every host uses the same check
        # settings (a new leader waits *its* check + timeout).

    @property
    def is_leader(self) -> bool:
        return self._leader

    async def start(self) -> bool:
        """First attempt inline, then keep the loop running in the background.

        With a fence (the default) the election itself happens in the loop,
        ``fence_seconds`` after the lock was taken.
        """
        self._stopping = asyncio.Event()
        if await self._attempt() and self.fence_seconds <= 0:
            await self._elect()
        self._task = asyncio.create_task(self._run(), name="scheduler-leadership")
        return self._leader

    def _abandon_lock(self) -> None:
        abandon = getattr(self.lock, "abandon", None)
        if abandon is not None:
            try:
                abandon()
            except Exception as e:
                logger.warning(f"Abandoning the scheduler leader lock connection failed: {e}")

    async def _attempt(self) -> bool:
        try:
            got = await asyncio.wait_for(asyncio.to_thread(self.lock.try_acquire),
                                         timeout=self.acquire_timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Scheduler leader lock attempt timed out after {self.acquire_timeout}s")
            self._abandon_lock()
            got = False
        except Exception as e:
            logger.warning(f"Scheduler leader lock attempt failed: {type(e).__name__}: {e}")
            got = False
        if got:
            self._held = True
            self._last_holder = object()
            logger.info(
                "Scheduler leader lock acquired (key=%s, backend pid=%s); running the scheduler in %ss",
                getattr(self.lock, "key", "?"), getattr(self.lock, "backend_pid", "?"),
                max(self.fence_seconds, 0),
            )
            return True
        holder = None
        try:
            holder = await asyncio.wait_for(asyncio.to_thread(self.lock.holder),
                                            timeout=self.acquire_timeout)
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

    async def _elect(self) -> None:
        self._leader = True
        self.elections += 1
        logger.info("Elected scheduler leader: this process runs the scheduler")
        await _call(self.on_elected, "on_elected")

    async def _lock_still_held(self) -> bool:
        """One bounded lock check; a check that hangs counts as a lost lock."""
        try:
            return bool(await asyncio.wait_for(asyncio.to_thread(self.lock.is_held),
                                               timeout=self.check_timeout))
        except asyncio.TimeoutError:
            logger.error(
                f"Scheduler leader lock check hung for {self.check_timeout}s; treating the lock as lost"
            )
            self._abandon_lock()
            return False
        except Exception as e:
            logger.warning(f"Scheduler leader lock check failed: {e}")
            return False

    async def _check(self) -> None:
        if await self._lock_still_held():
            return
        was_leader = self._leader
        self._leader = False
        self._held = False
        if was_leader:
            logger.error("Lost the scheduler leader lock; pausing the scheduler and standing by")
            await _call(self.on_demoted, "on_demoted")
        else:
            logger.warning("Lost the scheduler leader lock before taking over; standing by")

    async def _run(self) -> None:
        while not self._stopping.is_set():
            if self._leader:
                wait = self.check_seconds
            elif self._held:
                wait = self.fence_seconds
            else:
                wait = self.retry_seconds
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=wait)
                break
            except asyncio.TimeoutError:
                pass
            try:
                if self._held:
                    await self._check()
                    if self._held and not self._leader:
                        await self._elect()  # fence over and the lock is still ours
                elif await self._attempt() and self.fence_seconds <= 0:
                    await self._elect()
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
        self._held = False
        try:
            await asyncio.wait_for(asyncio.to_thread(self.lock.release), timeout=self.acquire_timeout)
        except asyncio.TimeoutError:
            logger.warning("Releasing the scheduler leader lock timed out; abandoning the connection")
            self._abandon_lock()
        except Exception as e:
            logger.warning(f"Releasing the scheduler leader lock failed: {e}")

    async def stop(self) -> None:
        await self.stop_loop()
        await self.release()


# ---------------------------------------------------------------------------
# Resolver for jobs left RUNNING by a dead process
# ---------------------------------------------------------------------------

# Other processes of the same role ("nexdata-api@<host>:<pid>") that are
# connected right now. Any of them may be running in-process ingestions
# (BackgroundTasks, or the plain-source schedules of a demoted leader) that
# have no queue row, so their RUNNING rows are indistinguishable from dead ones.
_OTHER_LIVE_PROCESSES_SQL = """
    SELECT DISTINCT application_name FROM pg_stat_activity
    WHERE application_name LIKE :pattern AND application_name <> :me
      AND pid <> pg_backend_pid()
"""


def _like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def resolve_stale_running_jobs(
    engine,
    hours: int = STALE_RUNNING_HOURS,
    *,
    started_before: Optional[datetime] = None,
    process_role: Optional[str] = None,
    process_name: Optional[str] = None,
) -> int:
    """Fail RUNNING ingestion jobs older than ``hours`` that nothing runs.

    Leader-only (an API restart or a second API process must not fail jobs a
    worker is in the middle of). Rows with a live linked queue job are left
    to the worker; rows whose queue job already ended are settled here or by
    the SPEC_121 orphan sweep, whichever comes first.

    A RUNNING row with no queue row is an in-process run, and SQL cannot tell
    a dead process's run from a live one's (SPEC_160 review). So:

    - ``started_before`` (this process's boot time): rows started since then
      may be this process's own live runs and are skipped.
    - ``process_role`` / ``process_name``: if any other process of the same
      role is connected, it may own such rows, and the resolver does nothing;
      ``cleanup_stuck_jobs`` (per-source timeout) settles them later.
    """
    with engine.begin() as conn:
        if process_role and process_name:
            others = [r[0] for r in conn.execute(
                text(_OTHER_LIVE_PROCESSES_SQL),
                {"pattern": _like_escape(process_role) + "@%", "me": process_name},
            ).all()]
            if others:
                logger.info(
                    "Stale running-job resolver skipped: other live %s process(es) %s may own "
                    "in-process RUNNING jobs; cleanup_stuck_jobs settles real orphans",
                    process_role, others[:5],
                )
                return 0
        sql = _RESOLVE_STALE_SQL
        params: Dict[str, Any] = {"msg": STALE_RESOLVED_MESSAGE, "hours": hours}
        if started_before is not None:
            sql += "\n      AND ij.started_at < :started_before"
            params["started_before"] = started_before
        n = conn.execute(text(sql), params).rowcount
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
    """True: this process runs the scheduler. False: standby (or fencing).
    None: not taking part."""
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


def _resolver_identity() -> Dict[str, Any]:
    """This process's boot time and connection identity (app.core.database)."""
    from app.core import database

    return {
        "started_before": database.PROCESS_STARTED_AT,
        "process_role": database.process_role_name(),
        "process_name": database.process_application_name(),
    }


async def start_scheduler_runtime(
    register_jobs: Callable[[], Any],
    *,
    enabled: Optional[bool] = None,
    lock=None,
    url: Optional[str] = None,
    engine=None,
    retry_seconds: Optional[float] = None,
    check_seconds: Optional[float] = None,
    fence_seconds: Optional[float] = None,
    check_timeout: Optional[float] = None,
) -> Optional[bool]:
    """Start APScheduler paused, then lead it if allowed and elected.

    Returns the leader status (None when RUN_SCHEDULER is off). With the
    default fence the first election lands ``check + check timeout`` seconds
    after the lock is taken, so this returns False and the loop elects.
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

    resolved = {"done": False}

    async def on_elected():
        # Once per process (SPEC_160 review): on a re-election this process's
        # own in-process runs are alive, and a demoted leader's may be too.
        if not resolved["done"]:
            resolved["done"] = True
            try:
                from app.core.database import get_engine

                await asyncio.to_thread(
                    lambda: resolve_stale_running_jobs(engine or get_engine(), **_resolver_identity())
                )
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
        check_timeout=check_timeout,
        fence_seconds=fence_seconds,
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
