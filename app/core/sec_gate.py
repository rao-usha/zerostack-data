"""
SEC fair-access gate (SPEC_146, PLAN_090).

ONE limiter for every request NexData sends to a ``*.sec.gov`` host, from the
api process and every worker:

- token bucket, default 5 req/s with burst 2 (SEC fair access allows 10/s per
  IP; any 1 s window here is at most 7), shared across processes;
- on 429/403 from SEC: a GLOBAL cooldown of max(Retry-After, exponential
  backoff with jitter) that every caller waits out -- caller retry loops can no
  longer hammer SEC on a fixed interval;
- circuit breaker: ``breaker_threshold`` consecutive 429/403 (from any process)
  pause every NexData SEC call for ``breaker_seconds``; the trip and the reset
  are each logged once;
- a caller whose wait would exceed ``max_wait`` gets :class:`SecRateLimited`
  at once instead of sleeping (or retrying) through the pause;
- every SEC request carries ``settings.sec_user_agent`` (env SEC_USER_AGENT).

Cross-process state lives in Postgres, in two rows of the existing
``rate_limit_bucket`` table (no DDL): ``sec.gov`` is the bucket and
``sec.gov#breaker`` holds strikes (tokens), trips (max_tokens) and the
blocked-until time (last_refill_at, naive UTC). Rows are locked with
``SELECT ... FOR UPDATE`` in domain order and time comes from the DB clock, so
container clocks cannot skew the bucket. Every NexData process already shares
that DB; there is no Redis, and a lock file on a Docker-Desktop bind mount is
not a reliable cross-container lock. If the DB is unreachable the gate falls
back to an in-process bucket at ``fallback_rate`` (1 req/s) and logs once.

Coverage: :func:`install` patches ``httpx.HTTPTransport.handle_request`` and
``httpx.AsyncHTTPTransport.handle_async_request`` (pass-through for non-SEC
hosts), so every httpx client in the process is gated. ``SecHttp`` wraps its
transport explicitly and the aiohttp people collector calls the gate directly;
a context variable stops a request being counted twice.
"""

from __future__ import annotations

import asyncio
import contextvars
import email.utils
import logging
import random
import threading
import time
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable, Optional

import httpx

from app.core.config import SEC_DEFAULT_USER_AGENT as DEFAULT_USER_AGENT

logger = logging.getLogger(__name__)

BUCKET_ROW = "sec.gov"
BREAKER_ROW = "sec.gov#breaker"
STRIKE_STATUS = frozenset({429, 403})

# set while a request is inside the gate, so a nested gated layer passes through
_IN_GATE: contextvars.ContextVar[bool] = contextvars.ContextVar("sec_gate_in_gate", default=False)


class _Contended(Exception):
    """The shared gate rows are locked by another process past lock_timeout (busy, not down)."""


# Postgres SQLSTATEs that mean "busy", not "backend down": lock_not_available
# (lock_timeout), query_canceled (statement_timeout), deadlock, serialization.
_CONTENDED_PGCODES = frozenset({"55P03", "57014", "40P01", "40001"})
CONTENDED_WAIT = 0.25


class SecRateLimited(httpx.TransportError):
    """SEC calls are paused (cooldown or open breaker) longer than the caller may wait."""

    def __init__(self, message: str, wait: float):
        super().__init__(message)
        self.wait = wait


def is_sec_host(host: Optional[str]) -> bool:
    if not host:
        return False
    host = host.lower().rstrip(".")
    return host == "sec.gov" or host.endswith(".sec.gov")


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Retry-After as seconds (delta-seconds or HTTP-date); None when absent/unparseable."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def sec_user_agent() -> str:
    """The single SEC User-Agent (settings.sec_user_agent / env SEC_USER_AGENT)."""
    try:
        from app.core.config import get_settings

        ua = (get_settings().sec_user_agent or "").strip()
        return ua or DEFAULT_USER_AGENT
    except Exception:
        return DEFAULT_USER_AGENT


# ---------------------------------------------------------------------------
# Pure state machine
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateConfig:
    rate: float = 5.0  # tokens per second, across ALL processes
    burst: float = 2.0  # bucket capacity
    max_wait: float = 120.0  # longest a caller sleeps before SecRateLimited
    breaker_threshold: int = 3  # consecutive 429/403 that open the breaker
    breaker_seconds: float = 600.0
    backoff_base: float = 5.0  # first strike cooldown (full value; jittered down to half)
    backoff_cap: float = 600.0
    retry_after_cap: float = 3600.0
    fallback_rate: float = 1.0  # in-process rate when the shared backend is down


@dataclass
class GateState:
    tokens: float
    updated: float  # backend clock, seconds
    blocked_until: float = 0.0
    strikes: int = 0
    trips: int = 0


@dataclass(frozen=True)
class TakeResult:
    wait: float  # 0 => token granted
    now: float
    strikes: int


@dataclass(frozen=True)
class StrikeResult:
    tripped: bool  # this strike opened the breaker
    cooldown: float  # seconds from now until calls resume
    strikes: int


def take_token(st: GateState, now: float, cfg: GateConfig) -> float:
    """Consume one token if allowed; return 0, else the seconds to wait."""
    if now < st.blocked_until:
        return st.blocked_until - now
    elapsed = max(0.0, now - st.updated)
    tokens = min(cfg.burst, st.tokens + elapsed * cfg.rate)
    if tokens >= 1.0:
        st.tokens = tokens - 1.0
        st.updated = max(st.updated, now)
        return 0.0
    # No grant: leave the state untouched. The refill is recomputed from
    # ``updated`` next time with the same result, and nothing needs writing.
    return (1.0 - tokens) / cfg.rate


def strike(st: GateState, now: float, retry_after: Optional[float], cfg: GateConfig, rng: random.Random) -> bool:
    """Record a 429/403. Returns True when this strike opened the breaker.

    A 429/403 that arrives while a cooldown is already running came from a
    request admitted BEFORE that cooldown (none are admitted during one), so it
    is the same throttling event, not a new consecutive strike: it does not
    escalate the count (one burst of in-flight 429s must not trip the 10-minute
    breaker by itself), but its Retry-After still extends the pause.
    """
    if now < st.blocked_until:
        if retry_after is not None:
            st.blocked_until = max(st.blocked_until, now + min(retry_after, cfg.retry_after_cap))
        st.tokens = min(st.tokens, 0.0)
        return False
    already_open = st.strikes >= cfg.breaker_threshold and st.blocked_until > now
    st.strikes += 1
    # exponent capped: after days of 429s 2**strikes would overflow float maths
    full = min(cfg.backoff_cap, cfg.backoff_base * (2.0 ** min(st.strikes - 1, 32)))
    cooldown = rng.uniform(full / 2.0, full) if full > 0 else 0.0
    if retry_after is not None:
        cooldown = max(cooldown, min(retry_after, cfg.retry_after_cap))
    tripped = False
    if st.strikes >= cfg.breaker_threshold:
        cooldown = max(cooldown, cfg.breaker_seconds)
        if not already_open:
            st.trips += 1
            tripped = True
    st.blocked_until = max(st.blocked_until, now + cooldown)
    st.tokens = min(st.tokens, 0.0)
    return tripped


def success(st: GateState, now: float, cfg: GateConfig) -> bool:
    """Record a success. Returns True when it closed an open breaker.

    A success that lands while a pause is running came from a request admitted
    before the pause: it says nothing about SEC now, so it neither resets the
    strikes nor closes the breaker (else the breaker would log "closed" while
    still paused and lose its half-open re-trip).
    """
    if now < st.blocked_until:
        return False
    was_open = st.strikes >= cfg.breaker_threshold
    st.strikes = 0
    return was_open


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------


class _Backend:
    def _transact(self, cfg: GateConfig, fn: Callable[[GateState, float], object]):
        raise NotImplementedError

    def take(self, cfg: GateConfig) -> TakeResult:
        def fn(st, now):
            return TakeResult(take_token(st, now, cfg), now, st.strikes)

        return self._transact(cfg, fn)

    def strike(self, cfg: GateConfig, retry_after: Optional[float], rng: random.Random) -> StrikeResult:
        def fn(st, now):
            tripped = strike(st, now, retry_after, cfg, rng)
            return StrikeResult(tripped, max(0.0, st.blocked_until - now), st.strikes)

        return self._transact(cfg, fn)

    def success(self, cfg: GateConfig) -> bool:
        return self._transact(cfg, lambda st, now: success(st, now, cfg))


class LocalBackend(_Backend):
    """In-process state (tests, and the fallback when the DB is unreachable)."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self.state: Optional[GateState] = None
        self._lock = threading.Lock()

    def _transact(self, cfg, fn):
        with self._lock:
            now = self.clock()
            if self.state is None:
                self.state = GateState(tokens=cfg.burst, updated=now)
            return fn(self.state, now)


class PostgresBackend(_Backend):
    """State in ``rate_limit_bucket`` rows ``sec.gov`` and ``sec.gov#breaker`` (DB clock)."""

    def __init__(self, engine):
        self.engine = engine
        self._seeded = False

    def _seed(self, conn, cfg):
        from sqlalchemy import text

        conn.execute(
            text(
                "INSERT INTO rate_limit_bucket (domain, tokens, max_tokens, refill_rate, last_refill_at) "
                "VALUES (:b, :cap, :cap, :rate, (now() AT TIME ZONE 'UTC')), "
                "(:k, 0, 0, 0, to_timestamp(0) AT TIME ZONE 'UTC') "
                "ON CONFLICT (domain) DO NOTHING"
            ),
            {"b": BUCKET_ROW, "k": BREAKER_ROW, "cap": cfg.burst, "rate": cfg.rate},
        )
        self._seeded = True

    def _rows(self, conn):
        from sqlalchemy import text

        rows = conn.execute(
            text(
                "SELECT domain, tokens, max_tokens, EXTRACT(EPOCH FROM last_refill_at), "
                "EXTRACT(EPOCH FROM clock_timestamp()) "
                "FROM rate_limit_bucket WHERE domain IN (:b, :k) ORDER BY domain FOR UPDATE"
            ),
            {"b": BUCKET_ROW, "k": BREAKER_ROW},
        ).fetchall()
        return {r[0]: r for r in rows}

    def load(self) -> GateState:
        with self.engine.begin() as conn:
            rows = self._rows(conn)
        b, k = rows[BUCKET_ROW], rows[BREAKER_ROW]
        return GateState(
            tokens=float(b[1]), updated=float(b[3]), blocked_until=float(k[3]), strikes=int(k[1]), trips=int(k[2])
        )

    def _transact(self, cfg, fn):
        try:
            return self._transact_once(cfg, fn)
        except Exception as e:
            if getattr(getattr(e, "orig", None), "pgcode", None) in _CONTENDED_PGCODES:
                raise _Contended(str(e)) from e
            raise

    def _transact_once(self, cfg, fn):
        from sqlalchemy import text

        with self.engine.begin() as conn:
            if not self._seeded:
                self._seed(conn, cfg)
            rows = self._rows(conn)
            if BUCKET_ROW not in rows or BREAKER_ROW not in rows:
                self._seed(conn, cfg)
                rows = self._rows(conn)
            b, k = rows[BUCKET_ROW], rows[BREAKER_ROW]
            now = float(b[4])
            st = GateState(
                tokens=float(b[1]),
                updated=float(b[3] or 0.0),
                blocked_until=float(k[3] or 0.0),
                strikes=int(k[1] or 0),
                trips=int(k[2] or 0),
            )
            before_bucket = (st.tokens, st.updated)
            before_breaker = (st.strikes, st.trips, st.blocked_until)
            result = fn(st, now)
            # Write only what changed: the row lock is held per round trip (~40 ms
            # through the Cloud SQL proxy). An empty-bucket poll changes nothing.
            if (st.tokens, st.updated) != before_bucket or float(b[2] or 0.0) != cfg.burst:
                conn.execute(
                    text(
                        "UPDATE rate_limit_bucket SET tokens = :t, max_tokens = :cap, refill_rate = :rate, "
                        "last_refill_at = to_timestamp(:u) AT TIME ZONE 'UTC' WHERE domain = :b"
                    ),
                    {"t": st.tokens, "cap": cfg.burst, "rate": cfg.rate, "u": st.updated, "b": BUCKET_ROW},
                )
            if (st.strikes, st.trips, st.blocked_until) != before_breaker:
                conn.execute(
                    text(
                        "UPDATE rate_limit_bucket SET tokens = :s, max_tokens = :tr, refill_rate = 0, "
                        "last_refill_at = to_timestamp(:bu) AT TIME ZONE 'UTC' WHERE domain = :k"
                    ),
                    {"s": st.strikes, "tr": st.trips, "bu": st.blocked_until, "k": BREAKER_ROW},
                )
            return result


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


class SecGate:
    POLL_CAP = 5.0  # re-check the shared state at least this often while waiting
    FALLBACK_RETRY = 30.0  # seconds before retrying the shared backend after a failure

    def __init__(
        self,
        backend: _Backend,
        cfg: Optional[GateConfig] = None,
        sleep: Callable[[float], None] = time.sleep,
        async_sleep=asyncio.sleep,
        rng: Optional[random.Random] = None,
    ):
        self.backend = backend
        self.cfg = cfg or GateConfig()
        self._sleep = sleep
        self._async_sleep = async_sleep
        self._rng = rng or random.Random()
        self._sync_lock = threading.Lock()
        self._async_locks: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
        self._dirty = False  # this process saw strikes > 0
        self._fallback: Optional[LocalBackend] = None
        self._fallback_until = 0.0

    # -- backend with fail-safe fallback ------------------------------------
    def _shared(self, method: str, *args):
        """Call the shared backend. Row-lock contention is "busy", never "down": a
        take reports a short wait; a strike/success is retried a few times."""
        attempts = 1 if method == "take" else 4
        for i in range(attempts):
            try:
                return getattr(self.backend, method)(self.cfg, *args)
            except _Contended:
                if method == "take":
                    return TakeResult(CONTENDED_WAIT, time.monotonic(), 0)
                if i == attempts - 1:
                    raise
                self._sleep(CONTENDED_WAIT)

    def _call(self, method: str, *args):
        if self._fallback is not None and time.monotonic() < self._fallback_until:
            return getattr(self._fallback, method)(replace(self.cfg, rate=self.cfg.fallback_rate, burst=1.0), *args)
        try:
            result = self._shared(method, *args)
            if self._fallback is not None:
                logger.warning("SEC gate: shared backend reachable again; leaving in-process fallback")
                self._fallback = None
            return result
        except Exception as e:
            if self._fallback is None:
                logger.error(
                    f"SEC gate: shared backend unavailable ({e!r}); falling back to an in-process "
                    f"bucket at {self.cfg.fallback_rate} req/s"
                )
                self._fallback = LocalBackend()
            self._fallback_until = time.monotonic() + self.FALLBACK_RETRY
            return getattr(self._fallback, method)(replace(self.cfg, rate=self.cfg.fallback_rate, burst=1.0), *args)

    def _take(self) -> TakeResult:
        res = self._call("take")
        if res.strikes > 0:
            self._dirty = True
        return res

    def _next_sleep(self, wait: float) -> float:
        return min(wait, self.POLL_CAP) * (1.0 + self._rng.uniform(0.0, 0.1))

    def _too_long(self, wait: float, waited: float, budget: float) -> Optional[SecRateLimited]:
        if waited + wait > budget:
            return SecRateLimited(
                f"SEC calls paused for {wait:.0f}s more (cooldown or circuit breaker); not waiting past {budget:.0f}s",
                wait,
            )
        return None

    # -- acquire --------------------------------------------------------------
    def acquire(self, max_wait: Optional[float] = None) -> None:
        budget = self.cfg.max_wait if max_wait is None else max_wait
        queued_from = time.monotonic()
        if not self._sync_lock.acquire(timeout=max(0.001, budget)):
            raise SecRateLimited("SEC gate busy", budget)
        try:
            # time queued behind other threads counts toward the budget (the 2 s
            # event-loop cap must hold in total, not per phase)
            waited = time.monotonic() - queued_from
            while True:
                res = self._take()
                if res.wait <= 0:
                    return
                err = self._too_long(res.wait, waited, budget)
                if err:
                    raise err
                s = self._next_sleep(res.wait)
                self._sleep(s)
                waited += s
        finally:
            self._sync_lock.release()

    async def acquire_async(self, max_wait: Optional[float] = None) -> None:
        budget = self.cfg.max_wait if max_wait is None else max_wait
        loop = asyncio.get_running_loop()
        lock = self._async_locks.get(loop)
        if lock is None:
            lock = self._async_locks[loop] = asyncio.Lock()
        queued_from = time.monotonic()
        try:
            await asyncio.wait_for(lock.acquire(), timeout=max(0.001, budget))
        except asyncio.TimeoutError:
            raise SecRateLimited("SEC gate busy", budget) from None
        try:
            waited = time.monotonic() - queued_from
            while True:
                res = await asyncio.to_thread(self._take) if self._blocking_backend() else self._take()
                if res.wait <= 0:
                    return
                err = self._too_long(res.wait, waited, budget)
                if err:
                    raise err
                s = self._next_sleep(res.wait)
                await self._async_sleep(s)
                waited += s
        finally:
            lock.release()

    def _blocking_backend(self) -> bool:
        return not isinstance(self.backend, LocalBackend)

    # -- outcomes -------------------------------------------------------------
    def record(self, status: int, retry_after: Optional[str] = None) -> None:
        if status in STRIKE_STATUS:
            ra = parse_retry_after(retry_after)
            res = self._call("strike", ra, self._rng)
            self._dirty = True
            if res.tripped:
                logger.error(
                    f"SEC circuit breaker OPEN: {res.strikes} consecutive 429/403 from SEC; "
                    f"all NexData SEC calls paused for {res.cooldown / 60:.1f} min"
                )
            else:
                logger.warning(
                    f"SEC HTTP {status} (strike {res.strikes}, Retry-After={retry_after!r}); "
                    f"all NexData SEC calls wait {res.cooldown:.1f}s"
                )
        elif status < 400 and self._dirty:
            closed = self._call("success")
            self._dirty = False
            if closed:
                logger.warning("SEC circuit breaker closed: a SEC request succeeded; normal rate resumes")

    async def record_async(self, status: int, retry_after: Optional[str] = None) -> None:
        if (status in STRIKE_STATUS or (status < 400 and self._dirty)) and self._blocking_backend():
            await asyncio.to_thread(self.record, status, retry_after)
        else:
            self.record(status, retry_after)


# ---------------------------------------------------------------------------
# Process singleton
# ---------------------------------------------------------------------------

_GATE: Optional[SecGate] = None
_GATE_LOCK = threading.Lock()


def _config_from_settings() -> GateConfig:
    from app.core.config import get_settings

    s = get_settings()
    return GateConfig(
        rate=s.sec_rate_limit_rps,
        burst=s.sec_rate_limit_burst,
        max_wait=s.sec_gate_max_wait,
        breaker_threshold=s.sec_breaker_threshold,
        breaker_seconds=s.sec_breaker_minutes * 60.0,
    )


def _build_gate() -> SecGate:
    cfg = _config_from_settings()
    from sqlalchemy import create_engine

    from app.core.config import get_settings

    # Bounded DB waits: a transaction stuck elsewhere holding the gate rows (or a
    # hung proxy) must cost a SEC caller -- possibly on the api event loop -- at
    # most ~1 s, after which the take reports "busy" (lock) or the process falls
    # back to its own 1 req/s bucket (connect / other errors).
    engine = create_engine(
        get_settings().database_url,
        pool_size=1,
        max_overflow=1,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=5,
        connect_args={
            "connect_timeout": 5,
            "options": "-c lock_timeout=1000 -c statement_timeout=5000",
        },
    )
    logger.info(f"SEC gate: {cfg.rate} req/s burst {cfg.burst}, shared via rate_limit_bucket '{BUCKET_ROW}'")
    return SecGate(PostgresBackend(engine), cfg)


def get_sec_gate() -> SecGate:
    global _GATE
    if _GATE is None:
        with _GATE_LOCK:
            if _GATE is None:
                _GATE = _build_gate()
    return _GATE


def set_sec_gate(gate: Optional[SecGate]) -> None:
    global _GATE
    _GATE = gate


def reset_sec_gate() -> None:
    set_sec_gate(None)


# ---------------------------------------------------------------------------
# httpx integration
# ---------------------------------------------------------------------------

SYNC_IN_LOOP_MAX_WAIT = 2.0


def _in_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _prepare(request: httpx.Request) -> bool:
    """True when the request must be gated (SEC host, not already inside the gate)."""
    if _IN_GATE.get() or not is_sec_host(request.url.host):
        return False
    request.headers["User-Agent"] = sec_user_agent()
    return True


def gated_sync(orig):
    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if not _prepare(request):
            return orig(self, request)
        gate = get_sec_gate()
        # A sync client called from inside an event loop must not block the loop
        # for a long cooldown: wait at most a couple of seconds, then fail fast.
        gate.acquire(max_wait=min(gate.cfg.max_wait, SYNC_IN_LOOP_MAX_WAIT) if _in_event_loop() else None)
        token = _IN_GATE.set(True)
        try:
            response = orig(self, request)
        finally:
            _IN_GATE.reset(token)
        gate.record(response.status_code, response.headers.get("Retry-After"))
        return response

    handle_request.__sec_gated__ = True
    return handle_request


def gated_async(orig):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if not _prepare(request):
            return await orig(self, request)
        gate = get_sec_gate()
        await gate.acquire_async()
        token = _IN_GATE.set(True)
        try:
            response = await orig(self, request)
        finally:
            _IN_GATE.reset(token)
        await gate.record_async(response.status_code, response.headers.get("Retry-After"))
        return response

    handle_async_request.__sec_gated__ = True
    return handle_async_request


class SecGatedTransport(httpx.BaseTransport):
    """Explicit gated wrapper (SecHttp); works with or without :func:`install`."""

    def __init__(self, inner: httpx.BaseTransport):
        self.inner = inner
        self._handle = gated_sync(lambda _self, request: inner.handle_request(request))

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        return self._handle(self, request)

    def close(self) -> None:
        self.inner.close()


_ORIGINALS: dict = {}
_INSTALL_LOCK = threading.Lock()


def install() -> None:
    """Gate every httpx transport in this process (idempotent)."""
    with _INSTALL_LOCK:
        if _ORIGINALS:
            return
        _ORIGINALS["sync"] = httpx.HTTPTransport.handle_request
        _ORIGINALS["async"] = httpx.AsyncHTTPTransport.handle_async_request
        httpx.HTTPTransport.handle_request = gated_sync(_ORIGINALS["sync"])
        httpx.AsyncHTTPTransport.handle_async_request = gated_async(_ORIGINALS["async"])
        logger.info("SEC gate installed on httpx transports")


def uninstall() -> None:
    with _INSTALL_LOCK:
        if not _ORIGINALS:
            return
        httpx.HTTPTransport.handle_request = _ORIGINALS.pop("sync")
        httpx.AsyncHTTPTransport.handle_async_request = _ORIGINALS.pop("async")


def installed() -> bool:
    return bool(_ORIGINALS)


# ---------------------------------------------------------------------------
# Phase concurrency (people jobs' SEC phase)
# ---------------------------------------------------------------------------

_PHASE_SEMS: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


@asynccontextmanager
async def sec_phase_slot(name: str = "people_sec", limit: Optional[int] = None):
    """Cap concurrent SEC phases per process (``SEC_PHASE_CONCURRENCY``)."""
    if limit is None:
        try:
            from app.core.config import get_settings

            limit = get_settings().sec_phase_concurrency
        except Exception:
            limit = 2
    limit = max(1, int(limit))
    loop = asyncio.get_running_loop()
    sems = _PHASE_SEMS.setdefault(loop, {})
    key = (name, limit)
    sem = sems.get(key)
    if sem is None:
        sem = sems[key] = asyncio.Semaphore(limit)
    async with sem:
        yield
