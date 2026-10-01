"""
Tests for SPEC 146 — SEC fair-access gate (process-wide + cross-worker).

T13 (Postgres backend) needs TEST_PG_URL pointing at a DISPOSABLE database.
"""

import asyncio
import logging
import os
import random
import re
import threading
import time
from pathlib import Path

import httpx
import pytest

from app.core import sec_gate as sg

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self, start=1000.0):
        self.t = start
        self.sleeps = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s

    async def async_sleep(self, s):
        self.sleep(s)


class Recording(sg.LocalBackend):
    """LocalBackend that records the backend time of every granted token."""

    def __init__(self, clock=time.monotonic):
        super().__init__(clock=clock)
        self.grants = []

    def take(self, cfg):
        res = super().take(cfg)
        if res.wait <= 0:
            self.grants.append(res.now)
        return res


def _max_in_window(stamps, window=1.0):
    stamps = sorted(stamps)
    best = 0
    j = 0
    for i in range(len(stamps)):
        while stamps[i] - stamps[j] >= window:
            j += 1
        best = max(best, i - j + 1)
    return best


def _fake_gate(clock, backend=None, **cfg):
    defaults = dict(
        rate=5.0,
        burst=2.0,
        max_wait=120.0,
        breaker_threshold=3,
        breaker_seconds=600.0,
        backoff_base=5.0,
        backoff_cap=600.0,
    )
    defaults.update(cfg)
    backend = backend or Recording(clock=clock.now)
    return sg.SecGate(
        backend, sg.GateConfig(**defaults), sleep=clock.sleep, async_sleep=clock.async_sleep, rng=random.Random(7)
    )


@pytest.fixture
def ua_env(monkeypatch):
    from app.core.config import reset_settings

    if not os.environ.get("DATABASE_URL"):
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/unused")
    monkeypatch.setenv("SEC_USER_AGENT", "Gate Test gate-test@example.com")
    reset_settings()
    yield "Gate Test gate-test@example.com"
    reset_settings()


# ---------------------------------------------------------------------------
# rate
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRate:
    RATE = 20.0
    BURST = 2.0

    def _gate(self, backend):
        return sg.SecGate(backend, sg.GateConfig(rate=self.RATE, burst=self.BURST, max_wait=30.0))

    def test_concurrent_threads_never_exceed_rate(self):
        """T1: 8 threads x 6 calls through one gate stay within rate + burst per second."""
        backend = Recording()
        gate = self._gate(backend)
        start = time.monotonic()

        def worker():
            for _ in range(6):
                gate.acquire()

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start
        assert len(backend.grants) == 48
        assert _max_in_window(backend.grants) <= self.RATE + self.BURST
        # 48 tokens at 20/s with burst 2 cannot be granted faster than (48-2)/20 s
        assert elapsed >= (48 - self.BURST) / self.RATE - 0.05

    def test_concurrent_async_tasks_never_exceed_rate(self):
        """T2: 30 asyncio tasks x 2 calls stay within rate + burst per second."""
        backend = Recording()
        gate = self._gate(backend)

        async def run():
            async def one():
                for _ in range(2):
                    await gate.acquire_async()

            await asyncio.gather(*[one() for _ in range(30)])

        asyncio.run(run())
        assert len(backend.grants) == 60
        assert _max_in_window(backend.grants) <= self.RATE + self.BURST

    def test_two_gates_share_backend(self):
        """T3: two gates (= two processes) on one backend share one budget."""
        backend = Recording()
        g1, g2 = self._gate(backend), self._gate(backend)

        def worker(g):
            for _ in range(12):
                g.acquire()

        threads = [threading.Thread(target=worker, args=(g,)) for g in (g1, g2, g1, g2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(backend.grants) == 48
        assert _max_in_window(backend.grants) <= self.RATE + self.BURST


# ---------------------------------------------------------------------------
# Retry-After, backoff, fast fail, breaker
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBackoffAndBreaker:
    def test_retry_after_blocks_all_callers(self):
        """T4: a 429 with Retry-After: 30 blocks this gate and another gate on the same backend."""
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        g1 = _fake_gate(clock, backend)
        g2 = _fake_gate(clock, backend)
        g1.acquire()
        t0 = clock.now()
        g1.record(429, "30")
        g2.acquire()  # other "process" waits too
        assert clock.now() - t0 >= 30.0
        assert backend.grants[-1] - t0 >= 30.0

    def test_retry_after_http_date(self):
        """T4b: HTTP-date Retry-After is parsed."""
        assert sg.parse_retry_after("120") == 120.0
        assert sg.parse_retry_after(None) is None
        assert sg.parse_retry_after("garbage") is None
        from email.utils import format_datetime
        from datetime import datetime, timedelta, timezone

        when = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=90), usegmt=True)
        assert 80 <= sg.parse_retry_after(when) <= 91

    def test_backoff_exponential_with_jitter(self):
        """T5: without Retry-After the cooldown doubles per strike and is jittered."""
        cfg = sg.GateConfig(backoff_base=5.0, backoff_cap=600.0, breaker_threshold=100)
        cooldowns = {}
        for seed in (1, 2):
            st = sg.GateState(tokens=2.0, updated=0.0)
            rng = random.Random(seed)
            seen = []
            for k in range(1, 6):
                now = 1000.0 * k
                sg.strike(st, now, None, cfg, rng)
                seen.append(st.blocked_until - now)
            cooldowns[seed] = seen
        for seen in cooldowns.values():
            for k, c in enumerate(seen, start=1):
                full = 5.0 * 2 ** (k - 1)
                assert full / 2 <= c <= full
        assert cooldowns[1] != cooldowns[2]  # jitter, not a fixed interval
        assert all(c != 60.0 for c in cooldowns[1])

    def test_long_wait_raises_without_sleeping(self):
        """T6: Retry-After longer than max_wait -> SecRateLimited at once, no sleep, no request."""
        clock = FakeClock()
        gate = _fake_gate(clock)
        gate.record(429, "600")
        with pytest.raises(sg.SecRateLimited) as ei:
            gate.acquire(max_wait=120)
        assert clock.sleeps == []
        assert ei.value.wait >= 599
        assert isinstance(ei.value, httpx.TransportError)

    def test_breaker_trips_once_and_resets(self, caplog):
        """T7: 3 strikes open the breaker for 10 min, logged once; success closes it, logged once."""
        caplog.set_level(logging.INFO, logger="app.core.sec_gate")
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        g1 = _fake_gate(clock, backend)
        g2 = _fake_gate(clock, backend)
        for g in (g1, g2, g1):
            # each strike is a new probe after the previous cooldown ended
            clock.t = max(clock.t, backend.state.blocked_until if backend.state else 0.0)
            g.record(429, None)
        g2.record(403, None)  # in-flight 403 while already open: no second trip log
        opened = [r for r in caplog.records if "circuit breaker OPEN" in r.getMessage()]
        assert len(opened) == 1
        assert backend.state.trips == 1
        with pytest.raises(sg.SecRateLimited):
            g2.acquire(max_wait=120)
        assert backend.state.blocked_until - clock.now() >= 600 - 1
        clock.t = backend.state.blocked_until  # pause elapses
        g1.acquire()
        g1.record(200, None)
        g2.record(200, None)
        closed = [r for r in caplog.records if "circuit breaker closed" in r.getMessage()]
        assert len(closed) == 1
        assert backend.state.strikes == 0
        # a single strike after reset does not re-open the breaker
        g1.record(429, None)
        assert backend.state.trips == 1


# ---------------------------------------------------------------------------
# coverage: httpx hook, SecHttp, people collector, phase slot, UA
# ---------------------------------------------------------------------------


class FakePool:
    def __init__(self, status=200):
        self.status = status
        self.requests = []

    def handle_request(self, request):
        import httpcore

        self.requests.append(request)
        return httpcore.Response(
            self.status, headers=[(b"Retry-After", b"30")] if self.status == 429 else [], content=b"ok body"
        )

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None


def _ua_of(core_request):
    return dict((k.decode().lower(), v.decode()) for k, v in core_request.headers)["user-agent"]


@pytest.mark.unit
class TestCoverage:
    def test_transport_hook_gates_and_sets_ua(self, ua_env):
        """T8: after install() every httpx client to a SEC host is gated and carries SEC_USER_AGENT."""
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        sg.set_sec_gate(_fake_gate(clock, backend))
        sg.install()
        try:
            sg.install()  # idempotent
            pool = FakePool()
            transport = httpx.HTTPTransport()
            transport._pool = pool
            with httpx.Client(transport=transport, headers={"User-Agent": "Mozilla/5.0"}) as client:
                assert client.get("https://www.sec.gov/cgi-bin/browse-edgar").status_code == 200
                assert client.get("https://data.sec.gov/submissions/CIK0000320193.json").status_code == 200
                assert client.get("https://example.com/").status_code == 200
            assert len(backend.grants) == 2  # example.com not gated
            assert _ua_of(pool.requests[0]) == ua_env
            assert _ua_of(pool.requests[1]) == ua_env
            assert _ua_of(pool.requests[2]) == "Mozilla/5.0"
            pool.status = 429
            with httpx.Client(transport=transport) as client:
                assert client.get("https://efts.sec.gov/LATEST/search-index?q=x").status_code == 429
            assert backend.state.strikes == 1
            assert backend.state.blocked_until - clock.now() >= 30
        finally:
            sg.uninstall()
        assert not sg.installed()

    def test_async_transport_hook(self, ua_env):
        """T8b: the async transport is gated too."""
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        sg.set_sec_gate(_fake_gate(clock, backend))
        seen = []

        async def orig(self, request):
            seen.append(request.headers.get("user-agent"))
            return httpx.Response(200, text="ok")

        wrapped = sg.gated_async(orig)

        async def run():
            await wrapped(None, httpx.Request("GET", "https://www.sec.gov/x", headers={"User-Agent": "bad"}))
            await wrapped(None, httpx.Request("GET", "https://example.org/x", headers={"User-Agent": "bad"}))

        asyncio.run(run())
        assert seen == [ua_env, "bad"]
        assert len(backend.grants) == 1

    def test_sync_client_inside_event_loop_fails_fast(self, ua_env):
        """T8c: a sync httpx call made from inside an event loop never blocks it through a cooldown."""
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        gate = _fake_gate(clock, backend)
        sg.set_sec_gate(gate)
        gate.record(429, "30")
        wrapped = sg.gated_sync(lambda self, request: httpx.Response(200))

        async def in_loop():
            wrapped(None, httpx.Request("GET", "https://www.sec.gov/x"))

        with pytest.raises(sg.SecRateLimited):
            asyncio.run(in_loop())
        assert clock.sleeps == []
        # outside a loop the same call waits the 30 s out and succeeds
        assert wrapped(None, httpx.Request("GET", "https://www.sec.gov/x")).status_code == 200
        assert sum(clock.sleeps) >= 30

    def test_sec_http_gated_once(self, ua_env):
        """T9: SecHttp is gated exactly once per request even with the global hook installed."""
        from app.core.sec_http import SecHttp

        clock = FakeClock()
        backend = Recording(clock=clock.now)
        sg.set_sec_gate(_fake_gate(clock, backend))
        sg.install()
        try:
            pool = FakePool()
            transport = httpx.HTTPTransport()
            transport._pool = pool
            http = SecHttp(transport=transport)
            assert http.get_text("https://www.sec.gov/Archives/x.txt") == "ok body"
            assert len(backend.grants) == 1
            assert _ua_of(pool.requests[0]) == ua_env
        finally:
            sg.uninstall()

    def test_sec_http_gated_without_hook(self, ua_env):
        """T9b: SecHttp is gated even when install() was never called."""
        from app.core.sec_http import SecHttp

        clock = FakeClock()
        backend = Recording(clock=clock.now)
        sg.set_sec_gate(_fake_gate(clock, backend))
        seen = []

        def handler(request):
            seen.append(request.headers.get("user-agent"))
            return httpx.Response(200, text="ok body")

        http = SecHttp(transport=httpx.MockTransport(handler))
        assert http.get_text("https://www.sec.gov/x") == "ok body"
        assert len(backend.grants) == 1
        assert seen == [ua_env]

    def test_people_collector_uses_gate_no_fixed_sleep(self, ua_env, monkeypatch):
        """T10: the aiohttp people collector is gated, sends SEC_USER_AGENT, and never sleeps 60 s on 429."""
        from app.sources.people_collection import base_collector as bc

        clock = FakeClock()
        backend = Recording(clock=clock.now)
        sg.set_sec_gate(_fake_gate(clock, backend))
        sleeps = []

        async def fake_sleep(s):
            sleeps.append(s)

        monkeypatch.setattr(bc.asyncio, "sleep", fake_sleep)

        class Resp:
            def __init__(self, status):
                self.status = status
                self.headers = {}

            async def text(self):
                return "body"

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        class Session:
            closed = False

            def __init__(self):
                self.calls = []

            def get(self, url, headers=None, allow_redirects=True):
                self.calls.append(headers)
                return Resp(429)

            async def close(self):
                pass

        collector = bc.BaseCollector(source_type="sec_edgar")
        session = Session()
        collector._session = session
        result = asyncio.run(collector.fetch_url("https://www.sec.gov/cgi-bin/browse-edgar?x=1", use_cache=False))
        assert result is None
        assert len(session.calls) == 1  # no in-place retry against SEC
        assert session.calls[0]["User-Agent"] == ua_env
        assert not any(s >= 60 for s in sleeps)
        assert len(backend.grants) == 1
        assert backend.state.strikes == 1

    def test_phase_slot_caps_concurrency(self):
        """T11: sec_phase_slot admits at most `limit` concurrent SEC phases."""
        state = {"now": 0, "max": 0}

        async def phase():
            async with sg.sec_phase_slot("t11", limit=2):
                state["now"] += 1
                state["max"] = max(state["max"], state["now"])
                await asyncio.sleep(0.02)
                state["now"] -= 1

        async def run():
            await asyncio.gather(*[phase() for _ in range(6)])

        asyncio.run(run())
        assert state["max"] == 2

    def test_sec_agent_collect_uses_phase_slot(self, monkeypatch):
        """T11b: SECAgent.collect runs inside the people SEC phase slot."""
        from app.sources.people_collection.sec_agent import SECAgent

        monkeypatch.setenv("SEC_PHASE_CONCURRENCY", "1")
        from app.core.config import reset_settings

        if not os.environ.get("DATABASE_URL"):
            monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/unused")
        reset_settings()
        state = {"now": 0, "max": 0}

        async def inner(self, *a, **kw):
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
            await asyncio.sleep(0.02)
            state["now"] -= 1
            return "ok"

        monkeypatch.setattr(SECAgent, "_collect_unthrottled", inner)
        agent = SECAgent.__new__(SECAgent)

        async def run():
            return await asyncio.gather(*[agent.collect(company_id=i, company_name="X", cik="1") for i in range(3)])

        assert asyncio.run(run()) == ["ok"] * 3
        assert state["max"] == 1
        reset_settings()

    def test_user_agent_setting(self, monkeypatch):
        """T12: one setting; blank falls back to the default; no personal e-mail anywhere in app/."""
        from app.core.config import get_settings, reset_settings

        if not os.environ.get("DATABASE_URL"):
            monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/unused")
        monkeypatch.delenv("SEC_USER_AGENT", raising=False)
        reset_settings()
        default = get_settings().sec_user_agent
        assert default == "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"
        monkeypatch.setenv("SEC_USER_AGENT", "   ")
        reset_settings()
        assert get_settings().sec_user_agent == default
        assert sg.sec_user_agent() == default
        reset_settings()
        offenders = []
        for path in (REPO / "app").rglob("*.py"):
            if re.search(r"@(gmail|yahoo|hotmail|outlook)\.com", path.read_text(encoding="utf-8", errors="ignore")):
                offenders.append(str(path))
        assert offenders == []

    def test_is_sec_host(self):
        assert sg.is_sec_host("www.sec.gov")
        assert sg.is_sec_host("efts.sec.gov")
        assert sg.is_sec_host("SEC.GOV")
        assert not sg.is_sec_host("notsec.gov")
        assert not sg.is_sec_host("sec.gov.evil.com")
        assert not sg.is_sec_host(None)


# ---------------------------------------------------------------------------
# T13: Postgres backend (disposable DB only)
# ---------------------------------------------------------------------------


@pg
@pytest.mark.integration
class TestPostgresBackend:
    @pytest.fixture
    def engine(self):
        from sqlalchemy import create_engine, text

        from app.core.models_queue import RateLimitBucket

        eng = create_engine(PG_URL)
        RateLimitBucket.__table__.create(bind=eng, checkfirst=True)
        with eng.begin() as c:
            c.execute(text("DELETE FROM rate_limit_bucket WHERE domain IN ('sec.gov', 'sec.gov#breaker')"))
        yield eng
        eng.dispose()

    def test_postgres_backend_rate_and_breaker(self, engine):
        # backoff_base=0: the first strike's cooldown ends at once, so the second
        # strike is a new consecutive one (strikes inside a cooldown don't count)
        cfg = sg.GateConfig(rate=5.0, burst=2.0, breaker_threshold=2, breaker_seconds=1.0, backoff_base=0.0)
        b1, b2 = sg.PostgresBackend(engine), sg.PostgresBackend(engine)
        granted = [b.take(cfg).wait <= 0 for b in (b1, b2, b1)]
        assert granted == [True, True, False]  # burst 2 shared by both "processes"
        tripped = [b1.strike(cfg, None, random.Random(1)).tripped, b2.strike(cfg, None, random.Random(1)).tripped]
        assert tripped == [False, True]
        res = b1.take(cfg)
        assert res.wait >= 0.9
        assert b2.success(cfg) is False  # stale success during the pause: breaker stays open
        assert b1.load().strikes == 2
        time.sleep(1.1)
        assert b1.take(cfg).wait <= 0
        assert b2.success(cfg) is True
        assert b1.load().strikes == 0


def _pg_worker(url, n, q):
    from sqlalchemy import create_engine

    eng = create_engine(url, pool_size=1, max_overflow=0)
    backend = sg.PostgresBackend(eng)
    cfg = sg.GateConfig(rate=10.0, burst=2.0, max_wait=60.0)
    grants = []
    while len(grants) < n:
        res = backend.take(cfg)
        if res.wait <= 0:
            grants.append(res.now)
        else:
            time.sleep(min(res.wait, 0.5))
    q.put(grants)
    eng.dispose()


@pg
@pytest.mark.integration
def test_postgres_backend_separate_processes_share_rate():
    """T13b: 4 OS processes on one DB never exceed rate + burst in any 1 s window (DB clock)."""
    import multiprocessing as mp

    from sqlalchemy import create_engine, text

    from app.core.models_queue import RateLimitBucket

    eng = create_engine(PG_URL)
    RateLimitBucket.__table__.create(bind=eng, checkfirst=True)
    with eng.begin() as c:
        c.execute(text("DELETE FROM rate_limit_bucket WHERE domain IN ('sec.gov', 'sec.gov#breaker')"))
    eng.dispose()
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_pg_worker, args=(PG_URL, 8, q)) for _ in range(4)]
    for p in procs:
        p.start()
    stamps = []
    for _ in procs:
        stamps.extend(q.get(timeout=120))
    for p in procs:
        p.join(timeout=30)
    assert len(stamps) == 32
    assert _max_in_window(stamps) <= 10 + 2


# ---------------------------------------------------------------------------
# T14-T19: adversarial review fixes (2026-09-30)
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.engine.statements.append(sql)
        if self.engine.raise_on_select is not None and "FOR UPDATE" in sql:
            raise self.engine.raise_on_select
        if sql.lstrip().upper().startswith("SELECT"):
            e = self.engine
            return _FakeResult(
                [
                    (sg.BUCKET_ROW, e.tokens, 2.0, e.updated, e.now),
                    (sg.BREAKER_ROW, e.strikes, e.trips, e.blocked_until, e.now),
                ]
            )
        return _FakeResult()


class _FakeEngine:
    """Records the SQL PostgresBackend sends; serves the two gate rows."""

    def __init__(self, tokens=2.0, updated=1000.0, now=1000.0, strikes=0, trips=0, blocked_until=0.0):
        self.tokens, self.updated, self.now = tokens, updated, now
        self.strikes, self.trips, self.blocked_until = strikes, trips, blocked_until
        self.statements = []
        self.raise_on_select = None

    def begin(self):
        engine = self

        class _Ctx:
            def __enter__(self_inner):
                return _FakeConn(engine)

            def __exit__(self_inner, *a):
                return False

        return _Ctx()

    def updates(self):
        return [s for s in self.statements if s.lstrip().upper().startswith("UPDATE")]


@pytest.mark.unit
class TestReviewFixes:
    def test_inflight_success_does_not_close_open_breaker(self, caplog):
        """T14: a 200 from a request admitted before the trip neither resets strikes nor logs 'closed'
        while the pause is running; after the pause one more 429 re-opens it (half-open)."""
        caplog.set_level(logging.INFO, logger="app.core.sec_gate")
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        gate = _fake_gate(clock, backend, backoff_base=0.0)
        for _ in range(3):
            gate.record(429, None)
            clock.t += 0.001
        assert backend.state.trips == 1
        gate.record(200, None)  # stale in-flight success during the 10-min pause
        assert backend.state.strikes == 3
        assert not [r for r in caplog.records if "circuit breaker closed" in r.getMessage()]
        with pytest.raises(sg.SecRateLimited):
            gate.acquire(max_wait=120)
        clock.t = backend.state.blocked_until
        gate.acquire()
        gate.record(429, None)  # half-open probe fails -> breaker re-opens for the full pause
        assert backend.state.trips == 2
        assert backend.state.blocked_until - clock.now() >= 600 - 1

    def test_strikes_during_cooldown_do_not_escalate(self):
        """T15: 429s from requests already in flight when a cooldown began count once (but their
        Retry-After still extends the pause); a 429 after the cooldown is a new strike."""
        clock = FakeClock()
        backend = Recording(clock=clock.now)
        gate = _fake_gate(clock, backend)
        gate.record(429, None)
        gate.record(429, None)
        gate.record(429, "40")
        assert backend.state.strikes == 1
        assert backend.state.trips == 0
        assert backend.state.blocked_until - clock.now() >= 40
        clock.t = backend.state.blocked_until
        gate.record(429, None)
        assert backend.state.strikes == 2

    def test_strike_count_cannot_overflow(self):
        """T16: after a long block (thousands of strikes) the backoff maths cannot raise OverflowError
        (which would push the process into the per-process fallback and past the shared breaker)."""
        cfg = sg.GateConfig()
        st = sg.GateState(tokens=0.0, updated=0.0, strikes=5000)
        sg.strike(st, 10.0, None, cfg, random.Random(1))
        assert st.blocked_until >= 10.0 + cfg.breaker_seconds

    def test_lock_contention_is_a_wait_not_a_fallback(self):
        """T17: a Postgres lock_timeout on the gate rows means 'busy', not 'backend down': the
        process must not switch to its own bucket (which ignores the shared rate and breaker)."""
        from sqlalchemy.exc import OperationalError

        class LockNotAvailable(Exception):
            pgcode = "55P03"

        engine = _FakeEngine()
        engine.raise_on_select = OperationalError("SELECT ... FOR UPDATE", {}, LockNotAvailable())
        clock = FakeClock()
        gate = _fake_gate(clock, sg.PostgresBackend(engine))
        with pytest.raises(sg.SecRateLimited):
            gate.acquire(max_wait=3)
        assert gate._fallback is None
        assert sum(clock.sleeps) > 0

    def test_build_gate_bounds_db_waits(self, ua_env, monkeypatch):
        """T18: the gate's engine bounds connect, row-lock and statement time, so a stuck
        transaction elsewhere can never block a SEC caller (or the event loop) indefinitely."""
        import sqlalchemy

        seen = {}

        def fake_create_engine(url, **kw):
            seen.update(kw)
            return object()

        monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
        sg._build_gate()
        args = seen.get("connect_args", {})
        assert args.get("connect_timeout")
        assert "lock_timeout" in args.get("options", "")
        assert "statement_timeout" in args.get("options", "")

    def test_failed_take_writes_nothing_and_grant_writes_bucket_only(self):
        """T19: the row lock is held for the fewest round trips (Cloud SQL proxy RTT ~40 ms):
        an empty-bucket poll writes no row; a grant writes the bucket row only."""
        engine = _FakeEngine(tokens=0.0, updated=1000.0, now=1000.0)
        backend = sg.PostgresBackend(engine)
        backend._seeded = True
        cfg = sg.GateConfig()
        assert backend.take(cfg).wait > 0
        assert engine.updates() == []
        engine.statements.clear()
        engine.now = 1001.0
        assert backend.take(cfg).wait == 0
        ups = engine.updates()
        assert len(ups) == 1 and ":b" in ups[0]

    def test_sync_lock_wait_counts_against_budget(self):
        """T20: time spent queued behind another thread counts toward max_wait, so a sync caller
        inside the event loop is never blocked longer than its 2 s budget in total."""
        clock = FakeClock()
        gate = _fake_gate(clock, backoff_base=0.0)
        gate.record(429, "1.5")
        holder_in = threading.Event()

        def hold():
            with gate._sync_lock:
                holder_in.set()
                time.sleep(1.0)

        t = threading.Thread(target=hold)
        t.start()
        holder_in.wait()
        with pytest.raises(sg.SecRateLimited):
            gate.acquire(max_wait=2.0)  # 1 s queued + 1.5 s cooldown > 2 s
        t.join()


@pg
@pytest.mark.integration
def test_postgres_row_lock_held_elsewhere_is_busy_not_down():
    """T17b: with the rows locked by a stuck transaction, a take times out on lock_timeout and
    the gate reports busy (SecRateLimited past the budget) without dropping to its own bucket."""
    from sqlalchemy import create_engine, text

    from app.core.models_queue import RateLimitBucket

    holder = create_engine(PG_URL)
    RateLimitBucket.__table__.create(bind=holder, checkfirst=True)
    eng = create_engine(PG_URL, connect_args={"options": "-c lock_timeout=200 -c statement_timeout=2000"})
    cfg = sg.GateConfig()
    sg.PostgresBackend(eng).take(cfg)  # seed both rows
    clock = FakeClock()
    gate = _fake_gate(clock, sg.PostgresBackend(eng))
    with holder.connect() as c:
        tx = c.begin()
        c.execute(text("SELECT 1 FROM rate_limit_bucket WHERE domain IN ('sec.gov', 'sec.gov#breaker') FOR UPDATE"))
        start = time.monotonic()
        with pytest.raises(sg.SecRateLimited):
            gate.acquire(max_wait=1.0)
        assert time.monotonic() - start < 10
        assert gate._fallback is None
        tx.rollback()
    assert gate._take().wait <= 0  # lock released: shared bucket works again
    eng.dispose()
    holder.dispose()
