"""
SPEC_140 — CMS client rate limit, split states, streaming, idempotent reruns.

Unit tests drive CMSClient through an httpx.MockTransport. PG-backed tests
need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import asyncio
import json
import os
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

STATE_COL = "Rndrng_Prvdr_State_Abrvtn"


def _dkan_handler(rows_by_state, page_log=None, fail_state=None):
    """Serve DKAN pages: filter[State]=XX, size, offset."""

    def handler(request: httpx.Request) -> httpx.Response:
        q = parse_qs(urlparse(str(request.url)).query)
        state = (q.get(f"filter[{STATE_COL}]") or [None])[0]
        size = int(q["size"][0])
        offset = int(q["offset"][0])
        if page_log is not None:
            page_log.append((state, offset, dict(request.headers)))
        if fail_state and state == fail_state and offset > 0:
            return httpx.Response(500, json={"error": "boom"})
        rows = rows_by_state.get(state, [])
        return httpx.Response(200, json=rows[offset: offset + size])

    return handler


def _rows(state, n):
    return [
        {"Rndrng_Npi": f"{state}{i:05d}", STATE_COL: state, "HCPCS_Cd": "99213",
         "Tot_Srvcs": str(i)}
        for i in range(n)
    ]


def _client(handler, **kw):
    from app.sources.cms.client import CMSClient

    kw.setdefault("requests_per_second", 0)  # no pacing unless a test asks
    return CMSClient(transport=httpx.MockTransport(handler), **kw)


# ---------------------------------------------------------------------------
# Unit — client
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_t1_one_pooled_client_reused_and_closed():
    created = []
    orig = httpx.AsyncClient.__init__

    def spy(self, *a, **k):
        created.append(1)
        orig(self, *a, **k)

    async def run():
        c = _client(_dkan_handler({"NY": _rows("NY", 25)}))
        try:
            httpx.AsyncClient.__init__ = spy
            rows = await c.fetch_dkan_data("ds", size=10, filters={STATE_COL: "NY"})
        finally:
            httpx.AsyncClient.__init__ = orig
        assert len(rows) == 25
        assert created == [] or len(created) == 1  # created lazily at most once
        http = c._http
        await c.close()
        assert http is None or http.is_closed

    asyncio.run(run())


@pytest.mark.unit
def test_t2_every_page_goes_through_throttle():
    async def run():
        c = _client(_dkan_handler({"NY": _rows("NY", 35)}))
        calls = []
        orig = c._throttle

        async def counting(url):
            calls.append(url)
            return await orig(url)

        c._throttle = counting
        await c.fetch_dkan_data("ds", size=10, filters={STATE_COL: "NY"})
        await c.close()
        assert len(calls) == 4  # 10+10+10+5

    asyncio.run(run())


@pytest.mark.unit
def test_t3_local_pacing_spaces_requests():
    async def run():
        c = _client(_dkan_handler({"NY": _rows("NY", 30)}), requests_per_second=2.0)
        clock = {"t": 100.0}
        slept = []

        async def fake_sleep(s):
            slept.append(s)
            clock["t"] += s

        c._now = lambda: clock["t"]
        c._sleep = fake_sleep
        await c.fetch_dkan_data("ds", size=10, filters={STATE_COL: "NY"})
        await c.close()
        # 4 requests (3 full pages + empty tail) at 2 rps: >= 1.5s of waiting
        assert sum(slept) >= 1.5 - 1e-6

    asyncio.run(run())


@pytest.mark.unit
def test_t4_distributed_bucket_consulted_per_request():
    async def run():
        acquired = []

        async def acquire(domain):
            acquired.append(domain)
            return True

        c = _client(_dkan_handler({"NY": _rows("NY", 15)}), distributed_acquire=acquire)
        await c.fetch_dkan_data("ds", size=10, filters={STATE_COL: "NY"})
        await c.close()
        assert acquired == ["data.cms.gov", "data.cms.gov"]

    asyncio.run(run())


@pytest.mark.unit
def test_t4b_worker_mode_uses_shared_bucket_by_default(monkeypatch):
    from app.sources.cms import client as client_mod

    seen = []

    async def fake_shared(domain):
        seen.append(domain)
        return True

    monkeypatch.setenv("WORKER_MODE", "1")
    monkeypatch.setattr(client_mod, "_shared_bucket_acquire", fake_shared)

    async def run():
        c = _client(_dkan_handler({"NY": _rows("NY", 5)}))
        await c.fetch_dkan_data("ds", size=10, filters={STATE_COL: "NY"})
        await c.close()

    asyncio.run(run())
    assert seen == ["data.cms.gov"]


@pytest.mark.unit
def test_t5_user_agent_sent():
    log = []

    async def run():
        c = _client(_dkan_handler({"NY": _rows("NY", 3)}, page_log=log))
        await c.fetch_dkan_data("ds", size=10, filters={STATE_COL: "NY"})
        await c.close()

    asyncio.run(run())
    ua = log[0][2].get("user-agent", "")
    assert "NexdataResearch" in ua


@pytest.mark.unit
def test_t6_429_honours_retry_after():
    hits = {"n": 0}

    def handler(request):
        hits["n"] += 1
        if hits["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json=[])

    async def run():
        c = _client(handler)
        slept = []

        async def fake_sleep(s):
            slept.append(s)

        c._sleep = fake_sleep
        await c.fetch_dkan_data("ds", size=10)
        await c.close()
        assert 7.0 in slept

    asyncio.run(run())


@pytest.mark.unit
def test_t7_dispatch_forwards_states():
    from app.api.v1.jobs import SOURCE_DISPATCH

    assert "states" in SOURCE_DISPATCH["cms"][2]


# ---------------------------------------------------------------------------
# PG — ingest
# ---------------------------------------------------------------------------


@pytest.fixture
def pg_db():
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    from app.core.models import DatasetRegistry, IngestionJob

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
    for model in (IngestionJob, DatasetRegistry):
        model.__table__.create(engine, checkfirst=True)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
    engine.dispose()


def _counts(db):
    from sqlalchemy import text

    return dict(db.execute(text(
        "SELECT rndrng_prvdr_state_abrvtn, count(*) FROM cms_medicare_utilization GROUP BY 1"
    )).fetchall())


@pg
def test_t8_ingest_per_state_and_rerun_replaces(pg_db):
    from app.sources.cms.ingest import ingest_medicare_utilization

    data = {"NY": _rows("NY", 23), "WY": _rows("WY", 4)}
    log = []

    async def run():
        c = _client(_dkan_handler(data, page_log=log))
        try:
            return await ingest_medicare_utilization(
                db=pg_db, job_id=0, states=["NY", "WY"], client=c, page_size=10)
        finally:
            await c.close()

    r1 = asyncio.run(run())
    assert r1["rows_inserted"] == 27
    assert {s for s, _, _ in log} == {"NY", "WY"}  # every request is state-filtered
    assert _counts(pg_db) == {"NY": 23, "WY": 4}

    asyncio.run(run())
    assert _counts(pg_db) == {"NY": 23, "WY": 4}  # replaced, not duplicated


@pg
def test_t8b_failed_state_keeps_previous_rows(pg_db):
    from app.sources.cms.ingest import ingest_medicare_utilization

    good = {"NY": _rows("NY", 23)}

    async def run(handler):
        c = _client(handler)
        try:
            return await ingest_medicare_utilization(
                db=pg_db, job_id=0, states=["NY"], client=c, page_size=10)
        finally:
            await c.close()

    asyncio.run(run(_dkan_handler(good)))
    assert _counts(pg_db) == {"NY": 23}
    with pytest.raises(Exception):
        asyncio.run(run(_dkan_handler({"NY": _rows("NY", 40)}, fail_state="NY")))
    assert _counts(pg_db) == {"NY": 23}


@pg
def test_t9_rows_stream_before_last_page(pg_db):
    """Inserts happen page by page: by the time the last page is requested,
    earlier pages are already in the (uncommitted) state transaction."""
    from sqlalchemy import text

    from app.sources.cms.ingest import ingest_medicare_utilization

    rows = _rows("NY", 30)
    seen_before_last = {}

    def handler(request):
        q = parse_qs(urlparse(str(request.url)).query)
        offset = int(q["offset"][0])
        if offset == 30:  # the empty tail page
            seen_before_last["n"] = pg_db.execute(
                text("SELECT count(*) FROM cms_medicare_utilization")).scalar()
        return httpx.Response(200, json=rows[offset: offset + 10])

    async def run():
        c = _client(handler)
        try:
            await ingest_medicare_utilization(
                db=pg_db, job_id=0, states=["NY"], client=c, page_size=10)
        finally:
            await c.close()

    asyncio.run(run())
    assert seen_before_last["n"] == 30


@pg
def test_t10_insert_is_batched_not_row_by_row(pg_db):
    """1000 rows must not be 1000 round trips: over the Cloud SQL proxy that
    was ~50 s per page (measured live 2026-09-25)."""
    from sqlalchemy import event

    from app.sources.cms.ingest import ingest_medicare_utilization

    data = {"NY": _rows("NY", 1000)}
    inserts = []

    def count(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("INSERT INTO CMS_MEDICARE_UTILIZATION"):
            inserts.append(executemany)

    engine = pg_db.get_bind()
    event.listen(engine, "before_cursor_execute", count)
    try:
        async def run():
            c = _client(_dkan_handler(data))
            try:
                await ingest_medicare_utilization(
                    db=pg_db, job_id=0, states=["NY"], client=c, page_size=1000)
            finally:
                await c.close()

        asyncio.run(run())
    finally:
        event.remove(engine, "before_cursor_execute", count)
    assert _counts(pg_db) == {"NY": 1000}
    assert len(inserts) <= 10 and not any(inserts)  # multi-row VALUES, no executemany
