"""
Tests for SPEC 107 — BulkSource framework.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import hashlib
import importlib.util
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

ZIP_BYTES = b"PK\x03\x04" + b"x" * 200


def _http(handler, sleeps=None):
    from app.core.sec_http import SecHttp

    client = SecHttp(user_agent="Test agent test@example.com", rps=1000.0,
                     transport=httpx.MockTransport(handler))
    if sleeps is not None:
        client._sleep = lambda s: sleeps.append(s)
    return client


# ---------------------------------------------------------------------------
# sec_http
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSecHttp:
    def test_validate_payload_rejects_html_for_zip(self):
        """T1"""
        from app.core.sec_http import PayloadError, validate_payload

        with pytest.raises(PayloadError):
            validate_payload("2025q1_d.zip", b"<!DOCTYPE html><html>" + b" " * 100, "text/html")
        validate_payload("2025q1_d.zip", ZIP_BYTES, "application/zip")

    def test_stream_to_file_atomic_and_hashed(self, tmp_path):
        """T2"""
        seen = {}

        def handler(request):
            seen["ua"] = request.headers.get("user-agent")
            return httpx.Response(200, content=ZIP_BYTES, headers={"ETag": "abc"})

        dest = tmp_path / "sub" / "2025q1_d.zip"
        result = _http(handler).stream_to_file("https://www.sec.gov/files/2025q1_d.zip", dest)
        assert dest.read_bytes() == ZIP_BYTES
        assert not (tmp_path / "sub" / "2025q1_d.zip.part").exists()
        assert result.sha256 == hashlib.sha256(ZIP_BYTES).hexdigest()
        assert result.bytes == len(ZIP_BYTES)
        assert result.etag == "abc"
        assert seen["ua"] == "Test agent test@example.com"

    def test_retry_after_is_capped(self, tmp_path):
        """T3"""
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, headers={"Retry-After": "3600"})
            return httpx.Response(200, text="ok body")

        sleeps = []
        assert _http(handler, sleeps).get_text("https://www.sec.gov/x") == "ok body"
        assert calls["n"] == 2
        assert sleeps and max(sleeps) <= 60

    def test_failed_download_leaves_no_file(self, tmp_path):
        """T4"""
        from app.core.sec_http import PayloadError

        def handler(request):
            return httpx.Response(200, text="<!DOCTYPE html><html>blocked</html>" + " " * 100)

        dest = tmp_path / "2025q1_d.zip"
        with pytest.raises(PayloadError):
            _http(handler).stream_to_file("https://www.sec.gov/files/2025q1_d.zip", dest)
        assert not dest.exists()
        assert not (tmp_path / "2025q1_d.zip.part").exists()


# ---------------------------------------------------------------------------
# copy_loader
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_merge_sql_dedupes_and_counts():
    """T5"""
    from app.core.copy_loader import build_merge_sql

    sql = build_merge_sql("t_stage", "public.t_target", ["id", "name", "val"], ["id"], ["name", "val"])
    s = " ".join(sql.split()).lower()
    assert 'distinct on ("id")' in s
    assert "order by \"id\", _row_num desc" in s
    assert 'on conflict ("id") do update set "name" = excluded."name"' in s
    assert "xmax = 0" in s


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("DROP TABLE IF EXISTS public.t_target"))
        conn.execute(text("CREATE TABLE public.t_target (id INT PRIMARY KEY, name TEXT, val INT)"))
        conn.execute(text("DROP TABLE IF EXISTS raw.source_release"))
        mig = REPO / "alembic" / "versions" / "0004_bulk_framework.py"
        spec = importlib.util.spec_from_file_location("mig0004", mig)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for stmt in mod.SOURCE_RELEASE_DDL:
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


@pg
def test_copy_and_merge_pg(pg_engine):
    """T6"""
    from sqlalchemy import text
    from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging

    cols = [("id", "INT"), ("name", "TEXT"), ("val", "INT")]
    names = [c for c, _ in cols]
    with pg_engine.begin() as conn:
        create_staging(conn, "t_stage", cols)
        n = copy_rows(conn, "t_stage", names, [(1, "a", 1), (2, "b,with comma", None), (1, "a2", 5)])
        assert n == 3
        ins, upd = merge_staging(conn, "t_stage", "public.t_target", names, ["id"])
        assert (ins, upd) == (2, 0)
        assert conn.execute(text("SELECT name, val FROM t_target WHERE id=1")).one() == ("a2", 5)
        assert conn.execute(text("SELECT name, val FROM t_target WHERE id=2")).one() == ("b,with comma", None)

        create_staging(conn, "t_stage", cols)
        copy_rows(conn, "t_stage", names, [(2, "b2", 7)])
        assert merge_staging(conn, "t_stage", "public.t_target", names, ["id"]) == (0, 1)
        drop_staging(conn, "t_stage")
        assert conn.execute(text("SELECT to_regclass('stg.t_stage')")).scalar() is None


# ---------------------------------------------------------------------------
# base.run_source
# ---------------------------------------------------------------------------

def _fake_source_cls(fail_once=None):
    from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
    from app.ingest.bulk.base import BulkSource, Release

    class FakeSource(BulkSource):
        name = "fake_src"
        parser_version = "1"
        fetch_calls = []
        failed = set()

        def discover(self, http, since=None):
            return [Release("r1", "https://www.sec.gov/r1.csv"), Release("r2", "https://www.sec.gov/r2.csv")]

        def fetch(self, http, release, dest_dir):
            FakeSource.fetch_calls.append(release.release_key)
            path = Path(dest_dir) / f"{release.release_key}.csv"
            path.write_text(f"{release.release_key[-1]},name{release.release_key}\n")
            return path

        def load(self, conn, release, path):
            if fail_once and release.release_key == fail_once and fail_once not in FakeSource.failed:
                FakeSource.failed.add(fail_once)
                raise ValueError("parse blew up")
            rid, name = Path(path).read_text().strip().split(",")
            cols = [("id", "INT"), ("name", "TEXT"), ("val", "INT")]
            create_staging(conn, "fake_src_t", cols)
            copy_rows(conn, "fake_src_t", ["id", "name", "val"], [(int(rid), name, 0)])
            ins, upd = merge_staging(conn, "fake_src_t", "public.t_target", ["id", "name", "val"], ["id"])
            drop_staging(conn, "fake_src_t")
            return {"t_target": ins + upd}

        def ddl(self):
            return []

    return FakeSource


@pg
def test_run_source_loads_and_skips_loaded_pg(pg_engine, tmp_path):
    """T7"""
    from sqlalchemy import text
    from app.ingest.bulk.base import run_source

    src = _fake_source_cls()()
    r1 = run_source(src, engine=pg_engine, http=MagicMock(), raw_root=tmp_path)
    assert r1["loaded"] == 2 and r1["failed"] == 0
    with pg_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT release_key, status, rows_loaded->>'t_target' FROM raw.source_release "
            "WHERE source='fake_src' ORDER BY release_key")).fetchall()
    assert rows == [("r1", "loaded", "1"), ("r2", "loaded", "1")]
    type(src).fetch_calls.clear()
    r2 = run_source(src, engine=pg_engine, http=MagicMock(), raw_root=tmp_path)
    assert r2["loaded"] == 0 and r2["skipped"] == 2
    assert type(src).fetch_calls == []


@pg
def test_run_source_resumes_without_redownload_pg(pg_engine, tmp_path):
    """T8"""
    from sqlalchemy import text
    from app.ingest.bulk.base import run_source

    cls = _fake_source_cls(fail_once="r2")
    src = cls()
    r1 = run_source(src, engine=pg_engine, http=MagicMock(), raw_root=tmp_path)
    assert r1["loaded"] == 1 and r1["failed"] == 1
    with pg_engine.connect() as conn:
        status, err = conn.execute(text(
            "SELECT status, error FROM raw.source_release WHERE source='fake_src' AND release_key='r2'")).one()
    assert status == "failed" and "parse blew up" in err
    cls.fetch_calls.clear()
    r2 = run_source(src, engine=pg_engine, http=MagicMock(), raw_root=tmp_path)
    assert r2["loaded"] == 1
    assert cls.fetch_calls == []  # file reused, sha256 verified


# ---------------------------------------------------------------------------
# registry / worker / API / migration
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_registry_register_and_get():
    """T9"""
    from app.ingest.bulk import registry
    from app.ingest.bulk.base import BulkSource

    @registry.register_bulk_source
    class _TmpSource(BulkSource):
        name = "tmp_test_source"

        def discover(self, http, since=None):
            return []

        def load(self, conn, release, path):
            return {}

    try:
        assert isinstance(registry.get_source("tmp_test_source"), _TmpSource)
        assert "tmp_test_source" in registry.list_sources()
        with pytest.raises(KeyError):
            registry.get_source("nope_not_here")
    finally:
        registry.BULK_SOURCES.pop("tmp_test_source", None)


@pytest.mark.unit
class TestExecutor:
    @pytest.mark.asyncio
    async def test_executor_runs_in_thread_and_fails_when_all_failed(self):
        """T10"""
        from app.worker.executors import bulk_ingest

        job = MagicMock(payload={"bulk_source": "form_d", "since": "2024-01-01"})
        db = MagicMock()
        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, kwargs))
            return {"loaded": 0, "failed": 2, "skipped": 0, "rows": 0, "errors": ["x"]}

        with patch.object(bulk_ingest.asyncio, "to_thread", fake_to_thread), \
             patch.object(bulk_ingest, "get_source", return_value=MagicMock()):
            with pytest.raises(RuntimeError):
                await bulk_ingest.execute(job, db)
        assert calls and calls[0][0] is bulk_ingest.run_source
        assert calls[0][1]["since"] == "2024-01-01"


@pytest.mark.unit
def test_bulk_run_endpoint_queues_job():
    """T11"""
    from fastapi import HTTPException
    from app.api.v1 import bulk

    db = MagicMock()
    with patch.object(bulk, "list_sources", return_value=["form_d"]), \
         patch.object(bulk, "submit_job", return_value={"job_queue_id": 7, "mode": "queued"}) as sj:
        out = bulk.run_bulk_source("form_d", since="2024-01-01", max_releases=2, db=db)
        assert out["job_queue_id"] == 7
        kwargs = sj.call_args.kwargs
        assert kwargs["job_type"] == "bulk_ingest"
        assert kwargs["payload"] == {"bulk_source": "form_d", "since": "2024-01-01", "max_releases": 2}
        with pytest.raises(HTTPException) as exc:
            bulk.run_bulk_source("nope", since=None, max_releases=None, db=db)
        assert exc.value.status_code == 404


@pytest.mark.unit
def test_migration_0004_chain():
    """T12"""
    path = REPO / "alembic" / "versions" / "0004_bulk_framework.py"
    spec = importlib.util.spec_from_file_location("mig0004b", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.down_revision == "0003_quarantine_bad_data"
    joined = " ".join(mod.SOURCE_RELEASE_DDL).lower()
    assert "create table if not exists raw.source_release" in joined
    assert "unique (source, release_key)" in joined
