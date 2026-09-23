"""
Tests for SPEC 122 — conditional GET + raw snapshot retention.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import hashlib
import importlib.util
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/snap.zip"
ZIP_V1 = b"PK\x03\x04" + b"a" * 300
ZIP_V2 = b"PK\x03\x04" + b"b" * 400


@pytest.fixture(autouse=True)
def _settings_env(monkeypatch):
    """Settings needs DATABASE_URL; the api image sets it, a host venv may not."""
    from app.core.config import reset_settings

    if not os.environ.get("DATABASE_URL"):
        monkeypatch.setenv("DATABASE_URL", PG_URL or "postgresql://u:p@localhost:5432/unused")
    monkeypatch.delenv("BULK_RAW_RETENTION", raising=False)
    reset_settings()
    yield
    reset_settings()


def _http(handler):
    from app.core.sec_http import SecHttp

    client = SecHttp(user_agent="Test agent test@example.com", rps=1000.0,
                     transport=httpx.MockTransport(handler))
    client._sleep = lambda s: None
    return client


class FakeServer:
    """Serves one URL; honours conditional headers unless ``ignore_conditional``."""

    def __init__(self, body=ZIP_V1, etag='"e1"', last_modified="Wed, 01 Jan 2026 00:00:00 GMT",
                 ignore_conditional=False):
        self.body = body
        self.etag = etag
        self.last_modified = last_modified
        self.ignore_conditional = ignore_conditional
        self.requests = []

    def __call__(self, request):
        self.requests.append(dict(request.headers))
        inm = request.headers.get("if-none-match")
        ims = request.headers.get("if-modified-since")
        headers = {}
        if self.etag:
            headers["ETag"] = self.etag
        if self.last_modified:
            headers["Last-Modified"] = self.last_modified
        if not self.ignore_conditional and (
            (inm and inm == self.etag) or (not inm and ims and ims == self.last_modified)
        ):
            return httpx.Response(304, headers=headers)
        return httpx.Response(200, content=self.body, headers={**headers, "Content-Type": "application/zip"})


# ---------------------------------------------------------------------------
# sec_http
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestConditionalStream:
    def test_304_writes_nothing(self, tmp_path):
        """T1"""
        server = FakeServer()
        dest = tmp_path / "snap.zip"
        res = _http(server).stream_to_file(URL, dest, etag='"e1"',
                                           last_modified="Wed, 01 Jan 2026 00:00:00 GMT")
        assert res.not_modified is True
        assert res.bytes == 0
        assert res.etag == '"e1"'
        assert not dest.exists()
        assert not (tmp_path / "snap.zip.part").exists()
        sent = server.requests[0]
        assert sent["if-none-match"] == '"e1"'
        assert sent["if-modified-since"] == "Wed, 01 Jan 2026 00:00:00 GMT"

    def test_no_validators_no_conditional_headers(self, tmp_path):
        """T2"""
        server = FakeServer()
        dest = tmp_path / "snap.zip"
        http = _http(server)
        res = http.stream_to_file(URL, dest)
        assert res.not_modified is False
        assert dest.read_bytes() == ZIP_V1
        assert res.sha256 == hashlib.sha256(ZIP_V1).hexdigest()
        assert "if-none-match" not in server.requests[0]
        assert "if-modified-since" not in server.requests[0]
        assert http.bytes_downloaded == len(ZIP_V1)

    def test_stale_etag_gets_200(self, tmp_path):
        server = FakeServer(body=ZIP_V2, etag='"e2"')
        res = _http(server).stream_to_file(URL, tmp_path / "snap.zip", etag='"e1"')
        assert res.not_modified is False and res.etag == '"e2"'
        assert (tmp_path / "snap.zip").read_bytes() == ZIP_V2


@pytest.mark.unit
def test_retention_setting_default_and_env(monkeypatch):
    """T12"""
    from app.core.config import Settings

    monkeypatch.delenv("BULK_RAW_RETENTION", raising=False)
    assert Settings(_env_file=None).bulk_raw_retention == 2
    monkeypatch.setenv("BULK_RAW_RETENTION", "5")
    assert Settings(_env_file=None).bulk_raw_retention == 5


@pytest.mark.unit
def test_snapshot_sources_opt_in():
    from app.ingest.bulk.registry import BULK_SOURCES, load_all

    load_all()
    snap = sorted(n for n, cls in BULK_SOURCES.items() if getattr(cls, "snapshot", False))
    assert snap == ["sec_companyfacts", "sec_edgar_submissions"]


# ---------------------------------------------------------------------------
# PG: run_source with a snapshot source
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("DROP TABLE IF EXISTS raw.source_release"))
        mig = REPO / "alembic" / "versions" / "0004_bulk_framework.py"
        spec = importlib.util.spec_from_file_location("mig0004_122", mig)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for stmt in mod.SOURCE_RELEASE_DDL:
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _snap_source(snapshot=True):
    from app.ingest.bulk.base import BulkSource, Release

    class FakeSnap(BulkSource):
        name = "fake_snap"
        parser_version = "1"
        key = "snapshot:2026-01-01"
        loads = []

        def discover(self, http, since=None):
            return [Release(type(self).key, URL, {"snapshot_date": type(self).key.split(":")[1]})]

        def load(self, conn, release, path):
            type(self).loads.append((release.release_key, Path(path).read_bytes()))
            return {"t": 1}

    FakeSnap.snapshot = snapshot
    FakeSnap.loads = []
    return FakeSnap


def _rows(engine, source="fake_snap"):
    from sqlalchemy import text

    with engine.connect() as conn:
        return {
            r["release_key"]: dict(r)
            for r in conn.execute(text(
                "SELECT release_key, status, etag, last_modified, bytes, local_path, error, updated_at, "
                "loaded_at, parser_version "
                "FROM raw.source_release WHERE source = :s"), {"s": source}).mappings()
        }


def _run(src, engine, server, raw_root, **kw):
    from app.ingest.bulk.base import run_source

    http = _http(server)
    try:
        return run_source(src, engine=engine, http=http, raw_root=raw_root, **kw)
    finally:
        http.close()


@pg
def test_snapshot_304_does_not_mint_release_pg(pg_engine, tmp_path):
    """T3"""
    cls = _snap_source()
    src = cls()
    server = FakeServer()
    r1 = _run(src, pg_engine, server, tmp_path)
    assert r1["loaded"] == 1 and r1["unchanged"] == 0
    assert r1["bytes_downloaded"] == len(ZIP_V1)
    before = _rows(pg_engine)["snapshot:2026-01-01"]
    assert before["status"] == "loaded" and before["etag"] == '"e1"'

    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert server.requests[-1]["if-none-match"] == '"e1"'
    assert r2["loaded"] == 0 and r2["failed"] == 0
    assert r2["unchanged"] == 1
    assert r2["unchanged_releases"] == ["snapshot:2026-01-02"]
    assert r2["bytes_saved"] == len(ZIP_V1)
    assert r2["bytes_downloaded"] == 0
    rows = _rows(pg_engine)
    assert "snapshot:2026-01-02" not in rows  # never minted
    assert rows["snapshot:2026-01-01"]["updated_at"] >= before["updated_at"]
    assert len(cls.loads) == 1
    assert not (tmp_path / "fake_snap" / "snapshot_2026-01-02").exists() or not any(
        (tmp_path / "fake_snap" / "snapshot_2026-01-02").iterdir())


@pg
def test_snapshot_server_ignores_conditional_same_etag_pg(pg_engine, tmp_path):
    """T4"""
    cls = _snap_source()
    src = cls()
    server = FakeServer(ignore_conditional=True)
    _run(src, pg_engine, server, tmp_path)
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["unchanged"] == 1 and r2["loaded"] == 0
    assert r2["bytes_downloaded"] == len(ZIP_V1)  # paid for the download ...
    assert len(cls.loads) == 1  # ... but not for the merge
    rows = _rows(pg_engine)
    assert "snapshot:2026-01-02" not in rows
    assert all(r["status"] != "fetched" for r in rows.values())
    day2 = tmp_path / "fake_snap" / "snapshot_2026-01-02"
    assert not day2.exists() or not any(day2.iterdir())
    # the loaded file is untouched
    assert Path(rows["snapshot:2026-01-01"]["local_path"]).read_bytes() == ZIP_V1


@pg
def test_snapshot_same_bytes_no_etag_is_unchanged_pg(pg_engine, tmp_path):
    cls = _snap_source()
    src = cls()
    server = FakeServer(etag=None, last_modified=None)
    _run(src, pg_engine, server, tmp_path)
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["unchanged"] == 1 and len(cls.loads) == 1


@pg
def test_snapshot_changed_content_loads_pg(pg_engine, tmp_path):
    """T5"""
    cls = _snap_source()
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)
    server.body, server.etag = ZIP_V2, '"e2"'
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["loaded"] == 1 and r2["unchanged"] == 0
    assert cls.loads[-1] == ("snapshot:2026-01-02", ZIP_V2)
    row = _rows(pg_engine)["snapshot:2026-01-02"]
    assert row["status"] == "loaded" and row["etag"] == '"e2"' and row["bytes"] == len(ZIP_V2)


@pg
def test_non_snapshot_source_never_conditional_pg(pg_engine, tmp_path):
    """T6"""
    cls = _snap_source(snapshot=False)
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["loaded"] == 1
    assert all("if-none-match" not in h for h in server.requests)
    assert len(cls.loads) == 2


@pg
def test_stale_fetched_snapshot_superseded_and_pruned_pg(pg_engine, tmp_path):
    """T9"""
    from sqlalchemy import text
    from app.ingest.bulk.retention import SUPERSEDED_PREFIX

    cls = _snap_source()
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)  # 01-01 loaded
    # a snapshot downloaded but never merged (worker died mid-load)
    stuck_dir = tmp_path / "fake_snap" / "snapshot_2026-01-02"
    stuck_dir.mkdir(parents=True)
    stuck = stuck_dir / "snap.zip"
    stuck.write_bytes(ZIP_V2)
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw.source_release (source, release_key, url, status, local_path, bytes, "
            "fetched_at) VALUES ('fake_snap', 'snapshot:2026-01-02', :u, 'fetched', :p, :b, :f)"),
            {"u": URL, "p": str(stuck), "b": len(ZIP_V2), "f": datetime.utcnow() - timedelta(hours=1)})

    cls.key = "snapshot:2026-01-03"
    r = _run(src, pg_engine, server, tmp_path)
    assert r["unchanged"] == 1
    row = _rows(pg_engine)["snapshot:2026-01-02"]
    assert row["status"] == "failed" and row["error"].startswith(SUPERSEDED_PREFIX)
    assert not stuck.exists()  # auto-retention freed the superseded file
    assert r["retention"]["bytes_freed"] == len(ZIP_V2)


# ---------------------------------------------------------------------------
# PG: retention
# ---------------------------------------------------------------------------

def _seed(engine, root, rows):
    """rows: (key, status, loaded_at offset days or None, error, size)."""
    from sqlalchemy import text

    base = datetime(2026, 1, 1)
    paths = {}
    with engine.begin() as conn:
        for key, status, day, error, size in rows:
            d = root / "fake_snap" / key.replace(":", "_")
            d.mkdir(parents=True, exist_ok=True)
            p = d / "snap.zip"
            p.write_bytes(b"PK" + b"z" * (size - 2))
            paths[key] = p
            conn.execute(text(
                "INSERT INTO raw.source_release (source, release_key, url, status, local_path, bytes, "
                "loaded_at, error) VALUES ('fake_snap', :k, :u, :st, :p, :b, :la, :e)"),
                {"k": key, "u": URL, "st": status, "p": str(p), "b": size,
                 "la": base + timedelta(days=day) if day is not None else None, "e": error})
    return paths


@pg
def test_retention_keeps_newest_and_protects_unloaded_pg(pg_engine, tmp_path):
    """T7"""
    from app.ingest.bulk.retention import SUPERSEDED_PREFIX, apply_retention

    paths = _seed(pg_engine, tmp_path, [
        ("snapshot:2026-01-01", "loaded", 0, None, 100),
        ("snapshot:2026-01-02", "loaded", 1, None, 200),
        ("snapshot:2026-01-03", "loaded", 2, None, 300),
        ("snapshot:2026-01-04", "fetched", None, None, 400),
        ("snapshot:2026-01-05", "failed", None, "HTTPError: boom", 500),
        ("snapshot:2025-12-31", "failed", None,
         f"{SUPERSEDED_PREFIX} newer snapshot snapshot:2026-01-01 discovered; was fetched", 600),
    ])
    orphan = tmp_path / "fake_snap" / "stray" / "snap.zip.part"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"x" * 50)

    out = apply_retention(pg_engine, "fake_snap", tmp_path, keep=2)
    assert not out["dry_run"]
    deleted = {d["release_key"] for d in out["deleted"]}
    assert deleted == {"snapshot:2026-01-01", "snapshot:2025-12-31"}
    assert out["bytes_freed"] == 700
    for k in ("snapshot:2026-01-02", "snapshot:2026-01-03", "snapshot:2026-01-04", "snapshot:2026-01-05"):
        assert paths[k].exists(), k
    assert not paths["snapshot:2026-01-01"].exists()
    assert not paths["snapshot:2026-01-01"].parent.exists()  # emptied release dir removed
    assert orphan.exists()
    assert [o["path"] for o in out["orphans"]] == [str(orphan)]


@pg
def test_retention_dry_run_and_clamp_pg(pg_engine, tmp_path):
    """T8"""
    from app.ingest.bulk.retention import apply_retention

    paths = _seed(pg_engine, tmp_path, [
        ("snapshot:2026-01-01", "loaded", 0, None, 100),
        ("snapshot:2026-01-02", "loaded", 1, None, 200),
    ])
    out = apply_retention(pg_engine, "fake_snap", tmp_path, keep=0, dry_run=True)
    assert out["dry_run"] and out["keep"] == 1
    assert [d["release_key"] for d in out["deleted"]] == ["snapshot:2026-01-01"]
    assert out["bytes_to_free"] == 100 and out["bytes_freed"] == 0
    assert all(p.exists() for p in paths.values())

    out2 = apply_retention(pg_engine, "fake_snap", tmp_path, keep=0)
    assert out2["bytes_freed"] == 100
    assert paths["snapshot:2026-01-02"].exists()  # newest loaded always kept


@pg
def test_retention_never_deletes_outside_source_dir_pg(pg_engine, tmp_path):
    from sqlalchemy import text
    from app.ingest.bulk.retention import apply_retention

    outside = tmp_path / "elsewhere.zip"
    outside.write_bytes(b"PK" + b"q" * 98)
    _seed(pg_engine, tmp_path / "root", [("snapshot:2026-01-02", "loaded", 1, None, 200)])
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw.source_release (source, release_key, url, status, local_path, bytes, loaded_at) "
            "VALUES ('fake_snap', 'snapshot:2026-01-01', :u, 'loaded', :p, 100, '2025-12-01')"),
            {"u": URL, "p": str(outside)})
    out = apply_retention(pg_engine, "fake_snap", tmp_path / "root", keep=1)
    assert outside.exists()
    assert out["deleted"] == []


@pg
def test_cleanup_supersedes_and_dry_run_pg(pg_engine, tmp_path):
    from app.ingest.bulk.retention import SUPERSEDED_PREFIX, cleanup

    paths = _seed(pg_engine, tmp_path, [
        ("snapshot:2026-01-01", "loaded", 0, None, 100),
        ("snapshot:2026-01-02", "fetched", None, None, 200),
        ("snapshot:2026-01-03", "loaded", 2, None, 300),
    ])
    src = _snap_source()()
    with patch("app.ingest.bulk.retention.get_source", return_value=src):
        dry = cleanup(pg_engine, ["fake_snap"], raw_root=tmp_path, keep=2, dry_run=True,
                      current_key="snapshot:2026-01-04")
        res = dry["sources"]["fake_snap"]
        assert res["superseded"] == ["snapshot:2026-01-02"]
        assert [d["release_key"] for d in res["deleted"]] == ["snapshot:2026-01-02"]
        assert dry["bytes_to_free"] == 200
        assert _rows(pg_engine)["snapshot:2026-01-02"]["status"] == "fetched"
        assert paths["snapshot:2026-01-02"].exists()

        real = cleanup(pg_engine, ["fake_snap"], raw_root=tmp_path, keep=2, dry_run=False,
                       current_key="snapshot:2026-01-04")
    assert real["bytes_freed"] == 200
    row = _rows(pg_engine)["snapshot:2026-01-02"]
    assert row["status"] == "failed" and row["error"].startswith(SUPERSEDED_PREFIX)
    assert not paths["snapshot:2026-01-02"].exists()
    assert paths["snapshot:2026-01-01"].exists()


# ---------------------------------------------------------------------------
# executor + endpoint
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_reports_unchanged_and_bytes():
    """T10"""
    from app.worker.executors import bulk_ingest

    job = MagicMock(payload={"bulk_source": "sec_edgar_submissions"})
    db = MagicMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return {"loaded": 0, "failed": 0, "skipped": 0, "rows": 0, "errors": [], "unchanged": 1,
                "unchanged_releases": ["snapshot:2026-01-02"], "bytes_downloaded": 2_000,
                "bytes_saved": 1_564_000_000, "releases": []}

    with patch.object(bulk_ingest.asyncio, "to_thread", fake_to_thread), \
         patch.object(bulk_ingest, "get_source", return_value=MagicMock()):
        await bulk_ingest.execute(job, db)
    msg = job.progress_message
    assert "unchanged" in msg
    assert "1564" in msg.replace(",", "")  # MB saved
    assert "warning" not in msg


@pytest.mark.unit
def test_cleanup_endpoint_defaults_to_dry_run():
    """T11"""
    from app.api.v1 import bulk

    with patch.object(bulk, "run_raw_cleanup", return_value={"dry_run": True, "sources": {}}) as rc, \
         patch.object(bulk, "list_sources", return_value=["sec_companyfacts"]):
        out = bulk.cleanup_raw_files(source=None, keep=None, dry_run=True)
        assert out["dry_run"] is True
        kwargs = rc.call_args.kwargs
        assert kwargs["dry_run"] is True and kwargs["sources"] is None and kwargs["keep"] is None
        bulk.cleanup_raw_files(source=["sec_companyfacts"], keep=3, dry_run=False)
        kwargs = rc.call_args.kwargs
        assert kwargs["sources"] == ["sec_companyfacts"] and kwargs["keep"] == 3 and kwargs["dry_run"] is False
        from fastapi import HTTPException

        with pytest.raises(HTTPException):
            bulk.cleanup_raw_files(source=["nope"], keep=None, dry_run=True)


# ---------------------------------------------------------------------------
# Review fixes (spec-122-fix)
# ---------------------------------------------------------------------------

@pg
def test_unchanged_bumps_prior_loaded_at_pg(pg_engine, tmp_path):
    """F2: 'confirmed current' lands in loaded_at, the freshness signal consumers read."""
    from sqlalchemy import text

    cls = _snap_source()
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)
    with pg_engine.begin() as conn:  # pretend the load happened 10 days ago
        conn.execute(text("UPDATE raw.source_release SET loaded_at = loaded_at - INTERVAL '10 days'"))
    old = _rows(pg_engine)["snapshot:2026-01-01"]["loaded_at"]
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["unchanged"] == 1
    new = _rows(pg_engine)["snapshot:2026-01-01"]["loaded_at"]
    assert new - old > timedelta(days=9)


@pg
def test_parser_version_bump_reloads_unchanged_snapshot_pg(pg_engine, tmp_path):
    """F3: a prior loaded by an older parser is not a valid 'unchanged' baseline."""
    cls = _snap_source()
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)
    cls.parser_version = "2"
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["unchanged"] == 0 and r2["loaded"] == 1
    assert "if-none-match" not in server.requests[-1]
    assert len(cls.loads) == 2
    assert _rows(pg_engine)["snapshot:2026-01-02"]["parser_version"] == "2"
    # same parser again: the conditional path is back on
    cls.key = "snapshot:2026-01-03"
    r3 = _run(src, pg_engine, server, tmp_path)
    assert r3["unchanged"] == 1


@pg
def test_force_skips_conditional_and_equality_pg(pg_engine, tmp_path):
    """F3: force=True re-downloads and re-merges an unchanged snapshot."""
    cls = _snap_source()
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)
    cls.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path, force=True)
    assert r2["loaded"] == 1 and r2["unchanged"] == 0
    assert "if-none-match" not in server.requests[-1]
    assert len(cls.loads) == 2


@pg
def test_failed_snapshot_file_survives_until_newer_load_pg(pg_engine, tmp_path):
    """F4: D fails (e.g. publish guard), D+1 fails too -> D's raw file is kept."""
    from app.ingest.bulk.retention import SUPERSEDED_PREFIX, apply_retention

    cls = _snap_source()

    class Failing(cls):
        fail = False

        def load(self, conn, release, path):
            if type(self).fail:
                raise RuntimeError("publish guard: 90% drop")
            return super().load(conn, release, path)

    src = Failing()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)  # day 1 loads V1
    Failing.fail = True
    server.body, server.etag = ZIP_V2, '"e2"'
    Failing.key = "snapshot:2026-01-02"
    r2 = _run(src, pg_engine, server, tmp_path)
    assert r2["failed"] == 1
    d2 = Path(_rows(pg_engine)["snapshot:2026-01-02"]["local_path"])
    assert d2.exists()

    server.body, server.etag = ZIP_V2 + b"c", '"e3"'
    Failing.key = "snapshot:2026-01-03"
    r3 = _run(src, pg_engine, server, tmp_path)
    assert r3["failed"] == 1
    assert _rows(pg_engine)["snapshot:2026-01-02"]["error"].startswith(SUPERSEDED_PREFIX)
    assert d2.exists()  # no newer release loaded: keep it
    assert "snapshot:2026-01-02" in {p["release_key"] for p in r3["retention"]["protected"]}

    # day 4 loads fine; day 2 failed for a real error -> kept until an operator purges
    Failing.fail = False
    server.body, server.etag = ZIP_V2 + b"d", '"e4"'
    Failing.key = "snapshot:2026-01-04"
    r4 = _run(src, pg_engine, server, tmp_path)
    assert r4["loaded"] == 1
    assert d2.exists()

    out = apply_retention(pg_engine, "fake_snap", tmp_path, keep=2, purge_failed=True)
    assert "snapshot:2026-01-02" in {d["release_key"] for d in out["deleted"]}
    assert not d2.exists()


@pg
def test_superseded_stuck_file_needs_newer_load_pg(pg_engine, tmp_path):
    """F4: a superseded 'fetched' row newer than every load keeps its file."""
    from app.ingest.bulk.retention import SUPERSEDED_PREFIX, apply_retention

    note = f"{SUPERSEDED_PREFIX} newer snapshot snapshot:2026-01-03 discovered; was fetched"
    paths = _seed(pg_engine, tmp_path, [
        ("snapshot:2026-01-01", "loaded", 0, None, 100),
        ("snapshot:2026-01-02", "failed", None, note, 200),
    ])
    out = apply_retention(pg_engine, "fake_snap", tmp_path, keep=2)
    assert out["deleted"] == []
    assert paths["snapshot:2026-01-02"].exists()


@pg
def test_run_source_skips_when_source_locked_pg(pg_engine, tmp_path):
    """F5: a second concurrent run of the same source does nothing."""
    from sqlalchemy import text

    from app.ingest.bulk.base import source_lock
    from app.ingest.bulk.retention import cleanup

    cls = _snap_source()
    src = cls()
    server = FakeServer()
    _run(src, pg_engine, server, tmp_path)
    paths = _seed(pg_engine, tmp_path, [("snapshot:2025-12-01", "fetched", None, None, 100)])
    cls.key = "snapshot:2026-01-02"
    with source_lock(pg_engine, "fake_snap") as held:
        assert held
        r = _run(src, pg_engine, server, tmp_path)
        assert r["locked"] is True and r["loaded"] == 0 and r["unchanged"] == 0
        assert len(server.requests) == 1  # no fetch
        assert _rows(pg_engine)["snapshot:2025-12-01"]["status"] == "fetched"  # not superseded
        with patch("app.ingest.bulk.retention.get_source", return_value=src):
            out = cleanup(pg_engine, ["fake_snap"], raw_root=tmp_path, dry_run=False,
                          current_key="snapshot:2026-01-02")
        assert "skipped" in out["sources"]["fake_snap"]
        assert paths["snapshot:2025-12-01"].exists()
    # lock released: a later run proceeds
    r = _run(src, pg_engine, server, tmp_path)
    assert r["locked"] is False and r["unchanged"] == 1
    with pg_engine.connect() as conn:
        held = conn.execute(text(
            "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = 122")).scalar()
    assert held == 0


@pg
def test_watchdog_ignores_superseded_releases_pg(pg_engine):
    """F1: superseding is housekeeping, not a failed_release alert."""
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    from app.ingest.bulk.retention import SUPERSEDED_PREFIX
    from app.services.data_watchdog import rule_failed_releases

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw.source_release (source, release_key, url, status, error) VALUES "
            "('fake_snap', 'snapshot:2026-01-01', :u, 'failed', :sup), "
            "('fake_other', 'x', :u, 'failed', 'HTTPError: boom')"),
            {"u": URL, "sup": f"{SUPERSEDED_PREFIX} newer snapshot discovered; was fetched"})
    with Session(pg_engine) as db:
        findings = rule_failed_releases(db, datetime.utcnow())
    assert [f.details["source"] for f in findings] == ["fake_other"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_passes_force_and_reports_locked():
    from app.worker.executors import bulk_ingest

    job = MagicMock(payload={"bulk_source": "sec_companyfacts", "force": True})
    seen = {}

    async def fake_to_thread(fn, *args, **kwargs):
        seen.update(kwargs)
        return {"loaded": 0, "failed": 0, "skipped": 0, "rows": 0, "errors": ["busy"],
                "unchanged": 0, "locked": True, "releases": []}

    with patch.object(bulk_ingest.asyncio, "to_thread", fake_to_thread), \
         patch.object(bulk_ingest, "get_source", return_value=MagicMock()):
        await bulk_ingest.execute(job, MagicMock())
    assert seen["force"] is True
    assert "another sec_companyfacts run" in job.progress_message


@pytest.mark.unit
def test_run_endpoint_force_flag():
    from app.api.v1 import bulk

    with patch.object(bulk, "submit_job", return_value={"job_id": 1}) as sj, \
         patch.object(bulk, "list_sources", return_value=["sec_companyfacts"]):
        bulk.run_bulk_source("sec_companyfacts", since=None, max_releases=None,
                             publish_guard_override=None, force=True, db=MagicMock())
        assert sj.call_args.kwargs["payload"]["force"] is True
        bulk.run_bulk_source("sec_companyfacts", since=None, max_releases=None,
                             publish_guard_override=None, force=False, db=MagicMock())
        assert "force" not in sj.call_args.kwargs["payload"]
        with patch.object(bulk, "run_raw_cleanup", return_value={}) as rc:
            bulk.cleanup_raw_files(source=None, keep=None, dry_run=True, purge_failed=True)
            assert rc.call_args.kwargs["purge_failed"] is True
