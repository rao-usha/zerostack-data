"""
Tests for SPEC 125 — the dataset status page (frontend/status.html).

Static checks on the page (auth.js, read-only, API paths that exist, live
updates through a stream token, narrow-width columns) and on the repaired
index.html panel. The page's pure helpers (`StatusCore`, in the
`<script id="status-core">` block) run under node when it is installed; the
DOM smoke scenarios (tests/js/status_page_smoke.js) run under node + jsdom
when `jsdom` resolves (e.g. via NODE_PATH). Real-browser tests are not run:
Playwright is not available here.

Review fixes (T9-T12): recent runs come from GET /jobs?producer= (resolved
server side, split jobs included, bare dispatch keys kept apart from their
qualified siblings); the page keeps refreshing while SSE is quiet and polls
in WORKER_MODE=0; /pe/marts/builds shows error text to admins only.
"""
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
STATUS = REPO / "frontend" / "status.html"
INDEX = REPO / "frontend" / "index.html"
JWT_SECRET = "spec125-test-secret-" + "x" * 48
_DUMMY_DB = "postgresql://nobody:nobody@localhost:1/nothing"
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


@pytest.fixture(scope="module")
def status_html() -> str:
    return STATUS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def index_html() -> str:
    return INDEX.read_text(encoding="utf-8")


def _scripts(html: str):
    """(attrs, body) for every <script> element, in document order."""
    return re.findall(r"<script\b([^>]*)>(.*?)</script>", html, flags=re.S | re.I)


def _api_paths(html: str):
    """Every '/api/v1/...' literal in the page, query string and template
    parts cut off: '/api/v1/jobs?source=' -> '/api/v1/jobs'."""
    paths = set()
    for m in re.finditer(r"['\"`](/api/v1/[A-Za-z0-9_\-/{}]*)", html):
        p = m.group(1).split("${")[0].rstrip("/")
        paths.add(p)
    return paths


# =============================================================================
# T1-T5 — status.html
# =============================================================================


@pytest.mark.unit
class TestStatusPage:
    def test_t1_loads_auth_js_first(self, status_html):
        scripts = _scripts(status_html)
        assert scripts, "no <script> tags"
        srcs = [re.search(r'src="([^"]+)"', a) for a, _ in scripts]
        assert srcs[0] and srcs[0].group(1) == "/js/auth.js", \
            "auth.js must be the first script so every fetch carries the JWT"
        assert "NexdataAuth" in status_html

    def test_t2_read_only(self, status_html):
        buttons = re.findall(r"<button\b[^>]*>(.*?)</button>", status_html, flags=re.S | re.I)
        # buttons built in JS template strings count too
        buttons += re.findall(r"<button\b[^>]*>([^<`]*)", status_html, flags=re.I)
        for label in buttons:
            text = re.sub(r"<[^>]+>", "", label).strip().lower()
            assert not re.search(r"\b(run|pause|resume|trigger|enqueue)\b", text), label
        assert not re.search(r"method\s*:\s*['\"](post|put|patch|delete)['\"]",
                             status_html, flags=re.I)
        assert not re.search(r"/datasets/[^'\"`\s]*/run", status_html)
        assert "/run'" not in status_html and '/run"' not in status_html

    def test_t3_api_paths_exist(self, status_html, monkeypatch):
        from fastapi.routing import APIRoute

        from app.core.config import reset_settings

        monkeypatch.setenv("DATABASE_URL", _DUMMY_DB)
        monkeypatch.setenv("JWT_SECRET_KEY", JWT_SECRET)
        reset_settings()
        try:
            from app.main import app

            routes = [r for r in app.routes if isinstance(r, APIRoute) and "GET" in r.methods]
        finally:
            reset_settings()

        def matches(path: str) -> bool:
            for r in routes:
                pattern = "^" + re.sub(r"\{[^}]+\}", "[^/]+", r.path) + "$"
                if re.match(pattern, path) or r.path == path:
                    return True
            return False

        paths = _api_paths(status_html)
        expected = {
            "/api/v1/datasets/status", "/api/v1/catalog", "/api/v1/pe/marts/builds",
            "/api/v1/bulk/releases", "/api/v1/jobs", "/api/v1/job-queue/stream",
        }
        assert expected <= paths, expected - paths
        missing = sorted(p for p in paths if not matches(p))
        assert not missing, missing

    def test_t4_live_updates_use_stream_token(self, status_html):
        assert "NexdataAuth.streamUrl('/api/v1/job-queue/stream')" in status_html \
            or "NexdataAuth.streamUrl(ENDPOINTS.stream)" in status_html
        assert not re.search(r"new EventSource\(\s*['\"`]/api", status_html)
        # every timer the page runs on its own is slow (>= 60 s): the fallback
        # poll, the SSE retry, and the always-on tick / safety refresh
        for name in ("FALLBACK_POLL_MS", "SSE_RETRY_MS", "TICK_MS", "SAFETY_REFRESH_MS"):
            m = re.search(name + r"\s*=\s*(\d[\d_]*)", status_html)
            assert m, f"{name} must be a named constant"
            assert int(m.group(1).replace("_", "")) >= 60000, name
        intervals = re.findall(r"setInterval\(([^)]*)", status_html)
        assert intervals
        for args in intervals:
            assert re.search(r"\b(FALLBACK_POLL_MS|SSE_RETRY_MS|TICK_MS)\b", args), args

    def test_t5_narrow_widths_collapse_columns(self, status_html):
        queries = re.findall(r"@media\s*\(max-width:\s*(\d+)px\)\s*\{(.*?)\n\s*\}",
                             status_html, flags=re.S)
        hiding = [w for w, body in queries if "display: none" in body or "display:none" in body]
        assert len(hiding) >= 2, "expect at least two breakpoints that hide columns"
        assert not re.search(r"\.st-table[^{]*\{[^}]*min-width\s*:\s*[5-9]\d\dpx", status_html)
        assert 'name="viewport"' in status_html

    def test_escapes_api_text(self, status_html):
        assert "function esc(" in status_html
        assert "status_reason" in status_html
        # reasons and blocker messages are rendered through esc()
        assert re.search(r"esc\([^)]*status_reason", status_html)
        assert re.search(r"esc\([^)]*\.message", status_html)


# =============================================================================
# T6-T7 — index.html
# =============================================================================


@pytest.mark.unit
class TestIndexPanel:
    def test_t6_dead_fields_gone(self, index_html):
        for dead in ("avg_coverage_score", "coverage_depth", "total_records", "freshnessData"):
            assert dead not in index_html, dead

    def test_t6_links_status_page(self, index_html):
        assert 'href="/status.html"' in index_html
        nav = re.search(r'<nav class="tabs">(.*?)</nav>', index_html, flags=re.S).group(1)
        # tab highlighting is positional: the link must come after every tab button
        last_button = nav.rfind("<button")
        link = nav.find('href="/status.html"')
        assert link > last_button
        assert "<button" not in nav[link:]

    def test_t7_hero_card_reads_dataset_status(self, index_html):
        assert "${API}/datasets/status" in index_html
        assert "datasetStatusSummary" in index_html


# =============================================================================
# T8 — StatusCore under node
# =============================================================================

NODE = shutil.which("node")

_NODE_HARNESS = r"""
globalThis.window = globalThis;
%s
const C = window.StatusCore;
const out = {};
out.esc = C.esc('<a href="x">&\'</a>');
out.escNull = C.esc(null);
out.attention = C.attentionCount({dormant: 3, never_run: 4, stalled: 2, current: 9});
out.attentionMissing = C.attentionCount({current: 1});
out.lagMonthly = C.lagUnits({cadence: 'monthly', clocks: {expected_through: '2026-08-31', lag_days: 61}});
out.lagNone = C.lagUnits({cadence: 'monthly', clocks: {expected_through: null, lag_days: null}});
out.lagZero = C.lagUnits({cadence: 'daily', clocks: {expected_through: '2026-09-22', lag_days: 0}});
out.lagCap = C.lagUnits({cadence: 'daily', clocks: {expected_through: '2026-09-22', lag_days: 400}});
out.prefix = ['bulk_ingest', 'ingestion', 'site_intel', 'pe_mart_build', 'entity_resolve']
  .map(C.producerPrefixForJobType);
out.match = [C.matchesPrefix('job:pe_mart_build#firms', 'job:pe_mart_build'),
             C.matchesPrefix('bulk:sec_adv', 'bulk:'),
             C.matchesPrefix('dispatch:fred', 'bulk:'),
             C.matchesPrefix('job:pe_mart_buildx', 'job:pe_mart_build')];
out.groups = C.groupByKind([
  {key: 'b', kind: 'timeseries', display_name: 'B'},
  {key: 'a', kind: 'filings', display_name: 'A'},
  {key: 'c', kind: 'weird', display_name: 'C'},
  {key: 'd', kind: 'filings', display_name: 'D'},
]).map(g => [g[0], g[1].map(d => d.key)]);
out.changed = Array.from(C.changedKeys(
  [{key: 'a', status: 'current'}, {key: 'b', status: 'behind'}, {key: 'gone', status: 'x'}],
  [{key: 'a', status: 'current'}, {key: 'b', status: 'current'}, {key: 'new', status: 'x'}],
)).sort();
out.jobs = ['bulk:sec_adv', 'job:pe_mart_build#firms', 'dispatch:fred:series', 'dispatch:census',
            'collector:power_plants', 'api:foo', 'dispatch:'].map(C.jobsQueryFor);
const NOW = Date.parse('2026-09-23T12:00:00Z');
out.rel = [C.relText('2026-09-23T11:55:00Z', 'ago', NOW), C.relText('2026-09-23T11:55:00', 'ago', NOW),
           C.relText('2026-09-23T14:00:00Z', 'until', NOW), C.relText('2026-09-23T09:00:00Z', 'until', NOW),
           C.relText(null, 'ago', NOW)];
out.stale = [C.isStale('2026-09-23T11:55:00Z', NOW, 600000), C.isStale('2026-09-23T11:45:00Z', NOW, 600000),
             C.isStale(null, NOW, 600000)];
out.plan = [C.livePlan({worker: {worker_mode: false}}, true), C.livePlan({worker: {worker_mode: true}}, false),
            C.livePlan({worker: {worker_mode: true}}, true), C.livePlan(null, true)].map(p => [p.sse, p.retry]);
out.pending = C.pendingCoverage([{clocks: {coverage_error: 'deadline'}}, {clocks: {coverage_error: 'timeout'}},
                                 {clocks: {coverage_error: 'empty'}}, {clocks: {}}, {}]);
out.mart = ['job:pe_mart_build#funds', 'job:entity_resolve', 'bulk:x'].map(C.martFor);
out.bulk = ['bulk:sec_adv_schedule_d', 'job:x'].map(C.bulkSourceFor);
console.log(JSON.stringify(out));
"""


@pytest.mark.unit
@pytest.mark.skipif(NODE is None, reason="node not installed")
class TestStatusCoreNode:
    @pytest.fixture(scope="class")
    def out(self):
        html = STATUS.read_text(encoding="utf-8")
        m = re.search(r'<script id="status-core">(.*?)</script>', html, flags=re.S)
        assert m, "status-core script block missing"
        proc = subprocess.run([NODE, "-e", _NODE_HARNESS % m.group(1)],
                              capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_esc(self, out):
        assert out["esc"] == "&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;"
        assert out["escNull"] == ""

    def test_attention(self, out):
        assert out["attention"] == 9
        assert out["attentionMissing"] == 0

    def test_lag_units(self, out):
        assert out["lagMonthly"] == pytest.approx(61 / 31, rel=1e-6)
        assert out["lagNone"] is None
        assert out["lagZero"] == 0
        assert out["lagCap"] == 3

    def test_job_type_prefix(self, out):
        assert out["prefix"] == ["bulk:", "dispatch:", "collector:",
                                 "job:pe_mart_build", "job:entity_resolve"]
        assert out["match"] == [True, True, False, False]

    def test_group_order(self, out):
        assert out["groups"] == [["filings", ["a", "d"]], ["timeseries", ["b"]],
                                 ["weird", ["c"]]]

    def test_changed_keys(self, out):
        assert out["changed"] == ["b", "gone", "new"]

    def test_producer_lookups(self, out):
        assert out["jobs"] == [
            {"producer": "bulk:sec_adv"},
            {"producer": "job:pe_mart_build"},
            {"producer": "dispatch:fred:series"},
            {"producer": "dispatch:census"},
            None, None, None,
        ]
        assert out["mart"] == ["pe_marts", "entity_resolve", None]
        assert out["bulk"] == ["sec_adv_schedule_d", None]

    def test_t10_relative_times_and_staleness(self, out):
        assert out["rel"] == ["5m ago", "5m ago", "in 2h", "3h overdue", ""]
        assert out["stale"] == [False, True, False]

    def test_t10_live_plan(self, out):
        # WORKER_MODE=0: no SSE (no queue events would come), poll instead
        assert out["plan"] == [[False, False], [False, False], [True, True], [True, True]]
        assert out["pending"] == 2


# =============================================================================
# T9-T10 — page wiring after the review fixes (static)
# =============================================================================


@pytest.mark.unit
class TestReviewFixesStatic:
    def test_t9_runs_filtered_server_side(self, status_html):
        assert "?producer=${encodeURIComponent(q.producer)}" in status_html
        # no client-side config.dataset filtering and no source= history query
        assert "(j.config || {}).dataset" not in status_html
        assert "${ENDPOINTS.jobs}?source=" not in status_html
        assert "X-Producer-Scan-Truncated" in status_html

    def test_t10_refreshes_without_events(self, status_html):
        assert "C.livePlan(" in status_html and "worker_mode === false" in status_html
        assert "SAFETY_REFRESH_MS" in status_html and "function tick()" in status_html
        assert 'data-rel="${kind}"' in status_html
        assert "Retrying on the next update" not in status_html

    def test_t11_build_error_admin_only(self, status_html):
        assert re.search(r"b\.error && showErrors", status_html)
        assert "const showErrors = isAdmin && state.view === 'operator'" in status_html


# =============================================================================
# T9 — GET /jobs?producer= (server-side producer filter)
# =============================================================================


@pytest.mark.unit
def test_t9_producer_source_candidates():
    from app.api.v1.jobs import producer_source_candidates as cands

    assert cands("dispatch:treasury") == ["treasury"]
    assert cands("dispatch:treasury:auctions") == ["treasury", "treasury:auctions"]
    assert cands("bulk:sec_adv") == ["bulk:sec_adv"]
    assert cands("job:pe_mart_build#firms") == ["job:pe_mart_build"]
    assert cands("collector:power_plants") == []
    assert cands("api:foo") == [] and cands("") == [] and cands("dispatch:") == []


@pytest.fixture
def pgjobs():
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    from app.core.models import Base, IngestionJob

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS public.ingestion_jobs CASCADE"))
    Base.metadata.create_all(engine, tables=[IngestionJob.__table__])
    rows = [
        # (source, config, minutes ago)
        ("treasury", '{"dataset": "auctions"}', 1),
        ("treasury", "{}", 2),
        ("treasury:split_1", "{}", 3),
        ("treasury", '{"dataset": "not_a_catalog_key"}', 4),
        ("treasury", '{"dataset": "auctions"}', 5),
        ("treasuryx", "{}", 6),
        ("fred", "{}", 7),
    ]
    with engine.begin() as conn:
        for src, cfg, ago in rows:
            conn.execute(text(
                "INSERT INTO ingestion_jobs (source, status, config, created_at, retry_count, "
                "max_retries, data_origin) VALUES (:s, 'success', CAST(:c AS json), "
                "NOW() - make_interval(mins => :m), 0, 3, 'real')"), {"s": src, "c": cfg, "m": ago})
    db = sessionmaker(bind=engine)()
    yield db
    db.close()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ingestion_jobs"))
    engine.dispose()


def _producer_jobs(db, producer, **kw):
    from fastapi import Response

    from app.api.v1 import jobs

    resp = Response()
    out = jobs.list_jobs(response=resp, source=None, status=None, batch_run_id=None,
                         trigger=None, producer=producer, limit=kw.get("limit", 10),
                         offset=kw.get("offset", 0), db=db)
    return [(j.source, (j.config or {}).get("dataset")) for j in out], resp.headers


@pg
def test_t9_bare_dispatch_key_excludes_qualified_siblings(pgjobs):
    from app.catalog.job_keys import default_map

    assert "treasury:auctions" in default_map().dispatch_keys, "catalog changed: pick another pair"
    got, headers = _producer_jobs(pgjobs, "dispatch:treasury")
    assert got == [("treasury", None), ("treasury:split_1", None),
                   ("treasury", "not_a_catalog_key")]
    assert "x-producer-scan-truncated" not in headers


@pg
def test_t9_qualified_key_finds_its_jobs(pgjobs):
    got, _ = _producer_jobs(pgjobs, "dispatch:treasury:auctions")
    assert got == [("treasury", "auctions"), ("treasury", "auctions")]
    got, _ = _producer_jobs(pgjobs, "dispatch:treasury:auctions", limit=1, offset=1)
    assert got == [("treasury", "auctions")]
    assert _producer_jobs(pgjobs, "collector:power_plants")[0] == []


@pg
def test_t9_scan_cap_is_reported(pgjobs, monkeypatch):
    from app.api.v1 import jobs

    monkeypatch.setattr(jobs, "PRODUCER_SCAN_BATCH", 1)
    monkeypatch.setattr(jobs, "PRODUCER_SCAN_MAX", 2)
    got, headers = _producer_jobs(pgjobs, "dispatch:treasury")
    assert got == [("treasury", None)]          # newest auctions row skipped, cap hit
    assert headers.get("x-producer-scan-truncated") == "true"


# =============================================================================
# T11 — /pe/marts/builds: error text for admins only, free text redacted
# =============================================================================


@pytest.mark.unit
def test_t11_present_build_redacts_and_gates_errors():
    from app.api.v1.mart_builds import present_build

    row = {"id": 1, "status": "failed",
           "error": "HTTPStatusError for https://x/api?api_key=SEKRET123 boom",
           "refusal_reason": "input fetch failed: https://y/?token=TOK999",
           "inputs": [{"source": "sec_adv", "ok": False, "problem": "GET ?apikey=K1 failed"},
                      {"source": "x", "ok": True}]}
    admin = present_build(dict(row), include_errors=True)
    assert "SEKRET123" not in admin["error"] and "boom" in admin["error"]
    viewer = present_build(dict(row), include_errors=False)
    assert viewer["error"] is None and viewer["error_hidden"] is True
    for b in (admin, viewer):
        assert "TOK999" not in b["refusal_reason"]
        assert "K1" not in b["inputs"][0]["problem"]
        assert b["inputs"][1] == {"source": "x", "ok": True}
    ok = present_build({"id": 2, "status": "success", "error": None}, include_errors=False)
    assert ok["error"] is None and ok["error_hidden"] is False


@pg
def test_t11_builds_endpoint_hides_error_from_non_admin():
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    from app.api.v1 import mart_builds
    from app.core.authz import ROLE_ADMIN

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS core.mart_build (
                id BIGSERIAL PRIMARY KEY, mart VARCHAR(64) NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'running', dry_run BOOLEAN NOT NULL DEFAULT FALSE,
                started_at TIMESTAMP NOT NULL DEFAULT NOW(), finished_at TIMESTAMP,
                code_version VARCHAR(64), inputs JSONB, stage_counts JSONB, gate_results JSONB,
                overrides JSONB, refusal_reason TEXT, error TEXT, ingestion_job_id INTEGER,
                job_queue_id INTEGER)"""))
        conn.execute(text("DELETE FROM core.mart_build"))
        conn.execute(text("INSERT INTO core.mart_build (mart, status, error) "
                          "VALUES ('pe_marts', 'failed', 'psycopg2 trace password=hunter2')"))
    db = sessionmaker(bind=engine)()
    try:
        viewer = mart_builds.list_builds(mart=None, status=None, limit=5, db=db,
                                         principal={"role": "viewer"})
        assert viewer["builds"][0]["error"] is None
        assert viewer["builds"][0]["error_hidden"] is True
        admin = mart_builds.list_builds(mart=None, status=None, limit=5, db=db,
                                        principal={"role": ROLE_ADMIN})
        assert "psycopg2 trace" in admin["builds"][0]["error"]
        assert "hunter2" not in admin["builds"][0]["error"]
    finally:
        db.close()
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM core.mart_build"))
        engine.dispose()


# =============================================================================
# T12 — DOM smoke under node + jsdom (skipped when jsdom does not resolve)
# =============================================================================

SMOKE = REPO / "tests" / "js" / "status_page_smoke.js"


def _jsdom_available() -> bool:
    if NODE is None:
        return False
    proc = subprocess.run([NODE, "-e", "require.resolve('jsdom')"], capture_output=True,
                          text=True, timeout=30, env=dict(os.environ))
    return proc.returncode == 0


@pytest.mark.unit
@pytest.mark.skipif(not _jsdom_available(), reason="node + jsdom not available (set NODE_PATH)")
@pytest.mark.parametrize("scenario", ["admin", "inprocess", "viewer", "coverage"])
def test_t12_dom_smoke(scenario):
    proc = subprocess.run([NODE, str(SMOKE), str(REPO), scenario], capture_output=True,
                          text=True, timeout=120, env=dict(os.environ))
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    assert out["ok"], proc.stdout
    assert not out["fail"], out["fail"]
