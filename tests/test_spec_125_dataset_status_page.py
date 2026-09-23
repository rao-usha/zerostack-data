"""
Tests for SPEC 125 — the dataset status page (frontend/status.html).

Static checks on the page (auth.js, read-only, API paths that exist, live
updates through a stream token, narrow-width columns) and on the repaired
index.html panel. The page's pure helpers (`StatusCore`, in the
`<script id="status-core">` block) run under node when it is installed.
Browser tests are not run: Playwright is not available here.
"""
import json
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
        m = re.search(r"FALLBACK_POLL_MS\s*=\s*(\d[\d_]*)", status_html)
        assert m, "fallback poll interval must be a named constant"
        assert int(m.group(1).replace("_", "")) >= 60000
        # the only interval timer is the fallback poll (and the SSE retry)
        intervals = re.findall(r"setInterval\(([^)]*)", status_html)
        for args in intervals:
            assert "FALLBACK_POLL_MS" in args or "SSE_RETRY_MS" in args, args
        m = re.search(r"SSE_RETRY_MS\s*=\s*(\d[\d_]*)", status_html)
        assert m and int(m.group(1).replace("_", "")) >= 60000

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
            'collector:power_plants', 'api:foo'].map(C.jobsQueryFor);
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
            {"source": "bulk:sec_adv", "dataset": None},
            {"source": "job:pe_mart_build", "dataset": None},
            {"source": "fred", "dataset": "series"},
            {"source": "census", "dataset": None},
            None, None,
        ]
        assert out["mart"] == ["pe_marts", "entity_resolve", None]
        assert out["bulk"] == ["sec_adv_schedule_d", None]
