"""
Tests for SPEC_145 — catalog browser page, search/facets API and JSON-LD
(PLAN_088 §3 "SPEC_139").

- the in-memory search index: ranking, AND + prefix matching, column hits,
  filters, disjunctive facets, row estimates that never count rows;
- GET /catalog/search: validation, auth (user-level, mounted in app.main);
- to_jsonld / GET /catalog/{key}/jsonld: schema.org shape, rights gating,
  never an open licence for unreviewed or restricted rights;
- frontend/catalog.html: auth.js first, API paths exist, every interpolation
  escaped (a small JS template scanner), warning-banner logic, nav links;
- the page's pure helpers under node, and a jsdom smoke test rendering a
  detail page full of malicious catalog text (skipped without jsdom).
"""
import dataclasses
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CATALOG_HTML = REPO / "frontend" / "catalog.html"
INDEX = REPO / "frontend" / "index.html"
STATUS = REPO / "frontend" / "status.html"
SMOKE = REPO / "tests" / "js" / "catalog_page_smoke.js"
JWT_SECRET = "spec145-test-secret-" + "x" * 48
_DUMMY_DB = "postgresql://nobody:nobody@localhost:1/nothing"
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")
NODE = shutil.which("node")


def _catalog():
    from app.catalog import get_catalog

    return get_catalog()


def _spec(key):
    from app.catalog import get_spec

    s = get_spec(key)
    assert s is not None, key
    return s


def _search(q=None, **filters):
    from app.catalog.search import search

    return search(q=q, filters=filters)


def _keys(body):
    return [r["key"] for r in body["results"]]


# =============================================================================
# 1. Search index
# =============================================================================


@pytest.mark.unit
class TestRanking:
    def test_form_d_ranks_sec_form_d_first(self):
        body = _search("form d")
        assert _keys(body)[0] == "sec_form_d"
        assert body["results"][0]["score"] > body["results"][1]["score"]

    def test_cik_hits_via_column_names(self):
        body = _search("cik")
        by_key = {r["key"]: r for r in body["results"]}
        via_columns = [k for k, r in by_key.items() if r["matched"] == ["columns"]]
        assert via_columns, "some dataset should match cik only through its columns"
        from app.catalog.dictionary import dataset_tables_offline, load_dictionary

        d = load_dictionary()
        k = via_columns[0]
        cols = {c["name"] for t in dataset_tables_offline(_spec(k), d) for c in d["tables"][t]["columns"]}
        assert any("cik" in c for c in cols)

    def test_name_beats_column(self):
        body = _search("cik")
        # entity_cik_crd_bridge has cik in its key: ranked above column-only hits
        assert _keys(body)[0] == "entity_cik_crd_bridge"

    def test_and_semantics(self):
        a, b = set(_keys(_search("sec"))), set(_keys(_search("holdings")))
        both = set(_keys(_search("sec holdings")))
        assert both and both <= (a & b)

    def test_prefix_match(self):
        assert "sec_form_d" in _keys(_search("offer"))       # "offerings"
        assert _search("zzqqxx")["count"] == 0

    def test_one_letter_token_is_exact_only(self):
        from app.catalog.search import build_doc, score

        d = build_doc(_spec("sec_form_d"), [])
        assert score(d, "d")[0] > 0
        assert score(d, "x")[0] == 0    # "x" is not a prefix-match for anything

    def test_highlight_is_a_snippet_around_the_match(self):
        body = _search("offerings")
        hl = [r["highlight"] for r in body["results"] if r["highlight"]]
        assert hl and all("offer" in h.lower() and len(h) <= 170 for h in hl)
        assert all(r["matched"] for r in body["results"])
        assert _search()["results"][0]["highlight"] is None

    def test_search_is_fast(self):
        import time

        from app.catalog.search import default_index

        default_index()
        t = time.perf_counter()
        for q in ("form d", "cik", "interest rate", "adv private funds", "zip"):
            _search(q)
        assert (time.perf_counter() - t) / 5 < 0.25


@pytest.mark.unit
class TestFiltersAndFacets:
    def test_empty_query_lists_everything(self):
        body = _search()
        assert body["count"] == body["total"] == len(_catalog())
        assert set(body["facets"]) >= {"kind", "source", "data_state", "redistribution", "pii_class",
                                        "status_public", "identifier", "effective_redistribution"}

    def test_single_valued_facets_sum_to_count(self):
        for q in (None, "sec", "rate"):
            body = _search(q)
            for f in ("kind", "source", "status_public", "data_state", "redistribution",
                      "effective_redistribution", "pii_class", "origin"):
                assert sum(body["facets"][f].values()) == body["count"], (q, f)

    def test_disjunctive_facets(self):
        body = _search(kind=["filings"])
        assert all(r["kind"] == "filings" for r in body["results"])
        # the kind facet ignores its own filter: other kinds still counted
        assert len(body["facets"]["kind"]) > 1
        assert body["facets"]["kind"]["filings"] == body["count"]
        # other facets are counted over the filtered set
        assert sum(body["facets"]["pii_class"].values()) == body["count"]

    def test_comma_or_within_facet_and_across(self):
        a = _search(kind=["filings"])["count"]
        b = _search(kind=["entity"])["count"]
        assert _search(kind=["filings", "entity"])["count"] == a + b
        both = _search(kind=["filings"], pii_class=["none"])
        assert all(r["kind"] == "filings" and r["pii_class"] == "none" for r in both["results"])

    def test_identifier_facet(self):
        body = _search(identifier=["cik"])
        assert body["count"] > 0
        assert all("cik" in r["identifiers"] for r in body["results"])

    def test_data_state_unverified(self, monkeypatch):
        from app.catalog.search import build_index, search

        s = dataclasses.replace(_spec("fred_series"), data_state=None)
        monkeypatch.setattr("app.catalog.quality.CURATED_DATA_STATES", {})
        idx = build_index([s], body={"tables": {}})
        body = search(filters={"data_state": ["unverified"]}, index=idx)
        assert body["count"] == 1 and body["results"][0]["data_state"] == "unverified"

    def test_result_carries_rights_and_flags(self):
        r = next(r for r in _search()["results"] if r["rights_gate"])
        assert r["effective_redistribution"] in ("internal_only", "restricted")
        flagged = [r for r in _search(data_state=["fabricated"])["results"]]
        assert flagged and all("fabricated" in r["quality_flags"] for r in flagged)


@pytest.mark.unit
class TestRowEstimates:
    def _spec(self, **kw):
        fields = dict(key="t145_ds", tables=("t145_a", "t145_b"), table_patterns=(),
                      row_filters=(), missing_tables=(), data_state="ok")
        fields.update(kw)
        return dataclasses.replace(_spec("fred_series"), **fields)

    def test_from_live_cache(self, monkeypatch):
        from app.catalog import live, quality
        from app.catalog.search import row_estimates

        spec = self._spec()
        monkeypatch.setattr(live, "_cached", lambda key, ttl: {"rows_total": 42, "rows_exact": True})

        def boom(*a, **k):
            raise AssertionError("relations must not be read when the live cache holds the dataset")

        monkeypatch.setattr(quality, "relations", boom)
        assert row_estimates(object(), [spec])["t145_ds"] == {
            "row_estimate": 42, "rows_exact": True, "rows_from": "live_cache"}

    def test_from_relation_estimates(self, monkeypatch):
        from app.catalog import live, quality
        from app.catalog.search import row_estimates

        spec = self._spec()
        monkeypatch.setattr(live, "_cached", lambda key, ttl: None)
        rels = {"t145_a": {"kind": "table", "reltuples": 100, "n_live_tup": 0},
                "t145_b": {"kind": "table", "reltuples": 50, "n_live_tup": 60}}
        calls = []
        monkeypatch.setattr(quality, "relations", lambda engine, cached=False: calls.append(cached) or rels)
        out = row_estimates(object(), [spec, dataclasses.replace(spec, key="t145_other")])
        assert out["t145_ds"] == {"row_estimate": 160, "rows_exact": False, "rows_from": "estimate"}
        assert calls == [True], "one cached relations read for the whole page"

    def test_row_filtered_is_unknown_and_missing_is_unknown(self, monkeypatch):
        from app.catalog import live, quality
        from app.catalog.search import row_estimates

        monkeypatch.setattr(live, "_cached", lambda key, ttl: None)
        rels = {"t145_a": {"kind": "table", "reltuples": 100, "n_live_tup": None}}
        monkeypatch.setattr(quality, "relations", lambda engine, cached=False: rels)
        filtered = self._spec(row_filters=(("t145_a", "x = 1"),))
        missing = dataclasses.replace(self._spec(), key="t145_missing", tables=("t145_zz",))
        out = row_estimates(object(), [filtered, missing])
        assert out["t145_ds"]["row_estimate"] is None
        assert out["t145_missing"]["row_estimate"] is None

    def test_failure_is_null(self, monkeypatch):
        from app.catalog import live, quality
        from app.catalog.search import row_estimates

        monkeypatch.setattr(live, "_cached", lambda key, ttl: None)

        def boom(*a, **k):
            raise RuntimeError("db down")

        monkeypatch.setattr(quality, "relations", boom)
        assert row_estimates(object(), [self._spec()])["t145_ds"]["row_estimate"] is None


@pg
def test_row_estimates_never_count_on_postgres():
    """Real engine: the estimate path issues catalog reads only, never count(*)."""
    from sqlalchemy import create_engine, event, text

    from app.catalog import live, quality
    from app.catalog.search import row_estimates

    engine = create_engine(PG_URL)
    stmts = []
    event.listen(engine, "before_cursor_execute", lambda c, cur, s, p, ctx, many: stmts.append(s.lower()))
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS t145_a"))
            conn.execute(text("CREATE TABLE t145_a (id int)"))
            conn.execute(text("INSERT INTO t145_a SELECT generate_series(1, 500)"))
            conn.execute(text("ANALYZE t145_a"))
        live.clear_cache()
        quality.clear_cache()
        stmts.clear()
        spec = dataclasses.replace(_spec("fred_series"), key="t145_ds", tables=("t145_a",), table_patterns=(),
                                   row_filters=(), missing_tables=(), data_state="ok")
        out = row_estimates(engine, [spec])
        assert out["t145_ds"]["row_estimate"] == 500
        assert stmts and not any("count(" in s for s in stmts), stmts
        stmts.clear()
        row_estimates(engine, [spec])
        assert not stmts, "second call served from the relations cache"
    finally:
        quality.clear_cache()
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS t145_a"))
        engine.dispose()


# =============================================================================
# 2. GET /catalog/search and /catalog/{key}/jsonld (router)
# =============================================================================


class _Db:
    def get_bind(self):
        raise RuntimeError("no database in this test")


def _client(principal=None, dependencies=None):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api.v1 import catalog
    from app.core.authz import current_principal
    from app.core.database import get_db

    app = FastAPI()
    app.include_router(catalog.router, prefix="/api/v1", dependencies=dependencies or [])
    app.dependency_overrides[get_db] = lambda: _Db()
    if principal is not None:
        app.dependency_overrides[current_principal] = lambda: principal
    return TestClient(app)


USER = {"role": "user", "email": "u@x"}
ADMIN = {"role": "admin", "email": "a@x"}


@pytest.mark.unit
class TestSearchRoute:
    def test_search_ok(self):
        r = _client(USER).get("/api/v1/catalog/search", params={"q": "form d"})
        assert r.status_code == 200
        body = r.json()
        assert body["results"][0]["key"] == "sec_form_d"
        assert "facets" in body and body["facets"]["kind"]
        # no database: row estimates degrade to null, never a 500
        assert "row_estimate" in body["results"][0]

    def test_search_route_not_shadowed_by_key(self):
        r = _client(USER).get("/api/v1/catalog/search")
        assert r.status_code == 200 and r.json()["count"] == len(_catalog())

    def test_filters_comma_separated(self):
        r = _client(USER).get("/api/v1/catalog/search", params={"kind": "filings,entity", "rows": "false"})
        assert r.status_code == 200
        kinds = {x["kind"] for x in r.json()["results"]}
        assert kinds <= {"filings", "entity"} and kinds

    @pytest.mark.parametrize("param,value", [("kind", "nope"), ("data_state", "bogus"),
                                             ("redistribution", "free"), ("identifier", "ssn"),
                                             ("pii_class", "x"), ("keyword", "zz")])
    def test_bad_vocabulary_422(self, param, value):
        r = _client(USER).get("/api/v1/catalog/search", params={param: value})
        assert r.status_code == 422

    def test_row_estimates_attached(self, monkeypatch):
        from app.catalog import search as search_mod

        monkeypatch.setattr(search_mod, "row_estimates",
                            lambda engine, specs: {s.key: {"row_estimate": 7, "rows_exact": False,
                                                           "rows_from": "estimate"} for s in specs})
        r = _client(USER).get("/api/v1/catalog/search", params={"q": "form d", "limit": 3})
        assert [x["row_estimate"] for x in r.json()["results"]] == [7, 7, 7]


@pytest.mark.unit
class TestAuth:
    def test_mounted_user_level_in_main(self, monkeypatch):
        from fastapi.routing import APIRoute

        from app.core.authz import require_admin_for_writes
        from app.core.config import reset_settings

        monkeypatch.setenv("DATABASE_URL", _DUMMY_DB)
        monkeypatch.setenv("JWT_SECRET_KEY", JWT_SECRET)
        reset_settings()
        try:
            from app.main import app

            routes = {r.path: r for r in app.routes if isinstance(r, APIRoute)}
        finally:
            reset_settings()
        for path in ("/api/v1/catalog/search", "/api/v1/catalog/{key}/jsonld"):
            assert path in routes, path
            deps = [d.call for d in routes[path].dependant.dependencies]
            assert require_admin_for_writes in deps, path
        # /search is registered before /{key} (otherwise it would be a dataset key)
        order = [r.path for r in app.routes if isinstance(r, APIRoute)]
        assert order.index("/api/v1/catalog/search") < order.index("/api/v1/catalog/{key}")

    def test_401_without_credentials(self, monkeypatch):
        from fastapi import Depends

        from app.core.authz import require_admin_for_writes
        from app.core.config import reset_settings

        monkeypatch.setenv("REQUIRE_AUTH", "true")
        monkeypatch.setenv("DATABASE_URL", _DUMMY_DB)
        monkeypatch.setenv("JWT_SECRET_KEY", JWT_SECRET)
        reset_settings()
        try:
            c = _client(None, dependencies=[Depends(require_admin_for_writes)])
            assert c.get("/api/v1/catalog/search", params={"q": "cik"}).status_code == 401
            assert c.get("/api/v1/catalog/fred_series/jsonld").status_code == 401
        finally:
            reset_settings()

    def test_user_principal_allowed(self):
        from fastapi import Depends

        from app.core.authz import require_admin_for_writes

        c = _client(USER, dependencies=[Depends(require_admin_for_writes)])
        assert c.get("/api/v1/catalog/search", params={"q": "cik", "rows": "false"}).status_code == 200


# =============================================================================
# 3. JSON-LD
# =============================================================================


def _ungated():
    from app.catalog.jsonld import refusal_reasons

    return [s for s in _catalog() if not refusal_reasons(s)]


@pytest.mark.unit
class TestJsonLd:
    def test_required_fields_for_every_ungated_spec(self):
        from app.catalog.jsonld import REQUIRED_FIELDS, to_jsonld

        specs = _ungated()
        assert len(specs) > 100
        for s in specs:
            doc = to_jsonld(s)
            missing = [f for f in REQUIRED_FIELDS if f not in doc]
            assert not missing, (s.key, missing)
            assert doc["@type"] == "Dataset" and doc["isAccessibleForFree"] is False
            assert doc["@context"]["@vocab"] == "https://schema.org/"
            assert len(doc["description"]) >= 50
            json.dumps(doc)

    def test_pe_pack_shape(self):
        from app.catalog.dictionary import PE_ENTITY_PACK
        from app.catalog.jsonld import to_jsonld

        for key in PE_ENTITY_PACK:
            doc = to_jsonld(_spec(key), base_url="https://nexdata.example/")
            assert doc["@id"] == f"https://nexdata.example/api/v1/catalog/{key}"
            assert doc["variableMeasured"], key
            v = doc["variableMeasured"][0]
            assert v["@type"] == "PropertyValue" and v["name"]
            urls = [d["contentUrl"] for d in doc["distribution"]]
            assert f"https://nexdata.example/api/v1/catalog/{key}/schema" in urls
            assert all(d["@type"] == "DataDownload" for d in doc["distribution"])

    def test_temporal_spatial_and_frequency(self):
        from app.catalog.jsonld import to_jsonld

        s = dataclasses.replace(_spec("sec_form_d"), coverage_from="2009-01-01", spatial_coverage="US:state",
                                cadence="quarterly")
        doc = to_jsonld(s)
        assert doc["temporalCoverage"] == "2009-01-01/.."
        assert doc["spatialCoverage"] == {"@type": "Place", "name": "United States (state level)"}
        assert doc["dct:accrualPeriodicity"] == "http://purl.org/cld/freq/quarterly"
        s2 = dataclasses.replace(s, coverage_from=None, spatial_coverage=None)
        doc2 = to_jsonld(s2)
        assert "temporalCoverage" not in doc2 and "spatialCoverage" not in doc2

    def test_every_gated_spec_refused(self):
        from app.catalog.jsonld import JsonLdRefused, to_jsonld

        gated = [s for s in _catalog() if s.rights_gate]
        assert gated
        for s in gated:
            with pytest.raises(JsonLdRefused):
                to_jsonld(s)

    def test_retired_refused(self):
        from app.catalog.jsonld import JsonLdRefused, to_jsonld

        s = dataclasses.replace(_spec("sec_form_d"), status_public="retired")
        with pytest.raises(JsonLdRefused) as e:
            to_jsonld(s)
        assert "retired" in e.value.reasons

    def test_unreviewed_open_is_not_emitted_open(self):
        from app.catalog.jsonld import publishable, to_jsonld

        s = dataclasses.replace(_spec("sec_form_d"), redistribution="open", reviewed=False,
                                license_url="https://example.org/pd")
        doc = to_jsonld(s)
        assert isinstance(doc["license"], dict) and "pending" in doc["license"]["name"].lower()
        assert "https://example.org/pd" not in json.dumps(doc["license"])
        assert "Internal use only" in doc["conditionsOfAccess"]
        assert publishable(s) is False

    def test_restricted_never_open(self):
        from app.catalog.jsonld import to_jsonld

        for reviewed in (False, True):
            s = dataclasses.replace(_spec("sec_form_d"), redistribution="restricted", reviewed=reviewed)
            doc = to_jsonld(s)
            assert isinstance(doc["license"], dict)
            assert "restricted" in doc["license"]["name"].lower()
            assert "Redistribution permitted" not in doc["conditionsOfAccess"]

    def test_no_spec_emits_open_today(self):
        """No catalog spec is reviewed yet: none may carry an upstream licence as its licence."""
        from app.catalog.jsonld import to_jsonld

        for s in _ungated():
            lic = to_jsonld(s)["license"]
            if s.reviewed and s.effective_redistribution in ("open", "attribution"):
                continue
            assert isinstance(lic, dict) and lic["@type"] == "CreativeWork", s.key

    def test_reviewed_open_fixture_is_open_and_publishable(self, monkeypatch):
        from app.catalog.jsonld import publishable, to_jsonld

        s = dataclasses.replace(_spec("sec_form_d"), redistribution="open", reviewed=True,
                                license_url="https://example.org/pd", status_public="beta", data_state="ok",
                                storage="allowed", commercial_use="allowed")
        doc = to_jsonld(s)
        assert doc["license"] == "https://example.org/pd"
        assert "Redistribution permitted" in doc["conditionsOfAccess"]
        assert publishable(s) is True

    def test_short_description_raises(self):
        from app.catalog.jsonld import to_jsonld

        s = dataclasses.replace(_spec("sec_form_d"))
        object.__setattr__(s, "description", "too short")   # bypasses the spec's own check
        with pytest.raises(ValueError):
            to_jsonld(s)

    def test_variables_capped(self):
        from app.catalog.jsonld import variables

        body = {"tables": {"t": {"datasets": ["fred_series"], "columns": [
            {"name": f"c{i}", "description": "d", "unit": None, "semantic_type": None} for i in range(400)]}}}
        assert len(variables(_spec("fred_series"), body)) == 250

    def test_route(self):
        c = _client(USER)
        ok = next(s for s in _ungated())
        r = c.get(f"/api/v1/catalog/{ok.key}/jsonld")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("application/ld+json")
        assert r.headers["x-jsonld-publishable"] == "false"
        assert r.json()["identifier"] == ok.key
        gated = next(s for s in _catalog() if s.rights_gate)
        r = c.get(f"/api/v1/catalog/{gated.key}/jsonld")
        assert r.status_code == 403
        assert r.json()["detail"]["reasons"] == list(gated.rights_gate)
        assert c.get("/api/v1/catalog/no_such_ds/jsonld").status_code == 404


# =============================================================================
# 4. catalog.html (static)
# =============================================================================


@pytest.fixture(scope="module")
def page() -> str:
    return CATALOG_HTML.read_text(encoding="utf-8")


def _script(html, sid):
    m = re.search(r'<script id="%s">(.*?)</script>' % sid, html, flags=re.S)
    assert m, sid
    return m.group(1)


# --- a small JS template-literal scanner ------------------------------------


def _skip_string(s, i):
    q = s[i]
    i += 1
    while i < len(s):
        if s[i] == "\\":
            i += 2
            continue
        if s[i] == q:
            return i + 1
        i += 1
    raise ValueError("unterminated string")


def _parse_template(s, i, exprs):
    """s[i] == '`'. Appends every ${...} (normalised) to exprs; returns the index after '`'."""
    i += 1
    while i < len(s):
        c = s[i]
        if c == "\\":
            i += 2
            continue
        if c == "`":
            return i + 1
        if c == "$" and s[i + 1] == "{":
            i, expr = _parse_expr(s, i + 2, exprs, "}")
            exprs.append(expr)
            continue
        i += 1
    raise ValueError("unterminated template")


def _parse_expr(s, i, exprs, end):
    """Read an expression up to ``end`` at depth 0. Strings become S, templates T."""
    depth, out = 0, []
    while i < len(s):
        c = s[i]
        if c in "'\"":
            i = _skip_string(s, i)
            out.append("S")
            continue
        if c == "`":
            i = _parse_template(s, i, exprs)
            out.append("T")
            continue
        if depth == 0 and c in end:
            return i + (1 if c == "}" else 0), "".join(out).strip()
        if c in "{([":
            depth += 1
        elif c in "})]":
            depth -= 1
        out.append(c)
        i += 1
    raise ValueError("unterminated expression")


def scan_templates(src):
    """Every ${...} expression of every template literal in ``src`` (comments skipped)."""
    exprs, i = [], 0
    while i < len(src):
        if src.startswith("//", i):
            i = src.find("\n", i)
            i = len(src) if i < 0 else i
            continue
        if src.startswith("/*", i):
            i = src.index("*/", i) + 2
            continue
        c = src[i]
        if c in "'\"":
            i = _skip_string(src, i)
            continue
        if c == "`":
            i = _parse_template(src, i, exprs)
            continue
        i += 1
    return exprs


def inner_html_rhs(src):
    """(operator, normalised right-hand side) of every innerHTML assignment."""
    out = []
    for m in re.finditer(r"\.innerHTML\s*(\+?=)(?!=)", src):
        _, rhs = _parse_expr(src, m.end(), [], ";")
        out.append((m.group(1), rhs))
    return out


_CALL = re.compile(r"(esc|[A-Za-z_]\w*Html)\(")


def _balanced_call(n):
    m = _CALL.match(n)
    if not m:
        return False
    depth = 0
    for k in range(m.end() - 1, len(n)):
        if n[k] == "(":
            depth += 1
        elif n[k] == ")":
            depth -= 1
            if depth == 0:
                return k == len(n) - 1
    return False


def _top_level(n, ch, start=0):
    depth = 0
    for k in range(start, len(n)):
        c = n[k]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == ch and depth == 0:
            return k
    return None


_MAP_JOIN = re.compile(r".*\.map\(\s*(\(?[\w\s,]*\)?)\s*=>\s*(T|[A-Za-z_]\w*Html\(.*\))\s*\)\.join\(S\)", re.S)
_MAP_REF = re.compile(r".*\.map\([A-Za-z_]\w*Html\)\.join\(S\)", re.S)


def safe_expr(n):
    n = n.strip()
    if n in ("S", "T"):
        return True
    if re.fullmatch(r"[A-Za-z_]\w*Html", n) or _balanced_call(n):
        return True
    q = _top_level(n, "?")
    if q is not None:
        colon = _top_level(n, ":", q + 1)
        if colon is not None:
            return safe_expr(n[q + 1:colon]) and safe_expr(n[colon + 1:])
    return bool(_MAP_JOIN.fullmatch(n) or _MAP_REF.fullmatch(n))


@pytest.mark.unit
class TestEscapeChecker:
    """The checker itself: it must reject the mistakes it exists to catch."""

    @pytest.mark.parametrize("src", [
        "x.innerHTML = `<p>${d.description}</p>`;",
        "x.innerHTML = `<p>${esc(a) + b}</p>`;",
        "x.innerHTML = `<p>${list.map(r => r.key).join('')}</p>`;",
        "x.innerHTML = `<p>${ok ? d.name : ''}</p>`;",
        "x.innerHTML = d.description;",
        "x.innerHTML += `<p>${esc(a)}</p>`;",
        "const t = `<a title=\"${`nested ${raw}`}\">`;",
    ])
    def test_rejects(self, src):
        bad = [e for e in scan_templates(src) if not safe_expr(e)]
        bad += [rhs for op, rhs in inner_html_rhs(src) if op != "=" or not safe_expr(rhs)]
        assert bad, src

    @pytest.mark.parametrize("src", [
        "x.innerHTML = `<p>${esc(d.description)}</p>`;",
        "x.innerHTML = rowsHtml(d);",
        "x.innerHTML = ok ? `<b>${esc(a)}</b>` : '';",
        "const h = `<ul>${items.map(i => `<li>${esc(i)}</li>`).join('')}</ul>`;",
        "const h = `<i>${flag ? 'a' : 'b'}</i>${cardHtml(r)}${bodyHtml}`;",
        "const h = `${rows.map(cellHtml).join('')}${cols.map(c => cellHtml(r[c])).join('')}`;",
    ])
    def test_accepts(self, src):
        bad = [e for e in scan_templates(src) if not safe_expr(e)]
        bad += [rhs for op, rhs in inner_html_rhs(src) if op != "=" or not safe_expr(rhs)]
        assert not bad, bad


@pytest.mark.unit
class TestCatalogPage:
    def test_loads_auth_js_first(self, page):
        srcs = re.findall(r"<script\b([^>]*)>", page)
        first = re.search(r'src="([^"]+)"', srcs[0])
        assert first and first.group(1) == "/js/auth.js"
        assert page.count('src="/js/auth.js"') == 1

    def test_no_external_scripts_and_read_only(self, page):
        assert re.findall(r'<script[^>]+src="([^"]+)"', page) == ["/js/auth.js"]
        assert not re.search(r"method\s*:\s*['\"](post|put|patch|delete)['\"]", page, flags=re.I)

    def test_api_paths_exist(self, page, monkeypatch):
        from fastapi.routing import APIRoute

        from app.core.config import reset_settings

        monkeypatch.setenv("DATABASE_URL", _DUMMY_DB)
        monkeypatch.setenv("JWT_SECRET_KEY", JWT_SECRET)
        reset_settings()
        try:
            from app.main import app

            routes = [r for r in app.routes if isinstance(r, APIRoute)]
        finally:
            reset_settings()

        def route_for(path, method):
            concrete = path.replace("{key}", "some_key")
            for r in routes:
                if method not in r.methods:
                    continue
                pattern = "^" + re.sub(r"\{[^}]+\}", "[^/]+", r.path) + "$"
                if re.match(pattern, concrete):
                    return r
            return None

        script = _script(page, "catalog-page")
        endpoints = dict(re.findall(r"^\s*(\w+):\s*'(/api/v1/[^']*)'", script, flags=re.M))
        assert set(endpoints) >= {"search", "list", "status", "entry", "schema", "sample", "lineage",
                                  "rights", "jsonld"}
        for name, path in endpoints.items():
            r = route_for(path, "GET")
            assert r is not None, (name, path)
            # the first matching route is that exact route, not a catch-all /catalog/{key}
            assert r.path == path, (name, r.path)
        others = set(re.findall(r"['\"](/api/v1/[A-Za-z0-9_\-/{}]*)['\"]", script)) - set(endpoints.values())
        for p in others:   # e.g. the export curl text (POST)
            assert route_for(p, "GET") or route_for(p, "POST"), p

    def test_every_interpolation_is_escaped(self, page):
        script = _script(page, "catalog-page")
        exprs = scan_templates(script)
        assert len(exprs) > 100, "scanner found too few interpolations"
        bad = [e for e in exprs if not safe_expr(e)]
        assert not bad, bad[:10]

    def test_inner_html_assignments_are_safe(self, page):
        script = _script(page, "catalog-page")
        assigns = inner_html_rhs(script)
        assert assigns
        for op, rhs in assigns:
            assert op == "=", "innerHTML += is not allowed"
            assert safe_expr(rhs), rhs
        for banned in ("outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function"):
            assert banned not in script, banned

    def test_html_helpers_return_escaped_text(self, page):
        """Every *Html helper with a bare `return <expr>` returns a template, a string,
        esc(...), another *Html call or a map/join of those."""
        script = _script(page, "catalog-page")
        for m in re.finditer(r"function (\w+Html)\(([^)]*)\)\s*\{", script):
            body_start = m.end()
            depth, k = 1, body_start
            while depth:
                if script[k] == "{":
                    depth += 1
                elif script[k] == "}":
                    depth -= 1
                k += 1
            body = script[body_start:k - 1]
            for r in re.finditer(r"\breturn\b", body):
                _, rhs = _parse_expr(body, r.end(), [], ";")
                assert safe_expr(rhs), (m.group(1), rhs)

    def test_esc_helper(self, page):
        core = _script(page, "catalog-core")
        assert "function esc(" in core
        for ch, ent in (("&", "&amp;"), ("<", "&lt;"), (">", "&gt;"), ('"', "&quot;"), ("'", "&#39;")):
            assert ent in core, ent
        assert "const esc = C.esc" in _script(page, "catalog-page")

    def test_warning_banner_logic(self, page):
        core = _script(page, "catalog-core")
        script = _script(page, "catalog-page")
        assert "function flagReasons(" in core
        for needle in ("storage_forbidden", "'fabricated'", "'seeded'", "state !== 'ok'", "gate"):
            assert needle in core, needle
        assert "function bannerHtml(" in script and "${bannerHtml(d)}" in script
        assert 'class="flag-banner' in script

    def test_hash_routes_and_tabs(self, page):
        core = _script(page, "catalog-core")
        assert "'/dataset/" in core or "/dataset/" in core
        for tab in ("overview", "schema", "sample", "lineage", "quality", "rights", "access"):
            assert f"'{tab}'" in core, tab
        assert "hashchange" in _script(page, "catalog-page")

    def test_access_tab_curl_uses_api_key_header(self, page):
        assert "X-API-Key: $NEXDATA_API_KEY" in page

    def test_responsive(self, page):
        assert 'name="viewport"' in page
        widths = [int(w) for w in re.findall(r"@media\s*\(max-width:\s*(\d+)px\)", page)]
        assert widths and min(widths) <= 640
        assert "minmax(0, 1fr)" in page

    def test_dark_tokens_match_status_page(self, page):
        status = STATUS.read_text(encoding="utf-8")
        for token in ("--primary", "--bg", "--bg-card", "--text", "--text-muted", "--warning", "--error"):
            m1 = re.search(re.escape(token) + r":\s*([^;]+);", page)
            m2 = re.search(re.escape(token) + r":\s*([^;]+);", status)
            assert m1 and m2 and m1.group(1) == m2.group(1), token

    def test_nav_links(self):
        index = INDEX.read_text(encoding="utf-8")
        status = STATUS.read_text(encoding="utf-8")
        assert 'href="/catalog.html"' in index and 'href="/catalog.html"' in status
        nav = re.search(r'<nav class="tabs">(.*?)</nav>', index, flags=re.S).group(1)
        link = nav.find('href="/catalog.html"')
        assert link > nav.rfind("<button"), "tab highlighting is positional: link after every button"


# =============================================================================
# 5. catalog-core under node
# =============================================================================

_NODE_HARNESS = r"""
globalThis.window = globalThis;
%s
const C = window.CatalogCore;
const out = {};
out.esc = C.esc('<a href="x">&\'</a>');
out.escNull = C.esc(null);
out.parseDs = C.parseHash('#/dataset/sec_form_d/schema');
out.parseDsBadTab = C.parseHash('#/dataset/sec_form_d/evil');
out.parseBadKey = C.parseHash('#/dataset/%%3Cimg%%3E');
const p = C.parseHash('#/?q=form%%20d&kind=filings,entity&identifier=cik');
out.parseSearch = p;
out.roundTrip = C.parseHash(C.searchHash(p.params));
out.hash = C.searchHash(p.params);
out.query = C.searchQuery(p.params);
out.dsHash = [C.datasetHash('a_b'), C.datasetHash('a_b', 'rights')];
out.flagsGate = C.flagReasons({data_state: 'ok', rights: {gate: ['storage_forbidden'], storage: 'forbidden'}});
out.flagsState = C.flagReasons({data_state: 'fabricated', quality_flags: ['fabricated'], rights_gate: []});
out.flagsSeeded = C.flagReasons({data_state: 'ok', quality_flags: ['seeded'], rights_gate: []});
out.flagsStorage = C.flagReasons({data_state: 'ok', storage: 'forbidden', rights_gate: []});
out.flagsClean = C.flagReasons({data_state: 'ok', quality_flags: [], rights_gate: [], origin: 'official'});
out.flagsUnverified = C.flagReasons({data_state: 'unverified', rights_gate: []});
out.fresh = [C.freshness({status: 'current'}), C.freshness({status: 'failing'}), C.freshness({status: 'behind'}), C.freshness(null)].map(f => f.cls);
out.rows = [C.fmtRows(123), C.fmtRows(12345), C.fmtRows(2500000), C.fmtRows(null), C.fmtRows('x')];
out.spark = C.sparkPoints([{rows: 1}, {rows: 3}], 10, 10);
out.sparkShort = C.sparkPoints([{rows: 1}], 10, 10);
out.safe = [C.safeUrl('https://a.b/c'), C.safeUrl('javascript:alert(1)'), C.safeUrl('http://x" onmouseover=1')];
out.curl = C.curlFor('http://h', '/api/v1/catalog/x/sample');
console.log(JSON.stringify(out));
"""


@pytest.mark.unit
@pytest.mark.skipif(NODE is None, reason="node not installed")
class TestCatalogCoreNode:
    @pytest.fixture(scope="class")
    def out(self):
        core = _script(CATALOG_HTML.read_text(encoding="utf-8"), "catalog-core")
        proc = subprocess.run([NODE, "-e", _NODE_HARNESS % core], capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_esc(self, out):
        assert out["esc"] == "&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;"
        assert out["escNull"] == ""

    def test_hash_routes(self, out):
        assert out["parseDs"] == {"view": "dataset", "key": "sec_form_d", "tab": "schema"}
        assert out["parseDsBadTab"]["tab"] == "overview"
        assert out["parseBadKey"]["view"] == "search"
        assert out["parseSearch"]["params"]["q"] == "form d"
        assert out["parseSearch"]["params"]["kind"] == ["filings", "entity"]
        assert out["roundTrip"] == out["parseSearch"]
        assert out["query"] == "q=form%20d&kind=filings%2Centity&identifier=cik"
        assert out["dsHash"] == ["#/dataset/a_b", "#/dataset/a_b/rights"]

    def test_flag_reasons(self, out):
        assert [f["code"] for f in out["flagsGate"]] == ["storage_forbidden"]
        assert out["flagsGate"][0]["level"] == "error"
        assert [f["code"] for f in out["flagsState"]] == ["fabricated"]
        assert [f["code"] for f in out["flagsSeeded"]] == ["seeded"]
        assert [f["code"] for f in out["flagsStorage"]] == ["storage_forbidden"]
        assert out["flagsClean"] == [] and out["flagsUnverified"] == []

    def test_formatters(self, out):
        assert out["fresh"] == ["good", "bad", "warn", "none"]
        assert out["rows"] == ["123", "12k", "2.5M", "", ""]
        assert out["spark"] == "1.0,9.0 9.0,1.0" and out["sparkShort"] == ""
        assert out["safe"] == ["https://a.b/c", "", ""]
        assert out["curl"] == 'curl -s -H "X-API-Key: $NEXDATA_API_KEY" "http://h/api/v1/catalog/x/sample"'


# =============================================================================
# 6. jsdom smoke (skipped without node + jsdom)
# =============================================================================


def _jsdom_available() -> bool:
    if NODE is None:
        return False
    proc = subprocess.run([NODE, "-e", "require.resolve('jsdom')"], capture_output=True,
                          text=True, timeout=30, env=dict(os.environ))
    return proc.returncode == 0


@pytest.mark.unit
@pytest.mark.skipif(not _jsdom_available(), reason="node + jsdom not available (set NODE_PATH)")
@pytest.mark.parametrize("scenario", ["detail", "refused", "search"])
def test_dom_smoke(scenario):
    proc = subprocess.run([NODE, str(SMOKE), str(REPO), scenario], capture_output=True,
                          text=True, timeout=120, env=dict(os.environ))
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    assert out["ok"], proc.stdout
    assert not out["fail"], out["fail"]
