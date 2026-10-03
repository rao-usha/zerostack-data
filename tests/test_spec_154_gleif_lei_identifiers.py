"""
Tests for SPEC 154 -- LEI / UEI identifiers: GLEIF CC0 records, USAspending UEIs and EDGAR
LEIs into the gated resolver; the S&P DUNL licence conflict recorded.

All offline: the fetch loop runs through `open_web.OpenWebFetcher` on an httpx MockTransport,
the resolver through the pure `resolve_core.plan()`.
"""

import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

pytestmark = pytest.mark.unit

ACTIVE = ("2024-01-15", "2026-09-20")
L1 = "5493001KJTIIGC8Y1R12"
L2 = "254900O4TXAFSPI9ZM37"
L3 = "Y6X4K52KMJMZE7I7MY94"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _rec(record_key, **kw):
    from app.entities import norm
    from app.entities.resolve_core import _rec as base

    if kw.get("legal_name") and "name_norm" not in kw:
        kw["name_norm"] = norm.norm(kw["legal_name"])
    return base(record_key, **kw)


def _prof(name, first=ACTIVE[0], last=ACTIVE[1], **kw):
    p = {"name": name, "former_names": [], "sic": None, "first_filed": first,
         "last_filed": last, "latest_form": None, "tickers": None, "insider_owner": False,
         "insider_issuer": False, "owner_filings": 0, "formd_filings": 0,
         "formd_pooled": False, "f13_filings": 0, "k10_filings": 0}
    p.update(kw)
    return p


def _filer(cik, name, lei=None, state=None):
    pad = str(cik).zfill(10)       # a one-digit CIK is a degenerate placeholder: use real-sized ones
    return [_rec(f"edgar:{pad}", cik=pad, lei=lei, legal_name=name, state=state),
            _rec(f"formd:{pad}", cik=pad, legal_name=name, state=state)]


def _plan(records, profiles=None):
    from app.entities.resolve_core import plan

    return plan(records, [], {}, profiles=profiles or {})


def _comp_of(p, record_key):
    for i, c in enumerate(p["components"]):
        if record_key in c["members"]:
            return i
    return None


def _together(p, a, b):
    ca, cb = _comp_of(p, a), _comp_of(p, b)
    return ca is not None and ca == cb


def _api_record(lei=L2, name="CELESTIAL CODE LLC", juris="US-IN", reg_status="ISSUED", **extra):
    attrs = {
        "lei": lei,
        "entity": {
            "legalName": {"name": name, "language": "en"},
            "otherNames": [{"name": "Celestial", "type": "TRADING_OR_OPERATING_NAME"}],
            "legalAddress": {"addressLines": ["3600 Mesker Park Dr"], "city": "Evansville",
                             "region": "US-IN", "country": "US", "postalCode": "47720-1234"},
            "headquartersAddress": {"addressLines": ["1 Main St"], "city": "Chicago",
                                    "region": "US-IL", "country": "US", "postalCode": "60601"},
            "registeredAt": {"id": "RA000609", "other": None},
            "registeredAs": "202405281795066",
            "jurisdiction": juris,
            "category": "GENERAL",
            "legalForm": {"id": "DQBZ", "other": None},
            "status": "ACTIVE",
            "creationDate": "2024-05-28T00:00:00Z",
        },
        "registration": {
            "initialRegistrationDate": "2026-10-02T21:08:25Z",
            "lastUpdateDate": "2026-10-02T21:08:25Z",
            "status": reg_status,
            "nextRenewalDate": "2027-10-02T21:08:25Z",
            "managingLou": "5493001KJTIIGC8Y1R12",
            "corroborationLevel": "FULLY_CORROBORATED",
        },
    }
    attrs.update(extra)
    return {"type": "lei-records", "id": lei, "attributes": attrs}


def _page(records, next_cursor=None, publish="2026-10-03T08:00:00Z", total=3):
    links = {"first": "https://api.gleif.org/api/v1/lei-records?page%5Bcursor%5D=%2A"}
    if next_cursor:
        # GLEIF's next link drops the sparse fieldset: the client must add it back
        links["next"] = ("https://api.gleif.org/api/v1/lei-records?filter%5Bentity.legalAddress.country%5D=US"
                         f"&page%5Bcursor%5D={next_cursor}&page%5Bsize%5D=200")
    return {"meta": {"goldenCopy": {"publishDate": publish},
                     "pagination": {"perPage": 200, "total": total}},
            "links": links, "data": records}


def _fetcher(handler, sleeps=None):
    from app.core import open_web

    terms = open_web.parse_terms([{"host": "gleif.org", "verdict": "allowed",
                                   "citation": "GLEIF terms (test)", "reviewed": "2026-10-03"}])
    t = [0.0]

    def sleep(s):
        (sleeps if sleeps is not None else []).append(s)
        t[0] += s

    return open_web.OpenWebFetcher(terms, transport=httpx.MockTransport(handler),
                                   resolve_host=lambda h: ["8.8.8.8"], clock=lambda: t[0],
                                   sleep=sleep, min_interval=0.0, max_bytes=4_000_000)


def _robots_ok(req):
    return httpx.Response(200, text="User-agent: *\nDisallow:\n")


# ---------------------------------------------------------------------------
# T1-T3 rights and terms
# ---------------------------------------------------------------------------

def test_gleif_rights_entry():
    from app.catalog.rights import CC0_URL, SOURCE_RIGHTS

    r = SOURCE_RIGHTS["gleif"]
    assert "CC0" in r.license
    assert r.redistribution == "open"
    assert r.storage == "allowed" and r.commercial_use == "allowed"
    assert r.license_url == CC0_URL
    assert "gleif.org" in r.citation_url
    assert "CC0" in (r.citation_quote or "")
    assert r.proposed is None


def test_dunl_rights_conflict_recorded():
    from app.catalog.rights import SOURCE_RIGHTS

    r = SOURCE_RIGHTS["dunl"]
    assert r.redistribution == "restricted"
    assert r.proposed is None                      # "loosen if CC BY" closed: neither licence is CC BY
    assert r.share_alike is True
    assert r.commercial_use == "restricted"
    assert "BY-NC-SA" in r.notes and "BY-SA" in r.notes and "internal" in r.notes
    assert r.confidence == "high"


def test_terms_registry_entries():
    from app.core import open_web
    from app.entities.domain_probe import TERMS_PATH

    terms = open_web.load_terms(TERMS_PATH)
    assert terms["gleif.org"].verdict == "allowed"
    assert "CC0" in terms["gleif.org"].citation
    assert terms["dunl.org"].verdict == "refused"
    assert terms["spglobal.com"].verdict == "refused"


# ---------------------------------------------------------------------------
# T4-T10 connector
# ---------------------------------------------------------------------------

def test_parse_record():
    from app.sources.gleif import client

    row = client.parse_record(_api_record(spglobal=["21719"], ocid="us_ny/99979", bic=["X"]))
    assert row["lei"] == L2
    assert row["legal_name"] == "CELESTIAL CODE LLC"
    assert row["jurisdiction"] == "US-IN"
    assert row["state"] == "IN"                    # the US jurisdiction state
    assert row["legal_state"] == "IN" and row["hq_state"] == "IL"
    assert row["legal_postal"] == "47720-1234" and row["legal_city"] == "Evansville"
    assert row["registered_at"] == "RA000609" and row["registered_as"] == "202405281795066"
    assert row["registration_status"] == "ISSUED" and row["entity_status"] == "ACTIVE"
    assert row["last_update_date"].startswith("2026-10-02")
    assert not any(k in row for k in ("spglobal", "ocid", "bic"))
    # a non-US jurisdiction falls back to the legal-address state
    r2 = client.parse_record(_api_record(juris="KY"))
    assert r2["state"] == "IN"
    assert client.region_state("US-DE") == "DE"
    assert client.region_state("CA-ON") is None
    assert client.parse_record({"attributes": {"lei": "bad"}}) is None


def test_page_urls_keep_fields():
    from app.sources.gleif import client

    first = client.first_url("US")
    q = parse_qs(urlsplit(first).query)
    assert q["filter[entity.legalAddress.country]"] == ["US"]
    assert q["fields[lei-records]"] == ["lei,entity,registration"]
    assert q["page[cursor]"] == ["*"] and q["page[size]"] == ["200"]
    nxt = client.next_url(_page([], next_cursor="AoE%2FOjBO")["links"]["next"], "US")
    q2 = parse_qs(urlsplit(nxt).query)
    assert q2["page[cursor]"] == ["AoE/OjBO"]
    assert q2["fields[lei-records]"] == ["lei,entity,registration"]
    assert q2["filter[entity.legalAddress.country]"] == ["US"]
    assert client.next_url(None, "US") is None


def test_fetch_loop_follows_cursor():
    from app.core.open_web import USER_AGENT
    from app.sources.gleif import ingest

    seen = []

    def handler(req):
        seen.append(str(req.url))
        assert req.headers["user-agent"] == USER_AGENT
        if req.url.path == "/robots.txt":
            return _robots_ok(req)
        cursor = req.url.params.get("page[cursor]")
        assert req.url.params.get("fields[lei-records]") == "lei,entity,registration"
        if cursor == "*":
            return httpx.Response(200, json=_page([_api_record(L1), _api_record(L2)], next_cursor="C2"))
        assert cursor == "C2"
        return httpx.Response(200, json=_page([_api_record(L3)]))

    pages = []
    rep = ingest.collect(_fetcher(handler), country="US", on_page=pages.append, sleep=lambda s: None)
    assert seen[0].endswith("/robots.txt")
    assert rep["outcome"] == "complete"
    assert rep["pages"] == 2 and rep["records"] == 3
    assert rep["publish_date"] == "2026-10-03T08:00:00Z"
    assert rep["clock"] == "2026-10-03T08:00:00Z"
    assert [len(p) for p in pages] == [2, 1]


def test_fetch_retry_after_respected():
    from app.sources.gleif import ingest

    calls = {"n": 0}

    def handler(req):
        if req.url.path == "/robots.txt":
            return _robots_ok(req)
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json=_page([_api_record(L1)]))

    waits = []      # collect() waits on the fetcher's own clock (its Retry-After backoff lives there)
    rep = ingest.collect(_fetcher(handler, sleeps=waits), country="US", on_page=lambda rows: None)
    assert rep["outcome"] == "complete" and rep["records"] == 1
    assert 7 in [round(w) for w in waits]          # waited the server's Retry-After
    assert calls["n"] == 2


def test_fetch_robots_disallowed_no_pages():
    from app.sources.gleif import ingest

    hits = []

    def handler(req):
        hits.append(req.url.path)
        if req.url.path == "/robots.txt":
            return httpx.Response(403)              # goldencopy.gleif.org answers this
        return httpx.Response(200, json=_page([_api_record()]))

    rep = ingest.collect(_fetcher(handler), country="US", on_page=lambda rows: None, sleep=lambda s: None)
    assert rep["outcome"] == "refused"
    assert hits == ["/robots.txt"]
    assert rep["clock"] is None


def test_partial_run_keeps_clock():
    from app.sources.gleif import ingest

    def handler(req):
        if req.url.path == "/robots.txt":
            return _robots_ok(req)
        if req.url.params.get("page[cursor]") == "*":
            return httpx.Response(200, json=_page([_api_record(L1)], next_cursor="C2"))
        return httpx.Response(503)

    rep = ingest.collect(_fetcher(handler), country="US", on_page=lambda rows: None, sleep=lambda s: None)
    assert rep["outcome"] == "partial"
    assert rep["pages"] == 1 and rep["records"] == 1
    assert rep["clock"] is None
    assert rep["error"]


def test_dry_run_writes_nothing():
    from app.sources.gleif import ingest

    class DB:
        def __init__(self):
            self.calls = 0

        def execute(self, *a, **k):
            self.calls += 1

        def commit(self):
            self.calls += 1

    def handler(req):
        if req.url.path == "/robots.txt":
            return _robots_ok(req)
        return httpx.Response(200, json=_page([_api_record(L1)]))

    db = DB()
    rep = ingest.run(db, apply=False, fetcher=_fetcher(handler), sleep=lambda s: None)
    assert rep["apply"] is False and rep["records"] == 1
    assert db.calls == 0


# ---------------------------------------------------------------------------
# T11-T12 feeds
# ---------------------------------------------------------------------------

def test_feed_rows_lei_uei():
    from app.entities import feeds

    g = feeds._row(feeds.GLEIF, {"native_id": L1.lower(), "legal_name": "Alpha LLC", "lei": L1.lower(),
                                 "state": "DE", "observed_at": None})
    cols = dict(zip(feeds.COLUMN_NAMES, g))
    assert cols["record_key"] == f"gleif:{L1}" and cols["lei"] == L1 and cols["state"] == "DE"
    u = feeds._row(feeds.USASP, {"native_id": "abc123def456", "legal_name": "Beta Inc",
                                 "uei": "abc123def456", "observed_at": None})
    ucols = dict(zip(feeds.COLUMN_NAMES, u))
    assert ucols["record_key"] == "usasp:ABC123DEF456" and ucols["uei"] == "ABC123DEF456"
    assert ucols["lei"] is None
    e = feeds._row(feeds.EDGAR, {"native_id": "0000000001", "cik": "0000000001",
                                 "legal_name": "Gamma", "lei": L2})
    assert dict(zip(feeds.COLUMN_NAMES, e))["lei"] == L2
    assert "lei" in feeds.EDGAR.sql and "lei" in feeds.EDGAR_WITH_SPONSORS.sql
    # an invalid LEI / UEI native id feeds nothing
    assert feeds._row(feeds.GLEIF, {"native_id": "nope", "lei": "nope"}) is None
    assert feeds._row(feeds.USASP, {"native_id": "short", "uei": "short"}) is None


def test_feed_sql_excludes_invalid():
    from app.entities import feeds

    assert "DUPLICATE" in feeds.GLEIF.sql and "ANNULLED" in feeds.GLEIF.sql
    assert feeds.GLEIF.requires == "public.gleif_lei_record"
    assert feeds.USASP.requires == "public.usaspending_awards"


# ---------------------------------------------------------------------------
# T13-T17 the gate
# ---------------------------------------------------------------------------

def test_lei_shared_by_two_ciks_gated():
    from app.entities.resolve_core import GATED_KEY_TYPES

    assert "lei" in GATED_KEY_TYPES and "uei" in GATED_KEY_TYPES
    recs = _filer(1507673, "Alpha Holdings Inc", lei=L1) + _filer(2077508, "Beta Capital LLC", lei=L1)
    profs = {"1507673": _prof("Alpha Holdings Inc"), "2077508": _prof("Beta Capital LLC")}
    p = _plan(recs, profs)
    assert not _together(p, "edgar:0001507673", "edgar:0002077508")
    assert p["metrics"]["gate"]["pairs_considered"] == 1
    assert p["metrics"]["gate"]["pairs_joined"] == 0
    blocks = [c.get("gate") or {} for c in p["components"]]
    assert any(r["keys"] == [f"lei:{L1}"] for b in blocks for r in b.get("refused", []))

    same = _filer(1507673, "Alpha Holdings Inc", lei=L1) + _filer(2077508, "Alpha Holdings Inc", lei=L1)
    p2 = _plan(same, {"1507673": _prof("Alpha Holdings Inc"), "2077508": _prof("Alpha Holdings Inc")})
    assert _together(p2, "edgar:0001507673", "edgar:0002077508")
    assert p2["metrics"]["gate"]["joined_by_rule"] == {"R1_name_equal": 1}


def test_gleif_record_attaches_to_cik():
    recs = _filer(1469475, "Gamma Partners LLC", lei=L3) + [
        _rec(f"gleif:{L3}", lei=L3, legal_name="GAMMA PARTNERS LLC", state="DE")]
    p = _plan(recs)
    assert _together(p, "edgar:0001469475", f"gleif:{L3}")
    comp = p["components"][_comp_of(p, f"gleif:{L3}")]
    assert ("lei", L3) in comp["keys"]
    assert p["metrics"]["gate"]["anchor_attach"].get("only_class") == 1


def test_no_cik_records_share_lei_join():
    recs = [_rec(f"gleif:{L1}", lei=L1, legal_name="Delta LLC"),
            _rec(f"other:{L1}", lei=L1, legal_name="Delta LLC")]
    p = _plan(recs)
    assert _together(p, f"gleif:{L1}", f"other:{L1}")


def test_uei_only_singleton():
    recs = _filer(846788, "Epsilon Inc") + [_rec("usasp:ABC123DEF456", uei="ABC123DEF456",
                                           legal_name="Epsilon Inc")]
    p = _plan(recs)
    assert _comp_of(p, "usasp:ABC123DEF456") is None
    assert p["metrics"]["singleton_keyed"] >= 1


def test_contested_lei_single_owner():
    recs = (_filer(1507673, "Alpha Holdings Inc", lei=L1) + _filer(2077508, "Beta Capital LLC", lei=L1)
            + [_rec(f"gleif:{L1}", lei=L1, legal_name="Alpha Holdings Inc")])
    profs = {"1507673": _prof("Alpha Holdings Inc"), "2077508": _prof("Beta Capital LLC")}
    p = _plan(recs, profs)
    a, b = _comp_of(p, "edgar:0001507673"), _comp_of(p, "edgar:0002077508")
    assert a is not None and b is not None and a != b
    assert _together(p, "edgar:0001507673", f"gleif:{L1}")       # rule 5 name match
    holders = [i for i, c in enumerate(p["components"]) if ("lei", L1) in c["keys"]]
    assert holders == [a]                                         # one owner: the anchor's piece
    assert ("lei", L1) in p["components"][b]["withheld"]


# ---------------------------------------------------------------------------
# T18 weak tier
# ---------------------------------------------------------------------------

def test_weak_gleif_candidates():
    from app.entities.resolve_core import WEAK_LEI_SOURCES, weak_name_state

    base = (_filer(1710524, "Gamma Partners LLC", lei=L2, state="DE")
            + _filer(1893134, "Delta Co", state="NY")
            + [_rec("dol5500:261640968", ein="261640968", legal_name="Delta Co", state="NY")])
    gleif = [_rec(f"gleif:{L1}", lei=L1, legal_name="Gamma Partners LLC", state="DE"),
             _rec(f"gleif:{L3}", lei=L3, legal_name="Delta Co", state="NY")]
    p0 = _plan(base)
    p1 = _plan(base + gleif)
    w0 = weak_name_state(base, p0["record_keys"], p0["components"])
    w1 = weak_name_state(base + gleif, p1["record_keys"], p1["components"])
    assert w0["rows"] == w1["rows"]                     # dol5500 rows unchanged by GLEIF records
    assert all(r["candidate_record_key"].split(":")[0] != "gleif" for r in w1["rows"])

    wg = weak_name_state(base + gleif, p1["record_keys"], p1["components"], sources=WEAK_LEI_SOURCES)
    by = {}
    for r in wg["rows"]:
        by.setdefault(r["record_key"], []).append(r)
    gamma = by[f"gleif:{L1}"]
    assert {r["status"] for r in gamma} == {"conflict"}
    assert gamma[0]["conflicts"][0]["reason"] == "lei_conflict"
    assert gamma[0]["conflicts"][0]["candidate_leis"] == [L2]
    delta = by[f"gleif:{L3}"]
    assert {r["status"] for r in delta} == {"candidate"}
    assert all(r["candidate_record_key"].split(":")[0] != "dol5500" for r in delta)


# ---------------------------------------------------------------------------
# T19 catalog / dispatch
# ---------------------------------------------------------------------------

def test_catalog_and_dispatch():
    from app.api.v1.jobs import SOURCE_DISPATCH
    from app.catalog.registry import get_catalog

    specs = {s.key: s for s in get_catalog()}
    g = specs["gleif_lei_records"]
    assert g.source == "gleif"
    assert "gleif_lei_record" in g.tables
    assert "golden_copy_publish_date" in (g.coverage_sql or "") and "complete" in g.coverage_sql
    ins = specs["entity_source_records"].inputs
    assert "gleif_lei_records" in ins and "usaspending_awards" in ins
    assert SOURCE_DISPATCH["gleif"][0] == "app.sources.gleif.ingest"
    json.dumps(SOURCE_DISPATCH["gleif"])


def test_apply_upserts_one_statement_per_page():
    """T10b: --apply writes one parameterized multi-row INSERT per page (row-by-row executemany
    was ~10 s per page through the Cloud SQL proxy), duplicates inside a page collapsed."""
    from app.sources.gleif import ingest

    class R:
        def scalar(self):
            return 42

    class DB:
        def __init__(self):
            self.stmts = []

        def execute(self, stmt, params=None):
            self.stmts.append((str(stmt), params))
            return R()

        def commit(self):
            pass

        def rollback(self):
            pass

    def handler(req):
        if req.url.path == "/robots.txt":
            return _robots_ok(req)
        return httpx.Response(200, json=_page([_api_record(L1), _api_record(L1), _api_record(L2)]))

    db = DB()
    rep = ingest.run(db, apply=True, fetcher=_fetcher(handler), sleep=lambda s: None)
    inserts = [(s, p) for s, p in db.stmts if s.startswith("INSERT INTO gleif_lei_record")]
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert "ON CONFLICT (lei) DO UPDATE" in sql and L1 not in sql      # values are bound, not inlined
    assert sorted(v for k, v in params.items() if k.startswith("lei_")) == sorted([L1, L2])
    assert all(v == 42 for k, v in params.items() if k.startswith("last_seen_run_id_"))
    assert rep["run_id"] == 42 and rep["outcome"] == "complete"
    assert any(s.startswith("UPDATE gleif_fetch") for s, _p in db.stmts)


# ---------------------------------------------------------------------------
# T20-T22 adversarial review (2026-10-03): LEI checksum, name-gated LEI attachment
# ---------------------------------------------------------------------------

def test_lei_checksum_enforced():
    """T20: an LEI must pass ISO 17442's MOD 97-10 check. MEASURED live: 16 sec_filers LEIs of
    20 alphanumerics fail it (a trust's name, registry numbers, typos of real LEIs)."""
    from app.entities.resolve_core import _lei

    assert _lei(" 5493001kjtiigc8y1r12 ") == L1
    for bad in ("WUWALLACEFAMILYTRUST", "00000000000540342441", "549300UVUTOXK7EQZT63",
                "5493001KJTIIGC8Y1R13"):
        assert _lei(bad) is None, bad


def test_gleif_attach_needs_name_when_lei_is_the_only_link():
    """T21: a filer that typed another firm's LEI (22C Capital LLC carries Bloomberg Finance
    L.P.'s LEI, live) must not absorb that firm's GLEIF record: the LEI is self-reported on the
    EDGAR side, so a lone LEI link attaches only when the names agree. The LEI then belongs to
    the GLEIF record (anchor owner) and is withheld from the filer."""
    recs = _filer(2075659, "22C Capital LLC", lei=L1) + [
        _rec(f"gleif:{L1}", lei=L1, legal_name="Bloomberg Finance L.P.", state="DE")]
    p = _plan(recs)
    assert not _together(p, "edgar:0002075659", f"gleif:{L1}")
    comp = p["components"][_comp_of(p, "edgar:0002075659")]
    assert ("lei", L1) not in comp["keys"] and ("lei", L1) in comp["withheld"]
    assert p["metrics"]["gate"]["anchor_attach"].get("own_entity_name_mismatch") == 1
    assert p["metrics"]["gate"]["anchor_attach"].get("only_class") is None


def test_gleif_attach_on_former_name():
    """T22: a renamed filer still attaches on a former EDGAR name, and gated_ciks asks for its
    profile so the former names are known."""
    from app.entities.resolve_core import gated_ciks

    recs = _filer(2056320, "Rockbridge Asset Management LLC", lei=L3) + [
        _rec(f"gleif:{L3}", lei=L3, legal_name="Rockbridge Capital Management, LLC", state="VA")]
    assert "2056320" in gated_ciks(recs, [], {})
    profs = {"2056320": _prof("Rockbridge Asset Management LLC",
                              former_names=["Rockbridge Capital Management, LLC"])}
    assert _together(_plan(recs, profs), "edgar:0002056320", f"gleif:{L3}")
    assert not _together(_plan(recs), "edgar:0002056320", f"gleif:{L3}")   # no profile: no rename


def test_gleif_attach_on_squashed_name():
    """T23: spacing alone does not split a lone-LEI link (GLEIF 'THE GOOD EAR COMPANY, INC.' vs
    EDGAR 'TheGoodEarCompany, Inc.', a b2b / targets row live)."""
    recs = _filer(2091693, "TheGoodEarCompany, Inc.", lei=L2) + [
        _rec(f"gleif:{L2}", lei=L2, legal_name="THE GOOD EAR COMPANY, INC.", state="DE")]
    assert _together(_plan(recs), "edgar:0002091693", f"gleif:{L2}")
