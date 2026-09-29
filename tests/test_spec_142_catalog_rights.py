"""
Tests for SPEC 142 — rights proposals with citations, human review workflow and report.

`redistribution` could only gate export: it could not say "the terms forbid holding
this" (Yelp, FRED as retrieved, M5, Kalshi, NZA, PeeringDB, LoopNet) or "commercial
use needs an agreement" (AMA CPT, IMF, CourtListener). Rights carried no citation and
`reviewed` was a hand-set boolean. Now: cited tightenings are applied, loosenings are
proposals only, a review is an append-only audit row pinned to a rights hash, and
`reviewed` comes only from a committed hash that must match the current block.
"""
import dataclasses
import importlib.util
import json
import os
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]
BASELINE = REPO / "tests" / "fixtures" / "spec_142_rights_baseline.json"
EVIDENCE = REPO / "app" / "catalog" / "evidence" / "rights_research_2026-09-25.json"
PLAN_EVIDENCE = REPO / "docs" / "plans" / "PLAN_088_evidence.json"

# keys whose declared redistribution may be looser than the pre-SPEC_142 baseline, each
# with a recorded human review. Empty: SPEC_142 applies tightenings only.
LOOSENED_WITH_REVIEW: set = set()
# datasets added after the baseline snapshot (each split out of a baseline dataset; the
# fix-round tests pin their rights)
ADDED_AFTER_BASELINE = {"zip_medspa_scores"}

STORAGE_FLAGGED = {"yelp_businesses", "fred_series", "kaggle_m5", "prediction_markets", "si_zoning_districts",
                   "si_internet_exchanges", "si_warehouse_listings", "medspa_prospects"}


def _catalog():
    from app.catalog import get_catalog

    return get_catalog()


def _spec(key):
    from app.catalog import get_spec

    s = get_spec(key)
    assert s is not None, key
    return s


# a dataset with no committed sign-off (FDIC was held in rights review batch 1), used
# where a test needs "a dataset that is not reviewed"
UNREVIEWED_KEY = "fdic_institutions"


def _in_force(specs=None):
    """Keys whose committed REVIEWED hash equals the spec's current rights_hash."""
    from app.catalog.rights_reviewed import REVIEWED

    by_key = {s.key: s for s in (specs if specs is not None else _catalog())}
    return {k for k, (h, _) in REVIEWED.items() if k in by_key and by_key[k].rights_hash == h}


def _base(**over):
    kw = dict(
        key="t142_ds", source="sec", display_name="T142 dataset",
        description="A test dataset used by the SPEC_142 rights tests, long enough to pass.",
        kind="reference", grain="one row per thing", producer="api:t142", cadence="daily",
        rerun="idempotent", license="public domain", redistribution="open", pii_class="none",
        origin="official", status_public="internal", tables=("t142_rows",),
    )
    kw.update(over)
    return kw


def _cite(**over):
    kw = dict(citation_url="https://example.org/terms", citation_quote="You may copy it.", confidence="high")
    kw.update(over)
    return kw


def _migration():
    path = REPO / "alembic" / "versions" / "0015_catalog_rights_review.py"
    spec = importlib.util.spec_from_file_location("mig142", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# T1 vocabularies
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVocabularies:
    @pytest.mark.parametrize("over, fragment", [
        ({"storage": "sometimes"}, "storage"),
        ({"storage": "time_limited"}, "storage_max_age_days"),
        ({"storage": "time_limited", "storage_max_age_days": 0}, "storage_max_age_days"),
        ({"storage": "forbidden", "storage_max_age_days": 30}, "storage_max_age_days"),
        ({"commercial_use": "ok"}, "commercial_use"),
        ({"rights_confidence": "sure"}, "confidence"),
        ({"citation_quote": "quoted"}, "citation_url"),
        ({"citation_url": "example.org/terms"}, "citation_url"),
        ({"license_url": "ftp://x"}, "license_url"),
        ({"share_alike": "yes"}, "share_alike"),
    ])
    def test_dataset_spec_rejects(self, over, fragment):
        from app.catalog.spec import DatasetSpec

        with pytest.raises(ValueError, match=fragment):
            DatasetSpec(**_base(**over))

    def test_source_rights_rejects(self):
        from app.catalog.rights import SourceRights

        with pytest.raises(ValueError, match="storage"):
            SourceRights("x", "open", "none", "official", storage="nope")
        with pytest.raises(ValueError, match="REVIEWED"):
            SourceRights("x", "open", "none", "official", reviewed=True)
        with pytest.raises(ValueError, match="citation_url"):
            SourceRights("x", "open", "none", "official", confidence="high")

    def test_proposal_needs_citation_and_valid_values(self):
        from app.catalog.spec import RightsProposal

        with pytest.raises(ValueError, match="citation"):
            RightsProposal("loosen", "because", "https://example.org", None, "high")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="change"):
            RightsProposal("widen", "because", **_cite())
        with pytest.raises(ValueError, match="reason"):
            RightsProposal("loosen", " ", **_cite())
        with pytest.raises(ValueError, match="redistribution"):
            RightsProposal("loosen", "because", redistribution="public", **_cite())
        p = RightsProposal("loosen", "because", redistribution="attribution", share_alike=True, **_cite())
        assert p.changes() == {"redistribution": "attribution", "share_alike": True}

    def test_ga_needs_ungated_rights(self):
        from app.catalog.spec import DatasetSpec

        ok = dict(status_public="ga", reviewed=True, data_state="ok")
        DatasetSpec(**_base(**ok))
        with pytest.raises(ValueError, match="storing"):
            DatasetSpec(**_base(storage="forbidden", **ok))
        with pytest.raises(ValueError, match="agreement"):
            DatasetSpec(**_base(commercial_use="agreement_required", **ok))
        with pytest.raises(ValueError, match="reviewed"):
            DatasetSpec(**_base(status_public="ga", data_state="ok"))


# ---------------------------------------------------------------------------
# T2 tightenings applied, with citations; evidence file
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTightenings:
    def test_evidence_file_is_the_lens_data(self):
        lens = json.loads(PLAN_EVIDENCE.read_text(encoding="utf-8"))["lenses"]["lens:rights"]["data"]
        ev = json.loads(EVIDENCE.read_text(encoding="utf-8"))
        assert ev["rows"] == json.loads(lens)
        for row in ev["rows"]:
            assert {"source", "current", "proposed", "citation_url", "quote", "confidence"} <= set(row)

    def test_cms_utilization_ama_cpt(self):
        s = _spec("cms_medicare_utilization")
        assert s.redistribution == "restricted" and s.commercial_use == "agreement_required"
        assert "American Medical Association" in s.license and "CPT" in s.attribution
        assert s.citation_url.startswith("https://data.cms.gov/") and s.rights_confidence == "high"
        assert s.pii_class == "personal"

    def test_imf_bis_courtlistener(self):
        imf, bis, cl = _spec("intl_imf"), _spec("intl_bis"), _spec("courtlistener_dockets")
        assert imf.redistribution == "restricted" and imf.commercial_use == "agreement_required"
        assert "copyright@imf.org" in imf.citation_quote
        assert bis.redistribution == "attribution" and bis.commercial_use == "restricted"
        assert "additional charge" in bis.citation_quote
        assert cl.redistribution == "restricted" and cl.commercial_use == "agreement_required"
        assert "commercial agreement" in cl.citation_quote

    @pytest.mark.parametrize("key, storage, commercial", [
        ("yelp_businesses", "forbidden", "restricted"),
        ("medspa_prospects", "forbidden", "restricted"),
        ("vertical_prospects", "forbidden", "restricted"),
        ("fred_series", "forbidden", "restricted"),
        ("kaggle_m5", "forbidden", "forbidden"),
        ("prediction_markets", "forbidden", "forbidden"),
        ("si_zoning_districts", "forbidden", "forbidden"),
        ("si_internet_exchanges", "forbidden", "forbidden"),
        ("si_warehouse_listings", "forbidden", "forbidden"),
    ])
    def test_storage_forbidden(self, key, storage, commercial):
        s = _spec(key)
        assert (s.storage, s.commercial_use) == (storage, commercial), key
        assert s.redistribution == "restricted" and s.citation_url and s.citation_quote, key
        assert "storage_forbidden" in s.rights_gate

    def test_google_places_time_limited(self):
        for s in _catalog():
            if s.source == "foot_traffic":
                assert s.storage == "time_limited" and s.storage_max_age_days == 30, s.key
                assert "30 days" in s.citation_quote

    def test_nrel_license_wording(self):
        for key in ("afdc_ev_stations", "si_renewable_resources"):
            s = _spec(key)
            assert "§105" not in s.license and "NREL" in s.license and "credit" in s.license, key
            assert s.redistribution == "attribution" and s.citation_url == "https://developer.nrel.gov/terms/"

    def test_mixed_source_datasets(self):
        ur = _spec("si_utility_rates")
        assert "EIA" in ur.attribution or "Energy Information" in ur.attribution
        assert "36%" in ur.rights_notes
        zd = _spec("si_zoning_districts")
        assert "nj_sussex_county_gis" in zd.rights_notes and zd.storage == "forbidden"

    def test_mandated_notices(self):
        assert "not endorsed or certified by the Federal Reserve Bank of St. Louis" in _spec("fred_series").attribution
        assert "not endorsed by FEMA" in _spec("fema_disaster_declarations").attribution
        assert "not endorsed or certified by the Census Bureau" in _spec("census_acs5").attribution
        assert "adaptation of an original work by the OECD" in _spec("intl_oecd").attribution
        assert "Epoch AI" in _spec("si_frontier_datacenters").attribution
        osm = _spec("realestate_osm_buildings")
        assert "OpenStreetMap contributors" in osm.attribution and osm.share_alike
        assert "terminal.freightos.com" in _spec("si_freightos_fbx").attribution
        assert _spec("si_freightos_fbx").commercial_use == "restricted"

    def test_every_citation_is_complete(self):
        from app.catalog.rights import COLLECTOR_RIGHTS, DATASET_RIGHTS, SOURCE_RIGHTS

        for table in (SOURCE_RIGHTS, COLLECTOR_RIGHTS, DATASET_RIGHTS):
            for name, r in table.items():
                if r.citation_quote is not None or r.confidence is not None:
                    assert r.citation_url, name
                if r.storage in ("forbidden", "time_limited") or r.commercial_use in ("forbidden",
                                                                                     "agreement_required"):
                    assert r.citation_url and r.citation_quote and r.confidence, name


# ---------------------------------------------------------------------------
# T3 monotonicity
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMonotonicity:
    def test_redistribution_never_looser_than_baseline(self):
        from app.catalog.mirror import PII_RANK, REDISTRIBUTION_RANK

        base = json.loads(BASELINE.read_text(encoding="utf-8"))["datasets"]
        assert not LOOSENED_WITH_REVIEW
        for s in _catalog():
            b = base.get(s.key)
            if b is None:
                continue  # a dataset added after the baseline
            if s.key not in LOOSENED_WITH_REVIEW:
                assert REDISTRIBUTION_RANK.index(s.redistribution) >= \
                    REDISTRIBUTION_RANK.index(b["redistribution"]), (s.key, b["redistribution"], s.redistribution)
            assert PII_RANK.index(s.pii_class) >= PII_RANK.index(b["pii_class"]), s.key

    def test_baseline_covers_the_catalog(self):
        base = json.loads(BASELINE.read_text(encoding="utf-8"))["datasets"]
        assert {s.key for s in _catalog()} <= set(base) | ADDED_AFTER_BASELINE


# ---------------------------------------------------------------------------
# T4 proposals are shown, never applied
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProposals:
    def test_expected_proposals(self):
        props = {s.key: s.proposed_rights for s in _catalog() if s.proposed_rights}
        assert {"realestate_osm_buildings", "fred_series", "dunl_reference", "intl_imf"} <= set(props)
        osm = props["realestate_osm_buildings"]
        assert osm.change == "loosen" and osm.redistribution == "attribution" and osm.share_alike
        assert props["fred_series"].redistribution == "open"

    def test_proposals_not_applied(self):
        assert _spec("realestate_osm_buildings").redistribution == "restricted"
        assert _spec("fred_series").redistribution == "restricted" and _spec("fred_series").storage == "forbidden"
        assert _spec("dunl_reference").redistribution == "restricted"
        assert _spec("intl_imf").redistribution == "restricted"

    def test_every_proposal_is_cited_and_changes_something(self):
        for s in _catalog():
            p = s.proposed_rights
            if p is None:
                continue
            assert p.citation_url and p.citation_quote and p.confidence and p.reason, s.key
            assert s.proposed_block() != s.rights_block(), s.key


# ---------------------------------------------------------------------------
# T5 reviewed only from a matching committed hash
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReviewedHash:
    def test_reviewed_set_is_the_committed_hash_set(self):
        cat = _catalog()
        in_force = _in_force(cat)
        assert {s.key for s in cat if s.reviewed} == in_force
        for s in cat:
            assert s.status_public not in ("ga", "beta"), s.key
            if s.key in in_force:
                assert s.effective_redistribution == s.redistribution, s.key
            else:
                assert s.effective_redistribution == "internal_only", s.key
        assert UNREVIEWED_KEY not in in_force

    def test_every_committed_hash_matches_current_block(self):
        """A stale committed hash silently drops a sign-off (the dataset goes back to
        unreviewed). Fail loudly instead: the rights block changed since review."""
        from app.catalog.rights_reviewed import REVIEWED

        by_key = {s.key: s for s in _catalog()}
        unknown = sorted(set(REVIEWED) - set(by_key))
        assert not unknown, f"REVIEWED names datasets not in the catalog: {unknown}"
        stale = sorted(k for k, (h, _) in REVIEWED.items() if by_key[k].rights_hash != h)
        assert not stale, (
            f"rights block changed after sign-off for {stale}: the committed hash no longer matches, "
            "so the sign-off is not in force. Re-review the new block (POST "
            "/api/v1/catalog/rights/{key}/review) and re-emit app/catalog/rights_reviewed.py "
            "(python -m app.catalog.rights_review --emit), or revert the rights change.")

    def test_reviewed_file_is_what_emit_renders(self):
        from app.catalog import rights_reviewed
        from app.catalog.rights_review import render_reviewed_module

        path = Path(rights_reviewed.__file__)
        assert path.read_text(encoding="utf-8") == render_reviewed_module(rights_reviewed.REVIEWED)

    def test_matching_hash_reviews_and_any_change_reopens(self, monkeypatch):
        from app.catalog import rights_reviewed
        from app.catalog.spec import DatasetSpec

        spec = DatasetSpec(**_base())
        monkeypatch.setitem(rights_reviewed.REVIEWED, "t142_ds", (spec.rights_hash, 1))
        assert rights_reviewed.apply_review(spec).reviewed
        assert rights_reviewed.apply_review(spec).effective_redistribution == "open"
        for change in ({"attribution": "Source: X"}, {"storage": "allowed"}, {"commercial_use": "allowed"},
                       {"share_alike": True}, {"pii_class": "business_contact"}, {"license": "other"},
                       {"license_url": "https://example.org/l"}, {"origin": "derived"},
                       {"redistribution": "attribution"}):
            changed = DatasetSpec(**_base(**change))
            assert changed.rights_hash != spec.rights_hash, change
            assert not rights_reviewed.apply_review(changed).reviewed, change
        # citations and notes document, they do not decide
        cited = DatasetSpec(**_base(rights_notes="n", **{k if k != "confidence" else "rights_confidence": v
                                                          for k, v in _cite().items()}))
        assert cited.rights_hash == spec.rights_hash
        # a hand-set reviewed=True with no committed hash is cleared
        assert not rights_reviewed.apply_review(DatasetSpec(**_base(key="t142_other", reviewed=True))).reviewed

    def test_catalog_build_goes_through_apply_review(self, monkeypatch):
        from app.catalog import datasets, rights_reviewed

        s = _spec("bls_series")
        monkeypatch.setitem(rights_reviewed.REVIEWED, "bls_series", (s.rights_hash, 7))
        rebuilt = datasets._ds("bls_series", s.source, s.display_name, s.description, s.kind, s.grain,
                               s.producer, s.cadence, tables=s.tables, verified_at=s.verified_at,
                               data_state=s.data_state)
        assert rebuilt.reviewed


# ---------------------------------------------------------------------------
# T6 to_dict
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_to_dict_rights_block():
    r = _spec("realestate_osm_buildings").to_dict()["rights"]
    for f in ("storage", "storage_max_age_days", "commercial_use", "share_alike", "license_url", "citation_url",
              "citation_quote", "confidence", "notes", "gate", "rights_hash", "proposed"):
        assert f in r, f
    assert r["share_alike"] is True and r["proposed"]["changes"]["redistribution"] == "attribution"
    assert r["reviewed"] is False and r["effective_redistribution"] == "internal_only"
    y = _spec("yelp_businesses").to_dict()["rights"]
    assert y["gate"] == ["storage_forbidden"] and y["storage"] == "forbidden"


# ---------------------------------------------------------------------------
# T7 queue, T8 report (offline)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestQueueAndReport:
    def test_queue_sections(self):
        from app.catalog.dictionary import PE_ENTITY_PACK
        from app.catalog.rights_review import build_queue

        q = build_queue(None, live=False)
        cands = [i["key"] for i in q["candidates"]]
        for key in ("intl_worldbank", "intl_oecd", "si_frontier_datacenters", "si_utility_rates",
                    "realestate_fhfa_hpi"):
            assert key in cands, key
        # signed off and in force: out of every pending section
        in_force = _in_force()
        assert {"sec_13f", "sec_companyfacts", "treasury_daily_balance", "bls_series"} <= in_force
        pending = {i["key"] for sec in ("candidates", "proposals", "pii_blocked", "awaiting_commit", "stale")
                   for i in q[sec]}
        assert not in_force & pending
        # PE / entity pack first. The PE-pack candidates (SEC) are signed off now, so check
        # the ordering on the same catalog with every sign-off cleared.
        unsigned = [dataclasses.replace(s, reviewed=False) for s in _catalog()]
        all_cands = [i["key"] for i in build_queue(None, live=False, specs=unsigned)["candidates"]]
        pe = [k for k in all_cands if k in PE_ENTITY_PACK]
        assert all_cands[:len(pe)] == pe and pe
        assert [k for k in all_cands if k not in in_force] == cands
        # personal data waits on the PII policy; gated data is never a candidate
        blocked = {i["key"] for i in q["pii_blocked"]}
        assert {"nppes_providers", "sec_insider", "sec_form_d"} <= blocked
        assert not blocked & set(cands)
        assert "cms_medicare_utilization" not in cands and "intl_imf" not in cands
        flags = {i["key"]: i for i in q["storage_flags"]}
        assert STORAGE_FLAGGED <= set(flags)
        assert any(s.source == "foot_traffic" for s in _catalog() if s.key in flags)
        assert flags["yelp_businesses"]["holdings"]["measured"] is False
        assert flags["kaggle_m5"]["gating_effect"]["sample_non_admin"] == "refused (403)"
        props = {i["key"]: i for i in q["proposals"]}
        assert props["realestate_osm_buildings"]["diff"]["redistribution"] == ["restricted", "attribution"]
        assert q["counts"]["awaiting_commit"] == 0 and q["review_table"] is False

    def test_report_offline_json_and_md(self):
        from app.catalog.rights_review import build_report, render_markdown

        rep = build_report(None, live=False)
        sm = rep["summary"]
        assert sm["datasets"] == len(_catalog()) and sm["reviewed"] == len(_in_force()) > 0
        assert sm["gated"] >= len(STORAGE_FLAGGED) and sm["proposals"] >= 4
        assert rep["live"] is False
        keys = {h["key"] for h in rep["storage_holdings"]}
        assert STORAGE_FLAGGED <= keys
        assert all(h["rows_total"] is None for h in rep["storage_holdings"])
        fams = rep["families"]
        assert any(r["key"] == "fred_series" and r["proposed"] for r in fams["fred"])
        md = render_markdown(rep)
        assert md.startswith("# Catalog rights report")
        assert "yelp_businesses" in md and "realestate_osm_buildings" in md and "n/a" in md


# ---------------------------------------------------------------------------
# T9 enforcement
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnforcement:
    def test_schema_live_gates(self):
        from app.catalog.schema_live import rights_gate, storage_forbidden

        assert storage_forbidden([_spec("yelp_businesses")])
        assert not storage_forbidden([_spec("cms_medicare_utilization")])
        assert rights_gate([_spec("cms_medicare_utilization")]) == ["agreement_required"]
        assert rights_gate([_spec("sec_13f"), _spec("kaggle_m5")]) == ["storage_forbidden",
                                                                       "commercial_use_forbidden"]
        assert rights_gate([_spec("sec_13f")]) == []

    def test_export_policy(self):
        from app.core.export_policy import catalog_rights_gate, export_allowed

        gate = catalog_rights_gate("yelp_businesses")  # pattern yelp_businesses*
        assert gate and "storage_forbidden" in gate["reasons"] and "yelp_businesses" in gate["datasets"]
        assert catalog_rights_gate("m5_items")["reasons"] == ["storage_forbidden", "commercial_use_forbidden"]
        assert catalog_rights_gate("cms_medicare_utilization")["reasons"] == ["agreement_required"]
        assert not export_allowed("yelp_businesses", ["id", "name"], admin=False)
        assert export_allowed("yelp_businesses", ["id", "name"], admin=True)
        assert catalog_rights_gate("sec_13f_holdings") is None
        assert export_allowed("sec_13f_holdings", ["cik"], admin=False)
        assert not export_allowed("users", ["id"], admin=True)

    def test_mirror_merges_terms(self):
        from app.catalog.mirror import merge_rights

        m = merge_rights([_spec("sec_13f"), _spec("kaggle_m5")])
        assert m["storage"] == "forbidden" and m["commercial_use"] == "forbidden"
        assert m["rights_gate"] == ["storage_forbidden", "commercial_use_forbidden"]
        ft = next(s for s in _catalog() if s.source == "foot_traffic")
        m2 = merge_rights([ft])
        assert m2["storage"] == "time_limited" and m2["storage_max_age_days"] == 30
        # rights review batch 1 assessed sec_13f (storage allowed, cited); FDIC is held, not assessed
        assert merge_rights([_spec("sec_13f")])["storage"] == "allowed"
        assert merge_rights([_spec("fdic_institutions")])["storage"] is None


# ---------------------------------------------------------------------------
# T10 migration
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMigration0015:
    def test_revision_chain(self):
        mod = _migration()
        assert mod.revision == "0015_catalog_rights_review"
        assert mod.down_revision == "0014_dataset_status"
        ddl = " ".join(mod.UPGRADE_SQL)
        assert "CREATE TABLE IF NOT EXISTS catalog_rights_review" in ddl
        assert "BEFORE UPDATE OR DELETE" in ddl and "BEFORE TRUNCATE" in ddl

    def test_single_head(self):
        parents = [p.name for p in (REPO / "alembic" / "versions").glob("*.py")
                   if '"0014_dataset_status"' in p.read_text(encoding="utf-8").split("down_revision", 1)[-1][:60]]
        assert parents == ["0015_catalog_rights_review.py"]

    def test_route_table(self):
        from app.main import app

        paths = {(r.path, m) for r in app.routes for m in getattr(r, "methods", ()) or ()}
        for p, m in (("/api/v1/catalog/rights/review", "GET"), ("/api/v1/catalog/rights/{key}/review", "POST"),
                     ("/api/v1/catalog/rights/report", "GET"), ("/api/v1/catalog/rights/{key}", "GET")):
            assert (p, m) in paths, (p, m)
        order = [r.path for r in app.routes]
        assert order.index("/api/v1/catalog/rights/report") < order.index("/api/v1/catalog/{key}")


# ---------------------------------------------------------------------------
# PG: migration, review workflow, sample gate
# ---------------------------------------------------------------------------


@pytest.fixture
def pg142():
    from sqlalchemy import create_engine, text

    from app.catalog.rights_review import clear_holdings_cache

    engine = create_engine(PG_URL)
    mod = _migration()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS catalog_rights_review"))
    for _ in range(2):  # idempotent
        with engine.begin() as conn:
            for stmt in mod.UPGRADE_SQL:
                conn.execute(text(stmt))
    clear_holdings_cache()
    yield engine
    clear_holdings_cache()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS catalog_rights_review"))
        conn.execute(text("DROP TABLE IF EXISTS yelp_businesses_t142"))
    engine.dispose()


def _client(engine, principal=None):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import sessionmaker

    from app.api.v1 import catalog_rights
    from app.core.authz import current_principal
    from app.core.database import get_db

    factory = sessionmaker(bind=engine)

    def _db():
        db = factory()
        try:
            yield db
        finally:
            db.close()

    app = FastAPI()
    app.include_router(catalog_rights.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[current_principal] = lambda: principal or {
        "role": "admin", "email": "reviewer@nexdata.test", "user_id": 42}
    return TestClient(app)


@pg
class TestReviewWorkflowPg:
    def test_append_only(self, pg142):
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        with pg142.begin() as conn:
            conn.execute(text(
                "INSERT INTO catalog_rights_review (dataset_key, decision, rights_hash, rights_snapshot, "
                "reviewer, note) VALUES ('bls_series', 'reject', repeat('a', 64), '{}'::jsonb, 'x', "
                "'a long enough note')"))
        for stmt in ("UPDATE catalog_rights_review SET note = 'changed note here'",
                     "DELETE FROM catalog_rights_review", "TRUNCATE catalog_rights_review"):
            with pytest.raises(DBAPIError, match="append-only"):
                with pg142.begin() as conn:
                    conn.execute(text(stmt))
        with pytest.raises(DBAPIError):
            with pg142.begin() as conn:  # short note refused by the table too
                conn.execute(text(
                    "INSERT INTO catalog_rights_review (dataset_key, decision, rights_hash, rights_snapshot, "
                    "reviewer, note) VALUES ('bls_series', 'reject', repeat('a', 64), '{}'::jsonb, 'x', 'short')"))

    def test_confirm_records_actor_and_never_flips(self, pg142):
        from app.catalog.rights_review import build_queue, reviewed_entries

        key = UNREVIEWED_KEY  # no committed hash: a recorded confirm must not flip it
        c = _client(pg142)
        s = _spec(key)
        assert not s.reviewed
        r = c.post(f"/api/v1/catalog/rights/{key}/review",
                   json={"decision": "confirm_current", "rights_hash": s.rights_hash,
                         "note": "Checked the FDIC terms page 2026-09-26."})
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["reviewer"] == "reviewer@nexdata.test" and body["reviewer_user_id"] == 42
        assert body["reviewed_at"] and body["rights_hash"] == s.rights_hash
        assert body["rights_snapshot"]["redistribution"] == s.redistribution
        assert body["review_state"] == "awaiting_commit" and body["reviewed_now"] is False
        # nothing became reviewed
        assert not _spec(key).reviewed
        detail = c.get(f"/api/v1/catalog/rights/{key}").json()
        assert detail["review_state"] == "awaiting_commit" and len(detail["reviews"]) == 1
        assert detail["rights"]["reviewed"] is False
        q = build_queue(pg142, live=False)
        assert [i["key"] for i in q["awaiting_commit"]] == [key]
        # --emit lists it; a later reject removes it
        assert reviewed_entries(pg142) == {key: (s.rights_hash, body["id"])}
        c.post(f"/api/v1/catalog/rights/{key}/review",
               json={"decision": "reject", "rights_hash": s.rights_hash, "note": "Second look: not yet."})
        assert reviewed_entries(pg142) == {}

    def test_stale_hash_and_bad_requests(self, pg142):
        c = _client(pg142)
        s = _spec("bls_series")
        ok_note = "Checked the terms page today."
        r = c.post("/api/v1/catalog/rights/bls_series/review",
                   json={"decision": "confirm_current", "rights_hash": "0" * 64, "note": ok_note})
        assert r.status_code == 409
        assert c.post("/api/v1/catalog/rights/nope/review",
                      json={"decision": "reject", "rights_hash": s.rights_hash, "note": ok_note}).status_code == 404
        assert c.post("/api/v1/catalog/rights/bls_series/review",
                      json={"decision": "accept_proposal", "rights_hash": s.rights_hash,
                            "note": ok_note}).status_code == 422
        assert c.post("/api/v1/catalog/rights/bls_series/review",
                      json={"decision": "confirm_current", "rights_hash": s.rights_hash,
                            "note": "short"}).status_code == 422
        assert c.post("/api/v1/catalog/rights/bls_series/review",
                      json={"decision": "approve", "rights_hash": s.rights_hash, "note": ok_note}).status_code == 422
        user = _client(pg142, {"role": "user", "email": "u@x"})
        assert user.post("/api/v1/catalog/rights/bls_series/review",
                         json={"decision": "reject", "rights_hash": s.rights_hash, "note": ok_note}).status_code == 403
        assert user.get("/api/v1/catalog/rights/review").status_code == 403
        dev = _client(pg142, {"role": "admin", "local_dev": True, "email": "local-dev@localhost"})
        assert dev.post("/api/v1/catalog/rights/bls_series/review",
                        json={"decision": "reject", "rights_hash": s.rights_hash, "note": ok_note}).status_code == 403

    def test_accept_proposal_records_proposal_hash(self, pg142):
        from app.catalog.rights_review import proposal_hash, reviewed_entries

        c = _client(pg142)
        s = _spec("realestate_osm_buildings")
        r = c.post("/api/v1/catalog/rights/realestate_osm_buildings/review",
                   json={"decision": "accept_proposal", "rights_hash": s.rights_hash,
                         "note": "ODbL permits commercial use with attribution."})
        assert r.status_code == 201, r.text
        assert r.json()["proposal_hash"] == proposal_hash(s)
        assert r.json()["proposal_snapshot"]["redistribution"] == "attribution"
        assert r.json()["review_state"] == "proposal_accepted"
        assert _spec("realestate_osm_buildings").redistribution == "restricted"  # not applied
        assert reviewed_entries(pg142) == {}

    def test_stale_state_after_rights_change(self, pg142, monkeypatch):
        from app.catalog.rights_review import latest_reviews, review_state

        s = _spec("bls_series")
        _client(pg142).post("/api/v1/catalog/rights/bls_series/review",
                            json={"decision": "confirm_current", "rights_hash": s.rights_hash,
                                  "note": "Checked the BLS copyright page."})
        changed = dataclasses.replace(s, attribution="Source: BLS (changed)")
        assert review_state(changed, latest_reviews(pg142)["bls_series"]) == "stale"

    def test_queue_and_report_endpoints_live(self, pg142):
        from sqlalchemy import text

        from app.catalog import registry

        c = _client(pg142)
        q = c.get("/api/v1/catalog/rights/review", params={"live": "true"})
        assert q.status_code == 200 and q.json()["review_table"] is True
        rep = c.get("/api/v1/catalog/rights/report", params={"format": "md"})
        assert rep.status_code == 200 and rep.headers["content-type"].startswith("text/markdown")
        j = c.get("/api/v1/catalog/rights/report").json()
        assert j["live"] is True and j["summary"]["reviewed"] == len(_in_force()) > 0
        # a storage-forbidden table that exists is counted exactly
        t = "yelp_businesses"  # declared as the pattern yelp_businesses*
        with pg142.begin() as conn:
            exists = conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": t}).scalar()
        if exists:
            h = next(x for x in j["storage_holdings"] if x["key"] == "yelp_businesses")
            assert h["measured"] and h["rows_exact"]
        assert registry.get_spec("yelp_businesses").storage == "forbidden"

    def test_no_table_gives_503(self, pg142):
        from sqlalchemy import text

        with pg142.begin() as conn:
            conn.execute(text("DROP TABLE catalog_rights_review"))
        s = _spec("bls_series")
        r = _client(pg142).post("/api/v1/catalog/rights/bls_series/review",
                                json={"decision": "confirm_current", "rights_hash": s.rights_hash,
                                      "note": "Checked the BLS copyright page."})
        assert r.status_code == 503
        assert _client(pg142).get("/api/v1/catalog/rights/bls_series").json()["reviews"] == []

    def test_history_hides_reviewer_from_users_live(self, pg142):
        s = _spec("bls_series")
        _client(pg142).post("/api/v1/catalog/rights/bls_series/review",
                            json={"decision": "reject", "rights_hash": s.rights_hash,
                                  "note": "Internal note about the terms."})
        u = _client(pg142, dict(USER)).get("/api/v1/catalog/rights/bls_series").json()
        assert u["reviews"] and u["review_state"] == "rejected"
        assert not {"reviewer", "reviewer_user_id", "api_key_id", "note"} & set(u["reviews"][0])


# ---------------------------------------------------------------------------
# Fix round (review findings)
# ---------------------------------------------------------------------------

ADMIN = {"role": "admin", "email": "a@x"}
USER = {"role": "user", "email": "u@x"}


def _bls_ds(status, data_state="ok"):
    from app.catalog import datasets

    s = _spec("bls_series")
    return datasets._ds("bls_series", s.source, s.display_name, s.description, s.kind, s.grain,
                        s.producer, s.cadence, tables=s.tables, verified_at=s.verified_at,
                        data_state=data_state, status=status)


@pytest.mark.unit
class TestFixSignOffCanPromote:
    """F1: _ds built reviewed=False before apply_review, so a signed-off dataset could never be ga/beta."""

    def test_committed_hash_allows_ga_through_ds(self, monkeypatch):
        from app.catalog import rights_reviewed

        monkeypatch.setitem(rights_reviewed.REVIEWED, "bls_series", (_spec("bls_series").rights_hash, 7))
        for status in ("ga", "beta"):
            spec = _bls_ds(status)
            assert spec.reviewed and spec.status_public == status
            assert spec.effective_redistribution == spec.redistribution

    def test_without_hash_ga_is_still_refused(self, monkeypatch):
        from app.catalog import rights_reviewed

        monkeypatch.delitem(rights_reviewed.REVIEWED, "bls_series", raising=False)
        with pytest.raises(ValueError, match="reviewed rights block"):
            _bls_ds("ga")

    def test_stale_hash_ga_is_refused(self, monkeypatch):
        from app.catalog import rights_reviewed

        monkeypatch.setitem(rights_reviewed.REVIEWED, "bls_series", ("0" * 64, 7))
        with pytest.raises(ValueError, match="reviewed rights block"):
            _bls_ds("ga")

    def test_other_statuses_keep_their_status(self):
        assert _bls_ds("archival").status_public == "archival"
        assert _bls_ds("internal").status_public == "internal"
        cat = _catalog()
        assert {s.key for s in cat if s.reviewed} == _in_force(cat)
        assert all(s.status_public not in ("ga", "beta") for s in cat)


@pytest.mark.unit
class TestFixZipMedspaScoresSplit:
    """F2: zip_medspa_scores (IRS SOI only) was gated and counted as Yelp storage-forbidden."""

    def test_split_and_ungated(self):
        from app.core.export_policy import catalog_rights_gate

        z = _spec("zip_medspa_scores")
        assert z.tables == ("zip_medspa_scores",) and z.source == "zip_scores"
        assert z.storage is None and z.commercial_use is None and not z.rights_gate
        assert z.inputs == ("irs_soi",) and z.pii_class == "none" and z.origin == "derived"
        assert z.redistribution == "internal_only"
        assert catalog_rights_gate("zip_medspa_scores") is None
        m = _spec("medspa_prospects")
        assert "zip_medspa_scores" not in m.tables and m.storage == "forbidden"
        assert catalog_rights_gate("medspa_prospects")["datasets"] == ["medspa_prospects"]

    def test_fred_loosening_does_not_cite_the_fred_restrictions(self):
        s = _spec("fred_series")
        p = s.proposed_rights
        assert p.change == "loosen" and p.citation_url != s.citation_url
        assert "fred.stlouisfed.org" not in p.citation_url and "105" in p.citation_quote

    def test_not_in_storage_flags(self):
        from app.catalog.rights_review import build_report

        rep = build_report(None, live=False)
        tables = {t for h in rep["storage_holdings"] for t in _spec(h["key"]).tables}
        assert "zip_medspa_scores" not in tables


class _FakeJob:
    def __init__(self, table):
        from datetime import datetime

        from app.core.models import ExportFormat, ExportStatus

        self.id, self.table_name = 1, table
        self.format, self.status = ExportFormat("csv"), ExportStatus("pending")
        self.columns = self.row_limit = self.filters = self.file_name = None
        self.file_size_bytes = self.row_count = self.error_message = None
        self.compress = False
        self.created_at = datetime(2026, 9, 26)
        self.started_at = self.completed_at = self.expires_at = None


def _export_client(monkeypatch, principal):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api.v1 import export
    from app.core.authz import current_principal
    from app.core.database import get_db

    monkeypatch.setattr(export, "run_export_job", lambda job_id: None)
    monkeypatch.setattr(export.ExportService, "create_export_job",
                        lambda self, table_name, **kw: _FakeJob(table_name))
    monkeypatch.setattr(export.ExportService, "list_tables", lambda self: [
        {"table_name": "m5_items", "row_count": 3, "columns": ["id"],
         "rights_gate": {"reasons": ["storage_forbidden"], "datasets": ["kaggle_m5"]}},
        {"table_name": "sec_13f_holdings", "row_count": 3, "columns": ["cik"], "rights_gate": None}])
    app = FastAPI()
    app.include_router(export.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: object()
    app.dependency_overrides[current_principal] = lambda: principal
    return TestClient(app)


@pytest.mark.unit
class TestFixExportCarriesGate:
    """F3/F5: TableInfo dropped rights_gate; export jobs were unflagged; export_allowed was dead code."""

    def test_table_list_http_response_keeps_gate(self, monkeypatch):
        body = _export_client(monkeypatch, ADMIN).get("/api/v1/export/tables").json()
        by = {t["table_name"]: t for t in body}
        assert by["m5_items"]["rights_gate"]["reasons"] == ["storage_forbidden"]
        assert by["sec_13f_holdings"]["rights_gate"] is None

    def test_admin_export_job_of_gated_table_is_flagged(self, monkeypatch):
        r = _export_client(monkeypatch, ADMIN).post("/api/v1/export/jobs",
                                                    json={"table_name": "m5_items", "format": "csv"})
        assert r.status_code == 201, r.text
        assert r.json()["rights_gate"]["reasons"] == ["storage_forbidden", "commercial_use_forbidden"]

    def test_non_admin_export_of_gated_table_refused(self, monkeypatch):
        c = _export_client(monkeypatch, USER)
        r = c.post("/api/v1/export/jobs", json={"table_name": "yelp_businesses", "format": "csv"})
        assert r.status_code == 403 and r.json()["detail"]["rights_gate"]["datasets"] == ["yelp_businesses"]
        ok = c.post("/api/v1/export/jobs", json={"table_name": "sec_13f_holdings", "format": "csv"})
        assert ok.status_code == 201 and ok.json()["rights_gate"] is None

    def test_export_allowed_is_used(self):
        src = (REPO / "app" / "api" / "v1" / "export.py").read_text(encoding="utf-8")
        assert "export_allowed(" in src


def _guard_client(principal, *tables):
    from fastapi import APIRouter, Depends, FastAPI
    from fastapi.testclient import TestClient

    from app.core.authz import current_principal
    from app.core.rights_guard import require_rights_clear

    router = APIRouter()

    @router.get("/rows", dependencies=[Depends(require_rights_clear(*tables))])
    def rows():
        return [{"id": 1}]

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[current_principal] = lambda: principal
    return TestClient(app)


@pytest.mark.unit
class TestFixSourceRoutersGated:
    """F4: source routers served storage-forbidden / agreement-required rows to any signed-in user."""

    def test_guard_refuses_users_and_flags_admins(self):
        r = _guard_client(USER, "internet_exchange").get("/rows")
        assert r.status_code == 403 and r.json()["detail"]["error"] == "rights_gated"
        assert "si_internet_exchanges" in r.json()["detail"]["datasets"]
        a = _guard_client(ADMIN, "internet_exchange").get("/rows")
        assert a.status_code == 200
        assert a.headers["X-Dataset-Rights-Gate"] == "storage_forbidden,commercial_use_forbidden"
        free = _guard_client(USER, "sec_13f_holdings").get("/rows")
        assert free.status_code == 200 and "X-Dataset-Rights-Gate" not in free.headers

    def test_every_guarded_dataset_is_gated(self):
        from app.core.rights_guard import ENFORCED_PATHS

        for key in ENFORCED_PATHS:
            assert _spec(key).rights_gate, key

    @pytest.mark.parametrize("path", [
        "/api/v1/prediction-markets/markets/top", "/api/v1/prediction-markets/markets/{market_id}/history",
        "/api/v1/medspa-discovery/prospects", "/api/v1/courtlistener/search", "/api/v1/courtlistener/stats",
        "/api/v1/site-intel/telecom/ix", "/api/v1/site-intel/telecom/ix/nearby",
        "/api/v1/site-intel/telecom/data-centers", "/api/v1/site-intel/telecom/data-centers/nearby",
        "/api/v1/site-intel/logistics/warehouse-listings",
        "/api/v1/site-intel/logistics/warehouse-listings/market-summary",
        "/api/v1/realestate/zoning/districts", "/api/v1/realestate/zoning/summary",
        "/api/v1/realestate/zoning/dc-eligible",
    ])
    def test_routes_are_wired(self, path):
        from app.core.rights_guard import rights_gate_for
        from app.main import app

        routes = [r for r in app.routes if getattr(r, "path", None) == path and "GET" in (r.methods or ())]
        assert routes, path
        for r in routes:
            tables = [t for t in (getattr(d.dependency, "rights_tables", None) for d in r.dependencies) if t]
            assert tables, path
            assert rights_gate_for(*tables[0]), path

    def test_vertical_router_wired(self):
        from app.main import app

        vr = [r for r in app.routes if getattr(r, "path", "").startswith("/api/v1/vertical-discovery")]
        assert vr and all(any(hasattr(d.dependency, "rights_tables") for d in r.dependencies) for r in vr)

    def test_zip_scores_router_not_gated(self):
        from app.main import app

        zr = [r for r in app.routes if getattr(r, "path", "").startswith("/api/v1/zip-scores")]
        assert zr and not any(hasattr(d.dependency, "rights_tables") for r in zr for d in r.dependencies)

    def test_report_lists_enforced_and_unenforced_paths(self):
        from app.catalog.rights_review import build_report, render_markdown

        rep = build_report(None, live=False)
        sp = rep["serving_paths"]
        assert "si_internet_exchanges" in sp["enforced"] and sp["unenforced"]
        assert any("econ-snapshot" in u["paths"] for u in sp["unenforced"])
        h = {x["key"]: x for x in rep["storage_holdings"]}
        assert "403" in h["si_internet_exchanges"]["gating_effect"]["source_api"]
        assert "unenforced" in h["fred_series"]["gating_effect"]["source_api"]
        ft = next(x for x in rep["storage_holdings"] if _spec(x["key"]).storage == "time_limited")
        assert "not measured" in ft["gating_effect"]["time_limited"]
        assert "Where the gate is enforced" in render_markdown(rep)


class _Db:
    def get_bind(self):
        return object()


def _rights_client(monkeypatch, principal):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api.v1 import catalog_rights
    from app.catalog import rights_review
    from app.core.authz import current_principal
    from app.core.database import get_db

    row = {"id": 5, "dataset_key": "bls_series", "decision": "reject", "rights_hash": "a" * 64,
           "proposal_hash": None, "rights_snapshot": {}, "proposal_snapshot": None,
           "reviewer": "reviewer@nexdata.test", "reviewer_user_id": 42, "api_key_id": 9,
           "note": "internal note about the terms", "reviewed_at": "2026-09-26T00:00:00Z"}
    monkeypatch.setattr(rights_review, "review_history", lambda engine, key, limit=50: [dict(row)])
    monkeypatch.setattr(rights_review, "latest_reviews", lambda engine: {"bls_series": dict(row)})
    monkeypatch.setattr(rights_review, "build_report", lambda engine, live=True: {"ok": True})
    app = FastAPI()
    app.include_router(catalog_rights.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: _Db()
    app.dependency_overrides[current_principal] = lambda: principal
    return TestClient(app)


@pytest.mark.unit
class TestFixRightsReadsAdminOnly:
    """F6: any signed-in user could read reviewer identities, notes and the holdings report."""

    def test_report_is_admin_only(self, monkeypatch):
        assert _rights_client(monkeypatch, USER).get("/api/v1/catalog/rights/report").status_code == 403
        assert _rights_client(monkeypatch, USER).get("/api/v1/catalog/rights/report",
                                                     params={"format": "md"}).status_code == 403
        assert _rights_client(monkeypatch, ADMIN).get("/api/v1/catalog/rights/report").json() == {"ok": True}

    def test_history_hides_reviewer_from_users(self, monkeypatch):
        u = _rights_client(monkeypatch, USER).get("/api/v1/catalog/rights/bls_series").json()
        assert u["reviews"] and u["review_state"] == "stale"
        for f in ("reviewer", "reviewer_user_id", "api_key_id", "note"):
            assert f not in u["reviews"][0], f
        assert u["reviews"][0]["decision"] == "reject"
        a = _rights_client(monkeypatch, ADMIN).get("/api/v1/catalog/rights/bls_series").json()
        assert a["reviews"][0]["reviewer"] == "reviewer@nexdata.test" and a["reviews"][0]["note"]
