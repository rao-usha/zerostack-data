"""
Tests for SPEC_143 — catalog lineage graph (PLAN_088 §3 "SPEC_136").

- derived_mart / entity specs declare inputs, and those inputs equal the
  datasets whose tables the producer's SQL reads (the SQL-reference test);
- app/marts/inputs.py stage maps are derived from the catalog;
- the computed graph: nodes, edges, walks, cycles, views, observed ledger edges,
  drift;
- GET /catalog/lineage and GET /catalog/{key}/lineage (user-level);
- the legacy lineage_service and /lineage router are gone;
- job_keys aliases resolve the ingestion_jobs sources SPEC_144's backfill left
  unresolved.
"""
import json
import os
import runpy
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]


def _catalog():
    from app.catalog import get_catalog

    return get_catalog()


def _spec(key):
    from app.catalog import get_spec

    s = get_spec(key)
    assert s is not None, key
    return s


def _static():
    from app.catalog import lineage

    lineage.clear_cache()
    return lineage.full_graph(None, live=False, refresh=True)


# ---------------------------------------------------------------------------
# 1. inputs match the SQL
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSqlReference:
    def test_every_derived_spec_declares_inputs(self):
        from app.catalog.lineage import DERIVED_KINDS, NO_CATALOG_INPUTS

        missing = [s.key for s in _catalog()
                   if s.kind in DERIVED_KINDS and not s.inputs and s.key not in NO_CATALOG_INPUTS]
        assert not missing, missing
        # the exemption list only shrinks: every entry is a real entity spec with no inputs
        for key, reason in NO_CATALOG_INPUTS.items():
            s = _spec(key)
            assert s.kind in DERIVED_KINDS and not s.inputs and reason

    def test_every_input_key_exists(self):
        keys = {s.key for s in _catalog()}
        bad = [(s.key, i) for s in _catalog() for i in s.inputs if i not in keys]
        assert not bad, bad

    def test_every_derived_spec_is_sql_checked(self):
        """Fix round: no whole-spec exemption. Every derived/entity spec with
        catalog inputs has producer modules the scan checks."""
        from app.catalog.lineage import DERIVED_KINDS, NO_CATALOG_INPUTS, producer_modules

        unchecked = [s.key for s in _catalog() if s.kind in DERIVED_KINDS
                     and not producer_modules(s) and s.key not in NO_CATALOG_INPUTS]
        assert not unchecked, unchecked

    def test_api_inputs_are_declared_and_not_sql(self):
        """The only declared inputs allowed to be absent from the SQL are ones
        the producer fetches from the publisher's API; each is explained."""
        from app.catalog import lineage

        specs = _catalog()
        for key, extra in lineage.API_INPUTS.items():
            s = _spec(key)
            found = lineage.sql_inputs(s, specs)
            for inp, why in extra.items():
                assert inp in s.inputs and why, (key, inp)
                assert inp not in found["inputs"], (key, inp)

    def test_discovery_specs_declare_what_their_sql_reads(self):
        # regression: vertical_prospects enrichment reads nppes_providers; both
        # ownership classifiers read pe_portfolio_companies (pe_collection)
        from app.catalog.lineage import sql_inputs

        specs = _catalog()
        v = sql_inputs(_spec("vertical_prospects"), specs)
        assert {"irs_soi", "nppes_providers", "pe_collection"} == set(v["inputs"])
        assert "dental_prospects" in v["self"]
        assert {"nppes_providers", "pe_collection"} <= set(_spec("vertical_prospects").inputs)
        m = sql_inputs(_spec("medspa_prospects"), specs)
        assert {"irs_soi", "nppes_providers", "pe_collection"} == set(m["inputs"])
        r = sql_inputs(_spec("rollup_market_scores"), specs)
        assert set(r["inputs"]) == {"census_cbp", "irs_soi"}

    def test_producer_modules_exist_and_are_producers(self):
        from app.catalog.lineage import PRODUCER_MODULES

        producers = {p for s in _catalog() for p in s.producers}
        for producer, paths in PRODUCER_MODULES.items():
            assert producer in producers, producer
            for p in paths:
                assert (REPO / p).is_file(), p

    def test_inputs_equal_the_tables_the_sql_reads(self):
        """The SQL-reference test: every table a producer reads belongs to a
        declared input or the dataset itself, and every declared input is read."""
        from app.catalog.lineage import API_INPUTS, producer_modules, sql_inputs

        specs = _catalog()
        bad = []
        checked = 0
        for s in specs:
            if not producer_modules(s):
                continue
            checked += 1
            found = sql_inputs(s, specs)
            want = set(s.inputs) - set(API_INPUTS.get(s.key, {}))
            if set(found["inputs"]) != want or found["ambiguous"]:
                bad.append((s.key, sorted(s.inputs), found))
            assert found["self"], f"{s.key}: the scan found none of its own tables"
        assert checked >= 10
        assert not bad, bad

    def test_plan_corrections(self):
        assert _spec("pe_firms_sec").inputs == ("sec_adv_roster", "entity_master")
        assert "sec_iapd_feed" not in _spec("pe_firms_sec").inputs
        assert set(_spec("pe_funds_sec").inputs) == {
            "sec_form_d", "sec_adv_private_funds", "sec_adv_schedule_d", "sec_adv_roster",
            "pe_firms_sec"}
        assert set(_spec("pe_people_sec").inputs) == {
            "sec_form_d", "sec_adv_roster", "pe_firms_sec", "pe_funds_sec"}
        assert set(_spec("entity_cik_crd_bridge").inputs) == {
            "sec_13f", "sec_adv_roster", "sec_edgar_submissions"}

    def test_evidence_disposition_records_the_change(self):
        ev = json.loads((REPO / "app/catalog/evidence/verification_2026-09-25.json")
                        .read_text(encoding="utf-8"))
        by = {e["key"]: e for e in ev["entries"]}
        for key in ("pe_firms_sec", "pe_funds_sec", "pe_people_sec", "entity_cik_crd_bridge",
                    "medspa_prospects", "vertical_prospects"):
            assert by[key]["disposition"]["inputs"].startswith("amended:SPEC_143"), key

    def test_scanner_reads_constants_fstrings_and_format(self):
        from app.catalog.lineage import scan_source

        src = '''
FILINGS = "public.sec_adv_filings"
ADV = "public.sec_adv_private_funds"
TABLES = ["fred_interest_rates", "fred_commodities"]
A = f"""SELECT 1 FROM {FILINGS} f JOIN form_d_issuers i ON 1=1"""
B = """SELECT * FROM {fund_filings} pff JOIN "core"."identifier" x ON 1=1""".format(fund_filings="x")
FUND_FILINGS = "public.sec_adv_private_fund_filings"
def f(conn):
    conn.execute("select * from latest_cte")
DENTAL = VerticalConfig(slug="dental", table_name="dental_prospects")
'''
        refs = scan_source(src)
        assert {"sec_adv_filings", "form_d_issuers", "sec_adv_private_fund_filings",
                "core.identifier", "sec_adv_private_funds", "fred_interest_rates",
                "fred_commodities", "dental_prospects"} <= refs


# ---------------------------------------------------------------------------
# 2. stage maps derived from the catalog
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStageMaps:
    def test_maps_equal_the_catalog(self):
        from app.catalog.lineage import stage_inputs
        from app.marts import inputs

        assert (inputs.PE_MART_STAGE_INPUTS, inputs.PE_MART_STAGE_UPSTREAM) == \
            stage_inputs("pe_mart_build")
        assert (inputs.ENTITY_STAGE_INPUTS, inputs.ENTITY_STAGE_UPSTREAM) == \
            stage_inputs("entity_resolve")

    def test_derived_values(self):
        from app.marts import inputs

        assert inputs.PE_MART_STAGE_INPUTS == {
            "adv_private_funds": ["sec_adv_schedule_d"],
            "firms": ["sec_adv_roster"],
            "funds": ["sec_adv_roster", "sec_adv_schedule_d", "sec_form_d"],
            "people": ["sec_adv_roster", "sec_form_d"],
        }
        assert inputs.PE_MART_STAGE_UPSTREAM == {"firms": ["entity_resolve"]}
        assert inputs.ENTITY_STAGE_INPUTS["feeds"] == [
            "sec_13f", "sec_adv_roster", "sec_edgar_submissions", "sec_form_d",
            "sec_iapd_feed", "sec_insider"]
        assert inputs.ENTITY_STAGE_INPUTS["bridge"] == [
            "sec_13f", "sec_adv_roster", "sec_edgar_submissions"]
        assert inputs.ENTITY_STAGE_INPUTS["resolve"] == []
        assert inputs.ENTITY_STAGE_UPSTREAM == {}

    def test_stages_match_the_executors(self):
        from app.marts import inputs
        from app.worker.executors import entity_resolve, pe_marts

        assert set(pe_marts._stages(False, False, False)) == set(inputs.PE_MART_STAGE_INPUTS)
        assert set(entity_resolve._stages(False, False)) == set(inputs.ENTITY_STAGE_INPUTS)

    def test_every_asserted_source_has_a_max_age(self):
        from app.marts import inputs

        for m in (inputs.PE_MART_STAGE_INPUTS, inputs.ENTITY_STAGE_INPUTS,
                  inputs.PE_MART_STAGE_UPSTREAM):
            for sources in m.values():
                for src in sources:
                    assert src in inputs.MAX_AGE_DAYS, src

    def test_a_hand_edited_map_is_reported_as_drift(self, monkeypatch):
        from app.catalog import lineage
        from app.marts import inputs

        monkeypatch.setattr(inputs, "PE_MART_STAGE_INPUTS",
                            {**inputs.PE_MART_STAGE_INPUTS, "funds": ["sec_form_d"]})
        g = lineage.build_static(_catalog())
        kinds = {(d["kind"], d.get("stage")) for d in g["drift"]}
        assert ("stage_map_mismatch", "pe_mart_build#funds") in kinds


# ---------------------------------------------------------------------------
# 3. the graph
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGraph:
    def test_shape(self):
        from app.catalog.lineage import EDGE_TYPES

        g = _static()
        ids = [n["id"] for n in g["nodes"]]
        assert len(ids) == len(set(ids))
        idset = set(ids)
        for e in g["edges"]:
            assert e["type"] in EDGE_TYPES
            assert e["from"] in idset and e["to"] in idset, e
        datasets = {n["name"] for n in g["nodes"] if n["type"] == "dataset"}
        assert datasets == {s.key for s in _catalog()}
        assert g["counts"]["datasets"] == len(datasets)
        assert g["observed"]["available"] is False

    def test_downstream_of_form_d(self):
        from app.catalog.lineage import walk

        w = walk(_static(), "sec_form_d", direction="down", depth=5)
        down = {r["key"] for r in w["downstream"]}
        assert {"pe_funds_sec", "pe_people_sec", "entity_source_records"} <= down
        assert "entity_master" in down  # through entity_source_records
        assert w["upstream"] == []
        assert "form_d_filings" in w["tables"] and "bulk:sec_form_d" in w["producers"]

    def test_depth_limit(self):
        from app.catalog.lineage import walk

        w = walk(_static(), "sec_form_d", direction="down", depth=1)
        assert all(r["depth"] == 1 for r in w["downstream"])
        down = {r["key"] for r in w["downstream"]}
        assert "pe_funds_sec" in down and "entity_master" not in down

    def test_upstream_of_people(self):
        from app.catalog.lineage import walk

        w = walk(_static(), "pe_people_sec", direction="up", depth=3)
        up = {r["key"]: r for r in w["upstream"]}
        assert {"sec_form_d", "sec_adv_roster", "pe_firms_sec", "pe_funds_sec"} <= set(up)
        assert up["sec_adv_schedule_d"]["depth"] == 2
        assert "entity_master" in up
        assert "sql" in up["sec_adv_roster"]["via"] and "declared" in up["sec_adv_roster"]["via"]

    def test_no_cycles(self):
        from app.catalog.lineage import find_cycles

        assert find_cycles(_static()["edges"]) == []
        assert find_cycles([
            {"from": "dataset:a", "to": "dataset:b", "type": "declared"},
            {"from": "dataset:b", "to": "dataset:a", "type": "declared"},
        ]) == [["dataset:a", "dataset:b", "dataset:a"]]

    def test_walk_arguments(self):
        from app.catalog.lineage import MAX_DEPTH, walk

        with pytest.raises(ValueError):
            walk(_static(), "sec_form_d", direction="sideways")
        assert walk(_static(), "sec_form_d", depth=99)["depth"] == MAX_DEPTH

    def test_view_edges_from_code(self):
        g = _static()
        edges = {(e["from"], e["to"]) for e in g["edges"] if e["type"] == "view"}
        assert ("table:sec_financial_facts", "view:public_company_financials") in edges
        assert ("table:sec_income_statement", "view:public_company_financials") in edges
        assert ("table:fred_interest_rates", "view:fred_observations") in edges
        drift = [d for d in g["drift"] if d["kind"] == "undeclared_view_input"]
        assert {"table": "sec_company_metadata", "views": ["public_company_financials"]}.items() \
            <= next(d for d in drift if d["table"] == "sec_company_metadata").items()

    def test_views_are_downstream_of_their_dataset(self):
        from app.catalog.lineage import walk

        w = walk(_static(), "sec_companyfacts", direction="down", depth=1)
        assert "public_company_financials" in w["views"]

    def test_views_over_pattern_owned_tables_reach_their_dataset(self):
        # regression: fred_series declares fred_* only; fred_observations reads
        # the concrete fred_interest_rates ... tables
        from app.catalog.lineage import walk

        g = _static()
        w = walk(g, "fred_series", direction="down", depth=1)
        assert "fred_observations" in w["views"]
        assert "fred_interest_rates" in w["tables"]
        stores = next(e for e in g["edges"] if e["type"] == "stores"
                      and e["from"] == "dataset:fred_series"
                      and e["to"] == "table:fred_interest_rates")
        assert stores["via_pattern"] == ["fred_*"]

    def test_pattern_owned_view_input_from_pg_depend(self):
        from app.catalog import lineage

        b = lineage._Builder()
        claims, patterns = lineage.table_owners(_catalog())
        lineage._add_view_edges(b, {"v_rates": ["fred_commodities"]}, "pg_depend",
                                claims, patterns)
        g = b.result()
        assert ("dataset:fred_series", "table:fred_commodities", "stores") in {
            (e["from"], e["to"], e["type"]) for e in g["edges"]}

    def test_declared_and_sql_agree_today(self):
        kinds = {d["kind"] for d in _static()["drift"]}
        assert not kinds & {"declared_not_read", "read_not_declared", "ambiguous_table",
                            "stage_map_mismatch", "stage_without_dataset"}

    def test_producer_source_and_table_edges(self):
        g = _static()
        edges = {(e["from"], e["to"], e["type"]) for e in g["edges"]}
        assert ("producer:bulk:sec_form_d", "dataset:sec_form_d", "produces") in edges
        assert ("producer:api:form_d", "dataset:sec_form_d", "produces") in edges
        assert ("source:sec", "dataset:sec_form_d", "publishes") in edges
        assert ("dataset:pe_firms_sec", "table:pe_firms", "stores") in edges
        # a derived dataset is not "published" by its family name
        assert ("source:pe_marts", "dataset:pe_firms_sec", "publishes") not in edges
        assert ("dataset:sec_adv_roster", "dataset:pe_firms_sec", "stage_input") in edges

    def test_observed_edges_from_the_ledger(self):
        from app.catalog.lineage import observed_edges

        builds = [{"id": 7, "mart": "pe_marts", "finished_at": None, "inputs": [
            {"source": "sec_form_d", "kind": "bulk", "release_keys": ["2026q2"]},
            {"source": "entity_resolve", "kind": "mart", "release_keys": ["mart_build:3"]},
            {"source": "sec_bogus", "kind": "bulk"},
        ]}]
        edges, drift = observed_edges(builds, _catalog())
        got = {(e["from"], e["to"]) for e in edges}
        assert ("dataset:sec_form_d", "dataset:pe_funds_sec") in got
        assert ("dataset:sec_form_d", "dataset:pe_people_sec") in got
        assert ("dataset:entity_master", "dataset:pe_firms_sec") in got
        rk = next(e for e in edges if e["to"] == "dataset:pe_funds_sec")["release_keys"]
        assert rk == ["2026q2"]
        assert [d["input"] for d in drift if d["kind"] == "observed_not_declared"] == ["sec_bogus"]

    def test_declared_but_not_observed(self):
        """The other direction: a stage that ran without asserting a declared input."""
        from app.catalog.lineage import observed_edges

        full = [{"source": s, "kind": "bulk"} for s in
                ("sec_adv_roster", "sec_adv_schedule_d", "sec_form_d")] + \
               [{"source": "entity_resolve", "kind": "mart"}]
        ok = {"id": 1, "mart": "pe_marts", "stages": ["adv_private_funds", "firms", "funds",
                                                        "people"], "inputs": full}
        _, drift = observed_edges([ok], _catalog())
        assert drift == []
        # the funds stage ran, but the build dropped sec_adv_schedule_d
        dropped = dict(ok, id=2, inputs=[r for r in full if r["source"] != "sec_adv_schedule_d"])
        _, drift = observed_edges([dropped], _catalog())
        got = {(d["kind"], d["stage"], d["input"], d["dataset"]) for d in drift}
        assert got == {
            ("declared_not_observed", "pe_mart_build#adv_private_funds", "sec_adv_schedule_d",
             "sec_adv_private_funds"),
            ("declared_not_observed", "pe_mart_build#funds", "sec_adv_schedule_d",
             "pe_funds_sec")}
        # the upstream mart the firms stage declares
        no_up = dict(ok, id=3, inputs=[r for r in full if r["kind"] == "bulk"])
        _, drift = observed_edges([no_up], _catalog())
        assert [(d["stage"], d["input"], d["input_kind"]) for d in drift] == [
            ("pe_mart_build#firms", "entity_resolve", "mart")]
        # only the stages that ran are judged
        firms_only = dict(ok, id=4, stages=["firms"],
                          inputs=[{"source": "sec_adv_roster", "kind": "bulk"},
                                  {"source": "entity_resolve", "kind": "mart"}])
        assert observed_edges([firms_only], _catalog())[1] == []

    def test_impact_joins_status_verdicts(self):
        from app.catalog.lineage import impact, walk

        w = walk(_static(), "pe_funds_sec", direction="both", depth=1)
        statuses = {"pe_funds_sec": {"status": "current", "status_reason": "ok"},
                    "sec_form_d": {"status": "failing", "status_reason": "last run failed"},
                    "sec_adv_roster": {"status": "current", "status_reason": "ok"}}
        out = impact(w, statuses)
        up = {r["key"]: r for r in out["upstream"]}
        assert up["sec_form_d"]["status"] == "failing"
        assert up["sec_adv_private_funds"]["status"] is None
        imp = out["impact"]
        assert imp["available"] and imp["status"] == "current"
        assert [p["key"] for p in imp["upstream_problems"]] == ["sec_form_d"]
        assert "pe_people_sec" in imp["downstream_at_risk"]
        healthy = impact(w, {k: {"status": "current", "status_reason": ""}
                             for k in [r["key"] for r in w["upstream"] + w["downstream"]]
                             + ["pe_funds_sec"]})
        assert healthy["impact"]["upstream_problems"] == []
        assert healthy["impact"]["downstream_at_risk"] == []
        assert impact(w, None)["impact"]["available"] is False

    def test_non_pg_engine_degrades(self):
        from sqlalchemy import create_engine

        from app.catalog import lineage

        lineage.clear_cache()
        g = lineage.full_graph(create_engine("sqlite://"), live=True, refresh=True)
        assert g["observed"]["available"] is False and g["views_source"] == "code"

    def test_consumers_opt_in(self):
        from app.catalog import lineage

        lineage.clear_cache()
        g = lineage.full_graph(None, live=False, consumers=True, refresh=True)
        consumers = {e["to"] for e in g["edges"] if e["type"] == "consumes"
                     and e["from"] == "table:form_d_filings"}
        assert "consumer:app/api/v1/form_d.py" in consumers


# ---------------------------------------------------------------------------
# 4. API
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.api.v1 import catalog, catalog_lineage
    from app.catalog import lineage
    from app.core.authz import current_principal, require_admin_for_writes
    from app.core.database import get_db

    lineage.clear_cache()
    engine = create_engine("sqlite://")
    Session = sessionmaker(bind=engine)
    app = FastAPI()
    auth = [Depends(require_admin_for_writes)]
    app.include_router(catalog_lineage.router, prefix="/api/v1", dependencies=auth)
    app.include_router(catalog.router, prefix="/api/v1", dependencies=auth)

    def _db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[current_principal] = lambda: {"role": "user"}
    return TestClient(app)


@pytest.mark.unit
class TestApi:
    def test_full_graph_is_user_level(self, client):
        r = client.get("/api/v1/catalog/lineage")
        assert r.status_code == 200
        body = r.json()
        assert {"nodes", "edges", "drift", "observed", "counts"} <= set(body)
        assert body["counts"]["datasets"] == len(_catalog())

    def test_dataset_lineage(self, client):
        r = client.get("/api/v1/catalog/pe_funds_sec/lineage",
                       params={"direction": "up", "depth": 1})
        assert r.status_code == 200
        up = {x["key"] for x in r.json()["upstream"]}
        assert up == set(_spec("pe_funds_sec").inputs)
        down = client.get("/api/v1/catalog/pe_funds_sec/lineage",
                          params={"direction": "down"}).json()["downstream"]
        assert "pe_people_sec" in {x["key"] for x in down}

    def test_errors(self, client):
        assert client.get("/api/v1/catalog/nope/lineage").status_code == 404
        assert client.get("/api/v1/catalog/sec_form_d/lineage",
                          params={"direction": "x"}).status_code == 422
        assert client.get("/api/v1/catalog/sec_form_d/lineage",
                          params={"depth": 0}).status_code == 422

    def test_status_impact_param(self, client, monkeypatch):
        from app.services import dataset_status

        seen = {}

        def fake(db, **kw):
            seen.update(kw)
            return {"datasets": [{"key": k, "status": "failing" if k == "sec_form_d" else
                                  "current", "status_reason": "x"} for k in kw["keys"]]}

        monkeypatch.setattr(dataset_status, "build_status", fake)
        body = client.get("/api/v1/catalog/pe_funds_sec/lineage",
                          params={"direction": "up", "depth": 1, "status": "true"}).json()
        assert seen["include_errors"] is False and "pe_funds_sec" in seen["keys"]
        assert body["impact"]["available"] is True
        assert [p["key"] for p in body["impact"]["upstream_problems"]] == ["sec_form_d"]
        # without status=true there is no impact block
        assert "impact" not in client.get("/api/v1/catalog/pe_funds_sec/lineage").json()

        def boom(db, **kw):
            raise RuntimeError("no status")

        monkeypatch.setattr(dataset_status, "build_status", boom)
        body = client.get("/api/v1/catalog/pe_funds_sec/lineage",
                          params={"status": "true"}).json()
        assert body["impact"]["available"] is False

    def test_catalog_detail_still_served(self, client):
        # /catalog/{key} is not swallowed and /catalog/lineage is not a dataset key
        r = client.get("/api/v1/catalog/lineage")
        assert "nodes" in r.json()


# ---------------------------------------------------------------------------
# 5. the legacy lineage stack is retired
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLegacyRetired:
    def test_files_deleted(self):
        assert not (REPO / "app/core/lineage_service.py").exists()
        assert not (REPO / "app/api/v1/lineage.py").exists()

    def test_main_registration(self):
        main = (REPO / "app" / "main.py").read_text(encoding="utf-8")
        assert "lineage.router" not in main.replace("catalog_lineage.router", "")
        assert '"name": "lineage"' not in main
        mine = 'app.include_router(catalog_lineage.router, prefix="/api/v1", dependencies=_auth)'
        theirs = 'app.include_router(catalog.router, prefix="/api/v1", dependencies=_auth)'
        assert mine in main and main.index(mine) < main.index(theirs)

    def test_lineage_route_is_gone(self):
        from app.main import app

        paths = {getattr(r, "path", "") for r in app.routes}
        assert not any(p.startswith("/api/v1/lineage") for p in paths)
        assert "/api/v1/catalog/lineage" in paths
        assert "/api/v1/catalog/{key}/lineage" in paths

    def test_legacy_tables_stay_modelled(self):
        # user decision: stop serving, leave the tables
        from app.core.models import Base

        for t in ("lineage_nodes", "lineage_edges", "lineage_events", "dataset_versions",
                  "impact_analysis"):
            assert t in Base.metadata.tables, t


# ---------------------------------------------------------------------------
# 6. dataset_key aliases
# ---------------------------------------------------------------------------

# The 322 ingestion_jobs rows SPEC_144's dry run left unresolved, plus the one
# ambiguous job:pe_mart_build row (live, read-only, 2026-09-26): (source,
# config, rows). Configs are representative of each group.
UNRESOLVED_2026_09_26 = [
    ("job_postings", {"limit": 50, "skip_recent_hours": 24}, 162),
    ("international_econ_oecd", {"dataset": "mei"}, 16),
    ("census_bfs", {}, 14),
    ("usda", {}, 10),
    ("github", {}, 8),
    ("epa_echo", {}, 7),
    ("app_rankings", {}, 7),
    ("opencorporates", {}, 7),
    ("web_traffic", {}, 7),
    ("form_d", {}, 7),
    ("form_adv", {}, 7),
    ("job_postings_skills", {"action": "backfill_skills"}, 7),
    ("epa_ghg", {}, 6),
    ("international_econ_worldbank", {"dataset": "wdi"}, 6),
    ("dot_grants", {}, 6),
    ("ffiec_banks", {}, 4),
    ("cms_hospitals", {}, 4),
    ("census_cbp", {"year": 2022}, 3),
    ("international_econ_oecd", {"dataset": "batis"}, 2),
    ("international_econ_oecd", {"dataset": "tax"}, 2),
    ("international_econ_oecd", {"dataset": "kei"}, 2),
    ("international_econ_worldbank", {"dataset": "countries"}, 2),
    ("international_econ_oecd", {"dataset": "alfs"}, 2),
    ("family_office", {"tables": ["family_offices"], "backfill": True}, 1),
    ("pe_news", {"tables": ["pe_firm_news"], "backfill": True}, 1),
    ("international_econ_bis", {"dataset": "property"}, 1),
    ("pe_people", {"tables": ["pe_firm_people", "pe_people"], "backfill": True}, 1),
    ("pe_portfolio", {"tables": ["pe_portfolio_companies"], "backfill": True}, 1),
    ("international_econ_imf", {"dataset": "ifs"}, 1),
    ("test_manual", {"test": True}, 1),
    ("trade_gateway", {"tables": ["trade_gateway_stats"], "backfill": True}, 1),
    ("international_econ_worldbank", {"dataset": "indicators"}, 1),
    ("ferc_energy", {}, 1),
    ("pe_deals", {"tables": ["pe_deals"], "backfill": True}, 1),
    ("pe_collection", {"tables": ["pe_firms"], "backfill": True}, 1),
    ("eia_electricity", {"tables": ["electricity_price"], "backfill": True}, 1),
    ("three_pl", {"tables": ["three_pl_company"], "backfill": True}, 1),
    ("pe_fund_data", {"tables": ["pe_funds"], "backfill": True}, 1),
    ("portfolio_research", {"tables": ["portfolio_companies"], "backfill": True}, 1),
    ("news_collection", {"tables": ["news_items"], "backfill": True}, 1),
    ("people_collection", {"tables": ["people", "company_people", "org_chart_snapshots"],
                           "backfill": True}, 1),
    ("job:pe_mart_build", {}, 1),
    ("freight_index", {"tables": ["container_freight_index"], "backfill": True}, 1),
    ("google_trends", {}, 1),
    ("industrial_companies", {"tables": ["industrial_companies"], "backfill": True}, 1),
    ("lp_collection", {"tables": ["lp_key_contact", "lp_fund"], "backfill": True}, 1),
    ("international_econ_bis", {"dataset": "eer"}, 1),
    ("pe_funds", {"tables": ["pe_fund_investments"], "backfill": True}, 1),
]

# Sources whose router has no catalog dataset yet (a new spec is a truth-pass
# change, not an alias), a manual test row, rows naming a shared table, and the
# one multi-dataset producer.
EXPECTED_UNRESOLVED = {
    "census_bfs", "epa_ghg", "dot_grants", "cms_hospitals", "ffiec_banks", "ferc_energy",
    "google_trends", "test_manual", "job:pe_mart_build", "freight_index", "pe_collection",
    "pe_fund_data", "pe_people", "news_collection",
    # fix round: maintenance over job_postings, not a producer run
    "job_postings_skills",
}
# 280 before the fix round dropped the 7 job_postings_skills rows
EXPECTED_RESOLVED = 273


def _load_fixture(engine):
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE ingestion_jobs (id INTEGER PRIMARY KEY, source TEXT, "
                          "config TEXT, dataset_key TEXT)"))
        i = 0
        for source, config, n in UNRESOLVED_2026_09_26:
            for _ in range(n):
                i += 1
                conn.execute(text("INSERT INTO ingestion_jobs (id, source, config) "
                                  "VALUES (:i, :s, :c)"),
                             {"i": i, "s": source, "c": json.dumps(config)})


@pytest.mark.unit
class TestJobKeyAliases:
    def _map(self):
        from app.catalog.job_keys import ProducerMap

        return ProducerMap(_catalog())

    def test_fixture_is_the_322_plus_the_ambiguous_row(self):
        assert sum(n for _, _, n in UNRESOLVED_2026_09_26) == 323

    def test_resolves_the_backlog(self):
        m = self._map()
        resolved = unresolved = 0
        left = set()
        for source, config, n in UNRESOLVED_2026_09_26:
            if m.dataset_key_for_job(source, config):
                resolved += n
            else:
                unresolved += n
                left.add(source)
        assert left == EXPECTED_UNRESOLVED
        assert resolved == EXPECTED_RESOLVED and unresolved == 323 - EXPECTED_RESOLVED, \
            (resolved, unresolved)

    def test_backfill_plan_uses_the_aliases(self):
        """Regression: backfill_dataset_key.plan() resolved with producer_for_job
        only (53 of 323); it must resolve what dataset_key_for_job resolves."""
        from sqlalchemy import create_engine

        from app.catalog import backfill_dataset_key as bf

        engine = create_engine("sqlite://")
        _load_fixture(engine)
        m = self._map()
        out = bf.plan(engine, m)
        rep = out["report"]
        assert rep["null_before"] == 323
        assert rep["resolvable"] == EXPECTED_RESOLVED, rep
        assert rep["ambiguous"] == 1 and rep["ambiguous_sources"] == {"job:pe_mart_build": 1}
        assert set(rep["unresolved_sources"]) == EXPECTED_UNRESOLVED - {"job:pe_mart_build"}
        assert rep["by_dataset"]["job_postings"] == 162
        assert rep["by_dataset"]["intl_oecd"] == 24
        assert rep["by_dataset"]["usda_nass"] == 10
        assert rep["by_dataset"]["family_offices"] == 1  # config.tables
        # every planned row agrees with the insert listener's resolver
        from sqlalchemy import text

        with engine.connect() as conn:
            rows = {r[0]: (r[1], json.loads(r[2])) for r in conn.execute(
                text("SELECT id, source, config FROM ingestion_jobs"))}
        for key, ids in out["updates"].items():
            for i in ids:
                assert m.dataset_key_for_job(*rows[i]) == key
        # and --apply writes them
        applied = bf.backfill(engine, apply=True, pmap=m)
        assert applied["updated"] == EXPECTED_RESOLVED
        assert applied["null_after"] == 323 - EXPECTED_RESOLVED

    def test_one_resolver(self):
        m = self._map()
        assert m.datasets_for_job("job:pe_mart_build", {}) == m.datasets_for("job:pe_mart_build")
        assert len(m.datasets_for_job("job:pe_mart_build", {})) > 1
        assert m.datasets_for_job("usda", {}) == ("usda_nass",)
        assert m.datasets_for_job("job_postings_skills", {"action": "backfill_skills"}) == ()
        assert m.datasets_for_job(None, {}) == ()

    def test_skills_backfill_is_maintenance_not_a_run(self):
        from app.catalog.job_keys import JOB_SOURCE_DATASETS, MAINTENANCE_SOURCES

        assert "job_postings_skills" not in JOB_SOURCE_DATASETS
        assert MAINTENANCE_SOURCES["job_postings_skills"] == "job_postings"
        assert self._map().dataset_key_for_job("job_postings_skills", {}) is None

    def test_status_read_uses_the_aliases(self):
        """dataset_status's NULL-dataset_key fallback and batch-run counts use the
        same resolver; an api:<source> producer never becomes a batch default."""
        import inspect

        from app.services import dataset_status as ds

        src = inspect.getsource(ds.collect_facts)
        assert src.count("datasets_for_job(") == 2

        class Src:
            def __init__(self, key, cfg):
                self.key, self.default_config = key, cfg

        class Tier:
            def __init__(self, sources):
                self.sources = sources

        import app.core.batch_service as bs

        m = self._map()
        orig = bs.TIERS
        try:
            bs.TIERS = [Tier([Src("census_cbp", {"year": 2022}), Src("fred", {})])]
            got = ds._batch_defaults(m)
        finally:
            bs.TIERS = orig
        assert "fred" in got and "" not in got and not any(k.startswith("api") for k in got)
        assert len(got) == 1

    @pytest.mark.parametrize("source,config,key", [
        ("job_postings", {"company_id": 5}, "job_postings"),
        ("usda", {"incremental": True}, "usda_nass"),
        ("usda", {"dataset": "crop"}, "usda_nass"),
        ("international_econ_oecd", {"dataset": "mei"}, "intl_oecd"),
        ("international_econ_worldbank", {"dataset": "wdi"}, "intl_worldbank"),
        ("international_econ_bis", {"dataset": "eer"}, "intl_bis"),
        ("international_econ_imf", {"dataset": "ifs"}, "intl_imf"),
        ("census_cbp", {"year": 2022}, "census_cbp"),
        ("form_d", {}, "sec_form_d"),
        ("form_adv", {}, "sec_form_adv_legacy"),
        ("github", {}, "github_analytics"),
        ("epa_echo", {}, "epa_echo_facilities"),
        ("family_office", {"tables": ["family_offices"]}, "family_offices"),
        ("fred", {}, "fred_series"),
    ])
    def test_resolution(self, source, config, key):
        assert self._map().dataset_key_for_job(source, config) == key

    def test_ambiguous_stays_unresolved(self):
        m = self._map()
        assert m.dataset_key_for_job("job:pe_mart_build", {}) is None
        # a multi-dataset producer is never overridden by the tables rule
        assert m.dataset_key_for_job("job:pe_mart_build", {"tables": ["pe_funds"]}) is None
        # a shared table resolves to nothing
        assert m.dataset_key_for_job("x", {"tables": ["container_freight_index"]}) is None

    def test_router_producer(self):
        m = self._map()
        assert m.producer_for_job("census_cbp", {}) == "api:census_cbp"
        assert m.producer_for_job("form_d", {}) == "api:form_d"
        assert m.producer_for_job("not_a_source", {}) is None

    def test_alias_table_is_valid(self):
        from app.catalog.job_keys import JOB_SOURCE_DATASETS, MAINTENANCE_SOURCES

        m = self._map()
        keys = {s.key for s in _catalog()}
        assert not set(JOB_SOURCE_DATASETS) & set(MAINTENANCE_SOURCES)
        for source, key in JOB_SOURCE_DATASETS.items():
            assert key in keys, key
            # an alias never shadows a real dispatch key or router producer
            assert source not in m.dispatch_keys, source
            assert f"api:{source}" not in m.by_base, source


# ---------------------------------------------------------------------------
# 7. PostgreSQL: pg_depend views and the mart ledger
# ---------------------------------------------------------------------------


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, inspect, text

    from app.catalog import lineage, usage_build

    engine = create_engine(PG_URL)
    ddl = runpy.run_path(str(REPO / "alembic/versions/0013_mart_build.py"))["UPGRADE_SQL"]
    created = []
    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))
        if "form_d_filings" not in inspect(engine).get_table_names():
            conn.execute(text("CREATE TABLE form_d_filings (accession_number text)"))
            created.append("form_d_filings")
        conn.execute(text("DROP VIEW IF EXISTS t143_v"))
        conn.execute(text("CREATE VIEW t143_v AS SELECT accession_number FROM form_d_filings"))
        ids = [conn.execute(text(
            "INSERT INTO core.mart_build (mart, status, dry_run, finished_at, inputs) "
            "VALUES (:m, 'success', false, now(), CAST(:i AS jsonb)) RETURNING id"),
            {"m": "pe_marts", "i": json.dumps([
                {"source": "sec_form_d", "kind": "bulk", "release_keys": ["2026q2"]},
                {"source": "sec_adv_roster", "kind": "bulk",
                 "release_keys": ["ria:2026-09-01"]}])}).scalar()]
    lineage.clear_cache()
    usage_build.clear_cache()
    yield engine
    lineage.clear_cache()
    usage_build.clear_cache()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM core.mart_build WHERE id = ANY(:ids)"), {"ids": ids})
        conn.execute(text("DROP VIEW IF EXISTS t143_v"))
        for t in created:
            conn.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))


@pg
class TestLivePg:
    def test_views_and_observed_edges(self, pg_engine):
        from app.catalog import lineage

        g = lineage.full_graph(pg_engine, live=True, refresh=True)
        assert g["observed"]["available"] is True
        assert g["views_source"] == "pg_depend+code"
        view = [e for e in g["edges"] if e["type"] == "view" and e["to"] == "view:t143_v"]
        assert view and view[0]["from"] == "table:form_d_filings"
        assert "pg_depend" in view[0]["origin"]
        obs = {(e["from"], e["to"]): e for e in g["edges"] if e["type"] == "observed"}
        e = obs[("dataset:sec_form_d", "dataset:pe_funds_sec")]
        assert e["release_keys"] == ["2026q2"] and e["mart"] == "pe_marts"
        assert ("dataset:sec_adv_roster", "dataset:pe_firms_sec") in obs
        w = lineage.walk(g, "sec_form_d", direction="down", depth=1)
        assert "t143_v" in w["views"]
