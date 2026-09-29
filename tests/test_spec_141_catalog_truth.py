"""
Tests for SPEC 141 — the catalog truth pass (PLAN_088 §3 "SPEC_134").

The 2026-09-25 verification checked every catalog entry against the live DB
and the ingestor code. Its per-entry output is checked in
(app/catalog/evidence/verification_2026-09-25.json) with a disposition for
every proposed field; these tests keep the catalog and that evidence in step,
and keep the new honesty fields (data_state, limitations, missing_tables,
row_filters, coverage_basis) meaningful.
"""
import json
import os
import re
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")
integration = pytest.mark.skipif(os.environ.get("RUN_INTEGRATION_TESTS", "").lower() != "true",
                                 reason="RUN_INTEGRATION_TESTS not set")

REPO = Path(__file__).resolve().parents[1]
EVIDENCE = REPO / "app" / "catalog" / "evidence" / "verification_2026-09-25.json"
BUG_ID = re.compile(r"\bBUG-[A-Z0-9-]+\b")
GENERIC_GRAIN = "one row per feature / record as published"


def _catalog():
    from app.catalog import get_catalog

    return get_catalog()


def _in_force(specs):
    """Keys whose committed REVIEWED hash equals the spec's current rights_hash (SPEC_142)."""
    from app.catalog.rights_reviewed import REVIEWED

    by_key = {s.key: s for s in specs}
    return {k for k, (h, _) in REVIEWED.items() if k in by_key and by_key[k].rights_hash == h}


def _evidence():
    return json.loads(EVIDENCE.read_text(encoding="utf-8"))


def _entries():
    return {e["key"]: e for e in _evidence()["entries"]}


def _base(**over):
    kw = dict(
        key="demo_dataset", source="sec", display_name="Demo",
        description="A demo dataset used only by the validation tests of SPEC 141.",
        kind="filings", grain="one row per filing", producer="bulk:sec_form_d",
        cadence="monthly", rerun="idempotent", license="public domain",
        redistribution="open", pii_class="none", origin="official",
        status_public="internal", tables=("form_d_filings", "form_d_issuers"),
    )
    kw.update(over)
    return kw


# ---------------------------------------------------------------------------
# T1 — evidence is traceable, and applied means equal
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEvidence:
    def test_evidence_covers_the_verified_entries(self):
        ev = _evidence()
        assert ev["verified_at"] == "2026-09-25"
        assert len(ev["entries"]) == 150
        keys = {s.key for s in _catalog()}
        for e in ev["entries"]:
            if e["key"] in keys:
                continue
            folded = {v for v in e["disposition"].values()}
            assert folded and all(v.startswith(("folded:", "retired")) for v in folded), e["key"]
            target = next(iter(folded)).split(":", 1)[1] if next(iter(folded)).startswith("folded:") else None
            assert target is None or target in keys, (e["key"], target)

    def test_every_proposal_has_a_disposition(self):
        bad = []
        for e in _evidence()["entries"]:
            fields = set(e["proposed"]) | {f for f, v in e["issues"].items() if v and f != "other"}
            missing = fields - set(e["disposition"])
            if missing:
                bad.append((e["key"], sorted(missing)))
            for field, d in e["disposition"].items():
                if not re.match(r"^(applied|waived:.+|amended:.+|deferred:(SPEC|BUG)[-_A-Z0-9]+.*|folded:[a-z0-9_]+)$", d):
                    bad.append((e["key"], field, d))
        assert not bad, bad

    def test_applied_means_the_spec_equals_the_proposal(self):
        from app.catalog import get_spec

        bad = []
        for e in _evidence()["entries"]:
            spec = get_spec(e["key"])
            for field in ("primary_key", "coverage_sql", "coverage_from", "inputs"):
                if e["disposition"].get(field) != "applied":
                    continue
                want = e["proposed"][field]
                got = getattr(spec, field)
                if isinstance(want, list):
                    want = tuple(want)
                if got != want:
                    bad.append((e["key"], field, got, want))
        assert not bad, bad

    def test_most_verified_fields_are_applied(self):
        """Scoreboard floor (PLAN_088 §1.1): the pass is not a no-op."""
        specs = _catalog()
        assert sum(1 for s in specs if s.coverage_sql) >= 120
        assert sum(1 for s in specs if s.primary_key) >= 140
        assert sum(1 for s in specs if s.coverage_from) >= 60
        assert all(s.verified_at for s in specs)


# ---------------------------------------------------------------------------
# T2/T3 — coverage SQL and grains
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCoverageAndGrain:
    def test_coverage_sql_is_read_only_with_a_basis(self):
        from sqlalchemy import text

        from app.catalog.spec import _WRITE_SQL, COVERAGE_BASES

        for s in _catalog():
            if s.coverage_sql is None:
                assert s.coverage_basis is None, s.key
                continue
            sql = s.coverage_sql.strip()
            assert sql.lower().startswith(("select", "with")), s.key
            assert ";" not in sql and not _WRITE_SQL.search(sql), s.key
            assert not text(sql)._bindparams, (s.key, "bind parameter in coverage_sql")
            assert s.coverage_basis in COVERAGE_BASES, s.key

    def test_fred_coverage_uses_least(self):
        from app.catalog import get_spec

        sql = get_spec("fred_series").coverage_sql
        assert sql.lower().startswith("select least(") and "greatest" not in sql.lower()

    def test_guards_are_kept(self):
        from app.catalog import get_spec

        assert "current_date" in get_spec("sec_companyfacts").coverage_sql
        assert "current_date" in get_spec("fema_hma_projects").coverage_sql
        assert "length(county_fips) = 5" in get_spec("si_national_risk_index").coverage_sql
        assert get_spec("sec_companyfacts").coverage_basis == "rolling"
        assert any("rolling" in lim for lim in get_spec("sec_companyfacts").limitations)

    def test_no_generic_grain(self):
        bad = [s.key for s in _catalog() if s.grain.strip() == GENERIC_GRAIN]
        assert not bad, f"specs still using the generic grain: {bad}"

    def test_descriptions_are_long_enough(self):
        assert all(len(s.description.strip()) >= 50 for s in _catalog())


# ---------------------------------------------------------------------------
# T4 — shared tables
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSharedTables:
    def test_shared_tables_are_filtered_or_explained(self):
        from app.catalog.datasets import SHARED_TABLES, SHARED_UNFILTERED

        claims = {}
        for s in _catalog():
            for t in s.tables:
                claims.setdefault(t, []).append(s)
        bad = []
        for t, specs in claims.items():
            if len(specs) < 2:
                continue
            filtered = all(t in dict(s.row_filters) for s in specs)
            if not filtered and t not in SHARED_UNFILTERED:
                bad.append((t, [s.key for s in specs]))
        assert not bad, f"shared tables with no row filter and no reason: {bad}"
        assert set(SHARED_UNFILTERED) <= set(SHARED_TABLES)

    def test_folds(self):
        from app.catalog import get_spec, producer_index

        assert get_spec("sec_company_financials") is None
        assert producer_index()["dispatch:sec:financial_data"] == "sec_companyfacts"
        for c in ("three_pl_fmcsa", "three_pl_sec", "three_pl_website"):
            assert producer_index()[f"collector:{c}"] == "si_3pl_companies"
        cbp = get_spec("census_cbp")
        assert cbp.producer == "api:census_cbp" and cbp.tables == ("census_cbp",)
        assert cbp.primary_key == ("year", "naics_code", "geo_level", "county_fips")
        assert "census_cbp" not in get_spec("rollup_market_scores").tables
        assert "census_cbp" in get_spec("rollup_market_scores").inputs
        acs = get_spec("census_acs_county_tract")
        assert acs.table_patterns == ("acs5_county_*", "acs5_tract_*")
        assert acs.producer_kind == "script"
        from app.catalog.live import resolve_tables

        existing = {"acs5_2023_b19013", "acs5_county_2023_b19013", "acs5_tract_2023_demand"}
        assert resolve_tables(acs, existing) == ["acs5_county_2023_b19013", "acs5_tract_2023_demand"]
        assert resolve_tables(get_spec("census_acs5"), existing) == ["acs5_2023_b19013"]
        assert get_spec("census_acs5").table_patterns == ("acs5_20*",)
        assert "lp_collection_runs" not in get_spec("lp_collection").tables
        assert get_spec("synthetic_lp_gp_universe").tables[0] == "lp_gp_relationships"

    def test_row_filters_scope_the_shared_rows(self):
        from app.catalog import get_spec

        assert dict(get_spec("si_drewry_wci").row_filters)["container_freight_index"] == \
            "provider = 'drewry'"
        assert dict(get_spec("synthetic_job_postings").row_filters)["job_postings"] == \
            "ats_type = 'synthetic'"
        assert "crd_number IS NULL" in dict(get_spec("pe_collection").row_filters)["pe_firms"]
        assert "crd_number IS NOT NULL" in dict(get_spec("pe_firms_sec").row_filters)["pe_firms"]


# ---------------------------------------------------------------------------
# T5/T6 — phantoms and empties are visible
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHonesty:
    def test_phantom_or_empty_is_archival_or_explained(self):
        from app.catalog import get_spec

        bad = []
        for key, e in _entries().items():
            spec = get_spec(key)
            if spec is None:
                continue
            declared = set(spec.tables) | set(spec.table_patterns)
            still_missing = [m for m in e["missing_tables"] if m.split(" ")[0] in declared]
            if e["approx_rows"] == 0 or still_missing:
                ok = spec.status_public in ("archival", "retired") or any(
                    BUG_ID.search(lim) for lim in spec.limitations)
                if not ok:
                    bad.append(key)
                assert spec.data_state != "ok", key
        assert not bad, bad

    def test_every_spec_states_its_data(self):
        from app.catalog.spec import DATA_STATES

        for s in _catalog():
            assert s.data_state in DATA_STATES, s.key
            if s.data_state != "ok":
                assert s.limitations, f"{s.key}: data_state {s.data_state} needs a limitation"

    def test_declared_tables_exist_in_code_or_are_flagged_missing(self):
        from app.catalog.tables import declared_tables

        from app.catalog.tables import package_source, producer_module, table_generated_by

        def generated(spec, table):
            texts = [package_source(m) for m in
                     filter(None, (producer_module(p) for p in spec.producers))]
            return any(table_generated_by(table, t) for t in texts)

        declared = declared_tables()
        bad = sorted({(s.key, t) for s in _catalog() for t in s.tables
                      if t not in declared and t not in s.missing_tables and not generated(s, t)})
        assert not bad, bad
        # the escape hatch is not vacuous
        assert generated(next(s for s in _catalog() if s.key == "fbi_crime_leoka"),
                         "fbi_crime_leoka_national")
        assert not generated(next(s for s in _catalog() if s.key == "fbi_crime_leoka"), "zzz_table")

    def test_missing_tables_match_the_evidence(self):
        from app.catalog import get_spec

        for key in ("eia_steo", "noaa_climate", "opencorporates", "vertical_prospects", "kaggle_m5"):
            s = get_spec(key)
            assert s.data_state == "missing_tables" and s.missing_tables, key
            assert s.status_public == "archival", key
        assert set(get_spec("kaggle_m5").missing_tables) == {"m5_sales", "m5_prices"}
        assert get_spec("osha").missing_tables == ("osha_violations",)

    @pytest.mark.parametrize("key, state", [
        ("si_seismic_hazard", "fabricated"),
        ("si_natural_gas_infra", "seeded"),
        ("si_grid_infrastructure", "sample_mixed"),
        ("si_intermodal_terminals", "placeholder"),
        ("glassdoor", "demo"),
        ("fema_pa_projects", "key_columns_null"),
        ("treasury_interest_rates", "stale"),
        ("fdic_bank_financials", "ok"),
    ])
    def test_data_state(self, key, state):
        from app.catalog import get_spec

        assert get_spec(key).data_state == state

    def test_origins(self):
        from app.catalog import get_spec

        for key in ("si_foreign_trade_zones", "si_incentive_programs", "si_certified_sites",
                    "si_natural_gas_infra", "glassdoor"):
            assert get_spec(key).origin == "curated", key
        assert get_spec("si_incentive_deals").origin == "synthetic"
        assert get_spec("public_lp_strategies").origin == "synthetic"
        assert get_spec("app_rankings").origin == "official"

    def test_status_demotions(self):
        from app.catalog import get_spec

        for key in ("eia_petroleum", "eia_natural_gas", "eia_electricity", "noaa_climate",
                    "fbi_crime_estimates", "usaspending_awards", "fema_pa_projects",
                    "cms_hospital_cost_reports", "si_motor_carriers", "uspto_patents"):
            assert get_spec(key).status_public == "archival", key
        # healthy scheduled datasets stay internal
        for key in ("sec_form_d", "fdic_bank_financials", "fema_disaster_declarations"):
            assert get_spec(key).status_public == "internal", key


# ---------------------------------------------------------------------------
# T7 — PII and rights tightenings
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPiiAndRights:
    @pytest.mark.parametrize("key, pii", [
        ("sec_13f", "business_contact"),
        ("medspa_prospects", "personal"),
        ("si_motor_carriers", "personal"),
        ("osha", "business_contact"),
        ("afdc_ev_stations", "none"),
        ("yelp_categories", "none"),
        ("sec_company_filings", "none"),
        ("public_lp_strategies", "none"),
        ("sam_gov_entities", "none"),
        # conservative classes left alone
        ("fda_device_registrations", "business_contact"),
        ("si_internet_exchanges", "business_contact"),
        ("si_certified_sites", "business_contact"),
        ("si_3pl_companies", "business_contact"),
    ])
    def test_pii(self, key, pii):
        from app.catalog import get_spec

        assert get_spec(key).pii_class == pii

    def test_site_intel_default_is_not_open(self):
        from app.catalog.rights import COLLECTOR_RIGHTS, SOURCE_RIGHTS, rights_for

        assert SOURCE_RIGHTS["site_intel"].redistribution == "internal_only"
        assert rights_for("new_thing", "site_intel", "brand_new_collector").redistribution == \
            "internal_only"
        # every existing collector declares its rights explicitly
        import importlib

        import app.sources.site_intel.runner as runner

        for d in ("incentives", "labor", "logistics", "power", "risk", "telecom",
                  "transport", "water_utilities"):
            importlib.import_module(f"app.sources.site_intel.{d}")
        missing = sorted(s.value for s in runner.COLLECTOR_REGISTRY if s.value not in COLLECTOR_RIGHTS)
        assert not missing, f"collectors inheriting the site_intel default: {missing}"

    def test_hifld_tightened(self):
        from app.catalog import get_spec

        s = get_spec("si_grid_infrastructure")
        assert s.redistribution == "restricted" and s.origin == "scraped"

    def test_nothing_published_and_reviewed_only_by_hash(self):
        cat = _catalog()
        in_force = _in_force(cat)
        assert {s.key for s in cat if s.reviewed} == in_force
        assert all(s.status_public not in ("ga", "beta") for s in cat)


# ---------------------------------------------------------------------------
# T8 — validation of the new fields
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNewFieldValidation:
    @pytest.mark.parametrize("over, fragment", [
        ({"description": "Thirty characters of text here."}, "50"),
        ({"coverage_sql": "SELECT max(filed_at) FROM form_d_filings"}, "coverage_basis"),
        ({"coverage_sql": "SELECT max(x) FROM t WHERE y = :y", "coverage_basis": "period"}, "bind"),
        ({"coverage_basis": "sometimes"}, "coverage_basis"),
        ({"coverage_from": "2020"}, "coverage_from"),
        ({"keywords": ("vibes",)}, "keyword"),
        ({"subtitle": "x" * 101}, "subtitle"),
        ({"spatial_coverage": "mars"}, "spatial_coverage"),
        ({"data_state": "fine"}, "data_state"),
        ({"data_state": "missing_tables"}, "missing_tables"),
        ({"missing_tables": ("form_d_issuers",)}, "missing_tables"),
        ({"data_state": "missing_tables", "missing_tables": ("nope",)}, "missing_tables"),
        ({"row_filters": (("nope", "x = 1"),)}, "row_filter"),
        ({"row_filters": (("form_d_filings", "x = 1; DROP TABLE y"),)}, "row_filter"),
        ({"row_filters": (("form_d_filings", "x IN (DELETE FROM y)"),)}, "row_filter"),
        ({"row_filters": (("form_d_filings", "x = :v"),)}, "row_filter"),
        ({"limitations": ("",)}, "limitation"),
        ({"status_public": "beta", "reviewed": True, "data_state": "stale",
          "limitations": ("stale",)}, "data_state"),
        ({"producer": "script:"}, "producer"),
    ])
    def test_rejects(self, over, fragment):
        from app.catalog.spec import DatasetSpec

        with pytest.raises(ValueError, match=fragment):
            DatasetSpec(**_base(**over))

    def test_accepts_and_serialises(self):
        from app.catalog.spec import DatasetSpec

        s = DatasetSpec(**_base(
            coverage_sql="SELECT max(filed_at)::date FROM form_d_filings", coverage_basis="period",
            coverage_from="2023-07-03", keywords=("filings",), spatial_coverage="US:state",
            subtitle="Short", limitations=("holdings = latest release only",),
            row_filters=(("form_d_filings", "cik IS NOT NULL"),), data_state="missing_tables",
            missing_tables=("form_d_issuers",), verified_at="2026-09-25",
            producer="script:ingest_acs_tract_demand"))
        d = s.to_dict()
        assert d["coverage_basis"] == "period" and d["keywords"] == ["filings"]
        assert d["limitations"] == ["holdings = latest release only"]
        assert d["row_filters"] == [{"table": "form_d_filings", "predicate": "cik IS NOT NULL"}]
        assert d["data_state"] == "missing_tables" and d["missing_tables"] == ["form_d_issuers"]
        assert d["spatial_coverage"] == "US:state" and d["subtitle"] == "Short"
        assert d["verified_at"] == "2026-09-25"
        assert "coverage_sql" not in d
        assert s.producer_kind == "script"

    def test_curated_origin_is_valid(self):
        from app.catalog.spec import ORIGINS

        assert "curated" in ORIGINS

    def test_mirror_ranks_every_origin(self):
        from app.catalog.mirror import ORIGIN_SEVERITY
        from app.catalog.spec import ORIGINS

        assert set(ORIGINS) == set(ORIGIN_SEVERITY)


# ---------------------------------------------------------------------------
# T9 — API exposes the fields
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from app.api.v1 import catalog as catalog_api
    from app.catalog.live import clear_cache
    from app.core.database import get_db

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    factory = sessionmaker(bind=engine)

    def _db():
        db = factory()
        try:
            yield db
        finally:
            db.close()

    app = FastAPI()
    app.include_router(catalog_api.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = _db
    clear_cache()
    yield TestClient(app)
    clear_cache()


@pytest.mark.unit
class TestApi:
    FIELDS = ("coverage_basis", "subtitle", "keywords", "spatial_coverage", "limitations",
              "row_filters", "data_state", "missing_tables", "verified_at")

    def test_list_exposes_new_fields(self, client):
        body = client.get("/api/v1/catalog").json()
        for d in body["datasets"]:
            for f in self.FIELDS:
                assert f in d, (d["key"], f)
            assert "coverage_sql" not in d
        steo = next(d for d in body["datasets"] if d["key"] == "eia_steo")
        assert steo["data_state"] == "missing_tables" and steo["missing_tables"] == ["eia_steo*"]
        assert any("BUG-EIA-ONCONFLICT" in lim for lim in steo["limitations"])

    def test_detail_exposes_new_fields(self, client):
        body = client.get("/api/v1/catalog/si_drewry_wci").json()
        assert body["row_filters"] == [{"table": "container_freight_index",
                                        "predicate": "provider = 'drewry'"}]
        assert body["live"]["tables"][0]["table"] == "container_freight_index"


# ---------------------------------------------------------------------------
# T10 — live stats (PostgreSQL)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_views_are_never_pattern_expanded():
    from app.catalog import get_spec
    from app.catalog.live import KNOWN_VIEWS, resolve_tables

    assert "fred_observations" in KNOWN_VIEWS
    got = resolve_tables(get_spec("fred_series"),
                         {"fred_interest_rates", "fred_commodities", "fred_observations"})
    assert "fred_observations" not in got


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    from app.catalog.live import clear_cache

    engine = create_engine(PG_URL)

    def _drop(conn):
        conn.execute(text("DROP VIEW IF EXISTS fred_observations"))
        for t in ("fred_interest_rates", "fred_commodities", "container_freight_index"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))

    with engine.begin() as conn:
        _drop(conn)
        conn.execute(text("CREATE TABLE fred_interest_rates (series_id TEXT, date DATE)"))
        conn.execute(text("INSERT INTO fred_interest_rates VALUES ('DFF', '2026-09-21'), "
                          "('DGS10', '2026-09-21')"))
        conn.execute(text("CREATE TABLE fred_commodities (series_id TEXT, date DATE)"))
        conn.execute(text("INSERT INTO fred_commodities VALUES ('X', '2026-03-01')"))
        conn.execute(text("CREATE VIEW fred_observations AS SELECT * FROM fred_interest_rates "
                          "UNION ALL SELECT * FROM fred_commodities"))
        conn.execute(text("CREATE TABLE container_freight_index (index_code TEXT, rate_date DATE, "
                          "provider TEXT)"))
        conn.execute(text("INSERT INTO container_freight_index VALUES "
                          "('WCI', '2026-09-18', 'drewry'), ('FBX', '2026-09-19', 'freightos'), "
                          "('FBX01', '2026-09-19', 'freightos')"))
    clear_cache()
    yield engine
    clear_cache()
    with engine.begin() as conn:
        _drop(conn)
    engine.dispose()


@pg
class TestLivePg:
    def test_views_are_not_counted(self, pg_engine):
        from app.catalog import get_spec
        from app.catalog.live import dataset_live, existing_tables

        assert "fred_observations" not in existing_tables(pg_engine)
        live = dataset_live(pg_engine, get_spec("fred_series"))
        names = {t["table"] for t in live["tables"]}
        assert "fred_observations" not in names
        assert live["rows_total"] == 3  # base tables only, not 6

    def test_row_filters_apply_to_counts(self, pg_engine):
        from app.catalog import get_spec
        from app.catalog.live import dataset_live

        drewry = dataset_live(pg_engine, get_spec("si_drewry_wci"))
        stat = drewry["tables"][0]
        assert stat["rows"] == 1 and stat["rows_exact"] is True
        assert stat["row_filter"] == "provider = 'drewry'"
        fbx = dataset_live(pg_engine, get_spec("si_freightos_fbx"))
        assert fbx["tables"][0]["rows"] == 2
        assert fbx["coverage_through"] == "2026-09-19"

    def test_filtered_count_never_falls_back_to_whole_table_estimate(self, pg_engine):
        from app.catalog.live import count_rows

        stat = count_rows(pg_engine, "container_freight_index", True, exact=False,
                          where="provider = 'drewry'")
        assert stat["rows"] is None and stat["rows_exact"] is False

    def test_fred_coverage_least(self, pg_engine):
        """least() over the category tables: a stale category shows."""
        from sqlalchemy import text

        with pg_engine.connect() as conn:
            got = conn.execute(text(
                "SELECT least((SELECT max(date) FROM fred_interest_rates), "
                "(SELECT max(date) FROM fred_commodities))")).scalar()
        assert str(got) == "2026-03-01"


@integration
def test_every_coverage_sql_runs_on_the_live_db():
    """Every coverage_sql returns a date/timestamp or NULL in under 2 s."""
    import time
    from datetime import date

    from sqlalchemy import text

    from app.core.database import get_engine

    engine = get_engine()
    slow, bad = [], []
    for s in _catalog():
        if not s.coverage_sql:
            continue
        started = time.monotonic()
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = 5000"))
            value = conn.execute(text(s.coverage_sql)).scalar()
        if time.monotonic() - started > 2:
            slow.append(s.key)
        if value is not None and not isinstance(value, date):
            bad.append((s.key, type(value).__name__))
        elif value is not None and _as_date(value) > date.today():
            bad.append((s.key, "coverage in the future", str(value)))
    assert not slow and not bad, (slow, bad)


def _as_date(value):
    from datetime import datetime

    return value.date() if isinstance(value, datetime) else value


@integration
def test_every_primary_key_column_exists_on_the_live_db():
    """primary_key names columns of tables[0] (pattern-only: of some resolved
    table). A key column the loader does not write yet must be named in a
    limitation (cms_drug_pricing: mftr_name, BUG-CMS-IDEMPOTENT)."""
    from sqlalchemy import text

    from app.catalog.live import existing_tables, resolve_tables
    from app.catalog.tables import split
    from app.core.database import get_engine

    engine = get_engine()
    existing = existing_tables(engine, {split(t)[0] for s in _catalog() for t in s.tables})
    bad = []

    def columns(conn, table):
        schema, name = split(table)
        return {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t"), {"s": schema, "t": name})}

    with engine.connect() as conn:
        for s in _catalog():
            if not s.primary_key:
                continue
            if s.tables:
                candidates = [s.tables[0]] if s.tables[0] in existing else []
            else:
                candidates = [t for t in resolve_tables(s, existing) if t in existing]
            if not candidates:
                continue  # missing_tables: nothing to check
            best = min(([c for c in s.primary_key if c not in columns(conn, t)], t)
                       for t in candidates)
            missing, table = best
            flagged = " ".join(s.limitations)
            if any(c not in flagged for c in missing):
                bad.append((s.key, table, missing))
    assert not bad, bad


# ---------------------------------------------------------------------------
# Review fixes (spec-141-fix)
# ---------------------------------------------------------------------------

# multi-table coverage that is still greatest(): the tables are one stream
# (the same run writes all of them), so the freshest is the coverage
GREATEST_OK = {
    "sec_company_filings": "10-K/10-Q/8-K come from one submissions pull per company; least() "
                           "would report the seasonal 10-K date",
    "cftc_cot": "the three report types come from the same weekly release",
    "app_rankings": "rankings and rating history are written by the same lookup",
}
LEAST_KEYS = ("fred_series", "bls_series", "fbi_crime_estimates", "irs_soi", "intl_oecd",
              "intl_bis", "usda_nass")


@pytest.mark.unit
class TestReviewFixes:
    def test_multi_table_coverage_uses_least(self):
        from app.catalog import get_spec

        for key in LEAST_KEYS:
            sql = get_spec(key).coverage_sql.lower()
            assert "least(" in sql and "greatest(" not in sql, key
            assert "union all" not in sql, key

    def test_no_new_gap_hiding_coverage(self):
        for s in _catalog():
            sql = (s.coverage_sql or "").lower()
            if "greatest(" in sql:
                assert s.key in GREATEST_OK, f"{s.key}: greatest() hides a stale table"
            if "union all" in sql:
                pytest.fail(f"{s.key}: a max over a UNION hides a stale table; use _least_of")

    def test_usda_coverage_is_capped_at_today(self):
        from app.catalog import get_spec

        sql = get_spec("usda_nass").coverage_sql
        assert "current_date" in sql

    def test_least_of_shape(self):
        from app.catalog.datasets import _least_of

        assert _least_of("SELECT 1", "SELECT 2") == "SELECT least((SELECT 1), (SELECT 2))"
        capped = _least_of("SELECT 1", cap_today=True)
        assert capped.startswith("SELECT CASE WHEN c > current_date THEN current_date ELSE c END")

    def test_amended_dispositions_match_least(self):
        amended = {e["key"] for e in _evidence()["entries"]
                   if e["disposition"].get("coverage_sql", "").startswith("amended:")}
        assert amended == set(LEAST_KEYS) - {"fred_series"}

    def test_seismic_origin_is_not_official(self):
        from app.catalog import get_spec

        s = get_spec("si_seismic_hazard")
        assert s.origin == "synthetic" and s.data_state == "fabricated"

    def test_other_recommendations_have_a_disposition(self):
        """An evidence 'other' note that names origin or status has a
        disposition, so an omission like the seismic origin is recorded."""
        pats = {"origin": re.compile(r"\borigin\b", re.I),
                "status": re.compile(r"\bstatus\b|archival|retire", re.I)}
        bad = []
        for e in _evidence()["entries"]:
            for o in e["issues"].get("other") or []:
                for field, pat in pats.items():
                    if pat.search(o) and f"other.{field}" not in e["disposition"]:
                        bad.append((e["key"], field))
        assert not bad, sorted(set(bad))

    def test_applied_origin_recommendations_moved_off_official(self):
        from app.catalog import get_spec

        for e in _evidence()["entries"]:
            if e["disposition"].get("other.origin") != "applied":
                continue
            text_ = " ".join(o for o in e["issues"]["other"] if "origin" in o.lower())
            if "should not be" in text_ or "rather than 'official'" in text_ \
                    or "not be 'official'" in text_:
                assert get_spec(e["key"]).origin != "official", e["key"]

    def test_unverified_key_gets_no_default_state(self):
        from app.catalog.datasets import _ds

        s = _ds("zz_new_dataset", "sec", "New", "A dataset added after the verification pass, "
                "with no evidence entry.", "filings", "one row per filing", "bulk:sec_form_d",
                "monthly", tables=("form_d_filings",))
        assert s.data_state is None and s.verified_at is None
        v = _ds("sec_form_d", "sec", "Form D", "A dataset the 2026-09-25 verification checked, "
                "so it defaults to that date.", "filings", "one row per filing",
                "bulk:sec_form_d", "monthly", tables=("form_d_filings",))
        assert v.data_state == "ok" and v.verified_at == "2026-09-25"

    def test_every_spec_has_explicit_or_verified_state(self):
        from app.catalog.datasets import _verified_keys

        unverified = [s.key for s in _catalog() if s.key not in _verified_keys()]
        # the specs added after the evidence file state it themselves
        assert set(unverified) == {"census_cbp", "census_acs_county_tract",
                                   "census_cbp_county_yearly",
                                   "zip_medspa_scores"}  # SPEC_142 fix: split out of medspa_prospects
        for s in _catalog():
            assert s.verified_at and s.data_state is not None, s.key

    def test_census_cbp_county_yearly_is_declared(self):
        from app.catalog import get_spec

        s = get_spec("census_cbp_county_yearly")
        assert s.tables == ("census_cbp_county_yearly",)
        assert s.primary_key == ("year", "geo_id", "naics_code")
        assert s.producer == "script:ingest_cbp_county" and s.data_state == "ok"


@pytest.mark.unit
class TestMirrorFlags:
    def test_block_carries_worst_state_and_all_limitations(self):
        from app.catalog import get_spec
        from app.catalog.mirror import catalog_block

        b = catalog_block(get_spec("si_seismic_hazard"))
        assert b["data_state"] == "fabricated"
        assert any("BUG-SEISMIC-FABRICATED" in lim for lim in b["limitations"])
        assert b["origin"] == "synthetic"

    def test_merge_takes_the_most_severe_state(self):
        from dataclasses import replace

        from app.catalog import get_spec
        from app.catalog.mirror import DATA_STATE_SEVERITY, merge_flags

        from app.catalog.spec import DATA_STATES

        assert set(DATA_STATE_SEVERITY) == set(DATA_STATES)
        a = replace(get_spec("sec_form_d"), data_state="stale", limitations=("a", "b"))
        b = replace(get_spec("sec_form_d"), data_state="seeded", limitations=("b", "c"))
        got = merge_flags([a, b])
        assert got == {"data_state": "seeded", "limitations": ["a", "b", "c"]}

    def test_shared_seeded_table_is_flagged(self):
        """job_postings: scraped rows plus the (empty) synthetic writer."""
        from app.catalog.mirror import catalog_block, table_writers

        specs = _catalog()
        ws = table_writers(specs, {"job_postings"})["job_postings"]
        b = catalog_block(ws[0], ws)
        assert b["data_state"] is not None
        assert set(b["limitations"]) >= {lim for w in ws for lim in w.limitations}
