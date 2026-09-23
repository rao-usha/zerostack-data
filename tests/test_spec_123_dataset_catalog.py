"""
Tests for SPEC 123 — the dataset catalog.

Six catalogs competed (dataset_registry, SOURCE_DISPATCH, BULK_SOURCES,
COLLECTOR_REGISTRY, SOURCE_REGISTRY, API_REGISTRY) and none covered the SEC
bulk, entity and mart datasets together. The catalog is now the one declared
list; these tests are what keeps it complete: a new producer without a
DatasetSpec, or a DatasetSpec naming a table nobody declares, fails here with
the full list of offenders.
"""
import ast
import dataclasses
import importlib
import inspect
import os
from datetime import datetime
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]

SITE_INTEL_DOMAINS = ("incentives", "labor", "logistics", "power", "risk", "telecom",
                      "transport", "water_utilities")
# queue job types that only route to producers declared elsewhere
ROUTING_JOB_TYPES = {"ingestion": "dispatch", "bulk_ingest": "bulk", "site_intel": "collector"}
SPECIAL_DISPATCH = ("census", "public_lp_strategies")  # special-cased in jobs.run_ingestion_job


def _catalog():
    from app.catalog import get_catalog

    return get_catalog()


def _claims(producer):
    """Specs that name ``producer`` as primary or also_produced_by."""
    return [s.key for s in _catalog() if producer in s.producers]


def _collectors():
    import app.sources.site_intel.runner as runner

    for d in SITE_INTEL_DOMAINS:
        importlib.import_module(f"app.sources.site_intel.{d}")
    return runner.COLLECTOR_REGISTRY


def _base(**over):
    kw = dict(
        key="demo_dataset", source="sec", display_name="Demo",
        description="A demo dataset used only by the validation tests.",
        kind="filings", grain="one row per filing", producer="bulk:sec_form_d",
        cadence="monthly", rerun="idempotent", license="public domain",
        redistribution="open", pii_class="none", origin="official",
        status_public="internal", tables=("form_d_filings",),
    )
    kw.update(over)
    return kw


# ---------------------------------------------------------------------------
# T1/T2 — the dataclass
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatasetSpec:
    def test_valid_spec_is_frozen(self):
        from app.catalog.spec import DatasetSpec

        spec = DatasetSpec(**_base())
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.kind = "holdings"
        assert spec.producer_kind == "bulk"
        assert spec.producers == ("bulk:sec_form_d",)

    @pytest.mark.parametrize("over, fragment", [
        ({"key": "Bad-Key"}, "slug"),
        ({"kind": "cube"}, "kind"),
        ({"rerun": "sometimes"}, "rerun"),
        ({"redistribution": "public"}, "redistribution"),
        ({"pii_class": "secret"}, "pii_class"),
        ({"origin": "vibes"}, "origin"),
        ({"status_public": "live"}, "status_public"),
        ({"producer": "cron:nightly"}, "producer"),
        ({"producer": "bulk:"}, "producer"),
        ({"also_produced_by": ("bulk:sec_form_d",)}, "repeated"),
        ({"tables": ("form d filings",)}, "table"),
        ({"tables": (), "table_patterns": ()}, "declare tables"),
        ({"tables": (), "table_patterns": ("fred_%",)}, "pattern"),
        ({"tables": ("a", "a")}, "duplicate"),
        ({"description": "short"}, "description"),
        ({"license": " "}, "license"),
        ({"coverage_sql": "DELETE FROM form_d_filings"}, "SELECT"),
        ({"coverage_sql": "SELECT 1; DROP TABLE users"}, "single statement"),
        ({"coverage_sql": "SELECT max(x) FROM t WHERE (UPDATE t SET x = 1) IS NULL"}, "read-only"),
        ({"status_public": "ga"}, "reviewed"),
        ({"status_public": "beta", "reviewed": True, "origin": "synthetic"}, "synthetic"),
        ({"inputs": ("demo_dataset",)}, "own input"),
        ({"slo_lag_hours": 0}, "positive"),
    ])
    def test_rejects(self, over, fragment):
        from app.catalog.spec import DatasetSpec

        with pytest.raises(ValueError, match=fragment):
            DatasetSpec(**_base(**over))

    def test_effective_redistribution_internal_until_reviewed(self):
        """T2: 'open' is a fact about the licence; nothing leaves until a human reviewed it."""
        from app.catalog.spec import DatasetSpec

        assert DatasetSpec(**_base()).effective_redistribution == "internal_only"
        assert DatasetSpec(**_base(reviewed=True)).effective_redistribution == "open"
        d = DatasetSpec(**_base()).to_dict()
        assert d["rights"] == {"license": "public domain", "redistribution": "open",
                               "effective_redistribution": "internal_only",
                               "attribution": None, "reviewed": False}
        assert "coverage_sql" not in d and d["has_coverage_sql"] is False

    def test_catalog_builds_and_is_unique(self):
        from app.catalog import get_spec, producer_index

        specs = _catalog()
        keys = [s.key for s in specs]
        assert len(keys) == len(set(keys))
        assert len(specs) >= 140
        assert get_spec("sec_form_d").producer == "bulk:sec_form_d"
        idx = producer_index()
        assert idx["dispatch:irs_soi:all"] == "irs_soi"
        assert idx["job:pe_mart_build#firms"] == "pe_firms_sec"


# ---------------------------------------------------------------------------
# T3/T4/T5 — coverage of every producer and every legacy registry
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProducerCoverage:
    def test_every_bulk_source_maps_to_exactly_one_dataset(self):
        from app.ingest.bulk.registry import BULK_SOURCES, load_all

        load_all()
        assert len(BULK_SOURCES) >= 8
        bad = {n: _claims(f"bulk:{n}") for n in BULK_SOURCES if len(_claims(f"bulk:{n}")) != 1}
        assert not bad, f"bulk sources not mapped to exactly one DatasetSpec: {bad}"

    def test_every_dispatch_key_maps_to_exactly_one_dataset(self):
        from app.api.v1.jobs import SOURCE_DISPATCH

        keys = list(SOURCE_DISPATCH) + list(SPECIAL_DISPATCH)
        assert len(SOURCE_DISPATCH) >= 100
        bad = {k: _claims(f"dispatch:{k}") for k in keys if len(_claims(f"dispatch:{k}")) != 1}
        assert not bad, f"SOURCE_DISPATCH keys not mapped to exactly one DatasetSpec: {bad}"

    def test_every_collector_maps_to_exactly_one_dataset(self):
        registry = _collectors()
        assert len(registry) >= 40
        bad = {s.value: _claims(f"collector:{s.value}") for s in registry
               if len(_claims(f"collector:{s.value}")) != 1}
        assert not bad, f"site-intel collectors not mapped to exactly one DatasetSpec: {bad}"

    def test_every_data_writing_job_type_has_datasets(self):
        from app.core.models_queue import QueueJobType

        producers = [p for s in _catalog() for p in s.producers if p.startswith("job:")]
        missing = []
        for jt in QueueJobType:
            mine = [p for p in producers if p.split("#")[0] == f"job:{jt.value}"]
            if jt.value in ROUTING_JOB_TYPES:
                assert not mine, f"routing job type {jt.value} must not own a dataset: {mine}"
            elif not mine:
                missing.append(jt.value)
        assert not missing, f"job types that write data but have no DatasetSpec: {missing}"
        # a staged job type either uses stages throughout or not at all
        for jt in ("pe_mart_build", "entity_resolve"):
            assert f"job:{jt}" not in producers
            assert len([p for p in producers if p.startswith(f"job:{jt}#")]) >= 3

    def test_every_producer_names_a_real_producer(self):
        """T4: the reverse check — a renamed collector or dispatch key fails here."""
        from app.api.v1.jobs import SOURCE_DISPATCH
        from app.core.models_queue import QueueJobType
        from app.ingest.bulk.registry import BULK_SOURCES, load_all

        load_all()
        collectors = {s.value for s in _collectors()}
        job_types = {t.value for t in QueueJobType} - set(ROUTING_JOB_TYPES)
        dispatch = set(SOURCE_DISPATCH) | set(SPECIAL_DISPATCH)
        bad = []
        for spec in _catalog():
            for p in spec.producers:
                kind, _, rest = p.partition(":")
                name = rest.split("#", 1)[0]
                ok = {
                    "bulk": lambda: name in BULK_SOURCES,
                    "dispatch": lambda: name in dispatch,
                    "collector": lambda: name in collectors,
                    "job": lambda: name in job_types,
                    "api": lambda: (REPO / "app" / "api" / "v1" / f"{name}.py").is_file(),
                }[kind]()
                if not ok:
                    bad.append((spec.key, p))
        assert not bad, f"DatasetSpec producers that do not exist: {bad}"

    def test_source_and_api_registries_are_folded(self):
        """T5: every SOURCE_REGISTRY / API_REGISTRY key is a catalog source, an
        alias of one, or explicitly not a dataset."""
        from app.catalog.datasets import NON_DATASET_SOURCES, SOURCE_ALIASES
        from app.core.api_registry import API_REGISTRY
        from app.core.source_registry import SOURCE_REGISTRY

        sources = {s.source for s in _catalog()}
        assert set(SOURCE_ALIASES.values()) <= sources
        unfolded = sorted(
            k for k in set(SOURCE_REGISTRY) | set(API_REGISTRY)
            if k not in sources and k not in SOURCE_ALIASES and k not in NON_DATASET_SOURCES
        )
        assert not unfolded, f"registry keys with no dataset in the catalog: {unfolded}"
        assert not set(NON_DATASET_SOURCES) & sources

    def test_inputs_reference_catalog_datasets(self):
        from app.catalog import get_spec

        assert get_spec("pe_funds_sec").inputs == ("sec_form_d", "sec_adv_private_funds", "pe_firms_sec")
        assert "sec_adv_schedule_d" in get_spec("sec_adv_private_funds").inputs
        for spec in _catalog():
            for i in spec.inputs:
                assert get_spec(i) is not None, (spec.key, i)


# ---------------------------------------------------------------------------
# T6/T7 — tables
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTables:
    def test_every_table_is_declared_in_code(self):
        from app.catalog.tables import declared_tables

        declared = declared_tables()
        # the scan must see all three kinds of declaration
        assert {"pe_firms", "sec_13f_holdings", "core.entity", "courtlistener_dockets"} <= declared
        missing = sorted({(s.key, t) for s in _catalog() for t in s.tables if t not in declared})
        assert not missing, f"DatasetSpec tables declared nowhere (model / DDL / ddl()): {missing}"

    def test_every_pattern_is_generated_by_its_producer(self):
        from app.catalog.tables import package_source, pattern_generated_by, producer_module

        bad = []
        for spec in _catalog():
            texts = [package_source(m) for m in
                     filter(None, (producer_module(p) for p in spec.producers))]
            for pattern in spec.table_patterns:
                if not any(pattern_generated_by(pattern, t) for t in texts):
                    bad.append((spec.key, pattern))
        assert not bad, f"table patterns no producer generates: {bad}"

    def test_pattern_check_is_not_vacuous(self):
        from app.catalog.tables import package_source, pattern_generated_by

        fred = package_source("app.sources.fred.ingest")
        assert pattern_generated_by("fred_*", fred)
        assert not pattern_generated_by("eia_*", fred)
        census = package_source("app.sources.census.ingest")
        assert pattern_generated_by("acs5_*", census)
        assert not pattern_generated_by("zzz_*", census)

    def test_pattern_like_escapes_underscores(self):
        from app.catalog.tables import expand, pattern_like

        assert pattern_like("fred_*") == "fred\\_%"
        assert expand(["fred_*"], ["fred_rates", "fredx", "core.fred_a", "fred_"]) == ["fred_", "fred_rates"]

    def test_bulk_specs_cover_their_ddl_tables(self):
        """T7: tables are declared by hand, but a loader that grows a table fails here."""
        from app.catalog import dataset_for_producer
        from app.catalog.tables import bulk_tables
        from app.ingest.bulk.registry import BULK_SOURCES, load_all

        load_all()
        bad = {}
        for name, cls in BULK_SOURCES.items():
            spec = dataset_for_producer(f"bulk:{name}")
            extra = bulk_tables(cls) - set(spec.tables)
            if extra:
                bad[name] = sorted(extra)
        assert not bad, f"bulk ddl() tables missing from their DatasetSpec: {bad}"

    def test_collector_specs_cover_the_models_they_upsert(self):
        from app.catalog import dataset_for_producer

        bad = {}
        for source, cls in _collectors().items():
            mod = importlib.import_module(cls.__module__)
            tree = ast.parse(inspect.getsource(mod))
            written = set()
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr in ("bulk_upsert", "null_preserving_upsert")
                        and node.args and isinstance(node.args[0], ast.Name)):
                    tn = getattr(getattr(mod, node.args[0].id, None), "__tablename__", None)
                    if tn:
                        written.add(tn)
            spec = dataset_for_producer(f"collector:{source.value}")
            assert written, f"no upserts found for {source.value} (scan broken?)"
            if written - set(spec.tables):
                bad[source.value] = sorted(written - set(spec.tables))
        assert not bad, f"collector tables missing from their DatasetSpec: {bad}"


# ---------------------------------------------------------------------------
# T8/T9 — rights and status
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRightsAndStatus:
    def test_every_source_has_explicit_rights(self):
        from app.catalog.rights import SOURCE_RIGHTS

        missing = sorted({s.source for s in _catalog()} - set(SOURCE_RIGHTS))
        assert not missing, f"sources without an explicit SourceRights entry: {missing}"

    def test_rights_lookup_refuses_unknown_source(self):
        from app.catalog.rights import rights_for

        with pytest.raises(KeyError, match="no SourceRights"):
            rights_for("x", "made_up_source")

    def test_restricted_vendors_are_never_open(self):
        from app.catalog import get_spec

        for source in ("yelp", "kaggle", "foot_traffic", "opencorporates", "web_traffic",
                       "fred", "github", "glassdoor", "app_rankings", "dunl",
                       "prediction_markets", "medspa_discovery", "vertical_discovery"):
            specs = [s for s in _catalog() if s.source == source]
            assert specs, source
            for s in specs:
                assert s.redistribution in ("restricted", "internal_only"), (s.key, s.redistribution)
        # FRED: third-party copyrighted series
        assert get_spec("fred_series").redistribution == "restricted"
        # vendor collectors inside the otherwise-public site_intel family
        for key in ("si_drewry_wci", "si_freightos_fbx", "si_scfi", "si_warehouse_listings"):
            assert get_spec(key).redistribution == "restricted", key

    def test_nothing_leaves_before_review(self):
        for s in _catalog():
            assert not s.reviewed, f"{s.key} marked reviewed without a human review"
            assert s.effective_redistribution == "internal_only"
            assert s.status_public in ("internal", "archival"), s.key

    def test_us_government_data_is_open_but_unreviewed(self):
        from app.catalog import get_spec

        for key in ("sec_form_d", "sec_13f", "treasury_daily_balance", "bls_series",
                    "si_flood_zones"):
            s = get_spec(key)
            assert s.redistribution == "open" and s.origin == "official" and not s.reviewed, key

    def test_pii_and_origin(self):
        from app.catalog import get_spec

        assert get_spec("sec_insider").pii_class == "personal"
        assert get_spec("sec_form_d").pii_class == "personal"
        assert get_spec("nppes_providers").pii_class == "personal"
        assert get_spec("pe_people_sec").pii_class == "personal"
        assert get_spec("people_org_charts").pii_class == "personal"
        assert get_spec("sec_13f").pii_class == "none"
        assert get_spec("people_org_charts").origin == "llm_extracted"
        assert get_spec("pe_funds_sec").origin == "derived"
        for s in _catalog():
            if s.source == "synthetic":
                assert s.origin == "synthetic" and s.redistribution == "internal_only"

    def test_batch_schedule_set_matches_batch_service(self):
        """The archival rule reads a declared set; it must equal what the
        nightly batch actually launches (resolved like _run_dispatched_job)."""
        from app.api.v1.jobs import SOURCE_DISPATCH
        from app.catalog.datasets import BATCH_SCHEDULED_DISPATCH
        from app.core.batch_service import TIERS

        derived = set()
        for tier in TIERS:
            for sd in tier.sources:
                base = sd.key.split(":")[0]
                ds = (sd.default_config or {}).get("dataset")
                if ":" in sd.key:
                    derived.add(sd.key)
                elif ds and f"{base}:{ds}" in SOURCE_DISPATCH:
                    derived.add(f"{base}:{ds}")
                else:
                    derived.add(base)
        assert derived == set(BATCH_SCHEDULED_DISPATCH)

    def test_dormant_api_sources_are_archival(self):
        from app.catalog import get_spec
        from app.catalog.datasets import BATCH_SCHEDULED_DISPATCH

        for s in _catalog():
            keys = {p.split(":", 1)[1] for p in s.producers if p.startswith("dispatch:")}
            if s.producer_kind == "dispatch":
                expected = "internal" if keys & BATCH_SCHEDULED_DISPATCH else "archival"
                assert s.status_public == expected, (s.key, s.status_public)
            if s.producer_kind in ("bulk", "collector") or s.producer.startswith(
                    ("job:pe_mart_build", "job:entity_resolve")):
                assert s.status_public == "internal", s.key
        assert get_spec("yelp_businesses").status_public == "archival"
        assert get_spec("treasury_daily_balance").status_public == "internal"
        assert get_spec("github_analytics").status_public == "archival"


# ---------------------------------------------------------------------------
# T10 — API (no Postgres: list is static, detail counts on SQLite)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from app.api.v1 import catalog as catalog_api
    from app.catalog.live import clear_cache
    from app.core.database import get_db

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE courtlistener_dockets (id INTEGER PRIMARY KEY)"))
        conn.execute(text("INSERT INTO courtlistener_dockets (id) VALUES (1), (2)"))
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
    def test_list_all(self, sqlite_client):
        body = sqlite_client.get("/api/v1/catalog").json()
        assert body["count"] == body["total"] == len(_catalog())
        entry = next(d for d in body["datasets"] if d["key"] == "sec_13f")
        assert entry["kind"] == "holdings" and entry["producer"] == "bulk:sec_13f"
        assert entry["rights"]["effective_redistribution"] == "internal_only"
        assert "coverage_sql" not in entry  # SQL is internal

    @pytest.mark.parametrize("params, has, lacks", [
        ({"kind": "holdings"}, "sec_13f", "sec_form_d"),
        ({"source": "sec"}, "sec_form_d", "fred_series"),
        ({"status_public": "archival"}, "yelp_businesses", "sec_form_d"),
        ({"redistribution": "restricted"}, "fred_series", "sec_form_d"),
        ({"kind": "entity", "source": "entity_master"}, "entity_master", "people_org_charts"),
        ({"q": "private fund"}, "sec_adv_private_funds", "fred_series"),
    ])
    def test_filters(self, sqlite_client, params, has, lacks):
        body = sqlite_client.get("/api/v1/catalog", params=params).json()
        keys = {d["key"] for d in body["datasets"]}
        assert has in keys and lacks not in keys
        assert body["count"] == len(keys) < body["total"]
        for d in body["datasets"]:
            for k, v in params.items():
                if k in ("kind", "source", "status_public"):
                    assert d[k] == v
                elif k == "redistribution":
                    assert d["rights"]["redistribution"] == v

    def test_bad_filter_is_422(self, sqlite_client):
        assert sqlite_client.get("/api/v1/catalog", params={"kind": "cube"}).status_code == 422
        assert sqlite_client.get("/api/v1/catalog",
                                 params={"redistribution": "public"}).status_code == 422

    def test_unknown_key_is_404(self, sqlite_client):
        assert sqlite_client.get("/api/v1/catalog/nope").status_code == 404

    def test_detail_counts_rows(self, sqlite_client):
        body = sqlite_client.get("/api/v1/catalog/courtlistener_dockets").json()
        assert body["key"] == "courtlistener_dockets"
        assert body["live"]["tables"] == [
            {"table": "courtlistener_dockets", "exists": True, "rows": 2, "rows_exact": True}]
        assert body["live"]["rows_total"] == 2
        missing = sqlite_client.get("/api/v1/catalog/osha").json()["live"]["tables"]
        assert {t["table"] for t in missing} == {"osha_inspections", "osha_violations"}
        assert all(t["exists"] is False and t["rows"] is None for t in missing)

    def test_router_registered_with_user_auth(self):
        main = (REPO / "app" / "main.py").read_text(encoding="utf-8")
        assert 'app.include_router(catalog.router, prefix="/api/v1", dependencies=_auth)' in main
        assert '"name": "catalog"' in main
        assert "sync_dataset_registry(engine)" in main


# ---------------------------------------------------------------------------
# PostgreSQL-backed — T11 live stats, T12 timeout, T13 mirror
# ---------------------------------------------------------------------------

# the test DB is shared with other suites: drop what these tests assert is absent
_PG_TABLES = ("form_d_filings", "form_d_issuers", "fred_interest_rates", "fred_gdp_x",
              "legacy_thing", "dataset_registry", "catalog_slow_v")


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    from app.catalog.live import clear_cache
    from app.core.models import Base, DatasetRegistry

    engine = create_engine(PG_URL)

    def _drop(conn):
        conn.execute(text("DROP VIEW IF EXISTS catalog_slow_v"))
        for t in _PG_TABLES:
            if t != "catalog_slow_v":
                conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS core.entity CASCADE"))

    with engine.begin() as conn:
        _drop(conn)
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core"))
        conn.execute(text("CREATE TABLE form_d_filings (accession_number TEXT PRIMARY KEY, "
                          "filed_at TIMESTAMP)"))
        conn.execute(text("INSERT INTO form_d_filings VALUES ('a', '2026-06-30 10:00'), "
                          "('b', '2026-09-01 09:00'), ('c', '2025-01-02')"))
        conn.execute(text("CREATE TABLE fred_interest_rates (series_id TEXT, v NUMERIC)"))
        conn.execute(text("INSERT INTO fred_interest_rates VALUES ('DFF', 5.3)"))
        conn.execute(text("CREATE TABLE fred_gdp_x (series_id TEXT)"))
        conn.execute(text("CREATE TABLE core.entity (entity_id BIGINT PRIMARY KEY)"))
    Base.metadata.create_all(engine, tables=[DatasetRegistry.__table__])
    clear_cache()
    yield engine
    clear_cache()
    with engine.begin() as conn:
        _drop(conn)
    engine.dispose()


@pg
class TestLivePg:
    def test_detail_counts_coverage_and_patterns(self, pg_engine):
        """T11"""
        from app.catalog import get_spec
        from app.catalog.live import dataset_live

        live = dataset_live(pg_engine, get_spec("sec_form_d"))
        by_table = {t["table"]: t for t in live["tables"]}
        assert by_table["form_d_filings"] == {"table": "form_d_filings", "exists": True,
                                              "rows": 3, "rows_exact": True}
        assert by_table["form_d_issuers"]["exists"] is False
        assert live["coverage_through"] == "2026-09-01"
        assert live["coverage_error"] is None

        fred = dataset_live(pg_engine, get_spec("fred_series"))
        fred_tables = {t["table"]: t for t in fred["tables"]}
        assert {"fred_gdp_x", "fred_interest_rates"} <= set(fred_tables)
        assert fred_tables["fred_interest_rates"]["rows"] == 1
        assert fred_tables["fred_gdp_x"]["rows"] == 0

        ent = dataset_live(pg_engine, get_spec("entity_master"))
        assert {"table": "core.entity", "exists": True, "rows": 0,
                "rows_exact": True} in ent["tables"]
        # coverage SQL against a missing table degrades, it does not raise
        assert ent["coverage_through"] is None and ent["coverage_error"] == "unavailable"

    def test_cache_and_refresh(self, pg_engine):
        from sqlalchemy import text

        from app.catalog import get_spec
        from app.catalog.live import dataset_live

        spec = get_spec("sec_form_d")
        assert dataset_live(pg_engine, spec)["tables"][0]["rows"] == 3
        with pg_engine.begin() as conn:
            conn.execute(text("INSERT INTO form_d_filings VALUES ('d', '2026-09-02')"))
        assert dataset_live(pg_engine, spec)["tables"][0]["rows"] == 3  # cached
        assert dataset_live(pg_engine, spec, refresh=True)["tables"][0]["rows"] == 4

    def test_statement_timeout_falls_back_to_estimate(self, pg_engine):
        """T12: a count that exceeds the timeout returns an estimate, flagged inexact."""
        from sqlalchemy import text

        from app.catalog.live import count_rows

        with pg_engine.begin() as conn:
            conn.execute(text("CREATE VIEW catalog_slow_v AS SELECT pg_sleep(2)::text AS x"))
        started = datetime.utcnow()
        stat = count_rows(pg_engine, "catalog_slow_v", True, timeout_ms=100)
        assert (datetime.utcnow() - started).total_seconds() < 1.5
        assert stat["rows_exact"] is False
        assert count_rows(pg_engine, "form_d_filings", True)["rows_exact"] is True

    def test_api_detail_on_pg(self, pg_engine):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sqlalchemy.orm import sessionmaker

        from app.api.v1 import catalog as catalog_api
        from app.core.database import get_db

        factory = sessionmaker(bind=pg_engine)

        def _db():
            db = factory()
            try:
                yield db
            finally:
                db.close()

        app = FastAPI()
        app.include_router(catalog_api.router, prefix="/api/v1")
        app.dependency_overrides[get_db] = _db
        body = TestClient(app).get("/api/v1/catalog/sec_form_d").json()
        assert body["live"]["coverage_through"] == "2026-09-01"
        assert body["live"]["tables"][0]["rows"] == 3


@pg
class TestMirrorPg:
    def _rows(self, engine):
        from sqlalchemy import text

        with engine.connect() as conn:
            return {r["table_name"]: dict(r) for r in conn.execute(
                text("SELECT * FROM dataset_registry")).mappings()}

    def test_sync_creates_preserves_marks_and_is_idempotent(self, pg_engine):
        """T13"""
        import json

        from sqlalchemy import text

        from app.catalog.mirror import sync_dataset_registry

        legacy_ts = datetime(2026, 4, 16, 12, 0, 0)
        with pg_engine.begin() as conn:
            for table, source, ds, meta in (
                ("fred_interest_rates", "fred", "fred_interest_rates", {"series": ["DFF"]}),
                ("legacy_thing", "old", "legacy_thing", None),
            ):
                conn.execute(text(
                    "INSERT INTO dataset_registry (source, dataset_id, table_name, display_name, "
                    "source_metadata, created_at, last_updated_at) "
                    "VALUES (:s, :d, :t, :d, CAST(:m AS JSON), :ts, :ts)"),
                    {"s": source, "d": ds, "t": table,
                     "m": json.dumps(meta) if meta is not None else None, "ts": legacy_ts})

        first = sync_dataset_registry(pg_engine)
        rows = self._rows(pg_engine)

        # new rows for catalog tables that exist; none for tables that do not
        assert rows["form_d_filings"]["source"] == "sec"
        assert rows["form_d_filings"]["dataset_id"] == "sec_form_d"
        assert rows["form_d_filings"]["display_name"] == "SEC Form D offerings"
        assert rows["form_d_filings"]["source_metadata"]["catalog"]["key"] == "sec_form_d"
        assert rows["core.entity"]["dataset_id"] == "entity_master"
        assert rows["fred_gdp_x"]["dataset_id"] == "fred_series"  # pattern-expanded
        assert "form_d_issuers" not in rows

        # the legacy row keeps its identity, its metadata and its clock
        fred = rows["fred_interest_rates"]
        assert (fred["source"], fred["dataset_id"]) == ("fred", "fred_interest_rates")
        assert fred["last_updated_at"] == legacy_ts
        assert fred["source_metadata"]["series"] == ["DFF"]
        cat = fred["source_metadata"]["catalog"]
        assert cat["key"] == "fred_series" and cat["redistribution"] == "restricted"
        assert cat["effective_redistribution"] == "internal_only"

        # never deleted, only marked
        legacy = rows["legacy_thing"]
        assert legacy["source_metadata"] == {"catalog": {"in_catalog": False}}
        assert legacy["last_updated_at"] == legacy_ts

        assert first["inserted"] >= 3 and first["marked_not_in_catalog"] == 1
        assert first["updated"] == 1

        second = sync_dataset_registry(pg_engine)
        assert second == {"inserted": 0, "updated": 0,
                          "unchanged": first["inserted"] + first["updated"] + first["unchanged"],
                          "marked_not_in_catalog": 0}
        assert self._rows(pg_engine) == rows

    def test_existing_update_dataset_registry_still_works(self, pg_engine):
        """_update_dataset_registry (BaseSourceIngestor) keeps working on a mirrored row."""
        from sqlalchemy.orm import sessionmaker

        from app.catalog.mirror import sync_dataset_registry
        from app.core.ingest_base import BaseSourceIngestor

        sync_dataset_registry(pg_engine)

        class _Fred(BaseSourceIngestor):
            SOURCE_NAME = "fred"

        db = sessionmaker(bind=pg_engine)()
        try:
            entry = _Fred(db)._update_dataset_registry(
                dataset_id="fred_interest_rates", table_name="fred_interest_rates",
                source_metadata={"series": ["DGS10"]})
            assert entry.source == "fred"
        finally:
            db.close()
        rows = self._rows(pg_engine)
        # review fix: the ingestor's metadata replaces its own keys, the catalog block survives
        meta = rows["fred_interest_rates"]["source_metadata"]
        assert meta["series"] == ["DGS10"] and meta["catalog"]["key"] == "fred_series"
        assert sync_dataset_registry(pg_engine)["updated"] == 0


# ---------------------------------------------------------------------------
# Review fixes (spec-123-fix)
# ---------------------------------------------------------------------------


def _universe():
    """Every table name the code declares plus every concrete catalog table."""
    from app.catalog.tables import declared_tables

    return set(declared_tables()) | {t for s in _catalog() for t in s.tables}


@pytest.mark.unit
class TestPatternOverlap:
    """F1: a glob must never count another dataset's table."""

    def test_sec_8k_pattern_skips_bulk_8k_index(self):
        from app.catalog import get_spec
        from app.catalog.live import resolve_tables

        existing = {"sec_8k", "sec_8k_a", "sec_8k_index", "sec_10k", "sec_10q", "sec_s1"}
        got = resolve_tables(get_spec("sec_company_filings"), existing)
        assert "sec_8k_index" not in got
        assert set(got) == {"sec_8k", "sec_8k_a", "sec_10k", "sec_10q", "sec_s1"}
        # the bulk dataset still owns it
        assert "sec_8k_index" in resolve_tables(get_spec("sec_edgar_submissions"), existing)

    def test_usda_pattern_skips_site_intel_truck_rates(self):
        from app.catalog import get_spec
        from app.catalog.live import resolve_tables

        existing = {"usda_crop_production", "usda_livestock", "usda_truck_rate"}
        assert set(resolve_tables(get_spec("usda_nass"), existing)) == {
            "usda_crop_production", "usda_livestock"}

    def test_no_pattern_resolves_to_another_datasets_table(self):
        from app.catalog.live import claimed_tables, resolve_tables

        universe = _universe()
        claimed = claimed_tables(_catalog())
        bad = []
        for spec in _catalog():
            for t in resolve_tables(spec, universe, claimed):
                if t not in spec.tables and t in claimed:
                    bad.append((spec.key, t, sorted(claimed[t])))
        assert not bad, f"patterns that take another dataset's table: {bad}"

    def test_no_table_matches_two_datasets_patterns(self):
        from app.catalog.tables import pattern_matches

        bad = []
        for t in sorted(_universe()):
            owners = {s.key for s in _catalog()
                      if any(pattern_matches(p, t) for p in s.table_patterns)}
            if len(owners) > 1:
                bad.append((t, sorted(owners)))
        assert not bad, f"tables matched by several datasets' patterns: {bad}"

    def test_only_sec_8k_index_needs_the_exclusion(self):
        """Every other pattern is narrow enough on its own; keep it that way."""
        from app.catalog.tables import pattern_matches

        claimed = {t: s.key for s in _catalog() for t in s.tables}
        hits = sorted({(s.key, t) for s in _catalog() for p in s.table_patterns
                       for t in claimed if pattern_matches(p, t) and t not in s.tables})
        assert hits == [("sec_company_filings", "sec_8k_index")]


@pytest.mark.unit
class TestSharedTableRights:
    """F3: a table written by several datasets carries the most restrictive rights."""

    def _block(self, table):
        from app.catalog.mirror import catalog_block, table_writers

        ws = table_writers(_catalog(), {table})[table]
        return catalog_block(ws[0], ws)

    def test_shared_tables_are_declared(self):
        from app.catalog.datasets import SHARED_TABLES

        claims = {}
        for s in _catalog():
            for t in s.tables:
                claims.setdefault(t, []).append(s.key)
        shared = {t for t, ks in claims.items() if len(ks) > 1}
        assert shared == set(SHARED_TABLES), (
            f"undeclared shared tables: {sorted(shared - set(SHARED_TABLES))}; "
            f"stale entries: {sorted(set(SHARED_TABLES) - shared)}")

    def test_three_pl_company_is_restricted_and_llm_flagged(self):
        b = self._block("three_pl_company")
        assert b["redistribution"] == "restricted"
        assert b["pii_class"] == "business_contact"
        assert b["origin"] == "llm_extracted"
        assert set(b["origins"]) == {"llm_extracted", "scraped", "derived"}
        assert set(b["datasets"]) == {"si_3pl_fmcsa_enrichment", "si_3pl_sec_enrichment",
                                      "si_3pl_website_enrichment", "si_3pl_companies"}

    @pytest.mark.parametrize("table, origin, pii", [
        ("job_postings", "synthetic", "none"),
        ("lp_fund", "synthetic", "business_contact"),
        ("pe_firms", "llm_extracted", "personal"),
        ("pe_funds", "llm_extracted", "personal"),
    ])
    def test_worst_origin_and_pii_win(self, table, origin, pii):
        b = self._block(table)
        assert b["origin"] == origin and origin in b["origins"]
        assert b["pii_class"] == pii

    def test_merge_never_less_restrictive_than_any_writer(self):
        from app.catalog import get_spec
        from app.catalog.datasets import SHARED_TABLES
        from app.catalog.mirror import PII_RANK, REDISTRIBUTION_RANK

        for table in SHARED_TABLES:
            b = self._block(table)
            for key in b["datasets"]:
                s = get_spec(key)
                assert REDISTRIBUTION_RANK.index(b["redistribution"]) >= \
                    REDISTRIBUTION_RANK.index(s.redistribution), (table, key)
                assert PII_RANK.index(b["pii_class"]) >= PII_RANK.index(s.pii_class), (table, key)
                assert s.origin in b["origins"], (table, key)

    def test_single_writer_block_unchanged(self):
        from app.catalog import get_spec
        from app.catalog.mirror import catalog_block

        b = catalog_block(get_spec("sec_13f"))
        assert b["origin"] == "official" and b["origins"] == ["official"]
        assert "datasets" not in b

    def test_status_is_only_as_public_as_the_least_public_writer(self):
        from app.catalog.mirror import merge_rights
        from app.catalog.spec import DatasetSpec

        ga = DatasetSpec(**_base(status_public="ga", reviewed=True))
        beta = DatasetSpec(**_base(key="b", status_public="beta", reviewed=True))
        internal = DatasetSpec(**_base(key="c"))
        assert merge_rights([ga])["status_public"] == "ga"
        assert merge_rights([ga, beta])["status_public"] == "beta"
        assert merge_rights([ga, internal])["status_public"] == "internal"


@pytest.mark.unit
class TestPiiFixes:
    """F4: filer directory, entity master and physician utilization hold natural persons."""

    @pytest.mark.parametrize("key", ["sec_edgar_submissions", "entity_source_records",
                                     "entity_master", "cms_medicare_utilization"])
    def test_personal(self, key):
        from app.catalog import get_spec

        assert get_spec(key).pii_class == "personal"

    def test_other_cms_datasets_stay_none(self):
        from app.catalog import get_spec

        assert get_spec("cms_drug_pricing").pii_class == "none"


@pytest.mark.unit
class TestLiveBounds:
    """F7: refresh is admin-only; one request does a bounded amount of counting."""

    def _two_tables(self, client):
        from sqlalchemy import text

        from app.core.database import get_db

        gen = client.app.dependency_overrides[get_db]()
        db = next(gen)
        engine = db.get_bind()
        db.close()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE osha_inspections (id INTEGER)"))
            conn.execute(text("CREATE TABLE osha_violations (id INTEGER)"))
            conn.execute(text("INSERT INTO osha_violations VALUES (1)"))
        return engine

    def test_refresh_requires_admin(self, sqlite_client):
        from app.core.authz import current_principal

        sqlite_client.app.dependency_overrides[current_principal] = lambda: {"role": "user"}
        assert sqlite_client.get("/api/v1/catalog/courtlistener_dockets").status_code == 200
        r = sqlite_client.get("/api/v1/catalog/courtlistener_dockets", params={"refresh": True})
        assert r.status_code == 403
        sqlite_client.app.dependency_overrides[current_principal] = lambda: {"role": "admin"}
        r = sqlite_client.get("/api/v1/catalog/courtlistener_dockets", params={"refresh": True})
        assert r.status_code == 200

    def test_exact_counts_are_capped(self, sqlite_client):
        from app.catalog import get_spec
        from app.catalog.live import clear_cache, dataset_live

        engine = self._two_tables(sqlite_client)
        clear_cache()
        live = dataset_live(engine, get_spec("osha"), max_exact=1)
        assert [t["rows_exact"] for t in live["tables"]] == [True, False]
        assert live["rows_exact"] is False

    def test_deadline_stops_exact_counts(self, sqlite_client):
        from app.catalog import get_spec
        from app.catalog.live import clear_cache, dataset_live

        engine = self._two_tables(sqlite_client)
        clear_cache()
        live = dataset_live(engine, get_spec("osha"), deadline_s=0)
        assert all(t["rows_exact"] is False for t in live["tables"])

    def test_concurrent_requests_share_one_computation(self, sqlite_client, monkeypatch):
        import threading
        import time as _time

        from app.catalog import get_spec
        from app.catalog import live as live_mod

        engine = self._two_tables(sqlite_client)
        live_mod.clear_cache()
        calls = []
        real = live_mod.count_rows

        def slow(*a, **kw):
            calls.append(a[1])
            _time.sleep(0.2)
            return real(*a, **kw)

        monkeypatch.setattr(live_mod, "count_rows", slow)
        spec = get_spec("osha")
        threads = [threading.Thread(target=live_mod.dataset_live, args=(engine, spec))
                   for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(calls) == 2  # two tables, counted once


@pg
class TestMirrorScopePg:
    """F2/F5/F6: catalog-only rows stay out of DQ; the block survives ingest writes."""

    def _session(self, engine):
        from sqlalchemy.orm import sessionmaker

        return sessionmaker(bind=engine)()

    def test_inserted_rows_are_catalog_only(self, pg_engine):
        from app.catalog.mirror import sync_dataset_registry
        from app.core.models import CATALOG_ONLY_TS, DatasetRegistry

        sync_dataset_registry(pg_engine)
        db = self._session(pg_engine)
        try:
            row = db.query(DatasetRegistry).filter_by(table_name="form_d_filings").one()
            assert row.last_updated_at == CATALOG_ONLY_TS
            assert row.created_at > CATALOG_ONLY_TS
            ingested = {r.table_name for r in db.query(DatasetRegistry)
                        .filter(DatasetRegistry.ingested())}
            assert "form_d_filings" not in ingested and "core.entity" not in ingested
        finally:
            db.close()

    def test_ingestor_touch_makes_row_ingested(self, pg_engine):
        from app.catalog.mirror import sync_dataset_registry
        from app.core.ingest_base import BaseSourceIngestor
        from app.core.models import DatasetRegistry

        sync_dataset_registry(pg_engine)

        class _Sec(BaseSourceIngestor):
            SOURCE_NAME = "sec"

        db = self._session(pg_engine)
        try:
            _Sec(db)._update_dataset_registry(dataset_id="sec_form_d", table_name="form_d_filings")
        finally:
            db.close()
        # a re-sync keeps the ingestor's clock
        sync_dataset_registry(pg_engine)
        db = self._session(pg_engine)
        try:
            assert db.query(DatasetRegistry).filter(
                DatasetRegistry.ingested(), DatasetRegistry.table_name == "form_d_filings",
            ).count() == 1
        finally:
            db.close()

    def test_quality_gate_skips_catalog_only_rows(self, pg_engine, monkeypatch):
        import asyncio
        from types import SimpleNamespace

        import app.core.data_quality_service as dqs
        from app.api.v1.jobs import _run_quality_gate
        from app.catalog.mirror import sync_dataset_registry

        sync_dataset_registry(pg_engine)
        seen = []

        def _eval(db, job, table):
            seen.append(table)
            raise RuntimeError("stop")  # the gate swallows errors

        monkeypatch.setattr(dqs, "evaluate_rules_for_job", _eval)
        db = self._session(pg_engine)
        try:
            asyncio.run(_run_quality_gate(db, SimpleNamespace(id=1, source="sec")))
            asyncio.run(_run_quality_gate(db, SimpleNamespace(id=2, source="entity_master")))
        finally:
            db.close()
        assert seen == []

    def test_quality_gate_still_runs_for_ingested_rows(self, pg_engine, monkeypatch):
        import asyncio
        from types import SimpleNamespace

        from sqlalchemy import text

        import app.core.data_quality_service as dqs
        from app.api.v1.jobs import _run_quality_gate
        from app.catalog.mirror import sync_dataset_registry

        with pg_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO dataset_registry (source, dataset_id, table_name, created_at, "
                "last_updated_at) VALUES ('fred', 'fred_interest_rates', 'fred_interest_rates', "
                "now() - interval '1 day', now() - interval '1 day')"))
        sync_dataset_registry(pg_engine)  # adds catalog-only fred_gdp_x
        seen = []

        def _eval(db, job, table):
            seen.append(table)
            raise RuntimeError("stop")

        monkeypatch.setattr(dqs, "evaluate_rules_for_job", _eval)
        db = self._session(pg_engine)
        try:
            asyncio.run(_run_quality_gate(db, SimpleNamespace(id=1, source="fred")))
        finally:
            db.close()
        assert seen == ["fred_interest_rates"]

    def test_profile_all_tables_skips_catalog_only_rows(self, pg_engine, monkeypatch):
        from sqlalchemy import text

        import app.core.data_profiling_service as dps
        from app.catalog.mirror import sync_dataset_registry

        with pg_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO dataset_registry (source, dataset_id, table_name, created_at, "
                "last_updated_at) VALUES ('fred', 'fred_interest_rates', 'fred_interest_rates', "
                "now(), now())"))
        sync_dataset_registry(pg_engine)
        profiled = []
        monkeypatch.setattr(dps, "profile_table",
                            lambda db, table, **kw: profiled.append(table))
        db = self._session(pg_engine)
        try:
            dps.profile_all_tables(db)
        finally:
            db.close()
        assert profiled == ["fred_interest_rates"]

    def test_plain_orm_replace_keeps_catalog_block(self, pg_engine):
        """Per-source ingestors (bea, bls, eia, ...) assign source_metadata directly."""
        from app.catalog.mirror import sync_dataset_registry
        from app.core.models import DatasetRegistry

        sync_dataset_registry(pg_engine)
        db = self._session(pg_engine)
        try:
            row = db.query(DatasetRegistry).filter_by(table_name="fred_interest_rates").one()
            row.source_metadata = {"series": ["DGS10"]}
            db.commit()
        finally:
            db.close()
        db = self._session(pg_engine)
        try:
            row = db.query(DatasetRegistry).filter_by(table_name="fred_interest_rates").one()
            assert row.source_metadata["series"] == ["DGS10"]
            assert row.source_metadata["catalog"]["key"] == "fred_series"
            # an explicit new catalog block is not overridden
            row.source_metadata = {"catalog": {"in_catalog": False}}
            db.commit()
            db.refresh(row)
            assert row.source_metadata == {"catalog": {"in_catalog": False}}
        finally:
            db.close()
