"""
Tests for SPEC 144 — per-dataset quality and usage signals.

The DQ framework picks its tables from the catalog (``dq_targets``) instead of
``DatasetRegistry.ingested()``; the quality gate resolves tables through the
job's dataset; the row-delta check reads its baseline before profiling; the
profiling lock is server-keyed; freshness follows the SPEC_124 verdict;
``GET /catalog/{key}`` carries a quality block and consumers; historical
``ingestion_jobs.dataset_key`` rows can be backfilled; the status page draws
a row-count sparkline.
"""
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")


def _spec(key):
    from app.catalog import get_spec

    spec = get_spec(key)
    assert spec is not None, key
    return spec


# ---------------------------------------------------------------------------
# data_state classification (pure)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataState:
    def _classify(self, tables):
        from app.catalog.quality import classify_data_state

        return classify_data_state(tables)

    def test_eia_steo_is_phantom(self):
        spec = _spec("eia_steo")
        from app.catalog.quality import spec_tables

        tables = [{"table": t, "exists": False} for t in spec_tables(spec, set())]
        state, reasons = self._classify(tables)
        assert state == "phantom"
        assert reasons

    def test_fbi_crime_is_empty(self):
        state, _ = self._classify([
            {"table": "fbi_crime_estimates_national", "exists": True, "rows": 0},
            {"table": "fbi_crime_estimates_state", "exists": True, "rows": 0},
        ])
        assert state == "empty"

    def test_usaspending_is_defective(self):
        from app.catalog.quality import key_columns, profile_null_facts

        spec = _spec("usaspending_awards")
        # the live profile of 2026-09-26: naics_code / award_type 100% NULL
        cols = [{"column_name": c, "null_pct": p} for c, p in (
            ("award_id", 0.0), ("recipient_name", 0.0), ("naics_code", 100.0),
            ("naics_description", 100.0), ("award_type", 100.0), ("award_amount", 0.0),
            ("ingested_at", 0.0))]
        facts = profile_null_facts(cols, key_columns(spec))
        assert {"naics_code", "award_type"} <= set(facts["key_null_columns"])
        state, reasons = self._classify([{"table": "usaspending_awards", "exists": True,
                                          "rows": 10190, **facts}])
        assert state == "defective"
        assert any("naics_code" in r for r in reasons)

    def test_all_null_share_is_defective(self):
        from app.catalog.quality import profile_null_facts

        cols = [{"column_name": f"c{i}", "null_pct": 100.0} for i in range(16)] + \
               [{"column_name": "rpt_rec_num", "null_pct": 0.0}, {"column_name": "id", "null_pct": 0.0}]
        facts = profile_null_facts(cols, ())
        assert facts["all_null_share"] == pytest.approx(16 / 17)
        state, _ = self._classify([{"table": "cms_hospital_cost_reports", "exists": True,
                                    "rows": 10, **facts}])
        assert state == "defective"

    def test_substation_is_seed_contaminated(self):
        spec = _spec("si_grid_infrastructure")
        assert "substation" in spec.tables
        state, reasons = self._classify([
            {"table": "substation", "exists": True, "rows": 8738, "seed_rows": 26,
             "key_null_columns": [], "all_null_share": 0.1},
            {"table": "transmission_line", "exists": True, "rows": 52244, "seed_rows": 0},
        ])
        assert state == "seed_contaminated"
        assert "26" in reasons[0]

    def test_populated_notes_missing_tables(self):
        state, reasons = self._classify([
            {"table": "a", "exists": True, "rows": 5},
            {"table": "b", "exists": False},
        ])
        assert state == "populated"
        assert "b" in reasons[0]

    def test_states_are_closed(self):
        from app.catalog.quality import LIVE_STATES

        assert LIVE_STATES == ("phantom", "empty", "populated", "defective", "seed_contaminated")


# ---------------------------------------------------------------------------
# metadata completeness, freshness mapping, profile due
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCompletenessAndFreshness:
    def test_completeness_is_deterministic(self):
        from app.catalog.quality import COMPLETENESS_CHECKS, metadata_completeness

        spec = _spec("sec_form_d")
        a, b = metadata_completeness(spec), metadata_completeness(spec)
        assert a == b
        assert set(a["checks"]) == set(COMPLETENESS_CHECKS)
        assert 0 <= a["score"] <= 1
        assert a["checks"]["named_owner"] is False  # 'data-platform' is not a named owner
        assert a["score"] == round(sum(a["checks"].values()) / len(COMPLETENESS_CHECKS), 2)

    def test_completeness_counts_new_fields_once_present(self):
        import dataclasses

        from app.catalog.quality import metadata_completeness

        spec = _spec("sec_form_d")
        richer = dataclasses.replace(spec, owner="alex", upstream_url="https://sec.gov",
                                     slo_lag_hours=48, primary_key=("accession_number",))
        assert metadata_completeness(richer, column_doc_pct=0.9)["score"] > \
            metadata_completeness(spec)["score"]

    def test_every_status_has_a_freshness(self):
        from app.catalog.quality import FRESHNESS_BY_STATUS
        from app.services.dataset_status import STATUSES

        assert set(FRESHNESS_BY_STATUS) == set(STATUSES)
        assert FRESHNESS_BY_STATUS["current"] == 100.0
        assert FRESHNESS_BY_STATUS["never_run"] == 0.0
        assert FRESHNESS_BY_STATUS["unknown"] is None

    def test_freshness_follows_the_verdict(self, monkeypatch):
        import app.services.dataset_status as ds
        from app.catalog.quality import freshness_by_dataset

        monkeypatch.setattr(ds, "build_status", lambda db, **kw: {"datasets": [
            {"key": "sec_form_d", "status": "current"},
            {"key": "fred_series", "status": "behind"},
            {"key": "eia_steo", "status": "never_run"},
            {"key": "x", "status": "unknown"},
        ]})
        got = freshness_by_dataset(SimpleNamespace(rollback=lambda: None))
        assert got == {"sec_form_d": 100.0, "fred_series": 40.0, "eia_steo": 0.0, "x": None}

    def test_freshness_unavailable_falls_back(self, monkeypatch):
        import app.services.dataset_status as ds
        from app.catalog.quality import freshness_by_dataset

        def boom(db, **kw):
            raise RuntimeError("no db")

        monkeypatch.setattr(ds, "build_status", boom)
        assert freshness_by_dataset(SimpleNamespace(rollback=lambda: None)) == {}

    def test_profile_due(self):
        from app.core.data_profiling_service import _profile_due

        now = datetime(2026, 9, 26, 12)
        snap = SimpleNamespace(profiled_at=now - timedelta(hours=30), row_count=100)
        assert _profile_due(None, 5, 24, now) == "never profiled"
        assert _profile_due(SimpleNamespace(profiled_at=now - timedelta(hours=2), row_count=1), 9, 24, now) is None
        assert _profile_due(snap, 100, 24, now) is None          # unchanged estimate
        assert _profile_due(snap, 150, 24, now)                   # grew: due
        old = SimpleNamespace(profiled_at=now - timedelta(hours=24 * 5), row_count=100)
        assert _profile_due(old, 100, 24, now)                    # unchanged, but past 4x cadence


# ---------------------------------------------------------------------------
# Bug fixes: lock key, row delta order
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBugFixes:
    def test_lock_is_server_keyed(self):
        from app.core import data_profiling_service as dps

        src = (REPO / "app" / "core" / "data_profiling_service.py").read_text(encoding="utf-8")
        assert "hash(table_name)" not in src
        assert "hashtext(:t)" in dps.profile_lock_sql()

    def test_gate_reads_baseline_before_profiling(self, monkeypatch):
        import app.core.data_profiling_service as dps
        import app.core.data_quality_service as dqs
        from app.api.v1.jobs import _gate_table

        order, seen = [], {}
        monkeypatch.setattr(dqs, "previous_profile_count",
                            lambda db, t: order.append("baseline") or 1000)
        monkeypatch.setattr(dqs, "evaluate_rules_for_job",
                            lambda db, job, t: SimpleNamespace(overall_status="passed"))
        monkeypatch.setattr(dps, "profile_table",
                            lambda db, t, **kw: order.append("profile") or SimpleNamespace(row_count=700))

        def delta(db, job, t, current, previous_count=None):
            seen.update(current=current, previous=previous_count)

        monkeypatch.setattr(dqs, "check_row_count_delta", delta)
        monkeypatch.setattr(dqs, "check_date_gaps", lambda *a, **k: [])
        _gate_table(SimpleNamespace(rollback=lambda: None), SimpleNamespace(id=1, source="x"), "t144")
        assert order == ["baseline", "profile"]
        assert seen == {"current": 700, "previous": 1000}

    def test_check_row_count_delta_uses_given_baseline(self):
        from unittest.mock import MagicMock

        from app.core.data_quality_service import check_row_count_delta

        db = MagicMock()
        # the latest snapshot is the one just written (700): ignored when a baseline is given
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = \
            SimpleNamespace(row_count=700)
        alert = check_row_count_delta(db, SimpleNamespace(id=9), "t144", 700, previous_count=1000)
        assert alert is not None and alert.details["previous"] == 1000

    def test_qt_quotes_schema_qualified(self):
        from app.core.data_quality_service import qt

        assert qt("core.entity") == '"core"."entity"'
        assert qt("pe_firms") == '"pe_firms"'
        with pytest.raises(ValueError):
            qt("x; drop table y")


# ---------------------------------------------------------------------------
# Usage map
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUsage:
    def test_usage_json_is_current(self):
        """Drift gate: regenerate with `python -m app.catalog.usage_build`."""
        # a clean interpreter: other tests import extra model modules, which
        # would add tables to Base.metadata and change the scan
        env = dict(os.environ, PYTHONPATH=str(REPO))
        env.setdefault("DATABASE_URL", "postgresql://x:x@127.0.0.1:1/x")
        res = subprocess.run([sys.executable, "-m", "app.catalog.usage_build", "--check"],
                             capture_output=True, text=True, cwd=str(REPO), env=env, timeout=600)
        assert res.returncode == 0, res.stdout + res.stderr

    def test_scan_file_reads_sql_strings_and_orm_names(self):
        from app.catalog.usage_build import scan_file

        src = (
            "from app.core.models import IngestionJob\n"
            "from os import path\n"
            "SQL = '''SELECT * FROM form_d_filings f JOIN core.entity e ON 1=1 FROM cte_x'''\n"
            "# FROM pe_firms in a comment does not count\n"
            "rows = db.query(IngestionJob).all()\n"
        )
        found = scan_file(src, {"form_d_filings", "core.entity", "pe_firms", "path"},
                          {"IngestionJob": {"ingestion_jobs"}})
        assert found == {"form_d_filings": {"sql"}, "core.entity": {"sql"}, "ingestion_jobs": {"orm"}}

    def test_known_readers_are_in_the_map(self):
        from app.catalog.usage_build import load_usage

        tables = load_usage()["tables"]
        assert "app/api/v1/investor_intelligence.py" in {e["path"] for e in tables["public_company_financials"]}
        assert "app/api/v1/form_d.py" in {e["path"] for e in tables["form_d_filings"]}
        assert not any(p.startswith("app/api/v1/catalog") for es in tables.values() for p in (e["path"] for e in es))

    def test_consumers_follow_views(self):
        from app.catalog.usage_build import consumers_for

        deps = {"public_company_financials": ["sec_company_metadata", "sec_financial_facts",
                                              "sec_income_statement"]}
        out = consumers_for(_spec("sec_companyfacts"), deps=deps)
        assert out["views"] == [{"view": "public_company_financials",
                                 "reads": ["sec_financial_facts", "sec_income_statement"]}]
        via_view = [e for e in out["code"] if e["via"] == ["view:public_company_financials"]]
        assert "app/api/v1/investor_intelligence.py" in {e["path"] for e in via_view}
        assert out["count"] >= 2

    def test_undeclared_view_inputs(self):
        from app.catalog import get_catalog
        from app.catalog.usage_build import undeclared_view_inputs

        deps = {"public_company_financials": ["sec_company_metadata", "sec_financial_facts"],
                "unrelated_v": ["some_ops_table"]}
        base = {"sec_company_metadata", "sec_financial_facts", "some_ops_table"}
        assert undeclared_view_inputs(get_catalog(), deps, base) == [
            {"table": "sec_company_metadata", "views": ["public_company_financials"]}]


# ---------------------------------------------------------------------------
# Router shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRouter:
    def test_static_routes_precede_key_route(self):
        from app.api.v1.catalog import router

        paths = [r.path for r in router.routes]
        key_at = paths.index("/catalog/{key}")
        for p in ("/catalog/row-trends", "/catalog/usage", "/catalog/admin/backfill-dataset-key"):
            assert paths.index(p) < key_at, p

    def test_catalog_profiling_is_scheduled_without_main_changes(self):
        src = (REPO / "app" / "core" / "scheduler_service.py").read_text(encoding="utf-8")
        assert "register_catalog_profiling(hour=(hour - 1) % 24)" in src
        main = (REPO / "app" / "main.py").read_text(encoding="utf-8")
        assert "register_daily_quality_snapshots(hour=2)" in main

    def test_hooks_are_wired(self):
        bulk = (REPO / "app" / "ingest" / "bulk" / "base.py").read_text(encoding="utf-8")
        assert 'post_load(engine, f"bulk:{source.name}")' in bulk
        ledger = (REPO / "app" / "marts" / "build_ledger.py").read_text(encoding="utf-8")
        assert "post_load(engine, producer_for_mart(mart))" in ledger

    def test_post_load_disabled_and_unknown_producer(self, monkeypatch):
        from app.catalog.quality import post_load

        monkeypatch.setenv("CATALOG_POST_LOAD_PROFILE", "0")
        assert post_load(None, "bulk:sec_form_d")["skipped"] == "disabled"
        monkeypatch.setenv("CATALOG_POST_LOAD_PROFILE", "1")
        assert post_load(None, "bulk:not_a_source")["profiled"] == []


# ---------------------------------------------------------------------------
# status.html sparkline (node)
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.skipif(NODE is None, reason="node not installed")
class TestSparkline:
    def _run(self, js):
        html = (REPO / "frontend" / "status.html").read_text(encoding="utf-8")
        core = re.search(r'<script id="status-core">(.*?)</script>', html, flags=re.S).group(1)
        prog = "var window = {};\n" + core + "\nvar C = window.StatusCore; var out = {};\n" + js + \
            "\nprocess.stdout.write(JSON.stringify(out));"
        res = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=30)
        assert res.returncode == 0, res.stderr
        return json.loads(res.stdout)

    def test_points_and_svg(self):
        out = self._run("""
            out.none = C.sparklinePoints([], 60, 16);
            out.one = C.sparklinePoints([{date: 'a', rows: 1}], 60, 16);
            out.flat = C.sparklinePoints([{date: 'a', rows: 5}, {date: 'b', rows: 5}], 60, 16);
            out.up = C.sparklinePoints([{date: 'a', rows: 0}, {date: 'b', rows: 10}], 60, 16);
            out.skipNull = C.sparklinePoints([{date: 'a', rows: null}, {date: 'b', rows: 1}, {date: 'c', rows: 2}], 60, 16);
            out.svg = C.sparklineSvg([{date: '<img src=x onerror=alert(1)>', rows: 9}, {date: '2026-09-02', rows: 3}]);
            out.empty = C.sparklineSvg(null);
        """)
        assert out["none"] == "" and out["one"] == ""
        assert out["flat"] == "1.0,8.0 59.0,8.0"
        assert out["up"] == "1.0,15.0 59.0,1.0"
        assert out["skipNull"] == "1.0,15.0 59.0,1.0"
        assert out["empty"] == ""
        assert "<img" not in out["svg"] and "&lt;img" in out["svg"]
        assert 'class="spark down"' in out["svg"]


# ---------------------------------------------------------------------------
# PostgreSQL-backed
# ---------------------------------------------------------------------------

# table -> DDL; created only if absent, dropped only if this module created it
_TABLES = {
    "form_d_filings": "CREATE TABLE form_d_filings (accession_number TEXT PRIMARY KEY, filed_at TIMESTAMP)",
    "pe_firms": "CREATE TABLE pe_firms (id SERIAL PRIMARY KEY, name TEXT, crd_number TEXT)",
    "core.entity": "CREATE TABLE core.entity (entity_id BIGINT PRIMARY KEY)",
    "usaspending_awards": ("CREATE TABLE usaspending_awards (award_id TEXT PRIMARY KEY, recipient_name TEXT, "
                           "naics_code TEXT, award_type TEXT, period_of_performance_start DATE, "
                           "award_amount NUMERIC, ingested_at TIMESTAMP DEFAULT now())"),
    "fbi_crime_estimates_national": "CREATE TABLE fbi_crime_estimates_national (year INT, offense TEXT)",
    "substation": "CREATE TABLE substation (id SERIAL PRIMARY KEY, name TEXT, source TEXT)",
    "fred_t144": "CREATE TABLE fred_t144 (series_id TEXT, v NUMERIC)",
    "t144_gate": "CREATE TABLE t144_gate (id INT)",
    "t144_lock": "CREATE TABLE t144_lock (id INT)",
    "t144_seed": "CREATE TABLE t144_seed (id INT, data_source TEXT, origin TEXT)",
    "t144_contacts": "CREATE TABLE t144_contacts (id INT, full_name TEXT, email TEXT, city TEXT)",
}
_VIEWS = {
    "fred_observations_t144": "CREATE VIEW fred_observations_t144 AS SELECT * FROM fred_t144",
    "t144_v": "CREATE VIEW t144_v AS SELECT accession_number FROM form_d_filings",
}


def _dq_models():
    from app.core.models import (
        DatasetRegistry, DataProfileColumn, DataProfileSnapshot, DataQualityReport,
        DataQualityResult, DataQualityRule, DQAnomalyAlert, DQCrossSourceResult,
        DQCrossSourceValidation, DQQualitySnapshot, IngestionJob,
    )

    return [DatasetRegistry, DataProfileSnapshot, DataProfileColumn, DQQualitySnapshot,
            DQAnomalyAlert, DataQualityResult, DataQualityRule, DataQualityReport, IngestionJob,
            DQCrossSourceValidation, DQCrossSourceResult]


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, inspect, text

    from app.catalog import quality, usage_build
    from app.catalog.live import clear_cache
    from app.core.models import Base

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core"))
    insp = inspect(engine)
    created = []
    with engine.begin() as conn:
        for t, ddl in _TABLES.items():
            schema, name = t.split(".") if "." in t else ("public", t)
            if name not in insp.get_table_names(schema=schema):
                conn.execute(text(ddl))
                created.append(t)
        for v, ddl in _VIEWS.items():
            conn.execute(text(f"DROP VIEW IF EXISTS {v}"))
            conn.execute(text(ddl))
    Base.metadata.create_all(engine, tables=[m.__table__ for m in _dq_models()])
    with engine.begin() as conn:
        for m in _dq_models():
            conn.execute(text(f"DELETE FROM {m.__table__.name}"))
    quality.clear_cache()
    usage_build.clear_cache()
    clear_cache()
    engine._t144_created = created
    yield engine
    quality.clear_cache()
    usage_build.clear_cache()
    clear_cache()
    with engine.begin() as conn:
        for v in _VIEWS:
            conn.execute(text(f"DROP VIEW IF EXISTS {v}"))
        for t in reversed(created):
            conn.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
        for m in _dq_models():
            conn.execute(text(f"DELETE FROM {m.__table__.name}"))
    engine.dispose()


def _session(engine):
    from sqlalchemy.orm import sessionmaker

    return sessionmaker(bind=engine)()


def _require_created(engine, *tables):
    missing = [t for t in tables if t not in engine._t144_created]
    if missing:
        pytest.skip(f"{missing} pre-exist in this test DB with another shape; use a fresh PG_DB")


@pg
class TestSelectorPg:
    def test_selector_includes_catalog_tables_not_views(self, pg_engine):
        from app.catalog.quality import dq_targets

        db = _session(pg_engine)
        try:
            targets = {t["table"]: t for t in dq_targets(db)}
        finally:
            db.close()
        for t in ("form_d_filings", "pe_firms", "core.entity"):
            assert t in targets, t
        assert targets["form_d_filings"]["dataset_key"] == "sec_form_d"
        assert targets["core.entity"]["dataset_key"] == "entity_master"
        assert "fred_t144" in targets
        assert "fred_observations_t144" not in targets  # a view: never a DQ target
        from sqlalchemy import inspect

        if "form_d_issuers" not in inspect(pg_engine).get_table_names():
            assert "form_d_issuers" not in targets      # declared but absent

    def test_registry_rows_join_and_lend_their_source(self, pg_engine):
        from sqlalchemy import text

        from app.catalog.quality import dq_targets

        with pg_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO dataset_registry (source, dataset_id, table_name, created_at, last_updated_at) "
                "VALUES ('legacy_src', 'x', 't144_gate', now(), now()), "
                "('sec_legacy', 'y', 'form_d_filings', now(), now())"))
        db = _session(pg_engine)
        try:
            targets = {t["table"]: t for t in dq_targets(db)}
        finally:
            db.close()
        assert targets["t144_gate"]["dataset_key"] is None
        assert targets["t144_gate"]["source"] == "legacy_src"
        assert targets["form_d_filings"]["source"] == "sec_legacy"

    def test_profile_all_tables_is_catalog_driven(self, pg_engine, monkeypatch):
        import app.core.data_profiling_service as dps

        profiled = []
        monkeypatch.setattr(dps, "profile_table", lambda db, t, **kw: profiled.append(t))
        db = _session(pg_engine)
        try:
            dps.profile_all_tables(db)
        finally:
            db.close()
        assert {"form_d_filings", "pe_firms", "core.entity"} <= set(profiled)
        assert "fred_observations_t144" not in profiled

    def test_gate_uses_the_job_dataset(self, pg_engine, monkeypatch):
        import asyncio

        import app.core.data_quality_service as dqs
        from app.api.v1.jobs import _run_quality_gate

        seen = []

        def _eval(db, job, table):
            seen.append(table)
            raise RuntimeError("stop")  # each table is gated on its own

        monkeypatch.setattr(dqs, "evaluate_rules_for_job", _eval)
        db = _session(pg_engine)
        try:
            asyncio.run(_run_quality_gate(db, SimpleNamespace(
                id=1, source="entity_resolve", config={}, dataset_key="entity_master")))
        finally:
            db.close()
        # entity_master's existing tables, declared order (the shared DB may hold more core.*)
        assert seen[0] == "core.entity"
        assert set(seen) <= set(_spec("entity_master").tables)


@pg
class TestProfilingPg:
    def test_schema_qualified_profile(self, pg_engine):
        from sqlalchemy import text

        from app.core.data_profiling_service import profile_table

        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM core.entity"))
            conn.execute(text("INSERT INTO core.entity VALUES (1), (2), (3)"))
        db = _session(pg_engine)
        try:
            snap = profile_table(db, "core.entity", source="entity_master")
            assert snap is not None and snap.row_count == 3 and snap.table_name == "core.entity"
        finally:
            db.close()
        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM core.entity"))

    def test_lock_is_shared_across_processes(self, pg_engine):
        from app.core import data_profiling_service as dps

        holder = (
            "import sys\n"
            "from sqlalchemy import create_engine, text\n"
            "from app.core.data_profiling_service import PROFILE_LOCK_NAMESPACE, profile_lock_sql\n"
            f"e = create_engine({PG_URL!r})\n"
            "c = e.connect().execution_options(isolation_level='AUTOCOMMIT')\n"
            "ok = c.execute(text(profile_lock_sql()), {'ns': PROFILE_LOCK_NAMESPACE, 't': 't144_lock'}).scalar()\n"
            "print('locked' if ok else 'busy', flush=True)\n"
            "sys.stdin.readline()\n"
        )
        env = dict(os.environ, PYTHONPATH=str(REPO))
        proc = subprocess.Popen([sys.executable, "-c", holder], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, text=True, cwd=str(REPO), env=env)
        try:
            assert proc.stdout.readline().strip() == "locked"
            db = _session(pg_engine)
            try:
                assert dps.profile_table(db, "t144_lock") is None  # another process holds it
            finally:
                db.close()
        finally:
            proc.stdin.write("\n")
            proc.stdin.flush()
            proc.wait(timeout=30)
        db = _session(pg_engine)
        try:
            assert dps.profile_table(db, "t144_lock") is not None  # released with the process
        finally:
            db.close()

    def test_gate_row_delta_uses_prior_snapshot(self, pg_engine, monkeypatch):
        from sqlalchemy import text

        import app.core.data_quality_service as dqs
        from app.api.v1.jobs import _gate_table
        from app.core.models import DQAnomalyAlert

        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM t144_gate"))
            conn.execute(text("INSERT INTO t144_gate SELECT g FROM generate_series(1, 10) g"))
            conn.execute(text(
                "INSERT INTO data_profile_snapshots (table_name, row_count, column_count, "
                "total_null_count, profiled_at) VALUES ('t144_gate', 100, 1, 0, now() - interval '1 day')"))
        monkeypatch.setattr(dqs, "evaluate_rules_for_job",
                            lambda db, job, t: SimpleNamespace(overall_status="passed"))
        db = _session(pg_engine)
        try:
            _gate_table(db, SimpleNamespace(id=77, source="t144"), "t144_gate")
            alerts = db.query(DQAnomalyAlert).filter(DQAnomalyAlert.table_name == "t144_gate").all()
            drops = [a for a in alerts if a.details and a.details.get("previous") == 100]
            assert drops and drops[0].details["current"] == 10
        finally:
            db.close()

    def test_scheduled_profiling_skips_fresh_profiles(self, pg_engine, monkeypatch):
        import app.catalog.quality as q
        import app.core.data_profiling_service as dps

        # only the small test tables (the shared DB may hold others)
        mine = {"form_d_filings", "core.entity", "pe_firms"}
        real = q.dq_targets
        monkeypatch.setattr(q, "dq_targets", lambda db, **kw: [t for t in real(db, **kw) if t["table"] in mine])
        db = _session(pg_engine)
        try:
            first = dps.profile_stale_catalog_tables(db)
            assert set(first["profiled"]) == mine
            second = dps.profile_stale_catalog_tables(db)
            assert second["profiled"] == [] and second["due"] == 0
            capped = dps.profile_stale_catalog_tables(db, now=datetime.utcnow() + timedelta(days=400),
                                                      max_tables=1)
            assert len(capped["profiled"]) == 1 and len(capped["skipped"]) == 2
        finally:
            db.close()


@pg
class TestSnapshotsPg:
    def test_daily_snapshot_uses_verdict_and_row_estimate(self, pg_engine, monkeypatch):
        from sqlalchemy import text

        import app.catalog.quality as q
        from app.core import quality_trending_service as qts
        from app.core.models import DQQualitySnapshot

        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM form_d_filings"))
            conn.execute(text("INSERT INTO form_d_filings VALUES ('a', now()), ('b', now())"))
            conn.execute(text("ANALYZE form_d_filings"))
        monkeypatch.setattr(q, "freshness_by_dataset", lambda db: {"sec_form_d": 40.0})
        mine = {"form_d_filings", "core.entity"}
        real = q.dq_targets
        monkeypatch.setattr(q, "dq_targets", lambda db, **kw: [t for t in real(db, **kw) if t["table"] in mine])
        db = _session(pg_engine)
        try:
            qts.compute_daily_snapshots(db)
            rows = {s.table_name: s for s in db.query(DQQualitySnapshot).all()}
        finally:
            db.close()
        assert set(rows) == mine
        assert rows["form_d_filings"].freshness_score == 40.0
        assert rows["form_d_filings"].row_count == 2
        assert rows["form_d_filings"].source == "sec"

    def test_row_trends(self, pg_engine):
        from sqlalchemy import text

        from app.catalog import get_catalog
        from app.catalog.quality import row_trends

        today = date(2026, 9, 26)
        with pg_engine.begin() as conn:
            for i, (t, n) in enumerate([("form_d_filings", 10), ("form_d_filings", 12),
                                        ("form_d_issuers", 5), ("zzz_other", 1)]):
                conn.execute(text(
                    "INSERT INTO dq_quality_snapshots (snapshot_date, source, table_name, row_count, created_at) "
                    "VALUES (:d, 'sec', :t, :n, now())"),
                    {"d": today - timedelta(days=1 if i == 0 else 0), "t": t, "n": n})
        got = row_trends(pg_engine, get_catalog(), days=30, today=today)
        assert got["sec_form_d"] == [{"date": "2026-09-25", "rows": 10}, {"date": "2026-09-26", "rows": 17}]
        assert all("zzz" not in k for k in got)


@pg
class TestQualityBlockPg:
    def test_states_on_real_specs(self, pg_engine):
        from sqlalchemy import text

        from app.catalog.quality import quality_block
        from app.core.data_profiling_service import profile_table

        _require_created(pg_engine, "usaspending_awards", "substation", "fbi_crime_estimates_national")
        with pg_engine.begin() as conn:
            conn.execute(text("INSERT INTO usaspending_awards (award_id, recipient_name, award_amount) "
                              "VALUES ('A1', 'Acme', 5), ('A2', 'Beta', 7)"))
            conn.execute(text("INSERT INTO substation (name, source) VALUES "
                              "('s1', 'hifld'), ('s2', 'hifld_sample'), ('s3', 'hifld')"))
            conn.execute(text("DROP TABLE IF EXISTS eia_steo"))
        db = _session(pg_engine)
        try:
            assert profile_table(db, "usaspending_awards", source="usaspending") is not None
        finally:
            db.close()

        block = quality_block(pg_engine, _spec("usaspending_awards"), refresh=True)
        assert block["live_state"] == "defective"
        t = block["tables"][0]
        assert {"naics_code", "award_type"} <= set(t["key_null_columns"])
        assert t["key_null_pct"]["naics_code"] == 100.0
        assert t["rows"] == 2 and t["rows_exact"] is True
        assert block["profile"]["stale"] is False

        assert quality_block(pg_engine, _spec("fbi_crime_estimates"), refresh=True)["live_state"] == "empty"
        assert quality_block(pg_engine, _spec("eia_steo"), refresh=True)["live_state"] == "phantom"
        grid = quality_block(pg_engine, _spec("si_grid_infrastructure"), refresh=True)
        assert grid["live_state"] == "seed_contaminated"
        sub = [e for e in grid["tables"] if e["table"] == "substation"][0]
        assert sub["seed_rows"] == 1

    def test_scores_rules_anomalies_and_trend(self, pg_engine):
        from sqlalchemy import text

        from app.catalog.quality import quality_block

        today = date.today()
        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM form_d_filings"))
            conn.execute(text("INSERT INTO form_d_filings VALUES ('a', now())"))
            conn.execute(text(
                "INSERT INTO dq_quality_snapshots (snapshot_date, source, table_name, quality_score, "
                "freshness_score, row_count, created_at) VALUES "
                "(:d1, 'sec', 'form_d_filings', 80, 100, 1, now()), (:d2, 'sec', 'form_d_filings', 90, 100, 3, now())"),
                {"d1": today - timedelta(days=2), "d2": today})
            conn.execute(text(
                "INSERT INTO data_quality_rules (id, name, source, rule_type, severity, parameters, is_enabled, "
                "priority, times_evaluated, times_passed, times_failed, created_at, updated_at) VALUES "
                "(14401, 'fd not null', 'sec', 'not_null', 'error', '{}', 1, 5, 0, 0, 0, now(), now()), "
                "(14402, 'fd range', 'sec', 'range', 'warning', '{}', 1, 5, 0, 0, 0, now(), now())"))
            conn.execute(text(
                "INSERT INTO data_quality_results (rule_id, source, dataset_name, passed, severity, evaluated_at) VALUES "
                "(14401, 'sec', 'form_d_filings', 1, 'error', now() - interval '2 hours'), "
                "(14401, 'sec', 'form_d_filings', 0, 'error', now() - interval '1 hour'), "
                "(14402, 'sec', 'form_d_filings', 1, 'warning', now() - interval '1 hour')"))
            conn.execute(text(
                "INSERT INTO dq_anomaly_alerts (table_name, alert_type, status, severity, detected_at) "
                "VALUES ('form_d_filings', 'row_count_drop', 'open', 'warning', now())"))
        block = quality_block(pg_engine, _spec("sec_form_d"), refresh=True)
        assert block["score"] == 90.0
        assert block["components"]["freshness"] == 100.0
        assert block["rules"]["passed"] == 1 and block["rules"]["failed"] == 1   # latest per rule
        assert block["rules"]["failing"] == ["fd not null"]
        assert block["open_anomalies"] == 1
        assert [p["rows"] for p in block["row_trend"]] == [1, 3]
        assert block["live_state"] == "populated"
        assert "metadata_completeness" in block


@pg
class TestUsagePg:
    def test_view_dependencies(self, pg_engine):
        from app.catalog.usage_build import consumers_for, view_dependencies

        deps = view_dependencies(pg_engine, refresh=True)
        assert deps["t144_v"] == ["form_d_filings"]
        assert deps["fred_observations_t144"] == ["fred_t144"]
        out = consumers_for(_spec("sec_form_d"), deps=deps)
        assert {"view": "t144_v", "reads": ["form_d_filings"]} in out["views"]


@pg
class TestBackfillPg:
    def _insert(self, engine, rows):
        from sqlalchemy import text

        with engine.begin() as conn:
            for source, config, key in rows:
                conn.execute(text(
                    "INSERT INTO ingestion_jobs (source, status, config, created_at, retry_count, "
                    "max_retries, data_origin, dataset_key) "
                    "VALUES (:s, 'success', CAST(:c AS JSON), now(), 0, 3, 'real', :k)"),
                    {"s": source, "c": json.dumps(config), "k": key})

    def test_dry_run_apply_and_rerun(self, pg_engine):
        from sqlalchemy import text

        from app.catalog.backfill_dataset_key import backfill

        self._insert(pg_engine, [
            ("fred", {"category": "interest_rates"}, None),
            ("fred", {}, None),
            ("job:pe_mart_build", {}, None),          # several datasets: stays NULL
            ("not_a_source_144", {}, None),           # unresolved
            ("fred", {}, "custom_key"),               # never overwritten
        ])
        dry = backfill(pg_engine)
        assert dry["applied"] is False and dry["updated"] == 0
        assert dry["null_before"] == 4
        assert dry["resolvable"] == 2 and dry["by_dataset"] == {"fred_series": 2}
        assert dry["ambiguous"] == 1 and dry["ambiguous_sources"] == {"job:pe_mart_build": 1}
        assert dry["unresolved_sources"] == {"not_a_source_144": 1}
        with pg_engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM ingestion_jobs WHERE dataset_key IS NULL")).scalar() == 4

        done = backfill(pg_engine, apply=True, batch_size=1)
        assert done["updated"] == 2 and done["null_after"] == 2
        again = backfill(pg_engine, apply=True)
        assert again["updated"] == 0 and again["resolvable"] == 0
        with pg_engine.connect() as conn:
            keys = dict(conn.execute(text(
                "SELECT dataset_key, count(*) FROM ingestion_jobs GROUP BY dataset_key")).fetchall())
        assert keys == {"fred_series": 2, "custom_key": 1, None: 2}


@pg
class TestCatalogApiPg:
    @pytest.fixture
    def client(self, pg_engine):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sqlalchemy.orm import sessionmaker

        from app.api.v1 import catalog
        from app.core.authz import current_principal
        from app.core.database import get_db

        app = FastAPI()
        app.include_router(catalog.router, prefix="/api/v1")
        Session = sessionmaker(bind=pg_engine)
        role = {"role": "admin"}

        def _db():
            db = Session()
            try:
                yield db
            finally:
                db.close()

        app.dependency_overrides[get_db] = _db
        app.dependency_overrides[current_principal] = lambda: role
        c = TestClient(app)
        c.role = role
        return c

    def test_detail_carries_quality_and_consumers(self, client):
        body = client.get("/api/v1/catalog/sec_form_d").json()
        assert body["quality"]["live_state"] in ("populated", "empty")
        assert body["quality"]["metadata_completeness"]["score"] >= 0
        paths = {e["path"] for e in body["consumers"]["code"]}
        assert "app/api/v1/form_d.py" in paths
        assert {"view": "t144_v", "reads": ["form_d_filings"]} in body["consumers"]["views"]

    def test_row_trends_and_usage(self, client):
        r = client.get("/api/v1/catalog/row-trends?days=30")
        assert r.status_code == 200 and "datasets" in r.json()
        assert client.get("/api/v1/catalog/row-trends?days=1").status_code == 422
        u = client.get("/api/v1/catalog/usage").json()
        assert "sec_form_d" in u["datasets"] and isinstance(u["unread"], list)
        assert u["view_dependencies_available"] is True

    def test_backfill_endpoint_is_admin_only(self, client):
        assert client.post("/api/v1/catalog/admin/backfill-dataset-key").json()["applied"] is False
        client.role["role"] = "user"
        assert client.post("/api/v1/catalog/admin/backfill-dataset-key").status_code == 403


# ===========================================================================
# Review fixes (spec-144-fix)
# ===========================================================================


def _filtered_spec(base_key, key, table, predicate):
    """A spec with SPEC_141's ``row_filters`` (the field lands with SPEC_141;
    the quality code reads it with getattr, so set it on a copy)."""
    import dataclasses

    spec = dataclasses.replace(_spec(base_key), key=key, tables=(table,), table_patterns=(),
                               primary_key=())
    object.__setattr__(spec, "row_filters", ((table, predicate),))
    return spec


@pytest.mark.unit
class TestGateTableSelection:
    """Finding 1: the gate checks the table the job loaded, not the first N."""

    def _pick(self, monkeypatch, job, catalog, base=None, registry=None):
        from unittest.mock import MagicMock

        import app.catalog.quality as q

        base = set(base if base is not None else catalog)
        monkeypatch.setattr(q, "relations", lambda engine, cached=False: {
            t: {"kind": "table", "reltuples": 1, "n_live_tup": 1} for t in base})
        monkeypatch.setattr(q, "dataset_tables_for_job", lambda db, job: list(catalog))
        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = registry
        return q.gate_tables_for_job(db, job, max_tables=6)

    def test_census_job_gates_its_own_table(self, monkeypatch):
        catalog = ["acs5_2015_b01001", "acs5_2015_b19013", "acs5_2020_b01001", "acs5_2021_b01001",
                   "acs5_2022_b01001", "acs5_2022_b01003", "acs5_2022_b19013", "acs5_2023_b01001",
                   "acs5_2023_b19013"]
        job = SimpleNamespace(id=1, source="census", started_at=None, created_at=None, config={
            "survey": "acs5", "year": 2023, "table_id": "B01001", "geo_level": "state"})
        assert self._pick(monkeypatch, job, catalog) == ["acs5_2023_b01001"]

    def test_bls_dataset_gates_bls_oes(self, monkeypatch):
        catalog = ["bls_cpi", "bls_cps_labor_force", "bls_jolts", "bls_ces_employment",
                   "bls_cpi_consumer_prices", "bls_cps_unemployment", "bls_oes"]
        job = SimpleNamespace(id=1, source="bls", config={"dataset": "oes"})
        assert self._pick(monkeypatch, job, catalog) == ["bls_oes"]

    def test_dunl_source_suffix(self, monkeypatch):
        catalog = ["dunl_currencies", "dunl_ports", "dunl_uom", "dunl_uom_conversions", "dunl_calendars"]
        assert self._pick(monkeypatch, SimpleNamespace(id=1, source="dunl:ports", config={}),
                          catalog) == ["dunl_ports"]
        assert self._pick(monkeypatch, SimpleNamespace(id=1, source="dunl:uom_conversions", config={}),
                          catalog) == ["dunl_uom_conversions"]

    def test_registry_row_updated_by_the_job_comes_first(self, monkeypatch):
        started = datetime(2026, 9, 26, 10)
        reg = SimpleNamespace(table_name="fred_interest_rates", last_updated_at=started + timedelta(minutes=5))
        job = SimpleNamespace(id=1, source="fred", config={}, started_at=started)
        got = self._pick(monkeypatch, job, ["fred_gdp", "fred_interest_rates", "fred_cpi"], registry=reg)
        assert got == ["fred_interest_rates"]

    def test_config_named_table(self, monkeypatch):
        job = SimpleNamespace(id=1, source="x", config={"table_name": "T_B"})
        assert self._pick(monkeypatch, job, ["t_a", "t_b"]) == ["t_b"]

    def test_nothing_specific_falls_back_to_registry_then_catalog(self, monkeypatch):
        reg = SimpleNamespace(table_name="fred_cpi", last_updated_at=datetime(2026, 1, 1))
        job = SimpleNamespace(id=1, source="fred", config={}, started_at=datetime(2026, 9, 26))
        got = self._pick(monkeypatch, job, ["fred_gdp", "fred_interest_rates", "fred_cpi"], registry=reg)
        assert got == ["fred_cpi", "fred_gdp", "fred_interest_rates"]


@pytest.mark.unit
class TestGateOffTheEventLoop:
    """Finding 6: blocking gate work runs in a thread; large tables are not profiled."""

    def test_gate_runs_in_a_worker_thread(self, monkeypatch):
        import asyncio
        import threading

        from app.api.v1 import jobs

        seen = []
        monkeypatch.setattr(jobs, "_quality_gate_tables", lambda db, job: ["t1", "t2"])
        monkeypatch.setattr(jobs, "_gate_table", lambda db, job, t: seen.append((t, threading.get_ident())))

        async def main():
            beats = []

            async def heartbeat():
                for _ in range(3):
                    beats.append(1)
                    await asyncio.sleep(0)

            await asyncio.gather(jobs._run_quality_gate(SimpleNamespace(rollback=lambda: None),
                                                        SimpleNamespace(id=1, source="x")), heartbeat())
            return beats

        loop_thread = threading.get_ident()
        assert asyncio.run(main()) == [1, 1, 1]
        assert [t for t, _ in seen] == ["t1", "t2"]
        assert all(ident != loop_thread for _, ident in seen)

    def test_gate_errors_still_swallowed(self, monkeypatch):
        import asyncio

        from app.api.v1 import jobs

        def boom(db, job):
            raise RuntimeError("x")

        monkeypatch.setattr(jobs, "_quality_gate_tables", boom)
        asyncio.run(jobs._run_quality_gate(SimpleNamespace(rollback=lambda: None),
                                           SimpleNamespace(id=1, source="x")))

    def test_large_table_is_left_to_the_scheduler(self, monkeypatch):
        import app.core.data_profiling_service as dps
        import app.core.data_quality_service as dqs
        from app.api.v1 import jobs

        profiled, seen = [], {}
        monkeypatch.setattr(jobs, "_gate_row_estimate", lambda db, t: jobs.GATE_PROFILE_MAX_ROWS + 1)
        monkeypatch.setattr(dqs, "previous_profile_count", lambda db, t: 10 ** 7)
        monkeypatch.setattr(dqs, "evaluate_rules_for_job",
                            lambda db, job, t: SimpleNamespace(overall_status="passed"))
        monkeypatch.setattr(dps, "profile_table", lambda db, t, **kw: profiled.append(t))
        monkeypatch.setattr(dqs, "check_row_count_delta",
                            lambda db, job, t, current, previous_count=None: seen.update(current=current))
        monkeypatch.setattr(dqs, "check_date_gaps", lambda *a, **k: [])
        jobs._gate_table(SimpleNamespace(rollback=lambda: None), SimpleNamespace(id=1, source="x"), "big_t")
        assert profiled == []
        assert seen == {"current": jobs.GATE_PROFILE_MAX_ROWS + 1}  # the estimate, no count(*)

    def test_gate_limit_matches_post_load(self):
        from app.api.v1 import jobs
        from app.catalog.quality import POST_LOAD_MAX_ROWS

        assert jobs.GATE_PROFILE_MAX_ROWS == POST_LOAD_MAX_ROWS


@pytest.mark.unit
class TestUsageCoverage:
    """Finding 2 and the usage scope gaps."""

    def test_generic_ingestor_tables_have_readers(self):
        from app.catalog.usage_build import load_usage

        tables = load_usage()["tables"]
        assert "app/services/atlas/layers.py" in {e["path"] for e in tables["fdic_bank_financials"]}
        assert "app/api/v1/econ_snapshot.py" in {e["path"] for e in tables["bea_regional"]}
        assert tables.get("fema_disaster_declarations")
        assert tables.get("us_trade_exports_state")

    def test_patterns_count_in_scan(self):
        from app.catalog.usage_build import scan_file

        src = "SQL = 'SELECT * FROM acs5_2023_b01001 JOIN cte_x ON 1=1'\n"
        assert scan_file(src, set(), {}, patterns=("acs5_*",)) == {"acs5_2023_b01001": {"sql"}}
        assert scan_file(src, set(), {}) == {}

    def test_core_services_are_scanned_but_not_model_files(self):
        from app.catalog.usage_build import load_usage

        paths = {e["path"] for es in load_usage()["tables"].values() for e in es}
        assert any(p.startswith("app/core/") for p in paths)
        assert "app/core/models.py" not in paths and "app/core/pe_models.py" not in paths

    def test_view_over_view_readers(self):
        from app.catalog.usage_build import consumers_for

        usage = {"tables": {"v_outer": [{"path": "app/api/v1/x.py", "kind": "api_router", "via": ["sql"]}]}}
        deps = {"v_inner": ["form_d_filings"], "v_outer": ["v_inner"]}
        out = consumers_for(_spec("sec_form_d"), deps=deps, usage=usage)
        assert {"view": "v_outer", "reads": ["v_inner"]} in out["views"]
        assert {"path": "app/api/v1/x.py", "kind": "api_router", "table": "v_outer",
                "via": ["view:v_outer"]} in out["code"]


@pytest.mark.unit
class TestFlagsAndVocabulary:
    """Findings 4 and 7: one vocabulary; verified states and seed rows are flags."""

    def _classify(self, tables, verified=None):
        from app.catalog.quality import classify_live_state

        return classify_live_state(tables, verified)

    def test_verified_fabricated_is_never_populated(self):
        state, reasons, flags = self._classify(
            [{"table": "seismic_hazard", "exists": True, "rows": 539, "seed_rows": 0}], "fabricated")
        assert state == "seed_contaminated" and flags == ["fabricated"]
        assert any("fabricated" in r for r in reasons)

    def test_defective_keeps_the_seed_flag(self):
        state, reasons, flags = self._classify([
            {"table": "t", "exists": True, "rows": 10, "key_null_columns": ["k"], "seed_rows": 3}])
        assert state == "defective"
        assert set(flags) == {"key_columns_null", "seed_rows"}
        assert any("3 seed" in r for r in reasons)

    def test_verified_key_columns_null_is_defective(self):
        state, _, flags = self._classify([{"table": "t", "exists": True, "rows": 5}], "key_columns_null")
        assert state == "defective" and flags == ["key_columns_null"]

    def test_skipped_seed_scan_is_a_reason(self):
        state, reasons, _ = self._classify([
            {"table": "big", "exists": True, "rows": 10 ** 7, "seed_rows": None, "seed_scan": "skipped"}])
        assert state == "populated"
        assert any("seed scan skipped" in r and "big" in r for r in reasons)

    def test_unknown_rows_are_not_empty(self):
        state, reasons, _ = self._classify([{"table": "shared", "exists": True, "rows": None}])
        assert state == "populated" and any("row count unknown" in r for r in reasons)

    def test_curated_states_match_the_spec_141_vocabulary(self):
        from app.catalog import get_spec
        from app.catalog.quality import CURATED_DATA_STATES, FLAG_STATES, static_flags

        for key, state in CURATED_DATA_STATES.items():
            assert get_spec(key) is not None, key
            assert state in FLAG_STATES + ("key_columns_null",)
        for key in ("si_seismic_hazard", "si_incentive_deals", "public_lp_strategies",
                    "si_certified_sites", "si_opportunity_zones"):
            assert static_flags(_spec(key)), key
        assert static_flags(_spec("sec_form_d")) == []

    def test_spec_data_state_wins_over_curated(self):
        import dataclasses

        from app.catalog.quality import verified_state

        spec = dataclasses.replace(_spec("si_seismic_hazard"))
        object.__setattr__(spec, "data_state", "ok")
        assert verified_state(spec) == "ok"

    def test_seed_markers_cover_demo_and_hand_typed_rows(self):
        from app.catalog.quality import SEED_MARKER_COLUMNS, SEED_SOURCE_VALUES, seed_predicate

        assert {"source", "data_source"} <= set(SEED_MARKER_COLUMNS)
        assert {"demo_seeder", "gjf_expanded", "nrel_reference"} <= set(SEED_SOURCE_VALUES)
        sql, params = seed_predicate(["source", "data_source"])
        assert '"data_source"' in sql and "demo_seeder" in params.values()

    def test_list_carries_static_flags(self):
        from app.api.v1.catalog import list_catalog

        body = list_catalog(kind=None, source=None, status_public=None, redistribution=None, q=None)
        by = {d["key"]: d for d in body["datasets"]}
        assert by["si_seismic_hazard"]["quality_flags"] == ["fabricated"]
        assert by["sec_form_d"]["quality_flags"] == []


@pytest.mark.unit
class TestRawValuePolicy:
    """Finding 5: no raw values from personal/contact or restricted datasets."""

    def test_restricted_tables(self):
        from app.catalog.quality import raw_values_restricted

        for t in ("people", "company_people", "family_office_contacts", "pe_people"):
            assert raw_values_restricted(t), t
        # HIFLD substations: restricted since SPEC_141 (withdrawn from public release)
        assert raw_values_restricted("substation")
        assert not raw_values_restricted("fdic_bank_financials")

    def test_personal_columns(self):
        from app.core.data_profiling_service import is_personal_column

        for c in ("email", "contact_email", "phone", "linkedin_url", "full_name", "first_name"):
            assert is_personal_column(c), c
        for c in ("city", "company_name", "source", "state"):
            assert not is_personal_column(c), c

    def test_policy_fails_closed(self, monkeypatch):
        import app.catalog.quality as q
        from app.core.data_profiling_service import raw_values_allowed

        def boom(t):
            raise RuntimeError("no catalog")

        monkeypatch.setattr(q, "raw_values_restricted", boom)
        assert raw_values_allowed("substation") is False


@pg
class TestReviewFixesPg:
    def test_profile_of_a_personal_table_stores_no_raw_values(self, pg_engine, monkeypatch):
        from sqlalchemy import text

        import app.catalog.quality as q
        from app.core.data_profiling_service import get_column_stats, profile_table

        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM t144_contacts"))
            conn.execute(text("INSERT INTO t144_contacts VALUES (1, 'Ann Lee', 'ann@x.com', 'Austin'), "
                              "(2, 'Bo Diaz', 'bo@x.com', 'Austin')"))
        # pii='personal' dataset
        monkeypatch.setattr(q, "raw_values_restricted", lambda t, specs=None: t == "t144_contacts")
        db = _session(pg_engine)
        try:
            snap = profile_table(db, "t144_contacts")
            stats = {c.column_name: c.stats for c in get_column_stats(db, snap.id)}
        finally:
            db.close()
        blob = json.dumps(stats)
        assert "ann@x.com" not in blob and "Ann Lee" not in blob and "Austin" not in blob
        assert stats["email"]["top_values_withheld"] is True

        # a table the catalog allows still withholds name/email/phone columns
        monkeypatch.setattr(q, "raw_values_restricted", lambda t, specs=None: False)
        db = _session(pg_engine)
        try:
            snap = profile_table(db, "t144_contacts")
            stats = {c.column_name: c.stats for c in get_column_stats(db, snap.id)}
        finally:
            db.close()
        assert "top_values" not in stats["email"] and "top_values" not in stats["full_name"]
        assert stats["city"]["top_values"][0]["value"] == "Austin"

    def test_profile_columns_endpoint_strips_raw_values_for_non_admin(self, pg_engine, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sqlalchemy.orm import sessionmaker

        import app.catalog.quality as q
        from app.api.v1 import data_quality
        from app.core.authz import current_principal
        from app.core.database import get_db
        from app.core.models import DataProfileColumn, DataProfileSnapshot

        db = _session(pg_engine)
        try:
            snap = DataProfileSnapshot(table_name="t144_contacts", row_count=2, column_count=1,
                                       total_null_count=0, profiled_at=datetime.utcnow())
            db.add(snap)
            db.flush()
            db.add(DataProfileColumn(snapshot_id=snap.id, column_name="city", null_count=0, null_pct=0,
                                     stats={"top_values": [{"value": "Austin", "count": 2}]}))
            db.commit()
        finally:
            db.close()
        monkeypatch.setattr(q, "raw_values_restricted", lambda t, specs=None: t == "t144_contacts")
        monkeypatch.setattr(data_quality, "_require_profilable", lambda db, t: None)
        app = FastAPI()
        app.include_router(data_quality.router, prefix="/api/v1")
        Session = sessionmaker(bind=pg_engine)
        role = {"role": "user"}

        def _db():
            s = Session()
            try:
                yield s
            finally:
                s.close()

        app.dependency_overrides[get_db] = _db
        app.dependency_overrides[current_principal] = lambda: role
        c = TestClient(app)
        user = c.get("/api/v1/data-quality/profiles/t144_contacts/columns").json()
        assert "Austin" not in json.dumps(user) and user[0]["stats"]["top_values_withheld"] is True
        role["role"] = "admin"
        admin = c.get("/api/v1/data-quality/profiles/t144_contacts/columns").json()
        assert admin[0]["stats"]["top_values"][0]["value"] == "Austin"

    def test_seed_markers_in_data_source_and_origin(self, pg_engine):
        from sqlalchemy import text

        from app.catalog.quality import seed_rows

        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM t144_seed"))
            conn.execute(text("INSERT INTO t144_seed VALUES (1, 'demo_seeder', NULL), (2, 'sec', 'gjf_expanded'), "
                              "(3, 'sec', 'official'), (4, 'eia_sample', NULL)"))
        assert seed_rows(pg_engine, "t144_seed", 4) == 3
        assert seed_rows(pg_engine, "t144_seed", 4, where="id <> 1") == 2

    def test_profiler_stores_seed_rows_and_plain_get_does_not_scan(self, pg_engine, monkeypatch):
        from sqlalchemy import text

        import app.catalog.quality as q
        from app.core.data_profiling_service import profile_table

        _require_created(pg_engine, "substation")
        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM substation"))
            conn.execute(text("INSERT INTO substation (name, source) VALUES "
                              "('s1', 'hifld'), ('s2', 'hifld_sample'), ('s3', 'hifld')"))
        db = _session(pg_engine)
        try:
            assert profile_table(db, "substation", source="site_intel") is not None
        finally:
            db.close()

        def no_scan(*a, **k):
            raise AssertionError("a plain GET must not scan user tables")

        monkeypatch.setattr(q, "_count", no_scan)
        monkeypatch.setattr(q, "seed_rows", no_scan)
        spec = _spec("si_grid_infrastructure")
        live = {"tables": [{"table": "substation", "exists": True, "rows": 3, "rows_exact": True}]}
        block = q.quality_block(pg_engine, spec, live=live)
        sub = [e for e in block["tables"] if e["table"] == "substation"][0]
        assert sub["rows"] == 3 and sub["rows_exact"] is True
        assert sub["seed_rows"] == 1 and sub["seed_scan"] == "profile"
        assert block["live_state"] == "seed_contaminated" and block["measured"] == "cached"
        assert "seed_rows" in block["flags"] and "sample_mixed" in block["flags"]

    def test_row_filtered_table_gets_filtered_counts_and_no_whole_table_quality(self, pg_engine):
        from sqlalchemy import text

        from app.catalog.quality import quality_block, row_trends

        today = date.today()
        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM form_d_filings"))
            conn.execute(text("INSERT INTO form_d_filings VALUES ('a', now()), ('b', now()), ('c', now())"))
            conn.execute(text(
                "INSERT INTO dq_quality_snapshots (snapshot_date, source, table_name, quality_score, row_count, "
                "created_at) VALUES (:d, 'sec', 'form_d_filings', 90, 3, now())"), {"d": today})
        spec = _filtered_spec("sec_form_d", "t144_share", "form_d_filings", "accession_number = 'a'")
        block = quality_block(pg_engine, spec, refresh=True)
        t = block["tables"][0]
        assert t["rows"] == 1 and t["rows_exact"] is True
        assert t["row_filter"] == "accession_number = 'a'"
        assert t["quality_score"] is None and t["rules"] is None
        assert block["score"] is None and block["row_trend"] == []
        # a plain read without live counts: unknown, not the whole-table estimate
        from app.catalog import quality as q

        q.clear_cache()
        plain = quality_block(pg_engine, spec)
        assert plain["tables"][0]["rows"] is None and plain["tables"][0]["seed_scan"] == "filtered"
        assert "t144_share" not in row_trends(pg_engine, [spec, _spec("sec_form_d")], today=today)
        assert "sec_form_d" in row_trends(pg_engine, [spec, _spec("sec_form_d")], today=today)

    def test_dq_targets_mark_shared_tables(self, pg_engine):
        from app.catalog.quality import dq_targets

        db = _session(pg_engine)
        try:
            targets = {t["table"]: t for t in dq_targets(db)}
        finally:
            db.close()
        assert len(targets["pe_firms"]["shared_by"]) > 1
        assert targets["form_d_filings"]["shared_by"] == []
        assert targets["form_d_filings"]["row_filtered"] is False
