"""
Tests for SPEC 117 — PE firms and funds from Form ADV + Form D.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import importlib.util
import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


# ---------------------------------------------------------------------------
# row mapping + link tiers (pure)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestMapping:
    def test_firm_row_mapping(self):
        """T1"""
        from app.marts.pe_firms_sec import COLUMN_NAMES, _row

        raw = {
            "crd_number": "8361", "legal_name": "GENSTAR CAPITAL LLC",
            "business_name": "GENSTAR CAPITAL", "sec_number": "801-60166",
            "adviser_type": "ria", "main_office_city": "San Francisco",
            "main_office_state": "CA", "main_office_country": "United States",
            "website": "https://genstarcapital.com", "aum_total": 35_000_000_000,
            "sec_status": "APPROVED", "roster_date": date(2026, 9, 1),
            "any_pe": "Y", "any_vc": None, "cik": "1002784",
        }
        row = dict(zip(COLUMN_NAMES, _row(raw, date(2026, 9, 20))))
        assert row["name"] == "GENSTAR CAPITAL"           # business name
        assert row["legal_name"] == "GENSTAR CAPITAL LLC"
        assert row["crd_number"] == "8361" and row["sec_file_number"] == "801-60166"
        assert row["cik"] == "1002784"
        assert row["is_sec_registered"] is True
        assert row["aum_usd_millions"] == 35_000            # dollars -> millions
        assert row["firm_type"] == "PE"                   # the app's filter vocabulary
        assert row["primary_strategy"] == "Private Equity"
        assert json.loads(row["data_sources"]) == ["SEC ADV"]
        assert row["status"] == "Active"

    def test_firm_mapping_era_and_both_strategies(self):
        """T1b: ERAs report no AUM and are not SEC-registered"""
        from app.marts.pe_firms_sec import COLUMN_NAMES, _row

        row = dict(zip(COLUMN_NAMES, _row(
            {"crd_number": "99", "legal_name": "SEED FUND MGMT", "business_name": None,
             "adviser_type": "era", "aum_total": None, "any_pe": "Y", "any_vc": "Y",
             "sec_status": "TERMINATED"}, date(2026, 9, 20))))
        assert row["aum_usd_millions"] is None
        assert row["is_sec_registered"] is False
        assert row["firm_type"] == "PE"
        assert row["primary_strategy"] == "Private Equity & Venture Capital"
        assert row["status"] == "Inactive"
        assert row["name"] == "SEED FUND MGMT"   # falls back to legal name

    def test_firm_universe_is_pe_or_vc_only(self):
        """T2"""
        from app.marts.pe_firms_sec import SOURCE_SQL

        sql = " ".join(SOURCE_SQL.split()).lower()
        assert "'any pe funds' = 'y' or l.raw->>'any vc funds' = 'y'" in sql
        assert "distinct on (crd_number)" in sql  # latest snapshot only

    def test_link_name_core_tier(self):
        """T5"""
        from app.marts.links import build_core_index, match_name_core

        index = build_core_index([
            ("8361", "GENSTAR CAPITAL LLC", "GENSTAR CAPITAL"),
            ("4242", "NORTH PEAK ADVISORS LLC", None),
            # same core claimed by two advisers -> ambiguous, dropped
            ("1111", "SUMMIT PARTNERS LLC", None),
            ("2222", "SUMMIT PARTNERS LP", None),
        ])
        assert match_name_core("GENSTAR CAPITAL PARTNERS X, L.P.", index) == "8361"
        assert match_name_core("NORTH PEAK ADVISORS FUND II LP", index) == "4242"
        # measured limitation: a fund named off a different brand than the
        # adviser's legal name does not match here (the related-person tier does)
        assert match_name_core("NORTH PEAK VENTURES II LP", index) is None
        assert match_name_core("SUMMIT PARTNERS GROWTH EQUITY FUND XI", index) is None
        assert match_name_core("UNRELATED FUND LP", index) is None

    def test_link_related_person_tier(self):
        """T6"""
        from app.marts.links import build_core_index, match_related_persons

        index = build_core_index([("8361", "GENSTAR CAPITAL LLC", None),
                                  ("4242", "NORTH PEAK ADVISORS LLC", None)])
        assert match_related_persons(["Genstar Capital LLC", "Jane Doe"], index) == "8361"
        assert match_related_persons(["Jane Doe"], index) is None
        # two different advisers named -> ambiguous
        assert match_related_persons(["Genstar Capital", "North Peak Advisors"], index) is None

    def test_migration_0009_chain(self):
        """T10"""
        path = REPO / "alembic" / "versions" / "0009_pe_mart_keys.py"
        spec = importlib.util.spec_from_file_location("mig0009", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.revision == "0009_pe_mart_keys"
        assert mod.down_revision == "0008_entity_master"
        sql = " ".join(mod.UPGRADE_SQL).lower()
        assert "unique index if not exists uq_pe_firms_crd_number" in sql
        assert "alter table pe_funds alter column firm_id drop not null" in sql
        assert "add column if not exists cik" in sql

    def test_quarantine_rules_cover_sec_rows(self):
        """T11"""
        from app.core.quarantine import SEC_MART_RULES

        by_table = {r.table: r for r in SEC_MART_RULES}
        assert "SEC Form D" in by_table["pe_funds"].predicate
        assert "SEC ADV" in by_table["pe_firms"].predicate
        assert all(r.target_schema == "quarantine" for r in SEC_MART_RULES)


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core"))
        conn.execute(text("CREATE TABLE core.identifier (id_type TEXT, id_value TEXT, entity_id BIGINT)"))
        for t in ("pe_funds", "pe_firms", "sec_adv_roster_snapshots", "form_d_offerings",
                  "form_d_issuers", "form_d_related_persons"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        conn.execute(text("""
            CREATE TABLE pe_firms (id SERIAL PRIMARY KEY, name TEXT NOT NULL, legal_name TEXT,
                website TEXT, headquarters_city TEXT, headquarters_state TEXT,
                headquarters_country TEXT, firm_type TEXT, primary_strategy TEXT,
                aum_usd_millions NUMERIC, cik TEXT, sec_file_number TEXT, crd_number TEXT,
                is_sec_registered BOOLEAN, status TEXT, data_sources JSON,
                last_verified_date DATE, updated_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE pe_funds (id SERIAL PRIMARY KEY, firm_id INTEGER, name TEXT NOT NULL,
                cik TEXT, vintage_year INTEGER, target_size_usd_millions NUMERIC,
                final_close_usd_millions NUMERIC, strategy TEXT, status TEXT,
                first_close_date DATE, sec_file_number TEXT, data_source TEXT,
                updated_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
                legal_name TEXT, business_name TEXT, sec_number TEXT, adviser_type TEXT,
                main_office_city TEXT, main_office_state TEXT, main_office_country TEXT,
                website TEXT, aum_total NUMERIC, sec_status TEXT, raw JSONB)"""))
        conn.execute(text("""
            CREATE TABLE form_d_offerings (accession_number TEXT, investment_fund_type TEXT,
                date_of_first_sale DATE, total_offering_amount NUMERIC, total_amount_sold NUMERIC,
                is_indefinite BOOLEAN, file_num TEXT, loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("""
            CREATE TABLE form_d_issuers (accession_number TEXT, cik TEXT, entity_name TEXT,
                is_primary BOOLEAN)"""))
        conn.execute(text("""
            CREATE TABLE form_d_related_persons (accession_number TEXT, first_name TEXT,
                middle_name TEXT, last_name TEXT)"""))
        for stmt in _upgrade_sql():
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _upgrade_sql():
    path = REPO / "alembic" / "versions" / "0009_pe_mart_keys.py"
    spec = importlib.util.spec_from_file_location("mig0009b", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.UPGRADE_SQL


def _seed(engine):
    from sqlalchemy import text

    with engine.begin() as conn:
        # a hand-entered firm and fund that the build must not touch
        conn.execute(text("""
            INSERT INTO pe_firms (name, website, status, data_sources)
            VALUES ('Hand Typed Capital', 'https://handtyped.com', 'Active', '["manual"]')"""))
        conn.execute(text("""
            INSERT INTO pe_funds (firm_id, name, strategy)
            VALUES (1, 'Hand Typed Fund I', 'Buyout')"""))
        conn.execute(text("""
            INSERT INTO sec_adv_roster_snapshots (crd_number, roster_date, legal_name, business_name,
                sec_number, adviser_type, main_office_city, main_office_state, main_office_country,
                website, aum_total, sec_status, raw)
            VALUES
              ('8361','2026-09-01','GENSTAR CAPITAL LLC','GENSTAR CAPITAL','801-60166','ria',
               'San Francisco','CA','United States','https://genstarcapital.com',35000000000,'APPROVED',
               '{"Any PE Funds":"Y","Any VC Funds":"N"}'),
              ('8361','2026-07-01','GENSTAR CAPITAL LLC','OLD NAME','801-60166','ria',
               'San Francisco','CA','United States',NULL,30000000000,'APPROVED',
               '{"Any PE Funds":"Y","Any VC Funds":"N"}'),
              ('4242','2026-09-01','NORTH PEAK ADVISORS LLC',NULL,'802-11111','era',
               'Austin','TX','United States',NULL,NULL,'APPROVED',
               '{"Any PE Funds":"N","Any VC Funds":"Y"}'),
              ('7777','2026-09-01','HEDGEONLY PARTNERS LLC',NULL,'801-22222','ria',
               'New York','NY','United States',NULL,9000000000,'APPROVED',
               '{"Any PE Funds":"N","Any VC Funds":"N","Any Hedge Funds":"Y"}')"""))
        conn.execute(text("""
            INSERT INTO form_d_issuers (accession_number, cik, entity_name, is_primary) VALUES
              ('acc-1','0000000101','GENSTAR CAPITAL PARTNERS X, L.P.', true),
              ('acc-2','0000000101','GENSTAR CAPITAL PARTNERS X, L.P.', true),
              ('acc-3','0000000202','SUMMIT UNRELATED FUND II LP', true),
              ('acc-4','0000000303','NP VENTURES FUND I LP', true)"""))
        conn.execute(text("""
            INSERT INTO form_d_offerings (accession_number, investment_fund_type, date_of_first_sale,
                total_offering_amount, total_amount_sold, is_indefinite, file_num) VALUES
              ('acc-1','Private Equity Fund','2024-03-01', 500000000, 250000000, false, '021-111111'),
              ('acc-2','Private Equity Fund','2025-06-01', NULL, 600000000, true, '021-111111'),
              ('acc-3','Private Equity Fund','2023-01-15', 100000000, 90000000, false, '021-222222'),
              ('acc-4','Venture Capital Fund','2026-02-01', 75000000, 20000000, false, '021-333333')"""))
        conn.execute(text("""
            INSERT INTO form_d_related_persons (accession_number, first_name, middle_name, last_name)
            VALUES ('acc-4','North Peak',NULL,'Advisors LLC'), ('acc-3','Jane',NULL,'Doe')"""))


@pg
def test_build_firms_pg(pg_engine):
    """T7"""
    from sqlalchemy import text
    from app.marts import pe_firms_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        stats = pe_firms_sec.build(conn, today=date(2026, 9, 20))
    assert stats["inserted"] == 2 and stats["candidates"] == 2   # hedge-only adviser excluded

    with pg_engine.connect() as conn:
        rows = {r["crd_number"]: dict(r) for r in conn.execute(text(
            "SELECT crd_number, name, aum_usd_millions, firm_type, primary_strategy, is_sec_registered "
            "FROM pe_firms WHERE crd_number IS NOT NULL")).mappings()}
    assert rows["8361"]["name"] == "GENSTAR CAPITAL"      # latest snapshot wins
    assert rows["8361"]["aum_usd_millions"] == 35000
    assert rows["4242"]["firm_type"] == "VC"
    assert rows["4242"]["is_sec_registered"] is False

    with pg_engine.begin() as conn:
        again = pe_firms_sec.build(conn, today=date(2026, 9, 20))
    assert (again["inserted"], again["updated"]) == (0, 0)   # skip-unchanged merge


@pg
def test_duplicate_adviser_names_disambiguated_pg(pg_engine):
    """pe_firms.name is UNIQUE and advisers share names — suffix with the CRD."""
    from sqlalchemy import text
    from app.marts import pe_firms_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sec_adv_roster_snapshots (crd_number, roster_date, legal_name, business_name,
                adviser_type, sec_status, raw)
            VALUES ('5001','2026-09-01','TWIN PEAKS CAPITAL LLC','TWIN PEAKS CAPITAL','ria','APPROVED',
                    '{"Any PE Funds":"Y"}'),
                   ('5002','2026-09-01','TWIN PEAKS CAPITAL LP','TWIN PEAKS CAPITAL','ria','APPROVED',
                    '{"Any PE Funds":"Y"}')"""))
        stats = pe_firms_sec.build(conn, today=date(2026, 9, 20))
    assert stats["name_disambiguated"] == 2
    with pg_engine.connect() as conn:
        names = sorted(r[0] for r in conn.execute(text(
            "SELECT name FROM pe_firms WHERE crd_number IN ('5001','5002')")))
    assert names == ["TWIN PEAKS CAPITAL (CRD 5001)", "TWIN PEAKS CAPITAL (CRD 5002)"]
    with pg_engine.connect() as conn:
        legal = sorted(r[0] for r in conn.execute(text(
            "SELECT legal_name FROM pe_firms WHERE crd_number IN ('5001','5002')")))
    assert legal == ["TWIN PEAKS CAPITAL LLC", "TWIN PEAKS CAPITAL LP"]  # real spelling kept


@pg
def test_build_funds_pg(pg_engine):
    """T8 / T4"""
    from sqlalchemy import text
    from app.marts import pe_firms_sec, pe_funds_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        pe_firms_sec.build(conn, today=date(2026, 9, 20))
        stats = pe_funds_sec.build(conn, today=date(2026, 9, 20))

    assert stats["candidates"] == 3          # one row per issuer CIK, not per filing
    assert stats["inserted"] == 3
    assert stats["linked_name_core"] == 1    # Genstar fund -> Genstar adviser
    assert stats["linked_related_person"] == 1  # NP Ventures -> North Peak via related person
    assert stats["unlinked"] == 1

    with pg_engine.connect() as conn:
        funds = {r["cik"]: dict(r) for r in conn.execute(text(
            "SELECT cik, name, vintage_year, target_size_usd_millions, final_close_usd_millions, "
            "strategy, firm_id, sec_file_number FROM pe_funds WHERE data_source = 'SEC Form D'"
        )).mappings()}
    genstar = funds["0000000101"]
    assert genstar["vintage_year"] == 2025                     # latest filing wins
    assert genstar["target_size_usd_millions"] is None         # indefinite offering
    assert genstar["final_close_usd_millions"] == 600
    assert genstar["firm_id"] is not None
    assert funds["0000000202"]["firm_id"] is None              # unattributed, not guessed
    assert funds["0000000303"]["strategy"] == "Venture Capital"

    with pg_engine.begin() as conn:
        again = pe_funds_sec.build(conn, today=date(2026, 9, 20))
    assert (again["inserted"], again["updated"]) == (0, 0)


@pg
def test_existing_rows_untouched_pg(pg_engine):
    """T9"""
    from sqlalchemy import text
    from app.marts import pe_firms_sec, pe_funds_sec

    _seed(pg_engine)
    with pg_engine.connect() as conn:
        before = conn.execute(text(
            "SELECT id, name, website, data_sources FROM pe_firms WHERE crd_number IS NULL")).fetchall()
        before_fund = conn.execute(text(
            "SELECT id, name, firm_id, strategy FROM pe_funds WHERE cik IS NULL")).fetchall()

    with pg_engine.begin() as conn:
        pe_firms_sec.build(conn, today=date(2026, 9, 20))
        pe_funds_sec.build(conn, today=date(2026, 9, 20))

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT id, name, website, data_sources FROM pe_firms WHERE crd_number IS NULL")).fetchall()
        after_fund = conn.execute(text(
            "SELECT id, name, firm_id, strategy FROM pe_funds WHERE cik IS NULL")).fetchall()
    assert before == after and before_fund == after_fund


# ---------------------------------------------------------------------------
# executor / endpoint
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestExecutorAndEndpoint:
    @pytest.mark.asyncio
    async def test_executor_runs_in_thread(self):
        """T12"""
        from app.worker.executors import pe_marts

        job = MagicMock(payload={}, id=4)
        db = MagicMock()
        seen = {}

        async def fake_to_thread(fn, *args, **kwargs):
            seen["fn"] = fn
            return {"firms": {"inserted": 7452, "updated": 0},
                    "funds": {"inserted": 39141, "linked_name_core": 4855,
                              "linked_related_person": 1590}}

        with patch.object(pe_marts.asyncio, "to_thread", fake_to_thread):
            await pe_marts.execute(job, db)
        assert seen["fn"] is pe_marts.run_pe_marts
        assert "firms +7452" in job.progress_message

    def test_build_endpoint_queues_job(self):
        """T12"""
        from app.api.v1 import pe_marts

        db = MagicMock()
        with patch.object(pe_marts, "submit_job", return_value={"job_queue_id": 11}) as sj:
            out = pe_marts.queue_build(skip_firms=False, skip_funds=True, db=db)
        assert out == {"job_queue_id": 11}
        assert sj.call_args.kwargs["job_type"] == "pe_mart_build"
        assert sj.call_args.kwargs["payload"] == {"skip_firms": False, "skip_funds": True}
