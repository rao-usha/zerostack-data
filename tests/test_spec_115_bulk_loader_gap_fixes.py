"""
Tests for SPEC 115 — Bulk loader gap fixes (no-op merges, shared 8-Ks, stale facts).
"""
import importlib.util
import json
import os
import zipfile
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

COLS = [("id", "INT"), ("name", "TEXT"), ("val", "INT")]
NAMES = [c for c, _ in COLS]


# ---------------------------------------------------------------------------
# merge_staging: skip unchanged rows
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_merge_sql_has_is_distinct_from_guard():
    """T1"""
    from app.core.copy_loader import build_merge_sql

    sql = " ".join(build_merge_sql("s", "public.t", NAMES, ["id"]).split()).lower()
    assert "is distinct from" in sql
    assert '"t"."name", "t"."val"' in sql

    opted_out = " ".join(
        build_merge_sql("s", "public.t", NAMES, ["id"], skip_unchanged=False).split()
    ).lower()
    assert "is distinct from" not in opted_out


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        conn.execute(text("DROP TABLE IF EXISTS public.t"))
        conn.execute(text("CREATE TABLE public.t (id INT PRIMARY KEY, name TEXT, val INT)"))
    yield engine
    engine.dispose()


def _merge(engine, rows):
    from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging

    with engine.begin() as conn:
        create_staging(conn, "s", COLS)
        copy_rows(conn, "s", NAMES, rows)
        result = merge_staging(conn, "s", "public.t", NAMES, ["id"])
        drop_staging(conn, "s")
    return result


@pg
def test_merge_skips_unchanged_rows_pg(pg_engine):
    """T2"""
    from sqlalchemy import text

    assert _merge(pg_engine, [(1, "a", 1), (2, "b", 2)]) == (2, 0)
    with pg_engine.connect() as conn:
        before = conn.execute(text("SELECT id, xmin::text FROM t ORDER BY id")).fetchall()

    assert _merge(pg_engine, [(1, "a", 1), (2, "b", 2)]) == (0, 0)
    with pg_engine.connect() as conn:
        after = conn.execute(text("SELECT id, xmin::text FROM t ORDER BY id")).fetchall()
    assert before == after  # untouched: no new row versions

    assert _merge(pg_engine, [(1, "a", 1), (2, "b2", 2)]) == (0, 1)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT name FROM t WHERE id=2")).scalar() == "b2"


@pg
def test_merge_still_inserts_and_updates_pg(pg_engine):
    """T3"""
    assert _merge(pg_engine, [(1, "a", 1)]) == (1, 0)
    assert _merge(pg_engine, [(1, "a", 9), (2, "b", 2)]) == (1, 1)


# ---------------------------------------------------------------------------
# shared 8-K filings
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_8k_index_key_includes_cik():
    """T4"""
    from app.ingest.bulk.sec_edgar_submissions.source import EdgarSubmissionsBulk

    src = EdgarSubmissionsBulk()
    ddl = " ".join(" ".join(src.ddl()).split()).lower()
    assert "primary key (accession_number, cik)" in ddl or "unique (accession_number, cik)" in ddl
    targets = {t[1]: t for t in src._targets()} if hasattr(src, "_targets") else {}
    if targets:
        assert targets["public.sec_8k_index"][-1] == ["accession_number", "cik"]


def _filer_json(cik, name, accession, filed="2025-03-03"):
    return {
        "cik": cik, "name": name, "entityType": "operating", "sic": "6282",
        "sicDescription": "Investment Advice", "ein": "123456789",
        "stateOfIncorporation": "DE", "fiscalYearEnd": "1231",
        "tickers": [], "exchanges": [], "phone": "212-555-0100",
        "addresses": {"business": {"street1": "1 Main St", "city": "New York",
                                   "stateOrCountry": "NY", "zipCode": "10001"},
                      "mailing": {}},
        "formerNames": [],
        "filings": {"recent": {
            "accessionNumber": [accession], "form": ["8-K"], "filingDate": [filed],
            "reportDate": [filed], "acceptanceDateTime": [f"{filed}T16:30:00.000Z"],
            "items": ["2.01"], "primaryDocument": ["a8k.htm"],
            "primaryDocDescription": ["8-K"], "size": [12345],
            "fileNumber": ["001-00001"], "filmNumber": ["25000001"],
            "act": ["34"], "isXBRL": [1], "isInlineXBRL": [1], "core_type": [""],
        }, "files": []},
    }


@pg
def test_shared_8k_keeps_every_company_pg(pg_engine, tmp_path):
    """T5"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_edgar_submissions.source import EdgarSubmissionsBulk

    accession = "0001104659-25-000001"
    zip_path = tmp_path / "submissions.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("CIK0000320193.json", json.dumps(_filer_json("320193", "Apple Inc.", accession)))
        zf.writestr("CIK0001527166.json", json.dumps(_filer_json("1527166", "Carlyle Group Inc.", accession)))

    src = EdgarSubmissionsBulk()
    rel = Release("snapshot:2026-09-18", "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip")
    with pg_engine.begin() as conn:
        for t in ("sec_8k_index", "sec_filers", "sec_filer_former_names"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
        src.load(conn, rel, zip_path)

    with pg_engine.connect() as conn:
        ciks = [r[0] for r in conn.execute(text(
            "SELECT cik FROM sec_8k_index WHERE accession_number = :a ORDER BY cik"),
            {"a": accession})]
    assert ciks == ["0000320193", "0001527166"]


# ---------------------------------------------------------------------------
# financial facts allowlist
# ---------------------------------------------------------------------------

FIXTURE = REPO / "tests" / "fixtures" / "sec_bulk" / "companyfacts_apple_trimmed.json"


@pytest.mark.unit
def test_build_financial_facts_allowlist():
    """T6"""
    from app.sources.sec.xbrl_parser import FACT_CONCEPTS, build_financial_facts

    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rows = build_financial_facts(data, "0000320193", FACT_CONCEPTS, date(2000, 1, 1))
    assert rows, "expected at least one allowlisted fact in the fixture"
    assert {r["fact_name"] for r in rows} <= set(FACT_CONCEPTS)
    for r in rows:
        assert r["period_end_date"] is not None
        assert r["fiscal_year"] and r["fiscal_period"]
    keys = [(r["fact_name"], r["period_end_date"], r["period_start_date"], r["unit"]) for r in rows]
    assert len(keys) == len(set(keys))  # latest filed wins, one row per period


@pg
def test_companyfacts_loads_facts_pg(pg_engine, tmp_path):
    """T7"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_companyfacts.source import SecCompanyFactsSource
    from app.sources.sec.models import SECBalanceSheet, SECCashFlowStatement, SECFinancialFact, SECIncomeStatement

    with pg_engine.begin() as conn:
        for model in (SECIncomeStatement, SECBalanceSheet, SECCashFlowStatement, SECFinancialFact):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{model.__tablename__} CASCADE"))
        for model in (SECIncomeStatement, SECBalanceSheet, SECCashFlowStatement, SECFinancialFact):
            model.__table__.create(conn)

    zip_path = tmp_path / "companyfacts.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("CIK0000320193.json", FIXTURE.read_text(encoding="utf-8"))

    src = SecCompanyFactsSource()
    rel = Release("snapshot:2026-09-18", "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip")
    with pg_engine.begin() as conn:
        first = src.load(conn, rel, zip_path)
    assert first.get("sec_financial_facts", 0) > 0

    with pg_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fact_name, fiscal_year, fiscal_period FROM sec_financial_facts ORDER BY period_end_date")).fetchall()
    assert rows and all(r[1] and r[2] for r in rows)

    with pg_engine.begin() as conn:
        second = src.load(conn, rel, zip_path)
    with pg_engine.connect() as conn:
        after = conn.execute(text("SELECT COUNT(*) FROM sec_financial_facts")).scalar()
    assert after == len(rows)
    assert second.get("sec_financial_facts", 0) == 0  # nothing changed on reload


# ---------------------------------------------------------------------------
# migration
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_migration_0007_chain_and_sql():
    """T8"""
    path = REPO / "alembic" / "versions" / "0007_bulk_gap_fixes.py"
    spec = importlib.util.spec_from_file_location("mig0007", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.revision == "0007_bulk_gap_fixes"
    assert mod.down_revision == "0006_xbrl_period_keys"
    sql = " ".join(mod.UPGRADE_SQL).lower()
    assert "quarantine.sec_financial_facts_pre_period_fix" in sql
    assert "sec_8k_index" in sql and "accession_number, cik" in sql
