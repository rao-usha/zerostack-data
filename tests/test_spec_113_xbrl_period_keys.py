"""
Tests for SPEC 113 — XBRL period-key fix + sec_companyfacts bulk loader.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import importlib.util
import json
import os
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "sec_bulk" / "companyfacts_apple_trimmed.json"
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

APPLE = "0000320193"


def _apple():
    return json.loads(FIXTURE.read_text())


def _by_period(rows):
    return {(r.get("period_start_date"), r["period_end_date"]): r for r in rows}


def _load_migration():
    path = REPO / "alembic" / "versions" / "0006_xbrl_period_keys.py"
    spec = importlib.util.spec_from_file_location("mig0006", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestParserPeriods:
    def test_fixture_is_small(self):
        assert FIXTURE.stat().st_size < 50_000

    def test_apple_fy2023_revenue_on_own_period(self):
        """T1"""
        from app.sources.sec.xbrl_parser import parse_company_facts

        out = parse_company_facts(_apple(), APPLE)
        income = _by_period(out["income_statement"])
        fy23 = income[(date(2022, 9, 25), date(2023, 9, 30))]
        assert fy23["revenues"] == Decimal("383285000000")
        assert fy23["net_income"] == Decimal("96995000000")
        assert (fy23["fiscal_year"], fy23["fiscal_period"]) == (2023, "FY")
        # Originating filing (the FY2023 10-K), not a later comparative
        assert fy23["accession_number"] == "0000320193-23-000106"
        assert fy23["form_type"] == "10-K"
        assert fy23["filing_date"] == date(2023, 11, 3)

        fy24 = income[(date(2023, 10, 1), date(2024, 9, 28))]
        assert fy24["revenues"] == Decimal("391035000000")
        assert (fy24["fiscal_year"], fy24["fiscal_period"]) == (2024, "FY")

        # No row labeled FY2024/FY2025 holds FY2023 revenue
        for r in out["income_statement"]:
            if r["revenues"] == Decimal("383285000000"):
                assert r["period_end_date"] == date(2023, 9, 30)
                assert r["fiscal_year"] == 2023
        # Every row's period_end is within its own period (no mis-keyed comparatives)
        for r in out["income_statement"]:
            days = (r["period_end_date"] - r["period_start_date"]).days
            assert 80 <= days <= 100 or 350 <= days <= 380

    def test_quarter_vs_ytd_only_discrete_quarter(self):
        """T2"""
        from app.sources.sec.xbrl_parser import parse_company_facts

        out = parse_company_facts(_apple(), APPLE)
        income = _by_period(out["income_statement"])
        q2 = income[(date(2023, 12, 31), date(2024, 3, 30))]
        assert q2["revenues"] == Decimal("90753000000")
        assert (q2["fiscal_year"], q2["fiscal_period"]) == (2024, "Q2")
        assert q2["accession_number"] == "0000320193-24-000069"
        # The 6-month YTD (2023-10-01..2024-03-30) is not stored
        assert (date(2023, 10, 1), date(2024, 3, 30)) not in income
        q3 = income[(date(2024, 3, 31), date(2024, 6, 29))]
        assert q3["fiscal_period"] == "Q3" and q3["revenues"] == Decimal("85777000000")
        # Cash flow: YTD op cash flow never becomes a row either
        for r in out["cash_flow"]:
            days = (r["period_end_date"] - r["period_start_date"]).days
            assert 80 <= days <= 100 or 350 <= days <= 380

    def test_restated_fact_latest_filed_wins(self):
        """T3"""
        from app.sources.sec.xbrl_parser import parse_company_facts

        data = _apple()
        rev = data["facts"]["us-gaap"]["RevenueFromContractWithCustomerExcludingAssessedTax"]["units"]["USD"]
        rev.append({"start": "2022-09-25", "end": "2023-09-30", "val": 383000000001,
                    "accn": "0000320193-26-000001", "fy": 2023, "fp": "FY", "form": "10-K/A",
                    "filed": "2026-01-15"})
        out = parse_company_facts(data, APPLE)
        fy23 = _by_period(out["income_statement"])[(date(2022, 9, 25), date(2023, 9, 30))]
        assert fy23["revenues"] == Decimal("383000000001")
        assert fy23["accession_number"] == "0000320193-23-000106"  # original filing

    def test_three_year_cutoff(self):
        """T4"""
        from app.sources.sec.xbrl_parser import build_financial_statements, three_year_cutoff

        assert three_year_cutoff(date(2026, 9, 16)) == date(2023, 9, 16)
        assert three_year_cutoff(date(2028, 2, 29)) == date(2025, 2, 28)
        out = build_financial_statements(_apple(), APPLE, min_period_end=date(2024, 1, 1))
        assert "financial_facts" not in out
        for key in ("income_statement", "balance_sheet", "cash_flow"):
            assert out[key], key
            assert all(r["period_end_date"] >= date(2024, 1, 1) for r in out[key])
        ends = {r["period_end_date"] for r in out["income_statement"]}
        assert date(2023, 9, 30) not in ends and date(2024, 9, 28) in ends

    def test_balance_sheet_instants(self):
        """T5"""
        from app.sources.sec.xbrl_parser import parse_company_facts

        out = parse_company_facts(_apple(), APPLE)
        bs = {r["period_end_date"]: r for r in out["balance_sheet"]}
        assert len(bs) == len(out["balance_sheet"])  # one row per end date
        fy23 = bs[date(2023, 9, 30)]
        assert fy23["total_assets"] == Decimal("352583000000")
        assert (fy23["fiscal_year"], fy23["fiscal_period"]) == (2023, "FY")
        assert fy23["accession_number"] == "0000320193-23-000106"
        q1 = bs[date(2023, 12, 30)]
        assert (q1["fiscal_year"], q1["fiscal_period"]) == (2024, "Q1")
        assert "period_start_date" not in fy23

    def test_financial_facts_deduped_and_duration_labels(self):
        """T6"""
        from app.sources.sec.xbrl_parser import parse_company_facts

        out = parse_company_facts(_apple(), APPLE)
        facts = out["financial_facts"]
        keys = [(f["fact_name"], f["unit"], f["period_start_date"], f["period_end_date"]) for f in facts]
        assert len(keys) == len(set(keys))
        rev = [f for f in facts if f["fact_name"] == "RevenueFromContractWithCustomerExcludingAssessedTax"]
        by = {(f["period_start_date"], f["period_end_date"]): f for f in rev}
        assert by[(date(2023, 10, 1), date(2024, 3, 30))]["fiscal_period"] == "H1"
        assert by[(date(2023, 12, 31), date(2024, 3, 30))]["fiscal_period"] == "Q2"
        assert by[(date(2023, 10, 1), date(2024, 6, 29))]["fiscal_period"] == "9M"
        # The existing unique key is now unique per own period
        uk = [(f["cik"], f["fact_name"], f["period_end_date"], f["fiscal_year"], f["fiscal_period"], f["unit"])
              for f in facts]
        assert len(uk) == len(set(uk))

    def test_fallback_labels_without_originating_filing(self):
        from app.sources.sec.xbrl_parser import parse_company_facts

        data = {"entityName": "X", "facts": {"us-gaap": {"Revenues": {"units": {"USD": [
            # Only comparatives from a later filing (docEnd 2021-12-31)
            {"start": "2021-01-01", "end": "2021-12-31", "val": 10, "accn": "A2", "fy": 2021, "fp": "FY",
             "form": "10-K", "filed": "2022-02-01"},
            {"start": "2020-01-01", "end": "2020-12-31", "val": 9, "accn": "A2", "fy": 2021, "fp": "FY",
             "form": "10-K", "filed": "2022-02-01"},
            {"start": "2020-07-01", "end": "2020-09-30", "val": 3, "accn": "A2", "fy": 2021, "fp": "FY",
             "form": "10-K", "filed": "2022-02-01"},
        ]}}}}}
        rows = _by_period(parse_company_facts(data, "0000000001")["income_statement"])
        fy20 = rows[(date(2020, 1, 1), date(2020, 12, 31))]
        assert (fy20["fiscal_year"], fy20["fiscal_period"]) == (2020, "FY")
        q = rows[(date(2020, 7, 1), date(2020, 9, 30))]
        assert (q["fiscal_year"], q["fiscal_period"]) == (2020, "Q3")
        fy21 = rows[(date(2021, 1, 1), date(2021, 12, 31))]
        assert (fy21["fiscal_year"], fy21["fiscal_period"]) == (2021, "FY")


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------

def _unique_cols(model):
    from sqlalchemy import UniqueConstraint

    return {c.name: tuple(col.name for col in c.columns)
            for c in model.__table__.constraints if isinstance(c, UniqueConstraint)}


@pytest.mark.unit
def test_model_unique_keys():
    """T7"""
    from app.sources.sec.models import SECBalanceSheet, SECCashFlowStatement, SECIncomeStatement

    assert _unique_cols(SECIncomeStatement) == {
        "uq_sec_income_cik_period_bounds": ("cik", "period_end_date", "period_start_date")}
    assert _unique_cols(SECCashFlowStatement) == {
        "uq_sec_cashflow_cik_period_bounds": ("cik", "period_end_date", "period_start_date")}
    assert _unique_cols(SECBalanceSheet) == {"uq_sec_balance_cik_period_end": ("cik", "period_end_date")}
    assert SECIncomeStatement.__table__.c.period_start_date.nullable is False
    assert SECCashFlowStatement.__table__.c.period_start_date.nullable is False


@pytest.mark.unit
def test_ingest_conflict_columns():
    """T8"""
    from app.sources.sec import ingest_xbrl
    from app.sources.sec.models import SECBalanceSheet, SECCashFlowStatement, SECIncomeStatement

    assert ingest_xbrl.STATEMENT_CONFLICT_COLUMNS[SECIncomeStatement] == ["cik", "period_end_date", "period_start_date"]
    assert ingest_xbrl.STATEMENT_CONFLICT_COLUMNS[SECCashFlowStatement] == ["cik", "period_end_date", "period_start_date"]
    assert ingest_xbrl.STATEMENT_CONFLICT_COLUMNS[SECBalanceSheet] == ["cik", "period_end_date"]
    src = (REPO / "app" / "sources" / "sec" / "bulk_ingest_orchestrator.py").read_text()
    assert '"fiscal_year", "fiscal_period"]' not in src
    assert "STATEMENT_CONFLICT_COLUMNS" in src


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_migration_0006_chain():
    """T9"""
    mod = _load_migration()
    assert mod.revision == "0006_xbrl_period_keys"
    assert mod.down_revision == "0005_bulk_source_tables"
    joined = " ".join(mod.UPGRADE_SQL).lower()
    assert "create schema if not exists quarantine" in joined
    assert "sec_income_statement_pre_period_fix" in joined


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine

    engine = create_engine(PG_URL)
    yield engine
    engine.dispose()


def _reset_tables(conn, old_keys: bool):
    from sqlalchemy import text
    from app.sources.sec.models import SECBalanceSheet, SECCashFlowStatement, SECIncomeStatement

    for t in ("sec_income_statement", "sec_balance_sheet", "sec_cash_flow_statement"):
        conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        for suffix in ("_pre_period_fix", "_post_period_fix"):
            conn.execute(text(f"DROP TABLE IF EXISTS quarantine.{t}{suffix}"))
    for m in (SECIncomeStatement, SECBalanceSheet, SECCashFlowStatement):
        m.__table__.create(conn)
    if old_keys:
        mod = _load_migration()
        for t, (old_name, old_cols), (new_name, _) in mod.KEYS:
            conn.execute(text(f"ALTER TABLE {t} DROP CONSTRAINT {new_name}"))
            conn.execute(text(f"ALTER TABLE {t} ADD CONSTRAINT {old_name} UNIQUE ({old_cols})"))
            if t != "sec_balance_sheet":
                conn.execute(text(f"ALTER TABLE {t} ALTER COLUMN period_start_date DROP NOT NULL"))


def _constraints(conn, table):
    from sqlalchemy import text

    return set(conn.execute(text(
        "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:t) AND contype = 'u'"),
        {"t": f"public.{table}"}).scalars())


@pg
def test_migration_0006_sql_pg(pg_engine):
    """T10"""
    from sqlalchemy import text

    mod = _load_migration()
    with pg_engine.begin() as conn:
        _reset_tables(conn, old_keys=True)
        assert "uq_sec_income_cik_period" in _constraints(conn, "sec_income_statement")
        conn.execute(text(
            "INSERT INTO sec_income_statement (cik, period_end_date, fiscal_year, fiscal_period, revenues) "
            "VALUES (:c, :e, :fy, :fp, :r)"), {"c": APPLE, "e": date(2023, 9, 30), "fy": 2025, "fp": "FY", "r": 1})
        conn.execute(text(
            "INSERT INTO sec_balance_sheet (cik, period_end_date, fiscal_year, fiscal_period) "
            "VALUES (:c, :e, 2024, 'FY')"), {"c": APPLE, "e": date(2023, 9, 30)})

        for stmt in mod.UPGRADE_SQL:
            conn.execute(text(stmt))

        assert conn.execute(text("SELECT COUNT(*) FROM sec_income_statement")).scalar() == 0
        assert conn.execute(text("SELECT COUNT(*) FROM sec_balance_sheet")).scalar() == 0
        assert conn.execute(text(
            "SELECT fiscal_year FROM quarantine.sec_income_statement_pre_period_fix")).scalars().all() == [2025]
        assert conn.execute(text("SELECT COUNT(*) FROM quarantine.sec_balance_sheet_pre_period_fix")).scalar() == 1
        assert _constraints(conn, "sec_income_statement") == {"uq_sec_income_cik_period_bounds"}
        assert _constraints(conn, "sec_cash_flow_statement") == {"uq_sec_cashflow_cik_period_bounds"}
        assert _constraints(conn, "sec_balance_sheet") == {"uq_sec_balance_cik_period_end"}

        # New key works: annual + quarter with the same end coexist; duplicate conflicts.
        ins = ("INSERT INTO sec_income_statement (cik, period_end_date, period_start_date, fiscal_year, fiscal_period)"
               " VALUES (:c, :e, :s, 2024, :fp) ON CONFLICT (cik, period_end_date, period_start_date) DO NOTHING")
        conn.execute(text(ins), {"c": APPLE, "e": date(2024, 9, 28), "s": date(2023, 10, 1), "fp": "FY"})
        conn.execute(text(ins), {"c": APPLE, "e": date(2024, 9, 28), "s": date(2024, 6, 30), "fp": "Q4"})
        conn.execute(text(ins), {"c": APPLE, "e": date(2024, 9, 28), "s": date(2024, 6, 30), "fp": "Q4"})
        assert conn.execute(text("SELECT COUNT(*) FROM sec_income_statement")).scalar() == 2

        # Re-running upgrade is a no-op (does not delete the new rows)
        for stmt in mod.UPGRADE_SQL:
            conn.execute(text(stmt))
        assert conn.execute(text("SELECT COUNT(*) FROM sec_income_statement")).scalar() == 2

        # Downgrade restores the old rows + old key
        for stmt in mod.DOWNGRADE_SQL:
            conn.execute(text(stmt))
        assert conn.execute(text("SELECT fiscal_year FROM sec_income_statement")).scalars().all() == [2025]
        assert conn.execute(text(
            "SELECT COUNT(*) FROM quarantine.sec_income_statement_post_period_fix")).scalar() == 2
        assert _constraints(conn, "sec_income_statement") == {"uq_sec_income_cik_period"}
        assert _constraints(conn, "sec_balance_sheet") == {"uq_sec_balance_cik_period"}


@pg
def test_migration_0006_fresh_db_pg(pg_engine):
    """Tables absent at migration time (fresh DB: migrations run before create_all)."""
    from sqlalchemy import text

    mod = _load_migration()
    with pg_engine.begin() as conn:
        for t in ("sec_income_statement", "sec_balance_sheet", "sec_cash_flow_statement"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        for stmt in mod.UPGRADE_SQL:
            conn.execute(text(stmt))
        assert conn.execute(text("SELECT to_regclass('public.sec_income_statement')")).scalar() is None


# ---------------------------------------------------------------------------
# Bulk source
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_bulk_discover_snapshot():
    """T11"""
    from app.ingest.bulk import registry
    from app.ingest.bulk.sec_companyfacts.source import COMPANYFACTS_URL, SecCompanyFactsSource

    src = SecCompanyFactsSource()
    releases = src.discover(http=None)
    assert len(releases) == 1
    rel = releases[0]
    assert rel.release_key.startswith("snapshot:") and len(rel.release_key) == len("snapshot:2026-09-16")
    assert rel.url == COMPANYFACTS_URL == "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    assert src.name == "sec_companyfacts"
    assert "sec_companyfacts" in registry.list_sources()


@pytest.mark.unit
def test_bulk_row_sanitizer_nulls_overflow():
    from app.ingest.bulk.sec_companyfacts.source import TABLES

    spec = TABLES["sec_income_statement"]
    row = {"cik": APPLE, "period_end_date": date(2024, 1, 1), "period_start_date": date(2023, 1, 1),
           "fiscal_year": 2023, "fiscal_period": "FY", "revenues": Decimal("1e25"),
           "earnings_per_share_basic": Decimal("12.5")}
    out = dict(zip(spec.columns, spec.to_tuple(row)))
    assert out["revenues"] is None
    assert out["earnings_per_share_basic"] == Decimal("12.5")


def _fixture_zip(tmp_path):
    other = {"cik": 1, "entityName": "Tiny Co", "facts": {"us-gaap": {"Revenues": {"units": {"USD": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1000, "accn": "0000000001-25-000001",
         "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2025-03-01"}]}},
        "Assets": {"units": {"USD": [
            {"end": "2024-12-31", "val": 5000, "accn": "0000000001-25-000001", "fy": 2024, "fp": "FY",
             "form": "10-K", "filed": "2025-03-01"}]}}}}}
    path = tmp_path / "companyfacts.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("CIK0000320193.json", FIXTURE.read_text())
        zf.writestr("CIK0000000001.json", json.dumps(other))
        zf.writestr("CIK0000000002.json", "{not json")  # corrupt member is skipped
        zf.writestr("README.txt", "ignore me")
    return path


@pg
def test_bulk_load_fixture_zip_twice_pg(pg_engine, tmp_path):
    """T12"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_companyfacts.source import SecCompanyFactsSource

    with pg_engine.begin() as conn:
        _reset_tables(conn, old_keys=False)

    path = _fixture_zip(tmp_path)
    src = SecCompanyFactsSource(batch_companies=1)
    rel = Release("snapshot:2025-06-01", src.discover(None)[0].url)  # cutoff 2022-06-01
    with pg_engine.begin() as conn:
        first = src.load(conn, rel, path)

    def snapshot():
        with pg_engine.connect() as conn:
            return {t: conn.execute(text(
                f"SELECT cik, period_end_date, fiscal_year, fiscal_period FROM {t} ORDER BY 1, 2, 3, 4")).fetchall()
                for t in ("sec_income_statement", "sec_balance_sheet", "sec_cash_flow_statement")}

    s1 = snapshot()
    assert first["sec_income_statement"] == len(s1["sec_income_statement"]) > 0
    assert first["sec_balance_sheet"] == len(s1["sec_balance_sheet"]) > 0
    with pg_engine.connect() as conn:
        rev = conn.execute(text(
            "SELECT revenues, fiscal_year, fiscal_period FROM sec_income_statement "
            "WHERE cik = :c AND period_end_date = :e AND period_start_date = :s"),
            {"c": APPLE, "e": date(2023, 9, 30), "s": date(2022, 9, 25)}).one()
        assert rev == (Decimal("383285000000.00"), 2023, "FY")
        assert conn.execute(text(
            "SELECT total_assets FROM sec_balance_sheet WHERE cik = '0000000001'")).scalar() == Decimal("5000.00")
        assert conn.execute(text(
            "SELECT MIN(period_end_date) FROM sec_income_statement WHERE cik = :c"), {"c": APPLE}).scalar() \
            >= date(2022, 6, 1)
        assert conn.execute(text("SELECT to_regclass('stg.sec_companyfacts_sec_income_statement')")).scalar() is None

    with pg_engine.begin() as conn:
        second = src.load(conn, rel, path)
    assert snapshot() == s1
    assert all(v == 0 for v in second.values())  # unchanged rows are not rewritten (SPEC_115)


@pg
def test_bulk_load_requires_new_keys_pg(pg_engine, tmp_path):
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_companyfacts.source import SecCompanyFactsSource

    with pg_engine.begin() as conn:
        _reset_tables(conn, old_keys=True)
    src = SecCompanyFactsSource()
    with pytest.raises(RuntimeError, match="0006"):
        with pg_engine.begin() as conn:
            src.load(conn, Release("snapshot:2025-06-01", "x"), _fixture_zip(tmp_path))


# ---------------------------------------------------------------------------
# Removal
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_edgar_company_facts_removed():
    """T13"""
    assert not (REPO / "app" / "sources" / "edgar_company_facts").exists()
    hits = []
    for p in (REPO / "app").rglob("*.py"):
        if "edgar_company_facts" in p.read_text(encoding="utf-8", errors="ignore"):
            hits.append(str(p))
    assert hits == []
