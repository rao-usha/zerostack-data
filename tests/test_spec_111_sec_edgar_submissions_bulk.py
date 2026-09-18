"""
Tests for SPEC 111 — EDGAR submissions bulk loader + D18 SEC DDL fix.

Fixtures are trimmed copies of real data.sec.gov/submissions payloads
(Apple 0000320193, Warren Buffett 0000315090), fetched 2026-09-16.
PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import copy
import json
import os
import re
import zipfile
from datetime import date
from unittest.mock import MagicMock

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

SNAPSHOT = "snapshot:2026-09-16"

APPLE = {
    "cik": "0000320193", "entityType": "operating", "sic": "3571",
    "sicDescription": "Electronic Computers", "ownerOrg": "06 Technology",
    "insiderTransactionForOwnerExists": 0, "insiderTransactionForIssuerExists": 1,
    "name": "Apple Inc.", "tickers": ["AAPL"], "exchanges": ["Nasdaq"],
    "ein": "942404110", "lei": None, "description": "", "website": "",
    "investorWebsite": "", "category": "Large accelerated filer", "fiscalYearEnd": "0926",
    "stateOfIncorporation": "CA", "stateOfIncorporationDescription": "CA",
    "addresses": {
        "mailing": {"street1": "ONE APPLE PARK WAY", "street2": None, "city": "CUPERTINO",
                    "stateOrCountry": "CA", "zipCode": "95014", "stateOrCountryDescription": "CA",
                    "isForeignLocation": 0, "foreignStateTerritory": None, "country": None,
                    "countryCode": None},
        "business": {"street1": "ONE APPLE PARK WAY", "street2": "SUITE 1", "city": "CUPERTINO",
                     "stateOrCountry": "CA", "zipCode": "95014-2083",
                     "stateOrCountryDescription": "CA", "isForeignLocation": None,
                     "foreignStateTerritory": None, "country": None, "countryCode": None},
    },
    "phone": "(408) 996-1010", "flags": "",
    "formerNames": [
        {"name": "APPLE INC", "from": "2007-01-10T05:00:00.000Z", "to": "2019-08-05T04:00:00.000Z"},
        {"name": "APPLE COMPUTER INC", "from": "1994-01-26T05:00:00.000Z", "to": "2007-01-04T05:00:00.000Z"},
        {"name": "  ", "from": "1990-01-01T00:00:00.000Z", "to": None},
    ],
    "filings": {
        "recent": {
            "accessionNumber": ["0001140361-26-035325", "0000320193-26-000010",
                                "0000320193-25-000001", "0000320193-23-000070",
                                "0000320193-23-000050"],
            "filingDate": ["2026-09-01", "2026-08-01", "2025-01-31", "2023-09-20", "2023-09-10"],
            "reportDate": ["2026-04-17", "2026-06-28", "", "2023-09-18", "2023-09-08"],
            "acceptanceDateTime": ["2026-09-01T20:30:35.000Z", "2026-08-01T16:01:00.000Z",
                                   "2025-01-31T16:05:00.000Z", "2023-09-20T16:30:00.000Z",
                                   "2023-09-10T16:30:00.000Z"],
            "act": ["34", "34", "34", "34", "34"],
            "form": ["8-K/A", "10-Q", "8-K", "8-K", "8-K"],
            "fileNumber": ["001-36743", "001-36743", "001-36743", "001-36743", "001-36743"],
            "filmNumber": ["261351260", "1", "2", "3", "4"],
            "items": ["5.02", "", "2.02,9.01", "5.07", "8.01"],
            "core_type": ["XBRL", "XBRL", "XBRL", "XBRL", "XBRL"],
            "size": [241262, 100, 5000, 7000, 8000],
            "isXBRL": [1, 1, 1, 1, 1],
            "isInlineXBRL": [1, 1, 1, 1, 1],
            "isXBRLNumeric": [0, 0, 0, 0, 0],
            "primaryDocument": ["ef20081427_8ka.htm", "q.htm", "a8-k.htm", "b8-k.htm", "c8-k.htm"],
            "primaryDocDescription": ["8-K/A", "10-Q", "8-K", "", "8-K"],
        },
        "files": [{"name": "CIK0000320193-submissions-001.json", "filingCount": 1247}],
    },
}

BUFFETT = {
    "cik": "0000315090", "entityType": "other", "sic": "", "sicDescription": "",
    "name": "BUFFETT WARREN E", "tickers": [], "exchanges": [], "ein": "000000000",
    "category": "", "fiscalYearEnd": None, "stateOfIncorporation": "", "flags": "",
    "phone": "", "website": "",
    "addresses": {"mailing": {"street1": "3555 FARNAM STREET", "city": "OMAHA",
                              "stateOrCountry": "NE", "zipCode": "68131"},
                  "business": {"street1": None, "city": None, "stateOrCountry": None,
                               "zipCode": None}},
    "formerNames": [],
    "filings": {"recent": {"accessionNumber": ["0000315090-26-000001"],
                           "filingDate": ["2026-05-01"], "form": ["4"],
                           "reportDate": [""], "items": [""]}, "files": []},
}

FOREIGN = {
    "cik": "0001001807", "entityType": "operating", "name": "PT TELEKOMUNIKASI INDONESIA",
    "ein": "999999999", "tickers": ["TLK"], "exchanges": ["NYSE"],
    "phone": "62-21-521-5109",
    "addresses": {"business": {"street1": "JALAN JAPATI NO. 1", "city": "BANDUNG",
                               "stateOrCountry": "K8", "zipCode": "40133",
                               "stateOrCountryDescription": "Indonesia",
                               "isForeignLocation": 1, "country": "Indonesia",
                               "countryCode": "K8"}},
    "filings": {"recent": {}},
}

OVERFLOW = {"accessionNumber": ["0000320193-10-000001"], "filingDate": ["2010-01-01"],
            "form": ["8-K"], "items": ["1.01"]}


def _zip(tmp_path, apple=None, name="submissions.zip"):
    path = tmp_path / name
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("CIK0000320193.json", json.dumps(apple or APPLE))
        zf.writestr("CIK0000320193-submissions-001.json", json.dumps(OVERFLOW))
        zf.writestr("CIK0000315090.json", json.dumps(BUFFETT))
        zf.writestr("CIK0001001807.json", json.dumps(FOREIGN))
        zf.writestr("CIK0000000001.json", "{not json")
        zf.writestr("README.txt", "hello")
    return path


# ---------------------------------------------------------------------------
# discovery / normalizers / parser
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_discover_single_snapshot_release():
    """T1"""
    from app.ingest.bulk.sec_edgar_submissions.source import EdgarSubmissionsBulk, SUBMISSIONS_URL

    http = MagicMock()
    rels = EdgarSubmissionsBulk().discover(http, None)
    assert len(rels) == 1
    assert re.fullmatch(r"snapshot:\d{4}-\d{2}-\d{2}", rels[0].release_key)
    assert rels[0].url == SUBMISSIONS_URL == \
        "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    assert not http.method_calls


@pytest.mark.unit
def test_normalizers():
    """T2"""
    from app.ingest.bulk.sec_edgar_submissions import norm

    assert norm.clean_ein("000000000") is None
    assert norm.clean_ein("999999999") is None
    assert norm.clean_ein("26-1640968") == "261640968"
    assert norm.clean_ein("") is None
    assert norm.state2("US-PA") == "PA"
    assert norm.state2("ca") == "CA"
    assert norm.state2("K8") is None
    assert norm.zip5("95630-1234") == "95630"
    assert norm.zip5("00000") is None
    assert norm.zip5("123") is None
    assert norm.phone10("1-916-868-6960 x12") == "9168686960"
    assert norm.phone10("(408) 996-1010") == "4089961010"
    assert norm.phone10("555") is None
    assert norm.s("") is None and norm.s("  x ") == "x" and norm.s(None) is None
    assert norm.parse_date("2007-01-10T05:00:00.000Z") == date(2007, 1, 10)
    assert norm.parse_date("") is None
    assert norm.pg_text_array(["AAPL", 'a"b', "c,d"]) == '{"AAPL","a\\"b","c,d"}'
    assert norm.pg_text_array([]) is None


@pytest.mark.unit
def test_parse_filer_fields():
    """T3"""
    from app.ingest.bulk.sec_edgar_submissions import parse

    row = dict(zip(parse.FILER_COLUMNS, parse.filer_row(APPLE, SNAPSHOT)))
    assert row["cik"] == "0000320193"
    assert row["name"] == "Apple Inc."
    assert row["entity_type"] == "operating"
    assert row["sic"] == "3571" and row["owner_org"] == "06 Technology"
    assert row["category"] == "Large accelerated filer"
    assert row["ein"] == "942404110" and row["ein_raw"] == "942404110"
    assert row["tickers"] == '{"AAPL"}' and row["exchanges"] == '{"Nasdaq"}'
    assert row["website"] is None
    assert row["phone"] == "(408) 996-1010" and row["phone10"] == "4089961010"
    assert row["biz_street1"] == "ONE APPLE PARK WAY" and row["biz_street2"] == "SUITE 1"
    assert row["biz_zip"] == "95014-2083" and row["biz_zip5"] == "95014"
    assert row["biz_state2"] == "CA" and row["biz_country"] == "US" and row["biz_is_foreign"] is False
    assert row["mail_city"] == "CUPERTINO"
    assert row["insider_transaction_for_owner_exists"] is False
    assert row["insider_transaction_for_issuer_exists"] is True
    assert row["former_name_count"] == 2
    assert row["recent_filing_count"] == 5
    assert row["latest_filing_date"] == "2026-09-01" and row["latest_form"] == "8-K/A"
    assert row["source_release_key"] == SNAPSHOT

    b = dict(zip(parse.FILER_COLUMNS, parse.filer_row(BUFFETT, SNAPSHOT)))
    assert b["ein"] is None and b["ein_raw"] == "000000000"
    assert b["tickers"] is None and b["sic"] is None and b["category"] is None
    assert b["biz_country"] == "US"  # falls back to mailing block

    f = dict(zip(parse.FILER_COLUMNS, parse.filer_row(FOREIGN, SNAPSHOT)))
    assert f["ein"] is None and f["biz_country"] == "Indonesia" and f["biz_is_foreign"] is True
    assert f["biz_state2"] is None

    assert parse.filer_row({"name": "no cik"}, SNAPSHOT) is None
    assert len(parse.filer_row(APPLE, SNAPSHOT)) == len(parse.FILER_COLUMNS)


@pytest.mark.unit
def test_parse_former_names():
    """T4"""
    from app.ingest.bulk.sec_edgar_submissions import parse

    rows = [dict(zip(parse.FORMER_NAME_COLUMNS, r)) for r in parse.former_name_rows(APPLE, SNAPSHOT)]
    assert [r["name"] for r in rows] == ["APPLE INC", "APPLE COMPUTER INC"]
    assert rows[0]["cik"] == "0000320193"
    assert rows[0]["from_date"] == "2007-01-10" and rows[0]["to_date"] == "2019-08-05"
    doc = {"cik": "1", "formerNames": [{"name": "X", "from": None, "to": "2001-01-01T00:00:00Z"}]}
    (r,) = parse.former_name_rows(doc, SNAPSHOT)
    assert r[0] == "0000000001" and r[2] == parse.UNKNOWN_FROM_DATE


@pytest.mark.unit
def test_parse_8k_cutoff_and_items():
    """T5"""
    from app.ingest.bulk.sec_edgar_submissions import parse

    cutoff = parse.cutoff_date(date(2026, 9, 16))
    assert cutoff == date(2023, 9, 16)
    assert parse.cutoff_date(date(2028, 2, 29)) == date(2025, 2, 28)
    rows = [dict(zip(parse.EIGHT_K_COLUMNS, r)) for r in parse.eight_k_rows(APPLE, cutoff, SNAPSHOT)]
    assert [r["accession_number"] for r in rows] == [
        "0001140361-26-035325", "0000320193-25-000001", "0000320193-23-000070"]
    first = rows[0]
    assert first["form"] == "8-K/A" and first["items"] == "5.02"
    assert first["filing_date"] == "2026-09-01" and first["report_date"] == "2026-04-17"
    assert first["acceptance_datetime"] == "2026-09-01T20:30:35.000Z"
    assert first["primary_document"] == "ef20081427_8ka.htm" and first["size"] == 241262
    assert first["cik"] == "0000320193"
    assert rows[1]["report_date"] is None and rows[1]["items"] == "2.02,9.01"
    assert rows[2]["primary_doc_description"] is None
    assert parse.eight_k_rows(BUFFETT, cutoff, SNAPSHOT) == []
    assert parse.eight_k_rows(FOREIGN, cutoff, SNAPSHOT) == []


@pytest.mark.unit
def test_iter_members_skips_overflow(tmp_path):
    """T6"""
    from app.ingest.bulk.sec_edgar_submissions import parse

    assert parse.is_primary_member("CIK0000320193.json")
    assert parse.is_primary_member("sub/CIK0000320193.json")
    assert not parse.is_primary_member("CIK0000320193-submissions-001.json")
    stats = parse.MemberStats()
    docs = list(parse.iter_filer_docs(_zip(tmp_path), stats))
    assert sorted(d["cik"] for _, d in docs) == ["0000315090", "0000320193", "0001001807"]
    assert stats.overflow_skipped == 1
    assert stats.bad_json == 1
    assert stats.other_skipped == 1


@pytest.mark.unit
def test_registered():
    """T7"""
    from app.ingest.bulk import registry
    from app.ingest.bulk.sec_edgar_submissions.source import EdgarSubmissionsBulk

    registry.load_all()
    assert registry.BULK_SOURCES["sec_edgar_submissions"] is EdgarSubmissionsBulk
    ddl = " ".join(EdgarSubmissionsBulk().ddl()).lower()
    for t in ("public.sec_filers", "public.sec_filer_former_names", "public.sec_8k_index"):
        assert f"create table if not exists {t}" in ddl


# ---------------------------------------------------------------------------
# PG load
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for t in ("sec_filers", "sec_filer_former_names", "sec_8k_index"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
    yield engine
    engine.dispose()


@pg
def test_load_fixture_zip_twice_pg(pg_engine, tmp_path):
    """T8"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_edgar_submissions.source import EdgarSubmissionsBulk, SUBMISSIONS_URL

    src = EdgarSubmissionsBulk()
    rel = Release(SNAPSHOT, SUBMISSIONS_URL)
    path = _zip(tmp_path)
    with pg_engine.begin() as conn:
        out1 = src.load(conn, rel, path)
    assert out1 == {"sec_filers": 3, "sec_filer_former_names": 2, "sec_8k_index": 3}

    with pg_engine.connect() as conn:
        f = conn.execute(text(
            "SELECT name, ein, tickers, biz_zip5, insider_transaction_for_issuer_exists, "
            "latest_filing_date, source_release_key, loaded_at IS NOT NULL "
            "FROM sec_filers WHERE cik='0000320193'")).one()
        assert f == ("Apple Inc.", "942404110", ["AAPL"], "95014", True, date(2026, 9, 1), SNAPSHOT, True)
        k = conn.execute(text(
            "SELECT cik, form, filing_date, report_date, items, size, "
            "acceptance_datetime AT TIME ZONE 'UTC' FROM sec_8k_index "
            "WHERE accession_number='0001140361-26-035325'")).one()
        assert k[:6] == ("0000320193", "8-K/A", date(2026, 9, 1), date(2026, 4, 17), "5.02", 241262)
        assert str(k[6]) == "2026-09-01 20:30:35"
        assert conn.execute(text("SELECT count(*) FROM sec_8k_index WHERE filing_date < '2023-09-16'")).scalar() == 0
        idx = {r[0] for r in conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename='sec_8k_index'"))}
        assert "ix_sec_8k_index_cik_filing_date" in idx
        assert conn.execute(text("SELECT to_regclass('stg.sec_edgar_filers')")).scalar() is None

    # reload next day with a changed name: idempotent, nothing inserted
    apple2 = copy.deepcopy(APPLE)
    apple2["name"] = "Apple Inc. (renamed)"
    rel2 = Release("snapshot:2026-09-17", SUBMISSIONS_URL)
    with pg_engine.begin() as conn:
        out2 = src.load(conn, rel2, _zip(tmp_path, apple=apple2, name="s2.zip"))
    assert out2 == out1
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM sec_filers")).scalar() == 3
        assert conn.execute(text("SELECT count(*) FROM sec_filer_former_names")).scalar() == 2
        assert conn.execute(text("SELECT count(*) FROM sec_8k_index")).scalar() == 3
        assert conn.execute(text(
            "SELECT name, source_release_key FROM sec_filers WHERE cik='0000320193'")).one() == \
            ("Apple Inc. (renamed)", "snapshot:2026-09-17")


# ---------------------------------------------------------------------------
# D18 — SEC filing table DDL
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_d18_ddl_has_no_inline_index():
    """T9"""
    from app.sources.sec import metadata

    for ft in ("10-K", "10-Q", "8-K", "8-K/A"):
        table = metadata.generate_table_name(ft)
        create = metadata.generate_create_table_sql(table)
        assert "CREATE TABLE IF NOT EXISTS" in create
        assert not re.search(r"\bINDEX\b", create, re.IGNORECASE)
        idx = metadata.generate_create_index_sql(table)
        assert idx and all(s.strip().startswith("CREATE INDEX IF NOT EXISTS") for s in idx)
        assert all(f" ON {table} " in s for s in idx)


@pg
def test_d18_ddl_executes_on_pg():
    """T10"""
    from sqlalchemy import create_engine, text
    from app.sources.sec import metadata

    engine = create_engine(PG_URL)
    try:
        table = metadata.generate_table_name("8-K/A")
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        for _ in range(2):
            with engine.begin() as conn:
                conn.execute(text(metadata.generate_create_table_sql(table)))
                for stmt in metadata.generate_create_index_sql(table):
                    conn.execute(text(stmt))
        with engine.connect() as conn:
            idx = {r[0] for r in conn.execute(text(
                "SELECT indexname FROM pg_indexes WHERE tablename=:t"), {"t": table})}
        assert f"idx_{table}_cik" in idx and f"idx_{table}_filing_date" in idx
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
    finally:
        engine.dispose()
