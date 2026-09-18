"""
Tests for SPEC 110 — SEC Insider Transactions data sets bulk loader (sec_insider).

Fixture zip uses the real TSV headers observed in 2025q4_form345.zip.
PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import os
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

T = "\t"

# --- real headers (2025q4_form345.zip) -------------------------------------
SUBMISSION_H = ("ACCESSION_NUMBER FILING_DATE PERIOD_OF_REPORT DATE_OF_ORIG_SUB NO_SECURITIES_OWNED "
                "NOT_SUBJECT_SEC16 FORM3_HOLDINGS_REPORTED FORM4_TRANS_REPORTED DOCUMENT_TYPE ISSUERCIK "
                "ISSUERNAME ISSUERTRADINGSYMBOL REMARKS AFF10B5ONE").split()
OWNER_H = ("ACCESSION_NUMBER RPTOWNERCIK RPTOWNERNAME RPTOWNER_RELATIONSHIP RPTOWNER_TITLE RPTOWNER_TXT "
           "RPTOWNER_STREET1 RPTOWNER_STREET2 RPTOWNER_CITY RPTOWNER_STATE RPTOWNER_ZIPCODE "
           "RPTOWNER_STATE_DESC FILE_NUMBER").split()
NDT_H = ("ACCESSION_NUMBER NONDERIV_TRANS_SK SECURITY_TITLE SECURITY_TITLE_FN TRANS_DATE TRANS_DATE_FN "
         "DEEMED_EXECUTION_DATE DEEMED_EXECUTION_DATE_FN TRANS_FORM_TYPE TRANS_CODE EQUITY_SWAP_INVOLVED "
         "EQUITY_SWAP_TRANS_CD_FN TRANS_TIMELINESS TRANS_TIMELINESS_FN TRANS_SHARES TRANS_SHARES_FN "
         "TRANS_PRICEPERSHARE TRANS_PRICEPERSHARE_FN TRANS_ACQUIRED_DISP_CD TRANS_ACQUIRED_DISP_CD_FN "
         "SHRS_OWND_FOLWNG_TRANS SHRS_OWND_FOLWNG_TRANS_FN VALU_OWND_FOLWNG_TRANS VALU_OWND_FOLWNG_TRANS_FN "
         "DIRECT_INDIRECT_OWNERSHIP DIRECT_INDIRECT_OWNERSHIP_FN NATURE_OF_OWNERSHIP NATURE_OF_OWNERSHIP_FN").split()
NDH_H = ("ACCESSION_NUMBER NONDERIV_HOLDING_SK SECURITY_TITLE SECURITY_TITLE_FN TRANS_FORM_TYPE TRANS_FORM_TYPE_FN "
         "SHRS_OWND_FOLWNG_TRANS SHRS_OWND_FOLWNG_TRANS_FN VALU_OWND_FOLWNG_TRANS VALU_OWND_FOLWNG_TRANS_FN "
         "DIRECT_INDIRECT_OWNERSHIP DIRECT_INDIRECT_OWNERSHIP_FN NATURE_OF_OWNERSHIP NATURE_OF_OWNERSHIP_FN").split()
DT_H = ("ACCESSION_NUMBER DERIV_TRANS_SK SECURITY_TITLE SECURITY_TITLE_FN CONV_EXERCISE_PRICE CONV_EXERCISE_PRICE_FN "
        "TRANS_DATE TRANS_DATE_FN DEEMED_EXECUTION_DATE DEEMED_EXECUTION_DATE_FN TRANS_FORM_TYPE TRANS_CODE "
        "EQUITY_SWAP_INVOLVED EQUITY_SWAP_TRANS_CD_FN TRANS_TIMELINESS TRANS_TIMELINESS_FN TRANS_SHARES "
        "TRANS_SHARES_FN TRANS_TOTAL_VALUE TRANS_TOTAL_VALUE_FN TRANS_PRICEPERSHARE TRANS_PRICEPERSHARE_FN "
        "TRANS_ACQUIRED_DISP_CD TRANS_ACQUIRED_DISP_CD_FN EXCERCISE_DATE EXCERCISE_DATE_FN EXPIRATION_DATE "
        "EXPIRATION_DATE_FN UNDLYNG_SEC_TITLE UNDLYNG_SEC_TITLE_FN UNDLYNG_SEC_SHARES UNDLYNG_SEC_SHARES_FN "
        "UNDLYNG_SEC_VALUE UNDLYNG_SEC_VALUE_FN SHRS_OWND_FOLWNG_TRANS SHRS_OWND_FOLWNG_TRANS_FN "
        "VALU_OWND_FOLWNG_TRANS VALU_OWND_FOLWNG_TRANS_FN DIRECT_INDIRECT_OWNERSHIP DIRECT_INDIRECT_OWNERSHIP_FN "
        "NATURE_OF_OWNERSHIP NATURE_OF_OWNERSHIP_FN").split()
DH_H = ("ACCESSION_NUMBER DERIV_HOLDING_SK SECURITY_TITLE SECURITY_TITLE_FN CONV_EXERCISE_PRICE CONV_EXERCISE_PRICE_FN "
        "TRANS_FORM_TYPE TRANS_FORM_TYPE_FN EXERCISE_DATE EXERCISE_DATE_FN EXPIRATION_DATE EXPIRATION_DATE_FN "
        "UNDLYNG_SEC_TITLE UNDLYNG_SEC_TITLE_FN UNDLYNG_SEC_SHARES UNDLYNG_SEC_SHARES_FN UNDLYNG_SEC_VALUE "
        "UNDLYNG_SEC_VALUE_FN SHRS_OWND_FOLWNG_TRANS SHRS_OWND_FOLWNG_TRANS_FN VALU_OWND_FOLWNG_TRANS "
        "VALU_OWND_FOLWNG_TRANS_FN DIRECT_INDIRECT_OWNERSHIP DIRECT_INDIRECT_OWNERSHIP_FN NATURE_OF_OWNERSHIP "
        "NATURE_OF_OWNERSHIP_FN").split()
FOOT_H = ["ACCESSION_NUMBER", "FOOTNOTE_ID", "FOOTNOTE_TXT"]

A1 = "0001104659-25-125707"
A2 = "0001140361-25-047122"


def _tsv(header, rows):
    lines = [T.join(header)]
    for r in rows:
        assert len(r) == len(header), (header[1], len(r), len(header))
        lines.append(T.join(r))
    return "\n".join(lines) + "\n"


def _row(header, **vals):
    return [vals.get(h, "") for h in header]


def build_fixture_zip(path: Path, drop_nonderiv_sk=None) -> Path:
    sub = [
        _row(SUBMISSION_H, ACCESSION_NUMBER=A1, FILING_DATE="31-DEC-2025", PERIOD_OF_REPORT="29-DEC-2025",
             DOCUMENT_TYPE="4", ISSUERCIK="0001701051", ISSUERNAME="WideOpenWest, Inc.",
             ISSUERTRADINGSYMBOL="WOW", AFF10B5ONE="0", NOT_SUBJECT_SEC16="0"),
        _row(SUBMISSION_H, ACCESSION_NUMBER=A2, FILING_DATE="02-JAN-2026", PERIOD_OF_REPORT="31-DEC-2025",
             DOCUMENT_TYPE="4/A", DATE_OF_ORIG_SUB="30-DEC-2025", ISSUERCIK="0000898293", ISSUERNAME="JABIL INC",
             ISSUERTRADINGSYMBOL="JBL", AFF10B5ONE="true", REMARKS='Exhibit 24 "POA"'),
    ]
    own = [
        _row(OWNER_H, ACCESSION_NUMBER=A1, RPTOWNERCIK="0001746356", RPTOWNERNAME="Schena Don",
             RPTOWNER_RELATIONSHIP="Officer", RPTOWNER_TITLE="Chief Customer Exper. Officer",
             RPTOWNER_STREET1="C/O WIDEOPENWEST, INC.", RPTOWNER_CITY="ENGLEWOOD", RPTOWNER_STATE="CO",
             RPTOWNER_ZIPCODE="80111", FILE_NUMBER="001-38101"),
        _row(OWNER_H, ACCESSION_NUMBER=A2, RPTOWNERCIK="0001361828", RPTOWNERNAME="Rego John S",
             RPTOWNER_RELATIONSHIP="Director,TenPercentOwner,Other", RPTOWNER_TXT="Member of 13D group"),
    ]
    ndt = [
        _row(NDT_H, ACCESSION_NUMBER=A1, NONDERIV_TRANS_SK="8835098", SECURITY_TITLE="Common Stock",
             TRANS_DATE="29-DEC-2025", TRANS_FORM_TYPE="4", TRANS_CODE="A", EQUITY_SWAP_INVOLVED="0",
             EQUITY_SWAP_TRANS_CD_FN="F1", TRANS_SHARES="75974.0", TRANS_SHARES_FN="F2",
             TRANS_PRICEPERSHARE="0.0", TRANS_ACQUIRED_DISP_CD="A", SHRS_OWND_FOLWNG_TRANS="288032.0",
             DIRECT_INDIRECT_OWNERSHIP="D"),
        _row(NDT_H, ACCESSION_NUMBER=A1, NONDERIV_TRANS_SK="8835099", SECURITY_TITLE="Common Stock",
             TRANS_DATE="31-DEC-2025", TRANS_FORM_TYPE="4", TRANS_CODE="D", EQUITY_SWAP_INVOLVED="false",
             TRANS_SHARES="288032.0", TRANS_PRICEPERSHARE="", TRANS_ACQUIRED_DISP_CD="D",
             SHRS_OWND_FOLWNG_TRANS="0.0", DIRECT_INDIRECT_OWNERSHIP="I", NATURE_OF_OWNERSHIP="By Trust"),
    ]
    if drop_nonderiv_sk:
        ndt = [r for r in ndt if r[1] != drop_nonderiv_sk]
    ndh = [
        _row(NDH_H, ACCESSION_NUMBER=A1, NONDERIV_HOLDING_SK="2851523", SECURITY_TITLE="Common Stock",
             SHRS_OWND_FOLWNG_TRANS="519743904.0", SHRS_OWND_FOLWNG_TRANS_FN="F3", DIRECT_INDIRECT_OWNERSHIP="D"),
    ]
    dt = [
        _row(DT_H, ACCESSION_NUMBER=A2, DERIV_TRANS_SK="3337873", SECURITY_TITLE="Stock Option (Right to Buy)",
             CONV_EXERCISE_PRICE="12.68", TRANS_DATE="31-DEC-2025", TRANS_FORM_TYPE="4", TRANS_CODE="M",
             EQUITY_SWAP_INVOLVED="0", TRANS_SHARES="2449877.0", TRANS_PRICEPERSHARE="0.0",
             TRANS_ACQUIRED_DISP_CD="D", EXCERCISE_DATE_FN="F5", EXPIRATION_DATE="31-MAR-2026",
             UNDLYNG_SEC_TITLE="Common Shares", UNDLYNG_SEC_SHARES="2449877.0", SHRS_OWND_FOLWNG_TRANS="95718.0",
             DIRECT_INDIRECT_OWNERSHIP="D"),
    ]
    dh = [
        _row(DH_H, ACCESSION_NUMBER=A2, DERIV_HOLDING_SK="2004885", SECURITY_TITLE="Restricted Stock Units",
             CONV_EXERCISE_PRICE="0.0", EXERCISE_DATE="15-MAR-2026", EXPIRATION_DATE_FN="F1",
             UNDLYNG_SEC_TITLE="Common Stock", UNDLYNG_SEC_SHARES="14706.0", SHRS_OWND_FOLWNG_TRANS="14706.0",
             DIRECT_INDIRECT_OWNERSHIP="D"),
    ]
    foot = [
        [A1, "F1", '"In connection with the ""Merger Agreement""\tand more."'],
        [A1, "F2", "Plain footnote."],
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("SUBMISSION.tsv", _tsv(SUBMISSION_H, sub))
        zf.writestr("REPORTINGOWNER.tsv", _tsv(OWNER_H, own))
        zf.writestr("NONDERIV_TRANS.tsv", _tsv(NDT_H, ndt))
        zf.writestr("NONDERIV_HOLDING.tsv", _tsv(NDH_H, ndh))
        zf.writestr("DERIV_TRANS.tsv", _tsv(DT_H, dt))
        zf.writestr("DERIV_HOLDING.tsv", _tsv(DH_H, dh))
        zf.writestr("FOOTNOTES.tsv", "\n".join([T.join(FOOT_H)] + [T.join(r) for r in foot]) + "\n")
        zf.writestr("OWNER_SIGNATURE.tsv", "ACCESSION_NUMBER\tOWNERSIGNATURENAME\tOWNERSIGNATUREDATE\n")
    return path


@pytest.fixture
def fixture_zip(tmp_path):
    return build_fixture_zip(tmp_path / "2025q4_form345.zip")


INDEX_HTML = """
<html><body>
<a href="/files/datastandardsinnovation/data/insider-transactions-data-sets/2026q2_form345.zip" download>2026 Q2</a>
""" + "\n".join(
    f'<a href="/files/structureddata/data/insider-transactions-data-sets/{y}q{q}_form345.zip" download>{y} Q{q}</a>'
    for y in range(2026, 2021, -1) for q in (4, 3, 2, 1) if (y, q) < (2026, 2)
) + """
<a href="https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/2025q4_form345.zip">dup</a>
<a href="/files/insider_transactions_readme.pdf">readme</a>
</body></html>
"""


def _http_with_index():
    http = MagicMock()
    http.get_text.return_value = INDEX_HTML
    return http


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_discover_parses_index_default_window():
    """T1"""
    from app.ingest.bulk.sec_insider.source import INDEX_URL, SecInsiderDataSets

    http = _http_with_index()
    rels = SecInsiderDataSets().discover(http, None)
    http.get_text.assert_called_once_with(INDEX_URL)
    keys = [r.release_key for r in rels]
    assert len(keys) == 8
    assert keys[0] == "2024q3" and keys[-1] == "2026q2"
    assert keys == sorted(set(keys))
    by_key = {r.release_key: r.url for r in rels}
    assert by_key["2026q2"] == ("https://www.sec.gov/files/datastandardsinnovation/data/"
                                "insider-transactions-data-sets/2026q2_form345.zip")
    assert by_key["2025q4"].startswith("https://www.sec.gov/files/structureddata/")


@pytest.mark.unit
def test_discover_since_filters_by_quarter_end():
    """T2"""
    from app.ingest.bulk.sec_insider.source import SecInsiderDataSets

    rels = SecInsiderDataSets().discover(_http_with_index(), date(2025, 5, 1))
    assert [r.release_key for r in rels] == ["2025q2", "2025q3", "2025q4", "2026q1", "2026q2"]
    rels = SecInsiderDataSets().discover(_http_with_index(), date(2022, 1, 1))
    assert len(rels) == 18 and rels[0].release_key == "2022q1"


# ---------------------------------------------------------------------------
# value parsing
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_parse_sec_date():
    """T3"""
    from app.ingest.bulk.sec_insider.parse import parse_sec_date

    assert parse_sec_date("01-JAN-2024") == date(2024, 1, 1)
    assert parse_sec_date("31-dec-2025") == date(2025, 12, 31)
    assert parse_sec_date(" 2024-03-05 ") == date(2024, 3, 5)
    assert parse_sec_date("") is None
    assert parse_sec_date(None) is None
    assert parse_sec_date("garbage") is None
    assert parse_sec_date("31-FEB-2024") is None


@pytest.mark.unit
def test_parse_number_and_flag():
    """T4"""
    from app.ingest.bulk.sec_insider.parse import parse_flag, parse_number

    assert parse_number("14706.0") == Decimal("14706.0")
    assert parse_number("-1.5") == Decimal("-1.5")
    assert parse_number("1,234.5") == Decimal("1234.5")
    for bad in ("", "  ", None, "abc", "NaN", "Infinity"):
        assert parse_number(bad) is None
    assert parse_flag("1") is True and parse_flag("true") is True and parse_flag("Y") is True
    assert parse_flag("0") is False and parse_flag("FALSE") is False
    assert parse_flag("") is None and parse_flag("maybe") is None


@pytest.mark.unit
def test_parse_relationship():
    """T5"""
    from app.ingest.bulk.sec_insider.parse import parse_relationship

    assert parse_relationship("Director,Officer,TenPercentOwner") == (True, True, True, False)
    assert parse_relationship("Other") == (False, False, False, True)
    assert parse_relationship("") == (None, None, None, None)


# ---------------------------------------------------------------------------
# zip iterators
# ---------------------------------------------------------------------------

def _dicts(columns, rows):
    names = [c for c, _ in columns]
    return [dict(zip(names, r)) for r in rows]


@pytest.mark.unit
def test_iter_filings_and_owners_fixture_zip(fixture_zip):
    """T6"""
    from app.ingest.bulk.sec_insider import parse as p

    with zipfile.ZipFile(fixture_zip) as zf:
        filings = _dicts(p.FILING_COLUMNS, list(p.iter_filings(zf, "2025q4")))
        owners = _dicts(p.OWNER_COLUMNS, list(p.iter_owners(zf, "2025q4")))
    assert len(filings) == 2
    f1, f2 = filings
    assert f1["accession_number"] == A1 and f1["filing_date"] == date(2025, 12, 31)
    assert f1["issuer_cik"] == "0001701051" and f1["issuer_trading_symbol"] == "WOW"
    assert f1["aff10b5one"] is False and f1["remarks"] is None and f1["date_of_orig_sub"] is None
    assert f2["document_type"] == "4/A" and f2["aff10b5one"] is True
    assert f2["date_of_orig_sub"] == date(2025, 12, 30) and f2["remarks"] == 'Exhibit 24 "POA"'
    assert f2["source_release_key"] == "2025q4"
    o1, o2 = owners
    assert (o1["is_director"], o1["is_officer"], o1["is_ten_percent_owner"], o1["is_other"]) == (False, True, False, False)
    assert o1["officer_title"] == "Chief Customer Exper. Officer" and o1["rptowner_state"] == "CO"
    assert (o2["is_director"], o2["is_ten_percent_owner"], o2["is_other"]) == (True, True, True)
    assert o2["other_text"] == "Member of 13D group" and o2["rptowner_street1"] is None


@pytest.mark.unit
def test_iter_transactions_covers_all_four_tables(fixture_zip):
    """T7"""
    import json
    from app.ingest.bulk.sec_insider import parse as p

    with zipfile.ZipFile(fixture_zip) as zf:
        rows = _dicts(p.TRANSACTION_COLUMNS, list(p.iter_transactions(zf, "2025q4")))
    by = {(r["table_type"], r["trans_sk"]): r for r in rows}
    assert set(by) == {("nonderiv_trans", 8835098), ("nonderiv_trans", 8835099), ("nonderiv_holding", 2851523),
                       ("deriv_trans", 3337873), ("deriv_holding", 2004885)}
    nd = by[("nonderiv_trans", 8835098)]
    assert nd["trans_date"] == date(2025, 12, 29) and nd["trans_code"] == "A"
    assert nd["trans_shares"] == Decimal("75974.0") and nd["equity_swap_involved"] is False
    assert nd["conv_exercise_price"] is None and nd["expiration_date"] is None
    assert json.loads(nd["footnote_refs"]) == {"equity_swap_involved": "F1", "trans_shares": "F2"}
    nd2 = by[("nonderiv_trans", 8835099)]
    assert nd2["trans_pricepershare"] is None and nd2["nature_of_ownership"] == "By Trust"
    assert nd2["footnote_refs"] is None
    dtr = by[("deriv_trans", 3337873)]
    assert dtr["conv_exercise_price"] == Decimal("12.68") and dtr["expiration_date"] == date(2026, 3, 31)
    assert dtr["exercise_date"] is None and json.loads(dtr["footnote_refs"]) == {"exercise_date": "F5"}
    assert dtr["undlyng_sec_shares"] == Decimal("2449877.0")
    dh = by[("deriv_holding", 2004885)]
    assert dh["exercise_date"] == date(2026, 3, 15) and dh["trans_date"] is None
    ndh = by[("nonderiv_holding", 2851523)]
    assert ndh["shrs_ownd_folwng_trans"] == Decimal("519743904.0") and ndh["trans_code"] is None


@pytest.mark.unit
def test_iter_footnotes_quoted_text(fixture_zip):
    """T8"""
    from app.ingest.bulk.sec_insider import parse as p

    with zipfile.ZipFile(fixture_zip) as zf:
        rows = _dicts(p.FOOTNOTE_COLUMNS, list(p.iter_footnotes(zf, "2025q4")))
    assert rows[0]["footnote_txt"] == 'In connection with the "Merger Agreement"\tand more.'
    assert rows[1]["footnote_id"] == "F2"


@pytest.mark.unit
def test_ddl_and_registration():
    """T9"""
    from app.ingest.bulk import registry
    from app.ingest.bulk.sec_insider.source import SecInsiderDataSets

    assert isinstance(registry.get_source("sec_insider"), SecInsiderDataSets)
    ddl = " ".join(" ".join(SecInsiderDataSets().ddl()).split()).lower()
    for t in ("sec_insider_filings", "sec_insider_owners", "sec_insider_transactions", "sec_insider_footnotes"):
        assert f"create table if not exists public.{t}" in ddl
    assert "primary key (accession_number, table_type, trans_sk)" in ddl
    assert "primary key (accession_number, rptowner_cik)" in ddl
    for col in ("issuer_cik", "rptowner_cik", "trans_date"):
        assert f"({col})" in ddl
    assert "insider_transactions " not in ddl.replace("sec_insider_transactions", "")


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------

TABLES = ("sec_insider_filings", "sec_insider_owners", "sec_insider_transactions", "sec_insider_footnotes")


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for t in TABLES:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
    yield engine
    engine.dispose()


def _counts(engine):
    from sqlalchemy import text

    with engine.connect() as conn:
        return {t: conn.execute(text(f"SELECT COUNT(*) FROM public.{t}")).scalar() for t in TABLES}


@pg
def test_load_fixture_twice_idempotent_pg(pg_engine, fixture_zip, monkeypatch):
    """T10"""
    monkeypatch.setenv("BULK_INSIDER_LOAD_FOOTNOTES", "1")
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_insider.source import SecInsiderDataSets

    src = SecInsiderDataSets()
    rel = Release("2025q4", "https://www.sec.gov/x/2025q4_form345.zip")
    with pg_engine.begin() as conn:
        out = src.load(conn, rel, fixture_zip)
    expected = {"sec_insider_filings": 2, "sec_insider_owners": 2, "sec_insider_transactions": 5,
                "sec_insider_footnotes": 2}
    assert out == expected
    assert _counts(pg_engine) == expected
    with pg_engine.begin() as conn:
        out2 = src.load(conn, rel, fixture_zip)
    assert all(v == 0 for v in out2.values())  # unchanged rows are not rewritten (SPEC_115)
    assert _counts(pg_engine) == expected
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT trans_date, trans_shares, equity_swap_involved, footnote_refs->>'trans_shares', "
            "source_release_key, loaded_at IS NOT NULL FROM public.sec_insider_transactions "
            "WHERE accession_number = :a AND table_type = 'nonderiv_trans' AND trans_sk = 8835098"), {"a": A1}).one()
        assert row == (date(2025, 12, 29), Decimal("75974.0"), False, "F2", "2025q4", True)
        assert conn.execute(text(
            "SELECT expiration_date FROM public.sec_insider_transactions WHERE table_type='deriv_trans'")).scalar() \
            == date(2026, 3, 31)
        assert conn.execute(text(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='stg' "
            "AND table_name LIKE 'sec_insider_%'")).scalar() == 0


@pg
def test_republish_removes_stale_children_pg(pg_engine, fixture_zip, tmp_path):
    """T11"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_insider.source import SecInsiderDataSets

    src = SecInsiderDataSets()
    rel = Release("2025q4", "https://www.sec.gov/x/2025q4_form345.zip")
    with pg_engine.begin() as conn:
        src.load(conn, rel, fixture_zip)
    # another accession from a different quarter must survive
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO public.sec_insider_transactions (accession_number, table_type, trans_sk, source_release_key) "
            "VALUES ('0000000000-24-000001', 'nonderiv_trans', 1, '2024q1')"))
    (tmp_path / "republished").mkdir()
    republished = build_fixture_zip(tmp_path / "republished" / "2025q4_form345.zip", drop_nonderiv_sk="8835099")
    with pg_engine.begin() as conn:
        out = src.load(conn, rel, republished)
    # the 4 surviving rows are unchanged, so nothing is rewritten (SPEC_115);
    # the stale row is still deleted below
    assert out["sec_insider_transactions"] == 0
    with pg_engine.connect() as conn:
        sks = {r[0] for r in conn.execute(text("SELECT trans_sk FROM public.sec_insider_transactions"))}
    assert 8835099 not in sks and 8835098 in sks and 1 in sks


@pg
def test_footnotes_skipped_by_default_pg(pg_engine, fixture_zip, monkeypatch):
    """Lean budget: footnotes are opt-in via BULK_INSIDER_LOAD_FOOTNOTES=1."""
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_insider.source import SecInsiderDataSets

    monkeypatch.delenv("BULK_INSIDER_LOAD_FOOTNOTES", raising=False)
    rel = Release("2025q4", "https://www.sec.gov/x/2025q4_form345.zip")
    with pg_engine.begin() as conn:
        out = SecInsiderDataSets().load(conn, rel, fixture_zip)
    assert "sec_insider_footnotes" not in out
    assert out["sec_insider_transactions"] == 5
