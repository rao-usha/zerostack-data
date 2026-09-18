"""
Tests for SPEC 109 — SEC Form 13F data sets bulk loader (sec_13f).

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import csv
import io
import os
import zipfile
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

# Real headers, copied from 01jun2026-31aug2026_form13f.zip
H_SUBMISSION = ["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"]
H_COVERPAGE = [
    "ACCESSION_NUMBER", "REPORTCALENDARORQUARTER", "ISAMENDMENT", "AMENDMENTNO", "AMENDMENTTYPE",
    "CONFDENIEDEXPIRED", "DATEDENIEDEXPIRED", "DATEREPORTED", "REASONFORNONCONFIDENTIALITY",
    "FILINGMANAGER_NAME", "FILINGMANAGER_STREET1", "FILINGMANAGER_STREET2", "FILINGMANAGER_CITY",
    "FILINGMANAGER_STATEORCOUNTRY", "FILINGMANAGER_ZIPCODE", "REPORTTYPE", "FORM13FFILENUMBER",
    "CRDNUMBER", "SECFILENUMBER", "PROVIDEINFOFORINSTRUCTION5", "ADDITIONALINFORMATION",
]
H_OTHERMANAGER = ["ACCESSION_NUMBER", "OTHERMANAGER_SK", "CIK", "FORM13FFILENUMBER", "CRDNUMBER",
                  "SECFILENUMBER", "NAME"]
H_OTHERMANAGER2 = ["ACCESSION_NUMBER", "SEQUENCENUMBER", "CIK", "FORM13FFILENUMBER", "CRDNUMBER",
                   "SECFILENUMBER", "NAME"]
H_SIGNATURE = ["ACCESSION_NUMBER", "NAME", "TITLE", "PHONE", "SIGNATURE", "CITY", "STATEORCOUNTRY",
               "SIGNATUREDATE"]
H_SUMMARYPAGE = ["ACCESSION_NUMBER", "OTHERINCLUDEDMANAGERSCOUNT", "TABLEENTRYTOTAL", "TABLEVALUETOTAL",
                 "ISCONFIDENTIALOMITTED"]
H_INFOTABLE = [
    "ACCESSION_NUMBER", "INFOTABLE_SK", "NAMEOFISSUER", "TITLEOFCLASS", "CUSIP", "FIGI", "VALUE",
    "SSHPRNAMT", "SSHPRNAMTTYPE", "PUTCALL", "INVESTMENTDISCRETION", "OTHERMANAGER",
    "VOTING_AUTH_SOLE", "VOTING_AUTH_SHARED", "VOTING_AUTH_NONE",
]

A1 = "0002134841-26-000139"   # 2026 filing, dollars
A2 = "0001104659-22-000001"   # filed 2022 -> VALUE in thousands
A3 = "0001721242-26-000005"   # 13F-NT, no coverpage/summary extras


def _tsv(header, rows):
    return "\t".join(header) + "\n" + "".join("\t".join(r) + "\n" for r in rows)


def _members(acc_prefix=""):
    a1, a2, a3 = acc_prefix + A1, acc_prefix + A2, acc_prefix + A3
    return {
        "SUBMISSION.tsv": _tsv(H_SUBMISSION, [
            [a1, "31-JUL-2026", "13F-HR", "0002134841", "30-JUN-2026"],
            [a2, "14-FEB-2022", "13F-HR/A", "0000102909", "31-DEC-2021"],
            [a3, "01-AUG-2026", "13F-NT", "0001721242", "30-JUN-2026"],
        ]),
        "COVERPAGE.tsv": _tsv(H_COVERPAGE, [
            [a1, "30-JUN-2026", "", "", "", "", "", "", "", "First Nebraska Trust Co",
             "1010 LINCOLN MALL STE 103", "", "LINCOLN", "NE", "68508", "13F HOLDINGS REPORT",
             "028-27006", "", "", "N", '"Quoted ""info"" here"'],
            [a2, "31-DEC-2021", "Y", "1", "RESTATEMENT", "N", "", "", "", "Old Mgr LLC",
             "1 Main St", "Suite 2", "BOSTON", "MA", "02110", "13F HOLDINGS REPORT",
             "028-00001", "000123456", "801-1", "N", ""],
        ]),
        "SUMMARYPAGE.tsv": _tsv(H_SUMMARYPAGE, [
            [a1, "0", "2", "388237401526", "N"],
            [a2, "1", "1", "500", ""],
        ]),
        "SIGNATURE.tsv": _tsv(H_SIGNATURE, [
            [a1, "Scott A. Wendt", "Vice President", "4024772200", "Scott A. Wendt", "Lincoln", "NE",
             "31-AUG-2026"],
        ]),
        "OTHERMANAGER.tsv": _tsv(H_OTHERMANAGER, [
            [a2, "222761", "0000872259", "028-02825", "", "", '"Newton (""NIMNA"")"'],
        ]),
        "OTHERMANAGER2.tsv": _tsv(H_OTHERMANAGER2, [
            [a2, "1", "", "028-18621", "", "", "METLIFE INC"],
        ]),
        "INFOTABLE.tsv": _tsv(H_INFOTABLE, [
            [a1, "133648926", "CARDINAL HEALTH INC", "COM", "14149Y108", "", "388237", "5250", "SH", "",
             "SOLE", "", "5250", "0", "0"],
            [a1, "133648927", "CISCOSYSINC", "COM", "17275R102", "BBG000C3J3C9", "401526", "32966", "SH",
             "Call", "DFND", "1,2", "32966", "0", "0"],
            [a2, "5", "APPLE INC", "COM", "037833100", "", "500", "10", "SH", "", "SOLE", "", "", "", "x"],
        ]),
    }


def _make_zip(path, acc_prefix="", drop=()):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, body in _members(acc_prefix).items():
            if name not in drop:
                zf.writestr(name, body)
    return path


INDEX_HTML = """
<a href="/files/form_13f_readme.pdf">readme</a>
<a href="/files/datastandardsinnovation/data/form-13f-data-sets/01jun2026-31aug2026_form13f.zip" download>2026 Jun</a>
<a href="/files/structureddata/data/form-13f-data-sets/01mar2026-31may2026_form13f.zip" download>x</a>
<a href="/files/structureddata/data/form-13f-data-sets/01dec2025-28feb2026_form13f.zip" download>x</a>
<a href="/files/structureddata/data/form-13f-data-sets/01dec2025-28feb2026_form13f_1.zip" download>reup</a>
<a href="/files/structureddata/data/form-13f-data-sets/01jan2024-29feb2024_form13f.zip" download>x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2023q4_form13f.zip" download>x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2023q3_form13f.zip" download>x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2023q2_form13f.zip" download>x</a>
<a href="/files/structureddata/data/form-13f-data-sets/2013q2_form13f.zip" download>x</a>
<a href="/files/ocoo01-excess-pers-prop-guidance.pdf">other</a>
"""


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_parse_index_hrefs_both_schemes():
    """T1"""
    from app.ingest.bulk.sec_13f.parse import parse_index

    ds = parse_index(INDEX_HTML)
    keys = [d.release_key for d in ds]
    assert keys == [
        "2013q2_form13f", "2023q2_form13f", "2023q3_form13f", "2023q4_form13f",
        "01jan2024-29feb2024_form13f", "01dec2025-28feb2026_form13f_1",
        "01mar2026-31may2026_form13f", "01jun2026-31aug2026_form13f",
    ]
    newest = ds[-1]
    assert newest.url == ("https://www.sec.gov/files/datastandardsinnovation/data/form-13f-data-sets/"
                          "01jun2026-31aug2026_form13f.zip")
    assert (newest.start_date, newest.end_date) == (date(2026, 6, 1), date(2026, 8, 31))
    q = {d.release_key: d for d in ds}["2023q4_form13f"]
    assert (q.start_date, q.end_date) == (date(2023, 10, 1), date(2023, 12, 31))
    leap = {d.release_key: d for d in ds}["01jan2024-29feb2024_form13f"]
    assert leap.end_date == date(2024, 2, 29)


@pytest.mark.unit
def test_select_releases_window_and_holdings_flag(monkeypatch):
    """T2"""
    from app.ingest.bulk.sec_13f.parse import parse_index
    from app.ingest.bulk.sec_13f.source import select_releases

    monkeypatch.delenv("BULK_13F_HOLDINGS_RELEASES", raising=False)
    ds = parse_index(INDEX_HTML)
    rels = select_releases(ds, since=None, today=date(2026, 9, 16))
    keys = [r.release_key for r in rels]
    # 3-year window: end_date >= 2023-09-16 -> 2023q3 (ends 2023-09-30) in, 2023q2 out
    assert keys[0] == "2023q3_form13f"
    assert "2023q2_form13f" not in keys and "2013q2_form13f" not in keys
    assert keys == sorted(keys, key=lambda k: {r.release_key: r for r in rels}[k].meta["end_date"])
    flagged = [r.release_key for r in rels if r.meta["load_holdings"]]
    assert flagged == ["01jun2026-31aug2026_form13f"]
    assert set(rels[0].meta["holdings_keep_keys"]) == set(flagged)

    rels = select_releases(ds, since=date(2026, 1, 1), today=date(2026, 9, 16))
    assert [r.release_key for r in rels] == [
        "01dec2025-28feb2026_form13f_1", "01mar2026-31may2026_form13f", "01jun2026-31aug2026_form13f"]

    monkeypatch.setenv("BULK_13F_HOLDINGS_RELEASES", "1")
    rels = select_releases(ds, since=None, today=date(2026, 9, 16))
    assert [r.release_key for r in rels if r.meta["load_holdings"]] == ["01jun2026-31aug2026_form13f"]

    # holdings go to the newest N of ALL listed; a wide since never flags old quarters
    rels = select_releases(ds, since=date(2010, 1, 1), today=date(2026, 9, 16), holdings_n=2)
    assert rels[0].release_key == "2013q2_form13f" and not rels[0].meta["load_holdings"]


@pytest.mark.unit
def test_discover_uses_http_and_raises_on_empty():
    """T3"""
    from app.ingest.bulk.sec_13f.source import INDEX_URL, Sec13FDataSets

    http = MagicMock()
    http.get_text.return_value = INDEX_HTML
    rels = Sec13FDataSets().discover(http, since=date(2026, 3, 1))
    http.get_text.assert_called_once_with(INDEX_URL)
    assert [r.release_key for r in rels] == ["01mar2026-31may2026_form13f", "01jun2026-31aug2026_form13f"]

    http.get_text.return_value = "<html>redesigned page</html>"
    with pytest.raises(RuntimeError):
        Sec13FDataSets().discover(http)


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_value_helpers():
    """T4"""
    from app.ingest.bulk.sec_13f import parse as p

    assert p.parse_date("30-JUN-2026") == date(2026, 6, 30)
    assert p.parse_date("2026-06-30") == date(2026, 6, 30)
    assert p.parse_date("") is None and p.parse_date(None) is None and p.parse_date("garbage") is None
    assert p.parse_int("32966") == 32966 and p.parse_int("") is None and p.parse_int("x") is None
    assert p.parse_int("12.0") == 12
    assert p.parse_decimal("388237401526") == Decimal("388237401526")
    assert p.parse_decimal("1.5") == Decimal("1.5") and p.parse_decimal("n/a") is None
    assert p.parse_decimal("NaN") is None
    assert p.parse_bool("Y") is True and p.parse_bool("n") is False and p.parse_bool("") is None
    assert p.unquote('"Newton (""NIMNA"")"') == 'Newton ("NIMNA")'
    assert p.unquote('plain "x') == 'plain "x'
    assert p.unquote('"') == '"'
    assert p.value_multiplier(date(2022, 12, 30)) == 1000
    assert p.value_multiplier(date(2023, 1, 3)) == 1
    assert p.value_multiplier(None, fallback=date(2025, 1, 1)) == 1


@pytest.mark.unit
def test_iter_tsv_real_headers_and_limits(tmp_path):
    """T5"""
    from app.ingest.bulk.sec_13f.parse import iter_tsv

    big = "z" * 300_000  # > csv default 131072 field limit
    path = tmp_path / "t.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("sub/Coverpage.TSV", "﻿" + _tsv(H_COVERPAGE, [
            ["A", "30-JUN-2026", "", "", "", "", "", "", "", '"Mgr ""Q"" LLC"', "st", "", "c", "NE", "1",
             "13F NOTICE", "028-1", "", "", "N", big]]))
    with zipfile.ZipFile(path) as zf:
        rows = list(iter_tsv(zf, "COVERPAGE.tsv"))
        assert list(iter_tsv(zf, "SIGNATURE.tsv")) == []
    assert len(rows) == 1
    r = rows[0]
    assert set(r) == set(H_COVERPAGE)
    assert r["ACCESSION_NUMBER"] == "A"
    assert r["ISAMENDMENT"] is None
    assert r["FILINGMANAGER_NAME"] == 'Mgr "Q" LLC'
    assert r["ADDITIONALINFORMATION"] == big
    assert csv.field_size_limit() >= 131072  # restored/unchanged sanity


@pytest.mark.unit
def test_iter_filings_joins_members(tmp_path):
    """T6"""
    from app.ingest.bulk.sec_13f.parse import FILING_COLUMNS, iter_filings

    with zipfile.ZipFile(_make_zip(tmp_path / "a.zip")) as zf:
        rows = {r["accession_number"]: r for r in iter_filings(zf)}
    assert len(rows) == 3
    for r in rows.values():
        assert set(r) == set(FILING_COLUMNS)
    f1 = rows[A1]
    assert f1["cik"] == "0002134841" and f1["submission_type"] == "13F-HR"
    assert f1["filing_date"] == date(2026, 7, 31) and f1["period_of_report"] == date(2026, 6, 30)
    assert f1["filing_manager_name"] == "First Nebraska Trust Co"
    assert f1["additional_information"] == 'Quoted "info" here'
    assert f1["table_value_total"] == Decimal("388237401526") and f1["value_multiplier"] == 1
    assert f1["is_confidential_omitted"] is False
    assert f1["signature_name"] == "Scott A. Wendt" and f1["signature_date"] == date(2026, 8, 31)
    f2 = rows[A2]
    assert f2["is_amendment"] is True and f2["amendment_no"] == 1
    assert f2["value_multiplier"] == 1000 and f2["table_value_total"] == Decimal("500000")
    assert f2["signature_name"] is None
    f3 = rows[A3]
    assert f3["filing_manager_name"] is None and f3["table_entry_total"] is None

    with zipfile.ZipFile(_make_zip(tmp_path / "b.zip", drop=("SIGNATURE.tsv", "SUMMARYPAGE.tsv"))) as zf:
        assert len(list(iter_filings(zf))) == 3
    with zipfile.ZipFile(_make_zip(tmp_path / "c.zip", drop=("SUBMISSION.tsv",))) as zf:
        with pytest.raises(RuntimeError):
            list(iter_filings(zf))


@pytest.mark.unit
def test_iter_holdings_and_other_managers(tmp_path):
    """T7"""
    from app.ingest.bulk.sec_13f.parse import (HOLDING_COLUMNS, OTHER_MANAGER_COLUMNS,
                                                filing_dates, iter_holdings, iter_other_managers)

    with zipfile.ZipFile(_make_zip(tmp_path / "a.zip")) as zf:
        dates = filing_dates(zf)
        assert dates[A2] == date(2022, 2, 14)
        hold = list(iter_holdings(zf, dates, fallback_date=date(2026, 8, 31)))
        oms = list(iter_other_managers(zf))
    assert len(hold) == 3
    assert all(set(h) == set(HOLDING_COLUMNS) for h in hold)
    h = {(x["accession_number"], x["infotable_sk"]): x for x in hold}
    cisco = h[(A1, 133648927)]
    assert cisco["value"] == Decimal("401526") and cisco["figi"] == "BBG000C3J3C9"
    assert cisco["put_call"] == "Call" and cisco["other_manager"] == "1,2"
    assert cisco["ssh_prnamt"] == 32966 and cisco["voting_auth_shared"] == 0
    assert h[(A1, 133648926)]["figi"] is None
    apple = h[(A2, 5)]
    assert apple["value"] == Decimal("500000")  # thousands -> dollars
    assert apple["voting_auth_sole"] is None and apple["voting_auth_none"] is None

    assert all(set(o) == set(OTHER_MANAGER_COLUMNS) for o in oms)
    by = {(o["list_type"], o["sequence_number"]): o for o in oms}
    assert by[("cover", 222761)]["name"] == 'Newton ("NIMNA")'
    assert by[("summary", 1)]["cik"] is None and by[("summary", 1)]["name"] == "METLIFE INC"


@pytest.mark.unit
def test_registry_has_sec_13f():
    """T8"""
    from app.ingest.bulk.registry import get_source
    from app.ingest.bulk.sec_13f.source import Sec13FDataSets

    src = get_source("sec_13f")
    assert isinstance(src, Sec13FDataSets)
    joined = " ".join(src.ddl()).lower()
    for t in ("public.sec_13f_filings", "public.sec_13f_holdings", "public.sec_13f_other_managers"):
        assert f"create table if not exists {t}" in joined


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for t in ("sec_13f_filings", "sec_13f_holdings", "sec_13f_other_managers"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
    yield engine
    engine.dispose()


def _count(conn, table):
    from sqlalchemy import text

    return conn.execute(text(f"SELECT COUNT(*) FROM public.{table}")).scalar()


@pg
def test_load_idempotent_with_and_without_holdings_pg(pg_engine, tmp_path, monkeypatch):
    """T9"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_13f.source import Sec13FDataSets

    monkeypatch.delenv("BULK_13F_PRUNE_HOLDINGS", raising=False)
    path = _make_zip(tmp_path / "01jun2026-31aug2026_form13f.zip")
    src = Sec13FDataSets()
    key = "01jun2026-31aug2026_form13f"
    meta = {"start_date": date(2026, 6, 1), "end_date": date(2026, 8, 31), "holdings_keep_keys": [key]}

    rel = Release(key, "https://www.sec.gov/x.zip", dict(meta, load_holdings=False))
    with pg_engine.begin() as conn:
        out = src.load(conn, rel, path)
    assert out == {"sec_13f_filings": 3, "sec_13f_other_managers": 2}
    with pg_engine.connect() as conn:
        assert _count(conn, "sec_13f_holdings") == 0

    rel = Release(key, "https://www.sec.gov/x.zip", dict(meta, load_holdings=True))
    with pg_engine.begin() as conn:
        out = src.load(conn, rel, path)
    # filings/managers were already loaded above and are unchanged (SPEC_115)
    assert out == {"sec_13f_filings": 0, "sec_13f_other_managers": 0, "sec_13f_holdings": 3}
    # reload: same data, so nothing is rewritten (SPEC_115)
    with pg_engine.begin() as conn:
        out = src.load(conn, rel, path)
    assert out == {"sec_13f_filings": 0, "sec_13f_other_managers": 0, "sec_13f_holdings": 0}
    with pg_engine.connect() as conn:
        assert _count(conn, "sec_13f_filings") == 3
        assert _count(conn, "sec_13f_holdings") == 3
        assert _count(conn, "sec_13f_other_managers") == 2
        row = conn.execute(text(
            "SELECT value, figi, source_release_key, loaded_at IS NOT NULL FROM public.sec_13f_holdings "
            "WHERE accession_number = :a AND infotable_sk = 5"), {"a": A2}).one()
        assert row == (Decimal("500000"), None, key, True)
        f = conn.execute(text(
            "SELECT filing_date, is_amendment, table_value_total, value_multiplier, additional_information "
            "FROM public.sec_13f_filings WHERE accession_number = :a"), {"a": A1}).one()
        assert f == (date(2026, 7, 31), None, Decimal("388237401526"), 1, 'Quoted "info" here')
        assert conn.execute(text("SELECT to_regclass('stg.sec_13f_holdings')")).scalar() is None


@pg
def test_prune_old_holdings_pg(pg_engine, tmp_path, monkeypatch):
    """T10"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_13f.source import Sec13FDataSets

    monkeypatch.delenv("BULK_13F_PRUNE_HOLDINGS", raising=False)
    src = Sec13FDataSets()
    old = _make_zip(tmp_path / "old.zip", acc_prefix="9")
    new = _make_zip(tmp_path / "new.zip")
    with pg_engine.begin() as conn:
        src.load(conn, Release("old", "u", {"load_holdings": True, "holdings_keep_keys": ["old"],
                                            "end_date": date(2026, 5, 31)}), old)
    with pg_engine.begin() as conn:
        src.load(conn, Release("new", "u", {"load_holdings": True, "holdings_keep_keys": ["new"],
                                            "end_date": date(2026, 8, 31)}), new)
    with pg_engine.connect() as conn:
        keys = conn.execute(text(
            "SELECT source_release_key, COUNT(*) FROM public.sec_13f_holdings GROUP BY 1")).fetchall()
        assert keys == [("new", 3)]
        assert _count(conn, "sec_13f_filings") == 6  # filings are never pruned

    monkeypatch.setenv("BULK_13F_PRUNE_HOLDINGS", "0")
    with pg_engine.begin() as conn:
        src.load(conn, Release("old", "u", {"load_holdings": True, "holdings_keep_keys": ["old"],
                                            "end_date": date(2026, 5, 31)}), old)
    with pg_engine.connect() as conn:
        assert _count(conn, "sec_13f_holdings") == 6
