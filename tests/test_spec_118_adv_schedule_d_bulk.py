"""
Tests for SPEC 118 — Form ADV Schedule D 7.B.(1) bulk loader (T1-T14).

Fixtures mirror the real ``ADV_Filing_Data_20260801_20260831.zip`` measured
2026-09-20: the verbatim 38-column 7B1 header, 10-digit ``805-`` fund ids, the
IA/ERA base members (ERA's is ``ADV_Base``, not ``ADV_Base_A``), and the two
``DateSubmitted`` shapes. Synthetic zips only — the real file is never read here.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import csv
import importlib.util
import io
import json
import os
import zipfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

FOIA = "https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/"


# ---------------------------------------------------------------------------
# fixtures: manifest + zip members
# ---------------------------------------------------------------------------

def _mfile(period: str, uploaded: str, year: str = None):
    return {"fileName": f"ADV_Filing_Data_{period}.zip", "size": "7.2 MB",
            "year": year or period[:4], "fileType": "ADV Filing Data", "uploadedOn": uploaded}


MANIFEST = json.dumps({
    "advFilingData": {
        "2026": {"files": [_mfile("20260801_20260831", "2026-09-02"),
                           _mfile("20260301_20260331", "2026-04-03")]},
        # the SEC restated the whole 2025 window in place on 2026-05-04
        "2025": {"files": [_mfile("20250101_20250131", "2026-05-04")]},
    },
    "advPartFData": {"2026": {"files": [{"fileName": "ADV_Part_F_2026.zip"}]}},
})

BASE_HEADERS = ["FilingID", "FormVersion", "DateSubmitted", "1A", "1B1", "1C-Legal", "1D", "1E1",
                "1F1-Street 1", "1F1-City"]


def _base_row(**over):
    row = {h: "" for h in BASE_HEADERS}
    row.update({"FilingID": "2122778", "FormVersion": "10/2021",
                "DateSubmitted": "08/11/2026", "1A": "KATERI FUND MANAGER LLC",
                "1D": "802-137268", "1E1": "343311", "1F1-City": "NEW YORK"})
    row.update(over)
    return row


from app.ingest.bulk.sec_adv_schedule_d.parse import REQUIRED_7B1  # noqa: E402

HEADERS_7B1 = list(REQUIRED_7B1)


def _fund_row(**over):
    row = {h: "" for h in HEADERS_7B1}
    row.update({
        "FilingID": "2122778", "Fund Name": "KATERI PE FUND I LP",
        "Fund ID": "805-9253414470", "ReferenceID": "539064", "State": "Delaware",
        "Country": "United States", "3(c)(1) Exclusion": "Y", "3(c)(7) Exclusion": "N",
        "Master Fund": "N", "Feeder Fund": "N", "Fund of Funds": "Y",
        "Fund Invested Self or Related": "N", "Fund Invested in Securities": "N",
        "Fund Type": "Private Equity Fund", "Gross Asset Value": "7590397",
        "Minimum Investment": "500000", "Owners": "18", "%Owned You or Related": "58",
        "%Owned Funds": "0", "Sales Limited": "Y", "%Owned Non-US": "0", "Subadviser": "N",
        "Other IAs Advise": "N", "Clients Solicited": "N", "Percentage Invested": "0",
        "Exempt from Registration": "Y", "Annual Audit": "Y", "GAAP": "Y", "FS Distributed": "Y",
        "Unqualified Opinion": "Yes", "Prime Brokers": "N", "Custodians": "N",
        "Administrator": "Y", "% Assets Valued": "81", "Marketing": "N",
    })
    row.update(over)
    return row


def _csv_bytes(headers, rows, encoding="utf-8"):
    buf = io.StringIO(newline="")
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    w.writerow(headers)
    for r in rows:
        w.writerow([r.get(h, "") for h in headers])
    return buf.getvalue().encode(encoding)


def _zip(path, members):
    """members: {name: bytes}"""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return path


def _release_zip(path, period="20260801_20260831", ia_funds=None, era_funds=None,
                 ia_base=None, era_base=None, headers_7b1=None, extra=None):
    """A realistic release zip: IA members UTF-8, ERA members cp1252."""
    h = headers_7b1 or HEADERS_7B1
    members = {f"IA_Schedule_D_7B1A17b_{period}.csv": b"FilingID,Fund ID\r\n"}  # sub-schedule: ignored
    if ia_base is not None:
        members[f"IA_ADV_Base_A_{period}.csv"] = _csv_bytes(BASE_HEADERS, ia_base)
    if era_base is not None:
        members[f"ERA_ADV_Base_{period}.csv"] = _csv_bytes(BASE_HEADERS, era_base, "cp1252")
    if ia_funds is not None:
        members[f"IA_Schedule_D_7B1_{period}.csv"] = _csv_bytes(h, ia_funds)
    if era_funds is not None:
        members[f"ERA_Schedule_D_7B1_{period}.csv"] = _csv_bytes(h, era_funds, "cp1252")
    members.update(extra or {})
    return _zip(path, members)


def _rows(iterator, columns):
    return [dict(zip([c for c, _ in columns], r)) for r in iterator]


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_discover_keys_urls_and_order():
    """T1"""
    from app.ingest.bulk.sec_adv_schedule_d.source import SecAdvScheduleD

    http = MagicMock()
    http.get_text.return_value = MANIFEST
    rels = SecAdvScheduleD().discover(http)

    assert [r.release_key for r in rels] == [
        "adv1:2025-01:20260504", "adv1:2026-03:20260403", "adv1:2026-08:20260902"]
    assert [r.url for r in rels] == [
        FOIA + "2025/ADV_Filing_Data_20250101_20250131.zip",
        FOIA + "2026/ADV_Filing_Data_20260301_20260331.zip",
        FOIA + "2026/ADV_Filing_Data_20260801_20260831.zip",
    ]
    assert rels[-1].meta["period_start"] == "2026-08-01" and rels[-1].meta["period_end"] == "2026-08-31"
    assert rels[-1].meta["year"] == "2026"
    http.get_text.assert_called_once_with(
        "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json")

    since = SecAdvScheduleD().discover(http, since=date(2026, 4, 1))
    assert [r.release_key for r in since] == ["adv1:2026-08:20260902"]


@pytest.mark.unit
def test_discover_raises_on_manifest_drift():
    """T2"""
    from app.ingest.bulk.sec_adv_schedule_d.source import SecAdvScheduleD

    http = MagicMock()
    for payload in (json.dumps({"advPartFData": {}}), json.dumps({"advFilingData": {}}), "<html>"):
        http.get_text.return_value = payload
        with pytest.raises(RuntimeError, match="manifest"):
            SecAdvScheduleD().discover(http)

    # shape intact but no dated zips: the message names what it did see
    http.get_text.return_value = json.dumps(
        {"advFilingData": {"2026": {"files": [{"fileName": "ADV_Filing_Data_2026.zip"}]}}})
    with pytest.raises(RuntimeError, match="ADV_Filing_Data_2026.zip"):
        SecAdvScheduleD().discover(http)


@pytest.mark.unit
def test_release_key_tracks_reupload():
    """T3"""
    from app.ingest.bulk.sec_adv_schedule_d.source import SecAdvScheduleD

    http = MagicMock()
    one = {"advFilingData": {"2025": {"files": [_mfile("20250301_20250331", "2025-04-02")]}}}
    http.get_text.return_value = json.dumps(one)
    first = SecAdvScheduleD().discover(http)[0]
    one["advFilingData"]["2025"]["files"][0]["uploadedOn"] = "2026-05-04"
    http.get_text.return_value = json.dumps(one)
    second = SecAdvScheduleD().discover(http)[0]

    assert first.release_key == "adv1:2025-03:20250402"
    assert second.release_key == "adv1:2025-03:20260504"  # same month, new work
    assert first.url == second.url


# ---------------------------------------------------------------------------
# 7B1 / base parsing
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_7b1_parse_real_header(tmp_path):
    """T4"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    assert len(p.REQUIRED_7B1) == 38
    z = _release_zip(tmp_path / "adv.zip", ia_funds=[_fund_row()], ia_base=[_base_row()])
    rows = _rows(p.iter_fund_rows(z, "adv1:2026-08:20260902"), p.FUND_COLUMNS)
    assert len(rows) == 1
    r = rows[0]
    assert r["filing_id"] == "2122778" and r["adviser_type"] == "ria"
    assert r["private_fund_id"] == "805-9253414470"          # ten digits, never truncated
    assert r["fund_name"] == "KATERI PE FUND I LP" and r["fund_name_core"] == "kateri pe fund i"
    assert Decimal(r["gross_asset_value"]) == Decimal("7590397")
    assert Decimal(r["minimum_investment"]) == Decimal("500000")
    assert r["owners"] == "18" and r["pct_owned_you_or_related"] == "58"
    assert r["pct_assets_valued"] == "81" and r["reference_id"] == "539064"
    assert r["org_state"] == "Delaware" and r["org_country"] == "United States"
    assert r["excl_3c1"] == "t" and r["excl_3c7"] == "f" and r["is_fund_of_funds"] == "t"
    assert r["fund_type"] == "Private Equity Fund"
    assert r["source_release_key"] == "adv1:2026-08:20260902"
    assert p.parse_number("7590397") == Decimal("7590397")
    assert [c for c, _ in p.FUND_COLUMNS][-1] == "source_release_key"

    base = _rows(p.iter_filing_rows(z, "adv1:2026-08:20260902"), p.FILING_COLUMNS)
    assert base == [{"filing_id": "2122778", "crd_number": "343311", "adviser_type": "ria",
                     "sec_number": "802-137268", "legal_name": "KATERI FUND MANAGER LLC",
                     "form_version": "10/2021", "filed_at": "2026-08-11T00:00:00",
                     "source_release_key": "adv1:2026-08:20260902"}]
    # IA carries a time of day, ERA does not
    assert p.parse_timestamp("08/10/2026 09:45:08 PM") == datetime(2026, 8, 10, 21, 45, 8)
    assert p.parse_timestamp("08/11/2026") == datetime(2026, 8, 11)
    assert p.parse_timestamp("") is None and p.parse_timestamp("Aug 2026") is None


@pytest.mark.unit
def test_blank_flags_are_none_not_false(tmp_path):
    """T5"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    z = _release_zip(tmp_path / "adv.zip", ia_base=[_base_row()], ia_funds=[_fund_row(**{
        "Sales Limited": "", "GAAP": "", "FS Distributed": "", "Master Fund": "",
        "Unqualified Opinion": "Report Not Yet Received", "Gross Asset Value": "",
        "Owners": "", "% Assets Valued": "", "Master Fund Name": "", "State": "",
    })])
    r = _rows(p.iter_fund_rows(z, "k"), p.FUND_COLUMNS)[0]
    # a blank flag is "not answered", which is not the same as "no"
    for col in ("sales_limited", "gaap", "fs_distributed", "is_master_fund"):
        assert r[col] is None, col
    assert r["is_feeder_fund"] == "f"  # an explicit N still means False
    assert r["gross_asset_value"] is None and r["owners"] is None and r["pct_assets_valued"] is None
    assert r["master_fund_name"] is None and r["org_state"] is None
    # free text, not a flag
    assert r["unqualified_opinion"] == "Report Not Yet Received"


@pytest.mark.unit
def test_cp1252_and_utf8_members_round_trip(tmp_path):
    """T6"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    accented = "CAFÉ\xa0CAPITAL MÜNCHEN FONDS"
    z = _release_zip(
        tmp_path / "adv.zip",
        ia_base=[_base_row(**{"FilingID": "1", "1E1": "11", "1A": "UTF-8 ADVISERS"})],
        era_base=[_base_row(**{"FilingID": "2", "1E1": "22", "1A": "CRÊPE ADVISORS"})],
        ia_funds=[_fund_row(**{"FilingID": "1", "Fund ID": "805-0000000001",
                               "Fund Name": "NAÏVE GROWTH FUND"})],
        era_funds=[_fund_row(**{"FilingID": "2", "Fund ID": "805-0000000002",
                                "Fund Name": accented})],
    )
    funds = {r["private_fund_id"]: r for r in _rows(p.iter_fund_rows(z, "k"), p.FUND_COLUMNS)}
    filings = {r["filing_id"]: r for r in _rows(p.iter_filing_rows(z, "k"), p.FILING_COLUMNS)}
    assert funds["805-0000000001"]["fund_name"] == "NAÏVE GROWTH FUND"   # utf-8 member
    assert funds["805-0000000002"]["fund_name"] == accented              # cp1252 member
    assert filings["2"]["legal_name"] == "CRÊPE ADVISORS"


@pytest.mark.unit
def test_header_drift_raises_naming_columns(tmp_path):
    """T7"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    dropped = [h for h in HEADERS_7B1 if h not in ("Fund ID", "% Assets Valued")]
    z = _release_zip(tmp_path / "adv.zip", ia_base=[_base_row()], ia_funds=[_fund_row()],
                     headers_7b1=dropped)
    with pytest.raises(RuntimeError, match=r"Fund ID.*% Assets Valued"):
        list(p.iter_fund_rows(z, "k"))

    thin = _zip(tmp_path / "base.zip", {
        "IA_ADV_Base_A_20260801_20260831.csv": _csv_bytes(
            [h for h in BASE_HEADERS if h != "1E1"], [_base_row()])})
    with pytest.raises(RuntimeError, match="1E1"):
        list(p.iter_filing_rows(thin, "k"))


@pytest.mark.unit
def test_missing_7b1_member_raises_listing_members(tmp_path):
    """T8"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    # only the sub-schedules are present: 7B1A17b must not be mistaken for 7B1
    z = _release_zip(tmp_path / "adv.zip", ia_base=[_base_row()])
    with pytest.raises(RuntimeError, match="IA_Schedule_D_7B1A17b_20260801_20260831.csv"):
        list(p.iter_fund_rows(z, "k"))
    with pytest.raises(RuntimeError, match="Schedule_D_7B1"):
        list(p.iter_fund_rows(z, "k"))

    nobase = _release_zip(tmp_path / "nobase.zip", ia_funds=[_fund_row()])
    with pytest.raises(RuntimeError, match="ADV_Base"):
        list(p.iter_filing_rows(nobase, "k"))


@pytest.mark.unit
def test_unknown_fund_type_passes_through(tmp_path):
    """T9"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    z = _release_zip(tmp_path / "adv.zip", ia_base=[_base_row()], ia_funds=[_fund_row(**{
        "Fund Type": "Digital Asset Fund", "Fund Type Other": "TOKENIZED CREDIT"})])
    r = _rows(p.iter_fund_rows(z, "k"), p.FUND_COLUMNS)[0]
    # an SEC vocabulary addition must widen the data, not fail the load
    assert r["fund_type"] == "Digital Asset Fund" and r["fund_type_other"] == "TOKENIZED CREDIT"


@pytest.mark.unit
def test_ia_and_era_differ_only_by_adviser_type(tmp_path):
    """T10"""
    from app.ingest.bulk.sec_adv_schedule_d import parse as p

    z = _release_zip(tmp_path / "adv.zip",
                     ia_base=[_base_row()], era_base=[_base_row()],
                     ia_funds=[_fund_row()], era_funds=[_fund_row()])
    filings = _rows(p.iter_filing_rows(z, "k"), p.FILING_COLUMNS)
    funds = _rows(p.iter_fund_rows(z, "k"), p.FUND_COLUMNS)
    assert len(filings) == 2 and len(funds) == 2
    for pair in (filings, funds):
        era, ria = sorted(pair, key=lambda r: r["adviser_type"])
        assert era["adviser_type"] == "era" and ria["adviser_type"] == "ria"
        assert {k: v for k, v in era.items() if k != "adviser_type"} \
            == {k: v for k, v in ria.items() if k != "adviser_type"}
    # ERA is read first so the registered row wins the merge for a filing in both
    assert [r["adviser_type"] for r in filings] == ["era", "ria"]


@pytest.mark.unit
def test_source_registered():
    """T11"""
    from app.ingest.bulk import registry

    import app.ingest.bulk.sec_adv_schedule_d  # noqa: F401
    assert "sec_adv_schedule_d" in registry.BULK_SOURCES
    assert registry.BULK_SOURCES["sec_adv_schedule_d"].parser_version == "1"


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

TABLES = ("sec_adv_filings", "sec_adv_private_fund_filings")


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    path = REPO / "alembic" / "versions" / "0010_adv_private_funds.py"
    spec = importlib.util.spec_from_file_location("mig0010", path)
    mig = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mig)

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        # the migration also alters pe_funds; a stub is enough to apply it here
        conn.execute(text("CREATE TABLE IF NOT EXISTS pe_funds (id SERIAL PRIMARY KEY)"))
        for stmt in mig.UPGRADE_SQL:
            conn.execute(text(stmt))
        for t in TABLES:
            conn.execute(text(f"TRUNCATE public.{t}"))
    yield engine
    engine.dispose()


def _dump(engine, table, skip=("loaded_at",)):
    """Table contents for comparison, minus the columns that record WHEN/BY WHICH
    RUN a row was written rather than what it says."""
    from sqlalchemy import text

    with engine.connect() as conn:
        cols = [c for c in conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name = :t "
            "ORDER BY ordinal_position"), {"t": table}).scalars() if c not in skip]
        col_sql = ", ".join(f'"{c}"' for c in cols)
        return [tuple(r) for r in conn.execute(text(
            f"SELECT {col_sql} FROM public.{table} ORDER BY {col_sql}")).all()]


def _load(engine, src, key, path):
    from app.ingest.bulk.base import Release

    with engine.begin() as conn:
        return src.load(conn, Release(key, "https://x/ADV.zip", {}), path)


@pg
def test_load_is_idempotent_and_drops_staging_pg(pg_engine, tmp_path):
    """T12"""
    from sqlalchemy import text
    from app.ingest.bulk.sec_adv_schedule_d.source import SecAdvScheduleD

    src = SecAdvScheduleD()
    z = _release_zip(
        tmp_path / "aug.zip",
        ia_base=[_base_row(**{"FilingID": "2122778", "1E1": "343311",
                              "DateSubmitted": "08/11/2026 09:45:08 AM"})],
        era_base=[_base_row(**{"FilingID": "2122779", "1E1": "999888", "1A": "ÉLAN ERA LLC"})],
        ia_funds=[_fund_row(), _fund_row(**{"Fund ID": "805-1111111111", "Fund Name": "KATERI PE FUND II LP"})],
        era_funds=[_fund_row(**{"FilingID": "2122779", "Fund ID": "805-2222222222",
                                "Fund Name": "ÉLAN OPPORTUNITIES FUND"})],
    )
    out1 = _load(pg_engine, src, "adv1:2026-08:20260902", z)
    out2 = _load(pg_engine, src, "adv1:2026-08:20260902", z)
    assert out1 == {"sec_adv_filings": 2, "sec_adv_private_fund_filings": 3,
                    "refused_filings": 0, "dropped_fund_rows": 0}
    assert out2 == {"sec_adv_filings": 0, "sec_adv_private_fund_filings": 0,
                    "refused_filings": 0, "dropped_fund_rows": 0}

    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_private_fund_filings")).scalar() == 3
        row = conn.execute(text(
            "SELECT crd_number, adviser_type, sec_number, legal_name, filed_at "
            "FROM sec_adv_filings WHERE filing_id = 2122778")).one()
        assert row == ("343311", "ria", "802-137268", "KATERI FUND MANAGER LLC",
                       datetime(2026, 8, 11, 9, 45, 8))
        fund = conn.execute(text(
            "SELECT gross_asset_value, owners, pct_assets_valued, fund_name_core, sales_limited, "
            "unqualified_opinion, source_release_key FROM sec_adv_private_fund_filings "
            "WHERE private_fund_id = '805-9253414470'")).one()
        assert fund == (Decimal("7590397"), 18, Decimal("81.000"), "kateri pe fund i", True,
                        "Yes", "adv1:2026-08:20260902")
        assert conn.execute(text(
            "SELECT legal_name FROM sec_adv_filings WHERE filing_id = 2122779")).scalar() == "ÉLAN ERA LLC"
        for stg in ("sec_adv_sd_filings", "sec_adv_sd_funds"):
            assert conn.execute(text(f"SELECT to_regclass('stg.{stg}')")).scalar() is None


@pg
def test_release_order_does_not_change_the_result_pg(pg_engine, tmp_path):
    """T13 — the point of filing grain: March and August may load in either order."""
    from sqlalchemy import text
    from app.ingest.bulk.sec_adv_schedule_d.source import SecAdvScheduleD

    src = SecAdvScheduleD()
    # same adviser, same fund, restated in August with a bigger GAV and a second fund
    march = _release_zip(
        tmp_path / "mar.zip", period="20260301_20260331",
        ia_base=[_base_row(**{"FilingID": "2100001", "DateSubmitted": "03/15/2026 08:00:00 AM"})],
        ia_funds=[_fund_row(**{"FilingID": "2100001", "Gross Asset Value": "100000"})])
    august = _release_zip(
        tmp_path / "aug.zip",
        ia_base=[_base_row(**{"FilingID": "2122778", "DateSubmitted": "08/11/2026 09:45:08 AM"}),
                 # the restatement case: March's filing, corrected, shipped again
                 # inside the later release. Whichever order they load, the newer
                 # release's values must win.
                 _base_row(**{"FilingID": "2100001", "DateSubmitted": "03/15/2026 08:00:00 AM"})],
        ia_funds=[_fund_row(**{"Gross Asset Value": "200000"}),
                  _fund_row(**{"Fund ID": "805-1111111111", "Fund Name": "KATERI PE FUND II LP"}),
                  _fund_row(**{"FilingID": "2100001", "Gross Asset Value": "123456"})])
    mar_key, aug_key = "adv1:2026-03:20260403", "adv1:2026-08:20260902"

    # source_release_key is provenance, not data: when two releases carry an
    # identical row the merge skips the rewrite, so the row keeps the key of
    # whichever release happened to write it first. The DATA must not differ.
    ignore = ("loaded_at", "source_release_key")
    _load(pg_engine, src, aug_key, august)
    _load(pg_engine, src, mar_key, march)
    aug_then_mar = {t: _dump(pg_engine, t, ignore) for t in TABLES}

    with pg_engine.begin() as conn:
        for t in TABLES:
            conn.execute(text(f"TRUNCATE public.{t}"))
    _load(pg_engine, src, mar_key, march)
    _load(pg_engine, src, aug_key, august)
    mar_then_aug = {t: _dump(pg_engine, t, ignore) for t in TABLES}

    assert aug_then_mar == mar_then_aug
    assert len(mar_then_aug["sec_adv_filings"]) == 2          # both filings kept
    assert len(mar_then_aug["sec_adv_private_fund_filings"]) == 3
    with pg_engine.connect() as conn:
        restated = conn.execute(text(
            "SELECT gross_asset_value, source_release_key FROM sec_adv_private_fund_filings "
            "WHERE filing_id = 2100001")).one()
    # the August release restated March's filing: its value stands in both orders
    assert restated == (Decimal("123456"), aug_key)
    with pg_engine.connect() as conn:
        # neither order lets the older March filing regress the fund's current state
        gav = conn.execute(text(
            "SELECT f.gross_asset_value FROM sec_adv_private_fund_filings f "
            "JOIN sec_adv_filings a USING (filing_id) "
            "WHERE f.private_fund_id = '805-9253414470' ORDER BY a.filed_at DESC LIMIT 1")).scalar()
        assert gav == Decimal("200000")


@pg
def test_unknown_filing_id_raises_without_partial_rows_pg(pg_engine, tmp_path):
    """T14"""
    from sqlalchemy import text
    from app.ingest.bulk.sec_adv_schedule_d.source import SecAdvScheduleD

    src = SecAdvScheduleD()
    good = _release_zip(tmp_path / "aug.zip", ia_base=[_base_row()], ia_funds=[_fund_row()])
    _load(pg_engine, src, "adv1:2026-08:20260902", good)

    orphaned = _release_zip(
        tmp_path / "sep.zip", period="20260901_20260930",
        ia_base=[_base_row(**{"FilingID": "2130000", "1E1": "555555"})],
        ia_funds=[_fund_row(**{"FilingID": "2130000", "Fund ID": "805-3333333333"}),
                  _fund_row(**{"FilingID": "9999999", "Fund ID": "805-4444444444"})])
    with pytest.raises(RuntimeError, match="9999999"):
        _load(pg_engine, src, "adv1:2026-09:20261002", orphaned)

    with pg_engine.connect() as conn:
        # the whole release rolled back: no filing and no fund from it survived
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_filings")).scalar() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_private_fund_filings")).scalar() == 1
        assert conn.execute(text(
            "SELECT COUNT(*) FROM sec_adv_private_fund_filings "
            "WHERE private_fund_id = '805-3333333333'")).scalar() == 0


@pytest.mark.unit
def test_every_observed_date_format_parses():
    """The SEC changes DateSubmitted format between months; an unparsed date
    costs the filing its funds in the current-state mart (measured: February
    2025 uses "2/5/2025 13:19" and cost 31,209 filings their date)."""
    from app.ingest.bulk.sec_adv_schedule_d.parse import parse_timestamp

    assert parse_timestamp("08/10/2026 09:45:08 AM") == datetime(2026, 8, 10, 9, 45, 8)
    assert parse_timestamp("2/5/2025 13:19") == datetime(2025, 2, 5, 13, 19)
    assert parse_timestamp("08/11/2026") == datetime(2026, 8, 11)
    assert parse_timestamp("2025-01-09 08:50:29") == datetime(2025, 1, 9, 8, 50, 29)
    assert parse_timestamp("not a date") is None
    assert parse_timestamp("") is None
