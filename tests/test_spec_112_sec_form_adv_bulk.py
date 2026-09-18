"""
Tests for SPEC 112 — Form ADV bulk loaders (sec_adv_roster, sec_iapd_feed).

Fixtures mirror the real SEC formats measured 2026-09-16 (see the spec):
listing-page anchors, 448/171-column roster CSVs (cp1252 bytes, padded
money), and the IAPD <Firm> XML shape.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import csv
import gzip
import io
import json
import os
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

DIR_NEW = "/files/investment/data/other/information-about-registered-investment-advisers-exempt-reporting-advisers/"
DIR_OLD = "/files/investment/data/information-about-registered-investment-advisers-exempt-reporting-advisers/"


def _a(path, text):
    return f'<td class="views-field">  <a href="{path}" download>{text}</a>\n</td>'


LISTING_HTML = "\n".join([
    '<a href="https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers">self</a>',
    _a(DIR_NEW + "ia09012026-exempt.zip", "Exempt Investment Advisers, September 2026"),
    _a(DIR_NEW + "ia09012026-registered.zip", "Registered Investment Advisers, September 2026"),
    _a(DIR_NEW + "ia08032026-exempt_1.zip", "Exempt Investment Advisers, August 2026"),
    _a(DIR_NEW + "ia08032026_1.zip", "Registered Investment Advisers, August 2026"),
    _a(DIR_NEW + "ia08032026_0.zip", "Registered Investment Advisers, August 2026"),
    _a(DIR_NEW + "ia060126_0.zip", "Registered Investment Advisers, June 2026"),
    _a(DIR_NEW + "ia020226-exemptzip.zip", "Exempt Investment Advisers, February 2026"),
    _a(DIR_OLD + "ia122025-exempt.zip", "Exempt Investment Advisers, December 2025"),
    _a(DIR_OLD + "ia122025.zip", "Registered Investment Advisers, December 2025"),
    _a(DIR_OLD + "ia-no-data-110125.pdf", "Registered Investment Advisers, November 2025"),
    _a(DIR_OLD + "ia09022025.xlsx", "Registered Investment Advisers, September 2025"),
    _a(DIR_OLD + "ia042025-exempt.xlsx", "Exempt Investment Advisers, April 2025"),
])


# ---------------------------------------------------------------------------
# listing / discover
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_roster_listing_classification():
    """T1"""
    from app.ingest.bulk.sec_form_adv.parse import classify_listing

    files, skipped = classify_listing(LISTING_HTML)
    got = {(f.adviser_type, f.roster_date): f for f in files}
    assert (("ria", date(2026, 9, 1)) in got) and (("era", date(2026, 9, 1)) in got)
    aug = got[("ria", date(2026, 8, 3))]
    assert aug.filename == "ia08032026_1.zip" and aug.suffix == 1  # re-upload wins
    assert aug.url == "https://www.sec.gov" + DIR_NEW + "ia08032026_1.zip"
    assert ("ria", date(2026, 6, 1)) in got                      # MMDDYY
    assert ("era", date(2026, 2, 2)) in got                      # -exemptzip
    assert ("ria", date(2025, 12, 1)) in got                     # MMYYYY, anchor month
    assert ("era", date(2025, 12, 1)) in got
    assert len(files) == 8
    assert all(f.filename.endswith(".zip") for f in files)
    assert skipped["xlsx"] == 2 and skipped["pdf"] == 1


@pytest.mark.unit
def test_roster_discover_window_and_since():
    """T2"""
    from app.ingest.bulk.sec_form_adv.roster import SecAdvRoster

    anchors = []
    for i in range(40):
        y, m = 2026 + (i // 12), (i % 12) + 1
        month = date(y, m, 1).strftime("%B")
        anchors.append(_a(DIR_NEW + f"ia{m:02d}01{y}.zip", f"Registered Investment Advisers, {month} {y}"))
        anchors.append(_a(DIR_NEW + f"ia{m:02d}01{y}-exempt.zip", f"Exempt Investment Advisers, {month} {y}"))
    # pre-CSV-era zip (wraps an xlsx) and an xlsx-era file: never releases
    anchors.append(_a(DIR_OLD + "ia040423.zip", "Registered Investment Advisers, April 2023"))
    anchors.append(_a(DIR_OLD + "ia09022025.xlsx", "Registered Investment Advisers, September 2025"))
    http = MagicMock()
    http.get_text.return_value = "\n".join(reversed(anchors))

    rels = SecAdvRoster().discover(http)
    ria = [r for r in rels if r.meta["adviser_type"] == "ria"]
    era = [r for r in rels if r.meta["adviser_type"] == "era"]
    assert len(ria) == 3 and len(era) == 3  # 3 calendar months (Lean budget)
    assert ria[0].release_key == "ria:2029-02-01" and ria[-1].release_key == "ria:2029-04-01"
    dates = [r.meta["roster_date"] for r in rels]
    assert dates == sorted(dates)
    assert all(r.meta["newest_roster_date"] == "2029-04-01" for r in rels)

    since_rels = SecAdvRoster().discover(http, since=date(2029, 3, 1))
    assert sorted(r.release_key for r in since_rels) == ["era:2029-03-01", "era:2029-04-01",
                                                         "ria:2029-03-01", "ria:2029-04-01"]
    all_rels = SecAdvRoster().discover(http, since=date(2020, 1, 1))
    assert len(all_rels) == 80 and not any("2023" in r.release_key for r in all_rels)


# ---------------------------------------------------------------------------
# value parsing
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_parse_numbers_dates_flags():
    """T3"""
    from app.ingest.bulk.sec_form_adv import parse as p

    assert p.parse_number("           628,902,725.00") == Decimal("628902725.00")
    assert p.parse_number("                      .00") == Decimal("0.00")
    assert p.parse_number("35557038") == Decimal("35557038")
    assert p.parse_number(" More than 500") is None
    assert p.parse_number("Fewer than 5 clients") is None
    assert p.parse_number("") is None and p.parse_number(None) is None
    assert p.parse_int("               1,548") == 1548
    assert p.parse_int(" 18") == 18
    assert p.parse_yn("Y") is True and p.parse_yn("N") is False and p.parse_yn("") is None
    assert p.parse_date("06/26/2026") == date(2026, 6, 26)
    assert p.parse_date("2026-03-04") == date(2026, 3, 4)
    assert p.parse_date("11/2025") is None
    assert p.parse_notice_states("AZ-07/19/2004, CA-07/08/1997, VI-02/23/2021") == ["AZ", "CA", "VI"]
    assert p.parse_notice_states("") is None
    assert p.pg_array(["AZ", "CA"]) == "{AZ,CA}"


# ---------------------------------------------------------------------------
# roster CSV
# ---------------------------------------------------------------------------

RIA_HEADERS = [
    "SEC Region", "Organization CRD#", "SEC#", "Firm Type", "Umbrella Registration",
    "Primary Business Name", "Legal Name", "Main Office Street Address 1", "Main Office Street Address 2",
    "Main Office City", "Main Office State", "Main Office Country", "Main Office Postal Code",
    "Main Office Telephone Number", "Main Office Facsimile Number",
    "Total number of offices, other than your Principal Office and place of business",
    "Mail Office Street Address 1", "Mail Office City", "Mail Office State", "Mail Office Country",
    "SEC Current Status", "SEC Status Effective Date", "Jurisdiction Notice Filed-Effective Date",
    "Latest ADV Filing Date", "Form Version", "Website Address", "3A",
    "5A", "5B(1)", "5C(1)", "5C(1)-If more than 100, how many", "5D(a)(1)", "5D(b)(1)", "5D(f)(1)",
    "5F(2)(a)", "5F(2)(b)", "5F(2)(c)", "5F(2)(d)", "5F(2)(e)", "5F(2)(f)", "5H",
    "Count of Private Funds - 7B(1)", "Total Gross Assets of Private Funds",
    "9A(1)(a)", "9B(1)(a)", "Total Custody Amount",
    "11", "11A(1)", "Count of 11A(1) disclosures", "11H(2)", "Count of 11H(2) disclosures", "12A",
]


def _ria_row(**over):
    row = {h: "" for h in RIA_HEADERS}
    row.update({
        "SEC Region": "NYRO", "Organization CRD#": "249", "SEC#": "801-887", "Firm Type": "Registered",
        "Umbrella Registration": "N", "Primary Business Name": "OPPENHEIMER & CO. INC.",
        "Legal Name": "OPPENHEIMER & CO. INC.", "Main Office Street Address 1": "85 BROAD STREET",
        "Main Office City": "NEW YORK", "Main Office State": "NY", "Main Office Country": "United States",
        "Main Office Postal Code": "10004", "Main Office Telephone Number": "212-668-8000",
        "Total number of offices, other than your Principal Office and place of business": "109",
        "Mail Office Street Address 1": "PO BOX 1", "Mail Office City": "NEW YORK",
        "SEC Current Status": "Approved", "SEC Status Effective Date": "02/09/1955",
        "Jurisdiction Notice Filed-Effective Date": "AK-01/10/2003, AL-01/10/2003",
        "Latest ADV Filing Date": "06/23/2026", "Form Version": "10/2021",
        "Website Address": "HTTP://WWW.OPPENHEIMER.COM", "3A": "Corporation",
        "5A": "2806", "5B(1)": "1219", "5C(1)": " More than 100", "5C(1)-If more than 100, how many": "5500",
        "5D(a)(1)": "35818", "5D(b)(1)": "1229", "5D(f)(1)": "0",
        "5F(2)(a)": "        14,233,267,307.00", "5F(2)(b)": "        22,477,438,441.00",
        "5F(2)(c)": "        36,710,705,748.00", "5F(2)(d)": "              20,471",
        "5F(2)(e)": "              18,565", "5F(2)(f)": "              39,036", "5H": " More than 500",
        "Count of Private Funds - 7B(1)": "                   0",
        "Total Gross Assets of Private Funds": "                      .00",
        "9A(1)(a)": "Y", "9B(1)(a)": "N", "Total Custody Amount": "        26,337,270,635.00",
        "11": "Y", "11A(1)": "Y", "Count of 11A(1) disclosures": "2", "11H(2)": "N",
        "Count of 11H(2) disclosures": "0",
    })
    row.update(over)
    return row


def _csv_bytes(headers, rows, encoding="cp1252", bom=False):
    buf = io.StringIO(newline="")
    w = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    w.writerow(headers)
    for r in rows:
        w.writerow([r.get(h, "") for h in headers])
    data = buf.getvalue().encode(encoding)
    return (b"\xef\xbb\xbf" + data) if bom else data


def _zip(path, member, data):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member, data)
    return path


ERA_HEADERS = [
    "SEC Region", "Organization CRD#", "SEC#", "Firm Type", "Primary Business Name", "Legal Name",
    "Main Office Street Address 1", "Main Office City", "Main Office State", "Main Office Country",
    "SEC Current Status", "Latest ADV Filing Date", "Website Address",
    "Count of Private Funds - 7B(1)", "Any PE Funds", "Total number of PE funds",
    "Total Gross Assets of Private Funds", "11", "11A(1)", "Count of 11A(1) disclosures",
]


def _era_rows():
    return [{
        "SEC Region": "HQ", "Organization CRD#": "313308", "SEC#": "802-120930", "Firm Type": "ERA",
        "Primary Business Name": "TRILL IMPACT ADVISORY GMBH", "Legal Name": "TRILL IMPACT ADVISORY GMBH",
        "Main Office Street Address 1": "FÄRBERGRABEN 16", "Main Office City": "MUNICH",
        "Main Office State": "", "Main Office Country": "Germany", "SEC Current Status": "ERA - Active",
        "Latest ADV Filing Date": "03/30/2026", "Website Address": "https://www.linkedin.com/company/trill-impact/",
        "Count of Private Funds - 7B(1)": "                   1", "Any PE Funds": "Y",
        "Total number of PE funds": "1", "Total Gross Assets of Private Funds": "            78,960,522.00",
        "11": "N", "11A(1)": "N", "Count of 11A(1) disclosures": "0",
    }]


@pytest.mark.unit
def test_roster_csv_parse_real_headers(tmp_path):
    """T4"""
    from app.ingest.bulk.sec_form_adv.parse import ROSTER_COLUMNS, iter_roster_rows

    names = [c for c, _ in ROSTER_COLUMNS]
    zpath = _zip(tmp_path / "ia09012026-registered.zip", "IA_SEC_-_FIRM_ROSTER_FOIA_DOWNLOAD_-_1.CSV",
                 _csv_bytes(RIA_HEADERS, [_ria_row(), _ria_row(**{"Organization CRD#": "38",
                                                               "Primary Business Name": "CAFÉ\xa0ADVISORS",
                                                               "Main Office City": "MÜNCHEN"})]))
    rows = [dict(zip(names, r)) for r in iter_roster_rows(zpath, "ria", date(2026, 9, 1), "ria:2026-09-01")]
    assert len(rows) == 2
    r = rows[0]
    assert r["crd_number"] == "249" and r["adviser_type"] == "ria" and r["roster_date"] == "2026-09-01"
    assert r["aum_total"] == "36710705748.00" and r["accounts_total"] == "39036"
    assert r["employees_total"] == "2806" and r["employees_investment_advisory"] == "1219"
    assert r["clients_count"] == "5500"
    assert r["custody_client_cash_securities"] == "t" and r["custody_amount"] == "26337270635.00"
    assert r["private_fund_gross_assets"] == "0.00"
    assert r["latest_filing_date"] == "2026-06-23" and r["sec_status_effective_date"] == "1955-02-09"
    assert r["notice_filed_states"] == "{AK,AL}"
    assert r["has_disciplinary_disclosure"] == "t" and r["drp_disclosure_total"] == "2"
    drp = json.loads(r["drp_flags"])
    assert drp["11A(1)"] == "Y" and drp["Count of 11A(1) disclosures"] == "2" and "11" in drp
    raw = json.loads(r["raw"])
    assert raw["3A"] == "Corporation" and raw["Mail Office Street Address 1"] == "PO BOX 1"
    assert raw["5H"] == "More than 500"
    assert "Organization CRD#" not in raw and "5F(2)(c)" not in raw and "12A" not in raw
    assert "11A(1)" not in raw
    # cp1252 file: C9 A0 alone is valid UTF-8, so the encoding is detected per file
    assert rows[1]["business_name"] == "CAFÉ\xa0ADVISORS" and rows[1]["main_office_city"] == "MÜNCHEN"

    # ERA: 171-col layout without Item 5; UTF-8 with BOM also accepted
    epath = _zip(tmp_path / "ia09012026-exempt.zip", "X.CSV", _csv_bytes(ERA_HEADERS, _era_rows(), "utf-8", bom=True))
    erows = [dict(zip(names, r)) for r in iter_roster_rows(epath, "era", date(2026, 9, 1), "era:2026-09-01")]
    assert len(erows) == 1
    e = erows[0]
    assert e["crd_number"] == "313308" and e["main_office_street1"] == "FÄRBERGRABEN 16"
    assert e["aum_total"] is None and e["employees_total"] is None
    assert e["private_fund_count"] == "1" and e["private_fund_gross_assets"] == "78960522.00"
    assert json.loads(e["raw"])["Any PE Funds"] == "Y"


@pytest.mark.unit
def test_roster_missing_header_raises(tmp_path):
    """T5"""
    from app.ingest.bulk.sec_form_adv.parse import iter_roster_rows

    headers = [h for h in RIA_HEADERS if h != "5F(2)(c)"]
    zpath = _zip(tmp_path / "ia.zip", "X.CSV", _csv_bytes(headers, [_ria_row()]))
    with pytest.raises(RuntimeError, match="5F\\(2\\)\\(c\\)"):
        list(iter_roster_rows(zpath, "ria", date(2026, 9, 1), "k"))
    empty = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty, "w") as zf:
        zf.writestr("readme.txt", "x")
    with pytest.raises(RuntimeError, match="no CSV"):
        list(iter_roster_rows(empty, "ria", date(2026, 9, 1), "k"))


# ---------------------------------------------------------------------------
# IAPD feed
# ---------------------------------------------------------------------------

MANIFEST = json.dumps({"files": [
    {"name": "IA_FIRM_SEC_Feed_09_16_2026.xml.gz", "size": "79 MB", "date": "09/16/2026"},
    {"name": "IA_FIRM_STATE_Feed_09_16_2026.xml.gz", "size": "69 MB", "date": "09/16/2026"},
    {"name": "IA_INDVL_Feed_09_16_2026.xml.zip", "size": "168 MB", "date": "09/16/2026"},
]})


@pytest.mark.unit
def test_iapd_manifest_discover():
    """T6"""
    from app.ingest.bulk.sec_form_adv.iapd_feed import SecIapdFeed

    http = MagicMock()
    http.get_text.return_value = MANIFEST
    rels = SecIapdFeed().discover(http)
    assert len(rels) == 1
    assert rels[0].release_key == "edition:2026-09-16"
    assert rels[0].url == ("https://reports.adviserinfo.sec.gov/reports/CompilationReports/"
                           "IA_FIRM_SEC_Feed_09_16_2026.xml.gz")
    assert SecIapdFeed().discover(http, since=date(2026, 9, 17)) == []
    assert len(SecIapdFeed().discover(http, since=date(2026, 9, 16))) == 1

    http.get_text.return_value = json.dumps({"reports": []})
    with pytest.raises(RuntimeError, match="manifest"):
        SecIapdFeed().discover(http)
    http.get_text.return_value = json.dumps({"files": [{"name": "IA_INDVL_Feed_09_16_2026.xml.zip"}]})
    with pytest.raises(RuntimeError, match="IA_FIRM_SEC_Feed"):
        SecIapdFeed().discover(http)


FEED_XML = """<?xml version="1.0" encoding="ISO-8859-1"?>
<IAPDFirmSECReport GenOn="2026-09-16">
  <Firms>
    <Firm>
      <Info SECRgnCD="NYRO" FirmCrdNb="283882" SECNb="801-135399" BusNm="RABENOLD ADVISORS, INC." LegalNm="RABENOLD ADVISORS, INC." UmbrRgstn="N"/>
      <MainAddr Strt1="5930 MAIN STREET" Strt2="SUITE 400" City="WILLIAMSVILLE" State="NY" Cntry="United States" PostlCd="14221" PhNb="716-568-8790"/>
      <MailingAddr/>
      <Rgstn FirmType="Registered" St="APPROVED" Dt="2026-02-24"/>
      <NoticeFiled>
        <States RgltrCd="NY" St="FILED" Dt="2026-02-24"/>
        <States RgltrCd="CA" St="FILED" Dt="2026-02-25"/>
      </NoticeFiled>
      <Filing Dt="2026-03-04" FormVrsn="10/2021"/>
      <FormInfo>
        <Part1A>
          <Item1 Q1F5="0" Q1I="Y">
            <WebAddrs>
              <WebAddr>HTTP://WWW.RABENOLDADVISORS.COM</WebAddr>
              <WebAddr>HTTP://LINKEDIN.COM/RABENOLD</WebAddr>
            </WebAddrs>
          </Item1>
          <Item5A TtlEmp="4"/>
          <Item5F Q5F1="Y" Q5F2A="35557038" Q5F2B="0" Q5F2C="35557038" Q5F2D="117" Q5F2E="0" Q5F2F="117" Q5F3="0"/>
          <Item11 Q11="N"/>
        </Part1A>
      </FormInfo>
    </Firm>
    <Firm>
      <Info SECRgnCD="CHRO" FirmCrdNb="312360" SECNb="802-120553" BusNm="MK CAPITAL" LegalNm="MK CAPITAL COMPANY"/>
      <MainAddr Strt1="F\xc4RBERGRABEN 16" City="MUNICH" Cntry="Germany"/>
      <MailingAddr/>
      <Rgstn FirmType="ERA" St="ACTIVE" Dt="2021-02-16"/>
      <NoticeFiled/>
      <Filing Dt="2026-03-04" FormVrsn="10/2021"/>
      <FormInfo><Part1A><Item5A/><Item5F/><Item7B Q7B="Y"/></Part1A></FormInfo>
    </Firm>
  </Firms>
</IAPDFirmSECReport>
"""


def _feed_gz(path):
    with gzip.open(path, "wb") as fh:
        fh.write(FEED_XML.encode("latin-1"))
    return path


@pytest.mark.unit
def test_iapd_iterparse_fixture(tmp_path):
    """T7"""
    from app.ingest.bulk.sec_form_adv.parse import FEED_COLUMNS, iter_feed_rows

    names = [c for c, _ in FEED_COLUMNS]
    gz = _feed_gz(tmp_path / "IA_FIRM_SEC_Feed_09_16_2026.xml.gz")
    rows = [dict(zip(names, r)) for r in iter_feed_rows(gz, date(2026, 9, 16), "edition:2026-09-16")]
    assert len(rows) == 2
    a, b = rows
    assert a["crd_number"] == "283882" and a["edition_date"] == "2026-09-16"
    assert a["business_name"] == "RABENOLD ADVISORS, INC." and a["sec_number"] == "801-135399"
    assert a["firm_type"] == "Registered" and a["registration_status"] == "APPROVED"
    assert a["registration_date"] == "2026-02-24" and a["filing_date"] == "2026-03-04"
    assert a["main_office_state"] == "NY" and a["main_office_country"] == "United States"
    assert a["aum_total"] == "35557038" and a["accounts_total"] == "117" and a["employees_total"] == "4"
    assert a["website"] == "HTTP://WWW.RABENOLDADVISORS.COM"
    assert a["notice_filed_states"] == "{CA,NY}"
    raw = json.loads(a["raw"])
    assert raw["MainAddr.Strt1"] == "5930 MAIN STREET" and raw["Info.UmbrRgstn"] == "N"
    assert raw["Part1A"]["Q5F2D"] == "117" and "Q5F2C" not in raw["Part1A"]
    assert raw["WebAddrs"] == ["HTTP://WWW.RABENOLDADVISORS.COM", "HTTP://LINKEDIN.COM/RABENOLD"]
    assert "Info.FirmCrdNb" not in raw
    assert b["firm_type"] == "ERA" and b["aum_total"] is None and b["notice_filed_states"] is None
    assert json.loads(b["raw"])["MainAddr.Strt1"] == "FÄRBERGRABEN 16"


@pytest.mark.unit
def test_sources_registered():
    """T10"""
    from app.ingest.bulk import registry

    import app.ingest.bulk.sec_form_adv  # noqa: F401
    assert "sec_adv_roster" in registry.BULK_SOURCES
    assert "sec_iapd_feed" in registry.BULK_SOURCES


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        for t in ("sec_adv_roster_snapshots", "sec_adv_feed_firm_state", "sec_form_adv"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
    yield engine
    engine.dispose()


@pg
def test_roster_load_idempotent_and_form_adv_null_preserving_pg(pg_engine, tmp_path):
    """T8"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_form_adv.roster import SecAdvRoster

    src = SecAdvRoster()
    # Pre-existing, enriched sec_form_adv row from another writer.
    with pg_engine.begin() as conn:
        src.ensure_ddl(conn)
        conn.execute(text(
            "INSERT INTO sec_form_adv (crd_number, firm_name, business_email, website, "
            "assets_under_management, is_family_office, key_personnel) "
            "VALUES ('249', 'OLD NAME', 'ir@opco.com', 'http://kept.example', 1, TRUE, "
            "CAST('[{\"name\": \"x\"}]' AS JSONB))"))

    # Row 249 has NO website in the roster -> existing website must survive.
    zpath = _zip(tmp_path / "ia09012026-registered.zip", "X.CSV",
                 _csv_bytes(RIA_HEADERS, [_ria_row(**{"Website Address": ""}),
                                          _ria_row(**{"Organization CRD#": "38", "Primary Business Name": "AIC"})]))
    rel = Release("ria:2026-09-01", "https://www.sec.gov/x/ia09012026-registered.zip",
                  {"adviser_type": "ria", "roster_date": "2026-09-01", "newest_roster_date": "2026-09-01"})
    with pg_engine.begin() as conn:
        out1 = src.load(conn, rel, zpath)
    with pg_engine.begin() as conn:
        out2 = src.load(conn, rel, zpath)
    assert out1 == out2 == {"sec_adv_roster_snapshots": 2, "sec_form_adv": 2}

    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_roster_snapshots")).scalar() == 2
        row = conn.execute(text(
            "SELECT firm_name, business_email, website, assets_under_management, is_family_office, "
            "key_personnel IS NOT NULL, registration_status, state_registrations, is_registered_with_sec, "
            "mailing_address_street1, total_client_count "
            "FROM sec_form_adv WHERE crd_number='249'")).one()
        assert row[0] == "OPPENHEIMER & CO. INC."
        assert row[1] == "ir@opco.com"              # not in roster -> preserved
        assert row[2] == "http://kept.example"      # NULL in roster -> preserved
        assert row[3] == Decimal("36710705748.00")  # roster value wins
        assert row[4] is True and row[5] is True
        assert row[6] == "Approved" and row[7] == ["AK", "AL"] and row[8] is True
        assert row[9] == "PO BOX 1" and row[10] == 5500
        snap = conn.execute(text(
            "SELECT aum_total, drp_flags->>'11A(1)', raw->>'3A', source_release_key, loaded_at IS NOT NULL "
            "FROM sec_adv_roster_snapshots WHERE crd_number='249'")).one()
        assert snap == (Decimal("36710705748.00"), "Y", "Corporation", "ria:2026-09-01", True)

    # An OLDER roster loads snapshots but must not touch sec_form_adv.
    old = _zip(tmp_path / "ia08032026_1.zip", "X.CSV",
               _csv_bytes(RIA_HEADERS, [_ria_row(**{"Primary Business Name": "STALE NAME"})]))
    old_rel = Release("ria:2026-08-03", "https://www.sec.gov/x/ia08032026_1.zip",
                      {"adviser_type": "ria", "roster_date": "2026-08-03", "newest_roster_date": "2026-09-01"})
    with pg_engine.begin() as conn:
        assert src.load(conn, old_rel, old) == {"sec_adv_roster_snapshots": 1}
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT firm_name FROM sec_form_adv WHERE crd_number='249'")).scalar() \
            == "OPPENHEIMER & CO. INC."
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_roster_snapshots")).scalar() == 3
        assert conn.execute(text("SELECT to_regclass('stg.sec_adv_roster')")).scalar() is None

    # ERA newest roster: inserts with is_registered_with_sec = FALSE.
    epath = _zip(tmp_path / "ia09012026-exempt.zip", "X.CSV", _csv_bytes(ERA_HEADERS, _era_rows()))
    era_rel = Release("era:2026-09-01", "https://www.sec.gov/x/ia09012026-exempt.zip",
                      {"adviser_type": "era", "roster_date": "2026-09-01", "newest_roster_date": "2026-09-01"})
    with pg_engine.begin() as conn:
        assert src.load(conn, era_rel, epath) == {"sec_adv_roster_snapshots": 1, "sec_form_adv": 1}
    with pg_engine.connect() as conn:
        assert conn.execute(text(
            "SELECT is_registered_with_sec, registration_status FROM sec_form_adv WHERE crd_number='313308'")).one() \
            == (False, "ERA - Active")


@pg
def test_iapd_load_idempotent_pg(pg_engine, tmp_path):
    """T9"""
    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_form_adv.iapd_feed import SecIapdFeed

    src = SecIapdFeed()
    gz = _feed_gz(tmp_path / "IA_FIRM_SEC_Feed_09_16_2026.xml.gz")
    rel = Release("edition:2026-09-16", "https://reports.adviserinfo.sec.gov/reports/CompilationReports/"
                  "IA_FIRM_SEC_Feed_09_16_2026.xml.gz", {"edition_date": "2026-09-16"})
    with pg_engine.begin() as conn:
        out1 = src.load(conn, rel, gz)
    with pg_engine.begin() as conn:
        out2 = src.load(conn, rel, gz)
    assert out1 == out2 == {"sec_adv_feed_firm_state": 2}
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_feed_firm_state")).scalar() == 2
        row = conn.execute(text(
            "SELECT aum_total, filing_date, notice_filed_states, raw->'Part1A'->>'Q5F2D', source_release_key "
            "FROM sec_adv_feed_firm_state WHERE crd_number='283882'")).one()
        assert row == (Decimal("35557038"), date(2026, 3, 4), ["CA", "NY"], "117", "edition:2026-09-16")
