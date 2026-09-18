"""
Tests for SPEC 108 — SEC Form D data sets bulk loader (sec_form_d).

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import importlib.util
import os
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

# ---------------------------------------------------------------------------
# Fixtures: real header names (verified against 2023q3 + 2026q2 zips)
# ---------------------------------------------------------------------------

SUB_H = ["ACCESSIONNUMBER", "FILE_NUM", "FILING_DATE", "SIC_CODE", "SCHEMAVERSION", "SUBMISSIONTYPE",
         "TESTORLIVE", "OVER100PERSONSFLAG", "OVER100ISSUERFLAG"]
ISS_H = ["ACCESSIONNUMBER", "IS_PRIMARYISSUER_FLAG", "ISSUER_SEQ_KEY", "CIK", "ENTITYNAME", "STREET1", "STREET2",
         "CITY", "STATEORCOUNTRY", "STATEORCOUNTRYDESCRIPTION", "ZIPCODE", "ISSUERPHONENUMBER", "JURISDICTIONOFINC",
         "ISSUER_PREVIOUSNAME_1", "ISSUER_PREVIOUSNAME_2", "ISSUER_PREVIOUSNAME_3", "EDGAR_PREVIOUSNAME_1",
         "EDGAR_PREVIOUSNAME_2", "EDGAR_PREVIOUSNAME_3", "ENTITYTYPE", "ENTITYTYPEOTHERDESC",
         "YEAROFINC_TIMESPAN_CHOICE", "YEAROFINC_VALUE_ENTERED"]
OFF_H = ["ACCESSIONNUMBER", "INDUSTRYGROUPTYPE", "INVESTMENTFUNDTYPE", "IS40ACT", "REVENUERANGE",
         "AGGREGATENETASSETVALUERANGE", "FEDERALEXEMPTIONS_ITEMS_LIST", "ISAMENDMENT", "PREVIOUSACCESSIONNUMBER",
         "SALE_DATE", "YETTOOCCUR", "MORETHANONEYEAR", "ISEQUITYTYPE", "ISDEBTTYPE", "ISOPTIONTOACQUIRETYPE",
         "ISSECURITYTOBEACQUIREDTYPE", "ISPOOLEDINVESTMENTFUNDTYPE", "ISTENANTINCOMMONTYPE",
         "ISMINERALPROPERTYTYPE", "ISOTHERTYPE", "DESCRIPTIONOFOTHERTYPE", "ISBUSINESSCOMBINATIONTRANS",
         "BUSCOMBCLARIFICATIONOFRESP", "MINIMUMINVESTMENTACCEPTED", "OVER100RECIPIENTFLAG", "TOTALOFFERINGAMOUNT",
         "TOTALAMOUNTSOLD", "TOTALREMAINING", "SALESAMTCLARIFICATIONOFRESP", "HASNONACCREDITEDINVESTORS",
         "NUMBERNONACCREDITEDINVESTORS", "TOTALNUMBERALREADYINVESTED", "SALESCOMM_DOLLARAMOUNT",
         "SALESCOMM_ISESTIMATE", "FINDERSFEE_DOLLARAMOUNT", "FINDERSFEE_ISESTIMATE",
         "FINDERFEECLARIFICATIONOFRESP", "GROSSPROCEEDSUSED_DOLLARAMOUNT", "GROSSPROCEEDSUSED_ISESTIMATE",
         "GROSSPROCEEDSUSED_CLAROFRESP", "AUTHORIZEDREPRESENTATIVE"]
RP_H = ["ACCESSIONNUMBER", "RELATEDPERSON_SEQ_KEY", "FIRSTNAME", "MIDDLENAME", "LASTNAME", "STREET1", "STREET2",
        "CITY", "STATEORCOUNTRY", "STATEORCOUNTRYDESCRIPTION", "ZIPCODE", "RELATIONSHIP_1", "RELATIONSHIP_2",
        "RELATIONSHIP_3", "RELATIONSHIPCLARIFICATION"]
REC_H = ["ACCESSIONNUMBER", "RECIPIENT_SEQ_KEY", "RECIPIENTNAME", "RECIPIENTCRDNUMBER", "ASSOCIATEDBDNAME",
         "ASSOCIATEDBDCRDNUMBER", "STREET1", "STREET2", "CITY", "STATEORCOUNTRY", "STATEORCOUNTRYDESCRIPTION",
         "ZIPCODE", "STATES_OR_VALUE_LIST", "DESCRIPTIONS_LIST", "FOREIGNSOLICITATION"]
SIG_H = ["ACCESSIONNUMBER", "SIGNATURE_SEQ_KEY", "ISSUERNAME", "SIGNATURENAME", "NAMEOFSIGNER", "SIGNATURETITLE",
         "SIGNATUREDATE"]

A1 = "0000950142-26-001928"  # fund, two issuers, two related persons
A2 = "0002109930-26-000002"  # operating co, amounts, recipients
A3 = "0000000000-26-999999"  # TEST filing -> skipped everywhere


def _tsv(header, rows):
    lines = ["\t".join(header)]
    for r in rows:
        lines.append("\t".join(r.get(h, "") for h in header))
    return "\n".join(lines) + "\n"


def build_zip(path: Path, prefix="2026Q2_d") -> Path:
    subs = [
        {"ACCESSIONNUMBER": A1, "FILE_NUM": "021-589209               ", "FILING_DATE": "30-JUN-2026",
         "SCHEMAVERSION": "X0708", "SUBMISSIONTYPE": "D", "TESTORLIVE": "LIVE"},
        {"ACCESSIONNUMBER": A2, "FILE_NUM": "021-589208               ", "FILING_DATE": "29-JUN-2026",
         "SCHEMAVERSION": "X0708", "SUBMISSIONTYPE": "D/A", "TESTORLIVE": "LIVE"},
        {"ACCESSIONNUMBER": A3, "FILING_DATE": "29-JUN-2026", "SUBMISSIONTYPE": "D", "TESTORLIVE": "TEST"},
    ]
    issuers = [
        {"ACCESSIONNUMBER": A1, "IS_PRIMARYISSUER_FLAG": "YES", "ISSUER_SEQ_KEY": "101", "CIK": "0002138429",
         "ENTITYNAME": "AP Emma Co-Invest, L.P.", "STREET1": "9 WEST 57TH STREET", "STREET2": "41ST FLOOR",
         "CITY": "NEW YORK", "STATEORCOUNTRY": "NY", "STATEORCOUNTRYDESCRIPTION": "NEW YORK", "ZIPCODE": "10019",
         "ISSUERPHONENUMBER": "212-515-3200", "JURISDICTIONOFINC": "DELAWARE",
         "ENTITYTYPE": "Limited Partnership", "YEAROFINC_TIMESPAN_CHOICE": "withinFiveYears",
         "YEAROFINC_VALUE_ENTERED": "2026", "EDGAR_PREVIOUSNAME_1": "Old Emma LP"},
        {"ACCESSIONNUMBER": A1, "IS_PRIMARYISSUER_FLAG": "NO", "ISSUER_SEQ_KEY": "102", "CIK": "0002138430",
         "ENTITYNAME": "AP Emma Parallel, L.P.", "STREET1": "9 WEST 57TH STREET", "CITY": "NEW YORK",
         "STATEORCOUNTRY": "NY", "ENTITYTYPE": "Other", "ENTITYTYPEOTHERDESC": "Cayman Islands Exempted Company",
         "YEAROFINC_TIMESPAN_CHOICE": "overFiveYears"},
        {"ACCESSIONNUMBER": A2, "IS_PRIMARYISSUER_FLAG": "YES", "ISSUER_SEQ_KEY": "101", "CIK": "0002109930",
         "ENTITYNAME": "SignalCraft\xa0Analytics  Group Inc.", "STREET1": "175 GREENWICH ST", "CITY": "NEW YORK",
         "STATEORCOUNTRY": "NY", "ZIPCODE": "10014", "ISSUERPHONENUMBER": "18439819752",
         "JURISDICTIONOFINC": "NEW YORK", "ENTITYTYPE": "Corporation",
         "YEAROFINC_TIMESPAN_CHOICE": "withinFiveYears", "YEAROFINC_VALUE_ENTERED": "2026"},
        {"ACCESSIONNUMBER": A3, "IS_PRIMARYISSUER_FLAG": "YES", "ISSUER_SEQ_KEY": "101", "CIK": "0000000001",
         "ENTITYNAME": "Test Co"},
    ]
    offerings = [
        {"ACCESSIONNUMBER": A1, "INDUSTRYGROUPTYPE": "Pooled Investment Fund",
         "INVESTMENTFUNDTYPE": "Private Equity Fund", "IS40ACT": "false", "REVENUERANGE": "Decline to Disclose",
         "FEDERALEXEMPTIONS_ITEMS_LIST": "06b, 3C, 3C.7", "ISAMENDMENT": "false", "YETTOOCCUR": "true",
         "MORETHANONEYEAR": "true", "ISPOOLEDINVESTMENTFUNDTYPE": "true", "ISBUSINESSCOMBINATIONTRANS": "false",
         "MINIMUMINVESTMENTACCEPTED": "1", "TOTALOFFERINGAMOUNT": "Indefinite", "TOTALAMOUNTSOLD": "0",
         "TOTALREMAINING": "Indefinite", "HASNONACCREDITEDINVESTORS": "false",
         "TOTALNUMBERALREADYINVESTED": "0"},
        {"ACCESSIONNUMBER": A2, "INDUSTRYGROUPTYPE": "Investing", "REVENUERANGE": "Over\xa0$100,000,000",
         "FEDERALEXEMPTIONS_ITEMS_LIST": "06c", "ISAMENDMENT": "true",
         "PREVIOUSACCESSIONNUMBER": "0002109930-26-000001", "SALE_DATE": "2026-05-31",
         "MORETHANONEYEAR": "false", "ISEQUITYTYPE": "true", "ISDEBTTYPE": "true",
         "ISOPTIONTOACQUIRETYPE": "true", "ISSECURITYTOBEACQUIREDTYPE": "true",
         "ISBUSINESSCOMBINATIONTRANS": "false", "MINIMUMINVESTMENTACCEPTED": "1000",
         "TOTALOFFERINGAMOUNT": "100000000", "TOTALAMOUNTSOLD": "788172352.00", "TOTALREMAINING": "0",
         "HASNONACCREDITEDINVESTORS": "true", "NUMBERNONACCREDITEDINVESTORS": "35",
         "TOTALNUMBERALREADYINVESTED": "1000", "SALESCOMM_DOLLARAMOUNT": "1372377",
         "SALESCOMM_ISESTIMATE": "true", "FINDERSFEE_DOLLARAMOUNT": "2200.50",
         "GROSSPROCEEDSUSED_DOLLARAMOUNT": "450000", "AGGREGATENETASSETVALUERANGE": "Not Applicable",
         "IS40ACT": "", "ISOTHERTYPE": "true", "DESCRIPTIONOFOTHERTYPE": "Security Token Offering (STO)"},
        {"ACCESSIONNUMBER": A3, "INDUSTRYGROUPTYPE": "Other", "TOTALOFFERINGAMOUNT": "5"},
    ]
    related = [
        {"ACCESSIONNUMBER": A1, "RELATEDPERSON_SEQ_KEY": "105", "FIRSTNAME": "Stephanie", "LASTNAME": "Drescher",
         "STREET1": "9 West 57th Street", "STREET2": "41st Floor", "CITY": "New York", "STATEORCOUNTRY": "NY",
         "STATEORCOUNTRYDESCRIPTION": "NEW YORK", "ZIPCODE": "10019", "RELATIONSHIP_1": "Executive Officer",
         "RELATIONSHIP_2": "Director",
         "RELATIONSHIPCLARIFICATION": "Executive Officer of the GP of the Manager of the GP of the Issuer"},
        {"ACCESSIONNUMBER": A1, "RELATEDPERSON_SEQ_KEY": "103", "FIRSTNAME": "Brian", "MIDDLENAME": "B.",
         "LASTNAME": "Carney", "RELATIONSHIP_1": "Executive Officer"},
        {"ACCESSIONNUMBER": A2, "RELATEDPERSON_SEQ_KEY": "101", "FIRSTNAME": "Kevin", "LASTNAME": "Morgan",
         "RELATIONSHIP_1": "Director", "RELATIONSHIPCLARIFICATION": 'Says "hi", with comma'},
        {"ACCESSIONNUMBER": A3, "RELATEDPERSON_SEQ_KEY": "101", "FIRSTNAME": "T", "LASTNAME": "T"},
    ]
    recipients = [
        {"ACCESSIONNUMBER": A2, "RECIPIENT_SEQ_KEY": "101", "RECIPIENTNAME": "SignalCraft Analytics Group Inc.",
         "RECIPIENTCRDNUMBER": "None", "ASSOCIATEDBDNAME": "None", "ASSOCIATEDBDCRDNUMBER": "None",
         "STATES_OR_VALUE_LIST": "All States", "DESCRIPTIONS_LIST": "All States", "FOREIGNSOLICITATION": "true"},
        {"ACCESSIONNUMBER": A2, "RECIPIENT_SEQ_KEY": "303", "RECIPIENTNAME": "Morgan Stanley Smith Barney LLC",
         "RECIPIENTCRDNUMBER": "149777", "STATES_OR_VALUE_LIST": "CA, NY", "FOREIGNSOLICITATION": "false"},
    ]
    sigs = [
        {"ACCESSIONNUMBER": A1, "SIGNATURE_SEQ_KEY": "101", "ISSUERNAME": "AP Emma Co-Invest, L.P.",
         "SIGNATURENAME": "/s/ James Elworth", "NAMEOFSIGNER": "James Elworth",
         "SIGNATURETITLE": "Executive Officer", "SIGNATUREDATE": "2026-06-30"},
        {"ACCESSIONNUMBER": A1, "SIGNATURE_SEQ_KEY": "102", "ISSUERNAME": "AP Emma Parallel, L.P.",
         "SIGNATURENAME": "/s/ James Elworth", "NAMEOFSIGNER": "James Elworth",
         "SIGNATURETITLE": "Executive Officer", "SIGNATUREDATE": "2026-06-30"},
        {"ACCESSIONNUMBER": A2, "SIGNATURE_SEQ_KEY": "101", "ISSUERNAME": "SignalCraft Analytics Group Inc.",
         "SIGNATURENAME": "Kevin Morgan", "NAMEOFSIGNER": "Kevin Morgan", "SIGNATURETITLE": "Director",
         "SIGNATUREDATE": "2026-06-30"},
        {"ACCESSIONNUMBER": A3, "SIGNATURE_SEQ_KEY": "101", "NAMEOFSIGNER": "Test"},
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{prefix}/FORMDSUBMISSION.tsv", _tsv(SUB_H, subs))
        zf.writestr(f"{prefix}/ISSUERS.tsv", _tsv(ISS_H, issuers))
        zf.writestr(f"{prefix}/OFFERING.tsv", _tsv(OFF_H, offerings))
        zf.writestr(f"{prefix}/RELATEDPERSONS.tsv", _tsv(RP_H, related))
        zf.writestr(f"{prefix}/RECIPIENTS.tsv", _tsv(REC_H, recipients))
        zf.writestr(f"{prefix}/SIGNATURES.tsv", _tsv(SIG_H, sigs))
        zf.writestr(f"{prefix}/FormD_readme.html", "<html></html>")
    return path


def _index_html(quarters):
    """Mimic the real page: new path for 2026q2+, old path before, some `_0` suffixes."""
    parts = ["<html><body>"]
    for y, q in quarters:
        if (y, q) >= (2026, 2):
            href = f"/files/datastandardsinnovation/data/form-d-data-sets/{y}q{q}_d.zip"
        elif y <= 2013:
            href = f"/files/structureddata/data/form-d-data-sets/{y}q{q}_d_0.zip"
        else:
            href = f"https://www.sec.gov/files/structureddata/data/form-d-data-sets/{y}q{q}_d.zip"
        parts.append(f'<a href="{href}">{y} Q{q}</a>')
    parts.append('<a href="https://www.sec.gov/files/Form_D.pdf">pdf</a></body></html>')
    return "\n".join(parts)


ALL_QUARTERS = [(y, q) for y in range(2012, 2027) for q in (1, 2, 3, 4) if (y, q) <= (2026, 2)]


class _FakeHttp:
    def __init__(self, html):
        self.html = html
        self.urls = []

    def get_text(self, url):
        self.urls.append(url)
        return self.html


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDiscover:
    def test_discover_parses_hrefs_and_window(self):
        """T1"""
        from app.ingest.bulk.sec_form_d.source import INDEX_URL, SecFormDDataSets

        html = _index_html(list(reversed(ALL_QUARTERS)) + [(2025, 1)])  # newest first + duplicate
        http = _FakeHttp(html)
        rels = SecFormDDataSets().discover(http, None)
        assert http.urls == [INDEX_URL]
        keys = [r.release_key for r in rels]
        assert len(keys) == 12
        assert keys[0] == "2023q3" and keys[-1] == "2026q2"
        assert keys == sorted(keys)
        by_key = {r.release_key: r for r in rels}
        assert by_key["2026q2"].url == \
            "https://www.sec.gov/files/datastandardsinnovation/data/form-d-data-sets/2026q2_d.zip"
        assert by_key["2025q1"].url == \
            "https://www.sec.gov/files/structureddata/data/form-d-data-sets/2025q1_d.zip"
        assert by_key["2026q1"].meta == {"year": 2026, "quarter": 1, "quarter_end": "2026-03-31"}

        # _0 suffix on old files is understood
        old = SecFormDDataSets().discover(_FakeHttp(html), date(2012, 1, 1))
        assert old[0].release_key == "2012q1"
        assert old[0].url.endswith("/2012q1_d_0.zip") and old[0].url.startswith("https://www.sec.gov/")

    def test_discover_since_filter(self):
        """T2"""
        from app.ingest.bulk.sec_form_d.source import SecFormDDataSets

        http = _FakeHttp(_index_html(ALL_QUARTERS))
        keys = [r.release_key for r in SecFormDDataSets().discover(http, date(2025, 3, 31))]
        assert keys == ["2025q1", "2025q2", "2025q3", "2025q4", "2026q1", "2026q2"]
        keys = [r.release_key for r in SecFormDDataSets().discover(http, date(2025, 4, 1))]
        assert keys[0] == "2025q2"
        # since reaches back beyond the default window -> no 12-quarter cap
        assert len(SecFormDDataSets().discover(http, date(2020, 1, 1))) == 26

    def test_discover_raises_when_no_links(self):
        """T3"""
        from app.ingest.bulk.sec_form_d.source import SecFormDDataSets

        with pytest.raises(RuntimeError):
            SecFormDDataSets().discover(_FakeHttp("<html>maintenance</html>"), None)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_value_parsers():
    """T4"""
    from app.ingest.bulk.sec_form_d import parse as p

    assert p.clean("  a\xa0 b ") == "a b"
    assert p.clean("") is None and p.clean(None) is None and p.clean("   ") is None
    assert p.parse_amount("788172352.00") == 788172352
    assert p.parse_amount("$1,000") == 1000
    from decimal import Decimal
    assert p.parse_numeric("2200.50") == Decimal("2200.50")
    assert p.parse_numeric("99999999999999999999") == Decimal("99999999999999999999")
    assert p.parse_numeric("Indefinite") is None and p.parse_numeric("") is None
    assert p.is_indefinite("Indefinite") is True and p.is_indefinite("0") is False
    assert p.parse_amount("Indefinite") is None
    assert p.parse_amount("") is None
    assert p.parse_amount("99999999999999999999") is None  # > BIGINT
    assert p.parse_int("35") == 35 and p.parse_int("") is None and p.parse_int("x") is None
    assert p.parse_int("9999999999") is None  # > INT
    assert p.parse_date("30-JUN-2026") == date(2026, 6, 30)
    assert p.parse_date("2026-05-31") == date(2026, 5, 31)
    assert p.parse_date("31-FEB-2026") is None and p.parse_date("") is None
    assert p.parse_bool("true") is True and p.parse_bool("FALSE") is False and p.parse_bool("") is None
    assert p.split_list("06b, 3C, 3C.7") == ["06b", "3C", "3C.7"]
    assert p.split_list("") is None
    assert p.exemption_labels("06b, 3C, 3C.7, 99") == ["Rule 506(b)", "Section 3(c)", "Section 3(c)(7)", "Rule 99"]


@pytest.mark.unit
def test_parse_zip_real_headers(tmp_path):
    """T5"""
    from app.ingest.bulk.sec_form_d import parse as p

    z = build_zip(tmp_path / "2026q2_d.zip")

    subs = [dict(zip(p.SUBMISSION_COLS, r)) for r in p.iter_submissions(z)]
    assert [s["accession_number"] for s in subs] == [A1, A2]
    assert subs[0]["file_num"] == "021-589209"  # right-padded in the real file
    assert subs[0]["filed_at"] == date(2026, 6, 30)
    assert subs[1]["submission_type"] == "D/A"

    test_accs = p.non_live_accessions(z)
    assert test_accs == {A3}

    iss = [dict(zip(p.ISSUER_COLS, r)) for r in p.iter_issuers(z, skip=test_accs)]
    assert len(iss) == 3
    a1 = [i for i in iss if i["accession_number"] == A1]
    assert [(i["issuer_seq"], i["is_primary"]) for i in a1] == [(101, True), (102, False)]
    assert a1[0]["cik"] == "0002138429"
    assert a1[0]["year_of_inc_value"] == 2026
    assert a1[0]["edgar_previous_names"] == ["Old Emma LP"]
    assert a1[0]["issuer_previous_names"] is None
    assert a1[1]["zip_code"] is None and a1[1]["year_of_inc_value"] is None
    assert a1[1]["entity_type_other_desc"] == "Cayman Islands Exempted Company"
    a2 = [i for i in iss if i["accession_number"] == A2][0]
    assert a2["entity_name"] == "SignalCraft Analytics Group Inc."

    offs = [dict(zip(p.OFFERING_COLS, r)) for r in p.iter_offerings(z, skip=test_accs)]
    assert len(offs) == 2
    o1, o2 = offs
    assert o1["total_offering_amount"] is None and o1["is_indefinite"] is True
    assert o1["total_remaining"] is None
    assert o1["total_amount_sold"] == 0
    assert o1["federal_exemptions_items_list"] == ["06b", "3C", "3C.7"]
    assert o1["federal_exemptions"] == ["Rule 506(b)", "Section 3(c)", "Section 3(c)(7)"]
    assert o1["investment_fund_type"] == "Private Equity Fund" and o1["is_40_act"] is False
    assert o1["is_equity"] is None and o1["is_pooled_investment_fund"] is True
    assert o1["date_of_first_sale"] is None and o1["yet_to_occur"] is True
    assert o1["duration_of_offering_more_than_one_year"] is True
    assert o1["is_amendment"] is False and o1["previous_accession_number"] is None
    assert o1["sales_commission_amount"] is None and o1["minimum_investment_accepted"] == 1
    assert o2["is_indefinite"] is False and o2["total_offering_amount"] == 100000000
    assert o2["investment_fund_type"] is None and o2["is_40_act"] is None
    assert o2["is_amendment"] is True and o2["previous_accession_number"] == "0002109930-26-000001"
    assert o2["sales_commission_amount"] == 1372377 and str(o2["finders_fees_amount"]) == "2200.50"
    assert o2["gross_proceeds_used_amount"] == 450000
    assert o2["has_non_accredited_investors"] is True
    assert o2["aggregate_net_asset_value_range"] == "Not Applicable"
    assert o2["total_amount_sold"] == 788172352
    assert o2["revenue_range"] == "Over $100,000,000"
    assert o2["date_of_first_sale"] == date(2026, 5, 31)
    assert o2["non_accredited_investors"] == 35 and o2["total_number_already_invested"] == 1000

    rps = [dict(zip(p.RELATED_PERSON_COLS, r)) for r in p.iter_related_persons(z, skip=test_accs)]
    assert len(rps) == 3
    assert rps[0]["related_person_seq"] == 105
    assert rps[0]["relationships"] == ["Executive Officer", "Director"]
    assert rps[1]["middle_name"] == "B." and rps[1]["street1"] is None
    assert rps[2]["relationship_clarification"] == 'Says "hi", with comma'

    sigs = [dict(zip(p.SIGNATURE_COLS, r)) for r in p.iter_signatures(z, skip=test_accs)]
    assert len(sigs) == 3
    assert sigs[0]["signature_seq"] == 101 and sigs[0]["signature_date"] == date(2026, 6, 30)
    assert sigs[0]["signature_name"] == "/s/ James Elworth" and sigs[0]["signature_title"] == "Executive Officer"

    recs = [dict(zip(p.RECIPIENT_COLS, r)) for r in p.iter_recipients(z, skip=test_accs)]
    assert len(recs) == 2
    assert recs[0]["crd_number"] is None  # literal 'None'
    assert recs[1]["states"] == ["CA", "NY"]


@pytest.mark.unit
def test_registered():
    """T6"""
    from app.ingest.bulk.registry import get_source

    src = get_source("sec_form_d")
    assert src.name == "sec_form_d"
    ddl = "\n".join(src.ddl())
    assert "CREATE TABLE IF NOT EXISTS form_d_filings" in ddl
    assert "accession_number VARCHAR(25) NOT NULL UNIQUE" in ddl
    assert "form_d_issuers" in ddl and "form_d_related_persons" in ddl
    assert "form_d_offerings" in ddl and "form_d_signatures" in ddl
    assert "(investment_fund_type)" in ddl and "(date_of_first_sale)" in ddl


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for t in ("form_d_filings", "form_d_issuers", "form_d_related_persons", "form_d_offerings",
                  "form_d_signatures"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
        conn.execute(text("DROP TABLE IF EXISTS raw.source_release"))
        mig = REPO / "alembic" / "versions" / "0004_bulk_framework.py"
        spec = importlib.util.spec_from_file_location("mig0004", mig)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for stmt in mod.SOURCE_RELEASE_DDL:
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


@pg
def test_load_idempotent_pg(pg_engine, tmp_path):
    """T7"""
    import json

    from sqlalchemy import text
    from app.ingest.bulk.base import Release
    from app.ingest.bulk.sec_form_d.source import SecFormDDataSets

    z = build_zip(tmp_path / "2026q2_d.zip")
    src = SecFormDDataSets()
    rel = Release("2026q2", "https://www.sec.gov/x/2026q2_d.zip")

    with pg_engine.begin() as conn:
        rows = src.load(conn, rel, z)
    assert rows == {"form_d_filings": 2, "form_d_issuers": 3, "form_d_related_persons": 3,
                    "form_d_offerings": 2, "form_d_signatures": 3}
    assert src.last_stats["form_d_filings"] == (2, 0)

    with pg_engine.connect() as conn:
        f = conn.execute(text("SELECT * FROM form_d_filings WHERE accession_number = :a"), {"a": A2}).mappings().one()
        assert f["cik"] == "0002109930"
        assert f["submission_type"] == "D/A"
        assert f["filed_at"] == datetime(2026, 6, 29)
        assert f["issuer_name"] == "SignalCraft Analytics Group Inc."
        assert f["issuer_street"] == "175 GREENWICH ST" and f["issuer_zip"] == "10014"
        assert f["year_of_incorporation"] == 2026
        assert f["industry_group"] == "Investing"
        assert f["revenue_range"] == "Over $100,000,000"
        assert f["federal_exemptions"] == ["Rule 506(c)"]
        assert f["date_of_first_sale"] == date(2026, 5, 31)
        assert f["is_equity"] is True and f["is_pooled_investment_fund"] is False
        assert f["total_offering_amount"] == 100000000 and f["total_amount_sold"] == 788172352
        assert f["non_accredited_investors"] == 35 and f["accredited_investors"] is None
        assert f["related_persons"] == [{"first_name": "Kevin", "last_name": "Morgan", "relationship": ["Director"]}]
        assert f["sales_compensation"] == [
            {"name": "SignalCraft Analytics Group Inc.", "crd_number": None, "states": ["All States"]},
            {"name": "Morgan Stanley Smith Barney LLC", "crd_number": "149777", "states": ["CA", "NY"]},
        ]
        f1 = conn.execute(text("SELECT * FROM form_d_filings WHERE accession_number = :a"), {"a": A1}).mappings().one()
        assert f1["issuer_name"] == "AP Emma Co-Invest, L.P."  # primary issuer, not seq 102
        assert f1["total_offering_amount"] is None and f1["is_equity"] is False
        assert [p["last_name"] for p in f1["related_persons"]] == ["Carney", "Drescher"]  # ordered by seq
        assert f1["sales_compensation"] == []
        assert conn.execute(text("SELECT COUNT(*) FROM form_d_filings WHERE accession_number = :a"),
                            {"a": A3}).scalar() == 0

        i = conn.execute(text("SELECT * FROM form_d_issuers WHERE accession_number=:a AND issuer_seq=102"),
                         {"a": A1}).mappings().one()
        assert i["is_primary"] is False and i["source_release_key"] == "2026q2" and i["loaded_at"] is not None
        r = conn.execute(text("SELECT * FROM form_d_related_persons WHERE accession_number=:a AND related_person_seq=105"),
                         {"a": A1}).mappings().one()
        assert r["relationships"] == ["Executive Officer", "Director"]
        assert r["zip_code"] == "10019" and r["middle_name"] is None
        o = conn.execute(text("SELECT * FROM form_d_offerings WHERE accession_number=:a"), {"a": A1}).mappings().one()
        assert o["file_num"] == "021-589209" and o["investment_fund_type"] == "Private Equity Fund"
        assert o["total_offering_amount"] is None and o["is_indefinite"] is True
        assert o["federal_exemptions_items_list"] == ["06b", "3C", "3C.7"]
        assert o["yet_to_occur"] is True and o["duration_of_offering_more_than_one_year"] is True
        assert o["source_release_key"] == "2026q2" and o["loaded_at"] is not None
        o2 = conn.execute(text("SELECT * FROM form_d_offerings WHERE accession_number=:a"), {"a": A2}).mappings().one()
        assert o2["total_offering_amount"] == 100000000 and o2["is_indefinite"] is False
        assert o2["total_amount_sold"] == 788172352 and str(o2["finders_fees_amount"]) == "2200.50"
        assert o2["sales_commission_amount"] == 1372377 and o2["gross_proceeds_used_amount"] == 450000
        assert o2["is_amendment"] is True and o2["previous_accession_number"] == "0002109930-26-000001"
        assert o2["date_of_first_sale"] == date(2026, 5, 31) and o2["has_non_accredited_investors"] is True
        assert o2["minimum_investment_accepted"] == 1000 and o2["total_number_already_invested"] == 1000
        sg = conn.execute(text("SELECT * FROM form_d_signatures WHERE accession_number=:a AND signature_seq=102"),
                          {"a": A1}).mappings().one()
        assert sg["issuer_name"] == "AP Emma Parallel, L.P." and sg["signature_date"] == date(2026, 6, 30)
        assert conn.execute(text("SELECT COUNT(*) FROM form_d_signatures WHERE accession_number=:a"),
                            {"a": A3}).scalar() == 0
        created = conn.execute(text("SELECT created_at FROM form_d_filings WHERE accession_number=:a"),
                               {"a": A1}).scalar()
        assert conn.execute(text("SELECT to_regclass('stg.sec_form_d_filings')")).scalar() is None

    # second load: idempotent, and unchanged rows are not rewritten (SPEC_115)
    with pg_engine.begin() as conn:
        rows2 = src.load(conn, rel, z)
    assert set(rows2) == set(rows)
    assert all(v == 0 for v in rows2.values())
    assert all(ins == 0 for ins, _ in src.last_stats.values())
    with pg_engine.connect() as conn:
        counts = [conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                  for t in ("form_d_filings", "form_d_issuers", "form_d_related_persons",
                            "form_d_offerings", "form_d_signatures")]
        assert counts == [2, 3, 3, 2, 3]
        assert conn.execute(text("SELECT created_at FROM form_d_filings WHERE accession_number=:a"),
                            {"a": A1}).scalar() == created
        rp = conn.execute(text("SELECT related_persons FROM form_d_filings WHERE accession_number=:a"),
                          {"a": A1}).scalar()
        assert len(rp) == 2 and json.dumps(rp)


@pg
def test_run_source_flow_pg(pg_engine, tmp_path):
    """T8"""
    from sqlalchemy import text
    from app.core.sec_http import SecHttp
    from app.ingest.bulk.base import run_source
    from app.ingest.bulk.sec_form_d.source import SecFormDDataSets

    zip_bytes = build_zip(tmp_path / "fixture.zip").read_bytes()
    html = _index_html([(2026, 2), (2026, 1)])
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if str(request.url).endswith(".zip"):
            return httpx.Response(200, content=zip_bytes, headers={"Content-Type": "application/zip"})
        return httpx.Response(200, text=html, headers={"Content-Type": "text/html"})

    http = SecHttp(user_agent="Test agent test@example.com", rps=1000.0, transport=httpx.MockTransport(handler))
    src = SecFormDDataSets()
    raw = tmp_path / "raw"
    r1 = run_source(src, engine=pg_engine, http=http, raw_root=raw, release_keys=["2026q2"])
    assert r1["loaded"] == 1 and r1["failed"] == 0, r1
    assert (raw / "sec_form_d" / "2026q2" / "2026q2_d.zip").exists()
    with pg_engine.connect() as conn:
        st, rl = conn.execute(text("SELECT status, rows_loaded FROM raw.source_release "
                                   "WHERE source='sec_form_d' AND release_key='2026q2'")).one()
        assert st == "loaded" and rl["form_d_filings"] == 2
        assert conn.execute(text("SELECT COUNT(*) FROM form_d_filings")).scalar() == 2

    n_zip = sum(1 for c in calls if c.endswith(".zip"))
    r2 = run_source(src, engine=pg_engine, http=http, raw_root=raw, release_keys=["2026q2"])
    assert r2["skipped"] == 1 and r2["loaded"] == 0
    assert sum(1 for c in calls if c.endswith(".zip")) == n_zip
    http.close()
