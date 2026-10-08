"""
Rights review batch 2 (SPEC_142 workflow) — 30 federal datasets, bts_vmt held.

The reviewer of record approved 30 datasets (BEA, IRS SOI, CFTC, Census international
trade, Census ACS/CBP, FCC, BTS border crossings + FAF, USDA NASS, EIA, USAspending,
OpenFEMA, OSHA, EPA ECHO) on research whose every quote an independent verifier re-fetched
live, and HELD bts_vmt (its Socrata id is not a VMT dataset). This file pins the code side:
the 30 carry the approved terms and the verified citations, nothing else changed
(``tests/fixtures/rights_batch_2_before.json`` is every spec's full rights block at
daf24af), and none of the 30 is ``reviewed`` yet: a sign-off is a committed hash in
``app/catalog/rights_reviewed.py``, recorded separately against ``NEW_HASHES``.

Monotonicity: ``open -> attribution`` and ``open -> restricted`` are tightenings; ``None ->
allowed`` is an assessment, and only with a full citation (URL, quote, confidence).
"""
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BEFORE = REPO / "tests" / "fixtures" / "rights_batch_2_before.json"
SPEC142_BASELINE = REPO / "tests" / "fixtures" / "spec_142_rights_baseline.json"

BEA = "https://apps.bea.gov/API/_pdf/bea_api_tos.pdf"
CENSUS = "https://www.census.gov/data/developers/about/terms-of-service.html"
EIA = "https://www.eia.gov/about/copyrights_reuse.php"
FEMA = "https://www.fema.gov/about/openfema/terms-conditions"

APPROVED = {
    "bea_nipa": BEA, "bea_regional": BEA, "bea_gdp_industry": BEA, "bea_international": BEA,
    "irs_soi": "https://www.irs.gov/irm/part1/irm_01-017-008",
    "cftc_cot": "https://www.cftc.gov/WebPolicy/index.htm",
    "us_trade_exports_hs": CENSUS, "us_trade_imports_hs": CENSUS, "us_trade_exports_state": CENSUS,
    "us_trade_port_trade": CENSUS, "us_trade_summary": CENSUS,
    "census_acs5": CENSUS, "census_cbp": CENSUS, "census_acs_county_tract": CENSUS,
    "census_cbp_county_yearly": CENSUS,
    "fcc_broadband": "https://opendata.fcc.gov/api/views/4kuc-phrr.json",
    "bts_border_crossing": "https://data.transportation.gov/api/views/keg4-3bc2.json",
    "bts_faf_regional": "https://geodata.bts.gov/content/539fdab88ee74e38b28959494965ace2",
    "usda_nass": "https://quickstats.nass.usda.gov/api",
    "eia_petroleum": EIA, "eia_natural_gas": EIA, "eia_electricity": EIA, "eia_retail_gas_prices": EIA,
    "eia_steo": EIA,
    "usaspending_awards": "https://www.usaspending.gov/about",
    "fema_disaster_declarations": FEMA, "fema_pa_projects": FEMA, "fema_hma_projects": FEMA,
    "osha": "https://www.dol.gov/general/aboutdol/copyright",
    "epa_echo_facilities": "https://echo.epa.gov/resources/echo-data/about-the-data",
}
HELD = {"bts_vmt"}
# Approved, then held at sign-off (2026-10-04): the OpenFEMA terms add a
# "used solely for statistical research" clause the user had not seen. Their
# blocks are the approved ones; only the sign-off waits for the user.
SIGNOFF_HELD: set = set()  # FEMA x3 signed off 2026-10-04 after the user saw the clause
MEDIUM = {"irs_soi", "eia_steo", "usaspending_awards", "osha", "epa_echo_facilities"}
REDISTRIBUTION = {k: "attribution" for k in APPROVED} | {"irs_soi": "open", "eia_steo": "restricted"}

# rights_hash of each approved block: the lead records the sign-offs against exactly these.
_H_BEA = "75b77724b2eb2d285b8d081738787acda8827abc6984cfe5b095f26bd1167272"
_H_TRADE = "b5cd10667dbfdacf249b1e9eee1e966dbd0ef61b9fbad884a6d580ab61745a4f"
_H_CENSUS = "c2ce7fc3116463f730d85f6ebc3772f0c7149e7d363f938290095cc3e79259a6"
_H_EIA = "aeb82dfe22eed886000b7ab91e35c61187db5e794f771494f8b45393b8eb5072"
_H_FEMA = "12793c1aaf3135b920d2116c28d85f19df456a14ec058e04b1c1d0e96577aae0"
NEW_HASHES = {
    "bea_nipa": _H_BEA, "bea_regional": _H_BEA, "bea_gdp_industry": _H_BEA, "bea_international": _H_BEA,
    "irs_soi": "4b4248c47d94ed98da02fd20e402a97fef61104079fb12c8132046e176e8d366",
    "cftc_cot": "9b51da046d15d8126bef80c09fd59767121b4fa11610a375afa44ffb58182954",
    "us_trade_exports_hs": _H_TRADE, "us_trade_imports_hs": _H_TRADE, "us_trade_exports_state": _H_TRADE,
    "us_trade_port_trade": _H_TRADE, "us_trade_summary": _H_TRADE,
    "census_acs5": _H_CENSUS, "census_cbp": _H_CENSUS, "census_acs_county_tract": _H_CENSUS,
    "census_cbp_county_yearly": _H_CENSUS,
    "fcc_broadband": "b8dadc5968cd531c3188dbbcd41d6112633b46cb31267954429bec25d25f8689",
    "bts_border_crossing": "79de810a070c6278d3d7cd8087866f340f6593f67d87415c2e09b47044a153b0",
    "bts_faf_regional": "2308dc3d93a27b52bd8eb139622cade220de79793b71daec4adee3fce7c8dee1",
    "usda_nass": "25b495336903c2c98eecdda217a32faf2738f4f63c012e02d2cfcf20c5ec9475",
    "eia_petroleum": "1e1d9b0d05225e03f3ccbcbe38606dc8ea94098a3b8e128a8fa9b9c85b4daa7e",
    "eia_natural_gas": _H_EIA, "eia_electricity": _H_EIA, "eia_retail_gas_prices": _H_EIA,
    "eia_steo": "6fc89b0ac63e962f32e27d691c6ab957322d0d4a905679a449a6d1bdf4a67213",
    "usaspending_awards": "34992fdc69062666c908887cc232f0440be2de613d6596df524bc3a4cbc08e04",
    "fema_disaster_declarations": "42ec46dfa8cf2986edaf35b3f30365a1662ec5d1ce41d8bc41492e1135d3e6ef",
    "fema_pa_projects": _H_FEMA, "fema_hma_projects": _H_FEMA,
    "osha": "17064a8ee9b75e1e256499a4880255382e8b41e269efdb0b9263e1f8d2e0cb79",
    "epa_echo_facilities": "0aba1558ff4239712b87bc9cc90374217f039ba7c308fb041c9103556581d7fb",
}
BATCH_1 = {"sec_13f", "sec_companyfacts", "treasury_daily_balance", "treasury_debt_outstanding",
           "treasury_interest_rates", "treasury_monthly_statement", "treasury_auctions", "bls_series"}

FULL_OPENFEMA_NOTICE = ("This product uses the Federal Emergency Management Agency's OpenFEMA API, but is not "
                        "endorsed by FEMA. The Federal Government or FEMA cannot vouch for the data or analyses "
                        "derived from these data after the data have been retrieved from the Agency's website(s).")
CENSUS_NOTICE = "This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau."


def _catalog():
    from app.catalog import get_catalog

    return {s.key: s for s in get_catalog()}


def _before():
    return json.loads(BEFORE.read_text(encoding="utf-8"))["datasets"]


def _now(spec):
    return json.loads(json.dumps(spec.to_dict()["rights"], ensure_ascii=False, default=str))


def test_approved_is_thirty_and_excludes_the_held():
    assert len(APPROVED) == 30 and not HELD & set(APPROVED)
    assert set(NEW_HASHES) == set(APPROVED)


@pytest.mark.unit
class TestApprovedBlocks:
    @pytest.mark.parametrize("key, url", sorted(APPROVED.items()))
    def test_terms_and_verified_citation(self, key, url):
        s = _catalog()[key]
        assert s.storage == "allowed" and s.storage_max_age_days is None, key
        assert s.commercial_use == ("restricted" if key == "eia_steo" else "allowed"), key
        assert s.redistribution == REDISTRIBUTION[key], key
        assert s.citation_url == url and s.citation_quote and s.citation_quote.strip(), key
        assert s.rights_confidence == ("medium" if key in MEDIUM else "high"), key
        assert s.attribution and s.rights_notes, key
        assert s.origin == "official" and not s.share_alike and s.license_url is None, key
        assert s.proposed_rights is None and not s.rights_gate, key
        assert s.status_public not in ("ga", "beta"), key

    @pytest.mark.parametrize("key", sorted(APPROVED))
    def test_signed_off_by_committed_hash(self, key):
        """Reviewed only through the committed hash of exactly this block (27 signed off
        2026-10-04); the three FEMA blocks are approved but their sign-off is held."""
        from app.catalog.rights_reviewed import REVIEWED

        s = _catalog()[key]
        assert s.rights_hash == NEW_HASHES[key], key
        if key in SIGNOFF_HELD:
            assert key not in REVIEWED, key
            assert s.reviewed is False and s.effective_redistribution == "internal_only", key
        else:
            assert REVIEWED[key][0] == s.rights_hash, key
            assert s.reviewed is True and s.effective_redistribution == s.redistribution, key

    def test_required_notices_verbatim(self):
        cat = _catalog()
        for k in ("bea_nipa", "bea_regional", "bea_gdp_industry", "bea_international"):
            assert cat[k].attribution.startswith(
                "This product uses the Bureau of Economic Analysis (BEA) Data API but is not endorsed or "
                "certified by BEA."), k
        for k in ("census_acs5", "census_cbp", "census_acs_county_tract", "census_cbp_county_yearly",
                  "us_trade_exports_hs", "us_trade_imports_hs", "us_trade_exports_state",
                  "us_trade_port_trade", "us_trade_summary"):
            assert cat[k].attribution.endswith(CENSUS_NOTICE), k
        assert cat["usda_nass"].attribution == "This product uses the NASS API but is not endorsed or certified by NASS."
        for k in ("fema_disaster_declarations", "fema_pa_projects", "fema_hma_projects"):
            assert cat[k].attribution == FULL_OPENFEMA_NOTICE, k  # the truncated notice is gone
        for k in ("eia_petroleum", "eia_natural_gas", "eia_electricity", "eia_retail_gas_prices"):
            assert cat[k].attribution == "Source: U.S. Energy Information Administration (<publication/retrieval date>)"
        assert "Short-Term Energy Outlook (<Month YYYY>)" in cat["eia_steo"].attribution
        assert "Dun & Bradstreet" in cat["usaspending_awards"].attribution
        assert "Customs and Border Protection" in cat["bts_border_crossing"].attribution
        assert "Form 477" in cat["fcc_broadband"].attribution

    def test_citation_quotes_are_the_verified_sentences(self):
        cat = _catalog()
        assert "You may not use the BEA name, or the like to imply endorsement" in cat["bea_nipa"].citation_quote
        assert "otherwise 'get' information from Census Bureau data" in cat["us_trade_port_trade"].citation_quote
        assert "identify any individual person, household, business or other entity" in cat["census_cbp"].citation_quote
        assert "not-for-profit, commercial or otherwise." in cat["census_cbp_county_yearly"].citation_quote
        assert "after the data have been retrieved" in cat["fema_disaster_declarations"].citation_quote
        assert "does not include controls over its end use" in cat["fema_pa_projects"].citation_quote
        assert "destroy any copy you may have" in cat["fema_hma_projects"].citation_quote
        assert "requires the written permission of the copyright owners" in cat["eia_steo"].citation_quote
        assert "(Oct 2008)" in cat["eia_electricity"].citation_quote
        assert "Dun & Bradstreet" in cat["usaspending_awards"].citation_quote
        assert "USGOV_WORKS" in cat["fcc_broadband"].citation_quote
        assert "PUBLIC_DOMAIN" in cat["bts_border_crossing"].citation_quote
        assert "unrestricted public use" in cat["bts_faf_regional"].citation_quote
        assert "still claim the source is NASS" in cat["usda_nass"].citation_quote

    def test_conditions_carried_in_notes(self):
        cat = _catalog()
        for k in ("census_acs5", "census_cbp", "us_trade_summary"):
            assert "Re-identification ban" in cat[k].rights_notes, k
        assert "entity/company data" in cat["census_cbp"].rights_notes
        assert "company/entity" in cat["census_cbp_county_yearly"].rights_notes
        assert "households" in cat["census_acs_county_tract"].rights_notes
        for k in ("fema_disaster_declarations", "fema_pa_projects", "fema_hma_projects"):
            n = cat[k].rights_notes
            assert "destroy any copy" in n and "No re-identification" in n, k
        assert "never as BEA figures" in cat["bea_regional"].rights_notes
        assert "not present derived or modified figures as NASS estimates" in cat["usda_nass"].rights_notes
        steo = cat["eia_steo"].rights_notes
        assert "Refinitiv" in steo and "S&P Global" in steo
        fcc = cat["fcc_broadband"].rights_notes
        assert "Form 477" in fcc and "CostQuest Fabric licence does not apply" in fcc
        assert "D&B" in cat["usaspending_awards"].rights_notes
        assert "Refinitiv" in cat["eia_petroleum"].rights_notes


@pytest.mark.unit
class TestNothingElseChanged:
    def test_exactly_the_thirty_changed(self):
        before, cat = _before(), _catalog()
        added_after = {"cms_hospitals"}  # SPEC_162: catalogued on the cms family default
        assert set(cat) == set(before) | added_after
        changed = {k for k, s in cat.items() if k not in added_after and _now(s) != before[k]}
        assert changed == set(APPROVED), sorted(changed ^ set(APPROVED))

    def test_held_bts_vmt_untouched(self):
        before, cat = _before(), _catalog()
        for key in HELD:
            assert _now(cat[key]) == before[key]
            assert (cat[key].storage, cat[key].commercial_use) == (None, None)
        from app.catalog.rights import SOURCE_RIGHTS

        assert (SOURCE_RIGHTS["bts"].storage, SOURCE_RIGHTS["bts"].commercial_use) == (None, None)

    def test_only_terms_and_citations_changed(self):
        before, cat = _before(), _catalog()
        base = json.loads(SPEC142_BASELINE.read_text(encoding="utf-8"))["datasets"]
        for key in APPROVED:
            b, n = before[key], _now(cat[key])
            assert b["storage"] is None and b["commercial_use"] is None, key  # were not assessed
            for f in ("license_url", "share_alike", "storage_max_age_days", "proposed", "gate"):
                assert n[f] == b[f], (key, f)
            assert n["rights_hash"] != b["rights_hash"], key
            assert cat[key].pii_class == base[key]["pii_class"], key

    def test_redistribution_only_tightens(self):
        from app.catalog.mirror import REDISTRIBUTION_RANK

        before, cat = _before(), _catalog()
        for key in APPROVED:
            assert REDISTRIBUTION_RANK.index(cat[key].redistribution) >= \
                REDISTRIBUTION_RANK.index(before[key]["redistribution"]), key

    def test_batch_1_still_reviewed(self):
        from app.catalog.rights_reviewed import REVIEWED

        before, cat = _before(), _catalog()
        signed = BATCH_1 | (set(APPROVED) - SIGNOFF_HELD)
        assert set(REVIEWED) == signed
        assert {k for k, s in cat.items() if s.reviewed} == signed
        for key in BATCH_1:
            assert _now(cat[key]) == before[key], key
            assert REVIEWED[key][0] == cat[key].rights_hash, key

    def test_unapproved_collectors_keep_their_blocks(self):
        """The site_intel EIA / FEMA / FCC collectors share agencies with batch 2 but were not
        in it: their blocks (truncated OpenFEMA notice included) are left for their own review."""
        from app.catalog.rights import COLLECTOR_RIGHTS

        for c in ("eia", "eia_gas", "fema", "fema_nfhl", "fcc", "census_trade", "bts"):
            assert (COLLECTOR_RIGHTS[c].storage, COLLECTOR_RIGHTS[c].commercial_use) == (None, None), c
