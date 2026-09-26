"""
Rights, PII and origin per source family (SPEC_123).

Written by hand, deliberately: nothing here is inferred. Every ``source`` a
DatasetSpec names must have an entry — a missing one is a test failure, not
a silent ``open``.

Rules applied:

- US federal government works (17 U.S.C. §105) are ``open`` — but
  ``reviewed=False``, so ``effective_redistribution`` stays ``internal_only``
  until a human signs off on the specific dataset (some government portals
  republish third-party data).
- Open-licence data that needs a credit line (CC BY 4.0: World Bank, OECD,
  Data Commons, PatentsView, Epoch AI) is ``attribution``.
- Commercial APIs whose terms forbid storage or redistribution (Yelp, Google
  Places, Foursquare / SafeGraph / Placer, OpenCorporates, SimilarWeb,
  Kaggle competition data, GitHub, Glassdoor, app stores, prediction
  markets, S&P DUNL) are ``restricted``.
- Scraped or LLM-extracted collections are ``internal_only``: the underlying
  pages are someone else's content.
- Nexdata-derived marts are ``internal_only`` until the commercial posture
  (PLAN_087 deferred SPEC_130) is decided.
- The site_intel family default is ``internal_only`` (SPEC_141): a new
  collector must declare its own ``COLLECTOR_RIGHTS`` entry before it can be
  anything more permissive; it never silently inherits ``open``.

SPEC_141 applied the verified PII corrections (PLAN_088 §1.6) and the cited
tightenings (HIFLD, the site_intel default); loosenings stay proposals for
SPEC_142.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

USG = "US Government work (17 U.S.C. §105), public domain"
CC_BY_4 = "CC BY 4.0"


@dataclass(frozen=True)
class SourceRights:
    license: str
    redistribution: str        # internal_only | attribution | open | restricted
    pii_class: str             # none | business_contact | personal
    origin: str                # official | derived | llm_extracted | synthetic | scraped
    attribution: Optional[str] = None
    reviewed: bool = False
    notes: Optional[str] = None


def _usg(agency: str, pii: str = "none", notes: Optional[str] = None) -> SourceRights:
    return SourceRights(USG, "open", pii, "official", attribution=f"Source: {agency}", notes=notes)


SOURCE_RIGHTS: Dict[str, SourceRights] = {
    # ── US government (public domain; still unreviewed) ─────────────────
    "sec": _usg("U.S. Securities and Exchange Commission (EDGAR)", "business_contact",
                "Filings name natural persons (insiders, related persons, signatories); "
                "per-dataset pii_class overrides apply."),
    "treasury": _usg("U.S. Department of the Treasury, Fiscal Data"),
    "usaspending": _usg("USAspending.gov", "business_contact"),
    "eia": _usg("U.S. Energy Information Administration"),
    "noaa": _usg("NOAA National Centers for Environmental Information"),
    "afdc": _usg("U.S. DOE Alternative Fuels Data Center (NREL)", "business_contact"),
    "bls": _usg("U.S. Bureau of Labor Statistics"),
    "bea": _usg("U.S. Bureau of Economic Analysis"),
    "fema": _usg("FEMA (OpenFEMA)"),
    "fdic": _usg("Federal Deposit Insurance Corporation"),
    "cms": _usg("Centers for Medicare & Medicaid Services"),
    "nppes": _usg("CMS NPPES NPI Registry", "personal",
                  "Individual providers are natural persons (name, practice address)."),
    "fbi_crime": _usg("FBI Crime Data Explorer"),
    "irs_soi": _usg("IRS Statistics of Income"),
    "fcc_broadband": _usg("Federal Communications Commission"),
    "us_trade": _usg("U.S. Census Bureau, international trade"),
    "bts": _usg("Bureau of Transportation Statistics"),
    "cftc_cot": _usg("U.S. Commodity Futures Trading Commission"),
    "usda": _usg("USDA National Agricultural Statistics Service"),
    "fda": _usg("U.S. Food and Drug Administration (openFDA)", "business_contact"),
    "sam_gov": _usg("GSA SAM.gov", "business_contact",
                    "Entity registrations include points of contact."),
    "osha": _usg("U.S. Department of Labor, OSHA"),
    "census": _usg("U.S. Census Bureau"),
    "epa_echo": _usg("U.S. EPA ECHO"),
    # ── open licence, attribution required ──────────────────────────────
    "uspto": SourceRights(CC_BY_4, "attribution", "personal", "official",
                          attribution="Source: USPTO PatentsView",
                          notes="Inventor names are natural persons."),
    "international_econ": SourceRights(
        "CC BY 4.0 (World Bank, OECD); IMF and BIS terms of use", "attribution", "none", "official",
        attribution="Sources: World Bank WDI, IMF, OECD, BIS",
        notes="IMF/BIS terms permit reuse with attribution; per-dataset overrides apply."),
    "data_commons": SourceRights(
        "CC BY 4.0 (Data Commons); underlying sources carry their own terms", "attribution",
        "none", "official", attribution="Source: Data Commons (datacommons.org)"),
    "courtlistener": SourceRights(
        "Court records (public); CourtListener terms request attribution", "attribution",
        "personal", "official", attribution="Source: CourtListener, Free Law Project",
        notes="Bankruptcy dockets name individual debtors."),
    "realestate": SourceRights(
        "Mixed: FHFA/HUD public domain; Redfin Data Center and OpenStreetMap (ODbL) terms",
        "restricted", "none", "official",
        notes="Family default is the most restrictive member; per-dataset overrides apply."),
    # ── restricted commercial / vendor terms ────────────────────────────
    "fred": SourceRights(
        "FRED terms of use; many series are third-party copyrighted", "restricted", "none",
        "official", attribution="Source: Federal Reserve Bank of St. Louis (FRED)",
        notes="fred/client.py warns some series are copyrighted by their owners; "
              "redistribution needs per-series clearance."),
    "yelp": SourceRights("Yelp Fusion API terms (no storage / redistribution)", "restricted",
                         "business_contact", "official"),
    "kaggle": SourceRights("Kaggle competition rules (M5: non-commercial, competition use)",
                           "restricted", "none", "official"),
    "foot_traffic": SourceRights(
        "Vendor terms: Google Places, Foursquare, SafeGraph, Placer.ai", "restricted",
        "business_contact", "official"),
    "prediction_markets": SourceRights("Kalshi / Polymarket API terms", "restricted", "none", "official"),
    "dunl": SourceRights("S&P Global DUNL licence terms", "restricted", "none", "official"),
    "github": SourceRights("GitHub API terms of service", "restricted", "personal", "official",
                           notes="Contributor logins are personal data."),
    "glassdoor": SourceRights("Glassdoor terms of use (scraped)", "restricted", "none", "scraped"),
    "app_rankings": SourceRights("App store terms of service", "restricted", "none", "scraped"),
    "web_traffic": SourceRights("Tranco list terms / SimilarWeb terms", "restricted", "none", "official"),
    "opencorporates": SourceRights("OpenCorporates terms (ODbL share-alike + API ToS)", "restricted",
                                   "personal", "official", notes="Officer names are natural persons."),
    "medspa_discovery": SourceRights("Derived from Yelp Fusion data (Yelp terms)", "restricted",
                                     "business_contact", "derived"),
    "vertical_discovery": SourceRights("Derived from Yelp / Google Places data (vendor terms)",
                                       "restricted", "business_contact", "derived"),
    # ── scraped / LLM-extracted collections ─────────────────────────────
    "job_postings": SourceRights("Public ATS job boards (Greenhouse, Lever, Workday, Ashby); "
                                 "content owned by the employers", "internal_only", "none", "scraped"),
    "public_lp_strategies": SourceRights("Public pension documents (public records); LLM-extracted",
                                         "internal_only", "business_contact", "llm_extracted"),
    "people_collection": SourceRights("Company websites, SEC filings, news (mixed)", "internal_only",
                                      "personal", "llm_extracted",
                                      notes="Contains guessed emails (people_models.py); personal data."),
    "pe_collection": SourceRights("Firm websites, news, SEC filings (mixed)", "internal_only",
                                  "personal", "llm_extracted"),
    "lp_collection": SourceRights("Public LP websites and documents", "internal_only",
                                  "business_contact", "scraped"),
    "family_office_collection": SourceRights("Public family-office websites, SEC ADV", "internal_only",
                                             "personal", "scraped"),
    "agentic_research": SourceRights("Web research by LLM agents (mixed public pages)", "internal_only",
                                     "none", "llm_extracted"),
    "rollup_intel": SourceRights("Derived from Census CBP and IRS SOI (public domain inputs)",
                                 "internal_only", "none", "derived"),
    "synthetic": SourceRights("Nexdata synthetic data (generated)", "internal_only", "none", "synthetic",
                              notes="Never publish; written into real tables (job_postings, lp_fund)."),
    # ── site intelligence ───────────────────────────────────────────────
    # Default for a collector with no COLLECTOR_RIGHTS entry: nothing leaves.
    # Every existing collector is declared explicitly below (SPEC_141; the old
    # default made 27 collectors, HIFLD included, silently 'open').
    "site_intel": SourceRights(
        "Undeclared: add a COLLECTOR_RIGHTS entry for this collector", "internal_only", "none",
        "official"),
    # ── Nexdata-derived ─────────────────────────────────────────────────
    "entity_master": SourceRights("Nexdata-derived from SEC EDGAR (public domain inputs)",
                                  "internal_only", "business_contact", "derived"),
    "pe_marts": SourceRights("Nexdata-derived from SEC EDGAR (public domain inputs)",
                             "internal_only", "business_contact", "derived"),
}


# Per-collector overrides inside the site_intel family (key = SiteIntelSource value).
def _r(license: str, redistribution: str, origin: str, pii: str = "none",
       attribution: Optional[str] = None, notes: Optional[str] = None) -> SourceRights:
    return SourceRights(license, redistribution, pii, origin, attribution=attribution, notes=notes)


def _si_usg(agency: str, notes: Optional[str] = None) -> SourceRights:
    """A site-intel collector reading a US federal source (explicit, never a default)."""
    return _usg(agency, "none", notes)


COLLECTOR_RIGHTS: Dict[str, SourceRights] = {
    # ── US federal sources (17 U.S.C. §105; unreviewed) ─────────────────
    "bls": _si_usg("U.S. Bureau of Labor Statistics (LAUS, OEWS)"),
    "bls_qcew": _si_usg("U.S. Bureau of Labor Statistics (QCEW)"),
    "bts": _si_usg("Bureau of Transportation Statistics (NTAD)"),
    "bts_cargo": _si_usg("Bureau of Transportation Statistics (T-100)"),
    "bts_ntad": _si_usg("Bureau of Transportation Statistics (NTAD)"),
    "cdfi_oz": _si_usg("U.S. HUD / CDFI Fund (Opportunity Zones)"),
    "census_bps": _si_usg("U.S. Census Bureau, Building Permits Survey"),
    "census_gov": _si_usg("U.S. Census Bureau, Census of Governments"),
    "census_trade": _si_usg("U.S. Census Bureau, international trade"),
    "eia": _si_usg("U.S. Energy Information Administration"),
    "eia_gas": _si_usg("U.S. Energy Information Administration"),
    "epa_acres": _si_usg("U.S. EPA ACRES (via FRS)"),
    "epa_envirofacts": _si_usg("U.S. EPA Envirofacts (TRI)"),
    "epa_sdwis": _si_usg("U.S. EPA SDWIS"),
    "fcc": _si_usg("Federal Communications Commission"),
    "fema": _si_usg("FEMA National Risk Index"),
    "fema_nfhl": _si_usg("FEMA National Flood Hazard Layer"),
    "fra": _si_usg("Federal Railroad Administration / BTS NTAD rail network"),
    "ftz_board": _si_usg("Foreign-Trade Zones Board (U.S. Department of Commerce)",
                         "The loaded rows are a hand-compiled seed list (datasets origin 'curated')."),
    "nrel_resource": _r("NREL open data (DOE national laboratory, contractor-operated; free with credit)",
                        "open", "official", attribution="Source: National Renewable Energy Laboratory",
                        notes="NREL is a contractor-operated lab: not strictly 17 U.S.C. §105 (PLAN_088 §1.7)."),
    "usace": _si_usg("U.S. Army Corps of Engineers, Waterborne Commerce Statistics"),
    "usda_ams": _si_usg("USDA Agricultural Marketing Service"),
    "usfws_nwi": _si_usg("U.S. Fish and Wildlife Service, National Wetlands Inventory"),
    "usgs_3dep": _si_usg("U.S. Geological Survey (3DEP)"),
    "usgs_earthquake": _si_usg("U.S. Geological Survey"),
    "usgs_water": _si_usg("U.S. Geological Survey Water Services"),
    # HIFLD: DHS withdrew substations from public release in 2022 and shut
    # HIFLD Open on 2025-08-26; the collector reads a third-party (Rutgers)
    # mirror (hifld_collector.py:51-52). Cited tightening, PLAN_088 §1.7.
    "hifld": _r("HIFLD layers via a third-party mirror (HIFLD Open discontinued 2025-08-26)",
                "restricted", "scraped",
                notes="Citation: atcoordinates.info/2025/08/08/hifld-open-gis-portal-shuts-down-aug-26-2025. "
                      "Substation layer withdrawn from public release in 2022."),
    # ── vendors, scraped and derived ────────────────────────────────────
    "drewry": _r("Drewry WCI (proprietary index, scraped headline)", "restricted", "scraped"),
    "freightos": _r("Freightos FBX terms (proprietary index)", "restricted", "scraped"),
    "scfi": _r("Shanghai Shipping Exchange SCFI (proprietary index)", "restricted", "scraped"),
    "loopnet": _r("LoopNet / CoStar terms of use (listings)", "restricted", "scraped", "business_contact"),
    "good_jobs_first": _r("Good Jobs First Subsidy Tracker terms", "restricted", "scraped"),
    "national_zoning_atlas": _r("National Zoning Atlas terms", "restricted", "official"),
    "peeringdb": _r("PeeringDB acceptable use policy", "restricted", "official", "business_contact"),
    "epoch_dc": _r(CC_BY_4, "attribution", "official", attribution="Source: Epoch AI"),
    "openei_urdb": _r("OpenEI Utility Rate Database (CC0 / public)", "open", "official",
                      attribution="Source: OpenEI URDB (NREL)"),
    "njdep_lulc": _r("NJ DEP open data (public record)", "attribution", "official",
                     attribution="Source: New Jersey Department of Environmental Protection"),
    "state_edo": _r("State economic development office websites (scraped)", "internal_only", "scraped"),
    "state_edo_sites": _r("State EDO certified-site listings (scraped)", "internal_only", "scraped",
                          "business_contact"),
    "transport_topics": _r("Transport Topics Top 100 lists (scraped)", "restricted", "scraped"),
    "three_pl_website": _r("Company websites (scraped, LLM-assisted)", "internal_only", "llm_extracted",
                           "business_contact"),
    "three_pl_sec": _r(USG, "internal_only", "derived", notes="SEC facts joined onto scraped 3PL list"),
    "three_pl_fmcsa": _r(USG, "internal_only", "derived", notes="FMCSA facts joined onto scraped 3PL list"),
    # PII raised to personal (SPEC_141, PLAN_088 §1.6): the FMCSA census includes
    # owner-operators, whose legal name, phone, email and address are a natural person's.
    "fmcsa": _r(USG, "open", "official", "personal",
                attribution="Source: FMCSA",
                notes="Carrier registrations include sole proprietors (owner-operators): personal data."),
}

# Per-dataset overrides where one family mixes licences (key = dataset key).
DATASET_RIGHTS: Dict[str, SourceRights] = {
    "realestate_fhfa_hpi": _usg("Federal Housing Finance Agency"),
    "realestate_hud_permits": _usg("U.S. Department of Housing and Urban Development"),
    "realestate_redfin": _r("Redfin Data Center terms (attribution, no resale)", "restricted", "official"),
    "realestate_osm_buildings": _r("ODbL 1.0 (OpenStreetMap, share-alike)", "restricted", "official",
                                   attribution="© OpenStreetMap contributors"),
    "sec_edgar_submissions": _usg("U.S. Securities and Exchange Commission (EDGAR)", "personal",
                                  "Filers include natural persons (insiders filing under their own "
                                  "CIK) with name, phone and street addresses."),
    "sec_insider": _usg("U.S. Securities and Exchange Commission (EDGAR)", "personal",
                        "Reporting owners are natural persons."),
    "sec_form_d": _usg("U.S. Securities and Exchange Commission (EDGAR)", "personal",
                       "Related persons are natural persons."),
    # SPEC_141: signature_name/title/phone/city name the natural person who signed.
    "sec_13f": _usg("U.S. Securities and Exchange Commission (EDGAR)", "business_contact",
                    "Filings carry the signatory's name, title, phone and city and the "
                    "manager's street address."),
    "sec_companyfacts": _usg("U.S. Securities and Exchange Commission (EDGAR)", "none"),
    "sec_company_financials": _usg("U.S. Securities and Exchange Commission (EDGAR)", "none"),
    "entity_source_records": SourceRights(
        "Nexdata-derived from SEC EDGAR (public domain inputs)", "internal_only", "personal", "derived",
        notes="Feeds insider reporting owners (natural persons) with name, state and ZIP."),
    "entity_master": SourceRights(
        "Nexdata-derived from SEC EDGAR (public domain inputs)", "internal_only", "personal", "derived",
        notes="Resolved from entity_source_records, which include natural persons."),
    "cms_medicare_utilization": _usg("Centers for Medicare & Medicaid Services", "personal",
                                     "Rendering providers are individual physicians (name, gender, NPI)."),
    "pe_people_sec": SourceRights("Nexdata-derived from SEC Form D related persons", "internal_only",
                                  "personal", "derived"),
    "intl_imf": _r("IMF terms of use (reuse with attribution)", "attribution", "official",
                   attribution="Source: International Monetary Fund"),
    "intl_bis": _r("BIS terms of use (reuse with attribution)", "attribution", "official",
                   attribution="Source: Bank for International Settlements"),
}


def rights_for(dataset_key: str, source: str, collector: Optional[str] = None) -> SourceRights:
    """Most specific rights entry: dataset, then collector, then source family."""
    if dataset_key in DATASET_RIGHTS:
        return DATASET_RIGHTS[dataset_key]
    if collector and collector in COLLECTOR_RIGHTS:
        return COLLECTOR_RIGHTS[collector]
    if source not in SOURCE_RIGHTS:
        raise KeyError(f"no SourceRights declared for source {source!r} (dataset {dataset_key!r})")
    return SOURCE_RIGHTS[source]
