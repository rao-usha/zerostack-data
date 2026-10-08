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
tightenings (HIFLD, the site_intel default).

SPEC_142 (PLAN_088 §1.7) added ``storage`` / ``commercial_use`` /
``share_alike`` and a citation (URL, quote, confidence) on every block the
rights research covered — the research rows are checked in verbatim at
``evidence/rights_research_2026-09-25.json``. Rule: **tighten now, loosen by
proposal only.** Tightenings with a citation are applied here; loosenings are
``proposed`` (a ``RightsProposal``), shown in the review queue and report and
never applied. ``reviewed`` is never set here: it comes from the committed
``rights_reviewed.REVIEWED`` hash (see ``rights_review``).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Dict, Optional

from app.catalog.spec import RightsProposal, check_rights_terms

USG = "US Government work (17 U.S.C. §105), public domain"
CC_BY_4 = "CC BY 4.0"
CC_BY_4_URL = "https://creativecommons.org/licenses/by/4.0/"
CC0_URL = "https://creativecommons.org/publicdomain/zero/1.0/"
ODBL_URL = "https://opendatacommons.org/licenses/odbl/1-0/"


@dataclass(frozen=True)
class SourceRights:
    license: str
    redistribution: str        # internal_only | attribution | open | restricted
    pii_class: str             # none | business_contact | personal
    origin: str                # official | derived | llm_extracted | synthetic | scraped | curated
    attribution: Optional[str] = None
    reviewed: bool = False     # never True here: REVIEWED hash only (SPEC_142)
    notes: Optional[str] = None
    # -- SPEC_142 -------------------------------------------------------------
    storage: Optional[str] = None               # allowed | time_limited | forbidden; None = not assessed
    storage_max_age_days: Optional[int] = None
    commercial_use: Optional[str] = None        # allowed | restricted | agreement_required | forbidden
    share_alike: bool = False
    license_url: Optional[str] = None
    citation_url: Optional[str] = None
    citation_quote: Optional[str] = None
    confidence: Optional[str] = None
    proposed: Optional[RightsProposal] = None

    def __post_init__(self) -> None:
        if self.reviewed:
            raise ValueError("SourceRights.reviewed cannot be set by hand: a sign-off is a "
                             "committed rights_reviewed.REVIEWED hash (SPEC_142)")
        check_rights_terms(f"SourceRights {self.license!r}", storage=self.storage,
                           storage_max_age_days=self.storage_max_age_days,
                           commercial_use=self.commercial_use, license_url=self.license_url,
                           citation_url=self.citation_url, citation_quote=self.citation_quote,
                           confidence=self.confidence)
        if self.proposed is not None and not isinstance(self.proposed, RightsProposal):
            raise ValueError("SourceRights.proposed must be a RightsProposal")

    def spec_fields(self) -> Dict[str, Any]:
        """The SPEC_142 fields as DatasetSpec keyword arguments."""
        return {
            "storage": self.storage, "storage_max_age_days": self.storage_max_age_days,
            "commercial_use": self.commercial_use, "share_alike": self.share_alike,
            "license_url": self.license_url, "citation_url": self.citation_url,
            "citation_quote": self.citation_quote, "rights_confidence": self.confidence,
            "rights_notes": self.notes, "proposed_rights": self.proposed,
        }


def _cite(url: str, quote: Optional[str], confidence: str) -> Dict[str, Any]:
    """Citation kwargs. ``quote`` is None where the page was not fetched (never a paraphrase)."""
    return {"citation_url": url, "citation_quote": quote, "confidence": confidence}


def _usg(agency: str, pii: str = "none", notes: Optional[str] = None, **kw) -> SourceRights:
    kw.setdefault("attribution", f"Source: {agency}")
    return SourceRights(USG, "open", pii, "official", notes=notes, **kw)


# -- citations from the rights research (evidence/rights_research_2026-09-25.json) --
_SEC = _cite("https://www.sec.gov/about/privacy-information",
             "Information presented on sec.gov is considered public information and may be copied or "
             "further distributed by users of the web site without the SEC's permission.", "high")
_BLS = _cite("https://www.bls.gov/opub/copyright-information.htm",
             "everything that we publish, both in hard copy and electronically, is in the public domain, "
             "except for previously copyrighted photographs and illustrations ... we do ask that you cite "
             "the Bureau of Labor Statistics as the source.", "high")
_TREASURY = _cite("https://fiscaldata.treasury.gov/about-us/",
                  "The U.S. Department of the Treasury's Bureau of the Fiscal Service is committed to providing "
                  "open data. The data on this site is available to copy, adapt, redistribute, or otherwise use "
                  "for non-commercial and commercial purposes.", "high")
_EIA = _cite("https://www.eia.gov/about/copyrights_reuse.php",
             "U.S. government publications are in the public domain and are not subject to copyright "
             "protection ... may contain ... information resources contributed or licensed by private "
             "individuals, companies, or organizations that may be protected", "high")
_CENSUS = _cite("https://www.census.gov/about/policies/citation.html",
                "Data users who create their own estimates using data from disseminated tables ... should "
                "cite the Census Bureau as the source of the original data only.", "high")
_FEMA = _cite("https://www.fema.gov/about/openfema/terms-conditions",
              "This product uses the Federal Emergency Management Agency's OpenFEMA API, but is not "
              "endorsed by FEMA. The Federal Government or FEMA cannot vouch for the data or analyses "
              "derived from these data...", "high")
_FCC = _cite("https://help.bdc.fcc.gov/hc/en-us/articles/10419121200923-How-Entities-Can-Access-the-Location-Fabric",
             "The Fabric is available ... through a licensing agreement ... with CostQuest", "medium-high")
_NREL = _cite("https://developer.nrel.gov/terms/", None, "medium")
_FEDERAL = _cite("https://resources.data.gov/open-licenses/", None, "medium-high")
_YELP = _cite("https://terms.yelp.com/developers/api_terms/20250113_en_us/",
              "cache, record, pre-fetch, or otherwise store any portion of the Yelp Content for a period "
              "longer than twenty-four (24) hours ... use it to update or create your own database of "
              "business listing information", "high")
_FRED = _cite("https://fred.stlouisfed.org/legal/",
              "prohibited: 'caching, or archiving any portion of the FRED® Services' ... third-party "
              "copyrighted series restricted to non-commercial educational or personal use 'Without first "
              "obtaining the express written permission of the copyright holder'", "high")
_OPENFEMA_NOTICE = ("This product uses the Federal Emergency Management Agency's OpenFEMA API, but is not "
                    "endorsed by FEMA. The Federal Government or FEMA cannot vouch for the data or analyses "
                    "derived from these data.")
_CENSUS_NOTICE = "This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau."
_NREL_LICENSE = "DOE/NREL open data (contractor-operated lab; free for any use with credit)"
_NREL_NOTE = ("NREL is a contractor-operated DOE laboratory: its data is free for any use with credit, "
              "but it is not a 17 U.S.C. §105 work (PLAN_088 §1.7). Terms page fetch failed (DNS); "
              "search excerpt: data 'can be used for any purpose whatsoever'.")

# Rights review batch 1 (2026-09-29, approved by the reviewer of record): storage and
# commercial use assessed as allowed on the cited terms. Applied only to the approved
# datasets / families; the sign-off itself is the committed REVIEWED hash.
_ALLOWED_TERMS: Dict[str, Any] = {"storage": "allowed", "commercial_use": "allowed"}
_SEC_MARKS_NOTE = ("Do not use the SEC seal, logos or EDGAR trademarks (SEC, EDGAR, EDGARLink) in a trade name, "
                   "trademark or domain; SEC asks for citation as the source. Filings are authored by filers "
                   "but are public records.")

# Rights review batch 2 (2026-10-03, approved by the reviewer of record): 30 federal
# datasets, every quote re-fetched live by an independent verifier. bts_vmt is HELD (its
# Socrata id is not a VMT dataset), so the "bts" family entry is untouched and only
# bts_border_crossing / bts_faf_regional get dataset entries. The constants above (_EIA,
# _FEMA, _FCC, _CENSUS, _OPENFEMA_NOTICE) still back the unreviewed site_intel collectors.
_CENSUS_LICENSE = ("US Government work (17 U.S.C. §105), public domain; access governed by Census Bureau Data "
                   "API Terms of Service")
_BEA_LICENSE = ("US Government work (17 U.S.C. §105), public domain; use of the API is governed by the BEA API "
                "Terms of Service")
_BEA_NOTICE = ("This product uses the Bureau of Economic Analysis (BEA) Data API but is not endorsed or "
               "certified by BEA. Source: U.S. Bureau of Economic Analysis")
_BEA_TOS = "https://apps.bea.gov/API/_pdf/bea_api_tos.pdf"
_BEA_QUOTE = ("All services, which utilize or access the API, should display the following notice prominently "
              "within the application: \"This product uses the Bureau of Economic Analysis (BEA) Data API but "
              "is not endorsed or certified by BEA.\"")
_BEA_NOTE = ("BEA API ToS: 'You may not modify or falsely represent content accessed through the API and still "
             "claim the source is the BEA' -- label derived or transformed values as Nexdata calculations, never "
             "as BEA figures. No use of the BEA name to imply endorsement. Public-domain status rests on "
             "17 U.S.C. §105 (no explicit bea.gov statement found).")
_CENSUS_TOS = "https://www.census.gov/data/developers/about/terms-of-service.html"
_CENSUS_ATTRIBUTION_QUOTE = ("All services, which utilize or access the API, should display the following notice "
                             "prominently within the application: 'This product uses the Census Bureau Data API "
                             "but is not endorsed or certified by the Census Bureau.'")
_CENSUS_TOS_NOTE = ("Census Data API ToS: display the API notice prominently wherever the data is surfaced; no "
                    "use of the Census Bureau name to imply endorsement; do not modify or falsely represent "
                    "content and still claim the Census Bureau as the source. Re-identification ban: never use "
                    "the data, alone or combined with any other data, to identify a person, household, business "
                    "or other entity. The ToS is silent on storage (allowed rests on 17 U.S.C. §105); Census may "
                    "terminate API access at its discretion.")
_EIA_LICENSE = ("US Government work (17 U.S.C. §105), public domain; EIA API Terms of Service apply to API "
                "access")
_EIA_DATED = "Source: U.S. Energy Information Administration (<publication/retrieval date>)"
_EIA_REUSE = _cite("https://www.eia.gov/about/copyrights_reuse.php",
                   "U.S. government publications are in the public domain and are not subject to copyright "
                   "protection. You may use and/or distribute any of our data, files, databases, reports, graphs, "
                   "charts, and other information products that are on our website or that you receive through "
                   "our email distribution service. However, if you use or reproduce any of our information "
                   "products, you should use an acknowledgment, which includes the publication date, such as: "
                   "\"Source: U.S. Energy Information Administration (Oct 2008).\"", "high")
_EIA_NOTE = ("The acknowledgment must carry the publication (or retrieval) date. EIA API ToS: no use of the EIA "
             "names to imply endorsement, do not modify content and still claim EIA as the source; the EIA logo "
             "is trademarked. Third-party carve-out: Refinitiv (LSEG) spot prices redistributed by EIA "
             "(petroleum/pri/spt; Henry Hub spot, natural-gas/pri/fut) are not EIA works -- exclude them or tag "
             "them restricted if a custom route ever loads them. The default routes are EIA's own surveys.")
_OPENFEMA_NOTICE_FULL = ("This product uses the Federal Emergency Management Agency's OpenFEMA API, but is not "
                         "endorsed by FEMA. The Federal Government or FEMA cannot vouch for the data or analyses "
                         "derived from these data after the data have been retrieved from the Agency's "
                         "website(s).")
_OPENFEMA_TERMS = "https://www.fema.gov/about/openfema/terms-conditions"
_FEMA_NOTE = ("OpenFEMA terms: storage is allowed but revocable -- 'cease using the data and destroy any copy' "
              "if FEMA requests (keep a purge path). No re-identification of individuals whose data is "
              "aggregated (do not try to identify individual property owners); no determinations affecting an "
              "individual's rights or benefits eligibility. Do not modify content and still claim FEMA as the "
              "source; no FEMA/DHS logos or seals. Cite the API endpoint + version and the access date/time. "
              "The terms also say the data 'will be used solely for statistical research or as a reporting "
              "record' (ambiguous): commercial use rests on 'does not include controls over its end use' + "
              "17 U.S.C. §105 -- flagged to legal.")



def _b2(license: str, redistribution: str, pii: str, attribution: str, notes: str, citation_url: str,
        citation_quote: str, confidence: str, commercial_use: str = "allowed") -> SourceRights:
    """A batch-2 block: official federal source, storage assessed as allowed on the cited terms."""
    return SourceRights(license, redistribution, pii, "official", attribution=attribution, notes=notes,
                        storage="allowed", commercial_use=commercial_use,
                        **_cite(citation_url, citation_quote, confidence))


_STORAGE_FORBIDDEN_NOTE = ("Flagged only (PLAN_088 decision 3): the rows are kept until the purge / "
                           "licence decision; samples are refused to non-admins.")


SOURCE_RIGHTS: Dict[str, SourceRights] = {
    # ── US government (public domain; still unreviewed) ─────────────────
    "sec": _usg("U.S. Securities and Exchange Commission (EDGAR)", "business_contact",
                "Filings name natural persons (insiders, related persons, signatories); "
                "per-dataset pii_class overrides apply. Do not use the SEC seal or EDGAR marks.", **_SEC),
    "treasury": _usg("U.S. Department of the Treasury, Fiscal Data", **_ALLOWED_TERMS, **_TREASURY),
    # Rights review batch 2: usaspending_awards is the family's only dataset.
    "usaspending": _b2(
        "Open data, explicitly licensed for commercial and non-commercial reuse by Treasury/Bureau of the Fiscal "
        "Service (US Government work), SUBJECT TO the Dun & Bradstreet limitation on D&B-supplied entity data",
        "attribution", "business_contact",
        "Source: USAspending.gov (U.S. Department of the Treasury, Bureau of the Fiscal Service). Recipient "
        "business name/address data may include data supplied by Dun & Bradstreet, Inc. (D&B).",
        "D&B limitation (usaspending.gov/db_info): 'D&B Open Data' (business name, street address, city, "
        "state, country, county code, ZIP) needs written D&B attribution and may not be accessed, used or "
        "disseminated in bulk as an original source or substitute for D&B; no other D&B data for commercial, "
        "resale or marketing use. We store Recipient Name (D&B Open Data), UEI (SAM.gov-issued since April "
        "2022) and agency-reported place-of-performance city/state/ZIP5 -- no DUNS, no street address. Never "
        "resell or bulk-export recipient-name lists as a firmographic product; keep the D&B attribution on "
        "any export of recipient names. Quote taken from the site's official source (the About page is a JS "
        "app); confidence medium-high in substance.",
        "https://www.usaspending.gov/about",
        "The U.S. Department of the Treasury, Bureau of the Fiscal Service is committed to providing open "
        "data to enable effective tracking of federal spending.  The data on this site is available to copy, "
        "adapt, redistribute, or otherwise use for non-commercial or for commercial purposes, subject to the "
        "Limitation on Permissible Use of Dun & Bradstreet, Inc. Data noted on the homepage.", "medium"),
    # Rights review batch 2: all five eia datasets approved; eia_steo has its own entry.
    "eia": _b2(_EIA_LICENSE, "attribution", "none", _EIA_DATED, _EIA_NOTE, **_EIA_REUSE),
    "noaa": _usg("NOAA National Centers for Environmental Information",
                 notes="WMO Resolution 40: for non-U.S. locations, GHCN data or any derived product shall not "
                       "be provided to other users or used for the re-export of commercial services. Restrict "
                       "to U.S. stations or tag non-U.S. stations restricted.",
                 **_cite("https://www.nco.ncep.noaa.gov/pmb/docs/restricted_data/r40synop/",
                         "for non-U.S. locations data, the data or any derived product shall not be provided to "
                         "other users or be used for the re-export of commercial services.", "high")),
    # AFDC is run by NREL (contractor-operated): free with credit, not §105 (PLAN_088 §1.7).
    "afdc": SourceRights(_NREL_LICENSE, "attribution", "business_contact", "official",
                         attribution="Source: U.S. DOE Alternative Fuels Data Center (NREL)",
                         notes=_NREL_NOTE, **_NREL),
    "bls": _usg("U.S. Bureau of Labor Statistics", **_ALLOWED_TERMS, **_BLS),
    # Rights review batch 2: all four bea datasets approved (BEA API ToS).
    "bea": _b2(_BEA_LICENSE, "attribution", "none", _BEA_NOTICE, _BEA_NOTE, _BEA_TOS, _BEA_QUOTE, "high"),
    # Rights review batch 2: all three fema datasets approved (full OpenFEMA notice).
    "fema": _b2("OpenFEMA API Terms and Conditions (US Government work, 17 U.S.C. §105)", "attribution", "none",
                _OPENFEMA_NOTICE_FULL, _FEMA_NOTE, _OPENFEMA_TERMS,
                "Data accessed through FEMA's API does not include controls over its end use. However, as the "
                "data owner, FEMA retains version control. Once the data has been downloaded from the API, FEMA "
                "cannot verify the quality and/or timeliness of any analysis conducted using data retrieved.",
                "high"),
    "fdic": _usg("Federal Deposit Insurance Corporation",
                 notes="Terms page not fetched: confirm the FDIC website policy before sign-off.",
                 **_cite("https://www.fdic.gov/", None, "medium")),
    "cms": _usg("Centers for Medicare & Medicaid Services",
                **_cite("https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports",
                        None, "medium")),
    # SPEC_163: the taxonomy columns carry NUCC Health Care Provider Taxonomy content (AMA
    # copyright). Notes only: notes are not a hashed rights field, and nppes is not in REVIEWED.
    "nppes": _usg("CMS NPPES NPI Registry", "personal",
                  "Individual providers are natural persons (name, practice address). The taxonomy "
                  "columns (taxonomy_code / taxonomy_description) carry the NUCC Health Care Provider "
                  "Taxonomy code set, 'Copyright 2026 American Medical Association'; NUCC: 'For commercial "
                  "use, including sales or licensing, a license must be obtained from this web site.' "
                  "Owner decision D9 (PLAN_100, 2026-10-08): NUCC content is used INTERNALLY only; the NUCC "
                  "commercial licence is requested before any external release of taxonomy codes or "
                  "descriptions (customer export, published ontology, public API). See "
                  "REFERENCE_STANDARD_RIGHTS['nucc_taxonomy'].",
                  **_cite("https://www.cms.gov/medicare/regulations-guidance/administrative-simplification/data-dissemination",
                          "The information disclosed on the NPI Registry and in the downloadable files are "
                          "FOIA-disclosable ... There is no charge to download the NPPES file", "high")),
    "fbi_crime": _usg("FBI Crime Data Explorer", **_FEDERAL),
    # Rights review batch 2 (irs_soi, fcc_broadband, us_trade, cftc_cot, usda): every dataset in
    # each family approved. "bts" is NOT: bts_vmt is held (dataset entries below).
    "irs_soi": _b2(USG, "open", "none", "Source: IRS Statistics of Income",
                   "No dedicated IRS/SOI reuse page: the cite is the Internal Revenue Manual (an internal "
                   "publishing manual, not a public licence); public domain rests on 17 U.S.C. §105. IRS "
                   "suppresses small cells before release: never try to re-identify them. Do not imply IRS "
                   "endorsement.",
                   "https://www.irs.gov/irm/part1/irm_01-017-008",
                   "Government publications that are prepared by Government Officers or employees as part of "
                   "their official duties are not subject to copyright protection.", "medium"),
    "fcc_broadband": _b2(
        "US Government work, public domain. The opendata.fcc.gov dataset we query (4kuc-phrr) carries the "
        "Socrata license \"Public Domain U.S. Government\" (USGOV_WORKS, termsLink "
        "https://www.usa.gov/government-works).",
        "attribution", "none",
        "Source: Federal Communications Commission, Fixed Broadband Deployment Data (Form 477, June 2020 V1), "
        "opendata.fcc.gov",
        "Stored data is legacy Form 477 state aggregates (opendata.fcc.gov 4kuc-phrr, June 2020 V1; "
        "superseded by BDC): no location_id, BSL lat/lon or address fields, so the CostQuest Fabric licence "
        "does not apply. Guard rail: never call fetch_location_coverage or store BDC location-level "
        "availability (the Fabric is CostQuest-licensed). The county path (broadbandmap.fcc.gov map/summary, "
        "map/providers) has its own, unreviewed terms.",
        "https://opendata.fcc.gov/api/views/4kuc-phrr.json",
        "\"name\" : \"Fixed Broadband Deployment Data: June 2020 V1\" ... \"license\" : { \"name\" : "
        "\"Public Domain U.S. Government\", \"termsLink\" : \"https://www.usa.gov/government-works\" }, "
        "\"licenseId\" : \"USGOV_WORKS\"", "high"),
    "us_trade": _b2(_CENSUS_LICENSE, "attribution", "none",
                    f"Source: U.S. Census Bureau, international trade. {_CENSUS_NOTICE}",
                    _CENSUS_TOS_NOTE + " Do not link state x HS (or port) cells to entity/company data to infer "
                    "a single exporter or importer.", _CENSUS_TOS, _CENSUS_ATTRIBUTION_QUOTE, "high"),
    "bts": _usg("Bureau of Transportation Statistics", **_FEDERAL),
    "cftc_cot": _b2(USG, "attribution", "none",
                    "Source: U.S. Commodity Futures Trading Commission, Commitments of Traders",
                    "Acknowledgement is requested, not required. The site's third-party copyright carve-out "
                    "does not reach the CFTC-produced COT reports. Aggregate trader-category positions; no PII.",
                    "https://www.cftc.gov/WebPolicy/index.htm",
                    "Government information at the CFTC website is in the public domain. Public domain "
                    "information may be freely distributed and copied, but it is requested that in any "
                    "subsequent use the CFTC be given appropriate acknowledgement.", "high"),
    "usda": _b2(
        "US Government work, public domain. Use of the Quick Stats API is governed by the NASS API Terms of "
        "Service: required non-endorsement notice, no implied endorsement, no misrepresenting modified content "
        "as NASS.",
        "attribution", "none", "This product uses the NASS API but is not endorsed or certified by NASS.",
        "Show the exact NASS notice prominently in every UI or export serving this data. Do not present "
        "derived or modified figures as NASS estimates: label derived metrics as Nexdata calculations. Respect "
        "the API limits (access/call limits; at most 50,000 records per call).",
        "https://quickstats.nass.usda.gov/api",
        "All services which utilize or access the API should display the following notice prominently within "
        "the application: \"This product uses the NASS API but is not endorsed or certified by NASS.\" You "
        "may use the NASS name in order to identify the source of API content, subject to these rules. You "
        "may not use the NASS name to imply endorsement of any product, service, or entity (not-for-profit, "
        "commercial or otherwise). ... You may not modify or falsely represent content accessed through the "
        "NASS API and still claim the source is NASS.", "high"),
    "fda": _usg("U.S. Food and Drug Administration (openFDA)", "business_contact",
                "openFDA dedicates its data CC0 1.0 (private-party copyrighted submissions excluded).",
                **_cite("https://open.fda.gov/license",
                        "public domain and made available with a Creative Commons CC0 1.0 Universal dedication "
                        "... even for commercial purposes, all without asking permission.", "high")),
    "sam_gov": _usg("GSA SAM.gov", "business_contact",
                    "Entity registrations include points of contact. Open only with a public-sensitivity "
                    "key: FOUO / sensitive API values may not be disseminated.",
                    **_cite("https://open.gsa.gov/api/entity-api/",
                            "You may not use the Entity Management FOUO API to build out a public view of the "
                            "data ... You are not allowed to display or disseminate outside the U.S. Government "
                            "any values received in a sensitive API response.", "medium-high")),
    # Rights review batch 2 (osha, census, epa_echo): every dataset in each family approved.
    "osha": _b2(
        "US Government work (17 U.S.C. §105), public domain per DOL copyright policy", "attribution", "none",
        "Source: U.S. Department of Labor, Occupational Safety and Health Administration (https://www.dol.gov). "
        "Not endorsed by DOL. If data is pulled via the DOL Data API: \"This product uses the DOL Data API but "
        "is not endorsed or certified by the DOL\".",
        "DOL asks for credit to the U.S. Department of Labor and/or dol.gov and forbids implying affiliation or "
        "endorsement. The DOL Data API notice is from a search snippet (unverified on the live page); it applies "
        "only if ingest moves to that API. Keep accident/injury tables (worker-level data) out. Enforcement "
        "records are not final findings: never present them as adjudicated violations. Confidence medium-high "
        "in substance.",
        "https://www.dol.gov/general/aboutdol/copyright",
        "Materials created by the federal government are generally part of the public domain and may be used, "
        "reproduced and distributed without permission.", "medium"),
    "census": _b2(_CENSUS_LICENSE, "attribution", "none", f"Source: U.S. Census Bureau. {_CENSUS_NOTICE}",
                  _CENSUS_TOS_NOTE, _CENSUS_TOS, _CENSUS_ATTRIBUTION_QUOTE, "high"),
    "epa_echo": _b2(
        "US Government work (17 U.S.C. §105), public domain; data.gov catalog record for ECHO Facilities lists "
        "CC0 1.0 (creativecommons.org/publicdomain/zero/1.0/)",
        "attribution", "none",
        "Source: U.S. EPA Enforcement and Compliance History Online (ECHO). Made available by EPA for "
        "informational purposes only; EPA makes no warranty regarding the accuracy, completeness, or currency "
        "of this information and does not endorse this product.",
        "The quote is EPA's no-warranty disclaimer; the rights basis is the catalog.data.gov ECHO Facilities "
        "record (CC0 1.0) plus 17 U.S.C. §105. Attribution is good practice, not legally required under CC0. "
        "ECHO is 'not designed for large scale data transfers or robotic queries' and EPA may disable "
        "automated users: keep REST request rates low or use the ECHO bulk downloads for full refreshes. "
        "Confidence medium-high in substance.",
        "https://echo.epa.gov/resources/echo-data/about-the-data",
        "The information contained in ECHO is made available to the public by the U.S. Environmental "
        "Protection Agency (EPA) for informational purposes only. No warranty, express or implied, is made by "
        "EPA or any other agency of the U.S. Government regarding the accuracy, completeness, or currency of "
        "this information.", "medium"),
    # ── open licence, attribution required ──────────────────────────────
    "uspto": SourceRights(CC_BY_4, "attribution", "personal", "official",
                          attribution="Source: USPTO PatentsView",
                          notes="Inventor names are natural persons. CC BY 4.0: indicate changes.",
                          license_url=CC_BY_4_URL,
                          **_cite("https://search.patentsview.org/docs/",
                                  "PatentsView's terms of use are under the Creative Commons Attribution 4.0 "
                                  "International License.", "high")),
    "international_econ": SourceRights(
        "CC BY 4.0 (World Bank, OECD); IMF and BIS terms of use", "attribution", "none", "official",
        attribution="Sources: World Bank WDI, IMF, OECD, BIS",
        notes="Family fallback; every intl_* dataset has its own DATASET_RIGHTS entry."),
    "data_commons": SourceRights(
        "CC BY 4.0 (Data Commons); underlying sources carry their own terms", "attribution",
        "none", "official", attribution="Source: Data Commons (datacommons.org)",
        notes="Rights depend on each variable's provenance source: store provenance and derive per series.",
        license_url=CC_BY_4_URL,
        **_cite("https://datacommons.org/faq",
                "The Data Commons knowledge graph and the compilation of the datasets is licensed under CC BY "
                "... data provenance is provided for all the data", "medium")),
    # Tightened (SPEC_142): product use needs a Free Law Project commercial agreement.
    "courtlistener": SourceRights(
        "Court records (public domain); CourtListener API terms (commercial agreement for products)",
        "restricted", "personal", "official",
        attribution="Source: CourtListener, Free Law Project; not endorsed by FLP",
        commercial_use="agreement_required",
        notes="Bankruptcy dockets name individual debtors. Filings may contain third-party copyrighted works.",
        **_cite("https://wiki.free.law/c/terms/courtlistener/courtlistenercom-terms-of-service-and-policies",
                "If you need access for a product or a team, talk to us about a commercial agreement ... other "
                "court filings may contain third-party copyrighted works", "medium")),
    "realestate": SourceRights(
        "Mixed: FHFA/HUD public domain; Redfin Data Center and OpenStreetMap (ODbL) terms",
        "restricted", "none", "official",
        notes="Family default is the most restrictive member; per-dataset overrides apply."),
    # ── restricted commercial / vendor terms ────────────────────────────
    # Tightened (SPEC_142): FRED's terms forbid caching or archiving the FRED Services, so
    # holding series *as retrieved from FRED* is itself outside the terms. Loosening
    # (re-source the public-domain series from their originators) is a proposal only.
    "fred": SourceRights(
        "FRED Terms of Use (no caching/archiving); per-series originator rights", "restricted", "none",
        "official",
        attribution="This product uses the FRED® API but is not endorsed or certified by the Federal "
                    "Reserve Bank of St. Louis. Source: Federal Reserve Bank of St. Louis (FRED) and the "
                    "originating agency.",
        storage="forbidden", commercial_use="restricted",
        notes="23 of 24 stored series are public domain at their source (Fed H.15/H.6/G.17, BLS, BEA, "
              "Census, EIA); UMCSENT is University of Michigan copyright. The FRED API terms also ban use "
              "for AI/LLM training. " + _STORAGE_FORBIDDEN_NOTE,
        proposed=RightsProposal(
            "loosen",
            "Re-source the 23 public-domain series (DGS*, DFF, DPRIME, M1SL, M2SL, BOGMBASE, CURRCIR, "
            "INDPRO, IPMAN, IPMINE, TCU, CPIAUCSL, UNRATE, GDP, GDPC1, PCE, RSXFS, DCOILWTICO, DHHNGSP) "
            "from the originating agencies (an ingestion change); they are then US Government works. "
            "UMCSENT stays restricted.",
            license="Originating agencies (Federal Reserve Board, BLS, BEA, Census, EIA): US Government work",
            redistribution="open", storage="allowed", commercial_use="allowed",
            attribution="Source: the originating agency (Federal Reserve Board, BLS, BEA, U.S. Census Bureau, EIA)",
            # the FRED legal page restricts; it cannot justify loosening. The loosening rests on
            # the originating agencies' works being US Government works (17 U.S.C. §105); the
            # agency pages were not fetched, so a reviewer must check each one (SPEC_142 fix).
            citation_url="https://resources.data.gov/open-licenses/",
            citation_quote="(federal works under 17 U.S.C. §105; agency pages not individually fetched)",
            confidence="medium"),
        **_FRED),
    # Tightened (SPEC_142): no storage beyond 24 h, no database of listings.
    "yelp": SourceRights("Yelp API Terms of Use (2025-01-13): no storage beyond 24 h, no listings database",
                         "restricted", "business_contact", "official",
                         attribution="Yelp display requirements (logo, link back to the business page)",
                         storage="forbidden", commercial_use="restricted",
                         notes="yelp_businesses holds about 400 rows, already past the 24 h limit. "
                               + _STORAGE_FORBIDDEN_NOTE, **_YELP),
    "kaggle": SourceRights("Kaggle competition rules (M5: non-commercial, competition use)",
                           "restricted", "none", "official",
                           storage="forbidden", commercial_use="forbidden",
                           notes="Licensed for non-commercial / academic use only: a commercial company may not "
                                 "use it even internally. " + _STORAGE_FORBIDDEN_NOTE,
                           **_cite("https://www.kaggle.com/c/m5-forecasting-accuracy/rules",
                                   "The competition data may be accessed and used for non-commercial purposes "
                                   "only, including for participating in the Competition ... and for academic "
                                   "research.", "high")),
    # Tightened (SPEC_142): Google Places lat/lng may be cached 30 days (place_id indefinitely).
    "foot_traffic": SourceRights(
        "Vendor terms: Google Maps Platform (Places), Foursquare, SafeGraph, Placer.ai", "restricted",
        "business_contact", "official",
        attribution="Google Maps Platform attribution requirements",
        storage="time_limited", storage_max_age_days=30, commercial_use="restricted",
        notes="Only place_id may be cached indefinitely. Scraping Google Popular Times breaches its terms.",
        **_cite("https://developers.google.com/maps/documentation/places/web-service/policies",
                "Place IDs are exempt from the caching restrictions ... latitude/longitude may be cached for up "
                "to 30 days", "high")),
    # Tightened (SPEC_142): Kalshi data terms forbid archived data sets and commercial use.
    "prediction_markets": SourceRights(
        "Kalshi Data Terms of Service; Polymarket terms", "restricted", "none", "official",
        storage="forbidden", commercial_use="forbidden",
        notes="Quote is a search excerpt (the PDF would not decode). No AI training. " + _STORAGE_FORBIDDEN_NOTE,
        **_cite("https://kalshi-public-docs.s3.amazonaws.com/kalshi-data-terms-of-service.pdf",
                "personal use for non-commercial purposes only ... Non-commercial use does not include ... "
                "providing archived or cached data sets containing Kalshi Data to another person or entity",
                "medium-high")),
    # SPEC_154 (verified 2026-10-03): S&P's three licence statements conflict and none is CC BY,
    # so the SPEC_142 "loosen if CC BY" proposal is closed. Only commodity reference data is
    # fetched (app/sources/dunl/client.py); the company file (CIQ ids) is NOT loaded.
    "dunl": SourceRights(
        "S&P Global DUNL: CC BY-NC-SA 4.0 (site) / CC BY-SA 4.0 (dataset metadata) -- conflicting",
        "restricted", "none", "official",
        storage="allowed", commercial_use="restricted", share_alike=True,
        license_url="https://creativecommons.org/licenses/by-nc-sa/4.0/",
        notes="Three conflicting S&P statements (verified 2026-10-03): the dunl.org footer and landing "
              "page name CC BY-NC-SA 4.0 ('By using this site, you confirm your acceptance of the terms "
              "of this license'); dunl.org/assets/datasets/company/parquet/company.parquet-metadata.json "
              "names CC BY-SA 4.0; the 2025-09-11 press release says 'free internal organizational use'. "
              "All carry ShareAlike, none is plain CC BY; the S&P Terms of Use the footer links returned "
              "an error page (unread). Not usable for commercial bulk loading without S&P's written "
              "clearance (owner to ask dunl@spglobal.com). The S&P Capital IQ company file is not loaded.",
        **_cite("https://dunl.org/main-DAZ2NPE4.js",
                "DUNL (Data Unlocked) - Open Data Portal © 2026 by S&P Global Inc. is licensed under "
                "CC BY-NC-SA 4.0", "high")),
    # SPEC_154: GLEIF LEI reference data (CC0). Only lei / entity / registration fields are
    # requested; the third-party mapping fields (spglobal, ocid, bic ...) are never fetched.
    "gleif": SourceRights(
        "CC0 1.0 Universal (GLEIF LEI data)", "open", "business_contact", "official",
        storage="allowed", commercial_use="allowed", license_url=CC0_URL,
        attribution="Source: Global Legal Entity Identifier Foundation (GLEIF)",
        notes="Legal entity names and addresses; sole proprietors' names appear as legal or trading "
              "names. Whether CC0 also covers the S&P CIQ-to-LEI pairs GLEIF shows (field 'spglobal') "
              "is unconfirmed: that field is not fetched. Terms IV(c): never imply GLEIF provides, "
              "supports or endorses anything beyond the original LEI data, and no GLEIF logo without "
              "permission (liquidated damages CHF 100,000 per case).",
        **_cite("https://www.gleif.org/en/meta/lei-data-terms-of-use",
                "The Access Service on the website http(s)://www.gleif.org is provided for free. The data "
                "available through the Access Service are provided under the CC0 licence, see CC0 1.0 "
                "Universal (CC0 1.0).", "high")),
    "github": SourceRights("GitHub API terms of service", "restricted", "personal", "official",
                           notes="Contributor logins are personal data. No selling users' personal information.",
                           **_cite("https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies",
                                   "Any person, entity, or service collecting data from the Service must comply "
                                   "with the GitHub Privacy Statement ... [no] selling GitHub users' personal "
                                   "information, such as to recruiters, headhunters, and job boards", "high")),
    "glassdoor": SourceRights("Glassdoor terms of use (scraped)", "restricted", "none", "scraped"),
    "app_rankings": SourceRights("App store terms of service", "restricted", "none", "scraped"),
    "web_traffic": SourceRights("Tranco list terms / SimilarWeb terms", "restricted", "none", "official",
                                notes="Tranco inputs include Cloudflare Radar (CC BY-NC 4.0): the NC component "
                                      "taints commercial use.",
                                **_cite("https://tranco-list.eu/",
                                        "Cloudflare Radar (available under a CC BY-NC 4.0 license) (search excerpt)",
                                        "medium")),
    "opencorporates": SourceRights("OpenCorporates terms (ODbL share-alike + API ToS)", "restricted",
                                   "personal", "official", share_alike=True, license_url=ODBL_URL,
                                   attribution="from OpenCorporates (link to the object URL)",
                                   notes="Officer names are natural persons. A free key is share-alike only.",
                                   **_cite("https://opencorporates.com/legal/licence",
                                           "if you combine the information with your own data, the resultant "
                                           "information must be published under the Attribution Share-Alike ODbL "
                                           "... Paid-for API Accounts ... remove the OpenCorporates share-alike "
                                           "restrictions (search excerpt)", "high")),
    # Tightened (SPEC_142): derived rows carry Yelp content (names, ratings, categories).
    "medspa_discovery": SourceRights("Derived from Yelp Fusion data (Yelp terms)", "restricted",
                                     "business_contact", "derived",
                                     storage="forbidden", commercial_use="restricted",
                                     notes="Built from Yelp content, so the Yelp storage limit applies. "
                                           + _STORAGE_FORBIDDEN_NOTE, **_YELP),
    "vertical_discovery": SourceRights("Derived from Yelp / Google Places data (vendor terms)",
                                       "restricted", "business_contact", "derived",
                                       storage="forbidden", commercial_use="restricted",
                                       notes="Built from Yelp content (and Google Places), so the Yelp storage "
                                             "limit applies. " + _STORAGE_FORBIDDEN_NOTE, **_YELP),
    # ── scraped / LLM-extracted collections ─────────────────────────────
    "job_postings": SourceRights("Public ATS job boards (Greenhouse, Lever, Workday, Ashby); "
                                 "content owned by the employers", "internal_only", "none", "scraped"),
    # SPEC_151: the gated board lane. Same posture as job_postings (employers' content,
    # internal only); storage/commercial use left unassessed (None) -- the vendors' terms bind
    # customers and say nothing either way, which is not a licence.
    "ats_boards": SourceRights("Public job-board APIs (Greenhouse Job Board API, Lever Postings API; "
                               "Ashby refused by its robots.txt); content owned by the employers",
                               "internal_only", "none", "scraped",
                               citation_url="https://developers.greenhouse.io/job-board.html",
                               citation_quote="Job Board data is publicly available, so authentication is "
                                              "not required for any GET endpoints.",
                               confidence="medium",
                               notes="Per-host terms verdicts in app/entities/data/site_terms.json; every "
                                     "request through app.core.open_web (robots, terms, UA, Retry-After). "
                                     "Description text stored with e-mails and phone numbers redacted; pay "
                                     "parsed from text is derived (snippet + confidence kept)."),
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
    # SPEC_142 fix: the ZIP med-spa score is built from IRS SOI ZIP income only (no Yelp
    # content: app/ml/zip_medspa_metadata.py), so the Yelp storage limit does not reach it.
    "zip_scores": SourceRights("Derived from IRS SOI ZIP income (public domain input)",
                               "internal_only", "none", "derived",
                               notes="Nexdata score over IRS SOI ZIP-level aggregates; no Yelp or "
                                     "other vendor content."),
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
       attribution: Optional[str] = None, notes: Optional[str] = None, **kw) -> SourceRights:
    return SourceRights(license, redistribution, pii, origin, attribution=attribution, notes=notes, **kw)


def _si_usg(agency: str, notes: Optional[str] = None, **kw) -> SourceRights:
    """A site-intel collector reading a US federal source (explicit, never a default)."""
    return _usg(agency, "none", notes, **kw)


_NZA = _r("National Zoning Atlas Terms of Use: no hosting, storing or commercial use without written "
          "permission", "restricted", "official",
          attribution="National Zoning Atlas, online at zoningatlas.org",
          storage="forbidden", commercial_use="forbidden",
          notes="Loaded from Mercatus ZIPs (nza_zoning_collector.py), whose page states no licence. The NZA "
                "now has a licensing arm. " + _STORAGE_FORBIDDEN_NOTE,
          **_cite("https://www.zoningatlas.org/terms",
                  "you may not use, host, store, reproduce, modify, license, create derivative works of ... or "
                  "distribute any of the Website Content ... you must not commercially use (including sell)",
                  "high"))

_OPENEI = _r("OpenEI Utility Rate Database (CC0 1.0)", "open", "official",
             attribution="Source: OpenEI URDB (NREL)", license_url=CC0_URL,
             **_cite("https://openei.org/wiki/Utility_Rate_Database",
                     "Content is available under Creative Commons Zero unless otherwise noted.", "high"))

COLLECTOR_RIGHTS: Dict[str, SourceRights] = {
    # ── US federal sources (17 U.S.C. §105; unreviewed) ─────────────────
    "bls": _si_usg("U.S. Bureau of Labor Statistics (LAUS, OEWS)", **_BLS),
    "bls_qcew": _si_usg("U.S. Bureau of Labor Statistics (QCEW)", **_BLS),
    "bts": _si_usg("Bureau of Transportation Statistics (NTAD)"),
    "bts_cargo": _si_usg("Bureau of Transportation Statistics (T-100)"),
    "bts_ntad": _si_usg("Bureau of Transportation Statistics (NTAD)"),
    "cdfi_oz": _si_usg("U.S. HUD / CDFI Fund (Opportunity Zones)"),
    "census_bps": _si_usg("U.S. Census Bureau, Building Permits Survey"),
    "census_gov": _si_usg("U.S. Census Bureau, Census of Governments"),
    "census_trade": _si_usg("U.S. Census Bureau, international trade"),
    "eia": _si_usg("U.S. Energy Information Administration", **_EIA),
    "eia_gas": _si_usg("U.S. Energy Information Administration", **_EIA),
    "epa_acres": _si_usg("U.S. EPA ACRES (via FRS)"),
    "epa_envirofacts": _si_usg("U.S. EPA Envirofacts (TRI)"),
    "epa_sdwis": _si_usg("U.S. EPA SDWIS"),
    "fcc": _si_usg("Federal Communications Commission",
                   "Block-level aggregates only; never ingest the BSL Location Fabric (CostQuest-licensed).",
                   **_FCC),
    "fema": _si_usg("FEMA National Risk Index",
                    attribution=f"Source: FEMA National Risk Index. {_OPENFEMA_NOTICE}", **_FEMA),
    "fema_nfhl": _si_usg("FEMA National Flood Hazard Layer",
                         attribution=f"Source: FEMA National Flood Hazard Layer. {_OPENFEMA_NOTICE}", **_FEMA),
    "fra": _si_usg("Federal Railroad Administration / BTS NTAD rail network"),
    "ftz_board": _si_usg("Foreign-Trade Zones Board (U.S. Department of Commerce)",
                         "The loaded rows are a hand-compiled seed list (datasets origin 'curated')."),
    # Tightened (SPEC_142): contractor-operated lab, free with credit -> attribution.
    "nrel_resource": _r(_NREL_LICENSE, "attribution", "official",
                        attribution="Source: National Renewable Energy Laboratory (NSRDB)",
                        notes=_NREL_NOTE, **_NREL),
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
                notes="Substation layer withdrawn from public release in 2022; the collector reads a "
                      "Rutgers mirror. Re-source transmission lines from the originating agency.",
                **_cite("https://atcoordinates.info/2025/08/08/hifld-open-gis-portal-shuts-down-aug-26-2025/",
                        "HIFLD Open was discontinued on August 26, 2025 ... The substation dataset from the US "
                        "government's HIFLD dataset has been removed from the public since 2022 (search excerpts)",
                        "medium-high")),
    # ── vendors, scraped and derived ────────────────────────────────────
    "drewry": _r("Drewry WCI (proprietary index, scraped headline)", "restricted", "scraped",
                 attribution="Source: Drewry World Container Index",
                 notes="No public licence found.",
                 **_cite("https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index-assessed-by-drewry",
                         None, "medium")),
    # Tightened (SPEC_142): no resale, no derived indexes, no AI training; credit + link.
    "freightos": _r("Freightos Data Terms and Conditions (proprietary index)", "restricted", "scraped",
                    attribution="Source: Freightos Baltic Index (FBX), terminal.freightos.com",
                    commercial_use="restricted",
                    **_cite("https://www.freightos.com/freightos-data-terms-conditions/",
                            "may only be reproduced with full credit to Freightos and a link ... does not include "
                            "the right to resell Freightos Data or use it in producing derived data such as "
                            "derivative indexes ... will in no way be used to train any AI models/LLMs", "high")),
    "scfi": _r("Shanghai Shipping Exchange SCFI (proprietary index)", "restricted", "scraped",
               attribution="Source: Shanghai Shipping Exchange", notes="No public licence found.",
               **_cite("https://en.sse.net.cn/indices/introduction_scfi.jsp", None, "medium")),
    # Tightened (SPEC_142): scraping and building a database are both prohibited.
    "loopnet": _r("LoopNet Terms of Use: scraping and database creation prohibited", "restricted", "scraped",
                  "business_contact", storage="forbidden", commercial_use="forbidden",
                  notes="Collection itself is prohibited (conflicts with CLAUDE.md's abort rule): disable the "
                        "collector. " + _STORAGE_FORBIDDEN_NOTE,
                  **_cite("https://www.loopnet.com/solutions/LoopNetTerms-of-Use",
                          "prohibit users from scraping the Product without express written permission ... "
                          "prohibit creating any database or product from the Platform (search excerpt)", "high")),
    "good_jobs_first": _r("Good Jobs First Subsidy Tracker terms (subscription); provenance of stored rows unknown",
                          "restricted", "scraped",
                          attribution="Source: Good Jobs First Subsidy Tracker",
                          notes="The 48 stored rows (source='gjf_expanded') come from a hand-typed script; the "
                                "collector's data path is removed (PLAN_082). Terms page returned 403.",
                          **_cite("https://subsidytracker.goodjobsfirst.org/plans", None, "low-medium")),
    "national_zoning_atlas": _NZA,
    # Tightened (SPEC_142): no commercial application, no bulk pass-on.
    "peeringdb": _r("PeeringDB Acceptable Use Policy", "restricted", "official", "business_contact",
                    storage="forbidden", commercial_use="forbidden",
                    notes="Site-selection scoring is arguably a 'commercial application'. " + _STORAGE_FORBIDDEN_NOTE,
                    **_cite("https://www.peeringdb.com/aup",
                            "compiling marketing lists, demographic mapping, or any other commercial application "
                            "... The PeeringDB data may not be passed on in bulk to any other person or "
                            "organization unless approved by PeeringDB", "high")),
    "epoch_dc": _r(CC_BY_4, "attribution", "official", license_url=CC_BY_4_URL,
                   attribution="Epoch AI, 'AI Data Centers'. Published online at epoch.ai (CC BY 4.0)",
                   **_cite("https://epoch.ai/data/data-centers",
                           "Epoch AI's data is free to use, distribute, and reproduce provided the source and "
                           "authors are credited under the Creative Commons Attribution license.", "high")),
    "openei_urdb": _OPENEI,
    "njdep_lulc": _r("NJ DEP open data (public record)", "attribution", "official",
                     attribution="Source: New Jersey Department of Environmental Protection",
                     notes="NJGIN terms not verified: verify before sign-off.",
                     **_cite("https://gisdata-njdep.opendata.arcgis.com/", None, "low")),
    "state_edo": _r("State economic development office websites (scraped)", "internal_only", "scraped"),
    "state_edo_sites": _r("State EDO certified-site listings (scraped)", "internal_only", "scraped",
                          "business_contact"),
    "transport_topics": _r("Transport Topics Top 100 lists (scraped)", "restricted", "scraped"),
    "three_pl_website": _r("Company websites (scraped, LLM-assisted)", "internal_only", "llm_extracted",
                           "business_contact"),
    "three_pl_sec": _r("Derived: SEC facts (public domain) joined onto a scraped base list", "internal_only",
                       "derived", notes="SEC facts joined onto the scraped Transport Topics 3PL list"),
    "three_pl_fmcsa": _r("Derived: FMCSA facts (public domain) joined onto a scraped base list", "internal_only",
                         "derived", notes="FMCSA facts joined onto the scraped Transport Topics 3PL list"),
    # PII raised to personal (SPEC_141, PLAN_088 §1.6): the FMCSA census includes
    # owner-operators, whose legal name, phone, email and address are a natural person's.
    "fmcsa": _r(USG, "open", "official", "personal",
                attribution="Source: FMCSA",
                notes="Carrier registrations include sole proprietors (owner-operators): personal data."),
}

# Per-dataset overrides where one family mixes licences (key = dataset key).
DATASET_RIGHTS: Dict[str, SourceRights] = {
    "realestate_fhfa_hpi": _usg("Federal Housing Finance Agency",
                                **_cite("https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index-Datasets.aspx",
                                        None, "medium-high")),
    "realestate_hud_permits": _usg("U.S. Department of Housing and Urban Development (SOCDS, Census "
                                   "Building Permits Survey)"),
    "realestate_redfin": _r("Redfin Data Center terms (free use with citation and link; no explicit "
                            "redistribution grant)", "restricted", "official",
                            attribution="Source: Redfin, a national real estate brokerage (link to redfin.com)",
                            notes="No explicit licence found: ask Redfin for written permission before reselling.",
                            **_cite("https://www.redfin.com/news/data-center/",
                                    "You are welcome to use Redfin's data for your own purposes ... cite ... and "
                                    "link to Redfin for the first reference (search excerpt)", "low-medium")),
    # ODbL permits commercial redistribution with attribution + share-alike: the loosening
    # to 'attribution' is a proposal only (PLAN_088 §1.7).
    "realestate_osm_buildings": _r(
        "ODbL 1.0 (OpenStreetMap, share-alike)", "restricted", "official",
        attribution="© OpenStreetMap contributors, available under the Open Database License (ODbL)",
        share_alike=True, license_url=ODBL_URL,
        notes="Keep the layer separate so the share-alike obligation does not reach proprietary marts.",
        proposed=RightsProposal(
            "loosen",
            "ODbL allows commercial copying and redistribution with attribution; derived databases must "
            "stay ODbL (share-alike). Serve as a separate layer, never merged into proprietary tables.",
            redistribution="attribution", share_alike=True,
            **_cite("https://www.openstreetmap.org/copyright",
                    "You are free to copy, distribute, transmit and adapt our data, as long as you credit "
                    "OpenStreetMap ... If you alter or build upon our data, you may distribute the result only "
                    "under the same license.", "high")),
        **_cite("https://www.openstreetmap.org/copyright",
                "You are free to copy, distribute, transmit and adapt our data, as long as you credit OpenStreetMap "
                "... If you alter or build upon our data, you may distribute the result only under the same "
                "license.", "high")),
    "sec_edgar_submissions": _usg("U.S. Securities and Exchange Commission (EDGAR)", "personal",
                                  "Filers include natural persons (insiders filing under their own "
                                  "CIK) with name, phone and street addresses.", **_SEC),
    "sec_insider": _usg("U.S. Securities and Exchange Commission (EDGAR)", "personal",
                        "Reporting owners are natural persons.", **_SEC),
    "sec_form_d": _usg("U.S. Securities and Exchange Commission (EDGAR)", "personal",
                       "Related persons are natural persons.", **_SEC),
    # SPEC_141: signature_name/title/phone/city name the natural person who signed.
    # Rights review batch 1: only these two SEC datasets were approved; the "sec" family
    # entry (IAPD / Form ADV / filings index) stays not assessed.
    "sec_13f": _usg("U.S. Securities and Exchange Commission (EDGAR)", "business_contact",
                    "Filings carry the signatory's name, title, phone and city and the "
                    "manager's street address. " + _SEC_MARKS_NOTE, **_ALLOWED_TERMS, **_SEC),
    "sec_companyfacts": _usg("U.S. Securities and Exchange Commission (EDGAR)", "none", _SEC_MARKS_NOTE,
                             **_ALLOWED_TERMS, **_SEC),
    "sec_company_financials": _usg("U.S. Securities and Exchange Commission (EDGAR)", "none", **_SEC),
    # Rights review batch 2: dataset-level differences inside approved families (quote / licence /
    # notes per the verified research), and the two approved bts datasets (bts_vmt held).
    "bea_nipa": replace(
        SOURCE_RIGHTS["bea"],
        citation_quote=_BEA_QUOTE + " You may use the BEA name in order to identify the source of API content "
                                    "subject to these rules. You may not use the BEA name, or the like to imply "
                                    "endorsement of any product, service, or entity, not-for-profit, commercial "
                                    "or otherwise."),
    "us_trade_port_trade": replace(
        SOURCE_RIGHTS["us_trade"],
        citation_quote="You may use the Census Bureau API to develop a service or service to search, display, "
                       "analyze, retrieve, view and otherwise 'get' information from Census Bureau data."),
    "census_cbp": replace(
        SOURCE_RIGHTS["census"],
        notes=_CENSUS_TOS_NOTE + " CBP is establishment counts/payroll by geography x NAICS: never join CBP "
                                 "small cells with Nexdata entity/company data (entity_master, "
                                 "industrial_companies, rollup_intel) to infer a specific business's "
                                 "employment or payroll. Aggregate use (market sizing, rollups) is fine.",
        citation_quote="Users will not use these data, alone or in combination with any other Census or "
                       "non-Census data, to identify any individual person, household, business or other "
                       "entity; not link or combine these data with information in any other Census or "
                       "non-Census dataset in a manner that identifies an individual person, household, "
                       "business or other entity."),
    "census_cbp_county_yearly": replace(
        SOURCE_RIGHTS["census"],
        notes=_CENSUS_TOS_NOTE + " County x NAICS cells can be small: never join them with company/entity "
                                 "data to identify a specific business.",
        citation_quote="You may use the Census Bureau name in order to identify the source of API content "
                       "subject to these rules. You may not use the Census Bureau name, or the like to imply "
                       "endorsement of any product, service, or entity, not-for-profit, commercial or "
                       "otherwise."),
    "census_acs_county_tract": replace(
        SOURCE_RIGHTS["census"],
        notes=_CENSUS_TOS_NOTE + " Tract-level ACS is small-area: never combine it with people/address data "
                                 "to infer individual households."),
    "eia_petroleum": replace(
        SOURCE_RIGHTS["eia"],
        license="US Government work (17 U.S.C. §105), public domain; access via EIA API v2 is also subject to "
                "the EIA API Terms of Service (https://www.eia.gov/opendata/terms-of-service.php)"),
    # EIA STEO: the ingest pulls every STEO series, including Refinitiv spot prices and the S&P
    # Global-model macro forecasts, so the whole dataset is restricted until they are excluded.
    "eia_steo": _b2(
        "US Government work (17 U.S.C. §105), public domain, EXCEPT third-party series: Refinitiv (an LSEG "
        "company) spot prices in STEO history; macroeconomic forecasts based on the S&P Global model of the "
        "U.S. Economy",
        "restricted", "none",
        "Source: U.S. Energy Information Administration, Short-Term Energy Outlook (<Month YYYY>)",
        "STEO Table 2: WTI and Brent crude, Mt. Belvieu propane and Henry Hub spot prices are 'from "
        "Refinitiv,an LSEG company, via EIA'; Table 9a: macro forecasts 'are based on the S&P Global model of "
        "the U.S. Economy'. ingest_eia_steo_projections pulls every STEO series (no facet filter), so the "
        "dataset is restricted (redistribution and commercial use) until those series are excluded or tagged "
        "restricted by seriesId; EIA-own series could then be re-reviewed as attribution. Table 9a macro "
        "forecasts stay restricted until a human decides (historical 9a values are BEA/Fed G17/FHWA/FAA "
        "public domain). EIA API ToS: use must not infringe third-party IP. Same EIA dated-acknowledgment "
        "and no-endorsement rules as the eia family.",
        "https://www.eia.gov/about/copyrights_reuse.php",
        "You may see on our website documents, illustrations, photographs, or other information resources "
        "contributed or licensed by private individuals, companies, or organizations that may be protected by "
        "U.S. and foreign copyright laws. Transmission or reproduction of protected items beyond that allowed "
        "by fair use as defined in the copyright laws requires the written permission of the copyright owners.",
        "medium", commercial_use="restricted"),
    "fema_disaster_declarations": replace(
        SOURCE_RIGHTS["fema"],
        license="OpenFEMA API Terms and Conditions (US Government work, 17 U.S.C. §105; the terms do not use "
                "the words 'public domain')",
        citation_quote="Users must clearly state that \"This product uses the Federal Emergency Management "
                       "Agency's OpenFEMA API, but is not endorsed by FEMA. The Federal Government or FEMA "
                       "cannot vouch for the data or analyses derived from these data after the data have been "
                       "retrieved from the Agency's website(s).\""),
    "fema_hma_projects": replace(
        SOURCE_RIGHTS["fema"],
        citation_quote="You agree you will cease using the data and destroy any copy you may have if requested "
                       "by FEMA."),
    "bts_border_crossing": _b2(
        "Public Domain (data.transportation.gov licenseId PUBLIC_DOMAIN). The underlying data is from U.S. DHS "
        "Customs and Border Protection, published by BTS.",
        "attribution", "none",
        "Source: Bureau of Transportation Statistics, Border Crossing Entry Data (U.S. Department of Homeland "
        "Security, Customs and Border Protection), data.transportation.gov",
        "Dataset-level Socrata licence (keg4-3bc2), the id the client queries; the URL now redirects to "
        "data.bts.gov/api/views/keg4-3bc2.json. Aggregate counts by port, month and measure; no PII.",
        "https://data.transportation.gov/api/views/keg4-3bc2.json",
        "\"name\" : \"Border Crossing Entry Data\" ... \"attribution\" : \"U.S. Department of Homeland "
        "Security, Customs and Border Protection\" ... \"license\" : { \"name\" : \"Public Domain\" }, "
        "\"licenseId\" : \"PUBLIC_DOMAIN\"", "high"),
    "bts_faf_regional": _b2(
        "US Government work (17 U.S.C. §101/§105), not protected by U.S. copyright, unrestricted public use. "
        "Joint BTS/FHWA product with technical work by ORNL.",
        "attribution", "none",
        "Source: Freight Analysis Framework (FAF5), Bureau of Transportation Statistics and Federal Highway "
        "Administration",
        "The quote is the licenseInfo of the sibling ArcGIS item 'Freight Analysis Framework (FAF5) Regions'; "
        "we ingest the FAF5.7.1 regional/state flow tables (bts.gov zips) of the same program. The item asks "
        "for acknowledgment of FHWA, the US Census Bureau, OMB and BTS [distributor].",
        "https://geodata.bts.gov/content/539fdab88ee74e38b28959494965ace2",
        "This NTAD dataset is a work of the United States government as defined in 17 U.S.C. § 101 and as "
        "such are not protected by any U.S. copyrights. This work is available for unrestricted public use.",
        "high"),
    "entity_source_records": SourceRights(
        "Nexdata-derived from SEC EDGAR (public domain inputs)", "internal_only", "personal", "derived",
        notes="Feeds insider reporting owners (natural persons) with name, state and ZIP."),
    "entity_master": SourceRights(
        "Nexdata-derived from SEC EDGAR (public domain inputs)", "internal_only", "personal", "derived",
        notes="Resolved from entity_source_records, which include natural persons."),
    # Tightened (SPEC_142): CPT Level I codes and descriptions are AMA copyright, and every
    # row carries hcpcs_desc. CMS presents its AMA CPT licence before access.
    "cms_medicare_utilization": SourceRights(
        "CMS public use file; CPT codes and descriptions © American Medical Association", "restricted",
        "personal", "official",
        attribution="Source: Centers for Medicare & Medicaid Services. CPT © American Medical Association. "
                    "All rights reserved.",
        commercial_use="agreement_required",
        notes="Rendering providers are individual physicians (name, gender, NPI). All rows carry CPT "
              "descriptors (hcpcs_desc): redistribution needs an AMA distribution licence, or strip Level I "
              "codes and descriptors from any external surface. SPEC_163 (PLAN_100 §3, §5.4): hcpcs_desc "
              "(AMA CPT descriptors) never reaches an LLM prompt or output; Level I codes are join values "
              "only. app.ontology.standards.licence_guard detects CPT codes/descriptors and SNOMED IDs "
              "to block them.",
        **_cite("https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service",
                "CPT codes, descriptions and other data are copyright American Medical Association ... The "
                "complete CMS AMA CPT License agreement is presented to users when accessing the data.", "high")),
    "pe_people_sec": SourceRights("Nexdata-derived from SEC Form D related persons", "internal_only",
                                  "personal", "derived"),
    "intl_worldbank": _r(CC_BY_4, "attribution", "official", license_url=CC_BY_4_URL,
                         attribution="Source: World Bank, World Development Indicators (CC BY 4.0)",
                         notes="A few WDI series carry third-party licences, labelled per series.",
                         **_cite("https://datacatalog.worldbank.org/public-licenses",
                                 "allows users to copy, modify and distribute data in any format for any purpose, "
                                 "including commercial use ... only obligated to give appropriate credit", "high")),
    "intl_oecd": _r("CC BY 4.0 (OECD, from 1 July 2024)", "attribution", "official", license_url=CC_BY_4_URL,
                    attribution="Source: OECD (CC BY 4.0). Where transformed: 'This is an adaptation of an "
                                "original work by the OECD.'",
                    notes="Some OECD dataflows carry third-party data: check each dataflow's licence.",
                    **_cite("https://www.oecd.org/en/about/oecd-open-by-default-policy.html",
                            "From 1 July 2024, OECD content is generally available under CC licences, with the "
                            "default licence being ... CC BY 4.0", "high")),
    # Tightened (SPEC_142): commercial reuse of IMF Data needs permission first.
    "intl_imf": _r("IMF Copyright and Usage, special terms for Data", "restricted", "official",
                   attribution="Source: International Monetary Fund",
                   commercial_use="agreement_required",
                   notes="Direct fetch returned 403; the quote is a search excerpt. Standalone resale must "
                         "disclose the data is free from the IMF.",
                   proposed=RightsProposal(
                       "loosen",
                       "Back to attribution once copyright@imf.org confirms commercial reuse in writing.",
                       license="IMF terms of use (commercial reuse permitted by IMF, reference on file)",
                       redistribution="attribution", commercial_use="allowed",
                       **_cite("https://www.imf.org/en/about/copyright-and-terms",
                               "For any potential commercial reuse of IMF Data, please email copyright@imf.org "
                               "... If IMF Data is sold by Users as a standalone product; sellers must inform "
                               "purchasers that the Data is available free of charge from the IMF.", "medium")),
                   **_cite("https://www.imf.org/en/about/copyright-and-terms",
                           "For any potential commercial reuse of IMF Data, please email copyright@imf.org ... If "
                           "IMF Data is sold by Users as a standalone product; sellers must inform purchasers that "
                           "the Data is available free of charge from the IMF.", "medium")),
    # Tightened (SPEC_142): attribution stays, but commercial inclusion may not add a charge.
    "intl_bis": _r("Terms of permitted use of BIS statistics", "attribution", "official",
                   attribution="Source: Bank for International Settlements (link to the original); no implied "
                               "endorsement",
                   commercial_use="restricted",
                   notes="Direct fetch returned 403; the quote is a search excerpt. BIS statistics may not be "
                         "priced as a separate add-on. BIS publications (not statistics) are non-commercial only.",
                   **_cite("https://www.bis.org/terms_statistics.htm",
                           "The use of BIS statistics is unrestricted, provided that the BIS must be cited ... if "
                           "the statistics are used in a commercial publication or product, their inclusion ... "
                           "will not result in any additional charge to subscribers", "medium")),
    # Mixed sources (SPEC_142): about 36% of utility_rate rows are EIA-sourced.
    "si_utility_rates": replace(
        _OPENEI, license="OpenEI URDB (CC0 1.0); EIA-sourced rows US Government work",
        attribution="Source: OpenEI URDB (NREL); U.S. Energy Information Administration",
        notes="About 36% of rows come from EIA (public domain), the rest from OpenEI URDB (CC0)."),
    # Mixed sources (SPEC_142): 164 rows come from a county GIS the NZA terms do not cover.
    "si_zoning_districts": replace(
        _NZA, notes=_NZA.notes + " 164 rows come from nj_sussex_county_gis (Sussex County, NJ GIS), whose "
                                 "terms are not reviewed and are not covered by this block."),
}


# ── SPEC_163: vendored reference standards (app/ontology/standards/manifest.json) ─────────
# Not datasets (no DatasetSpec, no table): the ontology scorer's answer keys. One block per
# standard; redistribution here must agree with the manifest ``use`` (tested). Verified live
# 2026-10-08; quotes verbatim from the cited page (or file).
_HL7_LICENSE = "https://hl7.org/fhir/R4/license.html"
_NUCC_CSV_PAGE = "https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57"
_WHO_ICD_FAQ = ("https://cdn.who.int/media/docs/default-source/publishing-policies/copyright/"
                "who-faq-licensing-icd-10.pdf")
REFERENCE_STANDARD_RIGHTS: Dict[str, SourceRights] = {
    "fhir_r4": SourceRights(
        "CC0 1.0 (HL7 FHIR R4 licence); third-party terminologies inside the spec are not covered",
        "open", "none", "official", storage="allowed", commercial_use="allowed", license_url=CC0_URL,
        attribution="HL7® and FHIR® are trademarks of Health Level Seven International",
        notes="Never imply HL7 endorsement; do not call a derivative 'FHIR'. SNOMED CT, LOINC, CPT and ICD "
              "content referenced by the spec is Third Party IP and is not vendored (bindings keep only "
              "value-set URLs).",
        **_cite(_HL7_LICENSE,
                "This document is licensed under Creative Commons \"No Rights Reserved\" ( CC0 ). ... "
                "Acceptance of these License Terms does not grant any rights with respect to Third Party IP.",
                "high")),
    "us_core": SourceRights(
        "CC0 1.0 (HL7 US Core 9.0.0 package licence)", "open", "none", "official",
        storage="allowed", commercial_use="allowed", license_url=CC0_URL,
        notes="package.json and the ImplementationGuide resource both declare CC0-1.0.",
        **_cite("https://hl7.org/fhir/us/core/STU9/", "\"license\": \"CC0-1.0\"", "high")),
    "plan_net": SourceRights(
        "CC0 1.0 (HL7 Da Vinci PDex Plan-Net 1.2.0 package licence)", "open", "none", "official",
        storage="allowed", commercial_use="allowed", license_url=CC0_URL,
        notes="package.json and the ImplementationGuide resource both declare CC0-1.0.",
        **_cite("http://hl7.org/fhir/us/davinci-pdex-plan-net/STU1.2/", "\"license\": \"CC0-1.0\"", "high")),
    "omop_cdm": SourceRights(
        "Apache License 2.0 (OHDSI/CommonDataModel repository files, tag v5.4.3)", "attribution", "none",
        "official", storage="allowed", commercial_use="allowed",
        license_url="https://www.apache.org/licenses/LICENSE-2.0",
        attribution="OMOP Common Data Model v5.4, OHDSI (Apache License 2.0)",
        notes="Owner decision D3: CDM 5.4. Only the structural CSV columns are vendored; the OHDSI docs "
              "site prose is CC BY-SA 4.0 and is never copied. Athena vocabularies are separate (some "
              "carry EULAs) and are not vendored.",
        **_cite("https://raw.githubusercontent.com/OHDSI/CommonDataModel/"
                "746a15e0fb36a95ba6cc0993737f1273bbad92f2/DESCRIPTION",
                "License: Apache License 2.0", "high")),
    "nucc_taxonomy": SourceRights(
        "NUCC Health Care Provider Taxonomy Code Set, Copyright 2026 American Medical Association; "
        "commercial use needs a NUCC licence",
        "internal_only", "none", "official", storage="allowed", commercial_use="agreement_required",
        license_url=_NUCC_CSV_PAGE,
        attribution="Health Care Provider Taxonomy code set, National Uniform Claim Committee (NUCC). "
                    "Copyright 2026 American Medical Association.",
        notes="Owner decision D9 (2026-10-08): vendored for INTERNAL use now; the NUCC commercial licence "
              "is requested BEFORE any external release (customer export, published ontology release, "
              "public API) of taxonomy codes or descriptions. The further conditions an earlier draft "
              "listed (royalty-free, no modification, keep notices) are not on the CSV page and stay "
              "unverified. The vendored extract drops the Definition and Notes columns.",
        **_cite(_NUCC_CSV_PAGE,
                "For commercial use, including sales or licensing, a license must be obtained from this web "
                "site. Form to request license Copyright 2026 American Medical Association", "high")),
    "icd10cm": SourceRights(
        "ICD-10-CM FY2027 (NCHS clinical modification of WHO ICD-10); not verified as public domain",
        "internal_only", "none", "official", storage="allowed", license_url=_WHO_ICD_FAQ,
        notes="WHO holds the ICD-10 copyright; NCHS develops ICD-10-CM under WHO authorisation ('The World "
              "Health Organization (WHO), which owns and publishes ICD-10, authorized NCHS to develop "
              "ICD-10-CM', cdc.gov/nchs/icd/icd-10-cm). No public-domain statement found on the CDC or CMS "
              "pages. Vendored: chapters, blocks and 3-character category codes only. Confirm "
              "redistribution terms with NCHS before any external release (D10).",
        **_cite(_WHO_ICD_FAQ,
                "WHO is the copyright holder of ICD-10, and can grant licences for the use of ICD-10 "
                "worldwide for commercial and non-commercial uses.", "medium")),
    "hcpcs_l2": _usg("Centers for Medicare & Medicaid Services (HCPCS Level II)",
                     license_url="https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system",
                     notes="Level II only. Level I (CPT, AMA) is never vendored; the HCPCS D-series is CDT "
                           "(American Dental Association copyright) and is excluded from the vendored file.",
                     **_cite("https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system",
                             "CMS maintains HCPCS Level II codes, including decisions about additions, revisions, "
                             "and deletions to the codes.", "medium-high")),
    "cms_pos": _usg("Centers for Medicare & Medicaid Services (Place of Service code set)",
                    license_url="https://www.cms.gov/medicare/coding-billing/place-of-service-codes/code-sets",
                    notes="The page carries no licence statement; public domain rests on 17 U.S.C. §105.",
                    **_cite("https://www.cms.gov/medicare/coding-billing/place-of-service-codes/code-sets",
                            None, "medium")),
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
