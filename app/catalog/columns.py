"""
Column dictionary vocabulary (SPEC_137): ``ColumnSpec``, the glossary rules
and the PII masking helpers.

A ``ColumnSpec`` is one column of one table, keyed by ``(table, name)`` so a
table shared by several datasets (``pe_firms``, ``job_postings``) is
described once. Vocabularies are closed, like ``spec.py``: an unknown
semantic type, PII class, source or confidence raises at construction.

The glossary is the last-resort source of column text: a name rule
(``cik``, ``*_at``, ``zip_code`` ...) gives a generic description and, more
importantly, the ``semantic_type`` and column ``pii`` that the join-key
index and the sample masking rely on.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from app.catalog.identifiers import SEMANTIC_TYPES
from app.catalog.spec import PII_CLASSES

# where a column's text came from, highest precedence first
COLUMN_SOURCES = ("curated", "upstream", "bulk", "metadata", "model", "pg_comment", "glossary")
CONFIDENCE = ("high", "medium", "low")
SOURCE_CONFIDENCE = {
    "curated": "high", "upstream": "high", "bulk": "high",
    "metadata": "medium", "model": "medium", "pg_comment": "medium",
    "glossary": "low",
}
# text worth writing back to the database as COMMENT ON COLUMN
COMMENT_SOURCES = ("curated", "upstream", "bulk", "metadata", "model")

PII_RANK = {p: i for i, p in enumerate(PII_CLASSES)}   # none < business_contact < personal


def _check(cond: bool, where: str, msg: str) -> None:
    if not cond:
        raise ValueError(f"ColumnSpec {where}: {msg}")


@dataclass(frozen=True)
class ColumnSpec:
    table: str
    name: str
    pg_type: Optional[str] = None
    nullable: Optional[bool] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    example: Optional[str] = None
    semantic_type: Optional[str] = None
    pii: str = "none"
    source: Optional[str] = None           # of the description; None = undocumented
    confidence: Optional[str] = None
    upstream_url: Optional[str] = None

    def __post_init__(self) -> None:
        w = f"{self.table}.{self.name}"
        _check(bool(self.table) and bool(self.name), w, "table and name are required")
        _check(self.semantic_type is None or self.semantic_type in SEMANTIC_TYPES, w,
               f"semantic_type {self.semantic_type!r} not in {sorted(SEMANTIC_TYPES)}")
        _check(self.pii in PII_CLASSES, w, f"pii {self.pii!r} not in {PII_CLASSES}")
        _check(self.source is None or self.source in COLUMN_SOURCES, w,
               f"source {self.source!r} not in {COLUMN_SOURCES}")
        _check(self.confidence is None or self.confidence in CONFIDENCE, w,
               f"confidence {self.confidence!r} not in {CONFIDENCE}")
        _check(self.description is None or bool(self.description.strip()), w,
               "description must be None or non-empty")

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "name": self.name, "pg_type": self.pg_type, "nullable": self.nullable,
            "description": self.description, "unit": self.unit, "example": self.example,
            "semantic_type": self.semantic_type, "pii": self.pii, "source": self.source,
            "confidence": self.confidence,
        }
        if self.upstream_url:
            d["upstream_url"] = self.upstream_url
        return d


# ---------------------------------------------------------------------------
# Glossary: (regex, description, semantic_type, pii, unit) — first match wins
# ---------------------------------------------------------------------------

_B, _P = "business_contact", "personal"

GLOSSARY: Tuple[Tuple[str, str, Optional[str], str, Optional[str]], ...] = (
    # --- natural-person contact data (checked first: `signature_name` is a person, not an entity)
    (r"(^|_)(birth|dob|date_of_birth|ssn|social_security)(_|$)",
     "Personal data about a natural person (birth date / SSN).", None, _P, None),
    (r"(^|_)home_(address|phone|street|email)", "A natural person's home contact detail.", None, _P, None),
    (r"(^|_)e?mail$|(^|_)email(_address)?$|_email$",
     "Email address as published by the source.", None, _B, None),
    (r"(^|_)(phone|telephone|phone10|fax|mobile)(_number)?$|_(phone|fax)$",
     "Telephone or fax number as published by the source.", None, _B, None),
    (r"(^|_)(first|middle|last|given|family|full)_name$|^name_of_signer$",
     "Name of a natural person as published by the source.", None, _B, None),
    (r"(^|_)signature(_name|_title)?$", "Name or title of the natural person who signed the filing.",
     None, _B, None),
    # person names without a first_/last_ suffix, and a person's profile links / photo (SPEC_137 review)
    (r"^name_(first|middle|last|full)$|^(principal_name|principal_family|person_name|ceo_name|cfo_name|"
     r"officer_name|director_name|executive_name|contact_name|contact_person|inventor_name|signer_name)$"
     r"|_(contact|person)_name$",
     "Name of a natural person as published by the source.", None, _B, None),
    (r"^(linkedin|twitter|facebook|instagram)(_url|_id|_handle|_profile|_username)?$",
     "Social-media profile link or handle (of a natural person in people datasets) as published by the source.",
     None, _B, None),
    (r"^(photo|headshot|avatar|profile_photo|profile_image|profile_picture)(_url)?$|^personal_(website|url)$",
     "Photo or personal web page of a natural person.", None, _P, None),
    (r"(^|_)(street|street\d|street_address|address\d|address_line\d?|addr\d?)$",
     "Street address line as published by the source.", None, _B, None),
    # a bare `address` is usually a facility / site location, not a contact detail
    (r"^(address|location_address|site_address|facility_address)$",
     "Street address of the facility or site as published by the source.", None, "none", None),
    # --- row bookkeeping
    (r"^id$", "Surrogate row id assigned by the database; not stable across reloads.", None, "none", None),
    (r"^(created|updated|ingested|loaded|collected|fetched|scraped|inserted|modified|last_updated|synced|"
     r"processed|refreshed|last_seen|first_seen)_at$",
     "Row audit timestamp: when Nexdata wrote or last touched this row (not an upstream date).",
     None, "none", None),
    (r"^source_release_key$", "Key of the upstream bulk release (raw.source_release) this row was loaded from.",
     None, "none", None),
    (r"^(raw|raw_json|raw_data|payload|raw_payload)$|_json$|^raw_",
     "Raw upstream payload (JSON) kept as received.", None, "none", None),
    # --- identifiers
    (r"(^|_)cik$|^cik_(number|code)$", "SEC Central Index Key (CIK). Stored shapes vary; join via normalize_sql "
     "(10-digit zero-padded text).", "cik", "none", None),
    (r"(^|_)crd(_number|_no)?$", "FINRA/SEC Central Registration Depository (CRD) number.", "crd", "none", None),
    (r"(^|_)lei$", "Legal Entity Identifier (ISO 17442, 20 characters).", "lei", "none", None),
    (r"(^|_)cusip(6|9)?$", "CUSIP security identifier.", "cusip", "none", None),
    (r"(^|_)figi$", "OpenFIGI financial instrument identifier.", "figi", "none", None),
    (r"(^|_)npi$", "National Provider Identifier (10 digits, CMS NPPES).", "npi", "none", None),
    (r"(^|_)(ein|fein)$", "IRS Employer Identification Number (9 digits).", "ein", "none", None),
    (r"(^|_)uei$", "SAM.gov Unique Entity Identifier.", "uei", "none", None),
    (r"(^|_)duns(_number)?$", "Dun & Bradstreet DUNS number.", "duns", "none", None),
    (r"(^|_)accession(_number|_no|_num)?$", "SEC EDGAR accession number of the filing.",
     "accession_number", "none", None),
    (r"^(ticker|tickers|ticker_symbol|trading_symbol|issuer_trading_symbol)$", "Exchange ticker symbol.",
     "ticker", "none", None),
    (r"(^|_)series_id$", "Upstream time-series identifier.", "series_id", "none", None),
    (r"(^|_)naics(_code|_\d)?$", "NAICS industry classification code.", "naics", "none", None),
    (r"(^|_)sic(_code)?$", "Standard Industrial Classification (SIC) code.", "sic", "none", None),
    (r"^(county_fips|fips_county|county_fips_code|countyfp|geoid_county|fips_code_county)$",
     "County FIPS code (2-digit state + 3-digit county).", "fips_county", "none", None),
    (r"^(state_fips|fips_state|statefp|state_fips_code)$", "State FIPS code (2 digits).",
     "fips_state", "none", None),
    (r"(^|_)fips(_code)?$", "FIPS geographic code (2-digit state or 5-digit county, per table).",
     None, "none", None),
    (r"(^|_)(zip|zip5|zip_code|zipcode|postal_code|postcode)$", "US ZIP / postal code.", "zip5", "none", None),
    (r"^(lat|latitude)$|_(lat|latitude)$", "Latitude in decimal degrees (WGS84).", "latitude", "none", "degrees"),
    (r"^(lon|lng|long|longitude)$|_(lon|lng|longitude)$", "Longitude in decimal degrees (WGS84).",
     "longitude", "none", "degrees"),
    (r"^(state|state_code|state_abbr|state_abbreviation|state_or_country|state_usps)$|_state$",
     "US state (USPS code or name, as published).", "us_state", "none", None),
    (r"^(country_code|iso_country|country_iso|iso2|iso3|iso_code|country_iso3|country_iso2)$",
     "ISO 3166 country code.", "iso_country", "none", None),
    (r"(^|_)country(_name)?$", "Country name or code as published.", None, "none", None),
    (r"(^|_)city$", "City name.", None, "none", None),
    (r"(^|_)county(_name)?$", "County name.", None, "none", None),
    # --- time
    (r"^(period|period_date|as_of_date|as_of|record_date|observation_date|report_date|reporting_date|"
     r"period_end_date|period_start_date|period_end|period_start|period_of_report|date)$",
     "Date the observation or report refers to.", "period_date", "none", None),
    (r"^(filing_date|filed_at|filed_date|date_filed|submission_date|acceptance_datetime)$",
     "Date the filing was submitted to the regulator.", None, "none", None),
    (r"^(year|data_year|fiscal_year|calendar_year|report_year)$", "Year the observation refers to.",
     None, "none", None),
    (r"^(month|quarter|fiscal_period|period_month|period_quarter)$", "Month or quarter the observation refers to.",
     None, "none", None),
    (r"^(period_year|period_name|period_type|period_number|time_period|period_label)$",
     "Time period of the observation as published (year, name, type or number of the period).", None, "none", None),
    (r"^record_(fiscal|calendar)_(year|quarter|month|day)$",
     "Calendar or fiscal part of the record date, as published by Treasury FiscalData.", None, "none", None),
    (r"^realtime_(start|end)$", "FRED/ALFRED real-time window in which this value was the current vintage.",
     None, "none", None),
    (r"^(retrieved_at|ingestion_timestamp|collected_date|fetched_date|last_updated|last_updated_date)$",
     "When Nexdata retrieved or last refreshed this row (not an upstream date).", None, "none", None),
    (r"^snapshot_date$", "Date the snapshot was taken.", None, "none", None),
    (r"^frequency$", "Observation frequency as published (e.g. A, Q, M, W, D).", None, "none", None),
    # --- geography
    (r"^(geo_name|geography_name|region_name|state_name|area_name|place_name)$",
     "Name of the geography the row describes.", None, "none", None),
    (r"^(geo_id|geography_id|geoid|area_code|region_code)$",
     "Geographic identifier as published (FIPS / Census GEOID / area code, per table).", None, "none", None),
    (r"^(geometry|geom|geometry_geojson|geojson|wkt|shape)$", "Geometry as published (GeoJSON / WKT).",
     None, "none", None),
    (r"^county_code$", "County code as published (3-digit county part of the FIPS code unless noted).",
     None, "none", None),
    # --- series metadata
    (r"^(series_title|indicator_name|series_name|measure_name)$", "Human-readable name of the series or indicator.",
     None, "none", None),
    (r"^(indicator_code|indicator_id)$", "Upstream indicator code.", "series_id", "none", None),
    (r"^(unit|units|unit_measure|unit_of_measure|cl_unit)$", "Unit of measure of the value, as published.",
     None, "none", None),
    (r"^unit_mult$", "Unit multiplier (power of ten) the publisher applies to the value.", None, "none", None),
    (r"^(footnotes|footnote_codes|footnote)$", "Footnote codes or text attached by the publisher.", None, "none", None),
    (r"^(data_value|value_numeric|obs_value)$", "Observation value (units per the series or table).",
     None, "none", None),
    (r"^src_line_nbr$", "Line number of the row in the published source table.", None, "none", None),
    (r"^hs_code$", "Harmonized System (HS) commodity code.", None, "none", None),
    (r"^(form_type|filing_type)$", "Form type of the filing as published (e.g. 10-K, 8-K, D).", None, "none", None),
    (r"^(file_number|film_number)$", "SEC EDGAR file or film number.", None, "none", None),
    (r"^primary_document$", "File name of the filing's primary document.", None, "none", None),
    (r"^(status|record_status)$", "Status as published by the source.", None, "none", None),
    (r"^population$", "Population count.", None, "none", None),
    (r"^(employee_count|employees|num_employees)$", "Number of employees.", None, "none", None),
    (r"^company_id$", "Id of the company row this record belongs to (within the same source family).",
     None, "none", None),
    # --- measures
    (r"_usd$|^amount_usd$", "Money amount in US dollars.", "amount_usd", "none", "USD"),
    (r"(_pct|_percent|_percentage|^pct_.*)$", "Percentage.", "pct", "none", "percent"),
    # --- descriptive
    (r"^(website|url|homepage|domain|source_url|website_url|filing_url|document_url|link)$|_url$",
     "Web address as published by the source.", None, "none", None),
    (r"^(source|data_source|source_name|source_system|provider)$",
     "Provenance: which upstream source or collector wrote this row.", None, "none", None),
    (r"^(name|entity_name|company_name|firm_name|legal_name|business_name|issuer_name|organization_name|"
     r"org_name|fund_name)$", "Name of the organisation / entity as published by the source.", None, "none", None),
    (r"^(title|description|notes|remarks|comments)$", "Free text as published by the source.", None, "none", None),
    (r"^value$", "Observation value (units per the series or table).", None, "none", None),
)

_GLOSSARY_RX = tuple((re.compile(rx), d, st, pii, unit) for rx, d, st, pii, unit in GLOSSARY)

# PLAN_088: a column named like personal data must not be pii='none'
PII_NAME_RE = re.compile(r"email|phone|fax|first_name|last_name|name_first|name_last|signature|street|birth|"
                         r"linkedin|twitter|photo|principal_name|person_name|contact_name|ceo_name|officer_name")
# ... unless its type cannot hold contact data (signature_date, has_email, email_count)
_NON_CONTACT_TYPE = re.compile(
    r"^(bool|boolean|date|timestamp|time|int|integer|bigint|smallint|numeric|decimal|real|double|float)", re.I)
_NON_CONTACT_NAME = re.compile(r"(_date|_at|_count|_flag|_verified|_confidence|_status|_type|_source)$|^(is|has)_")


def classify(name: str) -> Optional[Dict[str, Any]]:
    """Glossary entry for a column name, or None."""
    n = (name or "").lower()
    for rx, desc, st, pii, unit in _GLOSSARY_RX:
        if rx.search(n):
            return {"description": desc, "semantic_type": st, "pii": pii, "unit": unit}
    return None


def pii_name_lint_applies(name: str, pg_type: Optional[str]) -> bool:
    """True when the PLAN_088 PII-name rule requires ``pii != 'none'``."""
    n = (name or "").lower()
    if not PII_NAME_RE.search(n) or _NON_CONTACT_NAME.search(n):
        return False
    return not (pg_type and _NON_CONTACT_TYPE.match(pg_type.strip()))


# In a dataset classed 'personal' every text column is masked for non-admins unless it is one of
# these clearly non-person columns, carries an identifier semantic type, or a curated row sets
# pii='none' explicitly (deny by default: a person column the name rules miss stays masked).
_PERSONAL_SAFE_NAME = re.compile(
    r"^id$|_(id|uuid|key)$|_(type|status|source|confidence|code|category|method|level|count)$"
    r"|^(status|source|data_source|confidence|category|country|city|state|state_province|region|currency|"
    r"industry|sector|seniority|department|function|title|job_title|role|position|company_name|firm_name|"
    r"entity_name|org_name|organization_name|legal_name|fund_name|ticker)$")


def personal_safe(name: str, pg_type: Optional[str], semantic_type: Optional[str] = None,
                  curated_none: bool = False) -> bool:
    """True when a pii='none' column of a 'personal' dataset may be shown to non-admins."""
    if curated_none or semantic_type:
        return True
    if pg_type and _NON_CONTACT_TYPE.match(pg_type.strip()):
        return True
    return bool(_PERSONAL_SAFE_NAME.search((name or "").lower()))


def max_pii(values) -> str:
    return max(values, key=lambda p: PII_RANK[p], default="none")


# ---------------------------------------------------------------------------
# Masking (sample endpoint)
# ---------------------------------------------------------------------------

_JSONISH = re.compile(r"^(json|jsonb)|\[\]$|^array|^user-defined|^geometry|^geography", re.I)
_PHONEISH = re.compile(r"phone|fax|mobile|tel")
_EMAILISH = re.compile(r"e?mail")


def is_jsonish(pg_type: Optional[str]) -> bool:
    return bool(pg_type and _JSONISH.search(pg_type.strip()))


def mask_value(value: Any, name: str, pii: str) -> Any:
    """Non-admin view of one value: personal -> None; business_contact -> partial."""
    if value is None or pii == "none":
        return value
    if pii == "personal":
        return None
    s = str(value)
    n = (name or "").lower()
    if _EMAILISH.search(n) and "@" in s:
        local, _, domain = s.partition("@")
        return f"{local[:1]}***@{domain}"
    if _PHONEISH.search(n):
        digits = re.sub(r"\D", "", s)
        return f"***{digits[-4:]}" if len(digits) >= 4 else "***"
    return f"{s[:1]}***" if s else s
