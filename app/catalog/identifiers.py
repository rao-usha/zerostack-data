"""
Semantic types for catalog columns (SPEC_137).

A closed vocabulary, like ``spec.KINDS``: a column tagged with a type that is
not listed here fails at import. Each identifier type carries its canonical
format and a ``normalize_sql`` template that turns any stored shape (varchar,
text, integer, unpadded) into that format, so two datasets can be joined
without knowing how each one happened to store the key:

    SELECT ... FROM core.identifier i
    JOIN sec_filers f
      ON lpad(ltrim(i.id_value::text,'0'),10,'0') = lpad(ltrim(f.cik::text,'0'),10,'0')

CIK is stored as varchar/text/integer/bigint across the warehouse and
``core.identifier`` holds it unpadded; the expression, not a data migration,
is the supported way to join (PLAN_088 open decision 9).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class SemanticType:
    name: str
    label: str
    canonical_format: str
    normalize_sql: Optional[str] = None   # template with {col}; None = not a join key
    specificity: int = 0                  # join ranking: 10 = one real-world entity
    join_key: bool = False


def _t(name, label, fmt, sql=None, spec=0, join=None) -> SemanticType:
    return SemanticType(name, label, fmt, sql, spec, bool(sql) if join is None else join)


_DIGITS = "regexp_replace({col}::text,'\\D','','g')"

SEMANTIC_TYPES: Dict[str, SemanticType] = {t.name: t for t in (
    _t("cik", "SEC Central Index Key", "10-digit zero-padded text",
       "lpad(ltrim({col}::text,'0'),10,'0')", 10),
    _t("crd", "FINRA/SEC Central Registration Depository number", "digits, no leading zeros",
       "ltrim({col}::text,'0')", 10),
    _t("lei", "Legal Entity Identifier (ISO 17442)", "20 upper-case alphanumerics",
       "upper(trim({col}::text))", 10),
    _t("npi", "National Provider Identifier", "10 digits", "trim({col}::text)", 10),
    _t("cusip", "CUSIP security identifier", "9 upper-case alphanumerics",
       "upper(left(trim({col}::text),9))", 9),
    _t("figi", "OpenFIGI identifier", "12 upper-case alphanumerics", "upper(trim({col}::text))", 9),
    _t("ein", "IRS Employer Identification Number", "9 digits",
       f"lpad({_DIGITS},9,'0')", 9),
    _t("uei", "SAM.gov Unique Entity Identifier", "12 upper-case alphanumerics",
       "upper(trim({col}::text))", 9),
    _t("duns", "Dun & Bradstreet DUNS number", "9 digits", f"lpad({_DIGITS},9,'0')", 9),
    _t("accession_number", "SEC EDGAR accession number", "18 digits, no dashes", _DIGITS, 8),
    _t("ticker", "Exchange ticker symbol", "upper-case", "upper(trim({col}::text))", 6),
    _t("series_id", "Upstream time-series id (FRED, BLS, EIA ...)", "as published",
       "trim({col}::text)", 5),
    _t("fips_county", "County FIPS code (state + county)", "5-digit zero-padded text",
       "lpad({col}::text,5,'0')", 4),
    _t("naics", "NAICS industry code", "2-6 digits",
       f"left({_DIGITS},6)", 3),
    _t("sic", "SIC industry code", "4-digit zero-padded text", f"lpad({_DIGITS},4,'0')", 3),
    _t("zip5", "US ZIP code", "5-digit zero-padded text", f"lpad(left({_DIGITS},5),5,'0')", 2),
    _t("fips_state", "State FIPS code", "2-digit zero-padded text", "lpad({col}::text,2,'0')", 1),
    _t("iso_country", "ISO 3166 country code", "upper-case alpha-2 or alpha-3",
       "upper(trim({col}::text))", 1),
    # too coarse to be offered as a join (PLAN_088: `state` is excluded)
    _t("us_state", "US state (USPS code or name, as published)", "as published",
       "upper(trim({col}::text))", 0, join=False),
    _t("latitude", "Latitude", "decimal degrees, WGS84"),
    _t("longitude", "Longitude", "decimal degrees, WGS84"),
    _t("period_date", "Date the observation or report refers to", "ISO date"),
    _t("amount_usd", "Money amount in US dollars", "numeric, USD"),
    _t("pct", "Percentage", "numeric, 0-100 unless noted"),
)}


def get_type(name: str) -> SemanticType:
    try:
        return SEMANTIC_TYPES[name]
    except KeyError:
        raise ValueError(f"unknown semantic_type {name!r}; allowed: {sorted(SEMANTIC_TYPES)}")


def normalize_expr(semantic_type: Optional[str], col_sql: str) -> Optional[str]:
    """``normalize_sql`` with ``{col}`` replaced by ``col_sql`` (already quoted)."""
    if not semantic_type:
        return None
    tmpl = get_type(semantic_type).normalize_sql
    return tmpl.replace("{col}", col_sql) if tmpl else None


def join_types() -> Dict[str, SemanticType]:
    return {k: v for k, v in SEMANTIC_TYPES.items() if v.join_key}
