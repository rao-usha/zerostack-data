"""
EDGAR submissions.zip parsing (SPEC_111). Pure: no DB, no HTTP.

Each primary member ``CIK##########.json`` is one filer document. Overflow
members ``CIK##########-submissions-NNN.json`` hold only older filing arrays
with no filer header, so they are skipped.

Rows are tuples in the order of the ``*_COLUMNS`` lists, with values already
formatted for COPY (ISO date strings, TEXT[] literals, bools).
"""

from __future__ import annotations

import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from app.ingest.bulk.sec_edgar_submissions import norm

logger = logging.getLogger(__name__)

EIGHT_K_FORMS = frozenset({"8-K", "8-K/A"})
LOOKBACK_YEARS = 3
# Key columns can't be NULL; a former name with no 'from' date gets this sentinel.
UNKNOWN_FROM_DATE = "1900-01-01"

_PRIMARY_RE = re.compile(r"(?:^|/)CIK\d{10}\.json$")
_OVERFLOW_RE = re.compile(r"(?:^|/)CIK\d{10}-submissions-\d+\.json$")

_US_STATES = frozenset("""
AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO
MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY
AS GU MP PR VI UM
""".split())

_ADDR_FIELDS = ("street1", "street2", "city", "state_or_country", "zip", "zip5", "state2")

FILER_COLUMNS: List[str] = [
    "cik", "name", "entity_type", "sic", "sic_description", "owner_org", "category",
    "ein", "ein_raw", "lei", "state_of_incorporation", "fiscal_year_end",
    "tickers", "exchanges", "website", "investor_website", "phone", "phone10",
    *[f"biz_{f}" for f in _ADDR_FIELDS], "biz_country", "biz_is_foreign",
    *[f"mail_{f}" for f in _ADDR_FIELDS],
    "insider_transaction_for_owner_exists", "insider_transaction_for_issuer_exists", "flags",
    "former_name_count", "recent_filing_count", "latest_filing_date",
    "earliest_recent_filing_date", "latest_form",
    "source_release_key", "loaded_at",
]

FILER_TYPES: Dict[str, str] = {c: "TEXT" for c in FILER_COLUMNS}
FILER_TYPES.update({
    "tickers": "TEXT[]", "exchanges": "TEXT[]",
    "biz_is_foreign": "BOOLEAN",
    "insider_transaction_for_owner_exists": "BOOLEAN",
    "insider_transaction_for_issuer_exists": "BOOLEAN",
    "former_name_count": "INTEGER", "recent_filing_count": "INTEGER",
    "latest_filing_date": "DATE", "earliest_recent_filing_date": "DATE",
    "loaded_at": "TIMESTAMP",
})

FORMER_NAME_COLUMNS: List[str] = ["cik", "name", "from_date", "to_date", "source_release_key", "loaded_at"]
FORMER_NAME_TYPES: Dict[str, str] = {
    "cik": "TEXT", "name": "TEXT", "from_date": "DATE", "to_date": "DATE",
    "source_release_key": "TEXT", "loaded_at": "TIMESTAMP",
}

EIGHT_K_COLUMNS: List[str] = [
    "accession_number", "cik", "form", "filing_date", "report_date", "acceptance_datetime",
    "items", "primary_document", "primary_doc_description", "size", "file_number",
    "film_number", "source_release_key", "loaded_at",
]
EIGHT_K_TYPES: Dict[str, str] = {c: "TEXT" for c in EIGHT_K_COLUMNS}
EIGHT_K_TYPES.update({
    "filing_date": "DATE", "report_date": "DATE", "acceptance_datetime": "TIMESTAMPTZ",
    "size": "BIGINT", "loaded_at": "TIMESTAMP",
})


@dataclass
class MemberStats:
    filers: int = 0
    overflow_skipped: int = 0
    bad_json: int = 0
    other_skipped: int = 0


def is_primary_member(name: str) -> bool:
    return bool(_PRIMARY_RE.search(name))


def iter_filer_docs(path: Path, stats: Optional[MemberStats] = None) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Yield (member_name, doc) one member at a time; never holds more than one doc."""
    stats = stats if stats is not None else MemberStats()
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if _OVERFLOW_RE.search(name):
                stats.overflow_skipped += 1
                continue
            if not is_primary_member(name):
                stats.other_skipped += 1
                continue
            try:
                with zf.open(info) as fh:
                    doc = json.load(fh)
            except (ValueError, UnicodeDecodeError) as e:
                stats.bad_json += 1
                logger.warning(f"[sec_edgar_submissions] unparseable member {name}: {e}")
                continue
            if not isinstance(doc, dict):
                stats.bad_json += 1
                continue
            if not doc.get("cik"):
                doc["cik"] = re.search(r"CIK(\d{10})", name).group(1)
            stats.filers += 1
            yield name, doc


def cik10(value) -> Optional[str]:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits or int(digits) == 0:
        return None
    return f"{int(digits):010d}"


def cutoff_date(snapshot: date, years: int = LOOKBACK_YEARS) -> date:
    try:
        return snapshot.replace(year=snapshot.year - years)
    except ValueError:  # Feb 29 -> Feb 28
        return snapshot.replace(year=snapshot.year - years, day=28)


def _flag(value) -> Optional[bool]:
    if value is None or value == "":
        return None
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return None


def _iso(value) -> Optional[str]:
    d = norm.parse_date(value)
    return d.isoformat() if d else None


def _country_from_addr(addr: Dict[str, Any]) -> Tuple[Optional[str], Optional[bool]]:
    """(country, is_foreign) for one address block.

    isForeignLocation is unreliable (0 for some Israeli addresses), so an explicit
    country wins, then stateOrCountry: a US state means domestic, any other code
    is an EDGAR country code.
    """
    if not addr:
        return None, None

    def tail(v):
        return (v.rsplit(",", 1)[-1].strip() or None) if v else None

    country, code = norm.s(addr.get("country")), norm.s(addr.get("countryCode"))
    soc, desc = norm.s(addr.get("stateOrCountry")), norm.s(addr.get("stateOrCountryDescription"))
    if country or code:
        return tail(country), True
    if soc in _US_STATES:
        return "US", False
    if soc:
        return tail(desc), True
    return None, None


def _country(doc: Dict[str, Any]) -> Tuple[Optional[str], Optional[bool]]:
    addrs = doc.get("addresses") or {}
    for block in ("business", "mailing"):
        got = _country_from_addr(addrs.get(block) or {})
        if got != (None, None):
            return got
    return None, None


def _addr(block: Dict[str, Any]) -> List[Any]:
    soc = norm.s(block.get("stateOrCountry"))
    zipc = norm.s(block.get("zipCode"))
    return [
        norm.s(block.get("street1")), norm.s(block.get("street2")), norm.s(block.get("city")),
        soc, zipc, norm.zip5(zipc), norm.state2(soc) if soc in _US_STATES else None,
    ]


def _former_names(doc: Dict[str, Any]) -> List[Tuple[str, Optional[str], Optional[str]]]:
    out = []
    for fn in doc.get("formerNames") or []:
        if not isinstance(fn, dict):
            continue
        name = norm.s(fn.get("name"))
        if name:
            out.append((name, _iso(fn.get("from")), _iso(fn.get("to"))))
    return out


def filer_row(doc: Dict[str, Any], release_key: str, loaded_at: Optional[str] = None) -> Optional[tuple]:
    cik = cik10(doc.get("cik"))
    if cik is None:
        return None
    addrs = doc.get("addresses") or {}
    recent = (doc.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    dates = [d for d in (recent.get("filingDate") or []) if d]
    country, is_foreign = _country(doc)
    ein_raw = norm.s(doc.get("ein"))
    phone = norm.s(doc.get("phone"))
    values = [
        cik, norm.s(doc.get("name")), norm.s(doc.get("entityType")), norm.s(doc.get("sic")),
        norm.s(doc.get("sicDescription")), norm.s(doc.get("ownerOrg")), norm.s(doc.get("category")),
        norm.clean_ein(ein_raw), ein_raw, norm.s(doc.get("lei")),
        norm.s(doc.get("stateOfIncorporation")), norm.s(doc.get("fiscalYearEnd")),
        norm.pg_text_array(doc.get("tickers")), norm.pg_text_array(doc.get("exchanges")),
        norm.s(doc.get("website")), norm.s(doc.get("investorWebsite")), phone, norm.phone10(phone),
        *_addr(addrs.get("business") or {}), country, is_foreign,
        *_addr(addrs.get("mailing") or {}),
        _flag(doc.get("insiderTransactionForOwnerExists")),
        _flag(doc.get("insiderTransactionForIssuerExists")),
        norm.s(doc.get("flags")),
        len(_former_names(doc)), len(forms) or None,
        max(dates) if dates else None, min(dates) if dates else None,
        norm.s(forms[0]) if forms else None,
        release_key, loaded_at,
    ]
    return tuple(values)


def former_name_rows(doc: Dict[str, Any], release_key: str, loaded_at: Optional[str] = None) -> List[tuple]:
    cik = cik10(doc.get("cik"))
    if cik is None:
        return []
    return [(cik, name, frm or UNKNOWN_FROM_DATE, to, release_key, loaded_at)
            for name, frm, to in _former_names(doc)]


def _col(recent: Dict[str, Any], key: str, i: int):
    arr = recent.get(key) or []
    return arr[i] if i < len(arr) else None


def eight_k_rows(doc: Dict[str, Any], cutoff: date, release_key: str,
                 loaded_at: Optional[str] = None) -> List[tuple]:
    """8-K / 8-K/A rows from filings.recent filed on or after ``cutoff``."""
    cik = cik10(doc.get("cik"))
    recent = (doc.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    if cik is None or not forms:
        return []
    cutoff_iso = cutoff.isoformat()
    out = []
    for i, form in enumerate(forms):
        if form not in EIGHT_K_FORMS:
            continue
        filing_date = _iso(_col(recent, "filingDate", i))
        accession = norm.s(_col(recent, "accessionNumber", i))
        if not accession or not filing_date or filing_date < cutoff_iso:
            continue
        size = _col(recent, "size", i)
        out.append((
            accession, cik, form, filing_date, _iso(_col(recent, "reportDate", i)),
            norm.s(_col(recent, "acceptanceDateTime", i)), norm.s(_col(recent, "items", i)),
            norm.s(_col(recent, "primaryDocument", i)), norm.s(_col(recent, "primaryDocDescription", i)),
            int(size) if isinstance(size, (int, float)) or (isinstance(size, str) and size.isdigit()) else None,
            norm.s(_col(recent, "fileNumber", i)), norm.s(_col(recent, "filmNumber", i)),
            release_key, loaded_at,
        ))
    return out
