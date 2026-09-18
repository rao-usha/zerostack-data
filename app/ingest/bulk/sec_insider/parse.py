"""
Pure parsing for the SEC Insider Transactions Data Sets (SPEC_110). No DB access.

Quarterly ``YYYYqN_form345.zip`` files contain tab-delimited UTF-8 TSVs with a
header row (SUBMISSION, REPORTINGOWNER, NONDERIV_TRANS, NONDERIV_HOLDING,
DERIV_TRANS, DERIV_HOLDING, FOOTNOTES, OWNER_SIGNATURE). Dates are DD-MON-YYYY,
numbers may be blank, flags mix 0/1 and true/false.

Iterators stream one zip member at a time and yield tuples in the order of the
matching ``*_COLUMNS`` list, ready for ``copy_loader.copy_rows``.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import sys
import zipfile
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:  # pragma: no cover - 32-bit C long
    csv.field_size_limit(2**31 - 1)

DEFAULT_QUARTERS = 8  # Lean disk budget (PLAN_082)

# ---------------------------------------------------------------------------
# Target column layouts (name, postgres type). Tuples are yielded in this order.
# ---------------------------------------------------------------------------

FILING_COLUMNS: List[Tuple[str, str]] = [
    ("accession_number", "TEXT"),
    ("filing_date", "DATE"),
    ("period_of_report", "DATE"),
    ("date_of_orig_sub", "DATE"),
    ("document_type", "TEXT"),
    ("issuer_cik", "TEXT"),
    ("issuer_name", "TEXT"),
    ("issuer_trading_symbol", "TEXT"),
    ("no_securities_owned", "BOOLEAN"),
    ("not_subject_sec16", "BOOLEAN"),
    ("form3_holdings_reported", "BOOLEAN"),
    ("form4_trans_reported", "BOOLEAN"),
    ("aff10b5one", "BOOLEAN"),
    ("remarks", "TEXT"),
    ("source_release_key", "TEXT"),
]

OWNER_COLUMNS: List[Tuple[str, str]] = [
    ("accession_number", "TEXT"),
    ("rptowner_cik", "TEXT"),
    ("rptowner_name", "TEXT"),
    ("relationship", "TEXT"),
    ("is_director", "BOOLEAN"),
    ("is_officer", "BOOLEAN"),
    ("is_ten_percent_owner", "BOOLEAN"),
    ("is_other", "BOOLEAN"),
    ("officer_title", "TEXT"),
    ("other_text", "TEXT"),
    ("rptowner_street1", "TEXT"),
    ("rptowner_street2", "TEXT"),
    ("rptowner_city", "TEXT"),
    ("rptowner_state", "TEXT"),
    ("rptowner_zipcode", "TEXT"),
    ("rptowner_state_desc", "TEXT"),
    ("file_number", "TEXT"),
    ("source_release_key", "TEXT"),
]

TRANSACTION_COLUMNS: List[Tuple[str, str]] = [
    ("accession_number", "TEXT"),
    ("table_type", "TEXT"),
    ("trans_sk", "BIGINT"),
    ("security_title", "TEXT"),
    ("trans_date", "DATE"),
    ("deemed_execution_date", "DATE"),
    ("trans_form_type", "TEXT"),
    ("trans_code", "TEXT"),
    ("equity_swap_involved", "BOOLEAN"),
    ("trans_timeliness", "TEXT"),
    ("trans_shares", "NUMERIC"),
    ("trans_pricepershare", "NUMERIC"),
    ("trans_total_value", "NUMERIC"),
    ("trans_acquired_disp_cd", "TEXT"),
    ("shrs_ownd_folwng_trans", "NUMERIC"),
    ("valu_ownd_folwng_trans", "NUMERIC"),
    ("direct_indirect_ownership", "TEXT"),
    ("nature_of_ownership", "TEXT"),
    ("conv_exercise_price", "NUMERIC"),
    ("exercise_date", "DATE"),
    ("expiration_date", "DATE"),
    ("undlyng_sec_title", "TEXT"),
    ("undlyng_sec_shares", "NUMERIC"),
    ("undlyng_sec_value", "NUMERIC"),
    ("footnote_refs", "JSONB"),
    ("source_release_key", "TEXT"),
]

FOOTNOTE_COLUMNS: List[Tuple[str, str]] = [
    ("accession_number", "TEXT"),
    ("footnote_id", "TEXT"),
    ("footnote_txt", "TEXT"),
    ("source_release_key", "TEXT"),
]

# zip member -> table_type, surrogate-key column
TRANSACTION_MEMBERS: List[Tuple[str, str, str]] = [
    ("NONDERIV_TRANS.tsv", "nonderiv_trans", "NONDERIV_TRANS_SK"),
    ("NONDERIV_HOLDING.tsv", "nonderiv_holding", "NONDERIV_HOLDING_SK"),
    ("DERIV_TRANS.tsv", "deriv_trans", "DERIV_TRANS_SK"),
    ("DERIV_HOLDING.tsv", "deriv_holding", "DERIV_HOLDING_SK"),
]

# target column -> accepted source headers (first non-empty wins)
_TRANS_SOURCES: Dict[str, Tuple[str, ...]] = {
    "security_title": ("SECURITY_TITLE",),
    "trans_date": ("TRANS_DATE",),
    "deemed_execution_date": ("DEEMED_EXECUTION_DATE",),
    "trans_form_type": ("TRANS_FORM_TYPE",),
    "trans_code": ("TRANS_CODE",),
    "equity_swap_involved": ("EQUITY_SWAP_INVOLVED",),
    "trans_timeliness": ("TRANS_TIMELINESS",),
    "trans_shares": ("TRANS_SHARES",),
    "trans_pricepershare": ("TRANS_PRICEPERSHARE",),
    "trans_total_value": ("TRANS_TOTAL_VALUE",),
    "trans_acquired_disp_cd": ("TRANS_ACQUIRED_DISP_CD",),
    "shrs_ownd_folwng_trans": ("SHRS_OWND_FOLWNG_TRANS",),
    "valu_ownd_folwng_trans": ("VALU_OWND_FOLWNG_TRANS",),
    "direct_indirect_ownership": ("DIRECT_INDIRECT_OWNERSHIP",),
    "nature_of_ownership": ("NATURE_OF_OWNERSHIP",),
    "conv_exercise_price": ("CONV_EXERCISE_PRICE",),
    "exercise_date": ("EXERCISE_DATE", "EXCERCISE_DATE"),  # DERIV_TRANS misspells it
    "expiration_date": ("EXPIRATION_DATE",),
    "undlyng_sec_title": ("UNDLYNG_SEC_TITLE",),
    "undlyng_sec_shares": ("UNDLYNG_SEC_SHARES",),
    "undlyng_sec_value": ("UNDLYNG_SEC_VALUE",),
}

# footnote-reference header -> key in footnote_refs
_FN_ALIASES = {
    "EQUITY_SWAP_TRANS_CD_FN": "equity_swap_involved",
    "EQUITY_SWAP_INVOLVED_FN": "equity_swap_involved",
    "EXCERCISE_DATE_FN": "exercise_date",
}

_TYPES = {name: typ for cols in (FILING_COLUMNS, OWNER_COLUMNS, TRANSACTION_COLUMNS, FOOTNOTE_COLUMNS)
          for name, typ in cols}

# ---------------------------------------------------------------------------
# Scalar parsers
# ---------------------------------------------------------------------------

_MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1)}
_DMY_RE = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$")
_TRUE = {"1", "true", "t", "y", "yes"}
_FALSE = {"0", "false", "f", "n", "no"}


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = value.replace("\x00", "").strip()
    return s or None


def parse_sec_date(value: Optional[str]) -> Optional[date]:
    """'01-JAN-2024' (any case) or ISO 'YYYY-MM-DD' -> date; blank/invalid -> None."""
    s = clean_text(value)
    if not s:
        return None
    m = _DMY_RE.match(s)
    try:
        if m:
            month = _MONTHS.get(m.group(2).upper())
            if month is None:
                return None
            return date(int(m.group(3)), month, int(m.group(1)))
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_number(value: Optional[str]) -> Optional[Decimal]:
    s = clean_text(value)
    if not s:
        return None
    try:
        d = Decimal(s.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None
    if not d.is_finite():
        return None
    return d


def parse_int(value: Optional[str]) -> Optional[int]:
    d = parse_number(value)
    if d is None or d != d.to_integral_value():
        return None
    return int(d)


def parse_flag(value: Optional[str]) -> Optional[bool]:
    s = clean_text(value)
    if not s:
        return None
    s = s.lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return None


def parse_relationship(value: Optional[str]) -> Tuple[Optional[bool], Optional[bool], Optional[bool], Optional[bool]]:
    """'Director,Officer,TenPercentOwner,Other' -> (director, officer, ten_percent_owner, other)."""
    s = clean_text(value)
    if not s:
        return (None, None, None, None)
    parts = {re.sub(r"[^a-z0-9]", "", p.lower()) for p in s.split(",")}
    return ("director" in parts, "officer" in parts,
            bool(parts & {"tenpercentowner", "10percentowner"}), "other" in parts)


def normalize_cik(value: Optional[str]) -> Optional[str]:
    s = clean_text(value)
    if s and s.isdigit():
        return s.zfill(10)
    return s


# ---------------------------------------------------------------------------
# Index page
# ---------------------------------------------------------------------------

_HREF_RE = re.compile(r"""href\s*=\s*["']([^"']*?(\d{4})q([1-4])_form345\.zip)["']""", re.IGNORECASE)


def parse_index(html: str, base_url: str) -> List[Tuple[str, str]]:
    """All (release_key 'YYYYqN', absolute url) pairs, oldest first, one per quarter."""
    found: Dict[str, str] = {}
    for m in _HREF_RE.finditer(html or ""):
        key = f"{m.group(2)}q{m.group(3)}"
        found.setdefault(key, urljoin(base_url, m.group(1)))
    return sorted(found.items())


def quarter_end(release_key: str) -> date:
    year, q = int(release_key[:4]), int(release_key[5])
    month = q * 3
    return date(year, month, 31 if month in (3, 12) else 30)


def select_releases(pairs: Sequence[Tuple[str, str]], since: Optional[date],
                    default_quarters: int = DEFAULT_QUARTERS) -> List[Tuple[str, str]]:
    pairs = sorted(pairs)
    if since is None:
        return list(pairs[-default_quarters:]) if default_quarters > 0 else []
    return [(k, u) for k, u in pairs if quarter_end(k) >= since]


# ---------------------------------------------------------------------------
# Zip member readers
# ---------------------------------------------------------------------------

def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    want = name.lower()
    for info in zf.infolist():
        if info.filename.rsplit("/", 1)[-1].lower() == want:
            return info.filename
    return None


def iter_tsv(zf: zipfile.ZipFile, member: str) -> Iterator[Dict[str, str]]:
    """Stream a TSV member as dicts with upper-cased, stripped headers."""
    actual = _find_member(zf, member)
    if actual is None:
        logger.warning(f"[sec_insider] zip has no {member}")
        return
    with zf.open(actual) as raw:
        text_stream = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
        reader = csv.DictReader(text_stream, delimiter="\t")
        if reader.fieldnames is None:
            return
        reader.fieldnames = [(h or "").replace("﻿", "").strip().upper() for h in reader.fieldnames]
        for row in reader:
            yield row


def _convert(name: str, raw: Optional[str]):
    typ = _TYPES[name]
    if typ == "DATE":
        return parse_sec_date(raw)
    if typ == "NUMERIC":
        return parse_number(raw)
    if typ == "BOOLEAN":
        return parse_flag(raw)
    if typ == "BIGINT":
        return parse_int(raw)
    return clean_text(raw)


def _first(row: Dict[str, str], headers: Sequence[str]) -> Optional[str]:
    for h in headers:
        v = row.get(h)
        if v is not None and v.strip() != "":
            return v
    return None


def iter_filings(zf: zipfile.ZipFile, release_key: str) -> Iterator[tuple]:
    sources = {
        "accession_number": "ACCESSION_NUMBER", "filing_date": "FILING_DATE",
        "period_of_report": "PERIOD_OF_REPORT", "date_of_orig_sub": "DATE_OF_ORIG_SUB",
        "document_type": "DOCUMENT_TYPE", "issuer_name": "ISSUERNAME",
        "issuer_trading_symbol": "ISSUERTRADINGSYMBOL", "no_securities_owned": "NO_SECURITIES_OWNED",
        "not_subject_sec16": "NOT_SUBJECT_SEC16", "form3_holdings_reported": "FORM3_HOLDINGS_REPORTED",
        "form4_trans_reported": "FORM4_TRANS_REPORTED", "aff10b5one": "AFF10B5ONE", "remarks": "REMARKS",
    }
    for row in iter_tsv(zf, "SUBMISSION.tsv"):
        acc = clean_text(row.get("ACCESSION_NUMBER"))
        if not acc:
            continue
        out = []
        for name, _ in FILING_COLUMNS:
            if name == "accession_number":
                out.append(acc)
            elif name == "issuer_cik":
                out.append(normalize_cik(row.get("ISSUERCIK")))
            elif name == "source_release_key":
                out.append(release_key)
            else:
                out.append(_convert(name, row.get(sources[name])))
        yield tuple(out)


def iter_owners(zf: zipfile.ZipFile, release_key: str) -> Iterator[tuple]:
    sources = {
        "rptowner_name": "RPTOWNERNAME", "relationship": "RPTOWNER_RELATIONSHIP",
        "officer_title": "RPTOWNER_TITLE", "other_text": "RPTOWNER_TXT",
        "rptowner_street1": "RPTOWNER_STREET1", "rptowner_street2": "RPTOWNER_STREET2",
        "rptowner_city": "RPTOWNER_CITY", "rptowner_state": "RPTOWNER_STATE",
        "rptowner_zipcode": "RPTOWNER_ZIPCODE", "rptowner_state_desc": "RPTOWNER_STATE_DESC",
        "file_number": "FILE_NUMBER",
    }
    for row in iter_tsv(zf, "REPORTINGOWNER.tsv"):
        acc = clean_text(row.get("ACCESSION_NUMBER"))
        cik = normalize_cik(row.get("RPTOWNERCIK"))
        if not acc or not cik:
            continue
        director, officer, ten_pct, other = parse_relationship(row.get("RPTOWNER_RELATIONSHIP"))
        derived = {"accession_number": acc, "rptowner_cik": cik, "is_director": director,
                   "is_officer": officer, "is_ten_percent_owner": ten_pct, "is_other": other,
                   "source_release_key": release_key}
        yield tuple(derived[name] if name in derived else _convert(name, row.get(sources[name]))
                    for name, _ in OWNER_COLUMNS)


def _footnote_refs(row: Dict[str, str]) -> Optional[str]:
    refs = {}
    for header, value in row.items():
        if not header or not header.endswith("_FN"):
            continue
        v = clean_text(value)
        if not v:
            continue
        key = _FN_ALIASES.get(header, header[:-3].lower())
        refs[key] = v
    return json.dumps(refs, sort_keys=True) if refs else None


def iter_transactions(zf: zipfile.ZipFile, release_key: str) -> Iterator[tuple]:
    for member, table_type, sk_col in TRANSACTION_MEMBERS:
        for row in iter_tsv(zf, member):
            acc = clean_text(row.get("ACCESSION_NUMBER"))
            sk = parse_int(row.get(sk_col))
            if not acc or sk is None:
                continue
            out = []
            for name, _ in TRANSACTION_COLUMNS:
                if name == "accession_number":
                    out.append(acc)
                elif name == "table_type":
                    out.append(table_type)
                elif name == "trans_sk":
                    out.append(sk)
                elif name == "footnote_refs":
                    out.append(_footnote_refs(row))
                elif name == "source_release_key":
                    out.append(release_key)
                else:
                    out.append(_convert(name, _first(row, _TRANS_SOURCES[name])))
            yield tuple(out)


def iter_footnotes(zf: zipfile.ZipFile, release_key: str) -> Iterator[tuple]:
    for row in iter_tsv(zf, "FOOTNOTES.tsv"):
        acc = clean_text(row.get("ACCESSION_NUMBER"))
        fid = clean_text(row.get("FOOTNOTE_ID"))
        if not acc or not fid:
            continue
        yield (acc, fid, clean_text(row.get("FOOTNOTE_TXT")), release_key)
