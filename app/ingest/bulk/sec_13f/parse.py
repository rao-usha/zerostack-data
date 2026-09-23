"""
Pure parsing for SEC Form 13F data sets (SPEC_109). No DB, no HTTP.

Format notes (verified on 01jun2026-31aug2026_form13f.zip):
- Tab-delimited, LF line endings, header row of upper-case column names.
- Fields containing a double quote are wrapped in quotes with inner quotes
  doubled (``"Newton (""NIMNA"")"``); no field contains a tab or newline. We
  read with QUOTE_NONE (a stray quote can never swallow rows) and unwrap.
- Dates are DD-MON-YYYY.
- INFOTABLE VALUE: "Starting on January 3, 2023, market value is reported
  rounded to the nearest dollar. Previously, market value was reported in
  thousands." (FORM13F_readme). We normalise to dollars by filing date.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterator, List, Optional
from urllib.parse import urljoin

INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
CSV_FIELD_LIMIT = 50_000_000
DOLLAR_VALUES_FROM = date(2023, 1, 3)

_MONTHS = {m: i + 1 for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"))}

# 01jun2026-31aug2026_form13f.zip | 2023q4_form13f.zip, optional _N re-upload suffix
_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']*?_form13f(?:_\d+)?\.zip)["\']', re.I)
_RANGE_RE = re.compile(r"^(\d{2})([a-z]{3})(\d{4})-(\d{2})([a-z]{3})(\d{4})_form13f(?:_(\d+))?$", re.I)
_QUARTER_RE = re.compile(r"^(\d{4})q([1-4])_form13f(?:_(\d+))?$", re.I)


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DataSet:
    release_key: str
    url: str
    start_date: date
    end_date: date


def dataset_dates(release_key: str) -> Optional[tuple]:
    """(start, end) for a zip stem like '01jun2026-31aug2026_form13f' or '2023q4_form13f'."""
    m = _RANGE_RE.match(release_key)
    if m:
        d1, m1, y1, d2, m2, y2, _ = m.groups()
        try:
            return (date(int(y1), _MONTHS[m1.lower()], int(d1)),
                    date(int(y2), _MONTHS[m2.lower()], int(d2)))
        except (KeyError, ValueError):
            return None
    m = _QUARTER_RE.match(release_key)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        start = date(year, 3 * q - 2, 1)
        end = date(year, 12, 31) if q == 4 else date(year, 3 * q + 1, 1) - timedelta(days=1)
        return start, end
    return None


def parse_index(html: str, base_url: str = INDEX_URL) -> List[DataSet]:
    """Every 13F data-set zip linked from the index page, oldest first.

    Among re-uploads of the same date range (``_N`` suffix) the lexically last
    basename wins.
    """
    best: Dict[tuple, DataSet] = {}
    for href in _HREF_RE.findall(html or ""):
        url = urljoin(base_url, href)
        stem = url.rsplit("/", 1)[-1][: -len(".zip")]
        dates = dataset_dates(stem)
        if dates is None:
            continue
        ds = DataSet(stem, url, dates[0], dates[1])
        cur = best.get(dates)
        if cur is None or stem > cur.release_key:
            best[dates] = ds
    return sorted(best.values(), key=lambda d: (d.end_date, d.release_key))


# ---------------------------------------------------------------------------
# value helpers
# ---------------------------------------------------------------------------

def unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1].replace('""', '"')
    return value


def parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    s = value.strip()
    try:
        if len(s) == 11 and s[2] == "-" and s[6] == "-":
            return date(int(s[7:]), _MONTHS[s[3:6].lower()], int(s[:2]))
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (KeyError, ValueError):
        return None


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if not value:
        return None
    try:
        d = Decimal(value.strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def parse_int(value: Optional[str]) -> Optional[int]:
    d = parse_decimal(value)
    if d is None or d != d.to_integral_value():
        return None
    return int(d)


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if not value:
        return None
    v = value.strip().upper()
    if v in ("Y", "YES", "TRUE", "1"):
        return True
    if v in ("N", "NO", "FALSE", "0"):
        return False
    return None


def value_multiplier(filing_date: Optional[date], fallback: Optional[date] = None) -> int:
    """1000 when VALUE was reported in thousands (filed before 2023-01-03), else 1."""
    d = filing_date or fallback
    if d is not None and d < DOLLAR_VALUES_FROM:
        return 1000
    return 1


# ---------------------------------------------------------------------------
# TSV streaming
# ---------------------------------------------------------------------------

def find_member(zf: zipfile.ZipFile, basename: str) -> Optional[str]:
    want = basename.lower()
    for name in zf.namelist():
        if name.rsplit("/", 1)[-1].lower() == want:
            return name
    return None


# Members every 13F data set carries. A missing one is a broken or truncated
# zip, never "no rows": loading it as empty would publish an empty table
# (INFOTABLE) or NULL out every manager name (COVERPAGE) with status `loaded`.
REQUIRED_MEMBERS = frozenset({"SUBMISSION.TSV", "COVERPAGE.TSV", "INFOTABLE.TSV"})

# Columns that must be in a member's header whenever the member is present.
# Keys plus what downstream reads -- not every optional column, so a column
# the SEC adds (or one older data sets lack, like FIGI) never breaks a load.
# A missing one would otherwise load as NULL on every row, silently.
REQUIRED_HEADERS: Dict[str, tuple] = {
    "SUBMISSION.TSV": ("ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"),
    "COVERPAGE.TSV": ("ACCESSION_NUMBER", "REPORTCALENDARORQUARTER", "FILINGMANAGER_NAME",
                      "REPORTTYPE"),
    "SUMMARYPAGE.TSV": ("ACCESSION_NUMBER", "TABLEENTRYTOTAL", "TABLEVALUETOTAL"),
    "SIGNATURE.TSV": ("ACCESSION_NUMBER", "NAME"),
    "OTHERMANAGER.TSV": ("ACCESSION_NUMBER", "OTHERMANAGER_SK", "NAME"),
    "OTHERMANAGER2.TSV": ("ACCESSION_NUMBER", "SEQUENCENUMBER", "NAME"),
    "INFOTABLE.TSV": ("ACCESSION_NUMBER", "INFOTABLE_SK", "NAMEOFISSUER", "CUSIP", "VALUE",
                      "SSHPRNAMT", "SSHPRNAMTTYPE"),
}


def validate_members(zf: zipfile.ZipFile) -> None:
    """Check every required member and header up front, before any COPY starts.

    ``iter_tsv`` enforces the same rules lazily, but a ValueError raised from
    inside a COPY aborts the transaction and surfaces as a psycopg2 error (or
    is masked by cleanup SQL). Failing here keeps the release error readable.
    """
    for basename in REQUIRED_HEADERS:
        for _ in iter_tsv(zf, basename.replace(".TSV", ".tsv")):
            break


def iter_tsv(zf: zipfile.ZipFile, basename: str) -> Iterator[Dict[str, Optional[str]]]:
    """Stream one TSV member as dicts keyed by upper-case header. Blank -> None.

    Optional members yield nothing when absent. A member in
    ``REQUIRED_MEMBERS`` that is absent, or any member whose header lacks a
    ``REQUIRED_HEADERS`` column, raises ``ValueError`` (the release fails).
    """
    key = basename.upper()
    member = find_member(zf, basename)
    if member is None:
        if key in REQUIRED_MEMBERS:
            raise ValueError(
                f"13F data set is missing required member {basename} "
                f"(members: {[n.rsplit('/', 1)[-1] for n in zf.namelist()][:12]})"
            )
        return
    if csv.field_size_limit() < CSV_FIELD_LIMIT:
        csv.field_size_limit(CSV_FIELD_LIMIT)
    with zf.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
        reader = csv.reader(text, delimiter="\t", quoting=csv.QUOTE_NONE)
        try:
            header = next(reader)
        except StopIteration:
            header = []
        header = [h.strip().lstrip("﻿").strip('"').upper() for h in header]
        missing = [c for c in REQUIRED_HEADERS.get(key, ()) if c not in header]
        if missing:
            raise ValueError(
                f"13F member {basename} header is missing required column(s) {missing} "
                f"(header: {header[:20]}); refusing to load them as NULL"
            )
        if not header:
            return
        width = len(header)
        for row in reader:
            if not row or (len(row) == 1 and not row[0].strip()):
                continue
            out: Dict[str, Optional[str]] = {}
            for i in range(width):
                v = row[i].strip() if i < len(row) else ""
                if v:
                    v = unquote(v).strip()
                out[header[i]] = v or None
            yield out


# ---------------------------------------------------------------------------
# row builders
# ---------------------------------------------------------------------------

FILING_COLUMNS = [
    "accession_number", "cik", "submission_type", "filing_date", "period_of_report",
    "report_calendar_or_quarter", "is_amendment", "amendment_no", "amendment_type",
    "conf_denied_expired", "date_denied_expired", "date_reported", "reason_for_non_confidentiality",
    "filing_manager_name", "filing_manager_street1", "filing_manager_street2", "filing_manager_city",
    "filing_manager_state_or_country", "filing_manager_zipcode", "report_type", "form13f_file_number",
    "crd_number", "sec_file_number", "provide_info_for_instruction5", "additional_information",
    "other_included_managers_count", "table_entry_total", "table_value_total", "is_confidential_omitted",
    "signature_name", "signature_title", "signature_phone", "signature", "signature_city",
    "signature_state_or_country", "signature_date", "value_multiplier",
]

HOLDING_COLUMNS = [
    "accession_number", "infotable_sk", "name_of_issuer", "title_of_class", "cusip", "figi", "value",
    "ssh_prnamt", "ssh_prnamt_type", "put_call", "investment_discretion", "other_manager",
    "voting_auth_sole", "voting_auth_shared", "voting_auth_none",
]

OTHER_MANAGER_COLUMNS = [
    "accession_number", "list_type", "sequence_number", "cik", "form13f_file_number", "crd_number",
    "sec_file_number", "name",
]


def _cover_fields(r: Dict[str, Optional[str]]) -> Dict:
    return {
        "report_calendar_or_quarter": parse_date(r.get("REPORTCALENDARORQUARTER")),
        "is_amendment": parse_bool(r.get("ISAMENDMENT")),
        "amendment_no": parse_int(r.get("AMENDMENTNO")),
        "amendment_type": r.get("AMENDMENTTYPE"),
        "conf_denied_expired": parse_bool(r.get("CONFDENIEDEXPIRED")),
        "date_denied_expired": parse_date(r.get("DATEDENIEDEXPIRED")),
        "date_reported": parse_date(r.get("DATEREPORTED")),
        "reason_for_non_confidentiality": r.get("REASONFORNONCONFIDENTIALITY"),
        "filing_manager_name": r.get("FILINGMANAGER_NAME"),
        "filing_manager_street1": r.get("FILINGMANAGER_STREET1"),
        "filing_manager_street2": r.get("FILINGMANAGER_STREET2"),
        "filing_manager_city": r.get("FILINGMANAGER_CITY"),
        "filing_manager_state_or_country": r.get("FILINGMANAGER_STATEORCOUNTRY"),
        "filing_manager_zipcode": r.get("FILINGMANAGER_ZIPCODE"),
        "report_type": r.get("REPORTTYPE"),
        "form13f_file_number": r.get("FORM13FFILENUMBER"),
        "crd_number": r.get("CRDNUMBER"),
        "sec_file_number": r.get("SECFILENUMBER"),
        "provide_info_for_instruction5": parse_bool(r.get("PROVIDEINFOFORINSTRUCTION5")),
        "additional_information": r.get("ADDITIONALINFORMATION"),
    }


def iter_filings(zf: zipfile.ZipFile, fallback_date: Optional[date] = None) -> Iterator[Dict]:
    """One dict per accession: SUBMISSION joined with COVERPAGE, SUMMARYPAGE, SIGNATURE.

    Filing-level members are ~12k rows per data set, so the join is in memory.
    COVERPAGE/SUMMARYPAGE/SIGNATURE rows whose accession is not in SUBMISSION
    are dropped (they cannot be keyed to a filing date/CIK).
    """
    if find_member(zf, "SUBMISSION.tsv") is None:
        raise RuntimeError(f"SUBMISSION.tsv missing from 13F data set (members: {zf.namelist()[:10]})")
    filings: Dict[str, Dict] = {}
    for r in iter_tsv(zf, "SUBMISSION.tsv"):
        acc = r.get("ACCESSION_NUMBER")
        if not acc:
            continue
        row = dict.fromkeys(FILING_COLUMNS)
        row.update({
            "accession_number": acc,
            "cik": r.get("CIK"),
            "submission_type": r.get("SUBMISSIONTYPE"),
            "filing_date": parse_date(r.get("FILING_DATE")),
            "period_of_report": parse_date(r.get("PERIODOFREPORT")),
        })
        row["value_multiplier"] = value_multiplier(row["filing_date"], fallback_date)
        filings[acc] = row

    for r in iter_tsv(zf, "COVERPAGE.tsv"):
        f = filings.get(r.get("ACCESSION_NUMBER"))
        if f is not None:
            f.update(_cover_fields(r))

    for r in iter_tsv(zf, "SUMMARYPAGE.tsv"):
        f = filings.get(r.get("ACCESSION_NUMBER"))
        if f is None:
            continue
        total = parse_decimal(r.get("TABLEVALUETOTAL"))
        f.update({
            "other_included_managers_count": parse_int(r.get("OTHERINCLUDEDMANAGERSCOUNT")),
            "table_entry_total": parse_int(r.get("TABLEENTRYTOTAL")),
            "table_value_total": None if total is None else total * f["value_multiplier"],
            "is_confidential_omitted": parse_bool(r.get("ISCONFIDENTIALOMITTED")),
        })

    for r in iter_tsv(zf, "SIGNATURE.tsv"):
        f = filings.get(r.get("ACCESSION_NUMBER"))
        if f is None:
            continue
        f.update({
            "signature_name": r.get("NAME"),
            "signature_title": r.get("TITLE"),
            "signature_phone": r.get("PHONE"),
            "signature": r.get("SIGNATURE"),
            "signature_city": r.get("CITY"),
            "signature_state_or_country": r.get("STATEORCOUNTRY"),
            "signature_date": parse_date(r.get("SIGNATUREDATE")),
        })

    yield from filings.values()


def filing_dates(zf: zipfile.ZipFile) -> Dict[str, Optional[date]]:
    """accession -> filing date (for VALUE unit normalisation of INFOTABLE rows)."""
    return {r["ACCESSION_NUMBER"]: parse_date(r.get("FILING_DATE"))
            for r in iter_tsv(zf, "SUBMISSION.tsv") if r.get("ACCESSION_NUMBER")}


def iter_holdings(zf: zipfile.ZipFile, dates: Dict[str, Optional[date]],
                  fallback_date: Optional[date] = None) -> Iterator[Dict]:
    """Stream INFOTABLE positions (millions of rows). VALUE normalised to dollars."""
    for r in iter_tsv(zf, "INFOTABLE.tsv"):
        acc = r.get("ACCESSION_NUMBER")
        sk = parse_int(r.get("INFOTABLE_SK"))
        if not acc or sk is None:
            continue
        value = parse_decimal(r.get("VALUE"))
        if value is not None:
            mult = value_multiplier(dates.get(acc), fallback_date)
            if mult != 1:
                value = value * mult
        yield {
            "accession_number": acc,
            "infotable_sk": sk,
            "name_of_issuer": r.get("NAMEOFISSUER"),
            "title_of_class": r.get("TITLEOFCLASS"),
            "cusip": r.get("CUSIP"),
            "figi": r.get("FIGI"),
            "value": value,
            "ssh_prnamt": parse_int(r.get("SSHPRNAMT")),
            "ssh_prnamt_type": r.get("SSHPRNAMTTYPE"),
            "put_call": r.get("PUTCALL"),
            "investment_discretion": r.get("INVESTMENTDISCRETION"),
            "other_manager": r.get("OTHERMANAGER"),
            "voting_auth_sole": parse_int(r.get("VOTING_AUTH_SOLE")),
            "voting_auth_shared": parse_int(r.get("VOTING_AUTH_SHARED")),
            "voting_auth_none": parse_int(r.get("VOTING_AUTH_NONE")),
        }


def iter_other_managers(zf: zipfile.ZipFile) -> Iterator[Dict]:
    """OTHERMANAGER (list_type 'cover', key OTHERMANAGER_SK) and
    OTHERMANAGER2 (list_type 'summary', key SEQUENCENUMBER)."""
    for member, list_type, seq_col in (("OTHERMANAGER.tsv", "cover", "OTHERMANAGER_SK"),
                                       ("OTHERMANAGER2.tsv", "summary", "SEQUENCENUMBER")):
        for r in iter_tsv(zf, member):
            acc = r.get("ACCESSION_NUMBER")
            seq = parse_int(r.get(seq_col))
            if not acc or seq is None:
                continue
            yield {
                "accession_number": acc,
                "list_type": list_type,
                "sequence_number": seq,
                "cik": r.get("CIK"),
                "form13f_file_number": r.get("FORM13FFILENUMBER"),
                "crd_number": r.get("CRDNUMBER"),
                "sec_file_number": r.get("SECFILENUMBER"),
                "name": r.get("NAME"),
            }
