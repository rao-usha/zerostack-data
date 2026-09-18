"""
Pure parsers for the Form ADV bulk sources (SPEC_112).

No DB and no HTTP here. Everything was checked against real files on
2026-09-16:

- The listing page anchors look like ``<a href=".../ia09012026-registered.zip" download>Registered
  Investment Advisers, September 2026</a>``.
- Roster zips hold one CSV member. RIA files have 448 columns and ERA files
  have 171. The bytes are cp1252, not UTF-8 (detected per file), with no BOM
  and CRLF line endings. Money is padded and comma-grouped (``'   628,902,725.00'``).
- The IAPD ``IA_FIRM_SEC_Feed`` is ISO-8859-1 XML, shaped
  ``<IAPDFirmSECReport><Firms><Firm>``.

Row iterators yield tuples of COPY-ready values (str or None) in the order
of ``ROSTER_COLUMNS`` / ``FEED_COLUMNS``.
"""

from __future__ import annotations

import codecs
import csv
import gzip
import html
import io
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urljoin, urlparse

SEC_WWW = "https://www.sec.gov/"
LISTING_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers"
)
MANIFEST_URL = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/CompilationReports.manifest.json"
FEED_BASE = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/"

CSV_FIELD_LIMIT = 10_000_000

# ---------------------------------------------------------------------------
# value parsers
# ---------------------------------------------------------------------------

_NUM_RE = re.compile(r"^-?(\d+(\.\d*)?|\.\d+)$")
_STATE_CODE_RE = re.compile(r"^[A-Z]{2}$")


def clean(value) -> Optional[str]:
    """Strip whitespace (including NBSP) and NUL. Empty -> None."""
    if value is None:
        return None
    s = str(value).replace("\x00", "").strip().strip("\xa0").strip()
    return s or None


def parse_number(value) -> Optional[Decimal]:
    """'   36,710,705,748.00' -> Decimal; '   .00' -> 0.00; text ('More than 500') -> None."""
    s = clean(value)
    if s is None:
        return None
    s = s.replace(",", "").replace("$", "")
    if not _NUM_RE.match(s):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def parse_int(value) -> Optional[int]:
    n = parse_number(value)
    if n is None:
        return None
    try:
        return int(n)
    except (ValueError, OverflowError):
        return None


def parse_yn(value) -> Optional[bool]:
    s = clean(value)
    if s is None:
        return None
    s = s.upper()
    if s in ("Y", "YES"):
        return True
    if s in ("N", "NO"):
        return False
    return None


def parse_date(value) -> Optional[date]:
    """MM/DD/YYYY (roster) or YYYY-MM-DD (IAPD feed). Anything else -> None."""
    s = clean(value)
    if s is None:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_notice_states(value) -> Optional[List[str]]:
    """'AZ-07/19/2004, CA-07/08/1997' -> ['AZ', 'CA'] (sorted, unique)."""
    s = clean(value)
    if s is None:
        return None
    codes = set()
    for part in s.split(","):
        code = part.strip().split("-", 1)[0].strip().upper()
        if _STATE_CODE_RE.match(code):
            codes.add(code)
    return sorted(codes) or None


def pg_array(values: Optional[Sequence[str]]) -> Optional[str]:
    """Postgres TEXT[] literal for simple codes (validated to [A-Z]{2})."""
    if not values:
        return None
    safe = [v for v in values if _STATE_CODE_RE.match(v)]
    return "{" + ",".join(safe) + "}" if safe else None


def _s(v) -> Optional[str]:
    """Python value -> COPY text."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def _json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------------------
# text decoding: per-file UTF-8 or cp1252
# ---------------------------------------------------------------------------
# Real roster CSVs are cp1252. Per-byte mixing isn't safe: the cp1252 pair
# "E-acute + NBSP" (bytes C9 A0) is also valid UTF-8 ("ɠ"). So each member is scanned
# once. Strict UTF-8 if the whole member is valid, otherwise cp1252, with
# latin-1 for its 5 undefined bytes.

_ERR_HANDLER = "sec_adv_latin1_fallback"


def _latin1_fallback(err: UnicodeError):
    if not isinstance(err, UnicodeDecodeError):
        raise err
    return err.object[err.start:err.end].decode("latin-1"), err.end


codecs.register_error(_ERR_HANDLER, _latin1_fallback)


def detect_encoding(binary, chunk: int = 1024 * 1024) -> str:
    """Stream a binary file once: 'utf-8-sig' if it is entirely valid UTF-8, else 'cp1252'."""
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    try:
        while True:
            block = binary.read(chunk)
            if not block:
                decoder.decode(b"", final=True)
                return "utf-8-sig"
            decoder.decode(block)
    except UnicodeDecodeError:
        return "cp1252"


def open_text(binary, encoding: str) -> io.TextIOWrapper:
    return io.TextIOWrapper(binary, encoding=encoding, errors=_ERR_HANDLER, newline="")


# ---------------------------------------------------------------------------
# listing page (sec_adv_roster discover)
# ---------------------------------------------------------------------------

# Seen: ia09012026-registered, ia08032026-exempt_1, ia020226-exemptzip, ia051023exempt,
# ia060319-1, ia020119-2-exempt, ia100118_ (trailing underscore).
FILE_RE = re.compile(r"^ia(\d{8}|\d{6})([-a-z0-9]*?)(?:_(\d*))?\.(zip|xlsx|xls|csv)$", re.I)
_ANCHOR_RE = re.compile(r"<a\s[^>]*?href=\"([^\"]+)\"[^>]*>(.*?)</a>", re.I | re.S)
_MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august",
           "september", "october", "november", "december"]
_MONTH_RE = re.compile(r"\b(" + "|".join(_MONTHS) + r")\s+(\d{4})\b", re.I)
_TAG_RE = re.compile(r"<[^>]+>")


@dataclass
class RosterFile:
    adviser_type: str        # 'ria' | 'era'
    roster_date: date
    suffix: int              # '_N' re-upload suffix, -1 when absent
    ext: str
    filename: str
    url: str


def _roster_date(digits: str, hint: Optional[Tuple[int, int]]) -> Optional[date]:
    """Resolve the filename date label.

    Three shapes are live: MMDDYYYY (ia09012026), MMDDYY (ia060126), and
    MMYYYY (ia122025, "December 2025"). When the anchor text names a month,
    that month decides between the shapes. A label that disagrees with it
    falls back to the 1st of the named month.
    """
    cands: List[date] = []

    def add(y, m, d):
        try:
            cands.append(date(y, m, d))
        except ValueError:
            pass

    if len(digits) == 8:
        add(int(digits[4:]), int(digits[:2]), int(digits[2:4]))
    else:
        if hint and digits == f"{hint[1]:02d}{hint[0]}":
            return date(hint[0], hint[1], 1)
        add(2000 + int(digits[4:]), int(digits[:2]), int(digits[2:4]))
        yyyy = int(digits[2:])
        if 2000 <= yyyy <= 2100:
            add(yyyy, int(digits[:2]), 1)
    if hint:
        for c in cands:
            if (c.year, c.month) == hint:
                return c
        return date(hint[0], hint[1], 1)
    return cands[0] if cands else None


def classify_listing(page_html: str, base_url: str = SEC_WWW) -> Tuple[List[RosterFile], Counter]:
    """All roster files on the listing page.

    Returns (zip files deduped per type+month, counts of skipped non-zip
    files by extension). xlsx files (before December 2025) and the "no data"
    PDFs are counted and skipped, never parsed.
    """
    skipped: Counter = Counter()
    best: Dict[Tuple[str, int, int], Tuple[Tuple[int, int], RosterFile]] = {}
    pos = 0
    for href, inner in _ANCHOR_RE.findall(page_html):
        href = html.unescape(href)
        basename = unquote(urlparse(href).path.rsplit("/", 1)[-1])
        if not basename.lower().startswith("ia"):
            continue
        label = html.unescape(_TAG_RE.sub(" ", inner))
        low_label = label.lower()
        m = FILE_RE.match(basename)
        if not m:
            ext = basename.rsplit(".", 1)[-1].lower() if "." in basename else ""
            if ext in ("pdf", "xlsx", "xls", "csv", "zip"):
                skipped[ext] += 1
            continue
        digits, variant, suffix_s, ext = m.group(1), (m.group(2) or "").lower(), m.group(3), m.group(4).lower()
        if "exempt" in low_label:
            adviser_type = "era"
        elif "registered" in low_label:
            adviser_type = "ria"
        else:
            adviser_type = "era" if "exempt" in variant else "ria"
        mm = _MONTH_RE.search(label)
        hint = (int(mm.group(2)), _MONTHS.index(mm.group(1).lower()) + 1) if mm else None
        rdate = _roster_date(digits, hint)
        if rdate is None:
            skipped["unparseable"] += 1
            continue
        if ext != "zip":
            skipped[ext] += 1
            continue
        suffix = int(suffix_s) if suffix_s else -1
        rf = RosterFile(adviser_type, rdate, suffix, ext, basename, urljoin(base_url, href))
        key = (adviser_type, rdate.year, rdate.month)
        rank = (suffix, -pos)  # highest re-upload suffix wins; ties -> first listed
        pos += 1
        if key not in best or rank > best[key][0]:
            best[key] = (rank, rf)
    files = sorted((v[1] for v in best.values()), key=lambda f: (f.roster_date, f.adviser_type))
    return files, skipped


# ---------------------------------------------------------------------------
# roster CSV
# ---------------------------------------------------------------------------

COL_CRD = "Organization CRD#"

# target column -> source header (plain text columns)
_ROSTER_TEXT = {
    "sec_region": "SEC Region",
    "sec_number": "SEC#",
    "firm_type": "Firm Type",
    "legal_name": "Legal Name",
    "business_name": "Primary Business Name",
    "main_office_street1": "Main Office Street Address 1",
    "main_office_street2": "Main Office Street Address 2",
    "main_office_city": "Main Office City",
    "main_office_state": "Main Office State",
    "main_office_country": "Main Office Country",
    "main_office_postal_code": "Main Office Postal Code",
    "main_office_phone": "Main Office Telephone Number",
    "main_office_fax": "Main Office Facsimile Number",
    "website": "Website Address",
    "sec_status": "SEC Current Status",
    "form_version": "Form Version",
}
_ROSTER_DATE = {
    "sec_status_effective_date": "SEC Status Effective Date",
    "latest_filing_date": "Latest ADV Filing Date",
}
_ROSTER_INT = {
    "other_office_count": "Total number of offices, other than your Principal Office and place of business",
    "employees_total": "5A",
    "employees_investment_advisory": "5B(1)",
    "clients_individuals": "5D(a)(1)",
    "clients_hnw": "5D(b)(1)",
    "clients_pooled_vehicles": "5D(f)(1)",
    "accounts_discretionary": "5F(2)(d)",
    "accounts_non_discretionary": "5F(2)(e)",
    "accounts_total": "5F(2)(f)",
    "private_fund_count": "Count of Private Funds - 7B(1)",
}
_ROSTER_NUM = {
    "aum_discretionary": "5F(2)(a)",
    "aum_non_discretionary": "5F(2)(b)",
    "aum_total": "5F(2)(c)",
    "custody_amount": "Total Custody Amount",
    "private_fund_gross_assets": "Total Gross Assets of Private Funds",
}
_ROSTER_BOOL = {
    "custody_client_cash_securities": "9A(1)(a)",
    "custody_related_person": "9B(1)(a)",
    "has_disciplinary_disclosure": "11",
}
COL_NOTICE = "Jurisdiction Notice Filed-Effective Date"
COL_CLIENTS = "5C(1)"
COL_CLIENTS_OVER = "5C(1)-If more than 100, how many"

REQUIRED_COMMON = (COL_CRD, "SEC#", "Primary Business Name", "Legal Name", "Main Office State",
                   "Main Office Country", "SEC Current Status", "Latest ADV Filing Date", "Website Address")
REQUIRED_RIA = REQUIRED_COMMON + ("5A", "5B(1)", COL_CLIENTS, "5F(2)(a)", "5F(2)(b)", "5F(2)(c)",
                                  "5F(2)(f)", "9A(1)(a)", "Total Custody Amount")
REQUIRED_BY_TYPE = {"ria": REQUIRED_RIA, "era": REQUIRED_COMMON}

_MAPPED_HEADERS = frozenset(
    [COL_CRD, COL_NOTICE, COL_CLIENTS, COL_CLIENTS_OVER]
    + list(_ROSTER_TEXT.values()) + list(_ROSTER_DATE.values()) + list(_ROSTER_INT.values())
    + list(_ROSTER_NUM.values()) + list(_ROSTER_BOOL.values())
)

DRP_FLAG_RE = re.compile(r"^11(?:[A-H]|$)")
DRP_COUNT_RE = re.compile(r"^Count of 11[A-H]\S*(?: \S+)* disclosures$")

ROSTER_COLUMNS: List[Tuple[str, str]] = (
    [("crd_number", "TEXT"), ("roster_date", "DATE"), ("adviser_type", "TEXT")]
    + [(c, "TEXT") for c in _ROSTER_TEXT]
    + [(c, "DATE") for c in _ROSTER_DATE]
    + [("notice_filed_states", "TEXT[]")]
    + [(c, "BIGINT") for c in _ROSTER_INT]
    + [("clients_count", "BIGINT")]
    + [(c, "NUMERIC") for c in _ROSTER_NUM]
    + [(c, "BOOLEAN") for c in _ROSTER_BOOL]
    + [("drp_flags", "JSONB"), ("drp_disclosure_total", "INTEGER"), ("raw", "JSONB"),
       ("source_release_key", "TEXT")]
)


def _roster_tuple(r: Dict[str, str], crd: str, adviser_type: str, roster_date: date,
                  drp_cols: List[str], release_key: str) -> tuple:
    g = r.get
    out: Dict[str, object] = {"crd_number": crd, "roster_date": roster_date, "adviser_type": adviser_type}
    for col, hdr in _ROSTER_TEXT.items():
        out[col] = clean(g(hdr))
    for col, hdr in _ROSTER_DATE.items():
        out[col] = parse_date(g(hdr))
    out["notice_filed_states"] = pg_array(parse_notice_states(g(COL_NOTICE)))
    for col, hdr in _ROSTER_INT.items():
        out[col] = parse_int(g(hdr))
    clients = parse_int(g(COL_CLIENTS_OVER))
    out["clients_count"] = clients if clients is not None else parse_int(g(COL_CLIENTS))
    for col, hdr in _ROSTER_NUM.items():
        out[col] = parse_number(g(hdr))
    for col, hdr in _ROSTER_BOOL.items():
        out[col] = parse_yn(g(hdr))

    drp = {c: clean(g(c)) for c in drp_cols}
    total = 0
    for c, v in drp.items():
        if DRP_COUNT_RE.match(c):
            total += parse_int(v) or 0
    out["drp_flags"] = _json(drp) if drp_cols else None
    out["drp_disclosure_total"] = total if drp_cols else None

    drp_set = set(drp_cols)
    raw = {}
    for k, v in r.items():
        if k is None or k in _MAPPED_HEADERS or k in drp_set:
            continue
        cv = clean(v)
        if cv is not None:
            raw[k] = cv
    out["raw"] = _json(raw)
    out["source_release_key"] = release_key
    return tuple(_s(out[c]) for c, _ in ROSTER_COLUMNS)


def iter_roster_rows(zip_path: Path | str, adviser_type: str, roster_date: date,
                     release_key: str) -> Iterator[tuple]:
    """Stream one roster zip into ROSTER_COLUMNS tuples. Header drift raises."""
    if adviser_type not in REQUIRED_BY_TYPE:
        raise ValueError(f"adviser_type must be ria|era, got {adviser_type!r}")
    prev_limit = csv.field_size_limit(CSV_FIELD_LIMIT)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not members:
                raise RuntimeError(
                    f"{Path(zip_path).name}: no CSV member (members: {zf.namelist()[:5]}); "
                    "pre-Dec-2025 roster zips wrap .xlsx, which this loader does not parse")
            for member in members:
                with zf.open(member) as probe:
                    encoding = detect_encoding(probe)
                with zf.open(member) as binary:
                    reader = csv.DictReader(open_text(binary, encoding))
                    headers = [h.strip() if h else h for h in (reader.fieldnames or [])]
                    reader.fieldnames = headers
                    missing = [c for c in REQUIRED_BY_TYPE[adviser_type] if c not in headers]
                    if missing:
                        raise RuntimeError(
                            f"{Path(zip_path).name}/{member}: header drift, missing {missing} "
                            f"(got {len(headers)} cols)")
                    drp_cols = [h for h in headers if h and (DRP_FLAG_RE.match(h) or DRP_COUNT_RE.match(h))]
                    for r in reader:
                        crd = clean(r.get(COL_CRD))
                        if not crd or not crd.isdigit():
                            continue
                        yield _roster_tuple(r, crd, adviser_type, roster_date, drp_cols, release_key)
    finally:
        csv.field_size_limit(prev_limit)


# ---------------------------------------------------------------------------
# IAPD compilation feed
# ---------------------------------------------------------------------------

FEED_RE = re.compile(r"^IA_FIRM_SEC_Feed_(\d{2})_(\d{2})_(\d{4})\.xml\.gz$")


def parse_manifest(text: str) -> Tuple[str, date]:
    """Manifest JSON -> (SEC firm feed filename, edition date). Shape drift raises."""
    try:
        manifest = json.loads(text)
    except ValueError as e:
        raise RuntimeError(f"IAPD manifest is not JSON: {e}") from e
    files = manifest.get("files") if isinstance(manifest, dict) else None
    if not isinstance(files, list):
        keys = sorted(manifest) if isinstance(manifest, dict) else type(manifest).__name__
        raise RuntimeError(f"IAPD manifest shape changed: expected {{'files': [...]}}, got {keys}")
    for entry in files:
        name = entry.get("name", "") if isinstance(entry, dict) else ""
        m = FEED_RE.match(name)
        if m:
            mm, dd, yyyy = m.groups()
            return name, date(int(yyyy), int(mm), int(dd))
    raise RuntimeError(
        "IAPD manifest lists no IA_FIRM_SEC_Feed_MM_DD_YYYY.xml.gz; entries: "
        f"{[f.get('name') if isinstance(f, dict) else f for f in files]}")


FEED_COLUMNS: List[Tuple[str, str]] = [
    ("crd_number", "TEXT"), ("edition_date", "DATE"), ("sec_region", "TEXT"), ("sec_number", "TEXT"),
    ("firm_type", "TEXT"), ("business_name", "TEXT"), ("legal_name", "TEXT"),
    ("registration_status", "TEXT"), ("registration_date", "DATE"), ("filing_date", "DATE"),
    ("form_version", "TEXT"), ("main_office_city", "TEXT"), ("main_office_state", "TEXT"),
    ("main_office_country", "TEXT"), ("website", "TEXT"), ("employees_total", "BIGINT"),
    ("aum_discretionary", "NUMERIC"), ("aum_non_discretionary", "NUMERIC"), ("aum_total", "NUMERIC"),
    ("accounts_total", "BIGINT"), ("notice_filed_states", "TEXT[]"), ("raw", "JSONB"),
    ("source_release_key", "TEXT"),
]

# (element, attribute) pairs lifted into columns and kept out of raw
_FEED_MAPPED = {
    ("Info", "FirmCrdNb"), ("Info", "SECRgnCD"), ("Info", "SECNb"), ("Info", "BusNm"), ("Info", "LegalNm"),
    ("MainAddr", "City"), ("MainAddr", "State"), ("MainAddr", "Cntry"),
    ("Rgstn", "FirmType"), ("Rgstn", "St"), ("Rgstn", "Dt"),
    ("Filing", "Dt"), ("Filing", "FormVrsn"),
}
_PART1A_MAPPED = {"TtlEmp", "Q5F2A", "Q5F2B", "Q5F2C", "Q5F2F"}


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


def _flatten(el, prefix: str, raw: Dict[str, object]) -> None:
    """Generic fallback for unexpected elements: 'Path.Attr' -> value."""
    for k, v in el.attrib.items():
        cv = clean(v)
        if cv is not None:
            raw[f"{prefix}.{k}"] = cv
    txt = clean(el.text)
    if txt is not None:
        raw[prefix] = txt
    for child in el:
        _flatten(child, f"{prefix}.{_local(child.tag)}", raw)


def _firm_tuple(firm, edition_date: date, release_key: str) -> Optional[tuple]:
    attrs: Dict[Tuple[str, str], str] = {}
    raw: Dict[str, object] = {}
    part1a: Dict[str, str] = {}
    websites: List[str] = []
    notice: List[Dict[str, str]] = []
    states: List[str] = []

    for child in firm:
        tag = _local(child.tag)
        if tag == "NoticeFiled":
            for st in child:
                entry = {k: v for k, v in st.attrib.items() if clean(v) is not None}
                if entry:
                    notice.append(entry)
                code = clean(st.get("RgltrCd"))
                if code:
                    states.append(code.upper())
        elif tag == "FormInfo":
            for part in child.iter():
                ptag = _local(part.tag)
                if ptag == "WebAddr":
                    w = clean(part.text)
                    if w:
                        websites.append(w)
                    continue
                for k, v in part.attrib.items():
                    cv = clean(v)
                    if cv is None:
                        continue
                    if k in _PART1A_MAPPED:
                        attrs[("Part1A", k)] = cv
                    else:
                        part1a[k] = cv
        elif not len(child):
            for k, v in child.attrib.items():
                cv = clean(v)
                if cv is None:
                    continue
                if (tag, k) in _FEED_MAPPED:
                    attrs[(tag, k)] = cv
                else:
                    raw[f"{tag}.{k}"] = cv
            txt = clean(child.text)
            if txt is not None:
                raw[tag] = txt
        else:
            _flatten(child, tag, raw)

    crd = attrs.get(("Info", "FirmCrdNb"))
    if not crd or not crd.isdigit():
        return None
    if notice:
        raw["NoticeFiled"] = notice
    if websites:
        raw["WebAddrs"] = websites
    if part1a:
        raw["Part1A"] = part1a

    a = attrs.get
    out = {
        "crd_number": crd,
        "edition_date": edition_date,
        "sec_region": a(("Info", "SECRgnCD")),
        "sec_number": a(("Info", "SECNb")),
        "firm_type": a(("Rgstn", "FirmType")),
        "business_name": a(("Info", "BusNm")),
        "legal_name": a(("Info", "LegalNm")),
        "registration_status": a(("Rgstn", "St")),
        "registration_date": parse_date(a(("Rgstn", "Dt"))),
        "filing_date": parse_date(a(("Filing", "Dt"))),
        "form_version": a(("Filing", "FormVrsn")),
        "main_office_city": a(("MainAddr", "City")),
        "main_office_state": a(("MainAddr", "State")),
        "main_office_country": a(("MainAddr", "Cntry")),
        "website": websites[0] if websites else None,
        "employees_total": parse_int(a(("Part1A", "TtlEmp"))),
        "aum_discretionary": parse_number(a(("Part1A", "Q5F2A"))),
        "aum_non_discretionary": parse_number(a(("Part1A", "Q5F2B"))),
        "aum_total": parse_number(a(("Part1A", "Q5F2C"))),
        "accounts_total": parse_int(a(("Part1A", "Q5F2F"))),
        "notice_filed_states": pg_array(sorted(set(states))),
        "raw": _json(raw),
        "source_release_key": release_key,
    }
    return tuple(_s(out[c]) for c, _ in FEED_COLUMNS)


def iter_feed_rows(gz_path: Path | str, edition_date: date, release_key: str) -> Iterator[tuple]:
    """Stream the gzip XML feed into FEED_COLUMNS tuples with bounded memory.

    The workbench cleared the document root. But ``<Firm>`` elements hang off
    ``<Firms>``, which the parser keeps building after it has been detached,
    so we clear the Firm's direct parent instead.
    """
    with gzip.open(gz_path, "rb") as fh:
        stack: List = []
        for event, el in ET.iterparse(fh, events=("start", "end")):
            if event == "start":
                stack.append(el)
                continue
            stack.pop()
            if _local(el.tag) != "Firm":
                continue
            row = _firm_tuple(el, edition_date, release_key)
            if stack:
                stack[-1].clear()
            else:
                el.clear()
            if row is not None:
                yield row
