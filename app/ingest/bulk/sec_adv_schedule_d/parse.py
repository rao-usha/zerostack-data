"""
Pure parser for the Form ADV filing-data zips (SPEC_118).

No DB and no HTTP here. Checked against the real
``ADV_Filing_Data_20260801_20260831.zip`` on 2026-09-20:

- 101 members. Only ``{IA,ERA}_Schedule_D_7B1_YYYYMMDD_YYYYMMDD.csv`` and the
  base members are read; the sibling ``7B1A*`` members (7B1A17b, 7B1A28, ...)
  are the sub-schedules and are matched out by the underscore after ``7B1``.
- 7B1 is exactly 38 columns, 18,218 IA rows + 876 ERA rows, and carries **no
  CRD**. CRD is ``1E1`` of the base member, joined on FilingID. ERA's base
  member is ``ERA_ADV_Base``, not ``ERA_ADV_Base_A``.
- ``Fund ID`` is ``805-`` + TEN digits (``805-9253414470``) and is kept verbatim.
- ``DateSubmitted`` is ``MM/DD/YYYY hh:mm:ss AM`` in IA and bare ``MM/DD/YYYY``
  in ERA.
- Members are UTF-8 on this host; the older sec.gov FOIA archive is cp1252, so
  the encoding is detected per member (helpers shared with sec_form_adv).

Row iterators yield COPY-ready tuples in the order of ``FILING_COLUMNS`` /
``FUND_COLUMNS``, which mirror migration 0010_adv_private_funds exactly.
"""

from __future__ import annotations

import csv
import logging
import re
import zipfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

from app.entities.norm import core as name_core
from app.ingest.bulk.sec_form_adv.parse import (
    CSV_FIELD_LIMIT,
    clean,
    detect_encoding,
    open_text,
    parse_int,
    parse_number,
    parse_yn,
)

logger = logging.getLogger(__name__)

MANIFEST_URL = "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json"
DOWNLOAD_BASE = "https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/"

# The trailing underscore is what separates 7B1 from its sub-schedules
# (7B1A17b, 7B1A28_websites, ...), which have different grains.
MEMBER_7B1_RE = re.compile(r"(?:^|/)(IA|ERA)_Schedule_D_7B1_[^/]*\.csv$", re.I)
# ERA's base member is ADV_Base; IA's is ADV_Base_A (ADV_Base_B is Part 2 text).
MEMBER_BASE_RE = re.compile(r"(?:^|/)(IA_ADV_Base_A|ERA_ADV_Base)_[^/]*\.csv$", re.I)

ADVISER_TYPE = {"ia": "ria", "era": "era"}

REQUIRED_7B1: Tuple[str, ...] = (
    "FilingID", "Fund Name", "Fund ID", "ReferenceID", "State", "Country",
    "3(c)(1) Exclusion", "3(c)(7) Exclusion", "Master Fund", "Feeder Fund",
    "Master Fund Name", "Master Fund ID", "Fund of Funds",
    "Fund Invested Self or Related", "Fund Invested in Securities", "Fund Type",
    "Fund Type Other", "Gross Asset Value", "Minimum Investment", "Owners",
    "%Owned You or Related", "%Owned Funds", "Sales Limited", "%Owned Non-US",
    "Subadviser", "Other IAs Advise", "Clients Solicited", "Percentage Invested",
    "Exempt from Registration", "Annual Audit", "GAAP", "FS Distributed",
    "Unqualified Opinion", "Prime Brokers", "Custodians", "Administrator",
    "% Assets Valued", "Marketing",
)

REQUIRED_BASE: Tuple[str, ...] = ("FilingID", "1A", "1D", "1E1", "DateSubmitted")

# Mirrors public.sec_adv_filings in 0010_adv_private_funds.
FILING_COLUMNS: List[Tuple[str, str]] = [
    ("filing_id", "BIGINT"),
    ("crd_number", "TEXT"),
    ("adviser_type", "TEXT"),
    ("sec_number", "TEXT"),
    ("legal_name", "TEXT"),
    ("form_version", "TEXT"),
    ("filed_at", "TIMESTAMP"),
    ("source_release_key", "TEXT"),
]

# Mirrors public.sec_adv_private_fund_filings in 0010_adv_private_funds.
FUND_COLUMNS: List[Tuple[str, str]] = [
    ("filing_id", "BIGINT"),
    ("private_fund_id", "TEXT"),
    ("adviser_type", "TEXT"),
    ("fund_name", "TEXT"),
    ("fund_name_core", "TEXT"),
    ("reference_id", "TEXT"),
    ("org_state", "TEXT"),
    ("org_country", "TEXT"),
    ("excl_3c1", "BOOLEAN"),
    ("excl_3c7", "BOOLEAN"),
    ("is_master_fund", "BOOLEAN"),
    ("is_feeder_fund", "BOOLEAN"),
    ("master_fund_name", "TEXT"),
    ("master_fund_id", "TEXT"),
    ("is_fund_of_funds", "BOOLEAN"),
    ("invests_in_self_or_related", "BOOLEAN"),
    ("invests_in_securities", "BOOLEAN"),
    ("fund_type", "TEXT"),
    ("fund_type_other", "TEXT"),
    ("gross_asset_value", "NUMERIC"),
    ("minimum_investment", "NUMERIC"),
    ("owners", "INTEGER"),
    ("pct_owned_you_or_related", "NUMERIC(6, 3)"),
    ("pct_owned_funds", "NUMERIC(6, 3)"),
    ("sales_limited", "BOOLEAN"),
    ("pct_owned_non_us", "NUMERIC(6, 3)"),
    ("has_subadviser", "BOOLEAN"),
    ("other_ias_advise", "BOOLEAN"),
    ("clients_solicited", "BOOLEAN"),
    ("pct_invested", "NUMERIC(6, 3)"),
    ("exempt_from_registration", "BOOLEAN"),
    ("annual_audit", "BOOLEAN"),
    ("gaap", "BOOLEAN"),
    ("fs_distributed", "BOOLEAN"),
    ("unqualified_opinion", "TEXT"),
    ("prime_brokers", "BOOLEAN"),
    ("custodians", "BOOLEAN"),
    ("administrator", "BOOLEAN"),
    ("pct_assets_valued", "NUMERIC(6, 3)"),
    ("marketing", "BOOLEAN"),
    ("source_release_key", "TEXT"),
]

# (fund column, 7B1 header) for the plain Y/N/blank flags
_FUND_BOOL = (
    ("excl_3c1", "3(c)(1) Exclusion"),
    ("excl_3c7", "3(c)(7) Exclusion"),
    ("is_master_fund", "Master Fund"),
    ("is_feeder_fund", "Feeder Fund"),
    ("is_fund_of_funds", "Fund of Funds"),
    ("invests_in_self_or_related", "Fund Invested Self or Related"),
    ("invests_in_securities", "Fund Invested in Securities"),
    ("sales_limited", "Sales Limited"),
    ("has_subadviser", "Subadviser"),
    ("other_ias_advise", "Other IAs Advise"),
    ("clients_solicited", "Clients Solicited"),
    ("exempt_from_registration", "Exempt from Registration"),
    ("annual_audit", "Annual Audit"),
    ("gaap", "GAAP"),
    ("fs_distributed", "FS Distributed"),
    ("prime_brokers", "Prime Brokers"),
    ("custodians", "Custodians"),
    ("administrator", "Administrator"),
    ("marketing", "Marketing"),
)
_FUND_NUMERIC = (
    ("gross_asset_value", "Gross Asset Value"),
    ("minimum_investment", "Minimum Investment"),
    ("pct_owned_you_or_related", "%Owned You or Related"),
    ("pct_owned_funds", "%Owned Funds"),
    ("pct_owned_non_us", "%Owned Non-US"),
    ("pct_invested", "Percentage Invested"),
    ("pct_assets_valued", "% Assets Valued"),
)
_FUND_TEXT = (
    ("fund_name", "Fund Name"),
    ("reference_id", "ReferenceID"),
    ("org_state", "State"),
    ("org_country", "Country"),
    ("master_fund_name", "Master Fund Name"),
    ("master_fund_id", "Master Fund ID"),
    ("fund_type", "Fund Type"),
    ("fund_type_other", "Fund Type Other"),
    # free text, not a flag: 'Yes' | 'No' | 'Report Not Yet Received' | blank
    ("unqualified_opinion", "Unqualified Opinion"),
)

_TIMESTAMP_FORMATS = (
    # The SEC is not consistent across months: August 2026 writes
    # "08/10/2026 09:45:08 AM", February 2025 writes "2/5/2025 13:19"
    # (no zero padding, 24-hour, no seconds), and the ERA base member often
    # carries a bare date. A format we do not parse costs the filing its date,
    # which silently drops its funds from the current-state mart.
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


def parse_timestamp(value) -> Optional[datetime]:
    """IA 'MM/DD/YYYY hh:mm:ss AM' or ERA 'MM/DD/YYYY'. Anything else -> None."""
    s = clean(value)
    if s is None:
        return None
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _s(v) -> Optional[str]:
    """Python value -> COPY text."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    return str(v)


def _members(zf: zipfile.ZipFile, pattern: re.Pattern) -> List[str]:
    """Matching members, ERA before IA.

    Three filings in the August file appear in BOTH populations (advisers that
    switched from ERA to registered in the same month). The merge keeps the
    last row per key, so IA must be copied last and win.
    """
    return sorted(n for n in zf.namelist() if pattern.search(n))


def _adviser_type(member: str) -> str:
    prefix = Path(member).name.split("_", 1)[0].lower()
    return ADVISER_TYPE.get(prefix, prefix)


def _reader(zf: zipfile.ZipFile, member: str, required: Sequence[str], zip_path) -> Iterator[Dict[str, str]]:
    """DictReader over one member, encoding detected per file. Header drift raises."""
    with zf.open(member) as probe:
        encoding = detect_encoding(probe)
    with zf.open(member) as binary:
        reader = csv.DictReader(open_text(binary, encoding))
        headers = [h.strip() if h else h for h in (reader.fieldnames or [])]
        reader.fieldnames = headers
        missing = [c for c in required if c not in headers]
        if missing:
            raise RuntimeError(
                f"{Path(zip_path).name}/{member}: header drift, missing {missing} "
                f"(got {len(headers)} cols)")
        yield from reader


def _require_members(zf: zipfile.ZipFile, pattern: re.Pattern, label: str, zip_path) -> List[str]:
    members = _members(zf, pattern)
    if not members:
        raise RuntimeError(
            f"{Path(zip_path).name}: no {label} member matching {pattern.pattern!r}; "
            f"members: {sorted(zf.namelist())}")
    return members


def iter_filing_rows(zip_path: Path | str, release_key: str,
                     refused: Optional[Set[str]] = None) -> Iterator[tuple]:
    """Stream the ADV base members into FILING_COLUMNS tuples.

    The base member is the only place a CRD appears, so a row without one is
    dropped: it could only produce funds nothing can be attributed to. Those
    filing ids are added to ``refused`` (when given) so the caller can drop
    their fund rows deliberately — a refused row is a counted refusal, not a
    reason to fail an entire month's release.
    """
    prev_limit = csv.field_size_limit(CSV_FIELD_LIMIT)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for member in _require_members(zf, MEMBER_BASE_RE, "ADV_Base", zip_path):
                adviser_type = _adviser_type(member)
                skipped = 0
                for r in _reader(zf, member, REQUIRED_BASE, zip_path):
                    filing_id = clean(r.get("FilingID"))
                    crd = clean(r.get("1E1"))
                    if not filing_id or not filing_id.isdigit() or not crd or not crd.isdigit():
                        skipped += 1
                        if refused is not None and filing_id:
                            refused.add(filing_id)
                        continue
                    yield tuple(_s(v) for v in (
                        filing_id,
                        crd,
                        adviser_type,
                        clean(r.get("1D")),
                        clean(r.get("1A")),
                        clean(r.get("FormVersion")),
                        parse_timestamp(r.get("DateSubmitted")),
                        release_key,
                    ))
                if skipped:
                    logger.warning(f"[sec_adv_schedule_d] {member}: skipped {skipped} rows "
                                   "without a numeric FilingID/CRD")
    finally:
        csv.field_size_limit(prev_limit)


def iter_fund_rows(zip_path: Path | str, release_key: str) -> Iterator[tuple]:
    """Stream the Schedule D 7.B.(1) members into FUND_COLUMNS tuples."""
    prev_limit = csv.field_size_limit(CSV_FIELD_LIMIT)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for member in _require_members(zf, MEMBER_7B1_RE, "Schedule_D_7B1", zip_path):
                adviser_type = _adviser_type(member)
                skipped = 0
                for r in _reader(zf, member, REQUIRED_7B1, zip_path):
                    row = _fund_tuple(r, adviser_type, release_key)
                    if row is None:
                        skipped += 1
                        continue
                    yield row
                if skipped:
                    logger.warning(f"[sec_adv_schedule_d] {member}: skipped {skipped} rows "
                                   "without a FilingID/Fund ID")
    finally:
        csv.field_size_limit(prev_limit)


def _fund_tuple(r: Dict[str, str], adviser_type: str, release_key: str) -> Optional[tuple]:
    g = r.get
    filing_id = clean(g("FilingID"))
    # 805- + TEN digits; kept verbatim, never re-formatted
    fund_id = clean(g("Fund ID"))
    if not filing_id or not filing_id.isdigit() or not fund_id:
        return None
    fund_name = clean(g("Fund Name"))
    out: Dict[str, object] = {
        "filing_id": filing_id,
        "private_fund_id": fund_id,
        "adviser_type": adviser_type,
        "fund_name_core": name_core(fund_name) if fund_name else None,
        "owners": parse_int(g("Owners")),
        "source_release_key": release_key,
    }
    for col, hdr in _FUND_TEXT:
        out[col] = clean(g(hdr))
    for col, hdr in _FUND_BOOL:
        out[col] = parse_yn(g(hdr))
    for col, hdr in _FUND_NUMERIC:
        out[col] = parse_number(g(hdr))
    return tuple(_s(out[c]) for c, _ in FUND_COLUMNS)
