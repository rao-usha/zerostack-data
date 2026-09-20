"""
pe_firms from Form ADV (SPEC_117).

The universe is advisers that self-declare a private equity or venture capital
fund on Form ADV (`Any PE Funds` / `Any VC Funds`), not merely "has private
funds" — that would pull in hedge, real estate and securitized-fund managers.
Measured: 7,452 of 24,017 advisers.

Additive: rows carry `data_sources = ["SEC ADV"]`, are keyed on CRD, and never
touch the hand-entered firms (which have no CRD at all).
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging

logger = logging.getLogger(__name__)

DATA_SOURCE = "SEC ADV"
STAGING = "mart_pe_firms"
TARGET = "public.pe_firms"

COLUMNS: List[Tuple[str, str]] = [
    ("name", "TEXT"),
    ("legal_name", "TEXT"),
    ("website", "TEXT"),
    ("headquarters_city", "TEXT"),
    ("headquarters_state", "TEXT"),
    ("headquarters_country", "TEXT"),
    ("firm_type", "TEXT"),
    ("primary_strategy", "TEXT"),
    ("aum_usd_millions", "NUMERIC"),
    ("cik", "TEXT"),
    ("sec_file_number", "TEXT"),
    ("crd_number", "TEXT"),
    ("is_sec_registered", "BOOLEAN"),
    ("status", "TEXT"),
    ("data_sources", "JSON"),
    ("last_verified_date", "DATE"),
    ("updated_at", "TIMESTAMP"),
]
COLUMN_NAMES = [c for c, _ in COLUMNS]

# latest roster snapshot per adviser, restricted to declared PE/VC managers,
# with the CIK the entity master resolved for that CRD (when there is one)
SOURCE_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (crd_number) *
    FROM sec_adv_roster_snapshots
    WHERE crd_number IS NOT NULL
    ORDER BY crd_number, roster_date DESC
)
SELECT l.crd_number, l.legal_name, l.business_name, l.sec_number, l.adviser_type,
       l.main_office_city, l.main_office_state, l.main_office_country,
       l.website, l.aum_total, l.sec_status, l.roster_date,
       (l.raw->>'Any PE Funds') AS any_pe,
       (l.raw->>'Any VC Funds') AS any_vc,
       ent_cik.id_value AS cik
FROM latest l
LEFT JOIN core.identifier idr
       ON idr.id_type = 'crd' AND idr.id_value = ltrim(l.crd_number, '0')
LEFT JOIN core.identifier ent_cik
       ON ent_cik.entity_id = idr.entity_id AND ent_cik.id_type = 'cik'
WHERE l.raw->>'Any PE Funds' = 'Y' OR l.raw->>'Any VC Funds' = 'Y'
"""


def _strategy(any_pe: Optional[str], any_vc: Optional[str]) -> str:
    """Descriptive label for primary_strategy."""
    pe, vc = any_pe == "Y", any_vc == "Y"
    if pe and vc:
        return "Private Equity & Venture Capital"
    return "Private Equity" if pe else "Venture Capital"


def _firm_type(any_pe: Optional[str], any_vc: Optional[str]) -> str:
    """The app filters firm_type on short codes ("PE", "VC", "Growth", ...),
    so SEC rows must speak that vocabulary or `?firm_type=PE` silently misses
    them. A manager doing both is filed under PE, with the detail kept in
    primary_strategy."""
    return "PE" if any_pe == "Y" else "VC"


def _row(raw: dict, today: date) -> tuple:
    strategy = _strategy(raw.get("any_pe"), raw.get("any_vc"))
    firm_type = _firm_type(raw.get("any_pe"), raw.get("any_vc"))
    aum = raw.get("aum_total")
    name = (raw.get("business_name") or raw.get("legal_name") or "").strip()
    return (
        name,
        (raw.get("legal_name") or "").strip() or None,
        raw.get("website"),
        raw.get("main_office_city"),
        raw.get("main_office_state"),
        raw.get("main_office_country"),
        firm_type,
        strategy,
        # ERAs do not report AUM; that stays NULL rather than becoming 0
        (aum / 1_000_000) if aum is not None else None,
        raw.get("cik"),
        raw.get("sec_number"),
        raw.get("crd_number"),
        (raw.get("adviser_type") == "ria"),
        "Active" if (raw.get("sec_status") or "").upper() not in ("TERMINATED", "WITHDRAWN") else "Inactive",
        json.dumps([DATA_SOURCE]),
        raw.get("roster_date") or today,
        today,
    )


NAME_IDX = COLUMN_NAMES.index("name")
CRD_IDX = COLUMN_NAMES.index("crd_number")


def _disambiguate(conn, rows: List[tuple]) -> int:
    """`pe_firms.name` is UNIQUE and advisers share names (27 of 7,452 measured).

    Rather than dropping rows or the constraint (POST /pe/firms upserts on
    name), the duplicates get their CRD appended. legal_name keeps the real
    spelling.
    """
    counts: Dict[str, int] = {}
    for row in rows:
        counts[row[NAME_IDX]] = counts.get(row[NAME_IDX], 0) + 1
    taken = {
        r["name"]: r["crd_number"]
        for r in conn.execute(text("SELECT name, crd_number FROM pe_firms")).mappings()
    }
    renamed = 0
    for i, row in enumerate(rows):
        name, crd = row[NAME_IDX], row[CRD_IDX]
        clashes_in_batch = counts.get(name, 0) > 1
        clashes_in_db = name in taken and taken[name] != crd
        if clashes_in_batch or clashes_in_db:
            row = list(row)
            row[NAME_IDX] = f"{name} (CRD {crd})"
            rows[i] = tuple(row)
            renamed += 1
    return renamed


def build(conn, today: Optional[date] = None) -> Dict[str, int]:
    """Upsert SEC-derived firms. Returns inserted/updated counts."""
    today = today or date.today()
    rows = [_row(dict(r), today) for r in conn.execute(text(SOURCE_SQL)).mappings()]
    rows = [r for r in rows if r[0]]  # a firm with no name is not a firm

    # One row per adviser: an entity carrying several CIKs would otherwise
    # yield the same CRD twice and look like a name clash with itself.
    by_crd: Dict[str, tuple] = {}
    for row in rows:
        by_crd.setdefault(row[CRD_IDX], row)
    rows = list(by_crd.values())

    renamed = _disambiguate(conn, rows)

    create_staging(conn, STAGING, COLUMNS)
    copied = copy_rows(conn, STAGING, COLUMN_NAMES, rows)
    # data_sources is `json`, which Postgres cannot compare; it is constant for
    # these rows anyway, so it is excluded from the changed-row check.
    compare = [c for c in COLUMN_NAMES
               if c not in ("crd_number", "data_sources", "last_verified_date", "updated_at")]
    inserted, updated = merge_staging(
        conn, STAGING, TARGET, COLUMN_NAMES, ["crd_number"], compare_columns=compare,
        conflict_where="crd_number IS NOT NULL",
    )
    drop_staging(conn, STAGING)

    stats = {"candidates": copied, "inserted": inserted, "updated": updated,
             "name_disambiguated": renamed}
    logger.info(f"[marts:pe_firms] {stats}")
    return stats
