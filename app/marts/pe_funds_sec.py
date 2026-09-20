"""
pe_funds from Form D (SPEC_117).

One row per PE/VC fund vehicle (the Form D issuer CIK), from its most recent
filing — 65k of 168k offerings are amendments, so counting filings instead of
deduping to the issuer would multiply-count the same fund.

`firm_id` is set only where `app.marts.links` can attribute the fund to exactly
one adviser (~15%). The rest load unattributed: Form D tells us a fund raised
money, not who manages it.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.marts import links

logger = logging.getLogger(__name__)

DATA_SOURCE = "SEC Form D"
STAGING = "mart_pe_funds"
TARGET = "public.pe_funds"
FUND_TYPES = ("Private Equity Fund", "Venture Capital Fund")

COLUMNS: List[Tuple[str, str]] = [
    ("cik", "TEXT"),
    ("firm_id", "INTEGER"),
    ("name", "TEXT"),
    ("vintage_year", "INTEGER"),
    ("target_size_usd_millions", "NUMERIC"),
    ("final_close_usd_millions", "NUMERIC"),
    ("strategy", "TEXT"),
    ("status", "TEXT"),
    ("first_close_date", "DATE"),
    ("sec_file_number", "TEXT"),
    ("data_source", "TEXT"),
    ("updated_at", "TIMESTAMP"),
]
COLUMN_NAMES = [c for c, _ in COLUMNS]

# newest filing per fund vehicle
SOURCE_SQL = f"""
SELECT DISTINCT ON (i.cik)
       i.cik, i.entity_name, o.investment_fund_type, o.date_of_first_sale,
       o.total_offering_amount, o.total_amount_sold, o.is_indefinite, o.file_num,
       o.accession_number
FROM form_d_offerings o
JOIN form_d_issuers i ON i.accession_number = o.accession_number AND i.is_primary
WHERE o.investment_fund_type IN {FUND_TYPES!r}
ORDER BY i.cik, o.date_of_first_sale DESC NULLS LAST, o.loaded_at DESC
"""

ADVISER_SQL = """
SELECT DISTINCT ON (crd_number) crd_number, legal_name, business_name
FROM sec_adv_roster_snapshots
WHERE crd_number IS NOT NULL
ORDER BY crd_number, roster_date DESC
"""

RELATED_SQL = f"""
SELECT i.cik, rp.first_name, rp.middle_name, rp.last_name
FROM form_d_offerings o
JOIN form_d_issuers i ON i.accession_number = o.accession_number AND i.is_primary
JOIN form_d_related_persons rp ON rp.accession_number = o.accession_number
WHERE o.investment_fund_type IN {FUND_TYPES!r}
"""


def _millions(value) -> Optional[float]:
    return float(value) / 1_000_000 if value is not None else None


def _load_related(conn) -> Dict[str, List[str]]:
    related: Dict[str, List[str]] = defaultdict(list)
    for r in conn.execute(text(RELATED_SQL)).mappings():
        name = " ".join(x for x in (r["first_name"], r["middle_name"], r["last_name"]) if x).strip()
        if name:
            related[r["cik"]].append(name)
    return related


def _firm_ids_by_crd(conn) -> Dict[str, int]:
    return {
        r["crd_number"]: r["id"]
        for r in conn.execute(
            text("SELECT id, crd_number FROM pe_firms WHERE crd_number IS NOT NULL")
        ).mappings()
    }


def build(conn, today: Optional[date] = None) -> Dict[str, int]:
    """Upsert SEC-derived funds, attributing the ones we can. Counts by tier."""
    today = today or date.today()
    funds = [dict(r) for r in conn.execute(text(SOURCE_SQL)).mappings()]
    advisers = [
        (r["crd_number"], r["legal_name"], r["business_name"])
        for r in conn.execute(text(ADVISER_SQL)).mappings()
    ]

    core_index = links.build_core_index(advisers)
    attributed = links.attribute(
        [(f["cik"], f["entity_name"]) for f in funds], _load_related(conn), core_index
    )
    firm_ids = _firm_ids_by_crd(conn)

    rows, tiers = [], defaultdict(int)
    for f in funds:
        crd_tier = attributed.get(f["cik"])
        firm_id = None
        if crd_tier:
            firm_id = firm_ids.get(crd_tier[0])
            if firm_id:
                tiers[crd_tier[1]] += 1
        first_sale = f.get("date_of_first_sale")
        strategy = "Private Equity" if f["investment_fund_type"] == "Private Equity Fund" else "Venture Capital"
        rows.append(
            (
                f["cik"],
                firm_id,
                (f.get("entity_name") or "").strip() or None,
                first_sale.year if first_sale else None,
                None if f.get("is_indefinite") else _millions(f.get("total_offering_amount")),
                _millions(f.get("total_amount_sold")),
                strategy,
                "Active",
                first_sale,
                (f.get("file_num") or "").strip() or None,
                DATA_SOURCE,
                today,
            )
        )
    rows = [r for r in rows if r[2]]

    create_staging(conn, STAGING, COLUMNS)
    copied = copy_rows(conn, STAGING, COLUMN_NAMES, rows)
    inserted, updated = merge_staging(
        conn, STAGING, TARGET, COLUMN_NAMES, ["cik"], conflict_where="cik IS NOT NULL"
    )
    drop_staging(conn, STAGING)

    stats = {
        "candidates": copied,
        "inserted": inserted,
        "updated": updated,
        "linked_name_core": tiers["name_core"],
        "linked_related_person": tiers["related_person"],
        "unlinked": copied - sum(tiers.values()),
    }
    logger.info(f"[marts:pe_funds] {stats}")
    return stats
