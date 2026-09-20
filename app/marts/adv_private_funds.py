"""
sec_adv_private_funds: current state of Form ADV Schedule D 7.B.(1) (SPEC_118).

The loader writes filing grain -- one row per fund per ADV filing -- because
monthly releases arrive in whatever order the SEC uploads (and re-uploads)
them. This mart collapses that to one row per (adviser CRD, private fund id),
taken from the newest filing that reports the fund, and is recomputed from
scratch on every run: replaying releases in any order lands on exactly the
same rows.

Columns mirror ``public.sec_adv_private_funds`` in migration 0010, which owns
the DDL for all three tables.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from app.core.copy_loader import STAGING_SCHEMA, create_staging, drop_staging, merge_staging
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

STAGING = "mart_adv_private_funds"
TARGET = "public.sec_adv_private_funds"
FILINGS = "public.sec_adv_filings"
FUND_FILINGS = "public.sec_adv_private_fund_filings"
KEY_COLUMNS = ["crd_number", "private_fund_id"]

# (mart column, expression over the source aliases f = filing, pff = 7B1 row)
SELECT_EXPRESSIONS: List[Tuple[str, str, str]] = [
    ("crd_number", "TEXT", "f.crd_number"),
    ("private_fund_id", "TEXT", "pff.private_fund_id"),
    ("filing_id", "BIGINT", "f.filing_id"),
    ("adviser_type", "TEXT", "f.adviser_type"),
    # what the mart is asserting: this was true as of the filing it came from
    ("as_of_date", "DATE", "f.filed_at::date"),
    ("fund_name", "TEXT", "pff.fund_name"),
    ("fund_name_core", "TEXT", "pff.fund_name_core"),
    ("fund_type", "TEXT", "pff.fund_type"),
    ("fund_type_other", "TEXT", "pff.fund_type_other"),
    ("gross_asset_value", "NUMERIC", "pff.gross_asset_value"),
    ("is_master_fund", "BOOLEAN", "pff.is_master_fund"),
    ("is_feeder_fund", "BOOLEAN", "pff.is_feeder_fund"),
    ("master_fund_id", "TEXT", "pff.master_fund_id"),
    ("master_fund_name", "TEXT", "pff.master_fund_name"),
    ("org_state", "TEXT", "pff.org_state"),
    ("org_country", "TEXT", "pff.org_country"),
    ("excl_3c1", "BOOLEAN", "pff.excl_3c1"),
    ("excl_3c7", "BOOLEAN", "pff.excl_3c7"),
    ("updated_at", "TIMESTAMP", "CAST(:today AS TIMESTAMP)"),
]
COLUMNS: List[Tuple[str, str]] = [(c, t) for c, t, _e in SELECT_EXPRESSIONS]
COLUMN_NAMES = [c for c, _ in COLUMNS]

# An undated filing cannot be ranked against the others and has no as_of_date
# to report, so it is left out and counted rather than dated with today.
UNDATED_SQL = f"""
SELECT COUNT(*) FROM {FUND_FILINGS} pff
JOIN {FILINGS} f ON f.filing_id = pff.filing_id
WHERE f.filed_at IS NULL
"""

# Newest filing per (adviser, fund). filing_id breaks ties: two amendments on
# the same day would otherwise be picked arbitrarily and the mart would differ
# between identical runs.
SOURCE_SQL = """
SELECT DISTINCT ON (f.crd_number, pff.private_fund_id)
       {select_list}
FROM {fund_filings} pff
JOIN {filings} f ON f.filing_id = pff.filing_id
WHERE f.crd_number IS NOT NULL
  AND pff.private_fund_id IS NOT NULL
  AND f.filed_at IS NOT NULL
ORDER BY f.crd_number, pff.private_fund_id, f.filed_at DESC, f.filing_id DESC
""".format(
    select_list=", ".join(f"{e} AS {qi(c)}" for c, _t, e in SELECT_EXPRESSIONS),
    fund_filings=FUND_FILINGS,
    filings=FILINGS,
)


def _staging_table() -> str:
    return f"{qi(STAGING_SCHEMA)}.{qi(STAGING)}"


INSERT_STAGING_SQL = (
    f"INSERT INTO {_staging_table()} ({', '.join(qi(c) for c in COLUMN_NAMES)})\n{SOURCE_SQL}"
)

# The mart is a projection, not a log: a fund the filings no longer support has
# to leave, or a withdrawn release stays visible forever.
DELETE_SQL = f"""
DELETE FROM {TARGET} t
WHERE NOT EXISTS (
    SELECT 1 FROM {_staging_table()} s
    WHERE s.crd_number = t.crd_number AND s.private_fund_id = t.private_fund_id
)
"""


def build(conn, today: Optional[date] = None) -> Dict[str, int]:
    """Recompute sec_adv_private_funds from the filing-grain tables."""
    today = today or date.today()

    create_staging(conn, STAGING, COLUMNS)
    candidates = conn.execute(text(INSERT_STAGING_SQL), {"today": today}).rowcount
    inserted, updated = merge_staging(conn, STAGING, TARGET, COLUMN_NAMES, KEY_COLUMNS)
    deleted = conn.execute(text(DELETE_SQL)).rowcount
    drop_staging(conn, STAGING)

    stats = {
        "candidates": int(candidates or 0),
        "inserted": inserted,
        "updated": updated,
        "deleted": int(deleted or 0),
        "skipped_undated": conn.execute(text(UNDATED_SQL)).scalar() or 0,
    }
    logger.info(f"[marts:adv_private_funds] {stats}")
    return stats
