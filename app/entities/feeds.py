"""
Feeds: SEC bulk tables -> core.source_record (SPEC_116).

One row per (source, native id). Only identifiers travel; names ride along for
the canonical row and aliases but never merge anything.

Scope is the PE-relevant subset, not all 985k EDGAR filers: advisers, 13F
filers, Form D issuers, and the filers those reference (plus 8-K filers and
insider-reporting owners).

Identifier traps handled here, measured on the live data:
- `sec_13f_filings.crd_number` is zero-padded ('000106326'), ADV CRDs are not
  ('8361'). Both are stored canonically (digits, no leading zeros).
- All-zero / repeated-digit placeholders are dropped rather than stored.
- The ADV roster is a snapshot series, so only the latest row per CRD is fed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.entities import norm
from app.entities.resolve_core import _cik, _crd

logger = logging.getLogger(__name__)

STAGING = "core_source_record"
TARGET = "core.source_record"

COLUMNS: List[Tuple[str, str]] = [
    ("record_key", "TEXT"),
    ("source", "VARCHAR(32)"),
    ("native_id", "TEXT"),
    ("legal_name", "TEXT"),
    ("name_norm", "TEXT"),
    ("name_norm_version", "VARCHAR(8)"),
    ("ein", "TEXT"),
    ("cik", "TEXT"),
    ("crd", "TEXT"),
    ("lei", "TEXT"),
    ("uei", "TEXT"),
    ("state_entity_id", "TEXT"),
    ("state", "VARCHAR(2)"),
    ("zip5", "VARCHAR(5)"),
    ("domain", "TEXT"),
    ("observed_at", "TIMESTAMP"),
]
COLUMN_NAMES = [c for c, _ in COLUMNS]


@dataclass
class Feed:
    name: str
    sql: str
    key_prefix: str


# The CIK universe we care about: everything referenced by an adviser-side or
# PE-side source. `sec_filers` alone is 985k rows and mostly irrelevant here.
_RELEVANT_CIKS = """
    SELECT cik FROM form_d_issuers
    UNION SELECT cik FROM sec_13f_filings
    UNION SELECT cik FROM sec_8k_index
    UNION SELECT rptowner_cik AS cik FROM sec_insider_owners
    UNION SELECT issuer_cik AS cik FROM sec_insider_filings
"""

FEEDS: List[Feed] = [
    Feed(
        "adv",
        # latest snapshot per CRD: the roster is a monthly series
        """
        SELECT DISTINCT ON (crd_number)
               crd_number AS native_id,
               COALESCE(legal_name, business_name) AS legal_name,
               crd_number AS crd,
               NULL::TEXT AS cik,
               NULL::TEXT AS ein,
               main_office_state AS state,
               website AS domain,
               roster_date::TIMESTAMP AS observed_at
        FROM sec_adv_roster_snapshots
        WHERE crd_number IS NOT NULL
        ORDER BY crd_number, roster_date DESC
        """,
        "adv",
    ),
    Feed(
        "iapd",
        """
        SELECT DISTINCT ON (crd_number)
               crd_number AS native_id,
               COALESCE(legal_name, business_name) AS legal_name,
               crd_number AS crd,
               NULL::TEXT AS cik,
               NULL::TEXT AS ein,
               main_office_state AS state,
               website AS domain,
               edition_date::TIMESTAMP AS observed_at
        FROM sec_adv_feed_firm_state
        WHERE crd_number IS NOT NULL
        ORDER BY crd_number, edition_date DESC
        """,
        "iapd",
    ),
    Feed(
        "f13",
        # The important one: a 13F cover page carries CIK *and* CRD on the same
        # record, so the union-find merges adviser and filer without a bridge.
        """
        SELECT DISTINCT ON (cik)
               cik AS native_id,
               filing_manager_name AS legal_name,
               crd_number AS crd,
               cik,
               NULL::TEXT AS ein,
               filing_manager_state_or_country AS state,
               NULL::TEXT AS domain,
               filing_date::TIMESTAMP AS observed_at
        FROM sec_13f_filings
        WHERE cik IS NOT NULL
        ORDER BY cik, filing_date DESC
        """,
        "f13",
    ),
    Feed(
        "formd",
        """
        SELECT DISTINCT ON (cik)
               cik AS native_id,
               entity_name AS legal_name,
               NULL::TEXT AS crd,
               cik,
               NULL::TEXT AS ein,
               state_or_country AS state,
               NULL::TEXT AS domain,
               loaded_at AS observed_at
        FROM form_d_issuers
        WHERE cik IS NOT NULL
        ORDER BY cik, loaded_at DESC
        """,
        "formd",
    ),
    Feed(
        "edgar",
        f"""
        SELECT f.cik AS native_id,
               f.name AS legal_name,
               NULL::TEXT AS crd,
               f.cik,
               f.ein,
               COALESCE(f.state_of_incorporation, f.biz_state2, f.biz_state_or_country) AS state,
               f.website AS domain,
               f.loaded_at AS observed_at
        FROM sec_filers f
        JOIN ({_RELEVANT_CIKS}) rel ON rel.cik = f.cik
        """,
        "edgar",
    ),
]


def _row(feed: Feed, raw: dict) -> Optional[tuple]:
    """Source row -> core.source_record tuple, or None when it carries no key."""
    cik = _cik(raw.get("cik"))
    crd = _crd(raw.get("crd"))
    ein = norm.clean_ein(raw.get("ein"))
    native_id = str(raw.get("native_id") or "").strip()
    if not native_id:
        return None
    legal_name = (raw.get("legal_name") or "").strip() or None
    return (
        f"{feed.key_prefix}:{native_id}",
        feed.name,
        native_id,
        legal_name,
        norm.norm(legal_name) if legal_name else None,
        norm.NAME_NORM_VERSION,
        ein,
        cik,
        crd,
        None,  # lei: not carried by these sources yet
        None,  # uei
        None,  # state_entity_id
        norm.state2(raw.get("state")),
        None,  # zip5: not carried by these sources yet
        norm.domain(raw.get("domain")),
        raw.get("observed_at"),
    )


FETCH_BATCH = 20_000


def _fetch_rows(conn, feed: Feed) -> List[tuple]:
    """Read the feed fully before COPY starts.

    psycopg2 allows only one operation at a time per connection: streaming a
    SELECT straight into COPY FROM STDIN on the same connection aborts the
    transaction ("no COPY in progress"). Batched fetch keeps memory bounded.
    """
    rows: List[tuple] = []
    result = conn.execute(text(feed.sql)).mappings()
    while True:
        batch = result.fetchmany(FETCH_BATCH)
        if not batch:
            break
        for raw in batch:
            row = _row(feed, dict(raw))
            if row is not None:
                rows.append(row)
    return rows


def run_feeds(conn, feeds: Optional[Sequence[Feed]] = None,
              progress: Optional[Callable[[str, float], None]] = None) -> Dict[str, int]:
    """Refresh core.source_record from the SEC tables. Returns rows per feed."""
    feeds = list(feeds if feeds is not None else FEEDS)
    counts: Dict[str, int] = {}
    for i, feed in enumerate(feeds):
        if progress:
            progress(f"feed {feed.name}", 100.0 * i / max(1, len(feeds)))
        rows = _fetch_rows(conn, feed)
        create_staging(conn, STAGING, COLUMNS)
        copied = copy_rows(conn, STAGING, COLUMN_NAMES, rows)
        inserted, updated = merge_staging(conn, STAGING, TARGET, COLUMN_NAMES, ["record_key"])
        drop_staging(conn, STAGING)
        counts[feed.name] = copied
        logger.info(
            f"[entities:feeds] {feed.name}: staged {copied}, inserted {inserted}, updated {updated}"
        )
    return counts
