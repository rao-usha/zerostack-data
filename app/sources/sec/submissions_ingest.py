"""
SEC EDGAR submissions metadata ingest — PLAN_062 rev_02 Step 1a.

Fetches /submissions/CIK{cik}.json for each CIK we already have XBRL data for,
extracts (sicCode, sicDescription, stateOfIncorporation, business address state,
fiscalYearEnd, name), maps SIC → NAICS-2, and upserts into sec_company_metadata.

This unblocks TabDDPM v2 retrain with NAICS-2 conditioning, which the v1 training
run identified as the dominant gap (rev_02 root-cause analysis).

CLI:
    python -m app.sources.sec.submissions_ingest --all  # all CIKs in sec_income_statement
    python -m app.sources.sec.submissions_ingest --ciks 0000320193 0000789019
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.sources.sec.client import SECClient
from app.sources.sec.models import SECCompanyMetadata

logger = logging.getLogger(__name__)


# =======================================================================
# SIC → NAICS-2 crosswalk
# =======================================================================
# Based on the Census Bureau's published SIC-to-NAICS concordance.
# We collapse to NAICS-2 (broad sector) — sufficient for TabDDPM conditioning.
# Maps SIC 2-digit prefix → NAICS-2 string.

_SIC2_TO_NAICS2: Dict[str, str] = {
    # Agriculture, Forestry, Fishing & Hunting
    "01": "11", "02": "11", "07": "11", "08": "11", "09": "11",
    # Mining, Quarrying, Oil & Gas Extraction
    "10": "21", "12": "21", "13": "21", "14": "21",
    # Construction
    "15": "23", "16": "23", "17": "23",
    # Manufacturing (SIC 20-39 → NAICS 31-33, collapsed to "31")
    "20": "31", "21": "31", "22": "31", "23": "31", "24": "31",
    "25": "31", "26": "31", "27": "31", "28": "31", "29": "31",
    "30": "31", "31": "31", "32": "31", "33": "31", "34": "31",
    "35": "31", "36": "31", "37": "31", "38": "31", "39": "31",
    # Transportation, Communications, Electric, Gas, Sanitary Services
    "40": "48", "41": "48", "42": "48", "43": "48", "44": "48",
    "45": "48", "46": "48", "47": "48",
    "48": "51",   # Communications → Information
    "49": "22",   # Utilities
    # Wholesale Trade
    "50": "42", "51": "42",
    # Retail Trade
    "52": "44", "53": "44", "54": "44", "55": "44", "56": "44",
    "57": "44", "58": "44", "59": "44",
    # Finance, Insurance, Real Estate
    "60": "52", "61": "52", "62": "52", "63": "52", "64": "52",
    "65": "53", "67": "52",
    # Services
    "70": "72",   # Hotels / Lodging → Accommodation & Food
    "72": "81",   # Personal Services → Other Services
    "73": "54",   # Business Services → Professional, Scientific & Technical
    "75": "81",   # Auto Repair → Other Services
    "76": "81",
    "78": "71",   # Motion Pictures → Arts, Entertainment, Recreation
    "79": "71",
    "80": "62",   # Health Services
    "81": "54",   # Legal Services → Professional
    "82": "61",   # Education
    "83": "62",   # Social Services → Health Care & Social Assistance
    "84": "71",   # Museums → Arts
    "86": "81",   # Membership orgs → Other Services
    "87": "54",   # Engineering/Accounting → Professional
    "88": "62",   # Private Households (atypical for SEC filers)
    "89": "81",
    # Public Administration
    "91": "92", "92": "92", "93": "92", "94": "92", "95": "92",
    "96": "92", "97": "92", "99": "92",
}


def sic_to_naics_2(sic_code: Optional[str]) -> Optional[str]:
    """Map a 4-digit SIC code to a NAICS-2 broad-sector bucket."""
    if not sic_code:
        return None
    sic = str(sic_code).zfill(4)
    return _SIC2_TO_NAICS2.get(sic[:2])


# =======================================================================
# Submission JSON parsing
# =======================================================================

def parse_submission(cik: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the fields we care about from an EDGAR submissions JSON blob.

    Note: EDGAR uses 'sic' (not 'sicCode') for the numeric code; falls back to
    'sicCode' for forward-compat with any other JSON shapes.
    """
    sic = (data.get("sic") or data.get("sicCode") or "").strip() or None
    sic_desc = (data.get("sicDescription") or "").strip() or None
    incorp = (data.get("stateOfIncorporation") or "").strip() or None
    fy_end = (data.get("fiscalYearEnd") or "").strip() or None
    name = (data.get("name") or "").strip() or None

    biz_state = None
    addresses = data.get("addresses") or {}
    biz_addr = addresses.get("business") or {}
    if biz_addr:
        biz_state = (biz_addr.get("stateOrCountry") or "").strip() or None

    return {
        "cik": str(cik).zfill(10),
        "company_name": name,
        "sic_code": sic,
        "sic_description": sic_desc,
        "naics_2": sic_to_naics_2(sic),
        "state_of_incorporation": incorp,
        "business_state": biz_state,
        "fiscal_year_end": fy_end,
        "ingested_at": date.today(),
    }


# =======================================================================
# Orchestrator
# =======================================================================

def get_ciks_with_xbrl(db: Session) -> List[str]:
    """Return the distinct CIKs that have at least one row in sec_income_statement."""
    rows = db.execute(text("SELECT DISTINCT cik FROM sec_income_statement ORDER BY cik")).fetchall()
    return [r[0] for r in rows]


async def _ingest_one(db: Session, client: SECClient, cik: str) -> Optional[Dict]:
    try:
        data = await client.get_company_submissions(cik)
    except Exception as e:
        logger.warning("Fetch failed for CIK %s: %s", cik, e)
        return None

    parsed = parse_submission(cik, data)

    # Upsert
    stmt = pg_insert(SECCompanyMetadata.__table__).values(**parsed)
    update_cols = {k: stmt.excluded[k] for k in parsed if k != "cik"}
    stmt = stmt.on_conflict_do_update(index_elements=["cik"], set_=update_cols)
    db.execute(stmt)
    db.commit()
    return parsed


async def bulk_ingest_submissions(ciks: List[str], log_every: int = 100) -> Dict[str, Any]:
    """Fetch + upsert metadata for each CIK. Returns aggregate stats."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    # Override the registry rate limit (10/MIN — way too slow; EDGAR allows 10/SEC).
    # Using max_concurrency=4 with the BaseAPIClient's per-request rate-limit
    # interval set to ~0.15s gives us ~6 req/sec aggregate — well under EDGAR's
    # 10/sec ceiling with safety margin.
    client = SECClient(max_concurrency=4)
    # Stomp the rate-limit interval after construction (BaseAPIClient stores it
    # as an instance attr; the constructor pulled it from registry as 6.0)
    client.rate_limit_interval = 0.15

    succeeded = 0
    failed = 0
    naics_counts: Dict[str, int] = {}
    sic_unmapped = 0

    try:
        for idx, cik in enumerate(ciks, start=1):
            result = await _ingest_one(db, client, cik)
            if result:
                succeeded += 1
                if result.get("naics_2"):
                    naics_counts[result["naics_2"]] = naics_counts.get(result["naics_2"], 0) + 1
                elif result.get("sic_code"):
                    sic_unmapped += 1
            else:
                failed += 1

            if idx % log_every == 0 or idx == len(ciks):
                logger.info(
                    "Submissions ingest: %d/%d (%d ok, %d failed, %d NAICS-unmapped SICs)",
                    idx, len(ciks), succeeded, failed, sic_unmapped,
                )
    finally:
        await client.close()
        db.close()

    return {
        "ciks_requested": len(ciks),
        "succeeded": succeeded,
        "failed": failed,
        "naics_2_distribution": dict(sorted(naics_counts.items())),
        "sic_unmapped_count": sic_unmapped,
    }


# =======================================================================
# CLI
# =======================================================================

def main():
    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--all", action="store_true", help="Ingest all CIKs with XBRL data")
    grp.add_argument("--ciks", nargs="+", help="Explicit CIKs to ingest")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        if args.all:
            ciks = get_ciks_with_xbrl(db)
        else:
            ciks = [str(c).zfill(10) for c in args.ciks]
        logger.info("Ingesting metadata for %d CIKs", len(ciks))
    finally:
        db.close()

    stats = asyncio.run(bulk_ingest_submissions(ciks))
    print()
    print("=" * 70)
    print(f"SEC submissions ingest complete: {stats['succeeded']} ok / {stats['failed']} failed")
    print(f"SIC codes with no NAICS-2 mapping: {stats['sic_unmapped_count']}")
    print("NAICS-2 distribution:")
    for naics, count in stats["naics_2_distribution"].items():
        print(f"  {naics}: {count}")


if __name__ == "__main__":
    main()
