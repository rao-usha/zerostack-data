"""
CIK list builder for bulk SEC XBRL ingest (PLAN_062 W1.A).

Sources CIKs from SEC EDGAR's public `company_tickers.json` (the canonical
authoritative list of public-company CIK + ticker mappings) and provides
ranked subsets matching the PLAN_062 acceptance criteria (≥1,500 CIKs).

The fetched list contains ~10,000 publicly-traded US issuers. For Phase A1
TabDDPM training, we need enough sector diversity and revenue scale — taking
the first N (sorted by EDGAR's intrinsic ordering, which roughly correlates
with market cap / filing volume) yields the desired training universe.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Dict

import httpx

logger = logging.getLogger(__name__)


EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
USER_AGENT = (
    "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"
)


def normalize_cik(cik: int | str) -> str:
    """Pad a CIK to 10 digits (EDGAR canonical form)."""
    return str(cik).zfill(10)


def fetch_all_edgar_ciks() -> List[Dict]:
    """
    Fetch EDGAR's full public-company CIK + ticker mapping.

    Returns a list of dicts: [{"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."}, ...]

    SEC explicitly publishes this file for programmatic use; no API key required.
    EDGAR rate limit: 10 req/sec (one call here counts as one).
    """
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}

    try:
        with httpx.Client(timeout=30.0, headers=headers) as client:
            response = client.get(EDGAR_TICKERS_URL)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        logger.error("Failed to fetch EDGAR company tickers: %s", exc)
        raise

    # EDGAR returns a dict keyed by 0, 1, 2... with values {cik_str, ticker, title}
    results = []
    for _, entry in data.items():
        results.append(
            {
                "cik": normalize_cik(entry["cik_str"]),
                "ticker": entry["ticker"],
                "name": entry["title"],
            }
        )

    logger.info("Fetched %d CIK/ticker entries from EDGAR", len(results))
    return results


def get_target_universe(limit: Optional[int] = 2500) -> List[str]:
    """
    Return the list of CIKs to ingest for Phase A1 training-data prep.

    Args:
        limit: Maximum number of CIKs to return. Default 2500 matches the
               PLAN_062 W1.A target (S&P 500 + Russell 2000 ≈ 2,500). Set to
               None to return all ~10K public CIKs.

    Returns:
        List of 10-digit normalized CIKs.
    """
    all_entries = fetch_all_edgar_ciks()

    if limit is not None and limit < len(all_entries):
        # EDGAR's intrinsic ordering correlates roughly with filing frequency
        # and market cap; first N captures the most filing-active issuers.
        all_entries = all_entries[:limit]

    return [entry["cik"] for entry in all_entries]
