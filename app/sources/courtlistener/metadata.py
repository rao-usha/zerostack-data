"""
CourtListener metadata utilities.

Handles:
- Table definitions for bankruptcy dockets
- CREATE TABLE SQL generation
- Data parsing from API search results
- Bankruptcy chapter detection and classification
"""

import logging
import re
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

TABLE_NAME = "courtlistener_dockets"
DATASET_ID = "courtlistener_dockets"
DISPLAY_NAME = "CourtListener Bankruptcy Dockets"
DESCRIPTION = (
    "Federal bankruptcy court docket data from CourtListener (Free Law Project). "
    "Includes Chapter 7, 11, and 13 bankruptcy filings with case names, "
    "court information, filing dates, and assigned judges."
)

COLUMNS = [
    "docket_id",
    "case_name",
    "case_number",
    "court_id",
    "court_name",
    "date_filed",
    "date_terminated",
    "chapter",
    "nature_of_suit",
    "cause",
    "assigned_to",
    "referred_to",
    "source_url",
]

CONFLICT_COLUMNS = ["docket_id"]

UPDATE_COLUMNS = [
    "case_name",
    "case_number",
    "court_id",
    "court_name",
    "date_filed",
    "date_terminated",
    "chapter",
    "nature_of_suit",
    "cause",
    "assigned_to",
    "referred_to",
    "source_url",
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    docket_id INTEGER PRIMARY KEY,
    case_name TEXT,
    case_number TEXT,
    court_id TEXT,
    court_name TEXT,
    date_filed DATE,
    date_terminated DATE,
    chapter TEXT,
    nature_of_suit TEXT,
    cause TEXT,
    assigned_to TEXT,
    referred_to TEXT,
    source_url TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_court
    ON {TABLE_NAME} (court_id);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_chapter
    ON {TABLE_NAME} (chapter);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_filed
    ON {TABLE_NAME} (date_filed);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_case_name
    ON {TABLE_NAME} (case_name);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_terminated
    ON {TABLE_NAME} (date_terminated);

COMMENT ON TABLE {TABLE_NAME} IS 'CourtListener bankruptcy court docket records';
"""

# Patterns for detecting bankruptcy chapter from case name or nature of suit
CHAPTER_PATTERNS = [
    (re.compile(r"\bch(?:apter)?[\s.]*7\b", re.IGNORECASE), "7"),
    (re.compile(r"\bch(?:apter)?[\s.]*11\b", re.IGNORECASE), "11"),
    (re.compile(r"\bch(?:apter)?[\s.]*12\b", re.IGNORECASE), "12"),
    (re.compile(r"\bch(?:apter)?[\s.]*13\b", re.IGNORECASE), "13"),
    (re.compile(r"\bch(?:apter)?[\s.]*15\b", re.IGNORECASE), "15"),
]


def detect_chapter(
    case_name: str = "",
    nature_of_suit: str = "",
    cause: str = "",
) -> Optional[str]:
    """
    Detect bankruptcy chapter from case metadata.

    Checks case name, nature of suit, and cause fields for
    chapter references (7, 11, 12, 13, 15).

    Args:
        case_name: Docket case name
        nature_of_suit: Nature of suit field
        cause: Cause field

    Returns:
        Chapter string (e.g., "7", "11", "13") or None
    """
    combined = f"{case_name} {nature_of_suit} {cause}"
    for pattern, chapter in CHAPTER_PATTERNS:
        if pattern.search(combined):
            return chapter
    return None


def extract_docket_id_from_url(url: str) -> Optional[int]:
    """
    Extract numeric docket ID from a CourtListener URL.

    Args:
        url: CourtListener docket URL

    Returns:
        Integer docket ID or None
    """
    try:
        # URL pattern: /api/rest/v4/dockets/12345/
        parts = urlparse(url).path.rstrip("/").split("/")
        for part in reversed(parts):
            if part.isdigit():
                return int(part)
    except (ValueError, AttributeError):
        pass
    return None


def parse_docket(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a single CourtListener search result into a database row.

    The search API returns results in a different format than the
    docket detail API. This handles the search result format.

    Args:
        result: Single result from CourtListener search API

    Returns:
        Parsed row dict or None if missing required fields
    """
    try:
        # Extract docket_id from different possible fields
        docket_id = result.get("docket_id")
        if docket_id is None:
            # Try extracting from absolute_url or docket URL
            docket_url = result.get("absolute_url", "") or result.get("docket", "")
            docket_id = extract_docket_id_from_url(str(docket_url))

        if docket_id is None:
            logger.debug(f"Skipping result with no docket_id: {result.get('caseName', 'unknown')}")
            return None

        case_name = result.get("caseName") or result.get("case_name", "")
        nature_of_suit = result.get("suitNature") or result.get("nature_of_suit", "")
        cause = result.get("cause", "")

        # Detect bankruptcy chapter
        chapter = detect_chapter(case_name, nature_of_suit, cause)

        # Build source URL
        absolute_url = result.get("absolute_url", "")
        if absolute_url and not absolute_url.startswith("http"):
            source_url = f"https://www.courtlistener.com{absolute_url}"
        else:
            source_url = absolute_url or None

        return {
            "docket_id": int(docket_id),
            "case_name": case_name or None,
            "case_number": result.get("docketNumber") or result.get("case_number"),
            "court_id": result.get("court_id") or result.get("court"),
            "court_name": result.get("court_citation_string") or result.get("court_name"),
            "date_filed": result.get("dateFiled") or result.get("date_filed"),
            "date_terminated": result.get("dateTerminated") or result.get("date_terminated"),
            "chapter": chapter,
            "nature_of_suit": nature_of_suit or None,
            "cause": cause or None,
            "assigned_to": result.get("assignedTo") or result.get("assigned_to_str"),
            "referred_to": result.get("referredTo") or result.get("referred_to_str"),
            "source_url": source_url,
        }

    except Exception as e:
        logger.warning(f"Failed to parse CourtListener docket: {e}")
        return None


def parse_dockets(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of CourtListener search results into database rows.

    Args:
        results: List of search result dicts from API

    Returns:
        List of parsed row dicts (skips results with missing required fields)
    """
    rows = []
    seen_ids = set()

    for result in results:
        row = parse_docket(result)
        if row and row["docket_id"] not in seen_ids:
            seen_ids.add(row["docket_id"])
            rows.append(row)

    logger.info(f"Parsed {len(rows)}/{len(results)} CourtListener docket records")
    return rows
