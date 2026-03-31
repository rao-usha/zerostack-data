"""
Form 990 HTML Parser — PLAN_037 LP Conviction 2.0

Parses Schedule D from EDGAR Form 990 HTML filings to extract PE/VC investment
holdings for foundations and endowments.

Usage:
    from app.sources.lp_collection.form_990_html_parser import parse_form_990_schedule_d

    records = parse_form_990_schedule_d(html_text, lp_name="Ford Foundation")
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Try to import BeautifulSoup (may not be installed)
try:
    from bs4 import BeautifulSoup, Tag

    BS4_AVAILABLE = True
except ImportError:
    BeautifulSoup = None  # type: ignore[assignment,misc]
    Tag = None  # type: ignore[assignment,misc]
    BS4_AVAILABLE = False
    logger.warning("beautifulsoup4 not installed — Form 990 HTML parsing unavailable")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Text signals that indicate a Schedule D / investment table
SCHEDULE_D_SIGNALS = {
    "Part XIV",
    "Other Assets",
    "Investment",
    "Fund",
    "Private Equity",
    "Partnership",
    "Venture",
}

# Investment type strings that suggest PE/VC holdings
PE_TYPE_KEYWORDS = {
    "pe",
    "vc",
    "partnership",
    "private equity",
    "venture",
    "fund",
}

# Investment name keywords that suggest PE/VC fund names
PE_NAME_KEYWORDS = {
    "fund",
    "partners",
    "capital",
    "ventures",
    "lp",
    "l.p.",
}

# Words to strip when inferring a GP name from a fund name
GP_STOP_WORDS = {"fund", "partners", "capital", "ventures", "lp", "l.p.", "i", "ii",
                 "iii", "iv", "v", "vi", "vii", "viii", "ix", "x", "xi", "xii",
                 "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _cell_text(cell) -> str:
    """Return stripped text content of a BeautifulSoup element."""
    return cell.get_text(separator=" ", strip=True)


def _parse_number(text: str) -> Optional[float]:
    """
    Parse a number from a table cell string.

    Handles:
        - Comma-separated values:  "1,234,567"
        - Parenthesized negatives: "(1,234)"
        - Dollar signs:            "$1,234,567"
        - Plain integers/floats:   "1234567"

    Returns float or None if unparseable.
    """
    if not text:
        return None
    # Strip currency symbols, whitespace
    cleaned = text.strip().replace("$", "").replace(",", "").replace(" ", "")
    # Handle parenthesized negatives: (1234) → -1234
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _is_header_row(cells: list) -> bool:
    """
    Return True if a table row looks like a header.

    Heuristics:
    - All cells are <th> elements
    - All cells contain no numbers (only text labels)
    - First cell text matches common header labels
    """
    if not cells:
        return True

    # If any cell is a <th>, treat as header
    if any(getattr(c, "name", None) == "th" for c in cells):
        return True

    # If no cell contains a digit, likely a header
    texts = [_cell_text(c) for c in cells]
    if not any(re.search(r"\d", t) for t in texts):
        return True

    return False


def _infer_gp_name(fund_name: str) -> str:
    """
    Infer a GP/manager name from a fund name string.

    Strategy: take the first 1-3 meaningful words that appear before the
    typical fund suffix tokens (Fund, Partners, Capital, Ventures).

    Examples:
        "KKR Americas Fund XII"       → "KKR"
        "Sequoia Capital Fund XV"     → "Sequoia Capital"
        "Blackstone Real Estate Partners IX" → "Blackstone Real Estate"
        "Accel Partners Growth Fund"  → "Accel"
    """
    if not fund_name:
        return ""

    words = fund_name.split()
    prefix: list[str] = []

    for word in words:
        # Stop at a known suffix token (case-insensitive)
        if word.lower().rstrip(".") in GP_STOP_WORDS:
            # Include "Partners" / "Capital" if it seems like part of the firm name
            # (i.e., it's one of the first two words)
            if word.lower() in {"partners", "capital"} and len(prefix) <= 1:
                prefix.append(word)
            break
        prefix.append(word)
        # Cap at 3 words to avoid grabbing fund-specific qualifiers
        if len(prefix) >= 3:
            break

    return " ".join(prefix).strip()


def _is_pe_investment(investment_name: str, investment_type: str) -> bool:
    """
    Return True if a row looks like a PE/VC fund based on name or type.
    """
    name_lower = investment_name.lower()
    type_lower = investment_type.lower()

    # Check investment_type field first (more reliable)
    for kw in PE_TYPE_KEYWORDS:
        if kw in type_lower:
            return True

    # Check investment_name for fund-name patterns
    for kw in PE_NAME_KEYWORDS:
        if kw in name_lower:
            return True

    return False


def _table_has_schedule_d_signal(table) -> bool:
    """
    Return True if any cell in the table contains a Schedule D signal keyword.
    """
    all_text = table.get_text(separator=" ", strip=True)
    for signal in SCHEDULE_D_SIGNALS:
        if signal.lower() in all_text.lower():
            return True
    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_form_990_schedule_d(html_text: str, lp_name: str) -> list[dict]:
    """
    Parse Schedule D (Other Assets / Investment schedule) from a Form 990 HTML filing.

    Looks for tables containing PE/VC fund investments and returns structured
    records for each identified holding.

    Args:
        html_text: Full HTML content of a Form 990 filing (from EDGAR or direct download)
        lp_name: Name of the LP/filer (foundation, endowment, etc.)

    Returns:
        List of dicts with keys:
            fund_name (str), gp_name (str), fair_value_usd (float), lp_name (str),
            data_source (str = "form_990")

        Returns [] if no Schedule D investment tables are found or on parse error.
    """
    if not BS4_AVAILABLE:
        logger.warning("beautifulsoup4 not available — returning empty list for Form 990 parse")
        return []

    if not html_text or not html_text.strip():
        return []

    try:
        soup = BeautifulSoup(html_text, "html.parser")
    except Exception as exc:
        logger.warning(f"[{lp_name}] BeautifulSoup parse error: {exc}")
        return []

    records: list[dict] = []
    tables = soup.find_all("table")

    if not tables:
        logger.debug(f"[{lp_name}] No <table> elements found in Form 990 HTML")
        return []

    for table in tables:
        # Only process tables that contain Schedule D signal text
        if not _table_has_schedule_d_signal(table):
            continue

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])

            if len(cells) < 2:
                continue

            if _is_header_row(cells):
                continue

            # --- Column layout heuristic ---
            # Form 990 Schedule D tables typically have:
            #   Col 0: Description / Investment Name
            #   Col 1: Investment Type / Category  (optional)
            #   Last numeric col: Book/Fair Value
            #
            # We try to be flexible: extract name from col 0, type from col 1 (if
            # it looks non-numeric), and book value from the last parseable column.

            investment_name = _cell_text(cells[0])

            # Determine investment_type column
            if len(cells) >= 3:
                col1_text = _cell_text(cells[1])
                # If col 1 is clearly a number, skip it as type
                investment_type = col1_text if _parse_number(col1_text) is None else ""
            else:
                investment_type = ""

            # Find book value: last cell that parses as a number
            book_value: Optional[float] = None
            for cell in reversed(cells):
                val = _parse_number(_cell_text(cell))
                if val is not None:
                    book_value = val
                    break

            if not investment_name:
                continue

            # Filter: only keep PE/VC-looking investments
            if not _is_pe_investment(investment_name, investment_type):
                continue

            gp_name = _infer_gp_name(investment_name)

            record: dict = {
                "fund_name": investment_name,
                "gp_name": gp_name,
                "fair_value_usd": book_value,
                "lp_name": lp_name,
                "data_source": "form_990",
            }
            records.append(record)

    logger.info(f"[{lp_name}] Form 990 Schedule D: extracted {len(records)} PE/VC investment records")
    return records
