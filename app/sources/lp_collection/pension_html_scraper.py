"""
Pension HTML Portal Scraper — PLAN_040 Path C

Scrapes pension investment portal pages (HTML, not PDF) to extract PE fund
commitment data. Complements lp_public_seed.py (Path B) and the PDF-based
pension_cafr_collector.py.

Target pages: CalPERS, Texas TRS, Oregon PERS, WSIB, NY Common.
Falls back gracefully (returns []) if a page 404s, returns no tables,
or has no recognisable PE data.
"""

import asyncio
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logger.warning("beautifulsoup4 not installed — HTML portal scraper disabled")

HTTP_TIMEOUT = 30
REQUEST_DELAY = 1.0  # seconds between requests
USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"

# ---------------------------------------------------------------------------
# Portal targets
# ---------------------------------------------------------------------------

PORTAL_TARGETS = [
    {
        "lp_name": "CalPERS",
        "url": "https://www.calpers.ca.gov/page/investments/asset-classes/private-equity",
    },
    {
        "lp_name": "Texas Teacher Retirement System",
        "url": "https://www.trs.texas.gov/Pages/investments_pe_equity.aspx",
    },
    {
        "lp_name": "Oregon Public Employees Retirement System",
        "url": "https://www.oregon.gov/treasury/financial-empowerment/Pages/Oregon-Investment-Council.aspx",
    },
    {
        "lp_name": "Washington State Investment Board",
        "url": "https://www.sib.wa.gov/financial-information/investment-reports",
    },
    {
        "lp_name": "New York State Common Retirement Fund",
        "url": "https://www.osc.ny.gov/retirement/investments",
    },
]

# Keywords that signal a PE-related HTML table
_PE_TABLE_SIGNALS = {
    "private equity", "pe fund", "buyout", "venture capital", "alternative",
    "manager", "fund name", "vintage", "commitment", "gp name", "investment manager",
}

# Keywords that identify a PE/VC investment row
_PE_ROW_SIGNALS = {
    "fund", "partners", "capital", "equity", "ventures", "growth",
    "buyout", "acquisition", "infrastructure",
}

# Stop words for GP name inference
_FUND_STOP_WORDS = {
    "fund", "funds", "partners", "capital", "lp", "llc", "inc",
    "ventures", "venture", "growth", "buyout", "acquisition",
    "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
    "xi", "xii", "xiii", "xiv", "xv",
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _cell_text(cell) -> str:
    """Extract clean text from a BeautifulSoup cell."""
    return cell.get_text(separator=" ", strip=True)


def _parse_amount(text: str) -> Optional[float]:
    """
    Parse a dollar amount string into a float (USD).
    Handles: "$1,234,567", "1,234M", "1.2B", "500K", "500,000,000"
    """
    text = text.strip().replace("$", "").replace(",", "").replace(" ", "")
    if not text:
        return None
    # Check for multiplier suffix
    m = re.match(r"^([\d.]+)([MBKmbk]?)$", text)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2).upper()
    if suffix == "B":
        num *= 1_000_000_000
    elif suffix == "M":
        num *= 1_000_000
    elif suffix == "K":
        num *= 1_000
    return num if num > 0 else None


def _parse_vintage(text: str) -> Optional[int]:
    """Extract a 4-digit year from text."""
    m = re.search(r"\b(19[89]\d|20[012]\d)\b", text)
    if m:
        return int(m.group(1))
    return None


def _infer_gp_name(fund_name: str) -> str:
    """
    Extract the managing firm name from a fund name.
    E.g. "KKR Americas Fund XII" → "KKR"
         "Blackstone Capital Partners VIII" → "Blackstone"
         "Hellman & Friedman Capital Partners X" → "Hellman & Friedman"
    """
    if not fund_name:
        return ""
    # Split on whitespace; take tokens until we hit a stop word
    tokens = fund_name.split()
    firm_tokens = []
    for tok in tokens:
        clean = tok.strip(".,;:").lower()
        if clean in _FUND_STOP_WORDS:
            break
        firm_tokens.append(tok)
    result = " ".join(firm_tokens).strip()
    return result if result else fund_name.split()[0]


def _table_has_pe_signal(table) -> bool:
    """Return True if the table text contains PE-related keywords."""
    text = table.get_text(separator=" ").lower()
    return any(sig in text for sig in _PE_TABLE_SIGNALS)


def _map_columns(headers: list[str]) -> dict:
    """
    Map column indices to semantic roles based on header text.
    Returns dict: {"manager": int, "fund": int, "vintage": int, "commitment": int}
    """
    mapping = {}
    for i, h in enumerate(headers):
        hl = h.lower()
        if any(k in hl for k in ("manager", "gp", "firm", "investment manager")):
            mapping.setdefault("manager", i)
        elif any(k in hl for k in ("fund name", "fund", "strategy")):
            mapping.setdefault("fund", i)
        elif any(k in hl for k in ("vintage", "year")):
            mapping.setdefault("vintage", i)
        elif any(k in hl for k in ("commit", "amount", "allocation", "$")):
            mapping.setdefault("commitment", i)
    return mapping


# ---------------------------------------------------------------------------
# Core parser (also used directly in tests)
# ---------------------------------------------------------------------------

def _parse_html_for_commitments_bs4(
    html: str, lp_name: str, source_url: str
) -> list[dict]:
    """
    Parse HTML for PE fund commitment tables using BeautifulSoup.
    Returns list of raw commitment dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for table in soup.find_all("table"):
        if not _table_has_pe_signal(table):
            continue

        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Extract headers from first row
        header_cells = rows[0].find_all(["th", "td"])
        headers = [_cell_text(c) for c in header_cells]
        col_map = _map_columns(headers)

        # If we couldn't map any useful columns, try positional heuristics
        use_positional = not any(k in col_map for k in ("manager", "fund"))

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            texts = [_cell_text(c) for c in cells]
            if not any(texts):
                continue

            # Skip header-like rows (all bold / all th)
            if all(c.name == "th" for c in cells):
                continue

            if use_positional:
                # Positional: col0 = manager/fund name, last numeric = commitment
                name_text = texts[0] if texts else ""
                vintage_text = " ".join(texts)
                commitment_text = ""
                for t in reversed(texts):
                    if re.search(r"\d", t):
                        commitment_text = t
                        break
            else:
                name_text = texts[col_map["manager"]] if "manager" in col_map and col_map["manager"] < len(texts) else texts[0]
                fund_text = texts[col_map["fund"]] if "fund" in col_map and col_map["fund"] < len(texts) else ""
                vintage_text = texts[col_map["vintage"]] if "vintage" in col_map and col_map["vintage"] < len(texts) else " ".join(texts)
                commitment_text = texts[col_map["commitment"]] if "commitment" in col_map and col_map["commitment"] < len(texts) else ""
                # Use fund name if manager not available
                if not name_text and fund_text:
                    name_text = fund_text

            if not name_text:
                continue

            # Determine gp_name and fund_name
            if "fund" in col_map:
                fund_name = texts[col_map["fund"]] if col_map["fund"] < len(texts) else ""
                gp_name = name_text if "manager" in col_map else _infer_gp_name(fund_name)
            else:
                fund_name = name_text
                gp_name = _infer_gp_name(name_text)

            if not gp_name:
                continue

            vintage = _parse_vintage(vintage_text)
            amount = _parse_amount(commitment_text) if commitment_text else None

            # Skip rows that don't look like PE investments
            combined = (gp_name + " " + fund_name).lower()
            if not any(sig in combined for sig in _PE_ROW_SIGNALS):
                continue

            results.append({
                "lp_name": lp_name,
                "gp_name": gp_name,
                "fund_name": fund_name,
                "fund_vintage": vintage,
                "commitment_amount_usd": amount,
                "data_source": "html_portal",
                "source_url": source_url,
                "status": "active",
            })

    return results


def _parse_html_for_commitments_regex(
    html: str, lp_name: str, source_url: str
) -> list[dict]:
    """
    Fallback regex-based HTML table parser (used when bs4 is unavailable).
    Less reliable but avoids hard dependency.
    """
    results = []
    table_re = re.compile(r"<table[^>]*>(.*?)</table>", re.DOTALL | re.IGNORECASE)
    row_re = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    cell_re = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)
    tag_re = re.compile(r"<[^>]+>")

    def clean(s: str) -> str:
        return tag_re.sub("", s).strip()

    for table_m in table_re.finditer(html):
        table_text = clean(table_m.group(1)).lower()
        if not any(sig in table_text for sig in _PE_TABLE_SIGNALS):
            continue

        rows = row_re.findall(table_m.group(1))
        if len(rows) < 2:
            continue

        header_cells = [clean(c.group(1)) for c in cell_re.finditer(rows[0])]
        col_map = _map_columns(header_cells)

        for row_html in rows[1:]:
            cells = [clean(c.group(1)) for c in cell_re.finditer(row_html)]
            if not cells:
                continue

            name_text = cells[col_map.get("manager", 0)] if col_map.get("manager", 0) < len(cells) else cells[0]
            fund_text = cells[col_map["fund"]] if "fund" in col_map and col_map["fund"] < len(cells) else ""
            vintage_text = cells[col_map["vintage"]] if "vintage" in col_map and col_map["vintage"] < len(cells) else " ".join(cells)
            commitment_text = cells[col_map["commitment"]] if "commitment" in col_map and col_map["commitment"] < len(cells) else ""

            gp_name = name_text or _infer_gp_name(fund_text)
            if not gp_name:
                continue

            fund_name = fund_text or name_text
            vintage = _parse_vintage(vintage_text)
            amount = _parse_amount(commitment_text) if commitment_text else None

            combined = (gp_name + " " + fund_name).lower()
            if not any(sig in combined for sig in _PE_ROW_SIGNALS):
                continue

            results.append({
                "lp_name": lp_name,
                "gp_name": gp_name,
                "fund_name": fund_name,
                "fund_vintage": vintage,
                "commitment_amount_usd": amount,
                "data_source": "html_portal",
                "source_url": source_url,
                "status": "active",
            })

    return results


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class PensionHtmlScraper:
    """
    Scrapes HTML investment portal pages from pension fund websites and
    extracts PE commitment data via table parsing.

    Falls back gracefully (returns []) on:
      - HTTP errors (404, 403, timeout)
      - Pages with no PE tables
      - bs4 not installed (uses regex fallback)
    """

    async def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a URL and return HTML text, or None on failure."""
        try:
            async with httpx.AsyncClient(
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            ) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.info(f"[HTML] HTTP {resp.status_code}: {url}")
                    return None
                content_type = resp.headers.get("content-type", "").lower()
                if "html" not in content_type:
                    logger.info(f"[HTML] Non-HTML content-type '{content_type}': {url}")
                    return None
                return resp.text
        except Exception as exc:
            logger.info(f"[HTML] Fetch failed {url}: {exc}")
            return None

    def _parse_html_for_commitments(
        self, html: str, lp_name: str, source_url: str
    ) -> list[dict]:
        """Parse HTML and extract PE commitment records."""
        if BS4_AVAILABLE:
            return _parse_html_for_commitments_bs4(html, lp_name, source_url)
        return _parse_html_for_commitments_regex(html, lp_name, source_url)

    async def _collect_one(self, target: dict) -> list[dict]:
        """Fetch and parse a single portal page."""
        lp_name = target["lp_name"]
        url = target["url"]

        html = await self._fetch_page(url)
        if not html:
            return []

        records = self._parse_html_for_commitments(html, lp_name, url)
        logger.info(f"[HTML] {lp_name}: {len(records)} records from {url}")
        return records

    async def collect_all(self) -> list[dict]:
        """
        Collect PE commitment records from all portal targets.
        Processes targets sequentially with a delay to be polite.
        Returns [] gracefully if all targets fail.
        """
        all_records: list[dict] = []
        for i, target in enumerate(PORTAL_TARGETS):
            if i > 0:
                await asyncio.sleep(REQUEST_DELAY)
            try:
                records = await self._collect_one(target)
                all_records.extend(records)
            except Exception as exc:
                logger.warning(f"[HTML] Unexpected error for {target['lp_name']}: {exc}")
        logger.info(f"PensionHtmlScraper.collect_all(): {len(all_records)} total records")
        return all_records
