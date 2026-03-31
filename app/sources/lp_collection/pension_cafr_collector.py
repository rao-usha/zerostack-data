"""
Pension CAFR Collector — PLAN_037 LP Conviction 2.0

Downloads CAFR PDFs from public pension websites and extracts PE portfolio data.

URL discovery order:
  1. Hardcoded URL patterns (fast; may be stale after site restructures)
  2. LLM URL discovery (asks LLM for current URL, verifies Content-Type=application/pdf)

Text extraction order:
  1. pdfplumber (handles digital/text-layer PDFs)
  2. OCR via pdf2image + pytesseract (handles scanned/image PDFs)
  3. Byte-scan fallback (last resort)
"""

import asyncio
import io
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    pdfplumber = None
    logger.warning("pdfplumber not installed — OCR-only mode")

try:
    import pytesseract
    from pdf2image import convert_from_bytes
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    logger.debug("pytesseract/pdf2image not available — OCR disabled")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PENSION_CAFR_TARGETS = [
    {
        "lp_name": "California Public Employees Retirement System",
        "short": "CalPERS",
        "url_patterns": [
            "https://www.calpers.ca.gov/docs/forms-publications/comprehensive-annual-financial-report-{year}.pdf",
        ],
    },
    {
        "lp_name": "California State Teachers Retirement System",
        "short": "CalSTRS",
        "url_patterns": [
            "https://www.calstrs.com/sites/main/files/file-attachments/cafr_{year}.pdf",
        ],
    },
    {
        "lp_name": "New York State Common Retirement Fund",
        "short": "NY Common",
        "url_patterns": [
            "https://www.osc.ny.gov/files/pdf/pension/{year}cafr.pdf",
        ],
    },
    {
        "lp_name": "Oregon Public Employees Retirement System",
        "short": "Oregon PERS",
        "url_patterns": [
            "https://www.oregon.gov/pers/Documents/PERS-CAFR-{year}.pdf",
        ],
    },
    {
        "lp_name": "Washington State Investment Board",
        "short": "WA WSIB",
        "url_patterns": [
            "https://www.sib.wa.gov/publications/annual_reports/{year}-annual-report.pdf",
        ],
    },
    {
        "lp_name": "Texas Teacher Retirement System",
        "short": "Texas TRS",
        "url_patterns": [
            "https://www.trs.texas.gov/TRS_Documents/comprehensive_annual_financial_report_{year}.pdf",
        ],
    },
    {
        "lp_name": "New Jersey Division of Pensions and Benefits",
        "short": "NJ Pension",
        "url_patterns": [
            "https://www.njtreasury.gov/doi/annualrpts/njdpb-annual-report-{year}.pdf",
        ],
    },
    {
        "lp_name": "State Teachers Retirement System of Ohio",
        "short": "Ohio STRS",
        "url_patterns": [
            "https://www.strsoh.org/assets/files/publications/cafr-{year}.pdf",
        ],
    },
    {
        "lp_name": "Pennsylvania Public School Employees Retirement System",
        "short": "PSERS",
        "url_patterns": [
            "https://www.psers.pa.gov/Publications/FinancialReports/PSERS-CAFR-{year}.pdf",
        ],
    },
    {
        "lp_name": "Illinois Teachers Retirement System",
        "short": "Illinois TRS",
        "url_patterns": [
            "https://www.trs.illinois.gov/Downloader/{year}AFRCOMBINED.pdf",
        ],
    },
]

CAFR_YEARS = [2024, 2023, 2022]
HTTP_TIMEOUT = 60
DOWNLOAD_DELAY = 3
USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"
MAX_TEXT_CHARS = 200_000
OCR_DPI = 150
OCR_MAX_PAGES = 50  # max pages to OCR per document


# ---------------------------------------------------------------------------
# PDF text extraction (shared with form_990_pe_extractor via import)
# ---------------------------------------------------------------------------

def extract_text_from_pdf(
    pdf_bytes: bytes,
    label: str = "",
    ocr_start_pct: float = 0.6,
    ocr_end_pct: float = 1.0,
) -> str:
    """
    Extract text from a PDF using up to three strategies:

    1. pdfplumber — works for digital PDFs with a text layer
    2. OCR (pdf2image + pytesseract) — works for scanned PDFs; processes the
       page range [ocr_start_pct, ocr_end_pct] of the document (max
       OCR_MAX_PAGES pages). Default covers last 40% (CAFR appendices). Pass
       ocr_start_pct=0.1, ocr_end_pct=0.7 for Form 990 (Schedule D is in
       the first half of the document).
    3. Byte-scan — last-resort fallback that grabs ASCII runs from raw bytes
    """
    # 1. pdfplumber
    if PDFPLUMBER_AVAILABLE:
        try:
            parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages[:100]:
                    page_text = page.extract_text()
                    if page_text:
                        parts.append(page_text)
            text = "\n\n".join(parts)
            if len(text) > 200:
                logger.debug(f"[{label}] pdfplumber: {len(text):,} chars")
                return text
            logger.debug(f"[{label}] pdfplumber: {len(text)} chars — falling back to OCR")
        except Exception as exc:
            logger.warning(f"[{label}] pdfplumber error: {exc}")

    # 2. OCR
    if OCR_AVAILABLE:
        result = _ocr_pdf(pdf_bytes, label, ocr_start_pct=ocr_start_pct, ocr_end_pct=ocr_end_pct)
        if result:
            return result

    # 3. Byte-scan
    return _byte_scan_pdf(pdf_bytes, label)


def _ocr_pdf(
    pdf_bytes: bytes,
    label: str = "",
    ocr_start_pct: float = 0.6,
    ocr_end_pct: float = 1.0,
) -> str:
    """OCR a slice of a PDF defined by [ocr_start_pct, ocr_end_pct] (capped at OCR_MAX_PAGES)."""
    try:
        # Determine total page count
        total_pages: Optional[int] = None
        if PDFPLUMBER_AVAILABLE:
            try:
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    total_pages = len(pdf.pages)
            except Exception:
                pass

        if total_pages:
            # pdf2image uses 1-indexed page numbers
            first_page = max(1, int(total_pages * ocr_start_pct) + 1)
            end_page = min(total_pages, int(total_pages * ocr_end_pct))
            last_page = min(end_page, first_page + OCR_MAX_PAGES - 1)
        else:
            first_page = 1
            last_page = OCR_MAX_PAGES

        logger.info(f"[{label}] OCR: pages {first_page}–{last_page} at dpi={OCR_DPI}")
        images = convert_from_bytes(
            pdf_bytes,
            dpi=OCR_DPI,
            first_page=first_page,
            last_page=last_page,
        )

        parts = [pytesseract.image_to_string(img) for img in images]
        result = "\n\n".join(p for p in parts if p.strip())
        logger.info(f"[{label}] OCR: {len(result):,} chars from {len(images)} pages")
        return result
    except Exception as exc:
        logger.warning(f"[{label}] OCR failed: {exc}")
        return ""


def _byte_scan_pdf(pdf_bytes: bytes, label: str = "") -> str:
    """Last-resort: scan raw PDF bytes for printable ASCII runs."""
    try:
        raw = pdf_bytes.decode("latin-1", errors="replace")
        runs = re.findall(r"[ -~]{4,}", raw)
        result = " ".join(runs)
        logger.debug(f"[{label}] byte-scan: {len(result):,} chars")
        return result
    except Exception as exc:
        logger.warning(f"[{label}] byte-scan failed: {exc}")
        return ""


# ---------------------------------------------------------------------------
# LLM-based PE extraction
# ---------------------------------------------------------------------------

async def _extract_pe_from_text(text: str, lp_name: str) -> list[dict]:
    """Extract PE portfolio records from CAFR text using the LLM client."""
    try:
        from app.agentic.llm_client import get_llm_client
    except ImportError:
        logger.error("app.agentic.llm_client not importable")
        return []

    llm = get_llm_client()
    if not llm:
        logger.warning(f"[{lp_name}] LLM client not available")
        return []

    pe_keywords = [
        "private equity portfolio", "investment schedule", "alternative investments",
        "venture capital", "private equity", "appendix", "fund commitments",
        "private market",
    ]
    chunk = _find_relevant_chunk(text, pe_keywords, chunk_size=20_000)
    if not chunk:
        logger.debug(f"[{lp_name}] No PE portfolio section found in text")
        return []

    prompt = """You are extracting a PE investment schedule from a pension fund annual report (CAFR).
Find any table or list showing the fund's private equity, venture capital, or alternative investments portfolio.
Look for sections titled "Private Equity Portfolio", "Investment Schedule", "Alternative Investments", or similar.

For each investment record found, extract:
- manager_name: the GP/fund manager name (required)
- fund_name: specific fund name e.g. "KKR Americas Fund XII" (if present)
- vintage_year: integer year the fund was launched (if present)
- commitment_amount_usd: LP's total commitment in USD (if present)
- called_capital_usd: capital called/drawn to date (if present)
- fair_value_usd: current fair value/NAV (if present)
- net_irr_pct: net IRR as decimal e.g. 0.152 for 15.2% (if present)

Return a JSON array. If no PE schedule is found, return [].
Extract all records found, typically 10-100 entries.

TEXT TO ANALYZE:
{text}
""".format(text=chunk)

    try:
        response = await llm.complete(
            prompt=prompt,
            system_prompt="You extract structured financial data from pension fund documents. Return valid JSON only.",
        )
        data = response.parse_json()
        if isinstance(data, list):
            valid = [r for r in data if isinstance(r, dict) and r.get("manager_name")]
            logger.info(f"[{lp_name}] extracted {len(valid)} PE records")
            return valid
        elif isinstance(data, dict):
            for key in ("investments", "records", "portfolio", "funds"):
                if key in data:
                    valid = [r for r in data[key] if isinstance(r, dict) and r.get("manager_name")]
                    logger.info(f"[{lp_name}] extracted {len(valid)} PE records (key={key})")
                    return valid
    except Exception as exc:
        logger.warning(f"[{lp_name}] LLM extraction failed: {exc}")

    return []


def _find_relevant_chunk(text: str, keywords: list[str], chunk_size: int = 15_000) -> Optional[str]:
    """Return the text chunk with the highest density of the given keywords."""
    text_lower = text.lower()
    positions: list[int] = []
    for keyword in keywords:
        for match in re.finditer(re.escape(keyword.lower()), text_lower):
            positions.append(match.start())

    if not positions:
        return None

    positions.sort()
    best_start = positions[0]
    best_count = 1
    for start_pos in positions:
        count = sum(1 for p in positions if start_pos <= p < start_pos + chunk_size)
        if count > best_count:
            best_count = count
            best_start = start_pos

    chunk_start = max(0, best_start - 500)
    return text[chunk_start: min(len(text), chunk_start + chunk_size)]


# ---------------------------------------------------------------------------
# Main collector class
# ---------------------------------------------------------------------------

class PensionCafrCollector:
    """
    Downloads CAFR PDFs from 10 major US public pension funds and extracts
    PE portfolio holdings via LLM-powered text analysis.

    URL discovery:
      Phase 1 — try hardcoded URL patterns (fast, works when patterns are valid)
      Phase 2 — ask LLM for current CAFR URL, verify Content-Type=application/pdf

    Text extraction:
      Phase 1 — pdfplumber (digital PDFs)
      Phase 2 — OCR via pdf2image + pytesseract (scanned PDFs)
      Phase 3 — byte-scan fallback
    """

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
        return self._client

    async def _is_pdf_url(self, url: str) -> bool:
        """
        HEAD request: returns True only if status is 200/206 AND
        Content-Type header contains 'pdf'.

        The Content-Type check prevents false positives from redirects
        that land on HTML search-result pages (e.g. pa.gov search).
        """
        client = self._get_client()
        try:
            resp = await client.head(url)
            if resp.status_code not in (200, 206):
                return False
            content_type = resp.headers.get("content-type", "").lower()
            return "pdf" in content_type
        except Exception:
            return False

    async def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF and return raw bytes, or None on failure."""
        client = self._get_client()
        try:
            resp = await client.get(url, headers={"Accept": "application/pdf"})
            if resp.status_code == 200:
                return resp.content
            logger.warning(f"PDF download HTTP {resp.status_code}: {url}")
            return None
        except Exception as exc:
            logger.warning(f"PDF download failed {url}: {exc}")
            return None

    async def _find_cafr_url_from_patterns(
        self, pension: dict
    ) -> Optional[tuple[str, int]]:
        """Try hardcoded URL patterns for years 2024 → 2023 → 2022."""
        short = pension["short"]
        for year in CAFR_YEARS:
            for pattern in pension["url_patterns"]:
                url = pattern.format(year=year)
                logger.debug(f"[{short}] Checking pattern: {url}")
                if await self._is_pdf_url(url):
                    logger.info(f"[{short}] Pattern match ({year}): {url}")
                    return url, year
        return None

    async def _discover_cafr_url_via_llm(self, pension: dict) -> Optional[str]:
        """
        Ask LLM for the current CAFR PDF URL, then verify it is actually a PDF.

        The LLM may know recent URL structures even when our hardcoded patterns
        are stale. Each suggested URL is verified by HEAD + Content-Type check.
        """
        try:
            from app.agentic.llm_client import get_llm_client
        except ImportError:
            return None

        llm = get_llm_client()
        if not llm:
            return None

        short = pension["short"]
        prompt = (
            f"What is the direct PDF download URL for the most recent Comprehensive "
            f"Annual Financial Report (CAFR) for {pension['lp_name']} ({short})?\n\n"
            f"Return JSON: {{\"url\": \"https://...\", \"year\": 2024}}\n"
            f"If you are not confident in the URL, set \"url\" to null."
        )

        try:
            response = await llm.complete(
                prompt=prompt,
                system_prompt=(
                    "Return a JSON object with keys 'url' (string or null) and "
                    "'year' (integer). The URL must be a direct PDF download link."
                ),
            )
            data = response.parse_json()
            if not data or not data.get("url"):
                return None

            url = str(data["url"]).strip()
            if not (url.startswith("https://") or url.startswith("http://")):
                return None

            if await self._is_pdf_url(url):
                logger.info(f"[{short}] LLM-discovered PDF URL verified: {url}")
                return url
            else:
                logger.debug(f"[{short}] LLM URL failed PDF check: {url}")
                return None
        except Exception as exc:
            logger.debug(f"[{short}] LLM URL discovery error: {exc}")
            return None

    def _map_records(
        self,
        raw_records: list[dict],
        lp_name: str,
        short: str,
        source_url: str,
    ) -> list[dict]:
        """Map LLM-extracted records to the output schema."""
        mapped = []
        for rec in raw_records:
            manager = rec.get("manager_name", "").strip()
            if not manager:
                continue
            mapped.append({
                "lp_name": short,
                "gp_name": manager,
                "fund_name": (rec.get("fund_name") or "").strip(),
                "vintage_year": rec.get("vintage_year"),
                "commitment_amount_usd": rec.get("commitment_amount_usd"),
                "data_source": "cafr",
                "source_url": source_url,
            })
        return mapped

    async def _collect_one(self, pension: dict) -> list[dict]:
        """Download and parse the CAFR for a single pension fund."""
        short = pension["short"]
        lp_name = pension["lp_name"]

        # Phase 1: hardcoded URL patterns
        url: Optional[str] = None
        result = await self._find_cafr_url_from_patterns(pension)
        if result:
            url = result[0]
        else:
            # Phase 2: LLM URL discovery
            logger.info(f"[{short}] URL patterns failed — trying LLM discovery")
            url = await self._discover_cafr_url_via_llm(pension)

        if not url:
            logger.info(f"[{short}] No CAFR PDF URL found (patterns + LLM both failed)")
            return []

        logger.info(f"[{short}] Downloading: {url}")
        pdf_bytes = await self._download_pdf(url)
        if not pdf_bytes:
            return []

        logger.info(f"[{short}] {len(pdf_bytes):,} bytes — extracting text")
        text = extract_text_from_pdf(pdf_bytes, label=short)
        if not text:
            logger.warning(f"[{short}] No text extracted (pdfplumber + OCR + byte-scan all failed)")
            return []

        if len(text) > MAX_TEXT_CHARS:
            logger.debug(f"[{short}] Truncating text {len(text):,} → {MAX_TEXT_CHARS:,}")
            text = text[:MAX_TEXT_CHARS]

        logger.info(f"[{short}] {len(text):,} chars — running LLM PE extraction")
        raw_records = await _extract_pe_from_text(text, lp_name)

        mapped = self._map_records(raw_records, lp_name, short, url)
        logger.info(f"[{short}] {len(mapped)} PE records mapped")
        return mapped

    async def collect_all(self) -> list[dict]:
        """
        Collect PE portfolio records from all target pensions.

        Processes pensions sequentially with DOWNLOAD_DELAY between requests
        to avoid hammering public servers.
        """
        all_records: list[dict] = []
        try:
            for i, pension in enumerate(PENSION_CAFR_TARGETS):
                if i > 0:
                    await asyncio.sleep(DOWNLOAD_DELAY)
                try:
                    records = await self._collect_one(pension)
                    all_records.extend(records)
                except Exception as exc:
                    logger.error(f"[{pension['short']}] Unexpected error: {exc}")
        finally:
            if self._client and not self._client.is_closed:
                await self._client.aclose()

        logger.info(f"PensionCafrCollector.collect_all(): {len(all_records)} total records")
        return all_records
