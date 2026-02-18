"""
Annual Report PDF Parsing Strategy - Extract portfolio from PDF annual reports.

Coverage: 50-70 public pensions and endowments
Confidence: HIGH (official publication)

Implementation:
- Search investor website for "Annual Report", "CAFR", "Investment Report" links
- Download most recent PDF
- Extract text using pdfplumber
- Find section with headers like "Portfolio", "Investment Holdings", "Schedule of Investments"
- Parse tables/lists in that section
- Store with source_type='annual_report', confidence_level='high'
"""

import asyncio
import hashlib
import io
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx

try:
    import pdfplumber

    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    pdfplumber = None

from bs4 import BeautifulSoup

from app.agentic.cache import get_cache, url_to_cache_key
from app.agentic.strategies.base import BaseStrategy, InvestorContext, StrategyResult

logger = logging.getLogger(__name__)


class AnnualReportStrategy(BaseStrategy):
    """
    Strategy for extracting portfolio data from PDF annual reports.

    Public pensions and endowments publish CAFRs (Comprehensive Annual
    Financial Reports) that contain detailed investment holdings.

    Confidence: HIGH - Official publications
    """

    name = "annual_report_pdf"
    display_name = "Annual Report PDF Parsing"
    source_type = "annual_report"
    default_confidence = "high"

    # Conservative rate limits
    max_requests_per_second = 0.5
    max_concurrent_requests = 1
    timeout_seconds = 300

    # PDF download limits
    MAX_PDF_SIZE_MB = 50
    MAX_PAGES_TO_SCAN = 100

    # Cache TTLs
    PDF_CACHE_TTL = 86400  # 24 hours for parsed PDF results

    # Keywords to find annual report links
    REPORT_KEYWORDS = [
        "annual report",
        "cafr",
        "comprehensive annual financial report",
        "investment report",
        "financial report",
        "fiscal year report",
        "annual investment report",
        "pension fund report",
    ]

    # Keywords to find portfolio sections in PDFs
    PORTFOLIO_SECTION_KEYWORDS = [
        "schedule of investments",
        "investment holdings",
        "portfolio holdings",
        "securities held",
        "equity holdings",
        "fixed income holdings",
        "alternative investments",
        "private equity investments",
        "investment schedule",
        "asset allocation",
    ]

    USER_AGENT = "Nexdata Research Bot (annual report research)"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = asyncio.Semaphore(1)
        self._last_request_time: Dict[str, float] = {}
        self._cache = get_cache()  # Use shared cache for PDF results

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={"User-Agent": self.USER_AGENT},
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _rate_limited_request(self, url: str) -> Optional[httpx.Response]:
        import time

        domain = urlparse(url).netloc

        async with self._rate_limiter:
            now = time.time()
            last_request = self._last_request_time.get(domain, 0)
            wait_time = (1.0 / self.max_requests_per_second) - (now - last_request)

            if wait_time > 0:
                await asyncio.sleep(wait_time)

            try:
                client = await self._get_client()
                response = await client.get(url)
                self._last_request_time[domain] = time.time()
                return response
            except Exception as e:
                logger.warning(f"Request failed for {url}: {e}")
                return None

    def is_applicable(self, context: InvestorContext) -> Tuple[bool, str]:
        """
        Check if annual report strategy is applicable.

        Most applicable for:
        - Public pensions (required to publish CAFRs)
        - Endowments (often publish annual reports)
        - Large foundations
        """
        if not PDF_AVAILABLE:
            return False, "pdfplumber not installed - PDF parsing unavailable"

        # Public pensions always publish CAFRs
        if context.lp_type == "public_pension":
            return (
                True,
                "Public pensions are required to publish annual reports (CAFRs)",
            )

        # Endowments often publish reports
        if context.lp_type == "endowment":
            return True, "Endowments typically publish annual investment reports"

        # Large foundations
        if context.lp_type == "foundation":
            return True, "Foundations often publish annual reports"

        # Need a website to find reports
        if context.website_url:
            # Check if name suggests public entity
            public_keywords = [
                "pension",
                "retirement",
                "state",
                "city",
                "county",
                "university",
            ]
            if any(kw in context.investor_name.lower() for kw in public_keywords):
                return True, "Name suggests public entity that may publish reports"

        return False, "Investor type unlikely to have public annual reports"

    def calculate_priority(self, context: InvestorContext) -> int:
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0

        # Public pensions get highest priority
        if context.lp_type == "public_pension":
            return 10

        if context.lp_type == "endowment":
            return 8

        return 6

    async def execute(self, context: InvestorContext) -> StrategyResult:
        """
        Execute annual report PDF extraction.

        Steps:
        1. Search investor website for annual report links
        2. Download most recent PDF
        3. Extract text and find portfolio section
        4. Parse holdings from that section
        """
        started_at = datetime.utcnow()
        requests_made = 0
        companies = []
        reasoning_parts = []

        if not PDF_AVAILABLE:
            return self._create_result(
                success=False,
                error_message="pdfplumber not installed",
                reasoning="PDF parsing library not available",
            )

        try:
            website_url = context.website_url
            if not website_url:
                return self._create_result(
                    success=False,
                    error_message="No website URL available",
                    reasoning="Cannot search for annual reports without website",
                )

            logger.info(f"Executing annual report strategy for {context.investor_name}")
            reasoning_parts.append(f"Searching for annual reports on {website_url}")

            # Step 1: Find annual report links on website
            report_links = await self._find_report_links(website_url)
            requests_made += 2  # Homepage + possibly investor relations page
            reasoning_parts.append(f"Found {len(report_links)} potential report links")

            if not report_links:
                return self._create_result(
                    success=False,
                    error_message="No annual report links found",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made,
                )

            # Step 2: Download and parse PDFs (try up to 2, with caching)
            for pdf_url in report_links[:2]:
                # Check cache first for this PDF
                cache_key = url_to_cache_key(pdf_url, prefix="pdf_holdings")
                cached_holdings = await self._cache.get(cache_key)

                if cached_holdings is not None:
                    reasoning_parts.append(f"Cache hit for PDF: {pdf_url[:60]}...")
                    companies.extend(cached_holdings)
                    reasoning_parts.append(
                        f"Retrieved {len(cached_holdings)} cached holdings"
                    )
                    break

                reasoning_parts.append(f"Downloading PDF: {pdf_url[:80]}...")

                pdf_content = await self._download_pdf(pdf_url)
                requests_made += 1

                if not pdf_content:
                    reasoning_parts.append("PDF download failed, trying next")
                    continue

                # Step 3: Extract text and find portfolio section
                holdings = self._extract_holdings_from_pdf(pdf_content, pdf_url)

                if holdings:
                    # Cache the parsed results
                    await self._cache.set(cache_key, holdings, ttl=self.PDF_CACHE_TTL)
                    companies.extend(holdings)
                    reasoning_parts.append(
                        f"Extracted {len(holdings)} holdings from PDF (cached)"
                    )
                    break
                else:
                    reasoning_parts.append("No holdings found in this PDF")

            if not companies:
                return self._create_result(
                    success=False,
                    error_message="No holdings extracted from annual reports",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made,
                )

            # Add source info to each company
            for company in companies:
                company["source_type"] = self.source_type
                company["confidence_level"] = self.default_confidence

            result = self._create_result(
                success=True,
                companies=companies,
                reasoning="\n".join(reasoning_parts),
                requests_made=requests_made,
            )
            result.started_at = started_at
            return result

        except Exception as e:
            logger.error(f"Error in annual report strategy: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning="\n".join(reasoning_parts) + f"\nError: {e}",
                requests_made=requests_made,
            )
        finally:
            await self.close()

    async def _find_report_links(self, website_url: str) -> List[str]:
        """Find links to annual report PDFs on the website."""
        report_links = []

        try:
            # Fetch homepage
            response = await self._rate_limited_request(website_url)
            if not response or response.status_code != 200:
                return []

            soup = BeautifulSoup(response.text, "lxml")

            # Look for links containing report keywords
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"].lower()
                text = a_tag.get_text().lower()

                # Check if it's a PDF or report link
                is_report = False
                for keyword in self.REPORT_KEYWORDS:
                    if keyword in href or keyword in text:
                        is_report = True
                        break

                if is_report or href.endswith(".pdf"):
                    full_url = urljoin(website_url, a_tag["href"])
                    if full_url not in report_links:
                        report_links.append(full_url)

            # Also try common investor relations paths
            ir_paths = [
                "/investors",
                "/investor-relations",
                "/financials",
                "/reports",
                "/about/financials",
            ]
            for path in ir_paths:
                ir_url = urljoin(website_url, path)
                ir_response = await self._rate_limited_request(ir_url)
                if ir_response and ir_response.status_code == 200:
                    ir_soup = BeautifulSoup(ir_response.text, "lxml")
                    for a_tag in ir_soup.find_all("a", href=True):
                        href = a_tag["href"].lower()
                        if href.endswith(".pdf") or any(
                            kw in href for kw in ["annual", "report", "cafr"]
                        ):
                            full_url = urljoin(ir_url, a_tag["href"])
                            if full_url not in report_links:
                                report_links.append(full_url)
                    break  # Only try one IR page

            # Prioritize PDFs and recent years
            def sort_key(url):
                score = 0
                url_lower = url.lower()
                if ".pdf" in url_lower:
                    score += 10
                if "cafr" in url_lower:
                    score += 5
                if "annual" in url_lower:
                    score += 3
                # Recent years get higher priority
                for year in ["2024", "2023", "2025"]:
                    if year in url_lower:
                        score += 5
                        break
                return -score

            report_links.sort(key=sort_key)
            return report_links[:5]

        except Exception as e:
            logger.warning(f"Error finding report links: {e}")
            return []

    async def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF file with size limit."""
        try:
            response = await self._rate_limited_request(pdf_url)
            if not response or response.status_code != 200:
                return None

            # Check content type
            content_type = response.headers.get("content-type", "")
            if "pdf" not in content_type.lower() and not pdf_url.lower().endswith(
                ".pdf"
            ):
                return None

            # Check size
            content_length = response.headers.get("content-length")
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > self.MAX_PDF_SIZE_MB:
                    logger.warning(f"PDF too large ({size_mb:.1f}MB): {pdf_url}")
                    return None

            return response.content

        except Exception as e:
            logger.warning(f"Error downloading PDF: {e}")
            return None

    def _extract_holdings_from_pdf(
        self, pdf_content: bytes, source_url: str
    ) -> List[Dict[str, Any]]:
        """Extract holdings from PDF content."""
        holdings = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                # Limit pages to scan
                pages_to_scan = min(len(pdf.pages), self.MAX_PAGES_TO_SCAN)

                # Find portfolio section
                portfolio_text = ""
                in_portfolio_section = False

                for i, page in enumerate(pdf.pages[:pages_to_scan]):
                    text = page.extract_text() or ""
                    text_lower = text.lower()

                    # Check if this page has portfolio section
                    for keyword in self.PORTFOLIO_SECTION_KEYWORDS:
                        if keyword in text_lower:
                            in_portfolio_section = True
                            break

                    if in_portfolio_section:
                        portfolio_text += text + "\n"

                        # Extract tables from this page
                        tables = page.extract_tables()
                        for table in tables:
                            table_holdings = self._parse_holdings_table(
                                table, source_url
                            )
                            holdings.extend(table_holdings)

                    # Stop after collecting enough or leaving portfolio section
                    if len(holdings) > 100:
                        break

                # If no tables found, try to extract from text
                if not holdings and portfolio_text:
                    text_holdings = self._extract_holdings_from_text(
                        portfolio_text, source_url
                    )
                    holdings.extend(text_holdings)

        except Exception as e:
            logger.error(f"Error extracting from PDF: {e}")

        return holdings

    def _parse_holdings_table(
        self, table: List[List], source_url: str
    ) -> List[Dict[str, Any]]:
        """Parse a table from PDF to extract holdings."""
        holdings = []

        if not table or len(table) < 2:
            return holdings

        # Try to identify column headers
        header_row = table[0] if table[0] else []
        header_lower = [str(h).lower() if h else "" for h in header_row]

        # Find relevant columns
        name_col = None
        value_col = None
        shares_col = None

        for i, h in enumerate(header_lower):
            if any(
                kw in h
                for kw in ["security", "issuer", "company", "name", "description"]
            ):
                name_col = i
            elif any(kw in h for kw in ["market value", "fair value", "value"]):
                value_col = i
            elif any(kw in h for kw in ["shares", "units", "quantity"]):
                shares_col = i

        # If no headers found, assume first column is name
        if name_col is None:
            name_col = 0

        # Parse data rows
        for row in table[1:]:
            if not row or len(row) <= name_col:
                continue

            company_name = str(row[name_col]).strip() if row[name_col] else ""

            # Skip if not a valid company name
            if not company_name or len(company_name) < 3:
                continue
            if company_name.lower() in ["total", "subtotal", "n/a", "-", ""]:
                continue

            holding = {
                "company_name": company_name,
                "investment_type": "public_equity",
                "source_url": source_url,
                "current_holding": 1,
            }

            # Add value if found
            if value_col is not None and len(row) > value_col and row[value_col]:
                try:
                    value_str = str(row[value_col]).replace(",", "").replace("$", "")
                    value = float(re.sub(r"[^\d.]", "", value_str))
                    holding["market_value_usd"] = str(int(value))
                except (ValueError, TypeError):
                    pass

            # Add shares if found
            if shares_col is not None and len(row) > shares_col and row[shares_col]:
                try:
                    shares_str = str(row[shares_col]).replace(",", "")
                    shares = int(float(re.sub(r"[^\d.]", "", shares_str)))
                    holding["shares_held"] = str(shares)
                except (ValueError, TypeError):
                    pass

            holdings.append(holding)

        return holdings

    def _extract_holdings_from_text(
        self, text: str, source_url: str
    ) -> List[Dict[str, Any]]:
        """Extract holdings from unstructured text."""
        holdings = []

        # Pattern to match company names (capitalized words)
        # This is a simple heuristic - real implementation would use NLP
        lines = text.split("\n")

        for line in lines:
            line = line.strip()

            # Skip short lines and common headers
            if len(line) < 5 or len(line) > 100:
                continue

            # Look for lines that look like company names
            # (start with capital, contain multiple words)
            if re.match(r"^[A-Z][A-Za-z\s\.\,\&\-]+$", line):
                # Check it's not a section header
                skip_patterns = [
                    "total",
                    "schedule",
                    "investment",
                    "holdings",
                    "portfolio",
                ]
                if any(p in line.lower() for p in skip_patterns):
                    continue

                holdings.append(
                    {
                        "company_name": line,
                        "investment_type": "unknown",
                        "source_url": source_url,
                        "current_holding": 1,
                    }
                )

            if len(holdings) >= 50:
                break

        return holdings
