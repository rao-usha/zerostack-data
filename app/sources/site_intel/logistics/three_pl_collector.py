"""
3PL Company Collector.

Fetches 3PL (Third-Party Logistics) company data from:
- Transport Topics Top 100 Logistics Companies
- Armstrong & Associates rankings (when available)
- SEC filings for public companies

Data sources:
- Transport Topics website (scraping with Playwright)
- SEC EDGAR API
- Company websites

No API key required for most public data.
"""

import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.core.models_site_intel import ThreePLCompany
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

# Optional Playwright import - graceful fallback if not installed
try:
    from playwright.async_api import async_playwright, Browser, BrowserContext

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.info(
        "Playwright not installed - JS rendering disabled. Install with: pip install playwright && playwright install chromium"
    )


# Known 3PL services mapping
SERVICE_KEYWORDS = {
    "freight brokerage": "freight_brokerage",
    "freight forwarding": "freight_forwarding",
    "warehousing": "warehousing",
    "distribution": "distribution",
    "fulfillment": "fulfillment",
    "dedicated": "dedicated_transportation",
    "contract logistics": "contract_logistics",
    "3pl": "third_party_logistics",
    "last mile": "last_mile",
    "intermodal": "intermodal",
    "ltl": "ltl",
    "truckload": "truckload",
    "cold chain": "cold_chain",
    "temperature controlled": "cold_chain",
    "managed transportation": "managed_transportation",
    "supply chain": "supply_chain_management",
}


@register_collector(SiteIntelSource.TRANSPORT_TOPICS)
class ThreePLCollector(BaseCollector):
    """
    Collector for 3PL company directory data.

    Fetches:
    - Company profiles and rankings from Transport Topics Top 100
    - Revenue and employee counts
    - Services offered and geographic coverage
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.TRANSPORT_TOPICS

    # Transport Topics configuration
    default_timeout = 120.0  # Longer timeout for JS rendering
    rate_limit_delay = 2.0  # Be respectful to the website

    # URLs
    RANKINGS_URL = "https://www.ttnews.com/logistics/rankings"
    BASE_URL = "https://www.ttnews.com"

    # Playwright settings
    PLAYWRIGHT_TIMEOUT = 30000  # 30 seconds for JS rendering

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        self._playwright = None
        self._browser: Optional["Browser"] = None
        self._browser_context: Optional["BrowserContext"] = None

    def get_default_base_url(self) -> str:
        return self.BASE_URL

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

    async def _init_playwright(self) -> bool:
        """Initialize Playwright browser for JS rendering."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not available - cannot scrape JS-rendered pages")
            return False

        if self._browser is not None:
            return True

        try:
            headed = os.getenv("BROWSER_HEADED", "0") == "1"
            launch_kwargs = {
                "headless": not headed,
                "args": [
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                ],
            }
            if headed:
                launch_kwargs["slow_mo"] = 250
                logger.info("Headed browser mode enabled — launching visible browser window")

            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(**launch_kwargs)
            self._browser_context = await self._browser.new_context(
                user_agent=self.get_default_headers()["User-Agent"],
                viewport={"width": 1280, "height": 720},
            )
            logger.info("Playwright browser initialized for Transport Topics scraping")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Playwright: {e}")
            if headed:
                logger.warning("Headed mode failed (no display?), falling back to headless")
            return False

    async def _close_playwright(self):
        """Clean up Playwright resources."""
        if self._browser_context:
            try:
                await self._browser_context.close()
            except Exception:
                pass
            self._browser_context = None

        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None

        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute 3PL company data collection.

        Collects company profiles from Transport Topics Top 100 Logistics.
        """
        try:
            logger.info("Collecting 3PL company data from Transport Topics...")

            # Collect from Transport Topics
            companies_result = await self._collect_companies(config)
            all_companies = companies_result.get("records", [])

            if not all_companies:
                logger.warning("No 3PL company data retrieved from Transport Topics")
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=0,
                    processed=0,
                    inserted=0,
                    error_message=companies_result.get("error"),
                )

            # Transform and dedupe records
            records = []
            seen_names = set()
            for company in all_companies:
                transformed = self._transform_company(company)
                if transformed:
                    name = transformed.get("company_name")
                    if name and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        records.append(transformed)

            logger.info(f"Transformed {len(records)} 3PL company records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    ThreePLCompany,
                    records,
                    unique_columns=["company_name"],
                    update_columns=[
                        "parent_company",
                        "headquarters_city",
                        "headquarters_state",
                        "headquarters_country",
                        "website",
                        "annual_revenue_million",
                        "revenue_year",
                        "employee_count",
                        "facility_count",
                        "services",
                        "industries_served",
                        "regions_served",
                        "states_coverage",
                        "countries_coverage",
                        "armstrong_rank",
                        "transport_topics_rank",
                        "has_cold_chain",
                        "has_hazmat",
                        "has_ecommerce_fulfillment",
                        "has_cross_dock",
                        "is_asset_based",
                        "is_non_asset",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_companies),
                    processed=len(all_companies),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_companies),
                processed=len(all_companies),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"3PL company collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )
        finally:
            await self._close_playwright()

    async def _collect_companies(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect 3PL company data from Transport Topics Top 100 Logistics.

        Uses Playwright for JS rendering since the page is dynamically loaded.
        """
        all_records = []

        # Try Playwright first for JS rendering
        if PLAYWRIGHT_AVAILABLE:
            try:
                playwright_records = await self._scrape_with_playwright()
                if playwright_records:
                    all_records.extend(playwright_records)
                    logger.info(
                        f"Scraped {len(playwright_records)} companies with Playwright"
                    )
            except Exception as e:
                logger.warning(f"Playwright scraping failed: {e}")

        # Fallback to httpx if Playwright fails or is unavailable
        if not all_records:
            try:
                httpx_records = await self._scrape_with_httpx()
                if httpx_records:
                    all_records.extend(httpx_records)
                    logger.info(f"Scraped {len(httpx_records)} companies with httpx")
            except Exception as e:
                logger.warning(f"httpx scraping failed: {e}")

        if not all_records:
            return {
                "records": [],
                "error": "Failed to scrape Transport Topics - page may require JS or structure changed",
            }

        return {"records": all_records}

    async def _scrape_with_playwright(self) -> List[Dict[str, Any]]:
        """Scrape Transport Topics using Playwright for JS rendering."""
        if not await self._init_playwright():
            return []

        records = []

        try:
            page = await self._browser_context.new_page()

            # Navigate to rankings page
            logger.info(f"Navigating to {self.RANKINGS_URL}")
            await page.goto(
                self.RANKINGS_URL,
                wait_until="networkidle",
                timeout=self.PLAYWRIGHT_TIMEOUT,
            )

            # Wait for the table/data to load
            await asyncio.sleep(3)  # Give extra time for dynamic content

            # Get page content
            content = await page.content()

            # Parse the HTML
            records = self._parse_rankings_page(content)

            await page.close()

        except Exception as e:
            logger.error(f"Playwright scraping error: {e}")
            raise

        return records

    async def _scrape_with_httpx(self) -> List[Dict[str, Any]]:
        """Fallback scraping with httpx (may not work if page requires JS)."""
        try:
            client = await self.get_client()
            await self.apply_rate_limit()

            response = await client.get(self.RANKINGS_URL)

            if response.status_code != 200:
                logger.warning(f"Transport Topics returned {response.status_code}")
                return []

            return self._parse_rankings_page(response.text)

        except Exception as e:
            logger.error(f"httpx scraping error: {e}")
            return []

    def _parse_rankings_page(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Parse the Transport Topics rankings page HTML.

        Extracts company data from the rankings table/list.
        """
        records = []
        soup = BeautifulSoup(html_content, "html.parser")

        # Try to find ranking table or list
        # Transport Topics uses various structures - try multiple selectors

        # Look for table rows with company data
        table_rows = soup.select(
            "table tr, .ranking-row, .company-row, [class*='rank']"
        )

        if not table_rows:
            # Try finding any structured data container
            table_rows = soup.find_all("tr")

        logger.info(f"Found {len(table_rows)} potential company rows")

        # Track current year for revenue
        current_year = datetime.now().year

        for row in table_rows:
            try:
                company_data = self._extract_company_from_row(row, current_year)
                if company_data and company_data.get("company_name"):
                    records.append(company_data)
            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")
                continue

        # If table parsing didn't work, try structured data (JSON-LD)
        if not records:
            records = self._extract_from_structured_data(soup, current_year)

        # If still no records, try text-based extraction
        if not records:
            records = self._extract_from_text_content(soup, current_year)

        return records

    def _extract_company_from_row(
        self, row, current_year: int
    ) -> Optional[Dict[str, Any]]:
        """Extract company data from a table row element."""
        cells = row.find_all(["td", "th", "div", "span"])

        if len(cells) < 2:
            return None

        # Try to identify data by position or class
        company_name = None
        rank = None
        gross_revenue = None
        net_revenue = None
        employees = None

        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)

            # Skip empty or header cells
            if not text or text.lower() in [
                "rank",
                "company",
                "revenue",
                "employees",
                "gross",
                "net",
            ]:
                continue

            # Check for rank (usually first column, numeric)
            if rank is None and text.isdigit() and int(text) <= 100:
                rank = int(text)
                continue

            # Check for company name (usually after rank, contains letters)
            if (
                company_name is None
                and re.search(r"[A-Za-z]{3,}", text)
                and not text.startswith("$")
            ):
                # Clean up company name
                company_name = re.sub(r"\s+", " ", text).strip()
                # Remove rank if embedded
                company_name = re.sub(r"^\d+\s*", "", company_name)
                continue

            # Check for revenue (contains $ or large numbers)
            if "$" in text or (
                text.replace(",", "").replace(".", "").isdigit() and len(text) > 3
            ):
                value = self._parse_revenue(text)
                if value:
                    if gross_revenue is None:
                        gross_revenue = value
                    elif net_revenue is None:
                        net_revenue = value
                continue

            # Check for employees (4-6 digit number)
            if text.replace(",", "").isdigit():
                num = int(text.replace(",", ""))
                if 100 <= num <= 500000:  # Reasonable employee range
                    employees = num

        if not company_name:
            return None

        return {
            "company_name": company_name,
            "transport_topics_rank": rank,
            "annual_revenue_million": gross_revenue,
            "net_revenue_million": net_revenue,
            "employee_count": employees,
            "revenue_year": current_year,
        }

    def _extract_from_structured_data(
        self, soup: BeautifulSoup, current_year: int
    ) -> List[Dict[str, Any]]:
        """Try to extract company data from JSON-LD or other structured data."""
        records = []

        # Look for JSON-LD scripts
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                import json

                data = json.loads(script.string)
                # Process structured data if it contains organization info
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") in ["Organization", "Corporation"]:
                            records.append(
                                {
                                    "company_name": item.get("name"),
                                    "website": item.get("url"),
                                    "revenue_year": current_year,
                                }
                            )
            except Exception:
                continue

        return records

    def _extract_from_text_content(
        self, soup: BeautifulSoup, current_year: int
    ) -> List[Dict[str, Any]]:
        """
        Extract company data from page text using pattern matching.

        This is a fallback when structured parsing fails.
        """
        records = []

        # Get all text content
        text = soup.get_text()

        # Pattern: "1. Company Name $X,XXX" or "1 Company Name $X,XXX"
        pattern = r"(\d{1,3})[\.\s]+([A-Z][A-Za-z\.\s&\-\']+(?:Inc|Corp|LLC|Ltd|Co|Group|Logistics|Services)?)\s*\$?([\d,]+)"

        matches = re.findall(pattern, text)

        for match in matches:
            rank, company_name, revenue = match
            rank = int(rank)

            if rank > 100:
                continue

            # Clean company name
            company_name = company_name.strip()
            company_name = re.sub(r"\s+", " ", company_name)

            # Parse revenue (assume millions)
            revenue_value = self._parse_revenue(revenue)

            if company_name and len(company_name) > 3:
                records.append(
                    {
                        "company_name": company_name,
                        "transport_topics_rank": rank,
                        "annual_revenue_million": revenue_value,
                        "revenue_year": current_year,
                    }
                )

        return records

    def _parse_revenue(self, text: str) -> Optional[float]:
        """Parse revenue value from text like '$16,848' or '16848'."""
        if not text:
            return None

        # Remove $ and commas
        cleaned = text.replace("$", "").replace(",", "").strip()

        # Handle 'est.' or other annotations
        cleaned = re.sub(r"[a-zA-Z\.\s]+", "", cleaned)

        try:
            value = float(cleaned)
            # If value seems too small, it might be in billions
            if value < 100:
                value *= 1000  # Convert to millions
            return value
        except (ValueError, TypeError):
            return None

    def _transform_company(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw company data to database format."""
        company_name = company.get("company_name")
        if not company_name:
            return None

        # Clean company name
        company_name = company_name.strip()
        # Remove common suffixes for deduplication
        company_name = re.sub(r"\s*\(.*?\)\s*$", "", company_name)
        # Truncate to max field length
        company_name = company_name[:255]

        # Filter out garbage names (navigation, headers, etc.)
        garbage_indicators = [
            "top menu",
            "subscribe",
            "log in",
            "log out",
            "sort:",
            "publish date",
            "relevance",
            "apply",
            "download",
            "share",
            "sponsored by",
            "up front",
            "rank this year",
            "rank last year",
            "gross revenue",
            "net revenue",
            "employees",
            "million",
            "advertising",
            "classifieds",
        ]
        name_lower = company_name.lower()
        if any(indicator in name_lower for indicator in garbage_indicators):
            return None

        # Filter names that are too short or too long (likely garbage)
        if len(company_name) < 3 or len(company_name) > 100:
            return None

        # Filter names with too many special characters
        special_char_count = sum(1 for c in company_name if c in "|[]{}()<>")
        if special_char_count > 2:
            return None

        # Infer services from company name
        services = []
        name_lower = company_name.lower()
        for keyword, service in SERVICE_KEYWORDS.items():
            if keyword in name_lower:
                services.append(service)

        # Default services for logistics companies
        if not services:
            services = ["third_party_logistics"]

        # Detect cold chain from name
        has_cold_chain = any(
            kw in name_lower
            for kw in ["cold", "refrigerat", "lineage", "americold", "temperature"]
        )

        # Detect asset-based vs non-asset
        is_asset_based = any(
            kw in name_lower
            for kw in ["ryder", "schneider", "fedex", "ups", "dhl", "gxo", "lineage"]
        )
        is_non_asset = any(
            kw in name_lower
            for kw in [
                "robinson",
                "expeditors",
                "tql",
                "echo",
                "coyote",
                "uber freight",
            ]
        )

        return {
            "company_name": company_name,
            "parent_company": company.get("parent_company"),
            "headquarters_city": company.get("headquarters_city"),
            "headquarters_state": company.get("headquarters_state"),
            "headquarters_country": company.get("headquarters_country", "USA"),
            "website": company.get("website"),
            "annual_revenue_million": self._safe_float(
                company.get("annual_revenue_million")
            ),
            "revenue_year": self._safe_int(company.get("revenue_year")),
            "employee_count": self._safe_int(company.get("employee_count")),
            "facility_count": self._safe_int(company.get("facility_count")),
            "services": services if services else None,
            "industries_served": company.get("industries_served"),
            "regions_served": company.get("regions_served", ["North America"]),
            "states_coverage": company.get("states_coverage"),
            "countries_coverage": company.get("countries_coverage"),
            "armstrong_rank": self._safe_int(company.get("armstrong_rank")),
            "transport_topics_rank": self._safe_int(
                company.get("transport_topics_rank")
            ),
            "has_cold_chain": has_cold_chain or company.get("has_cold_chain"),
            "has_hazmat": company.get("has_hazmat"),
            "has_ecommerce_fulfillment": company.get("has_ecommerce_fulfillment"),
            "has_cross_dock": company.get("has_cross_dock"),
            "is_asset_based": is_asset_based or company.get("is_asset_based"),
            "is_non_asset": is_non_asset or company.get("is_non_asset"),
            "source": "transport_topics",
            "collected_at": datetime.utcnow(),
        }

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
