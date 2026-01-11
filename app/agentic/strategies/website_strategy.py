"""
Website Portfolio Scraping Strategy - Find and scrape official portfolio pages.

Coverage: 60-80 investors (those with public portfolio pages)
Confidence: MEDIUM-HIGH

Implementation:
- Fetch investor homepage from website_url
- Search for links containing keywords: "portfolio", "investments", "companies", "holdings"
- Scrape up to 3 portfolio pages (bounded)
- Extract company names, industries, dates using pattern matching
- Store with source_type='website', confidence_level='medium'

Safeguards:
- Rate limiting: 1 request per 2 seconds per domain
- Respect robots.txt
- Max 5 pages per investor (prevent runaway)
- Timeout: 10 seconds per request
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.agentic.strategies.base import BaseStrategy, InvestorContext, StrategyResult

logger = logging.getLogger(__name__)


class WebsiteStrategy(BaseStrategy):
    """
    Strategy for scraping portfolio information from investor websites.
    
    Looks for public portfolio pages and extracts company names.
    Respects robots.txt and rate limits.
    
    Confidence: MEDIUM - Website data can be outdated or incomplete
    """
    
    name = "website_scraping"
    display_name = "Website Portfolio Scraping"
    source_type = "website"
    default_confidence = "medium"
    
    # Conservative rate limits for website scraping
    max_requests_per_second = 0.5  # 1 request per 2 seconds
    max_concurrent_requests = 1
    timeout_seconds = 300
    
    # Scraping limits
    MAX_PAGES_PER_INVESTOR = 5
    REQUEST_TIMEOUT = 10
    
    # Portfolio page keywords
    PORTFOLIO_KEYWORDS = [
        "portfolio", "investments", "companies", "holdings",
        "our-portfolio", "portfolio-companies", "investment-portfolio",
        "backed-companies", "our-investments", "current-investments"
    ]
    
    # User-Agent for respectful scraping
    USER_AGENT = "Nexdata Research Bot (portfolio research, respects robots.txt)"
    
    def __init__(
        self,
        max_requests_per_second: Optional[float] = None,
        max_concurrent_requests: Optional[int] = None,
        timeout_seconds: Optional[int] = None
    ):
        super().__init__(max_requests_per_second, max_concurrent_requests, timeout_seconds)
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = asyncio.Semaphore(1)
        self._last_request_time: Dict[str, float] = {}  # Per-domain rate limiting
        self._robots_cache: Dict[str, bool] = {}  # Cache robots.txt results
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.REQUEST_TIMEOUT, connect=5.0),
                headers={
                    "User-Agent": self.USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                },
                follow_redirects=True
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limited_request(self, url: str) -> Optional[httpx.Response]:
        """Make a rate-limited request with per-domain tracking."""
        import time
        
        domain = urlparse(url).netloc
        
        async with self._rate_limiter:
            # Enforce per-domain rate limit
            now = time.time()
            last_request = self._last_request_time.get(domain, 0)
            elapsed = now - last_request
            wait_time = (1.0 / self.max_requests_per_second) - elapsed
            
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
    
    async def _check_robots_txt(self, url: str) -> bool:
        """Check if scraping is allowed by robots.txt."""
        domain = urlparse(url).netloc
        
        # Check cache
        if domain in self._robots_cache:
            return self._robots_cache[domain]
        
        try:
            robots_url = f"https://{domain}/robots.txt"
            response = await self._rate_limited_request(robots_url)
            
            if response is None or response.status_code != 200:
                # No robots.txt or error - assume allowed
                self._robots_cache[domain] = True
                return True
            
            # Simple robots.txt parsing
            content = response.text.lower()
            
            # Check for disallow all
            if "disallow: /" in content and "user-agent: *" in content:
                # Check if there's a specific allow for our paths
                if "allow:" in content:
                    self._robots_cache[domain] = True
                    return True
                self._robots_cache[domain] = False
                return False
            
            self._robots_cache[domain] = True
            return True
        
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {domain}: {e}")
            # Assume allowed on error
            self._robots_cache[domain] = True
            return True
    
    def is_applicable(self, context: InvestorContext) -> Tuple[bool, str]:
        """
        Check if website scraping strategy is applicable.
        
        Requires:
        - A valid website URL
        """
        if context.website_url:
            # Validate URL format
            try:
                parsed = urlparse(context.website_url)
                if parsed.scheme in ('http', 'https') and parsed.netloc:
                    return True, f"Website URL available: {context.website_url}"
            except Exception:
                pass
        
        return False, "No valid website URL available"
    
    def calculate_priority(self, context: InvestorContext) -> int:
        """
        Calculate priority for website strategy.
        
        Higher priority for:
        - Investors with known portfolio pages
        - VCs and PE firms (more likely to have portfolio pages)
        """
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0
        
        # Base priority
        priority = 6
        
        # Family offices are less likely to have portfolio pages
        if context.investor_type == "family_office":
            priority = 5
        
        # Public pensions often have detailed portfolio pages
        if context.lp_type == "public_pension":
            priority = 8
        
        return priority
    
    async def execute(self, context: InvestorContext) -> StrategyResult:
        """
        Execute website scraping strategy.
        
        Steps:
        1. Fetch the homepage
        2. Find portfolio-related links
        3. Scrape portfolio pages for company names
        4. Extract and normalize company data
        """
        started_at = datetime.utcnow()
        requests_made = 0
        companies = []
        reasoning_parts = []
        
        try:
            website_url = context.website_url
            if not website_url:
                return self._create_result(
                    success=False,
                    error_message="No website URL provided",
                    reasoning="Cannot execute website strategy without a URL"
                )
            
            logger.info(f"Executing website strategy for {context.investor_name}: {website_url}")
            reasoning_parts.append(f"Starting website scrape for '{website_url}'")
            
            # Step 1: Check robots.txt
            allowed = await self._check_robots_txt(website_url)
            requests_made += 1
            
            if not allowed:
                reasoning_parts.append("robots.txt disallows scraping")
                return self._create_result(
                    success=False,
                    error_message="Scraping disallowed by robots.txt",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            reasoning_parts.append("robots.txt allows scraping")
            
            # Step 2: Fetch homepage
            homepage_response = await self._rate_limited_request(website_url)
            requests_made += 1
            
            if homepage_response is None or homepage_response.status_code != 200:
                status = homepage_response.status_code if homepage_response else "timeout"
                reasoning_parts.append(f"Homepage fetch failed: {status}")
                return self._create_result(
                    success=False,
                    error_message=f"Failed to fetch homepage: {status}",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            reasoning_parts.append("Homepage fetched successfully")
            
            # Step 3: Find portfolio links
            portfolio_links = self._find_portfolio_links(homepage_response.text, website_url)
            reasoning_parts.append(f"Found {len(portfolio_links)} potential portfolio links")
            
            if not portfolio_links:
                # No portfolio links found on homepage - still try to extract from homepage
                homepage_companies = self._extract_companies_from_page(homepage_response.text, website_url)
                if homepage_companies:
                    companies.extend(homepage_companies)
                    reasoning_parts.append(f"Extracted {len(homepage_companies)} companies from homepage")
            
            # Step 4: Scrape portfolio pages (up to MAX_PAGES)
            pages_scraped = 0
            for link in portfolio_links[:self.MAX_PAGES_PER_INVESTOR]:
                page_response = await self._rate_limited_request(link)
                requests_made += 1
                pages_scraped += 1
                
                if page_response and page_response.status_code == 200:
                    page_companies = self._extract_companies_from_page(page_response.text, link)
                    companies.extend(page_companies)
                    reasoning_parts.append(f"Extracted {len(page_companies)} companies from {link}")
            
            # Deduplicate companies
            unique_companies = self._deduplicate_companies(companies)
            reasoning_parts.append(f"Total unique companies found: {len(unique_companies)}")
            
            # Add investor context to each company
            for company in unique_companies:
                company["source_type"] = self.source_type
                company["confidence_level"] = self.default_confidence
                company["source_url"] = website_url
            
            result = self._create_result(
                success=len(unique_companies) > 0,
                companies=unique_companies,
                reasoning="\n".join(reasoning_parts),
                requests_made=requests_made
            )
            result.started_at = started_at
            return result
        
        except Exception as e:
            logger.error(f"Error executing website strategy: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning="\n".join(reasoning_parts) + f"\nError: {str(e)}",
                requests_made=requests_made
            )
        
        finally:
            await self.close()
    
    def _find_portfolio_links(self, html_content: str, base_url: str) -> List[str]:
        """Find links that likely lead to portfolio pages."""
        links = []
        seen_urls: Set[str] = set()
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                text = a_tag.get_text().lower().strip()
                
                # Check if link text or URL contains portfolio keywords
                is_portfolio_link = False
                
                for keyword in self.PORTFOLIO_KEYWORDS:
                    if keyword in href.lower() or keyword in text:
                        is_portfolio_link = True
                        break
                
                if is_portfolio_link:
                    # Resolve relative URLs
                    full_url = urljoin(base_url, href)
                    
                    # Only include same-domain URLs
                    if urlparse(full_url).netloc == urlparse(base_url).netloc:
                        if full_url not in seen_urls:
                            seen_urls.add(full_url)
                            links.append(full_url)
            
            return links
        
        except Exception as e:
            logger.warning(f"Error finding portfolio links: {e}")
            return []
    
    def _extract_companies_from_page(self, html_content: str, source_url: str) -> List[Dict[str, Any]]:
        """Extract company names from a portfolio page."""
        companies = []
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Strategy 1: Look for structured portfolio elements
            # Many sites use cards, grids, or lists for portfolio companies
            
            # Common patterns for portfolio items
            portfolio_selectors = [
                '.portfolio-item', '.portfolio-company', '.investment',
                '.company-card', '.portfolio-card', '.holding',
                '[class*="portfolio"]', '[class*="investment"]', '[class*="company"]'
            ]
            
            for selector in portfolio_selectors:
                items = soup.select(selector)
                for item in items:
                    company_name = self._extract_company_name_from_element(item)
                    if company_name:
                        company = {
                            "company_name": company_name,
                            "company_industry": self._extract_industry_from_element(item),
                            "company_website": self._extract_website_from_element(item),
                            "investment_type": "unknown",
                            "current_holding": 1,
                        }
                        companies.append(company)
            
            # Strategy 2: Look for headings followed by text
            # Many portfolio pages list company names as h3/h4 elements
            if len(companies) < 5:
                for heading in soup.find_all(['h2', 'h3', 'h4', 'h5']):
                    text = heading.get_text().strip()
                    if self._looks_like_company_name(text):
                        company = {
                            "company_name": text,
                            "investment_type": "unknown",
                            "current_holding": 1,
                        }
                        # Avoid duplicates
                        if not any(c["company_name"].lower() == text.lower() for c in companies):
                            companies.append(company)
            
            # Strategy 3: Look for image alt text (logos often have company names)
            if len(companies) < 5:
                for img in soup.find_all('img', alt=True):
                    alt_text = img.get('alt', '').strip()
                    if self._looks_like_company_name(alt_text):
                        # Check parent for more context
                        parent = img.parent
                        if parent and 'portfolio' in str(parent.get('class', [])).lower():
                            company = {
                                "company_name": alt_text,
                                "investment_type": "unknown",
                                "current_holding": 1,
                            }
                            if not any(c["company_name"].lower() == alt_text.lower() for c in companies):
                                companies.append(company)
            
            return companies
        
        except Exception as e:
            logger.warning(f"Error extracting companies from page: {e}")
            return []
    
    def _extract_company_name_from_element(self, element) -> Optional[str]:
        """Extract company name from a portfolio element."""
        # Try various common patterns
        
        # Look for heading
        heading = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        if heading:
            text = heading.get_text().strip()
            if self._looks_like_company_name(text):
                return text
        
        # Look for title class
        title_elem = element.find(class_=re.compile(r'title|name|company', re.I))
        if title_elem:
            text = title_elem.get_text().strip()
            if self._looks_like_company_name(text):
                return text
        
        # Look for link text
        link = element.find('a')
        if link:
            text = link.get_text().strip()
            if self._looks_like_company_name(text):
                return text
        
        return None
    
    def _extract_industry_from_element(self, element) -> Optional[str]:
        """Extract industry/sector from a portfolio element."""
        industry_elem = element.find(class_=re.compile(r'sector|industry|category', re.I))
        if industry_elem:
            return industry_elem.get_text().strip()
        return None
    
    def _extract_website_from_element(self, element) -> Optional[str]:
        """Extract company website from a portfolio element."""
        link = element.find('a', href=True)
        if link:
            href = link['href']
            # Check if it's an external link (company website)
            if href.startswith('http') and 'portfolio' not in href.lower():
                return href
        return None
    
    def _looks_like_company_name(self, text: str) -> bool:
        """Check if text looks like a company name."""
        if not text or len(text) < 2 or len(text) > 100:
            return False
        
        # Exclude common non-company strings
        exclude_patterns = [
            r'^(home|about|contact|portfolio|investments|news|blog|press)$',
            r'^(read more|learn more|view|see all|load more)$',
            r'^\d+$',  # Just numbers
            r'^[^a-zA-Z]*$',  # No letters
        ]
        
        text_lower = text.lower()
        for pattern in exclude_patterns:
            if re.match(pattern, text_lower):
                return False
        
        # Company names usually:
        # - Start with capital letter or number
        # - Contain letters
        # - May have spaces, dots, or common suffixes
        
        if re.match(r'^[A-Z0-9]', text) and re.search(r'[a-zA-Z]', text):
            return True
        
        return False
    
    def _deduplicate_companies(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate companies from list."""
        seen_names: Set[str] = set()
        unique = []
        
        for company in companies:
            name = company.get("company_name", "").lower().strip()
            if name and name not in seen_names:
                seen_names.add(name)
                unique.append(company)
        
        return unique
