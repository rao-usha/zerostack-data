"""
Reverse Search Strategy - Find companies that mention this investor on their website.

Coverage: 20-40 companies per large investor
Confidence: HIGH (company confirms relationship)

Implementation:
- Search for companies that mention the investor
- Filter out news sites, LinkedIn, SEC.gov
- Check company websites for investor mentions
- Extract company info (name, industry, location)
- Store with source_type='portfolio_company_website', confidence_level='high'
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urlparse

import httpx
from bs4 import BeautifulSoup

from app.agentic.strategies.base import BaseStrategy, InvestorContext, StrategyResult

logger = logging.getLogger(__name__)


class ReverseSearchStrategy(BaseStrategy):
    """
    Strategy for finding portfolio companies via reverse search.
    
    Searches for companies that mention the investor on their websites,
    typically on "About Us" or "Investors" pages.
    
    Confidence: HIGH - Company confirms the relationship
    """
    
    name = "reverse_search"
    display_name = "Portfolio Company Reverse Search"
    source_type = "portfolio_company_website"
    default_confidence = "high"
    
    # Rate limits
    max_requests_per_second = 0.5
    max_concurrent_requests = 1
    timeout_seconds = 300
    
    # Search limits
    MAX_SEARCH_RESULTS = 30
    MAX_COMPANIES = 20
    
    # Domains to exclude from search results
    EXCLUDED_DOMAINS = {
        'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
        'sec.gov', 'bloomberg.com', 'reuters.com', 'wsj.com',
        'businesswire.com', 'prnewswire.com', 'globenewswire.com',
        'youtube.com', 'wikipedia.org', 'crunchbase.com', 'pitchbook.com',
        'forbes.com', 'fortune.com', 'ft.com', 'cnbc.com'
    }
    
    # Keywords indicating investor relationship
    INVESTOR_KEYWORDS = [
        'investor', 'backed by', 'portfolio company', 'invested in us',
        'our investors', 'supported by', 'funding from', 'led by',
        'series a', 'series b', 'series c', 'seed round', 'growth equity'
    ]
    
    USER_AGENT = "Nexdata Research Bot (portfolio research)"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = asyncio.Semaphore(1)
        self._last_request_time = 0.0
    
    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={"User-Agent": self.USER_AGENT},
                follow_redirects=True
            )
        return self._client
    
    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limited_request(self, url: str) -> Optional[httpx.Response]:
        import time
        
        async with self._rate_limiter:
            now = time.time()
            wait_time = (1.0 / self.max_requests_per_second) - (now - self._last_request_time)
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            try:
                client = await self._get_client()
                response = await client.get(url)
                self._last_request_time = time.time()
                return response
            except Exception as e:
                logger.warning(f"Request failed for {url}: {e}")
                return None
    
    def is_applicable(self, context: InvestorContext) -> Tuple[bool, str]:
        """
        Check if reverse search strategy is applicable.
        
        Applicable for:
        - All investors (low cost, decent yield)
        - More effective for well-known investors
        """
        # Always applicable - it's a cheap strategy
        return True, "Reverse search is always worth trying (portfolio companies often list investors)"
    
    def calculate_priority(self, context: InvestorContext) -> int:
        # Always applicable but lower priority than primary sources
        priority = 6
        
        # Higher priority for VC/PE firms (portfolio companies more likely to list them)
        if context.investor_type == "family_office":
            priority = 7  # FOs have few other data sources
        
        # Large well-known investors more likely to be mentioned
        if context.aum_usd and context.aum_usd >= 10_000_000_000:
            priority = 7
        
        return priority
    
    async def execute(self, context: InvestorContext) -> StrategyResult:
        """
        Execute reverse search strategy.
        
        Steps:
        1. Search for pages mentioning investor + investment keywords
        2. Filter to company websites (exclude news, social, etc.)
        3. Visit websites to confirm investor mention and extract company info
        """
        started_at = datetime.utcnow()
        requests_made = 0
        companies = []
        reasoning_parts = []
        
        try:
            logger.info(f"Executing reverse search for {context.investor_name}")
            reasoning_parts.append(f"Searching for companies that mention '{context.investor_name}'")
            
            # Step 1: Search for mentions
            search_results = await self._search_for_mentions(context.investor_name)
            requests_made += 2  # Multiple search queries
            reasoning_parts.append(f"Found {len(search_results)} potential company websites")
            
            if not search_results:
                return self._create_result(
                    success=False,
                    error_message="No company websites found mentioning investor",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            # Step 2: Verify each website
            verified_companies = 0
            
            for result in search_results[:self.MAX_SEARCH_RESULTS]:
                url = result.get('url')
                if not url:
                    continue
                
                # Skip excluded domains
                domain = urlparse(url).netloc.lower()
                if any(excluded in domain for excluded in self.EXCLUDED_DOMAINS):
                    continue
                
                # Verify investor mention on the page
                company_info = await self._verify_and_extract(url, context.investor_name)
                requests_made += 1
                
                if company_info:
                    companies.append(company_info)
                    verified_companies += 1
                    reasoning_parts.append(f"Verified: {company_info.get('company_name')} ({domain})")
                
                if verified_companies >= self.MAX_COMPANIES:
                    break
            
            reasoning_parts.append(f"Verified {len(companies)} companies mentioning {context.investor_name}")
            
            if not companies:
                return self._create_result(
                    success=False,
                    error_message="No verified company mentions found",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            # Add source info
            for company in companies:
                company["source_type"] = self.source_type
                company["confidence_level"] = self.default_confidence
            
            result = self._create_result(
                success=True,
                companies=companies,
                reasoning="\n".join(reasoning_parts),
                requests_made=requests_made
            )
            result.started_at = started_at
            return result
        
        except Exception as e:
            logger.error(f"Error in reverse search: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning="\n".join(reasoning_parts) + f"\nError: {e}",
                requests_made=requests_made
            )
        finally:
            await self.close()
    
    async def _search_for_mentions(self, investor_name: str) -> List[Dict[str, Any]]:
        """Search for websites that mention the investor."""
        results = []
        seen_domains: Set[str] = set()
        
        try:
            # Search queries to find portfolio companies
            queries = [
                f'"{investor_name}" investor',
                f'"{investor_name}" "backed by"',
                f'"{investor_name}" "portfolio company"',
            ]
            
            for query in queries:
                # Use DuckDuckGo HTML search
                encoded_query = quote_plus(query)
                search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
                
                response = await self._rate_limited_request(search_url)
                if not response or response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                for result_div in soup.find_all('div', class_='result'):
                    link = result_div.find('a', class_='result__a')
                    if not link:
                        continue
                    
                    url = link.get('href', '')
                    title = link.get_text(strip=True)
                    
                    # Extract domain
                    domain = urlparse(url).netloc.lower()
                    
                    # Skip if already seen or excluded
                    if domain in seen_domains:
                        continue
                    if any(excluded in domain for excluded in self.EXCLUDED_DOMAINS):
                        continue
                    
                    seen_domains.add(domain)
                    results.append({
                        'url': url,
                        'title': title,
                        'domain': domain
                    })
                
                if len(results) >= self.MAX_SEARCH_RESULTS:
                    break
        
        except Exception as e:
            logger.warning(f"Error in search: {e}")
        
        return results
    
    async def _verify_and_extract(
        self, 
        url: str, 
        investor_name: str
    ) -> Optional[Dict[str, Any]]:
        """Verify investor mention on page and extract company info."""
        try:
            response = await self._rate_limited_request(url)
            if not response or response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Get page text
            for element in soup(['script', 'style', 'nav', 'footer']):
                element.decompose()
            
            text = soup.get_text(separator=' ', strip=True).lower()
            
            # Verify investor is mentioned
            if investor_name.lower() not in text:
                return None
            
            # Check for investor keywords nearby
            has_investor_context = False
            for keyword in self.INVESTOR_KEYWORDS:
                if keyword in text:
                    has_investor_context = True
                    break
            
            if not has_investor_context:
                return None
            
            # Extract company information
            company_name = self._extract_company_name(soup, url)
            if not company_name:
                return None
            
            company_info = {
                "company_name": company_name,
                "company_website": f"https://{urlparse(url).netloc}",
                "company_industry": self._extract_industry(soup, text),
                "company_location": self._extract_location(soup, text),
                "investment_type": "unknown",
                "source_url": url,
                "current_holding": 1,
            }
            
            return company_info
        
        except Exception as e:
            logger.warning(f"Error verifying {url}: {e}")
            return None
    
    def _extract_company_name(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """Extract company name from the page."""
        # Try meta tags first
        og_title = soup.find('meta', property='og:site_name')
        if og_title and og_title.get('content'):
            return og_title['content'].strip()
        
        # Try title tag
        title = soup.find('title')
        if title:
            title_text = title.get_text(strip=True)
            # Clean common suffixes
            for suffix in [' - Home', ' | Home', ' - Official', ' | Official', ' - Company']:
                if suffix in title_text:
                    return title_text.split(suffix)[0].strip()
            # Take first part of title (often company name)
            if ' - ' in title_text:
                return title_text.split(' - ')[0].strip()
            if ' | ' in title_text:
                return title_text.split(' | ')[0].strip()
            return title_text[:50] if len(title_text) <= 50 else None
        
        # Fallback to domain name
        domain = urlparse(url).netloc
        # Remove common prefixes/suffixes
        name = domain.replace('www.', '').split('.')[0]
        return name.title() if len(name) > 2 else None
    
    def _extract_industry(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        """Try to extract industry/sector from the page."""
        # Check meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            desc = meta_desc.get('content', '').lower()
        else:
            desc = text[:1000]
        
        # Industry keyword matching
        industry_keywords = {
            'fintech': ['fintech', 'financial technology', 'payments', 'banking'],
            'healthcare': ['healthcare', 'health tech', 'medical', 'biotech', 'pharma'],
            'technology': ['software', 'saas', 'technology', 'ai ', 'artificial intelligence', 'machine learning'],
            'e-commerce': ['e-commerce', 'ecommerce', 'marketplace', 'retail'],
            'climate_tech': ['climate', 'cleantech', 'sustainability', 'renewable', 'energy'],
            'enterprise': ['enterprise', 'b2b', 'business software'],
            'consumer': ['consumer', 'd2c', 'direct to consumer'],
            'real_estate': ['real estate', 'proptech', 'property'],
            'logistics': ['logistics', 'supply chain', 'shipping'],
            'food_tech': ['food tech', 'foodtech', 'delivery', 'restaurant'],
        }
        
        for industry, keywords in industry_keywords.items():
            for kw in keywords:
                if kw in desc:
                    return industry
        
        return None
    
    def _extract_location(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        """Try to extract company location."""
        # Check for address in structured data
        address = soup.find('address')
        if address:
            addr_text = address.get_text(strip=True)
            if len(addr_text) < 100:
                return addr_text
        
        # Look for location patterns
        location_patterns = [
            r'headquartered in ([A-Z][a-z]+(?:,\s*[A-Z]{2})?)',
            r'based in ([A-Z][a-z]+(?:,\s*[A-Z]{2})?)',
            r'located in ([A-Z][a-z]+(?:,\s*[A-Z]{2})?)',
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
