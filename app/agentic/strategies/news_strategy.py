"""
News Search Strategy - Search news and use LLM to extract investment information.

Coverage: 30-50 active investors (those with recent press coverage)
Confidence: MEDIUM

Implementation:
- Search for news articles about investor's investments
- Use LLM (OpenAI/Anthropic) to extract structured data
- Store with source_type='news', confidence_level='medium'
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

from app.agentic.strategies.base import BaseStrategy, InvestorContext, StrategyResult

logger = logging.getLogger(__name__)

# Import LLM client
from app.agentic.llm_client import get_llm_client, LLMClient


class NewsStrategy(BaseStrategy):
    """
    Strategy for extracting investment data from news articles using LLM.

    Searches for press releases and news articles about investments,
    then uses an LLM to extract structured data.

    Confidence: MEDIUM - News can be incomplete or outdated
    """

    name = "news_search"
    display_name = "News Search with LLM Extraction"
    source_type = "news"
    default_confidence = "medium"

    # Rate limits
    max_requests_per_second = 1.0
    max_concurrent_requests = 1
    timeout_seconds = 300

    # Search limits
    MAX_ARTICLES = 20
    MAX_ARTICLE_LENGTH = 5000

    USER_AGENT = "Nexdata Research Bot (news research)"

    # LLM extraction prompt
    EXTRACTION_PROMPT = """Extract investment information from this article about {investor_name}.

Article text:
{article_text}

Return a JSON object with this structure (use null for missing fields):
{{
    "investments": [
        {{
            "company_name": "Name of company invested in",
            "investment_date": "YYYY-MM-DD or null",
            "investment_amount_usd": number or null,
            "investment_type": "VC, PE, public_equity, etc.",
            "company_industry": "sector/industry",
            "co_investors": ["list of other investors"]
        }}
    ]
}}

If no investment information is found, return: {{"investments": []}}
Only include actual investments, not rumors or plans.
Return ONLY the JSON object, no other text."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = asyncio.Semaphore(1)
        self._last_request_time = 0.0
        self._llm_client: Optional[LLMClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
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

        async with self._rate_limiter:
            now = time.time()
            wait_time = (1.0 / self.max_requests_per_second) - (
                now - self._last_request_time
            )

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

    def _get_llm_client(self) -> Optional[LLMClient]:
        """Get or create LLM client (supports OpenAI and Anthropic)."""
        if self._llm_client is None:
            self._llm_client = get_llm_client()
        return self._llm_client

    def is_applicable(self, context: InvestorContext) -> Tuple[bool, str]:
        """
        Check if news strategy is applicable.

        Useful for:
        - Family offices (limited public data, news is main source)
        - Active investors with recent deals
        - Investors without 13F filings
        """
        # Family offices benefit most from news search
        if context.investor_type == "family_office":
            return True, "Family offices have limited public data; news is key source"

        # Large investors likely have news coverage
        if context.aum_usd and context.aum_usd >= 1_000_000_000:
            return True, "Large investor likely has press coverage"

        # Check for name recognition
        well_known_keywords = ["investment", "capital", "partners", "ventures", "fund"]
        if any(kw in context.investor_name.lower() for kw in well_known_keywords):
            return True, "Investment firm likely has news coverage"

        return False, "Investor unlikely to have significant news coverage"

    def calculate_priority(self, context: InvestorContext) -> int:
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0

        # Higher priority for family offices
        if context.investor_type == "family_office":
            return 8

        # Lower priority if we likely have other sources
        if context.lp_type == "public_pension":
            return 5  # They have CAFRs

        return 6

    async def execute(self, context: InvestorContext) -> StrategyResult:
        """
        Execute news search strategy.

        Steps:
        1. Search for news articles about investor's investments
        2. Fetch article content
        3. Use LLM to extract investment information
        4. Compile and return results
        """
        started_at = datetime.utcnow()
        requests_made = 0
        tokens_used = 0
        companies = []
        co_investors = []
        reasoning_parts = []

        try:
            logger.info(f"Executing news strategy for {context.investor_name}")
            reasoning_parts.append(f"Searching news for '{context.investor_name}'")

            # Step 1: Search for news articles
            search_results = await self._search_news(context.investor_name)
            requests_made += 1
            reasoning_parts.append(f"Found {len(search_results)} potential articles")

            if not search_results:
                return self._create_result(
                    success=False,
                    error_message="No news articles found",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made,
                )

            # Step 2: Fetch and process articles
            articles_processed = 0

            for article_info in search_results[: self.MAX_ARTICLES]:
                url = article_info.get("url")
                if not url:
                    continue

                # Fetch article content
                article_text = await self._fetch_article_content(url)
                requests_made += 1

                if not article_text:
                    continue

                articles_processed += 1

                # Step 3: Extract with LLM (or fallback to pattern matching)
                extracted, tokens = await self._extract_with_llm(
                    article_text, context.investor_name, url
                )
                tokens_used += tokens

                if extracted:
                    companies.extend(extracted.get("companies", []))
                    co_investors.extend(extracted.get("co_investors", []))

                # Limit processing
                if articles_processed >= 10 or len(companies) >= 20:
                    break

            reasoning_parts.append(f"Processed {articles_processed} articles")
            reasoning_parts.append(f"Extracted {len(companies)} potential investments")

            if not companies:
                return self._create_result(
                    success=False,
                    error_message="No investments extracted from news",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made,
                    tokens_used=tokens_used,
                )

            # Add source info
            for company in companies:
                company["source_type"] = self.source_type
                company["confidence_level"] = self.default_confidence

            result = self._create_result(
                success=True,
                companies=companies,
                co_investors=co_investors,
                reasoning="\n".join(reasoning_parts),
                requests_made=requests_made,
                tokens_used=tokens_used,
            )
            result.started_at = started_at
            return result

        except Exception as e:
            logger.error(f"Error in news strategy: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning="\n".join(reasoning_parts) + f"\nError: {e}",
                requests_made=requests_made,
                tokens_used=tokens_used,
            )
        finally:
            await self.close()

    async def _search_news(self, investor_name: str) -> List[Dict[str, Any]]:
        """Search for news articles about investor's investments."""
        results = []

        try:
            # Use DuckDuckGo HTML search (no API key required)
            # Search for investment-related news
            queries = [
                f'"{investor_name}" investment',
                f'"{investor_name}" invests',
                f'"{investor_name}" portfolio company',
            ]

            for query in queries[:2]:  # Limit queries
                encoded_query = quote_plus(query)
                search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"

                response = await self._rate_limited_request(search_url)
                if not response or response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "lxml")

                # Parse DuckDuckGo results
                for result_div in soup.find_all("div", class_="result"):
                    title_elem = result_div.find("a", class_="result__a")
                    snippet_elem = result_div.find("a", class_="result__snippet")

                    if title_elem:
                        url = title_elem.get("href", "")
                        title = title_elem.get_text(strip=True)
                        snippet = (
                            snippet_elem.get_text(strip=True) if snippet_elem else ""
                        )

                        # Filter out non-article sites
                        skip_domains = [
                            "linkedin.com",
                            "facebook.com",
                            "twitter.com",
                            "sec.gov",
                        ]
                        if any(d in url.lower() for d in skip_domains):
                            continue

                        results.append({"url": url, "title": title, "snippet": snippet})

                if len(results) >= self.MAX_ARTICLES:
                    break

        except Exception as e:
            logger.warning(f"Error searching news: {e}")

        return results

    async def _fetch_article_content(self, url: str) -> Optional[str]:
        """Fetch and extract main content from an article."""
        try:
            response = await self._rate_limited_request(url)
            if not response or response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "lxml")

            # Remove script and style elements
            for element in soup(["script", "style", "nav", "footer", "header"]):
                element.decompose()

            # Try to find article body
            article_selectors = [
                "article",
                ".article-body",
                ".article-content",
                ".post-content",
                ".entry-content",
                "main",
                '[role="main"]',
                ".story-body",
            ]

            content = ""
            for selector in article_selectors:
                article = soup.select_one(selector)
                if article:
                    content = article.get_text(separator=" ", strip=True)
                    break

            # Fallback to body
            if not content:
                content = (
                    soup.body.get_text(separator=" ", strip=True) if soup.body else ""
                )

            # Clean and truncate
            content = " ".join(content.split())  # Normalize whitespace
            content = content[: self.MAX_ARTICLE_LENGTH]

            return content if len(content) > 100 else None

        except Exception as e:
            logger.warning(f"Error fetching article: {e}")
            return None

    async def _extract_with_llm(
        self, article_text: str, investor_name: str, source_url: str
    ) -> Tuple[Optional[Dict], int]:
        """
        Extract investment info using LLM or fallback to pattern matching.

        Returns: (extracted_data, tokens_used)
        """
        tokens_used = 0

        # Try LLM extraction first
        llm_client = self._get_llm_client()
        if llm_client and llm_client.is_available:
            try:
                prompt = self.EXTRACTION_PROMPT.format(
                    investor_name=investor_name,
                    article_text=article_text[:3000],  # Limit for context
                )

                response = await llm_client.complete(prompt)
                tokens_used = response.total_tokens

                # Parse response as JSON
                data = response.parse_json()
                if data:
                    investments = data.get("investments", [])

                    companies = []
                    co_investors_all = []

                    for inv in investments:
                        if inv.get("company_name"):
                            companies.append(
                                {
                                    "company_name": inv["company_name"],
                                    "investment_date": inv.get("investment_date"),
                                    "investment_amount_usd": str(
                                        inv["investment_amount_usd"]
                                    )
                                    if inv.get("investment_amount_usd")
                                    else None,
                                    "investment_type": inv.get(
                                        "investment_type", "unknown"
                                    ),
                                    "company_industry": inv.get("company_industry"),
                                    "source_url": source_url,
                                    "current_holding": 1,
                                }
                            )

                            # Track co-investors
                            for co_inv in inv.get("co_investors", []):
                                if co_inv:
                                    co_investors_all.append(
                                        {
                                            "co_investor_name": co_inv,
                                            "deal_name": inv["company_name"],
                                        }
                                    )

                    return {
                        "companies": companies,
                        "co_investors": co_investors_all,
                    }, tokens_used
                else:
                    logger.warning("LLM returned non-JSON response")

            except Exception as e:
                logger.warning(f"LLM extraction failed: {e}")

        # Fallback to pattern matching
        return self._extract_with_patterns(
            article_text, investor_name, source_url
        ), tokens_used

    def _extract_with_patterns(
        self, article_text: str, investor_name: str, source_url: str
    ) -> Optional[Dict]:
        """Fallback pattern-based extraction."""
        companies = []

        # Pattern: "{investor} invests in {company}"
        patterns = [
            rf"{re.escape(investor_name)}.*?(?:invests?|invested|investment) in ([A-Z][A-Za-z\s]+)",
            rf"{re.escape(investor_name)}.*?(?:backs?|backed) ([A-Z][A-Za-z\s]+)",
            rf"{re.escape(investor_name)}.*?(?:leads?|led) .*? round (?:in|for) ([A-Z][A-Za-z\s]+)",
            rf"([A-Z][A-Za-z\s]+).*?(?:raises?|raised).*?{re.escape(investor_name)}",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, article_text, re.IGNORECASE)
            for match in matches:
                company_name = match.strip()
                if len(company_name) > 3 and len(company_name) < 50:
                    companies.append(
                        {
                            "company_name": company_name,
                            "investment_type": "unknown",
                            "source_url": source_url,
                            "current_holding": 1,
                        }
                    )

        return {"companies": companies, "co_investors": []} if companies else None
