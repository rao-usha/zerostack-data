"""
Performance data collector for LP funds.

Extracts investment returns and performance metrics from:
- CAFR (Comprehensive Annual Financial Reports)
- Annual reports
- Investment reports on LP websites

Data includes:
- 1/3/5/10 year returns
- Benchmark comparisons
- Value added metrics
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# Common performance page patterns
PERFORMANCE_PAGE_PATTERNS = [
    "/investments",
    "/investment-performance",
    "/performance",
    "/returns",
    "/investment-returns",
    "/financial-reports",
    "/annual-report",
]

# Return period patterns
RETURN_PATTERNS = {
    "one_year": [
        r"1[- ]?year",
        r"one[- ]?year",
        r"1[- ]?yr",
        r"annual\s+return",
        r"fy\s*\d{4}",
    ],
    "three_year": [
        r"3[- ]?year",
        r"three[- ]?year",
        r"3[- ]?yr",
    ],
    "five_year": [
        r"5[- ]?year",
        r"five[- ]?year",
        r"5[- ]?yr",
    ],
    "ten_year": [
        r"10[- ]?year",
        r"ten[- ]?year",
        r"10[- ]?yr",
    ],
    "twenty_year": [
        r"20[- ]?year",
        r"twenty[- ]?year",
        r"20[- ]?yr",
    ],
    "since_inception": [
        r"since\s+inception",
        r"inception[- ]?to[- ]?date",
        r"itd",
    ],
}

# Percentage pattern - matches numbers like 8.5%, -2.3%, etc.
PERCENTAGE_PATTERN = re.compile(r"(-?\d{1,3}(?:\.\d{1,2})?)\s*%")

# Fiscal year pattern
FISCAL_YEAR_PATTERN = re.compile(
    r"(?:fiscal\s+year|fy)\s*(\d{4})|(\d{4})\s*(?:fiscal|annual)", re.IGNORECASE
)


class PerformanceCollector(BaseCollector):
    """
    Collects performance return data from LP sources.

    Extracts:
    - Historical returns (1/3/5/10 year)
    - Benchmark comparisons
    - Value added metrics
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.CAFR  # Primary source is annual reports

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect performance data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL

        Returns:
            CollectionResult with performance items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting performance data for {lp_name}")

        if not website_url:
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message="No website URL provided",
                started_at=started_at,
            )

        try:
            # Try to find performance data on website
            perf_items = await self._collect_from_website(website_url, lp_id, lp_name)
            items.extend(perf_items)

            success = len(items) > 0
            if not items:
                warnings.append("No performance data found on website")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting performance for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _collect_from_website(
        self,
        website_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Collect performance data from LP website."""
        items = []

        for pattern in PERFORMANCE_PAGE_PATTERNS:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                perf_data = self._extract_performance_data(
                    response.text, page_url, lp_id, lp_name
                )
                if perf_data:
                    items.append(perf_data)
                    logger.info(f"Found performance data at {page_url}")
                    break

        return items

    def _extract_performance_data(
        self,
        html: str,
        source_url: str,
        lp_id: int,
        lp_name: str,
    ) -> Optional[CollectedItem]:
        """Extract performance return data from HTML."""
        html_lower = html.lower()

        # Check if this page has performance data
        if not any(kw in html_lower for kw in ["return", "performance", "benchmark"]):
            return None

        # Extract fiscal year
        fiscal_year = self._extract_fiscal_year(html)
        if not fiscal_year:
            fiscal_year = datetime.now().year - 1  # Default to last year

        # Extract returns for each period
        returns = {}
        for period, patterns in RETURN_PATTERNS.items():
            value = self._extract_return_for_period(html, patterns)
            if value is not None:
                returns[f"{period}_return_pct"] = str(value)

        if not returns:
            return None

        # Try to extract benchmark data
        benchmark_name, benchmark_returns = self._extract_benchmark_data(html)

        data = {
            "lp_id": lp_id,
            "lp_name": lp_name,
            "fiscal_year": fiscal_year,
            "source_type": "website",
            **returns,
        }

        if benchmark_name:
            data["benchmark_name"] = benchmark_name
            data.update(benchmark_returns)

        return CollectedItem(
            item_type="performance_return",
            data=data,
            source_url=source_url,
            confidence="medium",
        )

    def _extract_fiscal_year(self, html: str) -> Optional[int]:
        """Extract fiscal year from the page."""
        match = FISCAL_YEAR_PATTERN.search(html)
        if match:
            year_str = match.group(1) or match.group(2)
            try:
                return int(year_str)
            except ValueError:
                pass

        # Look for year in common formats
        year_pattern = re.compile(r"20[12][0-9]")
        years = year_pattern.findall(html)
        if years:
            # Return the most recent year found
            return max(int(y) for y in years)

        return None

    def _extract_return_for_period(
        self,
        html: str,
        period_patterns: List[str],
    ) -> Optional[float]:
        """Extract return percentage for a specific time period."""
        html_lower = html.lower()

        for pattern in period_patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.search(html_lower)

            if match:
                # Look for percentage near this match
                context_start = match.start()
                context_end = min(len(html), match.end() + 100)
                context = html[context_start:context_end]

                pct_match = PERCENTAGE_PATTERN.search(context)
                if pct_match:
                    try:
                        return float(pct_match.group(1))
                    except ValueError:
                        continue

        return None

    def _extract_benchmark_data(
        self,
        html: str,
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """Extract benchmark name and returns."""
        html_lower = html.lower()
        benchmark_returns = {}

        # Common benchmark patterns
        benchmark_patterns = [
            r"policy\s+benchmark",
            r"(?:60/40|70/30)\s+(?:benchmark|portfolio)",
            r"composite\s+benchmark",
            r"blended\s+benchmark",
            r"total\s+fund\s+benchmark",
        ]

        benchmark_name = None
        for pattern in benchmark_patterns:
            match = re.search(pattern, html_lower)
            if match:
                # Try to get the actual benchmark name from context
                context_start = max(0, match.start() - 20)
                context_end = min(len(html), match.end() + 50)
                context = html[context_start:context_end]
                benchmark_name = context.strip()[:100]
                break

        if not benchmark_name:
            return None, {}

        # Look for benchmark returns
        # This is simplified - in production would need more sophisticated parsing
        bm_section_match = re.search(
            r"benchmark.*?(\d{1,2}(?:\.\d{1,2})?)\s*%", html_lower, re.DOTALL
        )
        if bm_section_match:
            try:
                benchmark_returns["benchmark_one_year_pct"] = bm_section_match.group(1)
            except (IndexError, ValueError):
                pass

        return benchmark_name, benchmark_returns

    def _extract_total_fund_value(self, html: str) -> Optional[str]:
        """Extract total fund value/AUM from the page."""
        # Look for billion/million patterns
        value_pattern = re.compile(
            r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)\s*(?:billion|B)", re.IGNORECASE
        )

        match = value_pattern.search(html)
        if match:
            value_str = match.group(1).replace(",", "")
            try:
                # Convert to USD (assuming billions)
                value = float(value_str) * 1_000_000_000
                return str(value)
            except ValueError:
                pass

        return None
