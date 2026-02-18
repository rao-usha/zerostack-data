"""
Portfolio Comparison Service for T17.

Provides side-by-side portfolio comparison, historical diffs,
and industry allocation analysis.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class HoldingSummary:
    """Summary of a portfolio holding."""

    company_id: int
    company_name: str
    industry: Optional[str] = None
    market_value_usd: Optional[str] = None
    shares_held: Optional[str] = None


@dataclass
class InvestorSummary:
    """Summary of an investor."""

    id: int
    name: str
    investor_type: str
    total_holdings: int
    total_value: Optional[str] = None


@dataclass
class IndustryAllocation:
    """Industry allocation comparison."""

    industry: str
    count_a: int
    count_b: int
    percentage_a: float
    percentage_b: float


@dataclass
class PortfolioComparison:
    """Complete portfolio comparison result."""

    investor_a: InvestorSummary
    investor_b: InvestorSummary

    # Overlap metrics
    overlap_count: int
    overlap_percentage_a: float
    overlap_percentage_b: float
    jaccard_similarity: float
    jaccard_percentage: float

    # Holdings
    shared_holdings: List[HoldingSummary]
    unique_to_a: List[HoldingSummary]
    unique_to_b: List[HoldingSummary]

    # Top holdings
    top_holdings_a: List[HoldingSummary]
    top_holdings_b: List[HoldingSummary]

    # Industry comparison
    industry_comparison: List[IndustryAllocation]

    # Metadata
    comparison_date: str


@dataclass
class HistoricalDiff:
    """Historical portfolio diff result."""

    investor_id: int
    investor_name: str
    period_start: str
    period_end: str

    # Changes
    additions: List[HoldingSummary]
    removals: List[HoldingSummary]
    unchanged_count: int

    # Summary
    holdings_start: int
    holdings_end: int
    net_change: int


# =============================================================================
# Portfolio Comparison Service
# =============================================================================


class PortfolioComparisonService:
    """Service for comparing investor portfolios."""

    def __init__(self, db: Session):
        self.db = db

    def get_investor_info(self, investor_id: int) -> Optional[InvestorSummary]:
        """Get basic investor information."""
        result = self.db.execute(
            text("""
                SELECT id, name, lp_type
                FROM lp_fund
                WHERE id = :id
            """),
            {"id": investor_id},
        )
        row = result.fetchone()

        if not row:
            return None

        # Get holdings count
        count_result = self.db.execute(
            text("""
                SELECT COUNT(*)
                FROM portfolio_companies
                WHERE investor_id = :id
            """),
            {"id": investor_id},
        )
        holdings_count = count_result.scalar() or 0

        return InvestorSummary(
            id=row[0], name=row[1], investor_type=row[2], total_holdings=holdings_count
        )

    def get_holdings(self, investor_id: int) -> List[HoldingSummary]:
        """Get all holdings for an investor."""
        result = self.db.execute(
            text("""
                SELECT id, company_name, company_industry, market_value_usd, shares_held
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                ORDER BY company_name
            """),
            {"investor_id": investor_id},
        )

        return [
            HoldingSummary(
                company_id=row[0],
                company_name=row[1],
                industry=row[2],
                market_value_usd=row[3],
                shares_held=row[4],
            )
            for row in result.fetchall()
        ]

    def get_top_holdings(
        self, investor_id: int, limit: int = 10
    ) -> List[HoldingSummary]:
        """Get top holdings by market value (or alphabetically if no values)."""
        result = self.db.execute(
            text("""
                SELECT id, company_name, company_industry, market_value_usd, shares_held
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                ORDER BY
                    CASE WHEN market_value_usd IS NOT NULL AND market_value_usd != ''
                         THEN CAST(REGEXP_REPLACE(market_value_usd, '[^0-9.]', '', 'g') AS NUMERIC)
                         ELSE 0
                    END DESC,
                    company_name ASC
                LIMIT :limit
            """),
            {"investor_id": investor_id, "limit": limit},
        )

        return [
            HoldingSummary(
                company_id=row[0],
                company_name=row[1],
                industry=row[2],
                market_value_usd=row[3],
                shares_held=row[4],
            )
            for row in result.fetchall()
        ]

    def compare_portfolios(
        self, investor_a_id: int, investor_b_id: int, top_holdings: int = 10
    ) -> Optional[PortfolioComparison]:
        """Compare two investors' current portfolios."""

        # Get investor info
        investor_a = self.get_investor_info(investor_a_id)
        investor_b = self.get_investor_info(investor_b_id)

        if not investor_a or not investor_b:
            return None

        # Get holdings
        holdings_a = self.get_holdings(investor_a_id)
        holdings_b = self.get_holdings(investor_b_id)

        # Build name sets for comparison (normalize names)
        names_a = {h.company_name.lower().strip() for h in holdings_a}
        names_b = {h.company_name.lower().strip() for h in holdings_b}

        # Build lookup dicts
        holdings_a_dict = {h.company_name.lower().strip(): h for h in holdings_a}
        holdings_b_dict = {h.company_name.lower().strip(): h for h in holdings_b}

        # Calculate overlap
        shared_names = names_a & names_b
        unique_a_names = names_a - names_b
        unique_b_names = names_b - names_a

        overlap_count = len(shared_names)

        # Calculate percentages
        overlap_pct_a = (overlap_count / len(names_a) * 100) if names_a else 0
        overlap_pct_b = (overlap_count / len(names_b) * 100) if names_b else 0

        # Calculate Jaccard similarity
        union_count = len(names_a | names_b)
        jaccard = overlap_count / union_count if union_count > 0 else 0

        # Build holding lists
        shared_holdings = [holdings_a_dict[name] for name in sorted(shared_names)]
        unique_to_a = [holdings_a_dict[name] for name in sorted(unique_a_names)]
        unique_to_b = [holdings_b_dict[name] for name in sorted(unique_b_names)]

        # Get top holdings
        top_a = self.get_top_holdings(investor_a_id, top_holdings)
        top_b = self.get_top_holdings(investor_b_id, top_holdings)

        # Industry comparison
        industry_comparison = self._compare_industries(holdings_a, holdings_b)

        return PortfolioComparison(
            investor_a=investor_a,
            investor_b=investor_b,
            overlap_count=overlap_count,
            overlap_percentage_a=round(overlap_pct_a, 2),
            overlap_percentage_b=round(overlap_pct_b, 2),
            jaccard_similarity=round(jaccard, 4),
            jaccard_percentage=round(jaccard * 100, 2),
            shared_holdings=shared_holdings,
            unique_to_a=unique_to_a,
            unique_to_b=unique_to_b,
            top_holdings_a=top_a,
            top_holdings_b=top_b,
            industry_comparison=industry_comparison,
            comparison_date=datetime.utcnow().isoformat(),
        )

    def _compare_industries(
        self, holdings_a: List[HoldingSummary], holdings_b: List[HoldingSummary]
    ) -> List[IndustryAllocation]:
        """Compare industry allocations between two sets of holdings."""

        # Count industries for A
        industry_counts_a: Dict[str, int] = {}
        for h in holdings_a:
            industry = h.industry or "Unknown"
            industry_counts_a[industry] = industry_counts_a.get(industry, 0) + 1

        # Count industries for B
        industry_counts_b: Dict[str, int] = {}
        for h in holdings_b:
            industry = h.industry or "Unknown"
            industry_counts_b[industry] = industry_counts_b.get(industry, 0) + 1

        # Get all industries
        all_industries = set(industry_counts_a.keys()) | set(industry_counts_b.keys())

        # Calculate totals
        total_a = len(holdings_a)
        total_b = len(holdings_b)

        # Build comparison
        comparisons = []
        for industry in sorted(all_industries):
            count_a = industry_counts_a.get(industry, 0)
            count_b = industry_counts_b.get(industry, 0)

            comparisons.append(
                IndustryAllocation(
                    industry=industry,
                    count_a=count_a,
                    count_b=count_b,
                    percentage_a=round((count_a / total_a * 100) if total_a else 0, 2),
                    percentage_b=round((count_b / total_b * 100) if total_b else 0, 2),
                )
            )

        # Sort by total count descending
        comparisons.sort(key=lambda x: x.count_a + x.count_b, reverse=True)

        return comparisons

    def compare_history(
        self,
        investor_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[HistoricalDiff]:
        """Compare an investor's portfolio over time."""

        investor = self.get_investor_info(investor_id)
        if not investor:
            return None

        # Get distinct collection dates
        dates_result = self.db.execute(
            text("""
                SELECT DISTINCT DATE(collected_date) as cdate
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                ORDER BY cdate
            """),
            {"investor_id": investor_id},
        )
        dates = [row[0] for row in dates_result.fetchall()]

        if len(dates) < 2:
            # Not enough historical data
            return HistoricalDiff(
                investor_id=investor_id,
                investor_name=investor.name,
                period_start=str(dates[0]) if dates else "N/A",
                period_end=str(dates[0]) if dates else "N/A",
                additions=[],
                removals=[],
                unchanged_count=investor.total_holdings,
                holdings_start=investor.total_holdings,
                holdings_end=investor.total_holdings,
                net_change=0,
            )

        # Use provided dates or default to first and last
        if start_date and end_date:
            period_start = start_date.date()
            period_end = end_date.date()
        else:
            period_start = dates[0]
            period_end = dates[-1]

        # Get holdings at start date (closest to or before start)
        start_holdings = self.db.execute(
            text("""
                SELECT DISTINCT company_name, company_industry
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                AND DATE(collected_date) <= :start_date
            """),
            {"investor_id": investor_id, "start_date": period_start},
        )
        start_names = {row[0].lower().strip(): row for row in start_holdings.fetchall()}

        # Get holdings at end date
        end_holdings = self.db.execute(
            text("""
                SELECT DISTINCT company_name, company_industry, id
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                AND DATE(collected_date) <= :end_date
            """),
            {"investor_id": investor_id, "end_date": period_end},
        )
        end_names = {row[0].lower().strip(): row for row in end_holdings.fetchall()}

        # Calculate diff
        start_set = set(start_names.keys())
        end_set = set(end_names.keys())

        added_names = end_set - start_set
        removed_names = start_set - end_set
        unchanged_names = start_set & end_set

        additions = [
            HoldingSummary(
                company_id=end_names[name][2],
                company_name=end_names[name][0],
                industry=end_names[name][1],
            )
            for name in sorted(added_names)
        ]

        removals = [
            HoldingSummary(
                company_id=0,  # Don't have ID for removed
                company_name=start_names[name][0],
                industry=start_names[name][1],
            )
            for name in sorted(removed_names)
        ]

        return HistoricalDiff(
            investor_id=investor_id,
            investor_name=investor.name,
            period_start=str(period_start),
            period_end=str(period_end),
            additions=additions,
            removals=removals,
            unchanged_count=len(unchanged_names),
            holdings_start=len(start_set),
            holdings_end=len(end_set),
            net_change=len(end_set) - len(start_set),
        )

    def get_industry_comparison(
        self, investor_a_id: int, investor_b_id: int
    ) -> Optional[List[IndustryAllocation]]:
        """Get industry allocation comparison between two investors."""

        holdings_a = self.get_holdings(investor_a_id)
        holdings_b = self.get_holdings(investor_b_id)

        if not holdings_a and not holdings_b:
            return None

        return self._compare_industries(holdings_a, holdings_b)
