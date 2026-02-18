"""
Investment Trend Analysis Service.

T23: Surfaces investment trends across LP portfolios including
sector rotation, emerging themes, and geographic shifts.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# Region mapping for geographic normalization
REGION_MAPPING = {
    # US States to Regions
    "CA": "US West",
    "WA": "US West",
    "OR": "US West",
    "NV": "US West",
    "AZ": "US West",
    "CO": "US West",
    "UT": "US West",
    "NM": "US West",
    "ID": "US West",
    "MT": "US West",
    "WY": "US West",
    "AK": "US West",
    "HI": "US West",
    "NY": "US Northeast",
    "MA": "US Northeast",
    "CT": "US Northeast",
    "NJ": "US Northeast",
    "PA": "US Northeast",
    "NH": "US Northeast",
    "VT": "US Northeast",
    "ME": "US Northeast",
    "RI": "US Northeast",
    "DE": "US Northeast",
    "MD": "US Northeast",
    "DC": "US Northeast",
    "TX": "US South",
    "FL": "US South",
    "GA": "US South",
    "NC": "US South",
    "VA": "US South",
    "TN": "US South",
    "SC": "US South",
    "AL": "US South",
    "LA": "US South",
    "KY": "US South",
    "MS": "US South",
    "AR": "US South",
    "OK": "US South",
    "WV": "US South",
    "IL": "US Midwest",
    "OH": "US Midwest",
    "MI": "US Midwest",
    "IN": "US Midwest",
    "WI": "US Midwest",
    "MN": "US Midwest",
    "IA": "US Midwest",
    "MO": "US Midwest",
    "KS": "US Midwest",
    "NE": "US Midwest",
    "SD": "US Midwest",
    "ND": "US Midwest",
    # Countries/Regions
    "UK": "Europe",
    "United Kingdom": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Netherlands": "Europe",
    "Switzerland": "Europe",
    "Sweden": "Europe",
    "Norway": "Europe",
    "Denmark": "Europe",
    "Finland": "Europe",
    "Ireland": "Europe",
    "Spain": "Europe",
    "Italy": "Europe",
    "Belgium": "Europe",
    "Austria": "Europe",
    "Luxembourg": "Europe",
    "China": "Asia Pacific",
    "Japan": "Asia Pacific",
    "South Korea": "Asia Pacific",
    "Singapore": "Asia Pacific",
    "Hong Kong": "Asia Pacific",
    "Taiwan": "Asia Pacific",
    "Australia": "Asia Pacific",
    "New Zealand": "Asia Pacific",
    "India": "Asia Pacific",
    "Canada": "Canada",
    "Brazil": "Latin America",
    "Mexico": "Latin America",
    "Argentina": "Latin America",
    "UAE": "Middle East",
    "Saudi Arabia": "Middle East",
    "Israel": "Middle East",
    "Qatar": "Middle East",
    "Kuwait": "Middle East",
}


class TrendAnalysisService:
    """Service for analyzing investment trends across portfolios."""

    def __init__(self, db: Session):
        self.db = db

    def _normalize_region(self, location: Optional[str]) -> str:
        """Normalize location to a region."""
        if not location:
            return "Unknown"

        location = location.strip()

        # Direct mapping
        if location in REGION_MAPPING:
            return REGION_MAPPING[location]

        # Check if it contains a known key
        for key, region in REGION_MAPPING.items():
            if key.lower() in location.lower():
                return region

        # Check for US state abbreviations at the end (e.g., "San Francisco, CA")
        parts = location.split(",")
        if len(parts) >= 2:
            state = parts[-1].strip().upper()
            if state in REGION_MAPPING:
                return REGION_MAPPING[state]

        return "Other"

    def _calculate_momentum(self, values: List[int]) -> float:
        """
        Calculate momentum score from trend values.
        Positive = accelerating, Negative = decelerating
        Returns normalized score (-100 to +100)
        """
        if len(values) < 2:
            return 0.0

        # Calculate period-over-period changes
        changes = [values[i] - values[i - 1] for i in range(1, len(values))]

        if not changes:
            return 0.0

        # Average change
        avg_change = sum(changes) / len(changes)

        # Normalize by max value to get percentage-like score
        max_val = max(values) if max(values) > 0 else 1
        momentum = (avg_change / max_val) * 100

        return round(min(100, max(-100, momentum)), 2)

    def _get_period_key(self, date: datetime, period: str) -> str:
        """Get period key for a date."""
        if period == "month":
            return date.strftime("%Y-%m")
        elif period == "quarter":
            quarter = (date.month - 1) // 3 + 1
            return f"{date.year}-Q{quarter}"
        else:  # year
            return str(date.year)

    def get_sector_trends(
        self,
        period: str = "quarter",
        periods: int = 4,
        lp_type: Optional[str] = None,
        min_holdings: int = 5,
    ) -> Dict[str, Any]:
        """
        Get sector allocation trends over time.

        Args:
            period: Aggregation period (month, quarter, year)
            periods: Number of periods to return
            lp_type: Filter by LP type
            min_holdings: Minimum holdings for sector inclusion
        """
        # Build query with optional LP type filter
        lp_filter = ""
        params = {}

        if lp_type:
            lp_filter = """
                AND pc.investor_id IN (
                    SELECT id FROM lp_fund WHERE lp_type = :lp_type
                )
            """
            params["lp_type"] = lp_type

        query = text(f"""
            SELECT
                pc.company_industry as sector,
                COALESCE(pc.investment_date, pc.collected_date) as date,
                COUNT(*) as count
            FROM portfolio_companies pc
            WHERE pc.current_holding = 1
                AND pc.company_industry IS NOT NULL
                AND pc.company_industry != ''
                {lp_filter}
            GROUP BY pc.company_industry, date
            ORDER BY date
        """)

        result = self.db.execute(query, params)

        # Aggregate by sector and period
        sector_periods: Dict[str, Dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        all_periods: set = set()

        for row in result.mappings():
            if row["date"]:
                period_key = self._get_period_key(row["date"], period)
                all_periods.add(period_key)
                sector_periods[row["sector"]][period_key] += row["count"]

        # Get the most recent periods
        sorted_periods = sorted(all_periods)[-periods:]

        # Build response
        trends = []
        for sector, period_data in sector_periods.items():
            # Get values for the periods we care about
            values = [period_data.get(p, 0) for p in sorted_periods]
            total = sum(values)

            if total < min_holdings:
                continue

            # Calculate change
            if len(values) >= 2 and values[0] > 0:
                change_pct = ((values[-1] - values[0]) / values[0]) * 100
            else:
                change_pct = 0

            momentum = self._calculate_momentum(values)
            momentum_label = (
                "accelerating"
                if momentum > 10
                else "decelerating"
                if momentum < -10
                else "stable"
            )

            trends.append(
                {
                    "sector": sector,
                    "periods": [
                        {"period": p, "count": period_data.get(p, 0)}
                        for p in sorted_periods
                    ],
                    "total": total,
                    "change_pct": round(change_pct, 1),
                    "momentum": momentum,
                    "momentum_label": momentum_label,
                }
            )

        # Sort by total descending
        trends.sort(key=lambda x: x["total"], reverse=True)

        return {
            "period_type": period,
            "periods": sorted_periods,
            "sectors": trends,
            "total_sectors": len(trends),
        }

    def get_emerging_sectors(
        self,
        limit: int = 10,
        lp_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get sectors with positive momentum (accelerating investment).

        Returns sectors ranked by momentum score.
        """
        # Get quarterly trends
        trends = self.get_sector_trends(period="quarter", periods=4, lp_type=lp_type)

        # Filter to positive momentum and sort
        emerging = [s for s in trends["sectors"] if s["momentum"] > 0]
        emerging.sort(key=lambda x: x["momentum"], reverse=True)

        # Add rank and format
        result = []
        for i, sector in enumerate(emerging[:limit], 1):
            periods = sector["periods"]
            current = periods[-1]["count"] if periods else 0
            previous = periods[-2]["count"] if len(periods) >= 2 else 0

            qoq_change = ((current - previous) / previous * 100) if previous > 0 else 0

            result.append(
                {
                    "rank": i,
                    "sector": sector["sector"],
                    "current_count": current,
                    "total": sector["total"],
                    "momentum_score": sector["momentum"],
                    "qoq_change_pct": round(qoq_change, 1),
                    "trend": sector["momentum_label"],
                }
            )

        return result

    def get_geographic_trends(
        self,
        lp_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get geographic distribution of investments."""
        lp_filter = ""
        params = {}

        if lp_type:
            lp_filter = """
                AND pc.investor_id IN (
                    SELECT id FROM lp_fund WHERE lp_type = :lp_type
                )
            """
            params["lp_type"] = lp_type

        query = text(f"""
            SELECT
                pc.company_location as location,
                pc.company_industry as sector,
                COUNT(*) as count
            FROM portfolio_companies pc
            WHERE pc.current_holding = 1
                {lp_filter}
            GROUP BY pc.company_location, pc.company_industry
        """)

        result = self.db.execute(query, params)

        # Aggregate by region
        region_data: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "sectors": defaultdict(int)}
        )
        total = 0

        for row in result.mappings():
            region = self._normalize_region(row["location"])
            count = row["count"]
            region_data[region]["count"] += count
            total += count
            if row["sector"]:
                region_data[region]["sectors"][row["sector"]] += count

        # Build response
        regions = []
        for region, data in region_data.items():
            # Get top sectors for this region
            top_sectors = sorted(
                data["sectors"].items(), key=lambda x: x[1], reverse=True
            )[:5]

            regions.append(
                {
                    "region": region,
                    "count": data["count"],
                    "pct": round(data["count"] / total * 100, 1) if total > 0 else 0,
                    "top_sectors": [s[0] for s in top_sectors],
                }
            )

        regions.sort(key=lambda x: x["count"], reverse=True)

        return {
            "total_holdings": total,
            "regions": regions,
        }

    def get_stage_trends(
        self,
        lp_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get investment stage distribution and trends."""
        lp_filter = ""
        params = {}

        if lp_type:
            lp_filter = """
                AND pc.investor_id IN (
                    SELECT id FROM lp_fund WHERE lp_type = :lp_type
                )
            """
            params["lp_type"] = lp_type

        query = text(f"""
            SELECT
                COALESCE(pc.company_stage, 'Unknown') as stage,
                COUNT(*) as count
            FROM portfolio_companies pc
            WHERE pc.current_holding = 1
                {lp_filter}
            GROUP BY pc.company_stage
            ORDER BY count DESC
        """)

        result = self.db.execute(query, params)

        stages = []
        total = 0

        for row in result.mappings():
            stages.append(
                {
                    "stage": row["stage"],
                    "count": row["count"],
                }
            )
            total += row["count"]

        # Add percentages
        for stage in stages:
            stage["pct"] = round(stage["count"] / total * 100, 1) if total > 0 else 0

        return {
            "total_holdings": total,
            "stages": stages,
        }

    def get_trends_by_lp_type(self) -> Dict[str, Any]:
        """Compare sector allocations by LP type."""
        query = text("""
            SELECT
                lf.lp_type,
                pc.company_industry as sector,
                COUNT(*) as count
            FROM portfolio_companies pc
            JOIN lp_fund lf ON pc.investor_id = lf.id AND pc.investor_type = 'lp'
            WHERE pc.current_holding = 1
                AND pc.company_industry IS NOT NULL
                AND pc.company_industry != ''
            GROUP BY lf.lp_type, pc.company_industry
        """)

        result = self.db.execute(query)

        # Aggregate by LP type
        lp_type_data: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        lp_type_totals: Dict[str, int] = defaultdict(int)

        for row in result.mappings():
            lp_type = row["lp_type"] or "Unknown"
            sector = row["sector"]
            count = row["count"]
            lp_type_data[lp_type][sector] += count
            lp_type_totals[lp_type] += count

        # Build response with top sectors per LP type
        comparisons = []
        for lp_type, sectors in lp_type_data.items():
            total = lp_type_totals[lp_type]
            top_sectors = sorted(sectors.items(), key=lambda x: x[1], reverse=True)[:10]

            comparisons.append(
                {
                    "lp_type": lp_type,
                    "total_holdings": total,
                    "top_sectors": [
                        {
                            "sector": s[0],
                            "count": s[1],
                            "pct": round(s[1] / total * 100, 1) if total > 0 else 0,
                        }
                        for s in top_sectors
                    ],
                }
            )

        comparisons.sort(key=lambda x: x["total_holdings"], reverse=True)

        return {
            "lp_types": comparisons,
            "total_lp_types": len(comparisons),
        }

    def get_allocation_snapshot(self) -> Dict[str, Any]:
        """Get current allocation snapshot across all dimensions."""
        # Total holdings
        total_result = self.db.execute(
            text("""
            SELECT COUNT(*) as total FROM portfolio_companies WHERE current_holding = 1
        """)
        )
        total = total_result.fetchone()[0]

        # By sector
        sector_result = self.db.execute(
            text("""
            SELECT company_industry as sector, COUNT(*) as count
            FROM portfolio_companies
            WHERE current_holding = 1 AND company_industry IS NOT NULL
            GROUP BY company_industry
            ORDER BY count DESC
            LIMIT 20
        """)
        )
        by_sector = [
            {
                "sector": row[0],
                "count": row[1],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in sector_result
        ]

        # By LP type
        lp_type_result = self.db.execute(
            text("""
            SELECT lf.lp_type, COUNT(*) as count
            FROM portfolio_companies pc
            JOIN lp_fund lf ON pc.investor_id = lf.id AND pc.investor_type = 'lp'
            WHERE pc.current_holding = 1
            GROUP BY lf.lp_type
            ORDER BY count DESC
        """)
        )
        by_lp_type = [
            {
                "lp_type": row[0],
                "count": row[1],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in lp_type_result
        ]

        # By stage
        stage_result = self.db.execute(
            text("""
            SELECT COALESCE(company_stage, 'Unknown') as stage, COUNT(*) as count
            FROM portfolio_companies
            WHERE current_holding = 1
            GROUP BY company_stage
            ORDER BY count DESC
        """)
        )
        by_stage = [
            {
                "stage": row[0],
                "count": row[1],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in stage_result
        ]

        # Investor counts
        investor_result = self.db.execute(
            text("""
            SELECT
                (SELECT COUNT(DISTINCT id) FROM lp_fund) as lp_count,
                (SELECT COUNT(DISTINCT id) FROM family_offices) as fo_count
        """)
        )
        investor_row = investor_result.fetchone()

        return {
            "total_holdings": total,
            "total_investors": investor_row[0] + investor_row[1],
            "lp_count": investor_row[0],
            "family_office_count": investor_row[1],
            "by_sector": by_sector,
            "by_lp_type": by_lp_type,
            "by_stage": by_stage,
            "generated_at": datetime.utcnow().isoformat(),
        }
