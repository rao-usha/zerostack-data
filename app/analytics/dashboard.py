"""
Dashboard Analytics Engine (T13).

Provides pre-computed analytics for frontend dashboards with portfolio insights,
trends, data quality metrics, and system health statistics.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class DataQualityScore:
    """Data quality score breakdown."""

    overall_score: int  # 0-100
    completeness: int  # % of fields populated
    freshness: int  # Based on data age
    source_diversity: int  # Multiple sources = higher
    confidence_avg: float
    issues: List[str]


class DashboardAnalytics:
    """
    Analytics computation engine for dashboard endpoints.

    Design principles:
    - All methods are read-only (no writes)
    - Queries are optimized with appropriate indexes
    - Results can be cached (future enhancement)
    - Graceful handling of missing data
    """

    def __init__(self, db: Session):
        self.db = db

    async def get_system_overview(self) -> dict:
        """
        Compute system-wide statistics for main dashboard.

        Returns investor coverage, portfolio totals, collection activity,
        alert stats, and data freshness metrics.
        """
        result = {}

        # Investor counts
        lp_count = self.db.execute(text("SELECT COUNT(*) FROM lp_fund")).scalar() or 0

        fo_count = (
            self.db.execute(text("SELECT COUNT(*) FROM family_offices")).scalar() or 0
        )

        result["total_lps"] = lp_count
        result["total_family_offices"] = fo_count

        # Investors with portfolio data
        lps_with_data = (
            self.db.execute(
                text("""
                SELECT COUNT(DISTINCT investor_id)
                FROM portfolio_companies
                WHERE investor_type = 'lp'
            """)
            ).scalar()
            or 0
        )

        fos_with_data = (
            self.db.execute(
                text("""
                SELECT COUNT(DISTINCT investor_id)
                FROM portfolio_companies
                WHERE investor_type = 'family_office'
            """)
            ).scalar()
            or 0
        )

        result["lps_with_portfolio_data"] = lps_with_data
        result["fos_with_portfolio_data"] = fos_with_data

        total_investors = lp_count + fo_count
        investors_with_data = lps_with_data + fos_with_data
        result["coverage_percentage"] = (
            (investors_with_data / total_investors * 100)
            if total_investors > 0
            else 0.0
        )

        # Portfolio totals (market_value_usd is VARCHAR, need to cast)
        portfolio_stats = self.db.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT company_name) as unique_companies,
                    SUM(NULLIF(market_value_usd, '')::NUMERIC) as total_value
                FROM portfolio_companies
            """)
        ).fetchone()

        result["total_portfolio_companies"] = portfolio_stats[0] or 0
        result["unique_companies"] = portfolio_stats[1] or 0
        result["total_market_value_usd"] = (
            float(portfolio_stats[2]) if portfolio_stats[2] else None
        )

        # Source breakdown
        source_rows = self.db.execute(
            text("""
                SELECT source_type, COUNT(*) as cnt
                FROM portfolio_companies
                GROUP BY source_type
            """)
        ).fetchall()
        result["companies_by_source"] = {row[0]: row[1] for row in source_rows}

        # Collection stats
        result["collection_stats"] = await self._get_collection_stats()

        # Alert stats
        result["alert_stats"] = await self._get_alert_stats()

        # Data freshness
        freshness = self.db.execute(
            text("""
                SELECT
                    MAX(collected_date) as last_collection,
                    AVG(EXTRACT(DAY FROM NOW() - collected_date)) as avg_age_days
                FROM portfolio_companies
                WHERE collected_date IS NOT NULL
            """)
        ).fetchone()

        result["last_collection_at"] = (
            freshness[0].isoformat() if freshness[0] else None
        )
        result["avg_data_age_days"] = float(freshness[1]) if freshness[1] else 0.0

        return result

    async def _get_collection_stats(self) -> dict:
        """Get collection job statistics."""
        stats_24h = (
            self.db.execute(
                text("""
                SELECT COUNT(*) FROM agentic_collection_jobs
                WHERE created_at > NOW() - INTERVAL '1 day'
            """)
            ).scalar()
            or 0
        )

        stats_7d = self.db.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'success') as successful,
                    AVG(companies_found) as avg_companies,
                    SUM(companies_found) as total_companies
                FROM agentic_collection_jobs
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
        ).fetchone()

        stats_30d = (
            self.db.execute(
                text("""
                SELECT COUNT(*) FROM agentic_collection_jobs
                WHERE created_at > NOW() - INTERVAL '30 days'
            """)
            ).scalar()
            or 0
        )

        total_7d = stats_7d[0] or 0
        successful_7d = stats_7d[1] or 0
        success_rate = (successful_7d / total_7d * 100) if total_7d > 0 else 0.0

        return {
            "jobs_last_24h": stats_24h,
            "jobs_last_7d": total_7d,
            "jobs_last_30d": stats_30d,
            "success_rate_7d": round(success_rate, 1),
            "avg_companies_per_job": round(float(stats_7d[2] or 0), 1),
            "total_companies_collected_7d": stats_7d[3] or 0,
        }

    async def _get_alert_stats(self) -> dict:
        """Get alert statistics."""
        pending = (
            self.db.execute(
                text("""
                SELECT COUNT(*) FROM portfolio_alerts
                WHERE status = 'pending'
            """)
            ).scalar()
            or 0
        )

        today = (
            self.db.execute(
                text("""
                SELECT COUNT(*) FROM portfolio_alerts
                WHERE created_at > NOW() - INTERVAL '1 day'
            """)
            ).scalar()
            or 0
        )

        week = (
            self.db.execute(
                text("""
                SELECT COUNT(*) FROM portfolio_alerts
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            ).scalar()
            or 0
        )

        active_subs = (
            self.db.execute(
                text("""
                SELECT COUNT(*) FROM alert_subscriptions
                WHERE is_active = TRUE
            """)
            ).scalar()
            or 0
        )

        return {
            "pending_alerts": pending,
            "alerts_triggered_today": today,
            "alerts_triggered_7d": week,
            "active_subscriptions": active_subs,
        }

    async def get_investor_analytics(
        self, investor_id: int, investor_type: str
    ) -> dict:
        """
        Compute analytics for a single investor's portfolio.

        Returns portfolio summary, industry distribution, top holdings,
        data quality score, and collection history.
        """
        # Get investor name
        if investor_type == "lp":
            name_row = self.db.execute(
                text("SELECT name FROM lp_fund WHERE id = :id"), {"id": investor_id}
            ).fetchone()
        else:
            name_row = self.db.execute(
                text("SELECT name FROM family_offices WHERE id = :id"),
                {"id": investor_id},
            ).fetchone()

        investor_name = name_row[0] if name_row else "Unknown"

        result = {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "investor_name": investor_name,
        }

        # Portfolio summary (market_value_usd is VARCHAR, need to cast)
        summary = self.db.execute(
            text("""
                SELECT
                    COUNT(*) as total_companies,
                    SUM(NULLIF(market_value_usd, '')::NUMERIC) as total_value,
                    array_agg(DISTINCT source_type) as sources,
                    MAX(collected_date) as last_updated
                FROM portfolio_companies
                WHERE investor_id = :id AND investor_type = :type
            """),
            {"id": investor_id, "type": investor_type},
        ).fetchone()

        last_updated = summary[3]
        data_age_days = (datetime.now() - last_updated).days if last_updated else 999

        result["portfolio_summary"] = {
            "total_companies": summary[0] or 0,
            "total_market_value_usd": float(summary[1]) if summary[1] else None,
            "sources_used": [s for s in (summary[2] or []) if s],
            "last_updated": last_updated.isoformat() if last_updated else None,
            "data_age_days": data_age_days,
        }

        # Industry distribution (market_value_usd is VARCHAR, need to cast)
        industries = self.db.execute(
            text("""
                SELECT
                    COALESCE(company_industry, 'Unknown') as industry,
                    COUNT(*) as count,
                    SUM(NULLIF(market_value_usd, '')::NUMERIC) as value
                FROM portfolio_companies
                WHERE investor_id = :id AND investor_type = :type
                GROUP BY company_industry
                ORDER BY count DESC
            """),
            {"id": investor_id, "type": investor_type},
        ).fetchall()

        total_companies = result["portfolio_summary"]["total_companies"]
        result["industry_distribution"] = [
            {
                "industry": row[0],
                "company_count": row[1],
                "percentage": round(row[1] / total_companies * 100, 1)
                if total_companies > 0
                else 0,
                "total_value_usd": float(row[2]) if row[2] else None,
            }
            for row in industries
        ]

        # Top holdings (market_value_usd, shares_held, confidence_level are VARCHAR)
        holdings = self.db.execute(
            text("""
                SELECT company_name, company_industry, market_value_usd,
                       shares_held, source_type, confidence_level
                FROM portfolio_companies
                WHERE investor_id = :id AND investor_type = :type
                ORDER BY NULLIF(market_value_usd, '')::NUMERIC DESC NULLS LAST
                LIMIT 10
            """),
            {"id": investor_id, "type": investor_type},
        ).fetchall()

        result["top_holdings"] = [
            {
                "company_name": row[0],
                "industry": row[1],
                "market_value_usd": float(row[2])
                if row[2] and row[2].strip()
                else None,
                "shares_held": int(row[3])
                if row[3] and row[3].strip().isdigit()
                else None,
                "source_type": row[4],
                "confidence_level": self._parse_confidence(row[5]),
            }
            for row in holdings
        ]

        # Data quality score (confidence_level is VARCHAR text like 'high'/'medium'/'low')
        quality_metrics = self.db.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(company_industry) as has_industry,
                    COUNT(NULLIF(market_value_usd, '')) as has_value,
                    AVG(CASE
                        WHEN confidence_level = 'high' THEN 0.9
                        WHEN confidence_level = 'medium' THEN 0.7
                        WHEN confidence_level = 'low' THEN 0.5
                        ELSE NULL
                    END) as avg_confidence,
                    COUNT(DISTINCT source_type) as source_count
                FROM portfolio_companies
                WHERE investor_id = :id AND investor_type = :type
            """),
            {"id": investor_id, "type": investor_type},
        ).fetchone()

        result["data_quality"] = self._calculate_data_quality_score(
            total=quality_metrics[0] or 0,
            has_industry=quality_metrics[1] or 0,
            has_value=quality_metrics[2] or 0,
            avg_confidence=float(quality_metrics[3]) if quality_metrics[3] else 0.0,
            source_count=quality_metrics[4] or 0,
            data_age_days=data_age_days,
        )

        # Collection history
        history = self.db.execute(
            text("""
                SELECT id, created_at, status, companies_found, strategies_used
                FROM agentic_collection_jobs
                WHERE target_investor_id = :id AND target_investor_type = :type
                ORDER BY created_at DESC
                LIMIT 10
            """),
            {"id": investor_id, "type": investor_type},
        ).fetchall()

        result["collection_history"] = [
            {
                "job_id": row[0],
                "date": row[1].isoformat() if row[1] else None,
                "status": row[2],
                "companies_found": row[3] or 0,
                "strategies_used": row[4] or [],
            }
            for row in history
        ]

        return result

    def _parse_confidence(self, value: str) -> Optional[float]:
        """Convert confidence level text to numeric value."""
        if not value:
            return None
        value = value.lower().strip()
        confidence_map = {"high": 0.9, "medium": 0.7, "low": 0.5}
        if value in confidence_map:
            return confidence_map[value]
        # Try parsing as numeric
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _calculate_data_quality_score(
        self,
        total: int,
        has_industry: int,
        has_value: int,
        avg_confidence: float,
        source_count: int,
        data_age_days: int,
    ) -> dict:
        """
        Calculate data quality score (0-100).

        Components:
        1. Completeness (40 points max)
        2. Freshness (25 points max)
        3. Source Diversity (20 points max)
        4. Confidence (15 points max)
        """
        score = 0
        issues = []

        if total == 0:
            return {
                "overall_score": 0,
                "completeness": 0,
                "freshness": 0,
                "source_diversity": 0,
                "confidence_avg": 0.0,
                "issues": ["No portfolio data available"],
            }

        # Completeness (40 points)
        completeness_score = 0
        industry_pct = (has_industry / total) * 100

        if industry_pct >= 80:
            completeness_score += 15
        elif industry_pct >= 50:
            completeness_score += 10
        else:
            issues.append(
                f"Missing industry for {100 - industry_pct:.0f}% of companies"
            )

        value_pct = (has_value / total) * 100
        if value_pct >= 50:
            completeness_score += 15
        elif value_pct >= 25:
            completeness_score += 10
        else:
            issues.append(
                f"Missing market value for {100 - value_pct:.0f}% of companies"
            )

        # Confidence component of completeness
        if avg_confidence >= 0.7:
            completeness_score += 10
        elif avg_confidence >= 0.5:
            completeness_score += 5

        score += completeness_score

        # Freshness (25 points)
        freshness_score = 0
        if data_age_days < 7:
            freshness_score = 25
        elif data_age_days < 30:
            freshness_score = 15
        elif data_age_days < 90:
            freshness_score = 5
        else:
            issues.append(f"Data is {data_age_days} days old")

        score += freshness_score

        # Source Diversity (20 points)
        diversity_score = 0
        if source_count >= 3:
            diversity_score = 20
        elif source_count == 2:
            diversity_score = 12
        elif source_count == 1:
            diversity_score = 5
        else:
            issues.append("No data sources recorded")

        score += diversity_score

        # Confidence (15 points)
        confidence_score = int(avg_confidence * 15)
        score += confidence_score

        return {
            "overall_score": min(score, 100),
            "completeness": completeness_score,
            "freshness": freshness_score,
            "source_diversity": diversity_score,
            "confidence_avg": round(avg_confidence, 2),
            "issues": issues,
        }

    async def get_trends(
        self, period: str = "30d", metric: str = "collections"
    ) -> dict:
        """
        Compute time-series trend data for charts.

        Args:
            period: "7d", "30d", or "90d"
            metric: "collections", "companies", or "alerts"
        """
        # Parse period
        days = {"7d": 7, "30d": 30, "90d": 90}.get(period, 30)

        if metric == "collections":
            data = await self._get_collection_trends(days)
        elif metric == "companies":
            data = await self._get_company_trends(days)
        elif metric == "alerts":
            data = await self._get_alert_trends(days)
        else:
            data = []

        # Calculate summary
        values = [d["value"] for d in data]
        if values:
            total = sum(values)
            average = total / len(values)
            min_val = min(values)
            max_val = max(values)

            # Trend direction: compare first half to second half
            mid = len(values) // 2
            first_half_avg = sum(values[:mid]) / mid if mid > 0 else 0
            second_half_avg = (
                sum(values[mid:]) / (len(values) - mid) if len(values) > mid else 0
            )

            if second_half_avg > first_half_avg * 1.1:
                trend_direction = "up"
            elif second_half_avg < first_half_avg * 0.9:
                trend_direction = "down"
            else:
                trend_direction = "stable"

            change_pct = (
                ((second_half_avg - first_half_avg) / first_half_avg * 100)
                if first_half_avg > 0
                else 0.0
            )
        else:
            total = 0
            average = 0.0
            min_val = 0
            max_val = 0
            trend_direction = "stable"
            change_pct = 0.0

        return {
            "period": period,
            "metric": metric,
            "data_points": data,
            "summary": {
                "total": total,
                "average": round(average, 1),
                "min": min_val,
                "max": max_val,
                "trend_direction": trend_direction,
                "change_percentage": round(change_pct, 1),
            },
        }

    async def _get_collection_trends(self, days: int) -> List[dict]:
        """Get collection jobs per day."""
        rows = self.db.execute(
            text("""
                SELECT
                    DATE(created_at) as date,
                    COUNT(*) as job_count,
                    COUNT(*) FILTER (WHERE status = 'success') as successful,
                    SUM(companies_found) as companies_found
                FROM agentic_collection_jobs
                WHERE created_at > NOW() - INTERVAL :days
                GROUP BY DATE(created_at)
                ORDER BY date
            """),
            {"days": f"{days} days"},
        ).fetchall()

        return [
            {
                "date": row[0].isoformat() if row[0] else None,
                "value": row[1],
                "details": {
                    "successful": row[2],
                    "companies_found": row[3] or 0,
                },
            }
            for row in rows
        ]

    async def _get_company_trends(self, days: int) -> List[dict]:
        """Get new portfolio companies per day."""
        rows = self.db.execute(
            text("""
                SELECT
                    DATE(collected_date) as date,
                    COUNT(*) as new_companies
                FROM portfolio_companies
                WHERE collected_date > NOW() - INTERVAL :days
                GROUP BY DATE(collected_date)
                ORDER BY date
            """),
            {"days": f"{days} days"},
        ).fetchall()

        return [
            {
                "date": row[0].isoformat() if row[0] else None,
                "value": row[1],
                "details": None,
            }
            for row in rows
        ]

    async def _get_alert_trends(self, days: int) -> List[dict]:
        """Get alerts triggered per day."""
        rows = self.db.execute(
            text("""
                SELECT
                    DATE(created_at) as date,
                    COUNT(*) as alert_count
                FROM portfolio_alerts
                WHERE created_at > NOW() - INTERVAL :days
                GROUP BY DATE(created_at)
                ORDER BY date
            """),
            {"days": f"{days} days"},
        ).fetchall()

        return [
            {
                "date": row[0].isoformat() if row[0] else None,
                "value": row[1],
                "details": None,
            }
            for row in rows
        ]

    async def get_top_movers(
        self, limit: int = 20, change_type: Optional[str] = None
    ) -> dict:
        """
        Get recent significant portfolio changes for activity feed.

        Returns recent alerts/changes sorted by recency.
        """
        # Try to get from alerts table first (T11)
        query = """
            SELECT
                a.investor_id, a.investor_type, a.investor_name,
                a.change_type, a.company_name, a.details, a.created_at
            FROM portfolio_alerts a
            WHERE a.created_at > NOW() - INTERVAL '7 days'
        """
        params = {"limit": limit}

        if change_type:
            query += " AND a.change_type = :change_type"
            params["change_type"] = change_type

        query += " ORDER BY a.created_at DESC LIMIT :limit"

        rows = self.db.execute(text(query), params).fetchall()

        movers = [
            {
                "investor_id": row[0],
                "investor_type": row[1],
                "investor_name": row[2],
                "change_type": row[3],
                "company_name": row[4],
                "details": row[5] or {},
                "detected_at": row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]

        # If no alerts, fall back to recent portfolio additions
        if not movers:
            fallback_rows = self.db.execute(
                text("""
                    SELECT
                        pc.investor_id, pc.investor_type,
                        CASE WHEN pc.investor_type = 'lp'
                             THEN (SELECT name FROM lp_fund WHERE id = pc.investor_id)
                             ELSE (SELECT name FROM family_offices WHERE id = pc.investor_id)
                        END as investor_name,
                        'new_holding' as change_type,
                        pc.company_name,
                        json_build_object(
                            'market_value_usd', pc.market_value_usd,
                            'source', pc.source_type
                        ) as details,
                        pc.collected_date
                    FROM portfolio_companies pc
                    WHERE pc.collected_date > NOW() - INTERVAL '7 days'
                    ORDER BY pc.collected_date DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            ).fetchall()

            movers = [
                {
                    "investor_id": row[0],
                    "investor_type": row[1],
                    "investor_name": row[2],
                    "change_type": row[3],
                    "company_name": row[4],
                    "details": row[5] or {},
                    "detected_at": row[6].isoformat() if row[6] else None,
                }
                for row in fallback_rows
            ]

        return {
            "movers": movers,
            "generated_at": datetime.now().isoformat(),
        }

    async def get_industry_breakdown(
        self, investor_type: Optional[str] = None, limit: int = 20
    ) -> dict:
        """
        Compute aggregate industry distribution across all portfolios.

        Args:
            investor_type: Optional filter by "lp" or "family_office"
            limit: Top N industries to return
        """
        # Build query (market_value_usd is VARCHAR, need to cast)
        query = """
            SELECT
                COALESCE(company_industry, 'Unknown') as industry,
                COUNT(*) as company_count,
                COUNT(DISTINCT investor_id || '-' || investor_type) as investor_count,
                SUM(NULLIF(market_value_usd, '')::NUMERIC) as total_value
            FROM portfolio_companies
        """
        params = {"limit": limit}

        if investor_type:
            query += " WHERE investor_type = :investor_type"
            params["investor_type"] = investor_type

        query += """
            GROUP BY company_industry
            ORDER BY company_count DESC
            LIMIT :limit
        """

        rows = self.db.execute(text(query), params).fetchall()

        # Get total count
        total_query = "SELECT COUNT(*) FROM portfolio_companies"
        if investor_type:
            total_query += " WHERE investor_type = :investor_type"
        total_companies = (
            self.db.execute(
                text(total_query),
                {"investor_type": investor_type} if investor_type else {},
            ).scalar()
            or 0
        )

        # Calculate "other" count
        top_n_count = sum(row[1] for row in rows)
        other_count = max(0, total_companies - top_n_count)

        # Get top companies per industry (separate query for each)
        industries = []
        for row in rows:
            industry_name = row[0]

            # Get top 3 companies for this industry
            top_query = """
                SELECT DISTINCT company_name
                FROM portfolio_companies
                WHERE COALESCE(company_industry, 'Unknown') = :industry
            """
            if investor_type:
                top_query += " AND investor_type = :investor_type"
            top_query += " LIMIT 3"

            top_params = {"industry": industry_name}
            if investor_type:
                top_params["investor_type"] = investor_type

            top_companies = self.db.execute(text(top_query), top_params).fetchall()

            industries.append(
                {
                    "industry": industry_name,
                    "company_count": row[1],
                    "percentage": round(row[1] / total_companies * 100, 1)
                    if total_companies > 0
                    else 0,
                    "investor_count": row[2],
                    "total_value_usd": float(row[3]) if row[3] else None,
                    "top_companies": [c[0] for c in top_companies],
                }
            )

        return {
            "total_companies": total_companies,
            "industries": industries,
            "other_count": other_count,
        }


def get_dashboard_analytics(db: Session) -> DashboardAnalytics:
    """Factory function to get DashboardAnalytics instance."""
    return DashboardAnalytics(db)
