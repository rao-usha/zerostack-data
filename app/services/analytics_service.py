"""
Analytics Service for People Intelligence Platform.

Provides industry-wide analytics, trend analysis, and aggregation functions.
"""

from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, case, distinct

from app.core.people_models import (
    Person,
    CompanyPerson,
    IndustrialCompany,
    LeadershipChange,
    PeoplePortfolio,
    PeoplePortfolioCompany,
)


class AnalyticsService:
    """Service for computing analytics and trends."""

    def __init__(self, db: Session):
        self.db = db

    def get_industry_stats(
        self,
        industry: Optional[str] = None,
        days: int = 90,
    ) -> Dict[str, Any]:
        """
        Get comprehensive stats for an industry or all industries.

        Returns executive counts, change stats, tenure averages, etc.
        """
        # Base company query
        company_query = self.db.query(IndustrialCompany.id)
        if industry:
            company_query = company_query.filter(
                IndustrialCompany.industry_segment == industry
            )
        company_ids = [c[0] for c in company_query.all()]

        if not company_ids:
            return self._empty_stats(industry, days)

        # Total companies
        total_companies = len(company_ids)

        # Total executives
        total_executives = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id.in_(company_ids),
                CompanyPerson.is_current == True,
            )
            .count()
        )

        # C-suite count
        c_suite_count = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id.in_(company_ids),
                CompanyPerson.is_current == True,
                CompanyPerson.title_level == "c_suite",
            )
            .count()
        )

        # Board members
        board_count = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id.in_(company_ids),
                CompanyPerson.is_current == True,
                CompanyPerson.is_board_member == True,
            )
            .count()
        )

        # Changes in period
        cutoff_date = date.today() - timedelta(days=days)
        changes = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id.in_(company_ids),
                or_(
                    LeadershipChange.announced_date >= cutoff_date,
                    LeadershipChange.effective_date >= cutoff_date,
                ),
            )
            .all()
        )

        # Changes by type
        changes_by_type = defaultdict(int)
        c_suite_changes = 0
        board_changes = 0
        for change in changes:
            changes_by_type[change.change_type] += 1
            if change.is_c_suite:
                c_suite_changes += 1
            if change.is_board:
                board_changes += 1

        # Tenure calculations for C-suite
        tenure_data = self._calculate_tenure_stats(company_ids)

        # Companies with most changes (instability flags)
        company_change_counts = defaultdict(int)
        for change in changes:
            if change.is_c_suite:
                company_change_counts[change.company_id] += 1

        instability_flags = []
        for company_id, count in company_change_counts.items():
            if count >= 3:
                company = self.db.get(IndustrialCompany, company_id)
                if company:
                    instability_flags.append(
                        {
                            "company_id": company_id,
                            "company_name": company.name,
                            "c_suite_changes": count,
                            "flag": "high_turnover",
                        }
                    )

        return {
            "industry": industry or "all",
            "period_days": days,
            "total_companies": total_companies,
            "total_executives": total_executives,
            "c_suite_count": c_suite_count,
            "board_members": board_count,
            "changes_in_period": len(changes),
            "changes_by_type": dict(changes_by_type),
            "c_suite_changes": c_suite_changes,
            "board_changes": board_changes,
            "avg_ceo_tenure_months": tenure_data.get("ceo_avg", None),
            "avg_cfo_tenure_months": tenure_data.get("cfo_avg", None),
            "avg_c_suite_tenure_months": tenure_data.get("c_suite_avg", None),
            "instability_flags": instability_flags,
        }

    def _empty_stats(self, industry: Optional[str], days: int) -> Dict[str, Any]:
        """Return empty stats structure."""
        return {
            "industry": industry or "all",
            "period_days": days,
            "total_companies": 0,
            "total_executives": 0,
            "c_suite_count": 0,
            "board_members": 0,
            "changes_in_period": 0,
            "changes_by_type": {},
            "c_suite_changes": 0,
            "board_changes": 0,
            "avg_ceo_tenure_months": None,
            "avg_cfo_tenure_months": None,
            "avg_c_suite_tenure_months": None,
            "instability_flags": [],
        }

    def _calculate_tenure_stats(self, company_ids: List[int]) -> Dict[str, float]:
        """Calculate average tenure for C-suite roles."""
        # Get current C-suite with start dates
        c_suite = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id.in_(company_ids),
                CompanyPerson.is_current == True,
                CompanyPerson.title_level == "c_suite",
                CompanyPerson.start_date.isnot(None),
            )
            .all()
        )

        ceo_tenures = []
        cfo_tenures = []
        all_tenures = []

        today = date.today()
        for cp in c_suite:
            if cp.start_date:
                months = (today - cp.start_date).days / 30.44
                all_tenures.append(months)

                title_lower = (cp.title_normalized or cp.title or "").lower()
                if "ceo" in title_lower or "chief executive" in title_lower:
                    ceo_tenures.append(months)
                elif "cfo" in title_lower or "chief financial" in title_lower:
                    cfo_tenures.append(months)

        return {
            "ceo_avg": round(sum(ceo_tenures) / len(ceo_tenures), 1)
            if ceo_tenures
            else None,
            "cfo_avg": round(sum(cfo_tenures) / len(cfo_tenures), 1)
            if cfo_tenures
            else None,
            "c_suite_avg": round(sum(all_tenures) / len(all_tenures), 1)
            if all_tenures
            else None,
        }

    def get_talent_flow(
        self,
        industry: Optional[str] = None,
        days: int = 90,
    ) -> Dict[str, Any]:
        """
        Analyze talent flow - which companies are gaining/losing executives.

        Returns net importers and exporters of executive talent.
        """
        # Get company IDs for industry
        company_query = self.db.query(IndustrialCompany)
        if industry:
            company_query = company_query.filter(
                IndustrialCompany.industry_segment == industry
            )
        companies = {c.id: c.name for c in company_query.all()}
        company_ids = list(companies.keys())

        if not company_ids:
            return {
                "industry": industry or "all",
                "period_days": days,
                "net_importers": [],
                "net_exporters": [],
                "stable": [],
            }

        # Get changes in period
        cutoff_date = date.today() - timedelta(days=days)
        changes = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id.in_(company_ids),
                or_(
                    LeadershipChange.announced_date >= cutoff_date,
                    LeadershipChange.effective_date >= cutoff_date,
                ),
            )
            .all()
        )

        # Count hires and departures per company
        company_flow = defaultdict(lambda: {"hires": 0, "departures": 0})
        for change in changes:
            if change.change_type in ["hire", "board_appointment"]:
                company_flow[change.company_id]["hires"] += 1
            elif change.change_type in ["departure", "retirement", "board_departure"]:
                company_flow[change.company_id]["departures"] += 1

        # Categorize companies
        net_importers = []
        net_exporters = []
        stable = []

        for company_id, flow in company_flow.items():
            net = flow["hires"] - flow["departures"]
            entry = {
                "company_id": company_id,
                "company_name": companies.get(company_id, "Unknown"),
                "hires": flow["hires"],
                "departures": flow["departures"],
                "net": net,
            }
            if net > 0:
                net_importers.append(entry)
            elif net < 0:
                net_exporters.append(entry)
            else:
                stable.append(entry)

        # Sort by net flow
        net_importers.sort(key=lambda x: x["net"], reverse=True)
        net_exporters.sort(key=lambda x: x["net"])

        return {
            "industry": industry or "all",
            "period_days": days,
            "net_importers": net_importers[:10],
            "net_exporters": net_exporters[:10],
            "stable": stable[:10],
        }

    def get_change_trends(
        self,
        industry: Optional[str] = None,
        months: int = 12,
    ) -> Dict[str, Any]:
        """
        Get monthly change trends over time.

        Returns time series of changes by type.
        """
        # Get company IDs for industry
        company_query = self.db.query(IndustrialCompany.id)
        if industry:
            company_query = company_query.filter(
                IndustrialCompany.industry_segment == industry
            )
        company_ids = [c[0] for c in company_query.all()]

        if not company_ids:
            return {
                "industry": industry or "all",
                "months": months,
                "trends": [],
            }

        # Get changes for the period
        start_date = date.today() - timedelta(days=months * 30)
        changes = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id.in_(company_ids),
                LeadershipChange.announced_date >= start_date,
                LeadershipChange.announced_date.isnot(None),
            )
            .all()
        )

        # Group by month
        monthly_data = defaultdict(lambda: defaultdict(int))
        for change in changes:
            if change.announced_date:
                month_key = change.announced_date.strftime("%Y-%m")
                monthly_data[month_key][change.change_type] += 1
                monthly_data[month_key]["total"] += 1

        # Build trend list
        trends = []
        for month_key in sorted(monthly_data.keys()):
            data = monthly_data[month_key]
            trends.append(
                {
                    "month": month_key,
                    "total": data["total"],
                    "hires": data.get("hire", 0),
                    "departures": data.get("departure", 0),
                    "promotions": data.get("promotion", 0),
                    "retirements": data.get("retirement", 0),
                }
            )

        return {
            "industry": industry or "all",
            "months": months,
            "trends": trends,
        }

    def get_hot_roles(
        self,
        industry: Optional[str] = None,
        days: int = 90,
    ) -> List[Dict[str, Any]]:
        """
        Get most frequently hired roles in the period.

        Identifies roles with highest hiring activity.
        """
        # Get company IDs for industry
        company_query = self.db.query(IndustrialCompany.id)
        if industry:
            company_query = company_query.filter(
                IndustrialCompany.industry_segment == industry
            )
        company_ids = [c[0] for c in company_query.all()]

        if not company_ids:
            return []

        # Get hires in period
        cutoff_date = date.today() - timedelta(days=days)
        hires = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id.in_(company_ids),
                LeadershipChange.change_type == "hire",
                LeadershipChange.announced_date >= cutoff_date,
                LeadershipChange.new_title.isnot(None),
            )
            .all()
        )

        # Count by normalized title
        role_counts = defaultdict(int)
        for hire in hires:
            title = hire.new_title
            # Simple normalization
            title_lower = title.lower()
            if "ceo" in title_lower or "chief executive" in title_lower:
                role_counts["CEO"] += 1
            elif "cfo" in title_lower or "chief financial" in title_lower:
                role_counts["CFO"] += 1
            elif "coo" in title_lower or "chief operating" in title_lower:
                role_counts["COO"] += 1
            elif "cto" in title_lower or "chief technology" in title_lower:
                role_counts["CTO"] += 1
            elif "cmo" in title_lower or "chief marketing" in title_lower:
                role_counts["CMO"] += 1
            elif "chro" in title_lower or "chief human" in title_lower:
                role_counts["CHRO"] += 1
            elif "vp sales" in title_lower or "vice president sales" in title_lower:
                role_counts["VP Sales"] += 1
            elif "vp" in title_lower or "vice president" in title_lower:
                role_counts["VP (Other)"] += 1
            else:
                role_counts["Other Executive"] += 1

        # Sort by count
        hot_roles = [
            {"role": role, "hires": count}
            for role, count in sorted(
                role_counts.items(), key=lambda x: x[1], reverse=True
            )
        ]

        return hot_roles[:10]

    def get_portfolio_analytics(
        self,
        portfolio_id: int,
        days: int = 90,
    ) -> Dict[str, Any]:
        """
        Get analytics for a specific portfolio.
        """
        portfolio = self.db.get(PeoplePortfolio, portfolio_id)
        if not portfolio:
            return {"error": "Portfolio not found"}

        # Get portfolio company IDs
        company_ids = [
            pc.company_id
            for pc in self.db.query(PeoplePortfolioCompany)
            .filter(
                PeoplePortfolioCompany.portfolio_id == portfolio_id,
                PeoplePortfolioCompany.is_active == True,
            )
            .all()
        ]

        if not company_ids:
            return {
                "portfolio_id": portfolio_id,
                "portfolio_name": portfolio.name,
                "total_companies": 0,
                "total_executives": 0,
                "c_suite_count": 0,
                "changes_in_period": 0,
                "changes_by_type": {},
                "period_days": days,
            }

        # Get stats using industry stats method
        self.get_industry_stats(industry=None, days=days)

        # Override with portfolio-specific data
        total_executives = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id.in_(company_ids),
                CompanyPerson.is_current == True,
            )
            .count()
        )

        c_suite_count = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id.in_(company_ids),
                CompanyPerson.is_current == True,
                CompanyPerson.title_level == "c_suite",
            )
            .count()
        )

        # Changes in period
        cutoff_date = date.today() - timedelta(days=days)
        changes = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id.in_(company_ids),
                LeadershipChange.announced_date >= cutoff_date,
            )
            .all()
        )

        changes_by_type = defaultdict(int)
        for change in changes:
            changes_by_type[change.change_type] += 1

        return {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio.name,
            "total_companies": len(company_ids),
            "total_executives": total_executives,
            "c_suite_count": c_suite_count,
            "changes_in_period": len(changes),
            "changes_by_type": dict(changes_by_type),
            "period_days": days,
        }

    def get_company_benchmark_score(
        self,
        company_id: int,
        peer_company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Calculate team strength score for a company vs peers.

        Returns 0-100 score based on team completeness, tenure, and stability.
        """
        company = self.db.get(IndustrialCompany, company_id)
        if not company:
            return {"error": "Company not found"}

        # Get company's current leadership
        leadership = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company_id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        # Check for key roles
        has_ceo = any(
            "ceo" in (cp.title_normalized or cp.title or "").lower()
            or "chief executive" in (cp.title or "").lower()
            for cp in leadership
        )
        has_cfo = any(
            "cfo" in (cp.title_normalized or cp.title or "").lower()
            or "chief financial" in (cp.title or "").lower()
            for cp in leadership
        )
        has_coo = any(
            "coo" in (cp.title_normalized or cp.title or "").lower()
            or "chief operating" in (cp.title or "").lower()
            for cp in leadership
        )

        c_suite_count = sum(1 for cp in leadership if cp.title_level == "c_suite")
        vp_count = sum(1 for cp in leadership if cp.title_level in ["vp", "svp", "evp"])
        board_count = sum(1 for cp in leadership if cp.is_board_member)

        # Calculate tenure
        tenures = []
        today = date.today()
        for cp in leadership:
            if cp.start_date and cp.title_level == "c_suite":
                months = (today - cp.start_date).days / 30.44
                tenures.append(months)

        avg_c_suite_tenure = sum(tenures) / len(tenures) if tenures else 0

        # Calculate score components (each 0-25 points)
        # 1. Team completeness (25 points)
        completeness_score = 0
        if has_ceo:
            completeness_score += 10
        if has_cfo:
            completeness_score += 8
        if has_coo:
            completeness_score += 7

        # 2. Team depth (25 points)
        depth_score = min(25, c_suite_count * 4 + vp_count * 2)

        # 3. Tenure/stability (25 points)
        # Ideal tenure is 24-60 months
        if 24 <= avg_c_suite_tenure <= 60:
            tenure_score = 25
        elif 12 <= avg_c_suite_tenure < 24:
            tenure_score = 15
        elif avg_c_suite_tenure > 60:
            tenure_score = 20  # Long tenure is okay
        else:
            tenure_score = max(0, avg_c_suite_tenure)

        # 4. Board strength (25 points)
        board_score = min(25, board_count * 5)

        total_score = completeness_score + depth_score + tenure_score + board_score

        return {
            "company_id": company_id,
            "company_name": company.name,
            "team_score": round(total_score, 1),
            "components": {
                "completeness": round(completeness_score, 1),
                "depth": round(depth_score, 1),
                "tenure": round(tenure_score, 1),
                "board": round(board_score, 1),
            },
            "details": {
                "has_ceo": has_ceo,
                "has_cfo": has_cfo,
                "has_coo": has_coo,
                "c_suite_count": c_suite_count,
                "vp_count": vp_count,
                "board_count": board_count,
                "avg_c_suite_tenure_months": round(avg_c_suite_tenure, 1)
                if tenures
                else None,
            },
        }
