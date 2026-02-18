"""
Leadership Change Monitor.

Monitors for leadership changes and generates alerts:
- Detects changes from collection jobs
- Tracks watchlist alerts
- Generates portfolio-level alerts
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.people_models import (
    LeadershipChange,
    PeopleWatchlist,
    PeopleWatchlistPerson,
    PeoplePortfolio,
    PeoplePortfolioCompany,
    IndustrialCompany,
    Person,
)

logger = logging.getLogger(__name__)


class ChangeMonitor:
    """
    Monitors leadership changes and generates alerts.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_recent_changes(
        self,
        days: int = 1,
        company_ids: Optional[List[int]] = None,
        change_types: Optional[List[str]] = None,
        c_suite_only: bool = False,
    ) -> List[LeadershipChange]:
        """
        Get recent leadership changes.

        Args:
            days: Number of days to look back
            company_ids: Filter to specific companies
            change_types: Filter by change type (hire, departure, etc.)
            c_suite_only: Only C-suite changes
        """
        cutoff = date.today() - timedelta(days=days)

        query = self.db.query(LeadershipChange).filter(
            LeadershipChange.detected_date >= cutoff
        )

        if company_ids:
            query = query.filter(LeadershipChange.company_id.in_(company_ids))

        if change_types:
            query = query.filter(LeadershipChange.change_type.in_(change_types))

        if c_suite_only:
            query = query.filter(LeadershipChange.is_c_suite == True)

        return query.order_by(LeadershipChange.detected_date.desc()).all()

    def get_watchlist_alerts(
        self,
        watchlist_id: int,
        days: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        Get alerts for people on a watchlist.

        Returns changes for any person on the watchlist.
        """
        # Get person IDs from watchlist
        watchlist_people = self.db.query(PeopleWatchlistPerson).filter(
            PeopleWatchlistPerson.watchlist_id == watchlist_id
        ).all()

        person_ids = [wp.person_id for wp in watchlist_people]
        if not person_ids:
            return []

        # Get changes for these people
        cutoff = date.today() - timedelta(days=days)
        changes = self.db.query(LeadershipChange).filter(
            LeadershipChange.person_id.in_(person_ids),
            LeadershipChange.detected_date >= cutoff,
        ).order_by(LeadershipChange.detected_date.desc()).all()

        alerts = []
        for change in changes:
            company = self.db.get(IndustrialCompany, change.company_id)
            alerts.append({
                "change_id": change.id,
                "person_id": change.person_id,
                "person_name": change.person_name,
                "company_id": change.company_id,
                "company_name": company.name if company else "Unknown",
                "change_type": change.change_type,
                "old_title": change.old_title,
                "new_title": change.new_title,
                "announced_date": change.announced_date.isoformat() if change.announced_date else None,
                "detected_date": change.detected_date.isoformat() if change.detected_date else None,
                "is_c_suite": change.is_c_suite,
                "significance_score": change.significance_score,
            })

        return alerts

    def get_portfolio_alerts(
        self,
        portfolio_id: int,
        days: int = 7,
        c_suite_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get alerts for all companies in a portfolio.
        """
        # Get company IDs from portfolio
        portfolio_companies = self.db.query(PeoplePortfolioCompany).filter(
            PeoplePortfolioCompany.portfolio_id == portfolio_id,
            PeoplePortfolioCompany.is_active == True,
        ).all()

        company_ids = [pc.company_id for pc in portfolio_companies]
        if not company_ids:
            return []

        changes = self.get_recent_changes(
            days=days,
            company_ids=company_ids,
            c_suite_only=c_suite_only,
        )

        alerts = []
        for change in changes:
            company = self.db.get(IndustrialCompany, change.company_id)
            alerts.append({
                "change_id": change.id,
                "person_name": change.person_name,
                "company_id": change.company_id,
                "company_name": company.name if company else "Unknown",
                "change_type": change.change_type,
                "old_title": change.old_title,
                "new_title": change.new_title,
                "announced_date": change.announced_date.isoformat() if change.announced_date else None,
                "is_c_suite": change.is_c_suite,
                "is_board": change.is_board,
                "significance_score": change.significance_score,
            })

        return alerts

    def get_industry_alerts(
        self,
        industry: str,
        days: int = 7,
        c_suite_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get alerts for all companies in an industry.
        """
        # Get company IDs for industry
        companies = self.db.query(IndustrialCompany).filter(
            IndustrialCompany.industry_segment == industry
        ).all()

        company_ids = [c.id for c in companies]
        company_names = {c.id: c.name for c in companies}

        if not company_ids:
            return []

        changes = self.get_recent_changes(
            days=days,
            company_ids=company_ids,
            c_suite_only=c_suite_only,
        )

        alerts = []
        for change in changes:
            alerts.append({
                "change_id": change.id,
                "person_name": change.person_name,
                "company_id": change.company_id,
                "company_name": company_names.get(change.company_id, "Unknown"),
                "change_type": change.change_type,
                "old_title": change.old_title,
                "new_title": change.new_title,
                "announced_date": change.announced_date.isoformat() if change.announced_date else None,
                "is_c_suite": change.is_c_suite,
                "significance_score": change.significance_score,
            })

        return alerts

    def get_change_summary(
        self,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get summary of all changes in period.
        """
        cutoff = date.today() - timedelta(days=days)

        changes = self.db.query(LeadershipChange).filter(
            LeadershipChange.detected_date >= cutoff
        ).all()

        summary = {
            "period_days": days,
            "total_changes": len(changes),
            "by_type": {},
            "c_suite_changes": 0,
            "board_changes": 0,
            "high_significance": 0,
            "companies_affected": set(),
        }

        for change in changes:
            # By type
            summary["by_type"][change.change_type] = summary["by_type"].get(change.change_type, 0) + 1

            # C-suite and board
            if change.is_c_suite:
                summary["c_suite_changes"] += 1
            if change.is_board:
                summary["board_changes"] += 1

            # High significance
            if change.significance_score and change.significance_score >= 7:
                summary["high_significance"] += 1

            summary["companies_affected"].add(change.company_id)

        summary["companies_affected"] = len(summary["companies_affected"])

        return summary


class AlertDigestGenerator:
    """
    Generates digest reports of leadership changes.
    """

    def __init__(self, db: Session):
        self.db = db
        self.monitor = ChangeMonitor(db)

    def generate_weekly_digest(
        self,
        portfolio_id: Optional[int] = None,
        industry: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a weekly digest of leadership changes.

        Can be filtered to a specific portfolio or industry.
        """
        days = 7

        digest = {
            "generated_at": datetime.utcnow().isoformat(),
            "period": f"Last {days} days",
            "filter": None,
            "summary": None,
            "highlights": [],
            "all_changes": [],
        }

        if portfolio_id:
            digest["filter"] = {"type": "portfolio", "id": portfolio_id}
            portfolio = self.db.get(PeoplePortfolio, portfolio_id)
            if portfolio:
                digest["filter"]["name"] = portfolio.name
            alerts = self.monitor.get_portfolio_alerts(portfolio_id, days=days)
        elif industry:
            digest["filter"] = {"type": "industry", "name": industry}
            alerts = self.monitor.get_industry_alerts(industry, days=days)
        else:
            # All changes
            changes = self.monitor.get_recent_changes(days=days)
            alerts = []
            for change in changes:
                company = self.db.get(IndustrialCompany, change.company_id)
                alerts.append({
                    "change_id": change.id,
                    "person_name": change.person_name,
                    "company_id": change.company_id,
                    "company_name": company.name if company else "Unknown",
                    "change_type": change.change_type,
                    "old_title": change.old_title,
                    "new_title": change.new_title,
                    "is_c_suite": change.is_c_suite,
                    "significance_score": change.significance_score,
                })

        # Summary stats
        summary = self.monitor.get_change_summary(days=days)
        digest["summary"] = summary

        # Highlights (high significance changes)
        digest["highlights"] = [
            a for a in alerts
            if a.get("significance_score", 0) >= 7 or a.get("is_c_suite")
        ][:10]

        # All changes
        digest["all_changes"] = alerts

        return digest

    def generate_watchlist_digest(
        self,
        watchlist_id: int,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Generate digest for a specific watchlist.
        """
        watchlist = self.db.get(PeopleWatchlist, watchlist_id)
        if not watchlist:
            return {"error": "Watchlist not found"}

        alerts = self.monitor.get_watchlist_alerts(watchlist_id, days=days)

        # Group by person
        by_person = {}
        for alert in alerts:
            person_id = alert["person_id"]
            if person_id not in by_person:
                by_person[person_id] = {
                    "person_id": person_id,
                    "person_name": alert["person_name"],
                    "changes": [],
                }
            by_person[person_id]["changes"].append(alert)

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "watchlist_id": watchlist_id,
            "watchlist_name": watchlist.name,
            "period_days": days,
            "total_alerts": len(alerts),
            "people_with_changes": len(by_person),
            "by_person": list(by_person.values()),
            "all_alerts": alerts,
        }
