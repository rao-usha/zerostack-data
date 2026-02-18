"""
Report Service for People Intelligence Platform.

Generates management assessment reports, peer comparisons, and exports.
"""

import io
import json
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.people_models import (
    Person,
    CompanyPerson,
    IndustrialCompany,
    LeadershipChange,
    PersonExperience,
    PersonEducation,
    PeoplePeerSet,
    PeoplePeerSetMember,
)


class ReportService:
    """Service for generating reports and exports."""

    def __init__(self, db: Session):
        self.db = db

    def generate_management_assessment(
        self,
        company_id: int,
        include_bios: bool = True,
        include_experience: bool = True,
        include_education: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate a comprehensive management assessment report for a company.

        Includes team overview, individual profiles, tenure analysis, and gaps.
        """
        company = self.db.get(IndustrialCompany, company_id)
        if not company:
            return {"error": "Company not found"}

        # Get current leadership
        leadership = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company_id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        # Build team structure
        c_suite = []
        vp_level = []
        directors = []
        board = []

        for cp in leadership:
            person = self.db.get(Person, cp.person_id) if cp.person_id else None

            profile = self._build_person_profile(
                cp,
                person,
                include_bios=include_bios,
                include_experience=include_experience,
                include_education=include_education,
            )

            if cp.is_board_member:
                board.append(profile)
            elif cp.title_level == "c_suite":
                c_suite.append(profile)
            elif cp.title_level in ["vp", "svp", "evp"]:
                vp_level.append(profile)
            elif cp.title_level == "director":
                directors.append(profile)

        # Calculate team metrics
        team_metrics = self._calculate_team_metrics(leadership)

        # Get recent changes
        one_year_ago = date.today() - timedelta(days=365)
        recent_changes = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id == company_id,
                LeadershipChange.announced_date >= one_year_ago,
            )
            .order_by(LeadershipChange.announced_date.desc())
            .limit(10)
            .all()
        )

        change_items = [
            {
                "person_name": c.person_name,
                "change_type": c.change_type,
                "old_title": c.old_title,
                "new_title": c.new_title,
                "date": c.announced_date.isoformat() if c.announced_date else None,
            }
            for c in recent_changes
        ]

        # Identify gaps
        gaps = self._identify_leadership_gaps(c_suite)

        return {
            "report_type": "management_assessment",
            "generated_at": datetime.utcnow().isoformat(),
            "company": {
                "id": company.id,
                "name": company.name,
                "industry": company.industry_segment,
                "ownership_type": company.ownership_type,
                "employee_count": company.employee_count,
            },
            "team_summary": {
                "total_executives": len(leadership),
                "c_suite_count": len(c_suite),
                "vp_count": len(vp_level),
                "director_count": len(directors),
                "board_size": len(board),
            },
            "team_metrics": team_metrics,
            "c_suite": c_suite,
            "vp_level": vp_level,
            "directors": directors,
            "board": board,
            "recent_changes": change_items,
            "leadership_gaps": gaps,
        }

    def _build_person_profile(
        self,
        cp: CompanyPerson,
        person: Optional[Person],
        include_bios: bool = True,
        include_experience: bool = True,
        include_education: bool = True,
    ) -> Dict[str, Any]:
        """Build a complete person profile for reports."""
        profile = {
            "name": person.full_name if person else "Unknown",
            "title": cp.title,
            "title_normalized": cp.title_normalized,
            "title_level": cp.title_level,
            "department": cp.department,
            "start_date": cp.start_date.isoformat() if cp.start_date else None,
            "tenure_months": self._calculate_tenure(cp.start_date),
            "is_board_member": cp.is_board_member,
            "is_board_chair": cp.is_board_chair,
        }

        if person:
            profile["linkedin_url"] = person.linkedin_url
            profile["photo_url"] = person.photo_url

            if include_bios and person.bio:
                profile["bio"] = person.bio

            if include_experience:
                experience = (
                    self.db.query(PersonExperience)
                    .filter(PersonExperience.person_id == person.id)
                    .order_by(PersonExperience.start_date.desc().nullslast())
                    .limit(5)
                    .all()
                )

                profile["experience"] = [
                    {
                        "company": exp.company_name,
                        "title": exp.title,
                        "start_date": exp.start_date.isoformat()
                        if exp.start_date
                        else None,
                        "end_date": exp.end_date.isoformat() if exp.end_date else None,
                        "is_current": exp.is_current,
                    }
                    for exp in experience
                ]

            if include_education:
                education = (
                    self.db.query(PersonEducation)
                    .filter(PersonEducation.person_id == person.id)
                    .all()
                )

                profile["education"] = [
                    {
                        "institution": edu.institution,
                        "degree": edu.degree,
                        "field": edu.field_of_study,
                        "year": edu.graduation_year,
                    }
                    for edu in education
                ]

        return profile

    def _calculate_tenure(self, start_date: Optional[date]) -> Optional[int]:
        """Calculate tenure in months from start date."""
        if not start_date:
            return None
        days = (date.today() - start_date).days
        return int(days / 30.44)

    def _calculate_team_metrics(
        self, leadership: List[CompanyPerson]
    ) -> Dict[str, Any]:
        """Calculate team-level metrics."""
        c_suite = [cp for cp in leadership if cp.title_level == "c_suite"]

        tenures = []
        for cp in c_suite:
            tenure = self._calculate_tenure(cp.start_date)
            if tenure is not None:
                tenures.append(tenure)

        avg_tenure = sum(tenures) / len(tenures) if tenures else None
        min_tenure = min(tenures) if tenures else None
        max_tenure = max(tenures) if tenures else None

        # Calculate key role coverage
        titles = [cp.title.lower() if cp.title else "" for cp in c_suite]

        return {
            "avg_c_suite_tenure_months": round(avg_tenure, 1) if avg_tenure else None,
            "min_c_suite_tenure_months": min_tenure,
            "max_c_suite_tenure_months": max_tenure,
            "has_ceo": any("ceo" in t or "chief executive" in t for t in titles),
            "has_cfo": any("cfo" in t or "chief financial" in t for t in titles),
            "has_coo": any("coo" in t or "chief operating" in t for t in titles),
            "has_cto": any("cto" in t or "chief technology" in t for t in titles),
            "has_cmo": any("cmo" in t or "chief marketing" in t for t in titles),
            "has_chro": any(
                "chro" in t or "chief human" in t or "chief people" in t for t in titles
            ),
        }

    def _identify_leadership_gaps(self, c_suite: List[Dict]) -> List[str]:
        """Identify missing key roles."""
        titles = [p.get("title", "").lower() for p in c_suite]
        gaps = []

        key_roles = {
            "CEO": ["ceo", "chief executive"],
            "CFO": ["cfo", "chief financial"],
            "COO": ["coo", "chief operating"],
            "CTO": ["cto", "chief technology"],
            "CMO": ["cmo", "chief marketing"],
            "CHRO": ["chro", "chief human", "chief people"],
        }

        for role, keywords in key_roles.items():
            if not any(any(kw in t for kw in keywords) for t in titles):
                gaps.append(role)

        return gaps

    def generate_peer_comparison(
        self,
        company_id: int,
        peer_set_id: Optional[int] = None,
        peer_company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a peer comparison report.

        Compares a company's leadership against peer companies.
        """
        company = self.db.get(IndustrialCompany, company_id)
        if not company:
            return {"error": "Company not found"}

        # Get peer companies
        if peer_set_id:
            peer_members = (
                self.db.query(PeoplePeerSetMember)
                .filter(PeoplePeerSetMember.peer_set_id == peer_set_id)
                .all()
            )
            peer_company_ids = [m.company_id for m in peer_members]
        elif not peer_company_ids:
            # Auto-select peers from same industry
            peers = (
                self.db.query(IndustrialCompany)
                .filter(
                    IndustrialCompany.industry_segment == company.industry_segment,
                    IndustrialCompany.id != company_id,
                )
                .limit(5)
                .all()
            )
            peer_company_ids = [p.id for p in peers]

        if not peer_company_ids:
            return {
                "report_type": "peer_comparison",
                "company": {"id": company.id, "name": company.name},
                "error": "No peer companies found",
            }

        # Build comparison data
        target_metrics = self._get_company_metrics(company_id)
        peer_metrics = [self._get_company_metrics(pid) for pid in peer_company_ids]

        # Calculate peer averages
        peer_avg = self._calculate_peer_averages(peer_metrics)

        # Compare target vs peers
        comparison = self._compare_to_peers(target_metrics, peer_avg)

        return {
            "report_type": "peer_comparison",
            "generated_at": datetime.utcnow().isoformat(),
            "target_company": {
                "id": company.id,
                "name": company.name,
                "metrics": target_metrics,
            },
            "peer_companies": peer_metrics,
            "peer_averages": peer_avg,
            "comparison": comparison,
        }

    def _get_company_metrics(self, company_id: int) -> Dict[str, Any]:
        """Get leadership metrics for a single company."""
        company = self.db.get(IndustrialCompany, company_id)
        if not company:
            return {"company_id": company_id, "error": "Not found"}

        leadership = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company_id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        c_suite_count = sum(1 for cp in leadership if cp.title_level == "c_suite")
        vp_count = sum(1 for cp in leadership if cp.title_level in ["vp", "svp", "evp"])
        board_count = sum(1 for cp in leadership if cp.is_board_member)

        # Calculate tenures
        tenures = []
        for cp in leadership:
            if cp.title_level == "c_suite" and cp.start_date:
                tenure = (date.today() - cp.start_date).days / 30.44
                tenures.append(tenure)

        avg_tenure = sum(tenures) / len(tenures) if tenures else None

        # Get recent changes
        one_year_ago = date.today() - timedelta(days=365)
        changes_12m = (
            self.db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id == company_id,
                LeadershipChange.announced_date >= one_year_ago,
            )
            .count()
        )

        return {
            "company_id": company_id,
            "company_name": company.name,
            "total_executives": len(leadership),
            "c_suite_count": c_suite_count,
            "vp_count": vp_count,
            "board_size": board_count,
            "avg_c_suite_tenure_months": round(avg_tenure, 1) if avg_tenure else None,
            "changes_12m": changes_12m,
        }

    def _calculate_peer_averages(self, peer_metrics: List[Dict]) -> Dict[str, float]:
        """Calculate average metrics across peers."""
        valid_peers = [p for p in peer_metrics if "error" not in p]
        if not valid_peers:
            return {}

        def safe_avg(key):
            values = [p.get(key) for p in valid_peers if p.get(key) is not None]
            return round(sum(values) / len(values), 1) if values else None

        return {
            "total_executives": safe_avg("total_executives"),
            "c_suite_count": safe_avg("c_suite_count"),
            "vp_count": safe_avg("vp_count"),
            "board_size": safe_avg("board_size"),
            "avg_c_suite_tenure_months": safe_avg("avg_c_suite_tenure_months"),
            "changes_12m": safe_avg("changes_12m"),
        }

    def _compare_to_peers(
        self,
        target: Dict[str, Any],
        peer_avg: Dict[str, float],
    ) -> Dict[str, Any]:
        """Generate comparison insights."""
        insights = []

        # C-suite size comparison
        if target.get("c_suite_count") and peer_avg.get("c_suite_count"):
            diff = target["c_suite_count"] - peer_avg["c_suite_count"]
            if diff > 1:
                insights.append(
                    {
                        "metric": "c_suite_count",
                        "insight": "Larger C-suite than peers",
                        "diff": round(diff, 1),
                    }
                )
            elif diff < -1:
                insights.append(
                    {
                        "metric": "c_suite_count",
                        "insight": "Smaller C-suite than peers",
                        "diff": round(diff, 1),
                    }
                )

        # Tenure comparison
        if target.get("avg_c_suite_tenure_months") and peer_avg.get(
            "avg_c_suite_tenure_months"
        ):
            diff = (
                target["avg_c_suite_tenure_months"]
                - peer_avg["avg_c_suite_tenure_months"]
            )
            if diff > 12:
                insights.append(
                    {
                        "metric": "tenure",
                        "insight": "More experienced (longer tenure) C-suite",
                        "diff_months": round(diff, 1),
                    }
                )
            elif diff < -12:
                insights.append(
                    {
                        "metric": "tenure",
                        "insight": "Newer C-suite (shorter tenure)",
                        "diff_months": round(diff, 1),
                    }
                )

        # Turnover comparison
        if (
            target.get("changes_12m") is not None
            and peer_avg.get("changes_12m") is not None
        ):
            diff = target["changes_12m"] - peer_avg["changes_12m"]
            if diff > 2:
                insights.append(
                    {
                        "metric": "turnover",
                        "insight": "Higher leadership turnover than peers",
                        "diff": round(diff, 1),
                    }
                )
            elif diff < -2:
                insights.append(
                    {
                        "metric": "turnover",
                        "insight": "Lower leadership turnover than peers",
                        "diff": round(diff, 1),
                    }
                )

        return {
            "insights": insights,
            "metrics_vs_peer_avg": {
                "c_suite_count": {
                    "target": target.get("c_suite_count"),
                    "peer_avg": peer_avg.get("c_suite_count"),
                },
                "vp_count": {
                    "target": target.get("vp_count"),
                    "peer_avg": peer_avg.get("vp_count"),
                },
                "avg_tenure": {
                    "target": target.get("avg_c_suite_tenure_months"),
                    "peer_avg": peer_avg.get("avg_c_suite_tenure_months"),
                },
                "changes_12m": {
                    "target": target.get("changes_12m"),
                    "peer_avg": peer_avg.get("changes_12m"),
                },
            },
        }

    def export_to_json(self, report: Dict[str, Any]) -> str:
        """Export report to JSON string."""
        return json.dumps(report, indent=2, default=str)

    def export_to_csv_data(self, report: Dict[str, Any]) -> List[List[str]]:
        """Export key report data as CSV rows."""
        rows = []

        if report.get("report_type") == "management_assessment":
            # Header
            rows.append(
                [
                    "Name",
                    "Title",
                    "Department",
                    "Start Date",
                    "Tenure (months)",
                    "Board Member",
                ]
            )

            # C-Suite
            for person in report.get("c_suite", []):
                rows.append(
                    [
                        person.get("name", ""),
                        person.get("title", ""),
                        person.get("department", ""),
                        person.get("start_date", ""),
                        str(person.get("tenure_months", "")),
                        "Yes" if person.get("is_board_member") else "No",
                    ]
                )

            # VP Level
            for person in report.get("vp_level", []):
                rows.append(
                    [
                        person.get("name", ""),
                        person.get("title", ""),
                        person.get("department", ""),
                        person.get("start_date", ""),
                        str(person.get("tenure_months", "")),
                        "Yes" if person.get("is_board_member") else "No",
                    ]
                )

        elif report.get("report_type") == "peer_comparison":
            # Header
            rows.append(
                [
                    "Company",
                    "C-Suite Count",
                    "VP Count",
                    "Board Size",
                    "Avg Tenure",
                    "Changes 12M",
                ]
            )

            # Target company
            target = report.get("target_company", {})
            metrics = target.get("metrics", {})
            rows.append(
                [
                    target.get("name", ""),
                    str(metrics.get("c_suite_count", "")),
                    str(metrics.get("vp_count", "")),
                    str(metrics.get("board_size", "")),
                    str(metrics.get("avg_c_suite_tenure_months", "")),
                    str(metrics.get("changes_12m", "")),
                ]
            )

            # Peer companies
            for peer in report.get("peer_companies", []):
                rows.append(
                    [
                        peer.get("company_name", ""),
                        str(peer.get("c_suite_count", "")),
                        str(peer.get("vp_count", "")),
                        str(peer.get("board_size", "")),
                        str(peer.get("avg_c_suite_tenure_months", "")),
                        str(peer.get("changes_12m", "")),
                    ]
                )

        return rows
