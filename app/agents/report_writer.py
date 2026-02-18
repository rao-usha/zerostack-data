"""
Agentic Report Writer (T47).

AI agent that generates comprehensive natural language reports from data,
supporting multiple report types, customizable templates, and export formats.
"""

import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND CONSTANTS
# =============================================================================


class ReportType(str, Enum):
    COMPANY_PROFILE = "company_profile"
    DUE_DILIGENCE = "due_diligence"
    COMPETITIVE_LANDSCAPE = "competitive_landscape"
    PORTFOLIO_SUMMARY = "portfolio_summary"
    INVESTOR_PROFILE = "investor_profile"
    MARKET_OVERVIEW = "market_overview"


class ReportStatus(str, Enum):
    PENDING = "pending"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


class Tone(str, Enum):
    EXECUTIVE = "executive"
    FORMAL = "formal"
    PROFESSIONAL = "professional"
    CASUAL = "casual"


class DetailLevel(str, Enum):
    SUMMARY = "summary"
    STANDARD = "standard"
    DETAILED = "detailed"


# Default templates
DEFAULT_TEMPLATES = {
    "executive_brief": {
        "name": "executive_brief",
        "description": "Concise executive summary for quick review",
        "sections": ["executive_summary", "key_metrics", "alerts"],
        "tone": Tone.EXECUTIVE.value,
        "detail_level": DetailLevel.SUMMARY.value,
        "max_words": 500,
    },
    "full_report": {
        "name": "full_report",
        "description": "Comprehensive detailed analysis",
        "sections": [
            "executive_summary",
            "overview",
            "analysis",
            "metrics",
            "risks",
            "sources",
        ],
        "tone": Tone.FORMAL.value,
        "detail_level": DetailLevel.DETAILED.value,
        "max_words": 5000,
    },
    "investor_memo": {
        "name": "investor_memo",
        "description": "Investment-focused analysis",
        "sections": [
            "executive_summary",
            "investment_thesis",
            "risks",
            "recommendation",
        ],
        "tone": Tone.PROFESSIONAL.value,
        "detail_level": DetailLevel.STANDARD.value,
        "max_words": 2000,
    },
    "quick_overview": {
        "name": "quick_overview",
        "description": "Brief overview with key facts",
        "sections": ["overview", "key_metrics"],
        "tone": Tone.CASUAL.value,
        "detail_level": DetailLevel.SUMMARY.value,
        "max_words": 300,
    },
}


# =============================================================================
# REPORT WRITER AGENT
# =============================================================================


class ReportWriterAgent:
    """AI agent for generating comprehensive reports."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # -------------------------------------------------------------------------
    # TABLE SETUP
    # -------------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Ensure required tables exist."""
        create_reports = text("""
            CREATE TABLE IF NOT EXISTS generated_reports (
                id SERIAL PRIMARY KEY,
                report_id VARCHAR(50) UNIQUE NOT NULL,
                report_type VARCHAR(50) NOT NULL,
                template VARCHAR(50),
                title VARCHAR(500),
                entity_type VARCHAR(50),
                entity_name VARCHAR(255),
                entity_ids INTEGER[],
                content_json JSONB,
                content_markdown TEXT,
                content_html TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                progress INTEGER DEFAULT 0,
                error_message TEXT,
                word_count INTEGER,
                sections_count INTEGER,
                data_sources JSONB,
                confidence FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                requested_by VARCHAR(100)
            )
        """)

        create_templates = text("""
            CREATE TABLE IF NOT EXISTS report_templates (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                description TEXT,
                report_type VARCHAR(50),
                sections JSONB,
                tone VARCHAR(50),
                detail_level VARCHAR(50),
                max_words INTEGER,
                header_format TEXT,
                section_format TEXT,
                is_default BOOLEAN DEFAULT false,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_reports_type ON generated_reports(report_type)",
            "CREATE INDEX IF NOT EXISTS idx_reports_entity ON generated_reports(entity_name)",
            "CREATE INDEX IF NOT EXISTS idx_reports_status ON generated_reports(status)",
        ]

        try:
            self.db.execute(create_reports)
            self.db.execute(create_templates)
            for idx in create_indexes:
                self.db.execute(text(idx))
            self.db.commit()
        except Exception as e:
            logger.debug(f"Table setup: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # REPORT GENERATION
    # -------------------------------------------------------------------------

    def generate_report(
        self,
        report_type: str,
        entity_name: str,
        template_name: str = "full_report",
        options: Dict = None,
    ) -> Dict[str, Any]:
        """Generate a comprehensive report."""
        options = options or {}
        report_id = f"rpt_{uuid.uuid4().hex[:12]}"

        # Create report record
        self._create_report_record(report_id, report_type, entity_name, template_name)

        try:
            # Update status to generating
            self._update_status(report_id, ReportStatus.GENERATING)

            # Get template
            template = self.get_template(template_name)
            if not template:
                template = DEFAULT_TEMPLATES.get("full_report")

            # Gather data based on report type
            data = self._gather_data(report_type, entity_name, options)

            # Generate content sections
            content = {}
            sections = template.get("sections", ["executive_summary", "overview"])
            total_sections = len(sections)

            for i, section in enumerate(sections):
                content[section] = self._write_section(section, data, template)
                progress = int((i + 1) / total_sections * 100)
                self._update_progress(report_id, progress, section)

            # Generate title
            title = self._generate_title(report_type, entity_name)

            # Render to markdown and HTML
            markdown = self._render_markdown(content, template, title)
            html = self._render_html(markdown)

            # Calculate metadata
            word_count = len(markdown.split())
            confidence = data.get("confidence", 0.7)
            data_sources = data.get("sources", [])

            # Save completed report
            self._save_report(
                report_id=report_id,
                content=content,
                markdown=markdown,
                html=html,
                title=title,
                word_count=word_count,
                sections_count=len(sections),
                data_sources=data_sources,
                confidence=confidence,
            )

            return self.get_report(report_id)

        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            self._update_status(report_id, ReportStatus.FAILED, str(e))
            return {"report_id": report_id, "status": "failed", "error": str(e)}

    def _create_report_record(
        self,
        report_id: str,
        report_type: str,
        entity_name: str,
        template: str,
    ) -> None:
        """Create initial report record."""
        query = text("""
            INSERT INTO generated_reports
            (report_id, report_type, template, entity_name, entity_type, status, created_at)
            VALUES (:id, :type, :template, :entity, 'company', 'pending', NOW())
        """)
        self.db.execute(
            query,
            {
                "id": report_id,
                "type": report_type,
                "template": template,
                "entity": entity_name,
            },
        )
        self.db.commit()

    def _update_status(
        self,
        report_id: str,
        status: ReportStatus,
        error: str = None,
    ) -> None:
        """Update report status."""
        if status == ReportStatus.GENERATING:
            query = text("""
                UPDATE generated_reports
                SET status = :status, started_at = NOW()
                WHERE report_id = :id
            """)
        elif status == ReportStatus.FAILED:
            query = text("""
                UPDATE generated_reports
                SET status = :status, error_message = :error
                WHERE report_id = :id
            """)
        else:
            query = text("""
                UPDATE generated_reports
                SET status = :status
                WHERE report_id = :id
            """)

        self.db.execute(
            query, {"id": report_id, "status": status.value, "error": error}
        )
        self.db.commit()

    def _update_progress(self, report_id: str, progress: int, section: str) -> None:
        """Update generation progress."""
        query = text("""
            UPDATE generated_reports
            SET progress = :progress
            WHERE report_id = :id
        """)
        self.db.execute(query, {"id": report_id, "progress": progress})
        self.db.commit()

    def _save_report(
        self,
        report_id: str,
        content: Dict,
        markdown: str,
        html: str,
        title: str,
        word_count: int,
        sections_count: int,
        data_sources: List,
        confidence: float,
    ) -> None:
        """Save completed report."""
        import json

        query = text("""
            UPDATE generated_reports SET
                title = :title,
                content_json = :content,
                content_markdown = :markdown,
                content_html = :html,
                status = 'completed',
                progress = 100,
                word_count = :words,
                sections_count = :sections,
                data_sources = :sources,
                confidence = :confidence,
                completed_at = NOW()
            WHERE report_id = :id
        """)
        self.db.execute(
            query,
            {
                "id": report_id,
                "title": title,
                "content": json.dumps(content),
                "markdown": markdown,
                "html": html,
                "words": word_count,
                "sections": sections_count,
                "sources": json.dumps(data_sources),
                "confidence": confidence,
            },
        )
        self.db.commit()

    # -------------------------------------------------------------------------
    # DATA GATHERING
    # -------------------------------------------------------------------------

    def _gather_data(
        self,
        report_type: str,
        entity_name: str,
        options: Dict,
    ) -> Dict[str, Any]:
        """Gather data for report generation."""
        data = {
            "entity_name": entity_name,
            "report_type": report_type,
            "sources": [],
            "confidence": 0.7,
        }

        # Get company profile data
        profile = self._get_company_profile(entity_name)
        if profile:
            data["profile"] = profile
            data["sources"].append("company_enrichment")

        # Get company scores
        scores = self._get_company_scores(entity_name)
        if scores:
            data["scores"] = scores
            data["sources"].append("company_scores")

        # Get anomalies
        anomalies = self._get_company_anomalies(entity_name)
        if anomalies:
            data["anomalies"] = anomalies
            data["sources"].append("anomalies")

        # Get competitive data if requested
        if options.get("include_competitors", True):
            competitors = self._get_competitors(entity_name)
            if competitors:
                data["competitors"] = competitors
                data["sources"].append("competitive_analysis")

        # Get news if requested
        if options.get("include_news", True):
            news = self._get_recent_news(entity_name)
            if news:
                data["news"] = news
                data["sources"].append("news")

        # Calculate overall confidence
        data["confidence"] = self._calculate_confidence(data)

        return data

    def _get_company_profile(self, company_name: str) -> Optional[Dict]:
        """Get company profile from enrichment."""
        try:
            query = text("""
                SELECT company_name, sector, industry, employee_count,
                       total_funding, description, headquarters, website,
                       founding_date, status
                FROM company_enrichment
                WHERE LOWER(company_name) LIKE LOWER(:name)
                LIMIT 1
            """)
            result = self.db.execute(query, {"name": f"%{company_name}%"})
            row = result.mappings().fetchone()

            if row:
                return dict(row)
            return None
        except Exception as e:
            logger.warning(f"Error getting company profile: {e}")
            self.db.rollback()
            return None

    def _get_company_scores(self, company_name: str) -> Optional[Dict]:
        """Get company scores."""
        try:
            query = text("""
                SELECT composite_score, growth_score, stability_score,
                       market_score, tech_score, tier, confidence
                FROM company_scores
                WHERE LOWER(company_name) LIKE LOWER(:name)
                ORDER BY scored_at DESC
                LIMIT 1
            """)
            result = self.db.execute(query, {"name": f"%{company_name}%"})
            row = result.mappings().fetchone()

            if row:
                return dict(row)
            return None
        except Exception as e:
            logger.warning(f"Error getting company scores: {e}")
            self.db.rollback()
            return None

    def _get_company_anomalies(self, company_name: str) -> List[Dict]:
        """Get recent anomalies for company."""
        try:
            query = text("""
                SELECT anomaly_type, description, severity_level, detected_at
                FROM anomalies
                WHERE LOWER(company_name) LIKE LOWER(:name)
                  AND status != 'resolved'
                ORDER BY detected_at DESC
                LIMIT 5
            """)
            result = self.db.execute(query, {"name": f"%{company_name}%"})
            return [dict(row) for row in result.mappings()]
        except Exception as e:
            logger.warning(f"Error getting anomalies: {e}")
            self.db.rollback()
            return []

    def _get_competitors(self, company_name: str) -> List[Dict]:
        """Get competitor data."""
        try:
            query = text("""
                SELECT competitors
                FROM competitive_analyses
                WHERE LOWER(company_name) LIKE LOWER(:name)
                ORDER BY analyzed_at DESC
                LIMIT 1
            """)
            result = self.db.execute(query, {"name": f"%{company_name}%"})
            row = result.fetchone()

            if row and row[0]:
                return row[0][:5]  # Top 5 competitors
            return []
        except Exception as e:
            logger.warning(f"Error getting competitors: {e}")
            self.db.rollback()
            return []

    def _get_recent_news(self, company_name: str) -> List[Dict]:
        """Get recent news about company."""
        try:
            query = text("""
                SELECT title, source, published_at, sentiment
                FROM news_articles
                WHERE LOWER(company_name) LIKE LOWER(:name)
                ORDER BY published_at DESC
                LIMIT 5
            """)
            result = self.db.execute(query, {"name": f"%{company_name}%"})
            return [dict(row) for row in result.mappings()]
        except Exception as e:
            logger.warning(f"Error getting news: {e}")
            self.db.rollback()
            return []

    def _calculate_confidence(self, data: Dict) -> float:
        """Calculate overall data confidence."""
        sources = len(data.get("sources", []))
        base = 0.5 + (sources * 0.1)

        # Boost for key data
        if data.get("profile"):
            base += 0.1
        if data.get("scores"):
            base += 0.1

        return min(base, 0.95)

    # -------------------------------------------------------------------------
    # CONTENT WRITING
    # -------------------------------------------------------------------------

    def _generate_title(self, report_type: str, entity_name: str) -> str:
        """Generate report title."""
        type_titles = {
            ReportType.COMPANY_PROFILE.value: "Company Profile",
            ReportType.DUE_DILIGENCE.value: "Due Diligence Report",
            ReportType.COMPETITIVE_LANDSCAPE.value: "Competitive Analysis",
            ReportType.PORTFOLIO_SUMMARY.value: "Portfolio Summary",
            ReportType.INVESTOR_PROFILE.value: "Investor Profile",
            ReportType.MARKET_OVERVIEW.value: "Market Overview",
        }
        title_prefix = type_titles.get(report_type, "Report")
        return f"{title_prefix}: {entity_name}"

    def _write_section(
        self,
        section_type: str,
        data: Dict,
        template: Dict,
    ) -> Dict[str, Any]:
        """Write a report section."""
        writers = {
            "executive_summary": self._write_executive_summary,
            "overview": self._write_overview,
            "key_metrics": self._write_key_metrics,
            "analysis": self._write_analysis,
            "metrics": self._write_metrics_section,
            "risks": self._write_risks,
            "alerts": self._write_alerts,
            "investment_thesis": self._write_investment_thesis,
            "recommendation": self._write_recommendation,
            "sources": self._write_sources,
        }

        writer = writers.get(section_type, self._write_generic_section)
        return writer(data, template)

    def _write_executive_summary(self, data: Dict, template: Dict) -> Dict:
        """Write executive summary section."""
        profile = data.get("profile", {})
        scores = data.get("scores", {})
        anomalies = data.get("anomalies", [])
        entity = data.get("entity_name", "Company")

        # Generate key findings
        findings = []

        if profile.get("employee_count"):
            findings.append(
                f"{entity} has approximately {profile['employee_count']:,} employees"
            )

        if profile.get("total_funding"):
            funding_m = profile["total_funding"] / 1_000_000
            findings.append(f"Total funding raised: ${funding_m:,.0f}M")

        if scores.get("composite_score"):
            score = scores["composite_score"]
            findings.append(f"Health score: {score:.0f}/100 (Tier {scores.get('tier', 'N/A')})")

        if profile.get("sector"):
            findings.append(f"Operating in {profile['sector']} sector")

        if not findings:
            findings.append(f"Limited data available for {entity}")

        # Overall assessment
        assessment = self._generate_assessment(scores, anomalies)

        # Alerts
        alerts = []
        for anomaly in anomalies[:3]:
            if anomaly.get("severity_level") in ["high", "critical"]:
                alerts.append(anomaly.get("description", "Unknown alert"))

        return {
            "key_findings": findings[:5],
            "overall_assessment": assessment,
            "alerts": alerts,
        }

    def _generate_assessment(self, scores: Dict, anomalies: List) -> str:
        """Generate overall assessment text."""
        if not scores:
            return "Insufficient data for comprehensive assessment."

        score = scores.get("composite_score", 0)

        if score >= 80:
            base = "Strong performance with solid fundamentals."
        elif score >= 60:
            base = "Moderate performance with room for improvement."
        elif score >= 40:
            base = "Below average performance requiring attention."
        else:
            base = "Weak performance with significant concerns."

        # Add anomaly context
        critical_count = sum(
            1 for a in anomalies if a.get("severity_level") == "critical"
        )
        high_count = sum(1 for a in anomalies if a.get("severity_level") == "high")

        if critical_count > 0:
            base += f" {critical_count} critical issue(s) require immediate attention."
        elif high_count > 0:
            base += f" {high_count} high-priority issue(s) should be monitored."

        return base

    def _write_overview(self, data: Dict, template: Dict) -> Dict:
        """Write company overview section."""
        profile = data.get("profile", {})
        entity = data.get("entity_name", "Company")

        description = profile.get(
            "description", f"{entity} is a company in the technology sector."
        )

        basic_info = {
            "name": profile.get("company_name", entity),
            "sector": profile.get("sector", "Unknown"),
            "industry": profile.get("industry", "Unknown"),
            "headquarters": profile.get("headquarters", "Unknown"),
            "founded": profile.get("founding_date", "Unknown"),
            "website": profile.get("website", "N/A"),
            "status": profile.get("status", "Active"),
        }

        return {
            "description": description,
            "basic_info": basic_info,
        }

    def _write_key_metrics(self, data: Dict, template: Dict) -> Dict:
        """Write key metrics section."""
        profile = data.get("profile", {})
        scores = data.get("scores", {})

        metrics = {}

        if profile.get("employee_count"):
            metrics["employees"] = profile["employee_count"]

        if profile.get("total_funding"):
            metrics["funding"] = profile["total_funding"]

        if scores.get("composite_score"):
            metrics["health_score"] = scores["composite_score"]

        if scores.get("growth_score"):
            metrics["growth_score"] = scores["growth_score"]

        if scores.get("tier"):
            metrics["tier"] = scores["tier"]

        return {"metrics": metrics}

    def _write_analysis(self, data: Dict, template: Dict) -> Dict:
        """Write detailed analysis section."""
        scores = data.get("scores", {})
        competitors = data.get("competitors", [])

        analysis = {
            "score_breakdown": {},
            "competitive_position": "Unknown",
            "strengths": [],
            "weaknesses": [],
        }

        # Score breakdown
        if scores:
            analysis["score_breakdown"] = {
                "growth": scores.get("growth_score", 0),
                "stability": scores.get("stability_score", 0),
                "market": scores.get("market_score", 0),
                "technology": scores.get("tech_score", 0),
            }

            # Identify strengths and weaknesses
            for metric, value in analysis["score_breakdown"].items():
                if value >= 70:
                    analysis["strengths"].append(f"Strong {metric} ({value:.0f}/100)")
                elif value < 40:
                    analysis["weaknesses"].append(f"Weak {metric} ({value:.0f}/100)")

        # Competitive position
        if competitors:
            analysis["competitive_position"] = (
                f"Competing with {len(competitors)} similar companies"
            )
            analysis["top_competitors"] = [
                c.get("name", "Unknown") for c in competitors[:3]
            ]

        return analysis

    def _write_metrics_section(self, data: Dict, template: Dict) -> Dict:
        """Write comprehensive metrics section."""
        return self._write_key_metrics(data, template)

    def _write_risks(self, data: Dict, template: Dict) -> Dict:
        """Write risks section."""
        anomalies = data.get("anomalies", [])
        scores = data.get("scores", {})

        risks = []

        # From anomalies
        for anomaly in anomalies:
            risks.append(
                {
                    "type": anomaly.get("anomaly_type", "unknown"),
                    "description": anomaly.get("description", "Unknown risk"),
                    "severity": anomaly.get("severity_level", "medium"),
                }
            )

        # From low scores
        if scores:
            if scores.get("growth_score", 100) < 40:
                risks.append(
                    {
                        "type": "growth",
                        "description": "Low growth score indicates potential stagnation",
                        "severity": "medium",
                    }
                )
            if scores.get("stability_score", 100) < 40:
                risks.append(
                    {
                        "type": "stability",
                        "description": "Low stability score indicates operational concerns",
                        "severity": "high",
                    }
                )

        return {
            "risks": risks,
            "risk_count": len(risks),
            "high_severity_count": sum(
                1 for r in risks if r["severity"] in ["high", "critical"]
            ),
        }

    def _write_alerts(self, data: Dict, template: Dict) -> Dict:
        """Write alerts section."""
        anomalies = data.get("anomalies", [])

        alerts = []
        for anomaly in anomalies:
            if anomaly.get("severity_level") in ["high", "critical"]:
                alerts.append(
                    {
                        "message": anomaly.get("description"),
                        "severity": anomaly.get("severity_level"),
                        "detected_at": str(anomaly.get("detected_at", "")),
                    }
                )

        return {"alerts": alerts}

    def _write_investment_thesis(self, data: Dict, template: Dict) -> Dict:
        """Write investment thesis section."""
        profile = data.get("profile", {})
        scores = data.get("scores", {})
        entity = data.get("entity_name", "Company")

        thesis_points = []

        # Market position
        if profile.get("sector"):
            thesis_points.append(f"Positioned in {profile['sector']} market")

        # Growth potential
        if scores.get("growth_score", 0) >= 60:
            thesis_points.append("Demonstrates strong growth trajectory")
        elif scores.get("growth_score", 0) >= 40:
            thesis_points.append("Shows moderate growth potential")

        # Scale
        if profile.get("employee_count", 0) > 1000:
            thesis_points.append("Achieved significant scale")
        elif profile.get("employee_count", 0) > 100:
            thesis_points.append("Building organizational capacity")

        # Funding
        if profile.get("total_funding", 0) > 100_000_000:
            thesis_points.append("Well-capitalized for growth")

        return {
            "thesis_points": thesis_points,
            "summary": f"{entity} presents "
            + (
                "a compelling"
                if scores.get("composite_score", 0) >= 70
                else "a moderate"
            )
            + " investment opportunity.",
        }

    def _write_recommendation(self, data: Dict, template: Dict) -> Dict:
        """Write recommendation section."""
        scores = data.get("scores", {})
        anomalies = data.get("anomalies", [])

        score = scores.get("composite_score", 50)
        critical_issues = sum(
            1 for a in anomalies if a.get("severity_level") == "critical"
        )

        if critical_issues > 0:
            recommendation = "HOLD - Address critical issues before proceeding"
            rationale = "Critical issues have been identified that require resolution."
        elif score >= 80:
            recommendation = "STRONG BUY - Excellent fundamentals"
            rationale = "Company demonstrates strong metrics across all categories."
        elif score >= 60:
            recommendation = "BUY - Solid performance"
            rationale = "Company shows good performance with manageable risks."
        elif score >= 40:
            recommendation = "HOLD - Monitor closely"
            rationale = "Mixed signals suggest careful monitoring is warranted."
        else:
            recommendation = "AVOID - Significant concerns"
            rationale = "Multiple areas of concern suggest high risk."

        return {
            "recommendation": recommendation,
            "rationale": rationale,
            "confidence": scores.get("confidence", 0.6),
        }

    def _write_sources(self, data: Dict, template: Dict) -> Dict:
        """Write data sources section."""
        sources = data.get("sources", [])

        source_details = []
        for source in sources:
            source_details.append(
                {
                    "name": source,
                    "type": "database",
                }
            )

        return {
            "sources": source_details,
            "data_freshness": "Current",
            "confidence": data.get("confidence", 0.7),
        }

    def _write_generic_section(self, data: Dict, template: Dict) -> Dict:
        """Write a generic section."""
        return {"content": "Section content not available."}

    # -------------------------------------------------------------------------
    # RENDERING
    # -------------------------------------------------------------------------

    def _render_markdown(self, content: Dict, template: Dict, title: str) -> str:
        """Render report content to markdown."""
        lines = [
            f"# {title}",
            "",
            f"*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
            "",
        ]

        # Executive Summary
        if "executive_summary" in content:
            summary = content["executive_summary"]
            lines.append("## Executive Summary")
            lines.append("")

            if summary.get("key_findings"):
                lines.append("### Key Findings")
                for finding in summary["key_findings"]:
                    lines.append(f"- {finding}")
                lines.append("")

            if summary.get("overall_assessment"):
                lines.append("### Assessment")
                lines.append(summary["overall_assessment"])
                lines.append("")

            if summary.get("alerts"):
                lines.append("### Alerts")
                for alert in summary["alerts"]:
                    lines.append(f"- **WARNING:** {alert}")
                lines.append("")

        # Overview
        if "overview" in content:
            overview = content["overview"]
            lines.append("## Company Overview")
            lines.append("")

            if overview.get("description"):
                lines.append(overview["description"])
                lines.append("")

            if overview.get("basic_info"):
                lines.append("### Basic Information")
                lines.append("")
                lines.append("| Field | Value |")
                lines.append("|-------|-------|")
                for key, value in overview["basic_info"].items():
                    lines.append(f"| {key.replace('_', ' ').title()} | {value} |")
                lines.append("")

        # Key Metrics
        if "key_metrics" in content:
            metrics = content["key_metrics"].get("metrics", {})
            if metrics:
                lines.append("## Key Metrics")
                lines.append("")
                lines.append("| Metric | Value |")
                lines.append("|--------|-------|")
                for key, value in metrics.items():
                    if isinstance(value, (int, float)) and key != "tier":
                        if value >= 1_000_000:
                            display = f"${value/1_000_000:,.1f}M"
                        elif value >= 1_000:
                            display = f"{value:,.0f}"
                        else:
                            display = f"{value:.1f}"
                    else:
                        display = str(value)
                    lines.append(f"| {key.replace('_', ' ').title()} | {display} |")
                lines.append("")

        # Analysis
        if "analysis" in content:
            analysis = content["analysis"]
            lines.append("## Analysis")
            lines.append("")

            if analysis.get("score_breakdown"):
                lines.append("### Score Breakdown")
                lines.append("")
                for metric, score in analysis["score_breakdown"].items():
                    lines.append(f"- **{metric.title()}**: {score:.0f}/100")
                lines.append("")

            if analysis.get("strengths"):
                lines.append("### Strengths")
                for s in analysis["strengths"]:
                    lines.append(f"- {s}")
                lines.append("")

            if analysis.get("weaknesses"):
                lines.append("### Areas for Improvement")
                for w in analysis["weaknesses"]:
                    lines.append(f"- {w}")
                lines.append("")

        # Risks
        if "risks" in content:
            risks_section = content["risks"]
            if risks_section.get("risks"):
                lines.append("## Risk Assessment")
                lines.append("")
                for risk in risks_section["risks"]:
                    severity = risk.get("severity", "medium").upper()
                    lines.append(
                        f"- **[{severity}]** {risk.get('description', 'Unknown risk')}"
                    )
                lines.append("")

        # Investment Thesis
        if "investment_thesis" in content:
            thesis = content["investment_thesis"]
            lines.append("## Investment Thesis")
            lines.append("")
            if thesis.get("thesis_points"):
                for point in thesis["thesis_points"]:
                    lines.append(f"- {point}")
                lines.append("")
            if thesis.get("summary"):
                lines.append(thesis["summary"])
                lines.append("")

        # Recommendation
        if "recommendation" in content:
            rec = content["recommendation"]
            lines.append("## Recommendation")
            lines.append("")
            lines.append(f"**{rec.get('recommendation', 'N/A')}**")
            lines.append("")
            lines.append(rec.get("rationale", ""))
            lines.append("")

        # Sources
        if "sources" in content:
            sources = content["sources"]
            lines.append("## Data Sources")
            lines.append("")
            for source in sources.get("sources", []):
                lines.append(f"- {source.get('name', 'Unknown')}")
            lines.append("")
            lines.append(f"*Confidence: {sources.get('confidence', 0.7):.0%}*")

        return "\n".join(lines)

    def _render_html(self, markdown: str) -> str:
        """Render markdown to HTML."""
        # Simple markdown to HTML conversion
        html = markdown

        # Headers
        import re

        html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
        html = re.sub(r"^## (.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
        html = re.sub(r"^# (.+)$", r"<h1>\1</h1>", html, flags=re.MULTILINE)

        # Bold
        html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)

        # Italic
        html = re.sub(r"\*(.+?)\*", r"<em>\1</em>", html)

        # Lists
        html = re.sub(r"^- (.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)

        # Tables (simplified)
        html = re.sub(r"\|([^|]+)\|", r"<td>\1</td>", html)

        # Wrap in basic HTML structure
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #333; }}
        h2 {{ color: #555; border-bottom: 1px solid #ddd; }}
        table {{ border-collapse: collapse; width: 100%; }}
        td, th {{ border: 1px solid #ddd; padding: 8px; }}
        li {{ margin: 5px 0; }}
    </style>
</head>
<body>
{html}
</body>
</html>"""

        return html

    # -------------------------------------------------------------------------
    # REPORT RETRIEVAL
    # -------------------------------------------------------------------------

    def get_report(self, report_id: str) -> Optional[Dict]:
        """Get a generated report."""
        try:
            query = text("SELECT * FROM generated_reports WHERE report_id = :id")
            result = self.db.execute(query, {"id": report_id})
            row = result.mappings().fetchone()

            if not row:
                return None

            return {
                "report_id": row["report_id"],
                "report_type": row["report_type"],
                "template": row["template"],
                "title": row["title"],
                "entity_name": row["entity_name"],
                "status": row["status"],
                "progress": row["progress"],
                "content": row["content_json"],
                "content_markdown": row["content_markdown"],
                "content_html": row["content_html"],
                "word_count": row["word_count"],
                "sections_count": row["sections_count"],
                "confidence": row["confidence"],
                "data_sources": row["data_sources"],
                "error_message": row["error_message"],
                "created_at": row["created_at"].isoformat()
                if row["created_at"]
                else None,
                "completed_at": row["completed_at"].isoformat()
                if row["completed_at"]
                else None,
            }
        except Exception as e:
            logger.error(f"Error getting report: {e}")
            self.db.rollback()
            return None

    def get_report_status(self, report_id: str) -> Optional[Dict]:
        """Get report generation status."""
        try:
            query = text("""
                SELECT report_id, status, progress, error_message
                FROM generated_reports WHERE report_id = :id
            """)
            result = self.db.execute(query, {"id": report_id})
            row = result.mappings().fetchone()

            if not row:
                return None

            return {
                "report_id": row["report_id"],
                "status": row["status"],
                "progress": row["progress"],
                "error": row["error_message"],
            }
        except Exception as e:
            logger.error(f"Error getting report status: {e}")
            self.db.rollback()
            return None

    def list_reports(
        self,
        report_type: str = None,
        entity_name: str = None,
        status: str = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """List generated reports."""
        try:
            where_parts = []
            params = {"limit": limit}

            if report_type:
                where_parts.append("report_type = :type")
                params["type"] = report_type

            if entity_name:
                where_parts.append("LOWER(entity_name) LIKE LOWER(:entity)")
                params["entity"] = f"%{entity_name}%"

            if status:
                where_parts.append("status = :status")
                params["status"] = status

            where_sql = " AND ".join(where_parts) if where_parts else "1=1"

            query = text(f"""
                SELECT report_id, report_type, title, entity_name,
                       status, created_at, completed_at
                FROM generated_reports
                WHERE {where_sql}
                ORDER BY created_at DESC
                LIMIT :limit
            """)

            result = self.db.execute(query, params)
            reports = []

            for row in result.mappings():
                reports.append(
                    {
                        "report_id": row["report_id"],
                        "report_type": row["report_type"],
                        "title": row["title"],
                        "entity_name": row["entity_name"],
                        "status": row["status"],
                        "created_at": row["created_at"].isoformat()
                        if row["created_at"]
                        else None,
                        "completed_at": row["completed_at"].isoformat()
                        if row["completed_at"]
                        else None,
                    }
                )

            return {"reports": reports, "total": len(reports)}

        except Exception as e:
            logger.error(f"Error listing reports: {e}")
            self.db.rollback()
            return {"reports": [], "total": 0}

    # -------------------------------------------------------------------------
    # TEMPLATES
    # -------------------------------------------------------------------------

    def get_templates(self) -> List[Dict]:
        """Get available report templates."""
        templates = []

        # Add default templates
        for name, config in DEFAULT_TEMPLATES.items():
            templates.append(
                {
                    "name": config["name"],
                    "description": config["description"],
                    "sections": config["sections"],
                    "tone": config["tone"],
                    "detail_level": config["detail_level"],
                    "max_words": config["max_words"],
                    "is_default": True,
                }
            )

        # Add custom templates from database
        try:
            query = text("SELECT * FROM report_templates ORDER BY name")
            result = self.db.execute(query)

            for row in result.mappings():
                templates.append(
                    {
                        "name": row["name"],
                        "description": row["description"],
                        "sections": row["sections"],
                        "tone": row["tone"],
                        "detail_level": row["detail_level"],
                        "max_words": row["max_words"],
                        "is_default": row["is_default"],
                    }
                )
        except Exception as e:
            logger.warning(f"Error loading custom templates: {e}")
            self.db.rollback()

        return templates

    def get_template(self, name: str) -> Optional[Dict]:
        """Get a specific template."""
        # Check defaults first
        if name in DEFAULT_TEMPLATES:
            return DEFAULT_TEMPLATES[name]

        # Check database
        try:
            query = text("SELECT * FROM report_templates WHERE name = :name")
            result = self.db.execute(query, {"name": name})
            row = result.mappings().fetchone()

            if row:
                return {
                    "name": row["name"],
                    "sections": row["sections"],
                    "tone": row["tone"],
                    "detail_level": row["detail_level"],
                    "max_words": row["max_words"],
                }
        except Exception as e:
            logger.warning(f"Error getting template: {e}")
            self.db.rollback()

        return None

    def create_template(
        self,
        name: str,
        description: str,
        sections: List[str],
        tone: str = "formal",
        detail_level: str = "standard",
        max_words: int = 2000,
        report_type: str = None,
    ) -> Dict[str, Any]:
        """Create a custom template."""
        try:
            import json

            query = text("""
                INSERT INTO report_templates
                (name, description, report_type, sections, tone, detail_level, max_words)
                VALUES (:name, :desc, :type, :sections, :tone, :detail, :words)
                RETURNING id, name, created_at
            """)

            result = self.db.execute(
                query,
                {
                    "name": name,
                    "desc": description,
                    "type": report_type,
                    "sections": json.dumps(sections),
                    "tone": tone,
                    "detail": detail_level,
                    "words": max_words,
                },
            )
            row = result.fetchone()
            self.db.commit()

            return {
                "id": row[0],
                "name": row[1],
                "created_at": row[2].isoformat() if row[2] else None,
            }

        except Exception as e:
            logger.error(f"Error creating template: {e}")
            self.db.rollback()
            return {"error": str(e)}

    # -------------------------------------------------------------------------
    # EXPORT
    # -------------------------------------------------------------------------

    def export_report(
        self, report_id: str, format: str = "markdown"
    ) -> Optional[bytes]:
        """Export report in specified format."""
        report = self.get_report(report_id)

        if not report:
            return None

        if format == "markdown":
            return report.get("content_markdown", "").encode("utf-8")

        elif format == "html":
            return report.get("content_html", "").encode("utf-8")

        elif format == "json":
            import json

            return json.dumps(report.get("content", {}), indent=2).encode("utf-8")

        else:
            # Default to markdown
            return report.get("content_markdown", "").encode("utf-8")
