"""
Agentic Due Diligence.

Autonomous AI agent that generates comprehensive due diligence reports
for investment targets by analyzing risk signals across multiple data sources.
"""

import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.agents.company_researcher import CompanyResearchAgent

logger = logging.getLogger(__name__)


class RiskLevel(str, Enum):
    """Risk level classifications."""

    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class DDStatus(str, Enum):
    """Due diligence job status."""

    PENDING = "pending"
    RESEARCHING = "researching"
    ANALYZING = "analyzing"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


class RedFlagCategory(str, Enum):
    """Red flag categories."""

    LEGAL = "legal"
    FINANCIAL = "financial"
    TEAM = "team"
    MARKET = "market"
    COMPETITIVE = "competitive"
    OPERATIONAL = "operational"


class DueDiligenceAgent:
    """
    Autonomous due diligence agent.

    Builds on CompanyResearchAgent (T41) to perform comprehensive
    risk analysis and generate structured DD reports.
    """

    # Red flag patterns by category
    RED_FLAG_PATTERNS = {
        RedFlagCategory.LEGAL: [
            (r"lawsuit|litigation|sued", "medium", "Legal action mentioned"),
            (r"SEC investigation|securities fraud", "high", "SEC investigation"),
            (r"class action", "high", "Class action lawsuit"),
            (r"regulatory fine|penalty|settlement", "medium", "Regulatory penalty"),
            (r"fraud|embezzlement|misconduct", "critical", "Fraud allegation"),
            (r"bankruptcy|chapter 11|chapter 7", "critical", "Bankruptcy filing"),
        ],
        RedFlagCategory.TEAM: [
            (r"CEO (resign|depart|leave|step down|fired)", "high", "CEO departure"),
            (r"CFO (resign|depart|leave|step down|fired)", "high", "CFO departure"),
            (r"CTO (resign|depart|leave|step down|fired)", "medium", "CTO departure"),
            (r"layoff|workforce reduction|job cut", "medium", "Layoffs announced"),
            (r"executive exodus|leadership shakeup", "high", "Executive turnover"),
            (r"founder (resign|depart|leave)", "high", "Founder departure"),
        ],
        RedFlagCategory.FINANCIAL: [
            (r"revenue (decline|drop|fall|decrease)", "medium", "Revenue decline"),
            (r"cash burn|burning cash|runway concern", "medium", "Cash burn concerns"),
            (r"debt default|missed payment", "critical", "Debt default"),
            (r"going concern|material weakness", "critical", "Going concern warning"),
            (r"downgrade|credit rating cut", "medium", "Credit downgrade"),
            (r"loss widening|losses mount", "medium", "Increasing losses"),
        ],
        RedFlagCategory.OPERATIONAL: [
            (r"data breach|security incident|hack", "high", "Data breach"),
            (r"product recall|safety issue", "medium", "Product recall"),
            (r"service outage|downtime|disruption", "low", "Service disruption"),
            (r"quality issue|defect|complaint", "low", "Quality concerns"),
            (
                r"supply chain (issue|disruption|problem)",
                "medium",
                "Supply chain issues",
            ),
        ],
        RedFlagCategory.COMPETITIVE: [
            (r"market share (loss|decline|drop)", "medium", "Market share loss"),
            (
                r"losing to competitor|competitor gains",
                "medium",
                "Competitive pressure",
            ),
            (r"pricing pressure|margin compression", "medium", "Pricing pressure"),
            (r"disrupted by|disruption from", "medium", "Market disruption"),
        ],
        RedFlagCategory.MARKET: [
            (r"traffic (decline|drop|fall)", "medium", "Traffic decline"),
            (r"user (decline|drop|churn)", "medium", "User decline"),
            (r"rating (drop|decline|fall)", "low", "Rating decline"),
            (r"negative review|bad review", "low", "Negative reviews"),
        ],
    }

    # Category weights for risk scoring
    CATEGORY_WEIGHTS = {
        RedFlagCategory.FINANCIAL: 0.30,
        RedFlagCategory.LEGAL: 0.25,
        RedFlagCategory.TEAM: 0.20,
        RedFlagCategory.COMPETITIVE: 0.15,
        RedFlagCategory.OPERATIONAL: 0.10,
    }

    # Severity scores
    SEVERITY_SCORES = {
        "low": 10,
        "medium": 25,
        "high": 50,
        "critical": 80,
    }

    def __init__(self, db: Session):
        self.db = db
        self.research_agent = CompanyResearchAgent(db)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Ensure DD tables exist."""
        create_jobs = text("""
            CREATE TABLE IF NOT EXISTS diligence_jobs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(50) UNIQUE NOT NULL,
                company_name VARCHAR(255) NOT NULL,
                research_job_id VARCHAR(50),
                template VARCHAR(50) DEFAULT 'standard',
                status VARCHAR(20) DEFAULT 'pending',
                progress FLOAT DEFAULT 0,
                phases_completed JSONB DEFAULT '[]',
                risk_score FLOAT,
                risk_level VARCHAR(20),
                red_flags JSONB DEFAULT '[]',
                findings JSONB,
                memo JSONB,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        create_templates = text("""
            CREATE TABLE IF NOT EXISTS diligence_templates (
                id SERIAL PRIMARY KEY,
                template_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                sections JSONB NOT NULL,
                is_default BOOLEAN DEFAULT false,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_cache = text("""
            CREATE TABLE IF NOT EXISTS diligence_cache (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) UNIQUE NOT NULL,
                job_id VARCHAR(50),
                risk_score FLOAT,
                risk_level VARCHAR(20),
                memo JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        """)

        try:
            self.db.execute(create_jobs)
            self.db.execute(create_templates)
            self.db.execute(create_cache)
            self._seed_templates()
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def _seed_templates(self) -> None:
        """Seed default DD templates."""
        templates = [
            {
                "template_id": "standard",
                "name": "Standard Due Diligence",
                "description": "Comprehensive DD covering all major areas",
                "sections": [
                    "financial",
                    "team",
                    "legal",
                    "competitive",
                    "market",
                    "operational",
                ],
                "is_default": True,
            },
            {
                "template_id": "quick",
                "name": "Quick Assessment",
                "description": "Fast risk screening for initial evaluation",
                "sections": ["financial", "legal", "team"],
                "is_default": False,
            },
            {
                "template_id": "deep",
                "name": "Deep Dive",
                "description": "Exhaustive analysis for major investments",
                "sections": [
                    "financial",
                    "team",
                    "legal",
                    "competitive",
                    "market",
                    "operational",
                    "technical",
                    "esg",
                ],
                "is_default": False,
            },
        ]

        for t in templates:
            try:
                insert = text("""
                    INSERT INTO diligence_templates (template_id, name, description, sections, is_default)
                    VALUES (:template_id, :name, :description, :sections, :is_default)
                    ON CONFLICT (template_id) DO NOTHING
                """)
                self.db.execute(
                    insert,
                    {
                        "template_id": t["template_id"],
                        "name": t["name"],
                        "description": t["description"],
                        "sections": json.dumps(t["sections"]),
                        "is_default": t["is_default"],
                    },
                )
            except Exception:
                pass

    def _generate_job_id(self) -> str:
        """Generate unique DD job ID."""
        import uuid

        return f"dd_{uuid.uuid4().hex[:12]}"

    def start_diligence(
        self,
        company_name: str,
        domain: Optional[str] = None,
        template: str = "standard",
        focus_areas: Optional[List[str]] = None,
    ) -> str:
        """
        Start due diligence process for a company.

        Args:
            company_name: Company to analyze
            domain: Company domain for enrichment
            template: DD template to use (standard, quick, deep)
            focus_areas: Specific areas to focus on

        Returns:
            job_id for tracking
        """
        job_id = self._generate_job_id()

        # Create job record
        insert_query = text("""
            INSERT INTO diligence_jobs (job_id, company_name, template, status)
            VALUES (:job_id, :company_name, :template, 'pending')
        """)

        self.db.execute(
            insert_query,
            {
                "job_id": job_id,
                "company_name": company_name,
                "template": template,
            },
        )
        self.db.commit()

        # Run DD in background thread
        import threading

        thread = threading.Thread(
            target=self._run_diligence_sync,
            args=(job_id, company_name, domain, template, focus_areas),
        )
        thread.daemon = True
        thread.start()

        return job_id

    def _run_diligence_sync(
        self,
        job_id: str,
        company_name: str,
        domain: Optional[str],
        template: str,
        focus_areas: Optional[List[str]],
    ) -> None:
        """Execute DD process synchronously (for threading)."""
        import asyncio
        from app.core.database import get_session_factory

        # Create new session for thread
        SessionLocal = get_session_factory()
        db = SessionLocal()

        try:
            # Create thread-local agent
            thread_agent = DueDiligenceAgent.__new__(DueDiligenceAgent)
            thread_agent.db = db
            thread_agent.research_agent = CompanyResearchAgent(db)
            thread_agent.RED_FLAG_PATTERNS = self.RED_FLAG_PATTERNS
            thread_agent.CATEGORY_WEIGHTS = self.CATEGORY_WEIGHTS
            thread_agent.SEVERITY_SCORES = self.SEVERITY_SCORES

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    thread_agent._run_diligence(
                        job_id, company_name, domain, template, focus_areas
                    )
                )
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"DD thread failed: {e}")
            try:
                db.execute(
                    text("""
                    UPDATE diligence_jobs
                    SET status = 'failed', error_message = :error, completed_at = NOW()
                    WHERE job_id = :job_id
                """),
                    {"job_id": job_id, "error": str(e)},
                )
                db.commit()
            except Exception:
                pass
        finally:
            db.close()

    async def _run_diligence(
        self,
        job_id: str,
        company_name: str,
        domain: Optional[str],
        template: str,
        focus_areas: Optional[List[str]],
    ) -> None:
        """Execute the due diligence process."""
        phases_completed = []

        # Phase 1: Research
        self._update_status(job_id, DDStatus.RESEARCHING, 0.1, phases_completed)

        # Start company research using T41
        research_job_id = self.research_agent.start_research(
            company_name=company_name, domain=domain
        )

        # Update with research job ID
        self.db.execute(
            text("""
            UPDATE diligence_jobs SET research_job_id = :rid WHERE job_id = :jid
        """),
            {"rid": research_job_id, "jid": job_id},
        )
        self.db.commit()

        # Wait for research to complete (poll)
        company_profile = await self._wait_for_research(research_job_id)
        phases_completed.append("research")
        self._update_status(job_id, DDStatus.ANALYZING, 0.3, phases_completed)

        # Phase 2: Analyze each category
        findings = {}
        red_flags = []

        # Get template sections
        template_sections = self._get_template_sections(template)
        if focus_areas:
            template_sections = [s for s in template_sections if s in focus_areas]

        total_sections = len(template_sections)
        for i, section in enumerate(template_sections):
            section_findings, section_flags = await self._analyze_section(
                section, company_name, company_profile
            )
            findings[section] = section_findings
            red_flags.extend(section_flags)
            phases_completed.append(section)

            progress = 0.3 + (0.5 * (i + 1) / total_sections)
            self._update_status(job_id, DDStatus.ANALYZING, progress, phases_completed)

        # Phase 3: Calculate risk score
        risk_score, risk_level = self._calculate_risk_score(findings, red_flags)

        # Phase 4: Generate memo
        self._update_status(job_id, DDStatus.GENERATING, 0.9, phases_completed)
        memo = self._generate_memo(
            company_name, findings, red_flags, risk_score, risk_level
        )
        phases_completed.append("memo")

        # Save results
        final_update = text("""
            UPDATE diligence_jobs
            SET status = 'completed',
                progress = 1.0,
                phases_completed = :phases,
                risk_score = :risk_score,
                risk_level = :risk_level,
                red_flags = :red_flags,
                findings = :findings,
                memo = :memo,
                completed_at = NOW()
            WHERE job_id = :job_id
        """)

        self.db.execute(
            final_update,
            {
                "job_id": job_id,
                "phases": json.dumps(phases_completed),
                "risk_score": risk_score,
                "risk_level": risk_level,
                "red_flags": json.dumps(red_flags),
                "findings": json.dumps(findings),
                "memo": json.dumps(memo),
            },
        )
        self.db.commit()

        # Cache the result
        self._cache_result(company_name, job_id, risk_score, risk_level, memo)

    async def _wait_for_research(self, research_job_id: str, timeout: int = 60) -> Dict:
        """Wait for research job to complete."""
        start_time = datetime.utcnow()

        while True:
            result = self.research_agent.get_job_status(research_job_id)

            if not result:
                return {}

            if result["status"] in ("completed", "partial"):
                return result.get("results", {}).get("profile", {})

            if result["status"] == "failed":
                logger.warning(f"Research job failed: {result.get('error')}")
                return {}

            # Check timeout
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > timeout:
                logger.warning(f"Research job timed out after {timeout}s")
                return {}

            await asyncio.sleep(1)

    def _get_template_sections(self, template: str) -> List[str]:
        """Get sections for a template."""
        query = text("""
            SELECT sections FROM diligence_templates WHERE template_id = :tid
        """)
        result = self.db.execute(query, {"tid": template})
        row = result.fetchone()

        if row:
            return row[0] if isinstance(row[0], list) else json.loads(row[0])

        # Default sections
        return ["financial", "team", "legal", "competitive", "market", "operational"]

    async def _analyze_section(
        self, section: str, company_name: str, profile: Dict
    ) -> Tuple[Dict, List[Dict]]:
        """Analyze a specific DD section."""
        findings = {
            "score": 50,  # Default neutral score
            "summary": "",
            "details": {},
            "data_available": False,
        }
        flags = []

        if section == "financial":
            findings, flags = await self._analyze_financial(company_name, profile)
        elif section == "team":
            findings, flags = await self._analyze_team(company_name, profile)
        elif section == "legal":
            findings, flags = await self._analyze_legal(company_name, profile)
        elif section == "competitive":
            findings, flags = await self._analyze_competitive(company_name, profile)
        elif section == "market":
            findings, flags = await self._analyze_market(company_name, profile)
        elif section == "operational":
            findings, flags = await self._analyze_operational(company_name, profile)

        return findings, flags

    async def _analyze_financial(
        self, company_name: str, profile: Dict
    ) -> Tuple[Dict, List]:
        """Analyze financial health."""
        findings = {
            "score": 50,
            "summary": "Limited financial data available",
            "details": {},
            "data_available": False,
        }
        flags = []

        financials = profile.get("financials", {})
        health_score = profile.get("health_score", {})

        if financials:
            findings["data_available"] = True
            findings["details"] = {
                "revenue": financials.get("revenue"),
                "assets": financials.get("assets"),
                "net_income": financials.get("net_income"),
                "funding_total": financials.get("funding_total"),
            }

            # Calculate score based on available data
            score = 50
            if financials.get("revenue"):
                score += 10
            if financials.get("net_income") and financials["net_income"] > 0:
                score += 15
            if financials.get("funding_total"):
                score += 10

            findings["score"] = min(score, 100)
            findings["summary"] = self._generate_financial_summary(financials)

        if health_score:
            stability = health_score.get("stability", 50)
            growth = health_score.get("growth", 50)

            if stability < 40:
                flags.append(
                    {
                        "category": "financial",
                        "severity": "medium",
                        "signal": f"Low stability score ({stability}/100)",
                        "source": "scoring_model",
                    }
                )

            if growth < 30:
                flags.append(
                    {
                        "category": "financial",
                        "severity": "medium",
                        "signal": f"Low growth score ({growth}/100)",
                        "source": "scoring_model",
                    }
                )

        # Scan news for financial red flags
        news_flags = self._scan_for_red_flags(
            company_name, profile.get("news", {}), RedFlagCategory.FINANCIAL
        )
        flags.extend(news_flags)

        return findings, flags

    async def _analyze_team(
        self, company_name: str, profile: Dict
    ) -> Tuple[Dict, List]:
        """Analyze team and leadership."""
        findings = {
            "score": 50,
            "summary": "Limited team data available",
            "details": {},
            "data_available": False,
        }
        flags = []

        team = profile.get("team", {})
        employer_brand = profile.get("employer_brand", {})

        if team:
            findings["data_available"] = True
            findings["details"]["employee_count"] = team.get("employee_count")
            findings["details"]["employee_growth"] = team.get("employee_growth_yoy")

            score = 50
            if team.get("employee_count"):
                score += 10
            if team.get("employee_growth_yoy") and team["employee_growth_yoy"] > 0:
                score += 15
            elif team.get("employee_growth_yoy") and team["employee_growth_yoy"] < -10:
                score -= 15
                flags.append(
                    {
                        "category": "team",
                        "severity": "medium",
                        "signal": f"Employee count declined {abs(team['employee_growth_yoy'])}% YoY",
                        "source": "enrichment",
                    }
                )

            findings["score"] = max(min(score, 100), 0)

        if employer_brand:
            findings["data_available"] = True
            findings["details"]["glassdoor_rating"] = employer_brand.get(
                "overall_rating"
            )
            findings["details"]["ceo_approval"] = employer_brand.get("ceo_approval")
            findings["details"]["recommend_to_friend"] = employer_brand.get(
                "recommend_to_friend"
            )

            rating = employer_brand.get("overall_rating", 0)
            if rating:
                if rating >= 4.0:
                    findings["score"] = max(findings["score"], 75)
                elif rating >= 3.5:
                    findings["score"] = max(findings["score"], 60)
                elif rating < 3.0:
                    findings["score"] = min(findings["score"], 40)
                    flags.append(
                        {
                            "category": "team",
                            "severity": "medium",
                            "signal": f"Low Glassdoor rating ({rating}/5)",
                            "source": "glassdoor",
                        }
                    )

            findings["summary"] = self._generate_team_summary(team, employer_brand)

        # Scan news for team red flags
        news_flags = self._scan_for_red_flags(
            company_name, profile.get("news", {}), RedFlagCategory.TEAM
        )
        flags.extend(news_flags)

        return findings, flags

    async def _analyze_legal(
        self, company_name: str, profile: Dict
    ) -> Tuple[Dict, List]:
        """Analyze legal and regulatory risks."""
        findings = {
            "score": 70,  # Default to good (no news is good news for legal)
            "summary": "No significant legal issues detected",
            "details": {},
            "data_available": False,
        }
        flags = []

        sec_filings = profile.get("sec_filings", {})

        if sec_filings:
            findings["data_available"] = True
            findings["details"]["form_d_raised"] = sec_filings.get(
                "form_d_total_raised"
            )
            findings["details"]["industry_group"] = sec_filings.get("industry_group")

        # Scan news for legal red flags - this is the main source
        news_flags = self._scan_for_red_flags(
            company_name, profile.get("news", {}), RedFlagCategory.LEGAL
        )
        flags.extend(news_flags)

        # Adjust score based on flags
        if news_flags:
            severity_penalty = sum(
                self.SEVERITY_SCORES.get(f["severity"], 10) for f in news_flags
            )
            findings["score"] = max(70 - severity_penalty, 0)
            findings["summary"] = f"Found {len(news_flags)} potential legal concern(s)"

        return findings, flags

    async def _analyze_competitive(
        self, company_name: str, profile: Dict
    ) -> Tuple[Dict, List]:
        """Analyze competitive position."""
        findings = {
            "score": 50,
            "summary": "Limited competitive data available",
            "details": {},
            "data_available": False,
        }
        flags = []

        tech_presence = profile.get("tech_presence", {})
        health_score = profile.get("health_score", {})

        if tech_presence:
            findings["data_available"] = True
            findings["details"]["github_stars"] = tech_presence.get("total_stars")
            findings["details"]["github_contributors"] = tech_presence.get(
                "contributors"
            )
            findings["details"]["velocity_score"] = tech_presence.get("velocity_score")

            velocity = tech_presence.get("velocity_score", 0)
            if velocity:
                if velocity >= 70:
                    findings["score"] = 80
                elif velocity >= 50:
                    findings["score"] = 65
                elif velocity < 30:
                    findings["score"] = 40

        if health_score:
            market_score = health_score.get("market", 50)
            findings["details"]["market_position_score"] = market_score
            findings["score"] = (findings["score"] + market_score) / 2

        # Scan news for competitive red flags
        news_flags = self._scan_for_red_flags(
            company_name, profile.get("news", {}), RedFlagCategory.COMPETITIVE
        )
        flags.extend(news_flags)

        findings["summary"] = self._generate_competitive_summary(findings)

        return findings, flags

    async def _analyze_market(
        self, company_name: str, profile: Dict
    ) -> Tuple[Dict, List]:
        """Analyze market presence and traction."""
        findings = {
            "score": 50,
            "summary": "Limited market data available",
            "details": {},
            "data_available": False,
        }
        flags = []

        mobile = profile.get("mobile_presence", {})

        if mobile and mobile.get("app_count", 0) > 0:
            findings["data_available"] = True
            findings["details"]["app_count"] = mobile.get("app_count")
            findings["details"]["avg_rating"] = mobile.get("avg_rating")

            avg_rating = mobile.get("avg_rating", 0)
            if avg_rating:
                if avg_rating >= 4.5:
                    findings["score"] = 85
                elif avg_rating >= 4.0:
                    findings["score"] = 70
                elif avg_rating >= 3.5:
                    findings["score"] = 55
                elif avg_rating < 3.0:
                    findings["score"] = 35
                    flags.append(
                        {
                            "category": "market",
                            "severity": "medium",
                            "signal": f"Low app store rating ({avg_rating}/5)",
                            "source": "app_store",
                        }
                    )

        # Scan news for market red flags
        news_flags = self._scan_for_red_flags(
            company_name, profile.get("news", {}), RedFlagCategory.MARKET
        )
        flags.extend(news_flags)

        findings["summary"] = self._generate_market_summary(findings)

        return findings, flags

    async def _analyze_operational(
        self, company_name: str, profile: Dict
    ) -> Tuple[Dict, List]:
        """Analyze operational risks."""
        findings = {
            "score": 65,  # Default to good
            "summary": "No significant operational issues detected",
            "details": {},
            "data_available": False,
        }
        flags = []

        # Scan news for operational red flags
        news_flags = self._scan_for_red_flags(
            company_name, profile.get("news", {}), RedFlagCategory.OPERATIONAL
        )
        flags.extend(news_flags)

        if news_flags:
            severity_penalty = sum(
                self.SEVERITY_SCORES.get(f["severity"], 10) for f in news_flags
            )
            findings["score"] = max(65 - severity_penalty, 0)
            findings["summary"] = f"Found {len(news_flags)} operational concern(s)"
            findings["data_available"] = True

        return findings, flags

    def _scan_for_red_flags(
        self, company_name: str, news_data: Dict, category: RedFlagCategory
    ) -> List[Dict]:
        """Scan news articles for red flags."""
        flags = []
        patterns = self.RED_FLAG_PATTERNS.get(category, [])

        articles = news_data.get("recent_articles", [])

        for article in articles:
            title = article.get("title", "").lower()

            for pattern, severity, description in patterns:
                if re.search(pattern, title, re.IGNORECASE):
                    flags.append(
                        {
                            "category": category.value,
                            "severity": severity,
                            "signal": description,
                            "source": "news",
                            "headline": article.get("title"),
                            "date": article.get("date"),
                        }
                    )

        return flags

    def _calculate_risk_score(
        self, findings: Dict[str, Dict], red_flags: List[Dict]
    ) -> Tuple[float, str]:
        """Calculate overall risk score."""
        # Start with category scores (inverted: high score = low risk)
        weighted_risk = 0
        total_weight = 0

        for category, weight in self.CATEGORY_WEIGHTS.items():
            cat_key = category.value
            if cat_key in findings:
                cat_score = findings[cat_key].get("score", 50)
                # Convert to risk: risk = 100 - score
                weighted_risk += (100 - cat_score) * weight
                total_weight += weight

        # Normalize if we have weights
        if total_weight > 0:
            base_risk = weighted_risk / total_weight
        else:
            base_risk = 50

        # Add penalty for red flags
        flag_penalty = 0
        for flag in red_flags:
            severity = flag.get("severity", "low")
            flag_penalty += self.SEVERITY_SCORES.get(severity, 10) * 0.5

        # Cap penalty at 30 points
        flag_penalty = min(flag_penalty, 30)

        risk_score = min(base_risk + flag_penalty, 100)
        risk_score = round(risk_score, 1)

        # Determine level
        if risk_score <= 25:
            risk_level = RiskLevel.LOW.value
        elif risk_score <= 50:
            risk_level = RiskLevel.MODERATE.value
        elif risk_score <= 75:
            risk_level = RiskLevel.HIGH.value
        else:
            risk_level = RiskLevel.CRITICAL.value

        return risk_score, risk_level

    def _generate_memo(
        self,
        company_name: str,
        findings: Dict,
        red_flags: List[Dict],
        risk_score: float,
        risk_level: str,
    ) -> Dict:
        """Generate structured DD memo."""
        # Identify strengths (categories with score > 65)
        strengths = []
        for cat, data in findings.items():
            if data.get("score", 0) > 65:
                strengths.append(
                    {
                        "category": cat,
                        "score": data["score"],
                        "summary": data.get("summary", ""),
                    }
                )

        # Identify concerns (categories with score < 45 or flags)
        concerns = []
        for cat, data in findings.items():
            if data.get("score", 100) < 45:
                concerns.append(
                    {
                        "category": cat,
                        "score": data["score"],
                        "summary": data.get("summary", ""),
                    }
                )

        # Get top red flags
        sorted_flags = sorted(
            red_flags,
            key=lambda x: self.SEVERITY_SCORES.get(x["severity"], 0),
            reverse=True,
        )[:5]

        # Generate recommendation
        if risk_score <= 25:
            recommendation = "Proceed - Low risk profile with strong fundamentals"
        elif risk_score <= 50:
            recommendation = "Proceed with caution - Monitor identified concerns"
        elif risk_score <= 75:
            recommendation = "Significant due diligence required - Address red flags before proceeding"
        else:
            recommendation = "High risk - Recommend further investigation or pass"

        return {
            "company": company_name,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "executive_summary": self._generate_executive_summary(
                company_name,
                risk_score,
                risk_level,
                len(red_flags),
                strengths,
                concerns,
            ),
            "recommendation": recommendation,
            "risk_assessment": {
                "overall_score": risk_score,
                "level": risk_level,
                "red_flag_count": len(red_flags),
                "key_risks": [f["signal"] for f in sorted_flags],
            },
            "strengths": strengths,
            "concerns": concerns,
            "red_flags": sorted_flags,
            "category_scores": {
                cat: {
                    "score": data.get("score", 50),
                    "summary": data.get("summary", ""),
                }
                for cat, data in findings.items()
            },
            "data_coverage": {
                cat: data.get("data_available", False) for cat, data in findings.items()
            },
        }

    def _generate_executive_summary(
        self,
        company_name: str,
        risk_score: float,
        risk_level: str,
        flag_count: int,
        strengths: List,
        concerns: List,
    ) -> str:
        """Generate executive summary text."""
        level_desc = {
            "low": "favorable",
            "moderate": "moderate",
            "high": "elevated",
            "critical": "significant",
        }

        summary = f"{company_name} presents a {level_desc.get(risk_level, 'moderate')} risk profile "
        summary += f"with an overall risk score of {risk_score}/100. "

        if flag_count > 0:
            summary += f"Analysis identified {flag_count} potential red flag(s) requiring attention. "
        else:
            summary += "No significant red flags were identified. "

        if strengths:
            summary += f"Key strengths include {', '.join(s['category'] for s in strengths[:2])}. "

        if concerns:
            summary += f"Areas of concern include {', '.join(c['category'] for c in concerns[:2])}."

        return summary

    def _generate_financial_summary(self, financials: Dict) -> str:
        """Generate financial summary."""
        parts = []
        if financials.get("revenue"):
            parts.append(f"Revenue: ${financials['revenue']:,.0f}")
        if financials.get("funding_total"):
            parts.append(f"Total funding: ${financials['funding_total']:,.0f}")
        if financials.get("net_income"):
            status = (
                "profitable" if financials["net_income"] > 0 else "not yet profitable"
            )
            parts.append(f"Company is {status}")
        return "; ".join(parts) if parts else "Limited financial data available"

    def _generate_team_summary(self, team: Dict, employer_brand: Dict) -> str:
        """Generate team summary."""
        parts = []
        if team.get("employee_count"):
            parts.append(f"{team['employee_count']:,} employees")
        if team.get("employee_growth_yoy"):
            direction = "growth" if team["employee_growth_yoy"] > 0 else "decline"
            parts.append(f"{abs(team['employee_growth_yoy'])}% YoY {direction}")
        if employer_brand.get("overall_rating"):
            parts.append(f"Glassdoor rating: {employer_brand['overall_rating']}/5")
        return "; ".join(parts) if parts else "Limited team data available"

    def _generate_competitive_summary(self, findings: Dict) -> str:
        """Generate competitive summary."""
        details = findings.get("details", {})
        parts = []
        if details.get("velocity_score"):
            parts.append(f"Dev velocity score: {details['velocity_score']}/100")
        if details.get("github_stars"):
            parts.append(f"{details['github_stars']:,} GitHub stars")
        if details.get("market_position_score"):
            parts.append(
                f"Market position score: {details['market_position_score']}/100"
            )
        return "; ".join(parts) if parts else "Limited competitive data available"

    def _generate_market_summary(self, findings: Dict) -> str:
        """Generate market summary."""
        details = findings.get("details", {})
        parts = []
        if details.get("app_count"):
            parts.append(f"{details['app_count']} mobile app(s)")
        if details.get("avg_rating"):
            parts.append(f"Average rating: {details['avg_rating']:.1f}/5")
        return "; ".join(parts) if parts else "Limited market data available"

    def _update_status(
        self,
        job_id: str,
        status: DDStatus,
        progress: float,
        phases_completed: List[str],
    ) -> None:
        """Update job status."""
        query = text("""
            UPDATE diligence_jobs
            SET status = :status, progress = :progress, phases_completed = :phases,
                started_at = COALESCE(started_at, NOW())
            WHERE job_id = :job_id
        """)
        self.db.execute(
            query,
            {
                "job_id": job_id,
                "status": status.value,
                "progress": progress,
                "phases": json.dumps(phases_completed),
            },
        )
        self.db.commit()

    def _cache_result(
        self,
        company_name: str,
        job_id: str,
        risk_score: float,
        risk_level: str,
        memo: Dict,
    ) -> None:
        """Cache DD result."""
        expires = datetime.utcnow() + timedelta(days=30)

        query = text("""
            INSERT INTO diligence_cache (company_name, job_id, risk_score, risk_level, memo, expires_at)
            VALUES (:name, :job_id, :risk_score, :risk_level, :memo, :expires)
            ON CONFLICT (company_name) DO UPDATE SET
                job_id = EXCLUDED.job_id,
                risk_score = EXCLUDED.risk_score,
                risk_level = EXCLUDED.risk_level,
                memo = EXCLUDED.memo,
                created_at = NOW(),
                expires_at = EXCLUDED.expires_at
        """)

        self.db.execute(
            query,
            {
                "name": company_name,
                "job_id": job_id,
                "risk_score": risk_score,
                "risk_level": risk_level,
                "memo": json.dumps(memo),
                "expires": expires,
            },
        )
        self.db.commit()

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get DD job status and results."""
        query = text("SELECT * FROM diligence_jobs WHERE job_id = :job_id")
        result = self.db.execute(query, {"job_id": job_id})
        row = result.mappings().fetchone()

        if not row:
            return None

        response = {
            "job_id": row["job_id"],
            "company_name": row["company_name"],
            "template": row["template"],
            "status": row["status"],
            "progress": row["progress"],
            "phases_completed": row["phases_completed"] or [],
            "research_job_id": row["research_job_id"],
            "created_at": row["created_at"].isoformat() + "Z"
            if row["created_at"]
            else None,
            "started_at": row["started_at"].isoformat() + "Z"
            if row["started_at"]
            else None,
            "completed_at": row["completed_at"].isoformat() + "Z"
            if row["completed_at"]
            else None,
        }

        if row["status"] == "completed":
            response["risk_score"] = row["risk_score"]
            response["risk_level"] = row["risk_level"]
            response["red_flags"] = row["red_flags"] or []
            response["findings"] = row["findings"]
            response["memo"] = row["memo"]

        if row["error_message"]:
            response["error"] = row["error_message"]

        return response

    def get_cached_diligence(self, company_name: str) -> Optional[Dict]:
        """Get cached DD report."""
        query = text("""
            SELECT * FROM diligence_cache
            WHERE LOWER(company_name) = LOWER(:name) AND expires_at > NOW()
        """)
        result = self.db.execute(query, {"name": company_name})
        row = result.mappings().fetchone()

        if row:
            return {
                "company_name": row["company_name"],
                "job_id": row["job_id"],
                "risk_score": row["risk_score"],
                "risk_level": row["risk_level"],
                "memo": row["memo"],
                "cached_at": row["created_at"].isoformat() + "Z"
                if row["created_at"]
                else None,
            }
        return None

    def list_jobs(self, status: Optional[str] = None, limit: int = 20) -> List[Dict]:
        """List DD jobs."""
        conditions = ["1=1"]
        params = {"limit": limit}

        if status:
            conditions.append("status = :status")
            params["status"] = status

        where = " AND ".join(conditions)
        query = text(f"""
            SELECT job_id, company_name, status, progress, risk_score, risk_level,
                   created_at, completed_at
            FROM diligence_jobs
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, params)
        return [
            {
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "status": row["status"],
                "progress": row["progress"],
                "risk_score": row["risk_score"],
                "risk_level": row["risk_level"],
                "created_at": row["created_at"].isoformat() + "Z"
                if row["created_at"]
                else None,
                "completed_at": row["completed_at"].isoformat() + "Z"
                if row["completed_at"]
                else None,
            }
            for row in result.mappings()
        ]

    def get_templates(self) -> List[Dict]:
        """Get available DD templates."""
        query = text("SELECT * FROM diligence_templates ORDER BY is_default DESC, name")
        result = self.db.execute(query)
        return [
            {
                "id": row["template_id"],
                "name": row["name"],
                "description": row["description"],
                "sections": row["sections"]
                if isinstance(row["sections"], list)
                else json.loads(row["sections"]),
                "is_default": row["is_default"],
            }
            for row in result.mappings()
        ]

    def get_stats(self) -> Dict:
        """Get DD statistics."""
        stats_query = text("""
            SELECT
                COUNT(*) as total_jobs,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status IN ('pending', 'researching', 'analyzing', 'generating')) as in_progress,
                AVG(risk_score) FILTER (WHERE status = 'completed') as avg_risk_score,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) FILTER (WHERE completed_at IS NOT NULL) as avg_duration
            FROM diligence_jobs
        """)

        risk_query = text("""
            SELECT risk_level, COUNT(*) as count
            FROM diligence_jobs
            WHERE status = 'completed' AND risk_level IS NOT NULL
            GROUP BY risk_level
        """)

        stats = self.db.execute(stats_query).mappings().fetchone()
        risk_dist = self.db.execute(risk_query).mappings().fetchall()

        return {
            "jobs": {
                "total": stats["total_jobs"],
                "completed": stats["completed"],
                "failed": stats["failed"],
                "in_progress": stats["in_progress"],
                "avg_duration_seconds": round(stats["avg_duration"], 2)
                if stats["avg_duration"]
                else None,
            },
            "risk_analysis": {
                "avg_risk_score": round(stats["avg_risk_score"], 1)
                if stats["avg_risk_score"]
                else None,
                "distribution": {r["risk_level"]: r["count"] for r in risk_dist},
            },
            "templates": len(self.get_templates()),
        }
