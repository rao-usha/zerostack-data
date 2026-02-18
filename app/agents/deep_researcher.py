"""
Deep Company Research Agent.

Performs multi-turn agentic analysis on companies using structured prompts
and LLM reasoning. Goes beyond data collection to provide investment insights.
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ResearchPhase(str, Enum):
    """Phases of deep research."""

    COLLECTING = "collecting"
    ANALYZING = "analyzing"
    SYNTHESIZING = "synthesizing"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class ResearchStep:
    """A single step in the research process."""

    phase: str
    action: str
    input_data: Optional[str] = None
    output: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    tokens_used: int = 0
    cost_usd: float = 0.0


# Structured research prompt template
RESEARCH_PROMPT_TEMPLATE = """You are a senior investment analyst conducting due diligence on {company_name}.

## Research Objective
Provide a comprehensive investment analysis that a fund manager could use to make an investment decision.

## Company Data Collected
{collected_data}

## Analysis Framework
Analyze this company across these dimensions:

### 1. Business Model & Market Position
- What does this company do and who are their customers?
- What is their competitive moat?
- How do they make money?

### 2. Growth & Traction
- What signals indicate growth trajectory?
- How does their tech presence (GitHub activity, web traffic) reflect their momentum?
- What do recent news and events suggest about their trajectory?

### 3. Team & Culture
- What does employer brand data tell us about the company?
- Any leadership signals from news?

### 4. Risk Assessment
- What are the key risks to this investment?
- What data gaps should concern us?
- Any red flags in the data?

### 5. Investment Thesis
- In 2-3 sentences, what is the bull case for this company?
- What would need to be true for this to be a great investment?

## Instructions
Provide a thorough analysis based ONLY on the data provided. Be specific and cite the data.
If data is missing, explicitly note what additional information would be valuable.
Be balanced - identify both opportunities and risks.
"""

FOLLOW_UP_PROMPTS = [
    {
        "name": "competitive_analysis",
        "prompt": """Based on your initial analysis of {company_name}, now dive deeper into competitive positioning:

1. Who are the main competitors based on the data available?
2. What differentiates this company?
3. What would it take for a competitor to displace them?
4. Rate their competitive position: Strong / Moderate / Weak and explain why.

Previous analysis context:
{previous_analysis}
""",
    },
    {
        "name": "risk_deep_dive",
        "prompt": """Now conduct a detailed risk assessment for {company_name}:

1. **Execution Risk**: Based on team/employer data, how likely are they to execute?
2. **Market Risk**: What external factors could impact them?
3. **Financial Risk**: Any signals about cash position or sustainability?
4. **Regulatory Risk**: Any compliance concerns from SEC or news data?

Rate overall risk: Low / Medium / High and explain.

Previous analysis context:
{previous_analysis}
""",
    },
    {
        "name": "investment_recommendation",
        "prompt": """Based on all your analysis of {company_name}, provide your final investment recommendation:

## Investment Recommendation

**Recommendation**: [STRONG BUY / BUY / HOLD / PASS / STRONG PASS]

**Confidence Level**: [High / Medium / Low] - explain why

**Key Thesis** (2-3 sentences):
What is the core reason to invest or not invest?

**Critical Assumptions**:
What must be true for this thesis to play out?

**Key Metrics to Monitor**:
What should we track to validate/invalidate this thesis?

**Ideal Entry Point**:
Under what conditions would this be most attractive?

Previous analysis context:
{previous_analysis}
""",
    },
]


class DeepResearchAgent:
    """
    Multi-turn agentic research agent.

    Performs structured analysis using LLM reasoning across multiple turns
    to generate comprehensive investment insights.
    """

    def __init__(self, db: Session):
        self.db = db
        self.llm = None
        self._ensure_tables()

    def _ensure_tables(self):
        """Create deep research tables if they don't exist."""
        create_table = text("""
            CREATE TABLE IF NOT EXISTS deep_research_jobs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(50) UNIQUE NOT NULL,
                company_name VARCHAR(255) NOT NULL,
                status VARCHAR(20) DEFAULT 'collecting',
                phase VARCHAR(50) DEFAULT 'collecting',
                current_step INT DEFAULT 0,
                total_steps INT DEFAULT 4,
                collected_data JSONB,
                analysis_steps JSONB DEFAULT '[]',
                final_report JSONB,
                total_tokens INT DEFAULT 0,
                total_cost_usd FLOAT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT
            )
        """)
        try:
            self.db.execute(create_table)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def _get_llm(self):
        """Get LLM client."""
        if self.llm is None:
            from app.agentic.llm_client import get_llm_client

            self.llm = get_llm_client()
        return self.llm

    async def start_deep_research(
        self, company_name: str, include_follow_ups: bool = True
    ) -> str:
        """
        Start a deep research job.

        Args:
            company_name: Company to research
            include_follow_ups: Whether to do multi-turn follow-up analysis

        Returns:
            job_id for tracking
        """
        import hashlib

        job_id = f"deep_{hashlib.md5(f'{company_name}{datetime.utcnow()}'.encode()).hexdigest()[:12]}"

        # Calculate total steps
        total_steps = 2  # collect + initial analysis
        if include_follow_ups:
            total_steps += len(FOLLOW_UP_PROMPTS)

        # Create job record
        insert_query = text("""
            INSERT INTO deep_research_jobs
            (job_id, company_name, status, phase, total_steps)
            VALUES (:job_id, :company_name, 'collecting', 'collecting', :total_steps)
        """)

        self.db.execute(
            insert_query,
            {
                "job_id": job_id,
                "company_name": company_name,
                "total_steps": total_steps,
            },
        )
        self.db.commit()

        # Run research in background
        import threading

        thread = threading.Thread(
            target=self._run_deep_research_sync,
            args=(job_id, company_name, include_follow_ups),
        )
        thread.daemon = True
        thread.start()

        return job_id

    def _run_deep_research_sync(
        self, job_id: str, company_name: str, include_follow_ups: bool
    ):
        """Sync wrapper for async research."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                self._run_deep_research(job_id, company_name, include_follow_ups)
            )
        finally:
            loop.close()

    async def _run_deep_research(
        self, job_id: str, company_name: str, include_follow_ups: bool
    ):
        """Execute the deep research process."""
        steps: List[ResearchStep] = []
        total_tokens = 0
        total_cost = 0.0

        try:
            # Phase 1: Collect data
            logger.info(f"[{job_id}] Phase 1: Collecting data for {company_name}")
            self._update_job(job_id, phase="collecting", current_step=0)

            collected_data = await self._collect_company_data(company_name)
            steps.append(
                ResearchStep(
                    phase="collecting",
                    action="Gathered data from 10 sources",
                    output=f"Collected {len(collected_data)} data points",
                )
            )

            self._update_job(
                job_id,
                phase="analyzing",
                current_step=1,
                collected_data=collected_data,
                analysis_steps=steps,
            )

            # Phase 2: Initial Analysis
            logger.info(f"[{job_id}] Phase 2: Initial LLM analysis")

            llm = self._get_llm()
            if not llm:
                raise ValueError(
                    "No LLM API key configured. Set OPENAI_API_KEY or ANTHROPIC_API_KEY."
                )

            # Format collected data for prompt
            data_summary = self._format_data_for_prompt(collected_data)

            initial_prompt = RESEARCH_PROMPT_TEMPLATE.format(
                company_name=company_name, collected_data=data_summary
            )

            logger.info(f"[{job_id}] Sending initial analysis prompt to LLM")
            response = await llm.complete(
                initial_prompt,
                system_prompt="You are a senior investment analyst at a top-tier venture fund. Provide thorough, data-driven analysis.",
            )

            initial_analysis = response.content
            total_tokens += response.total_tokens
            total_cost += response.cost_usd

            steps.append(
                ResearchStep(
                    phase="analyzing",
                    action="Initial comprehensive analysis",
                    input_data=f"Prompt: {len(initial_prompt)} chars",
                    output=initial_analysis[:500] + "..."
                    if len(initial_analysis) > 500
                    else initial_analysis,
                    tokens_used=response.total_tokens,
                    cost_usd=response.cost_usd,
                )
            )

            self._update_job(
                job_id,
                phase="analyzing",
                current_step=2,
                analysis_steps=steps,
                total_tokens=total_tokens,
                total_cost_usd=total_cost,
            )

            # Phase 3+: Follow-up analyses
            previous_analysis = initial_analysis

            if include_follow_ups:
                for i, follow_up in enumerate(FOLLOW_UP_PROMPTS):
                    logger.info(f"[{job_id}] Follow-up {i+1}: {follow_up['name']}")

                    prompt = follow_up["prompt"].format(
                        company_name=company_name,
                        previous_analysis=previous_analysis[
                            -3000:
                        ],  # Last 3000 chars for context
                    )

                    response = await llm.complete(
                        prompt,
                        system_prompt="You are a senior investment analyst. Build on your previous analysis with deeper insights.",
                    )

                    total_tokens += response.total_tokens
                    total_cost += response.cost_usd

                    steps.append(
                        ResearchStep(
                            phase="analyzing",
                            action=f"Deep dive: {follow_up['name'].replace('_', ' ').title()}",
                            output=response.content[:500] + "..."
                            if len(response.content) > 500
                            else response.content,
                            tokens_used=response.total_tokens,
                            cost_usd=response.cost_usd,
                        )
                    )

                    previous_analysis = response.content

                    self._update_job(
                        job_id,
                        phase="analyzing",
                        current_step=3 + i,
                        analysis_steps=steps,
                        total_tokens=total_tokens,
                        total_cost_usd=total_cost,
                    )

            # Phase Final: Synthesize report
            logger.info(f"[{job_id}] Final phase: Synthesizing report")
            self._update_job(job_id, phase="synthesizing")

            final_report = {
                "company_name": company_name,
                "generated_at": datetime.utcnow().isoformat(),
                "collected_data": collected_data,
                "analysis": {
                    "initial_analysis": initial_analysis,
                    "follow_ups": [
                        {
                            "name": FOLLOW_UP_PROMPTS[i]["name"],
                            "analysis": steps[2 + i].output,
                        }
                        for i in range(len(FOLLOW_UP_PROMPTS))
                    ]
                    if include_follow_ups
                    else [],
                    "final_recommendation": previous_analysis,
                },
                "metadata": {
                    "total_tokens": total_tokens,
                    "total_cost_usd": total_cost,
                    "analysis_steps": len(steps),
                    "model": llm.model if llm else "unknown",
                },
            }

            # Mark complete
            self._update_job(
                job_id,
                status="complete",
                phase="complete",
                current_step=len(steps),
                analysis_steps=steps,
                final_report=final_report,
                total_tokens=total_tokens,
                total_cost_usd=total_cost,
                completed_at=datetime.utcnow(),
            )

            logger.info(
                f"[{job_id}] Deep research complete. Tokens: {total_tokens}, Cost: ${total_cost:.4f}"
            )

        except Exception as e:
            logger.error(f"[{job_id}] Deep research failed: {e}")
            self._update_job(
                job_id, status="failed", phase="failed", error_message=str(e)
            )

    async def _collect_company_data(self, company_name: str) -> Dict[str, Any]:
        """Collect data from all sources using the existing research agent."""
        from app.agents.company_researcher import CompanyResearchAgent

        agent = CompanyResearchAgent(self.db)

        # Start research and wait for completion
        job_id = agent.start_research(company_name)

        # Poll for completion (max 60 seconds)
        for _ in range(60):
            status = agent.get_job_status(job_id)
            if status and status.get("status") in ["completed", "partial", "failed"]:
                break
            await asyncio.sleep(1)

        # Get results
        status = agent.get_job_status(job_id)
        if status and status.get("results"):
            return status["results"].get("profile", {})

        return {}

    def _format_data_for_prompt(self, data: Dict) -> str:
        """Format collected data into a readable string for the LLM."""
        sections = []

        # Company basics
        sections.append(f"**Company**: {data.get('company_name', 'Unknown')}")
        if data.get("domain"):
            sections.append(f"**Domain**: {data.get('domain')}")

        # Financials
        financials = data.get("financials", {})
        if any(financials.values()):
            sections.append("\n**Financials**:")
            if financials.get("revenue"):
                sections.append(f"- Revenue: ${financials['revenue']:,.0f}")
            if financials.get("funding_total"):
                sections.append(f"- Total Funding: ${financials['funding_total']:,.0f}")
            if financials.get("assets"):
                sections.append(f"- Assets: ${financials['assets']:,.0f}")

        # Team
        team = data.get("team", {})
        if team.get("employee_count"):
            sections.append(f"\n**Team**: {team['employee_count']:,} employees")
            if team.get("employee_growth_yoy"):
                sections.append(f"- YoY Growth: {team['employee_growth_yoy']:.1%}")

        # Tech Presence
        tech = data.get("tech_presence", {})
        if tech:
            sections.append("\n**Tech Presence (GitHub)**:")
            if tech.get("github_org"):
                sections.append(f"- Organization: {tech['github_org']}")
            if tech.get("public_repos"):
                sections.append(f"- Public Repos: {tech['public_repos']}")
            if tech.get("total_stars"):
                sections.append(f"- Total Stars: {tech['total_stars']:,}")
            if tech.get("velocity_score"):
                sections.append(
                    f"- Developer Velocity Score: {tech['velocity_score']}/100"
                )
            if tech.get("primary_language"):
                sections.append(f"- Primary Language: {tech['primary_language']}")

        # Web Traffic
        traffic = data.get("web_traffic", {})
        if traffic.get("tranco_rank"):
            sections.append(
                f"\n**Web Traffic**: Tranco Rank #{traffic['tranco_rank']:,}"
            )

        # Employer Brand
        employer = data.get("employer_brand", {})
        if employer:
            sections.append("\n**Employer Brand (Glassdoor)**:")
            if employer.get("overall_rating"):
                sections.append(
                    f"- Overall Rating: {employer['overall_rating']:.1f}/5.0"
                )
            if employer.get("ceo_approval"):
                sections.append(f"- CEO Approval: {employer['ceo_approval']*100:.0f}%")
            if employer.get("work_life_balance"):
                sections.append(
                    f"- Work-Life Balance: {employer['work_life_balance']:.1f}/5.0"
                )
            if employer.get("compensation_rating"):
                sections.append(
                    f"- Compensation Rating: {employer['compensation_rating']:.1f}/5.0"
                )

        # SEC Filings
        sec = data.get("sec_filings", {})
        if sec and (sec.get("cik") or sec.get("issuer_name")):
            sections.append("\n**SEC Filings**:")
            if sec.get("cik"):
                sections.append(f"- CIK: {sec['cik']}")
            if sec.get("filing_type"):
                sections.append(f"- Recent Filing: {sec['filing_type']}")

        # Corporate Registry
        corp = data.get("corporate_registry", {})
        if corp:
            sections.append("\n**Corporate Registry**:")
            if corp.get("jurisdiction"):
                sections.append(f"- Jurisdiction: {corp['jurisdiction']}")
            if corp.get("incorporation_date"):
                sections.append(f"- Incorporated: {corp['incorporation_date']}")
            if corp.get("status"):
                sections.append(f"- Status: {corp['status']}")

        # Recent News
        news = data.get("news", {})
        if news.get("recent_articles"):
            sections.append("\n**Recent News**:")
            for article in news["recent_articles"][:5]:
                title = article.get("title", "Untitled")
                event = article.get("event_type", "news")
                sections.append(f"- [{event.upper()}] {title}")
            if news.get("content_summary"):
                sections.append(f"\n*AI Summary*: {news['content_summary']}")

        # Health Score
        health = data.get("health_score", {})
        if health.get("composite"):
            sections.append(
                f"\n**Health Score**: {health['composite']:.0f}/100 (Tier {health.get('tier', 'N/A')})"
            )
            if health.get("growth"):
                sections.append(f"- Growth: {health['growth']:.0f}")
            if health.get("stability"):
                sections.append(f"- Stability: {health['stability']:.0f}")

        # Data Gaps
        gaps = data.get("data_gaps", [])
        if gaps:
            sections.append(
                f"\n**Data Gaps**: {', '.join(g.replace('_', ' ') for g in gaps)}"
            )

        return "\n".join(sections)

    def _update_job(self, job_id: str, **kwargs):
        """Update job record."""
        set_clauses = []
        params = {"job_id": job_id}

        for key, value in kwargs.items():
            if key in ["collected_data", "analysis_steps", "final_report"]:
                # Use CAST for JSONB to avoid sqlalchemy text() issues
                set_clauses.append(f"{key} = CAST(:{key} AS jsonb)")
                params[key] = json.dumps(value, default=str)
            elif key == "completed_at":
                set_clauses.append(f"{key} = NOW()")
            else:
                set_clauses.append(f"{key} = :{key}")
                params[key] = value

        if set_clauses:
            query = text(f"""
                UPDATE deep_research_jobs
                SET {', '.join(set_clauses)}
                WHERE job_id = :job_id
            """)
            try:
                self.db.execute(query, params)
                self.db.commit()
            except Exception as e:
                logger.error(f"Error updating job {job_id}: {e}")
                self.db.rollback()

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get deep research job status."""
        query = text("""
            SELECT * FROM deep_research_jobs WHERE job_id = :job_id
        """)
        result = self.db.execute(query, {"job_id": job_id})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "job_id": row["job_id"],
            "company_name": row["company_name"],
            "status": row["status"],
            "phase": row["phase"],
            "current_step": row["current_step"],
            "total_steps": row["total_steps"],
            "progress": row["current_step"] / row["total_steps"]
            if row["total_steps"] > 0
            else 0,
            "analysis_steps": row["analysis_steps"] or [],
            "final_report": row["final_report"],
            "total_tokens": row["total_tokens"],
            "total_cost_usd": row["total_cost_usd"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "completed_at": row["completed_at"].isoformat()
            if row["completed_at"]
            else None,
            "error_message": row["error_message"],
        }
