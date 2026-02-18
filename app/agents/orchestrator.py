"""
Multi-Agent Orchestrator (T50).

Coordinates multiple agents to complete complex research tasks through
defined workflows with parallel execution, progress tracking, and
partial result handling.

Example workflows:
- "Full DD" = Company Research → Competitive Intel → News Monitor → Due Diligence → Report
- "Quick Scan" = Company Research → Scoring
- "Market Intel" = Market Scanner → News Monitor → Trend Analysis
"""

import asyncio
import json
import logging
import threading
import uuid
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND CONSTANTS
# =============================================================================


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"  # Some steps failed but have results
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Individual step status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class AgentType(str, Enum):
    """Available agent types."""

    COMPANY_RESEARCHER = "company_researcher"
    DUE_DILIGENCE = "due_diligence"
    NEWS_MONITOR = "news_monitor"
    COMPETITIVE_INTEL = "competitive_intel"
    DATA_HUNTER = "data_hunter"
    ANOMALY_DETECTOR = "anomaly_detector"
    REPORT_WRITER = "report_writer"
    MARKET_SCANNER = "market_scanner"


# Default workflow templates
DEFAULT_WORKFLOWS = {
    "full_due_diligence": {
        "name": "Full Due Diligence",
        "description": "Comprehensive DD combining research, competitive analysis, news, and final report",
        "steps": [
            {
                "id": "research",
                "agent": AgentType.COMPANY_RESEARCHER.value,
                "parallel_group": 1,
            },
            {
                "id": "competitive",
                "agent": AgentType.COMPETITIVE_INTEL.value,
                "parallel_group": 1,
            },
            {"id": "news", "agent": AgentType.NEWS_MONITOR.value, "parallel_group": 1},
            {
                "id": "diligence",
                "agent": AgentType.DUE_DILIGENCE.value,
                "depends_on": ["research"],
            },
            {
                "id": "report",
                "agent": AgentType.REPORT_WRITER.value,
                "depends_on": ["diligence", "competitive", "news"],
            },
        ],
        "estimated_duration_minutes": 5,
    },
    "quick_company_scan": {
        "name": "Quick Company Scan",
        "description": "Fast company research with scoring",
        "steps": [
            {
                "id": "research",
                "agent": AgentType.COMPANY_RESEARCHER.value,
                "parallel_group": 1,
            },
        ],
        "estimated_duration_minutes": 2,
    },
    "competitive_landscape": {
        "name": "Competitive Landscape",
        "description": "Deep competitive analysis with market context",
        "steps": [
            {
                "id": "research",
                "agent": AgentType.COMPANY_RESEARCHER.value,
                "parallel_group": 1,
            },
            {
                "id": "competitive",
                "agent": AgentType.COMPETITIVE_INTEL.value,
                "depends_on": ["research"],
            },
            {"id": "news", "agent": AgentType.NEWS_MONITOR.value, "parallel_group": 1},
            {
                "id": "report",
                "agent": AgentType.REPORT_WRITER.value,
                "depends_on": ["competitive", "news"],
            },
        ],
        "estimated_duration_minutes": 4,
    },
    "market_intelligence": {
        "name": "Market Intelligence",
        "description": "Market scanning with trend analysis and news",
        "steps": [
            {
                "id": "market",
                "agent": AgentType.MARKET_SCANNER.value,
                "parallel_group": 1,
            },
            {"id": "news", "agent": AgentType.NEWS_MONITOR.value, "parallel_group": 1},
        ],
        "estimated_duration_minutes": 3,
    },
    "data_enrichment": {
        "name": "Data Enrichment",
        "description": "Hunt for missing data and detect anomalies",
        "steps": [
            {
                "id": "research",
                "agent": AgentType.COMPANY_RESEARCHER.value,
                "parallel_group": 1,
            },
            {
                "id": "hunter",
                "agent": AgentType.DATA_HUNTER.value,
                "depends_on": ["research"],
            },
            {
                "id": "anomaly",
                "agent": AgentType.ANOMALY_DETECTOR.value,
                "depends_on": ["research"],
            },
        ],
        "estimated_duration_minutes": 4,
    },
    "investor_brief": {
        "name": "Investor Brief",
        "description": "Quick research with executive report",
        "steps": [
            {
                "id": "research",
                "agent": AgentType.COMPANY_RESEARCHER.value,
                "parallel_group": 1,
            },
            {"id": "news", "agent": AgentType.NEWS_MONITOR.value, "parallel_group": 1},
            {
                "id": "report",
                "agent": AgentType.REPORT_WRITER.value,
                "depends_on": ["research", "news"],
            },
        ],
        "estimated_duration_minutes": 3,
    },
}


# =============================================================================
# MULTI-AGENT ORCHESTRATOR
# =============================================================================


class MultiAgentOrchestrator:
    """
    Orchestrates multiple agents to complete complex research workflows.

    Features:
    - Parallel execution of independent steps
    - Dependency-aware execution ordering
    - Progress tracking with partial results
    - Failure handling with continuation
    - Result aggregation across agents
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create workflow tracking tables."""
        create_workflows = text("""
            CREATE TABLE IF NOT EXISTS workflow_executions (
                id SERIAL PRIMARY KEY,
                workflow_id VARCHAR(50) UNIQUE NOT NULL,
                workflow_type VARCHAR(50) NOT NULL,
                workflow_name VARCHAR(100),
                entity_type VARCHAR(50),
                entity_name VARCHAR(255),
                entity_params JSONB,
                status VARCHAR(20) DEFAULT 'pending',
                progress FLOAT DEFAULT 0,
                total_steps INTEGER,
                completed_steps INTEGER DEFAULT 0,
                failed_steps INTEGER DEFAULT 0,
                step_results JSONB DEFAULT '{}',
                aggregated_results JSONB,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                requested_by VARCHAR(100)
            )
        """)

        create_steps = text("""
            CREATE TABLE IF NOT EXISTS workflow_steps (
                id SERIAL PRIMARY KEY,
                workflow_id VARCHAR(50) NOT NULL,
                step_id VARCHAR(50) NOT NULL,
                agent_type VARCHAR(50) NOT NULL,
                parallel_group INTEGER,
                depends_on JSONB DEFAULT '[]',
                status VARCHAR(20) DEFAULT 'pending',
                agent_job_id VARCHAR(50),
                result JSONB,
                error_message TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                duration_seconds FLOAT,
                UNIQUE(workflow_id, step_id)
            )
        """)

        create_templates = text("""
            CREATE TABLE IF NOT EXISTS workflow_templates (
                id SERIAL PRIMARY KEY,
                template_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                steps JSONB NOT NULL,
                estimated_duration_minutes INTEGER,
                is_custom BOOLEAN DEFAULT false,
                created_by VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_indexes = text("""
            CREATE INDEX IF NOT EXISTS idx_workflow_exec_status
                ON workflow_executions(status);
            CREATE INDEX IF NOT EXISTS idx_workflow_exec_entity
                ON workflow_executions(entity_name);
            CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow
                ON workflow_steps(workflow_id);
            CREATE INDEX IF NOT EXISTS idx_workflow_steps_status
                ON workflow_steps(status);
        """)

        try:
            self.db.execute(create_workflows)
            self.db.execute(create_steps)
            self.db.execute(create_templates)
            self.db.execute(create_indexes)
            self._seed_templates()
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def _seed_templates(self) -> None:
        """Seed default workflow templates."""
        for template_id, template in DEFAULT_WORKFLOWS.items():
            try:
                insert = text("""
                    INSERT INTO workflow_templates (template_id, name, description, steps, estimated_duration_minutes, is_custom)
                    VALUES (:tid, :name, :desc, :steps, :duration, false)
                    ON CONFLICT (template_id) DO NOTHING
                """)
                self.db.execute(
                    insert,
                    {
                        "tid": template_id,
                        "name": template["name"],
                        "desc": template["description"],
                        "steps": json.dumps(template["steps"]),
                        "duration": template.get("estimated_duration_minutes", 5),
                    },
                )
            except Exception:
                pass

    def _generate_workflow_id(self) -> str:
        """Generate unique workflow ID."""
        return f"wf_{uuid.uuid4().hex[:12]}"

    # -------------------------------------------------------------------------
    # WORKFLOW EXECUTION
    # -------------------------------------------------------------------------

    def start_workflow(
        self,
        workflow_type: str,
        entity_name: str,
        entity_type: str = "company",
        entity_params: Optional[Dict] = None,
        requested_by: Optional[str] = None,
    ) -> str:
        """
        Start a workflow execution.

        Args:
            workflow_type: Template ID (e.g., "full_due_diligence")
            entity_name: Target entity (e.g., company name)
            entity_type: Type of entity ("company", "investor", "market")
            entity_params: Additional parameters for agents
            requested_by: User/system that requested the workflow

        Returns:
            workflow_id for tracking
        """
        # Get workflow template
        template = self._get_template(workflow_type)
        if not template:
            raise ValueError(f"Unknown workflow type: {workflow_type}")

        workflow_id = self._generate_workflow_id()
        steps = template["steps"]
        total_steps = len(steps)

        # Create workflow record
        insert_workflow = text("""
            INSERT INTO workflow_executions (
                workflow_id, workflow_type, workflow_name, entity_type, entity_name,
                entity_params, status, total_steps, requested_by
            ) VALUES (
                :wid, :wtype, :wname, :etype, :ename, :eparams, 'pending', :total, :requested_by
            )
        """)

        self.db.execute(
            insert_workflow,
            {
                "wid": workflow_id,
                "wtype": workflow_type,
                "wname": template["name"],
                "etype": entity_type,
                "ename": entity_name,
                "eparams": json.dumps(entity_params or {}),
                "total": total_steps,
                "requested_by": requested_by,
            },
        )

        # Create step records
        for step in steps:
            insert_step = text("""
                INSERT INTO workflow_steps (workflow_id, step_id, agent_type, parallel_group, depends_on)
                VALUES (:wid, :sid, :agent, :pgroup, :deps)
            """)
            self.db.execute(
                insert_step,
                {
                    "wid": workflow_id,
                    "sid": step["id"],
                    "agent": step["agent"],
                    "pgroup": step.get("parallel_group"),
                    "deps": json.dumps(step.get("depends_on", [])),
                },
            )

        self.db.commit()

        # Start execution in background thread
        thread = threading.Thread(
            target=self._run_workflow_sync,
            args=(workflow_id, entity_name, entity_type, entity_params or {}),
        )
        thread.daemon = True
        thread.start()

        return workflow_id

    def _run_workflow_sync(
        self,
        workflow_id: str,
        entity_name: str,
        entity_type: str,
        entity_params: Dict,
    ) -> None:
        """Execute workflow synchronously (for threading)."""
        from app.core.database import get_session_factory

        # Create new session for this thread
        SessionLocal = get_session_factory()
        db = SessionLocal()

        try:
            # Create thread-local orchestrator
            thread_orchestrator = MultiAgentOrchestrator.__new__(MultiAgentOrchestrator)
            thread_orchestrator.db = db

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    thread_orchestrator._run_workflow(
                        workflow_id, entity_name, entity_type, entity_params
                    )
                )
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Workflow thread failed: {e}")
            try:
                db.execute(
                    text("""
                    UPDATE workflow_executions
                    SET status = 'failed', error_message = :error, completed_at = NOW()
                    WHERE workflow_id = :wid
                """),
                    {"wid": workflow_id, "error": str(e)},
                )
                db.commit()
            except Exception:
                pass
        finally:
            db.close()

    async def _run_workflow(
        self,
        workflow_id: str,
        entity_name: str,
        entity_type: str,
        entity_params: Dict,
    ) -> None:
        """Execute the workflow asynchronously."""
        # Update status to running
        self.db.execute(
            text("""
            UPDATE workflow_executions
            SET status = 'running', started_at = NOW()
            WHERE workflow_id = :wid
        """),
            {"wid": workflow_id},
        )
        self.db.commit()

        # Get steps
        steps = self._get_workflow_steps(workflow_id)
        step_results = {}
        completed_steps = set()
        failed_steps = set()

        # Build dependency graph
        step_deps = {s["step_id"]: set(s["depends_on"] or []) for s in steps}
        # Execute steps in order, respecting dependencies
        while len(completed_steps) + len(failed_steps) < len(steps):
            # Find steps ready to run (dependencies satisfied)
            ready_steps = []
            for step in steps:
                sid = step["step_id"]
                if sid in completed_steps or sid in failed_steps:
                    continue
                if step["status"] == "running":
                    continue
                deps = step_deps.get(sid, set())
                # Check if all dependencies are completed
                if deps.issubset(completed_steps):
                    ready_steps.append(step)

            if not ready_steps:
                # No progress possible, check for deadlock
                pending = [
                    s
                    for s in steps
                    if s["step_id"] not in completed_steps
                    and s["step_id"] not in failed_steps
                ]
                if pending:
                    # Some steps blocked by failed dependencies
                    for s in pending:
                        deps = step_deps.get(s["step_id"], set())
                        if deps & failed_steps:
                            # Skip steps with failed dependencies
                            self._mark_step_skipped(
                                workflow_id, s["step_id"], "Dependency failed"
                            )
                            failed_steps.add(s["step_id"])
                break

            # Group ready steps by parallel_group for concurrent execution
            parallel_groups: Dict[Optional[int], List[Dict]] = {}
            for step in ready_steps:
                pg = step.get("parallel_group")
                if pg not in parallel_groups:
                    parallel_groups[pg] = []
                parallel_groups[pg].append(step)

            # Execute each parallel group
            for pgroup, group_steps in parallel_groups.items():
                if pgroup is not None:
                    # Run in parallel
                    tasks = [
                        self._execute_step(
                            workflow_id,
                            s,
                            entity_name,
                            entity_type,
                            entity_params,
                            step_results,
                        )
                        for s in group_steps
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for step, result in zip(group_steps, results):
                        sid = step["step_id"]
                        if isinstance(result, Exception):
                            failed_steps.add(sid)
                            logger.error(f"Step {sid} failed: {result}")
                        elif result:
                            step_results[sid] = result
                            completed_steps.add(sid)
                        else:
                            failed_steps.add(sid)
                else:
                    # Run sequentially
                    for step in group_steps:
                        sid = step["step_id"]
                        try:
                            result = await self._execute_step(
                                workflow_id,
                                step,
                                entity_name,
                                entity_type,
                                entity_params,
                                step_results,
                            )
                            if result:
                                step_results[sid] = result
                                completed_steps.add(sid)
                            else:
                                failed_steps.add(sid)
                        except Exception as e:
                            logger.error(f"Step {sid} failed: {e}")
                            failed_steps.add(sid)

            # Update progress
            total = len(steps)
            done = len(completed_steps) + len(failed_steps)
            progress = done / total if total > 0 else 0

            self.db.execute(
                text("""
                UPDATE workflow_executions
                SET progress = :progress, completed_steps = :completed, failed_steps = :failed,
                    step_results = :results
                WHERE workflow_id = :wid
            """),
                {
                    "wid": workflow_id,
                    "progress": progress,
                    "completed": len(completed_steps),
                    "failed": len(failed_steps),
                    "results": json.dumps(step_results),
                },
            )
            self.db.commit()

        # Aggregate results
        aggregated = self._aggregate_results(entity_name, step_results)

        # Determine final status
        if len(completed_steps) == len(steps):
            status = WorkflowStatus.COMPLETED
        elif len(completed_steps) > 0:
            status = WorkflowStatus.PARTIAL
        else:
            status = WorkflowStatus.FAILED

        # Final update
        self.db.execute(
            text("""
            UPDATE workflow_executions
            SET status = :status, progress = 1.0, completed_steps = :completed,
                failed_steps = :failed, step_results = :results,
                aggregated_results = :aggregated, completed_at = NOW()
            WHERE workflow_id = :wid
        """),
            {
                "wid": workflow_id,
                "status": status.value,
                "completed": len(completed_steps),
                "failed": len(failed_steps),
                "results": json.dumps(step_results),
                "aggregated": json.dumps(aggregated),
            },
        )
        self.db.commit()

    async def _execute_step(
        self,
        workflow_id: str,
        step: Dict,
        entity_name: str,
        entity_type: str,
        entity_params: Dict,
        prior_results: Dict,
    ) -> Optional[Dict]:
        """Execute a single workflow step."""
        step_id = step["step_id"]
        agent_type = step["agent_type"]

        # Mark step as running
        self.db.execute(
            text("""
            UPDATE workflow_steps
            SET status = 'running', started_at = NOW()
            WHERE workflow_id = :wid AND step_id = :sid
        """),
            {"wid": workflow_id, "sid": step_id},
        )
        self.db.commit()

        start_time = datetime.utcnow()

        try:
            # Execute the appropriate agent
            result = await self._run_agent(
                agent_type, entity_name, entity_type, entity_params, prior_results
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            # Mark step as completed
            self.db.execute(
                text("""
                UPDATE workflow_steps
                SET status = 'completed', result = :result, completed_at = NOW(),
                    duration_seconds = :duration
                WHERE workflow_id = :wid AND step_id = :sid
            """),
                {
                    "wid": workflow_id,
                    "sid": step_id,
                    "result": json.dumps(result) if result else None,
                    "duration": duration,
                },
            )
            self.db.commit()

            return result

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = str(e)

            # Mark step as failed
            self.db.execute(
                text("""
                UPDATE workflow_steps
                SET status = 'failed', error_message = :error, completed_at = NOW(),
                    duration_seconds = :duration
                WHERE workflow_id = :wid AND step_id = :sid
            """),
                {
                    "wid": workflow_id,
                    "sid": step_id,
                    "error": error_msg,
                    "duration": duration,
                },
            )
            self.db.commit()

            logger.error(f"Step {step_id} failed: {e}")
            return None

    async def _run_agent(
        self,
        agent_type: str,
        entity_name: str,
        entity_type: str,
        entity_params: Dict,
        prior_results: Dict,
    ) -> Optional[Dict]:
        """Run a specific agent and return its results."""
        domain = entity_params.get("domain")

        if agent_type == AgentType.COMPANY_RESEARCHER.value:
            return await self._run_company_researcher(entity_name, domain)

        elif agent_type == AgentType.DUE_DILIGENCE.value:
            return await self._run_due_diligence(entity_name, domain)

        elif agent_type == AgentType.NEWS_MONITOR.value:
            return await self._run_news_monitor(entity_name)

        elif agent_type == AgentType.COMPETITIVE_INTEL.value:
            return await self._run_competitive_intel(entity_name)

        elif agent_type == AgentType.DATA_HUNTER.value:
            return await self._run_data_hunter(entity_name)

        elif agent_type == AgentType.ANOMALY_DETECTOR.value:
            return await self._run_anomaly_detector(entity_name)

        elif agent_type == AgentType.REPORT_WRITER.value:
            return await self._run_report_writer(entity_name, prior_results)

        elif agent_type == AgentType.MARKET_SCANNER.value:
            return await self._run_market_scanner()

        else:
            raise ValueError(f"Unknown agent type: {agent_type}")

    # -------------------------------------------------------------------------
    # AGENT RUNNERS
    # -------------------------------------------------------------------------

    async def _run_company_researcher(
        self, company_name: str, domain: Optional[str]
    ) -> Optional[Dict]:
        """Run company research agent."""
        try:
            from app.agents.company_researcher import CompanyResearchAgent

            agent = CompanyResearchAgent(self.db)
            job_id = agent.start_research(company_name=company_name, domain=domain)

            # Wait for completion (poll)
            result = await self._wait_for_job(
                lambda: agent.get_job_status(job_id), timeout=120
            )

            if result and result.get("status") in ("completed", "partial"):
                return result.get("results", {}).get("profile", {})
            return None

        except Exception as e:
            logger.error(f"Company researcher failed: {e}")
            raise

    async def _run_due_diligence(
        self, company_name: str, domain: Optional[str]
    ) -> Optional[Dict]:
        """Run due diligence agent."""
        try:
            from app.agents.due_diligence import DueDiligenceAgent

            agent = DueDiligenceAgent(self.db)
            job_id = agent.start_diligence(company_name=company_name, domain=domain)

            # Wait for completion
            result = await self._wait_for_job(
                lambda: agent.get_job_status(job_id), timeout=180
            )

            if result and result.get("status") == "completed":
                return {
                    "risk_score": result.get("risk_score"),
                    "risk_level": result.get("risk_level"),
                    "memo": result.get("memo"),
                    "red_flags": result.get("red_flags", []),
                }
            return None

        except Exception as e:
            logger.error(f"Due diligence failed: {e}")
            raise

    async def _run_news_monitor(self, entity_name: str) -> Optional[Dict]:
        """Run news monitor agent."""
        try:
            from app.agents.news_monitor import NewsMonitorAgent

            agent = NewsMonitorAgent(self.db)

            # Process recent news to find matches for the entity
            result = agent.process_recent_news(days=7)

            # Filter for relevant articles
            articles = []
            for match in result.get("matches", []):
                if entity_name.lower() in (match.get("title", "") or "").lower():
                    articles.append(
                        {
                            "title": match.get("title"),
                            "source": match.get("source"),
                            "date": match.get("published_at"),
                            "event_type": match.get("event_type"),
                        }
                    )

            return {
                "article_count": len(articles),
                "articles": articles[:5],  # Limit for workflow
                "total_processed": result.get("processed", 0),
            }

        except Exception as e:
            logger.error(f"News monitor failed: {e}")
            raise

    async def _run_competitive_intel(self, company_name: str) -> Optional[Dict]:
        """Run competitive intelligence agent."""
        try:
            from app.agents.competitive_intel import CompetitiveIntelAgent

            agent = CompetitiveIntelAgent(self.db)

            # Check for cached analysis first
            analysis = agent.get_cached_analysis(company_name)

            if analysis:
                return {
                    "competitors": analysis.get("competitors", [])[:5],
                    "market_position": analysis.get("market_position"),
                    "moat_score": analysis.get("moat_assessment", {}).get(
                        "overall_score"
                    ),
                    "comparison_matrix": analysis.get("comparison_matrix"),
                }

            # If no cached analysis, try to compare with similar companies
            # by looking up sector peers
            comparison = agent.compare_companies([company_name])
            if comparison:
                return {
                    "competitors": [],
                    "comparison_data": comparison,
                }

            return None

        except Exception as e:
            logger.error(f"Competitive intel failed: {e}")
            raise

    async def _run_data_hunter(self, entity_name: str) -> Optional[Dict]:
        """Run data hunter agent."""
        try:
            from app.agents.data_hunter import DataHunterAgent

            agent = DataHunterAgent(self.db)

            # Start a hunt for the entity
            result = agent.hunt_entity(entity_name)

            return {
                "gaps_found": result.get("gaps_found", 0),
                "gaps_filled": result.get("gaps_filled", 0),
                "sources_checked": result.get("sources_checked", []),
                "fields_updated": result.get("fields_updated", []),
            }

        except Exception as e:
            logger.error(f"Data hunter failed: {e}")
            raise

    async def _run_anomaly_detector(self, entity_name: str) -> Optional[Dict]:
        """Run anomaly detector agent."""
        try:
            from app.agents.anomaly_detector import AnomalyDetectorAgent

            agent = AnomalyDetectorAgent(self.db)

            # Run all anomaly detection methods for this entity
            all_anomalies = []

            # Check score anomalies
            score_anomalies = agent.detect_score_anomalies(company_name=entity_name)
            all_anomalies.extend(score_anomalies)

            # Check employee anomalies
            emp_anomalies = agent.detect_employee_anomalies(company_name=entity_name)
            all_anomalies.extend(emp_anomalies)

            # Check traffic anomalies
            traffic_anomalies = agent.detect_traffic_anomalies(company_name=entity_name)
            all_anomalies.extend(traffic_anomalies)

            # Check rating anomalies
            rating_anomalies = agent.detect_rating_anomalies(company_name=entity_name)
            all_anomalies.extend(rating_anomalies)

            return {
                "anomaly_count": len(all_anomalies),
                "anomalies": all_anomalies[:5],  # Limit for workflow
                "by_type": {
                    "score": len(score_anomalies),
                    "employee": len(emp_anomalies),
                    "traffic": len(traffic_anomalies),
                    "rating": len(rating_anomalies),
                },
            }

        except Exception as e:
            logger.error(f"Anomaly detector failed: {e}")
            raise

    async def _run_report_writer(
        self, entity_name: str, prior_results: Dict
    ) -> Optional[Dict]:
        """Run report writer agent."""
        try:
            from app.agents.report_writer import ReportWriterAgent

            agent = ReportWriterAgent(self.db)

            # Generate a company profile report
            report = agent.generate_report(
                report_type="company_profile",
                entity_name=entity_name,
                template_name="executive_brief",
                options={"workflow_data": prior_results},
            )

            if report and report.get("status") == "completed":
                return {
                    "report_id": report.get("report_id"),
                    "title": report.get("title"),
                    "word_count": report.get("word_count"),
                    "sections": list(report.get("content", {}).keys())
                    if report.get("content")
                    else [],
                }

            return {
                "report_id": report.get("report_id") if report else None,
                "status": report.get("status") if report else "failed",
            }

        except Exception as e:
            logger.error(f"Report writer failed: {e}")
            # Report writer is optional, don't fail the workflow
            return {"error": str(e), "skipped": True}

    async def _run_market_scanner(self) -> Optional[Dict]:
        """Run market scanner agent."""
        try:
            from app.agents.market_scanner import MarketScannerAgent

            agent = MarketScannerAgent(self.db)

            # Get current market signals
            signals = agent.get_current_signals(limit=20)

            # Get trends
            trends = agent.get_trends(period_days=30)

            # Get opportunities
            opportunities = agent.get_opportunities()

            return {
                "signals": signals.get("signals", [])[:5],
                "signal_count": signals.get("total", 0),
                "trends": trends.get("trends", [])[:5],
                "opportunities": opportunities.get("opportunities", [])[:3],
            }

        except Exception as e:
            logger.error(f"Market scanner failed: {e}")
            raise

    # -------------------------------------------------------------------------
    # HELPER METHODS
    # -------------------------------------------------------------------------

    async def _wait_for_job(
        self,
        get_status: Callable,
        timeout: int = 120,
        poll_interval: float = 1.0,
    ) -> Optional[Dict]:
        """Wait for a job to complete."""
        start_time = datetime.utcnow()

        while True:
            result = get_status()

            if not result:
                return None

            status = result.get("status", "")
            if status in ("completed", "partial", "failed"):
                return result

            # Check timeout
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > timeout:
                logger.warning(f"Job timed out after {timeout}s")
                return result

            await asyncio.sleep(poll_interval)

    def _mark_step_skipped(self, workflow_id: str, step_id: str, reason: str) -> None:
        """Mark a step as skipped."""
        self.db.execute(
            text("""
            UPDATE workflow_steps
            SET status = 'skipped', error_message = :reason, completed_at = NOW()
            WHERE workflow_id = :wid AND step_id = :sid
        """),
            {"wid": workflow_id, "sid": step_id, "reason": reason},
        )
        self.db.commit()

    def _get_workflow_steps(self, workflow_id: str) -> List[Dict]:
        """Get all steps for a workflow."""
        query = text("""
            SELECT step_id, agent_type, parallel_group, depends_on, status
            FROM workflow_steps
            WHERE workflow_id = :wid
            ORDER BY id
        """)
        result = self.db.execute(query, {"wid": workflow_id})
        return [
            {
                "step_id": row["step_id"],
                "agent_type": row["agent_type"],
                "parallel_group": row["parallel_group"],
                "depends_on": row["depends_on"]
                if isinstance(row["depends_on"], list)
                else json.loads(row["depends_on"] or "[]"),
                "status": row["status"],
            }
            for row in result.mappings()
        ]

    def _get_template(self, template_id: str) -> Optional[Dict]:
        """Get workflow template."""
        query = text("""
            SELECT template_id, name, description, steps, estimated_duration_minutes
            FROM workflow_templates
            WHERE template_id = :tid
        """)
        result = self.db.execute(query, {"tid": template_id})
        row = result.mappings().fetchone()

        if row:
            return {
                "id": row["template_id"],
                "name": row["name"],
                "description": row["description"],
                "steps": row["steps"]
                if isinstance(row["steps"], list)
                else json.loads(row["steps"]),
                "estimated_duration_minutes": row["estimated_duration_minutes"],
            }

        # Check built-in templates
        if template_id in DEFAULT_WORKFLOWS:
            return {"id": template_id, **DEFAULT_WORKFLOWS[template_id]}

        return None

    def _aggregate_results(self, entity_name: str, step_results: Dict) -> Dict:
        """Aggregate results from all steps into a unified report."""
        aggregated = {
            "entity": entity_name,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "steps_completed": list(step_results.keys()),
        }

        # Company profile from research
        if "research" in step_results:
            aggregated["company_profile"] = step_results["research"]

        # Due diligence results
        if "diligence" in step_results:
            dd = step_results["diligence"]
            aggregated["due_diligence"] = {
                "risk_score": dd.get("risk_score"),
                "risk_level": dd.get("risk_level"),
                "red_flags": dd.get("red_flags", []),
            }

        # Competitive analysis
        if "competitive" in step_results:
            comp = step_results["competitive"]
            aggregated["competitive_landscape"] = {
                "market_position": comp.get("market_position"),
                "competitors": comp.get("competitors", []),
                "moat_score": comp.get("moat_score"),
            }

        # News summary
        if "news" in step_results:
            news = step_results["news"]
            aggregated["news_summary"] = {
                "article_count": news.get("article_count", 0),
                "sentiment": news.get("sentiment"),
                "recent_headlines": [
                    a.get("title") for a in news.get("articles", [])[:3]
                ],
            }

        # Market context
        if "market" in step_results:
            aggregated["market_context"] = step_results["market"]

        # Data quality
        if "hunter" in step_results:
            aggregated["data_quality"] = step_results["hunter"]

        # Anomalies
        if "anomaly" in step_results:
            aggregated["anomalies"] = step_results["anomaly"]

        # Report reference
        if "report" in step_results and not step_results["report"].get("skipped"):
            aggregated["report"] = step_results["report"]

        return aggregated

    # -------------------------------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------------------------------

    def get_workflow_status(self, workflow_id: str) -> Optional[Dict]:
        """Get workflow status and results."""
        query = text("""
            SELECT * FROM workflow_executions WHERE workflow_id = :wid
        """)
        result = self.db.execute(query, {"wid": workflow_id})
        row = result.mappings().fetchone()

        if not row:
            return None

        response = {
            "workflow_id": row["workflow_id"],
            "workflow_type": row["workflow_type"],
            "workflow_name": row["workflow_name"],
            "entity_type": row["entity_type"],
            "entity_name": row["entity_name"],
            "status": row["status"],
            "progress": row["progress"],
            "total_steps": row["total_steps"],
            "completed_steps": row["completed_steps"],
            "failed_steps": row["failed_steps"],
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

        # Include step details
        steps = self._get_step_details(workflow_id)
        response["steps"] = steps

        if row["status"] in ("completed", "partial"):
            response["step_results"] = row["step_results"]
            response["aggregated_results"] = row["aggregated_results"]

        if row["error_message"]:
            response["error"] = row["error_message"]

        return response

    def _get_step_details(self, workflow_id: str) -> List[Dict]:
        """Get details for all workflow steps."""
        query = text("""
            SELECT step_id, agent_type, status, started_at, completed_at, duration_seconds, error_message
            FROM workflow_steps
            WHERE workflow_id = :wid
            ORDER BY id
        """)
        result = self.db.execute(query, {"wid": workflow_id})

        return [
            {
                "step_id": row["step_id"],
                "agent_type": row["agent_type"],
                "status": row["status"],
                "started_at": row["started_at"].isoformat() + "Z"
                if row["started_at"]
                else None,
                "completed_at": row["completed_at"].isoformat() + "Z"
                if row["completed_at"]
                else None,
                "duration_seconds": row["duration_seconds"],
                "error": row["error_message"],
            }
            for row in result.mappings()
        ]

    def get_templates(self) -> List[Dict]:
        """Get all available workflow templates."""
        query = text("""
            SELECT template_id, name, description, steps, estimated_duration_minutes, is_custom, created_at
            FROM workflow_templates
            ORDER BY is_custom, name
        """)
        result = self.db.execute(query)

        return [
            {
                "id": row["template_id"],
                "name": row["name"],
                "description": row["description"],
                "steps": row["steps"]
                if isinstance(row["steps"], list)
                else json.loads(row["steps"]),
                "estimated_duration_minutes": row["estimated_duration_minutes"],
                "is_custom": row["is_custom"],
                "created_at": row["created_at"].isoformat() + "Z"
                if row["created_at"]
                else None,
            }
            for row in result.mappings()
        ]

    def create_custom_workflow(
        self,
        name: str,
        description: str,
        steps: List[Dict],
        created_by: Optional[str] = None,
    ) -> str:
        """Create a custom workflow template."""
        template_id = f"custom_{uuid.uuid4().hex[:8]}"

        # Validate steps
        for step in steps:
            if "id" not in step or "agent" not in step:
                raise ValueError("Each step must have 'id' and 'agent' fields")
            if step["agent"] not in [a.value for a in AgentType]:
                raise ValueError(f"Unknown agent type: {step['agent']}")

        insert = text("""
            INSERT INTO workflow_templates (template_id, name, description, steps, is_custom, created_by)
            VALUES (:tid, :name, :desc, :steps, true, :created_by)
        """)

        self.db.execute(
            insert,
            {
                "tid": template_id,
                "name": name,
                "desc": description,
                "steps": json.dumps(steps),
                "created_by": created_by,
            },
        )
        self.db.commit()

        return template_id

    def list_workflows(
        self,
        status: Optional[str] = None,
        entity_name: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict]:
        """List workflow executions."""
        conditions = ["1=1"]
        params = {"limit": limit}

        if status:
            conditions.append("status = :status")
            params["status"] = status

        if entity_name:
            conditions.append("LOWER(entity_name) LIKE LOWER(:entity)")
            params["entity"] = f"%{entity_name}%"

        where = " AND ".join(conditions)

        query = text(f"""
            SELECT workflow_id, workflow_type, workflow_name, entity_name, status,
                   progress, total_steps, completed_steps, failed_steps,
                   created_at, completed_at
            FROM workflow_executions
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, params)

        return [
            {
                "workflow_id": row["workflow_id"],
                "workflow_type": row["workflow_type"],
                "workflow_name": row["workflow_name"],
                "entity_name": row["entity_name"],
                "status": row["status"],
                "progress": row["progress"],
                "total_steps": row["total_steps"],
                "completed_steps": row["completed_steps"],
                "failed_steps": row["failed_steps"],
                "created_at": row["created_at"].isoformat() + "Z"
                if row["created_at"]
                else None,
                "completed_at": row["completed_at"].isoformat() + "Z"
                if row["completed_at"]
                else None,
            }
            for row in result.mappings()
        ]

    def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        # Check current status
        query = text("SELECT status FROM workflow_executions WHERE workflow_id = :wid")
        result = self.db.execute(query, {"wid": workflow_id})
        row = result.fetchone()

        if not row:
            return False

        if row[0] not in ("pending", "running"):
            return False

        # Update to cancelled
        self.db.execute(
            text("""
            UPDATE workflow_executions
            SET status = 'cancelled', completed_at = NOW()
            WHERE workflow_id = :wid AND status IN ('pending', 'running')
        """),
            {"wid": workflow_id},
        )

        # Cancel pending steps
        self.db.execute(
            text("""
            UPDATE workflow_steps
            SET status = 'skipped', error_message = 'Workflow cancelled'
            WHERE workflow_id = :wid AND status = 'pending'
        """),
            {"wid": workflow_id},
        )

        self.db.commit()
        return True

    def get_stats(self) -> Dict:
        """Get workflow statistics."""
        stats_query = text("""
            SELECT
                COUNT(*) as total_workflows,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'partial') as partial,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                    FILTER (WHERE completed_at IS NOT NULL) as avg_duration
            FROM workflow_executions
        """)

        type_query = text("""
            SELECT workflow_type, COUNT(*) as count
            FROM workflow_executions
            GROUP BY workflow_type
            ORDER BY count DESC
            LIMIT 5
        """)

        stats = self.db.execute(stats_query).mappings().fetchone()
        type_dist = self.db.execute(type_query).mappings().fetchall()

        return {
            "workflows": {
                "total": stats["total_workflows"],
                "completed": stats["completed"],
                "partial": stats["partial"],
                "failed": stats["failed"],
                "running": stats["running"],
                "avg_duration_seconds": round(stats["avg_duration"], 2)
                if stats["avg_duration"]
                else None,
            },
            "by_type": {r["workflow_type"]: r["count"] for r in type_dist},
            "available_agents": [a.value for a in AgentType],
            "template_count": len(self.get_templates()),
        }

    def get_available_agents(self) -> List[Dict]:
        """Get list of available agents."""
        agent_info = {
            AgentType.COMPANY_RESEARCHER: {
                "name": "Company Researcher",
                "description": "Researches companies across all data sources",
            },
            AgentType.DUE_DILIGENCE: {
                "name": "Due Diligence",
                "description": "Generates comprehensive DD reports with risk analysis",
            },
            AgentType.NEWS_MONITOR: {
                "name": "News Monitor",
                "description": "Monitors and summarizes relevant news",
            },
            AgentType.COMPETITIVE_INTEL: {
                "name": "Competitive Intelligence",
                "description": "Analyzes competitive landscape and market position",
            },
            AgentType.DATA_HUNTER: {
                "name": "Data Hunter",
                "description": "Finds and fills missing data gaps",
            },
            AgentType.ANOMALY_DETECTOR: {
                "name": "Anomaly Detector",
                "description": "Detects unusual patterns in data",
            },
            AgentType.REPORT_WRITER: {
                "name": "Report Writer",
                "description": "Generates natural language reports",
            },
            AgentType.MARKET_SCANNER: {
                "name": "Market Scanner",
                "description": "Scans market for trends and opportunities",
            },
        }

        return [
            {
                "id": agent.value,
                "name": agent_info[agent]["name"],
                "description": agent_info[agent]["description"],
            }
            for agent in AgentType
        ]
