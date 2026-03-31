"""
Eval Builder — Database Models.

Stores eval suites, cases, runs, and per-case results for the agentic
pipeline evaluation framework (PLAN_041 / SPEC_039).

Tables:
- eval_suites:  A collection of test assertions bound to one agent or API endpoint
- eval_cases:   Individual assertions within a suite (editable, with edit history)
- eval_runs:    One record per suite execution, with composite scores + regression flag
- eval_results: One record per case per run — the detailed per-assertion verdict
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from app.core.models import Base


# =============================================================================
# EVAL SUITES
# =============================================================================


class EvalSuite(Base):
    """
    A collection of eval cases bound to one agent class or API endpoint.

    binding_type + binding_target define what is being evaluated:
      "agent"  + "app.sources.people_collection.website_agent.WebsiteAgent"
      "api"    + "/api/v1/people-jobs/deep-collect/{company_id}"
      "report" + "/api/v1/people-reports/management-assessment"
      "db"     + "company"  (reads current DB state, no agent invocation)

    eval_mode controls how output is captured before scoring:
      "db_snapshot"   — read current DB for entity (fast, free)
      "api_response"  — HTTP call to running API, evaluate JSON response
      "agent_output"  — call agent directly, capture CollectionResult before DB write
      "report_output" — call report endpoint, parse HTML/JSON output

    priority drives schedule cadence and dashboard ordering:
      1 = daily (core product, demo-critical)
      2 = weekly (demo + research workflows)
      3 = monthly (background pipelines)
    """

    __tablename__ = "eval_suites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(500), nullable=False, unique=True)
    description = Column(Text)
    domain = Column(String(100), index=True)  # people, pe, reports, research, dd, etc.

    # Agent / API binding
    binding_type = Column(String(50), nullable=False)
    # "agent" | "api" | "report" | "db"

    binding_target = Column(String(500), nullable=False)
    # agent: "app.sources.people_collection.website_agent.WebsiteAgent"
    # api:   "/api/v1/people-jobs/deep-collect/{company_id}"
    # report:"/api/v1/people-reports/management-assessment"
    # db:    entity type string e.g. "company"

    eval_mode = Column(String(50), nullable=False, default="db_snapshot")
    # "db_snapshot" | "api_response" | "agent_output" | "report_output"

    priority = Column(Integer, nullable=False, default=2)
    # 1 = P1 daily | 2 = P2 weekly | 3 = P3 monthly

    schedule_cron = Column(String(100))
    # e.g. "0 9 * * 1" = every Monday 9am UTC. NULL = on-demand only.

    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    __table_args__ = (
        Index("ix_eval_suites_priority_active", "priority", "is_active"),
    )


# =============================================================================
# EVAL CASES
# =============================================================================


class EvalCase(Base):
    """
    One assertion within an eval suite.

    assertion_type is a string key that maps to a scorer function in EvalScorer.
    assertion_params is a JSON dict of parameters for that scorer.

    Tier controls scoring weight and regression behaviour:
      1 = hard — must pass; any failure zeroes the composite score
      2 = soft — partial credit proportional to closeness to expected
      3 = LLM judge — subjective quality rated by an LLM 1-10

    Edit history: previous_params / edited_at / edit_reason capture the last
    change so the dashboard can show what changed without a full audit log table.
    Past eval_results are never retroactively changed.
    """

    __tablename__ = "eval_cases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suite_id = Column(Integer, ForeignKey("eval_suites.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(String(500), nullable=False)
    description = Column(Text)

    # What entity this assertion is about
    entity_type = Column(String(50))
    # "company" | "pe_firm" | "three_pl" | "lp" | "county" | "report" | "api_response"

    entity_id = Column(Integer)       # e.g. industrial_companies.id; NULL for api_response mode
    entity_name = Column(String(500)) # denormalized display label

    # The assertion
    assertion_type = Column(String(100), nullable=False)
    assertion_params = Column(JSON, nullable=False, default=dict)

    # Scoring tier
    tier = Column(Integer, nullable=False, default=1)
    # 1 = hard | 2 = soft | 3 = LLM judge

    weight = Column(Numeric(5, 3), default=1.0)
    regression_threshold_pct = Column(Numeric(5, 2), default=15.0)

    # Edit history (last edit only — sufficient for dashboard display)
    previous_params = Column(JSON)
    edited_at = Column(DateTime)
    edit_reason = Column(String(500))

    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    __table_args__ = (
        Index("ix_eval_cases_suite_active", "suite_id", "is_active"),
    )


# =============================================================================
# EVAL RUNS
# =============================================================================


class EvalRun(Base):
    """
    One record per suite execution.

    captured_output stores the raw agent / API / report output for replay and
    debugging — the scorer receives this rather than querying the DB directly.

    composite_score formula (computed after all cases):
      tier1_all_pass = all tier-1 results passed
      composite = 0 if not tier1_all_pass else
                  round(0.50*avg_tier1 + 0.30*avg_tier2 + 0.20*avg_tier3)

    is_regression is set by _detect_regressions() after scoring:
      - Any previously-passing Tier 1 case now fails → always a regression
      - Any Tier 2/3 case drops >regression_threshold_pct from 5-run avg
      - Requires minimum 2 prior completed runs to fire
    """

    __tablename__ = "eval_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suite_id = Column(Integer, ForeignKey("eval_suites.id", ondelete="CASCADE"), nullable=False, index=True)

    # Lifecycle
    status = Column(String(50), nullable=False, default="running")
    # "running" | "completed" | "failed"

    triggered_by = Column(String(100), default="manual")
    # "manual" | "schedule" | "api" | "priority_run"

    triggered_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime)

    # Aggregate scores (NULL while running)
    composite_score = Column(Numeric(6, 2))
    tier1_pass_rate = Column(Numeric(6, 2))   # % of tier-1 cases that passed
    tier2_avg_score = Column(Numeric(6, 2))   # avg score of tier-2 cases
    tier3_avg_score = Column(Numeric(6, 2))   # avg score of tier-3 cases

    # Regression
    is_regression = Column(Boolean, default=False)
    regression_details = Column(JSON)
    # [{case_id, case_name, prev_avg_score, current_score, drop_pct}]

    # Raw output captured before scoring (for replay / debug)
    captured_output = Column(JSON)

    # LLM cost for any Tier 3 judge calls in this run
    llm_cost_usd = Column(Numeric(10, 4), default=0.0)

    # Counts (populated on completion)
    cases_total = Column(Integer, default=0)
    cases_passed = Column(Integer, default=0)
    cases_failed = Column(Integer, default=0)
    errors = Column(JSON)  # list of error strings from case scoring

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_eval_runs_suite_triggered", "suite_id", "triggered_at"),
        Index("ix_eval_runs_regression", "is_regression", "triggered_at"),
    )


# =============================================================================
# EVAL RESULTS
# =============================================================================


class EvalResult(Base):
    """
    One record per case per run — the detailed per-assertion verdict.

    actual_value and expected_value store JSON snapshots of what was found
    vs what was expected, enabling the dashboard to show a clear diff.

    LLM judge fields (tier=3 only): the full prompt, raw response, extracted
    score (0-100), and reasoning are stored for transparency and debugging.
    """

    __tablename__ = "eval_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("eval_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    case_id = Column(Integer, ForeignKey("eval_cases.id", ondelete="CASCADE"), nullable=False, index=True)

    # Verdict
    passed = Column(Boolean, nullable=False)
    score = Column(Numeric(6, 2))          # 0–100; Tier 1: 0 or 100
    partial_credit = Column(Boolean, default=False)

    # Evidence
    actual_value = Column(JSON)            # what the agent / API actually produced
    expected_value = Column(JSON)          # what the case params expected
    failure_reason = Column(Text)          # human-readable explanation if failed

    # LLM judge (Tier 3 only — NULL for Tier 1/2)
    llm_judge_prompt = Column(Text)
    llm_judge_response = Column(Text)
    llm_judge_score = Column(Numeric(6, 2))
    llm_judge_reasoning = Column(Text)

    evaluated_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("run_id", "case_id", name="uq_eval_result_run_case"),
        Index("ix_eval_results_run", "run_id"),
    )
