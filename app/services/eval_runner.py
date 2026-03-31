"""
Eval Runner — orchestrates eval suite execution for SPEC_039 / PLAN_041.

EvalRunner.run_suite(suite_id, db):
  1. Load suite + active cases
  2. Capture output via the appropriate capture mode
  3. Score each case with EvalScorer
  4. Detect regressions vs rolling 5-run average
  5. Persist EvalRun + EvalResult rows
  6. Return the completed EvalRun

Four capture modes (dispatched by EvalSuite.eval_mode):
  db_snapshot   — reads current DB state for the entity; zero cost
  api_response  — HTTP call to running API at localhost:8001
  agent_output  — imports + calls agent class, captures CollectionResult
  report_output — calls report endpoint, captures HTML/JSON

Capture failures are non-fatal: the run is marked "failed" with an error
message but never raises — the eval system must never break ingestion jobs.
"""
from __future__ import annotations

import importlib
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from statistics import mean
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.core.eval_models import EvalCase, EvalResult, EvalRun, EvalSuite
from app.services.eval_scorer import CapturedOutput, EvalScorer, ScorerResult

logger = logging.getLogger(__name__)

_API_BASE = "http://localhost:8001"


# ===========================================================================
# Capture helpers
# ===========================================================================

class DBSnapshotCapture:
    """Reads current DB state for the entity — no agent or API call."""

    def __init__(self, suite: EvalSuite, entity_id: int | None, entity_type: str | None):
        self.suite = suite
        self.entity_id = entity_id
        self.entity_type = entity_type

    def capture(self, db: Session) -> CapturedOutput:
        raw: dict = {}
        try:
            et = (self.entity_type or self.suite.binding_target or "").lower()
            if "company" in et:
                raw = self._snapshot_company(db)
            elif "pe_firm" in et or "pe" in et:
                raw = self._snapshot_pe_firm(db)
            elif "three_pl" in et or "3pl" in et:
                raw = self._snapshot_three_pl(db)
        except Exception as exc:
            logger.warning("DBSnapshotCapture failed for entity %s: %s", self.entity_id, exc)
            return CapturedOutput(mode="db_snapshot", entity_id=self.entity_id,
                                  entity_type=self.entity_type, raw={}, error=str(exc))
        return CapturedOutput(mode="db_snapshot", entity_id=self.entity_id,
                              entity_type=self.entity_type, raw=raw)

    def _snapshot_company(self, db: Session) -> dict:
        from app.core.people_models import CompanyPerson, OrgChartSnapshot
        snap = (
            db.query(OrgChartSnapshot)
            .filter(OrgChartSnapshot.company_id == self.entity_id)
            .order_by(OrgChartSnapshot.snapshot_date.desc())
            .first()
        )
        people_count = (
            db.query(CompanyPerson)
            .filter(CompanyPerson.company_id == self.entity_id,
                    CompanyPerson.is_current.is_(True))
            .count()
        )
        return {
            "total_people": snap.total_people if snap else people_count,
            "max_depth": snap.max_depth if snap else 0,
            "snapshot_date": str(snap.snapshot_date) if snap else None,
            "departments": (snap.chart_data or {}).get("departments", []) if snap else [],
        }

    def _snapshot_pe_firm(self, db: Session) -> dict:
        from app.core.pe_models import PEDeal, PEPortfolioCompany
        deal_count = db.query(PEDeal).filter(PEDeal.firm_id == self.entity_id).count()
        portfolio_count = db.query(PEPortfolioCompany).filter(
            PEPortfolioCompany.firm_id == self.entity_id
        ).count()
        return {"deal_count": deal_count, "portfolio_count": portfolio_count}

    def _snapshot_three_pl(self, db: Session) -> dict:
        from app.core.models_site_intel import ThreePLCompany
        total = db.query(ThreePLCompany).count()
        enriched = db.query(ThreePLCompany).filter(
            ThreePLCompany.website.isnot(None)
        ).count()
        return {"total": total, "enriched_with_website": enriched}


class APIResponseCapture:
    """Makes a live HTTP call to the running API and captures the JSON response."""

    def __init__(self, suite: EvalSuite, entity_id: int | None, params: dict | None = None):
        self.suite = suite
        self.entity_id = entity_id
        self.extra_params = params or {}

    def capture(self) -> CapturedOutput:
        endpoint = self.suite.binding_target or ""
        # Substitute {company_id}, {firm_id}, etc. placeholders
        if self.entity_id:
            for placeholder in ("{company_id}", "{firm_id}", "{id}", "{suite_id}"):
                endpoint = endpoint.replace(placeholder, str(self.entity_id))

        url = f"{_API_BASE}{endpoint}"
        method = self.extra_params.get("method", "GET").upper()
        body = self.extra_params.get("body")

        start = time.monotonic()
        try:
            with httpx.Client(timeout=30.0) as client:
                if method == "POST":
                    resp = client.post(url, json=body or {})
                else:
                    resp = client.get(url)
            latency_ms = (time.monotonic() - start) * 1000
            try:
                raw = resp.json()
            except Exception:
                raw = {"_text": resp.text[:2000]}
            return CapturedOutput(
                mode="api_response",
                entity_id=self.entity_id,
                raw=raw,
                status_code=resp.status_code,
                latency_ms=latency_ms,
            )
        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            logger.warning("APIResponseCapture failed for %s: %s", url, exc)
            return CapturedOutput(
                mode="api_response",
                entity_id=self.entity_id,
                raw={},
                status_code=None,
                latency_ms=latency_ms,
                error=str(exc),
            )


class AgentOutputCapture:
    """
    Imports the agent class from binding_target and calls its collection method.
    Captures CollectionResult / DeepCollectionResult before DB write.

    binding_target format:
      "app.sources.people_collection.website_agent.WebsiteAgent"
    """

    def __init__(self, suite: EvalSuite, entity_id: int | None):
        self.suite = suite
        self.entity_id = entity_id

    def capture(self, db: Session) -> CapturedOutput:
        target = self.suite.binding_target or ""
        try:
            module_path, class_name = target.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            agent_cls = getattr(mod, class_name)
            agent = agent_cls()
            # Try common collect method names
            for method_name in ("collect", "run", "collect_company"):
                if hasattr(agent, method_name):
                    result = getattr(agent, method_name)(self.entity_id, db=db)
                    raw = result.to_dict() if hasattr(result, "to_dict") else (
                        result.__dict__ if hasattr(result, "__dict__") else {"result": str(result)}
                    )
                    return CapturedOutput(
                        mode="agent_output",
                        entity_id=self.entity_id,
                        raw=raw,
                    )
            return CapturedOutput(
                mode="agent_output",
                entity_id=self.entity_id,
                raw={},
                error=f"No collect/run method found on {class_name}",
            )
        except Exception as exc:
            logger.warning("AgentOutputCapture failed for %s (entity %s): %s", target, self.entity_id, exc)
            return CapturedOutput(
                mode="agent_output",
                entity_id=self.entity_id,
                raw={},
                error=str(exc),
            )


class ReportOutputCapture:
    """Calls a report generator endpoint and captures the HTML/JSON output."""

    def __init__(self, suite: EvalSuite, entity_id: int | None, params: dict | None = None):
        self.suite = suite
        self.entity_id = entity_id
        self.extra_params = params or {}

    def capture(self) -> CapturedOutput:
        endpoint = self.suite.binding_target or ""
        if self.entity_id:
            for placeholder in ("{company_id}", "{firm_id}", "{id}"):
                endpoint = endpoint.replace(placeholder, str(self.entity_id))

        url = f"{_API_BASE}{endpoint}"
        body = self.extra_params.get("body", {})
        if self.entity_id and "company_id" not in body:
            body = {**body, "company_id": self.entity_id}

        start = time.monotonic()
        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(url, json=body)
            latency_ms = (time.monotonic() - start) * 1000

            content_type = resp.headers.get("content-type", "")
            if "html" in content_type or resp.text.strip().startswith("<"):
                raw = resp.text
            else:
                try:
                    raw = resp.json()
                    # If report is in a nested field, extract it
                    if isinstance(raw, dict):
                        for key in ("html", "content", "report", "body"):
                            if key in raw and isinstance(raw[key], str):
                                raw = raw[key]
                                break
                except Exception:
                    raw = resp.text

            return CapturedOutput(
                mode="report_output",
                entity_id=self.entity_id,
                raw=raw,
                status_code=resp.status_code,
                latency_ms=latency_ms,
            )
        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            logger.warning("ReportOutputCapture failed for %s: %s", url, exc)
            return CapturedOutput(
                mode="report_output",
                entity_id=self.entity_id,
                raw="",
                status_code=None,
                latency_ms=latency_ms,
                error=str(exc),
            )


# ===========================================================================
# Regression detection
# ===========================================================================

def _detect_regressions(
    suite_id: int,
    current_run_id: int,
    case_results: list[tuple[EvalCase, ScorerResult]],
    db: Session,
    window: int = 5,
) -> list[dict]:
    """
    Compare per-case scores against rolling average of last `window` completed runs.
    Returns list of regression dicts for cases that regressed.
    Requires minimum 2 prior runs before firing.
    """
    prior_runs = (
        db.query(EvalRun)
        .filter(
            EvalRun.suite_id == suite_id,
            EvalRun.status == "completed",
            EvalRun.id != current_run_id,
        )
        .order_by(EvalRun.triggered_at.desc())
        .limit(window)
        .all()
    )
    if len(prior_runs) < 2:
        return []

    prior_run_ids = [r.id for r in prior_runs]
    regressions = []

    for case, result in case_results:
        prior_results = (
            db.query(EvalResult)
            .filter(
                EvalResult.case_id == case.id,
                EvalResult.run_id.in_(prior_run_ids),
            )
            .all()
        )
        prior_scores = [float(r.score) for r in prior_results if r.score is not None]
        if not prior_scores:
            continue

        avg = mean(prior_scores)
        current_score = float(result.score)

        # Tier 1: if it previously passed (score=100) and now fails (score=0)
        if case.tier == 1 and not result.passed and any(s == 100 for s in prior_scores):
            regressions.append({
                "case_id": case.id,
                "case_name": case.name,
                "tier": case.tier,
                "prev_avg_score": round(avg, 1),
                "current_score": current_score,
                "drop_pct": 100.0,
                "reason": "Tier 1 case previously passed, now fails",
            })
            continue

        # Tier 2/3: score drop exceeds threshold
        if avg > 0:
            drop_pct = (avg - current_score) / avg * 100
            threshold = float(case.regression_threshold_pct or 15.0)
            if drop_pct > threshold:
                regressions.append({
                    "case_id": case.id,
                    "case_name": case.name,
                    "tier": case.tier,
                    "prev_avg_score": round(avg, 1),
                    "current_score": current_score,
                    "drop_pct": round(drop_pct, 1),
                    "reason": f"Score dropped {drop_pct:.1f}% (threshold {threshold}%)",
                })

    return regressions


# ===========================================================================
# Composite score
# ===========================================================================

def _fire_regression_webhook(run: "EvalRun", suite: "EvalSuite", regressions: list) -> None:
    """POST regression payload to EVAL_REGRESSION_WEBHOOK_URL (fire-and-forget, never raises)."""
    import os
    url = os.environ.get("EVAL_REGRESSION_WEBHOOK_URL", "").strip()
    if not url:
        return
    try:
        payload = {
            "suite_name": suite.name,
            "run_id": run.id,
            "composite_score": float(run.composite_score or 0),
            "regressions": regressions,
            "triggered_by": run.triggered_by,
            "triggered_at": run.triggered_at.isoformat() if run.triggered_at else None,
        }
        with httpx.Client(timeout=5.0) as client:
            client.post(url, json=payload)
        logger.info("EvalRunner: regression webhook fired for run %s → %s", run.id, url)
    except Exception as exc:
        logger.warning("EvalRunner: regression webhook failed (non-fatal): %s", exc)


def _compute_composite(case_results: list[tuple[EvalCase, ScorerResult]]) -> dict:
    """
    composite = 0 if any Tier 1 fails
    else 0.50*avg_t1 + 0.30*avg_t2 + 0.20*avg_t3
    """
    t1 = [(c, r) for c, r in case_results if c.tier == 1]
    t2 = [(c, r) for c, r in case_results if c.tier == 2]
    t3 = [(c, r) for c, r in case_results if c.tier == 3]

    t1_pass_rate = (sum(1 for _, r in t1 if r.passed) / len(t1) * 100) if t1 else 100.0
    t1_all_pass = all(r.passed for _, r in t1) if t1 else True

    avg_t1 = mean([float(r.score) for _, r in t1]) if t1 else 100.0
    avg_t2 = mean([float(r.score) for _, r in t2]) if t2 else 100.0
    avg_t3 = mean([float(r.score) for _, r in t3]) if t3 else 100.0

    if not t1_all_pass:
        composite = 0.0
    else:
        composite = round(0.50 * avg_t1 + 0.30 * avg_t2 + 0.20 * avg_t3, 2)

    return {
        "composite": composite,
        "t1_pass_rate": round(t1_pass_rate, 2),
        "avg_t1": round(avg_t1, 2),
        "avg_t2": round(avg_t2, 2),
        "avg_t3": round(avg_t3, 2),
    }


# ===========================================================================
# EvalRunner
# ===========================================================================

class EvalRunner:
    """
    Orchestrates a full eval suite run.

    Usage:
        runner = EvalRunner()
        run = runner.run_suite(suite_id=1, db=db, entity_id=142)
    """

    def run_suite(
        self,
        suite_id: int,
        db: Session,
        entity_id: int | None = None,
        triggered_by: str = "manual",
        existing_run_id: int | None = None,
    ) -> EvalRun:
        suite = db.get(EvalSuite, suite_id)
        if not suite:
            raise ValueError(f"EvalSuite {suite_id} not found")

        cases = (
            db.query(EvalCase)
            .filter(EvalCase.suite_id == suite_id, EvalCase.is_active.is_(True))
            .all()
        )

        # Reuse pre-created run (from API) or create a new one
        if existing_run_id is not None:
            run = db.get(EvalRun, existing_run_id)
            if run is None:
                raise ValueError(f"EvalRun {existing_run_id} not found")
            run.cases_total = len(cases)
            db.commit()
            db.refresh(run)
        else:
            run = EvalRun(
                suite_id=suite_id,
                status="running",
                triggered_by=triggered_by,
                triggered_at=datetime.utcnow(),
                cases_total=len(cases),
            )
            db.add(run)
            db.commit()
            db.refresh(run)

        logger.info("EvalRunner: starting suite '%s' (run %s), %d cases", suite.name, run.id, len(cases))

        try:
            # Step 1: capture output
            output = self._capture_output(suite, entity_id or cases[0].entity_id if cases else None, db)

            # Step 2: score each case
            case_results: list[tuple[EvalCase, ScorerResult]] = []
            errors: list[str] = []
            for case in cases:
                # Use entity_id override or fall back to case's own entity_id
                case_output = CapturedOutput(
                    mode=output.mode,
                    entity_id=entity_id or case.entity_id or output.entity_id,
                    entity_type=case.entity_type or output.entity_type,
                    raw=output.raw,
                    status_code=output.status_code,
                    latency_ms=output.latency_ms,
                    cost_usd=output.cost_usd,
                    error=output.error,
                )
                try:
                    result = EvalScorer.score(case, case_output, db)
                except Exception as exc:
                    result = ScorerResult(passed=False, score=0.0,
                                          failure_reason=f"Scorer exception: {exc}")
                    errors.append(f"case {case.id} ({case.name}): {exc}")
                case_results.append((case, result))

            # Step 3: compute composite score
            scores = _compute_composite(case_results)

            # Step 4: persist EvalResult rows (before regression detection which reads them)
            passed_count = 0
            failed_count = 0
            total_llm_cost = 0.0
            for case, result in case_results:
                er = EvalResult(
                    run_id=run.id,
                    case_id=case.id,
                    passed=result.passed,
                    score=result.score,
                    partial_credit=result.partial_credit,
                    actual_value=result.actual_value,
                    expected_value=result.expected_value,
                    failure_reason=result.failure_reason,
                    evaluated_at=datetime.utcnow(),
                    llm_judge_prompt=result.llm_judge_prompt,
                    llm_judge_response=result.llm_judge_response,
                    llm_judge_score=result.score if result.llm_judge_prompt else None,
                    llm_judge_reasoning=result.llm_judge_reasoning,
                )
                db.add(er)
                total_llm_cost += result.llm_cost_usd
                if result.passed:
                    passed_count += 1
                else:
                    failed_count += 1
            db.commit()

            # Step 5: detect regressions
            regressions = _detect_regressions(suite_id, run.id, case_results, db)

            # Step 6: update run record
            run.status = "completed"
            run.completed_at = datetime.utcnow()
            run.composite_score = scores["composite"]
            run.tier1_pass_rate = scores["t1_pass_rate"]
            run.tier2_avg_score = scores["avg_t2"]
            run.tier3_avg_score = scores["avg_t3"]
            run.is_regression = len(regressions) > 0
            run.regression_details = regressions
            if run.is_regression:
                _fire_regression_webhook(run, suite, regressions)
            run.captured_output = (
                output.raw if isinstance(output.raw, (dict, list))
                else {"_raw": str(output.raw)[:5000]}
            )
            run.cases_passed = passed_count
            run.cases_failed = failed_count
            run.errors = errors or None
            run.llm_cost_usd = total_llm_cost
            db.commit()

            logger.info(
                "EvalRunner: suite '%s' run %s complete — score=%.1f, t1_pass=%.0f%%, regression=%s",
                suite.name, run.id, scores["composite"], scores["t1_pass_rate"], run.is_regression,
            )

        except Exception as exc:
            logger.error("EvalRunner: suite %s run %s failed: %s", suite_id, run.id, exc)
            run.status = "failed"
            run.completed_at = datetime.utcnow()
            run.errors = [str(exc)]
            db.commit()

        return run

    def _capture_output(
        self,
        suite: EvalSuite,
        entity_id: int | None,
        db: Session,
    ) -> CapturedOutput:
        mode = suite.eval_mode or "db_snapshot"
        try:
            if mode == "db_snapshot":
                return DBSnapshotCapture(suite, entity_id, suite.binding_target).capture(db)
            elif mode == "api_response":
                return APIResponseCapture(suite, entity_id).capture()
            elif mode == "agent_output":
                return AgentOutputCapture(suite, entity_id).capture(db)
            elif mode == "report_output":
                return ReportOutputCapture(suite, entity_id).capture()
            else:
                logger.warning("EvalRunner: unknown eval_mode '%s', defaulting to db_snapshot", mode)
                return DBSnapshotCapture(suite, entity_id, suite.binding_target).capture(db)
        except Exception as exc:
            logger.error("EvalRunner: capture failed (mode=%s): %s", mode, exc)
            return CapturedOutput(mode=mode, entity_id=entity_id, raw={}, error=str(exc))

    def run_priority(
        self,
        priority: int,
        db: Session,
        triggered_by: str = "schedule",
    ) -> list[EvalRun]:
        """Run all active suites at the given priority level."""
        suites = (
            db.query(EvalSuite)
            .filter(EvalSuite.priority == priority, EvalSuite.is_active.is_(True))
            .all()
        )
        logger.info("EvalRunner: running %d P%d suites", len(suites), priority)
        runs = []
        for suite in suites:
            try:
                run = self.run_suite(suite.id, db, triggered_by=triggered_by)
                runs.append(run)
            except Exception as exc:
                logger.error("EvalRunner: P%d suite %s (%s) failed: %s",
                             priority, suite.id, suite.name, exc)
        return runs


# ---------------------------------------------------------------------------
# Module-level scheduled job functions (APScheduler requires top-level refs)
# ---------------------------------------------------------------------------

def scheduled_eval_p1() -> None:
    """APScheduler job: run all P1 eval suites (daily)."""
    from app.core.database import get_session_factory
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        EvalRunner().run_priority(1, db, triggered_by="schedule")
    except Exception as exc:
        logger.error("Scheduled eval P1 failed: %s", exc)
    finally:
        db.close()


def scheduled_eval_p2() -> None:
    """APScheduler job: run all P2 eval suites (weekly)."""
    from app.core.database import get_session_factory
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        EvalRunner().run_priority(2, db, triggered_by="schedule")
    except Exception as exc:
        logger.error("Scheduled eval P2 failed: %s", exc)
    finally:
        db.close()


def scheduled_eval_p3() -> None:
    """APScheduler job: run all P3 eval suites (monthly)."""
    from app.core.database import get_session_factory
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        EvalRunner().run_priority(3, db, triggered_by="schedule")
    except Exception as exc:
        logger.error("Scheduled eval P3 failed: %s", exc)
    finally:
        db.close()
