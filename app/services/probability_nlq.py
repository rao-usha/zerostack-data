"""
Deal Probability Engine — Natural Language Query (SPEC 047, PLAN_059 Phase 3).

Parses user queries like "show me healthcare companies with probability above 0.6"
into structured filter objects, validates them against an allow-list, and
executes safe ORM queries against TxnProbCompany/TxnProbScore.

The LLM NEVER generates raw SQL — only filter objects that the backend
validates against the ALLOWED_FIELDS whitelist. Keyword fallback kicks in
when the LLM is unavailable or the parse fails.

Follows the `deal_radar_nlq.py` pattern.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.probability_models import TxnProbCompany, TxnProbScore

logger = logging.getLogger(__name__)


# Whitelist: field → (type, target_model)
# target_model is "score" (TxnProbScore) or "company" (TxnProbCompany)
ALLOWED_FIELDS: Dict[str, Dict[str, str]] = {
    "probability": {"type": "float", "target": "score"},
    "raw_composite_score": {"type": "float", "target": "score"},
    "grade": {"type": "string", "target": "score"},
    "active_signal_count": {"type": "int", "target": "score"},
    "sector": {"type": "string", "target": "company"},
    "hq_state": {"type": "string", "target": "company"},
    "industry": {"type": "string", "target": "company"},
    "ownership_status": {"type": "string", "target": "company"},
    "employee_count_est": {"type": "int", "target": "company"},
}

ALLOWED_OPERATORS = {">=", "<=", ">", "<", "=", "!="}

ALLOWED_SORT_FIELDS = {"probability", "raw_composite_score", "active_signal_count"}


NLQ_SYSTEM_PROMPT = """You are a query parser for a PE deal-probability system.
The system stores one row per company with these numeric/string fields:

Score fields (from TxnProbScore):
- probability: 0-1 (calibrated P(transaction in 6-12 months))
- raw_composite_score: 0-100 composite
- grade: "A", "B", "C", "D", or "F"
- active_signal_count: count of signals above 60

Company fields (from TxnProbCompany):
- sector: e.g. "Healthcare", "Technology", "Industrial", "Finance"
- hq_state: 2-letter state code ("CA", "NY", ...)
- industry: more specific than sector
- ownership_status: "PE-Backed", "VC-Backed", "Private", "Public"
- employee_count_est: integer

Parse the user's question into JSON with EXACTLY this shape:
{
  "filters": [{"field": "<name>", "op": "<op>", "value": <value>}],
  "sort_by": "<probability|raw_composite_score|active_signal_count>",
  "sort_dir": "desc",
  "explanation": "<one sentence>"
}

Rules:
- Operators: >=, <=, >, <, =, != only
- "high probability" → probability >= 0.6
- "very high probability" → probability >= 0.8
- "ready to exit" → probability >= 0.7
- "top" without modifier → sort desc, no extra filter
- Numerical thresholds: match the user's stated number
- Default sort: probability desc
- Return ONLY valid JSON — no markdown, no backticks, no commentary."""


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class NLQFilter:
    field: str
    op: str
    value: Any


@dataclass
class NLQResult:
    query: str
    filters: List[Dict[str, Any]] = field(default_factory=list)
    results: List[Dict[str, Any]] = field(default_factory=list)
    explanation: str = ""
    total_matches: int = 0


# ---------------------------------------------------------------------------
# Validation (pure, testable)
# ---------------------------------------------------------------------------


def validate_filter(f: Dict) -> Optional[NLQFilter]:
    """Whitelist one filter dict. Returns None if invalid."""
    fld = f.get("field", "")
    op = f.get("op", "")
    value = f.get("value")

    if fld not in ALLOWED_FIELDS or op not in ALLOWED_OPERATORS or value is None:
        return None

    expected_type = ALLOWED_FIELDS[fld]["type"]
    try:
        if expected_type == "float":
            value = float(value)
        elif expected_type == "int":
            value = int(value)
        elif expected_type == "string":
            value = str(value)
    except (TypeError, ValueError):
        return None

    return NLQFilter(field=fld, op=op, value=value)


def validate_filters(raw: List[Dict]) -> List[NLQFilter]:
    """Drop any filters that fail validation. Silent by design."""
    return [v for v in (validate_filter(f) for f in raw or []) if v]


def keyword_fallback(query: str) -> Dict:
    """Best-effort filter synthesis from keywords when LLM unavailable."""
    q = (query or "").lower()
    filters: List[Dict] = []

    # Probability / grade
    if "very high" in q:
        filters.append({"field": "probability", "op": ">=", "value": 0.8})
    elif "high prob" in q or "ready to exit" in q:
        filters.append({"field": "probability", "op": ">=", "value": 0.7})
    elif "hot" in q:
        filters.append({"field": "probability", "op": ">=", "value": 0.6})
    elif "grade a" in q or "a grade" in q:
        filters.append({"field": "grade", "op": "=", "value": "A"})
    elif "grade b" in q:
        filters.append({"field": "grade", "op": "=", "value": "B"})

    # Sector
    for sector in ("Healthcare", "Technology", "Industrial", "Finance", "Retail", "Energy"):
        if sector.lower() in q:
            filters.append({"field": "sector", "op": "=", "value": sector})
            break

    # Ownership
    if "pe-backed" in q or "pe backed" in q:
        filters.append({"field": "ownership_status", "op": "=", "value": "PE-Backed"})
    elif "vc-backed" in q or "vc backed" in q:
        filters.append({"field": "ownership_status", "op": "=", "value": "VC-Backed"})

    return {
        "filters": filters,
        "sort_by": "probability",
        "sort_dir": "desc",
        "explanation": f"Keyword match for: {query}",
    }


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class ProbabilityNLQ:
    """Natural-language query engine for transaction probability data."""

    def __init__(self, db: Session):
        self.db = db

    async def query(self, user_query: str, limit: int = 50) -> NLQResult:
        user_query = (user_query or "").strip()

        if not user_query:
            rows = self._execute([], sort_by="probability", sort_dir="desc", limit=limit)
            return NLQResult(
                query="",
                filters=[],
                results=rows,
                explanation="Showing all scored companies.",
                total_matches=len(rows),
            )

        parsed = await self._parse(user_query)
        raw_filters = parsed.get("filters", [])
        sort_by = parsed.get("sort_by", "probability")
        sort_dir = parsed.get("sort_dir", "desc")
        explanation = parsed.get("explanation", "Filtered results")

        valid = validate_filters(raw_filters)

        if sort_by not in ALLOWED_SORT_FIELDS:
            sort_by = "probability"

        rows = self._execute(valid, sort_by=sort_by, sort_dir=sort_dir, limit=limit)

        return NLQResult(
            query=user_query,
            filters=[
                {"field": f.field, "op": f.op, "value": f.value} for f in valid
            ],
            results=rows,
            explanation=explanation,
            total_matches=len(rows),
        )

    # ---- Parsing ------------------------------------------------------

    async def _parse(self, user_query: str) -> Dict:
        """Try the LLM first; fall back to keywords on any failure."""
        try:
            from app.agentic.llm_client import LLMClient

            llm = LLMClient(
                provider="anthropic",
                model="claude-3-5-haiku-20241022",
                max_tokens=400,
                temperature=0.0,
            )
            response = await llm.complete(
                prompt=user_query, system_prompt=NLQ_SYSTEM_PROMPT
            )
            parsed = response.parse_json()
            if parsed:
                return parsed
            try:
                return json.loads(response.content.strip())
            except Exception:
                pass
            logger.debug("NLQ: LLM returned no parsable JSON — falling back")
        except Exception as exc:
            logger.debug("NLQ: LLM unavailable — falling back (%s)", exc)

        return keyword_fallback(user_query)

    # ---- Execution ----------------------------------------------------

    def _execute(
        self,
        filters: List[NLQFilter],
        sort_by: str,
        sort_dir: str,
        limit: int,
    ) -> List[Dict]:
        """Run an ORM query with the validated filters."""
        # Latest score per company
        latest_subq = (
            self.db.query(
                TxnProbScore.company_id,
                func.max(TxnProbScore.scored_at).label("latest_at"),
            )
            .group_by(TxnProbScore.company_id)
            .subquery()
        )
        q = (
            self.db.query(TxnProbScore, TxnProbCompany)
            .join(
                latest_subq,
                (TxnProbScore.company_id == latest_subq.c.company_id)
                & (TxnProbScore.scored_at == latest_subq.c.latest_at),
            )
            .join(TxnProbCompany, TxnProbCompany.id == TxnProbScore.company_id)
        )

        for f in filters:
            target = ALLOWED_FIELDS[f.field]["target"]
            model = TxnProbScore if target == "score" else TxnProbCompany
            col = getattr(model, f.field, None)
            if col is None:
                continue
            q = self._apply_op(q, col, f.op, f.value)

        sort_col = getattr(TxnProbScore, sort_by, TxnProbScore.probability)
        q = q.order_by(sort_col.desc() if sort_dir != "asc" else sort_col.asc())

        rows = q.limit(limit).all()
        return [
            {
                "company_id": c.id,
                "company_name": c.company_name,
                "sector": c.sector,
                "hq_state": c.hq_state,
                "ownership_status": c.ownership_status,
                "probability": s.probability,
                "raw_composite_score": s.raw_composite_score,
                "grade": s.grade,
                "active_signal_count": s.active_signal_count,
            }
            for s, c in rows
        ]

    @staticmethod
    def _apply_op(q, col, op: str, value):
        if op == ">=":
            return q.filter(col >= value)
        if op == "<=":
            return q.filter(col <= value)
        if op == ">":
            return q.filter(col > value)
        if op == "<":
            return q.filter(col < value)
        if op == "=":
            return q.filter(col == value)
        if op == "!=":
            return q.filter(col != value)
        return q
