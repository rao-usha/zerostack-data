"""
Deal Radar — Natural Language Query Engine.

Parses user queries like "show me regions with high EPA violations"
into structured filters, validates them, and executes safe database
queries against the convergence_regions table.

The LLM NEVER generates raw SQL — only filter objects that the
backend validates against an allowed field whitelist.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.convergence_models import ConvergenceRegion
from app.services.convergence_engine import REGION_DEFINITIONS

logger = logging.getLogger(__name__)


# Whitelist of fields that can be filtered
ALLOWED_FIELDS = {
    "epa_score": "float",
    "irs_migration_score": "float",
    "trade_score": "float",
    "water_score": "float",
    "macro_score": "float",
    "convergence_score": "float",
    "cluster_status": "string",
    "signal_count": "int",
    "region_id": "string",
    "label": "string",
}

ALLOWED_OPERATORS = {">=", "<=", ">", "<", "="}

ALLOWED_SORT_FIELDS = {
    "convergence_score", "epa_score", "irs_migration_score",
    "trade_score", "water_score", "macro_score", "signal_count",
}

# System prompt for Claude — teaches it the schema and output format
NLQ_SYSTEM_PROMPT = """You are a query parser for a geographic investment intelligence system called Deal Radar.

The system tracks convergence signals across 13 US regions. Each region has these numeric scores (0-100):
- epa_score: EPA environmental violations and penalties
- irs_migration_score: IRS population migration flows
- trade_score: trade export volume and diversity
- water_score: water system violations and stress
- macro_score: income levels, capital gains, business income

Derived fields:
- convergence_score: composite score (0-100+), higher = more signals converging
- cluster_status: HOT (>=72), ACTIVE (>=58), WATCH (>=44), LOW (<44)
- signal_count: number of signals above 60 (0-5)

Region IDs: pacnw, cal, mtnw, sw, plains, texas, mw, appalachia, southeast, grtlakes, midatl, ne, florida

Parse the user's natural language query into a JSON object with this exact format:
{
  "filters": [
    {"field": "<field_name>", "op": "<operator>", "value": <number_or_string>}
  ],
  "sort_by": "<field_name>",
  "sort_dir": "desc",
  "explanation": "<1-sentence plain English explanation of what the query returns>"
}

Rules:
- Operators: >=, <=, >, <, = only
- "high" means >= 60, "very high" means >= 80, "low" means <= 30
- "hot" means cluster_status = "HOT", "active" means cluster_status = "ACTIVE"
- "population inflow" or "migration" refers to irs_migration_score
- "environmental" or "EPA" refers to epa_score
- "water" refers to water_score
- "trade" or "commerce" or "exports" refers to trade_score
- "income" or "macro" or "wealth" refers to macro_score
- Default sort: convergence_score desc
- Return ONLY valid JSON, no markdown, no backticks, no explanation outside the JSON."""


@dataclass
class NLQFilter:
    field: str
    op: str
    value: Any


@dataclass
class NLQResult:
    query: str
    filters: List[Dict[str, Any]]
    regions: List[Dict[str, Any]]
    explanation: str
    region_count: int


def validate_filter(f: dict) -> Optional[NLQFilter]:
    """Validate a single filter against the whitelist."""
    field = f.get("field", "")
    op = f.get("op", "")
    value = f.get("value")

    if field not in ALLOWED_FIELDS:
        logger.warning("NLQ: rejected unknown field '%s'", field)
        return None
    if op not in ALLOWED_OPERATORS:
        logger.warning("NLQ: rejected unknown operator '%s'", op)
        return None
    if value is None:
        return None

    # Type-check value
    field_type = ALLOWED_FIELDS[field]
    if field_type == "float":
        try:
            value = float(value)
        except (ValueError, TypeError):
            return None
    elif field_type == "int":
        try:
            value = int(value)
        except (ValueError, TypeError):
            return None
    elif field_type == "string":
        value = str(value).upper() if field == "cluster_status" else str(value)

    return NLQFilter(field=field, op=op, value=value)


def validate_filters(raw_filters: List[dict]) -> List[NLQFilter]:
    """Validate all filters, dropping invalid ones."""
    result = []
    for f in raw_filters:
        valid = validate_filter(f)
        if valid:
            result.append(valid)
    return result


class DealRadarNLQ:
    """Natural language query engine for Deal Radar convergence data."""

    def __init__(self, db: Session):
        self.db = db

    async def query(self, user_query: str) -> NLQResult:
        """Parse natural language query → filter → execute → return results."""
        user_query = (user_query or "").strip()

        if not user_query:
            # Empty query → return all regions
            regions = self._get_all_regions()
            return NLQResult(
                query="",
                filters=[],
                regions=regions,
                explanation="Showing all regions",
                region_count=len(regions),
            )

        # Parse with LLM
        parsed = await self._parse_query(user_query)
        raw_filters = parsed.get("filters", [])
        explanation = parsed.get("explanation", "Filtered results")
        sort_by = parsed.get("sort_by", "convergence_score")
        sort_dir = parsed.get("sort_dir", "desc")

        # Validate filters
        valid_filters = validate_filters(raw_filters)

        # Validate sort
        if sort_by not in ALLOWED_SORT_FIELDS:
            sort_by = "convergence_score"

        # Execute query
        regions = self._execute_query(valid_filters, sort_by, sort_dir)

        return NLQResult(
            query=user_query,
            filters=[{"field": f.field, "op": f.op, "value": f.value} for f in valid_filters],
            regions=regions,
            explanation=explanation,
            region_count=len(regions),
        )

    async def _parse_query(self, query: str) -> dict:
        """Use Claude to parse query into structured filters."""
        try:
            from app.agentic.llm_client import LLMClient

            llm = LLMClient(
                provider="anthropic",
                model="claude-3-5-haiku-20241022",
                max_tokens=400,
                temperature=0.0,
            )

            response = await llm.complete(
                prompt=query,
                system_prompt=NLQ_SYSTEM_PROMPT,
            )

            parsed = response.parse_json()
            if parsed:
                return parsed

            # Try direct JSON parse
            try:
                return json.loads(response.content.strip())
            except json.JSONDecodeError:
                pass

            logger.warning("NLQ: failed to parse LLM response as JSON")
            return {"filters": [], "explanation": "Could not parse query"}

        except Exception as e:
            logger.warning("NLQ: LLM call failed: %s", e)
            # Fallback: try simple keyword matching
            return self._keyword_fallback(query)

    def _keyword_fallback(self, query: str) -> dict:
        """Simple keyword-based fallback when LLM is unavailable."""
        q = query.lower()
        filters = []

        if "epa" in q or "environmental" in q or "violation" in q:
            filters.append({"field": "epa_score", "op": ">=", "value": 60})
        if "migration" in q or "population" in q or "inflow" in q:
            filters.append({"field": "irs_migration_score", "op": ">=", "value": 60})
        if "trade" in q or "export" in q or "commerce" in q:
            filters.append({"field": "trade_score", "op": ">=", "value": 60})
        if "water" in q or "infrastructure" in q:
            filters.append({"field": "water_score", "op": ">=", "value": 60})
        if "income" in q or "macro" in q or "wealth" in q:
            filters.append({"field": "macro_score", "op": ">=", "value": 60})
        if "hot" in q:
            filters.append({"field": "cluster_status", "op": "=", "value": "HOT"})
        if "active" in q and "hot" not in q:
            filters.append({"field": "cluster_status", "op": "=", "value": "ACTIVE"})

        return {
            "filters": filters,
            "explanation": f"Keyword match for: {query}",
            "sort_by": "convergence_score",
        }

    def _execute_query(
        self, filters: List[NLQFilter], sort_by: str, sort_dir: str
    ) -> List[Dict[str, Any]]:
        """Execute validated filters against convergence_regions."""
        query = self.db.query(ConvergenceRegion)

        for f in filters:
            col = getattr(ConvergenceRegion, f.field, None)
            if col is None:
                continue
            if f.op == ">=":
                query = query.filter(col >= f.value)
            elif f.op == "<=":
                query = query.filter(col <= f.value)
            elif f.op == ">":
                query = query.filter(col > f.value)
            elif f.op == "<":
                query = query.filter(col < f.value)
            elif f.op == "=":
                query = query.filter(col == f.value)

        # Sort
        sort_col = getattr(ConvergenceRegion, sort_by, ConvergenceRegion.convergence_score)
        if sort_dir == "asc":
            query = query.order_by(sort_col.asc())
        else:
            query = query.order_by(sort_col.desc())

        rows = query.all()
        results = []
        for r in rows:
            d = r.to_dict()
            defn = REGION_DEFINITIONS.get(r.region_id, {})
            d["map_x"] = defn.get("map_x", 0)
            d["map_y"] = defn.get("map_y", 0)
            results.append(d)
        return results

    def _get_all_regions(self) -> List[Dict[str, Any]]:
        """Return all regions with scores."""
        rows = self.db.query(ConvergenceRegion).order_by(
            ConvergenceRegion.convergence_score.desc()
        ).all()
        results = []
        for r in rows:
            d = r.to_dict()
            defn = REGION_DEFINITIONS.get(r.region_id, {})
            d["map_x"] = defn.get("map_x", 0)
            d["map_y"] = defn.get("map_y", 0)
            results.append(d)
        return results
