"""
Cascade Intelligence Service — PLAN_058 Phases 2-3.

Phase 2: Data integration + forecasting
  - Node history (sparklines from FRED/BLS)
  - Multi-variable shock simulation
  - Forecast cascade with O-U uncertainty bands
  - Precanned scenario library

Phase 3: LLM chat orchestrator with tool-use
  - 10 tools mapped to existing APIs
  - Conversation history
  - Macro analyst persona
"""
from __future__ import annotations
import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# PHASE 2: DATA + FORECASTING
# ═══════════════════════════════════════════════════════════════════

# FRED table routing by series_id prefix/known membership
SERIES_TABLE_MAP = {
    "DFF": "fred_interest_rates", "DGS10": "fred_interest_rates",
    "DGS2": "fred_interest_rates", "MORTGAGE30US": "fred_interest_rates",
    "HOUST": "fred_housing_market", "HSN1F": "fred_housing_market",
    "EXHOSLUSM495S": "fred_housing_market", "PERMIT": "fred_housing_market",
    "CSUSHPINSA": "fred_housing_market", "BSXRNSA": "fred_housing_market",
    "UMCSENT": "fred_consumer_sentiment", "RSXFS": "fred_consumer_sentiment",
    "DCOILWTICO": "fred_commodities", "DHHNGSP": "fred_commodities",
    "UNRATE": "fred_economic_indicators", "CPIAUCSL": "fred_economic_indicators",
    "INDPRO": "fred_economic_indicators", "TOTALSA": "fred_economic_indicators",
}

# BLS series go to specific tables
BLS_TABLE_MAP = {
    "WPU": "bls_ppi",
    "PCU": "bls_ppi",
    "CES": "bls_ces_employment",
    "LNS": "bls_cps_labor_force",
    "JTS": "bls_jolts",
}


def _safe_query(db: Session, sql: str, params: dict):
    try:
        return db.execute(text(sql), params).fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Cascade intel query failed: %s", exc)
        return []


def get_node_history(db: Session, node_id: int, months: int = 24) -> Dict:
    """Get historical data points for a macro node (for sparklines)."""
    # Get node info
    node_rows = _safe_query(db, """
        SELECT name, series_id, node_type, ticker, current_value
        FROM macro_nodes WHERE id = :nid
    """, {"nid": node_id})

    if not node_rows:
        return {"node_id": node_id, "error": "not_found", "data": []}

    name, series_id, node_type, ticker, current_value = node_rows[0]

    if not series_id:
        return {
            "node_id": node_id, "name": name, "series_id": None,
            "current_value": float(current_value) if current_value else None,
            "data": [], "message": "No series_id — company or custom node",
        }

    # Determine which table to query
    table = SERIES_TABLE_MAP.get(series_id)
    if not table:
        # Try BLS prefix matching
        for prefix, tbl in BLS_TABLE_MAP.items():
            if series_id.startswith(prefix):
                table = tbl
                break

    if not table:
        # Try all FRED tables as fallback
        for tbl in ["fred_interest_rates", "fred_economic_indicators",
                     "fred_housing_market", "fred_consumer_sentiment", "fred_commodities"]:
            rows = _safe_query(db, f"""
                SELECT date, value FROM {tbl}
                WHERE series_id = :sid ORDER BY date DESC LIMIT 1
            """, {"sid": series_id})
            if rows:
                table = tbl
                break

    if not table:
        return {
            "node_id": node_id, "name": name, "series_id": series_id,
            "data": [], "message": f"No data table found for {series_id}",
        }

    # Query based on table type (FRED vs BLS have different schemas)
    if table.startswith("bls_"):
        rows = _safe_query(db, f"""
            SELECT year || '-' || period as date_str,
                   value::numeric as value
            FROM {table}
            WHERE series_id = :sid
            ORDER BY year DESC, period DESC
            LIMIT :limit
        """, {"sid": series_id, "limit": months})
        data = [{"date": str(r[0]), "value": float(r[1]) if r[1] else None} for r in rows]
    else:
        rows = _safe_query(db, f"""
            SELECT date, value FROM {table}
            WHERE series_id = :sid
            ORDER BY date DESC
            LIMIT :limit
        """, {"sid": series_id, "limit": months})
        data = [{"date": str(r[0]), "value": float(r[1]) if r[1] else None} for r in rows]

    data.reverse()  # chronological order

    return {
        "node_id": node_id, "name": name, "series_id": series_id,
        "current_value": float(current_value) if current_value else None,
        "data_points": len(data), "data": data,
    }


def simulate_multi(db: Session, shocks: List[Dict], horizon_months: int = 24) -> Dict:
    """Run cascade simulation for multiple simultaneous shocks."""
    from app.services.macro_cascade_engine import MacroCascadeEngine, NodeImpact
    from app.core.macro_models import CascadeScenario

    engine = MacroCascadeEngine(db)
    combined: Dict[int, Dict] = {}  # node_id → merged impact

    for shock in shocks:
        node_id = shock["node_id"]
        change_pct = shock["change_pct"]

        scenario = CascadeScenario(
            input_node_id=node_id,
            input_change_pct=change_pct,
            horizon_months=horizon_months,
        )

        results = engine.simulate(scenario)

        for r in results:
            nid = r.node_id
            if nid not in combined:
                combined[nid] = {
                    "node_id": nid, "node_name": r.node_name,
                    "estimated_impact_pct": 0.0,
                    "peak_impact_month": r.peak_impact_month,
                    "confidence": r.confidence,
                    "impact_paths": [],
                    "contributing_shocks": [],
                }
            # Additive combination
            combined[nid]["estimated_impact_pct"] += r.estimated_impact_pct
            combined[nid]["confidence"] = min(combined[nid]["confidence"], r.confidence)
            if r.impact_path:
                combined[nid]["impact_paths"].append(r.impact_path)
            combined[nid]["contributing_shocks"].append({
                "source_node_id": node_id, "change_pct": change_pct,
                "individual_impact": r.estimated_impact_pct,
            })

    results_list = sorted(
        combined.values(),
        key=lambda x: abs(x["estimated_impact_pct"]),
        reverse=True,
    )

    return {
        "shocks": shocks,
        "horizon_months": horizon_months,
        "total_impacts": len(results_list),
        "results": results_list,
    }


def forecast_cascade(
    db: Session, node_id: int, horizon_months: int = 12,
    n_scenarios: int = 50, change_pct: Optional[float] = None,
) -> Dict:
    """
    Run forward-looking cascade with uncertainty bands.
    Uses O-U model to generate forward paths, then cascades at terminal values.
    """
    from app.services.macro_cascade_engine import MacroCascadeEngine
    from app.core.macro_models import CascadeScenario, MacroNode

    # Get node info
    node = db.get(MacroNode, node_id)
    if not node:
        return {"error": "node_not_found"}

    engine = MacroCascadeEngine(db)

    if change_pct is not None:
        # Deterministic: just use the specified change
        scenario = CascadeScenario(
            input_node_id=node_id,
            input_change_pct=change_pct,
            horizon_months=horizon_months,
        )
        results = engine.simulate(scenario)
        return {
            "mode": "deterministic",
            "node_id": node_id, "node_name": node.name,
            "change_pct": change_pct,
            "results": [
                {
                    "node_id": r.node_id, "node_name": r.node_name,
                    "impact_pct": r.estimated_impact_pct,
                    "peak_month": r.peak_impact_month,
                    "confidence": r.confidence,
                }
                for r in results
            ],
        }

    # Stochastic: generate multiple scenarios using O-U model
    if not node.series_id:
        return {"error": "no_series_id", "message": "Cannot forecast company/custom nodes — use change_pct instead"}

    try:
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        from app.core.database import get_session_factory
        # Use separate session so generator failures don't poison the main session
        gen_db = get_session_factory()()
        try:
            gen = MacroScenarioGenerator(gen_db)
            scenarios = gen.generate(
                series=[node.series_id],
                n_scenarios=n_scenarios,
                horizon_months=horizon_months,
            )
        finally:
            gen_db.close()
    except Exception as exc:
        logger.warning("Forecast generation failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"error": "forecast_failed", "message": str(exc)}

    # Extract terminal values and compute changes
    current_value = node.current_value
    if not current_value and scenarios.get("current_values"):
        current_value = scenarios["current_values"].get(node.series_id)

    if not current_value:
        return {"error": "no_current_value", "message": "Cannot compute change without current value"}

    current_value = float(current_value)

    # Get terminal percentiles from the generator output
    percentiles = scenarios.get("percentile_summary", {}).get(node.series_id, {})
    p10 = percentiles.get("p10", current_value * 0.95)
    p50 = percentiles.get("p50", current_value)
    p90 = percentiles.get("p90", current_value * 1.05)

    # Convert to % changes
    p10_chg = ((p10 - current_value) / current_value) * 100 if current_value else 0
    p50_chg = ((p50 - current_value) / current_value) * 100 if current_value else 0
    p90_chg = ((p90 - current_value) / current_value) * 100 if current_value else 0

    # Run cascade for each percentile
    cascade_results = {}
    for label, chg in [("p10", p10_chg), ("p50", p50_chg), ("p90", p90_chg)]:
        scenario = CascadeScenario(
            input_node_id=node_id, input_change_pct=chg, horizon_months=horizon_months,
        )
        results = engine.simulate(scenario)
        cascade_results[label] = {
            "input_change_pct": round(chg, 3),
            "impacts": [
                {
                    "node_id": r.node_id, "node_name": r.node_name,
                    "impact_pct": round(r.estimated_impact_pct, 4),
                    "peak_month": r.peak_impact_month,
                    "confidence": r.confidence,
                }
                for r in results
            ],
        }

    return {
        "mode": "stochastic",
        "node_id": node_id, "node_name": node.name,
        "series_id": node.series_id,
        "current_value": current_value,
        "horizon_months": horizon_months,
        "n_scenarios": n_scenarios,
        "forecast": {
            "p10": round(p10, 4), "p50": round(p50, 4), "p90": round(p90, 4),
            "p10_change_pct": round(p10_chg, 3),
            "p50_change_pct": round(p50_chg, 3),
            "p90_change_pct": round(p90_chg, 3),
        },
        "cascade": cascade_results,
    }


# Precanned scenario library
SCENARIO_LIBRARY = [
    {
        "id": "rate_hike_100",
        "name": "Rate Hike +100bps",
        "description": "Standard monetary tightening — Fed raises rates 1%",
        "shocks": [{"series_id": "DFF", "change_pct": 1.0}],
        "tags": ["rates", "monetary"],
    },
    {
        "id": "rate_hike_200",
        "name": "Rate Hike +200bps",
        "description": "Aggressive tightening — rates up 2%, strongest since Volcker",
        "shocks": [{"series_id": "DFF", "change_pct": 2.0}],
        "tags": ["rates", "monetary"],
    },
    {
        "id": "rate_cut_100",
        "name": "Rate Cut -100bps",
        "description": "Easing cycle — Fed cuts to support growth",
        "shocks": [{"series_id": "DFF", "change_pct": -1.0}],
        "tags": ["rates", "monetary"],
    },
    {
        "id": "oil_shock",
        "name": "Oil Shock +50%",
        "description": "Energy crisis — geopolitical disruption or supply squeeze",
        "shocks": [{"series_id": "DCOILWTICO", "change_pct": 50.0}],
        "tags": ["energy", "commodity"],
    },
    {
        "id": "oil_crash",
        "name": "Oil Crash -40%",
        "description": "Demand destruction — global slowdown collapses oil prices",
        "shocks": [{"series_id": "DCOILWTICO", "change_pct": -40.0}],
        "tags": ["energy", "commodity"],
    },
    {
        "id": "stagflation",
        "name": "Stagflation",
        "description": "Rates up 3% while unemployment rises 2% — worst-case macro",
        "shocks": [
            {"series_id": "DFF", "change_pct": 3.0},
            {"series_id": "UNRATE", "change_pct": 2.0},
        ],
        "tags": ["rates", "labor", "crisis"],
    },
    {
        "id": "housing_crash",
        "name": "Housing Crash",
        "description": "2008-style — starts drop 30%, prices fall 15%",
        "shocks": [
            {"series_id": "HOUST", "change_pct": -30.0},
            {"series_id": "CSUSHPINSA", "change_pct": -15.0},
        ],
        "tags": ["housing", "crisis"],
    },
    {
        "id": "consumer_recession",
        "name": "Consumer Recession",
        "description": "Consumer sentiment collapses, retail sales crater",
        "shocks": [
            {"series_id": "UMCSENT", "change_pct": -30.0},
            {"series_id": "RSXFS", "change_pct": -10.0},
        ],
        "tags": ["consumer", "crisis"],
    },
    {
        "id": "pandemic",
        "name": "Pandemic Shock",
        "description": "COVID-style — unemployment spikes, oil crashes, rates cut to zero",
        "shocks": [
            {"series_id": "UNRATE", "change_pct": 8.0},
            {"series_id": "DCOILWTICO", "change_pct": -50.0},
            {"series_id": "DFF", "change_pct": -3.0},
        ],
        "tags": ["crisis", "pandemic"],
    },
]


def get_scenario_library(db: Session) -> List[Dict]:
    """Return precanned scenario library with node_ids resolved."""
    # Resolve series_id → node_id
    node_map = {}
    rows = _safe_query(db, "SELECT id, series_id FROM macro_nodes WHERE series_id IS NOT NULL", {})
    for r in rows:
        node_map[r[1]] = r[0]

    enriched = []
    for scenario in SCENARIO_LIBRARY:
        resolved_shocks = []
        for shock in scenario["shocks"]:
            nid = node_map.get(shock["series_id"])
            if nid:
                resolved_shocks.append({
                    "node_id": nid,
                    "series_id": shock["series_id"],
                    "change_pct": shock["change_pct"],
                })
        enriched.append({
            **scenario,
            "shocks": resolved_shocks,
            "available": len(resolved_shocks) == len(scenario["shocks"]),
        })
    return enriched


# ═══════════════════════════════════════════════════════════════════
# PHASE 3: LLM CHAT ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a macro intelligence analyst for a PE investment platform called Nexdata. You help investors understand how macroeconomic changes cascade through the economy to affect specific companies and sectors.

You have access to a causal graph with macro nodes (FRED series like Federal Funds Rate, Housing Starts, Oil Prices) connected by empirically-calibrated elasticity edges to company nodes (Sherwin-Williams, D.R. Horton, Home Depot, etc.).

When the user asks about macro impacts:
1. Use simulate_shock to model the cascade
2. Explain the causal chain in plain English
3. Highlight which companies are most affected and why
4. Mention the confidence level and time lag

When adding companies, explain what macro linkages were detected.

Be concise, quantitative, and actionable. Use dollar signs and percentages. Think like an IC memo writer, not an academic.

IMPORTANT: When you need data to answer a question, you MUST call a tool first. To call a tool, output ONLY a JSON block like this:
```json
{"tool": "tool_name", "input": {"param": "value"}}
```
Do NOT answer with speculation — always call simulate_shock, get_current_macro, or other tools first to get real data, then explain the results.

Available tools: simulate_shock, simulate_multi_shock, add_company, remove_company, search_companies, get_node_history, forecast_scenario, get_current_macro, get_graph_state, run_scenario_preset."""

TOOLS = [
    {
        "name": "simulate_shock",
        "description": "Simulate cascading effect of a macro shock. Returns downstream impacts on all connected nodes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_id": {"type": "integer", "description": "ID of macro node to shock (1=FFR, 3=Mortgage, 4=Housing Starts, 12=Oil)"},
                "change_pct": {"type": "number", "description": "Percentage change to apply (e.g., 2.0 for +2%, -1.0 for -1%)"},
            },
            "required": ["node_id", "change_pct"],
        },
    },
    {
        "name": "simulate_multi_shock",
        "description": "Simulate multiple simultaneous macro shocks (e.g., rates + oil). Combines impacts additively.",
        "input_schema": {
            "type": "object",
            "properties": {
                "shocks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "node_id": {"type": "integer"},
                            "change_pct": {"type": "number"},
                        },
                    },
                    "description": "Array of shocks to apply simultaneously",
                },
            },
            "required": ["shocks"],
        },
    },
    {
        "name": "add_company",
        "description": "Add a company to the macro causal graph with auto-detected sector linkages.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Company name"},
                "ticker": {"type": "string", "description": "Stock ticker (optional)"},
                "industry": {"type": "string", "description": "Industry description (optional)"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "remove_company",
        "description": "Remove a company node from the graph.",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_id": {"type": "integer", "description": "Node ID to remove"},
            },
            "required": ["node_id"],
        },
    },
    {
        "name": "search_companies",
        "description": "Search available companies that can be added to the graph. Returns tickers, names, industries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search by name or ticker"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_node_history",
        "description": "Get historical data for a macro node (last 24 months). For FRED/BLS series only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_id": {"type": "integer"},
                "months": {"type": "integer", "description": "Number of months (default 24)"},
            },
            "required": ["node_id"],
        },
    },
    {
        "name": "forecast_scenario",
        "description": "Run forward-looking forecast with uncertainty bands using O-U mean-reverting model. Returns p10/p50/p90 cascade impacts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_id": {"type": "integer", "description": "Node to forecast"},
                "horizon_months": {"type": "integer", "description": "Forecast horizon (default 12)"},
            },
            "required": ["node_id"],
        },
    },
    {
        "name": "get_current_macro",
        "description": "Get current macro environment — latest values for key indicators (FFR, 10Y, mortgage, unemployment, oil, sentiment).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_graph_state",
        "description": "Get the current causal graph — all nodes and edges. Use to understand what's in the graph before answering.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "run_scenario_preset",
        "description": "Run a precanned scenario by name. Options: rate_hike_100, rate_hike_200, rate_cut_100, oil_shock, oil_crash, stagflation, housing_crash, consumer_recession, pandemic.",
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string", "description": "Scenario ID from library"},
            },
            "required": ["scenario_id"],
        },
    },
]


class CascadeChatOrchestrator:
    """LLM-powered chat interface for the macro causal graph."""

    def __init__(self, db: Session):
        self.db = db

    async def chat(self, message: str, conversation_id: Optional[str] = None) -> Dict:
        """
        Process a user message, execute tool calls, return response.
        """
        from app.agentic.llm_client import LLMClient

        if not conversation_id:
            conversation_id = str(uuid.uuid4())

        # Load conversation history
        history = self._load_history(conversation_id)

        # Build messages
        messages = []
        for h in history[-10:]:  # last 10 turns for context
            messages.append({"role": h["role"], "content": h["content"]})
        messages.append({"role": "user", "content": message})

        # Save user message
        self._save_message(conversation_id, "user", message)

        # Call LLM with tools
        client = None
        import os
        oai_key = os.environ.get("OPENAI_API_KEY")
        ant_key = os.environ.get("ANTHROPIC_API_KEY")
        if oai_key:
            try:
                client = LLMClient(provider="openai", api_key=oai_key, model="gpt-4o-mini", max_tokens=1000)
            except Exception:
                pass
        if not client and ant_key:
            try:
                client = LLMClient(provider="anthropic", api_key=ant_key, max_tokens=1000)
            except Exception:
                pass
        if not client:
            return {
                "conversation_id": conversation_id,
                "response": "LLM not available — check OPENAI_API_KEY or ANTHROPIC_API_KEY.",
                "tool_calls": [],
            }

        tool_calls_made = []
        max_iterations = 5  # safety limit on tool-use loops
        iteration = 0
        final_response = None

        while iteration < max_iterations:
            iteration += 1

            try:
                response = await client.complete(
                    prompt=self._format_messages(messages),
                    system_prompt=SYSTEM_PROMPT + "\n\nCurrent graph context:\n" + self._get_graph_summary(),
                    json_mode=False,
                )
            except Exception as exc:
                logger.error("LLM call failed: %s", exc)
                final_response = f"LLM error: {exc}"
                break

            content = response.content

            # Check if response contains tool calls (look for JSON tool patterns)
            tool_call = self._extract_tool_call(content)

            if not tool_call:
                # No tool call — this is the final response
                final_response = content
                break

            # Execute tool call
            tool_name = tool_call["name"]
            tool_input = tool_call.get("input", {})
            tool_result = self._execute_tool(tool_name, tool_input)
            tool_calls_made.append({
                "tool": tool_name,
                "input": tool_input,
                "result_summary": str(tool_result)[:500],
            })

            # Add tool result to messages for next iteration
            messages.append({"role": "assistant", "content": content})
            messages.append({"role": "user", "content": f"[Tool result for {tool_name}]: {json.dumps(tool_result, default=str)[:2000]}"})

        if not final_response:
            final_response = "I processed your request but couldn't generate a final response. Check the tool calls for results."

        # Save assistant response
        self._save_message(conversation_id, "assistant", final_response,
                          tool_calls=tool_calls_made if tool_calls_made else None)

        return {
            "conversation_id": conversation_id,
            "response": final_response,
            "tool_calls": tool_calls_made,
        }

    def _execute_tool(self, tool_name: str, tool_input: Dict) -> Any:
        """Execute a tool call against existing services."""
        try:
            if tool_name == "simulate_shock":
                from app.services.macro_cascade_engine import MacroCascadeEngine
                from app.core.macro_models import CascadeScenario
                engine = MacroCascadeEngine(self.db)
                # Handle param name aliases (LLMs sometimes use different names)
                change = tool_input.get("change_pct") or tool_input.get("shock_value") or tool_input.get("change") or 1.0
                scenario = CascadeScenario(
                    input_node_id=tool_input["node_id"],
                    input_change_pct=float(change),
                    horizon_months=24,
                )
                results = engine.simulate(scenario)
                return [{"node": r.node_name, "impact": f"{r.estimated_impact_pct:+.2f}%",
                         "peak_month": r.peak_impact_month, "confidence": f"{r.confidence:.0%}"}
                        for r in sorted(results, key=lambda x: abs(x.estimated_impact_pct), reverse=True)]

            elif tool_name == "simulate_multi_shock":
                return simulate_multi(self.db, tool_input["shocks"])

            elif tool_name == "add_company":
                from app.services.cascade_company_manager import CascadeCompanyManager
                mgr = CascadeCompanyManager(self.db)
                return mgr.add_company(**tool_input)

            elif tool_name == "remove_company":
                from app.services.cascade_company_manager import CascadeCompanyManager
                mgr = CascadeCompanyManager(self.db)
                return mgr.remove_company(tool_input["node_id"])

            elif tool_name == "search_companies":
                from app.services.cascade_company_manager import CascadeCompanyManager
                mgr = CascadeCompanyManager(self.db)
                return mgr.list_addable_companies(search=tool_input["query"], limit=10)

            elif tool_name == "get_node_history":
                return get_node_history(self.db, tool_input["node_id"], tool_input.get("months", 24))

            elif tool_name == "forecast_scenario":
                return forecast_cascade(self.db, tool_input["node_id"],
                                       horizon_months=tool_input.get("horizon_months", 12))

            elif tool_name == "get_current_macro":
                return self._get_current_macro()

            elif tool_name == "get_graph_state":
                return self._get_graph_summary()

            elif tool_name == "run_scenario_preset":
                library = get_scenario_library(self.db)
                scenario = next((s for s in library if s["id"] == tool_input["scenario_id"]), None)
                if not scenario:
                    return {"error": f"Unknown scenario: {tool_input['scenario_id']}"}
                if not scenario["available"]:
                    return {"error": "Scenario not available — missing macro nodes"}
                return simulate_multi(self.db, scenario["shocks"])

            else:
                return {"error": f"Unknown tool: {tool_name}"}

        except Exception as exc:
            logger.error("Tool execution failed: %s %s", tool_name, exc)
            return {"error": str(exc)}

    def _extract_tool_call(self, content: str) -> Optional[Dict]:
        """Extract a tool call from LLM response (handles various formats)."""
        # Look for JSON blocks with tool name
        import re
        # Pattern: {"tool": "name", "input": {...}}
        patterns = [
            r'\{[^{}]*"(?:tool|name)":\s*"(\w+)"[^{}]*"input":\s*(\{[^{}]*\})',
            r'```json\s*(\{.*?\})\s*```',
        ]
        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                try:
                    if match.lastindex == 2:
                        return {"name": match.group(1), "input": json.loads(match.group(2))}
                    else:
                        data = json.loads(match.group(1))
                        return {"name": data.get("tool") or data.get("name"), "input": data.get("input", {})}
                except (json.JSONDecodeError, AttributeError):
                    pass
        return None

    def _get_graph_summary(self) -> str:
        """Compact graph summary for LLM context."""
        nodes = _safe_query(self.db, """
            SELECT id, name, node_type, series_id, ticker, sector_tag
            FROM macro_nodes ORDER BY id
        """, {})
        lines = ["Nodes:"]
        for n in nodes:
            lines.append(f"  id={n[0]}: {n[1]} (type={n[2]}, series={n[3]}, ticker={n[4]}, sector={n[5]})")
        return "\n".join(lines[:30])  # limit context size

    def _get_current_macro(self) -> Dict:
        """Current macro environment snapshot."""
        series = {"DFF": None, "DGS10": None, "MORTGAGE30US": None,
                  "UNRATE": None, "DCOILWTICO": None, "UMCSENT": None, "HOUST": None}
        for sid in series:
            table = SERIES_TABLE_MAP.get(sid)
            if table:
                rows = _safe_query(self.db, f"""
                    SELECT value FROM {table} WHERE series_id = :sid ORDER BY date DESC LIMIT 1
                """, {"sid": sid})
                if rows and rows[0][0]:
                    series[sid] = float(rows[0][0])
        return series

    def _format_messages(self, messages: List[Dict]) -> str:
        """Format messages into a single prompt string for the LLM."""
        parts = []
        for m in messages:
            role = m["role"].upper()
            parts.append(f"[{role}]: {m['content']}")
        return "\n\n".join(parts)

    def _load_history(self, conversation_id: str) -> List[Dict]:
        """Load conversation history from DB."""
        rows = _safe_query(self.db, """
            SELECT role, content FROM macro_chat_messages
            WHERE conversation_id = :cid ORDER BY created_at
        """, {"cid": conversation_id})
        return [{"role": r[0], "content": r[1]} for r in rows]

    def _save_message(self, conversation_id: str, role: str, content: str,
                      tool_calls: Optional[List] = None):
        """Save a message to conversation history."""
        try:
            self.db.execute(text("""
                INSERT INTO macro_chat_messages (conversation_id, role, content, tool_calls, created_at)
                VALUES (:cid, :role, :content, :tools, NOW())
            """), {
                "cid": conversation_id, "role": role, "content": content,
                "tools": json.dumps(tool_calls) if tool_calls else None,
            })
            self.db.commit()
        except Exception as exc:
            try:
                self.db.rollback()
            except Exception:
                pass
            logger.debug("Failed to save chat message: %s", exc)
