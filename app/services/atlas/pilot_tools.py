"""
SPEC_078 Phase B v0 — tool surface for the Atlas Pilot agent.

Constrained 6-tool set wrapping existing endpoints. Each tool has a
JSON Schema definition (OpenAI Chat Completions function-calling
format) and a Python callable that executes against the DB.

v0 is intentionally read-only. v1 (Phase C+) adds map-mutating tools
(plant_focal_node, toggle_layer, compute_demand_surface, etc).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, List, Tuple

from sqlalchemy.orm import Session

from app.services.atlas import layers as layers_mod
from app.services.atlas import series as series_mod

logger = logging.getLogger(__name__)


# ─── Tool callables ─────────────────────────────────────────────────────────
# Each callable: (db, **args) -> dict (JSON-serializable)

def _tool_list_layers(db: Session) -> Dict[str, Any]:
    """Return the layer registry — what the agent has to choose from."""
    grouped = layers_mod.list_layers_by_domain()
    flat = []
    for dom, specs in grouped.items():
        for s in specs:
            flat.append({
                "id": s["id"], "domain": dom, "grain": s["grain"],
                "label": s["label"], "unit": s.get("unit"),
                "vintage": s.get("vintage"),
            })
    return {"count": len(flat), "layers": flat}


def _tool_query_place(db: Session, geo_id: str) -> Dict[str, Any]:
    """Multi-layer aggregate for one place. Accepts county FIPS (5-digit),
    state FIPS (2-digit), or tract FIPS (11-digit)."""
    if not geo_id or not geo_id.isdigit() or len(geo_id) not in (2, 5, 11):
        return {"error": f"geo_id must be 2-digit state, 5-digit county, "
                          f"or 11-digit tract FIPS; got {geo_id!r}"}
    grain = {2: "state", 5: "county", 11: "tract"}[len(geo_id)]
    layers = layers_mod.place_aggregate(db, geo_id)
    return {"geo_id": geo_id, "grain": grain, "layer_count": len(layers),
            "layers": layers}


def _tool_compare_places(db: Session, geo_ids: List[str]) -> Dict[str, Any]:
    """Side-by-side multi-layer aggregate for 2-5 places."""
    if not geo_ids or len(geo_ids) < 2 or len(geo_ids) > 5:
        return {"error": "compare_places needs 2-5 geo_ids"}
    out = []
    for gid in geo_ids:
        result = _tool_query_place(db, gid)
        out.append(result)
    return {"places": out, "n": len(out)}


def _tool_get_recent_events(db: Session, limit: int = 20,
                            sources: str = "fema,sec") -> Dict[str, Any]:
    """Most-recent FEMA + SEC events; merged + sorted by date desc."""
    src_list = [s.strip() for s in sources.split(",") if s.strip()]
    try:
        items = series_mod.fetch_recent_events(db, src_list, limit=limit)
    except ValueError as exc:
        return {"error": str(exc)}
    return {"items": items, "n": len(items)}


def _tool_get_migration_flows(db: Session, top_n: int = 20) -> Dict[str, Any]:
    """Top-N IRS county-to-county migration flows for the latest tax year."""
    try:
        return series_mod.fetch_top_migration_flows(db, top_n=top_n)
    except ValueError as exc:
        return {"error": str(exc)}


def _tool_cite(db: Session, claim: str, source: str) -> Dict[str, Any]:
    """Log a citation — pure side-effect tool. Every numerical claim
    in the final narration MUST be backed by a cite() call."""
    return {"acknowledged": True, "claim": claim, "source": source}


# ─── JSON Schema definitions (OpenAI function-calling format) ──────────────

TOOLS: List[Tuple[Dict[str, Any], Callable]] = [
    (
        {
            "type": "function",
            "function": {
                "name": "list_layers",
                "description": "Return the registry of all Atlas data layers. "
                                "Use this first if you don't know which layer "
                                "contains the data you need.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        _tool_list_layers,
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "query_place",
                "description": "Read all available layer values for one US "
                                "place. Accepts 2-digit state FIPS, 5-digit "
                                "county FIPS, or 11-digit tract FIPS. "
                                "Returns layer values with units and vintage.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "geo_id": {
                            "type": "string",
                            "description": "FIPS code (2, 5, or 11 digits). "
                                           "E.g. '48' = Texas, '48201' = Harris "
                                           "County TX, '48201212301' = a tract.",
                        }
                    },
                    "required": ["geo_id"],
                },
            },
        },
        _tool_query_place,
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "compare_places",
                "description": "Side-by-side multi-layer comparison of 2-5 places "
                                "by FIPS. Same shape as query_place per place.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "geo_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 2, "maxItems": 5,
                            "description": "List of FIPS codes (2, 5, or 11 digits)."
                        }
                    },
                    "required": ["geo_ids"],
                },
            },
        },
        _tool_compare_places,
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "get_recent_events",
                "description": "Most-recent dated events across FEMA disaster "
                                "declarations and SEC filings (10-K, 10-Q). "
                                "Use for 'what's new' questions.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
                        "sources": {"type": "string", "default": "fema,sec",
                                     "description": "Comma-separated: 'fema', 'sec', or both."},
                    },
                },
            },
        },
        _tool_get_recent_events,
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "get_migration_flows",
                "description": "Top IRS county-to-county migration $-flows for "
                                "the latest tax year. Use for 'where are people "
                                "moving from / to' questions.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "top_n": {"type": "integer", "default": 20, "minimum": 1, "maximum": 200},
                    },
                },
            },
        },
        _tool_get_migration_flows,
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "cite",
                "description": "Log a citation for a numerical claim. "
                                "EVERY numerical claim in your final answer "
                                "MUST be backed by a cite() call. The 'source' "
                                "should identify the layer + place (e.g. "
                                "'demo_acs_median_income for 48201').",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "claim": {"type": "string", "description": "The claim being cited (1 sentence)."},
                        "source": {"type": "string", "description": "Layer + place or tool + args."},
                    },
                    "required": ["claim", "source"],
                },
            },
        },
        _tool_cite,
    ),
]


# Public dispatcher
TOOL_DEFS = [t[0] for t in TOOLS]
TOOL_CALLABLES: Dict[str, Callable] = {t[0]["function"]["name"]: t[1] for t in TOOLS}


def dispatch(db: Session, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one tool call. Returns dict (JSON-serializable)."""
    if name not in TOOL_CALLABLES:
        return {"error": f"unknown tool {name!r}; available: {sorted(TOOL_CALLABLES)}"}
    try:
        return TOOL_CALLABLES[name](db, **args)
    except TypeError as exc:
        return {"error": f"bad args for {name}: {exc}"}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Tool %s raised: %s", name, exc)
        return {"error": f"tool execution failed: {exc}"}
