"""
SPEC_078 + SPEC_079 — tool surface for the Atlas Pilot agent.

Two kinds of tools:

  * **read tools** execute server-side and return data the agent
    reasons over (list_layers, query_place, compare_places,
    get_recent_events, get_migration_flows, cite).

  * **ui tools** return an action descriptor — the server doesn't
    perform side effects on the map; it queues the action, returns
    success to the agent, and the frontend applies the actions in
    order after rendering the narration. This is the bridge that
    lets the LLM pilot the actual UI (PLAN_073 §3 differentiator).

Each tool entry is `(definition, callable, kind)` where kind is
"read" or "ui". The dispatcher routes accordingly.
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


# ─── UI tools (return action descriptors; frontend applies post-render) ───

def _tool_plant_focal_node(db: Session, naics: str, lat: float, lon: float,
                            label: str = "") -> Dict[str, Any]:
    """Drop a focal-node glyph on the map. Returns an action the
    frontend executes after narration."""
    return {
        "ok": True,
        "action": {
            "name": "plant_focal_node",
            "args": {"naics": naics, "lat": float(lat), "lon": float(lon),
                      "label": label or naics},
        },
        "note": f"Focal node will be planted at ({lat}, {lon}) for NAICS {naics}.",
    }


def _tool_zoom_to(db: Session, lat: float, lon: float,
                   zoom_level: int = 12) -> Dict[str, Any]:
    """Recenter + zoom the map. zoom_level 4=national, 12=neighborhood,
    14=street."""
    return {
        "ok": True,
        "action": {
            "name": "zoom_to",
            "args": {"lat": float(lat), "lon": float(lon),
                      "zoom_level": int(zoom_level)},
        },
        "note": f"Map will zoom to ({lat}, {lon}) at level {zoom_level}.",
    }


def _tool_toggle_layer(db: Session, layer_id: str,
                        mode: str = "choropleth") -> Dict[str, Any]:
    """Activate a registered Atlas layer. mode='choropleth' for county
    or tract layers; mode='overlay' for point layers."""
    from app.services.atlas import layers as layers_mod
    if layer_id not in layers_mod.LAYERS:
        return {"error": f"unknown layer_id {layer_id!r}; "
                          f"use list_layers to see available."}
    spec = layers_mod.LAYERS[layer_id]
    if mode not in ("choropleth", "overlay"):
        return {"error": f"mode must be 'choropleth' or 'overlay'; got {mode!r}"}
    return {
        "ok": True,
        "action": {
            "name": "toggle_layer",
            "args": {"layer_id": layer_id, "mode": mode,
                      "grain": spec.grain, "label": spec.label},
        },
        "note": f"Layer '{spec.label}' ({spec.grain}) will be activated as {mode}.",
    }


def _tool_highlight_place(db: Session, geo_id: str) -> Dict[str, Any]:
    """Visually highlight a county/tract on the map + open its place panel."""
    if not geo_id.isdigit() or len(geo_id) not in (2, 5, 11):
        return {"error": "geo_id must be 2/5/11-digit FIPS"}
    return {
        "ok": True,
        "action": {
            "name": "highlight_place",
            "args": {"geo_id": geo_id},
        },
        "note": f"Map will highlight + open the place panel for {geo_id}.",
    }


# ─── JSON Schema definitions (OpenAI function-calling format) ──────────────

# Each entry: (definition, callable, kind: "read"|"ui")
TOOLS: List[Tuple[Dict[str, Any], Callable, str]] = [
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
        _tool_list_layers, "read",
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
        _tool_query_place, "read",
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
        _tool_compare_places, "read",
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
        _tool_get_recent_events, "read",
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
        _tool_get_migration_flows, "read",
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
        _tool_cite, "read",
    ),
    # ─── UI tools (return action descriptors) ────────────────────────────
    (
        {
            "type": "function",
            "function": {
                "name": "plant_focal_node",
                "description": "Place a focal-node marker on the map at the "
                                "given coordinates. Use when the user wants to "
                                "evaluate or simulate a business at a specific "
                                "location. The agent should provide reasonable "
                                "lat/lon (the system prompt has common-place "
                                "FIPS but not addresses; use approximate coords "
                                "from your knowledge if needed).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "naics": {"type": "string",
                                   "description": "NAICS code (e.g. 442110 = furniture stores, 722515 = snack/non-alcoholic bars)"},
                        "lat": {"type": "number"},
                        "lon": {"type": "number"},
                        "label": {"type": "string", "description": "Display label (defaults to naics)"},
                    },
                    "required": ["naics", "lat", "lon"],
                },
            },
        },
        _tool_plant_focal_node, "ui",
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "zoom_to",
                "description": "Recenter + zoom the map. Use to direct the user's "
                                "attention to a specific location. zoom_level "
                                "guide: 4=national, 8=state, 11=metro, 12=neighborhood, "
                                "14=street, 16=block.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "lat": {"type": "number"},
                        "lon": {"type": "number"},
                        "zoom_level": {"type": "integer", "minimum": 2, "maximum": 18, "default": 12},
                    },
                    "required": ["lat", "lon"],
                },
            },
        },
        _tool_zoom_to, "ui",
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "toggle_layer",
                "description": "Activate an Atlas layer on the map. Use when the "
                                "user's question is about a specific data layer "
                                "(income, broadband, federal dollars, etc) — "
                                "showing it visually is part of the answer.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "layer_id": {"type": "string", "description": "Use list_layers to discover available ids."},
                        "mode": {"type": "string", "enum": ["choropleth", "overlay"],
                                  "default": "choropleth",
                                  "description": "'choropleth' for county/state/tract layers; 'overlay' for point layers."},
                    },
                    "required": ["layer_id"],
                },
            },
        },
        _tool_toggle_layer, "ui",
    ),
    (
        {
            "type": "function",
            "function": {
                "name": "highlight_place",
                "description": "Open the place panel for a county or tract and "
                                "fit the map to it. Use when answering a question "
                                "specifically about one place.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "geo_id": {"type": "string", "description": "2/5/11-digit FIPS"},
                    },
                    "required": ["geo_id"],
                },
            },
        },
        _tool_highlight_place, "ui",
    ),
]


# Public dispatcher
TOOL_DEFS = [t[0] for t in TOOLS]
TOOL_CALLABLES: Dict[str, Callable] = {t[0]["function"]["name"]: t[1] for t in TOOLS}
TOOL_KINDS: Dict[str, str] = {t[0]["function"]["name"]: t[2] for t in TOOLS}


def dispatch(db: Session, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one tool call. Returns dict (JSON-serializable).
    UI tools return action descriptors that the frontend applies after
    the agent's narration finishes."""
    if name not in TOOL_CALLABLES:
        return {"error": f"unknown tool {name!r}; available: {sorted(TOOL_CALLABLES)}"}
    try:
        return TOOL_CALLABLES[name](db, **args)
    except TypeError as exc:
        return {"error": f"bad args for {name}: {exc}"}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Tool %s raised: %s", name, exc)
        return {"error": f"tool execution failed: {exc}"}


def is_ui_tool(name: str) -> bool:
    return TOOL_KINDS.get(name) == "ui"
