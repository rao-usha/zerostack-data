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
import re as _re
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


def _tool_geocode_address(db: Session, query: str) -> Dict[str, Any]:
    """Resolve a free-text address ('Lamar and 6th, Austin TX') into
    {lat, lon, display_name}. Uses Nominatim's free public API.

    Nominatim doesn't natively understand 'X and Y' intersections in
    free text, so we cascade: try the full query, then progressively
    simplify (drop the 'and Y' clause) until we get a hit."""
    import time
    import httpx
    if not query or not query.strip():
        return {"error": "empty query"}

    # Build a cascade of progressively simpler queries
    attempts = [query]
    q = query
    if " and " in q.lower():
        # "Lamar Blvd and 6th St, Austin TX" → "Lamar Blvd, Austin TX"
        prefix = _re.split(r"\s+and\s+", q, maxsplit=1, flags=_re.IGNORECASE)[0]
        comma = q.find(",")
        suffix = q[comma:] if comma >= 0 else ""
        attempts.append((prefix + suffix).strip())
    if "&" in q:
        prefix = q.split("&", 1)[0]
        comma = q.find(",")
        suffix = q[comma:] if comma >= 0 else ""
        attempts.append((prefix + suffix).strip())

    seen = set()
    for attempt in attempts:
        if attempt in seen or not attempt:
            continue
        seen.add(attempt)
        try:
            with httpx.Client(timeout=10.0) as cli:
                resp = cli.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": attempt, "format": "json", "limit": 3,
                             "countrycodes": "us"},
                    headers={"User-Agent": "Nexdata-Atlas-Pilot/0.1 (research@nexdata.io)"}
                )
                resp.raise_for_status()
                results = resp.json()
        except httpx.HTTPError as exc:
            return {"error": f"geocoder request failed: {exc}"}
        time.sleep(1.0)  # Nominatim's 1 req/sec etiquette
        if results:
            top = results[0]
            return {
                "ok": True,
                "lat": float(top["lat"]),
                "lon": float(top["lon"]),
                "display_name": top.get("display_name", ""),
                "query_used": attempt,
                "fallback_used": attempt != query,
                "candidates": [
                    {"lat": float(r["lat"]), "lon": float(r["lon"]),
                     "display_name": r.get("display_name", "")}
                    for r in results[:3]
                ],
            }
    return {"error": f"no geocoder match for {query!r} (tried {len(seen)} variants)",
            "tried": list(seen)}


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


# ─── SPEC_090 — Decision Map Pilot tools ─────────────────────────────────
# These drive the Phase A-D primitives from the chat:
#   recommend_candidates → read tool (top-N from /fit-score)
#   add_constraint / remove_constraint → UI tools (push/pop chips)
#   enter_trade_area / exit_trade_area → UI tools (Trade Area card)

_VALID_DIMENSIONS = {
    "hhi_min", "hhi_max", "establishments_min", "broadband_min", "exclude_nri",
}


def _tool_recommend_candidates(db: Session, top_n: int = 5,
                                 thesis_context: Optional[Dict[str, Any]] = None,
                                 ) -> Dict[str, Any]:
    """Return the top-N counties by current thesis fit. Read tool."""
    from app.services.atlas.fit_score import compute_fit_score
    n = max(1, min(20, int(top_n or 5)))
    r = compute_fit_score(db, thesis=thesis_context or {}, top_n=n)
    return {
        "candidates": r.get("top_n", []),
        "recipe": r.get("recipe"),
        "weights": r.get("weights", []),
        "total_candidates": r.get("total_candidates", 0),
    }


def _tool_add_constraint(db: Session, dimension: str, value: float
                          ) -> Dict[str, Any]:
    if dimension not in _VALID_DIMENSIONS:
        return {"error": f"unknown dimension {dimension!r}; "
                          f"valid: {sorted(_VALID_DIMENSIONS)}"}
    try:
        v = float(value)
    except (TypeError, ValueError):
        return {"error": "value must be numeric"}
    return {"ok": True,
            "action": {"name": "add_constraint",
                        "args": {"dimension": dimension, "value": v}},
            "note": f"Constraint {dimension}={v} queued."}


def _tool_remove_constraint(db: Session, dimension: str) -> Dict[str, Any]:
    if dimension not in _VALID_DIMENSIONS:
        return {"error": f"unknown dimension {dimension!r}"}
    return {"ok": True,
            "action": {"name": "remove_constraint",
                        "args": {"dimension": dimension}},
            "note": f"Removed constraint on {dimension}."}


def _tool_enter_trade_area(db: Session, geo_id: str,
                            radius_mi: float = 50.0) -> Dict[str, Any]:
    try:
        rm = float(radius_mi or 50.0)
    except (TypeError, ValueError):
        rm = 50.0
    rm = max(1.0, min(250.0, rm))
    return {"ok": True,
            "action": {"name": "enter_trade_area",
                        "args": {"geo_id": str(geo_id), "radius_mi": rm}},
            "note": f"Trade-area mode queued for {geo_id} ({rm} mi)."}


def _tool_exit_trade_area(db: Session) -> Dict[str, Any]:
    return {"ok": True,
            "action": {"name": "exit_trade_area", "args": {}},
            "note": "Exit trade-area queued."}


def _tool_present_options(db: Session, intro: str,
                           options: List[Dict[str, str]]) -> Dict[str, Any]:
    """SPEC_081 — present 2-4 clickable next-step options. Use AFTER
    narrating, to keep the user moving through a guided tour. Each
    option's 'prompt' becomes the next user question on click."""
    if not options or len(options) < 2 or len(options) > 4:
        return {"error": "options must be a list of 2-4 entries"}
    cleaned = []
    for o in options:
        if not isinstance(o, dict): continue
        label = (o.get("label") or "").strip()[:80]
        prompt = (o.get("prompt") or "").strip()[:400]
        if label and prompt:
            cleaned.append({"label": label, "prompt": prompt})
    if len(cleaned) < 2:
        return {"error": "need at least 2 well-formed options"}
    return {
        "ok": True,
        "action": {
            "name": "present_options",
            "args": {"intro": (intro or "").strip()[:200],
                      "options": cleaned},
        },
        "note": f"{len(cleaned)} guided-tour options queued for the user.",
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
    (
        {
            "type": "function",
            "function": {
                "name": "geocode_address",
                "description": "Resolve a free-text US address or place name "
                                "('Lamar and 6th, Austin TX') into lat/lon. "
                                "USE THIS before plant_focal_node or zoom_to "
                                "if the user gave a street address — don't "
                                "guess coordinates from memory. Returns top "
                                "match + up to 3 candidates.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string",
                                   "description": "Free-text US address or place name."},
                    },
                    "required": ["query"],
                },
            },
        },
        _tool_geocode_address, "read",
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
    (
        {
            "type": "function",
            "function": {
                "name": "present_options",
                "description": "Present 2-4 clickable next-step choices to the "
                                "user. Use this AFTER you've mutated the map "
                                "and narrated, to keep the user moving through "
                                "a guided exploration. Each option carries a "
                                "'prompt' that becomes the next user question "
                                "if clicked. Use for EXPLORATORY questions "
                                "(open-ended, multi-step); skip for SPECIFIC "
                                "factual queries.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "intro": {
                            "type": "string",
                            "description": "Short lead-in to the choices (e.g. "
                                           "'What would you like to look at next?')",
                        },
                        "options": {
                            "type": "array",
                            "minItems": 2, "maxItems": 4,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "label": {"type": "string",
                                               "description": "Short button text (≤6 words)"},
                                    "prompt": {"type": "string",
                                                "description": "Full question to submit if clicked"},
                                },
                                "required": ["label", "prompt"],
                            },
                        },
                    },
                    "required": ["intro", "options"],
                },
            },
        },
        _tool_present_options, "ui",
    ),
    # ─── SPEC_090 — Decision Map tools ────────────────────────────────
    (
        {"type": "function", "function": {
            "name": "recommend_candidates",
            "description": "Return the top-N counties by current thesis "
                            "fit-score. Use this when the user asks 'where "
                            "should I open/locate X?' instead of toggling "
                            "individual layers. The current thesis is the "
                            "one in the system prompt's <thesis> block.",
            "parameters": {"type": "object", "properties": {
                "top_n": {"type": "integer", "minimum": 1, "maximum": 20,
                          "description": "How many candidates to return"},
            }, "required": []},
        }},
        _tool_recommend_candidates, "read",
    ),
    (
        {"type": "function", "function": {
            "name": "add_constraint",
            "description": "Add a hard filter to the Decision Map "
                            "(equivalent to clicking + Add chip). Use when "
                            "the user states a must-have (e.g. 'only "
                            "counties above $80K HHI').",
            "parameters": {"type": "object", "properties": {
                "dimension": {"type": "string", "enum": [
                    "hhi_min", "hhi_max", "establishments_min",
                    "broadband_min", "exclude_nri"]},
                "value": {"type": "number",
                          "description": "Threshold value (e.g. 80000 for "
                                         "$80K HHI, 50 for max NRI)"},
            }, "required": ["dimension", "value"]},
        }},
        _tool_add_constraint, "ui",
    ),
    (
        {"type": "function", "function": {
            "name": "remove_constraint",
            "description": "Remove an active constraint chip by dimension.",
            "parameters": {"type": "object", "properties": {
                "dimension": {"type": "string", "enum": [
                    "hhi_min", "hhi_max", "establishments_min",
                    "broadband_min", "exclude_nri"]},
            }, "required": ["dimension"]},
        }},
        _tool_remove_constraint, "ui",
    ),
    (
        {"type": "function", "function": {
            "name": "enter_trade_area",
            "description": "Open the Trade Area view for a candidate county "
                            "(zoom + radius + neighbour summary card). Use "
                            "after recommend_candidates to drill into the "
                            "top pick.",
            "parameters": {"type": "object", "properties": {
                "geo_id": {"type": "string",
                            "description": "5-digit county FIPS"},
                "radius_mi": {"type": "number", "minimum": 1, "maximum": 250,
                               "description": "Trade-area radius (default 50)"},
            }, "required": ["geo_id"]},
        }},
        _tool_enter_trade_area, "ui",
    ),
    (
        {"type": "function", "function": {
            "name": "exit_trade_area",
            "description": "Close the Trade Area card and return to the "
                            "fit-score view.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        }},
        _tool_exit_trade_area, "ui",
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
