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


# ─── SPEC_097 — Chat-driven nav tools (PLAN_078 Layer 3) ─────────────────

_VALID_THESIS_KEYS = (
    "industry_label", "industry_naics", "region",
    "target_hhi_min", "target_hhi_max", "target_age_band",
    "target_pop_density_min", "exclude_layers", "notes",
)
_VALID_WEIGHT_KEYS = ("income", "commercial", "broadband")


def _tool_select_pin(db: Session, rank: Optional[int] = None,
                      geo_id: Optional[str] = None) -> Dict[str, Any]:
    """UI tool. Opens the trade-area card for the candidate at `rank`
    or with `geo_id`. The frontend applies the action via the existing
    enterTradeArea path (top_pick:true shorthand handled too)."""
    args: Dict[str, Any] = {}
    if geo_id:
        args["geo_id"] = str(geo_id)
    elif rank is not None:
        try:
            r = int(rank)
        except (TypeError, ValueError):
            return {"error": "rank must be int"}
        if not 1 <= r <= 10:
            return {"error": "rank must be 1..10"}
        args["rank"] = r
    else:
        return {"error": "provide either rank or geo_id"}
    return {"ok": True,
            "action": {"name": "select_pin", "args": args},
            "note": "Pin selection queued."}


def _tool_set_fit_weights(db: Session, weights: Dict[str, Any]
                            ) -> Dict[str, Any]:
    """UI tool. Override the active recipe weights and re-paint the
    fit-score. weights is `{income, commercial, broadband}` (any
    subset). Values renormalised to sum to 1 on the backend at
    paint time; the frontend just forwards them on the next /fit-score."""
    if not isinstance(weights, dict):
        return {"error": "weights must be an object"}
    clean: Dict[str, float] = {}
    for k, v in weights.items():
        if k not in _VALID_WEIGHT_KEYS:
            continue
        try:
            clean[k] = max(0.0, float(v))
        except (TypeError, ValueError):
            continue
    if not clean:
        return {"error": f"no valid weights; keys must be in {_VALID_WEIGHT_KEYS}"}
    return {"ok": True,
            "action": {"name": "set_fit_weights", "args": {"weights": clean}},
            "note": f"Weights override queued: {clean}"}


def _tool_describe_session(db: Session) -> Dict[str, Any]:
    """Read tool. Returns a directive telling the agent that the
    `<session_state>` block at the top of its system prompt is already
    the answer — no separate fetch needed. Mostly serves as an
    on-ramp the agent can explicitly call when asked 'describe what
    I'm looking at'."""
    return {
        "ok": True,
        "note": ("The <session_state> block at the top of your system "
                  "prompt already contains the current view. Quote from it "
                  "directly — thesis, chips, fit-score counter, top pins, "
                  "current trade area, recent actions, map view."),
    }


def _tool_reset_thesis(db: Session) -> Dict[str, Any]:
    """UI tool. Clears the thesis form."""
    return {"ok": True,
            "action": {"name": "reset_thesis", "args": {}},
            "note": "Reset thesis queued."}


def _tool_set_thesis_field(db: Session, key: str, value: Any
                             ) -> Dict[str, Any]:
    """UI tool. Sets one thesis field. Only whitelisted keys allowed."""
    if key not in _VALID_THESIS_KEYS:
        return {"error": f"key {key!r} not editable; "
                          f"valid: {sorted(_VALID_THESIS_KEYS)}"}
    v = value
    if isinstance(v, str):
        v = v.strip()[:200]
    return {"ok": True,
            "action": {"name": "set_thesis_field",
                        "args": {"key": key, "value": v}},
            "note": f"set_thesis_field({key}={v}) queued."}


def _tool_find_competition(db: Session, geo_id: str,
                            radius_mi: float = 5.0,
                            term: Optional[str] = None,
                            ) -> Dict[str, Any]:
    """SPEC_100 — count competing establishments at the thesis NAICS
    across a county and its trade-area neighbours, using Census CBP.

    Read tool. Replaces the SPEC_091 Yelp lookup. Resolves the focal
    geo_id, sweeps neighbour counties by haversine, maps the thesis
    `term` to a NAICS code via industry_naics, and aggregates from
    `census_cbp_county_yearly`.

    Returns a `per_county` breakdown (focal + sorted neighbours by
    establishments desc, top 5) in place of the old Yelp `top`
    businesses. No ratings/reviews — that's a separate spec.
    """
    from app.services.atlas.trade_area import county_centroids
    from app.services.atlas.competition import (
        find_competition_cbp, _neighbor_geo_ids_for,
    )
    from app.services.atlas.industry_naics import industry_to_naics
    centroids = county_centroids(db)
    c = centroids.get(str(geo_id))
    if not c:
        return {"error": f"unknown geo_id {geo_id!r}"}
    _lat, _lon, name = c
    neighbours = _neighbor_geo_ids_for(db, str(geo_id), float(radius_mi))
    naics = industry_to_naics(term)
    r = find_competition_cbp(
        db, focal_geo_id=str(geo_id),
        neighbor_geo_ids=neighbours, naics=naics,
    )
    return {
        "geo_id": geo_id, "name": name,
        "count": r.get("count", 0),
        "focal_count": r.get("focal_count", 0),
        "neighbours_count": r.get("neighbours_count", 0),
        "term_used": term,
        "naics_used": r.get("naics_used"),
        "naics_label": r.get("naics_label"),
        "radius_mi": float(radius_mi),
        "year": r.get("year"),
        # `top` keeps its meaning ("the top entries") — now top counties
        # by establishment count rather than top Yelp businesses.
        "top": (r.get("per_county") or [])[:5],
        "error": r.get("error"),
    }


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
    (
        {"type": "function", "function": {
            "name": "find_competition",
            "description": "Count competing businesses (Yelp Fusion) near "
                            "a county's centroid for the given search term. "
                            "Use after recommend_candidates / enter_trade_area "
                            "to answer 'how much competition is already there?' "
                            "Yelp caps radius at 25 mi.",
            "parameters": {"type": "object", "properties": {
                "geo_id": {"type": "string",
                            "description": "5-digit county FIPS"},
                "radius_mi": {"type": "number", "minimum": 0.5, "maximum": 25,
                               "description": "Search radius (default 5)"},
                "term": {"type": "string",
                          "description": "Search term — usually the thesis "
                                         "industry_label (e.g. 'Furniture stores')"},
            }, "required": ["geo_id"]},
        }},
        _tool_find_competition, "read",
    ),
    # ─── SPEC_097 — Decision Map nav tools ───────────────────────────
    (
        {"type": "function", "function": {
            "name": "select_pin",
            "description": "Open the trade-area card for one of the top-N "
                            "pins. Provide either `rank` (1..10) for the "
                            "ranked candidate from the current fit-score "
                            "or `geo_id` for a specific county FIPS.",
            "parameters": {"type": "object", "properties": {
                "rank":   {"type": "integer", "minimum": 1, "maximum": 10},
                "geo_id": {"type": "string"},
            }, "required": []},
        }},
        _tool_select_pin, "ui",
    ),
    (
        {"type": "function", "function": {
            "name": "set_fit_weights",
            "description": "Override the recipe weights and re-paint the "
                            "fit-score. weights is {income, commercial, "
                            "broadband} (any subset). Values renormalised "
                            "to sum to 1. Use when the user wants to "
                            "retune (e.g. 'weight broadband more').",
            "parameters": {"type": "object", "properties": {
                "weights": {"type": "object", "properties": {
                    "income":     {"type": "number", "minimum": 0},
                    "commercial": {"type": "number", "minimum": 0},
                    "broadband":  {"type": "number", "minimum": 0},
                }},
            }, "required": ["weights"]},
        }},
        _tool_set_fit_weights, "ui",
    ),
    (
        {"type": "function", "function": {
            "name": "describe_session",
            "description": "Read tool you can call when the user asks to "
                            "describe or summarize the current view. It "
                            "reminds you that the <session_state> block "
                            "at the top of your system prompt already has "
                            "the answer.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        }},
        _tool_describe_session, "read",
    ),
    (
        {"type": "function", "function": {
            "name": "reset_thesis",
            "description": "Clear every field of the thesis form. Use only "
                            "if the user explicitly asks to start over.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        }},
        _tool_reset_thesis, "ui",
    ),
    (
        {"type": "function", "function": {
            "name": "set_thesis_field",
            "description": "Set one field of the thesis form (and persist). "
                            "Valid keys: industry_label, industry_naics, "
                            "region, target_hhi_min, target_hhi_max, "
                            "target_age_band, target_pop_density_min, "
                            "exclude_layers, notes.",
            "parameters": {"type": "object", "properties": {
                "key":   {"type": "string", "enum": [
                    "industry_label", "industry_naics", "region",
                    "target_hhi_min", "target_hhi_max",
                    "target_age_band", "target_pop_density_min",
                    "exclude_layers", "notes",
                ]},
                "value": {},
            }, "required": ["key", "value"]},
        }},
        _tool_set_thesis_field, "ui",
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
