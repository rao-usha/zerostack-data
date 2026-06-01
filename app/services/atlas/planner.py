"""SPEC_094 — Atlas Decision Map planner.

Given a thesis (and optionally a user prompt), produce a structured
Plan that the frontend executor walks deterministically. One LLM call
returns the whole Plan — beat titles, rationales, and the tool calls
that mutate the Decision Map.

Planner → Executor replaces the SPEC_092/093 furniture-only hardcoded
runner. Works for any user-authored thesis.

Resilience:
  - LLM unavailable or returns invalid JSON → fallback to a frozen
    SPEC_092 furniture sequence so the user always gets a walkthrough.
  - Cache by SHA256(thesis_context, prompt) for 24h to keep repeat
    clicks cheap.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────

MODEL = os.environ.get("ATLAS_PILOT_MODEL", "gpt-4o-mini")

# Tool whitelist — every tool the executor knows how to dispatch.
ALLOWED_TOOLS = (
    "setup_thesis",
    "add_constraint",
    "remove_constraint",
    "recommend_candidates",
    "enter_trade_area",
    "exit_trade_area",
    "find_competition",
    "no_op",
)

# Mirrors pilot_tools._VALID_DIMENSIONS — kept local so the planner
# can be tested without importing the (heavier) pilot_tools module.
_VALID_DIMENSIONS = (
    "hhi_min", "hhi_max", "establishments_min",
    "broadband_min", "exclude_nri",
)

# In-process cache. key → (expiry_epoch, plan_dict)
_PLAN_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_TTL_SEC = 24 * 3600

# ─── Pydantic models ────────────────────────────────────────────────────


class BeatModel(BaseModel):
    title: str = Field(..., min_length=1, max_length=120)
    rationale: str = Field(..., min_length=1, max_length=700)
    tool: Literal[
        "setup_thesis", "add_constraint", "remove_constraint",
        "recommend_candidates", "enter_trade_area",
        "exit_trade_area", "find_competition", "no_op",
    ]
    args: Optional[Dict[str, Any]] = None
    wait_ms: Optional[int] = Field(default=1600, ge=200, le=5000)


class PlanModel(BaseModel):
    title: str = Field(..., min_length=1, max_length=120)
    summary: str = Field("", max_length=400)
    thesis_recap: Optional[str] = Field(None, max_length=400)
    beats: List[BeatModel] = Field(..., min_length=3, max_length=12)


_CAPS = {
    "title": 120, "summary": 400, "thesis_recap": 400,
    "beat_title": 120, "rationale": 700,
}


def _truncate_raw(raw: Any) -> Any:
    """Trim over-length strings BEFORE Pydantic validation so a verbose
    LLM doesn't trip an otherwise-fine plan into the fallback path."""
    if not isinstance(raw, dict):
        return raw
    for k in ("title", "summary", "thesis_recap"):
        v = raw.get(k)
        if isinstance(v, str) and len(v) > _CAPS.get(k, 999999):
            raw[k] = v[: _CAPS[k]].rstrip()
    beats = raw.get("beats")
    if isinstance(beats, list):
        for b in beats:
            if not isinstance(b, dict):
                continue
            if isinstance(b.get("title"), str) and len(b["title"]) > _CAPS["beat_title"]:
                b["title"] = b["title"][: _CAPS["beat_title"]].rstrip()
            if isinstance(b.get("rationale"), str) and len(b["rationale"]) > _CAPS["rationale"]:
                b["rationale"] = b["rationale"][: _CAPS["rationale"]].rstrip()
    return raw


# ─── Frozen fallback plan (SPEC_092 furniture sequence) ─────────────────

_FALLBACK_PLAN: Dict[str, Any] = {
    "title": "Furniture-store walkthrough (default)",
    "summary": (
        "Default walkthrough used when the planner couldn't author one. "
        "Filters by income + hazard risk, scores counties, drills into the "
        "top candidate's trade area."
    ),
    "beats": [
        {
            "title": "Loading thesis",
            "rationale": (
                "Loading the furniture-store demo thesis: NAICS 442110, "
                "Austin metro, target HHI ≥ $75K. This sets the context "
                "every downstream beat reasons against."
            ),
            "tool": "setup_thesis",
        },
        {
            "title": "Filter HHI ≥ $75K",
            "rationale": (
                "Furniture is a high-ticket discretionary purchase. "
                "Households below ~$75K rarely buy $1K+ furnishings new, "
                "so this threshold filters in the customer base."
            ),
            "tool": "add_constraint",
            "args": {"dimension": "hhi_min", "value": 75000},
        },
        {
            "title": "Exclude high-NRI counties",
            "rationale": (
                "Counties with FEMA NRI > 50 carry meaningful storefront "
                "downtime and insurance risk. Excluding them protects "
                "the operational base."
            ),
            "tool": "add_constraint",
            "args": {"dimension": "exclude_nri", "value": 50},
        },
        {
            "title": "Score the candidate set",
            "rationale": (
                "Blend median income (55%), commercial activity (30%), and "
                "broadband (15%) into a 0-100 thesis-fit score across the "
                "surviving counties."
            ),
            "tool": "recommend_candidates",
            "args": {"top_n": 10},
        },
        {
            "title": "Top candidates",
            "rationale": (
                "Indigo pins mark the top ten. The leaders are typically "
                "wealthy suburban DC and Bay Area counties — high income, "
                "dense commercial corridors, low hazard."
            ),
            "tool": "no_op",
        },
        {
            "title": "Open the top trade area",
            "rationale": (
                "Trade-area analysis answers 'who's already nearby?'. We "
                "drop a 50-mile radius around the top pick, list neighbour "
                "counties, and surface real competing businesses."
            ),
            "tool": "enter_trade_area",
            "args": {"top_pick": True, "radius_mi": 50},
        },
    ],
}


# ─── Validation ─────────────────────────────────────────────────────────


def _validate_args(tool: str, args: Optional[Dict[str, Any]]
                    ) -> Optional[str]:
    """Per-tool argument check. Returns error string or None."""
    a = args or {}
    if tool == "add_constraint":
        if a.get("dimension") not in _VALID_DIMENSIONS:
            return f"add_constraint dimension {a.get('dimension')!r} not in {_VALID_DIMENSIONS}"
        try:
            float(a.get("value"))
        except (TypeError, ValueError):
            return "add_constraint value must be numeric"
    elif tool == "remove_constraint":
        if a.get("dimension") not in _VALID_DIMENSIONS:
            return f"remove_constraint dimension {a.get('dimension')!r} invalid"
    elif tool == "recommend_candidates":
        n = a.get("top_n")
        if n is not None:
            try:
                ni = int(n)
                if not 1 <= ni <= 20:
                    return "recommend_candidates top_n must be 1..20"
            except (TypeError, ValueError):
                return "recommend_candidates top_n must be int"
    elif tool == "enter_trade_area":
        if not (a.get("geo_id") or a.get("top_pick")):
            return "enter_trade_area requires geo_id or top_pick:true"
        rm = a.get("radius_mi")
        if rm is not None:
            try:
                rmf = float(rm)
                if not 1 <= rmf <= 250:
                    return "enter_trade_area radius_mi must be 1..250"
            except (TypeError, ValueError):
                return "enter_trade_area radius_mi must be numeric"
    elif tool == "find_competition":
        if not (a.get("geo_id") or a.get("top_pick")):
            return "find_competition requires geo_id or top_pick:true"
        if not a.get("term"):
            return "find_competition requires term"
    # setup_thesis / exit_trade_area / no_op need no args
    return None


def validate_plan(raw: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Validate raw planner output. Returns (plan_dict, error_str).
    On error, plan_dict is None and the caller should fall back."""
    if not isinstance(raw, dict):
        return None, f"plan must be an object, got {type(raw).__name__}"
    # SPEC_094 — be lenient with LLM verbosity. Truncate any string
    # that overshoots the cap rather than rejecting the whole plan.
    raw = _truncate_raw(raw)
    try:
        m = PlanModel(**raw)
    except ValidationError as exc:
        return None, f"schema invalid: {exc.errors()[:3]}"
    # Per-tool arg checks
    for i, b in enumerate(m.beats):
        err = _validate_args(b.tool, b.args)
        if err:
            return None, f"beat {i+1} ({b.tool}): {err}"
    # First beat must be setup_thesis (auto-prepend if missing)
    plan = m.model_dump()
    if plan["beats"] and plan["beats"][0]["tool"] != "setup_thesis":
        plan["beats"].insert(0, {
            "title": "Loading thesis",
            "rationale": "Loading your saved thesis as context for the walkthrough.",
            "tool": "setup_thesis",
            "args": None, "wait_ms": 1200,
        })
        # Re-clamp beat count
        if len(plan["beats"]) > 12:
            plan["beats"] = plan["beats"][:12]
    return plan, None


# ─── Planner LLM call ───────────────────────────────────────────────────


PLANNER_SYSTEM_PROMPT = """\
You are a site-selection consultant authoring a guided walkthrough plan
for a Decision Map app. Output ONLY valid JSON. No prose, no markdown.

Schema:
{
  "title": "string (≤80 chars)",
  "summary": "string (≤240 chars, one paragraph)",
  "thesis_recap": "string (optional, ≤240 chars)",
  "beats": [
    {
      "title": "string (≤60 chars, banner step label)",
      "rationale": "string (2-3 analyst-grade sentences, ≤500 chars)",
      "tool": "<one of the tool names below>",
      "args": { ... } | null,
      "wait_ms": 1600
    }
  ]
}

Available tools:
  setup_thesis                          — fills the thesis form. No args. ALWAYS the first beat.
  add_constraint(dimension, value)      — pushes a filter chip. Dimensions:
                                            hhi_min, hhi_max, establishments_min, broadband_min, exclude_nri
  remove_constraint(dimension)          — pops a chip.
  recommend_candidates(top_n)           — paints fit-score + top-N pins. top_n ∈ [1,20].
  enter_trade_area(geo_id?, top_pick?, radius_mi?)
                                        — opens trade-area card. Pass top_pick:true to auto-pick the
                                          highest-fit candidate. radius_mi ∈ [1,250].
  exit_trade_area                       — closes the trade-area card.
  find_competition(geo_id?|top_pick?, radius_mi, term)
                                        — counts competing businesses (Yelp). term is a free-text
                                          industry label (e.g. "furniture stores").
  no_op                                 — used for context-only beats. No args.

Rules:
  - 4-8 beats total. First beat MUST be setup_thesis.
  - Pick constraint thresholds that match the user's thesis (use target_hhi_min if set;
    add exclude_nri only if the user mentioned risk or excluded NRI).
  - End with enter_trade_area(top_pick:true) UNLESS the thesis says otherwise.
  - Rationales are 2-3 sentences, analyst-grade. No emojis. No preambles like "Sure!" or
    "Great question!" — just the reasoning.
  - Don't invent layer ids, NAICS codes, or geographies. If unsure, use generic language.
"""


def _have_openai_key() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))


def _cache_key(thesis: Optional[Dict[str, Any]], prompt: Optional[str]) -> str:
    payload = json.dumps([thesis or {}, prompt or ""], sort_keys=True,
                          default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    item = _PLAN_CACHE.get(key)
    if not item:
        return None
    expiry, plan = item
    if time.time() > expiry:
        _PLAN_CACHE.pop(key, None)
        return None
    return plan


def _cache_put(key: str, plan: Dict[str, Any]) -> None:
    _PLAN_CACHE[key] = (time.time() + _TTL_SEC, plan)


def _user_message(thesis: Optional[Dict[str, Any]],
                   prompt: Optional[str]) -> str:
    parts: List[str] = []
    if thesis:
        # Keep it compact — the LLM has the schema; we just give the
        # thesis as a JSON blob to ground threshold picks.
        parts.append("User thesis:\n```json\n"
                      + json.dumps(thesis, indent=2, default=str)
                      + "\n```")
    if prompt:
        parts.append("User asks: " + prompt[:800])
    if not parts:
        parts.append("No thesis or prompt provided — generate a generic site-selection walkthrough.")
    parts.append(
        "Author a Plan JSON object now. 4-8 beats. First beat is setup_thesis. "
        "Output JSON ONLY."
    )
    return "\n\n".join(parts)


def generate_plan(
    db: Optional[Session],
    thesis_context: Optional[Dict[str, Any]] = None,
    prompt: Optional[str] = None,
    model: str = MODEL,
) -> Tuple[Dict[str, Any], Optional[str], bool]:
    """Return (plan, error_str_or_None, cache_hit_bool). Always returns
    a runnable plan — falls back to the frozen furniture sequence on
    any failure path."""
    key = _cache_key(thesis_context, prompt)
    cached = _cache_get(key)
    if cached is not None:
        return cached, None, True

    if not _have_openai_key():
        return _FALLBACK_PLAN, "OPENAI_API_KEY not set", False

    try:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
                {"role": "user",   "content": _user_message(thesis_context, prompt)},
            ],
            max_tokens=1500,
        )
        raw_text = resp.choices[0].message.content or "{}"
    except Exception as exc:  # noqa: BLE001
        logger.warning("planner LLM call failed: %s", exc)
        return _FALLBACK_PLAN, f"llm error: {exc}", False

    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return _FALLBACK_PLAN, f"json decode: {exc}", False

    plan, err = validate_plan(raw)
    if err or plan is None:
        return _FALLBACK_PLAN, f"validate: {err}", False

    _cache_put(key, plan)
    return plan, None, False


def clear_cache() -> None:
    """Reset the in-process cache. Used by tests."""
    _PLAN_CACHE.clear()
