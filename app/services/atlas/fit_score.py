"""SPEC_087 — Atlas Decision Map: thesis-driven fit-score.

Composes a weighted blend of existing choropleth layers into a single
0–100 "thesis fit" score per geo_id, with a top-N candidate list. The
fit-score replaces the old "default layer choropleth" as the entry-point
view of the map (PLAN_075).

Recipes are intentionally simple + interpretable: the user always sees
the weighted-component breakdown in the legend. Future revisions can
add more sophisticated routing (per-vertical sub-layers, growth signals,
spatial smoothing) — keep the contract stable.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.services.atlas.layers import build_layer

logger = logging.getLogger(__name__)


# ─── Recipes ──────────────────────────────────────────────────────────────
# Each recipe is an ordered list of contributing components. Polarity +1
# means "more is better"; -1 would mean "less is better" (not yet used).
# Layer ids must match the real registry — see /atlas/layers.

_RECIPES: Dict[str, List[Dict[str, Any]]] = {
    # SPEC_088 — county-grain only, so constraint chips applied to the
    # same set as the score actually intersect. (Mixing tract + county
    # layers made the funnel collapse to 0 even on plausible inputs.)
    "retail": [
        {"layer_id": "demo_acs_median_income",
         "label": "Median income",        "weight": 0.55, "polarity": 1},
        {"layer_id": "econ_cbp_establishments_county",
         "label": "Commercial activity",  "weight": 0.30, "polarity": 1},
        {"layer_id": "infra_broadband_subscription",
         "label": "Broadband access",     "weight": 0.15, "polarity": 1},
    ],
    "housing": [
        {"layer_id": "realestate_building_permits",
         "label": "Building permits",     "weight": 0.40, "polarity": 1},
        {"layer_id": "demo_acs_median_income",
         "label": "Median income",        "weight": 0.35, "polarity": 1},
        {"layer_id": "econ_cbp_establishments_county",
         "label": "Commercial activity",  "weight": 0.25, "polarity": 1},
    ],
    "industrial": [
        {"layer_id": "infra_broadband_subscription",
         "label": "Broadband access",     "weight": 0.40, "polarity": 1},
        {"layer_id": "econ_cbp_establishments_county",
         "label": "Commercial activity",  "weight": 0.35, "polarity": 1},
        {"layer_id": "demo_acs_median_income",
         "label": "Median income",        "weight": 0.25, "polarity": 1},
    ],
    "default": [
        {"layer_id": "demo_acs_median_income",
         "label": "Median income",        "weight": 0.50, "polarity": 1},
        {"layer_id": "econ_cbp_establishments_county",
         "label": "Commercial activity",  "weight": 0.30, "polarity": 1},
        {"layer_id": "infra_broadband_subscription",
         "label": "Broadband access",     "weight": 0.20, "polarity": 1},
    ],
}


def classify_industry(thesis: Optional[Dict[str, Any]]) -> str:
    """Route a thesis to a recipe key by industry-label keywords. Mirrors
    pickDefaultLayer() on the frontend; keep the two in sync."""
    if not isinstance(thesis, dict):
        return "default"
    label = (thesis.get("industry_label") or "").lower()
    if any(k in label for k in (
        "housing", "apartment", "real estate", "real-estate",
        "condo", "residential", "multifamily",
    )):
        return "housing"
    if any(k in label for k in (
        "industrial", "warehouse", "manufactur", "logist",
        "distribution", "factory", "3pl",
    )):
        return "industrial"
    if any(k in label for k in (
        "retail", "restaurant", "cafe", "store", "shop",
        "coffee", "bar", "salon", "boutique",
    )):
        return "retail"
    return "default"


# ─── SPEC_088 — Constraint funnel ─────────────────────────────────────────
# Hard filters on top of the fit-score. Each dimension maps to a layer
# from the registry and a comparator. The frontend renders these as
# removable pill chips above the map.
_CONSTRAINT_DEFS: Dict[str, Dict[str, Any]] = {
    # All county-grain so the survivor set intersects the fit-score set.
    "hhi_min":            {"layer": "demo_acs_median_income",        "op": ">="},
    "hhi_max":            {"layer": "demo_acs_median_income",        "op": "<="},
    "establishments_min": {"layer": "econ_cbp_establishments_county", "op": ">="},
    "broadband_min":      {"layer": "infra_broadband_subscription",  "op": ">="},
    # exclude_nri means "keep only places with NRI ≤ value" (default = 50)
    "exclude_nri":        {"layer": "disaster_nri",                  "op": "<="},
}


def _apply_constraints(
    db: Session,
    constraints: List[Dict[str, Any]],
    candidates,
) -> set:
    """Intersect candidate geo_ids with every constraint. A constraint
    whose layer fails to load is silently skipped (the survivor set is
    not narrowed by a broken constraint)."""
    survivors = set(candidates)
    if not constraints:
        return survivors
    # Cache layer values across constraints that share a layer
    layer_cache: Dict[str, Dict[str, Any]] = {}
    for c in constraints:
        spec = _CONSTRAINT_DEFS.get(c.get("dimension"))
        if not spec:
            continue
        try:
            threshold = float(c.get("value"))
        except (TypeError, ValueError):
            continue
        layer_id = spec["layer"]
        if layer_id not in layer_cache:
            try:
                layer_cache[layer_id] = build_layer(db, layer_id).values or {}
            except Exception as exc:  # noqa: BLE001
                logger.warning("constraint layer %s failed: %s", layer_id, exc)
                layer_cache[layer_id] = {}
                continue
        vals = layer_cache[layer_id]
        op = spec["op"]
        keep = set()
        for gid in survivors:
            v = vals.get(gid)
            if v is None:
                continue
            try:
                vf = float(v)
            except (TypeError, ValueError):
                continue
            ok = (vf >= threshold) if op == ">=" else (vf <= threshold)
            if ok:
                keep.add(gid)
        survivors = keep
    return survivors


def _normalize(values: Dict[str, Any]) -> Dict[str, float]:
    """Min-max normalize a {geo_id: value} dict to 0..1. Skips Nones."""
    if not values:
        return {}
    nums: Dict[str, float] = {}
    for gid, v in values.items():
        if v is None:
            continue
        try:
            nums[gid] = float(v)
        except (TypeError, ValueError):
            continue
    if not nums:
        return {}
    lo, hi = min(nums.values()), max(nums.values())
    if hi == lo:
        return {gid: 0.5 for gid in nums}
    span = hi - lo
    return {gid: (v - lo) / span for gid, v in nums.items()}


def compute_fit_score(
    db: Session,
    thesis: Optional[Dict[str, Any]] = None,
    top_n: int = 10,
    constraints: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return per-geo fit scores 0–100, the effective weight breakdown,
    and the top-N candidates by score.

    The fit score is the polarity-signed, weight-normalized sum of each
    component layer's min-max-normalized value. Weights are renormalized
    over the components that actually loaded (a failing layer doesn't
    silently down-weight the whole score).
    """
    recipe_key = classify_industry(thesis)
    recipe = _RECIPES.get(recipe_key, _RECIPES["default"])
    top_n = max(1, min(50, int(top_n) if top_n else 10))

    # Pull each component layer's values and normalize to 0..1
    components: List[Dict[str, Any]] = []
    for c in recipe:
        try:
            res = build_layer(db, c["layer_id"])
            vals = _normalize(res.values or {})
        except Exception as exc:  # noqa: BLE001
            logger.warning("fit-score: component %s failed: %s",
                            c["layer_id"], exc)
            continue
        if vals:
            components.append({**c, "_normalized": vals})

    if not components:
        return {"scores": {}, "weights": [], "top_n": [],
                "recipe": recipe_key,
                "total_candidates": 0, "filtered_candidates": 0}

    # Renormalize weights so the components that DID load sum to 1.0
    total_w = sum(c["weight"] for c in components)
    if total_w <= 0:
        return {"scores": {}, "weights": [], "top_n": [],
                "recipe": recipe_key}
    for c in components:
        c["_w_eff"] = c["weight"] / total_w

    # Score each geo_id present in at least one component
    all_gids: set = set()
    for c in components:
        all_gids.update(c["_normalized"].keys())

    scores: Dict[str, int] = {}
    for gid in all_gids:
        s = 0.0
        for c in components:
            # 0.5 fallback for missing geo_ids keeps the score
            # comparable; alternatives: skip, or penalize.
            v = c["_normalized"].get(gid, 0.5)
            s += v * c["_w_eff"] * c["polarity"]
        # Clamp and scale to 0..100
        scores[gid] = int(round(max(0.0, min(1.0, s)) * 100))

    # SPEC_088 — apply the constraint funnel. Surviving geo_ids keep
    # their scores; the rest drop out of the response so the frontend
    # paints them as "no data".
    total_candidates = len(scores)
    if constraints:
        survivors = _apply_constraints(db, constraints, scores.keys())
        scores = {gid: s for gid, s in scores.items() if gid in survivors}
    filtered_candidates = len(scores)

    top = sorted(scores.items(), key=lambda kv: -kv[1])[:top_n]

    return {
        "scores": scores,
        "weights": [
            {"layer_id": c["layer_id"], "label": c["label"],
             "weight": round(c["_w_eff"], 2)}
            for c in components
        ],
        "top_n": [{"geo_id": gid, "score": sc} for gid, sc in top],
        "recipe": recipe_key,
        "total_candidates": total_candidates,
        "filtered_candidates": filtered_candidates,
    }
