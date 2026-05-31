# SPEC 088 — Decision Map Phase C: constraint pill chips + counter

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-05-31
**Plan:** PLAN_075 (Phase C of E)
**Test file:** tests/test_spec_088_decision_map_chips.py
**Builds on:** SPEC_087 (fit-score + blank landing)

## Goal

Add **removable pill-chip constraints** above the map with a live
candidate counter. Each chip is a hard filter (e.g. `HHI ≥ $80K`,
`exclude high-NRI`); non-matching geos drop out of the fit-score result.
Counter reads `38 of 3,143 candidates`. Chips persist + can be derived
from the thesis automatically.

## User decisions (locked in PLAN_075)

- Constraint funnel UI = pill chips above the map.
- Counter on the right of the chip strip.

## Acceptance Criteria

- [ ] A horizontal chip strip renders above `#map` (under `#map-actions`),
      with: zero or more constraint chips, an `+ Add` chip, and a
      right-aligned counter `N of M candidates`.
- [ ] Removing a chip (×) instantly re-fits.
- [ ] `+ Add` opens a compact menu of presets:
      `HHI ≥ [80000]`, `Density ≥ [1000]`, `Broadband ≥ [80]`,
      `Exclude high-NRI (≥ 50)`. One click adds a chip and re-fits.
- [ ] Backend `/atlas/fit-score` accepts an optional `constraints` body
      field; the response includes `total_candidates`, `filtered_candidates`,
      and `scores` only for the filtered set.
- [ ] On a fit-score paint, only filtered geos are colored; the rest
      fall to the "no data" color so they read as out-of-set.
- [ ] Top-N pins come from the filtered set.
- [ ] On thesis save, chips are **auto-derived** from the thesis when
      empty: HHI from `target_hhi_min`, density from
      `target_pop_density_min`, exclude-NRI from `exclude_layers`.
- [ ] Chip set persists to localStorage `atlas_constraints_v1` and is
      restored on reload.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_apply_constraint_hhi_min | only geos with income ≥ threshold survive |
| T2 | test_apply_constraint_exclude_high_nri | high-NRI geos are dropped |
| T3 | test_apply_constraints_intersection | multiple constraints → AND of all |
| T4 | test_compute_fit_score_with_constraints_returns_filtered | response counts match filtered set; top_n is from filtered set |
| T5 | test_compute_fit_score_constraints_none | no constraints = back-compat (no change vs SPEC_087) |
| T6 | test_fit_score_body_accepts_constraints | POST body schema valid for `constraints` list |
| T7 | test_frontend_chip_strip_markup | atlas.html has `#chip-strip`, `#chip-counter`, `#chip-add` |
| T8 | test_frontend_chip_helpers | atlas.html declares loadConstraints/saveConstraints/renderChips/applyChipsAndRefit |
| T9 | test_frontend_constraint_storage_key | uses `atlas_constraints_v1` |
| T10 | test_frontend_thesis_to_chips_derivation | helper `deriveChipsFromThesis(t)` present |

## Design

### Constraint shape (wire + storage)

```json
{
  "id": "c1",
  "dimension": "hhi_min",
  "value": 80000,
  "label": "HHI ≥ $80K"
}
```

Recognized dimensions: `hhi_min`, `hhi_max`, `density_min`,
`broadband_min`, `exclude_nri`.

### Backend

```python
# app/services/atlas/fit_score.py
_CONSTRAINT_DEFS = {
  "hhi_min":      {"layer": "demo_acs_median_income",       "op": ">="},
  "hhi_max":      {"layer": "demo_acs_median_income",       "op": "<="},
  "density_min":  {"layer": "demo_tract_population",        "op": ">="},
  "broadband_min":{"layer": "infra_broadband_subscription", "op": ">="},
  "exclude_nri":  {"layer": "disaster_nri",                  "op": "<="},
}

def _apply_constraints(db, constraints, candidates):
    """Intersection of geo_ids that pass every constraint."""
    survivors = set(candidates)
    for c in constraints:
        spec = _CONSTRAINT_DEFS.get(c.get("dimension"))
        if not spec: continue
        try:
            vals = build_layer(db, spec["layer"]).values or {}
        except Exception:
            continue
        threshold = float(c.get("value"))
        op = spec["op"]
        keep = set()
        for gid in survivors:
            v = vals.get(gid)
            if v is None: continue
            ok = (v >= threshold) if op == ">=" else (v <= threshold)
            if ok: keep.add(gid)
        survivors = keep
    return survivors

def compute_fit_score(db, thesis=None, top_n=10, constraints=None):
    # ... (existing recipe + normalize + weighted sum) ...
    # NEW: filter
    total = len(scores)
    if constraints:
        survivors = _apply_constraints(db, constraints, scores.keys())
        scores = {g: s for g, s in scores.items() if g in survivors}
    top = sorted(scores.items(), key=lambda kv: -kv[1])[:top_n]
    return { ..., "total_candidates": total,
              "filtered_candidates": len(scores) }
```

`FitScoreBody.constraints: Optional[List[Dict[str, Any]]] = None`.

### Frontend

- New CSS for `#chip-strip` (sticky-ish row inside `#map`, top:60px).
- JS:
  ```js
  let CONSTRAINTS = []; // [{id, dimension, value, label}]
  function loadConstraints() { /* localStorage atlas_constraints_v1 */ }
  function saveConstraints() { /* persist */ }
  function deriveChipsFromThesis(t) { /* returns chips, no side-effects */ }
  function renderChips() { /* paint #chip-strip + counter */ }
  function applyChipsAndRefit() { /* call fetchAndPaintFitScore with constraints */ }
  function openAddChipMenu() { /* small popover with 4 presets */ }
  ```
- `fetchAndPaintFitScore(thesis)` now passes `constraints: CONSTRAINTS`.
- After paint, update counter: `${filtered_candidates} of ${total_candidates}`.
- On `saveThesis` + `loadFurnitureDemo`: if `CONSTRAINTS` is empty,
  call `deriveChipsFromThesis(t)` to seed it before refitting.
- On `doResetThesis`: clear `CONSTRAINTS` too.

## Files

| File | Action |
|------|--------|
| `app/services/atlas/fit_score.py` | Add `_CONSTRAINT_DEFS`, `_apply_constraints`, constraints param |
| `app/api/v1/atlas.py` | `FitScoreBody.constraints` field |
| `frontend/atlas.html` | chip strip markup + CSS + JS helpers + integrate |
| `tests/test_spec_088_decision_map_chips.py` | T1-T10 |
| `docs/specs/SPEC_088_decision_map_constraint_chips.md` | this file |

## Verification

1. Hard-refresh w/ no thesis → blank map; chip strip empty + counter
   hidden (no fit-score painted).
2. Click ⚡ Demo → thesis loads, chips auto-derive from
   thesis (`HHI ≥ $75K`, `Exclude high-NRI`); fit-score paints; counter
   reads e.g. `420 of 3,143 candidates`.
3. Click `+ Add` → menu opens; pick `Density ≥ 1000` → new chip, counter
   drops, map re-paints with smaller filtered set.
4. Click `×` on a chip → counter grows back; map updates.
5. Reload → chips restored from localStorage.

## Feedback History

_None yet._
