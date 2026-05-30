# SPEC 087 — Decision Map Phase A+B: blank landing + fit-score + top-N pins

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-05-30
**Plan:** PLAN_075_decision_map_framework (Phase A+B of E)
**Test file:** tests/test_spec_087_decision_map_fit_score.py

## Goal

Replace the "default-on choropleth" entry with a thesis-driven Decision
Map. Land **blank** with a prompt. When a thesis exists, paint a
**fit-score choropleth** (0-100) keyed to the industry and surface the
**top-10 candidate pins** on top. Legend shows the weighted blend.

## User decisions (from PLAN_075 questions, locked)

- Landing: pure blank with centered prompt.
- Fit-score expression: fit-score choropleth + top-N pins, legend
  shows weighted blend.
- Constraint funnel: pill chips above the map. *(Deferred to SPEC_088
  to keep this ship focused.)*

## Acceptance Criteria

- [ ] On first load with no saved thesis, the map shows ONLY the
      basemap + a centered prompt card. No choropleth, no legend.
- [ ] On first load with a saved thesis, `/atlas/fit-score` is fetched
      and rendered as a 0-100 choropleth.
- [ ] Top 10 candidate counties are rendered as numbered map pins (①
      through ⑩). Clicking a pin opens the place panel + zooms.
- [ ] Legend label reads "Thesis fit (0-100)"; an explainer line shows
      the weighted blend, e.g. "60% Median income · 30% Population
      density · 10% Broadband."
- [ ] Saving the thesis re-fetches and re-paints the fit-score (no
      page reload).
- [ ] Clicking ⚡ Demo (furniture preset) loads thesis → fit-score
      renders for the retail recipe end-to-end.
- [ ] The "default layer" (income) NO LONGER auto-loads on landing.
- [ ] Toggling a specific layer from the Layers view still works —
      it overrides the fit-score view as an investigative overlay.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_classify_industry_retail | "Furniture stores" → "retail" recipe |
| T2 | test_classify_industry_housing | "Apartment buildings" → "housing" |
| T3 | test_classify_industry_industrial | "Distribution warehouse" → "industrial" |
| T4 | test_classify_industry_default | empty / unknown → "default" |
| T5 | test_compute_fit_score_returns_scores_and_top_n | non-empty scores + top_n list |
| T6 | test_fit_score_endpoint_accepts_optional_thesis | POST with no thesis still works |
| T7 | test_fit_score_endpoint_thesis_present | populated thesis returns matching recipe |
| T8 | test_fit_score_weights_renormalize_when_a_layer_fails | weights still sum to ~1.0 |
| T9 | test_frontend_empty_landing_marker | atlas.html includes the centered prompt overlay + skipDefaultLayer init guard |
| T10 | test_frontend_paint_fit_score_helper | paintFitScore + renderTopNPins present |

## Design

### Backend: `app/services/atlas/fit_score.py`

```python
_RECIPES = {
  "retail":     [income .60, density .30, broadband .10],
  "housing":    [density .50, income .30, broadband .20],
  "industrial": [broadband .50, density .30, income .20],
  "default":    [income .60, density .40],
}
def classify_industry(thesis) -> str: ...   # keyword route on industry_label
def compute_fit_score(db, thesis, top_n=10) -> {
  "scores": {geo_id: 0-100}, "weights": [{layer_id,label,weight}],
  "top_n":  [{geo_id, score, name?}], "recipe": "retail"|...
}
```

Per component: `build_layer(db, layer_id).values` → normalize 0..1 via
min-max → weighted sum (weights renormalized over components that
actually loaded) → ×100. Top-N from sorted scores.

### Backend: `app/api/v1/atlas.py`

```python
class FitScoreBody(BaseModel):
    thesis: Optional[Dict[str, Any]] = None
    top_n: Optional[int] = 10

@router.post("/fit-score")
def atlas_fit_score(body, db=Depends(get_db)):
    return compute_fit_score(db, thesis=body.thesis, top_n=body.top_n or 10)
```

### Frontend: blank landing + fit-score

```js
// In init() — replace the existing "fetch the default layer" block.
const thesis = JSON.parse(localStorage.getItem('atlas_thesis_v1') || '{}');
if (!thesisIsEmpty(thesis)) {
  await fetchAndPaintFitScore(thesis);   // thesis exists → paint
} else {
  document.getElementById('map-empty').classList.add('show');  // centered prompt
}

async function fetchAndPaintFitScore(thesis) {
  const r = await fetch(API + '/fit-score',
    {method:'POST', headers:{'Content-Type':'application/json'},
     body:JSON.stringify({ thesis, top_n: 10 })}).then(r => r.json());
  paintFitScore(r);
}

function paintFitScore(r) {
  document.getElementById('map-empty').classList.remove('show');
  // Treat as synthetic choropleth result for the existing pipeline
  const synth = { layer_id:'fit_score', grain:'county',
    values: r.scores, legend:{ breaks:[20,40,60,80], min:0, max:100, unit:'' }};
  ACTIVE_RAMP = RAMPS.more;  // teal→gold reads "low→high fit"
  applyLayerToBoundaries(synth, true);
  renderFitLegend(r.weights);   // overrides label + adds weights explainer
  renderTopNPins(r.top_n);
}
```

### Frontend: top-N pins

Numbered Leaflet markers via `divIcon`. Click → existing `selectPlace`.
Pins live in a single `L.layerGroup` we add/remove cleanly.

### Frontend: empty-landing prompt

```html
<div id="map-empty">
  <div class="card">
    <div class="title">Decision Map</div>
    <p>Set your <b>investment thesis</b> on the left — I'll show you
       where it fits.</p>
    <button id="map-empty-demo">⚡ Try the furniture-store demo</button>
  </div>
</div>
```

CSS: absolutely centered over the map, glass-morphism card, hides via
`.show` toggle.

### Re-paint on thesis save

`saveThesis()` already exists. Add: after save, if non-empty, call
`fetchAndPaintFitScore(thesis)`. Already calls `pickDefaultLayer` / 
`activateLayer` — replace that path with the fit-score path.

## Rubric Checklist

- [ ] Async-safe, bounded (top_n capped 50)
- [ ] Parameterised SQL (reuses `build_layer`)
- [ ] No PII
- [ ] Honest defaults — recipe is documented; legend always shows weights
- [ ] Tests cover happy path + boundaries (empty thesis, failing component)

## Files

| File | Action |
|------|--------|
| `app/services/atlas/fit_score.py` | Create — recipes, classify_industry, compute_fit_score |
| `app/api/v1/atlas.py` | Add FitScoreBody + POST /fit-score |
| `frontend/atlas.html` | Empty-landing card + paintFitScore + renderTopNPins + init guard + thesis-save re-fit |
| `tests/test_spec_087_decision_map_fit_score.py` | T1-T10 |
| `docs/plans/PLAN_075_decision_map_framework.md` | (already created) |
| `docs/specs/SPEC_087_decision_map_fit_score.md` | this file |

## Verification

1. Clear localStorage + hard refresh → blank basemap + prompt card.
2. Click ⚡ Demo → thesis populates → map paints fit-score (teal→gold)
   + top-10 pins. Legend reads "Thesis fit (0-100) · 60% Median income
   · 30% Population density · 10% Broadband."
3. Click a top-N pin → place panel opens with that county's facts.
4. Toggle a specific layer (e.g. NRI) → that layer overrides the
   fit-score view (existing behaviour).
5. Hit "Reset" on the thesis → confirm → map returns to blank prompt.

## Phasing (PLAN_075)

This ships A + B. Next:
- **SPEC_088** — constraint pill chips + candidate counter (Phase C)
- **SPEC_089** — trade-area mode (Phase D)
- **SPEC_090** — Pilot tools (Phase E)
