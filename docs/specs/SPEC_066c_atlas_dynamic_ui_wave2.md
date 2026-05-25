# SPEC 066c — Atlas dynamic UI (Wave 2: time-cascade + heat + calendar)

**Status:** Draft
**Task type:** report (frontend + tiny server addition)
**Date:** 2026-05-24
**Plan:** PLAN_072 Wave 2 (Sub-waves a + b)
**Test file:** `tests/test_spec_066c_atlas_wave2.py`
**Builds on:** SPEC_066b (the dynamic-UI base — lives in the same `frontend/atlas.html`).

> **Numbering:** this is `066c`, not `068`, because it layers on top of
> the same `frontend/atlas.html` as SPEC_066 + SPEC_066b. It does not
> stand alone — it presupposes both prior layers exist.

## Goal

PLAN_072 Wave 2 sub-waves a + b — three "🥇 unique-to-Atlas" PLAN_070
features that were explicitly deferred from SPEC_066b. Make the
explorer compelling enough to fairly test the Phase-2 loop:

1. **EPA kernel-density heatmap** *(½ day)* — toggle a point overlay
   from individual dots to a Leaflet.heat KDE surface. Especially
   meaningful for `env_epa_facilities` (10k+ points cluttered as dots).
2. **Calendar heatmap (place panel)** *(½ day)* — for a county with
   FEMA disaster history, render a 28-year × 12-month cell grid
   showing event density per (year, month). Reads as "how busy is this
   county's disaster calendar."
3. **FEMA time-cascade scrubber** *(1 day, the signature feature)* —
   when `disaster_fema_declarations` is the active layer, show a
   year slider. As the user drags, the choropleth recolors to show
   *only that year's* declarations. Animates US disaster history at
   a glance — every year's storms, fires, floods light up in turn.

Sub-wave **c** (swipe compare + lasso-select) is **deferred** to a
follow-on SPEC_066d so this commit is reviewable as one concern.

## Acceptance Criteria

### Server additions
- [ ] `GET /atlas/place/{geo_id}/events?source=fema&limit=N` returns
      a list of raw FEMA events for the county
      `{events: [{date, type, title, disaster_number}, ...]}`.
- [ ] `GET /atlas/layer/disaster_fema_declarations/cascade` returns
      year-bucketed county counts
      `{years: [1999..2026], values_by_year: {year: {geo_id: count}}}`.
      Single fetch, ≤1MB JSON.

### Frontend — calendar heatmap
- [ ] When place panel opens and the place has ≥1 FEMA event, render a
      28-year × 12-month grid below the FEMA sparkline (Observable Plot
      `Plot.cell`), color-intensity by event count.
- [ ] Hover a cell → tooltip `{year, month, count, types}`.

### Frontend — kernel-density heat toggle
- [ ] Load `leaflet.heat` via CDN (~10KB).
- [ ] When ANY point overlay is active, header shows a "🔥 Heat"
      toggle button.
- [ ] Toggling on → re-render the overlay as `L.heatLayer(...)`;
      toggling off → return to dot CircleMarkers.
- [ ] Telemetry: `heatmap_enabled: {layer_id}`.

### Frontend — FEMA time-cascade scrubber
- [ ] When `disaster_fema_declarations` is the active base layer,
      render a horizontal year-slider above the legend.
- [ ] Slider shows years 1999..maxYear; current year in big readout.
- [ ] Dragging the slider re-styles the choropleth in <100ms per step
      (client-side lookup into the pre-fetched cascade payload).
- [ ] Telemetry: `time_scrubbed: {layer: disaster_fema_declarations, year}`.

### Quality
- [ ] No regression — `python scripts/smoke_atlas.py` still 33/33 pass.
- [ ] Bundle stays < 250 KB of JS over CDN (Leaflet.heat is the only new dep).
- [ ] All three features are individually keyboard-accessible.

## Test Cases (the smoke harness IS the regression surface)

| ID | What | Where |
|---|---|---|
| T1 | `/atlas/place/48201/events?source=fema` returns ≥20 Harris Co events | new pytest |
| T2 | `/atlas/layer/disaster_fema_declarations/cascade` returns `years` ≥ 28, payload ≤ 1MB | new pytest |
| T3 | cascade `values_by_year` keys are 4-digit years, county geo_ids are 5-digit FIPS | new pytest |
| T4 | smoke harness adds two new scenarios: scrubber-on-fema, heatmap-on-epa | smoke_atlas.py |
| T5 | smoke harness existing 33 scenarios still pass | smoke_atlas.py |

## Design Notes

### `events` endpoint
```python
@router.get("/place/{geo_id}/events")
def place_events(geo_id: str, source: str = "fema", limit: int = 200, db = Depends(get_db)):
    # 5-digit FIPS only; source whitelist {'fema'}
    ...
```
Returns `{events: [{date: 'YYYY-MM-DD', type, title, disaster_number}, ...]}` sorted desc.

### `cascade` endpoint
```python
@router.get("/layer/disaster_fema_declarations/cascade")
def fema_cascade(db = Depends(get_db)):
    # SELECT geo_id, year, COUNT(*); group + return
    ...
```
Returns:
```json
{
  "years": [1999, 2000, ..., 2026],
  "values_by_year": {
    "1999": {"48201": 1, "06037": 2, ...},
    ...
  }
}
```
Pre-flight measured 32,188 non-zero cells; JSON serializes to ~600 KB
gzip-compressible.

### Frontend — calendar heatmap (Observable Plot)
```js
Plot.plot({
  width: 320, height: 110,
  x: { axis: "top", tickRotate: -45 },
  y: { type: "linear", reverse: true, label: "month" },
  color: { type: "sqrt", scheme: "ylorrd", legend: false },
  marks: [
    Plot.cell(events, {
      x: d => d.year,
      y: d => d.month,
      fill: "count",
      tip: true,
    }),
  ],
})
```
Aggregated client-side from the events array.

### Frontend — heat toggle
```js
import "leaflet.heat";  // via CDN
function activatePointOverlay(layerId, mode='dots') {
  const features = await fetchPointLayer(layerId);
  if (mode === 'heat') {
    OVERLAY_LAYER = L.heatLayer(
      features.map(f => [f.geometry.coordinates[1], f.geometry.coordinates[0], 0.5]),
      { radius: 18, blur: 22, maxZoom: 10 }
    );
  } else { /* existing CircleMarker path */ }
  OVERLAY_LAYER.addTo(MAP);
}
```

### Frontend — time scrubber
- On entering disaster_fema layer: fetch cascade payload, render slider DOM
- On slider change: lookup `cascade.values_by_year[year]`, swap `CURRENT_LAYER_RESULT.values`,
  call `applyLayerToBoundaries(CURRENT_LAYER_RESULT, false)` for instant recolor
- All-time view = slider position "ALL" (current default)

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_066c_atlas_dynamic_ui_wave2.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_066c_atlas_dynamic_ui_wave2` |
| `tests/test_spec_066c_atlas_wave2.py` | Create | T1-T3 server endpoint tests |
| `app/services/atlas/series.py` | Modify | Add `fetch_place_events` + `fetch_fema_cascade` |
| `app/api/v1/atlas.py` | Modify | Two new endpoints |
| `frontend/atlas.html` | Modify | Add Leaflet.heat CDN; calendar heatmap; heat toggle; scrubber |
| `scripts/smoke_atlas.py` | Modify | Add two new scenarios |

## Verification (manual)

1. `docker-compose restart api` — pickup new endpoints
2. `curl /atlas/place/48201/events?source=fema | jq '.events[0]'` returns date/type/title.
3. `curl /atlas/layer/disaster_fema_declarations/cascade | jq '.years|length'` returns ≥28.
4. Open `/atlas.html?place=48201` — place panel shows calendar grid with 27 Harris Co events.
5. Open `/atlas.html?layer=disaster_nri&overlay=env_epa_facilities` — header "🔥 Heat" button; toggle on → KDE blobs replace dots.
6. Open `/atlas.html?layer=disaster_fema_declarations` — year slider above legend; drag through years; choropleth re-colors per year (2024 Hurricane Helene year should light up a band of southeastern counties).
7. Run `python scripts/smoke_atlas.py` — all scenarios pass.

## Feedback History

_No corrections yet._
