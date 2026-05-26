# SPEC 066e — Generalize the time-cascade scrubber

**Status:** Draft
**Task type:** report (frontend refactor)
**Date:** 2026-05-25
**Plan:** PLAN_072 follow-on (Wave 2 sub-wave c was deferred to SPEC_066d; this is e)
**Test file:** _none_ — verified by SPEC_073 smoke harness via two scenarios (FEMA + CBP)
**Builds on:** SPEC_066c (the FEMA scrubber); SPEC_075 (the CBP cascade endpoint).

## Goal

The SPEC_066c scrubber is hardcoded to `disaster_fema_declarations` —
`FEMA_CASCADE`, `FEMA_ALL_VALUES`, `activateFemaScrubber()`. SPEC_075
shipped a second cascade endpoint (`econ_cbp_establishments_county/cascade`)
with the same shape but the UI can't consume it. This spec generalizes
the scrubber so any layer that registers a cascade gets the same year-
slider behavior for free.

## Acceptance Criteria

- [ ] Frontend `SCRUBBER_LAYERS = { layer_id: {label} }` config drives
      which layers show the scrubber on activation.
- [ ] FEMA (`disaster_fema_declarations`) continues to work — no regression.
- [ ] CBP (`econ_cbp_establishments_county`) shows the scrubber when active;
      slider re-colors choropleth per year (2018-2022).
- [ ] Scrubber DOM label updates per layer ("FEMA year" vs "CBP year").
- [ ] Cascade payloads cached per-layer (`CASCADES[layer_id] = ...`).
- [ ] Smoke harness gains a `cbp-time-scrubber` scenario; existing
      `fema-time-scrubber` still passes.
- [ ] No regression — 39/39 smoke scenarios stay green.

## Design Notes

State refactor:
```js
// Before — FEMA-hardcoded
let FEMA_CASCADE = null;
let FEMA_ALL_VALUES = null;

// After — generic
const SCRUBBER_LAYERS = {
  'disaster_fema_declarations':       { label: 'FEMA year' },
  'econ_cbp_establishments_county':   { label: 'CBP year' },
};
let CASCADES = {};               // layer_id → cascade payload
let SCRUBBER_ALL_VALUES = null;  // pre-scrub all-years aggregate
let SCRUBBER_LAYER_ID = null;    // which layer the scrubber is bound to
```

Function rename: `activateFemaScrubber → activateScrubber(layerId)`,
`deactivateFemaScrubber → deactivateScrubber()`.

`applyScrubberYear` reads from `CASCADES[SCRUBBER_LAYER_ID]` instead of
the FEMA-specific global.

In `onLayerClick` and `init()`, replace:
```js
if (spec.id === 'disaster_fema_declarations') await activateFemaScrubber();
else deactivateFemaScrubber();
```
with:
```js
if (SCRUBBER_LAYERS[spec.id]) await activateScrubber(spec.id);
else deactivateScrubber();
```

That's the whole refactor — ~25 LOC delta in `frontend/atlas.html`.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_066e_scrubber_generalization.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_066e_scrubber_generalization` |
| `frontend/atlas.html` | Modify | Scrubber state + functions generalized |
| `scripts/smoke_atlas.py` | Modify | Add `cbp-time-scrubber` scenario |

## Verification

1. `/atlas.html?layer=disaster_fema_declarations` — scrubber shows
   "FEMA year"; drag through 1999-2026.
2. `/atlas.html?layer=econ_cbp_establishments_county` — scrubber shows
   "CBP year"; drag through 2018-2022.
3. Smoke harness 40/40 (added one scenario).

## Feedback History

_No corrections yet._
