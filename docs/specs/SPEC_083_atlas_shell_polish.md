# SPEC 083 — Atlas shell polish + default-layer correction

**Status:** Draft
**Task type:** service (frontend-heavy)
**Date:** 2026-05-27
**Plan:** PLAN_074_atlas_vscode_shell_rev_01
**Test file:** tests/test_spec_083_atlas_shell_polish.py
**Builds on:** SPEC_082 (workspace shell)

## Goal

Replace emoji icons with inline SVG, refresh the palette, soften
borders, add micro-transitions, polish typography. Change the
default landing layer from `disaster_nri` to
`median_household_income_acs`, and pick a smarter default when the
user has saved a thesis. Add a first-load onboarding nudge.

## Acceptance Criteria

- [ ] Activity bar icons are inline SVG (briefcase, layers stack,
      location pin, clock), not emoji. Crisp at 24×24, single-color
      with `currentColor` so they recolor on hover/active.
- [ ] New palette: `--primary:#7c83ff`, `--accent:#67e8f9`, panel
      bg slightly warmer, border opacity dropped.
- [ ] Activity-bar active state shows a 2-px gradient bar on the
      left + subtle inner glow (instead of flat color block).
- [ ] Sashes have a hover affordance (subtle line indicator), not
      just background-color flash.
- [ ] Form inputs: focus ring (2-px primary @ 30% opacity outset),
      rounded 7-px, larger padding.
- [ ] Save buttons: gradient bg, 1-px translate on press,
      box-shadow lift on hover.
- [ ] Bottom-panel tabs: underline indicator transitions smoothly
      between active tabs (180-ms ease).
- [ ] View-switch in left panel uses a 4-px y-translate +
      opacity fade (120-ms).
- [ ] Typography: load Inter via Google Fonts; fallback to
      system font stack.
- [ ] Bottom panel + status bar use glass-morphism
      (`backdrop-filter: blur(8px)` with translucent bg).
- [ ] Subtle soft drop-shadow under floating overlays.
- [ ] **Default layer** changed from `disaster_nri` to
      `median_household_income_acs`.
- [ ] `pickDefaultLayer(thesis)` helper: if thesis.industry_label
      contains "housing"/"apartment"/"real estate" → density;
      "retail"/"restaurant"/"store" → income; "industrial"/
      "warehouse"/"manufactur" → broadband; else income.
- [ ] First-load banner if no thesis saved:
      *"Set your investment thesis on the left — I'll tailor the
       map to your industry."*
      Dismissible; sticks dismissed-state in localStorage.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_pick_default_layer_housing | Housing thesis → density |
| T2 | test_pick_default_layer_retail | Retail thesis → income |
| T3 | test_pick_default_layer_industrial | Industrial → broadband |
| T4 | test_pick_default_layer_empty_thesis | No thesis → income |
| T5 | test_pick_default_layer_unknown_industry | Unknown industry → income (fallback) |

Frontend visual polish verified manually (screenshot diff would
need a headless browser harness not currently set up).

## Design Notes

### SVG icon set

Drawn inline in the HTML so each icon recolors via `currentColor`.
Approximate paths (stroke-width 1.75, linecap round, linejoin round):

- **Thesis (briefcase):** `M3 7 h18 v12 H3z M8 7 v-2 a2 2 0 0 1 2-2 h4 a2 2 0 0 1 2 2 v2`
- **Layers (stack):** 3 stacked parallelograms
- **Places (pin):** `M12 22 s-7-8-7-13 a7 7 0 0 1 14 0 c0 5-7 13-7 13z` + circle
- **History (clock):** circle + `M12 7 v5 l3 2`

### Palette

```css
:root {
  --bg:        #0a0e1a;
  --panel:     #161d31;   /* slightly warmer */
  --panel-2:   #1c2440;
  --primary:   #7c83ff;   /* richer indigo */
  --primary-2: #6066ff;
  --accent:    #67e8f9;   /* softer cyan */
  --text:      #e7eaf1;
  --muted:     #9aa3b8;
  --dim:       #6b7280;
  --border:    rgba(255,255,255,.08);   /* 8% white over panel */
  --border-2:  rgba(255,255,255,.04);
  --warn:      #fbbf24;
  --success:   #34d399;
  --shadow-soft: 0 1px 3px rgba(0,0,0,.25), 0 8px 24px rgba(0,0,0,.35);
  --shadow-inset: inset 0 0 0 1px rgba(255,255,255,.04);
}
```

### `pickDefaultLayer(thesis)` algorithm

```js
function pickDefaultLayer(thesis) {
  const industry = (thesis?.industry_label || '').toLowerCase();
  if (/housing|apartment|real ?estate|condo|residential/.test(industry))
    return 'population_density';
  if (/retail|restaurant|cafe|store|shop|coffee|bar|salon/.test(industry))
    return 'median_household_income_acs';
  if (/industrial|warehouse|manufactur|logist|distribution|factory/.test(industry))
    return 'broadband_fixed_25_3';
  return 'median_household_income_acs';  // safe universal default
}
```

### Onboarding banner

```html
<div id="onboarding-nudge" class="show">
  <span>💡 Set your <b>investment thesis</b> on the left —
        I'll tailor the map to your industry.</span>
  <button id="onboarding-dismiss">×</button>
</div>
```

CSS positions it under the header, glass-morphism bg, gentle slide-in
on first load. Hidden if `atlas_onboarding_dismissed_v1` is set in
localStorage or if a thesis already exists.

## Rubric Checklist (service)

- [ ] Async-safe (no blocking calls)
- [ ] Bounded (all string fields capped)
- [ ] No SQL changes
- [ ] No PII
- [ ] Honest defaults — fallback layer is universally meaningful
- [ ] Tests cover the new helper + boundaries

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `frontend/atlas.html` | Modify | SVG icons, palette refresh, polish CSS, default layer, pickDefaultLayer, onboarding nudge |
| `app/services/atlas/pilot.py` | Optional minor | None expected — thesis flow unchanged |
| `docs/plans/PLAN_074_atlas_vscode_shell_rev_01.md` | Created | (done) |
| `docs/specs/SPEC_083_atlas_shell_polish.md` | Create | this file |
| `tests/test_spec_083_atlas_shell_polish.py` | Create | T1-T5 (likely JS-level checks via HTML string parsing) |

## Verification

1. Reload `/atlas.html` — first impression should feel premium:
   - SVG icons crisp + monochromatic
   - Subtle shadows on overlays
   - Smoother color palette
2. Open with no thesis → map renders with median household income
   active by default (not NRI). Onboarding banner visible.
3. Save a thesis with industry "Furniture stores" → reload → still
   income (correct for retail).
4. Save a thesis with industry "Apartment buildings" → reload →
   population density active.
5. Save thesis with industry "Distribution warehouse" → reload →
   broadband active.
6. Hover activity-bar icons → color smoothly transitions; active
   icon has left gradient bar.
7. Switch left views → 120-ms fade + slide.
8. Click a thesis-save button → micro press animation.

## Feedback History

- 2026-05-27 — Original feedback that drove this spec:
  *"The icons and the borders look like 1990s windows this needs
  to be sexier fix it the ui. Also still nto sure why were focused
  on Natural Hazard risk as the primary customer"* → see
  `memory/feedback/corrections.md` 2026-05-27 entry.
