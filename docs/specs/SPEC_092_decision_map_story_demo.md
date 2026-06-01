# SPEC 092 — Decision Map storytelling demo + kill the empty-landing modal

**Status:** Draft
**Task type:** service (frontend-heavy)
**Date:** 2026-05-31
**Builds on:** SPEC_087/088/089/090/091

## Trigger

> "I hate this [Where does your thesis fit?] modal — remove it. I just
> want to click Demo in the Investment Thesis. But then it should walk
> almost like a storytelling convention. The cards don't appear
> nothing happens."

## Goal

- **Remove `#map-empty` entirely.** No centered card on landing. The
  existing onboarding nudge under the header is the only hint.
- **Rewrite `loadFurnitureDemo` as a 6-beat storytelling sequence**
  that shows a top-center step banner ("Step N / 6 · …") and times
  the existing actions so the user can SEE the Decision Map being
  built around them.

## Acceptance Criteria

- [ ] `#map-empty` markup + CSS + JS references all gone.
- [ ] In init(), no centered modal in any entry mode. Blank-mode is
      truly blank (basemap + the existing onboarding nudge only).
- [ ] A new `#story-banner` element top-center of the map shows
      `Step N / 6 · <message>` with a progress dot row. Auto-hides
      4 seconds after the last beat.
- [ ] Clicking ⚡ Demo runs `runFurnitureStoryDemo()`:
      1. Beat 1: thesis loaded (fills form, opens 2 blocks).
      2. Beat 2: HHI ≥ $75K chip pushed.
      3. Beat 3: Exclude high-NRI chip pushed.
      4. Beat 4: fit-score paints (choropleth + top-N pins).
      5. Beat 5: legend + counter narration.
      6. Beat 6: auto-enter trade-area for top pick; narration
         "Loudoun VA · score 70/100 · click any pin to switch."
- [ ] Each beat is ≥1.4 s apart, with the banner text changing.
- [ ] "Skip ▶︎" button on the banner that fast-completes the rest.
- [ ] No accidental loops: re-clicking Demo while a story is running
      restarts cleanly (cancellation token).

## Files

| File | Action |
|------|--------|
| `frontend/atlas.html` | Remove #map-empty markup + CSS + JS; add #story-banner CSS + JS; rewrite loadFurnitureDemo |
| `tests/test_spec_092_decision_map_story_demo.py` | Frontend HTML/JS structural tests |

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_map_empty_removed | atlas.html no longer contains `#map-empty` (markup, CSS, JS) |
| T2 | test_story_banner_present | `#story-banner` markup + CSS class + Skip button |
| T3 | test_story_runner_exists | `function runFurnitureStoryDemo` + `function storyBeat` + cancellation token |
| T4 | test_demo_button_invokes_story | `thesis-demo` onClick wires to runFurnitureStoryDemo (not the old synchronous loadFurnitureDemo) |
| T5 | test_blank_init_no_modal | init blank-mode branch does not toggle a centered card |

## Design

### Beat sequence (high-level)

```js
async function runFurnitureStoryDemo() {
  cancelAnyRunningStory();
  const tok = { cancelled: false };
  RUNNING_STORY = tok;

  await storyBeat(1, 6, 'Loading thesis: Furniture stores in Austin metro');
  // … fill form, expand blocks, save thesis

  await storyBeat(2, 6, 'Filtering: must-have HHI ≥ $75K');
  if (tok.cancelled) return;
  addChip('hhi_min', 75000);          // existing helper

  await storyBeat(3, 6, 'Avoiding hazard-heavy areas (NRI > 50)');
  if (tok.cancelled) return;
  addChip('exclude_nri', 50);

  await storyBeat(4, 6, 'Scoring 3,248 counties against your thesis…');
  if (tok.cancelled) return;
  await fetchAndPaintFitScore(FURNITURE_DEMO); // paints choropleth + pins

  await storyBeat(5, 6, 'Top candidate: Loudoun VA (fit 70/100). Indigo pins are the top 10.');
  if (tok.cancelled) return;

  await storyBeat(6, 6, 'Opening trade area around Loudoun (25 mi)…');
  if (tok.cancelled) return;
  await enterTradeArea('51107', 50);
  setTimeout(closeStoryBanner, 4000);
}
```

### Story banner UI

```html
<div id="story-banner" hidden>
  <div class="kicker"><span class="step">Step 1 / 6</span> · Decision Map walkthrough</div>
  <div class="msg">…</div>
  <div class="dots"></div>
  <button class="skip">Skip ▶︎</button>
</div>
```

Top-center of `#map`. Glass-morphism style consistent with the rest of
the chrome.

### Why this also fixes "nothing happens"

Even if the user previously saw nothing visible, the timed step banner
makes EVERY beat explicit: chips animating in, counter changing, paint
fading, pins dropping. The user can't miss it.

## Verification

1. Hard refresh → blank map (no center modal). Header nudge still
   shows if no thesis.
2. Click ⚡ Demo from the Thesis panel → step banner top-center shows
   "Step 1 / 6 · Loading thesis…" then advances at ~1.6 s intervals.
3. Watch the chips drop in, the counter shrink (3,248 → 508 → 168),
   the choropleth fade in, the 10 pins drop, and finally the trade-
   area circle + competition pins appear around Loudoun VA.
4. Click Skip → banner closes immediately; current state preserved.

## Feedback History

- 2026-05-31 — Driving feedback: *"I hate this modal — remove it.
  Click Demo should walk like a storytelling convention. The cards
  don't appear nothing happens."*
