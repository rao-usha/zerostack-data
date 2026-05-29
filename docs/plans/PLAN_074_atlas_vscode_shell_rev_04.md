# PLAN 074 rev_04 — Map data-presentation system + Pilot chat rework

**Date:** 2026-05-28
**Parent:** PLAN_074_atlas_vscode_shell.md
**Prior revisions:** rev_01 (polish), rev_02 (thesis blocks), rev_03 (Pilot cleanup)
**Companion spec:** SPEC_086_atlas_map_presentation (created with this revision)

## Revision 04

Feedback after using the map + Pilot:

> "There's a bug when I overlay a layer on the map the fill color
> disappears. Then there's an issue that the colors make no sense and
> they're too thick over the map. I think we need to create a plan on
> how data is presented on the map so that it's more visually appealing.
> Right now it's too messy and hard to manage. Also you should be able
> to actively chat in the pilot — the questions shouldn't start at the
> top. That makes no sense. Tool calls should navigate the map and
> navigate the data."

Two themes: (A) **map data presentation** is messy — overlay bug,
opacity too heavy, color ramp unintuitive; and (B) the **Pilot is not a
real chat** — input lives in the global header, responses render
top-down, so it doesn't feel conversational.

## What Was Wrong

### A. Map presentation

1. **Overlay kills the choropleth fill.** Two interacting causes:
   - **Zoom-adaptive outline mode (SPEC_080):** at zoom ≥ 10,
     `styleFeature` sets `fillOpacity = 0` (outline-only). When the user
     is zoomed into a metro and adds a point overlay, the choropleth has
     *already* dropped its fill by design — so it reads as "the overlay
     made the fill disappear."
   - **Transition override bug (`applyLayerToBoundaries`):** the 400 ms
     fade does `fl.setStyle({ fillOpacity: state.opacity * eased })`
     AFTER `styleFeature` already chose a zoom-correct opacity — so the
     fade clobbers outline mode for one beat, then the next repaint
     (pan/zoom/overlay add) reasserts `fillOpacity = 0` and the fill
     visibly "vanishes." Inconsistent, janky.

2. **Opacity too heavy.** `state.opacity` defaults to **0.55**; at low
   zoom the choropleth sits like a thick slab over the basemap, hiding
   geography and labels. "Too thick over the map."

3. **Color ramp doesn't read.** A single-hue blue sequential ramp
   (`--c1..--c5`, dark-navy → light-blue) over a dark basemap has poor
   bucket separation and no semantic meaning — high vs low isn't
   obvious, and every layer looks identical. "The colors make no sense."

4. **Boundary strokes + hover are heavy.** 0.4–2.2 px borders plus a
   bright cyan hover that brings features to front — busy at metro zoom.

### B. Pilot chat

5. **Input is in the global header, not the chat.** `#pilot-input` lives
   in `#search-wrap` up top; the response renders top-down in the right
   column. You "ask from the top" and read down — the opposite of a
   chat. No persistent thread, no input at the bottom, no scrollback of
   the conversation as message bubbles.

6. **Tool calls do navigate the map already**, but because the chat
   isn't a thread, it doesn't *feel* like the conversation is driving
   the map. The ask is to make that loop legible: type at the bottom →
   message appears → map moves → assistant replies in-thread → options.

## What Was Fixed (→ SPEC_086)

### A. A coherent map-presentation system

- **Opacity model.** Default fill drops to **~0.40**; the opacity slider
  still overrides. Replace the binary fill→outline switch with a
  *graceful* zoom taper: full fill at low zoom → lighter fill (never 0)
  + crisper colored outline at high zoom, so fill never fully vanishes
  unless the user sets opacity to 0.
- **Fix the transition override** so the fade respects the zoom-adaptive
  target opacity (fade *to* the computed value, don't overwrite it).
- **Overlay coexistence.** When a point overlay is active, dim the
  choropleth fill a notch (not to 0) and give overlay points a
  contrasting halo so both read at once. Adding/removing an overlay
  never changes the base layer's own opacity rule.
- **Color ramps that mean something.** Move from one navy→blue ramp to a
  small set of perceptually-ordered ramps chosen per layer *polarity*:
  e.g. sequential teal→yellow for "more = notable", and a diverging ramp
  for risk-style layers. Brighter low-end so buckets separate on the
  dark basemap. (Exact palette pending the question below.)
- **Lighter chrome.** Thinner default borders, calmer hover (no jarring
  cyan flash), legend shows the actual bucket swatches + break values.

### B. Pilot as a real chat

- **Move the input to the BOTTOM of the right column** (sticky), with a
  send button; the global-header Pilot input is removed/retargeted.
- **Render a message thread**: user messages as right-aligned bubbles,
  assistant as left-aligned, options as buttons under the latest
  assistant turn. Newest at the bottom; auto-scroll. Tool activity stays
  in the bottom inspector (SPEC_085).
- The thread + live map nav makes the "chat drives the map" loop legible.

## Lessons Learned

- **Zoom-adaptive tricks need a floor.** Dropping fill to exactly 0 reads
  as a bug, not a feature. Taper, don't cut.
- **Don't restyle the same property in two places.** `styleFeature` and
  the transition both wrote `fillOpacity`; the second silently won.
  One owner per visual property.
- **A chat that takes input from the top isn't a chat.** Conversational
  UIs put the composer at the bottom and grow upward into history.
- **Color is semantics, not decoration.** A single ramp for every layer
  erases meaning; pick ramps by what "high" means for that layer.

## Files Affected

| File | Change |
|---|---|
| `frontend/atlas.html` | opacity model, ramp set, zoom taper, transition fix, overlay coexistence, lighter chrome, chat thread + bottom composer |
| `docs/plans/PLAN_074_atlas_vscode_shell_rev_04.md` | this file |
| `docs/plans/PLAN_074_atlas_vscode_shell.md` | `## Revisions` → rev_04 |
| `docs/specs/SPEC_086_atlas_map_presentation.md` | companion spec |
| `tests/test_spec_086_atlas_map_presentation.py` | tests |

## Acceptance (high level)

- [ ] Adding/removing a point overlay never makes the choropleth fill
      vanish; both render together.
- [ ] Default fill is lighter; basemap geography stays legible.
- [ ] Zoom-in tapers fill (never hard 0) and sharpens outlines.
- [ ] Color ramp reads clearly (buckets separate; high/low obvious).
- [ ] Pilot composer is at the bottom of the chat column; conversation
      renders as a bottom-growing message thread.
- [ ] Tool calls visibly drive the map mid-conversation.

## Open question for the user (answer before implementing the palette)

Color direction for choropleths — see the AskUserQuestion accompanying
this plan.
