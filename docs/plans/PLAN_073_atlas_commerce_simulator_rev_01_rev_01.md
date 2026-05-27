# PLAN 073 rev_01 → rev_01_rev_01 — small-polish iteration

**Status:** Approved (this commit)
**Date:** 2026-05-27
**Parent:** `PLAN_073_atlas_commerce_simulator_rev_01.md`
**Trigger:** User feedback after the laid-out action list — *"why don't you fix all those small things"*

## Revision 01

A polish-iteration appendix to PLAN_073 rev_01. Adds six small,
independent fixes that sharpen the existing surface without changing
the audience, phase sequencing, or differentiator. None of these
were enumerated in rev_01's Phase A-G work; they emerged from real
use of the shipped Phase B v1.1 surface.

## What Was Wrong

Strictly speaking, nothing was *wrong* with PLAN_073 rev_01. Six
small UX awkwardnesses surfaced during click-through:

1. **Citation validator false-positives on FIPS codes.** The regex
   for "specific numerical claims" flags 5-digit FIPS codes like
   `48201` as unlinked claims. They're not claims — they're
   identifiers the model uses to reference places. Validator should
   skip these contextually.

2. **No way to cancel a mid-flight Pilot run.** Once you submit a
   question, the only escape is closing the panel. The streaming
   endpoint has nothing wired to an AbortController; the agent keeps
   spending tokens.

3. **Pilot conversation history dies on refresh.** Every page load
   starts cold. Users browsing the result and refreshing accidentally
   lose the Q&A. Should persist last N exchanges in localStorage.

4. **Focal-node URL state is incomplete.** `FOCAL_MARKER` is in JS
   memory only. Sharing a deep-link with a planted focal node
   doesn't carry the focal node to the recipient — they get the
   layer + zoom but no glyph.

5. **Site-simulation prompts require raw lat/lon.** *"Plant a chair
   store at Lamar and 6th in Austin"* fails because the agent has no
   geocode tool. Has to be *"Plant a chair store at lat=30.272,
   lon=-97.7457"* — awkward.

6. **Layers have no default zoom or bbox.** Tract-grain layers
   benefit from a neighborhood-scale default zoom; point overlays
   benefit from a national-scale default. Currently all layers
   inherit the same landing zoom.

## What Was Fixed

Six small, independent changes (one commit each, or one batched
commit, depending on size):

1. **Citation validator skip-list** — extend `find_unlinked_claims`
   to scan citations' source field for "for {FIPS}" pattern and skip
   any matching FIPS strings in narration.

2. **Pilot cancel** — add an AbortController to `askPilot`, expose a
   "Cancel" button in the panel header while streaming. Frontend
   tears down the fetch; server-side the generator stops at the next
   yield.

3. **localStorage persistence** — save `{question, narration,
   ui_actions, citations, model_used, duration_seconds}` per
   exchange under `atlas_pilot_history` (last 10). Hydrate the panel
   on landing if history exists; add a "Clear history" button.

4. **Focal-node URL state** — extend `syncURL` / `loadURL` with
   `?focal_naics=&focal_lat=&focal_lon=&focal_label=`. On landing,
   if present, call `plantFocalMarker` immediately.

5. **`geocode_address` tool** — new read-tool that calls Nominatim
   public API (free, respectful: 1 req/sec, custom User-Agent). The
   agent can call it before `plant_focal_node` to resolve
   street-address questions.

6. **Per-layer default zoom** — `LayerSpec` gains optional
   `default_zoom` and `default_lat`/`default_lon` fields. URL
   handler applies them when a layer is toggled and no explicit zoom
   is in the URL.

## Lessons Learned

- **Polish surfaces only when you actually use the thing.** None of
  these six items were predictable from the spec; they emerged in
  the first hour of clicking through. Future revisions should expect
  a polish round after every major feature ships.
- **Tool surface design has a half-life.** The agent's tool set was
  designed for v0; once shipped, the gap between "what the agent
  needs to feel natural" (geocoding, cancel, history) and "what the
  agent currently has" became visible immediately. Schedule polish
  iterations as part of every Phase, not as afterthoughts.
- **Citation validators need contextual skip-lists.** A naive regex
  for "numbers in narration" will always have false positives on
  identifiers. The fix isn't tighter regex — it's "exclude things
  that appear in a citation source field."

## Out of Scope

Anything not on the six-item list. Bigger structural changes
(streaming over WebSockets, multi-turn conversation memory, the
B C/D Phase work) remain on PLAN_073 rev_01's roadmap.
