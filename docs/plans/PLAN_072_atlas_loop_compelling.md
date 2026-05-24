# PLAN 072 — Make the explorer more compelling (loop-test ready)

**Status:** Draft — awaiting approval
**Date:** 2026-05-24
**Phase:** 1c (between Phase 1b dynamic-UI ship and Phase 2 loop measurement)
**Builds on:** SPEC_066 + SPEC_066b shipped; 20 layers across 10 domains live
**Gating:** none — additive. Does NOT block Phase 2. But Phase 2 reads
*better* with this work shipped: an "explorer with no return-pull" would
produce a misleading 30-day loop measurement.

---

## 1 · Why this plan

`ATLAS_PROGRAM_ROADMAP.md` already calls out the trap: Phase 2 is a
**measurement window**, not a code task. If the measurement reads "low
return rate, low follow conversion," we have to ask *was the loop
actually wrong, or did we never give users enough reason to return?*
That ambiguity is fatal for the GATE 2 monetization decision.

PLAN_070 (dynamic UI) was the first answer to this — "ship the
compelling features alongside the static map so the loop has a fair
chance to retain." That landed (SPEC_066b). This plan is the **second
pass**: three concrete additions that turn the explorer from
*functional-and-interesting* into something users keep coming back to.

The thesis: **Phase 2 measures better against a compelling explorer
than a barely-compelling one.** Three things compound retention:

1. **Verification** — the surface actually works (no broken layers, no
   silently-dead features) so users aren't bouncing on bugs.
2. **Visual richness** — the second tier of dynamic UI (PLAN_070 §3 🥉
   deferred items) — time scrubber, kernel-density heatmap, swipe
   compare. Stuff that makes the *exploring* feel exploratory.
3. **News surface** — Recent Activity. A reason to come back daily.
   Today's FEMA decls, this-week's SEC filings, this-month's federal
   awards. Time-shaped data on a map.

Each wave is its own spec.

---

## 2 · Phase placement

```
Phase 1     — build the map        (SPEC_064/065/066)          ✅
Phase 1b    — dynamic UI           (SPEC_066b)                 ✅
Phase 1c    — loop-compelling      (this plan — PLAN_072)      ⏳
Phase 2     — prove the loop       (30-day measurement)
Phase 3a    — data coverage        (PLAN_067 — parallel)        ✅✅✅⏳⏳
Phase 3b    — growth & distribution (PLAN_068)
Phase 4     — monetization         (PLAN_069 — gated GATE 2)
Phase 5     — learning layer       (PLAN_071 — future)
```

This is **Phase 1c.** It sits before the loop measurement because the
measurement needs to fairly read a compelling product. It is not a
blocker — Phase 2 *can* start with just Phase 1b shipped, and Phase 3a
backfills continue in parallel. But the measurement is more honest if
this lands first.

---

## 3 · The three waves (specs)

### Wave 1 — SPEC_073 *(browser-smoke + tour regression)*

**Goal:** systematically verify the 20-layer + 5-dynamic-feature surface
actually renders for every state we ship. Catch the regressions where
a new layer's grain mismatch or a new dynamic feature breaks the page.

The cheapest valuable thing. We have `docs/ATLAS_TOUR.md` already — it
*is* the test plan. SPEC_073 codifies the headless-Chrome run of it.

**Approach:**
- Headless-Chrome harness: for each layer + each interesting deep-link,
  load the page, wait for `init` to complete, dump the DOM, assert key
  selectors are present + populated.
- Outputs: a CSV report (`docs/atlas_smoke_report.csv`) — one row per
  scenario with pass/fail and notable observations.
- One commit-time guard: run the harness as part of pre-deploy.

**Acceptance:**
- 20 layer URLs each render the choropleth + legend without console errors
- 5 dynamic features each render their expected SVGs in the DOM
- 8 deep-dive place URLs each open the place panel populated
- Total run ≤ 5 minutes.

**Cost:** half-day. **Value:** every future spec ships with a green
smoke; regressions die same-day.

---

### Wave 2 — SPEC_066c *(deferred dynamic features — second wave)*

**Goal:** ship the PLAN_070 §3 🥉 deferred features that were cut from
SPEC_066b to keep that spec single-concern. Layers on top of the same
`frontend/atlas.html`.

**Features (from PLAN_070 non-goals + new):**

| Feature | What it does | Cost |
|---|---|---|
| **FEMA time-cascade scrubber** | Drag a year-slider; FEMA declarations animate decade-by-decade across the map | day |
| **EPA kernel-density heatmap** | Toggle on the `env_epa_facilities` layer → render as Leaflet.heat KDE blobs instead of dots | half-day |
| **Calendar heatmap (place panel)** | For a clicked county with FEMA series: 365-day calendar showing event density per day | half-day |
| **Side-by-side swipe compare** | Two map panes with a draggable vertical divider; layer A left, layer B right | day |
| **Lasso-select / custom-region** | Draw a polygon on the map; the place panel summarizes all counties inside | day |

**Telemetry per feature** — every interaction is signal:
- `time_scrubbed: {year, layer}`
- `heatmap_enabled: {layer}`
- `calendar_inspected: {place_id, layer}`
- `swipe_compared: {layer_a, layer_b}`
- `lasso_selected: {n_counties}`

**Sequencing within SPEC_066c (waves of the wave):**
1. **Wave a** — calendar heatmap + EPA kernel-density (the two cheap-but-distinctive items, ~day)
2. **Wave b** — FEMA time-scrubber (the signature one — single-page animation that *teaches* US disaster history at a glance)
3. **Wave c** — swipe compare + lasso (deferrable if time runs short)

**Acceptance:**
- All five features ship to `frontend/atlas.html`
- Each is keyboard-accessible
- Each emits its telemetry event
- Bundle stays <300KB JS (Leaflet.heat is the new dep — ~10KB CDN)
- SPEC_066 + SPEC_066b verification still passes

**Cost:** ~3 days. **Value:** the explorer feels *generative*. Users
who toggle one feature toggle three.

---

### Wave 3 — SPEC_067 *(Recent Activity feed)*

**Goal:** a "what's new" surface that makes the Atlas worth revisiting.
The single biggest driver of return-visit behavior is a reason to
check again *today*. We have time-shaped data — FEMA declarations
since 1999, FDIC quarterly, IRS migration annual, USAspending FY,
ACS annual. Surface the recent edges.

**The feed:**
A new chrome region (header dropdown or right-rail sidebar tab) that
shows the last N events across the platform, ordered by date:
- **FEMA**: last 7 days of disaster declarations, with place link
- **USAspending**: top 10 awards by amount obligated this week
  (requires SPEC_071-stretch: top-N awards detail; clean follow-on)
- **SEC filings**: last 7 days of S-1 / 8-K / 10-K filings
  (requires SPEC_074: SEC filings with dates; PLAN_067)
- **ACS / FDIC / migration**: refreshes annotated when new data lands

Each row is clickable → opens the place panel + jumps the map to the
geography.

**Why this drives Phase 2 metrics:**
- **Return rate**: a daily-fresh feed gives reason to revisit
- **Breadth**: feed items pull users across domains they wouldn't
  have searched for
- **Follow / share**: feed items are the most shareable units — a
  surprising recent stat ("Hurricane Helene declared in 47 counties")
  is the unit-of-distribution for PLAN_068 social cards

**Acceptance:**
- New API endpoint `GET /atlas/recent?limit=50` returns merged event
  stream from FEMA + (optionally) USAspending + (optionally) SEC,
  sorted by date desc
- New frontend chrome — collapsible right-side panel or header dropdown
- Each item links to a place — clicking opens the place panel
- Item-clicked telemetry: `recent_item_clicked: {event_type, event_id, place_id}`

**Dependencies surfaced:**
- USAspending awards source: SPEC_071-stretch (top-N awards table)
- SEC source: SPEC_074 (filings with dates)
- Both can ship later; SPEC_067 v1 = FEMA-only as MVP, expandable.

**Cost:** ~2-3 days (FEMA-only MVP: 1 day; with all three sources: 3).
**Value:** the only feature on this list that creates *daily* return
visits.

---

## 4 · Sequencing

```
SPEC_073 (smoke harness) ──► SPEC_066c (deferred UI) ──► SPEC_067 (Recent Activity)
       1 day                       3 days                     2-3 days
```

Strong reasons for this order:
1. **SPEC_073 first** — every subsequent spec gets shipped against a
   green smoke harness. We catch regressions same-day rather than
   discovering them in Phase 2 telemetry.
2. **SPEC_066c next** — these are pure-frontend additions; lowest risk
   of breakage; each ships independently.
3. **SPEC_067 last** — it depends on data backfills (SPEC_071-stretch,
   SPEC_074) for full content. Shipping FEMA-only MVP first is fine;
   richer content lands as PLAN_067 specs close.

**Total budget:** ~1 week to ship all three at full scope. Cuttable
to 3 days if SPEC_066c ships only Waves a+b.

---

## 5 · Non-goals (deferred to later plans)

- **3D globe / orbiter visualizations** — explicitly 🚫 in PLAN_070.
- **AI-generated insights** — collapsing toward commodity; not our moat.
- **Anomaly engine** — surprise/alerts gated on telemetry signal (Phase 5).
- **Real-time data feeds** — our data is officially-published periodic;
  fake "real-time" is anti-the-honesty principle.
- **Custom-region SQL** — power-user surface; defer until telemetry
  proves demand.

---

## 6 · Definition of done

This plan closes when:
- SPEC_073 smoke harness runs green in CI on every commit
- SPEC_066c ships at least Waves a + b (3 of 5 features)
- SPEC_067 ships at least FEMA-only MVP
- ATLAS_TOUR.md updated with the new features + smoke-pass badge

At that point, the explorer is "loop-test ready" and Phase 2 can
begin its 30-day measurement window with confidence the read isn't
biased by missing compelling features.

---

## 7 · What this plan refuses to do

- **Block Phase 2 indefinitely.** This is "1c" — designed to ship in
  about a week. If any spec slips into multi-week scope, the spec
  trims (e.g. SPEC_066c Wave c gets cut) rather than the plan delays
  the loop measurement.
- **Add data sources.** Pure frontend + thin existing-data wiring.
  All new data goes through PLAN_067.
- **Add monetization scaffolding.** Hard-gated on GATE 2.
