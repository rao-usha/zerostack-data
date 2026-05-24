# Nexdata Atlas — Program Roadmap

**Status:** Living document
**Date:** 2026-05-22
**Owner plan:** the Atlas pivot (PLAN_065 rev_01 → SPEC_064 shipped)

This is the index + phase-gate logic for the Atlas plan series. Each plan
below is its own document; this roadmap is how they fit together and — more
importantly — **which gates separate them so we don't build ahead of proof.**

---

## The thesis (one paragraph)

Nexdata Atlas is **the general explorer of American public data** — an
interactive, map-first, layered, source-cited surface over the ~400 governed
public-data tables Nexdata holds. It is *not* a vertical tool for one
persona; it is a curiosity-driven exploration product where **breadth is the
asset.** We design for the *behavior* (exploring) rather than a pre-picked
user, instrument it heavily, and **let telemetry reveal the wedge** — which
domains and audiences actually show up — instead of guessing it. The moat is
**governed cross-dataset joins + provenance + the usage telemetry that
compounds** — not prose generation, which is collapsing toward free.

---

## The plan series

| Plan | Scope | Phase |
|---|---|---|
| **PLAN_066** — Atlas Map v2 | The map surface: region-open landing, honest layers, click-to-cards drill-down, Recent Activity, follow-a-place, stories | 1 |
| **PLAN_067** — Data Coverage Expansion | Backfill the verified data gaps (ACS county grain, USAspending real ingest, FCC county broadband, multi-year CBP, SEC filing dates) so more layers go ✅ | 3a (parallel) |
| **PLAN_068** — Growth & Distribution | The acquisition engine: embeddable map widget, social-card generation, the stories content cadence, EDO-network GTM | 3b |
| **PLAN_069** — Monetization | The paid tier: branded region exports, benchmarking pro, monitoring pro, team seats, API — reusing the SPEC_062 Stripe substrate | 4 |
| **PLAN_070** — Atlas Dynamic UI | Coupling / motion / surprise — the features that make the explorer interesting to use. Design source-of-truth for SPEC_066 / 066b / 066c | 1b |
| **PLAN_071** — Learning Layer *(future, not yet drafted)* | Telemetry-driven ranking — the product learns which layers/cards/stories to surface first | 5 |
| **PLAN_072** — Loop-Compelling | Three waves that make Phase 2's loop measurement fair: smoke harness (SPEC_073), deferred dynamic features (SPEC_066c), Recent Activity feed (SPEC_067) | 1c |

---

## Phases & gates — the discipline

The whole point of the roadmap is the **gates**. We have shipped real
engineering twice into commercial framings that were wrong (PLAN_064 lead-ops,
PLAN_065 report-first). The lesson is in `memory/feedback/corrections.md`:
*don't build ahead of proof.* So each phase has an explicit gate.

### Phase 1 — Build the map  *(PLAN_066)*
Ship the region monitor: SPEC_065 (layer API) → SPEC_066 (map frontend) →
SPEC_067-069 (Recent Activity, follow, stories). Critical path is 065+066.

### Phase 2 — Prove the loop  *(GATE — no plan, a measurement window)*
Ship Phase 1, instrument it (telemetry exists), run a **30-day read** against
PLAN_066 §9 metrics:
- **Activation** — sessions that toggle a layer + click a place
- **Breadth** — distinct domains explored per session
- **Return** — 7-day repeat-visit rate
- **Follow / share** — conversion to follow; `share_created` rate
- **Wedge discovery** — which domains / layers / places dominate exploration

> **GATE 2 → 4:** Monetization (PLAN_069) does **not** start unless the
> 30-day read shows real *return + follow* behavior **and** the telemetry
> names a wedge (a domain/audience cluster worth targeting). If the loop
> doesn't retain, the hook is wrong — fix the loop, do not paywall a leaky
> bucket. This is the gate PLAN_064 and PLAN_065 never had.

### Phase 3a — Coverage expansion  *(PLAN_067 — parallelizable)*
Runs **in parallel** with Phase 2 — it does not gate on loop-proof, because
better data only makes the map more honest. It does NOT block Phase 1: the
map ships with the strong layers; PLAN_067 upgrades layers *in place* as each
backfill lands. Independent, additive, safe to run early.

### Phase 3b — Growth & distribution  *(PLAN_068)*
The cheap referral arm — embeddable widget, social cards, first stories —
can start the moment the map ships (it *is* PLAN_066's referral loop). But
**heavy** growth investment (paid story cadence, EDO-network outreach motion)
waits for **early Phase 2 signal** — enough to know the loop holds water
before pouring acquisition into it.

> **GATE 1 → 3b-heavy:** founder-led EDO outreach + sustained content cadence
> begin only after the activation metric clears a floor. Don't drive Danas to
> a map that doesn't activate them.

### Phase 4 — Monetization  *(PLAN_069)*
Gated hard on GATE 2. The paid tier is depth/branding/scale/team/API — never
"unlock the basic map." Free stays genuinely useful because the loop depends
on it.

### Phase 1b — Dynamic UI  *(PLAN_070)*
Sits *between* Phase 1 (build the map) and Phase 2 (prove the loop). The
SPEC_066 map alone is functional but plain; an "explorer with no dynamism"
undersells the data and produces a misleading Phase-2 read. PLAN_070
codifies the coupling/motion/surprise features (SPEC_066b) that ship
alongside SPEC_066 so the loop has a fair chance to retain. The dynamic UI
is also the **wedge-discovery signal**: every brush, scrub, bivariate pair,
and arc-click is a telemetry event that tells us which domains and
comparisons matter.

### Phase 1c — Loop-Compelling  *(PLAN_072)*
Same logic as Phase 1b, second pass. Three additive surfaces:
- **SPEC_073** — headless-Chrome smoke harness that runs `ATLAS_TOUR.md`
  end-to-end. Future specs ship against a green baseline; regressions
  die same-day rather than biasing the Phase-2 measurement.
- **SPEC_066c** — the PLAN_070 §3 🥉 deferred features (FEMA
  time-cascade scrubber, EPA kernel-density heatmap, calendar heatmap,
  swipe-compare, lasso-select).
- **SPEC_067** — Recent Activity feed. The single biggest driver of
  return-visit behavior is *a reason to check again today*. Surfaces
  fresh FEMA / SEC / USAspending events on a map. Reuses PLAN_067
  data backfills as they land (FEMA-only MVP first).

Phase 2 *can* start with just Phase 1b shipped, but reads better with
1c shipped because compelling-but-unmeasured ≠ underbuilt-but-measured.

### Phase 5 — Learning layer  *(future PLAN_071)*
Once there's real telemetry volume, use `atlas_events` + `atlas_card_feedback`
to rank layers/cards/stories — surface what users value first. This is the
long-term compounding moat (SPEC_064's "the product learns which answers are
worth showing"). Not drafted until Phases 1-2 produce the data to learn from.

---

## Dependency graph

```
PLAN_066 (map) + PLAN_070 (dynamic UI) + PLAN_072 (loop-compelling) ──ship──► [GATE 2: 30-day loop proof] ──pass──► PLAN_069 (monetize)
              │                                                                          │
              │                                                                          └── early signal ──► PLAN_068-heavy (growth motion)
              │
              ├──► PLAN_068-light (widget, cards, first stories) — starts at map-ship
              │
              └──► PLAN_067 (coverage expansion) — parallel, additive, no gate
                                  │       │
                                  │       └──► feeds PLAN_072 SPEC_067 Recent Activity (SEC/USA as backfills land)
                                  │
                                  └──► (enough telemetry) ──► PLAN_071 (learning)
```

---

## What this roadmap refuses to do

- **No monetization before loop-proof.** GATE 2 is non-negotiable.
- **No data perfectionism blocking the ship.** PLAN_066 ships with strong
  layers; PLAN_067 backfills the rest after.
- **No growth spend on an unproven funnel.** GATE 1 → 3b-heavy.
- **No scope creep into OSIRIS territory** — flight tracking, CCTV, SIGINT
  are not our data and not our identity.

Each plan document carries its own specs, acceptance criteria, and honest
non-goals. Read them in phase order.
