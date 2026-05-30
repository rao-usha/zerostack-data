# PLAN 075 — Atlas Decision Map framework

**Date:** 2026-05-30
**Status:** Draft (awaiting user direction)
**Builds on:** PLAN_073 (commerce simulator) + PLAN_074 (workspace shell)
**Triggered by:** User feedback —
> "there should be better overlays or make the map overlays more
> dynamic. There's no reason to have income to be even on the map to
> start. Let's build a framework on how to use a map to actually
> enhance decision making."

## What's wrong today

The map is a **layer viewer**: pick one of ~26 layers, see a flat
choropleth, optionally swap to bivariate compare. That's classic GIS
data viz. It does not answer the question the actual customer is
asking: **"where should I locate my X?"** Defaulting to *any* single
layer (NRI, income, etc.) is an answer to a question the user didn't
ask — and worse, it sets a misleading frame for what the tool is for.

A site-selection consultant tool should:
1. Open **blank** (basemap only) — declare zero opinion until it has one.
2. Become opinionated only after a **thesis** exists.
3. Show **answers** (rankings, candidates, scores), not raw data — by
   default. Raw layers are for *investigating* an answer, not the entry.
4. Drive the entire view from the Pilot's chat + the thesis form. No
   manual layer-juggling required to get to "here are 5 places to look."

## Framework — "Decision Map"

Five primitives, composable. The framework is built once; the modes
below are stages of using it.

### Primitives

| # | Primitive | Purpose |
|---|-----------|---------|
| 1 | **Fit-score layer** | Weighted blend of N relevant layers → one synthesized 0-100 choropleth keyed to the thesis. "Where does this thesis fit?" |
| 2 | **Constraint funnel** | A list of must-have filters (HHI ≥ X, density ≥ Y, exclude high-NRI). Non-matching places dim or vanish. |
| 3 | **Top-N candidate pins** | The N best places under the current thesis + filters, ranked with mini-cards (score, why, key stats). |
| 4 | **Trade-area mode** | Click a candidate (or focal pin) → zoom + switch to a granular tract view of *just that radius*: competition, demographics, frictions. |
| 5 | **Layers as accents** | Individual choropleth layers become *investigative overlays* you toggle on top of a fit-score / candidate view, not the primary visual. |

### Modes (the user-visible loop)

```
LAND blank
  └── no opinion. "Set a thesis → I'll show you where it fits."
  └── (Pilot prompts the user OR user fills the thesis form)

THESIS SAVED → FIT-SCORE MODE
  └── single fit-score choropleth (0-100). Top candidates highlighted.
  └── Legend shows the weighted blend ("60% income · 30% density · 10% growth").

USER ADDS A CONSTRAINT (UI or "Pilot: only include tracts > $80K")
  └── Funnel applies; non-matching places fade. Counter: "127 → 38 candidates."

USER (or Pilot) CLICKS A CANDIDATE
  └── Trade-area mode: zoom to 5 mi, switch to tract grain, show
       competition + demographics + foot-traffic proxies INSIDE the radius.
  └── Place card on the right (or inline in chat) explains the score.

USER WANTS DETAIL → toggle an INVESTIGATIVE LAYER (income, broadband…)
  └── That layer paints on top of (or instead of) the fit-score.
  └── Closing it returns to fit-score view.
```

### Why this is different

- The map's **first frame is empty**, not a contested choropleth.
- The user never has to *invent* what to look at — the thesis + Pilot do.
- "Layers" become a power-user investigation tool, not the home screen.
- The Pilot has a clean API to drive: `set_fit_weights()`,
  `add_constraint()`, `recommend_candidates(n)`, `enter_trade_area(geo_id)`.

## Open design choices (need your call)

Even within this framework, several directions diverge. Below I'll ask
3 questions via AskUserQuestion to nail down:

1. **Landing posture** — pure blank? Or blank-with-prompt CTA?
2. **Fit-score expression** — single 0-100 choropleth, or top-N pins
   only, or both?
3. **Constraint funnel UI** — chips/pills above the map? Inline in the
   thesis blocks? Or Pilot-only (no chrome)?

## Phasing (if approved as the direction)

| Phase | Scope | Approx. work |
|---|---|---|
| **A** | Blank landing + fit-score primitive: backend `/atlas/fit-score?thesis=...` + frontend `paintFitScore()` + legend explainer | 1 spec |
| **B** | Constraint funnel + counter ("N candidates remaining") | 1 spec |
| **C** | Top-N candidate pins with cards | 1 spec |
| **D** | Trade-area mode (click candidate → zoom + tract-grain + radius queries) | 1 spec |
| **E** | Pilot tools (`set_fit_weights`, `add_constraint`, `recommend_candidates`, `enter_trade_area`) so the chat drives the whole loop | 1 spec |

Each phase ships independently; the user always has a working map.

## What this replaces / deprecates

- **Default-on choropleth.** Landing layer = none. Income/NRI/etc.
  become opt-in investigation layers.
- **"Pick a layer" as the entry mental model.** Replaced by "what's
  your thesis?" → "here's where it fits."
- **Bivariate Compare.** Still available (power-user) but moved out of
  the entry path.

## Risks / things to watch

- **Fit-score interpretability.** A single 0-100 must be defensible.
  Plan: legend always shows weighted-component breakdown ("60% income,
  30% density, 10% growth"); clicking the legend opens a detail
  popover; the Pilot can always explain the score for any place.
- **"Where does the data come from?"** Cite tools must still fire for
  any number the agent states.
- **Empty default could feel unfinished.** Mitigate with a friendly
  prompt + the existing onboarding nudge.
- **Backend cost.** Fit-score is computed per-thesis; cache by thesis
  hash. The compute is just weighted sum over already-loaded layers.

## Files (when approved)

Plan-only doc for now. Subsequent specs (SPEC_087 → 091, one per phase)
will modify `app/services/atlas/*` (fit_score service, candidate
ranker, trade-area queries), `app/api/v1/atlas.py` (new endpoints),
`app/services/atlas/pilot_tools.py` (new Pilot tools),
`frontend/atlas.html` (paintFitScore, funnel UI, candidate cards,
trade-area mode), and tests.

## Decision needed

See AskUserQuestion that follows: landing posture, fit-score
expression, constraint funnel UI. Once those are picked, I'll write
SPEC_087 (Phase A — blank landing + fit-score) and start.
