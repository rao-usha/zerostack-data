# PLAN 065 — Revision 01: Pivot from Report-First Monetization to Nexdata Atlas

**Date:** 2026-05-20
**Supersedes the commercial framing of:** PLAN_065 (Sector × Market Intelligence Pack)
**Driven by:** User correction via `/remote-control` — report-first monetization rejected.
**New active spec:** `SPEC_064_atlas_data_explorer`

## Revision 01

PLAN_065 built a real, working pipeline: a 14-section public-data report
(`market_intelligence_pack`, SPEC_061), a paid-orders API + Stripe routing
(SPEC_062), and a `$2,500 / $7,500` intake landing page (SPEC_063). The
engineering is sound and all of it is kept. **What was wrong was the
commercial shape** — the product led with "pay for an AI-generated report"
as the first user experience.

This revision repositions the work:

| Asset | Was | Now |
|---|---|---|
| `market_intelligence_pack` | The thing the customer buys ($2.5K) | Deterministic **export / deep-dive** renderer, downstream of exploration |
| `/diligence-pack/*` API + `diligence_orders` | Primary monetization surface | **Concierge / custom-work fallback** after a user has explored |
| `frontend/diligence.html` | Primary CTA / landing page | Demoted to "request a custom deep dive after exploring" |
| Synthetic Playground | Top-of-funnel | Proof/demo surface; CTAs route to Atlas |
| Lead capture | Funnel objective | Telemetry/monetization signal only |

The new primary product is **Nexdata Atlas** (SPEC_064): an interactive
public-data exploration engine. Query → resolved entities → insight cards →
cross-dataset connections → source/provenance trail → related/forked
queries → shareable URL → usage telemetry.

## What Was Wrong

1. **Report-first monetization.** Leading with "buy a $2,500 / $7,500
   report" assumes the buyer already trusts the data enough to pay before
   seeing value. They don't. The artifact has to demonstrate value
   *interactively and for free* before any payment ask.

2. **Competing with frontier LLMs on prose.** A static, AI-styled report is
   a commodity — users increasingly expect generated prose to be free or
   near-free. Nexdata's defensible moat is NOT the report; it's the
   **governed cross-dataset connective tissue**: entity/geography
   resolution, dataset joins, coverage honesty, provenance, and learning
   from how users explore.

3. **The funnel was query → order form → static report.** That's a
   purchase funnel with no exploration loop. No virality, no telemetry, no
   way for the product to learn which insights matter.

4. **`$2,500 / $7,500` pricing in first-touch hero copy.** Pricing as the
   first thing a visitor sees anchors the product as expensive consulting,
   not a tool worth trying. Pricing belongs deep in a fallback path, not
   the hero.

5. **PLAN_064 lead-ops (Slack alerts, digests, `leads.html`) optimized a
   funnel that didn't exist yet.** Already paused in PLAN_065 rev_00; this
   revision confirms it stays paused — Atlas telemetry replaces the need.

6. **Generic "see the platform" CTAs.** They route users somewhere without
   first showing them value. CTAs must lead into exploration.

## What Was Fixed

Per SPEC_064:

- **Build Nexdata Atlas** — `app/services/atlas/*` (resolver, cards, graph,
  telemetry, types) + `app/api/v1/atlas.py` + `frontend/atlas.html`.
- **Reuse, don't rebuild** — the SPEC_061 `gather_data` layer becomes the
  Atlas card data source. Every populated report section maps to an
  insight card with `{title, summary, metrics, why_it_matters,
  datasets_used, confidence, coverage, provenance, links}`.
- **Telemetry from day one** — `atlas_explorations`, `atlas_queries`,
  `atlas_events`, `atlas_card_feedback`, `atlas_shared_links`. V1 only
  records; future versions re-rank cards from the signal.
- **Demote the report flow** — `diligence.html` reframed as a custom-work
  fallback; playground + report CTAs repointed at `/atlas.html`;
  `$2,500 / $7,500` removed from first-touch surfaces.
- **Keep everything functional** — `diligence_orders`, `/diligence-pack/*`,
  and `market_intelligence_pack` all stay working as the concierge/export
  fallback. Nothing is deleted.

## Lessons Learned

1. **Validate the commercial shape before building the funnel, not after.**
   PLAN_065 went through TWO wedge revisions (diligence memo → market map)
   but never questioned the underlying assumption that a *paid report* was
   the product. The "what's the artifact" question got attention; the
   "should the artifact be sold, or used" question didn't.

2. **"Is it sellable?" and "is it the product?" are different questions.**
   The market intelligence pack IS a good artifact. It is NOT a good
   first-touch product. Good artifacts can be exports, proofs, or
   deep-dives without being the thing the funnel sells.

3. **When the cost of the core technology (LLM prose generation) is
   collapsing toward zero, monetize the thing that isn't collapsing** —
   here, the governed data joins + provenance + the usage telemetry that
   compounds.

4. **Telemetry is a feature, not an afterthought.** A product that can't
   observe which insights users value can't improve its ranking, its
   ingestion priorities, or its monetization. Build the event spine first.

5. **Engineering sunk cost is not commercial sunk cost.** SPEC_061-063 took
   real effort, and the instinct is to defend the funnel built around them.
   The right move was to keep 100% of the engineering and discard 100% of
   the commercial framing — those are separable.

## Migration / cleanup checklist (tracked in SPEC_064)

- [ ] `frontend/playground.html` CTA → `/atlas.html`
- [ ] `app/reports/templates/synthetic_playground.py` CTA copy/defaults → Atlas
- [ ] `frontend/diligence.html` hero reframed; first-screen pricing removed
- [ ] No new Slack/digest/leads work
- [ ] `diligence_orders` + `/diligence-pack/*` kept functional as fallback
