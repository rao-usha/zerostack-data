# PLAN 069 — Atlas Monetization

**Status:** Draft — **gated, do not execute before GATE 2 passes**
**Date:** 2026-05-22 (v2 — general-explorer reframe)
**Phase:** 4 (see `ATLAS_PROGRAM_ROADMAP.md`)
**Builds on:** PLAN_066 v3 (the free explorer), PLAN_068 (distribution)
**Hard dependency:** GATE 2 — the 30-day read shows the loop retains *and*
names a wedge (PLAN_066 §9).

---

## 1 · The gate, stated first

This plan **does not start** until the Phase-2 read shows (a) real return +
follow behavior and (b) a telemetry-named wedge — a domain/audience cluster
worth targeting. A general explorer that nobody returns to cannot be
monetized by paywalling it; and you cannot price tiers for an audience you
haven't identified. **Measure first, then monetize the cluster the data
revealed.**

This is the gate PLAN_064 (lead-ops) and PLAN_065 (report-first) never had.
`memory/feedback/corrections.md` records why it matters. We do not repeat it.

---

## 2 · The monetization principle

From the Atlas pivot and the corrections log:
- **Do not monetize the commodity.** Generated prose / reports trend toward
  free. We don't sell those.
- **Monetize what compounds:** the *saved workflow*, monitoring, exports,
  scale, team, programmatic access, private-data joins.
- **Free stays genuinely useful.** The free explorer — the map, every layer,
  drill-down cards, stories, shareable links, a baseline of follows — *is*
  the loop. Paid is **depth / scale / automation / team / API**, never
  "unlock the basic explorer." A crippled free tier kills the loop that
  feeds paid.

---

## 3 · The free / paid line (general explorer)

This is the SPEC_064 "Future Monetization" sketch, now concrete:

| Capability | Free | Paid |
|---|---|---|
| The map, every layer, drill-down cards, provenance | ✅ | ✅ |
| Stories, shareable links, embeds | ✅ | ✅ |
| Follow places / layers / domains | ✅ (a baseline) | ✅ unlimited + faster cadence + custom alert rules |
| Saved explorations | ✅ (public, a few) | ✅ private + unlimited saved workspaces |
| Compare | ✅ basic (2) | ✅ multi-way + scoring + export |
| Exports (PDF / PNG / data) | — | ✅ |
| Monitoring / alerting pro (Slack, custom rules) | — | ✅ |
| Team workspaces / seats | — | ✅ |
| API access / bulk pulls | — | ✅ (higher tier) |
| Private-data joins (your data × our governed layers) | — | ✅ (enterprise, later) |

The free column must stay compelling on its own — it is the growth loop.

## 4 · Who pays — determined by the wedge read, not pre-guessed

v1 of this plan pre-named the payer (economic-development officers). v2 does
not. **The Phase-2 telemetry names the wedge**; pricing is then shaped to
*that* cluster. Plausible payer shapes, to be confirmed by data:
- **Professionals who monitor places** (analysts, lenders, operators, EDOs)
  → monitoring pro + exports + saved workspaces.
- **Teams** that explore together → seats.
- **Builders** who want the data in their own systems → API / bulk /
  private-data joins (the highest tier).
Pricing posture is set once the wedge is known — annual billing, tiers sized
to whoever the data says actually shows up and stays.

---

## 5 · Build — reuse, don't rebuild

SPEC_062 already shipped a Stripe substrate (`diligence_orders` +
`/diligence-pack` + Stripe Payment Link config). Monetization reuses it.

| Need | Reuse |
|---|---|
| Accounts | passwordless auth (SPEC_053) — already powers follow |
| Billing | Stripe (Payment Links / Checkout), SPEC_062 config pattern; no custom billing engine |
| Tiers / entitlements | a small `atlas_subscriptions` table + a `require_tier` dependency |
| Exports | the report-render machinery (`market_intelligence_pack` / design system) — repositioned as the export renderer |
| Concierge fallback | `diligence_orders` + `diligence.html`, unchanged |

**Spec numbers intentionally unassigned** — this plan does not reach the spec
stage until GATE 2 passes.

---

## 6 · Non-goals
- No paywall on the basic explorer, layers, or drill-down — ever.
- No usage-metered surprise billing — flat annual tiers.
- No enterprise-SaaS theater (SSO / SOC2 / RBAC) until a paying customer
  genuinely needs it.
- No execution before GATE 2. This document exists so the *thinking* is
  ready — not so it ships early.

## 7 · "Ready to start" definition
PLAN_069 converts Draft → Active only when:
1. GATE 2 passed — 30-day read shows return + follow behavior, **and**
2. the telemetry **named a wedge** (a domain/audience cluster), **and**
3. PLAN_068's light workstreams confirm a working referral arm, **and**
4. there is ≥1 unsolicited "can I pay for X" signal from a real user —
   the cheapest possible demand validation.
