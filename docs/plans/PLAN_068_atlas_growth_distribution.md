# PLAN 068 — Atlas Growth & Distribution

**Status:** Draft — awaiting approval
**Date:** 2026-05-22 (v2 — general-explorer reframe)
**Phase:** 3b (see `ATLAS_PROGRAM_ROADMAP.md`)
**Builds on:** PLAN_066 v3 (general public-data explorer)
**Gating:** "light" workstreams start at map-ship; "heavy" / targeted
outreach waits for the Phase-2 telemetry read that names a wedge.

---

## 1 · The distribution thesis

Atlas is a general explorer — so its growth thesis is **earned distribution
powered by breadth.** Every dataset is a reason for someone to arrive; every
view is a shareable link; every story is an artifact. The channels, in
priority order:

1. **Journalist embeds.** A general explorer gives data journalists an
   *endless* supply of sourced map material — across disasters, environment,
   federal money, migration, health, infrastructure. A sourced Atlas map
   embedded in an article is high-trust, zero-cost reach.
2. **Search / SEO via breadth.** ~400 datasets = hundreds of indexable
   surfaces. "Map of [X]" for every X we hold. Breadth is the SEO moat.
3. **Social sharing.** Every exploration is a deep-link with a great preview
   card; "there's a map of *that*?" is the share trigger.
4. **Targeted community outreach — AFTER the wedge is known.** We do *not*
   pre-pick a community. The Phase-2 telemetry read (PLAN_066 §9) reveals
   which domains/audiences actually cluster; *then* we run founder-led
   outreach into whichever communities lit up.

The product *is* the distribution. This plan operationalizes it.

---

## 2 · Workstreams

### W1 — Embeddable map widget  *(light — starts at map-ship)*
A locked, `<iframe>`-able Atlas view — a place, a layer, or a story — clean
chrome, **"Powered by Nexdata Atlas →"** attribution linking back. The object
journalists embed. **Spec:** an `embed=1` mode of `atlas.html` + an
`/atlas/embed/{ref}` route, reusing PLAN_066 SPEC_066 deep-link state.

### W2 — Social-card (OG image) generation  *(light — starts at map-ship)*
Shared links must preview as a real map. **Spec:** generate an OG image —
a map screenshot — per exploration/layer/story via headless Chrome (proven
to work this session). Serve `og:image` on shareable routes.

### W3 — Stories content engine  *(light core, heavy cadence)*
PLAN_066 SPEC_069 builds the story *mechanism*. This workstream makes it a
**cadence** across all domains — N stories/month, each SEO-targeted and
embed-ready. Light: the first 3-4 ship with PLAN_066. Heavy: a sustained
operation — gated, see §3. Story range is deliberately broad (disaster,
environment, migration, federal money, infrastructure, health) — the breadth
is the hook.

### W4 — In-product referral loop  *(light — starts at map-ship)*
Baked into PLAN_066 SPEC_066: one-click "share this view" (deep-link + W2
card), "fork this exploration," "compare two places/layers." The weekly
follow digest (SPEC_068) is itself a forwardable artifact.

### W5 — Targeted community outreach  *(heavy — gated on the wedge read)*
Founder-led, non-code. **Deliberately deferred until telemetry names the
wedge.** When the Phase-2 read shows (e.g.) "disaster/risk explorers cluster
and return" or "environmental-data users dominate," *then* outreach targets
that community — its associations, forums, conferences. We do not guess the
community; we earn the right to target by measuring first.

### W6 — Referral attribution / measurement  *(light)*
Extend telemetry: add a `source` dimension (`embed`/`shared_link`/`story`/
`search`/`direct`) to `atlas_queries`. The question that matters: **which
channel + which domain brings users who FOLLOW and RETURN** — that joins
distribution data to the wedge-discovery read.

---

## 3 · Sequencing & gates

```
Map ships (PLAN_066) ──► W1 widget · W2 social cards · W3 first stories · W4 referral · W6 attribution
                              │   (light — cheap, they ARE the loop's referral arm)
                              │
                         [Phase-2 read: loop holds + telemetry names a wedge]
                              │
                              └──► W3 heavy cadence · W5 targeted community outreach
```

**The gate:** light workstreams ship immediately — they're how the explorer
refers at all, and they're cheap. The heavy workstreams (sustained content
production, a founder's outreach time) wait until telemetry both (a) confirms
the loop retains and (b) **names which community to target.** Spending
outreach effort before the wedge is known is exactly the PLAN_064/065
mistake.

---

## 4 · Non-goals
- No paid advertising in v1 — the thesis is earned distribution.
- No pre-committing a target community before telemetry names it.
- No growth hacking that degrades the product — no dark patterns, no
  share-gating basic features.
- No lead-ops resurrection — PLAN_064 stays paused.

## 5 · Success metrics
Feeds the PLAN_066 §9 read, with channel attribution (W6):
- **Embed reach** — sites carrying the widget; click-through back.
- **Search surfaces** — indexed layer/story pages; organic arrivals.
- **Share→activate** — % of shared-link arrivals that activate.
- **Channel × domain → follow/return** — the join that names the wedge.
