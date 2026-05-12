# PLAN_061 — Revise 4 PRDs to align with PLATFORM_POSITIONING_2026

## Context

On 2026-05-08 we drafted `docs/strategy/PLATFORM_POSITIONING_2026.md` as the canonical roll-up for the four vertical product PRDs (DCII, IRADev, Underwrite, LitInt). The roll-up reconciled math, sequenced products (Underwrite → DCII → IRADev → LitInt), sized Phase 0 as one shared 12-month program, and committed to bundle pricing + a single MCP investment.

The four PRDs still reflect their pre-roll-up standalone framing. Issues confirmed by ground-truth read:
- All four list MCP as a per-product Phase 3 deliverable (now a shared cross-cutting investment)
- None reference the positioning doc or the bundle-pricing motion
- DCII risks are in the wrong order; partner counts disagree across §1/§9/§10; competitor list is missing 3 names
- IRADev doesn't acknowledge 2025-26 IRA / OBBB policy changes; BESS revenue streams undersold; PPA type undifferentiated; two beachheads listed without primary/secondary
- Underwrite "$1.4T" lacks unit; <500ms SLO has no architecture note; ST-4 privacy mechanism unspecified; cyber peril is Phase 4 despite At-Bay in ICP; no FL/CA depth; reinsurer beachhead exclusion unjustified
- LitInt PACER has no fallback (RECAP/CourtListener); Phase 0 = 6mo conflicts with positioning doc's 12mo; DI-12 ethical posture not addressed; mass-tort PI underweighted

Goal: revise all four PRDs in place so they (a) reference the positioning doc as canonical, (b) drop the duplicate MCP line items, (c) fix the per-PRD content gaps from the 05-08 review, and (d) gain a small cross-sell section pointing to the other three products. PRDs keep their standalone Y2 ARR numbers as aspirational targets; positioning doc §7 owns the platform-reconciled aggregate.

## Approach

Treat this as 4 independent in-place revisions, each touching ~6-8 specific lines/sections. No new files. Order: DCII → IRADev → Underwrite → LitInt (matches positioning doc sequencing for cognitive coherence; not a dependency).

**Cross-PRD changes** (apply to all four):
- **C1** — Insert at top after `# Title` line: a "See also" callout pointing to `PLATFORM_POSITIONING_2026.md` for sequencing, bundle pricing, Phase 0 program, MCP cross-product investment.
- **C2** — Next to the Y2 ARR target: add inline note "Standalone target — see PLATFORM_POSITIONING_2026.md §7 for reconciled platform aggregate ($10.5–16.5M)."
- **C3** — Remove the "MCP / Computer connector" Phase 3 line item; replace with a one-line pointer "MCP server: shared cross-product investment — see PLATFORM_POSITIONING_2026.md §11."
- **C4** — Add a new short section "Cross-sell to other Nexdata products" listing the 1–3 accounts in this PRD's ICP that also fit DCII / IRADev / Underwrite / LitInt, and pointing to bundle pricing in positioning doc §6.

**Per-PRD changes:**

### PRD_DATACENTER_INVESTOR_INTEL.md (343 lines)
- **D1** Reorder risks (line 313–315): promote R3 (DigitalBridge/Stonepeak in-house build) to R1; demote current R1 (cap-ex disclosure quality) to R3.
- **D2** Add cross-reference between TD-2 (line 119, entity-graph) and SR-4 (line 109, hyperscaler permit/LLC). Both share the LLC entity-resolution layer; note this without merging — they serve different workflows.
- **D3** Add competitors to the table (lines 236–242): Datacenter Hawk (datacenter intel SaaS), Mighty (datacenter analytics), Rebellion Defense / Rebellion Research (LLC graph adjacency).
- **D4** Reconcile partner counts. Canonical: "**8–12 design partners** total in 2026-H2 (§1, §9 GTM target 12); **3 in Phase 1**, **5+ by Phase 2**, **12 by GA** (§10)". Update §9 line 251 from "Target 12" to "Target 8–12" or unify language.
- **D5** Add Phase 1 ARR pencil (line 285-288): "3 design partners × ~$200K avg ACV × 80% close = ~$500K contracted ARR Phase 1."
- **D6** Remove Phase 3 MCP line (line 298) per C3.

### PRD_IRA_ENERGY_DEVELOPER_INTEL.md (340 lines)
- **I1** Add a "Policy context (2025-26)" subsection near §3: acknowledge OBBB (One Big Beautiful Bill, 2025), 48E/45Y phaseout schedules, transferability rule changes. 2-3 paragraphs.
- **I2** Expand BESS row (line 106) revenue streams to include: frequency regulation, capacity markets, hybrid PPA, ITC stacking, ancillary services.
- **I3** In PPA off-take section (line 83 area): distinguish virtual PPA (VPPA, Scope 2 accounting) from physical PPA — different counterparty universe and pricing.
- **I4** Beachhead (line 34): mark Tier-A solar/storage devs as **primary** beachhead (4–6 mo cycle); transferability platforms (Crux/Reunion) as **secondary / partner-led** (different motion). Note explicitly.
- **I5** Remove Phase 3 MCP line (line 286) per C3.
- (No fix needed — Crux/Reunion already correctly framed as partner across lines 34, 204, 228, 286.)

### PRD_INSURANCE_UNDERWRITING.md (343 lines)
- **U1** Line 12: "$1.4T US market" → "$1.4T US GWP (Gross Written Premium, P&C)".
- **U2** Line 122: extend the bulk-API row to include architecture note: "Pre-computed feature store + Postgres BRIN/GIN indexes on geo + cached carrier-portfolio overlays. Sub-500ms p95 requires no live external joins on hot path."
- **U3** Line 120: ST-4 → specify mechanism: "Privacy-preserving via private set intersection (PSI) or salted-hash address join — carrier policy data never leaves their VPC."
- **U4** Cyber peril: promote from Phase 4 (line 288) to Phase 2 SKU (since At-Bay is explicitly in ICP line 29). Add a Phase 2 row "Cyber-peril enrichment (At-Bay style)" — even if minimal scope, it lands the SKU early enough to win that account.
- **U5** Add a short subsection "FL/CA market depth" (~2 paragraphs near §3 or §9) covering FL hurricane/Citizens dynamics and CA wildfire/FAIR Plan dynamics — these are the two largest single-state P&C markets with unique regulatory texture.
- **U6** Beachhead section (line 32 area): add one sentence "Reinsurers (Tier B) excluded from beachhead due to 12–18mo procurement cycles incompatible with the SLA-muscle-building goal of fast parametric design partners."
- **U7** Remove Phase 3 MCP line (line 283) per C3.

### PRD_LITIGATION_FINANCE.md (322 lines)
- **L1** Line 171: PACER access — add fallback row: "If Lex Machina / Bloomberg Law partnership terms unworkable: fall back to **RECAP archive + CourtListener** (Free Law Project) for federal coverage; coverage gaps in state court accepted in Phase 1."
- **L2** Phase 0 (lines 244–250): extend from 6mo to 12mo to align with positioning doc; explicitly call out PACER negotiation as 6–12mo workstream that can run inside Phase 0 without gating it.
- **L3** Line 105: DI-12 (defendant insurance program inference) — add "Governance: insurer inference outputs scored with confidence bands; restricted to litigation-finance underwriting use; NOT sold to insurers as competitive intel (avoids the conflict that would arise from dual-sale)."
- **L4** Beachhead (line 31): keep LF funds as primary; add explicit note that LF funds evaluate both plaintiff and defense angles, so the seller-neutral framing is intentional — but defer any direct-to-insurer or direct-to-plaintiff-firm sales to Phase 3+ until governance plan ratified.
- **L5** Mass-tort PI: add a paragraph in §3 or §9 quantifying the mass-tort PI lead-aggregation TAM (referral economics, top 5 firms, single-event volumes like 3M earplugs / Camp Lejeune scale). Boost from current 6 mentions to a real subsection.
- **L6** Audit unsourced stat claims (lines ~14, 76, 235) — add footnote markers to top 3 claims; cite Westfleet, Burford annual reports, ABA litigation finance survey, etc.
- **L7** Remove Phase 3 MCP line (line 267) per C3.

## Critical files

- `docs/strategy/PRD_DATACENTER_INVESTOR_INTEL.md`
- `docs/strategy/PRD_IRA_ENERGY_DEVELOPER_INTEL.md`
- `docs/strategy/PRD_INSURANCE_UNDERWRITING.md`
- `docs/strategy/PRD_LITIGATION_FINANCE.md`
- `docs/strategy/PLATFORM_POSITIONING_2026.md` (reference only — not edited)

## Out of scope

- No revisions to `PLATFORM_POSITIONING_2026.md` itself
- No revisions to `NEXDATA_SELLABLE_CAPABILITIES_2026.md` or `PERPLEXITY_COMPUTER_COMPARISON_2026.md`
- No git commit — user decides on commit timing after review
- No code changes — these are strategy docs, no `app/` edits

## Verification

Strategy docs — verification is editorial, not technical:
1. `wc -l docs/strategy/PRD_*.md` — net line counts changed sensibly (~+30 to +60 per PRD).
2. `grep -n "MCP / Computer connector" docs/strategy/PRD_*.md` → 0 matches.
3. `grep -n "PLATFORM_POSITIONING_2026" docs/strategy/PRD_*.md` → ≥4 matches.
4. Re-read each PRD's risks, beachhead, Phase 0/1, ARR sections — confirm changes don't conflict with positioning doc.
