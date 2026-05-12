# Platform Positioning — Site Intel as a Horizontal Moat (2026)

**Status:** Draft v1
**Date:** 2026-05-08
**Owner:** Product
**Companion docs:** `PRD_DATACENTER_INVESTOR_INTEL.md`, `PRD_IRA_ENERGY_DEVELOPER_INTEL.md`, `PRD_INSURANCE_UNDERWRITING.md`, `PRD_LITIGATION_FINANCE.md`, `NEXDATA_SELLABLE_CAPABILITIES_2026.md`, `PERPLEXITY_COMPUTER_COMPARISON_2026.md`

---

## 1. The Thesis (one sentence)

We are building a **horizontal Site Intelligence + governance + agentic-research warehouse**, then re-skinning it for four high-ACV buyer verticals. Each vertical is a standalone $50K–$1.5M-ACV product; ~50–70% of the engineering compounds across all four; the bundle is what the incumbents and the agent platforms cannot replicate.

This document exists because the four PRDs (DCII, IRADev, Underwrite, LitInt) are individually coherent but collectively under-reconciled. Their math doesn't add up to the same plan, their Phase 0s overlap without being merged, and they don't describe the cross-sell. This doc fixes that.

---

## 2. The Four Products at a Glance

| | **Underwrite** | **DCII** | **IRADev** | **LitInt** |
|---|---|---|---|---|
| **Buyer** | P&C carriers, parametric startups, MGAs, reinsurers | Digital-infra PE, hyperscaler M&A, infra debt, DC REITs | Solar/wind/BESS developers, IRA tax-credit platforms | Litigation-finance funds, mass-tort firms |
| **TAM proxy** | $1.4T US P&C GWP | $200B+/yr DC capital | $1T+ IRA capital wave | $15B AUM LF |
| **Beachhead** | Parametric startups (Arbol, Floodflash, Kettle) | Tier-A digital-infra PE (Stonepeak, DigitalBridge, KKR Infra) | IRA-credit transferability platforms (Crux, Reunion) | Tier-A LF funds (Burford, Parabellum, Longford) |
| **Sales cycle (beachhead)** | 3–6 months | 9–12 months | 4–8 months | 4–8 months |
| **Avg ACV (year 1, beachhead-discounted)** | $250K | $400K | $250K | $200K |
| **Hardest external dependency** | Sub-second SLA + climate-scenario partner | Hyperscaler shell-LLC entity resolution + queue historization | NREL + Davis-Bacon + state PUC dockets | PACER partnership (Lex Machina / Bloomberg Law) |
| **Net-new infra vs. shared core** | ~40% (real-time triggers, climate scenarios, peril depth) | ~30% (comp DB, queue history, entity graph) | ~30% (NREL, IRA shapefiles, prevailing wage) | ~50% (court data, OSHA/FDA/NHTSA, comp DBs) |
| **Regulatory burden** | High (state insurance, NAIC, Lloyd's) | Low | Medium (IRS attestation) | High (legal ethics, professional responsibility) |

The numbers in this table are the reconciled set. The individual PRDs predate this doc and contain looser figures; treat this table as canonical going forward.

---

## 3. Why This Works (the moat)

We are not competing with Perplexity, Computer, or any general-purpose agent. We are competing with **fragmented incumbents** (Verisk, CoreLogic, RMS, 451, S&P, Bloomberg Law, LexisNexis) and with **internal teams that do this manually**. The agent platforms are tailwinds, not threats — every hyperscaler agent that needs structured private-data access for an investor / underwriter / litigator workflow becomes a route into our MCP server.

The defensible assets stack as follows:

1. **Data depth across 28+ public sources, fused below the city level.** Incumbents are city-level or per-record. We're parcel-level and bundled.
2. **Historization.** ISO queues, EPA enforcement, FERC filings — the *delta over time* is the signal. Our pipeline records the deltas; agents and incumbents do not.
3. **Entity resolution.** Hyperscaler shell-LLCs, defendant subsidiaries, sponsor-equity stacks — the same graph capability serves DCII, IRADev, and LitInt.
4. **Governance + audit trail.** Three of the four products (Underwrite, IRADev, LitInt) require defensible lineage. We have it. Computer cannot produce it.
5. **Re-usable scoring engines.** A site-suitability scoring kernel that powers DC siting also powers solar/wind siting, parametric peril triage, and tort exposure radius modeling.
6. **Cross-product evidence.** When Brookfield buys DCII, the IRADev sale is shorter. When Microsoft buys DCII (asset side), they buy IRADev (off-take side) on the same MSA. The bundle is the moat.

---

## 4. Sequencing Recommendation

**Order:** Underwrite → DCII → IRADev → LitInt.

Each justified below.

### 4.1 Underwrite first (2026-Q2 / Q3 MVP)

- **Fastest cycle to first revenue.** Parametric-insurance startups (Arbol, Floodflash, Kettle) close in 3–6 months because they have urgent data needs, technical buyers, and few procurement gates.
- **Forces us to build sub-second SLA infrastructure + real-time trigger feed**, both of which are reusable across all four products. The hardest infra investment, done first, against the buyer most willing to pay for it.
- **Largest TAM proxy** ($1.4T US P&C GWP). Even a small market share is a large business.
- **Lowest engineering risk.** Most peril data (FEMA, USGS, EPA, NWI, BLS, EIA) is already in the warehouse. Net-new is wildfire / hurricane / severe-storm history + climate scenarios.
- **Reference value.** A signed parametric startup is a fast public reference. Carrier sales (slower, $400–750K) ride on parametric references.

### 4.2 DCII second (2026-Q4 MVP)

- **Highest individual ACV** ($300–750K Tier-A, up to $1.5M hyperscaler).
- **Compounds Underwrite work** — uses the historization muscle, the same governance stack, the same MCP layer.
- **Builds the entity-resolution layer** (hyperscaler shell-LLC graph) that IRADev and LitInt will both reuse.
- **Strategic anchor in capital markets.** A signed Stonepeak or DigitalBridge logo is the most leveragable reference Nexdata can hold — every PE shop in the adjacent verticals will field a call.

### 4.3 IRADev third (2027-Q1 MVP)

- **~70% re-skin of DCII.** Same ISO queues, same FERC, same EIA, same site-intel core, same scoring kernel. Marginal engineering is small relative to greenfield.
- **Adds NREL + IRA shapefiles + Davis-Bacon + state PUC** — incremental, not foundational.
- **Cross-sell wedge.** Brookfield, KKR Infra, Stonepeak, Macquarie, EQT all play in both DC and renewables. DCII customers are pre-qualified IRADev leads.
- **Hyperscaler off-take SKU.** Microsoft, Meta, Google, Amazon already need both DC and renewables intel. One MSA, two products.

### 4.4 LitInt last (2027-Q2+ MVP, contingent on partnership)

- **Smallest TAM** (~30 LF funds globally) — high-ACV but capacity-constrained.
- **Hardest external dependency.** PACER access partnership with Lex Machina, Bloomberg Law, or equivalent is a 6–12-month commercial negotiation. Begin in Phase 0 but don't gate the rest of the plan on it.
- **Re-uses entity-resolution graph** built for DCII (defendant subsidiaries = shell LLCs).
- **Re-uses EPA / OSHA / FDA / NHTSA** ingestion already done in part for Underwrite.
- **Quickest to first dollar once unblocked**, because LF buyers are sophisticated and willing to pay six figures off a single demo. Save for when other revenue funds the partnership negotiation.

---

## 5. Phase 0 — Shared Foundation (2026-Q2 → 2027-Q1)

This replaces the four separate "Phase 0" sections in the individual PRDs. **One shared 12-month foundation program**, sequenced by product launch order.

### 5.1 Quarter-by-quarter

| Quarter | Theme | Critical work | Owner |
|---|---|---|---|
| **2026-Q2** (May–Jun) | Underwrite foundation | Wildfire (USDA + USGS); Hurricane (NHC HURDAT); SCS (NOAA SPC); real-time NOAA + USGS trigger feed; sub-second API perf pass; queue historization v1 (PJM, ERCOT, CAISO) | Backend lead |
| **2026-Q3** | Underwrite MVP + DCII foundation | Underwrite MVP launch + 2 parametric DPs; climate-scenario v1; DC comp DB manual seed (200 deals); hyperscaler shell-LLC entity-resolution v0 (Meta, Microsoft); NREL ingestion begins | Two parallel pods |
| **2026-Q4** | DCII MVP + IRADev foundation | DCII MVP + 3 DPs; Underwrite scales to 5 DPs (3 parametric + 2 carrier); energy-community + Justice40 shapefile processing; Davis-Bacon prevailing-wage; PACER partnership negotiation begins | Three pods |
| **2027-Q1** | IRADev MVP + LitInt foundation | IRADev MVP + 3 DPs; DCII scales to 5 DPs; OSHA, FDA FAERS/MAUDE, NHTSA ingestion; SEC legal-proceedings NLP extractor; PACER deal close target | Four pods |

### 5.2 Critical-path items (anything here slipping shifts launches)

1. **Sub-second API + real-time trigger feed.** Underwrite parametric SLA depends on it. Done by end of Q2 2026.
2. **ISO queue historization.** DCII portfolio monitor + IRADev queue tracker + Underwrite trigger latency all depend. Done by mid-Q3 2026.
3. **Hyperscaler shell-LLC entity-resolution.** Required for DCII sourcing radar, IRADev competitive pipeline, LitInt defendant graph. v0 done by Q3 2026; iterative thereafter.
4. **Climate-scenario layer.** Either build native or partner (Jupiter / First Street). Decide by end of Q2 2026; deliver v1 by end of Q3.
5. **PACER partnership.** Long lead. Open conversations Q2 2026; signed by Q1 2027 or push LitInt to 2027-Q3.

### 5.3 Net-new infra explicitly NOT done in Phase 0

- Bordereaux + NAIC reporting templates (Underwrite Phase 2)
- BESS revenue stacking model (IRADev Phase 2)
- Verdict comp DB (LitInt Phase 2 — partnership with Verdictsearch)
- DC comp database expansion past 200 deals (DCII Phase 2)

---

## 6. Cross-Sell Motion

The four-product strategy is only as good as the cross-sell. We expect ~30% of Year-2 revenue to come from accounts holding two or more products.

### 6.1 Buyers who span multiple PRDs

| Buyer archetype | Example accounts | Likely products | Year-1 → Year-3 path |
|---|---|---|---|
| Diversified infra PE | Brookfield, KKR Infra, Macquarie, EQT, Blue Owl | DCII + IRADev (+ Underwrite for asset-level peril on portfolio) | Land DCII → expand IRADev → cross-sell Underwrite Y3 |
| Hyperscaler corp dev | Microsoft, Meta, Google, AWS, Oracle | DCII (asset side) + IRADev (off-take side) | DCII MSA → IRADev Off-taker SKU added Y2 |
| Mid-market commercial carrier | Hanover, Cincinnati, Erie | Underwrite + (later) LitInt for D&O/E&O exposure | Underwrite Y1 → adjacency Y3 |
| Plaintiff environmental firm + LF fund | Motley Rice + Burford / Levin Papantonio + Parabellum | LitInt | LF fund first; co-marketed plaintiff-firm sale |
| Sovereign / pension LP | CPP, GIC, OTPP, CalPERS | DCII + IRADev (LP-side SKU) | DCII LP-advisor → IRADev LP-advisor Y2 |

### 6.2 Bundle pricing

Standalone Enterprise SKUs across the four products would total $1.5–4M for a 3-product customer. We should not charge that. Bundle pricing reflects the shared infra reality and the strategic value of multi-product anchor accounts.

| Configuration | Standalone total | Bundle price | Discount | Strategic notes |
|---|---|---|---|---|
| DCII Enterprise + IRADev Enterprise | $1.0–2.0M | $0.85–1.5M | ~25% | Most common bundle (digital-infra PE + IPP) |
| DCII Enterprise + IRADev Enterprise + Underwrite Carrier | $1.4–2.7M | $1.1–2.0M | ~25–30% | Brookfield-style diversified infra |
| DCII Enterprise + IRADev Off-taker | $1.0–1.5M | $0.85–1.2M | ~20% | Hyperscaler bundle |
| Underwrite Carrier + Underwrite Reinsurer (multi-entity) | $1.2–2.3M | $1.0–1.8M | ~20% | Carrier + reinsurance arm |
| Three or four products | varies | varies | 30–35% | Custom; warrants exec-level deal review |

Bundle pricing is **per-MSA**, not per-product. One MSA, one CSM, one billing motion. This both reduces sales friction and discourages customers from playing the SKUs against each other.

### 6.3 Land-and-expand sequencing in a multi-product account

1. Land on the highest-pain product for that buyer (parametric → Underwrite; PE infra → DCII).
2. Co-prove a second use case in months 3–6 of the contract using the same data warehouse.
3. Expand into the bundle at renewal (month 12), with the second product priced as the "incremental" rather than additive ACV.
4. Reference-rights provision in the design-partner contract gives us logos to sell adjacent products into.

---

## 7. Reconciled Financials

The individual PRDs claimed Year-2 ARR aggregating to ~$15–30M. That math doesn't survive realistic sales-cycle and ACV assumptions. The reconciled view:

| Product | Y1 (2026) ARR | Y2 (2027) ARR | Y2 design partners | Avg ACV (Y2) | Notes |
|---|---|---|---|---|---|
| Underwrite | $0.5–1.0M (2026-Q4 only) | $4–6M | 5–7 | $700K | Mix of parametric ($200–500K) + 2 carriers ($400–750K) |
| DCII | $0.3–0.6M (Q4 only) | $3–5M | 6–8 | $500K | Tier-A PE beachhead at 50% discount; full-price by Q4 2027 |
| IRADev | — (launches Q1 2027) | $2–3M | 5–7 | $400K | Tier-A devs + Crux/Reunion; OBBB / IRA-amendment risk noted |
| LitInt | — (launches Q2 2027 if PACER signed; else later) | $1.5–2.5M | 5–8 | $250K | LF funds + plaintiff firms |
| **Aggregate** | **$0.8–1.6M** | **$10.5–16.5M** | **21–30** | — | Less aggressive than sum-of-PRDs but defensible |

Year-3 (2028) target: **$25–40M ARR** as design partners renew at full price, bundle expansion lands, and Phase 2 SKUs (treaty aggregation, comp engines, climate scenarios) carry premium pricing.

The **aggregate Year-2 figure is $10–17M, not $15–30M**, because of three corrections:
- Sales cycles for carrier and Tier-A PE are 9–12 months, so design partners signed in Q4 2026 are not full-year revenue in 2027.
- Beachhead discounts (40–50%) compress Year-2 ACV.
- LitInt may slip to 2027-Q3 if the PACER partnership negotiation runs long.

---

## 8. Org / Hiring Plan

Executing four products on this timeline requires deliberate org structure. Today's Nexdata team (assumed ~5–8 engineering FTE) is not enough.

### 8.1 Net-new hires through 2027-Q1

**Engineering:**
- 2 senior backend engineers — real-time triggers, sub-second API, queue historization
- 1 ML engineer — entity resolution, NLP for SEC / disclosure mining
- 1 data ops engineer — comp database curation across DCII + IRADev + LitInt

**Product:**
- 1 PM per vertical (4 PMs by 2027-Q1, hired in launch order)

**Sales / GTM:**
- 1 AE per vertical (4 AEs by 2027-Q1, hired ahead of MVP launch)
- 2 CSMs (one for Underwrite + DCII, one for IRADev + LitInt)

**Domain experts (advisor or fractional):**
- Ex-DC-investor (DCII) — currently or recently at Stonepeak / DigitalBridge / KKR Infra
- Ex-parametric-insurance founder (Underwrite) — Arbol / Floodflash alum
- Renewables-developer-side veteran (IRADev) — Pattern / Invenergy / AES alum
- Litigation attorney (LitInt) — comp curation, ethics review, advisory

**Total net-new:** ~12–15 FTE + 4 advisors through 2027-Q1.

### 8.2 Org structure principle

Pod-per-product on the front end (PM + AE + CSM owns the buyer relationship); shared engineering on the back end (one warehouse, one governance stack, one MCP server). Pods do not own engineering capacity — they pull from a shared backlog managed by the eng leadership. This is what makes the shared-infra story real instead of cosmetic.

---

## 9. Risks to the Platform Thesis (cross-PRD)

| # | Risk | Why it matters | Mitigation |
|---|---|---|---|
| R1 | **Sequencing slippage on Underwrite SLA work** | If real-time trigger feed slips past Q3 2026, parametric DPs walk and the cross-product SLA story collapses | Treat as critical-path; weekly exec review; Jupiter/First Street fallback for climate scenarios |
| R2 | **Hyperscaler entity-resolution accuracy is below threshold** | DCII sourcing radar + IRADev pipeline + LitInt defendant graph all degrade together | Confidence-tier outputs; manual review queue; allocate ML engineer as hire #1 |
| R3 | **PACER partnership fails** | LitInt MVP slips 6+ months | Plan B: RECAP Archive + CourtListener (Free Law Project). Lower-quality but unblocks MVP. |
| R4 | **IRA policy reversal under future US administration** | IRADev TAM evaporates | State-level RPS/CES + non-US (Canada, EU) expansion in Phase 4; hedge by selling IRA-stacking as one of many SKUs in IRADev, not the whole product |
| R5 | **Verisk / CoreLogic / RMS bundle a competitive offering at near-zero marginal cost** | Underwrite economics under pressure | Differentiate on parcel-fusion + audit + per-call pricing rather than per-record |
| R6 | **Bundle pricing erodes per-product ACV faster than cross-sell expands wallet** | Aggregate revenue underperforms standalone projections | Annual pricing review; minimum-floor per product in any bundle; reference-rights as non-cash compensation |
| R7 | **Sales hiring lags eng hiring** | MVPs ship to no buyer | Hire AEs *before* PM, *before* MVP — 90 days lead time on enterprise deals |
| R8 | **Customer-buy concentration in 3–5 mega-PE accounts (Brookfield, KKR, Stonepeak, etc.)** | One churn event = double-digit ARR loss | Diversify across DCII Tier-A + Tier-B; Underwrite carrier mid-market; intentional logo distribution |
| R9 | **Internal teams at top accounts (DigitalBridge, Stonepeak, Burford) build in-house** | Largest-ACV accounts could insource | Position below the model; sell warehouse + signal feeds, not the analytical layer; make in-house cheaper to *augment* than to *replace* |
| R10 | **Audit / lineage system fails a regulatory or legal-discovery test** | Reputational damage across all four products | Annual external audit of governance stack; clear methodology docs; hire compliance lead by 2027-Q1 |

---

## 10. Strategic Posture vs. Adjacent Categories

| Adjacent player | Their business | Our relationship |
|---|---|---|
| **Verisk, CoreLogic, RMS, AIR** | Catastrophe modeling + per-record peril data | Compete on parcel fusion + audit + price; partnership unlikely |
| **451 Research, Synergy, Cushman, JLL DC Practice** | DC market research | Compete on live-data; partnership opportunistic |
| **Wood Mackenzie, S&P Platts** | Energy industry research | Compete on parcel + IRA stack depth; partnership opportunistic |
| **Lex Machina, Bloomberg Law** | Court analytics | Partner (PACER access); coexist (we're defendant-side intel, they're outcomes data) |
| **Crux, Reunion, LevelTen** | IRA tax-credit transferability + PPA market | Partner (we're upstream — site sourcing); revisit if their data layer underperforms |
| **Jupiter, First Street** | Climate-projection models | Partner (climate scenarios); build native if margin justifies in Y3 |
| **Cape Analytics, Arturo** | Imagery-derived structural inference | Partner (joint UW sale); not competitive |
| **Computer / Perplexity / OpenAI / Gemini** | General-purpose agents | **Partner — they consume our MCP server.** Strategic surface area, low CAC, brand. Not competitor. |
| **In-house teams at Stonepeak, Burford, Hanover, etc.** | Custom analyst stacks | Position below the model; replace the plumbing, leave the alpha |

The recurring strategic posture across all four products: **be the data and governance layer below the buyer's model, not the model itself.** This protects the moat (incumbents and AI agents both lack our data depth) and avoids the most common B2B SaaS death spiral (competing with a well-funded customer's internal team).

---

## 11. The Agentic-Platform Bet

A cross-PRD investment that gets one section here instead of being repeated four times.

We ship a **single Nexdata MCP server** that exposes all four products' query surfaces to Claude Code, Computer, Gemini, ChatGPT, and any other LLM agent. One investment, four products, every hyperscaler agent platform as distribution.

Why this matters:
- The buyers in all four ICPs (PE associates, underwriters, developers, LF analysts) are increasingly using LLM agents day-to-day. A "site-intel API" that is hard for an agent to call is invisible to the next-generation user.
- MCP is a wedge for free-tier discovery. An agent at a non-customer firm hits our MCP server, gets metered + branded results, leaves a trail in our analytics — that's lead-gen with no SDR.
- Defensibly governed. Audit trail + lineage + provenance survive the LLM layer; an agent calling our MCP server still gets the same defensible outputs, with the agent's prompt logged.

Build target: MCP server v1 by 2026-Q4, exposing Underwrite + DCII first, IRADev + LitInt as they ship. Ship a public catalog page. Sponsor agent-platform showcases.

---

## 12. What Success Looks Like at the Platform Level

A managing director at Brookfield Renewable starts her morning. Her LLM agent (running locally, calling our MCP) summarizes overnight intel: a Vantage-affiliated permit in Atlanta (DCII signal), a NoVA queue position improvement on a portfolio asset (DCII portfolio monitor), an energy-community designation update covering two BlueOwl-funded wind sites in West Texas (IRADev), and a SCS event near a portfolio-level industrial property in Tulsa flagged by their P&C carrier's system (Underwrite — though they don't know that's us). She clicks the Atlanta opportunity, gets a full diligence pack with audit trail, drags it into the IC deck, and flags it to her partner. Three hours of work in nine minutes.

Six months later, Brookfield signs a $1.4M three-product MSA. They pay one bill. We have one CSM. The same warehouse fed all three workflows. The agent platform is the new front door.

That is the business.

---

## 13. Open Questions

| # | Question | Owner | Decision needed by |
|---|---|---|---|
| Q1 | Build climate scenarios native or partner with Jupiter / First Street? | Eng lead + Product | 2026-Q2 |
| Q2 | Which agent platform (Claude, Computer, Gemini, ChatGPT) gets first MCP showcase? | GTM lead | 2026-Q3 |
| Q3 | Common brand umbrella for the four products, or four standalone product brands? | Marketing + Founder | 2026-Q3 |
| Q4 | Hire the LF-side attorney before the PACER deal, or after? | Founder | 2026-Q4 |
| Q5 | Do we publish quarterly free reports per vertical (4 reports/quarter) or one combined "State of Capital Intelligence" quarterly? | Marketing | 2026-Q3 |
| Q6 | What is the ARR floor at which we open international expansion (UK, EU)? | Founder | 2027-Q2 |
| Q7 | Should design-partner contracts include exclusivity clauses (no competing GP / carrier in the same vertical for 12 months)? | Legal + GTM | 2026-Q2 |
| Q8 | Where does cyber peril live — extension of Underwrite, or fifth product? | Product + GTM | 2027-Q1 |

---

## 14. What This Doc Does Not Cover

- Detailed feature requirements per product (see individual PRDs).
- Detailed competitor analysis per product (see individual PRDs).
- Engineering architecture for the shared warehouse, governance stack, scoring kernel, MCP server (separate eng-led docs to follow).
- Funding / capital plan.
- Marketing playbook beyond beachhead motion.

---

## 15. Sources / Internal References

- `docs/strategy/PRD_DATACENTER_INVESTOR_INTEL.md`
- `docs/strategy/PRD_IRA_ENERGY_DEVELOPER_INTEL.md`
- `docs/strategy/PRD_INSURANCE_UNDERWRITING.md`
- `docs/strategy/PRD_LITIGATION_FINANCE.md`
- `docs/strategy/NEXDATA_SELLABLE_CAPABILITIES_2026.md`
- `docs/strategy/PERPLEXITY_COMPUTER_COMPARISON_2026.md`
