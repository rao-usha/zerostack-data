# PRD — Datacenter Investor Intelligence

**Status:** Draft v1
**Date:** 2026-04-30
**Owner:** Product
**Companion docs:** `NEXDATA_SELLABLE_CAPABILITIES_2026.md` (capability #6 is the seed), `PERPLEXITY_COMPUTER_COMPARISON_2026.md`

> **See also:** [`PLATFORM_POSITIONING_2026.md`](./PLATFORM_POSITIONING_2026.md) — canonical roll-up across the four vertical PRDs (DCII, IRADev, Underwrite, LitInt). Owns cross-product sequencing (Underwrite → DCII → IRADev → LitInt), the shared 12-month Phase 0 program, bundle pricing, the single MCP investment, and the platform-reconciled Y2 ARR aggregate. **DCII is sequenced second** — highest ACV in the four, and it builds the hyperscaler entity-resolution graph that IRADev and (partially) LitInt will reuse.

---

## 1. Executive Summary

Today's Datacenter Site Suitability product is a **single-site scoring tool**. Buyers are mostly *developers* asking "is this site good?" That's a small wedge in a large category.

The bigger product is **Datacenter Investor Intelligence (DCII)** — a platform built for the *capital* side of datacenters, not just the dirt side. Investors and lenders deploying $200B+ a year into DC assets need market-level, portfolio-level, and competitor-level intelligence — not just "score this parcel." They need to underwrite power risk across a fund, monitor interconnect-queue slippage on portfolio assets, track hyperscaler tenant exposure, model AI-driven demand by metro, and surface secondary-market deal flow before bankers shop it.

This PRD scopes a deeper datacenter product targeting **digital-infra PE, infra debt, hyperscaler M&A, DC REITs, and debt-fund underwriters**. It reuses 90% of the existing Site Intel + Org Chart + Deal Probability stack, layered with DC-specific data we already collect (PeeringDB, Epoch, ISO interconnect queues, EIA, FERC, FCC, NREL).

**Target outcome:** $200K–$1M ACV per investor firm. 8–12 design partners in 2026-H2. $5–10M ARR by end of 2027 *(standalone target — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled Y2 aggregate of $10.5–16.5M across all four products)*.

---

## 2. Target Buyers (ICP)

| Tier | Segment | Examples | Pain | ACV |
|---|---|---|---|---|
| **A** | Digital-infra PE / infra funds | Stonepeak, DigitalBridge, KKR Infra, Brookfield Infra, EQT Infra, Macquarie, Blue Owl Digital, GIP, IFM, Antin | Sourcing + monitoring DC assets across geographies, calibrating AI-demand assumptions, exit-comp triangulation | $300–750K |
| **A** | Hyperscaler M&A / corp dev | Microsoft, Meta, Google, AWS, Oracle, ByteDance | Site/operator/tenant intel, supply pipeline, who-owns-what-where | $500K–$1.5M |
| **B** | DC operator strategy / corp dev | QTS, Aligned, EdgeConneX, NTT, Vantage, CyrusOne, Compass, Iron Mountain DC | Competitive intel, M&A targets, tenant churn risk, market-supply forecasting | $150–400K |
| **B** | Infra debt funds | Carlyle Infra Credit, IFM Debt, Brookfield Infra Debt, KKR Credit, Apollo, Ares, BlackRock Infra Debt | Power-availability risk, interconnect-queue slippage on collateralized assets, lease-rollover risk | $200–500K |
| **B** | DC REITs / public-side analysts | Equinix, Digital Realty (and the buy-side / sell-side analysts covering them) | Market-by-market supply tracking, comp data, queue intel for guidance | $100–300K |
| **C** | Power-aware crypto / AI compute investors | Riot, Cipher, IREN, Core Scientific, Hut 8, CoreWeave, Lambda, Nebius | Power siting, lease-vs-build economics, ISO queue position | $100–250K |
| **C** | LP advisors / placement agents into infra | StepStone, Hamilton Lane, Cliffwater, Mercer, Cambridge | Sector exposure, GP underwriting, attribution | $75–200K |

**Beachhead:** Tier A digital-infra PE. They have the budget, the analytical sophistication to use the product, and the brand value to flip into reference customers.

---

## 3. Problem Statement

Datacenter capital allocation in 2026 is bottlenecked by data fragmentation. A typical investor workflow today:

1. **Sourcing.** A junior associate manually tracks `Data Center Frontier`, `Mighty Buildings`, LinkedIn announcements, FERC filings, and county news to find new operator/site activity. Misses the vast majority. Pipeline is reactive.
2. **Underwriting a target.** They pull EIA data, call the local utility, scrape the ISO interconnect queue manually, look up FCC fiber, check FEMA flood, all by hand. Each project burns 80–150 analyst-hours.
3. **Power risk.** Most investors have no view into queue-position changes after close. A site that was "approved interconnect Q3 2027" can slip to 2030. They find out from the operator, late.
4. **Tenant exposure.** "Who is in this datacenter and how concentrated are we?" is answered by reading press releases. There is no structured tenant-by-DC dataset.
5. **Market supply.** "How many MW are coming online in Northern Virginia in 2027?" is a Bloomberg article + a McKinsey deck. There is no live, parcel-level supply tracker.
6. **Exit comps.** EV/MW comps are pulled from press-release math. Dispersion is enormous and often wrong.

The result: capital is chasing fewer and fewer trophy assets while missing tier-2 markets, mispricing power risk, and underwriting AI-demand curves with no signal density.

---

## 4. Product Overview

DCII is a **four-layer product** built on the existing Nexdata warehouse:

```
┌─────────────────────────────────────────────────────┐
│ Layer 4 — Investor workflows                        │
│ Sourcing radar · Portfolio monitor · Exit comps     │
│ AI-demand model · Tenant exposure · Power-risk      │
├─────────────────────────────────────────────────────┤
│ Layer 3 — Market & competitor intel                 │
│ Operator graph · Site graph · Tenant graph          │
│ Market-supply tracker · Interconnect queue feed     │
├─────────────────────────────────────────────────────┤
│ Layer 2 — DC-specific data fusion                   │
│ Power · Fiber · Cooling · Latency · Incentives      │
├─────────────────────────────────────────────────────┤
│ Layer 1 — Existing Nexdata Site Intel + governance  │
└─────────────────────────────────────────────────────┘
```

### 4.1 The eight workflows the product supports

| # | Workflow | Persona | What they do today | What DCII gives them |
|---|---|---|---|---|
| 1 | **Sourcing radar** | Associate / VP | Manual scraping of news, LinkedIn, county filings | Live feed of new builds, M&A signals, interconnect filings, leadership changes at operators |
| 2 | **Target diligence** | Deal team | 80–150 analyst-hours per target | One-click full diligence pack: power, fiber, tenants, comps, queue, risk |
| 3 | **Power risk monitoring** | Asset manager | Operator self-reporting | Continuous queue-position tracking + utility curtailment risk |
| 4 | **Market supply tracker** | Strategy | Reads industry decks | Parcel-level live MW pipeline by metro, by year, by stage |
| 5 | **Tenant exposure / churn** | Asset manager | Press releases + operator reports | Tenant-by-DC graph with lease-term inference and concentration scoring |
| 6 | **Exit comps** | Deal team | Press-release math | EV/MW + EV/MW-leased + EV/EBITDA distribution by metro and tier |
| 7 | **AI-demand modeling** | Investment committee | Vendor research | First-party signal: hyperscaler cap-ex disclosures + permits + interconnect filings + chip shipment data |
| 8 | **Operator org / leadership intel** | BD / partners | LinkedIn detective work | Live org charts and leadership-change feed across 200+ operators |

### 4.2 Output formats

- **Web app** (the primary interface — dashboards, search, drilldowns)
- **Sourcing alerts** (email + Slack + webhook; daily / real-time)
- **Quarterly Market Reports** (HTML/PDF, per metro, branded)
- **Diligence Packs** (PDF + XLSX, one-click on any operator or address)
- **API + GraphQL** (for quants and integration with internal models)
- **MCP server** (so Computer/Claude/Gemini agents can query DCII data — ride the agent wave instead of fight it)

---

## 5. Feature Requirements

Coded as **M** (must, GA), **S** (should, post-GA), **C** (could, future).

### 5.1 Sourcing Radar

| ID | Feature | Priority | Notes |
|---|---|---|---|
| SR-1 | New-build detection (permits, FERC interconnect filings, county zoning, news) | M | Already partially built — extend to DC-specific |
| SR-2 | M&A / financing event detection (SEC, news, Form D for sponsors) | M | Reuse Deal Probability Engine |
| SR-3 | Operator leadership-change feed | M | Reuse Org Chart pipeline |
| SR-4 | Hyperscaler permit / land-acquisition tracking | M | Per-county zoning + LLC graph for shell entities (Greenlands, NoVA Realty, etc.). **Shares the underlying entity-resolution layer with TD-2 — different consumer surfaces, one graph.** |
| SR-5 | Distress signals (utility filings, tax-lien, tenant churn rumors) | S | Higher-rigor signal layer |
| SR-6 | Configurable saved-search alerts (email/Slack/webhook) | M | Per-firm thesis filters |
| SR-7 | "Who else is looking" intel (filings frequency, LP commitments, GP fund-raising activity) | C | Aggregate-only, anonymized |

### 5.2 Target Diligence Pack

| ID | Feature | Priority | Notes |
|---|---|---|---|
| TD-1 | Address / parcel → full Site Intel report (existing) | M | Already shipping |
| TD-2 | Operator → entity graph (parents, JV partners, shell LLCs) | M | New work — entity resolution layer. **Shared with SR-4 (hyperscaler permit / LLC tracking) — build once, both surfaces consume.** |
| TD-3 | Operator → tenant graph (who's in their DCs) | M | Tenant inference from press releases, NDA-respecting |
| TD-4 | Operator → financial / leverage profile (SEC filings, bank covenants if public, news) | M | Reuse SEC ingestor |
| TD-5 | Operator → comparable transactions (EV/MW, multiples, structure) | M | New comp database |
| TD-6 | Power-side diligence: utility, ISO queue, transmission proximity, grid stability, reliability index | M | Existing |
| TD-7 | Fiber-side diligence: PeeringDB neighbors, fiber-route density, latency to peering points | M | Existing |
| TD-8 | Cooling diligence: water (USGS, drought projection), ambient temperature trend | M | Existing |
| TD-9 | Risk diligence: FEMA, USGS, EPA, ACRES, NWI | M | Existing |
| TD-10 | Incentives diligence: state/local tax abatements, energy-community status, sales-tax exemptions | M | Existing |
| TD-11 | Latency diligence: distance to top-50 enterprise nodes, peering-point ms | S | Synthetic latency model |
| TD-12 | Generate one-click diligence PDF + XLSX | M | Reuse Reports framework |

### 5.3 Portfolio Monitor

| ID | Feature | Priority | Notes |
|---|---|---|---|
| PM-1 | Per-asset queue-position tracking with deltas over time | M | New — historized ISO queue snapshots |
| PM-2 | Per-asset utility curtailment / outage event log | M | EIA + ISO real-time |
| PM-3 | Per-asset transmission-build pipeline tracker | M | FERC + ISO |
| PM-4 | Per-asset tenant-concentration score + lease-rollover model | S | Inference layer |
| PM-5 | Per-asset risk re-score (FEMA / fire / drought) on update | M | Existing site intel |
| PM-6 | Portfolio aggregate dashboards (by ISO, by metro, by tier) | M | New visualization |
| PM-7 | Alert when an asset's score moves >X% | M | |

### 5.4 Market Supply Tracker

| ID | Feature | Priority | Notes |
|---|---|---|---|
| MS-1 | MW supply by metro × stage (announced, permitted, under construction, online) | M | Aggregate of permit data + news + operator disclosures |
| MS-2 | MW absorption by metro (lease-up rate, vacancy) | S | Inference — operator filings + 451 Research-style triangulation |
| MS-3 | Hyperscaler vs. retail mix per metro | S | |
| MS-4 | Per-metro power-availability constraint score | M | ISO queue + utility filings |
| MS-5 | Forecast 12/24/36-month MW pipeline by metro | M | Statistical model on top of MS-1 |
| MS-6 | Tier-2 emerging-metro discovery (where the next NoVA is) | M | Differentiator: most tools cover top-7 only |

### 5.5 Tenant & Lease Intelligence

| ID | Feature | Priority | Notes |
|---|---|---|---|
| TI-1 | Tenant graph by datacenter (which companies, inferred MW, inferred lease term) | M | Press release + permit + LLC parsing |
| TI-2 | Tenant concentration score per asset | M | |
| TI-3 | Hyperscaler take-down forecast by metro | S | |
| TI-4 | Enterprise tenant migration signals (companies announcing moves) | C | News + LinkedIn |

### 5.6 Comp & Valuation Engine

| ID | Feature | Priority | Notes |
|---|---|---|---|
| CV-1 | Comp database: EV/MW, EV/MW-leased, EV/EBITDA, $/sqft for DC transactions 2018+ | M | Build by hand from press + filings, then maintain |
| CV-2 | Comp filtering by metro, tier (1/2/3), tenant mix, contract length | M | |
| CV-3 | Build-vs-buy economics model (developer perspective) | S | |
| CV-4 | Lease-comp database (MW lease rates by metro, by tier, by tenant type) | S | Inference + survey |

### 5.7 AI-Demand Model

| ID | Feature | Priority | Notes |
|---|---|---|---|
| AD-1 | Hyperscaler cap-ex disclosure tracker (10-K, 10-Q, earnings) | M | Reuse SEC ingestor |
| AD-2 | Permit-flow tracker for hyperscaler-affiliated LLCs | M | |
| AD-3 | Chip-shipment / GPU-supply signal (export controls, customs, supplier filings) | S | |
| AD-4 | Power-density trend tracker (kW/rack, kW/sqft) | S | |
| AD-5 | Per-metro AI-demand index | M | Compose from AD-1..4 |

### 5.8 Operator & Leadership Intel

| ID | Feature | Priority | Notes |
|---|---|---|---|
| OL-1 | Operator org chart per company (live, versioned) | M | Reuse Org Chart pipeline |
| OL-2 | Leadership-change feed (departures, hires) | M | |
| OL-3 | Operator-to-LP capital tracking (who funded them) | S | Form D + news |
| OL-4 | Operator-to-operator JV / partnership graph | S | |

---

## 6. Data Sources

### Already built (reuse)
- ISO interconnect queues: PJM, CAISO, ERCOT, MISO, SPP, NYISO, ISONE
- EIA (grid, generation, capacity, fuel mix)
- FERC (filings, transmission, interconnection agreements)
- FCC fiber, PeeringDB, Epoch datacenters
- USGS water, FEMA flood, NWI wetlands, ACRES contamination
- Census BPS (building permits), county zoning, Yelp/Google maps
- BLS QCEW (labor)
- State/local incentive databases
- SEC EDGAR (10-K, 10-Q, 8-K, Form D)
- Org Chart + People stack
- News + press release ingestion

### New to build
- **DC transaction comp database** — scrape + manual seed for 500+ historical deals 2018+
- **DC tenant inference layer** — text mining from press releases, permits, LinkedIn
- **Hyperscaler shell-LLC graph** — entity resolution to track Greenlands / shadow entities back to MSFT/Meta/Google
- **Historized queue tracking** — daily snapshots of ISO interconnect queue positions, retroactive backfill where possible
- **Power-density signal feed** — manufacturer disclosures + permits + thermal spec leaks
- **Lease-rate inference layer** — triangulation from public REIT disclosures, operator filings, news

### Possible partner / license
- **451 Research / Synergy** — for established market sizing if we want a faster ramp on supply data (low priority — we'd rather build native)
- **Equinix Internet Exchange data** — for traffic / peering signal
- **Cushman / JLL DC reports** — if we want lease-rate corroboration

---

## 7. Why Perplexity / Computer Cannot Do This

1. **Historized queue data.** ISO interconnect queues update daily and the *delta* is the signal. A one-shot agent has no historical store. We do.
2. **Entity resolution.** Hyperscaler shell-LLCs (Greenlands → Meta) are not solved by web search. Requires a maintained graph.
3. **Comp database.** EV/MW dispersion is enormous because the comps require structure. A general agent will pull 5 numbers from press releases and average them — wrong.
4. **Tenant inference.** Tenants are NDA'd. Inference requires multi-source triangulation our pipeline does and a one-shot LLM cannot.
5. **Reproducibility.** Every number in an IC memo needs an audit trail. Computer cannot produce one.
6. **Domain-specific scoring.** Our scoring layer is calibrated against actual DC outcomes (queue-slippage rates, vacancy realization, multiple realization). A new agent run cannot calibrate from cold.

---

## 8. Differentiation vs. Existing Players

| Competitor | What they do | Where DCII wins |
|---|---|---|
| **DataCenterMap.com** | Static directory of operators/sites | We have power, fiber, queue, comps, tenants, governance |
| **datacenterHawk** | Wholesale DC inventory + leasing tracker (industry standard for inventory) | DCH stops at "where are the buildings, what's leased." We layer investor workflows (sourcing radar, comps, AI-demand, exposure) + governance / lineage on top. Co-existence likely — they're a candidate data input, not a substitute. |
| **Mighty Buildings / Mighty (DC news + analytics)** | News + announcement aggregation | News surface — upstream of us. We structure their feed into the warehouse and add the missing layers (queue history, tenants, comps). |
| **Rebellion Research / similar AI-demand modelers** | Vendor research on AI compute / power demand | Vendor reports; we're a live signal feed, not a quarterly thesis deck. (Note: 05-08 review listed "Rebellion" — re-validate which firm before GA marketing.) |
| **451 Research / S&P** | Industry research reports | We're live, parcel-level, queryable, not a quarterly PDF |
| **Cushman / JLL / CBRE DC Practice** | Brokerage data + lease comps | We have full investor stack — sourcing through monitoring |
| **Synergy Research Group** | Market-share data | Market-share is one report; we're the underlying warehouse |
| **In-house teams at DigitalBridge / Stonepeak** | Proprietary models | We become their *infrastructure* — they keep their alpha, we're below the model |

**Strategic posture:** Don't try to replace internal models. Be the **structured data layer** under them. Sell governed warehouse + raw signal feeds; let the LP/GP keep their model on top.

---

## 9. GTM

### Beachhead motion
- Target **8–12 Tier-A** digital-infra PE firms with named-account outbound from week 1 (canonical partner-count language; aligns with §1 and §10's 3 → 5+ → 12 ramp)
- Demo: real address → diligence pack in 90 seconds; live queue tracker on a known portfolio asset; AI-demand index for NoVA
- Pricing: $300K design-partner deal with 50% discount in exchange for product feedback + reference rights

### Channel plays
- **Investor conferences:** Datacloud, PTC, MWC, North American Datacenters Conference. Sponsor a small booth, demo on-site.
- **Newsletter:** quarterly "State of the DC Market" report, free, distributed to 1,000+ digital-infra investors. Lead-gen.
- **MCP distribution:** the unified Nexdata MCP server (shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11) exposes the DCII surface so AI agents at investors pull DCII data via Computer/Claude/Gemini. Brand surface area, low CAC. We do not stand up a DCII-specific MCP — the platform ships one.
- **Sell-side analyst seeding:** 5 analysts at MS / GS / JPM covering DLR/EQIX get free access in exchange for citation in their notes.

### Pricing
| Tier | ACV | Includes |
|---|---|---|
| **Analyst** | $75K | 3 seats, web app, sourcing radar, diligence packs (50/yr), monthly market report |
| **Team** | $250K | 10 seats, all of Analyst + portfolio monitor, comp database, alerts, API |
| **Enterprise** | $500K–$1M | Unlimited seats, all of Team + governed warehouse, lineage/audit, MCP, SSO, dedicated CSM |

### Success metrics (year 1 of DCII GA)
- 8 design partners signed by 2026-Q4
- $3M ARR by 2027-Q1
- $7–10M ARR by 2027-Q4 *(standalone — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled aggregate)*
- 80% renewal rate on design partners
- 3 named-customer case studies

### 9.5 Cross-sell to other Nexdata products

DCII's Tier-A digital-infra PE beachhead overlaps materially with the buyer universes of the other three vertical products. Per `PLATFORM_POSITIONING_2026.md` §6, multi-product MSAs price at ~25% off for two products and ~30% for three+, so cross-sell is real economic motion — not just a logo argument.

| DCII account | Also fits | Why |
|---|---|---|
| Brookfield Infra, KKR Infra, EQT Infra, GIP, IFM, Macquarie | **IRADev** | Same digital-infra PE firms underwriting both DC assets and renewable / battery developers. IRADev gives them the IRA-credit + transferability + interconnect-queue intel for the energy side of the same portfolio. Highest-likelihood 2-product bundle. |
| Microsoft, Meta, Google, AWS, Oracle | **IRADev**, **Underwrite** (corporate captive insurance) | Hyperscalers buy renewable PPAs at scale (IRADev) and run captive insurance entities (Underwrite). Three-product bundle plausible. |
| Carlyle Infra Credit, Apollo, Ares, BlackRock Infra Debt | **LitInt** | Debt-fund underwriting overlaps with litigation-finance funds — same legal-event + counterparty-risk modeling muscle. Adjacent buyer with shared analytical workflow. |
| Stonepeak, DigitalBridge | **All three** | Most likely Brookfield-style 3-product MSA candidates ($1.1–2.0M ACV — see positioning doc §6) |

**Action:** Tag every DCII design-partner contract with "candidate for IRADev cross-sell after Phase 1 close" so the IRADev pod has a warm pipeline at GA. Bundle MSA template lives in positioning doc §6.

---

## 10. Phasing

### Phase 0 — Foundation hardening (now → 2026-Q3)
- Historize ISO queue snapshots (daily, all 7 ISOs)
- Build DC transaction comp database (manual seed of 200 deals)
- Build hyperscaler shell-LLC entity-resolution layer (Meta, Microsoft, Google, AWS, Oracle, ByteDance — 6 graphs)
- Stabilize existing Site Intel layer for DC use

### Phase 1 — DCII MVP (2026-Q4)
- Workflows 1–3 (Sourcing Radar, Diligence Pack, Portfolio Monitor)
- Web app, alerts, API
- 3 design partners
- **Phase 1 ARR pencil:** 3 design partners × ~$200K avg ACV (Team tier at the 50% design-partner discount) × ~80% close on outbound pipeline of 8 → ~$500K contracted ARR exiting Phase 1. Real ARR realization lags contracts by 1–2 quarters.

### Phase 2 — Investor depth (2027-Q1 → Q2)
- Workflows 4–6 (Market Supply, Tenant Intel, Comps)
- AI-demand model v1 (workflow 7)
- 5+ design partners
- Quarterly market report

### Phase 3 — Enterprise (2027-Q3)
- Governed-warehouse SKU
- MCP server: shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11. Ship the DCII surface (datacenter operators, sites, queue, tenants, comps) on the unified MCP at the same time as the other three product surfaces; do not stand up a DCII-specific MCP.
- SSO + RBAC
- Operator-side product (sell to QTS/Aligned/etc.)

### Phase 4 — Adjacent expansion (2027-Q4+)
- Debt-fund SKU (covenant monitoring, lease-rollover models)
- LP-advisor SKU (sector exposure, GP underwriting)
- International expansion (Europe DC market, APAC)

---

## 11. Risks & Open Questions

| # | Risk / Question | Mitigation |
|---|---|---|
| R1 | **DigitalBridge / Stonepeak (and similar Tier-A buyers) may build internal** — this is the #1 strategic risk for DCII because Tier-A *is* the beachhead. They have the budget and the talent. | Position as below-the-model warehouse: sell governed data + signal feeds, not the model itself. Lean into bundle leverage — DCII + IRADev + Underwrite together is data they cannot internally replicate cost-effectively across three asset classes. Tight design-partner contracts (3yr) before they greenlight an internal build. |
| R2 | Tenant inference accuracy is variable | Confidence-tier outputs; never publish low-conf as fact; reuse existing provenance system |
| R3 | Hyperscaler cap-ex tracking depends on continued public disclosure quality | Build redundant signals (permits, queue filings, LLC graph) so any single disclosure channel going dark doesn't break the AI-demand index |
| R4 | Synergy / 451 may sue over scraped market data | Use only public sources; IP review before launch |
| R5 | Comp database is hand-built and degrades | Allocate 1 ops FTE to maintain |
| Q1 | Do we need our own tenant survey to corroborate inference? | Decide post-MVP based on customer feedback |
| Q2 | Should we build a financial model SKU (like Argus for DCs)? | No, out of scope; partner with existing model vendor |
| Q3 | Should we serve operators (QTS, Aligned, etc.) directly? | Yes, but as Phase 3 — investor-side first |
| Q4 | Should the AI-demand model be a separate SKU? | Bundle initially; spin out if pull is high |

---

## 12. What Success Looks Like

A digital-infra PE associate at Stonepeak opens DCII in the morning. There are 7 new sourcing alerts: a new permit at a Vantage-affiliated LLC in Atlanta, a leadership change at Compass, three new ISO queue applications in Phoenix, and a Meta shell-LLC land acquisition in Indiana. They click a Phoenix opportunity, get a full diligence pack — power, fiber, comps, tenants, governance — in 30 seconds. They drag it into the IC deck, with audit-trail-attached numbers. They flag the asset to their partner. The associate did 4 hours of analyst work in 7 minutes.

That is the product.

---

## 13. Sources / Internal References

- `app/sources/site_intel/` — existing Site Intel collectors
- `app/sources/iso/` — ISO interconnect queue ingestors
- `app/sources/eia/`, `app/sources/ferc/` — energy data
- `app/services/scoring/` — site scoring engines
- `app/core/people_models.py` — org chart
- `app/core/pe_models.py` — deal probability + transactions
- `app/reports/templates/` — report framework
- `docs/strategy/NEXDATA_SELLABLE_CAPABILITIES_2026.md` — sales-doc baseline (capability #6)
- `docs/strategy/PERPLEXITY_COMPUTER_COMPARISON_2026.md` — competitive framing
