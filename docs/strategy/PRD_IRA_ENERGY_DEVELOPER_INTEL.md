# PRD — IRA-Energy Developer Intelligence (IRADev)

**Status:** Draft v1
**Date:** 2026-04-30
**Owner:** Product
**Companion docs:** `PRD_DATACENTER_INVESTOR_INTEL.md` (sister product, shared infra), `NEXDATA_SELLABLE_CAPABILITIES_2026.md`

> **See also:** [`PLATFORM_POSITIONING_2026.md`](./PLATFORM_POSITIONING_2026.md) — canonical roll-up across the four vertical PRDs (DCII, IRADev, Underwrite, LitInt). Owns cross-product sequencing (Underwrite → DCII → IRADev → LitInt), the shared 12-month Phase 0 program, bundle pricing, the single MCP investment, and the platform-reconciled Y2 ARR aggregate. **IRADev is sequenced third** as a ~70% re-skin of DCII; cross-sell into existing DCII PE accounts is the primary GTM motion.

---

## 1. Executive Summary

The Inflation Reduction Act (2022) and its successors triggered the largest US infrastructure capital wave since the interstate highway system. **$1T+ of utility-scale energy capital** is currently chasing solar, wind, storage (BESS), green-hydrogen, and EV-charging projects — with site selection, tax-credit eligibility, interconnect queue position, and prevailing-wage compliance dictating which projects pencil and which die.

The developers and capital sponsors deploying this capital today rely on **fragmented data**: hand-pulled ISO interconnect queues, county zoning PDFs, IRS energy-community maps, BLS labor data, transmission-line shapefiles, off-take buyer rumors, and a stack of consultants. The pain is identical to the DC space — and the **tooling stack is identical too**. We have already built ~95% of the data plumbing for the DC Site Suitability product. IRADev is a vertical re-skin onto a different buyer.

**Target outcome:** $100K–$500K ACV per developer or sponsor. 6–10 design partners in 2026-H2. $3–6M ARR by end of 2027 *(standalone target — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled Y2 aggregate of $10.5–16.5M across all four products)*.

---

## 2. Target Buyers (ICP)

| Tier | Segment | Examples | Pain | ACV |
|---|---|---|---|---|
| **A** | Utility-scale solar + wind developers | Invenergy, Pattern, Avangrid, EDF Renewables, Lightsource bp, AES Clean Energy, Clearway, NextEra Energy Resources, Cypress Creek | Site sourcing, IRA credit stacking, queue-position management, off-take buyer intel | $250–500K |
| **A** | Utility-scale BESS developers | Plus Power, Hydrostor, esVolta, Available Power, Strata Clean Energy | BESS-specific siting (peak load, ancillary services markets), revenue stacking | $200–400K |
| **A** | IRA tax-credit advisory + transferability platforms | Crux, Reunion, Basis Climate, Evergrow, Bluerock | Project-eligibility verification, energy-community attestation, prevailing-wage | $150–350K |
| **B** | EV-charging operators | EVgo, ChargePoint, Electrify America, Ionna, Tesla Supercharger network ops | Site density, utility coordination, NEVI corridor compliance | $75–200K |
| **B** | Green hydrogen + clean fuels developers | Plug Power, Air Products, Bloom, Monolith, Nikola | Hub eligibility, transmission, water, off-take | $150–400K |
| **B** | Renewable infra PE / IPP capital | Brookfield Renewable, BlackRock GIP Renewables, Ares Infra, Quinbrook, Generate Capital, Energy Capital Partners | Sourcing, queue intel, comp data, exit modeling | $250–750K |
| **B** | Utility IPP / IOU corporate development | Duke, Southern, Xcel, NextEra | Buy-vs-build, queue position, M&A targeting | $200–500K |
| **C** | Off-takers (corporate PPA buyers) | Microsoft, Meta, Google, Amazon, Walmart, GM | Project diligence, basis risk, IRA delivery requirements | $100–300K |
| **C** | EPC / construction firms | Mortenson, Blattner, MasTec, Sterling Infrastructure | Project pipeline visibility, owner intel, labor availability | $100–250K |

**Primary beachhead:** Tier-A solar + storage developers (Invenergy, Pattern, AES, Lightsource bp, Clearway). 4–6 month sales cycle with urgent project-by-project workflows. Direct ARR.

**Secondary beachhead (partner-led):** IRA tax-credit transferability platforms (Crux, Reunion, Basis Climate). Different motion — distribution partnership where IRADev data feeds inside their platform reaches 500+ small developers + tax-equity buyers. Lower direct ACV, much higher reach. Run as a separate workstream so it doesn't gate the developer-direct beachhead.

---

## 3. Problem Statement

A solar-developer associate building a 200MW project today does the following manually:

1. **Site selection.** Pull county GIS, check FEMA wetlands, USGS slope, NWI, parcel zoning, and proximity to a 230kV+ transmission line (FERC + utility maps). 60–120 hours per site, 10 sites per project = thousands of hours.
2. **Interconnect queue.** Manually scrape PJM/MISO/SPP/ERCOT/etc. queue dashboards. Submit cost-network upgrade studies. Track competitors in queue. Watch queue position deteriorate from re-studies.
3. **IRA credit eligibility.** Verify energy-community status (closed-coal-plant + brownfield maps from Treasury/IRS), low-income community designation, prevailing-wage tracking, domestic-content sourcing. Each is a different government dataset, sometimes a PDF.
4. **Off-take buyer intel.** Who's buying utility-scale PPAs in this region? At what price? Today: news + relationships.
5. **Permitting risk.** State / county AHJ knowledge, PUC dockets, opposition history, environmental-review status. Local newspapers.
6. **Labor availability.** Davis-Bacon prevailing-wage rates, apprenticeship-program availability, BLS QCEW employment density. Multiple sources.
7. **Competitor pipeline.** Who else is building in this market? At what stage?

Each of these is a separate analyst workflow. None are integrated. None are reproducible. Most teams have no historical store — when ERCOT re-studies a queue position, they find out from the operator. **The fragmentation is the problem.**

### 3.1 Policy context (2025–26) — non-trivial

The IRA stack circa 2024 was relatively stable: PTC/ITC base + bonus adders, transferability under §6418, direct pay for tax-exempt entities. The **2025 One Big Beautiful Bill (OBBB)** materially altered this. Any IRADev product must encode the post-OBBB regime, not the 2024 one:

- **§48E (clean electricity ITC) and §45Y (clean electricity PTC) phaseout schedules** were compressed for solar and wind in the OBBB. Storage retains a longer runway. Project IRR sensitivity to construction-start dates is now an order of magnitude tighter than under the original IRA.
- **Transferability under §6418** survived but with tighter restrictions on foreign-entity-of-concern (FEOC) sourcing, narrower buyer pool eligibility, and adjusted discount/yield expectations. Crux/Reunion's spreads moved.
- **Domestic-content adder** thresholds were raised; FEOC supply-chain attestation is now load-bearing for credit qualification, not a nice-to-have.
- **Energy-community designations** (closed-coal-plant + brownfield) — basis maps unchanged in 2025–26 but interpretive guidance from Treasury continues to evolve. Project eligibility is a moving target.

IRADev must (a) version every credit-stack output against the regulatory regime as of the calculation date, (b) provide a "what-if regime change" sensitivity layer, and (c) refresh shapefiles and Treasury guidance daily — not on a quarterly tracker.

---

## 4. Product Overview

IRADev mirrors DCII's four-layer architecture, with energy-vertical data fusion:

```
┌─────────────────────────────────────────────────────┐
│ Layer 4 — Developer + sponsor workflows             │
│ Site sourcing · IRA-credit stack · Queue monitor    │
│ Off-take intel · Comp engine · Pipeline tracker     │
├─────────────────────────────────────────────────────┤
│ Layer 3 — Energy-specific intel layer               │
│ Interconnect queue · IRA eligibility · PPAs         │
│ Transmission · Resource (NREL) · Permitting         │
├─────────────────────────────────────────────────────┤
│ Layer 2 — Site fusion (shared with DCII)            │
│ Power · Land · Risk · Labor · Incentives            │
├─────────────────────────────────────────────────────┤
│ Layer 1 — Existing Nexdata Site Intel + governance  │
└─────────────────────────────────────────────────────┘
```

### 4.1 The seven workflows the product supports

| # | Workflow | Persona | What they do today | What IRADev gives them |
|---|---|---|---|---|
| 1 | **Site sourcing** | Origination team | County-by-county manual scraping | National solar/wind/storage parcel scorer with IRA stack baked in |
| 2 | **Site diligence** | Project dev | 60–120 hrs/site | One-click resource + queue + IRA + risk + permitting + labor |
| 3 | **Queue monitoring** | Asset manager | Manual ISO portal checks | Live queue tracker per project + competitor positions + re-study alerts |
| 4 | **IRA credit stacking** | Tax / structured finance | IRS PDFs + spreadsheets | Per-parcel: energy community, low-income, prevailing wage, domestic content |
| 5 | **Off-take / PPA intel** | Origination + finance | News + relationships | PPA price tracker by region, off-taker activity feed, RFP scraping |
| 6 | **Comp engine** | Investment committee | Press release math | $/W, $/MWh, $/MW-storage, IRR by region/tier/vintage |
| 7 | **Competitive pipeline** | Strategy | Industry decks | Live by-metro pipeline by stage |

### 4.2 Output formats

- Web app (primary)
- Sourcing alerts (email/Slack/webhook)
- Project Diligence Pack (PDF + XLSX, 1-click)
- Quarterly IRA-Eligibility Report (per state, branded)
- API + GraphQL
- MCP server

---

## 5. Feature Requirements

### 5.1 Site Sourcing & Scoring

| ID | Feature | Priority | Notes |
|---|---|---|---|
| SS-1 | Solar-suitability score per parcel (NREL irradiance, slope, soil, land use) | M | Reuse Site Intel |
| SS-2 | Wind-suitability score per parcel (NREL wind atlas, FAA hazard, military test ranges) | M | |
| SS-3 | BESS-suitability (load center, ancillary services market, peak demand correlation, **frequency-regulation market access, capacity-market product eligibility (PJM RPM, NYISO ICAP), hybrid PPA viability, ITC stacking with co-located solar, ancillary-services revenue stack**) | M | New. BESS revenue is multi-stream — siting must score *all* the markets the asset can stack, not just energy arbitrage. |
| SS-4 | Solar+storage hybrid score | M | Composite |
| SS-5 | Parcel-level title / ownership / acreage data | S | Partner with parcel data provider (Regrid) |
| SS-6 | Saved-search alerts on new parcels meeting thesis | M | |
| SS-7 | Greenfield vs. repower opportunity flag | S | Cross-reference with existing PJM/CAISO retiring assets |

### 5.2 Project Diligence Pack

| ID | Feature | Priority | Notes |
|---|---|---|---|
| PD-1 | Resource model: irradiance, wind speed, capacity factor projection | M | NREL data + analytical layer |
| PD-2 | Transmission proximity: kV, owner, substation distance, hosting capacity | M | FERC + utility data |
| PD-3 | Interconnect-queue context: same-substation queue position, study status | M | |
| PD-4 | Land-use diligence: zoning, easements, federal land status | M | |
| PD-5 | Environmental: wetlands (NWI), endangered species (USFWS), historic (NPS NRHP), sage grouse, Section 106 risk | M | |
| PD-6 | Labor: BLS QCEW, prevailing-wage rates, apprentice availability | M | Existing |
| PD-7 | Permitting: state/county AHJ history, opposition score (county news sentiment) | S | News + zoning |
| PD-8 | Tax/Incentives: IRA stack, state programs, county PILOT-eligibility | M | |
| PD-9 | Generate one-click PDF + XLSX | M | |

### 5.3 Interconnect Queue Monitoring

| ID | Feature | Priority | Notes |
|---|---|---|---|
| IQ-1 | Daily queue snapshots, all 7 ISOs + non-ISO regions (TVA, BPA, etc.) | M | Reuse from DCII |
| IQ-2 | Per-project queue-position delta tracking | M | |
| IQ-3 | Competitor position visibility ("who else is in front of me at this substation") | M | |
| IQ-4 | Re-study event alerts | M | |
| IQ-5 | Cluster-study tracking + cost network upgrade allocation | S | Highly sensitive — requires careful data acquisition |
| IQ-6 | Withdrawal-rate analysis (% projects that withdraw at each stage, by ISO) | S | |

### 5.4 IRA Credit Stacking Engine

| ID | Feature | Priority | Notes |
|---|---|---|---|
| IR-1 | Energy-community eligibility per parcel (Treasury/IRS shapefiles + statistical attestation) | M | Already public; need processing |
| IR-2 | Low-income community / Justice40 eligibility | M | |
| IR-3 | Prevailing-wage tracking (Davis-Bacon by county/job class) | M | |
| IR-4 | Apprenticeship-program availability (DOL data) | S | |
| IR-5 | Domestic-content compliance signal (vendor sourcing, manufacturer Section 232 data) | S | |
| IR-6 | Total credit-stack estimator (% ITC / $ PTC) per parcel | M | Composite |
| IR-7 | Adder eligibility report exportable for capital partners | M | |

### 5.5 Off-take / PPA Intelligence

| ID | Feature | Priority | Notes |
|---|---|---|---|
| OT-1 | PPA price tracker: region × tech × tenor × **type (physical PPA vs. virtual PPA / VPPA)** (anonymized aggregates) | M | LevelTen-style — built natively. **Distinguish physical PPA (delivered MWh, basis-risk, RTO settlement) from VPPA (financial swap, Scope 2 attestation, no physical delivery). Different counterparty universe (utilities/IPPs for physical; corporates for VPPA), different pricing curve, different IRA-eligibility considerations.** |
| OT-2 | Off-taker activity feed (corporate PPAs, utility RFPs) | M | News + state PUC dockets. Tag each off-take as physical vs. VPPA. |
| OT-3 | Utility RFP scraper (state IRP filings, RFP postings) | M | |
| OT-4 | Off-taker buying-history graph | S | |
| OT-5 | RFI / RFP deadline tracker | M | |

### 5.6 Comp & Capital Engine

| ID | Feature | Priority | Notes |
|---|---|---|---|
| CC-1 | Project transaction comp database ($/W solar, $/MW wind, $/MWh BESS, IRR ranges) | M | Hand-seeded from press + filings |
| CC-2 | Tax-credit transfer-market pricing tracker (Crux, Reunion partner data?) | S | Possible partnership |
| CC-3 | Construction-cost benchmarking by region | S | EPC partner / survey |
| CC-4 | Sponsor-equity / debt-multiple comps | S | |

### 5.7 Competitive Pipeline Tracker

| ID | Feature | Priority | Notes |
|---|---|---|---|
| CP-1 | Project pipeline by metro × stage (announced, permitted, queue, financed, COD) | M | |
| CP-2 | Sponsor-attribution (who owns each project) with shell-LLC resolution | M | |
| CP-3 | Capital-stack inference (sponsor, lender, tax-equity) | S | |
| CP-4 | Withdrawal / cancellation alerts | M | |

---

## 6. Data Sources

### Already built (reuse)
- ISO interconnect queues (PJM, CAISO, ERCOT, MISO, SPP, NYISO, ISONE) — daily snapshots
- EIA, FERC (transmission, RTO data)
- BLS QCEW, BPS, Census
- FEMA, USGS, NWI, NPS
- State EDO incentive data
- SEC EDGAR (for public sponsors)
- Org chart + people stack
- News + press release pipeline

### New to build for IRADev
- **NREL solar resource + wind resource overlays** — irradiance, wind speed, hub-height adjustment
- **Energy-community / Justice40 shapefile processing** — Treasury IRS data
- **Davis-Bacon prevailing-wage by county × job class**
- **Hosting-capacity maps** per utility (where available)
- **State PUC docket scraper** for utility IRPs and RFPs
- **PPA price tracking layer** — aggregate from public filings + REIT disclosures + corp announcements
- **Project transaction comp database** — manual + ongoing
- **TVA / BPA / non-ISO queue ingestion** (gap)
- **Parcel data partnership** (Regrid, ATTOM, or similar) — paid

### Possible partner / license
- **LevelTen Energy** for PPA data corroboration (or potentially compete)
- **Crux / Reunion** for tax-credit transfer-market integration
- **Wood Mackenzie / S&P Platts** for capacity-factor benchmarking

---

## 7. Why Perplexity / Computer Cannot Do This

1. **Energy-community + Justice40 maps** are released as government shapefiles. A general agent doesn't process these natively.
2. **Live ISO queue tracking** — daily delta is the signal; agent has no historical store.
3. **PPA price intelligence** is private + inferred. Requires multi-source triangulation, not a search.
4. **Davis-Bacon prevailing wage** is per county × job class × determination date. A general agent will guess.
5. **Project-comp accuracy** requires a curated, hand-cleaned comp set. LLM-only output averages press-release noise.
6. **Reproducibility for capital-partner reporting** — IRA tax credits require defensible attestation. We provide audit trails; Computer cannot.

---

## 8. Differentiation vs. Existing Players

| Competitor | What they do | Where IRADev wins |
|---|---|---|
| **LevelTen Energy** | PPA marketplace + price index | We're upstream — we tell you *where* to build. They sell the PPA. Could be partner. |
| **Anza Renewables** | Solar component procurement | Adjacent, not competitive. |
| **Wood Mackenzie / S&P** | Industry research | Quarterly PDFs vs. live warehouse. |
| **EnergyToolbase** | Solar+storage finance modeling | Below the model — they keep their model, we feed it data. |
| **Crux / Reunion** | Tax-credit transferability | Adjacent — partnership candidate. |
| **In-house teams at developers** | Custom analyst stacks | Replace the stack with one platform; analysts move to higher-value work. |

**Strategic posture:** Same as DCII — *be the data layer below the model*. Don't try to build the financial model.

---

## 9. GTM

### Beachhead motion
- 10 named-account outbound at Tier-A developers (Invenergy, Pattern, AES, Lightsource, Clearway)
- 3 named-account outbound at IRA-credit transfer platforms (Crux, Reunion, Basis Climate) — they need this data and can co-sell to their entire customer base
- Demo: a real candidate site → resource model + queue + IRA stack + comps in 60 seconds

### Channel plays
- **CleanPower (ACP) annual conference** — biggest US renewables conference, every developer is there
- **RE+ / Solar Power International** — go deep on solar ICP
- **State-by-state IRA stacking reports** — free, lead-gen
- **Crux / Reunion partnership** — a Nexdata data feed inside their platform = instant distribution to 500+ small developers + tax-equity buyers
- **DOE / NREL adjacency** — be cited in NREL reports; that's status with this buyer

### Pricing
| Tier | ACV | Includes |
|---|---|---|
| **Project** | $100K | 5 seats, web app, diligence packs (50/yr), queue tracker for owned projects |
| **Developer** | $300K | 25 seats, all of Project + IRA stacking + PPA intel + comp database + alerts |
| **Enterprise** | $500K–$1M | Unlimited seats, all of Developer + governed warehouse, lineage/audit, MCP, SSO, dedicated CSM, custom feeds |

### Success metrics
- 6 design partners by 2026-Q4
- $2M ARR by 2027-Q1
- $5–6M ARR by 2027-Q4 *(standalone — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled aggregate)*

### 9.5 Cross-sell to other Nexdata products

IRADev's beachhead and adjacent ICP overlap heavily with DCII (sister product) and partially with Underwrite. Per `PLATFORM_POSITIONING_2026.md` §6, multi-product MSAs price at ~25% off for two products and ~30% for three+. IRADev is sequenced specifically to harvest the DCII account base.

| IRADev account | Also fits | Why |
|---|---|---|
| Brookfield Renewable, BlackRock GIP Renewables, Quinbrook, Ares Infra, Generate Capital, Energy Capital Partners | **DCII** | Renewable-infra PE firms underwrite DC power-supply assets too; the DCII queue + AI-demand-modeling layer is a natural extension. Highest-likelihood 2-product bundle (often the same investment committee). |
| Microsoft, Meta, Google, Amazon (corporate PPA buyers) | **DCII**, **Underwrite** | Hyperscalers buy at-scale renewable PPAs (IRADev), own DC assets (DCII), and run captive-insurance entities for tower-and-asset risk (Underwrite). Three-product bundle plausible. |
| NextEra, Duke, Southern, Xcel (utility IPP / IOU corp dev) | **Underwrite** | Utilities carry significant property & liability exposure; Underwrite parametric-weather peril modeling is adjacent to their own portfolio risk function. Lower probability bundle. |
| Brookfield Infra, Stonepeak, KKR Infra (when they hold both DC and renewable assets) | **DCII**, possibly **LitInt** | Same Tier-A digital-infra PE firms in the DCII account list. Most likely Brookfield-style 3-product MSA candidates ($1.1–2.0M ACV — see positioning doc §6) |

**Action:** When DCII Phase 1 closes in 2026-Q4, IRADev BD inherits the DCII PE-firm account list as the primary outbound starting set. Don't restart sourcing from cold for these accounts — DCII pod hands over relationships at close.

---

## 10. Phasing

### Phase 0 — Foundation (now → 2026-Q3)
- NREL solar + wind resource integration
- Energy-community / Justice40 shapefile processing
- TVA / BPA queue ingestion (close ISO gap)
- Davis-Bacon prevailing-wage data layer

### Phase 1 — IRADev MVP (2026-Q4)
- Site sourcing + diligence pack workflows
- Queue monitoring (reuse from DCII)
- IRA credit-stack engine v1
- 3 design partners

### Phase 2 — Capital workflows (2027-Q1 → Q2)
- Off-take / PPA intel
- Project comp database
- Competitive pipeline tracker
- 5+ design partners

### Phase 3 — Enterprise + partnerships (2027-Q3)
- Governed warehouse SKU
- Crux/Reunion partnership integration
- MCP server: shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11. Ship the IRADev MCP surface (parcels, queue, IRA stack, PPA, pipeline) on the unified MCP at the same time as the other three product surfaces; do not stand up an IRADev-specific MCP.
- Tax-credit transfer-market pricing layer

### Phase 4 — Adjacent expansion (2027-Q4+)
- Off-taker SKU (Microsoft / Meta / Google / Amazon — they're already DCII customers)
- EPC / construction-firm SKU
- International (Canada, EU when policy aligns)

---

## 11. Risks & Open Questions

| # | Risk / Question | Mitigation |
|---|---|---|
| R1 | IRA policy reversal — already partially realized via the 2025 OBBB (compressed §48E/§45Y phaseouts, FEOC sourcing tightening, transferability buyer-pool changes). Further policy drift is the dominant macro risk. | Encode policy-version awareness into every credit-stack calculation (see §3.1). Diversify into state-level RPS/CES programs, EU/Canada expansion when policy aligns. The policy-versioning capability is itself a moat — tools built on a frozen 2024 IRA snapshot will silently mis-quote post-OBBB economics. |
| R2 | LevelTen + similar may bundle our data | Pre-emptive partnership; differentiate via parcel + queue + IRA stack depth |
| R3 | Parcel data licensing costs (Regrid) eat margin | Start with public-only data; negotiate volume |
| R4 | Davis-Bacon data is updated frequently; staleness risk | Daily refresh + versioning |
| Q1 | Is the off-take / PPA SKU too sensitive (utility / CIO data)? | Public sources only; aggregate-anonymized pricing |
| Q2 | Should we build an offshore-wind module? | Yes, Phase 4 |
| Q3 | Should we sell to off-takers (Microsoft, Meta, Google) — overlapping with DCII? | Yes — same logo, two motions, large incremental ACV |
| Q4 | Tax-credit transferability platform partnership: license vs. compete? | Partnership first; revisit if their data layer underperforms |

---

## 12. Shared Infra with DCII

DCII and IRADev share:
- Site Intel core
- ISO queue snapshots + historization
- Org chart pipeline
- Reports framework
- Governance / lineage / audit
- Unified MCP server (shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11; IRADev exposes its surface on the same MCP, not a product-specific one)

**~70% of the engineering is shared.** This is intentional. The vertical re-skin is in the scoring layer, the workflow UX, and the buyer-specific data (NREL for IRADev, PeeringDB for DCII).

---

## 13. What Success Looks Like

A development VP at Pattern Energy opens IRADev. There are 12 sourcing alerts: 4 new candidate parcels in West Texas with energy-community status, 3 PJM queue position changes on existing portfolio projects, 2 utility RFPs in MISO, and 3 Justice40 designation updates. They click a Texas parcel — get a full diligence pack with capacity factor, transmission proximity, queue context, full IRA stack (energy community + LIC + PWA), labor availability, and a $/W comp band, all in 45 seconds. They flag it for their VP of origination. The team did 80 hours of analyst work in 8 minutes.

That is the product.

---

## 14. Sources / Internal References

- `app/sources/iso/` — interconnect queue ingestors (shared with DCII)
- `app/sources/eia/`, `app/sources/ferc/` — energy infra
- `app/sources/bls/` — labor data
- `app/services/scoring/` — site scoring engines
- `docs/strategy/PRD_DATACENTER_INVESTOR_INTEL.md` — sister product
- `docs/strategy/NEXDATA_SELLABLE_CAPABILITIES_2026.md` — capability #6 origin
