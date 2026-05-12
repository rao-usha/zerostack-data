# PRD — Insurance Underwriting Intelligence (Underwrite)

**Status:** Draft v1
**Date:** 2026-04-30
**Owner:** Product
**Companion docs:** `NEXDATA_SELLABLE_CAPABILITIES_2026.md`

> **See also:** [`PLATFORM_POSITIONING_2026.md`](./PLATFORM_POSITIONING_2026.md) — canonical roll-up across the four vertical PRDs (DCII, IRADev, Underwrite, LitInt). Owns cross-product sequencing (Underwrite → DCII → IRADev → LitInt), the shared 12-month Phase 0 program, bundle pricing, the single MCP investment, and the platform-reconciled Y2 ARR aggregate. **Underwrite is sequenced first** — parametric-startup deals close in 3–6 months, the workload forces sub-second SLA muscle reusable across all four products, and reference-customer wins gate the longer-cycle carrier pipeline.

---

## 1. Executive Summary

Commercial Property & Casualty (P&C) is a **$1.4T US GWP market** (gross written premium, P&C; not policy count or asset value) with structurally analog underwriting. Carriers price location-specific risk against fragmented data (FEMA flood, USGS quake, EPA contamination, NWI wetlands, BLS labor concentration, EIA grid reliability, FCC fiber/comms — all of which Nexdata already ingests). Today they buy from CoreLogic, Verisk, RMS, ISO Verisk Analytics — old, expensive, per-record, and rarely fused below the city level.

**Underwrite** is a vertical re-skin of Nexdata's Site Intel + governance stack, packaged for **commercial P&C underwriters, parametric-insurance startups, MGAs, and reinsurers.** It delivers per-location risk fusion at parcel resolution, treaty-grade aggregation, and the audit trail regulators require — all of which Computer / general LLMs cannot provide.

**Target outcome:** $150K–$1M ACV per carrier, MGA, or reinsurer. 6 design partners in 2026-H2. $5–10M ARR by end of 2027 *(standalone target — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled Y2 aggregate of $10.5–16.5M across all four products)*.

---

## 2. Target Buyers (ICP)

| Tier | Segment | Examples | Pain | ACV |
|---|---|---|---|---|
| **A** | Parametric-insurance startups | Arbol, Skyline (acquired), Kettle, Setoo, Descartes Underwriting, Floodflash, Raincoat | Real-time risk-trigger data + reproducible attestation | $200–500K |
| **A** | Mid-market commercial carriers | Hanover, Cincinnati, Erie, FCCI, EMC, Westfield, Selective | Underwriting decision support, geo-risk concentration | $300–750K |
| **A** | MGAs / Lloyd's coverholders | Markel, Munich Re Specialty, Beazley, Vibrant, ICAT, Bowhead, Avant Underwriting | Submission triage + bind-or-decline data + audit | $200–600K |
| **B** | Reinsurers | Munich Re, Swiss Re, Hannover, Everest, RenaissanceRe, AXIS, PartnerRe | Treaty pricing, accumulation control, peril modeling corroboration | $500K–$1.5M |
| **B** | Catastrophe modeling firms | RMS (Moody's), AIR (Verisk), CoreLogic Hazard, ImageCat, Reask, Riskthinking | Underlying signal feeds, climate-scenario adjustment | $250–750K |
| **B** | Insurtech MGAs | Branch, Bestow, At-Bay (cyber+P&C), Cowbell, Coalition, Resilience | Underwriting automation data | $150–400K |
| **C** | Workers' comp / commercial auto | Travelers, Progressive Commercial, AmTrust | Industry / labor / fleet safety data | $200–500K |

**Beachhead:** Parametric-insurance startups (Arbol, Floodflash) — they have urgent data needs, fast decision cycles (3–6 months), fewer procurement gates, and are frequently early adopters of structured data. Wins here build the sub-second-API muscle the rest of the platform reuses.

**Reinsurers excluded from the beachhead** despite their high ACV ($500K–$1.5M, Tier B) — their procurement cycles run 12–18 months, which is incompatible with the Phase 1 goal of fast design-partner closes that exercise the sub-second SLA workload. Reinsurers re-enter the funnel in Phase 3 once the SLA is proven and reference customers exist.

---

## 3. Problem Statement

A commercial property underwriter pricing a $50M factory in Iowa today does the following:

1. Pulls FEMA flood zone (1 click — but only at zone-level, no return-period detail)
2. Buys a CoreLogic peril report ($30–80/location, days to retrieve, opaque methodology)
3. Manually checks EPA contamination (ACRES — separate site)
4. Pulls a wildfire risk score (varies by carrier; some buy CoreLogic, some Verisk)
5. Calls the local labor market (BLS QCEW — but rarely)
6. Checks for industrial-density risk (chemical neighbors, rail proximity) — informally
7. Re-types data into Guidewire / Duck Creek / proprietary policy admin
8. Gets pushback from reinsurance treaty rules — re-runs at the aggregate level

Each step is fragmented. **No one product fuses parcel-level peril + structural + economic + concentration data with a maintained audit trail.** Reinsurers run cat models on aggregate exposures but don't get parcel-level visibility into the underlying policies. Parametric-insurance startups have to build their own data layer to launch. MGAs auto-decline 60% of submissions on guesswork.

The opportunity is the same data Nexdata already collects — re-aggregated for the insurance buyer.

---

## 4. Product Overview

```
┌─────────────────────────────────────────────────────┐
│ Layer 4 — Underwriting workflows                    │
│ Submission triage · Account scoring · Treaty agg    │
│ Renewal monitor · Climate scenario · Audit / regs   │
├─────────────────────────────────────────────────────┤
│ Layer 3 — Insurance-specific layers                 │
│ Peril-specific feeds · Return-period curves         │
│ Concentration · Trigger feeds (parametric)          │
├─────────────────────────────────────────────────────┤
│ Layer 2 — Site fusion (shared with DCII / IRADev)   │
│ Risk · Power · Labor · Industry · Land              │
├─────────────────────────────────────────────────────┤
│ Layer 1 — Existing Nexdata + governance / audit     │
└─────────────────────────────────────────────────────┘
```

### 4.1 The six workflows the product supports

| # | Workflow | Persona | What they do today | What Underwrite gives them |
|---|---|---|---|---|
| 1 | **Submission triage** | UW assistants | Manual data lookups | Auto-scored submission with peril + concentration + audit |
| 2 | **Account scoring** | Underwriter | CoreLogic + manual | Parcel-level multi-peril fusion + structural risk |
| 3 | **Treaty aggregation** | Reinsurance treaty mgr | Aggregated portfolio runs in cat models | Parcel-level concentration + treaty-grade aggregation feed |
| 4 | **Renewal monitor** | Account team | Annual touch | Continuous re-scoring on new peril data, contamination, environmental |
| 5 | **Parametric-trigger feed** | Parametric-insurance ops | Custom-built per startup | Real-time multi-peril trigger data layer |
| 6 | **Climate-scenario / regulatory** | Risk officer / regulator | Outsourced consultancies | Per-portfolio climate-scenario rerun + reg-grade audit |

### 4.2 Output formats

- Web app (UW dashboard)
- API (high-throughput, sub-second latency for triage flows)
- Bulk batch (overnight portfolio reruns for treaty cycles)
- Bordereaux-format export (for Lloyd's MGAs)
- Parametric-trigger feed (real-time webhook)
- Audit / lineage report (regulatory)

---

## 5. Feature Requirements

### 5.1 Peril Layers (data depth is the moat)

| ID | Feature | Priority | Notes |
|---|---|---|---|
| PR-1 | Flood: 100/500-yr return period from FEMA + custom | M | Existing |
| PR-2 | Wildfire: WUI distance, fuel load, history | M | USGS + USDA Forest Service |
| PR-3 | Earthquake: USGS shake, fault distance, soil class | M | Existing |
| PR-4 | Wind / hurricane: NHC track history + return period | M | New ingest |
| PR-5 | Severe convective storm (hail/tornado): NOAA SPC + history | M | New ingest |
| PR-6 | Drought / water stress: USGS, Drought Monitor | S | |
| PR-7 | Subsidence / sinkhole: state geological surveys | S | |
| PR-8 | Industrial / contamination: EPA ACRES, EPA TRI, EPA Echo | M | Existing |
| PR-9 | Crime / theft: FBI UCR / NIBRS aggregated | S | |
| PR-10 | Climate-projection scenarios (RCP 4.5 / 8.5) | M | Partner with Jupiter / First Street or build native |

### 5.2 Submission Triage Engine

| ID | Feature | Priority | Notes |
|---|---|---|---|
| ST-1 | Address → unified peril score (0–100) per peril + composite | M | Composite index |
| ST-2 | Address → industry / occupancy classification (NAICS lookup + structural) | M | |
| ST-3 | Address → neighbor-risk score (rail proximity, chem facility neighbor) | M | |
| ST-4 | Address → 50-mile concentration with the carrier's existing book (requires customer policy data) | M | **Privacy-preserving via private set intersection (PSI) or salted-hash address join.** Carrier's policy-address book never leaves their VPC; carrier sends hashed/encrypted addresses, we return matching geo-bucket counts without ever seeing plaintext addresses. Implementation: choose between (a) PSI with elliptic-curve OPRF for highest privacy + complexity, or (b) salted-hash + bloom-filter for simpler ops with weaker guarantees. Decision in Phase 0 spike. |
| ST-5 | Auto-decline / auto-bind recommendation flag | S | Tunable per carrier |
| ST-6 | Bulk-API endpoint for inline submission flows | M | **<500ms p95 latency.** Architecture: pre-computed feature store (peril values per parcel pre-joined and indexed); Postgres BRIN/GIN indexes on geo + peril composite keys; cached carrier-portfolio overlays for ST-4 concentration calls; Redis-fronted hot tier for top-1% lookup volume. Sub-500ms p95 requires zero live external joins on the hot path — all peril data must be pre-fused at ingest time, never pulled from FEMA/USGS at submission time. |

### 5.3 Account Scoring

| ID | Feature | Priority | Notes |
|---|---|---|---|
| AS-1 | Parcel-level structural feature inference (roof type, age, sqft) | M | Partner — possibly Cape Analytics |
| AS-2 | Peril fusion model with carrier-specific weights | M | Configurable |
| AS-3 | Per-account loss-history reconciliation | S | Carrier provides; we attribute risk |
| AS-4 | Modeled AAL / 1-in-100-yr / 1-in-250-yr by peril | M | |
| AS-5 | Recommended limits / deductibles output | C | Out of scope for Phase 1 |

### 5.4 Treaty Aggregation

| ID | Feature | Priority | Notes |
|---|---|---|---|
| TA-1 | Carrier-portfolio bulk ingest (Excel / CSV / API) | M | |
| TA-2 | Parcel-level reroll of all locations against current peril data | M | Overnight batch |
| TA-3 | Treaty-grade aggregation: by zone, by peril, by return period | M | |
| TA-4 | Concentration-cluster detection (e.g., 50+ accounts within 1 mile) | M | |
| TA-5 | What-if scenarios (new accounts, new perils, new climate) | S | |

### 5.5 Renewal Monitor

| ID | Feature | Priority | Notes |
|---|---|---|---|
| RM-1 | Continuous re-scoring on new peril data (ACRES update, fire boundary expansion) | M | Webhook alerts |
| RM-2 | Renewal alerts triggered by score-delta thresholds | M | |
| RM-3 | Peril-specific newsfeed for portfolios | S | |

### 5.6 Parametric Trigger Layer

| ID | Feature | Priority | Notes |
|---|---|---|---|
| PT-1 | Real-time NOAA storm + earthquake + flood triggers | M | High-availability feed |
| PT-2 | Configurable trigger logic per policy (radius, intensity, duration) | M | |
| PT-3 | Attestation report on trigger events (regulatory + payer audit) | M | Audit trail = the product |
| PT-4 | Historical replay for back-testing parametric policies | M | |

### 5.7 Climate Scenario / Regulatory

| ID | Feature | Priority | Notes |
|---|---|---|---|
| CS-1 | RCP 4.5 / 8.5 portfolio rerun | M | |
| CS-2 | NAIC climate-disclosure-aligned reporting | M | |
| CS-3 | Per-state regulatory-format export | S | State-by-state UW data calls |
| CS-4 | Lloyd's bordereaux + MRC alignment | M | |

---

## 6. Data Sources

### Already built (reuse)
- FEMA flood, USGS earthquake, NWI wetlands, EPA ACRES, EPA TRI, EPA Echo
- Census BPS, BLS QCEW
- EIA, FERC (grid)
- FCC (broadband / comms)
- BTS (transport / rail proximity)
- USACE (waterways)
- Existing scoring engines + Site Intel layer
- Governance / lineage / audit stack

### New to build for Underwrite
- **Wildfire**: USDA Forest Service Wildfire Hazard Potential, WUI shapefiles, fire-history
- **Wind / hurricane**: NHC HURDAT, return-period curves
- **Severe convective storm**: NOAA SPC tornado/hail history
- **Drought / water stress**: US Drought Monitor, USGS drought
- **Climate-projection scenarios**: NOAA / NCAR downscaled climate
- **Real-time trigger feed**: NOAA WX alerts, USGS earthquake feed, river-gauge feed
- **Industry / occupancy classification engine** (NAICS + structural inference)
- **Bordereaux export module** (Lloyd's specific)
- **NAIC reporting templates**

### Possible partner / license
- **Cape Analytics / Arturo** — imagery-based structural feature inference
- **Jupiter Intelligence / First Street** — climate scenario modeling (or build)
- **Verisk / CoreLogic** — could explore data partnership rather than head-on

---

## 7. Why Perplexity / Computer Cannot Do This

1. **Sub-second latency.** Submission-triage flows need <500ms responses. A general agent cannot.
2. **Treaty aggregation.** Requires processing portfolios of 100K+ accounts in a single batch. Out of agent scope.
3. **Audit / regulatory grade.** State insurance departments require defensible data lineage. Computer cannot produce.
4. **Real-time trigger feeds.** Parametric policies pay out on trigger events; can't run through an LLM round-trip.
5. **Reproducibility.** Two underwriters running the same submission must get the same answer. Computer is non-deterministic.
6. **Data depth.** EPA ACRES, FEMA NFHL, NOAA SPC — these are not "browse the web" data sources. They require ETL.

---

## 8. Differentiation vs. Existing Players

| Competitor | What they do | Where Underwrite wins |
|---|---|---|
| **Verisk (ISO + AIR)** | Catastrophe modeling + UW data | Per-record pricing; closed ecosystem; no audit at transaction |
| **CoreLogic Spatial Solutions** | Property data + peril | Property-residential focused; commercial coverage thin; per-record cost |
| **RMS (Moody's)** | Cat modeling | Aggregate-level only; not parcel-level transactional UW |
| **ZestyAI** | AI underwriting | Imagery-first; less peril fusion |
| **Cape Analytics / Arturo** | Imagery-derived features | Adjacent, partnership candidate, not competitive |
| **Jupiter / First Street** | Climate risk models | Climate-only; we fuse climate with everything else |
| **Internal carrier teams** | Custom-built UW data | Replace the pipeline plumbing; carrier keeps its model on top |

**Strategic posture:** Sell **the fused data layer + governance**, not the cat model. Position as "the data plumbing every modern UW org needs" — not "another cat vendor."

---

## 9. GTM

### Beachhead motion
- Target 4 parametric-insurance startups (Arbol, Floodflash, Kettle, Descartes Underwriting). Fast cycles, technical buyers, urgent need.
- Target 4 mid-market commercial carriers (Cincinnati, Hanover, Erie, FCCI). High-pain UW workflows.
- Target 2 MGAs (Markel-style + an insurtech). Submission triage urgent need.

### Channel plays
- **InsurTech NY / InsurTech Connect** — the major industry conferences. Demo onsite.
- **NAIC / NAMIC channel** — these are the regulators + trade groups. Become the structured-data reference.
- **Reinsurance broker partnerships** — Aon, Guy Carpenter, Howden — they place treaties and recommend tools.
- **Cape Analytics partnership** — they have imagery, we have public data fusion. Joint sale.
- **Lloyd's "Future at Lloyd's" program** — curated channel for new tech.

### Pricing
| Tier | ACV | Includes |
|---|---|---|
| **MGA / Insurtech** | $150K | API access (250K/yr lookups), submission-triage engine, audit |
| **Carrier** | $400–750K | Treaty aggregation, account scoring, bulk API, climate scenario, MGA features |
| **Reinsurer** | $750K–$1.5M | All Carrier + climate scenarios + regulatory reporting + dedicated CSM + custom feeds |

### Success metrics
- 6 design partners by 2026-Q4 (target: 3 parametric, 2 carriers, 1 MGA)
- $3M ARR by 2027-Q1
- $7–10M ARR by 2027-Q4 *(standalone — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled aggregate)*
- 1 reinsurer landed by 2027-Q3

### 9.5 FL/CA market depth (top single-state P&C exposures)

Florida and California together carry a disproportionate share of US P&C exposure and have the most regulatory-distinctive operating models. Underwrite's commercial-property and parametric-property products must encode each state's specifics or sales conversations stall:

**Florida (hurricane / windstorm)**
- **Citizens Property Insurance** is the state-backed insurer of last resort and one of the largest property writers in Florida by exposure. Citizens depopulation programs (where private carriers take over Citizens policies) are an active Underwrite use case — the take-over carrier needs per-parcel re-underwriting on a portfolio overnight.
- **Hurricane deductible** structure is mandatory and non-trivial — separate hurricane vs. AOP deductibles, percentage-based, named-storm vs. tropical-storm triggers. Per-policy deductible eligibility is a triage-engine output.
- **Reinsurance market dependency**: Florida specialty carriers buy heavy reinsurance; Demotech ratings are load-bearing for solvency. Treaty aggregation outputs must align with Demotech / FLOIR conventions.
- **Assignment-of-Benefits (AOB)** litigation history — claim-fraud heatmaps by ZIP are a real input.

**California (wildfire / earthquake)**
- **California FAIR Plan** is the state-backed pool, increasingly load-bearing as private carriers withdraw from wildfire-exposed ZIP codes. FAIR Plan exposure data is part of the WUI underwriting picture.
- **Proposition 103** locks in prior-approval rate filings — carriers can't reflect catastrophe model output in rates without explicit Department of Insurance approval. Underwriting workflows in CA must produce reg-grade attestation that's defensible at a 103 rate hearing.
- **Wildfire mitigation discount mandates** (SB 824, Safer from Wildfires) — per-parcel mitigation feature inference is mandatory output, not nice-to-have.
- **Earthquake**: separate-policy peril; CEA (California Earthquake Authority) is the participating insurer pool. Quake-specific outputs are a separate SKU lane.

**Why this matters for product:** state-specific output formats (Florida hurricane deductible eligibility, CA Prop 103 attestation) ship in Phase 1 as part of the triage engine, not deferred. Carriers won't run a pilot on a tool that can't produce its required state outputs.

### 9.6 Cross-sell to other Nexdata products

Underwrite is the platform's first product in market — its design-partner accounts are a direct input to the DCII / IRADev / LitInt sales pipelines. Per `PLATFORM_POSITIONING_2026.md` §6, multi-product MSAs price at ~25% off for two products and ~30% for three+.

| Underwrite account | Also fits | Why |
|---|---|---|
| Microsoft, Meta, Google, Amazon, Walmart, GM (corporate captive insurance) | **DCII** (hyperscalers), **IRADev** (corporate PPA buyers) | Hyperscalers run captive insurance entities for property and tower-and-asset risk; same legal entities buy renewable PPAs and own DC assets. Three-product bundle plausible. |
| Munich Re, Swiss Re, RenaissanceRe (reinsurers) | **DCII** (asset-class exposure), **LitInt** (mass-tort accumulation modeling) | Reinsurers carry portfolio-level exposure to data-center asset class and to mass-tort accumulation. Adjacent buyer with shared analytical workflow. |
| Cincinnati, Hanover, Erie, Selective (mid-market commercial carriers) | **LitInt** (defense-side liability triage — Phase 3 governance permitting) | Commercial carriers underwrite defense-side liability; LitInt's defense-side intelligence has long-cycle relevance once governance plan ratified. Lower-probability, longer-horizon bundle. |
| Travelers, Progressive Commercial, AmTrust (Tier C, workers' comp / auto) | **IRADev** (labor concentration overlap) | Workers' comp underwriting depends on labor density / industry concentration data — overlap with IRADev's BLS/QCEW layer. Lower-probability cross-sell. |

**Action:** Underwrite is the platform tip of the spear. Every parametric / carrier / MGA close becomes a warm intro to (a) DCII via the carrier's invested-asset team, (b) IRADev via the carrier's renewable-PPA / corporate-procurement team, (c) LitInt only after governance plan ratified. Cross-product attribution tracked from Phase 1.

---

## 10. Phasing

### Phase 0 — Foundation (now → 2026-Q3)
- Wildfire (USDA + USGS) ingestor
- Hurricane (NHC HURDAT) ingestor
- NOAA SPC severe-storm history
- Real-time NOAA / USGS trigger feed
- Sub-second API endpoint optimization

### Phase 1 — Underwrite MVP (2026-Q4)
- Submission triage engine
- Account scoring
- Parametric trigger feed
- 3 design partners (parametric)

### Phase 2 — Carrier-grade (2027-Q1 → Q2)
- Treaty aggregation
- Renewal monitor
- Climate-scenario rerun
- **Cyber-peril enrichment SKU (At-Bay-style)** — minimal-scope cyber peril layer: BGP/CIDR exposure, breach-history database, supply-chain dependency mapping. Promoted from Phase 4 because At-Bay is in the explicit ICP (line 29) and a cyber SKU is needed to land that account. Cyber + property bundle is a beachhead-reinforcing pricing motion.
- 5+ design partners (parametric + carrier + 1 cyber-MGA)

### Phase 3 — Reinsurance & regulatory (2027-Q3)
- Regulatory reporting (NAIC, state)
- Bordereaux + Lloyd's export
- 1 reinsurer signed (re-entry of the segment excluded from Phase 1 beachhead — their 12–18mo cycle now lands here)
- MCP server: shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11. Ship the Underwrite MCP surface (peril scores, triage, treaty aggregation, trigger feed) on the unified MCP at the same time as the other three product surfaces; do not stand up an Underwrite-specific MCP.

### Phase 4 — Adjacent expansion (2027-Q4+)
- Workers' comp / commercial auto SKU
- Lloyd's / international expansion
- (Cyber peril promoted to Phase 2 — see above)

---

## 11. Risks & Open Questions

| # | Risk / Question | Mitigation |
|---|---|---|
| R1 | Verisk / CoreLogic respond with bundled discount | Differentiate on parcel-fusion + audit + price |
| R2 | Sales cycles are long (12–18 mo for carriers) | Beachhead with parametric (3–6 mo cycles); use as references |
| R3 | Regulatory variance state-by-state | Build state-by-state reporting templates incrementally |
| R4 | Cat-model accuracy benchmarking gets us into a comparison fight | Position as data layer, not as cat model |
| R5 | Climate-projection accuracy is contested | Be transparent about methodology + lineage |
| Q1 | Build climate projections natively or partner? | Partner first (Jupiter/First Street), build later if margin justifies |
| Q2 | Sell to E&S brokers (Risk Placement, CRC) directly? | Phase 4 |
| Q3 | Healthcare / med-mal as adjacent peril? | Out of scope; medmal is different shape |

---

## 12. Shared Infra with DCII / IRADev

Underwrite shares:
- Site Intel core + 9 domain layers
- FEMA / EPA / USGS / NWI ingestors
- Reports / audit / lineage stack
- Governance + RBAC + SSO
- Unified MCP server (shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11; Underwrite exposes its peril/triage/treaty/trigger surface on the same MCP, not a product-specific one)

**Different from DCII / IRADev:**
- Sub-second API SLA (vs. interactive web app primary)
- Bulk batch portfolio runs (vs. per-asset)
- New peril data (wildfire, hurricane, SCS, climate scenarios)
- Parametric real-time trigger feed (a real-time service we don't have today)
- Bordereaux + NAIC regulatory output formats

**~60% of the engineering is shared with DCII/IRADev**, plus a real-time trigger system that's net-new.

---

## 13. What Success Looks Like

A senior underwriter at Hanover Insurance opens a $30M factory submission in Iowa. The Underwrite triage engine returns: flood 22, wildfire 8, earthquake 4, SCS 67 (high), wind 31, contamination 11, concentration 14 — composite 38, "borderline." It flags two ACRES sites within 0.5 mile and 6 active SCS events within 25 miles in the last 5 years. The underwriter sees the audit trail (every number traceable to source + version). She prices in the SCS exposure with a deductible adjustment. She binds in 90 seconds — vs. 90 minutes manual.

Same product, different lens: Arbol prices a parametric drought policy for an Iowa corn-farm aggregator. Underwrite delivers real-time soil moisture + Drought Monitor + USGS trigger feed with attestation reports. Arbol launches the policy in 6 weeks rather than 6 months building the data layer.

That is the product.

---

## 14. Sources / Internal References

- `app/sources/fema/`, `app/sources/epa/`, `app/sources/usgs/`, `app/sources/nwi/` — peril ingestors
- `app/services/scoring/` — site scoring engines
- `app/core/audit_service.py`, `app/core/lineage_service.py` — governance stack
- `docs/strategy/PRD_DATACENTER_INVESTOR_INTEL.md`, `docs/strategy/PRD_IRA_ENERGY_DEVELOPER_INTEL.md` — sister PRDs (shared infra)
- `docs/strategy/NEXDATA_SELLABLE_CAPABILITIES_2026.md` — origin
