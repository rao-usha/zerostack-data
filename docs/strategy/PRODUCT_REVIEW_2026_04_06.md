# Nexdata Product Review — April 6, 2026

## Executive Summary

Nexdata has extraordinary backend infrastructure — more than most Series A companies. 60 data source modules, 200+ database tables, 170+ API routers, 8 signal chains, agentic DD, and a full worker/scheduler system. The competitive moat is real and hard to replicate. The gap is packaging: there's no production frontend, no auth/billing, and no self-service onboarding. The product is an API + demo pages, not an application a customer can use independently.

**TAM:** $8B (private markets data & analytics), growing 12%/yr to $18B by 2030 (BlackRock, Preqin acquisition filing).

**Recommendation:** Narrow to PE intelligence as the hero product. Build a real web app with auth. Ship one killer workflow.

---

## What's Built (Inventory)

| Dimension | Count | Notes |
|-----------|-------|-------|
| API routers | ~170 | Hundreds of individual endpoints |
| Database tables | ~200+ | Across 8 model files |
| Data source modules | 60 | Government, financial, commercial, specialty |
| Services/scorers | 37+ | Plus 5 synthetic generators |
| Frontend pages | 20 | Demos, dashboards, D3 visualizations |
| Signal chains | 8 | Fully operational scoring engines |
| Worker executors | 8 | Job types with queue infrastructure |
| Scheduled jobs | 15+ | Recurring automation |

---

## Product Surface Area — By Domain

### Core PE Intelligence (Strong — this is the product)
- Deal pipeline CRM with 7 stages, activity logging
- Company health scoring (0-100, A-F tier, 4 categories)
- Exit readiness scoring (6-signal composite)
- Financial benchmarking (vs peer median/P25/P75)
- Due diligence agent (risk scoring, red flags, structured memo)
- Buyer discovery (strategic + financial, with fit scores)
- Data room package assembly
- Deal win probability
- Competitive intelligence (moat assessment, movements)
- People/org chart (4-phase deep collection, 142+ companies)
- Job posting intelligence (5 ATS platforms, skills extraction)
- News monitoring, alerts, watchlists

### Site Intelligence / Datacenter (Strong — could be its own product)
- 57 tables covering power, telecom, transport, labor, risk, incentives
- 18 data sources for county-level scoring
- LLM-generated investment theses
- Pipeline tracking
- Interactive demo

### Macro Cascade Intelligence (Cutting-edge — but niche)
- Causal graph with FRED/BLS nodes
- Stochastic forecasting (O-U process, p10/p50/p90)
- Multi-variable shock simulation
- LLM chat with 10 tools
- 9 precanned scenarios
- D3 interactive explorer

### LP/Family Office (Moderate — foundation exists)
- LP collection, governance, performance, allocations
- GP pipeline scoring, LP→GP graph
- Family office profiles and contacts

### Data Infrastructure (Solid)
- Worker queue with SELECT FOR UPDATE SKIP LOCKED
- Data provenance (real vs synthetic tracking)
- Data quality framework with anomaly detection
- Entity resolution, lineage tracking
- Export to CSV/JSON/Parquet
- GraphQL layer
- SSE streaming for live job progress

---

## Critical Gaps

### 1. No Production Frontend
20 HTML demo files. Zero React/Vue/Next.js app. No component library, routing, or state management. For $2-10K/mo, buyers expect a polished web app.

### 2. No Auth / Multi-Tenancy / Billing
Skeleton auth router but no real authentication, RBAC, workspace isolation, or Stripe integration. Cannot onboard a customer today.

### 3. No Real Data Freshness
60 source modules are plumbing — they work when API keys are configured and manually triggered. "Collected nightly" requires all keys provisioned and schedules running.

### 4. Many Tables Are Empty
200+ tables exist in schema. Without running seeders + collection, most are hollow. Data fill rate for a new deployment is probably <20%.

### 5. No Onboarding / Self-Service
No "create account → pick portfolio → see insights" flow. Every customer needs white-glove setup.

### 6. Vertical Sprawl
Healthcare, medspa, 3PL, rollup intel, labor arbitrage — these fragment focus. A PE firm doesn't care about medspa unless they're in that vertical.

---

## Strategic Assessment

### What's Right
- Core thesis is correct: PE firms overpay for stale PitchBook/CapIQ data
- Job-posting-as-leading-indicator is genuinely differentiated
- Autonomous DD/scoring/monitoring loop is what PE associates want
- Site intel for datacenter/infrastructure is a real, growing need
- 8 signal chains are sophisticated and unique

### What's Concerning
- **Width vs. depth:** 60 sources, 200 tables, 170 routers — but a PE associate just wants search → score → memo → track
- **Demo-driven development:** 15 demo items checked off, but demos ≠ product
- **Three products in one:** PE intel, site intel, macro cascade — bundling dilutes the pitch

### Recommendation
Narrow to PE intelligence as hero product. Site intel becomes a vertical module. Macro cascade becomes advanced analytics. Kill/freeze niche verticals until 10 paying PE customers.

---

## TAM Analysis

### Market Sizing

| Market | Size (2025) | Growth | Source |
|--------|------------|--------|--------|
| **Private markets data & analytics** | **$8B** | **12%/yr → $18B by 2030** | BlackRock (Preqin acquisition filing) |
| Alternative data (all investors) | $14-19B | ~25% CAGR → $40B by 2030 | Neudata |
| Financial data services (total) | $28-29B | 8-10% CAGR | Multiple |
| Location analytics / site intel | $25B | 13.5% CAGR → $47B by 2030 | Industry reports |
| Family office software | ~$4B | 4.8%/yr | Industry reports |

### Comparable Companies

| Company | Revenue/ARR | Valuation | Pricing |
|---------|------------|-----------|---------|
| PitchBook | $618-670M | Part of Morningstar ($13B) | $12-40K/user/yr |
| Preqin | ~$240M ARR | $3.2B (acquired by BlackRock, 13x rev) | $25-80K/yr |
| AlphaSense+Tegus | $500M ARR | $4B | $10-20K/seat |
| Grata | ~$30-40M est. | $200M (acquired by Datasite) | ~$15-25K/yr |
| Placer.ai (location) | $100M ARR | $1.5B | — |

### Recent M&A (Extraordinary Multiples)
- BlackRock → Preqin: **$3.2B** (13x revenue) — March 2025
- S&P Global → With Intelligence: **$1.8B** (~14x revenue) — November 2025
- AlphaSense → Tegus: **$930M**, combined $4B valuation — 2024
- Datasite/CapVest → Grata: **$200M** + $500M platform commitment — June 2025
- Signal: acquirers paying **10-15x revenue** for private markets data moats

### Nexdata SAM (Serviceable Addressable Market)

| Segment | Firms | Price/yr | SAM |
|---------|-------|----------|-----|
| Mid-market PE ($500M-$5B AUM) | ~2,000 | $24-60K | $48-120M |
| Family offices | ~3,200 (N. America) | $12-36K | $38-115M |
| Infrastructure/RE (site intel) | ~1,000 | $12-24K | $12-24M |
| **Total SAM** | | | **$100-260M** |

**First-year realistic target:** 20-50 customers × $36-60K = **$720K - $3M ARR**

---

## Key Stats for Investor Conversations

- ~18,000 PE funds active globally (60% increase in 5 years)
- ~8,030 single family offices globally ($3.1T AUM), projected 10,720 by 2030
- PE firms spend $100K-500K/yr on data tools (PitchBook + CapIQ + consultants)
- 56% of PE firms now use alternative data (up from ~30% in 2022)
- 63% of investors plan to increase alt data spending
- BlackRock's $8B TAM figure is the most authoritative number in the space

---

## Next Steps (Prioritized)

1. **Build a real web app** — Next.js dashboard with auth, portfolio config, top 5 workflows
2. **Managed data freshness** — provisioning flow for nightly collection on a customer's portfolio
3. **One killer demo** — 5-minute video: company name → full DD memo in 60 seconds
4. **Pricing validation** — talk to 10 PE associates about willingness to pay
5. **Freeze vertical sprawl** — no new verticals until 10 paying customers
