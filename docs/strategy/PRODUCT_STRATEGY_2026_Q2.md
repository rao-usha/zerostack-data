# Nexdata Product Strategy — Q2 2026

**Date:** April 6, 2026
**Author:** Strategic analysis compiled from competitive research, buyer analysis, pricing analysis, and codebase audit
**Status:** Draft for founder review

---

## Executive Summary

Nexdata has built an extraordinary data infrastructure — 60 data source modules, 200+ database tables, 8 signal chains, autonomous DD agents, and a full worker/scheduler system. This is more backend than most Series A companies. The competitive moat is real and hard to replicate.

**The gap is packaging, not capability.**

There is no production frontend. No auth/billing. No self-service onboarding. The product is an API + demo pages, not an application a customer can use independently. Meanwhile, the market is consolidating fast: BlackRock bought Preqin for $3.2B, Datasite is spending $500M assembling a full-stack platform, AlphaSense hit $500M ARR and $4B valuation.

**The window is open but narrowing.** PE firms are actively adopting AI/alt-data tools. Incumbents are bolting on AI features but their data is still stale. Datasite's roll-up is still integrating. This is the moment to ship a focused product and land 10-20 customers.

This document proposes three strategic options, recommends one, and lays out a 90-day roadmap.

---

## The Market

### TAM / SAM / SOM

| Level | Market | Size | Growth |
|-------|--------|------|--------|
| **TAM** | Private markets data & analytics | **$8B** (2025) | 12%/yr → $18B by 2030 |
| **Adjacent TAM** | Alternative data for investors | $14-19B | ~25% CAGR |
| **SAM** | Mid-market PE + family offices + infra/RE | **$100-260M** | |
| **SOM (Year 1)** | 20-50 mid-market PE firms | **$500K-$2M** | |

Source: BlackRock (Preqin acquisition filing), Neudata, industry reports.

### Validation: M&A Multiples

Acquirers are paying 10-17x revenue for private markets data moats:

| Deal | Price | Multiple | Date |
|------|-------|----------|------|
| BlackRock → Preqin | $3.2B | 13x revenue | Mar 2025 |
| S&P → With Intelligence | $1.8B | ~14x revenue | Nov 2025 |
| AlphaSense → Tegus | $930M | ~8x revenue | Jul 2024 |
| Datasite → Grata | $200M | 17.5x revenue | Jun 2025 |

**Implication:** A Nexdata with $5M ARR and strong growth could be worth $50-85M in this market. At $20M ARR, $200-340M.

---

## Competitive Position

### Where We Win (Nobody Else Has This)

| Capability | PitchBook | Grata/Datasite | AlphaSense | ToltIQ | **Nexdata** |
|---|---|---|---|---|---|
| Job posting intelligence as deal signal | No | No | No | No | **Yes** |
| Autonomous DD from public data | No | No | No | VDR docs only | **Yes (28 sources)** |
| Company health scoring (0-100) | No | No | No | No | **Yes** |
| Exit readiness scoring | No | No | No | No | **Yes** |
| Macro cascade intelligence | No | No | No | No | **Yes** |
| Site/infrastructure intelligence | No | No | No | No | **Yes** |
| People/org chart construction | Limited | No | No | No | **Yes** |
| 28+ public API integrations | Manual | Web crawl | Document search | N/A | **Automated** |
| Price for mid-market | $25K+/seat | $15-50K | $10-20K/seat | TBD | **$10-20K/seat** |

### Where We Lose (Honest Assessment)

| Dimension | Incumbents | Nexdata |
|-----------|-----------|---------|
| Historical data depth | 15-20 years | Months |
| Production frontend | Polished apps | Demo HTML pages |
| Customer base | Thousands | Zero |
| Brand recognition | Industry standard | Unknown |
| Sales/support team | Hundreds | Solo founder |
| Company database size | 16-21M (Grata/SourceScrub) | ~200 seeded companies |
| Auth/billing/multi-tenancy | Enterprise-grade | Skeleton |

### Competitive Dynamics to Watch

1. **Datasite roll-up execution** — 6 acquisitions in 18 months. If integration stumbles (likely), there's a prolonged window. If it succeeds, formidable competitor.
2. **PitchBook mid-market retreat** — Layoffs in mid-market sales (Nov 2025). Our target segment is being actively deprioritized.
3. **AlphaSense expansion** — $500M ARR, moving aggressively. But they're content-focused (search what we have), not autonomous (go collect what we don't).
4. **ToltIQ** — Ex-KKR CIO founder, $12M Series A, 65+ PE clients. Most dangerous emerging competitor. But they work post-LOI on private docs; we work pre-LOI on public data. Potentially complementary.

---

## Three Strategic Options

### Option A: "AI Operating Partner Platform"

**Positioning:** Not a SaaS tool — an AI-powered operating partner that PE firms retain on a consulting-like basis. Nexdata is the core IP that powers the advisory relationship.

**Go-to-market:**
- Retainer ($10-20K/mo) + carry participation on deals sourced/improved
- Founder-led delivery leveraging McKinsey + Two Sigma credibility
- 5-10 deep client relationships, not 50 light ones
- Custom research, DD memos, portfolio monitoring as managed service

**Pros:**
- Avoids head-to-head with $4B+ incumbents
- Higher ACV ($120-240K/yr per client)
- Aligns with founder's background and network
- Carry participation creates massive upside
- Can start immediately — no frontend needed, service wraps the API

**Cons:**
- Doesn't scale without hiring people (consulting model)
- Lower multiple (services vs. SaaS) if raising VC or selling
- Harder to build a $100M+ business this way
- You become the bottleneck

**Best if:** You want to generate revenue immediately, validate with real PE clients, and build toward a product from client feedback. Can always productize later.

### Option B: "PE Intelligence SaaS"

**Positioning:** The AI-native intelligence platform for mid-market PE. Replaces the manual assembly of PitchBook + CapIQ + Excel + consultants with autonomous research, scoring, and monitoring.

**Go-to-market:**
- SaaS pricing: $10-35K/seat/year (3 tiers)
- Target ACV: $25-40K
- Founder-led sales to first 15-20 customers via network
- Hire first AE at $500K ARR
- Conference pipeline + content marketing

**Pros:**
- Scalable — software, not services
- Higher multiples (10-17x revenue for PE data companies)
- Clear path to VC funding (seed → Series A at $500K-$1M ARR)
- Leverages everything already built
- Network effects possible (deal data, company coverage)

**Cons:**
- Requires building a real frontend (3-6 months)
- Requires auth/billing/multi-tenancy infrastructure
- Longer time to first revenue
- Competing against well-funded incumbents on product surface area

**Best if:** You want to build a venture-scale business and are willing to invest 3-6 months in product packaging before monetizing.

### Option C: "Hybrid — Service-Led Product" (RECOMMENDED)

**Positioning:** Start as an AI operating partner (Option A) to generate immediate revenue and validate with real clients. Simultaneously build the SaaS product (Option B) informed by client workflows. Transition from service to product as the frontend matures.

**Go-to-market:**
- **Months 1-3:** Service-wrap the API. Deliver automated DD memos, portfolio monitoring, and market intelligence as a managed service to 3-5 PE firms from your network. Price at $10-15K/mo.
- **Months 4-6:** Build the production frontend (auth, dashboard, top 5 workflows) informed by what clients actually use. Continue landing service clients.
- **Months 7-12:** Transition service clients to self-serve SaaS. Add new clients directly to SaaS product. Target 15-20 total customers, mix of service and SaaS.
- **Month 12:** Raise seed round off $500K-$1M ARR, clear product-market fit, and 15+ customers.

**Pros:**
- Revenue from day 1 (no 6-month product build before monetizing)
- Real client feedback shapes the product (not guessing)
- Service relationships convert to high-retention SaaS (120%+ NRR)
- VC-fundable: service revenue validates demand, product in development
- Leverages McKinsey credibility for service sale, Two Sigma credibility for product
- Carry participation from service clients provides additional upside

**Cons:**
- Requires founder to do service delivery AND product development
- Service clients may have expectations that don't scale
- Risk of getting stuck in "consulting mode" and never shipping product

**Mitigation:** Set explicit milestones — if SaaS product isn't live by Month 6, hire a contractor to build the frontend while you continue service delivery.

---

## Recommended Strategy: Option C — Service-Led Product

### The Insight

**Grata took 5 years (2016-2021) to find product-market fit before growth exploded.** You don't have that runway. But you have something Grata didn't: a McKinsey Partner's network of PE relationships. Service-wrapping the API lets you monetize that network immediately while building the product.

**ToltIQ's playbook is instructive:** Founded by ex-KKR CIO, launched as a service, landed 65+ PE/LP/FO clients including HarbourVest and Fortress. Now raising $12M Series A to productize. Service-first with PE pedigree works.

### Wedge Product: The 60-Second DD Memo

Every research input points to the same wedge: **the automated company intelligence brief**.

- A PE associate currently spends 2-4 hours assembling a first-pass analysis of a target company
- Nexdata can produce an 80%-good brief in under 60 seconds from 28+ public data sources
- Bain says AI screening can reduce this time by 50-60%. Nexdata can reduce it by 90%.
- The "aha moment": "I just got in 5 minutes what takes my analyst a full day"

**The brief includes:**
1. Company overview (SEC filings, web presence, corporate registry)
2. Financial signals (revenue estimates, growth trajectory, margin trends)
3. Leadership team + org chart (people collection pipeline)
4. Hiring velocity + department mix (job posting intelligence — **unique to Nexdata**)
5. Competitive landscape (competitor mappings, market position)
6. Macro environment impact (cascade intelligence)
7. Risk flags (regulatory, environmental, litigation)
8. Health score (0-100) with tier rating (A-F)

**This is the thing that makes eyes light up.** Everything else (portfolio monitoring, exit readiness, site intel) is the expand.

### The Job Posting Moat

Job posting intelligence is Nexdata's most defensible differentiator:

- **Nobody else has it**: Not PitchBook, not Grata, not AlphaSense, not Datasite
- **It's a leading indicator**: Hiring velocity signals growth/distress 3-6 months before financials change
- **Real example**: Forever 21 job listings decreased 43% in August — a leading indicator that preceded public distress news
- **PE firms validate it**: 67% of investment managers now use alternative data; 94% plan to increase budgets
- **One firm reported**: Alternative workforce data reduced operational DD time by over 50%

**Position it as:** "The signal you can't get anywhere else — and the one that matters most."

---

## 90-Day Roadmap

### Month 1: Land First Service Clients (Revenue from Day 1)

**Week 1-2:**
- [ ] Write 5-page "AI Operating Partner" pitch deck
- [ ] Draft 60-second DD memo template (structured output from existing endpoints)
- [ ] Identify 10 PE contacts from McKinsey/Two Sigma network
- [ ] Send personalized outreach: "I built something — can I show you what it finds on one of your portfolio companies?"

**Week 3-4:**
- [ ] Run proof-of-value for 3-5 firms (generate DD memos on their actual targets)
- [ ] Close 1-2 service engagements at $10-15K/mo
- [ ] Deliver: weekly market intelligence brief + on-demand DD memos + pipeline scoring
- [ ] Document every client interaction: what they use, what they ignore, what they ask for

### Month 2: Build the Core Product

**Week 5-6:**
- [ ] Set up Next.js project with auth (Clerk/Auth0), routing, and design system
- [ ] Build "Company Search" page — search → results → company profile
- [ ] Build "DD Memo" page — enter company name → get structured brief in <60 seconds
- [ ] Wire up to existing API endpoints (search, scores, DD, people, job postings)

**Week 7-8:**
- [ ] Build "Deal Pipeline" page — add companies to pipeline, track stages, score
- [ ] Build "Alerts" page — job posting changes, leadership departures, news mentions
- [ ] Implement workspace isolation (basic multi-tenancy)
- [ ] Deploy to production (Vercel/Railway + existing Docker API)
- [ ] Land 1-2 more service clients (running total: 3-4)

### Month 3: Ship SaaS Product

**Week 9-10:**
- [ ] Build "Portfolio Dashboard" — health scores, exit readiness, benchmarking for tracked companies
- [ ] Add Stripe billing (3 tiers: Starter $10K, Pro $20K, Enterprise $35K per seat/yr)
- [ ] Build onboarding flow: create account → select portfolio companies → start seeing insights
- [ ] Set up managed data freshness: nightly collection for customer portfolios using free API sources

**Week 11-12:**
- [ ] Transition 1-2 service clients to SaaS pilot (with hands-on support)
- [ ] Launch to 5 additional prospects from conference/network pipeline
- [ ] Record 5-minute demo video: "From company name to full DD memo in 60 seconds"
- [ ] Publish first "PE Job Posting Intelligence" monthly report (content marketing)
- [ ] Target: 5-8 total customers, $30K-$60K MRR run rate

### Key Metrics at Day 90

| Metric | Target |
|--------|--------|
| Paying customers | 5-8 |
| MRR | $30-60K |
| ARR run rate | $360-720K |
| Demo-to-pilot conversion | >40% |
| Pilot-to-close conversion | >60% |
| DD memo generation time | <60 seconds |
| Data sources active in production | 15+ |

---

## Pricing Strategy

### Recommended Tiers

| Tier | Price/Seat/Year | Target | Core Capabilities |
|------|----------------|--------|-------------------|
| **Starter** | $10,000 | Small PE, family offices, corp dev | Company search, job posting signals, health scores, basic alerts, 50 tracked companies |
| **Professional** | $20,000 | Mid-market PE ($1-5B AUM) | + Automated DD memos, portfolio dashboard, exit readiness, benchmarking, 200 tracked companies |
| **Enterprise** | $35,000+ | Large PE, investment banks | + Site intelligence, LP/GP universe, macro cascade, API access, unlimited companies, dedicated CSM |

### Service Pricing (Months 1-6)

| Package | Price/Month | Deliverables |
|---------|-----------|--------------|
| **Intelligence Retainer** | $10,000 | Weekly market brief, 10 DD memos/month, pipeline scoring |
| **Operating Partner** | $15,000 | + Portfolio monitoring, exit readiness, quarterly LP data pack |
| **Full Platform** | $20,000 | + Custom research, site intelligence, unlimited DD memos |

### Financial Model (Year 1)

| Quarter | New Customers | Total Customers | Avg MRR/Customer | Total MRR | Cumulative ARR |
|---------|--------------|-----------------|-------------------|-----------|----------------|
| Q2 2026 | 3 (service) | 3 | $12K | $36K | $432K |
| Q3 2026 | 5 (mixed) | 8 | $8K | $64K | $768K |
| Q4 2026 | 7 (mostly SaaS) | 14 (1 churn) | $5K | $70K | $840K |
| Q1 2027 | 8 (SaaS) | 21 (1 churn) | $4K | $84K | $1.0M |

**Note:** MRR/customer decreases as mix shifts from high-touch service ($10-15K/mo) to SaaS ($2-3K/mo/seat). Total ARR grows through volume.

---

## Moat Analysis: What's Defensible

### Strong Moats (Hard to Replicate)

1. **28+ public API integrations** — Each source requires understanding the API, handling rate limits, parsing responses, normalizing data, and maintaining over time. Grata spent 8 years building their web crawling infrastructure. This is genuine engineering moat.

2. **Signal chain scoring models** — 8 composite scoring engines (deal environment, diligence composite, GP pipeline, executive signals, portfolio stress, site intelligence, healthcare practice, roll-up attractiveness). These encode domain expertise into algorithms.

3. **Job posting intelligence** — Proprietary signal nobody else has for PE deal assessment. Requires ATS platform integration, skills extraction, trend analysis. Not trivial to replicate.

4. **Founder credibility** — Ex-McKinsey Partner + Ex-Two Sigma SVP Data Science. This combination is exceptionally rare and opens doors that product alone cannot.

### Moderate Moats (Defensible but Catchable)

5. **Autonomous DD agent** — 4-phase pipeline (SEC → website → news → org chart) is sophisticated but could be replicated by a well-funded competitor in 6-12 months.

6. **Data provenance system** — Real vs. synthetic tracking with confidence scores. Valuable for compliance-conscious PE firms but not rocket science.

### Weak Moats (Easy to Copy)

7. **Company health scoring** — Conceptually simple (composite of financial/growth/stability/tech velocity). The value is in the data inputs, not the scoring formula.

8. **Deal pipeline CRM** — Commodity feature. DealCloud, Affinity, 4Degrees all do this well.

### What's NOT a Moat

- Historical data depth (PitchBook/Preqin have 15-20 years; we have months)
- Company database size (Grata has 21M; we have ~200 seeded)
- Brand recognition (zero outside the founder's network)

### Moat Strategy

**Double down on strong moats. Don't invest in weak moats.**

Specifically:
- **Expand job posting intelligence** — more ATS platforms, deeper analysis, predictive models
- **Deepen signal chains** — make the scoring models more sophisticated and data-rich
- **Maintain API integrations** — keep them fresh, add new sources
- **Leverage founder credibility** — the founder IS the brand at this stage

Don't waste time building a better CRM (DealCloud exists), a bigger company database (Grata exists), or a deeper content library (AlphaSense exists). Build what they can't.

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| No production frontend delays first sale | HIGH | HIGH | Service-wrap the API (Option C). Frontend is Month 2-3, not a blocker. |
| Datasite roll-up succeeds, becomes dominant | MEDIUM | HIGH | They don't have job postings, site intel, or macro data. Position as complementary layer. |
| PitchBook ships AI features that overlap | MEDIUM | MEDIUM | Their data is still stale. AI on stale data is still stale. Our moat is fresh data + autonomous collection. |
| Mid-market PE firms too price-sensitive | LOW | MEDIUM | They spend $150-500K/yr on data tools. $25K/yr is a rounding error. The champion justifies it with time savings. |
| Solo founder can't deliver service + build product | HIGH | MEDIUM | Hire a frontend contractor by Month 2. Use Claude Code for development velocity. Limit service clients to 5 until product is live. |
| PE firms won't adopt AI tools | LOW | HIGH | 56% already use alt data. 63% plan to increase. Bain/McKinsey/BCG all saying "adopt or die." Not a question of if, but when. |
| LLM costs escalate faster than revenue | LOW | MEDIUM | Track via llm_cost_tracker.py. Usage-based pricing on AI features passes cost through. Cap LLM calls per tier. |

---

## Investor Narrative (When You're Ready to Raise)

### The Pitch

> **Nexdata is the AI intelligence layer for private equity.** We autonomously collect, score, and monitor companies across 28+ public data sources — replacing the manual assembly of PitchBook, Capital IQ, consultants, and spreadsheets that PE firms cobble together today.
>
> **The market is $8B and growing 12%/year** (BlackRock's own number when they paid $3.2B for Preqin). PE firms spend $150-500K/year on stale data from incumbents. We deliver fresher, deeper intelligence — including job posting signals nobody else has — at 80% less cost.
>
> **We're the only platform that combines** autonomous data collection from 28+ public APIs, AI-driven scoring with 8 signal chains, automated due diligence, and real-time monitoring. PitchBook gives you a database. We give you an analyst.
>
> **Our founders bring** [decades] at McKinsey (PE network, operating playbook) and Two Sigma (data science, quantitative rigor). We've already built the backend — 200+ database tables, 60 source integrations, 8 scoring models. Now we're packaging it for mid-market PE firms.
>
> **We're raising $2-4M** to build the production frontend, hire 2 engineers and 1 AE, and scale from 5 service clients to 30+ SaaS customers. Target: $1M ARR in 12 months.

### Key Investor Metrics

| Metric | Value |
|--------|-------|
| TAM | $8B (growing 12%/yr) |
| SAM | $100-260M |
| Target ACV | $25-40K |
| Path to $1M ARR | 20-40 customers |
| Comparable exit multiples | 10-17x revenue |
| Grata comp | $11.4M revenue → $200M exit (17.5x) |
| AlphaSense comp | $500M ARR, $4B valuation |
| Defensibility | 28 API integrations + 8 signal chains + job posting moat |

---

## What to Kill / Freeze / Keep

### Kill (Remove from product, stop investing)
- Medspa discovery (niche vertical, not PE core)
- 3PL enrichment (completed project, no ongoing value)
- Labor arbitrage (interesting feature, not a selling point)
- Zip scores (too granular for PE buyers)

### Freeze (Keep in codebase, don't invest further until 10 customers)
- Healthcare practice scoring (could matter later if healthcare PE firms adopt)
- Rollup intelligence (interesting for roll-up focused PE but too niche for now)
- GraphQL layer (over-engineered for current stage)
- Synthetic data generators (useful for demos but not a product feature)
- Statistical validation dashboard (internal tool, not customer-facing)

### Keep and Invest (Core to the product)
- **Company search + scoring** — the entry point
- **Automated DD agent** — the "aha moment"
- **Job posting intelligence** — the moat
- **People / org chart collection** — high-value for PE
- **Deal pipeline CRM** — table stakes (keep it simple)
- **Portfolio monitoring + exit readiness** — the expand play
- **Financial benchmarking** — competitive positioning for exits
- **Site intelligence** — vertical module for infrastructure PE
- **Macro cascade** — advanced analytics (Enterprise tier only)
- **Alerts + monitoring** — retention driver

---

## Summary: The One-Page Strategy

**What:** AI intelligence platform for mid-market PE firms

**Who:** Deal sourcing VPs/Principals at $1-5B AUM PE firms

**Wedge:** 60-second automated DD memo from 28 public data sources

**Moat:** Job posting intelligence + autonomous collection + 8 signal chains

**GTM:** Service-led product — start with retainer clients, build SaaS in parallel, transition by Month 6

**Pricing:** $10-35K/seat/year (SaaS) or $10-20K/month (service retainer)

**90-day goal:** 5-8 customers, $30-60K MRR, production frontend live

**12-month goal:** 20+ customers, $1M ARR, raise seed round

**Why now:** PitchBook retreating from mid-market. Datasite roll-up still integrating. PE firms actively adopting AI. 56% already use alt data, 63% increasing budgets. The window is open.

**Why us:** McKinsey network opens PE doors. Two Sigma credibility validates the data science. 18 months of engineering creates technical moat. Nobody else has job posting intelligence + autonomous DD + 28 API sources in one platform.

---

*Next review: May 2026 — update based on first client feedback and competitive movements.*
