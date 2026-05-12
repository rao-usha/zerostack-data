# Nexdata — What You Can Sell Right Now

**Date:** 2026-04-25
**Purpose:** Concrete, packageable capabilities you can put on a price sheet today. Companion to `PERPLEXITY_COMPUTER_COMPARISON_2026.md` — that doc is analysis, this one is ammo.

---

## The Frame (Use This in Every Pitch)

> **Perplexity Computer is a smart intern with no memory. Nexdata is the warehouse that intern wishes they had.**

Computer does one thing very well: spin up a fresh research task, browse the web, and hand you a draft. When the task ends, the work evaporates. There is no warehouse, no governance, no reproducibility, no vertical depth.

Nexdata persists. Every signal we collect lands in a queryable, versioned, provenance-tagged database with quality rules, lineage, and audit trails. We ship 52 ingestors, 47 specialized agentic collectors, 9 domain-specific scoring engines, and 14 production report templates — all of it vertical to private markets, site selection, and PE workflows.

A buyer can use both. The right answer is almost always "yes, and."

---

## What's in the Bag (Sellable Today)

| # | Capability | Status | Primary Buyer | ACV Range |
|---|---|---|---|---|
| 1 | Site Intelligence Platform | **Shipping** | Industrial PE, infra funds, corp site selection | $50–250K |
| 2 | PE Deal Probability Engine | **Shipping** | LMM PE, search funds, family offices | $25–150K |
| 3 | Org Chart & Leadership Intelligence | **Shipping** | Exec search, BD, sales intel, PE talent | $20–80K |
| 4 | Family Office & LP Intelligence | **Shipping** | GPs raising funds, placement agents | $30–120K |
| 5 | Roll-up Target Screener (MedSpa + extensible) | **Shipping** | Roll-up PE, search funds, healthcare PE | $40–150K |
| 6 | Data Center / Power Site Suitability | **Shipping** | DC developers, hyperscaler M&A, infra PE | $50–200K |
| 7 | Cascade / Macro Signal Convergence | **Shipping** | Macro funds, sector-timing PE | $40–120K |
| 8 | Governed PE Data Warehouse | **Shipping** | LPs, pension funds, compliance teams | $75–300K |
| 9 | Reports-as-a-Service (PDF/XLSX/PPTX/HTML) | **Shipping** | Small PE shops, IR teams | $10–40K |
| 10 | Custom Vertical Build (any of the above as bespoke) | **Shipping** | Anyone with a niche thesis | $100K+ |

Below: each capability written as a sales card. Use these verbatim in decks.

---

## 1. Site Intelligence Platform

**Pitch:** "Pick the right site without flying ten people to ten counties."

**What it is:** Nine domain collectors fused into a unified scoring layer — power (EIA + ISO real-time + interconnect queues + utility rates), fiber/telecom (FCC + PeeringDB + Epoch data centers), transport (BTS + FRA + USACE waterways + FAA), labor (BLS QCEW + Census BPS), logistics (FMCSA + 3PL + port throughput), risk (FEMA + EPA + USGS earthquake/flood/wetlands + ACRES contamination), incentives (state EDOs + Good Jobs + CDFI + FTZ + zoning), water/utilities, and real estate (county zoning, MLS).

**Who buys:** Industrial PE buying manufacturing platforms. Infra funds. Corp dev teams running site selection. Data center developers.

**The problem we solve:** A site decision today requires pulling data from 15+ federal/state/utility sources, normalizing it, mapping it to a parcel, scoring it. This is a 3-month consulting engagement billed at $250K+. We do it in minutes against any address or county.

**Why Perplexity can't:** Real-time ISO data (PJM, CAISO, ERCOT, MISO, SPP, NYISO, ISONE), interconnect queue scraping, FERC structured filings, HIFLD substations, NWI wetlands, ACRES contamination — these are not "browsable web." They require working ingestors, schema knowledge, and per-source ETL we've already built. A general agent gets you an executive summary with hallucinated numbers; we deliver a parcel-level score against governed data.

**Demo:** Take a real address, return a 1-page site report with power, labor, fiber, risk, and incentives scored 0–100. 90 seconds.

**Pricing wedge:** $50K POC for one geography, $150–250K/yr for enterprise license + custom geographies.

---

## 2. PE Deal Probability Engine

**Pitch:** "We tell you who's about to sell before the banker pitches them."

**What it is:** Multi-signal probability model (`txn_prob_*` tables) that ingests SEC Form D, Form ADV, leadership changes, portfolio company news, fund vintage age, GP exit pressure signals, and competitor M&A activity — then scores each company on transaction probability with calibration tracking. Outputs ranked target lists with the signals that drove the score.

**Who buys:** Lower-middle-market PE associates and partners. Search funds. Family offices doing direct deals. Outbound BD teams.

**The problem we solve:** PE sourcing is a numbers game. A team of 5 associates calls 2,000 companies a year to find 20 LOIs. Our score lets them call the right 200 first.

**Why Perplexity can't:** Probability scoring requires a persistent feature store, calibrated weights, and outcome tracking over months. A one-shot agent run cannot tell you "this score is reliable because we've calibrated it against 400 closed deals." We have the schema, the historical signal store, and the calibration loop. Computer doesn't have a database.

**Demo:** Drop a sector + geography, get a ranked list of 50 targets with per-target signal explanations and probability bands. Compare to the firm's actual closed deals from the last year — show our score predicted them.

**Pricing wedge:** $25K seat × 3-seat minimum for a small PE shop. $80–150K firm license. Custom sector scoring +$50K.

---

## 3. Org Chart & Leadership Intelligence

**Pitch:** "Every reporting line at every private company you care about, refreshed monthly."

**What it is:** Four-phase deep collection per company — SEC EDGAR 10-K → website deep crawl with LLM extraction → news/press scan → org chart construction. Outputs versioned `org_chart_snapshots`, `leadership_changes`, `company_people` with email inference and MX verification, LinkedIn discovery + validation. Tracks departures and arrivals over time.

**Who buys:** Executive search firms. PE talent operating partners. Sales intelligence teams selling into private companies. BD teams at services firms (consulting, IB, law) targeting C-suites.

**The problem we solve:** ZoomInfo and Apollo have public-company coverage; private-company org charts are a black box. Searches like "who runs ops at portfolio company X" today require manual LinkedIn detective work. We hand it over assembled, versioned, and confidence-scored.

**Why Perplexity can't:** A general agent will scrape a LinkedIn page and call it done. We chain SEC filings → website → news → graph construction with leadership change tracking and confidence tiering ("SEC > Official websites > Annual reports > News"). The graph is the product, not the latest snapshot.

**Demo:** Pick a private company. Show the org chart we have, the change log over 18 months, and the confidence per node.

**Pricing wedge:** $20K/yr per seat for 100 companies tracked. $80K enterprise for unlimited. Per-company on-demand: $200/company.

---

## 4. Family Office & LP Intelligence

**Pitch:** "The LP database that knows what they actually invest in, not what the PPM says."

**What it is:** Family office discovery (news + regulatory), website research and contact discovery, fund holdings extraction, theme classification. Plus public LP intelligence via Form 990 + CAFR parsing — governance structure, performance history, investment thesis classification. Holdings normalization across sources.

**Who buys:** GPs raising funds. Placement agents. Wealth managers prospecting families. Service providers (fund admin, custody) targeting offices.

**The problem we solve:** Existing LP databases (Pitchbook, Preqin) are stale and miss family offices entirely. CAFR parsing, Form 990 extraction, and reconciling holdings across sources is grunt work nobody in fundraising wants to do.

**Why Perplexity can't:** Form 990 PDFs and CAFR extraction are not browse-able tasks — they're document parsing pipelines with structured outputs. Reconciling fund holdings across SEC + PPM + news requires a real entity resolution layer. We've built it.

**Demo:** Pick a state pension. Show 5-year governance changes, LP commitments by vintage, current sleeve allocation, and the data sources behind every line.

**Pricing wedge:** $30K seat for fundraisers, $80–120K firm license, $150K+ for placement agents needing white-label.

---

## 5. Roll-up Target Screener

**Pitch:** "County-level density maps for any roll-up thesis. Find your targets in 10 minutes."

**What it is:** Today: MedSpa Roll-up Screener (live) — county-level practice density, demographic match, competitor saturation, exit-multiple-friendly markets. Architecture is generic — point it at any vertical (HVAC, vet clinics, dental, auto repair, ABA therapy, ag services) and it scores counties using Census CBP, BLS, demographic data, and our discovery collectors.

**Who buys:** Roll-up PE. Search funds. Healthcare PE doing platform + add-on. Independent sponsors with a thesis but no analyst team.

**The problem we solve:** Roll-up sourcing today: an associate spends 6 weeks pulling Census CBP, doing density math in Excel, and cold-calling. We replace 6 weeks with 60 seconds.

**Why Perplexity can't:** Census CBP at the NAICS-6 × county level is not a "search the web" output. It's a structured ETL we've already done. The scoring model is calibrated against rollup-friendly market characteristics, not generated from a single LLM prompt.

**Demo:** Live MedSpa screener — pick a state, show top 20 counties with density, demographic fit, and current PE saturation. Then offer to clone it for the buyer's vertical in 4 weeks.

**Pricing wedge:** $40K for one vertical screener, $80–150K for 3 verticals + custom rules, $150K+ for a fully bespoke screener.

---

## 6. Data Center / Power Site Suitability

**Pitch:** "Where to build your next 200MW data center, ranked, with the interconnect queue position."

**What it is:** Specialization of Site Intelligence — but DC-specific. Power capacity available, transmission proximity, interconnect queue position by ISO, fiber convergence (PeeringDB + Epoch), water for cooling (USGS), state incentives (Virginia, Texas, Iowa, Oregon, etc.), tax abatements, latency to major peering points.

**Who buys:** Hyperscaler M&A teams. DC developers (QTS, Equinix-style). Infra funds. Power-aware crypto/AI compute investors.

**The problem we solve:** DC site selection is a real-time auction against AI demand. Power availability is changing month-to-month. Most DC developers run this analysis in Excel against stale EIA data. We give them live ISO interconnect queues plus full-stack site scoring.

**Why Perplexity can't:** ISO interconnect queue scraping (PJM, CAISO, MISO, ERCOT, SPP, NYISO, ISONE) is not stable web content — it's per-ISO portals with their own quirks. We have working ingestors. PeeringDB integration. NREL renewable resource overlays. Real-time-adjacent power data.

**Demo:** Take a DC developer's existing pipeline of 8 candidate sites — re-rank them with our data, show which one they're underweighting and which one is about to lose its power slot.

**Pricing wedge:** $50K POC for 5 sites. $150–200K/yr for unlimited. Hyperscaler tier $250K+ with custom integrations.

---

## 7. Cascade Intelligence / Macro Signal Convergence

**Pitch:** "Catch a sector turning before it shows up in your weekly research read."

**What it is:** Multi-signal convergence engine that aggregates macro indicators (FRED, BEA, BLS), sector-specific signals (CFTC COT, USDA, EIA, port throughput, FRA rail loadings, prediction markets via Kalshi/Polymarket), and corporate signals (SEC filings, FDIC bank data, USPTO patents) into convergence scores. Visualizes when a sector's signals start moving together.

**Who buys:** Macro hedge funds doing sector trades. PE that needs to time sector entry/exit. Corporate strategy teams forecasting their own market.

**The problem we solve:** Sector inflections are usually visible 3–6 months early in cross-source signals — but only if you're already pulling the right 12 signals into one chart. Most teams don't. We do.

**Why Perplexity can't:** This is signal density math against historical data. A one-shot LLM cannot calibrate "these 12 signals together predicted last 4 sector inflections." We have the historical store, the convergence model, and the visualization layer.

**Demo:** Pick a sector that's currently inflecting (or did 6 months ago). Replay our cascade dashboard at the inflection point — show the signals we lit up and how early.

**Pricing wedge:** $40K seat × small team. $80–120K firm license. Hedge fund tier with custom signals $150K+.

---

## 8. Governed PE Data Warehouse (the Compliance Story)

**Pitch:** "AI agents hallucinate. We don't. Here's the audit trail."

**What it is:** The full Nexdata stack as an enterprise warehouse — REST + GraphQL APIs, lineage tracking (`Lineage Service`: nodes, edges, events, dataset versions, impact analysis), audit service (every modification with user/timestamp), provenance tagging, data quality rules (range, null, freshness, regex, custom SQL), cross-source validation, quality trending. SSO + RBAC available.

**Who buys:** Large LPs (pension funds, sovereign wealth, foundations). Compliance teams at PE firms. IR teams that need defensible numbers in LP reports. Anyone with a regulator who asks "where did this number come from?"

**The problem we solve:** "An AI told us" is not an answer when the SEC, an LP advisory committee, or a fund auditor asks. Numbers in IC memos and LP reports need source attribution, version history, and reproducibility. Computer cannot provide any of those things by design.

**Why Perplexity can't:** Computer is non-deterministic. Run the same prompt twice, get different numbers. There is no lineage from a chart in a deck back to a row in a database back to a source citation back to a quality rule that validated it. Nexdata is engineered for exactly that chain.

**Demo:** Pull a number off a portfolio report. Click through: source row → ingestion job → lineage event → upstream source citation → quality rule that passed. End-to-end audit in 5 clicks.

**Pricing wedge:** This is the upsell path. $75K base seat + $200K enterprise warehouse + custom rules. The compliance story is the wedge that lets you charge enterprise prices.

---

## 9. Reports-as-a-Service

**Pitch:** "We run the data team you can't afford."

**What it is:** 14 production report templates (PE deal memo, portfolio report, fund tearsheet, market brief, data quality report, macro sector brief, investor profile, jobs monitor, MedSpa market, MedSpa opportunity map, datacenter site, portfolio detail, custom). HTML + PDF + XLSX + PPTX. Branded, repeatable, scheduled. Self-contained interactive reports with Chart.js.

**Who buys:** Small PE shops with no analyst team. Independent sponsors. IR teams generating monthly LP letters. Service firms (legal, accounting) producing client-facing reports.

**The problem we solve:** A small PE shop can't justify a $400K data analyst + $200K data engineer + $200K platform. We bundle the output of all three for a fraction.

**Why Perplexity can't:** Computer generates one-shot reports from a prompt. They look fine but the numbers behind them are wherever the model decided to look that day. Our reports come from the warehouse — same number every run, branded, on schedule.

**Demo:** Generate a PE deal memo from a real target name. Show the same template run for 3 different targets in 90 seconds. Show last month's version vs this month's — the diff.

**Pricing wedge:** $10K/mo for 50 reports/mo. $25K/mo for unlimited + branding. $40K+ for custom templates.

---

## 10. Bespoke Build

**Pitch:** "You have a thesis. We have the platform to operationalize it in 6 weeks."

**What it is:** Anything in this doc, customized. New roll-up vertical. New site-suitability domain. New scoring model. White-label deployment. Custom integrations. We've built 9 collector domains, 47 collectors, 9 scoring engines — adding the 48th and 10th is what we do.

**Who buys:** Anyone with a niche thesis and capital but no platform team.

**Pricing wedge:** $100K minimum, scoped per project. Recurring license on top.

---

## Bundle Strategy

| Bundle | Mix | Target | ACV |
|---|---|---|---|
| **Sourcing Pack** | Deal Probability + Org Chart + Reports | LMM PE associates | $80–150K |
| **Site Pack** | Site Intel + Data Center + Reports | Industrial PE / infra | $200–350K |
| **Fundraising Pack** | LP Intel + Family Office + Reports | GPs raising | $100–200K |
| **Roll-up Pack** | Target Screener + Org Chart + Probability | Roll-up PE / search funds | $120–200K |
| **Enterprise Warehouse** | All of the above + governance + SSO | Large LPs, regulated funds | $300–500K+ |

---

## Objection Handling: "Why Not Just Use Perplexity Computer?"

**"Computer can do research."**
> "Computer can browse. It can't pull EIA real-time grid data, FERC filings, ISO interconnect queues, or Form 990 extracts in any structured way. And tomorrow when you run the same query, you'll get different numbers. Show me a Computer-generated PE deal memo and I'll show you 6 numbers that are wrong or unverifiable."

**"It's $200/mo, you're $80K/yr."**
> "Sure — and at $200/mo per seat × 12 analysts × unpredictable credit burn, you're at $50–80K already. Except you have nothing persisted. We're a warehouse. The data doesn't evaporate when the task ends. Tomorrow's analyst inherits today's analyst's work."

**"Can't I just have my analyst use Computer?"**
> "Yes — and they should, for one-off horizontal tasks. We are not the right tool for booking flights or drafting emails. But the moment you need a number you can put in front of an LP, the IC, or a regulator, Computer can't help and we're the only option."

**"How do I know your data is right?"**
> "Every row has a source citation, a quality-rule pass record, and a lineage trail back to the upstream API. Click any number in a report, see the chain. We will literally walk you through it on the demo. Computer cannot do this — it is architecturally incapable."

**"What if Perplexity adds private-markets data?"**
> "They might. The day they buy AlphaSense or Tegus, this conversation gets harder. Until then, they have 400 OAuth connectors and zero of them are private-markets data providers. We are betting they remain horizontal — and we're building the connector that makes us their *source* of PE data, not their competitor."

---

## Channel Play: Be Their Connector

Computer has 400+ OAuth connectors. None are private-markets data providers. **Build a Nexdata MCP server** so Computer users can query our warehouse from Perplexity's UX. This converts a competitor into a distribution channel:

- Computer users hit our rate-limited endpoint instead of free-form web crawl
- We get usage telemetry and a low-cost top-of-funnel
- Heavy users hit credit ceilings and convert to direct Nexdata licenses
- Engineering lift: small — we have GraphQL + REST already. MCP wrapper is a week of work.

This is a 2026-Q3 priority if the win-rate against Computer-using prospects degrades.

---

## What to Do With This Doc

1. **Pick 1 capability and pitch it.** Don't lead with the platform. Lead with the wedge.
2. **Site Intel + Roll-up Screener + Deal Probability are the three sharpest first-touches.** They have the most defensible moat against Computer and the most measurable ROI for the buyer.
3. **Use the audit/governance angle as the upsell wedge.** Every buyer who lands on capability 1–7 should be re-pitched on capability 8 within 90 days.
4. **Reports-as-a-Service is the small-fish play.** Not strategic, but it covers fixed costs and seeds bigger deals.
5. **Don't ever pitch "AI agent that does research."** That is Computer's pitch. Ours is "governed vertical warehouse + scoring + reports."

---

## Sources

- Internal: `docs/strategy/PERPLEXITY_COMPUTER_COMPARISON_2026.md` (analytical companion)
- Internal: `docs/strategy/PE_PRICING_AND_GTM_RESEARCH.md`
- Internal: `docs/strategy/CUSTOMER_TARGET_PLAYBOOK_2026.md`
- Internal: `docs/strategy/COMPETITIVE_LANDSCAPE_2026.md`
- Internal repo capability scan: `app/sources/`, `app/services/`, `app/reports/`, `app/core/models_*.py`
