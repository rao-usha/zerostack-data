# Nexdata — Product State of the Union
> Last Updated: 2026-03-23 | Reviewed file-by-file across all modules

---

## Executive Summary

Nexdata is a FastAPI-based investment intelligence platform that ingests 28+ public data sources, runs agentic AI research pipelines, and provides a full PE Intelligence suite targeting mid-market private equity firms.

**Scale:**
- 689 Python source files across 44 modules
- 142 REST API endpoints + GraphQL layer
- 25 PE-specific database tables, 30+ core tables, 20+ site intel tables
- 1,544 test functions across 117 test files
- ~300K total lines of code

**Stack:** Python 3.11, FastAPI, SQLAlchemy 2.0, PostgreSQL 14, httpx async, APScheduler, OpenAI/Anthropic LLMs, Docker

---

## 1. Core Infrastructure

### `app/core/database.py`
- Singleton engine + session factory (QueuePool: size=5, overflow=10)
- Lazy httpx client initialization
- **Status:** ✅ Fully working

### `app/core/config.py` (34KB)
- Pydantic settings with 20+ optional API key configs
- All keys optional at startup — validated at ingestion time
- Multi-environment support (.env.local, .env.cloud, .env.test)
- **Status:** ✅ Fully working

### `app/core/http_client.py` (18KB) — `BaseAPIClient`
- Async httpx with bounded concurrency (`asyncio.Semaphore`)
- Exponential backoff + jitter (factor=2.0, max=60s)
- Per-source rate limiting (1–2 req/sec enforced)
- Connection pooling with keep-alive
- **Status:** ✅ Mature — used by all 28+ source clients

### `app/core/api_errors.py`
- `APIError → RetryableError | RateLimitError | FatalError | AuthenticationError | NotFoundError`
- Each error carries source, status_code, response_data
- **Status:** ✅ Complete, used consistently

### `app/core/ingest_base.py` (14KB) — `BaseSourceIngestor`
- `prepare_table()`, `_update_dataset_registry()`, `batch_insert()`
- All 28+ ingestors inherit this
- **Status:** ✅ Fully functional

### `app/core/batch_operations.py` (12KB)
- `bulk_upsert()` — overwrites ALL columns including nulls (⚠️ dangerous for enrichment)
- `null_preserving_upsert()` — COALESCE-safe for incremental enrichment
- `create_table_if_not_exists()` — dynamic table creation
- **Status:** ✅ Mature, critical path

### `app/core/models.py` (3,707 lines)
Key tables: `ingestion_jobs`, `dataset_registry`, `ingestion_schedules`, `job_queue`, `rate_limit_bucket`
- **Status:** ✅ All created on startup via `create_all()`
- ⚠️ **Tech debt:** No migration framework — schema changes require manual SQL

---

## 2. Data Sources (44 modules under `app/sources/`)

### Economic & Government Data

| Source | File | Status | API Key? | Coverage |
|--------|------|--------|----------|----------|
| FRED | `sources/fred/` | ✅ Working | Required | 800K+ macroeconomic series |
| BLS | `sources/bls/` | ✅ Working | Required | Employment, CPI, PPI, wages |
| EIA | `sources/eia/` | ✅ Working | Required | Energy prices, production, consumption |
| BEA | `sources/bea/` | ✅ Working | Required | GDP, personal income, accounts |
| Census | `sources/census/` | ✅ Working | Required | ACS demographics, bulk downloads |
| Treasury | `sources/treasury/` | ✅ Working | None | Interest rates, yields, debt |
| SEC | `sources/sec/` | ✅ Working | None | 10-K/10-Q/8-K/13F/Form D/ADV |
| CFTC COT | `sources/cftc/` | ✅ Working | None | Commitment of Traders reports |
| IRS SOI | `sources/irs/` | ✅ Working | None | Tax statistics |
| FEMA | `sources/fema/` | ✅ Working | None | Disaster declarations, NFIP |
| FBI Crime | `sources/fbi/` | ✅ Working | Data.gov key | UCR crime statistics |
| FCC | `sources/fcc/` | ✅ Working | None | Broadband availability |
| FDA | `sources/fda/` | ✅ Working | None | Drug approvals, recalls |
| OSHA | `sources/osha/` | ✅ Working | None | Safety violations |
| EPA ECHO | `sources/epa/` | ✅ Working | None | Environmental violations |
| US Trade | `sources/us_trade/` | ✅ Working | None | Import/export by country |
| USDA | `sources/usda/` | ✅ Working | None | Agricultural data |
| NOAA | `sources/noaa/` | ✅ Working | Optional | Weather, climate, sea level |
| Kaggle | `sources/kaggle/` | ✅ Working | Required | 10K+ public datasets |
| OpenCorporates | `sources/opencorporates/` | ✅ Working | Optional | 200M+ company registries |
| Data Commons | `sources/data_commons/` | ✅ Working | Optional | Google knowledge graph |
| USASpending | `sources/usaspending/` | ✅ Working | None | Federal contracts, grants |
| SAM.gov | `sources/sam_gov/` | ✅ Working | Required | Federal procurement |
| CourtListener | `sources/courtlistener/` | ✅ Working | Optional | Court filings |
| FDIC | `sources/fdic/` | ✅ Working | None | Bank data |
| Patent (USPTO) | `sources/uspatent/` | ✅ Working | None | Patent filings |

### Agentic / Collection Pipelines

| Source | File | Status | Notes |
|--------|------|--------|-------|
| PE Collection | `sources/pe/` | ✅ Working | Demo seeder + real SEC/web collection |
| People Collection | `sources/people_collection/` | ✅ Working | 4-phase deep collection, 142+ companies |
| LP Collection | `sources/lp_collection/` | 🟡 Partial | Limited public data availability |
| Family Office | `sources/family_office/` | 🟡 Partial | 308 FOs tracked, enrichment incomplete |
| Job Postings | `sources/job_postings/` | ✅ Working | 5 ATS platforms, 67 companies, 13K+ postings |
| Foot Traffic | `sources/foot_traffic/` | 🟡 Partial | Requires Placer/Foursquare/SafeGraph keys |
| Web Traffic | `sources/web_traffic/` | ✅ Working | SimilarWeb-compatible endpoints |
| Glassdoor | `sources/glassdoor/` | 🟡 Partial | Rate-limited web scraping |
| GitHub | `sources/github/` | ✅ Working | 293 repos monitored |
| App Stores | `sources/app_stores/` | 🟡 Stubbed | Basic structure, limited data |
| News | `sources/news/` | ✅ Working | 398+ items, multiple sources |
| DUNL | `sources/dunl/` | 🟡 Partial | D&B-style data, limited coverage |
| Yelp | `sources/yelp/` | ✅ Working | Business ratings, requires key |
| Real Estate | `sources/realestate/` | ✅ Working | Property data |

### Site Intelligence Collectors (`app/sources/site_intel/`)

All collectors use `@register_collector(SiteIntelSource.XXX)` decorator pattern. Modules must be imported to register — done via domain `__init__.py` files.

| Domain | Status | Sources |
|--------|--------|---------|
| Power | ✅ Working | EIA plant DB, HIFLD substations, ISO interconnection queues |
| Logistics | ✅ Working | Freightos, USDA trucking rates, FMCSA carriers, port throughput |
| Labor | 🟡 Partial | Job postings velocity, skills demand |
| Risk | 🟡 Partial | FEMA disasters, supply chain disruptions |
| Telecom | 🟡 Partial | FCC cell towers, fiber coverage |
| Transport | 🟡 Partial | Port stats, airport traffic, rail networks |
| Water Utilities | 🟡 Partial | EPA SDWIS (API reorganized — endpoint 404s) |
| Incentives | 🟡 Partial | Tax incentive databases |
| Scoring | 🟡 Partial | Composite risk scoring |

⚠️ **Known broken collectors:** HIFLD Substations (invalid ArcGIS URL), EPA SDWIS (API reorganized), EIA Gas (400 error), OpenEI URDB (403 forbidden)

---

## 3. PE Intelligence Platform

### Database (`app/core/pe_models.py`, 1,185 lines — 19 tables)

**Firm & Fund:**
- `pe_firms` — GP profiles (AUM, CIK, SEC registration, investment criteria, strategies)
- `pe_funds` — Fund vehicles (vintage, target/close size, terms: mgmt fee, carry, hurdle)
- `pe_fund_performance` — Quarterly snapshots (IRR, TVPI, DPI, RVPI, NAV)
- `pe_cash_flows` — Full ledger (capital calls, distributions, fees, dates)

**Portfolio Companies:**
- `pe_portfolio_companies` — Company master (industry, HQ, employees, revenue, ownership status)
- `pe_fund_investments` — Fund→Company links (entry date, entry valuation, exit info)
- `pe_company_financials` — Time-series P&L + balance sheet (revenue, EBITDA, margins, FCF)
- `pe_company_valuations` — Point-in-time valuations (entry/mark/exit multiples, methodology)
- `pe_company_leadership` — Executive roster (title, tenure, background, PE-appointed flag)
- `pe_competitor_mappings` — Competitive landscape (peer companies, market position)

**Deals:**
- `pe_deals` — M&A transactions (type, structure, EV, multiples, status)
- `pe_deal_participants` — Co-investors (equity contribution, fund used, role)
- `pe_deal_advisors` — Advisors by side (legal, financial, accounting)

**People:**
- `pe_people` — Person master (name, current role, contact, social)
- `pe_person_education` / `pe_person_experience` — Background records
- `pe_firm_people` — GP team directory (seniority, sectors, tenure)
- `pe_deal_person_involvement` — Deal team tracking

**Monitoring:**
- `pe_alerts` — Generated alerts (type, severity, title, detail)
- `pe_alert_subscriptions` — Webhook subscriptions per firm

### API Endpoints

**`app/api/v1/pe_firms.py`**
- `GET /pe/firms/` — List with filters (type, strategy, status, search, AUM range)
- `GET /pe/firms/search?q=` — Name search (ILIKE, ranked by AUM)
- `GET /pe/firms/{id}` — Detail + fund/company counts
- `GET /pe/firms/{id}/portfolio` — Portfolio companies with exit data
- `GET /pe/firms/{id}/funds` — All funds with terms & performance snapshots
- `GET /pe/firms/{id}/team` — Team members sorted by seniority
- `GET /pe/firms/stats/overview` — DB-wide statistics
- **Status:** ✅ Fully working

**`app/api/v1/pe_companies.py`**
- `GET /pe/companies/` — List with filters (industry, status, owner, search)
- `GET /pe/companies/{id}` — Full profile
- `GET /pe/companies/{id}/financials` — Multi-year time-series
- `GET /pe/companies/{id}/valuations` — Valuation history with multiples
- `GET /pe/companies/{id}/leadership` — Executive team with PE-appointed flags
- `GET /pe/companies/{id}/competitors` — Competitive landscape
- `GET /pe/companies/{id}/news` — Recent news
- `GET /pe/companies/{id}/potential-buyers` — Exit buyer analysis (fit scores)
- `GET /pe/data-room/{id}` — Due diligence readiness (% completeness)
- **Status:** ✅ Fully working

**`app/api/v1/pe_deals.py`**
- `GET /pe/deals/` — List with filters (type, status, buyer, year, min_ev)
- `GET /pe/deals/search` — Search by company/deal name
- `GET /pe/deals/{id}` — Full deal detail (participants, advisors, team)
- `GET /pe/deals/stats/overview` — Counts by type, avg multiples
- `GET /pe/deals/activity/recent` — 30-day activity window
- **Status:** ✅ Fully working

**`app/api/v1/pe_benchmarks.py`** (flagship analytics)
- `POST /pe/seed-demo` — Seed 3 firms, 6 funds, 24 companies, financials, people, deals. Returns `firm_id`.
- `DELETE /pe/seed-demo` — Clean up all demo data (idempotent)
- `GET /pe/benchmarks/{company_id}` — Company vs peer group (6 metrics, percentile ranks)
- `GET /pe/benchmarks/portfolio/{firm_id}` — Portfolio heatmap (all companies)
- `GET /pe/exit-readiness/{company_id}` — Composite 0-100 score (6 dimensions)
- `GET /pe/leadership-graph/{firm_id}` — D3 force graph data
- `GET /pe/buyer-analysis/{company_id}` — Strategic + financial buyers with fit scores
- `GET /pe/data-room/{company_id}` — Data room completeness
- `GET /pe/analytics/{firm_id}/performance` — Firm IRR, TVPI, concentration
- `GET /pe/analytics/{firm_id}/vintage` — Vintage year analysis
- `GET /pe/analytics/{firm_id}/pme` — Public Market Equivalent scoring
- `GET /pe/analytics/{firm_id}/risk` — Concentration & key-person risk
- `POST /pe/companies/{id}/thesis/refresh` — AI investment thesis (LLM, cached)
- **Status:** ✅ Fully working

**`app/api/v1/pe_people.py`**
- Full CRUD for PE persons, experience, education, deal involvement
- **Status:** ✅ Working

**`app/api/v1/pe_import.py`**
- 4 import templates (firms, funds, companies, deals)
- Upload CSV/Excel → preview → execute → rollback
- **Status:** ✅ Working

### Analytics Services

| File | Purpose | Status |
|------|---------|--------|
| `app/core/pe_benchmarking.py` | Percentile/quartile comparison vs industry peers | ✅ Working |
| `app/core/pe_exit_scoring.py` (22KB) | 6-dimension exit readiness (Financial, Market, Team, Ops, Governance, Timing) | ✅ Working |
| `app/core/pe_portfolio_analytics.py` (22KB) | Firm-level IRR, TVPI, DPI, concentration, PME, vintage | ✅ Working |
| `app/core/pe_deal_sourcing.py` (11KB) | Acquisition target discovery + scoring | ✅ Working |
| `app/core/pe_market_scanner.py` (17KB) | Sector momentum, deal flow trends | ✅ Working |
| `app/core/pe_deal_scorer.py` (21KB) | 8-point M&A opportunity rubric | ✅ Working |
| `app/core/pe_thesis_generator.py` | LLM investment thesis (cached, cost tracked) | ✅ Working |
| `app/core/pe_portfolio_monitor.py` | Change detection, alert generation | 🟡 Partial |
| `app/core/pe_rollup_screener.py` | Add-on acquisition screening | 🟡 Partial |

### Demo Seeder (`app/sources/pe/demo_seeder.py`, 2,033 lines)

**3 Demo Firms:**
- Summit Ridge Partners (Buyout, Healthcare/Tech, $4.2B AUM) — primary demo firm
- Cascade Growth Equity (Growth, Software/Fintech, $2.8B AUM)
- Ironforge Industrial Capital (Industrial, Manufacturing, $3.5B AUM)

**24 Portfolio Companies:** 8 per firm across invested sectors. Summit Ridge companies:
- MedVantage Health Systems (Healthcare IT, Nashville, 420 employees)
- Apex Revenue Solutions (Revenue Cycle, Atlanta, 280 employees)
- CloudShield Security (Cybersecurity, Austin, 195 employees)
- TrueNorth Behavioral (Behavioral Health, Denver, 850 employees)
- Precision Lab Diagnostics (Clinical Labs, Phoenix, 340 employees)
- Elevate Staffing Group (Healthcare Staffing, Charlotte, 160 employees)

**What's seeded:** Firms → Funds (6) → Fund Performance (quarterly IRR/TVPI) → Cash Flows → Companies → Financial Time-Series (FY2022-2024) → Valuations → Leadership Teams (10-30 per company) → Deals (~20 M&A transactions) → Competitors → Alerts + Subscriptions

### Demo Readiness: PE
| Feature | Status | Notes |
|---------|--------|-------|
| Portfolio heatmap | 🟢 Ready | Seed then call `/pe/benchmarks/portfolio/{firm_id}` |
| Exit readiness scoring | 🟢 Ready | Returns 0-100 composite, 6 sub-scores, recommendations |
| Buyer universe | 🟢 Ready | Strategic + financial fit scores |
| Data room readiness | 🟢 Ready | % completeness with gap list |
| Leadership network graph | 🟢 Ready | D3 force simulation, drag/zoom |
| Fund performance (IRR/TVPI) | 🟢 Ready | Per fund, multi-vintage |
| AI investment thesis | 🟢 Ready | Requires OPENAI_API_KEY or ANTHROPIC_API_KEY |
| Deal search + activity | 🟢 Ready | Filters by type, year, buyer, EV |
| Web demo (pe-demo.html) | 🟢 Ready | 4-step interactive web UI, auto-seeds |

---

## 4. People & Org Chart Intelligence

### Database (`app/core/people_models.py`, 26KB — 12 tables)
- `people` — Individual profiles (name, title, contact, bio, confidence)
- `company_people` — Employment records with tenure
- `people_collection_jobs` — Job lifecycle for collection pipeline
- `org_chart_snapshots` — Point-in-time org structures
- `people_changes` — Change log (title, departure, new hire)
- `industrial_companies` — Target company directory

### Collection Pipeline
Three-agent architecture:
1. **PageFinder** — 146 URL patterns + IR subdomain + sitemap + homepage crawl + DuckDuckGo (blocked in Docker)
2. **WebsiteAgent** — CSS selector extraction first; GPT-4-turbo LLM fallback if < 3 people found
3. **DeepCollector** — 4-phase: SEC EDGAR 10-K → Website deep crawl (BFS) → News scan → Org chart build (4-pass LLM)

Rate limiting: 0.5 req/sec per domain, 10s timeout for URL checks

### API (`app/api/v1/people_jobs.py`)
- `POST /people-jobs/test/{company_id}?sources=website` — Test collection on one company
- `POST /people-jobs/deep-collect/{company_id}` — Full 4-phase deep collection
- `GET /people-jobs/metrics` — Collection metrics by agent
- `GET /people-jobs/alerts` — Change alerts (C-suite moves, departures)
- `GET /people-jobs/digest` — Weekly summary digest

### Known Stats
- 142+ companies tracked
- Prudential: 37 people, 14 departments, ~3.6 min collection time
- Deep collection: ~3-4 min per company

### Demo Readiness: People
| Feature | Status | Notes |
|---------|--------|-------|
| Basic collection | 🟢 Ready | Real data from company websites |
| Org chart build | 🟢 Ready | LLM-assembled hierarchy |
| Change alerts | 🟢 Ready | Title changes, departures detected |
| Deep collection | 🟡 Slow | Works but 3-4 min per company |
| JS-rendered sites | 🔴 Off | ENABLE_PLAYWRIGHT=0 by default (+500MB) |
| DuckDuckGo search | 🔴 Blocked | IP-blocked in Docker |

---

## 5. Site Intelligence Platform

### Architecture
Decorator-based collector registry. All collectors inherit `BaseCollector` and register via `@register_collector(SiteIntelSource.XXX)`. Domain `__init__.py` files import all collector modules to trigger registration.

### Database (`app/core/models_site_intel.py`, 65KB — 20+ tables)
Power: `power_plants`, `substations`, `utility_territory`, `renewable_resources`, `interconnection_queue`
Logistics: `container_freight_index`, `trucking_lane_rates`, `motor_carriers`, `warehouse_facilities`, `three_pl_companies`
Labor: `job_postings`, `skill_demand`
Risk: `disaster_events`, `supply_chain_risks`
Telecom: `cell_towers`, `fiber_nodes`
Transport: `ports`, `airports`, `rail_networks`
Water: `water_systems`, `discharge_permits`

### API Endpoints

**Power** (`app/api/v1/site_intel_power.py`) — ✅ Working
- `GET /site-intel/power/plants` — Search by state, fuel type, capacity threshold
- `GET /site-intel/power/plants/nearby` — Geospatial radius search
- `GET /site-intel/power/substations` — Substation data (⚠️ HIFLD URL broken)
- `GET /site-intel/power/utilities` — Utility territories + rates
- `GET /site-intel/power/interconnection-queue` — Renewable pipeline

**Logistics** (`app/api/v1/site_intel_logistics.py`) — ✅ Working
- `GET /site-intel/logistics/container-rates` — Freightos, Drewry, SCFI indices
- `GET /site-intel/logistics/trucking-rates` — USDA spot rates by lane
- `GET /site-intel/logistics/motor-carriers` — FMCSA carrier DB
- `GET /site-intel/logistics/warehouses` — 3PL company directory
- `GET /site-intel/logistics/port-throughput` — Port volume statistics

**Labor** (`app/api/v1/site_intel_labor.py`) — 🟡 Partial
**Risk** (`app/api/v1/site_intel_risk.py`) — 🟡 Partial
**Telecom** (`app/api/v1/site_intel_telecom.py`) — 🟡 Partial
**Transport** (`app/api/v1/site_intel_transport.py`) — 🟡 Partial
**Water** (`app/api/v1/site_intel_water_utilities.py`) — 🔴 API broken (EPA SDWIS reorganized)
**Incentives** (`app/api/v1/site_intel_incentives.py`) — 🟡 Partial

⚠️ **Broken collectors:** HIFLD Substations (invalid ArcGIS URL), EPA SDWIS (404), EIA Gas (400), OpenEI URDB (403)

---

## 6. Job Queue & Distributed Workers

### Architecture
**`app/worker/main.py`** (17KB) — Standalone Python process (`python -m app.worker.main`)
- `SELECT FOR UPDATE SKIP LOCKED` — atomic job claiming, prevents duplicates
- Routes to 8 executors by `job_type`
- 30s heartbeat loop — stale detection catches dead workers
- Graceful shutdown: SIGTERM → 30s drain → exit
- `WORKER_MODE=1` (queue), `WORKER_MODE=0` (FastAPI BackgroundTasks fallback)

### Executors (`app/worker/executors/`)
`ingestion.py`, `people.py`, `site_intel.py`, `pe.py`, `agentic.py`, `lp.py`, `fo.py`, `foot_traffic.py`
— All **✅ Working**

### Job Lifecycle
`pending → running → success | failed`
- Retry logic with max_retries + next_retry_at
- Parent-child job chaining
- Batch run tracking (batch_run_id, trigger, tier)
- Auto-cancel after 4 hours pending
- Dead worker recovery: jobs reset to PENDING every 5 min

### Demo Readiness
- 🟢 Ready — Worker starts with `docker-compose up -d worker`
- Without workers: jobs sit PENDING indefinitely
- Scale: `docker-compose up -d --scale worker=4`

---

## 7. Agentic / AI Layer

### LLM Client (`app/agentic/llm_client.py`, 13KB)
- OpenAI: gpt-4o ($2.50/$10/1M tokens), gpt-4-turbo, gpt-3.5-turbo
- Anthropic: claude-3-5-sonnet ($3/$15/1M tokens), claude-3-5-haiku
- Token counting, cost tracking, JSON output parsing
- Retry + exponential backoff
- **Status:** ✅ Fully working

### Portfolio Agent (`app/agentic/portfolio_agent.py`, 29KB)
4 phases: Company overview → Competitive analysis → Team analysis → Investment thesis synthesis
Strategy pattern: pluggable collectors per phase
**Status:** ✅ Working (requires API key)

### Collection Strategies (`app/agentic/strategies/`)
| Strategy | File | Purpose | Status |
|----------|------|---------|--------|
| SEC 13F | `sec_13f_strategy.py` | Parse investor holdings | ✅ Working |
| Website | `website_strategy.py` | Structured HTML + LLM fallback | ✅ Working |
| Annual Report | `annual_report_strategy.py` | Financial document parsing | ✅ Working |
| News | `news_strategy.py` | News aggregation | ✅ Working |
| Reverse Search | `reverse_search_strategy.py` | People image lookup | ✅ Working |

### AI Agents (`app/agents/`)
| Agent | File | Purpose | Status |
|-------|------|---------|--------|
| Due Diligence | `due_diligence.py` | Risk scoring, red flags, IC memo | ✅ Working |
| Company Researcher | `company_researcher.py` | Deep company analysis | ✅ Working |
| Market Scanner | `market_scanner.py` | Sector momentum | ✅ Working |
| Competitive Intel | `competitive_intel.py` | Moat assessment | ✅ Working |
| News Monitor | `news_monitor.py` | Watchlists, digests | ✅ Working |
| Anomaly Detector | `anomaly_detector.py` | Statistical outliers | ✅ Working |
| Report Writer | `report_writer.py` | Markdown → structured report | ✅ Working |
| Deep Researcher | `deep_researcher.py` | Multi-source synthesis | 🟡 Partial |

### Entity Resolution (`app/core/entity_resolver.py`, 42KB)
Fuzzy matching for company/person deduplication across sources
**Status:** ✅ Fully working

---

## 8. Analytics & Scoring

### Company Health Score (`app/api/v1/health_scores.py`)
- Composite 0-100 score, A–F tier, 4 categories
- Confidence percentage
- **Status:** ✅ Working

### Benchmarks (`app/analytics/benchmarks.py` + `app/api/v1/benchmarks.py`)
- Company vs peer comparison
- Industry percentiles
- **Status:** ✅ Working

### Predictions (`app/api/v1/predictions.py` / `deal_models.py`)
- Deal win probability
- Pipeline insights
- **Status:** ✅ Working

### Data Quality Analytics (`app/core/data_quality_service.py`, 46KB)
- Completeness, freshness, schema validation, duplicate detection, anomaly scoring
- **Status:** ✅ Fully working

### Scheduler (`app/core/scheduler_service.py`, 1,673 lines)
APScheduler with 10+ background jobs (cleanup, retry, freshness, DQ snapshots, cross-source validation)
**Status:** ✅ Working

---

## 9. Report Generation

### Stack (`app/reports/`)
| File | Size | Purpose | Status |
|------|------|---------|--------|
| `builder.py` | 12KB | Orchestration (data → analysis → layout) | ✅ Working |
| `design_system.py` | 48KB | Consistent theme, colors, fonts | ✅ Mature |
| `pdf_renderer.py` | 21KB | HTML → PDF via WeasyPrint | ✅ Working |
| `pptx_renderer.py` | 44KB | PPTX via python-pptx | ✅ Working |

### Report Types (6)
`company_profile`, `competitive_landscape`, `market_brief`, `due_diligence_memo`, `fund_tearsheet`, `investor_memo`

### API
`POST /api/v1/reports-gen/generate` → async job → `GET /api/v1/reports-gen/{id}` → HTML/PDF/PPTX
**Status:** ✅ Working end-to-end

---

## 10. Authentication

### `app/auth/api_keys.py` + `app/api/v1/auth.py`
- JWT tokens (PyJWT) with SECRET_KEY
- API keys with rate limiting per key
- Roles: admin, analyst, viewer
- ⚠️ Auth is optional by default (no enforcement if JWT_SECRET_KEY unset)
- ⚠️ CORS allows all origins — no origin restriction

**Status:** ✅ Framework complete, 🟡 not enforced by default

---

## 11. Search

### `app/search/` + `app/api/v1/search.py`
- Full-text search across companies, people, datasets
- Faceted filtering (type, industry, source)
- Fuzzy matching via entity resolver
- **Status:** ✅ Working

---

## 12. Deal Pipeline (`app/deals/`)

### `app/deals/tracker.py`
- 7-stage pipeline CRM (sourcing → IC → LOI → DD → legal → closing → portfolio)
- Activity logging
- Module-level `_tables_ensured` flag (fixed 500 error from InsufficientPrivilege on CREATE INDEX)
- **Status:** ✅ Working (fix applied 2026-03-17)

### `app/api/v1/deals.py`
- Full CRUD, stage transitions, priority scoring
- **Status:** ✅ Working

---

## 13. Frontend

### `frontend/index.html` (14,166 lines)
- Vanilla JS + CSS, no build step
- Dark mode (`--bg: #0f172a` design system)
- Multi-tab navigation with sidebar
- Login overlay with JWT auth
- Real-time job queue monitoring (SSE stream)
- Data sources dashboard (health, alerts, freshness indicators)
- PE Intelligence tabs (4 sections added recently)
- D3 visualizations embedded via `<script src="d3/sankey.html">` iframes
- ⚠️ **14K lines of HTML is a major maintainability problem**
- **Status:** 🟡 Functional for monitoring, needs refactor

### `frontend/pe-demo.html` (671 lines)
- Standalone PE demo: 4-step interactive flow
- Step 1: Portfolio heatmap table (clickable companies)
- Step 2: Radar chart benchmarks + exit readiness gauge
- Step 3: Buyer table + data room completeness
- Step 4: D3 force graph (leadership network)
- Auto-seeds on first load via `POST /pe/seed-demo`
- Dynamic `_firmId` (fixed 2026-03-23 — was hardcoded 166)
- **Status:** ✅ Ready for demo

### `frontend/d3/` (6 files)
| File | Purpose | Status |
|------|---------|--------|
| `sankey.html` | Portfolio flow visualization | ✅ Working |
| `orgchart.html` | Org hierarchy | ✅ Working |
| `network.html` | Entity relationships | ✅ Working |
| `map.html` | Geographic mapping | ✅ Working |
| `radar.html` | Multi-dimensional scoring | ✅ Working |
| `index.html` | D3 demo index | ✅ Working |

---

## 14. Infrastructure & Deployment

### Docker (`Dockerfile`, 71 lines)
- Python 3.11-slim base
- `uv` package manager (fast installs)
- Non-root `appuser` for security
- Build arg `INSTALL_BROWSERS=1` adds Chromium for Playwright (+500MB)
- **Status:** ✅ Optimized

### Docker Compose (`docker-compose.yml`)
Services:
- `postgres:14-alpine` (port 5434 host → 5432 container)
- `api` (port 8001 host → 8000 container), volume-mounted `./app`
- `worker` (WORKER_MODE=1, WORKER_MAX_CONCURRENT=6)
- `cloudsqlproxy` (profile: cloud, port 5435)

Environments:
- `.env.local` — Local Docker Postgres
- `.env.cloud` — GCP Cloud SQL (`nexdata-cloud:us-central1:nexdata-pg`, db-f1-micro, 1.6GB, 388 tables)
- `.env.test` — Test isolation
- `switch-env.sh` — Quick env switcher

**Cloud SQL:** Postgres 14, db-f1-micro ($7/mo), daily backups 4am UTC, 7-day retention, PITR enabled, deletion protection on. Authorized: alexiusmichael@gmail.com (owner), usharao13@gmail.com (client)

**Status:** ✅ All environments working

### CI/CD (`.github/workflows/`)
- Ruff linting (non-blocking, `continue-on-error: true`)
- No automated deployment pipeline yet
- **Status:** 🟡 Lint only

### Dependencies (`requirements.txt`, 70 lines)
Core: `fastapi, uvicorn, sqlalchemy, psycopg2-binary, httpx, tenacity`
Data: `pandas, openpyxl, yfinance, pdfplumber`
AI: `openai, anthropic`
Scraping: `beautifulsoup4, lxml, robotexclusionrulesparser`
Geo: `geoalchemy2, shapely`
Reports: `weasyprint, python-pptx`
Auth: `PyJWT, bcrypt`
GraphQL: `strawberry-graphql`
**Status:** ✅ All pinned, working

---

## 15. Scripts & Utilities

### Data Seeding
| Script | Purpose | Status |
|--------|---------|--------|
| `scripts/seed_pe_financials.py` | 10 companies: ServiceTitan, SailPoint, Medline, etc. | ✅ Working |
| `scripts/build_demo_dataset.py` | 98 PE firms, 742 deals, $8.4T AUM | ✅ Working |
| `scripts/seed_demo_data.py` | 12 deals, company scores (Stripe 69.9, OpenAI 69.8) | ✅ Working |
| `scripts/seed_ats_companies.py` | 65 companies, 13,271 job postings | ✅ Working |
| `scripts/populate_demo_data.py` | 20 government sources | ✅ Working |

### Utilities
| Script | Purpose | Status |
|--------|---------|--------|
| `scripts/nexdata_client.py` | Python client SDK for API | ✅ Working |
| `scripts/enrich_pe_portfolio.py` | Website enrichment for PE companies | ✅ Working |
| `scripts/fetch_sec_proxy.py` | SEC filing cache | ✅ Working |
| `demo/pe_demo.py` | Terminal PE demo (acquisition + disposition stories) | ✅ Working |
| `demo/investor_demo.py` | Terminal investor demo (8 sections) | ✅ Working |

---

## 16. Testing

### Coverage Summary
- **117 test files**, **1,544 test functions**
- Markers: `@pytest.mark.unit` (offline), `@pytest.mark.integration` (requires API keys)

### Top Test Files by Count
| File | Tests | Area |
|------|-------|------|
| `test_medspa_sections_22_27.py` | 74 | Report sections |
| `test_scheduler_service.py` | 55 | Job scheduling |
| `test_ticker_resolver.py` | 48 | Symbol resolution |
| `test_rate_limiter.py` | 47 | Rate limiting |
| `test_vertical_discovery.py` | 38 | Market verticals |
| `test_llm_client.py` | 36 | LLM wrapper |
| `test_pe_persister.py` | 31 | PE data persistence |
| `test_api_key_preflight.py` | 31 | Auth validation |

### Coverage Gaps
- PE web demo (pe-demo.html) — not tested
- Agentic multi-step pipelines — limited
- GraphQL resolvers — not tested
- Report generation end-to-end — basic only
- Site Intelligence collectors — partial

---

## 17. Technical Debt Inventory

### Critical
| Issue | File | Impact |
|-------|------|--------|
| No DB migration framework | `alembic/` (unused) | Schema changes are manual; risky with live data |
| 14K-line monolithic HTML | `frontend/index.html` | Impossible to maintain at scale |
| Auth not enforced by default | `app/main.py` | No protection without JWT_SECRET_KEY set |
| API keys in docker-compose | `docker-compose.yml` | Keys visible in process list / logs |

### High
| Issue | File | Impact |
|-------|------|--------|
| CORS allows all origins | `app/main.py` | XSS risk in production |
| No multi-tenancy | All routers | Multiple clients share same DB |
| Deep collection slow (~3-4 min) | `deep_collection_orchestrator.py` | Poor live demo experience |
| Playwright off by default | `Dockerfile` | JS-rendered sites unsupported |
| DuckDuckGo blocked in Docker | `page_finder.py` | Reduces people collection coverage |

### Medium
| Issue | Area | Impact |
|-------|------|--------|
| Some Site Intel collectors broken | HIFLD, EPA, EIA, OpenEI | Gaps in site intel platform |
| Worker mode fallback confusion | `WORKER_MODE` env var | Silent job swallowing in production |
| Stale `.active_spec` hook | `docs/specs/.active_spec` | BYPASS_TRIVIAL must be set manually |
| No real-time push | Alerts system | Users must poll for changes |
| `bulk_upsert()` vs `null_preserving_upsert()` | `batch_operations.py` | Easy to use wrong one, silently loses data |

---

## 18. Market Fit Assessment

### What Nexdata Does Uniquely Well
1. **Job posting velocity as a leading indicator** — hiring trends 4-6 weeks ahead of financial results. No PE tool combines this with exit scoring natively.
2. **Exit readiness score (0-100)** — quantified, multi-signal, continuously updated. PE firms currently do this manually with consultants.
3. **Leadership network graph** — visualizes GP/company/executive relationships across a fund's portfolio. Board overlap, key-person risk.
4. **28+ data sources in one platform** — replaces PitchBook (sourcing) + Capital IQ (comps) + DealCloud (CRM) + consultants (DD) with a single API-first system.
5. **One-click data room assembly** — financials, leadership, competitors, valuations, benchmarks in a single call.

### Competitive Gap vs Incumbents
| Competitor | Their Strength | Nexdata's Edge |
|---|---|---|
| PitchBook ($25K/seat/yr) | Broad company database | Real-time job posting intel + AI scoring |
| Capital IQ ($20K/seat/yr) | Financial data + comps | Adds people intelligence + org charts |
| DealCloud ($15K/seat/yr) | CRM + deal workflow | CRM + intelligence + autonomous research |
| Grata | AI company search | Full DD, scoring, monitoring, org charts |
| Tegus | Expert call network | Public data (lower cost, always-on) |

### What's Missing for Market Fit
1. **Real PE data** — Demo is seeded/synthetic. Real clients need real data on real companies.
2. **Multi-tenancy** — Can't run multiple PE clients today without data bleed.
3. **Alerting / push** — "24/7 monitoring" story needs email/Slack notifications to be real.
4. **Mobile / boardroom UI** — Current frontend is a developer dashboard, not a partner-level product.
5. **PitchBook/Preqin integration** — No licensed data. Limits universe of companies tracked.

---

## 19. Demo Readiness Summary

| Capability | Status | Setup Required |
|---|---|---|
| PE Portfolio Heatmap | 🟢 Ready | `POST /pe/seed-demo` |
| PE Exit Readiness Score | 🟢 Ready | `POST /pe/seed-demo` |
| PE Buyer Analysis | 🟢 Ready | `POST /pe/seed-demo` |
| PE Leadership Network | 🟢 Ready | `POST /pe/seed-demo` |
| PE Fund Performance | 🟢 Ready | `POST /pe/seed-demo` |
| Deal Search & Pipeline | 🟢 Ready | `scripts/seed_demo_data.py` |
| People Collection | 🟢 Ready | Test on real company |
| Site Intel — Power | 🟢 Ready | Live EIA data, no seed needed |
| Site Intel — Logistics | 🟢 Ready | Live FMCSA/rates data |
| Economic Data (FRED/BLS) | 🟢 Ready | Requires API keys |
| PDF/PPTX Report Generation | 🟢 Ready | Works with seeded data |
| Company Health Scoring | 🟢 Ready | `scripts/seed_demo_data.py` |
| AI Due Diligence Agent | 🟡 Partial | Requires OPENAI_API_KEY, slow cold start |
| Job Posting Intelligence | 🟡 Partial | 65 companies seeded, analytics incomplete |
| Org Chart Deep Collection | 🟡 Slow | 3-4 min per company |
| Site Intel — Labor/Risk | 🟡 Partial | Collectors incomplete |
| Alerting / Notifications | 🔴 Missing | No push (email/Slack) — alerts in DB only |
| Mobile / Boardroom UI | 🔴 Missing | Needs frontend redesign |

---

## 20. Quick Start for Demo

```bash
# 1. Start all services
docker-compose up -d

# 2. Seed PE demo data
curl -s -X POST http://localhost:8001/api/v1/pe/seed-demo | python -m json.tool
# Note the returned firm_id

# 3. Open web demo
open http://localhost:3001/pe-demo.html

# 4. For economic data demo
curl -s "http://localhost:8001/api/v1/fred/series/GDP" | python -m json.tool

# 5. For site intelligence demo
curl -s "http://localhost:8001/api/v1/site-intel/power/plants?state=TX&fuel=wind" | python -m json.tool

# 6. For people collection demo
curl -s -X POST "http://localhost:8001/api/v1/people-jobs/test/1?sources=website"

# 7. Terminal PE demo
python demo/pe_demo.py --story acquisition --quick
python demo/pe_demo.py --story disposition --quick

# 8. API docs
open http://localhost:8001/docs
```

---

## 21. Overall Assessment

| Dimension | Score | Notes |
|---|---|---|
| **Feature completeness** | 8.5/10 | Comprehensive — everything in the GTM plan is built |
| **Demo readiness** | 7/10 | PE + Site Intel + Economic demos work well |
| **Code quality** | 7/10 | Consistent patterns, good error handling, some debt |
| **Test coverage** | 6/10 | 1,544 tests but gaps in integration + agentic |
| **Production readiness** | 5/10 | Needs auth enforcement, migrations, multi-tenancy |
| **Market fit** | 7/10 | Clear differentiation vs PitchBook; needs real data |
| **Frontend quality** | 4/10 | Functional but developer-grade, not product-grade |
| **Documentation** | 8/10 | 80+ docs, specs, plans, API reference |

**Overall: 6.5/10 — Strong foundation. Ready for customer demos. Not yet production SaaS.**

The core platform is architecturally sound. The bottleneck is no longer engineering — it's go-to-market: getting in front of PE firms, showing them real data, and closing the first engagement.
