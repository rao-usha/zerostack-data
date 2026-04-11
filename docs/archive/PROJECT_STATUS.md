# Nexdata Project Status

> Last Updated: February 2026

## Overview

Nexdata is a comprehensive data intelligence platform that ingests, enriches, and analyzes data from 30+ public and private data sources. It includes AI-powered agents for autonomous research, due diligence, and market analysis.

---

## Platform Summary

| Platform | Status | Description |
|----------|--------|-------------|
| **Core Data Ingestion** | Production | 25+ public data source adapters |
| **Agentic Portfolio Research** | Production | AI-powered LP/FO portfolio discovery |
| **AI Agents (Phase 5)** | Production | 8 autonomous research agents + orchestrator |
| **People & Leadership Intelligence** | Production | Company leadership tracking & collection |
| **PE Intelligence Platform** | Production | PE firms, companies, people, deals |
| **Site Intelligence Platform** | Production | Industrial/data center site selection |
| **Investment Analytics** | Production | Trends, benchmarks, comparisons, scoring |
| **Public API** | Production | Authenticated API with rate limiting |

---

## 1. Core Data Sources (25+ APIs)

### Location: `app/sources/`, `app/api/v1/`

| Source | Endpoint Prefix | Description | Status |
|--------|-----------------|-------------|--------|
| Census Bureau | `/census-*` | ACS demographics, batch ingestion | Working |
| FRED | `/fred/` | 800K+ economic time series | Working |
| EIA | `/eia/` | Energy prices, production | Working |
| SEC EDGAR | `/sec/` | Company filings, facts | Working |
| SEC Form D | `/form-d/` | Private placement filings | Working |
| SEC Form ADV | `/form-adv/` | Investment adviser data | Working |
| BLS | `/bls/` | Employment, CPI, PPI | Working |
| BEA | `/bea/` | GDP, personal income | Working |
| Treasury | `/treasury/` | Debt, interest rates | Working |
| FDIC | `/fdic/` | Bank financials, deposits | Working |
| IRS SOI | `/irs-soi/` | Income statistics by geography | Working |
| FEMA | `/fema/` | Disaster declarations | Working |
| FBI Crime | `/fbi-crime/` | UCR crime statistics | Working |
| BTS | `/bts/` | Transportation statistics | Working |
| FCC | `/fcc-broadband/` | Broadband coverage | Working |
| USPTO | `/uspto/` | Patent data | Working |
| CMS/HHS | `/cms/` | Healthcare data | Working |
| NOAA | `/noaa/` | Weather/climate | Working |
| USDA | `/usda/` | Agricultural data | Working |
| CFTC COT | `/cftc-cot/` | Futures positioning | Working |
| US Trade | `/us-trade/` | Import/export data | Working |
| Data Commons | `/data-commons/` | Google's unified data | Working |
| Yelp | `/yelp/` | Business listings | Working |
| International Econ | `/international-econ/` | World Bank, IMF, OECD | Working |
| Prediction Markets | `/prediction-markets/` | Kalshi, Polymarket | Working |

---

## 2. Agentic Portfolio Research

### Location: `app/agentic/`, `app/api/v1/agentic_research.py`

Autonomous AI-powered portfolio discovery for institutional investors.

**Features:**
- Multi-strategy data collection (13F, website, news, SEC, annual reports)
- Fuzzy matching for company deduplication
- Retry handling with exponential backoff
- Response caching (HTTP, PDF, robots.txt)
- Scheduled quarterly updates
- Portfolio export (CSV/Excel)

**Key Files:**
| File | Purpose |
|------|---------|
| `app/agentic/portfolio_agent.py` | Main portfolio research agent |
| `app/agentic/strategies/` | Collection strategies (13F, website, news) |
| `app/agentic/synthesizer.py` | Result aggregation |
| `app/agentic/scheduler.py` | Quarterly refresh scheduling |
| `app/agentic/exporter.py` | CSV/Excel export |
| `app/agentic/metrics.py` | Job metrics/monitoring |

**Endpoints:**
- `POST /api/v1/agentic/research/{investor_type}/{investor_id}` - Start research
- `GET /api/v1/agentic/research/jobs` - List jobs
- `GET /api/v1/agentic/metrics` - Collection metrics

---

## 3. AI Agents (Phase 5)

### Location: `app/agents/`, `app/api/v1/`

Eight specialized AI agents that work autonomously or together via the orchestrator.

| Agent | File | API Prefix | Purpose |
|-------|------|------------|---------|
| Company Researcher | `company_researcher.py` | `/agents/` | Comprehensive company research |
| Due Diligence | `due_diligence.py` | `/diligence/` | Automated DD reports with red flags |
| News Monitor | `news_monitor.py` | `/monitors/` | Real-time news tracking |
| Competitive Intel | `competitive_intel.py` | `/competitive/` | Competitive landscape analysis |
| Data Hunter | `data_hunter.py` | `/hunter/` | Find and fill missing data |
| Anomaly Detector | `anomaly_detector.py` | `/anomalies/` | Detect unusual patterns |
| Report Writer | `report_writer.py` | `/reports-gen/` | Generate natural language reports |
| Market Scanner | `market_scanner.py` | `/market/` | Market trends and opportunities |

### Multi-Agent Orchestrator

**Location:** `app/agents/orchestrator.py`, `app/api/v1/workflows.py`

Coordinates multiple agents for complex research tasks.

**Built-in Workflows:**
1. `full_due_diligence` - Company research + competitive + news + DD + report
2. `quick_company_scan` - Fast company research
3. `competitive_landscape` - Deep competitive analysis
4. `market_intelligence` - Market scanning with trends
5. `data_enrichment` - Find missing data + anomaly detection
6. `investor_brief` - Research + news + report

**Endpoints:**
- `POST /api/v1/workflows/start` - Start a workflow
- `GET /api/v1/workflows/{id}` - Get workflow status
- `GET /api/v1/workflows/templates/list` - List workflow templates
- `POST /api/v1/workflows/custom` - Create custom workflow

---

## 4. People & Leadership Intelligence

### Location: `app/sources/people_collection/`, `app/api/v1/people*.py`, `app/api/v1/companies_leadership.py`

Company leadership tracking, org charts, and executive changes.

**Features:**
- Website crawling for leadership pages
- SEC 8-K parsing for executive changes
- News scanning for leadership announcements
- Leadership change alerts
- Executive watchlists
- Peer company benchmarking

**Key Files:**
| File | Purpose |
|------|---------|
| `app/sources/people_collection/orchestrator.py` | Collection coordinator |
| `app/sources/people_collection/website_agent.py` | Website scraping |
| `app/sources/people_collection/sec_agent.py` | SEC filing parser |
| `app/sources/people_collection/news_agent.py` | News scanner |
| `app/jobs/people_collection_scheduler.py` | Job scheduling & processing |
| `app/jobs/change_monitor.py` | Leadership change detection |
| `app/core/people_models.py` | Database models |

**Endpoints:**
| Endpoint | Purpose |
|----------|---------|
| `/api/v1/people/` | Person CRUD, search |
| `/api/v1/companies-leadership/` | Company leadership teams |
| `/api/v1/people-jobs/` | Collection job management |
| `/api/v1/people-portfolios/` | Track leadership across portfolios |
| `/api/v1/people-watchlists/` | Executive watchlists |
| `/api/v1/peer-sets/` | Peer company benchmarking |
| `/api/v1/people-analytics/` | Leadership analytics |
| `/api/v1/people-reports/` | Generate leadership reports |
| `/api/v1/people-data-quality/` | Data quality metrics |

**Scheduled Jobs (APScheduler):**
| Job | Frequency | Purpose |
|-----|-----------|---------|
| `people_job_processor` | Every 10 min | Process pending collection jobs |
| `people_weekly_website_refresh` | Sundays 2 AM | Refresh leadership pages |
| `people_daily_sec_check` | Weekdays 6 PM | Check SEC 8-K filings |
| `people_daily_news_scan` | Daily 8 AM | Scan news for changes |
| `people_stuck_job_cleanup` | Every 2 hours | Mark stuck jobs as failed |

---

## 5. PE Intelligence Platform

### Location: `app/sources/pe_collection/`, `app/api/v1/pe_*.py`

Private equity firm, portfolio company, and deal tracking.

**Features:**
- PE firm profiles and AUM tracking
- Portfolio company management
- Deal/transaction tracking
- Key people at PE firms

**Endpoints:**
- `/api/v1/pe-firms/` - PE firm CRUD
- `/api/v1/pe-companies/` - Portfolio companies
- `/api/v1/pe-people/` - Key personnel
- `/api/v1/pe-deals/` - Deal tracking

---

## 6. Site Intelligence Platform

### Location: `app/sources/site_intel/`, `app/api/v1/site_intel_*.py`

Industrial and data center site selection intelligence.

**Data Layers:**
| Layer | Endpoint | Data Sources |
|-------|----------|--------------|
| Power | `/site-intel/power/` | EIA, utility territories |
| Telecom | `/site-intel/telecom/` | FCC broadband, fiber maps |
| Transportation | `/site-intel/transport/` | BTS, airports, rail |
| Labor | `/site-intel/labor/` | BLS employment, wages |
| Risk | `/site-intel/risk/` | FEMA disasters, crime |
| Incentives | `/site-intel/incentives/` | State/local tax incentives |
| Logistics | `/site-intel/logistics/` | Ports, FTZs, intermodal |
| Sites | `/site-intel/sites/` | Site search and scoring |

---

## 7. Investment Analytics

### Location: `app/analytics/`, `app/ml/`

**Analytics Features:**
| Feature | Location | Endpoints |
|---------|----------|-----------|
| Dashboard Stats | `analytics/dashboard.py` | `/analytics/overview` |
| Investment Trends | `analytics/trends.py` | `/trends/` |
| Market Benchmarks | `analytics/benchmarks.py` | `/benchmarks/` |
| Portfolio Comparison | `analytics/comparison.py` | `/compare/` |
| Investor Similarity | `analytics/recommendations.py` | `/discover/` |
| Co-investor Network | `network/graph.py` | `/network/` |

**ML Models:**
| Model | Location | Endpoints |
|-------|----------|-----------|
| Company Scoring | `ml/company_scorer.py` | `/scores/` |
| Deal Prediction | `ml/deal_scorer.py` | `/predictions/` |

---

## 8. Platform Features

### Authentication & API Keys
- **Location:** `app/auth/`, `app/api/v1/auth.py`, `app/api/v1/api_keys.py`
- JWT authentication with refresh tokens
- API key generation with rate limiting
- Workspace-based team collaboration

### Search & Discovery
- **Location:** `app/search/`, `app/api/v1/search.py`
- PostgreSQL full-text search with GIN indexes
- Fuzzy matching for typo tolerance
- Autocomplete suggestions

### Data Quality & Lineage
- **Location:** `app/core/data_quality.py`, `app/core/lineage_service.py`
- Data quality rules engine
- Provenance tracking

### Export & Import
- **Location:** `app/api/v1/export.py`, `app/api/v1/import_portfolio.py`
- CSV/Excel/Parquet export
- Bulk portfolio import with validation

### GraphQL API
- **Location:** `app/graphql/`
- Strawberry GraphQL schema
- Endpoint: `/graphql`

---

## Database Tables

### Core Tables
- `ingestion_jobs` - Job tracking
- `ingestion_schedules` - Scheduled jobs

### Investor Tables
- `lp_fund` - LP/institutional investors
- `family_offices` - Family offices
- `portfolio_companies` - Investor holdings

### People Tables
- `people` - Person records
- `company_person` - Person-company relationships
- `industrial_companies` - Companies with leadership
- `leadership_changes` - Executive changes
- `people_collection_jobs` - Collection job tracking

### PE Tables
- `pe_firms` - PE firm profiles
- `pe_portfolio_companies` - Portfolio companies
- `pe_deals` - Deal records

### Site Intel Tables
- `site_intel_sites` - Candidate sites
- Various layer tables (power, telecom, etc.)

---

## What's Working

1. **All 25+ data source adapters** - Full ingestion and storage
2. **Agentic portfolio research** - LP/FO portfolio discovery
3. **8 AI agents + orchestrator** - Autonomous research
4. **People collection pipeline** - Website, SEC, news collection
5. **Scheduled jobs** - APScheduler integration working
6. **Investment analytics** - Trends, benchmarks, scoring
7. **Public API** - Auth, rate limiting, usage tracking
8. **Search** - Full-text with fuzzy matching
9. **Site Intelligence** - 8 data layers for site selection

---

## Known Issues / Needs Work

### People Collection
1. **Empty LinkedIn URLs** - Fixed (converts empty strings to NULL)
2. **Agent implementations** - WebsiteAgent, SECAgent, NewsAgent exist but may need tuning
3. **People data quality** - Low people_found counts suggest agents need improvement

### General
1. **T14/T15 skipped** - Webhook integrations and email digests not implemented
2. **Test coverage** - Needs expansion beyond unit tests
3. **Documentation** - API docs exist but per-source guides incomplete

---

## Future Enhancements (Backlog)

1. **Enhanced SEC parsing** - Better 8-K/DEF14A extraction for leadership
2. **LinkedIn integration** - If API access available
3. **Email digests** - Weekly/daily summaries (T15)
4. **Webhook notifications** - Push updates to external systems (T14)
5. **Mobile app** - React Native client
6. **AI improvements** - Fine-tuned models for financial data

---

## Quick Start

### Run the API
```bash
docker-compose up --build -d
```

### Test Health
```bash
curl http://localhost:8001/health
```

### API Documentation
- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc
- GraphQL Playground: http://localhost:8001/graphql

### Run People Collection Jobs
```bash
# Manually trigger processing
curl -X POST "http://localhost:8001/api/v1/people-jobs/process?max_jobs=5"

# Check schedule status
curl http://localhost:8001/api/v1/people-jobs/schedules/status
```

### Start a Workflow
```bash
curl -X POST http://localhost:8001/api/v1/workflows/start \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "quick_company_scan", "entity_name": "Stripe", "entity_type": "company"}'
```

---

## File Structure

```
app/
├── agents/           # AI agents (T41-T50)
├── agentic/          # Portfolio research infrastructure
├── analytics/        # Investment analytics
├── api/v1/           # All REST endpoints
├── auth/             # Authentication
├── core/             # Shared models, database, config
├── deals/            # Deal flow tracker
├── enrichment/       # Company/investor enrichment
├── graphql/          # GraphQL schema
├── import_data/      # Bulk import
├── jobs/             # Job scheduling
├── ml/               # ML models
├── network/          # Co-investor network
├── news/             # News aggregation
├── notifications/    # Alerts
├── reports/          # Report generation
├── search/           # Full-text search
├── sources/          # Data source adapters (25+)
└── users/            # User auth, workspaces
```

---

## Contact / Support

- **API Issues:** Check `/api/v1/jobs/monitoring/dashboard` for system health
- **Logs:** `docker-compose logs api --tail 100`
- **Documentation:** `/docs` folder and Swagger UI
