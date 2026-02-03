# Nexdata - External Data Ingestion Platform

> **A unified API to ingest, search, and analyze data from 28+ public data sources**

Stop writing custom code for each data provider. Use one consistent REST API to access census data, economic indicators, energy statistics, SEC filings, and more—all stored in PostgreSQL with automatic rate limiting, scheduling, and analytics.

---

## Feature Status Overview

| Category | Feature | Status | Description |
|----------|---------|--------|-------------|
| **Data Ingestion** | 28 Data Sources | ✅ COMPLETE | Census, FRED, SEC, EIA, and 24 more |
| **Data Ingestion** | Job Management | ✅ COMPLETE | Track, retry, and monitor ingestion jobs |
| **Data Ingestion** | Scheduling | ✅ COMPLETE | Cron-based automated data refresh |
| **Data Ingestion** | Rate Limiting | ✅ COMPLETE | Per-source configurable limits |
| **Data Ingestion** | Data Quality | ✅ COMPLETE | Rule-based validation (range, null, regex) |
| **Data Ingestion** | Data Lineage | ✅ COMPLETE | Provenance tracking and impact analysis |
| **Data Ingestion** | Export | ✅ COMPLETE | CSV, JSON, Parquet formats |
| **Portfolio Research** | Agentic Discovery | ✅ COMPLETE | AI-powered investor portfolio research |
| **Portfolio Research** | SEC 13F Strategy | ✅ COMPLETE | Quarterly institutional holdings |
| **Portfolio Research** | Website Scraping | ✅ COMPLETE | JS rendering with Playwright |
| **Portfolio Research** | PDF Parsing | ✅ COMPLETE | Annual report extraction |
| **Analytics** | Dashboard Analytics | ✅ COMPLETE | System overview, trends, top movers |
| **Analytics** | Full-Text Search | ✅ COMPLETE | PostgreSQL FTS with fuzzy matching |
| **Analytics** | Recommendations | ✅ COMPLETE | Investor similarity, portfolio overlap |
| **Analytics** | Watchlists | ✅ COMPLETE | Saved searches and tracking |
| **Analytics** | Portfolio Alerts | ✅ COMPLETE | Change detection (internal tracking) |
| **Analytics** | Portfolio Comparison | ✅ COMPLETE | Side-by-side investor analysis |
| **Access** | GraphQL API | ✅ COMPLETE | Flexible query layer at /graphql |
| **Access** | Public API + Auth | ✅ COMPLETE | API keys and rate limits |
| **Agentic AI** | Company Research | ✅ COMPLETE | Multi-source company intelligence |
| **Agentic AI** | Due Diligence | ✅ COMPLETE | Automated DD with risk scoring |
| **Agentic AI** | Market Scanner | ✅ COMPLETE | Signal detection and monitoring |
| **Agentic AI** | Report Writer | ✅ COMPLETE | AI-generated investment reports |

**Full checklist:** [docs/MASTER_CHECKLIST.md](docs/MASTER_CHECKLIST.md)

---

## Platform Philosophy

**This is a data platform** - data is accessible FROM external systems (pull model).

| In Scope | Out of Scope |
|----------|--------------|
| REST/GraphQL APIs for querying | Push notifications to Slack/CRMs |
| Search, analytics, recommendations | Email digests to users |
| API authentication and rate limiting | Real-time webhooks for portfolio events |

---

## Data Sources (28 Integrated)

### Government & Economic Data
| Source | Description | Status |
|--------|-------------|--------|
| **Census Bureau** | Demographics, housing, economic by geography (ACS 5-year) | ✅ Ready |
| **FRED** | 800K+ economic time series (GDP, unemployment, CPI) | ✅ Ready |
| **EIA** | Energy prices, production, consumption | ✅ Ready |
| **BEA** | GDP, personal income, PCE, regional accounts | ✅ Ready |
| **BLS** | Employment, CPI, PPI, JOLTS | ✅ Ready |
| **USDA NASS** | Agricultural statistics (crops, yields, livestock) | ✅ Ready |
| **Treasury FiscalData** | Federal debt, interest rates, auctions | ✅ Ready |
| **BTS** | Border crossings, freight flows, vehicle miles | ✅ Ready |
| **CFTC COT** | Futures positioning (commercial vs non-commercial) | ✅ Ready |

### Financial & Corporate Data
| Source | Description | Status |
|--------|-------------|--------|
| **SEC (Edgar)** | 10-K, 10-Q, Form ADV, company facts (XBRL) | ✅ Ready |
| **USPTO Patents** | Patent search, inventors, assignees, CPC classifications | ✅ Ready |
| **FDIC BankFind** | Bank financials, failed banks, branch deposits | ✅ Ready |

### Real Estate & Geographic
| Source | Description | Status |
|--------|-------------|--------|
| **Real Estate** | FHFA house price index, HUD permits, Zillow | ✅ Ready |
| **GeoJSON** | State, county, tract, ZIP boundaries | ✅ Ready |

### Public Health & Infrastructure
| Source | Description | Status |
|--------|-------------|--------|
| **CMS/HHS** | Medicare utilization, hospital costs, drug pricing | ✅ Ready |
| **FEMA** | Disaster declarations, grants, mitigation | ✅ Ready |
| **FCC Broadband** | Broadband coverage, ISP availability | ✅ Ready |

### Other Specialized Data
| Source | Description | Status |
|--------|-------------|--------|
| **NOAA Weather** | Weather observations, climate data | ✅ Ready |
| **FBI Crime** | UCR stats, NIBRS incidents, hate crimes | ✅ Ready |
| **Yelp Fusion** | Business listings, reviews (500/day free) | ✅ Ready |
| **Data Commons** | Unified data from 200+ sources | ✅ Ready |
| **International Econ** | World Bank, IMF, OECD, BIS | ✅ Ready |
| **US Trade** | Import/export by HS code, port, partner | ✅ Ready |
| **IRS SOI** | Income/wealth by ZIP, county, migration | ✅ Ready |
| **Kaggle** | Competition datasets | ✅ Ready |
| **Prediction Markets** | Kalshi, Polymarket monitoring | ✅ Ready |

---

## Quick Start (5 Minutes)

### Prerequisites
- **Python 3.11+**
- **Docker Desktop** (for PostgreSQL)
- **Git**

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/rao-usha/zerostack-data.git
cd zerostack-data

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Mac/Linux
# venv\Scripts\activate   # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file (see Configuration section)

# 5. Start service
docker-compose up -d --build
```

**API running at:** http://localhost:8001
**Swagger UI:** http://localhost:8001/docs

---

## API Endpoints

### Core Data Ingestion
```
POST /api/v1/jobs              - Start ingestion job
GET  /api/v1/jobs/{id}         - Check job status
GET  /api/v1/jobs              - List all jobs
```

### Scheduling & Automation
```
POST /api/v1/schedules         - Create scheduled job
GET  /api/v1/schedules         - List schedules
DELETE /api/v1/schedules/{id}  - Remove schedule
```

### Search & Discovery
```
GET  /api/v1/search            - Full-text search (investors, companies)
GET  /api/v1/search/suggest    - Autocomplete suggestions
```

### Analytics
```
GET  /api/v1/analytics/overview           - System-wide stats
GET  /api/v1/analytics/investor/{id}      - Investor analytics
GET  /api/v1/analytics/trends             - Time-series trends
GET  /api/v1/analytics/top-movers         - Recent portfolio changes
GET  /api/v1/analytics/industry-breakdown - Sector distribution
```

### Recommendations
```
GET  /api/v1/discover/similar/{id}      - Find similar investors
GET  /api/v1/discover/recommended/{id}  - Company recommendations
GET  /api/v1/discover/overlap           - Portfolio overlap analysis
```

### Watchlists & Saved Searches
```
POST /api/v1/watchlists              - Create watchlist
GET  /api/v1/watchlists              - List watchlists
POST /api/v1/watchlists/{id}/items   - Add to watchlist
POST /api/v1/searches/saved          - Save search query
```

### Portfolio Alerts (Internal Tracking)
```
POST /api/v1/alerts/subscribe        - Subscribe to investor alerts
GET  /api/v1/alerts                  - Get pending alerts
POST /api/v1/alerts/{id}/acknowledge - Dismiss alert
```

### Data Quality & Export
```
POST /api/v1/data-quality/rules      - Create validation rule
GET  /api/v1/data-quality/evaluate   - Run validation
GET  /api/v1/export/{table}          - Export to CSV/JSON/Parquet
```

### Data Lineage
```
GET  /api/v1/lineage/nodes           - Get lineage nodes
GET  /api/v1/lineage/impact/{id}     - Impact analysis
```

---

## Agentic Portfolio Research

AI-powered system for discovering institutional investor portfolios.

### Strategies
| Strategy | Confidence | Description |
|----------|------------|-------------|
| SEC 13F | HIGH | Quarterly institutional holdings filings |
| Website Scraping | MEDIUM | Investor website portfolio pages (JS rendering) |
| Annual Reports | HIGH | PDF parsing of annual report documents |
| News Search | MEDIUM | LLM extraction from news articles |
| Reverse Search | HIGH | Company mentions across sources |

### Usage
```bash
# Discover portfolio for an LP
POST /api/v1/agentic/research
{
  "investor_id": 1,
  "investor_type": "lp",
  "strategies": ["sec_13f", "website", "annual_report"]
}
```

---

## Configuration

### Environment Variables (.env)

```bash
# Database
DATABASE_URL=postgresql://nexdata:nexdata@localhost:5432/nexdata

# Required API Keys
FRED_API_KEY=your_fred_key
EIA_API_KEY=your_eia_key
BEA_API_KEY=your_bea_key

# Recommended API Keys
CENSUS_SURVEY_API_KEY=your_census_key
YELP_API_KEY=your_yelp_key
DATA_GOV_API=your_data_gov_key

# Optional
OPENAI_API_KEY=your_openai_key  # For agentic research
ANTHROPIC_API_KEY=your_anthropic_key

# Rate Limiting
MAX_CONCURRENCY=5
MAX_REQUESTS_PER_SECOND=10
```

### API Key Sources (Free)
| Source | Get Key |
|--------|---------|
| Census | https://api.census.gov/data/key_signup.html |
| FRED | https://fred.stlouisfed.org/docs/api/api_key.html |
| EIA | https://www.eia.gov/opendata/register.php |
| BEA | https://apps.bea.gov/api/signup/ |
| Yelp | https://www.yelp.com/developers/v3/manage_app |

---

## Development Status

**All 5 phases complete!** See [docs/MASTER_CHECKLIST.md](docs/MASTER_CHECKLIST.md) for full details.

| Phase | Tasks | Status |
|-------|-------|--------|
| Phase 1: Agentic Infrastructure | T01-T10 | ✅ COMPLETE |
| Phase 2: Data Delivery | T11-T20 | ✅ COMPLETE |
| Phase 3: Investment Intelligence | T21-T30 | ✅ COMPLETE |
| Phase 4: Data Expansion | T31-T40 | ✅ COMPLETE |
| Phase 5: Agentic AI | T41-T50 | ✅ COMPLETE |

### Current Data
| Metric | Count |
|--------|-------|
| LPs Tracked | 564 |
| Family Offices | 308 |
| Portfolio Companies | 5,236 |
| Prediction Markets | 18 |
| GitHub Repos | 293 |
| News Items | 398 |

---

## Database Tables

### Core Tables
- `ingestion_jobs` - Job tracking
- `ingestion_schedules` - Scheduled jobs
- `dataset_registry` - Metadata registry

### Portfolio Research
- `lp_fund` - LP investors
- `family_offices` - Family office investors
- `portfolio_companies` - Holdings discovered
- `co_investments` - Co-investment relationships

### Analytics & Search
- `search_index` - Full-text search index
- `alert_subscriptions` - Alert subscriptions
- `portfolio_alerts` - Change alerts
- `watchlists` - User watchlists
- `saved_searches` - Saved search queries

---

## Project Structure

```
Nexdata/
├── app/
│   ├── main.py              # FastAPI entry point
│   ├── api/v1/              # 45 API routers
│   ├── core/                # 23 core services
│   ├── sources/             # 28 data source adapters
│   ├── agentic/             # Portfolio research agent
│   ├── analytics/           # Dashboard analytics
│   ├── search/              # Full-text search
│   ├── notifications/       # Alerts system
│   └── users/               # Watchlists
├── tests/                   # 13 test files (84+ tests)
├── docs/                    # Documentation
│   └── PROJECT_STATUS.md    # Detailed status report
├── docker-compose.yml
└── requirements.txt
```

---

## Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run specific test
pytest tests/test_llm_client.py -v
```

---

## Documentation

- **[PROJECT_STATUS.md](docs/PROJECT_STATUS.md)** - Comprehensive feature status
- **[PARALLEL_WORK.md](PARALLEL_WORK.md)** - Development task tracking
- **Swagger UI** - http://localhost:8001/docs (when running)
- **ReDoc** - http://localhost:8001/redoc

### Quick Start Guides
- [Census](docs/CENSUS_METADATA_API_REFERENCE.md)
- [FRED](docs/FRED_QUICK_START.md)
- [SEC](docs/SEC_QUICK_START.md)
- [BLS](docs/BLS_QUICK_START.md)

---

## Tech Stack

- **FastAPI** - Async Python web framework
- **PostgreSQL** - Data storage
- **SQLAlchemy 2.0** - ORM
- **httpx** - Async HTTP client
- **APScheduler** - Job scheduling
- **Playwright** - Browser automation (JS rendering)
- **OpenAI/Anthropic** - LLM clients

---

## 3PL Data Collection Status

### What's Implemented ✅

| Component | Status | Description |
|-----------|--------|-------------|
| **Database Model** | ✅ Complete | `ThreePLCompany` table with 25+ fields |
| **API Endpoints** | ✅ Complete | Search and coverage endpoints |
| **Collector Framework** | ✅ Complete | `ThreePLCollector` with transform logic |

**Database Fields Available:**
- Company basics: name, parent company, HQ location, website
- Financials: revenue, employees, facility count
- Services: JSON array (warehousing, transportation, fulfillment, etc.)
- Industries: JSON array (retail, manufacturing, automotive, etc.)
- Geographic coverage: regions, states, countries
- Rankings: Armstrong & Associates, Transport Topics
- Capabilities: cold chain, hazmat, e-commerce, cross-dock, asset model

**API Endpoints:**
```
GET /api/v1/site-intel/logistics/3pl-companies
    ?state=TX
    &has_cold_chain=true
    &min_revenue=1000
    &limit=50

GET /api/v1/site-intel/logistics/3pl-companies/{company_id}/coverage
```

### What's Needed 🔴

| Task | Priority | Notes |
|------|----------|-------|
| **Remove sample data** | P0 | `_get_sample_companies()` in collector needs removal |
| **Real data sources** | P0 | See data source options below |
| **Contact enrichment** | P1 | Executive contacts, sales contacts |
| **Technology data** | P2 | TMS/WMS vendors, integrations |
| **Certifications** | P2 | C-TPAT, ISO, SmartWay, etc. |

### Potential Data Sources

| Source | Type | Access | Coverage |
|--------|------|--------|----------|
| **Transport Topics Top 100** | Web scraping | Free (JS render) | Top 100 by revenue |
| **Armstrong & Associates** | API/Subscription | Paid (~$2K/yr) | Top 50 global 3PLs |
| **SEC EDGAR** | API | Free | Public 3PLs only (XPO, CHR, JBHT, etc.) |
| **FreightWaves SONAR** | API | Paid | Market data, limited company profiles |
| **LinkedIn Sales Navigator** | API | Paid | Contact data, company profiles |
| **ZoomInfo/Apollo** | API | Paid | Contact data enrichment |
| **Crunchbase** | API | Freemium | Funding, M&A, company profiles |

### Recommended Next Steps

1. **Phase 1: Public Data** (No cost)
   - Implement Transport Topics scraping with Playwright
   - Pull SEC filings for public 3PLs (XPO, C.H. Robinson, J.B. Hunt, etc.)
   - Scrape company websites for basic info

2. **Phase 2: Enrichment** (API costs)
   - Evaluate Armstrong & Associates subscription
   - Add Crunchbase for funding/M&A data
   - Consider ZoomInfo for contact enrichment

3. **Phase 3: Real-time** (Future)
   - FreightWaves integration for market signals
   - News monitoring for M&A, capacity changes

---

## Site Intelligence Platform Status

### Collectors Working ✅

| Collector | Source | Records | Notes |
|-----------|--------|---------|-------|
| EIA Electricity | EIA API | 500+ | State average prices by sector |
| USGS Water | USGS API | 1,780+ | Monitoring sites with real-time data |
| EIA Power | EIA API | 24+ | Electricity prices (plants API needs fix) |

### Collectors Needing Updates 🔴

| Collector | Issue | Fix Needed |
|-----------|-------|------------|
| **HIFLD Substations** | API URL invalid | Find new ArcGIS endpoint |
| **EPA SDWIS** | Table not available | EPA reorganized API - find new table |
| **EIA Gas** | API returns 400 | Update API parameters |
| **OpenEI URDB** | Returns 403 | Check API access/rate limits |

### Sample Data Removed ✅

All synthetic/sample data has been removed from collectors for training data purity:
- `eia_collector.py` (power)
- `hifld_collector.py` (power)
- `usgs_water_collector.py`
- `epa_sdwis_collector.py`
- `eia_gas_collector.py`
- `openei_rates_collector.py`
- `eia_electricity_collector.py`
- `three_pl_collector.py` (logistics)

---

## What's Next

### Remaining Tasks
1. **Fix broken collectors** - Update API endpoints for HIFLD, EPA, EIA Gas, OpenEI
2. **Implement 3PL real data** - Transport Topics scraping with Playwright, SEC enrichment

### Future Enhancements
- Response caching with Redis
- SDK/client libraries (Python, TypeScript)
- OpenAPI-generated client code

---

## License

MIT License

---

## Data Providers

Data provided by: U.S. Census Bureau, Federal Reserve (FRED), EIA, SEC, BEA, BLS, USDA, Treasury, BTS, CFTC, CMS, FEMA, FCC, NOAA, FBI, FDIC, USPTO, IRS, World Bank, IMF, OECD, Google Data Commons, Yelp, Kaggle, Kalshi, Polymarket.

---

**API Documentation:** http://localhost:8001/docs
**Detailed Status:** [docs/PROJECT_STATUS.md](docs/PROJECT_STATUS.md)
