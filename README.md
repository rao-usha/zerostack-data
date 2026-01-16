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
| **Access** | Public API + Auth | 📋 PLANNED | API keys and rate limits for external access |

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

### Phase 1: Agentic Infrastructure - COMPLETE
| Task | Description | Status |
|------|-------------|--------|
| T01 | Retry Handler (exponential backoff) | ✅ |
| T02 | Fuzzy Matching (85% threshold) | ✅ |
| T03 | Response Caching | ✅ |
| T04 | LLM Client Tests (36 tests) | ✅ |
| T05 | Ticker Resolver Tests (48 tests) | ✅ |
| T06 | Metrics & Monitoring | ✅ |
| T07 | Scheduled Updates | ✅ |
| T08 | Portfolio Export (CSV/Excel) | ✅ |
| T09 | PDF Caching | ✅ |
| T10 | JS Rendering (Playwright) | ✅ |

### Phase 2: Data Delivery - IN PROGRESS
| Task | Description | Status |
|------|-------------|--------|
| T11 | Portfolio Change Alerts | ✅ COMPLETE |
| T12 | Full-Text Search API | ✅ COMPLETE |
| T13 | Dashboard Analytics API | ✅ COMPLETE |
| T14 | Webhook Integrations | ⏭️ SKIPPED |
| T15 | Email Digest Reports | ⏭️ SKIPPED |
| T16 | GraphQL API Layer | ✅ COMPLETE |
| T17 | Portfolio Comparison Tool | ✅ COMPLETE |
| T18 | Investor Similarity | ✅ COMPLETE |
| T19 | Public API with Auth | 🔄 IN PROGRESS |
| T20 | Saved Searches & Watchlists | ✅ COMPLETE |

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

## What's Next

### Remaining Tasks
1. **T19 - Public API with Auth** - API keys and rate limits for external access (in progress)

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
