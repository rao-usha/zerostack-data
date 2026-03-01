"""
Main FastAPI application.

Source-agnostic entry point that routes to appropriate adapters.
"""

import logging
import os
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.api.v1.auth import get_current_user
from app.api.v1 import (
    jobs,
    census_geo,
    census_batch,
    metadata,
    fred,
    eia,
    sec,
    realestate,
    geojson,
    family_offices,
    family_office_contacts,
    cms,
    kaggle,
    international_econ,
    fbi_crime,
    bts,
    bea,
    fema,
    data_commons,
    yelp,
    us_trade,
    cftc_cot,
    usda,
    bls,
    fcc_broadband,
    treasury,
    fdic,
    irs_soi,
    agentic_research,
    foot_traffic,
    prediction_markets,
    schedules,
    webhooks,
    chains,
    rate_limits,
    data_quality,
    templates,
    lineage,
    export,
    uspto,
    alerts,
    search,
    discover,
    watchlists,
    analytics,
    compare,
    api_keys,
    public,
    network,
    trends,
    enrichment,
    import_portfolio,
    news,
    reports,
    deals,
    benchmarks,
    auth,
    workspaces,
    form_d,
    corporate_registry,
    form_adv,
    web_traffic,
    github,
    scores,
    entities,
    glassdoor,
    app_rankings,
    predictions,
    agents,
    diligence,
    monitors,
    competitive,
    hunter,
    anomalies,
    market,
    reports_gen,
    lp_collection,
    fo_collection,
    pe_firms,
    pe_companies,
    pe_people,
    pe_deals,
    pe_collection,
    app_stores,
    opencorporates,
    people,
    companies_leadership,
    collection_jobs,
    people_portfolios,
    peer_sets,
    people_watchlists,
    people_analytics,
    people_reports,
    people_data_quality,
    people_dedup,
    people_jobs,
    workflows,
    llm_costs,
    freshness,
    dunl,
    job_postings,
    job_postings_velocity,
    health_scores,
    lp_allocation,
    exit_readiness,
    acquisition_targets,
    quarterly_diff,
    zip_scores,
    medspa_discovery,
    deal_models,
)

# Job Queue Streaming & Monitor
from app.api.v1 import job_stream, jobs_monitor

# Site Intelligence Platform
from app.api.v1 import (
    site_intel_power,
    site_intel_telecom,
    site_intel_transport,
    site_intel_labor,
    site_intel_risk,
    site_intel_incentives,
    site_intel_logistics,
    site_intel_water_utilities,
    site_intel_sites,
)

# Collection Management
from app.api.v1 import source_configs, audit

# Settings
from app.api.v1 import settings as settings_router
from app.graphql import graphql_app

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.

    Runs on startup and shutdown.
    """
    # Startup
    settings = get_settings()
    logger.info("Starting External Data Ingestion Service")
    logger.info(f"Log level: {settings.log_level}")
    logger.info(f"Max concurrency: {settings.max_concurrency}")

    # Run database migrations
    try:
        from alembic.config import Config
        from alembic import command
        from app.core.database import get_engine, create_tables
        from sqlalchemy import inspect

        engine = get_engine()
        inspector = inspect(engine)
        has_alembic = "alembic_version" in inspector.get_table_names()

        alembic_cfg = Config("alembic.ini")

        if not has_alembic:
            # Fresh database — create all tables then stamp as current
            create_tables(engine)
            command.stamp(alembic_cfg, "head")
            logger.info("Fresh database: tables created and stamped at head")
        else:
            # Existing database — apply any pending migrations
            command.upgrade(alembic_cfg, "head")
            logger.info("Database migrations applied (alembic upgrade head)")
    except Exception as e:
        logger.error(f"Alembic migration failed, falling back to create_all: {e}")
        try:
            from app.core.database import create_tables

            create_tables()
            logger.info("Fallback: tables created via create_all()")
        except Exception as e2:
            logger.error(f"Fallback create_tables also failed: {e2}")
            raise

    # Start scheduler (optional - can be started manually via API)
    try:
        from app.core import scheduler_service
        from app.core.database import get_session_factory

        scheduler_service.start_scheduler()

        # Auto-create default schedules (idempotent — skips existing)
        SessionLocal = get_session_factory()
        db = SessionLocal()
        try:
            created = scheduler_service.create_default_schedules(db)
            if created:
                logger.info(f"Created {len(created)} default schedules (paused)")
        except Exception as e:
            logger.warning(f"Failed to create default schedules: {e}")
        finally:
            db.close()

        # Load active schedules
        db = SessionLocal()
        try:
            count = scheduler_service.load_all_schedules(db)
            logger.info(f"Scheduler started with {count} active schedules")
        finally:
            db.close()

        # Register automatic stuck job cleanup (runs every 30 minutes)
        scheduler_service.register_cleanup_job(interval_minutes=30)
        logger.info("Automatic stuck job cleanup registered")

        # Register automatic retry processor (runs every 5 minutes)
        scheduler_service.register_retry_processor(interval_minutes=5)
        logger.info("Automatic retry processor registered")

        # Register consecutive failure checker (runs every 30 minutes)
        scheduler_service.register_consecutive_failure_checker(interval_minutes=30)
        logger.info("Consecutive failure checker registered")

        # Register freshness auto-refresh checker (runs every 60 minutes)
        scheduler_service.register_freshness_checker(interval_minutes=60)
        logger.info("Freshness auto-refresh checker registered")

        # Register cross-source validation (runs every 6 hours)
        scheduler_service.register_cross_source_validation(interval_hours=6)
        logger.info("Cross-source validation registered")

        # Register daily quality snapshots (runs at 2 AM)
        scheduler_service.register_daily_quality_snapshots(hour=2)
        logger.info("Daily quality snapshots registered")

        # Register quality degradation checker (runs at 3 AM)
        scheduler_service.register_degradation_checker(hour=3)
        logger.info("Quality degradation checker registered")

        # Register rule evaluation (runs at 4 AM — after snapshots & degradation)
        scheduler_service.register_rule_evaluation(hour=4)
        logger.info("Rule evaluation registered")

        # Register people collection schedules
        try:
            from app.jobs.people_collection_scheduler import (
                register_people_collection_schedules,
            )

            people_results = register_people_collection_schedules()
            registered_count = sum(1 for v in people_results.values() if v)
            logger.info(
                f"People collection schedules registered: {registered_count}/{len(people_results)}"
            )
        except Exception as e:
            logger.warning(f"Failed to register people collection schedules: {e}")

        # Register PE collection schedules
        try:
            from app.jobs.pe_collection_scheduler import (
                register_pe_collection_schedules,
            )

            pe_results = register_pe_collection_schedules()
            registered_count = sum(1 for v in pe_results.values() if v)
            logger.info(
                f"PE collection schedules registered: {registered_count}/{len(pe_results)}"
            )
        except Exception as e:
            logger.warning(f"Failed to register PE collection schedules: {e}")

        # Register site intel collection schedules
        try:
            from app.jobs.site_intel_scheduler import register_site_intel_schedules

            site_intel_results = register_site_intel_schedules()
            registered_count = sum(1 for v in site_intel_results.values() if v)
            logger.info(
                f"Site intel schedules registered: {registered_count}/{len(site_intel_results)}"
            )
        except Exception as e:
            logger.warning(f"Failed to register site intel schedules: {e}")

        # Register stale job recovery (resets jobs with stale heartbeats back to pending)
        try:
            from app.core.job_queue_service import reset_stale_jobs
            from app.core.scheduler_service import get_scheduler

            sched = get_scheduler()
            sched.add_job(
                reset_stale_jobs,
                "interval",
                minutes=5,
                id="reset_stale_queue_jobs",
                replace_existing=True,
            )
            logger.info("Stale queue job recovery registered (every 5 min)")
        except Exception as e:
            logger.warning(f"Failed to register stale job recovery: {e}")

        # Nightly batch collection DISABLED — run manually via POST endpoints
        # To re-enable: uncomment the block below
        # try:
        #     from app.core.nightly_batch_service import scheduled_nightly_batch
        #     from app.core.scheduler_service import get_scheduler
        #     from apscheduler.triggers.cron import CronTrigger
        #
        #     sched = get_scheduler()
        #     sched.add_job(
        #         scheduled_nightly_batch,
        #         trigger=CronTrigger(hour=2, minute=0),
        #         id="nightly_batch",
        #         name="Nightly Collection Batch",
        #         replace_existing=True,
        #     )
        #     logger.info("Nightly batch collection registered (2:00 AM UTC)")
        # except Exception as e:
        #     logger.warning(f"Failed to register nightly batch: {e}")

    except Exception as e:
        logger.warning(f"Failed to start scheduler: {e}")

    # Seed distributed rate limit buckets (idempotent — only creates missing rows)
    try:
        from app.core.rate_limiter import seed_rate_limit_buckets
        from app.core.database import get_session_factory

        SessionLocal = get_session_factory()
        seed_db = SessionLocal()
        try:
            created = seed_rate_limit_buckets(seed_db)
            if created:
                logger.info(f"Seeded {created} distributed rate limit buckets")
        finally:
            seed_db.close()
    except Exception as e:
        logger.warning(f"Failed to seed rate limit buckets: {e}")

    # Start PG LISTEN → EventBus bridge for live job progress
    try:
        from app.core.pg_listener import start_pg_listener

        await start_pg_listener()
        logger.info("PG listener started for job event streaming")
    except Exception as e:
        logger.warning(f"Failed to start PG listener: {e}")

    yield

    # Shutdown
    logger.info("Shutting down")

    # Stop PG listener
    try:
        from app.core.pg_listener import stop_pg_listener

        await stop_pg_listener()
        logger.info("PG listener stopped")
    except Exception as e:
        logger.warning(f"Error stopping PG listener: {e}")

    # Stop scheduler
    try:
        from app.core import scheduler_service

        scheduler_service.stop_scheduler()
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.warning(f"Error stopping scheduler: {e}")


# Create FastAPI app
app = FastAPI(
    title="Nexdata External Data Ingestion API",
    description="""
# 🚀 Welcome to Nexdata External Data Ingestion API

## 🎯 What Is This?

**Nexdata** is a unified REST API that provides programmatic access to **10+ major U.S. public data sources**. Instead of learning different APIs for each data provider, writing custom scraping code, or manually downloading datasets, you can use one consistent interface to ingest, store, and query data from multiple sources—all automatically stored in PostgreSQL with proper schemas.

**Perfect for:**
- 📊 Data scientists building analytical datasets
- 💼 Financial analysts tracking economic indicators
- 🏗️ Researchers combining multiple data sources
- 🏢 Developers building data-driven applications
- 📈 Analysts creating dashboards and reports

---

## 📚 Available Data Sources (10+ Sources, 50+ Endpoints)

### 📊 U.S. Census Bureau
**What:** Demographics, housing, and economic characteristics for every geography in the U.S.

**Available Data:**
- **ACS 5-Year Survey** (2009-2023): Most detailed demographic data
- **Population**: Age, sex, race, ethnicity (B01001, B01002, B01003)
- **Income**: Household income, poverty status, earnings (B19013, B19001, B19301)
- **Housing**: Occupancy, values, costs, units (B25001, B25003, B25077)
- **Employment**: Labor force, occupation, commute (B23025, C24010)
- **Education**: School enrollment, attainment (B14001, B15003)

**Geographic Levels:** Nation, State, County, Census Tract, Block Group, ZIP Code, Metropolitan Area

**Typical Use Cases:**
- Market research by geography
- Site selection analysis
- Demographic profiling
- Community needs assessment

---

### 💰 Federal Reserve Economic Data (FRED)
**What:** 800,000+ economic time series from the Federal Reserve Bank of St. Louis

**Available Data:**
- **National Accounts**: GDP, GNP, personal income, disposable income
- **Employment**: Unemployment rates, job openings, initial claims, labor force participation
- **Prices**: CPI, PPI, PCE, inflation rates
- **Money & Banking**: Interest rates, monetary aggregates, credit conditions
- **Production**: Industrial production, capacity utilization, manufacturing
- **International**: Exchange rates, trade balance, foreign transactions

**Frequencies:** Daily, weekly, monthly, quarterly, annual

**Typical Use Cases:**
- Economic modeling and forecasting
- Interest rate analysis
- Inflation tracking
- Macroeconomic research
- Investment strategy backtesting

---

### ⚡ Energy Information Administration (EIA)
**What:** Comprehensive energy statistics for the United States

**Available Data:**
- **Petroleum**: Crude oil prices (WTI, Brent), gasoline prices, refinery operations
- **Natural Gas**: Production, consumption, storage, prices by region
- **Electricity**: Generation by fuel type, retail sales, wholesale prices
- **Coal**: Production, consumption, exports, stocks
- **Renewables**: Solar, wind, hydro generation and capacity
- **Forecasts**: Short-term energy outlook (STEO) projections

**Typical Use Cases:**
- Energy market analysis
- Commodity trading strategies
- Environmental impact studies
- Energy consumption forecasting
- Policy analysis

---

### 🏛️ Securities and Exchange Commission (SEC)
**What:** Corporate financial filings and investment adviser information

**Available Data:**
- **Company Financials**: Structured data from 10-K and 10-Q filings
- **Company Facts**: Assets, revenue, earnings, shares outstanding
- **Form ADV**: Investment adviser registrations and disclosures
- **Family Offices**: Private wealth management firms and contact information
- **Real-time Access**: Latest filings from EDGAR database

**Coverage:** 10,000+ public companies, 15,000+ registered investment advisers

**Typical Use Cases:**
- Financial statement analysis
- Investment research
- Due diligence
- Family office identification
- Regulatory compliance monitoring

---

### 🌦️ NOAA Weather & Climate
**What:** Weather observations and historical climate data from NOAA

**Available Data:**
- **Weather Observations**: Temperature, precipitation, wind, humidity
- **Climate Normals**: Historical averages and extremes
- **Severe Weather**: Storms, warnings, alerts
- **Station Data**: 10,000+ weather stations nationwide
- **Historical Archive**: Data going back 100+ years

**Typical Use Cases:**
- Climate change analysis
- Agriculture planning
- Risk assessment
- Travel and logistics optimization
- Historical trend analysis

---

### 🏠 Real Estate & Housing Data
**What:** Property values, rental prices, and housing market indicators

**Available Data:**
- **FHFA House Price Index**: Official U.S. house price trends
- **HUD Building Permits**: New construction permits nationwide
- **Redfin Market Data**: Home sales, inventory, prices by market
- **OpenStreetMap Buildings**: Building footprints and attributes
- **Time Series**: Historical trends and forecasts

**Geographic Coverage:** National, state, metro, county, ZIP code

**Typical Use Cases:**
- Real estate investment analysis
- Market trend analysis
- Housing affordability studies
- Property valuation modeling
- Development planning

---

### 🗺️ Geographic Boundaries (GeoJSON)
**What:** Geographic boundary files for mapping and spatial analysis

**Available Data:**
- **State Boundaries**: All 50 states + DC, territories
- **County Boundaries**: 3,000+ counties
- **Census Tract Boundaries**: 80,000+ tracts
- **ZIP Code Boundaries**: 40,000+ ZIP codes
- **Metro Area Boundaries**: Combined Statistical Areas

**Format:** GeoJSON (easily usable in mapping libraries)

**Typical Use Cases:**
- Interactive mapping applications
- Spatial data visualization
- Geographic analysis
- Location-based services

---

## 🎯 How to Use This API (3 Simple Steps)

### Step 1️⃣: Start an Ingestion Job

Use **POST /api/v1/jobs** to ingest data from any source. The API is source-agnostic—just specify the source and configuration.

**Example: Ingest Census Population Data**
```json
POST /api/v1/jobs
{
  "source": "census",
  "config": {
    "survey": "acs5",
    "year": 2023,
    "table_id": "B01001",
    "geo_level": "state"
  }
}
```

**Example: Ingest Economic Time Series**
```json
POST /api/v1/jobs
{
  "source": "fred",
  "config": {
    "series_id": "UNRATE",
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
  }
}
```

**Example: Ingest Energy Prices**
```json
POST /api/v1/jobs
{
  "source": "eia",
  "config": {
    "series_id": "PET.RWTC.W",
    "frequency": "weekly"
  }
}
```

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "pending",
  "source": "census",
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

### Step 2️⃣: Monitor Job Progress

Track your ingestion job with **GET /api/v1/jobs/{job_id}**

**Job Lifecycle:**
```
pending → running → success (or failed)
```

**Example Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "success",
  "source": "census",
  "rows_affected": 52,
  "started_at": "2024-01-15T10:30:05Z",
  "completed_at": "2024-01-15T10:30:45Z",
  "error_message": null,
  "config": {...}
}
```

**Status Indicators:**
- ✅ **success**: Data ingested successfully
- ⚠️ **failed**: Error occurred (see error_message)
- 🔄 **running**: Ingestion in progress
- ⏳ **pending**: Job queued, not started yet

---

### Step 3️⃣: Query Your Data

Once ingested, data is stored in PostgreSQL with strongly-typed schemas:

**Option A: Direct SQL Access**
```sql
-- Connect to PostgreSQL
psql -h localhost -U nexdata -d nexdata

-- Query ingested census data
SELECT * FROM acs5_2023_b01001 WHERE state = '34' LIMIT 10;

-- Query FRED time series
SELECT * FROM fred_series WHERE series_id = 'UNRATE' ORDER BY date DESC;

-- Query SEC company facts
SELECT * FROM sec_company_facts WHERE cik = '0000320193';
```

**Option B: Source-Specific API Endpoints**

Many sources provide query endpoints:
- **Census**: Browse metadata, search variables
- **FRED**: Search series, browse categories
- **SEC**: Query Form ADV firms, get company details
- **GeoJSON**: Get boundaries, search locations

---

## 🔑 Authentication & API Keys

### Which Sources Need API Keys?

| Source | API Key Required? | How to Get | Time to Get |
|--------|-------------------|------------|-------------|
| **Census** | Recommended (higher limits) | [Get Key](https://api.census.gov/data/key_signup.html) | 1 min |
| **FRED** | ✅ Required | [Get Key](https://fred.stlouisfed.org/docs/api/api_key.html) | 2 min |
| **EIA** | ✅ Required | [Get Key](https://www.eia.gov/opendata/register.php) | 2 min |
| **NOAA** | Optional (per-request) | [Get Token](https://www.ncdc.noaa.gov/cdo-web/token) | 2 min |
| **SEC** | ❌ Not required | N/A | - |

### How to Configure API Keys

Set environment variables in your `.env` file:
```bash
CENSUS_SURVEY_API_KEY=your_census_key_here
FRED_API_KEY=your_fred_key_here
EIA_API_KEY=your_eia_key_here

# Optional: Configure rate limits
MAX_CONCURRENCY=5
MAX_REQUESTS_PER_SECOND=10
```

**All API keys are free** and take 1-2 minutes to obtain. You only need keys for sources you plan to use.

---

## 💡 Common Use Cases & Examples

### Use Case 1: Economic Dashboard
**Goal:** Track unemployment, GDP, and inflation

1. Ingest FRED series: UNRATE, GDP, CPIAUCSL
2. Query time series data
3. Build dashboard with real-time updates

### Use Case 2: Market Research
**Goal:** Analyze demographics for site selection

1. Ingest Census ACS data for target counties
2. Pull income, population, education tables
3. Join with geographic boundaries for mapping

### Use Case 3: Investment Research
**Goal:** Analyze SEC filings for portfolio companies

1. Ingest company financials via SEC endpoints
2. Track quarterly earnings and balance sheets
3. Monitor Form ADV filings for institutional investors

### Use Case 4: Energy Trading
**Goal:** Track crude oil and natural gas prices

1. Ingest EIA petroleum and natural gas series
2. Get daily/weekly price updates
3. Combine with FRED economic indicators

### Use Case 5: Housing Market Analysis
**Goal:** Track home prices and construction

1. Ingest FHFA House Price Index
2. Pull HUD building permits
3. Combine with Census demographic data

---

## 🛠️ Technical Architecture

### Tech Stack
- **FastAPI**: Modern, fast Python web framework with automatic API documentation
- **PostgreSQL**: Robust relational database for structured data storage
- **SQLAlchemy**: Python ORM for database operations
- **httpx**: Async HTTP client with connection pooling and retry logic
- **Docker**: Containerized PostgreSQL for easy setup
- **Pydantic**: Data validation and serialization

### Design Principles

✅ **Plugin Architecture**: Each data source is self-contained and independently maintained

✅ **Job Tracking**: Every ingestion is tracked with status, timing, and row counts

✅ **Type Safety**: Strongly-typed database columns (no JSON blobs for data)

✅ **Rate Limiting**: Built-in respect for API rate limits with configurable concurrency

✅ **Error Handling**: Exponential backoff with jitter, graceful failure handling

✅ **Idempotency**: Safe to re-run ingestions without duplicating data

✅ **SQL Safety**: All queries use parameterization (no SQL injection risk)

### Data Storage

**Core Tables:**
- `ingestion_jobs`: Tracks all ingestion runs (status, errors, timing)
- `dataset_registry`: Metadata about available datasets

**Source-Specific Tables:**
- `acs5_2023_b01001`: Census ACS 5-year table B01001, year 2023
- `fred_series`: FRED economic time series observations
- `eia_petroleum_prices`: EIA petroleum price data
- `sec_company_facts`: SEC company financial facts
- And more...

---

## 📖 Documentation & Resources

### Interactive Documentation
- **This Page (Swagger UI)**: Interactive API testing and exploration
- **ReDoc**: [/redoc](/redoc) - Clean, readable documentation
- **OpenAPI Schema**: [/openapi.json](/openapi.json) - Machine-readable spec

### Getting Started
- **GitHub Repository**: [View on GitHub](https://github.com/yourusername/nexdata)
- **README**: Quick start guide and setup instructions
- **Health Check**: [/health](/health) - Check service status

### Import to Tools
- **Postman**: Import `/openapi.json` for full collection
- **Insomnia**: Import OpenAPI spec for all endpoints
- **Code Generation**: Use spec to generate client libraries (Python, TypeScript, Java, etc.)

---

## 🚦 Rate Limits & Best Practices

### Default Rate Limits
- **Max Concurrency**: 5 simultaneous requests
- **Requests Per Second**: 10 across all concurrent requests
- **Configurable**: Adjust via environment variables

### Best Practices

✅ **Start Small**: Test with small datasets before large ingestions

✅ **Monitor Jobs**: Always check job status before assuming success

✅ **Respect Limits**: Don't exceed provider rate limits

✅ **Handle Errors**: Jobs can fail—check error messages and retry

✅ **Use Batch Endpoints**: For multiple tables/series, use batch ingestion endpoints

✅ **Cache Results**: Store ingested data locally, don't re-ingest unnecessarily

---

## 🎉 Ready to Get Started?

### Try Your First Request

1. **Check Health**: GET `/health` - Verify service is running
2. **List Jobs**: GET `/api/v1/jobs` - See existing ingestion jobs
3. **Start Ingestion**: POST `/api/v1/jobs` - Ingest your first dataset
4. **Monitor Progress**: GET `/api/v1/jobs/{job_id}` - Track job status
5. **Query Data**: Connect to PostgreSQL or use query endpoints

### Explore Data Sources

Browse the endpoint sections below to see what's available:
- 📊 **census** - Demographics and housing
- 💰 **fred** - Economic indicators
- ⚡ **eia** - Energy data
- 🏛️ **sec** - Company financials
- 🌦️ **noaa** - Weather data
- 🏠 **realestate** - Housing market data

**👇 Scroll down to explore all endpoints organized by data source!**

---

**Questions or Issues?** Check the [GitHub repository](https://github.com/yourusername/nexdata) or open an issue.

**Built with ❤️ for the data community**
    """,
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "Nexdata External Data Ingestion",
        "url": "https://github.com/yourusername/nexdata",
    },
    license_info={"name": "MIT", "url": "https://opensource.org/licenses/MIT"},
    openapi_tags=[
        # ── Core / System ──────────────────────────────────────────────
        {"name": "Root", "description": "Service information and health checks"},
        {"name": "graphql", "description": "🔗 **GraphQL API** - Flexible query interface for cross-domain data access"},
        {"name": "jobs", "description": "⚙️ **Ingestion Job Management** - Start, monitor, and track data ingestion jobs"},
        {"name": "schedules", "description": "📅 **Scheduled Ingestion** - Automated data refresh with cron-based scheduling for all data sources"},
        {"name": "job-chains", "description": "🔗 **Job Dependency Chains** - Create DAG workflows with job dependencies, execute chains, and track progress"},
        {"name": "Job Queue", "description": "Distributed job queue - live streaming, active jobs, and queue status"},
        {"name": "Job Monitor", "description": "Live jobs monitoring dashboard with real-time SSE streaming"},
        {"name": "webhooks", "description": "🔔 **Webhook Notifications** - Configure webhooks to receive notifications for job events and monitoring alerts"},
        {"name": "rate-limits", "description": "⚡ **Per-Source Rate Limits** - Configure and monitor rate limits for each data source API"},
        {"name": "data-quality", "description": "✅ **Data Quality Rules Engine** - Define and evaluate data quality rules with range, null, regex, freshness checks"},
        {"name": "templates", "description": "📋 **Bulk Ingestion Templates** - Reusable templates for multi-source data ingestion with variable substitution"},
        {"name": "lineage", "description": "🔗 **Data Lineage Tracking** - Track data provenance, transformations, dataset versions, and impact analysis"},
        {"name": "export", "description": "📤 **Data Export** - Export table data to CSV, JSON, or Parquet files"},
        {"name": "import", "description": "📥 **Bulk Portfolio Import** - Upload CSV/Excel files to import portfolio data with validation, preview, and rollback"},
        {"name": "freshness", "description": "📊 **Data Freshness** - Monitor source staleness, auto-refresh status, and incremental loading"},
        {"name": "Source Configuration", "description": "Per-source timeouts, retry policies, and rate limits"},
        {"name": "Audit Trail", "description": "Collection audit trail - who triggered what, when, and how"},
        {"name": "Settings", "description": "Application settings - manage external source API keys"},
        {"name": "LLM Costs", "description": "💰 **LLM Cost Tracking** - Monitor token usage and costs across all LLM-powered features"},
        {"name": "workflows", "description": "🔄 **Workflows** - Automated multi-step data processing workflows"},
        # ── Auth & Access ──────────────────────────────────────────────
        {"name": "auth", "description": "🔐 **Authentication** - User registration, login, JWT tokens, and password management"},
        {"name": "workspaces", "description": "👥 **Workspaces** - Team collaboration spaces with member management and role-based access"},
        {"name": "API Keys", "description": "🔑 **API Key Management** - Create, list, update, and revoke API keys for public API access"},
        {"name": "Public API", "description": "🌐 **Public API** - Protected endpoints for external developers with API key authentication and rate limiting"},
        # ── Government / Economic Data ─────────────────────────────────
        {"name": "census-batch", "description": "📊 **U.S. Census Bureau - Batch** - Bulk census data ingestion"},
        {"name": "census-geography", "description": "📊 **U.S. Census Bureau - Geography** - Geographic hierarchy and FIPS codes"},
        {"name": "census-metadata", "description": "📊 **U.S. Census Bureau - Metadata** - Dataset catalogs and variable discovery"},
        {"name": "fred", "description": "💰 **Federal Reserve Economic Data** - 800K+ economic time series"},
        {"name": "eia", "description": "⚡ **Energy Information Administration** - Energy production, prices, and consumption"},
        {"name": "bea", "description": "📈 **Bureau of Economic Analysis** - GDP, Personal Income, PCE, Regional economic data, and International transactions"},
        {"name": "BLS Labor Statistics", "description": "📊 **Bureau of Labor Statistics** - Employment, unemployment, CPI, PPI, JOLTS job openings and labor turnover"},
        {"name": "bts", "description": "🚚 **Bureau of Transportation Statistics** - Border crossings, freight flows (FAF5), and vehicle miles traveled"},
        {"name": "fema", "description": "🌊 **OpenFEMA** - Disaster declarations, Public Assistance grants, and Hazard Mitigation projects"},
        {"name": "fbi_crime", "description": "🚔 **FBI Crime Data** - UCR crime statistics, NIBRS incident data, hate crimes, and LEOKA"},
        {"name": "irs-soi", "description": "💰 **IRS Statistics of Income (SOI)** - Income/wealth distribution by geography: ZIP code income, county income, migration flows, business income"},
        {"name": "Treasury FiscalData", "description": "💵 **U.S. Treasury FiscalData** - Federal debt, interest rates, revenue/spending, Treasury auction results"},
        {"name": "FDIC BankFind", "description": "🏦 **FDIC BankFind Suite** - Bank financials, demographics, failed banks, and branch-level deposits for 4,000+ U.S. banks"},
        {"name": "FCC Broadband & Telecom", "description": "📡 **FCC National Broadband Map** - Broadband coverage, ISP availability, technology deployment, digital divide metrics"},
        {"name": "CFTC COT", "description": "📈 **CFTC Commitments of Traders** - Weekly futures positioning data: commercial vs non-commercial, managed money, swap dealers"},
        {"name": "USDA Agriculture", "description": "🌾 **USDA NASS QuickStats** - Agricultural statistics: crop production, yields, prices, livestock inventory"},
        {"name": "us_trade", "description": "🚢 **US International Trade** - Census Bureau trade data: imports/exports by HS code, port, state, and trading partner"},
        {"name": "cms", "description": "🏥 **CMS / HHS Healthcare Data** - Medicare utilization, hospital costs, and drug pricing"},
        {"name": "data_commons", "description": "📊 **Google Data Commons** - Unified public data from 200+ sources (demographics, economy, health, crime, etc.)"},
        {"name": "international_econ", "description": "🌍 **International Economic Data** - World Bank, IMF, OECD, BIS global economic indicators"},
        {"name": "dunl", "description": "🔗 **DUNL (S&P Data Unlocked)** - Open reference data: currencies, ports, UOM, calendars"},
        # ── Securities / Finance ───────────────────────────────────────
        {"name": "SEC EDGAR", "description": "🏛️ **SEC EDGAR** - Company filings, financials, and regulatory disclosures"},
        {"name": "form-adv", "description": "📋 **SEC Form ADV** - Investment adviser registrations, AUM, client types, and regulatory information"},
        {"name": "Form ADV - Ingestion", "description": "📋 **Form ADV Ingestion** - Bulk ingest Form ADV filings from SEC IAPD"},
        {"name": "Form ADV - Query", "description": "📋 **Form ADV Query** - Search and filter investment adviser registrations"},
        {"name": "form-d", "description": "📄 **SEC Form D** - Private placement filings and exempt offering data"},
        {"name": "benchmarks", "description": "📊 **Financial Benchmarks** - Industry multiples, valuation comps, and financial benchmarks"},
        {"name": "13F Analysis", "description": "📊 **13F Quarterly Analysis** - Quarter-over-quarter holding diffs and cross-investor convergence detection"},
        # ── PE Intelligence ────────────────────────────────────────────
        {"name": "PE Intelligence - Firms", "description": "🏢 **PE Firms** - Private equity firm profiles, fund data, and investment strategies"},
        {"name": "PE Intelligence - Portfolio Companies", "description": "🏭 **PE Portfolio Companies** - Track portfolio companies across PE firms"},
        {"name": "PE Intelligence - People", "description": "👥 **PE People** - Investment professionals, operating partners, and advisory boards"},
        {"name": "PE Intelligence - Deals", "description": "💰 **PE Deals** - M&A transactions, add-ons, exits, and deal multiples"},
        {"name": "PE Intelligence - Collection", "description": "⚙️ **PE Data Collection** - Automated PE data collection pipelines"},
        # ── Site Intelligence ──────────────────────────────────────────
        {"name": "Site Intel - Power", "description": "⚡ **Power Infrastructure** - Power plants, substations, and energy capacity near sites"},
        {"name": "Site Intel - Telecom", "description": "📡 **Telecom Infrastructure** - Cell towers, fiber routes, and broadband availability"},
        {"name": "Site Intel - Transport", "description": "🚚 **Transportation** - Highways, rail, ports, airports near industrial sites"},
        {"name": "Site Intel - Labor", "description": "👷 **Labor Market** - Local workforce availability, wages, and skills"},
        {"name": "Site Intel - Risk", "description": "⚠️ **Risk Assessment** - Natural hazards, environmental risk, and regulatory risk"},
        {"name": "Site Intel - Incentives", "description": "💵 **Incentives & Tax Credits** - State/local incentives, opportunity zones, enterprise zones"},
        {"name": "Site Intel - Logistics", "description": "📦 **Logistics** - Warehousing, 3PL, and supply chain infrastructure"},
        {"name": "Site Intel - Water & Utilities", "description": "💧 **Water & Utilities** - Water supply, wastewater, and utility infrastructure"},
        {"name": "Site Intel - Scoring", "description": "📊 **Site Scoring** - Composite site quality scores across all intelligence domains"},
        # ── People & Leadership ────────────────────────────────────────
        {"name": "People & Leadership", "description": "👥 **People & Leadership** - Executive search, leadership profiles, and career history"},
        {"name": "Company Leadership", "description": "🏢 **Company Leadership** - Company leadership teams, org charts, and leadership changes"},
        {"name": "Collection Jobs", "description": "⚙️ **People Collection Jobs** - Manage leadership data collection jobs and batch processing"},
        {"name": "People Collection Jobs", "description": "⚙️ **People Collection Jobs** - Batch people data collection and pipeline management"},
        {"name": "People Portfolios", "description": "📁 **PE Portfolios** - Track leadership across portfolio companies"},
        {"name": "Peer Sets & Benchmarking", "description": "📊 **Peer Benchmarking** - Compare leadership structures across peer companies"},
        {"name": "People Watchlists", "description": "👁️ **Executive Watchlists** - Track specific executives and get change alerts"},
        {"name": "People Deduplication", "description": "🔗 **People Deduplication** - Scan, review, and merge duplicate person records"},
        {"name": "People Analytics", "description": "📊 **People Analytics** - Leadership analytics, tenure analysis, and executive benchmarking"},
        {"name": "People Data Quality", "description": "✅ **People Data Quality** - Completeness scoring, stale record detection, and data health metrics"},
        {"name": "People Reports", "description": "📝 **People Reports** - Generate leadership reports and org chart exports"},
        # ── Family Offices ─────────────────────────────────────────────
        {"name": "Family Offices - Query", "description": "💼 **Family Offices - Query** - Search and filter family office data"},
        {"name": "Family Offices - Tracking", "description": "👥 **Family Offices - Tracking** - Contact research and tracking for family offices"},
        {"name": "family_office_contacts", "description": "👥 **Family Office Contacts** - Contact research and enrichment for family offices"},
        {"name": "Family Office Collection", "description": "👨‍👩‍👧 **Family Office Data Collection** - Continuous data collection for 300+ family offices worldwide"},
        # ── LP / Investor Intelligence ─────────────────────────────────
        {"name": "LP Collection", "description": "🏦 **LP Data Collection** - Continuous data collection for 100+ institutional investors (pensions, SWFs, endowments)"},
        {"name": "Agentic Portfolio Research", "description": "🤖 **Agentic Portfolio Discovery** - AI-powered portfolio research for LPs and Family Offices using SEC 13F, website scraping, and more"},
        {"name": "lp_allocation", "description": "📊 **LP Allocation Gap Analysis** - Target vs current allocation gaps showing where LP capital must be deployed"},
        {"name": "Portfolio Comparison", "description": "🔀 **Portfolio Comparison** - Compare investor portfolios side-by-side, track historical changes, and analyze industry allocations"},
        {"name": "Portfolio Alerts", "description": "🔔 **Portfolio Alerts** - Automated alerts for portfolio changes, position shifts, and new filings"},
        {"name": "Watchlists & Saved Searches", "description": "📌 **Watchlists & Saved Searches** - Create watchlists to track investors/companies, save and re-execute search queries"},
        # ── Discovery & Search ─────────────────────────────────────────
        {"name": "Discovery & Recommendations", "description": "🔍 **Discovery & Recommendations** - AI-powered deal sourcing and investment recommendations"},
        {"name": "Search", "description": "🔎 **Search** - Full-text and semantic search across all data sources"},
        {"name": "Market Scanner", "description": "📡 **Market Scanner** - Scan markets for investment signals and opportunities"},
        {"name": "Competitive Intelligence", "description": "🏆 **Competitive Intelligence** - Competitive landscape analysis and market positioning"},
        {"name": "Entity Resolution", "description": "🔗 **Entity Resolution** - Match and deduplicate companies across data sources"},
        # ── Deals & Scoring ────────────────────────────────────────────
        {"name": "deals", "description": "💼 **Deal Flow Tracker** - Track investment opportunities through pipeline stages from sourcing to close"},
        {"name": "Deal Predictions", "description": "🎯 **Predictive Deal Scoring** - Win probability predictions, pipeline insights, and similar deal analysis"},
        {"name": "Deal Models", "description": "📈 **Deal Modeling** - PE roll-up deal modeling, scenarios, sensitivity analysis, and IC memo generation"},
        {"name": "exit_readiness", "description": "🚪 **Exit Readiness Score** - 7-signal composite score for PE portfolio exit timing"},
        {"name": "acquisition_targets", "description": "🎯 **Acquisition Target Score** - 5-signal score identifying attractive PE acquisition targets"},
        {"name": "Company Scores", "description": "📊 **Company Scoring** - ML-based health scores for portfolio companies (0-100 with category breakdowns)"},
        {"name": "company_health", "description": "🏥 **Private Company Health Score** - Multi-signal health proxy combining hiring, web traffic, sentiment, and foot traffic"},
        # ── AI Agents ──────────────────────────────────────────────────
        {"name": "Agentic Intelligence", "description": "🤖 **Autonomous AI Research** - AI agents that autonomously research companies across all data sources"},
        {"name": "Due Diligence", "description": "📋 **Automated Due Diligence** - AI-powered risk analysis and DD report generation"},
        {"name": "Data Hunter", "description": "🎯 **Autonomous Data Hunter** - AI agent that finds and fills missing data with provenance tracking"},
        {"name": "Anomaly Detection", "description": "🚨 **Anomaly Detection** - AI agent that detects unusual patterns and changes across data sources"},
        {"name": "Report Generation", "description": "📝 **Report Generation** - AI agent that generates comprehensive natural language reports"},
        # ── Alternative Data ───────────────────────────────────────────
        {"name": "yelp", "description": "🏪 **Yelp Fusion** - Business listings, reviews, and local business activity (500 calls/day free tier)"},
        {"name": "Foot Traffic", "description": "🚶 **Foot Traffic Intelligence** - Location discovery, foot traffic data collection, and competitive benchmarking for retail/hospitality investments"},
        {"name": "Prediction Markets", "description": "🎲 **Prediction Market Intelligence** - Monitor Kalshi, Polymarket for market consensus on economic, political, sports, and world events"},
        {"name": "Glassdoor", "description": "👥 **Glassdoor Data** - Company reviews, ratings, and salary data for talent intelligence"},
        {"name": "App Store Rankings", "description": "📱 **App Store Rankings** - iOS and Android app metrics, ratings, and ranking history"},
        {"name": "App Stores", "description": "📱 **App Stores** - App store data collection and monitoring"},
        {"name": "web-traffic", "description": "📊 **Web Traffic** - Website traffic intelligence from Tranco rankings and SimilarWeb"},
        {"name": "github", "description": "💻 **GitHub Analytics** - Repository metrics, developer velocity, and contributor trends"},
        {"name": "job_postings", "description": "💼 **Job Posting Intelligence** - Track hiring across companies via ATS APIs (Greenhouse, Lever, Workday, Ashby)"},
        {"name": "hiring_velocity", "description": "📈 **Hiring Velocity Score** - Cross-reference job postings with BLS baselines for expansion/contraction signals"},
        {"name": "News", "description": "📰 **News & Events** - Aggregated news from SEC EDGAR, Google News, and press releases for investors and portfolio companies"},
        {"name": "News Monitor", "description": "📰 **News Monitor** - Continuous news monitoring and alerting for tracked entities"},
        # ── Verticals ──────────────────────────────────────────────────
        {"name": "Real Estate / Housing", "description": "🏠 **Real Estate / Housing** - Zillow home values, rental market data, and housing analytics"},
        {"name": "ZIP Intelligence", "description": "📍 **ZIP Med-Spa Score** - Revenue potential scoring for US ZIPs based on IRS SOI income data"},
        {"name": "Med-Spa Discovery", "description": "💈 **Med-Spa Discovery** - Discover and rank med-spa acquisition prospects via Yelp + ZIP affluence scores"},
        # ── Other Data Sources ─────────────────────────────────────────
        {"name": "geojson", "description": "🗺️ **Geographic Boundaries** - GeoJSON boundaries for mapping"},
        {"name": "kaggle", "description": "🏆 **Kaggle Datasets** - Competition datasets (M5 Forecasting, etc.)"},
        {"name": "uspto", "description": "🔬 **USPTO Patent Data** - US patent search, inventors, assignees, and CPC classifications via PatentsView API"},
        {"name": "corporate-registry", "description": "🏢 **Corporate Registry** - Global company registry data from OpenCorporates (140+ jurisdictions)"},
        {"name": "OpenCorporates", "description": "🏢 **OpenCorporates** - Open corporate data search and entity lookup"},
        {"name": "network", "description": "🔗 **Network Analysis** - Relationship mapping and network visualization across entities"},
        # ── Analytics & Reporting ──────────────────────────────────────
        {"name": "Dashboard Analytics", "description": "📊 **Dashboard Analytics** - Pre-computed analytics for frontend dashboards: system overview, investor insights, trends, and industry breakdowns"},
        {"name": "Trends", "description": "📈 **Investment Trends** - Sector rotation, emerging themes, geographic shifts, and allocation trends across LP portfolios"},
        {"name": "enrichment", "description": "🔬 **Company Data Enrichment** - Enrich portfolio companies with SEC financials, funding data, employee counts, and industry classification"},
        {"name": "Reports", "description": "📊 **Custom Reports** - Generate investor profiles, portfolio summaries, and trend analysis as HTML/Excel reports"},
    ],
)

# CORS middleware — restrict to known frontend origins
_cors_origins = [
    "http://localhost:3001",  # frontend dev (nginx)
    "http://localhost:5173",  # frontend dev (vite)
    "http://127.0.0.1:3001",
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Global exception handlers — prevent leaking internal details to callers
# ---------------------------------------------------------------------------
_main_logger = logging.getLogger("app.main")


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all: log the real error, return a generic message."""
    _main_logger.error(
        "Unhandled exception on %s %s: %s",
        request.method,
        request.url.path,
        exc,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# Auth dependency for protected routers
# Set REQUIRE_AUTH=true to enforce JWT on all protected routes (default: off for dev)
_require_auth = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
_auth = [Depends(get_current_user)] if _require_auth else []

# Public routers (no auth required)
app.include_router(auth.router, prefix="/api/v1")  # login/register must be public
app.include_router(public.router, prefix="/api/v1")  # has its own API key auth
app.include_router(job_stream.router, prefix="/api/v1")  # SSE streaming
app.include_router(jobs_monitor.router, prefix="/api/v1", dependencies=_auth)  # Jobs dashboard

# Protected routers
app.include_router(jobs.router, prefix="/api/v1", dependencies=_auth)
app.include_router(census_geo.router, prefix="/api/v1", dependencies=_auth)
app.include_router(census_batch.router, prefix="/api/v1", dependencies=_auth)
app.include_router(metadata.router, prefix="/api/v1", dependencies=_auth)
app.include_router(geojson.router, prefix="/api/v1", dependencies=_auth)
app.include_router(fred.router, prefix="/api/v1", dependencies=_auth)
app.include_router(eia.router, prefix="/api/v1", dependencies=_auth)
app.include_router(sec.router, prefix="/api/v1", dependencies=_auth)
app.include_router(realestate.router, prefix="/api/v1", dependencies=_auth)
app.include_router(family_offices.router, prefix="/api/v1", dependencies=_auth)
app.include_router(
    family_office_contacts.router,
    prefix="/api/v1",
    tags=["family_office_contacts"],
    dependencies=_auth,
)
app.include_router(cms.router, prefix="/api/v1", dependencies=_auth)
app.include_router(kaggle.router, prefix="/api/v1", dependencies=_auth)
app.include_router(international_econ.router, prefix="/api/v1", dependencies=_auth)
app.include_router(fbi_crime.router, prefix="/api/v1", dependencies=_auth)
app.include_router(bts.router, prefix="/api/v1", dependencies=_auth)
app.include_router(bea.router, prefix="/api/v1", dependencies=_auth)
app.include_router(fema.router, prefix="/api/v1", dependencies=_auth)
app.include_router(data_commons.router, prefix="/api/v1", dependencies=_auth)
app.include_router(yelp.router, prefix="/api/v1", dependencies=_auth)
app.include_router(us_trade.router, prefix="/api/v1", dependencies=_auth)
app.include_router(cftc_cot.router, prefix="/api/v1", dependencies=_auth)
app.include_router(usda.router, prefix="/api/v1", dependencies=_auth)
app.include_router(bls.router, prefix="/api/v1", dependencies=_auth)
app.include_router(fcc_broadband.router, prefix="/api/v1", dependencies=_auth)
app.include_router(treasury.router, prefix="/api/v1", dependencies=_auth)
app.include_router(fdic.router, prefix="/api/v1", dependencies=_auth)
app.include_router(irs_soi.router, prefix="/api/v1", dependencies=_auth)
app.include_router(foot_traffic.router, prefix="/api/v1", dependencies=_auth)
app.include_router(dunl.router, prefix="/api/v1", dependencies=_auth)
app.include_router(prediction_markets.router, prefix="/api/v1", dependencies=_auth)
app.include_router(schedules.router, prefix="/api/v1", dependencies=_auth)
app.include_router(webhooks.router, prefix="/api/v1", dependencies=_auth)
app.include_router(chains.router, prefix="/api/v1", dependencies=_auth)
app.include_router(rate_limits.router, prefix="/api/v1", dependencies=_auth)
app.include_router(data_quality.router, prefix="/api/v1", dependencies=_auth)
app.include_router(templates.router, prefix="/api/v1", dependencies=_auth)
app.include_router(lineage.router, prefix="/api/v1", dependencies=_auth)
app.include_router(export.router, prefix="/api/v1", dependencies=_auth)
app.include_router(uspto.router, prefix="/api/v1", dependencies=_auth)
app.include_router(agentic_research.router, prefix="/api/v1", dependencies=_auth)
app.include_router(alerts.router, prefix="/api/v1", dependencies=_auth)
app.include_router(search.router, prefix="/api/v1", dependencies=_auth)
app.include_router(discover.router, prefix="/api/v1", dependencies=_auth)
app.include_router(watchlists.router, prefix="/api/v1", dependencies=_auth)
app.include_router(analytics.router, prefix="/api/v1", dependencies=_auth)
app.include_router(compare.router, prefix="/api/v1", dependencies=_auth)
app.include_router(api_keys.router, prefix="/api/v1", dependencies=_auth)
app.include_router(network.router, prefix="/api/v1", dependencies=_auth)
app.include_router(trends.router, prefix="/api/v1", dependencies=_auth)
app.include_router(enrichment.router, prefix="/api/v1", dependencies=_auth)
app.include_router(import_portfolio.router, prefix="/api/v1", dependencies=_auth)
app.include_router(news.router, prefix="/api/v1", dependencies=_auth)
app.include_router(reports.router, prefix="/api/v1", dependencies=_auth)
app.include_router(deals.router, prefix="/api/v1", dependencies=_auth)
app.include_router(benchmarks.router, prefix="/api/v1", dependencies=_auth)
app.include_router(workspaces.router, prefix="/api/v1", dependencies=_auth)
app.include_router(form_d.router, prefix="/api/v1", dependencies=_auth)
app.include_router(corporate_registry.router, prefix="/api/v1", dependencies=_auth)
app.include_router(form_adv.router, prefix="/api/v1", dependencies=_auth)
app.include_router(web_traffic.router, prefix="/api/v1", dependencies=_auth)
app.include_router(github.router, prefix="/api/v1", dependencies=_auth)
app.include_router(scores.router, prefix="/api/v1", dependencies=_auth)
app.include_router(entities.router, prefix="/api/v1", dependencies=_auth)
app.include_router(glassdoor.router, prefix="/api/v1", dependencies=_auth)
app.include_router(app_stores.router, prefix="/api/v1", dependencies=_auth)
app.include_router(opencorporates.router, prefix="/api/v1", dependencies=_auth)
app.include_router(app_rankings.router, prefix="/api/v1", dependencies=_auth)
app.include_router(predictions.router, prefix="/api/v1", dependencies=_auth)
app.include_router(agents.router, prefix="/api/v1", dependencies=_auth)
app.include_router(diligence.router, prefix="/api/v1", dependencies=_auth)
app.include_router(monitors.router, prefix="/api/v1", dependencies=_auth)
app.include_router(competitive.router, prefix="/api/v1", dependencies=_auth)
app.include_router(hunter.router, prefix="/api/v1", dependencies=_auth)
app.include_router(anomalies.router, prefix="/api/v1", dependencies=_auth)
app.include_router(market.router, prefix="/api/v1", dependencies=_auth)
app.include_router(reports_gen.router, prefix="/api/v1", dependencies=_auth)
app.include_router(lp_collection.router, prefix="/api/v1", dependencies=_auth)
app.include_router(fo_collection.router, prefix="/api/v1", dependencies=_auth)

# PE Intelligence Platform
app.include_router(pe_firms.router, prefix="/api/v1", dependencies=_auth)
app.include_router(pe_companies.router, prefix="/api/v1", dependencies=_auth)
app.include_router(pe_people.router, prefix="/api/v1", dependencies=_auth)
app.include_router(pe_deals.router, prefix="/api/v1", dependencies=_auth)
app.include_router(pe_collection.router, prefix="/api/v1", dependencies=_auth)

# 13F Quarterly Analysis
app.include_router(quarterly_diff.router, prefix="/api/v1", dependencies=_auth)

# People & Org Chart Intelligence
app.include_router(people.router, prefix="/api/v1", dependencies=_auth)
app.include_router(companies_leadership.router, prefix="/api/v1", dependencies=_auth)
app.include_router(collection_jobs.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_portfolios.router, prefix="/api/v1", dependencies=_auth)
app.include_router(peer_sets.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_watchlists.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_analytics.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_reports.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_data_quality.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_dedup.router, prefix="/api/v1", dependencies=_auth)
app.include_router(people_jobs.router, prefix="/api/v1", dependencies=_auth)

# Job Posting Intelligence
app.include_router(job_postings.router, prefix="/api/v1", dependencies=_auth)
app.include_router(job_postings_velocity.router, prefix="/api/v1", dependencies=_auth)

# Derived Data Scores
app.include_router(health_scores.router, prefix="/api/v1", dependencies=_auth)
app.include_router(lp_allocation.router, prefix="/api/v1", dependencies=_auth)
app.include_router(exit_readiness.router, prefix="/api/v1", dependencies=_auth)
app.include_router(acquisition_targets.router, prefix="/api/v1", dependencies=_auth)
app.include_router(zip_scores.router, prefix="/api/v1", dependencies=_auth)
app.include_router(medspa_discovery.router, prefix="/api/v1", dependencies=_auth)
app.include_router(deal_models.router, prefix="/api/v1", dependencies=_auth)

# Site Intelligence Platform
app.include_router(site_intel_power.router, prefix="/api/v1", dependencies=_auth)
app.include_router(site_intel_telecom.router, prefix="/api/v1", dependencies=_auth)
app.include_router(site_intel_transport.router, prefix="/api/v1", dependencies=_auth)
app.include_router(site_intel_labor.router, prefix="/api/v1", dependencies=_auth)
app.include_router(site_intel_risk.router, prefix="/api/v1", dependencies=_auth)
app.include_router(site_intel_incentives.router, prefix="/api/v1", dependencies=_auth)
app.include_router(site_intel_logistics.router, prefix="/api/v1", dependencies=_auth)
app.include_router(
    site_intel_water_utilities.router, prefix="/api/v1", dependencies=_auth
)
app.include_router(site_intel_sites.router, prefix="/api/v1", dependencies=_auth)

# Collection Management
app.include_router(source_configs.router, prefix="/api/v1", dependencies=_auth)
app.include_router(audit.router, prefix="/api/v1", dependencies=_auth)

# Settings
app.include_router(settings_router.router, prefix="/api/v1", dependencies=_auth)

# Multi-Agent Orchestrator
app.include_router(workflows.router, prefix="/api/v1", dependencies=_auth)

# LLM Cost Tracking
app.include_router(llm_costs.router, prefix="/api/v1", dependencies=_auth)

# Data Freshness Dashboard
app.include_router(freshness.router, prefix="/api/v1", dependencies=_auth)

# GraphQL API
app.include_router(graphql_app, prefix="/graphql", tags=["graphql"])


@app.get("/", tags=["Root"])
def root():
    """
    Service information and quick links.

    Use this endpoint to verify the service is running and get links to documentation.
    """
    return {
        "service": "External Data Ingestion Service",
        "version": "0.1.0",
        "status": "running",
        "sources": [
            "census",
            "fred",
            "eia",
            "sec",
            "realestate",
            "noaa",
            "cms",
            "kaggle",
            "international_econ",
            "fbi_crime",
            "bts",
            "bea",
            "fema",
            "data_commons",
            "yelp",
            "us_trade",
            "cftc_cot",
            "usda",
            "bls",
            "fcc_broadband",
            "treasury",
            "fdic",
            "irs_soi",
            "agentic_portfolio",
            "foot_traffic",
            "prediction_markets",
            "uspto",
        ],
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "openapi_schema": "/openapi.json",
        },
        "featured_endpoints": {
            "form_adv_query": "/api/v1/sec/form-adv/firms",
            "form_adv_ingest": "/api/v1/sec/form-adv/ingest/family-offices",
            "job_status": "/api/v1/jobs/{job_id}",
            "health_check": "/health",
        },
    }


@app.get("/health")
def health_check():
    """
    Health check endpoint.

    Returns status of the service and database connectivity.
    """
    from app.core.database import get_engine
    from sqlalchemy import text

    health_status = {"status": "healthy", "service": "running", "database": "unknown"}

    # Check database connectivity
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        health_status["status"] = "degraded"
        health_status["database"] = f"error: {str(e)}"
        logger.warning(f"Database health check failed: {e}")

    return health_status
