# Demo Guide - External Data Ingestion Service

Quick guide to demonstrate the capabilities of the External Data Ingestion Service.

## 🚀 Quick Start Demo (30 seconds)

The fastest way to show the system works:

```bash
# 1. Start the service
python scripts/start_service.py

# 2. In another terminal, run quick demo
python scripts/quick_demo.py
```

**What happens:**
- Service health check
- Ingests 3 key datasets:
  - FRED GDP data (2020-present)
  - FRED Unemployment Rate (2020-present)
  - Census Population by State (2023)
- Shows progress and results
- Takes ~30 seconds total

## 📊 Full Demo (5 minutes)

For a comprehensive demonstration:

```bash
python scripts/populate_demo_data.py
```

**What it showcases:**

### U.S. Census Bureau
- ✅ ACS 5-Year population data by state
- ✅ Median household income by county (California)

### Federal Reserve (FRED)
- ✅ GDP (Gross Domestic Product)
- ✅ Unemployment Rate
- ✅ Consumer Price Index (CPI)
- ✅ Federal Funds Rate
- ✅ Treasury Yield Curve (10Y-2Y)
- ✅ Crude Oil Prices (WTI)
- ✅ M2 Money Supply
- ✅ Industrial Production Index

### Energy Information Administration (EIA)
- ✅ Retail gasoline prices (national)
- ✅ Natural gas prices
- ✅ Electricity generation (total US)

### SEC Corporate Filings
- ✅ Financial data for major tech companies:
  - Apple Inc.
  - Amazon.com
  - Alphabet (Google)
  - Meta (Facebook)
  - Microsoft

### Real Estate & Housing
- ✅ FHFA House Price Index (national)
- ✅ HUD Building Permits (national)

## 🎯 Quick Mode (2 minutes)

For faster demos with fewer datasets:

```bash
python scripts/populate_demo_data.py --quick
```

Ingests only the first dataset from each source.

## 🔍 Viewing the Results

### 1. API Documentation
Visit: http://localhost:8001/docs

Interactive Swagger UI with all endpoints.

### 2. Health Check
```bash
curl http://localhost:8001/health
```

Shows service and database status.

### 3. List All Jobs
```bash
curl http://localhost:8001/api/v1/jobs
```

Or visit: http://localhost:8001/api/v1/jobs

### 4. Get Specific Job
```bash
curl http://localhost:8001/api/v1/jobs/{job_id}
```

Shows status, rows ingested, errors, etc.

### 5. Query Ingested Data

Example - Get FRED GDP data:
```bash
curl http://localhost:8001/api/v1/fred/series/GDP/observations
```

Example - Get Census population:
```sql
-- Direct database query
SELECT * FROM acs5_2023_b01001 LIMIT 10;
```

## 📋 Demo Script for Presentations

### Introduction (1 minute)
"This is a multi-source data ingestion service that pulls data from public APIs into PostgreSQL. It's designed to handle multiple data sources with a plugin architecture."

### Show Architecture (2 minutes)
```bash
# Show directory structure
tree -L 2 app/

# Key points:
# - Core module: source-agnostic
# - Sources module: isolated adapters
# - API module: HTTP endpoints
```

### Quick Demo (2 minutes)
```bash
# Start service (if not running)
python scripts/start_service.py

# Run quick demo
python scripts/quick_demo.py
```

"In 30 seconds, we just ingested GDP, unemployment, and census data. The system handled API calls, rate limiting, and database insertion automatically."

### Show API Docs (2 minutes)
1. Open http://localhost:8001/docs
2. Expand `/api/v1/jobs` endpoint
3. Show example request
4. Execute a live request
5. Show job status endpoint

### Show Database (2 minutes)
```sql
-- Show ingestion tracking
SELECT * FROM ingestion_jobs ORDER BY created_at DESC LIMIT 5;

-- Show dataset registry
SELECT * FROM dataset_registry;

-- Show actual data
SELECT * FROM fred_gdp ORDER BY date DESC LIMIT 10;
```

### Highlight Features (3 minutes)

**1. Multi-Source Support:**
"We currently support Census, FRED, EIA, SEC, Real Estate, NOAA, and more."

**2. Job Tracking:**
"Every ingestion is tracked with status, row counts, and error messages."

**3. Rate Limiting:**
"Built-in rate limiting and bounded concurrency to respect API limits."

**4. Plugin Architecture:**
"Adding a new source is isolated - no changes to core code."

**5. Type Safety:**
"Data goes into typed database columns, not JSON blobs."

**6. Auto-Restart:**
"The startup script handles database health checks and auto-restart."

## 🎬 Live Coding Demo

### Add a New Dataset (5 minutes)

1. **Show how easy it is to add data:**

```bash
# Via API
curl -X POST http://localhost:8001/api/v1/fred/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "series_ids": ["MORTGAGE30US"],
    "observation_start": "2020-01-01"
  }'
```

2. **Check the job:**
```bash
# Get job ID from response, then check status
curl http://localhost:8001/api/v1/jobs/123
```

3. **Query the data:**
```bash
curl http://localhost:8001/api/v1/fred/series/MORTGAGE30US/observations
```

### Show Error Handling (3 minutes)

1. **Invalid request:**
```bash
curl -X POST http://localhost:8001/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source": "invalid_source",
    "config": {}
  }'
```

Shows validation error.

2. **Check failed jobs:**
```bash
curl http://localhost:8001/api/v1/jobs?status=failed
```

## 📈 Performance Metrics

After running full demo, show:

```bash
# Count total jobs
curl http://localhost:8001/api/v1/jobs | jq 'length'

# Count successful jobs
curl http://localhost:8001/api/v1/jobs | jq '[.[] | select(.status=="success")] | length'

# Total rows ingested
curl http://localhost:8001/api/v1/jobs | jq '[.[] | .rows_ingested // 0] | add'
```

## 🎓 Teaching Points

### Architecture Principles
1. **Source Isolation** - Each source is independent
2. **Core Neutrality** - Core doesn't know about specific sources
3. **Plugin Pattern** - Easy to extend
4. **Type Safety** - Strongly typed schemas
5. **Observability** - Job tracking and logging

### Safety & Compliance
1. **Rate Limiting** - Respects API limits
2. **Bounded Concurrency** - No unbounded parallelism
3. **Job Tracking** - Never "fire and forget"
4. **Error Handling** - Graceful failures with retries
5. **Data Safety** - No PII violations

### Extensibility
"To add a new source, you just:
1. Create a module in `app/sources/{name}/`
2. Implement the client and ingest logic
3. Add API routes
4. Register in main.py
That's it! No changes to core code."

## 🔧 Troubleshooting Demo Issues

### Service Won't Start
```bash
# Check if database is running
docker ps

# Check logs
docker-compose logs db

# Restart everything
docker-compose down
python scripts/start_service.py
```

### Jobs Failing
```bash
# Check API keys in .env
cat .env

# Check specific job error
curl http://localhost:8001/api/v1/jobs/{job_id}
```

### No Data After Ingestion
```bash
# Check job actually succeeded
curl http://localhost:8001/api/v1/jobs/{job_id}

# Check table exists
docker-compose exec db psql -U nexdata_user -d nexdata -c "\dt"
```

## 📚 Follow-Up Resources

After the demo, share:
- `README.md` - Main documentation
- `QUICKSTART.md` - Setup guide
- `RULES.md` - Development guidelines
- `docs/` - Source-specific documentation

## 🎯 Key Takeaways

1. **Multi-source ingestion** from public APIs
2. **Plugin architecture** for easy extensibility
3. **Job tracking** for observability
4. **Safety first** - rate limits, error handling, job tracking
5. **Production ready** - auto-restart, health checks, logging

---

**Questions?** Check the docs or run:
```bash
python scripts/populate_demo_data.py --help
```

