# Nexdata Demo Guide

> Comprehensive guide for demonstrating Nexdata's AI-powered data ingestion and investment intelligence platform.

**Base URL:** `http://localhost:8001`
**API Docs:** `http://localhost:8001/docs`
**GraphQL:** `http://localhost:8001/graphql`

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

## 🤖 Agentic AI Features (The "Wow" Demos)

### Autonomous Company Research
*One call triggers research across 9+ sources in parallel*

```bash
# Start research on any company
curl -X POST http://localhost:8001/api/v1/agents/research/company \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe"}'

# Check results (usually ready in 2-5 seconds)
curl http://localhost:8001/api/v1/agents/research/{job_id}
```

**What it queries:**
- SEC filings (Form D, 10-K, 13F)
- GitHub (repos, activity, contributors)
- Glassdoor (ratings, reviews)
- App Store (rankings, ratings)
- Web traffic (Tranco data)
- News (recent coverage)
- Corporate registry

---

### Automated Due Diligence
*Complete DD report in under 60 seconds*

```bash
# Start DD process
curl -X POST http://localhost:8001/api/v1/diligence/start \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Anthropic", "template": "standard"}'

# Get results
curl http://localhost:8001/api/v1/diligence/{job_id}
```

**Output includes:**
- Risk score (0-100)
- Risk level (low/moderate/high/critical)
- Red flags by category (financial, legal, team, market, operations, tech)
- Executive summary with recommendation
- Structured memo for investment committee

---

### Company Health Scoring
*ML-powered quantified assessment*

```bash
curl http://localhost:8001/api/v1/scores/company/Stripe
```

**Output:**
```json
{
  "company_name": "Stripe",
  "composite_score": 72.5,
  "tier": "B",
  "category_scores": {
    "growth": 85,
    "stability": 70,
    "market": 65,
    "tech": 80
  }
}
```

---

### Portfolio Intelligence

```bash
# Co-investor network
curl http://localhost:8001/api/v1/network/investor/{id}

# Investment trends
curl http://localhost:8001/api/v1/trends/sectors

# Compare portfolios
curl -X POST http://localhost:8001/api/v1/compare/portfolios \
  -d '{"investor_ids": [1, 2]}' -H "Content-Type: application/json"
```

---

### Search & Discovery

```bash
# Full-text search (typo-tolerant)
curl "http://localhost:8001/api/v1/search?q=fintech%20payments"

# Autocomplete
curl "http://localhost:8001/api/v1/search/suggest?q=strip"

# Find similar investors
curl http://localhost:8001/api/v1/discover/similar/{id}
```

---

## 📊 Data Source Quick Reference

| Source | Endpoint | Example |
|--------|----------|---------|
| Census | `/api/v1/census/` | Population, income, housing |
| FRED | `/api/v1/fred/` | GDP, unemployment, CPI |
| EIA | `/api/v1/eia/` | Energy prices, generation |
| SEC | `/api/v1/sec/` | 10-K, 13F, Form D filings |
| BLS | `/api/v1/bls/` | Employment, wages |
| Treasury | `/api/v1/treasury/` | Rates, yields |
| GitHub | `/api/v1/github/` | Repos, activity |
| Glassdoor | `/api/v1/glassdoor/` | Ratings, reviews |
| App Store | `/api/v1/apps/` | Rankings, ratings |

**Total: 25+ sources, 100+ endpoints**

---

**Questions?** Check the docs or run:
```bash
python scripts/populate_demo_data.py --help
```

