# External Data Ingestion Service

> **A unified API to ingest and query data from multiple public data sources**

This service provides a single FastAPI-based REST API that automatically fetches, structures, and stores data from major U.S. government and public data providers into PostgreSQL. Instead of writing custom code for each data source, use one consistent API to access census data, economic indicators, energy statistics, and more.

## 🎯 What Can You Do With This?

- **Census Bureau**: Population demographics, housing data, economic characteristics by geography
- **Federal Reserve (FRED)**: Interest rates, GDP, unemployment, inflation, and 800,000+ economic time series
- **Energy (EIA)**: Oil prices, electricity generation, natural gas production, retail gas prices
- **SEC**: Public company financials, Form ADV investment advisor filings
- **NOAA**: Weather and climate data, historical observations
- **Real Estate**: Zillow home value indices, rental market data
- **Public LP Strategies**: Investment holdings from public pension funds and endowments

All through one consistent API, with automatic rate limiting, error handling, and structured storage.

---

## 🚀 Quick Start (5 Minutes)

> **TL;DR:** Want to just run it? Skip to [Fast Track Setup](#-fast-track-setup) below.

### Prerequisites

Before you begin, make sure you have:

1. **Python 3.11 or higher** ([Download](https://www.python.org/downloads/))
2. **Docker Desktop** ([Download](https://www.docker.com/products/docker-desktop/)) - We use this for PostgreSQL
3. **Git** ([Download](https://git-scm.com/downloads))
4. **A code editor** (VS Code, PyCharm, etc.)

### 🎬 Fast Track Setup

```bash
# 1. Clone the repository
git clone https://github.com/LearnToCodeNJ/datacollector.git
cd datacollector

# 2. Create Python virtual environment
python -m venv venv

# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start the service (includes PostgreSQL)
python scripts/start_service.py

# ✅ That's it! The API is now running at http://localhost:8000
```

Visit **http://localhost:8000/docs** to see the interactive API documentation.

---

## 📚 Detailed Setup Guide

### Step 1: Get API Keys (Optional but Recommended)

Most data sources work better with API keys. They're free and take 2 minutes to get:

| Source | Required? | Get Key Here | Notes |
|--------|-----------|--------------|-------|
| Census | Recommended | [Get Key](https://api.census.gov/data/key_signup.html) | Works without key but rate-limited |
| FRED | Required | [Get Key](https://fred.stlouisfed.org/docs/api/api_key.html) | Required for economic data |
| EIA | Required | [Get Key](https://www.eia.gov/opendata/register.php) | Required for energy data |
| NOAA | Optional | [Get Token](https://www.ncdc.noaa.gov/cdo-web/token) | Passed per-request |
| SEC | Not Needed | N/A | Public data, no key needed |

### Step 2: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Database (automatically managed by Docker)
DATABASE_URL=postgresql://nexdata:nexdata@localhost:5432/nexdata

# API Keys (add the ones you obtained above)
CENSUS_SURVEY_API_KEY=your_census_key_here
FRED_API_KEY=your_fred_key_here
EIA_API_KEY=your_eia_key_here

# Optional: Rate limiting configuration
MAX_CONCURRENCY=5
MAX_REQUESTS_PER_SECOND=10
```

### Step 3: Start the Service

**Option A: Automatic (Recommended)**

The startup script handles everything - database, health checks, auto-restart:

```bash
python scripts/start_service.py
```

Features:
- ✅ Starts PostgreSQL automatically
- ✅ Waits for database to be ready
- ✅ Auto-restarts on failures
- ✅ Graceful shutdown (Ctrl+C)
- ✅ Comprehensive logging

**Option B: Manual Control**

If you prefer to control each component:

```bash
# Terminal 1 - Start PostgreSQL
docker-compose up -d db

# Terminal 2 - Start API server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Step 4: Verify It's Working

Open your browser to **http://localhost:8000/docs** - you should see the interactive API documentation (Swagger UI).

Try a test request:

```bash
# Get Census metadata for available datasets
curl http://localhost:8000/api/v1/census/metadata/datasets
```

---

## 🎓 Your First Data Ingestion

Let's ingest some Census data about population by state:

### Using the API

```bash
# Start an ingestion job
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source": "census",
    "config": {
      "survey": "acs5",
      "year": 2023,
      "table_id": "B01001",
      "geo_level": "state"
    }
  }'

# You'll get back a job_id like: {"job_id": "123e4567-e89b-12d3-a456-426614174000"}

# Check the job status
curl http://localhost:8000/api/v1/jobs/123e4567-e89b-12d3-a456-426614174000

# When status is "success", query the data
# Data is stored in PostgreSQL table: acs5_2023_b01001
```

### Using Python

```python
import requests

# Start ingestion
response = requests.post('http://localhost:8000/api/v1/jobs', json={
    'source': 'census',
    'config': {
        'survey': 'acs5',
        'year': 2023,
        'table_id': 'B01001',
        'geo_level': 'state'
    }
})
job_id = response.json()['job_id']
print(f"Job started: {job_id}")

# Check status
status = requests.get(f'http://localhost:8000/api/v1/jobs/{job_id}')
print(status.json())
```

### Using the Demo Scripts

We provide ready-to-run examples:

```bash
# Quick demo (30 seconds) - Tests all sources
python scripts/quick_demo.py

# Full demo (5 minutes) - Populates real data
python scripts/populate_demo_data.py

# Specific examples
python scripts/example_usage.py          # Census examples
python scripts/direct_fred_ingest.py     # FRED examples
python scripts/example_noaa_usage.py     # NOAA examples
```

---

## 🏗️ Current Data Sources

| Source | Status | What You Get | API Endpoint |
|--------|--------|--------------|--------------|
| **Census** | ✅ Ready | Demographics, housing, economics by geography | `/api/v1/census/` |
| **FRED** | ✅ Ready | 800K+ economic time series (GDP, rates, etc.) | `/api/v1/fred/` |
| **EIA** | ✅ Ready | Energy production, prices, consumption | `/api/v1/eia/` |
| **SEC** | ✅ Ready | Company financials, Form ADV filings | `/api/v1/sec/` |
| **NOAA** | ✅ Ready | Weather observations, climate data | `/api/v1/noaa/` |
| **Real Estate** | ✅ Ready | Zillow home values, rental indices | `/api/v1/realestate/` |
| **LP Strategies** | ✅ Ready | Public pension fund investment data | Built-in loader |

---

## 📖 Tech Stack

- **FastAPI** - Modern async Python web framework
- **PostgreSQL** - Robust relational database
- **SQLAlchemy** - ORM for database operations
- **httpx** - Async HTTP client with retry logic
- **Docker** - PostgreSQL containerization
- **Pytest** - Testing framework

## 💡 Common Use Cases & Examples

### Example 1: Get Unemployment Rate by State

```python
import requests

# 1. Start FRED ingestion for unemployment rate
response = requests.post('http://localhost:8000/api/v1/jobs', json={
    'source': 'fred',
    'config': {
        'series_id': 'UNRATE',
        'start_date': '2020-01-01',
        'end_date': '2024-12-31'
    }
})
job_id = response.json()['job_id']

# 2. Wait for completion, then query from PostgreSQL
# SELECT * FROM fred_series WHERE series_id = 'UNRATE';
```

### Example 2: Get Population Demographics by County

```bash
curl -X POST http://localhost:8000/api/v1/census/batch-ingest \
  -H "Content-Type: application/json" \
  -d '{
    "survey": "acs5",
    "year": 2023,
    "tables": ["B01001", "B19013"],
    "geo_level": "county",
    "state": "34"
  }'
```

### Example 3: Track Oil Prices

```python
# Ingest crude oil prices from EIA
requests.post('http://localhost:8000/api/v1/jobs', json={
    'source': 'eia',
    'config': {
        'series_id': 'PET.RWTC.W',  # WTI Crude Oil Spot Price
        'frequency': 'weekly'
    }
})
```

---

## 🧪 Testing

```bash
# Run all unit tests (no API keys needed)
pytest tests/

# Run with coverage report
pytest --cov=app --cov-report=html tests/

# Run integration tests (requires API keys)
RUN_INTEGRATION_TESTS=true pytest tests/integration/

# Run specific test file
pytest tests/test_census_client_url_building.py -v
```

---

## 🔍 Troubleshooting

### "Connection refused" or "Database not ready"

**Problem**: PostgreSQL isn't running or isn't ready yet.

**Solution**:
```bash
# Check if PostgreSQL is running
docker ps

# If not running, start it
docker-compose up -d db

# Wait for it to be ready (usually 10-15 seconds)
# The start_service.py script does this automatically
```

### "API key invalid" or "Unauthorized"

**Problem**: Missing or incorrect API key.

**Solution**:
1. Check your `.env` file has the correct keys
2. Verify keys are valid at the provider's website
3. Restart the service after updating `.env`

### "Module not found" errors

**Problem**: Virtual environment not activated or dependencies not installed.

**Solution**:
```bash
# Activate virtual environment
# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Service won't start

**Problem**: Port 8000 or 5432 already in use.

**Solution**:
```bash
# Check what's using port 8000
# Windows:
netstat -ano | findstr :8000

# Mac/Linux:
lsof -i :8000

# Kill the process or change the port in start_service.py
```

### "No data returned" from API

**Problem**: Job completed but data query returns empty.

**Solution**:
1. Check job status: `GET /api/v1/jobs/{job_id}`
2. Look for errors in the job details
3. Verify your query parameters match the ingested data
4. Check PostgreSQL directly:
   ```bash
   docker exec -it nexdata-db psql -U nexdata -d nexdata
   \dt  # List all tables
   ```

---

## 📂 Project Structure

```
datacollector/
├── app/                      # Main application code
│   ├── main.py              # FastAPI application entry point
│   ├── core/                # Core functionality (source-agnostic)
│   │   ├── config.py        # Configuration management
│   │   ├── database.py      # Database connection & sessions
│   │   ├── models.py        # SQLAlchemy models (jobs, registry)
│   │   └── schemas.py       # Pydantic schemas for validation
│   ├── sources/             # Data source adapters (plugin pattern)
│   │   ├── census/          # Census Bureau adapter
│   │   ├── fred/            # Federal Reserve (FRED) adapter
│   │   ├── eia/             # Energy Information Admin adapter
│   │   ├── sec/             # SEC filings adapter
│   │   ├── noaa/            # NOAA weather adapter
│   │   ├── realestate/      # Real estate data adapter
│   │   └── public_lp_strategies/  # LP strategies adapter
│   └── api/                 # API routes
│       └── v1/              # Version 1 endpoints
│           ├── jobs.py      # Job management endpoints
│           ├── census_batch.py
│           ├── fred.py
│           └── ...
├── tests/                   # Test suite
│   ├── unit/               # Unit tests
│   └── integration/        # Integration tests
├── scripts/                # Utility scripts & examples
│   ├── start_service.py    # Robust startup script
│   ├── quick_demo.py       # Quick demonstration
│   ├── populate_demo_data.py
│   └── example_*.py        # Usage examples
├── docs/                   # Documentation
│   ├── QUICKSTART.md       # 5-minute quick start
│   ├── DEMO.md            # Demo guide
│   ├── *_QUICK_START.md   # Per-source guides
│   └── *.md               # Additional documentation
├── data/                   # Data files (gitignored except README)
├── docker-compose.yml      # Docker services configuration
├── Dockerfile             # Application container definition
├── requirements.txt        # Python dependencies
├── .env                   # Environment variables (create this)
├── RULES.md               # Development guidelines
└── README.md              # This file
```

## 🏛️ Architecture

The service uses a **plugin-based architecture** for easy extensibility:

```
┌─────────────────────────────────────────────┐
│          FastAPI REST API Layer             │
│         (app/api/v1/*.py)                   │
├─────────────────────────────────────────────┤
│          Core Service Layer                 │
│   - Job Management (ingestion_jobs)         │
│   - Dataset Registry (dataset_registry)     │
│   - Database Models & Schemas               │
├─────────────────────────────────────────────┤
│          Source Adapters (Plugins)          │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐      │
│  │Census│ │ FRED │ │ EIA  │ │ SEC  │ ...  │
│  └──────┘ └──────┘ └──────┘ └──────┘      │
├─────────────────────────────────────────────┤
│          PostgreSQL Database                │
│   - Job tracking                            │
│   - Dataset metadata                        │
│   - Source-specific data tables             │
└─────────────────────────────────────────────┘
```

### Key Design Principles

1. **Source Isolation**: Each data source lives in its own module with zero dependencies on other sources
2. **Consistent Interface**: All sources implement a common adapter pattern
3. **Job Tracking**: Every ingestion is tracked with status, errors, and row counts
4. **Rate Limiting**: Built-in respect for API rate limits with configurable concurrency
5. **Type Safety**: Strongly typed database columns (no JSON blobs for data storage)
6. **Idempotency**: Safe to re-run ingestions without duplicating data

## 🛠️ API Reference

### Core Endpoints

#### Start an Ingestion Job
```bash
POST /api/v1/jobs
Content-Type: application/json

{
  "source": "census",  # or "fred", "eia", "sec", etc.
  "config": {
    # Source-specific configuration
  }
}

# Returns: {"job_id": "uuid", "status": "pending"}
```

#### Check Job Status
```bash
GET /api/v1/jobs/{job_id}

# Returns:
{
  "job_id": "uuid",
  "source": "census",
  "status": "success",  # or "pending", "running", "failed"
  "rows_affected": 50,
  "started_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:30:45Z",
  "error_message": null,
  "config": {...}
}
```

#### List All Jobs
```bash
GET /api/v1/jobs?source=census&status=success&limit=50
```

### Source-Specific Endpoints

Each data source has specialized endpoints. Visit **http://localhost:8000/docs** for interactive documentation with live examples.

#### Census API Examples
```bash
# Get available datasets
GET /api/v1/census/metadata/datasets?survey=acs5&year=2023

# Get table variables
GET /api/v1/census/metadata/variables?survey=acs5&year=2023&table_id=B01001

# Batch ingest multiple tables
POST /api/v1/census/batch-ingest
{
  "survey": "acs5",
  "year": 2023,
  "tables": ["B01001", "B19013", "B25001"],
  "geo_level": "county",
  "state": "34"
}
```

#### FRED API Examples
```bash
# Search for series
GET /api/v1/fred/search?query=unemployment

# Get series metadata
GET /api/v1/fred/series/UNRATE

# Ingest time series data
POST /api/v1/fred/ingest
{
  "series_id": "UNRATE",
  "start_date": "2020-01-01",
  "end_date": "2024-12-31"
}
```

#### EIA API Examples
```bash
# Get crude oil prices
POST /api/v1/eia/ingest
{
  "series_id": "PET.RWTC.W",
  "frequency": "weekly"
}
```

---

## 📚 Documentation

### Quick Start Guides
- **[QUICKSTART.md](QUICKSTART.md)** - Get running in 5 minutes
- **[DEMO.md](DEMO.md)** - Demonstration scripts and presentation guide

### Source-Specific Documentation

| Source | Quick Start | Advanced Guide |
|--------|-------------|----------------|
| Census | [QUICKSTART.md](QUICKSTART.md) | [CENSUS_METADATA_API_REFERENCE.md](docs/CENSUS_METADATA_API_REFERENCE.md)<br>[GEOGRAPHIC_API_GUIDE.md](docs/GEOGRAPHIC_API_GUIDE.md)<br>[MULTI_YEAR_DATA_GUIDE.md](docs/MULTI_YEAR_DATA_GUIDE.md) |
| FRED | [FRED_QUICK_START.md](docs/FRED_QUICK_START.md) | [FRED_IMPLEMENTATION_SUMMARY.md](docs/FRED_IMPLEMENTATION_SUMMARY.md) |
| EIA | [EIA_QUICK_START.md](docs/EIA_QUICK_START.md) | Interactive docs at `/docs` |
| NOAA | [NOAA_QUICK_START.md](docs/NOAA_QUICK_START.md) | [NOAA_README.md](docs/NOAA_README.md)<br>[NOAA_DEPLOYMENT_CHECKLIST.md](docs/NOAA_DEPLOYMENT_CHECKLIST.md) |
| SEC | [SEC_QUICK_START.md](docs/SEC_QUICK_START.md) | [SEC_DATA_INGESTION_GUIDE.md](docs/SEC_DATA_INGESTION_GUIDE.md)<br>[SEC_COMPANIES_TRACKING.md](docs/SEC_COMPANIES_TRACKING.md) |
| Real Estate | [REALESTATE_QUICK_START.md](docs/REALESTATE_QUICK_START.md) | [REALESTATE_IMPLEMENTATION_SUMMARY.md](docs/REALESTATE_IMPLEMENTATION_SUMMARY.md) |
| LP Strategies | [PUBLIC_LP_STRATEGIES_QUICK_START.md](docs/PUBLIC_LP_STRATEGIES_QUICK_START.md) | [LP_STRATEGIES_README.md](docs/LP_STRATEGIES_README.md) |

### Development Documentation
- **[RULES.md](RULES.md)** - Development guidelines and adding new sources
- **[EXTERNAL_DATA_SOURCES.md](docs/EXTERNAL_DATA_SOURCES.md)** - Planned future data sources
- **[STATUS_REPORT.md](docs/STATUS_REPORT.md)** - Current implementation status

### General Reference
- **[QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md)** - Common commands and patterns
- **[USAGE.md](docs/USAGE.md)** - Detailed usage examples

---

## 🎭 Running the Demo

Want to see it in action?

```bash
# Quick demo (30 seconds) - Tests connectivity to all sources
python scripts/quick_demo.py

# Full demo (5 minutes) - Populates database with real data
python scripts/populate_demo_data.py

# Source-specific examples
python scripts/example_usage.py          # Census data
python scripts/direct_fred_ingest.py     # Economic indicators
python scripts/example_noaa_usage.py     # Weather data
```

---

## 🚀 Production Deployment

### Using Docker Compose (Recommended)

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Environment Variables for Production

```bash
# .env file for production
DATABASE_URL=postgresql://user:password@db-host:5432/nexdata
ENVIRONMENT=production

# API Keys
CENSUS_SURVEY_API_KEY=your_key
FRED_API_KEY=your_key
EIA_API_KEY=your_key

# Rate Limiting (adjust based on your API tier)
MAX_CONCURRENCY=10
MAX_REQUESTS_PER_SECOND=20

# Security
SECRET_KEY=your-secret-key-here
ALLOWED_HOSTS=your-domain.com,api.your-domain.com
```

### Health Check

```bash
# Check if service is running
curl http://localhost:8000/health

# Returns: {"status": "healthy", "database": "connected"}
```

---

## 🤝 Contributing

We welcome contributions! Here's how to add a new data source:

1. **Read [RULES.md](RULES.md)** - Understand the architecture and guidelines
2. **Create source module**: `app/sources/your_source/`
3. **Implement adapter**: Follow the plugin pattern (see existing sources)
4. **Add tests**: Unit and integration tests
5. **Document**: Create `docs/YOUR_SOURCE_QUICK_START.md`
6. **Submit PR**: With clear description and examples

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt
pip install pytest pytest-cov black flake8

# Run tests
pytest

# Format code
black app/ tests/

# Lint
flake8 app/ tests/
```

---

## 📊 Database Schema

### Core Tables

- **`ingestion_jobs`** - Tracks every ingestion run (status, timing, errors)
- **`dataset_registry`** - Metadata about available datasets

### Source-Specific Tables

Tables are created dynamically based on ingested data:
- `acs5_2023_b01001` - Census ACS 5-year data for table B01001, year 2023
- `fred_series` - FRED time series data
- `eia_petroleum_prices` - EIA petroleum price data
- `sec_company_facts` - SEC company financial facts
- And more...

Connect directly to PostgreSQL to query:

```bash
# Connect to database
docker exec -it nexdata-db psql -U nexdata -d nexdata

# List all tables
\dt

# Query data
SELECT * FROM acs5_2023_b01001 LIMIT 10;
```

---

## ❓ FAQ

**Q: Do I need API keys for all sources?**  
A: No, only for the sources you want to use. Census and SEC work without keys (with rate limits).

**Q: Can I use this in production?**  
A: Yes! The service includes rate limiting, error handling, and job tracking suitable for production.

**Q: How do I add a new data source?**  
A: See [RULES.md](RULES.md) for detailed guidelines. Each source is a self-contained plugin.

**Q: What if a job fails?**  
A: Check the job status at `/api/v1/jobs/{job_id}` - it will show the error message. Jobs are idempotent, so you can safely retry.

**Q: Can I ingest historical data?**  
A: Yes! All sources support historical data ingestion. See source-specific docs for date range limits.

**Q: How much data can I store?**  
A: PostgreSQL can handle massive datasets. The Census alone has thousands of tables with millions of rows.

---

## 📄 License

MIT License - See LICENSE file for details

---

## 🙏 Acknowledgments

Data provided by:
- U.S. Census Bureau
- Federal Reserve Bank of St. Louis (FRED)
- U.S. Energy Information Administration
- U.S. Securities and Exchange Commission
- National Oceanic and Atmospheric Administration
- Zillow Research

---

**Built with ❤️ for the data community**

For questions, issues, or feature requests, please open an issue on GitHub.


