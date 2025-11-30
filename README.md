# External Data Ingestion Service

A multi-source data ingestion service that ingests data from public data providers into PostgreSQL via a FastAPI HTTP API.

> **🚀 New to this project?**
> - [QUICKSTART.md](QUICKSTART.md) - 5-minute setup guide
> - [DEMO.md](DEMO.md) - Demonstration guide with sample data

## Current Data Sources

- **Census** ✅ - U.S. Census Bureau (ACS 5-year, Decennial, PUMS, TIGER/Line)
- **FRED** ✅ - Federal Reserve Economic Data (interest rates, monetary aggregates, industrial production, economic indicators)
- **EIA** ✅ - Energy Information Administration (petroleum, natural gas, electricity, retail gas prices, STEO projections)
- **Public LP Strategies** ✅ - Investment strategies from public pension funds and endowments

## Tech Stack

- **FastAPI** - Modern Python web framework
- **PostgreSQL** - Relational database
- **SQLAlchemy** - ORM and database toolkit
- **httpx** - Async HTTP client
- **Docker** - Containerization (optional)

## Quick Start

> **TL;DR:** Run `python scripts/start_service.py` from the project root to start everything!

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 14+
- **API Keys** (depending on which sources you use):
  - Census API key (required for Census): https://api.census.gov/data/key_signup.html
  - FRED API key (optional but recommended): https://fred.stlouisfed.org/docs/api/api_key.html
  - EIA API key (required for EIA): https://www.eia.gov/opendata/register.php

### 2. Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Environment variables are managed via Docker
# See docker-compose.yml for configuration
```

### 3. Configuration

Edit `.env` file:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/nexdata

# API Keys (only needed for sources you plan to use)
CENSUS_SURVEY_API_KEY=your_census_api_key
BLS_API_KEY=your_bls_api_key
FRED_API_KEY=your_fred_api_key
EIA_API_KEY=your_eia_api_key
# NOAA token is passed per-request (see NOAA_QUICK_START.md)
```

### 4. Run the Service

**Option A: Using the Robust Startup Script (Recommended)**

The startup scripts automatically handle database startup, health checks, and auto-restart on failures:

```bash
# Linux/Mac
bash scripts/start_service.sh

# Windows (PowerShell)
.\scripts\start_service.ps1

# Cross-platform (Python)
python scripts/start_service.py
```

Features:
- ✅ Starts PostgreSQL via Docker Compose
- ✅ Waits for database readiness (60s timeout)
- ✅ Starts FastAPI with health monitoring
- ✅ Auto-restarts on failures (max 3 attempts)
- ✅ Graceful shutdown on Ctrl+C
- ✅ Comprehensive logging

**Option B: Manual Start**

```bash
# Start database
docker-compose up -d db

# Run API server
uvicorn app.main:app --reload

# API will be available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

## Testing

```bash
# Run unit tests (no API keys or network required)
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run integration tests (requires API keys and network)
RUN_INTEGRATION_TESTS=true pytest tests/integration/
```

## Project Structure

```
Nexdata/
├── app/                    # Main application
│   ├── core/              # Core logic (source-agnostic)
│   ├── sources/           # Data source adapters
│   └── api/               # API routes
├── tests/                 # Unit and integration tests
├── docs/                  # Documentation (guides, quick starts, summaries)
├── scripts/               # Utility scripts and examples
├── data/                  # Data files and outputs
├── docker-compose.yml     # Docker configuration
├── Dockerfile             # Docker build file
├── requirements.txt       # Python dependencies
├── README.md             # This file
└── RULES.md              # Development guidelines
```

## Architecture

The service follows a **plugin-based architecture**:

- **Core** (`app/core/`) - Source-agnostic logic, models, database
- **Sources** (`app/sources/`) - Source-specific adapters (census, bls, noaa, fred, eia, sec, realestate, etc.)
- **API** (`app/api/`) - HTTP endpoints

Each data source is isolated in its own module with a clean adapter interface.

## Documentation

### Source-Specific Guides
- **Census:** `docs/CENSUS_METADATA_API_REFERENCE.md`, `docs/GEOGRAPHIC_API_GUIDE.md`, `docs/MULTI_YEAR_DATA_GUIDE.md`
- **FRED:** `docs/FRED_QUICK_START.md`, `docs/FRED_IMPLEMENTATION_SUMMARY.md`
- **EIA:** `docs/EIA_QUICK_START.md` + API endpoints at `/docs` (interactive documentation)
- **NOAA:** `docs/NOAA_QUICK_START.md`, `docs/NOAA_README.md`, `docs/NOAA_DEPLOYMENT_CHECKLIST.md`
- **Real Estate:** `docs/REALESTATE_QUICK_START.md`, `docs/REALESTATE_IMPLEMENTATION_SUMMARY.md`
- **SEC:** `docs/SEC_QUICK_START.md`, `docs/SEC_DATA_INGESTION_GUIDE.md`, `docs/SEC_COMPANIES_TRACKING.md`
- **LP Strategies:** `docs/PUBLIC_LP_STRATEGIES_QUICK_START.md`, `docs/LP_STRATEGIES_README.md`
- **General:** `docs/QUICK_REFERENCE.md`, `docs/USAGE.md`

### Development
- **Rules:** `RULES.md` - Guidelines for adding new sources
- **External Sources:** `docs/EXTERNAL_DATA_SOURCES.md` - Planned data sources
- **Status:** `docs/STATUS_REPORT.md` - Current implementation status

### Example Scripts & Utilities
- See `scripts/` directory for example usage scripts and utility tools

## Quick Demo

Want to see it work immediately?

```bash
# Ultra-fast demo (~30 seconds)
python scripts/quick_demo.py

# Full demo with all sources (~5 minutes)
python scripts/populate_demo_data.py
```

These scripts will populate your database with sample data from all sources and show you what the system can do!

## API Usage

### Start an Ingestion Job

```bash
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

### Check Job Status

```bash
GET /api/v1/jobs/{job_id}
```

## Demo & Presentations

See [DEMO.md](DEMO.md) for a complete guide to demonstrating the system, including:
- Quick 30-second demo
- Full 5-minute showcase
- Live coding examples
- Presentation scripts

## Adding New Data Sources

See `RULES.md` for detailed guidelines on adding new sources while maintaining safety, compliance, and architectural consistency.

## License

MIT


