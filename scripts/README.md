# Scripts Directory

This directory contains utility scripts and example usage code for the External Data Ingestion Service.

## 🚀 Service Management & Demo

### `quick_demo.py` (Quick Data Population)
Ultra-fast demo script that ingests a few key datasets in ~30 seconds.

**Perfect for:**
- Quick validation that everything works
- Demos and presentations
- Testing after setup

**Usage:**
```bash
# Just run it!
python scripts/quick_demo.py

# What it does:
# 1. Checks service health
# 2. Ingests 3 sample datasets (GDP, Unemployment, Census Population)
# 3. Shows results
# Takes ~30 seconds total
```

### `populate_demo_data.py` (Full Demo Population)
Comprehensive script that ingests sample data from all sources.

**Features:**
- Ingests from Census, FRED, EIA, SEC, and Real Estate sources
- Curated datasets showing key capabilities
- Progress tracking with visual feedback
- Handles API rate limits properly
- Safe and bounded concurrency

**Usage:**
```bash
# Ingest from all sources
python scripts/populate_demo_data.py

# Quick mode (fewer datasets)
python scripts/populate_demo_data.py --quick

# Specific sources only
python scripts/populate_demo_data.py --sources census,fred

# Custom API URL
python scripts/populate_demo_data.py --api-url http://localhost:9000
```

**Options:**
- `--sources` - Comma-separated list: census,fred,eia,sec,realestate
- `--quick` - Reduced datasets for faster completion
- `--api-url` - Custom API base URL (default: http://localhost:8000)

**What it ingests:**
- **Census:** Population by state, median income by county
- **FRED:** GDP, unemployment, CPI, interest rates, money supply
- **EIA:** Gas prices, natural gas, electricity generation
- **SEC:** Financial data for Apple, Amazon, Google, Meta, Microsoft
- **Real Estate:** FHFA house price index, HUD building permits

### `start_service.py` (Cross-platform)
Robust startup script with health checks, timeouts, and auto-restart capabilities.

**Features:**
- Starts PostgreSQL via Docker Compose
- Waits for database readiness with timeout
- Starts FastAPI application with health monitoring
- Auto-restarts on failures (configurable)
- Graceful shutdown handling
- Comprehensive logging

**Usage:**
```bash
# Basic usage
python scripts/start_service.py

# The script will:
# 1. Check prerequisites (Docker, docker-compose, files)
# 2. Start PostgreSQL database
# 3. Wait for database to be ready (60s timeout)
# 4. Start FastAPI application
# 5. Monitor health and auto-restart if needed (max 3 attempts)
```

**Configuration (edit the script):**
- `DB_STARTUP_TIMEOUT` - Database startup timeout (default: 60s)
- `APP_STARTUP_TIMEOUT` - Application startup timeout (default: 30s)
- `MAX_RESTART_ATTEMPTS` - Maximum restart attempts (default: 3)
- `APP_RESTART_DELAY` - Delay between restarts (default: 5s)

### `start_service.sh` (Linux/Mac Bash)
Bash version with same features, optimized for Linux and macOS.

**Usage:**
```bash
# Make executable (first time only)
chmod +x scripts/start_service.sh

# Basic usage
bash scripts/start_service.sh

# Or directly
./scripts/start_service.sh

# With custom parameters
./scripts/start_service.sh --max-restarts 5 --db-timeout 120

# Stop database on exit
./scripts/start_service.sh --stop-db

# Show help
./scripts/start_service.sh --help
```

**Options:**
- `--max-restarts N` - Maximum restart attempts (default: 3)
- `--db-timeout N` - Database startup timeout in seconds (default: 60)
- `--app-timeout N` - Application startup timeout in seconds (default: 30)
- `--restart-delay N` - Delay between restarts in seconds (default: 5)
- `--stop-db` - Stop database when script exits (default: false)
- `--help` - Show help message

### `start_service.ps1` (Windows PowerShell)
PowerShell version with same features as Python script, optimized for Windows.

**Usage:**
```powershell
# Basic usage
.\scripts\start_service.ps1

# With custom parameters
.\scripts\start_service.ps1 -MaxRestartAttempts 5 -DbStartupTimeout 120

# Stop database on exit
.\scripts\start_service.ps1 -StopDbOnExit
```

**Parameters:**
- `-MaxRestartAttempts` - Maximum restart attempts (default: 3)
- `-DbStartupTimeout` - Database startup timeout in seconds (default: 60)
- `-AppStartupTimeout` - Application startup timeout in seconds (default: 30)
- `-RestartDelay` - Delay between restarts in seconds (default: 5)
- `-StopDbOnExit` - Stop database when script exits (default: false)

## 📊 Usage Examples

- `example_usage.py` - Basic usage examples for the service
- `example_noaa_usage.py` - NOAA-specific examples

## 🔄 Ingestion Scripts

Scripts for triggering ingestion jobs:

- `ingest_fred_data.py` - FRED data ingestion
- `ingest_fred_now.py` - Immediate FRED ingestion
- `direct_fred_ingestion.py` - Direct FRED ingestion bypass
- `fred_sync_ingest.py` - Synchronous FRED ingestion
- `ingest_sec_companies.py` - SEC company data ingestion
- `ingest_100_companies.py` - Ingest specific 100 companies
- `ingest_200_companies.py` - Ingest specific 200 companies

## 📥 Data Fetching Scripts

- `fetch_100_companies_data.py` - Fetch data for 100 companies
- `fetch_all_229_companies.py` - Fetch all 229 companies
- `api_ingest_200.py` - API-based ingestion for 200 companies
- `sec_companies_200.py` - SEC companies list processing

## 🏛️ Family Office / Form ADV Scripts

**Location:** `/scripts/family_office/`

Specialized scripts for SEC Form ADV ingestion (family offices and investment advisers).

**Key Scripts:**
- `ingest_family_offices.ps1` - PowerShell batch ingestion
- `ingest_family_offices.sh` - Bash batch ingestion
- `ingest_all_family_offices.py` - Python comprehensive ingestion
- `monitor_ingestion.ps1` - Monitor ingestion progress

**Quick Start:**
```powershell
cd scripts/family_office
powershell -ExecutionPolicy Bypass -File .\ingest_family_offices.ps1
```

**Documentation:** See `/scripts/family_office/README.md` for complete details

## 🧪 Sample Data & Testing

- `load_lp_sample_data.py` - Load LP strategies sample data
- `test_formadv_ingestion.py` - Test Form ADV ingestion
- `test_fred_single.py` - Test single FRED series

## 🔍 Monitoring & Utilities

- `check_jobs.py` - Check job status
- `check_progress.py` - Monitor ingestion progress
- `monitor_fetch.py` - Monitor data fetching operations
- `trigger_fred_ingestion.ps1` - PowerShell script to trigger FRED ingestion

## General Usage Notes

These scripts are meant to be run from the project root directory:

```bash
# Activate virtual environment first
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Run a script
python scripts/example_usage.py
```

**Note:** Most scripts require environment variables to be set (API keys, DATABASE_URL, etc.). See the main README.md for configuration details.

## Logs

Service management scripts create log files:
- `service_startup.log` - Startup script logs
- `app_stdout.log` - Application stdout (PowerShell script)
- `app_stderr.log` - Application stderr (PowerShell script)
