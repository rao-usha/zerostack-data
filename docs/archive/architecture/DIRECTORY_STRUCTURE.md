# Directory Structure

This document describes the organization of the Nexdata External Data Ingestion Service.

## 📁 Root Directory

```
Nexdata/
├── README.md                   # Main project documentation
├── RULES.md                    # Project rules and guidelines
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Container definition
├── requirements.txt            # Python dependencies
├── pytest.ini                  # Test configuration
├── DIRECTORY_STRUCTURE.md      # This file
│
├── app/                        # Main application code
│   ├── main.py                 # FastAPI application entry point
│   ├── core/                   # Core functionality
│   ├── api/                    # API routes
│   └── sources/                # Data source adapters
│
├── docs/                       # Documentation
├── scripts/                    # Utility scripts
├── tests/                      # Test suite
├── data/                       # Data storage and samples
└── venv/                       # Python virtual environment (local only)
```

## 📚 `/docs/` - Documentation

All project documentation lives here.

### API Documentation
- `FORM_ADV_API_REFERENCE.md` - Form ADV API complete reference
- `FORM_ADV_GUIDE.md` - Form ADV user guide
- `FORM_ADV_QUICKSTART.md` - Form ADV quick start
- `SWAGGER_UI_GUIDE.md` - Interactive API documentation guide

### Implementation Summaries
- `SEC_IMPLEMENTATION_SUMMARY.md` - SEC EDGAR implementation
- `FRED_IMPLEMENTATION_SUMMARY.md` - FRED implementation
- `NOAA_IMPLEMENTATION_SUMMARY.md` - NOAA weather data
- `REALESTATE_IMPLEMENTATION_SUMMARY.md` - Real estate data
- `LP_STRATEGIES_IMPLEMENTATION_SUMMARY.md` - LP strategies

### Quick Start Guides
- `GETTING_STARTED.md` - Getting started guide
- `QUICKSTART.md` - Quick start guide
- `SEC_QUICK_START.md` - SEC quick start
- `FRED_QUICK_START.md` - FRED quick start
- `NOAA_QUICK_START.md` - NOAA quick start
- `REALESTATE_QUICK_START.md` - Real estate quick start

### Reference Documentation
- `EXTERNAL_DATA_SOURCES.md` - Complete checklist of data sources
- `COMPLETE_SYSTEM_GUIDE.md` - Comprehensive system guide
- `USAGE.md` - Usage instructions
- `GEOGRAPHIC_API_GUIDE.md` - Geographic data guide
- `MULTI_YEAR_DATA_GUIDE.md` - Multi-year data guide

### Status & Reports
- `FAMILY_OFFICE_INGESTION_REPORT.md` - Form ADV ingestion results
- `STATUS_REPORT.md` - Project status
- `CHANGELOG.md` - Project changelog
- `FIXES_AND_IMPROVEMENTS.md` - Recent fixes

### Additional Guides
- `SEC_DATA_INGESTION_GUIDE.md` - SEC data ingestion
- `PUBLIC_LP_STRATEGIES_QUICK_START.md` - LP strategies guide
- `CENSUS_METADATA_API_REFERENCE.md` - Census metadata
- `GEOJSON_QUICK_START.md` - GeoJSON quick start
- `PROJECT_ORGANIZATION.md` - Project organization
- `DEMO.md` - Demo instructions
- `ORGANIZATION_COMPLETE.md` - Organization details

## 🔧 `/scripts/` - Utility Scripts

### Service Management
- `start_service.py` - Cross-platform service starter
- `start_service.sh` - Bash service starter
- `start_service.ps1` - PowerShell service starter
- `quick_demo.py` - Quick demo (~30 seconds)
- `populate_demo_data.py` - Comprehensive demo data

### Family Office Scripts
**Location:** `/scripts/family_office/`
- `ingest_family_offices.ps1` - PowerShell batch ingestion
- `ingest_family_offices.sh` - Bash batch ingestion  
- `ingest_all_family_offices.py` - Python comprehensive ingestion
- `monitor_ingestion.ps1` - Monitor progress
- `README.md` - Complete documentation

### Data Ingestion
- `ingest_fred_data.py` - FRED data ingestion
- `ingest_sec_companies.py` - SEC company ingestion
- `ingest_100_companies.py` - 100 companies
- `ingest_200_companies.py` - 200 companies

### Monitoring & Utilities
- `check_jobs.py` - Check job status
- `check_progress.py` - Monitor progress
- `monitor_fetch.py` - Monitor fetching

### Testing
- `test_formadv_ingestion.py` - Test Form ADV
- `test_fred_single.py` - Test single FRED series
- `test_single_datapoint.py` - Test single datapoint

### See Also
- `/scripts/README.md` - Complete scripts documentation

## 🏗️ `/app/` - Application Code

### Structure

```
app/
├── main.py                     # FastAPI application
├── core/                       # Core functionality
│   ├── config.py               # Configuration management
│   ├── database.py             # Database connections
│   ├── models.py               # SQLAlchemy models
│   └── schemas.py              # Pydantic schemas
├── api/                        # API routes
│   └── v1/                     # API version 1
│       ├── jobs.py             # Job tracking
│       ├── sec.py              # SEC endpoints
│       ├── fred.py             # FRED endpoints
│       ├── census_*.py         # Census endpoints
│       └── ...                 # Other endpoints
└── sources/                    # Data source adapters
    ├── census/                 # Census adapter
    ├── fred/                   # FRED adapter
    ├── sec/                    # SEC adapter
    │   ├── formadv_*.py        # Form ADV modules
    │   └── ...                 # Other SEC modules
    ├── eia/                    # EIA adapter
    ├── noaa/                   # NOAA adapter
    ├── realestate/             # Real estate adapter
    └── public_lp_strategies/   # LP strategies adapter
```

### Key Principles
- **Source-agnostic core** - No hard-coded source logic in core/
- **Plugin pattern** - Each source in its own module
- **Separation of concerns** - API, logic, and data access separated

## 🧪 `/tests/` - Test Suite

```
tests/
├── __init__.py
├── test_census.py
├── test_fred.py
├── test_sec.py
├── test_jobs.py
└── ...
```

**Run tests:**
```bash
pytest tests/
```

## 💾 `/data/` - Data Storage

```
data/
├── README.md                   # Data directory documentation
├── census_variable_metadata*.csv  # Census metadata cache
├── sec_data_output/            # SEC data output
└── sample_*.json               # Sample data files
```

## 🐳 Docker Files

### `docker-compose.yml`
Defines services:
- `postgres` - PostgreSQL database
- `api` - FastAPI application

### `Dockerfile`
Python application container:
- Base: `python:3.11-slim`
- Installs dependencies from `requirements.txt`
- Runs uvicorn server

## 📦 Python Configuration

### `requirements.txt`
All Python dependencies:
- fastapi
- sqlalchemy
- psycopg2-binary
- httpx
- pandas
- And more...

### `pytest.ini`
Test configuration:
- Test discovery patterns
- Coverage settings
- Markers and fixtures

## 🔍 Finding Things

### "Where is the X functionality?"

| Feature | Location |
|---------|----------|
| Form ADV ingestion | `app/sources/sec/formadv_*.py` |
| Form ADV API endpoints | `app/api/v1/sec.py` (search "form-adv") |
| Form ADV scripts | `scripts/family_office/` |
| Form ADV docs | `docs/FORM_ADV_*.md` |
| SEC EDGAR filings | `app/sources/sec/` (not formadv_*) |
| FRED data | `app/sources/fred/` |
| Census data | `app/sources/census/` |
| Job tracking | `app/core/models.py` + `app/api/v1/jobs.py` |
| Database models | `app/core/models.py` |
| API schemas | `app/core/schemas.py` |
| Configuration | `app/core/config.py` + `.env` |

### "Where do I start?"

1. **First time:** `README.md` → `docs/GETTING_STARTED.md`
2. **Quick test:** `python scripts/quick_demo.py`
3. **API docs:** http://localhost:8001/docs (Swagger UI)
4. **Specific feature:** Check `docs/` for relevant guide
5. **Scripts:** Check `scripts/README.md`

## 📝 File Naming Conventions

### Documentation
- `UPPERCASE.md` - Project-level docs (README, RULES)
- `Title_Case.md` - Feature/implementation docs in /docs/

### Python Files
- `lowercase_with_underscores.py` - Standard Python convention
- `test_*.py` - Test files (pytest discovery)

### Scripts
- `*.py` - Python scripts
- `*.ps1` - PowerShell scripts
- `*.sh` - Bash scripts

## 🎯 Best Practices

### Adding New Features

1. **Source Adapters** → `app/sources/your_source/`
2. **API Endpoints** → `app/api/v1/your_endpoints.py`
3. **Documentation** → `docs/YOUR_FEATURE_GUIDE.md`
4. **Tests** → `tests/test_your_feature.py`
5. **Scripts** → `scripts/your_feature/` (if needed)

### Organization Rules

- ✅ Keep root directory clean (only essential files)
- ✅ Put docs in `/docs/`
- ✅ Put scripts in `/scripts/` or `/scripts/subdirectory/`
- ✅ Follow the plugin pattern for data sources
- ✅ Document in README files

## 🔄 Maintenance

### Cleaning Up

```bash
# Remove Python cache
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

# Remove logs (if any)
rm -f *.log

# Docker cleanup
docker-compose down
docker system prune -f
```

### Updating Structure

If you move files, update:
1. This document (`DIRECTORY_STRUCTURE.md`)
2. Import statements in code
3. Documentation links
4. Script paths
5. README references

## 📞 Questions?

- Check `README.md` first
- Browse `/docs/` for specific topics
- Open http://localhost:8001/docs for API reference
- See `scripts/README.md` for script usage

