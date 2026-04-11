# Project Organization Summary

This document describes the organizational structure of the External Data Ingestion Service codebase.

## Directory Structure

```
Nexdata/
├── app/                        # Main application code
│   ├── api/                   # API routes (v1)
│   │   └── v1/               # Version 1 API endpoints
│   ├── core/                  # Core functionality (source-agnostic)
│   │   ├── config.py         # Configuration management
│   │   ├── database.py       # Database connection & setup
│   │   ├── models.py         # SQLAlchemy models
│   │   └── schemas.py        # Pydantic schemas
│   ├── sources/              # Data source adapters (plugin architecture)
│   │   ├── census/           # U.S. Census Bureau adapter
│   │   ├── fred/             # Federal Reserve Economic Data
│   │   ├── eia/              # Energy Information Administration
│   │   ├── noaa/             # NOAA climate/weather data
│   │   ├── sec/              # SEC filings and data
│   │   ├── realestate/       # Real estate data
│   │   └── public_lp_strategies/  # LP investment strategies
│   └── main.py               # FastAPI application entry point
│
├── tests/                     # Test suite
│   ├── integration/          # Integration tests
│   └── test_*.py            # Unit tests
│
├── scripts/                   # Utility scripts & examples
│   ├── start_service.py      # Cross-platform startup script
│   ├── start_service.ps1     # Windows PowerShell startup script
│   ├── start_service.sh      # Linux/Mac bash startup script
│   ├── example_*.py          # Usage examples
│   ├── ingest_*.py           # Ingestion scripts
│   ├── fetch_*.py            # Data fetching utilities
│   └── README.md             # Scripts documentation
│
├── docs/                      # Documentation
│   ├── *_QUICK_START.md      # Source-specific quick start guides
│   ├── *_IMPLEMENTATION_SUMMARY.md  # Implementation details
│   ├── EXTERNAL_DATA_SOURCES.md     # All available sources
│   ├── STATUS_REPORT.md      # Current project status
│   └── README.md             # Documentation index
│
├── data/                      # Data files & outputs
│   ├── *.csv                 # Metadata files
│   ├── *.json                # Output data
│   ├── *.sql                 # SQL scripts
│   ├── sec_data_output/      # SEC company data
│   └── README.md             # Data directory guide
│
├── venv/                      # Python virtual environment
│
├── docker-compose.yml         # Docker services configuration
├── Dockerfile                 # Docker image definition
├── requirements.txt           # Python dependencies
├── pytest.ini                # Pytest configuration
├── .gitignore                # Git ignore rules
├── README.md                 # Main project documentation
├── QUICKSTART.md             # 5-minute setup guide
└── RULES.md                  # Development guidelines

```

## Root Directory Files

### Essential Files (Keep in Root)

- **README.md** - Main project documentation with overview and links
- **QUICKSTART.md** - Quick 5-minute setup guide for new users
- **RULES.md** - Development guidelines and architectural rules
- **requirements.txt** - Python package dependencies
- **docker-compose.yml** - Docker services configuration
- **Dockerfile** - Docker image build instructions
- **pytest.ini** - Test framework configuration
- **.gitignore** - Git version control ignore rules

### Configuration Files

- **.env** (not in repo) - Environment variables and API keys
- **docker-compose.override.yml** (optional) - Local Docker overrides

## Application Structure (`app/`)

### Core Module (`app/core/`)

Source-agnostic functionality:
- **config.py** - Settings management (env vars, validation)
- **database.py** - SQLAlchemy engine, session management, table creation
- **models.py** - Database models (ingestion_jobs, dataset_registry)
- **schemas.py** - Pydantic request/response schemas

### API Module (`app/api/`)

HTTP endpoints organized by version:
- **v1/jobs.py** - Job management endpoints
- **v1/census_*.py** - Census-specific endpoints
- **v1/fred.py** - FRED endpoints
- **v1/eia.py** - EIA endpoints
- **v1/sec.py** - SEC endpoints
- **v1/geojson.py** - GeoJSON endpoints
- *etc.*

### Sources Module (`app/sources/`)

Each data source has its own isolated module:

```
sources/
├── {source_name}/
│   ├── __init__.py
│   ├── client.py        # API client / HTTP requests
│   ├── ingest.py        # Ingestion logic
│   └── metadata.py      # Metadata handling (optional)
```

**Key Principle:** Each source is self-contained. Adding a new source doesn't require modifying core code.

## Scripts Organization (`scripts/`)

### Service Management
- `start_service.py` / `.ps1` / `.sh` - Robust startup with auto-restart

### Examples
- `example_usage.py` - Basic usage
- `example_noaa_usage.py` - NOAA-specific examples

### Ingestion Scripts
- `ingest_fred_data.py`, `ingest_sec_companies.py`, etc.
- Source-specific data ingestion utilities

### Data Fetching
- `fetch_100_companies_data.py`, `pull_sec_data.py`, etc.

### Testing & Utilities
- `test_*.py` - Test scripts
- `check_*.py` - Progress monitoring
- `populate_*.py` - Database population scripts

See `scripts/README.md` for detailed descriptions.

## Documentation Organization (`docs/`)

### Quick Start Guides
Pattern: `{SOURCE}_QUICK_START.md`
- `CENSUS_METADATA_API_REFERENCE.md`
- `FRED_QUICK_START.md`
- `EIA_QUICK_START.md`
- `NOAA_QUICK_START.md`
- `SEC_QUICK_START.md`
- `GEOJSON_QUICK_START.md`
- etc.

### Implementation Summaries
Pattern: `{SOURCE}_IMPLEMENTATION_SUMMARY.md`
- Detailed technical documentation for each source

### Reference Docs
- `EXTERNAL_DATA_SOURCES.md` - All available/planned sources
- `QUICK_REFERENCE.md` - General API reference
- `USAGE.md` - Usage patterns and examples

### Project Status
- `STATUS_REPORT.md` - Current implementation status
- `CHANGELOG.md` - Version history
- `FIXES_AND_IMPROVEMENTS.md` - Known issues and improvements

See `docs/README.md` for navigation guide.

## Data Directory (`data/`)

Contains generated data, outputs, and local files:
- **Metadata files** - Census variable metadata, etc.
- **Output data** - SEC company JSON files, ingested data
- **SQL scripts** - Table creation, migrations
- **.gitkeep** - Ensures directory exists in repo

**Note:** Most contents are gitignored. Only README and .gitkeep are tracked.

## Tests Organization (`tests/`)

```
tests/
├── conftest.py              # Pytest fixtures
├── test_*.py                # Unit tests
└── integration/             # Integration tests
    └── test_*.py
```

Run tests:
```bash
# Unit tests (no API keys needed)
pytest tests/

# Integration tests (requires API keys)
RUN_INTEGRATION_TESTS=true pytest tests/integration/
```

## Plugin Architecture

The service uses a **plugin-based architecture** for data sources:

1. **Core is source-agnostic** - `app/core/` doesn't know about specific sources
2. **Sources are isolated** - Each source in `app/sources/{name}/`
3. **Registration-based** - Sources register via API routes in `app/api/`
4. **Consistent interface** - All sources use same job tracking and database patterns

### Adding a New Source

1. Create `app/sources/{new_source}/`
2. Implement client, ingest logic, schemas
3. Create API routes in `app/api/v1/{new_source}.py`
4. Register routes in `app/main.py`
5. Add documentation in `docs/{NEW_SOURCE}_QUICK_START.md`
6. Add tests in `tests/test_{new_source}.py`

See `RULES.md` for detailed guidelines.

## File Naming Conventions

- **Python modules**: `lowercase_with_underscores.py`
- **Test files**: `test_{module_name}.py`
- **Documentation**: `UPPERCASE_WITH_UNDERSCORES.md`
- **Quick starts**: `{SOURCE}_QUICK_START.md`
- **Scripts**: `{action}_{source}_{detail}.py`

## Git Ignore Strategy

The `.gitignore` file excludes:
- Python build artifacts (`__pycache__`, `*.pyc`)
- Virtual environment (`venv/`)
- IDE files (`.vscode/`, `.idea/`)
- Environment files (`.env`)
- Data files (`data/*` except README)
- Log files (`*.log`)
- Test artifacts (`.pytest_cache/`, `.coverage`)

## Build Artifacts

Generated at runtime, not tracked in git:
- `__pycache__/` - Python bytecode cache
- `*.pyc` - Compiled Python files
- `.pytest_cache/` - Pytest cache
- `*.log` - Log files

## Best Practices

### Do:
✅ Keep root directory clean with only essential config files
✅ Organize code by function (app/, tests/, scripts/, docs/)
✅ Use meaningful names for files and directories
✅ Include README.md in major directories
✅ Keep data source logic isolated in sources/
✅ Document changes and new features

### Don't:
❌ Put random scripts in root directory
❌ Mix documentation with code
❌ Put data files in version control
❌ Hardcode source-specific logic in core
❌ Create deeply nested directory structures

## Maintenance

When adding new files, ask:
1. **Is this code?** → `app/` or `tests/`
2. **Is this a script?** → `scripts/`
3. **Is this documentation?** → `docs/`
4. **Is this data?** → `data/`
5. **Is this configuration?** → Root level (only if essential)

## Summary

This organization provides:
- **Clear separation of concerns** - Code, tests, scripts, docs, data
- **Easy navigation** - Find what you need quickly
- **Scalability** - Add new sources without cluttering
- **Maintainability** - Consistent structure and naming
- **Onboarding** - New developers can understand structure quickly

---

**Last Updated:** 2025-11-30

For questions or suggestions, see main `README.md` or `RULES.md`.

