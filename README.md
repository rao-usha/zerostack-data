# Nexdata - External Data Ingestion Service

> **A unified API to ingest and query data from multiple public data sources**

Stop writing custom code for each data provider. Use one consistent REST API to access census data, economic indicators, energy statistics, SEC filings, and more—all stored in PostgreSQL with automatic rate limiting and error handling.

---

## 🎯 What Can You Do With This?

Access data from major U.S. public data providers through a single API:

| Source | What You Get | Status |
|--------|--------------|--------|
| **📊 Census Bureau** | Demographics, housing, economic characteristics by geography | ✅ Ready |
| **💰 FRED** | 800,000+ economic time series (GDP, unemployment, inflation) | ✅ Ready |
| **⚡ EIA** | Energy prices, production, consumption data | ✅ Ready |
| **🏛️ SEC** | Company financials, Form ADV investment adviser filings | ✅ Ready |
| **🌦️ NOAA** | Weather observations, historical climate data | ✅ Ready |
| **🏠 Real Estate** | Zillow home values, rental market indices | ✅ Ready |

---

## 🚀 Quick Start (5 Minutes)

### Prerequisites

- **Python 3.11+** ([Download](https://www.python.org/downloads/))
- **Docker Desktop** ([Download](https://www.docker.com/products/docker-desktop/)) - For PostgreSQL
- **Git** ([Download](https://git-scm.com/downloads))

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/LearnToCodeNJ/datacollector.git
cd datacollector

# 2. Create Python virtual environment
python -m venv venv

# Activate it:
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file with your configuration
# Copy and customize:
cp .env.example .env  # Or create manually (see below)

# 5. Start the service (includes PostgreSQL)
python scripts/start_service.py
```

**✅ That's it!** The API is now running at **http://localhost:8000**

---

## 📖 View API Documentation (Swagger UI)

Once the service is running, visit:

### **🌐 http://localhost:8000/docs**

This opens the **interactive Swagger UI** where you can:
- ✅ Browse all available endpoints
- ✅ See request/response schemas
- ✅ Try API calls directly in your browser
- ✅ View detailed documentation for each data source

**Alternative documentation formats:**
- **ReDoc UI**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

---

## ⚙️ Configuration

### Step 1: Get API Keys (Free)

Some data sources work better with API keys. They're free and take 2 minutes:

| Source | Required? | Get Key Here | Time to Get |
|--------|-----------|--------------|-------------|
| Census | Recommended | [Get Key](https://api.census.gov/data/key_signup.html) | 1 minute |
| FRED | Required | [Get Key](https://fred.stlouisfed.org/docs/api/api_key.html) | 2 minutes |
| EIA | Required | [Get Key](https://www.eia.gov/opendata/register.php) | 2 minutes |
| NOAA | Optional | [Get Token](https://www.ncdc.noaa.gov/cdo-web/token) | 2 minutes |
| SEC | Not Needed | N/A | - |

### Step 2: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Database (automatically managed by Docker)
DATABASE_URL=postgresql://nexdata:nexdata@localhost:5432/nexdata

# API Keys (add the ones you obtained)
CENSUS_SURVEY_API_KEY=your_census_key_here
FRED_API_KEY=your_fred_key_here
EIA_API_KEY=your_eia_key_here

# Optional: Rate limiting (adjust as needed)
MAX_CONCURRENCY=5
MAX_REQUESTS_PER_SECOND=10
```

---

## 🎓 Your First Data Ingestion

### Example 1: Census Population Data

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

# Response: {"job_id": "123e4567-e89b-12d3-a456-426614174000"}

# Check status
curl http://localhost:8000/api/v1/jobs/123e4567-e89b-12d3-a456-426614174000

# When status is "success", data is in PostgreSQL table: acs5_2023_b01001
```

### Example 2: Economic Data (FRED)

```bash
# Ingest unemployment rate data
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source": "fred",
    "config": {
      "series_id": "UNRATE",
      "start_date": "2020-01-01",
      "end_date": "2024-12-31"
    }
  }'
```

### Example 3: Energy Prices (EIA)

```bash
# Ingest crude oil prices
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source": "eia",
    "config": {
      "series_id": "PET.RWTC.W",
      "frequency": "weekly"
    }
  }'
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

---

## 🎬 Run Demo Scripts

We provide ready-to-run examples:

```bash
# Quick demo (30 seconds) - Tests all sources
python scripts/quick_demo.py

# Full demo (5 minutes) - Populates real data
python scripts/populate_demo_data.py

# Source-specific examples
python scripts/example_usage.py          # Census examples
python scripts/direct_fred_ingest.py     # FRED examples
python scripts/example_noaa_usage.py     # NOAA examples
```

---

## 🔍 Accessing Your Data

### Option 1: Via PostgreSQL

```bash
# Connect to the database
docker exec -it nexdata-db psql -U nexdata -d nexdata

# List all tables
\dt

# Query ingested data
SELECT * FROM acs5_2023_b01001 LIMIT 10;
SELECT * FROM fred_series WHERE series_id = 'UNRATE';
```

### Option 2: Via API

Use source-specific query endpoints (see Swagger UI at `/docs`)

---

## 📊 Database Tables

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

---

## 🔧 Tech Stack

- **FastAPI** - Modern async Python web framework
- **PostgreSQL** - Robust relational database
- **SQLAlchemy** - ORM for database operations
- **httpx** - Async HTTP client with retry logic
- **Docker** - PostgreSQL containerization
- **Pytest** - Testing framework

---

## 🧪 Testing

```bash
# Run all unit tests
pytest tests/

# Run with coverage report
pytest --cov=app --cov-report=html tests/

# Run integration tests (requires API keys)
RUN_INTEGRATION_TESTS=true pytest tests/integration/

# Run specific test file
pytest tests/test_census_client_url_building.py -v
```

---

## 🚨 Troubleshooting

### Service won't start

**Problem**: PostgreSQL not running or port conflicts

**Solution**:
```bash
# Check if PostgreSQL is running
docker ps

# Start PostgreSQL
docker-compose up -d db

# Check port usage (Windows)
netstat -ano | findstr :8000

# Check port usage (Mac/Linux)
lsof -i :8000
```

### "Database connection refused"

**Problem**: PostgreSQL isn't ready yet

**Solution**: Wait 10-15 seconds after starting Docker, or use the automatic script:
```bash
python scripts/start_service.py  # Handles startup and health checks
```

### "API key invalid" errors

**Problem**: Missing or incorrect API keys

**Solution**:
1. Verify keys in your `.env` file
2. Check keys are valid at provider websites
3. Restart service after updating `.env`

### "Module not found" errors

**Problem**: Virtual environment not activated

**Solution**:
```bash
# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate

# Then reinstall
pip install -r requirements.txt
```

---

## 📚 Documentation

### Quick Start Guides (by Source)
- **[QUICKSTART.md](QUICKSTART.md)** - Get running in 5 minutes
- **[CENSUS_METADATA_API_REFERENCE.md](docs/CENSUS_METADATA_API_REFERENCE.md)** - Census API details
- **[FRED_QUICK_START.md](docs/FRED_QUICK_START.md)** - Federal Reserve data
- **[EIA_QUICK_START.md](docs/EIA_QUICK_START.md)** - Energy data
- **[SEC_QUICK_START.md](docs/SEC_QUICK_START.md)** - SEC filings
- **[NOAA_QUICK_START.md](docs/NOAA_QUICK_START.md)** - Weather data
- **[REALESTATE_QUICK_START.md](docs/REALESTATE_QUICK_START.md)** - Real estate data
- **[FORM_ADV_QUICKSTART.md](FAMILY_OFFICE_QUICKSTART.md)** - Family office data

### Advanced Guides
- **[DEMO.md](docs/DEMO.md)** - Demonstration guide
- **[GEOGRAPHIC_API_GUIDE.md](docs/GEOGRAPHIC_API_GUIDE.md)** - Geographic data handling
- **[MULTI_YEAR_DATA_GUIDE.md](docs/MULTI_YEAR_DATA_GUIDE.md)** - Multi-year ingestion
- **[USAGE.md](docs/USAGE.md)** - Detailed usage examples

### Development
- **[RULES.md](RULES.md)** - Development guidelines and architecture
- **[EXTERNAL_DATA_SOURCES.md](docs/EXTERNAL_DATA_SOURCES.md)** - Planned future sources
- **[STATUS_REPORT.md](docs/STATUS_REPORT.md)** - Implementation status

---

## 🏗️ Architecture

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

**Key Principles:**
1. **Source Isolation** - Each data source is self-contained
2. **Consistent Interface** - Common adapter pattern
3. **Job Tracking** - Every ingestion is tracked
4. **Rate Limiting** - Respects API limits
5. **Type Safety** - Strongly typed database schemas
6. **Idempotency** - Safe to re-run

---

## 📂 Project Structure

```
datacollector/
├── app/                      # Main application
│   ├── main.py              # FastAPI entry point
│   ├── core/                # Core functionality
│   │   ├── config.py        # Configuration
│   │   ├── database.py      # Database connection
│   │   ├── models.py        # SQLAlchemy models
│   │   └── schemas.py       # Pydantic schemas
│   ├── sources/             # Data source adapters
│   │   ├── census/
│   │   ├── fred/
│   │   ├── eia/
│   │   ├── sec/
│   │   ├── noaa/
│   │   └── realestate/
│   └── api/v1/              # API endpoints
├── scripts/                 # Utility scripts
├── tests/                   # Test suite
├── docs/                    # Documentation
├── docker-compose.yml       # Docker configuration
├── requirements.txt         # Python dependencies
├── .env                    # Environment variables (create this)
└── README.md               # This file
```

---

## 🚀 Production Deployment

### Using Docker Compose

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Production Environment Variables

```bash
# .env for production
DATABASE_URL=postgresql://user:password@db-host:5432/nexdata
ENVIRONMENT=production

# API Keys
CENSUS_SURVEY_API_KEY=your_key
FRED_API_KEY=your_key
EIA_API_KEY=your_key

# Rate Limiting (adjust for your API tier)
MAX_CONCURRENCY=10
MAX_REQUESTS_PER_SECOND=20

# Security
SECRET_KEY=your-secret-key-here
ALLOWED_HOSTS=your-domain.com,api.your-domain.com
```

### Health Check

```bash
curl http://localhost:8000/health

# Returns: {"status": "healthy", "database": "connected"}
```

---

## 🤝 Contributing

We welcome contributions! To add a new data source:

1. Read **[RULES.md](RULES.md)** - Understand architecture
2. Create module: `app/sources/your_source/`
3. Implement adapter following plugin pattern
4. Add tests
5. Document: Create `docs/YOUR_SOURCE_QUICK_START.md`
6. Submit PR

### Development Setup

```bash
# Install dev dependencies
pip install pytest pytest-cov black flake8

# Format code
black app/ tests/

# Lint
flake8 app/ tests/

# Run tests
pytest
```

---

## ❓ FAQ

**Q: Do I need API keys for all sources?**  
A: No, only for sources you want to use. Census and SEC work without keys (with rate limits).

**Q: Can I use this in production?**  
A: Yes! Includes rate limiting, error handling, and job tracking suitable for production.

**Q: How do I view the API documentation?**  
A: Visit **http://localhost:8000/docs** (Swagger UI) after starting the service.

**Q: Where is my data stored?**  
A: PostgreSQL database. Connect with: `docker exec -it nexdata-db psql -U nexdata -d nexdata`

**Q: What if a job fails?**  
A: Check status at `/api/v1/jobs/{job_id}` for error details. Jobs are idempotent—safe to retry.

**Q: Can I ingest historical data?**  
A: Yes! All sources support historical data. See source-specific docs for date range limits.

---

## 📄 License

MIT License - See LICENSE file for details

---

## 🙏 Data Providers

Data provided by:
- U.S. Census Bureau
- Federal Reserve Bank of St. Louis (FRED)
- U.S. Energy Information Administration
- U.S. Securities and Exchange Commission
- National Oceanic and Atmospheric Administration
- Zillow Research

---

## 📞 Support

- **GitHub Issues**: Report bugs or request features
- **Documentation**: Check `/docs` directory for guides
- **API Docs**: http://localhost:8000/docs (when running)

---

**Built with ❤️ for the data community**

**👉 Get Started Now:** [Jump to Quick Start](#-quick-start-5-minutes)
