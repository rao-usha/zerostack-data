# Getting Started - Quick Reference

**Welcome!** Here's everything you need to get up and running fast.

## 🎯 One-Command Start

```bash
# 1. Start the service
python scripts/start_service.py

# 2. In another terminal, populate demo data
python scripts/quick_demo.py
```

**Done!** You now have a working data ingestion service with sample data.

---

## 📚 What You Have

### Core Service
- **FastAPI application** with automatic API docs
- **PostgreSQL database** managed by Docker
- **Multiple data sources** (Census, FRED, EIA, SEC, Real Estate, NOAA)
- **Job tracking** for all ingestion operations
- **Health monitoring** and auto-restart capabilities

### Startup Scripts
Three ways to start (choose one):
```bash
python scripts/start_service.py      # Cross-platform (recommended)
bash scripts/start_service.sh        # Linux/Mac
.\scripts\start_service.ps1          # Windows PowerShell
```

All include:
- ✅ Database startup and health checks
- ✅ Application monitoring
- ✅ Auto-restart on failures
- ✅ Graceful shutdown

### Demo Scripts

**Quick Demo (~30 seconds):**
```bash
python scripts/quick_demo.py
```
Ingests 3 datasets to verify everything works.

**Full Demo (~5 minutes):**
```bash
python scripts/populate_demo_data.py

# Or quick mode (~2 minutes)
python scripts/populate_demo_data.py --quick

# Or specific sources only
python scripts/populate_demo_data.py --sources census,fred
```

Comprehensive demo with data from all sources.

---

## 🔗 Important URLs

Once the service is running:

- **API Documentation:** http://localhost:8001/docs
- **Health Check:** http://localhost:8001/health
- **List All Jobs:** http://localhost:8001/api/v1/jobs
- **Root Endpoint:** http://localhost:8001/

---

## 📖 Documentation Guide

| Document | Purpose | When to Read |
|----------|---------|--------------|
| **README.md** | Main overview | Start here |
| **QUICKSTART.md** | 5-minute setup guide | First time setup |
| **DEMO.md** | Demonstration guide | Before presenting |
| **RULES.md** | Development guidelines | Before adding features |
| **docs/PROJECT_ORGANIZATION.md** | Structure guide | Understanding layout |
| **scripts/README.md** | Scripts reference | Using utility scripts |

---

## 🚀 Typical Workflow

### First Time Setup
1. Clone/navigate to project
2. Create virtual environment: `python -m venv venv`
3. Activate: `venv\Scripts\activate` (Windows) or `source venv/bin/activate` (Linux/Mac)
4. Install: `pip install -r requirements.txt`
5. Configure: Create `.env` file with API keys
6. Start: `python scripts/start_service.py`
7. Demo: `python scripts/quick_demo.py`

### Daily Development
1. Start service: `python scripts/start_service.py`
2. Make changes to code
3. Test: API auto-reloads on save
4. Check: http://localhost:8001/docs

### Running Tests
```bash
# Unit tests
pytest tests/

# Integration tests (requires API keys)
RUN_INTEGRATION_TESTS=true pytest tests/integration/

# With coverage
pytest --cov=app tests/
```

---

## 🎓 Key Concepts

### Job-Based Ingestion
Every data ingestion creates a tracked job:
- **pending** → **running** → **success** or **failed**
- Stored in `ingestion_jobs` table
- Check status via API: `/api/v1/jobs/{job_id}`

### Plugin Architecture
- **Core** (`app/core/`) - Source-agnostic logic
- **Sources** (`app/sources/`) - Isolated source adapters
- **API** (`app/api/`) - HTTP endpoints
- Adding a new source doesn't require changing core code

### Data Safety
- Rate limiting and bounded concurrency
- Parameterized SQL queries
- Job tracking (never "fire and forget")
- Typed database columns (no JSON blobs)

---

## 🛠️ Common Tasks

### Ingest Data from a Specific Source

**Via API (interactive):**
1. Go to http://localhost:8001/docs
2. Find your source endpoint (e.g., `/api/v1/fred/ingest`)
3. Click "Try it out"
4. Fill in parameters
5. Execute

**Via curl:**
```bash
curl -X POST http://localhost:8001/api/v1/fred/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "series_ids": ["GDP"],
    "observation_start": "2020-01-01"
  }'
```

### Check Job Status
```bash
# Get job ID from ingestion response, then:
curl http://localhost:8001/api/v1/jobs/{job_id}
```

### List All Jobs
```bash
curl http://localhost:8001/api/v1/jobs

# Or visit in browser:
http://localhost:8001/api/v1/jobs
```

### Query Ingested Data
```bash
# Via API (if endpoint exists)
curl http://localhost:8001/api/v1/fred/series/GDP/observations

# Via database (direct)
docker-compose exec db psql -U nexdata_user -d nexdata
\dt  # List tables
SELECT * FROM fred_gdp LIMIT 10;
```

---

## 🔧 Troubleshooting

### Service Won't Start
```bash
# Check Docker
docker ps

# Restart everything
docker-compose down
python scripts/start_service.py
```

### Port 8000 Already in Use
```bash
# Find and kill process
# Windows:
netstat -ano | findstr :8000
taskkill /PID <pid> /F

# Linux/Mac:
lsof -ti:8000 | xargs kill -9
```

### Jobs Failing
```bash
# Check API keys in .env
cat .env

# Check specific job error
curl http://localhost:8001/api/v1/jobs/{job_id}

# Check logs
tail -f service_startup.log
```

### Database Issues
```bash
# Restart database
docker-compose restart db

# Check database logs
docker-compose logs db

# Connect directly
docker-compose exec db psql -U nexdata_user -d nexdata
```

---

## 📊 Data Sources

Currently implemented:

| Source | Description | API Key Required |
|--------|-------------|------------------|
| **Census** | ACS, Decennial, PUMS, TIGER/Line | Yes |
| **FRED** | Economic indicators, rates, aggregates | Optional (recommended) |
| **EIA** | Energy data, gas prices, electricity | Yes |
| **SEC** | Corporate filings, financials, XBRL | No |
| **Real Estate** | FHFA, HUD, Redfin, OSM | No |
| **NOAA** | Weather, climate normals | Yes (free) |
| **Public LP** | Pension fund strategies | No |

See `docs/EXTERNAL_DATA_SOURCES.md` for complete list.

---

## 🎯 Next Steps

1. **Explore the API:** http://localhost:8001/docs
2. **Try the demo:** `python scripts/populate_demo_data.py`
3. **Read source guides:** `docs/*_QUICK_START.md`
4. **Add your own data:** Follow patterns in `app/sources/`
5. **Run tests:** `pytest tests/`

---

## 📞 Quick Reference Commands

```bash
# Start service
python scripts/start_service.py

# Quick demo (30s)
python scripts/quick_demo.py

# Full demo (5min)
python scripts/populate_demo_data.py

# Run tests
pytest tests/

# Check health
curl http://localhost:8001/health

# View API docs
# Visit: http://localhost:8001/docs

# Stop everything
# Press Ctrl+C (in service terminal)
docker-compose down  # Stop database
```

---

**That's it! You're ready to go.** 🚀

For more details, see:
- Main docs: **README.md**
- Setup guide: **QUICKSTART.md**
- Demo guide: **DEMO.md**
- Development rules: **RULES.md**

