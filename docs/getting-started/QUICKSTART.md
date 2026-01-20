# Quick Start Guide

Get the External Data Ingestion Service up and running in under 5 minutes!

## Prerequisites

Before starting, ensure you have:
- ✅ Python 3.11+
- ✅ Docker and Docker Compose
- ✅ PostgreSQL client tools (optional, for manual testing)

## Step-by-Step Setup

### 1. Clone and Navigate

```bash
cd /path/to/Nexdata
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

Create a `.env` file in the project root:

```bash
# Database (Docker Compose will use these)
POSTGRES_USER=nexdata_user
POSTGRES_PASSWORD=nexdata_password
POSTGRES_DB=nexdata

DATABASE_URL=postgresql://nexdata_user:nexdata_password@localhost:5432/nexdata

# API Keys (get your own - these are optional for initial testing)
CENSUS_SURVEY_API_KEY=your_census_api_key_here
FRED_API_KEY=your_fred_api_key_here
EIA_API_KEY=your_eia_api_key_here

# Optional: Rate Limiting
MAX_CONCURRENCY=5
MAX_REQUESTS_PER_SECOND=10
```

**Get API Keys:**
- Census: https://api.census.gov/data/key_signup.html
- FRED: https://fred.stlouisfed.org/docs/api/api_key.html
- EIA: https://www.eia.gov/opendata/register.php

### 5. Start the Service

**Easy Mode (Recommended):**

```bash
# This script handles everything!
python scripts/start_service.py
```

The script will:
1. ✅ Check prerequisites
2. ✅ Start PostgreSQL in Docker
3. ✅ Wait for database to be ready
4. ✅ Start the FastAPI application
5. ✅ Monitor health and auto-restart if needed

**Manual Mode:**

```bash
# Terminal 1: Start database
docker-compose up -d db

# Wait for database to be ready (~10 seconds)
docker-compose exec db pg_isready -U postgres

# Terminal 2: Start API
uvicorn app.main:app --reload
```

### 6. Verify It's Running

Open your browser:
- **API Documentation:** http://localhost:8001/docs
- **Health Check:** http://localhost:8001/health

You should see:
```json
{
  "status": "healthy",
  "service": "running",
  "database": "connected"
}
```

### 7. Quick Demo (Optional but Recommended!)

Want to see it in action immediately? Run the quick demo script:

```bash
python scripts/quick_demo.py
```

This will:
- ✅ Verify everything is working
- ✅ Ingest 3 sample datasets (GDP, Unemployment, Census)
- ✅ Complete in ~30 seconds

**Or populate with more demo data:**

```bash
# Full demo with all sources (~5 minutes)
python scripts/populate_demo_data.py

# Quick mode (~2 minutes)
python scripts/populate_demo_data.py --quick

# Specific sources only
python scripts/populate_demo_data.py --sources census,fred
```

### 8. Test Your First Manual Ingestion

**Option A: Using the API Docs**

1. Go to http://localhost:8001/docs
2. Expand `POST /api/v1/jobs`
3. Click "Try it out"
4. Use this example:

```json
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

5. Click "Execute"
6. Copy the `job_id` from the response

**Option B: Using curl**

```bash
curl -X POST "http://localhost:8001/api/v1/jobs" \
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
```

### 9. Check Job Status

```bash
# Replace {job_id} with the actual job ID
curl http://localhost:8001/api/v1/jobs/{job_id}
```

Or visit: http://localhost:8001/api/v1/jobs/{job_id} in your browser

## Common Issues

### Database Won't Start

```bash
# Check if port 5432 is already in use
docker ps

# If needed, stop existing containers
docker-compose down

# Try again
python scripts/start_service.py
```

### Import Errors

```bash
# Make sure virtual environment is activated
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### API Key Errors

Some sources require API keys. For testing, you can:
1. Get free API keys (links above)
2. Use sources that don't require keys initially
3. Check logs for specific error messages

### Port 8000 Already in Use

```bash
# Find process using port 8000
# Windows:
netstat -ano | findstr :8000
# Linux/Mac:
lsof -i :8000

# Kill the process or use a different port
uvicorn app.main:app --port 8001
```

## Next Steps

Now that your service is running:

1. **Explore Available Sources:** See `docs/EXTERNAL_DATA_SOURCES.md`
2. **Read Source-Specific Guides:** Check `docs/*_QUICK_START.md` files
3. **Run Tests:** `pytest tests/`
4. **Check Examples:** See `scripts/example_*.py` files
5. **Monitor Jobs:** Use the API at `/api/v1/jobs`

## Stopping the Service

If you used the startup script:
- Press `Ctrl+C` to stop gracefully

If you started manually:
```bash
# Stop API: Press Ctrl+C in the uvicorn terminal

# Stop database:
docker-compose down
```

## Getting Help

- **Documentation:** See `docs/` directory
- **API Docs:** http://localhost:8001/docs (when running)
- **Rules & Guidelines:** See `RULES.md`
- **Project Structure:** See main `README.md`

---

**Happy Data Ingesting! 🚀**

