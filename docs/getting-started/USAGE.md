# External Data Ingestion Service - Usage Guide

## Quick Start

### 1. Set Up Environment

```bash
# Clone repository
git clone <repository-url>
cd Nexdata

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env
# Edit .env and add your configuration
```

### 2. Configure Environment Variables

Edit `.env`:

```bash
# Required
DATABASE_URL=postgresql://user:password@localhost:5432/nexdata

# Required for Census ingestion
CENSUS_SURVEY_API_KEY=your_actual_key_here

# Optional (defaults shown)
MAX_CONCURRENCY=4
LOG_LEVEL=INFO
```

Get a Census API key (free): https://api.census.gov/data/key_signup.html

### 3. Start Database (Option A: Docker)

```bash
docker-compose up postgres -d
```

### 3. Start Database (Option B: Local PostgreSQL)

Install and configure PostgreSQL, then create database:

```sql
CREATE DATABASE nexdata;
```

### 4. Run the Service

```bash
# Start API server
uvicorn app.main:app --reload

# API available at: http://localhost:8001
# Interactive docs: http://localhost:8001/docs
```

## Using the API

### Create an Ingestion Job

```bash
curl -X POST http://localhost:8001/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source": "census",
    "config": {
      "survey": "acs5",
      "year": 2021,
      "table_id": "B01001",
      "geo_level": "state"
    }
  }'
```

Response:
```json
{
  "id": 1,
  "source": "census",
  "status": "pending",
  "config": {
    "survey": "acs5",
    "year": 2021,
    "table_id": "B01001",
    "geo_level": "state"
  },
  "created_at": "2024-01-15T10:30:00Z",
  "started_at": null,
  "completed_at": null,
  "rows_inserted": null,
  "error_message": null
}
```

### Check Job Status

```bash
curl http://localhost:8001/api/v1/jobs/1
```

Response (completed):
```json
{
  "id": 1,
  "source": "census",
  "status": "success",
  "config": {...},
  "created_at": "2024-01-15T10:30:00Z",
  "started_at": "2024-01-15T10:30:01Z",
  "completed_at": "2024-01-15T10:31:45Z",
  "rows_inserted": 52,
  "error_message": null
}
```

### List Jobs

```bash
# All jobs
curl http://localhost:8001/api/v1/jobs

# Filter by source
curl http://localhost:8001/api/v1/jobs?source=census

# Filter by status
curl http://localhost:8001/api/v1/jobs?status=success
```

## Census Source Configuration

### Required Parameters

- `survey` - Survey type (default: "acs5")
  - Options: "acs1", "acs5"
- `year` - Survey year (required)
  - Example: 2021, 2022, 2023
- `table_id` - Census table ID (required)
  - Example: "B01001", "B19013", "B25001"
- `geo_level` - Geographic level (default: "state")
  - Options: "state", "county", "tract", "block group"

### Optional Parameters

- `geo_filter` - Geographic filter (optional)
  - Example: `{"state": "06"}` for California
  - Example: `{"state": "06", "county": "037"}` for Los Angeles County

### Example Configurations

#### State-level data for all states

```json
{
  "source": "census",
  "config": {
    "survey": "acs5",
    "year": 2021,
    "table_id": "B01001",
    "geo_level": "state"
  }
}
```

#### County-level data for specific state

```json
{
  "source": "census",
  "config": {
    "survey": "acs5",
    "year": 2021,
    "table_id": "B19013",
    "geo_level": "county",
    "geo_filter": {"state": "06"}
  }
}
```

#### Tract-level data for specific county

```json
{
  "source": "census",
  "config": {
    "survey": "acs5",
    "year": 2021,
    "table_id": "B25001",
    "geo_level": "tract",
    "geo_filter": {"state": "06", "county": "037"}
  }
}
```

## Common Census Tables

| Table ID | Description |
|----------|-------------|
| B01001 | Sex by Age |
| B19013 | Median Household Income |
| B25001 | Housing Units |
| B01003 | Total Population |
| B02001 | Race |
| B25077 | Median Home Value |
| B23025 | Employment Status |
| B15003 | Educational Attainment |

See full list: https://api.census.gov/data/2021/acs/acs5/groups.html

## Data Tables

### Ingested Data Location

Data is stored in tables named: `{survey}_{year}_{table_id}`

Example: `acs5_2021_b01001`

### Table Schema

Each table includes:
- `id` - Primary key (auto-generated)
- `geo_name` - Geographic area name (e.g., "California")
- `geo_id` - Geographic identifier
- `state_fips` - State FIPS code
- Census variables as typed columns (INTEGER, NUMERIC, TEXT)

### Querying Data

```sql
-- View all data for a table
SELECT * FROM acs5_2021_b01001 LIMIT 10;

-- Total population by state
SELECT geo_name, b01001_001e as total_population
FROM acs5_2021_b01001
ORDER BY b01001_001e DESC;
```

### Dataset Registry

Metadata about all ingested datasets is stored in `dataset_registry`:

```sql
SELECT * FROM dataset_registry;
```

## Testing

### Run Unit Tests

Unit tests run without API keys or network:

```bash
pytest tests/ -m unit
```

### Run Integration Tests

Integration tests require API keys and network:

```bash
RUN_INTEGRATION_TESTS=true pytest tests/integration/
```

### Run All Tests

```bash
pytest tests/
```

## Docker Deployment

### Build and run with Docker Compose

```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# View logs
docker-compose logs -f api

# Stop services
docker-compose down
```

### Environment Variables in Docker

Create `.env` file with your configuration. Docker Compose will automatically load it.

## Troubleshooting

### Missing Census API Key

**Error:** `CENSUS_SURVEY_API_KEY is required for Census ingestion operations`

**Solution:** Add your Census API key to `.env`:
```bash
CENSUS_SURVEY_API_KEY=your_key_here
```

### Database Connection Error

**Error:** `could not connect to server: Connection refused`

**Solution:** 
1. Ensure PostgreSQL is running
2. Check DATABASE_URL in `.env`
3. Verify database exists: `createdb nexdata`

### Rate Limiting

If you see rate limit warnings in logs, adjust:

```bash
MAX_CONCURRENCY=2  # Lower concurrency
MAX_REQUESTS_PER_SECOND=2.0  # Lower rate
```

### Table Not Found

If a table doesn't exist after ingestion, check job status:

```bash
curl http://localhost:8001/api/v1/jobs/{job_id}
```

Look for `error_message` field if status is "failed".

## Support

For issues or questions:
1. Check logs: `docker-compose logs api` or application logs
2. Review job error messages via API
3. Verify configuration in `.env`
4. See `RULES.md` for architectural guidelines




