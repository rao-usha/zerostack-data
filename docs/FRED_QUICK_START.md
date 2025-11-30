# FRED (Federal Reserve Economic Data) Quick Start Guide

## Overview

The FRED adapter ingests Federal Reserve Economic Data into PostgreSQL via the official FRED API. FRED provides access to over 800,000 time series from various sources, including the Federal Reserve Banks, Census Bureau, BLS, BEA, and more.

## Key Features

✅ Official FRED API integration (no scraping)
✅ Bounded concurrency with semaphores
✅ Exponential backoff with jitter
✅ Rate limiting compliance
✅ Typed database columns (not JSON blobs)
✅ Job tracking for all ingestion runs
✅ Dataset registry integration
✅ Parameterized queries (SQL injection safe)

## API Key Setup (Optional but Recommended)

### Get a Free FRED API Key

1. Visit: https://fred.stlouisfed.org/docs/api/api_key.html
2. Request a free API key (instant approval)
3. Add to `.env` file:

```bash
FRED_API_KEY=your_fred_api_key_here
```

### Rate Limits

| Mode | Rate Limit | Notes |
|------|------------|-------|
| **Without API Key** | Limited, throttled | Works but slower |
| **With API Key** | 120 requests/minute | Recommended for production |

## Supported Categories

The FRED adapter includes pre-configured series for common economic indicators:

### 1. Interest Rates (H.15)
- **Federal Funds Rate** (`DFF`) - Daily effective rate
- **Treasury Rates** (`DGS3MO`, `DGS2`, `DGS5`, `DGS10`, `DGS30`)
- **Prime Rate** (`DPRIME`)

### 2. Monetary Aggregates
- **M1 Money Stock** (`M1SL`) - Seasonally adjusted
- **M2 Money Stock** (`M2SL`) - Seasonally adjusted
- **Monetary Base** (`BOGMBASE`)
- **Currency in Circulation** (`CURRCIR`)

### 3. Industrial Production
- **Total Index** (`INDPRO`)
- **Manufacturing** (`IPMAN`)
- **Mining** (`IPMINE`)
- **Utilities** (`IPU`)
- **Capacity Utilization** (`TCU`)

### 4. Economic Indicators
- **GDP** (`GDP`, `GDPC1`)
- **Unemployment Rate** (`UNRATE`)
- **CPI** (`CPIAUCSL`)
- **Personal Consumption Expenditures** (`PCE`)
- **Retail Sales** (`RSXFS`)

## API Endpoints

### 1. List Available Categories

```bash
GET /api/v1/fred/categories
```

**Response:**
```json
{
  "categories": [
    {
      "name": "interest_rates",
      "display_name": "Interest Rates (H.15)",
      "description": "Federal Reserve interest rates including Federal Funds Rate, Treasury rates, and Prime Rate from the H.15 statistical release",
      "series_count": 7
    },
    ...
  ]
}
```

### 2. Get Series for a Category

```bash
GET /api/v1/fred/series/{category}
```

**Example:**
```bash
GET /api/v1/fred/series/interest_rates
```

**Response:**
```json
{
  "category": "interest_rates",
  "series": [
    {
      "series_id": "DFF",
      "name": "Federal Funds Rate",
      "description": "FRED series DFF"
    },
    {
      "series_id": "DGS10",
      "name": "10Y Treasury",
      "description": "FRED series DGS10"
    },
    ...
  ]
}
```

### 3. Ingest a Single Category

```bash
POST /api/v1/fred/ingest
Content-Type: application/json

{
  "category": "interest_rates",
  "observation_start": "2020-01-01",
  "observation_end": "2023-12-31"
}
```

**Optional Parameters:**
- `series_ids`: List of specific series IDs (uses defaults if omitted)
- `observation_start`: Start date (defaults to 10 years ago)
- `observation_end`: End date (defaults to today)

**Response:**
```json
{
  "job_id": 42,
  "status": "pending",
  "message": "FRED ingestion job created",
  "check_status": "/api/v1/jobs/42"
}
```

### 4. Ingest Multiple Categories (Batch)

```bash
POST /api/v1/fred/ingest/batch
Content-Type: application/json

{
  "categories": [
    "interest_rates",
    "monetary_aggregates",
    "economic_indicators"
  ],
  "observation_start": "2020-01-01",
  "observation_end": "2023-12-31"
}
```

**Response:**
```json
{
  "job_ids": [42, 43, 44],
  "status": "pending",
  "message": "Created 3 FRED ingestion jobs",
  "categories": ["interest_rates", "monetary_aggregates", "economic_indicators"]
}
```

### 5. Check Job Status

```bash
GET /api/v1/jobs/{job_id}
```

**Response:**
```json
{
  "id": 42,
  "source": "fred",
  "status": "success",
  "rows_inserted": 2534,
  "started_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:32:15Z",
  "config": {
    "category": "interest_rates",
    "observation_start": "2020-01-01",
    "observation_end": "2023-12-31"
  }
}
```

## Database Schema

Each FRED category creates a table with the following schema:

```sql
CREATE TABLE fred_{category} (
    series_id TEXT NOT NULL,           -- FRED series ID (e.g., "DFF", "GDP")
    date DATE NOT NULL,                 -- Observation date
    value NUMERIC,                      -- Observation value
    realtime_start DATE,                -- Real-time period start
    realtime_end DATE,                  -- Real-time period end
    ingested_at TIMESTAMP DEFAULT NOW(), -- Ingestion timestamp
    PRIMARY KEY (series_id, date)
);

-- Indexes for efficient querying
CREATE INDEX idx_fred_{category}_date ON fred_{category} (date);
CREATE INDEX idx_fred_{category}_series_id ON fred_{category} (series_id);
```

## Usage Examples

### Example 1: Ingest Interest Rates (Last 5 Years)

```bash
curl -X POST http://localhost:8000/api/v1/fred/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "category": "interest_rates",
    "observation_start": "2019-01-01",
    "observation_end": "2024-01-01"
  }'
```

### Example 2: Ingest Specific Series Only

```bash
curl -X POST http://localhost:8000/api/v1/fred/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "category": "interest_rates",
    "series_ids": ["DFF", "DGS10"],
    "observation_start": "2020-01-01",
    "observation_end": "2024-01-01"
  }'
```

### Example 3: Ingest All Economic Data (Batch)

```bash
curl -X POST http://localhost:8000/api/v1/fred/ingest/batch \
  -H "Content-Type: application/json" \
  -d '{
    "categories": [
      "interest_rates",
      "monetary_aggregates",
      "industrial_production",
      "economic_indicators"
    ],
    "observation_start": "2020-01-01",
    "observation_end": "2024-01-01"
  }'
```

### Example 4: Query Ingested Data

```sql
-- Get Federal Funds Rate for 2023
SELECT date, value
FROM fred_interest_rates
WHERE series_id = 'DFF'
  AND date >= '2023-01-01'
  AND date < '2024-01-01'
ORDER BY date;

-- Get all interest rates for a specific date
SELECT series_id, value
FROM fred_interest_rates
WHERE date = '2023-12-31';

-- Calculate average M1 money stock by year
SELECT 
  EXTRACT(YEAR FROM date) AS year,
  AVG(value) AS avg_m1
FROM fred_monetary_aggregates
WHERE series_id = 'M1SL'
GROUP BY year
ORDER BY year;
```

## Programmatic Usage (Python)

```python
import httpx

# Start ingestion
response = httpx.post(
    "http://localhost:8000/api/v1/fred/ingest",
    json={
        "category": "interest_rates",
        "observation_start": "2020-01-01",
        "observation_end": "2023-12-31"
    }
)
job_id = response.json()["job_id"]

# Check status
import time
while True:
    status_response = httpx.get(
        f"http://localhost:8000/api/v1/jobs/{job_id}"
    )
    status = status_response.json()["status"]
    
    if status == "success":
        print("Ingestion complete!")
        break
    elif status == "failed":
        print("Ingestion failed:", status_response.json().get("error_message"))
        break
    else:
        print(f"Status: {status}")
        time.sleep(5)
```

## Rate Limiting Configuration

Configure rate limits in `.env`:

```bash
# Maximum concurrent requests to FRED API
MAX_CONCURRENCY=4

# Maximum requests per second
MAX_REQUESTS_PER_SECOND=5.0

# Retry configuration
MAX_RETRIES=3
RETRY_BACKOFF_FACTOR=2.0
```

## Architecture Details

### Bounded Concurrency

The FRED client uses `asyncio.Semaphore` to ensure bounded concurrency:

```python
# From app/sources/fred/client.py
self.semaphore = asyncio.Semaphore(max_concurrency)

async with self.semaphore:
    # Only max_concurrency requests run simultaneously
    response = await client.get(url, params=params)
```

### Exponential Backoff with Jitter

Retries use exponential backoff with random jitter:

```python
delay = min(base_delay * (backoff_factor ** attempt), max_delay)
jitter = delay * 0.25 * (2 * random.random() - 1)
delay_with_jitter = max(0.1, delay + jitter)
```

### Error Handling

- **429 Rate Limit:** Respects `Retry-After` header
- **5xx Server Errors:** Automatic retry with backoff
- **4xx Client Errors:** No retry (bad request)
- **Network Errors:** Retry with backoff

## Troubleshooting

### Issue: Rate Limiting Errors

**Solution:** Add FRED API key or reduce `MAX_CONCURRENCY`:

```bash
FRED_API_KEY=your_key_here
MAX_CONCURRENCY=2
```

### Issue: "No data to insert"

**Possible causes:**
- Date range has no data for the series
- Series may not have data for that period
- API returned empty response

**Solution:** Check FRED website for series availability dates

### Issue: Slow Ingestion

**Solutions:**
1. Add FRED API key (120 requests/min vs throttled)
2. Reduce date range
3. Ingest specific series instead of all defaults

### Issue: "Failed to fetch series"

**Check:**
1. Network connectivity
2. FRED API status
3. Series ID is valid
4. Date format is YYYY-MM-DD

## Best Practices

1. **Use API Key in Production** - Get higher rate limits
2. **Batch Similar Requests** - Use `/ingest/batch` for multiple categories
3. **Configure Conservative Rate Limits** - Start with `MAX_CONCURRENCY=2`
4. **Monitor Job Status** - Check job completion via `/jobs/{job_id}`
5. **Use Date Ranges Wisely** - FRED has data going back decades, be selective
6. **Update Regularly** - FRED data is updated daily
7. **Handle Missing Values** - FRED uses "." for missing data (automatically skipped)

## Data Refresh Strategy

FRED data is updated daily. Recommended refresh strategy:

```python
# Daily refresh of recent data (last 90 days)
from datetime import datetime, timedelta

end_date = datetime.now()
start_date = end_date - timedelta(days=90)

# Ingest will use ON CONFLICT UPDATE to refresh existing data
```

The ingestion uses `ON CONFLICT (series_id, date) DO UPDATE` so it's safe to re-run with overlapping date ranges.

## Related Documentation

- [FRED API Documentation](https://fred.stlouisfed.org/docs/api/fred/)
- [Project Rules](./RULES.md)
- [External Data Sources Checklist](./EXTERNAL_DATA_SOURCES.md)
- [Main README](./README.md)

## Support

For issues or questions:
1. Check FRED API status: https://fred.stlouisfed.org/
2. Review logs in `ingestion_jobs` table
3. Check job error messages via `/api/v1/jobs/{job_id}`
