# BLS (Bureau of Labor Statistics) Quick Start Guide

## Overview

The BLS integration provides access to Bureau of Labor Statistics data, including:

- **CPS** (Current Population Survey) - Unemployment rate, labor force participation
- **CES** (Current Employment Statistics) - Employment by industry, hourly earnings
- **JOLTS** (Job Openings and Labor Turnover Survey) - Job openings, hires, quits
- **CPI** (Consumer Price Index) - Inflation measures
- **PPI** (Producer Price Index) - Producer/wholesale prices
- **OES** (Occupational Employment Statistics) - Wages by occupation

## API Key (Recommended)

BLS API key is **optional but highly recommended**:

| | Without Key | With Key |
|---|---|---|
| Queries per day | 25 | 500 |
| Years per query | 10 | 20 |
| Series per query | 25 | 50 |

**Get a free API key:** https://data.bls.gov/registrationEngine/

### Configure API Key

Add to your `.env` file:

```bash
BLS_API_KEY=your_key_here
```

## Quick Start

### 1. Ingest Unemployment Data (CPS)

```bash
curl -X POST "http://localhost:8001/api/v1/bls/cps/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "start_year": 2020,
    "end_year": 2024
  }'
```

**Response:**
```json
{
  "job_id": 123,
  "status": "pending",
  "message": "BLS CPS ingestion job created",
  "series_count": 7,
  "year_range": "2020-2024",
  "check_status": "/api/v1/jobs/123"
}
```

### 2. Ingest CPI Inflation Data

```bash
curl -X POST "http://localhost:8001/api/v1/bls/cpi/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "start_year": 2020,
    "end_year": 2024
  }'
```

### 3. Ingest Employment Data (CES)

```bash
curl -X POST "http://localhost:8001/api/v1/bls/ces/ingest" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### 4. Ingest Custom Series

```bash
curl -X POST "http://localhost:8001/api/v1/bls/series/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "series_ids": ["LNS14000000", "CUUR0000SA0"],
    "start_year": 2020,
    "end_year": 2024,
    "dataset": "cps"
  }'
```

### 5. Ingest All Datasets

```bash
curl -X POST "http://localhost:8001/api/v1/bls/all/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "start_year": 2020,
    "end_year": 2024
  }'
```

## Reference Endpoints

### Get Available Datasets

```bash
curl "http://localhost:8001/api/v1/bls/reference/datasets"
```

### Get Common Series IDs

```bash
# All series
curl "http://localhost:8001/api/v1/bls/reference/series"

# By dataset
curl "http://localhost:8001/api/v1/bls/reference/series/cps"
curl "http://localhost:8001/api/v1/bls/reference/series/cpi"
```

### Quick Reference

```bash
curl "http://localhost:8001/api/v1/bls/reference/quick"
```

## Database Tables

| Dataset | Table Name | Description |
|---------|------------|-------------|
| CPS | `bls_cps_labor_force` | Unemployment, labor force participation |
| CES | `bls_ces_employment` | Employment by industry |
| JOLTS | `bls_jolts` | Job openings, hires, quits |
| CPI | `bls_cpi` | Consumer price inflation |
| PPI | `bls_ppi` | Producer prices |
| OES | `bls_oes` | Employment by occupation |

### Table Schema

All BLS tables share this schema:

```sql
CREATE TABLE bls_cps_labor_force (
    id SERIAL PRIMARY KEY,
    series_id TEXT NOT NULL,
    series_title TEXT,
    year INTEGER NOT NULL,
    period TEXT NOT NULL,           -- "M01" for January, "Q1" for Q1
    period_name TEXT,               -- "January"
    value NUMERIC(20, 6),
    footnote_codes TEXT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT bls_cps_labor_force_unique UNIQUE (series_id, year, period)
);
```

### Query Examples

```sql
-- Get unemployment rate (seasonally adjusted)
SELECT year, period, period_name, value 
FROM bls_cps_labor_force 
WHERE series_id = 'LNS14000000' 
ORDER BY year DESC, period DESC 
LIMIT 12;

-- Get CPI all items
SELECT year, period, value 
FROM bls_cpi 
WHERE series_id = 'CUUR0000SA0' 
ORDER BY year DESC, period DESC;

-- Get total nonfarm employment
SELECT year, period, value 
FROM bls_ces_employment 
WHERE series_id = 'CES0000000001' 
ORDER BY year DESC, period DESC;

-- Get job openings rate
SELECT year, period, value 
FROM bls_jolts 
WHERE series_id = 'JTS000000000000000JOR' 
ORDER BY year DESC, period DESC;
```

## Common Series IDs

### Unemployment & Labor Force (CPS)

| Series ID | Description |
|-----------|-------------|
| LNS14000000 | Unemployment Rate (seasonally adjusted) |
| LNS11300000 | Labor Force Participation Rate |
| LNS12000000 | Employment Level |
| LNS13000000 | Unemployment Level |
| LNS12300000 | Employment-Population Ratio |
| LNS13327709 | U-6 Unemployment (underemployment) |

### Employment (CES)

| Series ID | Description |
|-----------|-------------|
| CES0000000001 | Total Nonfarm Employment |
| CES0500000001 | Total Private Employment |
| CES3000000001 | Manufacturing Employment |
| CES2000000001 | Construction Employment |
| CES0500000003 | Average Hourly Earnings (Private) |
| CES0500000002 | Average Weekly Hours (Private) |

### Inflation (CPI)

| Series ID | Description |
|-----------|-------------|
| CUUR0000SA0 | CPI-U All Items |
| CUUR0000SA0L1E | Core CPI (less food and energy) |
| CUUR0000SAF1 | CPI Food |
| CUUR0000SA0E | CPI Energy |
| CUUR0000SAH1 | CPI Shelter |
| CUSR0000SA0 | CPI-U All Items (seasonally adjusted) |

### Job Market (JOLTS)

| Series ID | Description |
|-----------|-------------|
| JTS000000000000000JOL | Job Openings Level |
| JTS000000000000000JOR | Job Openings Rate |
| JTS000000000000000QUL | Quits Level |
| JTS000000000000000QUR | Quits Rate |
| JTS000000000000000HIL | Hires Level |
| JTS000000000000000LDL | Layoffs Level |

### Producer Prices (PPI)

| Series ID | Description |
|-----------|-------------|
| WPSFD4 | PPI Final Demand |
| WPSFD41 | PPI Final Demand Goods |
| WPSFD42 | PPI Final Demand Services |
| WPSID61 | PPI Intermediate Demand |

## Python Usage

```python
from app.sources.bls import (
    BLSClient,
    get_series_for_dataset,
    ingest_bls_dataset,
)

# Initialize client
client = BLSClient(api_key="your_key_here")

# Fetch series directly
result = await client.fetch_series(
    series_ids=["LNS14000000", "CUUR0000SA0"],
    start_year=2020,
    end_year=2024
)

# Get default series for a dataset
cps_series = get_series_for_dataset("cps")
print(cps_series)  # ['LNS14000000', 'LNS11300000', ...]

# Close client
await client.close()
```

## Rate Limits & Best Practices

1. **Use an API key** - Get one free at https://data.bls.gov/registrationEngine/
2. **Batch requests** - Request multiple series in one call (up to 50 with key)
3. **Respect limits** - The client implements automatic rate limiting
4. **Use defaults** - Let the system determine optimal year ranges
5. **Monitor jobs** - Check job status for completion

## Troubleshooting

### "Year range too large"
- Without API key: max 10 years
- With API key: max 20 years
- Solution: Use shorter date ranges or add API key

### "Too many series"
- Without API key: max 25 series per request
- With API key: max 50 series per request
- Solution: The client auto-batches large requests

### Rate Limited
- BLS enforces daily query limits
- Solution: Add API key for 500 queries/day

### Invalid Series ID
- BLS returns empty data for unknown series
- Solution: Use reference endpoints to find valid series IDs

## Additional Resources

- [BLS Data Finder](https://www.bls.gov/data/)
- [BLS API Documentation](https://www.bls.gov/developers/)
- [Series ID Formats](https://www.bls.gov/help/hlpforma.htm)
- [Register for API Key](https://data.bls.gov/registrationEngine/)
