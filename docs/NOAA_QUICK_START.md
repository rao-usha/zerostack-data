# NOAA Weather & Climate Data - Quick Start Guide

This guide covers ingestion of NOAA climate and weather data using the External Data Ingestion Service.

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Available Datasets](#available-datasets)
4. [Quick Start Examples](#quick-start-examples)
5. [API Endpoints](#api-endpoints)
6. [Data Types](#data-types)
7. [Location and Station Filters](#location-and-station-filters)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

## Overview

**Data Source:** NOAA National Centers for Environmental Information (NCEI)  
**API:** Climate Data Online (CDO) Web Services v2  
**Official Documentation:** https://www.ncdc.noaa.gov/cdo-web/webservices/v2  
**License:** Public Domain (U.S. Government Work)  
**Rate Limits:** 5 requests/second, 10,000 requests/day

### What's Available

This adapter provides access to:
- **Daily Weather Observations** - Temperature, precipitation, wind, snow from weather stations
- **Climate Normals** - 30-year climate averages (daily, monthly)
- **Monthly Summaries** - Aggregated monthly climate data
- **Hourly Precipitation** - High-resolution precipitation data

## Prerequisites

### 1. Get a NOAA CDO API Token (Free)

1. Visit: https://www.ncdc.noaa.gov/cdo-web/token
2. Enter your email address
3. Check your email for the token (arrives immediately)
4. Save the token securely

**Important:** The token is required for ALL requests to NOAA CDO API.

### 2. Database Setup

Ensure your PostgreSQL database is running and accessible:

```bash
# Check docker-compose is running
docker-compose up -d

# The service will automatically create necessary tables
```

### 3. Start the Service

```bash
# Activate virtual environment
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Start FastAPI service
uvicorn app.main:app --reload
```

Access the API documentation at: http://localhost:8000/docs

## Available Datasets

| Dataset Key | NOAA Dataset ID | Description | Update Frequency |
|-------------|-----------------|-------------|------------------|
| `ghcnd_daily` | GHCND | Daily weather observations (temp, precip, wind, snow) | Daily |
| `normal_daily` | NORMAL_DLY | 30-year daily climate normals | Every 10 years |
| `normal_monthly` | NORMAL_MLY | 30-year monthly climate normals | Every 10 years |
| `gsom` | GSOM | Monthly climate summaries | Monthly |
| `precip_hourly` | PRECIP_HLY | Hourly precipitation data | Hourly |

### Get List of Available Datasets

```bash
curl http://localhost:8000/api/v1/noaa/datasets
```

## Quick Start Examples

### Example 1: Ingest Daily Weather for California (January 2024)

```bash
curl -X POST http://localhost:8000/api/v1/noaa/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_NOAA_TOKEN",
    "dataset_key": "ghcnd_daily",
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "location_id": "FIPS:06",
    "data_type_ids": ["TMAX", "TMIN", "PRCP"],
    "max_concurrency": 3,
    "requests_per_second": 4.0
  }'
```

**Response:**
```json
{
  "job_id": 123,
  "status": "success",
  "dataset_key": "ghcnd_daily",
  "rows_fetched": 15234,
  "rows_inserted": 15234,
  "table_name": "noaa_ghcnd_daily",
  "message": "Successfully ingested 15234 rows"
}
```

### Example 2: Ingest Data for Specific Weather Station

```bash
curl -X POST http://localhost:8000/api/v1/noaa/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_NOAA_TOKEN",
    "dataset_key": "ghcnd_daily",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "station_id": "GHCND:USW00023174",
    "data_type_ids": ["TMAX", "TMIN", "TAVG", "PRCP"],
    "max_results": 5000
  }'
```

### Example 3: Ingest Climate Normals

```bash
curl -X POST http://localhost:8000/api/v1/noaa/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_NOAA_TOKEN",
    "dataset_key": "normal_daily",
    "start_date": "2010-01-01",
    "end_date": "2010-12-31",
    "location_id": "FIPS:06"
  }'
```

### Example 4: Large Date Range with Chunking

For large date ranges, use chunking to avoid API limits:

```bash
curl -X POST http://localhost:8000/api/v1/noaa/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "token": "YOUR_NOAA_TOKEN",
    "dataset_key": "ghcnd_daily",
    "start_date": "2020-01-01",
    "end_date": "2024-12-31",
    "location_id": "FIPS:36",
    "data_type_ids": ["TMAX", "TMIN", "PRCP"],
    "use_chunking": true,
    "chunk_days": 30
  }'
```

## API Endpoints

### Data Ingestion

**POST** `/api/v1/noaa/ingest`

Trigger NOAA data ingestion.

**Request Body:**
- `token` (required): NOAA CDO API token
- `dataset_key` (required): Dataset to ingest (see Available Datasets)
- `start_date` (required): Start date (YYYY-MM-DD)
- `end_date` (required): End date (YYYY-MM-DD)
- `location_id` (optional): Filter by location (e.g., "FIPS:06")
- `station_id` (optional): Filter by station (e.g., "GHCND:USW00023174")
- `data_type_ids` (optional): List of data types (e.g., ["TMAX", "TMIN"])
- `max_results` (optional): Maximum total results
- `max_concurrency` (optional): Concurrent requests (default: 3)
- `requests_per_second` (optional): Rate limit (default: 4.0)
- `use_chunking` (optional): Split into chunks (default: false)
- `chunk_days` (optional): Days per chunk (default: 30)

### Dataset Information

**GET** `/api/v1/noaa/datasets`

List all available NOAA datasets.

**GET** `/api/v1/noaa/datasets/{dataset_key}`

Get detailed information about a specific dataset.

### Discovery Endpoints

**GET** `/api/v1/noaa/locations`

Get available locations (states, cities, ZIP codes).

**Query Parameters:**
- `token` (required): NOAA token
- `dataset_id` (optional): Dataset ID (default: "GHCND")
- `location_category_id` (optional): Category filter (e.g., "ST" for states)
- `limit` (optional): Max results (default: 100, max: 1000)

**Example:**
```bash
# Get all US states
curl "http://localhost:8000/api/v1/noaa/locations?token=YOUR_TOKEN&dataset_id=GHCND&location_category_id=ST&limit=100"
```

**GET** `/api/v1/noaa/stations`

Get available weather stations.

**Query Parameters:**
- `token` (required): NOAA token
- `dataset_id` (optional): Dataset ID (default: "GHCND")
- `location_id` (optional): Location filter (e.g., "FIPS:06")
- `limit` (optional): Max results (default: 100, max: 1000)

**Example:**
```bash
# Get California weather stations
curl "http://localhost:8000/api/v1/noaa/stations?token=YOUR_TOKEN&dataset_id=GHCND&location_id=FIPS:06&limit=100"
```

**GET** `/api/v1/noaa/data-types`

Get available data types for a dataset.

**Query Parameters:**
- `token` (required): NOAA token
- `dataset_id` (optional): Dataset ID (default: "GHCND")
- `limit` (optional): Max results (default: 100, max: 1000)

**Example:**
```bash
# Get GHCND data types
curl "http://localhost:8000/api/v1/noaa/data-types?token=YOUR_TOKEN&dataset_id=GHCND"
```

### Job Status

**GET** `/api/v1/jobs/{job_id}`

Check status of an ingestion job.

## Data Types

### Common Weather Observations (GHCND)

| Data Type ID | Description | Units (Standard/Metric) |
|--------------|-------------|-------------------------|
| `TMAX` | Maximum temperature | °F / °C |
| `TMIN` | Minimum temperature | °F / °C |
| `TAVG` | Average temperature | °F / °C |
| `PRCP` | Precipitation | inches / mm |
| `SNOW` | Snowfall | inches / mm |
| `SNWD` | Snow depth | inches / mm |
| `AWND` | Average wind speed | mph / m/s |
| `WSF2` | Fastest 2-minute wind | mph / m/s |
| `WSF5` | Fastest 5-second wind | mph / m/s |

### Climate Normals

| Data Type ID | Description |
|--------------|-------------|
| `DLY-TMAX-NORMAL` | Daily maximum temperature normal |
| `DLY-TMIN-NORMAL` | Daily minimum temperature normal |
| `DLY-TAVG-NORMAL` | Daily average temperature normal |
| `DLY-PRCP-PCTALL-GE001HI` | Probability of ≥0.01" precipitation |
| `MLY-TMAX-NORMAL` | Monthly maximum temperature normal |
| `MLY-TMIN-NORMAL` | Monthly minimum temperature normal |
| `MLY-PRCP-NORMAL` | Monthly precipitation normal |

## Location and Station Filters

### Location ID Formats

**FIPS Codes (States and Counties):**
- State: `FIPS:06` (California)
- State: `FIPS:36` (New York)
- County: `FIPS:06037` (Los Angeles County, CA)

**ZIP Codes:**
- `ZIP:10001` (New York, NY)
- `ZIP:90210` (Beverly Hills, CA)

**Cities:**
- `CITY:US530007` (San Francisco, CA)
- `CITY:US360019` (New York, NY)

**Location Categories:**
- `CITY` - Cities
- `ST` - States  
- `CNTRY` - Countries
- `ZIP` - ZIP codes

### Station ID Format

**Format:** `GHCND:STATION_CODE`

**Examples:**
- `GHCND:USW00023174` - Los Angeles International Airport (LAX)
- `GHCND:USW00094728` - New York Central Park
- `GHCND:USW00013874` - Denver International Airport

**Finding Stations:**

```bash
# Find stations in California
curl "http://localhost:8000/api/v1/noaa/stations?token=YOUR_TOKEN&location_id=FIPS:06&limit=100"
```

## Best Practices

### 1. Rate Limiting

**NOAA CDO API Limits:**
- 5 requests per second
- 10,000 requests per day

**Recommended Settings:**
```json
{
  "max_concurrency": 3,
  "requests_per_second": 4.0
}
```

These settings keep you safely under the limit.

### 2. Date Range Handling

**For date ranges > 1 year:** Use chunking to avoid timeouts and API limits.

```json
{
  "use_chunking": true,
  "chunk_days": 30
}
```

**For date ranges < 1 month:** Direct ingestion works fine.

### 3. Location vs Station Filtering

- **Location filtering** (`location_id`): Gets data from all stations in an area
- **Station filtering** (`station_id`): Gets data from a specific weather station

**Use location filtering when:**
- You want comprehensive coverage of an area
- You're analyzing regional patterns

**Use station filtering when:**
- You need data from a specific, known station
- You want consistent measurements from one location

### 4. Data Type Selection

**Always specify data types** to reduce data volume and improve performance:

```json
{
  "data_type_ids": ["TMAX", "TMIN", "PRCP"]
}
```

If omitted, the service uses the dataset's default data types.

### 5. Result Limiting

For exploratory queries, limit results:

```json
{
  "max_results": 1000
}
```

This prevents accidental over-ingestion.

## Troubleshooting

### Error: "Invalid token"

**Cause:** Token is missing, expired, or incorrect.

**Solution:**
1. Get a new token from https://www.ncdc.noaa.gov/cdo-web/token
2. Verify token is correct (no extra spaces)
3. Tokens don't expire, but check email for the original

### Error: Rate limit exceeded (429)

**Cause:** Exceeded 5 requests/second or 10,000 requests/day.

**Solution:**
- The service automatically retries with backoff
- Reduce `requests_per_second` to 3.0 or lower
- Reduce `max_concurrency` to 2
- Wait and retry if daily limit reached

### Error: No data returned

**Possible Causes:**
1. Date range has no data for the location/station
2. Data types not available for the dataset
3. Station/location ID is invalid

**Solutions:**
1. Check station has data for date range using `/noaa/stations`
2. Verify data types using `/noaa/data-types`
3. Verify location/station ID using `/noaa/locations` or `/noaa/stations`

### Slow Ingestion

**Causes:**
- Large date range
- Many weather stations in location
- High data type count

**Solutions:**
1. Use chunking: `"use_chunking": true`
2. Filter by specific station instead of location
3. Reduce data types to only what you need
4. Reduce max_concurrency to avoid rate limiting

### Database Connection Errors

**Solution:**
```bash
# Restart PostgreSQL
docker-compose restart postgres

# Check database is accessible
docker-compose logs postgres
```

## Database Schema

### Table: `noaa_ghcnd_daily` (and similar for other datasets)

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Observation date |
| `datatype` | VARCHAR(50) | Data type (TMAX, TMIN, etc.) |
| `station` | VARCHAR(50) | Weather station ID |
| `value` | NUMERIC | Measured value |
| `attributes` | VARCHAR(10) | Quality flags |
| `location_id` | VARCHAR(50) | Location identifier |
| `location_name` | TEXT | Location name |
| `latitude` | NUMERIC | Station latitude |
| `longitude` | NUMERIC | Station longitude |
| `elevation` | NUMERIC | Station elevation |
| `ingestion_timestamp` | TIMESTAMP | When data was ingested |

**Primary Key:** `(date, datatype, station)`

**Indexes:**
- `idx_*_date` - For time-series queries
- `idx_*_station` - For station-specific queries
- `idx_*_datatype` - For data type queries
- `idx_*_location` - For location queries
- `idx_*_date_type_station` - Composite index for common queries

### Example Queries

```sql
-- Get daily temperatures for a station
SELECT date, datatype, value
FROM noaa_ghcnd_daily
WHERE station = 'GHCND:USW00023174'
  AND datatype IN ('TMAX', 'TMIN')
  AND date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY date, datatype;

-- Get average monthly precipitation by location
SELECT 
  location_id,
  DATE_TRUNC('month', date) as month,
  AVG(value) as avg_precipitation
FROM noaa_ghcnd_daily
WHERE datatype = 'PRCP'
  AND date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY location_id, DATE_TRUNC('month', date)
ORDER BY location_id, month;
```

## Python Client Example

```python
import requests
from datetime import date

# Configuration
BASE_URL = "http://localhost:8000/api/v1"
NOAA_TOKEN = "YOUR_NOAA_TOKEN"

# Ingest daily weather data
response = requests.post(
    f"{BASE_URL}/noaa/ingest",
    json={
        "token": NOAA_TOKEN,
        "dataset_key": "ghcnd_daily",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "location_id": "FIPS:06",
        "data_type_ids": ["TMAX", "TMIN", "PRCP"],
        "max_concurrency": 3
    }
)

result = response.json()
print(f"Job ID: {result['job_id']}")
print(f"Rows inserted: {result['rows_inserted']}")
print(f"Table: {result['table_name']}")

# Check job status
job_id = result['job_id']
status_response = requests.get(f"{BASE_URL}/jobs/{job_id}")
print(status_response.json())

# Get available stations
stations_response = requests.get(
    f"{BASE_URL}/noaa/stations",
    params={
        "token": NOAA_TOKEN,
        "dataset_id": "GHCND",
        "location_id": "FIPS:06",
        "limit": 10
    }
)

stations = stations_response.json()
print(f"Found {stations['count']} stations")
for station in stations['stations'][:5]:
    print(f"- {station['name']} ({station['id']})")
```

## Additional Resources

### Official NOAA Documentation
- **CDO API Documentation:** https://www.ncdc.noaa.gov/cdo-web/webservices/v2
- **Dataset Catalog:** https://www.ncdc.noaa.gov/cdo-web/datasets
- **Data Types:** https://www.ncdc.noaa.gov/cdo-web/datatypes
- **Get API Token:** https://www.ncdc.noaa.gov/cdo-web/token

### NOAA Data Centers
- **NCEI Home:** https://www.ncei.noaa.gov/
- **Climate Data Online:** https://www.ncdc.noaa.gov/cdo-web/
- **Weather Station Search:** https://www.ncdc.noaa.gov/cdo-web/search

### Understanding Climate Data
- **Climate Normals Explained:** https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals
- **GHCND Documentation:** https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily

## Support and Feedback

For issues or questions:
1. Check this documentation
2. Review API documentation at `/docs`
3. Check ingestion job logs in `ingestion_jobs` table
4. Review NOAA CDO API official documentation

## License and Attribution

**NOAA Data:** Public Domain (U.S. Government Work)

**Attribution (Recommended):**
> Data provided by NOAA National Centers for Environmental Information (NCEI)

**Terms of Use:** https://www.noaa.gov/information-technology/open-data-dissemination



