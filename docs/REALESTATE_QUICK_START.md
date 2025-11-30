# Real Estate / Housing Data - Quick Start Guide

This guide covers ingesting real estate and housing data from four public data sources:

1. **FHFA House Price Index** - Federal Housing Finance Agency quarterly home price indices
2. **HUD Permits & Starts** - HUD monthly building permits and housing starts data
3. **Redfin Housing Market Data** - Redfin weekly housing market metrics
4. **OpenStreetMap Building Footprints** - OSM building location and metadata

## Table of Contents

- [Overview](#overview)
- [Data Sources](#data-sources)
- [Prerequisites](#prerequisites)
- [Quick Start Examples](#quick-start-examples)
- [API Endpoints](#api-endpoints)
- [Database Schema](#database-schema)
- [Common Use Cases](#common-use-cases)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

The real estate module provides ingestion capabilities for multiple housing and real estate data sources. All sources use **official APIs or bulk download endpoints** - no web scraping.

**Key Features:**
- ✅ Multiple geographic levels (National, State, MSA, ZIP, etc.)
- ✅ Time series data for market analysis
- ✅ Typed database schemas (no JSON blobs)
- ✅ Rate-limited API clients with retry logic
- ✅ Job tracking for all ingestion runs
- ✅ Idempotent table creation and data updates

## Data Sources

### 1. FHFA House Price Index

**Provider:** Federal Housing Finance Agency  
**Update Frequency:** Quarterly  
**Geographic Levels:** National, State, MSA, ZIP3  
**API Key:** Not required  
**License:** Public domain

**What it provides:**
- Quarterly house price indices
- Year-over-year and quarter-over-quarter percent changes
- Both seasonally adjusted and non-adjusted indices
- Coverage: All 50 states + DC, 400+ MSAs, ZIP3 codes

### 2. HUD Building Permits & Housing Starts

**Provider:** U.S. Department of Housing and Urban Development  
**Update Frequency:** Monthly  
**Geographic Levels:** National, State, MSA, County  
**API Key:** Not required  
**License:** Public domain

**What it provides:**
- Building permits issued (by unit type)
- Housing starts (by unit type)
- Housing completions (by unit type)
- Unit types: Single-family, 2-4 units, 5+ units

### 3. Redfin Housing Market Data

**Provider:** Redfin  
**Update Frequency:** Weekly  
**Geographic Levels:** ZIP, City, Neighborhood, Metro  
**API Key:** Not required  
**License:** Public domain / open data

**What it provides:**
- Median sale and list prices
- Homes sold, pending sales, new listings
- Inventory levels and months of supply
- Days on market
- Sale-to-list ratios
- Price drops and other market indicators

### 4. OpenStreetMap Building Footprints

**Provider:** OpenStreetMap  
**Update Frequency:** Real-time  
**Geographic Scope:** Global (query by bounding box)  
**API Key:** Not required  
**License:** ODbL (Open Database License)

**What it provides:**
- Building locations (lat/lon)
- Building types (residential, commercial, etc.)
- Building heights and number of floors
- Address information
- Building footprint geometry

## Prerequisites

1. **Service Running:**
   ```bash
   docker-compose up -d
   ```

2. **Environment Variables:**
   ```bash
   # .env file
   DATABASE_URL=postgresql://user:pass@localhost:5432/nexdata
   MAX_CONCURRENCY=2  # Conservative for public APIs
   MAX_RETRIES=3
   RETRY_BACKOFF_FACTOR=2.0
   ```

3. **Check Service Health:**
   ```bash
   curl http://localhost:8000/health
   ```

## Quick Start Examples

### Example 1: Ingest FHFA National House Price Index

**Goal:** Get quarterly national house price indices for the last 5 years.

```bash
curl -X POST "http://localhost:8000/api/v1/realestate/fhfa/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "geography_type": "National",
    "start_date": "2019-01-01",
    "end_date": "2024-12-31"
  }'
```

**Response:**
```json
{
  "job_id": 123,
  "status": "pending",
  "message": "FHFA HPI ingestion started",
  "source": "fhfa_hpi"
}
```

**Check Status:**
```bash
curl http://localhost:8000/api/v1/realestate/fhfa/status/123
```

**Query Data:**
```sql
SELECT 
    date,
    geography_name,
    index_nsa,
    index_sa,
    yoy_pct_change,
    qoq_pct_change
FROM realestate_fhfa_hpi
WHERE geography_type = 'National'
ORDER BY date DESC
LIMIT 20;
```

### Example 2: Ingest HUD State-Level Permits & Starts

**Goal:** Get building permits and housing starts for California.

```bash
curl -X POST "http://localhost:8000/api/v1/realestate/hud/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "geography_type": "State",
    "geography_id": "06",
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
  }'
```

**Query Data:**
```sql
SELECT 
    date,
    geography_name,
    permits_total,
    permits_1unit,
    starts_total,
    completions_total
FROM realestate_hud_permits
WHERE geography_type = 'State' 
  AND geography_id = '06'
ORDER BY date DESC;
```

### Example 3: Ingest Redfin Metro-Level Data

**Goal:** Get housing market metrics for all metro areas.

```bash
curl -X POST "http://localhost:8000/api/v1/realestate/redfin/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "region_type": "metro",
    "property_type": "All Residential"
  }'
```

**Query Data:**
```sql
SELECT 
    period_end,
    region,
    state_code,
    median_sale_price,
    homes_sold,
    inventory,
    median_dom,
    months_of_supply
FROM realestate_redfin
WHERE region_type = 'metro'
  AND state_code = 'CA'
ORDER BY period_end DESC, median_sale_price DESC
LIMIT 20;
```

### Example 4: Ingest OpenStreetMap Buildings

**Goal:** Get residential building footprints in San Francisco.

```bash
curl -X POST "http://localhost:8000/api/v1/realestate/osm/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "bounding_box": [37.7, -122.5, 37.8, -122.4],
    "building_type": "residential",
    "limit": 10000
  }'
```

**Important:** Keep bounding boxes small to avoid Overpass API timeouts.

**Query Data:**
```sql
SELECT 
    osm_id,
    latitude,
    longitude,
    building_type,
    levels,
    height,
    address,
    city,
    postcode
FROM realestate_osm_buildings
WHERE building_type = 'residential'
  AND city = 'San Francisco'
LIMIT 100;
```

## API Endpoints

### FHFA House Price Index

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/realestate/fhfa/ingest` | POST | Start FHFA ingestion job |
| `/api/v1/realestate/fhfa/status/{job_id}` | GET | Check job status |

### HUD Permits & Starts

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/realestate/hud/ingest` | POST | Start HUD ingestion job |
| `/api/v1/realestate/hud/status/{job_id}` | GET | Check job status |

### Redfin Data

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/realestate/redfin/ingest` | POST | Start Redfin ingestion job |
| `/api/v1/realestate/redfin/status/{job_id}` | GET | Check job status |

### OpenStreetMap Buildings

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/realestate/osm/ingest` | POST | Start OSM ingestion job |
| `/api/v1/realestate/osm/status/{job_id}` | GET | Check job status |

### General

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/realestate/info` | GET | Get metadata about all real estate sources |

## Database Schema

### Table: `realestate_fhfa_hpi`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `date` | DATE | Quarter end date |
| `geography_type` | TEXT | National, State, MSA, ZIP3 |
| `geography_id` | TEXT | Geography identifier |
| `geography_name` | TEXT | Human-readable name |
| `index_nsa` | NUMERIC | Not seasonally adjusted index |
| `index_sa` | NUMERIC | Seasonally adjusted index |
| `yoy_pct_change` | NUMERIC | Year-over-year % change |
| `qoq_pct_change` | NUMERIC | Quarter-over-quarter % change |
| `ingested_at` | TIMESTAMP | Ingestion timestamp |

**Unique constraint:** `(date, geography_type, geography_id)`

### Table: `realestate_hud_permits`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `date` | DATE | Month |
| `geography_type` | TEXT | National, State, MSA, County |
| `geography_id` | TEXT | Geography identifier |
| `geography_name` | TEXT | Human-readable name |
| `permits_total` | INTEGER | Total building permits |
| `permits_1unit` | INTEGER | Single-family permits |
| `permits_2to4units` | INTEGER | 2-4 unit permits |
| `permits_5plus` | INTEGER | 5+ unit permits |
| `starts_total` | INTEGER | Total housing starts |
| `starts_1unit` | INTEGER | Single-family starts |
| `starts_2to4units` | INTEGER | 2-4 unit starts |
| `starts_5plus` | INTEGER | 5+ unit starts |
| `completions_total` | INTEGER | Total completions |
| `completions_1unit` | INTEGER | Single-family completions |
| `completions_2to4units` | INTEGER | 2-4 unit completions |
| `completions_5plus` | INTEGER | 5+ unit completions |
| `ingested_at` | TIMESTAMP | Ingestion timestamp |

**Unique constraint:** `(date, geography_type, geography_id)`

### Table: `realestate_redfin`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `period_end` | DATE | Week ending date |
| `region_type` | TEXT | zip, city, neighborhood, metro |
| `region_type_id` | INTEGER | Region identifier |
| `region` | TEXT | Region name |
| `state_code` | TEXT | State abbreviation |
| `property_type` | TEXT | Property type |
| `median_sale_price` | NUMERIC | Median sale price |
| `median_list_price` | NUMERIC | Median list price |
| `median_ppsf` | NUMERIC | Median price per sq ft |
| `homes_sold` | INTEGER | Number of homes sold |
| `pending_sales` | INTEGER | Pending sales |
| `new_listings` | INTEGER | New listings |
| `inventory` | INTEGER | Inventory count |
| `months_of_supply` | NUMERIC | Months of supply |
| `median_dom` | INTEGER | Median days on market |
| `avg_sale_to_list` | NUMERIC | Sale-to-list ratio |
| `sold_above_list` | INTEGER | Sold above list count |
| `price_drops` | INTEGER | Price drops |
| `off_market_in_two_weeks` | INTEGER | Off market quickly count |
| `ingested_at` | TIMESTAMP | Ingestion timestamp |

**Unique constraint:** `(period_end, region_type, region_type_id, property_type)`

### Table: `realestate_osm_buildings`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `osm_id` | BIGINT | OpenStreetMap ID |
| `osm_type` | TEXT | way or relation |
| `latitude` | NUMERIC | Latitude |
| `longitude` | NUMERIC | Longitude |
| `building_type` | TEXT | Building type |
| `levels` | INTEGER | Number of floors |
| `height` | NUMERIC | Height in meters |
| `area_sqm` | NUMERIC | Area in sq meters |
| `address` | TEXT | Street address |
| `city` | TEXT | City |
| `state` | TEXT | State |
| `postcode` | TEXT | Postal code |
| `country` | TEXT | Country |
| `name` | TEXT | Building name |
| `tags` | JSONB | Additional OSM tags |
| `geometry_geojson` | JSONB | GeoJSON geometry |
| `ingested_at` | TIMESTAMP | Ingestion timestamp |

**Unique constraint:** `(osm_id, osm_type)`

## Common Use Cases

### Use Case 1: Track Housing Market Trends

Combine FHFA house price index with Redfin market data:

```sql
-- Compare house price index growth with inventory levels
SELECT 
    f.date,
    f.geography_name,
    f.yoy_pct_change as hpi_yoy_change,
    AVG(r.median_sale_price) as avg_sale_price,
    AVG(r.inventory) as avg_inventory,
    AVG(r.months_of_supply) as avg_supply
FROM realestate_fhfa_hpi f
JOIN realestate_redfin r 
    ON f.geography_name = r.region
    AND DATE_TRUNC('quarter', r.period_end) = f.date
WHERE f.geography_type = 'MSA'
    AND r.region_type = 'metro'
    AND f.date >= '2020-01-01'
GROUP BY f.date, f.geography_name, f.yoy_pct_change
ORDER BY f.date DESC, f.yoy_pct_change DESC;
```

### Use Case 2: Monitor Construction Activity

Track building permits and housing starts:

```sql
-- Year-over-year change in permits by state
WITH current_permits AS (
    SELECT 
        geography_name,
        SUM(permits_total) as total_permits
    FROM realestate_hud_permits
    WHERE date >= DATE_TRUNC('year', CURRENT_DATE)
        AND geography_type = 'State'
    GROUP BY geography_name
),
prior_permits AS (
    SELECT 
        geography_name,
        SUM(permits_total) as total_permits
    FROM realestate_hud_permits
    WHERE date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'
        AND date < DATE_TRUNC('year', CURRENT_DATE)
        AND geography_type = 'State'
    GROUP BY geography_name
)
SELECT 
    c.geography_name,
    c.total_permits as current_year,
    p.total_permits as prior_year,
    ((c.total_permits - p.total_permits)::float / p.total_permits * 100) as yoy_pct_change
FROM current_permits c
JOIN prior_permits p ON c.geography_name = p.geography_name
ORDER BY yoy_pct_change DESC;
```

### Use Case 3: Analyze Building Density

Use OSM building footprints to analyze urban density:

```sql
-- Count buildings by type within postal code
SELECT 
    postcode,
    city,
    building_type,
    COUNT(*) as building_count,
    AVG(levels) as avg_levels,
    AVG(height) as avg_height_meters,
    SUM(area_sqm) as total_area_sqm
FROM realestate_osm_buildings
WHERE city = 'San Francisco'
    AND postcode IS NOT NULL
GROUP BY postcode, city, building_type
ORDER BY building_count DESC;
```

### Use Case 4: Identify Hot Markets

Find markets with high price growth and low inventory:

```sql
-- Recent trends by metro area
SELECT 
    region,
    state_code,
    AVG(median_sale_price) as avg_price,
    AVG(months_of_supply) as avg_supply,
    AVG(median_dom) as avg_days_on_market,
    SUM(homes_sold) as total_sold
FROM realestate_redfin
WHERE region_type = 'metro'
    AND period_end >= CURRENT_DATE - INTERVAL '3 months'
    AND property_type = 'All Residential'
GROUP BY region, state_code
HAVING AVG(months_of_supply) < 3  -- Tight inventory
ORDER BY avg_price DESC
LIMIT 20;
```

## Best Practices

### 1. Rate Limiting & Concurrency

**FHFA & Redfin:**
- No documented rate limits
- Default: 2 concurrent requests
- Safe for production use

**HUD:**
- No documented rate limits
- Default: 2 concurrent requests
- Monitor for 429 responses

**OpenStreetMap:**
- Public Overpass API has rate limits
- **Use MAX 1 concurrent request**
- Queries timeout after 180 seconds
- **Keep bounding boxes small** (< 0.1 degrees)

### 2. Data Refresh Strategy

| Source | Update Frequency | Recommended Refresh |
|--------|------------------|---------------------|
| FHFA HPI | Quarterly | Monthly (to catch revisions) |
| HUD Permits | Monthly | Monthly |
| Redfin | Weekly | Weekly |
| OSM Buildings | Real-time | On-demand only |

### 3. Geographic Coverage

**Start broad, then narrow:**
1. National-level data first
2. State-level for targeted analysis
3. MSA/County for metro analysis
4. ZIP/City for granular analysis

### 4. Query Optimization

**Always use indexes:**
```sql
-- Queries are optimized when using indexed columns:
WHERE date >= '2023-01-01'  -- Indexed
WHERE geography_type = 'State'  -- Indexed
WHERE region_type = 'metro' AND state_code = 'CA'  -- Indexed
```

### 5. OSM Best Practices

**Keep bounding boxes small:**
```python
# Good: Small area
bbox = [37.75, -122.45, 37.80, -122.40]  # ~5km x 5km

# Bad: Large area (will timeout)
bbox = [37.0, -123.0, 38.0, -122.0]  # ~100km x 100km
```

**Use building type filters:**
```json
{
  "building_type": "residential",  // Reduces result size
  "limit": 10000
}
```

## Troubleshooting

### Issue: FHFA ingestion returns no data

**Possible causes:**
- Date filters too restrictive
- Geography type filter too specific
- Network issues

**Solution:**
```bash
# Try without filters first
curl -X POST "http://localhost:8000/api/v1/realestate/fhfa/ingest" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Issue: HUD API returns 404

**Possible causes:**
- Invalid geography ID
- Geography type/ID mismatch

**Solution:**
```bash
# Use valid FIPS codes
# State FIPS: "06" (California), "36" (New York), etc.
# For national data, omit geography_id
```

### Issue: Redfin ingestion fails

**Possible causes:**
- S3 file structure changed
- Network timeout on large downloads
- Invalid region_type

**Solution:**
```bash
# Valid region_types: zip, city, neighborhood, metro
# Start with smaller regions (zip) before large ones (metro)
```

### Issue: OSM Overpass API timeout

**Possible causes:**
- Bounding box too large
- Too many buildings in area
- Public API under load

**Solution:**
```bash
# Reduce bounding box size
# Add building type filter
# Reduce limit
# Retry during off-peak hours
```

### Issue: Job stuck in "running" status

**Check logs:**
```bash
docker-compose logs -f app
```

**Check job details:**
```bash
curl http://localhost:8000/api/v1/realestate/{source}/status/{job_id}
```

**Common causes:**
- Network timeout
- Rate limiting
- API changes

## Architecture Notes

### Source Module Structure

```
app/sources/realestate/
├── __init__.py       # Module exports
├── client.py         # API clients (FHFA, HUD, Redfin, OSM)
├── ingest.py         # Ingestion orchestration
└── metadata.py       # Schema definitions and parsing
```

### Key Design Principles

1. **Bounded Concurrency:** All clients use semaphores
2. **Retry Logic:** Exponential backoff with jitter
3. **Typed Schemas:** No JSON blobs for data storage
4. **Idempotent Operations:** Safe to re-run ingestion
5. **Job Tracking:** All ingestion tracked in `ingestion_jobs`

### Rate Limiting Implementation

```python
# Example from FHFAClient
self.semaphore = asyncio.Semaphore(max_concurrency)

async with self.semaphore:
    response = await client.get(url)
```

## Next Steps

1. **Review API Documentation:**
   - Visit `/docs` for interactive API documentation
   - Test endpoints with Swagger UI

2. **Start with National Data:**
   - Ingest FHFA national HPI
   - Ingest HUD national permits

3. **Expand Geographic Coverage:**
   - Add state-level data
   - Add MSA-level data for key metros

4. **Build Analytics:**
   - Create views for common queries
   - Set up scheduled refreshes

5. **Monitor Jobs:**
   - Check job statuses regularly
   - Review logs for errors

## Additional Resources

- **FHFA:** https://www.fhfa.gov/DataTools/Downloads/
- **HUD:** https://www.huduser.gov/portal/datasets/socds.html
- **Redfin:** https://www.redfin.com/news/data-center/
- **OpenStreetMap:** https://wiki.openstreetmap.org/wiki/Overpass_API

## Support

For issues or questions:
1. Check logs: `docker-compose logs -f app`
2. Review job status via API
3. Check source documentation links above
4. Review RULES.md for architecture guidelines

