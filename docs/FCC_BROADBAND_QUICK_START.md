# FCC Broadband & Telecom Data - Quick Start Guide

## Overview

The FCC Broadband module provides access to the **Federal Communications Commission's National Broadband Map** data, offering comprehensive information about broadband internet availability across the United States.

### What's Included

- **Broadband Coverage Data**: Provider availability by state and county
- **Technology Types**: Fiber, Cable, DSL, Fixed Wireless, Satellite
- **Speed Information**: Advertised download/upload speeds
- **Summary Statistics**: Digital divide metrics, competition analysis

### Use Cases

- 📊 **Digital Divide Analysis**: Identify underserved areas lacking broadband
- 🏢 **ISP Competition Analysis**: Analyze market concentration (monopoly vs competitive)
- 🏠 **Real Estate Investment**: Broadband availability impacts property values
- 📋 **Policy Research**: Universal broadband initiative analysis
- 🗺️ **Network Planning**: Infrastructure deployment decisions

## No API Key Required

FCC data is public domain (U.S. government data) and requires **no API key**!

## Quick Start Examples

### 1. Ingest Broadband Data for Specific States

```bash
# Ingest California, New York, and Texas
curl -X POST "http://localhost:8001/api/v1/fcc-broadband/state/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "state_codes": ["CA", "NY", "TX"],
    "include_summary": true
  }'
```

**Response:**
```json
{
  "job_id": 123,
  "status": "pending",
  "message": "FCC broadband ingestion job created for 3 state(s)",
  "states": ["CA", "NY", "TX"],
  "check_status": "/api/v1/jobs/123"
}
```

### 2. Ingest All 50 States + DC

⚠️ **Large operation - may take 30-60 minutes**

```bash
curl -X POST "http://localhost:8001/api/v1/fcc-broadband/all-states/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "include_summary": true
  }'
```

### 3. Ingest Specific Counties

```bash
# Ingest Alameda County, CA and Manhattan, NY
curl -X POST "http://localhost:8001/api/v1/fcc-broadband/county/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "county_fips_codes": ["06001", "36061"],
    "include_summary": true
  }'
```

### 4. Check Job Status

```bash
curl "http://localhost:8001/api/v1/jobs/123"
```

## Reference Endpoints

### Get All State Codes

```bash
curl "http://localhost:8001/api/v1/fcc-broadband/reference/states"
```

### Get Technology Type Codes

```bash
curl "http://localhost:8001/api/v1/fcc-broadband/reference/technologies"
```

### Get Speed Tier Classifications

```bash
curl "http://localhost:8001/api/v1/fcc-broadband/reference/speed-tiers"
```

### List Available Datasets

```bash
curl "http://localhost:8001/api/v1/fcc-broadband/datasets"
```

## Database Tables

After ingestion, data is stored in PostgreSQL:

### Table: `fcc_broadband_coverage`

Detailed provider-level coverage data.

| Column | Type | Description |
|--------|------|-------------|
| `geography_type` | TEXT | Level: state, county, census_block |
| `geography_id` | TEXT | FIPS code or state code |
| `geography_name` | TEXT | Human-readable name |
| `provider_id` | TEXT | FCC provider ID (FRN) |
| `provider_name` | TEXT | Provider name |
| `technology_code` | TEXT | FCC technology code |
| `technology_name` | TEXT | Technology description |
| `max_advertised_down_mbps` | NUMERIC | Download speed (Mbps) |
| `max_advertised_up_mbps` | NUMERIC | Upload speed (Mbps) |
| `speed_tier` | TEXT | sub_broadband, basic, high_speed, gigabit |
| `business_service` | BOOLEAN | Offers business service |
| `consumer_service` | BOOLEAN | Offers consumer service |
| `data_date` | DATE | Data collection date |

### Table: `fcc_broadband_summary`

Aggregated statistics by geography.

| Column | Type | Description |
|--------|------|-------------|
| `geography_type` | TEXT | Level: state, county |
| `geography_id` | TEXT | FIPS code |
| `total_providers` | INTEGER | Provider count |
| `fiber_available` | BOOLEAN | Fiber (FTTP) available |
| `cable_available` | BOOLEAN | Cable modem available |
| `dsl_available` | BOOLEAN | DSL available |
| `max_speed_down_mbps` | NUMERIC | Highest download speed |
| `broadband_coverage_pct` | NUMERIC | % with 25/3 Mbps |
| `gigabit_coverage_pct` | NUMERIC | % with 1000+ Mbps |
| `provider_competition` | TEXT | monopoly, duopoly, competitive |

## SQL Query Examples

### Find States with Lowest Broadband Coverage

```sql
SELECT geography_name, broadband_coverage_pct, total_providers
FROM fcc_broadband_summary
WHERE geography_type = 'state'
ORDER BY broadband_coverage_pct ASC
LIMIT 10;
```

### ISP Competition by State

```sql
SELECT 
    geography_name,
    total_providers,
    provider_competition,
    fiber_available,
    max_speed_down_mbps
FROM fcc_broadband_summary
WHERE geography_type = 'state'
ORDER BY total_providers DESC;
```

### Fiber Availability by State

```sql
SELECT 
    geography_name,
    fiber_available,
    gigabit_coverage_pct
FROM fcc_broadband_summary
WHERE geography_type = 'state' AND fiber_available = true
ORDER BY gigabit_coverage_pct DESC;
```

### Providers in a Specific State

```sql
SELECT DISTINCT 
    provider_name,
    technology_name,
    max_advertised_down_mbps,
    max_advertised_up_mbps
FROM fcc_broadband_coverage
WHERE geography_type = 'state' AND geography_id = '06'  -- California
ORDER BY max_advertised_down_mbps DESC;
```

### Digital Divide: Areas Below FCC Broadband Definition

```sql
SELECT 
    geography_name,
    max_speed_down_mbps,
    total_providers
FROM fcc_broadband_summary
WHERE broadband_coverage_pct < 50  -- Less than 50% have broadband
ORDER BY broadband_coverage_pct ASC;
```

## Technology Codes Reference

| Code | Technology |
|------|------------|
| 10 | Asymmetric xDSL |
| 11 | ADSL2, ADSL2+ |
| 12 | VDSL |
| 20 | Symmetric xDSL |
| 40 | Cable Modem - DOCSIS 3.0 |
| 41 | Cable Modem - DOCSIS 3.1 |
| 50 | Fiber to the Premises (FTTP) |
| 60 | Satellite |
| 70 | Terrestrial Fixed Wireless |
| 71 | Licensed Fixed Wireless |
| 72 | Unlicensed Fixed Wireless |

## Speed Tier Classifications

| Tier | Download Speed | Description |
|------|---------------|-------------|
| sub_broadband | < 25 Mbps | Below FCC definition |
| basic_broadband | 25-100 Mbps | Meets FCC minimum |
| high_speed | 100-1000 Mbps | High-speed |
| gigabit | 1000+ Mbps | Gigabit fiber-class |

**Note:** FCC defines broadband as 25 Mbps download / 3 Mbps upload (as of 2024).

## State FIPS Codes

| State | Code | FIPS | State | Code | FIPS |
|-------|------|------|-------|------|------|
| Alabama | AL | 01 | Montana | MT | 30 |
| Alaska | AK | 02 | Nebraska | NE | 31 |
| Arizona | AZ | 04 | Nevada | NV | 32 |
| Arkansas | AR | 05 | New Hampshire | NH | 33 |
| California | CA | 06 | New Jersey | NJ | 34 |
| Colorado | CO | 08 | New Mexico | NM | 35 |
| Connecticut | CT | 09 | New York | NY | 36 |
| Delaware | DE | 10 | North Carolina | NC | 37 |
| DC | DC | 11 | North Dakota | ND | 38 |
| Florida | FL | 12 | Ohio | OH | 39 |
| Georgia | GA | 13 | Oklahoma | OK | 40 |
| Hawaii | HI | 15 | Oregon | OR | 41 |
| Idaho | ID | 16 | Pennsylvania | PA | 42 |
| Illinois | IL | 17 | Rhode Island | RI | 44 |
| Indiana | IN | 18 | South Carolina | SC | 45 |
| Iowa | IA | 19 | South Dakota | SD | 46 |
| Kansas | KS | 20 | Tennessee | TN | 47 |
| Kentucky | KY | 21 | Texas | TX | 48 |
| Louisiana | LA | 22 | Utah | UT | 49 |
| Maine | ME | 23 | Vermont | VT | 50 |
| Maryland | MD | 24 | Virginia | VA | 51 |
| Massachusetts | MA | 25 | Washington | WA | 53 |
| Michigan | MI | 26 | West Virginia | WV | 54 |
| Minnesota | MN | 27 | Wisconsin | WI | 55 |
| Mississippi | MS | 28 | Wyoming | WY | 56 |
| Missouri | MO | 29 | | | |

## Data Sources

- **FCC National Broadband Map**: https://broadbandmap.fcc.gov
- **FCC Open Data**: https://opendata.fcc.gov
- **FCC Form 477 Data**: https://www.fcc.gov/general/broadband-deployment-data-fcc-form-477

## Rate Limits

- No official rate limit documented
- Recommended: ~60 requests/minute (be respectful)
- Built-in exponential backoff with jitter for errors

## License

Public domain (U.S. government data) - free to use for any purpose.

## Troubleshooting

### "Invalid state code" Error

Use 2-letter state codes (e.g., `CA`, not `California`).

### "Invalid county FIPS" Error

County FIPS must be 5 digits: 2-digit state FIPS + 3-digit county FIPS.
Example: `06001` = Alameda County, California

### Job Stuck in "running" State

Large ingestions (all states) can take 30-60 minutes. Check job status periodically.

### No Data Returned

Some rural counties may have limited provider data in the FCC database.

## Support

- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc
- Health Check: http://localhost:8001/health
