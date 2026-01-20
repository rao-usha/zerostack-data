# IRS Statistics of Income (SOI) - Quick Start Guide

## Overview

The IRS Statistics of Income (SOI) module provides access to income and wealth distribution data derived from individual tax returns. This data is published annually by the IRS and is available at various geographic levels.

**Source:** https://www.irs.gov/statistics/soi-tax-stats

**API Key:** ❌ **NOT REQUIRED** (public domain U.S. government data)

**Data Format:** Bulk CSV downloads (no REST API)

---

## Available Datasets

### 1. Individual Income by ZIP Code (`zip_income`)

Income statistics aggregated by 5-digit ZIP code and AGI bracket.

**Table:** `irs_soi_zip_income`

**Data includes:**
- Number of returns by filing status
- Total AGI, wages, dividends, interest
- Capital gains, business income, retirement distributions
- Tax liability, credits, deductions
- Breakdown by 6 AGI brackets

**Available years:** 2017-2021

**Endpoint:** `POST /api/v1/irs-soi/zip-income/ingest`

---

### 2. Individual Income by County (`county_income`)

Similar to ZIP data but aggregated at the county level with FIPS codes.

**Table:** `irs_soi_county_income`

**Data includes:**
- All income categories from ZIP data
- 5-digit county FIPS codes (joinable with Census data)
- County names

**Available years:** 2017-2021

**Endpoint:** `POST /api/v1/irs-soi/county-income/ingest`

---

### 3. County-to-County Migration (`migration`)

Migration patterns derived from year-over-year tax return address changes.

**Table:** `irs_soi_migration`

**Data includes:**
- Origin and destination counties
- Number of returns/exemptions migrating
- Aggregate income of migrants
- Inflow and outflow perspectives

**Use cases:**
- Track population movements
- Analyze income migration patterns
- Identify growth/decline areas
- Study tax base changes

**Available years:** 2017-2021

**Endpoint:** `POST /api/v1/irs-soi/migration/ingest`

---

### 4. Business Income by ZIP Code (`business_income`)

Business and self-employment income statistics by ZIP code.

**Table:** `irs_soi_business_income`

**Data includes:**
- Schedule C (sole proprietorships)
- Partnership/S-corp income (Schedule E)
- Rental real estate income
- Farm income
- Self-employment tax

**Available years:** 2017-2021

**Endpoint:** `POST /api/v1/irs-soi/business-income/ingest`

---

## AGI (Adjusted Gross Income) Brackets

Income data is categorized into the following AGI brackets:

| Code | Range |
|------|-------|
| 1 | $1 under $25,000 |
| 2 | $25,000 under $50,000 |
| 3 | $50,000 under $75,000 |
| 4 | $75,000 under $100,000 |
| 5 | $100,000 under $200,000 |
| 6 | $200,000 or more |
| 0 | Total (all income levels) |

---

## Quick Start Examples

### 1. Ingest ZIP Code Income Data

```bash
curl -X POST "http://localhost:8001/api/v1/irs-soi/zip-income/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2021}'
```

**Response:**
```json
{
  "job_id": 123,
  "status": "pending",
  "message": "IRS SOI ZIP income ingestion job created for year 2021",
  "check_status": "/api/v1/jobs/123"
}
```

### 2. Ingest County Income Data

```bash
curl -X POST "http://localhost:8001/api/v1/irs-soi/county-income/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2021}'
```

### 3. Ingest Migration Data

```bash
# Ingest both inflow and outflow
curl -X POST "http://localhost:8001/api/v1/irs-soi/migration/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2021, "flow_type": "both"}'

# Ingest only inflow data
curl -X POST "http://localhost:8001/api/v1/irs-soi/migration/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2021, "flow_type": "inflow"}'
```

### 4. Ingest Business Income Data

```bash
curl -X POST "http://localhost:8001/api/v1/irs-soi/business-income/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2021}'
```

### 5. Ingest All Datasets

```bash
curl -X POST "http://localhost:8001/api/v1/irs-soi/all/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2021}'
```

**Note:** This downloads multiple large files and may take 10+ minutes.

---

## Check Job Status

```bash
curl "http://localhost:8001/api/v1/jobs/123"
```

**Response:**
```json
{
  "id": 123,
  "source": "irs_soi",
  "status": "success",
  "config": {"dataset": "zip_income", "year": 2021},
  "rows_inserted": 165432,
  "started_at": "2024-01-15T10:30:05Z",
  "completed_at": "2024-01-15T10:35:45Z",
  "error_message": null
}
```

---

## Reference Endpoints

### Get AGI Bracket Definitions

```bash
curl "http://localhost:8001/api/v1/irs-soi/reference/agi-brackets"
```

### Get Available Years

```bash
curl "http://localhost:8001/api/v1/irs-soi/reference/years"
```

### List All Datasets

```bash
curl "http://localhost:8001/api/v1/irs-soi/datasets"
```

---

## Query Examples (SQL)

### Top 10 Highest-Income ZIP Codes

```sql
SELECT 
    zip_code,
    state_abbr,
    num_returns,
    total_agi / 1000 AS total_agi_millions,
    avg_agi
FROM irs_soi_zip_income
WHERE tax_year = 2021
  AND agi_class = '0'  -- Total row
ORDER BY total_agi DESC
LIMIT 10;
```

### Income Distribution by AGI Bracket

```sql
SELECT 
    agi_class_label,
    SUM(num_returns) AS total_returns,
    SUM(total_agi) / 1000 AS total_agi_millions,
    AVG(avg_agi) AS avg_agi_per_return
FROM irs_soi_zip_income
WHERE tax_year = 2021
  AND agi_class != '0'
GROUP BY agi_class, agi_class_label
ORDER BY agi_class;
```

### Top Migration Destinations (Inflows)

```sql
SELECT 
    dest_county_name,
    dest_state_abbr,
    SUM(num_returns) AS inbound_returns,
    SUM(total_agi) / 1000 AS inbound_agi_millions
FROM irs_soi_migration
WHERE tax_year = 2021
  AND flow_type = 'inflow'
  AND orig_county_code != dest_county_code  -- Exclude same-county
GROUP BY dest_county_code, dest_county_name, dest_state_abbr
ORDER BY inbound_returns DESC
LIMIT 20;
```

### Business Income by State

```sql
SELECT 
    state_abbr,
    SUM(num_returns) AS total_returns,
    SUM(total_business_income) / 1000 AS business_income_millions,
    SUM(total_partnership_income) / 1000 AS partnership_income_millions,
    SUM(total_rental_income) / 1000 AS rental_income_millions
FROM irs_soi_business_income
WHERE tax_year = 2021
GROUP BY state_abbr
ORDER BY business_income_millions DESC;
```

### County-Level Income Comparison

```sql
SELECT 
    county_name,
    state_abbr,
    county_code,
    num_returns,
    total_agi / num_returns * 1000 AS avg_agi_dollars,
    total_wages / num_returns * 1000 AS avg_wages_dollars
FROM irs_soi_county_income
WHERE tax_year = 2021
  AND agi_class = '0'  -- Total
  AND num_returns > 10000  -- Filter small counties
ORDER BY avg_agi_dollars DESC
LIMIT 50;
```

---

## Database Schema

### irs_soi_zip_income

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| tax_year | INTEGER | Tax year |
| state_code | TEXT | State FIPS code |
| state_abbr | TEXT | State abbreviation |
| zip_code | TEXT | 5-digit ZIP code |
| agi_class | TEXT | AGI bracket (1-6, 0=total) |
| agi_class_label | TEXT | AGI bracket description |
| num_returns | INTEGER | Number of returns |
| total_agi | BIGINT | Total AGI (thousands) |
| total_wages | BIGINT | Total wages (thousands) |
| total_dividends | BIGINT | Total dividends (thousands) |
| total_capital_gains | BIGINT | Total capital gains (thousands) |
| avg_agi | NUMERIC | Average AGI per return |
| ingested_at | TIMESTAMP | Ingestion timestamp |

### irs_soi_migration

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| tax_year | INTEGER | Tax year |
| flow_type | TEXT | inflow or outflow |
| dest_state_code | TEXT | Destination state FIPS |
| dest_county_code | TEXT | Destination county FIPS |
| dest_county_name | TEXT | Destination county name |
| orig_state_code | TEXT | Origin state FIPS |
| orig_county_code | TEXT | Origin county FIPS |
| orig_county_name | TEXT | Origin county name |
| num_returns | INTEGER | Migrating returns |
| num_exemptions | INTEGER | Migrating exemptions |
| total_agi | BIGINT | Total AGI of migrants |
| avg_agi | NUMERIC | Average AGI per migrant |
| ingested_at | TIMESTAMP | Ingestion timestamp |

---

## Data Notes

1. **Dollar amounts** are in thousands (divide display by 1,000 or multiply by 1,000 for actual dollars)

2. **Data suppression**: Some cells may be null due to IRS privacy rules (small counts suppressed)

3. **Lag time**: Data is typically 2-3 years behind current year

4. **ZIP codes**: Some ZIP codes may not appear due to privacy thresholds

5. **Migration data**: Represents address changes between consecutive tax years

6. **AGI bracket 0**: Represents totals across all income levels (not a bracket)

---

## Use Cases

### Real Estate Investment

- Identify high-income ZIP codes for premium developments
- Track income migration to emerging markets
- Analyze wealth concentration by geography

### Economic Research

- Study income inequality trends
- Analyze tax base changes over time
- Compare rural vs. urban income patterns

### Market Analysis

- Target high-net-worth customer segments
- Analyze business formation patterns
- Track self-employment trends

### Policy Analysis

- Evaluate tax policy impacts by income level
- Study migration in response to state tax changes
- Analyze wealth distribution trends

---

## Related Documentation

- [IRS SOI Tax Stats](https://www.irs.gov/statistics/soi-tax-stats)
- [ZIP Code Data Documentation](https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi)
- [Migration Data Documentation](https://www.irs.gov/statistics/soi-tax-stats-migration-data)

---

## Troubleshooting

### Download Timeouts

Large files may timeout on slow connections. Solutions:
1. Enable caching (`use_cache: true` in request)
2. Retry failed jobs
3. Increase timeout in settings

### Missing Data

Some ZIP codes or counties may not appear due to:
- IRS privacy suppression (small counts)
- Invalid or inactive geographic codes
- P.O. Box only ZIP codes

### Column Mapping

If parsing fails, check the IRS data dictionary for column name changes between years.

---

## Support

For issues with IRS SOI data ingestion, check:
1. Job status: `GET /api/v1/jobs/{job_id}`
2. Error messages in job details
3. Application logs for detailed errors
