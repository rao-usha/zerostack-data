# SEC EDGAR Quick Start Guide

This guide helps you quickly start ingesting SEC corporate filings into your database.

## Overview

The SEC EDGAR adapter provides access to corporate filings including:
- **10-K** - Annual reports
- **10-Q** - Quarterly reports
- **8-K** - Current reports (material events)
- **S-1/S-3/S-4** - Registration statements
- **XBRL data** - Structured financial data

## Key Features

✅ **No API Key Required** - SEC EDGAR is publicly accessible  
✅ **Rate Limit Compliant** - Respects SEC's 10 req/sec limit  
✅ **Job Tracking** - All ingestion runs tracked in database  
✅ **Idempotent** - Safe to re-run without duplicates  
✅ **Batch Support** - Ingest multiple companies at once  

## Quick Examples

### 1. Ingest Apple's Filings (Last 5 Years)

```bash
curl -X POST "http://localhost:8000/api/v1/sec/ingest/company" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193"
  }'
```

**Default behavior:**
- Filing types: 10-K and 10-Q
- Date range: Last 5 years

### 2. Ingest Specific Filing Types and Date Range

```bash
curl -X POST "http://localhost:8000/api/v1/sec/ingest/company" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193",
    "filing_types": ["10-K", "10-Q", "8-K"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
  }'
```

### 3. Ingest Multiple Companies at Once

```bash
curl -X POST "http://localhost:8000/api/v1/sec/ingest/multiple" \
  -H "Content-Type: application/json" \
  -d '{
    "ciks": ["0000320193", "0000789019", "0001652044"],
    "filing_types": ["10-K", "10-Q"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
  }'
```

**Companies in example:**
- `0000320193` - Apple Inc.
- `0000789019` - Microsoft Corporation
- `0001652044` - Alphabet Inc. (Google)

### 4. Check Job Status

```bash
curl "http://localhost:8000/api/v1/jobs/{job_id}"
```

Replace `{job_id}` with the ID returned from the ingestion request.

## Finding Company CIK Numbers

### Method 1: Use Common Companies Endpoint

```bash
curl "http://localhost:8000/api/v1/sec/common-companies"
```

Returns CIK numbers for major companies grouped by sector.

### Method 2: SEC EDGAR Search

1. Go to https://www.sec.gov/edgar/searchedgar/companysearch.html
2. Search for company name
3. CIK is displayed in search results

### Method 3: Ticker Lookup

- **Apple (AAPL)** → `0000320193`
- **Microsoft (MSFT)** → `0000789019`
- **Amazon (AMZN)** → `0001018724`
- **Tesla (TSLA)** → `0001318605`
- **NVIDIA (NVDA)** → `0001045810`

See `SEC_COMPANIES_TRACKING.md` for more CIK numbers.

## Supported Filing Types

Get the full list of supported filing types:

```bash
curl "http://localhost:8000/api/v1/sec/supported-filing-types"
```

**Currently supported:**
- `10-K`, `10-K/A` - Annual reports
- `10-Q`, `10-Q/A` - Quarterly reports
- `8-K`, `8-K/A` - Current reports
- `S-1`, `S-1/A` - Initial registration statements
- `S-3`, `S-3/A` - Registration statements
- `S-4`, `S-4/A` - Business combination registration statements

## Database Schema

Filings are stored in tables by type:
- `sec_10k` - Annual reports
- `sec_10q` - Quarterly reports
- `sec_8k` - Current reports
- `sec_s1` - S-1 registration statements
- `sec_s3` - S-3 registration statements
- `sec_s4` - S-4 registration statements

### Table Structure

```sql
CREATE TABLE sec_10k (
    id SERIAL PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT,
    company_name TEXT NOT NULL,
    accession_number TEXT NOT NULL UNIQUE,
    filing_type TEXT NOT NULL,
    filing_date DATE NOT NULL,
    report_date DATE,
    primary_document TEXT,
    filing_url TEXT,
    interactive_data_url TEXT,
    file_number TEXT,
    film_number TEXT,
    items TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);
```

## Query Examples

### Get All 10-K Filings for a Company

```sql
SELECT * FROM sec_10k
WHERE cik = '0000320193'
ORDER BY filing_date DESC;
```

### Get Latest Quarterly Report

```sql
SELECT * FROM sec_10q
WHERE cik = '0000320193'
ORDER BY filing_date DESC
LIMIT 1;
```

### Count Filings by Company

```sql
SELECT 
    company_name,
    ticker,
    COUNT(*) as filing_count
FROM sec_10k
GROUP BY company_name, ticker
ORDER BY filing_count DESC;
```

### Get All 8-K Filings (Material Events)

```sql
SELECT 
    company_name,
    filing_date,
    items,
    filing_url
FROM sec_8k
WHERE cik = '0000320193'
ORDER BY filing_date DESC;
```

### Join Multiple Filing Types

```sql
-- Get company with both annual and quarterly filings
SELECT DISTINCT
    k.company_name,
    k.ticker,
    COUNT(DISTINCT k.id) as annual_reports,
    COUNT(DISTINCT q.id) as quarterly_reports
FROM sec_10k k
LEFT JOIN sec_10q q ON k.cik = q.cik
GROUP BY k.company_name, k.ticker
ORDER BY annual_reports DESC;
```

## Rate Limits & Performance

**SEC Rate Limits:**
- 10 requests per second per IP address (strictly enforced)
- This service uses 8 req/sec to be conservative

**Performance Tips:**
1. Use batch ingestion for multiple companies
2. Ingestion runs in background - monitor job status
3. Re-running ingestion is safe (idempotent with `ON CONFLICT`)
4. Consider date range filtering to reduce API calls

## Error Handling

### Common Errors

**Invalid CIK:**
```json
{
  "detail": "Invalid CIK format: 123"
}
```
→ **Solution:** CIK must be numeric, up to 10 digits

**Rate Limited:**
- Service automatically handles rate limiting
- Uses exponential backoff with retry
- Job status will show if rate limited

**Company Not Found:**
- Check CIK is correct (use SEC EDGAR search)
- Some companies may not have EDGAR filings

## Monitoring Jobs

### Check All SEC Jobs

```sql
SELECT 
    id,
    status,
    config->>'cik' as cik,
    created_at,
    completed_at,
    rows_inserted,
    error_message
FROM ingestion_jobs
WHERE source = 'sec'
ORDER BY created_at DESC;
```

### Check Failed Jobs

```sql
SELECT * FROM ingestion_jobs
WHERE source = 'sec' AND status = 'failed'
ORDER BY created_at DESC;
```

## Advanced Usage

### Python Client Example

```python
import requests
from datetime import date

# Ingest Apple's filings
response = requests.post(
    "http://localhost:8000/api/v1/sec/ingest/company",
    json={
        "cik": "0000320193",
        "filing_types": ["10-K", "10-Q"],
        "start_date": "2020-01-01",
        "end_date": "2024-12-31"
    }
)

job_id = response.json()["job_id"]
print(f"Job ID: {job_id}")

# Check job status
status_response = requests.get(
    f"http://localhost:8000/api/v1/jobs/{job_id}"
)
print(status_response.json())
```

### Automated Ingestion Script

```python
import requests
import time

# List of companies to ingest
companies = [
    {"cik": "0000320193", "name": "Apple"},
    {"cik": "0000789019", "name": "Microsoft"},
    {"cik": "0001652044", "name": "Google"},
]

for company in companies:
    print(f"Ingesting {company['name']}...")
    
    response = requests.post(
        "http://localhost:8000/api/v1/sec/ingest/company",
        json={"cik": company["cik"]}
    )
    
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        print(f"  Job {job_id} created")
    else:
        print(f"  Error: {response.text}")
    
    # Small delay between requests
    time.sleep(1)
```

## Data Compliance

**Data Source:** SEC EDGAR (https://www.sec.gov/edgar)  
**License:** Public domain (U.S. government data)  
**Rate Limits:** 10 req/sec (enforced by SEC)  
**User-Agent:** Required by SEC (automatically set by service)  

**Compliance Notes:**
- All data is publicly available corporate filings
- No PII collection beyond public officer/director names in filings
- Safe to use for commercial purposes (public domain)

## Next Steps

1. **Track Your Ingestion:** Update `SEC_COMPANIES_TRACKING.md` after ingesting companies
2. **Query the Data:** Use SQL examples above to analyze filings
3. **Automate:** Set up scheduled ingestion for companies you monitor
4. **Extend:** Add more companies using the batch ingestion API

## Troubleshooting

### Issue: "Failed to fetch SEC data"

**Possible causes:**
1. SEC EDGAR is temporarily unavailable
2. CIK doesn't exist
3. Network connectivity issues

**Solution:**
- Check SEC EDGAR website status
- Verify CIK on SEC EDGAR website
- Check service logs: `docker-compose logs api`

### Issue: Slow ingestion

**Possible causes:**
1. Large date range (many filings)
2. Rate limiting active
3. Multiple concurrent jobs

**Solution:**
- Use smaller date ranges
- Monitor rate limits in logs
- Check `ingestion_jobs` table for active jobs

### Issue: Missing filings

**Possible causes:**
1. Date range doesn't include filing date
2. Filing type not specified
3. Company didn't file in that period

**Solution:**
- Check filing dates on SEC EDGAR website
- Verify filing_types parameter
- Query database to see what was ingested

## Support

- **API Documentation:** http://localhost:8000/docs
- **Source Code:** `/app/sources/sec/`
- **Job Tracking:** `/app/core/models.py` (IngestionJob model)

## Example Workflows

### Workflow 1: Monitor Tech Giants

```bash
# Ingest major tech companies
curl -X POST "http://localhost:8000/api/v1/sec/ingest/multiple" \
  -H "Content-Type: application/json" \
  -d '{
    "ciks": [
      "0000320193",  "0000789019",  "0001652044",
      "0001018724",  "0001326801",  "0001318605"
    ],
    "filing_types": ["10-K", "10-Q", "8-K"]
  }'
```

### Workflow 2: Banking Sector Analysis

```bash
# Ingest major banks
curl -X POST "http://localhost:8000/api/v1/sec/ingest/multiple" \
  -H "Content-Type: application/json" \
  -d '{
    "ciks": [
      "0000019617",  "0000070858",  "0000072971",
      "0000886982",  "0000895421"
    ],
    "filing_types": ["10-K", "10-Q"]
  }'
```

### Workflow 3: Quarterly Update

```bash
# Ingest latest quarter for tracked companies
curl -X POST "http://localhost:8000/api/v1/sec/ingest/multiple" \
  -H "Content-Type: application/json" \
  -d '{
    "ciks": ["YOUR", "COMPANY", "CIKS"],
    "filing_types": ["10-Q"],
    "start_date": "2024-10-01",
    "end_date": "2024-12-31"
  }'
```

---

**Ready to start?** Try ingesting your first company:

```bash
curl -X POST "http://localhost:8000/api/v1/sec/ingest/company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000320193"}'
```

Then check the results:

```sql
SELECT * FROM sec_10k WHERE cik = '0000320193' LIMIT 5;
```

