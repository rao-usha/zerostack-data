# SEC Form ADV Ingestion Guide

## Overview

The SEC Form ADV ingestion feature allows you to retrieve **business contact information** for registered investment advisers, including many family offices.

## What is Form ADV?

Form ADV is the uniform form used by investment advisers to register with the SEC and state securities authorities. It contains:

- **Part 1**: Registration information (business details, contact info, AUM, etc.)
- **Part 2**: Narrative brochure (services, fees, practices)

## What Data Can You Get?

### ✅ Available Information

- **Business Addresses**: Street address, city, state, ZIP, country
- **Business Contact**: Phone, fax, email, website
- **Key Personnel**: Names, titles, positions
- **Assets Under Management**: Total AUM and date
- **Client Information**: Client counts by type
- **Registration Details**: Status, dates, jurisdictions

### ❌ NOT Available

- Personal contact information (PII)
- Non-registered firms
- Family offices with registration exemptions
- Historical contact data (only current)

## Important Limitations

### Registration Exemptions

Many family offices qualify for the "family office exemption" under SEC rules and are **NOT required to register**. This means:

- They won't have Form ADV data
- They won't appear in IAPD searches
- We cannot retrieve their information via this method

### Who IS Registered?

Investment advisers must register if they:
- Manage over $100M in assets (SEC registration)
- Don't qualify for exemptions
- Provide advisory services to clients beyond family members

## API Endpoints

### 1. Ingest by Family Office Names

**Endpoint:** `POST /api/v1/sec/form-adv/ingest/family-offices`

**Use Case:** Search for multiple family offices by name

**Request Body:**
```json
{
  "family_office_names": [
    "Soros Fund Management",
    "Pritzker Group",
    "Cascade Investment"
  ],
  "max_concurrency": 1,
  "max_requests_per_second": 2.0
}
```

**Response:**
```json
{
  "job_id": 123,
  "status": "pending",
  "message": "Form ADV ingestion job created for 3 family offices"
}
```

### 2. Ingest by CRD Number

**Endpoint:** `POST /api/v1/sec/form-adv/ingest/crd`

**Use Case:** When you already know the CRD number

**Request Body:**
```json
{
  "crd_number": "158626"
}
```

**Response:**
```json
{
  "job_id": 124,
  "status": "pending",
  "message": "Form ADV ingestion job created for CRD 158626"
}
```

## Database Schema

### Main Table: `sec_form_adv`

Key columns:
- `crd_number` (TEXT, UNIQUE): Central Registration Depository number
- `sec_number` (TEXT): SEC file number
- `firm_name` (TEXT): Adviser firm name
- `legal_name` (TEXT): Legal business name
- `business_address_*` (TEXT): Business address components
- `business_phone` (TEXT): Business phone number
- `business_email` (TEXT): Business email address
- `website` (TEXT): Firm website
- `assets_under_management` (NUMERIC): Total AUM
- `is_family_office` (BOOLEAN): Identified as family office
- `key_personnel` (JSONB): Key personnel data

### Personnel Table: `sec_form_adv_personnel`

Key columns:
- `crd_number` (TEXT): Link to main firm
- `individual_crd_number` (TEXT): Individual's CRD
- `full_name` (TEXT): Person's full name
- `title` (TEXT): Job title
- `position_type` (TEXT): Position category
- `email` (TEXT): Business email (if available)
- `phone` (TEXT): Business phone (if available)

## Usage Examples

### Python Script

```python
import requests
import time

BASE_URL = "http://localhost:8001"

# 1. Trigger ingestion
response = requests.post(
    f"{BASE_URL}/api/v1/sec/form-adv/ingest/family-offices",
    json={
        "family_office_names": ["Soros Fund Management", "Pritzker Group"],
        "max_concurrency": 1,
        "max_requests_per_second": 2.0
    }
)
job = response.json()
job_id = job["job_id"]

# 2. Poll for completion
while True:
    status = requests.get(f"{BASE_URL}/api/v1/jobs/{job_id}").json()
    if status["status"] in ["success", "failed"]:
        break
    time.sleep(5)

# 3. Query results (via SQL)
# See SQL examples below
```

### cURL

```bash
# Trigger ingestion
curl -X POST http://localhost:8001/api/v1/sec/form-adv/ingest/family-offices \
  -H "Content-Type: application/json" \
  -d '{
    "family_office_names": ["Soros Fund Management", "Pritzker Group"],
    "max_concurrency": 1,
    "max_requests_per_second": 2.0
  }'

# Check job status
curl http://localhost:8001/api/v1/jobs/123
```

### SQL Queries

```sql
-- Get all family offices with contact info
SELECT 
    firm_name,
    business_phone,
    business_email,
    website,
    business_address_city,
    business_address_state,
    assets_under_management
FROM sec_form_adv
WHERE is_family_office = TRUE
ORDER BY assets_under_management DESC NULLS LAST;

-- Get key personnel for a specific firm
SELECT 
    f.firm_name,
    p.full_name,
    p.title,
    p.position_type,
    p.email,
    p.phone
FROM sec_form_adv f
JOIN sec_form_adv_personnel p ON f.crd_number = p.crd_number
WHERE f.firm_name ILIKE '%soros%'
ORDER BY p.full_name;

-- Search by state
SELECT 
    firm_name,
    business_phone,
    business_email,
    business_address_city
FROM sec_form_adv
WHERE business_address_state = 'NY'
    AND is_family_office = TRUE;
```

## Testing

### Quick Test

```bash
# Run the test script
python test_formadv_ingestion.py
```

The test script will:
1. Search for sample family offices
2. Poll job status until complete
3. Display results summary
4. Show how to query the data

### Manual Testing via Docker

```bash
# 1. Start the service
docker-compose up -d

# 2. Trigger ingestion
curl -X POST http://localhost:8001/api/v1/sec/form-adv/ingest/family-offices \
  -H "Content-Type: application/json" \
  -d '{
    "family_office_names": ["Pritzker Group"]
  }'

# 3. Check logs
docker-compose logs -f api

# 4. Query database
docker-compose exec postgres psql -U nexdata -d nexdata
```

```sql
SELECT * FROM sec_form_adv WHERE firm_name ILIKE '%pritzker%';
```

## Rate Limits

### SEC IAPD Rate Limits

- **Documented limit**: Not explicitly published
- **Our default**: 2 requests/second (conservative)
- **Max concurrency**: 1-3 parallel requests

### Why Conservative?

- IAPD is not a high-throughput API
- We want to be respectful of SEC resources
- Individual lookups can be slow
- Better to be safe and avoid throttling

## Troubleshooting

### Issue: "No data found for firm"

**Possible causes:**
1. Firm is not registered (has exemption)
2. Firm name doesn't match exactly in IAPD
3. Firm only registered with state (not SEC)

**Solutions:**
- Try variations of the name
- Search manually on https://adviserinfo.sec.gov/
- Use CRD number if you find it manually

### Issue: "Rate limit exceeded"

**Solutions:**
- Reduce `max_requests_per_second`
- Reduce `max_concurrency` to 1
- Add delays between batch runs

### Issue: "Search returns multiple matches"

**Expected behavior:**
- The system ingests ALL matches
- You can filter by `is_family_office` flag
- Review results and identify correct firm

## Compliance & Ethics

### What We Do

✅ Use official, public SEC data  
✅ Follow documented API patterns  
✅ Respect rate limits  
✅ Store only business contact info  
✅ Follow PII protection guidelines  

### What We DON'T Do

❌ Scrape websites  
❌ Collect personal contact information  
❌ Attempt to access restricted data  
❌ Bypass authentication  
❌ Join data to de-anonymize individuals  

## Future Enhancements

Possible improvements (user-requested only):

1. **Bulk CSV Import**: Download SEC's quarterly CSV files
2. **Historical Tracking**: Track changes over time
3. **State Registration**: Query state securities regulators
4. **Enhanced Search**: Fuzzy matching, aliases
5. **Auto-refresh**: Periodic updates for tracked firms

## References

- [SEC IAPD Website](https://adviserinfo.sec.gov/)
- [SEC Form ADV Data Files](https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data)
- [SEC Investment Adviser Registration](https://www.sec.gov/investment-advisers)
- [Family Office Exemption](https://www.sec.gov/rules/final/2011/ia-3220.pdf)

## Support

For issues or questions:
1. Check logs: `docker-compose logs -f api`
2. Review job status: `GET /api/v1/jobs/{job_id}`
3. Check database: Query `sec_form_adv` table
4. File issue in project repository

