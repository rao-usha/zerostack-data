# Agent Task: Expand SEC Form ADV Ingestion & Contact Extraction

## Objective
Enhance the existing SEC Form ADV ingestion to:
1. Ensure comprehensive data collection for all ~15,000 RIAs
2. Extract and structure contact information
3. Automatically identify and flag family offices
4. Link SEC ADV data to existing `family_offices` table

## Current State Analysis
- **sec_form_adv table:** Exists with some data
- **sec_form_adv_personnel table:** Exists (for key personnel)
- **Coverage:** Unknown - needs verification
- **Contact extraction:** Not systematically structured

## Project Rules
**CRITICAL:** Read and follow `RULES.md` in the project root. Key principles:
- **Analyst-Equivalent Research:** Collect any data a human analyst could collect through public research
- Use plugin architecture: code in `app/sources/sec_adv/`
- Official SEC EDGAR/IAPD API (preferred) + structured web extraction when beneficial
- Typed database columns, parameterized SQL
- Job tracking in `ingestion_jobs` table
- Bounded concurrency with `asyncio.Semaphore`
- Exponential backoff on API errors

## SEC Form ADV Information

### What is Form ADV?
- **Required Filing:** All SEC-registered investment advisors must file
- **Purpose:** Disclose business practices, fees, conflicts of interest, disciplinary history
- **Parts:**
  - **Part 1:** Registration information (firm details, AUM, clients, personnel)
  - **Part 2:** Brochure (narrative description of services)
- **Update Frequency:** Annual (within 90 days of fiscal year end) + amendments

### What Data is Available?
- **Firm Information:** Name, address, CRD number, SEC file number, website
- **Contact Information:** Business phone, business email (if provided)
- **Key Personnel:** Executive officers, compliance officers, managing members
- **Assets Under Management:** Regulatory AUM (required disclosure)
- **Client Types:** Individuals, high-net-worth, family offices, institutions
- **Organizational Structure:** Corporation, LLC, partnership
- **Filing Date:** When form was filed/amended

## Database Schema

### Verify/Update Existing Tables

**1. Check `sec_form_adv` table schema:**
```sql
-- Run this to see current schema
\d sec_form_adv
```

**Expected/Required Fields:**
```sql
CREATE TABLE IF NOT EXISTS sec_form_adv (
    id SERIAL PRIMARY KEY,
    crd_number TEXT NOT NULL UNIQUE,  -- Central Registration Depository Number
    sec_file_number TEXT,              -- SEC File Number (e.g., 801-12345)
    firm_name TEXT NOT NULL,
    legal_name TEXT,
    firm_type TEXT,                    -- Corporation, LLC, Partnership, etc.
    business_address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT,
    business_phone TEXT,
    business_email TEXT,
    website_url TEXT,
    
    -- Financial Info
    regulatory_aum NUMERIC(20,2),      -- Assets Under Management in millions
    num_clients INTEGER,
    
    -- Client Type Flags
    has_individual_clients BOOLEAN,
    has_high_net_worth_clients BOOLEAN,
    has_family_office_clients BOOLEAN,
    is_family_office BOOLEAN,          -- Firm itself is a family office
    
    -- Dates
    filing_date DATE,
    fiscal_year_end TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sec_adv_crd ON sec_form_adv(crd_number);
CREATE INDEX IF NOT EXISTS idx_sec_adv_is_family_office ON sec_form_adv(is_family_office);
CREATE INDEX IF NOT EXISTS idx_sec_adv_aum ON sec_form_adv(regulatory_aum);
```

**2. Check `sec_form_adv_personnel` table:**
```sql
CREATE TABLE IF NOT EXISTS sec_form_adv_personnel (
    id SERIAL PRIMARY KEY,
    form_adv_id INTEGER NOT NULL REFERENCES sec_form_adv(id),
    full_name TEXT NOT NULL,
    title TEXT,
    role TEXT,  -- CEO, CCO, CFO, Managing Member, etc.
    crd_number TEXT,  -- Individual CRD (if available)
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sec_adv_personnel_form_id ON sec_form_adv_personnel(form_adv_id);
```

## SEC EDGAR API Details

### API Endpoint
- **Base URL:** `https://www.sec.gov/cgi-bin/browse-edgar`
- **API Docs:** https://www.sec.gov/edgar/sec-api-documentation
- **Rate Limit:** 10 requests per second (be conservative: 5 req/sec)
- **User-Agent Required:** Must identify your organization and provide contact email

### Querying Form ADV
**Option 1: SEC EDGAR Full-Text Search API**
- Endpoint: `https://www.sec.gov/cgi-bin/browse-edgar`
- Params: `action=getcompany`, `type=ADV`, `output=xml`

**Option 2: SEC Investment Adviser Search**
- Better option: Use SEC's Investment Adviser Public Disclosure (IAPD) data
- Bulk download: https://www.adviserinfo.sec.gov/compilation
- Format: XML bulk data file (updated daily)

**Option 3: IAPD API (Recommended)**
- Endpoint: `https://api.adviserinfo.sec.gov/`
- Example: `/api/v1/firms` (paginated list of all firms)
- Example: `/api/v1/firms/{crd_number}` (individual firm details)

## Implementation Requirements

### 1. File Structure
```
app/sources/sec_adv/
├── __init__.py          # Export SECAdvisorAdapter
├── client.py            # SECAdvisorClient (IAPD API client)
├── metadata.py          # Schema management
├── ingest.py            # Full ingestion logic
└── contact_extraction.py  # Extract contacts for LPs/FOs

app/api/v1/sec_adv.py    # FastAPI router (expand existing)
```

### 2. `app/sources/sec_adv/client.py`
**Implement `SECAdvisorClient` class:**
- Base URL: `https://api.adviserinfo.sec.gov/api/v1`
- Methods:
  - `async def get_all_firms(skip: int = 0, take: int = 100)` - Paginate through all RIAs
  - `async def get_firm_details(crd_number: str)` - Get full Form ADV data for one firm
- Rate limiting: Semaphore with max 5 concurrent requests
- User-Agent: `"YourOrgName/1.0 (contact@yourdomain.com)"`
- Exponential backoff: 3 retries, base delay 2s

### 3. `app/sources/sec_adv/ingest.py`
**Implement ingestion functions:**

**`ingest_all_firms(db, full_refresh: bool = False)`**
- If `full_refresh=True`: Re-fetch all 15K firms
- If `full_refresh=False`: Only fetch firms filed/amended in last 90 days
- Steps:
  1. Query IAPD API for firm list (paginate through all)
  2. For each firm, fetch detailed Form ADV data
  3. Parse and extract fields (see schema above)
  4. Use `INSERT ... ON CONFLICT (crd_number) DO UPDATE` for upserts
  5. Extract personnel and insert into `sec_form_adv_personnel`

**`identify_family_offices(db)`**
- Query all firms where:
  - Firm name contains "family office", "family wealth", "family investment"
  - `has_family_office_clients = TRUE` AND `num_clients <= 5` (likely single-family office)
  - Regulatory AUM > $100M (exclude small operations)
- Set `is_family_office = TRUE` for identified firms

**`link_to_family_offices_table(db)`**
- Match `sec_form_adv` firms to `family_offices` by:
  - CRD number (if `family_offices.sec_crd_number` populated)
  - Name similarity (fuzzy matching)
- Update `family_offices.sec_crd_number` where match found

### 4. `app/sources/sec_adv/contact_extraction.py`
**Implement contact extraction for LP and FO tables:**

**`extract_lp_contacts_from_sec_adv(db)`**
- Match `sec_form_adv` firms to `lp_fund` by name similarity
- Extract contact fields: business_phone, business_email, website_url
- Insert personnel into `lp_key_contact` table
- Set `source_type='sec_adv'`, `confidence_level='high'`

**`extract_fo_contacts_from_sec_adv(db)`**
- Match `sec_form_adv` firms to `family_offices` by CRD number or name
- Extract contact fields from ADV filing
- **Optional Enhancement:** If family office has `website_url` in ADV, crawl for additional contacts
- Insert personnel into `family_office_contacts` table
- Set `source_type='sec_adv'` (or `'website'` for web-extracted), `confidence_level='high'`

**Optional: Website Enrichment (Future Enhancement)**
**`enrich_contacts_from_websites(db)`**
- For firms with website_url in `sec_form_adv` table
- Crawl "Team" or "People" pages for additional contacts beyond what's in ADV
- Useful for finding non-principal staff (analysts, portfolio managers)
- Insert with `source_type='website'`, `confidence_level='medium'`

### 5. `app/api/v1/sec_adv.py`
**Expand FastAPI router with new endpoints:**

**Existing endpoints (verify they work):**
- `POST /api/v1/sec/form-adv/ingest` - Ingest Form ADV data

**New endpoints to add:**
- `POST /api/v1/sec/form-adv/ingest-all` - Full ingestion (all 15K firms)
  - Query params: `full_refresh: bool = False`
  - Returns: job_id, firms_processed, firms_inserted, firms_updated
- `POST /api/v1/sec/form-adv/identify-family-offices` - Auto-identify FOs
  - Returns: job_id, family_offices_identified
- `POST /api/v1/sec/form-adv/link-to-family-offices` - Link to FO table
  - Returns: job_id, matches_found
- `POST /api/v1/sec/form-adv/extract-lp-contacts` - Extract LP contacts
  - Returns: job_id, lps_matched, contacts_extracted
- `POST /api/v1/sec/form-adv/extract-fo-contacts` - Extract FO contacts
  - Returns: job_id, fos_matched, contacts_extracted
- `GET /api/v1/sec/form-adv/stats` - Get coverage statistics
  - Returns: total_firms, family_offices_count, avg_aum, recent_filings_count

## Success Criteria
- [ ] `sec_form_adv` table schema verified/updated with all required fields
- [ ] `sec_form_adv_personnel` table exists and properly linked
- [ ] IAPD API client implemented with rate limiting and error handling
- [ ] Full ingestion process functional (can ingest all 15K RIAs)
- [ ] Family office identification logic implemented
- [ ] Linking to `family_offices` table functional (by CRD and name matching)
- [ ] Contact extraction for `lp_key_contact` table implemented
- [ ] Contact extraction for `family_office_contacts` table implemented
- [ ] All new API endpoints functional
- [ ] Job tracking in `ingestion_jobs` table
- [ ] Test: Full ingestion completes successfully (may take 1-2 hours)
- [ ] Test: Identify at least 1,500 family offices
- [ ] Test: Extract contacts for at least 30 LPs
- [ ] Test: Extract contacts for at least 40 family offices
- [ ] Documentation updated in `docs/EXTERNAL_DATA_SOURCES.md`

## Testing Steps

### 1. Verify Current State
```bash
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "\d sec_form_adv"
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM sec_form_adv;"
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM sec_form_adv_personnel;"
```

### 2. Test Incremental Ingestion (Recent Filings Only)
```bash
curl -X POST "http://localhost:8001/api/v1/sec/form-adv/ingest-all?full_refresh=false"
```

### 3. Test Full Ingestion (All 15K Firms - Long Running)
```bash
curl -X POST "http://localhost:8001/api/v1/sec/form-adv/ingest-all?full_refresh=true"
```
**Note:** This may take 1-2 hours. Monitor progress:
```bash
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
  SELECT source, status, row_count, started_at, completed_at 
  FROM ingestion_jobs 
  WHERE source='sec_adv' 
  ORDER BY started_at DESC 
  LIMIT 5;
"
```

### 4. Identify Family Offices
```bash
curl -X POST "http://localhost:8001/api/v1/sec/form-adv/identify-family-offices"

# Check results:
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
  SELECT COUNT(*) FROM sec_form_adv WHERE is_family_office = TRUE;
"
```

### 5. Link to Family Offices Table
```bash
curl -X POST "http://localhost:8001/api/v1/sec/form-adv/link-to-family-offices"

# Check results:
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
  SELECT COUNT(*) FROM family_offices WHERE sec_crd_number IS NOT NULL;
"
```

### 6. Extract LP Contacts
```bash
curl -X POST "http://localhost:8001/api/v1/sec/form-adv/extract-lp-contacts"

# Check results:
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
  SELECT COUNT(*) FROM lp_key_contact WHERE source_type='sec_adv';
"
```

### 7. Extract Family Office Contacts
```bash
curl -X POST "http://localhost:8001/api/v1/sec/form-adv/extract-fo-contacts"

# Check results:
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
  SELECT COUNT(*) FROM family_office_contacts WHERE source_type='sec_adv';
"
```

### 8. Get Statistics
```bash
curl "http://localhost:8001/api/v1/sec/form-adv/stats"
```

## Expected Outcomes

### SEC Form ADV Data:
- **Total RIAs:** ~15,000 firms
- **Family Offices Identified:** ~1,500-2,000 (10-13% of RIAs)
- **Firms with Contact Info:** 90%+ (phone almost always present, email sometimes)

### Contact Extraction:
- **LP Contacts:** 20-40 matches (LPs that are also SEC-registered RIAs)
- **Family Office Contacts:** 50-80 matches (family offices in your DB that are SEC-registered)

### Data Quality:
- **Business Phone:** 95%+ coverage
- **Business Email:** 40-60% coverage (not always disclosed)
- **Website URL:** 70-80% coverage
- **Key Personnel:** 90%+ have at least CCO listed

## Name Matching Algorithm

For linking SEC ADV firms to `lp_fund` and `family_offices`:

**Simple Fuzzy Matching:**
```python
from difflib import SequenceMatcher

def fuzzy_match(name1: str, name2: str, threshold: float = 0.85) -> bool:
    """Returns True if names are similar enough."""
    name1 = name1.lower().strip()
    name2 = name2.lower().strip()
    
    # Remove common suffixes for matching
    suffixes = [' llc', ' lp', ' inc', ' corporation', ' corp', ' ltd']
    for suffix in suffixes:
        name1 = name1.replace(suffix, '')
        name2 = name2.replace(suffix, '')
    
    ratio = SequenceMatcher(None, name1, name2).ratio()
    return ratio >= threshold
```

**Use this for:**
- Matching `sec_form_adv.firm_name` to `lp_fund.name` or `lp_fund.formal_name`
- Matching `sec_form_adv.firm_name` to `family_offices.name` or `family_offices.legal_name`

## Rate Limiting & Politeness

**SEC API Requirements:**
- **User-Agent Header:** MUST include your organization name and contact email
  - Example: `"Nexdata/1.0 (admin@nexdata.com)"`
- **Rate Limit:** 10 req/sec official limit
- **Recommended:** 5 req/sec to be conservative
- **Implement:** `asyncio.Semaphore(5)` for concurrent request limiting

**Exponential Backoff:**
- If 429 (rate limit): Wait 60 seconds, then retry
- If 5xx (server error): Exponential backoff (2s, 4s, 8s)
- Max retries: 3

## Dependencies
```
# Should already be in requirements.txt:
httpx>=0.24.0
asyncio
sqlalchemy>=2.0.0

# May need to add:
difflib  # For fuzzy name matching (built-in Python)
```

## Priority Order
1. **First:** Verify current schema and data coverage
2. **Second:** Implement full ingestion (all 15K RIAs)
3. **Third:** Implement family office identification
4. **Fourth:** Implement contact extraction (LP + FO)
5. **Fifth:** Implement linking to existing tables

## Notes
- Full ingestion of 15K firms will take 1-2 hours (at 5 req/sec)
- Consider running full ingestion as a background job
- Update strategy: Run incremental (last 90 days) nightly, full refresh quarterly
- SEC data is public domain, no licensing concerns
- Form ADV Part 2 (narrative brochure) could be extracted for additional insights (future enhancement)
- Some RIAs file Form ADV-E (custody surprise exams) - not relevant for this task
- CRD numbers are unique and stable (best identifier)
