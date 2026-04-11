# Agent Task: Implement IRS Statistics of Income (SOI) Ingestion

## Objective
Implement a data ingestion adapter for IRS Statistics of Income bulk data downloads to collect individual income by ZIP/county, migration data, and business income statistics.

## Project Rules
**CRITICAL:** Read and follow `RULES.md` in the project root. Key requirements:
- Use plugin architecture: all code in `app/sources/irs_soi/`
- Implement bounded concurrency with `asyncio.Semaphore`
- Use typed database columns (INT, NUMERIC, TEXT), NOT JSON blobs
- All ingestion must be tracked in `ingestion_jobs` table
- Parameterized SQL queries only
- Exponential backoff with jitter for download errors
- **IMPORTANT:** IRS SOI uses bulk CSV/Excel downloads, NOT an API

## Data Source Information
- **Website:** https://www.irs.gov/statistics/soi-tax-stats
- **Format:** CSV and Excel (XLS/XLSX) bulk downloads
- **Rate Limits:** No specific limits, but be respectful (1 concurrent download)
- **Authentication:** None required (public data)
- **License:** Public domain (U.S. government data)

## Datasets to Implement

### 1. Individual Income by ZIP Code
- **URL Pattern:** `https://www.irs.gov/pub/irs-soi/YYZIPCODE.csv` (YY = year, e.g., 21 for 2021)
- **Fields:** zipcode, agi_stub, n1 (returns), a00100 (AGI), n02 (joint returns), a00200 (wages), a00300 (interest), a00900 (business income)
- **Table:** `irs_soi_income_by_zip`
- **Update Frequency:** Annual (2-year lag)
- **Years Available:** 1998-2021 (as of 2024)

### 2. Individual Income by County
- **URL Pattern:** `https://www.irs.gov/pub/irs-soi/YYINcountydata.csv` (YY = year)
- **Fields:** statefips, countyfips, state, countyname, agi_stub, n1 (returns), a00100 (AGI), n2 (wages), a00200 (wage amount)
- **Table:** `irs_soi_income_by_county`
- **Update Frequency:** Annual (2-year lag)
- **Years Available:** 1989-2021

### 3. Migration Data (County-to-County Flows)
- **URL Pattern:** `https://www.irs.gov/pub/irs-soi/YYcountyoutflow.xls` (outflow) and `YYcountyinflow.xls` (inflow)
- **Fields:** y1_statefips, y1_countyfips, y2_statefips, y2_countyfips, n1 (returns), n2 (exemptions), agi (aggregate AGI)
- **Table:** `irs_soi_county_migration`
- **Update Frequency:** Annual (2-year lag)
- **Years Available:** 2011-2021

### 4. Business Income (Sole Proprietorships by Industry)
- **URL Pattern:** `https://www.irs.gov/pub/irs-soi/YYindincsole.xls`
- **Fields:** year, naics_code, industry_desc, num_returns, business_receipts, net_income, deductions
- **Table:** `irs_soi_sole_proprietor_income`
- **Update Frequency:** Annual (2-year lag)
- **Years Available:** 1980-2021

## File Structure to Create

```
app/sources/irs_soi/
├── __init__.py          # Export IRSSOIAdapter
├── client.py            # IRSSOIClient for downloads (CSV/Excel)
├── metadata.py          # Schema definitions and table management
└── ingest.py            # Ingestion logic with CSV/Excel parsing

app/api/v1/irs_soi.py    # FastAPI router
```

## Implementation Requirements

### 1. `app/sources/irs_soi/__init__.py`
```python
from .client import IRSSOIClient
from .ingest import (
    ingest_income_by_zip,
    ingest_income_by_county,
    ingest_county_migration,
    ingest_sole_proprietor_income
)

class IRSSOIAdapter:
    """IRS Statistics of Income adapter."""
    def __init__(self):
        self.client = IRSSOIClient()
```

### 2. `app/sources/irs_soi/client.py`
- Create `IRSSOIClient` class with async httpx
- Implement `download_csv(url)` -> returns pandas DataFrame
- Implement `download_excel(url, sheet_name)` -> returns pandas DataFrame
- Add exponential backoff with jitter (3 retries, base delay 2s)
- Use semaphore for downloads (1 concurrent max to be respectful)
- Parse CSV with pandas: `pd.read_csv(BytesIO(content))`
- Parse Excel with pandas: `pd.read_excel(BytesIO(content), sheet_name=sheet_name)`
- Handle missing files gracefully (404 = data not available for that year)

### 3. `app/sources/irs_soi/metadata.py`
Define SQL schemas for each table:

**irs_soi_income_by_zip:**
```
- id: SERIAL PRIMARY KEY
- year: INTEGER NOT NULL
- zipcode: TEXT NOT NULL
- agi_stub: INTEGER (AGI bracket: 1=$1-$25k, 2=$25-$50k, 3=$50-$75k, 4=$75-$100k, 5=$100-$200k, 6=$200k+)
- n1: INTEGER (Number of returns)
- a00100: NUMERIC(20,2) (Adjusted Gross Income in thousands)
- n02: INTEGER (Number of joint returns)
- a00200: NUMERIC(20,2) (Salaries and wages in thousands)
- a00300: NUMERIC(20,2) (Taxable interest in thousands)
- a00900: NUMERIC(20,2) (Business or profession net income in thousands)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(year, zipcode, agi_stub)
```

**irs_soi_income_by_county:**
```
- id: SERIAL PRIMARY KEY
- year: INTEGER NOT NULL
- statefips: TEXT NOT NULL
- countyfips: TEXT NOT NULL
- state: TEXT
- countyname: TEXT
- agi_stub: INTEGER (AGI bracket)
- n1: INTEGER (Number of returns)
- a00100: NUMERIC(20,2) (Adjusted Gross Income in thousands)
- n2: INTEGER (Number of returns with wages)
- a00200: NUMERIC(20,2) (Salaries and wages in thousands)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(year, statefips, countyfips, agi_stub)
```

**irs_soi_county_migration:**
```
- id: SERIAL PRIMARY KEY
- year: INTEGER NOT NULL
- flow_type: TEXT NOT NULL (inflow or outflow)
- y1_statefips: TEXT NOT NULL (Origin state FIPS)
- y1_countyfips: TEXT NOT NULL (Origin county FIPS)
- y2_statefips: TEXT NOT NULL (Destination state FIPS)
- y2_countyfips: TEXT NOT NULL (Destination county FIPS)
- n1: INTEGER (Number of returns/households)
- n2: INTEGER (Number of exemptions/individuals)
- agi: NUMERIC(20,2) (Aggregate AGI in thousands)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(year, flow_type, y1_statefips, y1_countyfips, y2_statefips, y2_countyfips)
```

**irs_soi_sole_proprietor_income:**
```
- id: SERIAL PRIMARY KEY
- year: INTEGER NOT NULL
- naics_code: TEXT NOT NULL
- industry_desc: TEXT
- num_returns: INTEGER (Number of sole proprietor returns)
- business_receipts: NUMERIC(20,2) (Total receipts in thousands)
- net_income: NUMERIC(20,2) (Net income/loss in thousands)
- deductions: NUMERIC(20,2) (Total deductions in thousands)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(year, naics_code)
```

### 4. `app/sources/irs_soi/ingest.py`
For each dataset, implement:
- `ingest_income_by_zip(year, db)` -> int (rows inserted)
- `ingest_income_by_county(year, db)` -> int
- `ingest_county_migration(year, db)` -> int (processes both inflow and outflow)
- `ingest_sole_proprietor_income(year, db)` -> int

Each function should:
1. Create table if not exists (idempotent)
2. Construct download URL based on year
3. Download CSV/Excel file using client
4. Parse data with pandas
5. Clean data (handle null values, convert types)
6. Use parameterized INSERT with ON CONFLICT DO NOTHING
7. Return row count

**Key parsing notes:**
- ZIP code data: CSV format, straightforward
- County data: CSV format, straightforward
- Migration data: Excel format, TWO files per year (inflow + outflow)
- Business income: Excel format, may have multiple sheets (use first data sheet)
- Handle suppressed values (represented as "*" or "d") -> store as NULL

### 5. `app/api/v1/irs_soi.py`
Create FastAPI router with endpoints:
- `POST /api/v1/irs-soi/ingest/income-by-zip` - Ingest ZIP code income data
  - Query params: `year: int`
- `POST /api/v1/irs-soi/ingest/income-by-county` - Ingest county income data
  - Query params: `year: int`
- `POST /api/v1/irs-soi/ingest/county-migration` - Ingest migration data
  - Query params: `year: int`
- `POST /api/v1/irs-soi/ingest/sole-proprietor-income` - Ingest business income
  - Query params: `year: int`
- `POST /api/v1/irs-soi/ingest/backfill` - Backfill multiple years
  - Query params: `start_year: int`, `end_year: int`, `datasets: List[str]`

Each endpoint should:
- Create job in `ingestion_jobs` table with status "running"
- Call appropriate ingest function
- Update job status to "success" or "failed"
- Return job_id and row count

### 6. Update `app/main.py`
Add router import and registration:
```python
from app.api.v1 import irs_soi

app.include_router(irs_soi.router, prefix="/api/v1", tags=["irs-soi"])
```

### 7. Add Dependencies to `requirements.txt`
```
pandas>=2.0.0
openpyxl>=3.0.0  # For Excel file parsing
xlrd>=2.0.0      # For legacy XLS files
```

## Success Criteria
- [ ] All 4 tables created with proper schemas
- [ ] All 4 ingestion endpoints functional
- [ ] CSV parsing with pandas implemented correctly
- [ ] Excel parsing with pandas implemented correctly (handle multiple sheets)
- [ ] Migration data: both inflow and outflow files processed
- [ ] Handle suppressed values ("*", "d") -> store as NULL
- [ ] Rate limiting with semaphore (1 concurrent download)
- [ ] Exponential backoff on download errors
- [ ] Job tracking in `ingestion_jobs` table
- [ ] Parameterized SQL queries
- [ ] Idempotent ingestion (ON CONFLICT DO NOTHING)
- [ ] Test ZIP code ingest for 2021
- [ ] Test county ingest for 2021
- [ ] Test migration ingest for 2021
- [ ] Test business income ingest for 2021
- [ ] Update `docs/EXTERNAL_DATA_SOURCES.md` status to "✅ IMPLEMENTED"

## Testing Steps
1. Start service: `docker-compose up -d`
2. Test ZIP code income: `curl -X POST "http://localhost:8001/api/v1/irs-soi/ingest/income-by-zip?year=2021"`
3. Test county income: `curl -X POST "http://localhost:8001/api/v1/irs-soi/ingest/income-by-county?year=2021"`
4. Test migration: `curl -X POST "http://localhost:8001/api/v1/irs-soi/ingest/county-migration?year=2021"`
5. Test business income: `curl -X POST "http://localhost:8001/api/v1/irs-soi/ingest/sole-proprietor-income?year=2021"`
6. Verify data: 
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM irs_soi_income_by_zip WHERE year=2021;"
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM irs_soi_income_by_county WHERE year=2021;"
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM irs_soi_county_migration WHERE year=2021;"
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM irs_soi_sole_proprietor_income WHERE year=2021;"
   ```
7. Check jobs: `docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT * FROM ingestion_jobs WHERE source='irs_soi' ORDER BY created_at DESC LIMIT 5;"`

## Notes
- IRS SOI data is public domain
- No PII concerns (all data is aggregated)
- Data has a 2-year lag (2021 data published in 2023)
- ZIP code data: ~40K ZIP codes × 6 AGI brackets = ~240K rows per year
- County data: ~3,200 counties × 6 AGI brackets = ~19K rows per year
- Migration data: Large (hundreds of thousands of county-to-county flows)
- File naming conventions vary by dataset (check actual IRS URLs)
- Some files use 2-digit years (21), others use 4-digit (2021) - be flexible
- Suppressed values (for privacy): "*" means data suppressed, "d" means withheld to avoid disclosure
- Consider implementing backfill endpoint to ingest multiple years at once
- Historical data available back to 1998 for most datasets
- All monetary values are in THOUSANDS of dollars
- Be prepared for large files (migration data can be 50+ MB)
