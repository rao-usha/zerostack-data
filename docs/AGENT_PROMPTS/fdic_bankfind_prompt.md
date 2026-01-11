# Agent Task: Implement FDIC BankFind Ingestion

## Objective
Implement a data ingestion adapter for the FDIC BankFind API to collect bank financials, institution demographics, failed banks, and summary of deposits.

## Project Rules
**CRITICAL:** Read and follow `RULES.md` in the project root. Key requirements:
- Use plugin architecture: all code in `app/sources/fdic/`
- Implement bounded concurrency with `asyncio.Semaphore`
- Use typed database columns (INT, NUMERIC, TEXT), NOT JSON blobs
- All ingestion must be tracked in `ingestion_jobs` table
- Parameterized SQL queries only
- Exponential backoff with jitter for API errors
- Respect rate limits (default: 10 req/sec max)

## Data Source Information
- **API:** https://banks.data.fdic.gov/docs/
- **Rate Limits:** No documented hard limit, but be conservative (10 req/sec)
- **Authentication:** None required (public API)
- **Format:** JSON REST API with pagination
- **License:** Public domain (U.S. government data)

## API Endpoints to Implement

### 1. Institution Data (Bank Demographics)
- **Endpoint:** `/api/institutions`
- **Fields:** CERT, NAME, CITY, STNAME, ZIP, ADDRESS, DATEUPDT, ACTIVE, STALP, ASSET, COUNTY, CBSA, DEPSUM
- **Table:** `fdic_institutions`
- **Update Frequency:** Quarterly

### 2. Bank Financials (Call Reports)
- **Endpoint:** `/api/financials`
- **Fields:** CERT, REPDTE, ASSET, DEP, NETINC, ROA, ROE, LNLSNET, INTINC, NONII, EQTOT
- **Table:** `fdic_bank_financials`
- **Update Frequency:** Quarterly

### 3. Failed Banks
- **Endpoint:** `/api/failures`
- **Fields:** CERT, NAME, CITY, STNAME, FAILDATE, QBFDEP, RESTYPE1, CHCLASS1, COST
- **Table:** `fdic_failed_banks`
- **Update Frequency:** As events occur

### 4. Summary of Deposits (Branch-Level)
- **Endpoint:** `/api/summary`
- **Fields:** CERT, YEAR, BRNUM, DEPSUMBR, BRSERTYP, ADDRESS, CITY, STNAME, ZIP, COUNTY
- **Table:** `fdic_summary_of_deposits`
- **Update Frequency:** Annual (June 30)

## File Structure to Create

```
app/sources/fdic/
├── __init__.py          # Export FDICAdapter
├── client.py            # FDICClient for API calls
├── metadata.py          # Schema definitions and table management
└── ingest.py            # Ingestion logic

app/api/v1/fdic.py       # FastAPI router
```

## Implementation Requirements

### 1. `app/sources/fdic/__init__.py`
```python
from .client import FDICClient
from .ingest import (
    ingest_institutions,
    ingest_bank_financials,
    ingest_failed_banks,
    ingest_summary_of_deposits
)

class FDICAdapter:
    """FDIC BankFind adapter."""
    def __init__(self):
        self.client = FDICClient()
```

### 2. `app/sources/fdic/client.py`
- Create `FDICClient` class with async httpx
- Base URL: `https://banks.data.fdic.gov`
- Implement pagination handling (use `limit` and `offset` params, max 10000 per page)
- Add exponential backoff with jitter (3 retries, base delay 1s)
- Respect rate limits using semaphore (10 concurrent max)
- Parse response: `response.json()["data"]`
- Handle filters: `filters=ACTIVE:1` or `filters=FAILDATE:[2020-01-01 TO 2023-12-31]`

### 3. `app/sources/fdic/metadata.py`
Define SQL schemas for each table:

**fdic_institutions:**
```
- id: SERIAL PRIMARY KEY
- cert: INTEGER NOT NULL UNIQUE (FDIC Certificate Number)
- name: TEXT NOT NULL
- city: TEXT
- stname: TEXT (Full state name)
- stalp: TEXT (State abbreviation)
- zip: TEXT
- address: TEXT
- county: TEXT
- cbsa: TEXT (Core-Based Statistical Area)
- asset: NUMERIC(20,2) (Total assets in thousands)
- depsum: NUMERIC(20,2) (Total deposits in thousands)
- active: SMALLINT (1=active, 0=inactive)
- dateupdt: DATE (Last update date)
- created_at: TIMESTAMP DEFAULT NOW()
```

**fdic_bank_financials:**
```
- id: SERIAL PRIMARY KEY
- cert: INTEGER NOT NULL (FDIC Certificate Number)
- repdte: DATE NOT NULL (Reporting date)
- asset: NUMERIC(20,2) (Total assets in thousands)
- dep: NUMERIC(20,2) (Total deposits in thousands)
- netinc: NUMERIC(20,2) (Net income in thousands)
- roa: NUMERIC(8,4) (Return on Assets %)
- roe: NUMERIC(8,4) (Return on Equity %)
- lnlsnet: NUMERIC(20,2) (Net loan losses in thousands)
- intinc: NUMERIC(20,2) (Interest income in thousands)
- nonii: NUMERIC(20,2) (Non-interest income in thousands)
- eqtot: NUMERIC(20,2) (Total equity capital in thousands)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(cert, repdte)
```

**fdic_failed_banks:**
```
- id: SERIAL PRIMARY KEY
- cert: INTEGER NOT NULL
- name: TEXT NOT NULL
- city: TEXT
- stname: TEXT
- faildate: DATE NOT NULL
- qbfdep: NUMERIC(20,2) (Deposits at failure in thousands)
- restype1: TEXT (Resolution type)
- chclass1: TEXT (Charter class)
- cost: NUMERIC(20,2) (Estimated cost to DIF in millions)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(cert, faildate)
```

**fdic_summary_of_deposits:**
```
- id: SERIAL PRIMARY KEY
- cert: INTEGER NOT NULL (FDIC Certificate Number)
- year: INTEGER NOT NULL (Survey year)
- brnum: INTEGER (Branch number)
- depsumbr: NUMERIC(20,2) (Branch deposits in thousands)
- brsertyp: TEXT (Branch service type: Full Service, Limited Service, etc.)
- address: TEXT
- city: TEXT
- stname: TEXT
- zip: TEXT
- county: TEXT
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(cert, year, brnum)
```

### 4. `app/sources/fdic/ingest.py`
For each dataset, implement:
- `ingest_institutions(active_only, db)` -> int (rows inserted)
- `ingest_bank_financials(start_date, end_date, db)` -> int
- `ingest_failed_banks(start_date, end_date, db)` -> int
- `ingest_summary_of_deposits(year, db)` -> int

Each function should:
1. Create table if not exists (idempotent)
2. Query API with appropriate filters
3. Handle pagination (loop until no more data)
4. Parse response data
5. Use parameterized INSERT with ON CONFLICT DO NOTHING or DO UPDATE
6. Return row count

### 5. `app/api/v1/fdic.py`
Create FastAPI router with endpoints:
- `POST /api/v1/fdic/ingest/institutions` - Ingest institution demographics
  - Query params: `active_only: bool = True`
- `POST /api/v1/fdic/ingest/financials` - Ingest bank financials
  - Query params: `start_date: str`, `end_date: str`
- `POST /api/v1/fdic/ingest/failed-banks` - Ingest failed banks
  - Query params: `start_date: str`, `end_date: str` (optional, default to all)
- `POST /api/v1/fdic/ingest/summary-of-deposits` - Ingest SOD data
  - Query params: `year: int`

Each endpoint should:
- Create job in `ingestion_jobs` table with status "running"
- Call appropriate ingest function
- Update job status to "success" or "failed"
- Return job_id and row count

### 6. Update `app/main.py`
Add router import and registration:
```python
from app.api.v1 import fdic

app.include_router(fdic.router, prefix="/api/v1", tags=["fdic"])
```

## Query Parameters
All API calls should use:
- `fields`: Comma-separated list of fields to return
- `filters`: Filter expressions like `ACTIVE:1` or `FAILDATE:[2020-01-01 TO 2023-12-31]`
- `limit`: Records per page (max 10000)
- `offset`: Starting record number
- `sort_by`: Field to sort by (e.g., `NAME`)
- `sort_order`: `ASC` or `DESC`

Example: `/api/institutions?fields=CERT,NAME,CITY,STNAME,ASSET&filters=ACTIVE:1&limit=10000&offset=0`

## Success Criteria
- [ ] All 4 tables created with proper schemas
- [ ] All 4 ingestion endpoints functional
- [ ] Pagination handling implemented correctly (loop until all data fetched)
- [ ] Rate limiting with semaphore (max 10 concurrent)
- [ ] Exponential backoff on errors
- [ ] Job tracking in `ingestion_jobs` table
- [ ] Parameterized SQL queries
- [ ] Idempotent ingestion (ON CONFLICT handling)
- [ ] Test institution ingest (should get ~4,500 active banks)
- [ ] Test financials ingest for 2023
- [ ] Test failed banks (all time)
- [ ] Test SOD for 2023
- [ ] Update `docs/EXTERNAL_DATA_SOURCES.md` status to "✅ IMPLEMENTED"

## Testing Steps
1. Start service: `docker-compose up -d`
2. Test institutions: `curl -X POST "http://localhost:8001/api/v1/fdic/ingest/institutions?active_only=true"`
3. Test financials: `curl -X POST "http://localhost:8001/api/v1/fdic/ingest/financials?start_date=2023-01-01&end_date=2023-12-31"`
4. Test failed banks: `curl -X POST "http://localhost:8001/api/v1/fdic/ingest/failed-banks"`
5. Test SOD: `curl -X POST "http://localhost:8001/api/v1/fdic/ingest/summary-of-deposits?year=2023"`
6. Verify data: `docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM fdic_institutions WHERE active=1;"`
7. Check jobs: `docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT * FROM ingestion_jobs WHERE source='fdic' ORDER BY created_at DESC LIMIT 5;"`

## Notes
- FDIC data is public domain
- No PII concerns (only institutional data)
- CERT (Certificate Number) is the primary identifier for banks
- Financials are reported quarterly (March 31, June 30, Sept 30, Dec 31)
- Summary of Deposits is annual (as of June 30 each year)
- Failed banks data goes back to 2000
- Be careful with pagination - some queries return 100K+ records
- Consider implementing incremental updates (only fetch recent quarters after initial backfill)
- Financial values are in THOUSANDS of dollars (except COST which is in MILLIONS)
