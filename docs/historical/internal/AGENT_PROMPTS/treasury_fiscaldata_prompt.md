# Agent Task: Implement Treasury FiscalData Ingestion

## Objective
Implement a data ingestion adapter for the U.S. Treasury's Fiscal Data API to collect federal debt, revenue, spending, and auction data.

## Project Rules
**CRITICAL:** Read and follow `RULES.md` in the project root. Key requirements:
- Use plugin architecture: all code in `app/sources/treasury/`
- Implement bounded concurrency with `asyncio.Semaphore`
- Use typed database columns (INT, NUMERIC, TEXT), NOT JSON blobs
- All ingestion must be tracked in `ingestion_jobs` table
- Parameterized SQL queries only
- Exponential backoff with jitter for API errors
- Respect rate limits (default: 10 req/sec max)

## Data Source Information
- **API:** https://fiscaldata.treasury.gov/api-documentation/
- **Rate Limits:** No documented hard limit, but be conservative (10 req/sec)
- **Authentication:** None required (public API)
- **Format:** JSON REST API with pagination
- **License:** Public domain (U.S. government data)

## API Endpoints to Implement

### 1. Federal Debt (Public Debt Outstanding)
- **Endpoint:** `/v1/accounting/dts/public_debt_outstanding`
- **Fields:** record_date, close_today_bal, debt_held_public_amt, intragov_hold_amt
- **Table:** `treasury_public_debt`
- **Update Frequency:** Daily

### 2. Treasury Interest Rates (Daily Treasury Yield Curve)
- **Endpoint:** `/v1/accounting/od/avg_interest_rates`
- **Fields:** record_date, security_desc, avg_interest_rate_amt
- **Table:** `treasury_interest_rates`
- **Update Frequency:** Daily

### 3. Federal Revenue & Spending (Monthly Treasury Statement)
- **Endpoint:** `/v1/accounting/mts/mts_table_5`
- **Fields:** record_date, classification_desc, current_fytd_rcpt_outly_amt, prior_fytd_rcpt_outly_amt
- **Table:** `treasury_revenue_spending`
- **Update Frequency:** Monthly

### 4. Treasury Auction Results
- **Endpoint:** `/v2/accounting/od/auctions_query`
- **Fields:** auction_date, security_term, security_type, high_yield, issue_date, maturity_date, total_accepted
- **Table:** `treasury_auction_results`
- **Update Frequency:** Weekly

## File Structure to Create

```
app/sources/treasury/
├── __init__.py          # Export TreasuryAdapter
├── client.py            # TreasuryClient for API calls
├── metadata.py          # Schema definitions and table management
└── ingest.py            # Ingestion logic

app/api/v1/treasury.py   # FastAPI router
```

## Implementation Requirements

### 1. `app/sources/treasury/__init__.py`
```python
from .client import TreasuryClient
from .ingest import (
    ingest_public_debt,
    ingest_interest_rates,
    ingest_revenue_spending,
    ingest_auction_results
)

class TreasuryAdapter:
    """Treasury FiscalData adapter."""
    def __init__(self):
        self.client = TreasuryClient()
```

### 2. `app/sources/treasury/client.py`
- Create `TreasuryClient` class with async httpx
- Base URL: `https://fiscaldata.treasury.gov/services/api/fiscal_service`
- Implement pagination handling (use `page[number]` and `page[size]` params)
- Add exponential backoff with jitter (3 retries, base delay 1s)
- Respect rate limits using semaphore (10 concurrent max)
- Parse response: `response.json()["data"]`

### 3. `app/sources/treasury/metadata.py`
Define SQL schemas for each table:

**treasury_public_debt:**
```
- id: SERIAL PRIMARY KEY
- record_date: DATE NOT NULL UNIQUE
- close_today_bal: NUMERIC(20,2)
- debt_held_public_amt: NUMERIC(20,2)
- intragov_hold_amt: NUMERIC(20,2)
- created_at: TIMESTAMP DEFAULT NOW()
```

**treasury_interest_rates:**
```
- id: SERIAL PRIMARY KEY
- record_date: DATE NOT NULL
- security_desc: TEXT NOT NULL
- avg_interest_rate_amt: NUMERIC(8,4)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(record_date, security_desc)
```

**treasury_revenue_spending:**
```
- id: SERIAL PRIMARY KEY
- record_date: DATE NOT NULL
- classification_desc: TEXT NOT NULL
- current_fytd_rcpt_outly_amt: NUMERIC(20,2)
- prior_fytd_rcpt_outly_amt: NUMERIC(20,2)
- created_at: TIMESTAMP DEFAULT NOW()
- UNIQUE(record_date, classification_desc)
```

**treasury_auction_results:**
```
- id: SERIAL PRIMARY KEY
- auction_date: DATE NOT NULL
- security_term: TEXT
- security_type: TEXT
- high_yield: NUMERIC(8,4)
- issue_date: DATE
- maturity_date: DATE
- total_accepted: NUMERIC(20,2)
- created_at: TIMESTAMP DEFAULT NOW()
```

### 4. `app/sources/treasury/ingest.py`
For each dataset, implement:
- `ingest_public_debt(start_date, end_date, db)` -> int (rows inserted)
- `ingest_interest_rates(start_date, end_date, db)` -> int
- `ingest_revenue_spending(start_date, end_date, db)` -> int
- `ingest_auction_results(start_date, end_date, db)` -> int

Each function should:
1. Create table if not exists (idempotent)
2. Query API with date filters
3. Parse response data
4. Use parameterized INSERT with ON CONFLICT DO NOTHING (for idempotency)
5. Return row count

### 5. `app/api/v1/treasury.py`
Create FastAPI router with endpoints:
- `POST /api/v1/treasury/ingest/public-debt` - Ingest public debt data
- `POST /api/v1/treasury/ingest/interest-rates` - Ingest interest rates
- `POST /api/v1/treasury/ingest/revenue-spending` - Ingest revenue/spending
- `POST /api/v1/treasury/ingest/auction-results` - Ingest auction results

Each endpoint should:
- Accept `start_date` and `end_date` as query params
- Create job in `ingestion_jobs` table with status "running"
- Call appropriate ingest function
- Update job status to "success" or "failed"
- Return job_id and row count

### 6. Update `app/main.py`
Add router import and registration:
```python
from app.api.v1 import treasury

app.include_router(treasury.router, prefix="/api/v1", tags=["treasury"])
```

## Query Parameters
All API calls should use:
- `fields`: Comma-separated list of fields to return
- `filter`: Date range filter like `record_date:gte:2020-01-01,record_date:lte:2023-12-31`
- `page[number]`: Page number (starts at 1)
- `page[size]`: Records per page (max 10000)
- `sort`: Sort field (e.g., `-record_date` for descending)

Example: `/v1/accounting/dts/public_debt_outstanding?fields=record_date,close_today_bal&filter=record_date:gte:2023-01-01&page[size]=1000`

## Success Criteria
- [ ] All 4 tables created with proper schemas
- [ ] All 4 ingestion endpoints functional
- [ ] Pagination handling implemented correctly
- [ ] Rate limiting with semaphore (max 10 concurrent)
- [ ] Exponential backoff on errors
- [ ] Job tracking in `ingestion_jobs` table
- [ ] Parameterized SQL queries
- [ ] Idempotent ingestion (ON CONFLICT DO NOTHING)
- [ ] Test with date range: 2020-01-01 to 2024-12-31
- [ ] Update `docs/EXTERNAL_DATA_SOURCES.md` status to "✅ IMPLEMENTED"

## Testing Steps
1. Start service: `docker-compose up -d`
2. Test public debt: `curl -X POST "http://localhost:8001/api/v1/treasury/ingest/public-debt?start_date=2023-01-01&end_date=2023-12-31"`
3. Test interest rates: `curl -X POST "http://localhost:8001/api/v1/treasury/ingest/interest-rates?start_date=2023-01-01&end_date=2023-12-31"`
4. Verify data: `docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM treasury_public_debt;"`
5. Check jobs: `docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT * FROM ingestion_jobs WHERE source='treasury' ORDER BY created_at DESC LIMIT 5;"`

## Notes
- Treasury data is public domain
- No PII concerns
- Historical data available back to 1980s for most series
- Some tables are large (millions of rows for daily data over decades)
- Consider implementing incremental updates (only fetch recent data after initial backfill)
