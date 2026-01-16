# Plan T22: Company Data Enrichment

**Task ID:** T22
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-16

---

## Goal

Enrich portfolio company data with financials, funding rounds, employee counts, and industry classification. Enable automated and batch enrichment of company records.

---

## Why This Matters

1. **Investment Analysis**: Enriched financial data enables better portfolio analysis
2. **Due Diligence**: Funding history and employee trends inform investment decisions
3. **Portfolio Monitoring**: Track company status changes (IPO, acquired, bankrupt)
4. **Data Completeness**: Fill gaps in portfolio company records

---

## Data Sources

### Primary Sources
1. **SEC EDGAR** - Public company financials (10-K, 10-Q filings)
2. **Crunchbase** (simulated) - Funding rounds and valuations
3. **LinkedIn** (simulated) - Employee counts
4. **Industry Classifications** - SIC/NAICS codes

### Note on External APIs
For MVP, we'll create a flexible enrichment framework with:
- Placeholder/mock implementations for external APIs
- Real SEC EDGAR integration (free, public API)
- Easy extensibility for future API integrations

---

## Design

### Database Schema Addition

```sql
-- Company enrichment data (extends portfolio_companies)
CREATE TABLE IF NOT EXISTS company_enrichment (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,

    -- Financials (from SEC EDGAR)
    sec_cik VARCHAR(20),
    latest_revenue BIGINT,
    latest_assets BIGINT,
    latest_filing_date DATE,

    -- Funding (from Crunchbase or similar)
    total_funding BIGINT,
    last_funding_round VARCHAR(50),
    last_funding_amount BIGINT,
    last_funding_date DATE,
    valuation BIGINT,
    valuation_date DATE,

    -- Employee data
    employee_count INTEGER,
    employee_count_date DATE,
    employee_growth_yoy FLOAT,

    -- Classification
    industry VARCHAR(100),
    sector VARCHAR(100),
    sic_code VARCHAR(10),
    naics_code VARCHAR(10),

    -- Status
    company_status VARCHAR(50) DEFAULT 'active',  -- active, acquired, ipo, bankrupt, closed
    status_date DATE,
    acquirer_name VARCHAR(255),
    ipo_date DATE,
    stock_symbol VARCHAR(20),

    -- Metadata
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    enrichment_source VARCHAR(100),
    confidence_score FLOAT DEFAULT 0.0,

    UNIQUE(company_name)
);

-- Enrichment job tracking
CREATE TABLE IF NOT EXISTS enrichment_jobs (
    id SERIAL PRIMARY KEY,
    job_type VARCHAR(50) NOT NULL,  -- single, batch
    company_name VARCHAR(255),
    status VARCHAR(20) DEFAULT 'pending',  -- pending, running, completed, failed
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    results JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/enrichment/company/{company_name}` | POST | Trigger enrichment for a company |
| `/enrichment/company/{company_name}/status` | GET | Get enrichment job status |
| `/enrichment/batch` | POST | Trigger batch enrichment |
| `/companies/{company_name}/enriched` | GET | Get enriched company data |
| `/companies/enriched` | GET | List all enriched companies |

### Enrichment Engine

```python
class CompanyEnrichmentEngine:
    """Company data enrichment engine."""

    async def enrich_company(self, company_name: str) -> Dict:
        """Run all enrichment sources for a company."""

    async def enrich_from_sec(self, company_name: str) -> Dict:
        """Fetch financials from SEC EDGAR."""

    async def enrich_funding(self, company_name: str) -> Dict:
        """Fetch funding data (placeholder for Crunchbase)."""

    async def enrich_employees(self, company_name: str) -> Dict:
        """Fetch employee data (placeholder for LinkedIn)."""

    async def classify_industry(self, company_name: str) -> Dict:
        """Classify company industry/sector."""

    async def check_status(self, company_name: str) -> Dict:
        """Check company status (IPO, acquired, etc)."""
```

---

## Implementation

### 1. `app/enrichment/__init__.py`
Package initialization.

### 2. `app/enrichment/company.py`
Main enrichment engine with all data source integrations.

### 3. `app/enrichment/sec_edgar.py`
SEC EDGAR API client for public company filings.

### 4. `app/api/v1/enrichment.py`
FastAPI router with enrichment endpoints.

### 5. Database migrations
Add new tables for enrichment data and job tracking.

---

## Response Formats

### Enriched Company Data
```json
{
  "company_name": "Stripe",
  "financials": {
    "revenue": null,
    "assets": null,
    "source": "private_company"
  },
  "funding": {
    "total_funding": 8700000000,
    "last_round": "Series I",
    "last_amount": 600000000,
    "last_date": "2023-03-15",
    "valuation": 50000000000
  },
  "employees": {
    "count": 8000,
    "date": "2024-01-01",
    "growth_yoy": 0.15
  },
  "classification": {
    "industry": "Financial Technology",
    "sector": "Technology",
    "sic_code": "7372",
    "naics_code": "522320"
  },
  "status": {
    "current": "active",
    "ipo_date": null,
    "acquirer": null
  },
  "enriched_at": "2026-01-16T10:30:00Z",
  "confidence_score": 0.85
}
```

### Enrichment Job Status
```json
{
  "job_id": 123,
  "company_name": "Stripe",
  "status": "completed",
  "started_at": "2026-01-16T10:30:00Z",
  "completed_at": "2026-01-16T10:30:05Z",
  "results": {
    "sec_edgar": "skipped_private",
    "funding": "success",
    "employees": "success",
    "classification": "success",
    "status": "success"
  }
}
```

---

## Files to Create

1. `app/enrichment/__init__.py` - Package init
2. `app/enrichment/company.py` - Main enrichment engine
3. `app/enrichment/sec_edgar.py` - SEC EDGAR API client
4. `app/api/v1/enrichment.py` - API endpoints

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Test endpoints:
   - `POST /api/v1/enrichment/company/Stripe` - Trigger enrichment
   - `GET /api/v1/enrichment/company/Stripe/status` - Check status
   - `GET /api/v1/companies/Stripe/enriched` - Get enriched data
   - `POST /api/v1/enrichment/batch` with `{"companies": ["Stripe", "SpaceX"]}`
   - `GET /api/v1/companies/enriched?limit=10` - List enriched

---

## Success Criteria

- [ ] Enrichment engine runs multiple data sources
- [ ] SEC EDGAR integration fetches real data for public companies
- [ ] Job tracking shows enrichment progress
- [ ] Enriched data persists in database
- [ ] Batch enrichment processes multiple companies
- [ ] API returns properly formatted enriched data

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Created `app/enrichment/` package with SEC EDGAR client and main enrichment engine
- Tables auto-created: `company_enrichment` and `enrichment_jobs`
- SEC EDGAR integration fetches from data.sec.gov with company tickers lookup
- Funding and employee data are placeholders (would need Crunchbase/LinkedIn)
- Industry classification based on keyword matching
- All 5 endpoints working and tested

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
