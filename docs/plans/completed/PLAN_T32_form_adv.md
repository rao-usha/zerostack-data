# Plan T32: SEC Form ADV Data

**Task ID:** T32
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-18

---

## Goal

Ingest SEC Form ADV data to track registered investment advisers, their AUM, clients, and regulatory information.

---

## Why This Matters

1. **Official AUM Data**: Form ADV contains SEC-verified assets under management
2. **Adviser Intelligence**: Client types, fee structures, investment strategies
3. **Key Personnel**: Identify principals and ownership structure
4. **Regulatory History**: Disciplinary events and disclosures
5. **LP Research**: Cross-reference with LP database for enhanced profiles

---

## SEC Form ADV Overview

### What is Form ADV?
- Required registration form for SEC-registered investment advisers
- Part 1: Firm info, AUM, clients, ownership (structured data)
- Part 2: Brochure with narrative disclosures (PDF)
- Part 3: Client Relationship Summary (CRS)

### Data Sources

1. **IAPD Website**: https://adviserinfo.sec.gov/
   - Individual adviser search and PDF downloads
   - Compilation reports available

2. **SEC Form ADV Data Files**: https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data
   - Quarterly CSV exports of all advisers
   - Multiple related tables (need to join)

3. **SEC EDGAR**: Form ADV-related filings

### Key Data Fields

| Category | Fields |
|----------|--------|
| **Identification** | CRD Number, SEC File Number, Legal Name, DBA |
| **Location** | Address, State, Country |
| **AUM** | Regulatory AUM, Discretionary AUM, Non-Discretionary |
| **Clients** | Number of clients, Client types (individuals, funds, pensions) |
| **Employees** | Total employees, Investment personnel |
| **Fees** | Fee types (hourly, fixed, % of AUM, performance) |
| **Custody** | Has custody of client assets |
| **Regulatory** | Registration status, Exam dates, Disciplinary events |

---

## Technical Design

### Database Schema

```sql
-- Investment Advisers from Form ADV
CREATE TABLE IF NOT EXISTS form_adv_advisers (
    id SERIAL PRIMARY KEY,
    crd_number VARCHAR(20) NOT NULL UNIQUE,
    sec_number VARCHAR(20),

    -- Identification
    legal_name VARCHAR(500) NOT NULL,
    dba_name VARCHAR(500),
    website VARCHAR(500),

    -- Location
    main_office_address TEXT,
    main_office_city VARCHAR(100),
    main_office_state VARCHAR(10),
    main_office_country VARCHAR(50),
    main_office_zip VARCHAR(20),

    -- AUM & Clients
    regulatory_aum BIGINT,
    discretionary_aum BIGINT,
    non_discretionary_aum BIGINT,
    total_accounts INTEGER,
    discretionary_accounts INTEGER,

    -- Client Types (percentages)
    pct_individuals INTEGER,
    pct_high_net_worth INTEGER,
    pct_banking_institutions INTEGER,
    pct_investment_companies INTEGER,
    pct_pension_plans INTEGER,
    pct_pooled_investment_vehicles INTEGER,
    pct_charitable_organizations INTEGER,
    pct_corporations INTEGER,
    pct_state_municipal INTEGER,
    pct_other INTEGER,

    -- Employees
    total_employees INTEGER,
    employees_investment_advisory INTEGER,
    employees_registered_reps INTEGER,

    -- Registration
    sec_registered BOOLEAN DEFAULT TRUE,
    registration_date DATE,
    fiscal_year_end VARCHAR(10),

    -- Firm Details
    form_of_organization VARCHAR(50),
    country_of_organization VARCHAR(50),
    state_of_organization VARCHAR(50),

    -- Custody
    has_custody BOOLEAN,
    custody_client_cash BOOLEAN,
    custody_client_securities BOOLEAN,

    -- Fees (JSONB for flexibility)
    fee_types JSONB,
    compensation_types JSONB,

    -- Regulatory
    has_disciplinary_events BOOLEAN,
    disciplinary_details JSONB,

    -- Metadata
    filing_date DATE,
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_adv_crd ON form_adv_advisers(crd_number);
CREATE INDEX idx_adv_name ON form_adv_advisers(legal_name);
CREATE INDEX idx_adv_aum ON form_adv_advisers(regulatory_aum);
CREATE INDEX idx_adv_state ON form_adv_advisers(main_office_state);
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/form-adv/search` | GET | Search advisers by name, state, AUM range |
| `/form-adv/adviser/{crd}` | GET | Get adviser details by CRD number |
| `/form-adv/aum-rankings` | GET | Top advisers by AUM |
| `/form-adv/stats` | GET | Aggregate statistics |
| `/form-adv/by-state` | GET | Advisers grouped by state |
| `/form-adv/ingest` | POST | Trigger data ingestion |

### Data Ingestion Strategy

1. **Primary Source**: SEC quarterly CSV files
   - Download from SEC FOIA page
   - Parse and load into PostgreSQL
   - Run quarterly updates

2. **Enrichment**: IAPD API for additional details
   - Individual adviser lookups
   - Real-time data validation

---

## Response Formats

### Adviser Detail
```json
{
  "crd_number": "123456",
  "sec_number": "801-12345",
  "legal_name": "Example Capital Advisors LLC",
  "dba_name": "Example Capital",
  "website": "https://examplecapital.com",
  "location": {
    "address": "100 Main Street, Suite 500",
    "city": "New York",
    "state": "NY",
    "country": "United States",
    "zip": "10001"
  },
  "aum": {
    "regulatory": 5000000000,
    "discretionary": 4500000000,
    "non_discretionary": 500000000
  },
  "clients": {
    "total_accounts": 150,
    "discretionary_accounts": 140,
    "breakdown": {
      "individuals": 10,
      "high_net_worth": 25,
      "pension_plans": 30,
      "pooled_investment_vehicles": 35
    }
  },
  "employees": {
    "total": 45,
    "investment_advisory": 20
  },
  "registration": {
    "sec_registered": true,
    "registration_date": "2005-03-15",
    "form_of_organization": "Limited Liability Company"
  },
  "fees": ["Percentage of AUM", "Performance-based"],
  "regulatory": {
    "has_disciplinary_events": false,
    "has_custody": true
  }
}
```

### AUM Rankings
```json
{
  "rankings": [
    {
      "rank": 1,
      "crd_number": "123456",
      "legal_name": "BlackRock Advisors",
      "regulatory_aum": 10000000000000,
      "state": "NY",
      "total_accounts": 5000
    }
  ],
  "total_advisers": 15000,
  "total_aum": 120000000000000
}
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/sources/sec_form_adv/__init__.py` | Module init |
| `app/sources/sec_form_adv/client.py` | IAPD/SEC data access |
| `app/sources/sec_form_adv/parser.py` | CSV/data parser |
| `app/sources/sec_form_adv/ingest.py` | Ingestion service |
| `app/api/v1/form_adv.py` | API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register form_adv router |

---

## Implementation Steps

1. Create `app/sources/sec_form_adv/` directory
2. Implement client for SEC data file download
3. Implement CSV parser for quarterly files
4. Implement ingestion service with database storage
5. Create API endpoints
6. Register router in main.py
7. Test with sample data

---

## Test Plan

```bash
# Search advisers
curl -s "http://localhost:8001/api/v1/form-adv/search?state=NY&min_aum=1000000000" | python -m json.tool

# Get specific adviser
curl -s "http://localhost:8001/api/v1/form-adv/adviser/123456" | python -m json.tool

# AUM rankings
curl -s "http://localhost:8001/api/v1/form-adv/aum-rankings?limit=20" | python -m json.tool

# Statistics
curl -s "http://localhost:8001/api/v1/form-adv/stats" | python -m json.tool

# By state
curl -s "http://localhost:8001/api/v1/form-adv/by-state" | python -m json.tool
```

---

## Success Criteria

- [ ] Can search advisers by name, state, AUM range
- [ ] Parses SEC CSV files correctly
- [ ] Stores adviser data in PostgreSQL
- [ ] AUM rankings work
- [ ] Statistics endpoint shows aggregates
- [ ] All 6 endpoints return properly formatted data

---

## Approval

- [x] **Approved by user** (2026-01-18)

---

*Plan created: 2026-01-18*

Sources:
- [SEC Form ADV Data](https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data)
- [IAPD Website](https://adviserinfo.sec.gov/)
- [SEC Investment Adviser Information](https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers)
