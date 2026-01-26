# Plan T31: SEC Form D Filings

**Task ID:** T31
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-18

---

## Goal

Ingest SEC Form D filings to track private placement offerings, fund formations, and unregistered securities offerings.

---

## Why This Matters

1. **Early Funding Signals**: Form D is filed within 15 days of first sale - reveals funding before press releases
2. **Fund Formation Tracking**: New VC/PE funds must file Form D when raising capital
3. **Investor Intelligence**: Shows which exemptions are used (Reg D 506(b), 506(c), etc.)
4. **Deal Flow Discovery**: Identify companies raising private capital in specific sectors

---

## SEC Form D Overview

### What is Form D?
- Filed when companies sell securities without SEC registration
- Required under Regulation D (Rule 504, 505, 506)
- Includes offering amount, investor types, and exemption claimed
- Amendments (D/A) filed for material changes

### Data Available
- **Issuer**: Name, CIK, address, industry, year of incorporation
- **Offering**: Amount sought, amount sold, minimum investment
- **Securities**: Type (equity, debt, pooled investment fund interests)
- **Exemptions**: Rule 506(b), 506(c), Rule 504, etc.
- **Investors**: Number of accredited/non-accredited investors
- **Sales Compensation**: Broker/finder fees
- **Related Persons**: Directors, executives, promoters

---

## Technical Design

### SEC EDGAR Access

Form D filings are accessible via:
1. **Full-text search API**: `https://efts.sec.gov/LATEST/search-index`
2. **Company submissions**: `https://data.sec.gov/submissions/CIK{cik}.json`
3. **Direct filing access**: `https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/primary_doc.xml`

### Database Schema

```sql
-- Form D filings
CREATE TABLE IF NOT EXISTS form_d_filings (
    id SERIAL PRIMARY KEY,
    accession_number VARCHAR(25) NOT NULL UNIQUE,
    cik VARCHAR(10) NOT NULL,

    -- Filing info
    submission_type VARCHAR(10) NOT NULL,  -- D, D/A
    filed_at TIMESTAMP NOT NULL,

    -- Issuer info (Item 1 & 2)
    issuer_name VARCHAR(500) NOT NULL,
    issuer_street VARCHAR(500),
    issuer_city VARCHAR(100),
    issuer_state VARCHAR(10),
    issuer_zip VARCHAR(20),
    issuer_phone VARCHAR(50),
    entity_type VARCHAR(50),  -- Corporation, LLC, LP, etc.
    jurisdiction VARCHAR(100),
    year_of_incorporation INTEGER,

    -- Industry (Item 3)
    industry_group VARCHAR(100),

    -- Issuer Size (Item 4)
    revenue_range VARCHAR(50),

    -- Related Persons (Item 3) - stored as JSONB
    related_persons JSONB,

    -- Offering Info (Items 5-9)
    federal_exemptions JSONB,  -- ["Rule 506(b)", "Rule 506(c)"]
    date_of_first_sale DATE,
    more_than_one_year BOOLEAN,
    is_equity BOOLEAN,
    is_debt BOOLEAN,
    is_option BOOLEAN,
    is_security_to_be_acquired BOOLEAN,
    is_pooled_investment_fund BOOLEAN,
    is_business_combination BOOLEAN,

    -- Amount (Item 13)
    total_offering_amount BIGINT,
    total_amount_sold BIGINT,
    total_remaining BIGINT,

    -- Investors (Item 14)
    total_number_already_invested INTEGER,
    accredited_investors INTEGER,
    non_accredited_investors INTEGER,

    -- Sales Compensation (Item 15)
    sales_compensation JSONB,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_form_d_cik ON form_d_filings(cik);
CREATE INDEX idx_form_d_filed_at ON form_d_filings(filed_at);
CREATE INDEX idx_form_d_issuer_name ON form_d_filings(issuer_name);
CREATE INDEX idx_form_d_industry ON form_d_filings(industry_group);
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/form-d/search` | GET | Search filings by company, date, industry |
| `/form-d/issuer/{cik}` | GET | Get all filings by issuer CIK |
| `/form-d/recent` | GET | Recent private placements |
| `/form-d/filing/{accession}` | GET | Get specific filing details |
| `/form-d/stats` | GET | Aggregate statistics |
| `/form-d/ingest` | POST | Trigger ingestion job |

### Form D Client

```python
class FormDClient:
    """SEC Form D filing client."""

    BASE_URL = "https://data.sec.gov"
    EFTS_URL = "https://efts.sec.gov/LATEST/search-index"

    async def search_filings(
        self,
        start_date: str,
        end_date: str,
        form_type: str = "D"
    ) -> List[Dict]:
        """Search for Form D filings by date range."""

    async def get_filing_xml(self, cik: str, accession: str) -> str:
        """Download Form D XML content."""

    def parse_form_d_xml(self, xml_content: str) -> Dict:
        """Parse Form D XML into structured data."""

    async def get_recent_filings(self, days: int = 7) -> List[Dict]:
        """Get filings from the last N days."""
```

### XML Parsing

Form D XML structure (simplified):
```xml
<edgarSubmission>
  <headerData>
    <submissionType>D</submissionType>
    <filerInfo>
      <filerCik>0001234567</filerCik>
    </filerInfo>
  </headerData>
  <formData>
    <issuerInfo>
      <issuerName>Example Company Inc.</issuerName>
      <jurisdictionOfInc>DE</jurisdictionOfInc>
      <yearOfInc>2020</yearOfInc>
    </issuerInfo>
    <offeringData>
      <industryGroup>Technology</industryGroup>
      <federalExemptionsExclusions>
        <item>06b</item>
      </federalExemptionsExclusions>
      <typesOfSecuritiesOffered>
        <isEquityType>true</isEquityType>
      </typesOfSecuritiesOffered>
      <minimumInvestmentAccepted>25000</minimumInvestmentAccepted>
      <totalOfferingAmount>10000000</totalOfferingAmount>
      <totalAmountSold>5000000</totalAmountSold>
    </offeringData>
  </formData>
</edgarSubmission>
```

---

## Response Formats

### Search Response
```json
{
  "query": {
    "industry": "Technology",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  },
  "total": 1523,
  "filings": [
    {
      "accession_number": "0001234567-24-000001",
      "cik": "0001234567",
      "issuer_name": "TechStartup Inc.",
      "filed_at": "2024-03-15",
      "submission_type": "D",
      "industry_group": "Technology",
      "total_offering_amount": 10000000,
      "total_amount_sold": 5000000,
      "exemptions": ["Rule 506(b)"]
    }
  ]
}
```

### Filing Detail Response
```json
{
  "accession_number": "0001234567-24-000001",
  "cik": "0001234567",
  "submission_type": "D",
  "filed_at": "2024-03-15T00:00:00Z",
  "issuer": {
    "name": "TechStartup Inc.",
    "address": {
      "street": "123 Innovation Way",
      "city": "San Francisco",
      "state": "CA",
      "zip": "94105"
    },
    "entity_type": "Corporation",
    "jurisdiction": "Delaware",
    "year_incorporated": 2020
  },
  "industry": "Technology",
  "offering": {
    "exemptions": ["Rule 506(b)"],
    "date_of_first_sale": "2024-03-01",
    "securities": {
      "is_equity": true,
      "is_debt": false,
      "is_pooled_fund": false
    },
    "amounts": {
      "total_offering": 10000000,
      "amount_sold": 5000000,
      "remaining": 5000000,
      "minimum_investment": 25000
    }
  },
  "investors": {
    "total": 15,
    "accredited": 15,
    "non_accredited": 0
  },
  "related_persons": [
    {
      "name": "John Founder",
      "title": "CEO",
      "relationship": ["Executive Officer", "Director"]
    }
  ]
}
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/sources/sec_form_d/__init__.py` | Module init |
| `app/sources/sec_form_d/client.py` | Form D client for EDGAR |
| `app/sources/sec_form_d/parser.py` | XML parser |
| `app/sources/sec_form_d/ingest.py` | Ingestion service |
| `app/api/v1/form_d.py` | API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register form_d router |

---

## Implementation Steps

1. Create `app/sources/sec_form_d/` directory structure
2. Implement `client.py` - EDGAR API access for Form D
3. Implement `parser.py` - XML to structured data
4. Implement `ingest.py` - Database storage
5. Create `app/api/v1/form_d.py` - REST endpoints
6. Register router in main.py
7. Test with recent filings

---

## Test Plan

```bash
# Search recent Form D filings
curl -s "http://localhost:8001/api/v1/form-d/recent?days=7" | python -m json.tool

# Search by industry
curl -s "http://localhost:8001/api/v1/form-d/search?industry=Technology&limit=10" | python -m json.tool

# Get filings by issuer
curl -s "http://localhost:8001/api/v1/form-d/issuer/0001234567" | python -m json.tool

# Get specific filing
curl -s "http://localhost:8001/api/v1/form-d/filing/0001234567-24-000001" | python -m json.tool

# Trigger ingestion
curl -s -X POST "http://localhost:8001/api/v1/form-d/ingest?days=30" | python -m json.tool
```

---

## Success Criteria

- [ ] Can search Form D filings by date, industry, company
- [ ] Parses XML correctly into structured data
- [ ] Stores filings in PostgreSQL
- [ ] Recent filings endpoint works
- [ ] Ingestion job processes historical data
- [ ] All 6 endpoints return properly formatted data

---

## Approval

- [x] **Approved by user** (2026-01-18)

---

*Plan created: 2026-01-18*

Sources:
- [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [Form D XML Technical Specification](https://www.sec.gov/info/edgar/formdxmltechspec.htm)
