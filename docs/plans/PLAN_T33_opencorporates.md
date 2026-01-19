# Plan T33: OpenCorporates Integration

## Overview
**Task:** T33
**Tab:** 2
**Feature:** Company registry data worldwide from OpenCorporates API
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

Investment teams need corporate registry data for:

1. **Due Diligence**: Verify company registration, officers, and legal status
2. **Company Discovery**: Find companies by name, jurisdiction, or industry
3. **Officer Research**: Identify directors, executives, and their other appointments
4. **Filing History**: Track corporate filings, amendments, and status changes
5. **Global Coverage**: Access registry data from 140+ jurisdictions worldwide

### User Scenarios

#### Scenario 1: Company Verification
**Analyst** researches a target company for due diligence.
- Query: "Get details for Apple Inc in Delaware"
- Result: Registration info, officers, filings, status

#### Scenario 2: Officer Background Check
**Compliance Officer** researches a potential board member.
- Query: "Find all companies where John Smith is an officer"
- Result: List of companies with roles and appointment dates

#### Scenario 3: Company Discovery
**Sourcing Team** finds companies in a specific jurisdiction.
- Query: "Search for 'renewable energy' companies in California"
- Result: Matching companies with basic details

---

## Success Criteria

### Must Have

| ID | Criteria | Verification |
|----|----------|--------------|
| M1 | Company search | Search by name, jurisdiction |
| M2 | Company details | Get full company profile |
| M3 | Officer search | Find officers by name |
| M4 | Company officers | List officers for a company |
| M5 | Filing history | Get company filings |

### Should Have

| ID | Criteria | Verification |
|----|----------|--------------|
| S1 | Jurisdiction list | Available jurisdictions |
| S2 | Industry codes | SIC/NAICS classification |
| S3 | Bulk search | Multiple companies at once |

---

## Technical Design

### OpenCorporates API

OpenCorporates provides a REST API for accessing global corporate registry data.

**Base URL:** `https://api.opencorporates.com/v0.4`

**Authentication:** API key passed as `api_token` query parameter

**Rate Limits:**
- Free tier: 50 requests/day
- API key: Based on plan

### API Endpoints We'll Use

| OpenCorporates Endpoint | Description |
|------------------------|-------------|
| `GET /companies/search` | Search companies by query |
| `GET /companies/{jurisdiction}/{number}` | Get company details |
| `GET /officers/search` | Search officers by name |
| `GET /companies/{jurisdiction}/{number}/officers` | Company officers |
| `GET /companies/{jurisdiction}/{number}/filings` | Company filings |
| `GET /jurisdictions` | List available jurisdictions |

### Our API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/corporate-registry/search` | Search companies |
| GET | `/api/v1/corporate-registry/company/{jurisdiction}/{number}` | Get company details |
| GET | `/api/v1/corporate-registry/company/{jurisdiction}/{number}/officers` | Get company officers |
| GET | `/api/v1/corporate-registry/company/{jurisdiction}/{number}/filings` | Get company filings |
| GET | `/api/v1/corporate-registry/officers/search` | Search officers |
| GET | `/api/v1/corporate-registry/jurisdictions` | List jurisdictions |

### Request/Response Models

**Company Search Response:**
```json
{
  "companies": [
    {
      "name": "Apple Inc.",
      "company_number": "12345",
      "jurisdiction_code": "us_de",
      "incorporation_date": "1976-04-01",
      "company_type": "Corporation",
      "registry_url": "https://opencorporates.com/companies/us_de/12345",
      "current_status": "Active",
      "registered_address": "1 Apple Park Way, Cupertino, CA"
    }
  ],
  "total_count": 150,
  "page": 1,
  "per_page": 30
}
```

**Company Details Response:**
```json
{
  "name": "Apple Inc.",
  "company_number": "12345",
  "jurisdiction_code": "us_de",
  "jurisdiction_name": "Delaware (US)",
  "incorporation_date": "1976-04-01",
  "dissolution_date": null,
  "company_type": "Corporation",
  "current_status": "Active",
  "registered_address": {
    "street": "1 Apple Park Way",
    "city": "Cupertino",
    "region": "CA",
    "postal_code": "95014",
    "country": "United States"
  },
  "agent_name": "Corporation Trust Company",
  "agent_address": "1209 Orange Street, Wilmington, DE 19801",
  "registry_url": "https://opencorporates.com/companies/us_de/12345",
  "officers_count": 25,
  "filings_count": 100,
  "retrieved_at": "2026-01-18T12:00:00Z"
}
```

**Officer Response:**
```json
{
  "officers": [
    {
      "name": "Tim Cook",
      "position": "Chief Executive Officer",
      "start_date": "2011-08-24",
      "end_date": null,
      "nationality": "United States",
      "occupation": "Executive"
    }
  ],
  "total_count": 25
}
```

**Filings Response:**
```json
{
  "filings": [
    {
      "title": "Annual Report",
      "filing_type": "Annual Report",
      "date": "2025-03-15",
      "description": "Annual report for fiscal year 2024",
      "url": "https://..."
    }
  ],
  "total_count": 100
}
```

### Client Implementation

```python
class OpenCorporatesClient:
    """Client for OpenCorporates API."""

    BASE_URL = "https://api.opencorporates.com/v0.4"

    def __init__(self, api_token: str = None):
        self.api_token = api_token or os.getenv("OPENCORPORATES_API_KEY")

    def search_companies(
        self,
        query: str,
        jurisdiction: str = None,
        page: int = 1,
        per_page: int = 30
    ) -> dict:
        """Search companies by name."""

    def get_company(
        self,
        jurisdiction: str,
        company_number: str
    ) -> dict:
        """Get company details."""

    def get_company_officers(
        self,
        jurisdiction: str,
        company_number: str
    ) -> dict:
        """Get officers for a company."""

    def get_company_filings(
        self,
        jurisdiction: str,
        company_number: str
    ) -> dict:
        """Get filings for a company."""

    def search_officers(
        self,
        query: str,
        jurisdiction: str = None,
        page: int = 1
    ) -> dict:
        """Search officers by name."""

    def get_jurisdictions(self) -> list:
        """Get list of available jurisdictions."""
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/sources/opencorporates/__init__.py` | Package init |
| `app/sources/opencorporates/client.py` | OpenCorporates API client |
| `app/api/v1/corporate_registry.py` | 6 API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register corporate_registry router |

---

## Implementation Steps

1. Create `app/sources/opencorporates/` directory structure
2. Implement `client.py` with OpenCorporatesClient
3. Create `app/api/v1/corporate_registry.py` with 6 endpoints
4. Register router in main.py
5. Test all endpoints

---

## Test Plan

| Test ID | Test | Expected |
|---------|------|----------|
| OC-001 | Search companies | Returns matching companies |
| OC-002 | Get company details | Returns full company profile |
| OC-003 | Get company officers | Returns officer list |
| OC-004 | Get company filings | Returns filing history |
| OC-005 | Search officers | Returns matching officers |
| OC-006 | List jurisdictions | Returns jurisdiction list |

### Test Commands

```bash
# Search companies
curl -s "http://localhost:8001/api/v1/corporate-registry/search?query=apple&jurisdiction=us_de" \
  | python -m json.tool

# Get company details
curl -s "http://localhost:8001/api/v1/corporate-registry/company/us_de/12345" \
  | python -m json.tool

# Get company officers
curl -s "http://localhost:8001/api/v1/corporate-registry/company/us_de/12345/officers" \
  | python -m json.tool

# Search officers
curl -s "http://localhost:8001/api/v1/corporate-registry/officers/search?query=tim+cook" \
  | python -m json.tool

# List jurisdictions
curl -s "http://localhost:8001/api/v1/corporate-registry/jurisdictions" \
  | python -m json.tool
```

---

## Environment Variables

```bash
OPENCORPORATES_API_KEY=your_api_key_here  # Optional, enables higher rate limits
```

---

## Approval

- [x] **Approved by user** (2026-01-18)

## Implementation Notes

- Created `app/sources/opencorporates/client.py` with OpenCorporatesClient
- Created `app/api/v1/corporate_registry.py` with 6 endpoints
- Registered router in main.py with OpenAPI tag
- API requires `OPENCORPORATES_API_KEY` environment variable for data access
- Jurisdictions endpoint includes fallback list if API unavailable
- All endpoints handle 404 gracefully

---

*Plan created: 2026-01-18*
*Completed: 2026-01-18*
