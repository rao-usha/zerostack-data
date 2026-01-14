# Plan 002: USPTO Patent Data Source

**Date:** 2026-01-14
**Author:** Tab 2 (Claude)
**Status:** PENDING_APPROVAL
**Assigned To:** Tab 2

---

## Goal

Enable ingestion of US patent data from the USPTO PatentsView API, providing access to granted patents, pre-grant publications, inventors, assignees, and patent classifications for research, competitive intelligence, and technology trend analysis.

---

## Research

### Available USPTO APIs

| API | Status | Use Case |
|-----|--------|----------|
| **PatentsView PatentSearch API** | Active (v2.3.0, Feb 2025) | Primary API for patent search, inventors, assignees, citations |
| **Patent Assignment Search** | Active (v1.4) | Patent ownership transfers and assignments |
| **PTAB API v3** | Active (migrated to ODP) | Patent Trial and Appeal Board proceedings |
| **PEDS (Patent Examination Data)** | Active | Filing status from 1981-present (9.4M+ records) |
| **USPTO Office Action APIs** | Decommissioning Jan 2026 | Avoid - being retired |

### Recommendation: PatentsView PatentSearch API

The PatentsView API is the best choice for this integration:
- **Comprehensive data**: Granted patents through Sept 2025, pre-grant publications
- **Free with API key**: No cost, just rate limits
- **Well-documented**: Swagger UI, full endpoint dictionary
- **Stable**: New API (legacy discontinued May 2025)

### Authentication

- **Method**: API key in `X-Api-Key` header
- **Obtaining**: Request via [PatentsView Help Center](https://patentsview-support.atlassian.net/servicedesk/customer/portal/1/group/1/create/18)
- **Limit**: One key per user, keys do not expire

### Rate Limits

- **45 requests/minute** per API key
- **429 Too Many Requests** response when exceeded
- **1,000 records max** per request (default 100)

### Base URL

```
https://search.patentsview.org/api/v1/
```

---

## Nexdata API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/uspto/patents` | Search patents with filters |
| GET | `/api/v1/uspto/patents/{patent_id}` | Get single patent details |
| GET | `/api/v1/uspto/inventors` | Search inventors |
| GET | `/api/v1/uspto/inventors/{inventor_id}` | Get inventor details |
| GET | `/api/v1/uspto/assignees` | Search assignees (companies) |
| GET | `/api/v1/uspto/assignees/{assignee_id}` | Get assignee details |
| GET | `/api/v1/uspto/classifications` | Search CPC/IPC classifications |
| POST | `/api/v1/uspto/ingest/patents` | Ingest patents by search criteria |
| POST | `/api/v1/uspto/ingest/assignee` | Ingest all patents for an assignee |
| GET | `/api/v1/uspto/metadata` | List available data fields |

---

## Features

1. **Patent Search**
   - Full-text search: `_text_all`, `_text_any`, `_text_phrase`
   - Field filters: patent_date, assignee, inventor, CPC code
   - Date range queries (ISO 8601 format)
   - Sort by date, relevance, citation count

2. **Entity Resolution**
   - Disambiguated inventors (PatentsView's name disambiguation)
   - Disambiguated assignees with organization info
   - Geographic location data for inventors/assignees

3. **Classification Support**
   - CPC (Cooperative Patent Classification)
   - IPC (International Patent Classification)
   - USPC (US Patent Classification)
   - WIPO technology fields

4. **Citation Network**
   - US patent citations
   - US application citations
   - Foreign patent citations
   - Non-patent literature references

5. **Ingestion Options**
   - By assignee name/ID (all company patents)
   - By CPC code (technology sector)
   - By date range (recent filings)
   - By inventor name/location

---

## Models

```python
class Patent(Base):
    __tablename__ = "uspto_patents"

    id = Column(Integer, primary_key=True)
    patent_id = Column(String(20), unique=True, index=True)  # e.g., "7861317"
    patent_title = Column(Text, nullable=False)
    patent_abstract = Column(Text)
    patent_date = Column(Date, index=True)
    patent_type = Column(String(50))  # utility, design, plant, reissue
    num_claims = Column(Integer)
    num_citations = Column(Integer)

    # Raw JSON for nested data
    inventors_json = Column(JSON)  # [{name, location, ...}]
    assignees_json = Column(JSON)  # [{name, type, location, ...}]
    cpc_codes_json = Column(JSON)  # [{section, class, subclass, ...}]

    # Metadata
    ingested_at = Column(DateTime, default=datetime.utcnow)
    source_api = Column(String(50), default="patentsview")


class Inventor(Base):
    __tablename__ = "uspto_inventors"

    id = Column(Integer, primary_key=True)
    inventor_id = Column(String(50), unique=True, index=True)
    name_first = Column(String(255))
    name_last = Column(String(255))
    location_city = Column(String(255))
    location_state = Column(String(50))
    location_country = Column(String(100))
    patent_count = Column(Integer)
    first_patent_date = Column(Date)
    last_patent_date = Column(Date)


class Assignee(Base):
    __tablename__ = "uspto_assignees"

    id = Column(Integer, primary_key=True)
    assignee_id = Column(String(50), unique=True, index=True)
    assignee_name = Column(String(500), index=True)
    assignee_type = Column(String(50))  # US Company, Foreign Company, Individual, etc.
    location_city = Column(String(255))
    location_state = Column(String(50))
    location_country = Column(String(100))
    patent_count = Column(Integer)
    first_patent_date = Column(Date)
    last_patent_date = Column(Date)
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/sources/uspto/__init__.py` | Module init, exports |
| `app/sources/uspto/client.py` | PatentsView API client (async, rate-limited) |
| `app/sources/uspto/ingest.py` | Ingestion logic, DB operations |
| `app/sources/uspto/metadata.py` | Field definitions, CPC code mappings |
| `app/api/v1/uspto.py` | REST API endpoints |
| `alembic/versions/xxx_add_uspto_tables.py` | Database migration |

---

## Dependencies

No new packages required. Using existing:
- `httpx` - Async HTTP client (via BaseAPIClient)
- `sqlalchemy` - ORM (existing)

---

## Example Usage

```bash
# Search patents by assignee name
curl "http://localhost:8001/api/v1/uspto/patents?assignee=Apple&limit=10"

# Get specific patent
curl "http://localhost:8001/api/v1/uspto/patents/7861317"

# Search patents by CPC code (machine learning)
curl "http://localhost:8001/api/v1/uspto/patents?cpc_code=G06N"

# Ingest all patents for a company
curl -X POST http://localhost:8001/api/v1/uspto/ingest/assignee \
  -H "Content-Type: application/json" \
  -d '{"assignee_name": "Apple Inc.", "date_from": "2020-01-01"}'

# Get inventor by ID
curl "http://localhost:8001/api/v1/uspto/inventors/fl:th_ln:edison-1"
```

---

## Implementation Notes

1. **Rate Limiting**: Implement 45 req/min limit using existing rate limiter pattern
2. **Pagination**: Use cursor-based pagination (`after` parameter) for large result sets
3. **Caching**: Consider caching patent lookups (patents don't change after grant)
4. **API Key**: Store in `.env` as `USPTO_PATENTSVIEW_API_KEY`

---

## Sources

- [USPTO Open Data Portal](https://developer.uspto.gov/api-catalog)
- [PatentsView API Reference](https://search.patentsview.org/docs/docs/Search%20API/SearchAPIReference/)
- [PatentsView Endpoint Dictionary](https://search.patentsview.org/docs/docs/Search%20API/EndpointDictionary/)
- [PatentsView Purpose & Overview](https://patentsview.org/apis/purpose)

---

## Approval

- [x] User approved (2026-01-14)
- [x] Ready to implement

**User feedback:**
Approved.
