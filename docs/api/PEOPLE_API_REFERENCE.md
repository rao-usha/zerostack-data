# People & Org Chart Intelligence Platform - API Reference

Complete API documentation for the People & Org Chart Intelligence Platform.

**Base URL:** `http://localhost:8001/api/v1`

---

## Table of Contents

1. [People Endpoints](#people-endpoints)
2. [Company Leadership Endpoints](#company-leadership-endpoints)
3. [Portfolio Management](#portfolio-management)
4. [Peer Sets](#peer-sets)
5. [Watchlists](#watchlists)
6. [Analytics](#analytics)
7. [Reports](#reports)
8. [Data Quality](#data-quality)
9. [Collection Jobs](#collection-jobs)
10. [Alerts & Digests](#alerts--digests)

---

## People Endpoints

### List People

```http
GET /people/
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | int | Max results (default: 50, max: 200) |
| `offset` | int | Pagination offset |
| `company_id` | int | Filter by company |
| `title_level` | string | Filter by level: `c_suite`, `vp`, `director`, `manager` |

**Response:**
```json
[
  {
    "id": 1,
    "full_name": "John Smith",
    "first_name": "John",
    "last_name": "Smith",
    "email": "john.smith@company.com",
    "linkedin_url": "https://linkedin.com/in/johnsmith",
    "current_title": "Chief Executive Officer",
    "current_company": "Acme Industrial"
  }
]
```

### Get Person

```http
GET /people/{person_id}
```

**Response:**
```json
{
  "id": 1,
  "full_name": "John Smith",
  "first_name": "John",
  "last_name": "Smith",
  "email": "john.smith@company.com",
  "email_confidence": "verified",
  "linkedin_url": "https://linkedin.com/in/johnsmith",
  "photo_url": "https://company.com/photos/john-smith.jpg",
  "bio": "20+ years of experience in industrial distribution...",
  "city": "Chicago",
  "state": "IL",
  "country": "USA",
  "current_positions": [...],
  "experience": [...],
  "education": [...],
  "data_quality_score": 85
}
```

### Search People

```http
GET /people/search
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | Search query (name, title, company) |
| `title` | string | Filter by title |
| `company` | string | Filter by company name |
| `industry` | string | Filter by industry |
| `limit` | int | Max results (default: 20) |

---

## Company Leadership Endpoints

### Get Company Leadership

```http
GET /companies/{company_id}/leadership
```

**Response:**
```json
{
  "company_id": 1,
  "company_name": "Acme Industrial Supply",
  "total_executives": 12,
  "c_suite_count": 5,
  "leadership": [
    {
      "person_id": 1,
      "full_name": "John Smith",
      "title": "Chief Executive Officer",
      "title_level": "c_suite",
      "is_board_member": true,
      "start_date": "2020-01-15",
      "tenure_months": 48,
      "linkedin_url": "https://linkedin.com/in/johnsmith"
    }
  ],
  "org_chart": {...}
}
```

### Get Leadership Changes

```http
GET /companies/{company_id}/leadership/changes
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `days` | int | Lookback period (default: 90) |
| `change_type` | string | Filter: `hire`, `departure`, `promotion`, `retirement` |

**Response:**
```json
{
  "company_id": 1,
  "period_days": 90,
  "total_changes": 3,
  "changes": [
    {
      "id": 1,
      "person_name": "Jane Doe",
      "change_type": "hire",
      "old_title": null,
      "new_title": "Chief Financial Officer",
      "announced_date": "2024-01-15",
      "is_c_suite": true,
      "significance_score": 9
    }
  ]
}
```

### Compare Leadership Teams

```http
GET /companies/{company_id}/leadership/compare
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `peer_ids` | string | Comma-separated company IDs |

---

## Portfolio Management

### List Portfolios

```http
GET /people-portfolios/
```

**Response:**
```json
[
  {
    "id": 1,
    "name": "Growth Fund I",
    "pe_firm": "Acme Capital Partners",
    "portfolio_type": "pe_portfolio",
    "company_count": 8,
    "is_active": true,
    "created_at": "2024-01-01T00:00:00Z"
  }
]
```

### Create Portfolio

```http
POST /people-portfolios/
```

**Request Body:**
```json
{
  "name": "Growth Fund II",
  "pe_firm": "Acme Capital Partners",
  "description": "Second growth-focused fund",
  "portfolio_type": "pe_portfolio"
}
```

### Get Portfolio

```http
GET /people-portfolios/{portfolio_id}
```

### Get Portfolio Companies

```http
GET /people-portfolios/{portfolio_id}/companies
```

### Add Company to Portfolio

```http
POST /people-portfolios/{portfolio_id}/companies
```

**Request Body:**
```json
{
  "company_id": 5,
  "investment_date": "2024-01-15",
  "notes": "Platform acquisition"
}
```

### Remove Company from Portfolio

```http
DELETE /people-portfolios/{portfolio_id}/companies/{company_id}
```

---

## Peer Sets

### List Peer Sets

```http
GET /peer-sets/
```

### Create Peer Set

```http
POST /peer-sets/
```

**Request Body:**
```json
{
  "name": "Fastener Distributors $50M-$200M",
  "industry": "distribution",
  "criteria": {
    "revenue_min": 50000000,
    "revenue_max": 200000000,
    "sub_segment": "fasteners"
  }
}
```

### Get Peer Set

```http
GET /peer-sets/{peer_set_id}
```

### Add Company to Peer Set

```http
POST /peer-sets/{peer_set_id}/companies
```

**Request Body:**
```json
{
  "company_id": 10,
  "is_primary": true
}
```

### Get Peer Comparison

```http
GET /peer-sets/{peer_set_id}/comparison
```

**Response:**
```json
{
  "peer_set_id": 1,
  "peer_set_name": "Fastener Distributors",
  "primary_company": {...},
  "comparison": {
    "c_suite_count": {"primary": 5, "peer_avg": 4.2},
    "avg_tenure_months": {"primary": 36, "peer_avg": 28},
    "changes_90d": {"primary": 1, "peer_avg": 2.1},
    "team_score": {"primary": 78, "peer_avg": 65}
  },
  "peers": [...]
}
```

---

## Watchlists

### List Watchlists

```http
GET /people-watchlists/
```

### Create Watchlist

```http
POST /people-watchlists/
```

**Request Body:**
```json
{
  "name": "CFO Candidates",
  "description": "Potential CFO hires for portfolio companies"
}
```

### Get Watchlist

```http
GET /people-watchlists/{watchlist_id}
```

### Add Person to Watchlist

```http
POST /people-watchlists/{watchlist_id}/people
```

**Request Body:**
```json
{
  "person_id": 25,
  "notes": "Strong background in distribution",
  "tags": ["cfo_candidate", "knows_industry"]
}
```

### Get Watchlist Alerts

```http
GET /people-watchlists/{watchlist_id}/alerts
```

---

## Analytics

### Get Industry Statistics

```http
GET /people-analytics/industry/{industry}
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `days` | int | Analysis period (default: 90) |

**Response:**
```json
{
  "industry": "distribution",
  "period_days": 90,
  "total_companies": 150,
  "total_executives": 1200,
  "c_suite_count": 450,
  "changes_in_period": 85,
  "changes_by_type": {
    "hire": 35,
    "departure": 28,
    "promotion": 15,
    "retirement": 7
  },
  "avg_ceo_tenure_months": 48,
  "avg_cfo_tenure_months": 36,
  "instability_flags": [...]
}
```

### Get Change Trends

```http
GET /people-analytics/trends
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `months` | int | Trend period (default: 12) |
| `industry` | string | Filter by industry |

**Response:**
```json
{
  "industry": "all",
  "months": 12,
  "trends": [
    {
      "month": "2024-01",
      "total": 45,
      "hires": 20,
      "departures": 15,
      "promotions": 8,
      "retirements": 2
    }
  ]
}
```

### Get Talent Flow

```http
GET /people-analytics/talent-flow
```

**Response:**
```json
{
  "industry": "distribution",
  "period_days": 90,
  "net_importers": [
    {"company_id": 1, "company_name": "Acme", "hires": 5, "departures": 1, "net": 4}
  ],
  "net_exporters": [
    {"company_id": 2, "company_name": "Beta Corp", "hires": 1, "departures": 4, "net": -3}
  ]
}
```

### Get Hot Roles

```http
GET /people-analytics/hot-roles
```

**Response:**
```json
[
  {"role": "CFO", "hires": 15},
  {"role": "VP Sales", "hires": 12},
  {"role": "CEO", "hires": 8}
]
```

### Get Company Benchmark

```http
GET /people-analytics/benchmark/{company_id}
```

**Response:**
```json
{
  "company_id": 1,
  "company_name": "Acme Industrial",
  "team_score": 78,
  "components": {
    "completeness": 25,
    "depth": 20,
    "tenure": 18,
    "board": 15
  },
  "details": {
    "has_ceo": true,
    "has_cfo": true,
    "has_coo": true,
    "c_suite_count": 5,
    "vp_count": 7,
    "board_count": 3,
    "avg_c_suite_tenure_months": 36
  }
}
```

### Get Portfolio Analytics

```http
GET /people-analytics/portfolio/{portfolio_id}
```

---

## Reports

### Generate Management Assessment

```http
GET /people-reports/assessment/{company_id}
```

**Response:**
```json
{
  "company_id": 1,
  "company_name": "Acme Industrial",
  "generated_at": "2024-01-30T12:00:00Z",
  "executive_summary": "...",
  "team_overview": {...},
  "key_executives": [...],
  "tenure_analysis": {...},
  "gap_analysis": {...},
  "recommendations": [...]
}
```

### Generate Peer Comparison Report

```http
GET /people-reports/peer-comparison/{company_id}
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `peer_set_id` | int | Use existing peer set |
| `peer_ids` | string | Or specify peer company IDs |

### Export Report

```http
GET /people-reports/export/{company_id}
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `format` | string | `json` or `csv` |

---

## Data Quality

### Get Overall Stats

```http
GET /people-data-quality/stats
```

**Response:**
```json
{
  "total_people": 5000,
  "total_companies": 200,
  "coverage": {
    "linkedin": 75.5,
    "photo": 45.2,
    "email": 68.3,
    "bio": 52.1
  },
  "avg_confidence_score": 0.72
}
```

### Get Freshness Stats

```http
GET /people-data-quality/freshness
```

**Response:**
```json
{
  "total_people": 5000,
  "freshness_buckets": {
    "0-7_days": 500,
    "8-30_days": 1200,
    "31-90_days": 1500,
    "91-180_days": 800,
    "181-365_days": 500,
    "over_365_days": 200,
    "never_verified": 300
  },
  "stale_pct": 36.0
}
```

### Get Person Quality Score

```http
GET /people-data-quality/score/{person_id}
```

**Response:**
```json
{
  "person_id": 1,
  "person_name": "John Smith",
  "quality_score": 85,
  "components": {
    "identity": 20,
    "contact": 20,
    "professional": 20,
    "history": 15,
    "freshness": 10
  },
  "issues": ["Data may be stale"]
}
```

### Find Duplicates

```http
GET /people-data-quality/duplicates
```

### Merge Duplicates

```http
POST /people-data-quality/merge
```

**Request Body:**
```json
{
  "canonical_id": 1,
  "duplicate_ids": [5, 8]
}
```

### Get Enrichment Queue

```http
GET /people-data-quality/enrichment-queue
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `enrichment_type` | string | `all`, `linkedin`, `email`, `photo` |
| `limit` | int | Max results (default: 100) |

### Infer Email

```http
POST /people-data-quality/infer-email
```

**Request Body:**
```json
{
  "first_name": "John",
  "last_name": "Smith",
  "company_domain": "acme.com"
}
```

---

## Collection Jobs

### List Jobs

```http
GET /people-jobs/
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `status` | string | `pending`, `running`, `success`, `failed` |
| `job_type` | string | `website_crawl`, `sec_parse`, `news_scan` |
| `limit` | int | Max results |

### Get Job Stats

```http
GET /people-jobs/stats
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `days` | int | Analysis period (default: 7) |

**Response:**
```json
{
  "period_days": 7,
  "total_jobs": 50,
  "by_status": {"success": 45, "failed": 3, "running": 2},
  "by_type": {"website_crawl": 30, "sec_parse": 15, "news_scan": 5},
  "total_people_found": 250,
  "total_people_created": 45,
  "success_rate": 90.0
}
```

### Get Job Details

```http
GET /people-jobs/{job_id}
```

### Schedule Job

```http
POST /people-jobs/schedule
```

**Request Body:**
```json
{
  "job_type": "website_crawl",
  "company_ids": [1, 2, 3],
  "priority": "portfolio",
  "limit": 50
}
```

### Cancel Job

```http
POST /people-jobs/{job_id}/cancel
```

### Cleanup Stuck Jobs

```http
POST /people-jobs/cleanup-stuck
```

---

## Alerts & Digests

### Get Recent Alerts

```http
GET /people-jobs/alerts/recent
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `days` | int | Lookback period (default: 7) |
| `c_suite_only` | bool | Filter to C-suite changes |

**Response:**
```json
{
  "period_days": 7,
  "total_alerts": 15,
  "alerts": [
    {
      "change_id": 1,
      "person_name": "Jane Doe",
      "company_id": 1,
      "company_name": "Acme Industrial",
      "change_type": "hire",
      "new_title": "CFO",
      "announced_date": "2024-01-28",
      "is_c_suite": true,
      "significance_score": 9
    }
  ]
}
```

### Get Portfolio Alerts

```http
GET /people-jobs/alerts/portfolio/{portfolio_id}
```

### Get Watchlist Alerts

```http
GET /people-jobs/alerts/watchlist/{watchlist_id}
```

### Get Industry Alerts

```http
GET /people-jobs/alerts/industry/{industry}
```

### Get Weekly Digest

```http
GET /people-jobs/digest/weekly
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `portfolio_id` | int | Filter to portfolio |
| `industry` | string | Filter to industry |

**Response:**
```json
{
  "generated_at": "2024-01-30T12:00:00Z",
  "period": "Last 7 days",
  "filter": {"type": "portfolio", "id": 1, "name": "Growth Fund I"},
  "summary": {
    "period_days": 7,
    "total_changes": 25,
    "by_type": {"hire": 10, "departure": 8, "promotion": 5, "retirement": 2},
    "c_suite_changes": 8,
    "board_changes": 3,
    "high_significance": 5,
    "companies_affected": 12
  },
  "highlights": [...],
  "all_changes": [...]
}
```

### Get Watchlist Digest

```http
GET /people-jobs/digest/watchlist/{watchlist_id}
```

### Get Change Summary

```http
GET /people-jobs/digest/summary
```

---

## Error Responses

All endpoints return standard error responses:

```json
{
  "detail": "Resource not found"
}
```

**Common HTTP Status Codes:**
- `200` - Success
- `201` - Created
- `400` - Bad Request (invalid parameters)
- `404` - Not Found
- `422` - Validation Error
- `500` - Internal Server Error

---

## Rate Limiting

API requests are rate-limited to prevent abuse:
- **Default:** 100 requests per minute
- **Batch operations:** 10 requests per minute

Rate limit headers are included in responses:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1706620800
```

---

## Authentication

Currently, the API does not require authentication. Future versions will support:
- API Key authentication
- JWT tokens
- OAuth 2.0

---

## Pagination

List endpoints support pagination:

```http
GET /people/?limit=50&offset=100
```

Response includes pagination metadata:
```json
{
  "total": 5000,
  "limit": 50,
  "offset": 100,
  "has_more": true,
  "data": [...]
}
```
