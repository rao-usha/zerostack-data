# T38: Glassdoor Company Data

**Status:** [x] Approved - Implemented 2026-01-19
**Owner:** Tab 2
**Dependencies:** None

---

## Goal

Integrate company reviews, ratings, and salary data to provide talent intelligence for portfolio company analysis. Since Glassdoor's API is partner-restricted, this implementation uses a hybrid approach: structured data storage with manual/bulk import capabilities, plus optional scraping when legally permissible.

---

## Data Model

### Key Metrics to Track
| Metric | Description | Use Case |
|--------|-------------|----------|
| Overall Rating | 1-5 star rating | Company health indicator |
| CEO Approval | % approval rating | Leadership quality |
| Recommend to Friend | % who would recommend | Employee satisfaction |
| Business Outlook | % positive outlook | Future trajectory |
| Work-Life Balance | 1-5 rating | Culture indicator |
| Compensation & Benefits | 1-5 rating | Comp competitiveness |
| Career Opportunities | 1-5 rating | Growth potential |
| Culture & Values | 1-5 rating | Cultural fit |
| Senior Management | 1-5 rating | Leadership quality |

### Salary Data
- Job title, base salary range (min/median/max)
- Total compensation (with bonus/equity)
- Location-adjusted figures
- Experience level breakdown

---

## Files to Create

### `app/sources/glassdoor/__init__.py`
Module initialization.

### `app/sources/glassdoor/client.py`
Client for Glassdoor data access:
- `GlassdoorClient` class
- Company search and lookup
- Rating/review data retrieval
- Salary data retrieval
- Rate limiting and caching

### `app/sources/glassdoor/models.py`
Data models for Glassdoor entities.

### `app/api/v1/glassdoor.py`
FastAPI router with endpoints.

### Database Tables

```sql
-- Company ratings and reviews summary
CREATE TABLE glassdoor_companies (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    glassdoor_id VARCHAR(50),
    logo_url TEXT,
    website VARCHAR(255),
    headquarters VARCHAR(255),
    industry VARCHAR(100),
    company_size VARCHAR(50),
    founded_year INTEGER,
    overall_rating FLOAT,
    ceo_name VARCHAR(255),
    ceo_approval FLOAT,
    recommend_to_friend FLOAT,
    business_outlook FLOAT,
    work_life_balance FLOAT,
    compensation_benefits FLOAT,
    career_opportunities FLOAT,
    culture_values FLOAT,
    senior_management FLOAT,
    review_count INTEGER,
    salary_count INTEGER,
    interview_count INTEGER,
    data_source VARCHAR(50) DEFAULT 'manual',
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_name)
);

-- Salary data by role
CREATE TABLE glassdoor_salaries (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES glassdoor_companies(id),
    job_title VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    base_salary_min INTEGER,
    base_salary_median INTEGER,
    base_salary_max INTEGER,
    total_comp_min INTEGER,
    total_comp_median INTEGER,
    total_comp_max INTEGER,
    sample_size INTEGER,
    experience_level VARCHAR(50),
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Review summaries (not individual reviews)
CREATE TABLE glassdoor_review_summaries (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES glassdoor_companies(id),
    period VARCHAR(20),
    avg_rating FLOAT,
    review_count INTEGER,
    top_pros TEXT[],
    top_cons TEXT[],
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## API Endpoints

### 1. `GET /api/v1/glassdoor/company/{name}`
Get Glassdoor data for a company.

**Response:**
```json
{
  "company_name": "Stripe",
  "glassdoor_id": "671932",
  "ratings": {
    "overall": 4.2,
    "work_life_balance": 3.8,
    "compensation_benefits": 4.5,
    "career_opportunities": 4.1,
    "culture_values": 4.3,
    "senior_management": 3.9
  },
  "sentiment": {
    "ceo_approval": 0.92,
    "recommend_to_friend": 0.85,
    "business_outlook": 0.78
  },
  "stats": {
    "review_count": 1250,
    "salary_count": 890,
    "interview_count": 320
  },
  "company_info": {
    "industry": "Financial Technology",
    "size": "1001-5000",
    "headquarters": "San Francisco, CA",
    "founded": 2010
  },
  "retrieved_at": "2026-01-15T10:30:00Z",
  "data_source": "manual"
}
```

### 2. `GET /api/v1/glassdoor/company/{name}/salaries`
Get salary data for a company.

**Query Params:**
- `job_title`: Filter by job title (partial match)
- `location`: Filter by location
- `limit`: Max results (default 20)

**Response:**
```json
{
  "company_name": "Stripe",
  "salaries": [
    {
      "job_title": "Software Engineer",
      "location": "San Francisco, CA",
      "base_salary": {"min": 150000, "median": 180000, "max": 220000},
      "total_comp": {"min": 200000, "median": 280000, "max": 400000},
      "sample_size": 156
    }
  ],
  "total_count": 45
}
```

### 3. `GET /api/v1/glassdoor/company/{name}/reviews`
Get review summary for a company.

**Response:**
```json
{
  "company_name": "Stripe",
  "overall_rating": 4.2,
  "review_count": 1250,
  "rating_trend": [
    {"period": "2025-Q4", "avg_rating": 4.3, "count": 85},
    {"period": "2025-Q3", "avg_rating": 4.1, "count": 92}
  ],
  "top_pros": ["Great compensation", "Smart colleagues", "Interesting problems"],
  "top_cons": ["Work-life balance", "Fast pace can be stressful"]
}
```

### 4. `GET /api/v1/glassdoor/compare`
Compare multiple companies.

**Query Params:**
- `companies`: Comma-separated company names

**Response:**
```json
{
  "comparison": [
    {"company": "Stripe", "overall": 4.2, "compensation": 4.5, "culture": 4.3},
    {"company": "Square", "overall": 3.9, "compensation": 4.0, "culture": 4.1}
  ]
}
```

### 5. `POST /api/v1/glassdoor/company`
Add or update company data (manual entry).

**Request:**
```json
{
  "company_name": "Stripe",
  "overall_rating": 4.2,
  "ceo_approval": 0.92,
  "work_life_balance": 3.8,
  "compensation_benefits": 4.5
}
```

### 6. `POST /api/v1/glassdoor/salaries/bulk`
Bulk import salary data.

**Request:**
```json
{
  "company_name": "Stripe",
  "salaries": [
    {"job_title": "Software Engineer", "base_salary_median": 180000, "location": "San Francisco"}
  ]
}
```

### 7. `GET /api/v1/glassdoor/search`
Search companies in database.

**Query Params:**
- `q`: Search query
- `industry`: Filter by industry
- `min_rating`: Minimum overall rating

### 8. `GET /api/v1/glassdoor/rankings`
Get top-rated companies.

**Query Params:**
- `metric`: Rating metric to rank by (overall, compensation, culture, etc.)
- `industry`: Filter by industry
- `limit`: Number of results

---

## Implementation Notes

1. **Data Source Strategy:**
   - Primary: Manual data entry via API
   - Secondary: Bulk CSV/JSON import
   - Optional: Web scraping (with proper rate limiting)

2. **Caching:**
   - Cache data for 30 days (Glassdoor data doesn't change frequently)
   - Flag data age in responses

3. **Legal Considerations:**
   - No automated scraping of Glassdoor without permission
   - Data entry must be from publicly visible pages
   - No storing individual review text (only summaries)

4. **Integration with T36 (Company Scoring):**
   - Glassdoor ratings can feed into company scoring model
   - Add `glassdoor_score` component when data available

---

## Test Plan

```bash
# Add company data
curl -X POST http://localhost:8001/api/v1/glassdoor/company \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe", "overall_rating": 4.2}'

# Get company data
curl http://localhost:8001/api/v1/glassdoor/company/Stripe

# Search companies
curl "http://localhost:8001/api/v1/glassdoor/search?q=stripe"

# Get rankings
curl "http://localhost:8001/api/v1/glassdoor/rankings?metric=overall&limit=10"

# Compare companies
curl "http://localhost:8001/api/v1/glassdoor/compare?companies=Stripe,Square"
```

---

## Approval

- [ ] User approves plan
- [ ] Ready to implement
