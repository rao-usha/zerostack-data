# Implementation Plan: People & Org Chart Intelligence Platform

**Status:** PENDING APPROVAL
**Date:** 2026-01-28
**PRD:** `docs/plans/PRD_PEOPLE_ORG_CHART_PLATFORM.md`
**Primary User:** PE Operating Partners, Asset Managers, Deal Teams

---

## Goal

Build a leadership intelligence platform that enables PE teams to:
1. **View portfolio company teams** - See leadership, org charts, backgrounds
2. **Benchmark against peers** - Compare team structure vs. competitors
3. **Monitor changes** - Alerts on executive movements across portfolio & industry
4. **Track key players** - Watchlists, search, recruit candidates

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FastAPI Application                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  API Layer (app/api/v1/)                                                    │
│  ├── people.py          - People search, profiles, experience               │
│  ├── companies.py       - Leadership teams, org charts                      │
│  ├── changes.py         - Leadership change feed, alerts                    │
│  ├── benchmarking.py    - Peer comparison, team scoring                     │
│  ├── watchlists.py      - Key player tracking                               │
│  └── portfolios.py      - Portfolio-wide views, dashboards                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Service Layer (app/services/)                                              │
│  ├── people_service.py       - People CRUD, search, dedup                   │
│  ├── org_chart_service.py    - Org chart building, snapshots                │
│  ├── change_detection.py     - Detect & classify leadership changes         │
│  ├── benchmarking_service.py - Peer comparison, scoring                     │
│  └── alert_service.py        - Notifications, digests                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  Collection Layer (app/sources/people_collection/)                          │
│  ├── orchestrator.py         - Coordinates all collection                   │
│  ├── website_agent.py        - Leadership page discovery & extraction       │
│  ├── sec_agent.py            - SEC filing parsing (DEF 14A, 8-K)            │
│  ├── news_agent.py           - Press release monitoring                     │
│  └── llm_extractor.py        - LLM-based data extraction                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  Data Layer (PostgreSQL)                                                    │
│  ├── people                  - Master person records                        │
│  ├── industrial_companies    - Company master for vertical                  │
│  ├── company_people          - Person-company relationships                 │
│  ├── people_experience       - Work history                                 │
│  ├── people_education        - Education records                            │
│  ├── org_chart_snapshots     - Point-in-time org structures                 │
│  ├── leadership_changes      - Executive movements                          │
│  ├── people_collection_jobs  - Track collection runs                        │
│  ├── portfolios              - PE portfolio definitions                     │
│  ├── portfolio_companies     - Portfolio membership                         │
│  ├── peer_sets               - Company peer group definitions               │
│  └── watchlists              - User watchlists for exec tracking            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Database Foundation (Week 1)

### Goal
Create all database tables and seed initial company data.

### Files to Create

| File | Description |
|------|-------------|
| `app/core/models/people.py` | SQLAlchemy models for people tables |
| `app/core/models/org_chart.py` | Models for org charts and changes |
| `app/core/models/portfolio.py` | Models for portfolios, peer sets, watchlists |
| `alembic/versions/xxx_create_people_tables.py` | Migration for all new tables |
| `data/seeds/industrial_companies.json` | Seed data for 200+ companies |
| `scripts/seed_companies.py` | Script to load seed data |

### Database Tables

```sql
-- Core tables (from PRD Section 5)
people
industrial_companies
company_people
people_experience
people_education
org_chart_snapshots
leadership_changes
people_collection_jobs

-- New tables for PE features
portfolios (id, name, pe_firm, created_at)
portfolio_companies (portfolio_id, company_id, added_at)
peer_sets (id, company_id, name, created_at)
peer_set_members (peer_set_id, peer_company_id)
watchlists (id, user_id, name, created_at)
watchlist_people (watchlist_id, person_id, added_at, notes)
```

### Tasks

- [ ] Create SQLAlchemy models for all 14 tables
- [ ] Create Alembic migration
- [ ] Run migration, verify tables created
- [ ] Create seed data file with 200 industrial companies
- [ ] Write and run seeding script
- [ ] Verify: `SELECT COUNT(*) FROM industrial_companies` = 200+

### Acceptance Criteria

```bash
# All tables exist
docker-compose exec db psql -U postgres -d nexdata -c "\dt" | grep -E "people|company|org_chart|portfolio|watchlist"

# Companies seeded
curl -s http://localhost:8001/api/v1/companies/industrial | jq '.total'
# Expected: 200+
```

---

## Phase 2: Collection Infrastructure (Week 2)

### Goal
Build the base collection framework and LLM extraction pipeline.

### Files to Create

| File | Description |
|------|-------------|
| `app/sources/people_collection/__init__.py` | Module init |
| `app/sources/people_collection/types.py` | Pydantic models for extraction |
| `app/sources/people_collection/config.py` | Rate limits, prompts, settings |
| `app/sources/people_collection/base_collector.py` | HTTP client, rate limiting |
| `app/sources/people_collection/llm_extractor.py` | Claude/GPT extraction wrapper |
| `app/sources/people_collection/orchestrator.py` | Coordinates collection agents |

### Key Types

```python
# types.py
class ExtractedPerson(BaseModel):
    full_name: str
    title: str
    title_normalized: Optional[str]
    title_level: Optional[str]  # c_suite, vp, director, manager
    department: Optional[str]
    bio: Optional[str]
    linkedin_url: Optional[str]
    photo_url: Optional[str]
    reports_to: Optional[str]
    is_board_member: bool = False
    is_executive: bool = True
    confidence: str = "medium"

class LeadershipChange(BaseModel):
    person_name: str
    change_type: str  # hire, departure, promotion, retirement
    old_title: Optional[str]
    new_title: Optional[str]
    old_company: Optional[str]
    effective_date: Optional[date]
    source_url: str

class CollectionResult(BaseModel):
    company_id: int
    source: str
    people_found: int
    people_created: int
    people_updated: int
    changes_detected: int
    errors: List[str]
```

### Tasks

- [ ] Create types.py with all Pydantic models
- [ ] Create config.py with rate limits and LLM settings
- [ ] Create base_collector.py with HTTP client, retry logic
- [ ] Create llm_extractor.py with Claude API integration
- [ ] Create orchestrator.py skeleton
- [ ] Write unit tests for LLM extraction parsing
- [ ] Test LLM extraction on sample HTML

### Acceptance Criteria

```python
# LLM extraction works
extractor = LLMExtractor()
result = await extractor.extract_leadership(sample_html, "Fastenal")
assert len(result.people) >= 5
assert any(p.title_level == "c_suite" for p in result.people)
```

---

## Phase 3: Website Collection Agent (Week 3)

### Goal
Build the agent that discovers and extracts leadership from company websites.

### Files to Create

| File | Description |
|------|-------------|
| `app/sources/people_collection/website_agent.py` | Main website collection agent |
| `app/sources/people_collection/page_finder.py` | Leadership page URL discovery |
| `app/sources/people_collection/html_cleaner.py` | Clean HTML for LLM |
| `tests/sources/test_website_agent.py` | Tests with fixture HTML |
| `tests/fixtures/leadership_pages/` | Sample HTML from real sites |

### Website Agent Flow

```
1. Input: company_id, website_url
2. Find leadership page URL (check common patterns, crawl links)
3. Fetch leadership page HTML
4. Clean HTML (remove scripts, nav, footer)
5. Send to LLM for extraction
6. Parse LLM response into ExtractedPerson objects
7. Match against existing people (dedup)
8. Create/update people records
9. Create company_people relationships
10. Return CollectionResult
```

### URL Patterns to Check

```python
URL_PATTERNS = [
    "/about/leadership",
    "/about/team",
    "/about-us/leadership",
    "/about-us/management",
    "/company/leadership",
    "/leadership",
    "/team",
    "/management",
    "/executives",
    "/about/board",
]
```

### Tasks

- [ ] Create page_finder.py with URL pattern checking
- [ ] Create html_cleaner.py to prep HTML for LLM
- [ ] Create website_agent.py with full extraction flow
- [ ] Download sample HTML from Fastenal, Grainger, MSC for fixtures
- [ ] Write tests using fixture HTML
- [ ] Test on 5 real company websites
- [ ] Handle edge cases: no leadership page, JavaScript-rendered pages

### Acceptance Criteria

```bash
# Run collection on Fastenal
curl -X POST http://localhost:8001/api/v1/collection/website \
  -H "Content-Type: application/json" \
  -d '{"company_id": 1}'

# Verify results
curl http://localhost:8001/api/v1/companies/1/leadership | jq '.total'
# Expected: 10+ executives
```

---

## Phase 4: SEC Filing Agent (Week 4)

### Goal
Extract executive data from SEC filings for public companies.

### Files to Create

| File | Description |
|------|-------------|
| `app/sources/people_collection/sec_agent.py` | SEC filing collection agent |
| `app/sources/people_collection/sec_client.py` | EDGAR API client |
| `app/sources/people_collection/proxy_parser.py` | DEF 14A parsing |
| `app/sources/people_collection/form8k_parser.py` | 8-K change detection |

### SEC Data Sources

| Filing | Use Case | Extraction |
|--------|----------|------------|
| DEF 14A (Proxy) | Named executive officers, compensation, bios | LLM extraction |
| 10-K | Executive officers section | LLM extraction |
| 8-K Item 5.02 | Leadership changes | LLM extraction |

### Tasks

- [ ] Create sec_client.py with EDGAR API integration
- [ ] Create proxy_parser.py for DEF 14A extraction
- [ ] Create form8k_parser.py for leadership change detection
- [ ] Create sec_agent.py to orchestrate SEC collection
- [ ] Test on Fastenal (CIK: 0000915389)
- [ ] Test on Grainger (CIK: 0000277135)
- [ ] Add compensation data to company_people table

### Acceptance Criteria

```bash
# Run SEC collection on Fastenal
curl -X POST http://localhost:8001/api/v1/collection/sec \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000915389"}'

# Verify compensation data
curl http://localhost:8001/api/v1/companies/1/leadership | jq '.leadership[0].total_compensation_usd'
# Expected: Non-null for NEOs
```

---

## Phase 5: News & Change Detection Agent (Week 5)

### Goal
Monitor press releases and detect leadership changes.

### Files to Create

| File | Description |
|------|-------------|
| `app/sources/people_collection/news_agent.py` | News/PR collection agent |
| `app/sources/people_collection/newsroom_finder.py` | Find company newsroom URLs |
| `app/sources/people_collection/pr_scraper.py` | Scrape press release lists |
| `app/sources/people_collection/change_extractor.py` | Extract changes from PR text |

### Change Detection Keywords

```python
APPOINTMENT_KEYWORDS = [
    "appoints", "names", "promotes", "announces appointment",
    "joins as", "hired as", "appointed", "elected",
]

DEPARTURE_KEYWORDS = [
    "retires", "departs", "steps down", "resigns",
    "leaves", "departure", "transition",
]
```

### Tasks

- [ ] Create newsroom_finder.py to discover press/news URLs
- [ ] Create pr_scraper.py to list and fetch press releases
- [ ] Create change_extractor.py for LLM-based change extraction
- [ ] Create news_agent.py to orchestrate news collection
- [ ] Set up scheduled job for daily/weekly news scanning
- [ ] Test on 10 companies with known recent changes
- [ ] Backfill historical changes (last 12 months)

### Acceptance Criteria

```bash
# Get recent changes
curl http://localhost:8001/api/v1/leadership-changes/feed?days=90 | jq '.count'
# Expected: 50+ changes detected

# Verify change details
curl http://localhost:8001/api/v1/leadership-changes/feed?days=90 | jq '.changes[0]'
# Expected: Has person_name, change_type, company_name, source_url
```

---

## Phase 6: Core API - People & Companies (Week 6)

### Goal
Build the API endpoints for viewing people and company leadership.

### Files to Create

| File | Description |
|------|-------------|
| `app/api/v1/people.py` | People search, profile, experience endpoints |
| `app/api/v1/companies_leadership.py` | Company leadership, org chart endpoints |
| `app/services/people_service.py` | People business logic |
| `app/services/org_chart_service.py` | Org chart building |

### Endpoints

```
# People
GET  /api/v1/people/search?q=&company=&title=&title_level=
GET  /api/v1/people/{person_id}
GET  /api/v1/people/{person_id}/experience
GET  /api/v1/people/{person_id}/education

# Company Leadership
GET  /api/v1/companies/{company_id}/leadership
GET  /api/v1/companies/{company_id}/org-chart
GET  /api/v1/companies/{company_id}/leadership-changes
GET  /api/v1/companies/{company_id}/board
```

### Tasks

- [ ] Create people_service.py with search, CRUD
- [ ] Create org_chart_service.py with hierarchy building
- [ ] Create people.py router with all endpoints
- [ ] Create companies_leadership.py router
- [ ] Add full-text search on people names
- [ ] Add filtering by title_level, department
- [ ] Write API tests

### Acceptance Criteria

```bash
# Search people
curl "http://localhost:8001/api/v1/people/search?title_level=c_suite&limit=10" | jq '.count'
# Expected: 50+

# Get company leadership
curl http://localhost:8001/api/v1/companies/1/leadership | jq '.leadership | length'
# Expected: 10+

# Get org chart
curl http://localhost:8001/api/v1/companies/1/org-chart | jq '.org_chart.root.title'
# Expected: "CEO" or similar
```

---

## Phase 7: Change Feed & Monitoring (Week 7)

### Goal
Build the leadership change feed and alerting system.

### Files to Create

| File | Description |
|------|-------------|
| `app/api/v1/changes.py` | Change feed, filtering endpoints |
| `app/services/change_service.py` | Change query, aggregation logic |
| `app/services/alert_service.py` | Alert generation, digest building |

### Endpoints

```
# Change Feed
GET  /api/v1/leadership-changes/feed?industry=&change_type=&title_level=&days=
GET  /api/v1/leadership-changes/{change_id}

# Alerts (future: webhook/email integration)
GET  /api/v1/alerts/config
POST /api/v1/alerts/config
GET  /api/v1/alerts/digest?period=weekly
```

### Tasks

- [ ] Create change_service.py with feed queries
- [ ] Create changes.py router
- [ ] Add filtering: by company, industry, change_type, title_level, date range
- [ ] Add aggregations: changes by type, by company, by month
- [ ] Create alert_service.py for digest generation
- [ ] Build weekly digest report format

### Acceptance Criteria

```bash
# Get change feed
curl "http://localhost:8001/api/v1/leadership-changes/feed?change_type=departure&title_level=c_suite&days=90" | jq '.changes | length'
# Expected: 5+

# Get change stats
curl "http://localhost:8001/api/v1/leadership-changes/stats?days=90" | jq '.by_change_type'
# Expected: {"hire": N, "departure": N, "promotion": N}
```

---

## Phase 8: Portfolio & Benchmarking (Week 8)

### Goal
Build PE-specific features: portfolios, peer sets, benchmarking.

### Files to Create

| File | Description |
|------|-------------|
| `app/api/v1/portfolios.py` | Portfolio management, dashboard |
| `app/api/v1/benchmarking.py` | Peer comparison endpoints |
| `app/services/portfolio_service.py` | Portfolio business logic |
| `app/services/benchmarking_service.py` | Team comparison, scoring |

### Endpoints

```
# Portfolios
POST /api/v1/portfolios
GET  /api/v1/portfolios/{portfolio_id}
GET  /api/v1/portfolios/{portfolio_id}/dashboard
GET  /api/v1/portfolios/{portfolio_id}/changes?days=30
POST /api/v1/portfolios/{portfolio_id}/companies

# Peer Sets
POST /api/v1/companies/{company_id}/peer-set
GET  /api/v1/companies/{company_id}/peer-set

# Benchmarking
GET  /api/v1/companies/{company_id}/benchmark
GET  /api/v1/companies/{company_id}/benchmark/team-comparison
GET  /api/v1/companies/{company_id}/benchmark/gaps
```

### Benchmarking Metrics

```python
class TeamBenchmark(BaseModel):
    company_id: int
    peer_set_avg: dict

    # Team completeness
    has_ceo: bool
    has_cfo: bool
    has_coo: bool
    has_cro: bool
    c_suite_count: int
    vp_count: int
    peer_avg_c_suite: float
    peer_avg_vp: float

    # Tenure
    ceo_tenure_months: int
    cfo_tenure_months: int
    avg_c_suite_tenure: float
    peer_avg_ceo_tenure: float

    # Gaps
    roles_missing_vs_peers: List[str]  # ["CDO", "CHRO"]
    roles_extra_vs_peers: List[str]

    # Score
    team_score: float  # 0-100
    peer_avg_score: float
```

### Tasks

- [ ] Create portfolio_service.py
- [ ] Create portfolios.py router
- [ ] Create benchmarking_service.py with comparison logic
- [ ] Create benchmarking.py router
- [ ] Build team completeness scoring
- [ ] Build tenure comparison
- [ ] Build role gap analysis
- [ ] Create dashboard aggregation endpoint

### Acceptance Criteria

```bash
# Create portfolio
curl -X POST http://localhost:8001/api/v1/portfolios \
  -H "Content-Type: application/json" \
  -d '{"name": "Industrial Fund II", "pe_firm": "Acme Capital"}'

# Get portfolio dashboard
curl http://localhost:8001/api/v1/portfolios/1/dashboard | jq '.summary'
# Expected: {"total_companies": N, "total_executives": N, "recent_changes": N}

# Get benchmark
curl http://localhost:8001/api/v1/companies/1/benchmark | jq '.team_score'
# Expected: 0-100 score
```

---

## Phase 9: Watchlists & Key Player Tracking (Week 9)

### Goal
Build executive watchlists and tracking features.

### Files to Create

| File | Description |
|------|-------------|
| `app/api/v1/watchlists.py` | Watchlist CRUD, alerts |
| `app/services/watchlist_service.py` | Watchlist business logic |

### Endpoints

```
# Watchlists
POST /api/v1/watchlists
GET  /api/v1/watchlists
GET  /api/v1/watchlists/{watchlist_id}
POST /api/v1/watchlists/{watchlist_id}/people
DELETE /api/v1/watchlists/{watchlist_id}/people/{person_id}

# Watchlist Alerts
GET  /api/v1/watchlists/{watchlist_id}/changes?days=30
```

### Tasks

- [ ] Create watchlist_service.py
- [ ] Create watchlists.py router
- [ ] Add person to watchlist with notes/tags
- [ ] Track changes for watchlisted people
- [ ] Build watchlist change feed
- [ ] Add search: find candidates by criteria

### Acceptance Criteria

```bash
# Create watchlist
curl -X POST http://localhost:8001/api/v1/watchlists \
  -H "Content-Type: application/json" \
  -d '{"name": "CFO Candidates"}'

# Add person to watchlist
curl -X POST http://localhost:8001/api/v1/watchlists/1/people \
  -H "Content-Type: application/json" \
  -d '{"person_id": 123, "notes": "Strong candidate"}'

# Get watchlist changes
curl http://localhost:8001/api/v1/watchlists/1/changes | jq '.changes'
```

---

## Phase 10: Industry Analytics & Reports (Week 10)

### Goal
Build industry-wide analytics and export capabilities.

### Files to Create

| File | Description |
|------|-------------|
| `app/api/v1/analytics.py` | Industry stats, trends |
| `app/api/v1/reports.py` | Export endpoints (PDF, Excel) |
| `app/services/analytics_service.py` | Aggregation, trend analysis |
| `app/services/report_service.py` | Report generation |

### Endpoints

```
# Analytics
GET  /api/v1/industries/{industry}/stats
GET  /api/v1/industries/{industry}/trends?months=12
GET  /api/v1/industries/{industry}/talent-flow

# Reports
POST /api/v1/reports/management-assessment
POST /api/v1/reports/peer-comparison
GET  /api/v1/reports/{report_id}
GET  /api/v1/reports/{report_id}/download
```

### Analytics Metrics

```python
class IndustryStats(BaseModel):
    industry: str
    period_days: int

    total_companies: int
    total_executives: int

    changes_summary: dict  # by type
    avg_ceo_tenure_months: float
    avg_cfo_tenure_months: float

    talent_flow: dict  # which companies gaining/losing
    hot_roles: List[str]  # most hired roles

    instability_flags: List[dict]  # companies with 3+ C-suite changes
```

### Tasks

- [ ] Create analytics_service.py
- [ ] Create analytics.py router
- [ ] Build industry stats aggregation
- [ ] Build talent flow analysis (net importer/exporter)
- [ ] Create report_service.py
- [ ] Build management assessment report template
- [ ] Build peer comparison report template
- [ ] Add PDF/Excel export

### Acceptance Criteria

```bash
# Get industry stats
curl http://localhost:8001/api/v1/industries/industrial_distribution/stats | jq '.total_executives'
# Expected: 2000+

# Generate report
curl -X POST http://localhost:8001/api/v1/reports/management-assessment \
  -H "Content-Type: application/json" \
  -d '{"company_id": 1}'

# Download report
curl http://localhost:8001/api/v1/reports/1/download -o report.pdf
```

---

## Phase 11: Enrichment & Data Quality (Week 11)

### Goal
Improve data quality with LinkedIn validation, photos, email inference.

### Files to Create

| File | Description |
|------|-------------|
| `app/sources/people_collection/linkedin_validator.py` | Google-indexed LinkedIn lookup |
| `app/sources/people_collection/photo_extractor.py` | Extract/store exec photos |
| `app/sources/people_collection/email_inferrer.py` | Infer work emails from patterns |
| `app/services/data_quality_service.py` | Confidence scoring, dedup |

### Tasks

- [ ] Create linkedin_validator.py (Google site: search only)
- [ ] Create photo_extractor.py
- [ ] Create email_inferrer.py with company pattern detection
- [ ] Create data_quality_service.py
- [ ] Add confidence scores to all records
- [ ] Improve deduplication logic
- [ ] Build data quality dashboard

### Acceptance Criteria

```bash
# Check data quality stats
curl http://localhost:8001/api/v1/data-quality/stats | jq '.'
# Expected:
# {
#   "total_people": 2000+,
#   "with_linkedin": 60%+,
#   "with_photo": 50%+,
#   "with_email": 40%+,
#   "avg_confidence": 0.75+
# }
```

---

## Phase 12: Scheduled Jobs & Monitoring (Week 12)

### Goal
Set up automated collection, monitoring, and alerting.

### Files to Create

| File | Description |
|------|-------------|
| `app/jobs/collection_scheduler.py` | Schedule collection runs |
| `app/jobs/change_monitor.py` | Monitor for new changes |
| `app/jobs/digest_sender.py` | Send weekly digests |

### Scheduled Jobs

| Job | Schedule | Description |
|-----|----------|-------------|
| `refresh_websites` | Weekly (Sunday) | Re-crawl all company websites |
| `check_8k_filings` | Daily (6am) | Check for new 8-K filings |
| `scan_newsrooms` | Daily (8am) | Scan company newsrooms |
| `send_change_digest` | Weekly (Monday 8am) | Send weekly change digest |
| `refresh_benchmarks` | Weekly | Recalculate all benchmarks |

### Tasks

- [ ] Create collection_scheduler.py with APScheduler
- [ ] Create change_monitor.py for continuous monitoring
- [ ] Create digest_sender.py
- [ ] Set up job monitoring/logging
- [ ] Add retry logic for failed collections
- [ ] Build admin dashboard for job status

### Acceptance Criteria

```bash
# Check job status
curl http://localhost:8001/api/v1/admin/jobs | jq '.jobs'
# Expected: List of scheduled jobs with last_run, next_run, status

# Verify collection is fresh
curl http://localhost:8001/api/v1/data-quality/freshness | jq '.median_age_days'
# Expected: < 30
```

---

## Summary: Files to Create

### Phase 1 - Database
- `app/core/models/people.py`
- `app/core/models/org_chart.py`
- `app/core/models/portfolio.py`
- `alembic/versions/xxx_create_people_tables.py`
- `data/seeds/industrial_companies.json`
- `scripts/seed_companies.py`

### Phase 2-5 - Collection
- `app/sources/people_collection/__init__.py`
- `app/sources/people_collection/types.py`
- `app/sources/people_collection/config.py`
- `app/sources/people_collection/base_collector.py`
- `app/sources/people_collection/llm_extractor.py`
- `app/sources/people_collection/orchestrator.py`
- `app/sources/people_collection/website_agent.py`
- `app/sources/people_collection/page_finder.py`
- `app/sources/people_collection/html_cleaner.py`
- `app/sources/people_collection/sec_agent.py`
- `app/sources/people_collection/sec_client.py`
- `app/sources/people_collection/proxy_parser.py`
- `app/sources/people_collection/form8k_parser.py`
- `app/sources/people_collection/news_agent.py`
- `app/sources/people_collection/newsroom_finder.py`
- `app/sources/people_collection/pr_scraper.py`
- `app/sources/people_collection/change_extractor.py`

### Phase 6-10 - API & Services
- `app/api/v1/people.py`
- `app/api/v1/companies_leadership.py`
- `app/api/v1/changes.py`
- `app/api/v1/portfolios.py`
- `app/api/v1/benchmarking.py`
- `app/api/v1/watchlists.py`
- `app/api/v1/analytics.py`
- `app/api/v1/reports.py`
- `app/services/people_service.py`
- `app/services/org_chart_service.py`
- `app/services/change_service.py`
- `app/services/alert_service.py`
- `app/services/portfolio_service.py`
- `app/services/benchmarking_service.py`
- `app/services/watchlist_service.py`
- `app/services/analytics_service.py`
- `app/services/report_service.py`

### Phase 11-12 - Quality & Jobs
- `app/sources/people_collection/linkedin_validator.py`
- `app/sources/people_collection/photo_extractor.py`
- `app/sources/people_collection/email_inferrer.py`
- `app/services/data_quality_service.py`
- `app/jobs/collection_scheduler.py`
- `app/jobs/change_monitor.py`
- `app/jobs/digest_sender.py`

---

## Approval Checklist

- [ ] Phase 1: Database tables approved
- [ ] Phase 2-5: Collection agents approach approved
- [ ] Phase 6-7: Core API design approved
- [ ] Phase 8-10: PE features (portfolio, benchmarking, watchlists) approved
- [ ] Phase 11-12: Enrichment and automation approved
- [ ] Ready to begin implementation

---

## Next Steps After Approval

1. Start with Phase 1: Create database models and migration
2. Seed 200+ industrial companies
3. Build collection infrastructure
4. Iterate through phases, testing at each checkpoint

**Estimated total effort:** 12 weeks
