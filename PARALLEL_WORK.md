# Parallel Work Queue

> **Agents: Read this file first. Claim an unclaimed task, update status, then execute.**

---

## How It Works

```
1. READ    → Check the task queue below
2. CLAIM   → Find a task with status "NOT_STARTED", change to "IN_PROGRESS [Agent X]"
3. PLAN    → Write plan in docs/plans/PLAN_XXX_<name>.md (if needed)
4. EXECUTE → Only touch files in YOUR task's scope
5. TEST    → docker compose up -d --build, curl endpoints
6. DONE    → Mark status "COMPLETE", update Communication Log
```

**Rules:**
- Only work on ONE task at a time
- Don't touch files owned by other tasks
- If task depends on another, wait for it to complete
- Tab 1 / first agent handles final integration commits

---

## Phase 1 Summary: Agentic Infrastructure (T01-T10) ✅

Phase 1 built the core infrastructure for automated portfolio data collection.

| ID | Task | What It Does | Key Files |
|----|------|--------------|-----------|
| T01 | Retry Handler | Exponential backoff with jitter, circuit breaker, HTTP 429 handling | `retry_handler.py`, `strategies/base.py` |
| T02 | Fuzzy Matching | Levenshtein distance for company name deduplication (85% threshold) | `fuzzy_matcher.py`, `synthesizer.py` |
| T03 | Response Caching | In-memory TTL cache with optional Redis, async decorator | `cache.py` |
| T04 | LLM Client Tests | 36 unit tests for OpenAI/Anthropic, retry logic, JSON parsing | `tests/test_llm_client.py` |
| T05 | Ticker Resolver Tests | 48 tests for ticker resolution, CUSIP fallback, batch operations | `tests/test_ticker_resolver.py` |
| T06 | Metrics & Monitoring | Job/strategy/investor metrics, 4 API endpoints, real-time tracking | `metrics.py`, `agentic_research.py` |
| T07 | Scheduled Updates | APScheduler integration, quarterly refresh, priority queue for stale data | `scheduler.py` |
| T08 | Portfolio Export | CSV/Excel export with multi-sheet formatting, filters | `exporter.py`, export endpoint |
| T09 | PDF Caching | 24-hour cache for parsed annual report PDFs | `annual_report_strategy.py` |
| T10 | JS Rendering | Playwright for JavaScript-heavy pages, static fallback | `website_strategy.py` |

**Phase 1 Achievements:**
- ✅ Robust error handling and retry logic across all data collection
- ✅ Intelligent deduplication with fuzzy matching
- ✅ Performance optimization via caching (HTTP, PDF, robots.txt)
- ✅ Comprehensive test coverage (84+ tests)
- ✅ Real-time metrics and monitoring
- ✅ Automated scheduling for data freshness
- ✅ Data export for analysis workflows

---

## Phase 2 Summary: Bringing Data to People (T11-T20) ✅

Phase 2 made collected data accessible, searchable, and actionable for end users.

| ID | Task | What It Does | Key Files |
|----|------|--------------|-----------|
| T11 | Portfolio Change Alerts | Subscription-based alerts for portfolio changes (new/removed holdings) | `alerts.py`, `api/v1/alerts.py` |
| T12 | Full-Text Search API | PostgreSQL FTS with fuzzy matching, faceted search, autocomplete | `search/engine.py`, `api/v1/search.py` |
| T13 | Dashboard Analytics API | System overview, investor analytics, trends, industry breakdown | `analytics/dashboard.py`, `api/v1/analytics.py` |
| T16 | GraphQL API Layer | Strawberry GraphQL with nested relationships, DataLoader | `graphql/schema.py`, `graphql/resolvers.py` |
| T17 | Portfolio Comparison Tool | Side-by-side comparison, Jaccard similarity, historical diff | `analytics/comparison.py`, `api/v1/compare.py` |
| T18 | Investor Similarity | Find similar investors, "also invest in" recommendations | `analytics/recommendations.py`, `api/v1/discover.py` |
| T19 | Public API with Auth | API key auth, rate limiting, usage tracking | `auth/api_keys.py`, `api/v1/public.py` |
| T20 | Watchlists & Saved Searches | Track investors/companies, save and re-execute searches | `users/watchlists.py`, `api/v1/watchlists.py` |

**Phase 2 Achievements:**
- ✅ Full-text search with typo tolerance across 4000+ records
- ✅ Real-time portfolio change alerts with subscriptions
- ✅ Pre-computed analytics for dashboards
- ✅ GraphQL API for flexible frontend queries
- ✅ Portfolio comparison and similarity analysis
- ✅ Public API with authentication and rate limiting
- ✅ User watchlists and saved searches

---

## Phase 3: Investment Intelligence (T21-T30)

**Mission:** Transform raw data into actionable investment intelligence through network analysis, enrichment, and advanced analytics.

### Task Queue

| ID | Task | Status | Agent | Files (Scope) | Dependencies |
|----|------|--------|-------|---------------|--------------|
| T21 | Co-investor Network Graph | COMPLETE | Tab 1 | `app/network/graph.py`, `app/api/v1/network.py` | None |
| T22 | Company Data Enrichment | COMPLETE | Tab 1 | `app/enrichment/company.py`, `app/api/v1/enrichment.py` | None |
| T23 | Investment Trend Analysis | COMPLETE | Tab 2 | `app/analytics/trends.py`, `app/api/v1/trends.py` | None |
| T24 | News & Event Feed | COMPLETE | Tab 2 | `app/news/aggregator.py`, `app/api/v1/news.py` | None |
| T25 | Custom Report Builder | COMPLETE | Tab 2 | `app/reports/builder.py`, `app/api/v1/reports.py` | None |
| T26 | Bulk Portfolio Import | COMPLETE | Tab 1 | `app/import_data/portfolio.py`, `app/api/v1/import_portfolio.py` | None |
| T27 | LP Profile Enrichment | COMPLETE | Tab 1 | `app/enrichment/investor.py` | None |
| T28 | Deal Flow Tracker | COMPLETE | Tab 2 | `app/deals/tracker.py`, `app/api/v1/deals.py` | None |
| T29 | Market Benchmarks | COMPLETE | Tab 1 | `app/analytics/benchmarks.py`, `app/api/v1/benchmarks.py` | T23 |
| T30 | User Auth & Workspaces | COMPLETE | Tab 2 | `app/users/auth.py`, `app/users/workspaces.py` | T19 |

---

## Phase 4: Data Expansion & Predictive Intelligence (T31-T40)

**Mission:** Expand data coverage with new sources and add predictive/ML capabilities for investment insights.

### Task Queue

| ID | Task | Status | Agent | Files (Scope) | Dependencies |
|----|------|--------|-------|---------------|--------------|
| T31 | SEC Form D Filings | COMPLETE | Tab 1 | `app/sources/sec_form_d/`, `app/api/v1/form_d.py` | None |
| T32 | SEC Form ADV Data | COMPLETE | Tab 1 | `app/sources/sec_form_adv/`, `app/api/v1/form_adv.py` | None |
| T33 | OpenCorporates Integration | COMPLETE | Tab 2 | `app/sources/opencorporates/`, `app/api/v1/corporate_registry.py` | None |
| T34 | GitHub Repository Analytics | COMPLETE | Tab 1 | `app/sources/github/`, `app/api/v1/github.py` | None |
| T35 | Web Traffic Data (SimilarWeb) | COMPLETE | Tab 2 | `app/sources/web_traffic/`, `app/api/v1/web_traffic.py` | None |
| T36 | Company Scoring Model | NOT_STARTED | - | `app/ml/company_scorer.py`, `app/api/v1/scores.py` | T22 |
| T37 | Entity Resolution Service | NOT_STARTED | - | `app/core/entity_resolver.py`, `app/api/v1/entities.py` | None |
| T38 | Glassdoor Company Data | NOT_STARTED | - | `app/sources/glassdoor/`, `app/api/v1/glassdoor.py` | None |
| T39 | App Store Rankings | NOT_STARTED | - | `app/sources/app_stores/`, `app/api/v1/app_rankings.py` | None |
| T40 | Predictive Deal Scoring | NOT_STARTED | - | `app/ml/deal_scorer.py`, `app/api/v1/predictions.py` | T28, T36 |

---

## Task Details

### Phase 3 Tasks

### T21: Co-investor Network Graph
**Goal:** Build network data structure showing investor relationships based on shared investments.

**Scope:**
- Create `app/network/graph.py` with:
  - Co-investor relationship calculation (shared portfolio companies)
  - Relationship strength scoring (# shared investments, recency)
  - Network centrality metrics (most connected LPs)
  - Cluster detection (investor groups with high overlap)
  - Graph export for visualization (nodes/edges JSON)
- Add `GET /api/v1/network/investor/{id}` - get investor's co-investor network
- Add `GET /api/v1/network/graph` - full network data for visualization
- Add `GET /api/v1/network/clusters` - detected investor clusters
- Add `GET /api/v1/network/central` - most connected investors

**Plan:** `docs/plans/PLAN_T21_network.md`

---

### T22: Company Data Enrichment
**Goal:** Enrich portfolio company data with financials, funding, and growth metrics.

**Scope:**
- Create `app/enrichment/company.py` with:
  - SEC EDGAR integration (revenue, assets from 10-K/10-Q)
  - Funding data aggregation (rounds, valuations)
  - Employee count tracking (LinkedIn, company sites)
  - Industry classification enrichment
  - Company status (active, acquired, IPO, bankrupt)
- Add `POST /api/v1/enrichment/company/{id}` - trigger enrichment job
- Add `GET /api/v1/enrichment/company/{id}/status` - enrichment status
- Add `GET /api/v1/companies/{id}/financials` - get enriched data
- Add `POST /api/v1/enrichment/batch` - bulk enrichment

**Plan:** `docs/plans/PLAN_T22_company_enrichment.md`

---

### T23: Investment Trend Analysis
**Goal:** Surface investment trends across LP portfolios (sector rotation, emerging themes).

**Scope:**
- Create `app/analytics/trends.py` with:
  - Sector allocation trends over time
  - Emerging sector detection (accelerating investment)
  - Declining sector detection (divestment patterns)
  - Geographic trends (where money is flowing)
  - Stage trends (early vs late stage shifts)
  - LP behavior clustering (growth-focused vs value-focused)
- Add `GET /api/v1/trends/sectors` - sector allocation trends
- Add `GET /api/v1/trends/emerging` - hot sectors with momentum
- Add `GET /api/v1/trends/geographic` - investment by region
- Add `GET /api/v1/trends/stages` - investment stage trends

**Plan:** `docs/plans/PLAN_T23_trends.md`

---

### T24: News & Event Feed
**Goal:** Aggregate news and events relevant to tracked investors and portfolio companies.

**Scope:**
- Create `app/news/aggregator.py` with:
  - SEC filing alerts (13F, 13D, 8-K, 10-K)
  - Company news aggregation (RSS, news APIs)
  - Funding announcement detection
  - M&A and IPO tracking
  - Event deduplication and relevance scoring
- Add `GET /api/v1/news/feed` - personalized news feed
- Add `GET /api/v1/news/investor/{id}` - news for specific investor
- Add `GET /api/v1/news/company/{id}` - news for specific company
- Add `GET /api/v1/events/calendar` - upcoming events (earnings, filings)

**Plan:** `docs/plans/PLAN_T24_news.md`

---

### T25: Custom Report Builder
**Goal:** Generate customizable PDF/Excel reports for sharing insights.

**Scope:**
- Create `app/reports/builder.py` with:
  - Report templates (investor profile, portfolio summary, comparison)
  - Dynamic content sections (charts, tables, text)
  - PDF generation (WeasyPrint or ReportLab)
  - Excel generation (multi-sheet with charts)
  - Scheduled report generation
  - Report history and versioning
- Add `POST /api/v1/reports/generate` - create report
- Add `GET /api/v1/reports/templates` - available templates
- Add `GET /api/v1/reports/{id}` - download generated report
- Add `POST /api/v1/reports/schedule` - schedule recurring reports

**Plan:** `docs/plans/PLAN_T25_reports.md`

---

### T26: Bulk Portfolio Import
**Goal:** Allow users to upload their own portfolio data via CSV/Excel.

**Scope:**
- Create `app/import/portfolio.py` with:
  - CSV/Excel parsing with flexible column mapping
  - Data validation and error reporting
  - Company name matching (fuzzy match to existing)
  - Duplicate detection and merge strategies
  - Import preview before commit
  - Import history and rollback
- Add `POST /api/v1/import/upload` - upload file
- Add `POST /api/v1/import/preview` - preview import results
- Add `POST /api/v1/import/confirm` - confirm and execute import
- Add `GET /api/v1/import/history` - past imports
- Add `POST /api/v1/import/{id}/rollback` - undo import

**Plan:** `docs/plans/PLAN_T26_import.md`

---

### T27: LP Profile Enrichment
**Goal:** Enrich investor profiles with contact data, AUM history, and preferences.

**Scope:**
- Create `app/enrichment/investor.py` with:
  - Contact information lookup (key personnel)
  - AUM history tracking over time
  - Investment preference extraction (sectors, stages, geography)
  - Commitment pace analysis (how often they invest)
  - LP classification refinement
- Add `POST /api/v1/enrichment/investor/{id}` - trigger enrichment
- Add `GET /api/v1/investors/{id}/contacts` - key contacts
- Add `GET /api/v1/investors/{id}/aum-history` - AUM over time
- Add `GET /api/v1/investors/{id}/preferences` - investment preferences

**Plan:** `docs/plans/PLAN_T27_lp_enrichment.md`

---

### T28: Deal Flow Tracker
**Goal:** Track potential investment opportunities through a pipeline.

**Scope:**
- Create `app/deals/tracker.py` with:
  - Deal/opportunity CRUD
  - Pipeline stages (sourced, reviewing, due diligence, closed)
  - Deal tagging and categorization
  - Activity logging (notes, meetings, documents)
  - Deal scoring and prioritization
  - Team collaboration (assign, comment)
- Add `POST /api/v1/deals` - create deal
- Add `GET /api/v1/deals` - list deals with filters
- Add `PATCH /api/v1/deals/{id}` - update deal stage
- Add `POST /api/v1/deals/{id}/activities` - log activity
- Add `GET /api/v1/deals/pipeline` - pipeline summary

**Plan:** `docs/plans/PLAN_T28_deals.md`

---

### T29: Market Benchmarks
**Goal:** Compare LP performance and allocations against market benchmarks.

**Scope:**
- Create `app/analytics/benchmarks.py` with:
  - Peer group construction (similar LPs by type, size)
  - Allocation benchmarks (median sector allocation by LP type)
  - Diversification scoring vs peers
  - Performance proxies (portfolio company outcomes)
  - Benchmark trend tracking
- Add `GET /api/v1/benchmarks/investor/{id}` - investor vs benchmark
- Add `GET /api/v1/benchmarks/peer-group/{id}` - peer comparison
- Add `GET /api/v1/benchmarks/sectors` - sector allocation benchmarks
- Add `GET /api/v1/benchmarks/diversification` - diversification scores

**Dependencies:** T23 (trends) for baseline data

**Plan:** `docs/plans/PLAN_T29_benchmarks.md`

---

### T30: User Auth & Workspaces
**Goal:** Add user authentication and team workspaces for collaboration.

**Scope:**
- Create `app/users/auth.py` with:
  - JWT-based authentication
  - User registration and login
  - Password reset flow
  - OAuth integration (Google, Microsoft)
  - Session management
- Create `app/users/workspaces.py` with:
  - Team/workspace creation
  - Member invitations and roles (admin, member, viewer)
  - Shared watchlists and saved searches
  - Workspace-scoped data isolation
- Add `POST /api/v1/auth/register` - user registration
- Add `POST /api/v1/auth/login` - login, get JWT
- Add `POST /api/v1/workspaces` - create workspace
- Add `POST /api/v1/workspaces/{id}/invite` - invite member

**Dependencies:** T19 (API keys) for auth foundation

**Plan:** `docs/plans/PLAN_T30_auth.md`

---

### Phase 4 Tasks

### T31: SEC Form D Filings
**Goal:** Ingest private placement filings to track unregistered securities offerings.

**Scope:**
- Create `app/sources/sec_form_d/` with:
  - Form D filing search and download from SEC EDGAR
  - Parse XML filings for offering details
  - Extract issuer info, offering amount, investor types
  - Track amendments and related filings
- Add `GET /api/v1/form-d/search` - search filings by company/date
- Add `GET /api/v1/form-d/issuer/{cik}` - filings by issuer
- Add `GET /api/v1/form-d/recent` - recent private placements
- Add `POST /api/v1/form-d/ingest` - trigger ingestion job

**Why:** Form D reveals early-stage funding rounds, fund formations, and private offerings not visible elsewhere.

---

### T32: SEC Form ADV Data
**Goal:** Ingest investment adviser registration data for LP/fund intelligence.

**Scope:**
- Create `app/sources/sec_form_adv/` with:
  - Form ADV Part 1 and Part 2 parsing
  - Extract AUM, client types, fee structures
  - Identify key personnel and ownership
  - Track regulatory history and disclosures
- Add `GET /api/v1/form-adv/search` - search advisers
- Add `GET /api/v1/form-adv/adviser/{crd}` - adviser details
- Add `GET /api/v1/form-adv/aum-rankings` - top advisers by AUM
- Add `POST /api/v1/form-adv/ingest` - trigger ingestion

**Why:** Form ADV provides official AUM data, fee structures, and personnel info for investment advisers.

---

### T33: OpenCorporates Integration
**Goal:** Access global company registry data for entity verification.

**Scope:**
- Create `app/sources/opencorporates/` with:
  - Company search across 140+ jurisdictions
  - Officer and director lookups
  - Corporate structure mapping
  - Incorporation date and status tracking
- Add `GET /api/v1/corporate-registry/search` - company search
- Add `GET /api/v1/corporate-registry/company/{jurisdiction}/{number}` - company details
- Add `GET /api/v1/corporate-registry/company/{jurisdiction}/{number}/officers` - officers list
- Add `GET /api/v1/corporate-registry/company/{jurisdiction}/{number}/filings` - filing history
- Add `GET /api/v1/corporate-registry/officers/search` - officer search
- Add `GET /api/v1/corporate-registry/jurisdictions` - list jurisdictions

**Why:** Verifies company existence, tracks international subsidiaries, identifies beneficial owners.

**Plan:** `docs/plans/PLAN_T33_opencorporates.md`

---

### T34: GitHub Repository Analytics
**Goal:** Track developer activity as a proxy for tech company health.

**Scope:**
- Create `app/sources/github/` with:
  - Organization and repository discovery
  - Commit activity and contributor trends
  - Stars, forks, issues metrics over time
  - Language and dependency analysis
  - Developer velocity scoring
- Add `GET /api/v1/github/org/{org}` - organization overview
- Add `GET /api/v1/github/org/{org}/repos` - repository list
- Add `GET /api/v1/github/org/{org}/activity` - activity trends
- Add `GET /api/v1/github/org/{org}/score` - developer velocity score

**Why:** GitHub activity correlates with engineering team health and product velocity.

---

### T35: Web Traffic Data
**Goal:** Track website traffic as alternative data for company performance.

**Scope:**
- Create `app/sources/web_traffic/` with:
  - Monthly visit estimates (SimilarWeb if available)
  - Traffic source breakdown (direct, search, referral)
  - Geographic distribution
  - Competitor benchmarking
  - Tranco rankings (free, top 1M domains)
- Add `GET /api/v1/web-traffic/domain/{domain}` - traffic overview
- Add `GET /api/v1/web-traffic/domain/{domain}/history` - historical trends
- Add `GET /api/v1/web-traffic/compare` - compare multiple domains
- Add `GET /api/v1/web-traffic/rankings` - top domain rankings
- Add `GET /api/v1/web-traffic/search` - search domains by keyword
- Add `GET /api/v1/web-traffic/providers` - list available providers

**Why:** Web traffic is a leading indicator for consumer companies and SaaS products.

**Plan:** `docs/plans/PLAN_T35_web_traffic.md`

---

### T36: Company Scoring Model
**Goal:** ML-based scoring model for portfolio company health assessment.

**Scope:**
- Create `app/ml/company_scorer.py` with:
  - Feature engineering from enriched data (T22)
  - Composite scoring (0-100) based on multiple signals
  - Category scores: growth, stability, market position
  - Confidence intervals and explainability
  - Model versioning and A/B testing
- Add `GET /api/v1/scores/company/{name}` - company score
- Add `GET /api/v1/scores/portfolio/{investor_id}` - portfolio scores
- Add `GET /api/v1/scores/rankings` - top/bottom scored companies
- Add `GET /api/v1/scores/methodology` - scoring methodology

**Dependencies:** T22 (Company Data Enrichment)

**Why:** Quantifies company health into actionable scores for portfolio monitoring.

---

### T37: Entity Resolution Service
**Goal:** Intelligent matching and deduplication across data sources.

**Scope:**
- Create `app/core/entity_resolver.py` with:
  - Fuzzy name matching with configurable thresholds
  - Multi-attribute matching (name + location + industry)
  - Canonical entity ID assignment
  - Match confidence scoring
  - Manual override capability
  - Merge/split entity management
- Add `GET /api/v1/entities/resolve` - resolve entity name
- Add `GET /api/v1/entities/{id}/aliases` - entity aliases
- Add `POST /api/v1/entities/merge` - merge duplicate entities
- Add `GET /api/v1/entities/duplicates` - potential duplicates

**Why:** Clean entity matching is critical for accurate cross-source data integration.

---

### T38: Glassdoor Company Data
**Goal:** Integrate company reviews and salary data for talent intelligence.

**Scope:**
- Create `app/sources/glassdoor/` with:
  - Company rating and review aggregation
  - Salary data by role and location
  - CEO approval ratings
  - Interview difficulty and experience
  - Company culture insights
- Add `GET /api/v1/glassdoor/company/{name}` - company overview
- Add `GET /api/v1/glassdoor/company/{name}/salaries` - salary data
- Add `GET /api/v1/glassdoor/company/{name}/reviews` - review summary
- Add `GET /api/v1/glassdoor/compare` - compare companies

**Why:** Employee sentiment and compensation data reveals company culture and talent competitiveness.

---

### T39: App Store Rankings
**Goal:** Track mobile app performance for consumer tech companies.

**Scope:**
- Create `app/sources/app_stores/` with:
  - iOS App Store and Google Play tracking
  - Rankings by category and country
  - Download estimates
  - Rating trends and review sentiment
  - Version history and update frequency
- Add `GET /api/v1/apps/search` - search apps
- Add `GET /api/v1/apps/{app_id}` - app details
- Add `GET /api/v1/apps/{app_id}/rankings` - ranking history
- Add `GET /api/v1/apps/company/{company}` - company's apps

**Why:** App store metrics are leading indicators for consumer mobile companies.

---

### T40: Predictive Deal Scoring
**Goal:** ML model to score deal opportunities in the pipeline.

**Scope:**
- Create `app/ml/deal_scorer.py` with:
  - Feature engineering from deal attributes
  - Integration with company scores (T36)
  - Win probability prediction
  - Optimal timing recommendations
  - Similar successful deals identification
- Add `GET /api/v1/predictions/deal/{id}` - deal score
- Add `GET /api/v1/predictions/pipeline` - scored pipeline
- Add `GET /api/v1/predictions/similar/{deal_id}` - similar deals
- Add `GET /api/v1/predictions/insights` - pipeline insights

**Dependencies:** T28 (Deal Flow Tracker), T36 (Company Scoring)

**Why:** Prioritizes deal pipeline based on predicted success probability.

---

### Phase 2 Tasks (Archived)

### T11: Portfolio Change Alerts
**Goal:** Notify users when portfolio data changes (new holdings, exits, value changes).

**Scope:**
- Create `app/notifications/alerts.py` with:
  - Change detection (new companies, removed companies, value changes)
  - Alert rules engine (threshold-based triggers)
  - Multi-channel support (in-app, email, webhook)
  - Alert history and acknowledgment
- Add `POST /api/v1/alerts/subscribe` - subscribe to investor alerts
- Add `GET /api/v1/alerts` - list user's alerts
- Add `DELETE /api/v1/alerts/{id}` - unsubscribe

**Plan:** `docs/plans/PLAN_T11_alerts.md`

---

### T12: Full-Text Search API
**Goal:** Enable fast, typo-tolerant search across investors, companies, and portfolios.

**Scope:**
- Create `app/search/engine.py` with:
  - PostgreSQL full-text search (GIN indexes)
  - Fuzzy matching for typo tolerance
  - Faceted search (filter by type, industry, location)
  - Search result ranking and relevance scoring
  - Search suggestions/autocomplete
- Add `GET /api/v1/search` - unified search endpoint
- Add `GET /api/v1/search/suggest` - autocomplete suggestions

**Plan:** `docs/plans/PLAN_T12_search.md`

---

### T13: Dashboard Analytics API
**Goal:** Provide pre-computed analytics for frontend dashboards.

**Scope:**
- Create `app/analytics/dashboard.py` with:
  - Portfolio growth over time
  - Industry distribution trends
  - Top performers and movers
  - Data quality scores
  - Collection activity summary
- Add `GET /api/v1/analytics/overview` - system-wide stats
- Add `GET /api/v1/analytics/investor/{id}` - investor-specific analytics
- Add `GET /api/v1/analytics/trends` - time-series data

**Plan:** `docs/plans/PLAN_T13_dashboard.md`

---

### T14: Webhook Integrations
**Goal:** Push data updates to external systems (Slack, CRMs, data warehouses).

**Scope:**
- Create `app/integrations/webhooks.py` with:
  - Webhook registration and management
  - Event types (portfolio.updated, investor.new, alert.triggered)
  - Retry logic with exponential backoff
  - Webhook signature verification (HMAC)
  - Delivery logs and debugging
- Add `POST /api/v1/webhooks` - register webhook
- Add `GET /api/v1/webhooks` - list webhooks
- Add `POST /api/v1/webhooks/{id}/test` - send test payload

**Plan:** `docs/plans/PLAN_T14_webhooks.md`

---

### T15: Email Digest Reports
**Goal:** Send periodic email summaries of portfolio changes and alerts.

**Scope:**
- Create `app/notifications/digest.py` with:
  - Daily/weekly/monthly digest options
  - HTML email templates (responsive)
  - Digest content aggregation
  - Unsubscribe handling
- Create `app/notifications/templates/` with:
  - `digest_daily.html`
  - `digest_weekly.html`
  - `alert_notification.html`
- Add `POST /api/v1/digests/subscribe` - configure digest preferences
- Add `GET /api/v1/digests/preview` - preview next digest

**Dependencies:** T11 (alerts) for change detection logic

**Plan:** `docs/plans/PLAN_T15_digest.md`

---

### T16: GraphQL API Layer
**Goal:** Provide flexible data querying for complex frontend needs.

**Scope:**
- Create `app/graphql/schema.py` with:
  - Investor type (LP, FamilyOffice)
  - PortfolioCompany type with connections
  - CoInvestor relationships
  - Query types for all entities
- Create `app/graphql/resolvers.py` with:
  - DataLoader for N+1 prevention
  - Pagination (cursor-based)
  - Field-level permissions
- Add `POST /graphql` endpoint

**Plan:** `docs/plans/PLAN_T16_graphql.md`

---

### T17: Portfolio Comparison Tool
**Goal:** Compare portfolios side-by-side (investor vs investor, or over time).

**Scope:**
- Create `app/analytics/comparison.py` with:
  - Portfolio overlap calculation
  - Unique holdings identification
  - Industry allocation diff
  - Time-based comparison (Q1 vs Q2)
  - Exportable comparison reports
- Add `POST /api/v1/compare/portfolios` - compare two investors
- Add `GET /api/v1/compare/investor/{id}/history` - compare over time

**Plan:** `docs/plans/PLAN_T17_comparison.md`

---

### T18: Investor Similarity & Recommendations
**Goal:** Find similar investors based on portfolio overlap and investment patterns.

**Scope:**
- Create `app/analytics/recommendations.py` with:
  - Similarity scoring (Jaccard index on holdings)
  - Investment pattern clustering
  - "Investors like X also invest in Y" recommendations
  - Similar company suggestions
- Add `GET /api/v1/discover/similar/{investor_id}` - find similar investors
- Add `GET /api/v1/discover/recommended/{investor_id}` - recommended companies

**Dependencies:** T12 (search) for efficient lookups

**Plan:** `docs/plans/PLAN_T18_recommendations.md`

---

### T19: Public API with Auth & Rate Limits
**Goal:** Expose data via authenticated public API for external developers.

**Scope:**
- Create `app/api/public/` with:
  - Versioned public endpoints (v1)
  - OpenAPI documentation
  - Response envelope (data, meta, errors)
- Create `app/auth/api_keys.py` with:
  - API key generation and management
  - Rate limiting (token bucket)
  - Usage tracking and quotas
  - Scope-based permissions
- Add `POST /api/v1/api-keys` - generate API key
- Add `GET /api/v1/api-keys/usage` - check usage stats

**Plan:** `docs/plans/PLAN_T19_public_api.md`

---

### T20: Saved Searches & Watchlists
**Goal:** Let users save searches and track specific investors/companies.

**Scope:**
- Create `app/users/watchlists.py` with:
  - Watchlist CRUD operations
  - Saved search queries
  - Watchlist sharing (public/private)
  - Change notifications for watched items
- Add `POST /api/v1/watchlists` - create watchlist
- Add `GET /api/v1/watchlists` - list user's watchlists
- Add `POST /api/v1/watchlists/{id}/items` - add to watchlist
- Add `POST /api/v1/searches/save` - save search query

**Dependencies:** T12 (search) for saved search execution

**Plan:** `docs/plans/PLAN_T20_watchlists.md`

---

## Communication Log

```
[SYSTEM] Phase 1 (T01-T10) complete. Phase 2 initialized with 10 new tasks.
[SYSTEM] Phase 2 focus: Bringing data to people - accessibility, search, notifications, integrations.
[Agent-T10] T10 COMPLETE: Implemented JS rendering support in website_strategy.py. Added Playwright for JS-heavy page detection and rendering with smart fallback to httpx for static pages. Features: SPA framework detection (React/Vue/Angular), empty content indicators, domain learning for known JS sites, content comparison to validate JS rendering improvement.
[Tab 1] T10 committed and pushed (fe9951f). Starting T11 - Portfolio Change Alerts.
[Tab 2] Claiming T12 - Full-Text Search API. Writing plan for user approval.
[Tab 1] T11 COMPLETE: Portfolio Change Alerts implemented. Features: subscription management, change detection engine (new/removed holdings, value/shares changes), alert lifecycle (pending/acknowledged/expired), 8 API endpoints. Tables: alert_subscriptions, portfolio_alerts, portfolio_snapshots.
[Tab 1] Starting T13 - Dashboard Analytics API.
[Tab 2] T12 COMPLETE: Full-Text Search API implemented. Features: PostgreSQL FTS with GIN indexes, pg_trgm fuzzy matching for typos, faceted filtering (type/industry/location), autocomplete suggestions. Indexed 4034 records (27 investors, 4007 companies). Endpoints: GET /search, GET /search/suggest, POST /search/reindex, GET /search/stats. Performance: <200ms P95.
[Tab 2] Claiming T18 - Investor Similarity & Recommendations. Writing plan for user approval.
[Tab 2] T18 COMPLETE: Investor Similarity & Recommendations implemented. Features: Jaccard similarity scoring, similar investors endpoint, company recommendations ("investors like X also invest in Y"), portfolio overlap analysis. Endpoints: GET /discover/similar/{id}, GET /discover/recommended/{id}, GET /discover/overlap. Performance: <200ms.
[Tab 2] Claiming T20 - Saved Searches & Watchlists. Writing detailed plan for user approval.
[Tab 1] T13 COMPLETE: Dashboard Analytics API implemented. Features: system overview (investor coverage, portfolio totals, collection stats, alert stats), investor analytics (portfolio summary, industry distribution, top holdings, data quality score), trends (time-series for collections/companies/alerts), top movers (recent portfolio changes), industry breakdown (aggregate distribution). 5 endpoints: GET /analytics/overview, /analytics/investor/{id}, /analytics/trends, /analytics/top-movers, /analytics/industry-breakdown.
[Tab 2] T20 COMPLETE: Saved Searches & Watchlists implemented. Features: watchlist CRUD (create/update/delete), watchlist items (add/remove/list investors/companies), saved searches (create/update/delete/execute), duplicate prevention (409 on duplicates), user isolation, item count triggers. 12 endpoints covering POST/GET/PATCH/DELETE for /watchlists, /watchlists/{id}/items, /searches/saved. Integration with T12 search engine for saved search execution.
[Tab 2] Claiming T17 - Portfolio Comparison Tool. Writing detailed plan for user approval.
[Tab 2] T17 COMPLETE: Portfolio Comparison Tool implemented. Features: side-by-side portfolio comparison (overlap count, Jaccard similarity, unique holdings), historical diff (additions/removals over time), industry allocation comparison. 3 endpoints: POST /compare/portfolios, GET /compare/investor/{id}/history, GET /compare/industry. Performance: 166ms.
[Tab 2] Claiming T19 - Public API with Auth & Rate Limits. Writing detailed plan for user approval.
[Tab 1] Claiming T16 - GraphQL API Layer. Writing detailed plan for user approval.
[Tab 1] T16 COMPLETE: GraphQL API Layer implemented using Strawberry GraphQL. Features: LPFund/FamilyOffice/PortfolioCompany types with nested relationships, search query (integrates with T12), analytics overview (integrates with T13), industry breakdown, top movers. Queries: lpFund, lpFunds, familyOffice, familyOffices, portfolioCompany, portfolioCompanies, search, analyticsOverview, industryBreakdown, topMovers. Endpoint: POST /graphql with GraphQL Playground at GET /graphql.
[Tab 2] T19 COMPLETE: Public API with Auth & Rate Limits implemented. Features: API key generation (SHA-256 hashed storage, nxd_ prefix), rate limiting (per-minute and per-day with token bucket), usage tracking (per-key, per-endpoint, daily breakdown), scope-based access (read/write/admin), key revocation. 9 endpoints: POST/GET/PATCH/DELETE /api-keys, GET /api-keys/{id}/usage, GET /public/investors, GET /public/investors/{id}, GET /public/search. Rate limit headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset.
[SYSTEM] Phase 2 (T11-T20) complete (8/10, T14-T15 skipped). Phase 3 initialized with 10 new tasks.
[SYSTEM] Phase 3 focus: Investment Intelligence - network analysis, data enrichment, trends, reports, deal tracking.
[Tab 1] Claiming T21 - Co-investor Network Graph. Writing plan for user approval.
[Tab 1] T21 COMPLETE: Co-investor Network Graph implemented. Features: network graph from co_investments + shared portfolios, centrality metrics (degree, weighted degree), investor ego networks (depth 1-3), cluster detection (connected components), path finding between investors. 5 endpoints: GET /network/graph, GET /network/investor/{id}, GET /network/central, GET /network/clusters, GET /network/path. Output compatible with D3.js/Cytoscape visualization.
[Tab 1] Claiming T22 - Company Data Enrichment. Writing plan for user approval.
[Tab 1] T22 COMPLETE: Company Data Enrichment implemented. Features: SEC EDGAR integration (company tickers, financials from 10-K/10-Q), funding data placeholder, employee data placeholder, industry classification (keyword-based), company status tracking, job tracking. 5 endpoints: POST /enrichment/company/{name}, GET /enrichment/company/{name}/status, GET /enrichment/companies/{name}, GET /enrichment/companies, POST /enrichment/batch. Tables: company_enrichment, enrichment_jobs.
[Tab 1] Claiming T24 - News & Event Feed. Writing plan for user approval.
[Tab 1] T24 claimed by Tab 2, switching to T26 - Bulk Portfolio Import. Writing plan for user approval.
[Tab 1] T26 COMPLETE: Bulk Portfolio Import implemented. Features: CSV/Excel file upload, column validation, fuzzy investor matching, preview with errors/warnings, import execution, rollback support, import history. 6 endpoints: POST /import/upload, GET /import/{id}/preview, POST /import/{id}/confirm, GET /import/{id}/status, GET /import/history, POST /import/{id}/rollback. Tables: portfolio_imports.
[Tab 1] Claiming T27 - LP Profile Enrichment. Writing plan for user approval.
[Tab 1] T27 COMPLETE: LP Profile Enrichment implemented. Features: portfolio-based preference analysis (sectors, stages, regions), commitment pace calculation (investments/year, timing), contact extraction, AUM history tracking. 5 endpoints: POST /enrichment/investor/{id}, GET /enrichment/investor/{id}/status, GET /investors/{id}/contacts, GET /investors/{id}/aum-history, GET /investors/{id}/preferences. Tables: investor_contacts, investor_aum_history, investor_preferences.
[Tab 2] T28 COMPLETE: Deal Flow Tracker implemented. Features: deal CRUD (create/get/update/delete), pipeline stages (sourced/reviewing/due_diligence/negotiation/closed_won/closed_lost/passed), activity logging (meetings/notes/calls/emails/documents), pipeline summary with counts by stage/priority, filtering (stage/sector/assignee/priority), priority ordering. 9 endpoints: POST/GET/PATCH/DELETE /deals, POST/GET /deals/{id}/activities, GET /deals/pipeline, GET /deals/stages. Tables: deals, deal_activities.
[Tab 1] Claiming T29 - Market Benchmarks. Writing plan for user approval.
[Tab 1] T29 COMPLETE: Market Benchmarks implemented. Features: peer group construction (by type/size), sector allocation benchmarks (P25/median/P75), HHI-based diversification scoring, investor vs benchmark comparison. 4 endpoints: GET /benchmarks/investor/{id}, GET /benchmarks/peer-group/{id}, GET /benchmarks/sectors, GET /benchmarks/diversification.
[Tab 2] T30 COMPLETE: User Auth & Workspaces implemented. Features: JWT authentication (bcrypt hashing, 1-hour access tokens, 7-day refresh tokens), user registration/login/logout, password reset flow, workspace CRUD, member invitation with email tokens, role-based access (admin/member/viewer). 19 endpoints total: 9 auth + 10 workspace. Tables: users, workspaces, workspace_members, workspace_invitations, password_reset_tokens, refresh_tokens.
[SYSTEM] Phase 3 complete (T21-T30). Phase 4 initialized with 10 new tasks.
[SYSTEM] Phase 4 focus: Data Expansion & Predictive Intelligence - new data sources, ML scoring, entity resolution.
[Tab 1] Claiming T31 - SEC Form D Filings. Writing plan for user approval.
[Tab 1] T31 COMPLETE: SEC Form D Filings implemented. Features: Form D client for EDGAR API, XML parser for all Form D fields (issuer, offering, investors, exemptions), PostgreSQL storage with JSON fields for related persons/compensation, search with filters. 8 endpoints: GET /form-d/search, /form-d/issuer/{cik}, /form-d/recent, /form-d/filing/{accession}, /form-d/stats, /form-d/industries, /form-d/exemptions, POST /form-d/ingest. Table: form_d_filings.
[Tab 1] Claiming T32 - SEC Form ADV Data. Writing plan for user approval.
[Tab 2] T33 COMPLETE: OpenCorporates Integration implemented. Features: company search across 140+ jurisdictions, company details by jurisdiction/number, officers and filings for companies, officer search, jurisdictions list. 6 endpoints: GET /corporate-registry/search, /corporate-registry/company/{jurisdiction}/{number}, /corporate-registry/company/{jurisdiction}/{number}/officers, /corporate-registry/company/{jurisdiction}/{number}/filings, /corporate-registry/officers/search, /corporate-registry/jurisdictions. Requires OPENCORPORATES_API_KEY env var for data access.
[Tab 1] T32 COMPLETE: SEC Form ADV Data implemented. Features: investment adviser search with filters (name/state/AUM), adviser details by CRD number, AUM rankings, aggregate stats (by state/organization type), sample data ingestion. 6 endpoints: GET /form-adv/search, /form-adv/adviser/{crd_number}, /form-adv/aum-rankings, /form-adv/stats, /form-adv/by-state, POST /form-adv/ingest. Table: form_adv_advisers with 76 columns for full Form ADV Part 1 data.
[Tab 1] T34 COMPLETE: GitHub Repository Analytics implemented. Features: org overview with velocity scoring, repo list with metrics (stars/forks/languages), activity trends (commit frequency, trends), contributor tracking, velocity score (0-100) with breakdown. 9 endpoints: GET /github/org/{org}, POST /github/org/{org}/fetch, GET /github/org/{org}/repos, /github/org/{org}/activity, /github/org/{org}/contributors, /github/org/{org}/score, /github/repo/{owner}/{repo}, /github/search, /github/stats. Tables: github_organizations, github_repositories, github_activity_snapshots, github_contributors. Requires GITHUB_TOKEN for API access.
[Tab 2] T35 COMPLETE: Web Traffic Data implemented. Features: multi-provider support (Tranco free rankings + SimilarWeb paid), domain traffic overview with Tranco rank, domain comparison (side-by-side ranking), domain search by keyword, top 1M domain rankings from Tranco. 6 endpoints: GET /web-traffic/domain/{domain}, /web-traffic/domain/{domain}/history, /web-traffic/compare, /web-traffic/rankings, /web-traffic/search, /web-traffic/providers. Optional SIMILARWEB_API_KEY for detailed traffic data.
```

---

## Completed Tasks (All Phases)

### Phase 1: Agentic Infrastructure
| ID | Task | Completed By | Commit |
|----|------|--------------|--------|
| T01 | Retry Handler with Exponential Backoff | Tab 1 | Plan 004 |
| T02 | Fuzzy Matching for Deduplication | Tab 1 | Plan 004 |
| T03 | Response Caching Layer | Tab 1 | Plan 004 |
| T04 | Unit Tests for LLM Client | Tab 1 | 490e43c |
| T05 | Unit Tests for Ticker Resolver | Agent-T05 | - |
| T06 | Metrics/Monitoring for Agentic Jobs | Tab 1 | 3f629b5 |
| T07 | Scheduled Portfolio Updates | Tab 1 | 5ef2e5e |
| T08 | Portfolio Export to CSV/Excel | Tab 1 | 88918b3 |
| T09 | Annual Report PDF Caching | Tab 1 | Plan 004 |
| T10 | Website Strategy - JS Rendering | Agent-T10 | fe9951f |

### Phase 2: Bringing Data to People
| ID | Task | Completed By | Commit |
|----|------|--------------|--------|
| T11 | Portfolio Change Alerts | Tab 1 | - |
| T12 | Full-Text Search API | Tab 2 | - |
| T13 | Dashboard Analytics API | Tab 1 | - |
| T14 | Webhook Integrations | SKIPPED | - |
| T15 | Email Digest Reports | SKIPPED | - |
| T16 | GraphQL API Layer | Tab 1 | - |
| T17 | Portfolio Comparison Tool | Tab 2 | - |
| T18 | Investor Similarity & Recommendations | Tab 2 | - |
| T19 | Public API with Auth & Rate Limits | Tab 2 | - |
| T20 | Saved Searches & Watchlists | Tab 2 | - |

### Pre-Phase Work
| Task | Completed By | Commit |
|------|--------------|--------|
| Data Export Service | Tab 1 | 790ca0e |
| USPTO Patent Data Source | Tab 2 | 6e305f6 |
| LLM Client + Ticker Resolver | Tab 1 | bce50d5 |

---

## Instructions for New Agents

1. **Read this entire file**
2. **Find a task with status `NOT_STARTED`** in the Task Queue table
3. **Claim it** by changing status to `IN_PROGRESS [Your ID]` (e.g., `IN_PROGRESS [Agent-3]`)
4. **Check Dependencies** - if task depends on another, wait for it
5. **Read the Task Details section** for your task
6. **Create a plan file** if indicated (or skip if "None needed")
7. **Execute the task** - only touch files in your scope
8. **Test thoroughly** - `docker compose up -d --build`
9. **Mark as COMPLETE** when done
10. **Add entry to Communication Log**

---

## File Ownership Quick Reference

### Phase 1 Files
| File | Owner Task |
|------|------------|
| `app/agentic/retry_handler.py` | T01 |
| `app/agentic/strategies/base.py` | T01 |
| `app/agentic/fuzzy_matcher.py` | T02 |
| `app/agentic/synthesizer.py` | T02 |
| `app/agentic/cache.py` | T03 |
| `app/agentic/strategies/website_strategy.py` | T03, T10 |
| `tests/test_llm_client.py` | T04 |
| `tests/test_ticker_resolver.py` | T05 |
| `app/agentic/metrics.py` | T06 |
| `app/agentic/scheduler.py` | T07 |
| `app/agentic/exporter.py` | T08 |
| `app/agentic/strategies/annual_report_strategy.py` | T09 |

### Phase 2 Files
| File | Owner Task |
|------|------------|
| `app/notifications/alerts.py` | T11 |
| `app/api/v1/alerts.py` | T11 |
| `app/search/engine.py` | T12 |
| `app/api/v1/search.py` | T12 |
| `app/analytics/dashboard.py` | T13 |
| `app/api/v1/analytics.py` | T13 |
| `app/integrations/webhooks.py` | T14 |
| `app/api/v1/webhooks.py` | T14 |
| `app/notifications/digest.py` | T15 |
| `app/notifications/templates/` | T15 |
| `app/graphql/schema.py` | T16 |
| `app/graphql/resolvers.py` | T16 |
| `app/analytics/comparison.py` | T17 |
| `app/api/v1/compare.py` | T17 |
| `app/analytics/recommendations.py` | T18 |
| `app/api/v1/discover.py` | T18 |
| `app/auth/__init__.py` | T19 |
| `app/auth/api_keys.py` | T19 |
| `app/api/v1/api_keys.py` | T19 |
| `app/api/v1/public.py` | T19 |
| `app/users/watchlists.py` | T20 |
| `app/api/v1/watchlists.py` | T20 |

### Phase 3 Files
| File | Owner Task |
|------|------------|
| `app/network/graph.py` | T21 |
| `app/api/v1/network.py` | T21 |
| `app/enrichment/company.py` | T22 |
| `app/api/v1/enrichment.py` | T22, T27 |
| `app/analytics/trends.py` | T23 |
| `app/api/v1/trends.py` | T23 |
| `app/news/aggregator.py` | T24 |
| `app/api/v1/news.py` | T24 |
| `app/reports/builder.py` | T25 |
| `app/api/v1/reports.py` | T25 |
| `app/import/portfolio.py` | T26 |
| `app/api/v1/import.py` | T26 |
| `app/enrichment/investor.py` | T27 |
| `app/deals/tracker.py` | T28 |
| `app/api/v1/deals.py` | T28 |
| `app/analytics/benchmarks.py` | T29 |
| `app/api/v1/benchmarks.py` | T29 |
| `app/users/auth.py` | T30 |
| `app/users/workspaces.py` | T30 |
