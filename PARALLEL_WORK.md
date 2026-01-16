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
| T28 | Deal Flow Tracker | IN_PROGRESS | Tab 2 | `app/deals/tracker.py`, `app/api/v1/deals.py` | None |
| T29 | Market Benchmarks | NOT_STARTED | - | `app/analytics/benchmarks.py`, `app/api/v1/benchmarks.py` | T23 |
| T30 | User Auth & Workspaces | NOT_STARTED | - | `app/users/auth.py`, `app/users/workspaces.py` | T19 |

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
