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

## Phase 2: Bringing Data to People (T11-T20)

**Mission:** Make collected data accessible, searchable, and actionable for end users.

### Task Queue

| ID | Task | Status | Agent | Files (Scope) | Dependencies |
|----|------|--------|-------|---------------|--------------|
| T11 | Portfolio Change Alerts | COMPLETE | Tab 1 | `app/notifications/alerts.py`, `app/api/v1/alerts.py` | None |
| T12 | Full-Text Search API | COMPLETE | Tab 2 | `app/search/engine.py`, `app/api/v1/search.py` | None |
| T13 | Dashboard Analytics API | COMPLETE | Tab 1 | `app/analytics/dashboard.py`, `app/api/v1/analytics.py` | None |
| T14 | Webhook Integrations | SKIPPED | - | `app/integrations/webhooks.py`, `app/api/v1/webhooks.py` | None |
| T15 | Email Digest Reports | SKIPPED | - | `app/notifications/digest.py`, `app/notifications/templates/` | T11 |
| T16 | GraphQL API Layer | NOT_STARTED | - | `app/graphql/schema.py`, `app/graphql/resolvers.py` | None |
| T17 | Portfolio Comparison Tool | IN_PROGRESS | Tab 2 | `app/analytics/comparison.py`, `app/api/v1/compare.py` | None |
| T18 | Investor Similarity & Recommendations | COMPLETE | Tab 2 | `app/analytics/recommendations.py`, `app/api/v1/discover.py` | T12 |
| T19 | Public API with Auth & Rate Limits | NOT_STARTED | - | `app/api/public/`, `app/auth/api_keys.py` | None |
| T20 | Saved Searches & Watchlists | COMPLETE | Tab 2 | `app/users/watchlists.py`, `app/api/v1/watchlists.py` | T12 |

---

## Task Details

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
| `app/api/public/` | T19 |
| `app/auth/api_keys.py` | T19 |
| `app/users/watchlists.py` | T20 |
| `app/api/v1/watchlists.py` | T20 |
