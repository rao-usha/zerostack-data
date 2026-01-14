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

## Task Queue

| ID | Task | Status | Agent | Files (Scope) | Dependencies |
|----|------|--------|-------|---------------|--------------|
| T01 | Retry Handler with Exponential Backoff | COMPLETE | Tab 1 | `app/agentic/retry_handler.py`, `app/agentic/strategies/base.py` | None |
| T02 | Fuzzy Matching for Deduplication | COMPLETE | Tab 1 | `app/agentic/fuzzy_matcher.py`, `app/agentic/synthesizer.py` | None |
| T03 | Response Caching Layer | COMPLETE | Tab 1 | `app/agentic/cache.py`, `app/agentic/strategies/website_strategy.py` | None |
| T04 | Unit Tests for LLM Client | COMPLETE | Tab 1 | `tests/test_llm_client.py` | None |
| T05 | Unit Tests for Ticker Resolver | COMPLETE | Agent-T05 | `tests/test_ticker_resolver.py` | None |
| T06 | Metrics/Monitoring for Agentic Jobs | NOT_STARTED | - | `app/agentic/metrics.py`, `app/api/v1/agentic_research.py` (metrics endpoint only) | None |
| T07 | Scheduled Portfolio Updates | NOT_STARTED | - | `app/agentic/scheduler.py` | None |
| T08 | Portfolio Export to CSV/Excel | NOT_STARTED | - | `app/agentic/exporter.py`, `app/api/v1/agentic_research.py` (export endpoint only) | None |
| T09 | Annual Report Strategy - PDF Caching | COMPLETE | Tab 1 | `app/agentic/strategies/annual_report_strategy.py` | T03 |
| T10 | Website Strategy - JS Rendering Support | NOT_STARTED | - | `app/agentic/strategies/website_strategy.py` | None |

---

## Task Details

### T01: Retry Handler with Exponential Backoff
**Goal:** Add robust retry logic for all HTTP requests in strategies.

**Scope:**
- Create `app/agentic/retry_handler.py` with:
  - Async retry decorator
  - Exponential backoff with jitter
  - Max retries config (default 3)
  - Circuit breaker for persistent failures
  - Special handling for HTTP 429
- Update `app/agentic/strategies/base.py` to use retry handler

**Plan:** `docs/plans/PLAN_T01_retry_handler.md`

---

### T02: Fuzzy Matching for Deduplication
**Goal:** Improve company name matching using string similarity.

**Scope:**
- Create `app/agentic/fuzzy_matcher.py` with:
  - Levenshtein distance calculation
  - Configurable similarity threshold (default 0.85)
  - Company name normalization
  - Batch matching support
- Update `app/agentic/synthesizer.py` to use fuzzy matching in deduplication

**Plan:** `docs/plans/PLAN_T02_fuzzy_matcher.md`

---

### T03: Response Caching Layer
**Goal:** Cache expensive HTTP responses to reduce API calls.

**Scope:**
- Create `app/agentic/cache.py` with:
  - In-memory TTL cache (fallback)
  - Optional Redis backend
  - Cache decorator for async functions
  - Key generation helpers
- Update `app/agentic/strategies/website_strategy.py` to cache pages

**Plan:** `docs/plans/PLAN_T03_cache.md`

---

### T04: Unit Tests for LLM Client
**Goal:** Test LLM client with mocked responses.

**Scope:**
- Create `tests/test_llm_client.py` with:
  - Test OpenAI completion (mocked)
  - Test Anthropic completion (mocked)
  - Test retry logic
  - Test JSON parsing
  - Test cost calculation

**Plan:** None needed (straightforward)

---

### T05: Unit Tests for Ticker Resolver
**Goal:** Test ticker resolution with mocked yfinance.

**Scope:**
- Create `tests/test_ticker_resolver.py` with:
  - Test single ticker resolution
  - Test batch resolution
  - Test CUSIP fallback
  - Test cache behavior
  - Test error handling

**Plan:** None needed (straightforward)

---

### T06: Metrics/Monitoring for Agentic Jobs
**Goal:** Track success rates, timing, and costs for agentic collection.

**Scope:**
- Create `app/agentic/metrics.py` with:
  - Job success/failure counters
  - Strategy execution times
  - Token usage tracking
  - Cost per investor stats
- Add `GET /api/v1/agentic/metrics` endpoint

**Plan:** `docs/plans/PLAN_T06_metrics.md`

---

### T07: Scheduled Portfolio Updates
**Goal:** Automatically refresh portfolio data on a schedule.

**Scope:**
- Create `app/agentic/scheduler.py` with:
  - Integration with existing APScheduler
  - Quarterly refresh for all investors
  - Priority queue for stale data
  - Incremental updates (only new data)

**Plan:** `docs/plans/PLAN_T07_scheduler.md`

---

### T08: Portfolio Export to CSV/Excel
**Goal:** Export portfolio data for analysis.

**Scope:**
- Create `app/agentic/exporter.py` with:
  - Export to CSV
  - Export to Excel (with formatting)
  - Include all portfolio fields
  - Optional filters
- Add `GET /api/v1/agentic/portfolio/{id}/export` endpoint

**Plan:** `docs/plans/PLAN_T08_exporter.md`

---

### T09: Annual Report Strategy - PDF Caching
**Goal:** Cache parsed PDF results to avoid re-parsing.

**Scope:**
- Update `app/agentic/strategies/annual_report_strategy.py` to:
  - Use cache layer from T03
  - Cache by URL hash
  - 24-hour TTL

**Dependencies:** T03 must be complete first.

**Plan:** None needed (small change)

---

### T10: Website Strategy - JS Rendering Support
**Goal:** Handle JavaScript-rendered pages with Playwright.

**Scope:**
- Update `app/agentic/strategies/website_strategy.py` to:
  - Detect JS-heavy pages
  - Use Playwright for rendering
  - Fallback to httpx for static pages

**Plan:** `docs/plans/PLAN_T10_js_rendering.md`

---

## Communication Log

```
[SYSTEM] Parallel work queue initialized with 10 tasks
[SYSTEM] Previous work: Export (790ca0e), USPTO (6e305f6), LLM+Ticker (bce50d5)
[Tab 1] Completed T01, T02, T03, T09 as part of Plan 004 (Agentic Enhancements)
[Agent-T01] Verified T01 implementation: retry_handler.py + base.py integration. Docker build OK.
[Agent-T05] T05 COMPLETE: Created tests/test_ticker_resolver.py with 48 tests (all passing). Covers: normalize_ticker, resolve_ticker_sync, resolve_ticker async, batch resolution, CUSIP fallback via SEC EDGAR, TickerResolver.resolve_holdings, cache behavior, and error handling. All mocked (yfinance, httpx).
[Tab 1] T04 COMPLETE: Created tests/test_llm_client.py with 36 tests (34 passed, 2 skipped). Covers: LLMResponse, OpenAI/Anthropic completion, retry logic, JSON parsing, cost calculation, token tracking.
```

---

## Completed Tasks

| ID | Task | Completed By | Commit |
|----|------|--------------|--------|
| - | Data Export Service | Tab 1 | 790ca0e |
| - | USPTO Patent Data Source | Tab 2 | 6e305f6 |
| - | LLM Client + Ticker Resolver | Tab 1 | bce50d5 |
| T01 | Retry Handler with Exponential Backoff | Tab 1 | (Plan 004) |
| T02 | Fuzzy Matching for Deduplication | Tab 1 | (Plan 004) |
| T03 | Response Caching Layer | Tab 1 | (Plan 004) |
| T09 | Annual Report PDF Caching | Tab 1 | (Plan 004) |
| T05 | Unit Tests for Ticker Resolver | Agent-T05 | (not committed) |
| T04 | Unit Tests for LLM Client | Tab 1 | (pending) |

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
