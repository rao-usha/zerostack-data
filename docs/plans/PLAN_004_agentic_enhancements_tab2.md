# PLAN 004: Agentic Enhancements - Tab 2

## Overview
**Tab:** 2
**Feature:** Error Handling + Fuzzy Matching + Caching
**Status:** PENDING_APPROVAL

---

## Scope

### 1. Improved Error Handling & Retries
Add robust retry logic with exponential backoff for all strategies.

**Files to modify:**
- `app/agentic/strategies/base.py` - Add retry decorator/mixin
- `app/agentic/retry_handler.py` (create) - Reusable retry logic

**Implementation:**
1. Create retry_handler.py:
   - Async retry decorator with exponential backoff
   - Configurable max retries (default 3)
   - Configurable base delay (default 1s)
   - Jitter to prevent thundering herd
   - Circuit breaker pattern for persistent failures
   - Specific handling for HTTP 429 (rate limit)

2. Update base.py:
   - Import retry handler
   - Add `@with_retry` decorator to `execute()` method
   - Add retry configuration to BaseStrategy

### 2. Fuzzy Matching for Deduplication
Improve company name matching using Levenshtein distance.

**Files to modify:**
- `app/agentic/synthesizer.py` - Add fuzzy matching to deduplication
- `app/agentic/fuzzy_matcher.py` (create) - Fuzzy string matching utilities

**Implementation:**
1. Create fuzzy_matcher.py:
   - Levenshtein distance calculation
   - Configurable similarity threshold (default 0.85)
   - Company name normalization helpers
   - Batch matching for efficiency

2. Update synthesizer.py:
   - Import fuzzy matcher
   - Use fuzzy matching in `_deduplicate_companies()`
   - Merge records when similarity > threshold
   - Track which records were fuzzy-matched

### 3. Response Caching Layer
Add Redis-compatible caching for expensive operations.

**Files to modify:**
- `app/agentic/cache.py` (create) - Caching utilities
- `app/agentic/strategies/website_strategy.py` - Add caching
- `app/agentic/strategies/annual_report_strategy.py` - Cache PDF parsing

**Implementation:**
1. Create cache.py:
   - In-memory cache with TTL (fallback when no Redis)
   - Optional Redis backend
   - Key generation helpers
   - Cache decorator for async functions

2. Update website_strategy.py:
   - Cache scraped portfolio pages (1 hour TTL)
   - Cache robots.txt responses (24 hour TTL)

3. Update annual_report_strategy.py:
   - Cache parsed PDF results (24 hour TTL)
   - Key by URL hash

---

## Files Owned by Tab 2

**Create:**
- `app/agentic/retry_handler.py`
- `app/agentic/fuzzy_matcher.py`
- `app/agentic/cache.py`

**Modify:**
- `app/agentic/strategies/base.py`
- `app/agentic/synthesizer.py`
- `app/agentic/strategies/website_strategy.py`
- `app/agentic/strategies/annual_report_strategy.py`

---

## Testing Plan

1. Test retry handler with mock failing requests
2. Test fuzzy matcher with similar company names:
   - "Apple Inc" vs "Apple, Inc."
   - "Microsoft Corporation" vs "Microsoft Corp"
3. Test cache with TTL expiration
4. Run full collection and verify deduplication improved

---

## Dependencies

- `python-Levenshtein` or `rapidfuzz` (add to requirements.txt)
- `redis` package (optional, for Redis caching)

---

## Estimated Effort
- Retry Handler: 1 hour
- Fuzzy Matching: 1-2 hours
- Caching: 1 hour
- Testing: 30 minutes
