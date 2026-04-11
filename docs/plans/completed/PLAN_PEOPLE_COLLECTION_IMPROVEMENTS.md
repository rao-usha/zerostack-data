# People Collection Improvement Plan

> **Status:** Planning
> **Created:** 2026-02-02
> **Goal:** Improve people_found counts in people collection jobs

---

## Problem Statement

People collection jobs are completing with `people_found: 0` despite having complete agent implementations. This plan analyzes root causes and proposes fixes.

---

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PeopleCollectionOrchestrator                │
│                    (app/sources/people_collection/orchestrator.py)
└─────────────────────────────────────────────────────────────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  ▼                 ▼                 ▼
         ┌────────────┐     ┌────────────┐     ┌────────────┐
         │WebsiteAgent│     │  SECAgent  │     │ NewsAgent  │
         └────────────┘     └────────────┘     └────────────┘
                │                  │                  │
         ┌──────┴──────┐    ┌──────┴──────┐   ┌──────┴──────┐
         │ PageFinder  │    │FilingFetcher│   │ NewsroomURL │
         │ HTMLCleaner │    │  SECParser  │   │ PR Parser   │
         │LLMExtractor │    │LLMExtractor │   │LLMExtractor │
         └─────────────┘    └─────────────┘   └─────────────┘
```

---

## Root Cause Analysis

### 1. WebsiteAgent (Primary Concern)

**File:** `app/sources/people_collection/website_agent.py`

**Issues Identified:**

| Issue | Impact | Location |
|-------|--------|----------|
| PageFinder URL patterns may not match modern websites | High | `config.py:268-298` |
| JavaScript-rendered pages not handled | High | `html_cleaner.py` |
| LLM extraction failures silently return empty | Medium | `llm_extractor.py:220-221` |
| No logging when zero people extracted | Medium | `website_agent.py` |
| robots.txt not being checked/cached | Low | `page_finder.py` |

**Evidence from Code:**
```python
# page_finder.py - Only 28 URL patterns, missing common variants
LEADERSHIP_URL_PATTERNS: List[str] = [
    "/about/leadership",
    "/about/team",
    # ... but missing /people/, /staff/, /company/about-us/team, etc.
]
```

### 2. SECAgent (Secondary Concern)

**File:** `app/sources/people_collection/sec_agent.py`

**Issues Identified:**

| Issue | Impact | Location |
|-------|--------|----------|
| Companies need CIK mapping to fetch filings | High | `sec_agent.py:62` |
| Private companies have no SEC filings | High | N/A - data limitation |
| Proxy statement parsing relies on text patterns | Medium | `sec_parser.py:278-287` |
| 8-K Item 5.02 parsing may miss formats | Medium | `sec_parser.py:487-543` |

### 3. NewsAgent (Secondary Concern)

**File:** `app/sources/people_collection/news_agent.py`

**Issues Identified:**

| Issue | Impact | Location |
|-------|--------|----------|
| Newsroom discovery limited to common paths | High | `news_agent.py:~80-120` |
| Google News search is commented out | High | `news_agent.py:~200` |
| Rate limiting may be too aggressive | Low | `config.py:41-46` |

### 4. Orchestrator Issues

**File:** `app/sources/people_collection/orchestrator.py`

**Issues Identified:**

| Issue | Impact | Location |
|-------|--------|----------|
| Exceptions in agents may not bubble up properly | High | `orchestrator.py` |
| No diagnostic logging for zero-result jobs | Medium | `orchestrator.py` |
| collect_batch runs agents sequentially, not parallel | Low | `orchestrator.py` |

### 5. Data Quality Issues

| Issue | Impact | Fix |
|-------|--------|-----|
| Companies missing website URLs | High | Add website discovery |
| Companies missing SEC CIK | High | Add CIK lookup via SEC EDGAR |
| Empty strings in nullable fields | Fixed | Already fixed in orchestrator |

---

## Proposed Improvements

### Phase 1: Diagnostics & Logging (Quick Wins)

**Goal:** Understand why zero people are being found

**Tasks:**

1. **Add diagnostic logging to orchestrator**
   - Log when each agent starts/completes
   - Log number of people found per agent
   - Log any exceptions with full stack traces

2. **Add extraction metrics**
   - Track success/failure rates per agent
   - Track most common failure reasons

3. **Add test endpoint**
   - `/api/v1/people-jobs/test/{company_id}` - Run collection with verbose logging

### Phase 2: WebsiteAgent Improvements

**Goal:** Find more leadership pages and extract people reliably

**Tasks:**

1. **Expand URL patterns** (config.py)
   - Add 20+ additional patterns
   - Add international variants (/en/about, /us/team)
   - Add SaaS-specific patterns (/company, /company/about)

2. **Add Google search fallback** (page_finder.py)
   - `site:example.com leadership team` query
   - Helps for non-standard URL structures

3. **Add JavaScript rendering support** (new: js_renderer.py)
   - Use playwright/pyppeteer for JS-heavy sites
   - Fall back to simple fetch if rendering fails

4. **Improve LLM extraction reliability** (llm_extractor.py)
   - Add retry with different prompt on empty results
   - Add structured extraction as first pass before LLM
   - Log full prompts/responses when extraction fails

5. **Add structured HTML extraction** (html_cleaner.py)
   - Look for schema.org Person markup
   - Parse common HTML patterns (cards, grid layouts)
   - Extract from visible contact/team sections

### Phase 3: SECAgent Improvements

**Goal:** Extract leadership from SEC filings more reliably

**Tasks:**

1. **Add CIK auto-discovery** (filing_fetcher.py)
   - Search SEC EDGAR by company name
   - Cache CIK mappings in database

2. **Improve proxy parsing** (sec_parser.py)
   - Add more name+title patterns
   - Handle table-based compensation sections better
   - Use LLM extraction more aggressively

3. **Add Form 4 parsing** (new: form4_parser.py)
   - Form 4s list officers and directors
   - Filed more frequently than proxies

### Phase 4: NewsAgent Improvements

**Goal:** Find leadership changes from news/press releases

**Tasks:**

1. **Enable and improve Google News search** (news_agent.py)
   - Un-comment Google News code
   - Add SerpAPI or ScraperAPI for reliability
   - Search: `"company name" (CEO OR CFO OR appointed OR hired)`

2. **Improve newsroom discovery** (news_agent.py)
   - Add more newsroom URL patterns
   - Search for RSS/Atom feeds
   - Check investor relations pages

3. **Add PR Newswire/Business Wire integration**
   - Official APIs available
   - Higher quality than scraped news

### Phase 5: Data Quality & Pipeline

**Goal:** Ensure companies have required data for collection

**Tasks:**

1. **Add website discovery job**
   - Use Clearbit, Apollo, or Google search
   - Enrich companies missing website URLs

2. **Add CIK mapping job**
   - Bulk lookup via SEC EDGAR
   - Store in companies table

3. **Add retry mechanism for failed jobs**
   - Track failure reasons
   - Retry with different strategy

---

## Implementation Priority

| Phase | Effort | Impact | Priority |
|-------|--------|--------|----------|
| Phase 1: Diagnostics | Low (1-2 days) | Medium | **1st** |
| Phase 2: WebsiteAgent | Medium (3-5 days) | High | **2nd** |
| Phase 4: NewsAgent | Medium (2-3 days) | Medium | **3rd** |
| Phase 3: SECAgent | Medium (2-3 days) | Medium | **4th** |
| Phase 5: Data Quality | Low (1-2 days) | High | **5th** |

---

## Specific Code Changes

### Phase 1 Changes

**orchestrator.py - Add diagnostic logging:**
```python
async def collect_for_company(self, company: IndustrialCompany) -> CollectionResult:
    logger.info(f"Starting collection for {company.name} (id={company.id})")

    results = {"website": [], "sec": [], "news": []}

    # Website collection
    if company.website:
        try:
            logger.info(f"Running WebsiteAgent for {company.name}")
            website_result = await self.website_agent.collect(company)
            results["website"] = website_result.people
            logger.info(f"WebsiteAgent found {len(website_result.people)} people")
        except Exception as e:
            logger.exception(f"WebsiteAgent failed for {company.name}: {e}")
    else:
        logger.warning(f"Company {company.name} has no website URL")

    # ... similar for SEC and News agents
```

**config.py - Expanded URL patterns:**
```python
LEADERSHIP_URL_PATTERNS: List[str] = [
    # Existing patterns...

    # Additional patterns
    "/people",
    "/staff",
    "/about/people",
    "/about/staff",
    "/company/about",
    "/company/team",
    "/company/leadership",
    "/company/about-us/team",
    "/who-we-are/our-team",
    "/who-we-are/leadership",
    "/our-people",
    "/our-leadership",
    "/en/about/team",
    "/en/about/leadership",
    "/us/about/team",
    "/us/about/leadership",
    "/about-us/our-team",
    "/about-us/leadership-team",
    "/about-us/management-team",
    "/corporate/about/leadership",
    "/corporate/about/team",
    "/investors/corporate-governance/management",
    "/investor-relations/leadership",
]
```

---

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| People found per job | 0 | >5 |
| Jobs with >0 people | 0% | >70% |
| C-suite extraction rate | Unknown | >80% |
| Data freshness | N/A | <30 days |

---

## Testing Plan

1. **Unit tests** for each improvement
2. **Integration tests** with sample companies
3. **Test companies** with known leadership:
   - Apple Inc (public, well-structured website)
   - Microsoft (public, complex website)
   - Stripe (private, good team page)
   - OpenAI (private, team page)

---

## Approval

- [ ] **Approved** - Ready to implement
- [ ] **Changes requested** - See comments

---

## Notes

- Phase 1 should be done first to understand actual failure modes
- May discover different root cause than expected
- LLM costs will increase with more extraction attempts
