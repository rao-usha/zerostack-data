# Parallel Development Coordination

> **Both tabs: Read this file before making changes. Update your status when done.**

---

## Workflow Reminder

```
1. PLAN      → Write plan to this file, wait for user approval
2. EXECUTE   → Only after user says "approved"
3. TEST      → Docker rebuild, curl endpoints
4. FIX       → If needed
5. INTEGRATE → Tab 1 updates main.py (if needed)
6. COMMIT    → Tab 1 commits all
7. PUSH      → Tab 1 pushes
```

---

## Current Sprint: Agentic Portfolio Enhancements

### TAB 1 - LLM Integration + Ticker Resolution
**Status:** COMPLETE
**Plan:** [PLAN_003_agentic_enhancements_tab1.md](docs/plans/PLAN_003_agentic_enhancements_tab1.md)

**Owner files (ONLY touch these):**
- `app/agentic/llm_client.py` (create)
- `app/agentic/ticker_resolver.py` (create)
- `app/agentic/strategies/news_strategy.py` (modify)
- `app/agentic/strategies/sec_13f_strategy.py` (modify)
- `app/core/config.py` (modify - LLM settings only)

**Scope:**
1. Create LLM client wrapper (OpenAI + Anthropic support)
2. Wire LLM to news_strategy.py for article extraction
3. Create ticker resolver using yfinance
4. Update SEC 13F strategy to resolve tickers to company names

---

### TAB 2 - Error Handling + Fuzzy Matching + Caching
**Status:** PENDING_APPROVAL
**Plan:** [PLAN_004_agentic_enhancements_tab2.md](docs/plans/PLAN_004_agentic_enhancements_tab2.md)

**Owner files (ONLY touch these):**
- `app/agentic/retry_handler.py` (create)
- `app/agentic/fuzzy_matcher.py` (create)
- `app/agentic/cache.py` (create)
- `app/agentic/strategies/base.py` (modify)
- `app/agentic/synthesizer.py` (modify)
- `app/agentic/strategies/website_strategy.py` (modify)
- `app/agentic/strategies/annual_report_strategy.py` (modify)

**Scope:**
1. Create retry handler with exponential backoff
2. Add fuzzy matching for better deduplication
3. Create caching layer for expensive operations

---

## PLANS (Stored in docs/plans/)

| Plan | File | Status | Approved |
|------|------|--------|----------|
| Tab 1: Export & Integration | [PLAN_001](docs/plans/PLAN_001_export_integration.md) | COMPLETE | [x] |
| Tab 2: USPTO Patents | [PLAN_002](docs/plans/PLAN_002_uspto_patents.md) | SKIPPED | [ ] |
| Tab 1: LLM + Ticker | [PLAN_003](docs/plans/PLAN_003_agentic_enhancements_tab1.md) | APPROVED | [x] |
| Tab 2: Retry + Fuzzy + Cache | [PLAN_004](docs/plans/PLAN_004_agentic_enhancements_tab2.md) | APPROVED | [x] |

**Instructions:**
1. Read your assigned plan file
2. Wait for user to check the box [x] in this table
3. Only then start coding
4. Update status after each phase

---

## Status Updates

| Tab | Phase | Status | Last Updated | Notes |
|-----|-------|--------|--------------|-------|
| Tab 1 | TEST | COMPLETE | 2026-01-14 | LLM client + Ticker resolver done |
| Tab 2 | EXECUTE | IN_PROGRESS | 2026-01-14 | Retry + Fuzzy + Cache |

---

## Integration Checklist (After both approved & done)

- [x] Tab 1 code complete (LLM + Ticker)
- [ ] Tab 2 code complete (Retry + Fuzzy + Cache)
- [ ] Docker rebuild successful
- [ ] All strategies tested
- [ ] Tab 1 commits all changes
- [ ] Tab 1 pushes
- [ ] CI passes

---

## Communication Log

```
[TAB 1] Export feature COMPLETE and pushed (790ca0e)
[TAB 1] New sprint: Agentic enhancements
[TAB 1] PLAN_003 written - LLM Integration + Ticker Resolution
[TAB 2] PLAN_004 written - Error Handling + Fuzzy Matching + Caching
[TAB 1] COMPLETE - llm_client.py, ticker_resolver.py created
[TAB 1] COMPLETE - news_strategy.py and sec_13f_strategy.py updated
[TAB 1] Waiting for Tab 2 to complete
```

---

## File Ownership Summary (NO OVERLAP)

| File | Tab 1 | Tab 2 |
|------|-------|-------|
| `app/agentic/llm_client.py` | ✅ | ❌ |
| `app/agentic/ticker_resolver.py` | ✅ | ❌ |
| `app/agentic/strategies/news_strategy.py` | ✅ | ❌ |
| `app/agentic/strategies/sec_13f_strategy.py` | ✅ | ❌ |
| `app/core/config.py` | ✅ (LLM only) | ❌ |
| `app/agentic/retry_handler.py` | ❌ | ✅ |
| `app/agentic/fuzzy_matcher.py` | ❌ | ✅ |
| `app/agentic/cache.py` | ❌ | ✅ |
| `app/agentic/strategies/base.py` | ❌ | ✅ |
| `app/agentic/synthesizer.py` | ❌ | ✅ |
| `app/agentic/strategies/website_strategy.py` | ❌ | ✅ |
| `app/agentic/strategies/annual_report_strategy.py` | ❌ | ✅ |

---

## Rules

1. **Read your PLAN file before starting**
2. **Wait for user to say "approved" before coding**
3. **Only touch files in YOUR section**
4. **Update status after each phase**
5. **Tab 1 handles final commit and push**
