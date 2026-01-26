# PLAN 003: Agentic Enhancements - Tab 1

## Overview
**Tab:** 1
**Feature:** LLM Integration + Ticker Resolution
**Status:** PENDING_APPROVAL

---

## Scope

### 1. Connect LLM for News Strategy
Wire up OpenAI/Anthropic API to extract structured portfolio data from news articles.

**Files to modify:**
- `app/core/config.py` - Add LLM configuration settings
- `app/agentic/strategies/news_strategy.py` - Connect LLM API calls
- `app/agentic/llm_client.py` (create) - Reusable LLM client wrapper

**Implementation:**
1. Add settings to config.py:
   - `OPENAI_API_KEY`
   - `ANTHROPIC_API_KEY`
   - `LLM_PROVIDER` (openai/anthropic)
   - `LLM_MODEL` (gpt-4o-mini, claude-3-haiku, etc.)
   - `LLM_MAX_TOKENS`
   - `LLM_TEMPERATURE`

2. Create llm_client.py:
   - Async client wrapper supporting both OpenAI and Anthropic
   - Structured output parsing (JSON mode)
   - Token counting and cost tracking
   - Rate limiting and retry logic

3. Update news_strategy.py:
   - Import and use LLM client
   - Create extraction prompt for portfolio companies
   - Parse LLM response into StrategyResult
   - Track tokens used

### 2. SEC 13F Ticker Resolution
Resolve stock tickers to full company names using yfinance.

**Files to modify:**
- `app/agentic/strategies/sec_13f_strategy.py` - Add ticker resolution
- `app/agentic/ticker_resolver.py` (create) - Ticker lookup with caching

**Implementation:**
1. Create ticker_resolver.py:
   - yfinance integration for ticker→company lookup
   - In-memory LRU cache (1000 tickers)
   - Fallback to SEC EDGAR company search
   - Batch resolution for efficiency

2. Update sec_13f_strategy.py:
   - Import ticker resolver
   - Resolve all holdings to full company names
   - Store both ticker and resolved name

---

## Files Owned by Tab 1

**Create:**
- `app/agentic/llm_client.py`
- `app/agentic/ticker_resolver.py`

**Modify:**
- `app/core/config.py`
- `app/agentic/strategies/news_strategy.py`
- `app/agentic/strategies/sec_13f_strategy.py`

---

## Testing Plan

1. Test LLM client with sample news article
2. Test ticker resolution with known tickers (AAPL, MSFT, GOOGL)
3. Run news strategy on test investor
4. Run SEC 13F strategy and verify company names resolved

---

## Dependencies

- `openai` package (likely already installed)
- `anthropic` package (likely already installed)
- `yfinance` package (may need to add to requirements.txt)

---

## Estimated Effort
- LLM Integration: 1-2 hours
- Ticker Resolution: 1 hour
- Testing: 30 minutes
