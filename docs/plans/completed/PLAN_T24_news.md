# Plan T24: News & Event Feed

**Task ID:** T24
**Status:** COMPLETE
**Agent:** Tab 2 (took over from Tab 1)
**Date:** 2026-01-16

---

## Goal

Aggregate news and events relevant to tracked investors and portfolio companies. Provide a feed of SEC filings, funding announcements, M&A activity, and company news.

---

## Why This Matters

1. **Stay Informed**: Track breaking news about portfolio companies and investors
2. **Due Diligence**: Monitor SEC filings (13F, 13D, 8-K) for regulatory events
3. **Deal Flow**: Detect funding announcements and M&A activity early
4. **Research**: Consolidate news from multiple sources in one place

---

## Data Sources

### Primary Sources (Free, No API Key)
1. **SEC EDGAR RSS** - Real-time SEC filing notifications
2. **Google News RSS** - Company and investor news
3. **PR Newswire RSS** - Press releases and funding announcements

### Secondary Sources (Future Integration)
- NewsAPI (requires key)
- Alpha Vantage News (requires key)
- Crunchbase News (requires key)

---

## Design

### Database Schema

```sql
-- News and event items
CREATE TABLE IF NOT EXISTS news_items (
    id SERIAL PRIMARY KEY,

    -- Item identification
    source VARCHAR(50) NOT NULL,  -- sec_edgar, google_news, pr_newswire
    source_id VARCHAR(255),       -- unique ID from source

    -- Content
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT,
    published_at TIMESTAMP,

    -- Classification
    event_type VARCHAR(50),       -- filing, funding, acquisition, ipo, news
    filing_type VARCHAR(20),      -- 13F, 13D, 8-K, 10-K, 10-Q (for SEC)

    -- Entity references (nullable - may be general market news)
    company_name VARCHAR(255),
    company_ticker VARCHAR(20),
    investor_id INTEGER,
    investor_type VARCHAR(50),

    -- Relevance
    relevance_score FLOAT DEFAULT 0.5,

    -- Metadata
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source, source_id)
);

-- Index for efficient queries
CREATE INDEX idx_news_published ON news_items(published_at DESC);
CREATE INDEX idx_news_company ON news_items(company_name);
CREATE INDEX idx_news_investor ON news_items(investor_id, investor_type);
CREATE INDEX idx_news_type ON news_items(event_type);
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/news/feed` | GET | Aggregated news feed with filters |
| `/news/investor/{id}` | GET | News for specific investor |
| `/news/company/{name}` | GET | News for specific company |
| `/news/filings` | GET | SEC filing feed |
| `/news/refresh` | POST | Trigger news refresh from sources |

### News Aggregator Engine

```python
class NewsAggregator:
    """News and event aggregation engine."""

    async def fetch_sec_filings(self, company_tickers: List[str] = None) -> List[Dict]:
        """Fetch recent SEC filings from EDGAR RSS."""

    async def fetch_google_news(self, query: str) -> List[Dict]:
        """Fetch news from Google News RSS."""

    async def fetch_pr_newswire(self, keywords: List[str] = None) -> List[Dict]:
        """Fetch press releases from PR Newswire RSS."""

    def classify_event(self, item: Dict) -> str:
        """Classify news item type (funding, acquisition, etc)."""

    def calculate_relevance(self, item: Dict) -> float:
        """Score relevance based on portfolio companies/investors."""

    async def refresh_all(self) -> Dict:
        """Refresh news from all sources."""
```

---

## Implementation

### 1. `app/news/__init__.py`
Package initialization.

### 2. `app/news/aggregator.py`
Main news aggregation engine with RSS parsing and classification.

### 3. `app/news/sources/sec_rss.py`
SEC EDGAR RSS feed parser.

### 4. `app/news/sources/google_news.py`
Google News RSS parser.

### 5. `app/api/v1/news.py`
FastAPI router with 5 endpoints.

---

## Response Format

### News Feed Item
```json
{
  "id": 123,
  "source": "sec_edgar",
  "title": "Form 13F-HR filed by CalPERS",
  "summary": "California Public Employees' Retirement System filed quarterly holdings report",
  "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=...",
  "published_at": "2026-01-16T14:30:00Z",
  "event_type": "filing",
  "filing_type": "13F",
  "company_name": null,
  "investor_id": 1,
  "investor_type": "lp",
  "relevance_score": 0.95
}
```

### News Feed Response
```json
{
  "items": [...],
  "total": 150,
  "page": 1,
  "page_size": 20,
  "filters": {
    "event_types": ["filing", "funding", "acquisition"],
    "sources": ["sec_edgar", "google_news"]
  }
}
```

---

## Files to Create

1. `app/news/__init__.py` - Package init
2. `app/news/aggregator.py` - Main aggregation engine
3. `app/news/sources/__init__.py` - Sources package init
4. `app/news/sources/sec_rss.py` - SEC EDGAR RSS parser
5. `app/news/sources/google_news.py` - Google News RSS parser
6. `app/api/v1/news.py` - API endpoints

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Test endpoints:
   - `POST /api/v1/news/refresh` - Trigger news refresh
   - `GET /api/v1/news/feed?limit=20` - Get news feed
   - `GET /api/v1/news/feed?event_type=filing` - Filter by type
   - `GET /api/v1/news/filings?filing_type=13F` - SEC filings
   - `GET /api/v1/news/company/Apple` - Company news
   - `GET /api/v1/news/investor/1?investor_type=lp` - Investor news

---

## Success Criteria

- [ ] SEC EDGAR RSS parsing fetches recent filings
- [ ] Google News RSS fetches company/investor news
- [ ] News items stored in database with deduplication
- [ ] Event classification works (filing, funding, acquisition)
- [ ] News feed returns filtered, paginated results
- [ ] Company and investor-specific feeds work

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Created `app/news/` package with aggregator and sources
- SEC EDGAR RSS source implemented (13F, 8-K, Form D feeds)
- Google News RSS source implemented with portfolio company queries
- Event classification by keywords (funding, acquisition, IPO, leadership)
- Relevance scoring based on query match and investment keywords
- Deduplication via unique(source, source_id) constraint
- 6 API endpoints implemented
- Tested: 398 items fetched from Google News on first refresh

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
