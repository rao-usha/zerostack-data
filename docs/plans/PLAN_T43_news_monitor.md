# Plan T43: Agentic News Monitor

**Task ID:** T43
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [ ] Approved by user (pending)

---

## Goal

Build an AI-powered news monitoring agent that continuously tracks news for watched entities (companies, investors, sectors, keywords), generates digests, and alerts on breaking/high-impact news.

---

## Dependencies

- **T24 (News & Event Feed)**: Provides `NewsAggregator` with feeds from SEC EDGAR, Google News
  - `GET /news/feed` - aggregated news
  - `GET /news/company/{name}` - company-specific news
  - `POST /news/refresh` - trigger news fetch

---

## Design

### Core Components

1. **Watch Lists**: Track entities user wants to monitor
2. **News Matching**: Match incoming news to watch list items
3. **Relevance Scoring**: AI-powered scoring of news importance
4. **Digest Generation**: Summarize news into daily/weekly digests
5. **Breaking Alerts**: Detect and alert on high-impact news

### Database Schema

```sql
-- Watch list items
CREATE TABLE news_watch_items (
    id SERIAL PRIMARY KEY,

    -- What to watch
    watch_type VARCHAR(20) NOT NULL,  -- company, investor, sector, keyword
    watch_value VARCHAR(255) NOT NULL,  -- e.g., "Stripe", "fintech", "acquisition"

    -- Optional filters
    event_types TEXT[],  -- filing, funding, acquisition, news
    min_relevance FLOAT DEFAULT 0.5,  -- minimum relevance score

    -- Notification settings
    alert_enabled BOOLEAN DEFAULT TRUE,
    digest_enabled BOOLEAN DEFAULT TRUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(watch_type, watch_value)
);

-- Matched news items with scores
CREATE TABLE news_matches (
    id SERIAL PRIMARY KEY,

    -- Source news
    news_id INTEGER,  -- reference to news item if stored
    news_title TEXT NOT NULL,
    news_url TEXT,
    news_source VARCHAR(50),
    news_published_at TIMESTAMP,

    -- Match info
    watch_item_id INTEGER REFERENCES news_watch_items(id) ON DELETE CASCADE,
    match_type VARCHAR(20),  -- exact, related, keyword

    -- AI scoring
    relevance_score FLOAT,  -- 0-1 how relevant to watch item
    impact_score FLOAT,  -- 0-1 potential business impact
    sentiment FLOAT,  -- -1 to 1 sentiment

    -- Categorization
    event_type VARCHAR(50),  -- filing, funding, acquisition, partnership, etc.
    summary TEXT,  -- AI-generated summary

    -- Status
    is_breaking BOOLEAN DEFAULT FALSE,
    is_alerted BOOLEAN DEFAULT FALSE,
    is_read BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generated digests
CREATE TABLE news_digests (
    id SERIAL PRIMARY KEY,

    -- Digest period
    period_type VARCHAR(20) NOT NULL,  -- daily, weekly
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,

    -- Content
    summary TEXT,  -- AI-generated summary
    highlights JSONB,  -- top stories
    stats JSONB,  -- counts by category

    -- Status
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(period_type, period_start)
);
```

---

## API Endpoints

### 1. Watch List Management

**POST /monitors/news/watch**
Add item to watch list.

```json
// Request
{
    "watch_type": "company",  // company, investor, sector, keyword
    "watch_value": "Stripe",
    "event_types": ["funding", "acquisition"],  // optional filter
    "min_relevance": 0.6,  // optional threshold
    "alert_enabled": true,
    "digest_enabled": true
}

// Response
{
    "id": 1,
    "watch_type": "company",
    "watch_value": "Stripe",
    "event_types": ["funding", "acquisition"],
    "created_at": "2026-01-19T12:00:00Z"
}
```

**GET /monitors/news/watch**
List all watch items.

**DELETE /monitors/news/watch/{id}**
Remove from watch list.

---

### 2. Personalized Feed

**GET /monitors/news/feed**
Get news matched to watch list, sorted by relevance.

Query params:
- `days`: Time range (default: 7)
- `min_relevance`: Minimum score (default: 0.5)
- `event_type`: Filter by type
- `limit`: Max items (default: 50)

```json
// Response
{
    "items": [
        {
            "id": 123,
            "title": "Stripe Raises $6.5B at $50B Valuation",
            "url": "https://...",
            "source": "google_news",
            "published_at": "2026-01-19T10:30:00Z",
            "matched_watch": {
                "id": 1,
                "type": "company",
                "value": "Stripe"
            },
            "relevance_score": 0.95,
            "impact_score": 0.88,
            "sentiment": 0.7,
            "event_type": "funding",
            "summary": "Stripe secured $6.5B in new funding, valuing the company at $50B..."
        }
    ],
    "total": 47,
    "unread": 12
}
```

---

### 3. Digest Generation

**GET /monitors/news/digest**
Get AI-generated news digest.

Query params:
- `period`: daily, weekly (default: daily)
- `date`: Specific date (default: today)

```json
// Response
{
    "period": "daily",
    "date": "2026-01-19",
    "summary": "Today saw significant activity in fintech with Stripe's major funding round...",
    "highlights": [
        {
            "title": "Stripe Raises $6.5B",
            "impact": "high",
            "summary": "..."
        }
    ],
    "by_category": {
        "funding": 5,
        "acquisitions": 2,
        "filings": 12,
        "partnerships": 3
    },
    "sentiment_summary": {
        "positive": 15,
        "neutral": 8,
        "negative": 2
    },
    "generated_at": "2026-01-19T18:00:00Z"
}
```

**POST /monitors/news/digest/generate**
Force regenerate digest for a period.

---

### 4. Breaking News Alerts

**GET /monitors/news/alerts**
Get breaking/high-impact news alerts.

```json
// Response
{
    "alerts": [
        {
            "id": 456,
            "title": "SEC Charges Major Hedge Fund with Fraud",
            "impact_score": 0.95,
            "event_type": "regulatory",
            "matched_watches": ["hedge funds", "SEC filings"],
            "summary": "The SEC announced charges against...",
            "published_at": "2026-01-19T14:30:00Z",
            "acknowledged": false
        }
    ],
    "unacknowledged": 3
}
```

**POST /monitors/news/alerts/{id}/acknowledge**
Mark alert as acknowledged.

---

### 5. Statistics

**GET /monitors/news/stats**
Get monitoring statistics.

```json
// Response
{
    "watch_items": 15,
    "matches_today": 23,
    "matches_this_week": 142,
    "unread": 34,
    "pending_alerts": 2,
    "top_sources": [
        {"source": "sec_edgar", "count": 89},
        {"source": "google_news", "count": 53}
    ],
    "top_event_types": [
        {"type": "filing", "count": 72},
        {"type": "funding", "count": 31}
    ]
}
```

---

## Implementation

### NewsMonitor Class

```python
class NewsMonitor:
    """Agentic news monitoring service."""

    def __init__(self, db: Session):
        self.db = db
        self.aggregator = NewsAggregator(db)

    # Watch list management
    def add_watch(self, watch_type, watch_value, **options) -> WatchItem
    def list_watches(self) -> List[WatchItem]
    def remove_watch(self, watch_id: int) -> bool

    # News matching
    def match_news(self, news_items: List[Dict]) -> List[NewsMatch]
    def score_relevance(self, news: Dict, watch: WatchItem) -> float
    def score_impact(self, news: Dict) -> float
    def analyze_sentiment(self, text: str) -> float

    # Feed & alerts
    def get_personalized_feed(self, days: int, min_relevance: float) -> List[NewsMatch]
    def get_breaking_alerts(self, min_impact: float = 0.8) -> List[NewsMatch]

    # Digests
    def generate_digest(self, period: str, date: date) -> Digest
    def get_digest(self, period: str, date: date) -> Optional[Digest]

    # Background processing
    async def process_new_news(self) -> int  # Match new news to watches
    async def check_for_alerts(self) -> List[NewsMatch]  # Find breaking news
```

### Relevance Scoring

Simple heuristic scoring (no external LLM needed):

1. **Exact match** (0.9-1.0): Company/investor name appears in title
2. **Strong match** (0.7-0.9): Name in body, sector matches
3. **Related** (0.5-0.7): Keyword match, same industry
4. **Weak** (0.3-0.5): Tangential relationship

### Impact Scoring

Heuristics for business impact:

- **Funding/M&A**: High impact (0.8-1.0)
- **SEC Enforcement**: High impact (0.9)
- **Leadership changes**: Medium-high (0.7-0.8)
- **Product launches**: Medium (0.5-0.7)
- **General news**: Low-medium (0.3-0.5)

### Digest Generation

Simple template-based digest (no LLM):

```
# Daily News Digest - January 19, 2026

## Top Stories
1. [Stripe Raises $6.5B] - Funding round at $50B valuation
2. [SEC Files Charges] - Regulatory action against hedge fund

## By Category
- Funding: 5 items
- Acquisitions: 2 items
- SEC Filings: 12 items

## Sentiment
- Positive: 15 (60%)
- Neutral: 8 (32%)
- Negative: 2 (8%)
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/agents/news_monitor.py` | Core monitoring logic |
| `app/api/v1/monitors.py` | API endpoints |

---

## Test Plan

1. **Unit Tests**
   - Watch list CRUD
   - Relevance scoring
   - Impact scoring
   - Digest generation

2. **Integration Tests**
   - Add watches, check news matches
   - Generate digest with real news data

3. **Manual Testing**
   ```bash
   # Add watch
   curl -X POST http://localhost:8001/api/v1/monitors/news/watch \
     -H "Content-Type: application/json" \
     -d '{"watch_type": "company", "watch_value": "Stripe"}'

   # Get personalized feed
   curl http://localhost:8001/api/v1/monitors/news/feed

   # Get digest
   curl http://localhost:8001/api/v1/monitors/news/digest

   # Get alerts
   curl http://localhost:8001/api/v1/monitors/news/alerts
   ```

---

## Success Criteria

- [ ] Watch list CRUD working
- [ ] News matched to watch items with relevance scores
- [ ] Personalized feed returns matched news sorted by relevance
- [ ] Digest generated with summary and stats
- [ ] Breaking alerts identified (high impact news)
- [ ] Stats endpoint shows monitoring activity

