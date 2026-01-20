# T49: Agentic Market Scanner

## Status
- [x] Approved
- [x] Completed

## Goal
Build an AI agent that scans all data sources to identify market trends, emerging patterns, and investment opportunities - generating actionable market intelligence briefs.

## Dependencies
- T23 (Investment Trend Analysis) - sector/geographic trends ✅
- T43 (News Monitor) - news events and sentiment ✅

## Features

### 1. Market Signal Detection
- Scans across all data sources for emerging patterns
- Detects signals: hot sectors, funding surges, geographic shifts, talent movements
- Assigns confidence and strength scores to each signal
- Tracks signal evolution over time

### 2. Opportunity Identification
- Identifies undervalued sectors (declining attention, improving fundamentals)
- Spots emerging themes before mainstream awareness
- Finds geographic arbitrage opportunities
- Detects talent migration patterns

### 3. Weekly Market Brief
- Auto-generates market intelligence summary
- Highlights top signals by category
- Includes "early signals" section for emerging patterns
- Compares current market to historical patterns

### 4. Customizable Focus Areas
- Users can configure watched sectors/regions
- Adjustable signal thresholds
- Custom alert triggers

## Data Sources Scanned

| Source | Signals Detected |
|--------|-----------------|
| Form D Filings | Funding activity by sector/region |
| GitHub Activity | Developer interest trends |
| Glassdoor | Talent movement, company growth signals |
| App Store | Consumer interest shifts |
| Web Traffic | Market attention changes |
| News | Event frequency, sentiment shifts |
| Company Scores | Aggregate health trends |

## API Endpoints

### 1. `GET /api/v1/market/scan`
Current market signals (cached, refreshed hourly).
```json
{
  "scan_timestamp": "2026-01-19T12:00:00Z",
  "signals": [
    {
      "signal_id": "sig_abc123",
      "type": "sector_momentum",
      "category": "AI/ML",
      "direction": "accelerating",
      "strength": 0.85,
      "confidence": 0.78,
      "description": "AI/ML sector showing 45% increase in Form D filings",
      "data_points": [...],
      "first_detected": "2026-01-10T00:00:00Z",
      "trend": "strengthening"
    }
  ],
  "total_signals": 12,
  "by_category": {"sector": 5, "geographic": 3, "talent": 2, "funding": 2}
}
```

### 2. `GET /api/v1/market/trends`
Emerging trend analysis with historical comparison.
```json
{
  "period": "30d",
  "trends": [
    {
      "trend_id": "trend_xyz",
      "name": "Healthcare AI Convergence",
      "sectors": ["healthcare", "ai_ml"],
      "momentum": 0.72,
      "stage": "emerging",  // early, emerging, mainstream, declining
      "supporting_signals": [...],
      "historical_comparison": "Similar to fintech surge Q2 2024"
    }
  ]
}
```

### 3. `GET /api/v1/market/opportunities`
Spotted investment opportunities.
```json
{
  "opportunities": [
    {
      "opportunity_id": "opp_123",
      "type": "sector_rotation",
      "title": "Climate Tech Revival",
      "thesis": "Declining attention + improving fundamentals",
      "confidence": 0.68,
      "signals": [...],
      "recommended_actions": ["Monitor Form D filings", "Track GitHub repos"]
    }
  ]
}
```

### 4. `GET /api/v1/market/brief`
Weekly market intelligence brief.
```json
{
  "brief_id": "brief_wk03_2026",
  "period": {"start": "2026-01-13", "end": "2026-01-19"},
  "summary": "Market activity up 12% WoW. AI/ML continues dominance...",
  "sections": {
    "top_signals": [...],
    "emerging_patterns": [...],
    "sector_spotlight": {...},
    "geographic_shifts": [...],
    "early_warnings": [...]
  },
  "generated_at": "2026-01-19T06:00:00Z"
}
```

### 5. `POST /api/v1/market/scan/trigger`
Manually trigger a market scan (admin).

### 6. `GET /api/v1/market/history`
Historical scans and briefs.

## Database Tables

### market_scans
```sql
CREATE TABLE market_scans (
    id SERIAL PRIMARY KEY,
    scan_id VARCHAR(50) UNIQUE NOT NULL,
    scan_type VARCHAR(20) DEFAULT 'scheduled',  -- scheduled, manual, triggered
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',
    sources_scanned JSONB DEFAULT '[]',
    signals_detected INT DEFAULT 0,
    results JSONB,
    error_message TEXT
);
```

### market_signals
```sql
CREATE TABLE market_signals (
    id SERIAL PRIMARY KEY,
    signal_id VARCHAR(50) UNIQUE NOT NULL,
    signal_type VARCHAR(50) NOT NULL,  -- sector_momentum, geographic_shift, talent_flow, funding_surge
    category VARCHAR(100),
    direction VARCHAR(20),  -- accelerating, decelerating, stable
    strength FLOAT,  -- 0-1
    confidence FLOAT,  -- 0-1
    description TEXT,
    data_points JSONB,
    first_detected TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'active',  -- active, resolved, stale
    scan_id VARCHAR(50) REFERENCES market_scans(scan_id)
);
```

### market_briefs
```sql
CREATE TABLE market_briefs (
    id SERIAL PRIMARY KEY,
    brief_id VARCHAR(50) UNIQUE NOT NULL,
    period_start DATE,
    period_end DATE,
    brief_type VARCHAR(20) DEFAULT 'weekly',  -- daily, weekly, monthly
    summary TEXT,
    sections JSONB,
    signals_included JSONB,
    generated_at TIMESTAMP DEFAULT NOW()
);
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/agents/market_scanner.py` | CREATE | Core market scanner agent |
| `app/api/v1/market.py` | CREATE | API endpoints |
| `app/main.py` | MODIFY | Register market router |
| `app/agents/__init__.py` | MODIFY | Export MarketScanner |

## Signal Detection Logic

### Sector Momentum
```python
# Compare current period vs previous
# - Form D filings by industry
# - GitHub repos created/starred
# - Company score improvements
# - News sentiment by sector
if current_activity > previous * 1.3:
    signal = "accelerating"
    strength = (current - previous) / previous
```

### Geographic Shift
```python
# Track regional investment flows
# - Form D filing locations
# - Company HQ changes
# - Talent movement signals
```

### Talent Flow
```python
# Glassdoor + company enrichment signals
# - Employee count changes
# - Rating trends
# - Hiring velocity
```

### Funding Surge
```python
# Form D + news signals
# - Offering amounts
# - Round frequency
# - New fund formations
```

## Implementation Steps

1. Create database tables for scans, signals, briefs
2. Implement signal detection algorithms for each type
3. Build scan orchestration (parallel source queries)
4. Implement brief generation with templated sections
5. Add caching layer (hourly refresh)
6. Create API endpoints
7. Test with real data

## Example Usage

```bash
# Get current market signals
curl http://localhost:8001/api/v1/market/scan

# Get emerging trends
curl http://localhost:8001/api/v1/market/trends?period=30d

# Get weekly brief
curl http://localhost:8001/api/v1/market/brief

# Trigger manual scan
curl -X POST http://localhost:8001/api/v1/market/scan/trigger
```
