# Plan T27: LP Profile Enrichment

**Task ID:** T27
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-16

---

## Goal

Enrich investor (LP and Family Office) profiles with contact information, AUM history, investment preferences, and commitment pace analysis.

---

## Why This Matters

1. **Contact Discovery**: Find key personnel at target investors
2. **AUM Tracking**: Monitor investor growth and capacity over time
3. **Preference Matching**: Understand what sectors/stages investors prefer
4. **Outreach Timing**: Know investment pace to time fundraising outreach

---

## Design

### Database Schema

```sql
-- Investor contacts
CREATE TABLE IF NOT EXISTS investor_contacts (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(50) NOT NULL,

    -- Contact info
    name VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    linkedin_url TEXT,

    -- Role classification
    role_type VARCHAR(50),  -- cio, partner, analyst, admin
    is_primary BOOLEAN DEFAULT FALSE,

    -- Metadata
    source VARCHAR(100),
    confidence_score FLOAT DEFAULT 0.5,
    verified_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- AUM history tracking
CREATE TABLE IF NOT EXISTS investor_aum_history (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(50) NOT NULL,

    -- AUM data
    aum_usd BIGINT,
    aum_date DATE NOT NULL,
    source VARCHAR(100),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(investor_id, investor_type, aum_date)
);

-- Investment preferences (derived from portfolio analysis)
CREATE TABLE IF NOT EXISTS investor_preferences (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(50) NOT NULL,

    -- Sector preferences
    preferred_sectors JSONB,      -- [{sector: "Technology", weight: 0.4}, ...]
    avoided_sectors JSONB,

    -- Stage preferences
    preferred_stages JSONB,       -- [{stage: "Series B", weight: 0.3}, ...]

    -- Geographic preferences
    preferred_regions JSONB,      -- [{region: "North America", weight: 0.6}, ...]

    -- Investment characteristics
    avg_check_size_usd BIGINT,
    min_check_size_usd BIGINT,
    max_check_size_usd BIGINT,

    -- Commitment pace
    investments_per_year FLOAT,
    last_investment_date DATE,

    -- Metadata
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(investor_id, investor_type)
);
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/enrichment/investor/{id}` | POST | Trigger investor enrichment |
| `/enrichment/investor/{id}/status` | GET | Get enrichment job status |
| `/investors/{id}/contacts` | GET | Get investor contacts |
| `/investors/{id}/aum-history` | GET | Get AUM history |
| `/investors/{id}/preferences` | GET | Get investment preferences |

### Investor Enrichment Engine

```python
class InvestorEnrichmentEngine:
    """Investor profile enrichment engine."""

    async def enrich_investor(self, investor_id: int, investor_type: str) -> Dict:
        """Run full enrichment for an investor."""

    def analyze_preferences(self, investor_id: int, investor_type: str) -> Dict:
        """Analyze portfolio to derive investment preferences."""

    def calculate_commitment_pace(self, investor_id: int, investor_type: str) -> Dict:
        """Calculate how frequently investor makes new investments."""

    def extract_contacts_from_form_adv(self, investor_id: int) -> List[Dict]:
        """Extract contacts from SEC Form ADV filings."""

    def track_aum_history(self, investor_id: int, investor_type: str) -> List[Dict]:
        """Build AUM history from available sources."""
```

---

## Implementation

### 1. `app/enrichment/investor.py`
Main investor enrichment engine with:
- Portfolio-based preference analysis
- Commitment pace calculation
- Contact extraction from Form ADV
- AUM history tracking

### 2. Updates to `app/api/v1/enrichment.py`
Add investor enrichment endpoints alongside existing company enrichment.

---

## Response Formats

### Investor Contacts
```json
{
  "investor_id": 1,
  "investor_type": "lp",
  "investor_name": "CalPERS",
  "contacts": [
    {
      "name": "John Smith",
      "title": "Chief Investment Officer",
      "role_type": "cio",
      "email": "jsmith@calpers.ca.gov",
      "is_primary": true,
      "confidence_score": 0.9
    }
  ]
}
```

### AUM History
```json
{
  "investor_id": 1,
  "investor_type": "lp",
  "current_aum_usd": 450000000000,
  "history": [
    {"date": "2024-12-31", "aum_usd": 450000000000, "source": "form_adv"},
    {"date": "2023-12-31", "aum_usd": 440000000000, "source": "form_adv"}
  ],
  "growth_rate_1y": 0.023
}
```

### Investment Preferences
```json
{
  "investor_id": 1,
  "investor_type": "lp",
  "sectors": [
    {"sector": "Technology", "weight": 0.35, "company_count": 15},
    {"sector": "Healthcare", "weight": 0.25, "company_count": 10}
  ],
  "stages": [
    {"stage": "Growth", "weight": 0.45},
    {"stage": "Late Stage", "weight": 0.30}
  ],
  "regions": [
    {"region": "North America", "weight": 0.70},
    {"region": "Europe", "weight": 0.20}
  ],
  "commitment_pace": {
    "investments_per_year": 12.5,
    "last_investment_date": "2024-11-15",
    "avg_days_between_investments": 29
  }
}
```

---

## Files to Create/Modify

1. `app/enrichment/investor.py` - New investor enrichment engine
2. `app/api/v1/enrichment.py` - Add investor endpoints (modify existing)

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Test endpoints:
   - `POST /api/v1/enrichment/investor/1?investor_type=lp` - Trigger enrichment
   - `GET /api/v1/enrichment/investor/1/status?investor_type=lp` - Check status
   - `GET /api/v1/investors/1/contacts?investor_type=lp` - Get contacts
   - `GET /api/v1/investors/1/aum-history?investor_type=lp` - Get AUM history
   - `GET /api/v1/investors/1/preferences?investor_type=lp` - Get preferences

---

## Success Criteria

- [ ] Preference analysis derives sectors/stages from portfolio
- [ ] Commitment pace calculates investment frequency
- [ ] AUM history tracks values over time
- [ ] Contact extraction works from available data
- [ ] All 5 endpoints return properly formatted data

---

## Approval

- [x] **Approved by user** (2026-01-16)

---

*Plan created: 2026-01-16*
