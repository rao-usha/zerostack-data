# Plan T40: Predictive Deal Scoring

**Task ID:** T40
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19)

---

## Goal

Build a predictive scoring model for deal opportunities in the pipeline. The model combines deal attributes, company health scores (T36), and historical patterns to predict win probability and provide actionable insights.

---

## Dependencies

- **T28 (Deal Flow Tracker)**: Provides deal data (company_name, sector, stage, pipeline_stage, deal_size, valuation, priority, fit_score, activities)
- **T36 (Company Scoring)**: Provides company health scores (0-100 composite, category scores, tier)

---

## Design

### Scoring Model

The deal scorer uses a weighted feature model combining:

1. **Company Quality (40%)** - from T36 company scores
   - Composite score (0-100)
   - Category scores (growth, stability, market, tech)
   - Tier (A-F)

2. **Deal Characteristics (30%)**
   - Deal size appropriateness (sweet spot analysis)
   - Valuation reasonableness
   - Sector fit with fund thesis
   - Stage alignment

3. **Pipeline Signals (20%)**
   - Pipeline velocity (days in each stage)
   - Activity frequency and recency
   - Priority score
   - Existing fit_score

4. **Historical Patterns (10%)**
   - Win rate for similar deals (sector, size, source)
   - Seasonal patterns
   - Source quality

### Win Probability

Output is a probability score (0-100%) with:
- **High confidence**: 70%+ win probability
- **Medium confidence**: 40-70% win probability
- **Low confidence**: <40% win probability

### Database Schema

**Table: `deal_predictions`**
```sql
CREATE TABLE IF NOT EXISTS deal_predictions (
    id SERIAL PRIMARY KEY,
    deal_id INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,

    -- Scores
    win_probability FLOAT NOT NULL,
    confidence VARCHAR(20),

    -- Category scores
    company_score FLOAT,
    deal_score FLOAT,
    pipeline_score FLOAT,
    pattern_score FLOAT,

    -- Insights
    strengths JSONB,
    risks JSONB,
    recommendations JSONB,

    -- Similar deals
    similar_deal_ids INTEGER[],

    -- Timing
    optimal_close_window VARCHAR(50),
    days_to_decision INTEGER,

    -- Metadata
    model_version VARCHAR(20) DEFAULT 'v1.0',
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(deal_id, model_version)
);
```

---

## API Endpoints

### 1. Score Deal
`GET /api/v1/predictions/deal/{deal_id}`

Score a single deal with full breakdown.

**Response:**
```json
{
    "deal_id": 123,
    "company_name": "TechCorp",
    "win_probability": 0.72,
    "confidence": "high",
    "tier": "A",
    "scores": {
        "company_score": 78.5,
        "deal_score": 65.0,
        "pipeline_score": 80.0,
        "pattern_score": 70.0
    },
    "strengths": [
        "Strong company health score (78/100)",
        "Active pipeline with 5 activities this week",
        "Similar deals from this source have 65% win rate"
    ],
    "risks": [
        "Valuation above typical range for sector",
        "Longer than average time in due diligence"
    ],
    "recommendations": [
        "Schedule founder meeting within 7 days",
        "Request updated financials for Q4"
    ],
    "optimal_close_window": "30-45 days",
    "similar_deals": [
        {"id": 45, "company_name": "SimilarCo", "outcome": "closed_won", "similarity": 0.85}
    ]
}
```

### 2. Scored Pipeline
`GET /api/v1/predictions/pipeline`

Get all active deals with scores, sorted by win probability.

**Query Parameters:**
- `pipeline_stage`: Filter by stage (default: active stages only)
- `min_probability`: Minimum win probability (default: 0)
- `limit`: Max results (default: 50)

**Response:**
```json
{
    "deals": [
        {
            "deal_id": 123,
            "company_name": "TechCorp",
            "pipeline_stage": "due_diligence",
            "win_probability": 0.72,
            "confidence": "high",
            "tier": "A",
            "priority": 1,
            "days_in_stage": 12,
            "next_action": "Schedule term sheet review"
        }
    ],
    "summary": {
        "total_deals": 25,
        "avg_probability": 0.45,
        "high_confidence_count": 8,
        "expected_wins": 11.2
    }
}
```

### 3. Similar Deals
`GET /api/v1/predictions/similar/{deal_id}`

Find similar historical deals to guide strategy.

**Query Parameters:**
- `limit`: Max similar deals (default: 5)
- `include_lost`: Include closed_lost deals (default: true)

**Response:**
```json
{
    "deal_id": 123,
    "similar_deals": [
        {
            "id": 45,
            "company_name": "SimilarCo",
            "sector": "fintech",
            "deal_size_millions": 15.0,
            "outcome": "closed_won",
            "days_to_close": 45,
            "similarity_score": 0.85,
            "similarity_factors": ["same_sector", "similar_size", "same_source"]
        }
    ],
    "pattern_insights": {
        "avg_days_to_close": 42,
        "win_rate": 0.65,
        "common_success_factors": ["founder_meeting_early", "reference_checks_positive"]
    }
}
```

### 4. Pipeline Insights
`GET /api/v1/predictions/insights`

Aggregate insights across the pipeline.

**Response:**
```json
{
    "pipeline_health": {
        "total_active_deals": 25,
        "total_pipeline_value_millions": 150.5,
        "expected_value_millions": 67.7,
        "avg_win_probability": 0.45
    },
    "stage_analysis": [
        {
            "stage": "sourced",
            "count": 10,
            "avg_probability": 0.25,
            "avg_days": 8
        },
        {
            "stage": "due_diligence",
            "count": 5,
            "avg_probability": 0.60,
            "avg_days": 15
        }
    ],
    "risk_alerts": [
        {
            "deal_id": 78,
            "company_name": "SlowCorp",
            "alert": "Stalled in negotiation for 30+ days",
            "recommendation": "Re-engage or pass"
        }
    ],
    "opportunities": [
        {
            "deal_id": 123,
            "company_name": "TechCorp",
            "insight": "High-probability deal ready for term sheet"
        }
    ],
    "sector_performance": {
        "fintech": {"deals": 8, "avg_probability": 0.55},
        "healthtech": {"deals": 5, "avg_probability": 0.42}
    }
}
```

### 5. Batch Score
`POST /api/v1/predictions/batch`

Score multiple deals at once.

**Request:**
```json
{
    "deal_ids": [123, 124, 125]
}
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/ml/deal_scorer.py` | Core prediction engine |
| `app/api/v1/predictions.py` | API endpoints |

---

## Implementation Notes

1. **Feature Engineering**
   - Normalize all scores to 0-100 scale
   - Handle missing data gracefully (impute with sector averages)
   - Log transform deal sizes for better distribution

2. **Similar Deal Matching**
   - Jaccard similarity on: sector, deal_type, company_stage
   - Euclidean distance on: deal_size, valuation
   - Weight recent deals higher

3. **Confidence Calculation**
   - Based on data completeness
   - Company score availability
   - Historical deal count for similar profile

4. **Caching**
   - Cache predictions for 24 hours
   - Invalidate on deal update

---

## Test Plan

1. **Unit Tests**
   - Feature engineering functions
   - Score calculation
   - Similar deal matching

2. **Integration Tests**
   - Score deals with company scores
   - Score deals without company scores
   - Pipeline aggregation

3. **Manual Testing**
   ```bash
   # Create test deal
   curl -X POST http://localhost:8001/api/v1/deals \
     -H "Content-Type: application/json" \
     -d '{"company_name": "TestCorp", "company_sector": "fintech", "deal_size_millions": 10}'

   # Score the deal
   curl http://localhost:8001/api/v1/predictions/deal/1

   # Get scored pipeline
   curl http://localhost:8001/api/v1/predictions/pipeline

   # Get insights
   curl http://localhost:8001/api/v1/predictions/insights
   ```

---

## Success Criteria

- [x] Deals scored with win probability 0-100%
- [x] Company scores integrated when available
- [x] Similar deals identified with similarity scores
- [x] Pipeline insights generated with risk alerts
- [x] Cached predictions updated on deal changes
