# Plan T23: Investment Trend Analysis

## Overview
**Task:** T23
**Tab:** 2
**Feature:** Surface investment trends across LP portfolios - sector rotation, emerging themes, geographic shifts
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

Investment professionals need to understand market-wide trends:

1. **Sector Rotation**: Which industries are LPs moving into/out of?
2. **Emerging Themes**: What sectors are seeing accelerating investment?
3. **Geographic Shifts**: Where is capital flowing geographically?
4. **Stage Preferences**: Are LPs shifting to earlier or later stage?
5. **LP Behavior**: How are different LP types (pension vs endowment) behaving differently?

### User Scenarios

#### Scenario 1: Sector Momentum
**Fund Manager** wants to know which sectors are seeing increased LP allocation.
- Query: "Show me sectors with increasing investment over the last 4 quarters"
- Result: Technology +15%, Healthcare +8%, Energy -5%

#### Scenario 2: Geographic Trends
**LP Relations** wants to understand where capital is flowing.
- Query: "What regions are getting more investment?"
- Result: US West Coast +12%, Asia +18%, Europe flat

#### Scenario 3: LP Type Comparison
**Research Analyst** wants to compare pension fund vs endowment behavior.
- Query: "How do pension funds differ from endowments in sector allocation?"
- Result: Pensions overweight Infrastructure, Endowments overweight Tech

---

## Success Criteria

### Must Have

| ID | Criteria | Verification |
|----|----------|--------------|
| M1 | Sector allocation over time | `/trends/sectors` returns time series |
| M2 | Emerging sectors (momentum) | `/trends/emerging` returns ranked sectors |
| M3 | Geographic distribution | `/trends/geographic` returns by region |
| M4 | Current allocation snapshot | Breakdown by industry |

### Should Have

| ID | Criteria | Verification |
|----|----------|--------------|
| S1 | Filter by LP type | Query param filters results |
| S2 | Filter by time range | Custom date ranges work |
| S3 | Stage trends | Early vs late stage shifts |
| S4 | LP type comparison | Compare allocations by LP type |

---

## Technical Design

### Data Sources

From `portfolio_companies` table:
- `company_industry` - sector classification
- `company_location` - geographic location
- `company_stage` - investment stage
- `investment_date` - when investment was made
- `current_holding` - 1 = active, 0 = exited
- `collected_date` - data freshness

From `lp_fund` table:
- `lp_type` - pension, endowment, sovereign wealth, etc.
- `jurisdiction` - LP location

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/trends/sectors` | Sector allocation trends over time |
| GET | `/api/v1/trends/emerging` | Sectors with momentum (accelerating) |
| GET | `/api/v1/trends/geographic` | Geographic distribution trends |
| GET | `/api/v1/trends/stages` | Investment stage trends |
| GET | `/api/v1/trends/by-lp-type` | Compare trends by LP type |
| GET | `/api/v1/trends/snapshot` | Current allocation snapshot |

### Query Parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | str | "quarter" | Aggregation period: month, quarter, year |
| `periods` | int | 4 | Number of periods to return |
| `lp_type` | str | None | Filter by LP type |
| `min_holdings` | int | 5 | Minimum holdings for inclusion |

### Response Models

```python
class SectorTrend(BaseModel):
    sector: str
    periods: List[PeriodData]  # [{period: "2025-Q1", count: 150, pct: 12.5}, ...]
    change_pct: float  # Change from first to last period
    momentum: str  # "accelerating", "decelerating", "stable"

class EmergingSector(BaseModel):
    sector: str
    current_count: int
    current_pct: float
    change_1q: float  # Quarter-over-quarter change
    change_yoy: float  # Year-over-year change
    momentum_score: float  # Normalized 0-100

class GeographicTrend(BaseModel):
    region: str
    count: int
    pct: float
    change_pct: float
    top_sectors: List[str]

class AllocationSnapshot(BaseModel):
    total_holdings: int
    by_sector: Dict[str, SectorBreakdown]
    by_region: Dict[str, int]
    by_stage: Dict[str, int]
    by_lp_type: Dict[str, int]
```

### Algorithms

**Sector Trend Calculation:**
```sql
-- Aggregate by sector and time period
SELECT
    company_industry as sector,
    DATE_TRUNC(:period, COALESCE(investment_date, collected_date)) as period,
    COUNT(*) as count
FROM portfolio_companies
WHERE current_holding = 1
    AND company_industry IS NOT NULL
GROUP BY company_industry, period
ORDER BY period, count DESC
```

**Momentum Score:**
```python
def calculate_momentum(trend: List[int]) -> float:
    """
    Calculate momentum from trend data.
    Positive = accelerating, Negative = decelerating
    """
    if len(trend) < 2:
        return 0.0

    # Linear regression slope normalized
    changes = [trend[i] - trend[i-1] for i in range(1, len(trend))]
    avg_change = sum(changes) / len(changes)

    # Normalize to -100 to +100 scale
    max_val = max(trend) if max(trend) > 0 else 1
    return (avg_change / max_val) * 100
```

**Geographic Normalization:**
```python
REGION_MAPPING = {
    # US Regions
    "CA": "US West", "WA": "US West", "OR": "US West",
    "NY": "US East", "MA": "US East", "CT": "US East",
    "TX": "US South", "FL": "US South",
    "IL": "US Midwest", "OH": "US Midwest",
    # International
    "UK": "Europe", "Germany": "Europe", "France": "Europe",
    "China": "Asia Pacific", "Japan": "Asia Pacific",
    # etc.
}
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/analytics/trends.py` | TrendAnalysisService with all calculations |
| `app/api/v1/trends.py` | 6 API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register trends router |

---

## Implementation Steps

1. Create `app/analytics/trends.py` with TrendAnalysisService
2. Implement sector trend calculations
3. Implement momentum scoring
4. Implement geographic aggregation
5. Create `app/api/v1/trends.py` with 6 endpoints
6. Register router in main.py
7. Test all endpoints

---

## Test Plan

| Test ID | Test | Expected |
|---------|------|----------|
| TRD-001 | Get sector trends | Returns time series by sector |
| TRD-002 | Get emerging sectors | Returns ranked by momentum |
| TRD-003 | Filter by LP type | Only that LP type's data |
| TRD-004 | Geographic trends | Returns by region |
| TRD-005 | Allocation snapshot | Current state summary |
| TRD-006 | Stage trends | Early/late stage breakdown |

### Test Commands

```bash
# Sector trends over 4 quarters
curl -s "http://localhost:8001/api/v1/trends/sectors?periods=4" | python -m json.tool

# Emerging sectors
curl -s "http://localhost:8001/api/v1/trends/emerging?limit=10" | python -m json.tool

# Geographic distribution
curl -s "http://localhost:8001/api/v1/trends/geographic" | python -m json.tool

# Current snapshot
curl -s "http://localhost:8001/api/v1/trends/snapshot" | python -m json.tool

# By LP type
curl -s "http://localhost:8001/api/v1/trends/by-lp-type" | python -m json.tool
```

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Created `app/analytics/trends.py` with TrendAnalysisService
- Created `app/api/v1/trends.py` with 6 endpoints
- All endpoints tested and working
- Data shows 5236 total holdings across 49 investors (27 LPs, 22 Family Offices)
- Geographic normalization working with REGION_MAPPING
- Momentum scoring algorithm implemented (-100 to +100 scale)

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
