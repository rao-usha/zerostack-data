# PLAN T17: Portfolio Comparison Tool

## Overview
**Task:** T17
**Tab:** 2
**Feature:** Portfolio Comparison Tool - Compare portfolios side-by-side (investor vs investor, or over time)
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

Users researching investors need to compare portfolios but have no efficient way to do so:

1. **Manual Comparison**: Analysts must manually export two portfolios to spreadsheets, align columns, and visually scan for differences. This takes 30+ minutes per comparison.

2. **No Overlap Visibility**: When evaluating potential co-investment opportunities, users can't quickly see which companies both investors hold vs. unique holdings.

3. **No Historical Context**: Users can't see how an investor's portfolio has evolved over time (Q1 vs Q2, or year-over-year).

4. **Industry Blind Spots**: Users can't quickly see if two investors have similar or different sector allocations.

### Real-World User Scenarios

#### Scenario 1: Due Diligence Analyst
**David** is conducting due diligence on a potential LP. He needs to:
- Compare the target LP's portfolio against a benchmark fund
- Identify unique holdings that differentiate the LP
- Understand sector concentration differences

**Pain Point**: He spends 2 hours manually creating comparison spreadsheets.

**With Portfolio Comparison**: One API call shows overlap, unique holdings, and industry differences in seconds.

#### Scenario 2: Business Development Rep
**Rachel** is preparing for a meeting with a family office prospect. She wants to:
- Show how the prospect's current portfolio compares to their successful clients
- Highlight gaps in the prospect's coverage
- Suggest companies held by similar investors but not the prospect

**Pain Point**: No way to quickly generate comparison insights.

**With Portfolio Comparison**: She can instantly compare any two portfolios and export the results.

#### Scenario 3: Research Analyst
**Tom** is tracking CalPERS' investment strategy evolution. He needs to:
- Compare CalPERS' Q4 2025 portfolio to Q4 2024
- Identify new positions and exits
- Track sector allocation shifts

**Pain Point**: Historical comparison requires downloading multiple snapshots and manual diffing.

**With Historical Comparison**: API returns a structured diff showing additions, removals, and changes.

### Business Value

| Value | Description | Metric |
|-------|-------------|--------|
| **Time Savings** | Eliminate manual spreadsheet comparison | 2 hrs → 10 sec |
| **Better Insights** | Quantified overlap and differences | +40% insight quality |
| **Competitive Intel** | Compare prospect vs. competitors | +25% conversion |
| **Trend Analysis** | Track portfolio evolution | Enable new use cases |
| **Export Ready** | Comparison results as PDF/CSV | Client-ready reports |

### Integration with Existing Features

| Feature | Integration |
|---------|-------------|
| **T18: Recommendations** | Builds on Jaccard similarity logic for overlap |
| **T13: Dashboard Analytics** | Can show top comparison requests |
| **T20: Watchlists** | Compare watchlist members against each other |
| **T11: Alerts** | Future: Alert when compared portfolios diverge |

---

## Success Criteria

### Must Have (Launch Blockers)

| ID | Criteria | Metric | Target | Verification Method |
|----|----------|--------|--------|---------------------|
| M1 | **Compare two investors** | API returns comparison | Both portfolios compared | `POST /compare/portfolios` returns structured diff |
| M2 | **Overlap count** | Shared holdings count | Accurate | Matches manual count |
| M3 | **Unique holdings A** | Companies only in A | Complete list | All unique companies returned |
| M4 | **Unique holdings B** | Companies only in B | Complete list | All unique companies returned |
| M5 | **Overlap percentage** | % of each portfolio | Calculated correctly | Math verified |
| M6 | **Historical comparison** | Compare investor over time | Returns diff | `GET /compare/investor/{id}/history` works |
| M7 | **Industry breakdown diff** | Sector allocation comparison | Per-industry stats | Shows industry distribution for both |
| M8 | **Response time** | API performance | < 500ms | Timed in tests |

### Should Have (Quality Bar)

| ID | Criteria | Metric | Target | Verification Method |
|----|----------|--------|--------|---------------------|
| S1 | **Value comparison** | Total portfolio value diff | Shown if available | Value deltas displayed |
| S2 | **Top holdings comparison** | Side-by-side top N | Configurable N | `?top_holdings=10` works |
| S3 | **Additions/Removals** | New and exited companies | Complete lists | Historical diff accurate |
| S4 | **Date range filter** | Compare specific periods | Date params work | `?start_date=&end_date=` |
| S5 | **Export format** | Return exportable data | JSON structured | Can convert to CSV/PDF |
| S6 | **Jaccard similarity** | Include similarity score | 0-1 score | Matches T18 algorithm |

### Nice to Have (Future)

| ID | Criteria | Description |
|----|----------|-------------|
| N1 | **Multi-investor comparison** | Compare 3+ investors at once |
| N2 | **Benchmark comparison** | Compare against index/benchmark |
| N3 | **Visual diff export** | PDF with charts |
| N4 | **Webhook on divergence** | Alert when portfolios diverge significantly |
| N5 | **Comparison templates** | Save common comparison pairs |

---

## User Stories & Acceptance Criteria

### Story 1: Compare Two Investors' Portfolios

**As a** user, **I want to** compare two investors' portfolios **so that** I can see overlap and differences.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 1.1 | Basic comparison | Two investors exist | POST `/compare/portfolios` with `{investor_a: 1, investor_b: 4}` | Returns comparison with overlap and unique holdings |
| 1.2 | Overlap metrics | Both have holdings | Comparison response | Includes `overlap_count`, `overlap_percentage_a`, `overlap_percentage_b`, `jaccard_similarity` |
| 1.3 | Unique holdings | Investors have different companies | Comparison response | `unique_to_a` and `unique_to_b` arrays populated |
| 1.4 | Shared holdings list | Overlap exists | Comparison response | `shared_holdings` array with company details |
| 1.5 | Industry breakdown | Holdings have industries | Comparison response | `industry_comparison` shows distribution for both |
| 1.6 | Invalid investor A | Investor doesn't exist | POST with bad ID | Returns 404 |
| 1.7 | Invalid investor B | Investor doesn't exist | POST with bad ID | Returns 404 |
| 1.8 | Same investor | A == B | POST with same IDs | Returns 400 "Cannot compare investor to itself" |
| 1.9 | Empty portfolio | One investor has no holdings | Comparison response | Returns valid response with 0 overlap |

**Test Commands:**
```bash
# AC 1.1: Basic comparison
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4}'
# Expected: 200, structured comparison response

# AC 1.6: Invalid investor
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 9999, "investor_b": 4}'
# Expected: 404

# AC 1.8: Same investor
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 1}'
# Expected: 400
```

---

### Story 2: Compare Investor's Portfolio Over Time

**As a** user, **I want to** compare an investor's portfolio at two points in time **so that** I can track strategy evolution.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 2.1 | Default time comparison | Investor has historical data | GET `/compare/investor/{id}/history` | Returns comparison of latest vs. previous snapshot |
| 2.2 | Custom date range | User specifies dates | GET with `?start_date=2025-01-01&end_date=2025-06-01` | Compares holdings at those dates |
| 2.3 | Additions list | New companies added | Response | `additions` array with new holdings |
| 2.4 | Removals list | Companies exited | Response | `removals` array with exited holdings |
| 2.5 | Unchanged holdings | Companies held in both periods | Response | `unchanged` array with consistent holdings |
| 2.6 | Value changes | Same company, different value | Response | `value_changes` shows delta if available |
| 2.7 | No historical data | Investor has only one snapshot | GET history | Returns 400 "Insufficient historical data" |
| 2.8 | Invalid investor | Investor doesn't exist | GET history | Returns 404 |

**Test Commands:**
```bash
# AC 2.1: Default history comparison
curl "http://localhost:8001/api/v1/compare/investor/4/history"
# Expected: 200, additions/removals/unchanged

# AC 2.2: Custom date range
curl "http://localhost:8001/api/v1/compare/investor/4/history?start_date=2025-01-01&end_date=2025-12-01"
# Expected: 200, comparison for date range

# AC 2.7: No historical data
curl "http://localhost:8001/api/v1/compare/investor/1/history"
# Expected: 400 if only one snapshot exists
```

---

### Story 3: Get Industry Allocation Comparison

**As a** user, **I want to** compare sector allocations between investors **so that** I can understand strategic differences.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 3.1 | Industry breakdown | Two investors with holdings | POST comparison | `industry_comparison` includes per-sector counts |
| 3.2 | Percentage by industry | Holdings have industries | Response | Shows % of portfolio in each industry |
| 3.3 | Industry overlap | Same industries | Response | Shows which industries both invest in |
| 3.4 | Unique industries | Different industries | Response | Shows industries unique to each |
| 3.5 | Null industry handling | Some holdings have no industry | Response | Groups as "Unknown" or excludes |

**Test Commands:**
```bash
# AC 3.1: Industry breakdown in comparison
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4}'
# Expected: industry_comparison field with per-sector breakdown
```

---

### Story 4: Compare Top Holdings

**As a** user, **I want to** compare the top N holdings of two investors **so that** I can see concentration differences.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 4.1 | Default top 10 | Both have 10+ holdings | POST comparison | `top_holdings_a` and `top_holdings_b` show top 10 each |
| 4.2 | Custom top N | User specifies limit | POST with `top_holdings: 5` | Returns top 5 for each |
| 4.3 | Ranking by value | Holdings have values | Response | Sorted by market_value_usd descending |
| 4.4 | Ranking by count | No values available | Response | Sorted by another metric or alphabetically |
| 4.5 | Top holdings overlap | Same companies in top | Response | Highlights overlapping top holdings |

**Test Commands:**
```bash
# AC 4.2: Custom top N
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4, "top_holdings": 5}'
# Expected: top_holdings_a and top_holdings_b each have 5 entries
```

---

## Technical Scope

### API Endpoints

| Method | Endpoint | Description | Request Body | Response |
|--------|----------|-------------|--------------|----------|
| POST | `/api/v1/compare/portfolios` | Compare two investors | `{investor_a, investor_b, top_holdings?}` | Full comparison |
| GET | `/api/v1/compare/investor/{id}/history` | Compare investor over time | Query: `start_date?, end_date?` | Historical diff |
| GET | `/api/v1/compare/industry` | Compare industry allocations only | Query: `investor_a, investor_b` | Industry breakdown |

### Data Models (Pydantic)

```python
# Request Models
class PortfolioCompareRequest(BaseModel):
    investor_a: int = Field(..., gt=0, description="First investor ID")
    investor_b: int = Field(..., gt=0, description="Second investor ID")
    top_holdings: int = Field(10, ge=1, le=50, description="Number of top holdings to include")

# Response Models
class HoldingSummary(BaseModel):
    company_name: str
    company_id: int
    industry: Optional[str]
    market_value_usd: Optional[str]
    shares_held: Optional[str]

class InvestorSummary(BaseModel):
    id: int
    name: str
    total_holdings: int
    total_value: Optional[str]

class IndustryAllocation(BaseModel):
    industry: str
    count_a: int
    count_b: int
    percentage_a: float
    percentage_b: float

class PortfolioComparisonResponse(BaseModel):
    investor_a: InvestorSummary
    investor_b: InvestorSummary

    # Overlap metrics
    overlap_count: int
    overlap_percentage_a: float  # % of A's portfolio that overlaps
    overlap_percentage_b: float  # % of B's portfolio that overlaps
    jaccard_similarity: float
    jaccard_percentage: float

    # Holdings
    shared_holdings: List[HoldingSummary]
    unique_to_a: List[HoldingSummary]
    unique_to_b: List[HoldingSummary]

    # Top holdings
    top_holdings_a: List[HoldingSummary]
    top_holdings_b: List[HoldingSummary]

    # Industry comparison
    industry_comparison: List[IndustryAllocation]

    # Metadata
    comparison_date: str

class HistoricalDiffResponse(BaseModel):
    investor_id: int
    investor_name: str
    period_start: str
    period_end: str

    # Changes
    additions: List[HoldingSummary]
    removals: List[HoldingSummary]
    unchanged: int  # Count only

    # Summary
    holdings_start: int
    holdings_end: int
    net_change: int
```

### Service Architecture

```python
# app/analytics/comparison.py

class PortfolioComparisonService:
    """Service for comparing investor portfolios."""

    def __init__(self, db: Session):
        self.db = db

    def compare_portfolios(
        self,
        investor_a_id: int,
        investor_b_id: int,
        top_holdings: int = 10
    ) -> PortfolioComparison:
        """Compare two investors' current portfolios."""
        # 1. Get holdings for both investors
        # 2. Calculate overlap (shared company names)
        # 3. Identify unique holdings
        # 4. Calculate metrics (overlap %, Jaccard)
        # 5. Get top holdings
        # 6. Build industry comparison
        pass

    def compare_history(
        self,
        investor_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> HistoricalDiff:
        """Compare an investor's portfolio over time."""
        # 1. Get holdings at start_date (or earliest)
        # 2. Get holdings at end_date (or latest)
        # 3. Compute diff: additions, removals, unchanged
        pass

    def get_industry_comparison(
        self,
        investor_a_id: int,
        investor_b_id: int
    ) -> List[IndustryAllocation]:
        """Compare industry allocations between two investors."""
        pass
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/analytics/comparison.py` | Portfolio comparison service |
| `app/api/v1/compare.py` | API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/analytics/__init__.py` | Export PortfolioComparisonService |
| `app/main.py` | Register compare router |

---

## Implementation Steps

1. Create `app/analytics/comparison.py`:
   - PortfolioComparisonService class
   - compare_portfolios() method
   - compare_history() method
   - get_industry_comparison() method
   - Helper methods for metrics calculation

2. Create `app/api/v1/compare.py`:
   - POST `/compare/portfolios` endpoint
   - GET `/compare/investor/{id}/history` endpoint
   - GET `/compare/industry` endpoint
   - Request/Response Pydantic models
   - Error handling

3. Update `app/analytics/__init__.py`:
   - Add conditional import for comparison module

4. Update `app/main.py`:
   - Import and register compare router

5. Test all endpoints

---

## Testing Plan

### Test Environment Setup

```bash
# Ensure Docker is running
docker-compose up -d

# Verify API is healthy
curl http://localhost:8001/health

# Check we have investors with portfolios
curl "http://localhost:8001/api/v1/discover/similar/4"
# Should return investors with holdings
```

### Test Suite

#### Suite 1: Portfolio Comparison

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| PC-001 | Compare two investors | POST with valid IDs | 200, comparison returned | |
| PC-002 | Compare with top_holdings | POST with top_holdings=5 | top_holdings arrays have 5 each | |
| PC-003 | Invalid investor A | POST with investor_a=9999 | 404 | |
| PC-004 | Invalid investor B | POST with investor_b=9999 | 404 | |
| PC-005 | Same investor | POST with a=b | 400 "Cannot compare to itself" | |
| PC-006 | Overlap metrics correct | Compare known investors | Metrics match manual calculation | |
| PC-007 | Unique holdings correct | Compare investors | unique_to_a and unique_to_b accurate | |
| PC-008 | Industry comparison | Compare investors | industry_comparison populated | |
| PC-009 | Response time | POST comparison | < 500ms | |

**Test Commands (PC-001 to PC-009):**
```bash
# PC-001: Compare two investors
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4}'
# Verify: Status 200, has overlap_count, unique_to_a, unique_to_b

# PC-002: With top_holdings
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4, "top_holdings": 5}'
# Verify: top_holdings_a and top_holdings_b each have <= 5 entries

# PC-003: Invalid investor A
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 9999, "investor_b": 4}'
# Verify: Status 404

# PC-005: Same investor
curl -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 4, "investor_b": 4}'
# Verify: Status 400

# PC-009: Response time
curl -w "\nTime: %{time_total}s\n" -X POST "http://localhost:8001/api/v1/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4}'
# Verify: Time < 0.5s
```

#### Suite 2: Historical Comparison

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| HC-001 | Default history | GET history for investor | 200, shows additions/removals | |
| HC-002 | Custom date range | GET with start/end dates | Compares specified period | |
| HC-003 | Invalid investor | GET history for ID 9999 | 404 | |
| HC-004 | No historical data | Investor with one snapshot | 400 or empty diff | |
| HC-005 | Additions correct | Known additions exist | additions array accurate | |
| HC-006 | Removals correct | Known removals exist | removals array accurate | |

**Test Commands (HC-001 to HC-006):**
```bash
# HC-001: Default history
curl "http://localhost:8001/api/v1/compare/investor/4/history"
# Verify: Status 200, has additions/removals arrays

# HC-002: Custom date range
curl "http://localhost:8001/api/v1/compare/investor/4/history?start_date=2025-01-01&end_date=2025-12-31"
# Verify: Status 200, compares within range

# HC-003: Invalid investor
curl "http://localhost:8001/api/v1/compare/investor/9999/history"
# Verify: Status 404
```

#### Suite 3: Industry Comparison

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| IC-001 | Industry breakdown | GET industry comparison | 200, per-industry stats | |
| IC-002 | Percentages sum to 100 | Check percentages | Each investor's percentages ≈ 100% | |
| IC-003 | Unknown industry | Holdings without industry | Handled gracefully | |

**Test Commands (IC-001 to IC-003):**
```bash
# IC-001: Industry comparison
curl "http://localhost:8001/api/v1/compare/industry?investor_a=1&investor_b=4"
# Verify: Status 200, industry_comparison array
```

---

## Integration Test Script

```bash
#!/bin/bash
# T17 Integration Test Suite
# Run: bash test_t17.sh

BASE_URL="http://localhost:8001/api/v1"
PASS=0
FAIL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_pass() { echo -e "${GREEN}PASS${NC}: $1"; ((PASS++)); }
log_fail() { echo -e "${RED}FAIL${NC}: $1 - $2"; ((FAIL++)); }

echo "=========================================="
echo "T17: Portfolio Comparison Tool Tests"
echo "=========================================="

# Test 1: Compare two investors
echo -e "\n--- Test 1: Compare Two Investors ---"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4}')
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" == "200" ]; then
  OVERLAP=$(echo "$BODY" | python -c "import sys,json; print(json.load(sys.stdin).get('overlap_count', -1))" 2>/dev/null)
  if [ "$OVERLAP" != "-1" ]; then
    log_pass "Compare portfolios (overlap: $OVERLAP)"
  else
    log_fail "Compare portfolios" "Missing overlap_count"
  fi
else
  log_fail "Compare portfolios" "HTTP $HTTP_CODE"
fi

# Test 2: Same investor (should fail)
echo -e "\n--- Test 2: Same Investor (Should 400) ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 4, "investor_b": 4}')
if [ "$HTTP_CODE" == "400" ]; then
  log_pass "Same investor rejected"
else
  log_fail "Same investor check" "Expected 400, got $HTTP_CODE"
fi

# Test 3: Invalid investor
echo -e "\n--- Test 3: Invalid Investor (Should 404) ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 9999, "investor_b": 4}')
if [ "$HTTP_CODE" == "404" ]; then
  log_pass "Invalid investor rejected"
else
  log_fail "Invalid investor check" "Expected 404, got $HTTP_CODE"
fi

# Test 4: Top holdings
echo -e "\n--- Test 4: Top Holdings Limit ---"
RESPONSE=$(curl -s -X POST "$BASE_URL/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4, "top_holdings": 5}')
TOP_A=$(echo "$RESPONSE" | python -c "import sys,json; print(len(json.load(sys.stdin).get('top_holdings_a', [])))" 2>/dev/null)
if [ "$TOP_A" -le "5" ]; then
  log_pass "Top holdings limit (got $TOP_A)"
else
  log_fail "Top holdings limit" "Expected <=5, got $TOP_A"
fi

# Test 5: Historical comparison
echo -e "\n--- Test 5: Historical Comparison ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/compare/investor/4/history")
if [ "$HTTP_CODE" == "200" ] || [ "$HTTP_CODE" == "400" ]; then
  log_pass "Historical comparison endpoint works (HTTP $HTTP_CODE)"
else
  log_fail "Historical comparison" "HTTP $HTTP_CODE"
fi

# Test 6: Industry comparison
echo -e "\n--- Test 6: Industry Comparison ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/compare/industry?investor_a=1&investor_b=4")
if [ "$HTTP_CODE" == "200" ]; then
  log_pass "Industry comparison"
else
  log_fail "Industry comparison" "HTTP $HTTP_CODE"
fi

# Test 7: Response time
echo -e "\n--- Test 7: Response Time ---"
TIME=$(curl -s -w "%{time_total}" -o /dev/null -X POST "$BASE_URL/compare/portfolios" \
  -H "Content-Type: application/json" \
  -d '{"investor_a": 1, "investor_b": 4}')
if (( $(echo "$TIME < 0.5" | bc -l) )); then
  log_pass "Response time (${TIME}s)"
else
  log_fail "Response time" "${TIME}s > 0.5s"
fi

# Summary
echo -e "\n=========================================="
echo "Test Summary: $PASS passed, $FAIL failed"
echo "=========================================="

if [ $FAIL -eq 0 ]; then
  echo -e "${GREEN}All tests passed!${NC}"
  exit 0
else
  echo -e "${RED}Some tests failed!${NC}"
  exit 1
fi
```

---

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Large portfolios slow | Query timeout | Medium | Limit results, add pagination |
| No historical data | Feature limited | Medium | Graceful error message |
| Industry data sparse | Incomplete comparison | Medium | Handle nulls, show "Unknown" |
| Company name matching | False positives/negatives | Low | Use company_id when available |

---

## Out of Scope (Future Work)

- Multi-investor comparison (3+ investors)
- Benchmark comparison (vs. S&P 500, etc.)
- PDF/chart export
- Comparison templates
- Real-time diff streaming
- Comparison history storage

---

## Approval

- [x] Approved by user
