# PLAN T18: Investor Similarity & Recommendations

## Overview
**Task:** T18
**Tab:** 2
**Feature:** Investor Similarity & Recommendations - Find similar investors and recommend companies
**Status:** PENDING_APPROVAL
**Dependency:** T12 (Search) - COMPLETE

---

## Business Context

### The Problem
Users researching investors need to answer questions like:
- "Which other pension funds have similar portfolios to CalPERS?"
- "If STRS Ohio invests in Microsoft, what other companies might they be interested in?"
- "Who are the most similar investors to Norway GPFG?"

Currently, answering these requires manual comparison of portfolio data across multiple investors - tedious and error-prone.

### Who Uses This
| User Type | Use Case | Example Query |
|-----------|----------|---------------|
| Investment Analysts | Find peer investors for benchmarking | "Similar funds to CalPERS" |
| BD/Sales Teams | Identify prospects based on portfolio fit | "Investors who might like Company X" |
| Portfolio Managers | Discover investment ideas | "What do similar investors hold that we don't?" |
| Researchers | Study investor behavior patterns | "Cluster investors by strategy" |

### Why This Matters
- **Peer Analysis**: Compare your fund against similar investors
- **Deal Sourcing**: Find companies popular among similar investors
- **Network Effects**: Discover co-investment opportunities
- **Due Diligence**: Validate investment theses ("Do similar funds agree?")

### Data Available
Based on current database analysis:
- **27 LP investors** with portfolio data
- **4,007 portfolio companies** indexed
- **Strong overlap**: Microsoft held by 7 investors, many companies by 5-6 investors
- **Investor 4 (STRS Ohio)**: 2,184 holdings - largest portfolio
- **Good coverage**: Mix of public_pension, sovereign_wealth, endowment types

---

## Success Criteria

### Must Have (Launch Blockers)
| Criteria | Metric | Target |
|----------|--------|--------|
| **Similarity accuracy** | Jaccard index calculation | Correct mathematical implementation |
| **Similar investors** | Returns ranked list | Top 10 most similar investors with scores |
| **Recommendations** | Company suggestions | At least 5 companies not in target portfolio |
| **Performance** | Response time | < 500ms for similarity, < 1s for recommendations |
| **Coverage** | Works for all investors | Any investor_id returns results |

### Should Have (Quality Bar)
| Criteria | Metric | Target |
|----------|--------|--------|
| **Confidence scores** | Similarity percentage | 0-100% scale, interpretable |
| **Overlap details** | Show shared holdings | List of common companies |
| **Filtering** | By investor type | Can filter to only pension funds, etc. |
| **Explanation** | Why recommended | "Held by 5 similar investors" |

### Nice to Have (Future)
- Industry-based clustering
- Time-series similarity (portfolio evolution)
- Machine learning recommendations
- Graph-based network analysis

---

## User Stories & Acceptance Criteria

### Story 1: Find Similar Investors
**As an** analyst, **I want to** find investors similar to a target investor **so that** I can benchmark and compare strategies.

**Acceptance Criteria:**
- [ ] `GET /api/v1/discover/similar/1` returns investors similar to CalPERS (id=1)
- [ ] Results sorted by similarity score (highest first)
- [ ] Each result includes: investor name, type, similarity score, overlap count
- [ ] Score is meaningful (0-1 or 0-100% scale)
- [ ] Returns at least 5 similar investors (if data exists)

### Story 2: Get Company Recommendations
**As an** analyst, **I want to** see companies that similar investors hold **so that** I can discover new investment ideas.

**Acceptance Criteria:**
- [ ] `GET /api/v1/discover/recommended/1` returns companies CalPERS might like
- [ ] Only returns companies NOT already in target investor's portfolio
- [ ] Results ranked by "popularity among similar investors"
- [ ] Each result includes: company name, how many similar investors hold it
- [ ] Returns at least 10 recommendations (if data exists)

### Story 3: View Portfolio Overlap
**As an** analyst, **I want to** see which holdings two investors share **so that** I understand their commonalities.

**Acceptance Criteria:**
- [ ] `GET /api/v1/discover/overlap?investor_a=1&investor_b=4` shows shared holdings
- [ ] Returns list of companies both investors hold
- [ ] Shows overlap statistics (count, percentage of each portfolio)

### Story 4: Filter by Investor Type
**As an** analyst, **I want to** find similar investors of the same type **so that** I compare apples to apples.

**Acceptance Criteria:**
- [ ] `GET /api/v1/discover/similar/1?investor_type=public_pension` filters to pension funds
- [ ] Can filter by: public_pension, sovereign_wealth, endowment, family_office

---

## Technical Approach

### Similarity Algorithm: Jaccard Index

The Jaccard index measures similarity between two sets:

```
J(A, B) = |A ∩ B| / |A ∪ B|
```

Where:
- A = set of companies held by investor A
- B = set of companies held by investor B
- |A ∩ B| = number of companies both hold
- |A ∪ B| = total unique companies between both

**Example:**
- CalPERS holds: {MSFT, AAPL, GOOG}
- STRS Ohio holds: {MSFT, AAPL, AMZN, NVDA}
- Intersection: {MSFT, AAPL} = 2
- Union: {MSFT, AAPL, GOOG, AMZN, NVDA} = 5
- Jaccard = 2/5 = 0.40 (40% similar)

### Recommendation Algorithm

"Investors like X also invest in Y":

1. Find top N similar investors to target
2. Collect all their holdings
3. Filter out companies target already holds
4. Rank by frequency (how many similar investors hold it)
5. Return top recommendations

```sql
-- Pseudocode
SELECT company_name, COUNT(*) as investor_count
FROM portfolio_companies
WHERE investor_id IN (similar_investor_ids)
  AND company_name NOT IN (target_holdings)
GROUP BY company_name
ORDER BY investor_count DESC
LIMIT 20
```

---

## Scope

### 1. Recommendations Engine (`app/analytics/recommendations.py`)

**Classes:**
```python
@dataclass
class SimilarInvestor:
    investor_id: int
    investor_type: str
    name: str
    similarity_score: float  # 0.0 to 1.0
    overlap_count: int       # Number of shared holdings
    overlap_companies: List[str]  # Sample of shared companies

@dataclass
class CompanyRecommendation:
    company_name: str
    company_industry: Optional[str]
    held_by_count: int       # How many similar investors hold it
    held_by_names: List[str] # Names of investors who hold it
    confidence: float        # 0.0 to 1.0

@dataclass
class PortfolioOverlap:
    investor_a_id: int
    investor_a_name: str
    investor_b_id: int
    investor_b_name: str
    overlap_count: int
    overlap_percentage_a: float  # % of A's portfolio
    overlap_percentage_b: float  # % of B's portfolio
    shared_companies: List[str]
```

**Functions:**
```python
class RecommendationEngine:
    def get_similar_investors(
        self,
        investor_id: int,
        investor_type: Optional[str] = None,
        limit: int = 10
    ) -> List[SimilarInvestor]

    def get_recommended_companies(
        self,
        investor_id: int,
        similar_count: int = 10,  # How many similar investors to consider
        limit: int = 20
    ) -> List[CompanyRecommendation]

    def get_portfolio_overlap(
        self,
        investor_a_id: int,
        investor_b_id: int
    ) -> PortfolioOverlap

    def _calculate_jaccard(
        self,
        holdings_a: Set[str],
        holdings_b: Set[str]
    ) -> float
```

### 2. API Endpoints (`app/api/v1/discover.py`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/discover/similar/{investor_id}` | Find similar investors |
| GET | `/api/v1/discover/recommended/{investor_id}` | Get company recommendations |
| GET | `/api/v1/discover/overlap` | Compare two investor portfolios |

**Request/Response Models:**
```python
class SimilarInvestorResponse(BaseModel):
    investor_id: int
    investor_type: str
    name: str
    similarity_score: float
    similarity_percentage: float  # score * 100
    overlap_count: int
    sample_overlap: List[str]  # First 5 shared companies

class SimilarInvestorsResponse(BaseModel):
    target_investor_id: int
    target_investor_name: str
    similar_investors: List[SimilarInvestorResponse]
    total_found: int

class CompanyRecommendationResponse(BaseModel):
    company_name: str
    industry: Optional[str]
    held_by_similar_count: int
    held_by_investors: List[str]
    confidence_score: float

class RecommendationsResponse(BaseModel):
    target_investor_id: int
    target_investor_name: str
    recommendations: List[CompanyRecommendationResponse]
    based_on_similar_count: int  # How many similar investors used

class OverlapResponse(BaseModel):
    investor_a: dict  # {id, name, total_holdings}
    investor_b: dict
    overlap_count: int
    overlap_percentage_a: float
    overlap_percentage_b: float
    jaccard_similarity: float
    shared_companies: List[str]
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/analytics/__init__.py` | Package init |
| `app/analytics/recommendations.py` | Similarity and recommendation engine |
| `app/api/v1/discover.py` | API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register discover router |

---

## Implementation Steps

1. Create `app/analytics/` directory with `__init__.py`
2. Implement `recommendations.py`:
   - Jaccard similarity calculation
   - get_similar_investors with SQL optimization
   - get_recommended_companies
   - get_portfolio_overlap
3. Implement `app/api/v1/discover.py` with endpoints
4. Register router in main.py
5. Test all acceptance criteria

---

## Testing Plan

### Test Data Requirements
Current database has:
- 27 investors with portfolios
- STRS Ohio (id=4) has 2,184 holdings - good test target
- CalPERS (id=1) has 33 holdings
- Strong overlap: Microsoft held by 7 investors

### Test Scenarios

#### 1. Similar Investors Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Basic similar | `similar/4` (STRS Ohio) | List of similar investors | At least 5 results, sorted by score |
| Filter by type | `similar/4?investor_type=public_pension` | Only pension funds | All results are public_pension |
| Small portfolio | `similar/1` (CalPERS, 33 holdings) | Similar investors | Returns results even for small portfolios |
| Invalid investor | `similar/9999` | 404 error | Proper error handling |

#### 2. Recommendations Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Basic recommendations | `recommended/1` | Company list | At least 10 recommendations |
| Excludes current | `recommended/1` | Companies NOT in CalPERS | None of their current holdings |
| Has explanations | `recommended/1` | Investor counts | Each has held_by_count > 0 |
| Large portfolio | `recommended/4` | Recommendations | Still finds new companies |

#### 3. Overlap Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Two investors | `overlap?investor_a=1&investor_b=4` | Overlap details | Valid Jaccard score |
| Symmetric | A vs B, B vs A | Same result | Scores match |
| No overlap | Two unrelated investors | Zero overlap | overlap_count = 0, handled gracefully |

#### 4. Performance Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Similar latency | `similar/4` | Response time | < 500ms |
| Recommendations latency | `recommended/4` | Response time | < 1000ms |
| Overlap latency | `overlap?...` | Response time | < 200ms |

### Manual Verification Commands

```bash
# 1. Find investors similar to STRS Ohio
curl "http://localhost:8001/api/v1/discover/similar/4" | jq '.similar_investors[:3]'

# 2. Get recommendations for CalPERS
curl "http://localhost:8001/api/v1/discover/recommended/1" | jq '.recommendations[:5]'

# 3. Compare CalPERS vs STRS Ohio
curl "http://localhost:8001/api/v1/discover/overlap?investor_a=1&investor_b=4" | jq

# 4. Filter similar to only pension funds
curl "http://localhost:8001/api/v1/discover/similar/4?investor_type=public_pension" | jq '.total_found'

# 5. Performance check
curl -w "\nTotal time: %{time_total}s\n" "http://localhost:8001/api/v1/discover/similar/4"
```

---

## SQL Optimization

For performance, use set operations in SQL rather than Python:

```sql
-- Calculate Jaccard similarity between investor_id=1 and all others
WITH target_holdings AS (
    SELECT DISTINCT company_name
    FROM portfolio_companies
    WHERE investor_id = 1
),
other_investors AS (
    SELECT DISTINCT investor_id
    FROM portfolio_companies
    WHERE investor_id != 1
),
similarity AS (
    SELECT
        o.investor_id,
        COUNT(DISTINCT CASE WHEN t.company_name IS NOT NULL THEN p.company_name END) as intersection,
        COUNT(DISTINCT p.company_name) + (SELECT COUNT(*) FROM target_holdings) -
            COUNT(DISTINCT CASE WHEN t.company_name IS NOT NULL THEN p.company_name END) as union_size
    FROM other_investors o
    JOIN portfolio_companies p ON p.investor_id = o.investor_id
    LEFT JOIN target_holdings t ON t.company_name = p.company_name
    GROUP BY o.investor_id
)
SELECT
    s.investor_id,
    l.name,
    l.lp_type,
    s.intersection as overlap_count,
    CAST(s.intersection AS FLOAT) / NULLIF(s.union_size, 0) as jaccard_score
FROM similarity s
JOIN lp_fund l ON l.id = s.investor_id
WHERE s.intersection > 0
ORDER BY jaccard_score DESC
LIMIT 10;
```

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Large portfolios slow | Performance degradation | Pre-compute similarity matrix, cache results |
| Cold start (no data) | No recommendations | Return helpful message, suggest reindex |
| Sparse overlap | Low similarity scores | Adjust thresholds, consider industry similarity |
| Stale data | Outdated recommendations | Link to T07 scheduler for refresh |

---

## Out of Scope (Future Work)

- Machine learning based recommendations (collaborative filtering)
- Industry/sector clustering
- Time-series analysis (portfolio evolution)
- Graph-based network analysis
- Pre-computed similarity matrix (scheduled job)

---

## Approval

- [ ] Approved by user
