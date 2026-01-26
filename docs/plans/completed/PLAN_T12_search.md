# PLAN T12: Full-Text Search API

## Overview
**Task:** T12
**Tab:** 2
**Feature:** Full-Text Search API - Fast, typo-tolerant search across investors and portfolio companies
**Status:** PENDING_APPROVAL

---

## Business Context

### The Problem
Nexdata collects portfolio data from 25+ sources for LPs (pension funds, endowments) and their portfolio companies. Currently, users have no way to quickly find specific investors or companies across this data. They would need to:
- Know exact table names and write SQL queries
- Browse through API responses manually
- Remember exact spelling of fund names like "California Public Employees' Retirement System"

This creates friction for the core use cases:
1. **Investment Research**: "Which pension funds are invested in climate tech?"
2. **Due Diligence**: "Show me everything we have on Sequoia Capital"
3. **Competitive Analysis**: "What companies overlap between CalPERS and CalSTRS?"
4. **Deal Sourcing**: "Find all Series B companies in healthcare"

### Who Uses This
| User Type | Use Case | Example Query |
|-----------|----------|---------------|
| Investment Analysts | Research investor strategies | "CalPERS private equity" |
| BD/Sales Teams | Find prospects by sector | "healthcare investors California" |
| Data Team | Verify data quality | "companies missing industry" |
| API Consumers | Build downstream apps | Programmatic access to search |

### Why Now
- Phase 1 built data collection infrastructure
- Phase 2 is about making data accessible
- T18 (Investor Similarity) and T20 (Watchlists) depend on search
- Search is foundational for any user-facing application

---

## Success Criteria

### Must Have (Launch Blockers)
| Criteria | Metric | Target |
|----------|--------|--------|
| **Search latency** | P95 response time | < 200ms for queries on 100K records |
| **Typo tolerance** | Fuzzy match accuracy | "calprs" finds "CalPERS", "sequoa" finds "Sequoia" |
| **Relevance** | Exact matches rank first | Searching "CalPERS" returns CalPERS as #1 result |
| **Coverage** | Index completeness | 100% of lp_fund, portfolio_companies, co_investments indexed |
| **Zero downtime** | Reindex without outage | Reindexing doesn't block search queries |

### Should Have (Quality Bar)
| Criteria | Metric | Target |
|----------|--------|--------|
| **Facet accuracy** | Filter counts match results | Clicking "healthcare" shows only healthcare results |
| **Autocomplete speed** | Suggestion latency | < 100ms for prefix queries |
| **Empty query handling** | Graceful degradation | Returns "no results" not errors |
| **Pagination** | Large result sets | Handle 10K+ results without timeout |

### Nice to Have (Future)
- Search analytics (popular queries, zero-result queries)
- Query spell-check suggestions ("Did you mean...?")
- Saved search queries (depends on T20)
- Search result caching

---

## User Stories & Acceptance Criteria

### Story 1: Basic Search
**As an** analyst, **I want to** search for investors by name **so that** I can quickly find their portfolio data.

**Acceptance Criteria:**
- [ ] `GET /api/v1/search?q=calpers` returns CalPERS as top result
- [ ] Results include investor name, type, and metadata
- [ ] Response includes total count and pagination info
- [ ] Empty query returns error with helpful message

### Story 2: Typo-Tolerant Search
**As a** user, **I want** search to handle my typos **so that** I don't need to know exact spellings.

**Acceptance Criteria:**
- [ ] "calprs" (missing 'e') finds "CalPERS"
- [ ] "califronia" finds "California" in locations
- [ ] "seqoia" finds "Sequoia Capital"
- [ ] Fuzzy matches rank below exact matches

### Story 3: Filtered Search
**As an** analyst, **I want to** filter search by industry and investor type **so that** I can narrow down results.

**Acceptance Criteria:**
- [ ] `?q=tech&industry=technology` returns only tech companies
- [ ] `?q=pension&investor_type=public_pension` returns only pension funds
- [ ] `?types=company` returns only portfolio companies, not investors
- [ ] Facets show accurate counts for each filter option

### Story 4: Autocomplete
**As a** user, **I want** search suggestions as I type **so that** I can find things faster.

**Acceptance Criteria:**
- [ ] `GET /api/v1/search/suggest?prefix=cal` returns "CalPERS", "CalSTRS", etc.
- [ ] Suggestions return within 100ms
- [ ] Suggestions are ranked by relevance
- [ ] Typing "seq" suggests "Sequoia Capital"

### Story 5: Cross-Entity Search
**As an** analyst, **I want to** search across investors AND companies in one query **so that** I get a complete picture.

**Acceptance Criteria:**
- [ ] Searching "Stripe" finds both the company AND investors who hold it
- [ ] Results are grouped/labeled by type
- [ ] Can filter to specific types if needed

---

## Scope

### 1. Search Engine (`app/search/engine.py`)

Core search functionality with PostgreSQL full-text search and fuzzy matching.

**Classes:**
```python
class SearchResultType(Enum):
    INVESTOR = "investor"       # LP funds
    COMPANY = "company"         # Portfolio companies
    CO_INVESTOR = "co_investor" # Co-investment partners

class SearchResult:
    id: int
    result_type: SearchResultType
    name: str
    description: Optional[str]
    relevance_score: float
    metadata: dict  # Type-specific fields
    highlight: Optional[str]  # Matched text with highlights

class SearchFacets:
    result_types: Dict[str, int]  # {investor: 10, company: 50}
    industries: Dict[str, int]
    investor_types: Dict[str, int]  # {public_pension: 5, endowment: 3}
    locations: Dict[str, int]

class SearchResponse:
    results: List[SearchResult]
    facets: SearchFacets
    total_count: int
    page: int
    page_size: int
    query: str
    search_time_ms: float
```

**Functions:**
```python
class SearchEngine:
    async def search(
        self,
        query: str,
        result_types: Optional[List[SearchResultType]] = None,
        industry: Optional[str] = None,
        investor_type: Optional[str] = None,
        location: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        fuzzy: bool = True
    ) -> SearchResponse

    async def suggest(
        self,
        prefix: str,
        limit: int = 10,
        result_types: Optional[List[SearchResultType]] = None
    ) -> List[SearchSuggestion]

    async def reindex(
        self,
        entity_type: Optional[SearchResultType] = None
    ) -> int  # Returns number of records indexed
```

### 2. Database Tables

**search_index:** (Unified search index table)
```sql
CREATE TABLE search_index (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,  -- 'investor', 'company', 'co_investor'
    entity_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,  -- Lowercase, stripped for matching
    description TEXT,
    industry VARCHAR(255),
    investor_type VARCHAR(100),  -- For investors only
    location VARCHAR(255),
    metadata JSONB,  -- Type-specific fields
    search_vector TSVECTOR,  -- PostgreSQL full-text search vector
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(entity_type, entity_id)
);

-- GIN index for fast full-text search
CREATE INDEX idx_search_vector ON search_index USING GIN(search_vector);

-- Indexes for faceted filtering
CREATE INDEX idx_search_entity_type ON search_index(entity_type);
CREATE INDEX idx_search_industry ON search_index(industry);
CREATE INDEX idx_search_investor_type ON search_index(investor_type);
CREATE INDEX idx_search_location ON search_index(location);

-- Trigram index for fuzzy matching (requires pg_trgm extension)
CREATE INDEX idx_search_name_trgm ON search_index USING GIN(name_normalized gin_trgm_ops);
```

**Trigger for search_vector auto-update:**
```sql
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector =
        setweight(to_tsvector('english', COALESCE(NEW.name, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.industry, '')), 'C');
    NEW.name_normalized = lower(regexp_replace(NEW.name, '[^a-zA-Z0-9 ]', '', 'g'));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_search_vector_update
BEFORE INSERT OR UPDATE ON search_index
FOR EACH ROW EXECUTE FUNCTION update_search_vector();
```

### 3. API Endpoints (`app/api/v1/search.py`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/search` | Unified full-text search |
| GET | `/api/v1/search/suggest` | Autocomplete suggestions |
| POST | `/api/v1/search/reindex` | Trigger reindexing (admin) |
| GET | `/api/v1/search/stats` | Search index statistics |

**Request/Response Models:**
```python
class SearchRequest(BaseModel):
    q: str  # Search query
    types: Optional[List[str]] = None  # Filter by result type
    industry: Optional[str] = None
    investor_type: Optional[str] = None
    location: Optional[str] = None
    page: int = 1
    page_size: int = 20
    fuzzy: bool = True

class SearchResultResponse(BaseModel):
    id: int
    type: str  # 'investor', 'company', 'co_investor'
    name: str
    description: Optional[str]
    relevance_score: float
    metadata: dict
    highlight: Optional[str]

class SearchResponse(BaseModel):
    results: List[SearchResultResponse]
    facets: dict  # {result_types: {...}, industries: {...}, ...}
    total: int
    page: int
    page_size: int
    query: str
    search_time_ms: float

class SuggestRequest(BaseModel):
    prefix: str
    limit: int = 10
    types: Optional[List[str]] = None

class SuggestionResponse(BaseModel):
    text: str
    type: str
    id: int
    score: float
```

### 4. Search Algorithm

**Full-text search query:**
```sql
-- Basic search with ranking
SELECT *,
    ts_rank_cd(search_vector, plainto_tsquery('english', :query)) as rank
FROM search_index
WHERE search_vector @@ plainto_tsquery('english', :query)
ORDER BY rank DESC
LIMIT :limit OFFSET :offset;

-- Fuzzy search (when no exact matches or fuzzy=True)
SELECT *,
    similarity(name_normalized, :normalized_query) as similarity_score
FROM search_index
WHERE name_normalized % :normalized_query
   OR search_vector @@ plainto_tsquery('english', :query)
ORDER BY
    CASE WHEN search_vector @@ plainto_tsquery('english', :query)
         THEN ts_rank_cd(search_vector, plainto_tsquery('english', :query)) + 0.5
         ELSE similarity(name_normalized, :normalized_query)
    END DESC
LIMIT :limit OFFSET :offset;
```

**Relevance scoring:**
- Exact name match: +1.0
- Full-text match in name (weight A): +0.8
- Full-text match in description (weight B): +0.4
- Full-text match in industry (weight C): +0.2
- Trigram similarity: +similarity_score

---

## Files to Create

| File | Description |
|------|-------------|
| `app/search/__init__.py` | Package init |
| `app/search/engine.py` | Search engine and indexing logic |
| `app/api/v1/search.py` | API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register search router |

---

## Implementation Steps

1. Create `app/search/` directory with `__init__.py`
2. Implement `engine.py` with SearchEngine class:
   - Database table creation (search_index)
   - Enable pg_trgm extension
   - Index population from lp_fund, portfolio_companies, co_investments
   - Full-text search using ts_rank_cd
   - Fuzzy matching using pg_trgm
   - Faceted filtering aggregation
   - Autocomplete using prefix matching + trigrams
3. Implement `app/api/v1/search.py` with endpoints
4. Register router in main.py
5. Test all acceptance criteria

---

## Testing Plan

### Test Data Requirements
Before testing, ensure database has:
- At least 5 investors in `lp_fund` (e.g., CalPERS, CalSTRS, NYSERS, etc.)
- At least 20 portfolio companies across different industries
- At least 10 co-investors

### Test Scenarios

#### 1. Index Population Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Reindex all | `POST /search/reindex` | Count of indexed records | Count > 0, matches source tables |
| Index stats | `GET /search/stats` | Breakdown by entity type | Numbers match source table counts |
| Partial reindex | `POST /search/reindex?type=investor` | Only investors reindexed | company count unchanged |

#### 2. Basic Search Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Exact match | `?q=CalPERS` | CalPERS as #1 result | relevance_score > 0.9 |
| Partial match | `?q=California` | All CA-based entities | Results contain "California" |
| Multi-word | `?q=public pension` | Public pension funds | investor_type = public_pension |
| No results | `?q=xyznonexistent123` | Empty results | total = 0, no error |
| Empty query | `?q=` | Error response | 400 status with message |

#### 3. Fuzzy Matching Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Missing letter | `?q=calprs` | CalPERS found | CalPERS in top 3 results |
| Swapped letters | `?q=CalPRES` | CalPERS found | CalPERS in top 3 results |
| Wrong vowel | `?q=CalPARS` | CalPERS found | CalPERS in top 3 results |
| Phonetic | `?q=sequoa` | Sequoia found | Sequoia in top 5 results |
| Fuzzy disabled | `?q=calprs&fuzzy=false` | No results | total = 0 |

#### 4. Filtered Search Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| By type | `?q=&types=investor` | Only investors | All results have type=investor |
| By industry | `?q=&industry=healthcare` | Healthcare only | All results have industry=healthcare |
| By investor type | `?q=&investor_type=public_pension` | Pension funds only | All investor_type=public_pension |
| Combined filters | `?q=tech&types=company&industry=technology` | Tech companies | Intersection of all filters |

#### 5. Facet Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Type facets | `?q=pension` | Facet counts by type | Sum of facets = total |
| Industry facets | `?q=&types=company` | Industry breakdown | Each industry count > 0 |
| Facet accuracy | Click facet filter | Results match count | Filtered total = facet count |

#### 6. Autocomplete Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Prefix match | `prefix=cal` | CalPERS, CalSTRS, etc. | All start with "Cal" |
| Short prefix | `prefix=c` | Top matches for "c" | Returns <= limit results |
| Filtered suggest | `prefix=cal&types=investor` | Only investors | No companies in results |
| Empty prefix | `prefix=` | Error or empty | Graceful handling |

#### 7. Performance Tests
| Test | Input | Expected Output | Pass Criteria |
|------|-------|-----------------|---------------|
| Search latency | `?q=pension` | Response with timing | search_time_ms < 200 |
| Suggest latency | `prefix=cal` | Suggestions | Response < 100ms |
| Pagination | `?q=&page=1&page_size=100` | 100 results | No timeout, correct count |
| Large offset | `?q=&page=100&page_size=20` | Page 100 results | No performance degradation |

### Manual Verification Commands

```bash
# 1. Reindex and verify
curl -X POST "http://localhost:8001/api/v1/search/reindex" | jq
curl "http://localhost:8001/api/v1/search/stats" | jq

# 2. Basic search
curl "http://localhost:8001/api/v1/search?q=calpers" | jq '.results[0]'

# 3. Typo tolerance
curl "http://localhost:8001/api/v1/search?q=calprs" | jq '.results[0].name'
# Expected: "CalPERS" or similar

# 4. Filtered search
curl "http://localhost:8001/api/v1/search?q=tech&types=company" | jq '.total'

# 5. Verify facets
curl "http://localhost:8001/api/v1/search?q=pension" | jq '.facets'

# 6. Autocomplete
curl "http://localhost:8001/api/v1/search/suggest?prefix=cal" | jq

# 7. Performance check
curl -w "\nTotal time: %{time_total}s\n" "http://localhost:8001/api/v1/search?q=investment"
# Expected: < 0.2s
```

---

## Dependencies

- **PostgreSQL pg_trgm extension** - for trigram-based fuzzy matching
  - Already commonly available in PostgreSQL
  - Enable with: `CREATE EXTENSION IF NOT EXISTS pg_trgm;`

---

## Performance Considerations

- **GIN indexes** on search_vector provide fast full-text search (O(log n))
- **Trigram indexes** enable efficient fuzzy matching
- **Facet aggregation** done in single query with GROUP BY
- **Index population** happens incrementally when data changes
- **Caching** of frequent queries can be added via cache.py (T03) later

---

## Data Sources Indexed

| Entity Type | Source Table | Fields Indexed |
|-------------|--------------|----------------|
| investor | lp_fund | name, formal_name, lp_type, jurisdiction |
| company | portfolio_companies | company_name, company_industry, company_location |
| co_investor | co_investments | co_investor_name, co_investor_type |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| pg_trgm not available | Fuzzy search won't work | Check for extension at startup, warn if missing |
| Large index slows writes | Insert latency increases | Use batch inserts, async reindexing |
| Stale index data | Search returns outdated results | Schedule periodic reindex via T07 |
| Query injection | Security vulnerability | Use parameterized queries only |

---

## Out of Scope (Future Work)

- Search analytics and query logging
- Spell-check suggestions ("Did you mean...?")
- Semantic/vector search (would require embeddings)
- Search result caching (can add via T03 cache)
- Federated search across external sources

---

## Approval

- [ ] Approved by user
