# Plan T37: Entity Resolution Service

**Task ID:** T37
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19)

---

## Goal

Build an intelligent entity resolution service that matches and deduplicates entities (companies, investors) across multiple data sources. This service will assign canonical entity IDs, score match confidence, and support manual overrides for corrections.

---

## Problem Statement

The system ingests data from 25+ sources where the same entity appears with variations:
- "Apple Inc" vs "Apple, Inc." vs "APPLE INC"
- "California Public Employees' Retirement System" vs "CalPERS"
- Different CIK/CRD numbers, websites, or addresses for the same entity

We need a centralized service to:
1. Match entities across sources with configurable confidence thresholds
2. Assign stable canonical IDs for deduplication
3. Track all aliases/variants for each entity
4. Allow human review and override of automated matches

---

## Design

### Entity Types

Support three entity types:
1. **company** - Portfolio companies (from `portfolio_companies`, Form D issuers, etc.)
2. **investor** - LP funds and family offices (from `lp_fund`, `family_offices`)
3. **person** - Key contacts and personnel (future extensibility)

### Database Schema

**Table: `canonical_entities`**
```sql
CREATE TABLE canonical_entities (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,  -- 'company', 'investor', 'person'
    canonical_name VARCHAR(500) NOT NULL,
    normalized_name VARCHAR(500) NOT NULL,  -- For matching

    -- Identifiers (any source)
    cik VARCHAR(20),          -- SEC CIK
    crd VARCHAR(20),          -- SEC CRD (for advisers)
    ticker VARCHAR(20),
    cusip VARCHAR(20),
    lei VARCHAR(50),          -- Legal Entity Identifier
    website VARCHAR(500),

    -- Location
    city VARCHAR(200),
    state VARCHAR(100),
    country VARCHAR(100),

    -- Classification
    industry VARCHAR(255),
    entity_subtype VARCHAR(100),  -- e.g., 'public_pension', 'family_office', 'startup'

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100) DEFAULT 'system',  -- 'system' or user email

    UNIQUE(entity_type, normalized_name)
);
```

**Table: `entity_aliases`**
```sql
CREATE TABLE entity_aliases (
    id SERIAL PRIMARY KEY,
    canonical_entity_id INTEGER NOT NULL REFERENCES canonical_entities(id),
    alias_name VARCHAR(500) NOT NULL,
    normalized_alias VARCHAR(500) NOT NULL,

    -- Source tracking
    source_type VARCHAR(100),  -- 'sec_13f', 'form_d', 'form_adv', 'manual'
    source_id VARCHAR(100),    -- Source record ID

    -- Match metadata
    match_confidence FLOAT,    -- 0.0 to 1.0
    is_manual_override BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(canonical_entity_id, normalized_alias)
);
```

**Table: `entity_merge_history`**
```sql
CREATE TABLE entity_merge_history (
    id SERIAL PRIMARY KEY,
    action VARCHAR(20) NOT NULL,  -- 'merge', 'split', 'create', 'update'

    -- For merges: source entity merged INTO target
    source_entity_id INTEGER,
    target_entity_id INTEGER,

    -- Metadata
    reason TEXT,
    performed_by VARCHAR(100) DEFAULT 'system',
    performed_at TIMESTAMP DEFAULT NOW(),

    -- Rollback support
    previous_state JSONB  -- Snapshot before action
);
```

### Matching Algorithm

Multi-stage matching with configurable weights:

1. **Exact Identifier Match** (confidence: 1.0)
   - Same CIK, CRD, ticker, CUSIP, or LEI

2. **Domain Match** (confidence: 0.95)
   - Same website domain (normalized)

3. **Name + Location Match** (confidence: 0.85-0.95)
   - Fuzzy name match (Levenshtein) + same state/country
   - Uses existing `CompanyNameMatcher` from `fuzzy_matcher.py`

4. **Name-Only Match** (confidence: 0.70-0.85)
   - Fuzzy name match without location confirmation
   - Requires manual review for merge

### Confidence Thresholds

- **Auto-merge**: confidence >= 0.90
- **Review queue**: 0.70 <= confidence < 0.90
- **No match**: confidence < 0.70

---

## API Endpoints

### 1. Resolve Entity
`GET /api/v1/entities/resolve`

Find or create a canonical entity for a given name.

**Query Parameters:**
- `name` (required): Entity name to resolve
- `entity_type` (required): 'company' or 'investor'
- `state`: State/province for location matching
- `country`: Country for location matching
- `industry`: Industry for additional scoring
- `cik`, `crd`, `ticker`: Known identifiers

**Response:**
```json
{
    "canonical_entity": {
        "id": 123,
        "canonical_name": "Apple Inc.",
        "entity_type": "company",
        "ticker": "AAPL",
        "website": "apple.com"
    },
    "match_confidence": 0.95,
    "match_method": "ticker_exact",
    "alternatives": [
        {
            "id": 456,
            "canonical_name": "Apple Hospitality REIT",
            "confidence": 0.72
        }
    ]
}
```

### 2. Get Entity Aliases
`GET /api/v1/entities/{id}/aliases`

Get all known aliases for a canonical entity.

**Response:**
```json
{
    "canonical_entity_id": 123,
    "canonical_name": "Apple Inc.",
    "aliases": [
        {
            "alias": "Apple, Inc.",
            "source": "sec_13f",
            "confidence": 1.0
        },
        {
            "alias": "APPLE INC",
            "source": "form_d",
            "confidence": 0.98
        }
    ],
    "total_aliases": 5
}
```

### 3. Merge Entities
`POST /api/v1/entities/merge`

Merge two entities (source merged into target).

**Request:**
```json
{
    "source_entity_id": 456,
    "target_entity_id": 123,
    "reason": "Same company, different name variants"
}
```

**Response:**
```json
{
    "success": true,
    "merged_entity_id": 123,
    "aliases_transferred": 3,
    "merge_history_id": 789
}
```

### 4. Get Potential Duplicates
`GET /api/v1/entities/duplicates`

Find potential duplicate entities for review.

**Query Parameters:**
- `entity_type`: Filter by type
- `min_confidence`: Minimum match confidence (default: 0.70)
- `max_confidence`: Maximum match confidence (default: 0.90)
- `limit`: Max results (default: 50)

**Response:**
```json
{
    "duplicates": [
        {
            "entity_a": {
                "id": 100,
                "canonical_name": "Microsoft Corporation"
            },
            "entity_b": {
                "id": 200,
                "canonical_name": "Microsoft Corp"
            },
            "confidence": 0.88,
            "match_method": "fuzzy_name"
        }
    ],
    "total_found": 15
}
```

### 5. Add Manual Alias
`POST /api/v1/entities/{id}/aliases`

Manually add an alias to an entity.

**Request:**
```json
{
    "alias": "MSFT Corp",
    "source": "manual"
}
```

### 6. Split Entity
`POST /api/v1/entities/{id}/split`

Split an incorrectly merged entity.

**Request:**
```json
{
    "aliases_to_split": ["Apple Hospitality REIT", "Apple Hospitality"],
    "new_entity_name": "Apple Hospitality REIT Inc.",
    "reason": "Different company incorrectly merged"
}
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/core/entity_resolver.py` | Core resolution engine with matching algorithms |
| `app/api/v1/entities.py` | API endpoints |

---

## Integration Points

1. **Fuzzy Matcher**: Reuse `CompanyNameMatcher` from `app/agentic/fuzzy_matcher.py`
2. **Search Engine**: Entity resolution can feed into T12 search index
3. **Enrichment**: T22 company enrichment should use canonical IDs
4. **Portfolio Import**: T26 bulk import should resolve entities on import

---

## Implementation Notes

1. **Normalization**: Use existing `CompanyNameMatcher.normalize()` for consistent name processing
2. **Batch Processing**: Support batch resolution for bulk operations
3. **Caching**: Cache recent resolutions in memory (LRU cache)
4. **Audit Trail**: Log all merge/split operations for accountability
5. **Idempotency**: Multiple resolve calls with same input should return same canonical ID

---

## Example Usage

```python
from app.core.entity_resolver import EntityResolver

resolver = EntityResolver(db_session)

# Resolve a company
result = resolver.resolve(
    name="Apple, Inc.",
    entity_type="company",
    ticker="AAPL"
)
print(result.canonical_id)  # 123
print(result.confidence)     # 1.0 (exact ticker match)

# Find duplicates for review
duplicates = resolver.find_duplicates(
    entity_type="investor",
    min_confidence=0.75,
    max_confidence=0.90
)
for dup in duplicates:
    print(f"{dup.entity_a.name} <-> {dup.entity_b.name}: {dup.confidence}")
```

---

## Test Plan

1. **Unit Tests**
   - Name normalization edge cases
   - Confidence scoring for different match types
   - Merge/split operations

2. **Integration Tests**
   - Resolve existing companies from `portfolio_companies`
   - Resolve investors from `lp_fund` and `family_offices`
   - Verify aliases are correctly tracked

3. **Manual Testing**
   ```bash
   # Resolve a company
   curl "http://localhost:8001/api/v1/entities/resolve?name=Apple%20Inc&entity_type=company"

   # Get duplicates
   curl "http://localhost:8001/api/v1/entities/duplicates?entity_type=company&limit=10"

   # Merge entities
   curl -X POST http://localhost:8001/api/v1/entities/merge \
     -H "Content-Type: application/json" \
     -d '{"source_entity_id": 456, "target_entity_id": 123, "reason": "Same company"}'
   ```

---

## Success Criteria

- [ ] Resolve endpoint returns consistent canonical IDs for name variants
- [ ] Duplicates endpoint surfaces potential matches for review
- [ ] Merge operation transfers all aliases and records history
- [ ] Split operation creates new entity with selected aliases
- [ ] All operations logged in merge_history table
