# PLAN T20: Saved Searches & Watchlists

## Overview
**Task:** T20
**Tab:** 2
**Feature:** Saved Searches & Watchlists - Let users save searches and track specific investors/companies
**Status:** COMPLETE
**Dependency:** T12 (Search) - COMPLETE

---

## Business Context

### The Problem

Users of Nexdata repeatedly perform the same searches and track the same entities:

1. **Repetitive Searches**: An analyst researching "healthcare pension funds in California" has to re-type this search every session. There's no way to save frequently-used queries.

2. **No Tracking**: Users interested in specific investors (e.g., "CalPERS", "STRS Ohio") or companies (e.g., "NVIDIA", "Microsoft") have no way to create a personal list to monitor.

3. **No Organization**: Power users researching multiple sectors or regions can't organize their research into logical groups (e.g., "Tech Companies to Watch", "Peer Pension Funds").

4. **Lost Context**: When a user returns to the platform, they lose all context from previous sessions. They must remember what they were researching.

### Real-World User Scenarios

#### Scenario 1: Investment Analyst (Daily User)
**Sarah** is an analyst at a pension fund. Every morning she:
- Searches for "public pension funds" to see peer activity
- Checks on 5 specific competitors (CalPERS, STRS Ohio, OMERS, etc.)
- Monitors 10 portfolio companies her fund is considering

**Pain Point**: She spends 15 minutes every day re-running searches and navigating to the same entities.

**With Watchlists**: She has a "Peer Funds" watchlist and a "Target Companies" watchlist. One click shows all her tracked items.

#### Scenario 2: BD/Sales Rep (Weekly User)
**Mike** is a BD rep selling data services. He tracks:
- Prospects by sector (healthcare investors, tech-focused funds)
- Recent meetings (funds he's pitched to)
- Competitive intelligence (what data competitors are tracking)

**Pain Point**: He maintains spreadsheets outside the platform to track his targets.

**With Saved Searches**: He saves "healthcare investors California" and "tech-focused sovereign wealth" as named searches. Returns to them weekly.

#### Scenario 3: Research Team (Collaborative)
**The Research Team** at an asset manager wants to:
- Share research lists across team members
- Create curated lists for specific projects
- Collaborate on due diligence targets

**Pain Point**: No way to share research across team members.

**With Shared Watchlists**: Team creates "Q1 Due Diligence Targets" watchlist, shares with team, everyone can add/remove items.

### Business Value

| Value | Description | Metric |
|-------|-------------|--------|
| **Time Savings** | Reduce repetitive search time | 15 min/day → 2 min/day |
| **User Retention** | Users return because their context is saved | +30% return visits |
| **Engagement** | Users invest in organizing their research | +50% session time |
| **Collaboration** | Teams share research, increasing platform value | +25% team adoption |
| **Stickiness** | Users build valuable lists they don't want to lose | -40% churn |

### Integration with Existing Features

| Feature | Integration |
|---------|-------------|
| **T12: Search** | Saved searches execute via search engine |
| **T11: Alerts** | Future: Alert when watchlist item changes |
| **T18: Recommendations** | Future: "Add similar investors to watchlist" |

---

## Success Criteria

### Must Have (Launch Blockers)

| ID | Criteria | Metric | Target | Verification Method |
|----|----------|--------|--------|---------------------|
| M1 | **Create watchlist** | API returns 201 | New watchlist with ID | `POST /watchlists` returns `{id: X}` |
| M2 | **Add items to watchlist** | Items persist | Item appears in list | `GET /watchlists/{id}` includes item |
| M3 | **Remove items** | Item deleted | Item no longer in list | `DELETE` then `GET` confirms removal |
| M4 | **List user's watchlists** | Returns all lists | All user's lists returned | `GET /watchlists?user_id=X` correct count |
| M5 | **Save search query** | Query persists | Can re-execute search | `GET /searches/saved/{id}/execute` returns results |
| M6 | **Delete watchlist** | Cascade delete | List and items removed | `DELETE /watchlists/{id}` removes all |
| M7 | **Item types supported** | investor, company | Both types work | Can add both to same list |
| M8 | **User isolation** | Users see only their data | No cross-user leakage | User A can't see User B's lists |

### Should Have (Quality Bar)

| ID | Criteria | Metric | Target | Verification Method |
|----|----------|--------|--------|---------------------|
| S1 | **Watchlist metadata** | Name, description | Editable after creation | `PATCH /watchlists/{id}` updates fields |
| S2 | **Item count** | Show count on list | Accurate count | `item_count` matches actual items |
| S3 | **Pagination** | Large lists paginated | 100+ items handled | `?page=2&page_size=50` works |
| S4 | **Search saved searches** | Find by name | Name search works | `GET /searches/saved?name=healthcare` |
| S5 | **Duplicate prevention** | No duplicate items | Same item can't be added twice | 409 on duplicate add |
| S6 | **Timestamps** | created_at, updated_at | Accurate times | Timestamps update on changes |
| S7 | **Performance** | Response time | < 200ms for list operations | Measured in tests |

### Nice to Have (Future)

| ID | Criteria | Description |
|----|----------|-------------|
| N1 | **Sharing** | Public/private visibility, share with specific users |
| N2 | **Watchlist folders** | Organize watchlists into folders |
| N3 | **Bulk operations** | Add/remove multiple items at once |
| N4 | **Export** | Export watchlist to CSV |
| N5 | **Change tracking** | "Last changed" indicator on items |
| N6 | **Notes on items** | Add notes/tags to watchlist items |

---

## User Stories & Acceptance Criteria

### Story 1: Create and Manage Watchlists

**As a** user, **I want to** create named watchlists **so that** I can organize entities I'm tracking.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 1.1 | Create watchlist | User is authenticated | POST `/watchlists` with `{name: "My List", user_id: "user@example.com"}` | Returns 201 with `{id, name, user_id, created_at}` |
| 1.2 | Create with description | User wants to describe list | POST with `{name, description}` | Description saved and returned |
| 1.3 | List my watchlists | User has 3 watchlists | GET `/watchlists?user_id=user@example.com` | Returns array of 3 watchlists |
| 1.4 | Empty list | User has no watchlists | GET `/watchlists?user_id=new@example.com` | Returns empty array `[]` |
| 1.5 | Update watchlist | User wants to rename | PATCH `/watchlists/{id}` with `{name: "New Name"}` | Name updated, updated_at changed |
| 1.6 | Delete watchlist | User wants to remove list | DELETE `/watchlists/{id}` | Returns 204, list no longer exists |
| 1.7 | Delete non-existent | User deletes already-deleted list | DELETE `/watchlists/9999` | Returns 404 |
| 1.8 | Name required | User omits name | POST `/watchlists` without name | Returns 422 validation error |
| 1.9 | User isolation | User A lists watchlists | GET `/watchlists?user_id=userA` | Does NOT include User B's lists |

**Test Commands:**
```bash
# AC 1.1: Create watchlist
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "Tech Companies", "user_id": "analyst@example.com"}'
# Expected: 201, {"id": 1, "name": "Tech Companies", ...}

# AC 1.3: List watchlists
curl "http://localhost:8001/api/v1/watchlists?user_id=analyst@example.com"
# Expected: 200, [{"id": 1, "name": "Tech Companies", ...}]

# AC 1.5: Update watchlist
curl -X PATCH "http://localhost:8001/api/v1/watchlists/1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Technology Companies"}'
# Expected: 200, name updated

# AC 1.6: Delete watchlist
curl -X DELETE "http://localhost:8001/api/v1/watchlists/1"
# Expected: 204 No Content
```

---

### Story 2: Add and Remove Watchlist Items

**As a** user, **I want to** add investors and companies to my watchlists **so that** I can track them.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 2.1 | Add investor | Watchlist exists | POST `/watchlists/{id}/items` with `{entity_type: "investor", entity_id: 1}` | Returns 201, item added |
| 2.2 | Add company | Watchlist exists | POST with `{entity_type: "company", entity_id: 100}` | Returns 201, item added |
| 2.3 | Add with note | User wants to annotate | POST with `{..., note: "Key competitor"}` | Note saved with item |
| 2.4 | List items | Watchlist has 5 items | GET `/watchlists/{id}/items` | Returns 5 items with details |
| 2.5 | Item details | Items in list | GET items response | Each item has `entity_name`, `entity_type`, `added_at` |
| 2.6 | Remove item | Item exists in list | DELETE `/watchlists/{id}/items/{item_id}` | Returns 204, item removed |
| 2.7 | Remove by entity | User knows entity, not item ID | DELETE `/watchlists/{id}/items?entity_type=investor&entity_id=1` | Item removed |
| 2.8 | Duplicate prevention | Item already in list | POST same item again | Returns 409 Conflict |
| 2.9 | Invalid entity type | User sends bad type | POST with `{entity_type: "invalid"}` | Returns 422 validation error |
| 2.10 | Non-existent entity | Entity doesn't exist | POST with `{entity_id: 99999}` | Returns 404 or adds anyway (configurable) |
| 2.11 | Wrong watchlist owner | User A tries to add to User B's list | POST to User B's watchlist | Returns 403 Forbidden |
| 2.12 | Pagination | Watchlist has 150 items | GET `/watchlists/{id}/items?page=2&page_size=50` | Returns items 51-100 |

**Test Commands:**
```bash
# AC 2.1: Add investor to watchlist
curl -X POST "http://localhost:8001/api/v1/watchlists/1/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 1}'
# Expected: 201, {"id": 1, "entity_type": "investor", "entity_id": 1, "entity_name": "CalPERS", ...}

# AC 2.2: Add company
curl -X POST "http://localhost:8001/api/v1/watchlists/1/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "company", "entity_id": 100, "note": "Potential investment"}'
# Expected: 201

# AC 2.4: List items
curl "http://localhost:8001/api/v1/watchlists/1/items"
# Expected: 200, [{"id": 1, "entity_type": "investor", "entity_name": "CalPERS", ...}, ...]

# AC 2.6: Remove item
curl -X DELETE "http://localhost:8001/api/v1/watchlists/1/items/1"
# Expected: 204 No Content

# AC 2.8: Duplicate prevention
curl -X POST "http://localhost:8001/api/v1/watchlists/1/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 1}'
# First: 201
# Second: 409 Conflict
```

---

### Story 3: Save and Execute Searches

**As a** user, **I want to** save search queries **so that** I can re-run them without retyping.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 3.1 | Save search | User has search params | POST `/searches/saved` with `{name, query, filters, user_id}` | Returns 201 with saved search |
| 3.2 | Save with all filters | Complex search | POST with `{query: "tech", types: ["company"], industry: "technology"}` | All filters saved |
| 3.3 | List saved searches | User has 3 saved searches | GET `/searches/saved?user_id=X` | Returns 3 searches |
| 3.4 | Execute saved search | Search exists | GET `/searches/saved/{id}/execute` | Returns search results via T12 |
| 3.5 | Execute returns fresh data | Data changed since save | Execute search | Returns current data, not cached |
| 3.6 | Update saved search | User modifies query | PATCH `/searches/saved/{id}` | Query updated |
| 3.7 | Delete saved search | User removes search | DELETE `/searches/saved/{id}` | Returns 204 |
| 3.8 | Duplicate name allowed | User saves two with same name | POST twice with same name | Both saved (different IDs) |
| 3.9 | Search saved searches | User has many saved | GET `/searches/saved?user_id=X&name=healthcare` | Filters by name substring |
| 3.10 | Last executed tracking | User runs search | Execute search | `last_executed_at` updated |
| 3.11 | Execution count | User runs search 5 times | Check search metadata | `execution_count: 5` |

**Test Commands:**
```bash
# AC 3.1: Save search
curl -X POST "http://localhost:8001/api/v1/searches/saved" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Healthcare Pension Funds",
    "user_id": "analyst@example.com",
    "query": "healthcare",
    "filters": {
      "types": ["investor"],
      "investor_type": "public_pension"
    }
  }'
# Expected: 201, {"id": 1, "name": "Healthcare Pension Funds", ...}

# AC 3.3: List saved searches
curl "http://localhost:8001/api/v1/searches/saved?user_id=analyst@example.com"
# Expected: 200, [{"id": 1, "name": "Healthcare Pension Funds", ...}]

# AC 3.4: Execute saved search
curl "http://localhost:8001/api/v1/searches/saved/1/execute"
# Expected: 200, search results from T12 search engine

# AC 3.6: Update saved search
curl -X PATCH "http://localhost:8001/api/v1/searches/saved/1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Healthcare Funds - Updated"}'
# Expected: 200

# AC 3.7: Delete saved search
curl -X DELETE "http://localhost:8001/api/v1/searches/saved/1"
# Expected: 204 No Content
```

---

### Story 4: View Watchlist Item Details

**As a** user, **I want to** see full details of items in my watchlist **so that** I have context without clicking through.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 4.1 | Investor details | Watchlist has investor | GET watchlist items | Returns investor name, type, location |
| 4.2 | Company details | Watchlist has company | GET watchlist items | Returns company name, industry |
| 4.3 | Added date | Items in list | GET watchlist items | Each item has `added_at` timestamp |
| 4.4 | User note | Item has note | GET watchlist items | Note returned with item |
| 4.5 | Entity link | Item in list | GET watchlist items | Returns `entity_id` for navigation |

**Test Commands:**
```bash
# AC 4.1-4.5: Get items with full details
curl "http://localhost:8001/api/v1/watchlists/1/items"
# Expected: 200, [
#   {
#     "id": 1,
#     "entity_type": "investor",
#     "entity_id": 1,
#     "entity_name": "CalPERS",
#     "entity_details": {
#       "investor_type": "public_pension",
#       "location": "CA"
#     },
#     "note": "Key competitor",
#     "added_at": "2026-01-15T20:00:00Z"
#   }
# ]
```

---

## Technical Scope

### Database Schema

```sql
-- Watchlists table
CREATE TABLE watchlists (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,  -- Email or user identifier
    name VARCHAR(255) NOT NULL,
    description TEXT,
    is_public BOOLEAN DEFAULT FALSE,  -- For future sharing
    item_count INTEGER DEFAULT 0,  -- Denormalized for performance
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_watchlists_user ON watchlists(user_id);
CREATE INDEX idx_watchlists_public ON watchlists(is_public) WHERE is_public = TRUE;

-- Watchlist items table
CREATE TABLE watchlist_items (
    id SERIAL PRIMARY KEY,
    watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
    entity_type VARCHAR(50) NOT NULL,  -- 'investor', 'company'
    entity_id INTEGER NOT NULL,
    note TEXT,  -- User's annotation
    added_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(watchlist_id, entity_type, entity_id)  -- Prevent duplicates
);

CREATE INDEX idx_watchlist_items_watchlist ON watchlist_items(watchlist_id);
CREATE INDEX idx_watchlist_items_entity ON watchlist_items(entity_type, entity_id);

-- Saved searches table
CREATE TABLE saved_searches (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    query TEXT,  -- Search query string
    filters JSONB DEFAULT '{}',  -- {types: [], industry: "", investor_type: "", location: ""}
    execution_count INTEGER DEFAULT 0,
    last_executed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_saved_searches_user ON saved_searches(user_id);
CREATE INDEX idx_saved_searches_name ON saved_searches(user_id, name);

-- Trigger to update item_count on watchlists
CREATE OR REPLACE FUNCTION update_watchlist_item_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE watchlists SET item_count = item_count + 1, updated_at = NOW()
        WHERE id = NEW.watchlist_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE watchlists SET item_count = item_count - 1, updated_at = NOW()
        WHERE id = OLD.watchlist_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_watchlist_item_count
AFTER INSERT OR DELETE ON watchlist_items
FOR EACH ROW EXECUTE FUNCTION update_watchlist_item_count();
```

### API Endpoints

| Method | Endpoint | Description | Request Body | Response |
|--------|----------|-------------|--------------|----------|
| POST | `/api/v1/watchlists` | Create watchlist | `{name, user_id, description?}` | `{id, name, ...}` |
| GET | `/api/v1/watchlists` | List user's watchlists | Query: `user_id` | `[{id, name, item_count, ...}]` |
| GET | `/api/v1/watchlists/{id}` | Get watchlist details | - | `{id, name, description, ...}` |
| PATCH | `/api/v1/watchlists/{id}` | Update watchlist | `{name?, description?}` | `{id, name, ...}` |
| DELETE | `/api/v1/watchlists/{id}` | Delete watchlist | - | 204 No Content |
| POST | `/api/v1/watchlists/{id}/items` | Add item to watchlist | `{entity_type, entity_id, note?}` | `{id, entity_name, ...}` |
| GET | `/api/v1/watchlists/{id}/items` | List watchlist items | Query: `page, page_size` | `[{id, entity_type, entity_name, ...}]` |
| DELETE | `/api/v1/watchlists/{id}/items/{item_id}` | Remove item | - | 204 No Content |
| POST | `/api/v1/searches/saved` | Save search | `{name, user_id, query, filters}` | `{id, name, ...}` |
| GET | `/api/v1/searches/saved` | List saved searches | Query: `user_id, name?` | `[{id, name, query, ...}]` |
| GET | `/api/v1/searches/saved/{id}` | Get saved search | - | `{id, name, query, filters, ...}` |
| GET | `/api/v1/searches/saved/{id}/execute` | Execute saved search | Query: `page?, page_size?` | Search results from T12 |
| PATCH | `/api/v1/searches/saved/{id}` | Update saved search | `{name?, query?, filters?}` | `{id, name, ...}` |
| DELETE | `/api/v1/searches/saved/{id}` | Delete saved search | - | 204 No Content |

### Data Models (Pydantic)

```python
# Request Models
class WatchlistCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    user_id: str = Field(..., description="User identifier (email)")
    description: Optional[str] = None

class WatchlistUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None

class WatchlistItemCreate(BaseModel):
    entity_type: str = Field(..., pattern="^(investor|company)$")
    entity_id: int = Field(..., gt=0)
    note: Optional[str] = None

class SavedSearchCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    user_id: str
    query: Optional[str] = ""
    filters: Optional[dict] = Field(default_factory=dict)

# Response Models
class WatchlistResponse(BaseModel):
    id: int
    user_id: str
    name: str
    description: Optional[str]
    item_count: int
    is_public: bool
    created_at: datetime
    updated_at: datetime

class WatchlistItemResponse(BaseModel):
    id: int
    entity_type: str
    entity_id: int
    entity_name: str
    entity_details: dict  # Type-specific fields
    note: Optional[str]
    added_at: datetime

class SavedSearchResponse(BaseModel):
    id: int
    user_id: str
    name: str
    query: Optional[str]
    filters: dict
    execution_count: int
    last_executed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/users/__init__.py` | Package init |
| `app/users/watchlists.py` | Watchlist service and data access |
| `app/api/v1/watchlists.py` | API endpoints for watchlists and saved searches |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register watchlists router |

---

## Implementation Steps

1. Create `app/users/` directory with `__init__.py`
2. Implement `watchlists.py`:
   - SQLAlchemy models (or raw SQL)
   - Table creation (idempotent)
   - WatchlistService class with CRUD operations
   - Entity name resolution (lookup investor/company names)
3. Implement `app/api/v1/watchlists.py`:
   - All watchlist endpoints
   - All saved search endpoints
   - Request validation
   - Error handling
4. Register router in main.py
5. Test all acceptance criteria

---

## Testing Plan

### Test Environment Setup

```bash
# Ensure Docker is running
docker-compose up -d

# Verify API is healthy
curl http://localhost:8001/

# Ensure search index is populated (T12 dependency)
curl http://localhost:8001/api/v1/search/stats
# Should show: {"total_indexed": 4034, ...}
```

### Test Data Setup

Before running tests, we need:
- At least 5 investors in database (CalPERS id=1, STRS Ohio id=4, etc.)
- At least 10 companies in database
- Search index populated

### Test Suite

#### Suite 1: Watchlist CRUD Operations

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| WL-001 | Create watchlist | POST `/watchlists` with valid data | 201, returns watchlist with ID | |
| WL-002 | Create without name | POST `/watchlists` without name | 422 validation error | |
| WL-003 | Create without user_id | POST `/watchlists` without user_id | 422 validation error | |
| WL-004 | List empty | GET `/watchlists?user_id=new@test.com` | 200, empty array | |
| WL-005 | List with data | Create 3, then GET | 200, array of 3 | |
| WL-006 | Get by ID | GET `/watchlists/1` | 200, watchlist details | |
| WL-007 | Get non-existent | GET `/watchlists/9999` | 404 not found | |
| WL-008 | Update name | PATCH `/watchlists/1` with new name | 200, name updated | |
| WL-009 | Update description | PATCH with description | 200, description updated | |
| WL-010 | Delete watchlist | DELETE `/watchlists/1` | 204 no content | |
| WL-011 | Delete non-existent | DELETE `/watchlists/9999` | 404 not found | |
| WL-012 | Delete cascades items | Delete watchlist with items | All items also deleted | |

**Test Commands (WL-001 to WL-012):**
```bash
# WL-001: Create watchlist
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "Test Watchlist", "user_id": "test@example.com", "description": "For testing"}'
# Verify: Status 201, response has "id"

# WL-002: Create without name
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test@example.com"}'
# Verify: Status 422

# WL-003: Create without user_id
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "No User"}'
# Verify: Status 422

# WL-004: List empty
curl "http://localhost:8001/api/v1/watchlists?user_id=nobody@test.com"
# Verify: Status 200, response is []

# WL-005: List with data (after creating 3)
curl "http://localhost:8001/api/v1/watchlists?user_id=test@example.com"
# Verify: Status 200, array length matches created count

# WL-006: Get by ID
curl "http://localhost:8001/api/v1/watchlists/1"
# Verify: Status 200, has all fields

# WL-007: Get non-existent
curl "http://localhost:8001/api/v1/watchlists/9999"
# Verify: Status 404

# WL-008: Update name
curl -X PATCH "http://localhost:8001/api/v1/watchlists/1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Updated Name"}'
# Verify: Status 200, name changed

# WL-009: Update description
curl -X PATCH "http://localhost:8001/api/v1/watchlists/1" \
  -H "Content-Type: application/json" \
  -d '{"description": "New description"}'
# Verify: Status 200, description changed

# WL-010: Delete watchlist
curl -X DELETE "http://localhost:8001/api/v1/watchlists/1"
# Verify: Status 204

# WL-011: Delete non-existent
curl -X DELETE "http://localhost:8001/api/v1/watchlists/9999"
# Verify: Status 404

# WL-012: Delete cascades (create watchlist, add items, delete, verify items gone)
# See integration test below
```

#### Suite 2: Watchlist Items Operations

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| WI-001 | Add investor | POST item with entity_type=investor | 201, item with entity_name | |
| WI-002 | Add company | POST item with entity_type=company | 201, item with entity_name | |
| WI-003 | Add with note | POST item with note | 201, note saved | |
| WI-004 | Add invalid type | POST with entity_type=invalid | 422 validation error | |
| WI-005 | Add duplicate | POST same item twice | 409 conflict on second | |
| WI-006 | List items | GET `/watchlists/{id}/items` | 200, array of items | |
| WI-007 | List empty | GET items for empty watchlist | 200, empty array | |
| WI-008 | Item has details | GET items | Each has entity_name, entity_details | |
| WI-009 | Remove item by ID | DELETE `/watchlists/{id}/items/{item_id}` | 204 no content | |
| WI-010 | Remove non-existent | DELETE item that doesn't exist | 404 not found | |
| WI-011 | Pagination | Add 25 items, GET with page_size=10 | Returns 10, has pagination info | |
| WI-012 | Item count updates | Add 3 items, check watchlist | item_count = 3 | |

**Test Commands (WI-001 to WI-012):**
```bash
# Setup: Create a watchlist first
WATCHLIST_ID=$(curl -s -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "Items Test", "user_id": "items@test.com"}' | python -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Created watchlist: $WATCHLIST_ID"

# WI-001: Add investor
curl -X POST "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 1}'
# Verify: Status 201, entity_name is "CalPERS"

# WI-002: Add company (find a company ID first)
curl -X POST "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "company", "entity_id": 100}'
# Verify: Status 201

# WI-003: Add with note
curl -X POST "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 4, "note": "Key competitor"}'
# Verify: Status 201, note in response

# WI-004: Add invalid type
curl -X POST "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "invalid", "entity_id": 1}'
# Verify: Status 422

# WI-005: Add duplicate
curl -X POST "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 1}'
# Verify: Status 409 (already added in WI-001)

# WI-006: List items
curl "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items"
# Verify: Status 200, array with 3 items

# WI-008: Check item has details
curl "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" | python -m json.tool
# Verify: Each item has entity_name, entity_details, added_at

# WI-009: Remove item (get item ID first)
ITEM_ID=$(curl -s "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items" | python -c "import sys,json; print(json.load(sys.stdin)['items'][0]['id'])")
curl -X DELETE "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID/items/$ITEM_ID"
# Verify: Status 204

# WI-012: Check item_count
curl "http://localhost:8001/api/v1/watchlists/$WATCHLIST_ID"
# Verify: item_count matches actual items
```

#### Suite 3: Saved Searches Operations

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| SS-001 | Save search | POST `/searches/saved` | 201, search saved | |
| SS-002 | Save with filters | POST with types, industry filters | 201, filters saved | |
| SS-003 | Save without query | POST with empty query | 201, allowed (browse all) | |
| SS-004 | List saved | GET `/searches/saved?user_id=X` | 200, array of searches | |
| SS-005 | Get by ID | GET `/searches/saved/1` | 200, search details | |
| SS-006 | Execute search | GET `/searches/saved/1/execute` | 200, search results from T12 | |
| SS-007 | Execute updates count | Execute twice | execution_count = 2 | |
| SS-008 | Execute updates timestamp | Execute search | last_executed_at updated | |
| SS-009 | Update search | PATCH `/searches/saved/1` | 200, updated | |
| SS-010 | Delete search | DELETE `/searches/saved/1` | 204 no content | |
| SS-011 | Filter by name | GET `?user_id=X&name=health` | Only matching searches | |
| SS-012 | Execute with pagination | GET `/execute?page=2&page_size=10` | Paginated results | |

**Test Commands (SS-001 to SS-012):**
```bash
# SS-001: Save search
curl -X POST "http://localhost:8001/api/v1/searches/saved" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Healthcare Investors",
    "user_id": "analyst@test.com",
    "query": "healthcare",
    "filters": {"types": ["investor"]}
  }'
# Verify: Status 201, has id

# SS-002: Save with all filters
curl -X POST "http://localhost:8001/api/v1/searches/saved" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Tech Companies California",
    "user_id": "analyst@test.com",
    "query": "technology",
    "filters": {
      "types": ["company"],
      "industry": "technology",
      "location": "CA"
    }
  }'
# Verify: Status 201, filters saved correctly

# SS-003: Save without query (browse)
curl -X POST "http://localhost:8001/api/v1/searches/saved" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "All Investors",
    "user_id": "analyst@test.com",
    "query": "",
    "filters": {"types": ["investor"]}
  }'
# Verify: Status 201

# SS-004: List saved searches
curl "http://localhost:8001/api/v1/searches/saved?user_id=analyst@test.com"
# Verify: Status 200, array with 3 searches

# SS-005: Get by ID
curl "http://localhost:8001/api/v1/searches/saved/1"
# Verify: Status 200, has name, query, filters

# SS-006: Execute search
curl "http://localhost:8001/api/v1/searches/saved/1/execute"
# Verify: Status 200, returns search results (like T12 /search endpoint)

# SS-007 & SS-008: Execute and check metadata
curl "http://localhost:8001/api/v1/searches/saved/1/execute" > /dev/null
curl "http://localhost:8001/api/v1/searches/saved/1/execute" > /dev/null
curl "http://localhost:8001/api/v1/searches/saved/1"
# Verify: execution_count >= 2, last_executed_at is recent

# SS-009: Update search
curl -X PATCH "http://localhost:8001/api/v1/searches/saved/1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Healthcare Investors - Updated"}'
# Verify: Status 200, name changed

# SS-010: Delete search
curl -X DELETE "http://localhost:8001/api/v1/searches/saved/1"
# Verify: Status 204

# SS-011: Filter by name
curl "http://localhost:8001/api/v1/searches/saved?user_id=analyst@test.com&name=Tech"
# Verify: Only searches with "Tech" in name
```

#### Suite 4: Security & Edge Cases

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| SEC-001 | User isolation - watchlists | User A creates, User B lists | User B sees empty | |
| SEC-002 | User isolation - items | User A adds item, User B GETs | User B gets 403 or 404 | |
| SEC-003 | User isolation - searches | User A saves, User B lists | User B sees empty | |
| SEC-004 | SQL injection - name | Create with `'; DROP TABLE--` | Safely escaped, no error | |
| SEC-005 | XSS in note | Add item with `<script>` note | Stored as-is, no execution | |
| SEC-006 | Long name | Create with 1000-char name | 422 validation error (max 255) | |
| SEC-007 | Empty name | Create with "" name | 422 validation error | |
| SEC-008 | Negative entity_id | Add item with entity_id=-1 | 422 validation error | |
| SEC-009 | Non-integer entity_id | Add item with entity_id="abc" | 422 validation error | |

**Test Commands (SEC-001 to SEC-009):**
```bash
# SEC-001: User isolation
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "User A List", "user_id": "userA@test.com"}'
curl "http://localhost:8001/api/v1/watchlists?user_id=userB@test.com"
# Verify: User B's list is empty

# SEC-004: SQL injection
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "Test'\'' OR 1=1; DROP TABLE watchlists;--", "user_id": "hacker@test.com"}'
# Verify: No SQL error, watchlist created with weird name

# SEC-006: Long name
curl -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"$(python -c "print('a'*1000)")\", \"user_id\": \"test@test.com\"}"
# Verify: Status 422

# SEC-008: Negative entity_id
curl -X POST "http://localhost:8001/api/v1/watchlists/1/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": -1}'
# Verify: Status 422
```

#### Suite 5: Performance Tests

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| PERF-001 | Create watchlist latency | Time POST request | < 100ms | |
| PERF-002 | List 100 watchlists | Create 100, time GET | < 200ms | |
| PERF-003 | List 1000 items | Add 1000 items, time GET | < 500ms | |
| PERF-004 | Execute saved search | Time execute | < 300ms (T12 + overhead) | |
| PERF-005 | Concurrent creates | 10 parallel POSTs | All succeed, < 500ms total | |

**Test Commands (PERF-001 to PERF-005):**
```bash
# PERF-001: Create latency
curl -w "\nTime: %{time_total}s\n" -X POST "http://localhost:8001/api/v1/watchlists" \
  -H "Content-Type: application/json" \
  -d '{"name": "Perf Test", "user_id": "perf@test.com"}'
# Verify: Time < 0.1s

# PERF-004: Execute latency
curl -w "\nTime: %{time_total}s\n" "http://localhost:8001/api/v1/searches/saved/1/execute"
# Verify: Time < 0.3s
```

---

## Integration Test Script

Complete end-to-end test script:

```bash
#!/bin/bash
# T20 Integration Test Suite
# Run: bash test_t20.sh

BASE_URL="http://localhost:8001/api/v1"
USER_ID="integration_test@example.com"
PASS=0
FAIL=0

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_pass() { echo -e "${GREEN}PASS${NC}: $1"; ((PASS++)); }
log_fail() { echo -e "${RED}FAIL${NC}: $1 - $2"; ((FAIL++)); }

echo "=========================================="
echo "T20: Saved Searches & Watchlists Tests"
echo "=========================================="

# Cleanup: Delete any existing test data
echo "Cleaning up previous test data..."

# Test 1: Create Watchlist
echo -e "\n--- Test 1: Create Watchlist ---"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/watchlists" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"Integration Test List\", \"user_id\": \"$USER_ID\"}")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
WATCHLIST_ID=$(echo "$BODY" | python -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

if [ "$HTTP_CODE" == "201" ] && [ -n "$WATCHLIST_ID" ]; then
  log_pass "Create watchlist (ID: $WATCHLIST_ID)"
else
  log_fail "Create watchlist" "HTTP $HTTP_CODE"
fi

# Test 2: Get Watchlist
echo -e "\n--- Test 2: Get Watchlist ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/watchlists/$WATCHLIST_ID")
if [ "$HTTP_CODE" == "200" ]; then
  log_pass "Get watchlist"
else
  log_fail "Get watchlist" "HTTP $HTTP_CODE"
fi

# Test 3: Add Investor to Watchlist
echo -e "\n--- Test 3: Add Investor ---"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 1}')
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
ITEM_ID=$(echo "$BODY" | python -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

if [ "$HTTP_CODE" == "201" ]; then
  log_pass "Add investor (Item ID: $ITEM_ID)"
else
  log_fail "Add investor" "HTTP $HTTP_CODE"
fi

# Test 4: Add Duplicate (should fail)
echo -e "\n--- Test 4: Duplicate Prevention ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/watchlists/$WATCHLIST_ID/items" \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "investor", "entity_id": 1}')
if [ "$HTTP_CODE" == "409" ]; then
  log_pass "Duplicate prevention"
else
  log_fail "Duplicate prevention" "Expected 409, got $HTTP_CODE"
fi

# Test 5: List Items
echo -e "\n--- Test 5: List Items ---"
RESPONSE=$(curl -s "$BASE_URL/watchlists/$WATCHLIST_ID/items")
ITEM_COUNT=$(echo "$RESPONSE" | python -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('items',d)))" 2>/dev/null)
if [ "$ITEM_COUNT" -ge "1" ]; then
  log_pass "List items (count: $ITEM_COUNT)"
else
  log_fail "List items" "Expected at least 1 item"
fi

# Test 6: Remove Item
echo -e "\n--- Test 6: Remove Item ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE_URL/watchlists/$WATCHLIST_ID/items/$ITEM_ID")
if [ "$HTTP_CODE" == "204" ]; then
  log_pass "Remove item"
else
  log_fail "Remove item" "HTTP $HTTP_CODE"
fi

# Test 7: Save Search
echo -e "\n--- Test 7: Save Search ---"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/searches/saved" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"Test Search\", \"user_id\": \"$USER_ID\", \"query\": \"pension\", \"filters\": {\"types\": [\"investor\"]}}")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
SEARCH_ID=$(echo "$BODY" | python -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

if [ "$HTTP_CODE" == "201" ] && [ -n "$SEARCH_ID" ]; then
  log_pass "Save search (ID: $SEARCH_ID)"
else
  log_fail "Save search" "HTTP $HTTP_CODE"
fi

# Test 8: Execute Saved Search
echo -e "\n--- Test 8: Execute Saved Search ---"
RESPONSE=$(curl -s "$BASE_URL/searches/saved/$SEARCH_ID/execute")
TOTAL=$(echo "$RESPONSE" | python -c "import sys,json; print(json.load(sys.stdin).get('total',0))" 2>/dev/null)
if [ "$TOTAL" -ge "0" ]; then
  log_pass "Execute saved search (results: $TOTAL)"
else
  log_fail "Execute saved search" "No results field"
fi

# Test 9: Delete Watchlist (cascade)
echo -e "\n--- Test 9: Delete Watchlist ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE_URL/watchlists/$WATCHLIST_ID")
if [ "$HTTP_CODE" == "204" ]; then
  log_pass "Delete watchlist"
else
  log_fail "Delete watchlist" "HTTP $HTTP_CODE"
fi

# Test 10: Delete Saved Search
echo -e "\n--- Test 10: Delete Saved Search ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE_URL/searches/saved/$SEARCH_ID")
if [ "$HTTP_CODE" == "204" ]; then
  log_pass "Delete saved search"
else
  log_fail "Delete saved search" "HTTP $HTTP_CODE"
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
| Entity lookup slow | Item add latency | Medium | Cache entity names, batch lookups |
| Large watchlists | Memory issues | Low | Pagination, limit 1000 items |
| Orphaned items | Data inconsistency | Low | ON DELETE CASCADE in schema |
| User spoofing | Security breach | Medium | Future: Add auth middleware |
| Search execution slow | Poor UX | Low | T12 is already optimized |

---

## Out of Scope (Future Work)

- Authentication/authorization middleware (T19 dependency)
- Watchlist sharing between users
- Watchlist folders/organization
- Bulk add/remove operations
- Export to CSV
- Real-time notifications when watched items change
- Watchlist templates (pre-built lists)

---

## Approval

- [x] Approved by user
