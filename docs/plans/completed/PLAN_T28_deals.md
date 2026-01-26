# Plan T28: Deal Flow Tracker

## Overview
**Task:** T28
**Tab:** 2
**Feature:** Track potential investment opportunities through a pipeline
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

Investment teams need to track deal flow:

1. **Pipeline Management**: Track deals from sourcing to close
2. **Stage Tracking**: Know where each deal is in the process
3. **Activity Logging**: Record meetings, notes, documents
4. **Prioritization**: Score and rank opportunities
5. **Collaboration**: Assign deals, share notes

### User Scenarios

#### Scenario 1: New Opportunity
**Deal Sourcer** finds a promising company.
- Action: Create deal with company info, source, initial notes
- Result: Deal enters pipeline at "sourced" stage

#### Scenario 2: Pipeline Review
**Investment Committee** reviews weekly pipeline.
- Query: "Show me all deals in due diligence"
- Result: Filtered list with scores and recent activity

#### Scenario 3: Deal Progress
**Deal Lead** moves deal through stages.
- Action: Update deal stage, log meeting notes
- Result: Deal history shows progression and activities

---

## Success Criteria

### Must Have

| ID | Criteria | Verification |
|----|----------|--------------|
| M1 | Create/update deals | CRUD operations work |
| M2 | Pipeline stages | Deals have stages, can be updated |
| M3 | Activity logging | Notes/meetings can be added |
| M4 | Pipeline summary | Aggregated view of deal counts |

### Should Have

| ID | Criteria | Verification |
|----|----------|--------------|
| S1 | Deal scoring | Priority/fit scores |
| S2 | Filtering | By stage, sector, assignee |
| S3 | Deal history | Activity timeline |

---

## Technical Design

### Database Schema

```sql
-- Deals table
CREATE TABLE IF NOT EXISTS deals (
    id SERIAL PRIMARY KEY,

    -- Company info
    company_name VARCHAR(255) NOT NULL,
    company_sector VARCHAR(100),
    company_stage VARCHAR(50),  -- seed, series_a, etc.
    company_location VARCHAR(100),
    company_website TEXT,

    -- Deal info
    deal_type VARCHAR(50),  -- primary, secondary, co-invest
    deal_size_millions FLOAT,
    valuation_millions FLOAT,

    -- Pipeline
    pipeline_stage VARCHAR(50) DEFAULT 'sourced',  -- sourced, reviewing, due_diligence, negotiation, closed_won, closed_lost, passed
    priority INTEGER DEFAULT 3,  -- 1=highest, 5=lowest
    fit_score FLOAT,  -- 0-100

    -- Source
    source VARCHAR(100),  -- referral, inbound, conference, etc.
    source_contact VARCHAR(255),

    -- Assignment
    assigned_to VARCHAR(255),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMP,

    -- Tags
    tags TEXT[]
);

-- Deal activities
CREATE TABLE IF NOT EXISTS deal_activities (
    id SERIAL PRIMARY KEY,
    deal_id INTEGER REFERENCES deals(id) ON DELETE CASCADE,

    activity_type VARCHAR(50) NOT NULL,  -- note, meeting, call, email, document
    title VARCHAR(255),
    description TEXT,

    -- Meeting specific
    meeting_date TIMESTAMP,
    attendees TEXT[],

    -- User
    created_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_deals_stage ON deals(pipeline_stage);
CREATE INDEX idx_deals_sector ON deals(company_sector);
CREATE INDEX idx_deals_priority ON deals(priority);
CREATE INDEX idx_activities_deal ON deal_activities(deal_id);
```

### Pipeline Stages

| Stage | Description |
|-------|-------------|
| `sourced` | Initial opportunity identified |
| `reviewing` | Initial review/screening |
| `due_diligence` | Active due diligence |
| `negotiation` | Terms negotiation |
| `closed_won` | Deal closed, investment made |
| `closed_lost` | Deal closed, did not invest |
| `passed` | Decided not to pursue |

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/deals` | Create new deal |
| GET | `/api/v1/deals` | List deals with filters |
| GET | `/api/v1/deals/{id}` | Get deal details |
| PATCH | `/api/v1/deals/{id}` | Update deal |
| DELETE | `/api/v1/deals/{id}` | Delete deal |
| POST | `/api/v1/deals/{id}/activities` | Add activity |
| GET | `/api/v1/deals/{id}/activities` | Get deal activities |
| GET | `/api/v1/deals/pipeline` | Pipeline summary |

### Request/Response Models

**Create Deal:**
```json
{
  "company_name": "TechStartup Inc",
  "company_sector": "AI/ML",
  "company_stage": "series_a",
  "deal_type": "primary",
  "deal_size_millions": 5.0,
  "source": "referral",
  "source_contact": "John Smith",
  "priority": 2,
  "tags": ["ai", "b2b", "saas"]
}
```

**Pipeline Summary:**
```json
{
  "total_deals": 45,
  "by_stage": {
    "sourced": 15,
    "reviewing": 12,
    "due_diligence": 8,
    "negotiation": 3,
    "closed_won": 5,
    "closed_lost": 2
  },
  "by_priority": {
    "1": 5,
    "2": 12,
    "3": 20,
    "4": 5,
    "5": 3
  },
  "recent_activity": 12
}
```

### Deal Tracker Service

```python
class DealTracker:
    """Deal flow tracking service."""

    def __init__(self, db: Session):
        self.db = db

    def create_deal(self, data: dict) -> dict:
        """Create a new deal."""

    def get_deal(self, deal_id: int) -> dict:
        """Get deal with activities."""

    def update_deal(self, deal_id: int, updates: dict) -> dict:
        """Update deal fields."""

    def list_deals(self, filters: DealFilters) -> dict:
        """List deals with filters and pagination."""

    def add_activity(self, deal_id: int, activity: dict) -> dict:
        """Add activity to deal."""

    def get_pipeline_summary(self) -> dict:
        """Get pipeline stage summary."""
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/deals/__init__.py` | Package init |
| `app/deals/tracker.py` | DealTracker service |
| `app/api/v1/deals.py` | 8 API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register deals router |

---

## Implementation Steps

1. Create `app/deals/` package
2. Create DealTracker service with database operations
3. Implement deal CRUD operations
4. Implement activity logging
5. Implement pipeline summary
6. Create API endpoints
7. Register router in main.py
8. Test all endpoints

---

## Test Plan

| Test ID | Test | Expected |
|---------|------|----------|
| DEAL-001 | Create deal | Deal created with ID |
| DEAL-002 | List deals | Returns paginated list |
| DEAL-003 | Filter by stage | Only matching stage |
| DEAL-004 | Update stage | Stage changes, updated_at set |
| DEAL-005 | Add activity | Activity linked to deal |
| DEAL-006 | Pipeline summary | Counts by stage |

### Test Commands

```bash
# Create deal
curl -s -X POST "http://localhost:8001/api/v1/deals" \
  -H "Content-Type: application/json" \
  -d '{"company_name": "TechCo", "company_sector": "AI/ML", "deal_type": "primary"}' \
  | python -m json.tool

# List deals
curl -s "http://localhost:8001/api/v1/deals?pipeline_stage=sourced" | python -m json.tool

# Update deal
curl -s -X PATCH "http://localhost:8001/api/v1/deals/1" \
  -H "Content-Type: application/json" \
  -d '{"pipeline_stage": "reviewing"}' \
  | python -m json.tool

# Add activity
curl -s -X POST "http://localhost:8001/api/v1/deals/1/activities" \
  -H "Content-Type: application/json" \
  -d '{"activity_type": "meeting", "title": "Intro call", "description": "Good initial discussion"}' \
  | python -m json.tool

# Pipeline summary
curl -s "http://localhost:8001/api/v1/deals/pipeline" | python -m json.tool
```

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Created `app/deals/` package with DealTracker service
- Tables auto-created: `deals` and `deal_activities`
- 9 API endpoints (including /stages helper)
- Pipeline stages with closed_at timestamp handling
- Activity logging for meetings, notes, calls, emails, documents
- Filtering by stage, sector, assignee, priority
- Priority-based ordering (1=highest first)

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
