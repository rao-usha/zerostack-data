# Plan T45: Agentic Data Hunter

**Task ID:** T45
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19)

---

## Goal

Build an AI agent that autonomously finds and fills missing data in company records by scanning for gaps, prioritizing by importance, searching multiple sources, and updating with provenance tracking.

---

## Dependencies

- **T22 (Company Enrichment)**: Provides enrichment data and tables to fill
- **T37 (Entity Resolution)**: Helps match entities across sources

---

## Design

### Core Components

1. **Gap Scanner**: Identifies records with missing fields
2. **Priority Queue**: Ranks gaps by importance and fillability
3. **Source Router**: Routes data requests to appropriate sources
4. **Validator**: Cross-validates data from multiple sources
5. **Updater**: Updates records with provenance tracking

### Data Fields to Hunt

| Field | Priority | Sources |
|-------|----------|---------|
| employee_count | High | LinkedIn proxy, Glassdoor, news |
| total_funding | High | Form D, Crunchbase proxy, news |
| revenue | High | SEC filings, news estimates |
| sector/industry | Medium | Description analysis, SEC SIC |
| founding_date | Medium | Corporate registry, news |
| headquarters | Medium | SEC filings, corporate registry |
| website/domain | Low | Search, corporate registry |
| description | Low | Website scrape, news |

### Priority Scoring

```
priority = (field_importance * 0.4) +
           (record_importance * 0.3) +
           (fill_likelihood * 0.3)
```

Where:
- `field_importance`: How critical this field is (0-1)
- `record_importance`: How important this company is (based on activity, watchlists)
- `fill_likelihood`: Probability of finding data (based on company type, field)

### Source Reliability Scores

Track success rates per source per field:

| Source | employees | funding | revenue | sector |
|--------|-----------|---------|---------|--------|
| SEC EDGAR | 0.2 | 0.3 | 0.9 | 0.7 |
| Form D | 0.1 | 0.8 | 0.1 | 0.3 |
| Glassdoor | 0.7 | 0.0 | 0.0 | 0.0 |
| News | 0.4 | 0.6 | 0.3 | 0.5 |
| Corporate Registry | 0.1 | 0.0 | 0.0 | 0.4 |

---

## Database Schema

```sql
-- Data gaps queue
CREATE TABLE data_gaps (
    id SERIAL PRIMARY KEY,

    -- Target record
    entity_type VARCHAR(50) NOT NULL,  -- company, investor
    entity_name VARCHAR(255) NOT NULL,
    entity_id INTEGER,

    -- Gap details
    field_name VARCHAR(100) NOT NULL,
    current_value TEXT,  -- null or incomplete

    -- Priority
    priority_score FLOAT,
    field_importance FLOAT,
    record_importance FLOAT,
    fill_likelihood FLOAT,

    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, hunting, filled, unfillable
    attempts INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP,

    -- Resolution
    filled_value TEXT,
    filled_source VARCHAR(100),
    confidence FLOAT,
    filled_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(entity_type, entity_name, field_name)
);

-- Hunt jobs (batch operations)
CREATE TABLE hunt_jobs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(50) UNIQUE NOT NULL,

    -- Scope
    entity_type VARCHAR(50),
    field_filter TEXT[],  -- specific fields to hunt
    limit_count INTEGER DEFAULT 100,

    -- Progress
    status VARCHAR(20) DEFAULT 'pending',
    total_gaps INTEGER,
    processed INTEGER DEFAULT 0,
    filled INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,

    -- Timing
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Source reliability tracking
CREATE TABLE source_reliability (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    field_name VARCHAR(100) NOT NULL,

    -- Stats
    attempts INTEGER DEFAULT 0,
    successes INTEGER DEFAULT 0,
    success_rate FLOAT,
    avg_confidence FLOAT,

    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source_name, field_name)
);

-- Provenance log (audit trail)
CREATE TABLE data_provenance (
    id SERIAL PRIMARY KEY,

    -- What was updated
    entity_type VARCHAR(50) NOT NULL,
    entity_name VARCHAR(255) NOT NULL,
    field_name VARCHAR(100) NOT NULL,

    -- Change details
    old_value TEXT,
    new_value TEXT,
    source VARCHAR(100),
    confidence FLOAT,

    -- Metadata
    hunt_job_id VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## API Endpoints

### 1. Start Hunt Job

**POST /api/v1/hunter/start**

```json
// Request
{
    "entity_type": "company",  // optional filter
    "fields": ["employee_count", "total_funding"],  // optional filter
    "limit": 50,  // max gaps to process
    "min_priority": 0.5  // minimum priority score
}

// Response
{
    "job_id": "hunt_abc123",
    "status": "running",
    "total_gaps": 45,
    "estimated_time_seconds": 60
}
```

### 2. Get Job Status

**GET /api/v1/hunter/job/{job_id}**

```json
// Response
{
    "job_id": "hunt_abc123",
    "status": "completed",
    "total_gaps": 45,
    "processed": 45,
    "filled": 32,
    "failed": 13,
    "fill_rate": 0.71,
    "duration_seconds": 48,
    "results": [
        {
            "entity": "Stripe",
            "field": "employee_count",
            "status": "filled",
            "value": 8000,
            "source": "glassdoor",
            "confidence": 0.85
        }
    ]
}
```

### 3. View Gap Queue

**GET /api/v1/hunter/queue**

Query params:
- `entity_type`: Filter by type
- `field`: Filter by field
- `status`: Filter by status (pending, hunting, filled, unfillable)
- `min_priority`: Minimum priority score
- `limit`: Max results (default 50)

```json
// Response
{
    "gaps": [
        {
            "id": 123,
            "entity_type": "company",
            "entity_name": "Acme Corp",
            "field": "employee_count",
            "priority_score": 0.85,
            "status": "pending",
            "attempts": 0
        }
    ],
    "total": 234,
    "by_field": {
        "employee_count": 89,
        "total_funding": 67,
        "revenue": 45,
        "sector": 33
    }
}
```

### 4. Get Hunt Stats

**GET /api/v1/hunter/stats**

```json
// Response
{
    "total_gaps": 500,
    "gaps_by_status": {
        "pending": 234,
        "filled": 198,
        "unfillable": 68
    },
    "fill_rate": 0.74,
    "by_field": {
        "employee_count": {"total": 150, "filled": 120, "rate": 0.80},
        "total_funding": {"total": 100, "filled": 65, "rate": 0.65}
    },
    "source_performance": [
        {"source": "glassdoor", "attempts": 200, "success_rate": 0.72},
        {"source": "form_d", "attempts": 150, "success_rate": 0.68}
    ],
    "recent_fills": [
        {"entity": "TechCo", "field": "employees", "value": 500, "source": "glassdoor"}
    ]
}
```

### 5. Scan for Gaps

**POST /api/v1/hunter/scan**

Trigger a scan to identify new data gaps.

```json
// Response
{
    "scanned_records": 1500,
    "new_gaps_found": 45,
    "gaps_by_field": {
        "employee_count": 15,
        "total_funding": 12
    }
}
```

### 6. Hunt Single Entity

**POST /api/v1/hunter/entity/{name}**

Hunt missing data for a specific company.

```json
// Response
{
    "entity": "Stripe",
    "gaps_found": 3,
    "gaps_filled": 2,
    "results": [...]
}
```

---

## Implementation

### DataHunterAgent Class

```python
class DataHunterAgent:
    """AI agent that finds and fills missing data."""

    def __init__(self, db: Session):
        self.db = db

    # Gap scanning
    def scan_for_gaps(self, entity_type: str = None) -> Dict
    def identify_missing_fields(self, record: Dict) -> List[str]
    def calculate_priority(self, entity: str, field: str) -> float

    # Hunting
    def hunt_gap(self, gap_id: int) -> Dict
    def search_source(self, source: str, entity: str, field: str) -> Optional[Any]
    def validate_value(self, value: Any, field: str, sources: List) -> Tuple[bool, float]

    # Source routing
    def get_best_sources(self, field: str) -> List[str]
    def update_source_reliability(self, source: str, field: str, success: bool)

    # Updates
    def fill_gap(self, gap_id: int, value: Any, source: str, confidence: float)
    def log_provenance(self, entity: str, field: str, old_val, new_val, source: str)

    # Jobs
    def start_hunt_job(self, **filters) -> str
    def process_hunt_job(self, job_id: str) -> Dict
    def get_job_status(self, job_id: str) -> Dict
```

### Source Integration

```python
# Source handlers
SOURCES = {
    "glassdoor": {
        "fields": ["employee_count"],
        "handler": hunt_from_glassdoor,
    },
    "form_d": {
        "fields": ["total_funding", "funding_date"],
        "handler": hunt_from_form_d,
    },
    "sec_edgar": {
        "fields": ["revenue", "assets", "sector"],
        "handler": hunt_from_sec,
    },
    "corporate_registry": {
        "fields": ["founding_date", "headquarters", "status"],
        "handler": hunt_from_registry,
    },
    "news": {
        "fields": ["employee_count", "funding", "revenue"],
        "handler": hunt_from_news,
    },
}
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/agents/data_hunter.py` | Core hunting logic |
| `app/api/v1/hunter.py` | API endpoints |

---

## Test Plan

1. **Unit Tests**
   - Gap scanning accuracy
   - Priority calculation
   - Source routing
   - Validation logic

2. **Integration Tests**
   - Full hunt job pipeline
   - Source reliability updates
   - Provenance logging

3. **Manual Testing**
   ```bash
   # Scan for gaps
   curl -X POST http://localhost:8001/api/v1/hunter/scan

   # View gap queue
   curl http://localhost:8001/api/v1/hunter/queue?limit=10

   # Start hunt job
   curl -X POST http://localhost:8001/api/v1/hunter/start \
     -H "Content-Type: application/json" \
     -d '{"limit": 20}'

   # Check job status
   curl http://localhost:8001/api/v1/hunter/job/hunt_abc123

   # Get stats
   curl http://localhost:8001/api/v1/hunter/stats
   ```

---

## Success Criteria

- [x] Gap scanning identifies missing fields
- [x] Priority scoring works correctly
- [x] Multiple sources searched per field
- [x] Data validated before updating
- [x] Provenance logged for all updates
- [x] Source reliability tracked
- [x] All endpoints functional

