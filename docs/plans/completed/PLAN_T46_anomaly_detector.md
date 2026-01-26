# Plan T46: Agentic Anomaly Detector

**Task ID:** T46
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19)

---

## Goal

Build an AI agent that detects unusual patterns and changes in company data, correlates anomalies across data sources, assigns severity scores, and provides proactive alerts.

---

## Dependencies

- **T36 (Company Scoring)**: Provides baseline health scores for comparison
- **T22 (Enrichment)**: Company data to monitor for changes
- **T35 (Web Traffic)**: Traffic data for spike detection
- **T34 (GitHub)**: GitHub activity for tech velocity anomalies

---

## Design

### Core Components

1. **Pattern Learner**: Learns normal patterns per company/sector
2. **Change Detector**: Identifies significant deviations from baseline
3. **Anomaly Correlator**: Links related anomalies across sources
4. **Severity Scorer**: Assigns impact scores to anomalies
5. **Alert Generator**: Creates actionable alerts with context

### Anomaly Types

| Type | Signals | Severity Range |
|------|---------|----------------|
| Score Drop | Composite score drops >10 points | Medium-Critical |
| Funding Spike | Unusual funding amount or timing | Low-High |
| Employee Change | >20% change in employee count | Medium-High |
| Traffic Anomaly | >50% traffic rank change | Low-Medium |
| Rating Drop | Glassdoor/App Store rating drop >0.5 | Medium-High |
| GitHub Stall | Activity drop >70% vs baseline | Low-Medium |
| Executive Exit | Key personnel departure | High-Critical |
| Status Change | Active → Inactive/Acquired/Bankrupt | Critical |

### Severity Scoring

```
severity = base_severity * magnitude_multiplier * recency_factor

Where:
- base_severity: Default severity for anomaly type (0-1)
- magnitude_multiplier: How far from normal (1.0-3.0)
- recency_factor: Recent anomalies weighted higher (0.5-1.5)
```

### Severity Levels

| Level | Score Range | Response |
|-------|-------------|----------|
| Low | 0.0-0.3 | Log only |
| Medium | 0.3-0.6 | Alert + monitor |
| High | 0.6-0.8 | Alert + investigate |
| Critical | 0.8-1.0 | Immediate alert + action |

---

## Database Schema

```sql
-- Detected anomalies
CREATE TABLE anomalies (
    id SERIAL PRIMARY KEY,

    -- Target
    company_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50) DEFAULT 'company',

    -- Anomaly details
    anomaly_type VARCHAR(50) NOT NULL,
    description TEXT,

    -- Values
    previous_value TEXT,
    current_value TEXT,
    change_magnitude FLOAT,

    -- Scoring
    severity_score FLOAT,
    severity_level VARCHAR(20),  -- low, medium, high, critical
    confidence FLOAT,

    -- Source
    data_source VARCHAR(100),
    source_record_id INTEGER,

    -- Status
    status VARCHAR(20) DEFAULT 'new',  -- new, acknowledged, investigating, resolved
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,

    -- Correlation
    correlated_anomaly_ids INTEGER[],
    root_cause_id INTEGER,

    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_anomalies_company (company_name),
    INDEX idx_anomalies_severity (severity_score DESC),
    INDEX idx_anomalies_type (anomaly_type)
);

-- Baseline patterns (what's "normal" for each company/sector)
CREATE TABLE baselines (
    id SERIAL PRIMARY KEY,

    -- Target
    entity_type VARCHAR(50) NOT NULL,  -- company, sector
    entity_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,

    -- Baseline stats
    baseline_value FLOAT,
    mean_value FLOAT,
    std_deviation FLOAT,
    min_value FLOAT,
    max_value FLOAT,
    sample_count INTEGER,

    -- Thresholds
    lower_threshold FLOAT,
    upper_threshold FLOAT,

    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(entity_type, entity_name, metric_name)
);

-- Detection jobs
CREATE TABLE anomaly_scans (
    id SERIAL PRIMARY KEY,
    scan_id VARCHAR(50) UNIQUE NOT NULL,

    -- Scope
    scan_type VARCHAR(50),  -- full, company, sector, metric
    target_filter TEXT,

    -- Results
    status VARCHAR(20) DEFAULT 'pending',
    records_scanned INTEGER DEFAULT 0,
    anomalies_found INTEGER DEFAULT 0,

    -- Timing
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## API Endpoints

### 1. Get Recent Anomalies

**GET /api/v1/anomalies/recent**

Query params:
- `hours`: Time window (default: 24)
- `severity`: Filter by level
- `type`: Filter by anomaly type
- `limit`: Max results (default: 50)

```json
// Response
{
    "anomalies": [
        {
            "id": 123,
            "company_name": "TechCo",
            "anomaly_type": "score_drop",
            "description": "Health score dropped from 85 to 72",
            "previous_value": "85",
            "current_value": "72",
            "change_magnitude": -0.15,
            "severity_score": 0.65,
            "severity_level": "high",
            "status": "new",
            "detected_at": "2026-01-19T14:30:00Z"
        }
    ],
    "total": 15,
    "by_severity": {"critical": 2, "high": 5, "medium": 6, "low": 2},
    "by_type": {"score_drop": 4, "traffic_anomaly": 6, "rating_drop": 5}
}
```

### 2. Get Company Anomalies

**GET /api/v1/anomalies/company/{name}**

Query params:
- `days`: Time window (default: 30)
- `status`: Filter by status
- `include_resolved`: Include resolved anomalies

```json
// Response
{
    "company": "TechCo",
    "anomalies": [...],
    "total": 8,
    "unresolved": 3,
    "risk_summary": {
        "overall_risk": "medium",
        "active_critical": 0,
        "active_high": 1,
        "trend": "improving"
    }
}
```

### 3. Investigate Anomaly

**POST /api/v1/anomalies/investigate**

```json
// Request
{
    "anomaly_id": 123,
    "depth": "deep"  // quick, standard, deep
}

// Response
{
    "anomaly": {...},
    "investigation": {
        "probable_causes": [
            {"cause": "Recent layoffs announced", "confidence": 0.8},
            {"cause": "Negative press coverage", "confidence": 0.6}
        ],
        "correlated_anomalies": [
            {"id": 120, "type": "employee_change", "correlation": 0.85}
        ],
        "historical_context": {
            "similar_events": 2,
            "typical_resolution_time": "14 days",
            "recovery_rate": 0.75
        },
        "recommendations": [
            "Monitor employee count changes",
            "Review news sentiment over next 2 weeks"
        ]
    }
}
```

### 4. Get Learned Patterns

**GET /api/v1/anomalies/patterns**

Query params:
- `entity_type`: company or sector
- `entity_name`: Filter by name
- `metric`: Filter by metric

```json
// Response
{
    "patterns": [
        {
            "entity_type": "company",
            "entity_name": "TechCo",
            "metric": "employee_count",
            "baseline": 5000,
            "mean": 5200,
            "std_deviation": 300,
            "lower_threshold": 4600,
            "upper_threshold": 5800,
            "sample_count": 24
        }
    ],
    "total": 150
}
```

### 5. Run Anomaly Scan

**POST /api/v1/anomalies/scan**

```json
// Request
{
    "scan_type": "full",  // full, company, sector
    "target": null,  // company name or sector
    "force": false  // force re-scan even if recent
}

// Response
{
    "scan_id": "scan_abc123",
    "status": "running",
    "estimated_time_seconds": 30
}
```

### 6. Get Scan Status

**GET /api/v1/anomalies/scan/{scan_id}**

```json
// Response
{
    "scan_id": "scan_abc123",
    "status": "completed",
    "records_scanned": 500,
    "anomalies_found": 12,
    "duration_seconds": 25,
    "new_anomalies": [...]
}
```

### 7. Acknowledge/Resolve Anomaly

**PATCH /api/v1/anomalies/{anomaly_id}**

```json
// Request
{
    "status": "resolved",
    "resolution_notes": "Investigated - normal seasonal fluctuation"
}

// Response
{
    "id": 123,
    "status": "resolved",
    "resolved_at": "2026-01-19T16:00:00Z"
}
```

---

## Implementation

### AnomalyDetectorAgent Class

```python
class AnomalyDetectorAgent:
    """AI agent for detecting anomalies in company data."""

    def __init__(self, db: Session):
        self.db = db

    # Detection
    def scan_for_anomalies(self, scan_type: str = "full", target: str = None) -> Dict
    def detect_score_anomalies(self, company_name: str = None) -> List[Dict]
    def detect_metric_anomalies(self, metric: str, company: str = None) -> List[Dict]

    # Baselines
    def learn_baseline(self, entity_type: str, entity_name: str, metric: str) -> Dict
    def get_baseline(self, entity_type: str, entity_name: str, metric: str) -> Optional[Dict]
    def update_baseline(self, entity_type: str, entity_name: str, metric: str, value: float)

    # Scoring
    def calculate_severity(self, anomaly_type: str, magnitude: float, ...) -> float
    def determine_severity_level(self, score: float) -> str

    # Correlation
    def correlate_anomalies(self, anomaly_id: int) -> List[Dict]
    def find_root_cause(self, anomaly_ids: List[int]) -> Optional[Dict]

    # Investigation
    def investigate(self, anomaly_id: int, depth: str = "standard") -> Dict
    def get_probable_causes(self, anomaly: Dict) -> List[Dict]
    def get_recommendations(self, anomaly: Dict) -> List[str]

    # Queries
    def get_recent_anomalies(self, hours: int = 24, **filters) -> Dict
    def get_company_anomalies(self, company_name: str, **filters) -> Dict
    def get_patterns(self, **filters) -> Dict
```

### Anomaly Detection Logic

```python
def detect_score_anomalies(self, company_name: str = None) -> List[Dict]:
    """Detect anomalies in company scores."""

    # Get recent scores with changes
    query = text("""
        SELECT cs1.company_name,
               cs1.composite_score as current_score,
               cs2.composite_score as previous_score,
               cs1.scored_at
        FROM company_scores cs1
        LEFT JOIN company_scores cs2 ON cs1.company_name = cs2.company_name
            AND cs2.scored_at < cs1.scored_at
        WHERE cs1.scored_at > NOW() - INTERVAL '7 days'
          AND cs2.scored_at IS NOT NULL
        ORDER BY cs1.company_name, cs1.scored_at DESC
    """)

    anomalies = []
    for row in results:
        change = row.current_score - row.previous_score
        pct_change = change / row.previous_score if row.previous_score else 0

        # Detect significant drops
        if change < -10 or pct_change < -0.15:
            anomalies.append({
                "company_name": row.company_name,
                "anomaly_type": "score_drop",
                "previous_value": row.previous_score,
                "current_value": row.current_score,
                "change_magnitude": pct_change,
                ...
            })

    return anomalies
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/agents/anomaly_detector.py` | Core anomaly detection logic |
| `app/api/v1/anomalies.py` | API endpoints |

---

## Test Plan

1. **Unit Tests**
   - Baseline learning and threshold calculation
   - Severity scoring accuracy
   - Anomaly correlation logic
   - Investigation recommendations

2. **Integration Tests**
   - Full scan pipeline
   - Cross-source correlation
   - Alert generation

3. **Manual Testing**
   ```bash
   # Run full scan
   curl -X POST http://localhost:8001/api/v1/anomalies/scan

   # Get recent anomalies
   curl http://localhost:8001/api/v1/anomalies/recent

   # Get company anomalies
   curl http://localhost:8001/api/v1/anomalies/company/Stripe

   # Investigate anomaly
   curl -X POST http://localhost:8001/api/v1/anomalies/investigate \
     -H "Content-Type: application/json" \
     -d '{"anomaly_id": 1}'

   # Get patterns
   curl http://localhost:8001/api/v1/anomalies/patterns
   ```

---

## Success Criteria

- [x] Anomaly detection across multiple data sources
- [x] Baseline learning per company/sector
- [x] Severity scoring with confidence
- [x] Anomaly correlation working
- [x] Investigation provides actionable insights
- [x] All endpoints functional

