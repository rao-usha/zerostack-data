# T42: Agentic Due Diligence

## Goal
Build an autonomous AI agent that generates comprehensive due diligence reports for investment targets by leveraging T41's company research and adding risk analysis, red flag detection, and structured DD memo generation.

## Status
- [ ] Approved

## Dependencies
- T41 (Agentic Company Researcher) - COMPLETE

## Architecture

### Core Components

```
DueDiligenceAgent
├── Uses CompanyResearchAgent (T41) for base data
├── RedFlagDetector - Identifies risk signals
├── CompetitiveAnalyzer - Market positioning
├── FinancialHealthChecker - Financial assessment
├── TeamAnalyzer - Leadership analysis
├── DDMemoGenerator - Structured report output
└── RiskScorer - Overall risk quantification
```

### Risk Categories & Signals

| Category | Signals Detected | Data Source |
|----------|------------------|-------------|
| Legal/Regulatory | Lawsuits, SEC violations, regulatory actions | SEC filings, news |
| Financial | Revenue decline, cash burn, debt issues | Enrichment, SEC |
| Team | Executive departures, high turnover, key person risk | Glassdoor, news |
| Market | Declining traffic, app ratings drop, negative reviews | Web traffic, App Store, Glassdoor |
| Competitive | Market share loss, new entrants, pricing pressure | News, web traffic |
| Operational | Product issues, service outages, quality problems | News, reviews |

### Risk Scoring Model

```python
Risk Score (0-100, higher = more risk):
- 0-25:  Low Risk (Green) - Strong fundamentals, no red flags
- 26-50: Moderate Risk (Yellow) - Some concerns, manageable
- 51-75: High Risk (Orange) - Significant concerns
- 76-100: Critical Risk (Red) - Major red flags, proceed with caution
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/agents/due_diligence.py` | Create | Core DD agent with risk detection |
| `app/api/v1/diligence.py` | Create | API endpoints |
| `app/agents/__init__.py` | Modify | Export DueDiligenceAgent |
| `app/main.py` | Modify | Register diligence router |

## Database Tables

### `diligence_jobs`
```sql
CREATE TABLE diligence_jobs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(50) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    research_job_id VARCHAR(50),  -- Links to T41 research job
    status VARCHAR(20) DEFAULT 'pending',
    risk_score FLOAT,
    risk_level VARCHAR(20),  -- low, moderate, high, critical
    red_flags JSONB DEFAULT '[]',
    findings JSONB,
    memo JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

### `diligence_templates`
```sql
CREATE TABLE diligence_templates (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    sections JSONB NOT NULL,  -- Required sections for this template
    is_default BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## API Endpoints

### POST /api/v1/diligence/start
Start due diligence process for a company.

**Request:**
```json
{
  "company_name": "Acme Corp",
  "domain": "acme.com",
  "template": "standard",  // optional: standard, quick, deep
  "focus_areas": ["financial", "team", "legal"]  // optional
}
```

**Response:**
```json
{
  "job_id": "dd_abc123",
  "status": "started",
  "company_name": "Acme Corp",
  "research_job_id": "research_xyz789",
  "message": "Due diligence started. Poll GET /diligence/{job_id} for results."
}
```

### GET /api/v1/diligence/{job_id}
Get DD status and results.

**Response (in progress):**
```json
{
  "job_id": "dd_abc123",
  "status": "running",
  "progress": 0.6,
  "phases_completed": ["research", "financial", "team"],
  "phases_pending": ["legal", "competitive", "memo"]
}
```

**Response (completed):**
```json
{
  "job_id": "dd_abc123",
  "status": "completed",
  "company_name": "Acme Corp",
  "risk_score": 35,
  "risk_level": "moderate",
  "red_flags": [
    {
      "category": "team",
      "severity": "medium",
      "signal": "CFO departed 3 months ago",
      "source": "news",
      "date": "2025-10-15"
    }
  ],
  "findings": {
    "financial": {
      "score": 72,
      "summary": "Strong revenue growth, healthy margins",
      "details": {...}
    },
    "team": {
      "score": 45,
      "summary": "Recent CFO departure raises concerns",
      "details": {...}
    },
    ...
  },
  "memo": {
    "executive_summary": "...",
    "recommendation": "Proceed with caution",
    "key_risks": [...],
    "key_strengths": [...]
  }
}
```

### GET /api/v1/diligence/templates
List available DD templates.

**Response:**
```json
{
  "templates": [
    {
      "id": "standard",
      "name": "Standard Due Diligence",
      "description": "Comprehensive DD covering all areas",
      "sections": ["financial", "team", "legal", "competitive", "market", "operational"]
    },
    {
      "id": "quick",
      "name": "Quick Assessment",
      "description": "Fast risk screening",
      "sections": ["financial", "legal", "team"]
    },
    {
      "id": "deep",
      "name": "Deep Dive",
      "description": "Exhaustive analysis for major investments",
      "sections": ["financial", "team", "legal", "competitive", "market", "operational", "technical", "esg"]
    }
  ]
}
```

### GET /api/v1/diligence/company/{name}
Get cached DD report for a company.

### GET /api/v1/diligence/jobs
List recent DD jobs.

## Implementation Details

### Red Flag Detection Logic

```python
class RedFlagDetector:
    """Detects risk signals from company data."""

    RED_FLAG_PATTERNS = {
        "legal": [
            ("lawsuit", "medium"),
            ("SEC investigation", "high"),
            ("class action", "high"),
            ("regulatory fine", "medium"),
            ("fraud", "critical"),
        ],
        "team": [
            ("CEO departure", "high"),
            ("CFO departure", "high"),
            ("mass layoff", "medium"),
            ("executive turnover", "medium"),
        ],
        "financial": [
            ("revenue decline", "medium"),
            ("cash burn", "medium"),
            ("debt default", "critical"),
            ("going concern", "critical"),
        ],
        "operational": [
            ("data breach", "high"),
            ("product recall", "medium"),
            ("service outage", "low"),
        ]
    }

    def detect(self, company_profile, news_articles):
        """Scan all data for red flags."""
        flags = []
        # Scan news for keywords
        # Check Glassdoor for rating drops
        # Check financial trends
        # Check app store rating changes
        return flags
```

### Risk Score Calculation

```python
def calculate_risk_score(findings: dict) -> tuple[float, str]:
    """Calculate overall risk score from findings."""

    weights = {
        "financial": 0.30,
        "legal": 0.25,
        "team": 0.20,
        "competitive": 0.15,
        "operational": 0.10
    }

    # Invert category scores (high score = low risk)
    # So we calculate: risk = 100 - weighted_avg(scores)

    weighted_sum = 0
    for category, weight in weights.items():
        if category in findings:
            # Category score is 0-100 where 100 = good
            # Risk contribution = (100 - score) * weight
            weighted_sum += (100 - findings[category]["score"]) * weight

    risk_score = weighted_sum

    if risk_score <= 25:
        risk_level = "low"
    elif risk_score <= 50:
        risk_level = "moderate"
    elif risk_score <= 75:
        risk_level = "high"
    else:
        risk_level = "critical"

    return risk_score, risk_level
```

### DD Memo Generation

```python
def generate_memo(company_name, findings, red_flags, risk_score):
    """Generate structured DD memo."""

    return {
        "company": company_name,
        "date": datetime.utcnow().isoformat(),
        "executive_summary": _generate_summary(findings, risk_score),
        "recommendation": _get_recommendation(risk_score, red_flags),
        "risk_assessment": {
            "overall_score": risk_score,
            "level": _get_risk_level(risk_score),
            "key_risks": [f["signal"] for f in red_flags[:5]],
        },
        "strengths": _identify_strengths(findings),
        "concerns": _identify_concerns(findings, red_flags),
        "sections": {
            category: {
                "score": data["score"],
                "summary": data["summary"],
                "details": data.get("details", {})
            }
            for category, data in findings.items()
        },
        "data_sources": _list_data_sources(findings),
        "confidence": _calculate_confidence(findings),
    }
```

## Test Plan

1. Start DD for a company with known data
2. Verify research phase completes (uses T41)
3. Check red flag detection works
4. Verify risk scoring calculation
5. Test memo generation
6. Test templates endpoint
7. Test cached DD retrieval

## Example Usage

```bash
# Start due diligence
curl -X POST http://localhost:8001/api/v1/diligence/start \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe", "template": "standard"}'

# Check status
curl http://localhost:8001/api/v1/diligence/dd_abc123

# List templates
curl http://localhost:8001/api/v1/diligence/templates

# Get cached DD
curl http://localhost:8001/api/v1/diligence/company/Stripe
```
