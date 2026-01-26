# Plan T47: Agentic Report Writer

**Task ID:** T47
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19)

---

## Goal

Build an AI agent that generates comprehensive natural language reports from data, supporting multiple report types, customizable templates, and export formats.

---

## Dependencies

- **T41 (Company Researcher)**: Provides company profile data
- **T42 (Due Diligence)**: Provides DD analysis data
- **T44 (Competitive Intel)**: Provides competitive landscape data
- **T46 (Anomaly Detector)**: Provides anomaly insights

---

## Design

### Report Types

| Type | Description | Data Sources |
|------|-------------|--------------|
| `company_profile` | Deep-dive company analysis | T41, enrichment, scores |
| `due_diligence` | Investment DD report | T42, T41, anomalies |
| `competitive_landscape` | Competitive analysis | T44, enrichment |
| `portfolio_summary` | Portfolio overview | All companies, scores |
| `investor_profile` | Investor analysis | Portfolios, deals |
| `market_overview` | Sector/market analysis | Multiple companies |

### Report Structure

```
1. Executive Summary
   - Key findings (3-5 bullet points)
   - Overall assessment
   - Critical alerts

2. Company/Entity Overview
   - Basic info (employees, funding, location)
   - Business description
   - Key metrics

3. Detailed Analysis (varies by report type)
   - For company: financials, tech, team, market
   - For DD: risks, red flags, recommendations
   - For competitive: comparison matrix, moat analysis

4. Data Sources & Confidence
   - Sources used
   - Data freshness
   - Confidence levels

5. Appendix (optional)
   - Raw data tables
   - Methodology notes
```

### Template System

Templates define:
- Sections to include
- Tone (formal, casual, executive)
- Detail level (summary, standard, detailed)
- Formatting preferences

```python
TEMPLATES = {
    "executive_brief": {
        "sections": ["summary", "key_metrics", "alerts"],
        "tone": "executive",
        "detail": "summary",
        "max_words": 500
    },
    "full_report": {
        "sections": ["summary", "overview", "analysis", "sources", "appendix"],
        "tone": "formal",
        "detail": "detailed",
        "max_words": 5000
    },
    "investor_memo": {
        "sections": ["summary", "investment_thesis", "risks", "recommendation"],
        "tone": "professional",
        "detail": "standard",
        "max_words": 2000
    }
}
```

---

## Database Schema

```sql
-- Generated reports
CREATE TABLE generated_reports (
    id SERIAL PRIMARY KEY,
    report_id VARCHAR(50) UNIQUE NOT NULL,

    -- Report metadata
    report_type VARCHAR(50) NOT NULL,
    template VARCHAR(50),
    title VARCHAR(500),

    -- Target
    entity_type VARCHAR(50),  -- company, investor, portfolio, market
    entity_name VARCHAR(255),
    entity_ids INTEGER[],

    -- Content
    content_json JSONB,  -- Structured report data
    content_markdown TEXT,  -- Rendered markdown
    content_html TEXT,  -- Rendered HTML

    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, generating, completed, failed
    progress INTEGER DEFAULT 0,
    error_message TEXT,

    -- Metadata
    word_count INTEGER,
    sections_count INTEGER,
    data_sources JSONB,
    confidence FLOAT,

    -- Timing
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    -- User
    requested_by VARCHAR(100),

    INDEX idx_reports_type (report_type),
    INDEX idx_reports_entity (entity_name),
    INDEX idx_reports_status (status)
);

-- Report templates (user-customizable)
CREATE TABLE report_templates (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,

    -- Template config
    report_type VARCHAR(50),
    sections JSONB,
    tone VARCHAR(50),
    detail_level VARCHAR(50),
    max_words INTEGER,

    -- Styling
    header_format TEXT,
    section_format TEXT,

    is_default BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## API Endpoints

### 1. Generate Report

**POST /api/v1/reports/generate**

```json
// Request
{
    "report_type": "company_profile",
    "entity_name": "Stripe",
    "template": "full_report",  // optional
    "options": {
        "include_competitors": true,
        "include_news": true,
        "time_range_days": 90
    }
}

// Response
{
    "report_id": "rpt_abc123",
    "status": "generating",
    "estimated_time_seconds": 15
}
```

### 2. Get Report

**GET /api/v1/reports/{report_id}**

```json
// Response
{
    "report_id": "rpt_abc123",
    "report_type": "company_profile",
    "title": "Company Profile: Stripe",
    "status": "completed",
    "entity_name": "Stripe",

    "content": {
        "executive_summary": {
            "key_findings": [
                "Stripe is a leading payments platform with $8.7B in funding",
                "Strong technical team with 8,000+ employees",
                "Health score of 92/100 indicates excellent position"
            ],
            "overall_assessment": "Strong investment candidate with solid fundamentals",
            "alerts": []
        },
        "overview": {
            "description": "Stripe is a technology company...",
            "key_metrics": {
                "employees": 8000,
                "funding": 8700000000,
                "health_score": 92
            }
        },
        "analysis": {...},
        "data_sources": [...]
    },

    "content_markdown": "# Company Profile: Stripe\n\n## Executive Summary...",
    "word_count": 2500,
    "confidence": 0.88,
    "completed_at": "2026-01-19T15:30:00Z"
}
```

### 3. Get Report Status

**GET /api/v1/reports/{report_id}/status**

```json
// Response
{
    "report_id": "rpt_abc123",
    "status": "generating",
    "progress": 65,
    "current_section": "competitive_analysis"
}
```

### 4. List Templates

**GET /api/v1/reports/templates**

```json
// Response
{
    "templates": [
        {
            "name": "executive_brief",
            "description": "Concise executive summary",
            "report_types": ["company_profile", "due_diligence"],
            "detail_level": "summary",
            "is_default": false
        },
        {
            "name": "full_report",
            "description": "Comprehensive detailed report",
            "report_types": ["all"],
            "detail_level": "detailed",
            "is_default": true
        }
    ]
}
```

### 5. Create Custom Template

**POST /api/v1/reports/templates**

```json
// Request
{
    "name": "my_template",
    "description": "Custom investor memo format",
    "report_type": "due_diligence",
    "sections": ["summary", "risks", "recommendation"],
    "tone": "professional",
    "detail_level": "standard",
    "max_words": 1500
}

// Response
{
    "id": 5,
    "name": "my_template",
    "created_at": "2026-01-19T15:00:00Z"
}
```

### 6. Export Report

**GET /api/v1/reports/{report_id}/export**

Query params:
- `format`: pdf, docx, html, markdown (default: markdown)

Returns file download or base64 encoded content.

### 7. List Reports

**GET /api/v1/reports**

Query params:
- `report_type`: Filter by type
- `entity_name`: Filter by entity
- `status`: Filter by status
- `limit`: Max results

```json
// Response
{
    "reports": [
        {
            "report_id": "rpt_abc123",
            "report_type": "company_profile",
            "title": "Company Profile: Stripe",
            "entity_name": "Stripe",
            "status": "completed",
            "created_at": "2026-01-19T15:00:00Z"
        }
    ],
    "total": 25
}
```

---

## Implementation

### ReportWriterAgent Class

```python
class ReportWriterAgent:
    """AI agent for generating comprehensive reports."""

    def __init__(self, db: Session):
        self.db = db
        self.researcher = CompanyResearchAgent(db)
        self.dd_agent = DueDiligenceAgent(db)

    # Report Generation
    def generate_report(self, report_type: str, entity_name: str, **options) -> Dict
    def _generate_company_profile(self, company: str, template: Dict) -> Dict
    def _generate_due_diligence(self, company: str, template: Dict) -> Dict
    def _generate_competitive_landscape(self, company: str, template: Dict) -> Dict
    def _generate_portfolio_summary(self, investor_id: int, template: Dict) -> Dict

    # Content Generation
    def _write_executive_summary(self, data: Dict) -> Dict
    def _write_section(self, section_type: str, data: Dict, template: Dict) -> str
    def _format_metrics(self, metrics: Dict) -> str
    def _generate_insights(self, data: Dict) -> List[str]

    # Rendering
    def _render_markdown(self, content: Dict, template: Dict) -> str
    def _render_html(self, markdown: str) -> str

    # Templates
    def get_templates(self) -> List[Dict]
    def get_template(self, name: str) -> Optional[Dict]
    def create_template(self, **config) -> Dict

    # Export
    def export_report(self, report_id: str, format: str) -> bytes
```

### Report Generation Flow

```python
def generate_report(self, report_type: str, entity_name: str, **options) -> Dict:
    """Generate a comprehensive report."""

    # 1. Create report record
    report_id = f"rpt_{uuid.uuid4().hex[:12]}"
    self._create_report_record(report_id, report_type, entity_name)

    # 2. Get template
    template = self.get_template(options.get("template", "full_report"))

    # 3. Gather data based on report type
    if report_type == "company_profile":
        data = self._gather_company_data(entity_name, options)
    elif report_type == "due_diligence":
        data = self._gather_dd_data(entity_name, options)
    # ... etc

    # 4. Generate content sections
    content = {}
    for section in template["sections"]:
        content[section] = self._write_section(section, data, template)
        self._update_progress(report_id, section)

    # 5. Render to markdown/HTML
    markdown = self._render_markdown(content, template)
    html = self._render_html(markdown)

    # 6. Save and return
    self._save_report(report_id, content, markdown, html)
    return self.get_report(report_id)
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/agents/report_writer.py` | Core report generation logic |
| `app/api/v1/reports_gen.py` | API endpoints |

---

## Test Plan

1. **Unit Tests**
   - Section generation
   - Template handling
   - Markdown rendering
   - Data aggregation

2. **Integration Tests**
   - Full report generation
   - Multi-source data gathering
   - Export functionality

3. **Manual Testing**
   ```bash
   # Generate company profile
   curl -X POST http://localhost:8001/api/v1/reports/generate \
     -H "Content-Type: application/json" \
     -d '{"report_type": "company_profile", "entity_name": "Stripe"}'

   # Get report
   curl http://localhost:8001/api/v1/reports/rpt_abc123

   # List templates
   curl http://localhost:8001/api/v1/reports/templates

   # Export as markdown
   curl http://localhost:8001/api/v1/reports/rpt_abc123/export?format=markdown
   ```

---

## Success Criteria

- [x] Company profile reports generate correctly
- [x] Due diligence reports with risk analysis
- [x] Template system working
- [x] Markdown/HTML rendering
- [x] Export functionality
- [x] All endpoints functional

