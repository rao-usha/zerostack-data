# Plan T25: Custom Report Builder

## Overview
**Task:** T25
**Tab:** 2
**Feature:** Generate customizable PDF/Excel reports for sharing insights
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

Investment professionals need to create shareable reports:

1. **Investor Profiles**: One-pagers on LP/Family Office for meetings
2. **Portfolio Summaries**: Overview of an investor's holdings
3. **Comparison Reports**: Side-by-side investor analysis
4. **Trend Reports**: Market trends and sector analysis
5. **Custom Reports**: User-defined content and layout

### User Scenarios

#### Scenario 1: Meeting Prep
**Fund Manager** preparing for CalPERS meeting.
- Request: "Generate a profile report for CalPERS"
- Result: PDF with overview, portfolio, recent activity, key contacts

#### Scenario 2: Portfolio Review
**LP Relations** needs quarterly portfolio summary.
- Request: "Generate portfolio report for investor 1"
- Result: Excel with holdings, sector breakdown, performance

#### Scenario 3: Market Analysis
**Research Analyst** preparing sector trends report.
- Request: "Generate trend report for Technology sector"
- Result: PDF with sector trends, top investors, emerging themes

---

## Success Criteria

### Must Have

| ID | Criteria | Verification |
|----|----------|--------------|
| M1 | Generate investor profile report | PDF/Excel output |
| M2 | Generate portfolio summary report | Holdings list with breakdown |
| M3 | Multiple output formats | PDF and Excel supported |
| M4 | Report templates | At least 3 templates |

### Should Have

| ID | Criteria | Verification |
|----|----------|--------------|
| S1 | Download via URL | Report accessible by ID |
| S2 | Report history | List past generated reports |
| S3 | Custom date range | Filter data by dates |

---

## Technical Design

### Report Templates

| Template | Description | Sections |
|----------|-------------|----------|
| `investor_profile` | One-pager on investor | Overview, Portfolio Summary, Top Holdings, Sector Allocation |
| `portfolio_detail` | Full portfolio breakdown | Holdings List, Sector Breakdown, Stage Breakdown, Recent Changes |
| `comparison` | Side-by-side investors | Both investors' stats, Overlap Analysis, Unique Holdings |
| `trend_analysis` | Market trends | Sector Trends, Geographic Trends, Emerging Themes |

### Database Schema

```sql
CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY,
    template VARCHAR(50) NOT NULL,
    title VARCHAR(255),
    format VARCHAR(10) NOT NULL,  -- 'pdf', 'excel'
    status VARCHAR(20) DEFAULT 'pending',  -- pending, generating, complete, failed

    -- Parameters
    params JSONB,

    -- Output
    file_path TEXT,
    file_size INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/reports/templates` | List available templates |
| POST | `/api/v1/reports/generate` | Generate a report |
| GET | `/api/v1/reports/{id}` | Get report status/metadata |
| GET | `/api/v1/reports/{id}/download` | Download report file |
| GET | `/api/v1/reports` | List generated reports |

### Request/Response Models

**Generate Report Request:**
```json
{
  "template": "investor_profile",
  "format": "pdf",
  "params": {
    "investor_id": 1,
    "investor_type": "lp"
  }
}
```

**Report Response:**
```json
{
  "id": 123,
  "template": "investor_profile",
  "title": "CalPERS - Investor Profile",
  "format": "pdf",
  "status": "complete",
  "file_size": 125000,
  "download_url": "/api/v1/reports/123/download",
  "created_at": "2026-01-16T12:00:00Z",
  "completed_at": "2026-01-16T12:00:05Z"
}
```

### Report Builder Service

```python
class ReportBuilder:
    """Report generation service."""

    def __init__(self, db: Session):
        self.db = db
        self.templates = {
            "investor_profile": InvestorProfileTemplate,
            "portfolio_detail": PortfolioDetailTemplate,
            "comparison": ComparisonTemplate,
            "trend_analysis": TrendAnalysisTemplate,
        }

    def generate(self, template: str, format: str, params: dict) -> int:
        """Generate a report, returns report ID."""

    def get_report(self, report_id: int) -> dict:
        """Get report metadata."""

    def get_download_path(self, report_id: int) -> str:
        """Get file path for download."""

    def list_reports(self, limit: int, offset: int) -> List[dict]:
        """List generated reports."""
```

### Template Implementation

Each template generates data and renders to format:

```python
class InvestorProfileTemplate:
    """Investor profile report template."""

    def gather_data(self, db: Session, params: dict) -> dict:
        """Gather all data needed for the report."""
        investor_id = params["investor_id"]
        investor_type = params["investor_type"]

        return {
            "investor": get_investor(db, investor_id, investor_type),
            "portfolio": get_portfolio_summary(db, investor_id, investor_type),
            "top_holdings": get_top_holdings(db, investor_id, investor_type, limit=10),
            "sector_allocation": get_sector_allocation(db, investor_id, investor_type),
            "recent_changes": get_recent_changes(db, investor_id, investor_type),
        }

    def render_pdf(self, data: dict) -> bytes:
        """Render report as PDF."""

    def render_excel(self, data: dict) -> bytes:
        """Render report as Excel."""
```

### PDF Generation

Using basic HTML-to-PDF approach (no external dependencies):

```python
def render_html_report(template_name: str, data: dict) -> str:
    """Render report as HTML string."""
    # Use simple string templating
    # Return HTML that can be saved or converted

def save_as_html(html: str, path: str):
    """Save HTML report to file."""
```

For MVP, we'll generate HTML reports that can be opened in browser and printed to PDF. Excel will use openpyxl (already in requirements).

### Excel Generation

```python
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

def render_excel_report(data: dict) -> bytes:
    """Generate Excel workbook."""
    wb = Workbook()

    # Summary sheet
    ws = wb.active
    ws.title = "Summary"
    # Add data...

    # Holdings sheet
    ws_holdings = wb.create_sheet("Holdings")
    # Add holdings table...

    # Save to bytes
    output = BytesIO()
    wb.save(output)
    return output.getvalue()
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/reports/__init__.py` | Package init |
| `app/reports/builder.py` | ReportBuilder service |
| `app/reports/templates/__init__.py` | Templates package |
| `app/reports/templates/investor_profile.py` | Investor profile template |
| `app/reports/templates/portfolio_detail.py` | Portfolio detail template |
| `app/api/v1/reports.py` | 5 API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register reports router |

---

## Implementation Steps

1. Create `app/reports/` package structure
2. Create ReportBuilder service with template registry
3. Implement investor_profile template (PDF + Excel)
4. Implement portfolio_detail template
5. Create API endpoints
6. Register router in main.py
7. Test all endpoints

---

## Test Plan

| Test ID | Test | Expected |
|---------|------|----------|
| RPT-001 | List templates | Returns 4 templates |
| RPT-002 | Generate investor profile PDF | Report created, downloadable |
| RPT-003 | Generate portfolio Excel | Excel file with multiple sheets |
| RPT-004 | Get report status | Returns metadata |
| RPT-005 | Download report | File downloads correctly |
| RPT-006 | List reports | Returns report history |

### Test Commands

```bash
# List templates
curl -s "http://localhost:8001/api/v1/reports/templates" | python -m json.tool

# Generate investor profile
curl -s -X POST "http://localhost:8001/api/v1/reports/generate" \
  -H "Content-Type: application/json" \
  -d '{"template": "investor_profile", "format": "excel", "params": {"investor_id": 1, "investor_type": "lp"}}' \
  | python -m json.tool

# Check status
curl -s "http://localhost:8001/api/v1/reports/1" | python -m json.tool

# Download
curl -s "http://localhost:8001/api/v1/reports/1/download" -o report.xlsx

# List reports
curl -s "http://localhost:8001/api/v1/reports" | python -m json.tool
```

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Created `app/reports/` package with builder and templates
- InvestorProfileTemplate: Overview, portfolio summary, top holdings, sectors
- PortfolioDetailTemplate: All holdings, sector/stage breakdown
- Excel output using openpyxl with styled headers
- HTML output for browser viewing and print-to-PDF
- Reports stored in /tmp/nexdata_reports/ inside container
- Database table tracks report status, params, and file paths

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
