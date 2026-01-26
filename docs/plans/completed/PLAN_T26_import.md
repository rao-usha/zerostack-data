# Plan T26: Bulk Portfolio Import

**Task ID:** T26
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-16

---

## Goal

Allow users to upload their own portfolio data via CSV/Excel files. Provide data validation, preview, and rollback capabilities for safe bulk imports.

---

## Why This Matters

1. **User Data Integration**: Users can bring their own portfolio data
2. **Bulk Operations**: Import thousands of holdings at once
3. **Data Quality**: Validation prevents bad data from entering the system
4. **Safety**: Preview and rollback capabilities for error recovery

---

## Design

### Supported Formats
- CSV (comma-separated)
- Excel (.xlsx, .xls)

### Required Columns
| Column | Required | Description |
|--------|----------|-------------|
| `company_name` | Yes | Name of the portfolio company |
| `investor_name` | Yes | Name of the investor (LP or Family Office) |
| `investor_type` | Yes | "lp" or "family_office" |

### Optional Columns
| Column | Description |
|--------|-------------|
| `company_website` | Company website URL |
| `company_industry` | Industry classification |
| `company_stage` | Investment stage (seed, series_a, etc.) |
| `investment_date` | Date of investment |
| `investment_amount` | Investment amount (USD) |
| `shares_held` | Number of shares |
| `market_value` | Current market value |
| `ownership_percentage` | Ownership % |

### Database Schema

```sql
-- Import job tracking
CREATE TABLE IF NOT EXISTS portfolio_imports (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    file_size INTEGER,
    row_count INTEGER,
    status VARCHAR(20) DEFAULT 'pending',  -- pending, validating, previewing, importing, completed, failed, rolled_back

    -- Validation results
    valid_rows INTEGER DEFAULT 0,
    invalid_rows INTEGER DEFAULT 0,
    validation_errors JSONB,

    -- Import results
    imported_count INTEGER DEFAULT 0,
    skipped_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,

    -- Rollback support
    rollback_data JSONB,  -- stores IDs of created records for rollback

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/import/upload` | POST | Upload CSV/Excel file |
| `/import/{id}/preview` | GET | Preview import with validation |
| `/import/{id}/confirm` | POST | Execute confirmed import |
| `/import/{id}/status` | GET | Get import job status |
| `/import/history` | GET | List past imports |
| `/import/{id}/rollback` | POST | Rollback a completed import |

### Import Flow

```
1. UPLOAD    → User uploads file → Returns import_id
2. VALIDATE  → System parses and validates → Shows errors/warnings
3. PREVIEW   → User reviews preview → Can fix file and re-upload
4. CONFIRM   → User confirms import → System inserts records
5. COMPLETE  → Import done → Can rollback if needed
```

### Import Engine

```python
class PortfolioImporter:
    """Bulk portfolio import engine."""

    def parse_file(self, file: UploadFile) -> List[Dict]:
        """Parse CSV or Excel file into records."""

    def validate_row(self, row: Dict, row_num: int) -> ValidationResult:
        """Validate a single row against schema."""

    def match_investor(self, name: str, investor_type: str) -> Optional[int]:
        """Find existing investor by name (fuzzy match)."""

    def match_company(self, name: str) -> Optional[str]:
        """Find existing company by name (fuzzy match)."""

    async def import_rows(self, import_id: int, rows: List[Dict]) -> ImportResult:
        """Execute the import, inserting validated rows."""

    async def rollback_import(self, import_id: int) -> bool:
        """Rollback a completed import by deleting created records."""
```

---

## Implementation

### 1. `app/import/__init__.py`
Package initialization.

### 2. `app/import/portfolio.py`
Main import engine with parsing, validation, and import logic.

### 3. `app/api/v1/import_portfolio.py`
FastAPI router with 6 endpoints. (Named import_portfolio to avoid Python reserved word)

---

## Response Formats

### Upload Response
```json
{
  "import_id": 123,
  "filename": "portfolio_2024.csv",
  "row_count": 500,
  "status": "validating",
  "message": "File uploaded, validation in progress"
}
```

### Preview Response
```json
{
  "import_id": 123,
  "status": "previewing",
  "validation": {
    "total_rows": 500,
    "valid_rows": 485,
    "invalid_rows": 15,
    "errors": [
      {"row": 12, "column": "investor_type", "error": "Invalid value 'hedge_fund', must be 'lp' or 'family_office'"},
      {"row": 45, "column": "company_name", "error": "Required field is empty"}
    ],
    "warnings": [
      {"row": 100, "message": "Investor 'ABC Capital' not found, will create new record"}
    ]
  },
  "preview_data": [
    {"company_name": "Stripe", "investor_name": "CalPERS", "investor_type": "lp", "status": "valid"},
    {"company_name": "SpaceX", "investor_name": "Unknown Fund", "investor_type": "lp", "status": "warning", "warning": "New investor"}
  ]
}
```

### Import Result Response
```json
{
  "import_id": 123,
  "status": "completed",
  "results": {
    "imported": 485,
    "skipped": 10,
    "errors": 5,
    "new_investors_created": 3,
    "existing_investors_matched": 482
  },
  "completed_at": "2026-01-16T15:30:00Z",
  "can_rollback": true
}
```

---

## Files to Create

1. `app/import/__init__.py` - Package init
2. `app/import/portfolio.py` - Import engine
3. `app/api/v1/import_portfolio.py` - API endpoints

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Create test CSV file
3. Test endpoints:
   - `POST /api/v1/import/upload` with CSV file
   - `GET /api/v1/import/{id}/preview` - Check validation
   - `POST /api/v1/import/{id}/confirm` - Execute import
   - `GET /api/v1/import/{id}/status` - Check status
   - `GET /api/v1/import/history` - List imports
   - `POST /api/v1/import/{id}/rollback` - Test rollback

---

## Success Criteria

- [ ] CSV file parsing works correctly
- [ ] Excel file parsing works correctly
- [ ] Validation catches required field errors
- [ ] Investor matching uses fuzzy matching
- [ ] Preview shows errors and warnings
- [ ] Import creates portfolio_companies records
- [ ] Rollback deletes imported records
- [ ] Import history tracks all operations

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Used `import_data` package name (avoiding Python reserved word `import`)
- CSV parsing with UTF-8/Latin-1 fallback
- Excel parsing via openpyxl (optional dependency)
- Fuzzy matching for investor/company names using SequenceMatcher
- In-memory row cache for preview/confirm flow
- Rollback stores imported IDs for deletion
- All 6 endpoints working and tested

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
