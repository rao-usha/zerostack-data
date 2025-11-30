# Public LP Strategies Source Implementation Summary

## Overview

Successfully implemented a new source adapter called **"public_lp_strategies"** for ingesting PUBLIC LP (Limited Partner) investment strategy documents from public pension funds such as CalPERS, CalSTRS, New York State Common Retirement Fund, and Texas TRS.

**Status:** ✅ **COMPLETE** - Ready for use (document parsing/extraction requires future implementation)

---

## Implementation Scope

### What Was Implemented

1. **8 New Database Models** (in `app/core/models.py`)
2. **Complete Source Module** (`app/sources/public_lp_strategies/`)
3. **Analytics View** with query helpers
4. **Integration** into existing job framework
5. **16 Unit Tests** (all passing)

### What Was NOT Implemented (As Required)

- ❌ Web scraping (by design - documents provided as input)
- ❌ Document parsing (PDF/PPTX extraction - TODOs marked)
- ❌ NLP/LLM extraction pipeline (stub functions with clear TODOs)

---

## Database Schema

### New Tables

All tables follow the project's conventions: snake_case names, typed columns, proper indexes, and foreign keys.

#### 1. `lp_fund`
**Purpose:** Identify each LP (CalPERS, CalSTRS, etc.)

**Columns:**
- `id`: INTEGER, primary key
- `name`: TEXT, NOT NULL, UNIQUE (e.g., "CalPERS")
- `formal_name`: TEXT (e.g., "California Public Employees' Retirement System")
- `lp_type`: TEXT, NOT NULL (e.g., 'public_pension', 'sovereign_wealth', 'endowment')
- `jurisdiction`: TEXT (e.g., 'CA', 'NY', 'TX')
- `website_url`: TEXT
- `created_at`: TIMESTAMPTZ, default now()

**Indexes:**
- Unique constraint on `name`

---

#### 2. `lp_document`
**Purpose:** Each publicly available strategy document

**Columns:**
- `id`: INTEGER, primary key
- `lp_id`: INTEGER, FK → lp_fund.id
- `title`: TEXT, NOT NULL
- `document_type`: TEXT, NOT NULL (e.g., 'investment_committee_presentation', 'quarterly_investment_report', 'policy_statement', 'pacing_plan')
- `program`: TEXT, NOT NULL (e.g., 'total_fund', 'private_equity', 'real_estate', 'infrastructure', 'fixed_income')
- `report_period_start`: DATE
- `report_period_end`: DATE
- `fiscal_year`: INTEGER
- `fiscal_quarter`: TEXT ('Q1', 'Q2', 'Q3', 'Q4')
- `source_url`: TEXT, NOT NULL
- `file_format`: TEXT, NOT NULL (e.g., 'pdf', 'pptx', 'html')
- `raw_file_location`: TEXT (S3 path or blob identifier)
- `ingested_at`: TIMESTAMPTZ, default now()
- `created_at`: TIMESTAMPTZ, default now()

**Indexes:**
- `(lp_id, fiscal_year, fiscal_quarter)`
- `(program, fiscal_year, fiscal_quarter)`

---

#### 3. `lp_document_text_section`
**Purpose:** Store parsed text chunks from documents

**Columns:**
- `id`: INTEGER, primary key
- `document_id`: INTEGER, FK → lp_document.id
- `section_name`: TEXT (e.g., 'Executive Summary', 'Private Equity Strategy')
- `page_start`: INTEGER
- `page_end`: INTEGER
- `sequence_order`: INTEGER, NOT NULL (for ordering sections)
- `text`: TEXT, NOT NULL
- `embedding_vector`: JSON (placeholder for vector embeddings)
- `language`: TEXT, default 'en'
- `created_at`: TIMESTAMPTZ, default now()

**Indexes:**
- `(document_id, sequence_order)`

---

#### 4. `lp_strategy_snapshot`
**Purpose:** Normalized LP strategy at per-LP, per-program, per-quarter level

**Columns:**
- `id`: INTEGER, primary key
- `lp_id`: INTEGER, FK → lp_fund.id
- `program`: TEXT, NOT NULL
- `fiscal_year`: INTEGER, NOT NULL
- `fiscal_quarter`: TEXT, NOT NULL ('Q1', 'Q2', 'Q3', 'Q4')
- `strategy_date`: DATE (board or IC date)
- `primary_document_id`: INTEGER, FK → lp_document.id
- `summary_text`: TEXT (high-level summary)
- `risk_positioning`: TEXT (e.g., 'risk_on', 'defensive', 'neutral')
- `liquidity_profile`: TEXT
- `tilt_description`: TEXT (e.g., 'overweight private markets, underweight public equity')
- `created_at`: TIMESTAMPTZ, default now()

**Constraints:**
- **Unique constraint:** `(lp_id, program, fiscal_year, fiscal_quarter)`

---

#### 5. `lp_asset_class_target_allocation`
**Purpose:** Target, range, and current allocation by asset class

**Columns:**
- `id`: INTEGER, primary key
- `strategy_id`: INTEGER, FK → lp_strategy_snapshot.id
- `asset_class`: TEXT, NOT NULL (e.g., 'public_equity', 'private_equity', 'real_estate', 'fixed_income', 'infrastructure', 'cash', 'hedge_funds', 'other')
- `target_weight_pct`: TEXT (stored as string for NUMERIC compatibility)
- `min_weight_pct`: TEXT
- `max_weight_pct`: TEXT
- `current_weight_pct`: TEXT
- `benchmark_weight_pct`: TEXT
- `source_section_id`: INTEGER, FK → lp_document_text_section.id
- `created_at`: TIMESTAMPTZ, default now()

**Indexes:**
- `(strategy_id, asset_class)`

---

#### 6. `lp_asset_class_projection`
**Purpose:** Forward-looking commitments / pacing / projected flows

**Columns:**
- `id`: INTEGER, primary key
- `strategy_id`: INTEGER, FK → lp_strategy_snapshot.id
- `asset_class`: TEXT, NOT NULL
- `projection_horizon`: TEXT, NOT NULL (e.g., '1_year', '3_year', '5_year')
- `net_flow_projection_amount`: TEXT (currency amount)
- `commitment_plan_amount`: TEXT (e.g., PE commitments over horizon)
- `expected_return_pct`: TEXT
- `expected_volatility_pct`: TEXT
- `source_section_id`: INTEGER, FK → lp_document_text_section.id
- `created_at`: TIMESTAMPTZ, default now()

**Indexes:**
- `(strategy_id, asset_class, projection_horizon)`

---

#### 7. `lp_manager_or_vehicle_exposure`
**Purpose:** Manager/fund-level exposures if documents disclose them

**Columns:**
- `id`: INTEGER, primary key
- `strategy_id`: INTEGER, FK → lp_strategy_snapshot.id
- `manager_name`: TEXT
- `vehicle_name`: TEXT
- `vehicle_type`: TEXT (e.g., 'separate_account', 'commingled', 'co_invest')
- `asset_class`: TEXT
- `market_value_amount`: TEXT
- `weight_pct`: TEXT
- `status`: TEXT (e.g., 'active', 'redeeming', 'new_commitment')
- `geo_region`: TEXT (e.g., 'US', 'Europe', 'Global', 'EM')
- `sector_focus`: TEXT
- `source_section_id`: INTEGER, FK → lp_document_text_section.id
- `created_at`: TIMESTAMPTZ, default now()

---

#### 8. `lp_strategy_thematic_tag`
**Purpose:** Tag strategies with themes like AI, energy transition, etc.

**Columns:**
- `id`: INTEGER, primary key
- `strategy_id`: INTEGER, FK → lp_strategy_snapshot.id
- `theme`: TEXT, NOT NULL (e.g., 'ai', 'energy_transition', 'climate_resilience', 'reshoring')
- `relevance_score`: TEXT (0.0–1.0 scale)
- `source_section_id`: INTEGER, FK → lp_document_text_section.id
- `created_at`: TIMESTAMPTZ, default now()

**Indexes:**
- `(strategy_id, theme)`

---

## Source Module Structure

```
app/sources/public_lp_strategies/
├── __init__.py          # Module initialization and metadata
├── config.py            # Constants, known LPs, valid values
├── types.py             # Pydantic models for validation
├── ingest.py            # Core ingestion functions (COMPLETE)
├── normalize.py         # Extraction utilities (STUBS with TODOs)
└── analytics_view.py    # SQL view and query helpers
```

---

## Core Ingestion Functions

All functions in `app/sources/public_lp_strategies/ingest.py`:

### 1. `register_lp_fund(db, fund_input) -> LpFund`
- **Idempotent:** Returns existing fund if name matches
- Creates new LP fund record if not exists

### 2. `register_lp_document(db, document_input) -> LpDocument`
- Registers a new document
- Does not check for duplicates (caller manages)

### 3. `store_document_text_sections(db, document_id, sections) -> List[LpDocumentTextSection]`
- Stores parsed text sections for a document

### 4. `upsert_strategy_snapshot(db, strategy_input) -> LpStrategySnapshot`
- **Idempotent:** Updates existing snapshot for (lp_id, program, FY, quarter)
- Creates new if doesn't exist

### 5. `upsert_asset_class_allocations(db, strategy_id, allocations) -> List[LpAssetClassTargetAllocation]`
- **Idempotent:** Updates existing allocations by (strategy_id, asset_class)
- Creates new if doesn't exist

### 6. `upsert_asset_class_projections(db, strategy_id, projections) -> List[LpAssetClassProjection]`
- **Idempotent:** Updates existing projections by (strategy_id, asset_class, horizon)
- Creates new if doesn't exist

### 7. `upsert_thematic_tags(db, strategy_id, tags) -> List[LpStrategyThematicTag]`
- **Idempotent:** Updates existing tags by (strategy_id, theme)
- Creates new if doesn't exist

### 8. `ingest_lp_strategy_document(...) -> Dict[str, Any]`
- **High-level orchestration function**
- Coordinates all the above functions
- Returns summary with counts

---

## Analytics View: `lp_strategy_quarterly_view`

**Purpose:** Single-row-per-(LP, program, fiscal_year, fiscal_quarter) analytics-ready view

**Provides:**
- Core strategy identifiers (lp_id, lp_name, program, FY, quarter)
- Strategy summary fields
- **Pivoted asset class allocations** (target and current for each asset class)
- **Forward-looking metrics** (3-year commitment plans for PE, RE, Infrastructure)
- **Thematic flags** (boolean columns: theme_ai, theme_energy_transition, etc.)

### Query Helper Functions

Located in `app/sources/public_lp_strategies/analytics_view.py`:

#### 1. `query_strategy_by_lp_program_quarter(db, lp_name, program, fiscal_year, fiscal_quarter)`
**Example:**
```python
result = query_strategy_by_lp_program_quarter(
    db, "CalPERS", "private_equity", 2025, "Q3"
)
```

#### 2. `query_strategies_by_lp_quarter(db, lp_name, fiscal_year, fiscal_quarter)`
**Example:**
```python
# Get all programs for CalPERS Q3 2025
results = query_strategies_by_lp_quarter(db, "CalPERS", 2025, "Q3")
```

#### 3. `query_strategies_by_program_quarter(db, program, fiscal_year, fiscal_quarter, limit=100)`
**Example:**
```python
# Get all LPs' private equity strategies for Q3 2025
results = query_strategies_by_program_quarter(db, "private_equity", 2025, "Q3")
```

#### 4. `query_strategies_with_theme(db, theme, fiscal_year=None, fiscal_quarter=None, limit=100)`
**Example:**
```python
# Get all strategies with AI theme in 2025
results = query_strategies_with_theme(db, "ai", fiscal_year=2025)
```

---

## Integration with Job Framework

### Updated Files

1. **`app/main.py`:** Added `"public_lp_strategies"` to sources list
2. **`app/api/v1/jobs.py`:** Added handler for `source = "public_lp_strategies"`

### Job Handler

When a job with `"source": "public_lp_strategies"` is posted:

**Expected Config Format:**
```json
{
  "source": "public_lp_strategies",
  "config": {
    "lp_name": "CalPERS",
    "program": "private_equity",
    "fiscal_year": 2025,
    "fiscal_quarter": "Q3",
    "document_metadata": {
      "title": "Q3 2025 Investment Committee Presentation",
      "document_type": "investment_committee_presentation",
      "source_url": "https://calpers.gov/docs/q3-2025.pdf",
      "file_format": "pdf",
      "report_period_start": "2025-07-01",
      "report_period_end": "2025-09-30"
    },
    "parsed_sections": [
      {
        "section_name": "Executive Summary",
        "page_start": 1,
        "page_end": 2,
        "sequence_order": 1,
        "text": "..."
      }
    ],
    "extracted_strategy": {
      "strategy": {
        "summary_text": "...",
        "risk_positioning": "risk_on"
      },
      "allocations": [
        {
          "asset_class": "private_equity",
          "target_weight_pct": 25.0,
          "current_weight_pct": 27.5
        }
      ],
      "projections": [
        {
          "asset_class": "private_equity",
          "projection_horizon": "3_year",
          "commitment_plan_amount": 5000000000
        }
      ],
      "thematic_tags": [
        {"theme": "ai", "relevance_score": 0.8}
      ]
    }
  }
}
```

**Handler Flow:**
1. Validates required parameters
2. Parses inputs into Pydantic models
3. Calls `ingest_lp_strategy_document(...)`
4. Updates job status and row counts
5. Handles errors gracefully

---

## Unit Tests

**Location:** `tests/test_public_lp_strategies.py`

**Test Coverage (16 tests, all passing):**

### Model Creation Tests
- ✅ `test_create_lp_fund`
- ✅ `test_create_lp_document`
- ✅ `test_create_strategy_snapshot`
- ✅ `test_unique_constraint_strategy_snapshot`

### Ingestion Function Tests
- ✅ `test_register_lp_fund_idempotent`
- ✅ `test_register_lp_document`
- ✅ `test_store_text_sections`
- ✅ `test_upsert_strategy_snapshot_create`
- ✅ `test_upsert_strategy_snapshot_update`
- ✅ `test_upsert_asset_class_allocations`
- ✅ `test_upsert_asset_class_allocations_idempotent`
- ✅ `test_upsert_thematic_tags`

### Input Validation Tests
- ✅ `test_lp_fund_input_validation`
- ✅ `test_document_input_validation`
- ✅ `test_asset_class_allocation_input_validation`

### Relationship Tests
- ✅ `test_strategy_with_full_relationships`

**Test Results:**
```
16 passed, 1 warning in 1.13s
```

---

## Known LPs (Pre-configured)

Located in `app/sources/public_lp_strategies/config.py`:

1. **CalPERS** - California Public Employees' Retirement System
2. **CalSTRS** - California State Teachers' Retirement System
3. **NYSCRF** - New York State Common Retirement Fund
4. **Texas TRS** - Teacher Retirement System of Texas

---

## Constants and Enumerations

All defined in `config.py`:

### LP Types
- `public_pension`, `sovereign_wealth`, `endowment`

### Document Types
- `investment_committee_presentation`, `quarterly_investment_report`, `policy_statement`, `pacing_plan`

### Programs
- `total_fund`, `private_equity`, `real_estate`, `infrastructure`, `fixed_income`, `public_equity`, `hedge_funds`, `cash`, `other`

### Asset Classes
- `public_equity`, `private_equity`, `real_estate`, `fixed_income`, `infrastructure`, `cash`, `hedge_funds`, `other`

### Projection Horizons
- `1_year`, `3_year`, `5_year`, `10_year`

### Themes
- `ai`, `energy_transition`, `climate_resilience`, `reshoring`, `healthcare`, `technology`, `sustainability`

### Fiscal Quarters
- `Q1`, `Q2`, `Q3`, `Q4`

---

## Future Work (TODOs)

The following are marked as TODOs in `app/sources/public_lp_strategies/normalize.py`:

### 1. Document Parsing
- PDF table extraction (use libraries like `camelot`, `tabula`)
- PPTX text extraction
- HTML parsing

### 2. Text Extraction Pipeline
- `extract_asset_class_allocations(text)` - Extract allocation tables
- `extract_asset_class_projections(text)` - Extract commitment/pacing plans
- `extract_thematic_tags(text)` - Theme detection via NLP/LLM
- `detect_risk_positioning(text)` - Risk stance classification
- `extract_liquidity_profile(text)` - Liquidity analysis
- `generate_strategy_summary(sections)` - LLM-based summarization
- `extract_manager_exposures(text)` - Manager/vehicle extraction

### 3. NLP/LLM Integration
- Embedding generation for semantic search
- LLM-based structured extraction
- Named entity recognition for manager names

### 4. Data Quality & Validation
- Cross-check allocations sum to 100%
- Validate projection amounts vs. AUM
- Detect anomalies in quarter-over-quarter changes

---

## How to Query: Examples

### Example 1: Query CalPERS Q3 2025 private equity strategy

```python
from app.sources.public_lp_strategies.analytics_view import query_strategy_by_lp_program_quarter
from app.core.database import get_db

db = next(get_db())
result = query_strategy_by_lp_program_quarter(
    db, "CalPERS", "private_equity", 2025, "Q3"
)
print(result)
# Output: {
#     'strategy_id': 1,
#     'lp_name': 'CalPERS',
#     'program': 'private_equity',
#     'fiscal_year': 2025,
#     'fiscal_quarter': 'Q3',
#     'target_private_equity_pct': '25.0',
#     'current_private_equity_pct': '27.5',
#     'pe_commitment_plan_3y_amount': '5000000000',
#     'theme_ai': 1,
#     ...
# }
```

### Example 2: Query all CalPERS programs for Q3 2025

```python
results = query_strategies_by_lp_quarter(db, "CalPERS", 2025, "Q3")
# Returns list of strategies for total_fund, private_equity, real_estate, etc.
```

### Example 3: Query all LPs' private equity strategies for Q3 2025

```python
results = query_strategies_by_program_quarter(db, "private_equity", 2025, "Q3")
# Returns private equity strategies for CalPERS, CalSTRS, etc.
```

### Example 4: Query all strategies with "AI" theme in 2025

```python
results = query_strategies_with_theme(db, "ai", fiscal_year=2025)
```

### Example 5: Direct SQL query

```python
from sqlalchemy import text

query = text("""
SELECT lp_name, program, fiscal_quarter,
       target_private_equity_pct, current_private_equity_pct
FROM lp_strategy_quarterly_view
WHERE fiscal_year = :year
  AND theme_ai = 1
ORDER BY lp_name, fiscal_quarter
""")

results = db.execute(query, {"year": 2025}).fetchall()
```

---

## Compliance with Global Rules

✅ **Data Safety:**
- Only public data from official LP disclosure portals
- No PII collection
- No web scraping (documents provided as input)
- Public domain / openly licensed data only

✅ **Network & Rate Limits:**
- No API calls in current implementation
- Future: will implement bounded concurrency with `asyncio.Semaphore`

✅ **Database Safety:**
- All queries use parameterized SQL
- No string concatenation with untrusted input
- Idempotent operations where appropriate
- Typed columns (no raw JSON blobs for data)

✅ **Job Tracking:**
- All ingestion goes through `ingestion_jobs` table
- Status updates: pending → running → success/failed
- Row counts and error messages recorded

✅ **Extensibility:**
- Plugin pattern: all LP logic isolated in source module
- Core service remains source-agnostic
- Easy to add new LPs to `KNOWN_LP_FUNDS`

---

## Files Created/Modified

### Created Files (7)
1. `app/sources/public_lp_strategies/__init__.py`
2. `app/sources/public_lp_strategies/config.py`
3. `app/sources/public_lp_strategies/types.py`
4. `app/sources/public_lp_strategies/ingest.py`
5. `app/sources/public_lp_strategies/normalize.py`
6. `app/sources/public_lp_strategies/analytics_view.py`
7. `tests/test_public_lp_strategies.py`

### Modified Files (3)
1. `app/core/models.py` - Added 8 LP-related models
2. `app/main.py` - Added source to registry
3. `app/api/v1/jobs.py` - Added job handler

---

## Summary

✅ **Database schema:** 8 tables, fully normalized, with proper indexes and constraints  
✅ **Source module:** Complete structure with types, config, and ingestion functions  
✅ **Analytics view:** SQL view with query helpers for "Q3 2025 CalPERS PE strategy" queries  
✅ **Integration:** Registered in job framework, callable via API  
✅ **Tests:** 16 unit tests, all passing  
✅ **Documentation:** Comprehensive, with examples  

🔧 **Remaining work (as designed):**
- Document parsing (PDF/PPTX → text sections)
- NLP/LLM extraction pipeline (text → structured data)

**The source is ready to receive structured inputs and store them in the database. Future work focuses on building the extraction pipeline to convert raw documents into the expected input format.**

---

## Contact / Next Steps

To use this source:

1. **Create tables:** Run `create_tables()` from `app/core/database.py`
2. **Create analytics view:** Run `create_analytics_view(db)` from `analytics_view.py`
3. **Submit job:** POST to `/api/v1/jobs` with config in expected format
4. **Query results:** Use query helper functions or direct SQL

For document parsing/extraction pipeline:
- See TODOs in `app/sources/public_lp_strategies/normalize.py`
- Recommended: Use LLM-based extraction for flexibility


