# Public LP Strategies - Quick Start Guide

## What is This Source?

The **public_lp_strategies** source adapter ingests investment strategy documents from public pension funds (Limited Partners) like CalPERS, CalSTRS, and others.

It captures:
- Asset allocation targets and current positions
- Forward-looking commitment plans
- Investment themes (AI, energy transition, etc.)
- Strategy summaries and risk positioning

---

## Quick Example: Query CalPERS Q3 2025 Private Equity Strategy

```python
from app.sources.public_lp_strategies.analytics_view import query_strategy_by_lp_program_quarter
from app.core.database import get_db

db = next(get_db())
result = query_strategy_by_lp_program_quarter(
    db, "CalPERS", "private_equity", 2025, "Q3"
)

print(f"LP: {result['lp_name']}")
print(f"Program: {result['program']}")
print(f"Target PE Allocation: {result['target_private_equity_pct']}%")
print(f"Current PE Allocation: {result['current_private_equity_pct']}%")
print(f"3-Year Commitment Plan: ${result['pe_commitment_plan_3y_amount']}")
print(f"AI Theme: {'Yes' if result['theme_ai'] else 'No'}")
```

---

## How to Ingest Data

### Step 1: Prepare Your Data

You need:
1. **Document metadata** (title, URL, fiscal period)
2. **Parsed text sections** (extracted from PDF/PPTX)
3. **Extracted strategy data** (allocations, projections, tags)

### Step 2: Submit a Job

POST to `/api/v1/jobs`:

```json
{
  "source": "public_lp_strategies",
  "config": {
    "lp_name": "CalPERS",
    "program": "private_equity",
    "fiscal_year": 2025,
    "fiscal_quarter": "Q3",
    "document_metadata": {
      "title": "Q3 2025 Investment Committee Report",
      "document_type": "investment_committee_presentation",
      "source_url": "https://calpers.gov/docs/q3-2025.pdf",
      "file_format": "pdf",
      "report_period_start": "2025-07-01",
      "report_period_end": "2025-09-30"
    },
    "parsed_sections": [
      {
        "section_name": "Executive Summary",
        "sequence_order": 1,
        "text": "The private equity program continues to perform well..."
      }
    ],
    "extracted_strategy": {
      "strategy": {
        "summary_text": "PE program on track with strong performance",
        "risk_positioning": "risk_on"
      },
      "allocations": [
        {
          "asset_class": "private_equity",
          "target_weight_pct": 25.0,
          "current_weight_pct": 27.5,
          "min_weight_pct": 20.0,
          "max_weight_pct": 30.0
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

### Step 3: Check Job Status

GET `/api/v1/jobs/{job_id}`

```json
{
  "id": 123,
  "source": "public_lp_strategies",
  "status": "success",
  "rows_inserted": 15,
  "completed_at": "2025-11-26T12:34:56Z"
}
```

---

## Common Queries

### 1. Get All Programs for an LP in a Quarter

```python
from app.sources.public_lp_strategies.analytics_view import query_strategies_by_lp_quarter

results = query_strategies_by_lp_quarter(db, "CalPERS", 2025, "Q3")
for r in results:
    print(f"{r['program']}: {r['target_private_equity_pct']}% PE allocation")
```

### 2. Compare Private Equity Strategies Across LPs

```python
from app.sources.public_lp_strategies.analytics_view import query_strategies_by_program_quarter

results = query_strategies_by_program_quarter(db, "private_equity", 2025, "Q3")
for r in results:
    print(f"{r['lp_name']}: Target {r['target_private_equity_pct']}%, Current {r['current_private_equity_pct']}%")
```

### 3. Find All Strategies Focused on AI

```python
from app.sources.public_lp_strategies.analytics_view import query_strategies_with_theme

results = query_strategies_with_theme(db, "ai", fiscal_year=2025)
for r in results:
    print(f"{r['lp_name']} - {r['program']} ({r['fiscal_quarter']})")
```

### 4. Direct SQL - Custom Analysis

```python
from sqlalchemy import text

query = text("""
SELECT 
    lp_name,
    program,
    fiscal_quarter,
    CAST(current_private_equity_pct AS FLOAT) - CAST(target_private_equity_pct AS FLOAT) AS pe_over_under
FROM lp_strategy_quarterly_view
WHERE fiscal_year = 2025
  AND program = 'total_fund'
ORDER BY pe_over_under DESC
""")

results = db.execute(query).fetchall()
for row in results:
    print(f"{row.lp_name} is {row.pe_over_under:+.1f}% {'over' if row.pe_over_under > 0 else 'under'}weight PE")
```

---

## Supported LPs

Pre-configured in `config.py`:
- **CalPERS** (California Public Employees' Retirement System)
- **CalSTRS** (California State Teachers' Retirement System)
- **NYSCRF** (New York State Common Retirement Fund)
- **Texas TRS** (Teacher Retirement System of Texas)

To add more, update `KNOWN_LP_FUNDS` in `app/sources/public_lp_strategies/config.py`.

---

## Supported Programs

- `total_fund` - Entire fund strategy
- `private_equity` - PE/VC investments
- `real_estate` - Real estate investments
- `infrastructure` - Infrastructure investments
- `fixed_income` - Fixed income / bonds
- `public_equity` - Public stocks
- `hedge_funds` - Hedge fund allocations
- `cash` - Cash holdings
- `other` - Other asset classes

---

## Supported Themes

- `ai` - Artificial intelligence
- `energy_transition` - Clean energy, renewables
- `climate_resilience` - Climate adaptation
- `reshoring` - Manufacturing reshoring
- `healthcare` - Healthcare / biotech
- `technology` - Technology infrastructure
- `sustainability` - ESG / sustainable investing

---

## Database Tables

| Table | Purpose |
|-------|---------|
| `lp_fund` | LP fund metadata (CalPERS, CalSTRS, etc.) |
| `lp_document` | Strategy documents (PDFs, presentations) |
| `lp_document_text_section` | Parsed text chunks |
| `lp_strategy_snapshot` | Core strategy data per quarter |
| `lp_asset_class_target_allocation` | Asset allocation targets and actuals |
| `lp_asset_class_projection` | Forward-looking commitment plans |
| `lp_manager_or_vehicle_exposure` | Manager/fund-level positions |
| `lp_strategy_thematic_tag` | Investment themes |

---

## Analytics View

**`lp_strategy_quarterly_view`** - One row per (LP, program, fiscal_year, fiscal_quarter)

**Columns include:**
- Core identifiers (lp_name, program, fiscal_year, fiscal_quarter)
- Strategy summary (summary_text, risk_positioning, liquidity_profile)
- Pivoted allocations (target_private_equity_pct, current_private_equity_pct, etc.)
- Forward metrics (pe_commitment_plan_3y_amount, etc.)
- Theme flags (theme_ai, theme_energy_transition, etc.)

---

## What's NOT Implemented Yet

⚠️ **Document parsing pipeline:**
- PDF table extraction
- Text section identification
- Structured data extraction (NLP/LLM)

These are marked as TODOs in `app/sources/public_lp_strategies/normalize.py`.

For now, you must provide **pre-parsed data** in the job config.

---

## Troubleshooting

### Error: "Unknown LP fund"
**Solution:** Add the LP to `KNOWN_LP_FUNDS` in `config.py`

### Error: "Invalid program"
**Solution:** Use one of the valid programs from `VALID_PROGRAMS` in `config.py`

### Error: "Invalid asset_class"
**Solution:** Use one of the valid asset classes from `VALID_ASSET_CLASSES` in `config.py`

### No results from view
**Solution:** Ensure:
1. Tables created: `create_tables()` from `database.py`
2. View created: `create_analytics_view(db)` from `analytics_view.py`
3. Data ingested: Check job status is "success"

---

## Full Documentation

See **PUBLIC_LP_STRATEGIES_IMPLEMENTATION_SUMMARY.md** for complete technical details.


