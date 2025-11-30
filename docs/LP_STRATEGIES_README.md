# Public LP Strategies - Data Source Documentation

## Overview

The **public_lp_strategies** source ingests investment strategy data from public pension funds, university endowments, and sovereign wealth funds (collectively "Limited Partners" or "LPs"). This data includes asset allocation targets, private equity commitments, and investment themes.

## Use Cases

- Track institutional capital flows into private equity and alternatives
- Analyze asset allocation trends across pension types
- Monitor thematic investment adoption (AI, climate, technology)
- Benchmark your own LP strategy against peers
- Research geographic and institutional-type patterns

## Data Coverage (27 Institutional Investors)

### U.S. Public Pension Funds (15 funds)
- **California**: CalPERS
- **Florida**: Florida SBA
- **Washington**: WSIB
- **Ohio**: STRS Ohio, Ohio OPERS
- **Oregon**: Oregon PERS
- **Massachusetts**: Massachusetts PRIM
- **Illinois**: Illinois TRS
- **Pennsylvania**: Pennsylvania PSERS
- **New Jersey**: New Jersey Division of Investment
- **North Carolina**: North Carolina Retirement Systems
- **Wisconsin**: Wisconsin SWIB
- **Virginia**: Virginia Retirement System

### University Endowments (3 funds)
- **Harvard**: Harvard Management Company
- **Yale**: Yale Investments Office
- **Stanford**: Stanford Management Company

### International (4 sovereign wealth funds)
- **Norway**: Norway GPFG (Government Pension Fund Global)
- **Singapore**: GIC Singapore
- **UAE**: ADIA (Abu Dhabi Investment Authority)
- **New Zealand**: New Zealand Super Fund

### Canadian Pensions (4 funds - world-class sophistication)
- **Canada**: CPP Investments (CPPIB)
- **Ontario**: Ontario Teachers' Pension Plan (OTPP)
- **Ontario**: OMERS (Ontario Municipal Employees)
- **Quebec**: CDPQ (Caisse de dépôt et placement du Québec)

### European Pensions (1 fund)
- **Netherlands**: Dutch ABP (Europe's largest pension)

### Asia-Pacific Funds (2 funds)
- **Australia**: AustralianSuper (largest Australian super fund)
- **Australia**: Future Fund (Australian sovereign wealth fund)

## Database Schema

### Core Tables

#### 1. `lp_fund`
Master table of institutional investors.

**Columns:**
- `id` - Primary key
- `name` - Short name (e.g., "CalPERS")
- `formal_name` - Full legal name
- `lp_type` - Type: `public_pension`, `endowment`, `sovereign_wealth`
- `jurisdiction` - State/country code
- `website_url` - Official website
- `created_at` - Timestamp

**Example:**
```sql
SELECT * FROM lp_fund WHERE name = 'CalPERS';
```

#### 2. `lp_document`
Source documents containing strategy data.

**Columns:**
- `id` - Primary key
- `lp_id` - FK to `lp_fund`
- `title` - Document title
- `document_type` - Type: `quarterly_investment_report`, `investment_committee_presentation`, etc.
- `program` - Program: `private_equity`, `real_estate`, `total_fund`, etc.
- `fiscal_year` - Year (e.g., 2025)
- `fiscal_quarter` - Quarter: Q1, Q2, Q3, Q4
- `report_period_start` - Start date
- `report_period_end` - End date
- `source_url` - URL to document
- `file_format` - Format: pdf, pptx, html
- `raw_file_location` - Optional storage path
- `ingested_at` - Timestamp

#### 3. `lp_strategy_snapshot`
Normalized strategy per LP/program/quarter.

**Columns:**
- `id` - Primary key
- `lp_id` - FK to `lp_fund`
- `program` - Program name
- `fiscal_year` - Year
- `fiscal_quarter` - Quarter
- `strategy_date` - Effective date
- `primary_document_id` - FK to `lp_document`
- `summary_text` - High-level strategy summary
- `risk_positioning` - Risk stance: `risk_on`, `neutral`, `defensive`
- `liquidity_profile` - Liquidity description
- `tilt_description` - Strategy tilts

**Unique constraint:** `(lp_id, program, fiscal_year, fiscal_quarter)`

#### 4. `lp_asset_class_target_allocation`
Asset class allocations (target vs. current).

**Columns:**
- `id` - Primary key
- `strategy_id` - FK to `lp_strategy_snapshot`
- `asset_class` - Asset class: `private_equity`, `public_equity`, `fixed_income`, `real_estate`, `infrastructure`, etc.
- `target_weight_pct` - Target allocation %
- `min_weight_pct` - Minimum allowed %
- `max_weight_pct` - Maximum allowed %
- `current_weight_pct` - Current allocation %
- `benchmark_weight_pct` - Benchmark %
- `source_section_id` - Optional FK to text section

#### 5. `lp_asset_class_projection`
Forward-looking commitments and projections.

**Columns:**
- `id` - Primary key
- `strategy_id` - FK to `lp_strategy_snapshot`
- `asset_class` - Asset class
- `projection_horizon` - Horizon: `1_year`, `3_year`, `5_year`
- `net_flow_projection_amount` - Projected net flows (USD)
- `commitment_plan_amount` - Planned commitments (USD)
- `expected_return_pct` - Expected return %
- `expected_volatility_pct` - Expected volatility %

#### 6. `lp_strategy_thematic_tag`
Investment themes per strategy.

**Columns:**
- `id` - Primary key
- `strategy_id` - FK to `lp_strategy_snapshot`
- `theme` - Theme: `ai`, `climate_resilience`, `energy_transition`, `technology`, `healthcare`, `infrastructure`, `sustainability`, `reshoring`
- `relevance_score` - Score (0.0 to 1.0)
- `source_section_id` - Optional FK to text section

#### 7. `lp_document_text_section`
Parsed text sections from documents (for search/NLP).

**Columns:**
- `id` - Primary key
- `document_id` - FK to `lp_document`
- `section_name` - Section name
- `page_start` - Start page
- `page_end` - End page
- `sequence_order` - Order within document
- `text` - Section text content
- `embedding_vector` - Optional vector embedding (JSONB placeholder)
- `language` - Language code

#### 8. `lp_manager_or_vehicle_exposure`
Manager/fund-level exposures (optional, not heavily used yet).

**Columns:**
- `id` - Primary key
- `strategy_id` - FK to `lp_strategy_snapshot`
- `manager_name` - Manager name
- `vehicle_name` - Vehicle name
- `vehicle_type` - Type: `separate_account`, `commingled`, `co_invest`
- `asset_class` - Asset class
- `market_value_amount` - Market value (USD)
- `weight_pct` - Weight %
- `status` - Status: `active`, `redeeming`, `new_commitment`
- `geo_region` - Geographic region
- `sector_focus` - Sector focus

## Querying the Database

### Example 1: Get All Funds
```sql
SELECT 
    name,
    formal_name,
    lp_type,
    jurisdiction,
    website_url
FROM lp_fund
ORDER BY lp_type, name;
```

### Example 2: Top 10 by PE Allocation
```sql
SELECT 
    f.name,
    f.lp_type,
    a.current_weight_pct AS pe_allocation,
    p.commitment_plan_amount AS pe_3y_commitment
FROM lp_fund f
JOIN lp_strategy_snapshot s ON f.id = s.lp_id
JOIN lp_asset_class_target_allocation a 
    ON s.id = a.strategy_id AND a.asset_class = 'private_equity'
LEFT JOIN lp_asset_class_projection p 
    ON s.id = p.strategy_id AND p.asset_class = 'private_equity' AND p.projection_horizon = '3_year'
ORDER BY a.current_weight_pct DESC
LIMIT 10;
```

### Example 3: Investment Theme Analysis
```sql
SELECT 
    t.theme,
    COUNT(DISTINCT s.lp_id) AS fund_count,
    ROUND(AVG(t.relevance_score) * 100, 0) AS avg_relevance_pct
FROM lp_strategy_thematic_tag t
JOIN lp_strategy_snapshot s ON t.strategy_id = s.id
GROUP BY t.theme
ORDER BY fund_count DESC, avg_relevance_pct DESC;
```

### Example 4: Risk Positioning by Type
```sql
SELECT 
    f.lp_type,
    s.risk_positioning,
    COUNT(*) AS fund_count
FROM lp_fund f
JOIN lp_strategy_snapshot s ON f.id = s.lp_id
GROUP BY f.lp_type, s.risk_positioning
ORDER BY f.lp_type, s.risk_positioning;
```

### Example 5: Endowments vs. Public Pensions
```sql
SELECT 
    f.lp_type,
    COUNT(DISTINCT f.id) AS fund_count,
    ROUND(AVG(a.target_weight_pct), 1) AS avg_pe_target,
    ROUND(AVG(a.current_weight_pct), 1) AS avg_pe_current,
    SUM(p.commitment_plan_amount) AS total_3y_commitments
FROM lp_fund f
JOIN lp_strategy_snapshot s ON f.id = s.lp_id
JOIN lp_asset_class_target_allocation a 
    ON s.id = a.strategy_id AND a.asset_class = 'private_equity'
LEFT JOIN lp_asset_class_projection p 
    ON s.id = p.strategy_id AND p.asset_class = 'private_equity' AND p.projection_horizon = '3_year'
GROUP BY f.lp_type
ORDER BY avg_pe_current DESC;
```

## Python API Usage

### Register a Fund
```python
from app.sources.public_lp_strategies.ingest import register_lp_fund
from app.sources.public_lp_strategies.types import LpFundInput

fund_input = LpFundInput(
    name="CalPERS",
    formal_name="California Public Employees' Retirement System",
    lp_type="public_pension",
    jurisdiction="CA",
    website_url="https://www.calpers.ca.gov/"
)
fund = register_lp_fund(db, fund_input)
```

### Register a Document
```python
from app.sources.public_lp_strategies.ingest import register_lp_document
from app.sources.public_lp_strategies.types import LpDocumentInput
from datetime import date

doc_input = LpDocumentInput(
    lp_id=fund.id,
    title="Q3 2025 Investment Report",
    document_type="quarterly_investment_report",
    program="private_equity",
    fiscal_year=2025,
    fiscal_quarter="Q3",
    report_period_start=date(2025, 7, 1),
    report_period_end=date(2025, 9, 30),
    source_url="https://calpers.ca.gov/reports/q3-2025.pdf",
    file_format="pdf"
)
document = register_lp_document(db, doc_input)
```

### Upsert Strategy Snapshot
```python
from app.sources.public_lp_strategies.ingest import upsert_strategy_snapshot
from app.sources.public_lp_strategies.types import StrategySnapshotInput

strategy_input = StrategySnapshotInput(
    lp_id=fund.id,
    program="private_equity",
    fiscal_year=2025,
    fiscal_quarter="Q3",
    strategy_date=date(2025, 10, 15),
    primary_document_id=document.id,
    summary_text="Increasing PE allocation, overweight technology",
    risk_positioning="risk_on"
)
strategy = upsert_strategy_snapshot(db, strategy_input)
```

### Add Asset Allocations
```python
from app.sources.public_lp_strategies.ingest import upsert_asset_class_allocations
from app.sources.public_lp_strategies.types import AssetClassAllocationInput

allocations = [
    AssetClassAllocationInput(
        asset_class="private_equity",
        target_weight_pct=26.0,
        current_weight_pct=27.5
    ),
    AssetClassAllocationInput(
        asset_class="public_equity",
        target_weight_pct=40.0,
        current_weight_pct=38.5
    ),
]
upsert_asset_class_allocations(db, strategy.id, allocations)
```

### Add Projections
```python
from app.sources.public_lp_strategies.ingest import upsert_asset_class_projections
from app.sources.public_lp_strategies.types import AssetClassProjectionInput

projections = [
    AssetClassProjectionInput(
        asset_class="private_equity",
        projection_horizon="3_year",
        commitment_plan_amount=15_000_000_000.0,  # $15B
        expected_return_pct=12.0
    ),
]
upsert_asset_class_projections(db, strategy.id, projections)
```

### Add Thematic Tags
```python
from app.sources.public_lp_strategies.ingest import upsert_thematic_tags
from app.sources.public_lp_strategies.types import ThematicTagInput

tags = [
    ThematicTagInput(theme="ai", relevance_score=0.85),
    ThematicTagInput(theme="technology", relevance_score=0.80),
    ThematicTagInput(theme="climate_resilience", relevance_score=0.75),
]
upsert_thematic_tags(db, strategy.id, tags)
```

## Loading Sample Data

To load the complete sample dataset (27 funds):

```bash
python load_lp_sample_data.py
```

The dataset includes comprehensive Q3 2025 data for all 27 funds across 4 continents.

## Key Statistics (Q3 2025 Sample Data)

### Total Coverage
- **27 Institutional Investors**
- **21 Geographic Jurisdictions**
- **$482.5 Billion** in 3-year PE commitments (estimated)
- **Average PE Allocation:** ~22%
- **4 Continents:** North America, Europe, Asia-Pacific, Middle East

### By Type
| Type | Count | Geographic Spread | Notable Characteristics |
|------|-------|-------------------|------------------------|
| Public Pensions | 20 | U.S., Canada, Netherlands, Australia | Largest group, diverse allocations |
| Endowments | 3 | U.S. (Ivy League) | Highest PE allocations (36-41%) |
| Sovereign Wealth | 5 | Norway, Singapore, UAE, NZ, Australia | Large scale, transparency varies |

### Top 7 by PE Allocation
1. **Yale** - 41.2% (Endowment)
2. **Stanford** - 38.8% (Endowment)
3. **Harvard** - 36.5% (Endowment)
4. **CPP Investments** - 30.5% (Canada)
5. **Ontario Teachers** - 28.2% (Canada)
6. **CDPQ** - 27.2% (Canada)
7. **CalPERS** - 27.5% (U.S.)

### Top 7 by Absolute Commitments
1. **CPP Investments** - $40B (Canada) 🥇
2. **CDPQ** - $38B (Canada)
3. **GIC Singapore** - $35B
4. **Ontario Teachers** - $32B (Canada)
5. **ADIA** - $30B (UAE)
6. **Dutch ABP** - $28B (Netherlands)
7. **Norway GPFG** - $25B

### Investment Themes
- **AI:** ~90% adoption (widespread across all types)
- **Infrastructure:** Strong focus in Canadian and Australian funds
- **Climate/Sustainability:** Led by European (Dutch ABP) and Nordic (Norway) funds
- **Technology:** Core focus for endowments and North American pensions
- **Healthcare:** Secondary theme for many U.S. pensions

## Future Enhancements

### Potential Extensions
1. **More LPs**: 40+ additional funds available in `EXTERNAL_DATA_SOURCES.md` (Swedish AP funds, more Asian funds, etc.)
2. **Time Series**: Track allocation changes over multiple quarters
3. **Manager Data**: Expand `lp_manager_or_vehicle_exposure` table
4. **Real Document Parsing**: Add PDF/PPTX extraction pipeline
5. **NLP/LLM Integration**: Auto-extract strategies from documents
6. **Analytics Dashboard**: Build Grafana/Superset dashboards
7. **API Endpoints**: Expose REST endpoints for querying strategies

### Compliance Notes
- **Data Sources**: Public documents only (investment reports, board presentations)
- **No Scraping**: Data is manually seeded from known public URLs
- **No PII**: Only institutional-level data, no individual managers/holdings beyond what's publicly disclosed
- **Licensing**: Public pension and endowment data is typically in the public domain

## Source Code Location

```
app/sources/public_lp_strategies/
├── __init__.py           # Package initialization
├── config.py             # Enums and KNOWN_LP_FUNDS registry
├── types.py              # Pydantic models for input/validation
├── ingest.py             # Database ingestion functions
├── normalize.py          # Placeholder for document parsing
└── analytics_view.py     # SQL view for quarterly analytics
```

## Tests

Run tests:
```bash
pytest tests/test_public_lp_strategies.py -v
```

Tests cover:
- Model creation and relationships
- Idempotent upserts
- Foreign key constraints
- Unique constraints
- Analytics view functionality

## Support

For questions or issues:
- Review `RULES.md` for general ingestion service guidelines
- Check `EXTERNAL_DATA_SOURCES.md` for available sources
- See `tests/test_public_lp_strategies.py` for usage examples

