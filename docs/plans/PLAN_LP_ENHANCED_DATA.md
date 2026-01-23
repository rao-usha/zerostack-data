# Plan: Enhanced LP Data Collection Features

## Goal
Expand LP data collection to capture holdings, performance, governance, and manager relationships from additional public sources.

---

## Priority 1: SEC 13F Holdings Tracker

### Overview
Track quarterly institutional holdings from SEC 13F filings. Applies to institutional investors managing $100M+ in US equities.

### Data Source
- **SEC EDGAR API**: `https://data.sec.gov/submissions/CIK{cik}.json`
- **13F Holdings**: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`

### New Database Tables

```sql
-- Holdings from 13F filings
CREATE TABLE lp_holding (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER REFERENCES lp_fund(id),
    filing_date DATE NOT NULL,
    report_period DATE NOT NULL,

    -- Security info
    cusip VARCHAR(9),
    issuer_name VARCHAR(500),
    security_class VARCHAR(50),

    -- Position
    shares BIGINT,
    value_usd DECIMAL(18,2),

    -- Change tracking
    shares_change BIGINT,
    is_new_position BOOLEAN DEFAULT FALSE,
    is_closed_position BOOLEAN DEFAULT FALSE,

    source_url VARCHAR(500),
    collected_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(lp_id, report_period, cusip)
);

-- Filing metadata
CREATE TABLE lp_13f_filing (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER REFERENCES lp_fund(id),
    cik VARCHAR(10) NOT NULL,
    accession_number VARCHAR(25) NOT NULL UNIQUE,
    filing_date DATE,
    report_period DATE,
    total_value_usd DECIMAL(18,2),
    holdings_count INTEGER,
    processed_at TIMESTAMP,
    raw_json JSONB
);
```

### New Files
```
app/sources/lp_collection/
├── sec_13f_source.py     # 13F filing collector
└── holdings_parser.py    # Parse 13F XML/JSON
```

### API Endpoints
```
GET  /api/v1/lp-collection/holdings/{lp_id}
GET  /api/v1/lp-collection/holdings/{lp_id}/changes
POST /api/v1/lp-collection/collect-13f/{lp_id}
```

---

## Priority 2: Historical Strategy Snapshots

### Overview
Track LP allocation changes over time to understand strategy evolution.

### New Database Table

```sql
CREATE TABLE lp_strategy_snapshot (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER REFERENCES lp_fund(id),
    snapshot_date DATE NOT NULL,
    fiscal_year INTEGER,

    -- Total assets
    total_aum_usd DECIMAL(18,2),

    -- Allocation percentages
    public_equity_pct DECIMAL(5,2),
    fixed_income_pct DECIMAL(5,2),
    private_equity_pct DECIMAL(5,2),
    real_estate_pct DECIMAL(5,2),
    real_assets_pct DECIMAL(5,2),
    hedge_funds_pct DECIMAL(5,2),
    cash_pct DECIMAL(5,2),
    other_pct DECIMAL(5,2),

    -- Performance
    one_year_return_pct DECIMAL(6,2),
    three_year_return_pct DECIMAL(6,2),
    five_year_return_pct DECIMAL(6,2),
    ten_year_return_pct DECIMAL(6,2),
    since_inception_return_pct DECIMAL(6,2),

    -- Benchmark comparison
    benchmark_name VARCHAR(100),
    benchmark_return_pct DECIMAL(6,2),

    -- Source
    source_type VARCHAR(50),
    source_url VARCHAR(500),
    collected_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(lp_id, snapshot_date)
);
```

### Collection Logic
- Parse CAFRs for annual allocation/performance data
- Scrape website "Investment" pages for current targets
- Extract from press releases and news

---

## Priority 3: CAFR Document Parser

### Overview
Extract structured data from Comprehensive Annual Financial Reports (PDF parsing).

### Data Extracted
1. **Asset Allocation** - Current vs target percentages
2. **Performance** - 1/3/5/10 year returns
3. **Manager List** - External investment managers
4. **Fees** - Management and performance fees
5. **Contributions/Distributions** - Cash flows

### New Files
```
app/sources/lp_collection/
├── cafr_parser.py        # PDF text extraction
├── cafr_extractor.py     # AI-powered data extraction
└── templates/            # CAFR structure templates by LP
```

### Implementation
1. Download CAFR PDFs (already have document links)
2. Extract text with PyPDF2 or pdfplumber
3. Use LLM to extract structured data
4. Validate and store in strategy_snapshot table

---

## Priority 4: Manager Relationship Tracking

### Overview
Track which GPs/managers each LP has committed to.

### New Database Tables

```sql
-- Investment managers used by LPs
CREATE TABLE lp_manager_relationship (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER REFERENCES lp_fund(id),

    -- Manager info
    manager_name VARCHAR(500) NOT NULL,
    manager_type VARCHAR(50), -- 'gp', 'hedge_fund', 'real_estate', 'public_equity'

    -- Relationship
    relationship_type VARCHAR(50), -- 'commitment', 'mandate', 'co-investment'
    asset_class VARCHAR(50),

    -- Size (if disclosed)
    commitment_usd DECIMAL(18,2),

    -- Dates
    first_observed_date DATE,
    last_observed_date DATE,
    is_active BOOLEAN DEFAULT TRUE,

    -- Source
    source_type VARCHAR(50),
    source_url VARCHAR(500),
    collected_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(lp_id, manager_name, asset_class)
);
```

### Data Sources
- CAFR manager lists
- Press releases announcing commitments
- SEC Form ADV Schedule D (advisory clients)

---

## Priority 5: Governance Data

### Overview
Track board members, committees, and governance structure.

### New Database Tables

```sql
-- Board/committee members
CREATE TABLE lp_governance_member (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER REFERENCES lp_fund(id),

    -- Person
    full_name VARCHAR(200) NOT NULL,
    title VARCHAR(200),

    -- Role
    governance_role VARCHAR(100), -- 'board_member', 'trustee', 'committee_chair'
    committee_name VARCHAR(200),

    -- Tenure
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,

    -- Additional info
    representing VARCHAR(200), -- e.g., "State Treasurer", "Retiree Representative"
    bio_url VARCHAR(500),

    source_url VARCHAR(500),
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Board meetings and minutes
CREATE TABLE lp_board_meeting (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER REFERENCES lp_fund(id),
    meeting_date DATE NOT NULL,
    meeting_type VARCHAR(50), -- 'regular', 'special', 'investment_committee'
    agenda_url VARCHAR(500),
    minutes_url VARCHAR(500),
    video_url VARCHAR(500),
    collected_at TIMESTAMP DEFAULT NOW()
);
```

---

## Implementation Order

### Phase 1: Historical Strategy Snapshots (Quick Win)
1. Add `lp_strategy_snapshot` table
2. Extend CAFR collector to parse allocation data
3. Add website collector for current allocation pages
4. Create snapshot history API

### Phase 2: SEC 13F Holdings
1. Add holding tables
2. Create 13F collector with SEC EDGAR API
3. Add change tracking logic
4. Create holdings API endpoints

### Phase 3: Manager Relationships
1. Add relationship table
2. Extract from CAFR manager lists
3. Parse press releases for commitments
4. Create relationship API

### Phase 4: Governance
1. Add governance tables
2. Extend website collector for board pages
3. Add board meeting document links
4. Create governance API

---

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/core/models.py` | Modify | Add new tables |
| `app/sources/lp_collection/sec_13f_source.py` | Create | 13F collector |
| `app/sources/lp_collection/cafr_parser.py` | Create | PDF parsing |
| `app/sources/lp_collection/strategy_extractor.py` | Create | Allocation extraction |
| `app/api/v1/lp_collection.py` | Modify | Add new endpoints |
| `app/data/` | Modify | Add manager name mappings |

---

## Verification

1. **Strategy Snapshots**: Collect CalPERS, verify allocation history populated
2. **13F Holdings**: Collect for LP with CIK, verify holdings imported
3. **Manager Relationships**: Parse CAFR, verify managers extracted
4. **API**: Test all new endpoints return correct data

---

## Approved: [ ]
