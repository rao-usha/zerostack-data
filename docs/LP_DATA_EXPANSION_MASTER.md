# LP Intelligence Platform: Comprehensive Data Expansion Plan

**Version:** 1.0
**Date:** 2026-01-27
**Status:** MASTER REFERENCE DOCUMENT

---

## 1. Executive Summary

### Current State Dashboard

| Metric | Current | Target | Gap |
|--------|---------|--------|-----|
| **LP Tables Defined** | 15 | 22 | 7 new tables needed |
| **Tables with Data** | ~8 | 22 | 14 empty/critical |
| **Collectors Active** | 8 | 12+ | 4 new collectors |
| **Total LP Records** | 564 | 2,000+ | Scale 3.5x |
| **Data Points per LP** | ~15 | 50+ | Expand 3x |

### Gap Analysis Heat Map

| Table | Status | Rows | Priority |
|-------|--------|------|----------|
| lp_fund | :green_circle: Healthy | 564 | - |
| lp_key_contact | :green_circle: Healthy | 3,173 | - |
| portfolio_companies | :green_circle: Healthy | 5,236 | - |
| lp_asset_class_target_allocation | :yellow_circle: Partial | 106 | P2 |
| lp_governance_member | :yellow_circle: Low | 35 | P2 |
| lp_performance_return | :red_circle: CRITICAL | 1 | P1 |
| lp_13f_holding | :red_circle: EMPTY | 0 | P1 |
| lp_manager_commitment | :red_circle: EMPTY | 0 | P1 |
| lp_pension_metrics | :red_circle: EMPTY | 0 | P2 |
| lp_aum_history | :red_circle: EMPTY | 0 | P2 |
| lp_allocation_history | :red_circle: EMPTY | 0 | P2 |
| lp_board_meeting | :red_circle: EMPTY | 0 | P3 |
| lp_manager_or_vehicle_exposure | :red_circle: EMPTY | 0 | P3 |
| lp_consultant | :red_circle: NOT DEFINED | 0 | P3 |

### Top 3 Priorities

1. **SEC 13F Holdings (lp_13f_holding)** - Institutional holdings for 30+ major LPs with CIKs
2. **Performance Returns (lp_performance_return)** - Historical returns from CAFRs/websites
3. **Manager Commitments (lp_manager_commitment)** - GP relationships from CAFRs

### Implementation Timeline

```
Phase 1 (Days 1-5):   13F Holdings + Performance + Manager Commitments
Phase 2 (Days 6-10):  Pension Metrics + Allocations + AUM History
Phase 3 (Days 11-13): Governance + Board Meetings + Consultants
```

---

## 2. Current State Analysis

### 2.1 Database Coverage Matrix

#### Core LP Tables (Existing & Healthy)

| Table | Rows | Purpose | Source |
|-------|------|---------|--------|
| `lp_fund` | 564 | Core LP entities | Registry seeding |
| `lp_key_contact` | 3,173 | Investment contacts | SEC ADV, Website |
| `lp_document` | ~200 | Strategy documents | Website crawl |
| `portfolio_companies` | 5,236 | Portfolio holdings | 13F, News, Website |

#### Strategy Tables (Existing, Partial Data)

| Table | Rows | Purpose | Gap |
|-------|------|---------|-----|
| `lp_strategy_snapshot` | ~50 | Point-in-time strategies | Need 500+ |
| `lp_asset_class_target_allocation` | 106 | Target allocations | Need 2,000+ |
| `lp_asset_class_projection` | ~20 | Forward projections | Need 500+ |
| `lp_strategy_thematic_tag` | ~30 | Investment themes | Need 1,000+ |

#### Critical Empty Tables

| Table | Rows | Required | Status |
|-------|------|----------|--------|
| `lp_performance_return` | 1 | 5,000+ | Schema exists, no data flow |
| `lp_governance_member` | 35 | 500+ | Collector exists, low yield |
| `lp_board_meeting` | 0 | 500+ | Schema exists, no data flow |
| `lp_manager_or_vehicle_exposure` | 0 | 2,000+ | Schema exists, no data flow |

#### Tables NOT YET DEFINED

| Table | Required | Purpose |
|-------|----------|---------|
| `lp_13f_holding` | 10,000+ | SEC 13F institutional holdings |
| `lp_manager_commitment` | 1,000+ | GP/manager relationships |
| `lp_pension_metrics` | 2,000+ | Funded ratio, contributions |
| `lp_aum_history` | 5,000+ | Historical AUM tracking |
| `lp_allocation_history` | 3,000+ | Historical allocations |
| `lp_consultant` | 300+ | Investment consultants |

### 2.2 Collector Status

#### Active Collectors (8)

| Collector | File | Target Tables | Status |
|-----------|------|---------------|--------|
| Website | `website_source.py` | lp_fund, lp_document, lp_key_contact | Active |
| SEC ADV | `sec_adv_source.py` | lp_key_contact, lp_fund | Active |
| SEC 13F | `sec_13f_source.py` | (lp_13f_holding) | Collector exists, table missing |
| Form 990 | `form_990_source.py` | strategy_snapshot | Active |
| CAFR | `cafr_parser.py` | performance, allocation, managers | Partial |
| News | `news_source.py` | portfolio_companies | Active |
| Governance | `governance_source.py` | lp_governance_member | Active, low yield |
| Performance | `performance_source.py` | lp_performance_return | Active, low yield |

#### Missing Collectors

| Collector | Target Tables | Source |
|-----------|---------------|--------|
| State Portal | pension_metrics, allocations | State CAFR portals |
| Press Release | manager_commitments | LP press releases |
| NASRA | pension_metrics | NASRA database |
| Form 5500 | pension_metrics | DOL EFAST |

---

## 3. Empty Table Deep Dive

### 3.1 lp_13f_holding (PRIORITY 1)

**Purpose:** Track quarterly institutional holdings from SEC 13F filings

**Why Critical:**
- Provides concrete, verified position data
- Shows actual investment decisions vs stated allocations
- Enables tracking of portfolio changes over time

**Current State:**
- Collector exists: `app/sources/lp_collection/sec_13f_source.py`
- 30+ LPs have known CIKs in collector
- Table schema NOT defined in models.py

**Root Cause:** Table was planned but not created; data extraction works but has no destination

**Schema Required:**
```sql
CREATE TABLE lp_13f_holding (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    report_date DATE NOT NULL,
    cusip VARCHAR(9),
    issuer_name VARCHAR(500),
    security_class VARCHAR(50),
    shares BIGINT,
    value_usd DECIMAL(18,2),
    filing_accession VARCHAR(25),
    filing_date DATE,
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_lp_13f_lp_date ON lp_13f_holding(lp_id, report_date);
```

**Implementation Steps:**
1. Add model to `app/core/models.py`
2. Run Alembic migration
3. Update 13F collector to save to table
4. Run collection for LPs with known CIKs

**LPs with Known CIKs (from `sec_13f_source.py`):**
- CalPERS (0001067983)
- CalSTRS (0001084267)
- NY State Common (0001030717)
- Texas TRS (0000917954)
- Florida SBA (0001053507)
- Yale (0001056666)
- Harvard (0001082339)
- Gates Foundation (0001166559)
- Norway GPFG (0001273515)
- GIC (0001277537)
- Plus 20+ more

**Effort:** 2 days
**Business Value:** HIGH - Verified holdings data

---

### 3.2 lp_performance_return (PRIORITY 1)

**Purpose:** Historical investment returns (1/3/5/10 year) with benchmarks

**Why Critical:**
- Core metric for LP quality assessment
- Enables performance-based filtering
- Provides benchmark comparison context

**Current State:**
- Table exists: `app/core/models.py:706`
- Only 1 row in database
- Collector exists: `app/sources/lp_collection/performance_source.py`
- CAFR parser extracts performance but data not saved

**Root Cause:**
1. Website collector finds few performance pages (most behind login)
2. CAFR parser extracts data but pipeline incomplete
3. No systematic collection of Form 990 endowment returns

**Schema (Already Defined):**
```python
class LpPerformanceReturn(Base):
    __tablename__ = "lp_performance_return"
    id, lp_id, fiscal_year, period_end_date
    one_year_return_pct, three_year_return_pct, five_year_return_pct
    ten_year_return_pct, twenty_year_return_pct, since_inception_return_pct
    benchmark_name, benchmark_one_year_pct, benchmark_three_year_pct...
    value_added_one_year_bps, value_added_five_year_bps
    total_fund_value_usd, net_cash_flow_usd
    source_type, source_url, collected_at
```

**Implementation Steps:**
1. Fix CAFR parser to save performance data to table
2. Add Form 990 -> performance pipeline
3. Curate list of LPs with public performance pages
4. Manual entry for top 50 LPs as seed data

**Data Sources:**
| Source | LPs Covered | Reliability |
|--------|-------------|-------------|
| CAFR PDFs | ~200 US public pensions | High |
| Form 990 | ~100 endowments/foundations | High |
| LP Websites | ~50 with public data | Medium |
| NASRA | ~150 public pensions | High |

**Effort:** 3 days
**Business Value:** HIGH - Core investment metric

---

### 3.3 lp_manager_commitment (PRIORITY 1)

**Purpose:** Track LP relationships with GPs/managers

**Why Critical:**
- Shows which GPs an LP has committed to
- Enables GP-to-LP relationship mapping
- Reveals investment preferences by manager type

**Current State:**
- Table NOT defined in models.py
- CAFR parser extracts manager names but no destination
- No systematic commitment tracking

**Root Cause:** Table never created; data available in CAFRs but pipeline incomplete

**Schema Required:**
```sql
CREATE TABLE lp_manager_commitment (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    manager_name VARCHAR(500) NOT NULL,
    fund_name VARCHAR(500),
    vintage_year INTEGER,
    commitment_usd DECIMAL(18,2),
    asset_class VARCHAR(50),  -- pe, re, hedge, credit
    status VARCHAR(50),  -- active, fully_invested, harvesting
    first_observed DATE,
    last_observed DATE,
    source_type VARCHAR(50),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_lp_manager_lp ON lp_manager_commitment(lp_id);
CREATE INDEX idx_lp_manager_name ON lp_manager_commitment(manager_name);
```

**Implementation Steps:**
1. Add model to `app/core/models.py`
2. Run Alembic migration
3. Update CAFR parser to save manager relationships
4. Add press release parser for commitment announcements
5. Seed with manual data for top 50 LPs

**Data Sources:**
- CAFR "External Managers" sections
- LP press releases
- GP Form ADV Schedule D (advisory clients)
- News articles on commitments

**Effort:** 3 days
**Business Value:** HIGH - Relationship intelligence

---

### 3.4 lp_pension_metrics (PRIORITY 2)

**Purpose:** Pension-specific health metrics (funded ratio, contributions)

**Why Critical:**
- Indicates LP financial health
- Predicts commitment capacity
- Flags LPs under stress

**Current State:**
- Table NOT defined in models.py
- Data available in CAFRs but not extracted

**Root Cause:** Pension-specific metrics not prioritized in initial build

**Schema Required:**
```sql
CREATE TABLE lp_pension_metrics (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    fiscal_year INTEGER NOT NULL,
    funded_ratio_pct DECIMAL(5,2),
    unfunded_liability_usd DECIMAL(18,2),
    actuarial_return_assumption DECIMAL(5,2),
    employer_contribution_usd DECIMAL(18,2),
    employee_contribution_usd DECIMAL(18,2),
    benefit_payments_usd DECIMAL(18,2),
    active_members INTEGER,
    retired_members INTEGER,
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(lp_id, fiscal_year)
);
```

**Implementation Steps:**
1. Add model to `app/core/models.py`
2. Extend CAFR parser with pension-specific extraction
3. Create NASRA collector for aggregate pension data
4. Collect for ~250 US public pensions

**Effort:** 3 days
**Business Value:** MEDIUM-HIGH - Pension health indicators

---

### 3.5 lp_allocation_history (PRIORITY 2)

**Purpose:** Track asset allocation changes over time

**Why Critical:**
- Shows allocation trends and shifts
- Indicates strategy changes
- Enables time-series analysis

**Current State:**
- Table NOT defined in models.py
- Current allocation captured in `lp_asset_class_target_allocation` but tied to strategy_snapshot
- No historical tracking

**Root Cause:** Only current allocations tracked, not historical

**Schema Required:**
```sql
CREATE TABLE lp_allocation_history (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    fiscal_year INTEGER NOT NULL,
    allocation_type VARCHAR(20),  -- 'actual' or 'target'
    public_equity_pct DECIMAL(5,2),
    fixed_income_pct DECIMAL(5,2),
    private_equity_pct DECIMAL(5,2),
    real_estate_pct DECIMAL(5,2),
    real_assets_pct DECIMAL(5,2),
    hedge_funds_pct DECIMAL(5,2),
    cash_pct DECIMAL(5,2),
    other_pct DECIMAL(5,2),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(lp_id, fiscal_year, allocation_type)
);
```

**Implementation Steps:**
1. Add model to `app/core/models.py`
2. Update CAFR parser to save allocation history
3. Backfill from existing strategy snapshots
4. Historical collection from CAFR archives

**Effort:** 2 days
**Business Value:** MEDIUM - Trend analysis

---

### 3.6 lp_aum_history (PRIORITY 2)

**Purpose:** Track total AUM over time

**Why Critical:**
- Shows fund growth/decline
- Indicates commitment capacity
- Enables AUM-based filtering

**Current State:**
- Table NOT defined in models.py
- Current AUM in `lp_fund.aum_usd_billions` but no history

**Root Cause:** Only current AUM stored, not historical

**Schema Required:**
```sql
CREATE TABLE lp_aum_history (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    fiscal_year INTEGER NOT NULL,
    total_aum_usd DECIMAL(18,2),
    source_type VARCHAR(50),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(lp_id, fiscal_year)
);
```

**Implementation Steps:**
1. Add model to `app/core/models.py`
2. Update CAFR parser to save AUM history
3. Seed from Form 990 (endowments)
4. Seed from current `lp_fund.aum_usd_billions` for Year 0

**Effort:** 1 day
**Business Value:** MEDIUM - Growth tracking

---

### 3.7 lp_board_meeting (PRIORITY 3)

**Purpose:** Board and committee meeting records with documents

**Current State:**
- Table exists: `app/core/models.py:658`
- 0 rows in database
- Governance collector extracts meetings but data not saved

**Root Cause:** Meeting extraction logic exists but save pipeline incomplete

**Schema (Already Defined):**
```python
class LpBoardMeeting(Base):
    __tablename__ = "lp_board_meeting"
    id, lp_id, meeting_date, meeting_type
    meeting_title, agenda_url, minutes_url
    materials_url, video_url
    summary_text, key_decisions
    source_url, collected_at
```

**Implementation Steps:**
1. Fix governance collector to save meeting data
2. Run collection for LPs with public meeting portals
3. ~100 US public pensions have public board meetings

**Effort:** 1 day
**Business Value:** LOW-MEDIUM - Meeting intelligence

---

### 3.8 lp_consultant (PRIORITY 3)

**Purpose:** Track investment consultant relationships

**Why Useful:**
- Identifies consultant influence
- Maps LP-consultant networks
- Indicates LP sophistication

**Current State:**
- Table NOT defined in models.py
- Consultant names appear in CAFRs but not extracted

**Schema Required:**
```sql
CREATE TABLE lp_consultant (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    consultant_name VARCHAR(500) NOT NULL,
    consultant_type VARCHAR(100),  -- general, pe, real_estate, hedge_fund
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);
```

**Major Consultants to Track:**
- Cambridge Associates
- Wilshire Associates
- Aon Hewitt
- Mercer
- NEPC
- Meketa
- RVK

**Effort:** 1 day
**Business Value:** LOW - Relationship mapping

---

### 3.9 lp_manager_or_vehicle_exposure (PRIORITY 3)

**Purpose:** Detailed manager/vehicle level exposures (when disclosed)

**Current State:**
- Table exists: `app/core/models.py:462`
- 0 rows in database
- CAFR parser could extract but not implemented

**Root Cause:** Granular vehicle data rarely disclosed; low priority

**Schema (Already Defined):**
```python
class LpManagerOrVehicleExposure(Base):
    __tablename__ = "lp_manager_or_vehicle_exposure"
    id, strategy_id, manager_name, vehicle_name
    vehicle_type, asset_class, market_value_amount
    weight_pct, status, geo_region, sector_focus
    source_section_id, created_at
```

**Implementation Steps:**
1. Extend CAFR parser for detailed manager tables
2. Parse investment committee presentations
3. Extract from public board materials

**Effort:** 2 days
**Business Value:** LOW - Granular detail

---

## 4. Data Source Mapping

### 4.1 Source-to-Table Matrix

| Source | Target Tables | LP Types | Access |
|--------|---------------|----------|--------|
| **SEC 13F** | lp_13f_holding | Pensions, Endowments, SWFs | Free API |
| **SEC ADV** | lp_key_contact, lp_fund | All with RIA | Free API |
| **Form 990** | performance, allocation, aum | Endowments, Foundations | Free API |
| **CAFR PDFs** | All LP tables | Public Pensions | Download + LLM |
| **LP Websites** | contacts, governance, documents | All | Web scraping |
| **NASRA** | pension_metrics | US Public Pensions | Scrape |
| **Form 5500** | pension_metrics | Corporate Pensions | DOL EFAST |
| **News/PR** | manager_commitment, portfolio | All | NewsAPI |

### 4.2 Coverage by LP Type

| LP Type | Count | 13F | Performance | Allocation | Managers |
|---------|-------|-----|-------------|------------|----------|
| US Public Pension | 200+ | ~50% | ~80% | ~80% | ~60% |
| US Endowment | 100+ | ~30% | ~70% | ~50% | ~40% |
| Sovereign Wealth | 30+ | ~20% | ~30% | ~40% | ~20% |
| Corporate Pension | 50+ | ~40% | ~50% | ~30% | ~30% |
| Foundation | 100+ | ~20% | ~60% | ~30% | ~20% |

### 4.3 Missing Identifiers

| Identifier Type | Have | Need | Source |
|-----------------|------|------|--------|
| SEC CIK | ~50 | 200+ | SEC EDGAR lookup |
| IRS EIN | ~50 | 200+ | ProPublica lookup |
| SEC CRD | ~100 | 300+ | IAPD lookup |

---

## 5. Feature Requirements

### 5.1 MVP Features (Phase 1)

| Feature | Tables Required | Business Value |
|---------|-----------------|----------------|
| **Historical Performance Tracking** | lp_performance_return | Compare LP returns over time |
| **Asset Allocation Trends** | lp_allocation_history | Track strategy shifts |
| **Portfolio Holdings (13F)** | lp_13f_holding | Verified position data |
| **Manager Relationships** | lp_manager_commitment | GP network mapping |

### 5.2 High-Value Features (Phase 2)

| Feature | Tables Required | Business Value |
|---------|-----------------|----------------|
| **Governance Intelligence** | lp_governance_member, lp_board_meeting | Decision-maker identification |
| **Pension Health Metrics** | lp_pension_metrics | LP financial health |
| **Consultant Relationships** | lp_consultant | Influence mapping |

### 5.3 Advanced Features (Phase 3)

| Feature | Tables Required | Business Value |
|---------|-----------------|----------------|
| **AUM History & Trends** | lp_aum_history | Growth trajectory |
| **Co-Investment Network** | portfolio_companies, co_investments | Network analysis |
| **Commitment Pacing** | lp_manager_commitment | Predict future activity |

---

## 6. Implementation Roadmap

### Phase 1: Foundation (Days 1-5)

```
Day 1-2: Database Setup
├── Define lp_13f_holding model
├── Define lp_manager_commitment model
├── Define lp_pension_metrics model
├── Define lp_aum_history model
├── Define lp_allocation_history model
├── Define lp_consultant model
└── Run Alembic migrations

Day 3-4: Collector Updates
├── Connect 13F collector to lp_13f_holding
├── Update CAFR parser for performance data
├── Update CAFR parser for manager relationships
└── Test end-to-end data flow

Day 5: Initial Collection
├── Run 13F collection for LPs with known CIKs
├── Run CAFR collection for top 50 pensions
└── Validate data quality
```

### Phase 2: Depth (Days 6-10)

```
Day 6-7: Pension Metrics
├── Extend CAFR parser for pension fields
├── Create NASRA collector (or manual seed)
├── Collect for 100+ US public pensions
└── Validate funded ratio accuracy

Day 8-9: Allocation History
├── Update CAFR parser for historical allocations
├── Backfill from existing strategy snapshots
├── Collect 5+ years of history for top 50 LPs
└── Build allocation trend API

Day 10: AUM History
├── Seed from current lp_fund.aum_usd_billions
├── Extract historical from CAFRs
├── Build AUM trend visualization
└── Quality check all Phase 2 data
```

### Phase 3: Breadth (Days 11-13)

```
Day 11: Governance & Meetings
├── Fix governance collector save pipeline
├── Run collection for LPs with public meetings
├── Collect board materials URLs
└── Extract meeting dates and agendas

Day 12: Consultants & Exposure
├── Create consultant extraction from CAFRs
├── Seed top consultants list
├── Build consultant relationship API
└── Add vehicle-level exposure parsing

Day 13: Quality & Documentation
├── Full data quality audit
├── Fill critical gaps with manual data
├── Update API documentation
├── Generate coverage report
```

### Dependency Graph

```
                    ┌─────────────────┐
                    │  Define Models  │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
       ┌────────────┐ ┌────────────┐ ┌────────────┐
       │ 13F Table  │ │Performance │ │ Managers   │
       └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
             │              │              │
             ▼              ▼              ▼
       ┌────────────┐ ┌────────────┐ ┌────────────┐
       │13F Collect │ │CAFR Parse  │ │CAFR Parse  │
       └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
             │              │              │
             └──────────────┼──────────────┘
                            ▼
                    ┌─────────────────┐
                    │  Pension Metrics │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
       ┌────────────┐ ┌────────────┐ ┌────────────┐
       │Allocations │ │AUM History │ │Governance  │
       └────────────┘ └────────────┘ └────────────┘
```

---

## 7. Technical Details

### 7.1 Collector Modifications Required

| Collector | File | Changes |
|-----------|------|---------|
| **SEC 13F** | `sec_13f_source.py:398-419` | Add database save logic |
| **CAFR Parser** | `cafr_parser.py:497-548` | Save allocations to new table |
| **CAFR Parser** | `cafr_parser.py:550-610` | Save performance to lp_performance_return |
| **CAFR Parser** | `cafr_parser.py:612-663` | Save managers to lp_manager_commitment |
| **Governance** | `governance_source.py:552-656` | Save meetings to lp_board_meeting |
| **Performance** | `performance_source.py:191-240` | Improve extraction accuracy |

### 7.2 New Collectors Needed

| Collector | Purpose | Priority |
|-----------|---------|----------|
| State Portal | Download CAFRs from state websites | P2 |
| NASRA | Aggregate pension statistics | P2 |
| Press Release | LP commitment announcements | P3 |
| Form 5500 | Corporate pension data | P3 |

### 7.3 Model Additions

Add to `app/core/models.py`:

```python
class Lp13fHolding(Base):
    """SEC 13F institutional holdings."""
    __tablename__ = "lp_13f_holding"
    # ... (schema above)

class LpManagerCommitment(Base):
    """LP commitments to GPs/managers."""
    __tablename__ = "lp_manager_commitment"
    # ... (schema above)

class LpPensionMetrics(Base):
    """Pension-specific health metrics."""
    __tablename__ = "lp_pension_metrics"
    # ... (schema above)

class LpAumHistory(Base):
    """Historical AUM tracking."""
    __tablename__ = "lp_aum_history"
    # ... (schema above)

class LpAllocationHistory(Base):
    """Historical asset allocations."""
    __tablename__ = "lp_allocation_history"
    # ... (schema above)

class LpConsultant(Base):
    """Investment consultant relationships."""
    __tablename__ = "lp_consultant"
    # ... (schema above)
```

### 7.4 API Endpoints to Add

```
# Performance & Allocations
GET  /api/v1/lps/{id}/performance-history
GET  /api/v1/lps/{id}/allocation-history
GET  /api/v1/lps/{id}/aum-history

# Holdings
GET  /api/v1/lps/{id}/holdings
GET  /api/v1/lps/{id}/holdings/changes
GET  /api/v1/lps/{id}/holdings/summary

# Manager Relationships
GET  /api/v1/lps/{id}/managers
GET  /api/v1/lps/{id}/commitments
GET  /api/v1/lps/by-manager?manager_name=Blackstone

# Pension Metrics
GET  /api/v1/lps/{id}/pension-metrics
GET  /api/v1/lps/by-funded-ratio?min=80&max=100

# Filtering
GET  /api/v1/lps/by-allocation?asset_class=private_equity&min_pct=10
GET  /api/v1/lps/by-performance?min_5yr_return=7
```

---

## 8. Success Metrics

### Target State

| Metric | Current | Target | Verification Query |
|--------|---------|--------|-------------------|
| LPs with 13F data | 0 | 200+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_13f_holding` |
| LPs with performance | 1 | 500+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_performance_return` |
| LPs with manager data | 0 | 400+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_manager_commitment` |
| LPs with pension metrics | 0 | 250+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_pension_metrics` |
| LPs with allocation history | 0 | 300+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_allocation_history` |
| LPs with AUM history | 0 | 400+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_aum_history` |
| Total 13F holdings | 0 | 50,000+ | `SELECT COUNT(*) FROM lp_13f_holding` |
| Total manager relationships | 0 | 5,000+ | `SELECT COUNT(*) FROM lp_manager_commitment` |
| Total data rows | ~13K | ~100K | Sum of all LP tables |

### Quality Metrics

| Metric | Target |
|--------|--------|
| Performance return coverage for top 100 LPs | 90%+ |
| Allocation data within 2 years for top 100 LPs | 85%+ |
| 13F data within 2 quarters for LPs with CIKs | 95%+ |
| Pension funded ratio data for US pensions | 80%+ |

---

## 9. Risk Assessment

### Technical Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| SEC rate limiting | Collection blocked | Medium | Respectful delays (0.5s), caching |
| CAFR PDF parsing errors | Bad data | High | LLM validation, confidence scores |
| Missing CIKs/EINs | Incomplete coverage | Medium | Manual lookup, SEC search |
| LLM extraction hallucinations | Incorrect data | Medium | Validation rules, source tracing |

### Data Quality Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Data staleness | Outdated info | Scheduled refresh, staleness alerts |
| Duplicate entities | Inflated counts | Deduplication on ingest |
| Inconsistent naming | Matching failures | Normalizer patterns |
| Missing historical data | Incomplete trends | Best-effort backfill, flag gaps |

### Operational Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Storage growth | Cost increase | Partition old data, archive policy |
| Collection job failures | Data gaps | Circuit breaker, retry logic |
| Schema changes break APIs | Client errors | Versioned APIs, migration testing |

---

## 10. Appendices

### A: Complete Table Schemas

See `app/core/models.py` for existing table definitions.

New tables defined in Section 3 above.

### B: Collector API Reference

| Collector | Endpoint | Rate Limit |
|-----------|----------|------------|
| SEC EDGAR | `data.sec.gov/submissions/` | 10 req/sec |
| ProPublica | `projects.propublica.org/nonprofits/api/v2` | No limit |
| SEC IAPD | `adviserinfo.sec.gov/` | 5 req/sec |
| NewsAPI | `newsapi.org/v2/` | 100 req/day |

### C: LP Registry Enrichment Template

```json
{
  "name": "CalPERS",
  "formal_name": "California Public Employees' Retirement System",
  "lp_type": "public_pension",
  "jurisdiction": "CA",
  "region": "us",
  "country_code": "US",
  "website_url": "https://www.calpers.ca.gov",
  "aum_usd_billions": "450",
  "has_cafr": true,
  "sec_cik": "0001067983",
  "irs_ein": null,
  "sec_crd_number": null,
  "collection_priority": 1,
  "cafr_url": "https://www.calpers.ca.gov/docs/forms-publications/cafr-2024.pdf",
  "investments_page": "https://www.calpers.ca.gov/page/investments",
  "board_page": "https://www.calpers.ca.gov/page/about/board"
}
```

### D: CAFR LLM Prompt Templates

See `app/sources/lp_collection/cafr_parser.py:46-129` for:
- `ALLOCATION_EXTRACTION_PROMPT`
- `PERFORMANCE_EXTRACTION_PROMPT`
- `MANAGERS_EXTRACTION_PROMPT`

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-27 | Claude Opus 4.5 | Initial comprehensive document |

---

**Next Steps:**
1. Review and approve this plan
2. Create database migration for new tables
3. Begin Phase 1 implementation
4. Track progress in `PARALLEL_WORK.md`
