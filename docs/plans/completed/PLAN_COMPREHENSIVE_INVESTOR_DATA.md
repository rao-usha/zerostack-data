# Plan: Comprehensive Investor Data Expansion

**Status:** PLANNING
**Date:** 2026-01-23
**Scope:** Major expansion of LP and Family Office data coverage

---

## Executive Summary

Expand Nexdata's investor coverage from current state to comprehensive institutional investor database:

| Metric | Current | Target |
|--------|---------|--------|
| LP Funds | 112 | 500+ |
| Family Offices | ~50 | 300+ |
| Data Sources | 4 | 12+ |
| Data Points per LP | ~15 | 50+ |
| Historical Years | 0 | 10+ |

---

## Part 1: LP Data Expansion

### 1.1 New LP Categories to Add

| Category | Current | Add | Total | Sources |
|----------|---------|-----|-------|---------|
| US Public Pensions | 45 | +100 | 145 | NASRA, State websites |
| US Corporate Pensions | 10 | +50 | 60 | Form 5500, SEC |
| US Endowments | 12 | +100 | 112 | NACUBO, 990s |
| US Foundations | 0 | +50 | 50 | IRS 990-PF |
| European Pensions | 15 | +50 | 65 | EIOPA, National registries |
| Asian Pensions | 10 | +30 | 40 | Government sites |
| Sovereign Wealth | 15 | +20 | 35 | SWFI database |
| Insurance GAs | 5 | +30 | 35 | State filings, AM Best |
| **Total** | **112** | **+430** | **542** | |

### 1.2 New Data Points to Capture

#### Performance Data
| Field | Source | Frequency |
|-------|--------|-----------|
| 1-year return | CAFR, Website | Annual |
| 3-year return | CAFR | Annual |
| 5-year return | CAFR | Annual |
| 10-year return | CAFR | Annual |
| Benchmark name | CAFR | Annual |
| Benchmark return | CAFR | Annual |
| Value added (bps) | Calculated | Annual |
| Sharpe ratio | CAFR (if available) | Annual |

#### Asset Allocation (Current + Target)
| Field | Source |
|-------|--------|
| Public Equity % | CAFR, Website |
| Fixed Income % | CAFR, Website |
| Private Equity % | CAFR, Website |
| Real Estate % | CAFR, Website |
| Real Assets/Infra % | CAFR, Website |
| Hedge Funds % | CAFR, Website |
| Cash % | CAFR, Website |
| Other % | CAFR, Website |

#### Holdings & Commitments
| Field | Source |
|-------|--------|
| SEC 13F holdings | SEC EDGAR |
| GP/Manager relationships | CAFR, PitchBook mentions |
| Fund commitments | CAFR, Press releases |
| Direct investments | News, CAFR |
| Co-investments | News, CAFR |

#### Governance
| Field | Source |
|-------|--------|
| Board members | Website, CAFR |
| CIO/CEO | Website, SEC |
| Investment committee | Website, Board minutes |
| Investment consultants | CAFR, RFPs |
| Custodian bank | CAFR |

#### Operational
| Field | Source |
|-------|--------|
| Fiscal year end | CAFR |
| AUM history (10yr) | CAFR series |
| Funded status (pensions) | CAFR |
| Contribution rate | CAFR |
| Benefit payments | CAFR |
| Investment policy URL | Website |

### 1.3 New Data Sources

| Source | Data Type | Access Method | Priority |
|--------|-----------|---------------|----------|
| **SEC 13F** | Public holdings | EDGAR API | P1 |
| **SEC Form ADV** | RIA data, AUM | IAPD/EDGAR | P1 |
| **IRS Form 990** | Endowment/Foundation | IRS EO BMF | P1 |
| **Form 5500** | Corporate pensions | DOL EFAST | P2 |
| **NASRA** | Public pension stats | Web scrape | P2 |
| **NACUBO** | Endowment stats | Partnership/scrape | P2 |
| **State CAFR Portals** | Annual reports | State websites | P1 |
| **Preqin** | PE/VC commitments | API (paid) | P3 |
| **PitchBook** | Deal data | API (paid) | P3 |
| **Bloomberg** | Market data | API (paid) | P3 |

### 1.4 LP Collection Enhancements

#### New Collectors to Build
```
app/sources/lp_collection/
├── sec_13f_source.py      # Quarterly holdings
├── form_990_source.py     # Endowment/foundation data
├── form_5500_source.py    # Corporate pension data
├── nasra_source.py        # Public pension stats
├── cafr_parser.py         # PDF extraction with LLM
├── state_portal_source.py # State-specific scrapers
└── press_release_source.py # News/PR monitoring
```

#### Enhanced CAFR Processing
```python
class CAFRProcessor:
    """AI-powered CAFR document processing."""

    def download_cafr(self, lp_id: int, fiscal_year: int) -> str:
        """Download CAFR PDF from LP website."""

    def extract_text(self, pdf_path: str) -> str:
        """Extract text from PDF."""

    def extract_with_llm(self, text: str) -> Dict:
        """Use LLM to extract structured data:
        - Asset allocation (current and target)
        - Performance returns
        - Manager list
        - Governance info
        - Key metrics
        """

    def validate_extraction(self, data: Dict) -> ValidationResult:
        """Validate extracted data against known ranges."""
```

---

## Part 2: Family Office Data Expansion

### 2.1 Current State Assessment

Need to audit current family office data:
- How many family offices in DB?
- What fields are populated?
- What's the data quality?

### 2.2 Family Office Categories

| Category | Target Count | Key Characteristics |
|----------|--------------|---------------------|
| Single Family Offices | 150 | $100M+ AUM, direct investments |
| Multi-Family Offices | 50 | Aggregated wealth management |
| Embedded FOs | 50 | Within operating companies |
| Virtual FOs | 30 | Outsourced structure |
| Investment Holding Cos | 20 | Family investment vehicles |
| **Total** | **300** | |

### 2.3 Family Office Data Points

#### Core Information
| Field | Source |
|-------|--------|
| Family name | SEC, News, LinkedIn |
| Source of wealth | News, Wikipedia |
| Estimated AUM | Various estimates |
| Generation | News, bios |
| Geographic focus | Investments, HQ |
| Structure type | SEC filings |

#### Investment Profile
| Field | Source |
|-------|--------|
| Direct investment appetite | Track record |
| Check size range | Deal history |
| Sector preferences | Portfolio |
| Stage preferences | Deal history |
| Geographic preferences | Portfolio |
| Co-investment interest | News, deals |

#### Key People
| Field | Source |
|-------|--------|
| Principal(s) | SEC, News |
| CIO | LinkedIn, News |
| Investment team | LinkedIn |
| External advisors | News |

#### Deal Activity
| Field | Source |
|-------|--------|
| Recent investments | News, Crunchbase |
| Board seats held | SEC, News |
| Fund commitments | News |
| Real estate holdings | Property records |

### 2.4 Family Office Data Sources

| Source | Data Type | Access | Priority |
|--------|-----------|--------|----------|
| **SEC Form ADV** | RIA registrations | IAPD | P1 |
| **SEC 13F** | Public holdings | EDGAR | P1 |
| **Crunchbase** | Deal history | API | P1 |
| **LinkedIn** | People, connections | Manual/API | P2 |
| **News/PR** | Deals, announcements | NewsAPI | P1 |
| **IRS 990-PF** | Foundation giving | IRS | P2 |
| **Property Records** | Real estate | County APIs | P3 |
| **Court Records** | Litigation, trusts | PACER | P3 |

### 2.5 Family Office Collection System

```
app/sources/family_office_collection/
├── __init__.py
├── types.py
├── config.py              # FO registry with 300+ offices
├── base_collector.py
├── sec_fo_source.py       # SEC filings for FOs
├── crunchbase_source.py   # Deal history
├── news_source.py         # News monitoring
├── linkedin_source.py     # People data (manual/enrichment)
├── runner.py              # Orchestrator
└── normalizer.py          # Deduplication
```

---

## Part 3: Database Schema Additions

### 3.1 New Tables for LPs

```sql
-- Historical AUM tracking
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

-- Asset allocation history
CREATE TABLE lp_allocation_history (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    fiscal_year INTEGER NOT NULL,
    allocation_type VARCHAR(20), -- 'actual' or 'target'
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

-- 13F holdings snapshots
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
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_lp_13f_lp_date ON lp_13f_holding(lp_id, report_date);

-- Manager/GP relationships
CREATE TABLE lp_manager_commitment (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    manager_name VARCHAR(500) NOT NULL,
    fund_name VARCHAR(500),
    vintage_year INTEGER,
    commitment_usd DECIMAL(18,2),
    asset_class VARCHAR(50),
    status VARCHAR(50), -- 'active', 'fully_invested', 'harvesting'
    first_observed DATE,
    last_observed DATE,
    source_type VARCHAR(50),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_lp_manager_lp ON lp_manager_commitment(lp_id);
CREATE INDEX idx_lp_manager_name ON lp_manager_commitment(manager_name);

-- Investment consultants
CREATE TABLE lp_consultant (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    consultant_name VARCHAR(500) NOT NULL,
    consultant_type VARCHAR(100), -- 'general', 'pe', 'real_estate', 'hedge_fund'
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Pension-specific data
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

### 3.2 New Tables for Family Offices

```sql
-- Enhanced family office table
ALTER TABLE family_offices ADD COLUMN IF NOT EXISTS
    source_of_wealth TEXT,
    generation INTEGER,
    founding_year INTEGER,
    family_members JSONB,
    structure_type VARCHAR(50),
    direct_investment_focus BOOLEAN,
    check_size_min_usd DECIMAL(18,2),
    check_size_max_usd DECIMAL(18,2),
    preferred_stages JSONB,
    preferred_sectors JSONB,
    co_investment_interest BOOLEAN,
    last_collection_at TIMESTAMP;

-- Family office deals/investments
CREATE TABLE family_office_investment (
    id SERIAL PRIMARY KEY,
    family_office_id INTEGER NOT NULL,
    company_name VARCHAR(500) NOT NULL,
    company_website VARCHAR(500),
    investment_date DATE,
    investment_type VARCHAR(50), -- 'direct', 'fund', 'co-invest', 'real_estate'
    investment_stage VARCHAR(50),
    investment_amount_usd DECIMAL(18,2),
    ownership_pct DECIMAL(5,2),
    board_seat BOOLEAN,
    lead_investor BOOLEAN,
    status VARCHAR(50), -- 'active', 'exited', 'written_off'
    exit_date DATE,
    exit_type VARCHAR(50),
    exit_multiple DECIMAL(5,2),
    source_type VARCHAR(50),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_fo_investment_fo ON family_office_investment(family_office_id);
CREATE INDEX idx_fo_investment_company ON family_office_investment(company_name);

-- Family office fund commitments
CREATE TABLE family_office_fund_commitment (
    id SERIAL PRIMARY KEY,
    family_office_id INTEGER NOT NULL,
    fund_name VARCHAR(500) NOT NULL,
    manager_name VARCHAR(500),
    vintage_year INTEGER,
    commitment_usd DECIMAL(18,2),
    fund_type VARCHAR(50), -- 'PE', 'VC', 'RE', 'HF', 'Credit'
    source_type VARCHAR(50),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Family office key contacts
CREATE TABLE family_office_contact (
    id SERIAL PRIMARY KEY,
    family_office_id INTEGER NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    title VARCHAR(200),
    role_type VARCHAR(50), -- 'principal', 'cio', 'investment_team', 'advisor'
    email VARCHAR(200),
    phone VARCHAR(50),
    linkedin_url VARCHAR(500),
    is_current BOOLEAN DEFAULT TRUE,
    source_type VARCHAR(50),
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT NOW()
);
```

---

## Part 4: Automated Task Workflow System

### 4.1 Task Tracking Pattern

For long-running tasks, Claude should use TodoWrite to create persistent task lists:

```markdown
## Task: Expand LP Database to 500+ Investors

### Phase 1: Data Source Setup [IN PROGRESS]
- [x] Create SEC 13F collector
- [x] Create Form 990 collector
- [ ] Create Form 5500 collector
- [ ] Create NASRA scraper
- [ ] Test all collectors individually

### Phase 2: LP Registry Expansion [NOT STARTED]
- [ ] Add 100 more US public pensions
- [ ] Add 50 corporate pensions
- [ ] Add 100 endowments
- [ ] Add 50 foundations
- [ ] Validate all entries have required fields

### Phase 3: Data Collection Run [NOT STARTED]
- [ ] Run website collector on all new LPs
- [ ] Run 13F collector on eligible LPs
- [ ] Run Form 990 collector on endowments
- [ ] Run CAFR processor on public pensions

### Phase 4: Data Quality [NOT STARTED]
- [ ] Audit completeness by LP type
- [ ] Fill gaps with targeted collection
- [ ] Generate coverage report

### Checkpoint
Last completed: Phase 1, Task 2
Next action: Create Form 5500 collector
Resume command: Continue from Phase 1, Task 3
```

### 4.2 Workflow Commands

Add to CLAUDE.md:

```markdown
## Long-Running Task Protocol

When starting a multi-step task:

1. **CREATE TODO LIST**
   - Use TodoWrite to create task breakdown
   - Include phases, sub-tasks, checkpoints
   - Mark current progress

2. **CHECKPOINT FREQUENTLY**
   - After each sub-task, update todo list
   - Record "Last completed" and "Next action"
   - Include "Resume command" for easy restart

3. **ON FAILURE/INTERRUPTION**
   - Todo list persists in conversation
   - On resume, read todo list first
   - Continue from last checkpoint

4. **COMPLETION**
   - Mark all tasks complete
   - Summarize what was done
   - Note any remaining issues

Example task list format:
```
## Task: [Task Name]
**Status:** IN_PROGRESS | COMPLETE | BLOCKED
**Started:** 2026-01-23
**Last Update:** 2026-01-23 14:30

### Phase N: [Phase Name] [STATUS]
- [x] Completed task
- [ ] Pending task
- [!] Blocked task (reason)

### Checkpoint
- Last completed: [specific task]
- Next action: [specific task]
- Blockers: [if any]
- Resume: [command to continue]
```
```

### 4.3 Implementation in Claude Workflow

```python
# Pseudo-code for how Claude should handle long tasks

async def execute_long_task(task_description: str):
    # 1. Plan the task
    phases = plan_task_phases(task_description)

    # 2. Create todo list
    todo_list = create_todo_list(phases)
    write_todo(todo_list)

    # 3. Execute with checkpoints
    for phase in phases:
        update_todo(phase, status="IN_PROGRESS")

        for task in phase.tasks:
            try:
                execute_task(task)
                update_todo(task, status="COMPLETE")
                checkpoint(phase, task)  # Save progress
            except Exception as e:
                update_todo(task, status="BLOCKED", reason=str(e))
                notify_user(f"Task blocked: {task}. Resume from checkpoint.")
                return

        update_todo(phase, status="COMPLETE")

    # 4. Final summary
    generate_completion_summary()
```

---

## Part 5: Implementation Phases

### Phase 1: Foundation (Week 1)
**Tasks:**
1. Create expanded LP registry JSON (500+ LPs)
2. Create family office registry JSON (300+ FOs)
3. Add new database tables
4. Update existing collectors for new fields

### Phase 2: New Collectors (Week 2)
**Tasks:**
1. SEC 13F collector with historical data
2. IRS Form 990 collector for endowments
3. Form 5500 collector for corporate pensions
4. Enhanced CAFR parser with LLM extraction
5. Family office news/deal collector

### Phase 3: Data Population (Week 3)
**Tasks:**
1. Seed new LPs from registry
2. Seed new family offices from registry
3. Run initial collection on all new entities
4. Run 13F collection for historical holdings
5. Process CAFRs for top 100 public pensions

### Phase 4: Quality & API (Week 4)
**Tasks:**
1. Data quality audit and gap filling
2. New API endpoints for expanded data
3. Coverage reporting dashboard
4. Documentation update

---

## Part 6: New API Endpoints

### LP Endpoints
```
GET  /api/v1/lps/{id}/allocation-history
GET  /api/v1/lps/{id}/performance-history
GET  /api/v1/lps/{id}/holdings
GET  /api/v1/lps/{id}/holdings/changes
GET  /api/v1/lps/{id}/managers
GET  /api/v1/lps/{id}/pension-metrics
GET  /api/v1/lps/by-allocation?asset_class=private_equity&min_pct=10
GET  /api/v1/lps/by-performance?min_5yr_return=7
GET  /api/v1/lps/by-manager?manager_name=Blackstone
```

### Family Office Endpoints
```
GET  /api/v1/family-offices/{id}/investments
GET  /api/v1/family-offices/{id}/fund-commitments
GET  /api/v1/family-offices/{id}/contacts
GET  /api/v1/family-offices/by-sector?sector=technology
GET  /api/v1/family-offices/by-check-size?min=5000000&max=50000000
GET  /api/v1/family-offices/active-investors?months=12
```

### Cross-Entity Endpoints
```
GET  /api/v1/investors/co-investors?company=Stripe
GET  /api/v1/investors/by-manager?manager=Sequoia
GET  /api/v1/investors/search?q=tech&type=all
```

---

## Part 7: Success Metrics

| Metric | Current | Target | Verification |
|--------|---------|--------|--------------|
| Total LPs | 112 | 500+ | `SELECT COUNT(*) FROM lp_fund` |
| LPs with AUM | ~50 | 400+ | Count non-null aum_usd_billions |
| LPs with allocation | ~20 | 300+ | Count in lp_allocation_history |
| LPs with performance | ~10 | 200+ | Count in lp_performance_return |
| LPs with 13F data | 0 | 100+ | Count distinct lp_id in lp_13f_holding |
| Family Offices | ~50 | 300+ | `SELECT COUNT(*) FROM family_offices` |
| FOs with investments | ~20 | 200+ | Count in family_office_investment |
| FOs with contacts | ~30 | 250+ | Count in family_office_contact |

---

## Part 8: Risk & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Rate limiting on SEC | Collection blocked | Respectful delays, caching |
| CAFR PDF parsing errors | Bad data | LLM validation, human review queue |
| Data staleness | Outdated info | Scheduled refresh, staleness alerts |
| Duplicate entities | Data quality | Entity resolution, dedup on ingest |
| Missing historical data | Incomplete trends | Best-effort backfill, note gaps |

---

## Approval Checklist

- [ ] LP expansion scope approved
- [ ] Family office expansion scope approved
- [ ] Database schema approved
- [ ] New collectors list approved
- [ ] API endpoints approved
- [ ] Task workflow system approved
- [ ] Timeline approved

---

**Approved:** [ ]
**Date:**
**Notes:**
