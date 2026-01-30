# Product Requirements Document: People & Org Chart Intelligence Platform

**Version:** 1.0
**Date:** 2026-01-28
**Author:** Nexdata Team
**Status:** DRAFT - PENDING APPROVAL

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Goals & Success Metrics](#3-goals--success-metrics)
4. [User Stories](#4-user-stories)
5. [Data Model](#5-data-model)
6. [System Architecture](#6-system-architecture)
7. [Collection Agents](#7-collection-agents)
8. [LLM Extraction Specifications](#8-llm-extraction-specifications)
9. [API Specifications](#9-api-specifications)
10. [Implementation Plan](#10-implementation-plan)
11. [Testing Strategy](#11-testing-strategy)
12. [Risks & Mitigations](#12-risks--mitigations)
13. [Appendices](#13-appendices)

---

## 1. Executive Summary

### 1.1 What We're Building

A **leadership intelligence platform for PE operating teams** that automatically discovers, extracts, and maintains corporate leadership information and organizational structures. The system enables:

**Portfolio Visibility:**
- See leadership teams across all portfolio companies in one place
- Track management stability and identify gaps

**Peer Benchmarking:**
- Compare team structures and leadership quality vs. similar companies
- Identify missing roles, unusual structures, or staffing gaps

**Industry Monitoring:**
- Real-time alerts on executive changes across portfolio and industry
- Track where talent is moving (leading indicator of company health)

**Key Player Tracking:**
- Build watchlists of executives to monitor and potentially recruit
- Map relationships between executives who've worked together

**Data Collection (Automated):**
- Crawl company websites to find and extract leadership team data
- Parse SEC filings for executive information on public companies
- Monitor press releases for leadership changes
- Infer organizational hierarchies from titles and page structures

### 1.2 Pilot Vertical: Industrial Parts & Distribution

We're starting with the **industrial parts distribution** sector because:

1. **Heavy PE Activity**: One of the most active sectors for PE roll-ups and platform builds
2. **Management is Key**: Leadership quality drives value creation in fragmented markets
3. **Data Gap**: No existing solution provides leadership intelligence for this vertical
4. **Mix of Company Types**: Public companies (SEC data), large private, PE-backed (tests all scenarios)
5. **Clear PE Use Case**: Monitor portfolio, benchmark vs. peers, track sector talent
6. **Expandable**: Framework applies to other PE-heavy industrials (building products, packaging, etc.)

### 1.3 Key Principles

| Principle | Description |
|-----------|-------------|
| **Agentic** | Fully automated collection, no manual data entry |
| **No Paid APIs** | Use web scraping and public data unless API provides 10x value |
| **LLM-Powered** | Use AI for extraction to handle unstructured data |
| **Respectful** | Rate-limited, follows robots.txt, no ToS violations |
| **Fresh** | Data should be < 30 days old for active companies |

---

## 2. Problem Statement

### 2.1 Current Pain Points

**For PE Operating Partners:**
- No single view of leadership teams across portfolio companies
- Can't quickly compare management depth to peer companies
- Rely on portfolio company self-reporting for team updates
- Miss early warning signs of management instability
- Difficult to identify candidates when upgrading portfolio leadership

**For PE Deal Teams:**
- Manual, time-consuming management team research during diligence
- No systematic way to assess "management quality" vs. benchmarks
- Can't identify key person risks or flight risks
- Miss executive background issues that surface post-close

**For PE Asset Managers:**
- No dashboard view of leadership stability across the portfolio
- Miss leadership changes until quarterly board meetings
- Can't track industry-wide executive movements for talent pipeline
- No early warning when key execs start looking elsewhere

**For PE Industry Analysts:**
- Manual tracking of executive movements across target sectors
- Can't identify companies with "executive exodus" (distress signal)
- No systematic way to map relationships between executives
- Miss sector trends in organizational structure and leadership

### 2.2 Why Existing Solutions Fall Short

| Solution | Limitation |
|----------|------------|
| **ZoomInfo** | $15K+/year, stale data, poor on private companies |
| **LinkedIn Sales Nav** | Can't export, no org charts, expensive |
| **Crunchbase** | Tech-focused, weak on industrial sectors |
| **D&B Hoovers** | Enterprise pricing, clunky interface |
| **Manual Research** | Time-consuming, inconsistent, doesn't scale |

### 2.3 Our Opportunity

Build a **PE-focused leadership intelligence platform** that:
- Provides portfolio-wide visibility into management teams
- Enables benchmarking against peer companies and industry standards
- Monitors executive changes across portfolio and target sectors in real-time
- Identifies key players for recruitment and networking
- Tracks leadership movements as leading indicators of company health
- Supports M&A diligence with automated management team research
- Starts with industrial vertical (PE-heavy, underserved by existing data providers)

---

## 3. Goals & Success Metrics

### 3.1 Primary Goals

| Goal | Description | Timeframe |
|------|-------------|-----------|
| **G1** | Portfolio visibility: See leadership teams for any tracked company | 8 weeks |
| **G2** | Peer benchmarking: Compare team structures across similar companies | 10 weeks |
| **G3** | Change monitoring: Real-time alerts on leadership changes across portfolio & industry | 12 weeks |
| **G4** | Key player tracking: Search, watchlist, and track executives across the sector | 12 weeks |
| **G5** | Cover 200+ industrial companies (public, private, PE-backed) | 12 weeks |

### 3.2 Success Metrics

**Data Coverage (by end of Month 3):**

| Metric | Target | Stretch |
|--------|--------|---------|
| Companies with leadership data | 200 | 500 |
| Total people profiles | 2,000 | 5,000 |
| Companies with full org charts | 50 | 150 |
| Executives with LinkedIn URLs | 60% | 80% |
| Executives with email patterns | 40% | 60% |

**Data Quality:**

| Metric | Target |
|--------|--------|
| Title accuracy | > 95% |
| Name accuracy | > 99% |
| Company attribution accuracy | > 98% |
| Duplicate rate | < 3% |
| Data freshness (median age) | < 30 days |

**System Performance:**

| Metric | Target |
|--------|--------|
| Collection success rate | > 85% of companies |
| LLM extraction accuracy | > 90% |
| API response time (p95) | < 500ms |
| Daily collection capacity | 100 companies |

---

## 4. User Stories

### 4.1 Portfolio Company Team Visibility

```
As a PE operating partner,
I want to see the complete leadership team for each portfolio company,
So that I can understand our management depth and identify gaps.

Acceptance Criteria:
- View all C-suite executives with titles, tenure, and backgrounds
- See org chart showing reporting relationships
- View executive bios, previous companies, and education
- See board composition and committee assignments
- Track when each exec was hired/promoted
- Flag open/interim positions

Example Use:
"Show me Acme Industrial's full leadership team, who reports to whom,
and how long each exec has been in their role."
```

```
As a PE asset manager overseeing multiple portfolio companies,
I want a unified dashboard showing leadership across my portfolio,
So that I can quickly assess management stability and strength.

Acceptance Criteria:
- Portfolio-wide view: one row per portco, key metrics
- Show: CEO tenure, CFO tenure, # C-suite positions, open roles
- Flag portcos with recent C-suite changes (last 90 days)
- Flag portcos with CEO tenure < 2 years
- Drill down to individual company team pages

Example Use:
"Across my 12 portfolio companies, which ones have had executive
turnover recently, and which have management gaps I need to address?"
```

### 4.2 Ongoing Portfolio Benchmarking (Post-Acquisition)

```
As a PE operating partner managing a portfolio company,
I want to continuously benchmark our leadership team against market competitors,
So that I can ensure we have the right team to win in the market.

Acceptance Criteria:
- Define a peer set for each portfolio company (competitors, aspirational peers)
- Ongoing comparison dashboard: our team vs. peer average
- Track: team completeness, tenure, functional coverage, seniority levels
- Alert when peers add strategic roles we don't have (e.g., CDO, Chief Growth Officer)
- Alert when peers are upgrading leadership (new hires from top companies)
- Quarterly "team health" report vs. peer benchmarks

Example Use:
"Grainger just hired a Chief Digital Officer from Amazon. Should we
be thinking about digital leadership for our portfolio company?
How do our competitors' teams compare to ours?"
```

```
As a PE operating partner responsible for value creation,
I want to see if my portfolio company is keeping pace with competitor investments in talent,
So that I can recommend leadership upgrades before we fall behind.

Acceptance Criteria:
- Track competitor hiring patterns: which functions are they investing in?
- Identify "talent wars" - functions where competitors are all hiring aggressively
- Show competitor exec quality trends (are they upgrading or maintaining?)
- Compare: our team's average experience/pedigree vs. competitor teams
- Flag when competitors poach from industry leaders (signal of ambition)

Example Use:
"Our competitors have all hired VPs of E-commerce in the last year.
We're behind on digital. Show me who they hired, where they came
from, and what that tells us about market direction."
```

```
As a PE operating partner,
I want to track whether my portfolio company is losing talent to competitors,
So that I can address retention issues before they become critical.

Acceptance Criteria:
- Alert when a portfolio company exec departs
- Track where they go: competitor? different industry? retirement?
- Show pattern analysis: are we a "net exporter" or "net importer" of talent?
- Compare our exec turnover rate vs. peer companies
- Identify if specific competitors are targeting our people

Example Use:
"We've lost two VPs to Grainger in the last year. Is Grainger
systematically recruiting from us? What are they offering that
we're not?"
```

```
As a PE board member,
I want to compare our portfolio company's board composition to peers,
So that I can ensure we have appropriate governance and expertise.

Acceptance Criteria:
- Compare board size, independence ratio, committee structure vs. peers
- Identify expertise gaps: do peers have board members with skills we lack?
- Track board changes at competitors (signals of strategic shifts)
- Benchmark board member backgrounds (industry experience, functional expertise)

Example Use:
"Our competitors all have board members with supply chain expertise.
Should we be adding that to our board? Who are the strong candidates
in the market?"
```

### 4.3 Pre-Acquisition Due Diligence

```
As a PE deal team member evaluating an acquisition target,
I want to assess the management team quality relative to peers,
So that I can factor management strength into valuation and retention planning.

Acceptance Criteria:
- View target's full team with backgrounds
- See "management score" based on tenure, experience quality, completeness
- Compare to peer average: avg CEO tenure, exec churn rate, team completeness
- Identify flight risks (short tenure, recently demoted, compensation below market)
- See previous M&A experience of management team
- Identify key person dependencies (single points of failure)

Example Use:
"We're looking at acquiring SunSource. How does their management
team stack up vs. peers? Who are the key people we need to retain?
What are the red flags?"
```

```
As a PE deal team member,
I want to see the target company's leadership history over time,
So that I can identify stability patterns and potential issues.

Acceptance Criteria:
- Timeline view: all leadership changes over past 3-5 years
- Identify periods of instability (multiple changes in short window)
- Track CEO/CFO tenure vs. industry average
- See if key roles have had high turnover (e.g., 3 CFOs in 4 years)
- Identify where departed execs went (competitors = red flag)

Example Use:
"Show me the leadership history at SunSource over the last 5 years.
Has the team been stable? Any concerning patterns I should dig into
during diligence?"
```

### 4.4 Industry & Portfolio Change Monitoring

```
As a PE operating partner,
I want to be alerted when there are leadership changes at my portfolio companies,
So that I can proactively address succession and recruitment needs.

Acceptance Criteria:
- Real-time alerts for: departures, new hires, promotions, interim appointments
- Filter by title level (C-suite only, VP+, all)
- Alert includes: who, what changed, effective date, source
- Weekly digest option showing all portfolio changes
- Flag "surprise" departures (short tenure, no succession announced)

Example Use:
"Alert me immediately if any C-suite exec leaves a portfolio company,
and send me a weekly digest of all VP+ changes."
```

```
As a PE analyst covering the industrial distribution sector,
I want to monitor executive movements across the entire industry,
So that I can identify sector trends, talent availability, and potential targets.

Acceptance Criteria:
- Industry-wide change feed: all exec changes in the sector
- Filter by: change type, title level, company type (public/private/PE-backed)
- See patterns: which companies are losing talent? gaining?
- Track where departing execs land (competitor moves)
- Flag companies with 3+ C-suite changes in 12 months (instability signal)
- Identify companies poaching from others (aggressive talent strategy)

Example Use:
"Show me all C-suite changes in industrial distribution over the
last 90 days. Which companies are hemorrhaging talent? Which
companies are picking up strong execs from competitors?"
```

```
As a PE operating partner,
I want to see press releases and announcements across my portfolio and industry,
So that I stay informed about strategic moves and market dynamics.

Acceptance Criteria:
- Aggregated news feed: leadership announcements, strategic hires, reorgs
- Filter by: my portfolio, my watchlist, full industry
- Categorize: new hire announcement, promotion, departure, reorg, board change
- Link to source (press release, 8-K, news article)
- Highlight announcements involving key tracked individuals

Example Use:
"What leadership announcements have happened in industrial distribution
this week? Specifically interested in any moves involving CFOs or
operations leaders."
```

### 4.5 Key Player Identification & Tracking

```
As a PE operating partner looking to upgrade a portfolio company's leadership,
I want to identify top executives at competitor and peer companies,
So that I can build a target list for recruitment.

Acceptance Criteria:
- Search by: title + industry + geography + company size
- View candidate profiles: current role, tenure, previous companies, education
- See career trajectory (rising star vs. plateaued)
- Filter by experience: "has scaled a business", "has M&A integration experience"
- Save to a watchlist for ongoing monitoring
- See when tracked executives change roles

Example Use:
"Find me CFOs at industrial distributors in the Midwest with 5+ years
experience at companies between $100M-$500M revenue. I'm building
a candidate list for our portco's CFO search."
```

```
As a PE investor tracking the industrial sector,
I want to maintain a watchlist of key executives I know or want to track,
So that I'm notified when they make career moves.

Acceptance Criteria:
- Add executives to a personal watchlist
- Get alerts when they: change companies, get promoted, depart, join boards
- See their full activity history
- Tag executives (e.g., "potential CEO", "strong CFO", "knows well")
- Track executives across companies over time (career timeline view)

Example Use:
"I met Jane Doe at a conference. She's VP Ops at Grainger but would
be a great CEO candidate. Add her to my watchlist and alert me if
she ever moves."
```

```
As a PE operating partner planning a strategic initiative (e.g., digital transformation),
I want to find executives with specific experience across the industry,
So that I can understand who has done this before and potentially recruit them.

Acceptance Criteria:
- Search by: functional expertise + specific experience tags
- Example searches: "Chief Digital Officers in manufacturing",
  "Execs who led ERP implementations", "Leaders from acquired companies"
- View their track record: what initiatives, at which companies, what outcomes
- Identify patterns: who are the "usual suspects" for turnarounds?
- Build a "people map" of who knows who (worked together at same company)

Example Use:
"Who in the industrial sector has experience leading digital transformation?
I want to either hire one or at least talk to them about best practices
for our portfolio."
```

### 4.6 Due Diligence Export & Reporting

```
As an M&A associate performing due diligence on an acquisition target,
I want a comprehensive management team assessment package,
So that I can present management quality and risks to the investment committee.

Acceptance Criteria:
- One-click export: management team summary for a target company
- Includes: all executives, bios, tenure, previous experience
- Includes: management stability score, avg tenure, recent changes
- Includes: compensation data (for public companies)
- Includes: peer comparison benchmarks
- Includes: identified risks (short tenure, key person dependencies)
- Export formats: PDF report, PowerPoint slides, Excel data

Example Use:
"Generate a management team assessment for our IC memo on the
SunSource acquisition. I need bios, tenure analysis, and how
they compare to peers."
```

---

## 5. Data Model

### 5.1 Entity Relationship Diagram

```
┌─────────────────┐       ┌─────────────────┐
│   companies     │       │     people      │
│─────────────────│       │─────────────────│
│ id              │       │ id              │
│ name            │       │ full_name       │
│ website         │       │ linkedin_url    │
│ industry        │       │ email           │
│ ...             │       │ ...             │
└────────┬────────┘       └────────┬────────┘
         │                         │
         │    ┌─────────────────┐  │
         └────┤ company_people  ├──┘
              │─────────────────│
              │ company_id (FK) │
              │ person_id (FK)  │
              │ title           │
              │ reports_to_id   │
              │ is_current      │
              │ start_date      │
              │ ...             │
              └─────────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
┌────────▼────────┐    ┌──────────▼──────────┐
│ leadership_     │    │   org_chart_        │
│ changes         │    │   snapshots         │
│─────────────────│    │─────────────────────│
│ company_id      │    │ company_id          │
│ person_id       │    │ snapshot_date       │
│ change_type     │    │ chart_json          │
│ old_title       │    │ source              │
│ new_title       │    └─────────────────────┘
│ change_date     │
└─────────────────┘
```

### 5.2 Table Definitions

#### 5.2.1 `people` - Master Person Records

```sql
CREATE TABLE people (
    id SERIAL PRIMARY KEY,

    -- Identity
    full_name VARCHAR(500) NOT NULL,
    first_name VARCHAR(200),
    last_name VARCHAR(200),
    middle_name VARCHAR(200),
    suffix VARCHAR(50),  -- Jr., III, PhD, etc.

    -- Contact
    email VARCHAR(300),
    email_confidence VARCHAR(20),  -- verified, inferred, guessed
    phone VARCHAR(50),

    -- Location
    city VARCHAR(200),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA',

    -- Social/External
    linkedin_url VARCHAR(500) UNIQUE,
    linkedin_id VARCHAR(100),  -- Extracted from URL
    twitter_url VARCHAR(500),
    personal_website VARCHAR(500),
    photo_url VARCHAR(500),

    -- Bio
    bio TEXT,
    bio_source VARCHAR(100),  -- website, linkedin, sec_filing

    -- Demographics (if available from SEC)
    birth_year INTEGER,
    age_as_of_date DATE,

    -- Data Quality
    data_sources JSONB,  -- ["website", "sec_proxy", "linkedin"]
    confidence_score DECIMAL(3,2),  -- 0.00 to 1.00
    last_verified_date DATE,
    last_enriched_date DATE,

    -- Deduplication
    canonical_id INTEGER REFERENCES people(id),  -- Points to master if duplicate
    is_canonical BOOLEAN DEFAULT TRUE,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,

    -- Indexes
    CONSTRAINT people_name_not_empty CHECK (full_name <> '')
);

CREATE INDEX idx_people_name ON people(full_name);
CREATE INDEX idx_people_lastname ON people(last_name);
CREATE INDEX idx_people_linkedin ON people(linkedin_url);
CREATE INDEX idx_people_canonical ON people(canonical_id) WHERE canonical_id IS NOT NULL;
```

#### 5.2.2 `industrial_companies` - Company Master

```sql
CREATE TABLE industrial_companies (
    id SERIAL PRIMARY KEY,

    -- Identity
    name VARCHAR(500) NOT NULL,
    legal_name VARCHAR(500),
    dba_names JSONB,  -- ["doing business as" names]

    -- Website
    website VARCHAR(500),
    leadership_page_url VARCHAR(500),
    careers_page_url VARCHAR(500),
    newsroom_url VARCHAR(500),

    -- Location
    headquarters_address TEXT,
    headquarters_city VARCHAR(200),
    headquarters_state VARCHAR(100),
    headquarters_country VARCHAR(100) DEFAULT 'USA',

    -- Classification
    industry_segment VARCHAR(200),  -- distribution, manufacturing, oem
    sub_segment VARCHAR(200),  -- fasteners, bearings, electrical, etc.
    naics_code VARCHAR(10),
    sic_code VARCHAR(10),

    -- Size
    employee_count INTEGER,
    employee_count_range VARCHAR(50),  -- "100-500", "1000-5000"
    employee_count_source VARCHAR(100),
    revenue_usd DECIMAL(15,2),
    revenue_range VARCHAR(50),  -- "$100M-$500M"
    revenue_source VARCHAR(100),

    -- Ownership
    ownership_type VARCHAR(50),  -- public, private, pe_backed, employee_owned
    ticker VARCHAR(20),
    stock_exchange VARCHAR(50),
    cik VARCHAR(20),  -- SEC identifier
    pe_sponsor VARCHAR(200),
    pe_acquisition_date DATE,

    -- Parent/Subsidiary
    parent_company_id INTEGER REFERENCES industrial_companies(id),
    is_subsidiary BOOLEAN DEFAULT FALSE,

    -- Status
    status VARCHAR(50) DEFAULT 'active',  -- active, acquired, bankrupt, inactive
    founded_year INTEGER,

    -- Data Quality
    data_sources JSONB,
    last_crawled_date DATE,
    leadership_last_updated DATE,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,

    CONSTRAINT companies_name_unique UNIQUE(name)
);

CREATE INDEX idx_companies_name ON industrial_companies(name);
CREATE INDEX idx_companies_segment ON industrial_companies(industry_segment);
CREATE INDEX idx_companies_ownership ON industrial_companies(ownership_type);
CREATE INDEX idx_companies_ticker ON industrial_companies(ticker) WHERE ticker IS NOT NULL;
```

#### 5.2.3 `company_people` - Person-Company Relationships

```sql
CREATE TABLE company_people (
    id SERIAL PRIMARY KEY,

    -- Foreign Keys
    company_id INTEGER NOT NULL REFERENCES industrial_companies(id),
    person_id INTEGER NOT NULL REFERENCES people(id),

    -- Role
    title VARCHAR(500) NOT NULL,
    title_normalized VARCHAR(200),  -- Standardized: "CEO", "CFO", "VP Sales"
    title_level VARCHAR(50),  -- c_suite, vp, director, manager, individual
    department VARCHAR(200),  -- sales, operations, finance, hr, it, marketing
    function_area VARCHAR(200),  -- More specific: "inside sales", "field sales"

    -- Hierarchy
    reports_to_id INTEGER REFERENCES company_people(id),
    management_level INTEGER,  -- 1 = CEO, 2 = C-suite, 3 = VP, etc.
    direct_reports_count INTEGER,

    -- Board
    is_board_member BOOLEAN DEFAULT FALSE,
    is_board_chair BOOLEAN DEFAULT FALSE,
    board_committee VARCHAR(200),  -- audit, compensation, nominating

    -- Employment
    is_current BOOLEAN DEFAULT TRUE,
    is_founder BOOLEAN DEFAULT FALSE,
    start_date DATE,
    end_date DATE,
    tenure_months INTEGER,  -- Calculated field

    -- Compensation (for public companies from proxy)
    base_salary_usd DECIMAL(12,2),
    total_compensation_usd DECIMAL(15,2),
    equity_awards_usd DECIMAL(15,2),
    compensation_year INTEGER,

    -- Contact at Company
    work_email VARCHAR(300),
    work_phone VARCHAR(50),
    office_location VARCHAR(200),

    -- Data Quality
    source VARCHAR(100),  -- website, sec_proxy, linkedin, press_release
    source_url VARCHAR(500),
    extraction_date DATE,
    confidence VARCHAR(20) DEFAULT 'medium',  -- high, medium, low

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE,

    CONSTRAINT company_people_unique UNIQUE(company_id, person_id, title, is_current)
);

CREATE INDEX idx_cp_company ON company_people(company_id);
CREATE INDEX idx_cp_person ON company_people(person_id);
CREATE INDEX idx_cp_current ON company_people(company_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_cp_title_level ON company_people(title_level);
CREATE INDEX idx_cp_reports_to ON company_people(reports_to_id) WHERE reports_to_id IS NOT NULL;
```

#### 5.2.4 `people_experience` - Work History

```sql
CREATE TABLE people_experience (
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    person_id INTEGER NOT NULL REFERENCES people(id),

    -- Company (may not be in our DB)
    company_name VARCHAR(500) NOT NULL,
    company_id INTEGER REFERENCES industrial_companies(id),  -- If in our DB

    -- Role
    title VARCHAR(500) NOT NULL,
    title_normalized VARCHAR(200),
    department VARCHAR(200),

    -- Tenure
    start_date DATE,
    start_year INTEGER,  -- If only year known
    end_date DATE,
    end_year INTEGER,
    is_current BOOLEAN DEFAULT FALSE,
    duration_months INTEGER,

    -- Details
    description TEXT,
    location VARCHAR(200),

    -- Source
    source VARCHAR(100),  -- linkedin, sec_filing, bio

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT experience_unique UNIQUE(person_id, company_name, title, start_year)
);

CREATE INDEX idx_exp_person ON people_experience(person_id);
CREATE INDEX idx_exp_company ON people_experience(company_name);
CREATE INDEX idx_exp_current ON people_experience(is_current) WHERE is_current = TRUE;
```

#### 5.2.5 `people_education` - Education Records

```sql
CREATE TABLE people_education (
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    person_id INTEGER NOT NULL REFERENCES people(id),

    -- Institution
    institution VARCHAR(500) NOT NULL,
    institution_type VARCHAR(100),  -- university, business_school, law_school

    -- Degree
    degree VARCHAR(200),  -- MBA, BS, BA, JD, PhD
    degree_type VARCHAR(50),  -- bachelors, masters, doctorate, certificate
    field_of_study VARCHAR(300),

    -- Dates
    start_year INTEGER,
    graduation_year INTEGER,

    -- Honors
    honors VARCHAR(300),  -- Summa Cum Laude, Valedictorian
    gpa VARCHAR(20),

    -- Activities
    activities TEXT,
    athletics VARCHAR(200),

    -- Source
    source VARCHAR(100),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_edu_person ON people_education(person_id);
CREATE INDEX idx_edu_institution ON people_education(institution);
```

#### 5.2.6 `org_chart_snapshots` - Point-in-Time Org Structures

```sql
CREATE TABLE org_chart_snapshots (
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    company_id INTEGER NOT NULL REFERENCES industrial_companies(id),

    -- Snapshot
    snapshot_date DATE NOT NULL,

    -- Org Chart Data (JSON structure)
    chart_data JSONB NOT NULL,
    /*
    Example structure:
    {
        "root": {
            "person_id": 123,
            "name": "John Smith",
            "title": "CEO",
            "children": [
                {
                    "person_id": 124,
                    "name": "Jane Doe",
                    "title": "CFO",
                    "children": []
                },
                {
                    "person_id": 125,
                    "name": "Bob Wilson",
                    "title": "COO",
                    "children": [...]
                }
            ]
        },
        "metadata": {
            "total_executives": 15,
            "max_depth": 4,
            "departments": ["Finance", "Operations", "Sales"]
        }
    }
    */

    -- Metadata
    total_people INTEGER,
    max_depth INTEGER,
    departments JSONB,  -- List of departments represented

    -- Source
    source VARCHAR(100),  -- website, inferred, manual
    source_url VARCHAR(500),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT org_snapshot_unique UNIQUE(company_id, snapshot_date)
);

CREATE INDEX idx_org_company ON org_chart_snapshots(company_id);
CREATE INDEX idx_org_date ON org_chart_snapshots(snapshot_date);
```

#### 5.2.7 `leadership_changes` - Executive Movements

```sql
CREATE TABLE leadership_changes (
    id SERIAL PRIMARY KEY,

    -- Foreign Keys
    company_id INTEGER NOT NULL REFERENCES industrial_companies(id),
    person_id INTEGER REFERENCES people(id),  -- May be NULL if person not in DB yet

    -- Person (denormalized for cases where person not in DB)
    person_name VARCHAR(500) NOT NULL,

    -- Change Details
    change_type VARCHAR(50) NOT NULL,
    -- Values: hire, departure, promotion, demotion, lateral, retirement,
    --         board_appointment, board_departure, interim, death

    old_title VARCHAR(500),
    new_title VARCHAR(500),
    old_company VARCHAR(500),  -- For hires from outside

    -- Dates
    announced_date DATE,
    effective_date DATE,
    detected_date DATE NOT NULL DEFAULT CURRENT_DATE,

    -- Context
    reason TEXT,  -- Retirement, pursuing other opportunities, etc.
    successor_person_id INTEGER REFERENCES people(id),
    predecessor_person_id INTEGER REFERENCES people(id),

    -- Source
    source_type VARCHAR(100),  -- press_release, 8k_filing, website_change, news
    source_url VARCHAR(500),
    source_headline TEXT,

    -- Significance
    is_c_suite BOOLEAN DEFAULT FALSE,
    is_board BOOLEAN DEFAULT FALSE,
    significance_score INTEGER,  -- 1-10, higher = more significant

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT change_unique UNIQUE(company_id, person_name, change_type, effective_date)
);

CREATE INDEX idx_changes_company ON leadership_changes(company_id);
CREATE INDEX idx_changes_person ON leadership_changes(person_id) WHERE person_id IS NOT NULL;
CREATE INDEX idx_changes_date ON leadership_changes(effective_date);
CREATE INDEX idx_changes_type ON leadership_changes(change_type);
CREATE INDEX idx_changes_recent ON leadership_changes(detected_date DESC);
```

#### 5.2.8 `collection_jobs` - Track Collection Runs

```sql
CREATE TABLE people_collection_jobs (
    id SERIAL PRIMARY KEY,

    -- Job Type
    job_type VARCHAR(100) NOT NULL,  -- website_crawl, sec_parse, news_scan

    -- Target
    company_id INTEGER REFERENCES industrial_companies(id),
    company_ids JSONB,  -- For batch jobs

    -- Configuration
    config JSONB,

    -- Status
    status VARCHAR(50) DEFAULT 'pending',  -- pending, running, success, failed

    -- Results
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    people_found INTEGER DEFAULT 0,
    people_created INTEGER DEFAULT 0,
    people_updated INTEGER DEFAULT 0,
    changes_detected INTEGER DEFAULT 0,

    errors JSONB,
    warnings JSONB,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_jobs_status ON people_collection_jobs(status);
CREATE INDEX idx_jobs_company ON people_collection_jobs(company_id);
```

---

## 6. System Architecture

### 6.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                                 │
│  (Schedules jobs, coordinates agents, manages rate limits)          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            ▼                       ▼                       ▼
    ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
    │   WEBSITE     │      │     SEC       │      │     NEWS      │
    │   AGENTS      │      │    AGENTS     │      │    AGENTS     │
    │               │      │               │      │               │
    │ - Page Finder │      │ - Proxy Parser│      │ - Newsroom    │
    │ - Extractor   │      │ - 10K Parser  │      │ - PR Monitor  │
    │ - Org Builder │      │ - 8K Monitor  │      │ - Extractor   │
    └───────┬───────┘      └───────┬───────┘      └───────┬───────┘
            │                       │                       │
            └───────────────────────┼───────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────┐
                    │      LLM EXTRACTION       │
                    │   (Claude / GPT-4)        │
                    │                           │
                    │ - Leadership extraction   │
                    │ - Bio parsing             │
                    │ - Change detection        │
                    │ - Org inference           │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │    DATA PROCESSING        │
                    │                           │
                    │ - Deduplication           │
                    │ - Entity resolution       │
                    │ - Normalization           │
                    │ - Change detection        │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │       DATABASE            │
                    │      (PostgreSQL)         │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │         API               │
                    │      (FastAPI)            │
                    └───────────────────────────┘
```

### 6.2 Component Details

#### 6.2.1 Orchestrator

**Responsibilities:**
- Schedule collection jobs (daily, weekly, on-demand)
- Manage job queue and priorities
- Enforce rate limits per domain
- Handle retries and failures
- Coordinate between agents

**Key Classes:**
```python
class PeopleCollectionOrchestrator:
    """
    Coordinates all people data collection activities.
    """

    async def run_company_collection(self, company_id: int, sources: List[str])
    async def run_batch_collection(self, company_ids: List[int], sources: List[str])
    async def run_change_detection(self, since_date: date)
    async def schedule_refresh(self, company_id: int, interval_days: int)
```

#### 6.2.2 Website Agents

**Leadership Page Finder:**
- Crawls company website to find leadership/team pages
- Checks common URL patterns
- Uses link text analysis ("About", "Team", "Leadership")
- Stores discovered URLs for future use

**Leadership Extractor:**
- Fetches leadership page HTML
- Sends to LLM for extraction
- Handles various page formats (cards, lists, grids)
- Extracts: names, titles, bios, photos

**Org Chart Builder:**
- Infers hierarchy from page structure
- Uses title analysis (CEO > CFO > VP > Director)
- Builds tree structure
- Identifies reporting relationships

#### 6.2.3 SEC Agents

**Proxy Parser (DEF 14A):**
- Downloads latest proxy statement
- Extracts Named Executive Officers
- Gets compensation data
- Parses executive bios

**8-K Monitor:**
- Monitors for Item 5.02 filings (leadership changes)
- Extracts appointment/departure details
- Links to existing person records

#### 6.2.4 News Agents

**Newsroom Scraper:**
- Finds company newsroom/press page
- Extracts press release links
- Filters for personnel announcements

**Appointment Extractor:**
- Processes press release text
- Uses LLM to extract structured change data
- Classifies change type

---

## 7. Collection Agents

### 7.1 Website Leadership Agent

#### 7.1.1 Page Discovery

```python
class LeadershipPageFinder:
    """
    Discovers leadership/team pages on company websites.
    """

    # Common URL patterns to check
    URL_PATTERNS = [
        "/about/leadership",
        "/about/team",
        "/about-us/leadership",
        "/about-us/team",
        "/about-us/management",
        "/company/leadership",
        "/company/team",
        "/company/management",
        "/our-team",
        "/our-leadership",
        "/leadership",
        "/team",
        "/management",
        "/executives",
        "/about/executives",
        "/about/board",
        "/about/board-of-directors",
    ]

    # Link text patterns (case-insensitive)
    LINK_PATTERNS = [
        r"leadership",
        r"team",
        r"management",
        r"executives",
        r"our people",
        r"about us",
        r"who we are",
        r"board of directors",
    ]

    async def find_leadership_page(self, website_url: str) -> Optional[str]:
        """
        Find the leadership/team page for a company website.

        Strategy:
        1. Check common URL patterns directly
        2. If not found, fetch homepage and search for links
        3. Follow promising links and validate content

        Returns: URL of leadership page or None
        """
        pass
```

#### 7.1.2 Leadership Extraction

```python
class LeadershipExtractor:
    """
    Extracts leadership data from company team pages.
    """

    async def extract_leadership(
        self,
        page_url: str,
        company_name: str
    ) -> List[ExtractedPerson]:
        """
        Extract all leadership from a team page.

        Steps:
        1. Fetch page HTML
        2. Clean HTML (remove scripts, styles, nav)
        3. Send to LLM with extraction prompt
        4. Parse LLM response
        5. Validate and normalize results

        Returns: List of extracted person records
        """
        pass

    def _build_extraction_prompt(
        self,
        html_content: str,
        company_name: str
    ) -> str:
        """Build the LLM prompt for extraction."""
        pass
```

#### 7.1.3 Org Chart Inference

```python
class OrgChartBuilder:
    """
    Infers organizational hierarchy from extracted leadership data.
    """

    # Title hierarchy (higher number = higher rank)
    TITLE_RANKS = {
        "ceo": 100,
        "president": 95,
        "coo": 90,
        "cfo": 90,
        "cto": 85,
        "cmo": 85,
        "chro": 85,
        "general counsel": 85,
        "evp": 80,
        "svp": 75,
        "vp": 70,
        "director": 60,
        "manager": 50,
    }

    def build_org_chart(
        self,
        people: List[ExtractedPerson]
    ) -> OrgChartNode:
        """
        Build organizational hierarchy from list of people.

        Strategy:
        1. Rank people by title seniority
        2. Group by department if available
        3. Infer reporting based on title patterns
        4. Build tree structure

        Returns: Root node of org chart tree
        """
        pass
```

### 7.2 SEC Filing Agent

#### 7.2.1 Proxy Statement Parser

```python
class ProxyParser:
    """
    Parses DEF 14A proxy statements for executive information.
    """

    async def parse_proxy(self, cik: str) -> ProxyData:
        """
        Download and parse latest proxy statement.

        Extracts:
        - Named Executive Officers (NEOs)
        - Executive compensation
        - Executive bios and ages
        - Board of Directors
        - Committee memberships

        Returns: Structured proxy data
        """
        pass

    def _find_neo_section(self, filing_text: str) -> str:
        """Find the Named Executive Officers section."""
        pass

    def _extract_compensation_table(self, filing_text: str) -> List[dict]:
        """Extract the Summary Compensation Table."""
        pass
```

#### 7.2.2 8-K Change Monitor

```python
class Form8KMonitor:
    """
    Monitors 8-K filings for leadership changes.
    """

    # Item 5.02 = Departure/Appointment of Officers
    TARGET_ITEMS = ["5.02"]

    async def check_for_changes(
        self,
        cik: str,
        since_date: date
    ) -> List[LeadershipChange]:
        """
        Check for leadership change 8-Ks since a given date.

        Returns: List of detected leadership changes
        """
        pass

    def _parse_item_502(self, filing_text: str) -> LeadershipChange:
        """Parse Item 5.02 disclosure for change details."""
        pass
```

### 7.3 News Agent

#### 7.3.1 Press Release Monitor

```python
class PressReleaseMonitor:
    """
    Monitors company newsrooms for leadership announcements.
    """

    ANNOUNCEMENT_KEYWORDS = [
        "appoints", "names", "promotes", "announces",
        "joins", "hired", "appointed", "elected",
        "retires", "departs", "steps down", "resigns",
        "succeeds", "succession", "transition",
    ]

    async def find_announcements(
        self,
        company: Company,
        since_date: date
    ) -> List[PressRelease]:
        """
        Find leadership-related press releases.

        Strategy:
        1. Find company newsroom URL
        2. Scrape press release list
        3. Filter by keywords
        4. Fetch full text of relevant releases

        Returns: List of relevant press releases
        """
        pass
```

#### 7.3.2 Appointment Extractor

```python
class AppointmentExtractor:
    """
    Extracts structured leadership change data from press releases.
    """

    async def extract_change(
        self,
        press_release: PressRelease,
        company_name: str
    ) -> Optional[LeadershipChange]:
        """
        Extract leadership change from press release text.

        Uses LLM to identify:
        - Person name
        - Change type (hire, promotion, departure)
        - Old title (if applicable)
        - New title
        - Effective date
        - Additional context

        Returns: Structured change record or None
        """
        pass
```

---

## 8. LLM Extraction Specifications

### 8.1 Leadership Page Extraction Prompt

```
You are extracting leadership information from a company webpage.

Company: {company_name}
Page URL: {page_url}

Extract ALL people mentioned who appear to be in leadership/management roles.
For each person, provide the following in JSON format:

{
  "people": [
    {
      "full_name": "First Last",
      "title": "Their exact title as shown",
      "title_normalized": "Standardized title (CEO, CFO, VP Sales, etc.)",
      "title_level": "c_suite|vp|director|manager|board",
      "department": "Department if mentioned (Sales, Finance, Operations, etc.)",
      "bio": "Brief bio if available (1-2 sentences max)",
      "is_board_member": true/false,
      "is_executive": true/false,
      "reports_to": "Name of person they report to if inferable",
      "linkedin_url": "LinkedIn URL if visible on page",
      "email": "Email if visible on page",
      "photo_url": "Photo URL if visible"
    }
  ],
  "extraction_confidence": "high|medium|low",
  "page_type": "leadership|team|about|board",
  "notes": "Any issues or observations"
}

Rules:
1. Only include people who appear to be employees/leaders, not testimonials or clients
2. Infer department from title if not explicit (e.g., "VP of Sales" -> department: "Sales")
3. Set title_level based on title keywords
4. If bio is very long, summarize to 1-2 sentences
5. reports_to can be inferred from page structure (e.g., if someone is under CEO section)
6. Set extraction_confidence based on page clarity

Page HTML:
{html_content}
```

### 8.2 Press Release Extraction Prompt

```
You are extracting leadership change information from a press release.

Company: {company_name}
Press Release Date: {date}

Extract any leadership changes (appointments, promotions, departures) mentioned.
Return JSON format:

{
  "changes": [
    {
      "person_name": "Full name",
      "change_type": "hire|promotion|departure|retirement|board_appointment|board_departure|interim",
      "new_title": "New title (null if departure)",
      "old_title": "Previous title (null if new hire)",
      "old_company": "Previous company if external hire",
      "effective_date": "YYYY-MM-DD if mentioned, null otherwise",
      "is_c_suite": true/false,
      "is_board": true/false,
      "context": "Brief context (1 sentence)",
      "successor_name": "Name of successor if mentioned",
      "predecessor_name": "Name of predecessor if mentioned"
    }
  ],
  "extraction_confidence": "high|medium|low"
}

Rules:
1. Only extract actual changes, not mentions of existing roles
2. "Appointed", "named", "joins" = hire or promotion
3. "Retires", "steps down", "resigns", "departs" = departure/retirement
4. "Promoted to", "elevated to" = promotion
5. Board appointments are separate from executive appointments
6. effective_date should be in YYYY-MM-DD format

Press Release Text:
{text}
```

### 8.3 Bio Parsing Prompt

```
You are parsing an executive bio to extract structured information.

Person: {person_name}
Current Company: {company_name}

Extract the following from the bio text:

{
  "experience": [
    {
      "company": "Company name",
      "title": "Title held",
      "start_year": 2020,
      "end_year": 2023,
      "is_current": false,
      "description": "Brief description if notable"
    }
  ],
  "education": [
    {
      "institution": "University name",
      "degree": "MBA, BS, etc.",
      "field": "Field of study",
      "graduation_year": 2005
    }
  ],
  "board_positions": [
    {
      "organization": "Board org name",
      "role": "Board Member, Director, etc.",
      "is_current": true
    }
  ],
  "certifications": ["CPA", "CFA", etc.],
  "military_service": "Branch and rank if mentioned",
  "notable_achievements": ["Key achievement 1"]
}

Rules:
1. Extract all work experience mentioned, ordered by recency
2. If only years are mentioned, use those (not full dates)
3. Education should include all degrees mentioned
4. Board positions include corporate boards, nonprofits, advisory
5. Only include certifications that are professional credentials

Bio Text:
{bio_text}
```

### 8.4 LLM Configuration

```python
LLM_CONFIG = {
    "model": "claude-3-5-sonnet-20241022",  # Or GPT-4
    "temperature": 0.1,  # Low for consistent extraction
    "max_tokens": 4000,
    "retry_attempts": 3,
    "retry_delay_seconds": 2,
}

# Cost estimates per extraction
COST_ESTIMATES = {
    "leadership_page": "$0.02-0.05",  # ~2K input, 1K output tokens
    "press_release": "$0.01-0.02",    # ~500 input, 500 output tokens
    "bio_parsing": "$0.01",           # ~300 input, 500 output tokens
}
```

---

## 9. API Specifications

### 9.1 People Endpoints

#### Search People
```
GET /api/v1/people/search

Query Parameters:
- q (string): Search query (name)
- company (string): Filter by company name
- title (string): Filter by title contains
- title_level (string): c_suite, vp, director, manager
- industry (string): Filter by industry segment
- limit (int): Max results (default 50, max 500)
- offset (int): Pagination offset

Response:
{
  "total": 150,
  "count": 50,
  "results": [
    {
      "id": 123,
      "full_name": "John Smith",
      "current_title": "CEO",
      "current_company": "Fastenal",
      "linkedin_url": "https://linkedin.com/in/johnsmith",
      "location": "Winona, MN"
    }
  ]
}
```

#### Get Person Details
```
GET /api/v1/people/{person_id}

Response:
{
  "id": 123,
  "full_name": "John Smith",
  "first_name": "John",
  "last_name": "Smith",
  "linkedin_url": "...",
  "photo_url": "...",
  "bio": "...",
  "current_position": {
    "company_id": 456,
    "company_name": "Fastenal",
    "title": "CEO",
    "start_date": "2019-01-15",
    "tenure_months": 84
  },
  "experience": [...],
  "education": [...],
  "board_positions": [...]
}
```

#### Get Person Experience
```
GET /api/v1/people/{person_id}/experience

Response:
{
  "person_id": 123,
  "experience": [
    {
      "company_name": "Fastenal",
      "title": "CEO",
      "start_date": "2019-01-15",
      "is_current": true
    },
    {
      "company_name": "Fastenal",
      "title": "President & COO",
      "start_date": "2016-03-01",
      "end_date": "2019-01-14"
    }
  ]
}
```

### 9.2 Company Endpoints

#### Get Company Leadership
```
GET /api/v1/companies/{company_id}/leadership

Query Parameters:
- current_only (bool): Only current employees (default true)
- include_board (bool): Include board members (default true)

Response:
{
  "company_id": 456,
  "company_name": "Fastenal",
  "leadership": [
    {
      "person_id": 123,
      "name": "John Smith",
      "title": "CEO",
      "title_level": "c_suite",
      "department": "Executive",
      "is_board_member": true,
      "linkedin_url": "...",
      "tenure_months": 84
    }
  ],
  "total_executives": 12,
  "last_updated": "2026-01-15"
}
```

#### Get Org Chart
```
GET /api/v1/companies/{company_id}/org-chart

Query Parameters:
- snapshot_date (date): Get historical snapshot (optional)
- max_depth (int): Levels to include (default all)

Response:
{
  "company_id": 456,
  "company_name": "Fastenal",
  "snapshot_date": "2026-01-28",
  "org_chart": {
    "root": {
      "person_id": 123,
      "name": "John Smith",
      "title": "CEO",
      "children": [
        {
          "person_id": 124,
          "name": "Jane Doe",
          "title": "CFO",
          "children": []
        },
        {
          "person_id": 125,
          "name": "Bob Wilson",
          "title": "COO",
          "children": [
            {
              "person_id": 126,
              "name": "Alice Brown",
              "title": "VP Operations",
              "children": []
            }
          ]
        }
      ]
    }
  },
  "metadata": {
    "total_people": 15,
    "max_depth": 3,
    "departments": ["Executive", "Finance", "Operations", "Sales"]
  }
}
```

#### Get Leadership Changes
```
GET /api/v1/companies/{company_id}/leadership-changes

Query Parameters:
- since (date): Changes since date
- change_type (string): Filter by type
- limit (int): Max results

Response:
{
  "company_id": 456,
  "changes": [
    {
      "id": 789,
      "person_name": "New Person",
      "change_type": "hire",
      "new_title": "VP Sales",
      "effective_date": "2026-01-15",
      "source_url": "https://...",
      "announced_date": "2026-01-10"
    }
  ]
}
```

### 9.3 Leadership Changes Feed

```
GET /api/v1/leadership-changes/feed

Query Parameters:
- industry (string): Filter by industry
- change_type (string): Filter by type
- title_level (string): Filter by level (c_suite, vp, etc.)
- days (int): Last N days (default 30)
- limit (int): Max results

Response:
{
  "count": 45,
  "changes": [
    {
      "id": 789,
      "company_id": 456,
      "company_name": "Fastenal",
      "person_name": "John Doe",
      "change_type": "hire",
      "new_title": "CFO",
      "is_c_suite": true,
      "effective_date": "2026-01-15",
      "source_url": "..."
    }
  ]
}
```

### 9.4 Industry Analytics

```
GET /api/v1/industries/{industry}/leadership-stats

Response:
{
  "industry": "industrial_distribution",
  "period": "last_90_days",
  "stats": {
    "total_companies": 200,
    "total_executives": 2500,
    "changes_detected": 145,
    "by_change_type": {
      "hire": 78,
      "departure": 45,
      "promotion": 22
    },
    "by_title_level": {
      "c_suite": 23,
      "vp": 67,
      "director": 55
    },
    "avg_ceo_tenure_months": 72,
    "companies_with_ceo_change": 8
  }
}
```

---

## 10. Implementation Plan

### Phase 1: Foundation (Weeks 1-2)

| Task | Description | Est. Hours |
|------|-------------|------------|
| Create database tables | All 8 tables with indexes | 4 |
| Set up collection module structure | Base classes, types, config | 4 |
| Implement base collector | HTTP client, rate limiting, retries | 6 |
| Set up LLM integration | Claude/GPT-4 API wrapper | 4 |
| Create company seed list | 200 industrial companies | 4 |
| Build basic API scaffolding | FastAPI routers, models | 4 |
| **Total** | | **26** |

**Deliverables:**
- Database schema deployed
- Collection module skeleton
- 200 companies in database
- Basic API endpoints (list companies)

### Phase 2: Website Collection (Weeks 3-4)

| Task | Description | Est. Hours |
|------|-------------|------------|
| Leadership page finder | URL patterns, link discovery | 8 |
| Leadership extractor | LLM prompts, response parsing | 12 |
| Org chart builder | Hierarchy inference | 8 |
| Deduplication logic | Name matching, merge handling | 6 |
| Run on Tier 1 companies | 10 public companies | 4 |
| Run on Tier 2 companies | 50 private companies | 4 |
| **Total** | | **42** |

**Deliverables:**
- 60+ companies with leadership data
- 500+ people profiles
- Basic org charts for 20+ companies

### Phase 3: SEC Integration (Weeks 5-6)

| Task | Description | Est. Hours |
|------|-------------|------------|
| Proxy (DEF 14A) parser | Download, extract NEOs, compensation | 12 |
| 8-K change monitor | Find Item 5.02, extract changes | 8 |
| Cross-reference with website data | Match people, update records | 6 |
| Add compensation data | Store and expose via API | 4 |
| Run on all public companies | ~15 companies | 4 |
| **Total** | | **34** |

**Deliverables:**
- All public company execs enriched with SEC data
- Compensation data for NEOs
- 8-K monitoring for public companies

### Phase 4: News & Press (Weeks 7-8)

| Task | Description | Est. Hours |
|------|-------------|------------|
| Newsroom finder | Discover press/news URLs | 6 |
| Press release scraper | List and fetch PRs | 8 |
| Appointment extractor | LLM extraction from PR text | 10 |
| Change detection pipeline | Compare to existing data | 6 |
| Set up monitoring schedule | Daily/weekly jobs | 4 |
| **Total** | | **34** |

**Deliverables:**
- Leadership change detection live
- 200+ historical changes captured
- Ongoing monitoring for all companies

### Phase 5: Enrichment & Quality (Weeks 9-10)

| Task | Description | Est. Hours |
|------|-------------|------------|
| LinkedIn validation | Google index lookup | 8 |
| Photo extraction | Download and store | 4 |
| Email pattern inference | Derive work emails | 6 |
| Data quality scoring | Confidence scores | 6 |
| Deduplication improvements | Fuzzy matching | 6 |
| Admin tools | Data review, manual corrections | 8 |
| **Total** | | **38** |

**Deliverables:**
- 70%+ LinkedIn URLs
- 50%+ inferred emails
- Quality scores on all records
- Admin interface for corrections

### Phase 6: API & Analytics (Weeks 11-12)

| Task | Description | Est. Hours |
|------|-------------|------------|
| Complete API endpoints | All endpoints from spec | 12 |
| Search optimization | Full-text search, filters | 8 |
| Org chart visualization endpoint | JSON tree structure | 6 |
| Leadership change feed | Real-time feed API | 4 |
| Industry analytics | Stats and aggregations | 6 |
| Documentation | API docs, examples | 4 |
| **Total** | | **40** |

**Deliverables:**
- Full API live
- Documentation complete
- Analytics dashboard data

### Total Effort: ~214 hours (5-6 weeks of focused dev time)

---

## 11. Testing Strategy

### 11.1 Unit Tests

```python
# Test leadership extraction
def test_extract_leadership_from_fastenal():
    html = load_fixture("fastenal_leadership.html")
    result = extractor.extract(html, "Fastenal")

    assert len(result.people) >= 10
    assert any(p.title == "CEO" for p in result.people)
    assert result.confidence == "high"

# Test org chart building
def test_build_org_chart():
    people = [
        Person(name="CEO", title="CEO"),
        Person(name="CFO", title="CFO"),
        Person(name="VP Sales", title="VP Sales"),
    ]

    chart = builder.build(people)

    assert chart.root.name == "CEO"
    assert len(chart.root.children) == 2

# Test change detection
def test_detect_departure():
    old_leadership = [Person(name="John", title="CFO")]
    new_leadership = []

    changes = detect_changes(old_leadership, new_leadership)

    assert len(changes) == 1
    assert changes[0].type == "departure"
```

### 11.2 Integration Tests

```python
# Test full collection pipeline
async def test_collect_company_leadership():
    company = create_test_company("https://www.grainger.com")

    result = await orchestrator.collect(company.id, sources=["website"])

    assert result.success
    assert result.people_found >= 5

    # Verify in database
    leadership = await db.get_company_leadership(company.id)
    assert len(leadership) >= 5

# Test SEC parsing
async def test_parse_fastenal_proxy():
    result = await proxy_parser.parse("0000915389")  # Fastenal CIK

    assert result.named_executives >= 5
    assert result.total_compensation > 0
    assert "CEO" in [e.title for e in result.executives]
```

### 11.3 Validation Tests

```python
# Test against known data
def test_fastenal_ceo():
    """Validate we correctly identify Fastenal's CEO."""
    leadership = api.get_company_leadership("Fastenal")

    ceo = next(p for p in leadership if p.title_level == "c_suite" and "CEO" in p.title)

    # As of 2026, this should be Dan Florness
    assert "Florness" in ceo.name
    assert ceo.is_current == True

# Test data freshness
def test_data_freshness():
    """Ensure data is not stale."""
    companies = db.get_all_companies()

    stale_count = 0
    for c in companies:
        if c.leadership_last_updated < date.today() - timedelta(days=60):
            stale_count += 1

    assert stale_count / len(companies) < 0.1  # Less than 10% stale
```

---

## 12. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **LinkedIn blocking** | Medium | High | Use only Google-indexed profiles; no login-based scraping; rate limit to 1 req/30s |
| **Website structure changes** | High | Medium | LLM-based extraction is more resilient than rule-based; monitor extraction quality |
| **LLM extraction errors** | Medium | Medium | Confidence scoring; validation against SEC data; manual review queue |
| **Rate limiting by sites** | Medium | Low | Respectful crawling (2s delay); distributed across time; retry with backoff |
| **Stale data** | Medium | Medium | Monthly refresh schedule; 8-K monitoring; news alerts |
| **Person deduplication** | High | Medium | LinkedIn URL as canonical ID; fuzzy name matching; manual merge tool |
| **Privacy/legal concerns** | Low | High | Only collect publicly available data; no personal data beyond professional |
| **LLM costs** | Low | Low | Cache extractions; batch processing; use smaller models for simple tasks |

---

## 13. Appendices

### Appendix A: Industrial Companies Seed List

See `data/seeds/industrial_companies.json` for full list.

**Tier 1 - Public Companies (Priority):**
| Company | Ticker | Website | CIK |
|---------|--------|---------|-----|
| Fastenal | FAST | fastenal.com | 0000915389 |
| W.W. Grainger | GWW | grainger.com | 0000277135 |
| MSC Industrial | MSM | mscdirect.com | 0001003078 |
| Applied Industrial | AIT | applied.com | 0000109563 |
| Wesco International | WCC | wesco.com | 0000929008 |
| Kaman Corporation | KAMN | kaman.com | 0000054381 |
| DXP Enterprises | DXPE | dxpe.com | 0001020569 |
| Houston Wire & Cable | HWCC | houwire.com | 0001303652 |

**Tier 2 - Large Private:**
| Company | Website | Est. Revenue |
|---------|---------|--------------|
| McMaster-Carr | mcmaster.com | $1B+ |
| Graybar Electric | graybar.com | $9B |
| Border States Electric | borderstates.com | $3B |
| Turtle & Hughes | quoteturtle.com | $800M |
| Van Meter Inc | vanmeterinc.com | $500M |

**Tier 3 - PE-Backed:**
| Company | PE Sponsor | Website |
|---------|------------|---------|
| Distribution International | Audax | distributioninternational.com |
| SunSource | Olympus Partners | sun-source.com |
| OTC Industrial | Gridiron Capital | otcindustrial.com |
| FCX Performance | H.I.G. Capital | fcxperformance.com |

### Appendix B: Title Normalization Map

```python
TITLE_NORMALIZATIONS = {
    # CEO variants
    "chief executive officer": "CEO",
    "ceo": "CEO",
    "president and ceo": "President & CEO",
    "president & ceo": "President & CEO",
    "president/ceo": "President & CEO",

    # CFO variants
    "chief financial officer": "CFO",
    "cfo": "CFO",
    "vp finance": "VP Finance",
    "vice president of finance": "VP Finance",
    "svp finance": "SVP Finance",

    # COO variants
    "chief operating officer": "COO",
    "coo": "COO",
    "president and coo": "President & COO",

    # Sales leadership
    "chief revenue officer": "CRO",
    "cro": "CRO",
    "chief sales officer": "CSO",
    "vp sales": "VP Sales",
    "svp sales": "SVP Sales",
    "evp sales": "EVP Sales",
    "vice president of sales": "VP Sales",
    "head of sales": "Head of Sales",

    # Operations
    "vp operations": "VP Operations",
    "vice president of operations": "VP Operations",
    "director of operations": "Director of Operations",

    # HR
    "chief human resources officer": "CHRO",
    "chro": "CHRO",
    "chief people officer": "CPO",
    "vp human resources": "VP Human Resources",
    "vp hr": "VP Human Resources",

    # Board
    "chairman": "Chairman",
    "chairman of the board": "Chairman",
    "board member": "Board Member",
    "director": "Board Director",  # Context-dependent
    "independent director": "Independent Director",
}

TITLE_LEVELS = {
    "c_suite": ["CEO", "CFO", "COO", "CTO", "CMO", "CHRO", "CRO", "CSO", "CPO", "CLO"],
    "president": ["President", "President & CEO", "President & COO"],
    "evp": ["EVP", "Executive Vice President"],
    "svp": ["SVP", "Senior Vice President"],
    "vp": ["VP", "Vice President"],
    "director": ["Director"],
    "manager": ["Manager"],
    "board": ["Chairman", "Board Member", "Board Director", "Independent Director"],
}
```

### Appendix C: Sample LLM Extraction Output

**Input HTML (simplified):**
```html
<div class="leadership-team">
  <div class="executive">
    <img src="/images/john-smith.jpg">
    <h3>John Smith</h3>
    <p class="title">Chief Executive Officer</p>
    <p class="bio">John joined Acme Industrial in 2015 and was named CEO in 2019.
    Prior to Acme, he spent 15 years at Grainger in various leadership roles.</p>
  </div>
  <div class="executive">
    <img src="/images/jane-doe.jpg">
    <h3>Jane Doe</h3>
    <p class="title">Chief Financial Officer</p>
    <p class="bio">Jane oversees all financial operations. She previously served
    as VP Finance at Applied Industrial.</p>
  </div>
</div>
```

**LLM Output:**
```json
{
  "people": [
    {
      "full_name": "John Smith",
      "title": "Chief Executive Officer",
      "title_normalized": "CEO",
      "title_level": "c_suite",
      "department": "Executive",
      "bio": "Joined Acme Industrial in 2015, named CEO in 2019. Previously spent 15 years at Grainger.",
      "is_board_member": false,
      "is_executive": true,
      "reports_to": null,
      "photo_url": "/images/john-smith.jpg"
    },
    {
      "full_name": "Jane Doe",
      "title": "Chief Financial Officer",
      "title_normalized": "CFO",
      "title_level": "c_suite",
      "department": "Finance",
      "bio": "Oversees financial operations. Previously VP Finance at Applied Industrial.",
      "is_board_member": false,
      "is_executive": true,
      "reports_to": "John Smith",
      "photo_url": "/images/jane-doe.jpg"
    }
  ],
  "extraction_confidence": "high",
  "page_type": "leadership",
  "notes": "Clean page structure with clear executive cards"
}
```

---

## Approval

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Product Owner | | | [ ] Approved |
| Tech Lead | | | [ ] Approved |
| Data Lead | | | [ ] Approved |

---

*End of PRD*
