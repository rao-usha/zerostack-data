# AGENT HANDOFF: Private Company Intelligence System

---

## 🎯 MISSION

Build an **agentic system** that automatically discovers and tracks comprehensive intelligence on private companies by synthesizing data from multiple public sources.

**Why This Matters:**
- Most companies are private (~30 million US businesses, only ~4,000 public)
- No single database has comprehensive private company data
- Manual research takes 45-90 minutes per company
- This system will do it in 5-10 minutes with higher quality

**Business Value:**
- **M&A Targeting:** Identify acquisition candidates with detailed profiles
- **Competitive Intelligence:** Track competitors' growth, funding, leadership
- **Due Diligence:** Pre-screen companies before deeper analysis
- **Lead Generation:** Find companies matching specific criteria
- **Market Research:** Understand private company landscape by sector

---

## 📋 WHAT YOU'RE BUILDING

### Core Deliverables:
1. **Agentic orchestrator** that plans which sources to check based on company info
2. **6 collection strategies** (website, funding data, SEC filings, news, job postings, social signals)
3. **Data synthesis engine** that builds comprehensive company profiles
4. **Validation system** that scores data quality and confidence
5. **API endpoints** for company search, profile retrieval, and bulk enrichment

### Database Tables to Create:
- `private_companies` - Core company profiles
- `company_leadership` - Executives and key personnel
- `company_funding_rounds` - VC/PE funding history
- `company_metrics` - Revenue estimates, employee count, growth metrics
- `company_intelligence_jobs` - Agent job tracking

### Expected Results:
- **Start with 500-1,000 high-priority companies** (linked to LP/FO portfolios)
- **80-90% profile completeness** (8-9 out of 10 key fields populated)
- **Average 4+ sources per company** (cross-validation)
- **Automatic updates** (quarterly refresh for active monitoring)

---

## 📚 BACKGROUND CONTEXT

### Current State:
- **Database:** PostgreSQL with existing LP/FO data
- **Tech Stack:** FastAPI + SQLAlchemy + httpx + asyncio
- **Related Data:** LP portfolio data (from agentic portfolio research) will feed target companies
- **Project Structure:** Plugin architecture `app/agentic/`

### Why Agentic (Not Traditional API):
Private company data is scattered across:
- Company website (About, Team, Press pages)
- Crunchbase/PitchBook (funding data - if accessible)
- SEC Form D (debt/equity offerings for larger raises)
- News articles (revenue estimates, growth stories, product launches)
- Job postings (employee count signals, expansion indicators)
- LinkedIn (leadership, employee count - manual viewing only)
- AngelList/ProductHunt (startup-specific data)

**No single source has complete data → Agent must reason about where to look**

---

## 🧠 HOW THE AGENT WORKS

### Agent Workflow:
```
1. PLAN: Agent analyzes company → decides which strategies to try
2. EXECUTE: Agent runs strategies in priority order
3. SYNTHESIZE: Agent combines findings into unified profile
4. VALIDATE: Agent cross-checks data across sources
5. SCORE: Agent assigns completeness and confidence scores
6. LOG: Agent records reasoning trail
```

### Example Agent Decision:
```
Input: "Stripe" (Fintech, San Francisco, Private)

Agent Reasoning:
→ "Well-known startup → check Crunchbase first" (priority: 10)
→ "Website exists → scrape About/Team pages" (priority: 9)
→ "Likely raised >$1M → check SEC Form D" (priority: 8)
→ "Tech company → check job postings on careers page" (priority: 7)
→ "Search news for revenue estimates" (priority: 6)

Execution:
✓ Crunchbase: Found $600M Series H, $95B valuation
✓ Website: Found 8 executives, company founding story
✓ SEC Form D: Found 3 debt offerings (2019, 2021, 2023)
✓ Careers page: 157 open positions → signals growth
✓ News search: "Stripe revenue $7.4B estimate" (2022)
✓ LinkedIn (manual check): 8,000+ employees

Synthesis:
→ Company Profile: 95% complete (9/10 fields)
→ Leadership: 8 executives identified
→ Funding: $2.2B total raised, $95B valuation
→ Metrics: $7.4B revenue (est), 8,000 employees
→ Confidence: HIGH (6 sources, high agreement)
```

---

## 🔧 THE 6 COLLECTION STRATEGIES

### Strategy 1: Company Website Scraping (Core Strategy)
**What:** Extract structured data from official company website
**Coverage:** 60-80% of companies (those with websites)
**Confidence:** MEDIUM-HIGH

**Implementation:**
- Fetch homepage from known URL or Google search result
- Extract from "About Us" page:
  - Founding year
  - Mission/description
  - Headquarters location
  - Industry/sector
- Extract from "Team" / "Leadership" / "About" pages:
  - Executive names and titles (CEO, CTO, CFO, etc.)
  - Board members (if disclosed)
  - Photos/bios
- Extract from "Press" / "News" / "Blog":
  - Recent milestones
  - Product launches
  - Expansion announcements
- Extract from "Careers" / "Jobs":
  - Number of open positions (growth signal)
  - Office locations (geographic footprint)
  - Job titles (tech stack, capabilities)

**Pattern Matching:**
```python
# Executive title patterns
exec_patterns = [
    r'\b(?:Chief Executive Officer|CEO|Co-CEO)\b',
    r'\b(?:Chief Technology Officer|CTO)\b',
    r'\b(?:Chief Financial Officer|CFO)\b',
    r'\b(?:Chief Operating Officer|COO)\b',
    r'\b(?:President|Co-President)\b',
    r'\b(?:Founder|Co-Founder|Co-founder)\b',
    r'\b(?:Managing Director|Managing Partner)\b'
]

# Revenue/metrics patterns (from blog posts, press releases)
revenue_patterns = [
    r'\$(\d+(?:\.\d+)?)\s*(?:million|M)\s+(?:in\s+)?revenue',
    r'\$(\d+(?:\.\d+)?)\s*(?:billion|B)\s+(?:in\s+)?revenue',
    r'revenue\s+of\s+\$(\d+(?:\.\d+)?)\s*(?:million|billion|M|B)'
]
```

**Safeguards:**
- Rate limiting: 0.5 requests/sec per domain
- Max 10 pages per company
- Timeout: 10 seconds per page
- Respect robots.txt

---

### Strategy 2: Funding Database APIs (Crunchbase/PitchBook)
**What:** Extract funding history, valuation, investors
**Coverage:** 30-50% of companies (startups with VC funding)
**Confidence:** HIGH (verified data)

**Implementation:**

**Option A: Crunchbase API (Paid)**
- Endpoint: `https://api.crunchbase.com/api/v4/entities/organizations/{company_name}`
- Data: Funding rounds, total raised, valuation, investors, employee count
- Cost: $29/month (basic) or $99/month (pro)
- Rate limit: 200 requests/day (basic), 1000/day (pro)

**Option B: Web Scraping Crunchbase (Free but fragile)**
- Search for company on Crunchbase
- Parse public profile page
- Extract: Total funding, last round, investors list
- **Note:** May break if they change layout, against ToS if aggressive

**Option C: PitchBook (If Available)**
- Similar to Crunchbase
- Often more institutional/PE focused
- Requires subscription

**Data to Extract:**
```json
{
  "total_funding_usd": 2200000000,
  "last_funding_round": {
    "type": "Series H",
    "amount_usd": 600000000,
    "date": "2023-03-14",
    "lead_investor": "Sequoia Capital",
    "other_investors": ["Andreessen Horowitz", "Tiger Global"]
  },
  "valuation_usd": 95000000000,
  "total_funding_rounds": 15,
  "investor_count": 30,
  "employee_count": 8000
}
```

---

### Strategy 3: SEC Form D Filings
**What:** Extract funding information from SEC filings (companies raising >$1M)
**Coverage:** 20-30% of companies (larger private raises)
**Confidence:** HIGH (regulatory filing)

**Implementation:**
- Search SEC EDGAR for Form D filings by company name
- Download recent Form D XML/HTML
- Extract:
  - Offering amount
  - Offering date
  - Company CIK (identifier)
  - Executive names (signing authority)
  - Industry classification (SIC code)
  - Company address
- Link multiple Form D filings to track funding over time

**API:**
```python
# SEC EDGAR Company Search
base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
params = {
    'action': 'getcompany',
    'company': company_name,
    'type': 'D',  # Form D
    'count': 10,
    'output': 'xml'
}
```

**Data Quality:**
- Form D is filed within 15 days of first sale
- Often delayed or amended
- May not include all deal terms
- Best for tracking timeline of raises

---

### Strategy 4: News & Press Release Search (LLM Extraction)
**What:** Extract revenue estimates, growth metrics, milestones from news
**Coverage:** 40-60% of companies (those with press coverage)
**Confidence:** MEDIUM (estimates, not verified)

**Implementation:**
- Google News search: `"{company_name}" (revenue OR funding OR valuation OR growth)`
- Filter last 2 years
- For top 20 articles:
  - Fetch article text
  - Use LLM to extract structured data

**LLM Prompt Template:**
```
Extract company intelligence from this news article about {company_name}.

Article: {text[:3000]}

Return JSON:
{
  "revenue_estimate_usd": number or null,
  "revenue_year": "YYYY" or null,
  "employee_count": number or null,
  "growth_rate_pct": number or null,
  "recent_milestone": "string or null",
  "expansion_location": "string or null",
  "product_launch": "string or null",
  "executive_hire": {"name": "string", "title": "string"} or null
}

Return null if no relevant data found.
Only extract explicitly stated facts, not speculation.
```

**Sources to Search:**
- TechCrunch, Forbes, Bloomberg (for tech/startups)
- Industry trade publications
- Local business journals
- Company press releases (PR Newswire, Business Wire)

---

### Strategy 5: Job Postings Analysis
**What:** Infer company growth, tech stack, locations from job postings
**Coverage:** 50-70% of companies (those hiring)
**Confidence:** MEDIUM (indirect signals)

**Implementation:**
- Scrape company careers page
- Or check job boards (LinkedIn Jobs, Indeed, Glassdoor - if allowed)
- Extract:
  - Number of open positions (growth signal)
  - Office locations (geographic footprint)
  - Job titles and descriptions (capabilities, tech stack)
  - Seniority levels (expansion stage indicators)

**Insights to Generate:**
```python
# Hiring velocity (growth signal)
if open_positions > 50:
    growth_stage = "rapid_expansion"
elif open_positions > 20:
    growth_stage = "growth"
elif open_positions > 5:
    growth_stage = "steady"
else:
    growth_stage = "maintenance"

# Tech stack inference
tech_stack = []
for job in job_descriptions:
    if "React" in job or "TypeScript" in job:
        tech_stack.append("modern_frontend")
    if "AWS" in job or "cloud" in job:
        tech_stack.append("cloud_native")
    if "ML" in job or "machine learning" in job:
        tech_stack.append("ai_ml")

# Geographic expansion
locations = set([job.location for job in jobs])
if len(locations) > 5:
    expansion_status = "multi_location"
```

---

### Strategy 6: Social/Product Signals
**What:** Gather signals from startup platforms and product launches
**Coverage:** 20-40% of companies (tech startups)
**Confidence:** LOW-MEDIUM (soft signals)

**Sources:**

**A. AngelList**
- Company profiles for startups
- Funding info (often outdated)
- Employee count ranges
- Hiring status

**B. ProductHunt**
- Product launches
- User engagement metrics
- Founder names
- Category/industry tags

**C. Y Combinator Directory** (if YC company)
- Batch year
- Company description
- Founder names
- Current status

**D. LinkedIn Company Page** (Manual/Limited)
- Employee count range
- Follower count (brand strength)
- Recent posts (activity level)
- **Note:** No API access, manual viewing only

**Implementation:**
```python
async def check_angellist(company_name: str):
    """
    Search AngelList for company profile
    Note: AngelList API is deprecated, would need web scraping
    """
    # Scrape company profile page if exists
    # Extract: employee_count_range, jobs_count, funding_stage
    pass

async def check_producthunt(company_name: str):
    """
    Search ProductHunt for product launches
    """
    # Use PH API or scrape
    # Extract: product_launch_date, upvotes, category
    pass
```

---

## 🗄️ DATABASE SCHEMA

### 1. private_companies (Core Table)
```sql
CREATE TABLE private_companies (
    id SERIAL PRIMARY KEY,
    
    -- Identifiers
    company_name TEXT NOT NULL UNIQUE,
    company_name_normalized TEXT, -- lowercase, no spaces for matching
    legal_name TEXT,
    dba_name TEXT, -- doing business as
    
    -- Basic Info
    website_url TEXT,
    founded_year INT,
    headquarters_city TEXT,
    headquarters_state TEXT,
    headquarters_country TEXT DEFAULT 'United States',
    
    -- Classification
    industry TEXT,
    sector TEXT,
    business_model TEXT, -- B2B, B2C, B2B2C, marketplace, etc.
    company_stage TEXT, -- seed, early, growth, late, unicorn
    
    -- Metrics
    employee_count_estimate INT,
    employee_count_source TEXT,
    employee_count_date DATE,
    
    revenue_estimate_usd NUMERIC,
    revenue_year INT,
    revenue_source TEXT,
    
    total_funding_usd NUMERIC,
    last_funding_date DATE,
    last_funding_amount_usd NUMERIC,
    valuation_estimate_usd NUMERIC,
    
    -- Profile Completeness
    profile_completeness_pct INT, -- 0-100, how many fields populated
    data_quality_score NUMERIC, -- 0-1, agent confidence in data
    source_count INT, -- how many sources contributed
    
    -- Provenance
    primary_source TEXT, -- website, crunchbase, sec, news, etc.
    all_sources TEXT[], -- list of all sources used
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_enrichment_job_id INT,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_acquired BOOLEAN DEFAULT FALSE,
    acquirer_name TEXT,
    acquisition_date DATE,
    
    -- Search & Matching
    description TEXT, -- company description/mission
    tags TEXT[], -- custom tags for categorization
    
    -- Links to Other Tables
    linked_lp_investors INT[], -- references lp_fund.id if LP invested
    linked_fo_investors INT[], -- references family_offices.id
    
    CONSTRAINT valid_completeness CHECK (profile_completeness_pct BETWEEN 0 AND 100),
    CONSTRAINT valid_quality CHECK (data_quality_score BETWEEN 0 AND 1)
);

CREATE INDEX idx_companies_name ON private_companies(company_name_normalized);
CREATE INDEX idx_companies_industry ON private_companies(industry);
CREATE INDEX idx_companies_stage ON private_companies(company_stage);
CREATE INDEX idx_companies_completeness ON private_companies(profile_completeness_pct);
CREATE INDEX idx_companies_updated ON private_companies(last_updated);
```

### 2. company_leadership
```sql
CREATE TABLE company_leadership (
    id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES private_companies(id) ON DELETE CASCADE,
    
    -- Person Details
    full_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    title TEXT NOT NULL, -- CEO, CTO, CFO, etc.
    
    -- Role Details
    is_founder BOOLEAN DEFAULT FALSE,
    is_board_member BOOLEAN DEFAULT FALSE,
    start_date DATE,
    end_date DATE, -- null if current
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Contact (if publicly available)
    email TEXT,
    linkedin_url TEXT,
    twitter_handle TEXT,
    
    -- Provenance
    source_type TEXT, -- website, crunchbase, news, sec
    source_url TEXT,
    confidence_level TEXT CHECK (confidence_level IN ('high', 'medium', 'low')),
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(company_id, full_name, title)
);

CREATE INDEX idx_leadership_company ON company_leadership(company_id);
CREATE INDEX idx_leadership_name ON company_leadership(full_name);
CREATE INDEX idx_leadership_current ON company_leadership(is_current);
```

### 3. company_funding_rounds
```sql
CREATE TABLE company_funding_rounds (
    id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES private_companies(id) ON DELETE CASCADE,
    
    -- Round Details
    round_type TEXT, -- Seed, Series A, B, C, etc., Debt, Convertible Note
    round_date DATE NOT NULL,
    amount_usd NUMERIC,
    valuation_usd NUMERIC, -- post-money valuation
    
    -- Investors
    lead_investor TEXT,
    other_investors TEXT[],
    investor_count INT,
    
    -- Provenance
    source_type TEXT, -- crunchbase, sec_form_d, news, pitchbook
    source_url TEXT,
    sec_form_d_url TEXT, -- if from SEC filing
    confidence_level TEXT CHECK (confidence_level IN ('high', 'medium', 'low')),
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(company_id, round_type, round_date)
);

CREATE INDEX idx_funding_company ON company_funding_rounds(company_id);
CREATE INDEX idx_funding_date ON company_funding_rounds(round_date);
CREATE INDEX idx_funding_amount ON company_funding_rounds(amount_usd);
```

### 4. company_metrics (Time Series)
```sql
CREATE TABLE company_metrics (
    id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES private_companies(id) ON DELETE CASCADE,
    
    -- Metric Details
    metric_date DATE NOT NULL,
    metric_type TEXT NOT NULL, -- revenue, employees, customers, users, etc.
    metric_value NUMERIC NOT NULL,
    metric_unit TEXT, -- USD, count, percent, etc.
    
    -- Context
    is_estimate BOOLEAN DEFAULT FALSE,
    is_annualized BOOLEAN DEFAULT FALSE,
    
    -- Provenance
    source_type TEXT,
    source_url TEXT,
    confidence_level TEXT CHECK (confidence_level IN ('high', 'medium', 'low')),
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(company_id, metric_date, metric_type)
);

CREATE INDEX idx_metrics_company ON company_metrics(company_id);
CREATE INDEX idx_metrics_type ON company_metrics(metric_type);
CREATE INDEX idx_metrics_date ON company_metrics(metric_date);
```

### 5. company_intelligence_jobs
```sql
CREATE TABLE company_intelligence_jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT, -- profile_enrichment, batch_discovery, update_refresh
    
    target_company_id INT,
    target_company_name TEXT,
    
    -- Job Status
    status TEXT CHECK (status IN ('pending', 'running', 'success', 'partial_success', 'failed')),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Results
    strategies_tried TEXT[],
    strategies_successful TEXT[],
    sources_checked INT,
    fields_populated INT,
    profile_completeness_before INT,
    profile_completeness_after INT,
    
    -- Agent Decision Trail
    reasoning_log JSONB,
    
    -- Errors/Warnings
    errors JSONB,
    warnings TEXT[],
    
    -- Resource Usage
    requests_made INT,
    tokens_used INT,
    cost_usd NUMERIC
);

CREATE INDEX idx_intel_jobs_company ON company_intelligence_jobs(target_company_id);
CREATE INDEX idx_intel_jobs_status ON company_intelligence_jobs(status);
```

---

## 📁 FILE STRUCTURE

Create these files:

```
app/
├── agentic/
│   ├── __init__.py
│   ├── company_intelligence_agent.py    # Main orchestrator
│   ├── company_synthesizer.py           # Profile building & deduplication
│   ├── company_validators.py            # Data quality checks
│   └── company_strategies/
│       ├── __init__.py
│       ├── website_strategy.py          # Strategy 1
│       ├── funding_api_strategy.py      # Strategy 2 (Crunchbase/PitchBook)
│       ├── sec_form_d_strategy.py       # Strategy 3
│       ├── news_strategy.py             # Strategy 4 (LLM)
│       ├── jobs_strategy.py             # Strategy 5
│       └── social_signals_strategy.py   # Strategy 6
│
└── api/
    └── v1/
        └── company_intelligence.py       # NEW - API endpoints

app/main.py                               # MODIFY - add router
app/core/config.py                        # MODIFY - add Crunchbase API key
```

---

## 🔌 API ENDPOINTS TO CREATE

### In `app/api/v1/company_intelligence.py`:

```python
from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

router = APIRouter(prefix="/companies", tags=["Company Intelligence"])

@router.post("/enrich")
async def enrich_company(
    company_name: str,
    company_website: Optional[str] = None,
    strategies: Optional[List[str]] = None,  # if None, agent decides
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger agentic enrichment for a single company.
    
    Returns: job_id for tracking
    """
    pass

@router.post("/batch/enrich")
async def batch_enrich_companies(
    company_names: List[str],
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Batch enrichment for multiple companies.
    Processes up to 50 companies at a time.
    """
    pass

@router.post("/discover")
async def discover_companies(
    industry: Optional[str] = None,
    location: Optional[str] = None,
    min_funding: Optional[int] = None,
    limit: int = 20,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Discover new companies matching criteria.
    Uses Google search + agent validation.
    """
    pass

@router.get("/{company_id}")
async def get_company_profile(
    company_id: int,
    db: Session = Depends(get_db)
):
    """
    Get comprehensive company profile including:
    - Basic info
    - Leadership team
    - Funding history
    - Metrics timeline
    - Data quality score
    """
    pass

@router.get("/search")
async def search_companies(
    query: str,
    industry: Optional[str] = None,
    stage: Optional[str] = None,
    min_completeness: int = 50,
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """
    Search companies by name, industry, stage, etc.
    """
    pass

@router.get("/{company_id}/leadership")
async def get_company_leadership(
    company_id: int,
    current_only: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get leadership team for a company
    """
    pass

@router.get("/{company_id}/funding")
async def get_funding_history(
    company_id: int,
    db: Session = Depends(get_db)
):
    """
    Get complete funding history
    """
    pass

@router.get("/intel-jobs/{job_id}")
async def get_job_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed job status including agent reasoning
    """
    pass

@router.post("/{company_id}/refresh")
async def refresh_company_data(
    company_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Refresh data for a company (quarterly update)
    """
    pass

@router.get("/portfolio-companies")
async def get_lp_portfolio_companies(
    investor_id: int,
    investor_type: str,  # "lp" or "family_office"
    db: Session = Depends(get_db)
):
    """
    Get enriched profiles for all portfolio companies of an LP/FO.
    Links to portfolio_companies table from agentic portfolio research.
    """
    pass
```

### Register in `app/main.py`:
```python
from app.api.v1 import company_intelligence

app.include_router(company_intelligence.router, prefix="/api/v1")
```

---

## 📦 DEPENDENCIES TO ADD

Add to `requirements.txt`:

```txt
# Existing (already in project from portfolio research)
httpx>=0.24.0
beautifulsoup4>=4.12.0
lxml>=4.9.0
openai>=1.0.0
anthropic>=0.5.0
robotexclusionrulesparser>=1.7.1

# NEW for Company Intelligence
python-whois>=0.8.0              # WHOIS lookup for company domains
googlesearch-python>=1.2.0       # Google search for company discovery
pycountry>=22.3.5                # Country/location normalization
fuzzywuzzy>=0.18.0               # Fuzzy string matching (company names)
python-Levenshtein>=0.20.0       # Fast string comparison
```

**Optional (if using paid APIs):**
```txt
# Crunchbase API
# (Install via pip if using)

# PitchBook API
# (Requires enterprise subscription)
```

---

## ⚙️ CONFIGURATION

### Add to `app/core/config.py`:

```python
class Settings(BaseSettings):
    # ... existing settings ...
    
    # Company Intelligence Settings
    crunchbase_api_key: Optional[str] = Field(
        default=None,
        description="Crunchbase API key (optional but recommended)"
    )
    
    pitchbook_api_key: Optional[str] = Field(
        default=None,
        description="PitchBook API key (optional, enterprise only)"
    )
    
    # Agent Behavior
    company_intel_max_strategies: int = 6
    company_intel_max_pages_per_site: int = 10
    company_intel_timeout_per_strategy: int = 180  # 3 minutes
    
    # Rate Limiting
    company_intel_requests_per_second: float = 0.5
    company_intel_max_concurrent: int = 3
    
    # LLM Settings (reuse from portfolio research)
    # openai_api_key, anthropic_api_key already defined
```

### Add to `.env`:
```
CRUNCHBASE_API_KEY=your_key_here  # Optional
PITCHBOOK_API_KEY=your_key_here   # Optional
```

---

## 🧠 AGENT DECISION LOGIC

### Core Decision Framework:

```python
class CompanyIntelligenceAgent:
    def plan_strategy(self, company_data: dict) -> List[dict]:
        """
        Agent analyzes company and plans enrichment strategies
        
        company_data = {
            'name': 'Stripe',
            'website': 'stripe.com',
            'industry': 'fintech',
            'known_funding': True  # if we know they raised VC
        }
        """
        strategies = []
        
        # Rule: Always start with company website (primary source)
        if company_data.get('website'):
            strategies.append({
                'method': 'website_scraping',
                'priority': 10,
                'confidence': 0.8,
                'reasoning': 'Official website is most authoritative source'
            })
        
        # Rule: If startup/tech company → check Crunchbase
        if company_data.get('industry') in ['technology', 'fintech', 'saas', 'biotech']:
            strategies.append({
                'method': 'funding_api',
                'priority': 9,
                'confidence': 0.9,
                'reasoning': 'Tech companies often tracked in Crunchbase'
            })
        
        # Rule: If likely raised >$1M → check SEC Form D
        if company_data.get('known_funding') or company_data.get('industry') in ['technology', 'biotech']:
            strategies.append({
                'method': 'sec_form_d',
                'priority': 8,
                'confidence': 0.7,
                'reasoning': 'May have filed Form D for fundraising'
            })
        
        # Rule: If any press coverage → search news
        strategies.append({
            'method': 'news_search',
            'priority': 7,
            'confidence': 0.6,
            'reasoning': 'News often has revenue/growth estimates'
        })
        
        # Rule: If hiring → check job postings
        if company_data.get('website'):
            strategies.append({
                'method': 'jobs_analysis',
                'priority': 6,
                'confidence': 0.5,
                'reasoning': 'Job postings signal growth and capabilities'
            })
        
        # Rule: If startup → check AngelList/ProductHunt
        if company_data.get('stage') in ['seed', 'early'] or company_data.get('industry') == 'technology':
            strategies.append({
                'method': 'social_signals',
                'priority': 5,
                'confidence': 0.4,
                'reasoning': 'Startups often listed on AngelList/ProductHunt'
            })
        
        return sorted(strategies, key=lambda x: x['priority'], reverse=True)
    
    def should_continue(self, current_profile: dict, strategies_tried: List, time_elapsed: int) -> tuple:
        """
        Agent decides whether to try more strategies or stop
        """
        completeness = self.calculate_completeness(current_profile)
        
        # Good completeness achieved
        if completeness >= 80 and len(strategies_tried) >= 3:
            return False, f"Good completeness achieved ({completeness}%)"
        
        # Tried everything, best effort
        if len(strategies_tried) >= 6:
            return False, "All strategies attempted"
        
        # Time limit
        if time_elapsed > 600:  # 10 minutes
            return False, "Time limit reached"
        
        # Poor completeness, keep trying
        if completeness < 50 and len(strategies_tried) < 4:
            return True, f"Completeness low ({completeness}%), trying more sources"
        
        # Single source only, seek validation
        if len(set(current_profile.get('sources', []))) == 1:
            return True, "Single source, seeking validation"
        
        return False, "Adequate coverage reached"
    
    def calculate_completeness(self, profile: dict) -> int:
        """
        Calculate profile completeness (0-100%)
        
        Key fields to check:
        - company_name ✓ (required)
        - website_url
        - founded_year
        - headquarters_city
        - industry
        - employee_count_estimate
        - revenue_estimate_usd
        - total_funding_usd
        - description
        - leadership (at least 1 executive)
        """
        total_fields = 10
        populated = 0
        
        # Required field
        populated += 1  # company_name always populated
        
        # Optional but valuable fields
        if profile.get('website_url'): populated += 1
        if profile.get('founded_year'): populated += 1
        if profile.get('headquarters_city'): populated += 1
        if profile.get('industry'): populated += 1
        if profile.get('employee_count_estimate'): populated += 1
        if profile.get('revenue_estimate_usd'): populated += 1
        if profile.get('total_funding_usd'): populated += 1
        if profile.get('description'): populated += 1
        if profile.get('leadership_count', 0) > 0: populated += 1
        
        return int((populated / total_fields) * 100)
```

---

## 🔄 DATA SYNTHESIS

### Profile Building Logic:

```python
class CompanySynthesizer:
    def build_profile(self, company_name: str, findings: List[dict]) -> dict:
        """
        Synthesize findings from multiple sources into unified profile
        
        Priority order for conflicts:
        1. Company website (official source)
        2. SEC filings (regulatory, verified)
        3. Crunchbase/PitchBook (curated databases)
        4. News articles (may have estimates)
        5. Social signals (weakest)
        """
        
        profile = {
            'company_name': company_name,
            'sources': [],
            'data_quality_score': 0.0
        }
        
        # Group findings by source type
        by_source = defaultdict(list)
        for finding in findings:
            by_source[finding['source_type']].append(finding)
        
        # Merge data with priority
        for source_type in ['website', 'sec_form_d', 'crunchbase', 'news', 'social']:
            if source_type in by_source:
                profile = self._merge_source_data(profile, by_source[source_type], source_type)
        
        # Calculate data quality score
        profile['data_quality_score'] = self._calculate_quality_score(profile)
        profile['profile_completeness_pct'] = self._calculate_completeness(profile)
        profile['source_count'] = len(set(profile['sources']))
        
        return profile
    
    def _merge_source_data(self, profile: dict, findings: List[dict], source_type: str) -> dict:
        """
        Merge data from a specific source type into profile
        """
        for finding in findings:
            profile['sources'].append(source_type)
            
            # Merge fields (keep first non-null value, with priority)
            for field, value in finding.items():
                if field not in profile or profile[field] is None:
                    profile[field] = value
                elif source_type == 'website' and field in profile:
                    # Website data takes precedence (most authoritative)
                    profile[field] = value
        
        return profile
    
    def _calculate_quality_score(self, profile: dict) -> float:
        """
        Score data quality 0-1 based on:
        - Number of sources (more is better)
        - Source types (website + SEC = higher quality)
        - Field agreement (same values from multiple sources)
        """
        score = 0.0
        
        # Source count (max 0.4)
        source_count = len(set(profile.get('sources', [])))
        score += min(source_count * 0.1, 0.4)
        
        # High-quality sources present (max 0.3)
        if 'website' in profile['sources']:
            score += 0.15
        if 'sec_form_d' in profile['sources'] or 'crunchbase' in profile['sources']:
            score += 0.15
        
        # Profile completeness (max 0.3)
        score += (profile.get('profile_completeness_pct', 0) / 100) * 0.3
        
        return min(score, 1.0)
    
    def normalize_company_name(self, raw_name: str) -> str:
        """
        Normalize company name for matching
        - Lowercase
        - Remove Inc, LLC, Ltd, Corp, etc.
        - Remove extra spaces
        - Handle common variations
        """
        name = raw_name.lower().strip()
        
        # Remove legal entity types
        suffixes = [' inc', ' inc.', ' llc', ' ltd', ' ltd.', ' corp', ' corp.', 
                   ' corporation', ' company', ' co.', ' co']
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Remove extra spaces
        name = ' '.join(name.split())
        
        # Remove special characters for matching
        name = re.sub(r'[^\w\s]', '', name)
        
        return name
```

---

## 🎯 IMPLEMENTATION PHASES

### PHASE 1 (Quick Win): Website + SEC Form D - Week 1
**Goal:** Build core infrastructure and 2 basic strategies

**Tasks:**
1. Create database tables
2. Implement `CompanyIntelligenceAgent` orchestrator
3. Implement Strategy 1 (Website scraping)
4. Implement Strategy 3 (SEC Form D)
5. Implement basic `CompanySynthesizer`
6. Create API endpoints for `/enrich` and `GET /{company_id}`
7. Test with 20 companies

**Success Criteria:**
- ✅ 50-70% profile completeness for tested companies
- ✅ Agent reasoning logs are clear
- ✅ No duplicate data
- ✅ API endpoints work

---

### PHASE 2: Funding APIs + News - Week 2
**Goal:** Add high-value data sources

**Tasks:**
1. Implement Strategy 2 (Crunchbase API or scraping)
2. Implement Strategy 4 (News search with LLM)
3. Improve `CompanySynthesizer` deduplication
4. Add validation logic
5. Test with 50 companies

**Success Criteria:**
- ✅ 70-85% completeness for tech companies
- ✅ Funding data for 40-60% of companies
- ✅ LLM extraction accuracy >75%
- ✅ Cost per company <$0.20

---

### PHASE 3: Jobs + Social Signals - Week 3
**Goal:** Add indirect signals

**Tasks:**
1. Implement Strategy 5 (Jobs analysis)
2. Implement Strategy 6 (Social signals)
3. Add growth stage classification
4. Add tech stack inference
5. Test with 100 companies

**Success Criteria:**
- ✅ 80-90% completeness for well-known companies
- ✅ Growth signals captured
- ✅ 5+ sources per company on average

---

### PHASE 4: Integration + Analytics - Week 4
**Goal:** Connect to LP/FO portfolio data

**Tasks:**
1. Link to `portfolio_companies` table from agentic portfolio research
2. Auto-enrich portfolio companies when discovered
3. Create analytics endpoints
4. Add bulk enrichment
5. Add refresh jobs (quarterly updates)

**Success Criteria:**
- ✅ All LP/FO portfolio companies enriched
- ✅ Batch processing works
- ✅ Refresh mechanism in place

---

## ✅ SUCCESS CRITERIA

### Minimum Viable (Phase 1):
- [ ] 20 companies enriched with 50-70% completeness
- [ ] Website + SEC strategies working
- [ ] Agent decision logic sound
- [ ] API endpoints functional

### Full Success (Phase 4):
- [ ] 500-1,000 companies enriched (starting with LP/FO portfolios)
- [ ] 80-90% average completeness
- [ ] Average 4+ sources per company
- [ ] <5% duplicate leadership records
- [ ] Cost <$0.20 per company
- [ ] Agent completes in 5-10 minutes per company
- [ ] Integration with portfolio data working
- [ ] Quarterly refresh jobs automated

---

## 🛡️ SAFEGUARDS & COMPLIANCE

### Rate Limiting:
```python
class CompanyIntelRateLimiter:
    """Per-domain rate limiting"""
    def __init__(self):
        self.default_rps = 0.5  # 1 request per 2 seconds
        self.domain_limits = {}
    
    async def acquire(self, url: str):
        domain = urlparse(url).netloc
        
        if domain not in self.domain_limits:
            self.domain_limits[domain] = {
                'semaphore': asyncio.Semaphore(1),
                'last_request': 0
            }
        
        limiter = self.domain_limits[domain]
        
        async with limiter['semaphore']:
            elapsed = time.time() - limiter['last_request']
            wait_time = (1.0 / self.default_rps) - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            limiter['last_request'] = time.time()
            yield
```

### Ethical Guidelines:
- ✅ Only collect publicly disclosed business information
- ✅ No personal information (home addresses, personal emails, SSNs)
- ✅ Respect robots.txt on all domains
- ✅ Rate limiting to avoid overloading servers
- ✅ Proper User-Agent identification
- ✅ No authentication bypass
- ✅ Comply with API terms of service

### Data Quality:
- ✅ Multi-source validation
- ✅ Confidence scoring
- ✅ Source attribution
- ✅ Timestamp all data
- ✅ Handle stale data (>1 year old flagged)

---

## 📊 TESTING PLAN

### Test Companies (Diverse Set):

```python
TEST_COMPANIES = [
    # Tech unicorns (should have high coverage)
    {'name': 'Stripe', 'expected_completeness': 90, 'expected_sources': ['website', 'crunchbase', 'sec', 'news']},
    {'name': 'Databricks', 'expected_completeness': 85, 'expected_sources': ['website', 'crunchbase', 'news']},
    {'name': 'SpaceX', 'expected_completeness': 80, 'expected_sources': ['website', 'sec', 'news']},
    
    # Mid-size tech companies
    {'name': 'Figma', 'expected_completeness': 85, 'expected_sources': ['website', 'crunchbase', 'producthunt']},
    {'name': 'Notion', 'expected_completeness': 80, 'expected_sources': ['website', 'crunchbase', 'news']},
    
    # Biotech/healthcare
    {'name': 'Moderna', 'expected_completeness': 90, 'expected_sources': ['website', 'sec', 'news']},  # Note: now public
    {'name': '23andMe', 'expected_completeness': 75, 'expected_sources': ['website', 'news']},
    
    # Less well-known (lower expected coverage)
    {'name': 'Acme Startup', 'expected_completeness': 30, 'expected_sources': ['website']},
]
```

### Validation Queries:

```sql
-- Check profile completeness distribution
SELECT 
    CASE 
        WHEN profile_completeness_pct >= 80 THEN 'Excellent (80-100%)'
        WHEN profile_completeness_pct >= 60 THEN 'Good (60-79%)'
        WHEN profile_completeness_pct >= 40 THEN 'Fair (40-59%)'
        ELSE 'Poor (<40%)'
    END as completeness_tier,
    COUNT(*) as company_count,
    AVG(source_count) as avg_sources
FROM private_companies
GROUP BY completeness_tier
ORDER BY MIN(profile_completeness_pct) DESC;

-- Check data quality scores
SELECT 
    AVG(data_quality_score) as avg_quality,
    AVG(profile_completeness_pct) as avg_completeness,
    AVG(source_count) as avg_sources,
    COUNT(*) as total_companies
FROM private_companies;

-- Leadership coverage
SELECT 
    pc.company_name,
    COUNT(cl.id) as exec_count
FROM private_companies pc
LEFT JOIN company_leadership cl ON pc.id = cl.company_id
GROUP BY pc.company_name
HAVING COUNT(cl.id) = 0;  -- Companies with no leadership data

-- Funding data coverage
SELECT 
    COUNT(DISTINCT company_id) as companies_with_funding,
    COUNT(*) as total_rounds,
    SUM(amount_usd) as total_funding
FROM company_funding_rounds;
```

---

## 💰 COST ESTIMATES

### Per Company:
- **HTTP Requests:** 15-30 (website, SEC, jobs pages)
- **Crunchbase API:** 1 call ($0.01 if paid tier)
- **LLM API Calls:** 2-4 (news extraction)
- **Tokens:** ~5,000-8,000 (at $0.01/1K = $0.05-0.08)
- **Total Cost:** $0.10-0.20 per company

### For 500 Companies (Initial):
- **Total Cost:** $50-100
- **Time:** ~40-80 hours (automated)

### For 1,000 Companies:
- **Total Cost:** $100-200
- **Time:** ~80-160 hours (automated, can run overnight)

### Quarterly Refresh (Updates Only):
- **Cost per company:** $0.05-0.10 (only refresh changed data)
- **Total for 1,000:** $50-100/quarter

---

## 🔗 INTEGRATION WITH PORTFOLIO DATA

### Link to LP/FO Portfolios:

```sql
-- Auto-enrich portfolio companies
WITH portfolio_cos AS (
    SELECT DISTINCT 
        company_name,
        investor_id,
        investor_type
    FROM portfolio_companies
    WHERE company_name NOT IN (SELECT company_name FROM private_companies)
)
INSERT INTO company_intelligence_jobs (job_type, target_company_name, status)
SELECT 
    'profile_enrichment',
    company_name,
    'pending'
FROM portfolio_cos;

-- Link enriched companies back to investors
UPDATE private_companies pc
SET 
    linked_lp_investors = ARRAY(
        SELECT DISTINCT investor_id 
        FROM portfolio_companies 
        WHERE company_name = pc.company_name 
          AND investor_type = 'lp'
    ),
    linked_fo_investors = ARRAY(
        SELECT DISTINCT investor_id 
        FROM portfolio_companies 
        WHERE company_name = pc.company_name 
          AND investor_type = 'family_office'
    );
```

### Use Case Example:

```python
# Get all portfolio companies for an LP with enriched profiles
@router.get("/lp/{lp_id}/portfolio-enriched")
async def get_enriched_portfolio(lp_id: int, db: Session):
    """
    Get LP's portfolio companies with full intelligence profiles
    """
    
    # Get portfolio companies
    portfolio = db.query(PortfolioCompany).filter(
        PortfolioCompany.investor_id == lp_id,
        PortfolioCompany.investor_type == 'lp'
    ).all()
    
    # Get enriched profiles
    company_names = [p.company_name for p in portfolio]
    enriched = db.query(PrivateCompany).filter(
        PrivateCompany.company_name.in_(company_names)
    ).all()
    
    # Combine data
    result = []
    for p in portfolio:
        enriched_profile = next((e for e in enriched if e.company_name == p.company_name), None)
        result.append({
            'investment_date': p.investment_date,
            'investment_amount': p.investment_amount_usd,
            'company_profile': enriched_profile.__dict__ if enriched_profile else None
        })
    
    return result
```

---

## 📖 REFERENCE DOCUMENTATION

All detailed documentation is in the Nexdata repository:

- **Project Rules:** `RULES.md` - Read for data collection guidelines
- **Similar Agentic Implementation:** `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md`
- **Plugin Pattern Example:** `app/sources/census/`
- **Existing LP/FO Data:** Tables `lp_fund`, `family_offices`, `portfolio_companies`

---

## 🚀 GETTING STARTED

1. **Read project rules:** `RULES.md`
2. **Review agentic portfolio system:** `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md` (similar architecture)
3. **Create database tables** (run migrations)
4. **Install dependencies:** `pip install googlesearch-python fuzzywuzzy python-Levenshtein`
5. **Add API keys to `.env`:** `OPENAI_API_KEY=sk-...` (required), `CRUNCHBASE_API_KEY=...` (optional)
6. **Start with Phase 1:** Website + SEC Form D strategies
7. **Test with 20 companies:** Validate approach before scaling
8. **Link to portfolio data:** Auto-enrich LP/FO portfolio companies

---

## 🔄 QUARTERLY REFRESH STRATEGY

### Automated Updates:

```python
async def schedule_quarterly_refresh():
    """
    Refresh data for companies that:
    - Are marked as active
    - Are in LP/FO portfolios
    - Haven't been updated in 90+ days
    """
    
    stale_companies = db.query(PrivateCompany).filter(
        PrivateCompany.is_active == True,
        PrivateCompany.last_updated < datetime.now() - timedelta(days=90)
    ).all()
    
    for company in stale_companies:
        # Queue refresh job
        job = CompanyIntelligenceJob(
            job_type='update_refresh',
            target_company_id=company.id,
            target_company_name=company.company_name,
            status='pending'
        )
        db.add(job)
    
    db.commit()
```

---

## 📈 EXPECTED TIMELINE

- **Phase 1 (Website + SEC):** 5-7 days
- **Phase 2 (Funding + News):** 5-7 days  
- **Phase 3 (Jobs + Social):** 5-7 days
- **Phase 4 (Integration):** 3-5 days
- **Testing & Refinement:** 5-7 days

**Total:** 4-5 weeks for full implementation

**Quick Win Option:** Phase 1 only (Website + SEC) in 1 week!

---

## 💡 KEY INSIGHTS

### What Makes This Valuable:

1. **No Competitor Has This:**
   - Most services have public company data only
   - Private company data requires expensive subscriptions (Crunchbase Pro, PitchBook)
   - Your agentic system aggregates from free sources

2. **Complements Your Data:**
   - LPs invest in private companies (from portfolio research)
   - Private company executives are contacts (from contact research)
   - Private company metrics inform investment decisions

3. **Continuous Value:**
   - Quarterly refreshes keep data current
   - New companies auto-discovered from LP portfolios
   - Growing dataset becomes more valuable over time

4. **Unique Insights:**
   - Cross-reference funding rounds with LP investments
   - Track executive movements across portfolio companies
   - Identify acquisition candidates for LPs/FOs

---

## ❓ QUESTIONS TO RESOLVE

If anything is unclear:
1. Check project rules: `RULES.md`
2. Review similar implementation: `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md`
3. Look at existing plugin structure: `app/sources/census/`
4. Ask the original agent/user for clarification

---

## 🎉 FINAL NOTES

**This is a differentiating asset.** Private company intelligence at scale requires:
- Multi-source synthesis (no single API has it all)
- Adaptive reasoning (each company discloses differently)
- LLM-powered extraction (unstructured news/reports)
- Continuous validation (cross-source checks)

You're building something competitors will struggle to replicate! 🚀

**Start with Phase 1 (Website + SEC) for quick wins, then expand!** 💪

**Good luck!** 🎯
