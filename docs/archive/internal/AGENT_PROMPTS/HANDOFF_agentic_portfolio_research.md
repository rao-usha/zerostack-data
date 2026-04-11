# AGENT HANDOFF: Agentic LP/FO Portfolio Research System

---

## 🎯 MISSION

Build an **agentic system** that automatically discovers and tracks portfolio companies, investments, and deal flow for Limited Partners (LPs) and Family Offices (FOs) by intelligently combining data from multiple sources.

**Why This Matters:**
- No single API has this data
- Current database has 131 LPs and 100 FOs but ZERO portfolio/investment data
- Manual research takes 30-60 min per investor
- This agent will automate it to 3-5 minutes with higher quality

---

## 📋 WHAT YOU'RE BUILDING

### Core Deliverables:
1. **Agentic orchestrator** that plans which sources to check based on investor type
2. **5 collection strategies** (SEC 13F, website scraping, PDF parsing, news extraction, reverse search)
3. **Data synthesis engine** that deduplicates and merges findings from multiple sources
4. **Job tracking system** with full agent reasoning logs
5. **API endpoints** for triggering collection and viewing results

### Database Tables to Create:
- `portfolio_companies` - Investment holdings
- `co_investments` - Co-investor network
- `investor_themes` - Investment pattern classification
- `agentic_collection_jobs` - Job tracking with reasoning

### Expected Results:
- **80-100 LPs** (60-75%) with 5+ portfolio companies identified
- **40-60 FOs** (40-60%) with investment history
- **50+ co-investor relationships** mapped
- **Average 3+ sources** per investor (cross-validation)

---

## 📚 BACKGROUND CONTEXT

### Current State:
- **Database:** PostgreSQL with existing tables `lp_fund` (131 records) and `family_offices` (100 records)
- **Tech Stack:** FastAPI + SQLAlchemy + httpx + asyncio
- **Data Collected:** Basic LP/FO profiles (name, AUM, location, website) but NO portfolio data
- **Project Structure:** Plugin architecture with `app/sources/{source_name}/` for each data source

### Why Agentic (Not Traditional API):
Portfolio data is scattered across:
- SEC 13F filings (API) - only public equity holdings
- Official websites (HTML) - some LPs publish portfolio pages
- Annual reports (PDF) - public pensions publish detailed CAFRs
- Press releases (unstructured text) - recent deals announced
- Portfolio company websites (reverse search) - companies often list their investors

**No single source has complete data → Agent must reason about where to look**

---

## 🤖 HOW THE AGENT WORKS

### Agent Workflow:
```
1. PLAN: Agent analyzes investor → decides which strategies to try
2. EXECUTE: Agent runs strategies in priority order
3. SYNTHESIZE: Agent deduplicates and merges findings
4. VALIDATE: Agent scores confidence based on source quality
5. LOG: Agent records full reasoning trail for debugging
```

### Example Agent Decision:
```
Input: CalPERS (Public Pension, $450B AUM, has website)

Agent Reasoning:
→ "AUM > $100M → likely files SEC 13F" (priority: 10)
→ "Type = public pension → publishes CAFR" (priority: 10)  
→ "Has website → check for portfolio page" (priority: 8)
→ "Large investor → search news for deals" (priority: 7)

Execution:
✓ SEC 13F: Found 150 public equity holdings
✓ Annual Report PDF: Found 50 PE/VC investments
✓ Website: Found 20 additional holdings
✓ News: Found 15 recent deals

Synthesis:
→ Total: 220 unique companies (after deduplication)
→ Confidence: HIGH (4 sources, high agreement)
→ Store with full provenance
```

---

## 🔧 THE 5 COLLECTION STRATEGIES

### Strategy 1: SEC 13F Filings (API-based)
**What:** Extract public equity holdings from SEC 13F filings
**Coverage:** 40-60 large investors with >$100M equity
**API:** `https://www.sec.gov/cgi-bin/browse-edgar`
**Confidence:** HIGH (regulatory filing)

**Implementation:**
- Search SEC EDGAR for 13F filers by investor name
- Download most recent 13F-HR XML filing
- Parse holdings table (ticker, shares, value, date)
- Resolve tickers to company names
- Store with `source_type='sec_13f'`, `confidence_level='high'`

---

### Strategy 2: Website Portfolio Scraping (HTML parsing)
**What:** Find and scrape official portfolio pages on investor websites
**Coverage:** 60-80 investors (those with public portfolios)
**Confidence:** MEDIUM-HIGH

**Implementation:**
- Fetch investor homepage from `lp_fund.website_url` or `family_offices.website`
- Search for links containing keywords: "portfolio", "investments", "companies", "holdings"
- Scrape up to 3 portfolio pages (bounded)
- Extract company names, industries, dates using pattern matching
- Store with `source_type='website'`, `confidence_level='medium'`

**Safeguards:**
- Rate limiting: 1 request per 2 seconds per domain
- Respect robots.txt
- Max 5 pages per investor (prevent runaway)
- Timeout: 10 seconds per request

---

### Strategy 3: Annual Report PDF Parsing
**What:** Extract portfolio sections from PDF annual reports (CAFRs)
**Coverage:** 50-70 public pensions and endowments
**Confidence:** HIGH (official publication)

**Implementation:**
- Search investor website for "Annual Report", "CAFR", "Investment Report" links
- Download most recent PDF
- Extract text using pdfplumber or PyPDF2
- Find section with headers like "Portfolio", "Investment Holdings", "Schedule of Investments"
- Parse tables/lists in that section
- Store with `source_type='annual_report'`, `confidence_level='high'`

---

### Strategy 4: Press Release & News Search (LLM extraction)
**What:** Search news for investment announcements and extract structured data
**Coverage:** 30-50 active investors (those with recent press coverage)
**Confidence:** MEDIUM

**Implementation:**
- Search Google News: `"{investor_name}" investment` OR `"invests in"`
- Filter last 2 years
- For top 20-30 articles:
  - Fetch article text
  - Use LLM (GPT-4 or Claude) to extract:
    - Company invested in
    - Investment date
    - Investment amount
    - Co-investors
    - Industry/theme
- Store with `source_type='news'`, `confidence_level='medium'`

**LLM Prompt Template:**
```
Extract investment information from this article about {investor_name}.

Article text: {text[:2000]}

Return JSON:
{
  "company_name": "string or null",
  "investment_date": "YYYY-MM-DD or null",
  "investment_amount_usd": number or null,
  "co_investors": ["list"],
  "company_industry": "string or null"
}

Return null if no investment found.
```

---

### Strategy 5: Portfolio Company Back-References (Reverse search)
**What:** Search for companies that mention this investor on their website
**Coverage:** 20-40 companies per large investor
**Confidence:** HIGH (company confirms relationship)

**Implementation:**
- Google search: `"{investor_name}" (investor OR "backed by" OR portfolio)`
- Filter out news sites, LinkedIn, SEC.gov
- Check company websites for investor mentions
- Extract company info (name, industry, location)
- Store with `source_type='portfolio_company_website'`, `confidence_level='high'`

---

## 🗄️ DATABASE SCHEMA

### 1. portfolio_companies
```sql
CREATE TABLE portfolio_companies (
    id SERIAL PRIMARY KEY,
    investor_id INT NOT NULL,
    investor_type TEXT NOT NULL CHECK (investor_type IN ('lp', 'family_office')),
    
    -- Company Details
    company_name TEXT NOT NULL,
    company_website TEXT,
    company_industry TEXT,
    company_stage TEXT,
    company_location TEXT,
    
    -- Investment Details
    investment_type TEXT, -- equity, PE, VC, real_estate, etc.
    investment_date DATE,
    investment_amount_usd NUMERIC,
    ownership_percentage NUMERIC,
    current_holding BOOLEAN DEFAULT TRUE,
    exit_date DATE,
    exit_type TEXT,
    
    -- Data Provenance
    source_type TEXT NOT NULL, -- sec_13f, website, annual_report, news, etc.
    source_url TEXT,
    confidence_level TEXT CHECK (confidence_level IN ('high', 'medium', 'low')),
    collected_date DATE DEFAULT CURRENT_DATE,
    last_verified_date DATE,
    
    -- Agent Metadata
    collection_method TEXT DEFAULT 'agentic_search',
    agent_reasoning TEXT,
    
    UNIQUE(investor_id, investor_type, company_name, investment_date)
);

CREATE INDEX idx_portfolio_investor ON portfolio_companies(investor_id, investor_type);
CREATE INDEX idx_portfolio_company ON portfolio_companies(company_name);
CREATE INDEX idx_portfolio_current ON portfolio_companies(current_holding);
```

### 2. co_investments
```sql
CREATE TABLE co_investments (
    id SERIAL PRIMARY KEY,
    primary_investor_id INT NOT NULL,
    primary_investor_type TEXT NOT NULL,
    co_investor_name TEXT NOT NULL,
    co_investor_type TEXT,
    
    deal_name TEXT,
    deal_date DATE,
    deal_size_usd NUMERIC,
    
    co_investment_count INT DEFAULT 1,
    
    source_type TEXT,
    source_url TEXT,
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(primary_investor_id, primary_investor_type, co_investor_name, deal_name)
);

CREATE INDEX idx_coinvest_primary ON co_investments(primary_investor_id, primary_investor_type);
```

### 3. investor_themes
```sql
CREATE TABLE investor_themes (
    id SERIAL PRIMARY KEY,
    investor_id INT NOT NULL,
    investor_type TEXT NOT NULL,
    
    theme_category TEXT, -- sector, geography, stage, asset_class
    theme_value TEXT, -- e.g., "climate_tech", "healthcare"
    
    investment_count INT,
    percentage_of_portfolio NUMERIC,
    
    confidence_level TEXT,
    evidence_sources TEXT[],
    
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(investor_id, investor_type, theme_category, theme_value)
);
```

### 4. agentic_collection_jobs
```sql
CREATE TABLE agentic_collection_jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT, -- portfolio_discovery, deal_flow_update, etc.
    
    target_investor_id INT,
    target_investor_type TEXT,
    target_investor_name TEXT,
    
    status TEXT CHECK (status IN ('pending', 'running', 'success', 'partial_success', 'failed')),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Results
    sources_checked INT,
    sources_successful INT,
    companies_found INT,
    new_companies INT,
    updated_companies INT,
    
    -- Agent Decision Trail
    strategy_used TEXT[],
    reasoning_log JSONB,
    
    errors JSONB,
    warnings TEXT[],
    
    requests_made INT,
    tokens_used INT,
    cost_usd NUMERIC
);
```

---

## 📁 FILE STRUCTURE

Create these files in the project:

```
app/
├── agentic/                               # NEW DIRECTORY
│   ├── __init__.py
│   ├── portfolio_agent.py                 # Main orchestrator
│   ├── synthesizer.py                     # Deduplication & merging
│   ├── validators.py                      # Data quality checks
│   └── strategies/                        # Collection strategies
│       ├── __init__.py
│       ├── sec_13f_strategy.py           # Strategy 1
│       ├── website_strategy.py           # Strategy 2
│       ├── annual_report_strategy.py     # Strategy 3
│       ├── news_strategy.py              # Strategy 4 (uses LLM)
│       └── reverse_search_strategy.py    # Strategy 5
│
└── api/
    └── v1/
        └── agentic_research.py           # NEW - API endpoints

app/main.py                                # MODIFY - add router
app/core/config.py                         # MODIFY - add LLM API keys
```

---

## 🔌 API ENDPOINTS TO CREATE

### In `app/api/v1/agentic_research.py`:

```python
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

router = APIRouter(prefix="/agentic", tags=["Agentic Research"])

@router.post("/portfolio/collect")
async def trigger_portfolio_collection(
    investor_id: int,
    investor_type: str,  # "lp" or "family_office"
    strategies: Optional[List[str]] = None,  # if None, agent decides
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger agentic portfolio collection for a single investor.
    
    Returns: job_id for tracking
    """
    pass

@router.post("/portfolio/batch")
async def batch_portfolio_collection(
    investor_type: str,
    limit: int = 10,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Batch collection for multiple investors.
    Prioritizes investors with missing portfolio data.
    """
    pass

@router.get("/portfolio/{investor_id}/summary")
async def get_portfolio_summary(
    investor_id: int,
    investor_type: str,
    db: Session = Depends(get_db)
):
    """
    Get portfolio summary:
    - Total companies found
    - Source breakdown
    - Investment themes
    - Co-investors
    - Data completeness score
    """
    pass

@router.get("/jobs/{job_id}")
async def get_job_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed job status including agent reasoning log
    """
    pass

@router.get("/co-investors/{investor_id}")
async def get_co_investors(
    investor_id: int,
    investor_type: str,
    min_count: int = 2,
    db: Session = Depends(get_db)
):
    """
    Find investors who frequently co-invest with this investor
    """
    pass
```

### Register in `app/main.py`:
```python
from app.api.v1 import agentic_research

app.include_router(agentic_research.router, prefix="/api/v1")
```

---

## 📦 DEPENDENCIES TO ADD

Add to `requirements.txt`:

```txt
# Existing (already in project)
httpx>=0.24.0
beautifulsoup4>=4.12.0
lxml>=4.9.0
pdfplumber>=0.10.0
robotexclusionrulesparser>=1.7.1

# NEW - Add these
openai>=1.0.0                    # LLM for entity extraction (Strategy 4)
anthropic>=0.5.0                 # Alternative LLM
newspaper3k>=0.2.8               # News article text extraction
yfinance>=0.2.0                  # Stock ticker → company name lookup
```

---

## ⚙️ CONFIGURATION

### Add to `app/core/config.py`:

```python
class Settings(BaseSettings):
    # ... existing settings ...
    
    # Agentic Collection Settings
    openai_api_key: Optional[str] = Field(
        default=None,
        description="OpenAI API key for LLM-powered entity extraction"
    )
    anthropic_api_key: Optional[str] = Field(
        default=None,
        description="Anthropic API key (alternative to OpenAI)"
    )
    
    # Agent Behavior
    agentic_max_strategies_per_job: int = 5
    agentic_max_requests_per_strategy: int = 20
    agentic_timeout_per_strategy: int = 300  # 5 minutes
    
    # Rate Limiting (per domain)
    agentic_requests_per_second: float = 0.5  # 1 request per 2 seconds
    agentic_max_concurrent: int = 3
    
    # LLM Settings
    agentic_llm_model: str = "gpt-4"
    agentic_llm_max_tokens: int = 500
```

### Add to `.env`:
```
OPENAI_API_KEY=sk-...
# OR
ANTHROPIC_API_KEY=sk-ant-...
```

---

## 🧠 AGENT DECISION LOGIC

### Core Decision Framework:

```python
class PortfolioResearchAgent:
    def plan_strategy(self, investor_data: dict) -> List[dict]:
        """
        Agent analyzes investor and plans which strategies to try
        
        Returns: List of strategies with priority and reasoning
        """
        strategies = []
        
        # Rule: Large AUM → likely files 13F
        if investor_data['aum_usd'] and investor_data['aum_usd'] > 100_000_000:
            strategies.append({
                'method': 'sec_13f',
                'priority': 10,
                'confidence': 0.9,
                'reasoning': 'AUM > $100M suggests 13F filing requirement'
            })
        
        # Rule: Has website → try scraping portfolio page
        if investor_data.get('website_url'):
            strategies.append({
                'method': 'website_scraping',
                'priority': 9,
                'confidence': 0.7,
                'reasoning': 'Official website likely has portfolio section'
            })
        
        # Rule: Public pension → publishes detailed CAFR
        if investor_data['type'] == 'public_pension':
            strategies.append({
                'method': 'annual_report_pdf',
                'priority': 10,
                'confidence': 0.95,
                'reasoning': 'Public pensions required to publish CAFRs'
            })
        
        # Rule: Family office → search news (limited public data)
        if investor_data['type'] == 'family_office':
            strategies.append({
                'method': 'news_search',
                'priority': 8,
                'confidence': 0.5,
                'reasoning': 'Family offices rarely disclose; press releases main source'
            })
        
        # Always try reverse search (low cost, decent yield)
        strategies.append({
            'method': 'reverse_search',
            'priority': 6,
            'confidence': 0.6,
            'reasoning': 'Portfolio companies often list their investors'
        })
        
        return sorted(strategies, key=lambda x: x['priority'], reverse=True)
    
    def should_continue(self, current_results: List, strategies_tried: List, time_elapsed: int) -> tuple:
        """
        Agent decides whether to try more strategies or stop
        """
        # Good coverage achieved
        if len(current_results) >= 10 and len(strategies_tried) >= 3:
            return False, "Sufficient coverage achieved"
        
        # No results after trying everything
        if len(strategies_tried) >= 5 and len(current_results) == 0:
            return False, "No data found after all strategies"
        
        # Time limit
        if time_elapsed > 600:  # 10 minutes
            return False, "Time limit reached"
        
        # Poor coverage, keep trying
        if len(current_results) < 5:
            return True, "Coverage below threshold, continuing"
        
        # Single source only, need validation
        unique_sources = set(r['source_type'] for r in current_results)
        if len(unique_sources) == 1:
            return True, "Single source detected, seeking validation"
        
        return False, "Default stop condition"
```

---

## 🔄 DATA SYNTHESIS

### Deduplication & Merging Logic:

```python
class DataSynthesizer:
    def synthesize_findings(self, all_findings: List[dict]) -> List[dict]:
        """
        Combine findings from multiple sources, deduplicate, and merge
        """
        seen_companies = {}
        
        for finding in all_findings:
            # Normalize company name
            company_key = self.normalize_company_name(finding['company_name'])
            
            if company_key in seen_companies:
                # Merge with existing record
                existing = seen_companies[company_key]
                merged = self.merge_records(existing, finding)
                seen_companies[company_key] = merged
            else:
                # New company
                seen_companies[company_key] = finding
        
        return list(seen_companies.values())
    
    def merge_records(self, record1: dict, record2: dict) -> dict:
        """
        Merge two records about the same company from different sources
        
        Priority: SEC filings > Annual reports > Website > Press releases > News
        """
        source_priority = {
            'sec_13f': 5,
            'annual_report': 4,
            'portfolio_company_website': 4,
            'website': 3,
            'press_release': 2,
            'news': 1
        }
        
        # Use data from higher priority source
        if source_priority.get(record1['source_type'], 0) > source_priority.get(record2['source_type'], 0):
            primary = record1
            secondary = record2
        else:
            primary = record2
            secondary = record1
        
        # Merge: keep primary data, fill missing fields from secondary
        merged = {**primary}
        
        for field in ['investment_date', 'investment_amount_usd', 'company_industry']:
            if not merged.get(field) and secondary.get(field):
                merged[field] = secondary[field]
        
        # Combine source URLs for provenance
        merged['source_urls'] = [primary.get('source_url'), secondary.get('source_url')]
        merged['confidence_level'] = 'high' if len([s for s in merged['source_urls'] if s]) >= 2 else primary['confidence_level']
        
        return merged
```

---

## 🎯 IMPLEMENTATION PHASES

### PHASE 1 (QUICK WIN): SEC 13F Only - 2-3 days
**Goal:** Prove the concept, get immediate value

**Tasks:**
1. Create database tables (just `portfolio_companies` and `agentic_collection_jobs`)
2. Implement SEC 13F strategy only
3. Create basic API endpoint to trigger collection
4. Test with 10 large LPs (CalPERS, Texas Teachers, etc.)

**Success Criteria:**
- ✅ 40-60 large LPs with public equity holdings
- ✅ Agent reasoning logs are clear
- ✅ Data quality is high (validated against known portfolios)

---

### PHASE 2: Core Infrastructure - Week 1-2
**Goal:** Build foundation for all strategies

**Tasks:**
1. Complete all database tables
2. Implement `PortfolioResearchAgent` orchestrator
3. Add Strategies 1 & 2 (SEC 13F + Website scraping)
4. Implement `DataSynthesizer` for deduplication
5. Add all API endpoints
6. Test with 20 diverse investors

**Success Criteria:**
- ✅ 60-80 LPs with data from 2+ sources
- ✅ Deduplication works correctly
- ✅ Agent decision logic is sound

---

### PHASE 3: Advanced Strategies - Week 3
**Goal:** Add LLM-powered extraction

**Tasks:**
1. Add Strategy 3 (Annual report PDF parsing)
2. Add Strategy 4 (News search with LLM extraction)
3. Add Strategy 5 (Reverse search)
4. Implement LLM integration (OpenAI or Anthropic)
5. Add error handling and retries

**Success Criteria:**
- ✅ 80+ LPs with comprehensive data
- ✅ LLM extraction accuracy >80%
- ✅ Cost per investor <$0.15

---

### PHASE 4: Network Analysis - Week 4
**Goal:** Extract insights from collected data

**Tasks:**
1. Implement co-investor extraction
2. Build investment theme classifier
3. Add analytics endpoints
4. Create summary reports

**Success Criteria:**
- ✅ 50+ co-investor relationships identified
- ✅ Investment themes classified for 80%+ investors
- ✅ Network graph can be generated

---

## ✅ SUCCESS CRITERIA

### Minimum Viable (Phase 1):
- [ ] 40-60 LPs with portfolio data (SEC 13F only)
- [ ] Agent reasoning logs are clear and debuggable
- [ ] API endpoint works
- [ ] Cost <$20 for full run

### Full Success (Phase 4):
- [ ] 80-100 LPs (60-75%) with 5+ companies each
- [ ] 40-60 FOs (40-60%) with investment history
- [ ] Average 3+ sources per investor (validation)
- [ ] <5% duplicate companies (deduplication works)
- [ ] 50+ co-investor pairs identified
- [ ] Investment themes classified for 80%+ investors
- [ ] Agent completes in <5 minutes per investor
- [ ] Cost <$0.15 per investor

---

## 🛡️ SAFEGUARDS & COMPLIANCE

### Rate Limiting:
```python
class AgenticRateLimiter:
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
- ✅ Only collect publicly disclosed data
- ✅ Respect robots.txt
- ✅ Rate limiting on all domains
- ✅ Proper User-Agent identification
- ✅ No authentication bypass
- ✅ Family office extra sensitivity (slower rate limits)

### Data Quality:
- ✅ Confidence scoring (high/medium/low)
- ✅ Source attribution for all data
- ✅ Deduplication across sources
- ✅ Validation against known portfolios

---

## 📊 TESTING PLAN

### Test Datasets:
```python
# Test with these investors (diverse coverage):
TEST_LPS = [
    {'name': 'CalPERS', 'expected_sources': ['sec_13f', 'annual_report', 'website']},
    {'name': 'Yale Endowment', 'expected_sources': ['sec_13f', 'annual_report']},
    {'name': 'Ontario Teachers', 'expected_sources': ['website', 'annual_report']},
    # ... 7 more
]

TEST_FOS = [
    {'name': 'Cascade Investment', 'expected_sources': ['sec_13f', 'news']},
    {'name': 'Iconiq Capital', 'expected_sources': ['sec_13f', 'website']},
    # ... 3 more
]
```

### Validation Queries:
```sql
-- Check for duplicates
SELECT company_name, investor_id, COUNT(*) 
FROM portfolio_companies 
GROUP BY company_name, investor_id 
HAVING COUNT(*) > 1;

-- Coverage by source
SELECT source_type, COUNT(*), AVG(CASE WHEN confidence_level = 'high' THEN 1.0 ELSE 0 END) as high_conf_pct
FROM portfolio_companies
GROUP BY source_type;

-- Investors with data
SELECT investor_type, COUNT(DISTINCT investor_id) as investors_with_data
FROM portfolio_companies
GROUP BY investor_type;
```

---

## 📖 REFERENCE DOCUMENTATION

All detailed documentation is in the Nexdata repository:

- **Full Implementation Plan:** `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md` (18 KB)
- **Quick Summary:** `docs/AGENT_PROMPTS/AGENTIC_APPROACH_SUMMARY.md` (8 KB)
- **Project Rules:** `RULES.md` - Read for data collection guidelines
- **Similar Implementation:** Look at `app/sources/census/` for plugin pattern example

---

## 💰 COST ESTIMATES

### Per Investor:
- **HTTP Requests:** 20-50
- **LLM API Calls:** 3-5 (only for news extraction)
- **Tokens:** ~10,000 (at $0.01/1K tokens = $0.10)
- **Total Cost:** $0.05-0.15
- **Time:** 3-5 minutes (automated)

### For 100 Investors:
- **Total Cost:** $5-15
- **Time:** 5-8 hours (fully automated)

### For Full Dataset (131 LPs + 100 FOs):
- **Total Cost:** ~$30-40
- **Time:** ~12-15 hours (one-time)
- **Quarterly Updates:** ~$10-15 (only new/changed data)

---

## 🚀 GETTING STARTED

1. **Read the full plan:** `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md`
2. **Create database tables** (run migrations)
3. **Install dependencies:** `pip install openai newspaper3k yfinance`
4. **Add API keys to `.env`:** `OPENAI_API_KEY=sk-...`
5. **Start with Phase 1:** Implement SEC 13F strategy only (quick win)
6. **Test with 10 investors:** Validate approach before scaling
7. **Iterate:** Add more strategies based on results

---

## ❓ QUESTIONS TO RESOLVE

If anything is unclear:
1. Check detailed docs: `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md`
2. Look at existing plugin structure: `app/sources/census/`
3. Review project rules: `RULES.md`
4. Ask the original agent/user for clarification

---

## 📈 EXPECTED TIMELINE

- **Phase 1 (SEC 13F Quick Win):** 2-3 days
- **Phase 2 (Core Infrastructure):** 5-7 days
- **Phase 3 (Advanced Strategies):** 7-10 days
- **Phase 4 (Network Analysis):** 3-5 days
- **Testing & Refinement:** 5-7 days

**Total:** 4-6 weeks for full implementation

**Quick Win Option:** Do Phase 1 only (SEC 13F) for immediate value in 2-3 days!

---

## 🎉 FINAL NOTES

**This is a high-value, differentiating feature.** No competitor has this data aggregated because it requires:
- Multi-source reasoning
- Adaptive strategy selection
- LLM-powered extraction
- Cross-validation across sources

You're building something unique! 🚀

**Good luck!** 💪
