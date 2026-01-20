# Agentic LP/FO Portfolio & Deal Flow Research

## Mission
Build an **agentic system** to automatically discover and track investments, portfolio companies, and deal flow for Limited Partners (LPs) and Family Offices (FOs) in the database.

**Why Agentic?**
- No single API contains this data
- Data scattered across 5-10 sources per LP/FO
- Requires multi-step reasoning and adaptive navigation
- Sources have different formats (HTML, PDF, press releases, SEC filings)
- Agent must evaluate data quality and synthesize findings

---

## Objective

### Primary Goals:
1. **Portfolio Discovery:** Find current holdings and past investments for each LP/FO
2. **Deal Flow Tracking:** Identify recent investments and co-investors
3. **Investment Pattern Analysis:** Understand themes, sectors, check sizes
4. **Network Mapping:** Build co-investor networks (who invests together)

### Success Metrics:
- **LP Coverage:** 80-100 LPs (60-75%) with at least 5 portfolio companies identified
- **FO Coverage:** 40-60 FOs (40-60%) with investment history
- **Data Freshness:** Quarterly updates for active investors
- **Source Diversity:** Average 3+ sources per LP/FO (validation)

---

## Database Schema

### 1. Core Portfolio Table
```sql
CREATE TABLE portfolio_companies (
    id SERIAL PRIMARY KEY,
    investor_id INT NOT NULL, -- references lp_fund.id or family_offices.id
    investor_type TEXT NOT NULL CHECK (investor_type IN ('lp', 'family_office')),
    
    -- Company Details
    company_name TEXT NOT NULL,
    company_website TEXT,
    company_industry TEXT,
    company_stage TEXT, -- seed, early, growth, public, etc.
    company_location TEXT, -- city, state, country
    
    -- Investment Details
    investment_type TEXT, -- equity, PE, VC, real_estate, infrastructure, etc.
    investment_date DATE,
    investment_amount_usd NUMERIC,
    ownership_percentage NUMERIC,
    current_holding BOOLEAN DEFAULT TRUE,
    exit_date DATE,
    exit_type TEXT, -- IPO, acquisition, secondary, liquidation
    
    -- Data Provenance
    source_type TEXT NOT NULL, -- sec_13f, website, press_release, annual_report, news
    source_url TEXT,
    confidence_level TEXT CHECK (confidence_level IN ('high', 'medium', 'low')),
    collected_date DATE DEFAULT CURRENT_DATE,
    last_verified_date DATE,
    
    -- Agent Metadata
    collection_method TEXT, -- agentic_search, manual, api
    agent_reasoning TEXT, -- why agent believes this is accurate
    
    UNIQUE(investor_id, investor_type, company_name, investment_date)
);

CREATE INDEX idx_portfolio_investor ON portfolio_companies(investor_id, investor_type);
CREATE INDEX idx_portfolio_company ON portfolio_companies(company_name);
CREATE INDEX idx_portfolio_current ON portfolio_companies(current_holding);
```

### 2. Co-Investor Network Table
```sql
CREATE TABLE co_investments (
    id SERIAL PRIMARY KEY,
    primary_investor_id INT NOT NULL,
    primary_investor_type TEXT NOT NULL,
    co_investor_name TEXT NOT NULL,
    co_investor_type TEXT, -- lp, family_office, vc_firm, pe_firm, corporate
    
    -- Deal Details
    deal_name TEXT, -- company or fund invested in
    deal_date DATE,
    deal_size_usd NUMERIC,
    
    -- Relationship Strength
    co_investment_count INT DEFAULT 1, -- how many times invested together
    
    -- Data Provenance
    source_type TEXT,
    source_url TEXT,
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(primary_investor_id, primary_investor_type, co_investor_name, deal_name)
);

CREATE INDEX idx_coinvest_primary ON co_investments(primary_investor_id, primary_investor_type);
```

### 3. Investment Themes Table
```sql
CREATE TABLE investor_themes (
    id SERIAL PRIMARY KEY,
    investor_id INT NOT NULL,
    investor_type TEXT NOT NULL,
    
    -- Theme Classification
    theme_category TEXT, -- sector, geography, stage, asset_class
    theme_value TEXT, -- e.g., "climate_tech", "healthcare", "emerging_markets"
    
    -- Evidence
    investment_count INT, -- how many portfolio companies match this theme
    percentage_of_portfolio NUMERIC, -- % of total investments
    
    -- Confidence
    confidence_level TEXT,
    evidence_sources TEXT[], -- URLs that support this theme
    
    collected_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(investor_id, investor_type, theme_category, theme_value)
);
```

### 4. Agent Collection Jobs Table
```sql
CREATE TABLE agentic_collection_jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT, -- portfolio_discovery, deal_flow_update, co_investor_mapping
    
    target_investor_id INT,
    target_investor_type TEXT,
    target_investor_name TEXT,
    
    -- Job Status
    status TEXT CHECK (status IN ('pending', 'running', 'success', 'partial_success', 'failed')),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Results
    sources_checked INT, -- how many sources agent tried
    sources_successful INT, -- how many returned data
    companies_found INT,
    new_companies INT, -- not previously in database
    updated_companies INT,
    
    -- Agent Decision Trail
    strategy_used TEXT[], -- ordered list of strategies agent tried
    reasoning_log JSONB, -- detailed agent thought process
    
    -- Errors/Warnings
    errors JSONB,
    warnings TEXT[],
    
    -- Resource Usage
    requests_made INT,
    tokens_used INT, -- if using LLM
    cost_usd NUMERIC
);
```

---

## Agentic Workflow

### Phase 1: Strategy Planning
**Agent evaluates what information is available and plans collection strategy**

```python
class PortfolioResearchAgent:
    def plan_strategy(self, investor_id, investor_type, investor_data):
        """
        Agent analyzes available info and creates collection plan
        
        Input: investor_data = {
            'name': 'CalPERS',
            'type': 'public_pension',
            'website_url': 'https://calpers.ca.gov',
            'sec_crd_number': None,  # not SEC registered
            'aum_usd': 450_000_000_000
        }
        
        Output: strategy = [
            ('check_sec_13f', priority=10, reason='Large AUM suggests equity holdings'),
            ('parse_annual_report', priority=9, reason='Public pension publishes CAFR'),
            ('scrape_website_portfolio', priority=8, reason='Website may list investments'),
            ('news_search', priority=7, reason='Recent deals often in press'),
            ('search_portfolio_companies', priority=6, reason='Companies may list investors')
        ]
        """
        
        strategies = []
        
        # Evaluate: Does this investor file SEC 13F?
        if investor_data['aum_usd'] > 100_000_000:  # $100M threshold
            strategies.append({
                'method': 'check_sec_13f',
                'priority': 10,
                'confidence': 0.9,
                'reasoning': 'AUM > $100M suggests 13F filing requirement'
            })
        
        # Evaluate: Does investor have public website?
        if investor_data['website_url']:
            strategies.append({
                'method': 'scrape_website_portfolio',
                'priority': 9,
                'confidence': 0.7,
                'reasoning': 'Official website likely has portfolio section'
            })
        
        # Evaluate: Type-specific strategies
        if investor_data['type'] == 'public_pension':
            strategies.append({
                'method': 'parse_annual_report',
                'priority': 10,
                'confidence': 0.95,
                'reasoning': 'Public pensions required to publish detailed CAFRs'
            })
        
        if investor_data['type'] == 'family_office':
            strategies.append({
                'method': 'news_search',
                'priority': 8,
                'confidence': 0.5,
                'reasoning': 'Family offices rarely disclose; press releases are main source'
            })
        
        # Always try portfolio company back-references
        strategies.append({
            'method': 'search_portfolio_companies',
            'priority': 6,
            'confidence': 0.6,
            'reasoning': 'Many companies list their investors on About pages'
        })
        
        return sorted(strategies, key=lambda x: x['priority'], reverse=True)
```

### Phase 2: Multi-Source Collection

#### Strategy 1: SEC 13F Filings (Public Equity Holdings)
```python
async def collect_from_sec_13f(investor_name: str) -> List[PortfolioCompany]:
    """
    Agent workflow:
    1. Search SEC EDGAR for 13F filers matching investor name
    2. Download most recent 13F XML filing
    3. Parse holdings table
    4. For each holding: extract ticker, shares, value
    5. Resolve ticker to company name via API (e.g., Yahoo Finance)
    6. Store with source='sec_13f', confidence='high'
    """
    
    agent_log = {
        'step': 'sec_13f_search',
        'reasoning': f'Searching SEC EDGAR for 13F filer: {investor_name}'
    }
    
    # Step 1: Search for filer
    search_url = f"https://www.sec.gov/cgi-bin/browse-edgar"
    params = {'action': 'getcompany', 'company': investor_name, 'type': '13F', 'count': 1}
    
    # Step 2: If found, get latest filing
    # Step 3: Parse XML for holdings
    # Step 4: Return structured data
    
    return holdings, agent_log
```

**Expected Coverage:** 40-60 large LPs/FOs with >$100M equity

---

#### Strategy 2: Website Portfolio Scraping
```python
async def collect_from_website(investor_data: dict) -> List[PortfolioCompany]:
    """
    Agent workflow:
    1. Navigate to investor website
    2. Search for links: "Portfolio", "Investments", "Companies", "Our Portfolio"
    3. If found → scrape portfolio page(s)
    4. If not found → check annual report PDF for portfolio list
    5. Extract: company names, industries, dates (if available)
    6. Store with source='website', confidence='medium-high'
    """
    
    website_url = investor_data['website_url']
    agent_log = []
    
    # Step 1: Fetch homepage
    agent_log.append({
        'step': 'fetch_homepage',
        'url': website_url,
        'reasoning': 'Looking for navigation links to portfolio section'
    })
    
    html = await fetch_page(website_url)
    soup = BeautifulSoup(html, 'lxml')
    
    # Step 2: Find portfolio links
    portfolio_keywords = ['portfolio', 'investments', 'companies', 'our investments', 'holdings']
    potential_links = []
    
    for link in soup.find_all('a', href=True):
        link_text = link.get_text().lower()
        link_href = link['href']
        
        for keyword in portfolio_keywords:
            if keyword in link_text or keyword in link_href:
                potential_links.append({
                    'url': urljoin(website_url, link_href),
                    'text': link_text,
                    'confidence': 0.8
                })
    
    agent_log.append({
        'step': 'found_portfolio_links',
        'count': len(potential_links),
        'links': potential_links[:5]  # top 5
    })
    
    # Step 3: Scrape each potential portfolio page
    companies = []
    for link in potential_links[:3]:  # max 3 pages
        page_html = await fetch_page(link['url'])
        companies.extend(extract_companies_from_page(page_html, investor_data))
    
    return companies, agent_log
```

**Expected Coverage:** 60-80 LPs with public portfolios

---

#### Strategy 3: Annual Report PDF Parsing
```python
async def collect_from_annual_reports(investor_data: dict) -> List[PortfolioCompany]:
    """
    Agent workflow:
    1. Search investor website for "Annual Report", "CAFR", "Investment Report"
    2. Download most recent PDF
    3. Extract text with pdfplumber
    4. Search for section headers: "Portfolio", "Investment Holdings", "Top Holdings"
    5. Parse tables or lists in that section
    6. Extract company names, values, descriptions
    7. Store with source='annual_report', confidence='high'
    """
    
    agent_log = []
    
    # Step 1: Find annual report link
    search_terms = ['annual report', 'cafr', 'comprehensive annual financial report', 
                    'investment report', 'acfr']
    
    agent_log.append({
        'step': 'searching_for_reports',
        'reasoning': f'Looking for annual reports on {investor_data["website_url"]}'
    })
    
    report_urls = await search_for_documents(
        base_url=investor_data['website_url'],
        search_terms=search_terms,
        file_types=['.pdf']
    )
    
    if not report_urls:
        agent_log.append({
            'step': 'no_reports_found',
            'action': 'skipping_strategy'
        })
        return [], agent_log
    
    # Step 2: Download and parse most recent report
    latest_report = report_urls[0]
    agent_log.append({
        'step': 'downloading_report',
        'url': latest_report,
        'reasoning': 'Most recent report likely has current portfolio'
    })
    
    pdf_text = await extract_pdf_text(latest_report)
    
    # Step 3: Find portfolio section
    portfolio_section = extract_section(pdf_text, 
        start_markers=['portfolio', 'investment holdings', 'schedule of investments'],
        end_markers=['notes to', 'footnotes', 'independent auditor']
    )
    
    # Step 4: Parse companies
    companies = parse_portfolio_section(portfolio_section, investor_data)
    
    agent_log.append({
        'step': 'parsed_portfolio',
        'companies_found': len(companies)
    })
    
    return companies, agent_log
```

**Expected Coverage:** 50-70 public pensions, endowments with published CAFRs

---

#### Strategy 4: Press Release & News Search
```python
async def collect_from_news(investor_name: str, investor_data: dict) -> List[PortfolioCompany]:
    """
    Agent workflow:
    1. Search Google News: "{investor_name} investment" OR "{investor_name} invests in"
    2. Filter results from last 2 years
    3. For each news article:
       - Extract company name being invested in
       - Extract deal size (if mentioned)
       - Extract co-investors (if mentioned)
    4. Validate: check if company exists, reasonable deal size
    5. Store with source='press_release' or 'news', confidence='medium'
    """
    
    agent_log = []
    
    # Step 1: Construct search queries
    search_queries = [
        f'"{investor_name}" investment',
        f'"{investor_name}" invests in',
        f'"{investor_name}" leads funding round',
        f'"{investor_name}" portfolio company'
    ]
    
    agent_log.append({
        'step': 'news_search',
        'queries': search_queries,
        'reasoning': 'Recent investments often announced in press'
    })
    
    all_articles = []
    for query in search_queries:
        articles = await search_news(
            query=query,
            date_range='2y',  # last 2 years
            max_results=20
        )
        all_articles.extend(articles)
    
    # Step 2: Extract investments from articles
    investments = []
    for article in all_articles[:30]:  # process top 30
        article_text = await fetch_article_text(article['url'])
        
        # Agent uses LLM to extract structured data
        extracted = await llm_extract_investment_info(
            text=article_text,
            investor_name=investor_name,
            prompt="""
            Extract investment information from this news article:
            - What company did {investor_name} invest in?
            - When was the investment? (date)
            - How much did they invest? (amount in USD)
            - Who else invested? (co-investors)
            - What is the company's industry?
            
            Return JSON or null if no investment mentioned.
            """
        )
        
        if extracted:
            investments.append({
                'company_name': extracted['company_name'],
                'investment_date': extracted['date'],
                'investment_amount_usd': extracted['amount'],
                'source_url': article['url'],
                'source_type': 'news',
                'confidence_level': 'medium',
                'agent_reasoning': f'Extracted from news article dated {article["date"]}'
            })
    
    agent_log.append({
        'step': 'extracted_investments',
        'count': len(investments)
    })
    
    return investments, agent_log
```

**Expected Coverage:** 30-50 FOs, 20-30 active LPs

---

#### Strategy 5: Portfolio Company Back-References
```python
async def collect_from_portfolio_companies(investor_name: str) -> List[PortfolioCompany]:
    """
    Agent workflow (REVERSE SEARCH):
    1. Search Google: "{investor_name}" + "investor" OR "backed by" OR "portfolio"
    2. Find company websites that mention the investor
    3. Navigate to company's "About" or "Investors" page
    4. Confirm investor relationship
    5. Extract any additional details (investment date, amount)
    6. Store with source='portfolio_company_website', confidence='high'
    """
    
    agent_log = []
    
    # Step 1: Search for companies that mention this investor
    search_query = f'"{investor_name}" (investor OR "backed by" OR portfolio) -site:linkedin.com'
    
    agent_log.append({
        'step': 'reverse_search',
        'query': search_query,
        'reasoning': 'Portfolio companies often list their investors'
    })
    
    search_results = await google_search(search_query, max_results=50)
    
    # Step 2: Filter for company websites (not news articles)
    company_sites = []
    for result in search_results:
        # Skip news sites, SEC.gov, etc.
        if any(domain in result['url'] for domain in ['news.', '.news', 'sec.gov', 'edgar', 'wikipedia']):
            continue
        
        company_sites.append(result)
    
    # Step 3: Verify investor relationship on each site
    confirmed_companies = []
    for site in company_sites[:20]:  # top 20
        html = await fetch_page(site['url'])
        
        # Look for investor name on the page
        if investor_name.lower() in html.lower():
            # Try to extract more context
            company_info = await extract_company_info(
                url=site['url'],
                html=html,
                investor_name=investor_name
            )
            
            if company_info:
                confirmed_companies.append(company_info)
    
    agent_log.append({
        'step': 'confirmed_relationships',
        'count': len(confirmed_companies)
    })
    
    return confirmed_companies, agent_log
```

**Expected Coverage:** 20-40 companies found per large investor

---

### Phase 3: Data Synthesis & Validation

```python
class DataSynthesizer:
    async def synthesize_findings(self, investor_id, investor_type, all_findings):
        """
        Agent combines data from multiple sources and resolves conflicts
        
        Example:
        - 13F says: "Apple Inc, $5M, 2024-12-31"
        - Website says: "Apple (Technology), 2024"
        - News says: "Invested in AAPL Q4 2024"
        
        Agent determines: These are the same investment, merge them
        """
        
        deduplicated = []
        seen_companies = {}
        
        for finding in all_findings:
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
    
    def merge_records(self, record1, record2):
        """
        Agent decides which data is more reliable
        
        Priority order:
        1. SEC filings (highest confidence)
        2. Official annual reports
        3. Company's own investor page
        4. Press releases
        5. News articles (lowest confidence)
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
        merged['source_urls'] = [primary['source_url'], secondary['source_url']]
        merged['confidence_level'] = 'high' if len(merged['source_urls']) >= 2 else primary['confidence_level']
        
        return merged
```

---

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)
**Files to Create:**
- `app/agentic/` - New directory for agentic collection
- `app/agentic/portfolio_agent.py` - Main agent orchestrator
- `app/agentic/strategies/` - Collection strategy modules
  - `sec_13f_strategy.py`
  - `website_strategy.py`
  - `annual_report_strategy.py`
  - `news_strategy.py`
  - `reverse_search_strategy.py`
- `app/agentic/synthesizer.py` - Data deduplication and merging
- `app/agentic/validators.py` - Data quality checks

**Database:**
```sql
-- Run migrations to create tables
-- See "Database Schema" section above
```

**API Endpoints:**
```python
# app/api/v1/agentic_research.py

@router.post("/agentic/portfolio/collect")
async def trigger_portfolio_collection(
    investor_id: int,
    investor_type: str,
    strategies: Optional[List[str]] = None,  # if None, agent decides
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger agentic portfolio collection for a single investor
    
    Strategies (optional):
    - "sec_13f"
    - "website"
    - "annual_report"
    - "news"
    - "reverse_search"
    - "all" (default)
    """
    pass

@router.post("/agentic/portfolio/batch")
async def batch_portfolio_collection(
    investor_type: str,  # "lp" or "family_office"
    limit: int = 10,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger batch collection for multiple investors
    Processes {limit} investors, prioritizes those with missing data
    """
    pass

@router.get("/agentic/portfolio/{investor_id}/summary")
async def get_portfolio_summary(
    investor_id: int,
    investor_type: str,
    db: Session = Depends(get_db)
):
    """
    Get portfolio summary for an investor:
    - Total companies found
    - Source breakdown
    - Investment themes
    - Co-investors
    - Data completeness score
    """
    pass

@router.get("/agentic/jobs/{job_id}")
async def get_job_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed agent job status including reasoning log
    """
    pass
```

---

### Phase 2: Basic Strategies (Week 2)
**Implement:**
1. SEC 13F strategy (easiest, structured data)
2. Website portfolio scraping (medium difficulty)
3. Data synthesis and deduplication

**Test with:**
- 10 large public pensions (e.g., CalPERS, CalSTRS, Texas Teachers)
- 5 large family offices with known portfolios

**Success Criteria:**
- 80%+ find at least 5 portfolio companies
- Agent reasoning logs are clear and debuggable
- No duplicate companies in database

---

### Phase 3: Advanced Strategies (Week 3)
**Implement:**
1. Annual report PDF parsing
2. News/press release search with LLM extraction
3. Portfolio company reverse search

**Add LLM Integration:**
```python
# Use OpenAI/Anthropic for entity extraction
async def llm_extract_investment_info(text: str, investor_name: str):
    """
    Use LLM to extract structured investment data from unstructured text
    """
    
    prompt = f"""
    Extract investment information from the following text about {investor_name}.
    
    Text: {text[:2000]}  # first 2000 chars
    
    Return JSON with these fields (or null if no investment found):
    {{
        "company_name": "string",
        "investment_date": "YYYY-MM-DD or null",
        "investment_amount_usd": number or null,
        "co_investors": ["list", "of", "names"],
        "company_industry": "string or null",
        "confidence": 0.0-1.0
    }}
    """
    
    # Call LLM API
    response = await openai_client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    
    return json.loads(response.choices[0].message.content)
```

**Add Config:**
```python
# app/core/config.py

class Settings(BaseSettings):
    # ... existing settings ...
    
    # Agentic Collection Settings
    openai_api_key: Optional[str] = None  # for LLM extraction
    anthropic_api_key: Optional[str] = None  # alternative
    
    agentic_max_strategies_per_job: int = 5
    agentic_max_requests_per_strategy: int = 20
    agentic_timeout_per_strategy: int = 300  # 5 minutes
    
    # Rate Limiting (per domain)
    agentic_requests_per_second: float = 0.5  # 1 request per 2 seconds
    agentic_max_concurrent: int = 3
```

---

### Phase 4: Co-Investor Network Analysis (Week 4)
**Implement:**
- Extract co-investors from news articles
- Build network graph of who invests together
- Identify "frequent co-investors" (warm introductions)

**Queries:**
```sql
-- Find LPs that frequently co-invest
SELECT 
    lp1.name AS lp1_name,
    lp2.name AS lp2_name,
    COUNT(*) AS co_investments,
    ARRAY_AGG(c.deal_name) AS deals
FROM co_investments c1
JOIN co_investments c2 ON c1.deal_name = c2.deal_name AND c1.id < c2.id
JOIN lp_fund lp1 ON c1.primary_investor_id = lp1.id
JOIN lp_fund lp2 ON c2.primary_investor_id = lp2.id
WHERE c1.primary_investor_type = 'lp' AND c2.primary_investor_type = 'lp'
GROUP BY lp1.name, lp2.name
HAVING COUNT(*) >= 3  -- at least 3 co-investments
ORDER BY COUNT(*) DESC;
```

---

## Agentic Decision Framework

### When to Continue vs Stop

```python
class AgentDecisionMaker:
    def should_continue(self, current_results, strategies_tried, time_elapsed):
        """
        Agent decides whether to try more strategies or stop
        
        Stop if:
        - Found 10+ companies AND tried 3+ strategies (good coverage)
        - Tried all strategies with no results (dead end)
        - Time elapsed > 10 minutes (resource limit)
        - Confidence of findings is uniformly high (no need for more validation)
        
        Continue if:
        - Found 0-5 companies (poor coverage)
        - Single source only (need validation)
        - Large investor with low coverage (expected more results)
        """
        
        # Good coverage achieved
        if len(current_results) >= 10 and len(strategies_tried) >= 3:
            return False, "Sufficient coverage achieved"
        
        # No results from all strategies
        if len(strategies_tried) >= 5 and len(current_results) == 0:
            return False, "No data found after trying all strategies"
        
        # Time limit
        if time_elapsed > 600:  # 10 minutes
            return False, "Time limit reached"
        
        # Poor coverage, keep trying
        if len(current_results) < 5:
            return True, "Coverage below threshold, continuing search"
        
        # Need validation (single source)
        unique_sources = set(r['source_type'] for r in current_results)
        if len(unique_sources) == 1:
            return True, "Single source detected, seeking validation"
        
        return False, "Default stop condition"
```

---

## Challenges & Solutions

### Challenge 1: Company Name Disambiguation
**Problem:** "Apple" could be Apple Inc, Apple Hospitality REIT, Apple Bank, etc.

**Solution:**
```python
def normalize_company_name(raw_name: str, context: dict) -> str:
    """
    Use multiple signals to disambiguate:
    1. Industry context from article
    2. Stock ticker if mentioned
    3. Location if mentioned
    4. Cross-reference with known companies database
    """
    
    # Check if ticker mentioned
    ticker_match = re.search(r'\b([A-Z]{1,5})\b', context.get('text', ''))
    if ticker_match:
        ticker = ticker_match.group(1)
        company = lookup_ticker(ticker)  # Use financial data API
        if company:
            return company['official_name']
    
    # Check industry context
    if context.get('industry') == 'technology' and 'apple' in raw_name.lower():
        return 'Apple Inc.'
    
    # Default: return cleaned raw name
    return raw_name.strip().title()
```

### Challenge 2: Stale Data
**Problem:** Portfolio from 2020 annual report may be outdated

**Solution:**
- Tag each record with `collected_date` and `source_date`
- Prioritize recent sources
- Flag records >2 years old as `potentially_stale=TRUE`
- Implement quarterly refresh jobs

### Challenge 3: Private vs Public Equity
**Problem:** 13F only shows public equity, but LPs invest in PE/VC/Real Estate

**Solution:**
- Combine multiple strategies (13F for public + news for private)
- Tag each holding with `investment_type`
- Annual reports often list PE/VC allocations

### Challenge 4: Rate Limiting & Resource Usage
**Problem:** Agentic collection could make 100s of requests

**Solution:**
```python
# Rate limiter per domain
class AgenticRateLimiter:
    def __init__(self):
        self.domain_limits = {}  # domain -> (semaphore, last_request_time)
        self.default_rps = 0.5  # 1 request per 2 seconds
    
    async def acquire(self, url: str):
        domain = urlparse(url).netloc
        
        if domain not in self.domain_limits:
            self.domain_limits[domain] = {
                'semaphore': asyncio.Semaphore(1),  # 1 concurrent request per domain
                'last_request': 0
            }
        
        limiter = self.domain_limits[domain]
        
        async with limiter['semaphore']:
            # Wait if needed
            elapsed = time.time() - limiter['last_request']
            wait_time = (1.0 / self.default_rps) - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            limiter['last_request'] = time.time()
            yield
```

---

## Success Metrics & KPIs

### Data Coverage:
- **Target:** 80+ LPs (60%+) with 5+ portfolio companies
- **Target:** 50+ FOs (50%+) with 3+ investments identified
- **Target:** Average 3+ sources per investor (validation)

### Data Quality:
- **Deduplication Rate:** <5% duplicate companies
- **Confidence Distribution:** >50% high confidence, <20% low confidence
- **Source Diversity:** >60% of investors have data from 2+ sources

### Agent Performance:
- **Success Rate:** >75% of jobs find at least 1 company
- **Average Time:** <5 minutes per investor
- **Resource Efficiency:** <$0.10 per investor (LLM costs)

### Business Value:
- **Network Insights:** Identify 20+ high-frequency co-investor pairs
- **Investment Themes:** Classify 80%+ of investors by primary theme
- **Actionable Contacts:** Link 30%+ of portfolio companies to contacts

---

## Ethical Considerations & Safeguards

### Respect for Privacy:
- **Family Offices:** Extra caution; skip if any "confidential" language detected
- **Private Companies:** Only collect if investor relationship is publicly disclosed
- **Personal Information:** No personal addresses, personal emails, or non-business data

### Compliance:
- **Rate Limiting:** Never exceed 2 requests/second per domain
- **robots.txt:** Respect all directives
- **Terms of Service:** No scraping of sites that explicitly prohibit it (e.g., LinkedIn)
- **Attribution:** Store source URLs for all data

### Data Usage:
- Internal research and analysis only
- Do not republish portfolio data without verification
- Provide opt-out mechanism if investors request removal

---

## Dependencies

```txt
# Add to requirements.txt

# Existing
httpx>=0.24.0
beautifulsoup4>=4.12.0
lxml>=4.9.0
pdfplumber>=0.10.0
robotexclusionrulesparser>=1.7.1

# New for Agentic
openai>=1.0.0                    # LLM for entity extraction
anthropic>=0.5.0                 # Alternative LLM
langchain>=0.1.0                 # Optional: for complex agentic workflows
googlesearch-python>=1.2.0       # Google search (use with caution)
newspaper3k>=0.2.8               # News article extraction
yfinance>=0.2.0                  # Ticker/company lookup
```

---

## Next Steps

1. ✅ Review this plan and provide feedback
2. Create database migrations for new tables
3. Implement Phase 1 (core infrastructure)
4. Test with 10 sample LPs
5. Iterate on agent decision logic
6. Expand to full dataset

---

## Estimated Timeline

- **Phase 1 (Infrastructure):** 5-7 days
- **Phase 2 (Basic Strategies):** 5-7 days
- **Phase 3 (Advanced + LLM):** 7-10 days
- **Phase 4 (Network Analysis):** 3-5 days
- **Testing & Refinement:** 5-7 days

**Total:** 4-6 weeks for full implementation

**Quick Win:** SEC 13F strategy alone can be done in 2-3 days and would cover 40-60 large investors immediately.
