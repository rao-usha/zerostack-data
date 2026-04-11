# Agent Task Prompts - Overview

This directory contains comprehensive implementation prompts for different agent tasks. Each prompt is designed to be handed off to a separate agent for parallel implementation.

## Available Prompts

### 1. 🤖 Agentic Portfolio Research (`agentic_portfolio_research_prompt.md`) **NEW**
**Objective:** Agentic system to discover LP/FO portfolio companies and investment patterns

**Current State:** 0 portfolio companies tracked
**Target Coverage:** 80-100 LPs, 40-60 FOs with investment history

**Why Agentic?**
- No single API has this data
- Requires multi-step reasoning across 5+ sources
- Agent adapts strategy based on investor type
- Synthesizes findings from varied formats (API, HTML, PDF, news)

**Data Sources:**
- SEC 13F filings (API) - 40-60 large investors
- Website portfolio scraping (HTML) - 60-80 investors
- Annual report parsing (PDF) - 50-70 public pensions
- News/press releases (LLM extraction) - 30-50 active investors
- Portfolio company back-references (reverse search) - 20-40 per investor

**Priority:** VERY HIGH (unique competitive advantage)
**Effort:** 4-6 weeks (Quick win: SEC 13F in 2-3 days)
**Impact:** 
- Understand investment patterns
- Identify warm introductions via co-investors
- Track deal flow
- Network mapping

**Quick Summary:** `AGENTIC_APPROACH_SUMMARY.md`

---

### 2. 🤖 Private Company Intelligence (`HANDOFF_private_company_intelligence.md`) **NEW**
**Objective:** Agentic system to build comprehensive private company profiles

**Current State:** No private company data (only public data exists)
**Target Coverage:** 500-1,000 companies (starting with LP/FO portfolios)

**Why Agentic?**
- No single API has comprehensive private company data
- Requires synthesis from 6+ sources per company
- Each company has different disclosure patterns
- Must validate and cross-reference across sources

**Data Sources:**
- Company websites (HTML) - About, Team, Press pages - 60-80% coverage
- Crunchbase/PitchBook (API) - Funding data - 30-50% coverage
- SEC Form D (API) - Private raises >$1M - 20-30% coverage
- News articles (LLM extraction) - Revenue estimates, milestones - 40-60% coverage
- Job postings (scraping) - Hiring signals, tech stack - 50-70% coverage
- Social signals (AngelList, ProductHunt) - Startup data - 20-40% coverage

**Priority:** HIGH (complements LP/FO portfolio data)
**Effort:** 4-5 weeks (Quick win: Website + SEC in 1 week)
**Impact:**
- M&A targeting and due diligence
- Competitive intelligence
- Portfolio company monitoring
- Links to LP/FO investments

**Database Tables:**
- `private_companies` - Core profiles (80-90% completeness target)
- `company_leadership` - Executives (average 3-5 per company)
- `company_funding_rounds` - VC/PE history
- `company_metrics` - Revenue/employee estimates over time

**Integration:** Auto-enriches companies found in LP/FO portfolios

**Cost:** $0.10-0.20 per company ($50-200 for 500-1,000 companies)

---

### 3. 🤖 Foot Traffic & Location Intelligence (`HANDOFF_foot_traffic_intelligence.md`) **NEW**
**Objective:** Track physical location activity for retail/restaurant investments

**Current State:** No foot traffic data
**Target Coverage:** 500-2,000 locations (portfolio companies' stores)

**Why Agentic?**
- Foot traffic data scattered across 5+ sources
- No single affordable comprehensive API
- Requires validation across sources
- POI discovery and enrichment needed

**Data Sources:**
- Google Popular Times (scraping) - Hourly patterns - FREE, 60-80% coverage
- SafeGraph API (paid $100-500/mo) - Weekly visits, demographics - 80-90% coverage
- Placer.ai (paid $500-2K+/mo) - Retail analytics - Major chains
- Foursquare (freemium) - POI metadata, check-ins
- City open data (free) - Pedestrian counters - ~20-30 cities

**Priority:** MEDIUM-HIGH (valuable for retail/hospitality portfolio monitoring)
**Effort:** 3-4 weeks
**Impact:**
- Track foot traffic at portfolio companies' locations
- Retail investment due diligence
- Real estate property valuation
- Competitive benchmarking
- Early warning system (declining traffic)

**Use Cases:**
- Monitor Chipotle locations foot traffic vs Panera
- Evaluate mall property value by foot traffic
- Detect declining traffic before revenue drops
- Compare regional performance

**Cost:** $0.05-0.20 per location/month ($25-400/mo for 500-2,000 locations)

---

### 4. 🤖 Management & Strategy Intelligence (`HANDOFF_management_strategy_intelligence.md`) **NEW**
**Objective:** Profile management quality and track strategic direction

**Current State:** Have company data, no management/strategy intelligence
**Target Coverage:** 500-1,000 companies (portfolio companies + targets)

**Why Agentic?**
- Management signals scattered across press, SEC, Glassdoor, news
- Strategic intent requires LLM extraction from unstructured text
- No single source for comprehensive management assessment
- Synthesis needed across multiple perspectives

**Data Sources:**
- Company press releases/blogs - Strategic announcements - 70-90% coverage
- SEC filings (MD&A) - Management discussion - Public companies only
- Earnings call transcripts - Strategic commentary - Public companies
- Glassdoor/Indeed (scraping) - Employee sentiment - 60-80% coverage
- News search (LLM) - Executive backgrounds, strategic shifts
- Trade publications - Industry analysis

**Priority:** HIGH (management quality drives investment outcomes)
**Effort:** 4 weeks
**Impact:**
- "Bet on the jockey, not the horse" - evaluate management before investing
- Track strategic shifts at portfolio companies
- Detect management red flags (departures, declining sentiment)
- Profile executive teams (backgrounds, tenure, track record)
- Understand competitive strategies

**Strategic Signals Tracked:**
- Product launches and roadmap
- Geographic/market expansion
- Partnerships and M&A
- Business model pivots
- Leadership changes
- Employee sentiment trends
- Competitive positioning

**Cost:** $0.15-0.30 per company ($75-300 for 500-1,000 companies)

---

### 5. 🤖 Prediction Market Intelligence (`HANDOFF_prediction_market_intelligence.md`) **NEW**
**Objective:** Monitor betting markets for risk assessment and scenario planning

**Current State:** No prediction market data
**Target Coverage:** 50-100 markets across 3 platforms (Kalshi, PredictIt, Polymarket)

**Why Agentic?**
- Browser-based monitoring (no API keys needed)
- Extracts structured data from market pages
- Tracks probability changes over time
- Alerts on significant shifts

**Platforms:**
- **Kalshi** (kalshi.com) - CFTC-regulated economic events
  - Fed rate decisions, CPI, unemployment, GDP
  - Real money markets, high quality signals
- **PredictIt** (predictit.org) - US political events
  - Elections, legislation, appointments
  - Political risk assessment
- **Polymarket** (polymarket.com) - Global, crypto-based
  - Business events (acquisitions, earnings)
  - Crypto markets, international politics

**Priority:** HIGH (unique data, high value for risk assessment)
**Effort:** 4 weeks
**Impact:**
- Quantify probability of adverse events
- Scenario planning with real probabilities
- Political risk assessment (election impacts)
- Economic forecasting (Fed decisions, recession)
- Market timing (detect sentiment shifts early)

**Market Categories:**
- **Economics:** Fed decisions, recession, inflation, unemployment
- **Politics:** Elections, legislation, regulatory changes
- **Business:** Acquisitions, earnings beats, product launches
- **Crypto:** Bitcoin price, protocol launches

**Use Cases:**
- "85% chance of Fed rate cut in March → adjust bond duration"
- "70% Republican Senate → healthcare sector impact"
- "40% recession probability (up from 25%) → reduce equity exposure"
- "Bitcoin >$100K probability spiked +20% → crypto thesis validation"

**Key Features:**
- Hourly monitoring (automated)
- Alert on >10% probability shifts
- Time-series tracking (90+ days history)
- Cross-platform validation
- Link markets to portfolio sectors/companies

**Cost:** $0 (free - browser-based, no APIs)

---

### 6. LP Contact Research (`lp_contact_research_prompt.md`)
**Objective:** Populate `lp_key_contact` table with executive contacts for 131 LPs

**Current State:** 0 contacts
**Target Coverage:** 120-131 LPs (90-100%)

**Data Sources:**
- SEC Form ADV (API) - 20-30 LPs
- Website extraction (HTML parsing) - 100-120 LPs
- PDF annual reports (parsing) - 40-60 LPs
- Manual research (CSV import) - 20-30 top LPs

**Priority:** HIGH (Essential for business use)
**Effort:** 12-16 hours
**Impact:** Transforms data from informational to actionable

---

### 7. Family Office Contact Research (`family_office_contact_research_prompt.md`)
**Objective:** Populate `family_office_contacts` table for 100 family offices

**Current State:** 0 contacts
**Target Coverage:** 70-90 family offices (70-90%)

**Data Sources:**
- SEC Form ADV (API) - 40-60 FOs
- Website extraction (HTML parsing) - 25-35 FOs
- SEC 13F filings (XML/HTML parsing) - 15-25 FOs
- Manual research (CSV import) - 20-30 top FOs

**Priority:** HIGH (Essential for business development)
**Effort:** 12-16 hours
**Impact:** Makes family office data usable for outreach
**Special Note:** Extra privacy safeguards required

---

### 8. SEC Form ADV Ingestion (`sec_form_adv_ingestion_prompt.md`)
**Objective:** Comprehensive SEC Form ADV ingestion + contact extraction

**Tasks:**
1. Full ingestion of ~15,000 RIAs (all SEC-registered advisors)
2. Identify ~1,500-2,000 family offices automatically
3. Link SEC ADV data to existing `family_offices` table
4. Extract contacts for LP and FO tables

**Priority:** HIGH (Quick win - API exists)
**Effort:** 2-4 hours (mostly API work)
**Impact:** 
- Automatic family office discovery
- 20-40 LP contacts extracted
- 50-80 FO contacts extracted

---

## Data Collection Philosophy

### Updated Approach (2025)
**"Analyst-Equivalent Research"**

We collect any data that a human analyst could reasonably collect through public research, with proper safeguards.

### ✅ PERMITTED Data Collection Methods:
- Official, documented APIs (preferred)
- Structured web extraction from public websites (HTML parsing)
- Parsing public contact pages, team directories, "About Us" pages
- Extracting publicly disclosed professional information
- Downloading and parsing bulk data files (CSV, Excel, PDF)
- Crawling linked pages (with depth limits and rate limiting)

### ❌ PROHIBITED Actions:
- Bypassing authentication or paywalls
- Aggressive/abusive scraping (ignoring robots.txt, no rate limiting)
- Scraping social media APIs (often violates ToS)
- Collecting personal information not publicly disclosed
- Purchasing third-party contact lists
- Inferring emails/contacts not explicitly stated

### Required Safeguards:
- **Respect robots.txt** (use robotexclusionrulesparser)
- **Conservative rate limiting** (1-2 req/sec per domain)
- **Proper User-Agent** (identify as research bot with contact email)
- **Exponential backoff** on errors
- **Max crawl depth/pages** (prevent runaway crawling)
- **Timeout limits** (10 seconds per request)
- **Respect "do not contact"** or removal requests

---

## Implementation Priority

### Recommended Order:

**Phase 0: AGENTIC QUICK WIN (2-3 days) 🎯 START HERE**
1. **SEC 13F Strategy Only** (from `agentic_portfolio_research_prompt.md`)
   - Implement just the SEC 13F collection strategy
   - Gets portfolio data for 40-60 large LPs immediately
   - Proves the agentic concept
   - **Highest ROI for time invested**

**Phase 1: Quick Wins (2-4 hours)**
2. **SEC Form ADV Full Ingestion** (`sec_form_adv_ingestion_prompt.md`)
   - API-based, structured, comprehensive
   - Automatically identifies family offices
   - Extracts contacts for both LPs and FOs

**Phase 2: Core Contact Collection (12-16 hours)**
3. **LP Contact Research** (`lp_contact_research_prompt.md`)
   - Website scraping + SEC ADV matching
   - PDF parsing of annual reports
   - Manual research for top 50 LPs
   
4. **Family Office Contact Research** (`family_office_contact_research_prompt.md`)
   - Website scraping (with extra privacy safeguards)
   - 13F filing parsing
   - Manual research for high-value FOs

**Phase 3: Second Agentic System (4-5 weeks)**
5. **Private Company Intelligence** (`HANDOFF_private_company_intelligence.md`)
   - Enriches LP/FO portfolio companies automatically
   - 6 strategies (website, funding APIs, SEC, news, jobs, social)
   - 80-90% profile completeness
   - M&A targeting, competitive intel, due diligence

**Phase 4: Additional Agentic Systems (Weeks 8-16)**
6. **Complete Agentic Portfolio Research** (`agentic_portfolio_research_prompt.md`)
   - All 5 strategies implemented
   - LLM-powered news extraction
   - Co-investor network mapping
   - Investment theme classification

7. **Management & Strategy Intelligence** (`HANDOFF_management_strategy_intelligence.md`)
   - Strategic initiative tracking
   - Management quality scoring
   - Executive profiling
   - Strategic positioning analysis

8. **Prediction Market Intelligence** (`HANDOFF_prediction_market_intelligence.md`)
   - Browser-based market monitoring
   - Track probabilities on economic/political events
   - Alert system for significant shifts
   - Risk assessment and scenario planning

9. **Foot Traffic Intelligence** (`HANDOFF_foot_traffic_intelligence.md`) *(Optional)*
   - Location discovery and tracking
   - Multi-source foot traffic data
   - Competitive benchmarking
   - Retail/hospitality portfolio monitoring

**Phase 5: Ongoing Enrichment**
10. Periodic updates (quarterly for SEC ADV, portfolios, management; semi-annual for contacts)

---

## Technical Stack

### Core Libraries:
```python
# HTTP & Async
httpx>=0.24.0
asyncio

# HTML Parsing
beautifulsoup4>=4.12.0
lxml>=4.9.0

# Web Crawling Safeguards
robotexclusionrulesparser>=1.7.1

# PDF Parsing
pdfplumber>=0.10.0
PyPDF2>=3.0.0

# Data Validation
phonenumbers>=8.13.0

# Database
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
```

---

## Success Metrics

### LP Contact Coverage:
- **Tier 1 (High Value):** 30-40 LPs with CIO/CFO direct contact
- **Tier 2 (Medium Value):** 40-50 LPs with named investment staff
- **Tier 3 (Basic):** 30-40 LPs with general inquiry contact
- **Total:** 120-131 LPs (90-100% coverage)

### Family Office Contact Coverage:
- **Tier 1 (High Value):** 20-30 FOs with CIO/CEO/Principal contact
- **Tier 2 (Medium Value):** 20-30 FOs with investment manager contact
- **Tier 3 (Basic):** 20-30 FOs with general inquiry contact
- **Total:** 70-90 FOs (70-90% coverage)

### Data Quality Standards:
- ✅ All emails validated with regex
- ✅ No personal emails (gmail, yahoo) unless in official bio
- ✅ All phone numbers standardized to `+1-XXX-XXX-XXXX` format
- ✅ Duplicate detection (by email, by name)
- ✅ Confidence level assigned (high/medium/low)
- ✅ Source type tracked (sec_adv, website, manual, etc.)

---

## Ethical Guidelines

### For LP Contacts:
- LPs are institutional investors (pensions, endowments, sovereign wealth)
- Most publish staff directories publicly
- Contact collection is standard for institutional outreach
- Focus on professional, publicly disclosed contacts

### For Family Office Contacts:
- **Extra sensitivity required** - family offices are highly private
- Many intentionally keep low profiles
- **Only collect business contacts** from official sources
- **Reject personal information** (residential addresses, personal phones)
- **Provide opt-out mechanism** for removal requests
- Flag contacts as `is_sensitive=TRUE` if principal family member

### Data Usage:
- For research and informational purposes
- If used for outreach: follow CAN-SPAM, provide opt-out, respect requests
- **Do not sell or share** contact data with third parties
- Respect "do not contact" requests immediately

---

## Agent Handoff Checklist

Before starting implementation, each agent should:

1. ✅ Read `RULES.md` in project root
2. ✅ Review the specific prompt for their task
3. ✅ Understand ethical guidelines and safeguards
4. ✅ Verify database schema exists (or create tables)
5. ✅ Install required dependencies
6. ✅ Test with small sample (5-10 records) before full run
7. ✅ Implement job tracking in `ingestion_jobs` table
8. ✅ Add API endpoints to FastAPI app
9. ✅ Register router in `app/main.py`
10. ✅ Test endpoints and verify data quality
11. ✅ Generate coverage report
12. ✅ Update `docs/EXTERNAL_DATA_SOURCES.md` if applicable

---

## Questions or Issues?

If a prompt is unclear or requirements conflict:
1. Prioritize **safety and data compliance** (P0)
2. Prioritize **deterministic, debuggable behavior** (P1)
3. Ask for clarification rather than guessing
4. Document assumptions in code comments

---

## Version History

- **2025-01-05:** Initial prompts created with flexible data collection approach
  - Updated RULES.md to permit "analyst-equivalent research"
  - Added structured web extraction with safeguards
  - Expanded data source coverage
  - Added ethical guidelines and privacy safeguards
