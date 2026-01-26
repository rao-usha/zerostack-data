# Agent Task: LP Contact Research & Enrichment

## Objective
Build an automated research pipeline to populate the `lp_key_contact` table with executive contacts (CIO, CFO, CEO, Investment Directors) for Limited Partners in the `lp_fund` table.

## Current State Analysis
- **lp_fund table:** 131 LPs (pensions, endowments, sovereign wealth funds)
- **lp_key_contact table:** 0 rows (EMPTY)
- **Impact:** Without contacts, the data is informational but not actionable for outreach

## Project Rules
**CRITICAL:** Read and follow `RULES.md` in the project root. Key principles:
- **Analyst-Equivalent Research:** Collect any data a human analyst could collect through public research
- **Respectful Data Collection:** Use proper rate limiting, respect robots.txt, identify as research bot
- **NO authentication bypass:** No content behind paywalls or requiring login
- **NO LinkedIn scraping** (violates ToS, requires authentication)
- **PII Protection:** Only collect publicly disclosed professional contact information
- All data collection must be tracked in `ingestion_jobs` table

## Data Collection Strategy

### Tier 1: Structured APIs (Highest Quality)
**1. SEC Form ADV**
- **API:** SEC EDGAR / IAPD API
- **Coverage:** ~15,000 RIAs (many LPs are SEC-registered)
- **Data:** Firm name, address, phone, email, key personnel
- **Quality:** High (regulatory filing)

### Tier 2: Structured Web Extraction (Good Quality)
**2. LP Official Websites**
- **Source:** URLs from `lp_fund.website_url` field
- **Target Pages:**
  - "Contact Us" pages
  - "Investment Team" or "Our People" pages
  - "About" sections with leadership bios
  - "Investor Relations" pages
- **Data:** Executive names, titles, emails, phone numbers
- **Method:** BeautifulSoup/Scrapy with respectful rate limiting

**3. University Staff Directories**
- **Source:** University investment office pages
- **Examples:**
  - Harvard Management Company: Staff directory
  - Yale Investments Office: Team page
  - Stanford Management Company: Leadership page
- **Data:** CIO, investment staff, titles, emails
- **Method:** Structured HTML parsing

**4. Public Pension Staff Pages**
- **Source:** State/municipal pension websites
- **Examples:**
  - CalPERS: Leadership & staff directory
  - CalSTRS: Investment team page
  - NYC Comptroller: Bureau of Asset Management staff
- **Data:** Board members, CIO, investment directors, contact info
- **Method:** HTML parsing + PDF text extraction (org charts)

### Tier 3: Document Extraction (Medium Quality)
**5. Annual Reports & CAFRs**
- **Source:** PDF annual reports from LP websites
- **Data:** Executive names from letter to stakeholders, org charts
- **Method:** PDF parsing (PyPDF2, pdfplumber)

**6. SEC 13F Filings**
- **Source:** SEC EDGAR (if LP files as investment manager)
- **Data:** Firm contact info from filing headers
- **Method:** XML/HTML parsing

### Tier 4: Manual Research (High-Value Targets)
**7. Targeted Manual Research**
- **Target:** Top 50 LPs by AUM or strategic importance
- **Sources:** LinkedIn (manual viewing, no API scraping), news articles, press releases
- **Method:** CSV template for bulk import
- **Quality:** High (verified by human)

## Database Schema

### Target Table: `lp_key_contact`
```sql
CREATE TABLE lp_key_contact (
    id SERIAL PRIMARY KEY,
    lp_id INTEGER NOT NULL REFERENCES lp_fund(id),
    full_name TEXT NOT NULL,
    title TEXT,
    role_category TEXT, -- CIO, CFO, CEO, Investment Director, Board Member
    email TEXT,
    phone TEXT,
    linkedin_url TEXT,
    source_document_id INTEGER REFERENCES lp_document(id),
    source_type TEXT, -- sec_adv, website, disclosure_doc, manual
    confidence_level TEXT, -- high, medium, low
    is_verified BOOLEAN DEFAULT FALSE,
    collected_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_lp_key_contact_lp_id ON lp_key_contact(lp_id);
CREATE INDEX idx_lp_key_contact_role_category ON lp_key_contact(role_category);
```

## Implementation Approach

### Phase 1: SEC Form ADV Contact Extraction (HIGH PRIORITY)
**File:** `app/sources/sec_adv/contact_extraction.py`

1. **Query existing `sec_form_adv` table** (if data exists)
2. **Match SEC firms to `lp_fund` records** by name/fuzzy matching
3. **Extract contact fields:**
   - Firm phone: `contact_phone` or `firm_phone`
   - Firm email: `contact_email` (if disclosed)
   - Key personnel: Chief Compliance Officer, principals
4. **Insert into `lp_key_contact`** with `source_type='sec_adv'`

### Phase 2: Website Contact Extraction (HIGH PRIORITY)
**File:** `app/research/lp_website_contacts.py`

**Libraries:**
```python
import httpx
from bs4 import BeautifulSoup
import asyncio
from urllib.parse import urljoin, urlparse
import re
```

**Implementation Steps:**
1. **Query `lp_fund` WHERE `website_url IS NOT NULL`**
2. **For each LP:**
   - Fetch homepage HTML
   - Find links to "Contact", "About", "Team", "People", "Investor Relations"
   - Crawl these pages (max depth: 2 levels)
   - Extract structured data:
     - Executive names (using title patterns: CIO, CFO, CEO, Director)
     - Email addresses (validate format, filter spam)
     - Phone numbers (standardize format)
     - LinkedIn URLs (for future manual enrichment)
3. **Pattern Matching Examples:**
   ```python
   # Executive titles
   title_patterns = [
       r'\b(?:Chief Investment Officer|CIO)\b',
       r'\b(?:Chief Financial Officer|CFO)\b',
       r'\b(?:Chief Executive Officer|CEO)\b',
       r'\b(?:Managing Director|Investment Director)\b',
       r'\b(?:Head of|Director of) Investments?\b'
   ]
   
   # Email extraction
   email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
   
   # Phone extraction
   phone_pattern = r'\b(?:\+?1[-.]?)?\(?\d{3}\)?[-.]?\d{3}[-.]?\d{4}\b'
   ```
4. **Store findings in `lp_key_contact`** with `source_type='website'`, `confidence_level='medium'`

**Safeguards:**
- `asyncio.Semaphore(2)` - Max 2 concurrent requests
- 2-second delay between requests per domain
- User-Agent: `"NexdataResearch/1.0 (research@nexdata.com)"`
- Respect robots.txt (use robotexclusionrulesparser library)
- Timeout: 10 seconds per request
- Max pages per LP: 5 (prevent runaway crawling)
- Skip if status code != 200 or content-type != text/html

### Phase 3: Manual Research Template (LOW PRIORITY)
**File:** `docs/MANUAL_RESEARCH/lp_contact_template.csv`

For high-value LPs without automated contact discovery:
1. **Create CSV template:**
   ```csv
   lp_id,lp_name,full_name,title,role_category,email,phone,source_url,notes
   1,CalPERS,Ben Meng,CIO,CIO,bmeng@calpers.ca.gov,,https://www.calpers.ca.gov/,Former CIO
   ```
2. **Manual research targets:**
   - Top 50 LPs by AUM
   - LPs with recent `lp_strategy_snapshot` entries (actively managed)
3. **Bulk import script:** `app/research/import_manual_contacts.py`

## Validation & Quality Checks

### Contact Data Quality Rules
1. **Email validation:**
   - Must match regex: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
   - Reject generic emails: info@, contact@, admin@, webmaster@ (unless LP-specific)
2. **Phone validation:**
   - Standardize format: `+1-XXX-XXX-XXXX` or `(XXX) XXX-XXXX`
   - Must have valid country code or area code
3. **Name validation:**
   - Must be 2+ words (first + last name minimum)
   - No generic titles like "Investment Office" without person name
4. **Duplicate detection:**
   - Check existing `lp_key_contact` records before inserting
   - Flag duplicates based on (lp_id, email) or (lp_id, full_name)

## API Endpoints

### Create FastAPI Router: `app/api/v1/lp_contacts.py`

**Endpoints:**
- `POST /api/v1/lp/contacts/extract-from-sec-adv` - Extract contacts from SEC ADV data
  - Returns: job_id, contacts_found_count
- `POST /api/v1/lp/contacts/extract-from-websites` - Crawl LP websites for contacts
  - Query params: `lp_ids: List[int]` (optional, default: all with website_url)
  - Returns: job_id, websites_processed, contacts_found
- `POST /api/v1/lp/contacts/import-manual` - Import from CSV
  - Body: CSV file upload
  - Returns: rows_imported, validation_errors
- `GET /api/v1/lp/contacts/summary` - Get contact coverage stats
  - Returns: lps_total, lps_with_contacts, contact_count_by_role

**Register in `app/main.py`:**
```python
from app.api.v1 import lp_contacts
app.include_router(lp_contacts.router, prefix="/api/v1", tags=["lp-contacts"])
```

## Success Criteria
- [ ] `lp_key_contact` table created with proper schema and indexes
- [ ] SEC Form ADV contact extraction implemented and tested
- [ ] Website contact extraction implemented with rate limiting
- [ ] Manual import template created and import script functional
- [ ] Contact validation rules implemented (email, phone, name)
- [ ] Duplicate detection working
- [ ] Job tracking in `ingestion_jobs` table for automated collection
- [ ] API endpoints functional
- [ ] Test: Extract contacts for at least 50 LPs
- [ ] Test: Validate contact quality (no spam emails, valid phone formats)
- [ ] Coverage report: "X% of LPs now have at least 1 contact"

## Testing Steps
1. **Start service:** `docker-compose up -d`
2. **Check current state:**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM lp_fund;"
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM lp_key_contact;"
   ```
3. **Test SEC ADV extraction:**
   ```bash
   curl -X POST "http://localhost:8001/api/v1/lp/contacts/extract-from-sec-adv"
   ```
4. **Test website extraction (5 LPs):**
   ```bash
   curl -X POST "http://localhost:8001/api/v1/lp/contacts/extract-from-websites" \
     -H "Content-Type: application/json" \
     -d '{"lp_ids": [1, 2, 3, 4, 5]}'
   ```
5. **Check results:**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
     SELECT lp_id, full_name, title, role_category, email, source_type 
     FROM lp_key_contact 
     LIMIT 20;
   "
   ```
6. **Coverage report:**
   ```bash
   curl "http://localhost:8001/api/v1/lp/contacts/summary"
   ```

## Ethical & Legal Considerations

### What is ALLOWED:
- ✅ Extracting contacts from SEC filings (public regulatory data)
- ✅ Parsing official LP websites (contact, team, about pages)
- ✅ Collecting publicly disclosed executive names and titles
- ✅ Extracting emails from official staff directories
- ✅ Phone numbers from contact pages
- ✅ Crawling linked pages (with depth limits and rate limiting)
- ✅ PDF parsing of annual reports for executive names
- ✅ Any data a human analyst could collect via public research

### What is PROHIBITED:
- ❌ LinkedIn API scraping (violates ToS)
- ❌ Bypassing authentication or paywalls
- ❌ Collecting personal emails (gmail, yahoo) not in professional context
- ❌ Aggressive scraping (ignoring robots.txt, no rate limiting)
- ❌ Purchasing contact lists from third-party vendors
- ❌ Scraping private social media profiles
- ❌ Inferring emails not explicitly stated (no "guessing" formats)

### Data Usage Guidelines:
- This data is for **research and informational purposes**
- If used for outreach, follow CAN-SPAM Act and GDPR principles
- Provide opt-out mechanism in any communications
- Do not sell or share contact data with third parties
- Respect "do not contact" requests

## Expected Outcomes

### Target Coverage (Realistic):
- **SEC ADV Match:** 20-30 LPs (SEC-registered RIAs)
- **Website Extraction:** 100-120 LPs (contact info + exec names from team pages)
- **PDF Annual Reports:** 40-60 LPs (executive names from reports)
- **Manual Research:** 20-30 high-priority LPs (CIO/CFO direct contacts)
- **Total:** 120-131 LPs with at least 1 contact (90-100% coverage)

### Contact Quality Tiers:
- **Tier 1 (High Value):** CIO, CEO, CFO with direct email/phone (Target: 30-40 LPs)
- **Tier 2 (Medium Value):** Investment Directors, named contacts (Target: 40-50 LPs)
- **Tier 3 (Basic):** General inquiry email/phone (Target: 30-40 LPs)

## Dependencies
```
# Add to requirements.txt:
beautifulsoup4>=4.12.0          # HTML parsing
phonenumbers>=8.13.0            # Phone validation/formatting
robotexclusionrulesparser>=1.7.1  # robots.txt handling
lxml>=4.9.0                     # Fast HTML/XML parsing
pdfplumber>=0.10.0              # PDF text extraction
PyPDF2>=3.0.0                   # PDF parsing (alternative)
```

## Priority Order
1. **First:** SEC Form ADV extraction (quickest, highest quality)
2. **Second:** Website contact page extraction (good coverage)
3. **Third:** Manual research template for top 50 LPs

## Notes
- Some LPs (especially sovereign wealth funds) may have minimal public contact info
- University endowments typically have the most transparent staff directories
- Public pensions often list board members but not investment staff
- Consider creating a "research notes" field for tracking manual research attempts
- Update `lp_fund` table with `last_contact_research_date` field to track coverage
