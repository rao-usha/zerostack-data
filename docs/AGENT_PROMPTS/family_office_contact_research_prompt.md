# Agent Task: Family Office Contact Research & Enrichment

## Objective
Build an automated research pipeline to populate the `family_office_contacts` table with key decision-maker contacts for family offices in the `family_offices` table.

## Current State Analysis
- **family_offices table:** 100 family offices (preliminary data)
- **family_office_contacts table:** 0 rows (EMPTY)
- **Impact:** Without contacts, family office data is unusable for business development

## Project Rules
**CRITICAL:** Read and follow `RULES.md` in the project root. Key principles:
- **Analyst-Equivalent Research:** Collect any data a human analyst could collect through public research
- **Respectful Data Collection:** Use proper rate limiting, respect robots.txt, identify as research bot
- **NO authentication bypass:** No content behind paywalls or requiring login
- **NO LinkedIn scraping** (violates ToS, requires authentication)
- **PII Protection:** Only collect BUSINESS contact information that is publicly disclosed
- **Family Office Sensitivity:** Family offices are private; be extra conservative and respectful
- All data collection must be tracked in `ingestion_jobs` table

## Special Considerations for Family Offices

### Privacy Concerns:
- Family offices are **HIGHLY PRIVATE** entities
- Many intentionally keep low profiles to protect principal families
- **ONLY** collect information that is:
  - Publicly disclosed by the family office itself
  - Available through regulatory filings (SEC Form ADV if registered)
  - Listed on official family office websites

### What NOT to Do:
- ❌ Do not scrape personal social media profiles
- ❌ Do not infer family member contact info
- ❌ Do not collect residential addresses
- ❌ Do not scrape news articles for "leaked" information
- ❌ Do not use third-party "wealth intelligence" databases without explicit permission

## Data Collection Strategy

### Tier 1: Structured APIs (Highest Quality)
**1. SEC Form ADV**
- **API:** SEC EDGAR / IAPD API
- **Coverage:** ~2,000 single-family offices (SEC-registered)
- **Data:** Firm name, address, phone, email, CCO, principals, AUM
- **Quality:** High (regulatory filing)
- **Method:** Match by `sec_crd_number` or name

### Tier 2: Structured Web Extraction (Good Quality)
**2. Family Office Official Websites**
- **Source:** URLs from `family_offices.website` field (~30-40 have websites)
- **Target Pages:**
  - "Contact Us" pages
  - "Our Team" or "People" pages
  - "About" sections with leadership bios
  - "Services" pages (for multi-family offices)
- **Data:** Executive names, titles, office email/phone
- **Method:** BeautifulSoup HTML parsing with strict rate limiting
- **Note:** Many family offices intentionally have minimal web presence

**3. SEC Form 13F Filings**
- **Source:** SEC EDGAR (family offices with >$100M equity)
- **Data:** Firm contact info from filing headers, signing authority names
- **Method:** XML/HTML parsing
- **Coverage:** ~150-200 large family offices

**4. RIA Firm Directories**
- **Source:** SEC.gov RIA search, publicly accessible profiles
- **Data:** Business contact info for registered family offices
- **Method:** Structured scraping of search results

### Tier 3: Public Disclosure Documents (Medium Quality)
**5. Annual/Quarterly Letters (if public)**
- **Source:** Some family offices publish investment letters
- **Data:** Principal names, investment team members
- **Method:** PDF parsing of publicly shared documents

**6. News Articles & Press Releases**
- **Source:** Google News, Bloomberg (free tier), press releases on FO websites
- **Data:** Executive names mentioned in public announcements
- **Method:** Text extraction and NLP for name recognition
- **Constraint:** Only executives explicitly named in professional capacity

### Tier 4: Manual Research (High-Value/Difficult)
**7. Targeted Manual Research**
- **Target:** Top 50 family offices by estimated AUM
- **Sources:** Manual LinkedIn viewing (no scraping), news articles, Pitchbook (if accessible)
- **Method:** CSV template for bulk import
- **Quality:** High (human-verified)

## Database Schema

### Target Table: `family_office_contacts`
Check existing schema and update if needed:

```sql
-- Check if table exists and view schema:
-- If missing fields, add them via migration

ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS source_type TEXT;
ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS confidence_level TEXT;
ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS collected_date DATE;

CREATE INDEX IF NOT EXISTS idx_fo_contacts_office_id ON family_office_contacts(office_id);
CREATE INDEX IF NOT EXISTS idx_fo_contacts_role ON family_office_contacts(role);
```

Expected schema:
```
- id: SERIAL PRIMARY KEY
- office_id: INTEGER NOT NULL REFERENCES family_offices(id)
- full_name: TEXT NOT NULL
- title: TEXT
- role: TEXT (CIO, CFO, CEO, Principal, Investment Manager)
- email: TEXT
- phone: TEXT
- linkedin_url: TEXT
- source_type: TEXT (sec_adv, website, manual, 13f)
- confidence_level: TEXT (high, medium, low)
- is_verified: BOOLEAN DEFAULT FALSE
- collected_date: DATE
- created_at: TIMESTAMP DEFAULT NOW()
```

## Implementation Approach

### Phase 1: SEC Form ADV Contact Extraction (HIGH PRIORITY)
**File:** `app/sources/sec_adv/family_office_contacts.py`

1. **Query `family_offices` WHERE `sec_registered = TRUE` AND `sec_crd_number IS NOT NULL`**
2. **Match to `sec_form_adv` table** by CRD number
3. **Extract contact fields:**
   - Firm business phone
   - Firm business email (if disclosed)
   - Chief Compliance Officer name
   - Managing Member/Principal names
4. **Insert into `family_office_contacts`** with `source_type='sec_adv'`, `confidence_level='high'`

### Phase 2: Website Contact Extraction (HIGH PRIORITY)
**File:** `app/research/fo_website_contacts.py`

**Libraries:**
```python
import httpx
from bs4 import BeautifulSoup
import asyncio
from urllib.parse import urljoin, urlparse
import re
```

**Implementation Steps:**
1. **Query `family_offices` WHERE `website IS NOT NULL`**
2. **For each family office:**
   - Fetch homepage HTML
   - Find links to "Contact", "About", "Team", "People" pages
   - Crawl these pages (max depth: 2, max pages: 3)
   - Extract structured data:
     - Executive names (CIO, CEO, Principal, Managing Director)
     - Office email (prefer business emails, reject personal)
     - Office phone
     - Mailing address (business only)
3. **Pattern Matching (Family Office Specific):**
   ```python
   # Family office executive titles
   title_patterns = [
       r'\b(?:Chief Investment Officer|CIO)\b',
       r'\b(?:Chief Executive Officer|CEO)\b',
       r'\b(?:Managing Director|Managing Member)\b',
       r'\b(?:Principal|Family Principal)\b',
       r'\b(?:Investment Director|Portfolio Manager)\b',
       r'\b(?:Chief Compliance Officer|CCO)\b'
   ]
   
   # Exclude personal/family names without professional context
   # Only capture names with associated titles
   ```
4. **Privacy Safeguards:**
   - **Skip if page mentions "private" or "invite-only"**
   - **Do not collect principal family surnames without explicit disclosure**
   - **Flag contacts as `is_sensitive=TRUE` if principal family member**
5. **Store findings in `family_office_contacts`** with `source_type='website'`, `confidence_level='medium'`

**Strict Safeguards (More Conservative than LPs):**
- `asyncio.Semaphore(1)` - Max 1 concurrent request (family offices are sensitive)
- 5-second delay between requests
- User-Agent: `"NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"`
- Respect robots.txt STRICTLY
- Timeout: 10 seconds per request
- Max pages per family office: 3 (prevent over-crawling)
- **Abort immediately if login required or paywall detected**
- **Skip if site has "do not contact" or similar language**

### Phase 3: Manual Research Template (CRITICAL FOR TOP FAMILY OFFICES)
**File:** `docs/MANUAL_RESEARCH/family_office_contact_template.csv`

For high-value family offices without automated contact discovery:
1. **Create CSV template:**
   ```csv
   office_id,office_name,full_name,title,role,email,phone,source_url,notes,confidence_level
   1,Cascade Investment,Michael Larson,CIO,CIO,,,https://example.com/,Manages Gates fortune,high
   ```
2. **Manual research targets:**
   - Top 50 family offices by estimated AUM
   - Family offices with known investment activity
   - Single-family offices (more likely to be decision-makers than multi-family offices)
3. **Bulk import script:** `app/research/import_fo_contacts.py`

### Phase 4: SEC Form 13F Cross-Reference (LOW PRIORITY)
**File:** `app/research/fo_13f_contact_matching.py`

1. **Query SEC 13F filings** for investment managers
2. **Match manager names to `family_offices` by name similarity**
3. **Extract firm contact info** from 13F headers
4. **Store in `family_office_contacts`** with `source_type='13f'`, `confidence_level='low'`

## Validation & Quality Checks

### Contact Data Quality Rules
1. **Email validation:**
   - Must match regex: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
   - **REJECT personal emails** (gmail, yahoo, hotmail) - family office contacts should be business emails
   - Acceptable generic emails: info@, contact@ (if family-office-specific domain)
2. **Phone validation:**
   - Standardize format: `+1-XXX-XXX-XXXX` or `(XXX) XXX-XXXX`
   - Must be business phone, not residential
3. **Name validation:**
   - Must be 2+ words (first + last name)
   - **DO NOT** store principal family last names without explicit public disclosure
4. **Duplicate detection:**
   - Check existing `family_office_contacts` before inserting
   - Flag duplicates based on (office_id, email) or (office_id, full_name)

### Privacy Safeguards:
- **Before storing ANY contact:** Verify it's from an official/public source
- **Flag sensitive contacts:** Add `is_sensitive = TRUE` for principal family members
- **Audit log:** Track all contact collection with source URL and date

## API Endpoints

### Create FastAPI Router: `app/api/v1/family_office_contacts.py`

**Endpoints:**
- `POST /api/v1/family-offices/contacts/extract-from-sec-adv` - Extract from SEC ADV
  - Returns: job_id, contacts_found_count
- `POST /api/v1/family-offices/contacts/extract-from-websites` - Crawl FO websites
  - Query params: `office_ids: List[int]` (optional)
  - Returns: job_id, websites_processed, contacts_found
- `POST /api/v1/family-offices/contacts/import-manual` - Import from CSV
  - Body: CSV file upload
  - Returns: rows_imported, validation_errors
- `GET /api/v1/family-offices/contacts/summary` - Get contact coverage stats
  - Returns: family_offices_total, offices_with_contacts, contact_count_by_role

**Register in `app/main.py`:**
```python
from app.api.v1 import family_office_contacts
app.include_router(family_office_contacts.router, prefix="/api/v1", tags=["family-office-contacts"])
```

## Success Criteria
- [ ] `family_office_contacts` table schema verified/updated
- [ ] SEC Form ADV contact extraction implemented and tested
- [ ] Website contact extraction implemented with strict rate limiting
- [ ] Manual import template created and import script functional
- [ ] Contact validation rules implemented (email, phone, name)
- [ ] Privacy safeguards implemented (reject personal emails, audit log)
- [ ] Duplicate detection working
- [ ] Job tracking in `ingestion_jobs` table
- [ ] API endpoints functional
- [ ] Test: Extract contacts for at least 30 family offices
- [ ] Test: Validate no personal emails or residential info collected
- [ ] Coverage report: "X% of family offices now have at least 1 contact"

## Testing Steps
1. **Start service:** `docker-compose up -d`
2. **Check current state:**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM family_offices;"
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM family_office_contacts;"
   ```
3. **Test SEC ADV extraction:**
   ```bash
   curl -X POST "http://localhost:8001/api/v1/family-offices/contacts/extract-from-sec-adv"
   ```
4. **Test website extraction (3 FOs):**
   ```bash
   curl -X POST "http://localhost:8001/api/v1/family-offices/contacts/extract-from-websites" \
     -H "Content-Type: application/json" \
     -d '{"office_ids": [1, 2, 3]}'
   ```
5. **Check results:**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
     SELECT fo.name, c.full_name, c.title, c.role, c.email, c.source_type 
     FROM family_office_contacts c
     JOIN family_offices fo ON c.office_id = fo.id
     LIMIT 20;
   "
   ```
6. **Coverage report:**
   ```bash
   curl "http://localhost:8001/api/v1/family-offices/contacts/summary"
   ```

## Ethical & Legal Considerations

### What is ALLOWED:
- ✅ Extracting contacts from SEC ADV filings (public regulatory data)
- ✅ Parsing official family office websites (contact, team, about pages)
- ✅ Extracting business emails from official staff directories
- ✅ Business phone numbers from contact pages
- ✅ Executive names with professional titles from team pages
- ✅ Parsing 13F filings for firm contact info
- ✅ News articles mentioning executives in professional capacity
- ✅ Any data a human analyst could collect via respectful public research

### What is PROHIBITED:
- ❌ LinkedIn API scraping (violates ToS)
- ❌ Bypassing authentication or paywalls
- ❌ Collecting principal family member PERSONAL information
- ❌ Personal emails (gmail, yahoo) unless in professional bio
- ❌ Residential addresses or personal phone numbers
- ❌ Purchasing contact lists from third-party vendors
- ❌ Scraping private social media profiles
- ❌ "Investigative" scraping of private information
- ❌ Ignoring "do not contact" or privacy requests

### Data Usage Guidelines:
- This data is for **research and informational purposes ONLY**
- If used for outreach:
  - Follow CAN-SPAM Act and GDPR principles
  - Provide clear opt-out mechanism
  - Respect "do not contact" requests IMMEDIATELY
  - Use professional, non-intrusive communication
- **DO NOT** sell or share family office contact data
- **DO NOT** use for cold-calling without explicit consent
- Consider adding "data usage policy" acknowledgment before export

## Expected Outcomes

### Target Coverage (Realistic):
- **SEC ADV Match:** 40-60 family offices (SEC-registered)
- **Website Extraction:** 25-35 family offices (limited web presence typical)
- **13F Filings:** 15-25 large family offices
- **Manual Research:** 20-30 high-priority family offices
- **News/Press Releases:** 10-15 family offices
- **Total:** 70-90 family offices with at least 1 contact (70-90% coverage)

### Contact Quality Tiers:
- **Tier 1 (High Value):** CIO, CEO, Principal with business email/phone (Target: 20-30 FOs)
- **Tier 2 (Medium Value):** Investment Managers, CCO with contact info (Target: 20-30 FOs)
- **Tier 3 (Basic):** General inquiry email/phone only (Target: 20-30 FOs)

### Expected Challenges:
- Many family offices intentionally keep low profile
- Limited public information available
- Manual research will be critical for high-value targets
- Some family offices may request removal from database (provide mechanism)

## Dependencies
```
# Add to requirements.txt:
beautifulsoup4>=4.12.0          # HTML parsing
phonenumbers>=8.13.0            # Phone validation/formatting
robotexclusionrulesparser>=1.7.1  # robots.txt handling
lxml>=4.9.0                     # Fast HTML/XML parsing
pdfplumber>=0.10.0              # PDF text extraction (for 13F, letters)
```

## Priority Order
1. **First:** SEC Form ADV extraction (best quality, compliance-vetted)
2. **Second:** Manual research for top 50 family offices (highest value)
3. **Third:** Website contact page extraction (limited coverage expected)

## Notes
- Family offices are MORE PRIVATE than LPs - be extra conservative
- Consider adding `opt_out` flag to `family_offices` table for removal requests
- Single-family offices (type='single') are highest priority for contacts
- Multi-family offices may have client service contacts rather than investment decision-makers
- Some family offices operate under stealth names (not family surname)
- Update `family_offices` table with `last_contact_research_date` to track coverage
- Consider creating "research difficulty" rating: easy/medium/hard/private
