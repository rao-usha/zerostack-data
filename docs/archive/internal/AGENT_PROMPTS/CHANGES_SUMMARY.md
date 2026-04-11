# Changes Summary - Updated Data Collection Rules & Prompts

## Date: 2025-01-05

## Overview
Updated project rules and agent prompts to support **"Analyst-Equivalent Research"** - allowing any data collection method that a human analyst could use through public research, with proper safeguards.

---

## 1. Updated RULES.md

### What Changed:
**Old Rule:**
- "Never scrape arbitrary websites"
- "Only use official, documented APIs"

**New Rule:**
- Permits **structured web extraction** from public websites with safeguards
- Permits **PDF parsing** of public documents
- Permits **any method a human analyst could use** for public research

### New Section: Data Collection Methods

**✅ Permitted:**
- Official, documented APIs (preferred)
- Structured data extraction from public websites
- Parsing public contact pages, directories, "About Us" pages
- Extracting publicly disclosed information
- Downloading bulk data files (CSV, Excel, PDF)

**❌ Prohibited:**
- Accessing content behind paywalls or requiring login
- Aggressive/abusive scraping (ignoring robots.txt, rate limits)
- Collecting personal information not publicly disclosed
- Circumventing access controls
- Scraping social media APIs (often violates ToS)

**Required Safeguards:**
- Respect robots.txt
- Conservative rate limiting (1-2 req/sec per domain)
- Proper User-Agent identification
- Exponential backoff on errors
- Respect "do not contact" requests

---

## 2. Updated Agent Prompts

### A. lp_contact_research_prompt.md

**Key Enhancements:**

**Data Sources Expanded:**
- ✅ SEC Form ADV (API) - unchanged
- ✅ **Website HTML parsing** - now includes team pages, not just contact pages
- ✅ **PDF parsing** - annual reports, CAFRs for executive names
- ✅ **Staff directories** - university investment offices, pension websites
- ✅ Manual research - unchanged

**New Capabilities:**
- Crawl linked pages (max depth: 2 levels, max 5 pages per LP)
- Pattern matching for executive titles (CIO, CFO, CEO, Investment Director)
- Email and phone extraction with validation
- PDF text extraction from annual reports

**Expected Coverage Increase:**
- Old: 75-100% (100-130 LPs)
- New: 90-100% (120-131 LPs)

**New Dependencies:**
```
beautifulsoup4>=4.12.0
robotexclusionrulesparser>=1.7.1
lxml>=4.9.0
pdfplumber>=0.10.0
PyPDF2>=3.0.0
phonenumbers>=8.13.0
```

---

### B. family_office_contact_research_prompt.md

**Key Enhancements:**

**Data Sources Expanded:**
- ✅ SEC Form ADV (API) - unchanged
- ✅ **Website HTML parsing** - team pages, contact pages
- ✅ **SEC 13F filings** - parse for firm contacts
- ✅ **News articles & press releases** - executive names in professional context
- ✅ Manual research - unchanged

**Extra Privacy Safeguards (Family Office Specific):**
- More conservative rate limiting (1 req/sec, 5 sec delays)
- Max 3 pages per family office (vs 5 for LPs)
- Skip if "private" or "invite-only" detected
- Flag principal family members as `is_sensitive=TRUE`
- Reject personal emails more strictly

**Expected Coverage Increase:**
- Old: 60-80% (60-80 FOs)
- New: 70-90% (70-90 FOs)

**New Capabilities:**
- 13F filing parsing for large family offices (>$100M equity)
- News article text extraction for executive mentions
- More robust website crawling (while maintaining privacy respect)

---

### C. sec_form_adv_ingestion_prompt.md

**Key Enhancements:**

**Optional Website Enrichment:**
- New function: `enrich_contacts_from_websites(db)`
- For firms with website_url in `sec_form_adv` table
- Crawl "Team" or "People" pages for additional contacts
- Finds non-principal staff (analysts, portfolio managers)

**No Major Changes:**
- Primary focus remains API-based (IAPD API)
- Website enrichment is optional enhancement
- Already well-structured for comprehensive ingestion

---

## 3. New Documentation

### Created: docs/AGENT_PROMPTS/README.md

**Contents:**
- Overview of all available prompts
- Data collection philosophy ("Analyst-Equivalent Research")
- Permitted vs prohibited actions (detailed)
- Required safeguards
- Implementation priority recommendations
- Technical stack and dependencies
- Success metrics and quality standards
- Ethical guidelines (LP vs FO differences)
- Agent handoff checklist

---

## 4. Impact Summary

### Coverage Improvements:

**LP Contacts:**
- Before: 75-100% coverage, mostly basic contacts
- After: 90-100% coverage, more executive-level contacts
- New sources: Website team pages, PDF reports, staff directories

**Family Office Contacts:**
- Before: 60-80% coverage, limited by strict rules
- After: 70-90% coverage, more comprehensive
- New sources: 13F filings, website team pages, news articles (with safeguards)

### Data Quality Improvements:
- ✅ More executive-level contacts (CIO, CFO, CEO)
- ✅ Better coverage of named investment professionals
- ✅ Multiple contact types per organization
- ✅ Source tracking for provenance

### Ethical Standards Maintained:
- ✅ All data publicly disclosed
- ✅ Proper attribution and sourcing
- ✅ Rate limiting and respectful crawling
- ✅ Privacy safeguards for sensitive entities
- ✅ Opt-out mechanisms

---

## 5. Implementation Recommendations

### Recommended Order:

**Week 1: Quick Win**
1. **SEC Form ADV Full Ingestion** (2-4 hours)
   - Gets 20-40 LP contacts
   - Gets 50-80 FO contacts
   - Identifies 1,500-2,000 additional family offices
   - API-based, reliable, comprehensive

**Week 2-3: Core Collection**
2. **LP Contact Research** (12-16 hours)
   - Website scraping (100-120 LPs)
   - PDF parsing (40-60 LPs)
   - Manual research (20-30 top LPs)

3. **Family Office Contact Research** (12-16 hours)
   - Website scraping (25-35 FOs)
   - 13F parsing (15-25 FOs)
   - Manual research (20-30 top FOs)

**Ongoing:**
4. Quarterly SEC ADV updates
5. Semi-annual contact refreshes
6. Respond to removal requests promptly

---

## 6. Technical Requirements

### New Dependencies to Add:
```bash
pip install beautifulsoup4>=4.12.0
pip install robotexclusionrulesparser>=1.7.1
pip install lxml>=4.9.0
pip install pdfplumber>=0.10.0
pip install PyPDF2>=3.0.0
pip install phonenumbers>=8.13.0
```

### Database Schema Additions:
```sql
-- Add to lp_key_contact if not exists:
ALTER TABLE lp_key_contact ADD COLUMN IF NOT EXISTS source_type TEXT;
ALTER TABLE lp_key_contact ADD COLUMN IF NOT EXISTS confidence_level TEXT;
ALTER TABLE lp_key_contact ADD COLUMN IF NOT EXISTS collected_date DATE;

-- Add to family_office_contacts if not exists:
ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS source_type TEXT;
ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS confidence_level TEXT;
ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS is_sensitive BOOLEAN DEFAULT FALSE;
ALTER TABLE family_office_contacts ADD COLUMN IF NOT EXISTS collected_date DATE;
```

---

## 7. Key Differences: Old vs New

| Aspect | Old Rules | New Rules |
|--------|-----------|-----------|
| **Philosophy** | API-only, very conservative | Analyst-equivalent research |
| **Web Scraping** | "Never scrape" | Permitted with safeguards |
| **PDF Parsing** | Not mentioned | Explicitly permitted |
| **Crawl Depth** | N/A (no crawling) | Max 2-3 levels, rate limited |
| **Data Sources** | SEC APIs only | APIs + websites + PDFs + documents |
| **Coverage** | 60-80% | 90-100% (LP), 70-90% (FO) |
| **Quality** | Basic contacts | Executive-level contacts |

---

## 8. Risk Mitigation

### Potential Concerns Addressed:

**Concern:** "Are we being too aggressive?"
- **Mitigation:** Rate limiting, robots.txt, proper User-Agent, max page limits

**Concern:** "Privacy issues with family offices?"
- **Mitigation:** Extra safeguards for FOs (slower crawling, skip if "private" detected, flag sensitive contacts)

**Concern:** "Legal/ToS violations?"
- **Mitigation:** No authentication bypass, no social media API scraping, public data only

**Concern:** "Data quality issues?"
- **Mitigation:** Validation rules (email, phone), confidence scoring, source tracking

---

## 9. Next Steps

For the user to proceed:

1. ✅ **Review updated RULES.md** - ensure comfortable with new approach
2. ✅ **Review agent prompts** - ensure they align with business needs
3. ✅ **Choose implementation order** - recommend SEC ADV first (quick win)
4. ✅ **Assign to agents** - hand off prompts to separate agents/workers
5. ✅ **Monitor first runs** - test with small samples before full ingestion
6. ✅ **Verify data quality** - check contact validation and coverage
7. ✅ **Establish removal process** - respond to opt-out requests

---

## 10. Files Changed

### Modified:
- `RULES.md` - Updated data collection rules
- `docs/AGENT_PROMPTS/lp_contact_research_prompt.md` - Expanded data sources
- `docs/AGENT_PROMPTS/family_office_contact_research_prompt.md` - Expanded with safeguards
- `docs/AGENT_PROMPTS/sec_form_adv_ingestion_prompt.md` - Added optional enrichment

### Created:
- `docs/AGENT_PROMPTS/README.md` - Overview and guidelines
- `docs/AGENT_PROMPTS/CHANGES_SUMMARY.md` - This file

---

## Conclusion

These changes enable **comprehensive, respectful, ethical contact collection** while maintaining all safety and privacy safeguards. The approach is now aligned with what a human analyst would do, making it both more effective and more realistic.

**Key Principle:** "If a human analyst can find it through public research, we can automate it—with proper rate limiting and respect."
