# People Data Collection Guide

Technical guide for collecting and enriching people data in the People & Org Chart Intelligence Platform.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Sources](#data-sources)
3. [Collection Agents](#collection-agents)
4. [Email Inference](#email-inference)
5. [Change Detection](#change-detection)
6. [Data Quality](#data-quality)
7. [Scheduling](#scheduling)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Collection Pipeline                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│  │   Website   │    │    SEC      │    │    News     │        │
│  │   Crawler   │    │   Parser    │    │   Scanner   │        │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘        │
│         │                  │                  │                │
│         └──────────────────┼──────────────────┘                │
│                            │                                    │
│                    ┌───────▼───────┐                           │
│                    │  LLM Extractor │                          │
│                    │  (Claude/GPT)  │                          │
│                    └───────┬───────┘                           │
│                            │                                    │
│                    ┌───────▼───────┐                           │
│                    │    Person     │                           │
│                    │   Resolver    │                           │
│                    └───────┬───────┘                           │
│                            │                                    │
│         ┌──────────────────┼──────────────────┐                │
│         │                  │                  │                │
│  ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐        │
│  │   People    │    │  Company    │    │ Leadership  │        │
│  │   Table     │    │   People    │    │  Changes    │        │
│  └─────────────┘    └─────────────┘    └─────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### 1. Company Websites

**Source Type:** `website_crawl`

The primary source for leadership data is company websites, specifically their "About Us" or "Leadership" pages.

**Data Extracted:**
- Executive names and titles
- Photos
- Bios
- Sometimes email addresses

**Configuration:**
```python
# Company model fields for website crawling
company.website = "https://www.acme.com"
company.leadership_page_url = "https://www.acme.com/about/leadership"
company.newsroom_url = "https://www.acme.com/news"
```

**LLM Extraction Prompt:**
```
Extract all executives and leadership team members from this webpage.
For each person, extract:
- Full name
- Title/Position
- Bio (if available)
- Photo URL (if available)

Return as structured JSON.
```

### 2. SEC EDGAR Filings

**Source Type:** `sec_parse`

For public companies (with CIK), we extract executive data from SEC filings.

**Filing Types:**
| Filing | Data |
|--------|------|
| DEF 14A (Proxy) | NEOs, board members, compensation |
| 10-K | Executive officers, directors |
| 8-K Item 5.02 | Executive changes |

**Data Extracted:**
- Named Executive Officers (NEOs)
- Board of Directors
- Compensation data
- Age, tenure, biography

### 3. News & Press Releases

**Source Type:** `news_scan`

Monitors company newsrooms for leadership announcements.

**Detection Patterns:**
- "appointed as"
- "promoted to"
- "departing from"
- "retiring from"
- "joins as"

### 4. SEC 8-K Filings

**Source Type:** `sec_8k_check`

Real-time detection of executive changes via 8-K Item 5.02 filings.

**Item 5.02 Categories:**
- (a) Departure of Directors
- (b) Departure of Principal Officers
- (c) Appointment of Principal Officers
- (d) Election of Directors
- (e) Compensatory Arrangements

---

## Collection Agents

### Base Collector

```python
# app/sources/people_collection/base_collector.py

class BaseCollector:
    """Base class for all collection agents."""

    def __init__(self, db: Session, llm_client: LLMClient):
        self.db = db
        self.llm = llm_client
        self.rate_limiter = RateLimiter()

    async def collect(self, company: IndustrialCompany) -> CollectionResult:
        """Collect leadership data for a company."""
        raise NotImplementedError

    async def extract_people(self, content: str) -> List[ExtractedPerson]:
        """Use LLM to extract people from content."""
        prompt = self.build_extraction_prompt(content)
        response = await self.llm.complete(prompt)
        return self.parse_extraction_response(response)
```

### Website Agent

```python
# app/sources/people_collection/website_agent.py

class WebsiteAgent(BaseCollector):
    """Collects leadership data from company websites."""

    async def collect(self, company: IndustrialCompany) -> CollectionResult:
        # 1. Fetch leadership page
        html = await self.fetch_page(company.leadership_page_url)

        # 2. Extract people using LLM
        people = await self.extract_people(html)

        # 3. Resolve to existing records
        resolved = await self.resolve_people(people, company)

        # 4. Detect changes
        changes = await self.detect_changes(resolved, company)

        return CollectionResult(
            people_found=len(people),
            people_created=len([p for p in resolved if p.is_new]),
            changes_detected=len(changes),
        )
```

### SEC Parser

```python
# app/sources/people_collection/sec_parser.py

class SECParser(BaseCollector):
    """Parses SEC filings for executive data."""

    async def collect(self, company: IndustrialCompany) -> CollectionResult:
        if not company.cik:
            return CollectionResult(error="No CIK for company")

        # 1. Fetch recent filings
        filings = await self.fetch_filings(company.cik, ["DEF 14A", "10-K"])

        # 2. Parse each filing
        for filing in filings:
            if filing.form_type == "DEF 14A":
                people = await self.parse_proxy(filing)
            elif filing.form_type == "10-K":
                people = await self.parse_10k(filing)

        # 3. Resolve and detect changes
        ...
```

---

## Email Inference

The platform includes an email inference system to guess corporate email addresses.

### Email Patterns

```python
# app/sources/people_collection/email_inferrer.py

class EmailPattern(Enum):
    FIRST_LAST = "first.last"        # john.smith@company.com
    FIRST_L = "first.l"              # john.s@company.com
    F_LAST = "f.last"                # j.smith@company.com
    FIRST = "first"                  # john@company.com
    FLAST = "flast"                  # jsmith@company.com
```

### Known Patterns

Pre-configured patterns for common companies:

```python
KNOWN_PATTERNS = {
    "fastenal.com": EmailPattern.FIRST_LAST,
    "grainger.com": EmailPattern.FIRST_LAST,
    "mscdirect.com": EmailPattern.FIRST_LAST,
    # ...
}
```

### Usage

```python
from app.sources.people_collection.email_inferrer import EmailInferrer

inferrer = EmailInferrer()

# Infer email for a person
candidates = inferrer.infer_email(
    first_name="John",
    last_name="Smith",
    company_domain="acme.com"
)

# Returns list of InferredEmail with confidence scores
# [
#   InferredEmail(email="john.smith@acme.com", confidence="medium"),
#   InferredEmail(email="jsmith@acme.com", confidence="low"),
#   ...
# ]
```

### Learning Patterns

The system can learn patterns from known emails:

```python
from app.sources.people_collection.email_inferrer import CompanyEmailPatternLearner

learner = CompanyEmailPatternLearner()

# Learn from known emails
known_emails = [
    ("john.smith@acme.com", "John", "Smith"),
    ("jane.doe@acme.com", "Jane", "Doe"),
]
pattern = learner.learn_from_known_emails(known_emails)
# Returns EmailPattern.FIRST_LAST
```

---

## Change Detection

### How Changes Are Detected

```python
# app/jobs/change_monitor.py

class ChangeDetector:
    """Detects leadership changes by comparing snapshots."""

    def detect_changes(
        self,
        company: IndustrialCompany,
        new_people: List[ExtractedPerson],
    ) -> List[LeadershipChange]:
        # Get current leadership
        current = self.get_current_leadership(company)

        changes = []

        # Detect new hires (in new, not in current)
        for person in new_people:
            if not self.find_match(person, current):
                changes.append(LeadershipChange(
                    company_id=company.id,
                    person_name=person.name,
                    change_type="hire",
                    new_title=person.title,
                ))

        # Detect departures (in current, not in new)
        for cp in current:
            if not self.find_match(cp, new_people):
                changes.append(LeadershipChange(
                    company_id=company.id,
                    person_name=cp.person.full_name,
                    change_type="departure",
                    old_title=cp.title,
                ))

        # Detect title changes
        for person in new_people:
            match = self.find_match(person, current)
            if match and match.title != person.title:
                changes.append(LeadershipChange(
                    company_id=company.id,
                    person_name=person.name,
                    change_type="promotion" if self.is_promotion(match.title, person.title) else "lateral",
                    old_title=match.title,
                    new_title=person.title,
                ))

        return changes
```

### Significance Scoring

Changes are scored 1-10 based on:

| Factor | Score Impact |
|--------|--------------|
| C-suite change | +5 |
| Board change | +3 |
| CEO/CFO/COO | +2 |
| Departure | +1 |
| Large company | +1 |

---

## Data Quality

### Quality Score Components

Each person record has a quality score (0-100):

| Component | Max Points | Criteria |
|-----------|------------|----------|
| Identity | 20 | Name, LinkedIn URL |
| Contact | 20 | Email, phone, photo |
| Professional | 20 | Bio, current role |
| History | 20 | Experience, education |
| Freshness | 20 | Recently verified |

### Enrichment Queue

```python
# Get people needing enrichment
from app.services.data_quality_service import DataQualityService

dqs = DataQualityService(db)
queue = dqs.get_enrichment_queue(
    enrichment_type="linkedin",  # or "email", "photo", "bio"
    limit=100
)

# Queue prioritizes:
# 1. People with current positions at companies
# 2. People with partial data (easier to enrich)
```

### Duplicate Detection

```python
# Find potential duplicates
duplicates = dqs.find_potential_duplicates()

# Duplicates are detected by:
# 1. Same LinkedIn URL
# 2. Exact name match

# Merge duplicates
dqs.merge_duplicates(
    canonical_id=1,
    duplicate_ids=[5, 8]
)
# Transfers data and updates references
```

---

## Scheduling

### Job Types

| Job Type | Frequency | Description |
|----------|-----------|-------------|
| `website_crawl` | Weekly | Crawl company leadership pages |
| `sec_parse` | Daily | Parse new SEC filings |
| `news_scan` | Daily | Scan company newsrooms |
| `sec_8k_check` | Daily | Check for 8-K Item 5.02 filings |

### Scheduler Functions

```python
# app/jobs/people_collection_scheduler.py

from app.jobs.people_collection_scheduler import (
    schedule_website_refresh,
    schedule_sec_check,
    schedule_news_scan,
)

# Schedule weekly website refresh (prioritize portfolio companies)
job_id = schedule_website_refresh(db, limit=50)

# Schedule daily SEC check (public companies only)
job_id = schedule_sec_check(db, limit=30)

# Schedule daily news scan
job_id = schedule_news_scan(db, limit=50)
```

### Manual Scheduling

```python
from app.jobs.people_collection_scheduler import PeopleCollectionScheduler

scheduler = PeopleCollectionScheduler(db)

# Get companies needing refresh
companies = scheduler.get_companies_for_refresh(
    job_type="website_crawl",
    limit=50,
    priority="portfolio",  # or "public", "all"
)

# Create batch job
job = scheduler.create_batch_job(
    job_type="website_crawl",
    company_ids=[c.id for c in companies],
    config={"source": "manual"},
)

# Mark job running
scheduler.mark_job_running(job.id)

# ... run collection ...

# Mark job complete
scheduler.mark_job_complete(
    job_id=job.id,
    people_found=25,
    people_created=5,
    changes_detected=3,
)
```

### Cleanup Stuck Jobs

```python
# Jobs running > 4 hours are considered stuck
scheduler.cleanup_stuck_jobs(max_age_hours=4)
```

---

## Configuration

### Environment Variables

```bash
# LLM Configuration
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
LLM_PROVIDER=anthropic  # or openai

# Rate Limiting
WEBSITE_REQUESTS_PER_MINUTE=30
SEC_REQUESTS_PER_SECOND=10

# Collection Settings
MAX_COLLECTION_CONCURRENCY=5
COLLECTION_TIMEOUT_SECONDS=60
```

### Rate Limiting

```python
# app/sources/people_collection/rate_limiter.py

class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, requests_per_minute: int = 30):
        self.rate = requests_per_minute / 60.0
        self.tokens = requests_per_minute
        self.last_update = time.time()

    async def acquire(self):
        """Wait until a request can be made."""
        while self.tokens < 1:
            await asyncio.sleep(0.1)
            self._refill()
        self.tokens -= 1
```

---

## Monitoring

### Job Statistics

```python
from app.jobs.people_collection_scheduler import PeopleCollectionScheduler

scheduler = PeopleCollectionScheduler(db)
stats = scheduler.get_job_stats(days=7)

print(f"Total Jobs: {stats['total_jobs']}")
print(f"Success Rate: {stats['success_rate']}%")
print(f"People Found: {stats['total_people_found']}")
print(f"People Created: {stats['total_people_created']}")
print(f"Changes Detected: {stats['total_changes_detected']}")
```

### Data Quality Metrics

```python
from app.services.data_quality_service import DataQualityService

dqs = DataQualityService(db)
stats = dqs.get_overall_stats()

print(f"Total People: {stats['total_people']}")
print(f"LinkedIn Coverage: {stats['coverage']['linkedin']}%")
print(f"Email Coverage: {stats['coverage']['email']}%")
print(f"Avg Confidence: {stats['avg_confidence_score']}")
```

---

## Troubleshooting

### Common Issues

**Website crawl returns no people**
- Check if leadership page URL is correct
- Verify the page doesn't require JavaScript rendering
- Check if the site blocks bots (respect robots.txt)

**Low email confidence**
- Collect more known emails to learn patterns
- Use the email inferrer with verified patterns

**Duplicate person records**
- Run duplicate detection regularly
- Add LinkedIn URLs to improve matching

**High job failure rate**
- Check rate limiting configuration
- Verify API keys are valid
- Review error logs for specific failures

### Logging

```python
import logging

logging.getLogger("app.sources.people_collection").setLevel(logging.DEBUG)
```
