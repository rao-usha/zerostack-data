# Nexdata - Development Rules

This document outlines the architectural principles and rules for developing the Nexdata External Data Ingestion Service.

## Service Configuration

**CRITICAL: Always verify ports from `docker-compose.yml` before connecting.**

| Service | Internal Port | Public/Host Port | Container Name |
|---------|---------------|------------------|----------------|
| API | 8000 | **8001** | `nexdata-api-1` |
| PostgreSQL | 5432 | **5433** | `nexdata-postgres-1` |

- **Documentation/curl commands:** Use public ports (8001, 5433)
- **Code inside containers:** Use internal ports (8000, 5432)

---

## Core Principles

### 1. Multi-Source Architecture

This is a **multi-source ingestion service** supporting 25+ data providers:

**Approved Sources:**
- Government/Economic: BEA, BLS, BTS, Census, EIA, FDIC, FEMA, FRED, IRS SOI, NOAA, Treasury, US Trade, USDA
- Financial/Regulatory: CFTC COT, CMS, SEC (EDGAR, Form ADV, 13F, XBRL)
- Other: Data Commons, FBI Crime, FCC Broadband, Kaggle, Real Estate, Yelp
- Research: Public LP Strategies, Family Office tracking, Portfolio research

**Key Rule:** The core service (`main.py`, `core/*`) must remain **source-agnostic**. All source-specific logic lives in `app/sources/{source_name}/`.

**Adding NEW sources:** Requires explicit user request and follows the source module checklist.

### 2. Safety & Compliance

#### Data Licensing
Only ingest data that is:
- Public domain
- Openly licensed for reuse
- Explicitly confirmed as licensed for our use

#### Data Collection Methods

**Permitted (Analyst-Equivalent Research):**
- ✅ Official, documented APIs (preferred)
- ✅ Structured data extraction from public websites (with safeguards)
- ✅ Parsing public contact pages, directories, "About Us" pages
- ✅ Extracting publicly disclosed information from official sources
- ✅ Downloading bulk data files (CSV, Excel, PDF parsing)
- ✅ SEC/regulatory filings
- ✅ News articles and press releases (public, professional context)

**Prohibited:**
- ❌ Accessing content behind paywalls or requiring login
- ❌ Aggressive/abusive scraping (ignoring robots.txt, rate limits)
- ❌ Collecting personal information not publicly disclosed
- ❌ Circumventing access controls or authentication
- ❌ Scraping social media profiles (violates ToS)
- ❌ Personal emails (gmail, yahoo) for business contacts

**Required Safeguards:**
- Respect robots.txt
- Conservative rate limiting (1-2 req/sec per domain)
- Proper User-Agent identification
- Exponential backoff on errors
- Respect "do not contact" or removal requests

#### PII Protection (CRITICAL)
**NEVER:**
- Collect, store, or infer PII beyond what the source explicitly provides
- Attempt to de-anonymize any dataset
- Join datasets in ways that increase re-identification risk

### 3. Network & Rate Limits

#### Bounded Concurrency (MANDATORY)
- **NEVER** design code with unbounded parallel requests
- **ALWAYS** use bounded concurrency: semaphores, worker pools, limited async tasks
- Use `asyncio.Semaphore` or similar mechanisms
- Configure via `MAX_CONCURRENCY` environment variable (default: 4)

#### Rate Limit Compliance
- **ALWAYS** obey documented rate limits for each source
- Default to **conservative values**
- Implement exponential backoff with jitter
- Respect `Retry-After` headers when available

### 4. Database & Schema Rules

#### Write Control
- All writes **MUST** go through well-defined ingestion functions
- **NO** arbitrary ad-hoc DDL/DML outside migrations or ingestion steps

#### Schema Management
- Use **typed columns** (INT, NUMERIC, TEXT, etc.)
- **NEVER** use raw JSON blobs as the final data form
- Schema changes must be explicit, deterministic, and idempotent

#### Destructive Operations
- **NEVER** automatically drop tables, truncate tables, or delete large amounts of data
- Destructive operations require **explicit user intent** and clear documentation

#### SQL Safety
- **ALWAYS** parameterize queries using `:param` style
- **NEVER** build SQL by string concatenation with untrusted input

### 5. Job Control & Observability

#### Job Tracking (MANDATORY)
- Every ingestion run **MUST** have a corresponding `ingestion_jobs` record
- **NEVER** run ingestion "fire and forget"

#### Job States
Use **ONLY** these states:
- `pending` - Job created, not yet started
- `running` - Job currently executing
- `success` - Job completed successfully
- `failed` - Job failed with error

#### Job Completion
- Record row counts where practical
- Record error messages in structured way when jobs fail
- Track start and completion timestamps

### 6. Agentic Research Rules

#### LLM Usage
- Approved providers: OpenAI, Anthropic
- Log all LLM API costs per job
- Soft limits: warn at $1/job

#### Confidence Scoring
- `high`: Multiple sources agree
- `medium`: Single reliable source (SEC, official website)
- `low`: Needs human review (news, LLM extraction)

#### Human Review
- LLM-extracted contacts require human verification before marking as verified
- Statistical data can be auto-accepted with source attribution

### 7. API Design Standards

#### Standard Response Format
```python
{
    "data": [...],
    "meta": {"count": N, "source": "...", "job_id": "..."},
    "errors": []
}
```

#### Error Handling
- Standard HTTP codes: 400, 404, 429, 500
- Structured error messages with error_code and message

### 8. Data Quality Standards

#### Validation (Source-Dependent)
- **Contacts:** Strict - reject invalid emails, validate phone formats, reject personal emails
- **Statistical data:** Flexible - accept with warnings/flags

#### Duplicate Detection
- Check existing records before inserting
- Use composite keys for deduplication

---

## Source Module Structure

### Directory Layout
```
app/sources/{source_name}/
├── __init__.py      # Exports
├── client.py        # HTTP/API client logic
├── ingest.py        # Ingestion orchestration
├── metadata.py      # Dataset schemas, field definitions
```

### Adding a New Source - Checklist
1. [ ] Create module under `app/sources/{source_name}/`
2. [ ] Implement `client.py` with HTTP logic and rate limiting
3. [ ] Implement `metadata.py` with dataset schemas
4. [ ] Implement `ingest.py` with orchestration and job tracking
5. [ ] Create API routes in `app/api/v1/{source_name}.py`
6. [ ] Register router in `app/main.py`
7. [ ] Add unit tests (mocked, no API calls)
8. [ ] Add integration tests (real API, `RUN_INTEGRATION_TESTS=true`)
9. [ ] Create `docs/{SOURCE}_QUICK_START.md`

---

## Testing Requirements

### Unit Tests
- Must run WITHOUT API keys or network access
- Use fixtures and mocks for external dependencies
- Mark with `@pytest.mark.unit`

### Integration Tests
- Only run when `RUN_INTEGRATION_TESTS=true`
- Require valid API keys
- Mark with `@pytest.mark.integration`

---

## Configuration

### Environment Variables
**Required:**
- `DATABASE_URL` - PostgreSQL connection URL

**Optional (with defaults):**
- `MAX_CONCURRENCY` - Default: 4
- `MAX_REQUESTS_PER_SECOND` - Default: 5.0
- `LOG_LEVEL` - Default: INFO
- `RUN_INTEGRATION_TESTS` - Default: false

**Source-specific API keys:** Required only when using that source.

---

## Priority Matrix

**P0 - Critical (Never Violate):**
- Data safety and licensing compliance
- PII protection
- SQL injection prevention
- Bounded concurrency
- Job tracking for all ingestion runs
- Service configuration verification (check ports!)

**P1 - High Priority:**
- Rate limit compliance
- Deterministic behavior
- Plugin pattern adherence
- Typed database schemas

**P2 - Important:**
- Error handling with retries
- Idempotent operations
- Clear documentation
- Performance optimization

---

## Common Pitfalls to Avoid

1. ❌ Unbounded concurrency → ✅ Use semaphores/rate limiters
2. ❌ Missing error handling → ✅ Implement retry with backoff
3. ❌ SQL string concatenation → ✅ Use parameterized queries
4. ❌ Missing job tracking → ✅ Always update job status
5. ❌ JSON storage for data → ✅ Use typed columns
6. ❌ Hardcoding source logic in core → ✅ Use plugin pattern
7. ❌ Assuming port 8000 → ✅ Check docker-compose.yml (API is 8001)
8. ❌ Personal emails for contacts → ✅ Require business emails

---

## Quick Reference

- **API Swagger UI:** http://localhost:8001/docs
- **API ReDoc:** http://localhost:8001/redoc
- **Database:** `postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata`
- **Start services:** `docker-compose up -d`
- **View logs:** `docker-compose logs -f api`
