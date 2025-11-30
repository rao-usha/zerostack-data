# External Data Ingestion Service - Development Rules

This document outlines the architectural principles and rules for developing this service.

## Core Principles

### 1. Multi-Source Architecture

This is a **multi-source ingestion service**. The architecture must support multiple data providers:

- **Current source:** `census` (ACS 5-year)
- **Future sources:** `bls`, `bea`, `fred`, `sec`, etc. (added only when explicitly requested)

**Key Rule:** The core service (`main.py`, `core/*`) must remain **source-agnostic**. All source-specific logic lives in `sources/{source_name}/`.

### 2. Safety & Compliance

#### Data Licensing
- Only ingest data that is:
  - Public domain
  - Openly licensed for reuse
  - Explicitly confirmed as licensed for our use
- **Never** scrape arbitrary websites
- **Never** access content behind paywalls or requiring login
- Only use official, documented APIs

#### PII Protection (CRITICAL)
**NEVER:**
- Collect, store, or infer PII beyond what the source explicitly provides
- Attempt to de-anonymize any dataset
- Join datasets in ways that increase re-identification risk

#### When in Doubt
- Treat questionable datasets as **RESTRICTED**
- **DO NOT** ingest them
- Leave a clear comment or TODO instead

### 3. Network & Rate Limits

#### Bounded Concurrency (MANDATORY)
- **NEVER** design code with unbounded parallel requests
- **ALWAYS** use bounded concurrency: semaphores, worker pools, limited async tasks
- Use `asyncio.Semaphore` or similar mechanisms
- Configure via `MAX_CONCURRENCY` environment variable

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
- Schema changes must be:
  - Explicit and deterministic
  - Idempotent (safe to run multiple times)

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

### 6. Extensibility & Plugin Pattern

#### Source Module Structure
Each data source **MUST**:
- Live in its own module: `app/sources/{source_name}/`
- Implement a clear adapter interface
- Handle its own API URLs, authentication, schema mapping

#### Directory Structure
```
app/
├── main.py              # FastAPI app, source-agnostic
├── core/                # Core logic, no source-specific code
│   ├── config.py
│   ├── database.py
│   ├── models.py
│   ├── schemas.py
│   └── ingestion.py
├── sources/             # Source adapters
│   ├── census/          # Census adapter
│   │   ├── adapter.py
│   │   ├── api.py
│   │   └── schemas.py
│   └── [future sources]
└── api/                 # API routes
    └── v1/
        └── jobs.py
```

### 7. Testing Requirements

#### Unit Tests
- Must run WITHOUT API keys or network access
- Must pass in clean environment
- Use fixtures and mocks for external dependencies
- Mark with `@pytest.mark.unit`

#### Integration Tests
- Only run when `RUN_INTEGRATION_TESTS=true`
- Require valid API keys
- Make real API calls to verify functionality
- Mark with `@pytest.mark.integration`

### 8. Configuration Management

#### Startup vs. Operation
- **App startup** should NOT require data source API keys
- **Ingestion operations** MUST validate API keys before proceeding
- Fail early with clear error messages

#### Environment Variables
Required:
- `DATABASE_URL` - PostgreSQL connection URL

Optional (with defaults):
- `CENSUS_SURVEY_API_KEY` - Required only for Census operations
- `MAX_CONCURRENCY` - Default: 4
- `MAX_REQUESTS_PER_SECOND` - Default: 5.0
- `LOG_LEVEL` - Default: INFO
- `RUN_INTEGRATION_TESTS` - Default: false

## Common Pitfalls to Avoid

1. ❌ Unbounded concurrency → ✅ Use semaphores/rate limiters
2. ❌ Missing error handling → ✅ Implement retry with backoff
3. ❌ SQL string concatenation → ✅ Use parameterized queries
4. ❌ Missing job tracking → ✅ Always update job status
5. ❌ JSON storage for data → ✅ Use typed columns
6. ❌ Adding sources proactively → ✅ Wait for explicit user request
7. ❌ Hardcoding source logic in core → ✅ Use plugin pattern
8. ❌ Scraping websites → ✅ Use official APIs only

## Priority Matrix

**P0 - Critical (Never Violate):**
- Data safety and licensing compliance
- PII protection
- SQL injection prevention
- Bounded concurrency
- Job tracking for all ingestion runs

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

## Adding New Data Sources

To add a new source:

1. Create module under `app/sources/{source_name}/`
2. Implement client with HTTP logic
3. Implement metadata parsing
4. Implement ingestion orchestration
5. Register in API routes
6. Add tests (unit + integration)
7. Document in README

**Remember:** Only add sources when explicitly requested by the user.





