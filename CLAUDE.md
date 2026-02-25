# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Nexdata is a FastAPI service that ingests data from 28+ public APIs (Census, FRED, SEC, EIA, BLS, etc.) into PostgreSQL. It includes agentic AI research (portfolio discovery, people/org chart collection, site intelligence), job scheduling, data quality validation, and a GraphQL layer.

**Stack:** Python 3.11, FastAPI, SQLAlchemy 2.0, PostgreSQL 14, httpx, APScheduler, OpenAI/Anthropic LLMs, Docker

---

## Build & Run Commands

```bash
# Start all services (API on :8001, Postgres on :5434)
docker-compose up --build -d

# Restart API after code changes (volume-mounted but needs process restart)
docker-compose restart api
# Wait ~20-30s for startup — the app has many imports

# Clean rebuild when Docker cache is stale
docker-compose down && docker-compose build --no-cache api && docker-compose up -d

# View logs
docker-compose logs api --tail 50
docker-compose logs api -f  # follow

# Test an endpoint
curl -s http://localhost:8001/api/v1/<endpoint> | python -m json.tool

# Swagger UI
# http://localhost:8001/docs
```

### Testing

```bash
# Run all unit tests (excludes integration tests)
pytest tests/ -v --ignore=tests/integration/

# Run a single test file
pytest tests/test_llm_client.py -v

# Run with coverage
pytest tests/ --cov=app --cov-report=xml --ignore=tests/integration/

# Integration tests (require API keys + network)
RUN_INTEGRATION_TESTS=true pytest tests/integration/ -v
```

Tests use markers: `@pytest.mark.unit` (offline) and `@pytest.mark.integration` (requires API keys).

### Linting

```bash
ruff check app/
```

CI runs ruff as non-blocking (`continue-on-error: true`).

---

## Port Mappings (Critical)

| Service    | Container Port | Host Port | Notes                        |
|------------|---------------|-----------|------------------------------|
| API        | 8000          | **8001**  | All curl/docs use 8001       |
| PostgreSQL | 5432          | **5434**  | Code inside containers: 5432 |

Code in containers uses internal ports. External access (curl, psql from host) uses host ports.

---

## Architecture

### Core Abstractions

Every data source follows the same plugin pattern:

1. **`BaseAPIClient`** (`app/core/http_client.py`) — All HTTP clients inherit this. Provides bounded concurrency (`asyncio.Semaphore`), exponential backoff with jitter, rate limiting, retry logic, and connection pooling via `httpx.AsyncClient`.

2. **`BaseSourceIngestor`** (`app/core/ingest_base.py`) — All ingestors inherit this. Handles table creation, dataset registry updates, and job tracking.

3. **API Router** (`app/api/v1/<source>.py`) — Creates an ingestion job, kicks off background work via `BackgroundTasks`, returns `job_id` immediately.

4. **Router Registration** — All routers are registered in `app/main.py` under `/api/v1` prefix. New routers also need an OpenAPI tag in main.py.

### Adding a New Data Source

```
app/sources/<name>/
├── client.py      # Inherits BaseAPIClient
├── ingest.py      # Inherits BaseSourceIngestor
└── metadata.py    # Dataset schemas, field definitions
app/api/v1/<name>.py  # Endpoints
app/main.py            # Register router + OpenAPI tag
```

### Site Intelligence Collector Pattern

Site intel collectors use a decorator-based registry:

```python
from app.sources.site_intel.runner import register_collector

@register_collector(SiteIntelSource.POWER_PLANTS)
class PowerCollector(BaseCollector):
    domain = SiteIntelDomain.POWER
    source = SiteIntelSource.POWER_PLANTS
```

The decorator populates `COLLECTOR_REGISTRY` at import time. For collectors to be available, their module **must be imported** — each domain's `__init__.py` imports all collector modules. When calling collectors from API endpoints, ensure the import chain reaches the domain `__init__.py`:
```python
import app.sources.site_intel.logistics  # noqa: F401
```

### People Collection Pipeline

**Flow:** PageFinder → WebsiteAgent (structured HTML + LLM fallback) → Orchestrator → Storage

- **DB tables:** `people`, `company_people`, `industrial_companies`, `org_chart_snapshots`
- **Deep collection** (4 phases): SEC EDGAR 10-K → Website deep crawl → News scan → Org chart build
- **Test endpoint:** `POST /api/v1/people-jobs/test/{company_id}?sources=website`
- **Deep collect:** `POST /api/v1/people-jobs/deep-collect/{company_id}`

### Key Models Files

- `app/core/models.py` (102KB) — Core tables: IngestionJob, IngestionSchedule, DatasetRegistry, etc.
- `app/core/models_site_intel.py` (65KB) — Site Intelligence domain tables
- `app/core/people_models.py` (23KB) — People and org chart tables
- `app/core/pe_models.py` (31KB) — PE Intelligence tables

### Database Patterns

- **`bulk_upsert()`** overwrites ALL columns including nulls — dangerous for incremental enrichment
- **`null_preserving_upsert()`** uses `COALESCE(EXCLUDED.col, existing.col)` — safe for enrichment workflows
- Tables are created at startup via `Base.metadata.create_all()` (no migration tool)
- All batch INSERT records must have identical keys — use `rec.setdefault(key, None)` before upsert

### Error Hierarchy (`app/core/api_errors.py`)

```
APIError
├── RetryableError    (500s, timeouts — auto-retry)
├── RateLimitError    (429 — backoff)
├── FatalError        (400, 404 — don't retry)
├── AuthenticationError (401, 403)
└── NotFoundError     (404)
```

### Job Lifecycle

Every ingestion creates an `ingestion_jobs` record: `pending → running → success | failed`. Jobs support retry logic, parent-child chaining, and scheduling via APScheduler (configured in lifespan).

---

## Critical Rules

### Data Safety
- **Only** use publicly available data — never access paywalled/login-required content
- **Never** collect PII beyond what sources explicitly provide; never de-anonymize datasets
- **Always** parameterize SQL queries (`:param` style) — never string-concatenate untrusted input
- **Always** use `asyncio.Semaphore` for bounded concurrency (default `MAX_CONCURRENCY=4`)

### Web Scraping Safeguards
- Respect `robots.txt` strictly
- Rate limit: 1-2 req/sec per domain (0.5 req/sec for sensitive targets like family offices)
- User-Agent: `"NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"`
- Exponential backoff with jitter on errors; respect `Retry-After` headers
- Abort immediately if login required or paywall detected

### LLM Usage
- Track all LLM costs via `llm_cost_tracker.py`
- LLM-extracted data gets `confidence='llm_extracted'` until human-verified
- Confidence priority: SEC filings > Official websites > Annual reports > News

---

## Session Logging (MANDATORY)

Every session must maintain a daily work log. This is critical for continuity across sessions.

**Log location:** `C:\Users\awron\.claude\projects\C--Users-awron-projects-Nexdata\memory\logs\YYYY-MM-DD.md`

### Automatic behavior:

1. **Session start:** Read today's log file (if it exists) and the last 2 days' logs. Use these to understand recent context and pick up where the previous session left off. Briefly acknowledge what you found (e.g., "Picking up from yesterday — you were working on X").

2. **After each significant task:** Append a timestamped entry to today's log with: what was done, decisions made, files changed, and next steps. A "significant task" = any code change, bug fix, feature addition, investigation with findings, or architectural decision.

3. **Before session ends (if the user says goodbye, thanks, or conversation naturally concludes):** Write a final checkpoint entry summarizing the full session and explicitly noting what to do next.

The `/session-log` skill can also be invoked manually to force a checkpoint at any time.

### Log entry format:
```markdown
## HH:MM — [Brief title]

**What:** [1-3 bullets of work done]
**Decisions:** [Choices made and why]
**Files changed:** [Specific paths]
**Next steps:** [What to do next]
**Blockers:** [Issues, or "None"]

---
```

---

## Workflow

1. **Plan** — Record in `docs/plans/PLAN_XXX_<name>.md`, wait for explicit user approval
2. **Execute** — Use TaskCreate/TaskUpdate for multi-step work
3. **Test** — `docker-compose up --build -d`, curl endpoints, check logs
4. **Commit** — Conventional Commits format: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`

### Parallel Work

If working alongside another Claude instance, read `PARALLEL_WORK.md` first. Only touch assigned files. Tab 1 handles main.py integration and commits.

### Long-Running Tasks

Use TaskCreate to break work into phases. Mark tasks `in_progress` before starting, `completed` when done. Include checkpoint info (last completed, next action, blockers, resume instructions) so work can continue if interrupted.

---

## Environment Variables

**Required:** `DATABASE_URL`

**Source-specific (required only when using that source):** `FRED_API_KEY`, `EIA_API_KEY`, `BEA_API_KEY`, `BLS_API_KEY`, `CENSUS_SURVEY_API_KEY`, `NOAA_API_TOKEN`, `YELP_API_KEY`, `DATA_GOV_API`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `KAGGLE_USERNAME`, `KAGGLE_KEY`

**System:** `MAX_CONCURRENCY` (default 4), `MAX_REQUESTS_PER_SECOND` (default 5.0), `LOG_LEVEL` (default INFO), `ENABLE_PLAYWRIGHT` (default 0)

App startup does NOT require data source API keys — keys are validated at ingestion time.

---

## Known Issues

- DuckDuckGo search returns 0 results from Docker (likely IP-blocked)
- JS-rendered pages not supported unless `ENABLE_PLAYWRIGHT=1` (adds ~500MB to image)
- Docker build cache can be stale — use `--no-cache` when changes aren't picked up
- Container logs may appear empty initially; app takes time to initialize
