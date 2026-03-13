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

## Worker Operations

The job queue uses a separate **worker container** (`python -m app.worker.main`) that claims and executes jobs from PostgreSQL.

### Starting Workers

```bash
# Start worker alongside other services
docker-compose up -d worker

# Or start everything (api + worker + postgres + frontend)
docker-compose up -d

# Note: deploy.replicas: 6 in docker-compose.yml is Swarm-only.
# Standard compose starts 1 worker instance. For more:
docker-compose up -d --scale worker=4
```

### Monitoring Workers

```bash
# Check if worker containers are running
docker-compose ps worker

# View worker logs
docker-compose logs worker --tail 50
docker-compose logs worker -f  # follow

# API: worker status (active workers, queue depth, last claimed)
curl -s http://localhost:8001/api/v1/jobs/workers | python -m json.tool

# API: health check (includes worker availability)
curl -s http://localhost:8001/health | python -m json.tool
```

### Stopping Workers

```bash
# Graceful stop (30s drain timeout)
docker-compose stop worker

# Restart after code changes
docker-compose restart worker
```

### Job Queue Behavior

- **WORKER_MODE=1** (default in docker-compose): Jobs go to PostgreSQL queue, claimed by workers via `SELECT FOR UPDATE SKIP LOCKED`
- **WORKER_MODE=0** (fallback): Jobs run in-process via FastAPI `BackgroundTasks` — no separate worker needed
- **Without workers running**: Jobs stay PENDING indefinitely. Auto-cancel kicks in after 4 hours (`cancel_stale_pending_jobs`)
- **Stale heartbeat recovery**: Jobs from dead workers reset to PENDING every 5 minutes (`reset_stale_jobs`)
- **Rate limiting**: Per-source rate limits enforced globally across all workers via `rate_limit_bucket` table

### Common Issues

- **Jobs stuck "pending"**: Worker container not running. Start with `docker-compose up -d worker`
- **Batch stuck**: Same cause — tier 1 jobs can't execute without workers
- **Worker won't start**: Check `docker-compose logs worker` for import errors. Worker shares the same codebase/image as API

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

## Session Logging (MANDATORY — zero exceptions)

Every session must maintain a daily work log. This is NOT optional. If you skip logging, context is lost and the user has to re-explain everything next session.

**Log location:** `C:\Users\awron\.claude\projects\C--Users-awron-projects-Nexdata\memory\logs\YYYY-MM-DD.md`

### Automatic behavior — YOU MUST DO ALL THREE:

1. **Session start (FIRST THING):** Read today's log file (if it exists) and the last 2 days' logs. Use these to understand recent context and pick up where the previous session left off. Briefly acknowledge what you found (e.g., "Picking up from yesterday — you were working on X").

2. **After EVERY completed task (not just "significant" ones):** Append a timestamped entry to today's log. This means after every code change, bug fix, feature addition, investigation, collection run, report generation, commit, or architectural decision. **If you changed a file or ran a command that produced results, log it.** Do not batch multiple tasks into one entry — log each one as you finish it.

3. **Before session ends:** Write a final checkpoint entry summarizing the full session and explicitly noting what to do next. If the user says goodbye, thanks, or the conversation naturally concludes — log it.

**Self-check:** If you've done work but haven't logged in the last 2-3 messages, you missed a log entry. Go write it now.

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

## Workflow (MANDATORY — follow every time)

### Step 0: Create a Spec First

**Before editing ANY source code in `app/`, you MUST have an active spec.**

- Run `/spec <task_type> <feature_name>` to create a spec document + skeleton test file
- Task types: `collector`, `api_endpoint`, `bug_fix`, `report`, `service`, `model`
- The `/spec` skill writes to `docs/specs/.active_spec` — without this, the `check-spec-exists` hook will **BLOCK** your edits
- Fill in test implementations BEFORE writing source code (spec-first / TDD)
- For genuinely trivial fixes (typo, single-line change): write `BYPASS_TRIVIAL` to `docs/specs/.active_spec`
- After task completion, run `/feedback` if the user provided any corrections

### Step 1: ALWAYS Plan First

**Every task that touches more than 1 file or takes more than a single obvious change MUST start with a plan.**

- Use `EnterPlanMode` to explore the codebase and design your approach
- Write the plan to `docs/plans/PLAN_XXX_<name>.md` (find the next number)
- Wait for explicit user approval before writing any code
- If the user says "just do it" or the task is genuinely trivial (typo fix, single-line change), you may skip planning — but still create a task checklist

### Step 2: ALWAYS Create a Task Checklist

**Every task with 2+ steps MUST use `TaskCreate` to create a visible checklist BEFORE starting work.**

- Break work into concrete, completable steps (e.g., "Add model columns", "Create service class", "Add API endpoint", "Write tests", "Restart and verify")
- Mark each task `in_progress` BEFORE starting it and `completed` AFTER finishing it
- The user should be able to see progress at any time via the task list
- If work is interrupted, the task list shows exactly where to resume

### Step 3: Execute, Test, Verify

- Build: `docker-compose up --build -d`, or `docker-compose restart api` for code-only changes
- Test: `pytest tests/ -v`, curl endpoints, check docker logs
- Verify: Confirm the feature works end-to-end before marking complete

### Step 4: Commit

- Conventional Commits format: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`
- Only commit when user explicitly asks

### Parallel Work

If working alongside another Claude instance, read `PARALLEL_WORK.md` first. Only touch assigned files. Tab 1 handles main.py integration and commits.

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
