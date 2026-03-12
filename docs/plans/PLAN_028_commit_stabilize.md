# PLAN 028: Commit & Stabilize

## Goal
Commit all outstanding work from March 11 into logical, well-organized commits. Run full test suite and health checks to confirm stability after system restart.

## Context
~30 modified files, 2 deleted files, 9 new untracked files — representing 2 days of work across: SQL injection fixes, DQ recommendation framework, orchestration cleanup, frontend enhancements, batch ingestion fixes, and config audit.

## Steps

### 1. Pre-commit audit
- [ ] Run `git diff --stat` to inventory all changes
- [ ] Run full test suite: `pytest tests/ -v --ignore=tests/integration/`
- [ ] Fix any test failures before committing

### 2. Commit 1: SQL injection hardening
**Files:**
- `app/core/safe_sql.py` (NEW)
- `app/core/data_quality_service.py`
- `app/core/cross_source_validation_service.py`
- `app/core/data_quality.py`
- `app/core/batch_operations.py`
- `app/api/v1/jobs.py`
- `app/reports/templates/datacenter_site.py`
- `app/sources/vertical_discovery/enrichment.py`
- `app/sources/vertical_discovery/ownership_classifier.py`
- `app/graphql/resolvers.py`
- `app/agents/news_monitor.py`

**Message:** `fix: harden 68+ SQL patterns with safe_identifier() across 10 files`

### 3. Commit 2: DQ recommendation framework
**Files:**
- `app/core/models.py` (DQRecommendation model)
- `app/core/dq_recommendation_engine.py` (NEW)
- `app/core/dq_post_ingestion_hook.py` (NEW)
- `app/api/v1/dq_review.py` (NEW)
- `app/core/ingest_base.py` (post-ingestion hook)
- `tests/test_dq_recommendation_engine.py` (NEW)

**Message:** `feat: add DQ recommendation engine with post-ingestion hooks and review API`

### 4. Commit 3: Orchestration cleanup + job dashboard
**Files:**
- `app/agents/orchestrator.py` (DELETED)
- `app/api/v1/workflows.py` (DELETED)
- `app/agents/__init__.py`
- `app/api/v1/job_stream.py`
- `app/main.py`

**Message:** `refactor: remove unused workflow engine, enhance job dashboard with domain detail and filters`

### 5. Commit 4: Batch ingestion fixes
**Files:**
- `app/sources/eia/ingest.py`
- `app/sources/bea/ingest.py`
- `app/sources/realestate/client.py`
- `app/sources/sec/formadv_ingest.py`
- `app/sources/nppes/client.py`
- `app/sources/epa_echo/metadata.py`
- `app/core/nightly_batch_service.py`
- `app/core/job_splitter.py`
- `app/core/config.py`
- `app/users/auth.py`

**Message:** `fix: fix 6 broken batch sources (EIA, BEA, Census, Realestate, SEC FormADV, split dispatch)`

### 6. Commit 5: Frontend enhancements
**Files:**
- `frontend/index.html`
- `app/core/watermark_service.py`
- `app/api/v1/site_intel_sites.py`

**Message:** `feat: add Reports tab, D3 visualizations, job dashboard filters, and URL routing to frontend`

### 7. Commit 6: MedSpa opportunity map + report builder
**Files:**
- `app/reports/templates/medspa_opportunity_map.py` (NEW)
- `app/reports/design_system.py`
- `app/reports/builder.py`

**Message:** `feat: add interactive MedSpa opportunity map report with Leaflet`

### 8. Post-commit verification
- [ ] Run test suite again to confirm nothing broke
- [ ] `docker-compose restart api` and wait for health
- [ ] Curl `/health` and `/api/v1/job-queue/summary` to verify API is up
- [ ] Check docker logs for startup errors

## Risks
- Some files appear in multiple logical groups — need careful staging
- Tests may fail if Docker/DB isn't running (unit tests should be fine)

## Parallel work note
This tab only does git operations. No file edits except test fixes.
