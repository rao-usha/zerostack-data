# PLAN 020 — Orchestration Bug Fixes (Phase 1)

**Goal:** Fix the real bugs found during deep orchestration review so incremental + batch jobs work reliably.

**Scope:** 4 targeted fixes across 4 files. No architecture changes, no new features.

---

## Fix 1: Duplicate scheduled job guard (`scheduler_service.py`)

**Bug:** `run_scheduled_job()` creates a new job every time APScheduler fires, even if the previous job for the same schedule is still PENDING or RUNNING. This causes duplicate data ingestion for the same window.

**Fix:** Before creating the job (line 185), query for existing active jobs:

```python
# Skip if a job for this schedule is already running
active = (
    db.query(IngestionJob)
    .filter(
        IngestionJob.schedule_id == schedule.id,
        IngestionJob.status.in_([JobStatus.PENDING, JobStatus.RUNNING]),
    )
    .first()
)
if active:
    logger.info(
        f"Schedule {schedule.name} skipped — job {active.id} still {active.status.value}"
    )
    return
```

**File:** `app/core/scheduler_service.py` — insert before line 185

---

## Fix 2: Call `_handle_job_completion()` after cancellation & pre-flight failure (`jobs.py`)

**Bug:** Three code paths mark a job as FAILED but skip `_handle_job_completion()`:
1. **Job cancellation** (`cancel_job`, line 1545) — dependent jobs hang, batch webhook never fires
2. **API key pre-flight failure** (`run_ingestion_job`, line 1044) — same effects
3. **Batch cancellation** calls `cancel_batch_run()` in nightly_batch_service.py which marks jobs FAILED but doesn't call completion handler — however this one already fires the batch webhook via the `check_and_notify_batch_completion` path, and calling `_handle_job_completion` for N jobs in a loop would be expensive. So we only need to fix the single-job cases.

**Fix A — Job cancel endpoint** (line 1545):
After `db.commit()` and `db.refresh(job)`, add:
```python
await _handle_job_completion(db, job)
```

**Fix B — Pre-flight failure** (line 1043-1044):
After `db.commit()`, add:
```python
await _handle_job_completion(db, job)
```

**File:** `app/api/v1/jobs.py`

---

## Fix 3: Fail job on watermark injection error (`executors/ingestion.py`)

**Bug:** If `inject_incremental_from_watermark()` throws, the exception is caught and logged as a warning (line 38-39). The job then proceeds with the original non-incremental config, causing a full reload that creates duplicate data.

**Fix:** Re-raise the exception so the job fails and gets retried:

```python
try:
    from app.core.watermark_service import inject_incremental_from_watermark
    config = inject_incremental_from_watermark(config, source, db)
except Exception as e:
    logger.error(f"Failed to inject incremental params for {source}: {e}")
    raise RuntimeError(
        f"Watermark injection failed for {source}: {e}"
    ) from e
```

This is retryable — the retry service will schedule a new attempt.

**File:** `app/worker/executors/ingestion.py` — replace lines 34-39

---

## Fix 4: Compute `wait_for_tiers` from actual effective tiers (`nightly_batch_service.py`)

**Bug:** Line 316 uses `list(range(1, tier.level))` which assumes tiers are consecutive integers. If a tier is disabled or removed, the worker waits for a nonexistent tier forever.

**Fix:** Compute from the actual `effective_tiers` list:

```python
# Each tier waits for all lower effective tiers (not hardcoded range)
lower_tiers = [t.level for t in effective_tiers if t.level < tier.level]
if lower_tiers:
    payload["wait_for_tiers"] = lower_tiers
```

Replace the existing lines 314-316 in `launch_batch_collection`.

**File:** `app/core/nightly_batch_service.py`

---

## Test Plan

- **Fix 1:** New test `test_scheduled_job_skips_if_active` — verify no new job created when one is PENDING/RUNNING
- **Fix 2:** Extend `test_batch_cancel.py` — verify `_handle_job_completion` is called after single job cancel; verify pre-flight failure calls completion handler
- **Fix 3:** New test `test_watermark_injection_failure_fails_job` — verify RuntimeError raised when watermark injection throws
- **Fix 4:** Extend `test_p0_scheduling_fixes.py` — verify `wait_for_tiers` uses actual tiers when tier 2 is disabled

---

## Files Changed

| File | Change |
|------|--------|
| `app/core/scheduler_service.py` | Add active job guard in `run_scheduled_job()` |
| `app/api/v1/jobs.py` | Add `_handle_job_completion()` call in cancel endpoint + pre-flight |
| `app/worker/executors/ingestion.py` | Raise on watermark injection failure |
| `app/core/nightly_batch_service.py` | Compute `wait_for_tiers` from effective tiers |
| `tests/test_p0_scheduling_fixes.py` | New/extended tests |
