# PLAN 019 — P0 Scheduling & Collection Fixes

## Problem Statement

Two critical data integrity issues exist in the scheduling/collection system:

1. **Watermark advances before job completes** — `last_run_at` is set at job creation time, not completion. Failed jobs cause the next scheduled run to skip data.
2. **Partial batch commits with no continuation** — Batch inserts commit per-1000 rows. If batch 6/10 fails, 5000 rows are committed but the job is marked FAILED. Retries start from scratch, causing duplicates or gaps.

Both issues can silently corrupt data across all 14 incremental sources.

---

## Fix 1: Watermark Advancement Timing

### Current Behavior (scheduler_service.py:165-169)
```python
# In run_scheduled_job() — BEFORE execution
schedule.last_run_at = datetime.utcnow()   # ← Set immediately
schedule.last_job_id = job.id
schedule.next_run_at = _calculate_next_run(schedule)
db.commit()

# ... then job executes (may fail)
```

### Desired Behavior
- `last_run_at` only advances on **SUCCESS**
- `last_job_id` and `next_run_at` still update immediately (for visibility)
- On **FAILED**, `last_run_at` stays unchanged → next run retries same window

### Implementation

#### Step 1: Split schedule update in `scheduler_service.py`

In `run_scheduled_job()` (line ~165), change from:
```python
schedule.last_run_at = datetime.utcnow()
schedule.last_job_id = job.id
schedule.next_run_at = _calculate_next_run(schedule)
db.commit()
```

To:
```python
# Track the job + next run immediately (visibility), but DON'T advance watermark yet
schedule.last_job_id = job.id
schedule.next_run_at = _calculate_next_run(schedule)
db.commit()
```

#### Step 2: Advance watermark on success in `jobs.py`

Add a new helper function `_advance_schedule_watermark()` that:
1. Finds the `IngestionSchedule` that spawned this job (via `schedule_id` on the job, or matching `last_job_id`)
2. Sets `schedule.last_run_at = job.completed_at` (use actual completion time, not creation time)
3. Commits

Call this from `_run_dispatched_job()` and `_run_census_job()` after setting `job.status = SUCCESS`.

#### Step 3: Add `schedule_id` to IngestionJob model

Add a nullable FK column to `IngestionJob`:
```python
schedule_id = Column(Integer, nullable=True, index=True)
```

This gives us a reliable link from job → schedule (instead of matching by `last_job_id`). Set it in `run_scheduled_job()` when creating the job:
```python
job = IngestionJob(
    source=schedule.source,
    status=JobStatus.PENDING,
    config=effective_config,
    schedule_id=schedule.id,      # ← NEW
    trigger="scheduled",          # ← Also set trigger
)
```

#### Step 4: Handle edge cases

- **First run (no `last_run_at`)**: Still does full load — no change needed
- **Failed job retried successfully**: Retry inherits `schedule_id` from parent job. On success, advances watermark.
- **Multiple schedules for same source**: Each schedule tracks its own `last_run_at` independently — works correctly by design.
- **Job succeeds with 0 rows**: Still advance watermark (empty result is valid — no new data in window)

### Files Changed
| File | Change |
|------|--------|
| `app/core/models.py` | Add `schedule_id` column to `IngestionJob` |
| `app/core/scheduler_service.py` | Remove `last_run_at` update from `run_scheduled_job()`; set `schedule_id` on job |
| `app/api/v1/jobs.py` | Add `_advance_schedule_watermark()` helper; call it on job success |

---

## Fix 2: Partial Batch Commit Handling

### Current Behavior (batch_operations.py:121-157)
```python
for i in range(0, total_rows, batch_size):
    batch = rows[i : i + batch_size]
    db.execute(text(sql), batch)
    result.rows_inserted += len(batch)
    if commit_per_batch:
        db.commit()       # ← Each batch committed independently
    # If next batch fails, previous batches are permanent
```

On failure, the batch loop raises immediately (line 157: `raise`). The job is then marked FAILED. But committed batches stay in the DB.

### Desired Behavior
- Track **progress checkpoint** (how many rows successfully committed) on the job record
- On retry, the ingestor can skip already-committed rows
- Use upsert (ON CONFLICT) to make re-processing safe even without skipping

### Implementation

#### Step 1: Add progress tracking columns to `IngestionJob`

```python
# New columns on IngestionJob
rows_committed = Column(Integer, nullable=True, default=0)     # Running total of committed rows
progress_checkpoint = Column(JSON, nullable=True)               # Source-specific checkpoint data
```

`progress_checkpoint` stores source-specific resumption info, e.g.:
```json
{
    "last_batch_index": 5,
    "last_date_processed": "2025-06-15",
    "rows_committed": 5000
}
```

#### Step 2: Update `batch_insert()` to track progress on the job

Add optional `job_id` parameter to `batch_insert()`:

```python
def batch_insert(
    db, table_name, rows, columns,
    batch_size=1000,
    conflict_columns=None,
    update_columns=None,
    progress_callback=None,
    commit_per_batch=True,
    job_id=None,              # ← NEW: track progress on job record
) -> BatchInsertResult:
```

After each successful batch commit, update the job record:
```python
if commit_per_batch:
    db.commit()
    if job_id:
        _update_job_progress(db, job_id, result.rows_inserted, batch_num)
```

The `_update_job_progress` helper:
```python
def _update_job_progress(db, job_id, rows_committed, batch_num):
    """Update job progress without loading full ORM object."""
    db.execute(
        text("UPDATE ingestion_jobs SET rows_committed = :rows, "
             "progress_checkpoint = jsonb_set(COALESCE(progress_checkpoint, '{}'), "
             "'{last_batch_index}', :batch::text::jsonb) "
             "WHERE id = :job_id"),
        {"rows": rows_committed, "batch": batch_num, "job_id": job_id}
    )
    # Don't commit here — rides on the next batch commit
```

#### Step 3: Make retries safe with upsert

Most sources already use `ON CONFLICT ... DO UPDATE` (upsert). Verify and enforce:
- All sources with incremental loading MUST use `conflict_columns` in their `batch_insert()` calls
- This makes re-processing the same data idempotent — no duplicates on retry

Audit which sources currently use upsert vs plain INSERT:
- Sources using upsert: FRED, EIA, BLS, Treasury, BEA, Census (most do)
- Sources using plain INSERT: Check and fix any that don't

#### Step 4: Propagate `rows_committed` to retry jobs

When `auto_schedule_retry()` creates a retry, copy `progress_checkpoint` from the failed job to the new job's config:
```python
retry_config = dict(original_job.config or {})
if original_job.progress_checkpoint:
    retry_config["_resume_from"] = original_job.progress_checkpoint
```

Individual ingestors can optionally use `_resume_from` to skip already-processed data. This is **opt-in** — ingestors that don't check it simply re-process (safe if using upsert).

### Files Changed
| File | Change |
|------|--------|
| `app/core/models.py` | Add `rows_committed`, `progress_checkpoint` to `IngestionJob` |
| `app/core/batch_operations.py` | Add `job_id` param to `batch_insert()`, update progress after each batch |
| `app/core/ingest_base.py` | Pass `job_id` through `insert_rows()` to `batch_insert()` |
| `app/core/retry_service.py` | Copy `progress_checkpoint` to retry job config |

---

## Execution Order

1. **Fix 1 first** — Watermark timing is the higher-risk issue (silent data loss). Smaller change surface.
2. **Fix 2 second** — Partial batch handling builds on Fix 1 (needs `schedule_id` FK).

## Testing Strategy

### Fix 1 Tests
- **Unit test**: Mock a scheduled job that fails → verify `last_run_at` NOT advanced
- **Unit test**: Mock a scheduled job that succeeds → verify `last_run_at` == `completed_at`
- **Unit test**: Verify `schedule_id` is set on jobs created by `run_scheduled_job()`
- **Integration test**: Run a real schedule → fail intentionally → run again → verify same date window

### Fix 2 Tests
- **Unit test**: `batch_insert()` with `job_id` → verify `rows_committed` updated after each batch
- **Unit test**: Simulate batch failure at batch 5 → verify `rows_committed` = 4000, `progress_checkpoint.last_batch_index` = 4
- **Unit test**: Retry job has `_resume_from` in config from failed parent
- **Integration test**: Trigger ingestion with rate limit that causes mid-batch failure → retry → verify no duplicates

## Risk Assessment

- **Fix 1 risk: LOW** — Changing when `last_run_at` is set is a targeted change. Fallback: if the new code has a bug, `last_run_at` stays at old value → worst case is re-fetching data (safe with upsert), not losing data.
- **Fix 2 risk: LOW-MEDIUM** — Adding columns is non-breaking. The `job_id` param to `batch_insert()` is optional (default None). Existing callers unchanged until explicitly opted in.
