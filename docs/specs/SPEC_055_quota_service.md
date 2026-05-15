# SPEC 055 — Free-Tier Quota / Metering Service

**Status:** Draft
**Task type:** service
**Date:** 2026-05-14
**Test file:** tests/test_spec_055_quota_service.py
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Component C3 / Step 4

## Goal

Meter playground generator runs per day so the free tier has a real ceiling. Net-new — Nexdata's only rate limiting is per-data-source and per-API-key, neither of which fits per-user/per-IP playground quotas. A simple daily counter keyed by `(subject_type, subject_key, day)` with an atomic `ON CONFLICT DO UPDATE` increment. The playground router (Step 6) enforces it via a FastAPI dependency.

## Acceptance Criteria

- [ ] New `playground_quota_buckets` table (idempotent `_ensure_tables`): `subject_type VARCHAR(10)` (`user`|`ip`), `subject_key VARCHAR(64)`, `day VARCHAR(10)` (`YYYY-MM-DD`), `run_count INTEGER DEFAULT 0`, `PRIMARY KEY (subject_type, subject_key, day)`.
- [ ] `_day_key(now=None)` returns the current UTC date as `YYYY-MM-DD`.
- [ ] `_reset_ts(now=None)` returns the next UTC midnight as an ISO timestamp string (when the bucket resets).
- [ ] `check_and_consume(subject_type, subject_key, limit) -> QuotaResult` — atomically: if the current day's `run_count` is `< limit`, increment it and return `allowed=True`; otherwise return `allowed=False` and do **not** increment. `remaining` reflects the post-call state; `reset_ts` is next UTC midnight. The increment uses `INSERT ... ON CONFLICT (subject_type, subject_key, day) DO UPDATE` so concurrent calls can't double-spend or skip.
- [ ] `peek(subject_type, subject_key, limit) -> QuotaResult` — read-only; never mutates the bucket; returns the same shape (`allowed` = would-the-next-call-succeed).
- [ ] A `limit` of `0` always denies; a negative `limit` is treated as `0`.
- [ ] Buckets are per-day — a call on a new `day` key starts fresh at 0 regardless of yesterday's count.
- [ ] `QuotaResult` carries `allowed: bool`, `remaining: int` (never negative), `reset_ts: str`, `limit: int`, `used: int`.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_day_key_and_reset_ts | `_day_key` is `YYYY-MM-DD` UTC; `_reset_ts` is the next UTC midnight, strictly after now |
| T2 | test_check_and_consume_under_limit | first calls under the limit return `allowed=True` and increment `run_count` |
| T3 | test_check_and_consume_at_limit_denies | the call at the limit returns `allowed=False` and does **not** increment |
| T4 | test_zero_limit_always_denies | `limit=0` (and negative) denies without incrementing |
| T5 | test_peek_does_not_mutate | `peek` returns the right `remaining`/`allowed` and never changes `run_count` |
| T6 | test_separate_subjects_independent | `(user, A)` and `(ip, A)` and `(user, B)` are independent buckets |
| T7 | test_new_day_resets | a different `day` key starts fresh at 0 |
| T8 | test_remaining_never_negative | `remaining` is clamped at 0 even when denied |

## Rubric Checklist

_No `service` rubric file exists in `memory/rubrics/` — generic checklist:_

- [ ] Service importable without side effects; table created lazily in `_ensure_tables`
- [ ] Schema idempotent (`CREATE TABLE IF NOT EXISTS`)
- [ ] Increment is atomic (`ON CONFLICT DO UPDATE`) — no double-spend under concurrency
- [ ] `peek` is strictly read-only
- [ ] Pure date helpers (`_day_key`, `_reset_ts`) unit-testable without a DB
- [ ] Parameterized SQL only
- [ ] Tests cover under/at/over limit, zero limit, peek, subject isolation, day reset

## Design Notes

```python
# app/services/quota/quota_service.py
@dataclass
class QuotaResult:
    allowed: bool
    remaining: int
    reset_ts: str
    limit: int
    used: int

class QuotaService:
    def __init__(self, db): self.db = db; self._ensure_tables()
    @staticmethod
    def _day_key(now: datetime | None = None) -> str
    @staticmethod
    def _reset_ts(now: datetime | None = None) -> str
    def check_and_consume(self, subject_type, subject_key, limit) -> QuotaResult
    def peek(self, subject_type, subject_key, limit) -> QuotaResult
```

`check_and_consume` reads the current count, and if `count < limit` does the
`ON CONFLICT` increment in the same call. (Per-row contention on a single
`(subject, day)` key is negligible at playground volume; correctness over a
fancier atomic-CTE is the priority for the MVP.) DB-backed tests use the same
real-Postgres `pg_session` pattern as SPEC_053/054 (`spec055-` subject_key
cleanup, skip without `DATABASE_URL`).

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/quota/__init__.py` | Create | package marker |
| `app/services/quota/quota_service.py` | Create | `playground_quota_buckets` table, `QuotaResult`, `QuotaService` |
| `tests/test_spec_055_quota_service.py` | Create | T1–T8 |

## Feedback History

_No corrections yet._
