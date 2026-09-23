# PLAN 085 — Revision 01

**Date:** 2026-09-23
**Trigger:** multi-lens DaaS review (`docs/reviews/2026-09-23_daas_platform_review.md`),
8 surveyors each checked by an adversarial verifier, key findings re-checked by hand.

## Revision 01

The plan's direction held up; three of its claims did not, and it missed
access control and alerting entirely. The execution order changes: the
"stop the bleeding" work in PLAN_086 ships before any portal spec.

## What Was Wrong

1. **§8.2 understated the SPEC_120 zombie bug.** The plan said
   `cancel_stale_pending_jobs` would mark the orphaned `IngestionJob` failed
   after 4h. It does not — that sweep selects unclaimed `job_queue` rows only
   (`job_queue_service.py:167-178`), and `cleanup_stuck_jobs` selects RUNNING
   only. The row stays PENDING forever, and `run_scheduled_job` skips any
   schedule with a PENDING/RUNNING job (`scheduler_service.py:216-229`). Effect:
   every `job:` schedule runs once and then **freezes silently**. Root cause is
   `worker/main.py` updating only `JobQueue`.
2. **Open question 3's premise was false.** `sec_companyfacts` feeds the
   `public_company_financials` view, read by `investor_intelligence` and the
   synthetic-financials trainer. It feeds no mart, but it is not unconsumed.
3. **§D5 was half right.** `bulk_ingest` fails the job when *every* release
   fails; only mixed partial failure reports success. Separately, a raising
   `discover()` leaves the `IngestionJob` RUNNING, which also blocks the schedule.
4. **"Four dataset concepts" is six** — `SOURCE_REGISTRY` and `API_REGISTRY`
   were missed.
5. **`has_live_worker` is not only a 409** — `GET /jobs/workers` exists.

## What Was Fixed

- SPEC_121 widened: generic IngestionJob write-back in the worker, orphan sweep,
  `finally` in `bulk_ingest`, scheduler `job_defaults`, restart policies.
- New specs ahead of the portal: SPEC_127 (access lockdown), SPEC_128 (watchdog
  + health), SPEC_129 (CI + publish guards). See PLAN_086.
- SPEC_123's DatasetSpec gains rights/PII/owner/SLO/inputs fields (deferred).
- "Run now" and schedule writes are out of v1; admin-only when they land.
- Spec numbering: SPEC_121–126 keep PLAN_085's meaning; ADV Schedule A → SPEC_133.

## Lessons Learned

- When a plan says "X will clean this up", read X's WHERE clause. A sweep's
  name is not its selection criteria.
- A "no consumer" claim needs a grep across views and services, not just marts.
- A status portal is pull-only; the outage it was meant to expose happened
  because nothing *pushed*. Alerting comes before dashboards.
