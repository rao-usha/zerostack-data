# SPEC 054 — Lead Capture + Intent Scoring Service

**Status:** Draft
**Task type:** service
**Date:** 2026-05-14
**Test file:** tests/test_spec_054_lead_service.py
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Component C2 / Step 3

## Goal

Capture every playground signup and generator run as a `lead` with an intent score, so enterprise sales can see who is worth a conversation. Net-new — Nexdata has no CRM today. `AuthService._notify_lead_event` (Step 2) already calls `LeadService.upsert_from_signup` / `mark_verified` behind a guarded import; once this service exists those hooks start working automatically. The playground router (Step 6) calls `record_run` after each generation.

## Acceptance Criteria

- [ ] New `leads` table (idempotent `_ensure_tables`): `id, email UNIQUE, user_id, company_domain, company_name, is_corporate_email, signup_source, first_seen_at, last_active_at, total_runs, verified, intent_score, intent_tier, generators_used JSONB, max_n_requested, routed_to_sales_at`.
- [ ] New `playground_runs` table (append-only event log): `id, lead_id, user_id, generator, n_requested, anon_ip, report_id, created_at` + indexes on `(lead_id, created_at)` and `(anon_ip, created_at)`.
- [ ] `derive_company_domain(email)` returns the lowercased domain part of an email, or `None` for malformed input.
- [ ] A free-mail blocklist (gmail/outlook/yahoo/proton/icloud/aol/hotmail/…) drives `is_corporate_email` — `True` only for non-free, valid domains.
- [ ] `_compute_intent_score(is_corporate, verified, total_runs, generators_used, max_n_requested)` is a **pure function** returning `(score, tier)`: corporate +30; verified +15; `+5 × min(total_runs, 6)`; any high-value generator used (`lp-gp-universe`, `private-financials`, `consumer-crowd`) +15; `max_n_requested ≥ 200` +10. Tiers: score ≥ 70 → `hot`, ≥ 40 → `warm`, else `cold`. Score clamped 0–100.
- [ ] `upsert_from_signup(email, signup_source, request_ip=None)` — `INSERT ... ON CONFLICT (email) DO UPDATE`; derives domain + corporate flag; recomputes intent; never raises on a duplicate.
- [ ] `record_run(email, user_id, generator, n_requested, anon_ip=None, report_id=None)` — always inserts a `playground_runs` row; when a lead is known (by email), bumps `total_runs`, merges `generators_used` (`{generator: count}`), updates `max_n_requested` + `last_active_at`, recomputes intent. Anonymous runs (no email) still log a `playground_runs` row.
- [ ] `mark_verified(email)` — sets `verified=TRUE`, recomputes intent.
- [ ] `recompute_intent(lead_id)` — reads the lead, applies `_compute_intent_score`, writes `intent_score`/`intent_tier`; the first transition into `hot` stamps `routed_to_sales_at` (idempotent — never overwritten).
- [ ] `list_hot_leads(min_score=70, since=None, limit=100)` — returns leads at/above the threshold, newest activity first.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_derive_company_domain | extracts lowercased domain; returns None for malformed input |
| T2 | test_corporate_email_classification | free-mail domains → not corporate; company domains → corporate |
| T3 | test_compute_intent_score_terms | each scoring term adds the right amount; pure function, no DB |
| T4 | test_compute_intent_tiers | score → tier thresholds (hot ≥70, warm ≥40, cold else); score clamped ≤100 |
| T5 | test_upsert_from_signup_creates_and_updates | first call creates the lead row; second call updates, no duplicate, no raise |
| T6 | test_record_run_logs_and_updates_lead | inserts a playground_runs row; bumps total_runs + generators_used + max_n_requested |
| T7 | test_record_run_anonymous | a run with no email still logs a playground_runs row (lead_id NULL) |
| T8 | test_mark_verified_recomputes | mark_verified sets verified + raises the score |
| T9 | test_hot_lead_routed_to_sales_stamped_once | crossing into hot stamps routed_to_sales_at once; later recomputes don't overwrite it |
| T10 | test_list_hot_leads | returns only leads ≥ threshold |

## Rubric Checklist

_No `service` rubric file exists in `memory/rubrics/` — generic checklist:_

- [ ] Service importable without side effects; tables created lazily in `_ensure_tables`
- [ ] Schema changes idempotent (`CREATE TABLE IF NOT EXISTS`, indexes `IF NOT EXISTS`)
- [ ] Intent scoring isolated as a pure function — unit-testable without a DB
- [ ] `upsert_from_signup` / `record_run` are safe to call repeatedly (no duplicate leads)
- [ ] Anonymous runs are still recorded (funnel measurement)
- [ ] Parameterized SQL only
- [ ] Tests cover pure logic offline + DB integration against real Postgres

## Design Notes

```python
# app/services/leads/lead_service.py
HIGH_VALUE_GENERATORS = {"lp-gp-universe", "private-financials", "consumer-crowd"}
FREE_EMAIL_DOMAINS = {"gmail.com", "outlook.com", "yahoo.com", "proton.me",
                      "protonmail.com", "icloud.com", "aol.com", "hotmail.com",
                      "live.com", "msn.com", "gmx.com", "mail.com", "yandex.com"}

def derive_company_domain(email: str) -> str | None       # module-level helper
def is_corporate_email(email: str) -> bool                # module-level helper

class LeadService:
    def __init__(self, db): self.db = db; self._ensure_tables()
    @staticmethod
    def _compute_intent_score(is_corporate, verified, total_runs,
                              generators_used, max_n_requested) -> tuple[int, str]
    def upsert_from_signup(self, email, signup_source, request_ip=None) -> dict
    def record_run(self, email, user_id, generator, n_requested,
                   anon_ip=None, report_id=None) -> None
    def mark_verified(self, email) -> None
    def recompute_intent(self, lead_id) -> None
    def list_hot_leads(self, min_score=70, since=None, limit=100) -> list[dict]
```

DB-backed tests use the same `pg_session` pattern as SPEC_053 (real Postgres, `spec054-` row cleanup, skip without `DATABASE_URL`). Pure-function tests (T1–T4) need no DB.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/leads/__init__.py` | Create | package marker |
| `app/services/leads/lead_service.py` | Create | tables, helpers, `LeadService` |
| `tests/test_spec_054_lead_service.py` | Create | T1–T10 |

## Feedback History

_No corrections yet._
