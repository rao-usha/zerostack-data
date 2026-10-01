# PLAN 094 — Gated shared keys (SPEC_150)

**Status:** Approved by the owner (2026-10-01: "1. do that, 2 keep local, 3 do that"), via the
workbench orchestrator · **Spec:** `docs/specs/SPEC_150_gated_shared_keys.md`

## Steps

- [x] Spec + failing tests (T1-T17), watched fail
- [x] app/entities/gate.py: pure rules (name norm, person / vehicle, sequential, corroborate, ambiguity)
- [x] resolve_core.plan(): ungated groups -> no-CIK anchor clusters -> gated CIK unions ->
      attachment -> contested-key ownership -> main rank; gate evidence + metrics
- [x] resolve_core.canonical_row / assign_ids: withheld keys; main-piece keeper
- [x] resolve.py: filer-profile loader (tolerant), provenance (gate, split_from,
      dissolved_by_gate), metrics
- [x] ruff + unit + PG tests (throwaway postgres); older resolver specs still green
- [x] Measure on live in ONE rolled-back transaction (no sequence use): old vs new plan on the
      471 entities (predicted vs actual), never-a-new-merge check, workbench bridge refresh diff
      and the D2 39-CRD override check
- [x] DRY RUN through the job path (one-off worker container, row inserted CLAIMED)
- [x] Session log

## Review fixes + live run (2026-10-01 PM; owner: "1. do that, 2 keep local, 3 do that")

- [x] Failing tests T18-T21 first (de-SPAC namesake, data-relative sequential, heavy-filer
      first date, EIN+CRD related-names flag), watched fail (5 failed)
- [x] gate_v2: V4 `name_adopted` + A6; `sequential()` data-relative (no run date anywhere in
      plan(); main-piece "active" against the newest profile filing); dated former names and
      `recent_filing_count` in the profile loader; truncated first filing -> former-name date or
      unknown (ranks oldest); A2 flag
- [x] Spec amended (rules, main piece, T18-T21, owner calls left open)
- [x] Rolled-back measurement on live + two job-path DRY runs (jobs 3063, 3064), identical
- [x] LIVE run through the job path (job 3065, mart_build #14 success); before/after counts
- [x] Workbench bridge refresh dry run (8,026 -> 8,062); `--apply` NOT run: the D2 set would go 39 -> 37 (stopped, owner call)
- [x] Workbench evals (core_identity PASS; bridge_core_view FAIL 4: B x2 = D2, C/D = matview not refreshed), NexData tests 672 passed / 2 env failures, ruff clean, session log

## Not in this plan

- Any owner ruling on the flagged pairs (kept split; listed in the spec).
- Any change to the D2 39-CRD override set or its eval (owner decision; reported only).
