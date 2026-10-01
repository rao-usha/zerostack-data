# PLAN 091 — Form 5500 sponsors into the core entity resolver (B1)

**Status:** Approved by task (workbench orchestrator, task B1, 2026-09-30) · **Spec:** `docs/specs/SPEC_147_form5500_core_source.md`

## Why (measured 2026-09-30, read-only)

- `workbench.dol5500_sponsor`: 53,425 rows, 51,189 distinct EINs (PK `(ein, form_year)`, all 9-digit text), loaded 2026-09-01.
- Only 744 sponsor EINs are in `core.identifier`: core's only EIN source is EDGAR (`sec_filers.ein`, 83,662 records in core).
- 1,567 sponsor EINs are in some `core.source_record` (the rest of those are keyed singletons), 3,283 in `sec_filers`; 2,166 matching CIKs are outside the edgar feed scope.
- No other NexData table carries a usable EIN (pe_portfolio_companies / canonical_entities: 0 filled; no 990 or PPP tables).
- Last resolver run 2026-09-18: 376,277 source records, 140,963 live entities, 0 merges, 0 vetoes.

## Steps

- [x] Spec + failing tests (T1–T16)
- [x] `feeds.py`: dol5500 feed (EIN-keyed, latest form_year), `_ein9`, zip5, edgar scope += sponsor-EIN CIKs, skip when the table is absent
- [x] `resolve_core.py`: pure `weak_name_state` + `source_metrics`
- [x] `resolve.py`: metrics + `core.weak_match` writes
- [x] Migration `0016_entity_weak_match`; catalog entries
- [x] ruff + unit + PG tests (throwaway postgres:14-alpine)
- [x] DRY RUN through the job path (one-off worker container running `execute_job` on a claimed `job_queue` row; shared workers are not restarted) — report strong/weak/new/merges/conflicts
- [x] LIVE run, same path; before/after counts
- [x] Workbench: `python -m ingest.bridge_view --refresh` (dry) then `--refresh --apply`; `scripts/eval_core_identity.py`, `scripts/eval_bridge_core_view.py`
- [x] Session log

## Why a one-off worker process, not the shared workers

The 6 shared workers loaded their code days ago; `app/entities/*` is imported lazily, so which code a
queued job would run depends on which worker claims it. Restarting them would also deploy the
uncommitted SEC-gate change (PLAN_090), whose deploy note needs `SEC_USER_AGENT` set first. The one-off
container (same image, same bind mounts, same `execute_job` → executor → `run_entity_master(guard=True)`
→ `core.mart_build` ledger) inserts the `job_queue` row already CLAIMED under its own worker id, so
no shared worker can take it, and heartbeats it like any worker.

## Owner calls left open

- Sponsor-only EINs (no partner record) stay keyed singletons, not entities (materialization rule).
- Clean weak candidates are never promoted to merges.
