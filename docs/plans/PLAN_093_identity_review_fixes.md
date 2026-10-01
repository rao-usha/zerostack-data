# PLAN 093 — Identity review fixes (adversarial review of B1 / B2)

**Status:** Approved by task (workbench orchestrator, adversarial review, 2026-10-01) · **Spec:** `docs/specs/SPEC_149_identity_review_fixes.md`

## Steps

- [x] Spec + failing tests (T1–T9)
- [x] feeds.py: one-CIK-per-sponsor-EIN expansion + expansion-only prune; `_ein9` placeholders
- [x] resolve_core.py: `_ein` placeholders; redirect generic target / chain / cycle
- [x] entity_master.py: EIN lookup normalization
- [x] ruff + unit + PG tests (throwaway postgres)
- [x] Rolled-back full rerun on live (measure), then DRY RUN + LIVE via the job path, before/after counts
- [x] Rolled-back rerun again: 0 changes (idempotency)
- [x] Workbench: bridge refresh dry then --apply; eval_core_identity.py, eval_bridge_core_view.py; idcast count
- [x] Session log

## Left to the owner

- EDGAR-on-EDGAR EIN joins (455 pre-existing multi-CIK entities, insiders carrying an issuer EIN):
  a merge-rule change, measured and reported, not made here.
