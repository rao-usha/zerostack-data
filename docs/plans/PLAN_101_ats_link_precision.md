# PLAN 101 — ATS link precision fixes after the run-1 review (SPEC_156)

**Status:** Approved via the workbench orchestrator (owner: "do one ultracode" on the 2026-10-06 review)
· **Spec:** `docs/specs/SPEC_156_ats_link_precision.md`

## Why

The review hand-checked 50 of 291 verified links: 92% precision (CI 81-97%), 6 confirmed false links
overall (Verve, Axios, Socket, One Medical, Orchestra, Metabase) + Motive likely. Causes: one-word
names verified on the board name alone, normalisation dropping "Q" / "AG", R7b defeated by a
state-code city and by same-name firms in one city. Four of the wrong careers domains would reach
core.domain_link at the 2026-10-10 04:00 UTC entity_resolve.

## Steps

- [x] Spec + failing tests (watched fail)
- [x] Migration 0021 (statuses `rejected` / `retracted`, `ats_board.ein`); apply in a one-off container
- [x] match.py R8; targets.py df loading, retract, reverify, same-firm seed, ein; collect.py refresh
      404 tolerance + reparse_pay; pay.py pay_v2; feeds.py ein
- [x] Tests green (155/156/151/153/148 + neighbours) against the throwaway PG; ruff; catalog regen
- [x] Retract the review-confirmed false links (dry run, then apply) and the candidate-board postings
- [x] Re-verify every verified link with R8 (dry run, then live) in a one-off worker container
- [x] Retry the `board_linked_to_other_firm` attempts (Cockroach Labs)
- [x] Re-parse stored pay (dry run, then apply)
- [x] Workbench: seeder links only verified active boards; clear the refused GlossGenius link
- [x] Fresh hand check of 20 links; before/after counts; workbench evals; session log

## Safety

- Every request through `open_web` (robots on every hop, honest UA, Retry-After, >= 2 s per host).
  Re-verification is ~2 requests per Greenhouse board and 1 per Lever board (~530 per pass).
- No shared container restarted; jobs in `docker-compose run --rm --no-deps worker`.
- Nothing deleted: rejected boards and retracted postings stay as rows with their evidence.
- Nothing committed (the orchestrator commits).

## Rollback

`UPDATE ats_board SET status='active', verification='verified', core_entity_id=..., ... FROM
discovery_evidence->'retracted'` restores a link (its previous keys are kept there); postings:
`UPDATE ats_posting SET status='open' WHERE status='retracted' AND board_id=...`.
