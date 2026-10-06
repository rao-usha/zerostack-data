# PLAN 099 — ATS board discovery over all kept PE targets (SPEC_155)

**Status:** Approved via the workbench orchestrator (owner request 2026-10-05: implement the pilot's verification rule, dry-run 200 firms, then run LIVE over all kept targets)
· **Spec:** `docs/specs/SPEC_155_ats_targets_discovery.md`

## Why

The 2026-10-05 pilot (300 kept targets, every hit hand-checked) showed the lane's SPEC_151 rule is
precise (11/11) but finds only 55% of the true boards; rule R7 (full name, or canonical name + the
firm's city/state in any posting; short one-word names need location unless the board name is
exact) found 17 of 20 true boards with 0 false links in-sample. Expected yield on 5,786 firms:
~420 verified boards (range 250-600), ~30k requests, ~11 h at 2 s per host.

## Steps

- [x] Spec + failing tests T1-T22 (watched fail)
- [x] Migration 0020: ats_board columns (target_id, verification, score, verified_at, careers
      domain + evidence), status `candidate`; `ats_discovery_run`; `ats_discovery_attempt`
- [x] `match.py` (pure): variants, canonical names, location, R7 verdict + score + evidence,
      careers domain
- [x] `collect.py`: refresh never re-verifies, never rewrites basis/evidence/link (the weekly run
      on 2026-10-06 would otherwise hit `ck_ats_board_basis`); `store_postings` split out
- [x] `targets.py`: target loading (names, aliases, Form D previous names, persons count), one
      runner thread per host, ledger skip / not_found cache / board reuse, retry pass, stop
      conditions, budget, stop file, run row metrics + checkpoint, linking, `--link-workbench`
- [x] SPEC_148: attach-only feed `atsboard` (family `ats_board`) -> weak domain claims
- [x] Catalog: new tables on dataset `ats_boards`; regenerate column dictionary
- [x] ruff; SPEC_151/153/155/148 suites (PG ones against a throwaway postgres)
- [x] Apply migration 0020 (one-off container, `app.core.migrate.run_migrations`)
- [x] DRY RUN 200 kept targets NOT in the pilot sample; hand-check every verified hit; compare with
      the pilot (precision 17/17)
- [x] LIVE run over all kept targets in a background one-off worker container
      (`docker-compose run --rm --no-deps worker python -m app.sources.ats_boards.targets --apply
      --link-workbench`), monitored every few minutes; on 429 / errors pause and resume from the
      ledger (never skip)
- [x] Report: firms scanned, candidates, verified, rejected by reason, requests, hours, postings,
      postings with pay, 30 lowest-score verified links
- [x] Session log

## Safety

- Every request through `open_web` (terms registry, robots on every hop, honest UA, Retry-After,
  >= 2 s per host; Lever Crawl-delay 1 s). Ashby never requested (no variant, the gate refuses).
- No shared container restarted; jobs only in one-off `docker-compose run --rm --no-deps worker`.
- Candidates never carry a firm link (the workbench joins boards by core entity / CIK), so a wrong
  candidate can never reach a firm page.
- Nothing committed (owner keeps NexData local; the orchestrator commits).

## Rollback

`UPDATE ats_board SET status='candidate', core_entity_id=NULL, cik=NULL, target_id=NULL WHERE
verification='verified' AND verified_at >= '<run start>'` removes the links (postings stay, unread
by the weekly refresh); migration 0020 downgrade drops the two new tables and columns.
