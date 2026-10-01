# SPEC 149 — Identity review fixes (SPEC_147 / SPEC_148 adversarial review)

**Status:** Active
**Task type:** service
**Date:** 2026-10-01
**Plan:** `docs/plans/PLAN_093_identity_review_fixes.md`
**Test file:** tests/test_spec_149_identity_review_fixes.py

## Goal

An adversarial review of the uncommitted B1 (SPEC_147, Form 5500 sponsors) and B2 (SPEC_148,
web domains) work, measured on the live core.* tables 2026-10-01, found defects that fuse
different organisations or would put a platform domain on an entity. Fix them without changing
the merge rules the owner has not decided (EDGAR-on-EDGAR EIN joins stay as they are).

## Findings this spec fixes (measured live, 2026-10-01)

1. **SPEC_147 scope expansion fuses distinct filers.** The edgar feed took EVERY `sec_filers` CIK
   carrying a sponsor EIN. 231 of the 3,283 sponsor EINs are carried by more than one CIK (693
   CIKs): parents and subsidiaries (Century Communities' EIN on 134 LLCs), issuers and their insiders
   (Church & Dwight's EIN on 5 reporting persons), ESOPs / deferred-comp trusts, and old "/ADV" or
   "/TA" duplicates. B1 created 153 new entities holding more than one CIK; 7 adviser entities lost
   their CIK (core.entity.cik NULL on conflict), which dropped 7 workbench bridge rows and is the
   40th "missing" CRD (104683, Norris Perne & French) failing `eval_bridge_core_view.py`.
2. **Placeholder EINs.** `123456789` (14 EDGAR filers, 8 in core, fused into one entity) and
   `987654321` pass the repeated-digit check. No IRS EIN begins with `00`; 7-digit values left-padded
   by `_ein9` become exactly such values.
3. **Redirects to a platform host.** A probe redirect A -> B moves A's claims to B without checking B:
   a site that redirects to linkedin.com or a parking page would hand that platform domain to the
   entity (strong, if A had two families). Redirect chains (A -> B -> C) stop at whichever alias was
   processed first, and a cycle (A <-> B) makes both aliases with duplicated claims.
4. **Stale scope records.** `run_feeds` only upserts, so narrowing a feed's scope leaves the old
   records resolving forever.

## Acceptance Criteria

- [x] The edgar scope expansion takes a `sec_filers` CIK only when its sponsor EIN is carried by
      exactly ONE `sec_filers` CIK; a shared EIN is reported (`sponsor_ein_shared`), never expanded.
- [x] Edgar records outside the current scope that only the expansion could have fed (CIK in no
      PE-relevant table) are pruned after the edgar feed; the count is reported. Other feeds and
      other stale records are untouched (pruning them is not this spec).
- [x] `123456789`, `987654321` and any EIN beginning `00` are refused as keys (`resolve_core._ein`,
      used for every record at resolve time, and `feeds._ein9`).
- [x] A redirect whose target is not a kept domain (`domains.domain(target)` is None: generic /
      invalid) is not an alias: the source domain's links become `conflict` with reason
      `redirect_to_generic`; its claims move nowhere.
- [x] Redirect chains resolve to the final non-redirecting domain (<= 5 hops); a cycle is refused
      (`conflict`, reason `redirect_cycle`), nothing moves.
- [x] `GET /entities/master/by-id/ein/{value}` accepts the hyphen form (`12-3456789`).
- [x] Re-running over an unchanged corpus changes 0 entities / memberships / weak / domain rows.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_placeholder_eins_refused | 123456789 / 987654321 / 00-prefix refused by `_ein` and `_ein9`; real EINs kept |
| T2 | test_placeholder_ein_never_joins | two records sharing 123456789 stay apart in `plan()` |
| T3 | test_scope_sql_requires_unique_ein | the expansion SQL carries the one-CIK-per-EIN rule |
| T4 | test_redirect_to_generic_not_alias | A -> linkedin.com: A conflict `redirect_to_generic`, no linkedin.com row |
| T5 | test_redirect_chain_resolves | A -> B -> C: A and B aliases of C, claims counted on C |
| T6 | test_redirect_cycle_refused | A <-> B: both conflict `redirect_cycle`, no alias |
| T7 | test_lookup_value_ein_hyphen | API lookup normalizes `12-3456789` |
| T8 | test_shared_sponsor_ein_not_expanded_pg | PG: a sponsor EIN on two CIKs feeds neither; a unique one still feeds |
| T9 | test_prune_expansion_only_pg | PG: an expansion-only edgar record left by a wider scope is pruned; an in-scope one is not |

## Rubric Checklist (generic — no service rubric file exists)

- [x] Tests written and watched fail before source code
- [x] Parameterized SQL only
- [x] Idempotent; nothing dropped silently (counts reported)
- [x] ruff clean

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/entities/feeds.py | Modify | unique-EIN expansion, expansion prune, `_ein9` placeholders |
| app/entities/resolve_core.py | Modify | `_ein` placeholders; redirect target / chain / cycle rules |
| app/api/v1/entity_master.py | Modify | EIN lookup normalization |
| tests/test_spec_149_identity_review_fixes.py | Create | T1–T9 |
