# PLAN 098 — LEI / UEI identifiers into the gated resolver; GLEIF CC0 connector (SPEC_154)

**Status:** Done 2026-10-03 (LIVE). Approved via the workbench orchestrator (owner request 2026-10-03: "go after the public
data S&P are creating"; verified decision: S&P DUNL not licensable, build the CC0 / in-house
alternatives) · **Spec:** `docs/specs/SPEC_154_gleif_lei_identifiers.md`

## Why

S&P's "DUNS equivalent" is the CIQ ID on DUNL.org: reachable, but its licence statements conflict
(CC BY-NC-SA site, CC BY-SA metadata, "internal organizational use" release). The licensed public
identifier with the same role is the LEI (GLEIF, CC0). NexData also already holds 841 EDGAR LEIs
and 2,622 USAspending UEIs that never reached the resolver.

## Choices

- **API, not Golden Copy.** goldencopy.gleif.org/robots.txt answers 403; the open_web gate reads
  that as disallow-all (same rule that keeps Ashby out). api.gleif.org/robots.txt allows all; the
  cursor API pages the 362,284 US-legal-address LEIs at 200 per page (~1,812 requests, >= 2 s each).
- **Gate LEI / UEI like EIN / CRD.** 6 LEIs are typed on more than one EDGAR CIK; under the old
  ungated rule they would fuse those filers without corroboration.
- **No Level 2, no spglobal / ocid fields.** No bulk relationship endpoint; mapping fields carry
  third-party rights questions.

## Steps

- [x] Spec + failing tests T1-T19 (watch them fail)
- [x] rights.py (gleif; dunl conflict, proposal closed); site_terms.json (gleif.org allowed,
      dunl.org / spglobal.com refused)
- [x] alembic 0019_gleif (gleif_lei_record, gleif_fetch)
- [x] app/sources/gleif client (URLs, parser) + ingest (fetch loop through open_web, upsert, ledger,
      CLI, job entry); jobs.py dispatch `gleif`; catalog dataset + domains
- [x] feeds.py: edgar lei, gleif feed, usasp feed; resolve_core GATED_KEY_TYPES + weak tier;
      resolve.py metrics; catalog entity inputs / limitations
- [x] ruff; SPEC_154 + SPEC_147/150/143/141/142/123 + rights suites
- [x] Migration in a one-off container; connector DRY RUN (2 pages) then LIVE full load
- [x] Resolver DRY RUN x2 (identical) then LIVE via the job path (one-off worker)
- [x] Coverage on the workbench universes + pilots; workbench bridge / evals
