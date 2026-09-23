# PLAN 087 — "Next": make it a product (wave 1 + wave 2)

**Status:** Approved by user 2026-09-23 ("start on the next camp")
**Source:** `docs/reviews/2026-09-23_daas_platform_review.md` §6 "Next", PLAN_085 (+rev_01), PLAN_086 (done)

## Waves

| Wave | Spec | Scope | Depends |
|---|---|---|---|
| 1 | **SPEC_122** Conditional GET + retention | Replay stored ETag/Last-Modified (`If-None-Match`/`If-Modified-Since`) in `SecHttp.stream_to_file`; skip on 304 without minting a release; raw-file retention (keep newest N per source, env-tunable); one-time cleanup of existing snapshots; bytes-saved metrics | — |
| 1 | **SPEC_123** Dataset catalog | `DatasetSpec` registry in code as the single catalog (key, source, display_name, description, kind, grain, tables, primary_key, cadence, coverage_sql, coverage_from, rerun policy, inputs, owner, SLO, license/redistribution, pii_class, origin, status_public). Covers every BulkSource, SOURCE_DISPATCH key, site-intel collector, mart and entity dataset; folds `SOURCE_REGISTRY`/`API_REGISTRY`; coverage test that every registered producer maps to exactly one spec; `dataset_registry` becomes a generated mirror; `GET /catalog`, `GET /catalog/{key}`. Rights default `internal_only` until reviewed. No schema changes. | — |
| 1 | **SPEC_126a** Mart gates + build ledger | `core.mart_build` ledger (Alembic 0013): inputs consumed (release keys), code version, per-stage counts, status; input assertions (latest `source_release` status/age for each input) before `entity_resolve`/`pe_mart_build`; ship gates as code with tolerance vs previous build; tri-state job result (success/partial/failed) for bulk runs with mixed failures | — |
| 2 | **SPEC_124** Status API | `GET /datasets/status` (three clocks, status taxonomy incl. awaiting_upstream/stalled/blocked), read-only; admin `POST /datasets/{key}/run` with `can_run` verdict + audit actor | 123, 126a |
| 2 | **SPEC_125** Status page | `frontend/status.html` (auth.js, read-only + operator view), SSE via stream token; delete/repair dead `index.html` panel | 124 |

Deferred pending user decisions (commercial posture, hosting): SPEC_130 `/data/v1`,
SPEC_131 bulk delivery, SPEC_132 mart history. SPEC_126b (cadence profiles,
backfills) after wave 2.
