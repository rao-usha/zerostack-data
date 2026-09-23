# SPEC 123 — Dataset catalog (DatasetSpec registry)

**Status:** Draft
**Task type:** service
**Date:** 2026-09-23
**Plan:** PLAN_087 wave 1; PLAN_085 §4 (+ rev_01); review `docs/reviews/2026-09-23_daas_platform_review.md` §4 "DatasetSpec fields", §5 rows 10 and 12
**Test file:** tests/test_spec_123_dataset_catalog.py

## Goal

One declared catalog that is both the ops spine and the customer contract.

Today six catalogs compete and none covers the SEC bulk, entity and mart
datasets together:

| Catalog | Grain | Gap |
|---|---|---|
| `dataset_registry` table | table | 66 % of rows missing, bulk loaders/marts never write it, timestamp = "attempt started" |
| `SOURCE_DISPATCH` (jobs.py, 104 keys) | API call | no tables, no rights |
| `BULK_SOURCES` (8) | file release | no tables attribute |
| site-intel `COLLECTOR_REGISTRY` (44 registered) | collector | tables only via the models it upserts |
| `SOURCE_REGISTRY` (51, served by `/sources`) | source family | no SEC bulk / entity / mart, no rights |
| `API_REGISTRY` (38) | API key | key requirements only |

## Design

### `app/catalog/` (new package)

- `spec.py` — frozen `DatasetSpec` + the closed vocabularies (`KINDS`,
  `RERUN_POLICIES`, `REDISTRIBUTION`, `PII_CLASSES`, `ORIGINS`,
  `STATUS_PUBLIC`, `PRODUCER_KINDS`). `__post_init__` validates every field
  (key slug, vocabularies, producer syntax, table names, `coverage_sql` is a
  single read-only `SELECT`, public datasets need a reviewed rights block).
  Fields: key, source, display_name, description (customer-facing), kind,
  grain, tables, table_patterns, primary_key, producer, also_produced_by,
  inputs, cadence, coverage_sql, coverage_from, rerun, owner, slo_lag_hours,
  license, redistribution, attribution, pii_class, origin, status_public,
  reviewed, notes.
  - `effective_redistribution` — `internal_only` until `reviewed`; this is
    what export / the consumer API must enforce.
- `rights.py` — `SourceRights` per source family, written by hand. Every
  source used by a spec must have one (no silent default). US-government
  works are `open` + `reviewed=False`; FRED is `restricted` (third-party
  copyrighted series); Yelp, Google Places, OpenCorporates, SimilarWeb,
  Kaggle, foot-traffic vendors, scraped sites are `restricted` /
  `internal_only`.
- `datasets.py` — the declarations:
  - 8 bulk SEC datasets (tables = the loader's `ddl()` tables),
  - entity master: `entity_source_records`, `entity_cik_crd_bridge`,
    `entity_master` (producer `job:entity_resolve#<stage>`),
  - PE marts: `pe_firms_sec`, `pe_funds_sec`, `pe_people_sec`,
    `sec_adv_private_funds` (producer `job:pe_mart_build#<stage>`),
  - every `SOURCE_DISPATCH` key plus the two special-cased sources
    (`census`, `public_lp_strategies`) — wrapper keys (`irs_soi:all`,
    `osha`, `bls:series` ...) are `also_produced_by` on the dataset they
    write, so each key maps to exactly one dataset,
  - every registered site-intel collector (tables = models it upserts),
  - the collection job types (`people`, `pe`, `lp`, `fo`, `agentic`,
    `foot_traffic`),
  - `SOURCE_REGISTRY` sources with no other producer (`api:<router>`), with
    `SOURCE_ALIASES` folding `API_REGISTRY`-only keys (worldbank, oecd,
    greenhouse_jobs, lever_jobs, smartrecruiters_jobs, foursquare, placer,
    safegraph) onto the family that uses them.
  Dynamic table names (`fred_{category}`, `acs5_{year}_{table}`) are
  `table_patterns` (glob `*`), never invented concrete names.
- `tables.py` — `declared_tables()`: every relation declared in code
  (SQLAlchemy models, `CREATE TABLE` in `app/` and `alembic/versions/`,
  `table_name`/`TABLE_NAME` literals, bulk `ddl()`); `pattern_like()`.
- `registry.py` — `get_catalog()`, `get_spec()`, `filter_specs()`,
  `producer_index()`.
- `live.py` — `table_stats()` / `coverage_through()`: `count(*)` per declared
  table (patterns expanded through `pg_tables`), `SET LOCAL
  statement_timeout` guarded, falls back to `pg_class.reltuples`
  (`rows_exact=false`) on timeout; 60 s in-process cache.
- `mirror.py` — `sync_dataset_registry(db)`: catalog → `dataset_registry`.
  One row per concrete table (pattern tables expanded from `pg_tables`),
  keyed on `table_name`. New rows get `source=spec.source`,
  `dataset_id=spec.key`. Existing rows keep `source`/`dataset_id`/
  `last_updated_at` (what `_update_dataset_registry` and DQ read) and gain a
  `source_metadata.catalog` block. Rows not in the catalog are never
  deleted; they get `catalog.in_catalog=false`. Idempotent: a second run
  changes nothing. Runs at startup (main.py, try/except).

### API — `app/api/v1/catalog.py`

- `GET /api/v1/catalog?kind=&source=&status_public=&redistribution=&q=`
  — static spec fields, no DB.
- `GET /api/v1/catalog/{key}` — spec + `tables` live stats
  (`rows`, `rows_exact`, `exists`) + `coverage_through`. 404 on unknown key.
- Registered with `_auth` (reads for any user). OpenAPI tag `catalog`.

### Not in scope

No schema changes, no `/sources` refactor, no `dataset_key` column on
`ingestion_jobs` (needs a migration; SPEC_124). Enforcement of
`effective_redistribution` at export / consumer API is SPEC_130.

## Acceptance Criteria

- [ ] `DatasetSpec` is frozen and rejects: bad key, unknown kind / rerun /
      redistribution / pii / origin / status, malformed producer, bad table
      name, non-SELECT or multi-statement `coverage_sql`, `ga`/`beta`
      without `reviewed`.
- [ ] Every `BULK_SOURCES` name, `SOURCE_DISPATCH` key (+ census,
      public_lp_strategies), registered collector and data-writing
      `QueueJobType` maps to exactly one spec; the routing types
      (`ingestion`, `bulk_ingest`, `site_intel`) route to the others.
- [ ] Every spec producer names a real producer (reverse check).
- [ ] Every `SOURCE_REGISTRY` and `API_REGISTRY` key is covered by a spec
      source or an alias.
- [ ] Every `tables` entry is declared in code; every `table_patterns` prefix
      is generated by the producer's package. Failures list every offender.
- [ ] Bulk spec tables ⊇ the loader's `ddl()` tables; collector spec tables ⊇
      the models the collector upserts.
- [ ] Every spec source has an explicit `SourceRights`; restricted vendors
      (Yelp, Google Places, OpenCorporates, SimilarWeb/web traffic, Kaggle,
      foot traffic) are not `open`/`attribution`; FRED is restricted.
- [ ] Dormant API sources (no active default schedule) are `archival`.
- [ ] `GET /catalog` filters by kind, source, status_public, redistribution;
      `GET /catalog/{key}` returns live counts + coverage (PG) and 404s.
- [ ] `sync_dataset_registry` creates rows, keeps legacy rows' identity,
      marks unknown rows, is idempotent (PG).

## Test Plan

| ID | Test | Kind |
|---|---|---|
| T1 | dataclass validation (each rejection) | unit |
| T2 | effective_redistribution internal until reviewed | unit |
| T3 | producer coverage: bulk / dispatch / collector / job types exactly one | unit |
| T4 | reverse producer check | unit |
| T5 | SOURCE_REGISTRY + API_REGISTRY folded | unit |
| T6 | tables declared, patterns generated | unit |
| T7 | bulk ddl ⊆ spec tables; collector upsert models ⊆ spec tables | unit |
| T8 | rights explicit; restricted vendors; FRED restricted | unit |
| T9 | archival for dormant; scheduled sources not archival | unit |
| T10 | API list filters + 404 (TestClient, no DB) | unit |
| T11 | detail with live counts + coverage + pattern expansion | PG |
| T12 | statement_timeout fallback to estimate | PG |
| T13 | registry mirror sync: insert, preserve, mark, idempotent | PG |
| T14 | patterns never take another dataset's concrete table; no table matched by two datasets' patterns | unit |
| T15 | shared tables declared in `SHARED_TABLES`; merged block is never less restrictive than any writer | unit |
| T16 | PII: filer directory, entity master, CMS utilization are `personal` | unit |
| T17 | `refresh=true` admin-only; exact-count cap, deadline, single-flight | unit |
| T18 | catalog-only rows excluded from quality gate / profiling; ingestor touch includes them; catalog block survives ingestor writes | PG |

## Review fixes (spec-123-fix)

- **Pattern overlap.** `resolve_tables` never lets a pattern take a table
  another dataset declares concretely (`sec_8k*` vs the bulk
  `sec_8k_index`). `usda_*` narrowed to `usda_crop_production*` /
  `usda_livestock*` (it matched the site-intel `usda_truck_rate`); SEC
  per-company patterns narrowed to the filing types `sec/metadata.py`
  generates.
- **Shared tables.** Tables written by several datasets are listed in
  `datasets.SHARED_TABLES` with the reason. The mirror's catalog block for
  such a table is merged (`mirror.merge_rights`): most restrictive
  redistribution (`restricted > internal_only > attribution > open`) and
  PII (`personal > business_contact > none`), `origin` = the most severe
  (`synthetic > llm_extracted > scraped > derived > official`), `origins` =
  all, `datasets` = all writers, `status_public` only as public as the least
  public writer.
- **PII.** `sec_edgar_submissions` (insiders file under their own CIK),
  `entity_source_records` / `entity_master` (fed insider owners) and
  `cms_medicare_utilization` (individual physicians) are `personal`.
- **DQ scope.** Mirror-inserted rows get `last_updated_at = CATALOG_ONLY_TS`
  (1970-01-01; the column is NOT NULL). `DatasetRegistry.ingested()` filters
  them out of the post-job quality gate, `profile_all_tables`,
  `evaluate_all_rules`, `compute_daily_snapshots`, the DQ recommendation
  engine and the DQ deep report. The first ingestor write stamps a real time
  and the row joins those consumers — so DQ scope is unchanged by the mirror.
- **Catalog block survival.** `_update_dataset_registry` merges the catalog
  block into the new metadata, and a `before_update` listener on
  `DatasetRegistry` does the same for per-source ingestors that assign
  `source_metadata` directly. (Raw-SQL writers are not covered; none of them
  rewrite `source_metadata` today.)
- **Live-count bounds.** `refresh=true` needs the admin role (403
  otherwise). One computation does at most `MAX_EXACT_COUNTS` (12) exact
  counts within `LIVE_DEADLINE_S` (20 s); the rest get the `pg_class`
  estimate (`rows_exact=false`). Concurrent requests for the same dataset
  share one computation.
