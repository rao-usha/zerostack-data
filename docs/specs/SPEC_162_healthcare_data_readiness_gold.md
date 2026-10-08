# SPEC 162 — Healthcare data readiness + gold set (PLAN_100 step 1)

**Status:** Implemented on branch `spec-162` (live apply pending: migration 0022 + NPI backfill, run by the lead)
**Task type:** service (+ model/migration, catalog)
**Date:** 2026-10-08
**Plan:** `docs/plans/PLAN_100_industry_ontologies.md` (§5.3, §6 R1/R2, §7, §8.4 row SPEC_162, Appendix B, §12)
**Test file:** `tests/test_spec_162_healthcare_data_readiness_gold.py`
**Owner decisions used:** D3 = OMOP CDM 5.4; D4 = NPPES Registry API backfill (not the V.2 bulk file); D11 = specs now, paid runs later.
**Numbering:** Alembic `0022_cms_utilization_year_key` (down_revision `0021_ats_link_precision`, the single head on 2026-10-08).
**Parallel work:** SPEC_163 owns `app/catalog/rights.py` (not touched here). SPEC_164 owns `app/ontology/__init__.py` (not created here; `app/ontology/gold/` is a namespace sub-package until it exists).

## Findings that shaped the spec (live, read-only, 2026-10-08)

- `cms_medicare_utilization` 40,054 rows, 29,189 distinct `(rndrng_npi, hcpcs_cd, place_of_srvc)`, 3,216 NPIs.
- The configured DKAN id `92396110-2aed-4d63-a6a2-5d6207d46a29` is **not a year**: it is the
  *DatasetSeries* id of "Medicare Physician & Other Practitioners - by Provider and Service", which
  always serves the latest release (data.cms.gov `data.json`: it is listed as `inSeries` of every year
  and as an API distribution of the 2024 release, modified 2026-05-21). The pinned per-year ids are
  2023 `0e9f2f2b-7bf9-451a-912c-e02e654dd725` and 2024 `335e5f35-eca6-482d-87b3-f99883e213e3`.
- So the table holds **two data years**, decided by load date (spot-checked against both pinned ids):
  - 2026-02-20 / 2026-02-25 loads (21,238 rows) = **data year 2023** (NPI 1003000126: 99221/F 12 services
    in our rows = 12 in the 2023 release, 36 in 2024). These loads are the duplicates: 10,373 keys, each
    loaded twice (+10 identical rows on 02-25). The first ~1,006 NPIs of the national file, all states.
  - 2026-09-25 load (18,816 rows) = **data year 2024**, Wyoming only, complete (DKAN stats: 18,816 WY rows
    of 9,781,673). NPI 1003029653 matches the 2024 release exactly, not 2023.
  - The two years share no key, so the dedupe removes exactly the 10,865 in-year duplicate rows.
- `nppes_providers`: 39,817 NPIs, every `status='A'`; `organization_subpart`, `dba_name` and `gender` are
  NULL on every row (the parser read keys the Registry API does not send: it sends
  `organizational_subpart`, `other_names`, `sex`).
- `cms_hospitals`: 5,426 rows, `facility_id` unique; `city` and `county` NULL on every row (parser bug,
  follow-up BUG-CMS-HOSP-GEO); `overall_rating` 2,866 non-NULL ("Not Available" otherwise).
- 2,983 of 3,216 utilization NPIs are absent from `nppes_providers` (233 overlap, 7.2%).

## Acceptance criteria

### 1. `cms_medicare_utilization` data year + unique key
- [x] `app/sources/cms/metadata.py`: `DKAN_VERSIONS` pins the per-year DKAN ids (2013-2024, from
  data.cms.gov `data.json`), `DEFAULT_DATA_YEAR = 2024`; the series id stays as documentation
  (`dkan_series_id`) and is never fetched. `data_year INTEGER NOT NULL` column; fresh tables get the
  named unique constraint `uq_cms_medicare_utilization_year_key (data_year, rndrng_npi, hcpcs_cd, place_of_srvc)`.
- [x] Ingest: `year` selects the pinned version (unknown year = ValueError naming the known years; the
  old code silently ignored `year`); every row carries `data_year`; per-state replace deletes only
  `(state, data_year)`; inserts are `ON CONFLICT (key) DO UPDATE` with in-chunk dedupe (last wins), so
  overlapping pages or re-runs never duplicate. Before writing, the ingest checks the constraint exists
  and fails with "run alembic upgrade 0022" if not.
- [x] Alembic `0022`: guarded by `to_regclass` (no table = no-op), idempotent, under
  `lock_timeout` with retries (0014 pattern). Steps: add `data_year`; backfill by load date
  (`< 2026-05-21` = 2023, else 2024, the date the series switched to 2024); delete duplicates keeping the
  latest `ingestion_timestamp` (tie: highest id) per key (`IS NOT DISTINCT FROM`, NULL-safe); `SET NOT NULL`;
  add the constraint if absent. Downgrade drops the constraint and column (deleted duplicates are not restored).
- [x] Live expectation: 40,054 -> 29,189 rows (2023: 10,373; 2024: 18,816); 10,865 deleted.
- [x] Catalog text for `cms_medicare_utilization` states both years, the grain, the sample, the key.

### 2. `cms_hospitals` catalogued
- [x] `DatasetSpec` (producer `api:cms_hospitals`, kind reference, PK `facility_id` = CCN, coverage as_of
  `max(ingested_at)`), limitations for NULL city/county and the "Not Available" ratings. Rights: cms
  family default (no `rights.py` edit; follow-up for SPEC_163).
- [x] SPEC_143 lineage fixture: `cms_hospitals` jobs now resolve (EXPECTED_UNRESOLVED -1, EXPECTED_RESOLVED +4).

### 3. Semantic types + column dictionary
- [x] `identifiers.py`: `ccn` (6 chars, `lpad(upper(trim)),6,'0')`), `nucc_taxonomy` (10 upper alnum),
  `hcpcs` (5 upper alnum), `icd10cm` (upper, no dot), `pac_id` (10 digits, lpad). `npi` unchanged.
- [x] The three tables had no column dictionary (their DDL was f-string templates). Their CREATE TABLE
  text is now static (`nppes_providers`, `cms_hospitals`: literal table name; utilization:
  `metadata.UTILIZATION_TABLE_DDL`, tested equal to the column map), so the harvester types them, and
  curated rows describe every column (30 / 32 / 19) with semantic types (npi, nucc_taxonomy, hcpcs, ccn,
  zip5, fips_state, us_state, amount_usd, period_date) and `pii` on natural-person fields.
  `columns.generated.json` regenerated; `usage.json` regenerated (no change).
- [x] Truth-pass test updates: SPEC_137 type count 24 -> 29; SPEC_141 unverified set + `cms_hospitals`,
  evidence disposition for the utilization PK `amended:SPEC_162`; SPEC_143 `cms_hospitals` jobs resolve
  (273 -> 277).

### 4. NPI gap backfill (`python -m app.sources.nppes.backfill`)
- [x] Finds `rndrng_npi` in `cms_medicare_utilization` not in `nppes_providers` (valid 10-digit NPIs only).
- [x] Dry run (default) writes nothing: reports the gap and fetches a small sample (`--sample`, default 3)
  to prove parsing. `--apply` fetches all (or `--limit`) through `NPPESClient` (local pacing
  `--rps`, default 1.0, capped at 2.0; the `npiregistry.cms.hhs.gov` distributed bucket under WORKER_MODE=1)
  and upserts with null-preserving `COALESCE(EXCLUDED.col, existing.col)`, committing every `--batch` rows.
  Re-runs skip NPIs already present (resumable). NPIs the Registry no longer returns (deactivated) are counted
  as `not_found`, never inserted.
- [x] Parser fix used by both ingest and backfill (Registry API v2.1 keys, checked live 2026-10-08):
  `organizational_subpart`, DBA from `other_names` code 3, gender from `sex` (old keys still read as fallback).
- [x] Live dry run (read-only transaction, 3 Registry calls): utilization NPIs 3,216, missing 2,983,
  3 of 3 sampled NPIs found and parsed.

### 5. Gold set (`app/ontology/gold/healthcare_provider/`)
- [x] `column_map.json`: hand-built gold map for every column of the three tables (`table.column` ->
  FHIR R4 element, OMOP CDM 5.4 `TABLE.field`, class, in-universe flag, `post_need` target set).
- [x] `cq_probes.sql` + `probes.py`: one read-only probe per CQ (30), with a deterministic rule to
  HOLD / PARTIAL / NEED / SCHEMA. `cq_labels.json`: labels and the live evidence of 2026-10-08.
- [x] `cq_gold.sql` + `cq_fixtures/`: gold SQL for the 13 groundable CQs (live HOLD+PARTIAL, plus CQ04
  pending SPEC_163's NUCC table), seed rows and expected results; a test runs every gold query on the
  seed and compares.
- [x] `MANIFEST.sha256`: sha256 of every gold file + an aggregate `gold_sha256`; a test fails on drift.
  Committed before any model run. `python -m app.ontology.gold --check | --write-manifest`.

### CQ labels, live 2026-10-08 (before 0022 and the backfill)

HOLD 3 (CQ02, 05, 15) / PARTIAL 7 (CQ01, 11, 12, 14, 18, 19, 25) / NEED 17 / SCHEMA 3: **10 groundable**
(plan: HOLD 5 / PARTIAL 7 / NEED 15, 12 + CQ04 = 13). Four labels moved, each from a data defect:
CQ14 HOLD -> PARTIAL (`cms_hospitals.county` NULL everywhere), CQ25 HOLD -> PARTIAL (six domain rating
columns NULL everywhere), CQ08 PARTIAL -> NEED (`organization_subpart` NULL: parser key), CQ23 PARTIAL ->
NEED (0 exact name matches among 362 portfolio companies). Gold SQL still covers the plan's 13 (CQ04, 08,
23 included) so nothing has to be re-frozen when the fixes land.

## Out of scope / follow-ups
- Running migration 0022 and `--apply` on live (the lead).
- Rights entry for `cms_hospitals` (SPEC_163; cms family default meanwhile).
- BUG-CMS-HOSP-GEO (`city`, `county` and the six domain rating columns NULL on every row) and
  re-fetching the 39,817 existing NPIs for `organizational_subpart` / DBA / gender (all three are NULL
  on every live row: the API never sent the keys the old parser read).
- `app/ontology/__init__.py` (SPEC_164).
