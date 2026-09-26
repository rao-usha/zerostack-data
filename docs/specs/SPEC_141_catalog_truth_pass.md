# SPEC_141 — Catalog truth pass (PLAN_088 §3 "SPEC_134")

**Type:** bug_fix
**Date:** 2026-09-26
**Plan:** `docs/plans/PLAN_088_catalog_improvements.md` (§1 findings, §2 target entry, §3 SPEC_134)
**Evidence:** `docs/plans/PLAN_088_evidence.json` (read-only live check of all 150 entries,
2026-09-25), copied per entry with a per-field disposition to
`app/catalog/evidence/verification_2026-09-25.json`.

## Why

The catalog (SPEC_123) describes 150 datasets. The 2026-09-25 verification found
51 entries with no usable data (14 phantom, 4 partly missing, 29 empty, 4 with key
columns all NULL), 116 wrong or overstated descriptions, 9 wrong PII classes, seed /
sample / fabricated rows presented as `official`, two specs counting the same four
tables, a `fred_*` pattern that double counts through a view, and only 11
`coverage_sql` / 8 `primary_key`. The catalog must tell the truth before anything
else (rights review, dictionary, browser) builds on it.

## User decisions applied (all reversible)

- Storage-forbidden and fabricated / seed / sample data are **flagged only**
  (`data_state`, `limitations`, `origin`); nothing is deleted or moved.
- The 14 phantom specs stay, as `status_public='archival'` with a stated limitation.
- Verified PII corrections are applied: 4 raises (`sec_13f`, `medspa_prospects`,
  `si_motor_carriers` via `COLLECTOR_RIGHTS['fmcsa']`, `osha`) and the 5
  schema-verified lowerings (`afdc_ev_stations`, `yelp_categories`,
  `sec_company_filings`, `public_lp_strategies`, `sam_gov_entities`).
- Rights: only tightenings. The `site_intel` family default no longer grants `open`:
  it is `internal_only` with an "undeclared collector" licence, and every existing
  federal collector gets an explicit `COLLECTOR_RIGHTS` entry with its agency. HIFLD
  (cited: public release withdrawn 2022, HIFLD Open shut 2025-08-26, collector reads a
  third-party mirror) is tightened to `restricted`/`scraped`. Loosenings stay proposals.
- Nothing is published as ga/beta; `reviewed` stays False everywhere.

## Design

### New `DatasetSpec` fields (`app/catalog/spec.py`)

| Field | Vocabulary / rule |
|---|---|
| `coverage_basis` | `period`, `as_of`, `fixed_vintage`, `rolling`; required when `coverage_sql` is set |
| `subtitle` | ≤ 100 chars (optional) |
| `keywords` | closed `KEYWORDS` |
| `spatial_coverage` | `US`, `US:state`, `US:county`, `global:country` … |
| `limitations` | tuple of known defects / gaps, each naming a bug id where one exists |
| `row_filters` | `((table, predicate), …)`: table must be declared, predicate read-only, no `;`, no bind params |
| `data_state` | `ok`, `stale`, `empty`, `missing_tables`, `key_columns_null`, `seeded`, `fabricated`, `sample_mixed`, `placeholder`, `demo` |
| `missing_tables` | declared tables / patterns verified absent; non-empty iff `data_state='missing_tables'` |
| `verified_at` | ISO date of the verification the entry reflects |

- `ORIGINS += "curated"` (hand-compiled or seed lists).
- `PRODUCER_KINDS += "script"` (`script:<scripts/*.py name>`), for tables only a
  checked-in script writes (ACS county / tract).
- Description minimum raised from 20 to 50 characters.
- `coverage_sql` also refuses `:name` bind parameters (SQLAlchemy `text()` would
  treat them as binds) and `coverage_from` must be an ISO date.
- ga/beta additionally needs `data_state == 'ok'`.
- All new fields are in `to_dict()`, so `GET /catalog` and `GET /catalog/{key}` expose
  them (`coverage_sql` itself stays internal).

### `app/catalog/datasets.py`

- Tested `coverage_sql` + `coverage_basis` applied (untested / missing-table ones are
  waived with the reason in the evidence file); guards kept: `<= current_date` on
  `sec_companyfacts`, `fema_hma_projects`, `pe_funds_sec`; `least()` for `fred_series`;
  sample sources excluded on `si_*`; `length(county_fips)=5` on NRI.
- Verified `primary_key` (of `tables[0]`), `coverage_from`, descriptions and grains.
- Explicit tables where a pattern hid the fact table (`bls_series` drops the 5 legacy
  duplicate tables, `intl_worldbank`, `fbi_crime_*`, `sec_company_filings`,
  `realestate_osm_buildings`); `census_acs5` tightened to `acs5_20*`.
- Structure (PLAN_088 decision 12): `sec_company_financials` folded into
  `sec_companyfacts` (`also_produced_by=dispatch:sec:financial_data`);
  `si_3pl_{fmcsa,sec,website}_enrichment` folded into `si_3pl_companies`; new
  `census_cbp` and `census_acs_county_tract` specs; `census_cbp` removed from
  `rollup_market_scores`; `lp_collection_runs` (telemetry) and `lp_document` (only
  `public_lp_strategies` writes it) removed from `lp_collection`;
  `lp_gp_relationships` first in `synthetic_lp_gp_universe`; `sec_form_adv` stays
  owned by `sec_adv_roster` (its bulk `ddl()` creates it and it wrote every row),
  `sec_form_adv_firms` keeps writing it (documented shared table).
- `row_filters` on shared tables: `container_freight_index` by provider,
  `job_postings` by `ats_type`, `pe_firms` / `pe_funds` / `pe_people` /
  `pe_firm_people` by the SEC key columns.
- Status: every phantom / empty / unusable entry whose producer never succeeded or is
  stuck is `archival` (eia_*, noaa, fbi_*, usaspending, fema_pa, cms_hospital, empty
  collectors, …).
- Origin: `curated` for FTZ, incentive programs, certified sites, natural-gas infra,
  glassdoor; `synthetic` for `si_incentive_deals` and `public_lp_strategies`;
  `official` for `app_rankings`.
- Cadence and kind corrections from PLAN_088 §3.
- Inputs: verified inputs applied for non-PE derived / entity specs (medspa,
  vertical, rollup, agentic, public_lp_strategies); PE mart inputs are deferred to
  SPEC_143 (the SQL-reference test decides them).

### `app/catalog/live.py`

- `existing_tables()` returns base tables only (no views), so `fred_*` no longer counts
  the `fred_observations` view (264,604 → 132,302).
- `KNOWN_VIEWS` allowlist (`fred_observations`, `public_company_financials`,
  `lp_strategy_quarterly_view`): never pattern-expanded; the mirror does not attribute
  them to a dataset.
- `row_filters` apply to exact counts (a filtered table never falls back to the
  whole-table estimate: `rows=None`, flagged `row_filter`).

## Tests (`tests/test_spec_141_catalog_truth.py`)

- T1 every evidence key is in the catalog or disposed as `folded:<key>`; every proposed
  field has a disposition; `applied` ⇒ spec value equals the proposal.
- T2 each `coverage_sql` is a single read-only SELECT with no bind parameters and has a
  `coverage_basis`.
- T3 derived_mart / entity / geo specs never use the generic site-intel grain.
- T4 a table declared by two specs is row-filtered on every writer or listed in
  `SHARED_UNFILTERED` with a reason.
- T5 phantom / empty in evidence ⇒ `archival`/`retired`, or a `limitations` entry
  naming a bug id.
- T6 every declared table exists in models / DDL, or the entry carries it in
  `missing_tables`; every spec has a `data_state`.
- T7 PII corrections; site_intel default not `open`; every collector has explicit rights.
- T8 new-field validation (vocabularies, row filter safety, 50-char description).
- T9 `GET /catalog` exposes the new fields; `coverage_sql` stays hidden.
- T10 live: views are not tables; row filters apply to counts (PG).
- (integration, `RUN_INTEGRATION_TESTS`) every `coverage_sql` runs in < 2 s on the live DB.

## Out of scope

Fixing ingestors (PLAN_088 §3.7 bug ids are referenced from `limitations`), deleting
or moving data, rights loosenings and the new rights fields (SPEC_142), lineage
(SPEC_143), dictionary (SPEC_137), quality block (SPEC_144).

## Review fixes (spec-141-fix)

- Status page (`app/services/dataset_status.py`): coverage runs for pattern-only specs
  (gate: every declared table exists and at least one resolved table exists, not
  `spec.tables`); a table with a `row_filter` reports `rows=None` on a live-cache miss;
  each dataset row carries `data_state` and `limitations`.
- Coverage batch: combined statements of `COVERAGE_CHUNK` (32) queries; both passes take
  the least recently attempted queries first; chunks not started within
  `COVERAGE_FALLBACK_DEADLINE_S` wait for the next pass; the serial pass always runs at
  least one query. A slow query late in the list is reached and isolated.
- Multi-table coverage over independent streams uses `_least_of()` (per-table maxima,
  least): `bls_series`, `fbi_crime_estimates`, `irs_soi`, `intl_oecd`, `intl_bis`,
  `usda_nass` (also capped at `current_date`). Their evidence disposition is
  `amended:<reason>`. `greatest()` stays only where the tables are one stream
  (`sec_company_filings`, `cftc_cot`, `app_rankings`).
- Mirror: the `dataset_registry` catalog block carries the writers' worst `data_state`
  (`DATA_STATE_SEVERITY`) and the union of their `limitations`.
- `si_seismic_hazard` origin `synthetic`; evidence `other` recommendations naming origin,
  status or kind have `other.<field>` dispositions.
- `_ds()` defaults `data_state="ok"` / `verified_at` only for keys in the evidence file;
  any other key must state them.
- New spec `census_cbp_county_yearly` (script:ingest_cbp_county, also api:atlas).
- Integration tests: no live coverage value after today; every `primary_key` column
  exists on the live table.
