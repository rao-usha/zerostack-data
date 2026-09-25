# PLAN_088: Catalog improvements (catalog truth, rights, dictionary, lineage, quality, browser)

**Status:** Draft for approval · **Date:** 2026-09-25 · **Scope:** `app/catalog/*`, `app/api/v1/catalog*.py`, DQ services, `frontend/catalog.html`
**Inputs:** a read-only check of all 150 catalog entries against the live DB (`nexdata-api-1`, session `default_transaction_read_only`) and the ingestor code, plus five lenses (rights, column dictionary, discovery UX, lineage/quality, benchmark). Context: `docs/reviews/2026-09-23_daas_platform_review.md`, SPEC_123, SPEC_124 and SPEC_125.

> **Numbering conflict, decide before filing.** This plan uses SPEC_134–139 as requested, with SPEC_133 kept for ADV Schedule A. The DaaS review has already given four of these numbers to other work: SPEC_134 (legacy firm dedup), SPEC_135 (queue hardening), SPEC_136 (hosted runtime) and SPEC_137 (Alembic plus column dictionary) (`review:218-221`). The column dictionary here is placed at **SPEC_137** so that one number lines up with the review. SPEC_140 is taken (`SPEC_140_cms_rate_limit.md`). The alternative mapping would be 134→141, 135→142, 136→143, 137→137, 138→144, 139→145. See Open decision 1.

---

## 1. Findings (measured)

### 1.1 Scoreboard

| Metric (150 specs) | Today | After this plan | Evidence |
|---|---|---|---|
| Declared tables that exist | 175 / 195 | all resolve, or the entry is `archival`/`retired` with the gap stated | information_schema |
| Entries with no usable data | **51** (14 phantom, 4 partially missing, 29 empty, 4 populated but key columns all NULL) | the same count, now visible in the catalog | per-entry verification |
| `coverage_sql` | 11 | 144 proposed and tested live (all under 0.4 s); 6 waived with a reason | verification |
| `primary_key` | 8 | 146 proposed (from unique indexes or verified `count(distinct)`) | verification |
| `inputs` | 10 | every `derived_mart`/`entity` spec, checked against the SQL it runs | lineage lens |
| `coverage_from` | 0 | about 110 fixed dates, with rolling windows documented | verification |
| Descriptions flagged wrong or overstated | **116** | 0 (each rewrite is in the evidence file) | verification |
| PII class wrong | 4 too low, 5 too high (verified) | corrected; column-level PII lint in CI | verification + dictionary lens |
| Rights blocks unsafe or wrong | 7 families, plus the site_intel default (27 collectors) | proposals with citations; tightenings applied | rights lens |
| `reviewed=True` | 0 | still 0; a review workflow exists, with about 35 candidates queued | rights lens |
| Column comments in the DB | 2 of 10,089 | about 56% from existing text plus a glossary, about 80% with upstream dictionaries | dictionary lens |
| Lineage recorded | `lineage_nodes`=2, `lineage_edges`=1 (hand test, 2026-01-14); `core.mart_build`=0 rows | a computed graph from declared, observed and view edges | lineage lens |
| DQ coverage of catalog tables | 147 of 263 registry rows excluded (every SEC bulk table, PE mart and `core.*`) | driven by the catalog | lineage lens |
| `ingestion_jobs.dataset_key` filled | 90 of 3,809 | 3,486 (3,396 resolvable now, plus aliases for the 322 unresolved) | lineage lens |
| `ga`/`beta` datasets | 0 | 0 (this plan does not publish anything) | spec.py:109-111 |

Note: the kinds rollup in the input stats adds up to 146. The 4 missing are `holdings` (sec_13f) and `other`. Where exact counts and `reltuples` disagree, the exact count wins: `container_freight_index` (reltuples about 982, count 0), `motor_carrier` (10 vs 0), `form_adv_advisers` (10 vs 0). **Unresolved conflict:** the rights lens reports "LoopNet 60 scraped rows" but the verifier counted 0 in `warehouse_listing`. Re-run `count(*)` before any purge decision.

### 1.2 Tables that do not exist (phantom specs)

No table at all (14): `eia_petroleum`, `eia_natural_gas`, `eia_electricity`, `eia_retail_gas_prices`, `eia_steo`, `noaa_climate` (5 tables), `afdc_ev_stations`, `yelp_categories`, `us_trade_summary` (`us_trade_trade_summary`), `bts_vmt`, `fda_device_registrations`, `sam_gov_entities`, `opencorporates` (`oc_*`), `vertical_prospects` (5 tables).

Partly missing (4): `kaggle_m5` (`m5_sales` and `m5_prices` are absent, but the grain describes `m5_sales`), `osha` (`osha_violations` is absent and `osha_inspections` is empty), `sec_company_filings` (patterns `sec_s1*`/`sec_s3*`/`sec_s4*` match nothing), `rollup_market_scores` (the mart table is absent; only `census_cbp` exists, which has its own producer, `api:census_cbp`).

Root causes found in code, not in the catalog:
- **EIA:** `generate_create_table_sql` (`app/sources/eia/metadata.py`) creates only non-unique indexes. Every upsert uses `ON CONFLICT (period, COALESCE(series_id,''), …)` (`app/sources/eia/ingest.py:239,405,561,709,853`), so Postgres raises an error and the rollback drops the table. The EIA keys are batch-scheduled (`BATCH_SCHEDULED_DISPATCH` includes `eia`, `eia:electricity`, `eia:natural_gas`), so they show as `internal` while producing nothing.
- **NOAA:** 59 of 59 jobs failed ("zombie: no worker since 2026-04-16"), and the scheduler still enqueues NOAA jobs every day.
- **Lazily created tables that never ran:** opencorporates, vertical_prospects, sam_gov, fda.

### 1.3 Tables that exist but are empty, or unusable

- **Empty (29):** `bea_gdp_industry` (2 "successful" jobs with 0 rows), `fdic_summary_deposits`, `fbi_crime_estimates/summarized/nibrs/hate_crime/leoka` (every run logs "no rows were inserted" but reports success; the latest 'ucr' success was 2026-09-24), `us_trade_port_trade`, `bts_faf_regional`, `intl_imf`, `intl_bis`, `realestate_hud_permits`, `realestate_redfin`, `uspto_patents`, `foot_traffic_locations`, `foot_traffic_observations`, `courtlistener_dockets`, `si_air_cargo`, `si_trade_gateways`, `si_drewry_wci`, `si_freightos_fbx`, `si_scfi`, `si_motor_carriers`, `si_warehouse_listings`, `si_port_throughput`, `si_truck_rates`, `sec_form_adv_legacy`, and `synthetic_job_postings` / `synthetic_lp_gp_universe` (0 synthetic rows).
- **Populated but key columns NULL (4):**
  - `cms_hospital_cost_reports`: 10 rows, every data column NULL.
  - `treasury_debt_outstanding`: 6 annual rows, every amount NULL. The mapping expects Debt-to-the-Penny fields from the `od/debt_outstanding` endpoint.
  - `fema_pa_projects`: exactly 100,000 rows (a 2×50k cap); state, damage_category, project_title and obligation_date are all NULL. `parse_pa_projects` uses the wrong OpenFEMA field names, and because the unique index includes the always-NULL `state`, distinct projects overwrite each other.
  - `usaspending_awards`: NAICS, award_type and period-of-performance are NULL on all 10,190 rows.
- **Stale against cadence:**
  - `treasury_interest_rates` 2026-01-31, `treasury_monthly_statement` 2025-12-31, `treasury_auctions` 2026-02-09. The nightly job refreshes only `daily_balance`.
  - 5 of 6 `fred_*` tables stopped 2026-05-11. `least()` gives 2026-03-01; `greatest()` hides the gap.
  - `cftc_cot` 2025-12-23 (weekly and batch-scheduled).
  - `prediction_markets` 2026-03-31, `fdic_institutions` 2026-02-13, `si_frontier_datacenters` 2026-03-11, `si_renewable_resources` 2026-04-15, `bea_regional` last success 2026-02-09.

### 1.4 Seeded, placeholder or fabricated data presented as `official`

| Dataset | What the rows are | Evidence |
|---|---|---|
| `si_seismic_hazard` | **fabricated**: `pga_2pct_50yr = magnitude*0.1` on 539 epicentres; `fault_line` holds an earthquake feed, not faults | `usgs_earthquake_collector.py:236` |
| `si_incentive_deals` | 48 hand-typed rows (`gjf_expanded`) from `app/scripts/expand_gjf_deals.py`; the GJF collector is disabled (PLAN_082) | `goodjobs_collector.py:84-92` |
| `public_lp_strategies` | 27 snapshots share one date; source URLs follow one template across different pension domains; section text is templated. It looks synthetic, and no generator is in the repo | verification |
| `si_natural_gas_infra` | all 15 rows are `eia_sample` | verification |
| `si_certified_sites` | 31 of 41 rows come from `app/scripts/expand_edo_sites.py`, a producer the spec does not declare | verification |
| `si_foreign_trade_zones`, `si_incentive_programs` | hard-coded seed lists (`FTZ_SEED_DATA`, `SEED_PROGRAMS`) | verification |
| Sample rows mixed into real tables | `power_plant` 21 and `electricity_price` 40 (`eia_sample`), `substation` 26 (`hifld_sample`), `utility_rate` 35, `renewable_resource` 40 (`nrel_reference`), `public_water_system` 4, `water_monitoring_site` 2 | verification |
| Placeholders | `si_intermodal_terminals` ('Terminal 1'), `si_opportunity_zones` ('County 005', no geometry) | verification |
| Demo rows | `glassdoor` (2 manual rows), `app_rankings` (1 app), `web_traffic` (3 rows) | verification |

`ORIGINS` (`spec.py:24`) has no value for hand-curated data, so all of these inherit `official` or `scraped`.

### 1.5 Descriptions and grains that are wrong (116 flagged; highest-impact shown)

- `sec_form_d` says "since 2008", but `min(filed_at)` is 2023-07-03.
- `sec_companyfacts`: `sec_financial_facts` holds only 3 depreciation concepts over a rolling 3-year window.
- `sec_company_financials` declares the **same four tables** as `sec_companyfacts`, so they are counted twice.
- `sec_13f`: holdings (3.83M rows) cover only the latest release, while filings span 13 releases.
- `bea_regional` claims GDP, but only CAINC1/SAINC1 personal income is loaded. `bea_international` is one indicator (BalGds).
- The `bls_series` pattern sweeps in 5 legacy duplicate tables that use a different schema.
- `us_trade_*_hs` are December year-to-date snapshots at the HS2 level, not monthly detail.
- `afdc_ev_stations` is a per-state aggregate, not a station list (its kind and grain are both wrong).
- Site intel:
  - `si_broadband_availability`: `block_geoid` is a made-up state-level id. Joins on it will silently match the wrong rows.
  - `si_flood_zones`: the `county` column holds a DFIRM_ID, and `geometry_geojson` holds `{zone_count}`.
  - `si_wetlands` stores state acreage totals, not polygons. `si_nj_land_use` stores county aggregates, not parcels.
  - `si_industry_employment` and `si_labor_markets` are state-level, not county.
  - `si_renewable_resources` is solar only.
  - `si_national_risk_index` has 332 rows with a 3-digit fips that collide under the unique index.
- All 25+ site-intel entries use the generic grain "one row per feature / record as published".
- Grains that describe the wrong table: `sec_adv_schedule_d`, `sec_companyfacts`, `prediction_markets`, `treasury_interest_rates`, `kaggle_m5`, `intl_worldbank`, `irs_soi`.
- Idempotency is broken (the grain is not enforced):
  - `cms_medicare_utilization`: about 2× duplicate rows, and no `data_year` column.
  - `cms_drug_pricing`: `mftr_name` was dropped, leaving 71,595 rows but only 17,990 distinct keys.
  - `electricity_price`: 45,918 rows where 1,860 are expected, because a NULL `period_month` counts as distinct under the unique index.
  - OECD/IMF/BIS have no unique constraint; `intl_oecd_kei` already has 1,577 duplicate key groups.
  - The `realestate_redfin` unique index leaves out the region.
- Shared tables counted per dataset without a filter:
  - `container_freight_index` (3 providers)
  - `three_pl_company` (4 "datasets"; the `source` column records only the last writer)
  - `job_postings` (synthetic rows only by `ats_type`)
  - `pe_*` (the `pe_collection` tables are about 99% SEC-mart rows)
  - `lp_document` (declared by two specs)
  - `sec_form_adv` (declared by `sec_adv_roster` and `sec_form_adv_firms`, which have different PII classes)
- **Live double count:** the `fred_*` pattern matches the `fred_observations` **view** (a UNION of the six base tables), so `fred_series` reports 264,604 rows instead of 132,302 (`live.py:50-65` includes views).

### 1.6 PII corrections (verified against columns and populated rows)

| Key | Declared | Should be | Evidence |
|---|---|---|---|
| `sec_13f` | none | **business_contact** | `signature_name/title/phone/city` belong to the natural person who signed |
| `medspa_prospects` | business_contact | **personal** | `medical_director_name` is filled on 747 rows (NPPES physicians) |
| `si_motor_carriers` | business_contact (with `open`) | **personal**, or filter out owner-operators | the FMCSA census includes sole proprietors. This is the riskiest classification. |
| `osha` | none | business_contact (suggested) | establishment names and addresses can identify sole proprietors |
| `afdc_ev_stations` | business_contact | none | the schema holds per-state counts only |
| `yelp_categories` | business_contact | none | a taxonomy with no contact data |
| `sec_company_filings` | business_contact | none | only cik, ticker, accession and URLs |
| `public_lp_strategies` | business_contact | none | no contact columns in any of its 6 tables |
| `sam_gov_entities` | business_contact | none (while no POC columns exist) | `sam_gov/metadata.py` DDL |

Guards needed beyond the dataset class:
- `people.email`: 156 of 172 values are `email_confidence='inferred'` (guessed). Needs a hard no-export guard.
- `lp_key_contact` has the same shape as `people` but is rated `business_contact` (currently empty).
- 93 columns in catalog tables have names that look like PII (dictionary lens), which argues for column-level PII tags.

### 1.7 Rights that do not match the source terms (citations in the rights evidence file)

| Source | Today | Problem | Citation |
|---|---|---|---|
| HIFLD (`substation` 8,738, `transmission_line` 52,244) | `open` (inherited from the site_intel default, `rights.py:146-148`) | substations withdrawn from public release in 2022, HIFLD Open shut down 2025-08-26, and the collector reads a Rutgers mirror (`hifld_collector.py:51-52`) | atcoordinates.info/2025/08/08/hifld-open-gis-portal-shuts-down-aug-26-2025 |
| `cms_medicare_utilization` | `_usg`, `open` (`rights.py:211`) | CPT Level I codes and descriptions are AMA copyright, and all 21,260 rows carry `hcpcs_desc` | data.cms.gov (AMA CPT licence notice) |
| IMF (`intl_imf`) | attribution | commercial reuse needs an email to copyright@imf.org first | imf.org/en/about/copyright-and-terms (403; search excerpt) |
| BIS (`intl_bis`) | attribution | no surcharge to subscribers allowed; no implied endorsement | bis.org/terms_statistics.htm (403; excerpt) |
| CourtListener | attribution | product use requires an FLP commercial agreement | wiki.free.law …/courtlistenercom-terms-of-service |
| site_intel family default (27 collectors) | `open`, license "Mixed…", attribution None | a new collector silently becomes `open` | `rights.py:146-148` |
| Terms forbid **storage itself**: Yelp (400 rows, plus medspa derived rows), FRED (no caching or archiving), Kaggle M5 (non-commercial), Kalshi (no archived sets), NZA (3,695 rows; no hosting or storing), PeeringDB (216 rows; no commercial application), LoopNet (scraping banned), Google Places (lat/lng 30 days) | `restricted` | the enum can only gate export, so it cannot say "we should not hold this at all" | terms.yelp.com/developers/api_terms/20250113_en_us, fred.stlouisfed.org/legal, zoningatlas.org/terms, peeringdb.com/aup, kaggle M5 rules, kalshi data ToS |
| OSM (`realestate_osm_buildings`) | restricted | **over**-restrictive: ODbL allows commercial use with attribution plus share-alike | openstreetmap.org/copyright |
| FRED | restricted | 23 of 24 stored series are public domain at source (H.15/H.6/G.17, BLS, BEA, Census, EIA); only UMCSENT is copyrighted | fred.stlouisfed.org/legal |
| GJF `incentive_deal` | `origin='scraped'` | provenance unknown (hand-typed script) | `rights.py:168` |
| `si_utility_rates` | OpenEI attribution only | 36% of rows are EIA-sourced | verification |
| `si_zoning_districts` | NZA block | 164 rows come from `nj_sussex_county_gis`, which the block does not cover | verification |
| AFDC and NREL | "17 U.S.C. §105" | NREL is a contractor-operated lab; its data is free with credit, but not §105 | developer.nrel.gov/terms (DNS fail; excerpt) |

Candidates for `reviewed=True` (a human still signs off):
- SEC non-PII: `sec_13f` (once its PII class is fixed), `sec_companyfacts`, `sec_company_financials`
- Treasury, BLS, BEA, EIA, Census/us_trade, BTS, CFTC, USDA NASS, FDIC, FEMA (with the OpenFEMA notice), IRS SOI, FBI, OSHA, EPA ECHO, USAspending
- FDA (CC0), FCC aggregates, FHFA/HUD
- World Bank and OECD (CC BY 4.0), PatentsView (CC BY 4.0), Epoch (CC BY 4.0), OpenEI (CC0)

PII-bearing public-domain datasets (`nppes`, `sec_insider`, `sec_form_d`, `sec_edgar_submissions`, `sam_gov`, `pe_people_sec`) wait on the PII-policy decision, not on a rights review.

### 1.8 Lineage, quality and usage

- **`app/core/lineage_service.py`** (693 lines): `record_job_lineage` and `record_job_failure` have **zero callers**. The only importer is `app/api/v1/lineage.py:26`, and no frontend calls `/lineage`. The router is registered at `main.py:68,1349,1644`.
- **`core.mart_build` has 0 rows.** SPEC_126a's ledger records nothing, so there is no observed lineage and no mart freshness.
- **Three declarations of mart inputs disagree:** `DatasetSpec.inputs`, `app/marts/inputs.py:62-76` and the SQL each mart actually runs.
  - `pe_firms_sec` declares `sec_iapd_feed` but reads only `sec_adv_roster_snapshots` and `core.identifier`.
  - `pe_funds_sec` reads `sec_adv_filings` and `sec_adv_roster_snapshots`, which are undeclared.
  - `pe_people_sec` reads `sec_adv_roster_snapshots`, which is undeclared.
  - The bridge stage asserts `sec_13f` and `sec_adv_roster`, but the catalog says `entity_source_records`.
- **The DQ framework is blind to the flagship data.** `DatasetRegistry.ingested()` is the selector (`data_profiling_service.py:434`, `quality_trending_service.py:153`, `jobs.py:622-628`), and it excludes 147 of 263 rows. None of the bulk loaders, marts or entity code calls `profile_table`.
- **Profiles are stale:** 107 of 110 tables were last profiled on 2026-03-11 (for example, `fred_interest_rates` profiled at 18,868 rows versus 105,283 live).
- **The freshness score ignores cadence:** it decays to 0 after 168 h (`quality_trending_service.py:57-78`), and 22 of 116 snapshots score 0.0.
- **The row-count-drop check compares a snapshot with itself** (`jobs.py:648` then `:667`).
- **The advisory lock** uses Python `hash()`, which is randomised per process (`data_profiling_service.py:229`), so it does not protect across the api and worker processes.
- **Usage:** 86 of 150 specs have no static SQL reader. The `public_company_financials` view reads `sec_company_metadata`, which no spec declares.

### 1.9 Discovery and schema

- **No page renders the catalog.** `GET /catalog` has no `response_model` and uses substring `q` search (`registry.py:220-241`). The detail route makes up to 12 exact counts under a 20 s deadline (`live.py:34-38`).
- **The export preview does not fit the catalog.** It is admin-only, covers the public schema only (`export.py:208`), runs an unbounded `COUNT(*)` (`:222`) and has no PII masking.
- **Identifiers are inconsistent.**
  - CIK is stored as varchar (24 columns), text (13), integer (7) and bigint (3).
  - `core.identifier` stores CIK unpadded (123,511 rows). A naive join of 1,000 of them to `sec_filers` matched **0**; with `lpad(…,10,'0')` it matched **1,000**.
  - CRD is text (15), integer (13) or varchar (8), with aliases `crd` and `crd_number`.
- **Column truth is split across 7 sources in 5 formats:**
  - PG comments: 2 columns
  - model inline `#` comments: 612 of 3,334
  - `metadata.py` description dicts: fdic 184, cms 66, nppes 30 and others
  - DDL `--` comments in realestate
  - 3 different shapes of bulk `COLUMNS`
  - `census_variable_metadata`: 196 rows
  - `data_profile_columns`: 9,255 rows

  Measured on 20 datasets (886 columns), the union of existing text covers 23.9%, adding Census labels gives 29.3%, and adding a 27-rule glossary gives 56.2%.

---

## 2. Target: a finished catalog entry

Three levels. Every spec must reach **Bronze**. Datasets used internally should reach **Silver**. The PE/entity pack (about 20 specs) should reach **Gold**, the sellable level.

| Field | Today | When done | Level | Benchmark equivalent |
|---|---|---|---|---|
| `description` | median 77 chars; 116 wrong | ≥50 chars (Google minimum) and states **what is actually loaded** (subset, window, geography) | B | Snowflake/Databricks description; schema.org `description` |
| `subtitle` (new) | none | under 100 chars | S | Databricks short description, Kaggle subtitle, AWS short description |
| `keywords` (new, closed vocabulary) | none | 1–5 from `KEYWORDS` (pe, entity, filings, macro, energy, real_estate, labor, health, trade, geo_risk…) | S | DCAT `keyword` (required), Kaggle tags, AWS categories |
| `kind`, `grain` | generic grain on site_intel; 7 grains describe the wrong table | grain for tables[0], with a per-table grain in notes | B | Datasheets "instances" |
| `tables` | patterns where explicit names would be clearer | explicit, **fact table first**; patterns only for generated names, and never matching views | B | DataHub dataset URN |
| `row_filters` (new) | none | a read-only predicate per shared table (`provider='drewry'`, `ats_type='synthetic'`) | B | Unity Catalog row filters |
| `primary_key` | 8 | natural key of tables[0], confirmed by a unique index or `count(distinct)` | B | AWS "primary key" |
| `inputs` | 10; wrong for PE marts | matches the SQL the producer runs (CI test) | B | DataHub upstreamLineage |
| `cadence` | 12+ mismatches | matches the schedule or source frequency | B | DCAT `accrualPeriodicity` |
| `coverage_sql` + `coverage_basis` (new: `period`/`as_of`/`fixed_vintage`/`rolling`) | 11 | tested, returns a DATE, guarded against junk future dates | B | schema.org `temporalCoverage`, Databricks time range |
| `coverage_from` | 0 | fixed date, or a documented rolling window | B | DCAT `temporal` |
| `spatial_coverage` (new) | none | e.g. `US:state`, `US:county`, `global:country` | S | schema.org `spatialCoverage`, DCAT `spatial` |
| `limitations` (new tuple) | notes on 6 | known defects and gaps, e.g. "holdings = latest release only", "EIA-ONCONFLICT bug" | S | Datasheets "known errors", AWS DDQ |
| `status_public` | schedule-derived only | also demoted when the producer never succeeded; phantom entries `archival`, retired ones removed | B | Unity certified/deprecated; DataHub deprecation |
| `origin` | no curated value | `ORIGINS += "curated"` for hand-compiled or seed lists | B | Datasheets "collection process" |
| `pii_class` | 9 wrong | at least the maximum column PII (CI lint) | B | AWS sensitive-info declaration |
| Rights block: `license`, `redistribution`, `attribution` | inherited defaults | mandated notice text (FRED, OpenFEMA, Census, OECD, OSM…) | S | Snowflake/AWS legal terms |
| `license_url`, `storage`, `commercial_use`, `share_alike` (new) | none | explicit values plus a `citation_url` | G | Google "license URL", DCAT `license` |
| `reviewed` + review record | 0 | reviewer, date and rights hash; invalidated when rights change | G | Unity certified tag |
| `owner` / `contact` | 'data-platform' ×150 | a named owner for Gold | G | DCAT `contactPoint` (required), AWS support contact |
| `upstream_url` | 8 | set | S | DCAT `landingPage` |
| Column dictionary (generated) | none | description, type, unit, semantic_type, PII per column; at least 80% described | S | Snowflake data dictionary, AWS schemas, DataHub schemaMetadata |
| Identifier tags / join keys | none | `semantic_type` + `normalize_sql` (cik→10-digit text) | S | DataHub glossary terms |
| Sample | admin export only | ≤20 masked rows | G | Snowflake/AWS samples, Kaggle preview |
| Lineage (up and down) | none | computed graph | S | DataHub lineage |
| Quality block | none for catalog tables | score, profile age, null % on key columns, anomalies, row trend, `data_state` | S | OpenMetadata data quality, Kaggle usability |
| Usage | none | consumers (views, endpoints, marts) | S | DataHub usage stats |
| Access paths (new) | none | endpoint, scope and a curl snippet | G | Snowflake quick-start SQL |
| JSON-LD | none | `to_jsonld()` mapper; emitted only for reviewed ga/beta | G | schema.org Dataset, W3C DCAT |

---

## 3. Specs

### Wave plan and file overlap

| | spec.py | datasets.py | rights.py | live.py | registry.py | catalog.py | new router | mirror.py | DQ services / jobs.py | marts/inputs.py, job_keys.py | main.py | migration | frontend |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **134** truth pass | ✎ fields | ✎✎✎ | ✎ PII only | ✎ views, filters | | | | | | | | | |
| **135** rights | ✎ rights fields | | ✎✎✎ | | | | `catalog_rights.py` | | | | +1 line | 0016 | |
| **136** lineage | | ✎ inputs | | | | | `catalog_lineage.py` | | | ✎ | −3 lines | 0015 | |
| **137** dictionary | | | | | | | `catalog_schema.py` | ✎ | | | +1 line | 0017 | |
| **138** quality/usage | | | | read | | ✎ quality block | | | ✎✎ | | | 0018 | status.html (sparkline) |
| **139** browser + JSON-LD | read | | | ✎ estimate | ✎ search | ✎✎ | | | | | | | catalog.html, index.html, status.html nav |

- **Wave A (parallel):** 134 ∥ 137 ∥ 138. They share no written files: 138 only imports `resolve_tables`, and 137 keys the dictionary by `(table, column)` without touching `spec.py`.
- **Wave B (after 134 merges):** 135 ∥ 136. They overlap only on `main.py` (one line each). 136's "derived_mart must declare inputs" rule lives in a test, not in `spec.py`, so it does not collide with 135.
- **Wave C:** 139. The frontend shell can start in Wave A against the existing `/catalog`; it wires schema, sample, lineage and quality as those specs land.
- **Migrations:** four branches off `0014` would create multiple heads. Revision ids are fixed now (0015 = 136, 0016 = 135, 0017 = 137, 0018 = 138) and each branch sets its `down_revision` to the previous one at merge time.
- **Hooks:** every spec needs `/spec` → `.active_spec` before touching `app/` (CLAUDE.md Step 0).

---

### SPEC_134: Catalog truth pass (apply the verified per-entry fixes)

**Type:** bug_fix · **Effort:** L (3–4 days) · **Wave A** · **Depends on:** none

**Scope**
1. **Evidence data file** `app/catalog/evidence/verification_2026-09-25.json`, holding this workflow's per-entry output verbatim, one object per key:
   - `key`, `verified_at`, `method` ("read-only live DB nexdata-api-1 + code")
   - `tables_ok`, `missing_tables[]`, `approx_rows`, `row_count_method` (`exact`|`reltuples`)
   - `proposed{primary_key, coverage_sql, coverage_from, coverage_basis, description, grain, pii_class, kind, cadence, status}`
   - `tested{coverage_result, ms}`
   - `issues{description, grain, pii, other[]}`
   - `disposition{field: "applied" | "waived:<reason>" | "deferred:<SPEC_x|BUG_x>"}`

   Proposed `inputs` go to SPEC_136 and are marked `deferred:SPEC_136`.
2. **New `DatasetSpec` fields** (all optional, validated like the existing ones):
   - `coverage_basis ∈ {period, as_of, fixed_vintage, rolling}`
   - `subtitle` (≤100 chars)
   - `keywords` (closed `KEYWORDS`)
   - `spatial_coverage`
   - `limitations: Tuple[str,...]`
   - `row_filters: Tuple[Tuple[table, predicate],...]`: read-only checked with `_WRITE_SQL`, no `;`
   - `ORIGINS += "curated"`
   - raise the description minimum from 20 to 50 characters

   Add all of these to `to_dict()`.
3. **Apply to `datasets.py`:**
   - **144 `coverage_sql` + `coverage_basis`.** Keep the guards from verification: `<= current_date` on `sec_companyfacts`, `sec_company_financials`, `fema_hma` and `epa_echo`; `least()` for `fred_series`; a filter excluding sample sources on `si_*`; `length(county_fips)=5` on NRI.
   - **146 `primary_key`.**
   - **About 110 `coverage_from`.** `sec_companyfacts` is `rolling` (3 years) and documented in `limitations`.
   - **116 descriptions and grains** rewritten as proposed.
   - **Explicit tables:**
     - `bls_series`: 6 tables; drop `bls_cpi_consumer_prices`, `bls_cps_unemployment`, `bls_ces_employment`, `bls_ppi_producer_prices`, `bls_jolts_openings`
     - `intl_worldbank`: `intl_worldbank_wdi` first
     - `fbi_crime_leoka`
     - `sec_company_filings`: `sec_10k`, `sec_10q`, `sec_8k`; drop the s1/s3/s4 patterns
     - `census_acs5`: tighten to `acs5_\d{4}_*`, and add a separate spec for the script-built `acs5_county_2023_*` / `acs5_tract_2023_demand` tables
   - **Cadence corrections:**
     - `sec_form_adv_firms` → the batch cadence
     - `treasury_debt_outstanding` → annual
     - Drewry, FBX and SCFI → weekly
     - `si_government_units` → quinquennial (as `ad_hoc` plus a note)
     - `si_opportunity_zones` and `si_nj_land_use` → `ad_hoc`
   - **Kind corrections:**
     - `afdc_ev_stations` → timeseries
     - `si_wetlands`, `si_brownfields`, `si_opportunity_zones`, `si_broadband_availability`, `github_analytics` → reference
     - `usaspending_awards` → reference
   - **Status:**
     - `archival` for every phantom, empty or unusable entry whose producer has never succeeded or is stuck. This covers the eia_*, noaa, fbi_*, and the usaspending/fema_pa/cms_hospital/treasury_debt entries until their bug fixes land.
     - `retired` only per Open decision 4.
   - **Structure:**
     - fold `sec_company_financials`'s tables into `sec_companyfacts` as `also_produced_by=dispatch:sec:financial_data`, and retire the duplicate spec
     - fold `si_3pl_{fmcsa,sec,website}_enrichment` into `si_3pl_companies` as `also_produced_by`
     - add a `census_cbp` spec (`api:census_cbp`, PK `year,naics_code,geo_level,county_fips`, open/USG) and remove it from `rollup_market_scores.tables`
     - remove `lp_collection_runs` from `lp_collection` (it is telemetry)
     - `lp_document` owned by `lp_collection`
     - `lp_gp_relationships` first in `synthetic_lp_gp_universe`
     - `row_filters` on `container_freight_index` (by provider), `job_postings` (`ats_type='synthetic'`), `three_pl_company`, and `pe_firms`/`pe_funds`/`pe_people` (`crd_number IS NULL` / legacy rows for `pe_collection`)
   - **Origin:** `curated` for `si_foreign_trade_zones`, `si_incentive_programs`, `si_certified_sites`, `si_natural_gas_infra` and `glassdoor`; `synthetic` for `si_incentive_deals` and `public_lp_strategies` (pending Open decision 5); `official` for `app_rankings`.
   - **`limitations`** carry every open code defect with a bug id (§3.7), so the catalog tells the truth before the fixes land.
4. **PII:**
   - apply the 4 raises now: `sec_13f`, `medspa_prospects`, `si_motor_carriers` (via `COLLECTOR_RIGHTS` in `rights.py`; PII field only), `osha`
   - apply the 5 lowerings only where the schema was verified to have no contact columns: `afdc_ev_stations`, `yelp_categories`, `sec_company_filings`, `public_lp_strategies`, `sam_gov_entities`
   - leave conservative classes alone (`fda`, `si_internet_exchanges`, `si_certified_sites`, `si_3pl_*`)
5. **`live.py`:**
   - `existing_tables()` returns base tables only (`relkind IN ('r','p')`), which fixes the `fred_observations` double count
   - apply `row_filters` to counts and to the fallback coverage
   - add `fred_observations` to a `KNOWN_VIEWS` allowlist that the mirror skips

**Files:** `app/catalog/spec.py`, `app/catalog/datasets.py`, `app/catalog/rights.py` (PII fields only), `app/catalog/live.py`, `app/catalog/evidence/verification_2026-09-25.json`

**Tests:** new `tests/test_spec_134_catalog_truth.py`, plus extending `test_spec_123`.
- (unit) every evidence key exists in `get_catalog()` or is marked `retired`; every `proposed` field has a `disposition`; `applied` ⇒ the spec value equals the proposal
- (unit) each `coverage_sql` passes the SELECT/read-only checks; `coverage_basis` is set whenever `coverage_sql` is set
- (unit) `derived_mart`, `entity` and `kind=geo` specs whose grain is the generic text fail
- (unit) no two specs declare the same table unless both declare `row_filters` for it
- (unit) phantom or empty in evidence (`approx_rows==0` or `missing_tables` set) ⇒ `status_public ∈ {archival, retired}`, or a `limitations` entry naming a bug id
- (integration, `RUN_INTEGRATION_TESTS`) every `coverage_sql` runs in under 2 s and returns a `date` or NULL; no pattern resolves a view; `dataset_live('fred_series').rows_total == Σ base tables`

**Out of scope:** fixing ingestors (§3.7), deleting data, and rights changes other than PII (SPEC_135).

---

### SPEC_135: Rights proposals with citations, human review workflow and report

**Type:** service + api_endpoint · **Effort:** M (2–3 days) · **Wave B** · **Depends on:** 134 (`spec.py`, `rights.py`)

**Scope**
1. **New rights fields** on `SourceRights` and `DatasetSpec`, each a closed vocabulary:
   - `license_url`
   - `storage ∈ {permitted, ttl_24h, ttl_30d, forbidden}`
   - `commercial_use ∈ {permitted, conditional, forbidden, unknown}`
   - `share_alike: bool`
   - `citation_url`

   `to_dict()["rights"]` adds these plus `notes`; today `SourceRights.notes` is dropped (`spec.py:150-156`).
2. **Evidence file** `app/catalog/evidence/rights_research_2026-09-25.json`: the rights lens's `data` array as-is. Each row has source, current, proposed, `citation_url`, `quote`, `confidence`, `caveats`, and `fetch_status` (`fetched` | `403` | `dns_fail` | `search_excerpt`).
3. **Apply rule: tighten now, loosen by proposal only.**
   - **Tightened directly in `rights.py`.** This is safe because `reviewed=False` already makes the effective value `internal_only`; these changes keep the declared value honest for the day review happens.
     - `hifld`: restricted, origin `scraped`, substation layer flagged
     - `cms_medicare_utilization`: restricted with a CPT © AMA note
     - `courtlistener`: restricted
     - `intl_imf`: restricted
     - site_intel family default → `internal_only`, with explicit `_usg(agency)` entries for the 25 federal collectors (list in the lens `data`)
     - `nrel_resource` and `afdc`: license text changed to "NREL open data, credit required"
     - GJF → `internal_only`
     - `storage` set on Yelp (and the medspa/vertical derived tables) `ttl_24h`, FRED `forbidden` (as retrieved from FRED), M5 and Kalshi `forbidden` with `commercial_use=forbidden`, NZA `forbidden`, PeeringDB `commercial_use=forbidden`, LoopNet `forbidden`, Google Places `ttl_30d`, Freightos `commercial_use=conditional`
     - `si_utility_rates` attribution adds EIA
     - `si_zoning_districts` note for the NJ Sussex rows
     - NOAA WMO Res. 40 note
   - **Mandated attribution text applied:** FRED, OpenFEMA, Census API non-endorsement line, OECD adaptation, Epoch citation, OSM ODbL, Freightos link.
   - **Proposals only** (shown in the report, not applied): OSM → attribution with share_alike; DUNL → maybe attribution; FRED's 23 public-domain series → re-source to `open` (a separate ingestion change); Tranco and Redfin kept restricted pending written permission.
   - `reviewed` stays **False everywhere**.
4. **Review workflow. The code remains the source of truth; the DB is the audit trail.**
   - Table `catalog_rights_review` (migration 0016): `id, dataset_key, rights_hash, decision ∈ {approve, reject, needs_changes}, reviewer_principal, reviewed_at, citation_url, evidence_quote, notes`.
   - `rights_hash` = sha256 of the canonical rights block (license, license_url, redistribution, attribution, storage, commercial_use, share_alike, pii_class, origin).
   - `app/catalog/rights_reviewed.py` holds a checked-in `REVIEWED = {key: (rights_hash, review_id)}`. `rights_for()` sets `reviewed=True` only when the hash matches the current block, so any change to the rights re-opens review. `python -m app.catalog.rights_review --emit` writes the approvals into this file, and a human commits it.
   - Endpoints in a new `app/api/v1/catalog_rights.py`:
     - `GET /catalog/rights/queue` (user-read): unreviewed specs ranked PE/entity pack first, then the §1.7 public-domain candidates, then the rest; each row carries the proposal, citation, confidence and a diff against the current block
     - `POST /catalog/{key}/rights/reviews` (admin): records a decision against the current hash; 409 if the hash changed
     - `GET /catalog/rights/report?format=json|md`: counts by redistribution, storage and commercial_use; mismatches; proposals; stale reviews (hash drift); datasets holding rows whose `storage=forbidden`, with `reltuples`
5. **Export guard:** `export_policy.is_exportable` (`app/core/export_policy.py:86`) also refuses `storage=forbidden` or `commercial_use=forbidden` datasets, even for admins, unless an override flag is set.

**Files:** `app/catalog/rights.py`, `app/catalog/spec.py` (rights fields and `to_dict`), `app/catalog/rights_reviewed.py` (new), `app/catalog/rights_review.py` (new CLI and service), `app/api/v1/catalog_rights.py` (new), `app/core/export_policy.py`, `app/core/models.py` (the review model), `alembic/versions/0016_catalog_rights_review.py`, `app/main.py` (+1 router), the evidence JSON

**Tests:** `tests/test_spec_135_catalog_rights.py`
- every proposal row has `citation_url` and `quote`
- **monotonicity:** for every key, the new declared redistribution is at least as strict as the pre-PLAN_088 snapshot, except keys listed in an explicit `LOOSENED_WITH_REVIEW` set (empty in this spec)
- `reviewed` is True only with a matching `REVIEWED` hash, and changing any rights field flips it back to False
- ga/beta still requires `reviewed` (spec.py:109-111)
- queue ordering
- a POST with a stale hash returns 409
- `is_exportable` is False for `storage=forbidden`
- the report renders with no DB (offline counts) and with the DB (integration)

---

### SPEC_136: Lineage graph (declared + observed + views) and retiring `lineage_service`

**Type:** service + api_endpoint · **Effort:** M (2 days) · **Wave B** · **Depends on:** 134 (`datasets.py`)

**Scope**
1. **Inputs match the SQL.** `app/marts/inputs.py` `PE_MART_STAGE_INPUTS` / `ENTITY_STAGE_INPUTS` are **derived from** `DatasetSpec.inputs`, not maintained by hand. Corrections:

   | Spec | inputs |
   |---|---|
   | `pe_firms_sec` | `sec_adv_roster, entity_master`; drop `sec_iapd_feed` |
   | `pe_funds_sec` | `sec_form_d, sec_adv_private_funds, sec_adv_schedule_d, sec_adv_roster, pe_firms_sec` |
   | `pe_people_sec` | `sec_form_d, sec_adv_roster, pe_firms_sec, pe_funds_sec` |
   | `sec_adv_private_funds` | `sec_adv_schedule_d` |
   | `entity_cik_crd_bridge` | whatever the SQL scan finds (declared `entity_source_records` vs the asserted `sec_13f`, `sec_adv_roster`); the test decides |

   Also apply the verified inputs for `entity_source_records`, `entity_master`, `agentic_portfolios`, `medspa_prospects`, `vertical_prospects`, `rollup_market_scores` (`census_cbp`, `irs_soi`), `public_lp_strategies`, `si_3pl_companies` (now with `also_produced_by`), and `synthetic_private_financials` (`sec_companyfacts`, currently a `NON_DATASET_SOURCES` entry, noted only).
2. **SQL-reference test.** `tests/test_spec_136_lineage.py` scans `FROM`/`JOIN` in `app/marts/*.py` and `app/entities/feeds.py`. Every table a producer reads must belong to a declared input or to the dataset itself. Every `derived_mart` or `entity` spec must declare inputs, and every input key must exist.
3. **Computed graph** in `app/catalog/lineage.py`, computed on read and cached 60 s. It adds no new tables.
   - Nodes: datasets, producers, consumers.
   - Edges:
     - `declared` (spec.inputs)
     - `observed` (the latest successful `core.mart_build.inputs` per mart, giving release keys)
     - `produces` (spec.producers)
     - `reads` (`pg_depend` on views: `public_company_financials`, `fred_observations`, `workbench.v_*`, `lp_strategy_quarterly_view`)
     - `consumes` (from `app/catalog/usage.json` if SPEC_138 has landed; optional)

   Routes in a new `app/api/v1/catalog_lineage.py`: `GET /catalog/lineage` (full graph) and `GET /catalog/{key}/lineage?direction=up|down&depth=N`. Impact analysis is a downstream walk joined to the SPEC_124 verdicts.
4. **Find out why `core.mart_build` is empty.** SPEC_126a's `finish_build` is either not called or writes to a different schema. Fix it, or file a bug with evidence. Observed edges depend on this.
5. **Retire the old lineage stack.** Delete `app/core/lineage_service.py` and `app/api/v1/lineage.py`, and unregister them (`main.py:68,1349,1644`). Migration 0015 moves `lineage_nodes`, `lineage_edges`, `lineage_events`, `dataset_versions` and `impact_analysis` to a `quarantine` schema (the PE-rebuild rule is quarantine, not delete).
6. **Backfill `ingestion_jobs.dataset_key`.**
   - Add the missing producer aliases in `app/catalog/job_keys.py`: `job_postings` (162), `international_econ_{oecd,worldbank,bis,imf}`, `census_bfs`, `usda`, `form_d`, `form_adv`, `app_rankings`, `web_traffic`, `opencorporates`, `epa_echo`, `epa_ghg`, `dot_grants`, `ffiec_banks`, `cms_hospitals`, `census_cbp`.
   - Migration 0015 then sets `dataset_key = dataset_key_for_job(source, config)` in batches of 500 under the lock_timeout/retry pattern from 0014.
   - Rows mapping to several datasets (`job:pe_mart_build`) stay NULL.
   - This reverses 0014's "no backfill" decision (Open decision 10).

**Files:** `app/catalog/datasets.py` (inputs only), `app/marts/inputs.py`, `app/catalog/job_keys.py`, `app/catalog/lineage.py` (new), `app/api/v1/catalog_lineage.py` (new), `app/main.py`, `app/core/lineage_service.py` (deleted), `app/api/v1/lineage.py` (deleted), `alembic/versions/0015_lineage_quarantine_dataset_key_backfill.py`, possibly `app/marts/build_ledger.py`

**Tests:**
- the SQL-reference test; the stage inputs equal the spec inputs
- graph: `downstream('sec_form_d') ⊇ {pe_funds_sec, pe_people_sec, entity_source_records}`; no cycles; depth limit respected
- alias coverage: a simulated resolve over a fixture of the 322 unresolved `source` strings resolves at least 95%
- (integration) the backfill is idempotent; the view edges include `public_company_financials → sec_financial_facts`
- `/lineage` returns 404 (the router is gone)

---

### SPEC_137: Column dictionary, `/catalog/{key}/schema`, masked sample, join-key identifiers

**Type:** service + api_endpoint · **Effort:** L (3 days) · **Wave A** · **Depends on:** none (reads spec tables through `resolve_tables`)

**Scope**
1. **`app/catalog/columns.py`.**
   - `ColumnSpec(table, name, pg_type, nullable, description, unit, example, semantic_type, pii ∈ PII_CLASSES, source ∈ {curated, upstream, bulk, metadata, model, pg_comment, glossary}, confidence)`
   - Closed `SEMANTIC_TYPES`: cik, crd, lei, cusip, figi, npi, ein, uei, duns, naics, sic, fips_state, fips_county, zip5, iso_country, us_state, accession_number, ticker, series_id, latitude, longitude, period_date, amount_usd, pct
   - Each type carries a `canonical_format` and a `normalize_sql` template:
     - cik: `lpad(ltrim({col}::text,'0'),10,'0')`
     - crd: `ltrim({col}::text,'0')`
     - fips_county: `lpad({col}::text,5,'0')`
     - naics: `left(regexp_replace({col},'\D','','g'),6)`
   - Import fails on an unknown type or PII value.
   - Aliases: crd ↔ crd_number, zip ↔ zip_code, and so on.
2. **Generator** `python -m app.catalog.dictionary_build`. It merges sources in this precedence order:
   1. curated `app/catalog/columns_curated.py`
   2. `census_variable_metadata`
   3. bulk `COLUMNS`, normalised across the three shapes (`sec_13f/parse.py:246`, `sec_insider/parse.py:40`, `sec_form_d/source.py:213`)
   4. `metadata.py` description dicts and DDL `--` comments
   5. `Column(comment=)` and inline `#` comments, via tokenize over `MODEL_MODULES` (`tables.py:36`)
   6. existing `col_description`
   7. glossary rules (27 rules, which also assign `semantic_type`)

   Types come from `information_schema` (or from the model when the table is absent). The output is the sorted, diffable `app/catalog/columns.generated.json` plus a `dictionary_hash`. Seed `columns_curated.py` with upstream text for SEC Form D, 13F and insider (the published readmes), Treasury (`meta.labels`) and Census `variables.json`, at `source='upstream'` with an `upstream_url`.
3. **Mirror** (in `mirror.py`, same idempotent, never-delete pattern as `sync_dataset_registry`):
   - a `catalog_column` table (migration 0017)
   - `COMMENT ON COLUMN` only where the text differs, under `SET lock_timeout='2s'`, skipping tables that fail
   - existing comments are adopted, not overwritten (the two 13F comments)
4. **Routes** in a new `app/api/v1/catalog_schema.py`, user-read:
   - **`GET /catalog/{key}/schema?table=`** returns per table: `exists`, `row_estimate`, declared and derived natural key (from `pg_index.indisunique`), and columns with type, nullable, description, unit, semantic_type, PII, source, plus `null_pct`/`distinct_count` from the latest `data_profile_columns`. Examples come from profile `top_values` for `pii='none'` columns only. The ETag is `dictionary_hash`. Declared tables that do not exist return `exists:false`.
   - **`GET /catalog/{key}/sample?table=&limit≤20`**:
     - only columns present in the dictionary (new, undocumented columns are hidden by default)
     - `SET LOCAL statement_timeout=3000`, no count
     - `ORDER BY` the natural key; `TABLESAMPLE SYSTEM` when `reltuples>1M`
     - cached 10 min
     - masking for non-admin callers: `personal` → NULL; `business_contact` → partial mask (email `j***@domain`, phone last 4); `people.email` where `email_confidence='inferred'` is **always** NULL, even for admins
     - 403 for non-admin callers when the declared redistribution is `restricted` or `storage=forbidden`
     - an `X-Dataset-Attribution` header
   - **`GET /catalog/{key}/joins`** and **`GET /catalog/join-keys?semantic_type=`**: other datasets that share an identifier, ranked by how specific it is (cik, crd, npi, lei and cusip rank above naics and zip; `state` is excluded), with join SQL ready to run using `normalize_sql` on both sides.
   - **`GET /catalog/columns?q=&semantic_type=&pii=`**: search across datasets.

**Files:** `app/catalog/columns.py`, `columns_curated.py`, `dictionary_build.py`, `columns.generated.json`, `identifiers.py` (all new); `app/catalog/mirror.py`; `app/api/v1/catalog_schema.py` (new); `app/core/models.py` (the `CatalogColumn` model); `alembic/versions/0017_catalog_column.py`; `app/main.py` (+1 router)

**Tests:** `tests/test_spec_137_column_dictionary.py`
- offline regeneration equals the checked-in JSON (drift gate)
- unknown `semantic_type` fails
- a column whose name matches `email|phone|fax|first_name|last_name|signature|street|birth` must have `pii != 'none'`
- dataset `pii_class` ≥ the maximum column PII (this would have caught `sec_13f`)
- the sample masks `personal` for non-admin callers, always nulls inferred emails, and returns 403 on restricted datasets
- (integration) the CIK join fixture: `core.identifier` joined to `sec_filers` through `normalize_sql` matches all 1,000 of 1,000
- (integration) the schema route answers in under 300 ms for `sec_13f`
- per-dataset coverage percentage reported; the target is ≥80% for the PE/entity pack (a gate) and ≥56% overall (tracked, not a gate)

**Not in scope:** Alembic-only DDL (the review's broader SPEC_137 item); normalising `core.identifier` on write (Open decision 9).

---

### SPEC_138: Per-dataset quality and usage signals

**Type:** service · **Effort:** M (2–3 days) · **Wave A** · **Depends on:** none (the completeness score is richer once 134 lands)

**Scope**
1. **DQ driven by the catalog.**
   - Replace `DatasetRegistry.ingested()` as the selector in `profile_all_tables`, `compute_daily_snapshots` and `_run_quality_gate` with "catalog tables that exist" (`resolve_tables`).
   - Add a post-load hook called from the bulk runner (per `raw.source_release` load), from `build_ledger.finish_build` on success and from the `entity_resolve` executor, passing `spec.tables`. It is advisory for now.
   - `_run_quality_gate` picks tables through `job.dataset_key` → `spec.tables`, not through `.first()` on the source.
2. **Bug fixes:**
   - read the previous profile snapshot **before** profiling (the row-delta check currently compares a snapshot with itself)
   - advisory lock via `pg_try_advisory_lock(hashtext(:t))`
   - freshness component taken from the SPEC_124 verdict and `slo_lag_hours`, not the 168 h decay
3. **Scheduled profiling.** An APScheduler job profiles each catalog table whose profile is older than its cadence. It uses `TABLESAMPLE` above 1M rows and skips tables whose `reltuples` is unchanged.
4. **`catalog_table_stats`** (migration 0018): a daily `(date, table, est_rows, n_live_tup)` from `pg_stat_user_tables`, which needs no `count(*)`. It drives a row-trend sparkline and a row-drop alert that does not depend on profiling.
5. **Quality block on `GET /catalog/{key}`**, in `catalog.py`:
   - `data_state` computed live: `phantom` | `empty` | `populated` | `defective` (key columns all NULL, from profile `null_pct`) | `seed_contaminated` (rows where the `source` column matches `%sample%|%_seed|nrel_reference`)
   - latest `dq_quality_snapshots` score and components
   - profile age and a staleness flag (older than 2× cadence)
   - `null_pct` on `primary_key` columns
   - open `dq_anomaly_alerts`
   - 30-day row trend
   - **metadata completeness score**: the share of description ≥50 characters, subtitle, keywords, coverage_sql, coverage_from, primary_key, upstream_url, SLO, attribution, `license_url`, column-doc percentage, and a named owner
6. **Usage.**
   - `app/catalog/usage.json`, generated in CI by a static `FROM`/`JOIN` scan of `app/api/v1`, `app/services`, `app/graphql` and `app/reports`
   - `pg_depend` view dependencies read at runtime
   - exposed as `consumers` in the detail response and consumed by the SPEC_136 graph
   - flags catalog tables that are read but not declared by any spec (for example `sec_company_metadata`)

**Files:** `app/services/data_profiling_service.py`, `app/services/quality_trending_service.py`, `app/services/data_quality_service.py`, `app/api/v1/jobs.py` (`_run_quality_gate`), the bulk runner, `app/marts/build_ledger.py` (hook call only), the scheduler setup, `app/catalog/quality.py` (new), `app/catalog/usage_build.py` + `usage.json` (new), `app/api/v1/catalog.py` (quality block), `alembic/versions/0018_catalog_table_stats.py`, `frontend/status.html` (sparkline; optional)

**Tests:** `tests/test_spec_138_catalog_quality.py`
- the selector includes `form_d_filings`, `pe_firms` and `core.entity`
- the row delta uses the prior snapshot (fixture)
- the lock key is identical across two subprocesses
- freshness follows the verdict
- `data_state` classification fixtures: `eia_steo` phantom, `fbi_crime_*` empty, `usaspending_awards` defective, `substation` seed_contaminated
- the completeness score is deterministic
- `usage.json` drift gate

---

### SPEC_139: Catalog browser page, search/facets API and JSON-LD mapper

**Type:** api_endpoint + frontend · **Effort:** M (2–3 days, plus 0.5 day for JSON-LD) · **Wave C** · **Depends on:** 137 (schema/sample), 136 (lineage), 138 (quality). Can start against stubs.

**Scope**
1. **API** in `catalog.py` and `registry.py`:
   - `response_model`s (`CatalogEntry`, `CatalogList`, `CatalogDetail`, `LiveStats`, `Rights`) with field descriptions taken from `spec.py:47-74`
   - `GET /catalog/search?q=`: tokenised AND with prefix matching; weights: key/name 5, tables 3, columns 2, other text 1; returns `matched[]` and a highlight; the in-memory index covers 150 specs and about 3,242 columns
   - `GET /catalog/facets`: counts per value for kind, source, status_public, effective and declared redistribution, pii_class, origin, cadence, keywords, `data_state` and freshness bucket
   - multi-value filters and sorting on `GET /catalog`
   - `GET /catalog/{key}?live=estimate|exact`: estimate is the default (`reltuples` plus the SPEC_124 clocks); exact is the admin refresh path
2. **`frontend/catalog.html`** (static, no build step), following the `status.html` conventions: `:root` tokens and dark mode, `/js/auth.js`, `esc()`, and a testable `<script id="catalog-core">` block tested the same way as SPEC_125.
   - **Hash routing** (`#/`, `#/d/{key}`, `#/p/pe-entity`), because nginx `try_files … /index.html` swallows paths (`frontend/nginx.conf:9`).
   - **Facet rail**, with state kept in the hash.
   - **Cards:** name, subtitle, badges for kind, rights (both declared and effective, e.g. "Open (public domain), pending review → internal only today"), PII, origin and `data_state`; `~rows`; coverage-through date; freshness dot.
   - **Detail tabs:** Overview, Coverage & freshness, Schema, Sample, Lineage (inline SVG DAG), Quality, Rights (copy-citation button), How to access, Related (joins).
   - "Show all (internal preview)" is the default, because 0 datasets are ga/beta.
   - **Nav:** a Catalog link next to Status (`index.html:2042`, `status.html:258`); status rows link to `#/d/{key}`; `/sources` rows get a `dataset_key` link.
3. **PE/entity data product page** (`app/catalog/products.py`, `GET /catalog/products[/{key}]`): 11 member specs plus their SEC inputs; the identifier spine (CIK↔CRD); overall rights = the most restrictive member.
4. **JSON-LD (cheap option only).**
   - A pure `app/catalog/jsonld.py` `to_jsonld(spec)` mapping to a schema.org `Dataset`: name, description, keywords, temporalCoverage, spatialCoverage, license (URL), creator/publisher, `isAccessibleForFree=false`, `distribution` for the access paths, `variableMeasured` from the dictionary.
   - `GET /catalog/{key}/jsonld` stays behind auth.
   - **Nothing is emitted publicly** until at least one spec is ga/beta with reviewed rights.
   - Use W3C DCAT only if it comes free; skip DCAT-US, whose bureauCode and programCode fields are federal-only.
   - The contract test doubles as a completeness gate for Gold.

**Files:** `app/api/v1/catalog.py`, `app/catalog/registry.py`, `app/catalog/live.py` (estimate mode), `app/catalog/products.py` (new), `app/catalog/jsonld.py` (new), `frontend/catalog.html` (new), `frontend/index.html`, `frontend/status.html` (nav links only)

**Tests:** `tests/test_spec_139_catalog_browser.py`
- ranking: `q="form d"` puts `sec_form_d` first; `q="cik"` hits via column names
- facet counts sum to the filtered total
- `live=estimate` makes no `count(*)` calls (mocked)
- node tests for the `catalog-core` helpers (filter, facet counts, freshness bucket, hash round-trip, `esc`)
- `to_jsonld` validates the required schema.org fields for the PE pack and raises on a description under 50 characters
- the page loads with `auth.js` and handles a 401 redirect

---

### 3.7 Follow-on data bug fixes (separate `bug_fix` specs; referenced from `limitations`)

| Bug id | Defect | Location |
|---|---|---|
| BUG-EIA-ONCONFLICT | ON CONFLICT with no matching unique index; tables are never created; jobs time out after 6.4 h | `app/sources/eia/metadata.py`, `ingest.py:239,405,561,709,853` |
| BUG-FEMA-PA | wrong OpenFEMA field names; unique index includes a NULL column; 2×50k cap | `app/sources/fema/metadata.py` `parse_pa_projects` |
| BUG-ZERO-ROW-SUCCESS | fbi_crime, bea_gdp_industry and cms_hospital report success with 0 or all-NULL rows | ingest base: treat 0 rows or all-NULL keys as a failure |
| BUG-CMS-IDEMPOTENT | utilization needs `data_year` plus a unique constraint (2× duplicates); drug pricing needs `mftr_name` | `app/sources/cms/metadata.py` |
| BUG-ELEC-PRICE-NULL | NULL `period_month` counts as distinct, giving 24× duplication | `uq_electricity_price` |
| BUG-NRI-FIPS | 332 three-digit fips colliding; 88 rows with NULL state and score | `risk/fema_collector.py:262` |
| BUG-INTL-UNIQUE | OECD/IMF/BIS have no natural unique constraint; OECD KEI has 1,577 duplicate groups | `app/sources/international_econ` |
| BUG-REDFIN-KEY | unique index leaves out the region | realestate metadata |
| BUG-TREASURY-DEBT / BUG-USASPENDING-MAP / BUG-FCC-DATADATE | field mappings drop every value | respective ingestors |
| BUG-FDIC-COLNAMES | `savession`, `uninession`, … (bad find/replace) | `app/sources/fdic/metadata.py` |
| BUG-SEISMIC-FABRICATED | `pga = magnitude*0.1`; `fault_line` misused | `usgs_earthquake_collector.py:195-290` |
| BUG-SYNTH-LP-OVERWRITE | the synthetic upsert overwrites real `lp_fund` AUM and tier | `app/services/synthetic/lp_gp_universe.py:291-297` |
| BUG-NOAA-ZOMBIE | the scheduler enqueues NOAA jobs that zombie every day | batch TIERS |
| BUG-STALE-SCHEDULES | treasury (3 endpoints), FRED (5 categories), cftc_cot and bea_regional are not refreshing | `batch_service.py` TIERS |

---

## 4. Open decisions for the user

1. **Spec numbers.** Keep SPEC_134–139 as used here and renumber the review's 134–136 (firm dedup, queue hardening, hosted runtime) to 141+? Or file this plan as 141–145 (with SPEC_137 kept for the dictionary either way)?
2. **Commercial posture** (review Q1, never answered). This decides whether storage-forbidden data gets purged, what the PII policy is, and whether Gold, sample and JSON-LD matter this quarter.
3. **Purge or keep data whose terms forbid storing it:** Yelp (400 rows, plus 5,396 medspa prospects derived from it), NZA (3,695), PeeringDB (216), LoopNet (re-count first; the lenses disagree: 60 vs 0), M5 (32k), Kalshi (25), and FRED as retrieved (re-source 23 series from the originating agencies, and keep UMCSENT restricted or drop it). SPEC_135 only flags these; deletion is your call.
4. **Retire or keep as archival** the 14 never-run phantom specs (eia_* ×5 until BUG-EIA is fixed, noaa, afdc, yelp_categories, us_trade_summary, bts_vmt, fda, sam_gov, opencorporates, vertical_prospects), plus `sec_form_adv_legacy`, `si_warehouse_listings` (a LoopNet scraper, which conflicts with CLAUDE.md), `glassdoor`, `app_rankings` and `web_traffic`. The review says "retire by default". Which of these should be revived?
5. **Fabricated or seed data:** quarantine rows (move them to the `quarantine` schema), delete them, or only flag them via `data_state`? This covers `si_seismic_hazard` pga values, `si_incentive_deals`, `public_lp_strategies` and the scattered `*_sample` rows. Also: is `public_lp_strategies` synthetic? No generator was found in the repo.
6. **`si_motor_carriers` PII:** classify as `personal`, or filter owner-operators out at ingest and keep `business_contact`?
7. **Lowering PII classes:** apply the 5 schema-verified lowerings now (the default in SPEC_134), or keep everything conservative until the column dictionary gives column-level PII?
8. **Rights review.** Who is the reviewer of record? Is code-truth plus a committed `REVIEWED` hash acceptable, or do you want the DB flag to drive `effective_redistribution` directly? Also: should the most-restrictive tightenings go into `rights.py` now (the default), or stay as proposals?
9. **CIK canonical form:** normalise `core.identifier` to 10-digit text on write, which means a data migration of about 123.5k rows plus a change to `app/entities/resolve.py:145`? Or only expose `normalize_sql` for joins?
10. **`dataset_key` backfill:** this reverses 0014's documented "no backfill" decision. Approve?
11. **Legacy lineage tables:** quarantine now and drop in a later migration (the default), or drop straight away? No reader exists.
12. **Catalog structure:** approve folding `sec_company_financials` and the three `si_3pl_*` enrichment specs into `also_produced_by` (net −4 specs), adding `census_cbp` and a script-built ACS county/tract spec (+2), and decide which spec owns `sec_form_adv` (shared by `sec_adv_roster` and `sec_form_adv_firms`, which have different PII classes).
13. **Which data bugs from §3.7 come first.** Suggested order: ZERO-ROW-SUCCESS and SYNTH-LP-OVERWRITE (they protect everything else), then EIA, then the idempotency set (CMS, electricity_price, INTL, Redfin), then FEMA-PA.
14. **First ga/beta candidates**, once rights are reviewed: `fdic_bank_financials` and `fema_disaster_declarations`, which are healthy and fresh. Or hold until the PE/entity pack reaches Gold?