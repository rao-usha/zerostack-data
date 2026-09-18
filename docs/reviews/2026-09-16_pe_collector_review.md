# Nexdata PE data collection: architecture review and rebuild-or-refactor call

*Review date 2026-09-16. Read-only. Inputs were eight cluster audits, an adversarial verification of each, the missing-source catalog and the live row-count snapshot. Where an auditor and its verifier disagree, I use the verifier's finding and say so.*

---

## 1. Bottom line

**Decision: a hybrid. Keep and harden the platform, replace how PE data gets collected, and don't rewrite the repo from scratch.** The core parts work and would cost months to rebuild: the SKIP LOCKED job queue, worker heartbeats and cancellation, `BaseAPIClient`, the parameterized upserts, the Postgres token bucket and the PE/people schemas. What fails is the approach sitting on top of them. Nexdata tries to reach PE scale by crawling one firm, company or filing at a time, starting from a hand-typed list of about 90 firms. That approach can't get past a few thousand rows, and most of what it did write is wrong: 94% of `pe_deals` are misread 8-K filings, 59% of `pe_portfolio_companies` are 13F public stock positions, every `pe_company_financials` row is demo data, and 89% of `lp_key_contact` is regex junk.

The data the goal needs is published free as bulk files: SEC Form D, 13F, insider filings, Form ADV, EDGAR indexes, CMS ownership files and DOL 5500. A sibling project, `wildcard-workbench`, already has correctly designed connectors for most of these. It filters them down to a small universe, and nexdata never reads what it loads.

The plan in one line: **bulk-first ingestion into raw → staging → core layers, one identifier-based entity master, and PE tables rebuilt as derived marts.** The prerequisites are running the worker queue as the only execution path, adding migrations, and hard-quarantining the fabricated and demo data.

Nothing through the queue has succeeded since 2026-04-16, so the first 72 hours go to getting a worker running and fixing about 20 lines of silent-data-loss bugs. The first two weeks should take the empty SEC tables from 0 to millions of rows.

---

## 2. Current state scorecard

| Cluster | Audit verdict → verified verdict | **My call** | PE value if working | Data actually produced | Key confirmed problems |
|---|---|---|---|---|---|
| **Core framework** | refactor → refactor | **Refactor** (keep the core, merge three execution paths into one) | Enables everything else | Queue: 268 successes total, **none since 2026-04-16**. `ingestion_jobs`: 1,806 blocked, 131 pending, 63 stuck running | No live worker. The cleanup job leaves `ingestion_jobs` rows orphaned. PE and people crons run inside the API process. Stale-job reset can run a job twice. Checkpoints and site_intel watermarks are dead code. Rate limiting fails open. No migrations. No row-level provenance |
| **PE collection** | rewrite → refactor | **Replace the collection front end, keep the model and persister.** Discovery must come from bulk ADV and Form D | Very high: firms, funds, deals, portfolio | 105 firms (29 with a CIK, **0 with a CRD**); 40 funds (27 are synthetic "13F Holdings", 6 demo); 788 deals (740 are 8-K false positives); 1,814 companies (1,069 are 13F positions, 170 are URLs or button labels); financials, valuations and performance are 100% demo | No discovery step. 8-K filers saved as deal targets. 13F holdings saved as portfolio companies, with the security class in `ticker`. The `role_type` column exists in the model but not the DB. One error rolls back a whole phase. Matching is exact-name only |
| **SEC / regulatory** | rewrite → rewrite | **Rewrite the nexdata-side SEC ingestion as bulk loaders, reusing the workbench connectors** | Highest: the authoritative source | XBRL statements have 97k rows **with every value filed under the wrong year**. `sec_8k`, `sec_form_adv`, `sec_form_adv_personnel`, `form_d_filings` and `insider_transactions` are all **0**. `form_adv_advisers` holds 10 fabricated sample rows | XBRL keys on filing fy/fp. Filings DDL uses MySQL syntax. Form ADV hits a guessed IAPD API, fails on table ownership, and is dispatched to the wrong function. Form D fetches one page. The API registry throttles SEC to 10 req/min. Eight or more different User-Agents |
| **People** | rewrite → rewrite (front end) | **Hotfix now, then swap the crawler for structured SEC and ADV people data.** Keep the schema, dedup and email inference | High: GP partners, management teams, boards | 2,231 people at 129 companies, mostly JPMorgan and Prudential subsidiaries. Leadership changes, experience, education, board seats and insider data all 0. 2 new people in August | Every detected leadership change dropped by a `.value` bug. A dedup FK violation leaves each batch's shared session unusable. Concurrent coroutines share one Session. The target list is 233 non-PE companies. Form 4 fetches the rendered HTML instead of the XML. All provenance stored as `website` |
| **LP / family office** | rewrite → refactor | **Refactor. Delete the regex contact extractor and the headline-based deal extractor.** | High: LP→GP commitments | `lp_gp_commitments` **0**. 3,174 contacts, about 89% junk. 4,464 "LP 13F" rows (CalPERS rows are Berkshire Hathaway's holdings at 1000x value). 26 family office "investments" that are headline fragments. Nothing since 2026-03-11 | No collector emits commitments. The persist step drops unknown item types but counts them as inserted. The CAFR parser is never called. First-token ILIKE matching attaches data to the wrong LP. 13F CIKs wrong and values inflated. The FO runner drops `team_member` items |
| **Company signals** | refactor → refactor | **Refactor a small core, freeze or delete the rest** | Medium: hiring, gov contracts, EPA, healthcare | epa_echo 1.07M, cms about 98k (about 50% duplicates), nppes 40k, job_postings 19k (about 20% generic-scraper junk), usaspending 10k (key columns 100% NULL). All stale since March | No link to PE companies (`er_entity_link` = 0). Generic scraper IDs built on per-process `hash()`. Scorers query columns that don't exist and fail silently. USPTO dispatch points at a missing function. OSHA URL dead. **Yelp API key stored in plaintext in `ingestion_jobs.config`** |
| **Entity resolution** | rewrite → refactor | **Build one new component: an identifier-graph master (port the workbench resolver) and delete `app/core/entity_resolver.py`.** Fix identifier capture first | Critical: the join layer for everything | `canonical_entities` 4 rows. The working ER is `workbench.er_*`, which covers 5.4% of records and contains no PE sources | Four company tables and two person tables share no key. People matched globally by name, so "Investor Relations" is a person linked to 4 firms. CUSIP thrown away. The nexdata resolver is dead code and doesn't scale |
| **Macro / sector** | refactor → refactor | **Freeze most of it. Keep CBP, IRS SOI, QCEW, FRED and FDIC. Delete the fabricated logistics data** | Low to medium (market sizing, roll-up context) | Biggest tables in the DB: fdic 1.67M, cbp 1.12M, irs_soi about 990k, trade 546k. site_intel has about 3.5M real geo rows | **site_intel logistics collectors generate random data** (for example Swift, DOT 1234567, with random safety scores). Year injection overwrites pinned vintages with 2026. The deal-environment scorer UNIONs a table that doesn't exist, so every input is None. CFTC and EIA broken |

**Overall:** of the 544 tables, the ones holding real, correct, PE-relevant data at volume are essentially `epa_echo`, CBP, IRS SOI, FDIC and a few of the workbench tables. None of those is core PE data.

---

## 3. Confirmed critical and high defects

These all survived verification. Line numbers are the verifier's corrections where the two disagreed.

### Silent data loss and corruption (fix before anything else)
| # | Defect | Location | Evidence |
|---|---|---|---|
| D1 | `PEFirmPeople.role_type` exists in the ORM model but not in the DB, so every `db.query(PEFirmPeople)` fails and the firm org endpoint returns 500 | `app/core/pe_models.py:1013`; queried at `app/sources/pe_collection/persister.py:551-558, 658-665, 705-712`; raw SQL at `app/api/v1/pe_firms.py:774,786`. Missing from the ALTER list at `app/core/database.py:84-93` | Column absent in information_schema. *Verifier correction: the people stall began 2026-03-12, before the 2026-03-25 commit, so this blocks recovery but isn't the original cause.* |
| D2 | One failed item rolls back the whole persist phase, and the stats still count the discarded items as saved | `app/sources/pe_collection/persister.py:180-196` (commits only at 125-129 and 136-140) | No `begin_nested` anywhere |
| D3 | Every detected leadership change is dropped: `.value` is called on a string enum | `app/sources/people_collection/orchestrator.py:929, 943` (enum config at `types.py:192-193`) | 4,890 detected, `leadership_changes` = 0 |
| D4 | Dedup deletes a `company_people` row that `reports_to_id` still references. The FK error is swallowed with no rollback, so later writes in the batch fail | `app/services/dedup_service.py:360`; `orchestrator.py:351-357` | Jobs 5298, 5380 and 5411 are marked "success" but contain the FK error |
| D5 | Concurrent coroutines share one SQLAlchemy Session | `app/sources/people_collection/orchestrator.py:450`; `app/sources/lp_collection/runner.py:94-104, 313-315`; `app/services/bio_parser_service.py:153-172`; worker at `app/worker/main.py:256` | 92 "Session's transaction has been rolled back" failures in the queue; 70 LP runs stuck |
| D6 | XBRL financial statements use the filing's fy/fp as the period key, so values land in the wrong year and Q2/Q3 hold year-to-date totals | `app/sources/sec/xbrl_parser.py:371-398, 401-437` (same helper used at 454, 519, 585) | Apple row labeled FY2025 holds FY2023 revenue of 383,285M. 23,490 of 26,389 FY rows have a period_end before their fiscal year |
| D7 | site_intel logistics collectors write random data labeled as real sources | `app/sources/site_intel/logistics/fmcsa_collector.py:157-159, 537-570`; `warehouse_listing_collector.py:81-84, 163-182`; `scfi_collector.py:198-201`; `freightos_collector.py:149-152`; `census_trade_collector.py:124-127`; `port_throughput_collector.py:112-115`; `air_cargo_collector.py:178-181`; `drewry_collector.py:230-233`; also `usda_truck_collector.py:202-205` and `incentives/goodjobs_collector.py:46` | `motor_carrier` has "Swift Transportation Co LLC", DOT 1234567, source='fmcsa'. About 2.4k fabricated rows in total |
| D8 | Demo data sits in production PE tables with "high" confidence tags | `app/sources/pe/demo_seeder.py:171-221, 550`; `app/services/pe_ecosystem_seed.py` | 100% of financials, valuations, fund_performance and cash_flows |

### Misclassification, so the data looks like PE data but isn't
| # | Defect | Location | Evidence |
|---|---|---|---|
| D9 | 8-K full-text search hits are saved as deals, with the filer as target and confidence "high" | `app/sources/pe_collection/deal_collectors/press_release_collector.py:162-183, 327-381`; `persister.py:909-940` | 740 of 788 deals ("8-K: Carlyle Group Inc.") |
| D10 | 13F positions are saved as portfolio companies inside synthetic funds, with `ticker` set to the security class and the CUSIP dropped | `persister.py:297-323, 772-823` (ticker at 783-786) | 1,069 companies, 3,621 investments, 27 funds; ticker "COM" appears 287 times |
| D11 | LP 13F: CalPERS mapped to Berkshire's CIK, substring matching ("MIT" matches "GIC Private Limited"), and values multiplied by 1000 | `app/sources/lp_collection/sec_13f_source.py:43, 230-236, 603` | CalPERS shown holding $12.5T of OCCIDENTAL |
| D12 | LP contact regex runs case-insensitively over raw HTML | `app/sources/lp_collection/website_source.py:291-316` | Only 349 of 3,174 names pass a basic name check |
| D13 | FO deal extraction stores headline fragments, and Form D issuers are treated as FO investments | `app/sources/family_office_collection/deals_source.py:176-300, 451-505` | All 26 rows are junk |
| D14 | pe_people matched globally by lowercase name, so job titles merge into single "people" across firms | `persister.py:276-286` | "Investor Relations" linked to 4 firms, "Vice President" to 3 |

### Structural blockers to volume
| # | Defect | Location |
|---|---|---|
| D15 | No firm or fund discovery; the universe is only existing `PEFirm` rows (and `TOP_PE_FIRMS` contains duplicates) | `app/sources/pe_collection/orchestrator.py:250-260`; `config.py:108` (duplicates at 145/491, 412/691, 199/636) |
| D16 | Form ADV relies on a reverse-engineered IAPD endpoint, errors are swallowed, the bulk method is a stub, and table ownership fails | `app/sources/sec/formadv_client.py:154-189, 216-245`; `formadv_ingest.py:200-207, 225-226` |
| D17 | Form ADV route sends jobs to `ingest_company_filings`; schedules use unregistered `form_adv`/`form_d` source keys | `app/api/v1/sec.py:772-781`; `app/api/v1/jobs.py:841-846`; `app/core/scheduler_service.py:852, 862` |
| D18 | Filings DDL puts `INDEX` inside `CREATE TABLE` (MySQL syntax), so 10-K/10-Q/8-K ingestion can never run | `app/sources/sec/metadata.py:91-96`; `ingest.py:165, 238` |
| D19 | Form D fetches one EFTS page, caps runs at 50 CIKs, runs inside the HTTP request, and the upsert updates only `updated_at` | `app/sources/sec_form_d/client.py:291-292`; `app/api/v1/form_d.py:326, 336`; `ingest.py:215-216` |
| D20 | PE Form D dedup key is identical for every filing of a CIK | `app/sources/pe_collection/deal_collectors/sec_formd_collector.py:135-141` (the audit cited 385-388; that was wrong) |
| D21 | Form 4 collector fetches the XSL-rendered HTML instead of the raw XML | `app/sources/people_collection/filing_fetcher.py:417-421, 640-645` |
| D22 | SEC API registry limit is 10 req/**min** while its own note says 10/sec | `app/core/api_registry.py:147` |
| D23 | No LP collector produces commitments; unknown item types are dropped but counted as inserted; CAFR parser never called | `app/sources/lp_collection/runner.py:56, 344-367`; `cafr_source.py:124-222` |
| D24 | LP resolution by first token and `matches[0]`, which creates placeholder LPs | `app/agents/fund_lp_tracker_agent.py:139-149, 180-185` |

### Platform and orchestration
| # | Defect | Location |
|---|---|---|
| D25 | The no-worker cleanup marks `job_queue` rows failed but never updates `ingestion_jobs` or promotes the batch; batches launch without checking for a live worker | `app/core/job_queue_service.py:144-202`; `app/core/batch_service.py:386`; promotion only at `app/worker/main.py:381-392` |
| D26 | Scheduled jobs, retries and PE/people crons run inside the API process; no leader lock | `app/core/scheduler_service.py:236-250`; `app/core/retry_service.py:519-527`; `app/jobs/pe_collection_scheduler.py:61-65, 130-134`; `app/jobs/people_collection_scheduler.py:405` |
| D27 | Stale reset after 2 minutes, no ownership check on final writes, no attempt limit | `job_queue_service.py:88-141`; `worker/main.py:297-302`; `models_queue.py:67-110` |
| D28 | Year watermark injection overwrites pinned vintage years | `app/core/scheduler_service.py:52-66, 122` via `app/worker/executors/ingestion.py:37` |
| D29 | Dispatcher marks jobs SUCCESS whatever row count comes back, and the watermark advances on 0-row runs | `app/api/v1/jobs.py:739-746, 861-885` |
| D30 | Distributed rate limiter fails open and has no bucket for data.sec.gov or www.sec.gov | `app/core/http_client.py:175-189`; `app/core/rate_limiter.py:645-667` |
| D31 | `BaseAPIClient` can only parse JSON, so ZIP, CSV and HTML sources bypass it | `app/core/http_client.py:352-356` |
| D32 | Site-intel watermark calls have the wrong signature, `update_watermark` doesn't exist, and checkpoints are never written | `app/sources/site_intel/base_collector.py:536-567` |
| D33 | Deal-environment scorer UNIONs the nonexistent `fred_housing_market`, so all macro inputs become None | `app/services/deal_environment_scorer.py:151-165` (also `econ_dq.py:42`, `econ_snapshot.py:44`, `macro_cascade.py:646`, `macro_sector_brief.py:117`) |
| D34 | **Security:** Yelp API key stored in `ingestion_jobs.config` | `app/api/v1/yelp.py:114, 155, 181` |

---

## 4. What to keep, and what to cut or freeze

### Keep (reusable assets)
**Platform**
- `app/worker/main.py` claim, heartbeat and drain logic; `app/core/models_queue.py`; `job_queue_service.promote_blocked_jobs`
- `app/core/http_client.py` `BaseAPIClient` and `app/core/api_errors.py` (extend to bytes and text responses)
- `app/core/rate_limiter.py` Postgres token bucket (make it fail closed; add the SEC hosts)
- `app/core/batch_operations.py` `_build_insert_sql` with `safe_sql.qi`; the `null_preserving_upsert` COALESCE pattern
- `app/core/retry_service.py` detection of non-retryable errors and repeated identical failures

**Schemas** (add provenance and keys)
- `app/core/pe_models.py`
- `app/core/people_models.py`
- `LpGpCommitment`/`LpGpRelationship` (`app/core/models.py:3719-3790`)

**Parsers and extractors**
- `app/sources/sec_form_d/parser.py`
- 13F infotable parser (`pe_collection/.../sec_13f_collector.py:265-399`)
- `app/sources/sec/submissions_ingest.py` SIC→NAICS crosswalk
- `ingest_xbrl._upsert_financial_statements`
- `pension_cafr_collector.extract_text_from_pdf`, `cafr_parser.py` prompts, `bio_extractor.py` prompt and JSON repair
- `people_collection/filing_fetcher.py`, `email_inferrer.py` + `mx_verifier.py`, `dedup_service.py` (after the FK fix)

**Signals and macro**
- Greenhouse, Lever, Ashby, SmartRecruiters and Workday ATS clients
- epa_echo pagination, `census/county_cbp.py`, the `census_cbp/` template, `fred/`
- Real site_intel collectors (HIFLD, EPA, FRA, FEMA, NWI)

**wildcard-workbench** (the most valuable code for this goal)
- `ingest/connectors/sec_form13f.py`, `adv_monthly_roster.py`, `iapd_compilation_feed.py`, `sec_form_d.py`, `sec_edgar_submissions.py`, `dol_form_5500.py`
- `ingest/cik_crd_bridge.py`, `ingest/resolve.py`, `norm.py`
- The data in `workbench.person_external_ids`, `person_company_links` and `cik_crd_bridge`

### Cut (delete code, purge or quarantine rows)
| Item | Action |
|---|---|
| 8-K deal path (`press_release_collector.py:162-183`, `persister.py:909-940`) | Delete. Purge 740 deals and the linked filer companies |
| 13F-as-portfolio path (`persister.py:772-823`) | Delete. Move the rows into a holdings table or discard them |
| `demo_seeder.py`, `pe_ecosystem_seed.py` writes to production tables | Move to a `demo` schema; add an `is_synthetic` guard |
| All `_get_sample_*` / `random.*` fallbacks in site_intel logistics, plus `usda_truck` and `goodjobs` seed data | Delete. Purge about 2.4k rows. Jobs must fail instead |
| `app/sources/sec_form_adv/client.get_sample_advisers` and its 10 rows | Delete |
| `app/sources/edgar_company_facts/` (writes into a view) | Delete |
| `app/core/entity_resolver.py`, `canonical_entities` (4 rows) | Delete once the new master exists |
| LP regex contact extractor, FO headline and Form D deal extraction | Delete. Purge about 2.8k contacts and 26 investments |
| LP 13F `KNOWN_CIKS` path and the `investor_type='lp'` rows in `portfolio_companies` | Purge; replace with the bulk 13F load |
| People: PageFinder 146-pattern HEAD sweep, Google SERP and LinkedIn scrapers, PR-wire search page scraping | Delete |
| kaggle/, prediction_markets/ (currently in the *critical* batch tier), dunl/ | Delete |
| glassdoor, app_stores, github (synthetic), web_traffic, foot_traffic, google_trends, opencorporates | Delete or put behind a flag |
| Orphan tables (`lp_manager_commitment`, `lp_13f_holding` until reused, `family_office_contact`, `rate_limit_buckets`, empty `acs5_*`, `bls_*_consumer/producer`) | Drop through migrations |

### Freeze (no nightly runs, no new work)
- international_econ (IMF, BIS, OECD BATIS; WDI read-only)
- cftc_cot, treasury daily balance, us_trade, data_commons, EIA
- site_intel beyond its existing real layers
- company-signals sources other than job_postings, usaspending, nppes, epa and cms

Macro sources worth keeping on a schedule: FRED rates, CBP, IRS SOI, QCEW and FDIC.

---

## 5. Target architecture for high-volume PE collection

### 5.1 Principles
1. **Bulk first.** Where a quarterly, monthly or nightly file exists, load the file. Per-entity API calls and crawling only fill gaps, and only for entities already in the master.
2. **Load everything, filter later.** The workbench connectors filter to a tracked universe at load time and throw away 68–93% of the data. Filters belong in the mart layer.
3. **Identifiers before names.** CIK, CRD, SEC file number (801-/802-), private fund ID (805-), CUSIP, LEI, EIN, NPI/CCN, UEI and domain. Names are only used for blocking and review, never for automatic merges.
4. **One execution path.** Everything runs as a worker-queue job, with a leader-locked scheduler. The API process never does ingestion work.
5. **Every row traceable.** Each row records source release, record ID, fetch time, parser version and confidence.

### 5.2 Layers, mapped onto the repo

```
raw (object store / disk + Postgres manifest)
  raw.source_release(id, source, release_key e.g. 'form_d:2026Q2', url, sha256, bytes,
                     fetched_at, status[discovered|fetched|staged|loaded|failed], parser_version)
  files: data/raw/<source>/<release_key>/<original filename>   (immutable, re-parseable)

staging (schema `stg_<source>`, typed 1:1 with the source file, COPY-loaded, replace-per-release)
  stg_form_d.issuers / offering / relatedpersons
  stg_13f.submission / infotable / coverpage
  stg_insider.submission / reportingowner / nonderiv_trans
  stg_adv.adviser_base / schedule_a / schedule_b / schedule_d_7b1
  stg_edgar.filer / filing_index (master.idx) / former_names
  stg_cms.snf_owners / hospice_owners / hha_owners ...
  stg_dol5500.f5500 / sched_c / sched_h
  (every row: _release_id, _row_num)

core (schema `core`, canonical; Alembic-managed)
  core.entity(entity_id, entity_type[org|fund|person|security], display_name, status,
              superseded_by, created_at)
  core.identifier(entity_id, id_type[cik|crd|sec_801|fund_805|cusip|lei|ein|npi|ccn|uei|domain],
                  id_value_normalized, source, first_seen, last_seen, UNIQUE(id_type,id_value))
  core.alias(entity_id, name, name_norm, source)
  core.relationship(subject_id, predicate[advises|gp_of|invests_in|owns_pct|officer_of|
                    director_of|related_person_of|acquired|lent_to|holds_position],
                    object_id, attrs jsonb, valid_from, valid_to,
                    source, source_release_id, source_record_id, confidence)
  core.attribute(entity_id, attr, value jsonb, as_of, source, source_release_id, confidence)
  core.er_key_veto / core.er_run   (ported from workbench resolve.py)

mart (schema `public` today): pe_firms, pe_funds, pe_portfolio_companies, pe_deals,
  pe_people, lp_gp_commitments, people, company_people ... become BUILT from core
  (materialized views or rebuild-by-SQL jobs), each carrying entity_id FKs.
```

**Where it goes in the repo**
- `app/ingest/bulk/<source>/{discover.py, fetch.py, stage.py, map_to_core.py}`: one module per bulk source, ported from the workbench connectors with their universe filters removed.
- `app/ingest/bulk/base.py` defines the `BulkSource` contract: `discover()` returns releases, `fetch(release)` writes to raw, `stage(release)` does COPY, `map(release)` upserts into core. Each step writes `raw.source_release.status`, and that status is the checkpoint. A retry resumes at the first incomplete step and never re-downloads.
- `app/entities/` holds the ported `resolve.py`, `norm.py` and `cik_crd_bridge.py`. The resolver runs as a queue job after every `map` step.
- `app/marts/pe/*.sql` plus the `app/marts/build.py` job. The existing `pe_*` API routes read from the marts unchanged.
- `app/core/sec_http.py` is the single SEC client: one configured `SEC_USER_AGENT` with a real contact address, one shared token bucket at about 8 req/s across data.sec.gov, www.sec.gov and efts.sec.gov, bytes and streaming support, a cap on HTTP-date Retry-After values, and fail-closed behavior.
- `migrations/` (Alembic): baseline autogenerated from the current DB. The ALTER statements in `main.py:257-345` and `database.py:84-93` move into it. Startup checks for schema drift, comparing ORM columns with `information_schema`, and refuses to boot on a mismatch.

### 5.3 Write path
- **COPY into staging, then one set-based `INSERT … ON CONFLICT` into core.** This replaces the row-by-row `executemany` in `batch_operations.py:131`. Dedup inside the batch and keep the last record; report inserted vs updated with `RETURNING (xmax = 0)`.
- **Savepoint per entity** in any persister that still handles single items (LLM or website gap-fill).
- **Confidence ranking enforced in SQL** when building marts: `sec_filing(100) > regulatory_bulk(90) > official_website(60) > annual_report(50) > news(30) > llm_extracted(20)`. The mart value for each attribute comes from the highest-ranked, most recent source. `demo` data never reaches a mart.

### 5.4 Orchestration
- APScheduler runs in a single `scheduler` container holding a `pg_try_advisory_lock`, and it only calls `submit_job`. PE, people, retry and site-intel crons stop running in the API.
- `job_queue` gains `attempts`, `max_attempts` and a `dead_letter` status. Final status writes are guarded by `WHERE worker_id = :me`. The stale threshold rises to at least 10 minutes. Each executor gets its own session.
- A batch won't launch unless a worker heartbeat has been seen within the last 5 minutes. The queue cleanup job also finishes the matching `ingestion_jobs` row.
- **A job with 0 rows is a warning, not a success.** The watermark only advances to the max data date in a release, never to the wall-clock time a job finished.
- Job config is validated against the ingest function's signature (`inspect.signature`) when it is submitted.

---

## 6. Prioritized roadmap

Volumes are order-of-magnitude estimates from the source catalog and audits. Effort: **S** is 1–2 days, **M** 3–7 days, **L** 2–4 weeks.

### Phase 0: stop the bleeding (days 1–3)
| Item | Volume effect | Effort |
|---|---|---|
| Get a worker (and the API) running on an always-on host; clear the 1,937 zombie jobs; fix D25 cleanup | Unblocks everything | S |
| Hotfixes: D1 (add `role_type` to the ALTER list), D2 (`begin_nested`), D3 (`getattr(ct,'value',ct)`), D4 (repoint `reports_to_id` and roll back), D5 (session per task) | About 4.9k leadership changes can persist on the next run; people and PE writes resume | S |
| Quarantine: tag and move demo rows, delete fabricated logistics rows and their fallbacks (D7), purge the 8-K deals, LP 13F rows, junk LP contacts and FO investments | Removes about 10k wrong rows; analytics become trustworthy | S |
| Rotate the Yelp key and scrub it from `ingestion_jobs.config` (D34) | Security | S |
| Remove kaggle and prediction_markets from the batch tiers; take frozen sources off the schedule | Frees batch window | S |

### Phase 1: bulk SEC foundation (first 2 weeks, highest volume per unit of effort)
| # | Item | Expected data gain | Effort |
|---|---|---|---|
| 1 | `BulkSource` framework, `raw.source_release`, `sec_http.py`, Alembic baseline | Prerequisite | M |
| 2 | **Form D data sets**, backfilled 2009 to now (ISSUERS, OFFERING, RELATEDPERSONS) | 0 → about 800k filings and several million related-person rows. Pooled-fund filings give **tens of thousands of PE/VC funds with size and first-sale date** | S–M |
| 3 | **13F data sets**, positions kept, backfilled 2013 to now | 5.4k filing summaries → about 3M positions per quarter (tens of millions total). Replaces the fake portfolio companies | S |
| 4 | **Insider transactions data sets** (Forms 3/4/5) | 0 → about 350k forms a year. Fills `insider_transactions`, sponsor 10%-owner and director links, and a first `board_seats` | S |
| 5 | **EDGAR submissions.zip + daily master.idx** into `sec_filings_index` (with 8-K `items`) | 2.3k → about 900k filers with former names; about 70k 8-Ks a year tagged 1.01/2.01/5.02. Replaces D18 | S–M |
| 6 | **Form ADV**: FOIA monthly files (through Dec 2024) plus the **unfiltered** IAPD compilation feed; Schedule A/B owners; Schedule D 7.B.(1) private funds where available | 0 → about 15.5k RIAs and about 6k ERAs with AUM and headcount; tens of thousands of private funds with gross asset value; control persons into `sec_form_adv_personnel`. **This replaces the 105-firm seed list** | M |
| 7 | **XBRL fix**: key on (end, duration, frame), load companyfacts.zip or Financial Statement Data Sets; delete edgar_company_facts | 97k wrong rows → correct statements for about 6–7k filers over about 15 years; real D&A and EBITDA | M |

*By the end of week 2, the empty SEC tables should hold millions of rows, all keyed by CIK, CRD or CUSIP.*

### Phase 2: entity master and PE marts (weeks 3–8)
| # | Item | Expected data gain | Effort |
|---|---|---|---|
| 8 | Port the workbench resolver into `app/entities`. Feed it EDGAR, ADV, Form D, 13F, insider data and the existing `pe_*`, `industrial_companies` and `portfolio_companies` tables. Build the CIK↔CRD bridge from Form D related persons, ADV and 13F cover pages | Joinable master of about 1M organizations; `pe_firms.crd_number` 0 → about 100% for ADV-registered firms | L |
| 9 | **Rebuild `pe_firms` and `pe_funds` as marts** from ADV Schedule D fund types (PE/VC) and Form D pooled funds, with GP↔fund links through 805- IDs and related persons | pe_firms 105 → about 4–6k PE/VC advisers; pe_funds 13 real → about 30–60k | M |
| 10 | Merge the people universe: one `core` person type; SEC owner CIK and CRD as identifiers; Form D related persons, ADV Schedule A/B and insider reporting owners as roles; close tenures instead of editing titles (fix `uq_company_people`) | 2.2k + 2.6k name-matched people → hundreds of thousands of role edges on strong keys | M–L |
| 11 | **GLEIF Golden Copy**, Level 1 and 2 | About 2.8M entities and about 500k parent links; cheap LEI↔CIK/fund linkage | S–M |
| 12 | **CMS ownership files** (SNF, hospice, HHA, hospitals, RHC, FQHC) with PE and REIT flags | 0 → hundreds of thousands of owner rows; healthcare roll-up ownership | S–M |
| 13 | **DOL 5500 in full** (unfiltered; Schedules C and H) | 53k sponsors → about 800k filings a year; EIN-keyed headcount proxy for private companies | M |
| 14 | **BDC schedules of investments** (FS&N data sets) | 0 → about 60–100k private-credit positions per quarter naming PE-backed borrowers | M |
| 15 | **LP commitments, top 10 HTML/Excel pensions** (CalPERS, CalSTRS, WSIB, Oregon, MN SBI, FL SBA, and others). Wire `CafrParser` and the table extractor into a commitment persister keyed by `lp_id`, never by name (D23, D24). Load `lp_public_seed` as `confidence='unverified'` | 0 → about 10–20k commitment rows with vintage, commitment and IRR | L |
| 16 | Deals v2: 8-K Item 2.01/1.01 + DEFM14A + SC TO-T where a resolved PE entity is the buyer (LLM check that buyer ≠ filer); press-wire **RSS** feeds; FTC HSR early termination API | About 40 real deals → about 5–20k deals a year with source text | M |
| 17 | Small one-off loads: SBIC directory, NPPES full monthly file (40k → about 8M NPIs), FFIEC NIC structure data | About 8M+ rows, mostly healthcare and banking context | S each |
| 18 | Platform hardening: merge four collector base classes into one; watermarks keyed by partition; fail-closed rate limiter; field-level provenance in marts | Safe horizontal scaling | M |

### Phase 3: later (months 3+)
| Item | Gain | Effort |
|---|---|---|
| Remaining public pensions and endowments (PDF board packets) | +20–40k commitments | L |
| Website gap-fill **only** for PE portfolio companies without SEC or Form D coverage: honest User-Agent, robots.txt, cheap model, `llm_extracted` tag | Management teams for about 1–5k private companies | M |
| ADV Part 2B brochure supplements → experience and education | Career history for GP professionals | M |
| USPTO patent and trademark assignments (security agreements signal LBO lenders) | About 10M assignments | M |
| UK Companies House (PSC + iXBRL accounts), if Europe is in scope | About 5M companies; private-company financials | M–L |
| SAM.gov extracts + USAspending bulk (fix field names first) | About 700k contractors; millions of awards | M |
| Job postings retargeted from `industrial_companies` to master portfolio companies; stable IDs in `generic.py`; more ATS clients | 19k → 100–300k postings | M |
| CourtListener bankruptcy dockets, OSHA via the current DOL API, state registries (FL, NY), IRS 990 XML | Distress and diligence signals | M each |

---

## 7. Open questions

1. **Which project is the single home for ingestion: nexdata or wildcard-workbench?** The working bulk connectors, the CIK↔CRD bridge and the ER engine are all in workbench, filtered to a "fin" universe. My plan assumes they get ported into nexdata and the workbench schema becomes legacy staging. If workbench is meant to stay the ingestion layer, the plan changes to "remove its filters and have nexdata read from it."
2. **How wide is the PE universe?** Buyout plus growth only, or also VC, private credit, real estate, hedge funds and family offices? This decides which ADV Schedule D fund types and Form D fund types count as "PE", and the size of the marts (about 4k vs about 20k managers).
3. **Geography.** US only, or UK and EU as well? Companies House is the only free source of private-company financials at scale, but it is a separate workstream.
4. **Purge or quarantine?** Can the 8-K deals, 13F-derived companies, fake funds, junk contacts and fabricated logistics rows be deleted outright, or do you want them kept in a `quarantine` schema? And do the demos still need `demo_seeder` data, and if so, can it live in a separate `demo` schema?
5. **Hosting.** Docker isn't running and nothing has run through the queue since April. Is there an always-on host for the worker and scheduler? Bulk backfills need about 50–200 GB of disk for raw files (NPPES alone is about 9 GB zipped; 13F history is large) plus sustained Postgres I/O.
6. **Form ADV after 2024.** The SEC's Form ADV Part 1 CSVs end on Dec 31, 2024. Are you willing to archive the IAPD compilation feed daily, since missed days can't be recovered, and accept that 2025+ private-fund detail may be thinner until we confirm what the feed contains?
7. **LLM budget and vendor.** Bulk-first sharply cuts LLM use. Deal verification and pension PDF extraction still need a model. What monthly spend ceiling applies, and which vendor or vendors should be supported? I've left the choice open on purpose.
8. **The SEC contact address.** SEC fair-access rules need a real, monitored contact address in the User-Agent. Which one? The repo currently uses 8+ made-up variants.
9. **Macro and site_intel.** Does another product line depend on the macro and site-intel clusters (such as site selection)? If not, I'd freeze them completely and move that maintenance time to PE sources.
10. **Commercial posture.** Is nexdata used internally only, or will data be redistributed to clients? That affects GLEIF and government data (fine either way), OpenCorporates and OpenSanctions (excluded), and how cautious the website gap-fill should be.