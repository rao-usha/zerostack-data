# PLAN_082: PE rebuild, Phase 0 (fix the data problems) + Phase 1 (bulk SEC loading)

## Context
The 2026-09-16 review (`docs/reviews/2026-09-16_pe_collector_review.md`) found:
- No worker has run since 2026-04-16.
- Most of the PE data is wrong: 740 of 788 deals are 8-K false positives, 1,069 "portfolio companies" are 13F stock positions, the financials are demo data, and the logistics rows are fabricated.
- Several bugs lose data silently.
- The SEC tables that matter are empty.

This plan fixes the data problems (Phase 0), then loads free SEC bulk files into Cloud SQL (Phase 1).

**Decisions already made (from the user):**
- Bad rows go to a `quarantine` schema and demo rows to a `demo` schema. Nothing is hard-deleted.
- The workbench connectors are ported into nexdata.
- Alembic manages schema changes.
- SEC User-Agent contact is `alexiusmichael@gmail.com`.
- The target database is Cloud SQL (proxy on :5435). Backfills cover only the **most recent 2–3 years** because the disk is 10 GB and cost matters.
- Quarantine is approved to run directly on the cloud DB.
- The Yelp key is **not** rotated; only its plaintext copy is scrubbed.

**Facts checked by the exploration agents (everything below still holds):**
- All the D-defects are still in the code.
- The database is 4 GB, the alembic baseline `3fb893199e22` is stamped, but nothing ever runs `alembic upgrade`.
- `job_queue` has nothing live. `ingestion_jobs` has 1,806 blocked, 131 pending and 63 running jobs, all zombies.
- Almost every FK uses NO ACTION, so the quarantine has to move child rows before parents.
- Only 2 of the workbench connectors actually use bulk files. There is no insider connector. Nothing in nexdata can stream a download to disk or use COPY.

**Runtime prerequisite:** Docker Desktop has to be running for the api and worker containers. Those containers reach Cloud SQL through the host `cloud-sql-proxy.exe` on :5435, which has to stay running.

---

## Phase 0: fix the data problems

Each item gets its own `/spec`, with tests written before code.

### SPEC_103: silent data-loss hotfixes (`bug_fix`)
- **D1:** new Alembic migration `0001_pe_firm_people_role_type` running `ADD COLUMN IF NOT EXISTS role_type VARCHAR(50)`. Also add the same ALTER to `_apply_schema_migrations` in `app/core/database.py:84` so fresh databases get it.
- **D2:** in `app/sources/pe_collection/persister.py:173-197`, wrap each item in `with self.db.begin_nested():` and count `persisted`/`updated` only after the savepoint succeeds. Fix `tests/test_pe_persister.py:580` so the item really raises.
- **D3:** in `app/sources/people_collection/orchestrator.py:929,943`, use `getattr(change.change_type, "value", change.change_type)`, the same pattern as lines 878/884.
- **D4:** in `app/services/dedup_service.py:360`, point `reports_to_id` at the surviving row before deleting, and run each auto-merge in `begin_nested()`. Add `session.rollback()` to the except blocks at `orchestrator.py:348-357` and `1139-1146`.
- **D5:** give each concurrent unit its own session from `get_session_factory()()`, closed in `finally`. Locations: `people_collection/orchestrator.py` `collect_batch` (451-458), `lp_collection/runner.py` (193-214, 303-316), `services/bio_parser_service.py:153-172`.
- **D22:** SEC rate limit set to 8 req/s: `api_registry.py:147` becomes `rate_limit_per_minute=480`, and `rate_limiter.py:58` becomes `requests_per_second: 8.0`.

### SPEC_104: stop the bad data from coming back (`bug_fix`)
Each item is deleted or disabled so the rows can't be written again after the quarantine:
- **8-K deal path:** `press_release_collector.py:162-183`, `persister.py:909-940`.
- **13F-as-portfolio path:** `persister.py:772-823` and the synthetic "13F Holdings" fund creation at 297-323.
- **Logistics sample fallbacks (D7):** every `_get_sample_*` in `site_intel/logistics/*` and `usda_truck`. On no data, the collectors return an empty or failed result with an error.
- **Form ADV samples:** `sec_form_adv/client.get_sample_advisers`.
- **LP regex contact extractor:** `lp_collection/website_source.py:291-316`.
- **FO headline/Form D deal extraction:** `family_office_collection/deals_source.py:176-300, 451-505`.
- **Demo seeders:** `seed-demo` and `pe/ecosystem/seed` return 403 unless `ALLOW_DEMO_SEED=1`. Also fix `pe_ecosystem_seed.purge()`, which deletes firms by name.
- **Batch tiers:** remove `prediction_markets` from TIER_1 and the critical group (`batch_service.py:67, 208`).

### SPEC_105: quarantine migration (`model`)
- **Rules module:** `app/core/quarantine_rules.py` holds an ordered list of `(table, target_schema, predicate SQL, expected_count, reason)`, ordered children before parents. A dry-run script (`scripts/quarantine_dry_run.py`) and the Alembic migration both use it.
- **Migration** `0002_quarantine_bad_data`:
  - `CREATE SCHEMA quarantine`, `CREATE SCHEMA demo`.
  - For each rule: `CREATE TABLE <schema>.<t> (LIKE public.<t> INCLUDING DEFAULTS)` plus `_reason` and `_quarantined_at` columns, then `INSERT … SELECT`, then `DELETE`.
  - **Count guard:** abort if a live count is more than 5% above expected, so it can never remove more than we verified.
  - `downgrade()` copies the rows back in reverse order.
- **Rows, using the verified predicates:**

| Group | Rows | Destination |
|---|---|---|
| 8-K deals (`data_source='SEC 8-K' AND deal_type='8-K Event'`) | 740 | quarantine |
| 13F investments | 3,621 | quarantine |
| 13F funds | 27 | quarantine |
| 13F companies, including JANUS id 1441 | 1,069 | quarantine |
| 8-K filer-only companies | 340 | quarantine |
| Demo cash flows | 66 | demo |
| Demo fund performance | 9 | demo |
| Demo fund investments | 24 | demo |
| Demo company child rows (leadership, news, alerts, competitor mappings, theses, snapshots, financials, valuations) | see detail below | demo |
| Demo deals, including the one non-demo deal attached to a demo company | 35 | demo |
| Demo funds | 6 | demo |
| Demo companies | 57 | demo |
| Demo people | 75 | demo |
| Demo firms | 2 | demo |
| Fabricated logistics rows | 2,286 | quarantine |
| `incentive_deal` with `source='gjf_seed'` | 28 | quarantine |
| All of `lp_key_contact` | 3,174 | quarantine |
| LP 13F rows in `portfolio_companies` | 4,464 | quarantine |
| `family_office_investment` with `source_type='news'` | 26 | quarantine |
| `form_adv_advisers` with `data_source='sample'` | 10 | quarantine |
| 9 title-named `pe_people` and their 16 firm links | 25 | quarantine |

  - **Demo company child rows:** financials match `data_source IN ('demo_seeder','Demo Seed','Industry estimate')` (262 rows, the whole table) and valuations match `data_source IN ('demo_seeder','Demo Seed')` (45 rows, the whole table). Both are moved row by row, not by company.
  - **Kept:** `incentive_deal` rows with `gjf_expanded` (curated from public disclosures), and the 3 real PE-backed companies attached to 8-K deals.
- **Also in this migration:**
  - **Zombie jobs:** set the 1,806 blocked, 131 pending and 63 running `ingestion_jobs` to `failed` with `error_message='zombie: no worker since 2026-04-16 (PLAN_082)'`.
  - **Yelp key scrub:** `UPDATE ingestion_jobs SET config = config::jsonb - 'api_key' WHERE config::jsonb ? 'api_key'` (1 row).
- **Running Alembic:** `app/core/migrate.py` runs `alembic upgrade head` under `pg_advisory_lock`. It's called at api and worker startup before `create_tables()`, in `app/main.py:247` and `worker/main.py:499`.

### SPEC_106: job lifecycle and Yelp key handling (`bug_fix`)
- **D25:**
  - `cancel_stale_pending_jobs` (`job_queue_service.py:144-202`) also marks the linked `IngestionJob` failed and calls `promote_blocked_jobs` for each affected batch.
  - `batch_service.py:386` refuses to launch when no `job_queue.heartbeat_at` is newer than 5 minutes. It warns in WORKER_MODE and has a `force` override.
- **D34:**
  - Remove `api_key` from the configs in `app/api/v1/yelp.py:114,155,181`.
  - `_run_dispatched_job` (`jobs.py:860`) fills `api_key` from `get_settings().get_api_key(source)` when the dispatch expects it.
  - Check the other routers that list `api_key` (FRED, EIA, BEA …) for the same leak.

---

## Phase 1: load SEC bulk files (recent 2–3 years)

### SPEC_107: BulkSource framework (`service`)
- **`app/core/sec_http.py`:** a single SEC client built on httpx.
  - `SEC_USER_AGENT` setting, default `"Nexdata research alexiusmichael@gmail.com"`.
  - `stream_to_file(url, path)` writes to `.part`, then does an atomic rename and a sha256 sidecar (the workbench `Ctx.fetch_file` design).
  - Shared distributed bucket at 8 req/s across `www.sec.gov`, `data.sec.gov`, `efts.sec.gov` and `reports.adviserinfo.sec.gov`. These buckets are added to `DISTRIBUTED_RATE_LIMITS` (`rate_limiter.py:645`) and fail closed for SEC hosts.
  - Retry-After is respected, with a cap.
  - Magic-byte validation is ported from workbench `harness._validate_payload`.
- **`app/core/copy_loader.py`:**
  - `copy_into_staging(conn, table, columns, row_iter)` uses psycopg2 `copy_expert` into an **UNLOGGED** `stg.<source>_<table>` table, truncated for each release.
  - `merge_staging(conn, stg, target, key_cols, update_cols)` runs `INSERT … SELECT DISTINCT ON(key) … ON CONFLICT DO UPDATE … RETURNING (xmax=0)` and returns (inserted, updated).
  - The staging table is dropped after the merge so it doesn't take up disk.
- **`app/ingest/bulk/base.py`:**
  - `BulkSource` with `name`, `discover(since) -> [Release]`, `fetch(release)`, `stage(release)` and `merge(release)`.
  - Status is tracked in `raw.source_release` (id, source, release_key, url, sha256, bytes, local_path, status `discovered|fetched|staged|loaded|failed`, row counts, parser_version, timestamps, `UNIQUE(source, release_key)`). A retry resumes at the first incomplete step.
  - Raw files go to `data/raw/<source>/<release_key>/`, which is volume-mounted and gitignored. They stay on the local disk, which is free.
- **Worker:**
  - Add `QueueJobType.BULK_INGEST` (`models_queue.py:37`). No DB change is needed.
  - New executor `app/worker/executors/bulk_ingest.py`. Blocking parse and COPY run in `asyncio.to_thread`, so the heartbeat keeps running.
  - A job with 0 rows is recorded as a warning, not a success.
  - Endpoint: `POST /api/v1/bulk/{source}/run?since=YYYY-MM-DD`. Status: `GET /api/v1/bulk/releases`.
- **Migration** `0003_bulk_framework` creates the `raw` and `stg` schemas and `raw.source_release`.

### SPEC_108: Form D data sets
- **Port from:** workbench `scripts/seed_b2b_edgar.py` (index scrape at 151-164, `parse_quarter` at 195-273). Its `gate()` universe filter (334-364) is not ported.
- **Scope:** the last 12 quarters of `YYYYqN_d.zip`.
- **Targets:** existing `form_d_filings` (unique `accession_number`), plus a new `form_d_related_persons` (accession, person name, relationship, address).
- **Size:** about 200k filings and 1M persons, roughly 0.4 GB.

### SPEC_109: 13F data sets
- **Port from:** workbench `sec_form13f.py` (index scrape, streaming TSV parse). The `finalize` fin_universe match is not ported. INFOTABLE is kept as positions instead of being folded into a count.
- **New tables:** `sec_13f_filings`, keyed by accession, with every cover/summary column from the workbench table. `sec_13f_holdings`, keyed by (accession, infotable_sk), with cusip, issuer, class, value, shares, put/call and discretion.
- **Scope, for cost:** filings for 3 years, but **holdings only for the latest 2 quarterly data sets** (about 6M rows, about 1.3 GB). The window is configurable through `BULK_13F_HOLDINGS_QUARTERS`.

### SPEC_110: insider transactions data sets (new; there is no workbench equivalent)
- **Source:** SEC "Insider Transactions Data Sets" quarterly ZIPs (SUBMISSION, REPORTINGOWNER, NONDERIV_TRANS, DERIV_TRANS).
- **Scope:** last 12 quarters.
- **New tables:** `sec_insider_filings`, `sec_insider_owners` and `sec_insider_transactions`, keyed by accession plus the SEC surrogate keys.
- The existing `insider_transactions` table (people-pipeline FKs, no unique key) is left alone. Linking it to people is Phase 2.
- **Size:** about 1.5M transactions, roughly 0.4 GB.

### SPEC_111: EDGAR filers and 8-K index
- **Source:** stream `submissions.zip` one member at a time, never extracting it to the DB whole.
- **Filers:** `sec_filers` (cik, name, sic, ein, state of incorporation, addresses, tickers) and `sec_filer_former_names`. About 900k rows. Normalizers are ported from workbench `norm.py`.
- **8-K index:** `sec_8k_index` (accession, cik, filed, items, primary doc), built from each filer's `filings.recent` for 8-K / 8-K/A in the last 3 years. It includes the 1.01, 2.01 and 5.02 items.
- **D18:** in `app/sources/sec/metadata.py:90-95`, move the MySQL-style `INDEX` clauses into separate `CREATE INDEX` statements.

### SPEC_112: Form ADV
- **Monthly adviser rosters:**
  - Port from workbench `adv_monthly_roster.py` without `_load_universe_crds`, the skip at 571-572, or the `finalize` differ.
  - **Include** the `-exempt` ERA files, which workbench skips.
  - Scope: the last 36 monthly files, loaded into `sec_adv_roster_snapshots` (crd, roster_date).
  - The latest snapshot is upserted into the existing `sec_form_adv` (unique crd).
- **IAPD compilation feed:**
  - Port from workbench `iapd_compilation_feed.py` without `TRACKED_UNIVERSES` or the universe membership test.
  - Target: `sec_adv_feed_firm_state` (crd, edition_date).
  - The SEC keeps only the latest edition, so this job is scheduled daily.
- **Stretch goal (separate follow-up if it runs long):** the Dec-2024 FOIA Schedule D 7.B.(1) private funds and Schedule A owners, loaded into `sec_adv_private_funds` and `sec_form_adv_personnel`.

### SPEC_113: XBRL period-key fix
- **Bug:** `app/sources/sec/xbrl_parser.py:371-437` groups facts by the filing's fy/fp.
- **Fix:** group by each fact's own `(end, duration bucket)`, where instants count as balance-sheet values. Take the latest `filed` value for restatements. Keep fy/fp only as derived labels.
- **Keys:** migration `0004_xbrl_period_keys` moves the 97k wrong rows in each of `sec_income_statement`, `sec_balance_sheet` and `sec_cash_flow_statement` into quarantine. It changes their unique keys (`sec/models.py:161/230/301`) and the matching `conflict_columns` in `ingest_xbrl.py:200-240` to `(cik, period_end_date, period_start_date)`.
- **Reload:** a `companyfacts.zip` BulkSource, limited to periods ending within the last 3 years.
- **Also:** remove `app/sources/edgar_company_facts/`, which writes into a view.

**Expected DB growth:** about 3–4 GB in total, taking the DB from 4 GB to roughly 8 GB. That fits the 10 GB disk or needs one small auto-resize, costing well under $1/month.

---

## Order and checklist
1. `/spec` + tests → SPEC_103 → SPEC_104 → SPEC_106 → SPEC_105. The quarantine runs last in Phase 0, after the code that writes bad rows has been removed.
2. Start Docker Desktop, then `docker-compose up -d api worker` so the migrations run at startup. Confirm a worker heartbeat appears.
3. SPEC_107 → 108 → 109 → 110 → 111 → 112 → 113. Each loader is run once on a single release, checked, then backfilled.
4. Log to `memory/logs/2026-09-16.md` after each spec. Nothing is committed until the user asks.

## Verification
- **Unit tests:** `pytest tests/ -v --ignore=tests/integration/` after each spec. New tests cover:
  - the savepoint isolation that really raises (D2)
  - `_store_changes` with string enums (D3)
  - `reports_to_id` re-pointing (D4)
  - the quarantine rule ordering and count guard
  - COPY/merge SQL builders
  - parsers for each bulk source, using small fixture ZIPs under `tests/fixtures/sec_bulk/`
  - XBRL grouping on a companyfacts fixture where a FY2024 10-K carries FY2023 comparatives
- **Quarantine dry run:** `scripts/quarantine_dry_run.py` runs every predicate inside `BEGIN … ROLLBACK` on Cloud SQL and prints live vs expected counts. The migration only runs after that output is reviewed.
- **After the migration:**
  - `SELECT COUNT(*)` on `pe_deals` should be 13 (788 minus 740 minus 35).
  - `quarantine.*` and `demo.*` counts should match the table above.
  - `GET /api/v1/pe/firms/{id}/org` should return 200, which was a 500 before (D1).
  - `alembic downgrade` of 0002 is tested inside a transaction via the dry-run script.
- **Loaders:**
  - Each run shows `raw.source_release` moving `loaded` with row counts.
  - Spot checks:
    - a known Form D pooled fund accession
    - Berkshire's 13F holdings in the latest quarter
    - a known Form 4 for AAPL
    - Carlyle's CIK in `sec_filers`
    - Apple FY2023 revenue of 383,285M landing on period_end 2023-09-30
  - Re-running a loaded release is a no-op, and a failed release resumes without re-downloading.
  - `pg_database_size` is checked after each loader.
