# Nexdata DaaS review: where we left off and what comes next

The database was unreachable during this review because Docker Desktop is hung. All row counts, sizes, job counts and "159 days dead" figures come from PLAN_085 and the session logs dated 2026-09-22, and none of them could be re-measured. Every other claim below is backed by code. Items marked **[SV]** I re-checked myself while writing this report.

---

## 1. Where we left off

- **Last shipped work (2026-09-16 to 09-22).** PLAN_082 to PLAN_084 and SPEC_107 to SPEC_120 built:
  - the SEC bulk framework and 8 loaders
  - the entity master (SPEC_116)
  - PE firms and funds from ADV and Form D (SPEC_117)
  - ADV Schedule D with platform attribution (SPEC_118)
  - GP people (SPEC_119)
  - monthly mart schedules (SPEC_120)

  The last commit is `15bd76d` (proxy runs as a container). **Local `main` is 90 commits ahead of origin.** The last push was `a306219` on 2026-04-17 **[SV]**, so the only offsite copy of the code is five months old.
- **Current frontier.** PLAN_085 (status portal and cadence) is untracked in git (`?? docs/plans/PLAN_085...`) and still waiting on its 5 open questions. None of SPEC_121 to SPEC_126 has been started. `docs/specs/.active_spec` still points at SPEC_120. Today's log (2026-09-23) holds only the auto-created stub.
- **Known bugs that are still unfixed:**
  - The SPEC_120 zombie `IngestionJob` bug is **worse than PLAN_085 says** (§4).
  - Scheduler misfires are dropped (1s grace), and api and worker have no restart policy.
  - The date-keyed SEC snapshots re-download on every run and nothing is ever deleted.
  - `pe_mart_build` with `dry_run=true` still writes `pe_firms` and `sec_adv_private_funds` (`app/worker/executors/pe_marts.py:23-31`) **[SV]**.
  - The API ingestor fleet has been dead about 159 days (DB-derived).
- **The first unattended cycle starts 2026-10-04** (Schedule D) and ends with the marts on **10-10**. The runtime is down right now.
- **The SPEC_121 number is claimed by two pieces of work:**
  - Log 2026-09-22:27: "Then ADV Schedule A (SPEC_121)".
  - PLAN_085 §9: SPEC_121 is misfire, write-back and restart policy.
  - SPEC_119:190 separately calls Schedule A "SPEC_120", a number that was used for mart scheduling.

  §6 resolves this.

---

## 2. DaaS maturity scorecard

| Lens | Score (0–5) | Why |
|---|---|---|
| Delivery surface | 1 | The only metered, keyed API (`/public`) serves legacy `lp_fund`/`portfolio_companies`. The SEC, PE-mart and entity-master products are reachable only through internal routers or a `SELECT *` table export. |
| Access, tenancy, commercialization | 1 | Hashed keys, rate limits and usage tables all exist, but auth is off by default, key scopes are never checked, there is no tenant or entitlement model, and GraphQL and job_stream are always public. |
| Catalog, metadata, lineage, licensing | 1 | Six competing "catalogs". No license, redistribution or PII field anywhere. 2 `COMMENT ON COLUMN` statements in the whole codebase. The lineage subsystem is built but nothing writes to it. |
| Freshness, reliability, observability | 1 | Nothing alerts on missing data. Missed runs are dropped. Mart schedules freeze after one run. `/health` cannot see the worker. All monitoring runs inside the process it monitors. |
| Data quality and contracts | 2 | Mart builds are careful (deterministic, refusal ledgers, link tiers), but gates were run by hand once, the DQ framework cannot see bulk or mart tables, and there is no publish guard on the 13F prune. |
| Product and data assets | 2 | The SEC-derived PE firm/fund/GP graph plus the CIK↔CRD crosswalk is genuinely differentiated. It is current-state only, provenance is hidden from readers, and there is no holdings layer yet. |
| Platform engineering for serving | 1 | Everything runs on one Windows laptop with personal ADC credentials, a shared db-f1-micro (per the docs), the scheduler inside the API, CI that has not run the last five months of work, and four owners of the schema. |
| Roadmap and execution hygiene | 2 | Planning discipline is strong (measured ship gates, honest self-audits). But 90 commits are unpushed, the plan is uncommitted, spec numbers collide, and review items D26–D29 are still open. |

---

## 3. The big picture

**Today Nexdata is a well-engineered SEC ingest-to-mart pipeline sitting next to about 70 dormant free-API mirrors, run from a laptop, with an internal operator console on top.** The SEC spine is real product-grade engineering: `raw.source_release` as a release ledger, per-row link tiers, a resolver run ledger, and reversible quarantine. Everything a customer would touch is missing:

- a declared contract: catalog, schema, license, SLO and as-of date
- an authenticated delivery channel for the valuable data
- trust signals: provenance and freshness shown on responses, and quality gates enforced on every build
- operations that notice failure

Today the flagship `pe_*` and `sec_adv_*` data can be pulled in bulk only through `/export/tables/{t}/preview`. That same endpoint will also return `users.password_hash` and plaintext password-reset tokens, and it has no authentication by default. The PE marts will silently stop refreshing after one cycle, and nothing would tell anyone, exactly as the fleet sat dead for 159 days.

Put bluntly, there is nothing a customer could buy today, and making the system sellable is mostly product and ops work, not more ingestion. The data assets are worth selling:
- the CIK/CRD crosswalk
- about 7.4k PE/VC advisers
- about 39k Form D fund vehicles with tiered GP attribution
- about 10k GP person–firm pairs

(Counts from the 09-18 to 09-22 logs.)

What is missing is a product layer around them. The next month should go on:

1. stopping silent loss and exposure
2. one catalogue that is both the ops spine and the customer contract (PLAN_085's DatasetSpec, expanded)
3. a small versioned `/data/v1` over the PE and entity products

More sources and more UI should wait. Revive dead sources only when they add identifiers or ownership edges to the entity master.

---

## 4. Is PLAN_085 the right next move?

**Mostly yes, if it is re-ordered and widened.** SPEC_121 and SPEC_122 are correct and urgent. The DatasetSpec in SPEC_123 is the right spine, but as drafted it is an ops record. If it ships that way, a second customer catalog will be needed later and the fragmentation it is meant to end will repeat. The portal and "Run now" (SPEC_124 and SPEC_125) should wait until access control exists.

### Claims that held up in code
- Five overlapping status surfaces exist (`sources.py:580`, `freshness.py:25,41`, `source_health.py:19`, `jobs.py:2012`, `bulk.py:73`), plus a sixth, `jobs.py:1234 /workers`.
- `/datasets/freshness` leaves out never-run sources (`freshness.py:56-63, 89-92`). It is also worse than the plan says: `:97-102` reports any source with no SLA as `"fresh"`.
- The SSE feed has no auth (`main.py:1525`). It also leaks `config` and `error_message` (`job_stream.py:214, 423-425, 480-482`).
- `_check_api_key_preflight` is internal only (`jobs.py:26`, called only at `:1119`).
- Bulk loaders never write `dataset_registry`. `_update_dataset_registry` runs before any fetch (`ingest_base.py:54-93, 132-133`).
- `misfire_grace_time` and `coalesce` appear nowhere in `app/`. There are no `job_defaults` (`scheduler_service.py:180-183, 504-511`).
- ETag is captured but never sent back (`sec_http.py:195, 262-263`; `bulk/base.py:97, 221`).
- `snapshot:{today}` release keys (`sec_companyfacts/source.py:153`, `sec_edgar_submissions/source.py:76`).
- No file retention in `app/ingest/bulk`.
- Only cloudsqlproxy has `restart:` (`docker-compose.yml:72`).
- Window constants: `sec_13f/source.py:46-47`, `sec_insider/parse.py:34`, `roster.py:48`, `sec_form_d/source.py:38`.
- The Schedule D cron runs on the 4th (`scheduler_service.py:1819-1820`).
- The API keys PLAN_085 names are absent from `.env`.
- The dead `index.html` panel exists (8 references, 0 backend hits). It renders empty or 0 values, not invented numbers.

### Claims that were refuted or need correcting
1. **§8.2 is wrong, and the real effect is worse [SV].** `cancel_stale_pending_jobs` only selects unclaimed queue rows (`job_queue_service.py:167-178`), and `cleanup_stuck_jobs` only selects RUNNING rows (`scheduler_service.py:1222-1228`), so the zombie `IngestionJob` stays PENDING forever. `run_scheduled_job` then skips any schedule that has a PENDING or RUNNING `IngestionJob` (`scheduler_service.py:216-229`, logged at INFO). The result:
   - If row 3891 is still pending, **the 10-10 `pe_mart_build` run is skipped**.
   - `entity_resolve` will leave its own zombie on 10-10 and then be skipped from 11-10 onward.
   - The marts freeze while the loaders keep refreshing.
   - The root cause is that `worker/main.py:306` updates only `JobQueue`. Any executor without its own write-back has this bug.
2. **"`sec_companyfacts` feeds nothing" is refuted [SV].** The loader writes `sec_income_statement`, `sec_balance_sheet`, `sec_cash_flow_statement` and `sec_financial_facts` (`sec_companyfacts/source.py:123-127`). Those feed the `public_company_financials` view (`app/sources/sec/views.py`, created in `main.py`), which `api/v1/investor_intelligence.py`, `services/synthetic/private_company_financials.py` and the TabDDPM trainer read. It feeds no mart, but it is not dead weight. Open question 3 rests on this premise.
3. **"`has_live_worker` only shows up as a 409" is overstated.** `GET /jobs/workers` (`jobs.py:1234`) already reports heartbeats.
4. **"Four incompatible dataset concepts" is actually six.** The plan misses `SOURCE_REGISTRY` (`app/core/source_registry.py`, 51 entries, which is what `/sources` serves) and `API_REGISTRY`. Neither contains the SEC bulk, entity or mart datasets.
5. **§D5 is only partly right.** `bulk_ingest.py:116-120` does fail the job when every release fails. Only mixed partial failures report success. Separately, if `discover()` raises, the `IngestionJob` stays RUNNING until timeout, which also blocks that schedule.

### What PLAN_085 is missing from a DaaS point of view
- **Authorization before any Run button.** Every router is anonymous by default (`main.py:1519-1520`; `REQUIRE_AUTH` is absent from `.env` **[SV]**). Self-registration hands out a JWT that passes every router (`auth.py:105`, plus the playground signup at `users/auth.py:295-323`). `collection_audit_log` has no actor column (`models.py:309-319`). The plan says "auth" nowhere except the SSE line.
- **Push alerting.** The portal is pull-only. The 159-day outage happened because nothing pushes an alert. `check_freshness_violations` (`freshness.py:217`) has no caller, `check_and_notify_alerts` runs only on manual POST, and the scheduled checker reads only `SiteIntelCollectionJob` (`monitoring.py:361-375`).
- **DatasetSpec fields.** It lacks license/redistribution, PII class, customer description, owner, SLO (as publish-to-load lag), `inputs` (which §D5 needs and the spec cannot express), schema version and primary key, `coverage_from`, origin (real/synthetic/llm), and `status_public`.
- **Mart output gates and a build ledger.** §D5 checks inputs only. There is no `mart_build` record, no tolerance check against the previous build, and no publish guard on the 13F holdings prune.
- **Consumer-facing freshness.** The coverage clock should appear as `meta.as_of` on data responses, not only on an internal page.
- **SSE auth.** EventSource cannot send an Authorization header (`index.html:10076`), so reusing `/job-queue/stream` needs a signed short-lived token or a cookie.

### Recommended changes to PLAN_085
1. Rewrite §8.2 with the correct consequence. Widen SPEC_121 to cover:
   - generic `IngestionJob` write-back in `worker/main.py` (success and failure, keyed on `job_table_id`)
   - a sweep for PENDING or RUNNING `IngestionJob` rows whose queue row is terminal
   - `_finish_ingestion_job` in a `finally` in `bulk_ingest`
   - repairing row 3891 once the DB is back
   - scheduler `job_defaults` `{misfire_grace_time by cadence, coalesce: True, max_instances: 1}`
   - `restart: unless-stopped`

   **Must land before 10-04.**
2. Add a push watchdog to SPEC_121, or as its own spec, before the portal (§6).
3. Widen SPEC_123's DatasetSpec with the fields listed above. Fold `SOURCE_REGISTRY` and `API_REGISTRY` into it. Add a unit test that every BulkSource, SOURCE_DISPATCH key, collector and mart maps to exactly one DatasetSpec.
4. Pull §D5 (mart input assertions) out of SPEC_126 and ship it ahead of the portal, combined with a `mart_build` ledger and the ship gates written as code.
5. In SPEC_124, make `POST /{key}/run` admin-role only, write the actor to the audit log, and add `requires: admin` to `can_run`. Split the portal into a read-only view and an operator view.
6. Treat "no expectation" as `unknown`, never `fresh`.
7. Re-state open question 3 using the corrected facts. Commit PLAN_085.

---

## 5. Top findings (confirmed only, severity after verifier downgrades)

| # | Finding | Sev | Evidence | Fix | Effort |
|---|---|---|---|---|---|
| 1 | Mart schedules freeze permanently after their first scheduled run | **Critical** | `scheduler_service.py:216-229, 327-336` [SV]; `worker/main.py:306`; `job_queue_service.py:167-178`; `scheduler_service.py:1222-1228`; executors `pe_marts.py`/`entity_resolve.py` never touch `IngestionJob` | Generic write-back in the worker plus an orphan sweep. Repair row 3891 before 10-10. | S |
| 2 | Generic export/preview serves every public table, including `users.password_hash`, **plaintext** `password_reset_tokens.token` (account takeover), `api_keys` and lead PII. Auth is off by default. | **Critical** | `export.py:208-226`; `export_service.py:92-95, 123-129, 176-199`; `users/auth.py:58-68, 74-80`; `main.py:1519-1520`; `docs/LAUNCH_READINESS.md:218` documents a Cloud Run deploy with `--allow-unauthenticated` **[SV]** | Explicit allowlist of product tables and views, hard-deny auth/lead/key tables, hash reset tokens, move export to an admin or API-key scope | S |
| 3 | Nothing alerts when data stops arriving (absence of jobs, worker or success) | **Critical** | `webhooks.py:394` (manual only); `freshness.py:217` (no caller); `monitoring.py:354-416` (site-intel only); `job_queue_service.py:186-210`, `batch_service.py:1022-1023` (log only); no notify in `app/worker` | Scheduled watchdog (success-only freshness, `worker_heartbeats`, stalled schedules, failed releases) to a Slack webhook set from env, plus an external dead-man's ping | M |
| 4 | No access-control model. Auth defaults off. Open register and playground signup mint JWTs. API-key `scope` is never checked (not even in `/public`). Admin mutations share routers with reads. GraphQL and job_stream are always public with no limit caps. | High | `main.py:1519-1525, 1748`; `auth.py:81-99, 105`; `users/auth.py:203, 295-323`; `api_keys.py:29-60`; `bulk.py:26-43`, `pe_firms.py:903`, `settings.py:300,323` (42 DELETE handlers); `graphql/schema.py:188-191`. `JWT_SECRET_KEY` is set (65 chars), so turning auth on is a config flip. | `REQUIRE_AUTH` defaults true, admin role on mutating routers, enforce scope in `APIKeyAuth`, close open signup or make it read-only trial, auth on GraphQL and job_stream, remove `authDevBypass` (`index.html:4798-4811`) | M |
| 5 | Missed scheduled runs are dropped (1s grace) and api/worker do not restart, ahead of the first unattended cycle | High | `scheduler_service.py:180-183, 504-511`; no `misfire` in `app/`; `docker-compose.yml:72` only | `job_defaults`, restart policy, a reconciliation pass (PLAN_085 D4) | S |
| 6 | `/health` can never report a live worker and always returns 200 | High | **[SV]** `main.py:1821-1822` closes `conn`, which is then reused at `:1827-1841`; the exception is swallowed at `:1847`, giving `worker="unknown"` and `status` still "healthy". The queries also use uppercase `'RUNNING','CLAIMED','SUCCESS','FAILED'`, but `models_queue.py:29-34` stores lowercase. | Separate `/livez`, `/readyz` (503) and `/health/data`. Read liveness from `worker_heartbeats`. Return generic DB errors (`:1851` leaks `str(e)`). | S |
| 7 | Credentials in git: `.kaggle/kaggle.json` is tracked at HEAD **[SV]**; a Cloud SQL password is in pushed history (`a1483fc`); `LAUNCH_READINESS.md:222` has a credential-bearing `DATABASE_URL` | High | `git ls-files`; `.gitignore` lacks `.kaggle/`; `49bc583` is not pushed | Rotate the DB password and Kaggle key, untrack the files, scrub the doc, add gitleaks to CI, consider filter-repo (depends on whether the repo is public; unknown) | S |
| 8 | Flagship datasets have no curated read API: 13F holdings, ADV private funds (SPEC_118 tables), insider, and the entity master (lookup by id only) | High | `pe_marts.py` (only `/build`, `/stats`); `entity_master.py:23,40,71`; `quarterly_diff.py:235-238` reads `portfolio_companies`, not bulk 13F; `public.py:170,262,340` reads only `lp_fund`/`portfolio_companies` | `/data/v1`: entities search, advisers/{crd}/private-funds, funds, holdings/13f, insider. API key + scope + `record_usage` + response models + `meta.as_of`. | L |
| 9 | CI has not run on five months of work, and every DB-backed spec test is skipped in CI | High | `git status` ahead 90; `ci.yml` never sets `TEST_PG_URL` [SV], used by 15 spec test files; `ci.yml:34` runs py3.10 vs Docker 3.11; the smoke test only sleeps | Push. Set `TEST_PG_URL` to the CI postgres service. Pin 3.11. Run `alembic upgrade head` + `alembic check`. | S |
| 10 | The DQ framework cannot see any live pipeline. The post-ingest gate and the scheduled profile/rules jobs resolve tables through `DatasetRegistry`, which bulk loaders, marts and entities never write. | High | `jobs.py:619-641` ("advisory only"); `data_profiling_service.py:419-421`; `data_quality_service.py:1145-1156`; 0 DQ hits in `app/ingest/bulk`, `app/marts`, `app/entities` | Post-load hook on DatasetSpec.tables. Error-severity rules fail the job. | M |
| 11 | A 13F zip without INFOTABLE empties `sec_13f_holdings` while the release is marked `loaded` | High | `sec_13f/parse.py:161-168` (yields nothing silently); `source.py:203-216` (prune in the same transaction); `base.py:163-179` (skipped on rerun) | Publish guard in `copy_loader`: abort if staged is 0 or the drop exceeds tolerance. Require members and headers (also Form D, `sec_form_d/parse.py:167-183`). | S |
| 12 | No license, redistribution or PII metadata. OpenAPI declares the whole service **MIT**. | High | `models.py:330-359`, `source_registry.py:35-48`, `api_registry.py:26-46` have no rights fields; Yelp, OpenCorporates, SimilarWeb, Kaggle and scraped Google data persist without gating; `main.py:1306` `license_info` MIT; `people_models.py:76-89` guessed emails with no PII tag | Rights and PII block in DatasetSpec, default `internal_only` until reviewed, enforced at export and the consumer API. Replace the MIT license_info. | M |
| 13 | Unscalable export, and not type-faithful: `fetchall()` in the API process, Parquet timestamps written as strings, no ORDER BY, `public` schema only (so `core.*` cannot be exported), files in `/tmp/exports` | High | `export_service.py:83, 176-199, 258, 275-291`; `export.py:285` (BackgroundTasks) | Worker-run, streamed via pyarrow in batches, schema-qualified allowlist, a snapshot per release to object storage | M |
| 14 | Staleness alerting counts *failed* jobs as activity | High | `monitoring.py:268-289` `MAX(created_at)` over all statuses, severity `info` | Freshness from successful landing only. Treat never succeeded or older than N×cadence as critical. | S |
| 15 | Provenance the marts compute is dropped by every read endpoint (link tiers, `is_spv_platform`, CRD, `data_sources`, first/last_seen). There is no `/pe/funds` list, so funds with a NULL firm cannot be reached. | Medium | 0 hits in `app/api` for `firm_link_method`/`is_spv_platform`/`address_confirmation`; `pe_firms.py:125-131, 556-567, 620-629` (detail `:347-361` does return cik/crd/data_sources) | Expose them plus filters `min_link_tier`, `exclude_platform`, `source=sec|manual` | S |
| 16 | Marts keep current state only: rows are never retired, `is_current=True` is hardcoded, and funds are always `'Active'` | Medium | `copy_loader.py:143-184` (upsert only); `pe_people_sec.py:368, 374`; `pe_funds_sec.py:255` | `mart_build_id`, derive `is_current` from `last_seen`, tombstones, a change table per build | M |
| 17 | Mart builds have no build ledger and no automated gates. Stages commit separately. **`dry_run` still writes firms and ADV funds** [SV]. | Medium | `pe_marts.py:23-39, 67`; the ship-gate script was a scratchpad file (log 09-20) | Fix the dry_run leak now (S). `core.mart_build` ledger mirroring `resolve_run`, with tolerance checks against the previous build. | M |
| 18 | Marts build on unchecked inputs (cron offsets only). Partial loader failures report success. | Medium | 0 `source_release` hits in marts and executors; `scheduler_service.py:1847-1861`; `bulk_ingest.py:116-120` | §D5 assertions, a tri-state job status, record consumed release keys on the build | M |
| 19 | Value errors in PE responses: `float(x) if x else None` turns a real 0 into null; `final_close_usd_millions` is actually amount sold to date; `vintage_year` is the first-sale year | Medium | `pe_firms.py:388, 582-583`; `pe_funds_sec.py:251-253` | Use `is not None`, rename or document the Form D-derived fields | S |
| 20 | Mart row → filing and build traceability: the accession is selected and then dropped; no `entity_id` on marts; `core.source_record` and companyfacts rows have no release key | Medium | `pe_funds_sec.py:35-50, 58`; `0008_entity_master.py:23-41` | Persist `source_accession`, `source_release_key`, `entity_id`, `mart_build_id` | M |
| 21 | No response contract discipline: about 33% of GETs have a `response_model`, 3 pagination styles, `detail=str(e)` 222 times, and `pe_firms` `total` ignores the strategy filter | Medium | `pe_firms.py:141-143` vs `:179-189`; `entity_master.py:76` `SELECT e.*` | Build one WHERE for both queries (S). One envelope with keyset pagination for `/data/v1`. | S/M |
| 22 | Anonymous Atlas routes make paid LLM and Places calls with no quota | Medium | `main.py:1532-1533`; `atlas.py:335-390, 481, 494` | Apply `PlaygroundQuota`/`QuotaService` | S |
| 23 | Consumer rate limiter is a non-atomic read-then-increment. Keys are accepted in the query string. DDL runs on every request. | Medium | `api_keys.py:105-178, 546-626`; `public.py:86-90, 131-142` | `INSERT … ON CONFLICT … RETURNING`, header-only keys, move DDL to Alembic | S |
| 24 | Queue has no attempt cap or dead-letter state, and a 2-minute stale reset with no ownership check (D27). Watermark advances to wall-clock time (D29). | Medium | `job_queue_service.py:88-132`; `models_queue.py` has no attempts column; `jobs.py:747-750` | Gate before any fleet revival | M |
| 25 | Schema has four owners, and a failed Alembic migration is swallowed | Medium | `migrate.py:22-50`; `database.py:78, 91-95`; `main.py:262-290`; about 40 runtime `CREATE TABLE IF NOT EXISTS` | Alembic as the only DDL path for served tables. Degrade health when not at head. Fix CLAUDE.md ("no migration tool" is stale). | M |
| 26 | Hosting is one Windows laptop: user ADC on the proxy, a `:latest` image, api/worker `depends_on` the unused local postgres, the API reserves a GPU, ports on 0.0.0.0 | High (pre-launch blocker, not a live outage) | `docker-compose.yml:54, 63-64, 69, 87, 103, 119-127, 164-166`; `.env` `DATABASE_URL` → `host.docker.internal:5435` | Short term: bind 127.0.0.1, pin the image, depend on `cloudsqlproxy`, service account. Later: Cloud Run and a scheduler service. | S → L |

**Also confirmed, lower priority:**
- The lineage subsystem is mounted but nothing writes to it (`lineage_service.py`; `/lineage` at `main.py:1599`). Wire it from DatasetSpec.inputs or delete it.
- 13F and Form D header drift loads NULL columns silently.
- `merge_staging` overwrites with NULL (`copy_loader.py:128`; impact medium-confidence).
- The row-count-drop check compares the run against the snapshot it just wrote (`jobs.py:655-676`, `data_quality_service.py:1349-1354`).
- FRED third-party copyrighted series have no rights tag (`fred/client.py:234-239`).
- Free-text provenance vocabularies. The demo quarantine rule matches `demo_seeder`, but the seeder writes `pe_ecosystem_seed` (`quarantine.py:142`, `pe_ecosystem_seed.py:45`).
- Stale source counts ("36" and "43" against 51 entries).
- The duplicate `/entities` (fuzzy resolver, 4 rows) vs `/entities/master`.
- Legacy firm twins: 49 of 78 have an SEC twin (SPEC_119).
- `pe_marts` job summary undercounts links: it leaves out the adv_* tiers (`pe_marts.py:60-62`).
- Rolling windows and one-quarter 13F holdings strand history.
- The Schedule D cron misses restatements by up to about 33 days.
- The API reserves a GPU for ML training.

---

## 6. Prioritized roadmap and spec numbering

**How the SPEC_121 collision is resolved:**
- Keep PLAN_085's **SPEC_121–126** as they are, since the plan of record already names them (amended as in §4).
- The log's "SPEC_121 = ADV Schedule A" is void. Schedule A becomes **SPEC_133**.
- Fix SPEC_119:190 and the dangling `PLAN_085_gp_people.md` link.
- New work starts at **SPEC_127**. Execution order differs from number order, and that is stated explicitly in the plan.
- Record all of this in PLAN_085 §9 and commit it.

### Now (this week: stop the bleeding; hard deadline 10-04)
| Spec | Scope |
|---|---|
| **SPEC_121** (amended) | Generic `IngestionJob` write-back in the worker, orphan sweep, `finally` in `bulk_ingest`, repair row 3891, `job_defaults` (misfire/coalesce/max_instances), `restart: unless-stopped`, `depends_on: cloudsqlproxy`, pin the proxy image |
| **SPEC_127** Access lockdown | `REQUIRE_AUTH=true` by default; export allowlist and deny list; hash reset tokens; admin role on mutating routers (bulk run/install, marts build, resolve, deletes, settings, api-keys, export cleanup); auth on GraphQL and job_stream; close open signup; ports on 127.0.0.1; quota on Atlas LLM routes; remove `authDevBypass`; replace the MIT license_info |
| **SPEC_128** Watchdog and health | Scheduled data watchdog to a Slack webhook from env; external dead-man's ping; fix `/health` (closed connection, casing, 503, `worker_heartbeats`); success-only staleness; "unknown" instead of "fresh" |
| Not a spec (ops) | Rotate the Cloud SQL password and Kaggle key; untrack `.kaggle/`; scrub `LAUNCH_READINESS.md`; **push main**; commit PLAN_085 |
| **SPEC_129** CI and guards | `TEST_PG_URL` in CI, py3.11, `alembic upgrade head`/`check`, gitleaks. Also the small correctness fixes: `pe_marts` dry_run leak, 13F INFOTABLE/header guard plus publish-guard helper, `pe_firms` strategy total, zero-coercion fix, `pe_marts` summary counting all tiers |

### Next (make it a product: 3–6 weeks)
| Spec | Scope |
|---|---|
| **SPEC_122** | Conditional GET and snapshot retention (as planned) |
| **SPEC_123** (widened) | DatasetSpec as the single catalog: rights, PII, description, owner, SLO, inputs, `coverage_from`/`through`, window, origin, `status_public`, primary key; fold in `SOURCE_REGISTRY` and `API_REGISTRY`; generate `dataset_registry`; public `GET /catalog` |
| **SPEC_126a** (pulled forward from 126) | Mart input assertions, a `core.mart_build` ledger (inputs, code version, counts), ship gates as code with tolerances against the previous build, DQ post-load hook on DatasetSpec.tables, a dependency trigger instead of cron offsets |
| **SPEC_124** | `GET /datasets/status`, read-only. The run endpoint is admin only and records the actor in the audit log. |
| **SPEC_125** | Status page (read-only public view plus operator view); delete the dead panel |
| **SPEC_130** Consumer data API `/data/v1` | API-key auth with scope and entitlement, atomic metering middleware, envelope `{data, page:{next_cursor}, meta:{as_of, release_key, license, schema_version}}`, response models; endpoints for entities search, PE firms, funds and people with provenance and tier filters, ADV private funds, 13F holdings; `statement_timeout` for the API role |
| **SPEC_131** Bulk delivery | Per-release Parquet snapshots (native types) of the entity master and crosswalk plus the PE marts, stored in GCS, served through signed URLs, with a `dataset.release_loaded` webhook |
| **SPEC_132** Mart history | `mart_build_id` on rows, `is_current` derived from `last_seen`, tombstones, a change table |
| **SPEC_126b** | Measured cadence profiles, daily Schedule D manifest check, the backfills from §D6 |

### Later
- **SPEC_133**: ADV Schedule A/B people (individual CRDs, real titles). About 594k rows are already on disk.
- **SPEC_134**: legacy firm dedup through `core.entity`, and retire `/entities` (fuzzy).
- **SPEC_135**: queue hardening D26–D29 (attempts, dead-letter, ownership checks, data-date watermark). This must come before any fleet revival.
- **SPEC_136**: hosted runtime. Cloud Run API, a scheduler service with a leader lock, service-account credentials, raw zone in GCS, split images; a dedicated-core Cloud SQL instance or read replica; a restore drill.
- **SPEC_137**: Alembic as the only DDL path; column dictionary (`COMMENT ON` generated from COLUMNS tuples) plus `/datasets/{key}/schema`.
- **SPEC_138+**: new data (BDC schedules of investments, CMS ownership, GLEIF LEI); singleton entities and `entity_id` on marts; retire or rebuild GraphQL over the entity master; wire lineage or delete it; FRED series-level rights.

---

## 7. Recommended answers to PLAN_085's open questions

1. **Scope. Show all ~81 families in the operator view**, filtered to live by default, as the plan recommends. The customer-facing catalog shows only datasets with `status_public ∈ {ga, beta}`. Dormant sources are labelled `archival` or `retired`, never shown as silently stale.
2. **Revive or retire. Retire by default.** Revive a source only if it adds an identifier or ownership edge to `core.*` (UEI, NPI, EIN, LEI, CMS ownership) or has a named consumer, and only after SPEC_135 and a license review. Blocked-on-key sources nobody plans to obtain keys for get deleted. Settle the commercial-posture question (question 1 in the list below) before answering this one.
3. **`sec_companyfacts`. The premise is wrong.** It does feed `public_company_financials`, which investor_intelligence and the synthetic trainer use. Ship SPEC_122 first, so an unchanged snapshot costs one 304 response. Then drop to monthly unless investor_intelligence has a real user. Do not pause it before checking that.
4. **"Run now". Leave it out of v1.** Add it after SPEC_127 as admin-role only, with a confirmation showing estimated bytes (from the last `source_release.bytes`) and the actor recorded in the audit log.
5. **Should the portal write? No, v1 is read-only.** Pause and resume go in the operator view only, behind the admin role and audit logging, in a later spec.

### New decisions the user needs to make
1. **Commercial posture.** Is Nexdata internal only, or will it redistribute data to clients? (Review Q10, `docs/reviews/2026-09-16_pe_collector_review.md:286`, never answered.) This decides the license gating, the PII policy and whether SPEC_130 and SPEC_131 exist at all.
2. **First product.** Recommendation: a "PE & entity pack" (crosswalk, firms, funds, GP people, ADV private funds). What it needs:
   - a per-dataset SLO (for example, marts within 1 day of their inputs)
   - a delivery mode (API, bulk, or both)
3. **Hosting and budget.** Stay on the laptop plus db-f1-micro, or move to Cloud Run with a larger instance? No SLA is possible on the current setup.
4. **Git history.** Is `rao-usha/zerostack-data` public or will it ever be shared? That decides whether a `filter-repo` purge is needed on top of rotating the credentials.
5. **PLATFORM ADVISOR / `adv_platform` tier.** Recommendation: exclude it by default from firm rankings and fund counts, with an opt-in to include it.
6. **History policy vs. the lean disk.** Recommendation: keep Cloud SQL lean and keep history as per-release Parquet in GCS. Set `HOLDINGS_RELEASES ≥ 2` once retention lands.
7. **Legacy surfaces.** Retire GraphQL, `/entities` (fuzzy), and the empty portfolio and exit endpoints, or label them?
8. **Timing.** Confirm Docker and the API are back up and SPEC_121 has landed **before 2026-10-04**. Otherwise the first unattended cycle will be skipped or frozen.