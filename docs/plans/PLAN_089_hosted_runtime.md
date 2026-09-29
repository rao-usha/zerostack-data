# PLAN 089 — Hosted runtime (laptop → GCP)

**Status:** Draft for decision · **Date:** 2026-09-29
**Research:** read-only `gcloud describe/list`, `docker stats`, read-only SQL on Cloud SQL, official GCP pricing pages (retrieved 2026-09-29). Nothing created or modified in GCP.

**Recommendation:** Option B now: one Compute Engine VM running a hardened copy of the compose stack, with GCS as the raw archive. Expected ≈ $62/mo, or ≈ $44 with a 1-year CUD, plus the existing Cloud SQL at ≈ $53. Move the API to Cloud Run (Option C) once external API customers exist.

---

## 0. Facts that shape the design (measured)

| Fact | Evidence | Why it matters |
|---|---|---|
| APScheduler starts unconditionally in every API process; the job store is shared in Cloud SQL (`apscheduler_jobs`, 35 jobs) | `app/main.py:382-798, 827-840`; `scheduler_service.py:229-241`; APScheduler 3.10.4 | Two API processes (laptop + cloud, or 2 Cloud Run instances) mean every schedule fires twice. There is no flag to turn the scheduler off. This is the main hazard for running old and new side by side. |
| Every API start marks running jobs older than 2 h as failed | `app/main.py:341-349` | An API restart or scale-out during a 4 h CMS or mart job corrupts that job's status. |
| Other always-on loops live in the API: PG LISTEN bridge, watchdog, stale-job reset, pending auto-cancel | `main.py:820, 450, 497-503, 514-520` | These need CPU all the time, which Cloud Run's request-based billing throttles. |
| Workers claim jobs with `FOR UPDATE SKIP LOCKED`, send heartbeats, and drain for 30 s on SIGTERM; jobs with a stale heartbeat return to pending after 2 min | `worker/main.py:108-146, 46, 536-583`; `job_queue_service.py:88-132` | Workers on two hosts are safe for the queue. A killed instance restarts a multi-hour job from scratch. |
| Raw bulk files are addressed by relative path (`raw.source_release.local_path`, `bulk_raw_dir="data/raw"`) | `config.py:241-244`; `ingest/bulk/base.py:365-385` | Keep `/app/data/raw` on the new host. A missing file just triggers a re-download. |
| data/raw is 7.0 GB (submissions 3.0, companyfacts 2.7, 13F 1.1); data/reports is 826 MB | `du` | Size the disk and bucket for 15-30 GB. |
| Images are stale and the code is bind-mounted; both services share one Dockerfile and requirements, which include torch | `nexdata-worker` 1.74 GB (5 months old), `nexdata-api` 9.57 GB; `requirements.txt:83` | torch is used only by the offline trainers. Split out a training image; the runtime image should be about 1.5-2 GB. Also drop the GPU reservation (`docker-compose.yml:129-135`). |
| Cloud SQL `nexdata-pg`: POSTGRES_14 ENTERPRISE, db-custom-1-3840 (dedicated core), ZONAL us-central1-c, 15 GB PD_SSD with auto-resize, 7 backups, PITR on, public IPv4 reached only through the proxy | `gcloud sql instances describe` | |
| max_connections = 100; 45 connections today, 8 of them "idle in transaction"; pool is 5 + 10 overflow per process | live query; `database.py:63-64` | Worst case is 6 workers × 15 plus about 31 for the API ≈ 121, which exceeds 100. Reduce the worker count or make the pool size configurable. |
| Project state: Compute, Cloud Scheduler, Budgets and Billing-catalog APIs are not enabled. Existing: bucket `nexdata-cloud_cloudbuild`, AR repo `wildcard`, Cloud Run `wildcard-workbench` | gcloud | Free tiers are per billing account and are partly used by wildcard already. |

## 1. Options

### A. Cloud Run native
- **Layout:**
  - API: a Cloud Run service (1 vCPU / 2 GiB, min 1).
  - Workers: a Cloud Run worker pool (2 × 1 vCPU / 4 GiB) running `python -m app.worker.main`.
  - Variant A2 uses on-demand Cloud Run Jobs instead (task timeout up to 168 h).
  - Frontend: a small nginx service.
- **Code changes needed:**
  - Extract the scheduler behind a `RUN_SCHEDULER` flag and a `pg_try_advisory_lock` leader lock, or convert the 35 schedules to Cloud Scheduler.
  - Gate the startup stale-job resolver.
  - Rework raw storage. The Cloud Run filesystem is in memory, the ephemeral disk is Preview, and GCS FUSE renames are copy+delete. The cleanest fix is download-to-scratch then upload to `gs://`.
  - Use the Cloud SQL socket, and move secrets to `--set-secrets`.
- **Pros:** no OS to manage, managed TLS and IAM, per-revision rollback.
- **Cons:** the most code change; CPU throttling breaks the in-process loops unless billing is instance-based; platform restarts kill multi-hour jobs.

### B. Single Compute Engine VM running compose (recommended now)
- **Layout:**
  - `e2-standard-2` (2 vCPU / 8 GB) in us-central1-c, the same zone as Cloud SQL.
  - 60 GB pd-balanced disk with daily snapshots.
  - Native Linux Docker Engine, not Docker Desktop/WSL2. That removes this week's hang.
- **Changes:**
  - `docker-compose.gcp.yml`: Artifact Registry images, no bind mounts, no local postgres, no GPU.
  - The proxy authenticates as the VM service account.
  - Secrets from Secret Manager rendered into `.env` at boot.
  - `data/raw` on the persistent disk, rsynced nightly to GCS.
  - 3 workers (or a configurable pool size) to stay under 100 connections.
- **Pros:** zero behaviour change for long jobs, the in-process scheduler and 1.5 GB files. Cheapest and flat cost, and the same mental model as today.
- **Cons:**
  - OS patching (use COS or unattended-upgrades).
  - A single zonal VM, though Cloud SQL is already zonal in the same zone.
  - You expose the API yourself: Caddy with TLS, IAP or Cloudflare Tunnel.

### C. Hybrid: VM for workers and scheduler, Cloud Run for the API
- The B VM runs workers and the scheduler (`RUN_SCHEDULER=1`). The Cloud Run API runs with `RUN_SCHEDULER=0`, min 1 instance, over the Cloud SQL socket.
- The natural next step after B, once external API consumers exist.

## 2. Monthly cost (USD, us-central1, list prices 2026-09-29)

### Unit prices used

| Item | Price | Source |
|---|---|---|
| Cloud Run request-based active | $0.000024/vCPU-s, $0.0000025/GiB-s | [run/pricing](https://cloud.google.com/run/pricing) |
| Cloud Run idle min-instance | $0.0000025/vCPU-s, $0.0000025/GiB-s | same |
| Cloud Run instance-based and Jobs | $0.000018/vCPU-s, $0.000002/GiB-s | same |
| Cloud Run worker pools | $0.000011244/vCPU-s, $0.000001235/GiB-s | same |
| Cloud Run jobs task timeout | up to 168 h | [task-timeout](https://cloud.google.com/run/docs/configuring/task-timeout) |
| e2-medium | $0.0335/h | [general-purpose pricing](https://cloud.google.com/products/compute/pricing/general-purpose) |
| e2-standard-2 | $0.0670/h; 1y CUD $0.0422/h | same |
| e2-standard-4 | $0.1340/h; 1y CUD $0.0844/h | same |
| E2 sustained-use discounts | not eligible | [SUD docs](https://docs.cloud.google.com/compute/docs/sustained-use-discounts) |
| pd-balanced | ≈ $0.10/GiB-mo | [disks-image-pricing](https://cloud.google.com/compute/disks-image-pricing) |
| Snapshots | ≈ $0.05/GiB-mo | same |
| External IPv4 on a VM | $0.005/h ≈ $3.65/mo | [network-pricing](https://cloud.google.com/vpc/network-pricing) |
| Internet egress to North America | $0.12/GiB after 1 GiB | same |
| Same-zone traffic to Cloud SQL | free | same |
| GCS Standard, regional | ≈ $0.020/GiB-mo | [storage/pricing](https://cloud.google.com/storage/pricing) |
| Artifact Registry | ≈ $0.10/GB-mo after 0.5 GB | [artifact-registry/pricing](https://cloud.google.com/artifact-registry/pricing) |
| Secret Manager | ≈ $0.06/version-mo after 6 | [secret-manager/pricing](https://cloud.google.com/secret-manager/pricing) |
| Cloud Scheduler | $0.10/job-mo after 3 | [scheduler/pricing](https://cloud.google.com/scheduler/pricing) |
| Cloud Logging | $0.50/GiB after 50 GiB/mo | [stackdriver/pricing](https://cloud.google.com/stackdriver/pricing) |
| Cloud Monitoring alerting | free until 2027-09-01 | same |
| Cloud SQL Enterprise | vCPU $0.0413/h, memory $0.007/GiB-h, SSD ≈ $0.17/GB-mo, backups ≈ $0.08/GB-mo | [sql/pricing](https://cloud.google.com/sql/pricing) |

### Existing Cloud SQL (unchanged by any option)

| Item | Cost |
|---|---|
| 1 vCPU | $30.15 |
| 3.75 GiB memory | $19.16 |
| 15 GB SSD | $2.55 |
| Backups | ≈ $0.6-1.6 |
| **Total** | **≈ $53/mo** |

Notes:
- No tier bump is needed for the move. db-custom-2-7680 ≈ $102/mo if the monthly marts pin the CPU.
- The laptop's reads of Cloud SQL are currently billed as internet egress; that goes to $0 in-zone (volume not measured).

### Totals excluding Cloud SQL

| Option | Low | Expected | High |
|---|---|---|---|
| **B** (e2-medium / e2-standard-2 / e2-standard-4 + disk, snapshots, IP, GCS, AR, secrets, egress) | ≈ $34 | **≈ $62** (1y CUD ≈ $44) | ≈ $123 |
| C (B's VM + Cloud Run API) | ≈ $55 | ≈ $85 | ≈ $180 |
| A (Cloud Run API min 1 + worker pool 1/2/3 × $42.53 + GCS etc.) | ≈ $54 | ≈ $111 | ≈ $200 |
| A2 (Jobs at 100-300 task-h) | ≈ $45 | ≈ $65 | ≈ $85 |

### Totals including Cloud SQL

| Option | Total |
|---|---|
| **B** | **≈ $115/mo** (≈ $97 with CUD) |
| C | ≈ $138 |
| A | ≈ $164 |

**Assumptions:** API active 10-30 h/mo; workers always on (bulk and mart activity ≈ 60-150 task-h/mo); logs under 50 GiB; user egress under 50 GiB.

**Not verified:**
- The Cloud Run request timeout page was not re-fetched.
- Whether worker pools support volume mounts.
- Whether adding private IP restarts Cloud SQL.
- How much of the free tiers wildcard-workbench already uses.
- Actual backup size.
- Peak worker RAM while parsing 1.5 GB files.
- Current Cloud SQL egress to the laptop.
- Budgets: the Budgets API is disabled and the account lacks billing-account permission.

## 3. Recommendation

Choose B now, with C later, and don't build A yet.

1. **Long jobs and large files work unchanged on a VM disk.** Cloud Run brings in-memory filesystems, restarts that re-run hours-long jobs, and API starts that fail jobs older than 2 h.
2. **It fixes the actual failure mode.** Docker Desktop/WSL2 hangs and laptop sleep are replaced by an always-on Linux Docker Engine with `restart: unless-stopped`.
3. **The ops load stays small for a solo developer:** one host, one compose file, one deploy command.
4. **There is exactly one scheduler by construction**, one API process, until the leader lock exists.
5. **It's the cheapest and the cost is flat.** Commit to a 1-year CUD only after 1-2 months of real usage data.
6. **Nothing is wasted.** B's prerequisites (scheduler flag, image split, production compose, GCS sync) are the same ones C needs.

**Risks:**
- A single zonal VM, the same failure domain as Cloud SQL.
- OS patching.
- Exposing the API over TLS.

Mitigate with COS or unattended-upgrades, a snapshot schedule, a rebuild runbook, and Caddy or IAP.

## 4. Phased migration plan

**Phase 0: foundations (0.5-1 day)**
- Enable the Compute, Budgets and (optionally) Cloud Scheduler APIs.
- Budget of ≈ $150/mo with alerts at 50/90/100% (needs billing-account admin).
- Service account `nexdata-runtime` with `cloudsql.client`, `secretmanager.secretAccessor`, `storage.objectAdmin` on the raw bucket, `logging.logWriter`, `monitoring.metricWriter` and `artifactregistry.reader`.
- AR repo `nexdata` with a keep-last-10 cleanup policy.
- Bucket `gs://nexdata-raw-usc1`: regional, uniform access, soft delete, Nearline after 30 days.
- Move only the secrets actually used into Secret Manager.
- Rotate the Kaggle key. The DB password was already rotated.

**Phase 1: runtime split code (1.5-2.5 days, SPEC_136a)**
- `RUN_SCHEDULER` flag plus a `pg_try_advisory_lock` leader lock around `start_scheduler()` and the `add_job` blocks.
- Gate or remove the startup stale-job resolver (`main.py:341-349`).
- Split `requirements-runtime.txt` from `requirements-train.txt`, with a multi-target Dockerfile and no GPU.
- Configurable `DB_POOL_SIZE` / `DB_MAX_OVERFLOW`; investigate the idle-in-transaction sessions.
- `docker-compose.gcp.yml`: AR images, no bind mounts or local postgres, proxy on the VM service account, `gcplogs` logging driver, 3 workers × `WORKER_MAX_CONCURRENT=6`.

**Phase 2: CI/CD (1 day, SPEC_136b)**
- GitHub Actions: tests → build the runtime image → push to AR (`:sha`) using Workload Identity Federation, with no JSON keys.
- Manual-approval deploy via `gcloud compute ssh --tunnel-through-iap … docker compose pull && up -d`.
- Rollback by redeploying the previous SHA.

**Phase 3: VM, dark (0.5-1 day)**
- e2-standard-2, us-central1-c, 60 GB pd-balanced, Ops Agent, snapshots kept 7 days.
- IAP SSH only.
- Start just the proxy and the API with `RUN_SCHEDULER=0` and no workers, then smoke-test.
- **Do not start a cloud API while the laptop API runs until Phase 1 has landed** (two schedulers).

**Phase 4: data/raw to GCS (0.5 day)**
- rsync laptop → bucket → VM disk at the same path, and verify against `raw.source_release.sha256`.
- Nightly local → GCS sync; later, a post-load upload hook and Parquet history in GCS.

**Phase 5: cutover, exactly one scheduler (≈ 2 h window)**
- Avoid 05:10-06:20 UTC and the 4th-10th of each month.
- The first unattended cycle starts 2026-10-04. Either finish Phases 1-4 and cut over by 2026-10-02, or run October on the laptop and cut over around 2026-10-11.
- Steps:
  1. Wait until there are no running or claimed jobs.
  2. Stop the laptop api, workers and frontend.
  3. Check `pg_stat_activity` for any laptop sessions left.
  4. Start the VM stack with `RUN_SCHEDULER=1`.
  5. Verify `/health`, the watchdog and the `HEARTBEAT_PING_URL` ping, and the next scheduled bulk job.
  6. Disable the laptop stack.
- Rollback: the reverse.

**Phase 6: monitoring (0.5 day)**
- The SPEC_128 watchdog posting to Slack, plus the healthchecks.io dead-man.
- Cloud Monitoring:
  - uptime check on `/readyz`
  - memory, disk and CPU alerts
  - log alerts on `Poll loop error` and `Drain timeout`
  - Cloud SQL CPU and connection alerts
- `systemctl enable docker`.

**Phase 7: cost controls (0.25 day)**
- Budget alerts.
- AR cleanup and GCS lifecycle rules.
- Log exclusions.
- Cloud SQL storage auto-resize limit.
- 1-year CUD after 60 days (saves ≈ $18/mo).

**Phase 8, later: Option C (1-2 days)**
- API on Cloud Run with `RUN_SCHEDULER=0`, once `/data/v1` and external consumers exist.
- Frontend API base changes (`frontend/nginx.conf:15, 24`).

**Effort:** ≈ 5-7 working days for Phases 0-7; +1-2 days for C; full A would add ≈ 4-6 days.

## Notes
- CLAUDE.md is stale in two places:
  - It says `deploy.replicas` is Swarm-only, but 6 workers run under Compose v2.
  - It says there is "no migration tool", but Alembic is in use.
- The unused local `postgres` container still runs, and api/worker `depends_on` it. Drop it in the production compose file.
