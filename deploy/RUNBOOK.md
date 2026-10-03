# Nexdata VM runbook (PLAN_089 Option B, SPEC_161)

One Compute Engine VM (`nexdata-vm`, e2-standard-2, us-central1-c, Debian 12, 60 GB pd-balanced)
runs `docker-compose.gcp.yml`: cloud-sql-proxy, one api, three workers. Cloud SQL `nexdata-pg` is
unchanged. Raw bulk files live on the VM disk at `/opt/nexdata/data/raw` and are copied to
`gs://nexdata-raw-usc1/raw` every night.

**The rule that matters:** there is exactly one scheduler. APScheduler runs inside the api process
and its job store is shared in Cloud SQL, so two api processes with the scheduler on (laptop + VM)
fire every schedule twice. The VM api only runs the scheduler in `live` mode, and `live` is only
switched on in the cutover below, after the laptop stack is stopped.

**Hard prerequisite: SPEC_160 in the image.** Before SPEC_160 the api ignores `RUN_SCHEDULER`,
always starts APScheduler, and on every start marks `running` jobs older than 2 h as `failed`.
`vm-startup.sh` therefore runs `deploy/check_scheduler_gate.py` inside the image it is about to
start and refuses `dark` and `live` unless the image has the `RUN_SCHEDULER` setting, the
Postgres leader lock (`app/core/scheduler_leader.py`), the leader-only stale-job resolver and the
`DB_POOL_SIZE` / `DB_MAX_OVERFLOW` knobs. An image without the probe (built before it existed)
is refused too. The only bypass is `NEXDATA_SKIP_SCHEDULER_GATE=1`, honoured in `live` mode only,
for when no other api (the laptop) uses the database at all.

Helper used below to run the startup script by hand with its exit status (the guest agent's
`google_metadata_script_runner startup` logs the status but does not return it):

```bash
run_startup() {  # usage: run_startup [VAR=value ...]   (on the VM)
  local f; f="$(mktemp)"
  curl -fsS -H 'Metadata-Flavor: Google' -o "$f" \
    http://metadata.google.internal/computeMetadata/v1/instance/attributes/startup-script \
    && sudo env "$@" bash "$f"; local rc=$?; rm -f "$f"; return $rc
}
```

## Moving parts

| Piece | Where | What it does |
|---|---|---|
| `deploy/vm-startup.sh` | instance metadata `startup-script` | every boot (and every deploy): install/upgrade Docker, AR auth, extract compose files from the image, render `/opt/nexdata/app.env`, install the raw-sync timer, `docker compose up` |
| `nexdata-tag` metadata | instance | image tag (git sha) to run on boot; the deploy job sets it only after the VM ran that tag successfully |
| `nexdata-mode` metadata | instance | `off` (stack down) / `dark` (default: proxy + api with `RUN_SCHEDULER=0`, no workers) / `live` (api with `RUN_SCHEDULER=1` + 3 workers) |
| `docker-compose.gcp.yml`, `deploy/compose.dark.yml`, `deploy/secrets.manifest`, `deploy/check_scheduler_gate.py`, `deploy/systemd/*` | baked into the image at `/app/deploy/`, staged in `/opt/nexdata/compose.new/`, swapped into `/opt/nexdata/compose/` only after every check passed | the compose files always match the image tag; the previous set stays in `/opt/nexdata/compose.prev/` |
| `/opt/nexdata/app.env` | VM, root 600 | app secrets from Secret Manager, one `NAME='value'` per manifest line |
| `/opt/nexdata/compose.env` | VM, root 600 | `TAG=<sha>` for compose interpolation |
| image `api:<sha>` = `worker:<sha>` | Artifact Registry | Dockerfile target `runtime`: no torch, no compiler, uid 1000, ≈1.6 GB on disk / ≈380 MB compressed (measured 2026-10-03; the laptop's `nexdata-api` image is 9.57 GB) |
| `nexdata-raw-sync.timer` | systemd | 07:30 UTC nightly `gcloud storage rsync --recursive /opt/nexdata/data/raw gs://nexdata-raw-usc1/raw` (copy only, never deletes) |
| `.github/workflows/build-push.yml` | GitHub | push to main -> deploy gate tests -> build runtime image -> push `api:<sha>`, `worker:<sha>` -> (approval) run the startup script over IAP with that tag and the AR digests -> on success set `nexdata-tag` |

Handy on the VM (`gcloud compute ssh nexdata-vm --zone us-central1-c --tunnel-through-iap`):

```bash
C="sudo docker compose -p nexdata --env-file /opt/nexdata/compose.env -f /opt/nexdata/compose/docker-compose.gcp.yml"
$C ps
$C logs api --tail 100          # dual logging: local copy as well as Cloud Logging (gcplogs)
curl -s localhost:8001/health | python3 -m json.tool
sudo journalctl -u google-startup-scripts --since -1h   # startup script output
sudo systemctl list-timers nexdata-raw-sync.timer
```

API from the laptop: `gcloud compute start-iap-tunnel nexdata-vm 8001 --local-host-port=localhost:18001 --zone us-central1-c`
(the api port is published on the VM's loopback only).

## Connection budget

Cloud SQL `max_connections = 100`. Worst case, every pool at `pool_size + max_overflow`. The main
pool is `DB_POOL_SIZE` + `DB_MAX_OVERFLOW` (SPEC_160; set explicitly in `docker-compose.gcp.yml`),
the SEC gate pool (`app/core/sec_gate.py`) and the APScheduler job store engine are fixed.

**Live (after cutover, laptop stopped):**

| Process | Pools | Each | Count | Total |
|---|---|---|---|---|
| api | main 5+10, SEC gate 1+1, APScheduler `SQLAlchemyJobStore` (own engine) 5+10, PG LISTEN bridge 1 | 33 | 1 | 33 |
| worker | main 5+10, SEC gate 1+1 | 17 | 3 | 51 |
| Cloud SQL `superuser_reserved_connections` | | | | 3 |
| **Worst case** | | | | **87** |

13 left for psql, `alembic` and the monitoring connection, **not** for the laptop: in live mode the
laptop stack is down (cutover step 6). 3 workers ×
`WORKER_MAX_CONCURRENT=6` = 18 job slots (the laptop ran 6 × 6 = 36). A 4th worker would make it 104:
first make the pool configurable and smaller (`DB_POOL_SIZE` / `DB_MAX_OVERFLOW`, SPEC_160), then
recompute this table and `tests/test_spec_161_production_image.py::test_workers_and_connection_budget`.
`app/api/v1/agentic_research.py` builds a throwaway engine per background portfolio run (+1 per run in flight).

**Dark phase (laptop live, VM api dark):** everything shares the same Cloud SQL.

| Process | Pools | Each | Count | Total |
|---|---|---|---|---|
| laptop api (unchanged) | main 5+10, SEC gate 2, job store 15, LISTEN 1 | 33 | 1 | 33 |
| laptop workers, as they run today | main 5+10, SEC gate 2 | 17 | 6 | 102 |
| VM dark api (`compose.dark.yml`: `DB_POOL_SIZE=2`, `DB_MAX_OVERFLOW=3`) | main 2+3, SEC gate 2, job store 15, LISTEN 1 | 23 | 1 | 23 |
| reserved | | | | 3 |
| **Worst case as the laptop runs today** | | | | **161** |

The laptop alone (135 + 3) is already over 100 in the worst case; adding the dark api makes it
worse. Before the dark start, with SPEC_160 on the laptop too, run the laptop with 4 workers at a
smaller pool:

```bash
WORKER_DB_POOL_SIZE=3 WORKER_DB_MAX_OVERFLOW=2 docker-compose up -d --scale worker=4 worker
```

| Process | Each | Count | Total |
|---|---|---|---|
| laptop api | 33 | 1 | 33 |
| laptop workers (main 3+2, SEC gate 2) | 7 | 4 | 28 |
| VM dark api | 23 | 1 | 23 |
| reserved | | | 3 |
| **Dark-phase worst case** | | | **87** |

Restore the laptop's worker count only if the dark start is rolled back (VM `nexdata-mode=off`).

## Secrets

`deploy/secrets.manifest` lists every variable rendered into `app.env`: `ENV_VAR SECRET_ID required|optional`,
project `nexdata-cloud`. A missing or empty `required` secret stops the boot before anything starts;
an `optional` one that does not exist is skipped. Values must be a single line without `'`.

Create each one once (values from the laptop `.env`, typed, never committed):

```bash
printf '%s' "$VALUE" | gcloud secrets create nexdata-fred-api-key --project nexdata-cloud \
  --replication-policy=user-managed --locations=us-central1 --data-file=-
# new value later:
printf '%s' "$VALUE" | gcloud secrets versions add nexdata-fred-api-key --data-file=-
```

Two values are not simply copied:

- **`nexdata-database-url`**: the VM form, `postgresql://<user>:<password>@cloudsqlproxy:5432/nexdata`.
- **`nexdata-encryption-key` (`ENCRYPTION_KEY`)**: the laptop never set it, so the Fernet key for stored
  source API keys (`source_api_keys`, `app/api/v1/settings.py`) was derived from the laptop's
  `DATABASE_URL` (`...@host.docker.internal:5435/nexdata`). Set `ENCRYPTION_KEY` to that exact old
  `DATABASE_URL` string, or every stored key becomes unreadable on the VM. Check before cutover:
  `SELECT COUNT(*) FROM source_api_keys;` (if 0, any long random value will do).

`JWT_SECRET_KEY` must equal the laptop's value or every session token is invalidated. Rotate
`KAGGLE_KEY` before uploading it (PLAN_089 Phase 0). After changing a secret, rerun the startup
script (below); the containers whose environment changed are recreated.

## Prerequisites (PLAN_089 Phase 0, not created by this repo)

- APIs: Compute, Secret Manager, Artifact Registry, IAP.
- Service account `nexdata-runtime@nexdata-cloud.iam.gserviceaccount.com` on the VM, scope `cloud-platform`, roles:
  `cloudsql.client`, `secretmanager.secretAccessor`, `storage.objectAdmin` on `gs://nexdata-raw-usc1`,
  `logging.logWriter`, `monitoring.metricWriter`, `artifactregistry.reader`.
- AR repo `us-central1-docker.pkg.dev/nexdata-cloud/nexdata` (keep-last-10 cleanup policy); bucket
  `gs://nexdata-raw-usc1` (regional, uniform access, soft delete, Nearline after 30 days).
- Firewall: SSH (22) only from IAP's range `35.235.240.0/20`; nothing else inbound.
- AR repo with immutable tags (`gcloud artifacts repositories update nexdata --location us-central1
  --immutable-tags`): a pushed `api:<sha>` can never be repointed. The deploy job also passes the
  digests it read from AR, and the VM refuses images whose digests differ.
- GitHub: variables `GCP_WIF_PROVIDER`, `GCP_PUSH_SERVICE_ACCOUNT`, `GCP_DEPLOY_SERVICE_ACCOUNT`
  (optional `NEXDATA_VM_NAME`, `NEXDATA_VM_ZONE`), environment `production` with required reviewers
  and deployment branches limited to `main`. Roles are listed in the workflow header.
- The image must include SPEC_160 (see the top of this page); `vm-startup.sh` enforces it.
- Merge SPEC_160 to main before SPEC_161 (or together): every push to main builds an image the
  deploy job can roll out, and a pre-SPEC_160 image is refused by the VM in dark and live mode.

### GitHub -> GCP trust (Workload Identity Federation)

Scope each service account to exactly one kind of GitHub token. A repo-wide `principalSet` would
let any workflow on any branch impersonate the deploy SA, which can set `startup-script` metadata
(root on the VM, so every secret) without passing the `production` approval.

```bash
PROJECT_NUMBER=$(gcloud projects describe nexdata-cloud --format 'value(projectNumber)')
POOL=github; PROVIDER=zerostack-data; REPO=rao-usha/zerostack-data
gcloud iam workload-identity-pools create $POOL --project nexdata-cloud --location global
gcloud iam workload-identity-pools providers create-oidc $PROVIDER --project nexdata-cloud \
  --location global --workload-identity-pool $POOL \
  --issuer-uri https://token.actions.githubusercontent.com \
  --attribute-mapping 'google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref' \
  --attribute-condition "assertion.repository == '$REPO'"
P=principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/subject
# push SA: only the main branch (the build job has no environment, so sub = ...:ref:refs/heads/main)
gcloud iam service-accounts add-iam-policy-binding gh-push@nexdata-cloud.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser --member "$P/repo:$REPO:ref:refs/heads/main"
# deploy SA: only jobs running in the approved `production` environment
gcloud iam service-accounts add-iam-policy-binding gh-deploy@nexdata-cloud.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser --member "$P/repo:$REPO:environment:production"
```

`GCP_WIF_PROVIDER` is then `projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/providers/zerostack-data`.

## Dark start (PLAN_089 Phase 3)

The VM runs the api against Cloud SQL with the scheduler off and no workers, while the laptop stays
the system of record.

Before step 1:

- The tag must be an image with SPEC_160; otherwise the boot stops at `scheduler gate` and nothing
  starts. (Pre-SPEC_160, a dark api would have fired every schedule a second time and, on every
  boot and every approved deploy, failed the laptop's jobs running for over 2 h.)
- Shrink the laptop's connection use (Connection budget, dark phase).
- Every dark api start still runs `alembic upgrade head` from the VM image against the shared
  Cloud SQL. Deploy to the VM only commits the laptop already runs, or whose migrations the
  laptop's code tolerates.

1. Create the VM (once):
   ```bash
   gcloud compute instances create nexdata-vm --project nexdata-cloud --zone us-central1-c \
     --machine-type e2-standard-2 --image-family debian-12 --image-project debian-cloud \
     --boot-disk-size 60GB --boot-disk-type pd-balanced --no-address \
     --service-account nexdata-runtime@nexdata-cloud.iam.gserviceaccount.com --scopes cloud-platform \
     --metadata-from-file startup-script=deploy/vm-startup.sh \
     --metadata nexdata-mode=dark,nexdata-tag=<sha pushed by build-push>,enable-oslogin=TRUE
   ```
   (`--no-address` needs Cloud NAT for outbound traffic; otherwise drop it and keep the firewall closed.)
2. Watch the boot: `gcloud compute instances get-serial-port-output nexdata-vm --zone us-central1-c | grep nexdata-startup`.
   Expect `mode dark ...` and `api ready`.
3. Smoke test over the IAP tunnel: `/readyz` 200, `/health` shows `database: connected`; a read-only
   endpoint with an API key; `docker compose ... ps` shows no worker.
4. Restore raw files (Phase 4): `sudo gcloud storage rsync --recursive gs://nexdata-raw-usc1/raw /opt/nexdata/data/raw`
   after the laptop has pushed its copy, then rerun the startup script (it fixes ownership to uid 1000).
   Verify a few files against `raw.source_release.sha256`.

## Cutover: exactly one scheduler (PLAN_089 Phase 5)

Window of about 2 h. Avoid 05:10-06:20 UTC and the 4th-10th of the month (monthly marts). Target 2026-10-11.

1. On the laptop, confirm nothing is running or claimed:
   `curl -s localhost:8001/health` -> `queue.running == 0`; also
   `SELECT id, job_type, status FROM job_queue WHERE LOWER(status) IN ('claimed','running');` returns no rows.
2. Stop the laptop stack: `docker-compose stop api worker frontend` (workers drain for 30 s).
3. Check Cloud SQL for leftover laptop sessions:
   `SELECT client_addr, application_name, state, backend_start FROM pg_stat_activity WHERE datname = 'nexdata';`
   Only the VM's proxy sessions may remain (dark api). Kill strays with `pg_terminate_backend(pid)`.
4. Switch the VM to live (this is the only place `nexdata-mode=live` is set):
   ```bash
   gcloud compute instances add-metadata nexdata-vm --zone us-central1-c --metadata nexdata-mode=live
   gcloud compute ssh nexdata-vm --zone us-central1-c --tunnel-through-iap
   # on the VM: define run_startup (top of this page), then
   run_startup NEXDATA_MODE=live; echo "exit $?"
   ```
   The api is recreated with `RUN_SCHEDULER=1` and three workers start. A non-zero exit means
   nothing was switched if it stopped before `VM now points at tag` (gate, running jobs, secrets).
5. Verify: `/health` -> `workers_alive: 3`; the api log shows the scheduler starting with the
   jobs from `apscheduler_jobs` (35 on 2026-09-29) exactly once; `GET /api/v1/watchdog` is clean; the
   `HEARTBEAT_PING_URL` check (healthchecks.io) receives its ping; the next scheduled bulk job is
   claimed by a VM worker (`worker_heartbeats` host names are the VM container ids).
6. Disable the laptop stack so a reboot cannot bring a second scheduler back:
   `docker-compose down` and remove any autostart (Docker Desktop "start on login", `scripts/start_service.*`).
   From now on the laptop never runs `docker-compose up` against Cloud SQL with the scheduler on.

Note: with SPEC_160 the stale-job resolver runs only in the scheduler leader and skips jobs whose
queue row is still live; step 1 is still what makes recreating the api and workers in step 4 safe.

## Deploying a new version

Push to main -> deploy gate tests -> `Build, push and deploy` builds and pushes `<sha>` -> approve the
`production` environment -> the job runs the startup script over IAP with `NEXDATA_TAG=<sha>` and the
AR digests, in the VM's current mode -> only if that exits 0 does it set `nexdata-tag=<sha>` (what
the next reboot runs).

The script stages everything first and changes nothing the stack uses until all checks pass
(digests, scheduler gate, running jobs in live mode, every required secret). A refusal fails the
deploy job and leaves `/opt/nexdata/compose`, `app.env`, `compose.env` and the `nexdata-tag`
metadata on the running version; rerun the job later. To override the running-jobs check on the VM,
accepting that running jobs are restarted by stale-heartbeat recovery:

```bash
run_startup NEXDATA_TAG=<sha> NEXDATA_FORCE=1
gcloud compute instances add-metadata nexdata-vm --zone us-central1-c --metadata nexdata-tag=<sha>
```

A boot run and a deploy run never overlap (`flock` on `/opt/nexdata/.startup.lock`).

## Docker Engine upgrades

The Docker packages are held (`apt-mark hold`) so unattended-upgrades never restarts dockerd under a
running job, and `live-restore` keeps containers up across a daemon restart. The startup script
upgrades Docker only while no `nexdata` container runs: set `nexdata-mode=off`, rerun the script (the
stack stops and Docker is upgraded on that run), then set the mode back and rerun.

## Rollback

- **Bad image:** run the workflow by hand (`Actions -> Build, push and deploy -> Run workflow`) with the
  previous sha (`gcloud artifacts docker images list us-central1-docker.pkg.dev/nexdata-cloud/nexdata/api --include-tags`).
  The compose files come from that image, so they roll back with it.
- **Back to the laptop (reverse cutover):** wait for no running jobs; set `nexdata-mode=dark` (or `off`) and
  rerun the startup script (`run_startup`), which removes the workers and restarts the api without the scheduler; check
  `pg_stat_activity`; then `docker-compose up -d` on the laptop. Raw files written on the VM since cutover
  are in `gs://nexdata-raw-usc1/raw` after the nightly sync (run `sudo systemctl start nexdata-raw-sync`
  first to copy them now); pull them to the laptop with `gcloud storage rsync`.
- **VM lost:** create a new VM with the same command and the last good tag; restore raw files from the
  bucket (step 4 of the dark start). Cloud SQL is unaffected.

## Not covered here

TLS / public exposure (Caddy, IAP for HTTP, or Cloudflare Tunnel), the frontend (nginx serving
`frontend/`, PLAN_089 Phase 8), Cloud Monitoring alerts and snapshot schedules (PLAN_089 Phases 6-7).
`data/kaggle` is on the VM disk (`/opt/nexdata/data/kaggle`) but not synced to GCS: it is a
re-downloadable cache. Nothing reads `data/seeds` at runtime.

Unverified until the first real VM / GitHub run: the script has only been checked with `bash -n`
and static tests; the gcplogs driver, OS Login over IAP and the WIF bindings have not run.
