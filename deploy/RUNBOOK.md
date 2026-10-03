# Nexdata VM runbook (PLAN_089 Option B, SPEC_161)

One Compute Engine VM (`nexdata-vm`, e2-standard-2, us-central1-c, Debian 12, 60 GB pd-balanced)
runs `docker-compose.gcp.yml`: cloud-sql-proxy, one api, three workers. Cloud SQL `nexdata-pg` is
unchanged. Raw bulk files live on the VM disk at `/opt/nexdata/data/raw` and are copied to
`gs://nexdata-raw-usc1/raw` every night.

**The rule that matters:** there is exactly one scheduler. APScheduler runs inside the api process
and its job store is shared in Cloud SQL, so two api processes with the scheduler on (laptop + VM)
fire every schedule twice. The VM api only runs the scheduler in `live` mode, and `live` is only
switched on in the cutover below, after the laptop stack is stopped.

## Moving parts

| Piece | Where | What it does |
|---|---|---|
| `deploy/vm-startup.sh` | instance metadata `startup-script` | every boot (and every deploy): install/upgrade Docker, AR auth, extract compose files from the image, render `/opt/nexdata/app.env`, install the raw-sync timer, `docker compose up` |
| `nexdata-tag` metadata | instance | image tag (git sha) to run |
| `nexdata-mode` metadata | instance | `off` (stack down) / `dark` (default: proxy + api with `RUN_SCHEDULER=0`, no workers) / `live` (api with `RUN_SCHEDULER=1` + 3 workers) |
| `docker-compose.gcp.yml`, `deploy/compose.dark.yml`, `deploy/secrets.manifest`, `deploy/systemd/*` | baked into the image at `/app/deploy/`, extracted to `/opt/nexdata/compose/` | the compose files always match the image tag; the previous set stays in `/opt/nexdata/compose.prev/` |
| `/opt/nexdata/app.env` | VM, root 600 | app secrets from Secret Manager, one `NAME='value'` per manifest line |
| `/opt/nexdata/compose.env` | VM, root 600 | `TAG=<sha>` for compose interpolation |
| image `api:<sha>` = `worker:<sha>` | Artifact Registry | Dockerfile target `runtime`: no torch, no compiler, uid 1000, ≈1.6 GB on disk / ≈380 MB compressed (measured 2026-10-03; the laptop's `nexdata-api` image is 9.57 GB) |
| `nexdata-raw-sync.timer` | systemd | 07:30 UTC nightly `gcloud storage rsync --recursive /opt/nexdata/data/raw gs://nexdata-raw-usc1/raw` (copy only, never deletes) |
| `.github/workflows/build-push.yml` | GitHub | green CI on main -> build runtime image -> push `api:<sha>`, `worker:<sha>` -> (approval) set `nexdata-tag`, rerun the startup script over IAP |

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

Cloud SQL `max_connections = 100`. Pools are hard-coded today (`app/core/database.py`, `app/core/sec_gate.py`);
worst case, every pool at `pool_size + max_overflow`:

| Process | Pools | Each | Count | Total |
|---|---|---|---|---|
| api | main 5+10, SEC gate 1+1, APScheduler `SQLAlchemyJobStore` (own engine) 5+10, PG LISTEN bridge 1 | 33 | 1 | 33 |
| worker | main 5+10, SEC gate 1+1 | 17 | 3 | 51 |
| Cloud SQL `superuser_reserved_connections` | | | | 3 |
| **Worst case** | | | | **87** |

13 left for psql, `alembic`, the monitoring connection and the laptop while it still runs. 3 workers ×
`WORKER_MAX_CONCURRENT=6` = 18 job slots (the laptop ran 6 × 6 = 36). A 4th worker would make it 104:
first make the pool configurable and smaller (`DB_POOL_SIZE` / `DB_MAX_OVERFLOW`, SPEC_160), then
recompute this table and `tests/test_spec_161_production_image.py::test_workers_and_connection_budget`.
`app/api/v1/agentic_research.py` builds a throwaway engine per background portfolio run (+1 per run in flight).

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
- GitHub: variables `GCP_WIF_PROVIDER`, `GCP_PUSH_SERVICE_ACCOUNT`, `GCP_DEPLOY_SERVICE_ACCOUNT`
  (optional `NEXDATA_VM_NAME`, `NEXDATA_VM_ZONE`), environment `production` with required reviewers.
  Roles are listed in the workflow header.
- The image must include SPEC_160 (`RUN_SCHEDULER` honoured) before the VM api starts while the laptop
  runs. Without it the dark api still runs a scheduler: do not start it until cutover.

## Dark start (PLAN_089 Phase 3)

The VM runs the api against Cloud SQL with the scheduler off and no workers, while the laptop stays
the system of record.

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
   gcloud compute ssh nexdata-vm --zone us-central1-c --tunnel-through-iap --command 'sudo google_metadata_script_runner startup'
   ```
   The api is recreated with `RUN_SCHEDULER=1` and three workers start.
5. Verify: `/health` -> `workers_alive: 3`; the api log shows the scheduler starting with the
   jobs from `apscheduler_jobs` (35 on 2026-09-29) exactly once; `GET /api/v1/watchdog` is clean; the
   `HEARTBEAT_PING_URL` check (healthchecks.io) receives its ping; the next scheduled bulk job is
   claimed by a VM worker (`worker_heartbeats` host names are the VM container ids).
6. Disable the laptop stack so a reboot cannot bring a second scheduler back:
   `docker-compose down` and remove any autostart (Docker Desktop "start on login", `scripts/start_service.*`).
   From now on the laptop never runs `docker-compose up` against Cloud SQL with the scheduler on.

Note: every api start marks running jobs older than 2 h as failed (`app/main.py`, until SPEC_160
gates it). Step 1 is what makes the api recreation in step 4 safe.

## Deploying a new version

Push to main -> CI green -> `Build, push and deploy` builds and pushes `<sha>` -> approve the
`production` environment -> the VM's `nexdata-tag` becomes `<sha>` and the startup script reruns
in the current mode. The script refuses to recreate workers while `/health` reports running jobs
(the deploy job fails; rerun it later). To override on the VM, accepting that running jobs are
restarted by stale-heartbeat recovery, run the script directly with the override set:

```bash
curl -s -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/startup-script \
  | sudo NEXDATA_FORCE=1 bash
```

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
  rerun the startup script, which removes the workers and restarts the api without the scheduler; check
  `pg_stat_activity`; then `docker-compose up -d` on the laptop. Raw files written on the VM since cutover
  are in `gs://nexdata-raw-usc1/raw` after the nightly sync (run `sudo systemctl start nexdata-raw-sync`
  first to copy them now); pull them to the laptop with `gcloud storage rsync`.
- **VM lost:** create a new VM with the same command and the last good tag; restore raw files from the
  bucket (step 4 of the dark start). Cloud SQL is unaffected.

## Not covered here

TLS / public exposure (Caddy, IAP for HTTP, or Cloudflare Tunnel), the frontend (nginx serving
`frontend/`), Cloud Monitoring alerts and snapshot schedules (PLAN_089 Phases 6-7).
