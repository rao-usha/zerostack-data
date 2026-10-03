# SPEC 161 — Production image, GCP compose and CI build

**Status:** Implemented (repo only; no GCP resources created, nothing pushed or deployed)
**Task type:** service
**Date:** 2026-10-03
**Plan:** `docs/plans/PLAN_089_hosted_runtime.md` (Option B: one VM running compose + GCS raw archive), Phases 1-2, second half
**Test file:** tests/test_spec_161_production_image.py
**Pairs with:** SPEC_160 (first half of Phase 1: the `RUN_SCHEDULER` flag / leader lock, startup stale-job resolver, pool sizing). This spec does not touch `app/`.

## Goal

Make the code deployable to the PLAN_089 VM without the laptop's assumptions: an image without torch
and without bind-mounted code, a compose file the VM can run from images alone, a boot script that
renders secrets and starts the stack, a nightly copy of `data/raw` to GCS, and a CI path that builds
and pushes the image and deploys it only after a human approves.

## Measured facts (2026-10-03)

| Fact | Evidence |
|---|---|
| torch, einops are imported only by `app/services/synthetic/training/*` (W2 skeletons, not reachable from any router or worker) | static import scan |
| wandb is imported nowhere | static import scan |
| lightgbm (`synthetic_crowd` router, `ml/probability_model`), scipy, sklearn, pyarrow (`export_service` parquet) are used by the running app | static import scan; they stay in runtime |
| No test imports torch / einops / wandb; tests import PyYAML, which was never declared | `tests/` import scan |
| `app/api/v1/settings.py` derives the Fernet key for stored API keys from `ENCRYPTION_KEY`, falling back to `DATABASE_URL` | `settings.py:35-40` |
| Connection pools: main engine 5+10, SEC gate 1+1, APScheduler job store (own engine, default 5+10, api only), PG LISTEN bridge 1 (api only) | `database.py:60-66`, `sec_gate.py:549-560`, `scheduler_service.py:239`, `pg_listener.py:50` |
| There is no `.dockerignore`; `docker build .` ships `data/` (7 GB raw) and `.env` in the context | `ls -a` |

## Deliverables

1. **Requirements split.** `requirements-runtime.txt` (api + worker), `requirements-train.txt`
   (`-r runtime` + torch, einops, wandb), `requirements-dev.txt` (pytest, pytest-asyncio, pytest-cov,
   PyYAML). `requirements.txt` = `-r` of all three, so local dev is unchanged.
2. **Dockerfile, multi-target.** `builder` (gcc + uv into `/opt/venv`), `builder-train`, `os-base`
   (runtime apt libs, uid 1000 `appuser`), `train`, and `runtime` (last stage = default target):
   no compiler, no torch, non-root, `HEALTHCHECK` on `/livez`. `.dockerignore` keeps `data/`,
   `.env*`, `.git`, frontend, docs and tests out of the context. Local `docker-compose.yml`: api
   builds `target: train` (keeps the GPU trainer path), worker builds `target: runtime`.
3. **`docker-compose.gcp.yml`.** Images `us-central1-docker.pkg.dev/nexdata-cloud/nexdata/{api,worker}:${TAG}`,
   no bind-mounted code, no postgres service, no GPU, cloud-sql-proxy authenticating as the VM
   service account (no credentials file), `RUN_SCHEDULER=1` on api only, 3 workers ×
   `WORKER_MAX_CONCURRENT=6`, `/opt/nexdata/data/raw` → `/app/data/raw` (and reports),
   `restart: unless-stopped` and the `gcplogs` driver on every service, `env_file` rendered at boot.
   `deploy/compose.dark.yml` overrides api to `RUN_SCHEDULER=0` for the dark start.
4. **`deploy/`.** `vm-startup.sh` (install/upgrade docker safely, AR auth, render `app.env` from the
   secrets manifest, extract compose files from the image of `nexdata-tag`, compose up in the mode
   from `nexdata-mode` metadata: `off` | `dark` (default) | `live`), `secrets.manifest`,
   `nexdata-raw-sync.{service,timer}`, `RUNBOOK.md` (dark start, cutover with exactly one
   scheduler, rollback, connection budget).
5. **`.github/workflows/build-push.yml`.** On a successful `CI` run for a push to main: build the
   runtime image, smoke it (imports, no torch), push `api:<sha>` and `worker:<sha>` via Workload
   Identity Federation. A `deploy` job in the `production` environment (required reviewers =
   manual approval) sets the VM's `nexdata-tag` metadata and reruns its startup script over IAP.
   `workflow_dispatch` with a tag redeploys an existing image (rollback). CI's test job installs
   runtime + dev only, and the CI docker job builds `--target runtime`.

## Verification (2026-10-03, local Docker Desktop, tag `nexdata-161-test`)

- `docker build --target runtime`: 1.62 GB on disk, 378 MB compressed content (laptop `nexdata-api`:
  9.57 GB / 3.24 GB). Build context 17.5 MB with the new `.dockerignore`.
- In the image: `import app.main` builds 1478 routes, `import app.worker.main` ok, lightgbm / scipy /
  sklearn / pyarrow / playwright / weasyprint import, `import torch` fails, `id` = uid 1000, no gcc,
  HEALTHCHECK on `/livez`, `/app/deploy/` root-owned.
- `docker compose config` resolves `docker-compose.gcp.yml` alone and with `deploy/compose.dark.yml`.
- `bash -n deploy/vm-startup.sh` ok. Nothing was run against GCP or the live stack.

## Connection budget (Cloud SQL `max_connections = 100`)

| Process | Pools (max) | Count | Total |
|---|---|---|---|
| api | main 5+10, SEC gate 1+1, APScheduler job store 5+10, LISTEN 1 | 1 | 33 |
| worker | main 5+10, SEC gate 1+1 | 3 | 51 |
| Cloud SQL reserved superuser slots | | | 3 |
| **Worst case** | | | **87** |

Leaves 13 for psql / alembic / the laptop during cutover. 3 × 6 = 18 job slots (was 36 on the
laptop's 6 workers). A fourth worker would make it 104 > 100: scale workers only after
`DB_POOL_SIZE` / `DB_MAX_OVERFLOW` (SPEC_160) exist and are set.

## Secrets manifest

Only variables that the code reads (Settings field or `os.getenv`) AND that are configured today
(names of non-empty keys in the laptop `.env`, values never read), plus the watchdog URLs PLAN_089
Phase 5/6 verify (optional: skipped when the secret does not exist), plus `ENCRYPTION_KEY`
(required: value = the laptop's current `DATABASE_URL` verbatim, so stored source API keys still
decrypt once `DATABASE_URL` changes host). Not included: keys set but read nowhere outside
`config.py`/the settings key-tester (GEMINI, XAI, YELP_CLIENT_ID), workbench-only keys (WB_*),
`GCLOUD_ADC` (the VM has no ADC file), and keys the code reads but nobody configured (ANTHROPIC etc.:
add a line when one is provisioned).

## Acceptance Criteria

- [x] requirements split; runtime has no torch/einops/wandb; train and requirements.txt include runtime
- [x] every third-party import under `app/` (any depth, guarded or not; minus the train-only modules
      and `shap`, used only behind `is_shap_available()`) resolves to a runtime requirement, and every
      import under `tests/` to runtime + dev
- [x] no runtime module imports a train-only module
- [x] Dockerfile has `runtime` and `train` targets; runtime is non-root, has a HEALTHCHECK, installs only runtime requirements
- [x] `docker-compose.gcp.yml` parses (docker compose config when available, else YAML), images from AR with `${TAG}`, no `build:`, no code bind mounts, no postgres, no GPU, no credentials file on the proxy
- [x] `RUN_SCHEDULER` set exactly on api (=1); dark override sets it to 0 on api only
- [x] 3 workers × `WORKER_MAX_CONCURRENT=6`; computed worst-case connections ≤ 100 and documented
- [x] `restart: unless-stopped` and `gcplogs` on every service; `env_file` on api and worker; raw on `/opt/nexdata/data/raw:/app/data/raw`
- [x] secrets manifest names are Settings fields or env vars the app reads; secret ids unique
- [x] build-push workflow: WIF auth, AR push, manual-approval deploy via `--tunnel-through-iap`
- [x] Docker image built locally (`nexdata-161-test`) and measured

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_runtime_requirements_exclude_training_packages | no torch/einops/wandb/pytest in runtime |
| T2 | test_requirement_files_compose | train → runtime, requirements.txt → all three, every pin appears once across files |
| T3 | test_app_imports_resolve_with_runtime_and_dev | static scan of all app imports vs runtime+dev |
| T4 | test_no_runtime_module_imports_train_only_code | train-only modules are leaves |
| T5 | test_dockerfile_targets | runtime/train stages, USER, HEALTHCHECK, runtime installs runtime reqs only |
| T6 | test_dockerignore_excludes_data_and_env | `.dockerignore` |
| T7 | test_gcp_compose_parses / test_gcp_compose_with_docker | YAML + `docker compose config` |
| T8 | test_gcp_compose_images_and_no_code_mounts | AR images, no build, no ./app mounts, no postgres, no GPU |
| T9 | test_run_scheduler_only_on_api | exactly api, value 1; dark override 0 |
| T10 | test_workers_and_connection_budget | 3 × 6; budget ≤ 100 and matches RUNBOOK |
| T11 | test_restart_logging_env_raw | restart, gcplogs, env_file, raw mount |
| T12 | test_proxy_uses_vm_service_account | no credentials volume / GOOGLE_APPLICATION_CREDENTIALS |
| T13 | test_secrets_manifest_matches_settings | names known to the app, ids unique, format |
| T14 | test_vm_startup_script | bash syntax, renders from manifest, dark default |
| T15 | test_raw_sync_units | rsync target bucket, timer |
| T16 | test_build_push_workflow | WIF, AR, environment approval, IAP |

## Fix round (review findings, 2026-10-03)

Review found that dark mode, the default, was unsafe on any image without SPEC_160, and that the
deploy path could report success after a refusal or leave the VM pointing at a tag it never ran.

- **Scheduler gate (findings 1, 2, 5).** `deploy/check_scheduler_gate.py` (stdlib, static, no DB)
  runs inside the image before anything switches. It requires the `run_scheduler` /
  `db_pool_size` / `db_max_overflow` Settings fields, `app/core/scheduler_leader.py`
  (`start_scheduler_runtime`, `resolve_stale_running_jobs`), and a `main.py` with no direct
  `start_scheduler()` call and no unconditional stale-job UPDATE. `vm-startup.sh` refuses dark and
  live without it. An image without the probe is refused too. `NEXDATA_SKIP_SCHEDULER_GATE=1` is
  honoured in live mode only.
- **Stage, check, then commit (finding 6).** The script pulls the images, checks digests and stages
  `compose.new` and `app.env.new`. It then runs the gate, the busy check (live) and the secret
  render, and only after those swaps the staged files in and writes `compose.env`. A refusal changes
  nothing. Runs are serialised with `flock`. There is no `compose pull` after the digest check.
- **Exit status and tag ordering (finding 3).** The deploy job runs the script directly over IAP
  (`sudo env NEXDATA_TAG=... bash <script>`), so its exit status reaches the job. It passes the AR
  digests and sets the `nexdata-tag` metadata only after the script succeeded.
- **Trigger (finding 4).** CI on main has failed on every run since 2026-04, checked through the
  public GitHub API. `build-push.yml` therefore runs on push to main behind its own `gate` job
  (SPEC_161/160/120/121/128 tests with Postgres) instead of `workflow_run` with success.
- **WIF scoping (finding 7).** The provider `attribute_condition` and the per-SA `principal://` subject
  bindings (`ref:refs/heads/main` for push, `environment:production` for deploy) are documented in the
  workflow header and the RUNBOOK, along with AR immutable tags and a main-only deploy.
- **Dark-phase budget (finding 8).** `compose.dark.yml` sets `DB_POOL_SIZE=2` and `DB_MAX_OVERFLOW=3`,
  which brings the dark api to 23 connections. The RUNBOOK shows the laptop today plus the dark api
  at 161 and the target of 87, reached with 4 laptop workers at pool 3+2. `docker-compose.gcp.yml`
  sets `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` explicitly.
- **Scope gap.** `/opt/nexdata/data/kaggle` is mounted at `/app/data/kaggle`.

| ID | Test | Verifies |
|----|------|----------|
| F1 | TestSchedulerGate::* | probe passes a gated tree, refuses each missing piece, matches this tree, stdlib-only, ships in the image |
| F2 | test_main_gates_scheduler_on_run_scheduler | strict xfail until SPEC_160 is merged, then must pass |
| F3 | TestStartupOrdering::* | stage → gate → busy → render → commit → units → up; gate covers dark+live; no re-pull |
| F4 | TestStartupBehaviour::* | Git Bash run with stubs: switch on success; refusals (gate, busy, missing secret, digest) leave compose/, app.env and compose.env unchanged and exit non-zero |
| F5 | test_deploy_propagates_exit_and_sets_tag_last / test_wif_trust_is_scoped | workflow ordering, digests, WIF docs |
| F6 | test_dark_phase_connection_budget | 23 / 161 / 87 numbers match compose + RUNBOOK |
