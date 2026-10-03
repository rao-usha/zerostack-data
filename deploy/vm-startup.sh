#!/usr/bin/env bash
# Nexdata VM boot / deploy script (SPEC_161, PLAN_089 Option B).
#
# Installed as the instance's `startup-script` metadata, so it runs as root on
# every boot. The deploy workflow runs it directly over IAP (fetched from
# metadata, `sudo env NEXDATA_TAG=... bash`) so its exit status reaches the
# job, and sets nexdata-tag only after it succeeded. Not via
# `google_metadata_script_runner startup`: that logs the status, exits 0.
#
# Instance metadata it reads:
#   nexdata-tag   image tag to run (git sha pushed by .github/workflows/build-push.yml)
#   nexdata-mode  off | dark | live   (default dark: api without scheduler, no workers)
# Overrides for a manual or deploy run (environment):
#   NEXDATA_TAG, NEXDATA_MODE
#   NEXDATA_API_DIGEST / NEXDATA_WORKER_DIGEST  sha256:... the pulled images must match
#                                               (the deploy job passes what it checked in AR)
#   NEXDATA_FORCE=1                 recreate workers even while jobs run (live mode)
#   NEXDATA_SKIP_SCHEDULER_GATE=1   live mode only, and only once no other api
#                                   (the laptop) uses the database
#
# What it does (nothing the running stack uses changes until step 5):
#   1. installs Docker Engine + compose plugin (upgrades only while the stack is down)
#   2. pulls api:<tag> and worker:<tag> as the VM service account, checks digests,
#      and stages the compose files, secrets manifest and systemd units from the image
#   3. scheduler gate: dark and live need an image that honours RUN_SCHEDULER and
#      gates the startup stale-job resolver (SPEC_160), checked by running
#      deploy/check_scheduler_gate.py inside the image; otherwise it refuses
#   4. live mode: refuses while jobs run (unless NEXDATA_FORCE=1)
#   5. renders app.env from Secret Manager (deploy/secrets.manifest), then swaps the
#      staged files in and writes compose.env (only now does the VM point at the tag)
#   6. installs the nightly data/raw -> GCS sync timer
#   7. docker compose up in the requested mode, then waits for /readyz
# Exit status is non-zero on any refusal or failure, so a caller that runs the
# script directly (the deploy job does) sees it.
#
# Never `set -x` here: the serial console log would carry every secret value.
set -euo pipefail
umask 077

ROOT=/opt/nexdata
PROJECT=nexdata-cloud
REGISTRY_HOST=us-central1-docker.pkg.dev
IMAGE_BASE="${REGISTRY_HOST}/${PROJECT}/nexdata"
APP_UID=1000
DOCKER_PKGS=(docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin)
MD_URL=http://metadata.google.internal/computeMetadata/v1/instance/attributes

log() { echo "nexdata-startup: $*"; }
die() { echo "nexdata-startup: ERROR: $*" >&2; exit 1; }
md() { curl -fsS -H 'Metadata-Flavor: Google' "${MD_URL}/$1" 2>/dev/null || true; }

TAG="${NEXDATA_TAG:-$(md nexdata-tag)}"
MODE="${NEXDATA_MODE:-$(md nexdata-mode)}"
MODE="${MODE:-dark}"
case "$MODE" in off|dark|live) ;; *) die "nexdata-mode must be off|dark|live, got '$MODE'" ;; esac

stack_running() {
  command -v docker >/dev/null 2>&1 \
    && [ -n "$(docker ps -q --filter label=com.docker.compose.project=nexdata 2>/dev/null)" ]
}

# --------------------------------------------------------------------------
# 1. Docker Engine
# --------------------------------------------------------------------------
install_docker() {
  export DEBIAN_FRONTEND=noninteractive
  if ! command -v docker >/dev/null 2>&1; then
    log "installing Docker Engine"
    apt-get update -q
    apt-get install -y -q ca-certificates curl gnupg
    install -m 0755 -d /etc/apt/keyrings
    # shellcheck disable=SC1091
    . /etc/os-release
    curl -fsSL "https://download.docker.com/linux/${ID}/gpg" -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
      > /etc/apt/sources.list.d/docker.list
    apt-get update -q
    apt-get install -y -q "${DOCKER_PKGS[@]}"
  elif ! stack_running; then
    # Upgrading restarts dockerd; only do it while nothing of ours runs.
    log "upgrading Docker Engine (stack is down)"
    apt-mark unhold "${DOCKER_PKGS[@]}" >/dev/null
    apt-get update -q
    apt-get install -y -q --only-upgrade "${DOCKER_PKGS[@]}"
  else
    log "stack running: leaving Docker Engine at $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo '?')"
  fi
  # unattended-upgrades must never restart dockerd under a multi-hour job
  apt-mark hold "${DOCKER_PKGS[@]}" >/dev/null

  # live-restore: containers keep running across a dockerd restart/upgrade
  local want='{ "live-restore": true }'
  if [ ! -f /etc/docker/daemon.json ] || [ "$(cat /etc/docker/daemon.json)" != "$want" ]; then
    mkdir -p /etc/docker
    echo "$want" > /etc/docker/daemon.json
    systemctl reload docker 2>/dev/null || true
  fi
  systemctl enable --now docker
}

# --------------------------------------------------------------------------
# 2. Pull + stage (nothing the running stack uses changes here)
# --------------------------------------------------------------------------
verify_digest() {
  # $1 image ref, $2 repo (api|worker), $3 expected sha256:... (may be empty)
  local want="$3" have
  [ -n "$want" ] || return 0
  [[ "$want" =~ ^sha256:[0-9a-f]{64}$ ]] || die "invalid expected digest for $2: '${want}'"
  have="$(docker image inspect --format '{{join .RepoDigests " "}}' "$1")"
  case " $have " in
    *" ${IMAGE_BASE}/$2@${want} "*) log "$2 digest ok (${want})" ;;
    *) die "$1 is ${have:-unknown}, expected ${IMAGE_BASE}/$2@${want} (tag moved?)" ;;
  esac
}

stage_release() {
  command -v gcloud >/dev/null 2>&1 || die "gcloud CLI missing (use a Debian GCE image)"
  gcloud auth configure-docker "$REGISTRY_HOST" --quiet >/dev/null 2>&1
  API_IMAGE="${IMAGE_BASE}/api:${TAG}"
  local worker_image="${IMAGE_BASE}/worker:${TAG}"
  log "pulling ${API_IMAGE} and ${worker_image}"
  docker pull -q "$API_IMAGE" >/dev/null
  docker pull -q "$worker_image" >/dev/null
  verify_digest "$API_IMAGE" api "${NEXDATA_API_DIGEST:-}"
  verify_digest "$worker_image" worker "${NEXDATA_WORKER_DIGEST:-}"
  rm -rf "${ROOT}/compose.new"
  mkdir -p "${ROOT}/compose.new"
  local cid
  cid="$(docker create "$API_IMAGE")"
  docker cp "${cid}:/app/deploy/." "${ROOT}/compose.new/"
  docker rm "$cid" >/dev/null
  [ -f "${ROOT}/compose.new/docker-compose.gcp.yml" ] || die "image ${TAG} carries no deploy/docker-compose.gcp.yml"
  # root runs compose from here: nobody else may edit it (docker cp keeps image uids)
  chown -R root:root "${ROOT}/compose.new"
  chmod -R go-w "${ROOT}/compose.new"
}

# --------------------------------------------------------------------------
# 3. Scheduler gate (SPEC_160 must be in the image)
# --------------------------------------------------------------------------
scheduler_gate() {
  # In dark mode the VM api shares Cloud SQL with the laptop's api: an image
  # that ignores RUN_SCHEDULER fires every schedule a second time, and an
  # ungated startup resolver fails the laptop's >2 h jobs on every start.
  # An image without the probe file fails here too (fail closed).
  if docker run --rm --network none "$API_IMAGE" python /app/deploy/check_scheduler_gate.py /app; then
    return 0
  fi
  if [ "$MODE" = "live" ] && [ "${NEXDATA_SKIP_SCHEDULER_GATE:-0}" = "1" ]; then
    log "WARNING: scheduler gate skipped (NEXDATA_SKIP_SCHEDULER_GATE=1): no other api may use the database"
    return 0
  fi
  die "image ${TAG} does not honour RUN_SCHEDULER / gate the stale-job resolver (SPEC_160); refusing mode ${MODE}"
}

# --------------------------------------------------------------------------
# 4. Busy check (live)
# --------------------------------------------------------------------------
running_jobs() {
  # jobs running on any worker (laptop or VM), from the live api's /health
  curl -fsS --max-time 5 http://127.0.0.1:8001/health 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("queue",{}).get("running",0))' 2>/dev/null \
    || echo 0
}

busy_check() {
  if [ "$MODE" = "live" ] && stack_running && [ "${NEXDATA_FORCE:-0}" != "1" ]; then
    local busy
    busy="$(running_jobs)"
    if [ "${busy:-0}" -gt 0 ]; then
      die "${busy} job(s) running; recreating workers would restart them. Nothing changed. Retry later or set NEXDATA_FORCE=1"
    fi
  fi
}

# --------------------------------------------------------------------------
# 5. Secrets -> app.env.new, then swap everything in
# --------------------------------------------------------------------------
render_env() {
  local manifest="${ROOT}/compose.new/secrets.manifest"
  local out="${ROOT}/app.env.new"
  local tmp="${out}.tmp"
  [ -f "$manifest" ] || die "secrets manifest missing: $manifest"
  : > "$tmp"
  chmod 600 "$tmp"
  local name secret mode value n=0 missing=()
  while read -r name secret mode _; do
    case "$name" in ''|'#'*) continue ;; esac
    if ! value="$(gcloud secrets versions access latest --secret="$secret" --project="$PROJECT" 2>/dev/null)" \
        || [ -z "$value" ]; then
      if [ "$mode" = "optional" ]; then
        log "optional secret ${secret} not set; ${name} omitted"
        continue
      fi
      missing+=("$secret")
      continue
    fi
    case "$value" in
      *$'\n'*|*"'"*) die "secret ${secret} contains a newline or a single quote; app.env cannot carry it" ;;
    esac
    # single quotes: compose takes the value literally (no $ interpolation)
    printf "%s='%s'\n" "$name" "$value" >> "$tmp"
    n=$((n + 1))
  done < "$manifest"
  unset value
  if [ "${#missing[@]}" -gt 0 ]; then
    rm -f "$tmp"
    die "required secrets missing or empty: ${missing[*]}"
  fi
  mv "$tmp" "$out"
  chmod 600 "$out"
  log "rendered ${n} variables"
}

commit_release() {
  rm -rf "${ROOT}/compose.prev"
  if [ -d "${ROOT}/compose" ]; then mv "${ROOT}/compose" "${ROOT}/compose.prev"; fi
  mv "${ROOT}/compose.new" "${ROOT}/compose"
  mv "${ROOT}/app.env.new" "${ROOT}/app.env"
  printf 'TAG=%s\n' "$TAG" > "${ROOT}/compose.env.tmp"
  chmod 600 "${ROOT}/compose.env.tmp"
  mv "${ROOT}/compose.env.tmp" "${ROOT}/compose.env"
  log "VM now points at tag ${TAG}"
}

cleanup_staged() {
  rm -rf "${ROOT}/compose.new" "${ROOT}/app.env.new" "${ROOT}/app.env.new.tmp" "${ROOT}/compose.env.tmp"
}

# --------------------------------------------------------------------------
# 6. Nightly raw sync
# --------------------------------------------------------------------------
install_units() {
  install -m 0644 "${ROOT}/compose/systemd/nexdata-raw-sync.service" /etc/systemd/system/
  install -m 0644 "${ROOT}/compose/systemd/nexdata-raw-sync.timer" /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now nexdata-raw-sync.timer
}

# --------------------------------------------------------------------------
# 7. Compose
# --------------------------------------------------------------------------
compose_up() {
  local c=(docker compose -p nexdata --env-file "${ROOT}/compose.env" -f "${ROOT}/compose/docker-compose.gcp.yml")
  # No `compose pull`: api/worker were pulled (and digest-checked) in stage_release,
  # and pulling again could fetch a tag that moved since. `up` pulls what is
  # missing (the proxy).
  case "$MODE" in
    dark)
      log "mode dark: proxy + api with RUN_SCHEDULER=0, no workers"
      c+=(-f "${ROOT}/compose/compose.dark.yml")
      # live -> dark: workers drain (stop_grace_period) and are removed
      "${c[@]}" rm --stop --force worker
      "${c[@]}" up -d --remove-orphans cloudsqlproxy api
      ;;
    live)
      log "mode live: proxy + api (RUN_SCHEDULER=1) + 3 workers"
      "${c[@]}" up -d --remove-orphans
      ;;
  esac

  local i
  for i in $(seq 1 40); do
    if curl -fsS --max-time 5 http://127.0.0.1:8001/readyz >/dev/null 2>&1; then
      log "api ready (tag ${TAG}, mode ${MODE})"
      "${c[@]}" ps
      return 0
    fi
    sleep 5
  done
  "${c[@]}" ps || true
  die "api not ready after 200 s (tag ${TAG}); see: docker compose -p nexdata logs api"
}

main() {
  # one run at a time (a boot run vs. a deploy over IAP)
  mkdir -p "$ROOT"
  exec 9>"${ROOT}/.startup.lock"
  flock -w 900 9 || die "another startup run holds ${ROOT}/.startup.lock"

  mkdir -p "${ROOT}/data/raw" "${ROOT}/data/reports" "${ROOT}/data/kaggle"
  # the app runs as uid 1000; files restored from GCS by root must stay writable
  find "${ROOT}/data" ! -uid "$APP_UID" -exec chown "${APP_UID}:${APP_UID}" {} +
  chmod 0755 "$ROOT" "${ROOT}/data"

  if [ "$MODE" = "off" ]; then
    if command -v docker >/dev/null 2>&1 \
        && [ -f "${ROOT}/compose/docker-compose.gcp.yml" ] && [ -f "${ROOT}/compose.env" ]; then
      log "mode off: stopping the stack"
      docker compose -p nexdata --env-file "${ROOT}/compose.env" \
        -f "${ROOT}/compose/docker-compose.gcp.yml" down
    fi
    install_docker   # stack is down now, so this is where Docker gets upgraded
    exit 0
  fi

  install_docker

  [ -n "$TAG" ] || die "no nexdata-tag metadata: set it to an image tag pushed to ${IMAGE_BASE}"
  [[ "$TAG" =~ ^[A-Za-z0-9._-]{1,128}$ ]] || die "invalid tag '${TAG}'"

  # until commit_release, a refusal or failure leaves compose/, app.env and
  # compose.env exactly as they were (the running stack keeps its tag)
  trap cleanup_staged EXIT
  stage_release
  scheduler_gate
  busy_check
  render_env
  commit_release
  trap - EXIT
  install_units
  compose_up
}

main "$@"
