#!/usr/bin/env bash
# Nexdata VM boot / deploy script (SPEC_161, PLAN_089 Option B).
#
# Installed as the instance's `startup-script` metadata, so it runs as root on
# every boot. The deploy workflow reruns it after changing metadata:
#   sudo google_metadata_script_runner startup
#
# Instance metadata it reads:
#   nexdata-tag   image tag to run (git sha pushed by .github/workflows/build-push.yml)
#   nexdata-mode  off | dark | live   (default dark: api without scheduler, no workers)
# Overrides for a manual run: NEXDATA_TAG, NEXDATA_MODE, NEXDATA_FORCE=1.
#
# What it does:
#   1. installs Docker Engine + compose plugin (upgrades only while the stack is down)
#   2. lets root's docker pull from Artifact Registry as the VM service account
#   3. extracts the compose files, secrets manifest and systemd units from the image
#   4. renders /opt/nexdata/app.env from Secret Manager (deploy/secrets.manifest)
#   5. installs the nightly data/raw -> GCS sync timer
#   6. docker compose pull + up in the requested mode, then waits for /readyz
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
# 2-3. Registry auth, compose files from the image
# --------------------------------------------------------------------------
fetch_compose_files() {
  command -v gcloud >/dev/null 2>&1 || die "gcloud CLI missing (use a Debian GCE image)"
  gcloud auth configure-docker "$REGISTRY_HOST" --quiet >/dev/null 2>&1
  local image="${IMAGE_BASE}/api:${TAG}"
  log "pulling ${image}"
  docker pull -q "$image" >/dev/null
  rm -rf "${ROOT}/compose.new"
  mkdir -p "${ROOT}/compose.new"
  local cid
  cid="$(docker create "$image")"
  docker cp "${cid}:/app/deploy/." "${ROOT}/compose.new/"
  docker rm "$cid" >/dev/null
  [ -f "${ROOT}/compose.new/docker-compose.gcp.yml" ] || die "image ${TAG} carries no deploy/docker-compose.gcp.yml"
  rm -rf "${ROOT}/compose.prev"
  [ -d "${ROOT}/compose" ] && mv "${ROOT}/compose" "${ROOT}/compose.prev"
  mv "${ROOT}/compose.new" "${ROOT}/compose"
  # root runs compose from here: nobody else may edit it (docker cp keeps image uids)
  chown -R root:root "${ROOT}/compose"
  chmod -R go-w "${ROOT}/compose"
}

# --------------------------------------------------------------------------
# 4. Secrets -> app.env
# --------------------------------------------------------------------------
render_env() {
  local manifest="${ROOT}/compose/secrets.manifest"
  local out="${ROOT}/app.env"
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
  log "rendered ${n} variables into ${out}"
}

# --------------------------------------------------------------------------
# 5. Nightly raw sync
# --------------------------------------------------------------------------
install_units() {
  install -m 0644 "${ROOT}/compose/systemd/nexdata-raw-sync.service" /etc/systemd/system/
  install -m 0644 "${ROOT}/compose/systemd/nexdata-raw-sync.timer" /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now nexdata-raw-sync.timer
}

# --------------------------------------------------------------------------
# 6. Compose
# --------------------------------------------------------------------------
running_jobs() {
  # jobs running on any worker (laptop or VM), from the live api's /health
  curl -fsS --max-time 5 http://127.0.0.1:8001/health 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("queue",{}).get("running",0))' 2>/dev/null \
    || echo 0
}

compose_up() {
  printf 'TAG=%s\n' "$TAG" > "${ROOT}/compose.env"
  chmod 600 "${ROOT}/compose.env"
  local c=(docker compose -p nexdata --env-file "${ROOT}/compose.env" -f "${ROOT}/compose/docker-compose.gcp.yml")

  if [ "$MODE" = "live" ] && stack_running && [ "${NEXDATA_FORCE:-0}" != "1" ]; then
    local busy
    busy="$(running_jobs)"
    if [ "${busy:-0}" -gt 0 ]; then
      die "${busy} job(s) running; recreating workers would restart them. Retry later or set NEXDATA_FORCE=1"
    fi
  fi

  case "$MODE" in
    dark)
      log "mode dark: proxy + api with RUN_SCHEDULER=0, no workers"
      c+=(-f "${ROOT}/compose/compose.dark.yml")
      "${c[@]}" pull -q cloudsqlproxy api
      # live -> dark: workers drain (stop_grace_period) and are removed
      "${c[@]}" rm --stop --force worker
      "${c[@]}" up -d --remove-orphans cloudsqlproxy api
      ;;
    live)
      log "mode live: proxy + api (RUN_SCHEDULER=1) + 3 workers"
      "${c[@]}" pull -q
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
  mkdir -p "${ROOT}/data/raw" "${ROOT}/data/reports"
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

  fetch_compose_files
  render_env
  install_units
  compose_up
}

main "$@"
