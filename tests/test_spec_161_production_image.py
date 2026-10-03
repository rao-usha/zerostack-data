"""SPEC_161 -- production image, GCP compose and CI build (PLAN_089 Phases 1-2).

Mostly static: requirement files, Dockerfile, the VM compose file, the
deploy/ scripts and the build-push workflow. ``docker compose config`` runs
only when Docker is on PATH (it parses, it never touches a running stack).
The fix round adds behaviour tests: deploy/check_scheduler_gate.py against
fake app trees, and deploy/vm-startup.sh under Git Bash with docker, gcloud
and curl stubbed and ROOT pointed at a temp dir (never /opt, never Docker).
"""

from __future__ import annotations

import ast
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
GCP_COMPOSE = REPO / "docker-compose.gcp.yml"
DARK_COMPOSE = REPO / "deploy" / "compose.dark.yml"
MANIFEST = REPO / "deploy" / "secrets.manifest"
RUNBOOK = REPO / "deploy" / "RUNBOOK.md"
AR_PREFIX = "us-central1-docker.pkg.dev/nexdata-cloud/nexdata/"

TRAIN_ONLY_PACKAGES = {"torch", "einops", "wandb"}
DEV_ONLY_PACKAGES = {"pytest", "pytest-asyncio", "pytest-cov"}
# Modules that only the offline trainers use; nothing in the api/worker may import them.
TRAIN_ONLY_MODULE_PREFIX = "app/services/synthetic/training/"

# import name -> distribution name, where they differ
IMPORT_TO_DIST = {
    "bs4": "beautifulsoup4",
    "dateutil": "python-dateutil",
    "dns": "dnspython",
    "jwt": "pyjwt",
    "pptx": "python-pptx",
    "sklearn": "scikit-learn",
    "yaml": "pyyaml",
    "psycopg2": "psycopg2-binary",
    "strawberry": "strawberry-graphql",
    "pydantic_settings": "pydantic-settings",
    "email_validator": "email-validator",
    "dotenv": "python-dotenv",
}
# Imported directly but installed by a declared parent (not pinned on their own).
PROVIDED_BY = {"numpy": "pandas", "starlette": "fastapi", "httpcore": "httpx"}
# Optional at runtime: every use is behind an availability check.
OPTIONAL_IMPORTS = {"shap": {"app/ml/probability_model.py"}}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _canon(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _requirements(filename: str, _seen=None) -> dict[str, str]:
    """{canonical dist name: requirement line}, following ``-r`` includes."""
    _seen = _seen if _seen is not None else set()
    path = REPO / filename
    if path in _seen:
        return {}
    _seen.add(path)
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith(("-r ", "--requirement ")):
            out.update(_requirements(line.split(None, 1)[1].strip(), _seen))
            continue
        assert not line.startswith("-"), f"{filename}: unsupported option {line!r}"
        m = re.match(r"[A-Za-z0-9][A-Za-z0-9_.\-]*", line)
        assert m, f"{filename}: cannot parse {line!r}"
        out[_canon(m.group(0))] = line
    return out


def _own_requirements(filename: str) -> dict[str, str]:
    """Requirement lines declared in this file only (no includes)."""
    out = {}
    for raw in (REPO / filename).read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            out[_canon(re.match(r"[A-Za-z0-9][A-Za-z0-9_.\-]*", line).group(0))] = line
    return out


def _includes(filename: str) -> list[str]:
    return [
        ln.split(None, 1)[1].strip()
        for ln in (REPO / filename).read_text(encoding="utf-8").splitlines()
        if ln.strip().startswith("-r ")
    ]


def _third_party_imports(root: Path, skip_prefix: str | None = None):
    """Yield (relative path, top-level import name) for every import, at any depth.

    Function-level and try/except-guarded imports count too: they run in the
    api/worker as soon as the code path does.
    """
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(REPO).as_posix()
        if skip_prefix and rel.startswith(skip_prefix):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        except SyntaxError:
            # tests/_mk_export.py is a stray, unparseable helper (never collected)
            if root.name == "tests":
                continue
            raise
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                top = name.split(".")[0]
                if top in ("app", "tests", "__future__") or top in sys.stdlib_module_names:
                    continue
                yield rel, top


def _unresolved(imports, available: dict[str, str]) -> list[str]:
    bad = []
    for rel, top in imports:
        if top in OPTIONAL_IMPORTS:
            if rel not in OPTIONAL_IMPORTS[top]:
                bad.append(f"{rel}: optional {top} used outside its guarded module")
            continue
        if top in PROVIDED_BY:
            if PROVIDED_BY[top] not in available:
                bad.append(f"{rel}: {top} (via {PROVIDED_BY[top]})")
            continue
        dist = _canon(IMPORT_TO_DIST.get(top, top))
        if dist not in available:
            bad.append(f"{rel}: {top} -> {dist}")
    return sorted(set(bad))


@pytest.fixture(scope="module")
def gcp():
    return yaml.safe_load(GCP_COMPOSE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def dark():
    return yaml.safe_load(DARK_COMPOSE.read_text(encoding="utf-8"))


def _env(svc: dict) -> dict:
    env = svc.get("environment") or {}
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env)
    return {k: str(v) for k, v in env.items()}


def _manifest() -> list[tuple[str, str, str]]:
    rows = []
    for raw in MANIFEST.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            parts = line.split()
            assert len(parts) == 3, f"bad manifest line: {raw!r}"
            rows.append(tuple(parts))
    return rows


# ---------------------------------------------------------------------------
# T1-T4 requirements
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRequirements:
    def test_runtime_requirements_exclude_training_packages(self):
        runtime = _requirements("requirements-runtime.txt")
        assert not (set(runtime) & TRAIN_ONLY_PACKAGES), "torch & co. belong in requirements-train.txt"
        assert not (set(runtime) & DEV_ONLY_PACKAGES), "test tooling belongs in requirements-dev.txt"
        # the things the running app needs are there
        for dist in ("fastapi", "uvicorn", "sqlalchemy", "psycopg2-binary", "apscheduler", "lightgbm", "pyarrow"):
            assert dist in runtime, dist

    def test_requirement_files_compose(self):
        assert "requirements-runtime.txt" in _includes("requirements-train.txt")
        assert set(_includes("requirements.txt")) == {
            "requirements-runtime.txt", "requirements-train.txt", "requirements-dev.txt",
        }
        train = _requirements("requirements-train.txt")
        assert TRAIN_ONLY_PACKAGES <= set(train)
        dev = _own_requirements("requirements-dev.txt")
        assert DEV_ONLY_PACKAGES <= set(dev)
        # each pin is declared in exactly one file, so the three cannot drift apart
        seen: dict[str, str] = {}
        for f in ("requirements-runtime.txt", "requirements-train.txt", "requirements-dev.txt"):
            for dist in _own_requirements(f):
                assert dist not in seen, f"{dist} pinned in both {seen[dist]} and {f}"
                seen[dist] = f
        # requirements.txt itself pins nothing (local dev = union of the three)
        assert _own_requirements("requirements.txt") == {}

    def test_app_imports_resolve_with_runtime_and_dev(self):
        runtime = _requirements("requirements-runtime.txt")
        bad = _unresolved(_third_party_imports(REPO / "app", skip_prefix=TRAIN_ONLY_MODULE_PREFIX), runtime)
        assert not bad, "app imports not covered by requirements-runtime.txt:\n" + "\n".join(bad)

        runtime_dev = {**runtime, **_requirements("requirements-dev.txt")}
        bad = _unresolved(_third_party_imports(REPO / "tests"), runtime_dev)
        assert not bad, "test imports not covered by runtime+dev:\n" + "\n".join(bad)

        # and the trainers resolve against the train set
        train = _requirements("requirements-train.txt")
        trainer_imports = [
            (rel, top) for rel, top in _third_party_imports(REPO / "app")
            if rel.startswith(TRAIN_ONLY_MODULE_PREFIX)
        ]
        assert {top for _, top in trainer_imports} & {"torch", "einops"}
        assert not _unresolved(trainer_imports, train)

    def test_no_runtime_module_imports_train_only_code(self):
        offenders = []
        for path in (REPO / "app").rglob("*.py"):
            rel = path.relative_to(REPO).as_posix()
            if rel.startswith(TRAIN_ONLY_MODULE_PREFIX):
                continue
            src = path.read_text(encoding="utf-8")
            if re.search(r"synthetic\.training|synthetic import training", src):
                offenders.append(rel)
        assert not offenders, offenders


# ---------------------------------------------------------------------------
# T5-T6 Dockerfile
# ---------------------------------------------------------------------------

def _stages() -> dict[str, str]:
    text = (REPO / "Dockerfile").read_text(encoding="utf-8")
    stages: dict[str, str] = {}
    order = []
    current = None
    for line in text.splitlines():
        m = re.match(r"\s*FROM\s+(\S+)(?:\s+AS\s+(\S+))?", line, re.I)
        if m:
            current = (m.group(2) or f"_{len(order)}").lower()
            order.append(current)
            stages[current] = line + "\n"
        elif current:
            stages[current] += line + "\n"
    stages["__order__"] = ",".join(order)
    return stages


def _ancestry(stages: dict[str, str], name: str) -> set[str]:
    seen = set()
    todo = [name]
    while todo:
        s = todo.pop()
        if s in seen or s not in stages:
            continue
        seen.add(s)
        body = stages[s]
        parent = re.match(r"\s*FROM\s+(\S+)", body, re.I).group(1).lower()
        todo.append(parent)
        todo.extend(x.lower() for x in re.findall(r"--from=(\S+)", body))
    return seen


@pytest.mark.unit
class TestDockerfile:
    def test_dockerfile_targets(self):
        st = _stages()
        assert "runtime" in st and "train" in st
        assert st["__order__"].split(",")[-1] == "runtime", "runtime must be the default (last) target"
        rt = st["runtime"]
        assert re.search(r"^USER\s+(appuser|1000)", rt, re.M), "runtime runs as non-root"
        assert "HEALTHCHECK" in rt
        chain = "".join(st[s] for s in _ancestry(st, "runtime"))
        assert "requirements-runtime.txt" in chain
        for forbidden in ("requirements-train.txt", "requirements-dev.txt", "requirements.txt "):
            assert forbidden not in chain, forbidden
        assert not re.search(r"COPY\s+requirements\.txt", chain)
        # no compiler in the runtime image itself
        assert "gcc" not in st["runtime"] and "gcc" not in st.get("os-base", "")
        train_chain = "".join(st[s] for s in _ancestry(st, "train"))
        assert "requirements-train.txt" in train_chain
        assert re.search(r"^USER\s+(appuser|1000)", st["train"], re.M)

    def test_dockerignore_excludes_data_and_env(self):
        lines = {
            ln.strip() for ln in (REPO / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")
        }
        for entry in (".git", ".env", "data"):
            assert entry in lines or f"{entry}/" in lines or f"/{entry}" in lines, entry
        assert "!data/reference" in lines or "!data/reference/" in lines


# ---------------------------------------------------------------------------
# T7-T12 docker-compose.gcp.yml
# ---------------------------------------------------------------------------

def _docker_compose_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return subprocess.run(
            ["docker", "compose", "version"], capture_output=True, timeout=30
        ).returncode == 0
    except Exception:
        return False


APSCHEDULER_STORE = 5 + 10   # SQLAlchemyJobStore(url=...) builds its own default-pool engine
LISTEN = 1                   # app/core/pg_listener.py psycopg2.connect
RESERVED = 3                 # Cloud SQL superuser_reserved_connections


def _main_pool(svc: dict) -> int:
    """Worst-case main pool of a service: DB_POOL_SIZE + DB_MAX_OVERFLOW (SPEC_160)."""
    env = _env(svc)
    return int(env["DB_POOL_SIZE"]) + int(env["DB_MAX_OVERFLOW"])


def _hardcoded_main_pool():
    """(size, overflow) while database.py still hard-codes them (pre-SPEC_160), else None."""
    db = (REPO / "app/core/database.py").read_text(encoding="utf-8")
    size = re.search(r"pool_size=(\d+)", db)
    over = re.search(r"max_overflow=(\d+)", db)
    if size and over:
        return int(size.group(1)), int(over.group(1))
    return None


def _sec_gate_pool() -> int:
    gate = (REPO / "app/core/sec_gate.py").read_text(encoding="utf-8")
    return int(re.search(r"pool_size=(\d+)", gate).group(1)) + int(re.search(r"max_overflow=(\d+)", gate).group(1))


@pytest.mark.unit
class TestGcpCompose:
    def test_gcp_compose_parses(self, gcp, dark):
        assert set(gcp["services"]) == {"cloudsqlproxy", "api", "worker"}
        assert set(dark["services"]) <= set(gcp["services"])

    @pytest.mark.skipif(not _docker_compose_available(), reason="docker compose not available")
    def test_gcp_compose_with_docker(self, tmp_path):
        env_file = tmp_path / "app.env"
        env_file.write_text("DATABASE_URL=postgresql://u:p@cloudsqlproxy:5432/nexdata\n")
        env = {**os.environ, "TAG": "spec161test", "NEXDATA_APP_ENV": str(env_file)}
        for files in (["docker-compose.gcp.yml"], ["docker-compose.gcp.yml", "deploy/compose.dark.yml"]):
            args = ["docker", "compose", "-p", "nexdata161test"]
            for f in files:
                args += ["-f", str(REPO / f)]
            res = subprocess.run(
                args + ["config", "--format", "json"],
                capture_output=True, text=True, timeout=120, env=env, cwd=str(tmp_path),
            )
            assert res.returncode == 0, res.stderr
            cfg = json.loads(res.stdout)
            api = cfg["services"]["api"]
            assert api["image"] == AR_PREFIX + "api:spec161test"
            expected = "0" if len(files) == 2 else "1"
            assert api["environment"]["RUN_SCHEDULER"] == expected
            assert cfg["services"]["worker"]["deploy"]["replicas"] == 3

    def test_tag_is_required(self):
        text = GCP_COMPOSE.read_text(encoding="utf-8")
        assert "${TAG:?" in text

    def test_gcp_compose_images_and_no_code_mounts(self, gcp):
        services = gcp["services"]
        assert "postgres" not in services
        assert services["api"]["image"].startswith(AR_PREFIX + "api:${TAG")
        assert services["worker"]["image"].startswith(AR_PREFIX + "worker:${TAG")
        assert re.fullmatch(r".*cloud-sql-proxy:\d+\.\d+\.\d+", services["cloudsqlproxy"]["image"])
        text = GCP_COMPOSE.read_text(encoding="utf-8")
        assert "nvidia" not in text and "gpu" not in text.lower()
        for name, svc in services.items():
            assert "build" not in svc, name
            for vol in svc.get("volumes") or []:
                src = (vol if isinstance(vol, str) else vol.get("source", "")).split(":")[0]
                assert not src.startswith("."), f"{name}: relative bind mount {vol}"
                target = vol.split(":")[1] if isinstance(vol, str) else vol.get("target", "")
                for code in ("/app/app", "/app/scripts", "/app/alembic"):
                    assert not target.startswith(code), f"{name}: code mount {vol}"
            deps = svc.get("depends_on") or {}
            assert "postgres" not in deps
            for port in svc.get("ports") or []:
                assert str(port).startswith("127.0.0.1:"), f"{name}: {port} not loopback"

    def test_run_scheduler_only_on_api(self, gcp, dark):
        holders = {name for name, svc in gcp["services"].items() if "RUN_SCHEDULER" in _env(svc)}
        assert holders == {"api"}
        assert _env(gcp["services"]["api"])["RUN_SCHEDULER"] == "1"
        assert _env(dark["services"]["api"])["RUN_SCHEDULER"] == "0"
        assert {n for n, s in dark["services"].items() if "RUN_SCHEDULER" in _env(s)} == {"api"}
        assert "RUN_SCHEDULER" not in MANIFEST.read_text(encoding="utf-8")

    def test_workers_and_connection_budget(self, gcp):
        worker = gcp["services"]["worker"]
        replicas = worker["deploy"]["replicas"]
        assert replicas == 3
        assert _env(worker)["WORKER_MAX_CONCURRENT"] == "6"
        # graceful drain (30 s in app/worker/main.py) must fit in the stop grace period
        grace = int(re.match(r"(\d+)s", str(worker["stop_grace_period"])).group(1))
        assert grace > 30

        hard = _hardcoded_main_pool()
        if hard is not None:   # the env is ignored until SPEC_160: it must say the truth
            for name in ("api", "worker"):
                env = _env(gcp["services"][name])
                assert (int(env["DB_POOL_SIZE"]), int(env["DB_MAX_OVERFLOW"])) == hard, name
        api_main = _main_pool(gcp["services"]["api"])
        worker_main = _main_pool(worker)
        gate_pool = _sec_gate_pool()
        api = api_main + gate_pool + APSCHEDULER_STORE + LISTEN
        workers = replicas * (worker_main + gate_pool)
        total = api + workers + RESERVED
        assert (api_main, gate_pool, api, workers, total) == (15, 2, 33, 51, 87)
        assert total <= 100
        runbook = RUNBOOK.read_text(encoding="utf-8")
        assert "87" in runbook and "max_connections" in runbook

    def test_dark_phase_connection_budget(self, gcp, dark):
        # Finding: the laptop (still live) + the dark VM api share Cloud SQL.
        merged = {**_env(gcp["services"]["api"]), **_env(dark["services"]["api"])}
        dark_api = _main_pool({"environment": merged}) + _sec_gate_pool() + APSCHEDULER_STORE + LISTEN
        assert dark_api == 23
        laptop_api = 15 + _sec_gate_pool() + APSCHEDULER_STORE + LISTEN
        laptop_today = laptop_api + 6 * (15 + _sec_gate_pool())
        assert laptop_today + dark_api + RESERVED == 161 > 100
        laptop_shrunk = laptop_api + 4 * ((3 + 2) + _sec_gate_pool())   # 4 workers, pool 3+2
        assert laptop_shrunk + dark_api + RESERVED == 87 <= 100
        runbook = RUNBOOK.read_text(encoding="utf-8")
        assert "Dark phase" in runbook and "161" in runbook
        assert "WORKER_DB_POOL_SIZE=3" in runbook and "--scale worker=4" in runbook
        assert "laptop while it still runs" not in runbook

    def test_restart_logging_env_raw(self, gcp):
        for name, svc in gcp["services"].items():
            assert svc.get("restart") == "unless-stopped", name
            assert (svc.get("logging") or {}).get("driver") == "gcplogs", name
        for name in ("api", "worker"):
            svc = gcp["services"][name]
            env_files = svc.get("env_file")
            env_files = [env_files] if isinstance(env_files, (str, dict)) else env_files
            paths = [e if isinstance(e, str) else e["path"] for e in env_files]
            assert any("app.env" in p for p in paths), name
            assert "/opt/nexdata/data/raw:/app/data/raw" in svc.get("volumes", []), name
            # scope gap: Kaggle downloads must survive a redeploy
            assert "/opt/nexdata/data/kaggle:/app/data/kaggle" in svc.get("volumes", []), name
            assert _env(svc)["DB_POOL_SIZE"] == "5" and _env(svc)["DB_MAX_OVERFLOW"] == "10", name
            deps = svc["depends_on"]
            assert deps["cloudsqlproxy"]["condition"] == "service_healthy"
            # secrets come only from the env_file: an environment: entry would shadow it
            assert not (set(_env(svc)) & {r[0] for r in _manifest()}), name
            assert _env(svc)["WORKER_MODE"] == "1"
            assert "DATABASE_URL" not in _env(svc)

    def test_proxy_uses_vm_service_account(self, gcp):
        proxy = gcp["services"]["cloudsqlproxy"]
        assert not proxy.get("volumes"), "no ADC / key file on the VM"
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in _env(proxy)
        cmd = proxy["command"] if isinstance(proxy["command"], str) else " ".join(proxy["command"])
        assert "nexdata-cloud:us-central1:nexdata-pg" in cmd
        assert "--credentials-file" not in cmd and "--json-credentials" not in cmd
        assert "--health-check" in cmd
        assert not proxy.get("ports"), "the proxy is reachable only on the compose network"


# ---------------------------------------------------------------------------
# T13 secrets manifest
# ---------------------------------------------------------------------------

def _env_names_read_by_app() -> set[str]:
    names = set()
    pat = re.compile(r"os\.(?:getenv|environ\.get)\(\s*[\"']([A-Z0-9_]+)[\"']|os\.environ\[\s*[\"']([A-Z0-9_]+)[\"']")
    for path in (REPO / "app").rglob("*.py"):
        for m in pat.finditer(path.read_text(encoding="utf-8")):
            names.add(m.group(1) or m.group(2))
    return names


@pytest.mark.unit
class TestSecretsManifest:
    def test_secrets_manifest_matches_settings(self):
        from app.core.config import Settings

        fields = {f.upper() for f in Settings.model_fields}
        known = fields | _env_names_read_by_app()
        rows = _manifest()
        names = [r[0] for r in rows]
        ids = [r[1] for r in rows]
        assert len(set(names)) == len(names) and len(set(ids)) == len(ids)
        for name, secret_id, mode in rows:
            assert name in known, f"{name} is neither a Settings field nor read by app/"
            assert re.fullmatch(r"nexdata-[a-z0-9-]+", secret_id), secret_id
            assert mode in ("required", "optional"), mode
        required = {n for n, _, m in rows if m == "required"}
        assert {"DATABASE_URL", "JWT_SECRET_KEY", "ENCRYPTION_KEY", "SEC_USER_AGENT"} <= required
        # nothing the VM must not carry, nothing set but unread
        for banned in ("GCLOUD_ADC", "GOOGLE_APPLICATION_CREDENTIALS", "XAI_API_KEY", "YELP_CLIENT_ID",
                       "WB_SERVICE_TOKEN", "WB_TOKEN_ENC_KEY", "RUN_SCHEDULER", "TAG"):
            assert banned not in names, banned

    def test_manifest_keys_are_read_somewhere_outside_config(self):
        """'Used' means read by code other than the Settings declaration / key tester."""
        corpus = "\n".join(
            p.read_text(encoding="utf-8") for p in (REPO / "app").rglob("*.py")
            if p.relative_to(REPO).as_posix() not in ("app/core/config.py", "app/api/v1/settings.py")
        )
        from app.core.config import Settings

        api_key_map = {k: v[0] for k, v in Settings._API_KEY_MAP.default.items()} \
            if hasattr(Settings._API_KEY_MAP, "default") else {k: v[0] for k, v in Settings._API_KEY_MAP.items()}
        for name, _, _ in _manifest():
            if name == "ENCRYPTION_KEY":    # read in app/api/v1/settings.py (Fernet key)
                continue
            lower = name.lower()
            sources = [s for s, field in api_key_map.items() if field == lower]
            used = (
                name in corpus or re.search(rf"\b{lower}\b", corpus)
                or any(re.search(rf"get_api_key\(\s*[\"']{s}[\"']", corpus) for s in sources)
            )
            assert used, f"{name} is not read anywhere outside config"


# ---------------------------------------------------------------------------
# T14-T16 deploy scripts and workflow
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDeployScripts:
    def test_vm_startup_script(self):
        path = REPO / "deploy" / "vm-startup.sh"
        text = path.read_text(encoding="utf-8")
        assert text.startswith("#!/usr/bin/env bash") or text.startswith("#!/bin/bash")
        assert "set -euo pipefail" in text
        assert not re.search(r"^\s*set\s+-\w*x", text, re.M), "would echo secret values into the serial log"
        assert "secrets.manifest" in text
        assert "gcloud secrets versions access" in text
        assert "nexdata-tag" in text and "nexdata-mode" in text
        assert 'MODE="${MODE:-dark}"' in text, "dark (no scheduler, no workers) is the default"
        assert "compose.dark.yml" in text
        assert "docker compose" in text and "docker-compose " not in text
        assert "ROOT=/opt/nexdata\n" in text and '"${ROOT}/data/raw"' in text
        assert "chmod 600" in text
        assert "\r\n" not in text, "CRLF breaks bash on the VM"
        bash = shutil.which("bash")
        if bash:
            res = subprocess.run([bash, "-n", str(path)], capture_output=True, text=True, timeout=30)
            assert res.returncode == 0, res.stderr

    def test_raw_sync_units(self):
        svc = (REPO / "deploy" / "systemd" / "nexdata-raw-sync.service").read_text(encoding="utf-8")
        timer = (REPO / "deploy" / "systemd" / "nexdata-raw-sync.timer").read_text(encoding="utf-8")
        exec_line = next(ln for ln in svc.splitlines() if ln.startswith("ExecStart="))
        assert "gcloud storage rsync" in exec_line
        assert "--recursive" in exec_line
        assert exec_line.rstrip().endswith("/opt/nexdata/data/raw gs://nexdata-raw-usc1/raw")
        assert "delete-unmatched" not in svc, "the archive never deletes"
        assert "Type=oneshot" in svc
        assert re.search(r"^OnCalendar=\*-\*-\* \d\d:\d\d", timer, re.M)
        assert "Persistent=true" in timer
        assert "nexdata-raw-sync.service" in timer or "Unit=" not in timer

    def test_runbook_sections(self):
        text = RUNBOOK.read_text(encoding="utf-8").lower()
        for heading in ("dark start", "cutover", "rollback", "connection budget", "secrets"):
            assert heading in text, heading
        assert "exactly one scheduler" in text
        assert "encryption_key" in text


@pytest.mark.unit
class TestBuildPushWorkflow:
    @pytest.fixture(scope="class")
    def wf(self):
        return yaml.safe_load((REPO / ".github" / "workflows" / "build-push.yml").read_text(encoding="utf-8"))

    def test_build_push_workflow(self, wf):
        on = wf.get("on", wf.get(True))
        # Finding: main's full CI has failed on every run since 2026-04, so a
        # workflow_run(conclusion == success) trigger would never fire. The
        # workflow runs on push to main behind its own deploy gate job.
        assert "workflow_run" not in on
        assert on["push"]["branches"] == ["main"]
        assert "workflow_dispatch" in on
        assert wf["permissions"]["id-token"] == "write"

        jobs = wf["jobs"]
        gate, build, deploy = jobs["gate"], jobs["build"], jobs["deploy"]
        gate_run = " ".join(str(s.get("run", "")) for s in gate["steps"])
        assert "pytest" in gate_run
        for spec in ("161", "160", "120", "121", "128"):
            assert f"tests/test_spec_{spec}_" in gate_run, spec
        assert "requirements-runtime.txt" in gate_run and "torch" not in gate_run
        assert build["needs"] == "gate" or build["needs"] == ["gate"]
        steps = build["steps"]
        auth = [s for s in steps if str(s.get("uses", "")).startswith("google-github-actions/auth@")]
        assert auth and "workload_identity_provider" in auth[0]["with"]
        assert "credentials_json" not in str(wf)
        text = (REPO / ".github" / "workflows" / "build-push.yml").read_text(encoding="utf-8")
        assert "us-central1-docker.pkg.dev" in text and "nexdata-cloud" in text
        assert "--target runtime" in text or "target: runtime" in text
        assert "import torch" in text, "smoke test proves torch is absent"

        assert deploy["environment"]["name"] == "production"   # required reviewers = approval
        assert "build" in deploy["needs"]
        assert "refs/heads/main" in deploy["if"]
        runs = " ".join(str(s.get("run", "")) for s in deploy["steps"])
        assert "gcloud compute ssh" in runs and "--tunnel-through-iap" in runs
        assert "nexdata-tag" in runs

    def test_deploy_propagates_exit_and_sets_tag_last(self, wf):
        # Finding: google_metadata_script_runner exits 0 whatever the script
        # did, and the tag used to be written before the script ran.
        steps = wf["jobs"]["deploy"]["steps"]
        runs = [str(s.get("run", "")) for s in steps]
        ssh_i = next(i for i, r in enumerate(runs) if "gcloud compute ssh" in r)
        tag_i = next(i for i, r in enumerate(runs) if "add-metadata" in r)
        assert tag_i > ssh_i, "nexdata-tag must be set only after the VM ran the tag"
        assert all("add-metadata" not in r for r in runs[:ssh_i])
        ssh = runs[ssh_i]
        assert "google_metadata_script_runner" not in ssh
        assert "NEXDATA_TAG=" in ssh and "startup-script" in ssh and "exit" in ssh
        assert "NEXDATA_API_DIGEST=" in ssh and "NEXDATA_WORKER_DIGEST=" in ssh
        digest = next(s for s in steps if s.get("id") == "digest")
        assert "sha256:[0-9a-f]{64}" in digest["run"]

    def test_wif_trust_is_scoped(self):
        text = (REPO / ".github" / "workflows" / "build-push.yml").read_text(encoding="utf-8")
        runbook = RUNBOOK.read_text(encoding="utf-8")
        for doc in (text, runbook):
            assert "assertion.repository == " in doc
            assert ":ref:refs/heads/main" in doc
            assert ":environment:production" in doc
        assert "--immutable-tags" in runbook
        assert "principalSet" not in runbook.replace("A repo-wide `principalSet`", "")



# ---------------------------------------------------------------------------
# Fix round: scheduler gate (SPEC_160 precondition, enforced on the VM)
# ---------------------------------------------------------------------------

GATE_SCRIPT = REPO / "deploy" / "check_scheduler_gate.py"
STARTUP = REPO / "deploy" / "vm-startup.sh"


def _run_gate(root: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(GATE_SCRIPT), str(root)], capture_output=True, text=True, timeout=60
    )


def _fake_app(root: Path, *, settings=True, leader=True, main_runtime=True,
              main_direct=False, main_stale=False) -> Path:
    core = root / "app" / "core"
    core.mkdir(parents=True)
    fields = "    run_scheduler: bool = True\n    db_pool_size: int = 5\n    db_max_overflow: int = 10\n"
    (core / "config.py").write_text(
        "class Settings:\n    database_url: str = ''\n" + (fields if settings else ""), encoding="utf-8"
    )
    if leader:
        (core / "scheduler_leader.py").write_text(
            "def resolve_stale_running_jobs(engine):\n    pass\n\n"
            "async def start_scheduler_runtime(register):\n    pass\n",
            encoding="utf-8",
        )
    body = ["async def lifespan(app):"]
    if main_runtime:
        body += ["    from app.core.scheduler_leader import start_scheduler_runtime",
                 "    await start_scheduler_runtime(None)"]
    if main_direct:
        body += ["    from app.core import scheduler_service", "    scheduler_service.start_scheduler()"]
    if main_stale:
        body += ["    sql = \"UPDATE ingestion_jobs SET error_message = 'Stale job auto-resolved on startup'\""]
    body += ["    yield"]
    (root / "app" / "main.py").write_text("\n".join(body) + "\n", encoding="utf-8")
    return root


@pytest.mark.unit
class TestSchedulerGate:
    def test_gated_image_passes(self, tmp_path):
        res = _run_gate(_fake_app(tmp_path))
        assert res.returncode == 0, res.stderr
        assert "ok" in res.stdout

    @pytest.mark.parametrize("kwargs, reason", [
        ({"settings": False}, "Settings lacks"),
        ({"leader": False}, "scheduler_leader.py missing"),
        ({"main_runtime": False}, "start_scheduler_runtime"),
        ({"main_direct": True}, "calls start_scheduler() directly"),
        ({"main_stale": True}, "stale resolver not gated"),
    ])
    def test_ungated_image_refused(self, tmp_path, kwargs, reason):
        res = _run_gate(_fake_app(tmp_path, **kwargs))
        assert res.returncode == 3
        assert reason in res.stderr

    def test_empty_root_refused(self, tmp_path):
        assert _run_gate(tmp_path).returncode == 3

    def test_probe_matches_this_tree(self):
        """The probe's verdict on this checkout matches what main.py really does."""
        res = _run_gate(REPO)
        main = (REPO / "app" / "main.py").read_text(encoding="utf-8")
        gated = "start_scheduler_runtime" in main and "scheduler_service.start_scheduler()" not in main
        assert (res.returncode == 0) == gated, res.stderr

    @pytest.mark.xfail(
        condition=not (REPO / "app" / "core" / "scheduler_leader.py").exists(),
        reason="SPEC_160 (RUN_SCHEDULER gating) not merged into this branch yet",
        strict=True,
    )
    def test_main_gates_scheduler_on_run_scheduler(self):
        """Finding: the compose RUN_SCHEDULER values must have code behind them."""
        assert _run_gate(REPO).returncode == 0
        config = (REPO / "app" / "core" / "config.py").read_text(encoding="utf-8")
        leader = (REPO / "app" / "core" / "scheduler_leader.py").read_text(encoding="utf-8")
        assert "run_scheduler" in config and "run_scheduler" in leader

    def test_probe_ships_in_the_runtime_image(self):
        dockerfile = (REPO / "Dockerfile").read_text(encoding="utf-8")
        runtime = dockerfile[dockerfile.index("AS runtime"):]
        assert "COPY deploy/ ./deploy/" in runtime
        ignore = (REPO / ".dockerignore").read_text(encoding="utf-8").split()
        assert "deploy" not in ignore and "*.py" not in ignore
        src = GATE_SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(src)
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        imported |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert imported <= {"__future__", "ast", "sys", "pathlib"}, "stdlib only: no app import, no DB"


@pytest.mark.unit
class TestStartupOrdering:
    def _main_body(self) -> str:
        text = STARTUP.read_text(encoding="utf-8")
        return text[text.index("\nmain() {"):]

    def test_every_check_runs_before_anything_is_switched(self):
        body = self._main_body()
        order = ["stage_release", "scheduler_gate", "busy_check", "render_env",
                 "commit_release", "install_units", "compose_up"]
        pos = [body.index(f"\n  {step}\n") for step in order]
        assert pos == sorted(pos), order
        assert "trap cleanup_staged EXIT" in body

    def test_gate_applies_to_dark_and_live(self):
        text = STARTUP.read_text(encoding="utf-8")
        fn = text[text.index("scheduler_gate() {"):text.index("\n}\n", text.index("scheduler_gate() {"))]
        assert "/app/deploy/check_scheduler_gate.py" in fn
        assert "--network none" in fn
        # the only bypass is live-only
        assert '[ "$MODE" = "live" ] && [ "${NEXDATA_SKIP_SCHEDULER_GATE:-0}" = "1" ]' in fn
        assert "die " in fn

    def test_compose_up_does_not_repull_or_write_state(self):
        text = STARTUP.read_text(encoding="utf-8")
        fn = text[text.index("compose_up() {"):text.index("\nmain() {")]
        code = "\n".join(ln for ln in fn.splitlines() if not ln.strip().startswith("#"))
        assert " pull" not in code, "images were pulled and digest-checked in stage_release"
        assert "TAG=" not in code, "compose.env is written only by commit_release"

    def test_runbook_dark_start_warns(self):
        text = RUNBOOK.read_text(encoding="utf-8")
        dark = text[text.index("## Dark start"):text.index("## Cutover")]
        assert "SPEC_160" in dark and "2 h" in dark and "alembic" in dark
        assert "google_metadata_script_runner startup'" not in text, "use run_startup (exit status)"


# ---------------------------------------------------------------------------
# Fix round: vm-startup.sh behaviour, with docker/gcloud/curl stubbed
# ---------------------------------------------------------------------------

def _git_bash():
    bash = shutil.which("bash")
    if not bash or "system32" in bash.lower():   # WSL's bash.exe is not a POSIX shell here
        return None
    return bash


GOOD_API = "sha256:" + "a" * 64
GOOD_WORKER = "sha256:" + "b" * 64

STUBS = r"""
log_call() { printf '%s\n' "$*" >> "$STUB_LOG"; }
install_docker() { :; }
flock() { :; }
find() { :; }
chown() { :; }
sleep() { :; }
systemctl() { log_call "systemctl $*"; }
install() { log_call "install $*"; }
curl() {
  case "$*" in
    *computeMetadata*) return 22 ;;
    */health*) printf '{"queue": {"running": %s}}' "${STUB_RUNNING:-0}" ;;
    */readyz*) return 0 ;;
    *) return 7 ;;
  esac
}
python3() { cat >/dev/null; echo "${STUB_RUNNING:-0}"; }
gcloud() {
  case "$1 $2" in
    "auth configure-docker") return 0 ;;
    "secrets versions") ;;
    *) log_call "gcloud $*"; return 0 ;;
  esac
  local s
  for a in "$@"; do case "$a" in --secret=*) s="${a#--secret=}" ;; esac; done
  case " ${STUB_MISSING:-} " in *" $s "*) return 1 ;; esac
  printf 'value-%s' "$s"
}
docker() {
  case "$1" in
    ps) [ "${STUB_STACK:-0}" = "1" ] && echo c0ffee; return 0 ;;
    pull) log_call "docker $*"; return 0 ;;
    image)
      local ref="${@: -1}" repo
      repo="${ref##*/}"; repo="${repo%%:*}"
      if [ "$repo" = api ]; then echo "${IMAGE_BASE}/api@${STUB_API_DIGEST}"; else echo "${IMAGE_BASE}/worker@${STUB_WORKER_DIGEST}"; fi ;;
    create) echo cid123 ;;
    cp) cp -r "$STUB_IMAGE_DEPLOY/." "${@: -1}" ;;
    rm) return 0 ;;
    run) log_call "docker $*"; return "${STUB_GATE:-0}" ;;
    compose) log_call "docker $*"; return 0 ;;
    *) log_call "docker $*"; return 0 ;;
  esac
}
"""


@pytest.mark.unit
@pytest.mark.skipif(_git_bash() is None, reason="POSIX bash not available")
class TestStartupBehaviour:
    @pytest.fixture
    def vm(self, tmp_path):
        root = tmp_path / "opt_nexdata"
        image_deploy = tmp_path / "image_deploy"
        shutil.copytree(REPO / "deploy", image_deploy)
        shutil.copy(GCP_COMPOSE, image_deploy / "docker-compose.gcp.yml")
        script = STARTUP.read_text(encoding="utf-8")
        assert "ROOT=/opt/nexdata\n" in script and script.rstrip().endswith('main "$@"')
        script = script.replace("ROOT=/opt/nexdata\n", f"ROOT='{root.as_posix()}'\n")
        script = script.rstrip()[: -len('main "$@"')]
        harness = tmp_path / "harness.sh"
        harness.write_text(
            script + "\n" + STUBS + '\nmain "$@"\n', encoding="utf-8", newline="\n"
        )
        log = tmp_path / "calls.log"
        log.write_text("", encoding="utf-8")

        def run(**env):
            full = {
                **os.environ,
                "NEXDATA_TAG": "newsha",
                "NEXDATA_MODE": "dark",
                "STUB_LOG": log.as_posix(),
                "STUB_IMAGE_DEPLOY": image_deploy.as_posix(),
                "STUB_API_DIGEST": GOOD_API,
                "STUB_WORKER_DIGEST": GOOD_WORKER,
                **env,
            }
            log.write_text("", encoding="utf-8")
            res = subprocess.run([_git_bash(), harness.as_posix()], capture_output=True,
                                 text=True, timeout=120, env=full)
            return res, log.read_text(encoding="utf-8")

        def seed_running(tag="oldsha"):
            (root / "compose").mkdir(parents=True)
            (root / "compose" / "marker").write_text(tag, encoding="utf-8")
            (root / "app.env").write_text("OLD=1\n", encoding="utf-8")
            (root / "compose.env").write_text(f"TAG={tag}\n", encoding="utf-8")

        def state():
            return {
                "marker": (root / "compose" / "marker").read_text(encoding="utf-8")
                if (root / "compose" / "marker").exists() else None,
                "app_env": (root / "app.env").read_text(encoding="utf-8") if (root / "app.env").exists() else None,
                "compose_env": (root / "compose.env").read_text(encoding="utf-8")
                if (root / "compose.env").exists() else None,
                "staged": [p.name for p in root.glob("*.new")] if root.exists() else [],
            }

        return run, seed_running, state

    def test_dark_with_gated_image_switches(self, vm):
        run, seed, state = vm
        seed()
        res, calls = run(NEXDATA_API_DIGEST=GOOD_API, NEXDATA_WORKER_DIGEST=GOOD_WORKER)
        assert res.returncode == 0, res.stderr + res.stdout
        s = state()
        assert s["compose_env"] == "TAG=newsha\n"
        assert s["marker"] is None   # compose/ is the new image's set
        assert "DATABASE_URL='value-nexdata-database-url'" in s["app_env"]
        assert s["staged"] == []
        assert "check_scheduler_gate.py" in calls
        assert "compose.dark.yml" in calls and "up -d --remove-orphans cloudsqlproxy api" in calls
        assert calls.index("docker run") < calls.index("docker compose")   # gate before compose
        compose_lines = [ln for ln in calls.splitlines() if ln.startswith("docker compose")]
        assert compose_lines and not any(" pull" in ln for ln in compose_lines)   # no re-pull

    @pytest.mark.parametrize("mode", ["dark", "live"])
    def test_ungated_image_refused_and_nothing_changes(self, vm, mode):
        run, seed, state = vm
        seed()
        before = state()
        res, calls = run(NEXDATA_MODE=mode, STUB_GATE="3")
        assert res.returncode != 0
        assert "SPEC_160" in res.stderr
        assert state() == before
        assert "docker compose" not in calls

    def test_gate_bypass_is_live_only(self, vm):
        run, seed, state = vm
        seed()
        res, _ = run(NEXDATA_MODE="dark", STUB_GATE="3", NEXDATA_SKIP_SCHEDULER_GATE="1")
        assert res.returncode != 0
        res, calls = run(NEXDATA_MODE="live", STUB_GATE="3", NEXDATA_SKIP_SCHEDULER_GATE="1")
        assert res.returncode == 0, res.stderr
        assert "gate skipped" in res.stdout
        assert state()["compose_env"] == "TAG=newsha\n"

    def test_busy_live_refusal_leaves_running_version(self, vm):
        run, seed, state = vm
        seed()
        before = state()
        res, calls = run(NEXDATA_MODE="live", STUB_STACK="1", STUB_RUNNING="2")
        assert res.returncode != 0
        assert "2 job(s) running" in res.stderr and "Nothing changed" in res.stderr
        assert state() == before
        assert "docker compose" not in calls
        res, _ = run(NEXDATA_MODE="live", STUB_STACK="1", STUB_RUNNING="2", NEXDATA_FORCE="1")
        assert res.returncode == 0, res.stderr
        assert state()["compose_env"] == "TAG=newsha\n"

    def test_missing_secret_leaves_running_version(self, vm):
        run, seed, state = vm
        seed()
        before = state()
        res, calls = run(STUB_MISSING="nexdata-database-url")
        assert res.returncode != 0
        assert "nexdata-database-url" in res.stderr
        assert state() == before

    def test_digest_mismatch_refused(self, vm):
        run, seed, state = vm
        seed()
        before = state()
        res, _ = run(NEXDATA_API_DIGEST="sha256:" + "c" * 64)
        assert res.returncode != 0 and "tag moved" in res.stderr
        assert state() == before
        res, _ = run(NEXDATA_WORKER_DIGEST="sha256:" + "c" * 64)
        assert res.returncode != 0
        res, _ = run(NEXDATA_API_DIGEST="not-a-digest")
        assert res.returncode != 0 and "invalid expected digest" in res.stderr
