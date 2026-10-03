"""Does this image's code keep a second api from double-running the scheduler?

SPEC_161 fix round. deploy/vm-startup.sh runs this INSIDE the image it is about
to start (``docker run --rm IMG python /app/deploy/check_scheduler_gate.py``)
and refuses dark and live mode unless it exits 0. It is static (stdlib, no
imports of app code, no DATABASE_URL), so it is fast and cannot touch a DB.

It requires the SPEC_160 guarantees the VM depends on while the laptop stack
still shares Cloud SQL:

  1. Settings has run_scheduler, db_pool_size and db_max_overflow
     (RUN_SCHEDULER / DB_POOL_SIZE / DB_MAX_OVERFLOW are honoured);
  2. app/core/scheduler_leader.py defines start_scheduler_runtime and
     resolve_stale_running_jobs (leader lock, leader-only stale resolver);
  3. app/main.py starts the scheduler only through start_scheduler_runtime:
     no direct scheduler_service.start_scheduler() call and no unconditional
     "Stale job auto-resolved on startup" UPDATE in the lifespan.

An image built before this file existed has no /app/deploy/check_scheduler_gate.py,
so `docker run` fails and the startup script refuses it too (fail closed).

Usage: python check_scheduler_gate.py [APP_ROOT]   (default: /app)
Exit 0 = gated; 3 = not gated (reasons on stderr); 2 = bad usage.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REQUIRED_SETTINGS = ("run_scheduler", "db_pool_size", "db_max_overflow")
REQUIRED_LEADER_FUNCS = ("start_scheduler_runtime", "resolve_stale_running_jobs")
STALE_UPDATE_MARKER = "Stale job auto-resolved on startup"


def _parse(path: Path):
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, ValueError):
        return None


def _settings_fields(tree) -> set[str]:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "Settings":
            return {
                stmt.target.id
                for stmt in node.body
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
            }
    return set()


def _top_level_funcs(tree) -> set[str]:
    return {
        n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _direct_start_scheduler_calls(tree) -> int:
    """Calls of <x>.start_scheduler(...) or start_scheduler(...) in main.py."""
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
            if name == "start_scheduler":
                count += 1
    return count


def _string_constants(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value


def problems(app_root: Path) -> list[str]:
    out: list[str] = []
    app = app_root / "app"

    config = _parse(app / "core" / "config.py")
    if config is None:
        out.append("app/core/config.py missing or unparseable")
    else:
        missing = [f for f in REQUIRED_SETTINGS if f not in _settings_fields(config)]
        if missing:
            out.append(f"Settings lacks {', '.join(missing)} (SPEC_160)")

    leader = _parse(app / "core" / "scheduler_leader.py")
    if leader is None:
        out.append("app/core/scheduler_leader.py missing (SPEC_160 leader lock)")
    else:
        missing = [f for f in REQUIRED_LEADER_FUNCS if f not in _top_level_funcs(leader)]
        if missing:
            out.append(f"scheduler_leader lacks {', '.join(missing)}")

    main = _parse(app / "main.py")
    if main is None:
        out.append("app/main.py missing or unparseable")
    else:
        names = {n.attr for n in ast.walk(main) if isinstance(n, ast.Attribute)} | {
            n.id for n in ast.walk(main) if isinstance(n, ast.Name)
        } | {
            a.name for n in ast.walk(main) if isinstance(n, ast.ImportFrom) for a in n.names
        }
        if "start_scheduler_runtime" not in names:
            out.append("app/main.py does not start the scheduler through start_scheduler_runtime")
        if _direct_start_scheduler_calls(main):
            out.append("app/main.py calls start_scheduler() directly (ignores RUN_SCHEDULER)")
        if any(STALE_UPDATE_MARKER in s and "UPDATE" in s for s in _string_constants(main)):
            out.append("app/main.py still fails >2 h running jobs on every start (stale resolver not gated)")
    return out


def main(argv: list[str]) -> int:
    if len(argv) > 2:
        print(__doc__, file=sys.stderr)
        return 2
    root = Path(argv[1]) if len(argv) == 2 else Path("/app")
    found = problems(root)
    if found:
        for p in found:
            print(f"scheduler gate: {p}", file=sys.stderr)
        return 3
    print("scheduler gate: ok (RUN_SCHEDULER, leader lock, gated stale resolver, DB pool knobs)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
