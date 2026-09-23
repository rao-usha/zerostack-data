"""Mart build ledger and the guarded build runner (SPEC_126a).

``run_guarded`` is what the ``pe_mart_build`` and ``entity_resolve`` workers
run. One ``core.mart_build`` row per run:

1. insert ``running`` (own transaction);
2. assert inputs (``app.marts.inputs``) -> ``refused`` and raise
   ``MartInputRefused``; nothing is built, the previous mart is untouched;
3. build every stage on ONE connection inside ONE transaction, count the
   published rows, evaluate the ship gates (``app.marts.gates``) against the
   previous successful non-dry-run build, then commit — or roll back when a
   gate fails (or always, for a dry run);
4. finish ``success`` / ``failed`` with inputs, counts, gate results, error.

Build-then-verify-then-commit is the safest semantics available: the stages
used to commit one by one, so a gate could only have reported damage already
published. A gate failure now leaves the previous mart exactly as it was, and
the job fails (``MartGateFailed``) — never a success with a bad mart.

The ledger writes use their own short transactions, so a rolled-back build
still leaves its row.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import text

from app.marts import gates as gates_mod
from app.marts import inputs as inputs_mod

logger = logging.getLogger(__name__)

APP_VERSION = "0.1.0"
ABANDONED_AFTER = timedelta(hours=6)
REPO_ROOT = Path(__file__).resolve().parents[2]


class MartInputRefused(RuntimeError):
    """An input's latest release failed, is missing or is stale."""


class MartGateFailed(RuntimeError):
    """The build ran but failed its ship gates; it was rolled back."""


# --- code version --------------------------------------------------------------


def _git_sha(root: Path) -> Optional[str]:
    git = root / ".git"
    try:
        if git.is_file():  # a worktree: "gitdir: <path>"
            git = Path(git.read_text(encoding="utf-8").split(":", 1)[1].strip())
        head = (git / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref:"):
            return head[:40] or None
        ref = head.split(":", 1)[1].strip()
        for base in (git, git.parent.parent if git.parent.name == "worktrees" else None):
            if base is None:
                continue
            p = base / ref
            if p.is_file():
                return p.read_text(encoding="utf-8").strip()[:40] or None
            packed = base / "packed-refs"
            if packed.is_file():
                for line in packed.read_text(encoding="utf-8").splitlines():
                    if line.endswith(" " + ref):
                        return line.split(" ", 1)[0][:40]
    except (OSError, IndexError, ValueError):
        return None
    return None


def code_version() -> str:
    """Env ``NEXDATA_GIT_SHA`` / ``GIT_SHA``, else the repo's git HEAD, else the app version."""
    for var in ("NEXDATA_GIT_SHA", "GIT_SHA"):
        v = (os.environ.get(var) or "").strip()
        if v:
            return v[:64]
    return _git_sha(REPO_ROOT) or f"app-{APP_VERSION}"


# --- ledger rows -----------------------------------------------------------------


def _json(value) -> Optional[str]:
    return None if value is None else json.dumps(value, default=str)


def compact_summary(summary: Mapping[str, Any]) -> Dict[str, Any]:
    """Keep per-stage scalar counters; drop lists and large detail blobs
    (``refused_detail`` can hold thousands of entries)."""
    out: Dict[str, Any] = {}
    for stage, value in (summary or {}).items():
        if isinstance(value, Mapping):
            kept = {}
            for k, v in value.items():
                if isinstance(v, (int, float, str, bool)) or v is None:
                    kept[k] = v
                elif isinstance(v, Mapping) and len(v) <= 50 and all(
                        isinstance(x, (int, float, str, bool)) or x is None for x in v.values()):
                    kept[k] = dict(v)
            out[stage] = kept
        elif isinstance(value, (int, float, str, bool)) or value is None:
            out[stage] = value
    return out


def start_build(engine, mart: str, *, dry_run: bool = False,
                ingestion_job_id: Optional[int] = None,
                job_queue_id: Optional[int] = None,
                overrides: Optional[Mapping[str, Any]] = None) -> int:
    with engine.begin() as conn:
        # a worker that died mid-build left its row running
        conn.execute(text(
            "UPDATE core.mart_build SET status = 'failed', finished_at = NOW(), "
            "error = 'abandoned: still running when a later build started' "
            "WHERE mart = :m AND status = 'running' AND started_at < :cutoff"
        ), {"m": mart, "cutoff": datetime.utcnow() - ABANDONED_AFTER})
        return int(conn.execute(text(
            "INSERT INTO core.mart_build (mart, status, dry_run, started_at, code_version, "
            "overrides, ingestion_job_id, job_queue_id) "
            "VALUES (:m, 'running', :dry, :now, :cv, CAST(:ov AS JSONB), :ij, :jq) RETURNING id"
        ), {"m": mart, "dry": bool(dry_run), "now": datetime.utcnow(), "cv": code_version(),
            "ov": _json(dict(overrides or {})), "ij": ingestion_job_id,
            "jq": job_queue_id}).scalar())


def finish_build(engine, build_id: int, status: str, **fields) -> None:
    allowed = {"inputs", "stage_counts", "gate_results", "overrides", "refusal_reason", "error"}
    assert set(fields) <= allowed, set(fields) - allowed
    sets = ["status = :status", "finished_at = :now"]
    params: Dict[str, Any] = {"id": build_id, "status": status, "now": datetime.utcnow()}
    for k, v in fields.items():
        if k in ("refusal_reason", "error"):
            sets.append(f"{k} = :{k}")
            params[k] = None if v is None else str(v)[:4000]
        else:
            sets.append(f"{k} = CAST(:{k} AS JSONB)")
            params[k] = _json(v)
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE core.mart_build SET {', '.join(sets)} WHERE id = :id"), params)


def previous_success(engine, mart: str, before_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """``stage_counts`` of the latest successful non-dry-run build."""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT id, stage_counts FROM core.mart_build "
            "WHERE mart = :m AND status = 'success' AND NOT dry_run "
            "AND (CAST(:b AS BIGINT) IS NULL OR id < :b) ORDER BY id DESC LIMIT 1"
        ), {"m": mart, "b": before_id}).mappings().first()
    if row is None:
        return None
    counts = row["stage_counts"]
    if isinstance(counts, str):
        counts = json.loads(counts)
    return {"build_id": row["id"], **(counts or {})}


def recent_builds(conn, mart: Optional[str] = None, status: Optional[str] = None,
                  limit: int = 50) -> List[Dict[str, Any]]:
    rows = conn.execute(text(
        "SELECT * FROM core.mart_build "
        "WHERE (CAST(:m AS TEXT) IS NULL OR mart = :m) "
        "AND (CAST(:s AS TEXT) IS NULL OR status = :s) "
        "ORDER BY started_at DESC, id DESC LIMIT :n"
    ), {"m": mart, "s": status, "n": int(limit)}).mappings().all()
    return [dict(r) for r in rows]


# --- the guarded run ---------------------------------------------------------------


def _failure_text(results: Mapping[str, Mapping[str, Any]], names: Sequence[str]) -> str:
    return "; ".join(f"{n}: {results[n].get('detail') or results[n].get('value')}" for n in names)


def run_guarded(
    engine,
    *,
    mart: str,
    sources: Sequence[str],
    build: Callable[[Any], Dict[str, Any]],
    dry_run: bool = False,
    input_override=None,
    input_max_age_days: Optional[Mapping[str, float]] = None,
    gate_override=None,
    ingestion_job_id: Optional[int] = None,
    job_queue_id: Optional[int] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Assert inputs, build in one transaction, gate, commit. See module doc.

    ``build(conn)`` runs every stage on ``conn`` and returns the summary.
    Returns the summary with ``mart_build`` ({id, status, gates_failed}).
    """
    overrides: Dict[str, Any] = {}
    if input_override:
        overrides["input_override"] = input_override
    if input_max_age_days:
        overrides["input_max_age_days"] = dict(input_max_age_days)
    if gate_override:
        overrides["gate_override"] = gate_override

    build_id = start_build(engine, mart, dry_run=dry_run, ingestion_job_id=ingestion_job_id,
                           job_queue_id=job_queue_id, overrides=overrides)

    # --- 1. inputs ---------------------------------------------------------------
    try:
        with engine.connect() as conn:
            records, problems = inputs_mod.check_inputs(conn, sources, now=now,
                                                        max_age_days=input_max_age_days)
    except Exception as e:
        finish_build(engine, build_id, "failed", error=f"input check failed: {type(e).__name__}: {e}")
        raise
    all_, allowed = inputs_mod.normalize_override(input_override)
    blocking = [r["problem"] for r in records
                if not r["ok"] and not (all_ or r["source"] in allowed)]
    accepted = [r["problem"] for r in records if not r["ok"] and r["problem"] not in blocking]
    if accepted:
        overrides["inputs_accepted"] = accepted
    if blocking:
        reason = "refused: " + "; ".join(blocking)
        finish_build(engine, build_id, "refused", inputs=records, refusal_reason=reason,
                     overrides=overrides)
        logger.warning(f"[mart_build:{mart}] #{build_id} {reason}")
        raise MartInputRefused(f"{mart} build #{build_id} {reason}")

    # --- 2. build + gates in one transaction ----------------------------------
    baseline = previous_success(engine, mart, before_id=build_id)
    summary: Dict[str, Any] = {}
    stage_counts: Dict[str, Any] = {}
    results: Dict[str, Any] = {}
    failed: List[str] = []
    try:
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                summary = build(conn) or {}
                counts = gates_mod.count_published(conn, mart)
                stage_counts = {"stages": compact_summary(summary), "tables": counts}
                results = gates_mod.evaluate(mart, summary, counts, baseline)
                failed = gates_mod.failures(results)
                blocked = gates_mod.failures(results, override=gate_override)
                for name in failed:
                    if name not in blocked:
                        results[name]["overridden"] = True
                if dry_run or blocked:
                    tx.rollback()
                else:
                    tx.commit()
            except BaseException:
                tx.rollback()
                raise
    except Exception as e:
        finish_build(engine, build_id, "failed", inputs=records,
                     stage_counts=stage_counts or None, gate_results=results or None,
                     overrides=overrides, error=f"{type(e).__name__}: {e}")
        raise

    if baseline:
        stage_counts["baseline_build_id"] = baseline.get("build_id")
    blocked = [n for n in failed if not results[n].get("overridden")]
    if blocked:
        error = "ship gates failed: " + _failure_text(results, blocked)
        finish_build(engine, build_id, "failed", inputs=records, stage_counts=stage_counts,
                     gate_results=results, overrides=overrides, error=error)
        logger.error(f"[mart_build:{mart}] #{build_id} {error}")
        raise MartGateFailed(
            f"{mart} build #{build_id} {'(dry run) ' if dry_run else ''}rolled back, {error}")

    finish_build(engine, build_id, "success", inputs=records, stage_counts=stage_counts,
                 gate_results=results, overrides=overrides)
    summary["mart_build"] = {"id": build_id, "status": "success",
                             "gates_overridden": [n for n in failed],
                             "dry_run": bool(dry_run)}
    logger.info(f"[mart_build:{mart}] #{build_id} success"
                f"{' (dry run, rolled back)' if dry_run else ''}")
    return summary
