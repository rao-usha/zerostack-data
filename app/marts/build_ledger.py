"""Mart build ledger and the guarded build runner (SPEC_126a).

``run_guarded`` is what the ``pe_mart_build`` and ``entity_resolve`` workers
run. One ``core.mart_build`` row per run:

1. open the build connection and transaction and take the per-mart advisory
   lock (``pg_try_advisory_xact_lock``); another build of the same mart
   holding it => ``refused`` "busy:" and ``MartBuildBusy``. Only a build that
   holds the lock closes older ``running`` rows as abandoned, so a long but
   live build is never marked dead;
2. insert ``running`` (own transaction);
3. assert inputs (``app.marts.inputs``: bulk sources and upstream marts) ->
   ``refused`` and raise ``MartInputRefused``; nothing is built, the previous
   mart is untouched;
4. build every stage on the ONE connection inside the ONE transaction, count
   the published rows, evaluate the ship gates (``app.marts.gates``) against
   the previous successful non-dry-run build (or, before the first one, the
   published counts read at the start of this transaction), then commit — or
   roll back when a gate fails (or always, for a dry run);
5. finish ``success`` / ``failed`` with inputs, counts, gate results, error.

Build-then-verify-then-commit is the safest semantics available: the stages
used to commit one by one, so a gate could only have reported damage already
published. A gate failure now leaves the previous mart exactly as it was, and
the job fails (``MartGateFailed``) — never a success with a bad mart.

Cancellation: the worker cannot interrupt the build thread, so the executor
passes a ``cancel_event`` (set when the job is cancelled or times out) and
``run_guarded`` also re-reads the ``job_queue`` row. Either one, checked
between stages (``checkpoint``) and again just before commit, rolls the build
back and finishes the row ``failed`` "cancelled" (``MartBuildCancelled``) —
the ledger never says success for a job the queue marked failed.

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

# pg advisory lock key space: (LOCK_NAMESPACE, hashtext('mart_build:' || mart))
LOCK_NAMESPACE = 1260
BUSY_PREFIX = "busy:"


class MartInputRefused(RuntimeError):
    """An input's latest release failed, is missing or is stale."""


class MartBuildBusy(MartInputRefused):
    """Another build of the same mart holds the lock."""


class MartGateFailed(RuntimeError):
    """The build ran but failed its ship gates; it was rolled back."""


class MartBuildCancelled(RuntimeError):
    """The job was cancelled or timed out while the build ran; rolled back."""


# --- locking -------------------------------------------------------------------------


def _lock_key(mart: str) -> str:
    return f"mart_build:{mart}"


def try_build_lock(conn, mart: str) -> bool:
    """Take the per-mart transaction-scoped advisory lock on ``conn``'s open tx."""
    return bool(conn.execute(
        text("SELECT pg_try_advisory_xact_lock(:ns, hashtext(:k))"),
        {"ns": LOCK_NAMESPACE, "k": _lock_key(mart)},
    ).scalar())


def build_lock_held(conn, mart: str) -> bool:
    """Whether some session holds ``mart``'s build lock (read from pg_locks,
    without taking it, so a check never races a starting build)."""
    return bool(conn.execute(text(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted "
        "AND objsubid = 2 AND classid::bigint = :ns "
        "AND objid::bigint = (hashtext(:k)::bigint & 4294967295))"
    ), {"ns": LOCK_NAMESPACE, "k": _lock_key(mart)}).scalar())


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
                overrides: Optional[Mapping[str, Any]] = None,
                close_abandoned: bool = True) -> int:
    """Insert the ``running`` row. ``close_abandoned``: the caller holds the
    mart's build lock, so any older ``running`` row is from a dead worker."""
    with engine.begin() as conn:
        if close_abandoned:
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


def close_abandoned(conn, mart: str, cutoff: datetime) -> int:
    """Mark ``running`` rows older than ``cutoff`` failed when nobody holds the
    mart's build lock (the watchdog sweep). Returns the number closed."""
    if build_lock_held(conn, mart):
        return 0
    return conn.execute(text(
        "UPDATE core.mart_build SET status = 'failed', finished_at = NOW(), "
        "error = 'abandoned: still running with no live build (worker died?)' "
        "WHERE mart = :m AND status = 'running' AND started_at < :cutoff"
    ), {"m": mart, "cutoff": cutoff}).rowcount or 0


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


def job_aborted(engine, job_queue_id: Optional[int]) -> Optional[str]:
    """The queue row's status when it is no longer running (cancelled, failed
    by a timeout), else None. A missing table or row is not an abort."""
    if not job_queue_id:
        return None
    try:
        with engine.connect() as conn:
            if conn.execute(text("SELECT to_regclass('public.job_queue')")).scalar() is None:
                return None
            status = conn.execute(text(
                "SELECT LOWER(CAST(status AS TEXT)) FROM job_queue WHERE id = :i"
            ), {"i": int(job_queue_id)}).scalar()
    except Exception as e:  # never fail a build on the check itself
        logger.warning(f"[mart_build] could not read job_queue #{job_queue_id}: {e}")
        return None
    if status is None or status in ("running", "pending", "claimed"):
        return None
    return status


def run_guarded(
    engine,
    *,
    mart: str,
    sources: Sequence[str],
    build: Callable[..., Dict[str, Any]],
    dry_run: bool = False,
    input_override=None,
    input_max_age_days: Optional[Mapping[str, float]] = None,
    gate_override=None,
    ingestion_job_id: Optional[int] = None,
    job_queue_id: Optional[int] = None,
    now: Optional[datetime] = None,
    upstream: Sequence[str] = (),
    cancel_event=None,
) -> Dict[str, Any]:
    """Lock, assert inputs, build in one transaction, gate, commit. See module doc.

    ``build(conn, checkpoint)`` runs every stage on ``conn`` and returns the
    summary; it should call ``checkpoint()`` between stages (raises
    ``MartBuildCancelled`` once the job is cancelled). ``upstream`` names
    marts whose latest ledger row must be a recent success.
    Returns the summary with ``mart_build`` ({id, status, gates_overridden}).
    """
    overrides: Dict[str, Any] = {}
    if input_override:
        overrides["input_override"] = input_override
    if input_max_age_days:
        overrides["input_max_age_days"] = dict(input_max_age_days)
    if gate_override:
        overrides["gate_override"] = gate_override

    def aborted() -> Optional[str]:
        if cancel_event is not None and cancel_event.is_set():
            return "job cancelled or timed out"
        status = job_aborted(engine, job_queue_id)
        return f"job_queue #{job_queue_id} is {status}" if status else None

    def checkpoint() -> None:
        why = aborted()
        if why:
            raise MartBuildCancelled(f"cancelled: {why}")

    with engine.connect() as conn:
        tx = conn.begin()
        try:
            # --- 0. one build of a mart at a time -------------------------------
            locked = try_build_lock(conn, mart)
            build_id = start_build(engine, mart, dry_run=dry_run,
                                   ingestion_job_id=ingestion_job_id,
                                   job_queue_id=job_queue_id, overrides=overrides,
                                   close_abandoned=locked)
            if not locked:
                tx.rollback()
                reason = f"{BUSY_PREFIX} another {mart} build is running"
                finish_build(engine, build_id, "refused", refusal_reason=reason,
                             overrides=overrides)
                logger.warning(f"[mart_build:{mart}] #{build_id} {reason}")
                raise MartBuildBusy(f"{mart} build #{build_id} refused, {reason}")

            # --- 1. inputs -------------------------------------------------------
            try:
                with engine.connect() as check_conn:
                    records, _ = inputs_mod.check_inputs(
                        check_conn, sources, now=now, max_age_days=input_max_age_days,
                        upstream=upstream)
            except Exception as e:
                tx.rollback()
                finish_build(engine, build_id, "failed",
                             error=f"input check failed: {type(e).__name__}: {e}")
                raise
            all_, allowed = inputs_mod.normalize_override(input_override)
            blocking = [r["problem"] for r in records
                        if not r["ok"] and not (all_ or r["source"] in allowed)]
            accepted = [r["problem"] for r in records
                        if not r["ok"] and r["problem"] not in blocking]
            if accepted:
                overrides["inputs_accepted"] = accepted
            if blocking:
                tx.rollback()
                reason = "refused: " + "; ".join(blocking)
                finish_build(engine, build_id, "refused", inputs=records,
                             refusal_reason=reason, overrides=overrides)
                logger.warning(f"[mart_build:{mart}] #{build_id} {reason}")
                raise MartInputRefused(f"{mart} build #{build_id} {reason}")

            # --- 2. build + gates in the one transaction ------------------------
            baseline = previous_success(engine, mart, before_id=build_id)
            stage_counts: Dict[str, Any] = {}
            results: Dict[str, Any] = {}
            try:
                before = gates_mod.count_published(conn, mart)
                if baseline is None and before:
                    # first ledgered build: the published rows it replaces are
                    # the only baseline there is (row-drop and mass-merge gates)
                    baseline = {"build_id": None, "stages": {}, "tables": before}
                summary = build(conn, checkpoint) or {}
                counts = gates_mod.count_published(conn, mart)
                stage_counts = {"stages": compact_summary(summary), "tables": counts,
                                "tables_before": before}
                probes = gates_mod.probe_published(conn, mart, summary)
                if probes:
                    stage_counts["probes"] = probes
                results = gates_mod.evaluate(mart, summary, counts, baseline, probes=probes)
                failed = gates_mod.failures(results)
                blocked = gates_mod.failures(results, override=gate_override)
                for name in failed:
                    if name not in blocked:
                        results[name]["overridden"] = True
                checkpoint()  # last look before anything becomes visible
                if dry_run or blocked:
                    tx.rollback()
                else:
                    tx.commit()
            except BaseException as e:
                if tx.is_active:
                    tx.rollback()
                status_error = (str(e) if isinstance(e, MartBuildCancelled)
                                else f"{type(e).__name__}: {e}")
                finish_build(engine, build_id, "failed", inputs=records,
                             stage_counts=stage_counts or None, gate_results=results or None,
                             overrides=overrides, error=status_error)
                raise
        except BaseException:
            if tx.is_active:
                tx.rollback()
            raise

    if baseline:
        stage_counts["baseline_build_id"] = baseline.get("build_id")
        if baseline.get("build_id") is None:
            stage_counts["baseline"] = "pre-build published counts"
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
