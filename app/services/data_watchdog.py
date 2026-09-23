"""
Data watchdog (SPEC_128).

The API ingestor fleet sat dead for ~159 days and nothing told anyone: every
status surface was pull-only, and the ones that existed counted failed jobs
as activity. This module *pushes*.

Every 15 minutes (APScheduler, registered from the app lifespan) it checks:

- **worker**        jobs waiting in ``job_queue`` and no live ``worker_heartbeats`` row
- **stalled**       an active schedule whose last *successful* run is older than
                    1.5x its cadence (or that never succeeded)
- **releases**      ``raw.source_release`` rows that failed in the last 24h
- **failure spike** many failed ``job_queue`` rows in the last hour
- **SLA**           ``source_freshness_sla`` rows that are violated

Each finding has a stable key. ``watchdog_alerts`` holds its state so an alert
is sent once, reminded at most every 24h, and followed by a ``resolved``
notice. Delivery is a Slack-compatible ``{"text": ...}`` POST to
``ALERT_WEBHOOK_URL`` plus a log line at WARNING/ERROR. After each completed
run ``HEARTBEAT_PING_URL`` is pinged, so an external dead-man's switch notices
when the whole stack -- API, scheduler, database -- is gone.

Success evidence is only ever read from successful rows: ``ingestion_jobs``
and ``job_queue`` with status ``success``, and ``raw.source_release`` rows
that ``loaded``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models_watchdog import WatchdogAlert

logger = logging.getLogger(__name__)

JOB_ID = "system_data_watchdog"
DEFAULT_INTERVAL_MINUTES = 15
STALL_FACTOR = 1.5
RENOTIFY_AFTER = timedelta(hours=24)
HTTP_TIMEOUT_SECONDS = 10.0
# Arbitrary constant for pg_try_advisory_xact_lock: one watchdog run at a time
# (the timer and POST /watchdog/run can overlap).
LOCK_KEY = 128_000_128

BULK_PREFIX = "bulk:"
JOB_PREFIX = "job:"

FREQUENCY_HOURS = {
    "hourly": 1,
    "daily": 24,
    "weekly": 7 * 24,
    "monthly": 31 * 24,
    "quarterly": 92 * 24,
}

SEVERITY_RANK = {"warning": 1, "critical": 2}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


@dataclass
class Finding:
    key: str
    rule: str
    severity: str  # warning | critical
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Cadence
# =============================================================================


def cron_cadence_hours(expr: str) -> Optional[float]:
    """Largest gap between consecutive fires of ``expr`` over a year, in hours.

    Uses the same CronTrigger APScheduler fires on. The largest gap, not the
    typical one, so a weekday-only cron is not "stalled" every Monday.
    """
    try:
        from apscheduler.triggers.cron import CronTrigger

        trigger = CronTrigger.from_crontab(expr, timezone="UTC")
        anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)
        horizon = anchor + timedelta(days=400)
        fires: List[datetime] = []
        prev = None
        cursor = anchor
        while len(fires) < 13:
            nxt = trigger.get_next_fire_time(prev, cursor)
            if nxt is None or nxt > horizon:
                break
            fires.append(nxt)
            prev, cursor = nxt, nxt + timedelta(seconds=1)
        if len(fires) < 2:
            return None
        gaps = [(b - a).total_seconds() / 3600 for a, b in zip(fires, fires[1:])]
        return round(max(gaps), 2)
    except Exception:
        return None


def schedule_cadence_hours(schedule) -> Optional[float]:
    """Expected hours between runs of an ``IngestionSchedule`` (or a row with
    ``frequency`` / ``cron_expression``). ``None`` when it cannot be known."""
    freq = getattr(schedule, "frequency", None)
    freq = getattr(freq, "value", freq)
    freq = str(freq).lower() if freq else None
    cron = getattr(schedule, "cron_expression", None)

    if freq == "custom":
        # Mirrors scheduler_service._get_trigger_for_schedule: custom without a
        # cron falls back to daily.
        return cron_cadence_hours(cron) if cron else 24
    return FREQUENCY_HOURS.get(freq)


# =============================================================================
# Rules
# =============================================================================


def _max_ts(*values) -> Optional[datetime]:
    present = [v for v in values if v is not None]
    return max(present) if present else None


def _has_source_release(db: Session) -> bool:
    return db.execute(text("SELECT to_regclass('raw.source_release')")).scalar() is not None


def rule_worker(db: Session, now: datetime) -> List[Finding]:
    """Work is waiting and no worker has been seen for N minutes."""
    dead_minutes = _env_int("WATCHDOG_WORKER_DEAD_MINUTES", 10)
    cutoff = now - timedelta(minutes=dead_minutes)

    pending, oldest = db.execute(text(
        "SELECT COUNT(*), MIN(created_at) FROM job_queue WHERE LOWER(status) = 'pending'"
    )).one()
    if not pending or oldest is None or oldest > cutoff:
        return []

    live = db.execute(
        text("SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at >= :cutoff"),
        {"cutoff": cutoff},
    ).scalar()
    if live:
        return []

    last_seen = db.execute(text("SELECT MAX(last_seen_at) FROM worker_heartbeats")).scalar()
    waited_h = (now - oldest).total_seconds() / 3600
    return [Finding(
        key="worker:none_alive",
        rule="worker",
        severity="critical",
        message=(
            f"No live worker for {dead_minutes}+ min while {pending} job(s) wait in the "
            f"queue (oldest {waited_h:.1f}h). Start it: docker-compose up -d worker"
        ),
        details={
            "pending_jobs": int(pending),
            "oldest_pending_at": oldest.isoformat(),
            "last_heartbeat_at": last_seen.isoformat() if last_seen else None,
        },
    )]


def last_success_for_schedule(db: Session, schedule_id: int, source: str,
                              has_release_table: bool) -> Optional[datetime]:
    """Newest evidence that this schedule's work succeeded. Success rows only."""
    params = {"id": schedule_id, "source": source}
    ingestion = db.execute(text(
        "SELECT MAX(completed_at) FROM ingestion_jobs "
        "WHERE (schedule_id = :id OR source = :source) AND LOWER(status) = 'success'"
    ), params).scalar()
    # Queue rows linked to this schedule's IngestionJobs: the worker may finish
    # a job without writing the IngestionJob back (fixed separately, SPEC_121).
    linked = db.execute(text(
        "SELECT MAX(q.completed_at) FROM job_queue q "
        "JOIN ingestion_jobs j ON j.id = q.job_table_id "
        "WHERE (j.schedule_id = :id OR j.source = :source) AND LOWER(q.status) = 'success'"
    ), params).scalar()

    extra: List[Optional[datetime]] = []
    if source.startswith(BULK_PREFIX):
        name = source[len(BULK_PREFIX):]
        extra.append(db.execute(text(
            "SELECT MAX(completed_at) FROM job_queue WHERE job_type = 'bulk_ingest' "
            "AND LOWER(status) = 'success' AND payload->>'bulk_source' = :name"
        ), {"name": name}).scalar())
        if has_release_table:
            extra.append(db.execute(text(
                "SELECT MAX(loaded_at) FROM raw.source_release "
                "WHERE source = :name AND status = 'loaded'"
            ), {"name": name}).scalar())
    elif source.startswith(JOB_PREFIX):
        extra.append(db.execute(text(
            "SELECT MAX(completed_at) FROM job_queue "
            "WHERE job_type = :t AND LOWER(status) = 'success'"
        ), {"t": source[len(JOB_PREFIX):]}).scalar())

    return _max_ts(ingestion, linked, *extra)


def rule_stalled_schedules(db: Session, now: datetime) -> List[Finding]:
    """Active schedules whose last success is older than 1.5x their cadence."""
    rows = db.execute(text(
        "SELECT id, name, source, frequency, cron_expression, created_at "
        "FROM ingestion_schedules WHERE is_active = 1 ORDER BY id"
    )).mappings().all()
    if not rows:
        return []
    has_release = _has_source_release(db)

    findings = []
    for row in rows:
        cadence = schedule_cadence_hours(_Row(row))
        if not cadence:
            continue
        threshold = cadence * STALL_FACTOR
        last = last_success_for_schedule(db, row["id"], row["source"], has_release)
        since = last or row["created_at"]
        if since is None:
            continue
        age_h = (now - since).total_seconds() / 3600
        if age_h <= threshold:
            continue
        if last is None:
            message = (f"Schedule '{row['name']}' ({row['source']}) has never succeeded "
                       f"(active {age_h / 24:.1f} days, expected every {cadence:.0f}h)")
        else:
            message = (f"Schedule '{row['name']}' ({row['source']}) has not succeeded in "
                       f"{age_h:.1f}h (expected every {cadence:.0f}h)")
        findings.append(Finding(
            key=f"schedule:stalled:{row['id']}",
            rule="stalled_schedule",
            severity="critical",
            message=message,
            details={
                "schedule_id": row["id"],
                "name": row["name"],
                "source": row["source"],
                "cadence_hours": cadence,
                "threshold_hours": round(threshold, 1),
                "last_success_at": last.isoformat() if last else None,
                "age_hours": round(age_h, 1),
            },
        ))
    return findings


def rule_failed_releases(db: Session, now: datetime) -> List[Finding]:
    """raw.source_release rows that failed in the last 24h, one alert per source."""
    if not _has_source_release(db):
        return []
    rows = db.execute(text(
        "SELECT source, release_key, LEFT(COALESCE(error, ''), 300) AS error "
        "FROM raw.source_release WHERE status = 'failed' AND updated_at >= :since "
        "ORDER BY source, updated_at DESC"
    ), {"since": now - timedelta(hours=24)}).mappings().all()

    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(dict(r))

    findings = []
    for source, items in by_source.items():
        keys = [i["release_key"] for i in items]
        last_error = items[0]["error"] or None
        findings.append(Finding(
            key=f"release:failed:{source}",
            rule="failed_release",
            severity="warning",
            message=(f"{len(keys)} {source} release(s) failed in the last 24h: "
                     f"{', '.join(keys[:5])}{' ...' if len(keys) > 5 else ''}"
                     + (f" -- {last_error[:160]}" if last_error else "")),
            details={"source": source, "release_keys": keys, "last_error": last_error},
        ))
    return findings


def rule_failure_spike(db: Session, now: datetime) -> List[Finding]:
    """Many failed queue jobs in the last hour."""
    threshold = _env_int("WATCHDOG_FAILURE_SPIKE", 5)
    rows = db.execute(text(
        "SELECT job_type, COUNT(*) FROM job_queue "
        "WHERE LOWER(status) = 'failed' AND completed_at >= :since GROUP BY job_type"
    ), {"since": now - timedelta(hours=1)}).all()
    by_type = {str(r[0]): int(r[1]) for r in rows}
    total = sum(by_type.values())
    if total < threshold:
        return []
    return [Finding(
        key="queue:failure_spike",
        rule="failure_spike",
        severity="warning",
        message=(f"{total} queue jobs failed in the last hour (threshold {threshold}): "
                 + ", ".join(f"{k}={v}" for k, v in sorted(by_type.items(), key=lambda kv: -kv[1]))),
        details={"failed_last_hour": total, "by_job_type": by_type, "threshold": threshold},
    )]


def rule_sla(db: Session, now: datetime) -> List[Finding]:
    """source_freshness_sla violations (replaces freshness.check_freshness_violations,
    which had no caller). Successful jobs only; never succeeded is critical."""
    slas = db.execute(text(
        "SELECT source, max_age_hours FROM source_freshness_sla WHERE alert_on_violation = 1"
    )).all()
    if not slas:
        return []
    sources = [s[0] for s in slas]
    last = dict(db.execute(text(
        "SELECT source, MAX(completed_at) FROM ingestion_jobs "
        "WHERE LOWER(status) = 'success' AND source = ANY(:sources) GROUP BY source"
    ), {"sources": sources}).all())

    findings = []
    for source, max_age in slas:
        last_at = last.get(source)
        if last_at is None:
            findings.append(Finding(
                key=f"sla:{source}", rule="sla", severity="critical",
                message=f"Source '{source}' has an SLA of {max_age:g}h and has never succeeded",
                details={"source": source, "max_age_hours": max_age,
                         "last_success_at": None, "age_hours": None},
            ))
            continue
        age_h = round((now - last_at).total_seconds() / 3600, 1)
        if age_h > max_age:
            findings.append(Finding(
                key=f"sla:{source}", rule="sla", severity="warning",
                message=f"Source '{source}' is {age_h}h old (SLA {max_age:g}h)",
                details={"source": source, "max_age_hours": max_age,
                         "last_success_at": last_at.isoformat(), "age_hours": age_h},
            ))
    return findings


class _Row:
    """Attribute access over a mapping row, for schedule_cadence_hours."""

    def __init__(self, mapping):
        self._m = mapping

    def __getattr__(self, name):
        return self._m.get(name)


RULES: List[Tuple[str, Callable[[Session, datetime], List[Finding]]]] = [
    ("worker", rule_worker),
    ("stalled_schedule", rule_stalled_schedules),
    ("failed_release", rule_failed_releases),
    ("failure_spike", rule_failure_spike),
    ("sla", rule_sla),
]


def evaluate(db: Session, now: Optional[datetime] = None) -> List[Finding]:
    """Run every rule. A crashing rule becomes its own finding, never a silent gap."""
    now = now or datetime.utcnow()
    findings: List[Finding] = []
    for name, rule in RULES:
        try:
            with db.begin_nested():  # a failed statement must not poison the others
                findings.extend(rule(db, now))
        except Exception as e:
            logger.error(f"Watchdog rule {name} failed: {e}", exc_info=True)
            findings.append(Finding(
                key=f"watchdog:rule_error:{name}",
                rule="rule_error",
                severity="warning",
                message=f"Watchdog check '{name}' could not run ({type(e).__name__}); see API logs",
                details={"rule": name, "error_type": type(e).__name__},
            ))
    return findings


# =============================================================================
# State
# =============================================================================


def reconcile(db: Session, findings: List[Finding], now: datetime) -> List[Tuple[str, Finding, WatchdogAlert]]:
    """Update ``watchdog_alerts`` and return what should be said this run.

    Actions: ``opened`` (new, reopened, or never delivered), ``escalated``
    (warning -> critical), ``reminded`` (still open after 24h), ``resolved``.
    """
    changes: List[Tuple[str, Finding, WatchdogAlert]] = []
    seen = set()

    for f in findings:
        if f.key in seen:
            continue
        seen.add(f.key)
        row = db.get(WatchdogAlert, f.key)
        action: Optional[str] = None
        if row is None:
            row = WatchdogAlert(key=f.key, rule=f.rule, severity=f.severity, status="open",
                                message=f.message, details=f.details, first_seen_at=now,
                                last_seen_at=now, notify_count=0)
            db.add(row)
            action = "opened"
        elif row.status != "open":
            row.status = "open"
            row.first_seen_at = now
            row.resolved_at = None
            row.last_notified_at = None
            action = "opened"
        elif row.last_notified_at is None:
            action = "opened" if not row.notify_count else "reminded"
        elif SEVERITY_RANK.get(f.severity, 0) > SEVERITY_RANK.get(row.severity, 0):
            action = "escalated"
        elif now - row.last_notified_at >= RENOTIFY_AFTER:
            action = "reminded"

        row.rule = f.rule
        row.severity = f.severity
        row.message = f.message
        row.details = f.details
        row.last_seen_at = now
        if action:
            changes.append((action, f, row))

    open_rows = db.query(WatchdogAlert).filter(WatchdogAlert.status == "open").all()
    for row in open_rows:
        if row.key in seen:
            continue
        row.status = "resolved"
        row.resolved_at = now
        f = Finding(key=row.key, rule=row.rule, severity=row.severity,
                    message=row.message, details=row.details or {})
        changes.append(("resolved", f, row))

    db.flush()
    return changes


def open_alerts(db: Session) -> List[Dict[str, Any]]:
    rows = db.query(WatchdogAlert).filter(WatchdogAlert.status == "open").all()
    rows.sort(key=lambda r: (-SEVERITY_RANK.get(r.severity, 0), r.first_seen_at or datetime.min))

    def iso(v):
        return v.isoformat() if v else None

    return [
        {
            "key": r.key,
            "rule": r.rule,
            "severity": r.severity,
            "message": r.message,
            "details": r.details,
            "first_seen_at": iso(r.first_seen_at),
            "last_seen_at": iso(r.last_seen_at),
            "last_notified_at": iso(r.last_notified_at),
            "notify_count": r.notify_count,
        }
        for r in rows
    ]


# =============================================================================
# Delivery
# =============================================================================


def _format_line(action: str, f: Finding) -> str:
    sev = f.severity.upper()
    if action == "resolved":
        return f"[RESOLVED] {f.message} (`{f.key}`)"
    if action == "reminded":
        return f"[{sev}] still open: {f.message} (`{f.key}`)"
    if action == "escalated":
        return f"[{sev}] escalated: {f.message} (`{f.key}`)"
    return f"[{sev}] {f.message} (`{f.key}`)"


def format_message(changes: List[Tuple[str, Finding]]) -> str:
    lines = [f"Nexdata data watchdog: {len(changes)} change(s)"]
    lines += [_format_line(a, f) for a, f in changes]
    return "\n".join(lines)


async def _post_json(url: str, body: Dict[str, Any], transport=None) -> None:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS, transport=transport) as client:
        resp = await client.post(url, json=body)
        resp.raise_for_status()


async def deliver(changes: List[Tuple[str, Finding]], transport=None) -> bool:
    """Log every change, then POST one Slack-compatible message if configured.

    Returns True when the message reached the webhook, or when no webhook is
    configured (the log is then the delivery). Never raises.
    """
    if not changes:
        return True
    for action, f in changes:
        line = _format_line(action, f)
        if action == "resolved":
            logger.warning(f"WATCHDOG {line}")
        elif f.severity == "critical":
            logger.error(f"WATCHDOG {line}")
        else:
            logger.warning(f"WATCHDOG {line}")

    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return True
    try:
        await _post_json(url, {"text": format_message(changes)}, transport=transport)
        return True
    except Exception as e:
        # The URL carries the webhook secret: never log it.
        logger.warning(f"Watchdog webhook delivery failed ({type(e).__name__}); will retry next run")
        return False


async def ping_heartbeat(transport=None) -> bool:
    """Ping the external dead-man's switch (healthchecks.io style). No-op when unset."""
    url = os.getenv("HEARTBEAT_PING_URL", "").strip()
    if not url:
        return False
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS, transport=transport) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(f"Watchdog heartbeat ping failed ({type(e).__name__})")
        return False


async def _fanout_to_subscribers(f: Finding) -> None:
    """Newly opened staleness alerts also go to registered webhook subscribers
    of ALERT_DATA_STALENESS (what check_freshness_violations used to do)."""
    try:
        from app.core.models import WebhookEventType
        from app.core.webhook_service import trigger_webhooks

        await trigger_webhooks(
            event_type=WebhookEventType.ALERT_DATA_STALENESS,
            event_data={"alert_key": f.key, "severity": f.severity,
                        "message": f.message, **(f.details or {})},
            source=(f.details or {}).get("source"),
        )
    except Exception as e:
        logger.warning(f"Watchdog subscriber fan-out failed for {f.key}: {type(e).__name__}")


# =============================================================================
# Run
# =============================================================================

_last_run: Dict[str, Any] = {}
_last_failure_notice_at: Optional[datetime] = None


def last_run() -> Dict[str, Any]:
    return dict(_last_run)


async def run_watchdog(db: Session, now: Optional[datetime] = None, transport=None) -> Dict[str, Any]:
    """Evaluate, reconcile state, deliver, ping. One run at a time."""
    global _last_run
    now = now or datetime.utcnow()

    got = db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": LOCK_KEY}).scalar()
    if not got:
        db.rollback()
        return {"skipped": True, "reason": "another watchdog run is in progress"}

    findings = evaluate(db, now)
    changes = reconcile(db, findings, now)

    delivered = await deliver([(a, f) for a, f, _ in changes], transport=transport)
    if delivered:
        for action, _, row in changes:
            if action != "resolved":
                row.last_notified_at = now
                row.notify_count = (row.notify_count or 0) + 1
    for action, f, _ in changes:
        if action == "opened" and f.rule in ("stalled_schedule", "sla"):
            await _fanout_to_subscribers(f)
    db.commit()

    pinged = await ping_heartbeat(transport=transport)

    def keys(action):
        return [f.key for a, f, _ in changes if a == action]

    result = {
        "skipped": False,
        "ran_at": now.isoformat(),
        "findings": sorted(f.key for f in findings),
        "opened": keys("opened"),
        "escalated": keys("escalated"),
        "reminded": keys("reminded"),
        "resolved": keys("resolved"),
        "delivered": delivered,
        "pinged": pinged,
    }
    _last_run = result
    return result


async def run_watchdog_job() -> None:
    """APScheduler entry point. On failure the heartbeat is *not* pinged, so the
    external dead-man's switch fires; the webhook is tried once per 24h."""
    global _last_failure_notice_at
    from app.core.database import get_session_factory

    db = None
    try:
        db = get_session_factory()()
        await run_watchdog(db)
    except Exception as e:
        logger.error(f"Data watchdog run failed: {type(e).__name__}: {e}", exc_info=True)
        now = datetime.utcnow()
        if _last_failure_notice_at is None or now - _last_failure_notice_at >= RENOTIFY_AFTER:
            ok = await deliver([("opened", Finding(
                key="watchdog:run_failed", rule="watchdog", severity="critical",
                message=f"The data watchdog itself failed ({type(e).__name__}); "
                        "the database may be unreachable",
            ))])
            if ok:
                _last_failure_notice_at = now
    finally:
        if db is not None:
            db.close()


def register_watchdog_job(interval_minutes: Optional[int] = None, scheduler=None) -> bool:
    """Register the watchdog on the app scheduler (every 15 min by default)."""
    try:
        from apscheduler.triggers.interval import IntervalTrigger

        if scheduler is None:
            from app.core.scheduler_service import get_scheduler
            scheduler = get_scheduler()
        minutes = interval_minutes or _env_int("WATCHDOG_INTERVAL_MINUTES", DEFAULT_INTERVAL_MINUTES)
        scheduler.add_job(
            run_watchdog_job,
            trigger=IntervalTrigger(minutes=minutes),
            id=JOB_ID,
            name="Data Watchdog",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            # First run shortly after startup, so the heartbeat starts right away.
            next_run_time=datetime.now(timezone.utc) + timedelta(minutes=2),
        )
        logger.info(f"Registered data watchdog to run every {minutes} minutes")
        return True
    except Exception as e:
        logger.error(f"Failed to register data watchdog: {e}")
        return False
