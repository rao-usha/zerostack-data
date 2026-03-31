"""
Source directory endpoints.

GET /sources                — list all 36 data sources with live status & row counts
GET /sources/health-summary — aggregated health view for Sources page redesign (PLAN_049)
GET /sources/{key}          — detailed view for a single source (tables, recent jobs, schedule)
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.domains import DOMAIN_LABELS
from app.core.source_registry import SOURCE_REGISTRY, get_source, get_all_sources
from app.core.api_registry import API_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sources", tags=["sources"])


# ---------------------------------------------------------------------------
# Health-summary constants (PLAN_049)
# ---------------------------------------------------------------------------

# Maps source registry keys to their domain lane
DOMAIN_MAP: Dict[str, List[str]] = {
    "macro_economic": [
        "fred", "bls", "bea", "census", "international_econ",
    ],
    "pe_intelligence": [
        "sec", "form_d", "form_adv",
        "job_postings",       # hiring signals → deal sourcing leading indicator
        "opencorporates",     # company registry
    ],
    "people_orgs": [
        "glassdoor",          # executive/employee intelligence
        "github",             # tech org intelligence
        "nppes",              # healthcare provider registry
        "app_rankings",       # consumer app performance
    ],
    "site_intelligence": [
        "eia", "noaa", "osha", "epa_echo", "fema",
        "location_diligence", "realestate", "foot_traffic",
    ],
    "regulatory": [
        "fdic", "fbi_crime", "treasury", "courtlistener",
        "sam_gov", "irs_soi", "usda", "fcc_broadband", "cms",
    ],
}

DOMAIN_LABELS_HEALTH: Dict[str, str] = {
    "macro_economic": "Macro Economic",
    "pe_intelligence": "PE Intelligence",
    "people_orgs": "People & Orgs",
    "site_intelligence": "Site Intelligence",
    "regulatory": "Regulatory",
}

# Expected refresh cadence in hours per source key (default: 168 = weekly)
EXPECTED_CADENCE_HOURS: Dict[str, int] = {
    "fred": 24,
    "bls": 48,
    "bea": 8760,          # annual
    "census_batch": 8760,
    "census_geo": 8760,
    "sec": 72,
    "sec_form_d": 168,
    "afdc": 168,
    "eia": 24,
    "noaa": 168,
    "fdic": 720,
    "fbi_crime": 8760,
    "epa_echo": 720,
    "osha": 720,
    "usaspending": 168,
    "people_jobs": 24,
    "lp_collection": 168,
    "fo_collection": 168,
    "form_adv": 720,
    "pe_collection": 168,
}


# ---------------------------------------------------------------------------
# Helpers — dynamic DB queries
# ---------------------------------------------------------------------------

def _safe_query(db: Session, sql: str, params: dict) -> List[Any]:
    """Execute a parameterized query; return [] if table doesn't exist yet."""
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        db.rollback()
        msg = str(exc).lower()
        if "does not exist" in msg or "no such table" in msg or "undefined" in msg:
            return []
        logger.warning("_safe_query failed: %s — %s", sql[:100], exc)
        return []


def _source_statuses(db: Session) -> Dict[str, Dict[str, Any]]:
    """
    Return per-source status summary from ingestion_jobs.

    One query, grouped by source — gives:
      - status: running | failed | idle | never_run
      - last successful job id, completed_at, rows_inserted
    """
    sql = text("""
        SELECT
            source,
            MAX(CASE WHEN status = 'running' THEN 1 ELSE 0 END)  AS has_running,
            MAX(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END)  AS has_failed,
            MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END)  AS has_success,
            (SELECT id FROM ingestion_jobs j2
             WHERE j2.source = ingestion_jobs.source AND j2.status = 'success'
             ORDER BY j2.completed_at DESC LIMIT 1)               AS last_job_id,
            (SELECT completed_at FROM ingestion_jobs j3
             WHERE j3.source = ingestion_jobs.source AND j3.status = 'success'
             ORDER BY j3.completed_at DESC LIMIT 1)               AS last_completed,
            (SELECT rows_inserted FROM ingestion_jobs j4
             WHERE j4.source = ingestion_jobs.source AND j4.status = 'success'
             ORDER BY j4.completed_at DESC LIMIT 1)               AS last_rows
        FROM ingestion_jobs
        GROUP BY source
    """)
    try:
        rows = db.execute(sql).mappings().all()
    except Exception:
        logger.warning("Failed to query ingestion_jobs for source statuses", exc_info=True)
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if r["has_running"]:
            status = "running"
        elif r["has_failed"] and not r["has_success"]:
            status = "failed"
        elif r["has_success"]:
            status = "idle"
        else:
            status = "never_run"

        last_run = None
        if r["last_job_id"]:
            last_run = {
                "job_id": r["last_job_id"],
                "completed_at": r["last_completed"].isoformat() if r["last_completed"] else None,
                "rows_inserted": r["last_rows"],
            }

        result[r["source"]] = {"status": status, "last_run": last_run}
    return result


def _table_stats(db: Session) -> Dict[str, Dict[str, int]]:
    """
    Return per-prefix table count and estimated row count.

    Uses pg_stat_user_tables (instant, no COUNT(*) scan).
    """
    sql = text("""
        SELECT relname, n_live_tup
        FROM pg_stat_user_tables
        ORDER BY relname
    """)
    try:
        rows = db.execute(sql).mappings().all()
    except Exception:
        logger.warning("Failed to query pg_stat_user_tables", exc_info=True)
        return {}

    # Build a lookup: source_key -> {table_count, total_rows, tables}
    # Match by source key prefix (key="fred" matches fred_interest_rates, fred_housing_market, etc.)
    # n_live_tup may be 0 after bulk inserts until PostgreSQL runs ANALYZE — row counts
    # are supplemented by rows_inserted from ingestion_jobs in get_health_summary().
    stats: Dict[str, Dict[str, Any]] = {}
    for src in SOURCE_REGISTRY.values():
        prefix = src.key + "_"
        count = 0
        total = 0
        tables: List[Dict[str, Any]] = []
        for r in rows:
            if r["relname"].startswith(prefix) or r["relname"] == src.key:
                count += 1
                total += int(r["n_live_tup"] or 0)
                tables.append({"name": r["relname"], "estimated_rows": int(r["n_live_tup"] or 0)})
        stats[src.key] = {"table_count": count, "total_rows": total, "tables": tables}
    return stats


def _recent_jobs(db: Session, source: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Return the N most recent ingestion jobs for a source."""
    sql = text("""
        SELECT id, status, created_at, started_at, completed_at,
               rows_inserted, error_message
        FROM ingestion_jobs
        WHERE source = :source
        ORDER BY created_at DESC
        LIMIT :limit
    """)
    try:
        rows = db.execute(sql, {"source": source, "limit": limit}).mappings().all()
    except Exception:
        logger.warning("Failed to query recent jobs for %s", source, exc_info=True)
        return []
    return [
        {
            "job_id": r["id"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "started_at": r["started_at"].isoformat() if r["started_at"] else None,
            "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
            "rows_inserted": r["rows_inserted"],
            "error_message": r["error_message"],
        }
        for r in rows
    ]


def _schedule_info(db: Session, source: str) -> Optional[Dict[str, Any]]:
    """Return active schedule for a source, if any."""
    sql = text("""
        SELECT id, name, frequency, cron_expression, is_active,
               last_run_at, next_run_at
        FROM ingestion_schedules
        WHERE source = :source AND is_active = 1
        ORDER BY id
        LIMIT 1
    """)
    try:
        row = db.execute(sql, {"source": source}).mappings().first()
    except Exception:
        logger.warning("Failed to query schedule for %s", source, exc_info=True)
        return None
    if not row:
        return None
    return {
        "schedule_id": row["id"],
        "name": row["name"],
        "frequency": row["frequency"],
        "cron_expression": row["cron_expression"],
        "is_active": row["is_active"],
        "last_run_at": row["last_run_at"].isoformat() if row["last_run_at"] else None,
        "next_run_at": row["next_run_at"].isoformat() if row["next_run_at"] else None,
    }


# ---------------------------------------------------------------------------
# Health-summary helpers
# ---------------------------------------------------------------------------

def _health_job_info(db: Session, source_key: str) -> Dict[str, Any]:
    """
    Return the last two successful jobs for a source so we can compute
    row trend and age.  Falls back to empty dict gracefully.
    """
    rows = _safe_query(
        db,
        """
        SELECT id, status, completed_at, rows_inserted
        FROM ingestion_jobs
        WHERE source = :src AND status = 'success'
        ORDER BY completed_at DESC
        LIMIT 2
        """,
        {"src": source_key},
    )
    if not rows:
        return {"last_run_at": None, "rows_inserted": None, "prev_rows_inserted": None}

    latest = rows[0]
    prev = rows[1] if len(rows) > 1 else None

    return {
        "last_run_at": latest.completed_at.isoformat() if latest.completed_at else None,
        "rows_inserted": int(latest.rows_inserted) if latest.rows_inserted is not None else None,
        "prev_rows_inserted": int(prev.rows_inserted) if prev and prev.rows_inserted is not None else None,
    }


def _health_failed_job(db: Session, source_key: str) -> bool:
    """Return True if the most recent job for this source has status='failed'."""
    rows = _safe_query(
        db,
        """
        SELECT status FROM ingestion_jobs
        WHERE source = :src
        ORDER BY created_at DESC
        LIMIT 1
        """,
        {"src": source_key},
    )
    return bool(rows and rows[0].status == "failed")


def _health_quality_scores(db: Session, source_key: str) -> Dict[str, Any]:
    """
    Return the latest DQ quality snapshot for a source.
    Returns empty dict if dq_quality_snapshots doesn't exist.
    """
    rows = _safe_query(
        db,
        """
        SELECT quality_score, completeness_score, freshness_score,
               validity_score, consistency_score
        FROM dq_quality_snapshots
        WHERE source = :src
        ORDER BY snapshot_at DESC
        LIMIT 1
        """,
        {"src": source_key},
    )
    if not rows:
        return {}
    r = rows[0]
    return {
        "quality_score": int(r.quality_score) if r.quality_score is not None else None,
        "quality_breakdown": {
            "completeness": int(r.completeness_score) if r.completeness_score is not None else None,
            "freshness": int(r.freshness_score) if r.freshness_score is not None else None,
            "validity": int(r.validity_score) if r.validity_score is not None else None,
            "consistency": int(r.consistency_score) if r.consistency_score is not None else None,
        },
    }


def _health_schedule(db: Session, source_key: str) -> Dict[str, Any]:
    """Return next_run_at and frequency for a source schedule."""
    rows = _safe_query(
        db,
        """
        SELECT next_run_at, frequency
        FROM ingestion_schedules
        WHERE source = :src AND is_active = 1
        ORDER BY id
        LIMIT 1
        """,
        {"src": source_key},
    )
    if not rows:
        return {"next_run_at": None, "frequency": None}
    r = rows[0]
    return {
        "next_run_at": r.next_run_at.isoformat() if r.next_run_at else None,
        "frequency": r.frequency,
    }


def _health_open_anomaly_count(db: Session, source_key: str) -> int:
    """Return count of open anomaly alerts for a source table prefix."""
    # We match on table_name LIKE 'source_prefix_%' — use the source key as prefix
    rows = _safe_query(
        db,
        """
        SELECT COUNT(*) AS cnt
        FROM dq_anomaly_alerts
        WHERE status = 'open'
          AND table_name LIKE :prefix
        """,
        {"prefix": f"{source_key}%"},
    )
    return int(rows[0][0]) if rows else 0


def _health_top_anomalies(db: Session, limit: int = 5) -> List[Dict]:
    """Return top open anomaly alerts across all sources."""
    rows = _safe_query(
        db,
        """
        SELECT id, alert_type, table_name, severity, message, detected_at
        FROM dq_anomaly_alerts
        WHERE status = 'open'
        ORDER BY detected_at DESC
        LIMIT :lim
        """,
        {"lim": limit},
    )
    result = []
    for r in rows:
        result.append({
            "id": r.id,
            "alert_type": r.alert_type,
            "table_name": r.table_name,
            "severity": r.severity,
            "message": r.message,
            "detected_at": r.detected_at.isoformat() if r.detected_at else None,
        })
    return result


def _health_top_recommendations(db: Session, limit: int = 5) -> List[Dict]:
    """Return top HIGH-priority open recommendations."""
    rows = _safe_query(
        db,
        """
        SELECT id, priority, source, category, message, fix_action
        FROM dq_recommendations
        WHERE status = 'open'
        ORDER BY
            CASE priority WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            created_at DESC
        LIMIT :lim
        """,
        {"lim": limit},
    )
    result = []
    for r in rows:
        result.append({
            "id": r.id,
            "priority": r.priority,
            "source": r.source,
            "category": r.category,
            "message": r.message,
            "fix_action": r.fix_action,
        })
    return result


def _row_trend(current: Optional[int], prev: Optional[int]) -> str:
    """Compare two successive row counts and return 'up' / 'stable' / 'down'."""
    if current is None or prev is None or prev == 0:
        return "stable"
    pct = (current - prev) / abs(prev) * 100
    if pct > 5:
        return "up"
    if pct < -5:
        return "down"
    return "stable"


def _age_hours(last_run_at_iso: Optional[str]) -> Optional[float]:
    """Return hours since last_run_at, or None if not available."""
    if not last_run_at_iso:
        return None
    try:
        dt = datetime.fromisoformat(last_run_at_iso)
        # Make timezone-aware if naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        return round((now - dt).total_seconds() / 3600, 2)
    except (ValueError, TypeError):
        return None


def _sla_status(age_h: Optional[float], sla_h: int) -> str:
    """
    Return 'fresh', 'approaching', or 'stale' based on age vs SLA cadence.
    'approaching' = within 20% of SLA threshold.
    """
    if age_h is None:
        return "unknown"
    if age_h <= sla_h:
        if age_h >= sla_h * 0.8:
            return "approaching"
        return "fresh"
    return "stale"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/health-summary",
    summary="Aggregated health view for Sources page (PLAN_049)",
    description=(
        "Returns everything the Sources health view needs in one call: "
        "per-source status, quality scores, staleness, anomaly counts, "
        "and the governance panel (top anomalies + recommendations)."
    ),
)
def get_health_summary(db: Session = Depends(get_db)):
    """
    Assemble a complete health payload for the Sources page redesign.

    Queries:
    - ingestion_jobs — last two successful jobs per source (last_run_at, row trend)
    - dq_quality_snapshots — latest quality/completeness/freshness/validity/consistency
    - ingestion_schedules — next_run_at, frequency
    - dq_anomaly_alerts — open alert count per source + top 5 overall
    - dq_recommendations — top 5 HIGH-priority open recommendations

    All DB calls use _safe_query so missing tables degrade gracefully.
    """
    # -- Batch-fetch existing status/stats (already efficient in list_sources) --
    statuses = _source_statuses(db)
    table_stats = _table_stats(db)

    # -- Build per-source health cards grouped by domain --
    # Build a reverse lookup: source_key -> domain_key
    key_to_domain: Dict[str, str] = {}
    for domain_key, keys in DOMAIN_MAP.items():
        for k in keys:
            key_to_domain[k] = domain_key

    # Collect all source keys from the registry
    all_source_keys = [src.key for src in get_all_sources()]

    # Per-source health data — cache to avoid duplicate DB hits
    source_health_cache: Dict[str, Dict] = {}

    stale_count = 0
    never_run_count = 0
    failed_count = 0
    open_anomaly_total = 0
    last_activity_candidates: List[str] = []

    for src in get_all_sources():
        key = src.key
        sla_h = EXPECTED_CADENCE_HOURS.get(key, 168)

        # Job info (last 2 successful jobs)
        job_info = _health_job_info(db, key)
        last_run_at = job_info["last_run_at"]
        rows_ins = job_info["rows_inserted"]
        prev_rows = job_info["prev_rows_inserted"]

        # Status
        st = statuses.get(key, {})
        raw_status = st.get("status", "never_run")

        # Row trend
        trend = _row_trend(rows_ins, prev_rows)

        # Age + SLA
        age_h = _age_hours(last_run_at)
        sla_st = _sla_status(age_h, sla_h)
        is_stale = sla_st == "stale"

        # Quality scores (graceful if table missing)
        quality = _health_quality_scores(db, key)

        # Schedule
        schedule = _health_schedule(db, key)

        # Open anomalies for this source
        open_anomalies = _health_open_anomaly_count(db, key)
        open_anomaly_total += open_anomalies

        # Table names from stats; n_live_tup may be 0 after bulk inserts until
        # PostgreSQL ANALYZE runs — fall back to rows_inserted from last job.
        tb = table_stats.get(key, {})
        table_names = [t["name"] for t in tb.get("tables", [])]
        total_rows = tb.get("total_rows") or rows_ins or 0

        # Counters for banner
        if raw_status == "never_run":
            never_run_count += 1
        if is_stale and raw_status != "never_run":
            stale_count += 1
        if raw_status == "failed":
            failed_count += 1
        if last_run_at:
            last_activity_candidates.append(last_run_at)

        card = {
            "key": key,
            "display_name": src.display_name,
            "short_name": src.short_name,
            "status": raw_status,
            "is_stale": is_stale,
            "sla_hours": sla_h,
            "sla_status": sla_st,
            "total_rows": total_rows,
            "row_trend": trend,
            "last_run_at": last_run_at,
            "age_hours": age_h,
            "next_run_at": schedule["next_run_at"],
            "frequency": schedule["frequency"],
            "open_anomalies": open_anomalies,
            "tables": table_names,
            **quality,  # quality_score, quality_breakdown (or empty if DQ not run)
        }
        source_health_cache[key] = card

    # -- Assemble domain swim lanes --
    # Start with explicit domain groupings; append uncategorised sources at end
    domains_out: List[Dict] = []
    assigned_keys: set = set()

    for domain_key, member_keys in DOMAIN_MAP.items():
        sources_in_lane = []
        for k in member_keys:
            if k in source_health_cache:
                sources_in_lane.append(source_health_cache[k])
                assigned_keys.add(k)
            # If key isn't in registry, skip silently
        domains_out.append({
            "key": domain_key,
            "label": DOMAIN_LABELS_HEALTH.get(domain_key, domain_key),
            "sources": sources_in_lane,
        })

    # Uncategorised sources (in registry but not in DOMAIN_MAP)
    uncategorised = [
        source_health_cache[k]
        for k in all_source_keys
        if k not in assigned_keys and k in source_health_cache
    ]
    if uncategorised:
        domains_out.append({
            "key": "other",
            "label": "Other",
            "sources": uncategorised,
        })

    # -- Governance data --
    top_anomalies = _health_top_anomalies(db, limit=5)
    top_recommendations = _health_top_recommendations(db, limit=5)

    # -- Banner --
    last_activity_at = (
        max(last_activity_candidates) if last_activity_candidates else None
    )

    if failed_count > 0 or open_anomaly_total > 0:
        overall_status = "critical" if failed_count > 0 else "warning"
    elif stale_count > 0:
        overall_status = "warning"
    else:
        overall_status = "ok"

    return {
        "banner": {
            "active_count": len(all_source_keys) - never_run_count,
            "stale_count": stale_count,
            "never_run_count": never_run_count,
            "failed_count": failed_count,
            "open_anomaly_count": open_anomaly_total,
            "last_activity_at": last_activity_at,
            "overall_status": overall_status,
        },
        "domains": domains_out,
        "governance": {
            "open_anomalies": top_anomalies,
            "top_recommendations": top_recommendations,
        },
    }


@router.get(
    "",
    summary="List all data sources",
    description="Returns every registered data source with static metadata and live status (row counts, last run).",
)
def list_sources(
    category: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Directory of all 36 data sources.

    Optional query param ``category`` filters by domain key
    (e.g. ``macro_economic``, ``alt_data``).
    """
    sources = get_all_sources()
    if category:
        sources = [s for s in sources if s.category == category]

    # Batch-fetch dynamic data (2 queries total)
    statuses = _source_statuses(db)
    stats = _table_stats(db)

    items = []
    for src in sources:
        st = statuses.get(src.key, {"status": "never_run", "last_run": None})
        tb = stats.get(src.key, {"table_count": 0, "total_rows": 0})
        items.append({
            "key": src.key,
            "display_name": src.display_name,
            "short_name": src.short_name,
            "category": src.category,
            "category_label": src.category_label,
            "description": src.description,
            "update_frequency": src.update_frequency,
            "api_key_required": src.api_key_required,
            "url": src.url,
            "status": st["status"],
            "last_run": st["last_run"],
            "total_tables": tb["table_count"],
            "total_rows": tb["total_rows"],
            "tags": src.tags,
            "collection_count": len(src.collections),
        })

    # Build category summary
    cat_counts: Dict[str, int] = {}
    for src in get_all_sources():
        cat_counts[src.category] = cat_counts.get(src.category, 0) + 1
    categories = [
        {"key": k, "label": DOMAIN_LABELS.get(k, k), "count": v}
        for k, v in sorted(cat_counts.items())
    ]

    return {
        "total_sources": len(items),
        "sources": items,
        "categories": categories,
    }


@router.get(
    "/{key}",
    summary="Source detail",
    description="Detailed view for a single data source — tables, row counts, recent jobs, schedule, and API config.",
)
def get_source_detail(
    key: str,
    db: Session = Depends(get_db),
):
    """Full detail for one source, including per-table breakdown and recent jobs."""
    src = get_source(key)
    if src is None:
        raise HTTPException(status_code=404, detail=f"Unknown source: {key}")

    # Dynamic data
    statuses = _source_statuses(db)
    stats = _table_stats(db)
    st = statuses.get(src.key, {"status": "never_run", "last_run": None})
    tb = stats.get(src.key, {"table_count": 0, "total_rows": 0, "tables": []})

    jobs = _recent_jobs(db, key)
    schedule = _schedule_info(db, key)

    # API config (rate limits, key requirement)
    api_cfg = API_REGISTRY.get(key)
    api_config = None
    if api_cfg:
        api_config = {
            "base_url": api_cfg.base_url,
            "api_key_requirement": api_cfg.api_key_requirement.value,
            "signup_url": api_cfg.signup_url or None,
            "max_concurrency": api_cfg.max_concurrency,
            "rate_limit_per_minute": api_cfg.rate_limit_per_minute,
            "timeout_seconds": api_cfg.timeout_seconds,
            "notes": api_cfg.notes,
        }

    return {
        "key": src.key,
        "display_name": src.display_name,
        "short_name": src.short_name,
        "category": src.category,
        "category_label": src.category_label,
        "description": src.description,
        "update_frequency": src.update_frequency,
        "api_key_required": src.api_key_required,
        "url": src.url,
        "tags": src.tags,
        "collections": [
            {
                "name": c.name,
                "endpoint": c.endpoint,
                "description": c.description,
                "table": c.table,
            }
            for c in src.collections
        ],
        "status": st["status"],
        "last_run": st["last_run"],
        "total_tables": tb["table_count"],
        "total_rows": tb["total_rows"],
        "tables": tb.get("tables", []),
        "recent_jobs": jobs,
        "schedule": schedule,
        "api_config": api_config,
    }
