"""
Source directory endpoints.

GET /sources      — list all 36 data sources with live status & row counts
GET /sources/{key} — detailed view for a single source (tables, recent jobs, schedule)
"""

import logging
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
# Helpers — dynamic DB queries
# ---------------------------------------------------------------------------

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
    stats: Dict[str, Dict[str, Any]] = {}
    for src in SOURCE_REGISTRY.values():
        if not src.table_prefix:
            continue
        count = 0
        total = 0
        tables: List[Dict[str, Any]] = []
        for r in rows:
            if r["relname"].startswith(src.table_prefix):
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
        WHERE source = :source AND is_active = true
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
# Endpoints
# ---------------------------------------------------------------------------

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
