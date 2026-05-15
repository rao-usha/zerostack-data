"""
Playground Admin — leads pipeline for enterprise sales.  PLAN_063 / SPEC_057.

JWT-gated (registered in main.py with `dependencies=_auth`). For the thin MVP
this is the sales surface: the `/leads/export.csv` endpoint feeds a CRM until
the dedicated `leads.html` admin page ships in the fast-follow plan.

Endpoints:
  GET /playground-admin/leads             — filtered lead list
  GET /playground-admin/leads/{id}        — one lead + its recent runs
  GET /playground-admin/leads/export.csv  — all leads as CSV
"""

import csv
import io
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/playground-admin", tags=["Playground Admin"])

_LEAD_COLUMNS = [
    "id", "email", "company_domain", "company_name", "is_corporate_email",
    "signup_source", "total_runs", "verified", "intent_score", "intent_tier",
    "max_n_requested", "first_seen_at", "last_active_at", "routed_to_sales_at",
]


def _lead_row_to_dict(row) -> dict:
    d = {col: row[i] for i, col in enumerate(_LEAD_COLUMNS)}
    for ts in ("first_seen_at", "last_active_at", "routed_to_sales_at"):
        if d.get(ts) is not None and hasattr(d[ts], "isoformat"):
            d[ts] = d[ts].isoformat()
    return d


@router.get("/leads")
def list_leads(
    tier: Optional[str] = Query(None, description="Filter by intent_tier (hot/warm/cold)"),
    min_score: Optional[int] = Query(None, description="Minimum intent_score"),
    since_days: Optional[int] = Query(None, description="Active within the last N days"),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """Filtered lead list, most recently active first."""
    clauses = []
    params: dict = {"limit": limit}
    if tier:
        clauses.append("intent_tier = :tier")
        params["tier"] = tier
    if min_score is not None:
        clauses.append("intent_score >= :min_score")
        params["min_score"] = min_score
    if since_days is not None:
        clauses.append("last_active_at >= :since")
        params["since"] = datetime.utcnow() - timedelta(days=since_days)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    try:
        rows = db.execute(
            text(f"SELECT {', '.join(_LEAD_COLUMNS)} FROM leads{where} "
                 f"ORDER BY last_active_at DESC LIMIT :limit"),
            params,
        ).fetchall()
    except Exception as exc:  # noqa: BLE001 — leads table may not exist yet
        logger.warning("lead list query failed: %s", exc)
        db.rollback()
        return {"leads": [], "total": 0}

    leads = [_lead_row_to_dict(r) for r in rows]
    return {"leads": leads, "total": len(leads)}


@router.get("/leads/export.csv")
def export_leads_csv(db: Session = Depends(get_db)):
    """All leads as CSV — the MVP sales surface (import into a CRM)."""
    try:
        rows = db.execute(
            text(f"SELECT {', '.join(_LEAD_COLUMNS)} FROM leads "
                 f"ORDER BY intent_score DESC, last_active_at DESC")
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.warning("lead CSV export query failed: %s", exc)
        db.rollback()
        rows = []

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_LEAD_COLUMNS)
    for r in rows:
        d = _lead_row_to_dict(r)
        writer.writerow([d[c] for c in _LEAD_COLUMNS])
    buf.seek(0)

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=playground_leads.csv"},
    )


@router.get("/leads/{lead_id}")
def get_lead_detail(lead_id: int, db: Session = Depends(get_db)):
    """A single lead plus its recent playground runs."""
    row = db.execute(
        text(f"SELECT {', '.join(_LEAD_COLUMNS)} FROM leads WHERE id = :id"),
        {"id": lead_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead = _lead_row_to_dict(row)

    run_rows = db.execute(
        text("""
        SELECT generator, n_requested, report_id, created_at
        FROM playground_runs
        WHERE lead_id = :id
        ORDER BY created_at DESC
        LIMIT 50
    """),
        {"id": lead_id},
    ).fetchall()
    lead["recent_runs"] = [
        {
            "generator": rr[0],
            "n_requested": rr[1],
            "report_id": rr[2],
            "created_at": rr[3].isoformat() if rr[3] else None,
        }
        for rr in run_rows
    ]
    return lead
