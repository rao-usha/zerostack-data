"""
PE mart build endpoints (SPEC_117).

Populates pe_firms / pe_funds from Form ADV and Form D. Additive: SEC-derived
rows are tagged (`data_sources` contains "SEC ADV", `data_source = 'SEC Form D'`)
and hand-entered rows are never touched.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_queue_service import submit_job

router = APIRouter(prefix="/pe/marts", tags=["PE Intelligence - Marts"])


@router.post("/build", summary="Queue a PE mart rebuild from SEC data")
def queue_build(
    skip_firms: bool = Query(False),
    skip_funds: bool = Query(False),
    publish_guard_override: Optional[List[str]] = Query(
        None, description="Tables whose publish guard this one build may bypass (SPEC_129)"
    ),
    input_override: Optional[List[str]] = Query(
        None, description="Inputs (or 'all') this build may use although their latest "
                          "release failed or is stale, incl. 'entity_resolve' (SPEC_126a)"
    ),
    input_max_age_days: Optional[List[str]] = Query(
        None, description="Per-input max age, as source=days (SPEC_126a)"
    ),
    dry_run: bool = Query(False, description="Build, gate and ledger; keep nothing"),
    gate_override: Optional[List[str]] = Query(
        None, description="Ship gates (or 'all') this build may fail and still commit (SPEC_126a)"
    ),
    db: Session = Depends(get_db),
):
    from app.marts.inputs import override_payload

    payload = {"skip_firms": skip_firms, "skip_funds": skip_funds}
    if dry_run is True:  # not a bare Query()
        payload["dry_run"] = True
    if isinstance(publish_guard_override, list) and publish_guard_override:  # not a bare Query()
        payload["publish_guard_override"] = publish_guard_override
    try:
        payload.update(override_payload(input_override, gate_override, input_max_age_days))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return submit_job(db=db, job_type="pe_mart_build", payload=payload)


@router.get("/stats", summary="SEC-derived vs hand-entered PE rows")
def get_stats(db: Session = Depends(get_db)):
    firms = db.execute(
        text(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE CAST(data_sources AS TEXT) LIKE '%SEC ADV%') AS from_sec,
                   COUNT(*) FILTER (WHERE crd_number IS NOT NULL) AS with_crd,
                   COUNT(*) FILTER (WHERE cik IS NOT NULL) AS with_cik,
                   COUNT(*) FILTER (WHERE aum_usd_millions IS NOT NULL) AS with_aum
            FROM pe_firms
            """
        )
    ).mappings().one()
    funds = db.execute(
        text(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE data_source = 'SEC Form D') AS from_sec,
                   COUNT(*) FILTER (WHERE firm_id IS NOT NULL) AS attributed,
                   COUNT(*) FILTER (WHERE vintage_year IS NOT NULL) AS with_vintage
            FROM pe_funds
            """
        )
    ).mappings().one()
    by_strategy = db.execute(
        text(
            "SELECT strategy, COUNT(*) AS n FROM pe_funds WHERE data_source = 'SEC Form D' "
            "GROUP BY strategy ORDER BY n DESC"
        )
    ).mappings().all()
    return {
        "firms": dict(firms),
        "funds": dict(funds),
        "funds_by_strategy": {r["strategy"]: r["n"] for r in by_strategy},
    }
