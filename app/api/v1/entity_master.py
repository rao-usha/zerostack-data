"""
Entity master endpoints (SPEC_116).

The master links organizations across SEC sources by strong identifier
(CIK, CRD, EIN, LEI). Distinct from `/api/v1/entities`, which is the older
name-similarity resolver over `canonical_entities`.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_queue_service import submit_job

router = APIRouter(prefix="/entities/master", tags=["Entity Master"])

ID_TYPES = ("cik", "crd", "ein", "lei", "uei", "sei")


@router.post("/resolve", summary="Queue an entity master refresh (admin)")
def queue_resolve(
    skip_feeds: bool = Query(False, description="Reuse core.source_record as-is"),
    skip_bridge: bool = Query(False, description="Keep the existing CIK/CRD bridge"),
    include_name_tier: bool = Query(True, description="Allow the name+state bridge tier"),
    dry_run: bool = Query(False, description="Build, gate and ledger; keep nothing"),
    input_override: Optional[List[str]] = Query(
        None, description="Input sources (or 'all') this build may use although their "
                          "latest release failed or is stale (SPEC_126a)"
    ),
    input_max_age_days: Optional[List[str]] = Query(
        None, description="Per-input max age, as source=days (SPEC_126a)"
    ),
    gate_override: Optional[List[str]] = Query(
        None, description="Ship gates (or 'all') this build may fail and still commit (SPEC_126a)"
    ),
    db: Session = Depends(get_db),
):
    # POST on this router needs admin (_auth = require_admin_for_writes, main.py),
    # so the SPEC_126a overrides are admin-only like /pe/marts/build's.
    from app.marts.inputs import override_payload

    payload = {
        "skip_feeds": skip_feeds,
        "skip_bridge": skip_bridge,
        "include_name_tier": include_name_tier,
        "dry_run": dry_run,
    }
    try:
        payload.update(override_payload(input_override, gate_override, input_max_age_days))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return submit_job(db=db, job_type="entity_resolve", payload=payload)


@router.get("/stats", summary="Entity master size and last run")
def get_stats(db: Session = Depends(get_db)):
    counts = db.execute(
        text(
            """
            SELECT (SELECT COUNT(*) FROM core.source_record) AS source_records,
                   (SELECT COUNT(*) FROM core.entity WHERE dissolved_at IS NULL) AS entities,
                   (SELECT COUNT(*) FROM core.membership) AS memberships,
                   (SELECT COUNT(*) FROM core.identifier) AS identifiers,
                   (SELECT COUNT(*) FROM core.cik_crd_bridge) AS bridge_rows,
                   (SELECT COUNT(*) FROM core.entity WHERE dissolved_at IS NULL
                      AND cik IS NOT NULL AND crd IS NOT NULL) AS entities_with_cik_and_crd
            """
        )
    ).mappings().one()
    by_source = db.execute(
        text("SELECT source, COUNT(*) AS n FROM core.source_record GROUP BY source ORDER BY source")
    ).mappings().all()
    last_run = db.execute(
        text(
            "SELECT run_at, resolver_version, dry_run, duration_seconds, metrics "
            "FROM core.resolve_run ORDER BY run_at DESC LIMIT 1"
        )
    ).mappings().first()
    return {
        "counts": dict(counts),
        "source_records_by_source": {r["source"]: r["n"] for r in by_source},
        "last_run": dict(last_run) if last_run else None,
    }


@router.get("/by-id/{id_type}/{value}", summary="Look up an entity by identifier")
def get_by_identifier(id_type: str, value: str, db: Session = Depends(get_db)):
    if id_type not in ID_TYPES:
        raise HTTPException(status_code=400, detail=f"id_type must be one of {ID_TYPES}")
    row = db.execute(
        text(
            """
            SELECT e.* FROM core.identifier i
            JOIN core.entity e ON e.entity_id = i.entity_id
            WHERE i.id_type = :t AND i.id_value = :v
            """
        ),
        {"t": id_type, "v": value.lstrip("0") if id_type in ("cik", "crd") else value},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"No entity for {id_type}={value}")
    entity = dict(row)
    entity["identifiers"] = [
        dict(r)
        for r in db.execute(
            text("SELECT id_type, id_value, sources FROM core.identifier WHERE entity_id = :e "
                 "ORDER BY id_type, id_value"),
            {"e": entity["entity_id"]},
        ).mappings()
    ]
    entity["members"] = [
        dict(r)
        for r in db.execute(
            text("SELECT record_key, match_tier, match_method FROM core.membership "
                 "WHERE entity_id = :e ORDER BY record_key"),
            {"e": entity["entity_id"]},
        ).mappings()
    ]
    entity["aliases"] = [
        r[0]
        for r in db.execute(
            text("SELECT DISTINCT name FROM core.alias WHERE entity_id = :e AND name IS NOT NULL "
                 "ORDER BY name"),
            {"e": entity["entity_id"]},
        )
    ]
    return entity
