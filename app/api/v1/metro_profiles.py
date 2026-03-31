"""
Metro Development Profiles API (PLAN_051 / SPEC_041)

Endpoints:
  GET  /metro-profiles/           — paginated list with scores
  GET  /metro-profiles/rankings   — sorted by build_hostility_score desc
  GET  /metro-profiles/{cbsa_code} — single metro detail
  POST /metro-profiles/ingest     — trigger background data collection job
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Metro Intelligence"])

# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

_LIST_SQL = text("""
    SELECT
        mp.cbsa_code,
        mr.cbsa_name,
        mr.metro_type,
        mr.state_abbr,
        mr.population_rank,
        mp.data_vintage,
        mp.permits_total,
        mp.permits_1unit,
        mp.permits_5plus,
        mp.permits_per_1000_units,
        mp.multifamily_share_pct,
        mp.hpi_current,
        mp.hpi_yoy_pct,
        mp.hpi_5yr_pct,
        mp.population,
        mp.median_hh_income,
        mp.housing_units_total,
        mp.cost_burden_severe_pct,
        mp.unemployment_rate,
        mp.permit_velocity_score,
        mp.multifamily_score,
        mp.supply_elasticity_score,
        mp.build_hostility_score,
        mp.build_hostility_grade,
        mp.sources_available,
        mp.data_completeness_pct,
        mp.updated_at
    FROM metro_profiles mp
    JOIN metro_reference mr ON mr.cbsa_code = mp.cbsa_code
    WHERE (:state IS NULL OR mr.state_abbr ILIKE :state_like)
      AND (:metro_type IS NULL OR mr.metro_type = :metro_type)
    ORDER BY mr.population_rank ASC NULLS LAST
    LIMIT :limit OFFSET :offset
""")

_COUNT_SQL = text("""
    SELECT COUNT(*)
    FROM metro_profiles mp
    JOIN metro_reference mr ON mr.cbsa_code = mp.cbsa_code
    WHERE (:state IS NULL OR mr.state_abbr ILIKE :state_like)
      AND (:metro_type IS NULL OR mr.metro_type = :metro_type)
""")

_RANKINGS_SQL = text("""
    SELECT
        mp.cbsa_code,
        mr.cbsa_name,
        mr.metro_type,
        mr.state_abbr,
        mr.population_rank,
        mp.data_vintage,
        mp.build_hostility_score,
        mp.build_hostility_grade,
        mp.permit_velocity_score,
        mp.multifamily_score,
        mp.supply_elasticity_score,
        mp.hpi_yoy_pct,
        mp.hpi_5yr_pct,
        mp.permits_per_1000_units,
        mp.multifamily_share_pct,
        mp.unemployment_rate,
        mp.population,
        mp.data_completeness_pct,
        mp.sources_available
    FROM metro_profiles mp
    JOIN metro_reference mr ON mr.cbsa_code = mp.cbsa_code
    WHERE mp.build_hostility_score IS NOT NULL
    ORDER BY mp.build_hostility_score DESC
    LIMIT :limit OFFSET :offset
""")

_DETAIL_SQL = text("""
    SELECT
        mp.*,
        mr.cbsa_name,
        mr.metro_type,
        mr.state_abbr,
        mr.population_rank
    FROM metro_profiles mp
    JOIN metro_reference mr ON mr.cbsa_code = mp.cbsa_code
    WHERE mp.cbsa_code = :cbsa_code
    ORDER BY mp.updated_at DESC
    LIMIT 1
""")


def _row_to_dict(row) -> Dict[str, Any]:
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


def _safe_query(db: Session, sql, params: dict) -> List[Dict[str, Any]]:
    try:
        result = db.execute(sql, params)
        return [_row_to_dict(r) for r in result]
    except Exception as e:
        logger.warning(f"Metro profiles query failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/metro-profiles/rankings")
async def get_metro_rankings(
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Return metros sorted by build_hostility_score descending.
    Score 76-100 (grade D) = very hard to build. Score 0-25 (grade A) = very buildable.
    """
    rows = _safe_query(db, _RANKINGS_SQL, {"limit": limit, "offset": offset})

    if not rows:
        return {
            "status": "not_ingested",
            "message": "No metro profiles found. POST /metro-profiles/ingest to collect data.",
            "total": 0,
            "data": [],
        }

    return {
        "status": "ok",
        "total": len(rows),
        "offset": offset,
        "limit": limit,
        "data": rows,
    }


@router.get("/metro-profiles/")
async def list_metro_profiles(
    state: Optional[str] = Query(default=None, description="Filter by state abbr, e.g. 'CA'"),
    metro_type: Optional[str] = Query(default=None, description="'metropolitan' or 'micropolitan'"),
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Paginated list of metro profiles ordered by population rank.
    Optionally filter by state or metro_type.
    """
    params = {
        "state": state,
        "state_like": f"%{state}%" if state else None,
        "metro_type": metro_type,
        "limit": limit,
        "offset": offset,
    }

    rows = _safe_query(db, _LIST_SQL, params)
    count_rows = _safe_query(db, _COUNT_SQL, params)
    total = count_rows[0]["count"] if count_rows else 0

    if not rows and offset == 0:
        return {
            "status": "not_ingested",
            "message": "No metro profiles found. POST /metro-profiles/ingest to collect data.",
            "total": 0,
            "data": [],
        }

    return {
        "status": "ok",
        "total": total,
        "offset": offset,
        "limit": limit,
        "data": rows,
    }


@router.get("/metro-profiles/{cbsa_code}")
async def get_metro_profile(
    cbsa_code: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Full profile for a single metro area, including all raw data and scores.
    CBSA codes are 5-digit strings, e.g. '35620' for New York.
    """
    cbsa_code = cbsa_code.strip().zfill(5)
    rows = _safe_query(db, _DETAIL_SQL, {"cbsa_code": cbsa_code})

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No profile found for CBSA {cbsa_code}. "
                   "Run POST /metro-profiles/ingest to collect data.",
        )

    profile = rows[0]

    # Build factor breakdown for UI
    factors = []
    if profile.get("permit_velocity_score") is not None:
        vel = float(profile["permit_velocity_score"])
        factors.append({
            "factor": "Permit Velocity",
            "buildability_score": vel,
            "hostility_contribution": round(100 - vel, 1),
            "raw_value": profile.get("permits_per_1000_units"),
            "unit": "permits per 1,000 housing units",
        })
    if profile.get("multifamily_score") is not None:
        mf = float(profile["multifamily_score"])
        factors.append({
            "factor": "Multifamily Permitting",
            "buildability_score": mf,
            "hostility_contribution": round(100 - mf, 1),
            "raw_value": profile.get("multifamily_share_pct"),
            "unit": "% of permits that are 5+ unit",
        })
    if profile.get("supply_elasticity_score") is not None:
        se = float(profile["supply_elasticity_score"])
        factors.append({
            "factor": "Supply Elasticity",
            "buildability_score": se,
            "hostility_contribution": round(100 - se, 1),
            "raw_value": profile.get("hpi_5yr_pct"),
            "unit": "permit velocity ÷ 5yr HPI appreciation",
        })

    return {
        "status": "ok",
        "profile": profile,
        "factor_breakdown": factors,
    }


@router.post("/metro-profiles/ingest")
async def ingest_metro_profiles(
    background_tasks: BackgroundTasks,
    vintage: Optional[str] = Query(default=None, description="Data vintage year, e.g. '2024'"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Trigger background ingestion of metro development profiles.
    Pulls from Census BPS, FHFA HPI, Census ACS, and BLS LAUS.
    """
    background_tasks.add_task(_run_ingest, vintage)

    return {
        "status": "accepted",
        "message": "Metro profiles ingestion started in background.",
        "vintage": vintage or "auto (prior year)",
        "sources": ["Census BPS (building permits)", "FHFA HPI", "Census ACS", "BLS LAUS"],
        "endpoints": {
            "rankings": "/api/v1/metro-profiles/rankings",
            "list": "/api/v1/metro-profiles/",
            "detail": "/api/v1/metro-profiles/{cbsa_code}",
        },
    }


async def _run_ingest(vintage: Optional[str]) -> None:
    """Background task: run the full metro profile ingestion pipeline."""
    from app.core.database import get_session_factory
    from app.sources.metro.ingest import MetroProfileIngestor

    db = get_session_factory()()
    try:
        ingestor = MetroProfileIngestor(db=db)
        summary = await ingestor.run(vintage=vintage)
        logger.info(f"Metro profile ingest complete: {summary}")
    except Exception as e:
        logger.error(f"Metro profile ingest background task failed: {e}", exc_info=True)
    finally:
        db.close()
