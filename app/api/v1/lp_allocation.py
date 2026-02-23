"""
LP Allocation Gap Analysis API endpoints.

Exposes allocation gap analysis for institutional LPs, showing where
capital must be deployed to reach target weights.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.public_lp_strategies.gap_analysis import (
    compute_all_lp_gaps,
    compute_lp_allocation_gaps,
    find_capital_deployment_opportunities,
    get_allocation_summary,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lp-allocation", tags=["lp_allocation"])


# ---------------------------------------------------------------------------
# GET /lp-allocation/gaps/summary — aggregate across all LPs (FIRST)
# ---------------------------------------------------------------------------

@router.get(
    "/gaps/summary",
    summary="Allocation gap summary across all LPs",
    response_description="Aggregate underweight capital by asset class",
)
def get_gaps_summary(db: Session = Depends(get_db)):
    """
    Return aggregate allocation gap summary across all LPs.

    Shows total LPs analyzed and underweight capital by asset class.
    """
    try:
        return get_allocation_summary(db)
    except Exception as e:
        logger.error(f"Gap summary error: {e}")
        raise HTTPException(status_code=500, detail="Failed to compute gap summary")


# ---------------------------------------------------------------------------
# GET /lp-allocation/gaps — all LP gaps
# ---------------------------------------------------------------------------

@router.get(
    "/gaps",
    summary="Allocation gaps for all LPs",
    response_description="Gap analysis per LP with optional asset class filter",
)
def get_all_gaps(
    asset_class: Optional[str] = Query(
        None,
        description="Filter to a specific asset class (e.g., private_equity, real_estate)",
    ),
    db: Session = Depends(get_db),
):
    """
    Return allocation gaps for all LPs that have strategy data.

    Optionally filter to show gaps for a single asset class.
    """
    try:
        results = compute_all_lp_gaps(db, asset_class_filter=asset_class)
        return {
            "total_lps": len(results),
            "asset_class_filter": asset_class,
            "lps": results,
        }
    except Exception as e:
        logger.error(f"All gaps query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to compute LP gaps")


# ---------------------------------------------------------------------------
# GET /lp-allocation/deployment-opportunities — underweight LPs
# ---------------------------------------------------------------------------

@router.get(
    "/deployment-opportunities",
    summary="Find LPs that need to deploy capital into an asset class",
    response_description="LPs underweight in given asset class, sorted by capital to deploy",
)
def get_deployment_opportunities(
    asset_class: str = Query(
        "private_equity",
        description="Asset class to find underweight LPs for",
    ),
    min_gap_pct: float = Query(
        1.0, ge=0.0,
        description="Minimum gap percentage to include",
    ),
    db: Session = Depends(get_db),
):
    """
    Find LPs that are underweight in a given asset class, sorted by the
    amount of capital they need to deploy. This is the killer PE demo endpoint.

    Default: find LPs underweight in private_equity with gap >= 1%.
    """
    try:
        opportunities = find_capital_deployment_opportunities(
            db, asset_class=asset_class, min_gap_pct=min_gap_pct
        )
        total_capital = sum(
            o["gap_capital_usd"] for o in opportunities if o["gap_capital_usd"]
        )
        return {
            "asset_class": asset_class,
            "min_gap_pct": min_gap_pct,
            "total_opportunities": len(opportunities),
            "total_deployment_capital_usd": round(total_capital, 0),
            "opportunities": opportunities,
        }
    except Exception as e:
        logger.error(f"Deployment opportunities error: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to find deployment opportunities"
        )


# ---------------------------------------------------------------------------
# GET /lp-allocation/gaps/{lp_id} — single LP detail (LAST — catch-all)
# ---------------------------------------------------------------------------

@router.get(
    "/gaps/{lp_id}",
    summary="Allocation gaps for a single LP",
    response_description="Detailed gap analysis for one LP",
)
def get_lp_gaps(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Return detailed allocation gap analysis for a single LP.

    Shows target vs current weight for each asset class, gap direction,
    urgency score, and estimated capital gap in USD.
    """
    try:
        result = compute_lp_allocation_gaps(db, lp_id)
        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"LP gap analysis error for lp_id={lp_id}: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to compute LP allocation gaps"
        )
