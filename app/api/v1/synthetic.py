"""
Synthetic Data API — SPEC_042 / PLAN_052 Phase A

POST /synthetic/private-financials   — on-demand private company financial profiles
POST /synthetic/macro-scenarios      — on-demand macro scenario paths
"""
from __future__ import annotations
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
from app.services.synthetic.macro_scenarios import MacroScenarioGenerator

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Synthetic Data"])


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class PrivateFinancialsRequest(BaseModel):
    sector: str = "industrials"
    revenue_min_millions: float = Field(default=10.0, gt=0)
    revenue_max_millions: float = Field(default=500.0, gt=0)
    n_companies: int = Field(default=20, ge=1, le=100)
    seed: Optional[int] = None


class MacroScenariosRequest(BaseModel):
    n_scenarios: int = Field(default=100, ge=1, le=1000)
    horizon_months: int = Field(default=24, ge=1, le=120)
    series: List[str] = Field(
        default=["DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT"]
    )
    seed: Optional[int] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/synthetic/private-financials")
def generate_private_financials(
    req: PrivateFinancialsRequest,
    db: Session = Depends(get_db),
):
    """
    Generate synthetic private company financial profiles on demand.

    Uses Gaussian copula fitted from EDGAR peer data (or sector priors as fallback).
    Returns correlated revenue + margin profiles for n_companies synthetic firms.
    """
    if req.revenue_min_millions >= req.revenue_max_millions:
        raise HTTPException(
            status_code=422,
            detail="revenue_min_millions must be less than revenue_max_millions",
        )

    gen = PrivateCompanyFinancialGenerator(db)
    result = gen.generate(
        n_companies=req.n_companies,
        sector=req.sector,
        revenue_min_millions=req.revenue_min_millions,
        revenue_max_millions=req.revenue_max_millions,
        seed=req.seed,
    )
    return result


@router.post("/synthetic/macro-scenarios")
def generate_macro_scenarios(
    req: MacroScenariosRequest,
    db: Session = Depends(get_db),
):
    """
    Generate N macro scenario paths via mean-reverting correlated random walk.

    Calibrated from FRED historical data in the DB. Returns full paths for each
    series + terminal value percentile summary across all scenarios.
    """
    if not req.series:
        raise HTTPException(status_code=422, detail="series list must not be empty")

    gen = MacroScenarioGenerator(db)
    result = gen.generate(
        n_scenarios=req.n_scenarios,
        horizon_months=req.horizon_months,
        series=req.series,
        seed=req.seed,
    )
    return result
