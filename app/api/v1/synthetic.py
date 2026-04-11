"""
Synthetic Data API — SPEC_042/044, PLAN_052/053

POST /synthetic/private-financials   — on-demand private company financial profiles
POST /synthetic/macro-scenarios      — on-demand macro scenario paths
POST /synthetic/job-postings         — seed job postings for exec signal scoring
POST /synthetic/lp-gp-universe       — seed LP funds + LP-GP relationships
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
from app.services.synthetic.job_postings import SyntheticJobPostingsGenerator
from app.services.synthetic.lp_gp_universe import SyntheticLpGpGenerator

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


class JobPostingsRequest(BaseModel):
    n_per_company: int = Field(default=80, ge=1, le=500)
    seed: Optional[int] = None


class LpGpUniverseRequest(BaseModel):
    n_lps: int = Field(default=500, ge=10, le=2000)
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


@router.post("/synthetic/job-postings")
def generate_job_postings(
    req: JobPostingsRequest,
    db: Session = Depends(get_db),
):
    """
    Generate synthetic job postings for all seeded companies.

    Creates realistic sector-aware postings with seniority distribution
    (~5% C-suite/VP, ~15% Director, ~30% Manager, ~50% IC).
    Unblocks exec_signal_scorer and company_diligence growth factor.
    All data tagged as data_origin='synthetic'.
    """
    gen = SyntheticJobPostingsGenerator(db)
    return gen.generate(n_per_company=req.n_per_company, seed=req.seed)


@router.post("/synthetic/lp-gp-universe")
def generate_lp_gp_universe(
    req: LpGpUniverseRequest,
    db: Session = Depends(get_db),
):
    """
    Generate synthetic LP fund universe and LP-GP commitment relationships.

    Creates ~N LP funds with realistic type distribution (pension 40%,
    endowment 20%, etc.) and power-law GP relationships linking to
    seeded PE firms. Unblocks gp_pipeline_scorer and lp_gp_graph.
    All data tagged as data_origin='synthetic'.
    """
    gen = SyntheticLpGpGenerator(db)
    return gen.generate(n_lps=req.n_lps, seed=req.seed)


# ---------------------------------------------------------------------------
# Validation endpoints — PLAN_057
# ---------------------------------------------------------------------------

from app.services.synthetic.validation import SyntheticValidator

_GENERATOR_MAP = {
    "job-postings": "job_postings",
    "lp-gp": "lp_gp",
    "macro-scenarios": "macro_scenarios",
    "private-financials": "private_financials",
}


@router.get("/synthetic/validate")
def validate_all_generators(seed: int = 42, n_samples: int = 500):
    """Run validation across all 4 synthetic generators."""
    return SyntheticValidator.validate_all(seed=seed)


@router.get("/synthetic/validate/{generator}")
def validate_generator(
    generator: str,
    seed: int = 42,
    n_samples: int = 1000,
    sector: str = "industrials",
):
    """Full validation for a single generator with histograms, tests, correlations."""
    key = _GENERATOR_MAP.get(generator)
    if not key:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown generator '{generator}'. "
                   f"Valid: {', '.join(_GENERATOR_MAP.keys())}",
        )

    if key == "job_postings":
        return SyntheticValidator.validate_job_postings(n_samples=n_samples, seed=seed)
    elif key == "lp_gp":
        return SyntheticValidator.validate_lp_gp(n_samples=n_samples, seed=seed)
    elif key == "macro_scenarios":
        return SyntheticValidator.validate_macro_scenarios(n_scenarios=n_samples, seed=seed)
    elif key == "private_financials":
        return SyntheticValidator.validate_private_financials(
            n_companies=n_samples, sector=sector, seed=seed,
        )


@router.get("/synthetic/validate/{generator}/compare")
def compare_algorithms(
    generator: str,
    seed: int = 42,
    n_samples: int = 1000,
):
    """Compare algorithms for a generator (future-ready, currently returns single result)."""
    key = _GENERATOR_MAP.get(generator)
    if not key:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown generator '{generator}'. "
                   f"Valid: {', '.join(_GENERATOR_MAP.keys())}",
        )

    # Currently one algorithm per generator
    if key == "job_postings":
        result = SyntheticValidator.validate_job_postings(n_samples=n_samples, seed=seed)
    elif key == "lp_gp":
        result = SyntheticValidator.validate_lp_gp(n_samples=n_samples, seed=seed)
    elif key == "macro_scenarios":
        result = SyntheticValidator.validate_macro_scenarios(n_scenarios=n_samples, seed=seed)
    elif key == "private_financials":
        result = SyntheticValidator.validate_private_financials(n_companies=n_samples, seed=seed)

    return {
        "generator": generator,
        "algorithms": [{"name": "default", "result": result}],
    }
