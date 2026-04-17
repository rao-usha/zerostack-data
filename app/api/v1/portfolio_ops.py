"""PE Intelligence — Portfolio Operations API (PLAN_060 Phase 2)."""

from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.portfolio_operations_engine import PortfolioOperationsEngine

router = APIRouter(prefix="/pe/portfolio-ops", tags=["PE Portfolio Operations"])


@router.get("/overview/{firm_id}", summary="Full portfolio overview with KPIs")
def get_overview(firm_id: int, db: Session = Depends(get_db)) -> Dict:
    result = PortfolioOperationsEngine(db).get_overview(firm_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.get("/company/{company_id}", summary="Per-company drill-down")
def get_company_detail(company_id: int, db: Session = Depends(get_db)) -> Dict:
    result = PortfolioOperationsEngine(db).get_company_detail(company_id)
    if not result:
        raise HTTPException(404, f"Company {company_id} not found")
    return result


@router.get("/heatmap/{firm_id}", summary="Portfolio heatmap matrix")
def get_heatmap(firm_id: int, db: Session = Depends(get_db)) -> Dict:
    return PortfolioOperationsEngine(db).get_heatmap(firm_id)


@router.get("/financials/{company_id}/timeseries", summary="Financial time-series")
def get_financials(company_id: int, db: Session = Depends(get_db)) -> List[Dict]:
    return PortfolioOperationsEngine(db).get_financial_timeseries(company_id)


@router.get("/kpis/{firm_id}", summary="Lightweight KPI summary")
def get_kpis(firm_id: int, db: Session = Depends(get_db)) -> Dict:
    return PortfolioOperationsEngine(db).get_kpis(firm_id)
