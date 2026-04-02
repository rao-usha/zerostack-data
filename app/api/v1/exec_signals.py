"""
Executive Signal API — Chain 4 of PLAN_052.

Detects leadership transition signals for deal sourcing.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.exec_signal_scorer import ExecSignalScorer

router = APIRouter(prefix="/exec-signals", tags=["Executive Signals (Chain 4)"])


@router.get("/scan")
def scan_executive_signals(
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Scan all companies for executive transition signals.
    Returns companies ranked by transition score — highest signal first.
    Flags: succession_in_progress, management_buildup, founder_transition.
    """
    scorer = ExecSignalScorer(db)
    results = scorer.scan_companies(limit=limit)
    return {
        "status": "ok",
        "total": len(results),
        "companies": [
            {
                "company_id": r.company_id,
                "company_name": r.company_name,
                "transition_score": r.transition_score,
                "flags": r.flags,
                "signals": [
                    {"signal": s.signal, "score": s.score, "reading": s.reading, "flag": s.flag}
                    for s in r.signals
                ],
                "details": r.details,
            }
            for r in results
        ],
    }


@router.get("/company/{company_id}")
def get_company_exec_signals(company_id: int, db: Session = Depends(get_db)):
    """Detailed executive transition signals for a single company."""
    scorer = ExecSignalScorer(db)
    r = scorer.score_company(company_id)
    return {
        "status": "ok",
        "company_id": r.company_id,
        "company_name": r.company_name,
        "transition_score": r.transition_score,
        "flags": r.flags,
        "signals": [
            {"signal": s.signal, "score": s.score, "reading": s.reading, "flag": s.flag}
            for s in r.signals
        ],
        "details": r.details,
    }
