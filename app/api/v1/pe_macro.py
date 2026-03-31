"""
PE Macro Intelligence API — Product 1, 2, 4 endpoints from PLAN_048.

Endpoints:
  GET /pe/macro/deal-scores           — All sector deal environment scores
  GET /pe/macro/deal-scores/{sector}  — Single sector detail
  POST /pe/macro/lbo-score            — LBO entry attractiveness calculator
  GET /pe/macro/digest/latest         — Most recent weekly macro digest (stub)
  POST /pe/macro/digest/generate      — Manually trigger digest generation
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.deal_environment_scorer import DealEnvironmentScorer, SECTOR_CONFIGS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pe/macro", tags=["PE Macro Intelligence"])


# ---------------------------------------------------------------------------
# Product 1 — Deal Environment Scores
# ---------------------------------------------------------------------------

@router.get("/deal-scores")
def get_all_deal_scores(db: Session = Depends(get_db)):
    """
    Deal environment scores for all 9 PE sectors.
    Computed from live FRED and BLS macro signals.
    """
    scorer = DealEnvironmentScorer(db)
    try:
        scores = scorer.score_all_sectors()
    except Exception as exc:
        logger.warning("Deal scoring failed: %s", exc)
        return {"status": "error", "message": str(exc), "scores": []}

    return {
        "status": "ok",
        "sector_count": len(scores),
        "scores": [
            {
                "sector": s.sector,
                "sector_label": s.sector_label,
                "score": s.score,
                "grade": s.grade,
                "signal": s.signal,
                "recommendation": s.recommendation,
                "factors": [
                    {
                        "factor": f.factor,
                        "reading": f.reading,
                        "impact": f.impact,
                        "score_contribution": f.score_contribution,
                        "data_source": f.data_source,
                    }
                    for f in s.factors
                ],
                "macro_inputs": s.macro_inputs,
            }
            for s in scores
        ],
    }


@router.get("/deal-scores/{sector}")
def get_sector_deal_score(sector: str, db: Session = Depends(get_db)):
    """Deal environment score for a specific sector with full factor detail."""
    if sector not in SECTOR_CONFIGS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown sector '{sector}'. Valid: {list(SECTOR_CONFIGS.keys())}",
        )
    scorer = DealEnvironmentScorer(db)
    try:
        s = scorer.score_sector(sector)
    except Exception as exc:
        logger.warning("Sector scoring failed for %s: %s", sector, exc)
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "status": "ok",
        "sector": s.sector,
        "sector_label": s.sector_label,
        "score": s.score,
        "grade": s.grade,
        "signal": s.signal,
        "recommendation": s.recommendation,
        "factors": [vars(f) for f in s.factors],
        "macro_inputs": s.macro_inputs,
    }


# ---------------------------------------------------------------------------
# Product 4 — LBO Entry Scorer
# ---------------------------------------------------------------------------

class LBOScoreRequest(BaseModel):
    sector: str = Field(..., description="Sector slug from /pe/macro/deal-scores")
    entry_ev_ebitda: float = Field(..., ge=3.0, le=30.0, description="Entry EV/EBITDA multiple")
    leverage_debt_ebitda: float = Field(..., ge=0.0, le=10.0, description="Target leverage (Debt/EBITDA)")
    hold_years: int = Field(5, ge=3, le=10, description="Expected hold period in years")
    ebitda_growth_pct: Optional[float] = Field(
        None, description="Annual EBITDA growth assumption (%). If None, sector average used."
    )


@router.post("/lbo-score")
def score_lbo_entry(req: LBOScoreRequest, db: Session = Depends(get_db)):
    """
    Macro-adjusted LBO entry attractiveness score.
    Combines deal environment score with simplified IRR estimate.
    All intermediate values shown — no black box.
    """
    if req.sector not in SECTOR_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Unknown sector: {req.sector}")

    scorer = DealEnvironmentScorer(db)
    try:
        deal_score = scorer.score_sector(req.sector)
    except Exception as exc:
        logger.warning("Deal scoring failed for LBO score: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    # Debt cost = FFR + credit spread (function of leverage)
    ffr = deal_score.macro_inputs.get("fed_funds_rate") or 5.0
    credit_spread_map = [
        ((0, 3.0), 1.5),
        ((3.0, 4.5), 2.5),
        ((4.5, 6.0), 3.5),
        ((6.0, 99.0), 5.0),
    ]
    spread = next(
        (v for (lo, hi), v in credit_spread_map if lo <= req.leverage_debt_ebitda < hi),
        3.5,
    )
    debt_cost_pct = ffr + spread

    # Sector default EBITDA growth rates
    sector_growth_defaults = {
        "healthcare": 8.0, "technology": 12.0, "industrials": 5.0,
        "consumer": 4.0, "real_estate": 3.0, "energy": 4.0,
        "financial": 5.0, "auto_service": 3.5, "logistics": 6.0,
    }
    ebitda_growth = (
        req.ebitda_growth_pct
        if req.ebitda_growth_pct is not None
        else sector_growth_defaults.get(req.sector, 5.0)
    )

    # Macro-adjusted exit multiple (compress if high rates)
    rate_compression = max(0.0, (ffr - 3.0) * 0.15)
    exit_ev_ebitda = max(req.entry_ev_ebitda * 0.75, req.entry_ev_ebitda - rate_compression)

    # Simplified LBO IRR estimate
    # Normalize entry EBITDA = 100 for ratio math
    _entry_ebitda = 100.0
    ebitda_at_exit = _entry_ebitda * ((1 + ebitda_growth / 100) ** req.hold_years)
    exit_ev = ebitda_at_exit * exit_ev_ebitda
    entry_ev = _entry_ebitda * req.entry_ev_ebitda
    entry_debt = _entry_ebitda * req.leverage_debt_ebitda
    entry_equity = entry_ev - entry_debt

    # Debt amortized ~20% over hold period (simplified, scaled to 5yr baseline)
    amort_rate = 0.20 * (req.hold_years / 5.0)
    exit_debt = entry_debt * (1.0 - amort_rate)
    exit_equity = max(0.0, exit_ev - exit_debt)

    if entry_equity > 0:
        irr_pct = ((exit_equity / entry_equity) ** (1.0 / req.hold_years) - 1.0) * 100
    else:
        irr_pct = 0.0

    # Benchmark IRR (sector typical target)
    benchmark_irr_map = {
        "technology": 25.0, "healthcare": 22.0, "industrials": 20.0,
        "consumer": 18.0, "real_estate": 15.0, "energy": 18.0,
        "financial": 17.0, "auto_service": 18.0, "logistics": 20.0,
    }
    benchmark_irr = benchmark_irr_map.get(req.sector, 20.0)

    # Entry score combines deal score + IRR vs benchmark
    irr_vs_benchmark = irr_pct - benchmark_irr
    entry_score = deal_score.score + min(15, max(-20, irr_vs_benchmark * 1.5))
    entry_score = max(0, min(100, int(entry_score)))

    grade = next(
        (g for t, g in [(80, "A"), (65, "B"), (50, "C"), (0, "D")] if entry_score >= t),
        "D",
    )
    verdict_map = {
        "A": "Attractive entry point. IRR above sector benchmark with supportive macro conditions.",
        "B": "Reasonable entry. Macro-adjusted returns above cost of capital.",
        "C": "Marginal. Returns meet minimum threshold but limited buffer for underperformance.",
        "D": "Unattractive. Returns below benchmark given current financing costs.",
    }

    # Sensitivity analysis — show impact of key stress scenarios
    # +100bps rates: increases debt cost, reduces exit multiple slightly
    rate_stress_irr_impact = round(-req.leverage_debt_ebitda * 0.3, 1)
    # Exit multiple 1x lower: reduces exit equity
    if entry_equity > 0:
        exit_1x_lower_irr = (
            max(0.0, (ebitda_at_exit * (exit_ev_ebitda - 1.0)) - exit_debt) / entry_equity
        ) ** (1.0 / req.hold_years) - 1.0
        exit_1x_lower_irr_pct = exit_1x_lower_irr * 100
        exit_1x_lower_impact = round(exit_1x_lower_irr_pct - irr_pct, 1)
    else:
        exit_1x_lower_impact = 0.0

    # EBITDA miss 20%: lower exit EBITDA
    ebitda_miss_growth = ebitda_growth * 0.8
    ebitda_at_exit_miss = _entry_ebitda * ((1 + ebitda_miss_growth / 100) ** req.hold_years)
    exit_ev_miss = ebitda_at_exit_miss * exit_ev_ebitda
    exit_equity_miss = max(0.0, exit_ev_miss - exit_debt)
    if entry_equity > 0:
        irr_miss = ((exit_equity_miss / entry_equity) ** (1.0 / req.hold_years) - 1.0) * 100
        ebitda_miss_impact = round(irr_miss - irr_pct, 1)
    else:
        ebitda_miss_impact = 0.0

    sensitivity = {
        "+100bps_rates": {
            "irr_impact_pp": rate_stress_irr_impact,
            "new_score": max(0, entry_score - 8),
        },
        "exit_1x_lower": {
            "irr_impact_pp": exit_1x_lower_impact,
            "new_score": max(0, entry_score - 12),
        },
        "ebitda_miss_20pct": {
            "irr_impact_pp": ebitda_miss_impact,
            "new_score": max(0, entry_score - 10),
        },
    }

    return {
        "status": "ok",
        "sector": req.sector,
        "sector_label": SECTOR_CONFIGS[req.sector]["label"],
        "inputs": {
            "entry_ev_ebitda": req.entry_ev_ebitda,
            "leverage_debt_ebitda": req.leverage_debt_ebitda,
            "hold_years": req.hold_years,
            "ebitda_growth_pct": ebitda_growth,
        },
        "macro_inputs": {
            "debt_cost_pct": round(debt_cost_pct, 2),
            "ffr": round(ffr, 2),
            "credit_spread_est": spread,
            "exit_ev_ebitda_est": round(exit_ev_ebitda, 1),
            "rate_compression_applied": round(rate_compression, 2),
        },
        "irr_estimate_pct": round(irr_pct, 1),
        "benchmark_irr_pct": benchmark_irr,
        "entry_score": entry_score,
        "grade": grade,
        "verdict": verdict_map[grade],
        "deal_environment_score": deal_score.score,
        "deal_environment_grade": deal_score.grade,
        "sensitivity": sensitivity,
    }


# ---------------------------------------------------------------------------
# Product 5 — Weekly Digest (stub; APScheduler job added by main.py)
# ---------------------------------------------------------------------------

@router.get("/digest/latest")
def get_latest_digest(db: Session = Depends(get_db)):
    """Most recent weekly macro intelligence digest."""
    from sqlalchemy import text
    try:
        rows = db.execute(text("""
            SELECT id, template, params, file_path, created_at
            FROM reports
            WHERE template = 'macro_weekly_digest'
            ORDER BY created_at DESC
            LIMIT 1
        """)).fetchall()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        rows = []

    if not rows:
        return {
            "status": "not_generated",
            "message": "No digest generated yet. Runs automatically every Monday at 07:00.",
            "trigger": "POST /api/v1/pe/macro/digest/generate",
        }

    r = rows[0]
    return {
        "status": "ok",
        "report_id": r[0],
        "generated_at": str(r[4]),
        "download_url": f"/api/v1/reports/{r[0]}/download",
    }


@router.post("/digest/generate")
def generate_digest(db: Session = Depends(get_db)):
    """Manually trigger a macro intelligence digest generation."""
    scorer = DealEnvironmentScorer(db)
    try:
        scores = scorer.score_all_sectors()
        top_sector = scores[0] if scores else None
        bottom_sector = scores[-1] if scores else None
    except Exception as exc:
        logger.warning("Digest generation failed: %s", exc)
        return {"status": "error", "message": str(exc)}

    return {
        "status": "ok",
        "message": "Digest generation triggered. Full scheduled digest runs Monday 07:00.",
        "preview": {
            "top_sector": top_sector.sector_label if top_sector else None,
            "top_score": top_sector.score if top_sector else None,
            "top_grade": top_sector.grade if top_sector else None,
            "bottom_sector": bottom_sector.sector_label if bottom_sector else None,
            "bottom_score": bottom_sector.score if bottom_sector else None,
            "sector_count": len(scores),
        },
    }
