"""
13F Quarterly Diff & Convergence API.

Endpoints for comparing investor 13F holdings quarter-over-quarter
and detecting cross-investor convergence signals.
"""

import logging
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.analytics.quarterly_diff import QuarterlyDiffService
from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/13f-analysis", tags=["13F Analysis"])


# =============================================================================
# Response Models
# =============================================================================


class QuarterInfo(BaseModel):
    """A quarter with 13F data."""

    quarter: str = Field(..., description="Quarter label (e.g., '2024-Q4')")
    date: str = Field(..., description="Filing date")
    holdings_count: int = Field(..., description="Number of holdings in this quarter")


class QuartersResponse(BaseModel):
    """Available quarters for an investor."""

    investor_id: int
    investor_type: str
    quarters: List[QuarterInfo]


class HoldingChangeResponse(BaseModel):
    """A single holding change between quarters."""

    key: str = Field(..., description="CUSIP or company ID")
    company_name: str
    cusip: Optional[str] = None
    ticker: Optional[str] = None
    change_type: str = Field(
        ..., description="new, exited, increased, decreased, or unchanged"
    )
    shares_prev: Optional[str] = None
    shares_curr: Optional[str] = None
    shares_change: Optional[str] = None
    shares_change_pct: Optional[float] = None
    value_prev: Optional[str] = None
    value_curr: Optional[str] = None
    value_change: Optional[str] = None


class DiffSummaryResponse(BaseModel):
    """Aggregate summary of quarterly changes."""

    total_positions_prev: int = 0
    total_positions_curr: int = 0
    new_positions: int = 0
    exited_positions: int = 0
    increased_positions: int = 0
    decreased_positions: int = 0
    unchanged_positions: int = 0
    total_value_prev: Optional[str] = None
    total_value_curr: Optional[str] = None
    total_value_change: Optional[str] = None
    turnover_rate: Optional[float] = None


class QuarterlyDiffResponse(BaseModel):
    """Full quarterly diff report."""

    investor_id: int
    investor_type: str
    investor_name: str
    quarter_prev: Optional[str] = None
    quarter_curr: Optional[str] = None
    summary: DiffSummaryResponse
    changes: List[HoldingChangeResponse]


class ConvergenceParticipantResponse(BaseModel):
    """An investor participating in a convergence signal."""

    investor_id: int
    investor_type: str
    investor_name: str
    action: str
    shares_change: Optional[str] = None
    value_change: Optional[str] = None


class ConvergenceSignalResponse(BaseModel):
    """A security with convergence activity."""

    company_name: str
    cusip: Optional[str] = None
    ticker: Optional[str] = None
    signal_strength: float = Field(..., description="0-100 score")
    participant_count: int
    new_position_count: int
    increased_position_count: int
    total_value_added: Optional[str] = None
    participants: List[ConvergenceParticipantResponse]


class ConvergenceResponse(BaseModel):
    """Full convergence detection report."""

    quarter: str
    quarter_prev: str
    total_investors_scanned: int
    total_signals: int
    signals: List[ConvergenceSignalResponse]


# =============================================================================
# Helpers
# =============================================================================


def _decimal_str(val: Optional[Decimal]) -> Optional[str]:
    """Convert Decimal to string for JSON response."""
    if val is None:
        return None
    return str(val)


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/quarters/{investor_id}", response_model=QuartersResponse)
async def get_investor_quarters(
    investor_id: int,
    investor_type: str = Query(
        "lp", description="Investor type", regex="^(lp|pe)$"
    ),
    db: Session = Depends(get_db),
):
    """
    List available quarters with 13F data for an investor.

    **Parameters:**
    - **investor_id**: LP fund ID or PE firm ID
    - **investor_type**: 'lp' or 'pe'

    **Returns:** List of quarters with filing dates and holding counts.
    """
    service = QuarterlyDiffService(db)
    quarters = service.get_available_quarters(investor_id, investor_type)

    return QuartersResponse(
        investor_id=investor_id,
        investor_type=investor_type,
        quarters=[QuarterInfo(**q) for q in quarters],
    )


@router.get("/diff/{investor_id}", response_model=QuarterlyDiffResponse)
async def get_quarterly_diff(
    investor_id: int,
    investor_type: str = Query(
        "lp", description="Investor type", regex="^(lp|pe)$"
    ),
    quarter_curr: Optional[str] = Query(
        None, description="Current quarter (e.g., '2024-Q4')"
    ),
    quarter_prev: Optional[str] = Query(
        None, description="Previous quarter (e.g., '2024-Q3')"
    ),
    material_threshold: float = Query(
        0.05, ge=0.0, le=1.0, description="Threshold for material change (default 5%)"
    ),
    db: Session = Depends(get_db),
):
    """
    Full quarterly diff report for an investor's 13F holdings.

    Compares holdings between two quarters and classifies each position as:
    - **new**: Position opened this quarter
    - **exited**: Position closed this quarter
    - **increased**: Shares/value up by more than threshold
    - **decreased**: Shares/value down by more than threshold
    - **unchanged**: Position within threshold

    If quarters not specified, uses the two most recent.

    **Example:**
    ```
    GET /13f-analysis/diff/1?investor_type=lp&quarter_curr=2024-Q4&quarter_prev=2024-Q3
    ```
    """
    service = QuarterlyDiffService(db)

    try:
        report = service.get_quarterly_diff(
            investor_id=investor_id,
            investor_type=investor_type,
            quarter_curr=quarter_curr,
            quarter_prev=quarter_prev,
            material_threshold=material_threshold,
        )
    except Exception as e:
        logger.error(f"Error computing quarterly diff: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to compute diff: {str(e)}")

    s = report.summary
    return QuarterlyDiffResponse(
        investor_id=report.investor_id,
        investor_type=report.investor_type,
        investor_name=report.investor_name,
        quarter_prev=report.quarter_prev,
        quarter_curr=report.quarter_curr,
        summary=DiffSummaryResponse(
            total_positions_prev=s.total_positions_prev,
            total_positions_curr=s.total_positions_curr,
            new_positions=s.new_positions,
            exited_positions=s.exited_positions,
            increased_positions=s.increased_positions,
            decreased_positions=s.decreased_positions,
            unchanged_positions=s.unchanged_positions,
            total_value_prev=_decimal_str(s.total_value_prev),
            total_value_curr=_decimal_str(s.total_value_curr),
            total_value_change=_decimal_str(s.total_value_change),
            turnover_rate=s.turnover_rate,
        ),
        changes=[
            HoldingChangeResponse(
                key=c.key,
                company_name=c.company_name,
                cusip=c.cusip,
                ticker=c.ticker,
                change_type=c.change_type,
                shares_prev=_decimal_str(c.shares_prev),
                shares_curr=_decimal_str(c.shares_curr),
                shares_change=_decimal_str(c.shares_change),
                shares_change_pct=(
                    round(c.shares_change_pct, 4) if c.shares_change_pct is not None else None
                ),
                value_prev=_decimal_str(c.value_prev),
                value_curr=_decimal_str(c.value_curr),
                value_change=_decimal_str(c.value_change),
            )
            for c in report.changes
        ],
    )


@router.get("/convergence", response_model=ConvergenceResponse)
async def detect_convergence(
    quarter: Optional[str] = Query(
        None, description="Quarter to analyze (e.g., '2024-Q4'). Defaults to latest."
    ),
    min_investors: int = Query(
        3, ge=2, le=100, description="Minimum investors for a signal"
    ),
    min_total_value: Optional[float] = Query(
        None, ge=0, description="Minimum total value added (USD)"
    ),
    include_types: Optional[str] = Query(
        None, description="Comma-separated investor types to include (e.g., 'lp,pe')"
    ),
    db: Session = Depends(get_db),
):
    """
    Detect cross-investor convergence signals.

    Scans all tracked investors (LP + PE) and identifies securities where
    multiple institutions opened new positions or increased existing ones
    in the same quarter.

    **Signal strength (0-100)** factors:
    - Breadth: proportion of tracked investors taking action
    - New-position bonus: higher weight for brand-new positions vs increases
    - Value bonus: larger dollar amounts score higher

    **Example:**
    ```
    GET /13f-analysis/convergence?quarter=2024-Q4&min_investors=3
    ```
    """
    service = QuarterlyDiffService(db)

    types = None
    if include_types:
        types = [t.strip() for t in include_types.split(",") if t.strip() in ("lp", "pe")]

    min_val = Decimal(str(min_total_value)) if min_total_value else None

    try:
        report = service.detect_convergence(
            quarter=quarter,
            min_investors=min_investors,
            min_total_value=min_val,
            include_types=types,
        )
    except Exception as e:
        logger.error(f"Error detecting convergence: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to detect convergence: {str(e)}"
        )

    return ConvergenceResponse(
        quarter=report.quarter,
        quarter_prev=report.quarter_prev,
        total_investors_scanned=report.total_investors_scanned,
        total_signals=report.total_signals,
        signals=[
            ConvergenceSignalResponse(
                company_name=sig.company_name,
                cusip=sig.cusip,
                ticker=sig.ticker,
                signal_strength=sig.signal_strength,
                participant_count=sig.participant_count,
                new_position_count=sig.new_position_count,
                increased_position_count=sig.increased_position_count,
                total_value_added=_decimal_str(sig.total_value_added),
                participants=[
                    ConvergenceParticipantResponse(
                        investor_id=p.investor_id,
                        investor_type=p.investor_type,
                        investor_name=p.investor_name,
                        action=p.action,
                        shares_change=_decimal_str(p.shares_change),
                        value_change=_decimal_str(p.value_change),
                    )
                    for p in sig.participants
                ],
            )
            for sig in report.signals
        ],
    )
