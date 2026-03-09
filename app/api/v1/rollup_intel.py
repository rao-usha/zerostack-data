"""
Roll-Up Market Intelligence — REST API.

Endpoints for market screening, rankings, add-on target finding, and
Census CBP data collection.
"""

import asyncio
import threading
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ml.rollup_market_scorer import RollupMarketScorer
from app.ml.addon_target_finder import AddonTargetFinder

router = APIRouter(prefix="/rollup-intel", tags=["Roll-Up Intelligence"])


# ------------------------------------------------------------------
# Methodology
# ------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="Roll-up market scoring methodology",
    response_description="Weights, sub-scores, data sources",
)
def get_methodology():
    return RollupMarketScorer.get_methodology()


# ------------------------------------------------------------------
# Screen markets (triggers CBP collection + scoring)
# ------------------------------------------------------------------

@router.post(
    "/screen",
    summary="Screen & score markets for roll-up attractiveness",
    response_description="Triggers CBP collection + scoring, returns summary",
)
def screen_markets(
    naics_code: str = Query(..., description="NAICS code (e.g. 621111)"),
    year: int = Query(2021, description="CBP data year"),
    state: Optional[str] = Query(None, description="2-digit state FIPS filter"),
    force: bool = Query(False, description="Re-score even if cached"),
    db: Session = Depends(get_db),
):
    """Collect CBP data (if not cached) and score all counties."""

    def _run_in_thread():
        from app.core.database import get_db as _get_db
        gen = _get_db()
        session = next(gen)
        try:
            # Step 1: Collect CBP data
            from app.sources.rollup_intel.cbp_collector import CBPCollector
            collector = CBPCollector(session)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    collector.collect(naics_code=naics_code, year=year, state=state, force=force)
                )
            finally:
                loop.close()

            # Step 2: Score markets
            scorer = RollupMarketScorer(session)
            scorer.score_markets(
                naics_code=naics_code, year=year, state=state, force=force
            )
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    threading.Thread(target=_run_in_thread, daemon=True).start()

    return {
        "status": "started",
        "message": (
            f"Collecting CBP data and scoring markets for NAICS {naics_code}. "
            f"Results will be available via GET /rollup-intel/rankings/{naics_code}"
        ),
        "naics_code": naics_code,
        "year": year,
        "state": state,
    }


# ------------------------------------------------------------------
# Rankings
# ------------------------------------------------------------------

@router.get(
    "/rankings/{naics_code}",
    summary="Get ranked counties for a NAICS code",
    response_description="Filtered, paginated list ordered by score",
)
def get_rankings(
    naics_code: str,
    state: Optional[str] = Query(None, description="2-digit state FIPS"),
    grade: Optional[str] = Query(None, description="Grade filter: A, B, C, D, F"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    scorer = RollupMarketScorer(db)
    return scorer.get_rankings(
        naics_code=naics_code,
        state=state,
        grade=grade,
        limit=limit,
        offset=offset,
    )


# ------------------------------------------------------------------
# Market detail
# ------------------------------------------------------------------

@router.get(
    "/market/{naics_code}/{county_fips}",
    summary="Get detailed score for a specific county + NAICS",
    response_description="Full score breakdown for one market",
)
def get_market_detail(
    naics_code: str,
    county_fips: str,
    db: Session = Depends(get_db),
):
    scorer = RollupMarketScorer(db)
    return scorer.get_market(naics_code=naics_code, county_fips=county_fips)


# ------------------------------------------------------------------
# Add-On Target Finder
# ------------------------------------------------------------------

@router.post(
    "/find-addons",
    summary="Find bolt-on acquisition markets near a portfolio company",
    response_description="Ranked add-on targets with proximity scoring",
)
def find_addon_targets(
    naics_code: str = Query(..., description="NAICS code"),
    state_fips: str = Query(..., description="2-digit state FIPS of portfolio company"),
    county_fips: Optional[str] = Query(None, description="5-digit county FIPS"),
    radius_states: int = Query(1, ge=0, le=3, description="Adjacency hops (0=same state only)"),
    limit: int = Query(25, ge=1, le=100),
    db: Session = Depends(get_db),
):
    finder = AddonTargetFinder(db)
    return finder.find_targets(
        naics_code=naics_code,
        state_fips=state_fips,
        county_fips=county_fips,
        radius_states=radius_states,
        limit=limit,
    )
