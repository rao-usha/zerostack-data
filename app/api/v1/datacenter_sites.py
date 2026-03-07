"""
Datacenter Site Selection API.

County-level scoring, rankings, and site analysis for datacenter development.
"""

import logging
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datacenter-sites", tags=["Datacenter Sites"])


# =============================================================================
# REQUEST MODELS
# =============================================================================


class ScoreCountiesRequest(BaseModel):
    state: Optional[str] = Field(None, description="Filter to a specific state (e.g. 'TX')")
    force: bool = Field(False, description="Force rescore even if scores exist for today")


class ScoreSiteRequest(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)


class CompareRequest(BaseModel):
    locations: List[Dict[str, Any]] = Field(
        ..., description="List of {name, latitude, longitude}"
    )


class ReportRequest(BaseModel):
    state: Optional[str] = None
    top_n: int = Field(20, ge=1, le=100)
    target_mw: int = Field(50, ge=1, le=1000)
    format: str = Field("html", description="html or excel")
    title: Optional[str] = None


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/methodology")
def get_methodology():
    """Get scoring weights and data sources."""
    from app.ml.datacenter_site_scorer import DatacenterSiteScorer
    from app.ml.county_regulatory_scorer import CountyRegulatoryScorer
    return {
        "site_suitability": DatacenterSiteScorer.get_methodology(),
        "regulatory_speed": CountyRegulatoryScorer.get_methodology(),
    }


@router.post("/score-counties")
def score_counties(
    request: ScoreCountiesRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Trigger batch county scoring (runs regulatory + site scorer)."""
    def _run_scoring(state: Optional[str], force: bool):
        from app.core.database import get_session_factory
        SessionLocal = get_session_factory()
        session = SessionLocal()
        try:
            from app.ml.county_regulatory_scorer import CountyRegulatoryScorer
            from app.ml.datacenter_site_scorer import DatacenterSiteScorer

            reg_scorer = CountyRegulatoryScorer(session)
            reg_result = reg_scorer.score_all_counties(force=force, state=state)
            logger.info(f"Regulatory scoring complete: {reg_result.get('total_counties', 0)} counties")

            site_scorer = DatacenterSiteScorer(session)
            site_result = site_scorer.score_all_counties(force=force, state=state)
            logger.info(f"Site scoring complete: {site_result.get('total_counties', 0)} counties")
        except Exception as e:
            logger.error(f"Scoring failed: {e}", exc_info=True)
        finally:
            session.close()

    background_tasks.add_task(_run_scoring, request.state, request.force)
    return {
        "status": "scoring_started",
        "state": request.state,
        "force": request.force,
        "message": "Scoring started in background. Check /rankings for results.",
    }


@router.get("/rankings")
def get_rankings(
    state: Optional[str] = Query(None, description="Filter by state"),
    grade: Optional[str] = Query(None, description="Filter by grade (A, B, C, D, F)"),
    min_score: Optional[float] = Query(None, description="Minimum overall score"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Get ranked counties with filters."""
    from sqlalchemy import text

    conditions = [
        "score_date = (SELECT MAX(score_date) FROM datacenter_site_scores)"
    ]
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if state:
        conditions.append("state = :state")
        params["state"] = state.upper()
    if grade:
        conditions.append("grade = :grade")
        params["grade"] = grade.upper()
    if min_score is not None:
        conditions.append("overall_score >= :min_score")
        params["min_score"] = min_score

    where = " AND ".join(conditions)

    try:
        count_result = db.execute(
            text(f"SELECT COUNT(*) FROM datacenter_site_scores WHERE {where}"),
            params,
        )
        total = count_result.scalar() or 0

        result = db.execute(
            text(f"""
                SELECT county_fips, county_name, state, overall_score, grade,
                       power_score, connectivity_score, regulatory_score,
                       labor_score, risk_score, cost_incentive_score,
                       national_rank, state_rank
                FROM datacenter_site_scores
                WHERE {where}
                ORDER BY overall_score DESC
                LIMIT :limit OFFSET :offset
            """),
            params,
        )
        counties = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        return {
            "counties": counties,
            "total": total,
            "has_more": (offset + limit) < total,
        }
    except Exception as e:
        logger.warning(f"Rankings query failed: {e}")
        return {"counties": [], "total": 0, "has_more": False}


@router.post("/score-site")
def score_site(
    request: ScoreSiteRequest,
    db: Session = Depends(get_db),
):
    """Score a specific lat/lng location."""
    from app.ml.datacenter_site_scorer import DatacenterSiteScorer

    scorer = DatacenterSiteScorer(db)
    result = scorer.score_single_site(request.latitude, request.longitude)
    if not result:
        raise HTTPException(
            status_code=404,
            detail="No scores available. Run score-counties first.",
        )
    return result


@router.post("/compare")
def compare_sites(
    request: CompareRequest,
    db: Session = Depends(get_db),
):
    """Compare multiple locations side-by-side."""
    from app.ml.datacenter_site_scorer import DatacenterSiteScorer

    scorer = DatacenterSiteScorer(db)
    results = scorer.compare_sites(request.locations)
    return {
        "locations": results,
        "best": results[0] if results else None,
        "total": len(results),
    }


@router.get("/top-states")
def get_top_states(
    limit: int = Query(15, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """State-level aggregation of county scores."""
    from sqlalchemy import text

    try:
        result = db.execute(
            text("""
                SELECT state,
                       ROUND(AVG(overall_score)::numeric, 1) as avg_score,
                       COUNT(*) as county_count,
                       COUNT(CASE WHEN grade = 'A' THEN 1 END) as a_grade,
                       COUNT(CASE WHEN grade IN ('A','B') THEN 1 END) as ab_grade,
                       ROUND(MAX(overall_score)::numeric, 1) as max_score,
                       ROUND(MIN(overall_score)::numeric, 1) as min_score
                FROM datacenter_site_scores
                WHERE score_date = (SELECT MAX(score_date) FROM datacenter_site_scores)
                GROUP BY state
                ORDER BY avg_score DESC
                LIMIT :limit
            """),
            {"limit": limit},
        )
        states = [dict(zip(result.keys(), row)) for row in result.fetchall()]
        return {"states": states, "total": len(states)}
    except Exception:
        return {"states": [], "total": 0}


@router.get("/data-sources")
def get_data_sources(db: Session = Depends(get_db)):
    """Get collection freshness per source."""
    from sqlalchemy import text

    tables = [
        ("power_plant", "EIA Power Plants"),
        ("substation", "HIFLD Substations"),
        ("data_center_facility", "PeeringDB"),
        ("broadband_availability", "FCC Broadband"),
        ("building_permit", "Census Building Permits"),
        ("government_unit", "Census of Governments"),
        ("industry_employment", "BLS QCEW"),
        ("flood_zone", "FEMA NRI"),
        ("brownfield_site", "EPA ACRES"),
        ("incentive_deal", "Good Jobs First"),
        ("renewable_resource", "NREL Solar/Wind"),
        ("epoch_datacenter", "Epoch AI DCs"),
        ("industrial_site", "Industrial Sites"),
    ]

    sources = []
    for table_name, display_name in tables:
        try:
            result = db.execute(
                text(f"SELECT COUNT(*), MAX(collected_at) FROM {table_name}")
            )
            row = result.fetchone()
            sources.append({
                "name": display_name,
                "table": table_name,
                "row_count": row[0] if row else 0,
                "last_collected": row[1].isoformat() if row and row[1] else None,
            })
        except Exception:
            sources.append({
                "name": display_name,
                "table": table_name,
                "row_count": 0,
                "last_collected": None,
            })

    return {"sources": sources}


# /{county_fips} MUST be last — it's a catch-all path parameter
@router.get("/{county_fips}")
def get_county_detail(
    county_fips: str,
    db: Session = Depends(get_db),
):
    """Get detailed score breakdown for one county."""
    from sqlalchemy import text

    try:
        result = db.execute(
            text("""
                SELECT *
                FROM datacenter_site_scores
                WHERE county_fips = :fips
                ORDER BY score_date DESC
                LIMIT 1
            """),
            {"fips": county_fips},
        )
        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"County {county_fips} not found")
        return dict(zip(result.keys(), row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/report")
def generate_report(
    request: ReportRequest,
    db: Session = Depends(get_db),
):
    """Generate full datacenter site selection report."""
    from app.reports.builder import ReportBuilder

    builder = ReportBuilder(db)
    try:
        result = builder.generate(
            template_name="datacenter_site",
            format=request.format,
            params={
                "state": request.state,
                "top_n": request.top_n,
                "target_mw": request.target_mw,
            },
            title=request.title or "Datacenter Site Selection Report",
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
