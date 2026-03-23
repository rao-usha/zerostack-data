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


class PipelineAddRequest(BaseModel):
    target_mw: int = Field(50, ge=1, le=2000)
    notes: str = Field("", max_length=1000)
    status: str = Field("Evaluating")


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


@router.post("/{county_fips}/thesis")
async def generate_county_thesis(
    county_fips: str,
    db: Session = Depends(get_db),
):
    """Generate AI investment thesis for a scored county. Cached 24h per county."""
    import os
    from datetime import date
    from sqlalchemy import text as _text

    # Add thesis columns if they don't exist yet
    try:
        db.execute(_text("ALTER TABLE datacenter_site_scores ADD COLUMN IF NOT EXISTS thesis_text TEXT"))
        db.execute(_text("ALTER TABLE datacenter_site_scores ADD COLUMN IF NOT EXISTS thesis_generated_at TIMESTAMP"))
        db.commit()
    except Exception:
        db.rollback()

    # Fetch latest score for this county (base columns only)
    score_res = db.execute(
        _text("""
            SELECT county_fips, county_name, state, overall_score, grade,
                   power_score, connectivity_score, regulatory_score,
                   labor_score, risk_score, cost_incentive_score,
                   electricity_price_cents_kwh, power_capacity_nearby_mw,
                   substations_count, ix_count, tech_employment, tech_avg_wage,
                   incentive_program_count, opportunity_zone
            FROM datacenter_site_scores
            WHERE county_fips = :fips
            ORDER BY score_date DESC LIMIT 1
        """),
        {"fips": county_fips},
    )
    result = score_res.fetchone()

    if not result:
        raise HTTPException(status_code=404, detail=f"County {county_fips} not scored. Run score-counties first.")

    row = dict(zip(score_res.keys(), result))

    # Check for cached thesis separately (columns may have just been added)
    try:
        cached_res = db.execute(
            _text("""
                SELECT thesis_text, thesis_generated_at
                FROM datacenter_site_scores
                WHERE county_fips = :fips
                ORDER BY score_date DESC LIMIT 1
            """),
            {"fips": county_fips},
        ).fetchone()
        if cached_res and cached_res[1] and cached_res[0]:
            if cached_res[1].date() == date.today():
                return {
                    "county_fips": row["county_fips"],
                    "county_name": row["county_name"],
                    "state": row["state"],
                    "overall_score": float(row["overall_score"]),
                    "thesis": cached_res[0],
                    "generated_at": cached_res[1].isoformat(),
                    "from_cache": True,
                }
    except Exception:
        pass  # Columns not yet visible — proceed to generate

    # Check for LLM API key
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {
            "county_fips": county_fips,
            "county_name": row["county_name"],
            "state": row["state"],
            "overall_score": float(row["overall_score"]),
            "thesis": None,
            "error": "LLM not configured — set OPENAI_API_KEY or ANTHROPIC_API_KEY",
        }

    # Build prompt with actual score data
    ozone = "Yes" if row.get("opportunity_zone") else "No"
    prompt = f"""You are a real estate investment analyst. Write a concise investment thesis for a datacenter development site in {row['county_name']} County, {row['state']}.

Site Scores (0-100 scale):
- Overall: {row['overall_score']} (Grade {row['grade']})
- Power Infrastructure (30% weight): {row['power_score']} — {row.get('power_capacity_nearby_mw') or 'N/A'} MW nearby, {row.get('substations_count') or 'N/A'} substations, {row.get('electricity_price_cents_kwh') or 'N/A'}¢/kWh electricity
- Connectivity (20% weight): {row['connectivity_score']} — {row.get('ix_count') or 'N/A'} internet exchanges nearby
- Regulatory Speed (20% weight): {row['regulatory_score']}
- Labor Workforce (15% weight): {row['labor_score']} — {row.get('tech_employment') or 'N/A'} tech workers, avg wage ${row.get('tech_avg_wage') or 'N/A'}
- Risk/Environment (10% weight): {row['risk_score']}
- Cost/Incentives (5% weight): {row['cost_incentive_score']} — {row.get('incentive_program_count') or 'N/A'} incentive programs, Opportunity Zone: {ozone}

Write exactly 3 paragraphs (~200 words total):
1. Why this site is compelling for datacenter investment (lead with the strongest scores)
2. Key risks to underwrite before committing capital
3. Recommended next steps for due diligence

Reference the actual scores and metrics. Be specific and analytical."""

    try:
        from app.agentic.llm_client import LLMClient
        provider = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic"
        client = LLMClient(provider=provider, api_key=api_key, max_tokens=600, temperature=0.3)
        response = await client.complete(prompt=prompt)
        thesis_text = response.content.strip()

        # Cache to DB
        db.execute(
            _text("""
                UPDATE datacenter_site_scores
                SET thesis_text = :thesis, thesis_generated_at = NOW()
                WHERE county_fips = :fips
                AND score_date = (SELECT MAX(score_date) FROM datacenter_site_scores WHERE county_fips = :fips2)
            """),
            {"thesis": thesis_text, "fips": county_fips, "fips2": county_fips},
        )
        db.commit()

        return {
            "county_fips": county_fips,
            "county_name": row["county_name"],
            "state": row["state"],
            "overall_score": float(row["overall_score"]),
            "thesis": thesis_text,
            "generated_at": "now",
            "from_cache": False,
        }

    except Exception as e:
        logger.warning(f"Thesis LLM call failed for {county_fips}: {e}")
        return {
            "county_fips": county_fips,
            "county_name": row["county_name"],
            "state": row["state"],
            "overall_score": float(row["overall_score"]),
            "thesis": None,
            "error": f"LLM call failed: {str(e)}",
        }


@router.post("/pipeline/{county_fips}")
def add_to_pipeline(
    county_fips: str,
    request: PipelineAddRequest,
    db: Session = Depends(get_db),
):
    """Add or update a county in the site selection pipeline. Idempotent."""
    from sqlalchemy import text as _text

    score_result = db.execute(
        _text("""
            SELECT county_fips, county_name, state, overall_score, grade
            FROM datacenter_site_scores
            WHERE county_fips = :fips
            ORDER BY score_date DESC LIMIT 1
        """),
        {"fips": county_fips},
    )
    score_row = score_result.fetchone()

    if not score_row:
        raise HTTPException(
            status_code=404,
            detail=f"County {county_fips} not scored. Run score-counties first.",
        )

    score = dict(zip(score_result.keys(), score_row))

    db.execute(
        _text("""
            INSERT INTO datacenter_site_pipeline
                (county_fips, county_name, state, overall_score, grade,
                 status, notes, target_mw, added_at, updated_at)
            VALUES
                (:fips, :name, :state, :score, :grade,
                 :status, :notes, :mw, NOW(), NOW())
            ON CONFLICT (county_fips) DO UPDATE SET
                status = EXCLUDED.status,
                notes = EXCLUDED.notes,
                target_mw = EXCLUDED.target_mw,
                updated_at = NOW()
        """),
        {
            "fips": county_fips,
            "name": score["county_name"],
            "state": score["state"],
            "score": float(score["overall_score"]),
            "grade": score["grade"],
            "status": request.status,
            "notes": request.notes,
            "mw": request.target_mw,
        },
    )
    db.commit()

    return {
        "county_fips": county_fips,
        "county_name": score["county_name"],
        "state": score["state"],
        "overall_score": float(score["overall_score"]),
        "grade": score["grade"],
        "status": request.status,
        "target_mw": request.target_mw,
        "notes": request.notes,
    }


@router.get("/pipeline")
def get_pipeline(
    status: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get shortlisted sites in the selection pipeline."""
    from sqlalchemy import text as _text

    conditions = ["1=1"]
    params: Dict[str, Any] = {}
    if status:
        conditions.append("status = :status")
        params["status"] = status
    if state:
        conditions.append("state = :state")
        params["state"] = state.upper()

    where = " AND ".join(conditions)
    try:
        result = db.execute(
            _text(f"""
                SELECT county_fips, county_name, state, overall_score, grade,
                       status, notes, target_mw, added_at, updated_at
                FROM datacenter_site_pipeline
                WHERE {where}
                ORDER BY overall_score DESC
            """),
            params,
        )
        rows = [dict(zip(result.keys(), r)) for r in result.fetchall()]
        for r in rows:
            if r.get("added_at"):
                r["added_at"] = r["added_at"].isoformat()
            if r.get("updated_at"):
                r["updated_at"] = r["updated_at"].isoformat()
        return rows
    except Exception as e:
        logger.warning(f"Pipeline query failed: {e}")
        return []


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
