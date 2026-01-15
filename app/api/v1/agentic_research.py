"""
Agentic Portfolio Research API endpoints.

Provides endpoints for triggering and monitoring portfolio collection
for LPs and Family Offices.
"""
import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from fastapi.responses import Response

from app.core.database import get_db
from app.agentic.portfolio_agent import PortfolioResearchAgent, InvestorContext
from app.agentic.metrics import get_metrics_collector
from app.agentic.exporter import export_portfolio, ExportFormat

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agentic", tags=["Agentic Portfolio Research"])


# =============================================================================
# Request/Response Models
# =============================================================================


class PortfolioCollectionRequest(BaseModel):
    """Request model for triggering portfolio collection."""
    
    investor_id: int = Field(..., description="Investor ID (lp_fund.id or family_offices.id)")
    investor_type: str = Field(..., description="'lp' or 'family_office'")
    strategies: Optional[List[str]] = Field(
        None, 
        description="Specific strategies to use (if None, agent decides). "
                    "Options: 'sec_13f', 'website_scraping'"
    )


class BatchCollectionRequest(BaseModel):
    """Request model for batch portfolio collection."""
    
    investor_type: str = Field(..., description="'lp' or 'family_office'")
    limit: int = Field(10, ge=1, le=100, description="Max investors to process")
    skip_existing: bool = Field(
        True, 
        description="Skip investors that already have portfolio data"
    )


class PortfolioSummaryResponse(BaseModel):
    """Response model for portfolio summary."""
    
    investor_id: int
    investor_type: str
    investor_name: str
    total_companies: int
    sources_breakdown: dict
    top_industries: List[dict]
    co_investors_count: int
    data_completeness_score: float
    last_updated: Optional[str]


class JobStatusResponse(BaseModel):
    """Response model for job status."""
    
    job_id: int
    status: str
    investor_name: Optional[str]
    started_at: Optional[str]
    completed_at: Optional[str]
    companies_found: Optional[int]
    strategies_used: Optional[List[str]]
    reasoning_summary: Optional[str]


# =============================================================================
# Background Tasks
# =============================================================================


async def run_portfolio_collection(
    investor_id: int,
    investor_type: str,
    strategies: Optional[List[str]],
    job_id: int,
    db_url: str
):
    """Background task to run portfolio collection."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Create new session for background task
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    
    try:
        # Update job status to running
        db.execute(
            text("UPDATE agentic_collection_jobs SET status = 'running', started_at = NOW() WHERE id = :job_id"),
            {"job_id": job_id}
        )
        db.commit()
        
        # Get investor info
        if investor_type == "lp":
            investor_query = text("""
                SELECT id, name, formal_name, lp_type, jurisdiction, website_url 
                FROM lp_fund WHERE id = :investor_id
            """)
        else:
            investor_query = text("""
                SELECT id, name, legal_name, NULL as lp_type, state_province, website 
                FROM family_offices WHERE id = :investor_id
            """)
        
        investor_row = db.execute(investor_query, {"investor_id": investor_id}).fetchone()
        
        if not investor_row:
            db.execute(
                text("""
                    UPDATE agentic_collection_jobs 
                    SET status = 'failed', 
                        completed_at = NOW(),
                        errors = :errors
                    WHERE id = :job_id
                """),
                {
                    "job_id": job_id,
                    "errors": '{"error": "Investor not found"}'
                }
            )
            db.commit()
            return
        
        # Create investor context
        context = InvestorContext(
            investor_id=investor_row[0],
            investor_type=investor_type,
            investor_name=investor_row[1],
            formal_name=investor_row[2],
            lp_type=investor_row[3],
            jurisdiction=investor_row[4],
            website_url=investor_row[5]
        )
        
        # Run agent
        agent = PortfolioResearchAgent(db)
        result = await agent.collect_portfolio(
            context=context,
            strategies_to_use=strategies,
            job_id=job_id
        )
        
        # Update job with results
        import json
        db.execute(
            text("""
                UPDATE agentic_collection_jobs 
                SET 
                    status = :status,
                    completed_at = NOW(),
                    sources_checked = :sources_checked,
                    sources_successful = :sources_successful,
                    companies_found = :companies_found,
                    new_companies = :new_companies,
                    updated_companies = :updated_companies,
                    strategies_used = :strategies_used,
                    reasoning_log = :reasoning_log,
                    errors = :errors,
                    warnings = :warnings,
                    requests_made = :requests_made,
                    tokens_used = :tokens_used
                WHERE id = :job_id
            """),
            {
                "job_id": job_id,
                "status": result["status"],
                "sources_checked": result["sources_checked"],
                "sources_successful": result["sources_successful"],
                "companies_found": result["companies_found"],
                "new_companies": result["new_companies"],
                "updated_companies": result["updated_companies"],
                "strategies_used": json.dumps(result["strategies_used"]),
                "reasoning_log": json.dumps(result["reasoning_log"]),
                "errors": json.dumps(result["errors"]) if result["errors"] else None,
                "warnings": json.dumps(result["warnings"]) if result["warnings"] else None,
                "requests_made": result["requests_made"],
                "tokens_used": result["tokens_used"]
            }
        )
        db.commit()
        
        logger.info(f"Portfolio collection completed for {context.investor_name}: {result['companies_found']} companies")
    
    except Exception as e:
        logger.error(f"Error in background portfolio collection: {e}", exc_info=True)
        import json
        db.execute(
            text("""
                UPDATE agentic_collection_jobs 
                SET 
                    status = 'failed',
                    completed_at = NOW(),
                    errors = :errors
                WHERE id = :job_id
            """),
            {
                "job_id": job_id,
                "errors": json.dumps([{"error": str(e)}])
            }
        )
        db.commit()
    
    finally:
        db.close()


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/portfolio/collect", response_model=dict)
async def trigger_portfolio_collection(
    request: PortfolioCollectionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    🔍 Trigger agentic portfolio collection for a single investor.
    
    This starts a background job that:
    1. Plans which collection strategies to use based on investor type
    2. Executes strategies (SEC 13F, website scraping, etc.)
    3. Deduplicates and synthesizes findings
    4. Stores results to portfolio_companies table
    
    **Investor Types:**
    - `lp`: Limited Partner (use ID from lp_fund table)
    - `family_office`: Family Office (use ID from family_offices table)
    
    **Available Strategies:**
    - `sec_13f`: Extract holdings from SEC 13F filings (high confidence)
    - `website_scraping`: Scrape portfolio pages from investor website
    
    **Example Request:**
    ```json
    {
        "investor_id": 1,
        "investor_type": "lp",
        "strategies": null  // Let agent decide
    }
    ```
    
    **Returns:** Job ID for tracking progress
    """
    try:
        # Validate investor exists
        if request.investor_type == "lp":
            check_query = text("SELECT name FROM lp_fund WHERE id = :investor_id")
        elif request.investor_type == "family_office":
            check_query = text("SELECT name FROM family_offices WHERE id = :investor_id")
        else:
            raise HTTPException(
                status_code=400, 
                detail="investor_type must be 'lp' or 'family_office'"
            )
        
        investor_row = db.execute(check_query, {"investor_id": request.investor_id}).fetchone()
        
        if not investor_row:
            raise HTTPException(
                status_code=404,
                detail=f"Investor not found: {request.investor_type} id={request.investor_id}"
            )
        
        investor_name = investor_row[0]
        
        # Create job record
        import json
        result = db.execute(
            text("""
                INSERT INTO agentic_collection_jobs (
                    job_type, target_investor_id, target_investor_type, 
                    target_investor_name, status, strategies_used
                ) VALUES (
                    'portfolio_discovery', :investor_id, :investor_type,
                    :investor_name, 'pending', :strategies
                )
                RETURNING id
            """),
            {
                "investor_id": request.investor_id,
                "investor_type": request.investor_type,
                "investor_name": investor_name,
                "strategies": json.dumps(request.strategies) if request.strategies else None
            }
        )
        job_id = result.fetchone()[0]
        db.commit()
        
        # Get database URL for background task
        from app.core.config import get_settings
        settings = get_settings()
        
        # Queue background task
        background_tasks.add_task(
            run_portfolio_collection,
            request.investor_id,
            request.investor_type,
            request.strategies,
            job_id,
            settings.database_url
        )
        
        return {
            "job_id": job_id,
            "status": "pending",
            "investor_id": request.investor_id,
            "investor_type": request.investor_type,
            "investor_name": investor_name,
            "message": "Portfolio collection job started. Use GET /api/v1/agentic/jobs/{job_id} to check status."
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering portfolio collection: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/portfolio/batch", response_model=dict)
async def batch_portfolio_collection(
    request: BatchCollectionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    📦 Batch collection for multiple investors.
    
    Automatically prioritizes investors with missing portfolio data.
    Creates one job per investor, up to the specified limit.
    
    **Example Request:**
    ```json
    {
        "investor_type": "lp",
        "limit": 10,
        "skip_existing": true
    }
    ```
    """
    try:
        # Get investors to process
        if request.investor_type == "lp":
            if request.skip_existing:
                query = text("""
                    SELECT lf.id, lf.name 
                    FROM lp_fund lf
                    LEFT JOIN portfolio_companies pc 
                        ON pc.investor_id = lf.id AND pc.investor_type = 'lp'
                    WHERE pc.id IS NULL
                    ORDER BY lf.id
                    LIMIT :limit
                """)
            else:
                query = text("""
                    SELECT id, name FROM lp_fund ORDER BY id LIMIT :limit
                """)
        else:
            if request.skip_existing:
                query = text("""
                    SELECT fo.id, fo.name 
                    FROM family_offices fo
                    LEFT JOIN portfolio_companies pc 
                        ON pc.investor_id = fo.id AND pc.investor_type = 'family_office'
                    WHERE pc.id IS NULL
                    ORDER BY fo.id
                    LIMIT :limit
                """)
            else:
                query = text("""
                    SELECT id, name FROM family_offices ORDER BY id LIMIT :limit
                """)
        
        investors = db.execute(query, {"limit": request.limit}).fetchall()
        
        if not investors:
            return {
                "message": "No investors to process",
                "jobs_created": 0,
                "job_ids": []
            }
        
        # Create jobs for each investor
        job_ids = []
        import json
        
        for investor_id, investor_name in investors:
            result = db.execute(
                text("""
                    INSERT INTO agentic_collection_jobs (
                        job_type, target_investor_id, target_investor_type, 
                        target_investor_name, status
                    ) VALUES (
                        'portfolio_discovery', :investor_id, :investor_type,
                        :investor_name, 'pending'
                    )
                    RETURNING id
                """),
                {
                    "investor_id": investor_id,
                    "investor_type": request.investor_type,
                    "investor_name": investor_name
                }
            )
            job_id = result.fetchone()[0]
            job_ids.append(job_id)
            
            # Get database URL
            from app.core.config import get_settings
            settings = get_settings()
            
            # Queue background task
            background_tasks.add_task(
                run_portfolio_collection,
                investor_id,
                request.investor_type,
                None,  # Let agent decide strategies
                job_id,
                settings.database_url
            )
        
        db.commit()
        
        return {
            "message": f"Batch collection started for {len(job_ids)} investors",
            "jobs_created": len(job_ids),
            "job_ids": job_ids,
            "investors": [
                {"id": inv[0], "name": inv[1]} 
                for inv in investors
            ]
        }
    
    except Exception as e:
        logger.error(f"Error starting batch collection: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{investor_id}/summary", response_model=dict)
async def get_portfolio_summary(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'"),
    db: Session = Depends(get_db)
):
    """
    📊 Get portfolio summary for an investor.
    
    Returns:
    - Total companies found
    - Source breakdown (which strategies found data)
    - Top industries
    - Co-investors
    - Data completeness score
    """
    try:
        # Get investor info
        if investor_type == "lp":
            investor_query = text("SELECT name FROM lp_fund WHERE id = :investor_id")
        else:
            investor_query = text("SELECT name FROM family_offices WHERE id = :investor_id")
        
        investor_row = db.execute(investor_query, {"investor_id": investor_id}).fetchone()
        
        if not investor_row:
            raise HTTPException(status_code=404, detail="Investor not found")
        
        investor_name = investor_row[0]
        
        # Get portfolio companies
        companies_result = db.execute(
            text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT source_type) as source_count,
                    MAX(collected_date) as last_updated
                FROM portfolio_companies
                WHERE investor_id = :investor_id AND investor_type = :investor_type
            """),
            {"investor_id": investor_id, "investor_type": investor_type}
        ).fetchone()
        
        total_companies = companies_result[0] or 0
        source_count = companies_result[1] or 0
        last_updated = companies_result[2]
        
        # Get source breakdown
        source_breakdown_result = db.execute(
            text("""
                SELECT source_type, COUNT(*) as count
                FROM portfolio_companies
                WHERE investor_id = :investor_id AND investor_type = :investor_type
                GROUP BY source_type
                ORDER BY count DESC
            """),
            {"investor_id": investor_id, "investor_type": investor_type}
        ).fetchall()
        
        source_breakdown = {row[0]: row[1] for row in source_breakdown_result}
        
        # Get top industries
        industries_result = db.execute(
            text("""
                SELECT company_industry, COUNT(*) as count
                FROM portfolio_companies
                WHERE investor_id = :investor_id 
                    AND investor_type = :investor_type
                    AND company_industry IS NOT NULL
                GROUP BY company_industry
                ORDER BY count DESC
                LIMIT 5
            """),
            {"investor_id": investor_id, "investor_type": investor_type}
        ).fetchall()
        
        top_industries = [
            {"industry": row[0], "count": row[1]} 
            for row in industries_result
        ]
        
        # Get co-investors count
        co_investors_result = db.execute(
            text("""
                SELECT COUNT(DISTINCT co_investor_name)
                FROM co_investments
                WHERE primary_investor_id = :investor_id 
                    AND primary_investor_type = :investor_type
            """),
            {"investor_id": investor_id, "investor_type": investor_type}
        ).fetchone()
        
        co_investors_count = co_investors_result[0] or 0
        
        # Calculate completeness score (0-100)
        # Based on: has data, multiple sources, industries classified, recent update
        score = 0
        if total_companies > 0:
            score += 30
        if total_companies >= 10:
            score += 20
        if source_count >= 2:
            score += 20
        if len(top_industries) >= 3:
            score += 15
        if co_investors_count > 0:
            score += 15
        
        return {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "investor_name": investor_name,
            "total_companies": total_companies,
            "sources_breakdown": source_breakdown,
            "top_industries": top_industries,
            "co_investors_count": co_investors_count,
            "data_completeness_score": min(score, 100),
            "last_updated": last_updated.isoformat() if last_updated else None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting portfolio summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{investor_id}/companies", response_model=dict)
async def get_portfolio_companies(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    db: Session = Depends(get_db)
):
    """
    📋 Get portfolio companies for an investor.
    
    Returns paginated list of portfolio holdings.
    """
    try:
        # Build query
        query = """
            SELECT 
                id, company_name, company_industry, company_stage,
                investment_type, investment_date, market_value_usd, shares_held,
                source_type, confidence_level, source_url, collected_date
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
        """
        params = {
            "investor_id": investor_id, 
            "investor_type": investor_type,
            "limit": limit,
            "offset": offset
        }
        
        if source_type:
            query += " AND source_type = :source_type"
            params["source_type"] = source_type
        
        query += " ORDER BY collected_date DESC LIMIT :limit OFFSET :offset"
        
        result = db.execute(text(query), params).fetchall()
        
        companies = []
        for row in result:
            companies.append({
                "id": row[0],
                "company_name": row[1],
                "company_industry": row[2],
                "company_stage": row[3],
                "investment_type": row[4],
                "investment_date": row[5].isoformat() if row[5] else None,
                "market_value_usd": row[6],
                "shares_held": row[7],
                "source_type": row[8],
                "confidence_level": row[9],
                "source_url": row[10],
                "collected_date": row[11].isoformat() if row[11] else None
            })
        
        # Get total count
        count_result = db.execute(
            text("""
                SELECT COUNT(*) FROM portfolio_companies
                WHERE investor_id = :investor_id AND investor_type = :investor_type
            """),
            {"investor_id": investor_id, "investor_type": investor_type}
        ).fetchone()
        
        return {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "total": count_result[0],
            "limit": limit,
            "offset": offset,
            "companies": companies
        }
    
    except Exception as e:
        logger.error(f"Error getting portfolio companies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{investor_id}/export")
async def export_portfolio_data(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'"),
    format: str = Query("csv", description="Export format: 'csv' or 'xlsx'"),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    db: Session = Depends(get_db)
):
    """
    📥 Export portfolio data to CSV or Excel.

    **Formats:**
    - `csv`: Simple comma-separated values (UTF-8 with BOM for Excel compatibility)
    - `xlsx`: Excel workbook with multiple sheets and formatting

    **Excel Export Includes:**
    - **Portfolio** sheet: All holdings with full details
    - **Summary** sheet: Key metrics and top industries
    - **By Source** sheet: Breakdown by data source

    **Example Usage:**
    ```
    GET /api/v1/agentic/portfolio/123/export?investor_type=lp&format=xlsx
    ```

    Returns file download with appropriate content-type headers.
    """
    try:
        # Validate format
        try:
            export_format = ExportFormat(format.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid format '{format}'. Use 'csv' or 'xlsx'"
            )

        # Get investor info
        if investor_type == "lp":
            investor_query = text("SELECT name FROM lp_fund WHERE id = :investor_id")
        elif investor_type == "family_office":
            investor_query = text("SELECT name FROM family_offices WHERE id = :investor_id")
        else:
            raise HTTPException(
                status_code=400,
                detail="investor_type must be 'lp' or 'family_office'"
            )

        investor_row = db.execute(investor_query, {"investor_id": investor_id}).fetchone()

        if not investor_row:
            raise HTTPException(status_code=404, detail="Investor not found")

        investor_name = investor_row[0]

        # Build query for portfolio companies
        query = """
            SELECT
                id, company_name, company_industry, company_stage,
                investment_type, investment_date, market_value_usd, shares_held,
                ownership_percentage, source_type, confidence_level, source_url,
                collected_date, ticker_symbol, cusip, company_description
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
        """
        params = {"investor_id": investor_id, "investor_type": investor_type}

        if source_type:
            query += " AND source_type = :source_type"
            params["source_type"] = source_type

        query += " ORDER BY company_name"

        rows = db.execute(text(query), params).fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No portfolio data found for {investor_type} {investor_id}"
            )

        # Export data
        content, filename, content_type = export_portfolio(
            rows=rows,
            investor_name=investor_name,
            investor_type=investor_type,
            investor_id=investor_id,
            format=export_format,
        )

        return Response(
            content=content,
            media_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(content)),
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting portfolio: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=dict)
async def get_job_status(
    job_id: int,
    db: Session = Depends(get_db)
):
    """
    📈 Get detailed status of a collection job.
    
    Includes:
    - Current status (pending, running, success, failed)
    - Strategies used and their results
    - Full agent reasoning log
    - Resource usage (requests, tokens)
    """
    try:
        result = db.execute(
            text("""
                SELECT 
                    id, job_type, target_investor_id, target_investor_type,
                    target_investor_name, status, started_at, completed_at,
                    sources_checked, sources_successful, companies_found,
                    new_companies, updated_companies, strategies_used,
                    reasoning_log, errors, warnings, requests_made, tokens_used,
                    cost_usd, created_at
                FROM agentic_collection_jobs
                WHERE id = :job_id
            """),
            {"job_id": job_id}
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        import json
        
        return {
            "job_id": result[0],
            "job_type": result[1],
            "target": {
                "investor_id": result[2],
                "investor_type": result[3],
                "investor_name": result[4]
            },
            "status": result[5],
            "timing": {
                "started_at": result[6].isoformat() if result[6] else None,
                "completed_at": result[7].isoformat() if result[7] else None,
                "created_at": result[20].isoformat() if result[20] else None
            },
            "results": {
                "sources_checked": result[8],
                "sources_successful": result[9],
                "companies_found": result[10],
                "new_companies": result[11],
                "updated_companies": result[12]
            },
            "strategies_used": json.loads(result[13]) if result[13] else [],
            "reasoning_log": json.loads(result[14]) if result[14] else [],
            "errors": json.loads(result[15]) if result[15] else [],
            "warnings": json.loads(result[16]) if result[16] else [],
            "resource_usage": {
                "requests_made": result[17],
                "tokens_used": result[18],
                "cost_usd": result[19]
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs", response_model=dict)
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    investor_type: Optional[str] = Query(None, description="Filter by investor type"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    📋 List agentic collection jobs.
    
    Returns paginated list of jobs with filtering options.
    """
    try:
        query = """
            SELECT 
                id, job_type, target_investor_name, target_investor_type,
                status, companies_found, started_at, completed_at, created_at
            FROM agentic_collection_jobs
            WHERE 1=1
        """
        params = {"limit": limit, "offset": offset}
        
        if status:
            query += " AND status = :status"
            params["status"] = status
        
        if investor_type:
            query += " AND target_investor_type = :investor_type"
            params["investor_type"] = investor_type
        
        query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        
        result = db.execute(text(query), params).fetchall()
        
        jobs = []
        for row in result:
            jobs.append({
                "job_id": row[0],
                "job_type": row[1],
                "investor_name": row[2],
                "investor_type": row[3],
                "status": row[4],
                "companies_found": row[5],
                "started_at": row[6].isoformat() if row[6] else None,
                "completed_at": row[7].isoformat() if row[7] else None,
                "created_at": row[8].isoformat() if row[8] else None
            })
        
        # Get total count
        count_query = "SELECT COUNT(*) FROM agentic_collection_jobs WHERE 1=1"
        count_params = {}
        if status:
            count_query += " AND status = :status"
            count_params["status"] = status
        if investor_type:
            count_query += " AND target_investor_type = :investor_type"
            count_params["investor_type"] = investor_type
        
        count_result = db.execute(text(count_query), count_params).fetchone()
        
        return {
            "total": count_result[0],
            "limit": limit,
            "offset": offset,
            "jobs": jobs
        }
    
    except Exception as e:
        logger.error(f"Error listing jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/co-investors/{investor_id}", response_model=dict)
async def get_co_investors(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'"),
    min_count: int = Query(1, ge=1, description="Minimum co-investment count"),
    db: Session = Depends(get_db)
):
    """
    🤝 Find investors who frequently co-invest with this investor.
    
    Returns list of co-investors sorted by number of shared deals.
    """
    try:
        result = db.execute(
            text("""
                SELECT 
                    co_investor_name, co_investor_type,
                    SUM(co_investment_count) as total_deals,
                    array_agg(DISTINCT deal_name) as deals
                FROM co_investments
                WHERE primary_investor_id = :investor_id 
                    AND primary_investor_type = :investor_type
                GROUP BY co_investor_name, co_investor_type
                HAVING SUM(co_investment_count) >= :min_count
                ORDER BY total_deals DESC
            """),
            {
                "investor_id": investor_id,
                "investor_type": investor_type,
                "min_count": min_count
            }
        ).fetchall()
        
        co_investors = []
        for row in result:
            co_investors.append({
                "name": row[0],
                "type": row[1],
                "total_deals": row[2],
                "deals": row[3] if row[3] else []
            })
        
        return {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "co_investors_count": len(co_investors),
            "co_investors": co_investors
        }
    
    except Exception as e:
        logger.error(f"Error getting co-investors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/themes/{investor_id}", response_model=dict)
async def get_investor_themes(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'"),
    db: Session = Depends(get_db)
):
    """
    🎯 Get investment themes for an investor.
    
    Returns classified investment patterns:
    - Sectors (technology, healthcare, etc.)
    - Stages (seed, growth, etc.)
    - Geography (US, Europe, etc.)
    """
    try:
        result = db.execute(
            text("""
                SELECT 
                    theme_category, theme_value,
                    investment_count, percentage_of_portfolio,
                    confidence_level
                FROM investor_themes
                WHERE investor_id = :investor_id 
                    AND investor_type = :investor_type
                ORDER BY theme_category, investment_count DESC
            """),
            {"investor_id": investor_id, "investor_type": investor_type}
        ).fetchall()
        
        themes_by_category = {}
        for row in result:
            category = row[0]
            if category not in themes_by_category:
                themes_by_category[category] = []
            
            themes_by_category[category].append({
                "value": row[1],
                "investment_count": row[2],
                "percentage": row[3],
                "confidence": row[4]
            })
        
        return {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "themes": themes_by_category
        }
    
    except Exception as e:
        logger.error(f"Error getting investor themes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/overview", response_model=dict)
async def get_portfolio_stats(db: Session = Depends(get_db)):
    """
    📊 Get overall portfolio collection statistics.

    Returns:
    - Total LPs and FOs with portfolio data
    - Total companies tracked
    - Source breakdown
    - Collection job statistics
    """
    try:
        # Count investors with portfolio data
        lp_count = db.execute(
            text("""
                SELECT COUNT(DISTINCT investor_id)
                FROM portfolio_companies
                WHERE investor_type = 'lp'
            """)
        ).fetchone()[0]

        fo_count = db.execute(
            text("""
                SELECT COUNT(DISTINCT investor_id)
                FROM portfolio_companies
                WHERE investor_type = 'family_office'
            """)
        ).fetchone()[0]

        # Total companies
        total_companies = db.execute(
            text("SELECT COUNT(*) FROM portfolio_companies")
        ).fetchone()[0]

        # Source breakdown
        source_breakdown = db.execute(
            text("""
                SELECT source_type, COUNT(*)
                FROM portfolio_companies
                GROUP BY source_type
            """)
        ).fetchall()

        # Job statistics
        job_stats = db.execute(
            text("""
                SELECT
                    status, COUNT(*) as count,
                    AVG(companies_found) as avg_companies
                FROM agentic_collection_jobs
                GROUP BY status
            """)
        ).fetchall()

        return {
            "coverage": {
                "lps_with_data": lp_count,
                "family_offices_with_data": fo_count,
                "total_investors_covered": lp_count + fo_count
            },
            "portfolio_data": {
                "total_portfolio_companies": total_companies,
                "by_source": {row[0]: row[1] for row in source_breakdown}
            },
            "collection_jobs": {
                row[0]: {"count": row[1], "avg_companies": float(row[2]) if row[2] else 0}
                for row in job_stats
            }
        }

    except Exception as e:
        logger.error(f"Error getting portfolio stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=dict)
async def get_agentic_metrics():
    """
    📈 Get real-time metrics for agentic collection system.

    Returns comprehensive metrics including:
    - **Job metrics**: Success/failure rates, throughput, duration
    - **Strategy metrics**: Per-strategy performance and costs
    - **Resource usage**: Token consumption, API costs
    - **Active jobs**: Currently running collection jobs

    This endpoint is designed for monitoring dashboards and alerting systems.

    **Example Response:**
    ```json
    {
        "uptime_seconds": 3600,
        "jobs": {
            "total_jobs": 100,
            "by_status": {"pending": 2, "running": 3, "successful": 90, "failed": 5},
            "success_rate": 94.74,
            "avg_duration_seconds": 45.2
        },
        "strategies": {
            "sec_13f": {"executions": 50, "success_rate": 98.0, ...},
            "website_scraping": {"executions": 80, "success_rate": 85.0, ...}
        },
        "summary": {
            "total_companies_found": 5000,
            "total_cost_usd": 12.50,
            "avg_cost_per_job": 0.125
        }
    }
    ```
    """
    try:
        metrics = get_metrics_collector()
        return metrics.get_metrics()
    except Exception as e:
        logger.error(f"Error getting metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/strategy/{strategy_name}", response_model=dict)
async def get_strategy_metrics(strategy_name: str):
    """
    📊 Get detailed metrics for a specific collection strategy.

    **Available strategies:**
    - `sec_13f`: SEC 13F filing extraction
    - `website_scraping`: Website portfolio page scraping
    - `annual_report_pdf`: PDF annual report parsing

    Returns:
    - Execution count and success rate
    - Average duration and companies found
    - Token usage and costs
    """
    try:
        metrics = get_metrics_collector()
        strategy_metrics = metrics.get_strategy_metrics(strategy_name)

        if not strategy_metrics:
            raise HTTPException(
                status_code=404,
                detail=f"No metrics found for strategy: {strategy_name}"
            )

        return strategy_metrics
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting strategy metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/investor/{investor_id}", response_model=dict)
async def get_investor_collection_metrics(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'")
):
    """
    💰 Get collection metrics for a specific investor.

    Returns:
    - Number of collection jobs run
    - Last collection date
    - Total companies collected
    - Total collection cost
    - Strategies used
    """
    try:
        metrics = get_metrics_collector()
        investor_metrics = metrics.get_investor_metrics(investor_id, investor_type)

        if not investor_metrics:
            raise HTTPException(
                status_code=404,
                detail=f"No metrics found for {investor_type} {investor_id}"
            )

        return investor_metrics
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting investor metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/top-cost-investors", response_model=dict)
async def get_top_cost_investors(limit: int = Query(10, ge=1, le=100)):
    """
    💸 Get investors with highest collection costs.

    Useful for identifying expensive data collection patterns
    and optimizing resource usage.
    """
    try:
        metrics = get_metrics_collector()
        top_investors = metrics.get_top_investors_by_cost(limit)

        return {
            "limit": limit,
            "investors": top_investors
        }
    except Exception as e:
        logger.error(f"Error getting top cost investors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
