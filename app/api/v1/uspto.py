"""
USPTO Patent API endpoints.

Provides HTTP endpoints for searching and ingesting USPTO patent data.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.uspto import ingest, metadata
from app.sources.uspto.client import USPTOClient, CPC_CODES, MAJOR_TECH_ASSIGNEES

logger = logging.getLogger(__name__)

router = APIRouter(tags=["uspto"])


# Request/Response Models

class PatentSearchRequest(BaseModel):
    """Request model for patent search."""
    query: Optional[str] = Field(
        None,
        description="Text search query (searches title and abstract)"
    )
    assignee: Optional[str] = Field(
        None,
        description="Filter by assignee name"
    )
    cpc_code: Optional[str] = Field(
        None,
        description="Filter by CPC code prefix (e.g., 'G06N' for machine learning)"
    )
    date_from: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM-DD)"
    )
    date_to: Optional[str] = Field(
        None,
        description="End date (YYYY-MM-DD)"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Maximum results (1-1000)"
    )


class IngestByAssigneeRequest(BaseModel):
    """Request model for ingesting patents by assignee."""
    assignee_name: str = Field(
        ...,
        description="Assignee name to search (e.g., 'Apple Inc.')"
    )
    date_from: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM-DD)"
    )
    date_to: Optional[str] = Field(
        None,
        description="End date (YYYY-MM-DD)"
    )
    max_patents: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum patents to ingest"
    )


class IngestByCPCRequest(BaseModel):
    """Request model for ingesting patents by CPC code."""
    cpc_code: str = Field(
        ...,
        description="CPC code prefix (e.g., 'G06N')"
    )
    date_from: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM-DD)"
    )
    date_to: Optional[str] = Field(
        None,
        description="End date (YYYY-MM-DD)"
    )
    max_patents: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum patents to ingest"
    )


class IngestBySearchRequest(BaseModel):
    """Request model for ingesting patents by search query."""
    search_query: str = Field(
        ...,
        description="Text to search in patent titles and abstracts"
    )
    date_from: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM-DD)"
    )
    date_to: Optional[str] = Field(
        None,
        description="End date (YYYY-MM-DD)"
    )
    max_patents: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum patents to ingest"
    )


# Search Endpoints

@router.get("/uspto/patents")
async def search_patents(
    query: Optional[str] = Query(None, description="Text search query"),
    assignee: Optional[str] = Query(None, description="Assignee name filter"),
    cpc_code: Optional[str] = Query(None, description="CPC code prefix filter"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max results"),
    db: Session = Depends(get_db)
):
    """
    Search patents from the local database.

    This endpoint searches patents that have already been ingested.
    Use the ingest endpoints to fetch new patents from USPTO.

    **Example queries:**
    - `/uspto/patents?assignee=Apple` - Patents assigned to Apple
    - `/uspto/patents?cpc_code=G06N` - Machine learning patents
    - `/uspto/patents?query=neural+network` - Text search
    """
    try:
        # Build SQL query
        conditions = []
        params = {}

        if query:
            conditions.append(
                "(patent_title ILIKE :query OR patent_abstract ILIKE :query)"
            )
            params["query"] = f"%{query}%"

        if assignee:
            conditions.append("assignees_json::text ILIKE :assignee")
            params["assignee"] = f"%{assignee}%"

        if cpc_code:
            conditions.append("cpc_codes_json::text ILIKE :cpc_code")
            params["cpc_code"] = f"%{cpc_code}%"

        if date_from:
            if not metadata.validate_date_format(date_from):
                raise HTTPException(400, "Invalid date_from format. Use YYYY-MM-DD")
            conditions.append("patent_date >= :date_from")
            params["date_from"] = date_from

        if date_to:
            if not metadata.validate_date_format(date_to):
                raise HTTPException(400, "Invalid date_to format. Use YYYY-MM-DD")
            conditions.append("patent_date <= :date_to")
            params["date_to"] = date_to

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params["limit"] = limit

        sql = f"""
            SELECT patent_id, patent_title, patent_date, patent_type,
                   num_claims, num_citations, inventors_json, assignees_json,
                   cpc_codes_json
            FROM uspto_patents
            WHERE {where_clause}
            ORDER BY patent_date DESC
            LIMIT :limit
        """

        result = db.execute(text(sql), params)
        rows = result.fetchall()

        patents = []
        for row in rows:
            patents.append({
                "patent_id": row[0],
                "title": row[1],
                "date": str(row[2]) if row[2] else None,
                "type": row[3],
                "claims": row[4],
                "citations": row[5],
                "inventors": row[6],
                "assignees": row[7],
                "cpc_codes": row[8]
            })

        return {
            "count": len(patents),
            "patents": patents
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Patent search failed: {e}", exc_info=True)
        # Table might not exist yet
        if "does not exist" in str(e):
            return {"count": 0, "patents": [], "message": "No patents ingested yet"}
        raise HTTPException(500, str(e))


@router.get("/uspto/patents/{patent_id}")
async def get_patent(
    patent_id: str,
    db: Session = Depends(get_db)
):
    """
    Get a single patent by ID from the local database.
    """
    try:
        sql = """
            SELECT patent_id, patent_title, patent_abstract, patent_date,
                   patent_type, num_claims, num_citations, inventors_json,
                   assignees_json, cpc_codes_json, ingested_at
            FROM uspto_patents
            WHERE patent_id = :patent_id
        """
        result = db.execute(text(sql), {"patent_id": patent_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(404, f"Patent {patent_id} not found in local database")

        return {
            "patent_id": row[0],
            "title": row[1],
            "abstract": row[2],
            "date": str(row[3]) if row[3] else None,
            "type": row[4],
            "claims": row[5],
            "citations": row[6],
            "inventors": row[7],
            "assignees": row[8],
            "cpc_codes": row[9],
            "ingested_at": str(row[10]) if row[10] else None
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get patent failed: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.get("/uspto/assignees")
async def search_assignees(
    name: Optional[str] = Query(None, description="Assignee name search"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Search assignees from the local database.
    """
    try:
        conditions = []
        params = {"limit": limit}

        if name:
            conditions.append("assignee_name ILIKE :name")
            params["name"] = f"%{name}%"

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            SELECT assignee_id, assignee_name, assignee_type, location_city,
                   location_state, location_country, patent_count
            FROM uspto_assignees
            WHERE {where_clause}
            ORDER BY patent_count DESC NULLS LAST
            LIMIT :limit
        """

        result = db.execute(text(sql), params)
        rows = result.fetchall()

        assignees = []
        for row in rows:
            assignees.append({
                "assignee_id": row[0],
                "name": row[1],
                "type": row[2],
                "city": row[3],
                "state": row[4],
                "country": row[5],
                "patent_count": row[6]
            })

        return {"count": len(assignees), "assignees": assignees}

    except Exception as e:
        if "does not exist" in str(e):
            return {"count": 0, "assignees": [], "message": "No assignees ingested yet"}
        raise HTTPException(500, str(e))


@router.get("/uspto/inventors")
async def search_inventors(
    name: Optional[str] = Query(None, description="Inventor name search"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Search inventors from the local database.
    """
    try:
        conditions = []
        params = {"limit": limit}

        if name:
            conditions.append(
                "(name_first ILIKE :name OR name_last ILIKE :name)"
            )
            params["name"] = f"%{name}%"

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            SELECT inventor_id, name_first, name_last, location_city,
                   location_state, location_country, patent_count
            FROM uspto_inventors
            WHERE {where_clause}
            ORDER BY patent_count DESC NULLS LAST
            LIMIT :limit
        """

        result = db.execute(text(sql), params)
        rows = result.fetchall()

        inventors = []
        for row in rows:
            inventors.append({
                "inventor_id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "city": row[3],
                "state": row[4],
                "country": row[5],
                "patent_count": row[6]
            })

        return {"count": len(inventors), "inventors": inventors}

    except Exception as e:
        if "does not exist" in str(e):
            return {"count": 0, "inventors": [], "message": "No inventors ingested yet"}
        raise HTTPException(500, str(e))


# Metadata Endpoints

@router.get("/uspto/cpc-codes")
async def get_cpc_codes():
    """
    Get common CPC classification codes.

    Returns a list of commonly used CPC codes with descriptions.
    """
    codes = []
    for name, code in CPC_CODES.items():
        codes.append({
            "code": code,
            "name": name.replace("_", " ").title(),
            "description": metadata.get_cpc_class_description(code)
        })
    return {"cpc_codes": codes}


@router.get("/uspto/cpc-sections")
async def get_cpc_sections():
    """
    Get CPC section definitions.
    """
    return {"sections": metadata.CPC_SECTIONS}


@router.get("/uspto/major-assignees")
async def get_major_assignees():
    """
    Get list of major tech company assignees for testing.
    """
    return {"assignees": MAJOR_TECH_ASSIGNEES}


# Ingestion Endpoints

@router.post("/uspto/ingest/assignee")
async def ingest_by_assignee(
    request: IngestByAssigneeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest all patents for an assignee.

    This endpoint creates an ingestion job that fetches patents from USPTO
    and stores them in the local database.

    **Example:**
    ```json
    {
        "assignee_name": "Apple Inc.",
        "date_from": "2020-01-01",
        "max_patents": 500
    }
    ```

    **Note:** Requires USPTO_PATENTSVIEW_API_KEY environment variable.
    """
    try:
        # Validate dates
        if request.date_from and not metadata.validate_date_format(request.date_from):
            raise HTTPException(400, "Invalid date_from format. Use YYYY-MM-DD")
        if request.date_to and not metadata.validate_date_format(request.date_to):
            raise HTTPException(400, "Invalid date_to format. Use YYYY-MM-DD")

        # Create job
        job_config = {
            "type": "assignee",
            "assignee_name": request.assignee_name,
            "date_from": request.date_from,
            "date_to": request.date_to,
            "max_patents": request.max_patents
        }

        job = IngestionJob(
            source="uspto",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        # Run in background
        background_tasks.add_task(
            _run_assignee_ingestion,
            job.id,
            request.assignee_name,
            request.date_from,
            request.date_to,
            request.max_patents
        )

        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"USPTO ingestion job created for assignee: {request.assignee_name}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create USPTO ingestion job: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.post("/uspto/ingest/cpc")
async def ingest_by_cpc(
    request: IngestByCPCRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest patents by CPC classification code.

    **Example:**
    ```json
    {
        "cpc_code": "G06N",
        "date_from": "2023-01-01",
        "max_patents": 1000
    }
    ```

    **Common CPC codes:**
    - G06N: Machine Learning / AI
    - G06F40: Natural Language Processing
    - H01L: Semiconductors
    - A61K: Pharmaceuticals
    """
    try:
        job_config = {
            "type": "cpc",
            "cpc_code": request.cpc_code,
            "date_from": request.date_from,
            "date_to": request.date_to,
            "max_patents": request.max_patents
        }

        job = IngestionJob(
            source="uspto",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        background_tasks.add_task(
            _run_cpc_ingestion,
            job.id,
            request.cpc_code,
            request.date_from,
            request.date_to,
            request.max_patents
        )

        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"USPTO ingestion job created for CPC: {request.cpc_code}",
            "cpc_description": metadata.get_cpc_class_description(request.cpc_code),
            "check_status": f"/api/v1/jobs/{job.id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create USPTO CPC ingestion job: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.post("/uspto/ingest/search")
async def ingest_by_search(
    request: IngestBySearchRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest patents by text search.

    Searches patent titles and abstracts for the given query.

    **Example:**
    ```json
    {
        "search_query": "autonomous vehicle",
        "date_from": "2022-01-01",
        "max_patents": 500
    }
    ```
    """
    try:
        job_config = {
            "type": "search",
            "search_query": request.search_query,
            "date_from": request.date_from,
            "date_to": request.date_to,
            "max_patents": request.max_patents
        }

        job = IngestionJob(
            source="uspto",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        background_tasks.add_task(
            _run_search_ingestion,
            job.id,
            request.search_query,
            request.date_from,
            request.date_to,
            request.max_patents
        )

        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"USPTO ingestion job created for search: {request.search_query}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create USPTO search ingestion job: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# Background task functions

async def _run_assignee_ingestion(
    job_id: int,
    assignee_name: str,
    date_from: Optional[str],
    date_to: Optional[str],
    max_patents: int
):
    """Run assignee ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    from datetime import datetime

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_api_key("uspto")

        await ingest.ingest_patents_by_assignee(
            db=db,
            job_id=job_id,
            assignee_name=assignee_name,
            date_from=date_from,
            date_to=date_to,
            max_patents=max_patents,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background USPTO ingestion failed: {e}", exc_info=True)
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job and job.status not in (JobStatus.SUCCESS, JobStatus.FAILED):
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                db.commit()
        except Exception as update_err:
            logger.error(f"Failed to update job status: {update_err}")
    finally:
        db.close()


async def _run_cpc_ingestion(
    job_id: int,
    cpc_code: str,
    date_from: Optional[str],
    date_to: Optional[str],
    max_patents: int
):
    """Run CPC ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    from datetime import datetime

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_api_key("uspto")

        await ingest.ingest_patents_by_cpc(
            db=db,
            job_id=job_id,
            cpc_code=cpc_code,
            date_from=date_from,
            date_to=date_to,
            max_patents=max_patents,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background USPTO CPC ingestion failed: {e}", exc_info=True)
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job and job.status not in (JobStatus.SUCCESS, JobStatus.FAILED):
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                db.commit()
        except Exception as update_err:
            logger.error(f"Failed to update job status: {update_err}")
    finally:
        db.close()


async def _run_search_ingestion(
    job_id: int,
    search_query: str,
    date_from: Optional[str],
    date_to: Optional[str],
    max_patents: int
):
    """Run search ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    from datetime import datetime

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_api_key("uspto")

        await ingest.ingest_patents_by_search(
            db=db,
            job_id=job_id,
            search_query=search_query,
            date_from=date_from,
            date_to=date_to,
            max_patents=max_patents,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background USPTO search ingestion failed: {e}", exc_info=True)
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job and job.status not in (JobStatus.SUCCESS, JobStatus.FAILED):
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                db.commit()
        except Exception as update_err:
            logger.error(f"Failed to update job status: {update_err}")
    finally:
        db.close()
