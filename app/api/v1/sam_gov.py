"""
SAM.gov Entity Registration API endpoints.

Provides access to the System for Award Management (SAM.gov) entity
registration database. Includes federal contractor registrations,
NAICS codes, business types, and contact information.

API Key: Required (free)
- Register at: https://sam.gov/content/entity-information
- Set SAM_GOV_API_KEY environment variable
- Rate limit: 1,000 requests per day
"""

import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sam-gov", tags=["SAM.gov Entity Registration"])


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================


class SAMGovIngestRequest(BaseModel):
    """Request model for SAM.gov entity ingestion."""

    state: Optional[str] = Field(
        default=None,
        description="Two-letter state code (e.g., 'TX', 'CA')",
        max_length=2,
    )
    naics_code: Optional[str] = Field(
        default=None,
        description="NAICS code to filter by (e.g., '541512' for IT consulting)",
    )
    legal_business_name: Optional[str] = Field(
        default=None,
        description="Business name search string",
    )
    max_pages: int = Field(
        default=50,
        description="Maximum pages to fetch (100 records per page)",
        ge=1,
        le=100,
    )


class SAMGovSearchRequest(BaseModel):
    """Request model for local database search."""

    state: Optional[str] = Field(default=None, description="Filter by state code")
    naics_code: Optional[str] = Field(default=None, description="Filter by NAICS code")
    business_name: Optional[str] = Field(
        default=None, description="Search by business name (partial match)"
    )
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================


@router.post("/ingest")
async def ingest_sam_gov_entities(
    request: SAMGovIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest entity registrations from SAM.gov.

    Downloads active entity registrations filtered by state, NAICS code,
    and/or business name. Data is stored in the `sam_gov_entities` table.

    **Requires:** SAM_GOV_API_KEY environment variable

    **Rate Limit:** 1,000 requests per day (each page = 1 request)

    **Example Request:**
    ```json
    {
        "state": "TX",
        "naics_code": "541512",
        "max_pages": 10
    }
    ```
    """
    try:
        if not request.state and not request.naics_code and not request.legal_business_name:
            raise HTTPException(
                status_code=400,
                detail="At least one filter is required: state, naics_code, or legal_business_name",
            )

        return create_and_dispatch_job(
            db,
            background_tasks,
            source="sam_gov",
            config={
                "dataset": "entities",
                "state": request.state,
                "naics_code": request.naics_code,
                "legal_business_name": request.legal_business_name,
                "max_pages": request.max_pages,
            },
            message="SAM.gov entity ingestion job created",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create SAM.gov job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SEARCH ENDPOINTS
# =============================================================================


@router.get("/search")
async def search_sam_gov_entities(
    state: Optional[str] = Query(default=None, description="Filter by state code"),
    naics_code: Optional[str] = Query(default=None, description="Filter by NAICS code"),
    business_name: Optional[str] = Query(
        default=None, description="Search by business name (partial match)"
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Search SAM.gov entities in the local database.

    Queries previously ingested entity registrations. Supports filtering
    by state, NAICS code, and business name (partial match).

    **Example:** `/api/v1/sam-gov/search?state=TX&naics_code=541512&limit=20`
    """
    try:
        conditions = []
        params = {"limit": limit, "offset": offset}

        if state:
            conditions.append("physical_address_state = :state")
            params["state"] = state.upper()
        if naics_code:
            conditions.append("naics_code_primary = :naics_code")
            params["naics_code"] = naics_code
        if business_name:
            conditions.append("legal_business_name ILIKE :business_name")
            params["business_name"] = f"%{business_name}%"

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = f"SELECT COUNT(*) FROM sam_gov_entities WHERE {where_clause}"
        total = db.execute(text(count_sql), params).scalar() or 0

        # Data query
        data_sql = f"""
            SELECT uei, cage_code, legal_business_name, dba_name,
                   physical_address_city, physical_address_state,
                   physical_address_zip, naics_code_primary,
                   entity_structure, registration_status, entity_url
            FROM sam_gov_entities
            WHERE {where_clause}
            ORDER BY legal_business_name
            LIMIT :limit OFFSET :offset
        """
        rows = db.execute(text(data_sql), params).fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "results": [dict(row._mapping) for row in rows],
        }

    except Exception as e:
        if "does not exist" in str(e):
            return {"total": 0, "limit": limit, "offset": offset, "results": []}
        logger.error(f"SAM.gov search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_sam_gov_stats(db: Session = Depends(get_db)):
    """
    Get summary statistics for ingested SAM.gov entities.
    """
    try:
        stats_sql = """
            SELECT
                COUNT(*) as total_entities,
                COUNT(DISTINCT physical_address_state) as states_represented,
                COUNT(DISTINCT naics_code_primary) as unique_naics_codes,
                MIN(registration_date) as earliest_registration,
                MAX(registration_date) as latest_registration
            FROM sam_gov_entities
        """
        result = db.execute(text(stats_sql)).fetchone()
        if not result:
            return {"total_entities": 0}

        return dict(result._mapping)

    except Exception as e:
        if "does not exist" in str(e):
            return {"total_entities": 0, "message": "Table not yet created. Run ingestion first."}
        logger.error(f"SAM.gov stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
