"""
OSHA Inspections and Violations API endpoints.

Provides access to OSHA enforcement data downloaded from the Department
of Labor enforcement data catalog. Includes workplace inspections,
violations, penalties, and abatement information.

No API key required - uses bulk CSV downloads from DOL.

Data source: https://enforcedata.dol.gov/views/data_catalogs.php
"""

import logging
from enum import Enum
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/osha", tags=["OSHA Inspections"])


# =============================================================================
# ENUMS AND REQUEST MODELS
# =============================================================================


class OSHADataset(str, Enum):
    """Available OSHA datasets."""

    INSPECTIONS = "inspections"
    VIOLATIONS = "violations"
    ALL = "all"


class OSHAIngestRequest(BaseModel):
    """Request model for OSHA data ingestion."""

    dataset: OSHADataset = Field(
        default=OSHADataset.ALL,
        description="Which dataset to ingest: inspections, violations, or all",
    )


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================


@router.post("/ingest")
async def ingest_osha_data(
    request: OSHAIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Download and ingest OSHA enforcement data from DOL.

    Downloads bulk CSV files from the Department of Labor enforcement
    data catalog and loads them into PostgreSQL.

    **No API key required.**

    **Datasets:**
    - **inspections**: Workplace inspection records (~200K+ records)
    - **violations**: Violation records linked to inspections (~500K+ records)
    - **all**: Both inspections and violations

    **Note:** Downloads can be large (50-100MB compressed). Allow several
    minutes for the full ingestion to complete.

    **Example Request:**
    ```json
    {
        "dataset": "all"
    }
    ```
    """
    try:
        return create_and_dispatch_job(
            db,
            background_tasks,
            source="osha",
            config={
                "dataset": request.dataset.value,
            },
            message=f"OSHA {request.dataset.value} ingestion job created",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create OSHA job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SEARCH ENDPOINTS
# =============================================================================


@router.get("/search")
async def search_osha_inspections(
    company_name: Optional[str] = Query(
        default=None, description="Search by establishment name (partial match)"
    ),
    state: Optional[str] = Query(
        default=None, description="Filter by state code (e.g., 'TX')"
    ),
    naics_code: Optional[str] = Query(
        default=None, description="Filter by NAICS code"
    ),
    min_penalty: Optional[float] = Query(
        default=None, description="Minimum total current penalty"
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Search OSHA inspections in the local database.

    Queries previously ingested inspection records. Supports filtering
    by company name, state, NAICS code, and minimum penalty.

    **Example:** `/api/v1/osha/search?state=TX&min_penalty=10000&limit=20`
    """
    try:
        conditions = []
        params = {"limit": limit, "offset": offset}

        if company_name:
            conditions.append("establishment_name ILIKE :company_name")
            params["company_name"] = f"%{company_name}%"
        if state:
            conditions.append("site_state = :state")
            params["state"] = state.upper()
        if naics_code:
            conditions.append("naics_code = :naics_code")
            params["naics_code"] = naics_code
        if min_penalty is not None:
            conditions.append("total_current_penalty >= :min_penalty")
            params["min_penalty"] = min_penalty

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = f"SELECT COUNT(*) FROM osha_inspections WHERE {where_clause}"
        total = db.execute(text(count_sql), params).scalar() or 0

        # Data query
        data_sql = f"""
            SELECT activity_nr, establishment_name, site_city, site_state,
                   naics_code, inspection_type, open_date, close_case_date,
                   violation_type_s, violation_type_o, total_current_penalty,
                   total_violations
            FROM osha_inspections
            WHERE {where_clause}
            ORDER BY total_current_penalty DESC NULLS LAST
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
        logger.error(f"OSHA search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/violations/{activity_nr}")
async def get_violations_for_inspection(
    activity_nr: str,
    db: Session = Depends(get_db),
):
    """
    Get all violations for a specific OSHA inspection.

    **Path Parameter:** activity_nr - The OSHA inspection activity number
    """
    try:
        sql = """
            SELECT activity_nr, citation_id, violation_type,
                   current_penalty, initial_penalty, issuance_date,
                   abate_date, standard, description
            FROM osha_violations
            WHERE activity_nr = :activity_nr
            ORDER BY current_penalty DESC NULLS LAST
        """
        rows = db.execute(text(sql), {"activity_nr": activity_nr}).fetchall()

        if not rows:
            return {
                "activity_nr": activity_nr,
                "total_violations": 0,
                "violations": [],
            }

        return {
            "activity_nr": activity_nr,
            "total_violations": len(rows),
            "violations": [dict(row._mapping) for row in rows],
        }

    except Exception as e:
        if "does not exist" in str(e):
            return {
                "activity_nr": activity_nr,
                "total_violations": 0,
                "violations": [],
                "message": "Table not yet created. Run ingestion first.",
            }
        logger.error(f"OSHA violations lookup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_osha_stats(db: Session = Depends(get_db)):
    """
    Get summary statistics for ingested OSHA data.
    """
    try:
        stats = {}

        # Inspections stats
        try:
            insp_sql = """
                SELECT
                    COUNT(*) as total_inspections,
                    COUNT(DISTINCT site_state) as states_represented,
                    SUM(total_current_penalty) as total_penalties,
                    AVG(total_current_penalty) as avg_penalty,
                    MAX(total_current_penalty) as max_penalty,
                    SUM(total_violations) as total_violations
                FROM osha_inspections
            """
            result = db.execute(text(insp_sql)).fetchone()
            if result:
                stats["inspections"] = dict(result._mapping)
        except Exception:
            stats["inspections"] = {"total_inspections": 0}

        # Violations stats
        try:
            viol_sql = """
                SELECT
                    COUNT(*) as total_violation_records,
                    COUNT(DISTINCT activity_nr) as inspections_with_violations,
                    SUM(current_penalty) as total_penalties,
                    COUNT(DISTINCT violation_type) as violation_types
                FROM osha_violations
            """
            result = db.execute(text(viol_sql)).fetchone()
            if result:
                stats["violations"] = dict(result._mapping)
        except Exception:
            stats["violations"] = {"total_violation_records": 0}

        return stats

    except Exception as e:
        logger.error(f"OSHA stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
