"""
NPPES NPI Registry API endpoints.

Provides REST API endpoints for ingesting and querying NPPES healthcare
provider data. No API key is required for the upstream NPPES API.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.nppes import metadata

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/nppes",
    tags=["nppes"],
    responses={404: {"description": "Not found"}},
)


# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------


class NPPESIngestRequest(BaseModel):
    """Request to ingest NPPES NPI provider data."""

    states: Optional[List[str]] = Field(
        None,
        description="List of two-letter state codes (e.g., ['CA', 'NY'])",
        examples=[["CA", "TX"]],
    )
    taxonomy_codes: Optional[List[str]] = Field(
        None,
        description="List of NUCC taxonomy codes to filter (e.g., ['207N00000X'] for Dermatology)",
        examples=[["207N00000X", "208200000X"]],
    )
    taxonomy_description: Optional[str] = Field(
        None,
        description="Taxonomy description to search (e.g., 'Dermatology')",
    )
    enumeration_type: Optional[str] = Field(
        None,
        description="NPI-1 (individual) or NPI-2 (organization)",
    )
    city: Optional[str] = Field(None, description="City name filter")
    postal_code: Optional[str] = Field(None, description="ZIP code filter")
    limit: Optional[int] = Field(
        None,
        description="Maximum records to ingest (for testing)",
        ge=1,
        le=100000,
    )


class NPPESSearchRequest(BaseModel):
    """Request to search local NPPES provider database."""

    state: Optional[str] = Field(None, description="Two-letter state code")
    taxonomy_code: Optional[str] = Field(None, description="NUCC taxonomy code")
    taxonomy_description: Optional[str] = Field(
        None, description="Taxonomy description (partial match)"
    )
    city: Optional[str] = Field(None, description="City name")
    postal_code: Optional[str] = Field(None, description="ZIP code (5-digit)")
    entity_type: Optional[str] = Field(
        None, description="1=Individual, 2=Organization"
    )
    name: Optional[str] = Field(
        None, description="Provider name (partial match)"
    )
    limit: int = Field(100, description="Maximum results", ge=1, le=1000)
    offset: int = Field(0, description="Results offset for pagination", ge=0)


class NPPESProviderInfo(BaseModel):
    """Information about a single NPPES provider."""

    npi: str
    entity_type: Optional[str] = None
    legal_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    credential: Optional[str] = None
    dba_name: Optional[str] = None
    practice_city: Optional[str] = None
    practice_state: Optional[str] = None
    practice_zip: Optional[str] = None
    practice_phone: Optional[str] = None
    taxonomy_code: Optional[str] = None
    taxonomy_description: Optional[str] = None
    status: Optional[str] = None


class NPPESTaxonomyInfo(BaseModel):
    """Information about a MedSpa-relevant taxonomy code."""

    code: str
    description: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/taxonomy-codes",
    response_model=list[NPPESTaxonomyInfo],
    summary="List MedSpa-Relevant Taxonomy Codes",
    description="""
    List taxonomy codes commonly associated with MedSpa and aesthetic
    medicine practices. Use these codes with the ingest endpoint to
    target specific provider types.
    """,
)
def list_taxonomy_codes():
    """List MedSpa-relevant taxonomy codes."""
    return [
        NPPESTaxonomyInfo(code=code, description=desc)
        for code, desc in metadata.MEDSPA_TAXONOMY_CODES.items()
    ]


@router.post(
    "/ingest",
    summary="Ingest NPPES Provider Data",
    description="""
    Start an NPPES NPI Registry ingestion job.

    **Data Source:** NPPES NPI Registry API (npiregistry.cms.hhs.gov)
    **No API key required.**

    **Contains:**
    - Provider NPI numbers, names, credentials
    - Practice and mailing addresses
    - Taxonomy codes and descriptions
    - Enumeration dates and status

    **Filtering:**
    - `states`: Filter by state(s) — recommended for manageable result sets
    - `taxonomy_codes`: Filter by NUCC taxonomy code(s)
    - `taxonomy_description`: Free-text taxonomy search
    - `enumeration_type`: NPI-1 (individual) or NPI-2 (organization)
    - `city`, `postal_code`: Geographic filters

    **MedSpa Use Case:**
    To find dermatology and aesthetic medicine providers in Texas:
    ```json
    {
        "states": ["TX"],
        "taxonomy_codes": ["207N00000X", "261QM0801X", "208200000X"]
    }
    ```

    **Note:** The NPPES API returns up to ~1200 results per query
    combination. For large-scale ingestion, filter by state.
    """,
    response_model=dict,
)
async def ingest_nppes(
    request: NPPESIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Start NPPES NPI provider data ingestion job."""
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="nppes",
        config={
            "states": request.states,
            "taxonomy_codes": request.taxonomy_codes,
            "taxonomy_description": request.taxonomy_description,
            "enumeration_type": request.enumeration_type,
            "city": request.city,
            "postal_code": request.postal_code,
            "limit": request.limit,
        },
        message="NPPES NPI Registry ingestion job created",
    )


@router.get(
    "/search",
    summary="Search Local NPPES Provider Database",
    description="""
    Search the locally ingested NPPES provider data.

    **Note:** This searches the local database, not the upstream API.
    You must first ingest data via the `/nppes/ingest` endpoint.

    **Filters:**
    - `state`: Two-letter state code
    - `taxonomy_code`: Exact taxonomy code match
    - `taxonomy_description`: Partial match on taxonomy description
    - `city`: Exact city match
    - `postal_code`: 5-digit ZIP code
    - `entity_type`: 1 (individual) or 2 (organization)
    - `name`: Partial match on legal name

    Results are ordered by legal name and paginated.
    """,
    response_model=dict,
)
async def search_nppes(
    state: Optional[str] = None,
    taxonomy_code: Optional[str] = None,
    taxonomy_description: Optional[str] = None,
    city: Optional[str] = None,
    postal_code: Optional[str] = None,
    entity_type: Optional[str] = None,
    name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    """Search locally ingested NPPES provider data."""
    try:
        # Build dynamic WHERE clause with parameterized queries
        conditions = []
        params = {"limit": min(limit, 1000), "offset": offset}

        if state:
            conditions.append("practice_state = :state")
            params["state"] = state.upper()

        if taxonomy_code:
            conditions.append("taxonomy_code = :taxonomy_code")
            params["taxonomy_code"] = taxonomy_code

        if taxonomy_description:
            conditions.append("taxonomy_description ILIKE :taxonomy_desc")
            params["taxonomy_desc"] = f"%{taxonomy_description}%"

        if city:
            conditions.append("practice_city ILIKE :city")
            params["city"] = city

        if postal_code:
            conditions.append("practice_zip LIKE :zip")
            params["zip"] = f"{postal_code}%"

        if entity_type:
            conditions.append("entity_type = :entity_type")
            params["entity_type"] = entity_type

        if name:
            conditions.append("legal_name ILIKE :name")
            params["name"] = f"%{name}%"

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = f"SELECT COUNT(*) FROM {metadata.TABLE_NAME} WHERE {where_clause}"
        count_result = db.execute(text(count_sql), params).scalar()

        # Data query
        data_sql = (
            f"SELECT npi, entity_type, legal_name, first_name, last_name, "
            f"credential, dba_name, practice_city, practice_state, practice_zip, "
            f"practice_phone, taxonomy_code, taxonomy_description, status "
            f"FROM {metadata.TABLE_NAME} "
            f"WHERE {where_clause} "
            f"ORDER BY legal_name "
            f"LIMIT :limit OFFSET :offset"
        )

        rows = db.execute(text(data_sql), params).fetchall()

        providers = []
        for row in rows:
            providers.append({
                "npi": row[0],
                "entity_type": row[1],
                "legal_name": row[2],
                "first_name": row[3],
                "last_name": row[4],
                "credential": row[5],
                "dba_name": row[6],
                "practice_city": row[7],
                "practice_state": row[8],
                "practice_zip": row[9],
                "practice_phone": row[10],
                "taxonomy_code": row[11],
                "taxonomy_description": row[12],
                "status": row[13],
            })

        return {
            "total_count": count_result,
            "limit": params["limit"],
            "offset": offset,
            "results": providers,
        }

    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            raise HTTPException(
                status_code=404,
                detail=(
                    "NPPES provider table not found. "
                    "Run POST /api/v1/nppes/ingest first to populate data."
                ),
            )
        logger.error(f"NPPES search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Search failed: {error_msg}")


@router.get(
    "/lookup/{npi}",
    summary="Look Up Provider by NPI",
    description="""
    Look up a single provider by their 10-digit NPI number
    from the local database.
    """,
    response_model=dict,
)
async def lookup_npi(npi: str, db: Session = Depends(get_db)):
    """Look up a provider by NPI from local database."""
    try:
        sql = (
            f"SELECT * FROM {metadata.TABLE_NAME} WHERE npi = :npi"
        )
        row = db.execute(text(sql), {"npi": npi}).fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"NPI {npi} not found in local database",
            )

        # Convert row to dict
        columns = row._fields if hasattr(row, "_fields") else row.keys()
        provider = dict(zip(columns, row))

        return {"provider": provider}

    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            raise HTTPException(
                status_code=404,
                detail=(
                    "NPPES provider table not found. "
                    "Run POST /api/v1/nppes/ingest first."
                ),
            )
        logger.error(f"NPI lookup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/stats",
    summary="Get NPPES Data Statistics",
    description="Get counts and breakdowns of ingested NPPES provider data.",
    response_model=dict,
)
async def nppes_stats(db: Session = Depends(get_db)):
    """Get statistics on ingested NPPES data."""
    try:
        stats = {}

        # Total count
        total = db.execute(
            text(f"SELECT COUNT(*) FROM {metadata.TABLE_NAME}")
        ).scalar()
        stats["total_providers"] = total

        # By entity type
        entity_counts = db.execute(
            text(
                f"SELECT entity_type, COUNT(*) "
                f"FROM {metadata.TABLE_NAME} "
                f"GROUP BY entity_type ORDER BY entity_type"
            )
        ).fetchall()
        stats["by_entity_type"] = {
            row[0]: row[1] for row in entity_counts
        }

        # By state (top 10)
        state_counts = db.execute(
            text(
                f"SELECT practice_state, COUNT(*) as cnt "
                f"FROM {metadata.TABLE_NAME} "
                f"WHERE practice_state IS NOT NULL "
                f"GROUP BY practice_state ORDER BY cnt DESC LIMIT 10"
            )
        ).fetchall()
        stats["top_states"] = {row[0]: row[1] for row in state_counts}

        # By taxonomy (top 10)
        taxonomy_counts = db.execute(
            text(
                f"SELECT taxonomy_description, COUNT(*) as cnt "
                f"FROM {metadata.TABLE_NAME} "
                f"WHERE taxonomy_description IS NOT NULL "
                f"GROUP BY taxonomy_description ORDER BY cnt DESC LIMIT 10"
            )
        ).fetchall()
        stats["top_taxonomies"] = {
            row[0]: row[1] for row in taxonomy_counts
        }

        return stats

    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            raise HTTPException(
                status_code=404,
                detail=(
                    "NPPES provider table not found. "
                    "Run POST /api/v1/nppes/ingest first."
                ),
            )
        logger.error(f"NPPES stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
