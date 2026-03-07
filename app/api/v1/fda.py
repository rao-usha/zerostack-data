"""
openFDA Device Registration API endpoints.

Provides HTTP endpoints for ingesting and querying FDA device registration data.

Datasets:
- Device Registrations: Manufacturer establishments, product listings, 510(k) data

openFDA API: https://open.fda.gov/apis/device/registrationlisting/
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fda", tags=["openFDA Devices"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================


class DeviceRegistrationIngestRequest(BaseModel):
    """Request model for FDA device registration ingestion."""

    states: Optional[List[str]] = Field(
        None,
        description=(
            "List of US state codes to ingest (e.g., ['CA', 'TX']). "
            "Defaults to all US states if not specified."
        ),
    )
    search_query: Optional[str] = Field(
        None,
        description=(
            "Additional Lucene search query to filter results "
            '(e.g., \'products.product_code:"GEX"\')'
        ),
    )
    limit_per_state: Optional[int] = Field(
        None,
        description=(
            "Maximum records per state (for testing). "
            "Omit for full ingestion."
        ),
        le=100,
    )


class DeviceSearchResult(BaseModel):
    """Response model for device search results."""

    registration_number: str
    firm_name: Optional[str] = None
    city: Optional[str] = None
    state_code: Optional[str] = None
    zip_code: Optional[str] = None
    establishment_type: Optional[str] = None
    product_codes: Optional[list] = None
    device_names: Optional[list] = None


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================


@router.post("/device-registrations/ingest")
async def ingest_device_registrations(
    request: DeviceRegistrationIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest FDA device registration and listing data from the openFDA API.

    **What's Included:**
    - Registration numbers and FEI numbers
    - Firm/establishment names and addresses
    - Establishment types (manufacturer, specification developer, etc.)
    - Product codes and device names (JSONB arrays)
    - 510(k) clearance numbers
    - Registration status

    **Coverage:** ~130,000+ registered device establishments in the US

    **Pagination:** Iterates by US state to stay within openFDA's 26,000 skip limit.
    Full ingestion across all states may take 15-30 minutes depending on rate limits.

    **API Key:** Optional but recommended. Set OPENFDA_API_KEY env var for
    120K requests/day (vs 1K/day without key).

    **Example Filters:**
    - `states: ["CA", "TX"]` - Only California and Texas
    - `search_query: 'products.product_code:"GEX"'` - Only Nd:YAG laser devices
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="fda",
        config={
            "dataset": "device_registrations",
            "states": request.states,
            "search_query": request.search_query,
            "limit_per_state": request.limit_per_state,
        },
        message="FDA device registration ingestion job created",
    )


# =============================================================================
# QUERY ENDPOINTS
# =============================================================================


@router.get("/search")
async def search_device_registrations(
    q: Optional[str] = Query(None, description="Search firm name (case-insensitive)"),
    state: Optional[str] = Query(None, description="Filter by state code (e.g., CA)"),
    city: Optional[str] = Query(None, description="Filter by city name"),
    product_code: Optional[str] = Query(
        None, description="Filter by product code (e.g., GEX)"
    ),
    limit: int = Query(100, description="Maximum results", le=1000),
    offset: int = Query(0, description="Pagination offset"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored FDA device registrations.

    Query the PostgreSQL table with flexible filters. Data must be
    ingested first via POST /fda/device-registrations/ingest.

    **Filters can be combined** - all conditions are ANDed together.

    **Examples:**
    - `?q=medtronic` - Search by firm name
    - `?state=CA` - All California registrations
    - `?product_code=GEX` - All Nd:YAG laser device registrations
    - `?state=TX&product_code=QMT` - RF devices in Texas
    """
    try:
        conditions = []
        params = {}

        if q:
            conditions.append("firm_name ILIKE :q")
            params["q"] = f"%{q}%"

        if state:
            conditions.append("state_code = :state")
            params["state"] = state.upper()

        if city:
            conditions.append("city ILIKE :city")
            params["city"] = f"%{city}%"

        if product_code:
            conditions.append("product_codes @> :product_code::jsonb")
            params["product_code"] = f'["{product_code.upper()}"]'

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM fda_device_registrations WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar()

        # Data query
        data_sql = text(
            f"""
            SELECT registration_number, firm_name, address_line1,
                   city, state_code, zip_code, country_code,
                   establishment_type, product_codes, device_names,
                   proprietary_names, k_numbers, registration_status
            FROM fda_device_registrations
            WHERE {where_clause}
            ORDER BY firm_name
            LIMIT :limit OFFSET :offset
            """
        )
        params["limit"] = limit
        params["offset"] = offset

        rows = db.execute(data_sql, params).mappings().all()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "results": [dict(row) for row in rows],
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "Table fda_device_registrations not found. "
                    "Run POST /api/v1/fda/device-registrations/ingest first."
                ),
            )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/aesthetic-devices")
async def get_aesthetic_devices(
    state: Optional[str] = Query(None, description="Filter by state code"),
    q: Optional[str] = Query(None, description="Search firm name"),
    limit: int = Query(100, description="Maximum results", le=1000),
    offset: int = Query(0, description="Pagination offset"),
    db: Session = Depends(get_db),
):
    """
    Query FDA device registrations filtered to aesthetic/MedSpa product codes.

    Returns only registrations that include at least one product code
    associated with aesthetic medical devices (lasers, IPL, RF, microdermabrasion,
    cryolipolysis, microneedling, etc.).

    **Aesthetic Product Codes Included:**
    - GEX: Nd:YAG Laser
    - ILY: Intense Pulsed Light (IPL)
    - QMT: Radiofrequency (RF) device
    - OOF: Microdermabrasion
    - GEY: CO2 Laser
    - GEW: Alexandrite Laser
    - IYE: Diode Laser
    - IYN: Erbium Laser
    - KQH: Cryolipolysis (CoolSculpting-type)
    - FRN: HIFU / Ultrasonic
    - OZP: Microneedling
    - And more...

    **Use Cases:**
    - MedSpa market research and competitive analysis
    - Device manufacturer identification for aesthetic verticals
    - Geographic density mapping of aesthetic device establishments
    """
    from app.sources.fda.metadata import AESTHETIC_PRODUCT_CODES

    try:
        # Build JSONB overlap query for aesthetic product codes
        code_list = list(AESTHETIC_PRODUCT_CODES.keys())
        code_json = "[" + ", ".join(f'"{c}"' for c in code_list) + "]"

        conditions = [f"product_codes ?| array{code_list!r}"]
        params = {}

        if state:
            conditions.append("state_code = :state")
            params["state"] = state.upper()

        if q:
            conditions.append("firm_name ILIKE :q")
            params["q"] = f"%{q}%"

        where_clause = " AND ".join(conditions)

        # Count
        count_sql = text(
            f"SELECT COUNT(*) FROM fda_device_registrations WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar()

        # Data
        data_sql = text(
            f"""
            SELECT registration_number, firm_name, address_line1,
                   city, state_code, zip_code, country_code,
                   establishment_type, product_codes, device_names,
                   proprietary_names, registration_status
            FROM fda_device_registrations
            WHERE {where_clause}
            ORDER BY state_code, firm_name
            LIMIT :limit OFFSET :offset
            """
        )
        params["limit"] = limit
        params["offset"] = offset

        rows = db.execute(data_sql, params).mappings().all()

        # Annotate each result with matched aesthetic codes
        results = []
        for row in rows:
            row_dict = dict(row)
            row_codes = row_dict.get("product_codes", []) or []
            matched = {
                code: AESTHETIC_PRODUCT_CODES[code]
                for code in code_list
                if code in row_codes
            }
            row_dict["matched_aesthetic_codes"] = matched
            results.append(row_dict)

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "aesthetic_codes_used": AESTHETIC_PRODUCT_CODES,
            "results": results,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "Table fda_device_registrations not found. "
                    "Run POST /api/v1/fda/device-registrations/ingest first."
                ),
            )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_registration_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested FDA device registration data.

    Returns counts by state, top firms, and product code distribution.
    """
    try:
        # Total count
        total = db.execute(
            text("SELECT COUNT(*) FROM fda_device_registrations")
        ).scalar()

        # By state (top 20)
        by_state = db.execute(
            text("""
                SELECT state_code, COUNT(*) as count
                FROM fda_device_registrations
                WHERE state_code IS NOT NULL
                GROUP BY state_code
                ORDER BY count DESC
                LIMIT 20
            """)
        ).mappings().all()

        # Top firms
        top_firms = db.execute(
            text("""
                SELECT firm_name, COUNT(*) as count
                FROM fda_device_registrations
                WHERE firm_name IS NOT NULL
                GROUP BY firm_name
                ORDER BY count DESC
                LIMIT 20
            """)
        ).mappings().all()

        return {
            "total_registrations": total,
            "by_state": [dict(r) for r in by_state],
            "top_firms": [dict(r) for r in top_firms],
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "Table fda_device_registrations not found. "
                    "Run POST /api/v1/fda/device-registrations/ingest first."
                ),
            )
        raise HTTPException(status_code=500, detail=str(e))
