"""
CFTC Commitments of Traders (COT) API endpoints.

Provides access to weekly futures positioning data.
"""

import logging
from datetime import datetime
from enum import Enum

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.cftc_cot import (
    MAJOR_CONTRACTS,
    COMMODITY_GROUPS,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/cftc-cot", tags=["CFTC COT"])


class ReportType(str, Enum):
    """COT report types."""

    LEGACY = "legacy"
    DISAGGREGATED = "disaggregated"
    TFF = "tff"
    ALL = "all"


class COTIngestRequest(BaseModel):
    """Request model for COT data ingestion."""

    year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year to ingest"
    )
    report_type: ReportType = Field(
        default=ReportType.LEGACY, description="Type of COT report"
    )
    combined: bool = Field(
        default=True, description="Include futures + options combined (vs futures only)"
    )


# ========== Ingestion Endpoints ==========


@router.post("/ingest")
async def ingest_cot_data(
    request: COTIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest CFTC COT data for a given year.

    Report types:
    - **legacy**: Commercial vs Non-commercial positions
    - **disaggregated**: Producer, Swap Dealer, Managed Money, Other
    - **tff**: Traders in Financial Futures (Dealer, Asset Manager, Leveraged)
    - **all**: All report types

    Data is released weekly on Tuesday afternoons.
    No API key required (public data).
    """
    return create_and_dispatch_job(
        db=db,
        background_tasks=background_tasks,
        source="cftc_cot",
        config={
            "dataset": "cot",
            "year": request.year,
            "report_type": request.report_type.value,
            "combined": request.combined,
        },
        message=f"CFTC COT {request.report_type.value} ingestion job created for {request.year}",
    )


# ========== Reference Endpoints ==========


@router.get("/reference/contracts")
async def get_major_contracts():
    """
    Get list of major futures contracts tracked in COT reports.
    """
    return {
        "contracts": [
            {"full_name": name, "short_name": short}
            for name, short in MAJOR_CONTRACTS.items()
        ]
    }


@router.get("/reference/commodity-groups")
async def get_commodity_groups():
    """
    Get commodity groupings for analysis.

    Groups: energy, metals, grains, softs, livestock, financials, currencies, rates
    """
    return {"groups": COMMODITY_GROUPS}


@router.get("/reference/report-types")
async def get_report_types():
    """
    Get available COT report types with descriptions.
    """
    return {
        "report_types": [
            {
                "type": "legacy",
                "description": "Legacy report - Commercial (hedgers) vs Non-commercial (speculators)",
                "categories": ["Commercial", "Non-Commercial", "Non-Reportable"],
            },
            {
                "type": "disaggregated",
                "description": "Disaggregated report - Detailed trader categories",
                "categories": [
                    "Producer/Merchant",
                    "Swap Dealers",
                    "Managed Money",
                    "Other Reportables",
                ],
            },
            {
                "type": "tff",
                "description": "Traders in Financial Futures - For financial contracts",
                "categories": [
                    "Dealer/Intermediary",
                    "Asset Manager",
                    "Leveraged Funds",
                    "Other Reportables",
                ],
            },
        ]
    }
