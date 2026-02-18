"""
OpenCorporates API endpoints.

Access global company registry data from 140+ jurisdictions.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.opencorporates.client import OpenCorporatesClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/opencorporates", tags=["OpenCorporates"])


class IngestRequest(BaseModel):
    company_names: Optional[List[str]] = Field(
        None, description="Companies to search. If empty, uses tracked companies."
    )
    jurisdiction: Optional[str] = Field(
        None, description="Jurisdiction filter (e.g., 'us_de')"
    )
    limit: Optional[int] = Field(
        None, ge=1, le=500, description="Max companies to process"
    )


@router.post("/ingest")
async def ingest_opencorporates(
    request: IngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Trigger OpenCorporates ingestion.

    Searches for companies, fetches officers and filings, stores in
    oc_companies, oc_officers, and oc_filings tables.
    """
    from app.sources.opencorporates.ingest import OpenCorporatesIngestor

    job = IngestionJob(source="opencorporates", status=JobStatus.PENDING, config={})
    db.add(job)
    db.commit()
    db.refresh(job)

    async def _run(job_id, names, jurisdiction, limit):
        ingestor = OpenCorporatesIngestor(db)
        await ingestor.run(job_id, names, jurisdiction, limit)

    background_tasks.add_task(
        _run, job.id, request.company_names, request.jurisdiction, request.limit
    )

    return {"status": "started", "job_id": job.id}


@router.get("/search")
def search_companies(
    q: str = Query(..., min_length=1, description="Company name search"),
    jurisdiction: Optional[str] = Query(None, description="Jurisdiction code"),
    page: int = Query(1, ge=1),
    per_page: int = Query(30, ge=1, le=100),
):
    """Search OpenCorporates for companies."""
    client = OpenCorporatesClient()
    try:
        return client.search_companies(
            q, jurisdiction=jurisdiction, page=page, per_page=per_page
        )
    finally:
        client.close()


@router.get("/company/{jurisdiction}/{company_number}")
def get_company(jurisdiction: str, company_number: str):
    """Get company details from OpenCorporates."""
    client = OpenCorporatesClient()
    try:
        result = client.get_company(jurisdiction, company_number)
        if not result:
            raise HTTPException(status_code=404, detail="Company not found")
        return result
    finally:
        client.close()


@router.get("/company/{jurisdiction}/{company_number}/officers")
def get_officers(
    jurisdiction: str,
    company_number: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(30, ge=1, le=100),
):
    """Get officers for a company."""
    client = OpenCorporatesClient()
    try:
        return client.get_company_officers(
            jurisdiction, company_number, page=page, per_page=per_page
        )
    finally:
        client.close()


@router.get("/company/{jurisdiction}/{company_number}/filings")
def get_filings(
    jurisdiction: str,
    company_number: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(30, ge=1, le=100),
):
    """Get filings for a company."""
    client = OpenCorporatesClient()
    try:
        return client.get_company_filings(
            jurisdiction, company_number, page=page, per_page=per_page
        )
    finally:
        client.close()


@router.get("/officers/search")
def search_officers(
    q: str = Query(..., min_length=1, description="Officer name search"),
    jurisdiction: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(30, ge=1, le=100),
):
    """Search for officers across companies."""
    client = OpenCorporatesClient()
    try:
        return client.search_officers(
            q, jurisdiction=jurisdiction, page=page, per_page=per_page
        )
    finally:
        client.close()


@router.get("/jurisdictions")
def get_jurisdictions():
    """Get list of supported jurisdictions."""
    client = OpenCorporatesClient()
    try:
        return {"jurisdictions": client.get_jurisdictions()}
    finally:
        client.close()
