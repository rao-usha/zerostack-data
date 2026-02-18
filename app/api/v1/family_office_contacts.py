"""
API endpoints for family office contact research and enrichment.
"""

from fastapi import APIRouter, Depends, HTTPException, File, UploadFile
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime
import logging
import csv
import io

from app.core.database import get_db
from app.research.fo_website_contacts import extract_contacts_for_family_offices

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response Models
class ExtractFromWebsitesRequest(BaseModel):
    """Request to extract contacts from family office websites."""

    office_ids: Optional[List[int]] = Field(
        None,
        description="Optional list of family office IDs (None = all with websites)",
    )
    max_concurrency: int = Field(
        1, description="Max concurrent requests (default 1 for privacy)"
    )
    delay_seconds: float = Field(
        5.0, description="Delay between requests (default 5 seconds)"
    )


class ExtractFromWebsitesResponse(BaseModel):
    """Response from website extraction."""

    status: str
    message: str
    family_offices_processed: int
    contacts_found: int
    contacts_inserted: int
    duplicates_skipped: int
    errors: int


class ContactSummaryResponse(BaseModel):
    """Summary of contact coverage."""

    total_family_offices: int
    family_offices_with_contacts: int
    total_contacts: int
    contacts_by_role: dict
    contacts_by_source: dict
    contacts_by_confidence: dict
    coverage_percentage: float


class ContactResponse(BaseModel):
    """Individual contact response."""

    id: int
    family_office_id: int
    family_office_name: str
    full_name: str
    title: Optional[str]
    role: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    data_source: Optional[str]
    confidence_level: Optional[str]
    is_primary_contact: bool
    status: str
    collected_date: Optional[date]
    created_at: datetime


class ManualImportResponse(BaseModel):
    """Response from manual CSV import."""

    status: str
    rows_imported: int
    rows_skipped: int
    validation_errors: List[str]


# Endpoints


@router.post(
    "/family-offices/contacts/extract-from-websites",
    response_model=ExtractFromWebsitesResponse,
)
async def extract_contacts_from_websites(
    request: ExtractFromWebsitesRequest, db: Session = Depends(get_db)
):
    """
    Extract contacts from family office websites.

    This endpoint crawls official family office websites to find executive contact information.
    **Privacy Controls:**
    - Rate limited to 1 request per 5 seconds by default
    - Respects robots.txt
    - Skips pages requiring authentication
    - Only collects publicly disclosed business contact information
    - Does NOT collect personal/residential information

    Returns summary of extraction results.
    """
    try:
        logger.info(
            f"Starting website contact extraction for office_ids: {request.office_ids}"
        )

        # Run extraction
        results = await extract_contacts_for_family_offices(
            db=db,
            office_ids=request.office_ids,
            max_concurrency=request.max_concurrency,
            delay_seconds=request.delay_seconds,
        )

        return ExtractFromWebsitesResponse(
            status="success",
            message=f"Processed {results['family_offices_processed']} family offices",
            family_offices_processed=results["family_offices_processed"],
            contacts_found=results["contacts_found"],
            contacts_inserted=results["contacts_inserted"],
            duplicates_skipped=results["duplicates_skipped"],
            errors=results["errors"],
        )

    except Exception as e:
        logger.error(f"Error in website contact extraction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/family-offices/contacts/summary", response_model=ContactSummaryResponse)
def get_contact_summary(db: Session = Depends(get_db)):
    """
    Get summary statistics about family office contact coverage.

    Returns:
    - Total family offices
    - Family offices with at least one contact
    - Total contacts
    - Breakdown by role, source, and confidence level
    - Coverage percentage
    """
    try:
        # Total family offices
        total_fos = db.execute(text("SELECT COUNT(*) FROM family_offices")).scalar()

        # Family offices with contacts
        fos_with_contacts = db.execute(
            text("""
            SELECT COUNT(DISTINCT family_office_id) FROM family_office_contacts
        """)
        ).scalar()

        # Total contacts
        total_contacts = db.execute(
            text("SELECT COUNT(*) FROM family_office_contacts")
        ).scalar()

        # By role
        by_role_result = db.execute(
            text("""
            SELECT role, COUNT(*) as count
            FROM family_office_contacts
            WHERE role IS NOT NULL
            GROUP BY role
            ORDER BY count DESC
        """)
        )
        by_role = {row[0]: row[1] for row in by_role_result}

        # By source
        by_source_result = db.execute(
            text("""
            SELECT 
                CASE 
                    WHEN data_source LIKE 'website%' THEN 'website'
                    WHEN data_source LIKE 'sec_%' THEN 'sec_adv'
                    WHEN data_source = 'manual' THEN 'manual'
                    ELSE 'other'
                END as source_type,
                COUNT(*) as count
            FROM family_office_contacts
            WHERE data_source IS NOT NULL
            GROUP BY source_type
            ORDER BY count DESC
        """)
        )
        by_source = {row[0]: row[1] for row in by_source_result}

        # By confidence
        by_confidence_result = db.execute(
            text("""
            SELECT confidence_level, COUNT(*) as count
            FROM family_office_contacts
            WHERE confidence_level IS NOT NULL
            GROUP BY confidence_level
            ORDER BY 
                CASE confidence_level
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                    ELSE 4
                END
        """)
        )
        by_confidence = {row[0]: row[1] for row in by_confidence_result}

        # Coverage percentage
        coverage_pct = (fos_with_contacts / total_fos * 100) if total_fos > 0 else 0.0

        return ContactSummaryResponse(
            total_family_offices=total_fos,
            family_offices_with_contacts=fos_with_contacts,
            total_contacts=total_contacts,
            contacts_by_role=by_role,
            contacts_by_source=by_source,
            contacts_by_confidence=by_confidence,
            coverage_percentage=round(coverage_pct, 1),
        )

    except Exception as e:
        logger.error(f"Error getting contact summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/family-offices/{office_id}/contacts", response_model=List[ContactResponse]
)
def get_contacts_for_office(office_id: int, db: Session = Depends(get_db)):
    """
    Get all contacts for a specific family office.

    Returns list of contacts with full details.
    """
    try:
        result = db.execute(
            text("""
            SELECT 
                c.id, c.family_office_id, fo.name as family_office_name,
                c.full_name, c.title, c.role, c.email, c.phone,
                c.data_source, c.confidence_level, c.is_primary_contact,
                c.status, c.collected_date, c.created_at
            FROM family_office_contacts c
            JOIN family_offices fo ON c.family_office_id = fo.id
            WHERE c.family_office_id = :office_id
            ORDER BY 
                c.is_primary_contact DESC,
                CASE c.confidence_level
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                    ELSE 4
                END,
                c.created_at DESC
        """),
            {"office_id": office_id},
        )

        contacts = []
        for row in result:
            contacts.append(
                ContactResponse(
                    id=row[0],
                    family_office_id=row[1],
                    family_office_name=row[2],
                    full_name=row[3],
                    title=row[4],
                    role=row[5],
                    email=row[6],
                    phone=row[7],
                    data_source=row[8],
                    confidence_level=row[9],
                    is_primary_contact=row[10],
                    status=row[11],
                    collected_date=row[12],
                    created_at=row[13],
                )
            )

        return contacts

    except Exception as e:
        logger.error(f"Error getting contacts for office {office_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/family-offices/contacts/import-manual", response_model=ManualImportResponse
)
async def import_manual_contacts(
    file: UploadFile = File(...), db: Session = Depends(get_db)
):
    """
    Import contacts from CSV file (manual research).

    Expected CSV columns:
    - office_id (required)
    - full_name (required)
    - title
    - role
    - email
    - phone
    - confidence_level (high/medium/low)
    - notes

    Returns summary of import results.
    """
    try:
        # Read CSV file
        contents = await file.read()
        csv_data = contents.decode("utf-8")
        csv_reader = csv.DictReader(io.StringIO(csv_data))

        rows_imported = 0
        rows_skipped = 0
        validation_errors = []

        for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 (1 is header)
            try:
                # Validate required fields
                if not row.get("office_id") or not row.get("full_name"):
                    validation_errors.append(
                        f"Row {row_num}: Missing required fields (office_id, full_name)"
                    )
                    rows_skipped += 1
                    continue

                office_id = int(row["office_id"])

                # Check if office exists
                office_exists = db.execute(
                    text("""
                    SELECT COUNT(*) FROM family_offices WHERE id = :office_id
                """),
                    {"office_id": office_id},
                ).scalar()

                if office_exists == 0:
                    validation_errors.append(
                        f"Row {row_num}: Family office ID {office_id} not found"
                    )
                    rows_skipped += 1
                    continue

                # Check for duplicate
                existing = db.execute(
                    text("""
                    SELECT COUNT(*) FROM family_office_contacts
                    WHERE family_office_id = :office_id
                    AND full_name = :name
                """),
                    {"office_id": office_id, "name": row["full_name"]},
                ).scalar()

                if existing > 0:
                    validation_errors.append(
                        f"Row {row_num}: Duplicate contact {row['full_name']}"
                    )
                    rows_skipped += 1
                    continue

                # Insert contact
                db.execute(
                    text("""
                    INSERT INTO family_office_contacts (
                        family_office_id, full_name, title, role, email, phone,
                        data_source, confidence_level, collected_date, status, created_at
                    ) VALUES (
                        :office_id, :name, :title, :role, :email, :phone,
                        'manual', :confidence, :collected_date, 'Active', NOW()
                    )
                """),
                    {
                        "office_id": office_id,
                        "name": row["full_name"],
                        "title": row.get("title"),
                        "role": row.get("role"),
                        "email": row.get("email"),
                        "phone": row.get("phone"),
                        "confidence": row.get("confidence_level", "medium"),
                        "collected_date": date.today(),
                    },
                )

                rows_imported += 1

            except Exception as e:
                validation_errors.append(f"Row {row_num}: {str(e)}")
                rows_skipped += 1

        db.commit()

        return ManualImportResponse(
            status="success" if rows_imported > 0 else "partial",
            rows_imported=rows_imported,
            rows_skipped=rows_skipped,
            validation_errors=validation_errors[:20],  # Limit to first 20 errors
        )

    except Exception as e:
        logger.error(f"Error importing manual contacts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/family-offices/contacts/export")
def export_contacts(db: Session = Depends(get_db)):
    """
    Export all family office contacts as CSV.

    **Privacy Notice:** This data should only be used for legitimate business purposes.
    Do not use for unsolicited marketing or cold outreach without consent.
    """
    try:
        result = db.execute(
            text("""
            SELECT 
                fo.name as family_office,
                c.full_name,
                c.title,
                c.role,
                c.email,
                c.phone,
                c.confidence_level,
                c.data_source,
                c.collected_date,
                c.status
            FROM family_office_contacts c
            JOIN family_offices fo ON c.family_office_id = fo.id
            ORDER BY fo.name, c.is_primary_contact DESC
        """)
        )

        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow(
            [
                "Family Office",
                "Name",
                "Title",
                "Role",
                "Email",
                "Phone",
                "Confidence",
                "Source",
                "Collected Date",
                "Status",
            ]
        )

        # Data
        for row in result:
            writer.writerow(row)

        csv_content = output.getvalue()

        from fastapi.responses import Response

        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=family_office_contacts_{date.today()}.csv"
            },
        )

    except Exception as e:
        logger.error(f"Error exporting contacts: {e}")
        raise HTTPException(status_code=500, detail=str(e))
