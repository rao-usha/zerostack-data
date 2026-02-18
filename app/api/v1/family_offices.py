"""
Family Office tracking API endpoints.

These endpoints manage general family office data (not just SEC-registered).
Use this to track all family offices regardless of registration status.
"""
import logging
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/family-offices",
    tags=["Family Offices - Tracking"]
)


# =============================================================================
# Request/Response Models
# =============================================================================


class FamilyOfficeCreate(BaseModel):
    """Request model for creating/updating a family office."""
    
    name: str = Field(..., description="Family office name", examples=["Soros Fund Management"])
    legal_name: Optional[str] = Field(None, examples=["Soros Fund Management LLC"])

    region: Optional[str] = Field(None, examples=["US"])
    country: Optional[str] = Field(None, examples=["United States"])

    principal_family: Optional[str] = Field(None, examples=["Soros"])
    principal_name: Optional[str] = Field(None, examples=["George Soros"])
    estimated_wealth: Optional[str] = Field(None, examples=["$30B+"])

    headquarters_address: Optional[str] = Field(None, examples=["250 West 55th Street"])
    city: Optional[str] = Field(None, examples=["New York"])
    state_province: Optional[str] = Field(None, examples=["NY"])
    postal_code: Optional[str] = Field(None, examples=["10019"])

    main_phone: Optional[str] = Field(None, examples=["+1-212-555-0100"])
    main_email: Optional[str] = Field(None, examples=["info@example.com"])
    website: Optional[str] = Field(None, examples=["https://www.example.com"])
    linkedin: Optional[str] = Field(None, examples=["https://linkedin.com/company/example"])

    investment_focus: Optional[List[str]] = Field(None, examples=[["Private Equity", "Venture Capital"]])
    sectors_of_interest: Optional[List[str]] = Field(None, examples=[["AI/ML", "Healthcare", "Climate Tech"]])
    geographic_focus: Optional[List[str]] = Field(None, examples=[["North America", "Europe"]])
    stage_preference: Optional[List[str]] = Field(None, examples=[["Growth", "Late Stage"]])
    check_size_range: Optional[str] = Field(None, examples=["$10M-$100M"])

    investment_thesis: Optional[str] = Field(None, examples=["Focus on transformative technology"])
    notable_investments: Optional[List[str]] = Field(None, examples=[["Company A", "Company B"]])

    sec_crd_number: Optional[str] = Field(None, examples=["158626"])
    sec_registered: Optional[bool] = Field(False)
    estimated_aum: Optional[str] = Field(None, examples=["$5B-$10B"])
    employee_count: Optional[str] = Field(None, examples=["50-100"])

    status: Optional[str] = Field("Active", examples=["Active"])
    actively_investing: Optional[bool] = Field(None)
    accepts_outside_capital: Optional[bool] = Field(False)

    notes: Optional[str] = Field(None, examples=["Additional notes here"])


class FamilyOfficeResponse(FamilyOfficeCreate):
    """Response model for family office data."""
    id: int
    created_at: str
    updated_at: Optional[str] = None


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/", response_model=FamilyOfficeResponse)
async def create_family_office(
    office: FamilyOfficeCreate,
    db: Session = Depends(get_db)
):
    """
    📝 Create or update a family office record.
    
    Use this to manually add family office data from research.
    If a family office with the same name exists, it will be updated.
    
    **Example:**
    ```json
    {
        "name": "Soros Fund Management",
        "region": "US",
        "country": "United States",
        "principal_family": "Soros",
        "city": "New York",
        "state_province": "NY",
        "main_phone": "+1-212-555-0100",
        "website": "https://www.soros.com",
        "investment_focus": ["Private Equity", "Venture Capital"],
        "sectors_of_interest": ["AI/ML", "Healthcare"],
        "check_size_range": "$10M-$100M"
    }
    ```
    """
    try:
        # Convert arrays to PostgreSQL format
        def to_pg_array(arr):
            if not arr:
                return None
            return "{" + ",".join(f'"{item}"' for item in arr) + "}"
        
        # First insert the record
        insert_sql = text("""
            INSERT INTO family_offices (
                name, legal_name, region, country, principal_family, principal_name,
                estimated_wealth, headquarters_address, city, state_province, postal_code,
                main_phone, main_email, website, linkedin,
                check_size_range, investment_thesis,
                sec_crd_number, sec_registered, estimated_aum, employee_count,
                status, actively_investing, accepts_outside_capital, notes,
                first_researched_date, last_updated_date
            ) VALUES (
                :name, :legal_name, :region, :country, :principal_family, :principal_name,
                :estimated_wealth, :headquarters_address, :city, :state_province, :postal_code,
                :main_phone, :main_email, :website, :linkedin,
                :check_size_range, :investment_thesis,
                :sec_crd_number, :sec_registered, :estimated_aum, :employee_count,
                :status, :actively_investing, :accepts_outside_capital, :notes,
                CURRENT_DATE, CURRENT_DATE
            )
            ON CONFLICT (name) DO UPDATE SET
                legal_name = EXCLUDED.legal_name,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                principal_family = EXCLUDED.principal_family,
                principal_name = EXCLUDED.principal_name,
                estimated_wealth = EXCLUDED.estimated_wealth,
                headquarters_address = EXCLUDED.headquarters_address,
                city = EXCLUDED.city,
                state_province = EXCLUDED.state_province,
                postal_code = EXCLUDED.postal_code,
                main_phone = EXCLUDED.main_phone,
                main_email = EXCLUDED.main_email,
                website = EXCLUDED.website,
                linkedin = EXCLUDED.linkedin,
                check_size_range = EXCLUDED.check_size_range,
                investment_thesis = EXCLUDED.investment_thesis,
                sec_crd_number = EXCLUDED.sec_crd_number,
                sec_registered = EXCLUDED.sec_registered,
                estimated_aum = EXCLUDED.estimated_aum,
                employee_count = EXCLUDED.employee_count,
                status = EXCLUDED.status,
                actively_investing = EXCLUDED.actively_investing,
                accepts_outside_capital = EXCLUDED.accepts_outside_capital,
                notes = EXCLUDED.notes,
                last_updated_date = CURRENT_DATE,
                updated_at = NOW()
            RETURNING id, name, created_at, updated_at
        """)
        
        # Prepare basic params (exclude arrays)
        params = office.dict(exclude={'investment_focus', 'sectors_of_interest', 'geographic_focus', 'stage_preference', 'notable_investments'})
        
        result = db.execute(insert_sql, params)
        row = result.fetchone()
        office_id = row[0]
        
        # Update array fields separately
        if office.investment_focus or office.sectors_of_interest or office.geographic_focus or office.stage_preference or office.notable_investments:
            # Pass arrays as Python lists - psycopg2 will handle conversion
            update_sql = text("""
                UPDATE family_offices
                SET 
                    investment_focus = :investment_focus,
                    sectors_of_interest = :sectors_of_interest,
                    geographic_focus = :geographic_focus,
                    stage_preference = :stage_preference,
                    notable_investments = :notable_investments
                WHERE id = :office_id
            """)
            
            db.execute(update_sql, {
                'office_id': office_id,
                'investment_focus': office.investment_focus or [],
                'sectors_of_interest': office.sectors_of_interest or [],
                'geographic_focus': office.geographic_focus or [],
                'stage_preference': office.stage_preference or [],
                'notable_investments': office.notable_investments or [],
            })
        
        db.commit()
        
        return FamilyOfficeResponse(
            id=row[0],
            created_at=row[2].isoformat() if row[2] else None,
            updated_at=row[3].isoformat() if row[3] else None,
            **office.dict()
        )
    
    except Exception as e:
        logger.error(f"Error creating family office: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", tags=["Family Offices - Query"])
async def list_family_offices(
    limit: int = 100,
    offset: int = 0,
    region: Optional[str] = None,
    country: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    📊 List all family offices with filtering and pagination.
    
    **Query Parameters:**
    - `limit`: Max results (default: 100, max: 1000)
    - `offset`: Pagination offset
    - `region`: Filter by region (US, Europe, Asia, etc.)
    - `country`: Filter by country
    - `status`: Filter by status (Active, Inactive, etc.)
    
    **Examples:**
    ```
    GET /api/v1/family-offices?limit=50
    GET /api/v1/family-offices?region=US
    GET /api/v1/family-offices?country=United%20States&status=Active
    ```
    """
    try:
        limit = min(limit, 1000)
        
        query = """
            SELECT 
                id, name, legal_name, region, country, principal_family,
                city, state_province, main_phone, main_email, website,
                investment_focus, sectors_of_interest, check_size_range,
                estimated_wealth, status, created_at
            FROM family_offices
            WHERE 1=1
        """
        
        params = {"limit": limit, "offset": offset}
        
        if region:
            query += " AND region = :region"
            params["region"] = region
        
        if country:
            query += " AND country = :country"
            params["country"] = country
        
        if status:
            query += " AND status = :status"
            params["status"] = status
        
        query += " ORDER BY name LIMIT :limit OFFSET :offset"
        
        result = db.execute(text(query), params)
        rows = result.fetchall()
        
        offices = []
        for row in rows:
            offices.append({
                "id": row[0],
                "name": row[1],
                "legal_name": row[2],
                "region": row[3],
                "country": row[4],
                "principal_family": row[5],
                "location": {
                    "city": row[6],
                    "state_province": row[7]
                },
                "contact": {
                    "phone": row[8],
                    "email": row[9],
                    "website": row[10]
                },
                "investment_focus": row[11],
                "sectors_of_interest": row[12],
                "check_size_range": row[13],
                "estimated_wealth": row[14],
                "status": row[15],
                "created_at": row[16].isoformat() if row[16] else None
            })
        
        return {
            "count": len(offices),
            "limit": limit,
            "offset": offset,
            "offices": offices
        }
    
    except Exception as e:
        logger.error(f"Error listing family offices: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{office_id}", tags=["Family Offices - Query"])
async def get_family_office(
    office_id: int,
    db: Session = Depends(get_db)
):
    """
    📋 Get detailed information for a specific family office.
    
    Returns complete profile including contacts and investment preferences.
    
    **Example:**
    ```
    GET /api/v1/family-offices/1
    ```
    """
    try:
        query = text("""
            SELECT 
                id, name, legal_name, region, country, type,
                principal_family, principal_name, estimated_wealth,
                headquarters_address, city, state_province, postal_code,
                main_phone, main_email, website, linkedin,
                key_contacts, investment_focus, sectors_of_interest,
                geographic_focus, stage_preference, check_size_range,
                investment_thesis, notable_investments,
                data_sources, sec_crd_number, sec_registered,
                estimated_aum, employee_count, status,
                actively_investing, accepts_outside_capital,
                first_researched_date, last_updated_date, last_verified_date,
                notes, created_at, updated_at
            FROM family_offices
            WHERE id = :office_id
        """)
        
        result = db.execute(query, {"office_id": office_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Family office {office_id} not found")
        
        # Get contacts
        contacts_query = text("""
            SELECT id, full_name, title, role, email, phone, linkedin_url,
                   is_primary_contact, status
            FROM family_office_contacts
            WHERE family_office_id = :office_id
            ORDER BY is_primary_contact DESC, full_name
        """)
        
        contacts_result = db.execute(contacts_query, {"office_id": office_id})
        contacts_rows = contacts_result.fetchall()
        
        contacts = []
        for c in contacts_rows:
            contacts.append({
                "id": c[0],
                "name": c[1],
                "title": c[2],
                "role": c[3],
                "email": c[4],
                "phone": c[5],
                "linkedin": c[6],
                "is_primary": c[7],
                "status": c[8]
            })
        
        return {
            "id": row[0],
            "name": row[1],
            "legal_name": row[2],
            "classification": {
                "region": row[3],
                "country": row[4],
                "type": row[5]
            },
            "principals": {
                "family": row[6],
                "name": row[7],
                "estimated_wealth": row[8]
            },
            "headquarters": {
                "address": row[9],
                "city": row[10],
                "state_province": row[11],
                "postal_code": row[12]
            },
            "contact": {
                "phone": row[13],
                "email": row[14],
                "website": row[15],
                "linkedin": row[16]
            },
            "key_contacts_json": row[17],
            "contacts": contacts,
            "investment_profile": {
                "focus": row[18],
                "sectors": row[19],
                "geography": row[20],
                "stage": row[21],
                "check_size": row[22],
                "thesis": row[23],
                "notable_investments": row[24]
            },
            "data_quality": {
                "sources": row[25],
                "sec_crd": row[26],
                "sec_registered": row[27]
            },
            "scale": {
                "aum": row[28],
                "employees": row[29]
            },
            "status": {
                "status": row[30],
                "actively_investing": row[31],
                "accepts_outside_capital": row[32]
            },
            "metadata": {
                "first_researched": row[33].isoformat() if row[33] else None,
                "last_updated": row[34].isoformat() if row[34] else None,
                "last_verified": row[35].isoformat() if row[35] else None,
                "notes": row[36],
                "created_at": row[37].isoformat() if row[37] else None,
                "updated_at": row[38].isoformat() if row[38] else None
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching family office: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/overview", tags=["Family Offices - Query"])
async def get_overview_stats(db: Session = Depends(get_db)):
    """
    📈 Get overview statistics for family office database.
    
    Returns counts by region, investment focus, and data completeness.
    """
    try:
        stats_query = text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN region = 'US' THEN 1 END) as us_count,
                COUNT(CASE WHEN region = 'Europe' THEN 1 END) as europe_count,
                COUNT(CASE WHEN region = 'Asia' OR region LIKE '%Asia%' THEN 1 END) as asia_count,
                COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_count,
                COUNT(main_email) as with_email,
                COUNT(main_phone) as with_phone,
                COUNT(website) as with_website,
                COUNT(sec_crd_number) as sec_registered_count
            FROM family_offices
        """)
        
        result = db.execute(stats_query)
        row = result.fetchone()
        
        return {
            "total_family_offices": row[0],
            "by_region": {
                "us": row[1],
                "europe": row[2],
                "asia": row[3]
            },
            "active_count": row[4],
            "contact_info_completeness": {
                "with_email": row[5],
                "with_phone": row[6],
                "with_website": row[7]
            },
            "sec_registered": row[8]
        }
    
    except Exception as e:
        logger.error(f"Error fetching stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{office_id}")
async def delete_family_office(
    office_id: int,
    db: Session = Depends(get_db)
):
    """
    🗑️ Delete a family office record.
    
    This will also delete associated contacts and interactions (CASCADE).
    """
    try:
        sql = text("DELETE FROM family_offices WHERE id = :office_id RETURNING name")
        result = db.execute(sql, {"office_id": office_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Family office {office_id} not found")
        
        db.commit()
        
        return {
            "message": f"Family office '{row[0]}' deleted successfully",
            "id": office_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting family office: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

