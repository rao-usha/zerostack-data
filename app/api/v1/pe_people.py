"""
PE People API endpoints.

Endpoints for managing people data including:
- Person profiles
- Education history
- Work experience
- Deal involvement
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pe/people", tags=["PE Intelligence - People"])


# =============================================================================
# Request/Response Models
# =============================================================================


class PersonCreate(BaseModel):
    """Request model for creating a person."""

    full_name: str = Field(..., examples=["John Smith"])
    first_name: Optional[str] = Field(None, examples=["John"])
    last_name: Optional[str] = Field(None, examples=["Smith"])

    email: Optional[str] = Field(None)
    phone: Optional[str] = Field(None)

    city: Optional[str] = Field(None, examples=["New York"])
    state: Optional[str] = Field(None, examples=["NY"])
    country: Optional[str] = Field(None, examples=["USA"])

    current_title: Optional[str] = Field(None, examples=["Managing Director"])
    current_company: Optional[str] = Field(None, examples=["Blackstone"])
    bio: Optional[str] = Field(None)

    linkedin_url: Optional[str] = Field(None)
    twitter_url: Optional[str] = Field(None)


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/")
async def list_people(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    search: Optional[str] = None,
    current_company: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    List people with filtering.
    """
    try:
        query = """
            SELECT
                id, full_name, first_name, last_name,
                city, state, country,
                current_title, current_company,
                linkedin_url, is_active, created_at
            FROM pe_people
            WHERE 1=1
        """
        params = {"limit": limit, "offset": offset}

        if search:
            query += " AND full_name ILIKE :search"
            params["search"] = f"%{search}%"

        if current_company:
            query += " AND current_company ILIKE :company"
            params["company"] = f"%{current_company}%"

        query += " ORDER BY full_name LIMIT :limit OFFSET :offset"

        result = db.execute(text(query), params)
        rows = result.fetchall()

        people = []
        for row in rows:
            people.append(
                {
                    "id": row[0],
                    "full_name": row[1],
                    "first_name": row[2],
                    "last_name": row[3],
                    "location": {"city": row[4], "state": row[5], "country": row[6]},
                    "current_title": row[7],
                    "current_company": row[8],
                    "linkedin": row[9],
                    "is_active": row[10],
                    "created_at": row[11].isoformat() if row[11] else None,
                }
            )

        return {
            "count": len(people),
            "limit": limit,
            "offset": offset,
            "people": people,
        }

    except Exception as e:
        logger.error(f"Error listing people: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_people(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db),
):
    """
    Search people by name.
    """
    try:
        query = text("""
            SELECT id, full_name, current_title, current_company, linkedin_url
            FROM pe_people
            WHERE full_name ILIKE :search
            ORDER BY
                CASE WHEN full_name ILIKE :exact THEN 0 ELSE 1 END,
                full_name
            LIMIT :limit
        """)

        result = db.execute(
            query, {"search": f"%{q}%", "exact": f"{q}%", "limit": limit}
        )
        rows = result.fetchall()

        people = []
        for row in rows:
            people.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "title": row[2],
                    "company": row[3],
                    "linkedin": row[4],
                }
            )

        return {"count": len(people), "results": people}

    except Exception as e:
        logger.error(f"Error searching people: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_person(person: PersonCreate, db: Session = Depends(get_db)):
    """
    Create a person record.
    """
    try:
        insert_sql = text("""
            INSERT INTO pe_people (
                full_name, first_name, last_name,
                email, phone,
                city, state, country,
                current_title, current_company, bio,
                linkedin_url, twitter_url
            ) VALUES (
                :full_name, :first_name, :last_name,
                :email, :phone,
                :city, :state, :country,
                :current_title, :current_company, :bio,
                :linkedin_url, :twitter_url
            )
            RETURNING id, full_name, created_at
        """)

        result = db.execute(insert_sql, person.dict())
        row = result.fetchone()
        db.commit()

        return {
            "id": row[0],
            "name": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
            "message": "Person created successfully",
        }

    except Exception as e:
        logger.error(f"Error creating person: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{person_id}")
async def get_person(person_id: int, db: Session = Depends(get_db)):
    """
    Get detailed information for a person.
    """
    try:
        query = text("""
            SELECT
                id, full_name, first_name, last_name, middle_name, suffix,
                email, phone,
                city, state, country,
                current_title, current_company, bio,
                linkedin_url, twitter_url, personal_website,
                is_active, data_sources, last_verified,
                created_at, updated_at
            FROM pe_people
            WHERE id = :person_id
        """)

        result = db.execute(query, {"person_id": person_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

        # Get education
        edu_query = text("""
            SELECT id, institution, institution_type, degree, field_of_study,
                   start_year, graduation_year, honors
            FROM pe_person_education
            WHERE person_id = :person_id
            ORDER BY graduation_year DESC NULLS LAST
        """)
        edu_result = db.execute(edu_query, {"person_id": person_id})
        education = [
            {
                "id": e[0],
                "institution": e[1],
                "type": e[2],
                "degree": e[3],
                "field": e[4],
                "start_year": e[5],
                "graduation_year": e[6],
                "honors": e[7],
            }
            for e in edu_result.fetchall()
        ]

        # Get experience
        exp_query = text("""
            SELECT id, company, company_id, title, role_level,
                   start_date, end_date, is_current, description, location
            FROM pe_person_experience
            WHERE person_id = :person_id
            ORDER BY
                is_current DESC,
                end_date DESC NULLS FIRST,
                start_date DESC
        """)
        exp_result = db.execute(exp_query, {"person_id": person_id})
        experience = [
            {
                "id": e[0],
                "company": e[1],
                "company_id": e[2],
                "title": e[3],
                "level": e[4],
                "start_date": e[5].isoformat() if e[5] else None,
                "end_date": e[6].isoformat() if e[6] else None,
                "is_current": e[7],
                "description": e[8],
                "location": e[9],
            }
            for e in exp_result.fetchall()
        ]

        # Get firm affiliations
        firm_query = text("""
            SELECT fp.id, f.id as firm_id, f.name as firm_name,
                   fp.title, fp.seniority, fp.department,
                   fp.start_date, fp.end_date, fp.is_current
            FROM pe_firm_people fp
            JOIN pe_firms f ON fp.firm_id = f.id
            WHERE fp.person_id = :person_id
            ORDER BY fp.is_current DESC, fp.end_date DESC NULLS FIRST
        """)
        firm_result = db.execute(firm_query, {"person_id": person_id})
        firm_affiliations = [
            {
                "id": f[0],
                "firm_id": f[1],
                "firm_name": f[2],
                "title": f[3],
                "seniority": f[4],
                "department": f[5],
                "start_date": f[6].isoformat() if f[6] else None,
                "end_date": f[7].isoformat() if f[7] else None,
                "is_current": f[8],
            }
            for f in firm_result.fetchall()
        ]

        return {
            "id": row[0],
            "name": {
                "full": row[1],
                "first": row[2],
                "last": row[3],
                "middle": row[4],
                "suffix": row[5],
            },
            "contact": {"email": row[6], "phone": row[7]},
            "location": {"city": row[8], "state": row[9], "country": row[10]},
            "current_position": {"title": row[11], "company": row[12]},
            "bio": row[13],
            "social": {"linkedin": row[14], "twitter": row[15], "website": row[16]},
            "is_active": row[17],
            "data_quality": {
                "sources": row[18],
                "last_verified": row[19].isoformat() if row[19] else None,
            },
            "metadata": {
                "created_at": row[20].isoformat() if row[20] else None,
                "updated_at": row[21].isoformat() if row[21] else None,
            },
            "education": education,
            "experience": experience,
            "firm_affiliations": firm_affiliations,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching person: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{person_id}/education")
async def get_person_education(person_id: int, db: Session = Depends(get_db)):
    """
    Get education history for a person.
    """
    try:
        query = text("""
            SELECT id, institution, institution_type, degree, field_of_study,
                   start_year, graduation_year, honors, activities
            FROM pe_person_education
            WHERE person_id = :person_id
            ORDER BY graduation_year DESC NULLS LAST
        """)

        result = db.execute(query, {"person_id": person_id})
        rows = result.fetchall()

        education = []
        for row in rows:
            education.append(
                {
                    "id": row[0],
                    "institution": row[1],
                    "type": row[2],
                    "degree": row[3],
                    "field_of_study": row[4],
                    "years": {"start": row[5], "graduation": row[6]},
                    "honors": row[7],
                    "activities": row[8],
                }
            )

        return {"person_id": person_id, "count": len(education), "education": education}

    except Exception as e:
        logger.error(f"Error fetching education: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{person_id}/experience")
async def get_person_experience(person_id: int, db: Session = Depends(get_db)):
    """
    Get work experience for a person.
    """
    try:
        query = text("""
            SELECT id, company, company_id, title, role_level,
                   start_date, end_date, is_current, description, location
            FROM pe_person_experience
            WHERE person_id = :person_id
            ORDER BY
                is_current DESC,
                end_date DESC NULLS FIRST,
                start_date DESC
        """)

        result = db.execute(query, {"person_id": person_id})
        rows = result.fetchall()

        experience = []
        for row in rows:
            experience.append(
                {
                    "id": row[0],
                    "company": row[1],
                    "company_id": row[2],
                    "title": row[3],
                    "level": row[4],
                    "tenure": {
                        "start_date": row[5].isoformat() if row[5] else None,
                        "end_date": row[6].isoformat() if row[6] else None,
                        "is_current": row[7],
                    },
                    "description": row[8],
                    "location": row[9],
                }
            )

        return {
            "person_id": person_id,
            "count": len(experience),
            "experience": experience,
        }

    except Exception as e:
        logger.error(f"Error fetching experience: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{person_id}/deals")
async def get_person_deals(person_id: int, db: Session = Depends(get_db)):
    """
    Get deals a person was involved in.
    """
    try:
        query = text("""
            SELECT
                dpi.id, d.id as deal_id, d.deal_name, d.deal_type,
                c.name as company_name, c.id as company_id,
                d.announced_date, d.closed_date,
                d.enterprise_value_usd, dpi.role, dpi.side,
                f.name as firm_name
            FROM pe_deal_person_involvement dpi
            JOIN pe_deals d ON dpi.deal_id = d.id
            JOIN pe_portfolio_companies c ON d.company_id = c.id
            LEFT JOIN pe_firms f ON dpi.firm_id = f.id
            WHERE dpi.person_id = :person_id
            ORDER BY d.closed_date DESC NULLS FIRST
        """)

        result = db.execute(query, {"person_id": person_id})
        rows = result.fetchall()

        deals = []
        for row in rows:
            deals.append(
                {
                    "id": row[0],
                    "deal_id": row[1],
                    "deal_name": row[2],
                    "deal_type": row[3],
                    "company": {"name": row[4], "id": row[5]},
                    "dates": {
                        "announced": row[6].isoformat() if row[6] else None,
                        "closed": row[7].isoformat() if row[7] else None,
                    },
                    "enterprise_value_usd": float(row[8]) if row[8] else None,
                    "involvement": {"role": row[9], "side": row[10], "firm": row[11]},
                }
            )

        return {"person_id": person_id, "count": len(deals), "deals": deals}

    except Exception as e:
        logger.error(f"Error fetching deals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
