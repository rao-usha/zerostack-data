"""
Glassdoor API endpoints.

Provides access to company reviews, ratings, and salary data.
"""

from fastapi import APIRouter, Depends, Query, HTTPException, UploadFile, File
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Optional, List

from app.core.database import get_db
from app.sources.glassdoor.client import GlassdoorClient
from app.sources.glassdoor.ingest import GlassdoorCSVImporter

router = APIRouter(prefix="/glassdoor", tags=["Glassdoor"])


# Request/Response Models
class CompanyData(BaseModel):
    """Company data for creation/update."""

    company_name: str
    glassdoor_id: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    headquarters: Optional[str] = None
    industry: Optional[str] = None
    company_size: Optional[str] = None
    founded_year: Optional[int] = None
    overall_rating: Optional[float] = Field(None, ge=1.0, le=5.0)
    ceo_name: Optional[str] = None
    ceo_approval: Optional[float] = Field(None, ge=0.0, le=1.0)
    recommend_to_friend: Optional[float] = Field(None, ge=0.0, le=1.0)
    business_outlook: Optional[float] = Field(None, ge=0.0, le=1.0)
    work_life_balance: Optional[float] = Field(None, ge=1.0, le=5.0)
    compensation_benefits: Optional[float] = Field(None, ge=1.0, le=5.0)
    career_opportunities: Optional[float] = Field(None, ge=1.0, le=5.0)
    culture_values: Optional[float] = Field(None, ge=1.0, le=5.0)
    senior_management: Optional[float] = Field(None, ge=1.0, le=5.0)
    review_count: Optional[int] = None
    salary_count: Optional[int] = None
    interview_count: Optional[int] = None
    data_source: Optional[str] = "manual"


class SalaryData(BaseModel):
    """Salary data for bulk import."""

    job_title: str
    location: Optional[str] = None
    base_salary_min: Optional[int] = None
    base_salary_median: Optional[int] = None
    base_salary_max: Optional[int] = None
    total_comp_min: Optional[int] = None
    total_comp_median: Optional[int] = None
    total_comp_max: Optional[int] = None
    sample_size: Optional[int] = None
    experience_level: Optional[str] = None


class SalaryBulkRequest(BaseModel):
    """Request for bulk salary import."""

    company_name: str
    salaries: List[SalaryData]


class ReviewSummaryRequest(BaseModel):
    """Request for adding review summary."""

    company_name: str
    period: str = Field(..., description="Period string, e.g., '2025-Q4'")
    avg_rating: float = Field(..., ge=1.0, le=5.0)
    review_count: int = Field(..., ge=0)
    top_pros: Optional[List[str]] = None
    top_cons: Optional[List[str]] = None


@router.get("/company/{name}")
def get_company(name: str, db: Session = Depends(get_db)):
    """
    Get Glassdoor data for a company.

    Returns company ratings, sentiment scores, and stats.
    """
    client = GlassdoorClient(db)
    result = client.get_company(name)

    if not result:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    return result


@router.post("/company")
def upsert_company(data: CompanyData, db: Session = Depends(get_db)):
    """
    Add or update company data.

    Supports partial updates - only provided fields are updated.
    """
    client = GlassdoorClient(db)
    result = client.upsert_company(data.model_dump(exclude_none=True))

    return result


@router.get("/company/{name}/salaries")
def get_company_salaries(
    name: str,
    job_title: Optional[str] = Query(None, description="Filter by job title"),
    location: Optional[str] = Query(None, description="Filter by location"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get salary data for a company.

    Returns salary ranges by job title with optional filters.
    """
    client = GlassdoorClient(db)
    result = client.get_salaries(
        name, job_title=job_title, location=location, limit=limit
    )

    return result


@router.post("/salaries/bulk")
def bulk_import_salaries(request: SalaryBulkRequest, db: Session = Depends(get_db)):
    """
    Bulk import salary data for a company.

    Accepts a list of salary entries and adds them to the database.
    """
    client = GlassdoorClient(db)
    salaries = [s.model_dump(exclude_none=True) for s in request.salaries]
    result = client.add_salaries(request.company_name, salaries)

    return result


@router.get("/company/{name}/reviews")
def get_company_reviews(name: str, db: Session = Depends(get_db)):
    """
    Get review summary for a company.

    Returns overall rating, review count, rating trend, and top pros/cons.
    """
    client = GlassdoorClient(db)
    result = client.get_reviews(name)

    if "error" in result and result["error"] == "Company not found":
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    return result


@router.post("/reviews/summary")
def add_review_summary(request: ReviewSummaryRequest, db: Session = Depends(get_db)):
    """
    Add review summary for a period.

    Used to track rating trends over time.
    """
    client = GlassdoorClient(db)
    result = client.add_review_summary(
        company_name=request.company_name,
        period=request.period,
        avg_rating=request.avg_rating,
        review_count=request.review_count,
        top_pros=request.top_pros,
        top_cons=request.top_cons,
    )

    return result


@router.get("/compare")
def compare_companies(
    companies: str = Query(..., description="Comma-separated company names"),
    db: Session = Depends(get_db),
):
    """
    Compare multiple companies side by side.

    Returns ratings and sentiment scores for comparison.
    """
    company_list = [c.strip() for c in companies.split(",") if c.strip()]

    if len(company_list) < 2:
        raise HTTPException(
            status_code=400, detail="At least 2 companies required for comparison"
        )

    if len(company_list) > 10:
        raise HTTPException(
            status_code=400, detail="Maximum 10 companies per comparison"
        )

    client = GlassdoorClient(db)
    result = client.compare_companies(company_list)

    return result


@router.get("/search")
def search_companies(
    q: Optional[str] = Query(None, description="Search query"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    min_rating: Optional[float] = Query(
        None, ge=1.0, le=5.0, description="Minimum rating"
    ),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Search companies in the database.

    Supports filtering by industry and minimum rating.
    """
    client = GlassdoorClient(db)
    result = client.search_companies(
        query=q,
        industry=industry,
        min_rating=min_rating,
        limit=limit,
        offset=offset,
    )

    return result


@router.get("/rankings")
def get_rankings(
    metric: str = Query(
        "overall",
        description="Metric to rank by",
        pattern="^(overall|compensation|culture|career|work_life_balance|management|ceo_approval)$",
    ),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get top-rated companies by metric.

    Available metrics: overall, compensation, culture, career,
    work_life_balance, management, ceo_approval
    """
    client = GlassdoorClient(db)
    result = client.get_rankings(metric=metric, industry=industry, limit=limit)

    return result


@router.post("/import-csv")
async def import_csv(
    file: UploadFile = File(..., description="CSV file with Glassdoor data"),
    data_type: str = Query(
        "companies",
        description="Type of data: 'companies' or 'salaries'",
    ),
    db: Session = Depends(get_db),
):
    """
    Import Glassdoor data from a CSV file.

    Supports two data types:
    - **companies**: Company ratings and metadata (requires company_name column)
    - **salaries**: Salary data by job title (requires company_name and job_title columns)
    """
    if data_type not in ("companies", "salaries"):
        raise HTTPException(
            status_code=400, detail="data_type must be 'companies' or 'salaries'"
        )

    content = await file.read()
    csv_content = content.decode("utf-8-sig")

    importer = GlassdoorCSVImporter(db)

    if data_type == "companies":
        result = importer.import_companies_csv(csv_content)
    else:
        result = importer.import_salaries_csv(csv_content)

    return result


@router.delete("/company/{name}")
def delete_company(name: str, db: Session = Depends(get_db)):
    """
    Delete a company and all its associated data.
    """
    from sqlalchemy import text

    # Check if company exists
    check_query = text("""
        SELECT id FROM glassdoor_companies
        WHERE LOWER(company_name) = LOWER(:name)
    """)
    result = db.execute(check_query, {"name": name})
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    # Delete company (cascades to salaries and reviews)
    delete_query = text("""
        DELETE FROM glassdoor_companies
        WHERE LOWER(company_name) = LOWER(:name)
    """)
    db.execute(delete_query, {"name": name})
    db.commit()

    return {"status": "deleted", "company_name": name}
