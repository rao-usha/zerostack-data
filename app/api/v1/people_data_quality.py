"""
People Data Quality API endpoints.

Provides endpoints for data quality monitoring, enrichment, and deduplication:
- Data quality statistics and scores
- Freshness metrics
- Duplicate detection and merging
- Enrichment queue management
- Email inference
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.services.data_quality_service import DataQualityService
from app.sources.people_collection.email_inferrer import (
    EmailInferrer,
    CompanyEmailPatternLearner,
)
from app.sources.people_collection.mx_verifier import MXVerifier


router = APIRouter(prefix="/people-data-quality", tags=["People Data Quality"])


# =============================================================================
# Response Models
# =============================================================================


class CoverageStats(BaseModel):
    """Coverage percentage stats."""

    linkedin: float
    photo: float
    email: float
    bio: float
    experience: float
    education: float


class CountStats(BaseModel):
    """Absolute count stats."""

    with_linkedin: int
    with_photo: int
    with_email: int
    with_bio: int
    with_experience: int
    with_education: int


class OverallStatsResponse(BaseModel):
    """Overall data quality statistics."""

    total_people: int
    total_companies: int
    total_active_positions: int
    companies_with_leadership: int
    coverage: CoverageStats
    counts: CountStats
    avg_confidence_score: Optional[float] = None
    recently_verified_count: int
    recently_verified_pct: float


class FreshnessBuckets(BaseModel):
    """Data freshness buckets."""

    zero_to_7_days: int = Field(alias="0-7_days")
    eight_to_30_days: int = Field(alias="8-30_days")
    thirty_one_to_90_days: int = Field(alias="31-90_days")
    ninety_one_to_180_days: int = Field(alias="91-180_days")
    one_eighty_one_to_365_days: int = Field(alias="181-365_days")
    over_365_days: int
    never_verified: int

    class Config:
        populate_by_name = True


class FreshnessStatsResponse(BaseModel):
    """Data freshness statistics."""

    total_people: int
    freshness_buckets: dict
    median_age_days: Optional[int] = None
    stale_count: int
    stale_pct: float


class QualityComponents(BaseModel):
    """Quality score components."""

    identity: int
    contact: int
    professional: int
    history: int
    freshness: int


class PersonQualityResponse(BaseModel):
    """Quality score for a single person."""

    person_id: int
    person_name: str
    quality_score: int
    components: QualityComponents
    issues: List[str]


class DuplicatePerson(BaseModel):
    """Person in a duplicate group."""

    id: int
    name: str
    linkedin: Optional[str] = None


class DuplicateGroup(BaseModel):
    """Group of potential duplicates."""

    match_type: str
    match_value: str
    people: List[DuplicatePerson]


class EnrichmentQueueItem(BaseModel):
    """Item in enrichment queue."""

    person_id: int
    full_name: str
    linkedin_url: Optional[str] = None
    has_email: bool
    has_photo: bool
    has_bio: bool
    has_current_role: bool
    priority_score: int


class MergeRequest(BaseModel):
    """Request to merge duplicate records."""

    canonical_id: int = Field(..., description="ID of the master record to keep")
    duplicate_ids: List[int] = Field(
        ..., description="IDs of duplicate records to merge"
    )


class MergeResponse(BaseModel):
    """Result of merge operation."""

    canonical_id: int
    merged_count: int
    status: str


class EmailCandidate(BaseModel):
    """Inferred email candidate."""

    email: str
    pattern: str
    confidence: str


class EmailInferenceRequest(BaseModel):
    """Request to infer email addresses."""

    first_name: str
    last_name: str
    company_domain: str


class EmailInferenceResponse(BaseModel):
    """Inferred email candidates."""

    first_name: str
    last_name: str
    company_domain: str
    candidates: List[EmailCandidate]


class BulkEmailInferenceRequest(BaseModel):
    """Request to infer emails for multiple people."""

    company_domain: str
    people: List[dict] = Field(..., description="List of {first_name, last_name}")
    known_emails: Optional[List[dict]] = Field(
        None,
        description="Known emails to learn pattern: [{email, first_name, last_name}]",
    )


class BackfillEmailsRequest(BaseModel):
    """Request to backfill inferred emails."""

    company_id: Optional[int] = Field(
        None, description="Specific company ID, or null for all"
    )
    dry_run: bool = Field(
        True, description="If true, report what would be done without saving"
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/stats", response_model=OverallStatsResponse)
async def get_data_quality_stats(
    db: Session = Depends(get_db),
):
    """
    Get overall data quality statistics.

    Returns coverage percentages, counts, and quality metrics.
    """
    service = DataQualityService(db)
    stats = service.get_overall_stats()

    return OverallStatsResponse(
        total_people=stats["total_people"],
        total_companies=stats["total_companies"],
        total_active_positions=stats["total_active_positions"],
        companies_with_leadership=stats["companies_with_leadership"],
        coverage=CoverageStats(**stats["coverage"]),
        counts=CountStats(**stats["counts"]),
        avg_confidence_score=stats["avg_confidence_score"],
        recently_verified_count=stats["recently_verified_count"],
        recently_verified_pct=stats["recently_verified_pct"],
    )


@router.get("/freshness", response_model=FreshnessStatsResponse)
async def get_freshness_stats(
    db: Session = Depends(get_db),
):
    """
    Get data freshness statistics.

    Shows distribution of data age across time buckets.
    """
    service = DataQualityService(db)
    stats = service.get_freshness_stats()

    return FreshnessStatsResponse(
        total_people=stats["total_people"],
        freshness_buckets=stats["freshness_buckets"],
        median_age_days=stats["median_age_days"],
        stale_count=stats["stale_count"],
        stale_pct=stats["stale_pct"],
    )


@router.get("/people/{person_id}/quality", response_model=PersonQualityResponse)
async def get_person_quality_score(
    person_id: int,
    db: Session = Depends(get_db),
):
    """
    Get data quality score for a specific person.

    Returns 0-100 score with breakdown by component and identified issues.
    """
    service = DataQualityService(db)
    result = service.calculate_person_quality_score(person_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return PersonQualityResponse(
        person_id=result["person_id"],
        person_name=result["person_name"],
        quality_score=result["quality_score"],
        components=QualityComponents(**result["components"]),
        issues=result["issues"],
    )


@router.post("/people/{person_id}/recalculate-score")
async def recalculate_person_score(
    person_id: int,
    db: Session = Depends(get_db),
):
    """
    Recalculate and update confidence score for a person.
    """
    service = DataQualityService(db)
    result = service.update_confidence_score(person_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@router.get("/duplicates", response_model=List[DuplicateGroup])
async def find_duplicates(
    limit: int = Query(50, ge=1, le=200, description="Max duplicate groups to return"),
    db: Session = Depends(get_db),
):
    """
    Find potential duplicate person records.

    Returns groups of records that may be the same person,
    matched by LinkedIn URL or exact name.
    """
    service = DataQualityService(db)
    duplicates = service.find_potential_duplicates(limit=limit)

    return [
        DuplicateGroup(
            match_type=d["match_type"],
            match_value=d["match_value"],
            people=[DuplicatePerson(**p) for p in d["people"]],
        )
        for d in duplicates
    ]


@router.post("/duplicates/merge", response_model=MergeResponse)
async def merge_duplicates(
    request: MergeRequest,
    db: Session = Depends(get_db),
):
    """
    Merge duplicate person records.

    Keeps the canonical record and merges data from duplicates.
    References are updated to point to the canonical record.
    """
    service = DataQualityService(db)
    result = service.merge_duplicates(
        canonical_id=request.canonical_id,
        duplicate_ids=request.duplicate_ids,
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return MergeResponse(**result)


@router.get("/enrichment-queue", response_model=List[EnrichmentQueueItem])
async def get_enrichment_queue(
    enrichment_type: str = Query(
        "all", description="Type: all, linkedin, email, photo, bio"
    ),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Get people that need enrichment.

    Returns prioritized list based on missing data and importance.
    People with current roles are prioritized higher.
    """
    valid_types = {"all", "linkedin", "email", "photo", "bio"}
    if enrichment_type not in valid_types:
        raise HTTPException(
            status_code=400, detail=f"enrichment_type must be one of: {valid_types}"
        )

    service = DataQualityService(db)
    queue = service.get_enrichment_queue(
        enrichment_type=enrichment_type,
        limit=limit,
    )

    return [EnrichmentQueueItem(**item) for item in queue]


@router.post("/infer-email", response_model=EmailInferenceResponse)
async def infer_email(
    request: EmailInferenceRequest,
):
    """
    Infer possible email addresses for a person.

    Returns ranked list of email candidates based on common patterns.
    """
    inferrer = EmailInferrer()
    candidates = inferrer.infer_email(
        first_name=request.first_name,
        last_name=request.last_name,
        company_domain=request.company_domain,
    )

    return EmailInferenceResponse(
        first_name=request.first_name,
        last_name=request.last_name,
        company_domain=request.company_domain,
        candidates=[
            EmailCandidate(
                email=c.email,
                pattern=c.pattern.value,
                confidence=c.confidence,
            )
            for c in candidates[:5]
        ],
    )


@router.post("/infer-emails-bulk")
async def infer_emails_bulk(
    request: BulkEmailInferenceRequest,
):
    """
    Infer emails for multiple people at a company.

    If known_emails provided, learns the company's email pattern first.
    """
    learner = CompanyEmailPatternLearner()

    known = None
    if request.known_emails:
        known = [
            (e["email"], e["first_name"], e["last_name"]) for e in request.known_emails
        ]

    people = [(p["first_name"], p["last_name"]) for p in request.people]

    results = learner.infer_company_emails(
        company_domain=request.company_domain,
        people=people,
        known_emails=known,
    )

    return {
        "company_domain": request.company_domain,
        "learned_pattern": learner.learn_from_known_emails(known).value
        if known
        else None,
        "results": results,
    }


@router.post("/backfill-emails")
async def backfill_emails(
    request: BackfillEmailsRequest,
    db: Session = Depends(get_db),
):
    """
    Backfill inferred email addresses for people missing work emails.

    For each company: extracts domain, checks MX records, learns email pattern
    from existing known emails, and infers emails for people missing them.
    Only stores high-confidence inferences.
    """
    from app.core.people_models import IndustrialCompany, CompanyPerson, Person

    email_inferrer = EmailInferrer()
    pattern_learner = CompanyEmailPatternLearner()
    mx_verifier = MXVerifier()

    stats = {
        "companies_processed": 0,
        "people_checked": 0,
        "emails_inferred": 0,
        "emails_skipped_no_mx": 0,
        "dry_run": request.dry_run,
    }

    # Get companies to process
    company_query = db.query(IndustrialCompany).filter(
        IndustrialCompany.website.isnot(None),
    )
    if request.company_id:
        company_query = company_query.filter(
            IndustrialCompany.id == request.company_id,
        )

    companies = company_query.all()

    for company in companies:
        domain = email_inferrer.extract_domain_from_website(company.website)
        if not domain:
            continue

        # Check MX
        mx_result = mx_verifier.verify_domain(domain)
        if not mx_result.has_mx:
            stats["emails_skipped_no_mx"] += 1
            continue

        stats["companies_processed"] += 1

        # Learn pattern from existing emails at this company
        learned_pattern = pattern_learner.learn_pattern_from_db(company.id, db)

        # Find people at this company missing work_email
        people_cps = (
            db.query(Person, CompanyPerson)
            .join(CompanyPerson, CompanyPerson.person_id == Person.id)
            .filter(
                CompanyPerson.company_id == company.id,
                CompanyPerson.is_current == True,
                CompanyPerson.work_email.is_(None),
                Person.first_name.isnot(None),
                Person.last_name.isnot(None),
            )
            .all()
        )

        for person, cp in people_cps:
            stats["people_checked"] += 1

            if person.email:
                continue

            candidates = email_inferrer.infer_email(
                first_name=person.first_name,
                last_name=person.last_name,
                company_domain=domain,
                known_pattern=learned_pattern,
            )

            if not candidates:
                continue

            top = candidates[0]
            if top.confidence != "high":
                continue

            stats["emails_inferred"] += 1

            if not request.dry_run:
                person.email = top.email
                person.email_confidence = "inferred"
                cp.work_email = top.email

    if not request.dry_run:
        db.commit()

    return stats


@router.get("/mx-check/{domain}")
async def check_mx_records(domain: str):
    """
    Check MX records for a domain.

    Useful for debugging whether a domain can receive email.
    """
    mx_verifier = MXVerifier()
    result = mx_verifier.verify_domain(domain)

    return {
        "domain": domain,
        "has_mx": result.has_mx,
        "mx_records": result.mx_records,
        "error": result.error,
    }


@router.get("/companies/{company_id}/coverage")
async def get_company_data_coverage(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Get data quality coverage for a specific company's leadership.
    """
    from app.core.people_models import IndustrialCompany, CompanyPerson, Person

    company = db.get(IndustrialCompany, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Get current leadership
    positions = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
        )
        .all()
    )

    if not positions:
        return {
            "company_id": company_id,
            "company_name": company.name,
            "total_positions": 0,
            "coverage": {},
        }

    # Get associated people
    person_ids = [p.person_id for p in positions if p.person_id]
    people = (
        db.query(Person).filter(Person.id.in_(person_ids)).all() if person_ids else []
    )

    total = len(people) if people else 1  # Avoid division by zero

    with_linkedin = sum(1 for p in people if p.linkedin_url)
    with_email = sum(1 for p in people if p.email)
    with_photo = sum(1 for p in people if p.photo_url)
    with_bio = sum(1 for p in people if p.bio)

    return {
        "company_id": company_id,
        "company_name": company.name,
        "total_positions": len(positions),
        "total_people": len(people),
        "coverage": {
            "linkedin": round(with_linkedin / total * 100, 1),
            "email": round(with_email / total * 100, 1),
            "photo": round(with_photo / total * 100, 1),
            "bio": round(with_bio / total * 100, 1),
        },
        "counts": {
            "with_linkedin": with_linkedin,
            "with_email": with_email,
            "with_photo": with_photo,
            "with_bio": with_bio,
        },
    }
