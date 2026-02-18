"""
Public API Endpoints (Protected by API Key Authentication).

T19: Public API with Auth & Rate Limits
- Investor list and details
- Search functionality
- All endpoints require valid API key
"""

import time
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Header, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.auth.api_keys import APIKeyService, RateLimiter, RateLimitInfo


# ============================================================================
# Response Models
# ============================================================================


class InvestorSummary(BaseModel):
    """Summary of an investor for list view."""

    id: int
    name: str
    lp_type: Optional[str]
    jurisdiction: Optional[str]
    website: Optional[str]


class InvestorDetail(BaseModel):
    """Detailed investor information."""

    id: int
    name: str
    formal_name: Optional[str]
    lp_type: Optional[str]
    jurisdiction: Optional[str]
    website: Optional[str]
    investment_focus: Optional[List[str]]
    portfolio_count: int


class SearchResult(BaseModel):
    """Search result item."""

    id: int
    name: str
    type: str  # 'investor' or 'company'
    snippet: Optional[str]
    relevance_score: float


class PaginatedResponse(BaseModel):
    """Paginated response wrapper."""

    data: List[Any]
    total: int
    page: int
    per_page: int
    has_more: bool


# ============================================================================
# Authentication Dependency
# ============================================================================


class APIKeyAuth:
    """Dependency for API key authentication and rate limiting."""

    def __init__(self):
        self.key_info: Optional[Dict[str, Any]] = None
        self.rate_info: Optional[RateLimitInfo] = None

    async def __call__(
        self,
        request: Request,
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
        api_key: Optional[str] = Query(None, alias="api_key"),
        db: Session = Depends(get_db),
    ) -> Dict[str, Any]:
        """Validate API key and check rate limits."""
        # Get key from header or query param
        raw_key = x_api_key or api_key

        if not raw_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key required. Provide via X-API-Key header or api_key query parameter.",
            )

        # Validate key
        service = APIKeyService(db)
        key_info = service.validate_key(raw_key)

        if not key_info:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API key",
            )

        # Check rate limits
        limiter = RateLimiter(db)
        limits = {
            "per_minute": key_info["rate_limit_per_minute"],
            "per_day": key_info["rate_limit_per_day"],
        }

        allowed, rate_info = limiter.check_rate_limit(key_info["id"], limits)

        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded ({rate_info.limit_type}). Try again later.",
                headers={
                    "X-RateLimit-Limit": str(rate_info.limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(rate_info.reset),
                    "Retry-After": str(rate_info.reset - int(time.time())),
                },
            )

        # Store rate info for response headers
        self.key_info = key_info
        self.rate_info = rate_info

        # Store in request state for usage tracking
        request.state.api_key_id = key_info["id"]
        request.state.rate_info = rate_info

        return key_info


# Create reusable auth dependency
require_api_key = APIKeyAuth()


# ============================================================================
# Router
# ============================================================================

router = APIRouter(prefix="/public", tags=["Public API"])


def add_rate_limit_headers(request: Request) -> Dict[str, str]:
    """Get rate limit headers from request state."""
    rate_info = getattr(request.state, "rate_info", None)
    if rate_info:
        return {
            "X-RateLimit-Limit": str(rate_info.limit),
            "X-RateLimit-Remaining": str(rate_info.remaining),
            "X-RateLimit-Reset": str(rate_info.reset),
        }
    return {}


# ============================================================================
# Endpoints
# ============================================================================


@router.get("/investors", response_model=PaginatedResponse)
async def list_investors(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Results per page"),
    lp_type: Optional[str] = Query(None, description="Filter by investor type"),
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction"),
    key_info: Dict = Depends(require_api_key),
    db: Session = Depends(get_db),
):
    """
    List investors with pagination and filtering.

    **Requires API Key** - Pass via `X-API-Key` header or `api_key` query param.

    **Filters:**
    - `lp_type`: Filter by investor type (e.g., "Pension Fund", "Endowment")
    - `jurisdiction`: Filter by location
    """
    start_time = time.time()

    # Build query
    where_clauses = []
    params = {"limit": per_page, "offset": (page - 1) * per_page}

    if lp_type:
        where_clauses.append("lp_type = :lp_type")
        params["lp_type"] = lp_type

    if jurisdiction:
        where_clauses.append("jurisdiction ILIKE :jurisdiction")
        params["jurisdiction"] = f"%{jurisdiction}%"

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Get total count
    count_result = db.execute(
        text(f"""
        SELECT COUNT(*) FROM lp_fund {where_sql}
    """),
        params,
    )
    total = count_result.fetchone()[0]

    # Get paginated results
    result = db.execute(
        text(f"""
        SELECT id, name, lp_type, jurisdiction, website_url
        FROM lp_fund
        {where_sql}
        ORDER BY name
        LIMIT :limit OFFSET :offset
    """),
        params,
    )

    investors = []
    for row in result:
        investors.append(
            InvestorSummary(
                id=row[0],
                name=row[1],
                lp_type=row[2],
                jurisdiction=row[3],
                website=row[4],
            ).model_dump()
        )

    # Record usage
    response_time = int((time.time() - start_time) * 1000)
    service = APIKeyService(db)
    service.record_usage(
        key_id=request.state.api_key_id,
        endpoint="/public/investors",
        method="GET",
        status_code=200,
        response_time_ms=response_time,
    )

    response = PaginatedResponse(
        data=investors,
        total=total,
        page=page,
        per_page=per_page,
        has_more=(page * per_page) < total,
    )

    return JSONResponse(
        content=response.model_dump(), headers=add_rate_limit_headers(request)
    )


@router.get("/investors/{investor_id}", response_model=InvestorDetail)
async def get_investor(
    request: Request,
    investor_id: int,
    key_info: Dict = Depends(require_api_key),
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a specific investor.

    **Requires API Key** - Pass via `X-API-Key` header or `api_key` query param.
    """
    start_time = time.time()

    # Get investor
    result = db.execute(
        text("""
        SELECT id, name, formal_name, lp_type, jurisdiction, website_url
        FROM lp_fund
        WHERE id = :investor_id
    """),
        {"investor_id": investor_id},
    )

    row = result.fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Investor not found"
        )

    # Get portfolio count
    portfolio_result = db.execute(
        text("""
        SELECT COUNT(*) FROM portfolio_companies WHERE investor_id = :investor_id
    """),
        {"investor_id": investor_id},
    )
    portfolio_count = portfolio_result.fetchone()[0]

    # Get investment focus (distinct industries from portfolio)
    focus_result = db.execute(
        text("""
        SELECT DISTINCT company_industry
        FROM portfolio_companies
        WHERE investor_id = :investor_id AND company_industry IS NOT NULL
        LIMIT 10
    """),
        {"investor_id": investor_id},
    )
    investment_focus = [r[0] for r in focus_result]

    investor = InvestorDetail(
        id=row[0],
        name=row[1],
        formal_name=row[2],
        lp_type=row[3],
        jurisdiction=row[4],
        website=row[5],
        investment_focus=investment_focus,
        portfolio_count=portfolio_count,
    )

    # Record usage
    response_time = int((time.time() - start_time) * 1000)
    service = APIKeyService(db)
    service.record_usage(
        key_id=request.state.api_key_id,
        endpoint=f"/public/investors/{investor_id}",
        method="GET",
        status_code=200,
        response_time_ms=response_time,
    )

    return JSONResponse(
        content=investor.model_dump(), headers=add_rate_limit_headers(request)
    )


@router.get("/search", response_model=PaginatedResponse)
async def search(
    request: Request,
    q: str = Query(..., min_length=2, description="Search query"),
    type: Optional[str] = Query(
        None, pattern="^(investor|company)$", description="Filter by type"
    ),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    key_info: Dict = Depends(require_api_key),
    db: Session = Depends(get_db),
):
    """
    Search investors and companies.

    **Requires API Key** - Pass via `X-API-Key` header or `api_key` query param.

    **Parameters:**
    - `q`: Search query (minimum 2 characters)
    - `type`: Filter by result type ("investor" or "company")
    """
    start_time = time.time()

    results = []
    total = 0
    offset = (page - 1) * per_page

    # Search investors
    if not type or type == "investor":
        investor_result = db.execute(
            text("""
            SELECT id, name, 'investor' as type,
                   CASE
                       WHEN name ILIKE :exact THEN 1.0
                       WHEN name ILIKE :starts THEN 0.8
                       ELSE 0.6
                   END as score
            FROM lp_fund
            WHERE name ILIKE :pattern
            ORDER BY score DESC, name
            LIMIT :limit OFFSET :offset
        """),
            {
                "pattern": f"%{q}%",
                "exact": q,
                "starts": f"{q}%",
                "limit": per_page if not type else per_page,
                "offset": offset if not type else offset,
            },
        )

        for row in investor_result:
            results.append(
                SearchResult(
                    id=row[0],
                    name=row[1],
                    type=row[2],
                    snippet=None,
                    relevance_score=float(row[3]),
                ).model_dump()
            )

        # Count investors
        count_result = db.execute(
            text("""
            SELECT COUNT(*) FROM lp_fund WHERE name ILIKE :pattern
        """),
            {"pattern": f"%{q}%"},
        )
        total += count_result.fetchone()[0]

    # Search companies
    if not type or type == "company":
        company_result = db.execute(
            text("""
            SELECT id, company_name, 'company' as type,
                   CASE
                       WHEN company_name ILIKE :exact THEN 1.0
                       WHEN company_name ILIKE :starts THEN 0.8
                       ELSE 0.6
                   END as score
            FROM portfolio_companies
            WHERE company_name ILIKE :pattern
            ORDER BY score DESC, company_name
            LIMIT :limit OFFSET :offset
        """),
            {
                "pattern": f"%{q}%",
                "exact": q,
                "starts": f"{q}%",
                "limit": per_page if not type else per_page,
                "offset": offset if not type else offset,
            },
        )

        for row in company_result:
            results.append(
                SearchResult(
                    id=row[0],
                    name=row[1],
                    type=row[2],
                    snippet=None,
                    relevance_score=float(row[3]),
                ).model_dump()
            )

        # Count companies
        count_result = db.execute(
            text("""
            SELECT COUNT(*) FROM portfolio_companies WHERE company_name ILIKE :pattern
        """),
            {"pattern": f"%{q}%"},
        )
        total += count_result.fetchone()[0]

    # Sort by relevance
    results.sort(key=lambda x: x["relevance_score"], reverse=True)

    # Trim to page size if searching both types
    if not type:
        results = results[:per_page]

    # Record usage
    response_time = int((time.time() - start_time) * 1000)
    service = APIKeyService(db)
    service.record_usage(
        key_id=request.state.api_key_id,
        endpoint="/public/search",
        method="GET",
        status_code=200,
        response_time_ms=response_time,
    )

    response = PaginatedResponse(
        data=results,
        total=total,
        page=page,
        per_page=per_page,
        has_more=(page * per_page) < total,
    )

    return JSONResponse(
        content=response.model_dump(), headers=add_rate_limit_headers(request)
    )
