"""
Corporate Registry API endpoints.

Provides access to global company registry data via OpenCorporates API.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from app.sources.opencorporates import OpenCorporatesClient

router = APIRouter(prefix="/corporate-registry", tags=["corporate-registry"])


def get_client() -> OpenCorporatesClient:
    """Get OpenCorporates client instance."""
    return OpenCorporatesClient()


@router.get("/search")
def search_companies(
    query: str = Query(..., min_length=1, description="Company name search query"),
    jurisdiction: Optional[str] = Query(None, description="Jurisdiction code (e.g., us_de, gb)"),
    company_type: Optional[str] = Query(None, description="Company type filter"),
    status: Optional[str] = Query(None, description="Current status filter (e.g., Active, Dissolved)"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(30, ge=1, le=100, description="Results per page"),
):
    """
    Search companies by name across global registries.

    - **query**: Company name to search for
    - **jurisdiction**: Optional jurisdiction filter (e.g., 'us_de' for Delaware, 'gb' for UK)
    - **company_type**: Optional company type filter
    - **status**: Optional status filter (Active, Dissolved, etc.)
    - **page**: Page number (default: 1)
    - **per_page**: Results per page (default: 30, max: 100)
    """
    try:
        with get_client() as client:
            result = client.search_companies(
                query=query,
                jurisdiction=jurisdiction,
                company_type=company_type,
                current_status=status,
                page=page,
                per_page=per_page,
            )
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenCorporates API error: {str(e)}")


@router.get("/company/{jurisdiction}/{company_number}")
def get_company(
    jurisdiction: str,
    company_number: str,
):
    """
    Get company details by jurisdiction and company number.

    - **jurisdiction**: Jurisdiction code (e.g., 'us_de' for Delaware, 'gb' for UK)
    - **company_number**: Company registration number

    Example: `/company/us_de/12345` for a Delaware company
    """
    try:
        with get_client() as client:
            company = client.get_company(jurisdiction, company_number)
            if not company:
                raise HTTPException(
                    status_code=404,
                    detail=f"Company not found: {jurisdiction}/{company_number}"
                )
            return company
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenCorporates API error: {str(e)}")


@router.get("/company/{jurisdiction}/{company_number}/officers")
def get_company_officers(
    jurisdiction: str,
    company_number: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(30, ge=1, le=100, description="Results per page"),
):
    """
    Get officers for a specific company.

    - **jurisdiction**: Jurisdiction code
    - **company_number**: Company registration number
    - **page**: Page number (default: 1)
    - **per_page**: Results per page (default: 30, max: 100)

    Returns list of officers including directors, executives, and secretaries.
    """
    try:
        with get_client() as client:
            result = client.get_company_officers(
                jurisdiction=jurisdiction,
                company_number=company_number,
                page=page,
                per_page=per_page,
            )
            result["jurisdiction"] = jurisdiction
            result["company_number"] = company_number
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenCorporates API error: {str(e)}")


@router.get("/company/{jurisdiction}/{company_number}/filings")
def get_company_filings(
    jurisdiction: str,
    company_number: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(30, ge=1, le=100, description="Results per page"),
):
    """
    Get filing history for a specific company.

    - **jurisdiction**: Jurisdiction code
    - **company_number**: Company registration number
    - **page**: Page number (default: 1)
    - **per_page**: Results per page (default: 30, max: 100)

    Returns list of corporate filings including annual reports, amendments, etc.
    """
    try:
        with get_client() as client:
            result = client.get_company_filings(
                jurisdiction=jurisdiction,
                company_number=company_number,
                page=page,
                per_page=per_page,
            )
            result["jurisdiction"] = jurisdiction
            result["company_number"] = company_number
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenCorporates API error: {str(e)}")


@router.get("/officers/search")
def search_officers(
    query: str = Query(..., min_length=1, description="Officer name search query"),
    jurisdiction: Optional[str] = Query(None, description="Jurisdiction code filter"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(30, ge=1, le=100, description="Results per page"),
):
    """
    Search officers by name across global registries.

    - **query**: Officer name to search for
    - **jurisdiction**: Optional jurisdiction filter
    - **page**: Page number (default: 1)
    - **per_page**: Results per page (default: 30, max: 100)

    Useful for finding all companies where a person serves as an officer.
    """
    try:
        with get_client() as client:
            result = client.search_officers(
                query=query,
                jurisdiction=jurisdiction,
                page=page,
                per_page=per_page,
            )
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenCorporates API error: {str(e)}")


@router.get("/jurisdictions")
def list_jurisdictions():
    """
    Get list of available jurisdictions.

    Returns all jurisdictions supported by OpenCorporates, including
    US states, countries, and special administrative regions.

    Jurisdiction codes are used in other endpoints (e.g., 'us_de' for Delaware).
    """
    try:
        with get_client() as client:
            jurisdictions = client.get_jurisdictions()
            return {
                "jurisdictions": jurisdictions,
                "count": len(jurisdictions),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenCorporates API error: {str(e)}")
