"""
API Key Management Endpoints.

T19: Public API with Auth & Rate Limits
- Create, list, update, and revoke API keys
- View usage statistics
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.auth.api_keys import (
    APIKeyService,
    APIKeyCreate,
    APIKeyUpdate,
    APIKeyResponse,
    APIKeyCreatedResponse,
    UsageStatsResponse
)

router = APIRouter(prefix="/api-keys", tags=["API Keys"])


def get_service(db: Session = Depends(get_db)) -> APIKeyService:
    """Dependency for API key service."""
    return APIKeyService(db)


@router.post("", response_model=APIKeyCreatedResponse, status_code=status.HTTP_201_CREATED)
def create_api_key(
    data: APIKeyCreate,
    service: APIKeyService = Depends(get_service)
):
    """
    Create a new API key.

    The full key is returned only once in this response.
    Store it securely - it cannot be retrieved again.

    **Scopes:**
    - `read`: Read-only access to public endpoints
    - `write`: Read and write access
    - `admin`: Full access including management endpoints
    """
    return service.create_key(data)


@router.get("", response_model=List[APIKeyResponse])
def list_api_keys(
    owner_email: Optional[str] = None,
    service: APIKeyService = Depends(get_service)
):
    """
    List API keys.

    Optionally filter by owner email address.
    Keys are returned without the secret portion.
    """
    return service.list_keys(owner_email)


@router.get("/{key_id}", response_model=APIKeyResponse)
def get_api_key(
    key_id: int,
    service: APIKeyService = Depends(get_service)
):
    """
    Get details of a specific API key.

    Returns key metadata without the secret.
    """
    key = service.get_key(key_id)
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    return key


@router.patch("/{key_id}", response_model=APIKeyResponse)
def update_api_key(
    key_id: int,
    data: APIKeyUpdate,
    service: APIKeyService = Depends(get_service)
):
    """
    Update an API key.

    Can update name, rate limits, and active status.
    """
    existing = service.get_key(key_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )

    updated = service.update_key(key_id, data)
    return updated


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_api_key(
    key_id: int,
    service: APIKeyService = Depends(get_service)
):
    """
    Revoke an API key.

    The key will be marked as inactive and can no longer be used
    for authentication. This action cannot be undone.
    """
    success = service.revoke_key(key_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    return None


@router.get("/{key_id}/usage", response_model=UsageStatsResponse)
def get_api_key_usage(
    key_id: int,
    service: APIKeyService = Depends(get_service)
):
    """
    Get usage statistics for an API key.

    Returns:
    - Total requests
    - Requests today and this month
    - Daily breakdown (last 30 days)
    - Breakdown by endpoint
    """
    usage = service.get_usage(key_id)
    if not usage:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    return usage
