"""
Source Configuration API.

CRUD endpoints for per-source timeouts, retry policies, and rate limits.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core import source_config_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/source-configs", tags=["Source Configuration"])


class SourceConfigUpdate(BaseModel):
    """Request body for creating/updating a source config."""
    timeout_seconds: Optional[int] = Field(None, ge=30, le=86400, description="Job timeout in seconds")
    max_retries: Optional[int] = Field(None, ge=0, le=20)
    retry_backoff_base_min: Optional[int] = Field(None, ge=1, le=1440)
    retry_backoff_max_min: Optional[int] = Field(None, ge=1, le=10080)
    retry_backoff_multiplier: Optional[int] = Field(None, ge=1, le=10)
    rate_limit_rps: Optional[float] = Field(None, ge=0.1, le=100.0)
    max_concurrent: Optional[int] = Field(None, ge=1, le=50)
    description: Optional[str] = None


@router.get("")
async def list_configs(db: Session = Depends(get_db)):
    """List all source configurations."""
    configs = source_config_service.list_source_configs(db)
    return {
        "configs": configs,
        "total": len(configs),
        "global_defaults": source_config_service.GLOBAL_DEFAULTS,
    }


@router.get("/{source}")
async def get_config(source: str, db: Session = Depends(get_db)):
    """
    Get configuration for a specific source.

    Returns the stored config or global defaults if none exists.
    """
    return source_config_service.get_source_config(db, source)


@router.put("/{source}")
async def upsert_config(
    source: str,
    body: SourceConfigUpdate,
    db: Session = Depends(get_db),
):
    """Create or update configuration for a source."""
    kwargs = body.model_dump(exclude_none=True)
    if not kwargs:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = source_config_service.upsert_source_config(db, source, **kwargs)
    return result


@router.delete("/{source}")
async def delete_config(source: str, db: Session = Depends(get_db)):
    """Delete a source configuration (reverts to global defaults)."""
    deleted = source_config_service.delete_source_config(db, source)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"No config found for source '{source}'")
    return {"deleted": True, "source": source, "message": "Reverted to global defaults"}
