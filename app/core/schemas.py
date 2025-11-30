"""
Pydantic schemas for API requests and responses.
"""
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from app.core.models import JobStatus


class JobCreate(BaseModel):
    """Request schema for creating a new ingestion job."""
    source: str = Field(
        ...,
        description="Data source identifier (e.g., 'census', 'bls')",
        min_length=1,
        max_length=50
    )
    config: Dict[str, Any] = Field(
        ...,
        description="Source-specific configuration"
    )
    
    @field_validator("source")
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Validate source name."""
        # Convert to lowercase for consistency
        v = v.lower().strip()
        if not v:
            raise ValueError("source cannot be empty")
        return v


class JobResponse(BaseModel):
    """Response schema for job information."""
    id: int
    source: str
    status: JobStatus
    config: Dict[str, Any]
    
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    rows_inserted: Optional[int] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    
    model_config = {"from_attributes": True}


class DatasetInfo(BaseModel):
    """Response schema for dataset metadata."""
    id: int
    source: str
    dataset_id: str
    table_name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    source_metadata: Optional[Dict[str, Any]] = None
    created_at: datetime
    last_updated_at: datetime
    
    model_config = {"from_attributes": True}

