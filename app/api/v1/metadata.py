"""
Metadata endpoints.

Query variable definitions and column mappings.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models import CensusVariableMetadata

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/census/metadata", tags=["census-metadata"])


class VariableInfo(BaseModel):
    """Variable metadata response."""
    variable_name: str
    column_name: str
    label: str
    concept: Optional[str]
    predicate_type: Optional[str]
    postgres_type: Optional[str]
    
    model_config = {"from_attributes": True}


class DatasetMetadata(BaseModel):
    """Complete dataset metadata response."""
    dataset_id: str
    total_variables: int
    variables: List[VariableInfo]


@router.get("/variables/{dataset_id}", response_model=DatasetMetadata)
def get_dataset_variables(
    dataset_id: str,
    db: Session = Depends(get_db)
) -> DatasetMetadata:
    """
    Get all variable definitions for a dataset.
    
    Args:
        dataset_id: Dataset identifier (e.g., "acs5_2021_b01001")
    
    Returns:
        Complete variable metadata with human-readable labels
    
    Example:
        GET /api/v1/census/metadata/variables/acs5_2021_b01001
    """
    variables = db.query(CensusVariableMetadata).filter(
        CensusVariableMetadata.dataset_id == dataset_id
    ).order_by(CensusVariableMetadata.variable_name).all()
    
    if not variables:
        raise HTTPException(
            status_code=404,
            detail=f"No metadata found for dataset: {dataset_id}"
        )
    
    return DatasetMetadata(
        dataset_id=dataset_id,
        total_variables=len(variables),
        variables=[VariableInfo.model_validate(v) for v in variables]
    )


@router.get("/search", response_model=List[VariableInfo])
def search_variables(
    dataset_id: str = Query(..., description="Dataset to search within"),
    query: str = Query(..., description="Search term (searches in label and concept)", min_length=2),
    db: Session = Depends(get_db)
) -> List[VariableInfo]:
    """
    Search for variables by label or concept.
    
    Args:
        dataset_id: Dataset identifier (e.g., "acs5_2021_b01001")
        query: Search term to find in labels/concepts
    
    Returns:
        List of matching variables
    
    Example:
        GET /api/v1/census/metadata/search?dataset_id=acs5_2021_b01001&query=male
    """
    search_term = f"%{query}%"
    
    variables = db.query(CensusVariableMetadata).filter(
        CensusVariableMetadata.dataset_id == dataset_id,
        (CensusVariableMetadata.label.ilike(search_term) | 
         CensusVariableMetadata.concept.ilike(search_term))
    ).order_by(CensusVariableMetadata.variable_name).all()
    
    return [VariableInfo.model_validate(v) for v in variables]


@router.get("/column/{dataset_id}/{column_name}", response_model=VariableInfo)
def get_column_info(
    dataset_id: str,
    column_name: str,
    db: Session = Depends(get_db)
) -> VariableInfo:
    """
    Get information about a specific column.
    
    Args:
        dataset_id: Dataset identifier (e.g., "acs5_2021_b01001")
        column_name: Column name in database (e.g., "b01001_001e")
    
    Returns:
        Variable metadata with human-readable label
    
    Example:
        GET /api/v1/census/metadata/column/acs5_2021_b01001/b01001_001e
    """
    variable = db.query(CensusVariableMetadata).filter(
        CensusVariableMetadata.dataset_id == dataset_id,
        CensusVariableMetadata.column_name == column_name.lower()
    ).first()
    
    if not variable:
        raise HTTPException(
            status_code=404,
            detail=f"No metadata found for column: {column_name} in dataset: {dataset_id}"
        )
    
    return VariableInfo.model_validate(variable)


@router.get("/datasets", response_model=List[str])
def list_datasets_with_metadata(db: Session = Depends(get_db)) -> List[str]:
    """
    List all datasets that have metadata stored.
    
    Returns:
        List of dataset IDs with available metadata
    
    Example:
        GET /api/v1/census/metadata/datasets
    """
    results = db.query(CensusVariableMetadata.dataset_id).distinct().all()
    return [r[0] for r in results]

