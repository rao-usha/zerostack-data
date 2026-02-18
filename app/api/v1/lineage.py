"""
Data Lineage API endpoints.

Provides access to data lineage tracking, including:
- Lineage graph queries (upstream/downstream)
- Event history
- Dataset versions
- Impact analysis
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    LineageNode,
    LineageEdge,
    LineageEvent,
    DatasetVersion,
    ImpactAnalysis,
    LineageNodeType,
    LineageEdgeType,
)
from app.core.lineage_service import LineageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lineage", tags=["lineage"])


# =============================================================================
# Pydantic Schemas
# =============================================================================


class NodeCreate(BaseModel):
    """Schema for creating a lineage node."""

    node_type: str = Field(
        ...,
        description="Node type: external_api, database_table, ingestion_job, dataset, file, transformation",
    )
    node_id: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=255)
    source: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class NodeResponse(BaseModel):
    """Response schema for a lineage node."""

    id: int
    node_type: str
    node_id: str
    name: str
    source: Optional[str]
    description: Optional[str]
    properties: Optional[Dict[str, Any]]
    version: int
    is_current: bool
    created_at: str
    updated_at: str


class EdgeCreate(BaseModel):
    """Schema for creating a lineage edge."""

    source_node_id: int
    target_node_id: int
    edge_type: str = Field(
        ...,
        description="Edge type: produces, consumes, derives_from, stored_in, exported_to",
    )
    job_id: Optional[int] = None
    properties: Optional[Dict[str, Any]] = None


class EdgeResponse(BaseModel):
    """Response schema for a lineage edge."""

    id: int
    source_node_id: int
    target_node_id: int
    edge_type: str
    job_id: Optional[int]
    properties: Optional[Dict[str, Any]]
    created_at: str


class EventResponse(BaseModel):
    """Response schema for a lineage event."""

    id: int
    event_type: str
    job_id: Optional[int]
    node_id: Optional[int]
    source: Optional[str]
    description: Optional[str]
    properties: Optional[Dict[str, Any]]
    rows_affected: Optional[int]
    bytes_processed: Optional[int]
    success: bool
    error_message: Optional[str]
    created_at: str


class DatasetVersionResponse(BaseModel):
    """Response schema for a dataset version."""

    id: int
    dataset_name: str
    source: str
    table_name: str
    version: int
    is_current: bool
    schema_hash: Optional[str]
    schema_definition: Optional[Dict[str, Any]]
    row_count: Optional[int]
    size_bytes: Optional[int]
    min_date: Optional[str]
    max_date: Optional[str]
    job_id: Optional[int]
    created_at: str
    superseded_at: Optional[str]


class LineageGraphResponse(BaseModel):
    """Response schema for a lineage graph."""

    node: Dict[str, Any]
    upstream: List[Dict[str, Any]]
    downstream: List[Dict[str, Any]]


class ImpactResponse(BaseModel):
    """Response schema for impact analysis."""

    source_node_id: int
    source_node_name: str
    impacted_node_id: int
    impacted_node_name: str
    impacted_node_type: str
    impact_level: int
    computed_at: str


# =============================================================================
# Helper Functions
# =============================================================================


def node_to_response(node: LineageNode) -> NodeResponse:
    """Convert node model to response."""
    return NodeResponse(
        id=node.id,
        node_type=node.node_type.value,
        node_id=node.node_id,
        name=node.name,
        source=node.source,
        description=node.description,
        properties=node.properties,
        version=node.version,
        is_current=bool(node.is_current),
        created_at=node.created_at.isoformat(),
        updated_at=node.updated_at.isoformat(),
    )


def edge_to_response(edge: LineageEdge) -> EdgeResponse:
    """Convert edge model to response."""
    return EdgeResponse(
        id=edge.id,
        source_node_id=edge.source_node_id,
        target_node_id=edge.target_node_id,
        edge_type=edge.edge_type.value,
        job_id=edge.job_id,
        properties=edge.properties,
        created_at=edge.created_at.isoformat(),
    )


def event_to_response(event: LineageEvent) -> EventResponse:
    """Convert event model to response."""
    return EventResponse(
        id=event.id,
        event_type=event.event_type,
        job_id=event.job_id,
        node_id=event.node_id,
        source=event.source,
        description=event.description,
        properties=event.properties,
        rows_affected=event.rows_affected,
        bytes_processed=event.bytes_processed,
        success=bool(event.success),
        error_message=event.error_message,
        created_at=event.created_at.isoformat(),
    )


def version_to_response(version: DatasetVersion) -> DatasetVersionResponse:
    """Convert dataset version model to response."""
    return DatasetVersionResponse(
        id=version.id,
        dataset_name=version.dataset_name,
        source=version.source,
        table_name=version.table_name,
        version=version.version,
        is_current=bool(version.is_current),
        schema_hash=version.schema_hash,
        schema_definition=version.schema_definition,
        row_count=version.row_count,
        size_bytes=version.size_bytes,
        min_date=version.min_date.isoformat() if version.min_date else None,
        max_date=version.max_date.isoformat() if version.max_date else None,
        job_id=version.job_id,
        created_at=version.created_at.isoformat(),
        superseded_at=version.superseded_at.isoformat()
        if version.superseded_at
        else None,
    )


# =============================================================================
# Node Endpoints
# =============================================================================


@router.get("/nodes", response_model=List[NodeResponse])
def list_nodes(
    node_type: Optional[str] = Query(default=None, description="Filter by node type"),
    source: Optional[str] = Query(default=None, description="Filter by data source"),
    current_only: bool = Query(default=True, description="Only show current versions"),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[NodeResponse]:
    """List lineage nodes with optional filtering."""
    service = LineageService(db)

    node_type_enum = None
    if node_type:
        try:
            node_type_enum = LineageNodeType(node_type)
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Invalid node_type: {node_type}"
            )

    nodes = service.list_nodes(
        node_type=node_type_enum, source=source, current_only=current_only, limit=limit
    )
    return [node_to_response(n) for n in nodes]


@router.get("/nodes/types")
def list_node_types():
    """List available node types."""
    return {
        "types": [
            {"value": t.value, "name": t.name, "description": _get_type_description(t)}
            for t in LineageNodeType
        ]
    }


def _get_type_description(t: LineageNodeType) -> str:
    """Get description for a node type."""
    descriptions = {
        LineageNodeType.EXTERNAL_API: "External data source API (Census, FRED, etc.)",
        LineageNodeType.DATABASE_TABLE: "PostgreSQL database table",
        LineageNodeType.INGESTION_JOB: "Data ingestion job",
        LineageNodeType.DATASET: "Logical dataset (may span multiple tables)",
        LineageNodeType.FILE: "Exported file (CSV, Parquet, etc.)",
        LineageNodeType.TRANSFORMATION: "Data transformation step",
    }
    return descriptions.get(t, "")


@router.get("/nodes/{node_id}", response_model=NodeResponse)
def get_node(node_id: int, db: Session = Depends(get_db)) -> NodeResponse:
    """Get a lineage node by ID."""
    service = LineageService(db)
    node = service.get_node(node_id)

    if not node:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    return node_to_response(node)


@router.post("/nodes", response_model=NodeResponse, status_code=201)
def create_node(request: NodeCreate, db: Session = Depends(get_db)) -> NodeResponse:
    """Create a new lineage node."""
    service = LineageService(db)

    try:
        node_type = LineageNodeType(request.node_type)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid node_type: {request.node_type}"
        )

    node = service.get_or_create_node(
        node_type=node_type,
        node_id=request.node_id,
        name=request.name,
        source=request.source,
        description=request.description,
        properties=request.properties,
    )
    return node_to_response(node)


# =============================================================================
# Edge Endpoints
# =============================================================================


@router.get("/edges", response_model=List[EdgeResponse])
def list_edges(
    source_node_id: Optional[int] = Query(
        default=None, description="Filter by source node"
    ),
    target_node_id: Optional[int] = Query(
        default=None, description="Filter by target node"
    ),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[EdgeResponse]:
    """List lineage edges with optional filtering."""
    query = db.query(LineageEdge)

    if source_node_id:
        query = query.filter(LineageEdge.source_node_id == source_node_id)
    if target_node_id:
        query = query.filter(LineageEdge.target_node_id == target_node_id)

    edges = query.order_by(LineageEdge.created_at.desc()).limit(limit).all()
    return [edge_to_response(e) for e in edges]


@router.get("/edges/types")
def list_edge_types():
    """List available edge types."""
    return {"types": [{"value": t.value, "name": t.name} for t in LineageEdgeType]}


@router.post("/edges", response_model=EdgeResponse, status_code=201)
def create_edge(request: EdgeCreate, db: Session = Depends(get_db)) -> EdgeResponse:
    """Create a lineage edge between two nodes."""
    service = LineageService(db)

    try:
        edge_type = LineageEdgeType(request.edge_type)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid edge_type: {request.edge_type}"
        )

    # Verify nodes exist
    if not service.get_node(request.source_node_id):
        raise HTTPException(
            status_code=404, detail=f"Source node not found: {request.source_node_id}"
        )
    if not service.get_node(request.target_node_id):
        raise HTTPException(
            status_code=404, detail=f"Target node not found: {request.target_node_id}"
        )

    edge = service.create_edge(
        source_node_id=request.source_node_id,
        target_node_id=request.target_node_id,
        edge_type=edge_type,
        job_id=request.job_id,
        properties=request.properties,
    )
    return edge_to_response(edge)


# =============================================================================
# Graph Traversal Endpoints
# =============================================================================


@router.get("/nodes/{node_id}/upstream", response_model=List[Dict])
def get_upstream(
    node_id: int,
    max_depth: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
):
    """
    Get upstream lineage for a node.

    Returns all data sources that flow INTO this node.
    """
    service = LineageService(db)

    if not service.get_node(node_id):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    upstream = service.get_upstream(node_id, max_depth=max_depth)
    return [
        {
            "id": item["node"].id,
            "type": item["node"].node_type.value,
            "node_id": item["node"].node_id,
            "name": item["node"].name,
            "source": item["node"].source,
            "edge_type": item["edge_type"],
            "depth": item["depth"],
        }
        for item in upstream
    ]


@router.get("/nodes/{node_id}/downstream", response_model=List[Dict])
def get_downstream(
    node_id: int,
    max_depth: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
):
    """
    Get downstream lineage for a node.

    Returns all nodes that receive data FROM this node.
    """
    service = LineageService(db)

    if not service.get_node(node_id):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    downstream = service.get_downstream(node_id, max_depth=max_depth)
    return [
        {
            "id": item["node"].id,
            "type": item["node"].node_type.value,
            "node_id": item["node"].node_id,
            "name": item["node"].name,
            "source": item["node"].source,
            "edge_type": item["edge_type"],
            "depth": item["depth"],
        }
        for item in downstream
    ]


@router.get("/nodes/{node_id}/graph", response_model=LineageGraphResponse)
def get_lineage_graph(node_id: int, db: Session = Depends(get_db)):
    """
    Get complete lineage graph for a node.

    Returns both upstream and downstream lineage.
    """
    service = LineageService(db)

    if not service.get_node(node_id):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    return service.get_full_lineage(node_id)


# =============================================================================
# Event Endpoints
# =============================================================================


@router.get("/events", response_model=List[EventResponse])
def list_events(
    event_type: Optional[str] = Query(default=None, description="Filter by event type"),
    job_id: Optional[int] = Query(default=None, description="Filter by job ID"),
    source: Optional[str] = Query(default=None, description="Filter by data source"),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[EventResponse]:
    """List lineage events with optional filtering."""
    service = LineageService(db)
    events = service.get_events(
        event_type=event_type, job_id=job_id, source=source, limit=limit
    )
    return [event_to_response(e) for e in events]


@router.get("/events/types")
def list_event_types():
    """List available event types."""
    return {
        "types": [
            {"value": "ingest", "description": "Data ingestion from external source"},
            {"value": "transform", "description": "Data transformation"},
            {
                "value": "export",
                "description": "Data export to file or external system",
            },
            {"value": "delete", "description": "Data deletion"},
            {"value": "schema_change", "description": "Schema modification"},
        ]
    }


# =============================================================================
# Dataset Version Endpoints
# =============================================================================


@router.get("/datasets", response_model=List[DatasetVersionResponse])
def list_datasets(
    source: Optional[str] = Query(default=None, description="Filter by data source"),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[DatasetVersionResponse]:
    """List current versions of all datasets."""
    service = LineageService(db)
    datasets = service.list_datasets(source=source, limit=limit)
    return [version_to_response(d) for d in datasets]


@router.get("/datasets/{dataset_name}", response_model=DatasetVersionResponse)
def get_dataset(
    dataset_name: str,
    version: Optional[int] = Query(
        default=None, description="Specific version (default: current)"
    ),
    db: Session = Depends(get_db),
) -> DatasetVersionResponse:
    """Get a dataset version."""
    service = LineageService(db)
    dataset = service.get_dataset_version(dataset_name, version=version)

    if not dataset:
        raise HTTPException(
            status_code=404, detail=f"Dataset not found: {dataset_name}"
        )

    return version_to_response(dataset)


@router.get(
    "/datasets/{dataset_name}/history", response_model=List[DatasetVersionResponse]
)
def get_dataset_history(
    dataset_name: str,
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> List[DatasetVersionResponse]:
    """Get version history for a dataset."""
    service = LineageService(db)
    versions = service.get_dataset_history(dataset_name, limit=limit)

    if not versions:
        raise HTTPException(
            status_code=404, detail=f"Dataset not found: {dataset_name}"
        )

    return [version_to_response(v) for v in versions]


# =============================================================================
# Impact Analysis Endpoints
# =============================================================================


@router.post("/nodes/{node_id}/impact", response_model=List[ImpactResponse])
def compute_impact_analysis(node_id: int, db: Session = Depends(get_db)):
    """
    Compute impact analysis for a node.

    Finds all downstream nodes that would be affected by changes.
    """
    service = LineageService(db)

    if not service.get_node(node_id):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    impacts = service.compute_impact(node_id)
    return [
        ImpactResponse(
            source_node_id=i.source_node_id,
            source_node_name=i.source_node_name,
            impacted_node_id=i.impacted_node_id,
            impacted_node_name=i.impacted_node_name,
            impacted_node_type=i.impacted_node_type,
            impact_level=i.impact_level,
            computed_at=i.computed_at.isoformat(),
        )
        for i in impacts
    ]


@router.get("/nodes/{node_id}/impact", response_model=List[ImpactResponse])
def get_impact_analysis(node_id: int, db: Session = Depends(get_db)):
    """Get cached impact analysis for a node."""
    service = LineageService(db)

    if not service.get_node(node_id):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    impacts = service.get_impact_analysis(node_id)
    return [
        ImpactResponse(
            source_node_id=i.source_node_id,
            source_node_name=i.source_node_name,
            impacted_node_id=i.impacted_node_id,
            impacted_node_name=i.impacted_node_name,
            impacted_node_type=i.impacted_node_type,
            impact_level=i.impact_level,
            computed_at=i.computed_at.isoformat(),
        )
        for i in impacts
    ]


# =============================================================================
# Summary Endpoints
# =============================================================================


@router.get("/summary")
def get_lineage_summary(db: Session = Depends(get_db)):
    """Get summary statistics for lineage data."""
    node_counts = {}
    for node_type in LineageNodeType:
        count = (
            db.query(LineageNode)
            .filter(LineageNode.node_type == node_type, LineageNode.is_current == 1)
            .count()
        )
        node_counts[node_type.value] = count

    edge_counts = {}
    for edge_type in LineageEdgeType:
        count = db.query(LineageEdge).filter(LineageEdge.edge_type == edge_type).count()
        edge_counts[edge_type.value] = count

    total_events = db.query(LineageEvent).count()
    total_datasets = (
        db.query(DatasetVersion).filter(DatasetVersion.is_current == 1).count()
    )

    return {
        "nodes": {"total": sum(node_counts.values()), "by_type": node_counts},
        "edges": {"total": sum(edge_counts.values()), "by_type": edge_counts},
        "events": total_events,
        "datasets": total_datasets,
    }
