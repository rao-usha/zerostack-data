"""
Data Lineage Tracking Service.

Provides comprehensive data lineage tracking including:
- Node management (sources, tables, jobs, transformations)
- Edge management (data flow relationships)
- Event logging (audit trail)
- Dataset versioning
- Impact analysis
"""

import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import (
    LineageNode,
    LineageEdge,
    LineageEvent,
    DatasetVersion,
    ImpactAnalysis,
    LineageNodeType,
    LineageEdgeType,
    IngestionJob,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Lineage Service
# =============================================================================


class LineageService:
    """Service for managing data lineage tracking."""

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------------
    # Node Management
    # -------------------------------------------------------------------------

    def get_or_create_node(
        self,
        node_type: LineageNodeType,
        node_id: str,
        name: str,
        source: Optional[str] = None,
        description: Optional[str] = None,
        properties: Optional[Dict] = None,
    ) -> LineageNode:
        """
        Get existing node or create new one.

        Returns the current version of the node.
        """
        existing = (
            self.db.query(LineageNode)
            .filter(
                LineageNode.node_type == node_type,
                LineageNode.node_id == node_id,
                LineageNode.is_current == 1,
            )
            .first()
        )

        if existing:
            return existing

        node = LineageNode(
            node_type=node_type,
            node_id=node_id,
            name=name,
            source=source,
            description=description,
            properties=properties,
            version=1,
            is_current=1,
        )
        self.db.add(node)
        self.db.commit()
        self.db.refresh(node)

        logger.info(f"Created lineage node: {node_type.value}/{node_id}")
        return node

    def get_node(self, node_id: int) -> Optional[LineageNode]:
        """Get a node by its database ID."""
        return self.db.query(LineageNode).filter(LineageNode.id == node_id).first()

    def get_node_by_type_and_id(
        self, node_type: LineageNodeType, node_id: str
    ) -> Optional[LineageNode]:
        """Get current version of a node by type and ID."""
        return (
            self.db.query(LineageNode)
            .filter(
                LineageNode.node_type == node_type,
                LineageNode.node_id == node_id,
                LineageNode.is_current == 1,
            )
            .first()
        )

    def update_node(
        self, node: LineageNode, create_version: bool = False, **updates
    ) -> LineageNode:
        """
        Update a node, optionally creating a new version.

        If create_version is True, creates a new version and marks old as superseded.
        """
        if create_version:
            # Mark current version as not current
            node.is_current = 0

            # Create new version
            new_node = LineageNode(
                node_type=node.node_type,
                node_id=node.node_id,
                name=updates.get("name", node.name),
                source=updates.get("source", node.source),
                description=updates.get("description", node.description),
                properties=updates.get("properties", node.properties),
                version=node.version + 1,
                is_current=1,
            )
            self.db.add(new_node)
            self.db.commit()
            self.db.refresh(new_node)
            return new_node
        else:
            # Update in place
            for key, value in updates.items():
                if hasattr(node, key):
                    setattr(node, key, value)
            self.db.commit()
            self.db.refresh(node)
            return node

    def list_nodes(
        self,
        node_type: Optional[LineageNodeType] = None,
        source: Optional[str] = None,
        current_only: bool = True,
        limit: int = 100,
    ) -> List[LineageNode]:
        """List nodes with optional filtering."""
        query = self.db.query(LineageNode)

        if current_only:
            query = query.filter(LineageNode.is_current == 1)

        if node_type:
            query = query.filter(LineageNode.node_type == node_type)

        if source:
            query = query.filter(LineageNode.source == source)

        return query.order_by(LineageNode.created_at.desc()).limit(limit).all()

    # -------------------------------------------------------------------------
    # Edge Management
    # -------------------------------------------------------------------------

    def create_edge(
        self,
        source_node_id: int,
        target_node_id: int,
        edge_type: LineageEdgeType,
        job_id: Optional[int] = None,
        properties: Optional[Dict] = None,
    ) -> LineageEdge:
        """Create an edge between two nodes."""
        # Check if edge already exists
        existing = (
            self.db.query(LineageEdge)
            .filter(
                LineageEdge.source_node_id == source_node_id,
                LineageEdge.target_node_id == target_node_id,
                LineageEdge.edge_type == edge_type,
            )
            .first()
        )

        if existing:
            # Update properties if provided
            if properties:
                existing.properties = properties
                existing.job_id = job_id
                self.db.commit()
            return existing

        edge = LineageEdge(
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            edge_type=edge_type,
            job_id=job_id,
            properties=properties,
        )
        self.db.add(edge)
        self.db.commit()
        self.db.refresh(edge)

        logger.debug(
            f"Created lineage edge: {source_node_id} --{edge_type.value}--> {target_node_id}"
        )
        return edge

    def get_upstream(self, node_id: int, max_depth: int = 10) -> List[Dict]:
        """
        Get all upstream nodes (data sources) for a node.

        Returns list of nodes that flow INTO this node.
        """
        result = []
        visited = set()

        def traverse(current_id: int, depth: int):
            if depth > max_depth or current_id in visited:
                return
            visited.add(current_id)

            edges = (
                self.db.query(LineageEdge)
                .filter(LineageEdge.target_node_id == current_id)
                .all()
            )

            for edge in edges:
                source_node = self.get_node(edge.source_node_id)
                if source_node:
                    result.append(
                        {
                            "node": source_node,
                            "edge_type": edge.edge_type.value,
                            "depth": depth,
                        }
                    )
                    traverse(edge.source_node_id, depth + 1)

        traverse(node_id, 1)
        return result

    def get_downstream(self, node_id: int, max_depth: int = 10) -> List[Dict]:
        """
        Get all downstream nodes (dependents) for a node.

        Returns list of nodes that receive data FROM this node.
        """
        result = []
        visited = set()

        def traverse(current_id: int, depth: int):
            if depth > max_depth or current_id in visited:
                return
            visited.add(current_id)

            edges = (
                self.db.query(LineageEdge)
                .filter(LineageEdge.source_node_id == current_id)
                .all()
            )

            for edge in edges:
                target_node = self.get_node(edge.target_node_id)
                if target_node:
                    result.append(
                        {
                            "node": target_node,
                            "edge_type": edge.edge_type.value,
                            "depth": depth,
                        }
                    )
                    traverse(edge.target_node_id, depth + 1)

        traverse(node_id, 1)
        return result

    def get_full_lineage(self, node_id: int) -> Dict:
        """Get complete lineage graph for a node (both upstream and downstream)."""
        node = self.get_node(node_id)
        if not node:
            return {}

        return {
            "node": {
                "id": node.id,
                "type": node.node_type.value,
                "node_id": node.node_id,
                "name": node.name,
                "source": node.source,
            },
            "upstream": [
                {
                    "id": item["node"].id,
                    "type": item["node"].node_type.value,
                    "name": item["node"].name,
                    "edge_type": item["edge_type"],
                    "depth": item["depth"],
                }
                for item in self.get_upstream(node_id)
            ],
            "downstream": [
                {
                    "id": item["node"].id,
                    "type": item["node"].node_type.value,
                    "name": item["node"].name,
                    "edge_type": item["edge_type"],
                    "depth": item["depth"],
                }
                for item in self.get_downstream(node_id)
            ],
        }

    # -------------------------------------------------------------------------
    # Event Logging
    # -------------------------------------------------------------------------

    def log_event(
        self,
        event_type: str,
        job_id: Optional[int] = None,
        node_id: Optional[int] = None,
        source: Optional[str] = None,
        description: Optional[str] = None,
        properties: Optional[Dict] = None,
        rows_affected: Optional[int] = None,
        bytes_processed: Optional[int] = None,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> LineageEvent:
        """Log a lineage event."""
        event = LineageEvent(
            event_type=event_type,
            job_id=job_id,
            node_id=node_id,
            source=source,
            description=description,
            properties=properties,
            rows_affected=rows_affected,
            bytes_processed=bytes_processed,
            success=1 if success else 0,
            error_message=error_message,
        )
        self.db.add(event)
        self.db.commit()
        self.db.refresh(event)

        logger.debug(f"Logged lineage event: {event_type} (job={job_id})")
        return event

    def get_events(
        self,
        event_type: Optional[str] = None,
        job_id: Optional[int] = None,
        source: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[LineageEvent]:
        """Get lineage events with filtering."""
        query = self.db.query(LineageEvent)

        if event_type:
            query = query.filter(LineageEvent.event_type == event_type)
        if job_id:
            query = query.filter(LineageEvent.job_id == job_id)
        if source:
            query = query.filter(LineageEvent.source == source)
        if since:
            query = query.filter(LineageEvent.created_at >= since)

        return query.order_by(LineageEvent.created_at.desc()).limit(limit).all()

    # -------------------------------------------------------------------------
    # Dataset Versioning
    # -------------------------------------------------------------------------

    def create_dataset_version(
        self,
        dataset_name: str,
        source: str,
        table_name: str,
        job_id: Optional[int] = None,
        schema_definition: Optional[Dict] = None,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
    ) -> DatasetVersion:
        """
        Create a new version of a dataset.

        Marks previous version as superseded.
        """
        # Get current version number
        current = (
            self.db.query(DatasetVersion)
            .filter(
                DatasetVersion.dataset_name == dataset_name,
                DatasetVersion.is_current == 1,
            )
            .first()
        )

        if current:
            new_version = current.version + 1
            current.is_current = 0
            current.superseded_at = datetime.utcnow()
        else:
            new_version = 1

        # Compute schema hash if provided
        schema_hash = None
        if schema_definition:
            schema_str = str(sorted(schema_definition.items()))
            schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()[:16]

        version = DatasetVersion(
            dataset_name=dataset_name,
            source=source,
            table_name=table_name,
            version=new_version,
            is_current=1,
            schema_hash=schema_hash,
            schema_definition=schema_definition,
            row_count=row_count,
            size_bytes=size_bytes,
            min_date=min_date,
            max_date=max_date,
            job_id=job_id,
        )
        self.db.add(version)
        self.db.commit()
        self.db.refresh(version)

        logger.info(f"Created dataset version: {dataset_name} v{new_version}")
        return version

    def get_dataset_version(
        self, dataset_name: str, version: Optional[int] = None
    ) -> Optional[DatasetVersion]:
        """Get a specific version of a dataset (or current if version is None)."""
        query = self.db.query(DatasetVersion).filter(
            DatasetVersion.dataset_name == dataset_name
        )

        if version:
            query = query.filter(DatasetVersion.version == version)
        else:
            query = query.filter(DatasetVersion.is_current == 1)

        return query.first()

    def get_dataset_history(
        self, dataset_name: str, limit: int = 20
    ) -> List[DatasetVersion]:
        """Get version history for a dataset."""
        return (
            self.db.query(DatasetVersion)
            .filter(DatasetVersion.dataset_name == dataset_name)
            .order_by(DatasetVersion.version.desc())
            .limit(limit)
            .all()
        )

    def list_datasets(
        self, source: Optional[str] = None, limit: int = 100
    ) -> List[DatasetVersion]:
        """List current versions of all datasets."""
        query = self.db.query(DatasetVersion).filter(DatasetVersion.is_current == 1)

        if source:
            query = query.filter(DatasetVersion.source == source)

        return query.order_by(DatasetVersion.dataset_name).limit(limit).all()

    # -------------------------------------------------------------------------
    # Impact Analysis
    # -------------------------------------------------------------------------

    def compute_impact(self, source_node_id: int) -> List[ImpactAnalysis]:
        """
        Compute impact analysis for a node.

        Finds all downstream nodes that would be affected by changes to this node.
        """
        source_node = self.get_node(source_node_id)
        if not source_node:
            return []

        # Clear existing impact analysis for this source
        self.db.query(ImpactAnalysis).filter(
            ImpactAnalysis.source_node_id == source_node_id
        ).delete()

        results = []
        downstream = self.get_downstream(source_node_id)

        for item in downstream:
            impacted_node = item["node"]
            impact = ImpactAnalysis(
                source_node_id=source_node_id,
                source_node_name=source_node.name,
                impacted_node_id=impacted_node.id,
                impacted_node_name=impacted_node.name,
                impacted_node_type=impacted_node.node_type.value,
                impact_level=item["depth"],
            )
            self.db.add(impact)
            results.append(impact)

        self.db.commit()
        logger.info(
            f"Computed impact analysis for node {source_node_id}: {len(results)} impacts"
        )
        return results

    def get_impact_analysis(self, source_node_id: int) -> List[ImpactAnalysis]:
        """Get cached impact analysis for a node."""
        return (
            self.db.query(ImpactAnalysis)
            .filter(ImpactAnalysis.source_node_id == source_node_id)
            .order_by(ImpactAnalysis.impact_level)
            .all()
        )

    # -------------------------------------------------------------------------
    # Job Lineage Integration
    # -------------------------------------------------------------------------

    def record_job_lineage(
        self,
        job: IngestionJob,
        table_name: str,
        rows_inserted: int,
        source_api_url: Optional[str] = None,
        schema_definition: Optional[Dict] = None,
    ) -> Dict:
        """
        Record complete lineage for a job execution.

        Creates nodes, edges, events, and dataset version.
        """
        # 1. Create/get external API node
        api_node = self.get_or_create_node(
            node_type=LineageNodeType.EXTERNAL_API,
            node_id=f"{job.source}_api",
            name=f"{job.source.upper()} API",
            source=job.source,
            properties={"api_url": source_api_url} if source_api_url else None,
        )

        # 2. Create job node
        job_node = self.get_or_create_node(
            node_type=LineageNodeType.INGESTION_JOB,
            node_id=f"job_{job.id}",
            name=f"Job {job.id}: {job.source}",
            source=job.source,
            properties={
                "job_id": job.id,
                "config": job.config,
                "status": job.status.value
                if hasattr(job.status, "value")
                else str(job.status),
            },
        )

        # 3. Create/get table node
        table_node = self.get_or_create_node(
            node_type=LineageNodeType.DATABASE_TABLE,
            node_id=table_name,
            name=table_name,
            source=job.source,
            properties={"table": table_name, "schema": "public"},
        )

        # 4. Create edges: API -> Job -> Table
        self.create_edge(
            source_node_id=api_node.id,
            target_node_id=job_node.id,
            edge_type=LineageEdgeType.CONSUMES,
            job_id=job.id,
            properties={"config": job.config},
        )

        self.create_edge(
            source_node_id=job_node.id,
            target_node_id=table_node.id,
            edge_type=LineageEdgeType.PRODUCES,
            job_id=job.id,
            properties={"rows": rows_inserted},
        )

        # 5. Log ingestion event
        event = self.log_event(
            event_type="ingest",
            job_id=job.id,
            node_id=table_node.id,
            source=job.source,
            description=f"Ingested {rows_inserted} rows into {table_name}",
            properties={
                "table": table_name,
                "config": job.config,
                "api_url": source_api_url,
            },
            rows_affected=rows_inserted,
            success=True,
        )

        # 6. Create dataset version
        dataset_name = f"{job.source}_{table_name.replace('_', '_')}"
        version = self.create_dataset_version(
            dataset_name=dataset_name,
            source=job.source,
            table_name=table_name,
            job_id=job.id,
            schema_definition=schema_definition,
            row_count=rows_inserted,
        )

        return {
            "api_node_id": api_node.id,
            "job_node_id": job_node.id,
            "table_node_id": table_node.id,
            "event_id": event.id,
            "dataset_version": version.version,
        }

    def record_job_failure(self, job: IngestionJob, error_message: str) -> LineageEvent:
        """Record a failed job in lineage."""
        return self.log_event(
            event_type="ingest",
            job_id=job.id,
            source=job.source,
            description=f"Job {job.id} failed",
            properties={"config": job.config},
            success=False,
            error_message=error_message,
        )


# =============================================================================
# Helper Functions
# =============================================================================


def get_table_schema(db: Session, table_name: str) -> Optional[Dict]:
    """Get schema definition for a table."""
    try:
        result = db.execute(
            text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """),
            {"table_name": table_name},
        )

        columns = []
        for row in result:
            columns.append(
                {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
            )

        return {"columns": columns} if columns else None
    except Exception as e:
        logger.warning(f"Failed to get schema for {table_name}: {e}")
        return None


def get_table_stats(db: Session, table_name: str) -> Dict:
    """Get row count and size for a table."""
    try:
        result = db.execute(
            text(f"""
            SELECT
                (SELECT COUNT(*) FROM {table_name}) as row_count,
                pg_total_relation_size(:table_name) as size_bytes
        """),
            {"table_name": table_name},
        )
        row = result.fetchone()
        return {"row_count": row[0] if row else 0, "size_bytes": row[1] if row else 0}
    except Exception as e:
        logger.warning(f"Failed to get stats for {table_name}: {e}")
        return {"row_count": 0, "size_bytes": 0}
