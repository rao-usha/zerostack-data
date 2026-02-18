"""
USPTO PatentsView ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.core.batch_operations import batch_insert
from app.sources.uspto.client import USPTOClient
from app.sources.uspto import metadata

logger = logging.getLogger(__name__)


# SQL for creating USPTO tables
CREATE_PATENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS uspto_patents (
    id SERIAL PRIMARY KEY,
    patent_id VARCHAR(20) UNIQUE NOT NULL,
    patent_title TEXT NOT NULL,
    patent_abstract TEXT,
    patent_date DATE,
    patent_type VARCHAR(50),
    num_claims INTEGER,
    num_citations INTEGER DEFAULT 0,
    inventors_json JSONB,
    assignees_json JSONB,
    cpc_codes_json JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_api VARCHAR(50) DEFAULT 'patentsview'
);

CREATE INDEX IF NOT EXISTS idx_uspto_patents_patent_id ON uspto_patents(patent_id);
CREATE INDEX IF NOT EXISTS idx_uspto_patents_date ON uspto_patents(patent_date);
CREATE INDEX IF NOT EXISTS idx_uspto_patents_type ON uspto_patents(patent_type);
"""

CREATE_INVENTORS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS uspto_inventors (
    id SERIAL PRIMARY KEY,
    inventor_id VARCHAR(50) UNIQUE NOT NULL,
    name_first VARCHAR(255),
    name_last VARCHAR(255),
    location_city VARCHAR(255),
    location_state VARCHAR(50),
    location_country VARCHAR(100),
    patent_count INTEGER,
    first_patent_date DATE,
    last_patent_date DATE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uspto_inventors_id ON uspto_inventors(inventor_id);
CREATE INDEX IF NOT EXISTS idx_uspto_inventors_name ON uspto_inventors(name_last, name_first);
"""

CREATE_ASSIGNEES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS uspto_assignees (
    id SERIAL PRIMARY KEY,
    assignee_id VARCHAR(50) UNIQUE NOT NULL,
    assignee_name VARCHAR(500),
    assignee_type VARCHAR(50),
    location_city VARCHAR(255),
    location_state VARCHAR(50),
    location_country VARCHAR(100),
    patent_count INTEGER,
    first_patent_date DATE,
    last_patent_date DATE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uspto_assignees_id ON uspto_assignees(assignee_id);
CREATE INDEX IF NOT EXISTS idx_uspto_assignees_name ON uspto_assignees(assignee_name);
"""


class USPTOIngestor(BaseSourceIngestor):
    """
    Ingestor for USPTO PatentsView data.

    Handles ingestion of patents, inventors, and assignees.
    """

    SOURCE_NAME = "uspto"

    def __init__(self, db: Session, api_key: Optional[str] = None):
        """
        Initialize USPTO ingestor.

        Args:
            db: SQLAlchemy database session
            api_key: Optional PatentsView API key
        """
        super().__init__(db)
        settings = get_settings()
        self.api_key = api_key or settings.get_api_key("uspto")
        self.settings = settings

    def _ensure_tables_exist(self):
        """Create USPTO tables if they don't exist."""
        try:
            self.db.execute(text(CREATE_PATENTS_TABLE_SQL))
            self.db.execute(text(CREATE_INVENTORS_TABLE_SQL))
            self.db.execute(text(CREATE_ASSIGNEES_TABLE_SQL))
            self.db.commit()
            logger.info("USPTO tables ensured")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create USPTO tables: {e}")
            raise

    def _transform_patent(self, patent_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform patent API response to database row.

        Args:
            patent_data: Patent data from PatentsView API

        Returns:
            Dict ready for database insertion
        """
        import json

        return {
            "patent_id": patent_data.get("patent_id"),
            "patent_title": patent_data.get("patent_title"),
            "patent_abstract": patent_data.get("patent_abstract"),
            "patent_date": patent_data.get("patent_date"),
            "patent_type": patent_data.get("patent_type"),
            "num_claims": patent_data.get("patent_num_claims"),
            "num_citations": patent_data.get("patent_num_cited_by_us_patents", 0),
            "inventors_json": json.dumps(patent_data.get("inventors", [])),
            "assignees_json": json.dumps(patent_data.get("assignees", [])),
            "cpc_codes_json": json.dumps(patent_data.get("cpc_current", [])),
            "ingested_at": datetime.utcnow(),
            "source_api": "patentsview",
        }

    def _transform_inventor(self, inventor_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform inventor API response to database row."""
        return {
            "inventor_id": inventor_data.get("inventor_id"),
            "name_first": inventor_data.get("inventor_name_first"),
            "name_last": inventor_data.get("inventor_name_last"),
            "location_city": inventor_data.get("inventor_city"),
            "location_state": inventor_data.get("inventor_state"),
            "location_country": inventor_data.get("inventor_country"),
            "patent_count": inventor_data.get("inventor_total_num_patents"),
            "first_patent_date": inventor_data.get("inventor_first_seen_date"),
            "last_patent_date": inventor_data.get("inventor_last_seen_date"),
            "ingested_at": datetime.utcnow(),
        }

    def _transform_assignee(self, assignee_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform assignee API response to database row."""
        return {
            "assignee_id": assignee_data.get("assignee_id"),
            "assignee_name": assignee_data.get("assignee_organization"),
            "assignee_type": metadata.get_assignee_type_description(
                assignee_data.get("assignee_type", "")
            ),
            "location_city": assignee_data.get("assignee_city"),
            "location_state": assignee_data.get("assignee_state"),
            "location_country": assignee_data.get("assignee_country"),
            "patent_count": assignee_data.get("assignee_total_num_patents"),
            "first_patent_date": assignee_data.get("assignee_first_seen_date"),
            "last_patent_date": assignee_data.get("assignee_last_seen_date"),
            "ingested_at": datetime.utcnow(),
        }

    async def ingest_patents_by_assignee(
        self,
        job_id: int,
        assignee_name: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        max_patents: int = 1000,
    ) -> Dict[str, Any]:
        """
        Ingest all patents for an assignee.

        Args:
            job_id: Ingestion job ID
            assignee_name: Assignee name to search
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            max_patents: Maximum patents to ingest

        Returns:
            Dictionary with ingestion results
        """
        client = USPTOClient(
            api_key=self.api_key,
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            # 2. Ensure tables exist
            self._ensure_tables_exist()

            # 3. Validate dates if provided
            if date_from and not metadata.validate_date_format(date_from):
                raise ValueError(
                    f"Invalid date_from format: {date_from}. Use YYYY-MM-DD"
                )
            if date_to and not metadata.validate_date_format(date_to):
                raise ValueError(f"Invalid date_to format: {date_to}. Use YYYY-MM-DD")

            logger.info(f"Ingesting patents for assignee: {assignee_name}")

            # 4. Fetch patents with pagination
            all_patents = []
            after_cursor = None

            async with client:
                while len(all_patents) < max_patents:
                    batch_size = min(1000, max_patents - len(all_patents))

                    response = await client.search_patents_by_assignee(
                        assignee_name=assignee_name,
                        date_from=date_from,
                        date_to=date_to,
                        size=batch_size,
                        after=after_cursor,
                    )

                    patents = response.get("patents", [])
                    if not patents:
                        break

                    all_patents.extend(patents)

                    # Check for more results
                    total_hits = response.get("total_hits", 0)
                    if len(all_patents) >= total_hits:
                        break

                    # Get next cursor (last patent_id)
                    after_cursor = patents[-1].get("patent_id")
                    if not after_cursor:
                        break

            logger.info(f"Fetched {len(all_patents)} patents for {assignee_name}")

            # 5. Transform and insert
            if all_patents:
                rows = [self._transform_patent(p) for p in all_patents]
                result = batch_insert(
                    self.db,
                    "uspto_patents",
                    rows,
                    conflict_columns=["patent_id"],
                    update_on_conflict=True,
                )
                rows_inserted = result.inserted_count
            else:
                rows_inserted = 0

            # 6. Complete job
            self.complete_job(job_id, rows_inserted=rows_inserted)

            return {
                "status": "success",
                "assignee": assignee_name,
                "patents_found": len(all_patents),
                "rows_inserted": rows_inserted,
                "table": "uspto_patents",
            }

        except Exception as e:
            logger.error(f"USPTO ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, str(e))
            raise

        finally:
            await client.close()

    async def ingest_patents_by_cpc(
        self,
        job_id: int,
        cpc_code: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        max_patents: int = 1000,
    ) -> Dict[str, Any]:
        """
        Ingest patents by CPC classification code.

        Args:
            job_id: Ingestion job ID
            cpc_code: CPC code prefix (e.g., "G06N")
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            max_patents: Maximum patents to ingest

        Returns:
            Dictionary with ingestion results
        """
        client = USPTOClient(
            api_key=self.api_key,
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            # 2. Ensure tables exist
            self._ensure_tables_exist()

            logger.info(f"Ingesting patents for CPC code: {cpc_code}")

            # 3. Fetch patents with pagination
            all_patents = []
            after_cursor = None

            async with client:
                while len(all_patents) < max_patents:
                    batch_size = min(1000, max_patents - len(all_patents))

                    response = await client.search_patents_by_cpc(
                        cpc_code=cpc_code,
                        date_from=date_from,
                        date_to=date_to,
                        size=batch_size,
                        after=after_cursor,
                    )

                    patents = response.get("patents", [])
                    if not patents:
                        break

                    all_patents.extend(patents)

                    total_hits = response.get("total_hits", 0)
                    if len(all_patents) >= total_hits:
                        break

                    after_cursor = patents[-1].get("patent_id")
                    if not after_cursor:
                        break

            logger.info(f"Fetched {len(all_patents)} patents for CPC {cpc_code}")

            # 4. Transform and insert
            if all_patents:
                rows = [self._transform_patent(p) for p in all_patents]
                result = batch_insert(
                    self.db,
                    "uspto_patents",
                    rows,
                    conflict_columns=["patent_id"],
                    update_on_conflict=True,
                )
                rows_inserted = result.inserted_count
            else:
                rows_inserted = 0

            # 5. Complete job
            self.complete_job(job_id, rows_inserted=rows_inserted)

            return {
                "status": "success",
                "cpc_code": cpc_code,
                "cpc_description": metadata.get_cpc_class_description(cpc_code),
                "patents_found": len(all_patents),
                "rows_inserted": rows_inserted,
                "table": "uspto_patents",
            }

        except Exception as e:
            logger.error(f"USPTO CPC ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, str(e))
            raise

        finally:
            await client.close()

    async def ingest_patents_by_search(
        self,
        job_id: int,
        search_query: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        max_patents: int = 1000,
    ) -> Dict[str, Any]:
        """
        Ingest patents by text search.

        Args:
            job_id: Ingestion job ID
            search_query: Text to search in title/abstract
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            max_patents: Maximum patents to ingest

        Returns:
            Dictionary with ingestion results
        """
        client = USPTOClient(
            api_key=self.api_key,
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)
            self._ensure_tables_exist()

            logger.info(f"Ingesting patents for search: {search_query}")

            # 2. Build query
            conditions = [
                {
                    "_or": [
                        {"patent_title": {"_text_any": search_query}},
                        {"patent_abstract": {"_text_any": search_query}},
                    ]
                }
            ]

            if date_from:
                conditions.append({"patent_date": {"_gte": date_from}})
            if date_to:
                conditions.append({"patent_date": {"_lte": date_to}})

            query = {"_and": conditions} if len(conditions) > 1 else conditions[0]

            # 3. Fetch patents
            all_patents = []
            after_cursor = None

            async with client:
                while len(all_patents) < max_patents:
                    batch_size = min(1000, max_patents - len(all_patents))

                    response = await client.search_patents(
                        query=query,
                        fields=metadata.DEFAULT_PATENT_FIELDS,
                        size=batch_size,
                        after=after_cursor,
                    )

                    patents = response.get("patents", [])
                    if not patents:
                        break

                    all_patents.extend(patents)

                    total_hits = response.get("total_hits", 0)
                    if len(all_patents) >= total_hits:
                        break

                    after_cursor = patents[-1].get("patent_id")
                    if not after_cursor:
                        break

            logger.info(
                f"Fetched {len(all_patents)} patents for search '{search_query}'"
            )

            # 4. Transform and insert
            if all_patents:
                rows = [self._transform_patent(p) for p in all_patents]
                result = batch_insert(
                    self.db,
                    "uspto_patents",
                    rows,
                    conflict_columns=["patent_id"],
                    update_on_conflict=True,
                )
                rows_inserted = result.inserted_count
            else:
                rows_inserted = 0

            # 5. Complete job
            self.complete_job(job_id, rows_inserted=rows_inserted)

            return {
                "status": "success",
                "search_query": search_query,
                "patents_found": len(all_patents),
                "rows_inserted": rows_inserted,
                "table": "uspto_patents",
            }

        except Exception as e:
            logger.error(f"USPTO search ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, str(e))
            raise

        finally:
            await client.close()


# Convenience functions for use in API endpoints


async def ingest_patents_by_assignee(
    db: Session,
    job_id: int,
    assignee_name: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    max_patents: int = 1000,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Convenience wrapper for ingesting patents by assignee."""
    ingestor = USPTOIngestor(db, api_key=api_key)
    return await ingestor.ingest_patents_by_assignee(
        job_id=job_id,
        assignee_name=assignee_name,
        date_from=date_from,
        date_to=date_to,
        max_patents=max_patents,
    )


async def ingest_patents_by_cpc(
    db: Session,
    job_id: int,
    cpc_code: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    max_patents: int = 1000,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Convenience wrapper for ingesting patents by CPC code."""
    ingestor = USPTOIngestor(db, api_key=api_key)
    return await ingestor.ingest_patents_by_cpc(
        job_id=job_id,
        cpc_code=cpc_code,
        date_from=date_from,
        date_to=date_to,
        max_patents=max_patents,
    )


async def ingest_patents_by_search(
    db: Session,
    job_id: int,
    search_query: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    max_patents: int = 1000,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Convenience wrapper for ingesting patents by search query."""
    ingestor = USPTOIngestor(db, api_key=api_key)
    return await ingestor.ingest_patents_by_search(
        job_id=job_id,
        search_query=search_query,
        date_from=date_from,
        date_to=date_to,
        max_patents=max_patents,
    )
