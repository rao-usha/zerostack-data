"""
CourtListener ingestion orchestration.

Coordinates searching the CourtListener API for bankruptcy dockets,
parsing results, and loading into PostgreSQL.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.sources.courtlistener.client import CourtListenerClient
from app.sources.courtlistener import metadata

logger = logging.getLogger(__name__)


# =========================================================================
# Standalone dispatch function (called by SOURCE_DISPATCH in jobs.py)
# =========================================================================


async def ingest_courtlistener_dockets(
    db: Session,
    job_id: int,
    query: Optional[str] = None,
    court: Optional[str] = None,
    filed_after: Optional[str] = None,
    filed_before: Optional[str] = None,
    max_pages: int = 20,
) -> Dict[str, Any]:
    """
    Dispatch-compatible wrapper for CourtListener bankruptcy docket ingestion.

    Called by run_ingestion_job via SOURCE_DISPATCH registry.
    """
    ingestor = CourtListenerIngestor(db)
    return await ingestor.ingest_bankruptcy_dockets(
        job_id=job_id,
        query=query,
        court=court,
        filed_after=filed_after,
        filed_before=filed_before,
        max_pages=max_pages,
    )


class CourtListenerIngestor(BaseSourceIngestor):
    """
    Ingestor for CourtListener bankruptcy docket data.

    Searches the CourtListener REST API for bankruptcy filings
    and loads docket records into PostgreSQL.
    """

    SOURCE_NAME = "courtlistener"

    def __init__(self, db: Session, api_token: Optional[str] = None):
        """
        Initialize CourtListener ingestor.

        Args:
            db: SQLAlchemy database session
            api_token: Optional CourtListener auth token for higher rate limits
        """
        super().__init__(db)
        self.settings = get_settings()
        self.api_token = api_token

    async def ingest_bankruptcy_dockets(
        self,
        job_id: int,
        query: Optional[str] = None,
        court: Optional[str] = None,
        filed_after: Optional[str] = None,
        filed_before: Optional[str] = None,
        max_pages: int = 20,
    ) -> Dict[str, Any]:
        """
        Search and ingest bankruptcy dockets from CourtListener.

        Args:
            job_id: Ingestion job ID
            query: Search query (company name, party, etc.)
            court: Specific bankruptcy court ID (e.g., "nysb")
            filed_after: Filter cases filed after this date (YYYY-MM-DD)
            filed_before: Filter cases filed before this date (YYYY-MM-DD)
            max_pages: Maximum pages to fetch (safety limit)

        Returns:
            Dictionary with ingestion results
        """
        client = CourtListenerClient(
            api_token=self.api_token,
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            filter_desc = []
            if query:
                filter_desc.append(f"q={query}")
            if court:
                filter_desc.append(f"court={court}")
            if filed_after:
                filter_desc.append(f"after={filed_after}")
            if filed_before:
                filter_desc.append(f"before={filed_before}")
            filter_str = ", ".join(filter_desc) or "all bankruptcy courts"

            logger.info(f"Ingesting CourtListener bankruptcy dockets: {filter_str}")

            # 2. Prepare table
            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=metadata.CREATE_TABLE_SQL,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "query": query,
                    "court": court,
                    "filed_after": filed_after,
                    "filed_before": filed_before,
                },
            )

            # 3. Fetch dockets from CourtListener
            raw_results = await client.search_bankruptcy_dockets(
                query=query,
                court=court,
                filed_after=filed_after,
                filed_before=filed_before,
                max_pages=max_pages,
            )

            # 4. Parse results
            rows = metadata.parse_dockets(raw_results)

            # 5. Insert data
            if not rows:
                logger.warning("No CourtListener docket records to insert")
                rows_inserted = 0
            else:
                result = self.insert_rows(
                    table_name=metadata.TABLE_NAME,
                    rows=rows,
                    columns=metadata.COLUMNS,
                    conflict_columns=metadata.CONFLICT_COLUMNS,
                    update_columns=metadata.UPDATE_COLUMNS,
                    batch_size=500,
                )
                rows_inserted = result.rows_inserted

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "filter": filter_str,
                "results_fetched": len(raw_results),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"CourtListener ingestion failed: {e}", exc_info=True
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()
