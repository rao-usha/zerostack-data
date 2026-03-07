"""
SAM.gov ingestion orchestration.

Coordinates data fetching from the SAM.gov Entity Information API,
table creation, and data loading into PostgreSQL.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.sources.sam_gov.client import SAMGovClient
from app.sources.sam_gov import metadata

logger = logging.getLogger(__name__)


# =========================================================================
# Standalone dispatch function (called by SOURCE_DISPATCH in jobs.py)
# =========================================================================


async def ingest_sam_gov_entities(
    db: Session,
    job_id: int,
    state: Optional[str] = None,
    naics_code: Optional[str] = None,
    legal_business_name: Optional[str] = None,
    max_pages: int = 50,
) -> Dict[str, Any]:
    """
    Dispatch-compatible wrapper for SAM.gov entity ingestion.

    Called by run_ingestion_job via SOURCE_DISPATCH registry.
    """
    ingestor = SAMGovIngestor(db)
    return await ingestor.ingest_entities(
        job_id=job_id,
        state=state,
        naics_code=naics_code,
        legal_business_name=legal_business_name,
        max_pages=max_pages,
    )


class SAMGovIngestor(BaseSourceIngestor):
    """
    Ingestor for SAM.gov entity registration data.

    Handles ingestion of federal contractor registrations
    filtered by state, NAICS code, or business name.
    """

    SOURCE_NAME = "sam_gov"

    def __init__(self, db: Session, api_key: Optional[str] = None):
        """
        Initialize SAM.gov ingestor.

        Args:
            db: SQLAlchemy database session
            api_key: Optional SAM.gov API key
        """
        super().__init__(db)
        settings = get_settings()
        self.api_key = api_key or settings.get_api_key("sam_gov")
        self.settings = settings

    async def ingest_entities(
        self,
        job_id: int,
        state: Optional[str] = None,
        naics_code: Optional[str] = None,
        legal_business_name: Optional[str] = None,
        max_pages: int = 50,
    ) -> Dict[str, Any]:
        """
        Ingest SAM.gov entity registrations into PostgreSQL.

        Args:
            job_id: Ingestion job ID
            state: Two-letter state code filter (e.g., "TX")
            naics_code: NAICS code filter (e.g., "541512")
            legal_business_name: Business name search string
            max_pages: Maximum pages to fetch (safety limit)

        Returns:
            Dictionary with ingestion results
        """
        if not self.api_key:
            raise ValueError(
                "SAM_GOV_API_KEY is required for SAM.gov ingestion. "
                "Get a free key at: https://sam.gov/content/entity-information"
            )

        client = SAMGovClient(
            api_key=self.api_key,
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            filter_desc = []
            if state:
                filter_desc.append(f"state={state}")
            if naics_code:
                filter_desc.append(f"naics={naics_code}")
            if legal_business_name:
                filter_desc.append(f"name={legal_business_name}")
            filter_str = ", ".join(filter_desc) or "all"

            logger.info(f"Ingesting SAM.gov entities: {filter_str}")

            # 2. Prepare table
            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=metadata.CREATE_TABLE_SQL,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "state": state,
                    "naics_code": naics_code,
                    "legal_business_name": legal_business_name,
                },
            )

            # 3. Fetch data from SAM.gov API
            raw_entities = await client.search_all_pages(
                state=state,
                naics_code=naics_code,
                legal_business_name=legal_business_name,
                max_pages=max_pages,
            )

            # 4. Parse data
            rows = metadata.parse_entities(raw_entities)

            # 5. Insert data
            if not rows:
                logger.warning("No SAM.gov entities to insert")
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
                "entities_fetched": len(raw_entities),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(f"SAM.gov ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()
