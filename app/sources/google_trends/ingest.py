"""
Google Trends ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor, create_ingestion_job
from app.core.batch_operations import batch_insert
from app.sources.google_trends.client import GoogleTrendsClient
from app.sources.google_trends import metadata

logger = logging.getLogger(__name__)


class GoogleTrendsIngestor(BaseSourceIngestor):
    """
    Ingestor for Google Trends search interest data.

    Handles ingestion of daily trending searches and interest by region.

    WARNING: Google Trends aggressively rate-limits automated access.
    Ingestion may fail with 429 errors. Use conservative concurrency.
    """

    SOURCE_NAME = "google_trends"

    def __init__(self, db: Session):
        """
        Initialize Google Trends ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_daily_trends(
        self,
        job_id: int,
        geo: str = "US",
        date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest daily trending searches from Google Trends.

        Args:
            job_id: Ingestion job ID
            geo: Geographic region code (e.g., "US")
            date: Optional date in YYYYMMDD format

        Returns:
            Dictionary with ingestion results
        """
        client = GoogleTrendsClient(
            max_concurrency=1,
            max_retries=self.settings.max_retries,
            backoff_factor=5.0,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting Google Trends daily trends for geo={geo}, date={date}"
            )

            # 2. Prepare table
            create_sql = metadata.generate_create_table_sql()

            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=create_sql,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "geo": geo,
                    "date": date,
                    "mode": "daily_trends",
                },
            )

            # 3. Fetch daily trends
            raw_response = await client.fetch_daily_trends(
                geo=geo,
                date=date,
            )

            # 4. Parse data
            rows = metadata.parse_daily_trends(raw_response, geo=geo)

            if not rows:
                logger.warning(f"No parseable trends for geo={geo}")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "geo": geo,
                    "rows_fetched": 0,
                    "rows_inserted": 0,
                }

            # 5. Insert data with upsert
            result = batch_insert(
                db=self.db,
                table_name=metadata.TABLE_NAME,
                rows=rows,
                columns=metadata.INSERT_COLUMNS,
                conflict_columns=metadata.CONFLICT_COLUMNS,
                update_columns=metadata.UPDATE_COLUMNS,
                batch_size=500,
            )

            rows_inserted = result.rows_inserted + result.rows_updated

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "geo": geo,
                "date": date,
                "rows_fetched": len(rows),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"Google Trends ingestion failed for geo={geo}: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()

    async def ingest_interest_by_region(
        self,
        job_id: int,
        keyword: str,
        geo: str = "US",
    ) -> Dict[str, Any]:
        """
        Ingest interest by region for a keyword.

        Args:
            job_id: Ingestion job ID
            keyword: Search keyword to analyze
            geo: Geographic region code (e.g., "US")

        Returns:
            Dictionary with ingestion results
        """
        client = GoogleTrendsClient(
            max_concurrency=1,
            max_retries=self.settings.max_retries,
            backoff_factor=5.0,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting Google Trends interest by region for "
                f"keyword={keyword}, geo={geo}"
            )

            # 2. Prepare table
            create_sql = metadata.generate_create_table_sql()

            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=create_sql,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "keyword": keyword,
                    "geo": geo,
                    "mode": "interest_by_region",
                },
            )

            # 3. Fetch interest data
            raw_response = await client.fetch_interest_by_region(
                keyword=keyword,
                geo=geo,
            )

            # 4. Parse data
            from datetime import date as date_module

            today = date_module.today().isoformat()
            rows = metadata.parse_interest_by_region(
                raw_response, keyword=keyword, geo=geo, date=today
            )

            if not rows:
                logger.warning(
                    f"No parseable interest data for keyword={keyword}"
                )
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "keyword": keyword,
                    "geo": geo,
                    "rows_fetched": 0,
                    "rows_inserted": 0,
                }

            # 5. Insert data with upsert
            result = batch_insert(
                db=self.db,
                table_name=metadata.TABLE_NAME,
                rows=rows,
                columns=metadata.INSERT_COLUMNS,
                conflict_columns=metadata.CONFLICT_COLUMNS,
                update_columns=metadata.UPDATE_COLUMNS,
                batch_size=500,
            )

            rows_inserted = result.rows_inserted + result.rows_updated

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "keyword": keyword,
                "geo": geo,
                "rows_fetched": len(rows),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"Google Trends interest ingestion failed for "
                f"keyword={keyword}: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_google_trends_daily(
    db: Session,
    job_id: int,
    geo: str = "US",
    date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Google Trends daily trending searches.

    Convenience wrapper around GoogleTrendsIngestor.ingest_daily_trends().
    """
    ingestor = GoogleTrendsIngestor(db)
    return await ingestor.ingest_daily_trends(
        job_id=job_id,
        geo=geo,
        date=date,
    )


async def ingest_google_trends_region(
    db: Session,
    job_id: int,
    keyword: str,
    geo: str = "US",
) -> Dict[str, Any]:
    """
    Ingest Google Trends interest by region.

    Convenience wrapper around GoogleTrendsIngestor.ingest_interest_by_region().
    """
    ingestor = GoogleTrendsIngestor(db)
    return await ingestor.ingest_interest_by_region(
        job_id=job_id,
        keyword=keyword,
        geo=geo,
    )
