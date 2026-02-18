"""
FRED ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.models import IngestionJob, JobStatus
from app.core.ingest_base import BaseSourceIngestor, create_ingestion_job
from app.core.batch_operations import batch_insert
from app.sources.fred.client import FREDClient
from app.sources.fred import metadata

logger = logging.getLogger(__name__)


class FREDIngestor(BaseSourceIngestor):
    """
    Ingestor for FRED (Federal Reserve Economic Data).

    Handles ingestion of economic time series data.
    """

    SOURCE_NAME = "fred"

    def __init__(self, db: Session, api_key: Optional[str] = None):
        """
        Initialize FRED ingestor.

        Args:
            db: SQLAlchemy database session
            api_key: Optional FRED API key
        """
        super().__init__(db)
        settings = get_settings()
        self.api_key = api_key or settings.get_api_key("fred")
        self.settings = settings

    async def ingest_category(
        self,
        job_id: int,
        category: str,
        series_ids: Optional[List[str]] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest FRED category data into Postgres.

        Args:
            job_id: Ingestion job ID
            category: Category name (e.g., "interest_rates", "monetary_aggregates")
            series_ids: Optional list of specific series IDs
            observation_start: Start date in YYYY-MM-DD format
            observation_end: End date in YYYY-MM-DD format

        Returns:
            Dictionary with ingestion results
        """
        client = FREDClient(
            api_key=self.api_key,
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            # 2. Validate and set defaults
            if not series_ids:
                series_ids = metadata.get_series_for_category(category)
                logger.info(f"Using default series for {category}: {series_ids}")

            if not observation_end or not observation_start:
                default_start, default_end = metadata.get_default_date_range()
                observation_start = observation_start or default_start
                observation_end = observation_end or default_end

            # Validate date formats
            if not metadata.validate_date_format(observation_start):
                raise ValueError(
                    f"Invalid start date format: {observation_start}. Use YYYY-MM-DD"
                )
            if not metadata.validate_date_format(observation_end):
                raise ValueError(
                    f"Invalid end date format: {observation_end}. Use YYYY-MM-DD"
                )

            logger.info(
                f"Ingesting FRED {category}: "
                f"{len(series_ids)} series, {observation_start} to {observation_end}"
            )

            # 3. Prepare table
            table_name = metadata.generate_table_name(category)
            create_sql = metadata.generate_create_table_sql(table_name, series_ids)

            table_info = self.prepare_table(
                dataset_id=f"fred_{category.lower()}",
                table_name=table_name,
                create_sql=create_sql,
                display_name=metadata.get_category_display_name(category),
                description=metadata.get_category_description(category),
                source_metadata={
                    "category": category,
                    "series_ids": series_ids,
                    "series_count": len(series_ids),
                },
            )

            # 4. Fetch data from FRED API
            logger.info(f"Fetching {len(series_ids)} series from FRED API")
            all_parsed_data = {}

            for i, series_id in enumerate(series_ids, 1):
                logger.info(f"Fetching series {i}/{len(series_ids)}: {series_id}")

                try:
                    api_response = await client.get_series_observations(
                        series_id=series_id,
                        observation_start=observation_start,
                        observation_end=observation_end,
                    )
                    parsed = metadata.parse_observations(api_response, series_id)
                    all_parsed_data[series_id] = parsed
                    logger.info(f"Parsed {len(parsed)} observations for {series_id}")

                except Exception as e:
                    logger.error(f"Failed to fetch series {series_id}: {e}")
                    all_parsed_data[series_id] = []

            # 5. Insert data
            rows = metadata.build_insert_values(all_parsed_data)

            if not rows:
                logger.warning("No data to insert")
                rows_inserted = 0
            else:
                logger.info(f"Inserting {len(rows)} rows into {table_name}")

                result = batch_insert(
                    db=self.db,
                    table_name=table_name,
                    rows=rows,
                    columns=[
                        "series_id",
                        "date",
                        "value",
                        "realtime_start",
                        "realtime_end",
                    ],
                    conflict_columns=["series_id", "date"],
                    update_columns=["value", "realtime_start", "realtime_end"],
                    batch_size=1000,
                )
                rows_inserted = result.rows_inserted

            # 6. Complete job (fail if no rows inserted)
            self.complete_job(job_id, rows_inserted, require_rows=True)

            return {
                "table_name": table_name,
                "category": category,
                "series_count": len(series_ids),
                "rows_inserted": rows_inserted,
                "date_range": f"{observation_start} to {observation_end}",
            }

        except Exception as e:
            logger.error(f"FRED ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS (backward compatibility)
# =============================================================================


async def prepare_table_for_fred_category(
    db: Session, category: str, series_ids: List[str]
) -> Dict[str, Any]:
    """
    Prepare database table for FRED data ingestion.

    This is a backward-compatible wrapper around FREDIngestor.prepare_table().
    """
    ingestor = FREDIngestor(db)
    table_name = metadata.generate_table_name(category)
    create_sql = metadata.generate_create_table_sql(table_name, series_ids)

    return ingestor.prepare_table(
        dataset_id=f"fred_{category.lower()}",
        table_name=table_name,
        create_sql=create_sql,
        display_name=metadata.get_category_display_name(category),
        description=metadata.get_category_description(category),
        source_metadata={
            "category": category,
            "series_ids": series_ids,
            "series_count": len(series_ids),
        },
    )


async def ingest_fred_category(
    db: Session,
    job_id: int,
    category: str,
    series_ids: Optional[List[str]] = None,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest FRED category data into Postgres.

    This is a backward-compatible wrapper around FREDIngestor.ingest_category().
    """
    ingestor = FREDIngestor(db, api_key=api_key)
    return await ingestor.ingest_category(
        job_id=job_id,
        category=category,
        series_ids=series_ids,
        observation_start=observation_start,
        observation_end=observation_end,
    )


async def ingest_all_fred_categories(
    db: Session,
    categories: List[str],
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest multiple FRED categories.

    This is a convenience function for ingesting multiple FRED categories
    at once (interest_rates, monetary_aggregates, industrial_production, etc.).
    """
    ingestor = FREDIngestor(db, api_key=api_key)
    results = {}

    for category in categories:
        logger.info(f"Starting ingestion for FRED category: {category}")

        # Create job
        job = create_ingestion_job(
            db=db,
            source="fred",
            config={
                "category": category,
                "observation_start": observation_start,
                "observation_end": observation_end,
            },
        )

        try:
            result = await ingestor.ingest_category(
                job_id=job.id,
                category=category,
                observation_start=observation_start,
                observation_end=observation_end,
            )

            results[category] = {"status": "success", "job_id": job.id, **result}

        except Exception as e:
            logger.error(f"Failed to ingest {category}: {e}")
            results[category] = {"status": "failed", "job_id": job.id, "error": str(e)}

    return results
