"""
EPA ECHO ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor, create_ingestion_job
from app.core.batch_operations import batch_insert
from app.sources.epa_echo.client import EPAECHOClient, US_STATES
from app.sources.epa_echo import metadata

logger = logging.getLogger(__name__)


class EPAECHOIngestor(BaseSourceIngestor):
    """
    Ingestor for EPA ECHO (Enforcement and Compliance History Online).

    Handles ingestion of facility compliance and enforcement data.
    """

    SOURCE_NAME = "epa_echo"

    def __init__(self, db: Session):
        """
        Initialize EPA ECHO ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_by_state(
        self,
        job_id: int,
        state: str,
        naics: Optional[str] = None,
        sic: Optional[str] = None,
        zip_code: Optional[str] = None,
        media: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest EPA ECHO facility data for a single state.

        Args:
            job_id: Ingestion job ID
            state: Two-letter state code (e.g., "TX")
            naics: Optional NAICS code filter
            sic: Optional SIC code filter
            zip_code: Optional ZIP code filter
            media: Optional media program filter (AIR, WATER, RCRA, ALL)

        Returns:
            Dictionary with ingestion results
        """
        client = EPAECHOClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            # 2. Validate inputs
            state = state.upper()
            if media:
                media = media.upper()
                if media not in metadata.VALID_MEDIA_PROGRAMS:
                    raise ValueError(
                        f"Invalid media program: {media}. "
                        f"Must be one of: {', '.join(metadata.VALID_MEDIA_PROGRAMS)}"
                    )

            logger.info(
                f"Ingesting EPA ECHO facilities for state={state}, "
                f"naics={naics}, media={media}"
            )

            # 3. Prepare table
            create_sql = metadata.generate_create_table_sql()

            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=create_sql,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "state": state,
                    "naics": naics,
                    "media": media,
                },
            )

            # 4. Fetch data from ECHO API (all pages)
            raw_facilities = await client.search_facilities_all_pages(
                state=state,
                naics=naics,
                sic=sic,
                zip_code=zip_code,
                media=media,
            )

            logger.info(
                f"Fetched {len(raw_facilities)} raw facilities for state={state}"
            )

            # 5. Parse data
            rows = metadata.parse_facilities(raw_facilities)

            if not rows:
                logger.warning(f"No parseable facilities for state={state}")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "state": state,
                    "rows_fetched": len(raw_facilities),
                    "rows_inserted": 0,
                }

            # 6. Insert data with upsert
            result = batch_insert(
                db=self.db,
                table_name=metadata.TABLE_NAME,
                rows=rows,
                columns=metadata.INSERT_COLUMNS,
                conflict_columns=metadata.CONFLICT_COLUMNS,
                update_columns=metadata.UPDATE_COLUMNS,
                batch_size=1000,
            )

            rows_inserted = result.rows_inserted + result.rows_updated

            # 7. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "state": state,
                "naics": naics,
                "media": media,
                "rows_fetched": len(raw_facilities),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"EPA ECHO ingestion failed for state={state}: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()

    async def ingest_all_states(
        self,
        job_id: int,
        states: Optional[List[str]] = None,
        naics: Optional[str] = None,
        media: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest EPA ECHO facility data for multiple states.

        Iterates through each state, fetches all facilities, and inserts
        into the database. Progress is tracked per-state.

        Args:
            job_id: Ingestion job ID
            states: List of state codes (defaults to all 50 states + DC)
            naics: Optional NAICS code filter
            media: Optional media program filter

        Returns:
            Dictionary with aggregated ingestion results
        """
        client = EPAECHOClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        target_states = states or US_STATES[:51]  # 50 states + DC

        try:
            # 1. Start job
            self.start_job(job_id)

            # 2. Prepare table
            create_sql = metadata.generate_create_table_sql()

            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=create_sql,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "states": target_states,
                    "naics": naics,
                    "media": media,
                    "mode": "all_states",
                },
            )

            # 3. Iterate through states
            total_fetched = 0
            total_inserted = 0
            state_results = {}
            errors = []

            for i, state in enumerate(target_states, 1):
                logger.info(
                    f"Processing state {i}/{len(target_states)}: {state}"
                )

                try:
                    raw_facilities = await client.search_facilities_all_pages(
                        state=state,
                        naics=naics,
                        media=media,
                    )

                    rows = metadata.parse_facilities(raw_facilities)

                    if rows:
                        result = batch_insert(
                            db=self.db,
                            table_name=metadata.TABLE_NAME,
                            rows=rows,
                            columns=metadata.INSERT_COLUMNS,
                            conflict_columns=metadata.CONFLICT_COLUMNS,
                            update_columns=metadata.UPDATE_COLUMNS,
                            batch_size=1000,
                        )
                        inserted = result.rows_inserted + result.rows_updated
                    else:
                        inserted = 0

                    total_fetched += len(raw_facilities)
                    total_inserted += inserted

                    state_results[state] = {
                        "fetched": len(raw_facilities),
                        "inserted": inserted,
                        "status": "success",
                    }

                    logger.info(
                        f"State {state}: {len(raw_facilities)} fetched, "
                        f"{inserted} inserted"
                    )

                except Exception as e:
                    logger.error(f"Failed to process state {state}: {e}")
                    errors.append({"state": state, "error": str(e)})
                    state_results[state] = {
                        "fetched": 0,
                        "inserted": 0,
                        "status": "failed",
                        "error": str(e),
                    }

            # 4. Complete job
            self.complete_job(job_id, total_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "states_processed": len(target_states),
                "total_fetched": total_fetched,
                "total_inserted": total_inserted,
                "errors": errors,
                "state_results": state_results,
            }

        except Exception as e:
            logger.error(
                f"EPA ECHO all-states ingestion failed: {e}", exc_info=True
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_epa_echo_state(
    db: Session,
    job_id: int,
    state: str,
    naics: Optional[str] = None,
    sic: Optional[str] = None,
    zip_code: Optional[str] = None,
    media: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest EPA ECHO facility data for a single state.

    Convenience wrapper around EPAECHOIngestor.ingest_by_state().
    """
    ingestor = EPAECHOIngestor(db)
    return await ingestor.ingest_by_state(
        job_id=job_id,
        state=state,
        naics=naics,
        sic=sic,
        zip_code=zip_code,
        media=media,
    )


async def ingest_epa_echo_all_states(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    naics: Optional[str] = None,
    media: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest EPA ECHO facility data for all states.

    Convenience wrapper around EPAECHOIngestor.ingest_all_states().
    """
    ingestor = EPAECHOIngestor(db)
    return await ingestor.ingest_all_states(
        job_id=job_id,
        states=states,
        naics=naics,
        media=media,
    )
