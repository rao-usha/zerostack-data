"""
AFDC ingestion orchestration.

Fetches EV charging station data from the NREL AFDC API
and persists it to the database following the standard ingestor pattern.
"""

import logging
from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from app.core.ingest_base import BaseSourceIngestor
from app.core.config import get_settings
from app.sources.afdc import client as afdc_client
from app.sources.afdc import metadata

logger = logging.getLogger(__name__)


class AFDCIngestor(BaseSourceIngestor):
    """Ingestor for NREL AFDC EV charging station data."""

    SOURCE_NAME = "afdc"

    def __init__(self, db: Session, api_key: Optional[str] = None):
        super().__init__(db)
        settings = get_settings()
        self.api_key = api_key or settings.data_gov_api


async def ingest_afdc_dataset(
    job_id: int,
    dataset: str,
    db: Session,
    api_key: Optional[str] = None,
) -> None:
    """
    Ingest an AFDC dataset.

    Args:
        job_id: Ingestion job ID for status tracking.
        dataset: Dataset to ingest. Currently supports: ev_stations.
        db: Database session.
        api_key: Optional NREL/data.gov API key (falls back to settings).
    """
    ingestor = AFDCIngestor(db=db, api_key=api_key)
    ingestor.start_job(job_id)

    client = afdc_client.AFDCClient(api_key=ingestor.api_key)
    try:
        dataset_info = metadata.get_dataset_info(dataset)
        table_name = metadata.get_table_name(dataset)
        create_sql = metadata.get_create_table_sql(dataset)

        ingestor.prepare_table(
            dataset_id=f"afdc_{dataset}",
            table_name=table_name,
            create_sql=create_sql,
            display_name=dataset_info["display_name"],
            description=dataset_info["description"],
        )

        if dataset == "ev_stations":
            raw = await client.get_ev_station_counts_by_state()
            rows = metadata.parse_ev_stations_response(raw, as_of=date.today())
        else:
            raise ValueError(f"Unsupported AFDC dataset: {dataset}")

        result = ingestor.insert_rows(
            table_name=table_name,
            rows=rows,
            columns=["state", "total_stations", "ev_level1", "ev_level2", "ev_dc_fast", "as_of_date"],
            conflict_columns=["state", "as_of_date"],
            update_columns=["total_stations", "ev_level1", "ev_level2", "ev_dc_fast"],
        )

        ingestor.complete_job(job_id, rows_inserted=result.rows_inserted, warn_on_empty=True)
        logger.info(f"AFDC {dataset}: inserted {result.rows_inserted} rows")

    except Exception as exc:
        ingestor.fail_job(job_id, exc)
        raise
    finally:
        await client.close()
