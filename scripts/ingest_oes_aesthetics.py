"""
Ingest BLS OES (Occupational Employment Statistics) wage data for
aesthetics-adjacent occupations into the bls_oes table.

Uses the existing BLS client and ingestion pipeline. The OES survey is annual
(published each May for the prior May reference period), so each series
typically returns one data point per year.

Series ID format: OEUN + area(7) + industry(6) + occupation(6) + datatype(2)
National, all-industries: area=0000000, industry=000000

Target occupations (SOC codes):
  29-1229  Physicians, All Other (incl. Dermatologists)
  29-1141  Registered Nurses
  29-1171  Nurse Practitioners
  31-9011  Massage Therapists
  39-5012  Hairdressers, Hairstylists, and Cosmetologists
  31-9099  Healthcare Support Workers, All Other
  29-1071  Physician Assistants
  29-2099  Health Technologists and Technicians, All Other

Data types ingested:
  01 = Employment count
  03 = Hourly mean wage
  04 = Annual mean wage
  08 = Hourly median wage
  13 = Annual median wage

Usage:
    docker exec nexdata-api-1 python scripts/ingest_oes_aesthetics.py
"""

import asyncio
import logging
import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import get_settings
from app.core.database import get_session_factory
from app.core.models import IngestionJob, JobStatus
from app.sources.bls.ingest import ingest_bls_series

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
logger = logging.getLogger("oes_aesthetics")

# ── Target occupations ────────────────────────────────────────────────────────
OCCUPATIONS = {
    "291229": "Physicians, All Other (incl. Dermatologists)",
    "291141": "Registered Nurses",
    "291171": "Nurse Practitioners",
    "319011": "Massage Therapists",
    "395012": "Hairdressers, Hairstylists, and Cosmetologists",
    "319099": "Healthcare Support Workers, All Other",
    "291071": "Physician Assistants",
    "292099": "Health Technologists and Technicians, All Other",
}

# Key data types for wage analysis
DATA_TYPES = {
    "01": "employment",
    "03": "hourly_mean_wage",
    "04": "annual_mean_wage",
    "08": "hourly_median_wage",
    "13": "annual_median_wage",
}


def build_series_ids() -> list[str]:
    """Build national-level OES series IDs for all occupation x data-type combos."""
    series_ids = []
    for occ_code in OCCUPATIONS:
        for dt_code in DATA_TYPES:
            # OEUN + area(7 zeros) + industry(6 zeros) + occupation(6) + datatype(2)
            sid = f"OEUN{'0' * 7}{'0' * 6}{occ_code}{dt_code}"
            series_ids.append(sid)
    return series_ids


def describe_series(series_id: str) -> str:
    """Return a human-readable label for a series ID."""
    occ_code = series_id[17:23]
    dt_code = series_id[23:25]
    occ_name = OCCUPATIONS.get(occ_code, occ_code)
    dt_name = DATA_TYPES.get(dt_code, dt_code)
    return f"{occ_name} [{dt_name}]"


async def main():
    settings = get_settings()
    api_key = settings.get_bls_api_key()
    if not api_key:
        logger.warning(
            "BLS_API_KEY not set. Will use unauthenticated access (25 queries/day limit)."
        )

    series_ids = build_series_ids()
    logger.info(
        f"Built {len(series_ids)} OES series IDs across "
        f"{len(OCCUPATIONS)} occupations x {len(DATA_TYPES)} data types"
    )
    for sid in series_ids:
        logger.debug(f"  {sid}  →  {describe_series(sid)}")

    # OES data is annual; recent surveys cover 2019-2024
    start_year = 2019
    end_year = 2024

    # Create a DB session and ingestion job
    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        job = IngestionJob(
            source="bls",
            status=JobStatus.PENDING,
            config={
                "dataset": "oes",
                "sub_dataset": "aesthetics_occupations",
                "start_year": start_year,
                "end_year": end_year,
                "series_count": len(series_ids),
                "occupations": list(OCCUPATIONS.keys()),
                "data_types": list(DATA_TYPES.keys()),
            },
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        logger.info(f"Created ingestion job {job.id}")

        # Run the existing BLS ingest pipeline
        result = await ingest_bls_series(
            db=db,
            job_id=job.id,
            series_ids=series_ids,
            start_year=start_year,
            end_year=end_year,
            dataset="oes",
        )

        logger.info("=" * 60)
        logger.info("INGESTION COMPLETE")
        logger.info(f"  Table:         {result['table_name']}")
        logger.info(f"  Dataset:       {result['dataset']}")
        logger.info(f"  Series count:  {result['series_count']}")
        logger.info(f"  Rows inserted: {result['rows_inserted']}")
        logger.info(f"  Year range:    {result['year_range']}")
        logger.info("")

        # Print summary by occupation
        logger.info("Per-series breakdown:")
        for sid, count in sorted(result.get("series_summary", {}).items()):
            label = describe_series(sid)
            logger.info(f"  {label}: {count} data point(s)")

        # Backfill series_title (BLS API doesn't return titles in the response)
        from sqlalchemy import text

        logger.info("")
        logger.info("Backfilling series_title column...")
        titles_updated = 0
        for occ_code, occ_name in OCCUPATIONS.items():
            for dt_code, dt_name in DATA_TYPES.items():
                sid = f"OEUN{'0' * 7}{'0' * 6}{occ_code}{dt_code}"
                title = f"{dt_name.replace('_', ' ').title()} for {occ_name} (National, All Industries)"
                r = db.execute(
                    text(
                        "UPDATE bls_oes SET series_title = :title "
                        "WHERE series_id = :sid AND series_title IS NULL"
                    ),
                    {"title": title, "sid": sid},
                )
                titles_updated += r.rowcount
        db.commit()
        logger.info(f"Updated {titles_updated} series titles")

        # Query and display the ingested data

        rows = db.execute(
            text(
                "SELECT series_id, year, period, value "
                "FROM bls_oes ORDER BY series_id, year"
            )
        ).fetchall()

        logger.info("")
        logger.info(f"Total rows in bls_oes: {len(rows)}")
        logger.info("")
        logger.info("Sample data:")
        for row in rows[:10]:
            sid, year, period, value = row
            label = describe_series(sid)
            logger.info(f"  {label}  {year} {period}: {value}")

    except Exception:
        logger.exception("OES ingestion failed")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
