"""
SPEC_070 — one-shot run script for county-grain ACS B19013 ingest.

Run from the api container:

    docker exec nexdata-api-1 sh -c "cd /app && python scripts/ingest_acs_county_b19013.py"

Or locally (requires CENSUS_API_KEY env + DATABASE_URL pointing at target):

    python scripts/ingest_acs_county_b19013.py

Idempotent — safe to re-run; UPSERTs on `geo_id`.
"""
import asyncio
import logging

from app.core.database import get_db
from app.sources.census.county_acs import ingest_county_acs_variable


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_acs_county_b19013")


async def main() -> None:
    db = next(get_db())
    try:
        result = await ingest_county_acs_variable(
            db, year=2023, table_id="B19013", variable="B19013_001E",
        )
    finally:
        db.close()

    print()
    print("=" * 60)
    print(f"ACS county-grain B19013 ingest — SPEC_070")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k:18s}: {v}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
