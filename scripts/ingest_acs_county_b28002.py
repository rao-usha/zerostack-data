"""
SPEC_072 — one-shot run script for county-grain ACS B28002 ingest
(broadband subscription).

Pulls two variables in one Census API call:
  B28002_001E  — total households
  B28002_004E  — households with a broadband internet subscription

The percentage is derived in the layer builder (raw counts preserved).

Run from the api container:

    docker exec nexdata-api-1 sh -c \\
      "cd /app && PYTHONPATH=/app python -m scripts.ingest_acs_county_b28002"

Idempotent — safe to re-run; UPSERTs on geo_id.
"""
import asyncio
import logging

from app.core.database import get_db
from app.sources.census.county_acs import ingest_county_acs_multi


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main() -> None:
    db = next(get_db())
    try:
        result = await ingest_county_acs_multi(
            db, year=2023, table_id="B28002",
            variables=["B28002_001E", "B28002_004E"],
        )
    finally:
        db.close()

    print()
    print("=" * 60)
    print(f"ACS county-grain B28002 (broadband subscription) ingest — SPEC_072")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k:20s}: {v}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
