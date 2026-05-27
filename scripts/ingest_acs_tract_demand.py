"""
SPEC_077 A.3 — one-shot runner for tract-grain ACS demand variables.

Wall-clock estimate: ~15-20 min (52 states × ~15s each).

Run from the api container:

    docker exec nexdata-api-1 sh -c \\
      "cd /app && PYTHONPATH=/app python -m scripts.ingest_acs_tract_demand"
"""
import asyncio
import logging

from app.core.database import get_db
from app.sources.census.tract_acs import ingest_tract_demand_all


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main():
    db = next(get_db())
    try:
        result = await ingest_tract_demand_all(db, year=2023)
    finally:
        db.close()
    print(); print("=" * 60)
    print("Tract ACS demand ingest — SPEC_077 A.3")
    print("=" * 60)
    print(f"  table_name        : {result['table_name']}")
    print(f"  rows_in_table     : {result['rows_in_table']}")
    print(f"  duration_seconds  : {result['duration_seconds']:.1f}")
    ok = sum(1 for s in result['per_state'] if 'error' not in s)
    fails = [s for s in result['per_state'] if 'error' in s]
    print(f"  states ok         : {ok} / {len(result['per_state'])}")
    if fails:
        print(f"  states failed     : {[s['state'] for s in fails]}")


if __name__ == "__main__":
    asyncio.run(main())
