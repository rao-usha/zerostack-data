"""
SPEC_075 — one-shot run script for multi-year county-grain CBP.

Default: 2018-2022, NAICS=00 (total). One Census API call per year;
~5 calls total, ~3,000 county rows each = ~15k row UPSERT.

From the api container:

    docker exec nexdata-api-1 sh -c \\
      "cd /app && PYTHONPATH=/app python -m scripts.ingest_cbp_county"
"""
import argparse
import asyncio
import logging

from app.core.database import get_db
from app.sources.census.county_cbp import ingest_county_cbp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main() -> None:
    ap = argparse.ArgumentParser(description="SPEC_075 multi-year county CBP ingest")
    ap.add_argument("--years", type=str, default="2018,2019,2020,2021,2022",
                    help="comma-separated list of years (default 2018-2022)")
    ap.add_argument("--naics", type=str, default="00",
                    help="NAICS code (default 00 = total for all sectors)")
    args = ap.parse_args()
    years = [int(y.strip()) for y in args.years.split(",") if y.strip()]

    db = next(get_db())
    try:
        result = ingest_county_cbp(db, years=years, naics_code=args.naics)
    finally:
        db.close()

    print()
    print("=" * 60)
    print(f"CBP county multi-year ingest — SPEC_075")
    print("=" * 60)
    print(f"  years          : {result['years']}")
    print(f"  naics_code     : {result['naics_code']}")
    print(f"  total_inserted : {result['total_inserted']}")
    print()
    print(f"  Per-year:")
    for s in result['per_year']:
        if 'error' in s:
            print(f"    {s['year']}: ERROR {s['error']}")
        else:
            print(f"    {s['year']}: api={s['api_results']:>5} inserted={s['rows_inserted']:>5} "
                  f"total_in_table_year={s['rows_in_table_year']:>5} "
                  f"({s['duration_seconds']:.1f}s)")
    print()


if __name__ == "__main__":
    main()
