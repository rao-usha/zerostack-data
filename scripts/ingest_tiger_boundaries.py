"""
SPEC_077 A.2 — one-shot runner for tract + ZCTA TIGER ingest.

Wall-clock estimate: ~10-20 min total
  - Tracts: ~55 state-scoped calls × 3-8s each = 5-10 min
  - ZCTAs: ~33 paginated calls = 2-5 min

Run from the api container:

    docker exec nexdata-api-1 sh -c \\
      "cd /app && PYTHONPATH=/app python -m scripts.ingest_tiger_boundaries"

Or with options:

    python -m scripts.ingest_tiger_boundaries --tracts-only
    python -m scripts.ingest_tiger_boundaries --zctas-only
    python -m scripts.ingest_tiger_boundaries --states 06,48     # CA + TX only
"""
import argparse
import asyncio
import logging

from app.core.database import get_db
from app.sources.census.tiger_bulk_ingest import (
    ingest_tracts, ingest_zctas, US_STATE_FIPS,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main_async(args):
    db = next(get_db())
    try:
        if not args.zctas_only:
            states = (args.states.split(",") if args.states else US_STATE_FIPS)
            r = await ingest_tracts(db, state_fips_codes=states)
            print(); print("=" * 60)
            print("Tract ingest")
            print("=" * 60)
            for k, v in r.items():
                if k == "per_state":
                    print(f"  per_state         : {{")
                    for st, n in v.items():
                        flag = "OK" if n >= 0 else "FAIL"
                        print(f"      {st}: {n:>6}  [{flag}]")
                    print("  }")
                else:
                    print(f"  {k:18s}: {v}")
            print()

        if not args.tracts_only:
            r = await ingest_zctas(db)
            print(); print("=" * 60)
            print("ZCTA ingest")
            print("=" * 60)
            for k, v in r.items():
                print(f"  {k:18s}: {v}")
            print()
    finally:
        db.close()


def main():
    ap = argparse.ArgumentParser(description="SPEC_077 A.2 tract + ZCTA ingest")
    ap.add_argument("--tracts-only", action="store_true")
    ap.add_argument("--zctas-only", action="store_true")
    ap.add_argument("--states", type=str, default=None,
                    help="comma-separated state FIPS list (default = all US states)")
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    main()
