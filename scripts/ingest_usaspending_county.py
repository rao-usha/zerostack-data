"""
SPEC_071 — one-shot run script for USAspending county-aggregate ingest.

Default: FY2024 prime contracts. Re-runnable; UPSERTs on
(geo_id, fiscal_year, award_type_group). Add more years/groups by
passing --fy and --group.

From the api container:

    docker exec nexdata-api-1 sh -c \\
      "cd /app && python scripts/ingest_usaspending_county.py"

With args:

    python scripts/ingest_usaspending_county.py --fy 2024 --group contracts
    python scripts/ingest_usaspending_county.py --fy 2023 --group contracts
    python scripts/ingest_usaspending_county.py --fy 2024 --group grants
"""
import argparse
import logging

from app.core.database import get_db
from app.sources.usaspending.county_aggregate import (
    AWARD_TYPE_GROUPS,
    ingest_county_fy_totals,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main() -> None:
    ap = argparse.ArgumentParser(description="SPEC_071 USAspending county ingest")
    ap.add_argument("--fy", type=int, default=2024, help="fiscal year (default 2024)")
    ap.add_argument("--group", type=str, default="contracts",
                    choices=sorted(AWARD_TYPE_GROUPS),
                    help="award type group (default contracts)")
    args = ap.parse_args()

    db = next(get_db())
    try:
        result = ingest_county_fy_totals(
            db, fiscal_year=args.fy, award_type_group=args.group,
        )
    finally:
        db.close()

    print()
    print("=" * 60)
    print(f"USAspending county-aggregate ingest — SPEC_071")
    print("=" * 60)
    for k, v in result.items():
        if k == "total_obligation_usd":
            print(f"  {k:22s}: ${v/1e9:.2f}B")
        else:
            print(f"  {k:22s}: {v}")
    print()


if __name__ == "__main__":
    main()
