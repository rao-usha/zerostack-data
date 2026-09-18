"""
Dry run of the SPEC_105 quarantine: executes every move inside a transaction,
prints what would move, then ROLLS BACK. Nothing is changed.

    python scripts/quarantine_dry_run.py            # uses DATABASE_URL
    python scripts/quarantine_dry_run.py --revert   # also exercises revert() before rollback
"""

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text  # noqa: E402

from app.core.quarantine import QuarantineGuardError, apply, revert  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--revert", action="store_true", help="also test revert() before rollback")
    parser.add_argument("--url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args()
    if not args.url:
        print("DATABASE_URL not set")
        return 2

    engine = create_engine(args.url)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            result = apply(conn)
        except QuarantineGuardError as e:
            trans.rollback()
            print(f"GUARD TRIPPED: {e}")
            return 1

        print(f"{'rule':40} {'table':28} {'expected':>8} {'live':>8}")
        for r in result["roots"]:
            flag = "" if r["live"] == r["expected"] else "  <-- differs"
            print(f"{r['rule']:40} {r['table']:28} {r['expected']:>8} {r['live']:>8}{flag}")

        print("\nMoves (children first; depth>0 = moved via foreign key):")
        per_table = defaultdict(int)
        for m in result["moves"]:
            per_table[(m["schema"], m["table"])] += m["rows"]
            if m["rows"]:
                print(f"  {m['rule']:40} depth={m['depth']} {m['schema']}.{m['table']}: {m['rows']}")
        print("\nTotals by destination table:")
        for (schema, table), n in sorted(per_table.items()):
            print(f"  {schema}.{table}: {n}")
        print(f"\nTOTAL rows moved: {result['total_rows']}")

        backups = conn.execute(
            text("SELECT table_name, column_name, COUNT(*) FROM quarantine.set_null_backup GROUP BY 1, 2")
        ).fetchall()
        for t, c, n in backups:
            print(f"SET NULL backups: {t}.{c}: {n}")

        zombies = conn.execute(
            text(
                "SELECT status, COUNT(*) FROM ingestion_jobs "
                "WHERE status IN ('pending','blocked','running') AND created_at < '2026-09-16' GROUP BY 1"
            )
        ).fetchall()
        print(f"Zombie ingestion_jobs to fail: {dict(zombies)}")
        keys = conn.execute(
            text("SELECT COUNT(*) FROM ingestion_jobs WHERE config IS NOT NULL AND config::jsonb ? 'api_key'")
        ).scalar()
        print(f"ingestion_jobs.config rows with api_key: {keys}")

        if args.revert:
            restored = revert(conn)
            print(f"\nrevert() restored {restored} rows (expected {result['total_rows']})")

        trans.rollback()
        print("\nROLLED BACK — no changes made.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
