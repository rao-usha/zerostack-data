#!/usr/bin/env python3
"""
Data Collection Script - Collect LP and Family Office data.

Usage:
    python demo/run_collection.py --lp-types public_pension --limit 50
    python demo/run_collection.py --fo --limit 50
    python demo/run_collection.py --all --limit 20
"""

import sys
import io
# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import asyncio
import argparse
import sys
import os
from datetime import datetime

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.core.models import LpFund

settings = get_settings()
from app.core.family_office_models import FamilyOffice
from app.sources.lp_collection.types import CollectionConfig, CollectionMode, LpCollectionSource
from app.sources.lp_collection.runner import LpCollectionOrchestrator


def get_db_session():
    """Create database session."""
    engine = create_engine(settings.database_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


async def collect_lps(
    db,
    lp_types: list = None,
    limit: int = 50,
    sources: list = None,
):
    """Collect data for LPs."""
    sources = sources or [LpCollectionSource.WEBSITE, LpCollectionSource.SEC_13F]

    print(f"\n🔍 Starting LP Collection")
    print(f"   Types: {lp_types or 'all'}")
    print(f"   Limit: {limit}")
    print(f"   Sources: {[s.value for s in sources]}")
    print("-" * 50)

    # Get LPs to collect
    query = db.query(LpFund).filter(LpFund.last_collection_at == None)
    if lp_types:
        query = query.filter(LpFund.lp_type.in_(lp_types))
    lps = query.order_by(LpFund.collection_priority.asc()).limit(limit).all()

    print(f"\n📋 Found {len(lps)} LPs to collect")

    total_items = 0
    success_count = 0

    for i, lp in enumerate(lps, 1):
        print(f"\n[{i}/{len(lps)}] {lp.name} ({lp.lp_type})")

        config = CollectionConfig(
            lp_id=lp.id,
            sources=sources,
            rate_limit_delay=1.0,
        )

        orchestrator = LpCollectionOrchestrator(db, config)

        try:
            results = await orchestrator.collect_single_lp(lp.id)

            items = sum(r.items_found for r in results)
            total_items += items

            if any(r.success for r in results):
                success_count += 1
                print(f"   ✅ Found {items} items")
            else:
                print(f"   ⚠️ No data found")
                for r in results:
                    if r.error_message:
                        print(f"      Error: {r.error_message[:80]}")
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:80]}")

    print("\n" + "=" * 50)
    print(f"📊 LP Collection Summary:")
    print(f"   LPs Processed: {len(lps)}")
    print(f"   Successful: {success_count}")
    print(f"   Total Items: {total_items}")
    print("=" * 50)

    return success_count, total_items


async def collect_family_offices(
    db,
    limit: int = 50,
):
    """Collect data for Family Offices."""
    from app.sources.family_office_collection.types import (
        FoCollectionConfig,
        FoCollectionSource,
    )
    from app.sources.family_office_collection.runner import FoCollectionOrchestrator

    print(f"\n🏠 Starting Family Office Collection")
    print(f"   Limit: {limit}")
    print("-" * 50)

    config = FoCollectionConfig(
        sources=[FoCollectionSource.WEBSITE, FoCollectionSource.NEWS],
        max_concurrent_fos=3,
        rate_limit_delay=2.0,
    )

    orchestrator = FoCollectionOrchestrator(config=config)

    try:
        result = await orchestrator.run_collection()

        print("\n" + "=" * 50)
        print(f"📊 FO Collection Summary:")
        print(f"   Status: {result.get('status')}")
        print(f"   FOs Processed: {result.get('total_fos', 0)}")
        print(f"   Successful: {result.get('successful_fos', 0)}")
        print(f"   Total Items: {result.get('total_items', 0)}")
        print("=" * 50)

        return result.get('successful_fos', 0), result.get('total_items', 0)
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0, 0


async def main():
    parser = argparse.ArgumentParser(description="Run data collection for LPs and Family Offices")
    parser.add_argument("--lp-types", nargs="+", help="LP types to collect (e.g., public_pension endowment)")
    parser.add_argument("--fo", action="store_true", help="Collect Family Office data")
    parser.add_argument("--all", action="store_true", help="Collect both LP and FO data")
    parser.add_argument("--limit", type=int, default=20, help="Maximum items to collect")
    parser.add_argument("--sources", nargs="+", default=["website"], help="Collection sources")

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("🚀 NEXDATA DATA COLLECTION")
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    db = get_db_session()

    try:
        lp_success = lp_items = 0
        fo_success = fo_items = 0

        # Parse sources
        source_map = {
            "website": LpCollectionSource.WEBSITE,
            "sec_13f": LpCollectionSource.SEC_13F,
            "sec_adv": LpCollectionSource.SEC_ADV,
            "cafr": LpCollectionSource.CAFR,
            "news": LpCollectionSource.NEWS,
            "form_990": LpCollectionSource.FORM_990,
        }
        sources = [source_map.get(s, LpCollectionSource.WEBSITE) for s in args.sources]

        if args.lp_types or args.all:
            lp_types = args.lp_types if args.lp_types else None
            lp_success, lp_items = await collect_lps(db, lp_types, args.limit, sources)

        if args.fo or args.all:
            fo_success, fo_items = await collect_family_offices(db, args.limit)

        if not args.lp_types and not args.fo and not args.all:
            print("\n⚠️ No collection type specified. Use --help for options.")
            print("\nExamples:")
            print("  python demo/run_collection.py --lp-types public_pension --limit 10")
            print("  python demo/run_collection.py --fo --limit 10")
            print("  python demo/run_collection.py --all --limit 5")

        print(f"\n✅ Collection complete!")
        print(f"   LP: {lp_success} successful, {lp_items} items")
        print(f"   FO: {fo_success} successful, {fo_items} items")

    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
