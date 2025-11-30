#!/usr/bin/env python3
"""Test FRED ingestion for a single category with full visibility."""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, '/app')

from app.core.database import get_session_factory
from app.core.models import IngestionJob, JobStatus
from app.sources.fred import ingest
from app.core.config import get_settings
from datetime import datetime, timedelta

async def main():
    print("\n" + "=" * 70)
    print("FRED Single Category Test - Interest Rates")
    print("=" * 70 + "\n")
    
    # Check API key
    settings = get_settings()
    api_key = settings.fred_api_key
    
    print(f"1. Checking FRED API Key...")
    if api_key:
        print(f"   ✅ API Key found: {api_key[:15]}...")
    else:
        print(f"   ⚠️  No API key - using throttled mode")
    
    # Create database session
    print(f"\n2. Creating database session...")
    SessionLocal = get_session_factory()
    db = SessionLocal()
    print(f"   ✅ Connected to database")
    
    # Date range - just last 30 days for quick test
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    print(f"\n3. Date range: {start_date} to {end_date}")
    
    # Create job
    print(f"\n4. Creating ingestion job...")
    job = IngestionJob(
        source="fred",
        status=JobStatus.PENDING,
        config={
            "category": "interest_rates",
            "observation_start": start_date,
            "observation_end": end_date
        }
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    print(f"   ✅ Job ID: {job.id}")
    
    # Run ingestion
    print(f"\n5. Starting FRED ingestion...")
    print(f"   Category: interest_rates")
    print(f"   Series: DFF, DGS10, DGS30, DGS3MO, DGS2, DGS5, DPRIME")
    print()
    
    try:
        result = await ingest.ingest_fred_category(
            db=db,
            job_id=job.id,
            category="interest_rates",
            observation_start=start_date,
            observation_end=end_date,
            api_key=api_key
        )
        
        print("\n" + "=" * 70)
        print("✅ INGESTION SUCCESSFUL!")
        print("=" * 70)
        print(f"\nResults:")
        print(f"  Table: {result['table_name']}")
        print(f"  Category: {result['category']}")
        print(f"  Series Count: {result['series_count']}")
        print(f"  Rows Inserted: {result['rows_inserted']:,}")
        print(f"  Date Range: {result['date_range']}")
        
        # Query the data
        from sqlalchemy import text
        print(f"\n6. Verifying data in database...")
        result_query = db.execute(text("""
            SELECT 
                series_id,
                COUNT(*) as observations,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM fred_interest_rates
            GROUP BY series_id
            ORDER BY series_id
        """))
        
        print(f"\n   Data by Series:")
        for row in result_query:
            print(f"   - {row[0]}: {row[1]:,} observations ({row[2]} to {row[3]})")
        
        print("\n" + "=" * 70)
        print("✅ TEST PASSED - FRED data successfully ingested!")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\n" + "=" * 70)
        print(f"❌ INGESTION FAILED")
        print("=" * 70)
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main())

