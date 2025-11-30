#!/usr/bin/env python3
"""
Direct FRED data ingestion - bypasses FastAPI background tasks.
Run this from the host machine with: python direct_fred_ingestion.py
"""
import os
import sys

# Check if we need to add path for Docker context
if os.path.exists('/app'):
    sys.path.insert(0, '/app')

import asyncio
from datetime import datetime, timedelta

def main():
    """Run FRED ingestion directly."""
    from app.core.database import get_session_factory
    from app.core.models import IngestionJob, JobStatus
    from app.sources.fred import ingest
    from app.core.config import get_settings
    
    print("\n" + "="*70)
    print("FRED Direct Data Ingestion (Bypassing Background Tasks)")
    print("="*70)
    
    # Get settings
    settings = get_settings()
    api_key = settings.fred_api_key
    
    print(f"\n1. Configuration Check:")
    print(f"   FRED API Key: {'✅ Present' if api_key else '❌ Missing'}")
    if api_key:
        print(f"   Key prefix: {api_key[:15]}...")
    
    # Date range: last 2 months for reasonable data
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    print(f"   Date range: {start_date} to {end_date}")
    
    categories = [
        "interest_rates",
        "monetary_aggregates",
        "industrial_production",
        "economic_indicators"
    ]
    
    print(f"\n2. Will ingest {len(categories)} categories")
    
    # Create database session
    SessionLocal = get_session_factory()
    
    async def run_ingestion():
        for i, category in enumerate(categories, 1):
            db = SessionLocal()
            try:
                print(f"\n[{i}/{len(categories)}] Ingesting {category}...")
                print("-" * 70)
                
                # Create job
                job = IngestionJob(
                    source="fred",
                    status=JobStatus.PENDING,
                    config={
                        "category": category,
                        "observation_start": start_date,
                        "observation_end": end_date,
                        "direct_ingestion": True
                    }
                )
                db.add(job)
                db.commit()
                db.refresh(job)
                
                print(f"   Job ID: {job.id}")
                
                # Run ingestion
                result = await ingest.ingest_fred_category(
                    db=db,
                    job_id=job.id,
                    category=category,
                    observation_start=start_date,
                    observation_end=end_date,
                    api_key=api_key
                )
                
                print(f"   ✅ SUCCESS!")
                print(f"      Table: {result['table_name']}")
                print(f"      Series: {result['series_count']}")
                print(f"      Rows: {result['rows_inserted']:,}")
                
            except Exception as e:
                print(f"   ❌ FAILED: {e}")
                import traceback
                traceback.print_exc()
            finally:
                db.close()
        
        print("\n" + "="*70)
        print("Ingestion Complete!")
        print("="*70)
        
        # Show summary
        db = SessionLocal()
        try:
            from sqlalchemy import text
            print("\n📊 Final Data Summary:")
            
            tables = [
                "fred_interest_rates",
                "fred_monetary_aggregates",
                "fred_industrial_production",
                "fred_economic_indicators"
            ]
            
            for table in tables:
                try:
                    result = db.execute(text(f"""
                        SELECT 
                            COUNT(*) as rows,
                            COUNT(DISTINCT series_id) as series,
                            MIN(date) as first_date,
                            MAX(date) as last_date
                        FROM {table}
                    """))
                    row = result.first()
                    if row and row[0] > 0:
                        print(f"\n   {table}:")
                        print(f"      Total rows: {row[0]:,}")
                        print(f"      Series count: {row[1]}")
                        print(f"      Date range: {row[2]} to {row[3]}")
                    else:
                        print(f"\n   {table}: No data")
                except Exception as e:
                    print(f"\n   {table}: Error - {e}")
        finally:
            db.close()
        
        print("\n" + "="*70)
        print("✅ FRED data ingestion complete!")
        print("="*70 + "\n")
    
    # Run the async ingestion
    asyncio.run(run_ingestion())

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Ingestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

