#!/usr/bin/env python3
"""
Direct FRED ingestion bypassing background tasks.
"""
import asyncio
from datetime import datetime, timedelta
from app.core.database import get_session_factory
from app.core.models import IngestionJob, JobStatus
from app.sources.fred import ingest
from app.core.config import get_settings

async def main():
    print("=" * 70)
    print("FRED Direct Data Ingestion")
    print("=" * 70)
    
    # Get settings and check for API key
    settings = get_settings()
    api_key = settings.get_fred_api_key()
    
    if api_key:
        print(f"✅ FRED API Key configured: {api_key[:10]}...")
    else:
        print("⚠️  No FRED API key found - will use throttled mode")
    
    # Date range: last 3 years
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365*3)).strftime("%Y-%m-%d")
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print()
    
    categories = [
        "interest_rates",
        "monetary_aggregates",
        "industrial_production",
        "economic_indicators"
    ]
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        for i, category in enumerate(categories, 1):
            print(f"\n[{i}/{len(categories)}] Ingesting {category}...")
            print("-" * 70)
            
            # Create job
            job = IngestionJob(
                source="fred",
                status=JobStatus.PENDING,
                config={
                    "category": category,
                    "observation_start": start_date,
                    "observation_end": end_date
                }
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            
            print(f"   Job ID: {job.id}")
            
            try:
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
                print(f"   Table: {result['table_name']}")
                print(f"   Series: {result['series_count']}")
                print(f"   Rows: {result['rows_inserted']:,}")
                
            except Exception as e:
                print(f"   ❌ FAILED: {e}")
                import traceback
                traceback.print_exc()
        
        print("\n" + "=" * 70)
        print("Ingestion Complete!")
        print("=" * 70)
        
        # Show summary
        print("\n📊 Data Summary:")
        for table in ["fred_interest_rates", "fred_monetary_aggregates", 
                      "fred_industrial_production", "fred_economic_indicators"]:
            try:
                from sqlalchemy import text
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
                    print(f"      Rows: {row[0]:,}")
                    print(f"      Series: {row[1]}")
                    print(f"      Date Range: {row[2]} to {row[3]}")
            except Exception as e:
                print(f"   {table}: Not created or error - {e}")
        
        print("\n" + "=" * 70)
        
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())

