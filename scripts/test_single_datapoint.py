#!/usr/bin/env python3
"""
Minimal test: Fetch ONE data point from FRED and save it.
This verifies the basic flow works.
"""
import sys
sys.path.insert(0, '/app')

import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text
from app.core.database import get_session_factory
from app.core.config import get_settings
from app.sources.fred.client import FREDClient

async def main():
    print("\n" + "="*70)
    print("FRED Single Data Point Test")
    print("="*70)
    
    # Step 1: Check API key
    print("\n[1/5] Checking FRED API key...")
    settings = get_settings()
    api_key = settings.fred_api_key
    
    if not api_key:
        print("   ❌ No API key found!")
        print("   Please set FRED_API_KEY in .env file")
        return
    
    print(f"   ✅ API key found: {api_key[:15]}...")
    
    # Step 2: Test FRED API connection
    print("\n[2/5] Testing FRED API connection...")
    client = FREDClient(api_key=api_key, max_concurrency=1)
    
    try:
        # Just get ONE data point from yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"   Fetching Federal Funds Rate (DFF) for {yesterday}...")
        
        response = await client.get_series_observations(
            series_id="DFF",
            observation_start=yesterday,
            observation_end=yesterday
        )
        
        observations = response.get("observations", [])
        print(f"   ✅ Received {len(observations)} observation(s)")
        
        if not observations:
            print("   ⚠️  No data for yesterday, trying last 7 days...")
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            response = await client.get_series_observations(
                series_id="DFF",
                observation_start=week_ago,
                observation_end=yesterday
            )
            observations = response.get("observations", [])
            print(f"   ✅ Received {len(observations)} observation(s) from last week")
        
        if not observations:
            print("   ❌ No data received from FRED API")
            return
        
        # Get the most recent non-missing value
        data_point = None
        for obs in reversed(observations):
            if obs.get('value') and obs.get('value') != '.':
                data_point = obs
                break
        
        if not data_point:
            print("   ❌ All observations have missing values")
            return
        
        print(f"\n   Data point to insert:")
        print(f"      Date: {data_point.get('date')}")
        print(f"      Value: {data_point.get('value')}%")
        print(f"      Series: DFF (Federal Funds Rate)")
        
    except Exception as e:
        print(f"   ❌ FRED API error: {e}")
        await client.close()
        return
    finally:
        await client.close()
    
    # Step 3: Connect to database
    print("\n[3/5] Connecting to database...")
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        # Test connection
        result = db.execute(text("SELECT 1"))
        result.scalar()
        print("   ✅ Database connection successful")
        
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        db.close()
        return
    
    # Step 4: Insert the single data point
    print("\n[4/5] Inserting data point into fred_interest_rates...")
    
    try:
        insert_sql = text("""
            INSERT INTO fred_interest_rates 
            (series_id, date, value, realtime_start, realtime_end)
            VALUES 
            (:series_id, :date, :value, :realtime_start, :realtime_end)
            ON CONFLICT (series_id, date) 
            DO UPDATE SET
                value = EXCLUDED.value,
                realtime_start = EXCLUDED.realtime_start,
                realtime_end = EXCLUDED.realtime_end,
                ingested_at = NOW()
        """)
        
        db.execute(insert_sql, {
            'series_id': 'DFF',
            'date': data_point.get('date'),
            'value': float(data_point.get('value')),
            'realtime_start': data_point.get('realtime_start'),
            'realtime_end': data_point.get('realtime_end')
        })
        
        db.commit()
        print("   ✅ Data inserted successfully!")
        
    except Exception as e:
        print(f"   ❌ Insert failed: {e}")
        import traceback
        traceback.print_exc()
        db.close()
        return
    
    # Step 5: Verify the data
    print("\n[5/5] Verifying data in database...")
    
    try:
        result = db.execute(text("""
            SELECT series_id, date, value, ingested_at
            FROM fred_interest_rates 
            WHERE series_id = 'DFF'
            ORDER BY date DESC
            LIMIT 1
        """))
        
        row = result.first()
        
        if row:
            print("   ✅ Data verified in database!")
            print(f"\n   Retrieved from database:")
            print(f"      Series: {row[0]}")
            print(f"      Date: {row[1]}")
            print(f"      Value: {row[2]}%")
            print(f"      Ingested at: {row[3]}")
        else:
            print("   ❌ No data found in database!")
            
    except Exception as e:
        print(f"   ❌ Verification failed: {e}")
    finally:
        db.close()
    
    print("\n" + "="*70)
    print("✅ TEST COMPLETE - Basic flow works!")
    print("="*70)
    print("\nNext step: Run full ingestion to populate all series")
    print("\n")

if __name__ == "__main__":
    asyncio.run(main())

