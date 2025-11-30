#!/usr/bin/env python3
"""Synchronous FRED ingestion - direct and simple."""
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
    print("FRED Direct Synchronous Ingestion")
    print("="*70)
    
    settings = get_settings()
    api_key = settings.fred_api_key
    
    print(f"\n1. API Key Check:")
    if api_key:
        print(f"   ✅ FRED_API_KEY: {api_key[:20]}...")
    else:
        print(f"   ❌ No API key found!")
        return
    
    print(f"\n2. Testing FRED API connection...")
    client = FREDClient(api_key=api_key, max_concurrency=2)
    
    try:
        # Test with a single series - Federal Funds Rate
        print(f"   Fetching Federal Funds Rate (DFF) for last 30 days...")
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        response = await client.get_series_observations(
            series_id="DFF",
            observation_start=start_date,
            observation_end=end_date
        )
        
        observations = response.get("observations", [])
        print(f"   ✅ Received {len(observations)} observations")
        
        if observations:
            print(f"\n3. Sample data:")
            for obs in observations[:5]:
                print(f"      {obs.get('date')}: {obs.get('value')}")
        
        # Insert into database
        if observations:
            print(f"\n4. Inserting data into database...")
            SessionLocal = get_session_factory()
            db = SessionLocal()
            
            try:
                # Prepare data
                rows = []
                for obs in observations:
                    if obs.get('value') and obs.get('value') != '.':
                        rows.append({
                            'series_id': 'DFF',
                            'date': obs.get('date'),
                            'value': float(obs.get('value')),
                            'realtime_start': obs.get('realtime_start'),
                            'realtime_end': obs.get('realtime_end')
                        })
                
                print(f"   Prepared {len(rows)} rows for insertion")
                
                # Insert
                insert_sql = text("""
                    INSERT INTO fred_interest_rates 
                    (series_id, date, value, realtime_start, realtime_end)
                    VALUES 
                    (:series_id, :date, :value, :realtime_start, :realtime_end)
                    ON CONFLICT (series_id, date) 
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        realtime_start = EXCLUDED.realtime_start,
                        realtime_end = EXCLUDED.realtime_end
                """)
                
                for row in rows:
                    db.execute(insert_sql, row)
                
                db.commit()
                print(f"   ✅ Inserted {len(rows)} rows!")
                
                # Verify
                result = db.execute(text("SELECT COUNT(*) FROM fred_interest_rates WHERE series_id='DFF'"))
                count = result.scalar()
                print(f"\n5. Verification:")
                print(f"   Total DFF observations in database: {count}")
                
                # Show sample
                result = db.execute(text("""
                    SELECT date, value 
                    FROM fred_interest_rates 
                    WHERE series_id='DFF' 
                    ORDER BY date DESC 
                    LIMIT 5
                """))
                
                print(f"\n   Latest 5 observations:")
                for row in result:
                    print(f"      {row[0]}: {row[1]}%")
                
            finally:
                db.close()
        
        print("\n" + "="*70)
        print("✅ SUCCESS - FRED data ingested!")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())

