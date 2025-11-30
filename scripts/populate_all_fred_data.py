#!/usr/bin/env python3
"""Populate all FRED data - simplified and reliable."""
import sys
sys.path.insert(0, '/app')

import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text
from app.core.database import get_session_factory
from app.core.config import get_settings
from app.sources.fred.client import FREDClient, COMMON_SERIES

async def main():
    print("\n" + "="*70)
    print("FRED Full Data Population")
    print("="*70)
    
    settings = get_settings()
    api_key = settings.fred_api_key
    
    if not api_key:
        print("\n❌ No FRED API key found!")
        return
    
    print(f"\n✅ API Key: {api_key[:15]}...")
    
    # Date range: last 2 years
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    print(f"📅 Date range: {start_date} to {end_date}")
    
    # Initialize client
    client = FREDClient(api_key=api_key, max_concurrency=3)
    SessionLocal = get_session_factory()
    
    # Category to table mapping
    category_tables = {
        "interest_rates": "fred_interest_rates",
        "monetary_aggregates": "fred_monetary_aggregates",
        "industrial_production": "fred_industrial_production",
        "economic_indicators": "fred_economic_indicators"
    }
    
    total_inserted = 0
    
    try:
        for category, table_name in category_tables.items():
            print(f"\n{'='*70}")
            print(f"Category: {category}")
            print(f"Table: {table_name}")
            print(f"{'='*70}")
            
            # Get series for this category
            series_dict = COMMON_SERIES[category]
            series_ids = list(series_dict.values())
            
            print(f"Series to fetch: {len(series_ids)}")
            
            category_rows = 0
            
            for i, series_id in enumerate(series_ids, 1):
                print(f"\n[{i}/{len(series_ids)}] Fetching {series_id}...")
                
                try:
                    # Fetch data
                    response = await client.get_series_observations(
                        series_id=series_id,
                        observation_start=start_date,
                        observation_end=end_date
                    )
                    
                    observations = response.get("observations", [])
                    print(f"   Received: {len(observations)} observations")
                    
                    if not observations:
                        print(f"   ⚠️  No data")
                        continue
                    
                    # Prepare rows
                    rows = []
                    for obs in observations:
                        value_str = obs.get('value')
                        if value_str and value_str != '.':
                            try:
                                rows.append({
                                    'series_id': series_id,
                                    'date': obs.get('date'),
                                    'value': float(value_str),
                                    'realtime_start': obs.get('realtime_start'),
                                    'realtime_end': obs.get('realtime_end')
                                })
                            except ValueError:
                                pass
                    
                    if not rows:
                        print(f"   ⚠️  All values missing")
                        continue
                    
                    print(f"   Inserting: {len(rows)} valid rows...")
                    
                    # Insert into database
                    db = SessionLocal()
                    try:
                        insert_sql = text(f"""
                            INSERT INTO {table_name}
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
                        
                        # Insert in batches of 100
                        for j in range(0, len(rows), 100):
                            batch = rows[j:j+100]
                            for row in batch:
                                db.execute(insert_sql, row)
                            db.commit()
                        
                        category_rows += len(rows)
                        total_inserted += len(rows)
                        print(f"   ✅ Inserted: {len(rows)} rows")
                        
                    except Exception as e:
                        print(f"   ❌ Insert error: {e}")
                        db.rollback()
                    finally:
                        db.close()
                    
                    # Small delay to respect rate limits
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    print(f"   ❌ Fetch error: {e}")
                    continue
            
            print(f"\n{category} complete: {category_rows} rows inserted")
        
    finally:
        await client.close()
    
    # Final summary
    print("\n" + "="*70)
    print("FINAL SUMMARY")
    print("="*70)
    
    db = SessionLocal()
    try:
        for table_name in category_tables.values():
            result = db.execute(text(f"""
                SELECT 
                    COUNT(*) as rows,
                    COUNT(DISTINCT series_id) as series,
                    MIN(date) as first_date,
                    MAX(date) as last_date
                FROM {table_name}
            """))
            row = result.first()
            if row and row[0] > 0:
                print(f"\n{table_name}:")
                print(f"  Rows: {row[0]:,}")
                print(f"  Series: {row[1]}")
                print(f"  Range: {row[2]} to {row[3]}")
    finally:
        db.close()
    
    print(f"\n{'='*70}")
    print(f"✅ COMPLETE! Total rows inserted: {total_inserted:,}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    asyncio.run(main())

