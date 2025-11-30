#!/usr/bin/env python3
"""
Script to ingest FRED data for all categories.
"""
import httpx
import json
import time
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000/api/v1"

def ingest_all_fred_categories():
    """Ingest all FRED categories with recent data."""
    
    # Date range: last 3 years for fast ingestion
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365*3)).strftime("%Y-%m-%d")
    
    print("=" * 70)
    print("FRED Data Ingestion")
    print("=" * 70)
    print(f"Date range: {start_date} to {end_date}")
    print()
    
    categories = [
        "interest_rates",
        "monetary_aggregates", 
        "industrial_production",
        "economic_indicators"
    ]
    
    # Start batch ingestion
    print("Starting batch ingestion for all categories...")
    response = httpx.post(
        f"{BASE_URL}/fred/ingest/batch",
        json={
            "categories": categories,
            "observation_start": start_date,
            "observation_end": end_date
        },
        timeout=30.0
    )
    
    if response.status_code != 200:
        print(f"❌ Failed to start ingestion: {response.status_code}")
        print(response.text)
        return
    
    result = response.json()
    job_ids = result["job_ids"]
    
    print(f"✅ Created {len(job_ids)} ingestion jobs")
    print(f"   Job IDs: {job_ids}")
    print()
    
    # Monitor job progress
    print("Monitoring job progress...")
    print("-" * 70)
    
    completed = set()
    failed = set()
    
    while len(completed) + len(failed) < len(job_ids):
        for job_id in job_ids:
            if job_id in completed or job_id in failed:
                continue
            
            try:
                job_response = httpx.get(f"{BASE_URL}/jobs/{job_id}")
                job_data = job_response.json()
                
                status = job_data["status"]
                category = job_data["config"]["category"]
                
                if status == "success":
                    completed.add(job_id)
                    rows = job_data.get("rows_inserted", 0)
                    print(f"✅ {category:25s} - SUCCESS ({rows:,} rows)")
                    
                elif status == "failed":
                    failed.add(job_id)
                    error = job_data.get("error_message", "Unknown error")
                    print(f"❌ {category:25s} - FAILED: {error}")
                    
                elif status == "running":
                    print(f"⏳ {category:25s} - Running...")
                    
            except Exception as e:
                print(f"⚠️  Error checking job {job_id}: {e}")
        
        if len(completed) + len(failed) < len(job_ids):
            time.sleep(2)
    
    print("-" * 70)
    print()
    print("=" * 70)
    print(f"Ingestion Complete!")
    print(f"  ✅ Successful: {len(completed)}/{len(job_ids)}")
    if failed:
        print(f"  ❌ Failed: {len(failed)}/{len(job_ids)}")
    print("=" * 70)
    
    return completed, failed


if __name__ == "__main__":
    try:
        completed, failed = ingest_all_fred_categories()
        
        if failed:
            print("\n⚠️  Some jobs failed. Check logs for details.")
            exit(1)
        else:
            print("\n✅ All data ingested successfully!")
            print("\n📊 Query your data:")
            print("   docker-compose exec postgres psql -U nexdata -d nexdata")
            print("   SELECT * FROM fred_interest_rates LIMIT 10;")
            exit(0)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

