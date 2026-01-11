#!/usr/bin/env python3
"""
Quick Demo Script - Ultra-fast data population

Ingests just a few key datasets to quickly show the system works.
Perfect for demos, testing, or quick validation.

Takes ~30 seconds to run.

Usage:
    python scripts/quick_demo.py
"""

import sys
import time
import requests
from pathlib import Path

# Configuration
API_BASE_URL = "http://localhost:8001"

def main():
    """Run quick demo data ingestion."""
    print("🚀 Quick Demo Data Population")
    print("=" * 50)
    
    # Check service
    print("\n1️⃣  Checking service health...")
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print("   ✓ Service is running")
        else:
            print("   ✗ Service health check failed")
            return 1
    except Exception as e:
        print(f"   ✗ Cannot connect to service: {e}")
        print("\n   Start the service with:")
        print("   python scripts/start_service.py")
        return 1
    
    # Ingest a few quick datasets
    datasets = [
        {
            "name": "FRED - GDP",
            "method": "POST",
            "url": f"{API_BASE_URL}/api/v1/fred/ingest",
            "json": {
                "series_ids": ["GDP"],
                "observation_start": "2020-01-01"
            }
        },
        {
            "name": "FRED - Unemployment Rate",
            "method": "POST",
            "url": f"{API_BASE_URL}/api/v1/fred/ingest",
            "json": {
                "series_ids": ["UNRATE"],
                "observation_start": "2020-01-01"
            }
        },
        {
            "name": "Census - Population by State",
            "method": "POST",
            "url": f"{API_BASE_URL}/api/v1/jobs",
            "json": {
                "source": "census",
                "config": {
                    "survey": "acs5",
                    "year": 2023,
                    "table_id": "B01001",
                    "geo_level": "state"
                }
            }
        }
    ]
    
    print("\n2️⃣  Ingesting demo datasets...")
    jobs = []
    
    for i, dataset in enumerate(datasets, 1):
        print(f"\n   [{i}/{len(datasets)}] {dataset['name']}")
        
        try:
            response = requests.request(
                dataset["method"],
                dataset["url"],
                json=dataset.get("json"),
                timeout=10
            )
            
            if response.status_code in [200, 201, 202]:
                data = response.json()
                job_id = data.get("job_id")
                print(f"       ✓ Job started (ID: {job_id})")
                jobs.append((job_id, dataset["name"]))
            else:
                print(f"       ✗ Failed: {response.status_code}")
        except Exception as e:
            print(f"       ✗ Error: {e}")
    
    # Wait for jobs to complete
    print("\n3️⃣  Waiting for jobs to complete...")
    time.sleep(2)  # Give jobs a moment to start
    
    completed = 0
    for job_id, name in jobs:
        for _ in range(30):  # Wait up to 30 seconds per job
            try:
                response = requests.get(f"{API_BASE_URL}/api/v1/jobs/{job_id}", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status")
                    
                    if status == "success":
                        rows = data.get("rows_ingested", "?")
                        print(f"   ✓ {name}: Complete ({rows} rows)")
                        completed += 1
                        break
                    elif status == "failed":
                        error = data.get("error_message", "Unknown")
                        print(f"   ✗ {name}: Failed - {error}")
                        break
                    elif status in ["pending", "running"]:
                        time.sleep(1)
                        continue
                else:
                    break
            except Exception as e:
                print(f"   ✗ Error checking {name}: {e}")
                break
    
    # Summary
    print("\n" + "=" * 50)
    print(f"✅ Complete! {completed}/{len(jobs)} jobs succeeded")
    print("\n📊 Next steps:")
    print(f"   • View API docs: {API_BASE_URL}/docs")
    print(f"   • Check all jobs: {API_BASE_URL}/api/v1/jobs")
    print("   • Run full demo: python scripts/populate_demo_data.py")
    print("=" * 50)
    
    return 0 if completed > 0 else 1


if __name__ == "__main__":
    sys.exit(main())

