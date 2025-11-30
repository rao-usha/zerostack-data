import httpx
import time
import sys

print("="*70)
print("FRED Ingestion via REST API")
print("="*70)

# Trigger ingestion
print("\nTriggering ingestion for interest_rates...")
response = httpx.post(
    "http://localhost:8001/api/v1/fred/ingest",
    json={
        "category": "interest_rates",
        "observation_start": "2024-10-01",
        "observation_end": "2025-11-28"
    },
    timeout=30.0
)

print(f"Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    job_id = data["job_id"]
    print(f"Job ID: {job_id}")
    
    # Wait for job to complete
    print("\nWaiting for job to complete...")
    for i in range(30):  # Wait up to 30 seconds
        time.sleep(1)
        job_response = httpx.get(f"http://localhost:8001/api/v1/jobs/{job_id}")
        job_data = job_response.json()
        status = job_data["status"]
        
        print(f"  [{i+1}s] Status: {status}", end="\r")
        
        if status in ["success", "failed"]:
            print(f"\n\nFinal status: {status}")
            if status == "success":
                print(f"Rows inserted: {job_data.get('rows_inserted', 0)}")
            else:
                print(f"Error: {job_data.get('error_message', 'Unknown')}")
            break
    
    print("\n" + "="*70)
else:
    print(f"Error: {response.text}")
    sys.exit(1)

