import httpx

job_ids = [36, 37, 38, 39]

print("FRED Ingestion Job Status")
print("=" * 70)

for job_id in job_ids:
    try:
        response = httpx.get(f"http://localhost:8001/api/v1/jobs/{job_id}")
        data = response.json()
        
        status = data["status"]
        category = data.get("config", {}).get("category", "unknown")
        rows = data.get("rows_inserted", 0)
        
        status_icon = "✅" if status == "success" else "❌" if status == "failed" else "⏳"
        
        print(f"{status_icon} Job {job_id}: {category:25s} - {status:10s} - {rows:,} rows")
        
        if status == "failed":
            print(f"   Error: {data.get('error_message', 'Unknown')}")
            
    except Exception as e:
        print(f"❌ Job {job_id}: Error checking - {e}")

print("=" * 70)

