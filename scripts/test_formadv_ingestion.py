"""
Test script for SEC Form ADV ingestion.

This script demonstrates how to use the Form ADV ingestion endpoints
to fetch business contact information for family offices.

Usage:
    python test_formadv_ingestion.py

Make sure the service is running first:
    docker-compose up -d
"""
import requests
import time
import json
from typing import List, Dict, Any


BASE_URL = "http://localhost:8000"


def ingest_family_offices(family_office_names: List[str]) -> Dict[str, Any]:
    """
    Trigger Form ADV ingestion for a list of family offices.
    
    Args:
        family_office_names: List of family office names to search
        
    Returns:
        Response with job_id
    """
    url = f"{BASE_URL}/api/v1/sec/form-adv/ingest/family-offices"
    
    payload = {
        "family_office_names": family_office_names,
        "max_concurrency": 1,
        "max_requests_per_second": 2.0
    }
    
    print(f"\n📤 Requesting Form ADV ingestion for {len(family_office_names)} family offices...")
    print(f"   POST {url}")
    print(f"   Payload: {json.dumps(payload, indent=2)}")
    
    response = requests.post(url, json=payload)
    response.raise_for_status()
    
    result = response.json()
    print(f"\n✅ Job created: ID={result['job_id']}, Status={result['status']}")
    
    return result


def check_job_status(job_id: int) -> Dict[str, Any]:
    """
    Check the status of an ingestion job.
    
    Args:
        job_id: Job ID to check
        
    Returns:
        Job status information
    """
    url = f"{BASE_URL}/api/v1/jobs/{job_id}"
    
    response = requests.get(url)
    response.raise_for_status()
    
    return response.json()


def wait_for_job(job_id: int, timeout: int = 300, poll_interval: int = 5) -> Dict[str, Any]:
    """
    Wait for a job to complete, polling periodically.
    
    Args:
        job_id: Job ID to wait for
        timeout: Maximum time to wait (seconds)
        poll_interval: Time between status checks (seconds)
        
    Returns:
        Final job status
    """
    print(f"\n⏳ Waiting for job {job_id} to complete...")
    
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed > timeout:
            print(f"\n⚠️  Timeout reached ({timeout}s)")
            break
        
        job_status = check_job_status(job_id)
        status = job_status["status"]
        
        print(f"   [{int(elapsed)}s] Status: {status}", end="")
        
        if status in ["success", "failed"]:
            print()
            break
        
        print(f" (polling again in {poll_interval}s...)", end="\r")
        time.sleep(poll_interval)
    
    return job_status


def query_form_adv_data(limit: int = 10):
    """
    Query the Form ADV data from the database via API.
    
    Note: You may need to create a query endpoint or use direct SQL.
    """
    # For now, this is a placeholder
    # You can add a query endpoint to your API or use psql directly
    
    print(f"\n📊 To query Form ADV data, use:")
    print(f"   docker-compose exec postgres psql -U nexdata -d nexdata")
    print(f"   SELECT firm_name, business_phone, business_email, website")
    print(f"   FROM sec_form_adv")
    print(f"   WHERE is_family_office = TRUE")
    print(f"   LIMIT {limit};")


def main():
    """Main test flow."""
    
    print("=" * 80)
    print("SEC Form ADV Ingestion Test")
    print("=" * 80)
    
    # Family offices from user's list
    family_offices = [
        "Soros Fund Management",
        "Pritzker Group",
        "Cascade Investment",
        "MSD Capital",
        "Emerson Collective",
    ]
    
    print(f"\n🎯 Target family offices:")
    for office in family_offices:
        print(f"   - {office}")
    
    print(f"\n⚠️  Important Notes:")
    print(f"   - Many family offices qualify for registration exemptions")
    print(f"   - Only registered investment advisers have Form ADV data")
    print(f"   - We retrieve BUSINESS contact info only (not personal PII)")
    print(f"   - IAPD API may have rate limits - being conservative")
    
    try:
        # Step 1: Trigger ingestion
        result = ingest_family_offices(family_offices)
        job_id = result["job_id"]
        
        # Step 2: Wait for completion
        final_status = wait_for_job(job_id, timeout=300, poll_interval=5)
        
        # Step 3: Display results
        print(f"\n" + "=" * 80)
        print(f"Job Complete!")
        print(f"=" * 80)
        print(f"\nStatus: {final_status['status']}")
        
        if final_status.get("metadata"):
            metadata = final_status["metadata"]
            print(f"\nResults:")
            print(f"   Searched offices: {metadata.get('searched_offices', 'N/A')}")
            print(f"   Matches found: {metadata.get('total_matches_found', 'N/A')}")
            print(f"   Successfully ingested: {metadata.get('total_ingested', 'N/A')}")
            
            if metadata.get("errors"):
                print(f"\nErrors encountered:")
                for error in metadata["errors"][:5]:
                    print(f"   - {error}")
        
        if final_status.get("error_message"):
            print(f"\nError: {final_status['error_message']}")
        
        # Step 4: Show how to query data
        query_form_adv_data()
        
        print(f"\n✅ Test complete!")
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ HTTP Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status code: {e.response.status_code}")
            print(f"   Response: {e.response.text}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

