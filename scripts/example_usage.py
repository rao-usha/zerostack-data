"""
Example script demonstrating how to use the Census ingestion service.

This script shows how to programmatically create and monitor ingestion jobs.
"""
import asyncio
import httpx
import time
from typing import Dict, Any


BASE_URL = "http://localhost:8001"


async def create_job(config: Dict[str, Any]) -> Dict[str, Any]:
    """Create an ingestion job."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/api/v1/jobs",
            json={"source": "census", "config": config}
        )
        response.raise_for_status()
        return response.json()


async def get_job_status(job_id: int) -> Dict[str, Any]:
    """Get status of a specific job."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/api/v1/jobs/{job_id}")
        response.raise_for_status()
        return response.json()


async def wait_for_job(job_id: int, timeout: int = 300) -> Dict[str, Any]:
    """Wait for a job to complete (or fail)."""
    start_time = time.time()
    
    while True:
        job = await get_job_status(job_id)
        status = job["status"]
        
        if status in ("success", "failed"):
            return job
        
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")
        
        print(f"Job {job_id} status: {status}")
        await asyncio.sleep(5)


async def main():
    """Example: Ingest Census ACS5 2021 B01001 (Sex by Age) for all states."""
    
    print("Creating ingestion job...")
    
    # Create job
    job = await create_job({
        "survey": "acs5",
        "year": 2021,
        "table_id": "B01001",
        "geo_level": "state"
    })
    
    job_id = job["id"]
    print(f"Job created: ID={job_id}")
    print(f"Status: {job['status']}")
    
    # Wait for completion
    print("\nWaiting for job to complete...")
    completed_job = await wait_for_job(job_id)
    
    # Print results
    print("\n" + "="*60)
    print(f"Job {job_id} completed!")
    print(f"Status: {completed_job['status']}")
    
    if completed_job['status'] == 'success':
        print(f"Rows inserted: {completed_job['rows_inserted']}")
        print(f"Started: {completed_job['started_at']}")
        print(f"Completed: {completed_job['completed_at']}")
        print("\nData is now available in table: acs5_2021_b01001")
    else:
        print(f"Error: {completed_job['error_message']}")
    
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())




