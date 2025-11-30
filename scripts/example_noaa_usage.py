"""
Example usage script for NOAA data ingestion.

This script demonstrates how to use the NOAA data source adapter
to ingest weather and climate data.

Prerequisites:
1. Service running: uvicorn app.main:app --reload
2. NOAA CDO API token (get from https://www.ncdc.noaa.gov/cdo-web/token)
3. PostgreSQL database running (docker-compose up -d)

Usage:
    python example_noaa_usage.py
"""
import requests
from datetime import date, timedelta
import time
import os

# Configuration
BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")
NOAA_TOKEN = os.getenv("NOAA_TOKEN", "YOUR_NOAA_TOKEN_HERE")

# Check if token is configured
if NOAA_TOKEN == "YOUR_NOAA_TOKEN_HERE":
    print("❌ Error: Please set NOAA_TOKEN environment variable")
    print("Get a token from: https://www.ncdc.noaa.gov/cdo-web/token")
    print()
    print("Set it with:")
    print("  export NOAA_TOKEN='your_token'  # Linux/Mac")
    print("  set NOAA_TOKEN=your_token       # Windows cmd")
    print("  $env:NOAA_TOKEN='your_token'    # Windows PowerShell")
    exit(1)


def print_section(title: str):
    """Print a formatted section header."""
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)
    print()


def check_service():
    """Check if the service is running."""
    print_section("1. Checking Service Health")
    
    try:
        response = requests.get(f"{BASE_URL.replace('/api/v1', '')}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Service is running")
            return True
        else:
            print(f"❌ Service returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Service is not running")
        print("Start it with: uvicorn app.main:app --reload")
        return False


def list_datasets():
    """List available NOAA datasets."""
    print_section("2. Listing Available Datasets")
    
    response = requests.get(f"{BASE_URL}/noaa/datasets")
    response.raise_for_status()
    
    data = response.json()
    print(f"Found {data['count']} datasets:")
    print()
    
    for dataset in data['datasets']:
        print(f"📊 {dataset['dataset_key']}")
        print(f"   Name: {dataset['name']}")
        print(f"   Description: {dataset['description']}")
        print(f"   Data Types: {', '.join(dataset['data_types'][:3])}...")
        print(f"   Table: {dataset['table_name']}")
        print()


def find_stations():
    """Find weather stations in California."""
    print_section("3. Finding Weather Stations")
    
    print("Searching for weather stations in California (FIPS:06)...")
    
    response = requests.get(
        f"{BASE_URL}/noaa/stations",
        params={
            "token": NOAA_TOKEN,
            "dataset_id": "GHCND",
            "location_id": "FIPS:06",
            "limit": 5
        }
    )
    response.raise_for_status()
    
    data = response.json()
    print(f"Found {data['count']} stations (showing first 5):")
    print()
    
    for station in data['stations'][:5]:
        print(f"🌡️  {station.get('name', 'Unknown')}")
        print(f"   ID: {station.get('id', 'N/A')}")
        print(f"   Latitude: {station.get('latitude', 'N/A')}")
        print(f"   Longitude: {station.get('longitude', 'N/A')}")
        print(f"   Elevation: {station.get('elevation', 'N/A')} m")
        print()


def ingest_sample_data():
    """Ingest sample weather data."""
    print_section("4. Ingesting Sample Data")
    
    # Get data for past week
    end_date = date.today() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=7)  # 7 days ago
    
    print(f"Ingesting daily weather data for California")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Data types: TMAX, TMIN, PRCP")
    print()
    print("This may take 30-60 seconds...")
    print()
    
    payload = {
        "token": NOAA_TOKEN,
        "dataset_key": "ghcnd_daily",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "location_id": "FIPS:06",
        "data_type_ids": ["TMAX", "TMIN", "PRCP"],
        "max_results": 1000,  # Limit for demo
        "max_concurrency": 3,
        "requests_per_second": 4.0
    }
    
    response = requests.post(f"{BASE_URL}/noaa/ingest", json=payload)
    response.raise_for_status()
    
    result = response.json()
    
    print("✅ Ingestion completed!")
    print()
    print(f"Job ID: {result['job_id']}")
    print(f"Status: {result['status']}")
    print(f"Rows fetched: {result['rows_fetched']}")
    print(f"Rows inserted: {result['rows_inserted']}")
    print(f"Table: {result['table_name']}")
    print()
    
    return result['job_id']


def check_job_status(job_id: int):
    """Check the status of an ingestion job."""
    print_section("5. Checking Job Status")
    
    response = requests.get(f"{BASE_URL}/jobs/{job_id}")
    response.raise_for_status()
    
    job = response.json()
    
    print(f"Job ID: {job['id']}")
    print(f"Source: {job['source']}")
    print(f"Dataset: {job['dataset_id']}")
    print(f"Status: {job['status']}")
    print(f"Started: {job['started_at']}")
    print(f"Completed: {job.get('completed_at', 'N/A')}")
    print()


def query_example():
    """Show example SQL queries for the data."""
    print_section("6. Example SQL Queries")
    
    print("Now that data is ingested, you can query it with SQL:")
    print()
    
    print("-- Get recent temperatures")
    print("SELECT date, station, datatype, value")
    print("FROM noaa_ghcnd_daily")
    print("WHERE datatype IN ('TMAX', 'TMIN')")
    print("  AND date >= CURRENT_DATE - INTERVAL '7 days'")
    print("ORDER BY date DESC, station, datatype")
    print("LIMIT 20;")
    print()
    
    print("-- Calculate average daily temperature by date")
    print("SELECT ")
    print("  date,")
    print("  ROUND(AVG(CASE WHEN datatype = 'TMAX' THEN value END), 1) as avg_max_temp,")
    print("  ROUND(AVG(CASE WHEN datatype = 'TMIN' THEN value END), 1) as avg_min_temp,")
    print("  ROUND(AVG(CASE WHEN datatype = 'PRCP' THEN value END), 2) as avg_precipitation")
    print("FROM noaa_ghcnd_daily")
    print("WHERE date >= CURRENT_DATE - INTERVAL '7 days'")
    print("GROUP BY date")
    print("ORDER BY date DESC;")
    print()


def main():
    """Run the example workflow."""
    print()
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║           NOAA Weather Data Ingestion - Example Usage             ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    try:
        # Step 1: Check service
        if not check_service():
            return
        
        # Step 2: List datasets
        list_datasets()
        
        # Step 3: Find stations
        find_stations()
        
        # Step 4: Ingest sample data
        job_id = ingest_sample_data()
        
        # Step 5: Check job status
        time.sleep(1)  # Brief pause
        check_job_status(job_id)
        
        # Step 6: Show query examples
        query_example()
        
        # Success summary
        print_section("Summary")
        print("✅ Successfully demonstrated NOAA data ingestion!")
        print()
        print("Next steps:")
        print("1. Check the FastAPI docs: http://localhost:8000/docs")
        print("2. Read NOAA_QUICK_START.md for detailed documentation")
        print("3. Query the data using the SQL examples above")
        print("4. Try other datasets: normal_daily, gsom, etc.")
        print()
        print("For production use:")
        print("- Store NOAA_TOKEN securely (environment variable or secrets manager)")
        print("- Adjust max_concurrency and requests_per_second for your needs")
        print("- Use chunking for large date ranges")
        print("- Monitor ingestion_jobs table for job status")
        print()
        
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ HTTP Error: {e}")
        if e.response is not None:
            print(f"Response: {e.response.text}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()



