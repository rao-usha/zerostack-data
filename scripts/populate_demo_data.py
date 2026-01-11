#!/usr/bin/env python3
"""
Demo Data Population Script

Quickly populates the database with sample data from all implemented sources
to showcase the External Data Ingestion Service capabilities.

Features:
- Ingests curated datasets from all sources
- Shows progress with visual feedback
- Handles API rate limits properly
- Safe and bounded (won't overwhelm APIs)
- Can be run multiple times (idempotent where possible)
- Provides summary of what was ingested

Usage:
    python scripts/populate_demo_data.py [--sources census,fred,eia] [--quick]
"""

import asyncio
import sys
import time
import argparse
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import requests
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configuration
API_BASE_URL = "http://localhost:8001"
POLL_INTERVAL = 3  # seconds between status checks
MAX_WAIT_TIME = 300  # maximum seconds to wait for a job

# Color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(text: str):
    """Print a section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}\n")


def print_success(text: str):
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_error(text: str):
    """Print error message."""
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_info(text: str):
    """Print info message."""
    print(f"{Colors.CYAN}ℹ {text}{Colors.END}")


def print_warning(text: str):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def check_service_health() -> bool:
    """Check if the service is running and healthy."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "healthy" and data.get("database") == "connected":
                return True
            else:
                print_warning(f"Service is {data.get('status')}, database is {data.get('database')}")
                return False
        return False
    except requests.exceptions.RequestException as e:
        print_error(f"Cannot connect to service: {e}")
        return False


def wait_for_job(job_id: int, job_name: str, timeout: int = MAX_WAIT_TIME) -> bool:
    """
    Wait for a job to complete and show progress.
    
    Returns True if job succeeded, False otherwise.
    """
    start_time = time.time()
    last_status = None
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{API_BASE_URL}/api/v1/jobs/{job_id}", timeout=10)
            if response.status_code == 200:
                job_data = response.json()
                status = job_data.get("status")
                
                # Print status change
                if status != last_status:
                    if status == "running":
                        print(f"  ⏳ {job_name}: Running...")
                    elif status == "success":
                        rows = job_data.get("rows_ingested", "unknown")
                        print_success(f"{job_name}: Completed ({rows} rows)")
                        return True
                    elif status == "failed":
                        error = job_data.get("error_message", "Unknown error")
                        print_error(f"{job_name}: Failed - {error}")
                        return False
                    last_status = status
            
            time.sleep(POLL_INTERVAL)
            
        except requests.exceptions.RequestException as e:
            print_warning(f"Error checking job status: {e}")
            time.sleep(POLL_INTERVAL)
    
    print_error(f"{job_name}: Timeout after {timeout} seconds")
    return False


def ingest_census_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample Census data."""
    print_header("📊 U.S. Census Bureau Data")
    
    results = {"attempted": 0, "succeeded": 0, "failed": 0}
    
    datasets = [
        {
            "name": "ACS 5-Year 2023 - Population by State",
            "payload": {
                "source": "census",
                "config": {
                    "survey": "acs5",
                    "year": 2023,
                    "table_id": "B01001",  # Sex by Age
                    "geo_level": "state"
                }
            }
        },
        {
            "name": "ACS 5-Year 2023 - Median Income by County",
            "payload": {
                "source": "census",
                "config": {
                    "survey": "acs5",
                    "year": 2023,
                    "table_id": "B19013",  # Median Household Income
                    "geo_level": "county",
                    "state": "06"  # California
                }
            }
        }
    ]
    
    if quick:
        datasets = datasets[:1]  # Only first dataset in quick mode
    
    for dataset in datasets:
        results["attempted"] += 1
        print_info(f"Ingesting: {dataset['name']}")
        
        try:
            response = requests.post(
                f"{API_BASE_URL}/api/v1/jobs",
                json=dataset["payload"],
                timeout=10
            )
            
            if response.status_code == 200:
                job_data = response.json()
                job_id = job_data.get("job_id")
                
                if wait_for_job(job_id, dataset["name"]):
                    results["succeeded"] += 1
                else:
                    results["failed"] += 1
            else:
                print_error(f"Failed to start job: {response.text}")
                results["failed"] += 1
                
        except Exception as e:
            print_error(f"Error: {e}")
            results["failed"] += 1
    
    return results


def ingest_fred_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample FRED economic data."""
    print_header("💰 Federal Reserve Economic Data (FRED)")
    
    results = {"attempted": 0, "succeeded": 0, "failed": 0}
    
    # Key economic indicators
    series_ids = [
        ("GDP", "Gross Domestic Product"),
        ("UNRATE", "Unemployment Rate"),
        ("CPIAUCSL", "Consumer Price Index"),
        ("DFF", "Federal Funds Rate"),
        ("T10Y2Y", "10-Year Treasury Minus 2-Year (Yield Curve)"),
        ("DCOILWTICO", "Crude Oil Prices (WTI)"),
        ("M2SL", "M2 Money Supply"),
        ("INDPRO", "Industrial Production Index")
    ]
    
    if quick:
        series_ids = series_ids[:3]  # Only first 3 in quick mode
    
    for series_id, description in series_ids:
        results["attempted"] += 1
        print_info(f"Ingesting: {description} ({series_id})")
        
        try:
            response = requests.post(
                f"{API_BASE_URL}/api/v1/fred/ingest",
                json={
                    "series_ids": [series_id],
                    "observation_start": "2020-01-01",
                    "observation_end": str(date.today())
                },
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                job_data = response.json()
                job_id = job_data.get("job_id")
                
                if wait_for_job(job_id, f"FRED: {series_id}"):
                    results["succeeded"] += 1
                else:
                    results["failed"] += 1
            else:
                print_warning(f"API returned {response.status_code}: {response.text[:200]}")
                results["failed"] += 1
                
        except Exception as e:
            print_error(f"Error: {e}")
            results["failed"] += 1
    
    return results


def ingest_eia_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample EIA energy data."""
    print_header("⚡ Energy Information Administration (EIA)")
    
    results = {"attempted": 0, "succeeded": 0, "failed": 0}
    
    datasets = [
        {
            "name": "Retail Gasoline Prices (National)",
            "series_id": "PET.EMM_EPMR_PTE_NUS_DPG.W"  # Weekly retail gas prices
        },
        {
            "name": "Natural Gas Prices",
            "series_id": "NG.N3035US3.M"  # Monthly natural gas prices
        },
        {
            "name": "Electricity Generation (Total US)",
            "series_id": "ELEC.GEN.ALL-US-99.M"  # Monthly electricity generation
        }
    ]
    
    if quick:
        datasets = datasets[:1]
    
    for dataset in datasets:
        results["attempted"] += 1
        print_info(f"Ingesting: {dataset['name']}")
        
        try:
            response = requests.post(
                f"{API_BASE_URL}/api/v1/eia/ingest",
                json={
                    "series_id": dataset["series_id"],
                    "start_date": "2020-01-01"
                },
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                job_data = response.json()
                job_id = job_data.get("job_id")
                
                if wait_for_job(job_id, dataset["name"]):
                    results["succeeded"] += 1
                else:
                    results["failed"] += 1
            else:
                print_warning(f"API returned {response.status_code}")
                results["failed"] += 1
                
        except Exception as e:
            print_error(f"Error: {e}")
            results["failed"] += 1
    
    return results


def ingest_sec_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample SEC company data."""
    print_header("📈 SEC Corporate Filings")
    
    results = {"attempted": 0, "succeeded": 0, "failed": 0}
    
    # Sample major companies
    companies = [
        ("0000320193", "Apple Inc."),
        ("0001018724", "Amazon.com Inc."),
        ("0001652044", "Alphabet Inc. (Google)"),
        ("0001326801", "Meta Platforms Inc. (Facebook)"),
        ("0000789019", "Microsoft Corporation")
    ]
    
    if quick:
        companies = companies[:2]
    
    for cik, name in companies:
        results["attempted"] += 1
        print_info(f"Ingesting: {name} (CIK: {cik})")
        
        try:
            response = requests.post(
                f"{API_BASE_URL}/api/v1/sec/ingest/financial",
                json={
                    "cik": cik,
                    "include_xbrl": False  # Skip XBRL for speed
                },
                timeout=10
            )
            
            if response.status_code in [200, 201, 202]:
                job_data = response.json()
                job_id = job_data.get("job_id")
                
                if wait_for_job(job_id, f"SEC: {name}", timeout=120):
                    results["succeeded"] += 1
                else:
                    results["failed"] += 1
            else:
                print_warning(f"API returned {response.status_code}")
                results["failed"] += 1
                
        except Exception as e:
            print_error(f"Error: {e}")
            results["failed"] += 1
    
    return results


def ingest_realestate_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample real estate data."""
    print_header("🏠 Real Estate & Housing Data")
    
    results = {"attempted": 0, "succeeded": 0, "failed": 0}
    
    datasets = [
        {
            "name": "FHFA House Price Index (National)",
            "endpoint": "/api/v1/realestate/fhfa/ingest",
            "payload": {
                "geography_type": "National"
            }
        },
        {
            "name": "HUD Building Permits (National)",
            "endpoint": "/api/v1/realestate/hud/ingest",
            "payload": {
                "geography_level": "National"
            }
        }
    ]
    
    if quick:
        datasets = datasets[:1]
    
    for dataset in datasets:
        results["attempted"] += 1
        print_info(f"Ingesting: {dataset['name']}")
        
        try:
            response = requests.post(
                f"{API_BASE_URL}{dataset['endpoint']}",
                json=dataset["payload"],
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                job_data = response.json()
                job_id = job_data.get("job_id")
                
                if wait_for_job(job_id, dataset["name"]):
                    results["succeeded"] += 1
                else:
                    results["failed"] += 1
            else:
                print_warning(f"API returned {response.status_code}")
                results["failed"] += 1
                
        except Exception as e:
            print_error(f"Error: {e}")
            results["failed"] += 1
    
    return results


def print_summary(all_results: Dict[str, Dict[str, int]]):
    """Print final summary of ingestion results."""
    print_header("📊 Ingestion Summary")
    
    total_attempted = 0
    total_succeeded = 0
    total_failed = 0
    
    for source, results in all_results.items():
        attempted = results["attempted"]
        succeeded = results["succeeded"]
        failed = results["failed"]
        
        total_attempted += attempted
        total_succeeded += succeeded
        total_failed += failed
        
        if attempted > 0:
            success_rate = (succeeded / attempted) * 100
            print(f"\n{Colors.BOLD}{source}:{Colors.END}")
            print(f"  Attempted: {attempted}")
            print(f"  Succeeded: {Colors.GREEN}{succeeded}{Colors.END}")
            print(f"  Failed: {Colors.RED}{failed}{Colors.END}")
            print(f"  Success Rate: {success_rate:.1f}%")
    
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}TOTAL:{Colors.END}")
    print(f"  Attempted: {total_attempted}")
    print(f"  Succeeded: {Colors.GREEN}{total_succeeded}{Colors.END}")
    print(f"  Failed: {Colors.RED}{total_failed}{Colors.END}")
    
    if total_attempted > 0:
        overall_success_rate = (total_succeeded / total_attempted) * 100
        print(f"  Overall Success Rate: {overall_success_rate:.1f}%")
    
    print(f"{Colors.BOLD}{'=' * 60}{Colors.END}")
    
    if total_succeeded > 0:
        print_success(f"\n✓ Successfully populated database with demo data!")
        print_info(f"  View the data at: {API_BASE_URL}/docs")
        print_info(f"  Check all jobs at: {API_BASE_URL}/api/v1/jobs")
    else:
        print_error("\n✗ No data was successfully ingested")
        print_info("  Check API keys in .env file")
        print_info("  Ensure service is running: python scripts/start_service.py")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Populate database with demo data from all sources"
    )
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of sources (census,fred,eia,sec,realestate). Default: all"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: ingest only a few datasets per source"
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default=API_BASE_URL,
        help=f"API base URL (default: {API_BASE_URL})"
    )
    
    args = parser.parse_args()
    
    global API_BASE_URL
    API_BASE_URL = args.api_url
    
    # Print welcome banner
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}External Data Ingestion Service - Demo Data Population{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 60}{Colors.END}")
    
    if args.quick:
        print_info("Running in QUICK mode (reduced datasets)")
    
    # Check service health
    print_info(f"Checking service health at {API_BASE_URL}...")
    if not check_service_health():
        print_error("Service is not healthy or not running")
        print_info("Start the service with: python scripts/start_service.py")
        return 1
    
    print_success("Service is healthy and ready!")
    
    # Determine which sources to ingest
    if args.sources:
        sources_to_ingest = [s.strip() for s in args.sources.split(",")]
    else:
        sources_to_ingest = ["census", "fred", "eia", "sec", "realestate"]
    
    print_info(f"Will ingest from: {', '.join(sources_to_ingest)}")
    print_warning(f"This may take several minutes...")
    
    # Ingest from each source
    all_results = {}
    start_time = time.time()
    
    if "census" in sources_to_ingest:
        all_results["Census"] = ingest_census_data(args.quick)
    
    if "fred" in sources_to_ingest:
        all_results["FRED"] = ingest_fred_data(args.quick)
    
    if "eia" in sources_to_ingest:
        all_results["EIA"] = ingest_eia_data(args.quick)
    
    if "sec" in sources_to_ingest:
        all_results["SEC"] = ingest_sec_data(args.quick)
    
    if "realestate" in sources_to_ingest:
        all_results["Real Estate"] = ingest_realestate_data(args.quick)
    
    elapsed_time = time.time() - start_time
    
    # Print summary
    print_summary(all_results)
    print_info(f"\nTotal time: {elapsed_time:.1f} seconds")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

