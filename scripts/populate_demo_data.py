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
    python scripts/populate_demo_data.py [--sources census,fred,bls,treasury] [--quick]
"""

import sys
import time
import argparse
import io
from datetime import date
from typing import Dict, Any

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
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
    datasets = [
        {
            "name": "ACS 5-Year 2023 - Population by State",
            "endpoint": "/api/v1/census/state",
            "payload": {
                "survey": "acs5",
                "year": 2023,
                "table_id": "B01001",
                "include_geojson": False,
            },
        },
        {
            "name": "ACS 5-Year 2023 - Median Income (CA Counties)",
            "endpoint": "/api/v1/census/county",
            "payload": {
                "survey": "acs5",
                "year": 2023,
                "table_id": "B19013",
                "state_fips": "06",
                "include_geojson": False,
            },
        },
    ]
    return _run_datasets("U.S. Census Bureau Data", datasets, quick, timeout=600)


def ingest_fred_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample FRED economic data."""
    datasets = [
        {
            "name": "FRED Interest Rates",
            "endpoint": "/api/v1/fred/ingest",
            "payload": {
                "category": "interest_rates",
                "observation_start": "2020-01-01",
                "observation_end": str(date.today()),
            },
        },
        {
            "name": "FRED Economic Indicators",
            "endpoint": "/api/v1/fred/ingest",
            "payload": {
                "category": "economic_indicators",
                "observation_start": "2020-01-01",
                "observation_end": str(date.today()),
            },
        },
        {
            "name": "FRED Monetary Aggregates",
            "endpoint": "/api/v1/fred/ingest",
            "payload": {
                "category": "monetary_aggregates",
                "observation_start": "2020-01-01",
                "observation_end": str(date.today()),
            },
        },
        {
            "name": "FRED Industrial Production",
            "endpoint": "/api/v1/fred/ingest",
            "payload": {
                "category": "industrial_production",
                "observation_start": "2020-01-01",
                "observation_end": str(date.today()),
            },
        },
    ]
    return _run_datasets("Federal Reserve Economic Data (FRED)", datasets, quick)


def ingest_eia_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample EIA energy data."""
    datasets = [
        {
            "name": "EIA Petroleum Consumption (Annual)",
            "endpoint": "/api/v1/eia/petroleum/ingest",
            "payload": {
                "subcategory": "consumption",
                "frequency": "annual",
            },
        },
        {
            "name": "EIA Natural Gas Prices",
            "endpoint": "/api/v1/eia/natural-gas/ingest",
            "payload": {
                "subcategory": "prices",
                "frequency": "annual",
            },
        },
        {
            "name": "EIA Electricity Retail Sales",
            "endpoint": "/api/v1/eia/electricity/ingest",
            "payload": {
                "subcategory": "retail_sales",
                "frequency": "annual",
            },
        },
    ]
    return _run_datasets("Energy Information Administration (EIA)", datasets, quick, timeout=600)


def ingest_sec_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest SEC corporate filing data."""
    datasets = [
        {
            "name": "SEC Apple Inc. (10-K/10-Q)",
            "endpoint": "/api/v1/sec/ingest/company",
            "payload": {
                "cik": "0000320193",
                "filing_types": ["10-K", "10-Q"],
                "start_date": "2022-01-01",
            },
            "timeout": 120,
        },
        {
            "name": "SEC Microsoft Corp. (10-K/10-Q)",
            "endpoint": "/api/v1/sec/ingest/company",
            "payload": {
                "cik": "0000789019",
                "filing_types": ["10-K", "10-Q"],
                "start_date": "2022-01-01",
            },
            "timeout": 120,
        },
        {
            "name": "SEC Apple Financial Data (XBRL)",
            "endpoint": "/api/v1/sec/ingest/financial-data",
            "payload": {"cik": "0000320193"},
            "timeout": 120,
        },
    ]
    return _run_datasets("SEC Corporate Filings", datasets, quick, timeout=600)


def ingest_realestate_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest sample real estate data."""
    datasets = [
        {
            "name": "FHFA House Price Index (State)",
            "endpoint": "/api/v1/realestate/fhfa/ingest",
            "payload": {"geography_type": "State"},
        },
        {
            "name": "HUD Building Permits (National)",
            "endpoint": "/api/v1/realestate/hud/ingest",
            "payload": {"geography_type": "National"},
        },
    ]
    return _run_datasets("Real Estate & Housing Data", datasets, quick)


def _run_datasets(header: str, datasets: list, quick: bool = False,
                   timeout: int = MAX_WAIT_TIME) -> Dict[str, Any]:
    """Generic runner for sources that use endpoint+payload datasets."""
    print_header(header)
    results = {"attempted": 0, "succeeded": 0, "failed": 0}

    if quick:
        datasets = datasets[:1]

    for dataset in datasets:
        results["attempted"] += 1
        name = dataset["name"]
        endpoint = dataset["endpoint"]
        payload = dataset.get("payload", {})
        job_timeout = dataset.get("timeout", timeout)
        print_info(f"Ingesting: {name}")

        try:
            response = requests.post(
                f"{API_BASE_URL}{endpoint}",
                json=payload,
                timeout=10,
            )

            if response.status_code in [200, 201, 202]:
                job_data = response.json()
                job_id = job_data.get("job_id")
                if wait_for_job(job_id, name, timeout=job_timeout):
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


# ── New source functions (15) ──────────────────────────────────────────


def ingest_bls_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest BLS employment, inflation, and unemployment data."""
    datasets = [
        {
            "name": "BLS CES Employment (Nonfarm Payrolls)",
            "endpoint": "/api/v1/bls/ces/ingest",
            "payload": {"start_year": 2020, "end_year": 2025},
            "timeout": 600,
        },
        {
            "name": "BLS CPI Inflation",
            "endpoint": "/api/v1/bls/cpi/ingest",
            "payload": {"start_year": 2020, "end_year": 2025},
            "timeout": 600,
        },
        {
            "name": "BLS CPS Unemployment",
            "endpoint": "/api/v1/bls/cps/ingest",
            "payload": {"start_year": 2020, "end_year": 2025},
            "timeout": 600,
        },
    ]
    return _run_datasets("Bureau of Labor Statistics (BLS)", datasets, quick, timeout=600)


def ingest_bea_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest BEA GDP and regional income data."""
    datasets = [
        {
            "name": "BEA NIPA GDP (Quarterly)",
            "endpoint": "/api/v1/bea/nipa/ingest",
            "payload": {
                "table_name": "T10101",
                "frequency": "Q",
                "year": "2020,2021,2022,2023,2024",
            },
        },
        {
            "name": "BEA Regional Income by State",
            "endpoint": "/api/v1/bea/regional/ingest",
            "payload": {
                "table_name": "SAGDP2N",
                "line_code": "1",
                "geo_fips": "STATE",
                "year": "2023",
            },
        },
    ]
    return _run_datasets("Bureau of Economic Analysis (BEA)", datasets, quick)


def ingest_treasury_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest Treasury debt and interest rate data."""
    datasets = [
        {
            "name": "Treasury National Debt",
            "endpoint": "/api/v1/treasury/debt/ingest",
            "payload": {
                "start_date": "2020-01-01",
                "end_date": "2025-12-31",
            },
        },
        {
            "name": "Treasury Interest Rates",
            "endpoint": "/api/v1/treasury/interest-rates/ingest",
            "payload": {
                "start_date": "2020-01-01",
                "end_date": "2025-12-31",
            },
        },
    ]
    return _run_datasets("U.S. Treasury", datasets, quick)


def ingest_fdic_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest FDIC bank institutions and failed banks data."""
    datasets = [
        {
            "name": "FDIC Active Institutions",
            "endpoint": "/api/v1/fdic/institutions/ingest",
            "payload": {"active_only": True, "limit": 10000},
            "timeout": 600,
        },
        {
            "name": "FDIC Failed Banks (2000-2025)",
            "endpoint": "/api/v1/fdic/failed-banks/ingest",
            "payload": {"year_start": 2000, "year_end": 2025},
        },
    ]
    return _run_datasets("FDIC Banking Data", datasets, quick, timeout=600)


def ingest_fema_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest FEMA disaster declarations."""
    datasets = [
        {
            "name": "FEMA Disaster Declarations (All)",
            "endpoint": "/api/v1/fema/disasters/ingest",
            "payload": {"max_records": 50000},
            "timeout": 600,
        },
    ]
    return _run_datasets("FEMA Disaster Data", datasets, quick, timeout=600)


def ingest_usda_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest USDA crop production data."""
    datasets = [
        {
            "name": "USDA All Major Crops 2024",
            "endpoint": "/api/v1/usda/all-major-crops/ingest",
            "payload": {"year": 2024},
        },
    ]
    return _run_datasets("USDA Agriculture Data", datasets, quick)


def ingest_cftc_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest CFTC Commitments of Traders data."""
    datasets = [
        {
            "name": "CFTC COT Legacy 2024",
            "endpoint": "/api/v1/cftc-cot/ingest",
            "payload": {
                "year": 2024,
                "report_type": "legacy",
                "combined": True,
            },
        },
        {
            "name": "CFTC COT Disaggregated 2024",
            "endpoint": "/api/v1/cftc-cot/ingest",
            "payload": {
                "year": 2024,
                "report_type": "disaggregated",
                "combined": True,
            },
        },
    ]
    return _run_datasets("CFTC Commitments of Traders", datasets, quick)


def ingest_irs_soi_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest IRS Statistics of Income data."""
    datasets = [
        {
            "name": "IRS SOI County Income 2021",
            "endpoint": "/api/v1/irs-soi/county-income/ingest",
            "payload": {"year": 2021, "use_cache": True},
        },
    ]
    return _run_datasets("IRS Statistics of Income", datasets, quick)


def ingest_us_trade_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest US international trade summary data."""
    datasets = [
        {
            "name": "US Trade Summary Dec 2024",
            "endpoint": "/api/v1/us-trade/summary/ingest",
            "payload": {"year": 2024, "month": 12},
            "timeout": 600,
        },
    ]
    return _run_datasets("U.S. International Trade", datasets, quick, timeout=600)


def ingest_data_commons_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest Data Commons US state statistics."""
    datasets = [
        {
            "name": "Data Commons US States (Population + Income)",
            "endpoint": "/api/v1/data-commons/us-states/ingest",
            "payload": {
                "variables": ["Count_Person", "Median_Income_Household"],
            },
        },
    ]
    return _run_datasets("Google Data Commons", datasets, quick)


def ingest_cms_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest CMS Medicare utilization data."""
    datasets = [
        {
            "name": "CMS Medicare Utilization 2023",
            "endpoint": "/api/v1/cms/ingest/medicare-utilization",
            "payload": {"year": 2023, "limit": 5000},
        },
    ]
    return _run_datasets("CMS Healthcare Data", datasets, quick)


def ingest_uspto_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest USPTO patent data for AI technology."""
    datasets = [
        {
            "name": "USPTO AI Patents (CPC G06N)",
            "endpoint": "/api/v1/uspto/ingest/cpc",
            "payload": {
                "cpc_code": "G06N",
                "date_from": "2023-01-01",
                "max_patents": 500,
            },
        },
    ]
    return _run_datasets("USPTO Patent Data", datasets, quick)


def ingest_fbi_crime_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest FBI crime estimates data."""
    datasets = [
        {
            "name": "FBI National Violent Crime Estimates",
            "endpoint": "/api/v1/fbi-crime/estimates/ingest",
            "payload": {
                "scope": "national",
                "offenses": ["violent-crime"],
            },
        },
    ]
    return _run_datasets("FBI Crime Data", datasets, quick)


def ingest_bts_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest BTS border crossing transportation data."""
    datasets = [
        {
            "name": "BTS US-Mexico Border Crossings (Trucks)",
            "endpoint": "/api/v1/bts/border-crossing/ingest",
            "payload": {
                "start_date": "2020-01",
                "end_date": "2024-12",
                "border": "US-Mexico Border",
                "measure": "Trucks",
            },
        },
    ]
    return _run_datasets("BTS Transportation Data", datasets, quick)


def ingest_fcc_data(quick: bool = False) -> Dict[str, Any]:
    """Ingest FCC broadband availability data."""
    datasets = [
        {
            "name": "FCC Broadband by State (CA, NY, TX)",
            "endpoint": "/api/v1/fcc-broadband/state/ingest",
            "payload": {"state_codes": ["CA", "NY", "TX"]},
            "timeout": 600,
        },
    ]
    return _run_datasets("FCC Broadband Data", datasets, quick, timeout=600)


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
    global API_BASE_URL

    parser = argparse.ArgumentParser(
        description="Populate database with demo data from all sources"
    )
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of sources (e.g. census,fred,bls,treasury). Default: all 20"
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
    
    # All available sources (order: existing 5 + 15 new)
    ALL_SOURCES = [
        "census", "fred", "eia", "sec", "realestate",
        "bls", "bea", "treasury", "fdic", "fema",
        "usda", "cftc", "irs_soi", "us_trade", "data_commons",
        "cms", "uspto", "fbi_crime", "bts", "fcc",
    ]

    # Map source key -> (display name, function)
    SOURCE_MAP = {
        "census":       ("Census",          ingest_census_data),
        "fred":         ("FRED",            ingest_fred_data),
        "eia":          ("EIA",             ingest_eia_data),
        "sec":          ("SEC",             ingest_sec_data),
        "realestate":   ("Real Estate",     ingest_realestate_data),
        "bls":          ("BLS",             ingest_bls_data),
        "bea":          ("BEA",             ingest_bea_data),
        "treasury":     ("Treasury",        ingest_treasury_data),
        "fdic":         ("FDIC",            ingest_fdic_data),
        "fema":         ("FEMA",            ingest_fema_data),
        "usda":         ("USDA",            ingest_usda_data),
        "cftc":         ("CFTC",            ingest_cftc_data),
        "irs_soi":      ("IRS SOI",         ingest_irs_soi_data),
        "us_trade":     ("US Trade",        ingest_us_trade_data),
        "data_commons": ("Data Commons",    ingest_data_commons_data),
        "cms":          ("CMS",             ingest_cms_data),
        "uspto":        ("USPTO",           ingest_uspto_data),
        "fbi_crime":    ("FBI Crime",       ingest_fbi_crime_data),
        "bts":          ("BTS",             ingest_bts_data),
        "fcc":          ("FCC",             ingest_fcc_data),
    }

    # Determine which sources to ingest
    if args.sources:
        sources_to_ingest = [s.strip() for s in args.sources.split(",")]
        unknown = [s for s in sources_to_ingest if s not in SOURCE_MAP]
        if unknown:
            print_error(f"Unknown source(s): {', '.join(unknown)}")
            print_info(f"Available: {', '.join(ALL_SOURCES)}")
            return 1
    else:
        sources_to_ingest = ALL_SOURCES

    print_info(f"Will ingest from {len(sources_to_ingest)} sources: {', '.join(sources_to_ingest)}")
    print_warning("This may take several minutes...")

    # Ingest from each source
    all_results = {}
    start_time = time.time()

    for source_key in sources_to_ingest:
        display_name, ingest_fn = SOURCE_MAP[source_key]
        all_results[display_name] = ingest_fn(args.quick)
    
    elapsed_time = time.time() - start_time
    
    # Print summary
    print_summary(all_results)
    print_info(f"\nTotal time: {elapsed_time:.1f} seconds")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

