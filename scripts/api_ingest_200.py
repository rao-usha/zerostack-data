"""
API-based batch SEC ingestion for 200+ companies.

Uses the FastAPI endpoints to trigger ingestion jobs.
More reliable than direct database access.
"""
import requests
import time
import json
from datetime import datetime
from sec_companies_200 import get_all_companies, COMPANIES_200

# API Configuration
API_BASE_URL = "http://localhost:8001/api/v1"
BATCH_SIZE = 10  # Companies per batch
DELAY_BETWEEN_COMPANIES = 1  # Seconds
DELAY_BETWEEN_BATCHES = 5  # Seconds


class Stats:
    def __init__(self):
        self.total = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []
        self.start_time = None
    
    def start(self):
        self.start_time = datetime.now()
    
    def print_progress(self, current):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        progress = (current / self.total) * 100
        rate = current / max(elapsed / 60, 0.01)  # companies per minute
        eta = (self.total - current) / max(rate, 0.01)
        
        print(f"\n{'='*80}")
        print(f"Progress: {current}/{self.total} ({progress:.1f}%)")
        print(f"Success: {self.successful} | Failed: {self.failed} | Skipped: {self.skipped}")
        print(f"Elapsed: {elapsed/60:.1f} min | Rate: {rate:.1f} companies/min | ETA: {eta:.1f} min")
        print(f"{'='*80}\n")
    
    def print_summary(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n{'='*80}")
        print("INGESTION COMPLETE - SUMMARY")
        print(f"{'='*80}")
        print(f"Total companies:  {self.total}")
        print(f"Successful:       {self.successful}")
        print(f"Failed:           {self.failed}")
        print(f"Skipped:          {self.skipped}")
        print(f"Total time:       {elapsed/60:.1f} minutes")
        print(f"Average:          {elapsed/max(self.successful, 1):.1f} seconds per company")
        print(f"{'='*80}\n")
        
        if self.errors:
            print("ERRORS (first 20):")
            for i, err in enumerate(self.errors[:20], 1):
                print(f"  {i}. {err['company']}: {err['error'][:80]}")
            if len(self.errors) > 20:
                print(f"  ... and {len(self.errors) - 20} more errors")
            print()


def check_service_health():
    """Check if the FastAPI service is running."""
    try:
        response = requests.get(f"{API_BASE_URL}/../health", timeout=5)
        if response.status_code == 200:
            print("✓ FastAPI service is running")
            return True
        else:
            print(f"✗ Service returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Cannot connect to service: {e}")
        print(f"\nMake sure the service is running:")
        print(f"  docker-compose up -d")
        print(f"  OR")
        print(f"  uvicorn app.main:app --reload")
        return False


def ingest_company(cik: str, name: str, sector: str, stats: Stats):
    """
    Ingest a single company via API.
    
    Returns True if successful, False otherwise.
    """
    try:
        print(f"[{name}] Starting ingestion... (CIK: {cik})")
        
        # Call the full company ingestion endpoint
        response = requests.post(
            f"{API_BASE_URL}/sec/ingest/full-company",
            json={
                "cik": cik,
                "filing_types": ["10-K", "10-Q"]
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            jobs = data.get("jobs", [])
            print(f"[{name}] ✓ Jobs created: {len(jobs)}")
            for job in jobs:
                print(f"        - {job['type']}: job_id={job['job_id']}")
            stats.successful += 1
            return True
        else:
            error_msg = response.text[:200]
            print(f"[{name}] ✗ API error {response.status_code}: {error_msg}")
            stats.failed += 1
            stats.errors.append({"company": name, "error": f"HTTP {response.status_code}: {error_msg}"})
            return False
    
    except requests.exceptions.Timeout:
        print(f"[{name}] ✗ Request timeout")
        stats.failed += 1
        stats.errors.append({"company": name, "error": "Request timeout"})
        return False
    
    except Exception as e:
        print(f"[{name}] ✗ Error: {e}")
        stats.failed += 1
        stats.errors.append({"company": name, "error": str(e)})
        return False


def main():
    print("\n" + "="*80)
    print("SEC DATA INGESTION - 229 COMPANIES VIA API")
    print("="*80 + "\n")
    
    # Check service health
    if not check_service_health():
        print("\nPlease start the FastAPI service and try again.\n")
        return
    
    # Get all companies
    companies = get_all_companies()
    
    print(f"\nTotal companies: {len(companies)}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Estimated time: {len(companies) * 2 / 60:.0f}-{len(companies) * 3 / 60:.0f} minutes\n")
    
    # Sector breakdown
    print("Companies by sector:")
    sector_counts = {}
    for company in companies:
        sector = company["sector"]
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
    
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        print(f"  {sector:30s}: {count:3d}")
    
    print("\n" + "="*80)
    print("\nStarting ingestion in 3 seconds... (Ctrl+C to cancel)")
    
    try:
        time.sleep(3)
    except KeyboardInterrupt:
        print("\n\nCancelled.\n")
        return
    
    print()
    
    # Initialize stats
    stats = Stats()
    stats.total = len(companies)
    stats.start()
    
    # Process companies
    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/{len(companies)}] {company['name']}")
        print(f"  Sector: {company['sector']}")
        print(f"  CIK: {company['cik']}")
        
        # Ingest company
        success = ingest_company(
            company["cik"],
            company["name"],
            company["sector"],
            stats
        )
        
        # Delay between companies
        if i < len(companies):
            time.sleep(DELAY_BETWEEN_COMPANIES)
        
        # Progress update every batch
        if i % BATCH_SIZE == 0:
            stats.print_progress(i)
            if i < len(companies):
                print(f"Waiting {DELAY_BETWEEN_BATCHES}s before next batch...")
                time.sleep(DELAY_BETWEEN_BATCHES)
    
    # Final summary
    stats.print_summary()
    
    print("="*80)
    print("To check the ingested data:")
    print("="*80)
    print("# Count distinct companies")
    print("SELECT COUNT(DISTINCT cik) FROM sec_10k;")
    print("SELECT COUNT(DISTINCT cik) FROM sec_income_statement;")
    print()
    print("# Check specific company")
    print("SELECT * FROM sec_10k WHERE company_name ILIKE '%apple%' LIMIT 5;")
    print()
    print("# View recent jobs")
    print("SELECT id, config->>'cik' as cik, status, rows_inserted, created_at")
    print("FROM ingestion_jobs WHERE source = 'sec' ORDER BY created_at DESC LIMIT 20;")
    print("="*80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nIngestion cancelled by user.\n")
    except Exception as e:
        print(f"\n\nFatal error: {e}\n")

