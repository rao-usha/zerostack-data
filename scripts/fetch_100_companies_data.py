"""
Direct SEC data fetcher for 100 companies.
Downloads data from SEC EDGAR API and saves to JSON files.
Works independently of database configuration.
"""
import asyncio
import httpx
import json
import os
from datetime import datetime
from pathlib import Path
from sec_companies_200 import get_all_companies

# Create output directory
OUTPUT_DIR = Path("sec_data_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# SEC API configuration
SEC_BASE_URL = "https://data.sec.gov"
USER_AGENT = "Nexdata Research (contact@nexdata.com)"
MAX_REQUESTS_PER_SECOND = 8
DELAY_BETWEEN_REQUESTS = 1.0 / MAX_REQUESTS_PER_SECOND


class SECFetcher:
    """Direct SEC API fetcher."""
    
    def __init__(self):
        self.client = None
        self.companies_data = []
        self.successful = 0
        self.failed = 0
        self.errors = []
    
    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json"
            }
        )
        return self
    
    async def __aexit__(self, *args):
        if self.client:
            await self.client.aclose()
    
    async def fetch_company_data(self, cik: str, name: str, sector: str, index: int, total: int):
        """Fetch all data for a company."""
        print(f"[{index}/{total}] {name} (CIK: {cik}, Sector: {sector})")
        
        cik_padded = cik.zfill(10)
        company_data = {
            "cik": cik,
            "name": name,
            "sector": sector,
            "fetch_time": datetime.utcnow().isoformat(),
            "submissions": None,
            "company_facts": None,
            "error": None
        }
        
        try:
            # 1. Fetch company submissions (filing metadata)
            print(f"  → Fetching submissions...")
            submissions_url = f"{SEC_BASE_URL}/submissions/CIK{cik_padded}.json"
            
            response = await self.client.get(submissions_url)
            response.raise_for_status()
            company_data["submissions"] = response.json()
            print(f"  ✓ Got submissions data")
            
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
            
            # 2. Fetch company facts (XBRL financial data)
            print(f"  → Fetching financial facts...")
            facts_url = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik_padded}.json"
            
            response = await self.client.get(facts_url)
            response.raise_for_status()
            company_data["company_facts"] = response.json()
            print(f"  ✓ Got financial facts")
            
            # Save individual company file
            company_file = OUTPUT_DIR / f"{cik}_{name.replace(' ', '_').replace('.', '')}.json"
            with open(company_file, 'w') as f:
                json.dump(company_data, f, indent=2)
            
            print(f"  ✓ Saved to {company_file.name}")
            
            self.companies_data.append(company_data)
            self.successful += 1
            return True
            
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}"
            if e.response.status_code == 404:
                error_msg += " - Company not found"
            print(f"  ✗ {error_msg}")
            company_data["error"] = error_msg
            self.failed += 1
            self.errors.append({"company": name, "error": error_msg})
            return False
            
        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ Error: {error_msg}")
            company_data["error"] = error_msg
            self.failed += 1
            self.errors.append({"company": name, "error": error_msg})
            return False
    
    def save_summary(self):
        """Save summary of all fetched data."""
        summary = {
            "fetch_date": datetime.utcnow().isoformat(),
            "total_companies": self.successful + self.failed,
            "successful": self.successful,
            "failed": self.failed,
            "companies": [
                {
                    "cik": c["cik"],
                    "name": c["name"],
                    "sector": c["sector"],
                    "has_submissions": c["submissions"] is not None,
                    "has_facts": c["company_facts"] is not None,
                    "error": c["error"]
                }
                for c in self.companies_data
            ],
            "errors": self.errors
        }
        
        summary_file = OUTPUT_DIR / "fetch_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n✓ Summary saved to {summary_file}")
        return summary


async def main():
    """Main function."""
    companies = get_all_companies()[:100]  # First 100 companies
    
    print("\n" + "="*80)
    print("SEC DATA FETCHER - 100 COMPANIES")
    print("="*80)
    print(f"\nTotal companies: {len(companies)}")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    print(f"Estimated time: 25-35 minutes")
    print("\nThis will download:")
    print("  • Filing metadata (submissions)")
    print("  • Financial data (XBRL facts)")
    print("  • Save each company to a JSON file")
    print("\n" + "="*80 + "\n")
    
    start_time = datetime.now()
    
    async with SECFetcher() as fetcher:
        for i, company in enumerate(companies, 1):
            try:
                await fetcher.fetch_company_data(
                    company["cik"],
                    company["name"],
                    company["sector"],
                    i,
                    len(companies)
                )
                
                # Progress update every 10 companies
                if i % 10 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = i / (elapsed / 60)
                    eta = (len(companies) - i) / max(rate, 0.1)
                    
                    print(f"\n{'='*80}")
                    print(f"Progress: {i}/{len(companies)} ({i/len(companies)*100:.1f}%)")
                    print(f"Success: {fetcher.successful} | Failed: {fetcher.failed}")
                    print(f"Elapsed: {elapsed/60:.1f} min | Rate: {rate:.1f}/min | ETA: {eta:.1f} min")
                    print(f"{'='*80}\n")
                
                # Rate limiting delay
                if i < len(companies):
                    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
                    
            except KeyboardInterrupt:
                print("\n\nInterrupted by user. Saving progress...")
                break
        
        # Save summary
        summary = fetcher.save_summary()
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*80}")
    print("FETCH COMPLETE")
    print(f"{'='*80}")
    print(f"Total companies: {len(companies)}")
    print(f"Successful: {fetcher.successful}")
    print(f"Failed: {fetcher.failed}")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Files saved to: {OUTPUT_DIR.absolute()}")
    print(f"{'='*80}\n")
    
    if fetcher.errors:
        print("Errors (first 10):")
        for err in fetcher.errors[:10]:
            print(f"  • {err['company']}: {err['error']}")
        if len(fetcher.errors) > 10:
            print(f"  ... and {len(fetcher.errors) - 10} more")
        print()
    
    print("Next steps:")
    print("  1. Review the JSON files in sec_data_output/")
    print("  2. Check fetch_summary.json for overview")
    print("  3. To load into database, start Docker and run the API ingestion")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nStopped by user.\n")

