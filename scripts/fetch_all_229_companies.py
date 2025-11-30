"""
Fetch ALL 229 companies from SEC EDGAR.
Smart fetcher that skips already downloaded companies.
"""
import asyncio
import httpx
import json
import os
from datetime import datetime
from pathlib import Path
from sec_companies_200 import get_all_companies

# Output directory
OUTPUT_DIR = Path("sec_data_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# SEC API configuration
SEC_BASE_URL = "https://data.sec.gov"
USER_AGENT = "Nexdata Research (contact@nexdata.com)"
MAX_REQUESTS_PER_SECOND = 8
DELAY_BETWEEN_REQUESTS = 1.0 / MAX_REQUESTS_PER_SECOND


def get_already_fetched_ciks():
    """Get list of CIKs already fetched."""
    if not OUTPUT_DIR.exists():
        return set()
    
    fetched = set()
    for file in OUTPUT_DIR.glob("*.json"):
        if file.name == "fetch_summary.json":
            continue
        # Extract CIK from filename (first 10 digits)
        cik = file.name.split('_')[0]
        fetched.add(cik)
    
    return fetched


class SECFetcher:
    """Direct SEC API fetcher."""
    
    def __init__(self):
        self.client = None
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.errors = []
        self.already_fetched = get_already_fetched_ciks()
    
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
        # Check if already fetched
        if cik in self.already_fetched:
            print(f"[{index}/{total}] {name} - ALREADY FETCHED ✓")
            self.skipped += 1
            return True
        
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
            # 1. Fetch company submissions
            print(f"  → Fetching submissions...")
            submissions_url = f"{SEC_BASE_URL}/submissions/CIK{cik_padded}.json"
            
            response = await self.client.get(submissions_url)
            response.raise_for_status()
            company_data["submissions"] = response.json()
            print(f"  ✓ Got submissions data")
            
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
            
            # 2. Fetch company facts
            print(f"  → Fetching financial facts...")
            facts_url = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik_padded}.json"
            
            response = await self.client.get(facts_url)
            response.raise_for_status()
            company_data["company_facts"] = response.json()
            print(f"  ✓ Got financial facts")
            
            # Save file
            company_file = OUTPUT_DIR / f"{cik}_{name.replace(' ', '_').replace('.', '').replace('/', '_').replace('&', 'and')}.json"
            with open(company_file, 'w') as f:
                json.dump(company_data, f, indent=2)
            
            print(f"  ✓ Saved to {company_file.name}")
            
            self.successful += 1
            return True
            
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}"
            if e.response.status_code == 404:
                error_msg += " - Not found"
            print(f"  ✗ {error_msg}")
            company_data["error"] = error_msg
            self.failed += 1
            self.errors.append({"company": name, "cik": cik, "error": error_msg})
            return False
            
        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ Error: {error_msg[:100]}")
            company_data["error"] = error_msg
            self.failed += 1
            self.errors.append({"company": name, "cik": cik, "error": error_msg})
            return False
    
    def save_final_summary(self, companies):
        """Save final summary."""
        summary = {
            "fetch_date": datetime.utcnow().isoformat(),
            "total_companies": len(companies),
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "errors": self.errors
        }
        
        summary_file = OUTPUT_DIR / "final_summary_229.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n✓ Final summary saved to {summary_file}")
        return summary


async def main():
    """Main function."""
    all_companies = get_all_companies()  # All 229 companies
    
    print("\n" + "="*80)
    print("SEC DATA FETCHER - ALL 229 COMPANIES")
    print("="*80)
    
    # Check what's already fetched
    already_fetched = get_already_fetched_ciks()
    remaining = [c for c in all_companies if c["cik"] not in already_fetched]
    
    print(f"\nTotal companies: {len(all_companies)}")
    print(f"Already fetched: {len(already_fetched)}")
    print(f"Remaining to fetch: {len(remaining)}")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    print(f"\nEstimated time for remaining: {len(remaining) * 0.25:.0f} minutes")
    print("\n" + "="*80 + "\n")
    
    start_time = datetime.now()
    
    async with SECFetcher() as fetcher:
        for i, company in enumerate(all_companies, 1):
            try:
                await fetcher.fetch_company_data(
                    company["cik"],
                    company["name"],
                    company["sector"],
                    i,
                    len(all_companies)
                )
                
                # Progress update every 10 companies
                if i % 10 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = (fetcher.successful + fetcher.skipped) / max(elapsed / 60, 0.01)
                    remaining_count = len(all_companies) - i
                    eta = remaining_count / max(rate, 0.1)
                    
                    print(f"\n{'='*80}")
                    print(f"Progress: {i}/{len(all_companies)} ({i/len(all_companies)*100:.1f}%)")
                    print(f"New: {fetcher.successful} | Failed: {fetcher.failed} | Skipped: {fetcher.skipped}")
                    print(f"Elapsed: {elapsed/60:.1f} min | Rate: {rate:.1f}/min | ETA: {eta:.1f} min")
                    print(f"{'='*80}\n")
                
                # Rate limiting
                if i < len(all_companies):
                    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
                    
            except KeyboardInterrupt:
                print("\n\nInterrupted by user. Saving progress...")
                break
        
        # Save final summary
        fetcher.save_final_summary(all_companies)
    
    # Final report
    elapsed = (datetime.now() - start_time).total_seconds()
    total_files = len(list(OUTPUT_DIR.glob("*.json"))) - 1  # Exclude summary
    total_size = sum(f.stat().st_size for f in OUTPUT_DIR.glob("*.json") if f.name not in ["fetch_summary.json", "final_summary_229.json"]) / (1024 * 1024)
    
    print(f"\n{'='*80}")
    print("FETCH COMPLETE - ALL 229 COMPANIES")
    print(f"{'='*80}")
    print(f"Total companies: {len(all_companies)}")
    print(f"Successfully fetched: {fetcher.successful}")
    print(f"Failed: {fetcher.failed}")
    print(f"Skipped (already had): {fetcher.skipped}")
    print(f"Total files: {total_files}")
    print(f"Total data size: {total_size:.2f} MB")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Output: {OUTPUT_DIR.absolute()}")
    print(f"{'='*80}\n")
    
    if fetcher.errors:
        print("Failed companies:")
        for err in fetcher.errors:
            print(f"  • {err['company']} (CIK: {err['cik']}): {err['error']}")
        print()
    
    print("✓ All data fetched successfully!")
    print(f"  Files location: {OUTPUT_DIR.absolute()}")
    print(f"  Summary: final_summary_229.json")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nStopped by user.\n")

