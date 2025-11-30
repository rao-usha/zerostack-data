"""
Pull SEC data for 100 major companies.
Downloads filing metadata and financial data from SEC EDGAR.
"""
import asyncio
import httpx
import json
from pathlib import Path
from datetime import datetime
from sec_companies_list import get_all_companies

# Configuration
OUTPUT_DIR = Path("sec_data_output")
OUTPUT_DIR.mkdir(exist_ok=True)

SEC_BASE_URL = "https://data.sec.gov"
USER_AGENT = "Nexdata Research (contact@nexdata.com)"
DELAY = 0.15  # 150ms between requests = ~6.7 req/sec (under SEC's 10/sec limit)

class DataFetcher:
    def __init__(self):
        self.client = None
        self.success = 0
        self.failed = 0
        self.errors = []
    
    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        return self
    
    async def __aexit__(self, *args):
        if self.client:
            await self.client.aclose()
    
    async def fetch_company(self, cik, name, sector, idx, total):
        """Fetch data for one company."""
        print(f"[{idx}/{total}] {name} (CIK: {cik})")
        
        cik_padded = cik.zfill(10)
        data = {
            "cik": cik,
            "name": name,
            "sector": sector,
            "fetch_time": datetime.utcnow().isoformat(),
            "submissions": None,
            "company_facts": None,
            "error": None
        }
        
        try:
            # Fetch submissions
            print(f"  → Fetching submissions...")
            url = f"{SEC_BASE_URL}/submissions/CIK{cik_padded}.json"
            resp = await self.client.get(url)
            resp.raise_for_status()
            data["submissions"] = resp.json()
            print(f"  ✓ Submissions OK")
            
            await asyncio.sleep(DELAY)
            
            # Fetch financial facts
            print(f"  → Fetching financial data...")
            url = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{cik_padded}.json"
            resp = await self.client.get(url)
            resp.raise_for_status()
            data["company_facts"] = resp.json()
            print(f"  ✓ Financial data OK")
            
            # Save file
            filename = f"{cik}_{name.replace(' ', '_').replace('.', '').replace('&', 'and')}.json"
            filepath = OUTPUT_DIR / filename
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"  ✓ Saved: {filename}")
            self.success += 1
            return True
            
        except httpx.HTTPStatusError as e:
            error = f"HTTP {e.response.status_code}"
            if e.response.status_code == 404:
                error += " - No data available"
            print(f"  ✗ {error}")
            data["error"] = error
            self.failed += 1
            self.errors.append({"name": name, "cik": cik, "error": error})
            return False
            
        except Exception as e:
            error = str(e)[:100]
            print(f"  ✗ Error: {error}")
            data["error"] = error
            self.failed += 1
            self.errors.append({"name": name, "cik": cik, "error": error})
            return False

async def main():
    companies = get_all_companies()
    
    print("\n" + "="*80)
    print(f"SEC DATA PULL - {len(companies)} COMPANIES")
    print("="*80)
    print(f"\nOutput: {OUTPUT_DIR.absolute()}")
    print(f"Estimated time: {len(companies) * 0.4:.0f} minutes\n")
    print("="*80 + "\n")
    
    start = datetime.now()
    
    async with DataFetcher() as fetcher:
        for i, co in enumerate(companies, 1):
            await fetcher.fetch_company(
                co["cik"], co["name"], co["sector"], i, len(companies)
            )
            
            # Progress every 10
            if i % 10 == 0:
                elapsed = (datetime.now() - start).total_seconds() / 60
                print(f"\n{'='*80}")
                print(f"Progress: {i}/{len(companies)} ({i/len(companies)*100:.1f}%)")
                print(f"Success: {fetcher.success} | Failed: {fetcher.failed}")
                print(f"Time: {elapsed:.1f} min | Remaining: {(len(companies)-i)*0.4:.0f} min")
                print(f"{'='*80}\n")
            
            if i < len(companies):
                await asyncio.sleep(DELAY)
        
        # Save summary
        summary = {
            "total": len(companies),
            "successful": fetcher.success,
            "failed": fetcher.failed,
            "errors": fetcher.errors,
            "fetch_date": datetime.utcnow().isoformat()
        }
        with open(OUTPUT_DIR / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
    
    elapsed = (datetime.now() - start).total_seconds() / 60
    total_files = len(list(OUTPUT_DIR.glob("*.json"))) - 1
    total_mb = sum(f.stat().st_size for f in OUTPUT_DIR.glob("*.json")) / (1024*1024)
    
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print(f"{'='*80}")
    print(f"Companies: {len(companies)}")
    print(f"Successful: {fetcher.success}")
    print(f"Failed: {fetcher.failed}")
    print(f"Files: {total_files}")
    print(f"Size: {total_mb:.1f} MB")
    print(f"Time: {elapsed:.1f} minutes")
    print(f"Location: {OUTPUT_DIR.absolute()}")
    print(f"{'='*80}\n")
    
    if fetcher.errors:
        print("Failed companies:")
        for e in fetcher.errors[:10]:
            print(f"  • {e['name']}: {e['error']}")
        print()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nStopped by user.\n")

