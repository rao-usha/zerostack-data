"""
Batch SEC Data Ingestion for 200+ Companies

Efficiently ingests filing metadata and financial data for all companies.
Includes progress tracking, error handling, and rate limit compliance.
"""
import asyncio
import logging
from datetime import date, datetime
import time

from app.core.database import get_session_factory
from app.core.models import IngestionJob, JobStatus
from app.sources.sec import ingest, ingest_xbrl, metadata
from sec_companies_200 import get_all_companies, COMPANIES_200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IngestionStats:
    """Track ingestion statistics."""
    
    def __init__(self):
        self.total_companies = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = None
        self.end_time = None
        self.errors = []
    
    def start(self):
        self.start_time = datetime.now()
    
    def finish(self):
        self.end_time = datetime.now()
    
    def add_success(self):
        self.successful += 1
    
    def add_failure(self, company_name, error):
        self.failed += 1
        self.errors.append({"company": company_name, "error": str(error)})
    
    def add_skip(self):
        self.skipped += 1
    
    def print_summary(self):
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        
        print("\n" + "="*80)
        print("INGESTION SUMMARY")
        print("="*80)
        print(f"Total companies:     {self.total_companies}")
        print(f"Successful:          {self.successful}")
        print(f"Failed:              {self.failed}")
        print(f"Skipped:             {self.skipped}")
        print(f"Duration:            {duration/60:.1f} minutes")
        print(f"Avg per company:     {duration/max(self.successful, 1):.1f} seconds")
        print("="*80)
        
        if self.errors:
            print("\nERRORS:")
            for err in self.errors[:10]:  # Show first 10 errors
                print(f"  - {err['company']}: {err['error'][:100]}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")
        print()


async def check_if_ingested(db, cik: str) -> bool:
    """Check if company data already exists."""
    # Check if we have any jobs for this CIK
    existing_job = db.query(IngestionJob).filter(
        IngestionJob.source == "sec",
        IngestionJob.config["cik"].astext == cik,
        IngestionJob.status == JobStatus.SUCCESS
    ).first()
    
    return existing_job is not None


async def ingest_company_full(cik: str, name: str, sector: str, stats: IngestionStats, skip_existing: bool = True):
    """
    Ingest full SEC data for a company.
    
    Args:
        cik: Company CIK
        name: Company name
        sector: Sector name
        stats: Statistics tracker
        skip_existing: Skip if already ingested
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        # Check if already ingested
        if skip_existing and await check_if_ingested(db, cik):
            logger.info(f"[{name}] Already ingested, skipping...")
            stats.add_skip()
            return
        
        logger.info(f"[{name}] Starting ingestion (CIK: {cik}, Sector: {sector})")
        
        # 1. Ingest filing metadata
        logger.info(f"[{name}] Ingesting filing metadata...")
        
        job_config_filings = {
            "source": "sec",
            "type": "filings",
            "cik": cik,
            "sector": sector,
            "filing_types": ["10-K", "10-Q"],
        }
        
        job_filings = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config_filings
        )
        db.add(job_filings)
        db.commit()
        db.refresh(job_filings)
        
        try:
            result_filings = await ingest.ingest_company_filings(
                db=db,
                job_id=job_filings.id,
                cik=cik,
                filing_types=["10-K", "10-Q"],
                start_date=date(2019, 1, 1),  # Last 5-6 years
                end_date=date.today()
            )
            logger.info(f"[{name}] ✓ Filing metadata: {result_filings['rows_inserted']} rows")
        except Exception as e:
            logger.error(f"[{name}] ✗ Filing metadata failed: {e}")
        
        # 2. Ingest XBRL financial data
        logger.info(f"[{name}] Ingesting XBRL financial data...")
        
        job_config_xbrl = {
            "source": "sec",
            "type": "xbrl_financial_data",
            "cik": cik,
            "sector": sector,
        }
        
        job_xbrl = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config_xbrl
        )
        db.add(job_xbrl)
        db.commit()
        db.refresh(job_xbrl)
        
        try:
            result_xbrl = await ingest_xbrl.ingest_company_financial_data(
                db=db,
                job_id=job_xbrl.id,
                cik=cik
            )
            logger.info(f"[{name}] ✓ Financial data: {result_xbrl['total_rows']} rows")
        except Exception as e:
            logger.error(f"[{name}] ✗ XBRL ingestion failed: {e}")
        
        stats.add_success()
        logger.info(f"[{name}] ✅ Complete\n")
    
    except Exception as e:
        logger.error(f"[{name}] ❌ Fatal error: {e}")
        stats.add_failure(name, e)
    
    finally:
        db.close()


async def ingest_batch(companies: list, batch_size: int = 10, delay: float = 2.0):
    """
    Ingest companies in batches to respect rate limits.
    
    Args:
        companies: List of company dicts (name, cik, sector)
        batch_size: Number of companies to process concurrently
        delay: Delay between batches in seconds
    """
    stats = IngestionStats()
    stats.total_companies = len(companies)
    stats.start()
    
    logger.info("="*80)
    logger.info(f"STARTING BATCH INGESTION: {len(companies)} COMPANIES")
    logger.info("="*80)
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Estimated time: {len(companies) * 20 / 60:.1f} minutes")
    logger.info("="*80 + "\n")
    
    # Process in batches
    for i in range(0, len(companies), batch_size):
        batch = companies[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(companies) + batch_size - 1) // batch_size
        
        logger.info(f"\n{'#'*80}")
        logger.info(f"BATCH {batch_num}/{total_batches} (Companies {i+1}-{min(i+batch_size, len(companies))})")
        logger.info(f"{'#'*80}\n")
        
        # Process batch concurrently
        tasks = []
        for company in batch:
            task = ingest_company_full(
                company["cik"],
                company["name"],
                company["sector"],
                stats,
                skip_existing=True
            )
            tasks.append(task)
        
        # Wait for batch to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Progress update
        progress = (i + len(batch)) / len(companies) * 100
        logger.info(f"\n{'='*80}")
        logger.info(f"PROGRESS: {progress:.1f}% ({i + len(batch)}/{len(companies)} companies)")
        logger.info(f"Success: {stats.successful} | Failed: {stats.failed} | Skipped: {stats.skipped}")
        logger.info(f"{'='*80}\n")
        
        # Delay between batches (except for last batch)
        if i + batch_size < len(companies):
            logger.info(f"Waiting {delay}s before next batch...\n")
            await asyncio.sleep(delay)
    
    stats.finish()
    stats.print_summary()
    
    return stats


async def main():
    """Main function."""
    print("\n" + "="*80)
    print("SEC DATA INGESTION - 200+ COMPANIES")
    print("="*80)
    
    # Get all companies
    companies = get_all_companies()
    
    print(f"\nTotal companies: {len(companies)}")
    print(f"Sectors: {len(COMPANIES_200)}\n")
    
    # Print sector breakdown
    sector_counts = {}
    for company in companies:
        sector = company["sector"]
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
    
    print("Companies by sector:")
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        print(f"  {sector:30s}: {count:3d}")
    
    print("\n" + "="*80)
    
    # User confirmation
    print("\nThis will ingest SEC data for all companies.")
    print("Estimated time: ~60-90 minutes")
    print("\nStarting in 5 seconds... (Ctrl+C to cancel)")
    
    try:
        await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("\n\nCancelled by user.")
        return
    
    # Run ingestion
    stats = await ingest_batch(
        companies,
        batch_size=5,  # Process 5 companies concurrently
        delay=2.0  # 2 second delay between batches
    )
    
    # Final summary
    print("\n" + "="*80)
    print("✅ INGESTION COMPLETE")
    print("="*80)
    print(f"\nSuccessfully ingested data for {stats.successful} companies!")
    
    if stats.failed > 0:
        print(f"\n⚠️  {stats.failed} companies failed (see errors above)")
    
    if stats.skipped > 0:
        print(f"\nℹ️  {stats.skipped} companies skipped (already ingested)")
    
    print("\n" + "="*80)
    print("\nTo verify the data:")
    print("  SELECT COUNT(DISTINCT cik) FROM sec_10k;")
    print("  SELECT COUNT(DISTINCT cik) FROM sec_income_statement;")
    print("  SELECT company_name, COUNT(*) FROM sec_10k GROUP BY company_name LIMIT 20;")
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nIngestion cancelled by user.\n")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

