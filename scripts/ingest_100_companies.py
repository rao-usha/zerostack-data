"""
Ingest SEC data for first 100 companies.
Direct database access for reliable execution.
"""
import asyncio
import logging
from datetime import date, datetime
from sec_companies_200 import get_all_companies

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress verbose SQLAlchemy logs
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


async def ingest_company(cik: str, name: str, sector: str, index: int, total: int):
    """Ingest a single company."""
    from app.core.database import get_session_factory
    from app.core.models import IngestionJob, JobStatus
    from app.sources.sec import ingest, ingest_xbrl
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        logger.info(f"[{index}/{total}] {name} (CIK: {cik}, Sector: {sector})")
        
        # Job 1: Filing metadata
        job_config_filings = {
            "source": "sec",
            "type": "filings",
            "cik": cik,
            "sector": sector,
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
            await ingest.ingest_company_filings(
                db=db,
                job_id=job_filings.id,
                cik=cik,
                filing_types=["10-K", "10-Q"],
                start_date=date(2019, 1, 1),
                end_date=date.today()
            )
            logger.info(f"  ✓ Filings ingested")
        except Exception as e:
            logger.error(f"  ✗ Filings failed: {e}")
        
        # Job 2: Financial data
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
            await ingest_xbrl.ingest_company_financial_data(
                db=db,
                job_id=job_xbrl.id,
                cik=cik
            )
            logger.info(f"  ✓ Financial data ingested")
        except Exception as e:
            logger.error(f"  ✗ Financial data failed: {e}")
        
        return True
    
    except Exception as e:
        logger.error(f"  ✗ Fatal error: {e}")
        return False
    
    finally:
        db.close()


async def main():
    """Main ingestion function."""
    companies = get_all_companies()[:100]  # First 100 companies
    
    print("\n" + "="*80)
    print("SEC DATA INGESTION - 100 COMPANIES")
    print("="*80)
    print(f"\nTotal companies: {len(companies)}")
    print(f"Estimated time: 30-45 minutes\n")
    print("="*80 + "\n")
    
    start_time = datetime.now()
    successful = 0
    failed = 0
    
    # Process sequentially to respect rate limits
    for i, company in enumerate(companies, 1):
        try:
            success = await ingest_company(
                company["cik"],
                company["name"],
                company["sector"],
                i,
                len(companies)
            )
            
            if success:
                successful += 1
            else:
                failed += 1
            
            # Progress update every 10 companies
            if i % 10 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = i / (elapsed / 60)
                eta = (len(companies) - i) / max(rate, 0.1)
                print(f"\n{'='*80}")
                print(f"Progress: {i}/{len(companies)} ({i/len(companies)*100:.1f}%)")
                print(f"Success: {successful} | Failed: {failed}")
                print(f"Elapsed: {elapsed/60:.1f} min | Rate: {rate:.1f}/min | ETA: {eta:.1f} min")
                print(f"{'='*80}\n")
            
            # Small delay between companies
            if i < len(companies):
                await asyncio.sleep(0.5)
        
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Stopping gracefully...")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            failed += 1
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*80}")
    print("INGESTION COMPLETE")
    print(f"{'='*80}")
    print(f"Total companies: {len(companies)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Average: {elapsed/max(successful, 1):.1f} seconds per company")
    print(f"{'='*80}\n")
    
    print("Verify the data:")
    print("  SELECT COUNT(DISTINCT cik) FROM sec_10k;")
    print("  SELECT COUNT(DISTINCT cik) FROM sec_income_statement;")
    print("  SELECT company_name, COUNT(*) FROM sec_10k GROUP BY company_name LIMIT 10;")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nStopped by user.\n")

