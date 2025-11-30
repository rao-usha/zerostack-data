"""
Script to ingest SEC data for major companies.

Ingests both filing metadata and XBRL financial data for key companies.

Usage:
    python ingest_sec_companies.py
"""
import asyncio
import logging
from datetime import date

from app.core.database import get_session_factory
from app.core.models import IngestionJob, JobStatus
from app.sources.sec import ingest, ingest_xbrl, metadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Companies to ingest
COMPANIES_TO_INGEST = {
    "tech": {
        "apple": "0000320193",
        "microsoft": "0000789019",
        "alphabet": "0001652044",  # Google
    },
    "finance": {
        "jpmorgan": "0000019617",
        "bank_of_america": "0000070858",
        "goldman_sachs": "0000886982",
    }
}


async def ingest_company_full(cik: str, name: str):
    """
    Ingest full SEC data for a company: filings + financial data.
    
    Args:
        cik: Company CIK
        name: Company name (for logging)
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Starting full ingestion for {name} (CIK: {cik})")
        logger.info(f"{'='*80}\n")
        
        # 1. Ingest filing metadata (10-K and 10-Q for last 5 years)
        logger.info(f"[{name}] Step 1: Ingesting filing metadata...")
        
        job_config_filings = {
            "source": "sec",
            "type": "filings",
            "cik": cik,
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
                start_date=date(2020, 1, 1),
                end_date=date.today()
            )
            
            logger.info(f"[{name}] Filing metadata ingestion completed:")
            logger.info(f"  - Company: {result_filings['company_name']}")
            logger.info(f"  - Ticker: {result_filings.get('ticker', 'N/A')}")
            logger.info(f"  - Filing types: {result_filings['filing_types']}")
            logger.info(f"  - Rows inserted: {result_filings['rows_inserted']}")
        
        except Exception as e:
            logger.error(f"[{name}] Filing metadata ingestion failed: {e}")
        
        # 2. Ingest XBRL financial data
        logger.info(f"\n[{name}] Step 2: Ingesting XBRL financial data...")
        
        job_config_xbrl = {
            "source": "sec",
            "type": "xbrl_financial_data",
            "cik": cik,
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
            
            logger.info(f"[{name}] XBRL financial data ingestion completed:")
            logger.info(f"  - Financial facts: {result_xbrl['financial_facts']}")
            logger.info(f"  - Income statements: {result_xbrl['income_statements']}")
            logger.info(f"  - Balance sheets: {result_xbrl['balance_sheets']}")
            logger.info(f"  - Cash flow statements: {result_xbrl['cash_flow_statements']}")
            logger.info(f"  - Total rows: {result_xbrl['total_rows']}")
        
        except Exception as e:
            logger.error(f"[{name}] XBRL ingestion failed: {e}")
        
        logger.info(f"\n[{name}] ✅ Full ingestion complete\n")
    
    except Exception as e:
        logger.error(f"[{name}] Fatal error during ingestion: {e}", exc_info=True)
    
    finally:
        db.close()


async def main():
    """
    Main function to ingest all companies.
    """
    logger.info("="*80)
    logger.info("SEC Data Ingestion Script")
    logger.info("="*80)
    logger.info("")
    
    all_companies = []
    for category, companies in COMPANIES_TO_INGEST.items():
        logger.info(f"{category.upper()}:")
        for name, cik in companies.items():
            logger.info(f"  - {name}: {cik}")
            all_companies.append((name, cik))
    
    logger.info(f"\nTotal companies to ingest: {len(all_companies)}")
    logger.info("")
    
    # Ingest each company sequentially (to respect rate limits)
    for i, (name, cik) in enumerate(all_companies, 1):
        logger.info(f"\n{'#'*80}")
        logger.info(f"Processing company {i}/{len(all_companies)}")
        logger.info(f"{'#'*80}")
        
        await ingest_company_full(cik, name)
        
        # Small delay between companies to be nice to SEC servers
        if i < len(all_companies):
            logger.info("Waiting 5 seconds before next company...")
            await asyncio.sleep(5)
    
    logger.info("\n" + "="*80)
    logger.info("✅ All companies ingested successfully!")
    logger.info("="*80)
    logger.info("")
    logger.info("To verify the data, run:")
    logger.info("")
    logger.info("  -- Check filing metadata")
    logger.info("  SELECT * FROM sec_10k ORDER BY filing_date DESC LIMIT 10;")
    logger.info("  SELECT * FROM sec_10q ORDER BY filing_date DESC LIMIT 10;")
    logger.info("")
    logger.info("  -- Check financial data")
    logger.info("  SELECT * FROM sec_income_statement ORDER BY period_end_date DESC LIMIT 10;")
    logger.info("  SELECT * FROM sec_balance_sheet ORDER BY period_end_date DESC LIMIT 10;")
    logger.info("  SELECT * FROM sec_cash_flow_statement ORDER BY period_end_date DESC LIMIT 10;")
    logger.info("")
    logger.info("  -- Count by company")
    logger.info("  SELECT company_name, COUNT(*) FROM sec_10k GROUP BY company_name;")
    logger.info("  SELECT company_name, COUNT(*) FROM sec_income_statement GROUP BY company_name;")
    logger.info("")


if __name__ == "__main__":
    asyncio.run(main())

