"""
SEC XBRL financial data ingestion.

Fetches and parses structured financial data from SEC Company Facts API.
"""
import logging
from typing import Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.sources.sec.client import SECClient
from app.sources.sec import xbrl_parser
from app.sources.sec.models import (
    SECFinancialFact,
    SECIncomeStatement,
    SECBalanceSheet,
    SECCashFlowStatement
)

logger = logging.getLogger(__name__)


async def ingest_company_financial_data(
    db: Session,
    job_id: int,
    cik: str
) -> Dict[str, Any]:
    """
    Ingest structured financial data from SEC XBRL.
    
    Fetches from /api/xbrl/companyfacts/CIK{cik}.json
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        cik: Company CIK
        
    Returns:
        Dictionary with ingestion results
    """
    client = SECClient()
    
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        logger.info(f"Fetching financial facts for CIK {cik}")
        
        # Fetch company facts from SEC
        facts_data = await client.get_company_facts(cik)
        
        # Parse financial data
        parsed_data = xbrl_parser.parse_company_facts(facts_data, cik)
        
        # Insert financial facts
        facts_inserted = 0
        if parsed_data["financial_facts"]:
            logger.info(f"Inserting {len(parsed_data['financial_facts'])} financial facts")
            
            for fact in parsed_data["financial_facts"]:
                fact_obj = SECFinancialFact(**fact)
                db.add(fact_obj)
                facts_inserted += 1
                
                # Commit in batches
                if facts_inserted % 100 == 0:
                    db.commit()
            
            db.commit()
            logger.info(f"Inserted {facts_inserted} financial facts")
        
        # Insert income statements
        income_inserted = 0
        if parsed_data["income_statement"]:
            logger.info(f"Inserting {len(parsed_data['income_statement'])} income statements")
            
            for income_stmt in parsed_data["income_statement"]:
                income_obj = SECIncomeStatement(**income_stmt)
                db.add(income_obj)
                income_inserted += 1
            
            db.commit()
            logger.info(f"Inserted {income_inserted} income statements")
        
        # Insert balance sheets
        balance_inserted = 0
        if parsed_data["balance_sheet"]:
            logger.info(f"Inserting {len(parsed_data['balance_sheet'])} balance sheets")
            
            for balance_sheet in parsed_data["balance_sheet"]:
                balance_obj = SECBalanceSheet(**balance_sheet)
                db.add(balance_obj)
                balance_inserted += 1
            
            db.commit()
            logger.info(f"Inserted {balance_inserted} balance sheets")
        
        # Insert cash flow statements
        cashflow_inserted = 0
        if parsed_data["cash_flow"]:
            logger.info(f"Inserting {len(parsed_data['cash_flow'])} cash flow statements")
            
            for cash_flow in parsed_data["cash_flow"]:
                cashflow_obj = SECCashFlowStatement(**cash_flow)
                db.add(cashflow_obj)
                cashflow_inserted += 1
            
            db.commit()
            logger.info(f"Inserted {cashflow_inserted} cash flow statements")
        
        total_rows = facts_inserted + income_inserted + balance_inserted + cashflow_inserted
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()
        
        return {
            "cik": cik,
            "financial_facts": facts_inserted,
            "income_statements": income_inserted,
            "balance_sheets": balance_inserted,
            "cash_flow_statements": cashflow_inserted,
            "total_rows": total_rows
        }
    
    except Exception as e:
        logger.error(f"XBRL ingestion failed for CIK {cik}: {e}", exc_info=True)
        
        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()

