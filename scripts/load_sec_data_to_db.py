"""
Load downloaded SEC data into PostgreSQL database.
Parses JSON files and populates all SEC tables.
"""
import json
import sys
import os
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set DATABASE_URL before importing app modules
os.environ['DATABASE_URL'] = 'postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata'

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.core.config import get_settings
from app.core.models import Base
from app.sources.sec.models import (
    SECFinancialFact,
    SECIncomeStatement, 
    SECBalanceSheet,
    SECCashFlowStatement
)
from app.sources.sec import xbrl_parser, metadata as sec_metadata

OUTPUT_DIR = Path("sec_data_output")

def get_db_engine():
    """Get database engine."""
    settings = get_settings()
    return create_engine(settings.database_url)

def create_filing_tables(engine):
    """Create filing tables (10-K, 10-Q, etc.) if they don't exist."""
    filing_types = ["10-K", "10-Q", "8-K"]
    
    with engine.connect() as conn:
        for filing_type in filing_types:
            table_name = sec_metadata.generate_table_name(filing_type)
            create_sql = sec_metadata.generate_create_table_sql(table_name)
            try:
                conn.execute(text(create_sql))
                conn.commit()
                print(f"✓ Created table: {table_name}")
            except Exception as e:
                print(f"  Table {table_name} already exists or error: {e}")

def load_company_data(json_file, engine, Session):
    """Load data from one JSON file into database."""
    company_name = json_file.stem.split('_', 1)[1]
    print(f"\n{'='*80}")
    print(f"Loading: {company_name}")
    print(f"{'='*80}")
    
    with open(json_file) as f:
        data = json.load(f)
    
    cik = data.get("cik")
    sector = data.get("sector", "Unknown")
    
    stats = {
        "filings": 0,
        "facts": 0,
        "income_stmts": 0,
        "balance_sheets": 0,
        "cash_flows": 0
    }
    
    session = Session()
    
    try:
        # 1. Load filing metadata (submissions)
        submissions = data.get("submissions")
        if submissions:
            print(f"  → Loading filings...")
            filings = sec_metadata.parse_filings(submissions, filing_types=["10-K", "10-Q"])
            
            if filings:
                # Group by type
                by_type = {}
                for filing in filings:
                    ftype = filing["filing_type"]
                    if ftype not in by_type:
                        by_type[ftype] = []
                    by_type[ftype].append(filing)
                
                # Insert each type
                for ftype, filing_list in by_type.items():
                    table_name = sec_metadata.generate_table_name(ftype)
                    
                    insert_sql = f"""
                        INSERT INTO {table_name} 
                        (cik, ticker, company_name, accession_number, filing_type, 
                         filing_date, report_date, primary_document, filing_url, 
                         interactive_data_url, file_number, film_number, items)
                        VALUES 
                        (:cik, :ticker, :company_name, :accession_number, :filing_type,
                         :filing_date, :report_date, :primary_document, :filing_url,
                         :interactive_data_url, :file_number, :film_number, :items)
                        ON CONFLICT (accession_number) DO NOTHING
                    """
                    
                    session.execute(text(insert_sql), filing_list)
                    stats["filings"] += len(filing_list)
                
                session.commit()
                print(f"  ✓ Loaded {stats['filings']} filings")
        
        # 2. Load XBRL financial data
        company_facts = data.get("company_facts")
        if company_facts:
            print(f"  → Parsing XBRL financial data...")
            parsed = xbrl_parser.parse_company_facts(company_facts, cik)
            
            # Load financial facts
            if parsed["financial_facts"]:
                print(f"  → Loading {len(parsed['financial_facts'])} financial facts...")
                for fact in parsed["financial_facts"][:5000]:  # Limit to avoid memory issues
                    fact_obj = SECFinancialFact(**fact)
                    session.add(fact_obj)
                    stats["facts"] += 1
                    
                    if stats["facts"] % 500 == 0:
                        session.commit()
                
                session.commit()
                print(f"  ✓ Loaded {stats['facts']} financial facts")
            
            # Load income statements
            if parsed["income_statement"]:
                print(f"  → Loading {len(parsed['income_statement'])} income statements...")
                for stmt in parsed["income_statement"]:
                    stmt_obj = SECIncomeStatement(**stmt)
                    session.add(stmt_obj)
                    stats["income_stmts"] += 1
                
                session.commit()
                print(f"  ✓ Loaded {stats['income_stmts']} income statements")
            
            # Load balance sheets
            if parsed["balance_sheet"]:
                print(f"  → Loading {len(parsed['balance_sheet'])} balance sheets...")
                for sheet in parsed["balance_sheet"]:
                    sheet_obj = SECBalanceSheet(**sheet)
                    session.add(sheet_obj)
                    stats["balance_sheets"] += 1
                
                session.commit()
                print(f"  ✓ Loaded {stats['balance_sheets']} balance sheets")
            
            # Load cash flow statements
            if parsed["cash_flow"]:
                print(f"  → Loading {len(parsed['cash_flow'])} cash flow statements...")
                for cf in parsed["cash_flow"]:
                    cf_obj = SECCashFlowStatement(**cf)
                    session.add(cf_obj)
                    stats["cash_flows"] += 1
                
                session.commit()
                print(f"  ✓ Loaded {stats['cash_flows']} cash flow statements")
        
        return stats
        
    except Exception as e:
        print(f"  ✗ Error loading {company_name}: {e}")
        session.rollback()
        import traceback
        traceback.print_exc()
        return stats
        
    finally:
        session.close()

def main():
    """Main function."""
    print("\n" + "="*80)
    print("LOAD SEC DATA INTO DATABASE")
    print("="*80)
    
    # Get JSON files
    if not OUTPUT_DIR.exists():
        print(f"\nError: {OUTPUT_DIR} does not exist!")
        print("Run pull_sec_data.py first to download the data.\n")
        return
    
    json_files = [f for f in OUTPUT_DIR.glob("*.json") if f.name != "summary.json"]
    
    print(f"\nFound {len(json_files)} company files to load")
    print(f"This will take approximately {len(json_files) * 0.5:.0f} minutes\n")
    print("="*80)
    
    # Setup database
    engine = get_db_engine()
    print("\n✓ Connected to database")
    
    # Create tables
    print("\nCreating tables...")
    Base.metadata.create_all(engine)
    create_filing_tables(engine)
    
    # Create session factory
    Session = sessionmaker(bind=engine)
    
    # Load each company
    start_time = datetime.now()
    total_stats = {
        "filings": 0,
        "facts": 0,
        "income_stmts": 0,
        "balance_sheets": 0,
        "cash_flows": 0
    }
    
    for i, json_file in enumerate(json_files, 1):
        print(f"\n[{i}/{len(json_files)}]")
        stats = load_company_data(json_file, engine, Session)
        
        for key in total_stats:
            total_stats[key] += stats[key]
        
        if i % 10 == 0:
            elapsed = (datetime.now() - start_time).total_seconds() / 60
            print(f"\n{'='*80}")
            print(f"Progress: {i}/{len(json_files)} ({i/len(json_files)*100:.1f}%)")
            print(f"Time elapsed: {elapsed:.1f} minutes")
            print(f"Estimated remaining: {(len(json_files)-i) * 0.5:.0f} minutes")
            print(f"{'='*80}")
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds() / 60
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print(f"{'='*80}")
    print(f"Companies loaded: {len(json_files)}")
    print(f"Total filings: {total_stats['filings']}")
    print(f"Total financial facts: {total_stats['facts']}")
    print(f"Total income statements: {total_stats['income_stmts']}")
    print(f"Total balance sheets: {total_stats['balance_sheets']}")
    print(f"Total cash flow statements: {total_stats['cash_flows']}")
    print(f"Time taken: {elapsed:.1f} minutes")
    print(f"{'='*80}\n")
    
    print("Verify with:")
    print("  SELECT COUNT(*) FROM sec_10k;")
    print("  SELECT COUNT(*) FROM sec_financial_facts;")
    print("  SELECT COUNT(*) FROM sec_income_statement;")
    print("  SELECT company_name, COUNT(*) FROM sec_10k GROUP BY company_name LIMIT 10;")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nStopped by user.\n")
    except Exception as e:
        print(f"\n\nError: {e}\n")
        import traceback
        traceback.print_exc()

