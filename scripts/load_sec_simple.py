"""
Simplified SEC data loader - loads filings and financial facts only.
Skips complex statement parsing.
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
from app.sources.sec.models import SECFinancialFact
from app.sources.sec import metadata as sec_metadata

OUTPUT_DIR = Path("sec_data_output")

def get_db_engine():
    """Get database engine."""
    settings = get_settings()
    return create_engine(settings.database_url)

def load_company_data(json_file, session):
    """Load filings and financial facts from one JSON file."""
    company_name = json_file.stem.split('_', 1)[1]
    print(f"\n{'='*80}")
    print(f"Loading: {company_name}")
    print(f"{'='*80}")
    
    with open(json_file) as f:
        data = json.load(f)
    
    cik = data.get("cik")
    sector = data.get("sector", "Unknown")
    
    stats = {"filings": 0, "facts": 0}
    
    try:
        # 1. Load filing metadata
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
        
        # 2. Load financial facts (raw XBRL data)
        company_facts = data.get("company_facts")
        if company_facts and "facts" in company_facts:
            print(f"  → Loading financial facts...")
            
            # Parse raw XBRL facts
            facts_data = company_facts["facts"]
            company_name_from_data = company_facts.get("entityName", company_name)
            
            fact_count = 0
            batch_size = 500
            batch = []
            
            for namespace, metrics in facts_data.items():
                for metric_name, metric_data in metrics.items():
                    if "units" not in metric_data:
                        continue
                    
                    for unit, values_list in metric_data["units"].items():
                        for value_item in values_list:
                            # Extract fact data
                            fact = {
                                "cik": cik,
                                "company_name": company_name_from_data,
                                "fact_name": metric_name,
                                "fact_label": metric_data.get("label", metric_name),
                                "namespace": namespace,
                                "value": value_item.get("val"),
                                "unit": unit,
                                "period_end_date": value_item.get("end"),
                                "period_start_date": value_item.get("start"),
                                "fiscal_year": value_item.get("fy"),
                                "fiscal_period": value_item.get("fp"),
                                "form_type": value_item.get("form"),
                                "accession_number": value_item.get("accn"),
                                "filing_date": value_item.get("filed"),
                                "frame": value_item.get("frame")
                            }
                            
                            batch.append(SECFinancialFact(**fact))
                            fact_count += 1
                            
                            # Batch insert
                            if len(batch) >= batch_size:
                                session.bulk_save_objects(batch)
                                session.commit()
                                batch = []
                                print(f"    • Loaded {fact_count} facts...", end="\r")
            
            # Insert remaining
            if batch:
                session.bulk_save_objects(batch)
                session.commit()
            
            stats["facts"] = fact_count
            print(f"  ✓ Loaded {stats['facts']} financial facts")
        
        return stats
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        session.rollback()
        import traceback
        traceback.print_exc()
        return stats

def main():
    """Main function."""
    print("\n" + "="*80)
    print("LOAD SEC DATA INTO DATABASE (SIMPLIFIED)")
    print("="*80)
    
    # Get JSON files
    if not OUTPUT_DIR.exists():
        print(f"\nError: {OUTPUT_DIR} does not exist!")
        return
    
    json_files = [f for f in OUTPUT_DIR.glob("*.json") if f.name != "summary.json"]
    
    print(f"\nFound {len(json_files)} company files to load")
    print(f"This will take approximately {len(json_files) * 0.3:.0f} minutes\n")
    print("="*80)
    
    # Setup database
    engine = get_db_engine()
    print("\n✓ Connected to database")
    
    # Create tables
    print("\nEnsuring tables exist...")
    Base.metadata.create_all(engine)
    
    # Create session factory
    Session = sessionmaker(bind=engine)
    
    # Load each company
    start_time = datetime.now()
    total_stats = {"filings": 0, "facts": 0}
    
    for i, json_file in enumerate(json_files, 1):
        print(f"\n[{i}/{len(json_files)}]")
        session = Session()
        stats = load_company_data(json_file, session)
        session.close()
        
        for key in total_stats:
            total_stats[key] += stats[key]
        
        if i % 10 == 0:
            elapsed = (datetime.now() - start_time).total_seconds() / 60
            print(f"\n{'='*80}")
            print(f"Progress: {i}/{len(json_files)} ({i/len(json_files)*100:.1f}%)")
            print(f"Time elapsed: {elapsed:.1f} minutes")
            print(f"Estimated remaining: {(len(json_files)-i) * 0.3:.0f} minutes")
            print(f"Total filings: {total_stats['filings']}, Total facts: {total_stats['facts']}")
            print(f"{'='*80}")
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds() / 60
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print(f"{'='*80}")
    print(f"Companies loaded: {len(json_files)}")
    print(f"Total filings: {total_stats['filings']:,}")
    print(f"Total financial facts: {total_stats['facts']:,}")
    print(f"Time taken: {elapsed:.1f} minutes")
    print(f"{'='*80}\n")
    
    print("Query examples:")
    print("  SELECT COUNT(*) FROM sec_10k;")
    print("  SELECT COUNT(*) FROM sec_financial_facts;")
    print("  SELECT cik, company_name, COUNT(*) FROM sec_financial_facts GROUP BY cik, company_name LIMIT 10;")
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

