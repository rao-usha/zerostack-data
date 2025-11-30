"""
Populate sec_income_statement table from financial facts.
Maps XBRL metrics to normalized income statement line items.
"""
import sys
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set DATABASE_URL
os.environ['DATABASE_URL'] = 'postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata'

sys.path.insert(0, str(Path(__file__).parent))

from app.core.config import get_settings

# XBRL to Income Statement field mapping
INCOME_STATEMENT_MAPPING = {
    # Revenue
    "revenues": [
        "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet", "RevenueFromContractWithCustomer"
    ],
    "cost_of_revenue": [
        "CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfSales"
    ],
    "gross_profit": [
        "GrossProfit"
    ],
    
    # Operating expenses
    "operating_expenses": [
        "OperatingExpenses", "OperatingCostsAndExpenses"
    ],
    "research_and_development": [
        "ResearchAndDevelopmentExpense", "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"
    ],
    "selling_general_administrative": [
        "SellingGeneralAndAdministrativeExpense", "SellingAndMarketingExpense"
    ],
    
    # Operating income
    "operating_income": [
        "OperatingIncomeLoss", "OperatingIncome"
    ],
    
    # Interest and other
    "interest_expense": [
        "InterestExpense", "InterestExpenseDebt"
    ],
    "interest_income": [
        "InterestIncome", "InterestIncomeOther", "InvestmentIncomeInterest"
    ],
    "other_income_expense": [
        "OtherNonoperatingIncomeExpense", "NonoperatingIncomeExpense"
    ],
    
    # Pre-tax and tax
    "income_before_tax": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"
    ],
    "income_tax_expense": [
        "IncomeTaxExpenseBenefit", "IncomeTaxesPaid"
    ],
    
    # Net income
    "net_income": [
        "NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic",
        "ProfitLoss", "NetIncome"
    ],
    
    # EPS
    "earnings_per_share_basic": [
        "EarningsPerShareBasic"
    ],
    "earnings_per_share_diluted": [
        "EarningsPerShareDiluted"
    ],
    "weighted_average_shares_basic": [
        "WeightedAverageNumberOfSharesOutstandingBasic"
    ],
    "weighted_average_shares_diluted": [
        "WeightedAverageNumberOfDilutedSharesOutstanding"
    ],
}

def get_fact_value(facts_by_metric, metric_names):
    """Get the first available fact value from a list of possible metric names."""
    for name in metric_names:
        if name in facts_by_metric and facts_by_metric[name]:
            return facts_by_metric[name]
    return None

def build_income_statements(engine):
    """Build income statements from financial facts."""
    
    print("\n" + "="*80)
    print("POPULATING INCOME STATEMENTS")
    print("="*80)
    
    # Get distinct companies and periods from financial facts
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT 
                cik, 
                company_name,
                fiscal_year, 
                fiscal_period,
                period_end_date,
                period_start_date,
                form_type,
                accession_number,
                filing_date
            FROM sec_financial_facts
            WHERE fiscal_year IS NOT NULL 
                AND fiscal_period IS NOT NULL
                AND period_end_date IS NOT NULL
            ORDER BY cik, fiscal_year DESC, fiscal_period
        """))
        
        periods = result.fetchall()
    
    print(f"\nFound {len(periods)} distinct reporting periods")
    print("Building income statements...\n")
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    income_statements = []
    processed = 0
    
    for period in periods:
        cik = period[0]
        company_name = period[1]
        fiscal_year = period[2]
        fiscal_period = period[3]
        period_end = period[4]
        period_start = period[5]
        form_type = period[6]
        accession = period[7]
        filing_date = period[8]
        
        # Get all facts for this period
        with engine.connect() as conn:
            facts_result = conn.execute(text("""
                SELECT fact_name, value
                FROM sec_financial_facts
                WHERE cik = :cik
                    AND fiscal_year = :fy
                    AND fiscal_period = :fp
                    AND period_end_date = :end_date
                    AND value IS NOT NULL
            """), {
                "cik": cik,
                "fy": fiscal_year,
                "fp": fiscal_period,
                "end_date": period_end
            })
            
            facts_list = facts_result.fetchall()
        
        if not facts_list:
            continue
        
        # Build dictionary of facts by metric name
        facts_by_metric = {}
        for fact_name, value in facts_list:
            facts_by_metric[fact_name] = value
        
        # Build income statement record
        income_stmt = {
            "cik": cik,
            "company_name": company_name,
            "ticker": None,  # Could extract from facts if available
            "period_end_date": period_end,
            "period_start_date": period_start,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "accession_number": accession,
            "form_type": form_type,
            "filing_date": filing_date,
        }
        
        # Map XBRL facts to income statement fields
        for field, metric_names in INCOME_STATEMENT_MAPPING.items():
            income_stmt[field] = get_fact_value(facts_by_metric, metric_names)
        
        # Only add if we have at least revenue or net income
        if income_stmt["revenues"] or income_stmt["net_income"]:
            income_statements.append(income_stmt)
            processed += 1
            
            if processed % 100 == 0:
                print(f"  • Processed {processed} income statements...", end="\r")
    
    print(f"\n\nBuilt {len(income_statements)} income statements")
    print("Inserting into database...\n")
    
    # Insert into database
    if income_statements:
        insert_sql = """
            INSERT INTO sec_income_statement (
                cik, company_name, ticker,
                period_end_date, period_start_date,
                fiscal_year, fiscal_period,
                accession_number, form_type, filing_date,
                revenues, cost_of_revenue, gross_profit,
                operating_expenses, research_and_development, selling_general_administrative,
                operating_income,
                interest_expense, interest_income, other_income_expense,
                income_before_tax, income_tax_expense,
                net_income,
                earnings_per_share_basic, earnings_per_share_diluted,
                weighted_average_shares_basic, weighted_average_shares_diluted
            ) VALUES (
                :cik, :company_name, :ticker,
                :period_end_date, :period_start_date,
                :fiscal_year, :fiscal_period,
                :accession_number, :form_type, :filing_date,
                :revenues, :cost_of_revenue, :gross_profit,
                :operating_expenses, :research_and_development, :selling_general_administrative,
                :operating_income,
                :interest_expense, :interest_income, :other_income_expense,
                :income_before_tax, :income_tax_expense,
                :net_income,
                :earnings_per_share_basic, :earnings_per_share_diluted,
                :weighted_average_shares_basic, :weighted_average_shares_diluted
            )
            ON CONFLICT DO NOTHING
        """
        
        batch_size = 500
        for i in range(0, len(income_statements), batch_size):
            batch = income_statements[i:i+batch_size]
            session.execute(text(insert_sql), batch)
            session.commit()
            print(f"  • Inserted {min(i+batch_size, len(income_statements))}/{len(income_statements)}", end="\r")
        
        print(f"\n\n[OK] Inserted {len(income_statements)} income statements")
    
    session.close()
    
    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM sec_income_statement"))
        count = result.fetchone()[0]
    
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print(f"{'='*80}")
    print(f"Total income statements in database: {count:,}")
    print(f"{'='*80}\n")
    
    print("Sample queries:")
    print("  -- View Apple's quarterly revenue")
    print("  SELECT fiscal_year, fiscal_period, revenues FROM sec_income_statement")
    print("  WHERE company_name LIKE '%APPLE%' ORDER BY fiscal_year DESC, fiscal_period;")
    print()
    print("  -- Compare net income across companies")
    print("  SELECT company_name, fiscal_year, fiscal_period, net_income")
    print("  FROM sec_income_statement WHERE fiscal_year = 2024 ORDER BY net_income DESC LIMIT 10;")
    print()

def main():
    engine = create_engine('postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata')
    print("\n[OK] Connected to database")
    
    build_income_statements(engine)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nStopped by user.\n")
    except Exception as e:
        print(f"\n\nError: {e}\n")
        import traceback
        traceback.print_exc()

