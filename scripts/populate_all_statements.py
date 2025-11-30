"""
Populate all SEC financial statement tables from financial facts.
Maps XBRL metrics to normalized statement line items.
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

# XBRL Mappings
INCOME_STATEMENT_MAPPING = {
    "revenues": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "gross_profit": ["GrossProfit"],
    "operating_expenses": ["OperatingExpenses"],
    "research_and_development": ["ResearchAndDevelopmentExpense"],
    "selling_general_administrative": ["SellingGeneralAndAdministrativeExpense"],
    "operating_income": ["OperatingIncomeLoss"],
    "interest_expense": ["InterestExpense"],
    "interest_income": ["InterestIncome"],
    "other_income_expense": ["OtherNonoperatingIncomeExpense"],
    "income_before_tax": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxes"],
    "income_tax_expense": ["IncomeTaxExpenseBenefit"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "earnings_per_share_basic": ["EarningsPerShareBasic"],
    "earnings_per_share_diluted": ["EarningsPerShareDiluted"],
    "weighted_average_shares_basic": ["WeightedAverageNumberOfSharesOutstandingBasic"],
    "weighted_average_shares_diluted": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
}

BALANCE_SHEET_MAPPING = {
    "cash_and_equivalents": ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
    "short_term_investments": ["ShortTermInvestments", "AvailableForSaleSecuritiesCurrent"],
    "accounts_receivable": ["AccountsReceivableNetCurrent"],
    "inventory": ["InventoryNet"],
    "current_assets": ["AssetsCurrent"],
    "property_plant_equipment": ["PropertyPlantAndEquipmentNet"],
    "goodwill": ["Goodwill"],
    "intangible_assets": ["IntangibleAssetsNetExcludingGoodwill"],
    "long_term_investments": ["LongTermInvestments"],
    "other_long_term_assets": ["OtherAssetsNoncurrent"],
    "total_assets": ["Assets"],
    "accounts_payable": ["AccountsPayableCurrent"],
    "short_term_debt": ["ShortTermBorrowings", "DebtCurrent"],
    "current_liabilities": ["LiabilitiesCurrent"],
    "long_term_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "other_long_term_liabilities": ["OtherLiabilitiesNoncurrent"],
    "total_liabilities": ["Liabilities"],
    "common_stock": ["CommonStockValue"],
    "retained_earnings": ["RetainedEarningsAccumulatedDeficit"],
    "treasury_stock": ["TreasuryStockValue"],
    "stockholders_equity": ["StockholdersEquity"],
}

CASH_FLOW_MAPPING = {
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "depreciation_amortization": ["DepreciationDepletionAndAmortization"],
    "stock_based_compensation": ["ShareBasedCompensation"],
    "deferred_income_taxes": ["DeferredIncomeTaxExpenseBenefit"],
    "changes_in_working_capital": ["IncreaseDecreaseInOperatingCapital"],
    "cash_from_operations": ["NetCashProvidedByUsedInOperatingActivities"],
    "capital_expenditures": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "acquisitions": ["PaymentsToAcquireBusinessesNetOfCashAcquired"],
    "purchases_of_investments": ["PaymentsToAcquireInvestments"],
    "sales_of_investments": ["ProceedsFromSaleOfInvestments"],
    "cash_from_investing": ["NetCashProvidedByUsedInInvestingActivities"],
    "debt_issued": ["ProceedsFromIssuanceOfLongTermDebt"],
    "debt_repaid": ["RepaymentsOfLongTermDebt"],
    "dividends_paid": ["PaymentsOfDividends"],
    "stock_repurchased": ["PaymentsForRepurchaseOfCommonStock"],
    "stock_issued": ["ProceedsFromIssuanceOfCommonStock"],
    "cash_from_financing": ["NetCashProvidedByUsedInFinancingActivities"],
    "net_change_in_cash": ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"],
}

def get_fact_value(facts_by_metric, metric_names):
    """Get the first available fact value from a list of possible metric names."""
    for name in metric_names:
        if name in facts_by_metric and facts_by_metric[name]:
            return facts_by_metric[name]
    return None

def build_statements(engine, statement_type, mapping, table_name):
    """Generic function to build any statement type."""
    
    print(f"\n{'='*80}")
    print(f"POPULATING {statement_type.upper()}")
    print(f"{'='*80}")
    
    # Get distinct periods
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT 
                cik, company_name, fiscal_year, fiscal_period,
                period_end_date, period_start_date,
                form_type, accession_number, filing_date
            FROM sec_financial_facts
            WHERE fiscal_year IS NOT NULL 
                AND fiscal_period IS NOT NULL
                AND period_end_date IS NOT NULL
            ORDER BY cik, fiscal_year DESC, fiscal_period
        """))
        periods = result.fetchall()
    
    print(f"\nFound {len(periods)} distinct reporting periods")
    print(f"Building {statement_type}...")
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    statements = []
    processed = 0
    
    for period in periods:
        cik, company_name, fiscal_year, fiscal_period = period[0:4]
        period_end, period_start, form_type, accession, filing_date = period[4:9]
        
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
            """), {"cik": cik, "fy": fiscal_year, "fp": fiscal_period, "end_date": period_end})
            facts_list = facts_result.fetchall()
        
        if not facts_list:
            continue
        
        # Build dictionary
        facts_by_metric = {fact_name: value for fact_name, value in facts_list}
        
        # Build statement record
        stmt = {
            "cik": cik,
            "company_name": company_name,
            "ticker": None,
            "period_end_date": period_end,
            "period_start_date": period_start,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "accession_number": accession,
            "form_type": form_type,
            "filing_date": filing_date,
        }
        
        # Map XBRL facts to statement fields
        for field, metric_names in mapping.items():
            stmt[field] = get_fact_value(facts_by_metric, metric_names)
        
        # Check if we have meaningful data
        has_data = any(v is not None for k, v in stmt.items() if k not in 
                      ["cik", "company_name", "ticker", "period_end_date", "period_start_date",
                       "fiscal_year", "fiscal_period", "accession_number", "form_type", "filing_date"])
        
        if has_data:
            statements.append(stmt)
            processed += 1
            
            if processed % 1000 == 0:
                print(f"  Processed {processed} {statement_type}...", end="\r")
    
    print(f"\n\nBuilt {len(statements)} {statement_type}")
    print("Inserting into database...")
    
    # Build INSERT statement
    if statements:
        fields = list(statements[0].keys())
        placeholders = ", ".join([f":{f}" for f in fields])
        columns = ", ".join(fields)
        
        insert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """
        
        batch_size = 500
        for i in range(0, len(statements), batch_size):
            batch = statements[i:i+batch_size]
            session.execute(text(insert_sql), batch)
            session.commit()
            print(f"  Inserted {min(i+batch_size, len(statements))}/{len(statements)}", end="\r")
        
        print(f"\n[OK] Inserted {len(statements)} {statement_type}")
    
    session.close()
    
    # Verify
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.fetchone()[0]
    
    print(f"Total in database: {count:,}\n")
    return count

def main():
    engine = create_engine('postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata')
    print("\n[OK] Connected to database")
    
    # Populate all three statement types
    income_count = build_statements(engine, "income statements", INCOME_STATEMENT_MAPPING, "sec_income_statement")
    balance_count = build_statements(engine, "balance sheets", BALANCE_SHEET_MAPPING, "sec_balance_sheet")
    cashflow_count = build_statements(engine, "cash flow statements", CASH_FLOW_MAPPING, "sec_cash_flow_statement")
    
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print(f"{'='*80}")
    print(f"Income statements: {income_count:,}")
    print(f"Balance sheets: {balance_count:,}")
    print(f"Cash flow statements: {cashflow_count:,}")
    print(f"{'='*80}\n")
    
    print("Sample queries:")
    print("  -- Apple's quarterly revenue")
    print("  SELECT fiscal_year, fiscal_period, revenues, net_income")
    print("  FROM sec_income_statement WHERE company_name LIKE '%APPLE%'")
    print("  ORDER BY fiscal_year DESC, fiscal_period;")
    print()
    print("  -- Compare total assets across companies")
    print("  SELECT company_name, fiscal_year, total_assets")
    print("  FROM sec_balance_sheet WHERE fiscal_year = 2024 AND fiscal_period = 'FY'")
    print("  ORDER BY total_assets DESC LIMIT 10;")
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

