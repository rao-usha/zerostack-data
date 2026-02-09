"""
XBRL data parser for SEC company facts.

Parses structured financial data from SEC's Company Facts API endpoint.
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import date, datetime
from decimal import Decimal

logger = logging.getLogger(__name__)


# Common XBRL fact mappings to standardized names
INCOME_STATEMENT_MAPPINGS = {
    # Revenue
    "Revenues": ["Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerExcludingAssessedTax"],
    "CostOfRevenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "GrossProfit": ["GrossProfit"],
    
    # Operating expenses
    "OperatingExpenses": ["OperatingExpenses", "OperatingExpensesAbstract"],
    "ResearchAndDevelopmentExpense": ["ResearchAndDevelopmentExpense"],
    "SellingGeneralAndAdministrativeExpense": ["SellingGeneralAndAdministrativeExpense"],
    
    # Operating income
    "OperatingIncomeLoss": ["OperatingIncomeLoss", "OperatingIncome"],
    
    # Other income/expense
    "InterestExpense": ["InterestExpense", "InterestExpenseDebt"],
    "InterestIncomeExpenseNet": ["InterestIncomeExpenseNet"],
    "OtherNonoperatingIncomeExpense": ["OtherNonoperatingIncomeExpense", "NonoperatingIncomeExpense"],
    
    # Pre-tax and tax
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": 
        ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", 
         "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"],
    "IncomeTaxExpenseBenefit": ["IncomeTaxExpenseBenefit"],
    
    # Net income
    "NetIncomeLoss": ["NetIncomeLoss", "ProfitLoss", "NetIncome"],
    
    # EPS
    "EarningsPerShareBasic": ["EarningsPerShareBasic"],
    "EarningsPerShareDiluted": ["EarningsPerShareDiluted"],
    "WeightedAverageNumberOfSharesOutstandingBasic": ["WeightedAverageNumberOfSharesOutstandingBasic"],
    "WeightedAverageNumberOfDilutedSharesOutstanding": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
}

BALANCE_SHEET_MAPPINGS = {
    # Current assets
    "CashAndCashEquivalentsAtCarryingValue": ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
    "ShortTermInvestments": ["ShortTermInvestments", "MarketableSecuritiesCurrent"],
    "AccountsReceivableNetCurrent": ["AccountsReceivableNetCurrent", "AccountsReceivableNet"],
    "InventoryNet": ["InventoryNet"],
    "AssetsCurrent": ["AssetsCurrent"],
    
    # Long-term assets
    "PropertyPlantAndEquipmentNet": ["PropertyPlantAndEquipmentNet"],
    "Goodwill": ["Goodwill"],
    "IntangibleAssetsNetExcludingGoodwill": ["IntangibleAssetsNetExcludingGoodwill"],
    "LongTermInvestments": ["LongTermInvestments", "MarketableSecuritiesNoncurrent"],
    
    # Total assets
    "Assets": ["Assets"],
    
    # Current liabilities
    "AccountsPayableCurrent": ["AccountsPayableCurrent", "AccountsPayableCurrentAndNoncurrent"],
    "ShortTermBorrowings": ["ShortTermBorrowings", "DebtCurrent"],
    "LiabilitiesCurrent": ["LiabilitiesCurrent"],
    
    # Long-term liabilities
    "LongTermDebt": ["LongTermDebt", "LongTermDebtNoncurrent"],
    "Liabilities": ["Liabilities"],
    
    # Equity
    "CommonStockValue": ["CommonStockValue"],
    "RetainedEarningsAccumulatedDeficit": ["RetainedEarningsAccumulatedDeficit"],
    "TreasuryStockValue": ["TreasuryStockValue"],
    "StockholdersEquity": ["StockholdersEquity"],
}

# =============================================================================
# Column name mapping: _to_snake_case() output → actual DB column names
# The XBRL PascalCase names produce long snake_case names that don't match
# the shorter column names in the SQLAlchemy models.
# =============================================================================

INCOME_COLUMN_MAP = {
    "operating_income_loss": "operating_income",
    "net_income_loss": "net_income",
    "research_and_development_expense": "research_and_development",
    "selling_general_and_administrative_expense": "selling_general_administrative",
    "income_loss_from_continuing_operations_before_income_taxes_extraordinary_items_noncontrolling_interest": "income_before_tax",
    "income_tax_expense_benefit": "income_tax_expense",
    "interest_income_expense_net": "interest_income",
    "other_nonoperating_income_expense": "other_income_expense",
    "weighted_average_number_of_shares_outstanding_basic": "weighted_average_shares_basic",
    "weighted_average_number_of_diluted_shares_outstanding": "weighted_average_shares_diluted",
}

BALANCE_SHEET_COLUMN_MAP = {
    "assets": "total_assets",
    "assets_current": "current_assets",
    "cash_and_cash_equivalents_at_carrying_value": "cash_and_equivalents",
    "accounts_receivable_net_current": "accounts_receivable",
    "inventory_net": "inventory",
    "property_plant_and_equipment_net": "property_plant_equipment",
    "intangible_assets_net_excluding_goodwill": "intangible_assets",
    "liabilities": "total_liabilities",
    "liabilities_current": "current_liabilities",
    "accounts_payable_current": "accounts_payable",
    "short_term_borrowings": "short_term_debt",
    "common_stock_value": "common_stock",
    "retained_earnings_accumulated_deficit": "retained_earnings",
    "treasury_stock_value": "treasury_stock",
}

CASH_FLOW_COLUMN_MAP = {
    "net_income_loss": "net_income",
    "depreciation_depletion_and_amortization": "depreciation_amortization",
    "share_based_compensation": "stock_based_compensation",
    "deferred_income_tax_expense_benefit": "deferred_income_taxes",
    "increase_decrease_in_operating_capital": "changes_in_working_capital",
    "net_cash_provided_by_used_in_operating_activities": "cash_from_operations",
    "payments_to_acquire_property_plant_and_equipment": "capital_expenditures",
    "payments_to_acquire_businesses_net_of_cash_acquired": "acquisitions",
    "payments_to_acquire_investments": "purchases_of_investments",
    "proceeds_from_sale_of_investments": "sales_of_investments",
    "net_cash_provided_by_used_in_investing_activities": "cash_from_investing",
    "proceeds_from_issuance_of_debt": "debt_issued",
    "repayments_of_debt": "debt_repaid",
    "payments_of_dividends": "dividends_paid",
    "payments_for_repurchase_of_common_stock": "stock_repurchased",
    "proceeds_from_issuance_of_common_stock": "stock_issued",
    "net_cash_provided_by_used_in_financing_activities": "cash_from_financing",
    "cash_cash_equivalents_restricted_cash_and_restricted_cash_equivalents_period_increase_decrease_including_exchange_rate_effect": "net_change_in_cash",
    "cash_and_cash_equivalents_at_carrying_value": "cash_end_of_period",
}

CASH_FLOW_MAPPINGS = {
    # Operating activities
    "NetIncomeLoss": ["NetIncomeLoss"],
    "DepreciationDepletionAndAmortization": ["DepreciationDepletionAndAmortization"],
    "ShareBasedCompensation": ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"],
    "DeferredIncomeTaxExpenseBenefit": ["DeferredIncomeTaxExpenseBenefit"],
    "IncreaseDecreaseInOperatingCapital": ["IncreaseDecreaseInOperatingCapital"],
    "NetCashProvidedByUsedInOperatingActivities": ["NetCashProvidedByUsedInOperatingActivities"],
    
    # Investing activities
    "PaymentsToAcquirePropertyPlantAndEquipment": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "PaymentsToAcquireBusinessesNetOfCashAcquired": ["PaymentsToAcquireBusinessesNetOfCashAcquired"],
    "PaymentsToAcquireInvestments": ["PaymentsToAcquireInvestments", "PaymentsToAcquireAvailableForSaleSecurities"],
    "ProceedsFromSaleOfInvestments": ["ProceedsFromSaleOfAvailableForSaleSecurities"],
    "NetCashProvidedByUsedInInvestingActivities": ["NetCashProvidedByUsedInInvestingActivities"],
    
    # Financing activities
    "ProceedsFromIssuanceOfDebt": ["ProceedsFromIssuanceOfLongTermDebt"],
    "RepaymentsOfDebt": ["RepaymentsOfLongTermDebt"],
    "PaymentsOfDividends": ["PaymentsOfDividends"],
    "PaymentsForRepurchaseOfCommonStock": ["PaymentsForRepurchaseOfCommonStock"],
    "ProceedsFromIssuanceOfCommonStock": ["ProceedsFromIssuanceOfCommonStock"],
    "NetCashProvidedByUsedInFinancingActivities": ["NetCashProvidedByUsedInFinancingActivities"],
    
    # Net change
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect": 
        ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"],
    "CashAndCashEquivalentsAtCarryingValue": ["CashAndCashEquivalentsAtCarryingValue"],
}


def parse_company_facts(facts_data: Dict[str, Any], cik: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse SEC Company Facts API response into structured financial data.
    
    Args:
        facts_data: Raw API response from /api/xbrl/companyfacts/CIK{cik}.json
        cik: Company CIK
        
    Returns:
        Dictionary with keys:
        - "financial_facts": List of all financial facts
        - "income_statement": List of income statement records
        - "balance_sheet": List of balance sheet records
        - "cash_flow": List of cash flow records
    """
    try:
        company_name = facts_data.get("entityName", "")
        
        # Extract facts from us-gaap taxonomy (most common)
        us_gaap = facts_data.get("facts", {}).get("us-gaap", {})
        
        # Parse all facts
        all_facts = []
        for fact_name, fact_data in us_gaap.items():
            parsed_facts = _parse_fact(fact_name, fact_data, cik, company_name, "us-gaap")
            all_facts.extend(parsed_facts)
        
        # Parse dei (Document and Entity Information) taxonomy
        dei = facts_data.get("facts", {}).get("dei", {})
        for fact_name, fact_data in dei.items():
            parsed_facts = _parse_fact(fact_name, fact_data, cik, company_name, "dei")
            all_facts.extend(parsed_facts)
        
        logger.info(f"Parsed {len(all_facts)} financial facts for CIK {cik}")
        
        # Build normalized financial statements
        income_statements = _build_income_statements(all_facts, cik, company_name)
        balance_sheets = _build_balance_sheets(all_facts, cik, company_name)
        cash_flows = _build_cash_flow_statements(all_facts, cik, company_name)
        
        return {
            "financial_facts": all_facts,
            "income_statement": income_statements,
            "balance_sheet": balance_sheets,
            "cash_flow": cash_flows,
        }
    
    except Exception as e:
        logger.error(f"Failed to parse company facts for CIK {cik}: {e}", exc_info=True)
        return {
            "financial_facts": [],
            "income_statement": [],
            "balance_sheet": [],
            "cash_flow": [],
        }


def _parse_fact(
    fact_name: str,
    fact_data: Dict[str, Any],
    cik: str,
    company_name: str,
    namespace: str
) -> List[Dict[str, Any]]:
    """
    Parse a single XBRL fact into database records.
    
    Args:
        fact_name: Name of the fact (e.g., "Assets", "Revenues")
        fact_data: Fact data from API
        cik: Company CIK
        company_name: Company name
        namespace: Taxonomy namespace (e.g., "us-gaap", "dei")
        
    Returns:
        List of parsed fact records
    """
    facts = []
    
    label = fact_data.get("label", fact_name)
    
    # Parse units (USD, shares, etc.)
    units_data = fact_data.get("units", {})
    
    for unit, values in units_data.items():
        for value_entry in values:
            try:
                # Extract value
                val = value_entry.get("val")
                if val is None:
                    continue
                
                # Try to convert to Decimal
                try:
                    numeric_value = Decimal(str(val))
                except:
                    numeric_value = None
                
                # Extract dates
                end_date = value_entry.get("end")
                start_date = value_entry.get("start")
                
                # Parse end date
                if end_date:
                    try:
                        period_end = datetime.strptime(end_date, "%Y-%m-%d").date()
                    except:
                        period_end = None
                else:
                    period_end = None
                
                # Parse start date
                if start_date:
                    try:
                        period_start = datetime.strptime(start_date, "%Y-%m-%d").date()
                    except:
                        period_start = None
                else:
                    period_start = None
                
                # Extract fiscal year and period
                fiscal_year = value_entry.get("fy")
                fiscal_period = value_entry.get("fp")  # Q1, Q2, Q3, Q4, FY
                
                # Extract form and accession number
                form_type = value_entry.get("form")
                accession_number = value_entry.get("accn")
                filed = value_entry.get("filed")
                
                # Parse filing date
                if filed:
                    try:
                        filing_date = datetime.strptime(filed, "%Y-%m-%d").date()
                    except:
                        filing_date = None
                else:
                    filing_date = None
                
                # Extract frame
                frame = value_entry.get("frame")
                
                fact_record = {
                    "cik": cik,
                    "company_name": company_name,
                    "fact_name": fact_name,
                    "fact_label": label,
                    "namespace": namespace,
                    "value": numeric_value,
                    "unit": unit,
                    "period_end_date": period_end,
                    "period_start_date": period_start,
                    "fiscal_year": fiscal_year,
                    "fiscal_period": fiscal_period,
                    "form_type": form_type,
                    "accession_number": accession_number,
                    "filing_date": filing_date,
                    "frame": frame,
                }
                
                facts.append(fact_record)
            
            except Exception as e:
                logger.debug(f"Failed to parse value entry for {fact_name}: {e}")
                continue
    
    return facts


def _find_fact_value(
    all_facts: List[Dict[str, Any]],
    fact_names: List[str],
    period_end: Optional[date],
    fiscal_year: Optional[int],
    fiscal_period: Optional[str]
) -> Optional[Decimal]:
    """
    Find a fact value matching the given criteria.
    
    Args:
        all_facts: List of all parsed facts
        fact_names: List of possible fact names to search for
        period_end: Period end date to match
        fiscal_year: Fiscal year to match
        fiscal_period: Fiscal period to match
        
    Returns:
        Fact value or None if not found
    """
    for fact in all_facts:
        if fact["fact_name"] in fact_names:
            if fiscal_year and fact["fiscal_year"] == fiscal_year:
                if fiscal_period and fact["fiscal_period"] == fiscal_period:
                    if fact["unit"] in ["USD", "USD/shares"]:
                        return fact["value"]
    
    return None


def _build_income_statements(
    all_facts: List[Dict[str, Any]],
    cik: str,
    company_name: str
) -> List[Dict[str, Any]]:
    """Build normalized income statement records from facts."""
    # Group facts by fiscal year and period
    periods = {}
    for fact in all_facts:
        fy = fact.get("fiscal_year")
        fp = fact.get("fiscal_period")
        if fy and fp:
            key = (fy, fp)
            if key not in periods:
                periods[key] = []
            periods[key].append(fact)
    
    income_statements = []
    
    for (fiscal_year, fiscal_period), period_facts in periods.items():
        # Find period end date
        period_end = None
        period_start = None
        accession_number = None
        form_type = None
        filing_date = None
        
        for fact in period_facts:
            if fact.get("period_end_date"):
                period_end = fact["period_end_date"]
                period_start = fact.get("period_start_date")
                accession_number = fact.get("accession_number")
                form_type = fact.get("form_type")
                filing_date = fact.get("filing_date")
                break
        
        if not period_end:
            continue
        
        # Extract income statement line items
        income_stmt = {
            "cik": cik,
            "company_name": company_name,
            "ticker": None,  # Will be filled by caller if available
            "period_end_date": period_end,
            "period_start_date": period_start,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "accession_number": accession_number,
            "form_type": form_type,
            "filing_date": filing_date,
        }
        
        # Map each line item
        for std_name, fact_names in INCOME_STATEMENT_MAPPINGS.items():
            value = _find_fact_value(period_facts, fact_names, period_end, fiscal_year, fiscal_period)
            col_name = _to_snake_case(std_name)
            col_name = INCOME_COLUMN_MAP.get(col_name, col_name)
            income_stmt[col_name] = value

        income_statements.append(income_stmt)
    
    logger.info(f"Built {len(income_statements)} income statement records for CIK {cik}")
    return income_statements


def _build_balance_sheets(
    all_facts: List[Dict[str, Any]],
    cik: str,
    company_name: str
) -> List[Dict[str, Any]]:
    """Build normalized balance sheet records from facts."""
    # Group facts by fiscal year and period
    periods = {}
    for fact in all_facts:
        fy = fact.get("fiscal_year")
        fp = fact.get("fiscal_period")
        if fy and fp:
            key = (fy, fp)
            if key not in periods:
                periods[key] = []
            periods[key].append(fact)
    
    balance_sheets = []
    
    for (fiscal_year, fiscal_period), period_facts in periods.items():
        # Find period end date
        period_end = None
        accession_number = None
        form_type = None
        filing_date = None
        
        for fact in period_facts:
            if fact.get("period_end_date"):
                period_end = fact["period_end_date"]
                accession_number = fact.get("accession_number")
                form_type = fact.get("form_type")
                filing_date = fact.get("filing_date")
                break
        
        if not period_end:
            continue
        
        # Extract balance sheet line items
        balance_sheet = {
            "cik": cik,
            "company_name": company_name,
            "ticker": None,
            "period_end_date": period_end,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "accession_number": accession_number,
            "form_type": form_type,
            "filing_date": filing_date,
        }
        
        # Map each line item
        for std_name, fact_names in BALANCE_SHEET_MAPPINGS.items():
            value = _find_fact_value(period_facts, fact_names, period_end, fiscal_year, fiscal_period)
            col_name = _to_snake_case(std_name)
            col_name = BALANCE_SHEET_COLUMN_MAP.get(col_name, col_name)
            balance_sheet[col_name] = value

        balance_sheets.append(balance_sheet)
    
    logger.info(f"Built {len(balance_sheets)} balance sheet records for CIK {cik}")
    return balance_sheets


def _build_cash_flow_statements(
    all_facts: List[Dict[str, Any]],
    cik: str,
    company_name: str
) -> List[Dict[str, Any]]:
    """Build normalized cash flow statement records from facts."""
    # Group facts by fiscal year and period
    periods = {}
    for fact in all_facts:
        fy = fact.get("fiscal_year")
        fp = fact.get("fiscal_period")
        if fy and fp:
            key = (fy, fp)
            if key not in periods:
                periods[key] = []
            periods[key].append(fact)
    
    cash_flows = []
    
    for (fiscal_year, fiscal_period), period_facts in periods.items():
        # Find period end date
        period_end = None
        period_start = None
        accession_number = None
        form_type = None
        filing_date = None
        
        for fact in period_facts:
            if fact.get("period_end_date"):
                period_end = fact["period_end_date"]
                period_start = fact.get("period_start_date")
                accession_number = fact.get("accession_number")
                form_type = fact.get("form_type")
                filing_date = fact.get("filing_date")
                break
        
        if not period_end:
            continue
        
        # Extract cash flow line items
        cash_flow = {
            "cik": cik,
            "company_name": company_name,
            "ticker": None,
            "period_end_date": period_end,
            "period_start_date": period_start,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "accession_number": accession_number,
            "form_type": form_type,
            "filing_date": filing_date,
        }
        
        # Map each line item
        for std_name, fact_names in CASH_FLOW_MAPPINGS.items():
            value = _find_fact_value(period_facts, fact_names, period_end, fiscal_year, fiscal_period)
            col_name = _to_snake_case(std_name)
            col_name = CASH_FLOW_COLUMN_MAP.get(col_name, col_name)
            cash_flow[col_name] = value

        # Calculate free cash flow if we have the components
        if cash_flow.get("cash_from_operations") and cash_flow.get("capital_expenditures"):
            operating_cf = cash_flow["cash_from_operations"]
            capex = cash_flow["capital_expenditures"]
            # SEC XBRL reports CapEx as positive (PaymentsToAcquire...), so subtract
            cash_flow["free_cash_flow"] = operating_cf - capex

        cash_flows.append(cash_flow)
    
    logger.info(f"Built {len(cash_flows)} cash flow statement records for CIK {cik}")
    return cash_flows


def _to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

