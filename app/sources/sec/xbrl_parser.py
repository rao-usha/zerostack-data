"""
XBRL data parser for SEC company facts.

Parses structured financial data from SEC's Company Facts API endpoint.

SPEC_113: facts are grouped by each fact's OWN period (start/end), not by the
filing's fy/fp. See docs/specs/SPEC_113_xbrl_period_keys.md for the rules.
"""

import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)


# Common XBRL fact mappings to standardized names
INCOME_STATEMENT_MAPPINGS = {
    # Revenue
    "Revenues": [
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
    ],
    "CostOfRevenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "GrossProfit": ["GrossProfit"],
    # Operating expenses
    "OperatingExpenses": ["OperatingExpenses", "OperatingExpensesAbstract"],
    "ResearchAndDevelopmentExpense": ["ResearchAndDevelopmentExpense"],
    "SellingGeneralAndAdministrativeExpense": [
        "SellingGeneralAndAdministrativeExpense"
    ],
    # Operating income
    "OperatingIncomeLoss": ["OperatingIncomeLoss", "OperatingIncome"],
    # Other income/expense
    "InterestExpense": ["InterestExpense", "InterestExpenseDebt"],
    "InterestIncomeExpenseNet": ["InterestIncomeExpenseNet"],
    "OtherNonoperatingIncomeExpense": [
        "OtherNonoperatingIncomeExpense",
        "NonoperatingIncomeExpense",
    ],
    # Pre-tax and tax
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxes",
    ],
    "IncomeTaxExpenseBenefit": ["IncomeTaxExpenseBenefit"],
    # Net income
    "NetIncomeLoss": ["NetIncomeLoss", "ProfitLoss", "NetIncome"],
    # EPS
    "EarningsPerShareBasic": ["EarningsPerShareBasic"],
    "EarningsPerShareDiluted": ["EarningsPerShareDiluted"],
    "WeightedAverageNumberOfSharesOutstandingBasic": [
        "WeightedAverageNumberOfSharesOutstandingBasic"
    ],
    "WeightedAverageNumberOfDilutedSharesOutstanding": [
        "WeightedAverageNumberOfDilutedSharesOutstanding"
    ],
}

BALANCE_SHEET_MAPPINGS = {
    # Current assets
    "CashAndCashEquivalentsAtCarryingValue": [
        "CashAndCashEquivalentsAtCarryingValue",
        "Cash",
    ],
    "ShortTermInvestments": ["ShortTermInvestments", "MarketableSecuritiesCurrent"],
    "AccountsReceivableNetCurrent": [
        "AccountsReceivableNetCurrent",
        "AccountsReceivableNet",
    ],
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
    "AccountsPayableCurrent": [
        "AccountsPayableCurrent",
        "AccountsPayableCurrentAndNoncurrent",
    ],
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
    "ShareBasedCompensation": [
        "ShareBasedCompensation",
        "AllocatedShareBasedCompensationExpense",
    ],
    "DeferredIncomeTaxExpenseBenefit": ["DeferredIncomeTaxExpenseBenefit"],
    "IncreaseDecreaseInOperatingCapital": ["IncreaseDecreaseInOperatingCapital"],
    "NetCashProvidedByUsedInOperatingActivities": [
        "NetCashProvidedByUsedInOperatingActivities"
    ],
    # Investing activities
    "PaymentsToAcquirePropertyPlantAndEquipment": [
        "PaymentsToAcquirePropertyPlantAndEquipment"
    ],
    "PaymentsToAcquireBusinessesNetOfCashAcquired": [
        "PaymentsToAcquireBusinessesNetOfCashAcquired"
    ],
    "PaymentsToAcquireInvestments": [
        "PaymentsToAcquireInvestments",
        "PaymentsToAcquireAvailableForSaleSecurities",
    ],
    "ProceedsFromSaleOfInvestments": ["ProceedsFromSaleOfAvailableForSaleSecurities"],
    "NetCashProvidedByUsedInInvestingActivities": [
        "NetCashProvidedByUsedInInvestingActivities"
    ],
    # Financing activities
    "ProceedsFromIssuanceOfDebt": ["ProceedsFromIssuanceOfLongTermDebt"],
    "RepaymentsOfDebt": ["RepaymentsOfLongTermDebt"],
    "PaymentsOfDividends": ["PaymentsOfDividends"],
    "PaymentsForRepurchaseOfCommonStock": ["PaymentsForRepurchaseOfCommonStock"],
    "ProceedsFromIssuanceOfCommonStock": ["ProceedsFromIssuanceOfCommonStock"],
    "NetCashProvidedByUsedInFinancingActivities": [
        "NetCashProvidedByUsedInFinancingActivities"
    ],
    # Net change
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect": [
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"
    ],
}

# Cash balances are instants; a cash-flow row reads them at the period's end
# (cash_end_of_period) and at the day before its start (cash_beginning_of_period).
CASH_INSTANT_CONCEPTS = [
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "CashAndCashEquivalentsAtCarryingValue",
]

# Units accepted for statement line items (EPS in USD/shares, share counts in shares)
STATEMENT_UNITS = ("USD", "USD/shares", "shares")

# =============================================================================
# Period model (SPEC_113)
#
# Facts are grouped by their OWN period — (start, end) for durations, end for
# instants — never by the filing's fy/fp (a FY2024 10-K carries FY2023/FY2022
# comparatives tagged fy=2024, and 10-Qs mix 3-month and YTD durations).
# =============================================================================

BUCKET_ANNUAL = "FY"
BUCKET_QUARTER = "Q"
BUCKET_H1 = "H1"
BUCKET_9M = "9M"

# Income / cash-flow rows keep only these durations
STATEMENT_BUCKETS = (BUCKET_ANNUAL, BUCKET_QUARTER)


def duration_bucket(start: Optional[date], end: date) -> Optional[str]:
    """Classify a duration: FY (350-380d), Q (80-100d), H1 (170-190d), 9M (260-285d),
    else 'D<days>'. Instants (no start) return None."""
    if start is None:
        return None
    days = (end - start).days
    if 80 <= days <= 100:
        return BUCKET_QUARTER
    if 350 <= days <= 380:
        return BUCKET_ANNUAL
    if 170 <= days <= 190:
        return BUCKET_H1
    if 260 <= days <= 285:
        return BUCKET_9M
    return f"D{days}"


def three_year_cutoff(as_of: date) -> date:
    """Earliest period_end kept by the bulk loader: same calendar day 3 years earlier."""
    try:
        return as_of.replace(year=as_of.year - 3)
    except ValueError:  # Feb 29 -> Feb 28
        return as_of.replace(year=as_of.year - 3, day=28)


def _fallback_fiscal_year(end: date) -> int:
    """Fiscal year when no filing reports the period as current: the end's year,
    except 52/53-week years ending in the first week of January."""
    if end.month == 1 and end.day <= 7:
        return end.year - 1
    return end.year


def _to_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        try:
            return datetime.strptime(str(value), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None


class _Entry:
    """One normalized companyfacts value."""

    __slots__ = ("ns", "name", "label", "unit", "start", "end", "val", "accn", "fy", "fp", "form",
                 "filed", "frame")

    def __init__(self, ns, name, label, unit, start, end, val, accn, fy, fp, form, filed, frame):
        self.ns = ns
        self.name = name
        self.label = label
        self.unit = unit
        self.start = start
        self.end = end
        self.val = val
        self.accn = accn
        self.fy = fy
        self.fp = fp
        self.form = form
        self.filed = filed
        self.frame = frame

    @property
    def rank(self):
        """Restatement preference: latest filed, then highest accession."""
        return (self.filed or date.min, self.accn or "")


def _iter_entries(
    facts_data: Dict[str, Any],
    namespaces: Tuple[str, ...],
    names: Optional[Set[str]] = None,
    min_period_end: Optional[date] = None,
) -> Iterator[_Entry]:
    facts = facts_data.get("facts", {}) or {}
    for ns in namespaces:
        for fact_name, fact_data in (facts.get(ns) or {}).items():
            if names is not None and fact_name not in names:
                continue
            label = fact_data.get("label", fact_name)
            for unit, values in (fact_data.get("units") or {}).items():
                for v in values:
                    try:
                        raw = v.get("val")
                        end = _to_date(v.get("end"))
                        if raw is None or end is None:
                            continue
                        if min_period_end is not None and end < min_period_end:
                            continue
                        try:
                            val = Decimal(str(raw))
                        except (InvalidOperation, ValueError, TypeError):
                            continue
                        fy = v.get("fy")
                        yield _Entry(
                            ns, fact_name, label, unit, _to_date(v.get("start")), end, val,
                            v.get("accn"), fy if isinstance(fy, int) else None, v.get("fp"),
                            v.get("form"), _to_date(v.get("filed")), v.get("frame"),
                        )
                    except Exception as e:  # malformed entry
                        logger.debug(f"Skipping malformed value for {fact_name}: {e}")


def _document_period_ends(facts_data: Dict[str, Any]) -> Dict[str, date]:
    """accession -> the filing's own (current) period end.

    Max end over the filing's us-gaap DURATION facts of >= 80 days. Instants are
    ignored so dei cover dates / subsequent-event instants don't skew it.
    """
    doc_end: Dict[str, str] = {}
    us_gaap = (facts_data.get("facts", {}) or {}).get("us-gaap", {}) or {}
    for fact_data in us_gaap.values():
        for values in (fact_data.get("units") or {}).values():
            for v in values:
                start, end, accn = v.get("start"), v.get("end"), v.get("accn")
                if not (start and end and accn):
                    continue
                if end <= doc_end.get(accn, ""):
                    continue
                s, e = _to_date(start), _to_date(end)
                if s and e and (e - s).days >= 80:
                    doc_end[accn] = end
    return {k: d for k, v in doc_end.items() if (d := _to_date(v))}


class _PeriodLabeler:
    """Derives fiscal_year / fiscal_period / originating filing for a period.

    If a filing's document period end equals the period end, that filing
    reports the period as its current period: its fy is the fiscal year and
    the earliest such filing (the original, not an amendment/comparative)
    supplies accession/form/filing date. Otherwise labels are derived from
    the period (see _fallback_fiscal_year and the next annual period end).
    """

    def __init__(self, doc_ends: Dict[str, date], entries: Iterable[_Entry]):
        self.doc_ends = doc_ends
        self.by_period: Dict[Tuple[Optional[date], date], List[_Entry]] = defaultdict(list)
        annual: Set[date] = set()
        for e in entries:
            self.by_period[(e.start, e.end)].append(e)
            if e.start is not None and duration_bucket(e.start, e.end) == BUCKET_ANNUAL:
                annual.add(e.end)
        self.annual_ends = sorted(annual)
        self._cache: Dict[Tuple[Optional[date], date], Dict[str, Any]] = {}
        self._annual_fy_cache: Dict[date, int] = {}

    def _own_filing(self, key) -> Optional[_Entry]:
        end = key[1]
        own = [e for e in self.by_period.get(key, ()) if e.accn and self.doc_ends.get(e.accn) == end]
        if not own:
            return None
        return min(own, key=lambda e: (e.filed or date.max, e.accn or ""))

    def _next_annual_end(self, end: date) -> Optional[date]:
        for ae in self.annual_ends:
            if ae >= end - timedelta(days=3):
                return ae if (ae - end).days <= 370 else None
        return None

    def _annual_fy(self, annual_end: date) -> int:
        if annual_end not in self._annual_fy_cache:
            fy = _fallback_fiscal_year(annual_end)
            for (start, end) in self.by_period:
                if end == annual_end and start is not None and duration_bucket(start, end) == BUCKET_ANNUAL:
                    own = self._own_filing((start, end))
                    if own is not None and own.fy:
                        fy = own.fy
                        break
            self._annual_fy_cache[annual_end] = fy
        return self._annual_fy_cache[annual_end]

    def _fallback(self, end: date, bucket: Optional[str]) -> Tuple[int, str]:
        if bucket == BUCKET_ANNUAL:
            return _fallback_fiscal_year(end), "FY"
        ae = self._next_annual_end(end)
        if ae is not None:
            q = 4 - int(round((ae - end).days / 91.3))
            q = min(4, max(1, q))
            fy = self._annual_fy(ae)
        else:
            q = (end.month - 1) // 3 + 1
            fy = _fallback_fiscal_year(end)
        if bucket is None:  # instant
            return fy, "FY" if q == 4 and ae is not None else f"Q{q}"
        if bucket == BUCKET_QUARTER:
            return fy, f"Q{q}"
        return fy, bucket

    def label(self, start: Optional[date], end: date) -> Dict[str, Any]:
        key = (start, end)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        bucket = duration_bucket(start, end)
        own = self._own_filing(key)
        if own is not None:
            if bucket == BUCKET_ANNUAL:
                fp = "FY"
            elif bucket == BUCKET_QUARTER:
                fp = own.fp if own.fp in ("Q1", "Q2", "Q3", "Q4") else ("Q4" if own.fp == "FY" else None)
            elif bucket is None:
                fp = own.fp if own.fp in ("Q1", "Q2", "Q3", "Q4", "FY") else None
            else:
                fp = bucket
            fy = own.fy
            if fy is None or fp is None:
                fb_fy, fb_fp = self._fallback(end, bucket)
                fy, fp = fy or fb_fy, fp or fb_fp
            src = own
        else:
            fy, fp = self._fallback(end, bucket)
            entries = self.by_period.get(key, ())
            src = max(entries, key=lambda e: e.rank) if entries else None
        out = {
            "fiscal_year": fy,
            "fiscal_period": fp,
            "accession_number": src.accn if src else None,
            "form_type": src.form if src else None,
            "filing_date": src.filed if src else None,
        }
        self._cache[key] = out
        return out


def _best_by_period(entries: Iterable[_Entry]) -> Dict[Tuple[str, str, Optional[date], date], _Entry]:
    """(name, unit, start, end) -> latest-filed entry (restatements win)."""
    best: Dict[Tuple[str, str, Optional[date], date], _Entry] = {}
    for e in entries:
        k = (e.name, e.unit, e.start, e.end)
        cur = best.get(k)
        if cur is None or e.rank > cur.rank:
            best[k] = e
    return best


def _lookup(best, names: List[str], start: Optional[date], end: date) -> Optional[Decimal]:
    for name in names:
        for unit in STATEMENT_UNITS:
            e = best.get((name, unit, start, end))
            if e is not None:
                return e.val
    return None


def _column(std_name: str, column_map: Dict[str, str]) -> str:
    col = _to_snake_case(std_name)
    return column_map.get(col, col)


_INCOME_NAMES = {n for names in INCOME_STATEMENT_MAPPINGS.values() for n in names}
_BALANCE_NAMES = {n for names in BALANCE_SHEET_MAPPINGS.values() for n in names}
_CASH_FLOW_NAMES = {n for names in CASH_FLOW_MAPPINGS.values() for n in names}
# Concepts that make a period a cash-flow statement period (net income alone does not)
_CASH_FLOW_DEFINING = {
    n for std, names in CASH_FLOW_MAPPINGS.items() if std != "NetIncomeLoss" for n in names
}
_STATEMENT_NAMES = _INCOME_NAMES | _BALANCE_NAMES | _CASH_FLOW_NAMES | set(CASH_INSTANT_CONCEPTS)


def _build_statements_from_entries(
    entries: List[_Entry], doc_ends: Dict[str, date], cik: str, company_name: str
) -> Dict[str, List[Dict[str, Any]]]:
    labeler = _PeriodLabeler(doc_ends, entries)
    best = _best_by_period(entries)

    income_periods: Set[Tuple[date, date]] = set()
    cash_periods: Set[Tuple[date, date]] = set()
    balance_ends: Set[date] = set()
    for (name, _unit, start, end) in best:
        if start is None:
            if name in _BALANCE_NAMES:
                balance_ends.add(end)
            continue
        if duration_bucket(start, end) not in STATEMENT_BUCKETS:
            continue  # YTD H1/9M and odd durations are never stored as rows
        if name in _INCOME_NAMES:
            income_periods.add((start, end))
        if name in _CASH_FLOW_DEFINING:
            cash_periods.add((start, end))

    def base(start, end, with_start=True):
        row = {"cik": cik, "company_name": company_name, "ticker": None, "period_end_date": end}
        if with_start:
            row["period_start_date"] = start
        row.update(labeler.label(start, end))
        return row

    income_statements = []
    for start, end in sorted(income_periods):
        row = base(start, end)
        for std_name, names in INCOME_STATEMENT_MAPPINGS.items():
            row[_column(std_name, INCOME_COLUMN_MAP)] = _lookup(best, names, start, end)
        income_statements.append(row)

    balance_sheets = []
    for end in sorted(balance_ends):
        row = base(None, end, with_start=False)
        for std_name, names in BALANCE_SHEET_MAPPINGS.items():
            row[_column(std_name, BALANCE_SHEET_COLUMN_MAP)] = _lookup(best, names, None, end)
        balance_sheets.append(row)

    cash_flows = []
    for start, end in sorted(cash_periods):
        row = base(start, end)
        for std_name, names in CASH_FLOW_MAPPINGS.items():
            row[_column(std_name, CASH_FLOW_COLUMN_MAP)] = _lookup(best, names, start, end)
        row["cash_end_of_period"] = _lookup(best, CASH_INSTANT_CONCEPTS, None, end)
        row["cash_beginning_of_period"] = _lookup(
            best, CASH_INSTANT_CONCEPTS, None, start - timedelta(days=1)
        )
        ocf, capex = row.get("cash_from_operations"), row.get("capital_expenditures")
        # SEC XBRL reports CapEx as positive (PaymentsToAcquire...), so subtract
        row["free_cash_flow"] = ocf - capex if ocf is not None and capex is not None else None
        cash_flows.append(row)

    return {"income_statement": income_statements, "balance_sheet": balance_sheets, "cash_flow": cash_flows}


def build_financial_statements(
    facts_data: Dict[str, Any], cik: str, min_period_end: Optional[date] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """Income statement / balance sheet / cash flow rows only (no raw facts).

    Used by the sec_companyfacts bulk loader. Only mapped us-gaap concepts are
    materialized, so memory stays proportional to one company's statements.
    Raises on malformed input (callers decide whether to skip the company).
    """
    company_name = facts_data.get("entityName", "")
    doc_ends = _document_period_ends(facts_data)
    entries = list(_iter_entries(facts_data, ("us-gaap",), _STATEMENT_NAMES, min_period_end))
    return _build_statements_from_entries(entries, doc_ends, cik, company_name)


# Concepts materialized into sec_financial_facts by the bulk loader. The
# public_company_financials view reads depreciation from here; loading every
# us-gaap + dei fact would add ~2M rows for a 3-year window (SPEC_115).
FACT_CONCEPTS = (
    "DepreciationDepletionAndAmortization",
    "DepreciationAndAmortization",
    "Depreciation",
)


def build_financial_facts(
    facts_data: Dict[str, Any],
    cik: str,
    names: Optional[Sequence[str]] = None,
    min_period_end: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Fact rows for an allowlist of concepts, keyed on their own period.

    Same period logic as the statements: one row per (concept, unit, period),
    latest filed wins, labels derived from the period rather than the filing.
    """
    names = set(names or FACT_CONCEPTS)
    company_name = facts_data.get("entityName", "")
    doc_ends = _document_period_ends(facts_data)
    entries = list(_iter_entries(facts_data, ("us-gaap",), names, min_period_end))
    if not entries:
        return []

    labeler = _PeriodLabeler(doc_ends, entries)
    best: Dict[Tuple[str, str, Optional[date], date], Any] = {}
    for e in entries:
        k = (e.name, e.unit, e.start, e.end)
        cur = best.get(k)
        if cur is None or e.rank > cur.rank:
            best[k] = e

    rows = []
    for e in best.values():
        labels = labeler.label(e.start, e.end)
        rows.append(
            {
                "cik": cik,
                "company_name": company_name,
                "fact_name": e.name,
                "fact_label": e.label,
                "namespace": e.ns,
                "value": e.val,
                "unit": e.unit,
                "period_end_date": e.end,
                "period_start_date": e.start,
                "fiscal_year": labels["fiscal_year"],
                "fiscal_period": labels["fiscal_period"],
                "form_type": e.form,
                "accession_number": e.accn,
                "filing_date": e.filed,
                "frame": e.frame,
            }
        )
    return rows


def parse_company_facts(
    facts_data: Dict[str, Any], cik: str, min_period_end: Optional[date] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse SEC Company Facts API response into structured financial data.

    Args:
        facts_data: Raw API response from /api/xbrl/companyfacts/CIK{cik}.json
        cik: Company CIK
        min_period_end: Optional cutoff; periods ending earlier are dropped

    Returns:
        Dictionary with keys:
        - "financial_facts": one record per (namespace, fact, unit, own period), latest filed
        - "income_statement": annual + discrete-quarter rows keyed by own period
        - "balance_sheet": instant rows keyed by period end
        - "cash_flow": annual + discrete-quarter rows keyed by own period
    """
    try:
        company_name = facts_data.get("entityName", "")
        doc_ends = _document_period_ends(facts_data)
        all_entries = list(_iter_entries(facts_data, ("us-gaap", "dei"), None, min_period_end))

        facts_labeler = _PeriodLabeler(doc_ends, all_entries)
        best_facts: Dict[Tuple[str, str, str, Optional[date], date], _Entry] = {}
        for e in all_entries:
            k = (e.ns, e.name, e.unit, e.start, e.end)
            cur = best_facts.get(k)
            if cur is None or e.rank > cur.rank:
                best_facts[k] = e

        all_facts = []
        for e in best_facts.values():
            labels = facts_labeler.label(e.start, e.end)
            all_facts.append(
                {
                    "cik": cik,
                    "company_name": company_name,
                    "fact_name": e.name,
                    "fact_label": e.label,
                    "namespace": e.ns,
                    "value": e.val,
                    "unit": e.unit,
                    "period_end_date": e.end,
                    "period_start_date": e.start,
                    # Derived own-period labels (duration-aware: FY/Q1-Q4/H1/9M/D<days>)
                    "fiscal_year": labels["fiscal_year"],
                    "fiscal_period": labels["fiscal_period"],
                    # The filing that supplied this (latest filed) value
                    "form_type": e.form,
                    "accession_number": e.accn,
                    "filing_date": e.filed,
                    "frame": e.frame,
                }
            )
        logger.info(f"Parsed {len(all_facts)} financial facts for CIK {cik}")

        statement_entries = [e for e in all_entries if e.ns == "us-gaap" and e.name in _STATEMENT_NAMES]
        statements = _build_statements_from_entries(statement_entries, doc_ends, cik, company_name)
        logger.info(
            f"Built {len(statements['income_statement'])} income, {len(statements['balance_sheet'])} balance, "
            f"{len(statements['cash_flow'])} cash flow records for CIK {cik}"
        )
        return {"financial_facts": all_facts, **statements}

    except Exception as e:
        logger.error(f"Failed to parse company facts for CIK {cik}: {e}", exc_info=True)
        return {
            "financial_facts": [],
            "income_statement": [],
            "balance_sheet": [],
            "cash_flow": [],
        }


def _to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
