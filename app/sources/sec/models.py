"""
SEC EDGAR specific database models for structured financial data.

These tables store parsed XBRL financial data and filing sections.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Date,
    DECIMAL,
    Index,
    UniqueConstraint,
)

from app.core.models import Base


class SECFinancialFact(Base):
    """
    Stores individual XBRL financial facts/metrics.

    XBRL data from SEC contains structured financial metrics like:
    - Assets, Liabilities, Equity
    - Revenue, Net Income, EPS
    - Cash flows
    - Per-share data
    """

    __tablename__ = "sec_financial_facts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Company identifiers
    cik = Column(String(10), nullable=False, index=True)
    company_name = Column(Text, nullable=True)

    # Fact metadata
    fact_name = Column(
        Text, nullable=False, index=True
    )  # e.g., "Assets", "Revenues", "NetIncomeLoss"
    fact_label = Column(Text, nullable=True)  # Human-readable label
    namespace = Column(String(50), nullable=True, index=True)  # e.g., "us-gaap", "dei"

    # Financial data
    value = Column(DECIMAL(20, 2), nullable=True)  # Numeric value
    unit = Column(String(20), nullable=True)  # e.g., "USD", "shares"

    # Time period
    period_end_date = Column(Date, nullable=True, index=True)
    period_start_date = Column(Date, nullable=True)
    fiscal_year = Column(Integer, nullable=True, index=True)
    fiscal_period = Column(String(10), nullable=True, index=True)  # Q1, Q2, Q3, Q4, FY

    # Form metadata
    form_type = Column(String(20), nullable=True, index=True)  # 10-K, 10-Q, etc.
    accession_number = Column(String(20), nullable=True, index=True)
    filing_date = Column(Date, nullable=True)

    # Additional context
    frame = Column(String(50), nullable=True)  # e.g., "CY2023Q4"

    __table_args__ = (
        UniqueConstraint(
            "cik",
            "fact_name",
            "period_end_date",
            "fiscal_year",
            "fiscal_period",
            "unit",
            name="uq_sec_facts_cik_fact_period_unit",
        ),
        Index("idx_sec_facts_cik_fact_period", "cik", "fact_name", "period_end_date"),
        Index("idx_sec_facts_fiscal", "fiscal_year", "fiscal_period"),
    )


class SECIncomeStatement(Base):
    """
    Normalized income statement data from SEC filings.

    Key line items from income statements across companies.
    """

    __tablename__ = "sec_income_statement"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Company identifiers
    cik = Column(String(10), nullable=False, index=True)
    company_name = Column(Text, nullable=True)
    ticker = Column(String(10), nullable=True, index=True)

    # Period
    period_end_date = Column(Date, nullable=False, index=True)
    period_start_date = Column(Date, nullable=True)
    fiscal_year = Column(Integer, nullable=False, index=True)
    fiscal_period = Column(String(10), nullable=False, index=True)  # Q1, Q2, Q3, Q4, FY

    # Filing reference
    accession_number = Column(String(20), nullable=True)
    form_type = Column(String(20), nullable=True)
    filing_date = Column(Date, nullable=True)

    # Income statement line items (in USD)
    revenues = Column(DECIMAL(20, 2), nullable=True)
    cost_of_revenue = Column(DECIMAL(20, 2), nullable=True)
    gross_profit = Column(DECIMAL(20, 2), nullable=True)

    operating_expenses = Column(DECIMAL(20, 2), nullable=True)
    research_and_development = Column(DECIMAL(20, 2), nullable=True)
    selling_general_administrative = Column(DECIMAL(20, 2), nullable=True)

    operating_income = Column(DECIMAL(20, 2), nullable=True)

    interest_expense = Column(DECIMAL(20, 2), nullable=True)
    interest_income = Column(DECIMAL(20, 2), nullable=True)
    other_income_expense = Column(DECIMAL(20, 2), nullable=True)

    income_before_tax = Column(DECIMAL(20, 2), nullable=True)
    income_tax_expense = Column(DECIMAL(20, 2), nullable=True)

    net_income = Column(DECIMAL(20, 2), nullable=True)

    # Per-share data
    earnings_per_share_basic = Column(DECIMAL(10, 4), nullable=True)
    earnings_per_share_diluted = Column(DECIMAL(10, 4), nullable=True)
    weighted_average_shares_basic = Column(DECIMAL(20, 0), nullable=True)
    weighted_average_shares_diluted = Column(DECIMAL(20, 0), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "cik",
            "period_end_date",
            "fiscal_year",
            "fiscal_period",
            name="uq_sec_income_cik_period",
        ),
        Index("idx_sec_income_cik_period", "cik", "period_end_date"),
        Index("idx_sec_income_fiscal", "fiscal_year", "fiscal_period"),
    )


class SECBalanceSheet(Base):
    """
    Normalized balance sheet data from SEC filings.

    Key line items from balance sheets across companies.
    """

    __tablename__ = "sec_balance_sheet"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Company identifiers
    cik = Column(String(10), nullable=False, index=True)
    company_name = Column(Text, nullable=True)
    ticker = Column(String(10), nullable=True, index=True)

    # Period
    period_end_date = Column(Date, nullable=False, index=True)
    fiscal_year = Column(Integer, nullable=False, index=True)
    fiscal_period = Column(String(10), nullable=False, index=True)

    # Filing reference
    accession_number = Column(String(20), nullable=True)
    form_type = Column(String(20), nullable=True)
    filing_date = Column(Date, nullable=True)

    # Assets (in USD)
    cash_and_equivalents = Column(DECIMAL(20, 2), nullable=True)
    short_term_investments = Column(DECIMAL(20, 2), nullable=True)
    accounts_receivable = Column(DECIMAL(20, 2), nullable=True)
    inventory = Column(DECIMAL(20, 2), nullable=True)
    current_assets = Column(DECIMAL(20, 2), nullable=True)

    property_plant_equipment = Column(DECIMAL(20, 2), nullable=True)
    goodwill = Column(DECIMAL(20, 2), nullable=True)
    intangible_assets = Column(DECIMAL(20, 2), nullable=True)
    long_term_investments = Column(DECIMAL(20, 2), nullable=True)
    other_long_term_assets = Column(DECIMAL(20, 2), nullable=True)

    total_assets = Column(DECIMAL(20, 2), nullable=True)

    # Liabilities (in USD)
    accounts_payable = Column(DECIMAL(20, 2), nullable=True)
    short_term_debt = Column(DECIMAL(20, 2), nullable=True)
    current_liabilities = Column(DECIMAL(20, 2), nullable=True)

    long_term_debt = Column(DECIMAL(20, 2), nullable=True)
    other_long_term_liabilities = Column(DECIMAL(20, 2), nullable=True)
    total_liabilities = Column(DECIMAL(20, 2), nullable=True)

    # Equity (in USD)
    common_stock = Column(DECIMAL(20, 2), nullable=True)
    retained_earnings = Column(DECIMAL(20, 2), nullable=True)
    treasury_stock = Column(DECIMAL(20, 2), nullable=True)
    stockholders_equity = Column(DECIMAL(20, 2), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "cik",
            "period_end_date",
            "fiscal_year",
            "fiscal_period",
            name="uq_sec_balance_cik_period",
        ),
        Index("idx_sec_balance_cik_period", "cik", "period_end_date"),
        Index("idx_sec_balance_fiscal", "fiscal_year", "fiscal_period"),
    )


class SECCashFlowStatement(Base):
    """
    Normalized cash flow statement data from SEC filings.

    Key line items from cash flow statements across companies.
    """

    __tablename__ = "sec_cash_flow_statement"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Company identifiers
    cik = Column(String(10), nullable=False, index=True)
    company_name = Column(Text, nullable=True)
    ticker = Column(String(10), nullable=True, index=True)

    # Period
    period_end_date = Column(Date, nullable=False, index=True)
    period_start_date = Column(Date, nullable=True)
    fiscal_year = Column(Integer, nullable=False, index=True)
    fiscal_period = Column(String(10), nullable=False, index=True)

    # Filing reference
    accession_number = Column(String(20), nullable=True)
    form_type = Column(String(20), nullable=True)
    filing_date = Column(Date, nullable=True)

    # Operating activities (in USD)
    net_income = Column(DECIMAL(20, 2), nullable=True)
    depreciation_amortization = Column(DECIMAL(20, 2), nullable=True)
    stock_based_compensation = Column(DECIMAL(20, 2), nullable=True)
    deferred_income_taxes = Column(DECIMAL(20, 2), nullable=True)
    changes_in_working_capital = Column(DECIMAL(20, 2), nullable=True)
    cash_from_operations = Column(DECIMAL(20, 2), nullable=True)

    # Investing activities (in USD)
    capital_expenditures = Column(DECIMAL(20, 2), nullable=True)
    acquisitions = Column(DECIMAL(20, 2), nullable=True)
    purchases_of_investments = Column(DECIMAL(20, 2), nullable=True)
    sales_of_investments = Column(DECIMAL(20, 2), nullable=True)
    cash_from_investing = Column(DECIMAL(20, 2), nullable=True)

    # Financing activities (in USD)
    debt_issued = Column(DECIMAL(20, 2), nullable=True)
    debt_repaid = Column(DECIMAL(20, 2), nullable=True)
    dividends_paid = Column(DECIMAL(20, 2), nullable=True)
    stock_repurchased = Column(DECIMAL(20, 2), nullable=True)
    stock_issued = Column(DECIMAL(20, 2), nullable=True)
    cash_from_financing = Column(DECIMAL(20, 2), nullable=True)

    # Net change
    net_change_in_cash = Column(DECIMAL(20, 2), nullable=True)
    cash_beginning_of_period = Column(DECIMAL(20, 2), nullable=True)
    cash_end_of_period = Column(DECIMAL(20, 2), nullable=True)

    # Free cash flow (calculated)
    free_cash_flow = Column(DECIMAL(20, 2), nullable=True)  # Operating CF - CapEx

    __table_args__ = (
        UniqueConstraint(
            "cik",
            "period_end_date",
            "fiscal_year",
            "fiscal_period",
            name="uq_sec_cashflow_cik_period",
        ),
        Index("idx_sec_cashflow_cik_period", "cik", "period_end_date"),
        Index("idx_sec_cashflow_fiscal", "fiscal_year", "fiscal_period"),
    )


class SECFilingSection(Base):
    """
    Text sections extracted from SEC filings.

    Stores parsed sections like:
    - Item 1: Business
    - Item 1A: Risk Factors
    - Item 7: MD&A (Management Discussion & Analysis)
    - Item 8: Financial Statements
    """

    __tablename__ = "sec_filing_sections"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Company identifiers
    cik = Column(String(10), nullable=False, index=True)
    company_name = Column(Text, nullable=True)

    # Filing reference
    accession_number = Column(String(20), nullable=False, index=True)
    form_type = Column(String(20), nullable=False, index=True)
    filing_date = Column(Date, nullable=True)

    # Section metadata
    section_number = Column(
        String(10), nullable=True, index=True
    )  # e.g., "1", "1A", "7", "8"
    section_name = Column(
        Text, nullable=False, index=True
    )  # e.g., "Business", "Risk Factors"
    section_type = Column(String(50), nullable=True, index=True)  # Normalized category

    # Content
    section_text = Column(Text, nullable=False)  # Full section text
    word_count = Column(Integer, nullable=True)

    # For later: vector embeddings for semantic search
    # embedding_vector = Column(Vector(1536), nullable=True)  # If using pgvector

    __table_args__ = (
        Index("idx_sec_sections_cik_type", "cik", "section_type"),
        Index(
            "idx_sec_sections_accession_section", "accession_number", "section_number"
        ),
    )
