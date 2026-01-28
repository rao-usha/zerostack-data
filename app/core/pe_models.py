"""
PE Portfolio Intelligence Platform - Database Models.

This module defines 19 tables for comprehensive PE/VC tracking:

Core PE Firm Tables (3):
- pe_firms: GP master data
- pe_funds: Fund vehicles
- pe_fund_performance: Returns history

Portfolio Company Tables (6):
- pe_portfolio_companies: Company master
- pe_fund_investments: Ownership records
- pe_company_financials: Financial time-series
- pe_company_valuations: Valuation estimates
- pe_company_leadership: Executive team
- pe_competitor_mappings: Competitive landscape

Deal Tables (3):
- pe_deals: M&A transactions
- pe_deal_participants: Co-investors
- pe_deal_advisors: Advisory firms

People Tables (5):
- pe_people: Person master
- pe_person_education: Education history
- pe_person_experience: Work history
- pe_firm_people: Firm team members
- pe_deal_person_involvement: Deal team tracking

News Tables (2):
- pe_company_news: Company news
- pe_firm_news: Firm-level news
"""

from sqlalchemy import (
    Column, Integer, String, Text, Date, DateTime, Boolean,
    Numeric, JSON, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.sql import func
from app.core.models import Base


# =============================================================================
# CORE PE FIRM TABLES (3)
# =============================================================================

class PEFirm(Base):
    """
    PE/VC firm (General Partner) master data.

    Stores comprehensive information about private equity and venture capital
    firms including their investment focus, AUM, and SEC registration details.
    """
    __tablename__ = "pe_firms"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Basic Information
    name = Column(String(500), nullable=False, unique=True, index=True)
    legal_name = Column(String(500))
    website = Column(String(500))
    headquarters_city = Column(String(200))
    headquarters_state = Column(String(100))
    headquarters_country = Column(String(100), default="USA")

    # Classification
    firm_type = Column(String(100))  # PE, VC, Growth Equity, Credit, etc.
    primary_strategy = Column(String(200))  # Buyout, Venture, Growth, Distressed, etc.
    sector_focus = Column(JSON)  # ["Technology", "Healthcare", "Consumer"]
    geography_focus = Column(JSON)  # ["North America", "Europe", "Asia"]

    # Scale
    aum_usd_millions = Column(Numeric(15, 2))  # Assets Under Management
    employee_count = Column(Integer)
    office_locations = Column(JSON)  # List of office cities

    # Investment Parameters
    typical_check_size_min = Column(Numeric(15, 2))  # Minimum investment in millions
    typical_check_size_max = Column(Numeric(15, 2))  # Maximum investment in millions
    target_company_revenue_min = Column(Numeric(15, 2))  # Target company revenue min
    target_company_revenue_max = Column(Numeric(15, 2))  # Target company revenue max
    target_company_ebitda_min = Column(Numeric(15, 2))  # Target EBITDA min
    target_company_ebitda_max = Column(Numeric(15, 2))  # Target EBITDA max

    # SEC Registration
    cik = Column(String(20), index=True)  # SEC Central Index Key
    sec_file_number = Column(String(50))  # SEC File Number (e.g., 801-XXXXX)
    crd_number = Column(String(20))  # FINRA CRD Number
    is_sec_registered = Column(Boolean, default=False)

    # Status
    founded_year = Column(Integer)
    status = Column(String(50), default="Active")  # Active, Inactive, Acquired, etc.

    # External Links
    linkedin_url = Column(String(500))
    crunchbase_url = Column(String(500))
    pitchbook_url = Column(String(500))

    # Data Quality
    data_sources = Column(JSON)  # ["SEC", "Website", "LinkedIn", "Manual"]
    last_verified_date = Column(Date)
    confidence_score = Column(Numeric(3, 2))  # 0.00 to 1.00

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<PEFirm {self.name}>"


class PEFund(Base):
    """
    PE/VC fund vehicles.

    Tracks individual fund vintages for each firm with target sizes,
    focus areas, and status.
    """
    __tablename__ = "pe_funds"

    id = Column(Integer, primary_key=True, autoincrement=True)
    firm_id = Column(Integer, ForeignKey("pe_firms.id"), nullable=False, index=True)

    # Fund Identification
    name = Column(String(500), nullable=False, index=True)
    fund_number = Column(Integer)  # Fund I, II, III, etc.
    vintage_year = Column(Integer, index=True)

    # Fund Size
    target_size_usd_millions = Column(Numeric(15, 2))
    final_close_usd_millions = Column(Numeric(15, 2))
    called_capital_pct = Column(Numeric(5, 2))  # Percentage of capital called

    # Strategy
    strategy = Column(String(200))  # Buyout, Growth, Venture, etc.
    sector_focus = Column(JSON)  # Fund-specific sector focus
    geography_focus = Column(JSON)  # Fund-specific geography

    # Terms
    management_fee_pct = Column(Numeric(5, 2))
    carried_interest_pct = Column(Numeric(5, 2))
    preferred_return_pct = Column(Numeric(5, 2))
    fund_life_years = Column(Integer)
    investment_period_years = Column(Integer)

    # Status
    status = Column(String(50), default="Active")  # Fundraising, Active, Harvesting, Closed
    first_close_date = Column(Date)
    final_close_date = Column(Date)

    # SEC/Regulatory
    sec_file_number = Column(String(50))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_pe_funds_firm_vintage", "firm_id", "vintage_year"),
    )

    def __repr__(self):
        return f"<PEFund {self.name} ({self.vintage_year})>"


class PEFundPerformance(Base):
    """
    Fund performance history.

    Tracks IRR, TVPI, DPI, RVPI over time for each fund.
    """
    __tablename__ = "pe_fund_performance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("pe_funds.id"), nullable=False, index=True)

    # Reporting Period
    as_of_date = Column(Date, nullable=False, index=True)
    reporting_quarter = Column(String(10))  # Q1 2024, Q2 2024, etc.

    # Performance Metrics
    net_irr_pct = Column(Numeric(8, 2))  # Net IRR
    gross_irr_pct = Column(Numeric(8, 2))  # Gross IRR
    tvpi = Column(Numeric(6, 3))  # Total Value to Paid-In
    dpi = Column(Numeric(6, 3))  # Distributions to Paid-In
    rvpi = Column(Numeric(6, 3))  # Residual Value to Paid-In

    # Capital Metrics
    committed_capital = Column(Numeric(15, 2))
    called_capital = Column(Numeric(15, 2))
    distributed_capital = Column(Numeric(15, 2))
    remaining_value = Column(Numeric(15, 2))

    # Portfolio Stats
    active_investments = Column(Integer)
    realized_investments = Column(Integer)
    written_off_investments = Column(Integer)

    # Source
    data_source = Column(String(100))  # LP Report, SEC Filing, Preqin, etc.
    source_document_url = Column(String(500))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", name="uq_fund_performance_date"),
        Index("ix_fund_performance_date", "fund_id", "as_of_date"),
    )

    def __repr__(self):
        return f"<PEFundPerformance Fund {self.fund_id} @ {self.as_of_date}>"


# =============================================================================
# PORTFOLIO COMPANY TABLES (6)
# =============================================================================

class PEPortfolioCompany(Base):
    """
    Portfolio company master data.

    Companies that are or were owned by PE/VC firms.
    """
    __tablename__ = "pe_portfolio_companies"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Basic Information
    name = Column(String(500), nullable=False, index=True)
    legal_name = Column(String(500))
    website = Column(String(500))
    description = Column(Text)

    # Location
    headquarters_city = Column(String(200))
    headquarters_state = Column(String(100))
    headquarters_country = Column(String(100))

    # Classification
    industry = Column(String(200), index=True)
    sub_industry = Column(String(200))
    naics_code = Column(String(10))
    sic_code = Column(String(10))
    sector = Column(String(200))

    # Company Details
    founded_year = Column(Integer)
    employee_count = Column(Integer)
    employee_count_range = Column(String(50))  # "100-500", "1000+"

    # Ownership Status
    ownership_status = Column(String(50), index=True)  # PE-Backed, VC-Backed, Public, Private
    current_pe_owner = Column(String(500))  # Current majority PE owner name
    is_platform_company = Column(Boolean, default=False)

    # External IDs
    linkedin_url = Column(String(500))
    crunchbase_url = Column(String(500))
    ticker = Column(String(20))  # If public
    ein = Column(String(20))  # Employer Identification Number

    # Status
    status = Column(String(50), default="Active")  # Active, Exited, Bankrupt, Merged

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<PEPortfolioCompany {self.name}>"


class PEFundInvestment(Base):
    """
    Ownership records linking funds to portfolio companies.

    Tracks when a fund invested, ownership percentage, and exit details.
    """
    __tablename__ = "pe_fund_investments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("pe_funds.id"), nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)

    # Investment Details
    investment_date = Column(Date, index=True)
    investment_type = Column(String(100))  # Platform, Add-on, Growth, etc.
    investment_round = Column(String(100))  # Series A, Series B, Buyout, etc.

    # Capital Deployed
    invested_amount_usd = Column(Numeric(15, 2))
    ownership_pct = Column(Numeric(5, 2))  # Ownership percentage
    fully_diluted_pct = Column(Numeric(5, 2))

    # Entry Valuation
    entry_ev_usd = Column(Numeric(15, 2))  # Enterprise Value at entry
    entry_ev_ebitda_multiple = Column(Numeric(6, 2))
    entry_ev_revenue_multiple = Column(Numeric(6, 2))

    # Board/Control
    has_board_seat = Column(Boolean, default=False)
    board_seats = Column(Integer)
    has_control = Column(Boolean, default=False)

    # Status and Exit
    status = Column(String(50), default="Active")  # Active, Exited, Written-Off
    exit_date = Column(Date)
    exit_type = Column(String(100))  # IPO, Strategic Sale, Secondary, Recap, Write-Off
    exit_amount_usd = Column(Numeric(15, 2))
    exit_multiple = Column(Numeric(6, 2))  # Multiple of invested capital
    exit_irr_pct = Column(Numeric(8, 2))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_fund_investments_company", "company_id", "investment_date"),
    )

    def __repr__(self):
        return f"<PEFundInvestment Fund {self.fund_id} -> Company {self.company_id}>"


class PECompanyFinancials(Base):
    """
    Portfolio company financial time-series.

    Historical financials for portfolio companies.
    """
    __tablename__ = "pe_company_financials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)

    # Period
    fiscal_year = Column(Integer, nullable=False, index=True)
    fiscal_period = Column(String(20))  # "FY", "Q1", "Q2", etc.
    period_end_date = Column(Date)

    # Income Statement
    revenue_usd = Column(Numeric(15, 2))
    revenue_growth_pct = Column(Numeric(8, 2))
    gross_profit_usd = Column(Numeric(15, 2))
    gross_margin_pct = Column(Numeric(5, 2))
    ebitda_usd = Column(Numeric(15, 2))
    ebitda_margin_pct = Column(Numeric(5, 2))
    ebit_usd = Column(Numeric(15, 2))
    net_income_usd = Column(Numeric(15, 2))

    # Balance Sheet
    total_assets_usd = Column(Numeric(15, 2))
    total_debt_usd = Column(Numeric(15, 2))
    cash_usd = Column(Numeric(15, 2))
    net_debt_usd = Column(Numeric(15, 2))
    shareholders_equity_usd = Column(Numeric(15, 2))

    # Cash Flow
    operating_cash_flow_usd = Column(Numeric(15, 2))
    capex_usd = Column(Numeric(15, 2))
    free_cash_flow_usd = Column(Numeric(15, 2))

    # Ratios
    debt_to_ebitda = Column(Numeric(6, 2))
    interest_coverage = Column(Numeric(6, 2))

    # Data Quality
    is_audited = Column(Boolean, default=False)
    is_estimated = Column(Boolean, default=False)
    data_source = Column(String(200))
    confidence = Column(String(20), default="medium")  # high, medium, low

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("company_id", "fiscal_year", "fiscal_period",
                        name="uq_company_financials_period"),
    )

    def __repr__(self):
        return f"<PECompanyFinancials Company {self.company_id} FY{self.fiscal_year}>"


class PECompanyValuation(Base):
    """
    Valuation estimates for portfolio companies.

    Tracks point-in-time valuations from various sources.
    """
    __tablename__ = "pe_company_valuations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)

    # Valuation Date
    valuation_date = Column(Date, nullable=False, index=True)

    # Valuation Amounts
    enterprise_value_usd = Column(Numeric(15, 2))
    equity_value_usd = Column(Numeric(15, 2))
    net_debt_usd = Column(Numeric(15, 2))

    # Multiples
    ev_revenue_multiple = Column(Numeric(8, 2))
    ev_ebitda_multiple = Column(Numeric(8, 2))
    ev_ebit_multiple = Column(Numeric(8, 2))
    price_earnings_multiple = Column(Numeric(8, 2))

    # Methodology
    valuation_type = Column(String(100))  # Transaction, Mark-to-Market, Third-Party
    methodology = Column(String(200))  # DCF, Comparable Companies, Precedent Transactions

    # Context
    event_type = Column(String(100))  # Investment, Exit, Quarterly Mark, Fundraise
    related_deal_id = Column(Integer, ForeignKey("pe_deals.id"))

    # Data Quality
    data_source = Column(String(200))  # LP Report, Press Release, SEC Filing
    source_url = Column(String(500))
    confidence = Column(String(20), default="medium")

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<PECompanyValuation Company {self.company_id} @ {self.valuation_date}>"


class PECompanyLeadership(Base):
    """
    Executive team for portfolio companies.

    Links people to their roles at portfolio companies.
    """
    __tablename__ = "pe_company_leadership"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)
    person_id = Column(Integer, ForeignKey("pe_people.id"), nullable=False, index=True)

    # Role
    title = Column(String(300), nullable=False)
    role_category = Column(String(100))  # C-Suite, VP, Director, Board
    is_ceo = Column(Boolean, default=False)
    is_cfo = Column(Boolean, default=False)
    is_board_member = Column(Boolean, default=False)
    is_board_chair = Column(Boolean, default=False)

    # Tenure
    start_date = Column(Date)
    end_date = Column(Date)
    is_current = Column(Boolean, default=True, index=True)

    # Appointment
    appointed_by_pe = Column(Boolean)  # Was this person appointed by PE sponsor?
    pe_firm_affiliation = Column(String(500))  # If operating partner, which firm?

    # Compensation (if known)
    base_salary_usd = Column(Numeric(12, 2))
    total_comp_usd = Column(Numeric(15, 2))
    equity_pct = Column(Numeric(5, 2))

    # Data Source
    data_source = Column(String(200))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_company_leadership_current", "company_id", "is_current"),
    )

    def __repr__(self):
        return f"<PECompanyLeadership Person {self.person_id} @ Company {self.company_id}>"


class PECompetitorMapping(Base):
    """
    Competitive landscape for portfolio companies.

    Maps each portfolio company to its competitors.
    """
    __tablename__ = "pe_competitor_mappings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)

    # Competitor Info
    competitor_name = Column(String(500), nullable=False)
    competitor_company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"))  # If in our DB

    # Competitor Classification
    is_public = Column(Boolean, default=False)
    ticker = Column(String(20))
    is_pe_backed = Column(Boolean)
    pe_owner = Column(String(500))

    # Competitive Position
    competitor_type = Column(String(100))  # Direct, Indirect, Adjacent
    relative_size = Column(String(50))  # Larger, Similar, Smaller
    market_position = Column(String(100))  # Leader, Challenger, Niche

    # Notes
    notes = Column(Text)

    # Data Source
    data_source = Column(String(200))
    last_verified = Column(Date)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<PECompetitorMapping {self.competitor_name} for Company {self.company_id}>"


# =============================================================================
# DEAL TABLES (3)
# =============================================================================

class PEDeal(Base):
    """
    M&A transactions and investments.

    Every deal involving PE/VC investors.
    """
    __tablename__ = "pe_deals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)

    # Deal Basics
    deal_name = Column(String(500))
    deal_type = Column(String(100), nullable=False, index=True)  # LBO, Growth, Add-on, Exit, Recap
    deal_sub_type = Column(String(100))  # Platform, Bolt-on, Carve-out, etc.

    # Dates
    announced_date = Column(Date, index=True)
    closed_date = Column(Date, index=True)
    expected_close_date = Column(Date)

    # Deal Size
    enterprise_value_usd = Column(Numeric(15, 2))
    equity_value_usd = Column(Numeric(15, 2))
    debt_amount_usd = Column(Numeric(15, 2))

    # Valuation Multiples
    ev_revenue_multiple = Column(Numeric(8, 2))
    ev_ebitda_multiple = Column(Numeric(8, 2))
    ev_ebit_multiple = Column(Numeric(8, 2))

    # Company Financials at Deal
    ltm_revenue_usd = Column(Numeric(15, 2))  # Last Twelve Months Revenue
    ltm_ebitda_usd = Column(Numeric(15, 2))  # Last Twelve Months EBITDA

    # Deal Structure
    equity_pct = Column(Numeric(5, 2))  # Equity as % of total cap
    debt_pct = Column(Numeric(5, 2))  # Debt as % of total cap
    management_rollover_pct = Column(Numeric(5, 2))

    # Buyer/Seller
    buyer_name = Column(String(500))
    seller_name = Column(String(500))
    seller_type = Column(String(100))  # PE, Strategic, Founder, Public

    # Status
    status = Column(String(50), default="Closed")  # Announced, Pending, Closed, Terminated
    is_announced = Column(Boolean, default=True)
    is_confidential = Column(Boolean, default=False)

    # Data Source
    data_source = Column(String(200))
    source_url = Column(String(500))
    press_release_url = Column(String(500))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<PEDeal {self.deal_type} - {self.deal_name}>"


class PEDealParticipant(Base):
    """
    Investors and co-investors in deals.

    Tracks all firms involved in each deal.
    """
    __tablename__ = "pe_deal_participants"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(Integer, ForeignKey("pe_deals.id"), nullable=False, index=True)
    firm_id = Column(Integer, ForeignKey("pe_firms.id"), index=True)  # If in our DB

    # Participant Info
    participant_name = Column(String(500), nullable=False)  # Always store name
    participant_type = Column(String(100))  # PE Firm, Strategic, Family Office, etc.

    # Role
    role = Column(String(100), nullable=False)  # Lead Sponsor, Co-Investor, Seller, etc.
    is_lead = Column(Boolean, default=False)

    # Investment
    equity_contribution_usd = Column(Numeric(15, 2))
    ownership_pct = Column(Numeric(5, 2))

    # Fund Used (if known)
    fund_id = Column(Integer, ForeignKey("pe_funds.id"))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_deal_participants_firm", "firm_id", "deal_id"),
    )

    def __repr__(self):
        return f"<PEDealParticipant {self.participant_name} ({self.role})>"


class PEDealAdvisor(Base):
    """
    Advisory firms involved in deals.

    Investment banks, law firms, accounting firms, consultants.
    """
    __tablename__ = "pe_deal_advisors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(Integer, ForeignKey("pe_deals.id"), nullable=False, index=True)

    # Advisor Info
    advisor_name = Column(String(500), nullable=False)
    advisor_type = Column(String(100), nullable=False)  # Investment Bank, Law Firm, Accounting, Consulting

    # Role
    side = Column(String(50))  # Buy-Side, Sell-Side
    role_description = Column(String(200))  # Financial Advisor, Legal Counsel, Due Diligence

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<PEDealAdvisor {self.advisor_name} ({self.advisor_type})>"


# =============================================================================
# PEOPLE TABLES (5)
# =============================================================================

class PEPerson(Base):
    """
    Person master data.

    Executives, investors, board members in the PE ecosystem.
    """
    __tablename__ = "pe_people"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Name
    full_name = Column(String(500), nullable=False, index=True)
    first_name = Column(String(200))
    last_name = Column(String(200), index=True)
    middle_name = Column(String(200))
    suffix = Column(String(50))  # Jr., III, etc.

    # Contact
    email = Column(String(300))
    phone = Column(String(50))

    # Location
    city = Column(String(200))
    state = Column(String(100))
    country = Column(String(100))

    # Professional
    current_title = Column(String(300))
    current_company = Column(String(500))
    bio = Column(Text)

    # Social/External
    linkedin_url = Column(String(500), unique=True)
    twitter_url = Column(String(500))
    personal_website = Column(String(500))

    # Status
    is_active = Column(Boolean, default=True)

    # Data Quality
    data_sources = Column(JSON)
    last_verified = Column(Date)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<PEPerson {self.full_name}>"


class PEPersonEducation(Base):
    """
    Education history for people.
    """
    __tablename__ = "pe_person_education"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("pe_people.id"), nullable=False, index=True)

    # Institution
    institution = Column(String(500), nullable=False)
    institution_type = Column(String(100))  # University, Business School, Law School

    # Degree
    degree = Column(String(200))  # MBA, JD, BS, etc.
    field_of_study = Column(String(300))  # Finance, Engineering, etc.

    # Dates
    start_year = Column(Integer)
    graduation_year = Column(Integer)

    # Notes
    honors = Column(String(300))  # Summa Cum Laude, etc.
    activities = Column(Text)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<PEPersonEducation {self.institution} ({self.degree})>"


class PEPersonExperience(Base):
    """
    Work history for people.
    """
    __tablename__ = "pe_person_experience"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("pe_people.id"), nullable=False, index=True)

    # Company
    company = Column(String(500), nullable=False)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"))  # If in our DB

    # Role
    title = Column(String(300), nullable=False)
    role_level = Column(String(100))  # C-Suite, VP, Director, Manager, Analyst

    # Dates
    start_date = Column(Date)
    end_date = Column(Date)
    is_current = Column(Boolean, default=False)

    # Details
    description = Column(Text)
    location = Column(String(200))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_person_experience_current", "person_id", "is_current"),
    )

    def __repr__(self):
        return f"<PEPersonExperience {self.title} @ {self.company}>"


class PEFirmPeople(Base):
    """
    Team members at PE/VC firms.

    Links people to firms with their roles and seniority.
    """
    __tablename__ = "pe_firm_people"

    id = Column(Integer, primary_key=True, autoincrement=True)
    firm_id = Column(Integer, ForeignKey("pe_firms.id"), nullable=False, index=True)
    person_id = Column(Integer, ForeignKey("pe_people.id"), nullable=False, index=True)

    # Role
    title = Column(String(300), nullable=False)
    seniority = Column(String(100))  # Partner, Principal, VP, Associate, Analyst
    department = Column(String(200))  # Investment, Operations, IR, Finance

    # Investment Focus
    sector_focus = Column(JSON)
    geography_focus = Column(JSON)

    # Tenure
    start_date = Column(Date)
    end_date = Column(Date)
    is_current = Column(Boolean, default=True, index=True)

    # Contact
    work_email = Column(String(300))
    work_phone = Column(String(50))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_firm_people_current", "firm_id", "is_current"),
    )

    def __repr__(self):
        return f"<PEFirmPeople Person {self.person_id} @ Firm {self.firm_id}>"


class PEDealPersonInvolvement(Base):
    """
    Track which people worked on which deals.

    Deal team tracking for both buy-side and sell-side.
    """
    __tablename__ = "pe_deal_person_involvement"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(Integer, ForeignKey("pe_deals.id"), nullable=False, index=True)
    person_id = Column(Integer, ForeignKey("pe_people.id"), nullable=False, index=True)

    # Role
    role = Column(String(200))  # Deal Lead, Deal Team, Board Observer, etc.
    firm_id = Column(Integer, ForeignKey("pe_firms.id"))  # Which firm they represented

    # Side
    side = Column(String(50))  # Buy-Side, Sell-Side, Advisor

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("deal_id", "person_id", name="uq_deal_person"),
    )

    def __repr__(self):
        return f"<PEDealPersonInvolvement Person {self.person_id} on Deal {self.deal_id}>"


# =============================================================================
# NEWS TABLES (2)
# =============================================================================

class PECompanyNews(Base):
    """
    News articles about portfolio companies.
    """
    __tablename__ = "pe_company_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("pe_portfolio_companies.id"), nullable=False, index=True)

    # Article Info
    title = Column(String(1000), nullable=False)
    source_name = Column(String(200))  # Bloomberg, Reuters, WSJ, etc.
    source_url = Column(String(1000), nullable=False, unique=True)
    author = Column(String(300))

    # Content
    summary = Column(Text)
    full_text = Column(Text)

    # Dates
    published_date = Column(DateTime, index=True)
    collected_at = Column(DateTime(timezone=True), server_default=func.now())

    # Classification
    news_type = Column(String(100))  # Deal, Earnings, Management, Product, etc.
    sentiment = Column(String(50))  # Positive, Negative, Neutral
    sentiment_score = Column(Numeric(4, 3))  # -1.0 to 1.0

    # Relevance
    relevance_score = Column(Numeric(4, 3))  # 0.0 to 1.0
    is_primary = Column(Boolean, default=False)  # Company is main subject

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_company_news_date", "company_id", "published_date"),
    )

    def __repr__(self):
        return f"<PECompanyNews {self.title[:50]}...>"


class PEFirmNews(Base):
    """
    News articles about PE/VC firms.
    """
    __tablename__ = "pe_firm_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    firm_id = Column(Integer, ForeignKey("pe_firms.id"), nullable=False, index=True)

    # Article Info
    title = Column(String(1000), nullable=False)
    source_name = Column(String(200))
    source_url = Column(String(1000), nullable=False, unique=True)
    author = Column(String(300))

    # Content
    summary = Column(Text)
    full_text = Column(Text)

    # Dates
    published_date = Column(DateTime, index=True)
    collected_at = Column(DateTime(timezone=True), server_default=func.now())

    # Classification
    news_type = Column(String(100))  # Fundraise, Deal, Hire, Strategy, etc.
    sentiment = Column(String(50))
    sentiment_score = Column(Numeric(4, 3))

    # Related Entities
    related_deal_id = Column(Integer, ForeignKey("pe_deals.id"))
    related_fund_id = Column(Integer, ForeignKey("pe_funds.id"))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_firm_news_date", "firm_id", "published_date"),
    )

    def __repr__(self):
        return f"<PEFirmNews {self.title[:50]}...>"
