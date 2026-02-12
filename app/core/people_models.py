"""
People & Org Chart Intelligence Platform - Database Models.

This module defines tables for tracking corporate leadership, organizational
structures, and executive movements across industrial companies.

Core People Tables (5):
- people: Master person records
- company_people: Person-company relationships with hierarchy
- people_experience: Work history
- people_education: Education records
- people_collection_jobs: Track collection runs

Company Tables (1):
- industrial_companies: Company master for industrial vertical

Org Chart Tables (2):
- org_chart_snapshots: Point-in-time org structures
- leadership_changes: Executive movements tracking

PE Feature Tables (6):
- people_portfolios: PE portfolio definitions
- people_portfolio_companies: Portfolio membership
- people_peer_sets: Company peer group definitions
- people_peer_set_members: Peer set membership
- people_watchlists: User watchlists for exec tracking
- people_watchlist_people: People in watchlists
"""

from sqlalchemy import (
    Column, Integer, String, Text, Date, DateTime, Boolean,
    Numeric, JSON, ForeignKey, Index, UniqueConstraint, CheckConstraint
)
from sqlalchemy.sql import func
from app.core.models import Base


# =============================================================================
# CORE PEOPLE TABLES
# =============================================================================

class Person(Base):
    """
    Master person records.

    Stores comprehensive information about executives, managers, and board members.
    LinkedIn URL is used as canonical identifier for deduplication.
    """
    __tablename__ = "people"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    full_name = Column(String(500), nullable=False, index=True)
    first_name = Column(String(200))
    last_name = Column(String(200), index=True)
    middle_name = Column(String(200))
    suffix = Column(String(50))  # Jr., III, PhD, etc.

    # Contact
    email = Column(String(300))
    email_confidence = Column(String(20))  # verified, inferred, guessed
    phone = Column(String(50))

    # Location
    city = Column(String(200))
    state = Column(String(100))
    country = Column(String(100), default="USA")

    # Social/External
    linkedin_url = Column(String(500), unique=True, index=True)
    linkedin_id = Column(String(100))  # Extracted from URL
    twitter_url = Column(String(500))
    personal_website = Column(String(500))
    photo_url = Column(String(500))

    # Bio
    bio = Column(Text)
    bio_source = Column(String(100))  # website, linkedin, sec_filing

    # Demographics (if available from SEC)
    birth_year = Column(Integer)
    age_as_of_date = Column(Date)

    # Data Quality
    data_sources = Column(JSON)  # ["website", "sec_proxy", "linkedin"]
    confidence_score = Column(Numeric(3, 2))  # 0.00 to 1.00
    last_verified_date = Column(Date)
    last_enriched_date = Column(Date)

    # Deduplication
    canonical_id = Column(Integer, ForeignKey("people.id"))  # Points to master if duplicate
    is_canonical = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_people_name_search", "last_name", "first_name"),
        Index("ix_people_canonical", "canonical_id"),
        CheckConstraint("full_name <> ''", name="ck_people_name_not_empty"),
    )

    def __repr__(self):
        return f"<Person {self.full_name}>"


class IndustrialCompany(Base):
    """
    Company master for industrial vertical.

    Stores comprehensive information about industrial distributors, manufacturers,
    and related companies. Supports public, private, and PE-backed companies.
    """
    __tablename__ = "industrial_companies"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    name = Column(String(500), nullable=False, unique=True, index=True)
    legal_name = Column(String(500))
    dba_names = Column(JSON)  # ["doing business as" names]

    # Website
    website = Column(String(500))
    leadership_page_url = Column(String(500))
    careers_page_url = Column(String(500))
    newsroom_url = Column(String(500))

    # Location
    headquarters_address = Column(Text)
    headquarters_city = Column(String(200))
    headquarters_state = Column(String(100))
    headquarters_country = Column(String(100), default="USA")

    # Classification
    industry_segment = Column(String(200), index=True)  # distribution, manufacturing, oem
    sub_segment = Column(String(200))  # fasteners, bearings, electrical, etc.
    naics_code = Column(String(10))
    sic_code = Column(String(10))

    # Size
    employee_count = Column(Integer)
    employee_count_range = Column(String(50))  # "100-500", "1000-5000"
    employee_count_source = Column(String(100))
    revenue_usd = Column(Numeric(15, 2))
    revenue_range = Column(String(50))  # "$100M-$500M"
    revenue_source = Column(String(100))

    # Ownership
    ownership_type = Column(String(50), index=True)  # public, private, pe_backed, employee_owned
    ticker = Column(String(20), index=True)
    stock_exchange = Column(String(50))
    cik = Column(String(20), index=True)  # SEC identifier
    pe_sponsor = Column(String(200))
    pe_acquisition_date = Column(Date)

    # Parent/Subsidiary
    parent_company_id = Column(Integer, ForeignKey("industrial_companies.id"))
    is_subsidiary = Column(Boolean, default=False)

    # Status
    status = Column(String(50), default="active")  # active, acquired, bankrupt, inactive
    founded_year = Column(Integer)

    # Data Quality
    data_sources = Column(JSON)
    last_crawled_date = Column(Date)
    leadership_last_updated = Column(Date)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_industrial_companies_ownership", "ownership_type"),
        Index("ix_industrial_companies_segment", "industry_segment", "sub_segment"),
    )

    def __repr__(self):
        return f"<IndustrialCompany {self.name}>"


class CompanyPerson(Base):
    """
    Person-company relationships with hierarchy.

    Links people to their roles at companies, including reporting relationships,
    board memberships, and compensation data.
    """
    __tablename__ = "company_people"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    company_id = Column(Integer, ForeignKey("industrial_companies.id"), nullable=False, index=True)
    person_id = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Role
    title = Column(String(500), nullable=False)
    title_normalized = Column(String(200))  # Standardized: "CEO", "CFO", "VP Sales"
    title_level = Column(String(50), index=True)  # c_suite, vp, director, manager, individual
    department = Column(String(200))  # sales, operations, finance, hr, it, marketing
    function_area = Column(String(200))  # More specific: "inside sales", "field sales"

    # Hierarchy
    reports_to_id = Column(Integer, ForeignKey("company_people.id"))
    management_level = Column(Integer)  # 1 = CEO, 2 = C-suite, 3 = VP, etc.
    direct_reports_count = Column(Integer)

    # Board
    is_board_member = Column(Boolean, default=False)
    is_board_chair = Column(Boolean, default=False)
    board_committee = Column(String(200))  # audit, compensation, nominating

    # Employment
    is_current = Column(Boolean, default=True, index=True)
    is_founder = Column(Boolean, default=False)
    start_date = Column(Date)
    end_date = Column(Date)
    tenure_months = Column(Integer)  # Calculated field

    # Compensation (for public companies from proxy)
    base_salary_usd = Column(Numeric(12, 2))
    total_compensation_usd = Column(Numeric(15, 2))
    equity_awards_usd = Column(Numeric(15, 2))
    compensation_year = Column(Integer)

    # Contact at Company
    work_email = Column(String(300))
    work_phone = Column(String(50))
    office_location = Column(String(200))

    # Data Quality
    source = Column(String(100))  # website, sec_proxy, linkedin, press_release
    source_url = Column(String(500))
    extraction_date = Column(Date)
    confidence = Column(String(20), default="medium")  # high, medium, low

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("company_id", "person_id", "title", "is_current", name="uq_company_people"),
        Index("ix_company_people_current", "company_id", "is_current"),
        Index("ix_company_people_level", "company_id", "title_level"),
        Index("ix_company_people_reports", "reports_to_id"),
    )

    def __repr__(self):
        return f"<CompanyPerson {self.person_id} @ {self.company_id}: {self.title}>"


class PersonExperience(Base):
    """
    Work history for people.

    Tracks all positions held, including those at companies not in our database.
    """
    __tablename__ = "people_experience"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key
    person_id = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Company (may not be in our DB)
    company_name = Column(String(500), nullable=False)
    company_id = Column(Integer, ForeignKey("industrial_companies.id"))  # If in our DB

    # Role
    title = Column(String(500), nullable=False)
    title_normalized = Column(String(200))
    department = Column(String(200))

    # Tenure
    start_date = Column(Date)
    start_year = Column(Integer)  # If only year known
    end_date = Column(Date)
    end_year = Column(Integer)
    is_current = Column(Boolean, default=False)
    duration_months = Column(Integer)

    # Details
    description = Column(Text)
    location = Column(String(200))

    # Source
    source = Column(String(100))  # linkedin, sec_filing, bio

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("person_id", "company_name", "title", "start_year", name="uq_experience"),
        Index("ix_experience_person", "person_id"),
        Index("ix_experience_company", "company_name"),
        Index("ix_experience_current", "is_current"),
    )

    def __repr__(self):
        return f"<PersonExperience {self.person_id} @ {self.company_name}>"


class PersonEducation(Base):
    """
    Education records for people.
    """
    __tablename__ = "people_education"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key
    person_id = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Institution
    institution = Column(String(500), nullable=False)
    institution_type = Column(String(100))  # university, business_school, law_school

    # Degree
    degree = Column(String(200))  # MBA, BS, BA, JD, PhD
    degree_type = Column(String(50))  # bachelors, masters, doctorate, certificate
    field_of_study = Column(String(300))

    # Dates
    start_year = Column(Integer)
    graduation_year = Column(Integer)

    # Honors
    honors = Column(String(300))  # Summa Cum Laude, Valedictorian
    gpa = Column(String(20))

    # Activities
    activities = Column(Text)
    athletics = Column(String(200))

    # Source
    source = Column(String(100))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_education_person", "person_id"),
        Index("ix_education_institution", "institution"),
    )

    def __repr__(self):
        return f"<PersonEducation {self.person_id} @ {self.institution}>"


# =============================================================================
# ORG CHART TABLES
# =============================================================================

class OrgChartSnapshot(Base):
    """
    Point-in-time organizational structures.

    Stores complete org chart as JSON for historical tracking.
    """
    __tablename__ = "org_chart_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key
    company_id = Column(Integer, ForeignKey("industrial_companies.id"), nullable=False, index=True)

    # Snapshot
    snapshot_date = Column(Date, nullable=False, index=True)

    # Org Chart Data (JSON structure)
    chart_data = Column(JSON, nullable=False)
    # Example structure:
    # {
    #     "root": {
    #         "person_id": 123,
    #         "name": "John Smith",
    #         "title": "CEO",
    #         "children": [...]
    #     },
    #     "metadata": {...}
    # }

    # Metadata
    total_people = Column(Integer)
    max_depth = Column(Integer)
    departments = Column(JSON)  # List of departments represented

    # Source
    source = Column(String(100))  # website, inferred, manual
    source_url = Column(String(500))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("company_id", "snapshot_date", name="uq_org_snapshot"),
        Index("ix_org_chart_company_date", "company_id", "snapshot_date"),
    )

    def __repr__(self):
        return f"<OrgChartSnapshot Company {self.company_id} @ {self.snapshot_date}>"


class LeadershipChange(Base):
    """
    Executive movements tracking.

    Records all leadership changes: hires, departures, promotions, etc.
    """
    __tablename__ = "leadership_changes"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    company_id = Column(Integer, ForeignKey("industrial_companies.id"), nullable=False, index=True)
    person_id = Column(Integer, ForeignKey("people.id"), index=True)  # May be NULL if person not in DB yet

    # Person (denormalized for cases where person not in DB)
    person_name = Column(String(500), nullable=False)

    # Change Details
    change_type = Column(String(50), nullable=False, index=True)
    # Values: hire, departure, promotion, demotion, lateral, retirement,
    #         board_appointment, board_departure, interim, death

    old_title = Column(String(500))
    new_title = Column(String(500))
    old_company = Column(String(500))  # For hires from outside

    # Dates
    announced_date = Column(Date)
    effective_date = Column(Date, index=True)
    detected_date = Column(Date, nullable=False, server_default=func.current_date())

    # Context
    reason = Column(Text)  # Retirement, pursuing other opportunities, etc.
    successor_person_id = Column(Integer, ForeignKey("people.id"))
    predecessor_person_id = Column(Integer, ForeignKey("people.id"))

    # Source
    source_type = Column(String(100))  # press_release, 8k_filing, website_change, news
    source_url = Column(String(500))
    source_headline = Column(Text)

    # Significance
    is_c_suite = Column(Boolean, default=False)
    is_board = Column(Boolean, default=False)
    significance_score = Column(Integer)  # 1-10, higher = more significant

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("company_id", "person_name", "change_type", "effective_date", name="uq_leadership_change"),
        Index("ix_leadership_changes_company", "company_id"),
        Index("ix_leadership_changes_date", "effective_date"),
        Index("ix_leadership_changes_type", "change_type"),
        Index("ix_leadership_changes_recent", "detected_date"),
    )

    def __repr__(self):
        return f"<LeadershipChange {self.person_name} {self.change_type} @ {self.company_id}>"


class PeopleCollectionJob(Base):
    """
    Track collection runs for people data.
    """
    __tablename__ = "people_collection_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Job Type
    job_type = Column(String(100), nullable=False, index=True)  # website_crawl, sec_parse, news_scan

    # Target
    company_id = Column(Integer, ForeignKey("industrial_companies.id"))
    company_ids = Column(JSON)  # For batch jobs

    # Configuration
    config = Column(JSON)

    # Status
    status = Column(String(50), default="pending", index=True)  # pending, running, success, failed

    # Results
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))

    people_found = Column(Integer, default=0)
    people_created = Column(Integer, default=0)
    people_updated = Column(Integer, default=0)
    changes_detected = Column(Integer, default=0)

    errors = Column(JSON)
    warnings = Column(JSON)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_collection_jobs_status", "status"),
        Index("ix_collection_jobs_company", "company_id"),
    )

    def __repr__(self):
        return f"<PeopleCollectionJob {self.id} {self.job_type} - {self.status}>"


# =============================================================================
# DEDUPLICATION TABLES
# =============================================================================

class PeopleMergeCandidate(Base):
    """
    Tracks potential duplicate person records for review and merge.

    Pairs are stored with person_id_a < person_id_b to prevent duplicate pairs.
    Auto-merged pairs go straight to 'auto_merged' status; ambiguous matches
    land in 'pending' for manual review.
    """
    __tablename__ = "people_merge_candidates"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # The two people being compared (a < b to prevent duplicate pairs)
    person_id_a = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)
    person_id_b = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Match details
    match_type = Column(String(50), nullable=False)
    # Values: linkedin_url, name_exact, name_fuzzy, name_fuzzy_company
    similarity_score = Column(Numeric(4, 3))  # 0.000 to 1.000
    shared_company_ids = Column(JSON)  # List of company IDs they share
    evidence_notes = Column(Text)  # Explanation of why they matched

    # Review workflow
    status = Column(String(20), nullable=False, default="pending", index=True)
    # Values: pending, auto_merged, approved, rejected
    canonical_person_id = Column(Integer, ForeignKey("people.id"))  # Person kept after merge

    # Timestamps
    reviewed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("person_id_a", "person_id_b", name="uq_merge_candidate_pair"),
        CheckConstraint("person_id_a < person_id_b", name="ck_merge_candidate_order"),
        Index("ix_merge_candidates_status", "status"),
        Index("ix_merge_candidates_persons", "person_id_a", "person_id_b"),
    )

    def __repr__(self):
        return f"<PeopleMergeCandidate {self.person_id_a} <-> {self.person_id_b} ({self.status})>"


# =============================================================================
# PE FEATURE TABLES
# =============================================================================

class PeoplePortfolio(Base):
    """
    PE portfolio definitions for people intelligence.

    Groups companies into portfolios for tracking leadership.
    """
    __tablename__ = "people_portfolios"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    name = Column(String(500), nullable=False, unique=True, index=True)
    pe_firm = Column(String(500))
    description = Column(Text)
    portfolio_type = Column(String(50), default="pe_portfolio")  # pe_portfolio, watchlist, peer_group

    # Status
    is_active = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<PeoplePortfolio {self.name}>"


class PeoplePortfolioCompany(Base):
    """
    Portfolio membership for people intelligence.

    Links companies to portfolios.
    """
    __tablename__ = "people_portfolio_companies"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    portfolio_id = Column(Integer, ForeignKey("people_portfolios.id"), nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("industrial_companies.id"), nullable=False, index=True)

    # Investment tracking
    investment_date = Column(Date)
    exit_date = Column(Date)
    is_active = Column(Boolean, default=True)

    # Details
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    notes = Column(Text)

    __table_args__ = (
        UniqueConstraint("portfolio_id", "company_id", name="uq_people_portfolio_company"),
    )

    def __repr__(self):
        return f"<PeoplePortfolioCompany Portfolio {self.portfolio_id} - Company {self.company_id}>"


class PeoplePeerSet(Base):
    """
    Company peer group definitions for people intelligence.

    Defines comparison sets for benchmarking leadership.
    """
    __tablename__ = "people_peer_sets"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    name = Column(String(500), nullable=False)
    description = Column(Text)
    industry = Column(String(200))  # Industry filter for the peer set
    criteria = Column(JSON)  # Selection criteria (revenue range, employee count, etc.)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("ix_people_peer_sets_industry", "industry"),
    )

    def __repr__(self):
        return f"<PeoplePeerSet {self.name}>"


class PeoplePeerSetMember(Base):
    """
    Peer set membership for people intelligence.
    """
    __tablename__ = "people_peer_set_members"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    peer_set_id = Column(Integer, ForeignKey("people_peer_sets.id"), nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("industrial_companies.id"), nullable=False, index=True)

    # Details
    is_primary = Column(Boolean, default=False)  # Is this the primary company being compared
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    notes = Column(Text)

    __table_args__ = (
        UniqueConstraint("peer_set_id", "company_id", name="uq_people_peer_set_member"),
    )

    def __repr__(self):
        return f"<PeoplePeerSetMember Set {self.peer_set_id} - Company {self.company_id}>"


class PeopleWatchlist(Base):
    """
    User watchlists for exec tracking in people intelligence.
    """
    __tablename__ = "people_watchlists"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    name = Column(String(500), nullable=False, index=True)
    user_id = Column(String(200))  # For future user system
    description = Column(Text)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<PeopleWatchlist {self.name}>"


class PeopleWatchlistPerson(Base):
    """
    People in watchlists for people intelligence.
    """
    __tablename__ = "people_watchlist_people"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    watchlist_id = Column(Integer, ForeignKey("people_watchlists.id"), nullable=False, index=True)
    person_id = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Details
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    notes = Column(Text)
    tags = Column(JSON)  # ["potential_ceo", "cfo_candidate", "knows_well"]

    __table_args__ = (
        UniqueConstraint("watchlist_id", "person_id", name="uq_people_watchlist_person"),
    )

    def __repr__(self):
        return f"<PeopleWatchlistPerson Watchlist {self.watchlist_id} - Person {self.person_id}>"
