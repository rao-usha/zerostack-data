"""
Family Office tracking models.

This is separate from SEC Form ADV data - this table tracks ALL family offices
regardless of registration status, and includes manually researched data.
"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Boolean, ARRAY, JSON
from sqlalchemy.sql import func
from app.core.models import Base


class FamilyOffice(Base):
    """
    General family office tracking table.
    
    This table stores information about family offices from various sources:
    - Manual research
    - Public sources (websites, press releases)
    - LinkedIn profiles
    - SEC Form ADV (if registered)
    - 13F filings
    - News articles
    
    Unlike sec_form_adv (only registered advisers), this table tracks ALL
    family offices regardless of registration status.
    """
    __tablename__ = "family_offices"
    
    # Primary identifiers
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(500), nullable=False, unique=True, index=True)
    legal_name = Column(String(500))
    
    # Classification
    region = Column(String(50), index=True)  # US, Europe, Asia, LatAm, Middle East
    country = Column(String(100))
    type = Column(String(100))  # Single Family Office, Multi-Family Office, etc.
    
    # Contact Information
    headquarters_address = Column(Text)
    city = Column(String(200))
    state_province = Column(String(100))
    postal_code = Column(String(20))
    
    main_phone = Column(String(50))
    main_email = Column(String(200))
    website = Column(String(500))
    linkedin = Column(String(500))
    
    # Key Contacts (JSONB for flexibility)
    key_contacts = Column(JSON)  
    # Structure: [
    #   {
    #     "name": "John Doe",
    #     "title": "Chief Investment Officer",
    #     "email": "jdoe@example.com",
    #     "phone": "+1-555-1234",
    #     "linkedin": "https://linkedin.com/in/johndoe"
    #   }
    # ]
    
    # Family/Principal Information
    principal_family = Column(String(500))  # e.g., "Gates Family", "Soros"
    principal_name = Column(String(500))  # Main family member
    estimated_wealth = Column(String(100))  # e.g., "$100B+", "$10-50B"
    
    # Investment Profile
    investment_focus = Column(ARRAY(String))  
    # e.g., ["Private Equity", "Venture Capital", "Real Estate", "Public Equities"]
    
    sectors_of_interest = Column(ARRAY(String))  
    # e.g., ["AI/ML", "Healthcare", "Climate Tech", "Fintech"]
    
    geographic_focus = Column(ARRAY(String))  
    # e.g., ["North America", "Europe", "Asia Pacific"]
    
    stage_preference = Column(ARRAY(String))  
    # e.g., ["Seed", "Series A-B", "Growth", "Late Stage"]
    
    check_size_range = Column(String(100))  # e.g., "$1M-$10M", "$10M-$50M"
    
    # Investment Philosophy
    investment_thesis = Column(Text)  # Free-form notes about their approach
    notable_investments = Column(ARRAY(String))  # List of known investments
    
    # Data Sources & Verification
    data_sources = Column(ARRAY(String))  
    # e.g., ["Company Website", "LinkedIn", "13F Filing", "News Article"]
    
    sec_crd_number = Column(String(50))  # If registered, link to Form ADV
    sec_registered = Column(Boolean, default=False)
    
    # Assets & Scale
    estimated_aum = Column(String(100))  # Assets under management estimate
    employee_count = Column(String(50))  # e.g., "10-50", "100+"
    
    # Status & Activity
    status = Column(String(50), default='Active')  # Active, Inactive, Unknown
    actively_investing = Column(Boolean)
    accepts_outside_capital = Column(Boolean, default=False)
    
    # Metadata
    first_researched_date = Column(Date)
    last_updated_date = Column(Date)
    last_verified_date = Column(Date)
    
    notes = Column(Text)  # Free-form notes
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<FamilyOffice {self.name} ({self.region})>"


class FamilyOfficeContact(Base):
    """
    Detailed contact information for family office personnel.
    
    Separate table for normalized contact storage.
    """
    __tablename__ = "family_office_contacts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    family_office_id = Column(Integer, nullable=False, index=True)
    
    # Personal Information
    full_name = Column(String(300), nullable=False)
    title = Column(String(300))
    role = Column(String(100))  # CIO, Partner, Analyst, etc.
    
    # Contact Details
    email = Column(String(200))
    phone = Column(String(50))
    linkedin_url = Column(String(500))
    
    # Professional Background
    bio = Column(Text)
    previous_experience = Column(ARRAY(String))
    education = Column(ARRAY(String))
    
    # Areas of Focus
    investment_areas = Column(ARRAY(String))  # Their specific areas
    sectors = Column(ARRAY(String))
    
    # Status
    is_primary_contact = Column(Boolean, default=False)
    status = Column(String(50), default='Active')  # Active, Left, Unknown
    
    # Data Quality
    data_source = Column(String(200))  # Where this info came from
    last_verified = Column(Date)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<FamilyOfficeContact {self.full_name} - {self.title}>"


class FamilyOfficeInvestment(Base):
    """
    Track family office investments and deal activity.

    Stores information about investments collected from:
    - News articles
    - Press releases
    - SEC Form D filings
    - Crunchbase/PitchBook (public data)
    """
    __tablename__ = "family_office_investment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    family_office_id = Column(Integer, nullable=False, index=True)

    # Company/Investment Target
    company_name = Column(String(500), nullable=False, index=True)
    company_website = Column(String(500))

    # Investment Details
    investment_date = Column(Date, index=True)
    investment_type = Column(String(50))  # venture, private_equity, real_estate, etc.
    investment_stage = Column(String(50))  # seed, series_a, growth, buyout, etc.
    investment_amount_usd = Column(String(50))  # Using string for flexibility

    # Ownership & Role
    ownership_pct = Column(String(20))  # Percentage owned
    board_seat = Column(Boolean)
    lead_investor = Column(Boolean)

    # Status & Exit
    status = Column(String(50))  # active, exited, written_off
    exit_date = Column(Date)
    exit_type = Column(String(50))  # ipo, acquisition, secondary, etc.
    exit_multiple = Column(String(20))

    # Data Source
    source_type = Column(String(50))  # news, sec_form_d, website, etc.
    source_url = Column(Text)

    # Timestamps
    collected_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<FamilyOfficeInvestment {self.company_name} by FO {self.family_office_id}>"


class FamilyOfficeInteraction(Base):
    """
    Track interactions, outreach, and engagement with family offices.
    """
    __tablename__ = "family_office_interactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    family_office_id = Column(Integer, nullable=False, index=True)
    contact_id = Column(Integer)  # Optional: specific contact
    
    interaction_date = Column(Date, nullable=False)
    interaction_type = Column(String(100))  # Email, Call, Meeting, Conference, etc.
    
    subject = Column(String(500))
    notes = Column(Text)
    outcome = Column(String(200))  # Follow-up scheduled, Not interested, etc.
    
    next_action = Column(Text)
    next_action_date = Column(Date)
    
    created_by = Column(String(200))  # User who logged this
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    def __repr__(self):
        return f"<Interaction {self.interaction_type} on {self.interaction_date}>"

