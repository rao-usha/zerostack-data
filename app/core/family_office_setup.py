"""
Setup script for family office tracking tables.

Run this once to create the tables in your database.
"""
from sqlalchemy import text
from app.core.database import get_engine


def create_family_office_tables():
    """
    Create family office tracking tables.
    
    This is idempotent - safe to run multiple times.
    """
    engine = get_engine()
    
    # Main family offices table
    create_fo_sql = """
    CREATE TABLE IF NOT EXISTS family_offices (
        id SERIAL PRIMARY KEY,
        
        -- Identifiers
        name VARCHAR(500) NOT NULL UNIQUE,
        legal_name VARCHAR(500),
        
        -- Classification
        region VARCHAR(50),
        country VARCHAR(100),
        type VARCHAR(100),
        
        -- Contact Information
        headquarters_address TEXT,
        city VARCHAR(200),
        state_province VARCHAR(100),
        postal_code VARCHAR(20),
        main_phone VARCHAR(50),
        main_email VARCHAR(200),
        website VARCHAR(500),
        linkedin VARCHAR(500),
        
        -- Key Contacts (JSONB)
        key_contacts JSONB,
        
        -- Family/Principal
        principal_family VARCHAR(500),
        principal_name VARCHAR(500),
        estimated_wealth VARCHAR(100),
        
        -- Investment Profile
        investment_focus TEXT[],
        sectors_of_interest TEXT[],
        geographic_focus TEXT[],
        stage_preference TEXT[],
        check_size_range VARCHAR(100),
        
        -- Investment Philosophy
        investment_thesis TEXT,
        notable_investments TEXT[],
        
        -- Data Sources
        data_sources TEXT[],
        sec_crd_number VARCHAR(50),
        sec_registered BOOLEAN DEFAULT FALSE,
        
        -- Assets & Scale
        estimated_aum VARCHAR(100),
        employee_count VARCHAR(50),
        
        -- Status
        status VARCHAR(50) DEFAULT 'Active',
        actively_investing BOOLEAN,
        accepts_outside_capital BOOLEAN DEFAULT FALSE,
        
        -- Metadata
        first_researched_date DATE,
        last_updated_date DATE,
        last_verified_date DATE,
        notes TEXT,
        
        -- Timestamps
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE
    );
    
    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_fo_name ON family_offices(name);
    CREATE INDEX IF NOT EXISTS idx_fo_region ON family_offices(region);
    CREATE INDEX IF NOT EXISTS idx_fo_country ON family_offices(country);
    CREATE INDEX IF NOT EXISTS idx_fo_status ON family_offices(status);
    """
    
    # Contacts table
    create_contacts_sql = """
    CREATE TABLE IF NOT EXISTS family_office_contacts (
        id SERIAL PRIMARY KEY,
        family_office_id INTEGER NOT NULL,
        
        -- Personal Information
        full_name VARCHAR(300) NOT NULL,
        title VARCHAR(300),
        role VARCHAR(100),
        
        -- Contact Details
        email VARCHAR(200),
        phone VARCHAR(50),
        linkedin_url VARCHAR(500),
        
        -- Professional Background
        bio TEXT,
        previous_experience TEXT[],
        education TEXT[],
        
        -- Areas of Focus
        investment_areas TEXT[],
        sectors TEXT[],
        
        -- Status
        is_primary_contact BOOLEAN DEFAULT FALSE,
        status VARCHAR(50) DEFAULT 'Active',
        
        -- Data Quality
        data_source VARCHAR(200),
        last_verified DATE,
        
        -- Timestamps
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE,
        
        -- Foreign key
        FOREIGN KEY (family_office_id) REFERENCES family_offices(id) ON DELETE CASCADE
    );
    
    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_foc_office_id ON family_office_contacts(family_office_id);
    CREATE INDEX IF NOT EXISTS idx_foc_name ON family_office_contacts(full_name);
    CREATE INDEX IF NOT EXISTS idx_foc_role ON family_office_contacts(role);
    """
    
    # Interactions table
    create_interactions_sql = """
    CREATE TABLE IF NOT EXISTS family_office_interactions (
        id SERIAL PRIMARY KEY,
        family_office_id INTEGER NOT NULL,
        contact_id INTEGER,
        
        interaction_date DATE NOT NULL,
        interaction_type VARCHAR(100),
        
        subject VARCHAR(500),
        notes TEXT,
        outcome VARCHAR(200),
        
        next_action TEXT,
        next_action_date DATE,
        
        created_by VARCHAR(200),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        
        -- Foreign keys
        FOREIGN KEY (family_office_id) REFERENCES family_offices(id) ON DELETE CASCADE,
        FOREIGN KEY (contact_id) REFERENCES family_office_contacts(id) ON DELETE SET NULL
    );
    
    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_foi_office_id ON family_office_interactions(family_office_id);
    CREATE INDEX IF NOT EXISTS idx_foi_date ON family_office_interactions(interaction_date);
    CREATE INDEX IF NOT EXISTS idx_foi_type ON family_office_interactions(interaction_type);
    """
    
    with engine.connect() as conn:
        print("Creating family_offices table...")
        conn.execute(text(create_fo_sql))
        conn.commit()
        
        print("Creating family_office_contacts table...")
        conn.execute(text(create_contacts_sql))
        conn.commit()
        
        print("Creating family_office_interactions table...")
        conn.execute(text(create_interactions_sql))
        conn.commit()
        
        print("✅ Family office tables created successfully!")


if __name__ == "__main__":
    create_family_office_tables()

