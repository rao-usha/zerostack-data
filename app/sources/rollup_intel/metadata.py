"""
Roll-Up Market Intelligence — table schemas and constants.

Defines census_cbp (County Business Patterns cache) and
rollup_market_scores (scored counties for roll-up attractiveness).
"""

# ---------------------------------------------------------------------------
# NAICS descriptions for common PE roll-up verticals
# ---------------------------------------------------------------------------

NAICS_DESCRIPTIONS = {
    "621111": "Offices of Physicians (except Mental Health)",
    "621210": "Offices of Dentists",
    "621310": "Offices of Chiropractors",
    "621340": "Offices of Physical Therapists",
    "621399": "Offices of All Other Miscellaneous Health Practitioners",
    "621410": "Family Planning Centers",
    "621491": "HMO Medical Centers",
    "621610": "Home Health Care Services",
    "541330": "Engineering Services",
    "541611": "Administrative Management Consulting",
    "238220": "Plumbing, Heating & Air-Conditioning Contractors",
    "811111": "General Automotive Repair",
    "811192": "Car Washes",
    "541110": "Offices of Lawyers",
    "541211": "Offices of CPAs",
    "531210": "Offices of Real Estate Agents & Brokers",
    "722511": "Full-Service Restaurants",
    "812111": "Barber Shops",
    "812112": "Beauty Salons",
    "812199": "Other Personal Care Services",
    "541940": "Veterinary Services",
    "624410": "Child Day Care Services",
    "453910": "Pet & Pet Supplies Stores",
    "611110": "Elementary & Secondary Schools",
    "621498": "All Other Outpatient Care Centers",
}

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

MODEL_VERSION = "v1.0"

ROLLUP_WEIGHTS = {
    "fragmentation": 0.35,
    "market_size": 0.25,
    "affluence": 0.20,
    "growth": 0.10,
    "labor": 0.10,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

# Census CBP API variables
CBP_VARIABLES = ["NAICS2017", "ESTAB", "EMP", "PAYANN"]
CBP_SIZE_CLASSES = {
    "1-4":   "EMPSZES:212",
    "5-9":   "EMPSZES:220",
    "10-19": "EMPSZES:230",
    "20-49": "EMPSZES:241",
    "50-99": "EMPSZES:242",
    "100-249": "EMPSZES:251",
    "250+":  "EMPSZES:254",
}


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

def generate_create_census_cbp_sql() -> str:
    """Return CREATE TABLE + index DDL for census_cbp."""
    return """
    CREATE TABLE IF NOT EXISTS census_cbp (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        naics_code VARCHAR(6) NOT NULL,
        geo_level VARCHAR(10) NOT NULL,
        state_fips VARCHAR(2),
        county_fips VARCHAR(5),
        geo_name TEXT,
        establishments INTEGER,
        employees INTEGER,
        annual_payroll_thousands BIGINT,
        -- Size distribution
        estab_1_4 INTEGER,
        estab_5_9 INTEGER,
        estab_10_19 INTEGER,
        estab_20_49 INTEGER,
        estab_50_99 INTEGER,
        estab_100_249 INTEGER,
        estab_250_plus INTEGER,
        -- Derived
        avg_employees_per_estab NUMERIC(8,2),
        small_biz_pct NUMERIC(5,4),
        hhi NUMERIC(8,6),
        fetched_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(year, naics_code, geo_level, county_fips)
    );

    CREATE INDEX IF NOT EXISTS idx_cbp_naics ON census_cbp(naics_code);
    CREATE INDEX IF NOT EXISTS idx_cbp_county ON census_cbp(county_fips);
    CREATE INDEX IF NOT EXISTS idx_cbp_state ON census_cbp(state_fips);
    CREATE INDEX IF NOT EXISTS idx_cbp_year ON census_cbp(year);
    """


def generate_create_rollup_scores_sql() -> str:
    """Return CREATE TABLE + index DDL for rollup_market_scores."""
    return """
    CREATE TABLE IF NOT EXISTS rollup_market_scores (
        id SERIAL PRIMARY KEY,
        naics_code VARCHAR(6) NOT NULL,
        naics_description TEXT,
        county_fips VARCHAR(5) NOT NULL,
        state_fips VARCHAR(2),
        geo_name TEXT,
        score_date DATE NOT NULL,
        data_year INTEGER NOT NULL,
        -- Composite
        overall_score NUMERIC(5,2) NOT NULL,
        grade VARCHAR(1) NOT NULL,
        -- Sub-scores (each 0-100)
        fragmentation_score NUMERIC(5,2),
        market_size_score NUMERIC(5,2),
        affluence_score NUMERIC(5,2),
        growth_score NUMERIC(5,2),
        labor_score NUMERIC(5,2),
        -- Raw metrics
        establishment_count INTEGER,
        hhi NUMERIC(8,6),
        small_biz_pct NUMERIC(5,4),
        avg_estab_size NUMERIC(8,2),
        total_employees INTEGER,
        total_payroll_thousands BIGINT,
        avg_agi NUMERIC(12,2),
        pct_returns_100k_plus NUMERIC(6,4),
        total_returns INTEGER,
        -- Rankings
        national_rank INTEGER,
        state_rank INTEGER,
        model_version VARCHAR(20) DEFAULT 'v1.0',
        UNIQUE(naics_code, county_fips, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_rms_naics ON rollup_market_scores(naics_code);
    CREATE INDEX IF NOT EXISTS idx_rms_county ON rollup_market_scores(county_fips);
    CREATE INDEX IF NOT EXISTS idx_rms_state ON rollup_market_scores(state_fips);
    CREATE INDEX IF NOT EXISTS idx_rms_score ON rollup_market_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_rms_grade ON rollup_market_scores(grade);
    CREATE INDEX IF NOT EXISTS idx_rms_date ON rollup_market_scores(score_date);
    """
