"""
Med-Spa Discovery — table schema and scoring constants.

Defines the medspa_prospects table (Yelp businesses scored as acquisition
prospects) and the weighting/grading parameters for the acquisition model.
Includes enrichment column migrations (NPPES, revenue, density) and
the medspa_prospect_snapshots table for review velocity tracking.
"""

# ---------------------------------------------------------------------------
# Search & category constants
# ---------------------------------------------------------------------------

DEFAULT_SEARCH_TERMS = ["med spa", "medical spa"]
MEDSPA_CATEGORIES = "medicalspa,skincare,laser_hair_removal,cosmeticsurgeons,dayspas"
MODEL_VERSION = "v1.0"

# ---------------------------------------------------------------------------
# Acquisition prospect scoring weights (must sum to 1.0)
# ---------------------------------------------------------------------------

PROSPECT_WEIGHTS = {
    "zip_affluence": 0.30,
    "yelp_rating": 0.25,
    "review_volume": 0.20,
    "low_competition": 0.15,
    "price_tier": 0.10,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

PRICE_SCORE_MAP = {
    "$$$$": 100,
    "$$$": 75,
    "$$": 50,
    "$": 25,
}
DEFAULT_PRICE_SCORE = 37.5  # When Yelp doesn't report price


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

def generate_create_medspa_prospects_sql() -> str:
    """Return CREATE TABLE + index DDL for medspa_prospects."""
    return """
    CREATE TABLE IF NOT EXISTS medspa_prospects (
        id SERIAL PRIMARY KEY,

        -- Yelp business data (denormalized)
        yelp_id         TEXT NOT NULL,
        name            TEXT NOT NULL,
        alias           TEXT,
        rating          NUMERIC(2,1),
        review_count    INTEGER DEFAULT 0,
        price           TEXT,
        phone           TEXT,
        url             TEXT,
        image_url       TEXT,
        latitude        NUMERIC(10,7),
        longitude       NUMERIC(10,7),
        address         TEXT,
        city            TEXT,
        state           TEXT,
        zip_code        VARCHAR(10),
        categories      TEXT[],
        is_closed       BOOLEAN DEFAULT false,

        -- ZIP score context (snapshot at discovery time)
        zip_overall_score       NUMERIC(5,2),
        zip_grade               VARCHAR(1),
        zip_affluence_density   NUMERIC(5,2),
        zip_total_returns       INTEGER,
        zip_avg_agi             NUMERIC(12,2),

        -- Acquisition prospect score
        acquisition_score       NUMERIC(5,2) NOT NULL,
        acquisition_grade       VARCHAR(1) NOT NULL,

        -- Sub-scores (each 0-100)
        zip_affluence_sub       NUMERIC(5,2),
        yelp_rating_sub         NUMERIC(5,2),
        review_volume_sub       NUMERIC(5,2),
        low_competition_sub     NUMERIC(5,2),
        price_tier_sub          NUMERIC(5,2),

        -- Competition context
        competitor_count_in_zip INTEGER DEFAULT 0,

        -- Ownership classification
        ownership_type          VARCHAR(20),
        parent_entity           TEXT,
        location_count          INTEGER DEFAULT 1,
        classification_confidence NUMERIC(4,3),
        classified_at           TIMESTAMP,
        adjusted_acquisition_score NUMERIC(5,2),

        -- Discovery metadata
        search_term     TEXT,
        batch_id        TEXT,
        model_version   VARCHAR(20) DEFAULT 'v1.0',
        discovered_at   TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW(),

        UNIQUE(yelp_id)
    );

    CREATE INDEX IF NOT EXISTS idx_mp_yelp_id
        ON medspa_prospects(yelp_id);
    CREATE INDEX IF NOT EXISTS idx_mp_state
        ON medspa_prospects(state);
    CREATE INDEX IF NOT EXISTS idx_mp_zip_code
        ON medspa_prospects(zip_code);
    CREATE INDEX IF NOT EXISTS idx_mp_acquisition_score
        ON medspa_prospects(acquisition_score DESC);
    CREATE INDEX IF NOT EXISTS idx_mp_acquisition_grade
        ON medspa_prospects(acquisition_grade);
    CREATE INDEX IF NOT EXISTS idx_mp_batch_id
        ON medspa_prospects(batch_id);
    CREATE INDEX IF NOT EXISTS idx_mp_ownership_type
        ON medspa_prospects(ownership_type);
    CREATE INDEX IF NOT EXISTS idx_mp_parent_entity
        ON medspa_prospects(parent_entity);
    """


def generate_ownership_migration_sql() -> str:
    """Return idempotent ALTER TABLE DDL to add ownership columns to existing table."""
    return """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'medspa_prospects' AND column_name = 'ownership_type'
        ) THEN
            ALTER TABLE medspa_prospects
                ADD COLUMN ownership_type VARCHAR(20),
                ADD COLUMN parent_entity TEXT,
                ADD COLUMN location_count INTEGER DEFAULT 1,
                ADD COLUMN classification_confidence NUMERIC(4,3),
                ADD COLUMN classified_at TIMESTAMP,
                ADD COLUMN adjusted_acquisition_score NUMERIC(5,2);
            CREATE INDEX IF NOT EXISTS idx_mp_ownership_type
                ON medspa_prospects(ownership_type);
            CREATE INDEX IF NOT EXISTS idx_mp_parent_entity
                ON medspa_prospects(parent_entity);
        END IF;
    END $$;
    """


def generate_enrichment_migration_sql() -> str:
    """Return idempotent ALTER TABLE DDL to add enrichment columns (NPPES, revenue, density)."""
    return """
    DO $$
    BEGIN
        -- NPPES Medical Provider Cross-Reference columns
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'medspa_prospects' AND column_name = 'has_physician_oversight'
        ) THEN
            ALTER TABLE medspa_prospects
                ADD COLUMN has_physician_oversight BOOLEAN,
                ADD COLUMN nppes_provider_count INTEGER DEFAULT 0,
                ADD COLUMN nppes_provider_credentials TEXT[],
                ADD COLUMN nppes_match_confidence NUMERIC(4,3),
                ADD COLUMN medical_director_name TEXT,
                ADD COLUMN nppes_enriched_at TIMESTAMP;
        END IF;

        -- Revenue Estimation columns
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'medspa_prospects' AND column_name = 'estimated_annual_revenue'
        ) THEN
            ALTER TABLE medspa_prospects
                ADD COLUMN estimated_annual_revenue NUMERIC(12,2),
                ADD COLUMN revenue_estimate_low NUMERIC(12,2),
                ADD COLUMN revenue_estimate_high NUMERIC(12,2),
                ADD COLUMN revenue_confidence VARCHAR(20),
                ADD COLUMN revenue_model_version VARCHAR(20) DEFAULT 'v1.0',
                ADD COLUMN revenue_estimated_at TIMESTAMP;
        END IF;

        -- Enhanced Competitive Density columns
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'medspa_prospects' AND column_name = 'zip_total_filers'
        ) THEN
            ALTER TABLE medspa_prospects
                ADD COLUMN zip_total_filers INTEGER,
                ADD COLUMN medspas_per_10k_filers NUMERIC(6,2),
                ADD COLUMN market_saturation_index VARCHAR(20),
                ADD COLUMN density_enriched_at TIMESTAMP;
        END IF;

        -- Website Scraping columns (Phase 2A)
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'medspa_prospects' AND column_name = 'website_url'
        ) THEN
            ALTER TABLE medspa_prospects
                ADD COLUMN website_url TEXT,
                ADD COLUMN services_offered TEXT[],
                ADD COLUMN has_botox BOOLEAN,
                ADD COLUMN has_fillers BOOLEAN,
                ADD COLUMN has_laser BOOLEAN,
                ADD COLUMN has_coolsculpting BOOLEAN,
                ADD COLUMN has_prp BOOLEAN,
                ADD COLUMN has_iv_therapy BOOLEAN,
                ADD COLUMN provider_bios JSONB,
                ADD COLUMN website_scraped_at TIMESTAMP;
        END IF;

        -- Review velocity columns
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'medspa_prospects' AND column_name = 'review_velocity_30d'
        ) THEN
            ALTER TABLE medspa_prospects
                ADD COLUMN review_velocity_30d NUMERIC(6,2),
                ADD COLUMN rating_trend NUMERIC(4,2);
        END IF;
    END $$;
    """


def generate_snapshot_table_sql() -> str:
    """Return CREATE TABLE DDL for medspa_prospect_snapshots (review velocity tracking)."""
    return """
    CREATE TABLE IF NOT EXISTS medspa_prospect_snapshots (
        id SERIAL PRIMARY KEY,
        yelp_id TEXT NOT NULL,
        snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
        review_count INTEGER,
        rating NUMERIC(2,1),
        is_closed BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT NOW(),

        UNIQUE(yelp_id, snapshot_date)
    );

    CREATE INDEX IF NOT EXISTS idx_mps_yelp_id
        ON medspa_prospect_snapshots(yelp_id);
    CREATE INDEX IF NOT EXISTS idx_mps_snapshot_date
        ON medspa_prospect_snapshots(snapshot_date);
    """


# ---------------------------------------------------------------------------
# Revenue estimation constants
# ---------------------------------------------------------------------------

REVENUE_BENCHMARKS = {
    "$":    400_000,
    "$$":   700_000,
    "$$$":  1_200_000,
    "$$$$": 2_000_000,
    None:   600_000,
}

REVENUE_MODEL_VERSION = "v1.0"

# National median AGI (approx, from IRS SOI data)
NATIONAL_MEDIAN_AGI = 45_000.0

# Market saturation thresholds (medspas per 10k tax filers)
SATURATION_THRESHOLDS = [
    (1.0, "Undersaturated"),
    (2.5, "Balanced"),
    (5.0, "Saturated"),
    (float("inf"), "Oversaturated"),
]
