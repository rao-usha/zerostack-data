"""
Vertical Discovery — generic DDL generator parameterized by config.

Creates per-vertical prospect tables following the medspa_prospects schema,
plus enrichment columns for NPPES, revenue, and density.
"""

from app.sources.vertical_discovery.configs import VerticalConfig


def generate_create_prospects_sql(config: VerticalConfig) -> str:
    """Return CREATE TABLE + index DDL for a vertical's prospect table."""
    t = config.table_name
    return f"""
    CREATE TABLE IF NOT EXISTS {t} (
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

        -- NPPES enrichment (healthcare verticals only)
        has_physician_oversight  BOOLEAN,
        nppes_provider_count     INTEGER DEFAULT 0,
        nppes_provider_credentials TEXT[],
        nppes_match_confidence   NUMERIC(4,3),
        medical_director_name    TEXT,
        nppes_enriched_at        TIMESTAMP,

        -- Revenue estimation
        estimated_annual_revenue NUMERIC(12,2),
        revenue_estimate_low     NUMERIC(12,2),
        revenue_estimate_high    NUMERIC(12,2),
        revenue_confidence       VARCHAR(20),
        revenue_model_version    VARCHAR(20) DEFAULT 'v1.0',
        revenue_estimated_at     TIMESTAMP,

        -- Competitive density
        zip_total_filers         INTEGER,
        businesses_per_10k_filers NUMERIC(6,2),
        market_saturation_index  VARCHAR(20),
        density_enriched_at      TIMESTAMP,

        -- Discovery metadata
        vertical        VARCHAR(30) DEFAULT '{config.slug}',
        search_term     TEXT,
        batch_id        TEXT,
        model_version   VARCHAR(20) DEFAULT '{config.model_version}',
        discovered_at   TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW(),

        UNIQUE(yelp_id)
    );

    CREATE INDEX IF NOT EXISTS idx_{t}_yelp_id
        ON {t}(yelp_id);
    CREATE INDEX IF NOT EXISTS idx_{t}_state
        ON {t}(state);
    CREATE INDEX IF NOT EXISTS idx_{t}_zip_code
        ON {t}(zip_code);
    CREATE INDEX IF NOT EXISTS idx_{t}_acquisition_score
        ON {t}(acquisition_score DESC);
    CREATE INDEX IF NOT EXISTS idx_{t}_acquisition_grade
        ON {t}(acquisition_grade);
    CREATE INDEX IF NOT EXISTS idx_{t}_batch_id
        ON {t}(batch_id);
    CREATE INDEX IF NOT EXISTS idx_{t}_ownership_type
        ON {t}(ownership_type);
    """
