"""
Hiring Velocity Score — table schema.

Stores computed velocity scores that cross-reference job posting
snapshots with BLS employment baselines.
"""


def generate_create_hiring_velocity_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS hiring_velocity_scores (
        id                      SERIAL PRIMARY KEY,
        company_id              INTEGER REFERENCES industrial_companies(id),
        score_date              DATE NOT NULL,

        -- Composite
        overall_score           NUMERIC(5,2) NOT NULL,
        grade                   VARCHAR(1) NOT NULL,
        confidence              NUMERIC(4,3) DEFAULT 0,

        -- Sub-scores (each 0-100)
        posting_growth_score    NUMERIC(5,2),
        industry_relative_score NUMERIC(5,2),
        momentum_score          NUMERIC(5,2),
        seniority_signal_score  NUMERIC(5,2),
        dept_diversity_score    NUMERIC(5,2),

        -- Raw metrics: posting growth
        posting_growth_rate_wow NUMERIC(8,4),
        posting_growth_rate_mom NUMERIC(8,4),

        -- Raw metrics: BLS baseline
        bls_baseline_series_id  TEXT,
        bls_baseline_growth_pct NUMERIC(8,4),
        industry_relative_rate  NUMERIC(8,4),

        -- Raw metrics: momentum
        momentum_acceleration   NUMERIC(8,4),

        -- Raw metrics: breadth
        active_departments      INTEGER,
        total_open_postings     INTEGER,

        -- JSONB detail fields
        seniority_distribution  JSONB,
        metadata                JSONB,

        -- Versioning
        model_version           VARCHAR(20) DEFAULT 'v1.0',
        created_at              TIMESTAMP DEFAULT NOW(),

        UNIQUE(company_id, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_hvs_company_id
        ON hiring_velocity_scores(company_id);
    CREATE INDEX IF NOT EXISTS idx_hvs_score_date
        ON hiring_velocity_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_hvs_overall
        ON hiring_velocity_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_hvs_grade
        ON hiring_velocity_scores(grade);
    """
