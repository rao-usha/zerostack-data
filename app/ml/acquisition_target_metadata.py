"""
Acquisition Target Score — table schemas.

Stores multi-signal acquisition attractiveness scores for PE portfolio
companies, combining growth signals, market attractiveness, management
gaps, deal activity, and sector momentum into a 0-100 composite score.
"""


def generate_create_acquisition_target_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS acquisition_target_scores (
        id                          SERIAL PRIMARY KEY,
        company_id                  INTEGER NOT NULL,
        score_date                  DATE NOT NULL,

        -- Composite
        overall_score               NUMERIC(5,2) NOT NULL,
        grade                       VARCHAR(1) NOT NULL,
        confidence                  NUMERIC(4,3) DEFAULT 0,

        -- Sub-scores (each 0-100)
        growth_signal_score         NUMERIC(5,2),
        market_attractiveness_score NUMERIC(5,2),
        management_gap_score        NUMERIC(5,2),
        deal_activity_score         NUMERIC(5,2),
        sector_momentum_score       NUMERIC(5,2),

        -- Raw metrics
        revenue_growth_pct          NUMERIC(8,4),
        employee_count              INTEGER,
        leadership_count            INTEGER,
        sector_pe_deal_count        INTEGER,

        -- JSONB detail fields
        strengths                   JSONB,
        risks                       JSONB,
        metadata                    JSONB,

        -- Versioning
        model_version               VARCHAR(20) DEFAULT 'v1.0',
        created_at                  TIMESTAMP DEFAULT NOW(),

        UNIQUE(company_id, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_ats_company_id
        ON acquisition_target_scores(company_id);
    CREATE INDEX IF NOT EXISTS idx_ats_score_date
        ON acquisition_target_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_ats_overall
        ON acquisition_target_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_ats_grade
        ON acquisition_target_scores(grade);
    """
