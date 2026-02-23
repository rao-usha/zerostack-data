"""
Exit Readiness Score — table schemas.

Stores multi-signal exit readiness scores for PE portfolio companies,
combining financial health, trajectory, leadership, valuation, market
position, hold period, and hiring signals into a 0-100 composite score.
"""


def generate_create_exit_readiness_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS exit_readiness_scores (
        id                          SERIAL PRIMARY KEY,
        company_id                  INTEGER NOT NULL,
        score_date                  DATE NOT NULL,

        -- Composite
        overall_score               NUMERIC(5,2) NOT NULL,
        grade                       VARCHAR(1) NOT NULL,
        confidence                  NUMERIC(4,3) DEFAULT 0,

        -- Sub-scores (each 0-100)
        financial_health_score      NUMERIC(5,2),
        financial_trajectory_score  NUMERIC(5,2),
        leadership_stability_score  NUMERIC(5,2),
        valuation_momentum_score    NUMERIC(5,2),
        market_position_score       NUMERIC(5,2),
        hold_period_score           NUMERIC(5,2),
        hiring_signal_score         NUMERIC(5,2),

        -- Raw metrics
        latest_revenue_usd          NUMERIC(18,2),
        ebitda_margin_pct           NUMERIC(8,4),
        hold_years                  NUMERIC(5,1),
        hiring_velocity_raw         NUMERIC(5,2),

        -- JSONB detail fields
        strengths                   JSONB,
        risks                       JSONB,
        metadata                    JSONB,

        -- Versioning
        model_version               VARCHAR(20) DEFAULT 'v1.0',
        created_at                  TIMESTAMP DEFAULT NOW(),

        UNIQUE(company_id, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_ers_company_id
        ON exit_readiness_scores(company_id);
    CREATE INDEX IF NOT EXISTS idx_ers_score_date
        ON exit_readiness_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_ers_overall
        ON exit_readiness_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_ers_grade
        ON exit_readiness_scores(grade);
    """
