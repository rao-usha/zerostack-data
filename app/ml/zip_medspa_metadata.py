"""
ZIP Med-Spa Revenue Potential Score — table schema.

Stores percentile-ranked revenue potential scores for every US ZIP code,
combining affluence density, discretionary wealth, market size, professional
density, and wealth concentration signals from IRS SOI data.
"""


def generate_create_zip_medspa_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS zip_medspa_scores (
        id                          SERIAL PRIMARY KEY,
        zip_code                    VARCHAR(5) NOT NULL,
        state_abbr                  VARCHAR(2),
        score_date                  DATE NOT NULL,

        -- Composite
        overall_score               NUMERIC(5,2) NOT NULL,
        grade                       VARCHAR(1) NOT NULL,
        confidence                  NUMERIC(4,3) DEFAULT 0,

        -- Sub-scores (each 0-100, percentile-ranked)
        affluence_density_score     NUMERIC(5,2),
        discretionary_wealth_score  NUMERIC(5,2),
        market_size_score           NUMERIC(5,2),
        professional_density_score  NUMERIC(5,2),
        wealth_concentration_score  NUMERIC(5,2),

        -- Raw metrics (for transparency / drill-down)
        pct_returns_100k_plus       NUMERIC(6,4),
        pct_returns_200k_plus       NUMERIC(6,4),
        avg_agi                     NUMERIC(12,2),
        total_returns               INTEGER,
        cap_gains_per_return        NUMERIC(12,2),
        dividends_per_return        NUMERIC(12,2),
        total_market_income         NUMERIC(15,2),
        partnership_density         NUMERIC(6,4),
        self_employment_density     NUMERIC(6,4),
        joint_pct_top_bracket       NUMERIC(6,4),
        amt_per_return              NUMERIC(12,2),

        model_version               VARCHAR(20) DEFAULT 'v1.0',
        created_at                  TIMESTAMP DEFAULT NOW(),

        UNIQUE(zip_code, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_zms_zip_code
        ON zip_medspa_scores(zip_code);
    CREATE INDEX IF NOT EXISTS idx_zms_score_date
        ON zip_medspa_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_zms_overall
        ON zip_medspa_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_zms_grade
        ON zip_medspa_scores(grade);
    CREATE INDEX IF NOT EXISTS idx_zms_state
        ON zip_medspa_scores(state_abbr);
    """
