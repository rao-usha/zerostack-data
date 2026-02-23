"""
Private Company Health Score — table schemas.

Stores multi-signal health scores that combine hiring momentum, web traffic,
employee sentiment, and foot traffic into a 0-100 composite score.
"""


def generate_create_company_health_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS company_health_scores (
        id                          SERIAL PRIMARY KEY,
        company_id                  INTEGER NOT NULL,
        score_date                  DATE NOT NULL,

        -- Composite
        overall_score               NUMERIC(5,2) NOT NULL,
        grade                       VARCHAR(1) NOT NULL,
        confidence                  NUMERIC(4,3) DEFAULT 0,

        -- Sub-scores (each 0-100)
        hiring_momentum_score       NUMERIC(5,2),
        web_presence_score          NUMERIC(5,2),
        employee_sentiment_score    NUMERIC(5,2),
        foot_traffic_score          NUMERIC(5,2),

        -- Raw metrics
        hiring_velocity_raw         NUMERIC(5,2),
        tranco_rank                 INTEGER,
        glassdoor_rating            NUMERIC(3,2),
        glassdoor_outlook           VARCHAR(50),
        foot_traffic_trend_pct      NUMERIC(8,4),

        -- JSONB detail fields
        metadata                    JSONB,
        signals_available           JSONB,

        -- Versioning
        model_version               VARCHAR(20) DEFAULT 'v1.0',
        created_at                  TIMESTAMP DEFAULT NOW(),

        UNIQUE(company_id, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_chs_company_id
        ON company_health_scores(company_id);
    CREATE INDEX IF NOT EXISTS idx_chs_score_date
        ON company_health_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_chs_overall
        ON company_health_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_chs_grade
        ON company_health_scores(grade);
    """


def generate_create_company_web_traffic_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS company_web_traffic (
        id              SERIAL PRIMARY KEY,
        company_id      INTEGER NOT NULL,
        domain          TEXT NOT NULL,
        tranco_rank     INTEGER,
        list_date       DATE NOT NULL,
        fetched_at      TIMESTAMP DEFAULT NOW(),

        UNIQUE(company_id, list_date)
    );

    CREATE INDEX IF NOT EXISTS idx_cwt_company_id
        ON company_web_traffic(company_id);
    CREATE INDEX IF NOT EXISTS idx_cwt_domain
        ON company_web_traffic(domain);
    """
