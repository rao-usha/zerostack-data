"""
County Regulatory Speed Score — table schema.

Stores percentile-ranked regulatory speed scores for US counties,
combining permit velocity, jurisdictional simplicity, energy siting
friendliness, and historical datacenter deals.
"""


def generate_create_county_regulatory_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS county_regulatory_scores (
        id                          SERIAL PRIMARY KEY,
        county_fips                 VARCHAR(5) NOT NULL,
        county_name                 VARCHAR(255),
        state                       VARCHAR(2),
        score_date                  DATE NOT NULL,

        -- Composite
        overall_score               NUMERIC(5,2) NOT NULL,
        grade                       VARCHAR(1) NOT NULL,
        national_rank               INTEGER,
        state_rank                  INTEGER,

        -- Factor scores (each 0-100, percentile-ranked)
        permit_velocity_score       NUMERIC(5,2),
        jurisdictional_simplicity_score NUMERIC(5,2),
        energy_siting_score         NUMERIC(5,2),
        historical_dc_deals_score   NUMERIC(5,2),

        -- Raw metrics (for transparency / drill-down)
        permits_per_10k_pop         NUMERIC(8,2),
        permit_yoy_growth_pct       NUMERIC(8,2),
        total_govt_units            INTEGER,
        govts_per_10k_pop           NUMERIC(8,2),
        dc_incentive_programs       INTEGER,
        dc_disclosed_deals          INTEGER,

        model_version               VARCHAR(20) DEFAULT 'v1.0',
        created_at                  TIMESTAMP DEFAULT NOW(),

        UNIQUE(county_fips, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_crs_county_fips
        ON county_regulatory_scores(county_fips);
    CREATE INDEX IF NOT EXISTS idx_crs_score_date
        ON county_regulatory_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_crs_overall
        ON county_regulatory_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_crs_grade
        ON county_regulatory_scores(grade);
    CREATE INDEX IF NOT EXISTS idx_crs_state
        ON county_regulatory_scores(state);
    """


FACTOR_DOCUMENTATION = {
    "permit_velocity": {
        "weight": 0.30,
        "description": "Building permit issuance rate and year-over-year growth. "
                       "Higher permits per capita + positive growth = faster local approvals.",
        "source_tables": ["building_permit"],
        "signal": "permits_per_10k_pop + yoy_growth_pct",
    },
    "jurisdictional_simplicity": {
        "weight": 0.25,
        "description": "Fewer overlapping government layers = simpler permitting. "
                       "Inverted percentile: fewer govts per capita scores higher.",
        "source_tables": ["government_unit"],
        "signal": "inverted govts_per_10k_pop",
    },
    "energy_siting_friendliness": {
        "weight": 0.20,
        "description": "State has datacenter-specific incentive programs or energy "
                       "siting fast-track processes.",
        "source_tables": ["incentive_program"],
        "signal": "count of DC-specific incentive programs in state",
    },
    "historical_dc_deals": {
        "weight": 0.25,
        "description": "Past datacenter subsidy deals in the county or state "
                       "indicate a track record of DC-friendly governance.",
        "source_tables": ["incentive_deal"],
        "signal": "count of past datacenter deals in county/state",
    },
}
