"""
Datacenter Site Suitability Score — table schema.

Stores multi-domain suitability scores for US counties across 6 dimensions:
power infrastructure, connectivity, regulatory speed, labor/workforce,
risk/environment, and cost/incentives.
"""


def generate_create_datacenter_site_scores_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS datacenter_site_scores (
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

        -- Domain scores (each 0-100)
        power_score                 NUMERIC(5,2),
        connectivity_score          NUMERIC(5,2),
        regulatory_score            NUMERIC(5,2),
        labor_score                 NUMERIC(5,2),
        risk_score                  NUMERIC(5,2),
        cost_incentive_score        NUMERIC(5,2),

        -- Key raw metrics
        power_capacity_nearby_mw    NUMERIC(12,2),
        substations_count           INTEGER,
        electricity_price_cents_kwh NUMERIC(6,2),
        ix_count                    INTEGER,
        dc_facility_count           INTEGER,
        broadband_coverage_pct      NUMERIC(5,2),
        regulatory_speed_score      NUMERIC(5,2),
        tech_employment             INTEGER,
        tech_avg_wage               NUMERIC(10,2),
        flood_risk_rating           NUMERIC(5,2),
        brownfield_sites            INTEGER,
        incentive_program_count     INTEGER,
        opportunity_zone            BOOLEAN DEFAULT FALSE,
        renewable_ghi               NUMERIC(6,2),
        transmission_line_count     INTEGER,
        mean_elevation_ft           NUMERIC(10,2),
        flood_high_risk_zones       INTEGER,
        wetland_acres               NUMERIC(12,2),

        model_version               VARCHAR(20) DEFAULT 'v1.0',
        created_at                  TIMESTAMP DEFAULT NOW(),

        UNIQUE(county_fips, score_date)
    );

    CREATE INDEX IF NOT EXISTS idx_dss_county_fips
        ON datacenter_site_scores(county_fips);
    CREATE INDEX IF NOT EXISTS idx_dss_score_date
        ON datacenter_site_scores(score_date DESC);
    CREATE INDEX IF NOT EXISTS idx_dss_overall
        ON datacenter_site_scores(overall_score DESC);
    CREATE INDEX IF NOT EXISTS idx_dss_grade
        ON datacenter_site_scores(grade);
    CREATE INDEX IF NOT EXISTS idx_dss_state
        ON datacenter_site_scores(state);
    """


DOMAIN_DOCUMENTATION = {
    "power_infrastructure": {
        "weight": 0.30,
        "description": "Nearby power generation capacity, substation density, "
                       "and electricity pricing. Critical for data center operations "
                       "that require reliable, affordable power.",
        "source_tables": ["power_plant", "substation", "electricity_price"],
    },
    "connectivity": {
        "weight": 0.20,
        "description": "Internet exchange presence, existing datacenter cluster "
                       "density, and broadband infrastructure. Network connectivity "
                       "is essential for low-latency operations.",
        "source_tables": ["internet_exchange", "data_center_facility", "broadband_availability"],
    },
    "regulatory_speed": {
        "weight": 0.20,
        "description": "Pre-computed county regulatory speed score from Phase 2. "
                       "Incorporates permit velocity, jurisdictional simplicity, "
                       "and DC-friendly governance history.",
        "source_tables": ["county_regulatory_scores"],
    },
    "labor_workforce": {
        "weight": 0.15,
        "description": "Tech-sector employment concentration (NAICS 518210) "
                       "and wage competitiveness for datacenter operations staff.",
        "source_tables": ["industry_employment"],
    },
    "risk_environment": {
        "weight": 0.10,
        "description": "Natural hazard exposure (FEMA NRI), flood zone presence, "
                       "and environmental facility density. Lower risk = higher score.",
        "source_tables": ["flood_zone", "environmental_facility"],
    },
    "cost_incentives": {
        "weight": 0.05,
        "description": "Electricity cost, tax incentive programs, and opportunity "
                       "zone overlay. Economic incentives reduce total cost of ownership.",
        "source_tables": ["electricity_price", "incentive_program", "opportunity_zone"],
    },
}
