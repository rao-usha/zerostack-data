"""
Sector registry: maps sector slugs to their relevant data sources.

Each sector entry defines which FRED categories, BLS datasets, EIA datasets,
AFDC datasets, SIC codes, and NAICS codes apply. The Investor Intelligence API
uses this registry to know which DB tables to query for any sector deep dive.

Adding a new sector: add a key to SECTOR_REGISTRY with the same schema.
Adding a new data source type: add the key to every sector that uses it,
and update the investor_intelligence router to query it.
"""

from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Sector Registry
# ---------------------------------------------------------------------------
# Keys: slug (URL-safe, lowercase, underscores)
# Values: dict with label, description, and data source mappings

SECTOR_REGISTRY: Dict[str, Dict[str, Any]] = {
    "auto_service": {
        "label": "Auto Service & Tire Retail",
        "description": (
            "Tire retailers, quick-lube, brake/alignment shops. Exposed to EV "
            "fleet transition (brake erosion), AV ownership reduction, and ADAS "
            "calibration as a new revenue stream."
        ),
        "fred_categories": ["auto_sector", "consumer_sentiment", "economic_indicators"],
        "bls_datasets": ["auto_sector", "ces"],
        "eia_datasets": [],
        "afdc_datasets": ["ev_stations"],
        "edgar_sic_codes": ["5571", "7549", "5013"],  # Auto dealers, auto repair, auto parts
        "naics_codes": ["441", "4413", "8111", "44132"],
        "key_fred_series": ["TOTALSA", "UMCSENT", "GASREGCOVW", "UNRATE"],
        "key_bls_series": ["CES4244130001", "OEUN000000000000049302301"],
        "disruption_vectors": ["ev_adoption", "av_ownership_reduction", "adas_calibration"],
    },
    "retail": {
        "label": "Retail & Consumer Discretionary",
        "description": (
            "Brick-and-mortar and e-commerce retail. Exposed to consumer spending "
            "cycles, e-commerce shift, and labor cost inflation."
        ),
        "fred_categories": ["economic_indicators", "consumer_sentiment"],
        "bls_datasets": ["ces", "cpi"],
        "eia_datasets": [],
        "afdc_datasets": [],
        "edgar_sic_codes": ["5300", "5400", "5600", "5700", "5900"],
        "naics_codes": ["44", "45", "452", "454"],
        "key_fred_series": ["RSXFS", "UMCSENT", "CPIAUCSL", "UNRATE"],
        "key_bls_series": ["CES4200000001", "CES0500000003"],
        "disruption_vectors": ["ecommerce_shift", "labor_cost_inflation", "consumer_spending"],
    },
    "healthcare": {
        "label": "Healthcare Services",
        "description": (
            "Hospitals, outpatient clinics, physician practices, home health. "
            "Exposed to CMS reimbursement changes, labor shortages, and aging demographics."
        ),
        "fred_categories": ["economic_indicators"],
        "bls_datasets": ["ces", "oes"],
        "eia_datasets": [],
        "afdc_datasets": [],
        "edgar_sic_codes": ["8000", "8011", "8049", "8051", "8062", "8099"],
        "naics_codes": ["621", "622", "623", "624"],
        "key_fred_series": ["UNRATE", "PCE", "GDP"],
        "key_bls_series": ["CES6500000001", "OEUN000000000000029114101"],
        "disruption_vectors": ["cms_reimbursement", "labor_shortage", "aging_demographics"],
    },
    "industrials": {
        "label": "Industrials & Manufacturing",
        "description": (
            "Industrial manufacturers, equipment makers, contract manufacturers. "
            "Exposed to capacity utilization cycles, energy costs, and reshoring trends."
        ),
        "fred_categories": ["industrial_production", "economic_indicators", "commodities"],
        "bls_datasets": ["ces", "ppi"],
        "eia_datasets": ["petroleum", "electricity"],
        "afdc_datasets": [],
        "edgar_sic_codes": ["2000", "3000", "3400", "3500", "3600", "3700"],
        "naics_codes": ["31", "32", "33"],
        "key_fred_series": ["INDPRO", "TCU", "IPMAN", "DCOILWTICO"],
        "key_bls_series": ["CES3000000001", "WPSFD4"],
        "disruption_vectors": ["energy_cost", "reshoring", "automation"],
    },
    "technology": {
        "label": "Technology & Software",
        "description": (
            "Software, SaaS, cloud infrastructure, semiconductors. Exposed to "
            "interest rate cycles (growth stock valuation), talent costs, and AI disruption."
        ),
        "fred_categories": ["interest_rates", "economic_indicators"],
        "bls_datasets": ["ces", "oes"],
        "eia_datasets": [],
        "afdc_datasets": [],
        "edgar_sic_codes": ["7370", "7371", "7372", "7374", "3674"],
        "naics_codes": ["511", "517", "518", "519", "334"],
        "key_fred_series": ["DFF", "DGS10", "UNRATE"],
        "key_bls_series": ["CES6000000001", "CES0500000003"],
        "disruption_vectors": ["ai_disruption", "interest_rate_sensitivity", "talent_costs"],
    },
    "real_estate": {
        "label": "Real Estate & REITs",
        "description": (
            "Commercial RE, multifamily, industrial REITs, office. Exposed to "
            "interest rates, cap rate expansion, and WFH/e-commerce structural shifts."
        ),
        "fred_categories": ["housing_market", "interest_rates", "economic_indicators"],
        "bls_datasets": ["ces"],
        "eia_datasets": [],
        "afdc_datasets": [],
        "edgar_sic_codes": ["6500", "6512", "6552", "6798"],
        "naics_codes": ["531", "5311", "5312", "5313"],
        "key_fred_series": ["MORTGAGE30US", "HOUST", "DGS10", "CSUSHPINSA"],
        "key_bls_series": ["CES2000000001"],
        "disruption_vectors": ["interest_rate_sensitivity", "wfh_shift", "ecommerce_demand"],
    },
    "consumer_staples": {
        "label": "Consumer Staples & Food",
        "description": (
            "Grocery, household products, food & beverage manufacturing. "
            "Defensive sector; exposed to input cost inflation and private-label substitution."
        ),
        "fred_categories": ["economic_indicators", "consumer_sentiment"],
        "bls_datasets": ["ces", "cpi", "ppi"],
        "eia_datasets": [],
        "afdc_datasets": [],
        "edgar_sic_codes": ["2000", "2100", "5140", "5400", "5900"],
        "naics_codes": ["311", "312", "445", "446"],
        "key_fred_series": ["CPIAUCSL", "RSXFS", "PCE", "UMCSENT"],
        "key_bls_series": ["CES4200000001", "WPSFD4"],
        "disruption_vectors": ["input_cost_inflation", "private_label", "consumer_trade_down"],
    },
    "energy": {
        "label": "Energy — Oil, Gas & Utilities",
        "description": (
            "E&P, midstream, downstream, utilities. Exposed to commodity price "
            "cycles, energy transition capital requirements, and regulatory risk."
        ),
        "fred_categories": ["commodities", "industrial_production", "economic_indicators"],
        "bls_datasets": ["ces", "ppi"],
        "eia_datasets": ["petroleum", "natural_gas", "electricity"],
        "afdc_datasets": ["ev_stations"],
        "edgar_sic_codes": ["1311", "1381", "1382", "4911", "4924", "5170"],
        "naics_codes": ["211", "213", "221", "486"],
        "key_fred_series": ["DCOILWTICO", "DHHNGSP", "INDPRO"],
        "key_bls_series": ["CES1000000001"],
        "disruption_vectors": ["energy_transition", "commodity_cycle", "ev_demand_destruction"],
    },
    "financial_services": {
        "label": "Financial Services & Banking",
        "description": (
            "Banks, insurance, asset managers, specialty finance. Exposed to "
            "interest rate spreads, credit cycles, and fintech disruption."
        ),
        "fred_categories": ["interest_rates", "monetary_aggregates", "economic_indicators"],
        "bls_datasets": ["ces"],
        "eia_datasets": [],
        "afdc_datasets": [],
        "edgar_sic_codes": ["6020", "6022", "6035", "6141", "6311", "6321", "6411"],
        "naics_codes": ["521", "522", "523", "524", "525"],
        "key_fred_series": ["DFF", "DGS10", "M2SL", "UNRATE"],
        "key_bls_series": ["CES5500000001"],
        "disruption_vectors": ["interest_rate_spread", "credit_cycle", "fintech_disruption"],
    },
}


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_sector(slug: str) -> Dict[str, Any]:
    """Return sector config or raise KeyError."""
    if slug not in SECTOR_REGISTRY:
        raise KeyError(f"Unknown sector: '{slug}'. Available: {list(SECTOR_REGISTRY)}")
    return SECTOR_REGISTRY[slug]


def list_sectors() -> List[Dict[str, Any]]:
    """Return all sectors as a list with slug included."""
    return [{"slug": slug, **info} for slug, info in SECTOR_REGISTRY.items()]


# ---------------------------------------------------------------------------
# Report context schemas
# ---------------------------------------------------------------------------
# Maps question_type → which KPI slots map to which data fields.
# The investor intelligence router uses this to build the structured payload.

QUESTION_TYPES = {
    "disruption_analysis": {
        "label": "Disruption Analysis",
        "description": "Assess the magnitude and timeline of a structural disruption to the sector.",
        "kpi_slots": [
            {"id": "revenue_at_risk", "label": "Revenue at Risk", "source": "estimate"},
            {"id": "disruption_timeline", "label": "Disruption Timeline", "source": "estimate"},
            {"id": "ev_fleet_share", "label": "EV Fleet Share", "source": "afdc_ev_stations"},
            {"id": "sector_employment", "label": "Sector Employment", "source": "bls_auto_sector"},
            {"id": "consumer_sentiment", "label": "Consumer Sentiment", "source": "fred_consumer_sentiment"},
        ],
        "chart_types": ["adoption_curve", "revenue_waterfall", "scenario_fan"],
    },
    "market_sizing": {
        "label": "Market Sizing",
        "description": "Estimate total addressable market and share of wallet for the sector.",
        "kpi_slots": [
            {"id": "total_market_size", "label": "Total Market Size", "source": "fred"},
            {"id": "sector_employment", "label": "Sector Employment", "source": "bls"},
            {"id": "retail_sales", "label": "Sector Retail Sales", "source": "fred_auto_sector"},
            {"id": "gdp_contribution", "label": "GDP Contribution %", "source": "fred_economic_indicators"},
            {"id": "growth_rate", "label": "5yr Growth Rate", "source": "fred"},
        ],
        "chart_types": ["market_size_bar", "growth_trend_line", "geographic_heatmap"],
    },
    "operations_benchmarking": {
        "label": "Operations Benchmarking",
        "description": "Benchmark a company's operating metrics against sector peers.",
        "kpi_slots": [
            {"id": "revenue_per_employee", "label": "Revenue / Employee", "source": "edgar"},
            {"id": "gross_margin", "label": "Gross Margin %", "source": "edgar"},
            {"id": "ebitda_margin", "label": "EBITDA Margin %", "source": "edgar"},
            {"id": "wage_growth", "label": "Sector Wage Growth", "source": "bls"},
            {"id": "job_openings_rate", "label": "Job Openings Rate", "source": "bls_jolts"},
        ],
        "chart_types": ["margin_waterfall", "comp_table", "wage_trend"],
    },
    "exit_readiness": {
        "label": "Exit Readiness Assessment",
        "description": "Assess macro and sector conditions for timing a PE exit.",
        "kpi_slots": [
            {"id": "credit_spread", "label": "Credit Spread (10Y)", "source": "fred_interest_rates"},
            {"id": "consumer_sentiment", "label": "Consumer Sentiment", "source": "fred_consumer_sentiment"},
            {"id": "sector_employment", "label": "Sector Employment Trend", "source": "bls"},
            {"id": "comparable_multiples", "label": "Comp EV/EBITDA", "source": "edgar"},
            {"id": "gdp_growth", "label": "GDP Growth Rate", "source": "fred_economic_indicators"},
        ],
        "chart_types": ["macro_conditions_radar", "comp_multiples_bar", "timing_window"],
    },
}


def get_question_type(question_type: str) -> Dict[str, Any]:
    """Return question type schema or raise KeyError."""
    if question_type not in QUESTION_TYPES:
        raise KeyError(
            f"Unknown question type: '{question_type}'. "
            f"Available: {list(QUESTION_TYPES)}"
        )
    return QUESTION_TYPES[question_type]
