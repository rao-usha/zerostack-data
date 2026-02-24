"""
Canonical domain classification for Nexdata tables.

Single source of truth for grouping database tables into business domains.
Used by reports, skills, and dashboards. When adding new tables, add their
prefix pattern to the appropriate domain below.

Classification uses prefix-based matching checked top-to-bottom; first
match wins. More specific domains (PE, People) are checked before broader
ones (Alt Data, Platform).
"""

from typing import List, Dict

# ---------------------------------------------------------------------------
# Color constants (duplicated from design_system to avoid circular imports)
# ---------------------------------------------------------------------------

BLUE = "#2b6cb0"
BLUE_LIGHT = "#63b3ed"
ORANGE = "#ed8936"
GREEN = "#38a169"
RED = "#e53e3e"
GRAY = "#a0aec0"
PURPLE = "#805ad5"
TEAL = "#319795"
PINK = "#d53f8c"
INDIGO = "#5a67d8"
AMBER = "#d69e2e"
PINK_LIGHT = "#ed64a6"

# ---------------------------------------------------------------------------
# Domain definitions — ORDER MATTERS (first match wins)
# ---------------------------------------------------------------------------

DOMAINS: List[Dict] = [
    {
        "key": "pe_intel",
        "label": "PE Intelligence",
        "color": TEAL,
        "prefixes": [
            "pe_", "deals", "deal_", "exit_readiness_",
            "acquisition_target_", "diligence_", "hunt_job",
        ],
    },
    {
        "key": "people",
        "label": "People & Org Charts",
        "color": PURPLE,
        "prefixes": [
            "people", "company_people", "org_chart_",
            "leadership_", "industrial_companies",
        ],
    },
    {
        "key": "family_office_lp",
        "label": "Family Office & LP",
        "color": PINK,
        "prefixes": [
            "family_office", "lp_", "investor_", "co_invest",
            "portfolio_",
        ],
    },
    {
        "key": "site_intel",
        "label": "Site Intelligence",
        "color": ORANGE,
        "prefixes": [
            "airport", "air_cargo_", "broadband_", "carrier_safety",
            "cell_tower", "climate_", "cold_storage", "commute_",
            "container_freight", "data_center_", "educational_attainment",
            "environmental_", "ev_charging", "fault_line", "fema_",
            "fbi_crime", "flood_", "freight_", "grain_elevator",
            "heavy_haul", "incentive_", "industrial_site",
            "interconnection_", "intermodal_", "internet_exchange",
            "labor_market", "lng_terminal", "motor_carrier",
            "national_risk_index", "natural_gas_", "network_latency",
            "noaa_", "occupational_wage", "opportunity_zone",
            "pipeline_", "port", "power_plant", "public_water_",
            "rail_", "renewable_", "seismic_", "site_intel_",
            "site_score", "solar_farm", "submarine_cable", "substation",
            "three_pl_", "trucking_", "utility_", "warehouse_",
            "water_", "wetland", "wind_farm", "zoning_",
        ],
    },
    {
        "key": "macro_economic",
        "label": "Macro Economic",
        "color": BLUE,
        "prefixes": [
            "fred_", "bea_", "bls_", "treasury_",
            "intl_", "data_commons_",
        ],
    },
    {
        "key": "trade_commerce",
        "label": "Trade & Commerce",
        "color": GREEN,
        "prefixes": [
            "us_trade_", "cftc_cot_", "irs_soi_", "acs5_",
            "bts_", "dunl_", "trade_gateway_", "census_variable_",
            "foreign_trade_zone",
        ],
    },
    {
        "key": "financial_regulatory",
        "label": "Financial & Regulatory",
        "color": INDIGO,
        "prefixes": [
            "sec_", "fdic_", "fcc_", "form_adv", "form_d",
        ],
    },
    {
        "key": "energy_agriculture",
        "label": "Energy & Agriculture",
        "color": AMBER,
        "prefixes": [
            "usda_", "electricity_", "eia_",
        ],
    },
    {
        "key": "real_estate",
        "label": "Real Estate",
        "color": RED,
        "prefixes": [
            "realestate_", "hud_",
        ],
    },
    {
        "key": "healthcare",
        "label": "Healthcare",
        "color": PINK_LIGHT,
        "prefixes": [
            "cms_", "fda_", "medspa_", "zip_medspa_",
        ],
    },
    {
        "key": "alt_data",
        "label": "Alternative Data",
        "color": BLUE_LIGHT,
        "prefixes": [
            "m5_", "job_posting", "app_store_", "glassdoor_",
            "github_", "prediction_market", "foot_traffic_",
            "company_web_", "company_app_", "company_ats_",
            "company_health_", "company_score", "company_enrichment",
            "competitive_", "news_", "hiring_velocity", "market_",
        ],
    },
    {
        "key": "platform",
        "label": "Platform",
        "color": GRAY,
        "prefixes": [],  # catch-all
    },
]

# ---------------------------------------------------------------------------
# Convenience lookups
# ---------------------------------------------------------------------------

DOMAIN_KEYS: List[str] = [d["key"] for d in DOMAINS]
DOMAIN_LABELS: Dict[str, str] = {d["key"]: d["label"] for d in DOMAINS}
DOMAIN_COLORS: Dict[str, str] = {d["key"]: d["color"] for d in DOMAINS}


def classify_table(name: str) -> str:
    """Return the domain key for a table name. First prefix match wins."""
    for domain in DOMAINS:
        for prefix in domain["prefixes"]:
            if name.startswith(prefix):
                return domain["key"]
    return "platform"


def get_domain_label(key: str) -> str:
    """Return the human-readable label for a domain key."""
    return DOMAIN_LABELS.get(key, "Platform")


def get_domain_color(key: str) -> str:
    """Return the hex color for a domain key."""
    return DOMAIN_COLORS.get(key, GRAY)
