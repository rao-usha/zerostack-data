"""
Configuration and constants for the public_lp_strategies source.

This file contains:
- Known LP fund metadata (for quick registration)
- Document type constants
- Program constants
- Projection horizon constants
"""
from typing import Dict, Any, List


# =============================================================================
# LP TYPE CONSTANTS
# =============================================================================

LP_TYPE_PUBLIC_PENSION = "public_pension"
LP_TYPE_SOVEREIGN_WEALTH = "sovereign_wealth"
LP_TYPE_ENDOWMENT = "endowment"

VALID_LP_TYPES = [LP_TYPE_PUBLIC_PENSION, LP_TYPE_SOVEREIGN_WEALTH, LP_TYPE_ENDOWMENT]


# =============================================================================
# DOCUMENT TYPE CONSTANTS
# =============================================================================

DOC_TYPE_IC_PRESENTATION = "investment_committee_presentation"
DOC_TYPE_QUARTERLY_REPORT = "quarterly_investment_report"
DOC_TYPE_POLICY_STATEMENT = "policy_statement"
DOC_TYPE_PACING_PLAN = "pacing_plan"

VALID_DOCUMENT_TYPES = [
    DOC_TYPE_IC_PRESENTATION,
    DOC_TYPE_QUARTERLY_REPORT,
    DOC_TYPE_POLICY_STATEMENT,
    DOC_TYPE_PACING_PLAN,
]


# =============================================================================
# PROGRAM (ASSET CLASS / PORTFOLIO) CONSTANTS
# =============================================================================

PROGRAM_TOTAL_FUND = "total_fund"
PROGRAM_PRIVATE_EQUITY = "private_equity"
PROGRAM_REAL_ESTATE = "real_estate"
PROGRAM_INFRASTRUCTURE = "infrastructure"
PROGRAM_FIXED_INCOME = "fixed_income"
PROGRAM_PUBLIC_EQUITY = "public_equity"
PROGRAM_HEDGE_FUNDS = "hedge_funds"
PROGRAM_CASH = "cash"
PROGRAM_OTHER = "other"

VALID_PROGRAMS = [
    PROGRAM_TOTAL_FUND,
    PROGRAM_PRIVATE_EQUITY,
    PROGRAM_REAL_ESTATE,
    PROGRAM_INFRASTRUCTURE,
    PROGRAM_FIXED_INCOME,
    PROGRAM_PUBLIC_EQUITY,
    PROGRAM_HEDGE_FUNDS,
    PROGRAM_CASH,
    PROGRAM_OTHER,
]


# =============================================================================
# ASSET CLASS CONSTANTS
# =============================================================================

ASSET_CLASS_PUBLIC_EQUITY = "public_equity"
ASSET_CLASS_PRIVATE_EQUITY = "private_equity"
ASSET_CLASS_REAL_ESTATE = "real_estate"
ASSET_CLASS_FIXED_INCOME = "fixed_income"
ASSET_CLASS_INFRASTRUCTURE = "infrastructure"
ASSET_CLASS_CASH = "cash"
ASSET_CLASS_HEDGE_FUNDS = "hedge_funds"
ASSET_CLASS_OTHER = "other"

VALID_ASSET_CLASSES = [
    ASSET_CLASS_PUBLIC_EQUITY,
    ASSET_CLASS_PRIVATE_EQUITY,
    ASSET_CLASS_REAL_ESTATE,
    ASSET_CLASS_FIXED_INCOME,
    ASSET_CLASS_INFRASTRUCTURE,
    ASSET_CLASS_CASH,
    ASSET_CLASS_HEDGE_FUNDS,
    ASSET_CLASS_OTHER,
]


# =============================================================================
# PROJECTION HORIZON CONSTANTS
# =============================================================================

HORIZON_1_YEAR = "1_year"
HORIZON_3_YEAR = "3_year"
HORIZON_5_YEAR = "5_year"
HORIZON_10_YEAR = "10_year"

VALID_HORIZONS = [HORIZON_1_YEAR, HORIZON_3_YEAR, HORIZON_5_YEAR, HORIZON_10_YEAR]


# =============================================================================
# KNOWN LP FUNDS (for quick reference and bootstrapping)
# =============================================================================

KNOWN_LP_FUNDS: Dict[str, Dict[str, Any]] = {
    "CalPERS": {
        "name": "CalPERS",
        "formal_name": "California Public Employees' Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "CA",
        "website_url": "https://www.calpers.ca.gov/",
    },
    "CalSTRS": {
        "name": "CalSTRS",
        "formal_name": "California State Teachers' Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "CA",
        "website_url": "https://www.calstrs.com/",
    },
    "NYSCRF": {
        "name": "NYSCRF",
        "formal_name": "New York State Common Retirement Fund",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "NY",
        "website_url": "https://www.osc.state.ny.us/",
    },
    "Texas TRS": {
        "name": "Texas TRS",
        "formal_name": "Teacher Retirement System of Texas",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "TX",
        "website_url": "https://www.trs.texas.gov/",
    },
    "Florida SBA": {
        "name": "Florida SBA",
        "formal_name": "Florida State Board of Administration",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "FL",
        "website_url": "https://www.sbafla.com/",
    },
    "WSIB": {
        "name": "WSIB",
        "formal_name": "Washington State Investment Board",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "WA",
        "website_url": "https://www.sib.wa.gov/",
    },
    "STRS Ohio": {
        "name": "STRS Ohio",
        "formal_name": "State Teachers Retirement System of Ohio",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "OH",
        "website_url": "https://www.strsoh.org/",
    },
    "Oregon PERS": {
        "name": "Oregon PERS",
        "formal_name": "Oregon Public Employees Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "OR",
        "website_url": "https://www.oregon.gov/pers/",
    },
    "Massachusetts PRIM": {
        "name": "Massachusetts PRIM",
        "formal_name": "Massachusetts Pension Reserves Investment Management Board",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "MA",
        "website_url": "https://www.mapension.com/",
    },
    "Illinois TRS": {
        "name": "Illinois TRS",
        "formal_name": "Illinois Teachers' Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "IL",
        "website_url": "https://www.trsil.org/",
    },
    "Pennsylvania PSERS": {
        "name": "Pennsylvania PSERS",
        "formal_name": "Pennsylvania Public School Employees' Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "PA",
        "website_url": "https://www.psers.pa.gov/",
    },
    "New Jersey DI": {
        "name": "New Jersey DI",
        "formal_name": "New Jersey Division of Investment",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "NJ",
        "website_url": "https://www.state.nj.us/treasury/doinvest/",
    },
    "Ohio OPERS": {
        "name": "Ohio OPERS",
        "formal_name": "Ohio Public Employees Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "OH",
        "website_url": "https://www.opers.org/",
    },
    "North Carolina RS": {
        "name": "North Carolina RS",
        "formal_name": "North Carolina Retirement Systems",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "NC",
        "website_url": "https://www.nctreasurer.com/retirement-and-savings",
    },
    "Harvard": {
        "name": "Harvard",
        "formal_name": "Harvard Management Company",
        "lp_type": LP_TYPE_ENDOWMENT,
        "jurisdiction": "MA",
        "website_url": "https://www.hmc.harvard.edu/",
    },
    "Yale": {
        "name": "Yale",
        "formal_name": "Yale Investments Office",
        "lp_type": LP_TYPE_ENDOWMENT,
        "jurisdiction": "CT",
        "website_url": "https://investments.yale.edu/",
    },
    "Stanford": {
        "name": "Stanford",
        "formal_name": "Stanford Management Company",
        "lp_type": LP_TYPE_ENDOWMENT,
        "jurisdiction": "CA",
        "website_url": "https://smc.stanford.edu/",
    },
    "Norway GPFG": {
        "name": "Norway GPFG",
        "formal_name": "Government Pension Fund Global (Norges Bank Investment Management)",
        "lp_type": LP_TYPE_SOVEREIGN_WEALTH,
        "jurisdiction": "Norway",
        "website_url": "https://www.nbim.no/",
    },
    "GIC Singapore": {
        "name": "GIC Singapore",
        "formal_name": "GIC Private Limited",
        "lp_type": LP_TYPE_SOVEREIGN_WEALTH,
        "jurisdiction": "Singapore",
        "website_url": "https://www.gic.com.sg/",
    },
    "ADIA": {
        "name": "ADIA",
        "formal_name": "Abu Dhabi Investment Authority",
        "lp_type": LP_TYPE_SOVEREIGN_WEALTH,
        "jurisdiction": "UAE",
        "website_url": "https://www.adia.ae/",
    },
    "CPP Investments": {
        "name": "CPP Investments",
        "formal_name": "Canada Pension Plan Investment Board",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "Canada",
        "website_url": "https://www.cppinvestments.com/",
    },
    "Ontario Teachers": {
        "name": "Ontario Teachers",
        "formal_name": "Ontario Teachers' Pension Plan",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "Canada",
        "website_url": "https://www.otpp.com/",
    },
    "Wisconsin SWIB": {
        "name": "Wisconsin SWIB",
        "formal_name": "State of Wisconsin Investment Board",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "WI",
        "website_url": "https://www.swib.state.wi.us/",
    },
    "Virginia RS": {
        "name": "Virginia RS",
        "formal_name": "Virginia Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "VA",
        "website_url": "https://www.varetire.org/",
    },
    "NZ Super Fund": {
        "name": "NZ Super Fund",
        "formal_name": "New Zealand Superannuation Fund",
        "lp_type": LP_TYPE_SOVEREIGN_WEALTH,
        "jurisdiction": "New Zealand",
        "website_url": "https://www.nzsuperfund.nz/",
    },
    "OMERS": {
        "name": "OMERS",
        "formal_name": "Ontario Municipal Employees Retirement System",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "Canada",
        "website_url": "https://www.omers.com/",
    },
    "CDPQ": {
        "name": "CDPQ",
        "formal_name": "Caisse de dépôt et placement du Québec",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "Canada",
        "website_url": "https://www.cdpq.com/",
    },
    "Dutch ABP": {
        "name": "Dutch ABP",
        "formal_name": "Stichting Pensioenfonds ABP",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "Netherlands",
        "website_url": "https://www.abp.nl/",
    },
    "AustralianSuper": {
        "name": "AustralianSuper",
        "formal_name": "AustralianSuper Pty Ltd",
        "lp_type": LP_TYPE_PUBLIC_PENSION,
        "jurisdiction": "Australia",
        "website_url": "https://www.australiansuper.com/",
    },
    "Future Fund": {
        "name": "Future Fund",
        "formal_name": "Future Fund Board of Guardians",
        "lp_type": LP_TYPE_SOVEREIGN_WEALTH,
        "jurisdiction": "Australia",
        "website_url": "https://www.futurefund.gov.au/",
    },
}


# =============================================================================
# THEMATIC TAG CONSTANTS
# =============================================================================

THEME_AI = "ai"
THEME_ENERGY_TRANSITION = "energy_transition"
THEME_CLIMATE_RESILIENCE = "climate_resilience"
THEME_RESHORING = "reshoring"
THEME_HEALTHCARE = "healthcare"
THEME_TECHNOLOGY = "technology"
THEME_SUSTAINABILITY = "sustainability"
THEME_INFRASTRUCTURE = "infrastructure"

VALID_THEMES = [
    THEME_AI,
    THEME_ENERGY_TRANSITION,
    THEME_CLIMATE_RESILIENCE,
    THEME_RESHORING,
    THEME_HEALTHCARE,
    THEME_TECHNOLOGY,
    THEME_SUSTAINABILITY,
    THEME_INFRASTRUCTURE,
]


# =============================================================================
# FILE FORMAT CONSTANTS
# =============================================================================

FILE_FORMAT_PDF = "pdf"
FILE_FORMAT_PPTX = "pptx"
FILE_FORMAT_HTML = "html"
FILE_FORMAT_DOCX = "docx"

VALID_FILE_FORMATS = [FILE_FORMAT_PDF, FILE_FORMAT_PPTX, FILE_FORMAT_HTML, FILE_FORMAT_DOCX]


# =============================================================================
# FISCAL QUARTER CONSTANTS
# =============================================================================

FISCAL_Q1 = "Q1"
FISCAL_Q2 = "Q2"
FISCAL_Q3 = "Q3"
FISCAL_Q4 = "Q4"

VALID_FISCAL_QUARTERS = [FISCAL_Q1, FISCAL_Q2, FISCAL_Q3, FISCAL_Q4]


