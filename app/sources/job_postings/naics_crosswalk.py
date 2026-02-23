"""
NAICS 2-digit code → BLS CES series_id crosswalk.

Maps industry sectors to their corresponding BLS Current Employment
Statistics (CES) supersector employment series for baseline comparison
in the Hiring Velocity Score.
"""

# ---------------------------------------------------------------------------
# NAICS 2-digit → BLS CES supersector series mapping
# Series format: CESssssssss001 where s = supersector code
# All series use data type 01 = All Employees, Thousands
# ---------------------------------------------------------------------------

NAICS_TO_BLS_CES: dict[str, str] = {
    # Goods-producing
    "11": "CES1000000001",  # Mining and Logging (BLS combines 11+21)
    "21": "CES1000000001",  # Mining and Logging
    "23": "CES2000000001",  # Construction
    "31": "CES3000000001",  # Manufacturing
    "32": "CES3000000001",  # Manufacturing
    "33": "CES3000000001",  # Manufacturing
    # Service-providing
    "42": "CES4142000001",  # Wholesale Trade (BLS combines 42+44-45)
    "44": "CES4142000001",  # Retail Trade
    "45": "CES4142000001",  # Retail Trade
    "48": "CES4300000001",  # Transportation and Warehousing
    "49": "CES4300000001",  # Transportation and Warehousing
    "51": "CES5000000001",  # Information
    "52": "CES5500000001",  # Financial Activities (BLS combines 52+53)
    "53": "CES5500000001",  # Real Estate
    "54": "CES6054000001",  # Professional and Business Services
    "55": "CES6054000001",  # Management of Companies
    "56": "CES6054000001",  # Administrative and Support
    "61": "CES6500000001",  # Education and Health Services
    "62": "CES6500000001",  # Health Care
    "71": "CES7000000001",  # Leisure and Hospitality
    "72": "CES7000000001",  # Accommodation and Food Services
    "81": "CES8000000001",  # Other Services
    "92": "CES9000000001",  # Government
}

# Fallback: Total Private employment (all non-government)
BLS_CES_FALLBACK = "CES0500000001"

# Human-readable labels for each series
BLS_SERIES_LABELS: dict[str, str] = {
    "CES0500000001": "Total Private",
    "CES1000000001": "Mining and Logging",
    "CES2000000001": "Construction",
    "CES3000000001": "Manufacturing",
    "CES4142000001": "Trade, Transportation, and Utilities",
    "CES4300000001": "Transportation and Warehousing",
    "CES5000000001": "Information",
    "CES5500000001": "Financial Activities",
    "CES6054000001": "Professional and Business Services",
    "CES6500000001": "Education and Health Services",
    "CES7000000001": "Leisure and Hospitality",
    "CES8000000001": "Other Services",
    "CES9000000001": "Government",
}


def get_bls_series_for_company(naics_code: str | None) -> str:
    """
    Resolve a company's NAICS code to a BLS CES series_id.

    Uses the first 2 digits of the NAICS code. Falls back to
    Total Private if the code is missing or unrecognized.
    """
    if not naics_code:
        return BLS_CES_FALLBACK
    prefix = str(naics_code).strip()[:2]
    return NAICS_TO_BLS_CES.get(prefix, BLS_CES_FALLBACK)


def get_series_label(series_id: str) -> str:
    """Return human-readable name for a BLS CES series."""
    return BLS_SERIES_LABELS.get(series_id, f"Unknown ({series_id})")
