"""
Prediction Market Metadata and Classification.

Defines market categories, classification logic, and alert thresholds.
"""

from typing import Optional, Dict, List

# =============================================================================
# MARKET CATEGORIES
# =============================================================================

MARKET_CATEGORIES: Dict[str, Dict] = {
    # Economics & Finance
    "fed_rates": {
        "display_name": "Federal Reserve Interest Rates",
        "parent": "economics",
        "keywords": [
            "fed",
            "fomc",
            "interest rate",
            "rate cut",
            "rate hike",
            "federal reserve",
            "powell",
        ],
        "relevant_sectors": ["all"],
        "impact_level": "high",
        "monitoring_priority": 5,
        "alert_threshold": 0.10,
    },
    "inflation": {
        "display_name": "Inflation / CPI",
        "parent": "economics",
        "keywords": ["cpi", "inflation", "consumer price", "pce", "core inflation"],
        "relevant_sectors": ["retail", "consumer", "real_estate"],
        "impact_level": "high",
        "monitoring_priority": 5,
        "alert_threshold": 0.10,
    },
    "recession": {
        "display_name": "Recession Probability",
        "parent": "economics",
        "keywords": ["recession", "gdp", "economic downturn", "economic growth"],
        "relevant_sectors": ["all"],
        "impact_level": "high",
        "monitoring_priority": 5,
        "alert_threshold": 0.10,
    },
    "unemployment": {
        "display_name": "Unemployment / Jobs",
        "parent": "economics",
        "keywords": [
            "unemployment",
            "jobs report",
            "nonfarm payroll",
            "jobless claims",
        ],
        "relevant_sectors": ["all"],
        "impact_level": "medium",
        "monitoring_priority": 4,
        "alert_threshold": 0.08,
    },
    "stock_market": {
        "display_name": "Stock Market / Indices",
        "parent": "economics",
        "keywords": ["s&p", "dow", "nasdaq", "stock market", "spy", "qqq"],
        "relevant_sectors": ["finance"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    # Politics - US
    "presidential_election": {
        "display_name": "US Presidential Election",
        "parent": "politics",
        "keywords": [
            "president",
            "presidential",
            "white house",
            "2024 election",
            "2028 election",
            "democratic nominee",
            "republican nominee",
        ],
        "relevant_sectors": ["all"],
        "impact_level": "high",
        "monitoring_priority": 5,
        "alert_threshold": 0.10,
    },
    "congressional": {
        "display_name": "Congressional Elections",
        "parent": "politics",
        "keywords": ["senate", "house", "congress", "midterm", "congressional"],
        "relevant_sectors": ["all"],
        "impact_level": "medium",
        "monitoring_priority": 4,
        "alert_threshold": 0.10,
    },
    "legislation": {
        "display_name": "Legislation & Policy",
        "parent": "politics",
        "keywords": [
            "bill",
            "legislation",
            "law",
            "act",
            "pass congress",
            "signed into law",
        ],
        "relevant_sectors": ["varies"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    "cabinet": {
        "display_name": "Cabinet & Appointments",
        "parent": "politics",
        "keywords": ["cabinet", "secretary", "appointed", "confirmation", "nominee"],
        "relevant_sectors": ["varies"],
        "impact_level": "low",
        "monitoring_priority": 2,
        "alert_threshold": 0.15,
    },
    # Geopolitics
    "geopolitics": {
        "display_name": "Geopolitics & International",
        "parent": "world",
        "keywords": [
            "war",
            "strike",
            "invasion",
            "sanctions",
            "iran",
            "russia",
            "china",
            "ukraine",
            "israel",
            "military",
            "conflict",
        ],
        "relevant_sectors": ["energy", "defense", "commodities"],
        "impact_level": "high",
        "monitoring_priority": 5,
        "alert_threshold": 0.10,
    },
    "international_leaders": {
        "display_name": "International Leaders",
        "parent": "world",
        "keywords": [
            "khamenei",
            "putin",
            "xi jinping",
            "leader",
            "regime",
            "supreme leader",
            "prime minister",
        ],
        "relevant_sectors": ["all"],
        "impact_level": "medium",
        "monitoring_priority": 4,
        "alert_threshold": 0.10,
    },
    # Sports
    "nfl": {
        "display_name": "NFL Football",
        "parent": "sports",
        "keywords": [
            "nfl",
            "football",
            "super bowl",
            "touchdown",
            "quarterback",
            "chiefs",
            "eagles",
            "bills",
            "49ers",
            "cowboys",
            "packers",
            "bears",
            "rams",
            "jaguars",
        ],
        "relevant_sectors": ["entertainment", "media"],
        "impact_level": "low",
        "monitoring_priority": 2,
        "alert_threshold": 0.15,
    },
    "nba": {
        "display_name": "NBA Basketball",
        "parent": "sports",
        "keywords": [
            "nba",
            "basketball",
            "lakers",
            "celtics",
            "warriors",
            "cavaliers",
            "timberwolves",
            "clippers",
            "pistons",
        ],
        "relevant_sectors": ["entertainment", "media"],
        "impact_level": "low",
        "monitoring_priority": 2,
        "alert_threshold": 0.15,
    },
    "mlb": {
        "display_name": "MLB Baseball",
        "parent": "sports",
        "keywords": ["mlb", "baseball", "world series", "yankees", "dodgers"],
        "relevant_sectors": ["entertainment", "media"],
        "impact_level": "low",
        "monitoring_priority": 2,
        "alert_threshold": 0.15,
    },
    "sports_other": {
        "display_name": "Other Sports",
        "parent": "sports",
        "keywords": [
            "ufc",
            "mma",
            "boxing",
            "tennis",
            "golf",
            "soccer",
            "hockey",
            "nhl",
        ],
        "relevant_sectors": ["entertainment", "media"],
        "impact_level": "low",
        "monitoring_priority": 1,
        "alert_threshold": 0.20,
    },
    # Crypto
    "bitcoin": {
        "display_name": "Bitcoin",
        "parent": "crypto",
        "keywords": ["bitcoin", "btc", "satoshi"],
        "relevant_sectors": ["crypto", "finance"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    "ethereum": {
        "display_name": "Ethereum",
        "parent": "crypto",
        "keywords": ["ethereum", "eth", "vitalik"],
        "relevant_sectors": ["crypto", "finance"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    "crypto_other": {
        "display_name": "Other Crypto",
        "parent": "crypto",
        "keywords": [
            "solana",
            "cardano",
            "ripple",
            "xrp",
            "dogecoin",
            "altcoin",
            "defi",
        ],
        "relevant_sectors": ["crypto", "finance"],
        "impact_level": "low",
        "monitoring_priority": 2,
        "alert_threshold": 0.15,
    },
    # Business
    "company_earnings": {
        "display_name": "Company Earnings",
        "parent": "business",
        "keywords": [
            "earnings",
            "revenue",
            "eps",
            "beat estimates",
            "miss estimates",
            "quarterly report",
        ],
        "relevant_sectors": ["varies"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    "mergers_acquisitions": {
        "display_name": "M&A / Acquisitions",
        "parent": "business",
        "keywords": ["acquisition", "merger", "acquire", "buy", "takeover", "deal"],
        "relevant_sectors": ["varies"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    "tech_products": {
        "display_name": "Tech Products & Launches",
        "parent": "business",
        "keywords": [
            "launch",
            "release",
            "announce",
            "iphone",
            "android",
            "ai",
            "chatgpt",
            "openai",
        ],
        "relevant_sectors": ["technology"],
        "impact_level": "low",
        "monitoring_priority": 2,
        "alert_threshold": 0.15,
    },
    # Climate & Weather
    "climate": {
        "display_name": "Climate & Weather",
        "parent": "environment",
        "keywords": [
            "temperature",
            "weather",
            "climate",
            "hurricane",
            "storm",
            "drought",
            "flood",
        ],
        "relevant_sectors": ["agriculture", "insurance", "energy"],
        "impact_level": "medium",
        "monitoring_priority": 3,
        "alert_threshold": 0.10,
    },
    # Entertainment
    "entertainment": {
        "display_name": "Entertainment & Pop Culture",
        "parent": "entertainment",
        "keywords": ["oscar", "emmy", "grammy", "movie", "film", "celebrity", "award"],
        "relevant_sectors": ["entertainment", "media"],
        "impact_level": "low",
        "monitoring_priority": 1,
        "alert_threshold": 0.20,
    },
}


# =============================================================================
# CATEGORY CLASSIFICATION
# =============================================================================


def categorize_market(question: str, description: str = "") -> Dict[str, Optional[str]]:
    """
    Automatically categorize a market based on its question and description.

    Uses keyword matching to assign category and subcategory.

    Args:
        question: The market question text
        description: Optional market description

    Returns:
        Dict with 'category', 'subcategory', 'impact_level', 'monitoring_priority', 'alert_threshold'
    """
    text = f"{question} {description}".lower()

    best_match = None
    best_score = 0

    for subcategory, config in MARKET_CATEGORIES.items():
        score = 0
        for keyword in config["keywords"]:
            if keyword.lower() in text:
                # Longer keywords get more weight
                score += len(keyword.split())

        if score > best_score:
            best_score = score
            best_match = subcategory

    if best_match:
        config = MARKET_CATEGORIES[best_match]
        return {
            "category": config["parent"],
            "subcategory": best_match,
            "impact_level": config["impact_level"],
            "monitoring_priority": config["monitoring_priority"],
            "alert_threshold": config["alert_threshold"],
        }

    # Default for unclassified markets
    return {
        "category": "other",
        "subcategory": None,
        "impact_level": "low",
        "monitoring_priority": 1,
        "alert_threshold": 0.20,
    }


def get_alert_threshold(category: str, subcategory: Optional[str] = None) -> float:
    """
    Get the probability change threshold that triggers an alert.

    Args:
        category: Main category
        subcategory: Specific subcategory

    Returns:
        Float threshold (e.g., 0.10 = 10% change)
    """
    if subcategory and subcategory in MARKET_CATEGORIES:
        return MARKET_CATEGORIES[subcategory]["alert_threshold"]

    # Default thresholds by parent category
    category_defaults = {
        "economics": 0.10,
        "politics": 0.10,
        "world": 0.10,
        "sports": 0.15,
        "crypto": 0.10,
        "business": 0.10,
        "environment": 0.10,
        "entertainment": 0.20,
    }

    return category_defaults.get(category, 0.15)


def get_high_priority_categories() -> List[str]:
    """
    Get list of high-priority categories for focused monitoring.

    Returns:
        List of subcategory names with priority >= 4
    """
    return [
        name
        for name, config in MARKET_CATEGORIES.items()
        if config["monitoring_priority"] >= 4
    ]


# =============================================================================
# KALSHI SERIES MAPPING
# =============================================================================

# Kalshi organizes markets into "series" - these are the key ones to monitor
KALSHI_SERIES = {
    # Economics
    "FED": {
        "category": "economics",
        "subcategory": "fed_rates",
        "description": "Federal Reserve rate decisions",
    },
    "CPI": {
        "category": "economics",
        "subcategory": "inflation",
        "description": "Consumer Price Index",
    },
    "UNRATE": {
        "category": "economics",
        "subcategory": "unemployment",
        "description": "Unemployment rate",
    },
    "GDP": {
        "category": "economics",
        "subcategory": "recession",
        "description": "GDP growth",
    },
    "INXD": {
        "category": "economics",
        "subcategory": "stock_market",
        "description": "Stock indices",
    },
    # Politics
    "PRES": {
        "category": "politics",
        "subcategory": "presidential_election",
        "description": "Presidential election",
    },
    "SENATE": {
        "category": "politics",
        "subcategory": "congressional",
        "description": "Senate control",
    },
    "HOUSE": {
        "category": "politics",
        "subcategory": "congressional",
        "description": "House control",
    },
    # Crypto
    "BTCUSD": {
        "category": "crypto",
        "subcategory": "bitcoin",
        "description": "Bitcoin price",
    },
    "ETHUSD": {
        "category": "crypto",
        "subcategory": "ethereum",
        "description": "Ethereum price",
    },
    # Climate
    "CLIMATE": {
        "category": "environment",
        "subcategory": "climate",
        "description": "Climate/weather",
    },
}


def get_kalshi_series_list() -> List[str]:
    """Get list of Kalshi series tickers to monitor."""
    return list(KALSHI_SERIES.keys())
