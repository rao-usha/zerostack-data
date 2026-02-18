"""
Metadata and configuration for foot traffic data collection.

Defines location categories, source confidence levels, and data mappings.
"""

from typing import Dict, List, Any

# =============================================================================
# LOCATION CATEGORIES
# =============================================================================

LOCATION_CATEGORIES = {
    "restaurant": {
        "name": "Restaurant",
        "subcategories": [
            "fast_food",
            "fast_casual",
            "casual_dining",
            "fine_dining",
            "coffee_shop",
            "bar",
            "cafe",
            "bakery",
            "pizza",
            "mexican",
            "asian",
            "american",
            "italian",
            "seafood",
            "steakhouse",
            "ice_cream",
            "juice_bar",
        ],
    },
    "retail": {
        "name": "Retail Store",
        "subcategories": [
            "clothing",
            "electronics",
            "grocery",
            "department_store",
            "home_improvement",
            "furniture",
            "sporting_goods",
            "pharmacy",
            "convenience",
            "dollar_store",
            "discount",
            "luxury",
            "outlet",
            "pet_store",
            "bookstore",
            "toy_store",
        ],
    },
    "office": {
        "name": "Office Building",
        "subcategories": [
            "corporate_hq",
            "coworking",
            "professional_services",
            "tech_office",
            "medical_office",
            "government",
        ],
    },
    "venue": {
        "name": "Entertainment Venue",
        "subcategories": [
            "movie_theater",
            "concert_venue",
            "sports_arena",
            "museum",
            "theme_park",
            "bowling",
            "arcade",
            "nightclub",
            "casino",
        ],
    },
    "fitness": {
        "name": "Fitness & Wellness",
        "subcategories": [
            "gym",
            "yoga_studio",
            "pilates",
            "crossfit",
            "martial_arts",
            "spa",
            "salon",
        ],
    },
    "hospitality": {
        "name": "Hospitality",
        "subcategories": [
            "hotel",
            "motel",
            "resort",
            "vacation_rental",
        ],
    },
    "healthcare": {
        "name": "Healthcare",
        "subcategories": [
            "hospital",
            "urgent_care",
            "clinic",
            "dental",
            "pharmacy",
        ],
    },
    "automotive": {
        "name": "Automotive",
        "subcategories": [
            "car_dealer",
            "gas_station",
            "auto_repair",
            "car_wash",
            "ev_charging",
        ],
    },
    "financial": {
        "name": "Financial Services",
        "subcategories": [
            "bank",
            "credit_union",
            "insurance",
            "atm",
        ],
    },
}


# =============================================================================
# SOURCE CONFIDENCE LEVELS
# =============================================================================

SOURCE_CONFIDENCE_LEVELS = {
    "safegraph": {
        "confidence": "high",
        "data_type": "absolute",  # Actual visitor counts
        "description": "Mobile location data with ~10-15% population sample",
        "refresh_frequency": "weekly",
        "historical_depth": "2+ years",
        "cost_tier": "medium",  # $100-500/month
    },
    "placer": {
        "confidence": "high",
        "data_type": "absolute",
        "description": "Retail analytics with trade area analysis",
        "refresh_frequency": "weekly",
        "historical_depth": "2+ years",
        "cost_tier": "high",  # $500-2000+/month
    },
    "foursquare": {
        "confidence": "medium",
        "data_type": "check_ins",  # Opt-in, not representative
        "description": "POI metadata and check-in data",
        "refresh_frequency": "daily",
        "historical_depth": "varies",
        "cost_tier": "low",  # Free tier + $0.01-0.05/call
    },
    "google": {
        "confidence": "medium",
        "data_type": "relative",  # 0-100 scale
        "description": "Popular Times relative traffic patterns",
        "refresh_frequency": "real-time",
        "historical_depth": "current_week_only",
        "cost_tier": "free",
        "tos_risk": "high",  # Scraping violates ToS
    },
    "city_data": {
        "confidence": "high",
        "data_type": "absolute",  # Actual sensor counts
        "description": "Public pedestrian counters in select cities",
        "refresh_frequency": "daily",
        "historical_depth": "varies_by_city",
        "cost_tier": "free",
    },
}


# =============================================================================
# POPULAR RETAIL CHAINS (for discovery)
# =============================================================================

MAJOR_RETAIL_CHAINS = {
    "fast_food": [
        "McDonald's",
        "Wendy's",
        "Burger King",
        "Taco Bell",
        "Chick-fil-A",
        "KFC",
        "Subway",
        "Domino's",
        "Pizza Hut",
        "Dunkin'",
        "Popeyes",
        "Sonic Drive-In",
        "Jack in the Box",
        "Arby's",
        "Hardee's",
    ],
    "fast_casual": [
        "Chipotle",
        "Panera Bread",
        "Sweetgreen",
        "Five Guys",
        "Shake Shack",
        "Noodles & Company",
        "Qdoba",
        "Wingstop",
        "Mod Pizza",
        "Blaze Pizza",
        "Jersey Mike's",
        "Jimmy John's",
        "Firehouse Subs",
        "Potbelly",
    ],
    "coffee": [
        "Starbucks",
        "Dunkin'",
        "Peet's Coffee",
        "Dutch Bros",
        "Caribou Coffee",
        "Tim Hortons",
        "The Coffee Bean & Tea Leaf",
    ],
    "grocery": [
        "Walmart",
        "Kroger",
        "Costco",
        "Target",
        "Albertsons",
        "Safeway",
        "Publix",
        "H-E-B",
        "Aldi",
        "Trader Joe's",
        "Whole Foods",
        "Sprouts",
    ],
    "department_store": [
        "Target",
        "Walmart",
        "Kohl's",
        "JCPenney",
        "Macy's",
        "Nordstrom",
        "Belk",
        "Dillard's",
    ],
    "home_improvement": [
        "Home Depot",
        "Lowe's",
        "Menards",
        "Ace Hardware",
    ],
    "pharmacy": [
        "CVS",
        "Walgreens",
        "Rite Aid",
    ],
    "fitness": [
        "Planet Fitness",
        "LA Fitness",
        "24 Hour Fitness",
        "Gold's Gym",
        "Anytime Fitness",
        "Equinox",
        "Orangetheory",
        "CrossFit",
    ],
}


# =============================================================================
# CITY OPEN DATA SOURCES
# =============================================================================

CITY_PEDESTRIAN_DATA_SOURCES = {
    "Seattle": {
        "base_url": "https://data.seattle.gov/resource",
        "endpoint": "pedestrian-counts.json",
        "description": "Automated pedestrian counters at major intersections",
        "availability": "2015-present",
    },
    "New York": {
        "base_url": "https://data.cityofnewyork.us/resource",
        "endpoints": {
            "turnstile": "wujg-7c2s.json",  # Subway turnstile data
            "bike": "u7pb-jz8j.json",  # Bike counters
        },
        "description": "MTA turnstile data as foot traffic proxy",
        "availability": "2010-present",
    },
    "San Francisco": {
        "base_url": "https://data.sfgov.org/resource",
        "endpoint": "pedestrian-count.json",
        "description": "Pedestrian counts at select locations",
        "availability": "2016-present",
    },
    "Chicago": {
        "base_url": "https://data.cityofchicago.org/resource",
        "endpoint": "pedestrian-counts.json",
        "description": "Pedestrian counts via sensors",
        "availability": "2017-present",
    },
    "Los Angeles": {
        "base_url": "https://data.lacity.org/resource",
        "endpoint": None,  # Limited availability
        "description": "Limited pedestrian data",
        "availability": "varies",
    },
}


# =============================================================================
# FOURSQUARE CATEGORY MAPPINGS
# =============================================================================

FOURSQUARE_CATEGORY_MAP = {
    # Restaurants
    "13000": "restaurant",
    "13001": "restaurant",  # Fast Food
    "13002": "restaurant",  # Fast Casual
    "13003": "restaurant",  # Casual Dining
    "13004": "restaurant",  # Fine Dining
    "13034": "restaurant",  # Coffee Shop
    "13035": "restaurant",  # Bar
    # Retail
    "17000": "retail",
    "17001": "retail",  # Clothing Store
    "17002": "retail",  # Electronics
    "17003": "retail",  # Grocery
    "17004": "retail",  # Department Store
    # Entertainment
    "10000": "venue",
    "10001": "venue",  # Movie Theater
    "10002": "venue",  # Concert Hall
    "10003": "venue",  # Sports Arena
    # Fitness
    "18000": "fitness",
    "18001": "fitness",  # Gym
    "18002": "fitness",  # Yoga Studio
}


# =============================================================================
# API RATE LIMITS
# =============================================================================

API_RATE_LIMITS = {
    "safegraph": {
        "requests_per_minute": 60,
        "concurrent_requests": 5,
    },
    "placer": {
        "requests_per_minute": 30,
        "concurrent_requests": 3,
    },
    "foursquare": {
        "requests_per_minute": 50,
        "concurrent_requests": 5,
    },
    "google_scraping": {
        "requests_per_minute": 12,  # 1 per 5 seconds
        "concurrent_requests": 1,
        "max_per_day": 100,
    },
    "city_data": {
        "requests_per_minute": 60,
        "concurrent_requests": 10,
    },
}


# =============================================================================
# OBSERVATION PERIODS
# =============================================================================

OBSERVATION_PERIODS = {
    "hourly": {
        "format": "%Y-%m-%d %H:00",
        "aggregation": "hour",
    },
    "daily": {
        "format": "%Y-%m-%d",
        "aggregation": "day",
    },
    "weekly": {
        "format": "%Y-W%W",
        "aggregation": "week",
    },
    "monthly": {
        "format": "%Y-%m",
        "aggregation": "month",
    },
}
