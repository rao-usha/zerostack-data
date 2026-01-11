"""
Yelp Fusion metadata and schema generation utilities.

Handles:
- Table name generation for Yelp datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for business data
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# Business listings schema
BUSINESS_COLUMNS = {
    "yelp_id": ("TEXT", "Yelp business ID"),
    "name": ("TEXT", "Business name"),
    "alias": ("TEXT", "Business URL alias"),
    "image_url": ("TEXT", "Main image URL"),
    "is_closed": ("BOOLEAN", "Whether business is permanently closed"),
    "url": ("TEXT", "Yelp business page URL"),
    "review_count": ("INTEGER", "Number of reviews"),
    "rating": ("NUMERIC(2,1)", "Average rating (1-5)"),
    "latitude": ("NUMERIC(10,7)", "Latitude coordinate"),
    "longitude": ("NUMERIC(10,7)", "Longitude coordinate"),
    "price": ("TEXT", "Price level ($, $$, $$$, $$$$)"),
    "phone": ("TEXT", "Phone number"),
    "display_phone": ("TEXT", "Formatted phone number"),
    "distance": ("NUMERIC(12,2)", "Distance from search location in meters"),
    "address1": ("TEXT", "Street address"),
    "address2": ("TEXT", "Address line 2"),
    "address3": ("TEXT", "Address line 3"),
    "city": ("TEXT", "City"),
    "state": ("TEXT", "State code"),
    "zip_code": ("TEXT", "ZIP/postal code"),
    "country": ("TEXT", "Country code"),
    "categories": ("TEXT[]", "Array of category aliases"),
    "category_titles": ("TEXT[]", "Array of category display names"),
    "transactions": ("TEXT[]", "Available transactions (delivery, pickup, etc.)"),
    "search_location": ("TEXT", "Original search location"),
    "search_term": ("TEXT", "Original search term"),
}

# Category schema
CATEGORY_COLUMNS = {
    "alias": ("TEXT", "Category alias (ID)"),
    "title": ("TEXT", "Category display name"),
    "parent_aliases": ("TEXT[]", "Parent category aliases"),
    "country_whitelist": ("TEXT[]", "Countries where category is available"),
    "country_blacklist": ("TEXT[]", "Countries where category is not available"),
}

# Business reviews schema (limited data on free tier)
REVIEW_COLUMNS = {
    "review_id": ("TEXT", "Yelp review ID"),
    "business_id": ("TEXT", "Associated business ID"),
    "rating": ("INTEGER", "Review rating (1-5)"),
    "text": ("TEXT", "Review text (first 160 chars on free tier)"),
    "time_created": ("TIMESTAMP", "Review creation time"),
    "user_id": ("TEXT", "Reviewer user ID"),
    "user_name": ("TEXT", "Reviewer display name"),
    "user_image_url": ("TEXT", "Reviewer profile image URL"),
}


def generate_table_name(dataset: str, identifier: Optional[str] = None) -> str:
    """
    Generate PostgreSQL table name for Yelp data.
    
    Args:
        dataset: Dataset identifier (businesses, categories, reviews)
        identifier: Optional specific identifier (e.g., location, category)
        
    Returns:
        PostgreSQL table name
        
    Examples:
        >>> generate_table_name("businesses")
        'yelp_businesses'
        >>> generate_table_name("businesses", "san_francisco")
        'yelp_businesses_san_francisco'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")
    
    if identifier:
        id_clean = identifier.lower().replace("-", "_").replace(" ", "_").replace(",", "")[:30]
        return f"yelp_{dataset_clean}_{id_clean}"
    else:
        return f"yelp_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for Yelp data.
    
    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema
        
    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "businesses":
        columns = BUSINESS_COLUMNS
        unique_constraint = "yelp_id"
        indexes = [
            ("yelp_id", "yelp_id"),
            ("city_state", "city, state"),
            ("rating", "rating"),
            ("review_count", "review_count"),
            ("search_location", "search_location"),
        ]
    elif dataset == "categories":
        columns = CATEGORY_COLUMNS
        unique_constraint = "alias"
        indexes = [
            ("alias", "alias"),
            ("title", "title"),
        ]
    elif dataset == "reviews":
        columns = REVIEW_COLUMNS
        unique_constraint = "review_id"
        indexes = [
            ("review_id", "review_id"),
            ("business_id", "business_id"),
            ("rating", "rating"),
            ("time_created", "time_created"),
        ]
    else:
        raise ValueError(f"Unknown Yelp dataset: {dataset}")
    
    # Build column definitions
    column_defs = []
    column_defs.append("id SERIAL PRIMARY KEY")
    
    for col_name, (col_type, _) in columns.items():
        column_defs.append(f"{col_name} {col_type}")
    
    column_defs.append("ingested_at TIMESTAMP DEFAULT NOW()")
    
    columns_sql = ",\n        ".join(column_defs)
    
    # Build indexes
    index_sql_parts = []
    for idx_name, idx_cols in indexes:
        index_sql_parts.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idx_name} "
            f"ON {table_name} ({idx_cols});"
        )
    
    indexes_sql = "\n    ".join(index_sql_parts)
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql},
        CONSTRAINT {table_name}_unique UNIQUE ({unique_constraint})
    );
    
    {indexes_sql}
    """
    
    return sql


def parse_business_search_response(
    response: Dict[str, Any],
    search_location: str,
    search_term: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Parse business search response from Yelp API.
    
    Args:
        response: Raw API response
        search_location: Original search location
        search_term: Original search term
        
    Returns:
        List of parsed business records ready for insertion
    """
    parsed_records = []
    
    try:
        businesses = response.get("businesses", [])
        
        if not businesses:
            logger.warning("No businesses in Yelp response")
            return []
        
        for biz in businesses:
            try:
                location = biz.get("location", {})
                coordinates = biz.get("coordinates", {})
                categories = biz.get("categories", [])
                
                parsed = {
                    "yelp_id": biz.get("id"),
                    "name": biz.get("name"),
                    "alias": biz.get("alias"),
                    "image_url": biz.get("image_url"),
                    "is_closed": biz.get("is_closed", False),
                    "url": biz.get("url"),
                    "review_count": biz.get("review_count", 0),
                    "rating": _safe_float(biz.get("rating")),
                    "latitude": _safe_float(coordinates.get("latitude")),
                    "longitude": _safe_float(coordinates.get("longitude")),
                    "price": biz.get("price"),
                    "phone": biz.get("phone"),
                    "display_phone": biz.get("display_phone"),
                    "distance": _safe_float(biz.get("distance")),
                    "address1": location.get("address1"),
                    "address2": location.get("address2"),
                    "address3": location.get("address3"),
                    "city": location.get("city"),
                    "state": location.get("state"),
                    "zip_code": location.get("zip_code"),
                    "country": location.get("country"),
                    "categories": [c.get("alias") for c in categories if c.get("alias")],
                    "category_titles": [c.get("title") for c in categories if c.get("title")],
                    "transactions": biz.get("transactions", []),
                    "search_location": search_location,
                    "search_term": search_term,
                }
                
                if parsed["yelp_id"]:
                    parsed_records.append(parsed)
            
            except Exception as e:
                logger.warning(f"Failed to parse business: {e}")
        
        logger.info(f"Parsed {len(parsed_records)} business records")
        return parsed_records
    
    except Exception as e:
        logger.error(f"Failed to parse business search response: {e}")
        return []


def parse_categories_response(
    response: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Parse categories response from Yelp API.
    
    Args:
        response: Raw API response
        
    Returns:
        List of parsed category records ready for insertion
    """
    parsed_records = []
    
    try:
        categories = response.get("categories", [])
        
        if not categories:
            logger.warning("No categories in Yelp response")
            return []
        
        for cat in categories:
            try:
                parsed = {
                    "alias": cat.get("alias"),
                    "title": cat.get("title"),
                    "parent_aliases": cat.get("parent_aliases", []),
                    "country_whitelist": cat.get("country_whitelist", []),
                    "country_blacklist": cat.get("country_blacklist", []),
                }
                
                if parsed["alias"]:
                    parsed_records.append(parsed)
            
            except Exception as e:
                logger.warning(f"Failed to parse category: {e}")
        
        logger.info(f"Parsed {len(parsed_records)} category records")
        return parsed_records
    
    except Exception as e:
        logger.error(f"Failed to parse categories response: {e}")
        return []


def parse_reviews_response(
    response: Dict[str, Any],
    business_id: str
) -> List[Dict[str, Any]]:
    """
    Parse reviews response from Yelp API.
    
    Args:
        response: Raw API response
        business_id: Associated business ID
        
    Returns:
        List of parsed review records ready for insertion
    """
    parsed_records = []
    
    try:
        reviews = response.get("reviews", [])
        
        if not reviews:
            logger.warning("No reviews in Yelp response")
            return []
        
        for review in reviews:
            try:
                user = review.get("user", {})
                
                parsed = {
                    "review_id": review.get("id"),
                    "business_id": business_id,
                    "rating": review.get("rating"),
                    "text": review.get("text"),
                    "time_created": review.get("time_created"),
                    "user_id": user.get("id"),
                    "user_name": user.get("name"),
                    "user_image_url": user.get("image_url"),
                }
                
                if parsed["review_id"]:
                    parsed_records.append(parsed)
            
            except Exception as e:
                logger.warning(f"Failed to parse review: {e}")
        
        logger.info(f"Parsed {len(parsed_records)} review records")
        return parsed_records
    
    except Exception as e:
        logger.error(f"Failed to parse reviews response: {e}")
        return []


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "businesses": "Yelp Business Listings",
        "categories": "Yelp Business Categories",
        "reviews": "Yelp Business Reviews",
    }
    return display_names.get(dataset, f"Yelp {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "businesses": (
            "Business listings from Yelp including name, location, "
            "ratings, reviews count, categories, and contact information."
        ),
        "categories": (
            "Yelp business categories and their hierarchical relationships."
        ),
        "reviews": (
            "Business reviews from Yelp (limited to 3 per business on free tier)."
        ),
    }
    return descriptions.get(
        dataset,
        "Business data from Yelp Fusion API"
    )


# Major US cities for business data collection
US_CITIES = [
    "New York, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "San Jose, CA",
    "Austin, TX",
    "Jacksonville, FL",
    "Fort Worth, TX",
    "Columbus, OH",
    "San Francisco, CA",
    "Charlotte, NC",
    "Indianapolis, IN",
    "Seattle, WA",
    "Denver, CO",
    "Boston, MA",
    "Nashville, TN",
    "Portland, OR",
    "Las Vegas, NV",
    "Miami, FL",
    "Atlanta, GA",
]
