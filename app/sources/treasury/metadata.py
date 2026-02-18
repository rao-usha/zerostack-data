"""
Treasury FiscalData metadata utilities.

Handles:
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def generate_table_name(dataset: str) -> str:
    """
    Generate table name for Treasury dataset.

    Convention: treasury_{dataset}

    Args:
        dataset: Dataset name (e.g., "daily_balance", "debt_outstanding")

    Returns:
        Table name (e.g., "treasury_daily_balance")
    """
    from app.sources.treasury.client import TREASURY_DATASETS

    if dataset in TREASURY_DATASETS:
        return TREASURY_DATASETS[dataset]["table_name"]

    # Sanitize dataset name
    sanitized = dataset.lower().replace("-", "_").replace(" ", "_")
    return f"treasury_{sanitized}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for Treasury data.

    Each dataset has its own schema based on the API response structure.

    Args:
        table_name: Name of the table to create
        dataset: Dataset identifier

    Returns:
        CREATE TABLE SQL statement
    """
    if dataset == "daily_balance":
        return _generate_daily_balance_schema(table_name)
    elif dataset == "debt_outstanding":
        return _generate_debt_outstanding_schema(table_name)
    elif dataset == "interest_rates":
        return _generate_interest_rates_schema(table_name)
    elif dataset == "monthly_statement":
        return _generate_monthly_statement_schema(table_name)
    elif dataset == "auctions":
        return _generate_auctions_schema(table_name)
    else:
        raise ValueError(f"Unknown Treasury dataset: {dataset}")


def _generate_daily_balance_schema(table_name: str) -> str:
    """Generate schema for Daily Treasury Balance data."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        record_date DATE NOT NULL,
        account_type TEXT,
        close_today_bal NUMERIC,
        open_today_bal NUMERIC,
        open_month_bal NUMERIC,
        open_fiscal_year_bal NUMERIC,
        transaction_type TEXT,
        transaction_catg TEXT,
        transaction_catg_desc TEXT,
        transaction_today_amt NUMERIC,
        transaction_mtd_amt NUMERIC,
        transaction_fytd_amt NUMERIC,
        table_nbr TEXT,
        table_nm TEXT,
        sub_table_name TEXT,
        src_line_nbr TEXT,
        record_fiscal_year INTEGER,
        record_fiscal_quarter INTEGER,
        record_calendar_year INTEGER,
        record_calendar_quarter INTEGER,
        record_calendar_month INTEGER,
        record_calendar_day INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(record_date, account_type, transaction_type, transaction_catg, src_line_nbr)
    );
    
    -- Create index on date for time-series queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (record_date);
    
    -- Create index on transaction type
    CREATE INDEX IF NOT EXISTS idx_{table_name}_type ON {table_name} (transaction_type);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'Treasury FiscalData - Daily Treasury Statement: Deposits, Withdrawals, and Operating Cash';
    """


def _generate_debt_outstanding_schema(table_name: str) -> str:
    """Generate schema for Debt Outstanding data."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        record_date DATE NOT NULL,
        debt_held_public_amt NUMERIC,
        intragov_hold_amt NUMERIC,
        tot_pub_debt_out_amt NUMERIC,
        src_line_nbr TEXT,
        record_fiscal_year INTEGER,
        record_fiscal_quarter INTEGER,
        record_calendar_year INTEGER,
        record_calendar_quarter INTEGER,
        record_calendar_month INTEGER,
        record_calendar_day INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(record_date)
    );
    
    -- Create index on date for time-series queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (record_date);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'Treasury FiscalData - Total Public Debt Outstanding (Debt to the Penny)';
    """


def _generate_interest_rates_schema(table_name: str) -> str:
    """Generate schema for Interest Rates data."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        record_date DATE NOT NULL,
        security_type_desc TEXT NOT NULL,
        security_desc TEXT,
        avg_interest_rate_amt NUMERIC,
        src_line_nbr TEXT,
        record_fiscal_year INTEGER,
        record_fiscal_quarter INTEGER,
        record_calendar_year INTEGER,
        record_calendar_quarter INTEGER,
        record_calendar_month INTEGER,
        record_calendar_day INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(record_date, security_type_desc, security_desc)
    );
    
    -- Create index on date for time-series queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (record_date);
    
    -- Create index on security type
    CREATE INDEX IF NOT EXISTS idx_{table_name}_security ON {table_name} (security_type_desc);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'Treasury FiscalData - Average Interest Rates on U.S. Treasury Securities';
    """


def _generate_monthly_statement_schema(table_name: str) -> str:
    """Generate schema for Monthly Treasury Statement data."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        record_date DATE NOT NULL,
        classification_desc TEXT,
        current_month_net_rcpt_outly_amt NUMERIC,
        fiscal_year_to_date_net_rcpt_outly_amt NUMERIC,
        prior_fiscal_year_to_date_net_rcpt_outly_amt NUMERIC,
        current_fytd_net_outly_rcpt_amt NUMERIC,
        prior_fytd_net_outly_rcpt_amt NUMERIC,
        category_desc TEXT,
        table_nbr TEXT,
        table_nm TEXT,
        sub_table_desc TEXT,
        src_line_nbr TEXT,
        record_fiscal_year INTEGER,
        record_fiscal_quarter INTEGER,
        record_calendar_year INTEGER,
        record_calendar_quarter INTEGER,
        record_calendar_month INTEGER,
        record_calendar_day INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(record_date, classification_desc, category_desc, src_line_nbr)
    );
    
    -- Create index on date for time-series queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (record_date);
    
    -- Create index on classification
    CREATE INDEX IF NOT EXISTS idx_{table_name}_classification ON {table_name} (classification_desc);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'Treasury FiscalData - Monthly Treasury Statement (Revenue and Spending)';
    """


def _generate_auctions_schema(table_name: str) -> str:
    """Generate schema for Treasury Auctions data."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        auction_date DATE NOT NULL,
        issue_date DATE,
        maturity_date DATE,
        security_type TEXT,
        security_term TEXT,
        cusip TEXT,
        high_investment_rate NUMERIC,
        interest_rate NUMERIC,
        allotted_pct NUMERIC,
        avg_med_disc_rate NUMERIC,
        avg_med_invest_rate NUMERIC,
        avg_med_price NUMERIC,
        avg_med_yield NUMERIC,
        bid_to_cover_ratio NUMERIC,
        competitive_accepted NUMERIC,
        competitive_tendered NUMERIC,
        non_competitive_accepted NUMERIC,
        non_competitive_tendered NUMERIC,
        total_accepted NUMERIC,
        total_tendered NUMERIC,
        primary_dealer_accepted NUMERIC,
        primary_dealer_tendered NUMERIC,
        direct_bidder_accepted NUMERIC,
        direct_bidder_tendered NUMERIC,
        indirect_bidder_accepted NUMERIC,
        indirect_bidder_tendered NUMERIC,
        fima_noncomp_accepted NUMERIC,
        fima_noncomp_tendered NUMERIC,
        soma_accepted NUMERIC,
        soma_tendered NUMERIC,
        price_per_100 NUMERIC,
        reopening TEXT,
        security_term_day_month TEXT,
        security_term_week_year TEXT,
        spread NUMERIC,
        treasury_direct_accepted NUMERIC,
        treasury_direct_tendered NUMERIC,
        record_date DATE,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(auction_date, cusip)
    );
    
    -- Create index on auction date
    CREATE INDEX IF NOT EXISTS idx_{table_name}_auction_date ON {table_name} (auction_date);
    
    -- Create index on security type
    CREATE INDEX IF NOT EXISTS idx_{table_name}_security_type ON {table_name} (security_type);
    
    -- Create index on CUSIP
    CREATE INDEX IF NOT EXISTS idx_{table_name}_cusip ON {table_name} (cusip);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'Treasury FiscalData - Treasury Securities Auction Results';
    """


def parse_treasury_response(
    api_response: Dict[str, Any], dataset: str
) -> List[Dict[str, Any]]:
    """
    Parse Treasury API response into database rows.

    Treasury API response format:
    {
        "data": [...],
        "meta": {"count": N, "labels": {...}},
        "links": {...}
    }

    Args:
        api_response: Raw API response dict
        dataset: Dataset identifier

    Returns:
        List of dictionaries suitable for database insertion
    """
    data = api_response.get("data", [])

    if not data:
        logger.warning(f"No data in Treasury API response for {dataset}")
        return []

    parsed_rows = []
    for record in data:
        try:
            parsed = _parse_record(record, dataset)
            if parsed:
                parsed_rows.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse record: {e}")
            continue

    return parsed_rows


def _parse_record(record: Dict[str, Any], dataset: str) -> Optional[Dict[str, Any]]:
    """Parse a single record based on dataset type."""
    if dataset == "daily_balance":
        return _parse_daily_balance_record(record)
    elif dataset == "debt_outstanding":
        return _parse_debt_outstanding_record(record)
    elif dataset == "interest_rates":
        return _parse_interest_rates_record(record)
    elif dataset == "monthly_statement":
        return _parse_monthly_statement_record(record)
    elif dataset == "auctions":
        return _parse_auctions_record(record)
    else:
        raise ValueError(f"Unknown dataset: {dataset}")


def _parse_daily_balance_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse daily balance record."""
    return {
        "record_date": record.get("record_date"),
        "account_type": record.get("account_type"),
        "close_today_bal": _parse_numeric(record.get("close_today_bal")),
        "open_today_bal": _parse_numeric(record.get("open_today_bal")),
        "open_month_bal": _parse_numeric(record.get("open_month_bal")),
        "open_fiscal_year_bal": _parse_numeric(record.get("open_fiscal_year_bal")),
        "transaction_type": record.get("transaction_type"),
        "transaction_catg": record.get("transaction_catg"),
        "transaction_catg_desc": record.get("transaction_catg_desc"),
        "transaction_today_amt": _parse_numeric(record.get("transaction_today_amt")),
        "transaction_mtd_amt": _parse_numeric(record.get("transaction_mtd_amt")),
        "transaction_fytd_amt": _parse_numeric(record.get("transaction_fytd_amt")),
        "table_nbr": record.get("table_nbr"),
        "table_nm": record.get("table_nm"),
        "sub_table_name": record.get("sub_table_name"),
        "src_line_nbr": record.get("src_line_nbr"),
        "record_fiscal_year": _parse_int(record.get("record_fiscal_year")),
        "record_fiscal_quarter": _parse_int(record.get("record_fiscal_quarter")),
        "record_calendar_year": _parse_int(record.get("record_calendar_year")),
        "record_calendar_quarter": _parse_int(record.get("record_calendar_quarter")),
        "record_calendar_month": _parse_int(record.get("record_calendar_month")),
        "record_calendar_day": _parse_int(record.get("record_calendar_day")),
    }


def _parse_debt_outstanding_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse debt outstanding record."""
    return {
        "record_date": record.get("record_date"),
        "debt_held_public_amt": _parse_numeric(record.get("debt_held_public_amt")),
        "intragov_hold_amt": _parse_numeric(record.get("intragov_hold_amt")),
        "tot_pub_debt_out_amt": _parse_numeric(record.get("tot_pub_debt_out_amt")),
        "src_line_nbr": record.get("src_line_nbr"),
        "record_fiscal_year": _parse_int(record.get("record_fiscal_year")),
        "record_fiscal_quarter": _parse_int(record.get("record_fiscal_quarter")),
        "record_calendar_year": _parse_int(record.get("record_calendar_year")),
        "record_calendar_quarter": _parse_int(record.get("record_calendar_quarter")),
        "record_calendar_month": _parse_int(record.get("record_calendar_month")),
        "record_calendar_day": _parse_int(record.get("record_calendar_day")),
    }


def _parse_interest_rates_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse interest rates record."""
    return {
        "record_date": record.get("record_date"),
        "security_type_desc": record.get("security_type_desc"),
        "security_desc": record.get("security_desc"),
        "avg_interest_rate_amt": _parse_numeric(record.get("avg_interest_rate_amt")),
        "src_line_nbr": record.get("src_line_nbr"),
        "record_fiscal_year": _parse_int(record.get("record_fiscal_year")),
        "record_fiscal_quarter": _parse_int(record.get("record_fiscal_quarter")),
        "record_calendar_year": _parse_int(record.get("record_calendar_year")),
        "record_calendar_quarter": _parse_int(record.get("record_calendar_quarter")),
        "record_calendar_month": _parse_int(record.get("record_calendar_month")),
        "record_calendar_day": _parse_int(record.get("record_calendar_day")),
    }


def _parse_monthly_statement_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse monthly statement record."""
    return {
        "record_date": record.get("record_date"),
        "classification_desc": record.get("classification_desc"),
        "current_month_net_rcpt_outly_amt": _parse_numeric(
            record.get("current_month_net_rcpt_outly_amt")
        ),
        "fiscal_year_to_date_net_rcpt_outly_amt": _parse_numeric(
            record.get("fiscal_year_to_date_net_rcpt_outly_amt")
        ),
        "prior_fiscal_year_to_date_net_rcpt_outly_amt": _parse_numeric(
            record.get("prior_fiscal_year_to_date_net_rcpt_outly_amt")
        ),
        "current_fytd_net_outly_rcpt_amt": _parse_numeric(
            record.get("current_fytd_net_outly_rcpt_amt")
        ),
        "prior_fytd_net_outly_rcpt_amt": _parse_numeric(
            record.get("prior_fytd_net_outly_rcpt_amt")
        ),
        "category_desc": record.get("category_desc"),
        "table_nbr": record.get("table_nbr"),
        "table_nm": record.get("table_nm"),
        "sub_table_desc": record.get("sub_table_desc"),
        "src_line_nbr": record.get("src_line_nbr"),
        "record_fiscal_year": _parse_int(record.get("record_fiscal_year")),
        "record_fiscal_quarter": _parse_int(record.get("record_fiscal_quarter")),
        "record_calendar_year": _parse_int(record.get("record_calendar_year")),
        "record_calendar_quarter": _parse_int(record.get("record_calendar_quarter")),
        "record_calendar_month": _parse_int(record.get("record_calendar_month")),
        "record_calendar_day": _parse_int(record.get("record_calendar_day")),
    }


def _parse_auctions_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse auctions record."""
    return {
        "auction_date": record.get("auction_date"),
        "issue_date": record.get("issue_date"),
        "maturity_date": record.get("maturity_date"),
        "security_type": record.get("security_type"),
        "security_term": record.get("security_term"),
        "cusip": record.get("cusip"),
        "high_investment_rate": _parse_numeric(record.get("high_investment_rate")),
        "interest_rate": _parse_numeric(record.get("interest_rate")),
        "allotted_pct": _parse_numeric(record.get("allotted_pct")),
        "avg_med_disc_rate": _parse_numeric(record.get("avg_med_disc_rate")),
        "avg_med_invest_rate": _parse_numeric(record.get("avg_med_invest_rate")),
        "avg_med_price": _parse_numeric(record.get("avg_med_price")),
        "avg_med_yield": _parse_numeric(record.get("avg_med_yield")),
        "bid_to_cover_ratio": _parse_numeric(record.get("bid_to_cover_ratio")),
        "competitive_accepted": _parse_numeric(record.get("competitive_accepted")),
        "competitive_tendered": _parse_numeric(record.get("competitive_tendered")),
        "non_competitive_accepted": _parse_numeric(
            record.get("non_competitive_accepted")
        ),
        "non_competitive_tendered": _parse_numeric(
            record.get("non_competitive_tendered")
        ),
        "total_accepted": _parse_numeric(record.get("total_accepted")),
        "total_tendered": _parse_numeric(record.get("total_tendered")),
        "primary_dealer_accepted": _parse_numeric(
            record.get("primary_dealer_accepted")
        ),
        "primary_dealer_tendered": _parse_numeric(
            record.get("primary_dealer_tendered")
        ),
        "direct_bidder_accepted": _parse_numeric(record.get("direct_bidder_accepted")),
        "direct_bidder_tendered": _parse_numeric(record.get("direct_bidder_tendered")),
        "indirect_bidder_accepted": _parse_numeric(
            record.get("indirect_bidder_accepted")
        ),
        "indirect_bidder_tendered": _parse_numeric(
            record.get("indirect_bidder_tendered")
        ),
        "fima_noncomp_accepted": _parse_numeric(record.get("fima_noncomp_accepted")),
        "fima_noncomp_tendered": _parse_numeric(record.get("fima_noncomp_tendered")),
        "soma_accepted": _parse_numeric(record.get("soma_accepted")),
        "soma_tendered": _parse_numeric(record.get("soma_tendered")),
        "price_per_100": _parse_numeric(record.get("price_per_100")),
        "reopening": record.get("reopening"),
        "security_term_day_month": record.get("security_term_day_month"),
        "security_term_week_year": record.get("security_term_week_year"),
        "spread": _parse_numeric(record.get("spread")),
        "treasury_direct_accepted": _parse_numeric(
            record.get("treasury_direct_accepted")
        ),
        "treasury_direct_tendered": _parse_numeric(
            record.get("treasury_direct_tendered")
        ),
        "record_date": record.get("record_date"),
    }


def _parse_numeric(value: Any) -> Optional[float]:
    """Parse numeric value, handling nulls and empty strings."""
    if value is None or value == "" or value == "null":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    """Parse integer value, handling nulls and empty strings."""
    if value is None or value == "" or value == "null":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def build_insert_values(parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build list of dictionaries for parameterized INSERT.

    Args:
        parsed_data: List of parsed records

    Returns:
        List of dictionaries for parameterized INSERT
    """
    logger.info(f"Built {len(parsed_data)} rows for insertion")
    return parsed_data


def get_default_date_range() -> tuple[str, str]:
    """
    Get default date range for Treasury data ingestion.

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format

    Default: Last 5 years
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 5)  # 5 years

    return (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))


def validate_date_format(date_str: str) -> bool:
    """
    Validate date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_dataset_display_name(dataset: str) -> str:
    """
    Get display name for a Treasury dataset.

    Args:
        dataset: Dataset identifier

    Returns:
        Human-readable display name
    """
    display_names = {
        "daily_balance": "Daily Treasury Balance",
        "debt_outstanding": "Total Public Debt Outstanding",
        "interest_rates": "Treasury Interest Rates",
        "monthly_statement": "Monthly Treasury Statement",
        "auctions": "Treasury Auction Results",
    }

    return display_names.get(dataset, dataset.replace("_", " ").title())


def get_dataset_description(dataset: str) -> str:
    """
    Get description for a Treasury dataset.

    Args:
        dataset: Dataset identifier

    Returns:
        Description text
    """
    from app.sources.treasury.client import TREASURY_DATASETS

    if dataset in TREASURY_DATASETS:
        return TREASURY_DATASETS[dataset]["description"]

    return f"Treasury FiscalData - {dataset.replace('_', ' ').title()}"
