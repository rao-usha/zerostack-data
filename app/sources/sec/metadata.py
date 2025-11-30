"""
SEC EDGAR metadata parsing and schema generation.

Handles:
- Parsing SEC submission data
- Extracting filings by type (10-K, 10-Q, 8-K, etc.)
- Generating table schemas for filings
- Extracting XBRL data
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
import re

logger = logging.getLogger(__name__)


# SEC filing types we support
SUPPORTED_FILING_TYPES = {
    "10-K": "Annual report",
    "10-K/A": "Annual report (amended)",
    "10-Q": "Quarterly report",
    "10-Q/A": "Quarterly report (amended)",
    "8-K": "Current report",
    "8-K/A": "Current report (amended)",
    "S-1": "Initial registration statement",
    "S-1/A": "Initial registration statement (amended)",
    "S-3": "Registration statement",
    "S-3/A": "Registration statement (amended)",
    "S-4": "Registration statement (business combination)",
    "S-4/A": "Registration statement (business combination, amended)",
}


def generate_table_name(filing_type: str) -> str:
    """
    Generate Postgres table name for SEC filings.
    
    Args:
        filing_type: Filing type (e.g., "10-K", "10-Q", "8-K")
        
    Returns:
        Table name (e.g., "sec_10k", "sec_10q", "sec_8k")
    """
    # Convert filing type to safe table name
    # "10-K" -> "10k", "10-K/A" -> "10k_a"
    safe_name = filing_type.lower()
    safe_name = safe_name.replace("-", "")
    safe_name = safe_name.replace("/", "_")
    
    return f"sec_{safe_name}"


def generate_create_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for SEC filings.
    
    All SEC filing tables have the same schema structure.
    
    Args:
        table_name: Target table name
        
    Returns:
        SQL CREATE TABLE statement
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            
            -- Company identifiers
            cik TEXT NOT NULL,
            ticker TEXT,
            company_name TEXT NOT NULL,
            
            -- Filing metadata
            accession_number TEXT NOT NULL UNIQUE,
            filing_type TEXT NOT NULL,
            filing_date DATE NOT NULL,
            report_date DATE,
            
            -- Filing URLs
            primary_document TEXT,
            filing_url TEXT,
            interactive_data_url TEXT,
            
            -- File details
            file_number TEXT,
            film_number TEXT,
            items TEXT,  -- For 8-K, which items were triggered
            
            -- Processing metadata
            ingested_at TIMESTAMP DEFAULT NOW(),
            
            -- Indexes for efficient queries
            INDEX idx_{table_name}_cik (cik),
            INDEX idx_{table_name}_ticker (ticker),
            INDEX idx_{table_name}_filing_date (filing_date),
            INDEX idx_{table_name}_report_date (report_date),
            INDEX idx_{table_name}_accession (accession_number)
        );
    """


def parse_company_info(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse company information from SEC submissions response.
    
    Args:
        data: Raw SEC submissions API response
        
    Returns:
        Parsed company info
    """
    return {
        "cik": data.get("cik"),
        "ticker": ",".join(data.get("tickers", [])) if data.get("tickers") else None,
        "company_name": data.get("name"),
        "sic": data.get("sic"),
        "sic_description": data.get("sicDescription"),
        "ein": data.get("ein"),
        "fiscal_year_end": data.get("fiscalYearEnd"),
        "business_address": data.get("addresses", {}).get("business"),
        "mailing_address": data.get("addresses", {}).get("mailing"),
    }


def parse_filings(
    data: Dict[str, Any],
    filing_types: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[Dict[str, Any]]:
    """
    Parse filings from SEC submissions response.
    
    Args:
        data: Raw SEC submissions API response
        filing_types: Optional list of filing types to filter (e.g., ["10-K", "10-Q"])
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        List of parsed filings
    """
    recent = data.get("filings", {}).get("recent", {})
    
    if not recent:
        logger.warning("No recent filings found in response")
        return []
    
    # SEC returns parallel arrays for filing data
    accession_numbers = recent.get("accessionNumber", [])
    form_types = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_documents = recent.get("primaryDocument", [])
    file_numbers = recent.get("fileNumber", [])
    film_numbers = recent.get("filmNumber", [])
    items = recent.get("items", [])
    
    company_info = parse_company_info(data)
    
    filings = []
    
    for i in range(len(accession_numbers)):
        form_type = form_types[i] if i < len(form_types) else None
        filing_date_str = filing_dates[i] if i < len(filing_dates) else None
        
        # Filter by filing type
        if filing_types and form_type not in filing_types:
            continue
        
        # Parse filing date
        try:
            filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            logger.warning(f"Invalid filing date: {filing_date_str}")
            continue
        
        # Filter by date range
        if start_date and filing_date < start_date:
            continue
        if end_date and filing_date > end_date:
            continue
        
        # Parse report date (may be null)
        report_date_str = report_dates[i] if i < len(report_dates) else None
        try:
            report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date() if report_date_str else None
        except (ValueError, TypeError):
            report_date = None
        
        accession_number = accession_numbers[i]
        primary_document = primary_documents[i] if i < len(primary_documents) else None
        
        # Build filing URLs
        accession_clean = accession_number.replace("-", "")
        cik_padded = str(company_info["cik"]).zfill(10)
        
        filing_url = f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik_padded}&accession_number={accession_number}"
        
        interactive_data_url = None
        if primary_document:
            # Some filings have interactive data viewers
            interactive_data_url = f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik_padded}&accession_number={accession_number}&xbrl_type=v"
        
        filing = {
            "cik": company_info["cik"],
            "ticker": company_info["ticker"],
            "company_name": company_info["company_name"],
            "accession_number": accession_number,
            "filing_type": form_type,
            "filing_date": filing_date,
            "report_date": report_date,
            "primary_document": primary_document,
            "filing_url": filing_url,
            "interactive_data_url": interactive_data_url,
            "file_number": file_numbers[i] if i < len(file_numbers) else None,
            "film_number": film_numbers[i] if i < len(film_numbers) else None,
            "items": items[i] if i < len(items) else None,
        }
        
        filings.append(filing)
    
    return filings


def build_insert_values(filings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build parameterized INSERT values from parsed filings.
    
    Args:
        filings: List of parsed filings
        
    Returns:
        List of dicts ready for parameterized SQL insertion
    """
    return filings


def get_filing_type_description(filing_type: str) -> str:
    """Get human-readable description for filing type."""
    return SUPPORTED_FILING_TYPES.get(filing_type, filing_type)


def get_default_date_range() -> Tuple[date, date]:
    """
    Get default date range for SEC filings.
    
    Returns last 5 years by default.
    """
    end_date = date.today()
    start_date = date(end_date.year - 5, 1, 1)
    return start_date, end_date


def validate_cik(cik: str) -> bool:
    """
    Validate CIK format.
    
    CIK should be numeric, up to 10 digits.
    """
    if not cik:
        return False
    
    # Remove leading zeros for validation
    cik_clean = cik.lstrip("0")
    
    # Should be numeric and not empty after removing leading zeros
    return cik_clean.isdigit() and len(cik_clean) <= 10


def normalize_cik(cik: str) -> str:
    """
    Normalize CIK to 10-digit format with leading zeros.
    
    Args:
        cik: CIK number (may be int or str, with or without leading zeros)
        
    Returns:
        10-digit CIK string with leading zeros
    """
    return str(cik).zfill(10)

