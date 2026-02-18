"""
FDIC BankFind metadata utilities.

Handles:
- Financial metric definitions (1,100+ variables)
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation

FDIC BankFind API: https://banks.data.fdic.gov/docs/
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# KEY FINANCIAL METRICS
# =============================================================================

# Common financial metrics from FDIC Call Reports
# These are the most frequently used metrics for bank analysis
COMMON_FINANCIAL_METRICS = {
    # Bank identification
    "CERT": {"description": "FDIC Certificate Number", "type": "INTEGER"},
    "NAME": {"description": "Institution Name", "type": "TEXT"},
    "REPDTE": {"description": "Report Date (YYYYMMDD)", "type": "DATE"},
    # Balance Sheet - Assets
    "ASSET": {"description": "Total Assets", "type": "NUMERIC"},
    "ASTEMPM": {"description": "Assets per Employee (thousands)", "type": "NUMERIC"},
    "LNLSNET": {"description": "Net Loans and Leases", "type": "NUMERIC"},
    "SC": {"description": "Total Securities", "type": "NUMERIC"},
    "SCUS": {"description": "U.S. Government Securities", "type": "NUMERIC"},
    "SCMUNI": {"description": "Municipal Securities", "type": "NUMERIC"},
    "SCODOT": {"description": "Other Securities", "type": "NUMERIC"},
    "CHBAL": {"description": "Cash and Balances Due", "type": "NUMERIC"},
    "FREPO": {"description": "Federal Funds and Repos - Sold", "type": "NUMERIC"},
    "INTANG": {"description": "Intangible Assets", "type": "NUMERIC"},
    "OREO": {"description": "Other Real Estate Owned", "type": "NUMERIC"},
    "OTEFN": {"description": "Other Assets", "type": "NUMERIC"},
    # Balance Sheet - Liabilities
    "LIAB": {"description": "Total Liabilities", "type": "NUMERIC"},
    "DEP": {"description": "Total Deposits", "type": "NUMERIC"},
    "DEPI": {"description": "Insured Deposits", "type": "NUMERIC"},
    "DEPUNA": {"description": "Uninsured Deposits", "type": "NUMERIC"},
    "DEPDOM": {"description": "Domestic Deposits", "type": "NUMERIC"},
    "DEPFOR": {"description": "Foreign Deposits", "type": "NUMERIC"},
    "FREPP": {"description": "Federal Funds and Repos - Purchased", "type": "NUMERIC"},
    "OTHBRF": {"description": "Other Borrowed Funds", "type": "NUMERIC"},
    "SUBND": {"description": "Subordinated Notes and Debentures", "type": "NUMERIC"},
    # Balance Sheet - Equity
    "EQ": {"description": "Total Equity Capital", "type": "NUMERIC"},
    "EQTOT": {"description": "Total Shareholders' Equity", "type": "NUMERIC"},
    "RBCT1J": {"description": "Tier 1 Capital", "type": "NUMERIC"},
    "RBCT2": {"description": "Tier 2 Capital", "type": "NUMERIC"},
    "RBC1AAJ": {"description": "Total Risk-Based Capital", "type": "NUMERIC"},
    # Income Statement
    "NETINC": {"description": "Net Income", "type": "NUMERIC"},
    "NIMY": {"description": "Net Interest Margin", "type": "NUMERIC"},
    "NIM": {"description": "Net Interest Margin (%)", "type": "NUMERIC"},
    "INTINC": {"description": "Total Interest Income", "type": "NUMERIC"},
    "INTEXP": {"description": "Total Interest Expense", "type": "NUMERIC"},
    "NETII": {"description": "Net Interest Income", "type": "NUMERIC"},
    "NONII": {"description": "Total Noninterest Income", "type": "NUMERIC"},
    "NONIX": {"description": "Total Noninterest Expense", "type": "NUMERIC"},
    "EEFFR": {"description": "Efficiency Ratio (%)", "type": "NUMERIC"},
    "ELNATR": {"description": "Loan Loss Provision", "type": "NUMERIC"},
    "EPREEM": {"description": "Pre-tax Net Operating Income", "type": "NUMERIC"},
    # Performance Ratios
    "ROA": {"description": "Return on Assets (%)", "type": "NUMERIC"},
    "ROE": {"description": "Return on Equity (%)", "type": "NUMERIC"},
    "ROAPTX": {"description": "Pre-tax ROA (%)", "type": "NUMERIC"},
    "ROAPTXQ": {"description": "Pre-tax ROA - Quarterly (%)", "type": "NUMERIC"},
    "ROAQ": {"description": "ROA - Quarterly Annualized (%)", "type": "NUMERIC"},
    "ROEQ": {"description": "ROE - Quarterly Annualized (%)", "type": "NUMERIC"},
    # Capital Ratios
    "RBC1RWAJ": {
        "description": "Tier 1 Risk-Based Capital Ratio (%)",
        "type": "NUMERIC",
    },
    "RBCRWAJ": {"description": "Total Risk-Based Capital Ratio (%)", "type": "NUMERIC"},
    "IDT1CER": {"description": "Tier 1 Leverage Ratio (%)", "type": "NUMERIC"},
    "LNLSDEPR": {"description": "Loans to Deposits Ratio (%)", "type": "NUMERIC"},
    # Asset Quality
    "P3ASSET": {
        "description": "Noncurrent Assets + OREO / Assets (%)",
        "type": "NUMERIC",
    },
    "P9ASSET": {
        "description": "Noncurrent Loans + OREO / Assets (%)",
        "type": "NUMERIC",
    },
    "NCLNLSR": {"description": "Noncurrent Loans / Loans (%)", "type": "NUMERIC"},
    "NTLNLSR": {"description": "Net Charge-offs / Loans (%)", "type": "NUMERIC"},
    "LNLSNTV": {"description": "Noncurrent Loans", "type": "NUMERIC"},
    "LNATRES": {"description": "Loan Loss Allowance", "type": "NUMERIC"},
    "LNRESNCR": {
        "description": "Loan Loss Reserve / Noncurrent Loans (%)",
        "type": "NUMERIC",
    },
    # Loan Composition
    "LNRE": {"description": "Real Estate Loans", "type": "NUMERIC"},
    "LNRECONS": {"description": "Construction & Development Loans", "type": "NUMERIC"},
    "LNRENRES": {"description": "Non-farm Non-residential RE Loans", "type": "NUMERIC"},
    "LNREMULT": {"description": "Multifamily RE Loans", "type": "NUMERIC"},
    "LNRERES": {"description": "1-4 Family Residential RE Loans", "type": "NUMERIC"},
    "LNCI": {"description": "Commercial & Industrial Loans", "type": "NUMERIC"},
    "LNCON": {"description": "Consumer Loans", "type": "NUMERIC"},
    "LNCRCD": {"description": "Credit Card Loans", "type": "NUMERIC"},
    "LNAG": {"description": "Agricultural Loans", "type": "NUMERIC"},
    "LNOTH": {"description": "Other Loans", "type": "NUMERIC"},
    # Deposit Composition
    "DEPNIDOM": {"description": "Non-Interest Bearing Deposits", "type": "NUMERIC"},
    "DEPTI": {"description": "Interest Bearing Deposits", "type": "NUMERIC"},
    "DEPSMAMT": {"description": "Deposits < $250K", "type": "NUMERIC"},
    "DEPLGAMT": {"description": "Deposits >= $250K", "type": "NUMERIC"},
    "COTEFN": {"description": "Core Deposits", "type": "NUMERIC"},
    "BKDEP": {"description": "Brokered Deposits", "type": "NUMERIC"},
    # Other Metrics
    "NUMEMP": {"description": "Number of Full-time Employees", "type": "INTEGER"},
    "OFFDOM": {"description": "Number of Domestic Offices", "type": "INTEGER"},
    "OFFFOR": {"description": "Number of Foreign Offices", "type": "INTEGER"},
    "RSSDHCR": {"description": "Holding Company ID", "type": "TEXT"},
}

# Extended financial metrics (all available from FDIC API)
FINANCIAL_METRICS = {
    **COMMON_FINANCIAL_METRICS,
    # Add more metrics as needed
}


# =============================================================================
# INSTITUTION FIELDS
# =============================================================================

INSTITUTION_FIELDS = {
    "CERT": {"description": "FDIC Certificate Number", "type": "INTEGER"},
    "NAME": {"description": "Institution Name", "type": "TEXT"},
    "ACTIVE": {
        "description": "Active Status (0=Inactive, 1=Active)",
        "type": "INTEGER",
    },
    "CITY": {"description": "City", "type": "TEXT"},
    "STALP": {"description": "State Abbreviation", "type": "TEXT"},
    "STNAME": {"description": "State Name", "type": "TEXT"},
    "ZIP": {"description": "ZIP Code", "type": "TEXT"},
    "ADDRESS": {"description": "Street Address", "type": "TEXT"},
    "COUNTY": {"description": "County Name", "type": "TEXT"},
    "CBSA": {"description": "CBSA Code", "type": "TEXT"},
    "CBSA_DIV": {"description": "CBSA Division Code", "type": "TEXT"},
    "CBSA_DIV_FLG": {"description": "CBSA Division Flag", "type": "TEXT"},
    "CBSA_DIV_NO": {"description": "CBSA Division Number", "type": "TEXT"},
    "CBSA_METRO": {"description": "CBSA Metro Code", "type": "TEXT"},
    "CBSA_METRO_FLG": {"description": "CBSA Metro Flag", "type": "TEXT"},
    "CBSA_METRO_NAME": {"description": "CBSA Metro Name", "type": "TEXT"},
    "CBSA_MICRO_FLG": {"description": "CBSA Micro Flag", "type": "TEXT"},
    "CBSA_NO": {"description": "CBSA Number", "type": "TEXT"},
    "FDICREGN": {"description": "FDIC Supervisory Region", "type": "TEXT"},
    "FDICSUPV": {"description": "FDIC Field Office", "type": "TEXT"},
    "FED": {"description": "Federal Reserve District", "type": "TEXT"},
    "FEDCHRTR": {"description": "Federal Charter Flag", "type": "INTEGER"},
    "INSFDIC": {"description": "FDIC Insured", "type": "INTEGER"},
    "OCCDIST": {"description": "OCC District", "type": "TEXT"},
    "REGAGNT": {"description": "Primary Regulator", "type": "TEXT"},
    "BKCLASS": {"description": "Bank Class", "type": "TEXT"},
    "CHARTER": {"description": "Charter Type", "type": "TEXT"},
    "CHRTAGNT": {"description": "Chartering Agency", "type": "TEXT"},
    "CONSERVE": {"description": "Conservatorship Flag", "type": "TEXT"},
    "DENESSION": {"description": "De Novo Institution", "type": "TEXT"},
    "ESTYMD": {"description": "Established Date", "type": "DATE"},
    "INSDATE": {"description": "FDIC Insurance Date", "type": "DATE"},
    "INSTCRCD": {"description": "Institution Category Code", "type": "TEXT"},
    "MUTUAL": {"description": "Mutual Ownership Flag", "type": "INTEGER"},
    "NEWCERT": {"description": "New Certificate Number", "type": "INTEGER"},
    "OAKESSION": {"description": "OTS Acquisition", "type": "TEXT"},
    "OTHESSION": {"description": "Other Acquisition", "type": "TEXT"},
    "PARCERT": {"description": "Parent Certificate", "type": "INTEGER"},
    "QBPRCOML": {
        "description": "Quarterly Banking Profile Commercial Flag",
        "type": "TEXT",
    },
    "RISESSION": {"description": "Resolution", "type": "TEXT"},
    "RUNDATE": {"description": "Run Date", "type": "DATE"},
    "SESSION": {"description": "Session", "type": "TEXT"},
    "SPECGRP": {"description": "Specialty Group", "type": "TEXT"},
    "SPECGRPN": {"description": "Specialty Group Number", "type": "INTEGER"},
    "STALPHBR": {"description": "State Alphabetic Branch", "type": "TEXT"},
    "STCHRTR": {"description": "State Charter Flag", "type": "INTEGER"},
    "STCNTY": {"description": "State/County FIPS", "type": "TEXT"},
    "STNUM": {"description": "State Number", "type": "INTEGER"},
    "UNESSION": {"description": "Unassisted Merger", "type": "TEXT"},
    "WEBADDR": {"description": "Website URL", "type": "TEXT"},
    "ASSET": {"description": "Total Assets", "type": "NUMERIC"},
    "DEP": {"description": "Total Deposits", "type": "NUMERIC"},
    "DEPDOM": {"description": "Domestic Deposits", "type": "NUMERIC"},
    "EQ": {"description": "Equity Capital", "type": "NUMERIC"},
    "NETINC": {"description": "Net Income", "type": "NUMERIC"},
    "OFFDOM": {"description": "Domestic Offices", "type": "INTEGER"},
    "OFFFOR": {"description": "Foreign Offices", "type": "INTEGER"},
    "ROA": {"description": "Return on Assets (%)", "type": "NUMERIC"},
    "ROE": {"description": "Return on Equity (%)", "type": "NUMERIC"},
    "DATEUPDT": {"description": "Last Update Date", "type": "DATE"},
}


# =============================================================================
# FAILED BANKS FIELDS
# =============================================================================

FAILED_BANKS_FIELDS = {
    "CERT": {"description": "FDIC Certificate Number", "type": "INTEGER"},
    "NAME": {"description": "Institution Name", "type": "TEXT"},
    "CITYST": {"description": "City, State", "type": "TEXT"},
    "CITY": {"description": "City", "type": "TEXT"},
    "STATE": {"description": "State", "type": "TEXT"},
    "FAILDATE": {"description": "Failure Date", "type": "DATE"},
    "SAVESSION": {"description": "Acquiring Institution", "type": "TEXT"},
    "RESESSION": {"description": "Resolution Type", "type": "TEXT"},
    "RESTYPE": {"description": "Resolution Type Code", "type": "TEXT"},
    "RESTYPE1": {"description": "Resolution Type Description", "type": "TEXT"},
    "CHESSION": {"description": "Charter Class", "type": "TEXT"},
    "QBFASSET": {"description": "Estimated Assets at Failure", "type": "NUMERIC"},
    "QBFDEP": {"description": "Estimated Deposits at Failure", "type": "NUMERIC"},
    "FUND": {"description": "Insurance Fund", "type": "TEXT"},
    "COST": {"description": "Estimated Cost to FDIC", "type": "NUMERIC"},
    "PSESSION": {"description": "P&A Transaction", "type": "TEXT"},
    "FESSION": {"description": "Failed Institution Session", "type": "TEXT"},
}


# =============================================================================
# SUMMARY OF DEPOSITS FIELDS
# =============================================================================

SOD_FIELDS = {
    "CERT": {"description": "FDIC Certificate Number", "type": "INTEGER"},
    "NAME": {"description": "Institution Name", "type": "TEXT"},
    "YEAR": {"description": "Report Year", "type": "INTEGER"},
    "UNINESSION": {"description": "Branch Unique Number", "type": "TEXT"},
    "BRNUM": {"description": "Branch Number", "type": "INTEGER"},
    "BRSERTYP": {"description": "Branch Service Type", "type": "TEXT"},
    "BRCENM": {"description": "Branch Census Name", "type": "TEXT"},
    "ADDRESS": {"description": "Street Address", "type": "TEXT"},
    "CITY": {"description": "City", "type": "TEXT"},
    "STNAME": {"description": "State Name", "type": "TEXT"},
    "STALP": {"description": "State Abbreviation", "type": "TEXT"},
    "ZIPBR": {"description": "ZIP Code", "type": "TEXT"},
    "COUNTY": {"description": "County Name", "type": "TEXT"},
    "STCNTY": {"description": "State/County FIPS", "type": "TEXT"},
    "CBSA": {"description": "CBSA Code", "type": "TEXT"},
    "CBSA_DIV": {"description": "CBSA Division Code", "type": "TEXT"},
    "CBSA_METRO": {"description": "CBSA Metro Code", "type": "TEXT"},
    "CSA": {"description": "CSA Code", "type": "TEXT"},
    "ASSET": {"description": "Total Assets", "type": "NUMERIC"},
    "DESSION": {"description": "Branch Deposits", "type": "NUMERIC"},
    "DEPSUM": {"description": "Sum of Deposits", "type": "NUMERIC"},
    "DEPDOM": {"description": "Domestic Deposits", "type": "NUMERIC"},
    "BKCLASS": {"description": "Bank Class", "type": "TEXT"},
    "CHARTER": {"description": "Charter Type", "type": "TEXT"},
    "ESESSION": {"description": "Establishment Date", "type": "DATE"},
    "MAINOFF": {"description": "Main Office Flag", "type": "INTEGER"},
    "SPECGRP": {"description": "Specialty Group", "type": "TEXT"},
    "LATITUDE": {"description": "Latitude", "type": "NUMERIC"},
    "LONGITUDE": {"description": "Longitude", "type": "NUMERIC"},
    "RUNDATE": {"description": "Run Date", "type": "DATE"},
}


# =============================================================================
# TABLE NAME GENERATION
# =============================================================================


def generate_table_name(dataset: str) -> str:
    """
    Generate table name for FDIC dataset.

    Convention: fdic_{dataset}

    Args:
        dataset: Dataset name (financials, institutions, failed_banks, summary_deposits)

    Returns:
        Table name (e.g., "fdic_bank_financials")
    """
    dataset_mapping = {
        "financials": "fdic_bank_financials",
        "institutions": "fdic_institutions",
        "failed_banks": "fdic_failed_banks",
        "summary_deposits": "fdic_summary_deposits",
        "sod": "fdic_summary_deposits",
    }

    return dataset_mapping.get(dataset.lower(), f"fdic_{dataset.lower()}")


# =============================================================================
# CREATE TABLE SQL GENERATION
# =============================================================================


def generate_financials_table_sql(table_name: str = "fdic_bank_financials") -> str:
    """
    Generate CREATE TABLE SQL for bank financials data.

    Uses key financial metrics with proper typing (NUMERIC for financials, DATE for dates).
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Primary Key
        id SERIAL PRIMARY KEY,
        
        -- Bank Identification
        cert INTEGER NOT NULL,
        name TEXT,
        repdte DATE NOT NULL,
        
        -- Balance Sheet - Assets
        asset NUMERIC,
        lnlsnet NUMERIC,
        sc NUMERIC,
        scus NUMERIC,
        scmuni NUMERIC,
        chbal NUMERIC,
        frepo NUMERIC,
        intang NUMERIC,
        oreo NUMERIC,
        
        -- Balance Sheet - Liabilities
        liab NUMERIC,
        dep NUMERIC,
        depi NUMERIC,
        depuna NUMERIC,
        depdom NUMERIC,
        depfor NUMERIC,
        frepp NUMERIC,
        othbrf NUMERIC,
        subnd NUMERIC,
        
        -- Balance Sheet - Equity
        eq NUMERIC,
        eqtot NUMERIC,
        rbct1j NUMERIC,
        rbct2 NUMERIC,
        rbc1aaj NUMERIC,
        
        -- Income Statement
        netinc NUMERIC,
        nim NUMERIC,
        nimy NUMERIC,
        intinc NUMERIC,
        intexp NUMERIC,
        netii NUMERIC,
        nonii NUMERIC,
        nonix NUMERIC,
        eeffr NUMERIC,
        elnatr NUMERIC,
        epreem NUMERIC,
        
        -- Performance Ratios
        roa NUMERIC,
        roe NUMERIC,
        roaptx NUMERIC,
        roaq NUMERIC,
        roeq NUMERIC,
        
        -- Capital Ratios
        rbc1rwaj NUMERIC,
        rbcrwaj NUMERIC,
        idt1cer NUMERIC,
        lnlsdepr NUMERIC,
        
        -- Asset Quality
        p3asset NUMERIC,
        p9asset NUMERIC,
        nclnlsr NUMERIC,
        ntlnlsr NUMERIC,
        lnlsntv NUMERIC,
        lnatres NUMERIC,
        lnresncr NUMERIC,
        
        -- Loan Composition
        lnre NUMERIC,
        lnrecons NUMERIC,
        lnrenres NUMERIC,
        lnremult NUMERIC,
        lnreres NUMERIC,
        lnci NUMERIC,
        lncon NUMERIC,
        lncrcd NUMERIC,
        lnag NUMERIC,
        lnoth NUMERIC,
        
        -- Deposit Composition
        depnidom NUMERIC,
        depti NUMERIC,
        depsmamt NUMERIC,
        deplgamt NUMERIC,
        cotefn NUMERIC,
        bkdep NUMERIC,
        
        -- Other
        numemp INTEGER,
        offdom INTEGER,
        offfor INTEGER,
        rssdhcr TEXT,
        
        -- Metadata
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        -- Unique constraint
        UNIQUE (cert, repdte)
    );
    
    -- Create indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_cert ON {table_name} (cert);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_repdte ON {table_name} (repdte);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_cert_repdte ON {table_name} (cert, repdte);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_asset ON {table_name} (asset);
    
    -- Add table comment
    COMMENT ON TABLE {table_name} IS 'FDIC Bank Financials - Balance sheet, income statement, and performance ratios';
    """

    return sql


def generate_institutions_table_sql(table_name: str = "fdic_institutions") -> str:
    """
    Generate CREATE TABLE SQL for bank institutions data.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Primary Key
        id SERIAL PRIMARY KEY,
        
        -- Bank Identification
        cert INTEGER NOT NULL UNIQUE,
        name TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        
        -- Location
        address TEXT,
        city TEXT,
        stalp TEXT,
        stname TEXT,
        zip TEXT,
        county TEXT,
        stcnty TEXT,
        
        -- CBSA/Metro Area
        cbsa TEXT,
        cbsa_div TEXT,
        cbsa_metro TEXT,
        cbsa_metro_name TEXT,
        
        -- Regulatory
        fdicregn TEXT,
        fdicsupv TEXT,
        fed TEXT,
        fedchrtr INTEGER,
        insfdic INTEGER,
        occdist TEXT,
        regagnt TEXT,
        
        -- Charter/Class
        bkclass TEXT,
        charter TEXT,
        chrtagnt TEXT,
        stchrtr INTEGER,
        instcrcd TEXT,
        mutual INTEGER,
        
        -- Dates
        estymd DATE,
        insdate DATE,
        dateupdt DATE,
        rundate DATE,
        
        -- Ownership
        parcert INTEGER,
        newcert INTEGER,
        
        -- Financials (summary)
        asset NUMERIC,
        dep NUMERIC,
        depdom NUMERIC,
        eq NUMERIC,
        netinc NUMERIC,
        roa NUMERIC,
        roe NUMERIC,
        
        -- Branch Info
        offdom INTEGER,
        offfor INTEGER,
        
        -- Other
        specgrp TEXT,
        specgrpn INTEGER,
        webaddr TEXT,
        
        -- Metadata
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (name);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_city ON {table_name} (city);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_stalp ON {table_name} (stalp);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_active ON {table_name} (active);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_asset ON {table_name} (asset);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_bkclass ON {table_name} (bkclass);
    
    -- Add table comment
    COMMENT ON TABLE {table_name} IS 'FDIC Bank Institutions - Bank demographics, locations, and regulatory info';
    """

    return sql


def generate_failed_banks_table_sql(table_name: str = "fdic_failed_banks") -> str:
    """
    Generate CREATE TABLE SQL for failed banks data.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Primary Key
        id SERIAL PRIMARY KEY,
        
        -- Bank Identification
        cert INTEGER NOT NULL,
        name TEXT NOT NULL,
        
        -- Location
        city TEXT,
        state TEXT,
        cityst TEXT,
        
        -- Failure Info
        faildate DATE NOT NULL,
        savession TEXT,
        restype TEXT,
        restype1 TEXT,
        resession TEXT,
        chession TEXT,
        
        -- Financial Estimates
        qbfasset NUMERIC,
        qbfdep NUMERIC,
        cost NUMERIC,
        
        -- Other
        fund TEXT,
        psession TEXT,
        fession TEXT,
        
        -- Metadata
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        -- Unique constraint
        UNIQUE (cert, faildate)
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_{table_name}_cert ON {table_name} (cert);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_faildate ON {table_name} (faildate);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_qbfasset ON {table_name} (qbfasset);
    
    -- Add table comment
    COMMENT ON TABLE {table_name} IS 'FDIC Failed Banks - Historical bank failures and resolution info';
    """

    return sql


def generate_deposits_table_sql(table_name: str = "fdic_summary_deposits") -> str:
    """
    Generate CREATE TABLE SQL for Summary of Deposits (SOD) data.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Primary Key
        id SERIAL PRIMARY KEY,
        
        -- Bank Identification
        cert INTEGER NOT NULL,
        name TEXT,
        year INTEGER NOT NULL,
        
        -- Branch Identification
        uninession TEXT,
        brnum INTEGER,
        brsertyp TEXT,
        mainoff INTEGER,
        
        -- Location
        address TEXT,
        city TEXT,
        stname TEXT,
        stalp TEXT,
        zipbr TEXT,
        county TEXT,
        stcnty TEXT,
        
        -- CBSA/Metro Area
        cbsa TEXT,
        cbsa_div TEXT,
        cbsa_metro TEXT,
        csa TEXT,
        
        -- Geographic Coordinates
        latitude NUMERIC,
        longitude NUMERIC,
        
        -- Financial Data
        asset NUMERIC,
        dession NUMERIC,
        depsum NUMERIC,
        depdom NUMERIC,
        
        -- Classification
        bkclass TEXT,
        charter TEXT,
        specgrp TEXT,
        
        -- Dates
        esession DATE,
        rundate DATE,
        
        -- Metadata
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        -- Unique constraint for branch-level data
        UNIQUE (cert, year, brnum)
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_{table_name}_cert ON {table_name} (cert);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_city ON {table_name} (city);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_stalp ON {table_name} (stalp);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_depsum ON {table_name} (depsum);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_cert_year ON {table_name} (cert, year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_location ON {table_name} (latitude, longitude);
    
    -- Add table comment
    COMMENT ON TABLE {table_name} IS 'FDIC Summary of Deposits - Branch-level deposit data';
    """

    return sql


# =============================================================================
# DATA PARSING UTILITIES
# =============================================================================


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse FDIC date string to ISO format (YYYY-MM-DD).

    FDIC dates can be in various formats:
    - YYYYMMDD (e.g., "20230630")
    - YYYY-MM-DD (already ISO)
    - MM/DD/YYYY

    Args:
        date_str: Date string from FDIC API

    Returns:
        ISO formatted date string or None
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # Already ISO format
    if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
        return date_str

    # YYYYMMDD format
    if len(date_str) == 8 and date_str.isdigit():
        try:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except Exception:
            pass

    # Try parsing various formats
    formats = ["%Y%m%d", "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y"]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def parse_numeric(value: Any) -> Optional[float]:
    """
    Parse numeric value from FDIC API response.

    Args:
        value: Value from API (can be string, int, float, or None)

    Returns:
        Float value or None
    """
    if value is None or value == "" or value == "null":
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_integer(value: Any) -> Optional[int]:
    """
    Parse integer value from FDIC API response.

    Args:
        value: Value from API

    Returns:
        Integer value or None
    """
    if value is None or value == "" or value == "null":
        return None

    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_financials_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a financial record from FDIC API.

    Args:
        record: Raw record from API

    Returns:
        Parsed record with proper types
    """
    data = record.get("data", record)

    parsed = {
        "cert": parse_integer(data.get("CERT")),
        "name": data.get("NAME"),
        "repdte": parse_date(data.get("REPDTE")),
        # Assets
        "asset": parse_numeric(data.get("ASSET")),
        "lnlsnet": parse_numeric(data.get("LNLSNET")),
        "sc": parse_numeric(data.get("SC")),
        "scus": parse_numeric(data.get("SCUS")),
        "scmuni": parse_numeric(data.get("SCMUNI")),
        "chbal": parse_numeric(data.get("CHBAL")),
        "frepo": parse_numeric(data.get("FREPO")),
        "intang": parse_numeric(data.get("INTANG")),
        "oreo": parse_numeric(data.get("OREO")),
        # Liabilities
        "liab": parse_numeric(data.get("LIAB")),
        "dep": parse_numeric(data.get("DEP")),
        "depi": parse_numeric(data.get("DEPI")),
        "depuna": parse_numeric(data.get("DEPUNA")),
        "depdom": parse_numeric(data.get("DEPDOM")),
        "depfor": parse_numeric(data.get("DEPFOR")),
        "frepp": parse_numeric(data.get("FREPP")),
        "othbrf": parse_numeric(data.get("OTHBRF")),
        "subnd": parse_numeric(data.get("SUBND")),
        # Equity
        "eq": parse_numeric(data.get("EQ")),
        "eqtot": parse_numeric(data.get("EQTOT")),
        "rbct1j": parse_numeric(data.get("RBCT1J")),
        "rbct2": parse_numeric(data.get("RBCT2")),
        "rbc1aaj": parse_numeric(data.get("RBC1AAJ")),
        # Income Statement
        "netinc": parse_numeric(data.get("NETINC")),
        "nim": parse_numeric(data.get("NIM")),
        "nimy": parse_numeric(data.get("NIMY")),
        "intinc": parse_numeric(data.get("INTINC")),
        "intexp": parse_numeric(data.get("INTEXP")),
        "netii": parse_numeric(data.get("NETII")),
        "nonii": parse_numeric(data.get("NONII")),
        "nonix": parse_numeric(data.get("NONIX")),
        "eeffr": parse_numeric(data.get("EEFFR")),
        "elnatr": parse_numeric(data.get("ELNATR")),
        "epreem": parse_numeric(data.get("EPREEM")),
        # Performance Ratios
        "roa": parse_numeric(data.get("ROA")),
        "roe": parse_numeric(data.get("ROE")),
        "roaptx": parse_numeric(data.get("ROAPTX")),
        "roaq": parse_numeric(data.get("ROAQ")),
        "roeq": parse_numeric(data.get("ROEQ")),
        # Capital Ratios
        "rbc1rwaj": parse_numeric(data.get("RBC1RWAJ")),
        "rbcrwaj": parse_numeric(data.get("RBCRWAJ")),
        "idt1cer": parse_numeric(data.get("IDT1CER")),
        "lnlsdepr": parse_numeric(data.get("LNLSDEPR")),
        # Asset Quality
        "p3asset": parse_numeric(data.get("P3ASSET")),
        "p9asset": parse_numeric(data.get("P9ASSET")),
        "nclnlsr": parse_numeric(data.get("NCLNLSR")),
        "ntlnlsr": parse_numeric(data.get("NTLNLSR")),
        "lnlsntv": parse_numeric(data.get("LNLSNTV")),
        "lnatres": parse_numeric(data.get("LNATRES")),
        "lnresncr": parse_numeric(data.get("LNRESNCR")),
        # Loan Composition
        "lnre": parse_numeric(data.get("LNRE")),
        "lnrecons": parse_numeric(data.get("LNRECONS")),
        "lnrenres": parse_numeric(data.get("LNRENRES")),
        "lnremult": parse_numeric(data.get("LNREMULT")),
        "lnreres": parse_numeric(data.get("LNRERES")),
        "lnci": parse_numeric(data.get("LNCI")),
        "lncon": parse_numeric(data.get("LNCON")),
        "lncrcd": parse_numeric(data.get("LNCRCD")),
        "lnag": parse_numeric(data.get("LNAG")),
        "lnoth": parse_numeric(data.get("LNOTH")),
        # Deposit Composition
        "depnidom": parse_numeric(data.get("DEPNIDOM")),
        "depti": parse_numeric(data.get("DEPTI")),
        "depsmamt": parse_numeric(data.get("DEPSMAMT")),
        "deplgamt": parse_numeric(data.get("DEPLGAMT")),
        "cotefn": parse_numeric(data.get("COTEFN")),
        "bkdep": parse_numeric(data.get("BKDEP")),
        # Other
        "numemp": parse_integer(data.get("NUMEMP")),
        "offdom": parse_integer(data.get("OFFDOM")),
        "offfor": parse_integer(data.get("OFFFOR")),
        "rssdhcr": data.get("RSSDHCR"),
    }

    return parsed


def parse_institution_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse an institution record from FDIC API.

    Args:
        record: Raw record from API

    Returns:
        Parsed record with proper types
    """
    data = record.get("data", record)

    parsed = {
        "cert": parse_integer(data.get("CERT")),
        "name": data.get("NAME"),
        "active": parse_integer(data.get("ACTIVE")),
        # Location
        "address": data.get("ADDRESS"),
        "city": data.get("CITY"),
        "stalp": data.get("STALP"),
        "stname": data.get("STNAME"),
        "zip": data.get("ZIP"),
        "county": data.get("COUNTY"),
        "stcnty": data.get("STCNTY"),
        # CBSA
        "cbsa": data.get("CBSA"),
        "cbsa_div": data.get("CBSA_DIV"),
        "cbsa_metro": data.get("CBSA_METRO"),
        "cbsa_metro_name": data.get("CBSA_METRO_NAME"),
        # Regulatory
        "fdicregn": data.get("FDICREGN"),
        "fdicsupv": data.get("FDICSUPV"),
        "fed": data.get("FED"),
        "fedchrtr": parse_integer(data.get("FEDCHRTR")),
        "insfdic": parse_integer(data.get("INSFDIC")),
        "occdist": data.get("OCCDIST"),
        "regagnt": data.get("REGAGNT"),
        # Charter/Class
        "bkclass": data.get("BKCLASS"),
        "charter": data.get("CHARTER"),
        "chrtagnt": data.get("CHRTAGNT"),
        "stchrtr": parse_integer(data.get("STCHRTR")),
        "instcrcd": data.get("INSTCRCD"),
        "mutual": parse_integer(data.get("MUTUAL")),
        # Dates
        "estymd": parse_date(data.get("ESTYMD")),
        "insdate": parse_date(data.get("INSDATE")),
        "dateupdt": parse_date(data.get("DATEUPDT")),
        "rundate": parse_date(data.get("RUNDATE")),
        # Ownership
        "parcert": parse_integer(data.get("PARCERT")),
        "newcert": parse_integer(data.get("NEWCERT")),
        # Financials
        "asset": parse_numeric(data.get("ASSET")),
        "dep": parse_numeric(data.get("DEP")),
        "depdom": parse_numeric(data.get("DEPDOM")),
        "eq": parse_numeric(data.get("EQ")),
        "netinc": parse_numeric(data.get("NETINC")),
        "roa": parse_numeric(data.get("ROA")),
        "roe": parse_numeric(data.get("ROE")),
        # Offices
        "offdom": parse_integer(data.get("OFFDOM")),
        "offfor": parse_integer(data.get("OFFFOR")),
        # Other
        "specgrp": data.get("SPECGRP"),
        "specgrpn": parse_integer(data.get("SPECGRPN")),
        "webaddr": data.get("WEBADDR"),
    }

    return parsed


def parse_failed_bank_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a failed bank record from FDIC API.

    Args:
        record: Raw record from API

    Returns:
        Parsed record with proper types
    """
    data = record.get("data", record)

    parsed = {
        "cert": parse_integer(data.get("CERT")),
        "name": data.get("NAME"),
        # Location
        "city": data.get("CITY"),
        "state": data.get("STATE"),
        "cityst": data.get("CITYST"),
        # Failure Info
        "faildate": parse_date(data.get("FAILDATE")),
        "savession": data.get("SAVESSION"),
        "restype": data.get("RESTYPE"),
        "restype1": data.get("RESTYPE1"),
        "resession": data.get("RESESSION"),
        "chession": data.get("CHESSION"),
        # Financials
        "qbfasset": parse_numeric(data.get("QBFASSET")),
        "qbfdep": parse_numeric(data.get("QBFDEP")),
        "cost": parse_numeric(data.get("COST")),
        # Other
        "fund": data.get("FUND"),
        "psession": data.get("PSESSION"),
        "fession": data.get("FESSION"),
    }

    return parsed


def parse_sod_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a Summary of Deposits record from FDIC API.

    Args:
        record: Raw record from API

    Returns:
        Parsed record with proper types
    """
    data = record.get("data", record)

    parsed = {
        "cert": parse_integer(data.get("CERT")),
        "name": data.get("NAME"),
        "year": parse_integer(data.get("YEAR")),
        # Branch Info
        "uninession": data.get("UNINUMBR"),
        "brnum": parse_integer(data.get("BRNUM")),
        "brsertyp": data.get("BRSERTYP"),
        "mainoff": parse_integer(data.get("MAINOFF")),
        # Location
        "address": data.get("ADDRESS"),
        "city": data.get("CITY"),
        "stname": data.get("STNAME"),
        "stalp": data.get("STALP"),
        "zipbr": data.get("ZIPBR"),
        "county": data.get("COUNTY"),
        "stcnty": data.get("STCNTY"),
        # CBSA
        "cbsa": data.get("CBSA"),
        "cbsa_div": data.get("CBSA_DIV"),
        "cbsa_metro": data.get("CBSA_METRO"),
        "csa": data.get("CSA"),
        # Coordinates
        "latitude": parse_numeric(data.get("SIMS_LATITUDE")),
        "longitude": parse_numeric(data.get("SIMS_LONGITUDE")),
        # Financials
        "asset": parse_numeric(data.get("ASSET")),
        "dession": parse_numeric(data.get("DESSION") or data.get("DEPSUMBR")),
        "depsum": parse_numeric(data.get("DEPSUM") or data.get("DEPSUMBR")),
        "depdom": parse_numeric(data.get("DEPDOM")),
        # Classification
        "bkclass": data.get("BKCLASS"),
        "charter": data.get("CHARTER"),
        "specgrp": data.get("SPECGRP"),
        # Dates
        "esession": parse_date(data.get("ESTYMD")),
        "rundate": parse_date(data.get("RUNDATE")),
    }

    return parsed


def get_display_name(dataset: str) -> str:
    """Get display name for dataset."""
    names = {
        "financials": "FDIC Bank Financials",
        "institutions": "FDIC Bank Institutions",
        "failed_banks": "FDIC Failed Banks",
        "summary_deposits": "FDIC Summary of Deposits",
        "sod": "FDIC Summary of Deposits",
    }
    return names.get(dataset.lower(), f"FDIC {dataset.title()}")


def get_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "financials": "Bank balance sheets, income statements, and 1,100+ financial metrics from quarterly call reports",
        "institutions": "Bank demographics including name, location, charter type, regulator, and summary financials",
        "failed_banks": "Historical list of FDIC-insured bank failures with resolution information",
        "summary_deposits": "Branch-level deposit data for all FDIC-insured institutions",
        "sod": "Branch-level deposit data for all FDIC-insured institutions",
    }
    return descriptions.get(dataset.lower(), f"FDIC {dataset} data")
