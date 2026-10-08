"""
CMS dataset metadata and schema definitions.

Maps CMS data to PostgreSQL schemas with proper typing.
"""

import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# CMS Dataset definitions
# NOTE: CMS has transitioned from Socrata to DKAN API format
# Dataset IDs need to be obtained from data.cms.gov for current data
# See: https://downloads.cms.gov/files/Socrata-DKAN-API-Endpoints-Mapping.pdf

DATASETS = {
    "medicare_utilization": {
        "table_name": "cms_medicare_utilization",
        "display_name": "Medicare Provider Utilization and Payment Data",
        "description": "Medicare Part B claims data for physicians and other healthcare practitioners",
        "socrata_dataset_id": None,  # DEPRECATED: CMS moved to DKAN format
        # SPEC_162: the *series* id serves whichever release is latest (2023 until 2026-05-21,
        # 2024 since), so it is never fetched: the ingest reads a pinned per-year id from
        # "dkan_versions" and stores the year in data_year.
        "dkan_series_id": "92396110-2aed-4d63-a6a2-5d6207d46a29",
        "dkan_dataset_id": None,
        # data year -> DKAN dataset id of that year's release (data.cms.gov/data.json, 2026-10-08)
        "dkan_versions": {
            2013: "ad5e7548-98ab-4325-af4b-b2a7099b9351",
            2014: "f63b48ae-946e-48f7-9f56-327a68da4e0b",
            2015: "f8cdb11a-d5f7-4fbe-aac4-05abc8ee2c83",
            2016: "7918e22a-fbfb-4a07-9f59-f8aab2b757d4",
            2017: "85bf3c9c-2244-490d-ad7d-c34e4c28f8ea",
            2018: "fb6d9fe8-38c1-4d24-83d4-0b7b291000b2",
            2019: "867b8ac7-ccb7-4cc9-873d-b24340d89e32",
            2020: "c957b49e-1323-49e7-8678-c09da387551d",
            2021: "31dc2c47-f297-4948-bfb4-075e1bec3a02",
            2022: "e650987d-01b7-4f09-b75e-b0b075afbf98",
            2023: "0e9f2f2b-7bf9-451a-912c-e02e654dd725",
            2024: "335e5f35-eca6-482d-87b3-f99883e213e3",
        },
        "default_data_year": 2024,
        # unique key (alembic 0022): one row per year x rendering NPI x HCPCS x place of service
        "unique_key": ("data_year", "rndrng_npi", "hcpcs_cd", "place_of_srvc"),
        "unique_constraint": "uq_cms_medicare_utilization_year_key",
        "source_url": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service",
        "columns": {
            "data_year": {
                "type": "INTEGER",
                "description": "Calendar year of the Medicare claims (the CMS release year), set by the "
                               "ingest from the pinned DKAN id",
            },
            "rndrng_npi": {
                "type": "TEXT",
                "description": "National Provider Identifier",
            },
            "rndrng_prvdr_last_org_name": {
                "type": "TEXT",
                "description": "Provider Last Name/Organization Name",
            },
            "rndrng_prvdr_first_name": {
                "type": "TEXT",
                "description": "Provider First Name",
            },
            "rndrng_prvdr_mi": {
                "type": "TEXT",
                "description": "Provider Middle Initial",
            },
            "rndrng_prvdr_crdntls": {
                "type": "TEXT",
                "description": "Provider Credentials",
            },
            "rndrng_prvdr_gndr": {"type": "TEXT", "description": "Provider Gender"},
            "rndrng_prvdr_ent_cd": {
                "type": "TEXT",
                "description": "Provider Entity Type Code",
            },
            "rndrng_prvdr_st1": {
                "type": "TEXT",
                "description": "Provider Street Address 1",
            },
            "rndrng_prvdr_st2": {
                "type": "TEXT",
                "description": "Provider Street Address 2",
            },
            "rndrng_prvdr_city": {"type": "TEXT", "description": "Provider City"},
            "rndrng_prvdr_state_abrvtn": {
                "type": "TEXT",
                "description": "Provider State",
            },
            "rndrng_prvdr_state_fips": {
                "type": "TEXT",
                "description": "Provider State FIPS Code",
            },
            "rndrng_prvdr_zip5": {"type": "TEXT", "description": "Provider ZIP Code"},
            "rndrng_prvdr_ruca": {
                "type": "TEXT",
                "description": "Provider Rural-Urban Commuting Area Code",
            },
            "rndrng_prvdr_ruca_desc": {
                "type": "TEXT",
                "description": "Provider RUCA Description",
            },
            "rndrng_prvdr_cntry": {"type": "TEXT", "description": "Provider Country"},
            "rndrng_prvdr_type": {"type": "TEXT", "description": "Provider Type"},
            "rndrng_prvdr_mdcr_prtcptg_ind": {
                "type": "TEXT",
                "description": "Medicare Participation Indicator",
            },
            "hcpcs_cd": {"type": "TEXT", "description": "HCPCS Code"},
            "hcpcs_desc": {"type": "TEXT", "description": "HCPCS Description"},
            "hcpcs_drug_ind": {"type": "TEXT", "description": "HCPCS Drug Indicator"},
            "place_of_srvc": {"type": "TEXT", "description": "Place of Service"},
            "tot_benes": {
                "type": "INTEGER",
                "description": "Total Number of Medicare Beneficiaries",
            },
            "tot_srvcs": {"type": "NUMERIC", "description": "Total Number of Services"},
            "tot_bene_day_srvcs": {
                "type": "INTEGER",
                "description": "Total Beneficiary Day Services",
            },
            "avg_sbmtd_chrg": {
                "type": "NUMERIC",
                "description": "Average Submitted Charge Amount",
            },
            "avg_mdcr_alowd_amt": {
                "type": "NUMERIC",
                "description": "Average Medicare Allowed Amount",
            },
            "avg_mdcr_pymt_amt": {
                "type": "NUMERIC",
                "description": "Average Medicare Payment Amount",
            },
            "avg_mdcr_stdzd_amt": {
                "type": "NUMERIC",
                "description": "Average Medicare Standardized Amount",
            },
        },
    },
    "hospital_cost_reports": {
        "table_name": "cms_hospital_cost_reports",
        "display_name": "Hospital Cost Report Data (HCRIS)",
        "description": "Hospital Cost Reporting Information System data including financial information, utilization data, and cost reports",
        "source_url": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports",
        "columns": {
            "rpt_rec_num": {"type": "TEXT", "description": "Report Record Number"},
            "prvdr_ctrl_type_cd": {
                "type": "TEXT",
                "description": "Provider Control Type Code",
            },
            "prvdr_num": {"type": "TEXT", "description": "Provider CCN Number"},
            "npi": {"type": "TEXT", "description": "National Provider Identifier"},
            "rpt_stus_cd": {"type": "TEXT", "description": "Report Status Code"},
            "fy_bgn_dt": {"type": "DATE", "description": "Fiscal Year Begin Date"},
            "fy_end_dt": {"type": "DATE", "description": "Fiscal Year End Date"},
            "proc_dt": {"type": "DATE", "description": "Process Date"},
            "initl_rpt_sw": {"type": "TEXT", "description": "Initial Report Switch"},
            "last_rpt_sw": {"type": "TEXT", "description": "Last Report Switch"},
            "trnsmtl_num": {"type": "TEXT", "description": "Transmittal Number"},
            "fi_num": {"type": "TEXT", "description": "Fiscal Intermediary Number"},
            "adr_vndr_cd": {
                "type": "TEXT",
                "description": "Automated Desk Review Vendor Code",
            },
            "fi_creat_dt": {"type": "DATE", "description": "FI Create Date"},
            "util_cd": {"type": "TEXT", "description": "Utilization Code"},
            "npr_dt": {
                "type": "DATE",
                "description": "Notice of Program Reimbursement Date",
            },
            "spec_ind": {"type": "TEXT", "description": "Special Indicator"},
            "fi_rcpt_dt": {"type": "DATE", "description": "FI Receipt Date"},
            "bed_cnt": {"type": "INTEGER", "description": "Number of Beds"},
            "tot_charges": {"type": "NUMERIC", "description": "Total Charges"},
            "tot_costs": {"type": "NUMERIC", "description": "Total Costs"},
            "net_income": {"type": "NUMERIC", "description": "Net Income"},
        },
    },
    "drug_pricing": {
        "table_name": "cms_drug_pricing",
        "display_name": "Medicare Part D Drug Spending",
        "description": "Medicare Part D prescription drug costs and utilization by brand name and generic drugs",
        "socrata_dataset_id": None,  # DEPRECATED: CMS moved to DKAN format
        "dkan_dataset_id": "7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b",
        "source_url": "https://data.cms.gov/medicare-drug-spending",
        "columns": {
            "brnd_name": {"type": "TEXT", "description": "Brand Name"},
            "gnrc_name": {"type": "TEXT", "description": "Generic Name"},
            "tot_spndng": {"type": "NUMERIC", "description": "Total Spending"},
            "tot_dsg_unts": {"type": "NUMERIC", "description": "Total Dosage Units"},
            "tot_clms": {"type": "INTEGER", "description": "Total Claims"},
            "tot_benes": {"type": "INTEGER", "description": "Total Beneficiaries"},
            "unit_cnt_per_clm": {
                "type": "NUMERIC",
                "description": "Unit Count Per Claim",
            },
            "spndng_per_dsg_unt": {
                "type": "NUMERIC",
                "description": "Spending Per Dosage Unit",
            },
            "spndng_per_clm": {"type": "NUMERIC", "description": "Spending Per Claim"},
            "spndng_per_bene": {
                "type": "NUMERIC",
                "description": "Spending Per Beneficiary",
            },
            "outlier_flag": {"type": "TEXT", "description": "Outlier Flag"},
            "year": {"type": "INTEGER", "description": "Year"},
        },
    },
}


# Fresh-table DDL for cms_medicare_utilization (SPEC_162). The live table gets data_year and
# the unique key from alembic 0022_cms_utilization_year_key.
UTILIZATION_TABLE_DDL = """CREATE TABLE IF NOT EXISTS cms_medicare_utilization (
    id SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_year INTEGER NOT NULL,
    rndrng_npi TEXT,
    rndrng_prvdr_last_org_name TEXT,
    rndrng_prvdr_first_name TEXT,
    rndrng_prvdr_mi TEXT,
    rndrng_prvdr_crdntls TEXT,
    rndrng_prvdr_gndr TEXT,
    rndrng_prvdr_ent_cd TEXT,
    rndrng_prvdr_st1 TEXT,
    rndrng_prvdr_st2 TEXT,
    rndrng_prvdr_city TEXT,
    rndrng_prvdr_state_abrvtn TEXT,
    rndrng_prvdr_state_fips TEXT,
    rndrng_prvdr_zip5 TEXT,
    rndrng_prvdr_ruca TEXT,
    rndrng_prvdr_ruca_desc TEXT,
    rndrng_prvdr_cntry TEXT,
    rndrng_prvdr_type TEXT,
    rndrng_prvdr_mdcr_prtcptg_ind TEXT,
    hcpcs_cd TEXT,
    hcpcs_desc TEXT,
    hcpcs_drug_ind TEXT,
    place_of_srvc TEXT,
    tot_benes INTEGER,
    tot_srvcs NUMERIC,
    tot_bene_day_srvcs INTEGER,
    avg_sbmtd_chrg NUMERIC,
    avg_mdcr_alowd_amt NUMERIC,
    avg_mdcr_pymt_amt NUMERIC,
    avg_mdcr_stdzd_amt NUMERIC,
    CONSTRAINT uq_cms_medicare_utilization_year_key UNIQUE (data_year, rndrng_npi, hcpcs_cd, place_of_srvc)
);
"""


def get_dataset_metadata(dataset_type: str) -> Dict[str, Any]:
    """
    Get metadata for a specific CMS dataset.

    Args:
        dataset_type: Type of dataset (medicare_utilization, hospital_cost_reports, drug_pricing)

    Returns:
        Dataset metadata dictionary

    Raises:
        ValueError: If dataset type is not supported
    """
    if dataset_type not in DATASETS:
        raise ValueError(
            f"Unknown dataset type: {dataset_type}. "
            f"Supported types: {list(DATASETS.keys())}"
        )

    return DATASETS[dataset_type]


def generate_create_table_sql(dataset_type: str) -> str:
    """
    Generate CREATE TABLE SQL for a CMS dataset.

    Args:
        dataset_type: Type of dataset

    Returns:
        SQL CREATE TABLE statement
    """
    meta = get_dataset_metadata(dataset_type)
    table_name = meta["table_name"]
    columns = meta["columns"]

    if dataset_type == "medicare_utilization":
        # static text, so the column dictionary can read the table (SPEC_162); a test
        # keeps it equal to the "columns" map above
        sql = UTILIZATION_TABLE_DDL + "\n-- Add indexes for common queries\n"
    else:
        # Build column definitions
        col_defs = ["    id SERIAL PRIMARY KEY"]
        col_defs.append("    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        for col_name, col_info in columns.items():
            col_defs.append(f"    {col_name} {col_info['type']}")
        columns_sql = ",\n".join(col_defs)
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
);

-- Add indexes for common queries
"""

    # Add indexes based on dataset type
    if dataset_type == "medicare_utilization":
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_npi ON {table_name}(rndrng_npi);\n"
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name}(rndrng_prvdr_state_abrvtn);\n"
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_hcpcs ON {table_name}(hcpcs_cd);\n"

    elif dataset_type == "hospital_cost_reports":
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_prvdr_num ON {table_name}(prvdr_num);\n"
        sql += (
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_npi ON {table_name}(npi);\n"
        )
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_fy_end ON {table_name}(fy_end_dt);\n"

    elif dataset_type == "drug_pricing":
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_brand ON {table_name}(brnd_name);\n"
        sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_generic ON {table_name}(gnrc_name);\n"
        sql += (
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(year);\n"
        )

    return sql


def dkan_version_for_year(dataset_type: str, year: Optional[int]) -> Tuple[int, str]:
    """(data_year, pinned DKAN dataset id) for ``year`` (None = the dataset's default year).

    Raises ValueError for a year with no pinned release, naming the known years: the old
    code ignored ``year`` and fetched whatever the series served (SPEC_162)."""
    meta = get_dataset_metadata(dataset_type)
    versions = meta.get("dkan_versions") or {}
    data_year = int(year) if year is not None else meta.get("default_data_year")
    if data_year not in versions:
        raise ValueError(
            f"CMS {dataset_type}: no pinned DKAN release for data year {data_year}; "
            f"known years: {sorted(versions)}"
        )
    return data_year, versions[data_year]


def get_column_mapping(dataset_type: str) -> Dict[str, str]:
    """
    Get column mapping for a dataset (maps source columns to DB columns).

    For CMS, source and DB columns are the same (no transformation needed).

    Args:
        dataset_type: Type of dataset

    Returns:
        Dictionary mapping source column -> DB column
    """
    meta = get_dataset_metadata(dataset_type)
    # Identity mapping - CMS column names are already clean
    return {col_name: col_name for col_name in meta["columns"].keys()}
