"""
CMS dataset metadata and schema definitions.

Maps CMS data to PostgreSQL schemas with proper typing.
"""

import logging
from typing import Dict, Any, List

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
        "dkan_dataset_id": None,  # TODO: Get current ID from data.cms.gov
        "source_url": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service",
        "columns": {
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
        "dkan_dataset_id": None,  # TODO: Get current ID from data.cms.gov
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

    # Build column definitions
    col_defs = ["    id SERIAL PRIMARY KEY"]
    col_defs.append("    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    for col_name, col_info in columns.items():
        col_type = col_info["type"]
        col_defs.append(f"    {col_name} {col_type}")

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
