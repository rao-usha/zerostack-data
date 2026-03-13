"""
PE Demo Data Seeder.

Populates realistic PE firms, funds, portfolio companies, financials,
people, and deals for demo purposes. Idempotent — safe to run multiple times.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from datetime import datetime

from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyLeadership,
    PECompanyNews,
    PECompanyValuation,
    PECompetitorMapping,
    PEDeal,
    PEDealParticipant,
    PEFirm,
    PEFund,
    PEFundInvestment,
    PEFundPerformance,
    PEPerson,
    PEFirmPeople,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: upsert by unique columns
# ---------------------------------------------------------------------------

def _upsert_rows(
    db: Session,
    model,
    rows: List[Dict[str, Any]],
    unique_columns: List[str],
    has_db_constraint: bool = True,
    update_columns: Optional[List[str]] = None,
) -> int:
    """Insert rows. If has_db_constraint, uses ON CONFLICT; otherwise select-or-insert."""
    if not rows:
        return 0

    if has_db_constraint:
        return _pg_upsert(db, model, rows, unique_columns, update_columns)
    else:
        return _select_or_insert(db, model, rows, unique_columns)


def _pg_upsert(
    db: Session,
    model,
    rows: List[Dict[str, Any]],
    unique_columns: List[str],
    update_columns: Optional[List[str]] = None,
) -> int:
    """Insert rows using ON CONFLICT (requires DB unique constraint)."""
    table = model.__table__
    if update_columns is None:
        update_columns = [
            c.name for c in table.columns
            if c.name not in unique_columns
            and c.name != "id"
            and not c.server_default
            and c.name not in ("created_at", "updated_at")
        ]

    inserted = 0
    for row in rows:
        stmt = pg_insert(table).values(**row)
        if update_columns:
            update_dict = {col: stmt.excluded[col] for col in update_columns if col in row}
            stmt = stmt.on_conflict_do_update(
                index_elements=unique_columns,
                set_=update_dict,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=unique_columns)
        db.execute(stmt)
        inserted += 1

    db.flush()
    return inserted


def _select_or_insert(
    db: Session,
    model,
    rows: List[Dict[str, Any]],
    unique_columns: List[str],
) -> int:
    """Insert rows using select-then-insert (for tables without unique constraints)."""
    count = 0
    for row in rows:
        stmt = select(model.id)
        for col in unique_columns:
            stmt = stmt.where(getattr(model, col) == row.get(col))
        existing = db.execute(stmt).scalar_one_or_none()
        if existing:
            count += 1
            continue
        db.add(model(**row))
        count += 1
    db.flush()
    return count


def _get_person_firm(person_name: str) -> Optional[str]:
    """Look up a person's firm from PERSON_FIRM_MAP."""
    entry = PERSON_FIRM_MAP.get(person_name)
    return entry[0] if entry else None


def _lookup_id(db: Session, model, **filters) -> Optional[int]:
    """Look up a row's ID by filter columns."""
    stmt = select(model.id)
    for col, val in filters.items():
        stmt = stmt.where(getattr(model, col) == val)
    result = db.execute(stmt).scalar_one_or_none()
    return result


# ---------------------------------------------------------------------------
# Firm data
# ---------------------------------------------------------------------------

FIRMS = [
    {
        "name": "Summit Ridge Partners",
        "legal_name": "Summit Ridge Partners LLC",
        "website": "https://summitridgepartners.com",
        "headquarters_city": "Boston",
        "headquarters_state": "MA",
        "headquarters_country": "USA",
        "firm_type": "PE",
        "primary_strategy": "Buyout",
        "sector_focus": ["Healthcare", "Technology", "Business Services"],
        "geography_focus": ["North America"],
        "aum_usd_millions": Decimal("4200"),
        "employee_count": 68,
        "typical_check_size_min": Decimal("50"),
        "typical_check_size_max": Decimal("250"),
        "target_company_revenue_min": Decimal("30"),
        "target_company_revenue_max": Decimal("500"),
        "target_company_ebitda_min": Decimal("10"),
        "target_company_ebitda_max": Decimal("75"),
        "is_sec_registered": True,
        "founded_year": 2008,
        "status": "Active",
        "data_sources": ["demo_seeder"],
        "confidence_score": Decimal("0.95"),
    },
    {
        "name": "Cascade Growth Equity",
        "legal_name": "Cascade Growth Equity LP",
        "website": "https://cascadegrowth.com",
        "headquarters_city": "San Francisco",
        "headquarters_state": "CA",
        "headquarters_country": "USA",
        "firm_type": "Growth Equity",
        "primary_strategy": "Growth",
        "sector_focus": ["Software", "Fintech", "Data & Analytics"],
        "geography_focus": ["North America", "Europe"],
        "aum_usd_millions": Decimal("2800"),
        "employee_count": 42,
        "typical_check_size_min": Decimal("20"),
        "typical_check_size_max": Decimal("150"),
        "target_company_revenue_min": Decimal("10"),
        "target_company_revenue_max": Decimal("200"),
        "target_company_ebitda_min": Decimal("5"),
        "target_company_ebitda_max": Decimal("40"),
        "is_sec_registered": True,
        "founded_year": 2014,
        "status": "Active",
        "data_sources": ["demo_seeder"],
        "confidence_score": Decimal("0.95"),
    },
    {
        "name": "Ironforge Industrial Capital",
        "legal_name": "Ironforge Industrial Capital LLC",
        "website": "https://ironforgecapital.com",
        "headquarters_city": "Chicago",
        "headquarters_state": "IL",
        "headquarters_country": "USA",
        "firm_type": "PE",
        "primary_strategy": "Sector-Focused Buyout",
        "sector_focus": ["Industrials", "Manufacturing", "Aerospace & Defense"],
        "geography_focus": ["North America"],
        "aum_usd_millions": Decimal("3500"),
        "employee_count": 55,
        "typical_check_size_min": Decimal("75"),
        "typical_check_size_max": Decimal("300"),
        "target_company_revenue_min": Decimal("50"),
        "target_company_revenue_max": Decimal("750"),
        "target_company_ebitda_min": Decimal("15"),
        "target_company_ebitda_max": Decimal("100"),
        "is_sec_registered": True,
        "founded_year": 2005,
        "status": "Active",
        "data_sources": ["demo_seeder"],
        "confidence_score": Decimal("0.95"),
    },
]


# ---------------------------------------------------------------------------
# Fund data (2 per firm)
# ---------------------------------------------------------------------------

FUNDS = {
    "Summit Ridge Partners": [
        {
            "name": "Summit Ridge Partners Fund III",
            "fund_number": 3,
            "vintage_year": 2019,
            "target_size_usd_millions": Decimal("1500"),
            "final_close_usd_millions": Decimal("1650"),
            "called_capital_pct": Decimal("92"),
            "strategy": "Buyout",
            "sector_focus": ["Healthcare", "Technology"],
            "management_fee_pct": Decimal("2.00"),
            "carried_interest_pct": Decimal("20.00"),
            "preferred_return_pct": Decimal("8.00"),
            "fund_life_years": 10,
            "investment_period_years": 5,
            "status": "Harvesting",
            "first_close_date": date(2019, 3, 1),
            "final_close_date": date(2019, 9, 15),
        },
        {
            "name": "Summit Ridge Partners Fund IV",
            "fund_number": 4,
            "vintage_year": 2023,
            "target_size_usd_millions": Decimal("2200"),
            "final_close_usd_millions": Decimal("2550"),
            "called_capital_pct": Decimal("35"),
            "strategy": "Buyout",
            "sector_focus": ["Healthcare", "Business Services", "Technology"],
            "management_fee_pct": Decimal("2.00"),
            "carried_interest_pct": Decimal("20.00"),
            "preferred_return_pct": Decimal("8.00"),
            "fund_life_years": 10,
            "investment_period_years": 5,
            "status": "Active",
            "first_close_date": date(2023, 1, 15),
            "final_close_date": date(2023, 7, 30),
        },
    ],
    "Cascade Growth Equity": [
        {
            "name": "Cascade Growth Fund II",
            "fund_number": 2,
            "vintage_year": 2020,
            "target_size_usd_millions": Decimal("800"),
            "final_close_usd_millions": Decimal("920"),
            "called_capital_pct": Decimal("88"),
            "strategy": "Growth",
            "sector_focus": ["Software", "Fintech"],
            "management_fee_pct": Decimal("2.00"),
            "carried_interest_pct": Decimal("20.00"),
            "preferred_return_pct": Decimal("8.00"),
            "fund_life_years": 10,
            "investment_period_years": 5,
            "status": "Active",
            "first_close_date": date(2020, 6, 1),
            "final_close_date": date(2020, 12, 15),
        },
        {
            "name": "Cascade Growth Fund III",
            "fund_number": 3,
            "vintage_year": 2024,
            "target_size_usd_millions": Decimal("1400"),
            "final_close_usd_millions": Decimal("1500"),
            "called_capital_pct": Decimal("18"),
            "strategy": "Growth",
            "sector_focus": ["Software", "Data & Analytics", "Fintech"],
            "management_fee_pct": Decimal("2.00"),
            "carried_interest_pct": Decimal("20.00"),
            "preferred_return_pct": Decimal("8.00"),
            "fund_life_years": 10,
            "investment_period_years": 5,
            "status": "Active",
            "first_close_date": date(2024, 2, 1),
            "final_close_date": date(2024, 8, 30),
        },
    ],
    "Ironforge Industrial Capital": [
        {
            "name": "Ironforge Industrial Fund IV",
            "fund_number": 4,
            "vintage_year": 2018,
            "target_size_usd_millions": Decimal("1800"),
            "final_close_usd_millions": Decimal("2000"),
            "called_capital_pct": Decimal("95"),
            "strategy": "Buyout",
            "sector_focus": ["Industrials", "Manufacturing"],
            "management_fee_pct": Decimal("1.75"),
            "carried_interest_pct": Decimal("20.00"),
            "preferred_return_pct": Decimal("8.00"),
            "fund_life_years": 10,
            "investment_period_years": 5,
            "status": "Harvesting",
            "first_close_date": date(2018, 4, 1),
            "final_close_date": date(2018, 10, 30),
        },
        {
            "name": "Ironforge Industrial Fund V",
            "fund_number": 5,
            "vintage_year": 2023,
            "target_size_usd_millions": Decimal("2500"),
            "final_close_usd_millions": Decimal("2700"),
            "called_capital_pct": Decimal("40"),
            "strategy": "Buyout",
            "sector_focus": ["Industrials", "Aerospace & Defense", "Manufacturing"],
            "management_fee_pct": Decimal("1.75"),
            "carried_interest_pct": Decimal("20.00"),
            "preferred_return_pct": Decimal("8.00"),
            "fund_life_years": 10,
            "investment_period_years": 5,
            "status": "Active",
            "first_close_date": date(2023, 5, 1),
            "final_close_date": date(2023, 11, 30),
        },
    ],
}


# ---------------------------------------------------------------------------
# Fund performance snapshots
# ---------------------------------------------------------------------------

FUND_PERFORMANCE = {
    "Summit Ridge Partners Fund III": [
        {"as_of_date": date(2024, 6, 30), "reporting_quarter": "Q2 2024", "net_irr_pct": Decimal("18.4"), "gross_irr_pct": Decimal("23.1"), "tvpi": Decimal("1.92"), "dpi": Decimal("0.85"), "rvpi": Decimal("1.07"), "active_investments": 4, "realized_investments": 4, "written_off_investments": 0, "data_source": "demo_seeder"},
        {"as_of_date": date(2025, 6, 30), "reporting_quarter": "Q2 2025", "net_irr_pct": Decimal("19.2"), "gross_irr_pct": Decimal("24.0"), "tvpi": Decimal("2.10"), "dpi": Decimal("1.15"), "rvpi": Decimal("0.95"), "active_investments": 3, "realized_investments": 5, "written_off_investments": 0, "data_source": "demo_seeder"},
    ],
    "Summit Ridge Partners Fund IV": [
        {"as_of_date": date(2025, 6, 30), "reporting_quarter": "Q2 2025", "net_irr_pct": Decimal("12.5"), "gross_irr_pct": Decimal("16.8"), "tvpi": Decimal("1.28"), "dpi": Decimal("0.00"), "rvpi": Decimal("1.28"), "active_investments": 5, "realized_investments": 0, "written_off_investments": 0, "data_source": "demo_seeder"},
    ],
    "Cascade Growth Fund II": [
        {"as_of_date": date(2024, 6, 30), "reporting_quarter": "Q2 2024", "net_irr_pct": Decimal("25.3"), "gross_irr_pct": Decimal("31.0"), "tvpi": Decimal("2.45"), "dpi": Decimal("0.60"), "rvpi": Decimal("1.85"), "active_investments": 6, "realized_investments": 2, "written_off_investments": 1, "data_source": "demo_seeder"},
        {"as_of_date": date(2025, 6, 30), "reporting_quarter": "Q2 2025", "net_irr_pct": Decimal("27.1"), "gross_irr_pct": Decimal("33.2"), "tvpi": Decimal("2.68"), "dpi": Decimal("0.90"), "rvpi": Decimal("1.78"), "active_investments": 5, "realized_investments": 3, "written_off_investments": 1, "data_source": "demo_seeder"},
    ],
    "Cascade Growth Fund III": [
        {"as_of_date": date(2025, 6, 30), "reporting_quarter": "Q2 2025", "net_irr_pct": Decimal("8.2"), "gross_irr_pct": Decimal("11.5"), "tvpi": Decimal("1.10"), "dpi": Decimal("0.00"), "rvpi": Decimal("1.10"), "active_investments": 3, "realized_investments": 0, "written_off_investments": 0, "data_source": "demo_seeder"},
    ],
    "Ironforge Industrial Fund IV": [
        {"as_of_date": date(2024, 6, 30), "reporting_quarter": "Q2 2024", "net_irr_pct": Decimal("15.7"), "gross_irr_pct": Decimal("20.3"), "tvpi": Decimal("1.78"), "dpi": Decimal("0.95"), "rvpi": Decimal("0.83"), "active_investments": 3, "realized_investments": 5, "written_off_investments": 1, "data_source": "demo_seeder"},
        {"as_of_date": date(2025, 6, 30), "reporting_quarter": "Q2 2025", "net_irr_pct": Decimal("16.3"), "gross_irr_pct": Decimal("21.0"), "tvpi": Decimal("1.88"), "dpi": Decimal("1.20"), "rvpi": Decimal("0.68"), "active_investments": 2, "realized_investments": 6, "written_off_investments": 1, "data_source": "demo_seeder"},
    ],
    "Ironforge Industrial Fund V": [
        {"as_of_date": date(2025, 6, 30), "reporting_quarter": "Q2 2025", "net_irr_pct": Decimal("10.8"), "gross_irr_pct": Decimal("14.2"), "tvpi": Decimal("1.18"), "dpi": Decimal("0.00"), "rvpi": Decimal("1.18"), "active_investments": 4, "realized_investments": 0, "written_off_investments": 0, "data_source": "demo_seeder"},
    ],
}


# ---------------------------------------------------------------------------
# Portfolio companies: 8 per firm = 24 total
# ---------------------------------------------------------------------------

PORTFOLIO_COMPANIES = {
    "Summit Ridge Partners": [
        {"name": "MedVantage Health Systems", "industry": "Healthcare", "sub_industry": "Healthcare IT", "headquarters_city": "Nashville", "headquarters_state": "TN", "headquarters_country": "USA", "founded_year": 2011, "employee_count": 420, "ownership_status": "PE-Backed", "current_pe_owner": "Summit Ridge Partners", "is_platform_company": True, "status": "Active", "description": "Cloud-based EHR and revenue cycle management platform for mid-market hospitals and ambulatory care networks."},
        {"name": "Apex Revenue Solutions", "industry": "Business Services", "sub_industry": "Revenue Cycle Management", "headquarters_city": "Atlanta", "headquarters_state": "GA", "headquarters_country": "USA", "founded_year": 2014, "employee_count": 280, "ownership_status": "PE-Backed", "current_pe_owner": "Summit Ridge Partners", "is_platform_company": True, "status": "Active", "description": "End-to-end revenue cycle outsourcing for physician groups and specialty practices."},
        {"name": "CloudShield Security", "industry": "Technology", "sub_industry": "Cybersecurity", "headquarters_city": "Austin", "headquarters_state": "TX", "headquarters_country": "USA", "founded_year": 2016, "employee_count": 195, "ownership_status": "PE-Backed", "current_pe_owner": "Summit Ridge Partners", "is_platform_company": True, "status": "Active", "description": "Cloud-native SIEM and XDR platform for mid-market enterprises with managed SOC services."},
        {"name": "TrueNorth Behavioral", "industry": "Healthcare", "sub_industry": "Behavioral Health", "headquarters_city": "Denver", "headquarters_state": "CO", "headquarters_country": "USA", "founded_year": 2013, "employee_count": 850, "ownership_status": "PE-Backed", "current_pe_owner": "Summit Ridge Partners", "is_platform_company": True, "status": "Active", "description": "Multi-site outpatient behavioral health platform spanning 12 states, treating anxiety, depression, and substance use disorders."},
        {"name": "Precision Lab Diagnostics", "industry": "Healthcare", "sub_industry": "Clinical Laboratories", "headquarters_city": "Phoenix", "headquarters_state": "AZ", "headquarters_country": "USA", "founded_year": 2009, "employee_count": 340, "ownership_status": "PE-Backed", "current_pe_owner": "Summit Ridge Partners", "is_platform_company": False, "status": "Active", "description": "Regional clinical laboratory network specializing in toxicology and molecular diagnostics."},
        {"name": "Elevate Staffing Group", "industry": "Business Services", "sub_industry": "Staffing & Recruiting", "headquarters_city": "Charlotte", "headquarters_state": "NC", "headquarters_country": "USA", "founded_year": 2015, "employee_count": 160, "ownership_status": "PE-Backed", "current_pe_owner": "Summit Ridge Partners", "is_platform_company": True, "status": "Active", "description": "Technology-enabled healthcare staffing platform connecting travel nurses and allied health professionals."},
        {"name": "DataBridge Analytics", "industry": "Technology", "sub_industry": "Data & Analytics", "headquarters_city": "Raleigh", "headquarters_state": "NC", "headquarters_country": "USA", "founded_year": 2017, "employee_count": 110, "ownership_status": "Exited", "current_pe_owner": None, "is_platform_company": False, "status": "Exited", "description": "Healthcare analytics platform providing payer-provider benchmarking and population health insights."},
        {"name": "NovaCare Urgent Clinics", "industry": "Healthcare", "sub_industry": "Urgent Care", "headquarters_city": "Tampa", "headquarters_state": "FL", "headquarters_country": "USA", "founded_year": 2012, "employee_count": 520, "ownership_status": "Exited", "current_pe_owner": None, "is_platform_company": True, "status": "Exited", "description": "42-location urgent care chain across the Southeast, sold to national strategic acquirer in 2024."},
    ],
    "Cascade Growth Equity": [
        {"name": "FinLedger Technologies", "industry": "Software", "sub_industry": "Fintech", "headquarters_city": "New York", "headquarters_state": "NY", "headquarters_country": "USA", "founded_year": 2017, "employee_count": 230, "ownership_status": "PE-Backed", "current_pe_owner": "Cascade Growth Equity", "is_platform_company": True, "status": "Active", "description": "API-first embedded lending platform enabling banks and fintechs to offer real-time credit decisioning."},
        {"name": "Nimbus Data Cloud", "industry": "Software", "sub_industry": "Cloud Infrastructure", "headquarters_city": "Seattle", "headquarters_state": "WA", "headquarters_country": "USA", "founded_year": 2018, "employee_count": 175, "ownership_status": "PE-Backed", "current_pe_owner": "Cascade Growth Equity", "is_platform_company": True, "status": "Active", "description": "Multi-cloud data lakehouse platform with built-in governance and real-time analytics for enterprise customers."},
        {"name": "VeriComply AI", "industry": "Software", "sub_industry": "RegTech", "headquarters_city": "Boston", "headquarters_state": "MA", "headquarters_country": "USA", "founded_year": 2019, "employee_count": 95, "ownership_status": "PE-Backed", "current_pe_owner": "Cascade Growth Equity", "is_platform_company": True, "status": "Active", "description": "AI-powered compliance monitoring for financial institutions — AML, KYC, and sanctions screening automation."},
        {"name": "PayGrid Systems", "industry": "Software", "sub_industry": "Payments", "headquarters_city": "San Francisco", "headquarters_state": "CA", "headquarters_country": "USA", "founded_year": 2016, "employee_count": 310, "ownership_status": "PE-Backed", "current_pe_owner": "Cascade Growth Equity", "is_platform_company": True, "status": "Active", "description": "B2B payment orchestration platform processing $45B+ annually across ACH, wire, and real-time payment rails."},
        {"name": "InsightFlow Analytics", "industry": "Software", "sub_industry": "Business Intelligence", "headquarters_city": "Chicago", "headquarters_state": "IL", "headquarters_country": "USA", "founded_year": 2015, "employee_count": 140, "ownership_status": "PE-Backed", "current_pe_owner": "Cascade Growth Equity", "is_platform_company": True, "status": "Active", "description": "Self-service BI platform for mid-market companies with embedded AI copilot for natural language querying."},
        {"name": "ShieldPay Fraud Detection", "industry": "Software", "sub_industry": "Fintech", "headquarters_city": "Austin", "headquarters_state": "TX", "headquarters_country": "USA", "founded_year": 2020, "employee_count": 65, "ownership_status": "PE-Backed", "current_pe_owner": "Cascade Growth Equity", "is_platform_company": False, "status": "Active", "description": "Real-time transaction fraud detection using graph neural networks, serving 40+ banks and payment processors."},
        {"name": "CloudMetrics Pro", "industry": "Software", "sub_industry": "DevOps", "headquarters_city": "Portland", "headquarters_state": "OR", "headquarters_country": "USA", "founded_year": 2019, "employee_count": 85, "ownership_status": "Exited", "current_pe_owner": None, "is_platform_company": False, "status": "Exited", "description": "Cloud cost optimization and infrastructure monitoring platform. Acquired by Datadog in 2025."},
        {"name": "TrueVault Data", "industry": "Software", "sub_industry": "Data Security", "headquarters_city": "Denver", "headquarters_state": "CO", "headquarters_country": "USA", "founded_year": 2018, "employee_count": 55, "ownership_status": "Exited", "current_pe_owner": None, "is_platform_company": False, "status": "Exited", "description": "Data privacy vault for healthcare and financial services. Written off in 2024 — product-market fit challenges."},
    ],
    "Ironforge Industrial Capital": [
        {"name": "Titan Precision Manufacturing", "industry": "Industrials", "sub_industry": "Precision Manufacturing", "headquarters_city": "Cincinnati", "headquarters_state": "OH", "headquarters_country": "USA", "founded_year": 2001, "employee_count": 780, "ownership_status": "PE-Backed", "current_pe_owner": "Ironforge Industrial Capital", "is_platform_company": True, "status": "Active", "description": "CNC machining and precision-engineered components for aerospace and defense prime contractors."},
        {"name": "AeroSpec Coatings", "industry": "Industrials", "sub_industry": "Specialty Coatings", "headquarters_city": "Wichita", "headquarters_state": "KS", "headquarters_country": "USA", "founded_year": 2006, "employee_count": 320, "ownership_status": "PE-Backed", "current_pe_owner": "Ironforge Industrial Capital", "is_platform_company": True, "status": "Active", "description": "Aerospace-grade thermal and corrosion-resistant coatings for engine components and structural parts."},
        {"name": "Continental Packaging Solutions", "industry": "Industrials", "sub_industry": "Packaging", "headquarters_city": "Milwaukee", "headquarters_state": "WI", "headquarters_country": "USA", "founded_year": 2003, "employee_count": 920, "ownership_status": "PE-Backed", "current_pe_owner": "Ironforge Industrial Capital", "is_platform_company": True, "status": "Active", "description": "Custom corrugated and rigid packaging for food, beverage, and consumer goods manufacturers."},
        {"name": "Midwest Valve & Controls", "industry": "Industrials", "sub_industry": "Flow Control", "headquarters_city": "Houston", "headquarters_state": "TX", "headquarters_country": "USA", "founded_year": 1998, "employee_count": 450, "ownership_status": "PE-Backed", "current_pe_owner": "Ironforge Industrial Capital", "is_platform_company": True, "status": "Active", "description": "Engineered valves, actuators, and flow control systems for oil & gas, chemical, and water infrastructure."},
        {"name": "SteelCore Fabrication", "industry": "Industrials", "sub_industry": "Steel Fabrication", "headquarters_city": "Pittsburgh", "headquarters_state": "PA", "headquarters_country": "USA", "founded_year": 2007, "employee_count": 380, "ownership_status": "PE-Backed", "current_pe_owner": "Ironforge Industrial Capital", "is_platform_company": False, "status": "Active", "description": "Structural steel fabrication and erection for commercial, industrial, and infrastructure projects."},
        {"name": "DefenseTech Systems", "industry": "Industrials", "sub_industry": "Aerospace & Defense", "headquarters_city": "Huntsville", "headquarters_state": "AL", "headquarters_country": "USA", "founded_year": 2010, "employee_count": 260, "ownership_status": "PE-Backed", "current_pe_owner": "Ironforge Industrial Capital", "is_platform_company": True, "status": "Active", "description": "Electronic warfare subsystems and ruggedized computing platforms for DoD programs."},
        {"name": "Great Lakes Plastics", "industry": "Industrials", "sub_industry": "Plastics & Composites", "headquarters_city": "Grand Rapids", "headquarters_state": "MI", "headquarters_country": "USA", "founded_year": 2004, "employee_count": 290, "ownership_status": "Exited", "current_pe_owner": None, "is_platform_company": False, "status": "Exited", "description": "Injection-molded plastic components for automotive and appliance OEMs. Sold to strategic in 2024."},
        {"name": "Northwest Automation Group", "industry": "Industrials", "sub_industry": "Industrial Automation", "headquarters_city": "Minneapolis", "headquarters_state": "MN", "headquarters_country": "USA", "founded_year": 2008, "employee_count": 170, "ownership_status": "Exited", "current_pe_owner": None, "is_platform_company": False, "status": "Exited", "description": "Turnkey industrial automation solutions — robotic cells, PLC programming, and vision systems for manufacturing."},
    ],
}


# ---------------------------------------------------------------------------
# Financials: 5 years per active company (2021-2025)
# ---------------------------------------------------------------------------

def _generate_financials(company_name: str, base_revenue: float, base_ebitda_margin: float,
                         growth_rates: List[float], debt_ratio: float) -> List[Dict]:
    """Generate 5 years of financial data with realistic progression."""
    records = []
    revenue = base_revenue
    for i, yr in enumerate(range(2021, 2026)):
        growth = growth_rates[i] if i < len(growth_rates) else growth_rates[-1]
        if i > 0:
            revenue = revenue * (1 + growth / 100)
        ebitda_margin = base_ebitda_margin + (i * 0.5)  # slight margin expansion
        ebitda = revenue * ebitda_margin / 100
        records.append({
            "fiscal_year": yr,
            "fiscal_period": "FY",
            "period_end_date": date(yr, 12, 31),
            "revenue_usd": Decimal(str(round(revenue * 1_000_000, 2))),
            "revenue_growth_pct": Decimal(str(round(growth, 2))) if i > 0 else None,
            "ebitda_usd": Decimal(str(round(ebitda * 1_000_000, 2))),
            "ebitda_margin_pct": Decimal(str(round(ebitda_margin, 2))),
            "gross_margin_pct": Decimal(str(round(ebitda_margin + 18, 2))),
            "gross_profit_usd": Decimal(str(round(revenue * (ebitda_margin + 18) / 100 * 1_000_000, 2))),
            "net_income_usd": Decimal(str(round(ebitda * 0.55 * 1_000_000, 2))),
            "total_debt_usd": Decimal(str(round(revenue * debt_ratio * 1_000_000, 2))),
            "cash_usd": Decimal(str(round(revenue * 0.08 * 1_000_000, 2))),
            "net_debt_usd": Decimal(str(round(revenue * (debt_ratio - 0.08) * 1_000_000, 2))),
            "total_assets_usd": Decimal(str(round(revenue * 1.8 * 1_000_000, 2))),
            "shareholders_equity_usd": Decimal(str(round(revenue * (1.8 - debt_ratio) * 1_000_000, 2))),
            "operating_cash_flow_usd": Decimal(str(round(ebitda * 0.7 * 1_000_000, 2))),
            "capex_usd": Decimal(str(round(revenue * 0.04 * 1_000_000, 2))),
            "free_cash_flow_usd": Decimal(str(round((ebitda * 0.7 - revenue * 0.04) * 1_000_000, 2))),
            "debt_to_ebitda": Decimal(str(round(revenue * debt_ratio / ebitda, 2))) if ebitda > 0 else None,
            "interest_coverage": Decimal(str(round(ebitda / (revenue * debt_ratio * 0.06), 2))) if debt_ratio > 0 else None,
            "is_audited": yr < 2026,
            "is_estimated": yr >= 2025,
            "data_source": "demo_seeder",
            "confidence": "high" if yr < 2025 else "medium",
        })
    return records


COMPANY_FINANCIALS = {
    # Summit Ridge — Healthcare + Services
    "MedVantage Health Systems":    {"base_revenue": 85,  "base_ebitda_margin": 22, "growth_rates": [0, 15, 18, 22, 20], "debt_ratio": 0.45},
    "Apex Revenue Solutions":       {"base_revenue": 52,  "base_ebitda_margin": 25, "growth_rates": [0, 12, 14, 16, 13], "debt_ratio": 0.35},
    "CloudShield Security":         {"base_revenue": 28,  "base_ebitda_margin": 18, "growth_rates": [0, 35, 42, 38, 30], "debt_ratio": 0.20},
    "TrueNorth Behavioral":         {"base_revenue": 120, "base_ebitda_margin": 20, "growth_rates": [0, 25, 30, 28, 22], "debt_ratio": 0.50},
    "Precision Lab Diagnostics":    {"base_revenue": 65,  "base_ebitda_margin": 28, "growth_rates": [0, 8, 10, 12, 9],   "debt_ratio": 0.40},
    "Elevate Staffing Group":       {"base_revenue": 35,  "base_ebitda_margin": 15, "growth_rates": [0, 20, 25, 22, 18], "debt_ratio": 0.30},
    "DataBridge Analytics":         {"base_revenue": 18,  "base_ebitda_margin": 12, "growth_rates": [0, 40, 55, 45, 35], "debt_ratio": 0.15},
    "NovaCare Urgent Clinics":      {"base_revenue": 95,  "base_ebitda_margin": 18, "growth_rates": [0, 20, 22, 18, 15], "debt_ratio": 0.55},
    # Cascade Growth — Software
    "FinLedger Technologies":       {"base_revenue": 22,  "base_ebitda_margin": 10, "growth_rates": [0, 45, 55, 50, 40], "debt_ratio": 0.10},
    "Nimbus Data Cloud":            {"base_revenue": 15,  "base_ebitda_margin": 5,  "growth_rates": [0, 60, 70, 55, 45], "debt_ratio": 0.05},
    "VeriComply AI":                {"base_revenue": 8,   "base_ebitda_margin": -5, "growth_rates": [0, 80, 95, 70, 55], "debt_ratio": 0.05},
    "PayGrid Systems":              {"base_revenue": 55,  "base_ebitda_margin": 20, "growth_rates": [0, 25, 30, 28, 22], "debt_ratio": 0.15},
    "InsightFlow Analytics":        {"base_revenue": 30,  "base_ebitda_margin": 15, "growth_rates": [0, 22, 28, 25, 20], "debt_ratio": 0.10},
    "ShieldPay Fraud Detection":    {"base_revenue": 5,   "base_ebitda_margin": -15,"growth_rates": [0, 100, 120, 80, 60],"debt_ratio": 0.05},
    "CloudMetrics Pro":             {"base_revenue": 12,  "base_ebitda_margin": 8,  "growth_rates": [0, 50, 60, 45, 35], "debt_ratio": 0.05},
    "TrueVault Data":               {"base_revenue": 3,   "base_ebitda_margin": -40,"growth_rates": [0, 30, 10, -5, -20],"debt_ratio": 0.10},
    # Ironforge — Industrials
    "Titan Precision Manufacturing":{"base_revenue": 180, "base_ebitda_margin": 22, "growth_rates": [0, 8, 10, 12, 9],   "debt_ratio": 0.50},
    "AeroSpec Coatings":            {"base_revenue": 75,  "base_ebitda_margin": 26, "growth_rates": [0, 10, 12, 15, 11], "debt_ratio": 0.40},
    "Continental Packaging Solutions":{"base_revenue": 210,"base_ebitda_margin": 18, "growth_rates": [0, 6, 8, 10, 7],   "debt_ratio": 0.55},
    "Midwest Valve & Controls":     {"base_revenue": 110, "base_ebitda_margin": 24, "growth_rates": [0, 5, 7, 9, 6],    "debt_ratio": 0.45},
    "SteelCore Fabrication":        {"base_revenue": 90,  "base_ebitda_margin": 16, "growth_rates": [0, 4, 6, 8, 5],    "debt_ratio": 0.50},
    "DefenseTech Systems":          {"base_revenue": 55,  "base_ebitda_margin": 20, "growth_rates": [0, 15, 18, 22, 20], "debt_ratio": 0.30},
    "Great Lakes Plastics":         {"base_revenue": 65,  "base_ebitda_margin": 19, "growth_rates": [0, 3, 5, 7, 4],    "debt_ratio": 0.45},
    "Northwest Automation Group":   {"base_revenue": 40,  "base_ebitda_margin": 14, "growth_rates": [0, 12, 15, 18, 14], "debt_ratio": 0.25},
}


# ---------------------------------------------------------------------------
# People: 20+ across the 3 firms
# ---------------------------------------------------------------------------

PEOPLE = [
    # Summit Ridge Partners
    {"full_name": "James Harrington", "first_name": "James", "last_name": "Harrington", "current_title": "Managing Partner", "current_company": "Summit Ridge Partners", "city": "Boston", "state": "MA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Sarah Chen", "first_name": "Sarah", "last_name": "Chen", "current_title": "Partner, Healthcare", "current_company": "Summit Ridge Partners", "city": "Boston", "state": "MA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Michael Torres", "first_name": "Michael", "last_name": "Torres", "current_title": "Partner, Technology", "current_company": "Summit Ridge Partners", "city": "Austin", "state": "TX", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Amanda Brooks", "first_name": "Amanda", "last_name": "Brooks", "current_title": "Principal", "current_company": "Summit Ridge Partners", "city": "Boston", "state": "MA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "David Patel", "first_name": "David", "last_name": "Patel", "current_title": "Vice President", "current_company": "Summit Ridge Partners", "city": "Boston", "state": "MA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Jennifer Walsh", "first_name": "Jennifer", "last_name": "Walsh", "current_title": "CFO", "current_company": "Summit Ridge Partners", "city": "Boston", "state": "MA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Robert Kim", "first_name": "Robert", "last_name": "Kim", "current_title": "Operating Partner", "current_company": "Summit Ridge Partners", "city": "Nashville", "state": "TN", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    # Cascade Growth Equity
    {"full_name": "Elena Vasquez", "first_name": "Elena", "last_name": "Vasquez", "current_title": "Founder & Managing Partner", "current_company": "Cascade Growth Equity", "city": "San Francisco", "state": "CA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Ryan Mitchell", "first_name": "Ryan", "last_name": "Mitchell", "current_title": "Partner", "current_company": "Cascade Growth Equity", "city": "San Francisco", "state": "CA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Lisa Nakamura", "first_name": "Lisa", "last_name": "Nakamura", "current_title": "Partner", "current_company": "Cascade Growth Equity", "city": "New York", "state": "NY", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Andrew Foster", "first_name": "Andrew", "last_name": "Foster", "current_title": "Principal", "current_company": "Cascade Growth Equity", "city": "San Francisco", "state": "CA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Priya Sharma", "first_name": "Priya", "last_name": "Sharma", "current_title": "Vice President", "current_company": "Cascade Growth Equity", "city": "San Francisco", "state": "CA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Marcus Thompson", "first_name": "Marcus", "last_name": "Thompson", "current_title": "Operating Partner", "current_company": "Cascade Growth Equity", "city": "Seattle", "state": "WA", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    # Ironforge Industrial Capital
    {"full_name": "William Blackwell", "first_name": "William", "last_name": "Blackwell", "current_title": "Founder & CEO", "current_company": "Ironforge Industrial Capital", "city": "Chicago", "state": "IL", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Katherine Dawson", "first_name": "Katherine", "last_name": "Dawson", "current_title": "Managing Director", "current_company": "Ironforge Industrial Capital", "city": "Chicago", "state": "IL", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Thomas O'Brien", "first_name": "Thomas", "last_name": "O'Brien", "current_title": "Partner, A&D", "current_company": "Ironforge Industrial Capital", "city": "Chicago", "state": "IL", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Maria Rodriguez", "first_name": "Maria", "last_name": "Rodriguez", "current_title": "Principal", "current_company": "Ironforge Industrial Capital", "city": "Houston", "state": "TX", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Daniel Kowalski", "first_name": "Daniel", "last_name": "Kowalski", "current_title": "Vice President", "current_company": "Ironforge Industrial Capital", "city": "Chicago", "state": "IL", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Susan Chang", "first_name": "Susan", "last_name": "Chang", "current_title": "CFO", "current_company": "Ironforge Industrial Capital", "city": "Chicago", "state": "IL", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Richard Hoffman", "first_name": "Richard", "last_name": "Hoffman", "current_title": "Operating Partner", "current_company": "Ironforge Industrial Capital", "city": "Cincinnati", "state": "OH", "country": "USA", "is_active": True, "data_sources": ["demo_seeder"]},
]

# Map person → firm for firm_people
PERSON_FIRM_MAP = {
    "James Harrington": ("Summit Ridge Partners", "Managing Partner", "senior_partner"),
    "Sarah Chen": ("Summit Ridge Partners", "Partner, Healthcare", "partner"),
    "Michael Torres": ("Summit Ridge Partners", "Partner, Technology", "partner"),
    "Amanda Brooks": ("Summit Ridge Partners", "Principal", "principal"),
    "David Patel": ("Summit Ridge Partners", "Vice President", "vice_president"),
    "Jennifer Walsh": ("Summit Ridge Partners", "CFO", "c_suite"),
    "Robert Kim": ("Summit Ridge Partners", "Operating Partner", "operating_partner"),
    "Elena Vasquez": ("Cascade Growth Equity", "Founder & Managing Partner", "senior_partner"),
    "Ryan Mitchell": ("Cascade Growth Equity", "Partner", "partner"),
    "Lisa Nakamura": ("Cascade Growth Equity", "Partner", "partner"),
    "Andrew Foster": ("Cascade Growth Equity", "Principal", "principal"),
    "Priya Sharma": ("Cascade Growth Equity", "Vice President", "vice_president"),
    "Marcus Thompson": ("Cascade Growth Equity", "Operating Partner", "operating_partner"),
    "William Blackwell": ("Ironforge Industrial Capital", "Founder & CEO", "senior_partner"),
    "Katherine Dawson": ("Ironforge Industrial Capital", "Managing Director", "partner"),
    "Thomas O'Brien": ("Ironforge Industrial Capital", "Partner, A&D", "partner"),
    "Maria Rodriguez": ("Ironforge Industrial Capital", "Principal", "principal"),
    "Daniel Kowalski": ("Ironforge Industrial Capital", "Vice President", "vice_president"),
    "Susan Chang": ("Ironforge Industrial Capital", "CFO", "c_suite"),
    "Richard Hoffman": ("Ironforge Industrial Capital", "Operating Partner", "operating_partner"),
}


# ---------------------------------------------------------------------------
# Portfolio Company Executives (~2-3 C-suite per company)
# ---------------------------------------------------------------------------

COMPANY_EXECUTIVES = [
    # Summit Ridge — Healthcare + Tech + Services
    # MedVantage Health Systems
    {"full_name": "Dr. Robert Chen", "first_name": "Robert", "last_name": "Chen", "current_title": "CEO", "current_company": "MedVantage Health Systems", "city": "Nashville", "state": "TN", "country": "USA", "bio": "20+ years healthcare operations; former COO at HCA Healthcare division.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Patricia Flores", "first_name": "Patricia", "last_name": "Flores", "current_title": "CFO", "current_company": "MedVantage Health Systems", "city": "Nashville", "state": "TN", "country": "USA", "bio": "Former VP Finance at Envision Healthcare; CPA, MBA Vanderbilt.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Dr. Alan Gupta", "first_name": "Alan", "last_name": "Gupta", "current_title": "CMO", "current_company": "MedVantage Health Systems", "city": "Nashville", "state": "TN", "country": "USA", "bio": "Board-certified internal medicine; led clinical quality at Tenet Healthcare.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Apex Revenue Solutions
    {"full_name": "Kevin McCarthy", "first_name": "Kevin", "last_name": "McCarthy", "current_title": "CEO", "current_company": "Apex Revenue Solutions", "city": "Dallas", "state": "TX", "country": "USA", "bio": "Serial entrepreneur; founded two prior RCM companies, both PE-exited.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Linda Tran", "first_name": "Linda", "last_name": "Tran", "current_title": "CFO", "current_company": "Apex Revenue Solutions", "city": "Dallas", "state": "TX", "country": "USA", "bio": "Former Director FP&A at R1 RCM; expertise in healthcare billing analytics.", "is_active": True, "data_sources": ["demo_seeder"]},
    # CloudShield Security
    {"full_name": "Jason Park", "first_name": "Jason", "last_name": "Park", "current_title": "CEO & Co-Founder", "current_company": "CloudShield Security", "city": "Austin", "state": "TX", "country": "USA", "bio": "Former VP Engineering at CrowdStrike; built endpoint detection product line.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Rachel Adams", "first_name": "Rachel", "last_name": "Adams", "current_title": "CFO", "current_company": "CloudShield Security", "city": "Austin", "state": "TX", "country": "USA", "bio": "CFO track from Goldman Sachs TMT → SailPoint CFO; IPO experience.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Dmitri Volkov", "first_name": "Dmitri", "last_name": "Volkov", "current_title": "CTO", "current_company": "CloudShield Security", "city": "Austin", "state": "TX", "country": "USA", "bio": "PhD CS Stanford; former Principal Engineer at Palo Alto Networks.", "is_active": True, "data_sources": ["demo_seeder"]},
    # TrueNorth Behavioral
    {"full_name": "Dr. Margaret Sullivan", "first_name": "Margaret", "last_name": "Sullivan", "current_title": "CEO", "current_company": "TrueNorth Behavioral", "city": "Phoenix", "state": "AZ", "country": "USA", "bio": "25 years behavioral health; former President of Acadia Healthcare West region.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Steven Greenfield", "first_name": "Steven", "last_name": "Greenfield", "current_title": "CFO", "current_company": "TrueNorth Behavioral", "city": "Phoenix", "state": "AZ", "country": "USA", "bio": "Healthcare CFO with multi-site experience; former CFO at Universal Health Services division.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Dr. Anita Rao", "first_name": "Anita", "last_name": "Rao", "current_title": "Chief Clinical Officer", "current_company": "TrueNorth Behavioral", "city": "Phoenix", "state": "AZ", "country": "USA", "bio": "Psychiatrist; designed evidence-based treatment protocols adopted by 50+ facilities.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Precision Lab Diagnostics
    {"full_name": "Frank DeLuca", "first_name": "Frank", "last_name": "DeLuca", "current_title": "CEO", "current_company": "Precision Lab Diagnostics", "city": "Philadelphia", "state": "PA", "country": "USA", "bio": "Former SVP Operations at Quest Diagnostics; scaled lab network from 40 to 120 sites.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Janet Okonkwo", "first_name": "Janet", "last_name": "Okonkwo", "current_title": "CFO", "current_company": "Precision Lab Diagnostics", "city": "Philadelphia", "state": "PA", "country": "USA", "bio": "CPA; former audit partner at Deloitte focused on healthcare and life sciences.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Elevate Staffing Group
    {"full_name": "Marcus Whitfield", "first_name": "Marcus", "last_name": "Whitfield", "current_title": "CEO", "current_company": "Elevate Staffing Group", "city": "Charlotte", "state": "NC", "country": "USA", "bio": "Built staffing platform from $5M to $80M revenue; former Randstad regional VP.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Diane Hoffman", "first_name": "Diane", "last_name": "Hoffman", "current_title": "CFO", "current_company": "Elevate Staffing Group", "city": "Charlotte", "state": "NC", "country": "USA", "bio": "Staffing industry finance veteran; built financial infrastructure for rapid M&A integration.", "is_active": True, "data_sources": ["demo_seeder"]},
    # DataBridge Analytics (exited)
    {"full_name": "Yusuf Ibrahim", "first_name": "Yusuf", "last_name": "Ibrahim", "current_title": "CEO", "current_company": "DataBridge Analytics", "city": "Boston", "state": "MA", "country": "USA", "bio": "Data science PhD MIT; founded DataBridge in 2017 to democratize healthcare analytics.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Samantha Lee", "first_name": "Samantha", "last_name": "Lee", "current_title": "CFO", "current_company": "DataBridge Analytics", "city": "Boston", "state": "MA", "country": "USA", "bio": "Early-stage finance leader; guided DataBridge from Series A through strategic exit.", "is_active": True, "data_sources": ["demo_seeder"]},
    # NovaCare Urgent Clinics (exited)
    {"full_name": "Dr. James Morton", "first_name": "James", "last_name": "Morton", "current_title": "CEO", "current_company": "NovaCare Urgent Clinics", "city": "Atlanta", "state": "GA", "country": "USA", "bio": "Emergency medicine physician; built NovaCare from 8 to 45 clinic locations.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Carolyn Briggs", "first_name": "Carolyn", "last_name": "Briggs", "current_title": "CFO", "current_company": "NovaCare Urgent Clinics", "city": "Atlanta", "state": "GA", "country": "USA", "bio": "Healthcare multi-site financial ops; managed $150M+ P&L at TeamHealth.", "is_active": True, "data_sources": ["demo_seeder"]},

    # Cascade Growth — Software + Fintech
    # FinLedger Technologies
    {"full_name": "Nathan Cho", "first_name": "Nathan", "last_name": "Cho", "current_title": "CEO & Founder", "current_company": "FinLedger Technologies", "city": "New York", "state": "NY", "country": "USA", "bio": "Former Goldman Sachs quant; founded FinLedger to modernize trade settlement.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Elizabeth Warren-Hughes", "first_name": "Elizabeth", "last_name": "Warren-Hughes", "current_title": "CFO", "current_company": "FinLedger Technologies", "city": "New York", "state": "NY", "country": "USA", "bio": "Fintech CFO; led financial operations at Stripe and Plaid before FinLedger.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Nimbus Data Cloud
    {"full_name": "Arjun Mehta", "first_name": "Arjun", "last_name": "Mehta", "current_title": "CEO & Co-Founder", "current_company": "Nimbus Data Cloud", "city": "Seattle", "state": "WA", "country": "USA", "bio": "Former AWS principal engineer; built Nimbus to simplify multi-cloud data pipelines.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Claire Donovan", "first_name": "Claire", "last_name": "Donovan", "current_title": "CFO", "current_company": "Nimbus Data Cloud", "city": "Seattle", "state": "WA", "country": "USA", "bio": "SaaS finance expert; former VP Finance at Snowflake during hypergrowth phase.", "is_active": True, "data_sources": ["demo_seeder"]},
    # PayGrid Systems
    {"full_name": "Monica Alvarez", "first_name": "Monica", "last_name": "Alvarez", "current_title": "CEO", "current_company": "PayGrid Systems", "city": "Miami", "state": "FL", "country": "USA", "bio": "Payments industry veteran; former SVP at FIS Global, led B2B payments division.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Derek Simmons", "first_name": "Derek", "last_name": "Simmons", "current_title": "CFO", "current_company": "PayGrid Systems", "city": "Miami", "state": "FL", "country": "USA", "bio": "CFA; former Director of Finance at Square, expertise in payment unit economics.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Raj Patel", "first_name": "Raj", "last_name": "Patel", "current_title": "CTO", "current_company": "PayGrid Systems", "city": "Miami", "state": "FL", "country": "USA", "bio": "Built real-time payment processing engine handling 50M+ transactions/month.", "is_active": True, "data_sources": ["demo_seeder"]},
    # VeriComply AI
    {"full_name": "Sarah Goldstein", "first_name": "Sarah", "last_name": "Goldstein", "current_title": "CEO & Co-Founder", "current_company": "VeriComply AI", "city": "San Francisco", "state": "CA", "country": "USA", "bio": "Former compliance officer at JPMorgan; built AI-first regulatory compliance platform.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Brian Liu", "first_name": "Brian", "last_name": "Liu", "current_title": "CFO", "current_company": "VeriComply AI", "city": "San Francisco", "state": "CA", "country": "USA", "bio": "Early-stage SaaS finance; managed VeriComply through Series A and B rounds.", "is_active": True, "data_sources": ["demo_seeder"]},
    # InsightFlow Analytics
    {"full_name": "Natalie Richards", "first_name": "Natalie", "last_name": "Richards", "current_title": "CEO", "current_company": "InsightFlow Analytics", "city": "Chicago", "state": "IL", "country": "USA", "bio": "Data analytics leader; former VP Product at Tableau, led enterprise analytics suite.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Gregory Tanaka", "first_name": "Gregory", "last_name": "Tanaka", "current_title": "CFO", "current_company": "InsightFlow Analytics", "city": "Chicago", "state": "IL", "country": "USA", "bio": "MBA Wharton; scaled finance operations at three analytics startups through exit.", "is_active": True, "data_sources": ["demo_seeder"]},
    # ShieldPay Fraud Detection
    {"full_name": "Alex Petrov", "first_name": "Alex", "last_name": "Petrov", "current_title": "CEO & Founder", "current_company": "ShieldPay Fraud Detection", "city": "New York", "state": "NY", "country": "USA", "bio": "Former ML lead at Stripe Radar; founded ShieldPay to fight real-time payment fraud.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Wendy Chung", "first_name": "Wendy", "last_name": "Chung", "current_title": "CFO", "current_company": "ShieldPay Fraud Detection", "city": "New York", "state": "NY", "country": "USA", "bio": "Fintech finance; built financial controls and reporting at Affirm pre-IPO.", "is_active": True, "data_sources": ["demo_seeder"]},
    # CloudMetrics Pro (exited)
    {"full_name": "Tyler Brennan", "first_name": "Tyler", "last_name": "Brennan", "current_title": "CEO & Founder", "current_company": "CloudMetrics Pro", "city": "Portland", "state": "OR", "country": "USA", "bio": "DevOps pioneer; founded CloudMetrics to solve cloud cost visibility challenges.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Amy Nakagawa", "first_name": "Amy", "last_name": "Nakagawa", "current_title": "CFO", "current_company": "CloudMetrics Pro", "city": "Portland", "state": "OR", "country": "USA", "bio": "SaaS metrics expert; guided CloudMetrics through Datadog acquisition process.", "is_active": True, "data_sources": ["demo_seeder"]},
    # TrueVault Data (written off)
    {"full_name": "Oliver Reese", "first_name": "Oliver", "last_name": "Reese", "current_title": "CEO & Founder", "current_company": "TrueVault Data", "city": "Denver", "state": "CO", "country": "USA", "bio": "Privacy tech entrepreneur; pivoted TrueVault through multiple market strategies.", "is_active": False, "data_sources": ["demo_seeder"]},
    {"full_name": "Hannah Spiegel", "first_name": "Hannah", "last_name": "Spiegel", "current_title": "CFO", "current_company": "TrueVault Data", "city": "Denver", "state": "CO", "country": "USA", "bio": "Managed wind-down financials and creditor negotiations for TrueVault.", "is_active": False, "data_sources": ["demo_seeder"]},

    # Ironforge Industrial — Manufacturing + A&D
    # Titan Precision Manufacturing
    {"full_name": "Gary Stevenson", "first_name": "Gary", "last_name": "Stevenson", "current_title": "CEO", "current_company": "Titan Precision Manufacturing", "city": "Cincinnati", "state": "OH", "country": "USA", "bio": "30-year manufacturing veteran; former President at Precision Castparts aerospace division.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Donna Kessler", "first_name": "Donna", "last_name": "Kessler", "current_title": "CFO", "current_company": "Titan Precision Manufacturing", "city": "Cincinnati", "state": "OH", "country": "USA", "bio": "CPA; former Controller at Parker Hannifin, expert in manufacturing cost accounting.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "James Nakamura", "first_name": "James", "last_name": "Nakamura", "current_title": "COO", "current_company": "Titan Precision Manufacturing", "city": "Cincinnati", "state": "OH", "country": "USA", "bio": "Lean manufacturing expert; led operational transformation at Spirit AeroSystems.", "is_active": True, "data_sources": ["demo_seeder"]},
    # AeroSpec Coatings
    {"full_name": "Thomas Grant", "first_name": "Thomas", "last_name": "Grant", "current_title": "CEO", "current_company": "AeroSpec Coatings", "city": "Wichita", "state": "KS", "country": "USA", "bio": "Materials scientist turned exec; holds 12 patents in thermal barrier coatings.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Karen Wojcik", "first_name": "Karen", "last_name": "Wojcik", "current_title": "CFO", "current_company": "AeroSpec Coatings", "city": "Wichita", "state": "KS", "country": "USA", "bio": "Former VP Finance at Sherwin-Williams industrial division; aerospace P&L experience.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Continental Packaging Solutions
    {"full_name": "Raymond Cross", "first_name": "Raymond", "last_name": "Cross", "current_title": "CEO", "current_company": "Continental Packaging Solutions", "city": "Milwaukee", "state": "WI", "country": "USA", "bio": "Packaging industry leader; grew Continental from regional to national footprint via 5 add-ons.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Sandra Nilsson", "first_name": "Sandra", "last_name": "Nilsson", "current_title": "CFO", "current_company": "Continental Packaging Solutions", "city": "Milwaukee", "state": "WI", "country": "USA", "bio": "M&A integration specialist; managed financial integration of 5 bolt-on acquisitions.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Victor Huang", "first_name": "Victor", "last_name": "Huang", "current_title": "COO", "current_company": "Continental Packaging Solutions", "city": "Milwaukee", "state": "WI", "country": "USA", "bio": "Supply chain expert; optimized Continental's 12-plant network for 15% cost reduction.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Midwest Valve & Controls
    {"full_name": "Robert Magnusson", "first_name": "Robert", "last_name": "Magnusson", "current_title": "CEO", "current_company": "Midwest Valve & Controls", "city": "Houston", "state": "TX", "country": "USA", "bio": "Flow control industry veteran; former EVP at Emerson's valve division.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Christine Blake", "first_name": "Christine", "last_name": "Blake", "current_title": "CFO", "current_company": "Midwest Valve & Controls", "city": "Houston", "state": "TX", "country": "USA", "bio": "Energy sector CFO; managed through oil price cycles at Cameron International.", "is_active": True, "data_sources": ["demo_seeder"]},
    # SteelCore Fabrication
    {"full_name": "Anthony Russo", "first_name": "Anthony", "last_name": "Russo", "current_title": "CEO", "current_company": "SteelCore Fabrication", "city": "Pittsburgh", "state": "PA", "country": "USA", "bio": "Third-generation steelworker turned exec; modernized SteelCore with automated welding.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Michelle Chen-Park", "first_name": "Michelle", "last_name": "Chen-Park", "current_title": "CFO", "current_company": "SteelCore Fabrication", "city": "Pittsburgh", "state": "PA", "country": "USA", "bio": "Industrial finance; built project-based costing system that improved margins 3pts.", "is_active": True, "data_sources": ["demo_seeder"]},
    # DefenseTech Systems
    {"full_name": "Col. (Ret.) Mark Henderson", "first_name": "Mark", "last_name": "Henderson", "current_title": "CEO", "current_company": "DefenseTech Systems", "city": "Huntsville", "state": "AL", "country": "USA", "bio": "Retired Army Colonel; 20 years in DoD acquisition, former PM for EW programs.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Laura Castellano", "first_name": "Laura", "last_name": "Castellano", "current_title": "CFO", "current_company": "DefenseTech Systems", "city": "Huntsville", "state": "AL", "country": "USA", "bio": "Defense CFO; FAR/DFAR compliance expert, former Controller at L3Harris division.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Dr. Wei Zhang", "first_name": "Wei", "last_name": "Zhang", "current_title": "CTO", "current_company": "DefenseTech Systems", "city": "Huntsville", "state": "AL", "country": "USA", "bio": "PhD EE Georgia Tech; holds Top Secret clearance, led DARPA-funded EW programs.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Great Lakes Plastics (exited)
    {"full_name": "Paul Henriksen", "first_name": "Paul", "last_name": "Henriksen", "current_title": "CEO", "current_company": "Great Lakes Plastics", "city": "Grand Rapids", "state": "MI", "country": "USA", "bio": "Plastics manufacturing exec; led Great Lakes through Berry Global acquisition.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Tamara Novak", "first_name": "Tamara", "last_name": "Novak", "current_title": "CFO", "current_company": "Great Lakes Plastics", "city": "Grand Rapids", "state": "MI", "country": "USA", "bio": "Manufacturing finance; managed sell-side due diligence and transition.", "is_active": True, "data_sources": ["demo_seeder"]},
    # Northwest Automation Group (exited)
    {"full_name": "Eric Johansson", "first_name": "Eric", "last_name": "Johansson", "current_title": "CEO", "current_company": "Northwest Automation Group", "city": "Minneapolis", "state": "MN", "country": "USA", "bio": "Automation engineer turned CEO; grew Northwest from 40 to 170 employees.", "is_active": True, "data_sources": ["demo_seeder"]},
    {"full_name": "Rebecca Foley", "first_name": "Rebecca", "last_name": "Foley", "current_title": "CFO", "current_company": "Northwest Automation Group", "city": "Minneapolis", "state": "MN", "country": "USA", "bio": "Industrial services finance; structured Rockwell Automation sale for 12.1x EBITDA.", "is_active": True, "data_sources": ["demo_seeder"]},
]


# Map company → executive leadership records
# Tuple: (person_name, title, role_category, is_ceo, is_cfo, is_board, appointed_by_pe, pe_affiliation)
COMPANY_LEADERSHIP_MAP = {
    "MedVantage Health Systems": [
        ("Dr. Robert Chen", "Chief Executive Officer", "C-Suite", True, False, False, True, "Summit Ridge Partners"),
        ("Patricia Flores", "Chief Financial Officer", "C-Suite", False, True, False, True, "Summit Ridge Partners"),
        ("Dr. Alan Gupta", "Chief Medical Officer", "C-Suite", False, False, False, False, None),
    ],
    "Apex Revenue Solutions": [
        ("Kevin McCarthy", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Linda Tran", "Chief Financial Officer", "C-Suite", False, True, False, True, "Summit Ridge Partners"),
    ],
    "CloudShield Security": [
        ("Jason Park", "Chief Executive Officer & Co-Founder", "C-Suite", True, False, False, False, None),
        ("Rachel Adams", "Chief Financial Officer", "C-Suite", False, True, False, True, "Summit Ridge Partners"),
        ("Dmitri Volkov", "Chief Technology Officer", "C-Suite", False, False, False, False, None),
    ],
    "TrueNorth Behavioral": [
        ("Dr. Margaret Sullivan", "Chief Executive Officer", "C-Suite", True, False, False, True, "Summit Ridge Partners"),
        ("Steven Greenfield", "Chief Financial Officer", "C-Suite", False, True, False, True, "Summit Ridge Partners"),
        ("Dr. Anita Rao", "Chief Clinical Officer", "C-Suite", False, False, False, False, None),
    ],
    "Precision Lab Diagnostics": [
        ("Frank DeLuca", "Chief Executive Officer", "C-Suite", True, False, False, True, "Summit Ridge Partners"),
        ("Janet Okonkwo", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "Elevate Staffing Group": [
        ("Marcus Whitfield", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Diane Hoffman", "Chief Financial Officer", "C-Suite", False, True, False, True, "Summit Ridge Partners"),
    ],
    "DataBridge Analytics": [
        ("Yusuf Ibrahim", "Chief Executive Officer & Founder", "C-Suite", True, False, False, False, None),
        ("Samantha Lee", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "NovaCare Urgent Clinics": [
        ("Dr. James Morton", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Carolyn Briggs", "Chief Financial Officer", "C-Suite", False, True, False, True, "Summit Ridge Partners"),
    ],
    "FinLedger Technologies": [
        ("Nathan Cho", "Chief Executive Officer & Founder", "C-Suite", True, False, False, False, None),
        ("Elizabeth Warren-Hughes", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "Nimbus Data Cloud": [
        ("Arjun Mehta", "Chief Executive Officer & Co-Founder", "C-Suite", True, False, False, False, None),
        ("Claire Donovan", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "PayGrid Systems": [
        ("Monica Alvarez", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Derek Simmons", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
        ("Raj Patel", "Chief Technology Officer", "C-Suite", False, False, False, False, None),
    ],
    "VeriComply AI": [
        ("Sarah Goldstein", "Chief Executive Officer & Co-Founder", "C-Suite", True, False, False, False, None),
        ("Brian Liu", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "InsightFlow Analytics": [
        ("Natalie Richards", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Gregory Tanaka", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "ShieldPay Fraud Detection": [
        ("Alex Petrov", "Chief Executive Officer & Founder", "C-Suite", True, False, False, False, None),
        ("Wendy Chung", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "CloudMetrics Pro": [
        ("Tyler Brennan", "Chief Executive Officer & Founder", "C-Suite", True, False, False, False, None),
        ("Amy Nakagawa", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "TrueVault Data": [
        ("Oliver Reese", "Chief Executive Officer & Founder", "C-Suite", True, False, False, False, None),
        ("Hannah Spiegel", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "Titan Precision Manufacturing": [
        ("Gary Stevenson", "Chief Executive Officer", "C-Suite", True, False, False, True, "Ironforge Industrial Capital"),
        ("Donna Kessler", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
        ("James Nakamura", "Chief Operating Officer", "C-Suite", False, False, False, True, "Ironforge Industrial Capital"),
    ],
    "AeroSpec Coatings": [
        ("Thomas Grant", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Karen Wojcik", "Chief Financial Officer", "C-Suite", False, True, False, True, "Ironforge Industrial Capital"),
    ],
    "Continental Packaging Solutions": [
        ("Raymond Cross", "Chief Executive Officer", "C-Suite", True, False, False, True, "Ironforge Industrial Capital"),
        ("Sandra Nilsson", "Chief Financial Officer", "C-Suite", False, True, False, True, "Ironforge Industrial Capital"),
        ("Victor Huang", "Chief Operating Officer", "C-Suite", False, False, False, False, None),
    ],
    "Midwest Valve & Controls": [
        ("Robert Magnusson", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Christine Blake", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "SteelCore Fabrication": [
        ("Anthony Russo", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Michelle Chen-Park", "Chief Financial Officer", "C-Suite", False, True, False, True, "Ironforge Industrial Capital"),
    ],
    "DefenseTech Systems": [
        ("Col. (Ret.) Mark Henderson", "Chief Executive Officer", "C-Suite", True, False, False, True, "Ironforge Industrial Capital"),
        ("Laura Castellano", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
        ("Dr. Wei Zhang", "Chief Technology Officer", "C-Suite", False, False, False, False, None),
    ],
    "Great Lakes Plastics": [
        ("Paul Henriksen", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Tamara Novak", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
    "Northwest Automation Group": [
        ("Eric Johansson", "Chief Executive Officer", "C-Suite", True, False, False, False, None),
        ("Rebecca Foley", "Chief Financial Officer", "C-Suite", False, True, False, False, None),
    ],
}


# Board seats: PE partners sitting on portfolio company boards
# Tuple: (person_name, company_name, title, is_board_chair)
BOARD_SEATS = [
    # Summit Ridge partners on their portfolio boards
    ("James Harrington", "MedVantage Health Systems", "Board Chair", True),
    ("Sarah Chen", "MedVantage Health Systems", "Board Member", False),
    ("Sarah Chen", "TrueNorth Behavioral", "Board Chair", True),
    ("Sarah Chen", "NovaCare Urgent Clinics", "Board Member", False),
    ("Michael Torres", "CloudShield Security", "Board Chair", True),
    ("Michael Torres", "DataBridge Analytics", "Board Member", False),
    ("James Harrington", "Apex Revenue Solutions", "Board Member", False),
    ("Amanda Brooks", "Precision Lab Diagnostics", "Board Member", False),
    ("Robert Kim", "TrueNorth Behavioral", "Board Member", False),
    ("Robert Kim", "Elevate Staffing Group", "Board Chair", True),
    ("David Patel", "Elevate Staffing Group", "Board Member", False),
    # Cascade Growth partners on boards
    ("Elena Vasquez", "FinLedger Technologies", "Board Member", False),
    ("Elena Vasquez", "PayGrid Systems", "Board Chair", True),
    ("Ryan Mitchell", "Nimbus Data Cloud", "Board Member", False),
    ("Ryan Mitchell", "VeriComply AI", "Board Member", False),
    ("Lisa Nakamura", "InsightFlow Analytics", "Board Chair", True),
    ("Lisa Nakamura", "ShieldPay Fraud Detection", "Board Member", False),
    ("Marcus Thompson", "PayGrid Systems", "Board Member", False),
    ("Andrew Foster", "CloudMetrics Pro", "Board Member", False),
    # Ironforge partners on boards
    ("William Blackwell", "Titan Precision Manufacturing", "Board Chair", True),
    ("William Blackwell", "Continental Packaging Solutions", "Board Chair", True),
    ("Katherine Dawson", "AeroSpec Coatings", "Board Chair", True),
    ("Katherine Dawson", "Midwest Valve & Controls", "Board Member", False),
    ("Thomas O'Brien", "DefenseTech Systems", "Board Chair", True),
    ("Thomas O'Brien", "Titan Precision Manufacturing", "Board Member", False),
    ("Maria Rodriguez", "SteelCore Fabrication", "Board Member", False),
    ("Maria Rodriguez", "Continental Packaging Solutions", "Board Member", False),
    ("Richard Hoffman", "Great Lakes Plastics", "Board Chair", True),
    ("Richard Hoffman", "Northwest Automation Group", "Board Member", False),
    ("Daniel Kowalski", "Midwest Valve & Controls", "Board Member", False),
]


# ---------------------------------------------------------------------------
# Competitor Mappings (2-3 per portfolio company)
# ---------------------------------------------------------------------------

# Dict: company_name → list of (competitor_name, type, relative_size, market_position, is_public, ticker, is_pe_backed, pe_owner, notes)
COMPETITOR_MAPPINGS = {
    # Summit Ridge — Healthcare
    "MedVantage Health Systems": [
        ("Amedisys", "Direct", "Larger", "Leader", True, "AMED", False, None, "National home health and hospice services"),
        ("Addus HomeCare", "Direct", "Similar", "Challenger", True, "ADUS", False, None, "Personal care and home health"),
        ("BrightSpring Health", "Direct", "Larger", "Leader", False, None, True, "KKR", "Diversified health services platform"),
    ],
    "TrueNorth Behavioral": [
        ("Acadia Healthcare", "Direct", "Larger", "Leader", True, "ACHC", False, None, "Behavioral health facilities operator"),
        ("Universal Health Services", "Direct", "Larger", "Leader", True, "UHS", False, None, "Acute and behavioral health"),
        ("Refresh Mental Health", "Direct", "Similar", "Challenger", False, None, True, "Lee Equity Partners", "Outpatient behavioral health"),
    ],
    "NovaCare Urgent Clinics": [
        ("CityMD", "Direct", "Larger", "Leader", False, None, True, "Warburg Pincus", "Walk-in urgent care clinics"),
        ("GoHealth Urgent Care", "Direct", "Similar", "Challenger", False, None, True, "TPG", "Joint-venture urgent care"),
    ],
    "Precision Lab Diagnostics": [
        ("Quest Diagnostics", "Direct", "Larger", "Leader", True, "DGX", False, None, "National clinical laboratory"),
        ("Sonic Healthcare", "Direct", "Larger", "Leader", True, "SHL.AX", False, None, "Global pathology and diagnostics"),
        ("BioReference Labs", "Direct", "Similar", "Challenger", False, None, True, "OPKO Health", "Regional specialty lab"),
    ],
    # Summit Ridge — Software/Services
    "CloudShield Security": [
        ("CrowdStrike", "Direct", "Larger", "Leader", True, "CRWD", False, None, "Cloud-native endpoint security"),
        ("SentinelOne", "Direct", "Larger", "Challenger", True, "S", False, None, "AI-powered security platform"),
        ("Arctic Wolf", "Indirect", "Similar", "Challenger", False, None, True, "Viking Global", "Security operations as a service"),
    ],
    "Apex Revenue Solutions": [
        ("R1 RCM", "Direct", "Larger", "Leader", True, "RCM", False, None, "Revenue cycle management"),
        ("Waystar", "Direct", "Similar", "Challenger", False, None, True, "EQT/CPPIB", "Healthcare payment solutions"),
    ],
    "DataBridge Analytics": [
        ("Health Catalyst", "Direct", "Similar", "Challenger", True, "HCAT", False, None, "Healthcare data platform"),
        ("Innovaccer", "Indirect", "Similar", "Niche", False, None, True, "Tiger Global", "Healthcare data unification"),
    ],
    "Elevate Staffing Group": [
        ("AMN Healthcare", "Direct", "Larger", "Leader", True, "AMN", False, None, "Healthcare staffing and workforce"),
        ("Cross Country Healthcare", "Direct", "Similar", "Challenger", True, "CCRN", False, None, "Healthcare staffing"),
    ],
    # Cascade Growth — Fintech/SaaS
    "FinLedger Technologies": [
        ("nCino", "Direct", "Larger", "Leader", True, "NCNO", False, None, "Cloud banking platform"),
        ("Blend Labs", "Direct", "Similar", "Challenger", True, "BLND", False, None, "Digital lending platform"),
    ],
    "PayGrid Systems": [
        ("Marqeta", "Direct", "Similar", "Challenger", True, "MQ", False, None, "Card issuing and payment infrastructure"),
        ("Adyen", "Direct", "Larger", "Leader", True, "ADYEN.AS", False, None, "Payment platform"),
        ("Stripe", "Direct", "Larger", "Leader", False, None, True, "Sequoia/a16z", "Payment infrastructure"),
    ],
    "Nimbus Data Cloud": [
        ("Snowflake", "Direct", "Larger", "Leader", True, "SNOW", False, None, "Cloud data warehouse"),
        ("Databricks", "Direct", "Larger", "Leader", False, None, True, "a16z", "Unified data analytics platform"),
    ],
    "InsightFlow Analytics": [
        ("Domo", "Direct", "Similar", "Challenger", True, "DOMO", False, None, "Cloud BI platform"),
        ("Sisense", "Direct", "Similar", "Challenger", False, None, True, "Insight Partners", "Embedded analytics"),
    ],
    "ShieldPay Fraud Detection": [
        ("Featurespace", "Direct", "Similar", "Challenger", False, None, True, "Merian Chrysalis", "Adaptive fraud detection"),
        ("Feedzai", "Direct", "Similar", "Challenger", False, None, True, "KKR", "AI fraud prevention"),
        ("NICE Actimize", "Direct", "Larger", "Leader", False, None, False, None, "Financial crime detection (div of NICE)"),
    ],
    "VeriComply AI": [
        ("ComplyAdvantage", "Direct", "Similar", "Challenger", False, None, True, "Goldman Sachs", "AI-powered compliance"),
        ("Alloy", "Direct", "Similar", "Challenger", False, None, True, "Lightspeed", "Identity risk decisioning"),
    ],
    "CloudMetrics Pro": [
        ("Datadog", "Direct", "Larger", "Leader", True, "DDOG", False, None, "Cloud monitoring and analytics"),
        ("New Relic", "Direct", "Larger", "Challenger", True, "NEWR", False, None, "Observability platform"),
    ],
    "TrueVault Data": [
        ("OneTrust", "Direct", "Larger", "Leader", False, None, True, "Insight Partners", "Privacy management platform"),
        ("BigID", "Direct", "Similar", "Challenger", False, None, True, "Bessemer", "Data intelligence and privacy"),
    ],
    # Ironforge — Industrial
    "Titan Precision Manufacturing": [
        ("Precision Castparts", "Direct", "Larger", "Leader", False, None, False, None, "Berkshire Hathaway subsidiary — precision components"),
        ("Triumph Group", "Direct", "Similar", "Challenger", True, "TGI", False, None, "Aerospace structures and systems"),
        ("TransDigm Group", "Direct", "Larger", "Leader", True, "TDG", False, None, "Proprietary aerospace components"),
    ],
    "AeroSpec Coatings": [
        ("PPG Industries", "Direct", "Larger", "Leader", True, "PPG", False, None, "Aerospace coatings division"),
        ("AkzoNobel", "Direct", "Larger", "Leader", True, "AKZA.AS", False, None, "Performance coatings"),
    ],
    "Continental Packaging Solutions": [
        ("Sealed Air", "Direct", "Larger", "Leader", True, "SEE", False, None, "Packaging solutions"),
        ("Sonoco Products", "Direct", "Larger", "Challenger", True, "SON", False, None, "Industrial packaging"),
        ("ProMach", "Direct", "Similar", "Challenger", False, None, True, "ProMach Holdings", "Packaging machinery"),
    ],
    "Midwest Valve & Controls": [
        ("Emerson Electric", "Direct", "Larger", "Leader", True, "EMR", False, None, "Flow control division (Fisher, Bettis)"),
        ("Flowserve", "Direct", "Larger", "Challenger", True, "FLS", False, None, "Industrial valve manufacturer"),
    ],
    "SteelCore Fabrication": [
        ("Nucor Corporation", "Direct", "Larger", "Leader", True, "NUE", False, None, "Steel fabrication & products"),
        ("Commercial Metals", "Direct", "Larger", "Challenger", True, "CMC", False, None, "Steel fabrication & recycling"),
    ],
    "DefenseTech Systems": [
        ("L3Harris Technologies", "Direct", "Larger", "Leader", True, "LHX", False, None, "Defense technology and comms"),
        ("Mercury Systems", "Direct", "Similar", "Challenger", True, "MRCY", False, None, "Defense electronics processing"),
        ("CACI International", "Direct", "Similar", "Challenger", True, "CACI", False, None, "Defense technology & services"),
    ],
    "Great Lakes Plastics": [
        ("Berry Global", "Direct", "Larger", "Leader", True, "BERY", False, None, "Plastic packaging solutions"),
        ("AptarGroup", "Direct", "Larger", "Challenger", True, "ATR", False, None, "Dispensing solutions"),
    ],
    "Northwest Automation Group": [
        ("Rockwell Automation", "Direct", "Larger", "Leader", True, "ROK", False, None, "Industrial automation"),
        ("Emerson Electric", "Direct", "Larger", "Leader", True, "EMR", False, None, "Automation solutions division"),
    ],
}


# ---------------------------------------------------------------------------
# Company News (2-3 items per company, mix of sentiment)
# ---------------------------------------------------------------------------

# Dict: company_name → list of (title, source, url_slug, summary, news_type, sentiment, score, published_date)
COMPANY_NEWS = {
    "MedVantage Health Systems": [
        ("MedVantage Expands to 12 New Markets Across Southeast", "Modern Healthcare", "medvantage-expansion-southeast", "MedVantage Health Systems announced expansion into 12 new metropolitan markets across the Southeast, bringing its total footprint to 85 markets. The expansion is backed by a $45M growth capital investment from Summit Ridge Partners.", "Expansion", "Positive", 0.82, datetime(2025, 8, 15)),
        ("Home Health Industry Faces Medicare Reimbursement Headwinds", "Healthcare Dive", "home-health-medicare-headwinds", "CMS proposed a 1.7% cut to home health reimbursement rates for 2026, affecting providers including MedVantage, Amedisys, and Addus HomeCare. Industry groups are lobbying for reversal.", "Regulatory", "Negative", -0.45, datetime(2025, 7, 20)),
        ("MedVantage CEO Named to Modern Healthcare's Top 25 Innovators", "Modern Healthcare", "medvantage-ceo-top25-innovators", "Dr. Sarah Mitchell, CEO of MedVantage Health Systems, was recognized for pioneering hybrid telehealth-home health delivery models.", "Management", "Positive", 0.65, datetime(2025, 9, 1)),
    ],
    "TrueNorth Behavioral": [
        ("TrueNorth Behavioral Opens 5 New Outpatient Centers", "Behavioral Health Business", "truenorth-new-outpatient-centers", "TrueNorth Behavioral expanded its outpatient network with 5 new centers across Texas and Florida, targeting underserved communities.", "Expansion", "Positive", 0.75, datetime(2025, 6, 10)),
        ("Behavioral Health Demand Surges Post-Pandemic, Creating M&A Opportunities", "PE Hub", "behavioral-health-ma-surge", "Private equity firms are increasingly targeting behavioral health platforms, with TrueNorth Behavioral among the most active acquirers.", "Deal", "Positive", 0.60, datetime(2025, 5, 22)),
    ],
    "CloudShield Security": [
        ("CloudShield Security Achieves FedRAMP High Authorization", "CyberScoop", "cloudshield-fedramp-high", "CloudShield Security received FedRAMP High authorization, opening the door to federal contracts worth an estimated $200M TAM.", "Product", "Positive", 0.88, datetime(2025, 9, 5)),
        ("Cybersecurity Funding Cools as Valuations Reset", "TechCrunch", "cybersec-funding-cools-2025", "Cybersecurity startups face valuation compression as investors demand profitability over growth. Affects late-stage companies including CloudShield and Arctic Wolf.", "Market", "Negative", -0.30, datetime(2025, 7, 15)),
    ],
    "Precision Lab Diagnostics": [
        ("Precision Lab Diagnostics Launches At-Home Testing Platform", "MedTech Dive", "precision-lab-home-testing", "Precision Lab introduced a direct-to-consumer at-home testing platform for common lab panels, targeting the $5B home diagnostics market.", "Product", "Positive", 0.72, datetime(2025, 8, 20)),
        ("Lab Industry Consolidation Accelerates with 12 Deals in Q2", "Dark Daily", "lab-consolidation-q2-2025", "The clinical laboratory industry saw 12 M&A transactions in Q2 2025, with Precision Lab and BioReference among rumored targets.", "Deal", "Neutral", 0.10, datetime(2025, 7, 8)),
    ],
    "FinLedger Technologies": [
        ("FinLedger Partners with Top 20 Regional Bank for Digital Transformation", "American Banker", "finledger-regional-bank-partnership", "FinLedger Technologies signed a 5-year partnership with a Top 20 regional bank to digitize commercial lending, representing $8M ARR.", "Deal", "Positive", 0.78, datetime(2025, 9, 12)),
        ("Community Banks Accelerate Tech Adoption to Compete with Neobanks", "Banking Dive", "community-banks-tech-adoption", "Survey shows 78% of community banks plan to increase technology spending, benefiting vendors like FinLedger and nCino.", "Market", "Positive", 0.55, datetime(2025, 6, 28)),
    ],
    "PayGrid Systems": [
        ("PayGrid Systems Processes $10B in Annual Volume", "PYMNTS", "paygrid-10b-annual-volume", "PayGrid Systems crossed the $10B annual payment processing milestone, up 45% YoY, driven by B2B embedded payments growth.", "Earnings", "Positive", 0.85, datetime(2025, 8, 1)),
        ("Payment Fraud Losses Hit $32B Globally, Pressuring Processors", "Payments Journal", "payment-fraud-losses-32b", "Rising fraud rates are forcing payment processors including PayGrid and Marqeta to invest heavily in fraud prevention technology.", "Market", "Negative", -0.25, datetime(2025, 7, 5)),
        ("PayGrid Named to Forbes Fintech 50", "Forbes", "paygrid-forbes-fintech-50", "PayGrid Systems earned a spot on the 2025 Forbes Fintech 50 list for its B2B payment infrastructure innovation.", "Management", "Positive", 0.70, datetime(2025, 6, 15)),
    ],
    "Titan Precision Manufacturing": [
        ("Titan Precision Wins $120M Multi-Year Defense Contract", "Defense News", "titan-defense-contract-120m", "Titan Precision Manufacturing secured a $120M contract to supply precision-machined components for next-generation fighter aircraft.", "Deal", "Positive", 0.90, datetime(2025, 9, 8)),
        ("Titan Precision Exploring Bolt-On Acquisition of Southwest Machine Works", "PE Hub", "titan-southwest-bolt-on", "Ironforge Industrial Capital-backed Titan Precision is in advanced talks to acquire Southwest Machine Works for approximately $45M.", "Deal", "Positive", 0.65, datetime(2025, 8, 28)),
        ("Aerospace Supply Chain Faces Skilled Labor Shortage", "Aviation Week", "aerospace-labor-shortage-2025", "Aerospace manufacturers including Titan Precision and Triumph Group report 15-20% unfilled machinist positions.", "Market", "Negative", -0.35, datetime(2025, 7, 12)),
    ],
    "Continental Packaging Solutions": [
        ("Continental Packaging Launches Sustainable Product Line", "Packaging World", "continental-sustainable-packaging", "Continental Packaging Solutions introduced a fully recyclable packaging line that reduces material costs by 18% while meeting ESG targets.", "Product", "Positive", 0.73, datetime(2025, 8, 5)),
        ("Raw Material Costs Stabilize After Two-Year Surge", "Packaging Digest", "raw-material-costs-stabilize", "Resin and corrugated board prices have stabilized, improving margins for converters like Continental Packaging and Sonoco.", "Market", "Positive", 0.40, datetime(2025, 6, 20)),
    ],
    "DefenseTech Systems": [
        ("DefenseTech Systems Selected for JADC2 Pilot Program", "C4ISRNet", "defensetech-jadc2-pilot", "DefenseTech Systems was selected as a technology partner for the DoD's JADC2 initiative, focused on secure tactical communications.", "Deal", "Positive", 0.88, datetime(2025, 9, 15)),
        ("Defense Budget Request Includes 7% Increase for Technology Modernization", "Defense One", "defense-budget-tech-increase", "The FY2026 defense budget proposal includes a 7% increase in technology modernization funding, benefiting contractors like DefenseTech.", "Regulatory", "Positive", 0.60, datetime(2025, 5, 30)),
    ],
}


# ---------------------------------------------------------------------------
# Deals
# ---------------------------------------------------------------------------

DEALS = [
    # Summit Ridge exits
    {"company_name": "NovaCare Urgent Clinics", "deal_name": "NovaCare Sale to National Health Corp", "deal_type": "Exit", "deal_sub_type": "Strategic Sale", "announced_date": date(2024, 3, 15), "closed_date": date(2024, 6, 1), "enterprise_value_usd": Decimal("480000000"), "ev_ebitda_multiple": Decimal("14.5"), "ltm_revenue_usd": Decimal("165000000"), "ltm_ebitda_usd": Decimal("33000000"), "buyer_name": "National Health Corp", "seller_name": "Summit Ridge Partners", "seller_type": "PE", "status": "Closed", "data_source": "demo_seeder"},
    {"company_name": "DataBridge Analytics", "deal_name": "DataBridge Acquisition by Optum Insight", "deal_type": "Exit", "deal_sub_type": "Strategic Sale", "announced_date": date(2024, 8, 1), "closed_date": date(2024, 11, 15), "enterprise_value_usd": Decimal("220000000"), "ev_revenue_multiple": Decimal("5.8"), "ltm_revenue_usd": Decimal("38000000"), "ltm_ebitda_usd": Decimal("7600000"), "buyer_name": "Optum Insight", "seller_name": "Summit Ridge Partners", "seller_type": "PE", "status": "Closed", "data_source": "demo_seeder"},
    # Cascade exit
    {"company_name": "CloudMetrics Pro", "deal_name": "CloudMetrics Pro Acquisition by Datadog", "deal_type": "Exit", "deal_sub_type": "Strategic Sale", "announced_date": date(2025, 1, 10), "closed_date": date(2025, 4, 1), "enterprise_value_usd": Decimal("350000000"), "ev_revenue_multiple": Decimal("12.5"), "ltm_revenue_usd": Decimal("28000000"), "ltm_ebitda_usd": Decimal("5600000"), "buyer_name": "Datadog Inc.", "seller_name": "Cascade Growth Equity", "seller_type": "PE", "status": "Closed", "data_source": "demo_seeder"},
    # Ironforge exits
    {"company_name": "Great Lakes Plastics", "deal_name": "Great Lakes Sale to Berry Global", "deal_type": "Exit", "deal_sub_type": "Strategic Sale", "announced_date": date(2024, 5, 20), "closed_date": date(2024, 9, 1), "enterprise_value_usd": Decimal("195000000"), "ev_ebitda_multiple": Decimal("10.2"), "ltm_revenue_usd": Decimal("78000000"), "ltm_ebitda_usd": Decimal("19000000"), "buyer_name": "Berry Global", "seller_name": "Ironforge Industrial Capital", "seller_type": "PE", "status": "Closed", "data_source": "demo_seeder"},
    {"company_name": "Northwest Automation Group", "deal_name": "Northwest Automation Sale to Rockwell", "deal_type": "Exit", "deal_sub_type": "Strategic Sale", "announced_date": date(2024, 10, 5), "closed_date": date(2025, 1, 15), "enterprise_value_usd": Decimal("145000000"), "ev_revenue_multiple": Decimal("2.8"), "ev_ebitda_multiple": Decimal("12.1"), "ltm_revenue_usd": Decimal("52000000"), "ltm_ebitda_usd": Decimal("12000000"), "buyer_name": "Rockwell Automation", "seller_name": "Ironforge Industrial Capital", "seller_type": "PE", "status": "Closed", "data_source": "demo_seeder"},
    # Active pipeline
    {"company_name": "Titan Precision Manufacturing", "deal_name": "Titan Bolt-on: Southwest Machine Works", "deal_type": "Add-on", "deal_sub_type": "Bolt-on", "announced_date": date(2025, 9, 1), "enterprise_value_usd": Decimal("45000000"), "ev_ebitda_multiple": Decimal("7.5"), "ltm_revenue_usd": Decimal("22000000"), "ltm_ebitda_usd": Decimal("6000000"), "buyer_name": "Titan Precision Manufacturing", "seller_name": "Founder", "seller_type": "Founder", "status": "Pending", "data_source": "demo_seeder"},
]


# ---------------------------------------------------------------------------
# Investment records (fund → company)
# ---------------------------------------------------------------------------

INVESTMENTS = {
    "Summit Ridge Partners Fund III": [
        {"company_name": "MedVantage Health Systems", "investment_date": date(2019, 6, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("125000000"), "ownership_pct": Decimal("72"), "entry_ev_usd": Decimal("210000000"), "entry_ev_ebitda_multiple": Decimal("11.2"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "Apex Revenue Solutions", "investment_date": date(2019, 11, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("80000000"), "ownership_pct": Decimal("68"), "entry_ev_usd": Decimal("145000000"), "entry_ev_ebitda_multiple": Decimal("10.5"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "NovaCare Urgent Clinics", "investment_date": date(2020, 3, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("150000000"), "ownership_pct": Decimal("80"), "entry_ev_usd": Decimal("240000000"), "entry_ev_ebitda_multiple": Decimal("12.0"), "has_board_seat": True, "board_seats": 3, "has_control": True, "status": "Exited", "exit_date": date(2024, 6, 1), "exit_type": "Strategic Sale", "exit_amount_usd": Decimal("384000000"), "exit_multiple": Decimal("2.56"), "exit_irr_pct": Decimal("25.8")},
        {"company_name": "DataBridge Analytics", "investment_date": date(2020, 9, 15), "investment_type": "Growth", "invested_amount_usd": Decimal("30000000"), "ownership_pct": Decimal("45"), "entry_ev_usd": Decimal("55000000"), "entry_ev_revenue_multiple": Decimal("3.1"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Exited", "exit_date": date(2024, 11, 15), "exit_type": "Strategic Sale", "exit_amount_usd": Decimal("99000000"), "exit_multiple": Decimal("3.30"), "exit_irr_pct": Decimal("35.2")},
    ],
    "Summit Ridge Partners Fund IV": [
        {"company_name": "CloudShield Security", "investment_date": date(2023, 4, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("95000000"), "ownership_pct": Decimal("65"), "entry_ev_usd": Decimal("170000000"), "entry_ev_revenue_multiple": Decimal("4.2"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "TrueNorth Behavioral", "investment_date": date(2023, 7, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("200000000"), "ownership_pct": Decimal("75"), "entry_ev_usd": Decimal("350000000"), "entry_ev_ebitda_multiple": Decimal("13.5"), "has_board_seat": True, "board_seats": 3, "has_control": True, "status": "Active"},
        {"company_name": "Precision Lab Diagnostics", "investment_date": date(2023, 11, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("110000000"), "ownership_pct": Decimal("70"), "entry_ev_usd": Decimal("195000000"), "entry_ev_ebitda_multiple": Decimal("10.8"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "Elevate Staffing Group", "investment_date": date(2024, 2, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("55000000"), "ownership_pct": Decimal("60"), "entry_ev_usd": Decimal("90000000"), "entry_ev_revenue_multiple": Decimal("2.0"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
    ],
    "Cascade Growth Fund II": [
        {"company_name": "FinLedger Technologies", "investment_date": date(2020, 9, 1), "investment_type": "Growth", "invested_amount_usd": Decimal("45000000"), "ownership_pct": Decimal("28"), "entry_ev_usd": Decimal("120000000"), "entry_ev_revenue_multiple": Decimal("5.5"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Active"},
        {"company_name": "Nimbus Data Cloud", "investment_date": date(2021, 1, 15), "investment_type": "Growth", "invested_amount_usd": Decimal("35000000"), "ownership_pct": Decimal("25"), "entry_ev_usd": Decimal("95000000"), "entry_ev_revenue_multiple": Decimal("6.3"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Active"},
        {"company_name": "PayGrid Systems", "investment_date": date(2021, 6, 1), "investment_type": "Growth", "invested_amount_usd": Decimal("75000000"), "ownership_pct": Decimal("30"), "entry_ev_usd": Decimal("250000000"), "entry_ev_revenue_multiple": Decimal("4.5"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Active"},
        {"company_name": "CloudMetrics Pro", "investment_date": date(2021, 3, 1), "investment_type": "Growth", "invested_amount_usd": Decimal("20000000"), "ownership_pct": Decimal("22"), "entry_ev_usd": Decimal("65000000"), "entry_ev_revenue_multiple": Decimal("5.4"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Exited", "exit_date": date(2025, 4, 1), "exit_type": "Strategic Sale", "exit_amount_usd": Decimal("77000000"), "exit_multiple": Decimal("3.85"), "exit_irr_pct": Decimal("42.1")},
        {"company_name": "TrueVault Data", "investment_date": date(2021, 8, 1), "investment_type": "Growth", "invested_amount_usd": Decimal("15000000"), "ownership_pct": Decimal("20"), "entry_ev_usd": Decimal("40000000"), "entry_ev_revenue_multiple": Decimal("13.3"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Written-Off"},
    ],
    "Cascade Growth Fund III": [
        {"company_name": "VeriComply AI", "investment_date": date(2024, 4, 1), "investment_type": "Growth", "invested_amount_usd": Decimal("40000000"), "ownership_pct": Decimal("22"), "entry_ev_usd": Decimal("130000000"), "entry_ev_revenue_multiple": Decimal("7.2"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Active"},
        {"company_name": "InsightFlow Analytics", "investment_date": date(2024, 6, 15), "investment_type": "Growth", "invested_amount_usd": Decimal("55000000"), "ownership_pct": Decimal("25"), "entry_ev_usd": Decimal("180000000"), "entry_ev_revenue_multiple": Decimal("4.8"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Active"},
        {"company_name": "ShieldPay Fraud Detection", "investment_date": date(2024, 9, 1), "investment_type": "Growth", "invested_amount_usd": Decimal("25000000"), "ownership_pct": Decimal("18"), "entry_ev_usd": Decimal("85000000"), "entry_ev_revenue_multiple": Decimal("8.5"), "has_board_seat": True, "board_seats": 1, "has_control": False, "status": "Active"},
    ],
    "Ironforge Industrial Fund IV": [
        {"company_name": "Titan Precision Manufacturing", "investment_date": date(2018, 7, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("200000000"), "ownership_pct": Decimal("80"), "entry_ev_usd": Decimal("380000000"), "entry_ev_ebitda_multiple": Decimal("9.6"), "has_board_seat": True, "board_seats": 3, "has_control": True, "status": "Active"},
        {"company_name": "AeroSpec Coatings", "investment_date": date(2019, 2, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("120000000"), "ownership_pct": Decimal("75"), "entry_ev_usd": Decimal("210000000"), "entry_ev_ebitda_multiple": Decimal("10.8"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "Great Lakes Plastics", "investment_date": date(2019, 6, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("85000000"), "ownership_pct": Decimal("70"), "entry_ev_usd": Decimal("155000000"), "entry_ev_ebitda_multiple": Decimal("9.2"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Exited", "exit_date": date(2024, 9, 1), "exit_type": "Strategic Sale", "exit_amount_usd": Decimal("136500000"), "exit_multiple": Decimal("1.61"), "exit_irr_pct": Decimal("10.5")},
        {"company_name": "Northwest Automation Group", "investment_date": date(2020, 1, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("60000000"), "ownership_pct": Decimal("65"), "entry_ev_usd": Decimal("100000000"), "entry_ev_ebitda_multiple": Decimal("11.5"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Exited", "exit_date": date(2025, 1, 15), "exit_type": "Strategic Sale", "exit_amount_usd": Decimal("94250000"), "exit_multiple": Decimal("1.57"), "exit_irr_pct": Decimal("10.2")},
    ],
    "Ironforge Industrial Fund V": [
        {"company_name": "Continental Packaging Solutions", "investment_date": date(2023, 8, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("250000000"), "ownership_pct": Decimal("78"), "entry_ev_usd": Decimal("450000000"), "entry_ev_ebitda_multiple": Decimal("10.5"), "has_board_seat": True, "board_seats": 3, "has_control": True, "status": "Active"},
        {"company_name": "Midwest Valve & Controls", "investment_date": date(2023, 11, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("160000000"), "ownership_pct": Decimal("72"), "entry_ev_usd": Decimal("290000000"), "entry_ev_ebitda_multiple": Decimal("10.0"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "SteelCore Fabrication", "investment_date": date(2024, 3, 1), "investment_type": "Platform", "invested_amount_usd": Decimal("100000000"), "ownership_pct": Decimal("68"), "entry_ev_usd": Decimal("175000000"), "entry_ev_ebitda_multiple": Decimal("9.8"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
        {"company_name": "DefenseTech Systems", "investment_date": date(2024, 7, 15), "investment_type": "Platform", "invested_amount_usd": Decimal("90000000"), "ownership_pct": Decimal("65"), "entry_ev_usd": Decimal("160000000"), "entry_ev_ebitda_multiple": Decimal("12.3"), "has_board_seat": True, "board_seats": 2, "has_control": True, "status": "Active"},
    ],
}


# ---------------------------------------------------------------------------
# Independent Target Companies (acquisition targets for screener)
# ---------------------------------------------------------------------------

INDEPENDENT_TARGETS = [
    # Healthcare — physician practices / urgent care (NAICS 621111)
    {"name": "Pinnacle Primary Care", "industry": "Healthcare", "sub_industry": "Primary Care", "naics_code": "621111", "headquarters_city": "Nashville", "headquarters_state": "TN", "headquarters_country": "USA", "founded_year": 2008, "employee_count": 85, "ownership_status": "Private", "status": "Active", "description": "8-location primary care group across middle Tennessee with strong payer mix and 15% EBITDA margins."},
    {"name": "Coastal Family Medicine", "industry": "Healthcare", "sub_industry": "Family Medicine", "naics_code": "621111", "headquarters_city": "Charleston", "headquarters_state": "SC", "headquarters_country": "USA", "founded_year": 2005, "employee_count": 120, "ownership_status": "Private", "status": "Active", "description": "12-physician family medicine practice with integrated lab and imaging, dominant position in Charleston metro."},
    {"name": "Desert Sun Medical Group", "industry": "Healthcare", "sub_industry": "Multi-Specialty", "naics_code": "621111", "headquarters_city": "Scottsdale", "headquarters_state": "AZ", "headquarters_country": "USA", "founded_year": 2001, "employee_count": 200, "ownership_status": "Private", "status": "Active", "description": "Multi-specialty physician group with 22 providers, strong ancillary revenue from ASC and imaging center."},
    {"name": "Heartland Urgent Care", "industry": "Healthcare", "sub_industry": "Urgent Care", "naics_code": "621498", "headquarters_city": "Indianapolis", "headquarters_state": "IN", "headquarters_country": "USA", "founded_year": 2012, "employee_count": 65, "ownership_status": "Private", "status": "Active", "description": "5-location urgent care chain in Indianapolis suburbs with occupational health contracts and 20% visit growth YoY."},
    {"name": "Bayview Behavioral Health", "industry": "Healthcare", "sub_industry": "Behavioral Health", "naics_code": "621399", "headquarters_city": "Tampa", "headquarters_state": "FL", "headquarters_country": "USA", "founded_year": 2014, "employee_count": 95, "ownership_status": "Private", "status": "Active", "description": "Outpatient behavioral health practice with 18 therapists and psychiatrists, teletherapy-enabled, 30% revenue CAGR."},
    # Software / IT services (various NAICS)
    {"name": "Clearpath EHR Solutions", "industry": "Software", "sub_industry": "Healthcare IT", "naics_code": "541511", "headquarters_city": "Austin", "headquarters_state": "TX", "headquarters_country": "USA", "founded_year": 2015, "employee_count": 45, "ownership_status": "Private", "status": "Active", "description": "EHR implementation and support for small physician practices, 800+ active clients, 95% retention rate."},
    {"name": "RedLeaf Cybersecurity", "industry": "Software", "sub_industry": "Cybersecurity", "naics_code": "541512", "headquarters_city": "Raleigh", "headquarters_state": "NC", "headquarters_country": "USA", "founded_year": 2017, "employee_count": 60, "ownership_status": "Private", "status": "Active", "description": "Managed detection and response (MDR) for mid-market companies, SOC-as-a-service with proprietary threat intelligence."},
    {"name": "Brightline Analytics", "industry": "Software", "sub_industry": "Business Intelligence", "naics_code": "541511", "headquarters_city": "Denver", "headquarters_state": "CO", "headquarters_country": "USA", "founded_year": 2016, "employee_count": 35, "ownership_status": "Private", "status": "Active", "description": "Self-service BI platform for healthcare payers, embedded analytics for claims and utilization management."},
    {"name": "StackVault Cloud", "industry": "Software", "sub_industry": "Cloud Infrastructure", "naics_code": "518210", "headquarters_city": "Portland", "headquarters_state": "OR", "headquarters_country": "USA", "founded_year": 2018, "employee_count": 40, "ownership_status": "VC-Backed", "status": "Active", "description": "Kubernetes-native backup and disaster recovery for SMB cloud workloads, $4.2M ARR growing 45% YoY."},
    {"name": "ComplianceForge", "industry": "Software", "sub_industry": "RegTech", "naics_code": "541511", "headquarters_city": "Chicago", "headquarters_state": "IL", "headquarters_country": "USA", "founded_year": 2013, "employee_count": 55, "ownership_status": "Private", "status": "Active", "description": "Compliance workflow automation for community banks and credit unions, 200+ FI clients, capital-efficient growth."},
    # Industrial / manufacturing (various NAICS)
    {"name": "Lone Star Precision Parts", "industry": "Industrials", "sub_industry": "Precision Manufacturing", "naics_code": "332710", "headquarters_city": "Dallas", "headquarters_state": "TX", "headquarters_country": "USA", "founded_year": 2003, "employee_count": 140, "ownership_status": "Private", "status": "Active", "description": "CNC machining and wire EDM for aerospace and medical device OEMs, AS9100 and ISO 13485 certified."},
    {"name": "Summit HVAC Services", "industry": "Industrials", "sub_industry": "HVAC", "naics_code": "238220", "headquarters_city": "Salt Lake City", "headquarters_state": "UT", "headquarters_country": "USA", "founded_year": 2006, "employee_count": 110, "ownership_status": "Private", "status": "Active", "description": "Commercial HVAC installation and service across Utah and Idaho, recurring maintenance contracts drive 60% of revenue."},
    {"name": "Pacific Coast Packaging", "industry": "Industrials", "sub_industry": "Packaging", "naics_code": "322211", "headquarters_city": "Fresno", "headquarters_state": "CA", "headquarters_country": "USA", "founded_year": 1999, "employee_count": 180, "ownership_status": "Private", "status": "Active", "description": "Corrugated packaging for agricultural producers in Central Valley, vertically integrated with in-house printing."},
    {"name": "Cascade Fluid Systems", "industry": "Industrials", "sub_industry": "Flow Control", "naics_code": "332911", "headquarters_city": "Portland", "headquarters_state": "OR", "headquarters_country": "USA", "founded_year": 2007, "employee_count": 75, "ownership_status": "Private", "status": "Active", "description": "Industrial pump distribution and repair for water treatment and food processing, Pacific NW market leader."},
    {"name": "Appalachian Steel Works", "industry": "Industrials", "sub_industry": "Steel Fabrication", "naics_code": "332312", "headquarters_city": "Charleston", "headquarters_state": "WV", "headquarters_country": "USA", "founded_year": 1995, "employee_count": 220, "ownership_status": "Private", "status": "Active", "description": "Structural steel fabrication for commercial and infrastructure projects across the Mid-Atlantic region."},
    # Auto repair (NAICS 811111 — high fragmentation)
    {"name": "AutoCare Express", "industry": "Services", "sub_industry": "Auto Repair", "naics_code": "811111", "headquarters_city": "Phoenix", "headquarters_state": "AZ", "headquarters_country": "USA", "founded_year": 2010, "employee_count": 90, "ownership_status": "Private", "status": "Active", "description": "12-location auto repair chain in Phoenix metro with proprietary shop management software and OEM partnerships."},
    {"name": "Mountain West Auto Group", "industry": "Services", "sub_industry": "Auto Repair", "naics_code": "811111", "headquarters_city": "Boise", "headquarters_state": "ID", "headquarters_country": "USA", "founded_year": 2008, "employee_count": 55, "ownership_status": "Private", "status": "Active", "description": "6-location general auto repair network in Boise area, strong Google reviews, 25% revenue growth from fleet contracts."},
]

INDEPENDENT_TARGET_FINANCIALS = {
    # company_name: (base_revenue_millions, base_ebitda_margin, growth_rates, debt_ratio)
    "Pinnacle Primary Care": (12, 15.0, [8, 10, 12, 11, 9], 0.3),
    "Coastal Family Medicine": (22, 14.0, [6, 7, 8, 9, 10], 0.2),
    "Desert Sun Medical Group": (45, 18.0, [5, 6, 7, 8, 7], 0.25),
    "Heartland Urgent Care": (8, 12.0, [15, 18, 20, 22, 19], 0.35),
    "Bayview Behavioral Health": (14, 20.0, [25, 28, 30, 32, 27], 0.15),
    "Clearpath EHR Solutions": (6.5, 22.0, [18, 20, 25, 22, 20], 0.1),
    "RedLeaf Cybersecurity": (9, 16.0, [30, 35, 40, 38, 32], 0.15),
    "Brightline Analytics": (4.2, 18.0, [20, 25, 30, 28, 24], 0.1),
    "StackVault Cloud": (4.2, -5.0, [80, 60, 45, 50, 42], 0.5),
    "ComplianceForge": (8.5, 24.0, [12, 15, 18, 16, 14], 0.1),
    "Lone Star Precision Parts": (28, 16.0, [6, 8, 10, 9, 7], 0.3),
    "Summit HVAC Services": (18, 14.0, [8, 10, 12, 14, 11], 0.2),
    "Pacific Coast Packaging": (35, 12.0, [4, 5, 6, 7, 5], 0.35),
    "Cascade Fluid Systems": (11, 15.0, [7, 9, 11, 10, 8], 0.2),
    "Appalachian Steel Works": (42, 11.0, [3, 4, 5, 6, 4], 0.4),
    "AutoCare Express": (15, 16.0, [12, 15, 18, 20, 16], 0.25),
    "Mountain West Auto Group": (7.5, 14.0, [10, 15, 20, 25, 22], 0.2),
}


# ---------------------------------------------------------------------------
# Valuation records (for comp analysis)
# ---------------------------------------------------------------------------

# (company_name, valuation_date, enterprise_value_M, ev_revenue, ev_ebitda,
#  valuation_type, methodology, event_type)
VALUATIONS = [
    # Summit Ridge — Healthcare
    ("MedVantage Health Systems", date(2025, 9, 30), 310, 2.5, 11.2, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("MedVantage Health Systems", date(2024, 9, 30), 250, 2.2, 10.5, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("Apex Revenue Solutions", date(2025, 9, 30), 195, 2.6, 10.8, "Mark-to-Market", "DCF", "Quarterly Mark"),
    ("CloudShield Security", date(2025, 9, 30), 280, 4.8, 22.5, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("TrueNorth Behavioral", date(2025, 9, 30), 520, 2.4, 12.5, "Mark-to-Market", "DCF", "Quarterly Mark"),
    ("Precision Lab Diagnostics", date(2025, 9, 30), 230, 2.8, 10.2, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("Elevate Staffing Group", date(2025, 9, 30), 140, 2.2, 14.0, "Mark-to-Market", "DCF", "Quarterly Mark"),
    # Cascade — Software
    ("FinLedger Technologies", date(2025, 9, 30), 350, 5.5, 28.0, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("Nimbus Data Cloud", date(2025, 9, 30), 280, 6.2, None, "Mark-to-Market", "Revenue Multiple", "Quarterly Mark"),
    ("VeriComply AI", date(2025, 9, 30), 190, 7.5, None, "Mark-to-Market", "Revenue Multiple", "Quarterly Mark"),
    ("PayGrid Systems", date(2025, 9, 30), 420, 4.2, 18.5, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("InsightFlow Analytics", date(2025, 9, 30), 240, 4.5, 20.0, "Mark-to-Market", "DCF", "Quarterly Mark"),
    ("ShieldPay Fraud Detection", date(2025, 9, 30), 120, 8.0, None, "Mark-to-Market", "Revenue Multiple", "Quarterly Mark"),
    # Ironforge — Industrials
    ("Titan Precision Manufacturing", date(2025, 9, 30), 550, 1.8, 9.5, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("AeroSpec Coatings", date(2025, 9, 30), 290, 2.1, 10.8, "Mark-to-Market", "DCF", "Quarterly Mark"),
    ("Continental Packaging Solutions", date(2025, 9, 30), 520, 1.6, 9.0, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("Midwest Valve & Controls", date(2025, 9, 30), 350, 2.0, 10.0, "Mark-to-Market", "DCF", "Quarterly Mark"),
    ("SteelCore Fabrication", date(2025, 9, 30), 210, 1.7, 9.8, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
    ("DefenseTech Systems", date(2025, 9, 30), 200, 2.5, 13.5, "Mark-to-Market", "Comparable Companies", "Quarterly Mark"),
]


# ===========================================================================
# Main seeder function
# ===========================================================================

async def seed_pe_demo_data(db: Session) -> Dict[str, int]:
    """
    Seed all PE demo data. Idempotent — upserts by unique key.

    Returns dict of table → row counts inserted/updated.
    """
    counts: Dict[str, int] = {}

    # 1. Firms
    logger.info("Seeding %d PE firms", len(FIRMS))
    counts["pe_firms"] = _upsert_rows(db, PEFirm, FIRMS, ["name"])

    # 2. Funds
    all_funds = []
    for firm_name, fund_list in FUNDS.items():
        firm_id = _lookup_id(db, PEFirm, name=firm_name)
        if not firm_id:
            logger.warning("Firm not found: %s", firm_name)
            continue
        for fund in fund_list:
            all_funds.append({**fund, "firm_id": firm_id, "data_source": "demo_seeder"})
    counts["pe_funds"] = _upsert_rows(db, PEFund, all_funds, ["name"], has_db_constraint=False)

    # 3. Fund performance
    all_perf = []
    for fund_name, perf_list in FUND_PERFORMANCE.items():
        fund_id = _lookup_id(db, PEFund, name=fund_name)
        if not fund_id:
            logger.warning("Fund not found: %s", fund_name)
            continue
        for perf in perf_list:
            all_perf.append({**perf, "fund_id": fund_id})
    counts["pe_fund_performance"] = _upsert_rows(
        db, PEFundPerformance, all_perf, ["fund_id", "as_of_date"],
    )

    # 4. Portfolio companies
    all_companies = []
    for firm_name, co_list in PORTFOLIO_COMPANIES.items():
        for co in co_list:
            all_companies.append({**co, "data_source": "demo_seeder"})
    counts["pe_portfolio_companies"] = _upsert_rows(
        db, PEPortfolioCompany, all_companies, ["name"], has_db_constraint=False,
    )

    # 5. Company financials
    all_financials = []
    for co_name, params in COMPANY_FINANCIALS.items():
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Company not found: %s", co_name)
            continue
        for fin in _generate_financials(co_name, **params):
            all_financials.append({**fin, "company_id": co_id})
    counts["pe_company_financials"] = _upsert_rows(
        db, PECompanyFinancials, all_financials,
        ["company_id", "fiscal_year", "fiscal_period"],
    )

    # 6. People
    counts["pe_people"] = _upsert_rows(db, PEPerson, PEOPLE, ["full_name"], has_db_constraint=False)

    # 7. Firm-people links (no unique constraint, so select-or-insert)
    fp_count = 0
    for person_name, (firm_name, title, seniority) in PERSON_FIRM_MAP.items():
        person_id = _lookup_id(db, PEPerson, full_name=person_name)
        firm_id = _lookup_id(db, PEFirm, name=firm_name)
        if not (person_id and firm_id):
            continue
        existing = db.execute(
            select(PEFirmPeople.id).where(
                PEFirmPeople.firm_id == firm_id,
                PEFirmPeople.person_id == person_id,
            )
        ).scalar_one_or_none()
        if existing:
            fp_count += 1
            continue
        db.add(PEFirmPeople(
            firm_id=firm_id, person_id=person_id,
            title=title, seniority=seniority, is_current=True,
        ))
        fp_count += 1
    db.flush()
    counts["pe_firm_people"] = fp_count

    # 8. Company executives (PEPerson records)
    logger.info("Seeding %d company executives", len(COMPANY_EXECUTIVES))
    counts["pe_people_executives"] = _upsert_rows(
        db, PEPerson, COMPANY_EXECUTIVES, ["full_name"], has_db_constraint=False,
    )

    # 9. Company leadership records (executive → company)
    leadership_count = 0
    for co_name, leaders in COMPANY_LEADERSHIP_MAP.items():
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Company not found for leadership: %s", co_name)
            continue
        for (person_name, title, role_cat, is_ceo, is_cfo, is_board, appointed_pe, pe_affil) in leaders:
            person_id = _lookup_id(db, PEPerson, full_name=person_name)
            if not person_id:
                logger.warning("Person not found: %s", person_name)
                continue
            existing = db.execute(
                select(PECompanyLeadership.id).where(
                    PECompanyLeadership.company_id == co_id,
                    PECompanyLeadership.person_id == person_id,
                    PECompanyLeadership.title == title,
                )
            ).scalar_one_or_none()
            if existing:
                leadership_count += 1
                continue
            db.add(PECompanyLeadership(
                company_id=co_id, person_id=person_id, title=title,
                role_category=role_cat, is_ceo=is_ceo, is_cfo=is_cfo,
                is_board_member=is_board, is_current=True,
                appointed_by_pe=appointed_pe,
                pe_firm_affiliation=pe_affil,
                start_date=date(2020, 1, 1),
                data_source="demo_seeder",
            ))
            leadership_count += 1
    db.flush()
    counts["pe_company_leadership"] = leadership_count

    # 10. Board seats (PE partners → portfolio company boards)
    board_count = 0
    for (person_name, co_name, title, is_chair) in BOARD_SEATS:
        person_id = _lookup_id(db, PEPerson, full_name=person_name)
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not (person_id and co_id):
            logger.warning("Board seat lookup failed: %s → %s", person_name, co_name)
            continue
        existing = db.execute(
            select(PECompanyLeadership.id).where(
                PECompanyLeadership.company_id == co_id,
                PECompanyLeadership.person_id == person_id,
                PECompanyLeadership.is_board_member == True,
            )
        ).scalar_one_or_none()
        if existing:
            board_count += 1
            continue
        db.add(PECompanyLeadership(
            company_id=co_id, person_id=person_id, title=title,
            role_category="Board", is_board_member=True, is_board_chair=is_chair,
            is_current=True, appointed_by_pe=True,
            pe_firm_affiliation=_get_person_firm(person_name),
            start_date=date(2019, 1, 1),
            data_source="demo_seeder",
        ))
        board_count += 1
    db.flush()
    counts["pe_company_leadership_board"] = board_count

    # 11. Competitor mappings
    comp_count = 0
    for co_name, competitors in COMPETITOR_MAPPINGS.items():
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Company not found for competitor mapping: %s", co_name)
            continue
        for (comp_name, comp_type, rel_size, mkt_pos, is_pub, ticker, is_pe, pe_owner, notes) in competitors:
            existing = db.execute(
                select(PECompetitorMapping.id).where(
                    PECompetitorMapping.company_id == co_id,
                    PECompetitorMapping.competitor_name == comp_name,
                )
            ).scalar_one_or_none()
            if existing:
                comp_count += 1
                continue
            db.add(PECompetitorMapping(
                company_id=co_id, competitor_name=comp_name,
                competitor_type=comp_type, relative_size=rel_size,
                market_position=mkt_pos, is_public=is_pub, ticker=ticker,
                is_pe_backed=is_pe, pe_owner=pe_owner, notes=notes,
                data_source="demo_seeder", last_verified=date(2025, 9, 1),
            ))
            comp_count += 1
    db.flush()
    counts["pe_competitor_mappings"] = comp_count

    # 12. Company news
    news_count = 0
    for co_name, articles in COMPANY_NEWS.items():
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Company not found for news: %s", co_name)
            continue
        for (title, source_name, url_slug, summary, news_type, sentiment, score, pub_date) in articles:
            source_url = f"https://example.com/news/{url_slug}"
            existing = db.execute(
                select(PECompanyNews.id).where(
                    PECompanyNews.source_url == source_url,
                )
            ).scalar_one_or_none()
            if existing:
                news_count += 1
                continue
            db.add(PECompanyNews(
                company_id=co_id, title=title, source_name=source_name,
                source_url=source_url, summary=summary, news_type=news_type,
                sentiment=sentiment, sentiment_score=Decimal(str(score)),
                relevance_score=Decimal("0.85"), is_primary=True,
                published_date=pub_date,
            ))
            news_count += 1
    db.flush()
    counts["pe_company_news"] = news_count

    # 13. Deals
    all_deals = []
    for deal in DEALS:
        co_name = deal.pop("company_name")
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Company not found for deal: %s", co_name)
            deal["company_name"] = co_name  # restore
            continue
        all_deals.append({**deal, "company_id": co_id})
        deal["company_name"] = co_name  # restore for idempotency
    counts["pe_deals"] = _upsert_rows(db, PEDeal, all_deals, ["deal_name"], has_db_constraint=False)

    # 12. Investments (fund → company) — no unique constraint, select-or-insert
    inv_count = 0
    for fund_name, inv_list in INVESTMENTS.items():
        fund_id = _lookup_id(db, PEFund, name=fund_name)
        if not fund_id:
            logger.warning("Fund not found: %s", fund_name)
            continue
        for inv in inv_list:
            co_name = inv.pop("company_name")
            co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
            if not co_id:
                logger.warning("Company not found: %s", co_name)
                inv["company_name"] = co_name
                continue
            existing = db.execute(
                select(PEFundInvestment.id).where(
                    PEFundInvestment.fund_id == fund_id,
                    PEFundInvestment.company_id == co_id,
                )
            ).scalar_one_or_none()
            inv["company_name"] = co_name  # restore
            if existing:
                inv_count += 1
                continue
            row = {k: v for k, v in inv.items() if k != "company_name"}
            row["fund_id"] = fund_id
            row["company_id"] = co_id
            db.add(PEFundInvestment(**row))
            inv_count += 1
    db.flush()
    counts["pe_fund_investments"] = inv_count

    # 14. Valuations (for comp analysis)
    val_count = 0
    for (co_name, val_date, ev_m, ev_rev, ev_ebitda, val_type, method, event) in VALUATIONS:
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Company not found for valuation: %s", co_name)
            continue
        existing = db.execute(
            select(PECompanyValuation.id).where(
                PECompanyValuation.company_id == co_id,
                PECompanyValuation.valuation_date == val_date,
            )
        ).scalar_one_or_none()
        if existing:
            val_count += 1
            continue
        db.add(PECompanyValuation(
            company_id=co_id,
            valuation_date=val_date,
            enterprise_value_usd=Decimal(str(ev_m * 1_000_000)),
            ev_revenue_multiple=Decimal(str(ev_rev)) if ev_rev else None,
            ev_ebitda_multiple=Decimal(str(ev_ebitda)) if ev_ebitda else None,
            valuation_type=val_type,
            methodology=method,
            event_type=event,
            data_source="demo_seeder",
            confidence="high",
        ))
        val_count += 1
    db.flush()
    counts["pe_valuations"] = val_count

    # 15. Independent target companies (for screener)
    target_rows = [{**t, "data_source": "demo_seeder"} for t in INDEPENDENT_TARGETS]
    counts["pe_independent_targets"] = _upsert_rows(
        db, PEPortfolioCompany, target_rows, ["name"], has_db_constraint=False,
    )

    # 15. Independent target financials
    all_target_fins = []
    for co_name, params in INDEPENDENT_TARGET_FINANCIALS.items():
        co_id = _lookup_id(db, PEPortfolioCompany, name=co_name)
        if not co_id:
            logger.warning("Target company not found: %s", co_name)
            continue
        base_rev, base_margin, growths, debt = params
        for fin in _generate_financials(co_name, base_revenue=base_rev,
                                         base_ebitda_margin=base_margin,
                                         growth_rates=growths, debt_ratio=debt):
            all_target_fins.append({**fin, "company_id": co_id})
    counts["pe_target_financials"] = _upsert_rows(
        db, PECompanyFinancials, all_target_fins,
        ["company_id", "fiscal_year", "fiscal_period"],
    )

    db.commit()

    total = sum(counts.values())
    logger.info("PE demo seeder complete: %d total rows across %d tables", total, len(counts))
    for table, count in counts.items():
        logger.info("  %s: %d rows", table, count)

    return counts
