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

from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyLeadership,
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
            all_funds.append({**fund, "firm_id": firm_id})
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
            all_companies.append(co)
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

    # 8. Deals
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

    # 9. Investments (fund → company) — no unique constraint, select-or-insert
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

    db.commit()

    total = sum(counts.values())
    logger.info("PE demo seeder complete: %d total rows across %d tables", total, len(counts))
    for table, count in counts.items():
        logger.info("  %s: %d rows", table, count)

    return counts
