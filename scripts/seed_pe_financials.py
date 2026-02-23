"""
Seed PE financial demo data for benchmark and exit-readiness endpoints.

Populates: pe_company_financials, pe_competitor_mappings, pe_company_leadership,
pe_company_valuations for ~10 key demo companies with realistic data.

Usage:
    docker exec nexdata-api-1 python scripts/seed_pe_financials.py
"""

import sys
import os
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://nexdata:nexdata@postgres:5432/nexdata"
)
engine = create_engine(DATABASE_URL)


# =============================================================================
# Demo Company Data (based on public knowledge of PE-backed companies)
# =============================================================================

COMPANIES = {
    # ServiceTitan - Thoma Bravo, field service management SaaS
    274: {
        "name": "SERVICETITAN INC",
        "pe_owner": "Thoma Bravo",
        "industry": "Software",
        "sub_industry": "Field Service Management",
        "sector": "Technology",
        "founded_year": 2012,
        "employee_count": 2100,
        "financials": [
            {"year": 2021, "revenue": 250e6, "growth": 42.0, "ebitda_margin": -15.0, "gross_margin": 65.0},
            {"year": 2022, "revenue": 400e6, "growth": 60.0, "ebitda_margin": -8.0, "gross_margin": 67.0},
            {"year": 2023, "revenue": 560e6, "growth": 40.0, "ebitda_margin": 2.0, "gross_margin": 68.0},
            {"year": 2024, "revenue": 720e6, "growth": 28.5, "ebitda_margin": 10.0, "gross_margin": 70.0},
            {"year": 2025, "revenue": 900e6, "growth": 25.0, "ebitda_margin": 18.0, "gross_margin": 72.0},
        ],
        "valuations": [
            {"date": "2021-11-01", "ev": 9500e6, "ev_rev": 38.0, "ev_ebitda": None, "type": "Transaction", "event": "Investment"},
            {"date": "2023-06-01", "ev": 8000e6, "ev_rev": 14.3, "ev_ebitda": None, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2024-12-01", "ev": 12000e6, "ev_rev": 16.7, "ev_ebitda": 166.7, "type": "Transaction", "event": "IPO"},
        ],
        "competitors": [
            {"name": "Housecall Pro", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "Goldman Sachs"},
            {"name": "Jobber", "type": "Direct", "size": "Smaller", "position": "Niche", "public": False, "pe_backed": True, "pe_owner": "Summit Partners"},
            {"name": "FieldEdge", "type": "Direct", "size": "Smaller", "position": "Niche", "public": False, "pe_backed": True, "pe_owner": "Advent International"},
            {"name": "Salesforce Field Service", "type": "Indirect", "size": "Larger", "position": "Leader", "public": True, "ticker": "CRM"},
        ],
        "leadership": [
            {"name": "Ara Mahdessian", "title": "Co-Founder & CEO", "category": "C-Suite", "ceo": True, "start": "2012-01-01", "pe_appointed": False},
            {"name": "Vahe Kuzoyan", "title": "Co-Founder & President", "category": "C-Suite", "start": "2012-01-01", "pe_appointed": False},
            {"name": "Ramin Beheshti", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2022-03-01", "pe_appointed": True},
            {"name": "David Mandell", "title": "General Counsel & Secretary", "category": "C-Suite", "start": "2020-06-01"},
            {"name": "Orlando Bravo", "title": "Board Member", "category": "Board", "board": True, "start": "2021-11-01", "pe_firm": "Thoma Bravo"},
            {"name": "Holden Spaht", "title": "Board Member", "category": "Board", "board": True, "start": "2018-01-01", "pe_firm": "Thoma Bravo"},
        ],
    },
    # SailPoint - Thoma Bravo, identity security
    308: {
        "name": "SAILPOINT INC",
        "pe_owner": "Thoma Bravo",
        "industry": "Cybersecurity",
        "sub_industry": "Identity Governance",
        "sector": "Technology",
        "founded_year": 2005,
        "employee_count": 2400,
        "financials": [
            {"year": 2021, "revenue": 350e6, "growth": 18.0, "ebitda_margin": 12.0, "gross_margin": 75.0},
            {"year": 2022, "revenue": 420e6, "growth": 20.0, "ebitda_margin": 15.0, "gross_margin": 76.0},
            {"year": 2023, "revenue": 510e6, "growth": 21.4, "ebitda_margin": 22.0, "gross_margin": 78.0},
            {"year": 2024, "revenue": 620e6, "growth": 21.5, "ebitda_margin": 28.0, "gross_margin": 80.0},
            {"year": 2025, "revenue": 750e6, "growth": 21.0, "ebitda_margin": 32.0, "gross_margin": 81.0},
        ],
        "valuations": [
            {"date": "2022-08-01", "ev": 6900e6, "ev_rev": 16.4, "ev_ebitda": 109.5, "type": "Transaction", "event": "Investment"},
            {"date": "2024-01-01", "ev": 8500e6, "ev_rev": 13.7, "ev_ebitda": 49.0, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2025-06-01", "ev": 11500e6, "ev_rev": 15.3, "ev_ebitda": 47.9, "type": "Transaction", "event": "IPO"},
        ],
        "competitors": [
            {"name": "CyberArk", "type": "Direct", "size": "Similar", "position": "Challenger", "public": True, "ticker": "CYBR"},
            {"name": "Okta", "type": "Direct", "size": "Larger", "position": "Leader", "public": True, "ticker": "OKTA"},
            {"name": "Saviynt", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "AB Private Credit"},
            {"name": "ForgeRock", "type": "Direct", "size": "Smaller", "position": "Niche", "public": False, "pe_backed": True, "pe_owner": "Thoma Bravo"},
            {"name": "OneSpan", "type": "Indirect", "size": "Smaller", "position": "Niche", "public": True, "ticker": "OSPN"},
        ],
        "leadership": [
            {"name": "Mark McClain", "title": "Founder & CEO", "category": "C-Suite", "ceo": True, "start": "2005-01-01", "pe_appointed": False},
            {"name": "Matt Mills", "title": "President & COO", "category": "C-Suite", "start": "2022-10-01", "pe_appointed": True},
            {"name": "Chris Schmitt", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2023-01-01", "pe_appointed": True},
            {"name": "Grady Summers", "title": "Chief Technology Officer", "category": "C-Suite", "start": "2021-05-01"},
            {"name": "Seth Boro", "title": "Board Member", "category": "Board", "board": True, "start": "2022-08-01", "pe_firm": "Thoma Bravo"},
        ],
    },
    # Medline - Carlyle/Hellman & Friedman, healthcare distribution
    1001: {
        "name": "Medline Inc",
        "pe_owner": "Carlyle Group",
        "industry": "Healthcare",
        "sub_industry": "Medical Distribution",
        "sector": "Healthcare",
        "founded_year": 1966,
        "employee_count": 34000,
        "financials": [
            {"year": 2021, "revenue": 20500e6, "growth": 12.0, "ebitda_margin": 14.0, "gross_margin": 28.0},
            {"year": 2022, "revenue": 22000e6, "growth": 7.3, "ebitda_margin": 13.5, "gross_margin": 27.5},
            {"year": 2023, "revenue": 23200e6, "growth": 5.4, "ebitda_margin": 14.5, "gross_margin": 29.0},
            {"year": 2024, "revenue": 24800e6, "growth": 6.9, "ebitda_margin": 15.0, "gross_margin": 29.5},
            {"year": 2025, "revenue": 26500e6, "growth": 6.8, "ebitda_margin": 15.5, "gross_margin": 30.0},
        ],
        "valuations": [
            {"date": "2021-06-01", "ev": 34000e6, "ev_rev": 1.66, "ev_ebitda": 11.8, "type": "Transaction", "event": "Investment"},
            {"date": "2023-12-01", "ev": 38000e6, "ev_rev": 1.64, "ev_ebitda": 11.3, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2025-06-01", "ev": 42000e6, "ev_rev": 1.58, "ev_ebitda": 10.2, "type": "Mark-to-Market", "event": "Quarterly Mark"},
        ],
        "competitors": [
            {"name": "McKesson", "type": "Direct", "size": "Larger", "position": "Leader", "public": True, "ticker": "MCK"},
            {"name": "Cardinal Health", "type": "Direct", "size": "Larger", "position": "Leader", "public": True, "ticker": "CAH"},
            {"name": "Owens & Minor", "type": "Direct", "size": "Similar", "position": "Challenger", "public": True, "ticker": "OMI"},
            {"name": "Henry Schein", "type": "Indirect", "size": "Similar", "position": "Challenger", "public": True, "ticker": "HSIC"},
        ],
        "leadership": [
            {"name": "Charlie Mills", "title": "Chief Executive Officer", "category": "C-Suite", "ceo": True, "start": "2015-01-01"},
            {"name": "Jim Abrams", "title": "President", "category": "C-Suite", "start": "2018-03-01"},
            {"name": "John Weiland", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2021-09-01", "pe_appointed": True},
            {"name": "Kara Hartnett", "title": "Chief Human Resources Officer", "category": "C-Suite", "start": "2019-06-01"},
            {"name": "Sandra Horbach", "title": "Board Member", "category": "Board", "board": True, "start": "2021-06-01", "pe_firm": "Carlyle Group"},
        ],
    },
    # Coupa Software - Thoma Bravo, business spend management
    1341: {
        "name": "Coupa Software Inc",
        "pe_owner": "Thoma Bravo",
        "industry": "Software",
        "sub_industry": "Business Spend Management",
        "sector": "Technology",
        "founded_year": 2006,
        "employee_count": 3500,
        "financials": [
            {"year": 2021, "revenue": 725e6, "growth": 16.0, "ebitda_margin": 8.0, "gross_margin": 68.0},
            {"year": 2022, "revenue": 840e6, "growth": 15.8, "ebitda_margin": 12.0, "gross_margin": 70.0},
            {"year": 2023, "revenue": 970e6, "growth": 15.5, "ebitda_margin": 20.0, "gross_margin": 72.0},
            {"year": 2024, "revenue": 1100e6, "growth": 13.4, "ebitda_margin": 28.0, "gross_margin": 74.0},
            {"year": 2025, "revenue": 1240e6, "growth": 12.7, "ebitda_margin": 32.0, "gross_margin": 75.0},
        ],
        "valuations": [
            {"date": "2023-02-01", "ev": 8000e6, "ev_rev": 8.2, "ev_ebitda": 41.2, "type": "Transaction", "event": "Investment"},
            {"date": "2024-06-01", "ev": 10500e6, "ev_rev": 9.5, "ev_ebitda": 34.1, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2025-12-01", "ev": 13000e6, "ev_rev": 10.5, "ev_ebitda": 32.7, "type": "Mark-to-Market", "event": "Quarterly Mark"},
        ],
        "competitors": [
            {"name": "SAP Ariba", "type": "Direct", "size": "Larger", "position": "Leader", "public": True, "ticker": "SAP"},
            {"name": "Jaggaer", "type": "Direct", "size": "Similar", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "Cinven"},
            {"name": "GEP", "type": "Direct", "size": "Similar", "position": "Challenger", "public": False},
            {"name": "Ivalua", "type": "Direct", "size": "Smaller", "position": "Niche", "public": False, "pe_backed": True, "pe_owner": "Eurazeo"},
        ],
        "leadership": [
            {"name": "Rob Bernshteyn", "title": "Chairman & CEO", "category": "C-Suite", "ceo": True, "start": "2009-01-01"},
            {"name": "Todd Ford", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2023-04-01", "pe_appointed": True},
            {"name": "Donna Wilczek", "title": "Chief Product Officer", "category": "C-Suite", "start": "2015-01-01"},
            {"name": "A.J. Hanna", "title": "Chief Revenue Officer", "category": "C-Suite", "start": "2023-06-01", "pe_appointed": True},
            {"name": "Hudson Smith", "title": "Board Member", "category": "Board", "board": True, "start": "2023-02-01", "pe_firm": "Thoma Bravo"},
        ],
    },
    # Qualtrics - Silver Lake/CPP, experience management
    1349: {
        "name": "Qualtrics International Inc.",
        "pe_owner": "Silver Lake",
        "industry": "Software",
        "sub_industry": "Experience Management",
        "sector": "Technology",
        "founded_year": 2002,
        "employee_count": 5800,
        "financials": [
            {"year": 2021, "revenue": 1080e6, "growth": 41.0, "ebitda_margin": -5.0, "gross_margin": 72.0},
            {"year": 2022, "revenue": 1470e6, "growth": 36.1, "ebitda_margin": 2.0, "gross_margin": 73.0},
            {"year": 2023, "revenue": 1680e6, "growth": 14.3, "ebitda_margin": 12.0, "gross_margin": 74.0},
            {"year": 2024, "revenue": 1850e6, "growth": 10.1, "ebitda_margin": 20.0, "gross_margin": 76.0},
            {"year": 2025, "revenue": 2020e6, "growth": 9.2, "ebitda_margin": 25.0, "gross_margin": 77.0},
        ],
        "valuations": [
            {"date": "2023-06-01", "ev": 12500e6, "ev_rev": 7.4, "ev_ebitda": 62.0, "type": "Transaction", "event": "Investment"},
            {"date": "2024-12-01", "ev": 15000e6, "ev_rev": 8.1, "ev_ebitda": 40.5, "type": "Mark-to-Market", "event": "Quarterly Mark"},
        ],
        "competitors": [
            {"name": "Medallia", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "Thoma Bravo"},
            {"name": "SurveyMonkey (Momentive)", "type": "Direct", "size": "Smaller", "position": "Niche", "public": False, "pe_backed": True, "pe_owner": "Symphony Technology Group"},
            {"name": "Sprinklr", "type": "Indirect", "size": "Smaller", "position": "Niche", "public": True, "ticker": "CXM"},
            {"name": "Salesforce", "type": "Indirect", "size": "Larger", "position": "Leader", "public": True, "ticker": "CRM"},
        ],
        "leadership": [
            {"name": "Zig Serafin", "title": "Chief Executive Officer", "category": "C-Suite", "ceo": True, "start": "2021-07-01"},
            {"name": "Rob Bachman", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2023-09-01", "pe_appointed": True},
            {"name": "Brad Anderson", "title": "President", "category": "C-Suite", "start": "2022-01-01"},
            {"name": "Greg Henry", "title": "Board Member", "category": "Board", "board": True, "start": "2023-06-01", "pe_firm": "Silver Lake"},
        ],
    },
    # Alteryx - Clearlake/Insight, analytics automation
    1533: {
        "name": "Alteryx, Inc.",
        "pe_owner": "Clearlake Capital",
        "industry": "Software",
        "sub_industry": "Analytics Automation",
        "sector": "Technology",
        "founded_year": 1997,
        "employee_count": 2800,
        "financials": [
            {"year": 2021, "revenue": 530e6, "growth": 13.0, "ebitda_margin": -8.0, "gross_margin": 88.0},
            {"year": 2022, "revenue": 620e6, "growth": 17.0, "ebitda_margin": -2.0, "gross_margin": 87.0},
            {"year": 2023, "revenue": 660e6, "growth": 6.4, "ebitda_margin": 8.0, "gross_margin": 86.0},
            {"year": 2024, "revenue": 730e6, "growth": 10.6, "ebitda_margin": 18.0, "gross_margin": 87.0},
            {"year": 2025, "revenue": 810e6, "growth": 11.0, "ebitda_margin": 24.0, "gross_margin": 88.0},
        ],
        "valuations": [
            {"date": "2024-03-01", "ev": 4400e6, "ev_rev": 6.0, "ev_ebitda": 33.3, "type": "Transaction", "event": "Investment"},
            {"date": "2025-06-01", "ev": 5800e6, "ev_rev": 7.2, "ev_ebitda": 29.8, "type": "Mark-to-Market", "event": "Quarterly Mark"},
        ],
        "competitors": [
            {"name": "Databricks", "type": "Direct", "size": "Larger", "position": "Leader", "public": False, "pe_backed": True, "pe_owner": "a16z"},
            {"name": "Dataiku", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "CapitalG"},
            {"name": "KNIME", "type": "Direct", "size": "Smaller", "position": "Niche", "public": False},
            {"name": "Palantir", "type": "Indirect", "size": "Larger", "position": "Leader", "public": True, "ticker": "PLTR"},
        ],
        "leadership": [
            {"name": "Mark Anderson", "title": "Chief Executive Officer", "category": "C-Suite", "ceo": True, "start": "2024-05-01", "pe_appointed": True},
            {"name": "Kevin Rubin", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2024-06-01", "pe_appointed": True},
            {"name": "Suresh Vittal", "title": "Chief Product Officer", "category": "C-Suite", "start": "2021-01-01"},
            {"name": "Behdad Eghbali", "title": "Board Member", "category": "Board", "board": True, "start": "2024-03-01", "pe_firm": "Clearlake Capital"},
        ],
    },
    # StandardAero - Carlyle, aerospace MRO
    203: {
        "name": "STANDARDAERO INC",
        "pe_owner": "Carlyle Group",
        "industry": "Aerospace & Defense",
        "sub_industry": "MRO Services",
        "sector": "Industrials",
        "founded_year": 1911,
        "employee_count": 7200,
        "financials": [
            {"year": 2021, "revenue": 3800e6, "growth": 8.0, "ebitda_margin": 16.0, "gross_margin": 32.0},
            {"year": 2022, "revenue": 4200e6, "growth": 10.5, "ebitda_margin": 17.0, "gross_margin": 33.0},
            {"year": 2023, "revenue": 4700e6, "growth": 11.9, "ebitda_margin": 18.0, "gross_margin": 34.0},
            {"year": 2024, "revenue": 5300e6, "growth": 12.7, "ebitda_margin": 19.0, "gross_margin": 35.0},
            {"year": 2025, "revenue": 5900e6, "growth": 11.3, "ebitda_margin": 19.5, "gross_margin": 35.5},
        ],
        "valuations": [
            {"date": "2019-12-01", "ev": 5000e6, "ev_rev": 1.4, "ev_ebitda": 8.9, "type": "Transaction", "event": "Investment"},
            {"date": "2024-09-01", "ev": 10000e6, "ev_rev": 1.9, "ev_ebitda": 9.9, "type": "Transaction", "event": "IPO"},
        ],
        "competitors": [
            {"name": "AAR Corp", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": True, "ticker": "AIR"},
            {"name": "MTU Aero Engines", "type": "Direct", "size": "Similar", "position": "Challenger", "public": True, "ticker": "MTX.DE"},
            {"name": "ST Engineering", "type": "Direct", "size": "Similar", "position": "Challenger", "public": True, "ticker": "S63.SI"},
            {"name": "Heico", "type": "Indirect", "size": "Larger", "position": "Leader", "public": True, "ticker": "HEI"},
        ],
        "leadership": [
            {"name": "Russell Ford", "title": "Chief Executive Officer", "category": "C-Suite", "ceo": True, "start": "2019-12-01", "pe_appointed": True},
            {"name": "Jason Yates", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2020-03-01", "pe_appointed": True},
            {"name": "Marc Drobny", "title": "Chief Operating Officer", "category": "C-Suite", "start": "2020-06-01"},
            {"name": "Pooja Goyal", "title": "Board Member", "category": "Board", "board": True, "start": "2019-12-01", "pe_firm": "Carlyle Group"},
        ],
    },
    # N-ABLE - Thoma Bravo spin-off, IT management
    355: {
        "name": "N-ABLE INC",
        "pe_owner": "Thoma Bravo",
        "industry": "Software",
        "sub_industry": "IT Management",
        "sector": "Technology",
        "founded_year": 2000,
        "employee_count": 1800,
        "financials": [
            {"year": 2021, "revenue": 340e6, "growth": 14.0, "ebitda_margin": 22.0, "gross_margin": 82.0},
            {"year": 2022, "revenue": 390e6, "growth": 14.7, "ebitda_margin": 24.0, "gross_margin": 83.0},
            {"year": 2023, "revenue": 440e6, "growth": 12.8, "ebitda_margin": 26.0, "gross_margin": 84.0},
            {"year": 2024, "revenue": 495e6, "growth": 12.5, "ebitda_margin": 28.0, "gross_margin": 84.5},
            {"year": 2025, "revenue": 555e6, "growth": 12.1, "ebitda_margin": 30.0, "gross_margin": 85.0},
        ],
        "valuations": [
            {"date": "2021-07-01", "ev": 2100e6, "ev_rev": 6.2, "ev_ebitda": 28.0, "type": "Transaction", "event": "Investment"},
            {"date": "2024-01-01", "ev": 3200e6, "ev_rev": 6.5, "ev_ebitda": 23.1, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2025-06-01", "ev": 4000e6, "ev_rev": 7.2, "ev_ebitda": 24.0, "type": "Mark-to-Market", "event": "Quarterly Mark"},
        ],
        "competitors": [
            {"name": "ConnectWise", "type": "Direct", "size": "Similar", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "Thoma Bravo"},
            {"name": "Datto", "type": "Direct", "size": "Similar", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "Kaseya/Insight"},
            {"name": "NinjaOne", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "ICONIQ"},
            {"name": "Kaseya", "type": "Direct", "size": "Similar", "position": "Leader", "public": False, "pe_backed": True, "pe_owner": "Insight Partners"},
        ],
        "leadership": [
            {"name": "John Pagliuca", "title": "Chief Executive Officer", "category": "C-Suite", "ceo": True, "start": "2021-07-01", "pe_appointed": True},
            {"name": "Tim O'Brien", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2021-08-01", "pe_appointed": True},
            {"name": "Andrew Doherty", "title": "Chief Revenue Officer", "category": "C-Suite", "start": "2022-01-01"},
            {"name": "Byron Deeter", "title": "Board Member", "category": "Board", "board": True, "start": "2021-07-01", "pe_firm": "Thoma Bravo"},
        ],
    },
    # Ingram Micro - Platinum Equity, IT distribution
    733: {
        "name": "INGRAM MICRO HLDG CORP",
        "pe_owner": "Platinum Equity",
        "industry": "Technology Distribution",
        "sub_industry": "IT Distribution",
        "sector": "Technology",
        "founded_year": 1979,
        "employee_count": 24000,
        "financials": [
            {"year": 2021, "revenue": 49600e6, "growth": 12.0, "ebitda_margin": 3.2, "gross_margin": 6.8},
            {"year": 2022, "revenue": 50900e6, "growth": 2.6, "ebitda_margin": 3.0, "gross_margin": 6.5},
            {"year": 2023, "revenue": 47200e6, "growth": -7.3, "ebitda_margin": 2.8, "gross_margin": 6.3},
            {"year": 2024, "revenue": 48500e6, "growth": 2.8, "ebitda_margin": 3.1, "gross_margin": 6.6},
            {"year": 2025, "revenue": 51000e6, "growth": 5.2, "ebitda_margin": 3.3, "gross_margin": 6.8},
        ],
        "valuations": [
            {"date": "2021-03-01", "ev": 7500e6, "ev_rev": 0.15, "ev_ebitda": 4.7, "type": "Transaction", "event": "Investment"},
            {"date": "2024-10-01", "ev": 8500e6, "ev_rev": 0.18, "ev_ebitda": 5.6, "type": "Transaction", "event": "IPO"},
        ],
        "competitors": [
            {"name": "TD SYNNEX", "type": "Direct", "size": "Larger", "position": "Leader", "public": True, "ticker": "SNX"},
            {"name": "Arrow Electronics", "type": "Direct", "size": "Similar", "position": "Challenger", "public": True, "ticker": "ARW"},
            {"name": "ScanSource", "type": "Direct", "size": "Smaller", "position": "Niche", "public": True, "ticker": "SCSC"},
        ],
        "leadership": [
            {"name": "Paul Bay", "title": "Chief Executive Officer", "category": "C-Suite", "ceo": True, "start": "2022-04-01", "pe_appointed": True},
            {"name": "Mike Zilis", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2021-06-01", "pe_appointed": True},
            {"name": "Sanjib Sahoo", "title": "Chief Digital Officer", "category": "C-Suite", "start": "2021-01-01"},
            {"name": "Tom Gores", "title": "Board Chairman", "category": "Board", "board": True, "board_chair": True, "start": "2021-03-01", "pe_firm": "Platinum Equity"},
        ],
    },
    # Topgolf Callaway - public but PE involvement, entertainment/golf
    406: {
        "name": "TOPGOLF CALLAWAY BRANDS CORP",
        "pe_owner": None,
        "industry": "Entertainment & Sports",
        "sub_industry": "Golf & Entertainment",
        "sector": "Consumer Discretionary",
        "founded_year": 1982,
        "employee_count": 26000,
        "financials": [
            {"year": 2021, "revenue": 3100e6, "growth": 82.0, "ebitda_margin": 15.0, "gross_margin": 42.0},
            {"year": 2022, "revenue": 3950e6, "growth": 27.4, "ebitda_margin": 16.0, "gross_margin": 43.0},
            {"year": 2023, "revenue": 4250e6, "growth": 7.6, "ebitda_margin": 14.0, "gross_margin": 41.0},
            {"year": 2024, "revenue": 4100e6, "growth": -3.5, "ebitda_margin": 11.0, "gross_margin": 39.0},
            {"year": 2025, "revenue": 4300e6, "growth": 4.9, "ebitda_margin": 13.0, "gross_margin": 40.0},
        ],
        "valuations": [
            {"date": "2022-03-01", "ev": 13000e6, "ev_rev": 3.3, "ev_ebitda": 20.6, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2024-06-01", "ev": 7500e6, "ev_rev": 1.8, "ev_ebitda": 16.6, "type": "Mark-to-Market", "event": "Quarterly Mark"},
            {"date": "2025-12-01", "ev": 8200e6, "ev_rev": 1.9, "ev_ebitda": 14.7, "type": "Mark-to-Market", "event": "Quarterly Mark"},
        ],
        "competitors": [
            {"name": "Acushnet (Titleist)", "type": "Direct", "size": "Similar", "position": "Challenger", "public": True, "ticker": "GOLF"},
            {"name": "TaylorMade", "type": "Direct", "size": "Smaller", "position": "Challenger", "public": False, "pe_backed": True, "pe_owner": "Centroid"},
            {"name": "Drive Shack", "type": "Direct", "size": "Smaller", "position": "Niche", "public": True, "ticker": "DS"},
            {"name": "Peloton", "type": "Indirect", "size": "Similar", "position": "Niche", "public": True, "ticker": "PTON"},
        ],
        "leadership": [
            {"name": "Chip Brewer", "title": "President & CEO", "category": "C-Suite", "ceo": True, "start": "2012-03-01"},
            {"name": "Brian Lynch", "title": "Chief Financial Officer", "category": "C-Suite", "cfo": True, "start": "2017-01-01"},
            {"name": "Pat Macdonald", "title": "President - Topgolf", "category": "C-Suite", "start": "2022-01-01"},
            {"name": "Oliver Brewer III", "title": "Board Chairman", "category": "Board", "board": True, "board_chair": True, "start": "2020-01-01"},
        ],
    },
}


def update_company_metadata(session: Session, company_id: int, data: dict):
    """Update company fields like industry, pe_owner, etc."""
    sql = text("""
        UPDATE pe_portfolio_companies SET
            industry = COALESCE(:industry, industry),
            sub_industry = COALESCE(:sub_industry, sub_industry),
            sector = COALESCE(:sector, sector),
            current_pe_owner = COALESCE(:pe_owner, current_pe_owner),
            founded_year = COALESCE(:founded_year, founded_year),
            employee_count = COALESCE(:employee_count, employee_count),
            ownership_status = COALESCE(:ownership_status, ownership_status),
            updated_at = NOW()
        WHERE id = :id
    """)
    session.execute(sql, {
        "id": company_id,
        "industry": data.get("industry"),
        "sub_industry": data.get("sub_industry"),
        "sector": data.get("sector"),
        "pe_owner": data.get("pe_owner"),
        "founded_year": data.get("founded_year"),
        "employee_count": data.get("employee_count"),
        "ownership_status": "PE-Backed" if data.get("pe_owner") else "Public",
    })


def seed_financials(session: Session, company_id: int, financials: list):
    """Seed pe_company_financials for a company."""
    for f in financials:
        revenue = f["revenue"]
        ebitda = revenue * (f["ebitda_margin"] / 100.0)
        gross_profit = revenue * (f["gross_margin"] / 100.0)
        net_income = ebitda * 0.55  # rough approximation
        total_debt = revenue * 2.5 if f["ebitda_margin"] < 15 else revenue * 1.8
        cash = revenue * 0.08
        ocf = ebitda * 0.85
        capex = revenue * 0.04
        fcf = ocf - capex

        sql = text("""
            INSERT INTO pe_company_financials (
                company_id, fiscal_year, fiscal_period, period_end_date,
                revenue_usd, revenue_growth_pct,
                gross_profit_usd, gross_margin_pct,
                ebitda_usd, ebitda_margin_pct,
                net_income_usd,
                total_assets_usd, total_debt_usd, cash_usd, net_debt_usd,
                operating_cash_flow_usd, capex_usd, free_cash_flow_usd,
                debt_to_ebitda, interest_coverage,
                is_audited, is_estimated, data_source, confidence
            ) VALUES (
                :company_id, :fiscal_year, 'FY', :period_end,
                :revenue, :growth,
                :gross_profit, :gross_margin,
                :ebitda, :ebitda_margin,
                :net_income,
                :total_assets, :total_debt, :cash, :net_debt,
                :ocf, :capex, :fcf,
                :debt_to_ebitda, :interest_coverage,
                false, true, 'Demo Seed', 'medium'
            )
            ON CONFLICT (company_id, fiscal_year, fiscal_period) DO UPDATE SET
                revenue_usd = EXCLUDED.revenue_usd,
                revenue_growth_pct = EXCLUDED.revenue_growth_pct,
                gross_profit_usd = EXCLUDED.gross_profit_usd,
                gross_margin_pct = EXCLUDED.gross_margin_pct,
                ebitda_usd = EXCLUDED.ebitda_usd,
                ebitda_margin_pct = EXCLUDED.ebitda_margin_pct,
                net_income_usd = EXCLUDED.net_income_usd,
                total_debt_usd = EXCLUDED.total_debt_usd,
                cash_usd = EXCLUDED.cash_usd,
                net_debt_usd = EXCLUDED.net_debt_usd,
                operating_cash_flow_usd = EXCLUDED.operating_cash_flow_usd,
                capex_usd = EXCLUDED.capex_usd,
                free_cash_flow_usd = EXCLUDED.free_cash_flow_usd,
                debt_to_ebitda = EXCLUDED.debt_to_ebitda
        """)

        debt_to_ebitda = total_debt / ebitda if ebitda > 0 else None
        interest_coverage = ebitda / (total_debt * 0.06) if total_debt > 0 else None

        session.execute(sql, {
            "company_id": company_id,
            "fiscal_year": f["year"],
            "period_end": date(f["year"], 12, 31),
            "revenue": revenue,
            "growth": f["growth"],
            "gross_profit": gross_profit,
            "gross_margin": f["gross_margin"],
            "ebitda": ebitda,
            "ebitda_margin": f["ebitda_margin"],
            "net_income": net_income if ebitda > 0 else -abs(net_income),
            "total_assets": revenue * 3.2,
            "total_debt": total_debt,
            "cash": cash,
            "net_debt": total_debt - cash,
            "ocf": ocf if ebitda > 0 else ebitda * 0.5,
            "capex": capex,
            "fcf": fcf if ebitda > 0 else ebitda * 0.5 - capex,
            "debt_to_ebitda": round(debt_to_ebitda, 2) if debt_to_ebitda and debt_to_ebitda > 0 else None,
            "interest_coverage": round(interest_coverage, 2) if interest_coverage else None,
        })


def seed_valuations(session: Session, company_id: int, valuations: list):
    """Seed pe_company_valuations."""
    for v in valuations:
        sql = text("""
            INSERT INTO pe_company_valuations (
                company_id, valuation_date,
                enterprise_value_usd, ev_revenue_multiple, ev_ebitda_multiple,
                valuation_type, event_type,
                data_source, confidence
            ) VALUES (
                :company_id, :val_date,
                :ev, :ev_rev, :ev_ebitda,
                :val_type, :event_type,
                'Demo Seed', 'medium'
            )
            ON CONFLICT DO NOTHING
        """)
        session.execute(sql, {
            "company_id": company_id,
            "val_date": date.fromisoformat(v["date"]),
            "ev": v["ev"],
            "ev_rev": v["ev_rev"],
            "ev_ebitda": v["ev_ebitda"],
            "val_type": v["type"],
            "event_type": v["event"],
        })


def seed_competitors(session: Session, company_id: int, competitors: list):
    """Seed pe_competitor_mappings."""
    for c in competitors:
        sql = text("""
            INSERT INTO pe_competitor_mappings (
                company_id, competitor_name,
                is_public, ticker, is_pe_backed, pe_owner,
                competitor_type, relative_size, market_position,
                data_source, last_verified
            ) VALUES (
                :company_id, :name,
                :is_public, :ticker, :is_pe_backed, :pe_owner,
                :type, :size, :position,
                'Demo Seed', :verified
            )
            ON CONFLICT DO NOTHING
        """)
        session.execute(sql, {
            "company_id": company_id,
            "name": c["name"],
            "is_public": c.get("public", False),
            "ticker": c.get("ticker"),
            "is_pe_backed": c.get("pe_backed", False),
            "pe_owner": c.get("pe_owner"),
            "type": c["type"],
            "size": c["size"],
            "position": c["position"],
            "verified": date.today(),
        })


def seed_leadership(session: Session, company_id: int, leadership: list):
    """Seed pe_people and pe_company_leadership."""
    for leader in leadership:
        # Check if person exists
        check_sql = text("SELECT id FROM pe_people WHERE full_name = :name LIMIT 1")
        result = session.execute(check_sql, {"name": leader["name"]})
        row = result.fetchone()
        if row:
            person_id = row[0]
        else:
            insert_sql = text("""
                INSERT INTO pe_people (full_name, current_title)
                VALUES (:name, :title) RETURNING id
            """)
            result = session.execute(insert_sql, {
                "name": leader["name"],
                "title": leader["title"],
            })
            person_id = result.fetchone()[0]

        # Check if leadership record exists
        check_lead = text("""
            SELECT id FROM pe_company_leadership
            WHERE company_id = :company_id AND person_id = :person_id LIMIT 1
        """)
        existing = session.execute(check_lead, {
            "company_id": company_id, "person_id": person_id
        }).fetchone()
        if existing:
            continue

        lead_sql = text("""
            INSERT INTO pe_company_leadership (
                company_id, person_id, title, role_category,
                is_ceo, is_cfo, is_board_member, is_board_chair,
                start_date, is_current,
                appointed_by_pe, pe_firm_affiliation
            ) VALUES (
                :company_id, :person_id, :title, :category,
                :is_ceo, :is_cfo, :is_board, :is_chair,
                :start_date, true,
                :pe_appointed, :pe_firm
            )
        """)
        session.execute(lead_sql, {
            "company_id": company_id,
            "person_id": person_id,
            "title": leader["title"],
            "category": leader["category"],
            "is_ceo": leader.get("ceo", False),
            "is_cfo": leader.get("cfo", False),
            "is_board": leader.get("board", False),
            "is_chair": leader.get("board_chair", False),
            "start_date": date.fromisoformat(leader["start"]) if "start" in leader else None,
            "pe_appointed": leader.get("pe_appointed", False),
            "pe_firm": leader.get("pe_firm"),
        })


def main():
    print("=" * 60)
    print("  PE Financial Demo Data Seeder")
    print("=" * 60)

    with Session(engine) as session:
        total_financials = 0
        total_valuations = 0
        total_competitors = 0
        total_leadership = 0

        for company_id, data in COMPANIES.items():
            print(f"\n  [{company_id}] {data['name']}")

            # Update company metadata
            update_company_metadata(session, company_id, data)

            # Seed financials
            if "financials" in data:
                seed_financials(session, company_id, data["financials"])
                n = len(data["financials"])
                total_financials += n
                print(f"    + {n} financial periods")

            # Seed valuations
            if "valuations" in data:
                seed_valuations(session, company_id, data["valuations"])
                n = len(data["valuations"])
                total_valuations += n
                print(f"    + {n} valuations")

            # Seed competitors
            if "competitors" in data:
                seed_competitors(session, company_id, data["competitors"])
                n = len(data["competitors"])
                total_competitors += n
                print(f"    + {n} competitors")

            # Seed leadership
            if "leadership" in data:
                seed_leadership(session, company_id, data["leadership"])
                n = len(data["leadership"])
                total_leadership += n
                print(f"    + {n} leaders")

        session.commit()

        print("\n" + "=" * 60)
        print(f"  COMPLETE: {len(COMPANIES)} companies seeded")
        print(f"  Financial periods: {total_financials}")
        print(f"  Valuations:        {total_valuations}")
        print(f"  Competitors:       {total_competitors}")
        print(f"  Leadership:        {total_leadership}")
        print("=" * 60)


if __name__ == "__main__":
    main()
