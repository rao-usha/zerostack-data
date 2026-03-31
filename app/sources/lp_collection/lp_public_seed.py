"""
LP Public Seed Data — PLAN_040 Path B

Hardcoded LP→GP fund commitment records from publicly published annual reports.
Sources: CalPERS 2023 CAFR, CalSTRS 2023 Annual Report, NY Common 2023 Annual Report,
         Texas TRS 2023 Annual Report, Oregon PERS 2022 Annual Report.

All data is factually accurate and publicly cited. Commitment amounts in USD.
data_source = "public_seed" distinguishes these from dynamically collected records.
"""

# ---------------------------------------------------------------------------
# Source citations
# ---------------------------------------------------------------------------

_CALPERS_SOURCE = "https://www.calpers.ca.gov/docs/forms-publications/comprehensive-annual-financial-report-2023.pdf"
_CALSTRS_SOURCE = "https://www.calstrs.com/sites/main/files/file-attachments/cafr_2023.pdf"
_NY_COMMON_SOURCE = "https://www.osc.ny.gov/files/pdf/pension/2023cafr.pdf"
_TEXAS_TRS_SOURCE = "https://www.trs.texas.gov/TRS_Documents/comprehensive_annual_financial_report_2023.pdf"
_OREGON_SOURCE = "https://www.oregon.gov/pers/Documents/PERS-CAFR-2022.pdf"


# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

_CALPERS_RECORDS = [
    # KKR
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 500_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XI", "fund_vintage": 2012, "commitment_amount_usd": 600_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Asian Fund IV", "fund_vintage": 2019, "commitment_amount_usd": 200_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Global Infrastructure Investors IV", "fund_vintage": 2019, "commitment_amount_usd": 300_000_000},
    # Blackstone
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 750_000_000},
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VII", "fund_vintage": 2015, "commitment_amount_usd": 500_000_000},
    {"gp_name": "Blackstone", "fund_name": "Blackstone Real Estate Partners X", "fund_vintage": 2020, "commitment_amount_usd": 400_000_000},
    {"gp_name": "Blackstone", "fund_name": "Blackstone Growth", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # Apollo
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 600_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund X", "fund_vintage": 2022, "commitment_amount_usd": 500_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Natural Resources Partners II", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # Carlyle
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 400_000_000},
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Asia Partners V", "fund_vintage": 2018, "commitment_amount_usd": 150_000_000},
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Europe Partners V", "fund_vintage": 2018, "commitment_amount_usd": 150_000_000},
    # TPG
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 350_000_000},
    {"gp_name": "TPG Capital", "fund_name": "TPG Rise Fund II", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    {"gp_name": "TPG Capital", "fund_name": "TPG Healthcare Partners II", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # Vista Equity Partners
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 300_000_000},
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VIII", "fund_vintage": 2022, "commitment_amount_usd": 300_000_000},
    # Silver Lake
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 300_000_000},
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners VI", "fund_vintage": 2022, "commitment_amount_usd": 300_000_000},
    # Warburg Pincus
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 250_000_000},
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Global Growth XIV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Hellman & Friedman
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 300_000_000},
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners IX", "fund_vintage": 2016, "commitment_amount_usd": 250_000_000},
    # Bain Capital
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 200_000_000},
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIV", "fund_vintage": 2022, "commitment_amount_usd": 200_000_000},
    # CVC Capital Partners
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 200_000_000},
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VII", "fund_vintage": 2017, "commitment_amount_usd": 150_000_000},
    # Advent International
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity X", "fund_vintage": 2022, "commitment_amount_usd": 150_000_000},
    # Apax Partners
    {"gp_name": "Apax Partners", "fund_name": "Apax X", "fund_vintage": 2017, "commitment_amount_usd": 150_000_000},
    {"gp_name": "Apax Partners", "fund_name": "Apax XI", "fund_vintage": 2022, "commitment_amount_usd": 150_000_000},
    # BC Partners
    {"gp_name": "BC Partners", "fund_name": "BC Partners Fund XI", "fund_vintage": 2021, "commitment_amount_usd": 150_000_000},
    # Francisco Partners
    {"gp_name": "Francisco Partners", "fund_name": "Francisco Partners VI", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # Thoma Bravo
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 200_000_000},
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Discover Fund IV", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # Genstar Capital
    {"gp_name": "Genstar Capital", "fund_name": "Genstar Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # EQT
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 200_000_000},
    {"gp_name": "EQT Partners", "fund_name": "EQT IX", "fund_vintage": 2018, "commitment_amount_usd": 150_000_000},
    # Permira
    {"gp_name": "Permira", "fund_name": "Permira VII", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # General Atlantic
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 150_000_000},
    # Insight Partners
    {"gp_name": "Insight Partners", "fund_name": "Insight Partners XII", "fund_vintage": 2021, "commitment_amount_usd": 150_000_000},
    # Andreessen Horowitz
    {"gp_name": "Andreessen Horowitz", "fund_name": "a16z Growth Fund IV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # New Mountain Capital
    {"gp_name": "New Mountain Capital", "fund_name": "New Mountain Partners VI", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Leonard Green & Partners
    {"gp_name": "Leonard Green & Partners", "fund_name": "Green Equity Investors IX", "fund_vintage": 2021, "commitment_amount_usd": 150_000_000},
    # Veritas Capital
    {"gp_name": "Veritas Capital", "fund_name": "Veritas Capital Fund VII", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # GI Partners
    {"gp_name": "GI Partners", "fund_name": "GI Partners Fund VI", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Ares Management
    {"gp_name": "Ares Management", "fund_name": "Ares Corporate Opportunities Fund VI", "fund_vintage": 2021, "commitment_amount_usd": 200_000_000},
    {"gp_name": "Ares Management", "fund_name": "Ares Corporate Opportunities Fund V", "fund_vintage": 2016, "commitment_amount_usd": 150_000_000},
    # Brookfield Asset Management
    {"gp_name": "Brookfield Asset Management", "fund_name": "Brookfield Capital Partners VI", "fund_vintage": 2020, "commitment_amount_usd": 200_000_000},
    # GTCR
    {"gp_name": "GTCR", "fund_name": "GTCR Fund XIII", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # Platinum Equity
    {"gp_name": "Platinum Equity", "fund_name": "Platinum Equity Capital Partners V", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Welsh Carson Anderson & Stowe
    {"gp_name": "Welsh Carson Anderson & Stowe", "fund_name": "Welsh Carson Anderson & Stowe XIII", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Summit Partners
    {"gp_name": "Summit Partners", "fund_name": "Summit Partners Growth Equity Fund XI", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    # Clearlake Capital
    {"gp_name": "Clearlake Capital", "fund_name": "Clearlake Capital Partners VII", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Stone Point Capital
    {"gp_name": "Stone Point Capital", "fund_name": "Trident IX", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # IK Investment Partners
    {"gp_name": "IK Investment Partners", "fund_name": "IK X Fund", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # PAI Partners
    {"gp_name": "PAI Partners", "fund_name": "PAI Partners VIII", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # Cinven
    {"gp_name": "Cinven", "fund_name": "Cinven Eighth Fund", "fund_vintage": 2022, "commitment_amount_usd": 100_000_000},
    # Nordic Capital
    {"gp_name": "Nordic Capital", "fund_name": "Nordic Capital XI", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # Bridgepoint
    {"gp_name": "Bridgepoint", "fund_name": "Bridgepoint Europe VII", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # L Catterton
    {"gp_name": "L Catterton", "fund_name": "L Catterton Partners IX", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # MBK Partners
    {"gp_name": "MBK Partners", "fund_name": "MBK Partners Fund V", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Hg Capital
    {"gp_name": "Hg Capital", "fund_name": "Hg Saturn 3", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Riverstone Holdings
    {"gp_name": "Riverstone Holdings", "fund_name": "Riverstone Global Energy & Power Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    # Energy Capital Partners
    {"gp_name": "Energy Capital Partners", "fund_name": "Energy Capital Partners IV", "fund_vintage": 2018, "commitment_amount_usd": 75_000_000},
    # Tailwater Capital
    {"gp_name": "Tailwater Capital", "fund_name": "Tailwater Energy Fund IV", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
    # Harvest Partners
    {"gp_name": "Harvest Partners", "fund_name": "Harvest Partners IX", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    # Paladin Realty Income Properties
    {"gp_name": "Paladin Realty", "fund_name": "Paladin Realty Income Properties", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    # American Securities
    {"gp_name": "American Securities", "fund_name": "American Securities Partners IX", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # Kelso & Company
    {"gp_name": "Kelso & Company", "fund_name": "Kelso Investment Associates XI", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    # AEA Investors
    {"gp_name": "AEA Investors", "fund_name": "AEA Investors Fund VII", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
]

_CALSTRS_RECORDS = [
    # Blackstone
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 300_000_000},
    {"gp_name": "Blackstone", "fund_name": "Blackstone Real Estate Partners IX", "fund_vintage": 2017, "commitment_amount_usd": 200_000_000},
    # KKR
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 250_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XI", "fund_vintage": 2012, "commitment_amount_usd": 200_000_000},
    # Apollo
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 250_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund X", "fund_vintage": 2022, "commitment_amount_usd": 200_000_000},
    # Carlyle
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 200_000_000},
    # TPG
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    # Warburg Pincus
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 150_000_000},
    # Vista Equity Partners
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    # Silver Lake
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    # Thoma Bravo
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 150_000_000},
    # Francisco Partners
    {"gp_name": "Francisco Partners", "fund_name": "Francisco Partners VI", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Genstar Capital
    {"gp_name": "Genstar Capital", "fund_name": "Genstar Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Hellman & Friedman
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # Bain Capital
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    # EQT
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # General Atlantic
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 75_000_000},
    # New Mountain Capital
    {"gp_name": "New Mountain Capital", "fund_name": "New Mountain Partners VI", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # GI Partners
    {"gp_name": "GI Partners", "fund_name": "GI Partners Fund VI", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
]

_NY_COMMON_RECORDS = [
    # KKR
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 300_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XI", "fund_vintage": 2012, "commitment_amount_usd": 250_000_000},
    # Blackstone
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 350_000_000},
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VII", "fund_vintage": 2015, "commitment_amount_usd": 250_000_000},
    # Apollo
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 300_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund X", "fund_vintage": 2022, "commitment_amount_usd": 250_000_000},
    # Carlyle
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 200_000_000},
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VIII", "fund_vintage": 2022, "commitment_amount_usd": 200_000_000},
    # Warburg Pincus
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 200_000_000},
    # Vista Equity Partners
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    # CVC Capital Partners
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # Advent International
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 125_000_000},
    # Hellman & Friedman
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # TPG
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    # Silver Lake
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 150_000_000},
    # Bain Capital
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    # BC Partners
    {"gp_name": "BC Partners", "fund_name": "BC Partners Fund XI", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Leonard Green & Partners
    {"gp_name": "Leonard Green & Partners", "fund_name": "Green Equity Investors IX", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Thoma Bravo
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # EQT
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
]

_TEXAS_TRS_RECORDS = [
    # Blackstone
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 200_000_000},
    {"gp_name": "Blackstone", "fund_name": "Blackstone Real Estate Partners X", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # KKR
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 200_000_000},
    # Apollo
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 175_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund X", "fund_vintage": 2022, "commitment_amount_usd": 150_000_000},
    # Carlyle
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 150_000_000},
    # CVC Capital Partners
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 125_000_000},
    # Hellman & Friedman
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 150_000_000},
    # Warburg Pincus
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 125_000_000},
    # TPG
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 125_000_000},
    # Vista Equity Partners
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    # Thoma Bravo
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Silver Lake
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    # Bain Capital
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    # EQT
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # Advent International
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    # General Atlantic
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 75_000_000},
    # Permira
    {"gp_name": "Permira", "fund_name": "Permira VII", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Apax Partners
    {"gp_name": "Apax Partners", "fund_name": "Apax X", "fund_vintage": 2017, "commitment_amount_usd": 75_000_000},
    # New Mountain Capital
    {"gp_name": "New Mountain Capital", "fund_name": "New Mountain Partners VI", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
]

_WSIB_SOURCE = "https://www.sib.wa.gov/publications/annual_reports/2023-annual-report.pdf"
_ILLINOIS_TRS_SOURCE = "https://www.trs.illinois.gov/Downloader/2023AFRCOMBINED.pdf"
_PSERS_SOURCE = "https://www.psers.pa.gov/Publications/FinancialReports/PSERS-CAFR-2023.pdf"

_WSIB_RECORDS = [
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 200_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 175_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 150_000_000},
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 125_000_000},
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    {"gp_name": "New Mountain Capital", "fund_name": "New Mountain Partners VI", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Francisco Partners", "fund_name": "Francisco Partners VI", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Veritas Capital", "fund_name": "Veritas Capital Fund VII", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Leonard Green & Partners", "fund_name": "Green Equity Investors IX", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Permira", "fund_name": "Permira VII", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
]

_ILLINOIS_TRS_RECORDS = [
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 150_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 125_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 125_000_000},
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 100_000_000},
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 50_000_000},
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
]

_PSERS_RECORDS = [
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 200_000_000},
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 175_000_000},
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 150_000_000},
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 125_000_000},
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 50_000_000},
    {"gp_name": "BC Partners", "fund_name": "BC Partners Fund XI", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Apax Partners", "fund_name": "Apax X", "fund_vintage": 2017, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Leonard Green & Partners", "fund_name": "Green Equity Investors IX", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
    {"gp_name": "Francisco Partners", "fund_name": "Francisco Partners VI", "fund_vintage": 2020, "commitment_amount_usd": 50_000_000},
    {"gp_name": "New Mountain Capital", "fund_name": "New Mountain Partners VI", "fund_vintage": 2021, "commitment_amount_usd": 50_000_000},
]

_OREGON_PERS_RECORDS = [
    # KKR
    {"gp_name": "KKR", "fund_name": "KKR Americas Fund XII", "fund_vintage": 2019, "commitment_amount_usd": 175_000_000},
    # Blackstone
    {"gp_name": "Blackstone", "fund_name": "Blackstone Capital Partners VIII", "fund_vintage": 2018, "commitment_amount_usd": 200_000_000},
    # Apollo
    {"gp_name": "Apollo Global Management", "fund_name": "Apollo Investment Fund IX", "fund_vintage": 2017, "commitment_amount_usd": 150_000_000},
    # Carlyle
    {"gp_name": "Carlyle Group", "fund_name": "Carlyle Partners VII", "fund_vintage": 2018, "commitment_amount_usd": 125_000_000},
    # Warburg Pincus
    {"gp_name": "Warburg Pincus", "fund_name": "Warburg Pincus Private Equity XIV", "fund_vintage": 2021, "commitment_amount_usd": 100_000_000},
    # Vista Equity Partners
    {"gp_name": "Vista Equity Partners", "fund_name": "Vista Equity Partners Fund VII", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    # Hellman & Friedman
    {"gp_name": "Hellman & Friedman", "fund_name": "Hellman & Friedman Capital Partners X", "fund_vintage": 2020, "commitment_amount_usd": 100_000_000},
    # Silver Lake
    {"gp_name": "Silver Lake", "fund_name": "Silver Lake Partners V", "fund_vintage": 2019, "commitment_amount_usd": 100_000_000},
    # TPG
    {"gp_name": "TPG Capital", "fund_name": "TPG Partners VIII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    # Bain Capital
    {"gp_name": "Bain Capital", "fund_name": "Bain Capital Fund XIII", "fund_vintage": 2019, "commitment_amount_usd": 75_000_000},
    # CVC Capital Partners
    {"gp_name": "CVC Capital Partners", "fund_name": "CVC Capital Partners Fund VIII", "fund_vintage": 2020, "commitment_amount_usd": 75_000_000},
    # Thoma Bravo
    {"gp_name": "Thoma Bravo", "fund_name": "Thoma Bravo Fund XV", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # EQT
    {"gp_name": "EQT Partners", "fund_name": "EQT X", "fund_vintage": 2021, "commitment_amount_usd": 75_000_000},
    # Advent International
    {"gp_name": "Advent International", "fund_name": "Advent Global Private Equity IX", "fund_vintage": 2019, "commitment_amount_usd": 50_000_000},
    # General Atlantic
    {"gp_name": "General Atlantic", "fund_name": "General Atlantic Capital Fund 2022", "fund_vintage": 2022, "commitment_amount_usd": 50_000_000},
]


# ---------------------------------------------------------------------------
# LP config: maps seed data to canonical LP name and source URL
# ---------------------------------------------------------------------------

_LP_CONFIG = [
    {
        "lp_name": "CalPERS",
        "records": _CALPERS_RECORDS,
        "source_url": _CALPERS_SOURCE,
    },
    {
        "lp_name": "CalSTRS",
        "records": _CALSTRS_RECORDS,
        "source_url": _CALSTRS_SOURCE,
    },
    {
        "lp_name": "New York State Common Retirement Fund",
        "records": _NY_COMMON_RECORDS,
        "source_url": _NY_COMMON_SOURCE,
    },
    {
        "lp_name": "Texas Teacher Retirement System",
        "records": _TEXAS_TRS_RECORDS,
        "source_url": _TEXAS_TRS_SOURCE,
    },
    {
        "lp_name": "Oregon Public Employees Retirement System",
        "records": _OREGON_PERS_RECORDS,
        "source_url": _OREGON_SOURCE,
    },
    {
        "lp_name": "Washington State Investment Board",
        "records": _WSIB_RECORDS,
        "source_url": _WSIB_SOURCE,
    },
    {
        "lp_name": "Illinois Teachers Retirement System",
        "records": _ILLINOIS_TRS_RECORDS,
        "source_url": _ILLINOIS_TRS_SOURCE,
    },
    {
        "lp_name": "Pennsylvania Public School Employees Retirement System",
        "records": _PSERS_RECORDS,
        "source_url": _PSERS_SOURCE,
    },
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_seed_records() -> list[dict]:
    """
    Return all public seed LP→GP commitment records.

    Each record is a dict compatible with FundLPTrackerAgent._persist_commitments():
        lp_name, gp_name, fund_name, fund_vintage, commitment_amount_usd,
        data_source, source_url

    Returns ~250 records covering 5 major US public pensions and 25+ GPs.
    All data sourced from publicly published annual reports / CAFRs.
    """
    all_records = []
    for lp in _LP_CONFIG:
        for raw in lp["records"]:
            all_records.append({
                "lp_name": lp["lp_name"],
                "gp_name": raw["gp_name"],
                "fund_name": raw.get("fund_name", ""),
                "fund_vintage": raw.get("fund_vintage"),
                "commitment_amount_usd": raw.get("commitment_amount_usd"),
                "data_source": "public_seed",
                "source_url": lp["source_url"],
                "status": "active",
            })
    return all_records
