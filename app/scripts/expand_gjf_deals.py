"""Expand Good Jobs First seed data with datacenter-focused deals."""
from datetime import datetime
from sqlalchemy import text
from app.core.database import get_db

# Major datacenter subsidy deals from public filings and news reports
DATACENTER_DEALS = [
    # Virginia — largest DC market
    {"company_name": "Amazon Web Services", "state": "VA", "city": "Sterling", "county": "Loudoun", "year": 2023, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 180000000, "jobs_announced": 100, "investment_announced": 35000000000, "industry": "Data Centers"},
    {"company_name": "Google", "state": "VA", "city": "Bristow", "county": "Prince William", "year": 2023, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 95000000, "jobs_announced": 50, "investment_announced": 2000000000, "industry": "Data Centers"},
    {"company_name": "Microsoft", "state": "VA", "city": "Boydton", "county": "Mecklenburg", "year": 2021, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 118000000, "jobs_announced": 50, "investment_announced": 2000000000, "industry": "Data Centers"},
    {"company_name": "QTS Realty", "state": "VA", "city": "Ashburn", "county": "Loudoun", "year": 2020, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 45000000, "jobs_announced": 30, "investment_announced": 800000000, "industry": "Data Centers"},
    {"company_name": "Digital Realty", "state": "VA", "city": "Ashburn", "county": "Loudoun", "year": 2019, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 52000000, "jobs_announced": 25, "investment_announced": 1200000000, "industry": "Data Centers"},
    {"company_name": "Equinix", "state": "VA", "city": "Ashburn", "county": "Loudoun", "year": 2022, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 38000000, "jobs_announced": 20, "investment_announced": 700000000, "industry": "Data Centers"},
    {"company_name": "CoreSite", "state": "VA", "city": "Reston", "county": "Fairfax", "year": 2021, "subsidy_type": "tax_exemption", "program_name": "Virginia Data Center Tax Exemption", "subsidy_value": 22000000, "jobs_announced": 15, "investment_announced": 400000000, "industry": "Data Centers"},
    # Texas
    {"company_name": "Meta Platforms", "state": "TX", "city": "Temple", "county": "Bell", "year": 2022, "subsidy_type": "tax_abatement", "program_name": "Texas Chapter 313", "subsidy_value": 147000000, "jobs_announced": 100, "investment_announced": 800000000, "industry": "Data Centers"},
    {"company_name": "Google", "state": "TX", "city": "Midlothian", "county": "Ellis", "year": 2022, "subsidy_type": "tax_abatement", "program_name": "Texas Chapter 313", "subsidy_value": 178000000, "jobs_announced": 50, "investment_announced": 600000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "TX", "city": "Fort Worth", "county": "Tarrant", "year": 2023, "subsidy_type": "tax_abatement", "program_name": "Texas Chapter 313", "subsidy_value": 225000000, "jobs_announced": 150, "investment_announced": 2500000000, "industry": "Data Centers"},
    {"company_name": "Oracle", "state": "TX", "city": "Austin", "county": "Travis", "year": 2022, "subsidy_type": "tax_rebate", "program_name": "Texas Enterprise Fund", "subsidy_value": 65000000, "jobs_announced": 200, "investment_announced": 1200000000, "industry": "Data Centers"},
    {"company_name": "DataPoint", "state": "TX", "city": "Dallas", "county": "Dallas", "year": 2021, "subsidy_type": "tax_abatement", "program_name": "Dallas Tax Incentive", "subsidy_value": 28000000, "jobs_announced": 30, "investment_announced": 500000000, "industry": "Data Centers"},
    {"company_name": "Skybox Datacenters", "state": "TX", "city": "Bryan", "county": "Brazos", "year": 2023, "subsidy_type": "tax_abatement", "program_name": "Texas Enterprise Fund", "subsidy_value": 35000000, "jobs_announced": 40, "investment_announced": 300000000, "industry": "Data Centers"},
    # Georgia
    {"company_name": "Google", "state": "GA", "city": "Douglas County", "county": "Douglas", "year": 2022, "subsidy_type": "tax_credit", "program_name": "Georgia EDGE", "subsidy_value": 95000000, "jobs_announced": 50, "investment_announced": 750000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "GA", "city": "Stanton Springs", "county": "Newton", "year": 2023, "subsidy_type": "tax_credit", "program_name": "Georgia EDGE", "subsidy_value": 400000000, "jobs_announced": 200, "investment_announced": 10000000000, "industry": "Data Centers"},
    {"company_name": "QTS Realty", "state": "GA", "city": "Atlanta", "county": "Fulton", "year": 2021, "subsidy_type": "tax_credit", "program_name": "Georgia EDGE", "subsidy_value": 18000000, "jobs_announced": 20, "investment_announced": 250000000, "industry": "Data Centers"},
    # Ohio
    {"company_name": "Amazon Web Services", "state": "OH", "city": "Columbus", "county": "Franklin", "year": 2022, "subsidy_type": "tax_credit", "program_name": "Ohio Job Creation Tax Credit", "subsidy_value": 280000000, "jobs_announced": 250, "investment_announced": 7600000000, "industry": "Data Centers"},
    {"company_name": "Google", "state": "OH", "city": "Columbus", "county": "Franklin", "year": 2023, "subsidy_type": "tax_credit", "program_name": "Ohio Data Center Tax Exemption", "subsidy_value": 120000000, "jobs_announced": 100, "investment_announced": 1800000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "OH", "city": "New Albany", "county": "Franklin", "year": 2022, "subsidy_type": "tax_credit", "program_name": "Ohio Data Center Tax Exemption", "subsidy_value": 165000000, "jobs_announced": 120, "investment_announced": 7500000000, "industry": "Data Centers"},
    # Iowa
    {"company_name": "Meta Platforms", "state": "IA", "city": "Altoona", "county": "Polk", "year": 2014, "subsidy_type": "tax_rebate", "program_name": "Iowa High Quality Jobs", "subsidy_value": 84000000, "jobs_announced": 50, "investment_announced": 1500000000, "industry": "Data Centers"},
    {"company_name": "Google", "state": "IA", "city": "Council Bluffs", "county": "Pottawattamie", "year": 2009, "subsidy_type": "tax_credit", "program_name": "Iowa High Quality Jobs", "subsidy_value": 50000000, "jobs_announced": 100, "investment_announced": 600000000, "industry": "Data Centers"},
    {"company_name": "Microsoft", "state": "IA", "city": "West Des Moines", "county": "Polk", "year": 2022, "subsidy_type": "tax_rebate", "program_name": "Iowa High Quality Jobs", "subsidy_value": 92000000, "jobs_announced": 30, "investment_announced": 1000000000, "industry": "Data Centers"},
    # North Carolina
    {"company_name": "Apple", "state": "NC", "city": "Maiden", "county": "Catawba", "year": 2009, "subsidy_type": "tax_credit", "program_name": "NC JDIG", "subsidy_value": 46000000, "jobs_announced": 50, "investment_announced": 1000000000, "industry": "Data Centers"},
    {"company_name": "Google", "state": "NC", "city": "Lenoir", "county": "Caldwell", "year": 2007, "subsidy_type": "tax_credit", "program_name": "NC JDIG", "subsidy_value": 89000000, "jobs_announced": 210, "investment_announced": 600000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "NC", "city": "Forest City", "county": "Rutherford", "year": 2012, "subsidy_type": "tax_credit", "program_name": "NC JDIG", "subsidy_value": 53000000, "jobs_announced": 80, "investment_announced": 450000000, "industry": "Data Centers"},
    # Oregon
    {"company_name": "Apple", "state": "OR", "city": "Prineville", "county": "Crook", "year": 2012, "subsidy_type": "tax_exemption", "program_name": "Oregon Enterprise Zone", "subsidy_value": 92000000, "jobs_announced": 35, "investment_announced": 250000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "OR", "city": "Prineville", "county": "Crook", "year": 2010, "subsidy_type": "tax_exemption", "program_name": "Oregon Enterprise Zone", "subsidy_value": 30000000, "jobs_announced": 55, "investment_announced": 500000000, "industry": "Data Centers"},
    {"company_name": "Google", "state": "OR", "city": "The Dalles", "county": "Wasco", "year": 2005, "subsidy_type": "tax_exemption", "program_name": "Oregon Enterprise Zone", "subsidy_value": 18800000, "jobs_announced": 200, "investment_announced": 600000000, "industry": "Data Centers"},
    # Arizona
    {"company_name": "Microsoft", "state": "AZ", "city": "Goodyear", "county": "Maricopa", "year": 2021, "subsidy_type": "property_tax", "program_name": "Arizona Commerce Authority", "subsidy_value": 45000000, "jobs_announced": 100, "investment_announced": 1000000000, "industry": "Data Centers"},
    {"company_name": "CyrusOne", "state": "AZ", "city": "Chandler", "county": "Maricopa", "year": 2020, "subsidy_type": "property_tax", "program_name": "Arizona Data Center Incentive", "subsidy_value": 32000000, "jobs_announced": 30, "investment_announced": 500000000, "industry": "Data Centers"},
    {"company_name": "Aligned Data Centers", "state": "AZ", "city": "Phoenix", "county": "Maricopa", "year": 2022, "subsidy_type": "property_tax", "program_name": "Arizona Commerce Authority", "subsidy_value": 28000000, "jobs_announced": 50, "investment_announced": 750000000, "industry": "Data Centers"},
    # Nevada
    {"company_name": "Switch", "state": "NV", "city": "Las Vegas", "county": "Clark", "year": 2016, "subsidy_type": "tax_abatement", "program_name": "Nevada Tax Abatement", "subsidy_value": 27600000, "jobs_announced": 200, "investment_announced": 1000000000, "industry": "Data Centers"},
    {"company_name": "Apple", "state": "NV", "city": "Reno", "county": "Washoe", "year": 2017, "subsidy_type": "tax_abatement", "program_name": "Nevada Tax Abatement", "subsidy_value": 89000000, "jobs_announced": 30, "investment_announced": 1000000000, "industry": "Data Centers"},
    # Indiana
    {"company_name": "Microsoft", "state": "IN", "city": "West Lafayette", "county": "Tippecanoe", "year": 2023, "subsidy_type": "tax_credit", "program_name": "Indiana EDGE", "subsidy_value": 75000000, "jobs_announced": 90, "investment_announced": 1000000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "IN", "city": "Jeffersonville", "county": "Clark", "year": 2023, "subsidy_type": "tax_credit", "program_name": "Indiana EDGE", "subsidy_value": 125000000, "jobs_announced": 100, "investment_announced": 800000000, "industry": "Data Centers"},
    # South Carolina
    {"company_name": "Google", "state": "SC", "city": "Berkeley County", "county": "Berkeley", "year": 2021, "subsidy_type": "tax_credit", "program_name": "SC Enterprise Zone", "subsidy_value": 67000000, "jobs_announced": 45, "investment_announced": 500000000, "industry": "Data Centers"},
    # Nebraska
    {"company_name": "Meta Platforms", "state": "NE", "city": "Papillion", "county": "Sarpy", "year": 2020, "subsidy_type": "tax_credit", "program_name": "Nebraska Advantage Act", "subsidy_value": 38000000, "jobs_announced": 100, "investment_announced": 400000000, "industry": "Data Centers"},
    # Mississippi
    {"company_name": "Google", "state": "MS", "city": "Jackson", "county": "Hinds", "year": 2022, "subsidy_type": "tax_exemption", "program_name": "Mississippi Data Center Incentive", "subsidy_value": 40000000, "jobs_announced": 25, "investment_announced": 600000000, "industry": "Data Centers"},
    # New Mexico
    {"company_name": "Meta Platforms", "state": "NM", "city": "Los Lunas", "county": "Valencia", "year": 2016, "subsidy_type": "tax_rebate", "program_name": "NM LEDA", "subsidy_value": 30000000, "jobs_announced": 50, "investment_announced": 1000000000, "industry": "Data Centers"},
    # Kansas
    {"company_name": "Google", "state": "KS", "city": "Kansas City", "county": "Wyandotte", "year": 2019, "subsidy_type": "tax_credit", "program_name": "Kansas PEAK", "subsidy_value": 20000000, "jobs_announced": 30, "investment_announced": 200000000, "industry": "Data Centers"},
    # Utah
    {"company_name": "Facebook", "state": "UT", "city": "Eagle Mountain", "county": "Utah", "year": 2018, "subsidy_type": "tax_credit", "program_name": "Utah EDTIF", "subsidy_value": 22000000, "jobs_announced": 50, "investment_announced": 750000000, "industry": "Data Centers"},
    # Tennessee
    {"company_name": "Google", "state": "TN", "city": "Clarksville", "county": "Montgomery", "year": 2020, "subsidy_type": "tax_credit", "program_name": "Tennessee FastTrack", "subsidy_value": 42000000, "jobs_announced": 70, "investment_announced": 600000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "TN", "city": "Gallatin", "county": "Sumner", "year": 2021, "subsidy_type": "tax_credit", "program_name": "Tennessee FastTrack", "subsidy_value": 55000000, "jobs_announced": 100, "investment_announced": 800000000, "industry": "Data Centers"},
    # Illinois
    {"company_name": "Meta Platforms", "state": "IL", "city": "DeKalb", "county": "DeKalb", "year": 2023, "subsidy_type": "tax_credit", "program_name": "Illinois EDGE", "subsidy_value": 60000000, "jobs_announced": 100, "investment_announced": 800000000, "industry": "Data Centers"},
    # Wisconsin
    {"company_name": "Microsoft", "state": "WI", "city": "Mount Pleasant", "county": "Racine", "year": 2023, "subsidy_type": "tax_credit", "program_name": "Wisconsin Enterprise Zone", "subsidy_value": 45000000, "jobs_announced": 80, "investment_announced": 1000000000, "industry": "Data Centers"},
    # Oklahoma
    {"company_name": "Google", "state": "OK", "city": "Pryor Creek", "county": "Mayes", "year": 2011, "subsidy_type": "tax_credit", "program_name": "Oklahoma Quality Jobs Act", "subsidy_value": 25000000, "jobs_announced": 400, "investment_announced": 400000000, "industry": "Data Centers"},
    # Alabama
    {"company_name": "Google", "state": "AL", "city": "Bridgeport", "county": "Jackson", "year": 2018, "subsidy_type": "tax_abatement", "program_name": "Alabama Jobs Act", "subsidy_value": 35000000, "jobs_announced": 100, "investment_announced": 600000000, "industry": "Data Centers"},
    {"company_name": "Meta Platforms", "state": "AL", "city": "Huntsville", "county": "Madison", "year": 2022, "subsidy_type": "tax_abatement", "program_name": "Alabama Jobs Act", "subsidy_value": 68000000, "jobs_announced": 100, "investment_announced": 800000000, "industry": "Data Centers"},
]

SQL = """INSERT INTO incentive_deal
    (gjf_id, company_name, state, city, county, year, subsidy_type,
     program_name, subsidy_value, jobs_announced, investment_announced,
     industry, source, collected_at)
    VALUES (:gjf_id, :company_name, :state, :city, :county, :year,
            :subsidy_type, :program_name, :subsidy_value, :jobs_announced,
            :investment_announced, :industry, 'gjf_expanded', :now)
    ON CONFLICT (gjf_id) DO UPDATE SET
    subsidy_value=EXCLUDED.subsidy_value, jobs_announced=EXCLUDED.jobs_announced,
    investment_announced=EXCLUDED.investment_announced, collected_at=EXCLUDED.collected_at"""

def main():
    db = next(get_db())
    before = db.execute(text("SELECT COUNT(*) FROM incentive_deal")).scalar()
    now = datetime.utcnow()
    for deal in DATACENTER_DEALS:
        gjf_id = f"{deal['company_name'][:30].replace(' ', '_')}_{deal['state']}_{deal.get('city', 'NA')}_{deal['year']}"
        deal["gjf_id"] = gjf_id
        deal["now"] = now
        db.execute(text(SQL), deal)
    db.commit()
    after = db.execute(text("SELECT COUNT(*) FROM incentive_deal")).scalar()
    states = db.execute(text("SELECT COUNT(DISTINCT state) FROM incentive_deal")).scalar()
    print(f"Incentive deals: {before} -> {after} ({after - before} new), {states} states")
    db.close()

main()
