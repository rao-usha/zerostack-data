#!/usr/bin/env python3
"""
Seed Apollo Global Management's known private portfolio companies.

These are major PE-backed companies from Apollo's 10-K, press releases, and
public deal announcements that won't appear in 13F filings (13F only covers
public equity positions). Sets current_pe_owner = 'Apollo Global Management'
so they appear in the investor profile report via Path 2 (UNION query).

Sources: Apollo 10-K (2024), press releases, SEC filings, public deal databases.

Usage:
    python scripts/seed_apollo_portfolio.py
    python scripts/seed_apollo_portfolio.py --dry-run
"""

import argparse
import io
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))


# Apollo's major private/PE-backed portfolio companies (from public sources)
# Excludes public equity positions that come from 13F filings
APOLLO_PORTFOLIO = [
    # Large-cap buyouts
    {
        "name": "ADT Inc.",
        "industry": "Security & Alarm Services",
        "sector": "Industrials",
        "headquarters_city": "Boca Raton",
        "headquarters_state": "FL",
        "headquarters_country": "US",
        "founded_year": 1874,
        "employee_count": 20000,
        "ownership_status": "Public",
        "ticker": "ADT",
        "website": "https://www.adt.com",
        "description": "Leading provider of security, automation, and smart home solutions in the US and Canada.",
    },
    {
        "name": "Yahoo",
        "industry": "Internet Content & Information",
        "sector": "Technology",
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "headquarters_country": "US",
        "founded_year": 1994,
        "employee_count": 8600,
        "ownership_status": "PE-Backed",
        "website": "https://www.yahoo.com",
        "description": "Global media and technology company. Acquired by Apollo from Verizon in 2021 for $5B.",
    },
    {
        "name": "The Venetian Resort",
        "industry": "Hotels, Resorts & Cruise Lines",
        "sector": "Consumer Discretionary",
        "headquarters_city": "Las Vegas",
        "headquarters_state": "NV",
        "headquarters_country": "US",
        "founded_year": 1999,
        "employee_count": 8000,
        "ownership_status": "PE-Backed",
        "website": "https://www.venetianlasvegas.com",
        "description": "Luxury resort and casino on the Las Vegas Strip. Acquired by Apollo and VICI Properties from Las Vegas Sands for $6.25B in 2021.",
    },
    {
        "name": "Shutterfly",
        "industry": "Specialty Retail",
        "sector": "Consumer Discretionary",
        "headquarters_city": "San Jose",
        "headquarters_state": "CA",
        "headquarters_country": "US",
        "founded_year": 1999,
        "employee_count": 4000,
        "ownership_status": "PE-Backed",
        "website": "https://www.shutterfly.com",
        "description": "Online retailer of personalized products and photo printing services. Taken private by Apollo in 2019 for $2.7B.",
    },
    {
        "name": "Cox Media Group",
        "industry": "Broadcasting",
        "sector": "Communication Services",
        "headquarters_city": "Atlanta",
        "headquarters_state": "GA",
        "headquarters_country": "US",
        "founded_year": 2008,
        "employee_count": 3500,
        "ownership_status": "PE-Backed",
        "website": "https://www.coxmediagroup.com",
        "description": "Media company operating TV stations, radio stations, and digital media properties. Acquired by Apollo in 2019.",
    },
    {
        "name": "Rackspace Technology",
        "industry": "IT Services",
        "sector": "Technology",
        "headquarters_city": "San Antonio",
        "headquarters_state": "TX",
        "headquarters_country": "US",
        "founded_year": 1998,
        "employee_count": 10000,
        "ownership_status": "PE-Backed",
        "website": "https://www.rackspace.com",
        "description": "Multicloud technology services company. Taken private by Apollo in 2016 for $4.3B; briefly relisted then delisted.",
    },
    {
        "name": "Sun Country Airlines",
        "industry": "Airlines",
        "sector": "Industrials",
        "headquarters_city": "Minneapolis",
        "headquarters_state": "MN",
        "headquarters_country": "US",
        "founded_year": 1982,
        "employee_count": 2500,
        "ownership_status": "Public",
        "ticker": "SNCY",
        "website": "https://www.suncountry.com",
        "description": "Low-cost airline and cargo carrier. Acquired by Apollo in 2018, taken public in 2021.",
    },
    {
        "name": "CareerBuilder",
        "industry": "Human Resource & Employment Services",
        "sector": "Industrials",
        "headquarters_city": "Chicago",
        "headquarters_state": "IL",
        "headquarters_country": "US",
        "founded_year": 1995,
        "employee_count": 1800,
        "ownership_status": "PE-Backed",
        "website": "https://www.careerbuilder.com",
        "description": "Online employment platform providing HR software, talent acquisition, and job board services. Acquired by Apollo in 2017.",
    },
    {
        "name": "Lumen Technologies",
        "industry": "Telecom Services",
        "sector": "Communication Services",
        "headquarters_city": "Monroe",
        "headquarters_state": "LA",
        "headquarters_country": "US",
        "founded_year": 1930,
        "employee_count": 27000,
        "ownership_status": "Public",
        "ticker": "LUMN",
        "website": "https://www.lumen.com",
        "description": "Global networking and enterprise tech company. Apollo invested in Lumen fiber assets in 2022.",
    },
    {
        "name": "Univision Communications",
        "industry": "Broadcasting",
        "sector": "Communication Services",
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "headquarters_country": "US",
        "founded_year": 1962,
        "employee_count": 4000,
        "ownership_status": "PE-Backed",
        "website": "https://www.univision.com",
        "description": "Leading Spanish-language media company in the US. Apollo is a major investor alongside other sponsors.",
    },
    {
        "name": "Athene Holding",
        "industry": "Life & Health Insurance",
        "sector": "Financials",
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "headquarters_country": "US",
        "founded_year": 2009,
        "employee_count": 2100,
        "ownership_status": "Subsidiary",
        "website": "https://www.athene.com",
        "description": "Retirement services company managing $300B+ in assets. Co-founded by Apollo, merged into Apollo Global Management in 2022.",
    },
    {
        "name": "ClubCorp Holdings",
        "industry": "Leisure Facilities",
        "sector": "Consumer Discretionary",
        "headquarters_city": "Dallas",
        "headquarters_state": "TX",
        "headquarters_country": "US",
        "founded_year": 1957,
        "employee_count": 18000,
        "ownership_status": "PE-Backed",
        "website": "https://www.clubcorp.com",
        "description": "Largest owner-operator of private clubs in the US with 200+ golf and country clubs. Taken private by Apollo in 2017.",
    },
    {
        "name": "Vericast",
        "industry": "Advertising",
        "sector": "Communication Services",
        "headquarters_city": "San Antonio",
        "headquarters_state": "TX",
        "headquarters_country": "US",
        "founded_year": 2020,
        "employee_count": 4500,
        "ownership_status": "PE-Backed",
        "website": "https://www.vericast.com",
        "description": "Marketing solutions company focused on direct mail, digital, and data-driven marketing. Apollo-backed since formation from Harland Clarke merger.",
    },
    {
        "name": "Constellation Automotive",
        "industry": "Specialty Retail",
        "sector": "Consumer Discretionary",
        "headquarters_city": "Leeds",
        "headquarters_state": "England",
        "headquarters_country": "UK",
        "founded_year": 2002,
        "employee_count": 5000,
        "ownership_status": "PE-Backed",
        "website": "https://www.constellationautomotive.com",
        "description": "UK's largest integrated online car marketplace (Cinch, BCA, WeBuyAnyCar). Backed by Apollo and TDR Capital.",
    },
    {
        "name": "Everi Holdings",
        "industry": "Gaming Equipment",
        "sector": "Consumer Discretionary",
        "headquarters_city": "Las Vegas",
        "headquarters_state": "NV",
        "headquarters_country": "US",
        "founded_year": 1998,
        "employee_count": 3000,
        "ownership_status": "PE-Backed",
        "website": "https://www.everi.com",
        "description": "Gaming technology company providing casino gaming machines, financial technology, and player loyalty solutions. Taken private by Apollo in 2024.",
    },
    {
        "name": "AP Moller - Maersk Logistics",
        "industry": "Marine Transportation",
        "sector": "Industrials",
        "headquarters_city": "Copenhagen",
        "headquarters_state": "",
        "headquarters_country": "Denmark",
        "founded_year": 1904,
        "employee_count": 5000,
        "ownership_status": "PE-Backed",
        "description": "Apollo co-invested in Maersk's logistics assets as part of the supply chain/logistics carve-out.",
    },
    {
        "name": "Aspen Insurance Holdings",
        "industry": "Property & Casualty Insurance",
        "sector": "Financials",
        "headquarters_city": "Hamilton",
        "headquarters_state": "Bermuda",
        "headquarters_country": "BM",
        "founded_year": 2002,
        "employee_count": 900,
        "ownership_status": "PE-Backed",
        "website": "https://www.aspen.co",
        "description": "Specialty insurance and reinsurance company. Acquired by Apollo in 2019 for $2.6B.",
    },
    {
        "name": "Catalina Marketing",
        "industry": "Advertising",
        "sector": "Communication Services",
        "headquarters_city": "St. Petersburg",
        "headquarters_state": "FL",
        "headquarters_country": "US",
        "founded_year": 1983,
        "employee_count": 1500,
        "ownership_status": "PE-Backed",
        "website": "https://www.catalina.com",
        "description": "Shopper intelligence and media company providing personalized digital media. Apollo-backed.",
    },
]


def seed_portfolio(db, firm_name="Apollo Global Management", dry_run=False):
    """Insert Apollo private portfolio companies into pe_portfolio_companies."""
    from sqlalchemy import text

    print(f"\nSeeding {len(APOLLO_PORTFOLIO)} portfolio companies for {firm_name}...")

    added = 0
    skipped = 0
    enriched = 0

    for company in APOLLO_PORTFOLIO:
        name = company["name"]

        # Check if company already exists (by name)
        r = db.execute(
            text("SELECT id, current_pe_owner FROM pe_portfolio_companies WHERE UPPER(name) = UPPER(:name)"),
            {"name": name},
        )
        existing = r.fetchone()

        if existing:
            company_id = existing[0]
            existing_owner = existing[1]

            if existing_owner and firm_name.lower() in existing_owner.lower():
                print(f"  [--] {name} (already exists with PE owner)")
                skipped += 1
                continue

            # Enrich existing record with PE owner and other fields
            if not dry_run:
                db.execute(
                    text("""
                        UPDATE pe_portfolio_companies SET
                            current_pe_owner = COALESCE(current_pe_owner, :pe_owner),
                            industry = COALESCE(industry, :industry),
                            sector = COALESCE(sector, :sector),
                            headquarters_city = COALESCE(headquarters_city, :hq_city),
                            headquarters_state = COALESCE(headquarters_state, :hq_state),
                            headquarters_country = COALESCE(headquarters_country, :hq_country),
                            founded_year = COALESCE(founded_year, :founded_year),
                            employee_count = COALESCE(employee_count, :employee_count),
                            ownership_status = COALESCE(ownership_status, :ownership_status),
                            website = COALESCE(website, :website),
                            description = COALESCE(description, :description),
                            ticker = COALESCE(ticker, :ticker),
                            updated_at = NOW()
                        WHERE id = :cid
                    """),
                    {
                        "cid": company_id,
                        "pe_owner": firm_name,
                        "industry": company.get("industry"),
                        "sector": company.get("sector"),
                        "hq_city": company.get("headquarters_city"),
                        "hq_state": company.get("headquarters_state"),
                        "hq_country": company.get("headquarters_country"),
                        "founded_year": company.get("founded_year"),
                        "employee_count": company.get("employee_count"),
                        "ownership_status": company.get("ownership_status"),
                        "website": company.get("website"),
                        "description": company.get("description"),
                        "ticker": company.get("ticker"),
                    },
                )
            enriched += 1
            print(f"  [++] {name} (enriched existing record)")
            continue

        if dry_run:
            print(f"  [OK] {name} — {company.get('industry', '?')} (dry run)")
            added += 1
            continue

        # Insert new portfolio company
        db.execute(
            text("""
                INSERT INTO pe_portfolio_companies (
                    name, current_pe_owner, industry, sector,
                    headquarters_city, headquarters_state, headquarters_country,
                    founded_year, employee_count, ownership_status,
                    website, description, ticker,
                    created_at, updated_at
                ) VALUES (
                    :name, :pe_owner, :industry, :sector,
                    :hq_city, :hq_state, :hq_country,
                    :founded_year, :employee_count, :ownership_status,
                    :website, :description, :ticker,
                    NOW(), NOW()
                )
            """),
            {
                "name": name,
                "pe_owner": firm_name,
                "industry": company.get("industry"),
                "sector": company.get("sector"),
                "hq_city": company.get("headquarters_city"),
                "hq_state": company.get("headquarters_state"),
                "hq_country": company.get("headquarters_country"),
                "founded_year": company.get("founded_year"),
                "employee_count": company.get("employee_count"),
                "ownership_status": company.get("ownership_status"),
                "website": company.get("website"),
                "description": company.get("description"),
                "ticker": company.get("ticker"),
            },
        )
        print(f"  [OK] {name} — {company.get('industry', '?')}")
        added += 1

    if not dry_run and (added > 0 or enriched > 0):
        db.commit()
    print(f"\nPortfolio: {added} added, {enriched} enriched, {skipped} skipped")
    return added + enriched


def main():
    parser = argparse.ArgumentParser(description="Seed Apollo private portfolio companies")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    from app.core.database import get_session_factory
    SessionFactory = get_session_factory()
    db = SessionFactory()

    try:
        seed_portfolio(db, dry_run=args.dry_run)
    finally:
        db.close()


if __name__ == "__main__":
    main()
