#!/usr/bin/env python3
"""
PE Firms Seed Script

Populates the database with top 100 PE/VC firms as seed data.
Run with: python scripts/seed_pe_firms.py

Options:
    --direct    Insert directly into database (requires database connection)
    --api       Use API endpoints (requires running API server)
"""

import os
import sys
import argparse
from datetime import datetime
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =============================================================================
# PE FIRM SEED DATA
# =============================================================================

PE_FIRM_SEEDS = [
    # Mega-cap PE
    {"name": "Blackstone", "website": "https://www.blackstone.com", "cik": "1393818", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 1000, "founded_year": 1985, "hq_city": "New York", "hq_state": "NY"},
    {"name": "KKR", "website": "https://www.kkr.com", "cik": "1404912", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 553, "founded_year": 1976, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Apollo Global Management", "website": "https://www.apollo.com", "cik": "1411494", "firm_type": "PE", "strategy": "Credit/PE", "aum_billions": 651, "founded_year": 1990, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Carlyle Group", "website": "https://www.carlyle.com", "cik": "1527166", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 426, "founded_year": 1987, "hq_city": "Washington", "hq_state": "DC"},
    {"name": "TPG", "website": "https://www.tpg.com", "cik": "1880661", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 224, "founded_year": 1992, "hq_city": "Fort Worth", "hq_state": "TX"},
    {"name": "Brookfield Asset Management", "website": "https://www.brookfield.com", "cik": "1001085", "firm_type": "Alternative", "strategy": "Infrastructure/Real Estate", "aum_billions": 925, "founded_year": 1899, "hq_city": "Toronto", "hq_country": "Canada"},
    {"name": "Ares Management", "website": "https://www.aresmgmt.com", "cik": "1571123", "firm_type": "Credit", "strategy": "Credit/PE", "aum_billions": 428, "founded_year": 1997, "hq_city": "Los Angeles", "hq_state": "CA"},

    # Large PE
    {"name": "Warburg Pincus", "website": "https://www.warburgpincus.com", "firm_type": "PE", "strategy": "Growth", "aum_billions": 83, "founded_year": 1966, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Advent International", "website": "https://www.adventinternational.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 91, "founded_year": 1984, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "Thoma Bravo", "website": "https://www.thomabravo.com", "firm_type": "PE", "strategy": "Software Buyout", "aum_billions": 131, "founded_year": 2008, "hq_city": "Chicago", "hq_state": "IL"},
    {"name": "Vista Equity Partners", "website": "https://www.vistaequitypartners.com", "firm_type": "PE", "strategy": "Enterprise Software", "aum_billions": 101, "founded_year": 2000, "hq_city": "Austin", "hq_state": "TX"},
    {"name": "Silver Lake", "website": "https://www.silverlake.com", "firm_type": "PE", "strategy": "Technology", "aum_billions": 102, "founded_year": 1999, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Hellman & Friedman", "website": "https://www.hf.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 90, "founded_year": 1984, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "General Atlantic", "website": "https://www.generalatlantic.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 84, "founded_year": 1980, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Bain Capital", "website": "https://www.baincapital.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 180, "founded_year": 1984, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "CVC Capital Partners", "website": "https://www.cvc.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 188, "founded_year": 1981, "hq_city": "London", "hq_country": "UK"},
    {"name": "EQT", "website": "https://www.eqtgroup.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 130, "founded_year": 1994, "hq_city": "Stockholm", "hq_country": "Sweden"},
    {"name": "Permira", "website": "https://www.permira.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 80, "founded_year": 1985, "hq_city": "London", "hq_country": "UK"},

    # Top VC
    {"name": "Andreessen Horowitz", "website": "https://a16z.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 35, "founded_year": 2009, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Sequoia Capital", "website": "https://www.sequoiacap.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 85, "founded_year": 1972, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Accel", "website": "https://www.accel.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 50, "founded_year": 1983, "hq_city": "Palo Alto", "hq_state": "CA"},
    {"name": "Benchmark", "website": "https://www.benchmark.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 10, "founded_year": 1995, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Greylock Partners", "website": "https://greylock.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 12, "founded_year": 1965, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Bessemer Venture Partners", "website": "https://www.bvp.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 20, "founded_year": 1911, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Lightspeed Venture Partners", "website": "https://lsvp.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 25, "founded_year": 2000, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "NEA", "website": "https://www.nea.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 25, "founded_year": 1977, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "General Catalyst", "website": "https://www.generalcatalyst.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 25, "founded_year": 2000, "hq_city": "Cambridge", "hq_state": "MA"},
    {"name": "Index Ventures", "website": "https://www.indexventures.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 20, "founded_year": 1996, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "Founders Fund", "website": "https://foundersfund.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 12, "founded_year": 2005, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "Khosla Ventures", "website": "https://www.khoslaventures.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 15, "founded_year": 2004, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Battery Ventures", "website": "https://www.battery.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 15, "founded_year": 1983, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "GGV Capital", "website": "https://www.ggvc.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 10, "founded_year": 2000, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Menlo Ventures", "website": "https://www.menlovc.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 6, "founded_year": 1976, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "IVP", "website": "https://www.ivp.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 10, "founded_year": 1980, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Spark Capital", "website": "https://www.sparkcapital.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 6, "founded_year": 2005, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "Union Square Ventures", "website": "https://www.usv.com", "firm_type": "VC", "strategy": "Venture", "aum_billions": 2, "founded_year": 2003, "hq_city": "New York", "hq_state": "NY"},
    {"name": "First Round Capital", "website": "https://firstround.com", "firm_type": "VC", "strategy": "Seed", "aum_billions": 3, "founded_year": 2004, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "Y Combinator", "website": "https://www.ycombinator.com", "firm_type": "Accelerator", "strategy": "Seed", "aum_billions": 5, "founded_year": 2005, "hq_city": "Mountain View", "hq_state": "CA"},

    # Growth Equity
    {"name": "Summit Partners", "website": "https://www.summitpartners.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 40, "founded_year": 1984, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "TA Associates", "website": "https://www.ta.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 50, "founded_year": 1968, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "Technology Crossover Ventures", "website": "https://www.tcv.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 25, "founded_year": 1995, "hq_city": "Menlo Park", "hq_state": "CA"},
    {"name": "Insight Partners", "website": "https://www.insightpartners.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 90, "founded_year": 1995, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Spectrum Equity", "website": "https://www.spectrumequity.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 10, "founded_year": 1994, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "JMI Equity", "website": "https://jmi.com", "firm_type": "Growth", "strategy": "Growth Equity", "aum_billions": 6, "founded_year": 1992, "hq_city": "Baltimore", "hq_state": "MD"},

    # Middle Market PE
    {"name": "GTCR", "website": "https://www.gtcr.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 35, "founded_year": 1980, "hq_city": "Chicago", "hq_state": "IL"},
    {"name": "Madison Dearborn Partners", "website": "https://www.mdcp.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 30, "founded_year": 1992, "hq_city": "Chicago", "hq_state": "IL"},
    {"name": "Genstar Capital", "website": "https://www.genstarcapital.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 40, "founded_year": 1988, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "Veritas Capital", "website": "https://www.veritascapital.com", "firm_type": "PE", "strategy": "Government/Defense", "aum_billions": 45, "founded_year": 1992, "hq_city": "New York", "hq_state": "NY"},
    {"name": "American Securities", "website": "https://www.american-securities.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 30, "founded_year": 1994, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Audax Group", "website": "https://www.audaxgroup.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 35, "founded_year": 1999, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "New Mountain Capital", "website": "https://www.newmountaincapital.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 45, "founded_year": 2000, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Platinum Equity", "website": "https://www.platinumequity.com", "firm_type": "PE", "strategy": "M&A/Carve-out", "aum_billions": 48, "founded_year": 1995, "hq_city": "Beverly Hills", "hq_state": "CA"},
    {"name": "Roark Capital Group", "website": "https://www.roarkcapital.com", "firm_type": "PE", "strategy": "Franchise/Multi-unit", "aum_billions": 40, "founded_year": 2001, "hq_city": "Atlanta", "hq_state": "GA"},
    {"name": "Stone Point Capital", "website": "https://www.stonepoint.com", "firm_type": "PE", "strategy": "Financial Services", "aum_billions": 45, "founded_year": 2005, "hq_city": "Greenwich", "hq_state": "CT"},
    {"name": "THL Partners", "website": "https://www.thl.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 22, "founded_year": 1974, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "Leonard Green & Partners", "website": "https://www.leonardgreen.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 50, "founded_year": 1989, "hq_city": "Los Angeles", "hq_state": "CA"},
    {"name": "Providence Equity Partners", "website": "https://www.provequity.com", "firm_type": "PE", "strategy": "Media/Telecom", "aum_billions": 45, "founded_year": 1989, "hq_city": "Providence", "hq_state": "RI"},
    {"name": "Welsh Carson Anderson & Stowe", "website": "https://www.wcas.com", "firm_type": "PE", "strategy": "Healthcare/Tech", "aum_billions": 35, "founded_year": 1979, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Francisco Partners", "website": "https://www.franciscopartners.com", "firm_type": "PE", "strategy": "Technology", "aum_billions": 45, "founded_year": 1999, "hq_city": "San Francisco", "hq_state": "CA"},

    # Credit/Distressed
    {"name": "Oaktree Capital", "website": "https://www.oaktreecapital.com", "firm_type": "Credit", "strategy": "Distressed", "aum_billions": 189, "founded_year": 1995, "hq_city": "Los Angeles", "hq_state": "CA"},
    {"name": "Angelo Gordon", "website": "https://www.angelogordon.com", "firm_type": "Credit", "strategy": "Distressed", "aum_billions": 55, "founded_year": 1988, "hq_city": "New York", "hq_state": "NY"},
    {"name": "HIG Capital", "website": "https://www.higcapital.com", "firm_type": "PE", "strategy": "Buyout/Turnaround", "aum_billions": 60, "founded_year": 1993, "hq_city": "Miami", "hq_state": "FL"},
    {"name": "Cerberus Capital", "website": "https://www.cerberuscapital.com", "firm_type": "PE", "strategy": "Distressed", "aum_billions": 60, "founded_year": 1992, "hq_city": "New York", "hq_state": "NY"},

    # Infrastructure
    {"name": "Global Infrastructure Partners", "website": "https://www.global-infra.com", "firm_type": "Infrastructure", "strategy": "Infrastructure", "aum_billions": 100, "founded_year": 2006, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Stonepeak Partners", "website": "https://www.stonepeakpartners.com", "firm_type": "Infrastructure", "strategy": "Infrastructure", "aum_billions": 70, "founded_year": 2011, "hq_city": "New York", "hq_state": "NY"},
    {"name": "I Squared Capital", "website": "https://www.isquaredcapital.com", "firm_type": "Infrastructure", "strategy": "Infrastructure", "aum_billions": 40, "founded_year": 2012, "hq_city": "Miami", "hq_state": "FL"},
    {"name": "ArcLight Capital", "website": "https://www.arclightcapital.com", "firm_type": "Infrastructure", "strategy": "Energy Infrastructure", "aum_billions": 30, "founded_year": 2001, "hq_city": "Boston", "hq_state": "MA"},

    # Real Estate
    {"name": "Starwood Capital", "website": "https://www.starwoodcapital.com", "firm_type": "Real Estate", "strategy": "Real Estate", "aum_billions": 115, "founded_year": 1991, "hq_city": "Greenwich", "hq_state": "CT"},
    {"name": "Lone Star Funds", "website": "https://www.lonestarfunds.com", "firm_type": "Real Estate", "strategy": "Real Estate/Distressed", "aum_billions": 85, "founded_year": 1995, "hq_city": "Dallas", "hq_state": "TX"},

    # International
    {"name": "BC Partners", "website": "https://www.bcpartners.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 45, "founded_year": 1986, "hq_city": "London", "hq_country": "UK"},
    {"name": "PAI Partners", "website": "https://www.paipartners.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 25, "founded_year": 1998, "hq_city": "Paris", "hq_country": "France"},
    {"name": "Bridgepoint", "website": "https://www.bridgepoint.eu", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 55, "founded_year": 2000, "hq_city": "London", "hq_country": "UK"},
    {"name": "Nordic Capital", "website": "https://www.nordiccapital.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 25, "founded_year": 1989, "hq_city": "Stockholm", "hq_country": "Sweden"},
    {"name": "Cinven", "website": "https://www.cinven.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 40, "founded_year": 1977, "hq_city": "London", "hq_country": "UK"},
    {"name": "Apax Partners", "website": "https://www.apax.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 65, "founded_year": 1969, "hq_city": "London", "hq_country": "UK"},
    {"name": "Triton Partners", "website": "https://www.triton-partners.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 22, "founded_year": 1997, "hq_city": "Jersey", "hq_country": "Channel Islands"},
    {"name": "3i Group", "website": "https://www.3i.com", "cik": "916079", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 25, "founded_year": 1945, "hq_city": "London", "hq_country": "UK"},
    {"name": "Partners Group", "website": "https://www.partnersgroup.com", "firm_type": "PE", "strategy": "Multi-strategy", "aum_billions": 149, "founded_year": 1996, "hq_city": "Baar", "hq_country": "Switzerland"},
    {"name": "Hillhouse Capital", "website": "https://www.hillhousecap.com", "firm_type": "PE", "strategy": "Growth/PE", "aum_billions": 100, "founded_year": 2005, "hq_city": "Beijing", "hq_country": "China"},

    # Healthcare
    {"name": "Water Street Healthcare Partners", "website": "https://www.waterstreet.com", "firm_type": "PE", "strategy": "Healthcare", "aum_billions": 8, "founded_year": 2008, "hq_city": "Chicago", "hq_state": "IL"},
    {"name": "Frazier Healthcare Partners", "website": "https://www.frazierhealthcare.com", "firm_type": "PE", "strategy": "Healthcare", "aum_billions": 5, "founded_year": 1991, "hq_city": "Seattle", "hq_state": "WA"},
    {"name": "OrbiMed", "website": "https://www.orbimed.com", "firm_type": "VC", "strategy": "Healthcare/Life Sciences", "aum_billions": 20, "founded_year": 1989, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Deerfield Management", "website": "https://www.deerfield.com", "firm_type": "Healthcare", "strategy": "Healthcare", "aum_billions": 15, "founded_year": 1994, "hq_city": "New York", "hq_state": "NY"},

    # Tech-focused
    {"name": "Clearlake Capital", "website": "https://www.clearlake.com", "firm_type": "PE", "strategy": "Technology", "aum_billions": 80, "founded_year": 2006, "hq_city": "Santa Monica", "hq_state": "CA"},
    {"name": "Hg", "website": "https://www.hgcapital.com", "firm_type": "PE", "strategy": "Software", "aum_billions": 65, "founded_year": 2000, "hq_city": "London", "hq_country": "UK"},
    {"name": "Vector Capital", "website": "https://www.vectorcapital.com", "firm_type": "PE", "strategy": "Technology", "aum_billions": 4, "founded_year": 1997, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "Symphony Technology Group", "website": "https://www.symphonytg.com", "firm_type": "PE", "strategy": "Enterprise Software", "aum_billions": 10, "founded_year": 2002, "hq_city": "Palo Alto", "hq_state": "CA"},
    {"name": "Marlin Equity Partners", "website": "https://www.marlinequity.com", "firm_type": "PE", "strategy": "Technology", "aum_billions": 8, "founded_year": 2005, "hq_city": "Hermosa Beach", "hq_state": "CA"},

    # Lower Middle Market
    {"name": "Alpine Investors", "website": "https://www.alpineinvestors.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 17, "founded_year": 2001, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "The Riverside Company", "website": "https://www.riversidecompany.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 14, "founded_year": 1988, "hq_city": "Cleveland", "hq_state": "OH"},
    {"name": "Gryphon Investors", "website": "https://www.gryphoninvestors.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 9, "founded_year": 1997, "hq_city": "San Francisco", "hq_state": "CA"},
    {"name": "Wind Point Partners", "website": "https://www.windpointpartners.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 6, "founded_year": 1984, "hq_city": "Chicago", "hq_state": "IL"},
    {"name": "Parthenon Capital", "website": "https://www.parthenoncapital.com", "firm_type": "PE", "strategy": "Services", "aum_billions": 8, "founded_year": 1998, "hq_city": "Boston", "hq_state": "MA"},
    {"name": "Shore Capital Partners", "website": "https://www.shorecp.com", "firm_type": "PE", "strategy": "Healthcare Services", "aum_billions": 7, "founded_year": 2009, "hq_city": "Chicago", "hq_state": "IL"},
    {"name": "Clayton Dubilier & Rice", "website": "https://www.cdr-inc.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 50, "founded_year": 1978, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Kelso & Company", "website": "https://www.kelso.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 20, "founded_year": 1980, "hq_city": "New York", "hq_state": "NY"},
    {"name": "Montagu Private Equity", "website": "https://www.montagu.com", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 10, "founded_year": 2002, "hq_city": "London", "hq_country": "UK"},
    {"name": "Charterhouse", "website": "https://www.charterhouse.co.uk", "firm_type": "PE", "strategy": "Buyout", "aum_billions": 15, "founded_year": 1934, "hq_city": "London", "hq_country": "UK"},
]


def seed_via_api(base_url: str = "http://localhost:8001"):
    """Seed PE firms via API endpoints."""
    import requests

    print("\nSeeding PE firms via API...")

    success_count = 0
    error_count = 0

    for firm in PE_FIRM_SEEDS:
        try:
            payload = {
                "name": firm["name"],
                "website": firm["website"],
                "cik": firm.get("cik"),
                "firm_type": firm.get("firm_type", "PE"),
                "primary_strategy": firm.get("strategy"),
                "aum_usd_millions": firm.get("aum_billions", 0) * 1000 if firm.get("aum_billions") else None,
                "founded_year": firm.get("founded_year"),
                "headquarters_city": firm.get("hq_city"),
                "headquarters_state": firm.get("hq_state"),
                "headquarters_country": firm.get("hq_country", "USA") if not firm.get("hq_state") else "USA",
            }

            resp = requests.post(f"{base_url}/api/v1/pe/firms", json=payload, timeout=30)
            if resp.ok:
                print(f"  + {firm['name']}")
                success_count += 1
            else:
                print(f"  - {firm['name']}: {resp.status_code}")
                error_count += 1

        except Exception as e:
            print(f"  ! {firm['name']}: {e}")
            error_count += 1

    print(f"\nComplete: {success_count} inserted, {error_count} errors")


def seed_via_database():
    """Seed PE firms directly into database."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.core.pe_models import PEFirm
    from app.core.models import Base

    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/nexdata")

    print(f"\nConnecting to database...")
    engine = create_engine(database_url)

    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    print("\nSeeding PE firms directly to database...")

    success_count = 0
    error_count = 0
    skip_count = 0

    for firm_data in PE_FIRM_SEEDS:
        try:
            # Check if firm already exists
            existing = session.query(PEFirm).filter(PEFirm.name == firm_data["name"]).first()
            if existing:
                print(f"  ~ {firm_data['name']}: already exists")
                skip_count += 1
                continue

            firm = PEFirm(
                name=firm_data["name"],
                website=firm_data["website"],
                cik=firm_data.get("cik"),
                firm_type=firm_data.get("firm_type", "PE"),
                primary_strategy=firm_data.get("strategy"),
                aum_usd_millions=Decimal(str(firm_data.get("aum_billions", 0) * 1000)) if firm_data.get("aum_billions") else None,
                founded_year=firm_data.get("founded_year"),
                headquarters_city=firm_data.get("hq_city"),
                headquarters_state=firm_data.get("hq_state"),
                headquarters_country=firm_data.get("hq_country", "USA") if not firm_data.get("hq_state") else "USA",
                status="Active",
            )
            session.add(firm)
            print(f"  + {firm_data['name']}")
            success_count += 1

        except Exception as e:
            print(f"  ! {firm_data['name']}: {e}")
            error_count += 1

    try:
        session.commit()
        print(f"\nComplete: {success_count} inserted, {skip_count} skipped, {error_count} errors")
    except Exception as e:
        session.rollback()
        print(f"\nError committing: {e}")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Seed PE firms into the database")
    parser.add_argument("--direct", action="store_true", help="Insert directly into database")
    parser.add_argument("--api", action="store_true", help="Use API endpoints")
    parser.add_argument("--url", default="http://localhost:8001", help="API base URL")
    args = parser.parse_args()

    print("""
╔═══════════════════════════════════════════════════════════════╗
║                   PE FIRMS SEED SCRIPT                         ║
║       Populating database with top PE/VC firms...              ║
╚═══════════════════════════════════════════════════════════════╝
    """)

    print(f"Total firms to seed: {len(PE_FIRM_SEEDS)}")

    if args.api:
        seed_via_api(args.url)
    elif args.direct:
        seed_via_database()
    else:
        print("\nUsage:")
        print("  python scripts/seed_pe_firms.py --direct    # Direct database insert")
        print("  python scripts/seed_pe_firms.py --api       # Use API endpoints")
        print("\nDefaulting to direct database insert...")
        seed_via_database()


if __name__ == "__main__":
    main()
