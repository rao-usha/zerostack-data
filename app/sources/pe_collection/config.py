"""
Configuration for PE collection system.

Contains API keys, rate limits, and other settings.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class PECollectionSettings:
    """
    Global settings for PE collection.
    """

    # Rate limiting
    default_rate_limit_delay: float = 2.0  # seconds between requests
    sec_rate_limit_delay: float = 0.1  # SEC allows 10 req/sec
    linkedin_rate_limit_delay: float = 5.0  # LinkedIn is more restrictive
    news_rate_limit_delay: float = 1.0

    # Timeouts
    default_timeout: float = 30.0
    long_timeout: float = 120.0  # For large downloads

    # Retries
    default_max_retries: int = 3

    # Concurrency
    max_concurrent_firms: int = 5
    max_concurrent_companies: int = 10
    max_concurrent_people: int = 5

    # API Keys (loaded from environment)
    crunchbase_api_key: Optional[str] = None
    newsapi_key: Optional[str] = None
    linkedin_api_key: Optional[str] = None  # If using official API
    openai_api_key: Optional[str] = None  # For LLM-powered extraction

    # User Agents
    user_agent: str = "Nexdata-PE-Collector/1.0 (Data Research; contact@nexdata.io)"
    sec_user_agent: str = "Nexdata/1.0 (Data Research; contact@nexdata.io)"

    # Data Quality
    min_confidence_for_insert: str = "low"  # low, medium, high
    require_source_url: bool = True

    # Collection Behavior
    collect_full_text_news: bool = True
    extract_bios_with_llm: bool = True
    estimate_valuations: bool = True

    @classmethod
    def from_env(cls) -> "PECollectionSettings":
        """Load settings from environment variables."""
        return cls(
            crunchbase_api_key=os.getenv("CRUNCHBASE_API_KEY"),
            newsapi_key=os.getenv("NEWSAPI_KEY"),
            linkedin_api_key=os.getenv("LINKEDIN_API_KEY"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            default_rate_limit_delay=float(os.getenv("PE_RATE_LIMIT_DELAY", "2.0")),
            max_concurrent_firms=int(os.getenv("PE_MAX_CONCURRENT_FIRMS", "5")),
            max_concurrent_companies=int(
                os.getenv("PE_MAX_CONCURRENT_COMPANIES", "10")
            ),
        )


# Global settings instance
settings = PECollectionSettings.from_env()


# SEC EDGAR configuration
SEC_EDGAR_BASE_URL = "https://www.sec.gov"
SEC_EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
SEC_FULL_TEXT_SEARCH_URL = "https://efts.sec.gov/LATEST/search"
SEC_FILINGS_URL = "https://data.sec.gov/submissions"

# Common Form ADV Item mappings
FORM_ADV_ITEMS = {
    "1A": "Identifying Information",
    "1B": "Other Business Activities",
    "5": "Information About Your Advisory Business",
    "5D": "AUM",
    "5F": "Employees",
    "6": "Other Business Activities",
    "7": "Financial Industry Affiliations",
    "8": "Participation in Client Transactions",
}

# News API configuration
NEWSAPI_BASE_URL = "https://newsapi.org/v2"
GDELT_BASE_URL = "https://api.gdeltproject.org/api/v2"

# LinkedIn configuration (if using official API)
LINKEDIN_API_BASE = "https://api.linkedin.com/v2"

# Crunchbase configuration
CRUNCHBASE_API_BASE = "https://api.crunchbase.com/api/v4"

# Financial data sources
YAHOO_FINANCE_BASE = "https://query1.finance.yahoo.com/v8"
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"

# List of top PE/VC firms to seed the database
TOP_PE_FIRMS = [
    {
        "name": "Blackstone",
        "website": "https://www.blackstone.com",
        "cik": "1393818",
        "firm_type": "PE",
        "aum_billions": 1000,
    },
    {
        "name": "KKR",
        "website": "https://www.kkr.com",
        "cik": "1404912",
        "firm_type": "PE",
        "aum_billions": 553,
    },
    {
        "name": "Apollo Global Management",
        "website": "https://www.apollo.com",
        "cik": "1411494",
        "firm_type": "PE",
        "aum_billions": 651,
    },
    {
        "name": "Carlyle Group",
        "website": "https://www.carlyle.com",
        "cik": "1527166",
        "firm_type": "PE",
        "aum_billions": 426,
    },
    {
        "name": "TPG",
        "website": "https://www.tpg.com",
        "cik": "1880661",
        "firm_type": "PE",
        "aum_billions": 224,
    },
    {
        "name": "Warburg Pincus",
        "website": "https://www.warburgpincus.com",
        "firm_type": "PE",
        "aum_billions": 83,
    },
    {
        "name": "Advent International",
        "website": "https://www.adventinternational.com",
        "firm_type": "PE",
        "aum_billions": 91,
    },
    {
        "name": "Thoma Bravo",
        "website": "https://www.thomabravo.com",
        "firm_type": "PE",
        "aum_billions": 131,
    },
    {
        "name": "Vista Equity Partners",
        "website": "https://www.vistaequitypartners.com",
        "firm_type": "PE",
        "aum_billions": 101,
    },
    {
        "name": "Silver Lake",
        "website": "https://www.silverlake.com",
        "firm_type": "PE",
        "aum_billions": 102,
    },
    {
        "name": "Hellman & Friedman",
        "website": "https://www.hf.com",
        "firm_type": "PE",
        "aum_billions": 90,
    },
    {
        "name": "General Atlantic",
        "website": "https://www.generalatlantic.com",
        "firm_type": "Growth",
        "aum_billions": 84,
    },
    {
        "name": "Leonard Green & Partners",
        "website": "https://www.leonardgreen.com",
        "firm_type": "PE",
        "aum_billions": 50,
    },
    {
        "name": "Providence Equity Partners",
        "website": "https://www.provequity.com",
        "firm_type": "PE",
        "aum_billions": 45,
    },
    {
        "name": "Welsh Carson Anderson & Stowe",
        "website": "https://www.wcas.com",
        "firm_type": "PE",
        "aum_billions": 35,
    },
    {
        "name": "Insight Partners",
        "website": "https://www.insightpartners.com",
        "firm_type": "Growth",
        "aum_billions": 90,
    },
    {
        "name": "Francisco Partners",
        "website": "https://www.franciscopartners.com",
        "firm_type": "PE",
        "aum_billions": 45,
    },
    {
        "name": "Permira",
        "website": "https://www.permira.com",
        "firm_type": "PE",
        "aum_billions": 80,
    },
    {
        "name": "EQT",
        "website": "https://www.eqtgroup.com",
        "firm_type": "PE",
        "aum_billions": 130,
    },
    {
        "name": "CVC Capital Partners",
        "website": "https://www.cvc.com",
        "firm_type": "PE",
        "aum_billions": 188,
    },
    {
        "name": "Bain Capital",
        "website": "https://www.baincapital.com",
        "firm_type": "PE",
        "aum_billions": 180,
    },
    {
        "name": "Clayton Dubilier & Rice",
        "website": "https://www.cdr-inc.com",
        "firm_type": "PE",
        "aum_billions": 50,
    },
    {
        "name": "Apax Partners",
        "website": "https://www.apax.com",
        "firm_type": "PE",
        "aum_billions": 65,
    },
    {
        "name": "Cinven",
        "website": "https://www.cinven.com",
        "firm_type": "PE",
        "aum_billions": 40,
    },
    {
        "name": "Brookfield Asset Management",
        "website": "https://www.brookfield.com",
        "cik": "1001085",
        "firm_type": "Alternative",
        "aum_billions": 925,
    },
    # Top VC Firms
    {
        "name": "Andreessen Horowitz",
        "website": "https://a16z.com",
        "firm_type": "VC",
        "aum_billions": 35,
    },
    {
        "name": "Sequoia Capital",
        "website": "https://www.sequoiacap.com",
        "firm_type": "VC",
        "aum_billions": 85,
    },
    {
        "name": "Accel",
        "website": "https://www.accel.com",
        "firm_type": "VC",
        "aum_billions": 50,
    },
    {
        "name": "Benchmark",
        "website": "https://www.benchmark.com",
        "firm_type": "VC",
        "aum_billions": 10,
    },
    {
        "name": "Greylock Partners",
        "website": "https://greylock.com",
        "firm_type": "VC",
        "aum_billions": 12,
    },
    {
        "name": "Bessemer Venture Partners",
        "website": "https://www.bvp.com",
        "firm_type": "VC",
        "aum_billions": 20,
    },
    {
        "name": "Lightspeed Venture Partners",
        "website": "https://lsvp.com",
        "firm_type": "VC",
        "aum_billions": 25,
    },
    {
        "name": "NEA",
        "website": "https://www.nea.com",
        "firm_type": "VC",
        "aum_billions": 25,
    },
    {
        "name": "General Catalyst",
        "website": "https://www.generalcatalyst.com",
        "firm_type": "VC",
        "aum_billions": 25,
    },
    {
        "name": "Index Ventures",
        "website": "https://www.indexventures.com",
        "firm_type": "VC",
        "aum_billions": 20,
    },
    {
        "name": "Founders Fund",
        "website": "https://foundersfund.com",
        "firm_type": "VC",
        "aum_billions": 12,
    },
    {
        "name": "Khosla Ventures",
        "website": "https://www.khoslaventures.com",
        "firm_type": "VC",
        "aum_billions": 15,
    },
    {
        "name": "Battery Ventures",
        "website": "https://www.battery.com",
        "firm_type": "VC",
        "aum_billions": 15,
    },
    {
        "name": "GGV Capital",
        "website": "https://www.ggvc.com",
        "firm_type": "VC",
        "aum_billions": 10,
    },
    {
        "name": "Menlo Ventures",
        "website": "https://www.menlovc.com",
        "firm_type": "VC",
        "aum_billions": 6,
    },
    {
        "name": "IVP",
        "website": "https://www.ivp.com",
        "firm_type": "VC",
        "aum_billions": 10,
    },
    {
        "name": "Spark Capital",
        "website": "https://www.sparkcapital.com",
        "firm_type": "VC",
        "aum_billions": 6,
    },
    {
        "name": "Union Square Ventures",
        "website": "https://www.usv.com",
        "firm_type": "VC",
        "aum_billions": 2,
    },
    {
        "name": "First Round Capital",
        "website": "https://firstround.com",
        "firm_type": "VC",
        "aum_billions": 3,
    },
    {
        "name": "Y Combinator",
        "website": "https://www.ycombinator.com",
        "firm_type": "Accelerator",
        "aum_billions": 5,
    },
    # Growth Equity
    {
        "name": "Summit Partners",
        "website": "https://www.summitpartners.com",
        "firm_type": "Growth",
        "aum_billions": 40,
    },
    {
        "name": "TA Associates",
        "website": "https://www.ta.com",
        "firm_type": "Growth",
        "aum_billions": 50,
    },
    {
        "name": "Technology Crossover Ventures",
        "website": "https://www.tcv.com",
        "firm_type": "Growth",
        "aum_billions": 25,
    },
    {
        "name": "Spectrum Equity",
        "website": "https://www.spectrumequity.com",
        "firm_type": "Growth",
        "aum_billions": 10,
    },
    {
        "name": "JMI Equity",
        "website": "https://jmi.com",
        "firm_type": "Growth",
        "aum_billions": 6,
    },
    # Middle Market PE
    {
        "name": "GTCR",
        "website": "https://www.gtcr.com",
        "firm_type": "PE",
        "aum_billions": 35,
    },
    {
        "name": "Madison Dearborn Partners",
        "website": "https://www.mdcp.com",
        "firm_type": "PE",
        "aum_billions": 30,
    },
    {
        "name": "Genstar Capital",
        "website": "https://www.genstarcapital.com",
        "firm_type": "PE",
        "aum_billions": 40,
    },
    {
        "name": "Veritas Capital",
        "website": "https://www.veritascapital.com",
        "firm_type": "PE",
        "aum_billions": 45,
    },
    {
        "name": "American Securities",
        "website": "https://www.american-securities.com",
        "firm_type": "PE",
        "aum_billions": 30,
    },
    {
        "name": "Audax Group",
        "website": "https://www.audaxgroup.com",
        "firm_type": "PE",
        "aum_billions": 35,
    },
    {
        "name": "Kelso & Company",
        "website": "https://www.kelso.com",
        "firm_type": "PE",
        "aum_billions": 20,
    },
    {
        "name": "New Mountain Capital",
        "website": "https://www.newmountaincapital.com",
        "firm_type": "PE",
        "aum_billions": 45,
    },
    {
        "name": "Platinum Equity",
        "website": "https://www.platinumequity.com",
        "firm_type": "PE",
        "aum_billions": 48,
    },
    {
        "name": "Roark Capital Group",
        "website": "https://www.roarkcapital.com",
        "firm_type": "PE",
        "aum_billions": 40,
    },
    {
        "name": "Stone Point Capital",
        "website": "https://www.stonepoint.com",
        "firm_type": "PE",
        "aum_billions": 45,
    },
    {
        "name": "THL Partners",
        "website": "https://www.thl.com",
        "firm_type": "PE",
        "aum_billions": 22,
    },
    {
        "name": "Warburg Pincus",
        "website": "https://www.warburgpincus.com",
        "firm_type": "PE",
        "aum_billions": 83,
    },
    # Credit/Distressed
    {
        "name": "Ares Management",
        "website": "https://www.aresmgmt.com",
        "cik": "1571123",
        "firm_type": "Credit",
        "aum_billions": 428,
    },
    {
        "name": "Oaktree Capital",
        "website": "https://www.oaktreecapital.com",
        "firm_type": "Credit",
        "aum_billions": 189,
    },
    {
        "name": "Angelo Gordon",
        "website": "https://www.angelogordon.com",
        "firm_type": "Credit",
        "aum_billions": 55,
    },
    {
        "name": "HIG Capital",
        "website": "https://www.higcapital.com",
        "firm_type": "PE",
        "aum_billions": 60,
    },
    {
        "name": "Cerberus Capital",
        "website": "https://www.cerberuscapital.com",
        "firm_type": "PE",
        "aum_billions": 60,
    },
    # Infrastructure
    {
        "name": "Global Infrastructure Partners",
        "website": "https://www.global-infra.com",
        "firm_type": "Infrastructure",
        "aum_billions": 100,
    },
    {
        "name": "Stonepeak Partners",
        "website": "https://www.stonepeakpartners.com",
        "firm_type": "Infrastructure",
        "aum_billions": 70,
    },
    {
        "name": "I Squared Capital",
        "website": "https://www.isquaredcapital.com",
        "firm_type": "Infrastructure",
        "aum_billions": 40,
    },
    {
        "name": "ArcLight Capital",
        "website": "https://www.arclightcapital.com",
        "firm_type": "Infrastructure",
        "aum_billions": 30,
    },
    # Real Estate PE
    {
        "name": "Starwood Capital",
        "website": "https://www.starwoodcapital.com",
        "firm_type": "Real Estate",
        "aum_billions": 115,
    },
    {
        "name": "Colony Capital",
        "website": "https://www.colonyinc.com",
        "firm_type": "Real Estate",
        "aum_billions": 53,
    },
    {
        "name": "Lone Star Funds",
        "website": "https://www.lonestarfunds.com",
        "firm_type": "Real Estate",
        "aum_billions": 85,
    },
    # International PE
    {
        "name": "BC Partners",
        "website": "https://www.bcpartners.com",
        "firm_type": "PE",
        "aum_billions": 45,
    },
    {
        "name": "PAI Partners",
        "website": "https://www.paipartners.com",
        "firm_type": "PE",
        "aum_billions": 25,
    },
    {
        "name": "Bridgepoint",
        "website": "https://www.bridgepoint.eu",
        "firm_type": "PE",
        "aum_billions": 55,
    },
    {
        "name": "Nordic Capital",
        "website": "https://www.nordiccapital.com",
        "firm_type": "PE",
        "aum_billions": 25,
    },
    {
        "name": "Charterhouse",
        "website": "https://www.charterhouse.co.uk",
        "firm_type": "PE",
        "aum_billions": 15,
    },
    {
        "name": "Montagu Private Equity",
        "website": "https://www.montagu.com",
        "firm_type": "PE",
        "aum_billions": 10,
    },
    {
        "name": "Triton Partners",
        "website": "https://www.triton-partners.com",
        "firm_type": "PE",
        "aum_billions": 22,
    },
    {
        "name": "3i Group",
        "website": "https://www.3i.com",
        "cik": "916079",
        "firm_type": "PE",
        "aum_billions": 25,
    },
    {
        "name": "Partners Group",
        "website": "https://www.partnersgroup.com",
        "firm_type": "PE",
        "aum_billions": 149,
    },
    {
        "name": "Hillhouse Capital",
        "website": "https://www.hillhousecap.com",
        "firm_type": "PE",
        "aum_billions": 100,
    },
    # Healthcare PE
    {
        "name": "WCAS",
        "website": "https://www.wcas.com",
        "firm_type": "PE",
        "aum_billions": 35,
    },
    {
        "name": "Water Street Healthcare Partners",
        "website": "https://www.waterstreet.com",
        "firm_type": "PE",
        "aum_billions": 8,
    },
    {
        "name": "Frazier Healthcare Partners",
        "website": "https://www.frazierhealthcare.com",
        "firm_type": "PE",
        "aum_billions": 5,
    },
    {
        "name": "OrbiMed",
        "website": "https://www.orbimed.com",
        "firm_type": "VC",
        "aum_billions": 20,
    },
    {
        "name": "Deerfield Management",
        "website": "https://www.deerfield.com",
        "firm_type": "Healthcare",
        "aum_billions": 15,
    },
    # Tech-focused PE
    {
        "name": "Clearlake Capital",
        "website": "https://www.clearlake.com",
        "firm_type": "PE",
        "aum_billions": 80,
    },
    {
        "name": "Hg",
        "website": "https://www.hgcapital.com",
        "firm_type": "PE",
        "aum_billions": 65,
    },
    {
        "name": "Vector Capital",
        "website": "https://www.vectorcapital.com",
        "firm_type": "PE",
        "aum_billions": 4,
    },
    {
        "name": "Symphony Technology Group",
        "website": "https://www.symphonytg.com",
        "firm_type": "PE",
        "aum_billions": 10,
    },
    {
        "name": "JMI Equity",
        "website": "https://www.jmi.com",
        "firm_type": "Growth",
        "aum_billions": 6,
    },
    {
        "name": "Marlin Equity Partners",
        "website": "https://www.marlinequity.com",
        "firm_type": "PE",
        "aum_billions": 8,
    },
    # Lower Middle Market
    {
        "name": "Alpine Investors",
        "website": "https://www.alpineinvestors.com",
        "firm_type": "PE",
        "aum_billions": 17,
    },
    {
        "name": "The Riverside Company",
        "website": "https://www.riversidecompany.com",
        "firm_type": "PE",
        "aum_billions": 14,
    },
    {
        "name": "Gryphon Investors",
        "website": "https://www.gryphoninvestors.com",
        "firm_type": "PE",
        "aum_billions": 9,
    },
    {
        "name": "Wind Point Partners",
        "website": "https://www.windpointpartners.com",
        "firm_type": "PE",
        "aum_billions": 6,
    },
    {
        "name": "Parthenon Capital",
        "website": "https://www.parthenoncapital.com",
        "firm_type": "PE",
        "aum_billions": 8,
    },
    {
        "name": "Shore Capital Partners",
        "website": "https://www.shorecp.com",
        "firm_type": "PE",
        "aum_billions": 7,
    },
]
