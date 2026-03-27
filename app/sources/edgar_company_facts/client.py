"""
SEC EDGAR Company Facts Client.

Fetches XBRL-tagged financial data for public companies from SEC EDGAR.
Free API, no key required. Rate limit: ~10 req/sec.

URL: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json
Returns: all reported financial facts (revenues, net income, EPS, etc.)
"""
import asyncio
import logging
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

EDGAR_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
    "Accept": "application/json",
}

# Target companies: causal chain anchors for macro cascade graph
TARGET_COMPANIES = [
    {"ticker": "SHW", "cik": 89089,   "name": "Sherwin-Williams Co",    "sector": "housing"},
    {"ticker": "DHI", "cik": 45012,   "name": "D.R. Horton Inc",        "sector": "housing"},
    {"ticker": "LEN", "cik": 720005,  "name": "Lennar Corp",            "sector": "housing"},
    {"ticker": "HD",  "cik": 354950,  "name": "Home Depot Inc",         "sector": "housing"},
    {"ticker": "LOW", "cik": 60667,   "name": "Lowes Companies Inc",    "sector": "housing"},
    {"ticker": "XOM", "cik": 34088,   "name": "Exxon Mobil Corp",       "sector": "energy"},
]

# XBRL concept names for key financial metrics
# These are the US-GAAP taxonomy names used in SEC filings
REVENUE_CONCEPTS = [
    "us-gaap/Revenues",
    "us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax",
    "us-gaap/SalesRevenueNet",
    "us-gaap/SalesRevenueGoodsNet",
]
NET_INCOME_CONCEPTS = [
    "us-gaap/NetIncomeLoss",
    "us-gaap/ProfitLoss",
]
GROSS_PROFIT_CONCEPTS = [
    "us-gaap/GrossProfit",
]
EPS_CONCEPTS = [
    "us-gaap/EarningsPerShareBasic",
    "us-gaap/EarningsPerShareDiluted",
]


class EDGARCompanyFactsClient:
    """Fetches quarterly financial data from SEC EDGAR Company Facts API."""

    def __init__(self):
        self._semaphore = asyncio.Semaphore(3)  # max 3 concurrent SEC requests

    async def _get_json(self, url: str) -> Optional[dict]:
        """Fetch JSON from SEC EDGAR with rate limiting."""
        async with self._semaphore:
            await asyncio.sleep(0.15)  # ~7 req/sec, well within SEC limits
            try:
                async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        return resp.json()
                    elif resp.status_code == 404:
                        logger.warning(f"Company not found: {url}")
                        return None
                    else:
                        logger.error(f"EDGAR HTTP {resp.status_code}: {url}")
                        return None
            except Exception as e:
                logger.error(f"EDGAR request error: {e}")
                return None

    def _extract_quarterly_facts(
        self, facts_json: dict, concept_candidates: list[str]
    ) -> list[dict]:
        """
        Extract quarterly observations for the first matching concept.
        Returns list of {end_date, value, accn, form, filed, frame} dicts.
        """
        us_gaap = facts_json.get("facts", {}).get("us-gaap", {})

        for concept_path in concept_candidates:
            # concept_path like "us-gaap/Revenues" → key "Revenues"
            concept_key = concept_path.split("/")[-1]
            concept_data = us_gaap.get(concept_key)
            if not concept_data:
                continue

            units = concept_data.get("units", {})
            # Revenue/income in USD, EPS in USD/shares
            observations = units.get("USD") or units.get("USD/shares") or []

            # Filter to 10-Q and 10-K quarterly/annual filings only
            quarterly = [
                obs for obs in observations
                if obs.get("form") in ("10-Q", "10-K")
                and obs.get("end")  # has end date
                and obs.get("val") is not None
            ]

            if quarterly:
                return quarterly, concept_key

        return [], None

    async def fetch_company_financials(self, company: dict) -> list[dict]:
        """
        Fetch quarterly financials for one company.
        Returns list of records ready for DB insertion.
        """
        cik = company["cik"]
        ticker = company["ticker"]
        name = company["name"]

        url = EDGAR_COMPANY_FACTS_URL.format(cik=cik)
        facts_json = await self._get_json(url)
        if not facts_json:
            return []

        records = []

        # Extract revenue
        revenue_obs, rev_concept = self._extract_quarterly_facts(facts_json, REVENUE_CONCEPTS)
        # Extract net income
        ni_obs, ni_concept = self._extract_quarterly_facts(facts_json, NET_INCOME_CONCEPTS)
        # Extract gross profit
        gp_obs, gp_concept = self._extract_quarterly_facts(facts_json, GROSS_PROFIT_CONCEPTS)
        # Extract EPS
        eps_obs, eps_concept = self._extract_quarterly_facts(facts_json, EPS_CONCEPTS)

        # Build lookup dicts by (end_date, form) for joining
        def to_lookup(obs_list):
            lookup = {}
            for obs in obs_list:
                key = (obs["end"], obs.get("form", ""))
                # Prefer most recently filed if duplicate
                if key not in lookup or obs.get("filed", "") > lookup[key].get("filed", ""):
                    lookup[key] = obs
            return lookup

        rev_lookup = to_lookup(revenue_obs)
        ni_lookup = to_lookup(ni_obs)
        gp_lookup = to_lookup(gp_obs)
        eps_lookup = to_lookup(eps_obs)

        # Union of all end dates
        all_keys = set(rev_lookup) | set(ni_lookup) | set(gp_lookup)

        for (end_date, form) in sorted(all_keys):
            rev = rev_lookup.get((end_date, form))
            ni = ni_lookup.get((end_date, form))
            gp = gp_lookup.get((end_date, form))
            eps = eps_lookup.get((end_date, form))

            # Determine fiscal period label from form + end date
            fiscal_period = "FY" if form == "10-K" else _quarter_from_date(end_date)

            record = {
                "ticker": ticker,
                "cik": str(cik),
                "company_name": name,
                "period_end_date": end_date,
                "fiscal_period": fiscal_period,
                "revenue_usd": rev["val"] if rev else None,
                "gross_profit_usd": gp["val"] if gp else None,
                "net_income_usd": ni["val"] if ni else None,
                "ebitda_usd": None,  # Not directly available from XBRL without calculation
                "eps_basic": eps["val"] if eps else None,
                "data_source": "edgar_xbrl",
                "filed_at": rev["filed"] if rev else (ni["filed"] if ni else None),
            }
            records.append(record)

        logger.info(f"  {ticker}: {len(records)} quarterly records extracted")
        return records

    async def fetch_all(self) -> list[dict]:
        """Fetch financials for all target companies."""
        tasks = [self.fetch_company_financials(co) for co in TARGET_COMPANIES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_records = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error for {TARGET_COMPANIES[i]['ticker']}: {result}")
            else:
                all_records.extend(result)

        logger.info(f"Total EDGAR records fetched: {len(all_records)}")
        return all_records


def _quarter_from_date(date_str: str) -> str:
    """Convert end date to fiscal quarter label. e.g. '2024-03-31' → 'Q1'."""
    try:
        month = int(date_str[5:7])
        if month <= 3:
            return "Q1"
        elif month <= 6:
            return "Q2"
        elif month <= 9:
            return "Q3"
        else:
            return "Q4"
    except Exception:
        return "Q?"
