"""
Macro Sensitivity Agent — extracts company-macro linkages from SEC 10-K Risk Factors.

Uses heuristic keyword matching (primary) and optional LLM (fallback) to parse
the "Risk Factors" section of 10-K filings and identify which macroeconomic
factors each company discloses sensitivity to.

Creates CompanyMacroLinkage records automatically.

Flow:
    1. Fetch CIK submissions list from EDGAR data API
    2. Locate most recent 10-K primary document URL
    3. Download filing text (first 100K chars)
    4. Extract Risk Factors section via regex
    5. Run heuristic keyword scan → CompanyMacroLinkage records
    6. Persist linkages (skip duplicates)
"""

import asyncio
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
    "Accept-Encoding": "gzip, deflate",
}

# Mapping of plain-English keyword phrases → canonical FRED/BLS series_id.
# The cascade engine uses series_id to join back to MacroNode records.
MACRO_KEYWORD_MAP = {
    "interest rate": "DFF",
    "federal funds": "DFF",
    "mortgage rate": "MORTGAGE30US",
    "housing start": "HOUST",
    "housing market": "HOUST",
    "new home sale": "HSN1F",
    "home sale": "HSN1F",
    "home price": "CSUSHPINSA",
    "oil price": "DCOILWTICO",
    "crude oil": "DCOILWTICO",
    "energy cost": "DCOILWTICO",
    "consumer confidence": "UMCSENT",
    "consumer sentiment": "UMCSENT",
    "unemployment": "UNRATE",
    "inflation": "UMCSENT",
    "construction": "HOUST",
    "building permit": "PERMIT",
    "paint": "WPU0613",
    "coatings": "WPU0613",
    "lumber": "WPU132",
    "retail sale": "RSXFS",
}

# Words that indicate an adverse/negative linkage in the surrounding context
ADVERSE_WORDS = frozenset([
    "adverse", "adversely", "negatively", "decline", "declines", "declined",
    "declining", "reduce", "reduces", "reduced", "reducing", "lower", "lowers",
    "decrease", "decreases", "decreased", "hurt", "hurts", "harm", "harms",
    "impair", "impairs", "impaired", "risk", "risks", "volatile", "volatility",
])

# Target companies for sensitivity extraction
TARGET_COMPANIES = [
    {"ticker": "SHW", "cik": 89089,  "name": "Sherwin-Williams"},
    {"ticker": "DHI", "cik": 45012,  "name": "D.R. Horton"},
    {"ticker": "LEN", "cik": 720005, "name": "Lennar"},
    {"ticker": "HD",  "cik": 354950, "name": "Home Depot"},
    {"ticker": "LOW", "cik": 60667,  "name": "Lowe's"},
]

# LLM prompt (used only when self.llm is provided — not invoked by default)
LLM_PROMPT = """\
You are analyzing the Risk Factors section of a company's 10-K filing.

Extract all macroeconomic and market risk factors mentioned. For each one, return:
- factor_name: short name (e.g. "interest rates", "housing starts", "oil prices")
- direction: "positive" (factor rising helps company) or "negative" (factor rising hurts company)
- strength: "high", "medium", or "low" based on emphasis in the text
- quote: the most relevant 1-2 sentence excerpt from the filing

Return as a JSON array. Only include macroeconomic factors (interest rates, commodity \
prices, employment, GDP, housing, etc.) — not company-specific operational risks.

Text to analyze:
{risk_factors_text}"""


# =============================================================================
# AGENT
# =============================================================================


class MacroSensitivityAgent:
    """
    Extracts company-macro sensitivity linkages from SEC 10-K Risk Factor sections.

    Parameters:
        db_session: synchronous SQLAlchemy Session
        llm_client: optional LLMClient instance — if provided, LLM extraction is
                    used as a supplement to the heuristic pass; if None only
                    the keyword heuristic runs (no LLM cost incurred).
    """

    def __init__(self, db_session, llm_client=None):
        self.db = db_session
        self.llm = llm_client  # optional — not used in primary heuristic path

    # ------------------------------------------------------------------
    # EDGAR fetching
    # ------------------------------------------------------------------

    async def _fetch_latest_10k_text(self, cik: int, ticker: str) -> Optional[str]:
        """
        Fetch the primary document text for the most recent 10-K filing.

        Returns up to the first 100,000 characters of the filing, or None
        if no 10-K is found or a network error occurs.
        """
        try:
            async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
                # Step 1: fetch submissions metadata
                url = EDGAR_SUBMISSIONS_URL.format(cik=cik)
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.warning(
                        f"{ticker}: EDGAR submissions returned {resp.status_code} for CIK {cik}"
                    )
                    return None

                data = resp.json()
                filings = data.get("filings", {}).get("recent", {})
                forms = filings.get("form", [])
                docs = filings.get("primaryDocument", [])
                accns = filings.get("accessionNumber", [])

                # Step 2: find the most recent 10-K entry
                for i, form in enumerate(forms):
                    if form != "10-K":
                        continue
                    if i >= len(docs) or i >= len(accns):
                        break

                    accn_clean = accns[i].replace("-", "")
                    doc_filename = docs[i]
                    filing_url = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{cik}/{accn_clean}/{doc_filename}"
                    )

                    logger.debug(f"{ticker}: fetching 10-K from {filing_url}")
                    await asyncio.sleep(0.2)  # polite EDGAR rate limit

                    doc_resp = await client.get(filing_url)
                    if doc_resp.status_code == 200:
                        text = doc_resp.text
                        logger.info(
                            f"{ticker}: fetched 10-K ({len(text):,} chars) "
                            f"from accession {accns[i]}"
                        )
                        return text[:100_000]  # cap to first 100K chars

                    logger.warning(
                        f"{ticker}: primary doc returned {doc_resp.status_code}, "
                        "skipping this filing"
                    )
                    break  # only try the most recent 10-K

            logger.warning(f"{ticker}: no 10-K found in EDGAR submissions")
            return None

        except Exception as exc:
            logger.error(f"{ticker}: error fetching 10-K — {exc}")
            return None

    # ------------------------------------------------------------------
    # Section extraction
    # ------------------------------------------------------------------

    def _extract_risk_factors_section(self, text: str) -> str:
        """
        Extract the Risk Factors section (Item 1A) from raw 10-K text.

        Tries two regex patterns:
          1. Item 1A header → Item 1B header (strict)
          2. "Risk Factors" heading → next major section (loose)

        Falls back to collecting paragraphs containing economic keywords.
        Returns at most 20,000 characters.
        """
        patterns = [
            r'(?i)item\s+1a[\.\s]*risk\s+factors(.*?)item\s+1b',
            r'(?i)risk\s+factors(.*?)(?:item\s+2|quantitative\s+and\s+qualitative)',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.DOTALL)
            if m:
                section = m.group(1).strip()
                logger.debug(f"Risk factors section found via regex ({len(section):,} chars)")
                return section[:20_000]

        # Fallback: collect paragraphs mentioning economic terms
        economic_keywords = [
            "interest rate", "housing", "economic", "inflation", "oil",
            "unemployment", "consumer", "federal reserve",
        ]
        paragraphs = []
        total_len = 0
        for para in text.split("\n\n"):
            para_lower = para.lower()
            if any(kw in para_lower for kw in economic_keywords):
                paragraphs.append(para.strip())
                total_len += len(para)
                if total_len > 10_000:
                    break

        result = "\n\n".join(paragraphs)
        if result:
            logger.debug(
                f"Risk factors extracted via keyword fallback ({len(result):,} chars)"
            )
        return result

    # ------------------------------------------------------------------
    # Heuristic linkage extraction
    # ------------------------------------------------------------------

    def _heuristic_linkages(self, text: str, ticker: str, company_name: str) -> list[dict]:
        """
        Fast keyword-based macro linkage extraction — no LLM cost.

        For each keyword match:
        - Scans ±100/200 chars around the match for adverse sentiment words
        - Classifies direction as 'negative' (adverse context) or 'positive'
        - Captures a short evidence quote
        - Maps to the canonical series_id via MACRO_KEYWORD_MAP

        Deduplicates by series_id within the same ticker.
        """
        text_lower = text.lower()
        linkages: list[dict] = []
        seen_series: set[str] = set()  # prevent duplicate series per ticker

        for keyword, series_id in MACRO_KEYWORD_MAP.items():
            if series_id in seen_series:
                continue
            if keyword not in text_lower:
                continue

            idx = text_lower.find(keyword)
            # Context window for sentiment analysis
            context = text_lower[max(0, idx - 100): idx + 200]
            is_adverse = any(w in context for w in ADVERSE_WORDS)
            direction = "negative" if is_adverse else "positive"

            # Evidence quote (slightly wider window for readability)
            start = max(0, idx - 50)
            end = min(len(text), idx + 200)
            quote = text[start:end].strip().replace("\n", " ")

            linkages.append({
                "ticker": ticker,
                "company_name": company_name,
                "series_id": series_id,
                "direction": direction,
                "linkage_type": "risk_factor" if is_adverse else "revenue_driver",
                "linkage_strength": 0.6,
                "evidence_source": "sec_10k_risk_factors",
                "evidence_text": quote[:500],
            })
            seen_series.add(series_id)

        logger.debug(
            f"{ticker}: heuristic pass found {len(linkages)} candidate linkages"
        )
        return linkages

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist_linkages(self, linkages: list[dict]) -> int:
        """
        Persist CompanyMacroLinkage records, skipping duplicates.

        Looks up each MacroNode by series_id.  If the node is not seeded yet,
        the linkage is silently skipped (seed_causal_graph must run first).

        Returns count of new records created.
        """
        from sqlalchemy import select

        from app.core.macro_models import CompanyMacroLinkage, MacroNode

        persisted = 0
        for linkage in linkages:
            series_id = linkage.get("series_id")
            if not series_id:
                continue

            node = self.db.execute(
                select(MacroNode).where(MacroNode.series_id == series_id)
            ).scalar_one_or_none()

            if not node:
                logger.warning(
                    f"MacroNode not found for series_id={series_id!r} — "
                    "run seed_causal_graph first"
                )
                continue

            ticker = linkage.get("ticker")
            existing = self.db.execute(
                select(CompanyMacroLinkage).where(
                    CompanyMacroLinkage.ticker == ticker,
                    CompanyMacroLinkage.node_id == node.id,
                )
            ).scalar_one_or_none()

            if existing:
                continue  # idempotent — skip duplicates

            rec = CompanyMacroLinkage(
                ticker=ticker,
                company_name=linkage.get("company_name"),
                node_id=node.id,
                linkage_type=linkage.get("linkage_type", "risk_factor"),
                linkage_strength=linkage.get("linkage_strength", 0.5),
                direction=linkage["direction"],
                evidence_source=linkage.get("evidence_source", "sec_10k_risk_factors"),
                evidence_text=linkage.get("evidence_text"),
            )
            self.db.add(rec)
            persisted += 1

        self.db.commit()
        return persisted

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self, companies: Optional[list[dict]] = None) -> dict:
        """
        Run sensitivity extraction for all target companies (or a custom list).

        Parameters:
            companies: optional list of dicts with keys 'ticker', 'cik', 'name'.
                       Defaults to TARGET_COMPANIES if not provided.

        Returns:
            Summary dict: companies_processed, linkages_created, errors.
        """
        targets = companies if companies is not None else TARGET_COMPANIES
        summary: dict = {
            "companies_processed": 0,
            "linkages_created": 0,
            "errors": [],
        }

        for company in targets:
            ticker = company["ticker"]
            cik = company["cik"]
            name = company.get("name", ticker)

            try:
                logger.info(f"Processing {ticker} (CIK {cik})...")

                text = await self._fetch_latest_10k_text(cik=cik, ticker=ticker)
                if not text:
                    summary["errors"].append(f"{ticker}: could not fetch 10-K")
                    continue

                risk_text = self._extract_risk_factors_section(text)
                if not risk_text:
                    summary["errors"].append(f"{ticker}: no risk factors section found")
                    continue

                linkages = self._heuristic_linkages(
                    text=risk_text, ticker=ticker, company_name=name
                )
                created = self._persist_linkages(linkages)

                summary["companies_processed"] += 1
                summary["linkages_created"] += created
                logger.info(f"  {ticker}: {created} linkages created")

            except Exception as exc:
                logger.error(f"Error processing {ticker}: {exc}", exc_info=True)
                summary["errors"].append(f"{ticker}: {exc}")

            # Polite delay between companies (respects EDGAR rate limits)
            await asyncio.sleep(1.0)

        logger.info(
            f"MacroSensitivityAgent complete: "
            f"{summary['companies_processed']} companies, "
            f"{summary['linkages_created']} linkages, "
            f"{len(summary['errors'])} errors"
        )
        return summary
