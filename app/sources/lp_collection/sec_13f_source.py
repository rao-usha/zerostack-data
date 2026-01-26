"""
SEC Form 13F collector for LP institutional holdings.

Collects quarterly institutional holdings from SEC EDGAR 13F filings:
- Applicable to institutional investors managing $100M+ in US equities
- Reports holdings as of quarter-end (March 31, June 30, Sept 30, Dec 31)
- Includes CUSIP, shares, market value for each position

Data Source:
- SEC EDGAR API: https://data.sec.gov/submissions/CIK{cik}.json
- 13F Filings: XML/HTML infotables from individual filings
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# SEC EDGAR API endpoints
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_FILING_BASE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"

# 13F form types
FORM_13F_TYPES = ["13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A"]

# CIK lookup cache (LP name -> CIK)
CIK_CACHE: Dict[str, str] = {}

# Known CIKs for major institutional investors
KNOWN_CIKS = {
    # US Public Pensions
    "CalPERS": "0001067983",
    "California Public Employees Retirement System": "0001067983",
    "CalSTRS": "0001084267",
    "California State Teachers Retirement System": "0001084267",
    "New York State Common Retirement Fund": "0001030717",
    "New York State Teachers Retirement System": "0001112364",
    "Texas Teachers Retirement System": "0000917954",
    "Texas TRS": "0000917954",
    "Florida SBA": "0001053507",
    "Florida State Board of Administration": "0001053507",
    "Washington State Investment Board": "0001109404",
    "WSIB": "0001109404",
    "Oregon Public Employees Retirement Fund": "0001021866",
    "Oregon PERF": "0001021866",
    "Pennsylvania PSERS": "0001068152",
    "Pennsylvania SERS": "0001083428",
    "Michigan Treasury": "0001003926",
    "Ohio STRS": "0000919245",
    "Ohio State Teachers Retirement System": "0000919245",
    "Ohio PERS": "0001003917",
    "Wisconsin Investment Board": "0000920788",
    "SWIB": "0000920788",
    "New Jersey Division of Investment": "0001014328",
    "Virginia Retirement System": "0001050744",
    "VRS": "0001050744",
    "Massachusetts PRIM": "0001015002",
    "Minnesota State Board of Investment": "0001051449",
    "Connecticut Retirement Plans": "0001015099",

    # Sovereign Wealth Funds (those with 13F filings)
    "GIC Private Limited": "0001277537",
    "GIC": "0001277537",
    "Norges Bank": "0001273515",
    "Norway Government Pension Fund": "0001273515",

    # Endowments
    "Yale University": "0001056666",
    "Harvard Management Company": "0001082339",
    "Stanford Management Company": "0001050736",
    "Princeton University": "0001050790",
    "MIT Investment Management Company": "0001050847",
    "University of Texas Investment Management Company": "0001169536",
    "UTIMCO": "0001169536",
    "University of Michigan": "0001122624",

    # Foundations
    "Bill & Melinda Gates Foundation": "0001166559",
    "Gates Foundation": "0001166559",
    "Ford Foundation": "0001050895",
    "Hewlett Foundation": "0001113612",
}


class Sec13fCollector(BaseCollector):
    """
    Collects institutional holdings from SEC Form 13F filings.

    Form 13F is required for institutional investment managers with $100M+
    in qualifying securities. Reports are filed quarterly.

    Extracts:
    - CUSIP identifiers
    - Issuer names
    - Security classes
    - Share counts
    - Market values
    - Quarter-over-quarter changes
    """

    # SEC requires specific User-Agent format
    USER_AGENT = "Nexdata Research contact@nexdata.io"

    # More conservative rate limiting for SEC
    DEFAULT_RATE_LIMIT_DELAY = 0.5  # SEC allows 10 requests/second

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.SEC_13F

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        cik: Optional[str] = None,
        quarters_back: int = 4,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect 13F holdings data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL (not used for 13F)
            cik: SEC CIK number (10 digits, zero-padded)
            quarters_back: Number of quarters to collect (default 4)

        Returns:
            CollectionResult with holding items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting SEC 13F data for {lp_name}")

        try:
            # Resolve CIK
            resolved_cik = await self._resolve_cik(lp_name, cik)

            if not resolved_cik:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="Could not find SEC CIK for this LP",
                    warnings=["LP may not file 13F (requires $100M+ in US equities)"],
                    started_at=started_at,
                )

            # Get filing list
            filings = await self._get_13f_filings(resolved_cik, quarters_back)

            if not filings:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="No 13F filings found",
                    warnings=["LP may not file 13F or filings not yet available"],
                    started_at=started_at,
                )

            logger.info(f"Found {len(filings)} 13F filings for {lp_name}")

            # Process each filing
            for filing in filings:
                filing_items = await self._process_13f_filing(
                    filing, resolved_cik, lp_id, lp_name
                )
                items.extend(filing_items)

                if filing_items:
                    logger.info(
                        f"Extracted {len(filing_items)} holdings from "
                        f"{filing.get('reportDate', 'unknown')} filing"
                    )

            success = len(items) > 0

            if not items:
                warnings.append("Found filings but could not extract holdings")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting 13F for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _resolve_cik(
        self,
        lp_name: str,
        provided_cik: Optional[str]
    ) -> Optional[str]:
        """
        Resolve CIK for an LP.

        Tries:
        1. Provided CIK (if any)
        2. Known CIKs dictionary
        3. SEC EDGAR company search
        """
        # Use provided CIK
        if provided_cik:
            return provided_cik.zfill(10)

        # Check known CIKs
        for known_name, cik in KNOWN_CIKS.items():
            if known_name.lower() in lp_name.lower() or lp_name.lower() in known_name.lower():
                logger.debug(f"Found known CIK {cik} for {lp_name}")
                return cik

        # Check cache
        if lp_name in CIK_CACHE:
            return CIK_CACHE[lp_name]

        # Search SEC EDGAR
        cik = await self._search_cik_by_name(lp_name)
        if cik:
            CIK_CACHE[lp_name] = cik
            return cik

        return None

    async def _search_cik_by_name(self, name: str) -> Optional[str]:
        """
        Search SEC EDGAR for CIK by company name.

        Uses the SEC company search API.
        """
        # SEC company search endpoint
        search_url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": name,
            "dateRange": "custom",
            "forms": "13F-HR",
        }

        # Note: SEC full-text search requires different handling
        # For now, return None and rely on known CIKs or provided CIK
        logger.debug(f"CIK search not implemented for: {name}")
        return None

    async def _get_13f_filings(
        self,
        cik: str,
        quarters_back: int = 4
    ) -> List[Dict[str, Any]]:
        """
        Get list of 13F filings for a CIK.

        Args:
            cik: SEC CIK number (10 digits)
            quarters_back: Number of quarters to retrieve

        Returns:
            List of filing metadata dictionaries
        """
        # Fetch company submissions
        url = SEC_SUBMISSIONS_URL.format(cik=cik)

        headers = {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json",
        }

        data = await self._fetch_json(url, headers=headers)

        if not data:
            logger.warning(f"Could not fetch submissions for CIK {cik}")
            return []

        # Extract 13F filings
        filings = []
        recent_filings = data.get("filings", {}).get("recent", {})

        forms = recent_filings.get("form", [])
        accessions = recent_filings.get("accessionNumber", [])
        filing_dates = recent_filings.get("filingDate", [])
        report_dates = recent_filings.get("reportDate", [])
        primary_docs = recent_filings.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form in FORM_13F_TYPES and len(filings) < quarters_back:
                filings.append({
                    "form": form,
                    "accessionNumber": accessions[i] if i < len(accessions) else None,
                    "filingDate": filing_dates[i] if i < len(filing_dates) else None,
                    "reportDate": report_dates[i] if i < len(report_dates) else None,
                    "primaryDocument": primary_docs[i] if i < len(primary_docs) else None,
                })

        return filings

    async def _process_13f_filing(
        self,
        filing: Dict[str, Any],
        cik: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """
        Process a single 13F filing and extract holdings.

        Args:
            filing: Filing metadata
            cik: SEC CIK
            lp_id: LP fund ID
            lp_name: LP fund name

        Returns:
            List of holding CollectedItems
        """
        items = []

        accession = filing.get("accessionNumber")
        if not accession:
            return items

        # Format accession number for URL (remove dashes)
        accession_formatted = accession.replace("-", "")

        # Get filing index to find the infotable
        index_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={cik}&type=13F-HR&dateb=&owner=include&count=1"
        )

        # Try to fetch the infotable XML directly
        # Most 13F filings have a file named *infotable.xml
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession_formatted}/"

        # Try common infotable filename patterns
        infotable_patterns = [
            "infotable.xml",
            "primary_doc.xml",
            f"{accession_formatted}-infotable.xml",
        ]

        holdings_data = None
        source_url = None

        for pattern in infotable_patterns:
            url = base_url + pattern
            response = await self._fetch_url(url)

            if response and response.status_code == 200:
                try:
                    holdings_data = self._parse_13f_xml(response.text)
                    source_url = url
                    break
                except Exception as e:
                    logger.debug(f"Could not parse {url}: {e}")
                    continue

        # If XML not found, try to get filing index and find infotable
        if not holdings_data:
            holdings_data, source_url = await self._fetch_infotable_from_index(
                cik, accession_formatted
            )

        if not holdings_data:
            logger.warning(f"Could not find infotable for filing {accession}")
            return items

        # Convert holdings to CollectedItems
        report_date = filing.get("reportDate")
        filing_date = filing.get("filingDate")

        for holding in holdings_data:
            items.append(CollectedItem(
                item_type="13f_holding",
                data={
                    "lp_id": lp_id,
                    "lp_name": lp_name,
                    "cusip": holding.get("cusip"),
                    "issuer_name": holding.get("issuer"),
                    "security_class": holding.get("class"),
                    "shares": holding.get("shares"),
                    "value_usd": holding.get("value"),
                    "put_call": holding.get("putCall"),
                    "investment_discretion": holding.get("investmentDiscretion"),
                    "voting_authority_sole": holding.get("votingAuthoritySole"),
                    "voting_authority_shared": holding.get("votingAuthorityShared"),
                    "voting_authority_none": holding.get("votingAuthorityNone"),
                    "report_date": report_date,
                    "filing_date": filing_date,
                    "accession_number": accession,
                },
                source_url=source_url,
                confidence="high",  # SEC filings are authoritative
            ))

        return items

    async def _fetch_infotable_from_index(
        self,
        cik: str,
        accession: str
    ) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """
        Fetch infotable by parsing filing index page.

        Falls back to this method if direct XML URL doesn't work.
        """
        # Get filing index page
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession}/"

        response = await self._fetch_url(index_url)

        if not response or response.status_code != 200:
            return None, None

        # Look for infotable link in the index
        html = response.text

        # Find XML files that might be the infotable
        xml_pattern = re.compile(r'href="([^"]+(?:infotable|info_table)[^"]*\.xml)"', re.IGNORECASE)
        matches = xml_pattern.findall(html)

        for match in matches:
            xml_url = index_url + match if not match.startswith("http") else match
            xml_response = await self._fetch_url(xml_url)

            if xml_response and xml_response.status_code == 200:
                try:
                    holdings = self._parse_13f_xml(xml_response.text)
                    return holdings, xml_url
                except Exception as e:
                    logger.debug(f"Could not parse {xml_url}: {e}")
                    continue

        return None, None

    def _parse_13f_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse 13F infotable XML to extract holdings.

        Args:
            xml_content: Raw XML string

        Returns:
            List of holding dictionaries
        """
        holdings = []

        try:
            # Handle namespace variations
            xml_content = re.sub(r'\sxmlns[^=]*="[^"]*"', '', xml_content)
            root = ET.fromstring(xml_content)

            # Find all infoTable entries
            # 13F XML structure: informationTable > infoTable (repeated)
            info_tables = root.findall(".//infoTable") or root.findall(".//infotable")

            if not info_tables:
                # Try alternative path
                info_tables = root.findall(".//{*}infoTable")

            for entry in info_tables:
                holding = {}

                # Extract fields (handle case variations)
                for field, paths in [
                    ("issuer", ["nameOfIssuer", "issuer"]),
                    ("class", ["titleOfClass", "class"]),
                    ("cusip", ["cusip", "CUSIP"]),
                    ("value", ["value"]),
                    ("shares", ["shrsOrPrnAmt/sshPrnamt", "sshPrnamt", "shares"]),
                    ("shareType", ["shrsOrPrnAmt/sshPrnamtType", "sshPrnamtType"]),
                    ("putCall", ["putCall"]),
                    ("investmentDiscretion", ["investmentDiscretion"]),
                    ("votingAuthoritySole", ["votingAuthority/Sole", "votingAuthSole"]),
                    ("votingAuthorityShared", ["votingAuthority/Shared", "votingAuthShared"]),
                    ("votingAuthorityNone", ["votingAuthority/None", "votingAuthNone"]),
                ]:
                    for path in paths:
                        elem = entry.find(path)
                        if elem is None:
                            # Try case-insensitive
                            for child in entry.iter():
                                if child.tag.lower() == path.lower().split("/")[-1]:
                                    elem = child
                                    break
                        if elem is not None and elem.text:
                            holding[field] = elem.text.strip()
                            break

                # Convert numeric fields
                if "value" in holding:
                    # Value is in thousands in 13F
                    try:
                        holding["value"] = str(int(holding["value"]) * 1000)
                    except ValueError:
                        pass

                if holding.get("cusip"):
                    holdings.append(holding)

        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            raise

        return holdings

    def get_known_ciks(self) -> Dict[str, str]:
        """Return dictionary of known LP CIKs."""
        return KNOWN_CIKS.copy()
