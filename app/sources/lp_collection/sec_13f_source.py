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
from datetime import datetime
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
        self, lp_name: str, provided_cik: Optional[str]
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
            if (
                known_name.lower() in lp_name.lower()
                or lp_name.lower() in known_name.lower()
            ):
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

        # Note: SEC full-text search requires different handling
        # For now, return None and rely on known CIKs or provided CIK
        logger.debug(f"CIK search not implemented for: {name}")
        return None

    async def _get_13f_filings(
        self, cik: str, quarters_back: int = 4
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
                filings.append(
                    {
                        "form": form,
                        "accessionNumber": accessions[i]
                        if i < len(accessions)
                        else None,
                        "filingDate": filing_dates[i]
                        if i < len(filing_dates)
                        else None,
                        "reportDate": report_dates[i]
                        if i < len(report_dates)
                        else None,
                        "primaryDocument": primary_docs[i]
                        if i < len(primary_docs)
                        else None,
                    }
                )

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

        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession_formatted}/"

        holdings_data = None
        source_url = None

        # First, get the filing index to discover all XML files
        holdings_data, source_url = await self._fetch_infotable_from_index(
            cik, accession_formatted
        )

        # If not found via index, try common patterns as fallback
        if not holdings_data:
            infotable_patterns = [
                "infotable.xml",
                f"{accession_formatted}-infotable.xml",
            ]

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

        if not holdings_data:
            logger.warning(f"Could not find infotable for filing {accession}")
            return items

        # Convert holdings to CollectedItems
        report_date = filing.get("reportDate")
        filing_date = filing.get("filingDate")

        for holding in holdings_data:
            items.append(
                CollectedItem(
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
                )
            )

        return items

    async def _fetch_infotable_from_index(
        self, cik: str, accession: str
    ) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """
        Fetch infotable by parsing filing index page.

        SEC 13F infotables often have numeric filenames (e.g., 46994.xml)
        rather than predictable names. This method:
        1. Fetches the filing index page
        2. Finds all XML files listed
        3. Sorts them by size (largest first, as infotables are typically larger)
        4. Tries to parse each as a 13F infotable
        """
        # Get filing index page
        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession}/"
        )

        response = await self._fetch_url(index_url)

        if not response or response.status_code != 200:
            return None, None

        html = response.text

        # Find all XML files with their sizes
        # SEC index HTML format: <td><a href="/path/file.xml"><img...>file.xml</a></td><td>SIZE</td>
        xml_files = []

        # Pattern to find XML file entries with size (handles <img> tags)
        file_pattern = re.compile(
            r'href="([^"]*?([^/"]+\.xml))"[^>]*>.*?</a></td>\s*<td[^>]*>(\d*)</td>',
            re.IGNORECASE | re.DOTALL,
        )
        matches = file_pattern.findall(html)

        for full_path, filename, size_str in matches:
            # Skip primary_doc.xml as it contains form metadata, not holdings
            if filename.lower() == "primary_doc.xml":
                continue
            size = int(size_str) if size_str else 0
            xml_files.append((filename, size))

        # Sort by size descending (infotable is usually the largest XML)
        xml_files.sort(key=lambda x: x[1], reverse=True)

        logger.debug(f"Found {len(xml_files)} XML files in index: {xml_files}")

        # Try each XML file, largest first
        for filename, size in xml_files:
            xml_url = index_url + filename
            xml_response = await self._fetch_url(xml_url)

            if xml_response and xml_response.status_code == 200:
                try:
                    holdings = self._parse_13f_xml(xml_response.text)
                    if holdings:  # Found valid holdings data
                        logger.debug(
                            f"Successfully parsed infotable from {filename} ({len(holdings)} holdings)"
                        )
                        return holdings, xml_url
                except Exception as e:
                    logger.debug(f"Could not parse {xml_url}: {e}")
                    continue

        return None, None

    def _parse_13f_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse 13F infotable XML to extract holdings.

        Handles various SEC XML formats including:
        - Files with default namespace (xmlns="...")
        - Files with prefixed namespaces (ns1:infoTable)
        - Files without namespaces

        Args:
            xml_content: Raw XML string

        Returns:
            List of holding dictionaries
        """
        holdings = []

        try:
            # Check if content looks like a 13F infotable
            if (
                "infoTable" not in xml_content.lower()
                and "infotable" not in xml_content.lower()
            ):
                logger.debug("XML does not appear to be a 13F infotable")
                return []

            # Method 1: Try parsing with namespace stripping
            # Remove all namespace declarations and prefixes for simpler parsing
            clean_xml = xml_content

            # Remove namespace declarations
            clean_xml = re.sub(r'\sxmlns(?::[^=]*)?\s*=\s*"[^"]*"', "", clean_xml)

            # Remove namespace prefixes from tags (e.g., ns1:infoTable -> infoTable)
            clean_xml = re.sub(r"<(/?)(\w+):", r"<\1", clean_xml)

            try:
                root = ET.fromstring(clean_xml)
            except ET.ParseError:
                # Method 2: If cleaning fails, try raw XML with namespace-aware search
                root = ET.fromstring(xml_content)

            # Find all infoTable entries - try multiple approaches
            info_tables = []

            # Try without namespace
            info_tables = root.findall(".//infoTable")
            if not info_tables:
                info_tables = root.findall(".//infotable")

            # Try with wildcard namespace
            if not info_tables:
                info_tables = root.findall(".//{*}infoTable")

            # Try direct children if this is the informationTable root
            if not info_tables and root.tag.lower().endswith("informationtable"):
                info_tables = list(root)

            for entry in info_tables:
                holding = self._extract_holding_from_entry(entry)
                if holding.get("cusip"):
                    holdings.append(holding)

        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            raise

        return holdings

    def _extract_holding_from_entry(self, entry: ET.Element) -> Dict[str, Any]:
        """
        Extract holding data from a single infoTable XML element.

        Args:
            entry: XML Element representing one holding

        Returns:
            Dictionary with holding fields
        """
        holding = {}

        # Field mappings: (output_field, list of possible XML paths)
        field_mappings = [
            ("issuer", ["nameOfIssuer", "issuer"]),
            ("class", ["titleOfClass", "class"]),
            ("cusip", ["cusip", "CUSIP"]),
            ("value", ["value"]),
            ("shares", ["shrsOrPrnAmt/sshPrnamt", "sshPrnamt", "shares"]),
            ("shareType", ["shrsOrPrnAmt/sshPrnamtType", "sshPrnamtType"]),
            ("putCall", ["putCall"]),
            ("investmentDiscretion", ["investmentDiscretion"]),
            ("votingAuthoritySole", ["votingAuthority/Sole", "votingAuthSole", "Sole"]),
            (
                "votingAuthorityShared",
                ["votingAuthority/Shared", "votingAuthShared", "Shared"],
            ),
            ("votingAuthorityNone", ["votingAuthority/None", "votingAuthNone", "None"]),
        ]

        for field, paths in field_mappings:
            for path in paths:
                elem = entry.find(path)
                if elem is None:
                    # Try case-insensitive search through all descendants
                    target_tag = path.lower().split("/")[-1]
                    for child in entry.iter():
                        # Handle namespaced tags by taking only the local name
                        local_tag = (
                            child.tag.split("}")[-1] if "}" in child.tag else child.tag
                        )
                        if local_tag.lower() == target_tag:
                            elem = child
                            break
                if elem is not None and elem.text:
                    holding[field] = elem.text.strip()
                    break

        # Convert numeric fields
        if "value" in holding:
            # Value is in thousands in 13F filings
            try:
                holding["value"] = str(int(holding["value"]) * 1000)
            except ValueError:
                pass

        return holding

    def get_known_ciks(self) -> Dict[str, str]:
        """Return dictionary of known LP CIKs."""
        return KNOWN_CIKS.copy()
