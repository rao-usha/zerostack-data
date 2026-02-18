"""
SEC 13F/13D Collector for PE portfolio discovery.

Fetches quarterly 13F-HR filings (institutional holdings) and SC 13D filings
(>5% beneficial ownership) from SEC EDGAR. Maps holdings to portfolio companies.
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)
from app.sources.pe_collection.config import settings, SEC_FILINGS_URL

logger = logging.getLogger(__name__)

# SEC EDGAR endpoints
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

# Form types to search for
FORM_13F_TYPES = ["13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A"]
FORM_13D_TYPES = ["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"]


class SEC13FCollector(BasePECollector):
    """
    Collects SEC 13F and 13D filings for PE firms.

    13F-HR: Quarterly institutional holdings reports (>$100M AUM managers).
    SC 13D/13G: Beneficial ownership reports (>5% stakes).
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.SEC_13D

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", settings.sec_rate_limit_delay
        )

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        cik: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect 13F holdings and 13D stakes for a PE firm.

        Args:
            entity_id: PE firm ID
            entity_name: Firm name
            website_url: Not used
            cik: SEC Central Index Key (required)
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        if not cik:
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message="No CIK provided — cannot fetch SEC filings",
                started_at=started_at,
            )

        # Zero-pad CIK to 10 digits for SEC API
        cik_padded = cik.lstrip("0").zfill(10)

        try:
            # Fetch company submissions
            submissions = await self._fetch_submissions(cik_padded)
            if not submissions:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=False,
                    error_message=f"Could not fetch SEC submissions for CIK {cik}",
                    started_at=started_at,
                )

            # Extract 13F filings (limit to 4 quarters)
            filings_13f = self._extract_filings(submissions, FORM_13F_TYPES, limit=4)
            logger.info(
                f"Found {len(filings_13f)} 13F filings for {entity_name} (CIK {cik})"
            )

            # Process each 13F filing
            for filing in filings_13f:
                filing_items = await self._process_13f_filing(
                    filing, cik_padded, entity_id, entity_name
                )
                items.extend(filing_items)
                if not filing_items:
                    warnings.append(
                        f"No holdings parsed from 13F filing {filing.get('accessionNumber')}"
                    )

            # Extract 13D/13G filings (limit to 10)
            filings_13d = self._extract_filings(submissions, FORM_13D_TYPES, limit=10)
            logger.info(f"Found {len(filings_13d)} 13D/13G filings for {entity_name}")

            # Process 13D filings (extract issuer/stake info from metadata)
            for filing in filings_13d:
                item = self._process_13d_filing(filing, entity_id, entity_name)
                if item:
                    items.append(item)

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting 13F/13D for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    async def _fetch_submissions(self, cik: str) -> Optional[Dict[str, Any]]:
        """Fetch SEC EDGAR submissions JSON for a CIK."""
        url = SEC_SUBMISSIONS_URL.format(cik=cik)
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
        }
        return await self._fetch_json(url, headers=headers)

    def _extract_filings(
        self,
        submissions: Dict[str, Any],
        form_types: List[str],
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Extract filings of specified form types from submissions data."""
        filings = []
        recent = submissions.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form in form_types and len(filings) < limit:
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
        entity_id: int,
        entity_name: str,
    ) -> List[PECollectedItem]:
        """Process a single 13F filing and extract holdings."""
        items = []
        accession = filing.get("accessionNumber")
        if not accession:
            return items

        accession_formatted = accession.replace("-", "")

        # Fetch infotable via index page
        holdings_data, source_url = await self._fetch_infotable_from_index(
            cik, accession_formatted
        )

        # Fallback: try common filename patterns
        if not holdings_data:
            base_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik.lstrip('0')}/{accession_formatted}/"
            )
            for pattern in ["infotable.xml", f"{accession_formatted}-infotable.xml"]:
                url = base_url + pattern
                response = await self._fetch_url(
                    url, headers={"User-Agent": settings.sec_user_agent}
                )
                if response and response.status_code == 200:
                    try:
                        holdings_data = self._parse_13f_xml(response.text)
                        source_url = url
                        break
                    except Exception:
                        continue

        if not holdings_data:
            return items

        report_date = filing.get("reportDate")
        filing_date = filing.get("filingDate")

        for holding in holdings_data:
            items.append(
                self._create_item(
                    item_type="13f_holding",
                    data={
                        "firm_id": entity_id,
                        "firm_name": entity_name,
                        "cusip": holding.get("cusip"),
                        "issuer_name": holding.get("issuer"),
                        "security_class": holding.get("class"),
                        "shares": holding.get("shares"),
                        "value_usd": holding.get("value"),
                        "put_call": holding.get("putCall"),
                        "investment_discretion": holding.get("investmentDiscretion"),
                        "report_date": report_date,
                        "filing_date": filing_date,
                        "accession_number": accession,
                    },
                    source_url=source_url,
                    confidence="high",
                )
            )

        return items

    async def _fetch_infotable_from_index(
        self, cik: str, accession: str
    ) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """
        Fetch infotable by parsing the filing index page.

        SEC 13F infotables often have numeric filenames. This discovers them
        from the index, sorts by size (largest first), and tries to parse each.
        """
        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik.lstrip('0')}/{accession}/"
        )
        response = await self._fetch_url(
            index_url, headers={"User-Agent": settings.sec_user_agent}
        )
        if not response or response.status_code != 200:
            return None, None

        html = response.text

        # Find XML files with sizes from SEC index HTML
        file_pattern = re.compile(
            r'href="([^"]*?([^/"]+\.xml))"[^>]*>.*?</a></td>\s*<td[^>]*>(\d*)</td>',
            re.IGNORECASE | re.DOTALL,
        )
        xml_files = []
        for full_path, filename, size_str in file_pattern.findall(html):
            if filename.lower() == "primary_doc.xml":
                continue
            size = int(size_str) if size_str else 0
            xml_files.append((filename, size))

        # Sort by size descending — infotable is usually the largest XML
        xml_files.sort(key=lambda x: x[1], reverse=True)

        for filename, _size in xml_files:
            xml_url = index_url + filename
            xml_response = await self._fetch_url(
                xml_url, headers={"User-Agent": settings.sec_user_agent}
            )
            if xml_response and xml_response.status_code == 200:
                try:
                    holdings = self._parse_13f_xml(xml_response.text)
                    if holdings:
                        logger.debug(
                            f"Parsed infotable from {filename} ({len(holdings)} holdings)"
                        )
                        return holdings, xml_url
                except Exception:
                    continue

        return None, None

    def _parse_13f_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse 13F infotable XML to extract holdings.

        Handles namespace variations (default, prefixed, none).
        """
        holdings = []

        if "infotable" not in xml_content.lower():
            return []

        # Strip namespace declarations and prefixes for simpler parsing
        clean_xml = re.sub(r'\sxmlns(?::[^=]*)?\s*=\s*"[^"]*"', "", xml_content)
        clean_xml = re.sub(r"<(/?)(\w+):", r"<\1", clean_xml)

        try:
            root = ET.fromstring(clean_xml)
        except ET.ParseError:
            root = ET.fromstring(xml_content)

        # Find infoTable entries via multiple search strategies
        info_tables = root.findall(".//infoTable")
        if not info_tables:
            info_tables = root.findall(".//infotable")
        if not info_tables:
            info_tables = root.findall(".//{*}infoTable")
        if not info_tables and root.tag.lower().endswith("informationtable"):
            info_tables = list(root)

        for entry in info_tables:
            holding = self._extract_holding_from_entry(entry)
            if holding.get("cusip"):
                holdings.append(holding)

        return holdings

    def _extract_holding_from_entry(self, entry: ET.Element) -> Dict[str, Any]:
        """Extract holding data from a single infoTable XML element."""
        holding = {}

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
                    target_tag = path.lower().split("/")[-1]
                    for child in entry.iter():
                        local_tag = (
                            child.tag.split("}")[-1] if "}" in child.tag else child.tag
                        )
                        if local_tag.lower() == target_tag:
                            elem = child
                            break
                if elem is not None and elem.text:
                    holding[field] = elem.text.strip()
                    break

        # Ensure value is a clean integer string (SEC XML reports whole dollars)
        if "value" in holding:
            try:
                holding["value"] = str(int(holding["value"]))
            except ValueError:
                pass

        return holding

    def _process_13d_filing(
        self,
        filing: Dict[str, Any],
        entity_id: int,
        entity_name: str,
    ) -> Optional[PECollectedItem]:
        """
        Process a 13D/13G filing from submission metadata.

        Extracts issuer name and filing details. Full text parsing of the
        ownership percentage would require fetching the filing document,
        which can be added as an enhancement.
        """
        accession = filing.get("accessionNumber")
        if not accession:
            return None

        return self._create_item(
            item_type="13d_stake",
            data={
                "firm_id": entity_id,
                "firm_name": entity_name,
                "form_type": filing.get("form"),
                "filing_date": filing.get("filingDate"),
                "report_date": filing.get("reportDate"),
                "accession_number": accession,
                "primary_document": filing.get("primaryDocument"),
            },
            source_url=(
                f"https://www.sec.gov/cgi-bin/browse-edgar"
                f"?action=getcompany&CIK={entity_id}&type=SC+13D"
            ),
            confidence="high",
        )
