"""
SEC Form ADV Collector for PE firms.

Collects information from SEC Form ADV filings for registered investment advisers.
Form ADV contains detailed information about:
- Firm identification and registration
- Assets under management
- Types of clients
- Investment strategies
- Key personnel
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)
from app.sources.pe_collection.config import SEC_FILINGS_URL, settings

logger = logging.getLogger(__name__)


class SECADVCollector(BasePECollector):
    """
    Collects data from SEC Form ADV filings.

    Form ADV is the registration form for investment advisers.
    Contains AUM, employee count, client types, and more.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.SEC_ADV

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # SEC allows 10 requests/second, but we're conservative
        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", settings.sec_rate_limit_delay
        )

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        cik: Optional[str] = None,
        crd_number: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect Form ADV data for a PE firm.

        Args:
            entity_id: PE firm ID in our database
            entity_name: Firm name
            website_url: Firm website (not used for SEC)
            cik: SEC Central Index Key
            crd_number: FINRA CRD number

        Returns:
            Collection result with firm data items
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        # Need either CIK or CRD to look up SEC filings
        if not cik and not crd_number:
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message="No CIK or CRD number provided for SEC lookup",
                started_at=started_at,
            )

        try:
            # If we have CIK, fetch submissions directly
            if cik:
                submissions = await self._fetch_sec_submissions(cik)
                if submissions:
                    firm_data = self._parse_submissions(submissions, entity_name)
                    if firm_data:
                        items.append(
                            self._create_item(
                                item_type="firm_update",
                                data=firm_data,
                                source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}",
                                confidence="high",
                            )
                        )

                    # Extract Form ADV filings
                    adv_filings = await self._get_form_adv_filings(cik, submissions)
                    for filing in adv_filings:
                        items.append(
                            self._create_item(
                                item_type="form_adv_filing",
                                data=filing,
                                source_url=filing.get("filing_url"),
                                confidence="high",
                            )
                        )

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting SEC data for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    async def _fetch_sec_submissions(self, cik: str) -> Optional[Dict[str, Any]]:
        """
        Fetch SEC submissions for a CIK.

        Args:
            cik: SEC Central Index Key

        Returns:
            Submissions data or None
        """
        # Pad CIK to 10 digits
        cik_padded = cik.zfill(10)
        url = f"{SEC_FILINGS_URL}/CIK{cik_padded}.json"

        data = await self._fetch_json(
            url, headers={"User-Agent": settings.sec_user_agent}
        )
        return data

    def _parse_submissions(
        self, submissions: Dict[str, Any], entity_name: str
    ) -> Dict[str, Any]:
        """
        Parse SEC submissions data into firm update data.

        Args:
            submissions: Raw SEC submissions data
            entity_name: Firm name for reference

        Returns:
            Parsed firm data
        """
        firm_data = {}

        # Basic info from submissions
        firm_data["sec_name"] = submissions.get("name")
        firm_data["cik"] = submissions.get("cik")
        firm_data["sic_code"] = submissions.get("sic")
        firm_data["sic_description"] = submissions.get("sicDescription")

        # Address info
        addresses = submissions.get("addresses", {})
        if "business" in addresses:
            business = addresses["business"]
            firm_data["headquarters_city"] = business.get("city")
            firm_data["headquarters_state"] = business.get("stateOrCountry")

        # Filing counts
        filings = submissions.get("filings", {})
        recent = filings.get("recent", {})
        if recent:
            form_types = recent.get("form", [])
            firm_data["total_filings"] = len(form_types)
            firm_data["has_13f"] = "13F-HR" in form_types
            firm_data["has_form_d"] = "D" in form_types

        return firm_data

    async def _get_form_adv_filings(
        self, cik: str, submissions: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract Form ADV filings from submissions.

        Args:
            cik: SEC Central Index Key
            submissions: Raw SEC submissions data

        Returns:
            List of Form ADV filing data
        """
        adv_filings = []

        filings = submissions.get("filings", {})
        recent = filings.get("recent", {})

        if not recent:
            return adv_filings

        forms = recent.get("form", [])
        accession_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_documents = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            # Look for Form ADV and related forms
            if "ADV" in form.upper():
                accession = accession_numbers[i] if i < len(accession_numbers) else None
                filing_date = filing_dates[i] if i < len(filing_dates) else None
                primary_doc = (
                    primary_documents[i] if i < len(primary_documents) else None
                )

                if accession:
                    # Format accession number for URL
                    accession_clean = accession.replace("-", "")
                    filing_url = (
                        f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                        f"{accession_clean}/{primary_doc}"
                    )

                    adv_filings.append(
                        {
                            "form_type": form,
                            "filing_date": filing_date,
                            "accession_number": accession,
                            "filing_url": filing_url,
                        }
                    )

        return adv_filings[:5]  # Return only most recent 5
