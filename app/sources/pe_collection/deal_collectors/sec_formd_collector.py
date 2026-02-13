"""
SEC Form D Collector for PE deal intelligence.

Wraps the existing Form D client and parser to collect private placement
filings (Reg D) as PE deal data via the PE collection orchestrator.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)
from app.sources.pe_collection.config import settings
from app.sources.sec_form_d.client import FormDClient
from app.sources.sec_form_d.parser import FormDParser

logger = logging.getLogger(__name__)


class SECFormDCollector(BasePECollector):
    """
    Collects SEC Form D filings for PE firms.

    Form D is filed for private placements under Regulation D.
    Contains offering details, exemptions, investor counts, and related persons.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.SEC_FORM_D

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", settings.sec_rate_limit_delay
        )
        self._client = FormDClient()
        self._parser = FormDParser()

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        cik: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect Form D filings for a PE firm.

        Args:
            entity_id: PE firm ID in database
            entity_name: Firm name
            website_url: Firm website (not used)
            cik: SEC Central Index Key (required)

        Returns:
            Collection result with deal/offering items
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
                error_message="No CIK provided for Form D lookup",
                started_at=started_at,
            )

        try:
            # Get Form D filings from submissions
            filings = await self._client.get_form_d_filings_from_submissions(cik)

            if not filings:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=["No Form D filings found"],
                    started_at=started_at,
                )

            # Parse each filing (limit to most recent 10)
            for filing in filings[:10]:
                accession = filing.get("accession_number")
                primary_doc = filing.get("primary_document")
                if not accession:
                    continue

                xml_content = await self._client.get_filing_xml(
                    cik, accession, primary_doc
                )
                if not xml_content:
                    warnings.append(f"Could not fetch XML for {accession}")
                    continue

                parsed = self._parser.parse(xml_content)
                if not parsed:
                    warnings.append(f"Could not parse XML for {accession}")
                    continue

                # Build deal item from parsed Form D
                offering = parsed.get("offering", {})
                issuer = parsed.get("issuer", {})
                deal_data = {
                    "form_type": filing.get("form"),
                    "filing_date": filing.get("filing_date"),
                    "accession_number": accession,
                    "issuer_name": issuer.get("name"),
                    "issuer_cik": issuer.get("cik"),
                    "exemption": offering.get("exemption"),
                    "offering_amount": offering.get("amount_usd"),
                    "total_amount_sold": offering.get("total_amount_sold"),
                    "total_remaining": offering.get("total_remaining"),
                    "industry": offering.get("industry"),
                    "investor_count": offering.get("number_investors_already_invested"),
                }

                items.append(
                    self._create_item(
                        item_type="form_d_filing",
                        data=deal_data,
                        source_url=(
                            f"https://www.sec.gov/cgi-bin/browse-edgar"
                            f"?action=getcompany&CIK={cik}&type=D"
                        ),
                        confidence="high",
                    )
                )

                # Extract related persons as people items
                for person in parsed.get("related_persons", []):
                    person_data = {
                        "name": person.get("name"),
                        "relationship": person.get("role"),
                        "filing_date": filing.get("filing_date"),
                        "issuer_name": issuer.get("name"),
                    }
                    items.append(
                        self._create_item(
                            item_type="related_person",
                            data=person_data,
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
            logger.error(f"Error collecting Form D for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )
