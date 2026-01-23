"""
SEC Form ADV collector for LP data.

Collects investment adviser disclosures from SEC EDGAR:
- AUM (Assets Under Management)
- Client types
- Fee structures
- Key personnel
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# SEC IAPD (Investment Adviser Public Disclosure) API
SEC_IAPD_SEARCH_URL = "https://api.advfn.com/apa/v4/firms"  # Example - actual SEC API varies
SEC_FORM_ADV_BASE_URL = "https://reports.advfn.com/v4/firms"


class SecAdvCollector(BaseCollector):
    """
    Collects LP data from SEC Form ADV filings.

    Form ADV is filed by registered investment advisers and includes:
    - AUM and client information
    - Fee structures
    - Business activities
    - Disciplinary history
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.SEC_ADV

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        sec_crd_number: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect Form ADV data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL (for matching)
            sec_crd_number: SEC CRD number (if known)

        Returns:
            CollectionResult with Form ADV data
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting SEC Form ADV data for {lp_name}")

        try:
            # If we have a CRD number, fetch directly
            if sec_crd_number:
                adv_data = await self._fetch_form_adv_by_crd(sec_crd_number)
            else:
                # Search by name
                adv_data = await self._search_form_adv_by_name(lp_name)

            if not adv_data:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="No Form ADV filing found",
                    warnings=["Could not find Form ADV filing for this LP"],
                    started_at=started_at,
                )

            # Extract items from Form ADV data
            items = self._extract_items_from_adv(adv_data, lp_id, lp_name)

            success = len(items) > 0

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting SEC Form ADV for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _fetch_form_adv_by_crd(self, crd_number: str) -> Optional[Dict[str, Any]]:
        """
        Fetch Form ADV data by CRD number.

        Note: This is a placeholder implementation. In production, would use
        the actual SEC IAPD API or EDGAR filing system.
        """
        # SEC IAPD API endpoint
        url = f"https://api.sec.gov/submissions/CIK{crd_number.zfill(10)}.json"

        data = await self._fetch_json(url)
        return data

    async def _search_form_adv_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Search for Form ADV filing by firm name.

        Note: This is a placeholder implementation. In production, would use
        the SEC full-text search API or IAPD search.
        """
        # Simplified search - in production would use SEC EDGAR full-text search
        # For now, return None to indicate not found
        logger.debug(f"Searching SEC Form ADV for: {name}")
        return None

    def _extract_items_from_adv(
        self,
        adv_data: Dict[str, Any],
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """
        Extract collection items from Form ADV data.

        Extracts:
        - AUM information
        - Client type breakdown
        - Key personnel
        - Fee information
        """
        items = []

        # Extract AUM
        aum = adv_data.get("aum") or adv_data.get("regulatory_assets_under_management")
        if aum:
            items.append(CollectedItem(
                item_type="strategy_info",
                data={
                    "lp_id": lp_id,
                    "aum_usd_millions": str(aum),
                    "source_type": "sec_adv",
                },
                source_url=f"SEC Form ADV for {lp_name}",
                confidence="high",
            ))

        # Extract CRD number
        crd = adv_data.get("crd_number") or adv_data.get("cik")
        if crd:
            items.append(CollectedItem(
                item_type="identifier",
                data={
                    "lp_id": lp_id,
                    "sec_crd_number": str(crd),
                    "source_type": "sec_adv",
                },
                source_url=f"SEC Form ADV for {lp_name}",
                confidence="high",
            ))

        # Extract officers/executives
        officers = adv_data.get("officers", [])
        for officer in officers[:10]:  # Limit
            name = officer.get("name")
            title = officer.get("title")

            if name:
                role_category = self._categorize_sec_role(title or "")
                items.append(CollectedItem(
                    item_type="contact",
                    data={
                        "lp_id": lp_id,
                        "full_name": name,
                        "title": title,
                        "role_category": role_category,
                        "source_type": "sec_adv",
                    },
                    source_url=f"SEC Form ADV for {lp_name}",
                    confidence="high",
                ))

        # Extract client types
        client_types = adv_data.get("client_types", {})
        if client_types:
            items.append(CollectedItem(
                item_type="client_info",
                data={
                    "lp_id": lp_id,
                    "client_types": client_types,
                    "source_type": "sec_adv",
                },
                source_url=f"SEC Form ADV for {lp_name}",
                confidence="high",
            ))

        # Extract investment types
        investment_types = adv_data.get("advisory_activities", {})
        if investment_types:
            items.append(CollectedItem(
                item_type="investment_activities",
                data={
                    "lp_id": lp_id,
                    "advisory_activities": investment_types,
                    "source_type": "sec_adv",
                },
                source_url=f"SEC Form ADV for {lp_name}",
                confidence="high",
            ))

        return items

    def _categorize_sec_role(self, title: str) -> str:
        """Categorize SEC officer title into standard role category."""
        title_lower = title.lower()

        if "chief investment" in title_lower or "cio" in title_lower:
            return "CIO"
        if "chief executive" in title_lower or "ceo" in title_lower:
            return "CEO"
        if "chief financial" in title_lower or "cfo" in title_lower:
            return "CFO"
        if "chief compliance" in title_lower or "cco" in title_lower:
            return "CCO"
        if "managing director" in title_lower:
            return "Managing Director"
        if "director" in title_lower:
            return "Investment Director"
        if "president" in title_lower:
            return "CEO"

        return "Other"
