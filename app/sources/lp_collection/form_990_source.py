"""
IRS Form 990 collector for endowments and foundations.

Collects financial data from IRS Form 990 filings:
- Total assets and net assets
- Investment income and expenses
- Grants made and received
- Executive compensation
- Program services

Data Sources:
- ProPublica Nonprofit Explorer API (free, easy access)
- IRS Exempt Organizations Data (bulk download)
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# ProPublica Nonprofit Explorer API
PROPUBLICA_API_BASE = "https://projects.propublica.org/nonprofits/api/v2"
PROPUBLICA_SEARCH_URL = f"{PROPUBLICA_API_BASE}/search.json"
PROPUBLICA_ORG_URL = f"{PROPUBLICA_API_BASE}/organizations/{{ein}}.json"

# Known EINs for major endowments and foundations
KNOWN_EINS = {
    # University Endowments
    "Harvard University": "042103580",
    "Harvard Management Company": "042103580",
    "Yale University": "060646973",
    "Stanford University": "941156365",
    "Princeton University": "210634501",
    "MIT": "042103594",
    "Massachusetts Institute of Technology": "042103594",
    "University of Pennsylvania": "231352685",
    "Northwestern University": "362167817",
    "Duke University": "560532129",
    "Columbia University": "131623978",
    "University of Chicago": "362177139",
    "Cornell University": "150532082",
    "University of Notre Dame": "350868188",
    "Washington University St Louis": "430653611",
    "Dartmouth College": "020222111",
    "Rice University": "741109620",
    "Vanderbilt University": "620476822",
    "Emory University": "580566256",
    "University of Southern California": "951642394",
    "Brown University": "050258776",
    "Johns Hopkins University": "520595110",
    # Major Foundations
    "Bill & Melinda Gates Foundation": "562618866",
    "Gates Foundation": "562618866",
    "Ford Foundation": "131684331",
    "Lilly Endowment": "350868122",
    "Robert Wood Johnson Foundation": "221024240",
    "W.K. Kellogg Foundation": "381359264",
    "William and Flora Hewlett Foundation": "941655673",
    "Hewlett Foundation": "941655673",
    "David and Lucile Packard Foundation": "942278431",
    "Packard Foundation": "942278431",
    "Andrew W. Mellon Foundation": "131879954",
    "Mellon Foundation": "131879954",
    "Bloomberg Philanthropies": "205765454",
    "Gordon and Betty Moore Foundation": "943397785",
    "MacArthur Foundation": "237093598",
    "Rockefeller Foundation": "131659629",
    "Carnegie Corporation": "135562629",
    "Walton Family Foundation": "132986429",
    "Silicon Valley Community Foundation": "206206752",
    "Simons Foundation": "061578856",
    "Arnold Foundation": "260374892",
    "Ballmer Group": "814660262",
    "Open Society Foundations": "137029285",
    "Soros Foundation": "137029285",
    "Chan Zuckerberg Initiative": "814686096",
    "Bezos Earth Fund": "853954636",
    "Omidyar Network Fund": "943373820",
}


class Form990Collector(BaseCollector):
    """
    Collects financial data from IRS Form 990 filings.

    Form 990 is the annual return filed by tax-exempt organizations.
    For endowments and foundations, it includes:
    - Total assets and liabilities
    - Investment income
    - Grants made
    - Executive compensation
    - Program expenses
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.FORM_990

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        ein: Optional[str] = None,
        years_back: int = 3,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect Form 990 data for an LP (endowment/foundation).

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL (not used for 990)
            ein: IRS Employer Identification Number
            years_back: Number of years of filings to collect

        Returns:
            CollectionResult with financial items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting Form 990 data for {lp_name}")

        try:
            # Resolve EIN
            resolved_ein = self._resolve_ein(lp_name, ein)

            if not resolved_ein:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="Could not find EIN for this organization",
                    warnings=[
                        "Organization may not be tax-exempt or EIN not in database"
                    ],
                    started_at=started_at,
                )

            # Fetch organization data from ProPublica
            org_data = await self._fetch_organization(resolved_ein)

            if not org_data:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="Could not fetch Form 990 data",
                    warnings=["ProPublica API may be unavailable"],
                    started_at=started_at,
                )

            # Extract organization info
            org_info = self._extract_org_info(org_data, lp_id, lp_name, resolved_ein)
            if org_info:
                items.append(org_info)

            # Extract financial data from filings
            filings = org_data.get("filings_with_data", [])[:years_back]
            for filing in filings:
                filing_items = self._extract_filing_data(
                    filing, lp_id, lp_name, resolved_ein
                )
                items.extend(filing_items)

            success = len(items) > 0

            if not items:
                warnings.append("Found organization but no financial data available")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting Form 990 for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    def _resolve_ein(self, lp_name: str, provided_ein: Optional[str]) -> Optional[str]:
        """
        Resolve EIN for an organization.

        Args:
            lp_name: Organization name
            provided_ein: EIN if already known

        Returns:
            9-digit EIN or None
        """
        # Use provided EIN
        if provided_ein:
            # Clean EIN (remove dashes, etc.)
            return re.sub(r"[^0-9]", "", provided_ein).zfill(9)

        # Check known EINs
        for known_name, ein in KNOWN_EINS.items():
            if (
                known_name.lower() in lp_name.lower()
                or lp_name.lower() in known_name.lower()
            ):
                logger.debug(f"Found known EIN {ein} for {lp_name}")
                return ein

        return None

    async def _fetch_organization(self, ein: str) -> Optional[Dict[str, Any]]:
        """
        Fetch organization data from ProPublica Nonprofit Explorer.

        Args:
            ein: 9-digit EIN

        Returns:
            Organization data dictionary or None
        """
        url = PROPUBLICA_ORG_URL.format(ein=ein)

        data = await self._fetch_json(url)

        if not data:
            logger.warning(f"Could not fetch organization {ein} from ProPublica")
            return None

        org = data.get("organization")
        if not org:
            logger.warning(f"No organization found for EIN {ein}")
            return None

        # Add filings data
        org["filings_with_data"] = data.get("filings_with_data", [])

        return org

    def _extract_org_info(
        self,
        org_data: Dict[str, Any],
        lp_id: int,
        lp_name: str,
        ein: str,
    ) -> Optional[CollectedItem]:
        """
        Extract basic organization info from ProPublica data.
        """
        return CollectedItem(
            item_type="990_org_info",
            data={
                "lp_id": lp_id,
                "lp_name": lp_name,
                "ein": ein,
                "legal_name": org_data.get("name"),
                "city": org_data.get("city"),
                "state": org_data.get("state"),
                "ntee_code": org_data.get("ntee_code"),
                "subsection_code": org_data.get("subsection_code"),
                "ruling_date": org_data.get("ruling_date"),
                "tax_period": org_data.get("tax_period"),
                "total_revenue": org_data.get("total_revenue"),
                "total_expenses": org_data.get("total_expenses"),
                "total_assets": org_data.get("total_assets"),
            },
            source_url=f"https://projects.propublica.org/nonprofits/organizations/{ein}",
            confidence="high",
        )

    def _extract_filing_data(
        self,
        filing: Dict[str, Any],
        lp_id: int,
        lp_name: str,
        ein: str,
    ) -> List[CollectedItem]:
        """
        Extract financial data from a single Form 990 filing.
        """
        items = []

        tax_period = filing.get("tax_prd_yr") or filing.get("tax_period")
        if not tax_period:
            return items

        # Convert to integer year
        try:
            fiscal_year = int(str(tax_period)[:4])
        except (ValueError, TypeError):
            return items

        # Main financials
        financials_item = CollectedItem(
            item_type="990_financials",
            data={
                "lp_id": lp_id,
                "lp_name": lp_name,
                "ein": ein,
                "fiscal_year": fiscal_year,
                "tax_period": tax_period,
                # Revenue
                "total_revenue": filing.get("totrevenue"),
                "contributions": filing.get("totcntrbgfts"),
                "program_service_revenue": filing.get("prgmservrev"),
                "investment_income": filing.get("invstmntinc"),
                "other_revenue": filing.get("othrevnue"),
                # Expenses
                "total_expenses": filing.get("totfuncexpns"),
                "program_expenses": filing.get("prgmservexp")
                or filing.get("totprgmrevnue"),
                "management_expenses": filing.get("mgmtgenexp"),
                "fundraising_expenses": filing.get("fundfees")
                or filing.get("fundrsngexp"),
                # Assets/Liabilities
                "total_assets": filing.get("totassetsend"),
                "total_liabilities": filing.get("totliabend"),
                "net_assets": filing.get("netassetsend")
                or filing.get("totnetassetend"),
                # Investments
                "investments_securities": filing.get("invstmntsec"),
                "investments_land_buildings": filing.get("invstmntsland"),
                "investments_other": filing.get("invstmntsother"),
                # Grants
                "grants_paid": filing.get("grntstogovt") or filing.get("grntspayable"),
                # Form type
                "form_type": filing.get("formtype"),
            },
            source_url=filing.get("pdf_url"),
            confidence="high",
        )
        items.append(financials_item)

        # Strategy snapshot for investment allocations if available
        total_assets = filing.get("totassetsend")
        investments_securities = filing.get("invstmntsec")

        if total_assets and investments_securities:
            try:
                total_assets_val = float(total_assets)
                investments_val = float(investments_securities)
                if total_assets_val > 0:
                    securities_pct = (investments_val / total_assets_val) * 100

                    items.append(
                        CollectedItem(
                            item_type="strategy_snapshot",
                            data={
                                "lp_id": lp_id,
                                "fiscal_year": fiscal_year,
                                "total_aum_usd": str(total_assets_val),
                                "public_equity_pct": f"{securities_pct:.1f}",
                                "source_type": "form_990",
                            },
                            source_url=filing.get("pdf_url"),
                            confidence="medium",
                        )
                    )
            except (ValueError, TypeError):
                pass

        return items

    async def search_organization(self, name: str) -> List[Dict[str, Any]]:
        """
        Search for an organization by name.

        Args:
            name: Organization name to search

        Returns:
            List of matching organizations
        """
        params = {"q": name}
        data = await self._fetch_json(PROPUBLICA_SEARCH_URL, params=params)

        if not data:
            return []

        return data.get("organizations", [])

    def get_known_eins(self) -> Dict[str, str]:
        """Return dictionary of known organization EINs."""
        return KNOWN_EINS.copy()
