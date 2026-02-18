"""
SEC Form D XML Parser.

Parses Form D XML filings into structured data.
"""

import logging
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class FormDParser:
    """
    Parser for SEC Form D XML filings.

    Form D XML structure follows the SEC EDGAR Form D Submission Taxonomy.
    """

    # Exemption code mapping
    EXEMPTION_MAP = {
        "06b": "Rule 506(b)",
        "06c": "Rule 506(c)",
        "04": "Rule 504",
        "05": "Rule 505",  # No longer available for new offerings
        "3C": "Section 3(c)",
        "3C.1": "Section 3(c)(1)",
        "3C.7": "Section 3(c)(7)",
    }

    # Industry group mapping
    INDUSTRY_MAP = {
        "Agriculture": "Agriculture",
        "Banking & Financial Services": "Financial Services",
        "Commercial Banking": "Financial Services",
        "Energy": "Energy",
        "Health Care": "Healthcare",
        "Technology": "Technology",
        "Real Estate": "Real Estate",
        "Retailing": "Retail",
        "Manufacturing": "Manufacturing",
        "Other": "Other",
        "Pooled Investment Fund": "Investment Fund",
    }

    def parse(self, xml_content: str) -> Optional[Dict[str, Any]]:
        """
        Parse Form D XML into structured data.

        Args:
            xml_content: Raw XML content

        Returns:
            Parsed Form D data as dictionary
        """
        try:
            root = ET.fromstring(xml_content)
            return self._parse_form_d(root)
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected parse error: {e}")
            return None

    def _parse_form_d(self, root: ET.Element) -> Dict[str, Any]:
        """Parse the main Form D structure."""
        # Handle namespaces - Form D XML uses default namespace
        ns = self._get_namespaces(root)

        result = {
            "submission_type": self._find_text(root, ".//submissionType", ns),
            "issuer": self._parse_issuer(root, ns),
            "related_persons": self._parse_related_persons(root, ns),
            "offering": self._parse_offering(root, ns),
            "investors": self._parse_investors(root, ns),
            "sales_compensation": self._parse_sales_compensation(root, ns),
        }

        return result

    def _get_namespaces(self, root: ET.Element) -> Dict[str, str]:
        """Extract namespaces from XML."""
        ns = {}
        # Common Form D namespaces
        if root.tag.startswith("{"):
            default_ns = root.tag.split("}")[0][1:]
            ns[""] = default_ns
        return ns

    def _find_text(
        self, elem: ET.Element, path: str, ns: Dict, default: str = None
    ) -> Optional[str]:
        """Find element text, handling namespaces."""
        # Try without namespace first
        found = elem.find(path)
        if found is not None and found.text:
            return found.text.strip()

        # Try with various path variations
        path_variants = [
            path,
            path.replace("./", ""),
            f".//{path.split('/')[-1]}",
        ]

        for p in path_variants:
            found = elem.find(p)
            if found is not None and found.text:
                return found.text.strip()

        return default

    def _find_bool(self, elem: ET.Element, path: str, ns: Dict) -> bool:
        """Find element and convert to boolean."""
        text = self._find_text(elem, path, ns)
        if text:
            return text.lower() in ("true", "yes", "1", "y")
        return False

    def _find_int(self, elem: ET.Element, path: str, ns: Dict) -> Optional[int]:
        """Find element and convert to integer."""
        text = self._find_text(elem, path, ns)
        if text:
            try:
                # Remove currency symbols, commas
                clean = text.replace("$", "").replace(",", "").strip()
                return int(float(clean))
            except (ValueError, TypeError):
                pass
        return None

    def _parse_issuer(self, root: ET.Element, ns: Dict) -> Dict[str, Any]:
        """Parse issuer information (Items 1 & 2)."""
        issuer = {}

        # Try multiple possible paths for issuer data
        issuer_elem = (
            root.find(".//issuerInfo") or root.find(".//primaryIssuer") or root
        )

        issuer["name"] = (
            self._find_text(issuer_elem, ".//issuerName", ns)
            or self._find_text(issuer_elem, ".//entityName", ns)
            or self._find_text(root, ".//issuerName", ns)
        )

        issuer["cik"] = (
            self._find_text(issuer_elem, ".//issuerCik", ns)
            or self._find_text(issuer_elem, ".//cik", ns)
            or self._find_text(root, ".//filerCik", ns)
        )

        # Address
        issuer["street"] = self._find_text(issuer_elem, ".//street1", ns)
        issuer["street2"] = self._find_text(issuer_elem, ".//street2", ns)
        issuer["city"] = self._find_text(issuer_elem, ".//city", ns)
        issuer["state"] = self._find_text(
            issuer_elem, ".//stateOrCountry", ns
        ) or self._find_text(issuer_elem, ".//state", ns)
        issuer["zip"] = self._find_text(
            issuer_elem, ".//zipCode", ns
        ) or self._find_text(issuer_elem, ".//zip", ns)
        issuer["phone"] = self._find_text(issuer_elem, ".//issuerPhoneNumber", ns)

        # Entity details
        issuer["entity_type"] = self._find_text(issuer_elem, ".//entityType", ns)
        issuer["jurisdiction"] = self._find_text(
            issuer_elem, ".//jurisdictionOfInc", ns
        ) or self._find_text(issuer_elem, ".//stateOfIncorporation", ns)

        year_text = self._find_text(issuer_elem, ".//yearOfInc", ns)
        if year_text:
            try:
                issuer["year_incorporated"] = int(year_text)
            except ValueError:
                issuer["year_incorporated"] = None
        else:
            issuer["year_incorporated"] = None

        return issuer

    def _parse_related_persons(self, root: ET.Element, ns: Dict) -> List[Dict]:
        """Parse related persons (Item 3)."""
        persons = []

        # Find related person elements
        for person in root.findall(".//relatedPersonInfo") or root.findall(
            ".//relatedPersonsList/relatedPersonInfo"
        ):
            p = {
                "first_name": self._find_text(
                    person, ".//relatedPersonName/firstName", ns
                ),
                "last_name": self._find_text(
                    person, ".//relatedPersonName/lastName", ns
                ),
                "relationship": [],
            }

            # Check relationship flags
            if self._find_bool(person, ".//isDirector", ns):
                p["relationship"].append("Director")
            if self._find_bool(person, ".//isOfficer", ns):
                p["relationship"].append("Executive Officer")
            if self._find_bool(person, ".//isTenPercentOwner", ns):
                p["relationship"].append("10% Owner")
            if self._find_bool(person, ".//isPromoter", ns):
                p["relationship"].append("Promoter")

            if p["first_name"] or p["last_name"]:
                persons.append(p)

        return persons

    def _parse_offering(self, root: ET.Element, ns: Dict) -> Dict[str, Any]:
        """Parse offering information (Items 4-13)."""
        offering = {}

        offering_elem = root.find(".//offeringData") or root

        # Industry (Item 4)
        industry = self._find_text(
            offering_elem, ".//industryGroup/industryGroupType", ns
        )
        offering["industry_group"] = self.INDUSTRY_MAP.get(industry, industry)

        # Revenue range (Item 5)
        offering["revenue_range"] = self._find_text(
            offering_elem, ".//issuerSize/revenueRange", ns
        )

        # Federal exemptions (Item 6)
        exemptions = []
        for exempt in (
            offering_elem.findall(".//federalExemptionsExclusions/item") or []
        ):
            if exempt.text:
                code = exempt.text.strip()
                exemptions.append(self.EXEMPTION_MAP.get(code, f"Rule {code}"))
        offering["exemptions"] = exemptions

        # Date of first sale (Item 7)
        first_sale = self._find_text(offering_elem, ".//dateOfFirstSale/value", ns)
        offering["date_of_first_sale"] = first_sale
        offering["yet_to_occur"] = self._find_bool(
            offering_elem, ".//dateOfFirstSale/yetToOccur", ns
        )

        # Duration (Item 8)
        offering["more_than_one_year"] = self._find_bool(
            offering_elem, ".//durationOfOffering/moreThanOneYear", ns
        )

        # Types of securities (Item 9)
        offering["is_equity"] = self._find_bool(
            offering_elem, ".//typesOfSecuritiesOffered/isEquityType", ns
        )
        offering["is_debt"] = self._find_bool(
            offering_elem, ".//typesOfSecuritiesOffered/isDebtType", ns
        )
        offering["is_option"] = self._find_bool(
            offering_elem, ".//typesOfSecuritiesOffered/isOptionToAcquireType", ns
        )
        offering["is_security_to_be_acquired"] = self._find_bool(
            offering_elem, ".//typesOfSecuritiesOffered/isSecurityToBeAcquiredType", ns
        )
        offering["is_pooled_investment_fund"] = self._find_bool(
            offering_elem, ".//typesOfSecuritiesOffered/isPooledInvestmentFundType", ns
        )

        # Business combination (Item 10)
        offering["is_business_combination"] = self._find_bool(
            offering_elem,
            ".//businessCombinationTransaction/isBusinessCombinationTransaction",
            ns,
        )

        # Minimum investment (Item 11)
        offering["minimum_investment"] = self._find_int(
            offering_elem, ".//minimumInvestmentAccepted", ns
        )

        # Amounts (Item 13)
        offering["total_offering_amount"] = self._find_int(
            offering_elem, ".//offeringSalesAmounts/totalOfferingAmount", ns
        )
        offering["total_amount_sold"] = self._find_int(
            offering_elem, ".//offeringSalesAmounts/totalAmountSold", ns
        )
        offering["total_remaining"] = self._find_int(
            offering_elem, ".//offeringSalesAmounts/totalRemaining", ns
        )

        # Clarification of response (if indefinite offering)
        offering["indefinite"] = self._find_bool(
            offering_elem,
            ".//offeringSalesAmounts/totalOfferingAmount[@indefiniteInd='true']",
            ns,
        )

        return offering

    def _parse_investors(self, root: ET.Element, ns: Dict) -> Dict[str, Any]:
        """Parse investor information (Item 14)."""
        investors = {}

        investors_elem = root.find(".//investors") or root

        investors["total"] = self._find_int(
            investors_elem, ".//totalNumberAlreadyInvested", ns
        )
        investors["accredited"] = self._find_int(
            investors_elem, ".//accreditedInvestors/numberInvested", ns
        ) or self._find_int(investors_elem, ".//numberAccreditedInvestors", ns)
        investors["non_accredited"] = self._find_int(
            investors_elem, ".//nonAccreditedInvestors/numberInvested", ns
        ) or self._find_int(investors_elem, ".//numberNonAccreditedInvestors", ns)

        return investors

    def _parse_sales_compensation(self, root: ET.Element, ns: Dict) -> List[Dict]:
        """Parse sales compensation (Item 15)."""
        compensation = []

        for recipient in root.findall(".//salesCompensationList/recipient") or []:
            comp = {
                "name": self._find_text(recipient, ".//recipientName", ns),
                "crd_number": self._find_text(recipient, ".//recipientCRDNumber", ns),
                "states": [],
            }

            for state in recipient.findall(".//statesOfSolicitationList/state") or []:
                if state.text:
                    comp["states"].append(state.text.strip())

            if comp["name"]:
                compensation.append(comp)

        return compensation
