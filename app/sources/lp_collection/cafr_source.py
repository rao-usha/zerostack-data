"""
CAFR (Comprehensive Annual Financial Report) collector for LP data.

Collects data from public pension CAFRs:
- Asset allocation details
- Investment performance
- Manager relationships
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


# Common CAFR document patterns
CAFR_PATTERNS = [
    r"cafr|acfr",
    r"comprehensive.*annual.*financial.*report",
    r"annual.*report.*\d{4}",
    r"financial.*statements.*\d{4}",
]


class CafrCollector(BaseCollector):
    """
    Collects LP data from CAFR (Comprehensive Annual Financial Report).

    CAFRs are required for public pension funds and contain:
    - Detailed asset allocation
    - Investment returns by asset class
    - Manager/fund relationships
    - Actuarial information
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.CAFR

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect CAFR data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL

        Returns:
            CollectionResult with CAFR-derived data
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting CAFR data for {lp_name}")

        if not website_url:
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message="No website URL provided for CAFR search",
                started_at=started_at,
            )

        try:
            # Find CAFR document links
            cafr_links = await self._find_cafr_links(website_url)

            if not cafr_links:
                warnings.append("No CAFR documents found")
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="No CAFR documents found on website",
                    warnings=warnings,
                    started_at=started_at,
                )

            # Process each CAFR link
            for cafr_link in cafr_links[:3]:  # Limit to 3 most recent
                doc_items = self._create_cafr_document_items(cafr_link, lp_id)
                items.extend(doc_items)

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
            logger.error(f"Error collecting CAFR for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _find_cafr_links(self, website_url: str) -> List[Dict[str, Any]]:
        """Find links to CAFR documents on LP website."""
        cafr_links = []

        # Fetch website
        response = await self._fetch_url(website_url)
        if not response or response.status_code != 200:
            return cafr_links

        html = response.text

        # Find PDF links that look like CAFRs
        pdf_pattern = re.compile(
            r'href=["\']([^"\']*\.pdf)["\'][^>]*>([^<]*)',
            re.IGNORECASE
        )

        for match in pdf_pattern.finditer(html):
            href = match.group(1)
            link_text = match.group(2).strip()

            # Check if matches CAFR pattern
            combined_text = f"{href} {link_text}".lower()
            if any(re.search(pattern, combined_text, re.IGNORECASE) for pattern in CAFR_PATTERNS):
                # Try to extract year
                year_match = re.search(r"20\d{2}", combined_text)
                year = int(year_match.group()) if year_match else None

                # Make absolute URL
                if href.startswith("/"):
                    from urllib.parse import urljoin
                    full_url = urljoin(website_url, href)
                elif href.startswith("http"):
                    full_url = href
                else:
                    full_url = f"{website_url.rstrip('/')}/{href}"

                cafr_links.append({
                    "url": full_url,
                    "title": link_text,
                    "fiscal_year": year,
                })

        # Sort by year (most recent first)
        cafr_links.sort(key=lambda x: x.get("fiscal_year") or 0, reverse=True)

        return cafr_links

    def _create_cafr_document_items(
        self,
        cafr_info: Dict[str, Any],
        lp_id: int,
    ) -> List[CollectedItem]:
        """Create collection items from CAFR document info."""
        items = []

        # Document link item
        items.append(CollectedItem(
            item_type="document_link",
            data={
                "lp_id": lp_id,
                "url": cafr_info["url"],
                "title": cafr_info.get("title", "CAFR"),
                "document_type": "cafr",
                "file_format": "pdf",
                "fiscal_year": cafr_info.get("fiscal_year"),
                "source_type": "cafr",
            },
            source_url=cafr_info["url"],
            confidence="high",
        ))

        return items

    def _extract_cafr_metadata(
        self,
        cafr_url: str,
        pdf_content: bytes,
    ) -> Dict[str, Any]:
        """
        Extract metadata from CAFR PDF.

        Note: Full PDF parsing would require additional libraries
        (PyPDF2, pdfplumber, etc.). This is a placeholder.
        """
        # In production, would parse PDF to extract:
        # - Total fund value
        # - Asset allocation percentages
        # - Performance returns
        # - Manager relationships

        return {}
