"""
Corporate Structure Discovery Agent.

Discovers a company's full operating structure (subsidiaries, divisions,
affiliates) from multiple sources:

1. SEC Exhibit 21 from 10-K filing (authoritative subsidiary list)
2. Company website "About" / "Our Businesses" pages
3. LLM general knowledge (cross-referenced with other sources)

Discovered business units are inserted into industrial_companies with
parent_company_id linking back to the parent.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from app.core.people_models import IndustrialCompany
from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.llm_extractor import LLMExtractor

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredUnit:
    """A discovered business unit / subsidiary / division."""

    name: str
    description: str = ""
    parent_name: str = ""
    website: Optional[str] = None
    domains: List[str] = field(default_factory=list)
    unit_type: str = "subsidiary"  # "division", "subsidiary", "affiliate"
    is_public: bool = False
    cik: Optional[str] = None
    jurisdiction: Optional[str] = None
    ownership_pct: Optional[float] = None
    source: str = ""  # "exhibit_21", "website", "llm"


# Shell company / holding company name patterns to skip
SKIP_PATTERNS = [
    r"\bholding\b.*\b(corp|co|company|inc|llc)\b",
    r"\b(trust|trustees?)\b",
    r"\bfunding\b",
    r"\bcapital\s*(markets?\s*)?(corp|co|company|inc|llc)\b",
    r"\bfinancial\s*products?\s*(corp|co|company|inc|llc)\b",
    r"\b(re)?insurance\s*(corp|co|company|inc|llc)\b",
    r"\binternational\s*(holdings?)\b",
    r"\bglobal\s*funding\b",
    r"\bsecurities\s*(corp|co|company|inc|llc)\b",
    r"\bassignment\b",
    r"\bconduit\b",
]


class StructureDiscoveryAgent(BaseCollector):
    """
    Discovers the corporate structure of a company from multiple sources.

    Sources (in priority order):
    1. SEC Exhibit 21 (subsidiaries list from 10-K)
    2. Company website (about/businesses pages)
    3. LLM general knowledge

    Results are merged, deduplicated, and stored as IndustrialCompany records.
    """

    def __init__(self):
        super().__init__(source_type="website")
        self._llm = LLMExtractor()
        self._filing_fetcher = None

    def _get_filing_fetcher(self):
        """Lazy init filing fetcher."""
        if self._filing_fetcher is None:
            from app.sources.people_collection.filing_fetcher import FilingFetcher

            self._filing_fetcher = FilingFetcher()
        return self._filing_fetcher

    async def discover(
        self,
        company_id: int,
        db_session: Session,
        max_units: int = 25,
    ) -> List[DiscoveredUnit]:
        """
        Discover all major operating units for a company.

        Args:
            company_id: Database company ID
            db_session: SQLAlchemy session
            max_units: Maximum units to return

        Returns:
            List of discovered business units
        """
        company = (
            db_session.query(IndustrialCompany)
            .filter(IndustrialCompany.id == company_id)
            .first()
        )

        if not company:
            logger.error(f"[StructureDiscovery] Company {company_id} not found")
            return []

        logger.info(
            f"[StructureDiscovery] Discovering structure for {company.name} "
            f"(id={company_id}, cik={company.cik})"
        )

        all_sources: List[List[DiscoveredUnit]] = []

        # Source 1: SEC Exhibit 21
        if company.cik:
            try:
                exhibit_units = await self._parse_exhibit_21(company.cik, company.name)
                logger.info(
                    f"[StructureDiscovery] Exhibit 21: found {len(exhibit_units)} units"
                )
                all_sources.append(exhibit_units)
            except Exception as e:
                logger.warning(f"[StructureDiscovery] Exhibit 21 failed: {e}")

        # Source 2: Company website
        if company.website:
            try:
                website_units = await self._discover_from_website(
                    company.website, company.name
                )
                logger.info(
                    f"[StructureDiscovery] Website: found {len(website_units)} units"
                )
                all_sources.append(website_units)
            except Exception as e:
                logger.warning(f"[StructureDiscovery] Website discovery failed: {e}")

        # Source 3: LLM general knowledge
        try:
            llm_units = await self._discover_from_llm(company.name)
            logger.info(f"[StructureDiscovery] LLM: found {len(llm_units)} units")
            all_sources.append(llm_units)
        except Exception as e:
            logger.warning(f"[StructureDiscovery] LLM discovery failed: {e}")

        # Merge and deduplicate
        merged = await self._merge_and_deduplicate(all_sources, company.name)

        # Filter out shell companies
        filtered = self._filter_shell_companies(merged)

        # Limit
        filtered = filtered[:max_units]

        logger.info(
            f"[StructureDiscovery] Final: {len(filtered)} business units "
            f"for {company.name} (from {sum(len(s) for s in all_sources)} raw)"
        )

        # Store in database
        await self._store_units(filtered, company_id, company.name, db_session)

        return filtered

    async def _parse_exhibit_21(
        self,
        cik: str,
        company_name: str,
    ) -> List[DiscoveredUnit]:
        """
        Parse Exhibit 21 (subsidiary list) from the most recent 10-K filing.

        Exhibit 21 is a required attachment to 10-K that lists all significant
        subsidiaries, their jurisdiction of incorporation, and ownership %.
        """
        fetcher = self._get_filing_fetcher()

        # Get the latest 10-K
        filing = await fetcher.get_latest_filing(cik, "10-K")
        if not filing:
            logger.info(f"[StructureDiscovery] No 10-K found for CIK {cik}")
            return []

        # Get filing content
        content = await fetcher.get_filing_content(filing, max_length=1000000)
        if not content:
            logger.warning(f"[StructureDiscovery] Could not fetch 10-K content")
            return []

        # Find Exhibit 21 section
        exhibit_text = self._extract_exhibit_21(content)
        if not exhibit_text:
            logger.info(f"[StructureDiscovery] No Exhibit 21 found in 10-K")
            return []

        # Use LLM to extract structured data from Exhibit 21
        prompt = (
            f"You are parsing SEC Exhibit 21 (List of Subsidiaries) from the 10-K filing "
            f"of {company_name}.\n\n"
            f"Extract the list of subsidiaries. For each, provide:\n"
            f"- name: The subsidiary name\n"
            f"- jurisdiction: State/country of incorporation\n"
            f"- ownership_pct: Percentage owned (default 100 if not stated)\n"
            f"- description: Brief description based on the name (e.g., 'investment management')\n"
            f"- is_major: true if this appears to be a major operating entity "
            f"(not a shell company, funding vehicle, or holding company)\n\n"
            f'Return JSON: {{"subsidiaries": [...]}}\n\n'
            f"Exhibit 21 text:\n{exhibit_text[:15000]}\n\n"
            f"Return ONLY valid JSON, no markdown."
        )

        response = await self._llm._call_llm(prompt)
        if not response:
            return []

        parsed = self._llm._parse_json_response(response)
        if not parsed or "subsidiaries" not in parsed:
            return []

        units = []
        for sub in parsed["subsidiaries"]:
            if not sub.get("is_major", True):
                continue

            name = sub.get("name", "").strip()
            if not name or name.lower() == company_name.lower():
                continue

            units.append(
                DiscoveredUnit(
                    name=name,
                    description=sub.get("description", ""),
                    parent_name=company_name,
                    jurisdiction=sub.get("jurisdiction"),
                    ownership_pct=sub.get("ownership_pct", 100),
                    unit_type="subsidiary",
                    source="exhibit_21",
                )
            )

        return units

    def _extract_exhibit_21(self, filing_content: str) -> Optional[str]:
        """Extract Exhibit 21 section from 10-K filing HTML."""
        content_lower = filing_content.lower()

        # Try various markers for Exhibit 21
        markers = [
            r"exhibit\s*21",
            r"subsidiaries\s*of\s*(?:the\s*)?(?:registrant|company)",
            r"list\s*of\s*subsidiaries",
            r"significant\s*subsidiaries",
        ]

        best_start = None
        for marker in markers:
            match = re.search(marker, content_lower)
            if match:
                if best_start is None or match.start() < best_start:
                    best_start = match.start()

        if best_start is None:
            return None

        # Extract from this point forward, up to next exhibit or end
        text = filing_content[best_start:]

        # Find end: next exhibit marker, or limit to reasonable length
        end_markers = [
            r"exhibit\s*2[2-9]",
            r"exhibit\s*[3-9]",
            r"signatures?\s*$",
            r"pursuant\s*to\s*the\s*requirements",
        ]

        end_pos = len(text)
        for marker in end_markers:
            match = re.search(marker, text.lower())
            if match and match.start() > 100:  # Don't clip too early
                end_pos = min(end_pos, match.start())

        text = text[: min(end_pos, 30000)]

        # Clean HTML tags for readability
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        return text if len(text) > 50 else None

    async def _discover_from_website(
        self,
        website: str,
        company_name: str,
    ) -> List[DiscoveredUnit]:
        """
        Discover business units from the company's about/businesses pages.
        """
        if not website.startswith("http"):
            website = "https://" + website

        domain = urlparse(website).netloc
        if not domain:
            domain = (
                website.replace("https://", "").replace("http://", "").split("/")[0]
            )

        # Try both with and without www prefix
        domains_to_try = [domain]
        if not domain.startswith("www."):
            domains_to_try.insert(0, f"www.{domain}")

        # Try various pages that typically list business units
        # Prioritize the most common patterns first
        candidate_urls = []
        for d in domains_to_try:
            base = f"https://{d}"
            candidate_urls.extend(
                [
                    f"{base}/about",
                    f"{base}/about/our-businesses",
                    f"{base}/about/businesses",
                    f"{base}/businesses",
                    f"{base}/our-businesses",
                ]
            )

        all_text = []
        for url in candidate_urls:
            try:
                content = await self.fetch_url(url, cache_ttl_seconds=7200)
                if content and len(content) > 500:
                    # Clean HTML
                    cleaned = re.sub(r"<script[^>]*>[\s\S]*?</script>", "", content)
                    cleaned = re.sub(r"<style[^>]*>[\s\S]*?</style>", "", cleaned)
                    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
                    cleaned = re.sub(r"\s+", " ", cleaned).strip()

                    if len(cleaned) > 200:
                        all_text.append(f"--- Page: {url} ---\n{cleaned[:5000]}")

                    if len(all_text) >= 3:
                        break
            except Exception as e:
                logger.debug(f"[StructureDiscovery] Failed to fetch {url}: {e}")
                continue

        if not all_text:
            return []

        combined_text = "\n\n".join(all_text)

        prompt = (
            f"You are analyzing the website of {company_name} to identify their major "
            f"business units, divisions, subsidiaries, and affiliates.\n\n"
            f"From the following web page content, extract the major operating entities.\n"
            f"For each, provide:\n"
            f"- name: The business unit name\n"
            f"- description: What it does\n"
            f"- website: Its own website URL if mentioned\n"
            f"- unit_type: 'division', 'subsidiary', or 'affiliate'\n\n"
            f"Only include substantial operating entities, not products or campaigns.\n\n"
            f"Web content:\n{combined_text[:12000]}\n\n"
            f'Return JSON: {{"units": [...]}}\n'
            f"Return ONLY valid JSON, no markdown."
        )

        response = await self._llm._call_llm(prompt)
        if not response:
            return []

        parsed = self._llm._parse_json_response(response)
        if not parsed or "units" not in parsed:
            return []

        units = []
        for u in parsed["units"]:
            name = u.get("name", "").strip()
            if not name or name.lower() == company_name.lower():
                continue

            unit_website = u.get("website")
            domains = []
            if unit_website:
                try:
                    d = urlparse(unit_website).netloc
                    if d:
                        domains = [d.lstrip("www.")]
                except Exception:
                    pass

            units.append(
                DiscoveredUnit(
                    name=name,
                    description=u.get("description", ""),
                    parent_name=company_name,
                    website=unit_website,
                    domains=domains,
                    unit_type=u.get("unit_type", "division"),
                    source="website",
                )
            )

        return units

    async def _discover_from_llm(
        self,
        company_name: str,
    ) -> List[DiscoveredUnit]:
        """
        Use LLM general knowledge to discover business units.
        Cross-referenced with other sources for validation.
        """
        prompt = (
            f"List the major operating divisions, subsidiaries, and affiliates of "
            f"{company_name}.\n\n"
            f"For each entity, provide:\n"
            f"- name: The entity name\n"
            f"- description: What it does (1 sentence)\n"
            f"- parent_name: Its immediate parent entity\n"
            f"- website: Its website domain if known\n"
            f"- unit_type: 'division', 'subsidiary', or 'affiliate'\n"
            f"- is_public: Whether it has its own SEC filings (true/false)\n\n"
            f"Focus on major operating entities with their own leadership teams. "
            f"Do not include shell companies, holding companies, or inactive entities.\n"
            f"Include up to 25 entities.\n\n"
            f'Return JSON: {{"units": [...]}}\n'
            f"Return ONLY valid JSON, no markdown."
        )

        response = await self._llm._call_llm(prompt)
        if not response:
            return []

        parsed = self._llm._parse_json_response(response)
        if not parsed or "units" not in parsed:
            return []

        units = []
        for u in parsed["units"]:
            name = u.get("name", "").strip()
            if not name or name.lower() == company_name.lower():
                continue

            unit_website = u.get("website")
            domains = []
            if unit_website:
                if not unit_website.startswith("http"):
                    unit_website = "https://" + unit_website
                try:
                    d = urlparse(unit_website).netloc
                    if d:
                        domains = [d.lstrip("www.")]
                except Exception:
                    pass

            units.append(
                DiscoveredUnit(
                    name=name,
                    description=u.get("description", ""),
                    parent_name=u.get("parent_name", company_name),
                    website=unit_website,
                    domains=domains,
                    unit_type=u.get("unit_type", "subsidiary"),
                    is_public=u.get("is_public", False),
                    source="llm",
                )
            )

        return units

    async def _merge_and_deduplicate(
        self,
        sources: List[List[DiscoveredUnit]],
        parent_company_name: str,
    ) -> List[DiscoveredUnit]:
        """
        Merge findings from all sources, deduplicate by name similarity.

        Priority: exhibit_21 > website > llm
        """
        if not sources:
            return []

        # Flatten
        all_units = []
        for source_list in sources:
            all_units.extend(source_list)

        if not all_units:
            return []

        # Group by normalized name
        name_groups: Dict[str, List[DiscoveredUnit]] = {}
        for unit in all_units:
            key = self._normalize_name(unit.name)
            if key not in name_groups:
                name_groups[key] = []
            name_groups[key].append(unit)

        # Also check for substring matches (e.g., "PGIM" matches "PGIM Real Estate")
        # We want to keep both as separate entities
        merged = []
        for key, group in name_groups.items():
            # Pick the best version (prefer exhibit_21 > website > llm)
            source_priority = {"exhibit_21": 0, "website": 1, "llm": 2}
            group.sort(key=lambda u: source_priority.get(u.source, 3))
            best = group[0]

            # Enrich from other sources
            for other in group[1:]:
                if not best.description and other.description:
                    best.description = other.description
                if not best.website and other.website:
                    best.website = other.website
                if not best.domains and other.domains:
                    best.domains = other.domains
                if not best.cik and other.cik:
                    best.cik = other.cik
                if other.is_public:
                    best.is_public = True

            # Track how many sources confirmed this unit
            source_count = len(set(u.source for u in group))
            if source_count > 1:
                best.source = f"{best.source}+{source_count}_sources"

            merged.append(best)

        return merged

    def _normalize_name(self, name: str) -> str:
        """Normalize a company name for deduplication."""
        name = name.lower().strip()
        # Remove common suffixes
        for suffix in [
            " inc",
            " inc.",
            " llc",
            " corp",
            " corporation",
            " company",
            " co",
            " co.",
            " ltd",
            " limited",
            " group",
            " holdings",
            ",",
            ".",
        ]:
            name = name.replace(suffix, "")
        return name.strip()

    def _filter_shell_companies(
        self,
        units: List[DiscoveredUnit],
    ) -> List[DiscoveredUnit]:
        """Filter out likely shell companies, holding entities, etc."""
        filtered = []
        for unit in units:
            name_lower = unit.name.lower()

            is_shell = False
            for pattern in SKIP_PATTERNS:
                if re.search(pattern, name_lower):
                    # Don't skip if it has a website or description suggesting real operations
                    if not unit.website and not unit.description:
                        is_shell = True
                        break

            if not is_shell:
                filtered.append(unit)
            else:
                logger.debug(
                    f"[StructureDiscovery] Filtered shell company: {unit.name}"
                )

        return filtered

    async def _store_units(
        self,
        units: List[DiscoveredUnit],
        parent_id: int,
        parent_name: str,
        db_session: Session,
    ) -> int:
        """
        Insert or update IndustrialCompany records for discovered units.

        Returns:
            Number of records created/updated
        """
        stored = 0

        for unit in units:
            # Check if already exists (by name match under same parent)
            existing = (
                db_session.query(IndustrialCompany)
                .filter(
                    IndustrialCompany.parent_company_id == parent_id,
                    IndustrialCompany.name == unit.name,
                )
                .first()
            )

            if existing:
                # Update fields that may be new
                if unit.website and not existing.website:
                    existing.website = unit.website
                if unit.cik and not existing.cik:
                    existing.cik = unit.cik
                logger.debug(f"[StructureDiscovery] Updated existing: {unit.name}")
            else:
                # Create new record
                new_company = IndustrialCompany(
                    name=unit.name,
                    website=unit.website,
                    cik=unit.cik,
                    parent_company_id=parent_id,
                    is_subsidiary=True,
                    ownership_type=unit.unit_type,
                    industry_segment=unit.description[:200]
                    if unit.description
                    else None,
                    status="active",
                )
                db_session.add(new_company)
                logger.info(f"[StructureDiscovery] Created subsidiary: {unit.name}")

            stored += 1

        if stored > 0:
            db_session.commit()

        logger.info(
            f"[StructureDiscovery] Stored {stored} business units for {parent_name}"
        )

        return stored
