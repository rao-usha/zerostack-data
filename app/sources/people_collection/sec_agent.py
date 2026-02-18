"""
SEC Filing collection agent.

Collects executive and board data from SEC EDGAR filings:
- DEF 14A proxy statements for executives and board
- Form 4 filings for officers and directors
- 8-K filings for leadership changes
- 10-K for additional executive info

Includes CIK auto-discovery to find CIK by company name.
"""

import logging
from typing import List
from datetime import datetime, date, timedelta

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.filing_fetcher import FilingFetcher
from app.sources.people_collection.sec_parser import SECParser
from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    CollectionResult,
    ExtractionConfidence,
    TitleLevel,
)

logger = logging.getLogger(__name__)


class SECAgent(BaseCollector):
    """
    Agent for collecting leadership data from SEC filings.

    Process:
    1. Fetch recent filings from EDGAR
    2. Parse DEF 14A for executives and board
    3. Parse 8-K for leadership changes
    4. Deduplicate and validate results
    """

    def __init__(self):
        super().__init__(source_type="sec_edgar")
        self.fetcher = FilingFetcher()
        self.parser = SECParser()

    async def close(self) -> None:
        """Close all resources."""
        await super().close()
        await self.fetcher.close()

    async def collect(
        self,
        company_id: int,
        company_name: str,
        cik: str = None,
        include_8k: bool = True,
        include_form4: bool = True,
        days_back: int = 365,
    ) -> CollectionResult:
        """
        Collect leadership data from SEC filings.

        Args:
            company_id: Database ID of the company
            company_name: Name of the company
            cik: SEC CIK number (if None, will try to auto-discover)
            include_8k: Whether to check 8-K filings for changes
            include_form4: Whether to check Form 4s for officers/directors
            days_back: How far back to look for filings

        Returns:
            CollectionResult with extracted people and changes
        """
        started_at = datetime.utcnow()

        result = CollectionResult(
            company_id=company_id,
            company_name=company_name,
            source="sec",
            started_at=started_at,
        )

        # Track filings checked for diagnostics
        result.filings_checked = 0

        logger.info(f"[SECAgent] Starting SEC collection for {company_name}")

        # Try to auto-discover CIK if not provided
        if not cik:
            logger.info(
                f"[SECAgent] No CIK provided, attempting auto-discovery for '{company_name}'"
            )
            cik = await self.fetcher.get_cik_for_company(company_name, min_score=0.7)

            if cik:
                logger.info(f"[SECAgent] Auto-discovered CIK: {cik}")
                result.warnings.append(f"CIK auto-discovered: {cik}")
            else:
                result.errors.append(
                    f"No CIK provided and auto-discovery failed for '{company_name}'. "
                    "Company may be private or name may not match SEC records."
                )
                result.success = False
                return self._finalize_result(result)

        logger.info(
            f"[SECAgent] Collecting from SEC EDGAR for {company_name} (CIK: {cik})"
        )

        try:
            all_people: List[ExtractedPerson] = []
            all_changes: List[LeadershipChange] = []

            # 1. Get and parse latest proxy statement (DEF 14A)
            logger.info("[SECAgent] Step 1: Fetching proxy statement")
            proxy_people = await self._collect_from_proxy(cik, company_name, result)
            all_people.extend(proxy_people)

            # 2. Get executives from 10-K Item 10
            logger.info("[SECAgent] Step 2: Checking 10-K filing")
            tenk_people = await self._collect_from_10k(cik, company_name, result)
            all_people.extend(tenk_people)

            # 3. Get officers/directors from Form 4 filings
            if include_form4:
                logger.info("[SECAgent] Step 3: Checking Form 4 filings")
                form4_people = await self._collect_from_form4s(
                    cik, company_name, result
                )
                all_people.extend(form4_people)

            # 4. Check recent 8-K filings for leadership changes
            if include_8k:
                logger.info("[SECAgent] Step 4: Checking 8-K filings")
                since_date = date.today() - timedelta(days=days_back)
                changes = await self._collect_from_8ks(
                    cik, company_name, since_date, result
                )
                all_changes.extend(changes)

            # 4. Deduplicate people
            logger.info(f"[SECAgent] Deduplicating {len(all_people)} people")
            unique_people = self._deduplicate_people(all_people)

            # 5. Deduplicate changes
            unique_changes = self._deduplicate_changes(all_changes)

            # Update result
            result.extracted_people = unique_people
            result.extracted_changes = unique_changes
            result.people_found = len(unique_people)
            result.changes_detected = len(unique_changes)
            result.success = True

            logger.info(
                f"[SECAgent] SEC collection complete for {company_name}: "
                f"{result.people_found} people, {result.changes_detected} changes, "
                f"{result.filings_checked} filings checked"
            )

        except Exception as e:
            logger.exception(
                f"[SECAgent] Error collecting SEC data for {company_name}: {e}"
            )
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

    async def _collect_from_proxy(
        self,
        cik: str,
        company_name: str,
        result: CollectionResult,
    ) -> List[ExtractedPerson]:
        """Collect people from proxy statement."""
        people = []

        proxy = await self.fetcher.get_latest_proxy(cik)

        if not proxy:
            result.warnings.append("No proxy statement (DEF 14A) found")
            logger.warning(f"[SECAgent] No proxy statement found for CIK {cik}")
            return people

        logger.info(
            f"[SECAgent] Found proxy from {proxy.filing_date}: {proxy.accession_number}"
        )
        result.filings_checked += 1

        proxy_content = await self.fetcher.get_filing_content(proxy)

        if not proxy_content:
            result.warnings.append("Failed to fetch proxy content")
            logger.warning("[SECAgent] Failed to fetch proxy content")
            return people

        logger.debug(f"[SECAgent] Proxy content length: {len(proxy_content)} chars")

        proxy_result = await self.parser.parse_proxy_statement(
            proxy_content, company_name, cik
        )

        # Add source info
        for person in proxy_result.executives:
            person.source_url = proxy.document_url
        for person in proxy_result.board_members:
            person.source_url = proxy.document_url
            person.is_board_member = True

        people.extend(proxy_result.executives)
        people.extend(proxy_result.board_members)

        logger.info(
            f"[SECAgent] Proxy extraction: {len(proxy_result.executives)} executives, "
            f"{len(proxy_result.board_members)} board members "
            f"(confidence: {proxy_result.extraction_confidence})"
        )

        return people

    async def _collect_from_10k(
        self,
        cik: str,
        company_name: str,
        result: CollectionResult,
    ) -> List[ExtractedPerson]:
        """Collect executives from 10-K Item 10 section."""
        people = []

        try:
            tenk = await self.fetcher.get_latest_filing(cik, filing_type="10-K")

            if not tenk:
                result.warnings.append("No 10-K filing found")
                logger.warning(f"[SECAgent] No 10-K filing found for CIK {cik}")
                return people

            logger.info(
                f"[SECAgent] Found 10-K from {tenk.filing_date}: {tenk.accession_number}"
            )
            result.filings_checked += 1

            # 10-K filings are very large (5-10MB for Fortune 500); Item 10 is
            # often at 40-60% into the document. Request up to 5MB.
            tenk_content = await self.fetcher.get_filing_content(
                tenk, max_length=5000000
            )

            if not tenk_content:
                result.warnings.append("Failed to fetch 10-K content")
                return people

            logger.debug(f"[SECAgent] 10-K content length: {len(tenk_content)} chars")

            executives = await self.parser.parse_10k_executives(
                tenk_content, company_name
            )

            # Add source info
            for person in executives:
                person.source_url = tenk.document_url
                person.confidence = ExtractionConfidence.HIGH

            people.extend(executives)

            logger.info(f"[SECAgent] 10-K extraction: {len(people)} executives")

        except Exception as e:
            logger.warning(f"[SECAgent] 10-K extraction failed: {e}")
            result.warnings.append(f"10-K extraction failed: {e}")

        return people

    async def _collect_from_form4s(
        self,
        cik: str,
        company_name: str,
        result: CollectionResult,
    ) -> List[ExtractedPerson]:
        """Collect officers/directors from Form 4 filings."""
        people = []

        try:
            filers = await self.fetcher.get_form4_filers(cik, limit=100)
            result.filings_checked += min(100, len(filers))

            for filer in filers:
                name = filer.get("name", "")
                title = filer.get("title", "")
                is_officer = filer.get("is_officer", False)
                is_director = filer.get("is_director", False)

                if not name:
                    continue

                # Determine title level
                title_level = TitleLevel.UNKNOWN
                if is_director:
                    title_level = TitleLevel.BOARD
                elif title:
                    title_level = self._infer_title_level(title)

                person = ExtractedPerson(
                    full_name=name,
                    title=title or ("Director" if is_director else "Officer"),
                    title_level=title_level,
                    is_board_member=is_director,
                    is_executive=is_officer,
                    confidence=ExtractionConfidence.HIGH,  # Form 4 is authoritative
                    extraction_notes="From SEC Form 4 filing",
                )
                people.append(person)

            logger.info(f"[SECAgent] Form 4 extraction: {len(people)} filers")

        except Exception as e:
            logger.warning(f"[SECAgent] Form 4 extraction failed: {e}")
            result.warnings.append(f"Form 4 extraction failed: {e}")

        return people

    async def _collect_from_8ks(
        self,
        cik: str,
        company_name: str,
        since_date: date,
        result: CollectionResult,
    ) -> List[LeadershipChange]:
        """Collect leadership changes from 8-K filings."""
        changes = []

        filings_8k = await self.fetcher.get_recent_8ks(cik, since_date, limit=10)
        logger.info(
            f"[SECAgent] Found {len(filings_8k)} 8-K filings since {since_date}"
        )

        leadership_8ks = 0
        for filing in filings_8k:
            result.filings_checked += 1

            # Quick check if likely contains leadership info
            has_leadership = await self.fetcher.search_8k_for_leadership(filing)

            if has_leadership:
                leadership_8ks += 1
                logger.debug(
                    f"[SECAgent] 8-K {filing.accession_number} has leadership content"
                )

                content = await self.fetcher.get_filing_content(filing)
                if content:
                    result_8k = await self.parser.parse_8k_filing(
                        content, company_name, filing.filing_date
                    )

                    for change in result_8k.changes:
                        change.source_url = filing.document_url
                        changes.append(change)

        logger.info(
            f"[SECAgent] 8-K extraction: {len(changes)} changes from {leadership_8ks} leadership 8-Ks"
        )

        return changes

    def _infer_title_level(self, title: str) -> TitleLevel:
        """Infer title level from title string."""
        if not title:
            return TitleLevel.UNKNOWN

        title_lower = title.lower()

        # C-Suite
        if any(kw in title_lower for kw in ["chief", "ceo", "cfo", "coo", "cto"]):
            return TitleLevel.C_SUITE

        # President
        if "president" in title_lower and "vice" not in title_lower:
            return TitleLevel.PRESIDENT

        # VP levels
        if "executive vice president" in title_lower or "evp" in title_lower:
            return TitleLevel.EVP
        if "senior vice president" in title_lower or "svp" in title_lower:
            return TitleLevel.SVP
        if "vice president" in title_lower or " vp " in f" {title_lower} ":
            return TitleLevel.VP

        # Director
        if "director" in title_lower:
            return TitleLevel.DIRECTOR

        return TitleLevel.UNKNOWN

    def _deduplicate_people(
        self,
        people: List[ExtractedPerson],
    ) -> List[ExtractedPerson]:
        """Deduplicate people by name."""
        seen = {}  # normalized_name -> person

        for person in people:
            name_key = self._normalize_name(person.full_name)

            if name_key in seen:
                # Merge info
                existing = seen[name_key]
                self._merge_person(existing, person)
            else:
                seen[name_key] = person

        return list(seen.values())

    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        if not name:
            return ""

        # Lowercase and remove punctuation
        name = name.lower()
        name = "".join(c for c in name if c.isalnum() or c.isspace())
        name = " ".join(name.split())

        return name

    def _merge_person(
        self,
        target: ExtractedPerson,
        source: ExtractedPerson,
    ) -> None:
        """Merge source person info into target."""
        # Fill in missing fields
        if not target.bio and source.bio:
            target.bio = source.bio

        # Combine board/executive flags
        if source.is_board_member:
            target.is_board_member = True
        if source.is_executive:
            target.is_executive = True

        # Use higher confidence
        if source.confidence == ExtractionConfidence.HIGH:
            target.confidence = ExtractionConfidence.HIGH

    def _deduplicate_changes(
        self,
        changes: List[LeadershipChange],
    ) -> List[LeadershipChange]:
        """Deduplicate leadership changes."""
        seen = set()
        unique = []

        for change in changes:
            # Create a key for comparison
            key = (
                self._normalize_name(change.person_name),
                change.change_type.value
                if hasattr(change.change_type, "value")
                else str(change.change_type),
                str(change.announced_date or change.effective_date or ""),
            )

            if key not in seen:
                seen.add(key)
                unique.append(change)

        return unique

    def _finalize_result(self, result: CollectionResult) -> CollectionResult:
        """Finalize collection result with timing."""
        result.completed_at = datetime.utcnow()
        result.duration_seconds = (
            result.completed_at - result.started_at
        ).total_seconds()
        return result


async def collect_company_sec(
    company_id: int,
    company_name: str,
    cik: str,
) -> CollectionResult:
    """
    Convenience function to collect SEC data for a company.

    Args:
        company_id: Database ID
        company_name: Company name
        cik: SEC CIK number

    Returns:
        CollectionResult with extracted data
    """
    async with SECAgent() as agent:
        return await agent.collect(company_id, company_name, cik)
