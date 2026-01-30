"""
SEC Filing collection agent.

Collects executive and board data from SEC EDGAR filings:
- DEF 14A proxy statements for executives and board
- 8-K filings for leadership changes
- 10-K for additional executive info
"""

import asyncio
import logging
from typing import Optional, List
from datetime import datetime, date, timedelta

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.filing_fetcher import FilingFetcher, SECFiling
from app.sources.people_collection.sec_parser import SECParser, ProxyParseResult
from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    CollectionResult,
    ExtractionConfidence,
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
        cik: str,
        include_8k: bool = True,
        days_back: int = 365,
    ) -> CollectionResult:
        """
        Collect leadership data from SEC filings.

        Args:
            company_id: Database ID of the company
            company_name: Name of the company
            cik: SEC CIK number
            include_8k: Whether to check 8-K filings for changes
            days_back: How far back to look for 8-K filings

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

        if not cik:
            result.errors.append("No CIK provided")
            result.success = False
            return self._finalize_result(result)

        try:
            all_people: List[ExtractedPerson] = []
            all_changes: List[LeadershipChange] = []

            # 1. Get and parse latest proxy statement
            logger.info(f"Fetching proxy statement for {company_name} (CIK: {cik})")
            proxy = await self.fetcher.get_latest_proxy(cik)

            if proxy:
                logger.debug(f"Found proxy from {proxy.filing_date}: {proxy.accession_number}")
                proxy_content = await self.fetcher.get_filing_content(proxy)

                if proxy_content:
                    proxy_result = await self.parser.parse_proxy_statement(
                        proxy_content, company_name, cik
                    )

                    # Add source info
                    for person in proxy_result.executives:
                        person.source_url = proxy.document_url
                    for person in proxy_result.board_members:
                        person.source_url = proxy.document_url

                    all_people.extend(proxy_result.executives)
                    all_people.extend(proxy_result.board_members)

                    logger.info(
                        f"Extracted from proxy: {len(proxy_result.executives)} executives, "
                        f"{len(proxy_result.board_members)} board members"
                    )
                else:
                    result.warnings.append("Failed to fetch proxy content")
            else:
                result.warnings.append("No proxy statement found")

            # 2. Check recent 8-K filings for leadership changes
            if include_8k:
                since_date = date.today() - timedelta(days=days_back)
                logger.info(f"Checking 8-K filings since {since_date}")

                filings_8k = await self.fetcher.get_recent_8ks(cik, since_date, limit=10)
                logger.debug(f"Found {len(filings_8k)} 8-K filings")

                for filing in filings_8k:
                    # Quick check if likely contains leadership info
                    has_leadership = await self.fetcher.search_8k_for_leadership(filing)

                    if has_leadership:
                        logger.debug(f"8-K {filing.accession_number} has leadership content")
                        content = await self.fetcher.get_filing_content(filing)

                        if content:
                            result_8k = await self.parser.parse_8k_filing(
                                content, company_name, filing.filing_date
                            )

                            for change in result_8k.changes:
                                change.source_url = filing.document_url
                                all_changes.append(change)

            # 3. Deduplicate people
            unique_people = self._deduplicate_people(all_people)

            # 4. Deduplicate changes
            unique_changes = self._deduplicate_changes(all_changes)

            # Update result
            result.extracted_people = unique_people
            result.extracted_changes = unique_changes
            result.people_found = len(unique_people)
            result.changes_detected = len(unique_changes)
            result.success = True

            logger.info(
                f"SEC collection for {company_name}: "
                f"{result.people_found} people, {result.changes_detected} changes"
            )

        except Exception as e:
            logger.exception(f"Error collecting SEC data for {company_name}: {e}")
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

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
        name = ''.join(c for c in name if c.isalnum() or c.isspace())
        name = ' '.join(name.split())

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
                change.change_type.value if hasattr(change.change_type, 'value') else str(change.change_type),
                str(change.announced_date or change.effective_date or ""),
            )

            if key not in seen:
                seen.add(key)
                unique.append(change)

        return unique

    def _finalize_result(self, result: CollectionResult) -> CollectionResult:
        """Finalize collection result with timing."""
        result.completed_at = datetime.utcnow()
        result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
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
