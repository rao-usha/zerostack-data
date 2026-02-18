"""
Family Office Collection Orchestrator.

Main entry point for running FO data collection jobs:
- Selects FOs based on filters and staleness
- Coordinates multiple collectors
- Tracks progress and handles errors
- Persists results to database
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any, Type

from sqlalchemy.orm import Session

from app.sources.family_office_collection.types import (
    FoCollectionConfig,
    FoCollectionResult,
    FoCollectedItem,
    FoCollectionSource,
    FoJobProgress,
    FoRegistryEntry,
)
from app.sources.family_office_collection.base_collector import FoBaseCollector
from app.sources.family_office_collection.website_source import FoWebsiteCollector
from app.sources.family_office_collection.news_source import FoNewsCollector
from app.sources.family_office_collection.deals_source import FoDealsCollector
from app.sources.family_office_collection.config import get_fo_registry

logger = logging.getLogger(__name__)


# Collector registry
FO_COLLECTORS: Dict[FoCollectionSource, Type[FoBaseCollector]] = {
    FoCollectionSource.WEBSITE: FoWebsiteCollector,
    FoCollectionSource.NEWS: FoNewsCollector,
    FoCollectionSource.DEALS: FoDealsCollector,
}


class FoCollectionOrchestrator:
    """
    Orchestrates Family Office data collection across multiple sources.

    Responsibilities:
    - Select FOs for collection based on filters
    - Instantiate and coordinate collectors
    - Handle rate limiting and concurrency
    - Track progress and persist results
    """

    def __init__(
        self,
        config: Optional[FoCollectionConfig] = None,
        db: Optional[Session] = None,
    ):
        """
        Initialize the orchestrator.

        Args:
            config: Collection configuration
            db: Database session for persistence (optional)
        """
        self.config = config or FoCollectionConfig()
        self.db = db
        self._collectors: Dict[FoCollectionSource, FoBaseCollector] = {}
        self._progress: Optional[FoJobProgress] = None

    def _get_collector(self, source: FoCollectionSource) -> FoBaseCollector:
        """Get or create a collector for the given source."""
        if source not in self._collectors:
            collector_class = FO_COLLECTORS.get(source)
            if not collector_class:
                raise ValueError(f"No collector available for source: {source}")
            self._collectors[source] = collector_class(
                rate_limit_delay=self.config.rate_limit_delay,
                max_retries=self.config.max_retries,
            )
        return self._collectors[source]

    def select_fos_for_collection(self) -> List[FoRegistryEntry]:
        """
        Select family offices for collection based on configuration.

        Returns:
            List of FoRegistryEntry to collect
        """
        entries = get_fo_registry()

        # Filter by specific FO ID(s) - not applicable for registry entries
        # Registry entries don't have IDs, so we skip this filter

        # Filter by FO type
        if self.config.fo_types:
            entries = [e for e in entries if e.fo_type in self.config.fo_types]

        # Filter by region
        if self.config.regions:
            entries = [e for e in entries if e.region in self.config.regions]

        # Sort by collection priority
        entries.sort(key=lambda x: x.collection_priority)

        return entries

    async def run_collection(self) -> Dict[str, Any]:
        """
        Run a complete collection job.

        Returns:
            Dictionary with job results
        """
        logger.info("Starting FO collection job")

        # Select FOs
        fos = self.select_fos_for_collection()

        if not fos:
            logger.info("No FOs to collect")
            return {
                "status": "success",
                "total_fos": 0,
                "completed_fos": 0,
                "results": [],
            }

        # Initialize progress tracking
        self._progress = FoJobProgress(
            job_id=0,
            total_fos=len(fos),
        )

        # Collect data
        all_results: List[FoCollectionResult] = []

        # Process FOs with concurrency limit
        semaphore = asyncio.Semaphore(self.config.max_concurrent_fos)

        async def collect_fo(fo: FoRegistryEntry, fo_id: int) -> None:
            async with semaphore:
                self._progress.current_fo = fo.name
                results = await self._collect_single_fo(fo, fo_id)
                all_results.extend(results)

                # Track progress
                self._progress.completed_fos += 1
                if all(r.success for r in results):
                    self._progress.successful_fos += 1
                else:
                    self._progress.failed_fos += 1

        # Run collection tasks
        tasks = [collect_fo(fo, i + 1) for i, fo in enumerate(fos)]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(
            f"Completed FO collection: "
            f"{self._progress.successful_fos}/{self._progress.total_fos} successful"
        )

        return {
            "status": "success" if self._progress.successful_fos > 0 else "failed",
            "total_fos": self._progress.total_fos,
            "completed_fos": self._progress.completed_fos,
            "successful_fos": self._progress.successful_fos,
            "failed_fos": self._progress.failed_fos,
            "total_items": sum(r.items_found for r in all_results),
            "results": [r.to_dict() for r in all_results[:100]],  # Limit
        }

    async def _collect_single_fo(
        self,
        fo: FoRegistryEntry,
        fo_id: int,
    ) -> List[FoCollectionResult]:
        """
        Collect data for a single family office from all configured sources.

        Args:
            fo: FO registry entry
            fo_id: Synthetic FO ID (position in list)

        Returns:
            List of FoCollectionResult (one per source)
        """
        results: List[FoCollectionResult] = []

        # Get database ID for this FO (for persistence)
        fo_db_id = self._get_fo_db_id(fo.name)

        for source in self.config.sources:
            try:
                collector = self._get_collector(source)
                result = await collector.collect(
                    fo_id=fo_id,
                    fo_name=fo.name,
                    website_url=fo.website_url,
                    principal_name=fo.principal_name,
                )

                # Persist items if we have a database session and FO exists
                if result.success and result.items and fo_db_id:
                    try:
                        persisted = self._persist_items(fo_db_id, result.items)
                        # Note: items_new is computed from item.is_new flags set during persist
                        logger.info(
                            f"Persisted {persisted} new items for {fo.name} from {source.value}"
                        )
                    except Exception as persist_error:
                        logger.error(
                            f"Error persisting items for {fo.name}: {persist_error}"
                        )
                        result.warnings.append(f"Failed to persist: {persist_error}")

                results.append(result)

            except Exception as e:
                logger.error(f"Error collecting {source.value} for {fo.name}: {e}")
                results.append(
                    FoCollectionResult(
                        fo_id=fo_id,
                        fo_name=fo.name,
                        source=source,
                        success=False,
                        error_message=str(e),
                    )
                )

        return results

    async def collect_single_fo(self, fo_name: str) -> List[FoCollectionResult]:
        """
        Convenience method to collect data for a single FO by name.

        Args:
            fo_name: Family office name

        Returns:
            List of FoCollectionResult
        """
        from app.sources.family_office_collection.config import get_fo_by_name

        fo = get_fo_by_name(fo_name)
        if not fo:
            raise ValueError(f"FO not found: {fo_name}")

        return await self._collect_single_fo(fo, fo_id=0)

    def get_progress(self) -> Optional[Dict[str, Any]]:
        """Get current progress if job is running."""
        if not self._progress:
            return None

        return {
            "total_fos": self._progress.total_fos,
            "completed_fos": self._progress.completed_fos,
            "successful_fos": self._progress.successful_fos,
            "failed_fos": self._progress.failed_fos,
            "current_fo": self._progress.current_fo,
            "progress_pct": self._progress.progress_pct,
            "is_complete": self._progress.is_complete,
        }

    def _persist_items(self, fo_db_id: int, items: List[FoCollectedItem]) -> int:
        """
        Persist collected items to the database.

        Args:
            fo_db_id: Family office database ID
            items: List of collected items

        Returns:
            Number of items persisted
        """
        if not self.db:
            logger.warning("No database session - items not persisted")
            return 0

        persisted = 0
        for item in items:
            try:
                if item.item_type == "deal":
                    if self._persist_deal(fo_db_id, item):
                        persisted += 1
                elif item.item_type == "contact":
                    if self._persist_contact(fo_db_id, item):
                        persisted += 1
                elif item.item_type == "news":
                    # News items don't persist to a separate table
                    pass
            except Exception as e:
                logger.error(f"Error persisting {item.item_type}: {e}")
                if self.db:
                    self.db.rollback()

        return persisted

    def _persist_deal(self, fo_db_id: int, item: FoCollectedItem) -> bool:
        """Persist a deal/investment item."""
        from app.core.family_office_models import FamilyOfficeInvestment

        data = item.data
        company_name = data.get("company_name")

        if not company_name:
            return False

        # Check for existing
        existing = (
            self.db.query(FamilyOfficeInvestment)
            .filter(
                FamilyOfficeInvestment.family_office_id == fo_db_id,
                FamilyOfficeInvestment.company_name == company_name,
            )
            .first()
        )

        if existing:
            # Update if we have more info
            if data.get("investment_amount_usd") and not existing.investment_amount_usd:
                existing.investment_amount_usd = str(data["investment_amount_usd"])
            if data.get("investment_stage") and not existing.investment_stage:
                existing.investment_stage = data["investment_stage"]
            if data.get("investment_type") and not existing.investment_type:
                existing.investment_type = data["investment_type"]
            if data.get("lead_investor") is not None:
                existing.lead_investor = data["lead_investor"]
            self.db.commit()
            item.is_new = False
            return False  # Not new

        # Create new investment record
        investment = FamilyOfficeInvestment(
            family_office_id=fo_db_id,
            company_name=company_name,
            company_website=data.get("company_website"),
            investment_date=self._parse_date(
                data.get("investment_date") or data.get("filing_date")
            ),
            investment_type=data.get("investment_type"),
            investment_stage=data.get("investment_stage"),
            investment_amount_usd=str(data["investment_amount_usd"])
            if data.get("investment_amount_usd")
            else None,
            lead_investor=data.get("lead_investor"),
            source_type=data.get("source_type", "news"),
            source_url=item.source_url,
            status="active",
        )
        self.db.add(investment)
        self.db.commit()
        item.is_new = True
        return True

    def _persist_contact(self, fo_db_id: int, item: FoCollectedItem) -> bool:
        """Persist a contact item."""
        from app.core.family_office_models import FamilyOfficeContact

        data = item.data
        name = data.get("full_name") or data.get("name")

        if not name:
            return False

        # Check for existing
        existing = (
            self.db.query(FamilyOfficeContact)
            .filter(
                FamilyOfficeContact.family_office_id == fo_db_id,
                FamilyOfficeContact.full_name == name,
            )
            .first()
        )

        if existing:
            # Update with new info
            if data.get("title") and not existing.title:
                existing.title = data["title"]
            if data.get("email") and not existing.email:
                existing.email = data["email"]
            if data.get("linkedin_url") and not existing.linkedin_url:
                existing.linkedin_url = data["linkedin_url"]
            self.db.commit()
            item.is_new = False
            return False

        # Create new contact
        contact = FamilyOfficeContact(
            family_office_id=fo_db_id,
            full_name=name,
            title=data.get("title"),
            role=data.get("role"),
            email=data.get("email"),
            phone=data.get("phone"),
            linkedin_url=data.get("linkedin_url"),
            bio=data.get("bio"),
            data_source=item.source_url,
            status="Active",
        )
        self.db.add(contact)
        self.db.commit()
        item.is_new = True
        return True

    def _parse_date(self, date_str: Optional[str]):
        """Parse a date string to date object."""
        if not date_str:
            return None
        try:
            from datetime import date

            if isinstance(date_str, date):
                return date_str
            # Try ISO format
            if "T" in date_str:
                date_str = date_str.split("T")[0]
            parts = date_str.split("-")
            if len(parts) == 3:
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            pass
        return None

    def _get_fo_db_id(self, fo_name: str) -> Optional[int]:
        """Get database ID for a family office by name."""
        if not self.db:
            return None

        from app.core.family_office_models import FamilyOffice

        fo = self.db.query(FamilyOffice).filter(FamilyOffice.name == fo_name).first()

        return fo.id if fo else None
