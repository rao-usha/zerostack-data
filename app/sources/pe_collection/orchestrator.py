"""
PE Collection Orchestrator.

Coordinates data collection across multiple sources and entities.
Manages concurrency, progress tracking, and result aggregation.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type

from sqlalchemy.ext.asyncio import AsyncSession

from app.sources.pe_collection.types import (
    PECollectionConfig,
    PECollectionResult,
    PEJobProgress,
    EntityType,
    PECollectionSource,
)
from app.sources.pe_collection.base_collector import BasePECollector
from app.core.pe_models import PEFirm, PEPortfolioCompany, PEPerson, PEDeal

logger = logging.getLogger(__name__)


class PECollectionOrchestrator:
    """
    Orchestrates PE data collection across multiple sources and entities.

    Usage:
        orchestrator = PECollectionOrchestrator(db_session)
        results = await orchestrator.run_collection(config)
    """

    # Registry of collectors by source type
    _collectors: Dict[PECollectionSource, Type[BasePECollector]] = {}

    def __init__(self, db_session: Optional[AsyncSession] = None):
        """
        Initialize the orchestrator.

        Args:
            db_session: Database session for persisting results
        """
        self.db_session = db_session
        self._job_id: Optional[int] = None
        self._progress = PEJobProgress(job_id=0)
        self._results: List[PECollectionResult] = []

    @classmethod
    def register_collector(
        cls, source: PECollectionSource, collector_class: Type[BasePECollector]
    ):
        """
        Register a collector for a source type.

        Args:
            source: The collection source type
            collector_class: The collector class to use
        """
        cls._collectors[source] = collector_class
        logger.info(
            f"Registered collector for {source.value}: {collector_class.__name__}"
        )

    @classmethod
    def get_collector(
        cls, source: PECollectionSource
    ) -> Optional[Type[BasePECollector]]:
        """Get the registered collector for a source type."""
        return cls._collectors.get(source)

    async def run_collection(
        self,
        config: PECollectionConfig,
        entities: Optional[List[Dict[str, Any]]] = None,
    ) -> List[PECollectionResult]:
        """
        Run collection for the given configuration.

        Args:
            config: Collection configuration
            entities: List of entities to collect (if not using config filters)
                Each entity should have: id, name, website (optional)

        Returns:
            List of collection results
        """
        start_time = datetime.utcnow()
        self._results = []

        # Get entities to collect
        if entities is None:
            entities = await self._get_entities_from_db(config)

        if not entities:
            logger.warning("No entities to collect")
            return []

        # Initialize progress
        self._progress = PEJobProgress(
            job_id=self._job_id or 0,
            total_entities=len(entities) * len(config.sources),
        )

        logger.info(
            f"Starting PE collection: {len(entities)} entities, "
            f"{len(config.sources)} sources"
        )

        # Process entities with concurrency control
        semaphore = asyncio.Semaphore(config.max_concurrent)

        tasks = []
        for entity in entities:
            for source in config.sources:
                task = self._collect_entity_source(
                    entity=entity,
                    source=source,
                    config=config,
                    semaphore=semaphore,
                )
                tasks.append(task)

        # Run all tasks
        await asyncio.gather(*tasks)

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            f"PE collection complete: {self._progress.successful_entities} successful, "
            f"{self._progress.failed_entities} failed in {elapsed:.1f}s"
        )

        return self._results

    async def _collect_entity_source(
        self,
        entity: Dict[str, Any],
        source: PECollectionSource,
        config: PECollectionConfig,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """
        Collect data for a single entity from a single source.

        Args:
            entity: Entity data (id, name, website)
            source: Collection source
            config: Collection configuration
            semaphore: Concurrency semaphore
        """
        async with semaphore:
            entity_id = entity.get("id")
            entity_name = entity.get("name", f"Entity {entity_id}")
            website = entity.get("website")

            self._progress.current_entity = entity_name
            self._progress.current_source = source.value

            # Get collector class
            collector_class = self._collectors.get(source)
            if collector_class is None:
                logger.warning(f"No collector registered for source: {source.value}")
                self._progress.failed_entities += 1
                self._progress.completed_entities += 1
                return

            # Run collection
            try:
                collector = collector_class(
                    rate_limit_delay=config.rate_limit_delay,
                    max_retries=config.max_retries,
                )

                # Forward entity fields (cik, crd_number, ticker, etc.) to collectors
                extra_kwargs = {
                    k: v
                    for k, v in entity.items()
                    if k not in ("id", "name", "website") and v is not None
                }
                result = await collector.collect(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    website_url=website,
                    **extra_kwargs,
                )

                self._results.append(result)

                if result.success:
                    self._progress.successful_entities += 1
                    logger.info(
                        f"Collected {result.items_found} items from {source.value} "
                        f"for {entity_name}"
                    )
                else:
                    self._progress.failed_entities += 1
                    logger.warning(
                        f"Collection failed for {entity_name} from {source.value}: "
                        f"{result.error_message}"
                    )

            except Exception as e:
                logger.error(f"Error collecting {entity_name} from {source.value}: {e}")
                self._progress.failed_entities += 1

            finally:
                self._progress.completed_entities += 1

    async def _get_entities_from_db(
        self, config: PECollectionConfig
    ) -> List[Dict[str, Any]]:
        """
        Get entities to collect from the database based on config filters.

        Args:
            config: Collection configuration

        Returns:
            List of entity dictionaries
        """
        if self.db_session is None:
            logger.warning("No database session, cannot fetch entities")
            return []

        db = self.db_session

        def _to_entity(row, extra: dict = None) -> Dict[str, Any]:
            """Convert a model instance to an entity dict."""
            d = {
                "id": row.id,
                "name": getattr(row, "name", None) or getattr(row, "full_name", None),
            }
            if hasattr(row, "website"):
                d["website"] = row.website
            if hasattr(row, "cik"):
                d["cik"] = row.cik
            if hasattr(row, "crd_number"):
                d["crd_number"] = row.crd_number
            if hasattr(row, "ticker"):
                d["ticker"] = row.ticker
            if extra:
                d.update(extra)
            return d

        entities = []

        if config.entity_type == EntityType.FIRM:
            query = db.query(PEFirm)
            if config.firm_id:
                query = query.filter(PEFirm.id == config.firm_id)
            elif config.firm_ids:
                query = query.filter(PEFirm.id.in_(config.firm_ids))
            else:
                query = query.filter(PEFirm.status == "Active")
                if config.firm_types:
                    query = query.filter(PEFirm.firm_type.in_(config.firm_types))
            entities = [_to_entity(r) for r in query.all()]

        elif config.entity_type == EntityType.COMPANY:
            query = db.query(PEPortfolioCompany)
            if config.company_id:
                query = query.filter(PEPortfolioCompany.id == config.company_id)
            elif config.company_ids:
                query = query.filter(PEPortfolioCompany.id.in_(config.company_ids))
            else:
                query = query.filter(PEPortfolioCompany.status == "Active")
                if config.sectors:
                    query = query.filter(
                        PEPortfolioCompany.industry.in_(config.sectors)
                    )
            entities = [_to_entity(r) for r in query.all()]

        elif config.entity_type == EntityType.PERSON:
            query = db.query(PEPerson)
            if config.person_id:
                query = query.filter(PEPerson.id == config.person_id)
            elif config.person_ids:
                query = query.filter(PEPerson.id.in_(config.person_ids))
            else:
                query = query.filter(PEPerson.is_active.is_(True))
            entities = [_to_entity(r) for r in query.all()]

        elif config.entity_type == EntityType.DEAL:
            query = db.query(PEDeal)
            entities = [
                {"id": r.id, "name": r.deal_name or f"Deal {r.id}"} for r in query.all()
            ]

        logger.info(
            f"Fetched {len(entities)} {config.entity_type.value} entities from DB"
        )
        return entities

    @property
    def progress(self) -> PEJobProgress:
        """Get current progress."""
        return self._progress

    @property
    def results(self) -> List[PECollectionResult]:
        """Get collected results."""
        return self._results


# Convenience function for running collection
async def run_pe_collection(
    config: PECollectionConfig,
    entities: Optional[List[Dict[str, Any]]] = None,
    db_session: Optional[AsyncSession] = None,
) -> List[PECollectionResult]:
    """
    Run PE data collection.

    Args:
        config: Collection configuration
        entities: Optional list of entities to collect
        db_session: Optional database session

    Returns:
        List of collection results
    """
    orchestrator = PECollectionOrchestrator(db_session)
    return await orchestrator.run_collection(config, entities)
