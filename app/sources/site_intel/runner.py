"""
Site Intelligence Platform - Collection Orchestrator.

Coordinates data collection across all domains and sources.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Type

from sqlalchemy.orm import Session

from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.base_collector import BaseCollector

logger = logging.getLogger(__name__)


# Registry of available collectors
# Will be populated as collectors are implemented
COLLECTOR_REGISTRY: Dict[SiteIntelSource, Type[BaseCollector]] = {}


def register_collector(source: SiteIntelSource):
    """Decorator to register a collector class."""

    def decorator(cls: Type[BaseCollector]):
        COLLECTOR_REGISTRY[source] = cls
        return cls

    return decorator


class SiteIntelOrchestrator:
    """
    Orchestrates site intelligence data collection.

    Usage:
        orchestrator = SiteIntelOrchestrator(db)

        # Collect from a specific source
        result = await orchestrator.collect(
            domain=SiteIntelDomain.POWER,
            source=SiteIntelSource.EIA,
        )

        # Collect all sources in a domain
        results = await orchestrator.collect_domain(SiteIntelDomain.POWER)

        # Full sync across all domains
        results = await orchestrator.full_sync()
    """

    def __init__(self, db: Session, api_keys: Optional[Dict[str, str]] = None):
        """
        Initialize the orchestrator.

        Args:
            db: SQLAlchemy database session
            api_keys: Dictionary of API keys by source name
        """
        self.db = db
        self.api_keys = api_keys or {}

    def get_collector(
        self,
        source: SiteIntelSource,
        api_key: Optional[str] = None,
    ) -> Optional[BaseCollector]:
        """
        Get a collector instance for a source.

        Args:
            source: Data source identifier
            api_key: Optional API key override

        Returns:
            Collector instance or None if not registered
        """
        collector_cls = COLLECTOR_REGISTRY.get(source)
        if collector_cls is None:
            logger.warning(f"No collector registered for source: {source}")
            return None

        key = api_key or self.api_keys.get(source.value)
        return collector_cls(db=self.db, api_key=key)

    async def collect(
        self,
        domain: SiteIntelDomain,
        source: SiteIntelSource,
        config: Optional[CollectionConfig] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Execute collection for a specific source.

        Args:
            domain: Data domain
            source: Data source
            config: Collection configuration (created if not provided)
            **kwargs: Additional config options

        Returns:
            CollectionResult with statistics
        """
        if config is None:
            config = CollectionConfig(
                domain=domain,
                source=source,
                **kwargs,
            )

        collector = self.get_collector(source)
        if collector is None:
            return CollectionResult(
                status=CollectionStatus.FAILED,
                domain=domain,
                source=source,
                error_message=f"No collector available for {source.value}",
            )

        try:
            logger.info(f"Starting collection: {domain.value}/{source.value}")
            bridge = kwargs.pop("bridge_to_ingestion", False)
            collector.create_job(config, bridge_to_ingestion=bridge)
            collector.start_job()

            result = await collector.collect(config)

            collector.complete_job(result)

            # Auto-update watermark on success
            if result.status == CollectionStatus.SUCCESS:
                collector.update_watermark(
                    datetime.utcnow(),
                    records=result.inserted_items,
                )

            logger.info(
                f"Completed {domain.value}/{source.value}: "
                f"{result.inserted_items} inserted, {result.updated_items} updated"
            )

            return result

        except Exception as e:
            logger.error(f"Collection failed for {source.value}: {e}", exc_info=True)
            error_result = CollectionResult(
                status=CollectionStatus.FAILED,
                domain=domain,
                source=source,
                error_message=str(e),
            )
            # Rollback any failed transaction before recording the error
            try:
                self.db.rollback()
                collector.complete_job(error_result)
            except Exception as job_err:
                logger.error(
                    f"Could not record job failure for {source.value}: {job_err}"
                )
                try:
                    self.db.rollback()
                except Exception:
                    pass
            return error_result

        finally:
            await collector.close_client()

    async def collect_domain(
        self,
        domain: SiteIntelDomain,
        sources: Optional[List[SiteIntelSource]] = None,
        **kwargs,
    ) -> Dict[SiteIntelSource, CollectionResult]:
        """
        Collect from all sources in a domain.

        Args:
            domain: Data domain to collect
            sources: Specific sources to collect (None = all in domain)
            **kwargs: Collection configuration options

        Returns:
            Dictionary of results by source
        """
        results = {}

        # Get sources for domain, filtered to only registered collectors
        domain_sources = self.get_sources_for_domain(domain)
        registered_sources = [s for s in domain_sources if s in COLLECTOR_REGISTRY]
        if sources:
            registered_sources = [s for s in registered_sources if s in sources]

        skipped = [s.value for s in domain_sources if s not in COLLECTOR_REGISTRY]
        if skipped:
            logger.info(
                f"Skipping unregistered collectors for {domain.value}: {skipped}"
            )

        # Publish domain_started SSE event
        self._publish_event(
            "domain_started",
            {
                "domain": domain.value,
                "sources": [s.value for s in registered_sources],
                "total_sources": len(registered_sources),
            },
        )

        # Run all sources in the domain concurrently
        async def _collect_one(src):
            return src, await self.collect(domain, src, **kwargs)

        collected = await asyncio.gather(
            *[_collect_one(src) for src in registered_sources],
            return_exceptions=True,
        )
        for item in collected:
            if isinstance(item, Exception):
                logger.error(f"Concurrent collect error in {domain.value}: {item}")
                continue
            src, result = item
            results[src] = result

        # Publish domain_completed SSE event
        success_count = sum(
            1 for r in results.values() if r.status == CollectionStatus.SUCCESS
        )
        self._publish_event(
            "domain_completed",
            {
                "domain": domain.value,
                "total_sources": len(registered_sources),
                "success": success_count,
                "failed": len(registered_sources) - success_count,
            },
        )

        return results

    async def full_sync(
        self,
        domains: Optional[List[SiteIntelDomain]] = None,
        **kwargs,
    ) -> Dict[SiteIntelDomain, Dict[SiteIntelSource, CollectionResult]]:
        """
        Execute full sync across all domains.

        Args:
            domains: Specific domains to sync (None = all)
            **kwargs: Collection configuration options

        Returns:
            Nested dictionary of results by domain and source
        """
        results = {}

        target_domains = domains or list(SiteIntelDomain)
        # Filter out SCORING (no data collectors)
        target_domains = [d for d in target_domains if d != SiteIntelDomain.SCORING]

        # Run all domains concurrently
        async def _sync_domain(domain):
            logger.info(f"Starting full sync for domain: {domain.value}")
            domain_results = await self.collect_domain(domain, **kwargs)
            return domain, domain_results

        collected = await asyncio.gather(
            *[_sync_domain(d) for d in target_domains],
            return_exceptions=True,
        )
        for item in collected:
            if isinstance(item, Exception):
                logger.error(f"Full sync domain error: {item}")
                continue
            domain, domain_results = item
            results[domain] = domain_results

        return results

    def _publish_event(self, event_type: str, data: dict):
        """Publish an SSE event to the event bus (best-effort)."""
        try:
            from app.core.event_bus import EventBus

            EventBus.publish("collection_all", event_type, data)
        except Exception:
            pass

    async def collect_with_plan(
        self,
        plan: List[Dict[str, Any]],
    ) -> Dict[str, CollectionResult]:
        """
        Execute collection with dependency ordering.

        Each item in the plan is {"domain": str, "depends_on": [str]}.
        Domains with no dependencies run first; dependents wait.

        Args:
            plan: List of {"domain": "...", "depends_on": ["..."]}

        Returns:
            Dict of domain -> CollectionResult
        """
        results = {}
        completed_domains = set()

        # Build adjacency: domain -> set of dependencies
        dep_map = {item["domain"]: set(item.get("depends_on", [])) for item in plan}

        while len(completed_domains) < len(plan):
            # Find domains whose dependencies are all completed
            ready = [
                d
                for d, deps in dep_map.items()
                if d not in completed_domains and deps.issubset(completed_domains)
            ]

            if not ready:
                # Deadlock or circular dependency
                remaining = set(dep_map.keys()) - completed_domains
                logger.error(f"Dependency deadlock: {remaining}")
                break

            for domain_name in ready:
                try:
                    domain = SiteIntelDomain(domain_name)
                    domain_results = await self.collect_domain(
                        domain, bridge_to_ingestion=True
                    )
                    for source, result in domain_results.items():
                        results[f"{domain_name}/{source.value}"] = result
                except Exception as e:
                    logger.error(f"Failed to collect domain {domain_name}: {e}")
                completed_domains.add(domain_name)

        return results

    def get_sources_for_domain(self, domain: SiteIntelDomain) -> List[SiteIntelSource]:
        """Get all sources for a domain."""
        source_map = {
            SiteIntelDomain.POWER: [
                SiteIntelSource.EIA,
                SiteIntelSource.NREL,
                SiteIntelSource.HIFLD,
                SiteIntelSource.ISO_PJM,
                SiteIntelSource.ISO_CAISO,
                SiteIntelSource.ISO_ERCOT,
            ],
            SiteIntelDomain.TELECOM: [
                SiteIntelSource.FCC,
                SiteIntelSource.PEERINGDB,
                SiteIntelSource.TELEGEOGRAPHY,
            ],
            SiteIntelDomain.TRANSPORT: [
                SiteIntelSource.BTS,
                SiteIntelSource.BTS_NTAD,
                SiteIntelSource.FRA,
                SiteIntelSource.USACE,
                SiteIntelSource.FAA,
                SiteIntelSource.FHWA,
            ],
            SiteIntelDomain.LABOR: [
                SiteIntelSource.BLS,
                SiteIntelSource.BLS_OES,
                SiteIntelSource.BLS_QCEW,
                SiteIntelSource.CENSUS_LEHD,
                SiteIntelSource.CENSUS_ACS,
            ],
            SiteIntelDomain.RISK: [
                SiteIntelSource.FEMA,
                SiteIntelSource.FEMA_NFHL,
                SiteIntelSource.USGS_EARTHQUAKE,
                SiteIntelSource.NOAA_CLIMATE,
                SiteIntelSource.EPA_ENVIROFACTS,
                SiteIntelSource.USFWS_NWI,
            ],
            SiteIntelDomain.INCENTIVES: [
                SiteIntelSource.CDFI_OZ,
                SiteIntelSource.FTZ_BOARD,
                SiteIntelSource.GOOD_JOBS_FIRST,
                SiteIntelSource.STATE_EDO,
            ],
            SiteIntelDomain.LOGISTICS: [
                SiteIntelSource.FREIGHTOS,
                SiteIntelSource.USDA_AMS,
                SiteIntelSource.FMCSA,
                SiteIntelSource.DREWRY,
                SiteIntelSource.SCFI,
                SiteIntelSource.USACE,
                SiteIntelSource.BTS_CARGO,
                SiteIntelSource.CENSUS_TRADE,
                SiteIntelSource.LOOPNET,
                SiteIntelSource.TRANSPORT_TOPICS,
                SiteIntelSource.THREE_PL_WEBSITE,
                SiteIntelSource.THREE_PL_SEC,
                SiteIntelSource.THREE_PL_FMCSA,
            ],
            SiteIntelDomain.WATER_UTILITIES: [
                SiteIntelSource.USGS_WATER,
                SiteIntelSource.EPA_SDWIS,
                SiteIntelSource.EIA_GAS,
                SiteIntelSource.OPENEI_URDB,
            ],
            SiteIntelDomain.SCORING: [],
        }
        return source_map.get(domain, [])

    def get_available_collectors(self) -> Dict[str, List[str]]:
        """Get list of available (registered) collectors by domain."""
        available = {}
        for domain in SiteIntelDomain:
            sources = self.get_sources_for_domain(domain)
            registered = [s.value for s in sources if s in COLLECTOR_REGISTRY]
            if registered:
                available[domain.value] = registered
        return available

    def get_collection_status(self) -> Dict[str, Any]:
        """Get overall collection status and statistics."""
        from sqlalchemy import func
        from app.core.models_site_intel import SiteIntelCollectionJob

        # Get recent job counts by status
        status_counts = dict(
            self.db.query(
                SiteIntelCollectionJob.status, func.count(SiteIntelCollectionJob.id)
            )
            .filter(
                SiteIntelCollectionJob.created_at
                >= datetime.utcnow().replace(hour=0, minute=0, second=0)
            )
            .group_by(SiteIntelCollectionJob.status)
            .all()
        )

        # Get latest job by domain
        latest_by_domain = {}
        for domain in SiteIntelDomain:
            latest = (
                self.db.query(SiteIntelCollectionJob)
                .filter(SiteIntelCollectionJob.domain == domain.value)
                .order_by(SiteIntelCollectionJob.created_at.desc())
                .first()
            )
            if latest:
                latest_by_domain[domain.value] = {
                    "status": latest.status,
                    "source": latest.source,
                    "completed_at": latest.completed_at.isoformat()
                    if latest.completed_at
                    else None,
                    "inserted": latest.inserted_items,
                }

        return {
            "available_collectors": self.get_available_collectors(),
            "today_jobs": status_counts,
            "latest_by_domain": latest_by_domain,
        }
