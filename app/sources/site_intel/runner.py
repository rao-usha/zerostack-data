"""
Site Intelligence Platform - Collection Orchestrator.

Coordinates data collection across all domains and sources.
"""
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
            job = collector.create_job(config)
            collector.start_job()

            result = await collector.collect(config)

            collector.complete_job(result)
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
            collector.complete_job(error_result)
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

        # Get sources for domain
        domain_sources = self.get_sources_for_domain(domain)
        if sources:
            domain_sources = [s for s in domain_sources if s in sources]

        for source in domain_sources:
            result = await self.collect(domain, source, **kwargs)
            results[source] = result

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

        for domain in target_domains:
            if domain == SiteIntelDomain.SCORING:
                # Skip scoring domain - it doesn't have data collectors
                continue

            logger.info(f"Starting full sync for domain: {domain.value}")
            domain_results = await self.collect_domain(domain, **kwargs)
            results[domain] = domain_results

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
                SiteIntelSource.BTS_NTAD,
                SiteIntelSource.FRA,
                SiteIntelSource.USACE,
                SiteIntelSource.FAA,
                SiteIntelSource.FHWA,
            ],
            SiteIntelDomain.LABOR: [
                SiteIntelSource.BLS_OES,
                SiteIntelSource.BLS_QCEW,
                SiteIntelSource.CENSUS_LEHD,
                SiteIntelSource.CENSUS_ACS,
            ],
            SiteIntelDomain.RISK: [
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
                SiteIntelCollectionJob.status,
                func.count(SiteIntelCollectionJob.id)
            )
            .filter(SiteIntelCollectionJob.created_at >= datetime.utcnow().replace(hour=0, minute=0, second=0))
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
                    "completed_at": latest.completed_at.isoformat() if latest.completed_at else None,
                    "inserted": latest.inserted_items,
                }

        return {
            "available_collectors": self.get_available_collectors(),
            "today_jobs": status_counts,
            "latest_by_domain": latest_by_domain,
        }
