"""
Deep Collection Orchestrator - Multi-phase deep people collection pipeline.

Wraps SEC EDGAR, deep website crawling, deep news scanning, and org chart
construction into a single callable pipeline for Fortune 500 companies.

Designed for companies like Prudential Financial with 40,000+ employees
spread across multiple web domains and subsidiaries.
"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.core.people_models import (
    IndustrialCompany,
    Person,
    CompanyPerson,
    PeopleCollectionJob,
)
from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    CollectionResult,
    ExtractionConfidence,
)

logger = logging.getLogger(__name__)


@dataclass
class DeepCollectionConfig:
    """Configuration for deep collection pipeline."""

    # Phase toggles
    run_sec: bool = True
    run_website: bool = True
    run_news: bool = True
    build_org_chart: bool = True

    # Website crawl settings
    seed_urls: Optional[List[str]] = None
    allowed_domains: Optional[List[str]] = None
    max_crawl_pages: int = 50
    max_crawl_depth: int = 3

    # News settings
    subsidiary_names: Optional[List[str]] = None
    newsroom_url: Optional[str] = None
    news_days_back: int = 1825  # 5 years

    # SEC settings
    form4_limit: int = 100

    # Org chart settings
    division_context: Optional[str] = None

    # Subsidiary inclusion
    include_subsidiaries: bool = True


@dataclass
class DeepCollectionResult:
    """Result of a deep collection run."""

    company_id: int
    company_name: str
    success: bool = False

    # Phase results
    sec_people: int = 0
    sec_changes: int = 0
    website_people: int = 0
    news_changes: int = 0

    # Totals after dedup
    total_people_found: int = 0
    total_people_created: int = 0
    total_people_updated: int = 0
    total_changes: int = 0

    # Org chart
    org_chart_built: bool = False
    org_chart_depth: int = 0
    org_chart_departments: List[str] = field(default_factory=list)

    # Timing
    duration_seconds: float = 0
    phase_durations: Dict[str, float] = field(default_factory=dict)

    # Errors
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company": {
                "id": self.company_id,
                "name": self.company_name,
            },
            "success": self.success,
            "phases": {
                "sec": {"people": self.sec_people, "changes": self.sec_changes},
                "website": {"people": self.website_people},
                "news": {"changes": self.news_changes},
            },
            "totals": {
                "people_found": self.total_people_found,
                "people_created": self.total_people_created,
                "people_updated": self.total_people_updated,
                "changes": self.total_changes,
            },
            "org_chart": {
                "built": self.org_chart_built,
                "depth": self.org_chart_depth,
                "departments": self.org_chart_departments,
            },
            "timing": {
                "total_seconds": round(self.duration_seconds, 1),
                "phases": {k: round(v, 1) for k, v in self.phase_durations.items()},
            },
            "errors": self.errors,
            "warnings": self.warnings,
        }


class DeepCollectionOrchestrator:
    """
    Orchestrates deep, multi-phase people collection for large companies.

    Pipeline:
    1. SEC EDGAR (proxy + 10-K + Form 4 + 8-K) -> authoritative people + changes
    2. Website deep crawl (BFS across multiple domains) -> additional people
    3. News deep scan (5-year lookback, multi-query) -> leadership changes
    4. Org chart construction (LLM-powered hierarchy inference)
    """

    def __init__(self, db_session: Optional[Session] = None):
        self._provided_session = db_session
        self._sec_agent = None
        self._deep_crawler = None
        self._news_agent = None

    def _get_session(self) -> Session:
        if self._provided_session:
            return self._provided_session
        SessionLocal = get_session_factory()
        return SessionLocal()

    async def deep_collect(
        self,
        company_id: int,
        config: Optional[DeepCollectionConfig] = None,
    ) -> DeepCollectionResult:
        """
        Run the full deep collection pipeline for a company.

        Args:
            company_id: Database company ID
            config: Pipeline configuration (uses defaults if None)

        Returns:
            DeepCollectionResult with comprehensive metrics
        """
        if config is None:
            config = DeepCollectionConfig()

        session = self._get_session()
        started_at = datetime.utcnow()

        # Get company
        company = (
            session.query(IndustrialCompany)
            .filter(IndustrialCompany.id == company_id)
            .first()
        )

        if not company:
            return DeepCollectionResult(
                company_id=company_id,
                company_name="Unknown",
                errors=[f"Company {company_id} not found"],
            )

        result = DeepCollectionResult(
            company_id=company_id,
            company_name=company.name,
        )

        logger.info(
            f"[DeepCollect] Starting deep collection for {company.name} (id={company_id})"
        )

        # Create collection job record
        job = PeopleCollectionJob(
            job_type="deep_collection",
            company_id=company_id,
            config={
                "type": "deep_collection",
                "run_sec": config.run_sec,
                "run_website": config.run_website,
                "run_news": config.run_news,
                "build_org_chart": config.build_org_chart,
                "include_subsidiaries": config.include_subsidiaries,
            },
            status="running",
            started_at=started_at,
        )
        session.add(job)
        session.commit()

        all_people: List[ExtractedPerson] = []
        all_changes: List[LeadershipChange] = []

        try:
            # Phase 1: SEC EDGAR
            if config.run_sec and company.cik:
                phase_start = datetime.utcnow()
                logger.info(f"[DeepCollect] Phase 1: SEC EDGAR (CIK={company.cik})")

                sec_result = await self._run_sec_phase(company, config)
                all_people.extend(sec_result.get("people", []))
                all_changes.extend(sec_result.get("changes", []))
                result.sec_people = len(sec_result.get("people", []))
                result.sec_changes = len(sec_result.get("changes", []))
                result.errors.extend(sec_result.get("errors", []))

                result.phase_durations["sec"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()
                logger.info(
                    f"[DeepCollect] Phase 1 complete: {result.sec_people} people, "
                    f"{result.sec_changes} changes"
                )
            elif config.run_sec:
                result.warnings.append("SEC phase skipped: no CIK configured")

            # Phase 2: Website deep crawl
            if config.run_website:
                phase_start = datetime.utcnow()
                logger.info("[DeepCollect] Phase 2: Website deep crawl")

                website_result = await self._run_website_phase(company, config)
                all_people.extend(website_result.get("people", []))
                result.website_people = len(website_result.get("people", []))
                result.errors.extend(website_result.get("errors", []))

                result.phase_durations["website"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()
                logger.info(
                    f"[DeepCollect] Phase 2 complete: {result.website_people} people"
                )

            # Phase 3: News deep scan
            if config.run_news:
                phase_start = datetime.utcnow()
                logger.info("[DeepCollect] Phase 3: News deep scan")

                news_result = await self._run_news_phase(company, config)
                all_changes.extend(news_result.get("changes", []))
                result.news_changes = len(news_result.get("changes", []))
                result.errors.extend(news_result.get("errors", []))

                result.phase_durations["news"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()
                logger.info(
                    f"[DeepCollect] Phase 3 complete: {result.news_changes} changes"
                )

            # Store people and changes
            logger.info(
                f"[DeepCollect] Storing {len(all_people)} people, {len(all_changes)} changes"
            )
            from app.sources.people_collection.orchestrator import (
                PeopleCollectionOrchestrator,
            )

            base_orchestrator = PeopleCollectionOrchestrator(db_session=session)
            stored = await base_orchestrator._store_people(all_people, company, session)
            result.total_people_created = stored["created"]
            result.total_people_updated = stored["updated"]
            result.total_people_found = len(all_people)

            await base_orchestrator._store_changes(all_changes, company, session)
            result.total_changes = len(all_changes)

            # Update company metadata
            company.last_crawled_date = date.today()
            company.leadership_last_updated = date.today()
            session.commit()

            # Phase 4: Org chart construction
            if (
                config.build_org_chart
                and (result.total_people_created + result.total_people_updated) > 0
            ):
                phase_start = datetime.utcnow()
                logger.info("[DeepCollect] Phase 4: Org chart construction")

                org_result = await self._run_org_chart_phase(
                    company_id, company.name, session, config
                )
                result.org_chart_built = not org_result.get("error")
                result.org_chart_depth = org_result.get("max_depth", 0)
                result.org_chart_departments = org_result.get("departments", [])

                result.phase_durations["org_chart"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()
                logger.info(
                    f"[DeepCollect] Phase 4 complete: depth={result.org_chart_depth}, "
                    f"departments={len(result.org_chart_departments)}"
                )

            result.success = True

            # Update job
            job.status = "success"
            job.completed_at = datetime.utcnow()
            job.people_found = result.total_people_found
            job.people_created = result.total_people_created
            job.people_updated = result.total_people_updated
            job.changes_detected = result.total_changes
            session.commit()

        except Exception as e:
            logger.exception(f"[DeepCollect] Error in deep collection: {e}")
            result.errors.append(str(e))
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.errors = result.errors
            session.commit()

        finally:
            await self._close_agents()
            if not self._provided_session:
                session.close()

        result.duration_seconds = (datetime.utcnow() - started_at).total_seconds()

        logger.info(
            f"[DeepCollect] Deep collection complete for {company.name}: "
            f"people={result.total_people_found}, changes={result.total_changes}, "
            f"org_chart={result.org_chart_built}, "
            f"duration={result.duration_seconds:.0f}s"
        )

        return result

    async def _run_sec_phase(
        self,
        company: IndustrialCompany,
        config: DeepCollectionConfig,
    ) -> Dict[str, Any]:
        """Run SEC EDGAR collection phase."""
        try:
            from app.sources.people_collection.sec_agent import SECAgent

            if self._sec_agent is None:
                self._sec_agent = SECAgent()

            sec_result = await self._sec_agent.collect(
                company_id=company.id,
                company_name=company.name,
                cik=company.cik,
                include_8k=True,
                include_form4=True,
            )

            return {
                "people": sec_result.extracted_people,
                "changes": sec_result.extracted_changes,
                "errors": sec_result.errors,
            }

        except Exception as e:
            logger.error(f"[DeepCollect] SEC phase error: {e}")
            return {"people": [], "changes": [], "errors": [str(e)]}

    async def _run_website_phase(
        self,
        company: IndustrialCompany,
        config: DeepCollectionConfig,
    ) -> Dict[str, Any]:
        """Run deep website crawl phase."""
        try:
            from app.sources.people_collection.deep_crawler import DeepCrawler

            if self._deep_crawler is None:
                self._deep_crawler = DeepCrawler()

            # Build seed URLs
            seed_urls = config.seed_urls or []
            allowed_domains = config.allowed_domains or []

            # Auto-generate from company website if not provided
            if not seed_urls and company.website:
                from urllib.parse import urlparse

                website = company.website
                if not website.startswith("http"):
                    website = "https://" + website

                domain = urlparse(website).netloc
                if not domain:
                    domain = (
                        website.replace("https://", "")
                        .replace("http://", "")
                        .split("/")[0]
                    )

                # Ensure www. prefix for domains that need it
                www_domain = domain if domain.startswith("www.") else f"www.{domain}"

                seed_urls = [
                    f"https://{www_domain}",
                    f"https://{www_domain}/about",
                    f"https://{www_domain}/about/leadership",
                    f"https://{www_domain}/about/management",
                    f"https://{www_domain}/leadership",
                    f"https://{www_domain}/team",
                ]

                if not allowed_domains:
                    bare_domain = domain.lstrip("www.")
                    allowed_domains = [bare_domain, f"www.{bare_domain}"]

            # Add leadership page URL if known
            if company.leadership_page_url:
                seed_urls.append(company.leadership_page_url)

            # Add newsroom URL if known
            if company.newsroom_url:
                seed_urls.append(company.newsroom_url)

            if not seed_urls:
                return {"people": [], "errors": ["No seed URLs or website configured"]}

            crawl_result = await self._deep_crawler.crawl(
                company_id=company.id,
                company_name=company.name,
                seed_urls=seed_urls,
                allowed_domains=allowed_domains,
                max_pages=config.max_crawl_pages,
                max_depth=config.max_crawl_depth,
            )

            return {
                "people": crawl_result.extracted_people,
                "errors": crawl_result.errors,
            }

        except Exception as e:
            logger.error(f"[DeepCollect] Website phase error: {e}")
            return {"people": [], "errors": [str(e)]}

    async def _run_news_phase(
        self,
        company: IndustrialCompany,
        config: DeepCollectionConfig,
    ) -> Dict[str, Any]:
        """Run deep news collection phase."""
        try:
            from app.sources.people_collection.news_agent import NewsAgent

            if self._news_agent is None:
                self._news_agent = NewsAgent()

            news_result = await self._news_agent.deep_collect(
                company_id=company.id,
                company_name=company.name,
                subsidiary_names=config.subsidiary_names,
                website_url=company.website,
                newsroom_url=config.newsroom_url or company.newsroom_url,
                days_back=config.news_days_back,
            )

            return {
                "changes": news_result.extracted_changes,
                "errors": news_result.errors,
            }

        except Exception as e:
            logger.error(f"[DeepCollect] News phase error: {e}")
            return {"changes": [], "errors": [str(e)]}

    async def _run_org_chart_phase(
        self,
        company_id: int,
        company_name: str,
        db_session: Session,
        config: DeepCollectionConfig,
    ) -> Dict[str, Any]:
        """Run org chart construction phase."""
        try:
            from app.sources.people_collection.org_chart_builder import OrgChartBuilder

            builder = OrgChartBuilder()
            return await builder.build_org_chart(
                company_id=company_id,
                company_name=company_name,
                db_session=db_session,
                division_context=config.division_context,
            )

        except Exception as e:
            logger.error(f"[DeepCollect] Org chart phase error: {e}")
            return {"error": str(e)}

    async def _close_agents(self):
        """Close all agent HTTP sessions."""
        for agent in [self._sec_agent, self._deep_crawler, self._news_agent]:
            if agent:
                try:
                    await agent.close()
                except Exception:
                    pass
        self._sec_agent = None
        self._deep_crawler = None
        self._news_agent = None
