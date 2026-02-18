"""
Recursive Corporate Structure Discovery & Deep People Collection.

Top-level orchestrator that:
1. Discovers corporate structure (subsidiaries, divisions, affiliates)
2. Runs DeepCollectionOrchestrator for each business unit
3. Discovers additional people via LinkedIn Google search
4. Builds functional org maps (technology, finance, etc.)
5. Constructs master cross-subsidiary org chart

Designed as a general-purpose system reusable for any Fortune 500 company.
"""

import asyncio
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.core.people_models import (
    IndustrialCompany,
    PeopleCollectionJob,
)

logger = logging.getLogger(__name__)


@dataclass
class RecursiveCollectConfig:
    """Configuration for recursive collection pipeline."""

    # Structure discovery
    discover_structure: bool = True
    max_units: int = 25

    # Per-unit collection
    run_sec_per_unit: bool = True
    run_website_per_unit: bool = True
    run_news_per_unit: bool = False  # Slow, skip by default
    max_crawl_pages_per_unit: int = 20

    # LinkedIn discovery
    run_linkedin: bool = True
    max_linkedin_searches: int = 100  # Total across all units

    # Functional org mapping
    map_functions: List[str] = field(default_factory=lambda: ["technology"])
    function_depth: int = 3

    # Org chart
    build_master_org_chart: bool = True


@dataclass
class RecursiveCollectResult:
    """Result of a recursive collection run."""

    company_id: int
    company_name: str
    success: bool = False

    # Structure discovery
    units_discovered: int = 0
    unit_names: List[str] = field(default_factory=list)

    # Per-unit collection
    units_collected: int = 0
    per_unit_results: Dict[str, Dict] = field(default_factory=dict)

    # LinkedIn discovery
    linkedin_people: int = 0

    # Functional org mapping
    functional_maps: Dict[str, Dict] = field(default_factory=dict)

    # Master org chart
    master_org_chart_built: bool = False
    master_org_chart_depth: int = 0

    # Totals
    total_people_found: int = 0
    total_people_created: int = 0
    total_people_updated: int = 0
    total_changes: int = 0

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
            "structure": {
                "units_discovered": self.units_discovered,
                "unit_names": self.unit_names,
            },
            "collection": {
                "units_collected": self.units_collected,
                "per_unit": self.per_unit_results,
            },
            "linkedin": {
                "people_found": self.linkedin_people,
            },
            "functional_org": self.functional_maps,
            "org_chart": {
                "built": self.master_org_chart_built,
                "depth": self.master_org_chart_depth,
            },
            "totals": {
                "people_found": self.total_people_found,
                "people_created": self.total_people_created,
                "people_updated": self.total_people_updated,
                "changes": self.total_changes,
            },
            "timing": {
                "total_seconds": round(self.duration_seconds, 1),
                "phases": {k: round(v, 1) for k, v in self.phase_durations.items()},
            },
            "errors": self.errors,
            "warnings": self.warnings,
        }


class RecursiveCollector:
    """
    Top-level orchestrator for recursive corporate structure discovery
    and deep people collection.

    Pipeline:
    1. Discover corporate structure          (StructureDiscoveryAgent)
    2. For EACH business unit:
       a. Run DeepCollectionOrchestrator     (SEC + website + news + org chart)
       b. Run LinkedIn discovery             (LinkedInDiscovery)
    3. Build functional org maps             (FunctionalOrgMapper)
    4. Build master org chart                (OrgChartBuilder)
    """

    def __init__(self, db_session: Optional[Session] = None):
        self._provided_session = db_session

    def _get_session(self) -> Session:
        if self._provided_session:
            return self._provided_session
        SessionLocal = get_session_factory()
        return SessionLocal()

    async def collect(
        self,
        company_id: int,
        config: Optional[RecursiveCollectConfig] = None,
    ) -> RecursiveCollectResult:
        """
        Run the full recursive collection pipeline.

        Args:
            company_id: Database company ID for the parent company
            config: Pipeline configuration

        Returns:
            RecursiveCollectResult with comprehensive metrics
        """
        if config is None:
            config = RecursiveCollectConfig()

        session = self._get_session()
        started_at = datetime.utcnow()

        company = (
            session.query(IndustrialCompany)
            .filter(IndustrialCompany.id == company_id)
            .first()
        )

        if not company:
            return RecursiveCollectResult(
                company_id=company_id,
                company_name="Unknown",
                errors=[f"Company {company_id} not found"],
            )

        result = RecursiveCollectResult(
            company_id=company_id,
            company_name=company.name,
        )

        logger.info(
            f"[RecursiveCollector] Starting recursive collection for "
            f"{company.name} (id={company_id})"
        )

        # Create job record
        job = PeopleCollectionJob(
            job_type="recursive_collection",
            company_id=company_id,
            config={
                "type": "recursive_collection",
                "discover_structure": config.discover_structure,
                "max_units": config.max_units,
                "run_linkedin": config.run_linkedin,
                "map_functions": config.map_functions,
            },
            status="running",
            started_at=started_at,
        )
        session.add(job)
        session.commit()

        try:
            # ==============================
            # Phase 1: Structure Discovery
            # ==============================
            subsidiary_ids = []
            if config.discover_structure:
                phase_start = datetime.utcnow()
                logger.info(
                    "[RecursiveCollector] Phase 1: Corporate structure discovery"
                )

                from app.sources.people_collection.structure_discovery import (
                    StructureDiscoveryAgent,
                )

                discovery_agent = StructureDiscoveryAgent()

                try:
                    units = await discovery_agent.discover(
                        company_id=company_id,
                        db_session=session,
                        max_units=config.max_units,
                    )
                    result.units_discovered = len(units)
                    result.unit_names = [u.name for u in units]

                    logger.info(
                        f"[RecursiveCollector] Phase 1 complete: "
                        f"{len(units)} business units discovered"
                    )
                except Exception as e:
                    logger.error(f"[RecursiveCollector] Phase 1 error: {e}")
                    result.errors.append(f"Structure discovery: {str(e)}")
                finally:
                    await discovery_agent.close()

                result.phase_durations["structure_discovery"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()

            # Get all subsidiary IDs (whether just discovered or pre-existing)
            subsidiaries = (
                session.query(IndustrialCompany)
                .filter(
                    IndustrialCompany.parent_company_id == company_id,
                    IndustrialCompany.status == "active",
                )
                .all()
            )
            subsidiary_ids = [s.id for s in subsidiaries]

            logger.info(
                f"[RecursiveCollector] Total subsidiaries to process: "
                f"{len(subsidiary_ids)}"
            )

            # ==============================
            # Phase 2: Per-unit Deep Collection
            # ==============================
            phase_start = datetime.utcnow()
            logger.info("[RecursiveCollector] Phase 2: Per-unit collection")

            # Collect for parent company first
            parent_result = await self._collect_for_unit(company, config, session)
            result.per_unit_results[company.name] = parent_result
            result.total_people_found += parent_result.get("people_found", 0)
            result.total_people_created += parent_result.get("people_created", 0)
            result.total_people_updated += parent_result.get("people_updated", 0)
            result.total_changes += parent_result.get("changes", 0)
            result.units_collected = 1

            # Then collect for each subsidiary
            for sub in subsidiaries:
                try:
                    sub_result = await self._collect_for_unit(sub, config, session)
                    result.per_unit_results[sub.name] = sub_result
                    result.total_people_found += sub_result.get("people_found", 0)
                    result.total_people_created += sub_result.get("people_created", 0)
                    result.total_people_updated += sub_result.get("people_updated", 0)
                    result.total_changes += sub_result.get("changes", 0)
                    result.units_collected += 1

                except Exception as e:
                    logger.error(
                        f"[RecursiveCollector] Collection failed for "
                        f"{sub.name}: {e}"
                    )
                    result.errors.append(f"Collection for {sub.name}: {str(e)}")
                    result.per_unit_results[sub.name] = {"error": str(e)}

            result.phase_durations["per_unit_collection"] = (
                datetime.utcnow() - phase_start
            ).total_seconds()

            logger.info(
                f"[RecursiveCollector] Phase 2 complete: "
                f"{result.units_collected} units collected, "
                f"{result.total_people_found} total people"
            )

            # ==============================
            # Phase 3: LinkedIn Discovery
            # ==============================
            if config.run_linkedin:
                phase_start = datetime.utcnow()
                logger.info("[RecursiveCollector] Phase 3: LinkedIn discovery")

                linkedin_people = await self._run_linkedin_discovery(
                    company, subsidiaries, config, session
                )
                result.linkedin_people = len(linkedin_people)

                result.phase_durations["linkedin_discovery"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()

                logger.info(
                    f"[RecursiveCollector] Phase 3 complete: "
                    f"{result.linkedin_people} people from LinkedIn"
                )

            # ==============================
            # Phase 4: Functional Org Mapping
            # ==============================
            if config.map_functions:
                phase_start = datetime.utcnow()
                logger.info(
                    f"[RecursiveCollector] Phase 4: Functional org mapping "
                    f"({config.map_functions})"
                )

                from app.sources.people_collection.functional_org_mapper import (
                    FunctionalOrgMapper,
                )

                mapper = FunctionalOrgMapper()

                try:
                    for func_name in config.map_functions:
                        try:
                            func_result = await mapper.map_function(
                                company_id=company_id,
                                function_name=func_name,
                                db_session=session,
                                depth=config.function_depth,
                                include_subsidiaries=True,
                                fill_gaps_via_linkedin=config.run_linkedin,
                                max_linkedin_searches=min(
                                    30, config.max_linkedin_searches // 3
                                ),
                            )
                            result.functional_maps[func_name] = func_result

                        except Exception as e:
                            logger.error(
                                f"[RecursiveCollector] Functional mapping "
                                f"failed for {func_name}: {e}"
                            )
                            result.errors.append(
                                f"Functional map ({func_name}): {str(e)}"
                            )
                finally:
                    await mapper.close()

                result.phase_durations["functional_org_mapping"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()

            # ==============================
            # Phase 5: Master Org Chart
            # ==============================
            if config.build_master_org_chart:
                phase_start = datetime.utcnow()
                logger.info("[RecursiveCollector] Phase 5: Master org chart")

                from app.sources.people_collection.org_chart_builder import (
                    OrgChartBuilder,
                )

                builder = OrgChartBuilder()

                try:
                    # Build org chart for parent company (includes all people
                    # stored under its ID)
                    division_context = ", ".join(result.unit_names[:20])
                    chart_result = await builder.build_org_chart(
                        company_id=company_id,
                        company_name=company.name,
                        db_session=session,
                        division_context=division_context or None,
                    )

                    result.master_org_chart_built = not chart_result.get("error")
                    result.master_org_chart_depth = chart_result.get("max_depth", 0)

                except Exception as e:
                    logger.error(f"[RecursiveCollector] Master org chart failed: {e}")
                    result.errors.append(f"Master org chart: {str(e)}")

                result.phase_durations["master_org_chart"] = (
                    datetime.utcnow() - phase_start
                ).total_seconds()

            # Update company metadata
            company.last_crawled_date = date.today()
            company.leadership_last_updated = date.today()

            result.success = True

            # Update job record
            job.status = "success"
            job.completed_at = datetime.utcnow()
            job.people_found = result.total_people_found
            job.people_created = result.total_people_created
            job.people_updated = result.total_people_updated
            job.changes_detected = result.total_changes
            session.commit()

        except Exception as e:
            logger.exception(
                f"[RecursiveCollector] Fatal error in recursive collection: {e}"
            )
            result.errors.append(str(e))
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.errors = result.errors
            session.commit()

        finally:
            if not self._provided_session:
                session.close()

        result.duration_seconds = (datetime.utcnow() - started_at).total_seconds()

        logger.info(
            f"[RecursiveCollector] Recursive collection complete for "
            f"{company.name}: units={result.units_discovered}, "
            f"people={result.total_people_found}, "
            f"linkedin={result.linkedin_people}, "
            f"duration={result.duration_seconds:.0f}s"
        )

        return result

    async def _collect_for_unit(
        self,
        company: IndustrialCompany,
        config: RecursiveCollectConfig,
        session: Session,
    ) -> Dict[str, Any]:
        """
        Run DeepCollectionOrchestrator for a single business unit.
        """
        from app.sources.people_collection.deep_collection_orchestrator import (
            DeepCollectionOrchestrator,
            DeepCollectionConfig,
        )

        logger.info(
            f"[RecursiveCollector] Collecting for unit: {company.name} "
            f"(id={company.id}, cik={company.cik}, website={company.website})"
        )

        # Build config appropriate for this unit
        deep_config = DeepCollectionConfig(
            run_sec=config.run_sec_per_unit and bool(company.cik),
            run_website=config.run_website_per_unit and bool(company.website),
            run_news=config.run_news_per_unit,
            build_org_chart=False,  # We build master chart at the end
            max_crawl_pages=config.max_crawl_pages_per_unit,
            news_days_back=365,  # Shorter lookback for subsidiaries
        )

        # If unit has no website and no CIK, skip
        if not deep_config.run_sec and not deep_config.run_website:
            logger.info(
                f"[RecursiveCollector] Skipping {company.name}: "
                f"no CIK or website configured"
            )
            return {
                "skipped": True,
                "reason": "no CIK or website",
                "people_found": 0,
                "people_created": 0,
                "people_updated": 0,
                "changes": 0,
            }

        orchestrator = DeepCollectionOrchestrator(db_session=session)
        deep_result = await orchestrator.deep_collect(
            company_id=company.id,
            config=deep_config,
        )

        return {
            "people_found": deep_result.total_people_found,
            "people_created": deep_result.total_people_created,
            "people_updated": deep_result.total_people_updated,
            "changes": deep_result.total_changes,
            "sec_people": deep_result.sec_people,
            "website_people": deep_result.website_people,
            "errors": deep_result.errors,
            "duration_seconds": round(deep_result.duration_seconds, 1),
        }

    async def _run_linkedin_discovery(
        self,
        parent_company: IndustrialCompany,
        subsidiaries: List[IndustrialCompany],
        config: RecursiveCollectConfig,
        session: Session,
    ) -> list:
        """
        Run LinkedIn discovery across all business units.
        """
        from app.sources.people_collection.linkedin_discovery import LinkedInDiscovery

        discovery = LinkedInDiscovery()
        all_people = []

        try:
            division_names = [s.name for s in subsidiaries]

            # General people search
            searches_for_general = config.max_linkedin_searches // 2
            general_people = await discovery.discover_people(
                company_name=parent_company.name,
                division_names=division_names,
                max_searches=searches_for_general,
            )

            # Store general people
            if general_people:
                from app.sources.people_collection.orchestrator import (
                    PeopleCollectionOrchestrator,
                )

                orchestrator = PeopleCollectionOrchestrator(db_session=session)
                await orchestrator._store_people(
                    general_people, parent_company, session
                )
                all_people.extend(general_people)

            # Tech org targeted search (if technology is in map_functions)
            if "technology" in config.map_functions:
                searches_for_tech = config.max_linkedin_searches - searches_for_general
                tech_people = await discovery.discover_tech_org(
                    company_name=parent_company.name,
                    division_names=division_names,
                    depth=config.function_depth,
                    max_searches=searches_for_tech,
                )

                if tech_people:
                    from app.sources.people_collection.orchestrator import (
                        PeopleCollectionOrchestrator,
                    )

                    orchestrator = PeopleCollectionOrchestrator(db_session=session)
                    await orchestrator._store_people(
                        tech_people, parent_company, session
                    )
                    all_people.extend(tech_people)

        except Exception as e:
            logger.error(f"[RecursiveCollector] LinkedIn discovery error: {e}")
        finally:
            await discovery.close()

        return all_people
