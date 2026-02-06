"""
People Collection Orchestrator - Coordinates all collection agents.

The orchestrator is the main entry point for collecting leadership data.
It coordinates between different collection agents (website, SEC, news)
and handles the full pipeline from collection to database storage.
"""

import asyncio
import logging
import traceback
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.core.people_models import (
    IndustrialCompany,
    Person,
    CompanyPerson,
    PersonExperience,
    PersonEducation,
    LeadershipChange as LeadershipChangeModel,
    PeopleCollectionJob,
)
from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    CollectionResult,
    BatchCollectionResult,
    ExtractionConfidence,
)
from app.sources.people_collection.config import COLLECTION_SETTINGS

logger = logging.getLogger(__name__)


@dataclass
class DiagnosticInfo:
    """Diagnostic information for debugging collection issues."""
    company_id: int
    company_name: str

    # Data availability
    has_website: bool = False
    website_url: Optional[str] = None
    has_cik: bool = False
    cik: Optional[str] = None

    # Agent results
    website_agent_ran: bool = False
    website_pages_found: int = 0
    website_pages_checked: List[str] = field(default_factory=list)
    website_people_extracted: int = 0
    website_errors: List[str] = field(default_factory=list)
    website_duration_ms: int = 0

    sec_agent_ran: bool = False
    sec_filings_found: int = 0
    sec_people_extracted: int = 0
    sec_changes_extracted: int = 0
    sec_errors: List[str] = field(default_factory=list)
    sec_duration_ms: int = 0

    news_agent_ran: bool = False
    news_releases_found: int = 0
    news_changes_extracted: int = 0
    news_errors: List[str] = field(default_factory=list)
    news_duration_ms: int = 0

    # Storage results
    people_stored_created: int = 0
    people_stored_updated: int = 0
    changes_stored: int = 0
    storage_errors: List[str] = field(default_factory=list)

    # Overall
    total_duration_ms: int = 0
    success: bool = False
    failure_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "company": {
                "id": self.company_id,
                "name": self.company_name,
                "has_website": self.has_website,
                "website_url": self.website_url,
                "has_cik": self.has_cik,
                "cik": self.cik,
            },
            "website_agent": {
                "ran": self.website_agent_ran,
                "pages_found": self.website_pages_found,
                "pages_checked": self.website_pages_checked,
                "people_extracted": self.website_people_extracted,
                "errors": self.website_errors,
                "duration_ms": self.website_duration_ms,
            },
            "sec_agent": {
                "ran": self.sec_agent_ran,
                "filings_found": self.sec_filings_found,
                "people_extracted": self.sec_people_extracted,
                "changes_extracted": self.sec_changes_extracted,
                "errors": self.sec_errors,
                "duration_ms": self.sec_duration_ms,
            },
            "news_agent": {
                "ran": self.news_agent_ran,
                "releases_found": self.news_releases_found,
                "changes_extracted": self.news_changes_extracted,
                "errors": self.news_errors,
                "duration_ms": self.news_duration_ms,
            },
            "storage": {
                "people_created": self.people_stored_created,
                "people_updated": self.people_stored_updated,
                "changes_stored": self.changes_stored,
                "errors": self.storage_errors,
            },
            "summary": {
                "total_duration_ms": self.total_duration_ms,
                "success": self.success,
                "failure_reason": self.failure_reason,
            },
        }


class PeopleCollectionOrchestrator:
    """
    Orchestrates people data collection across multiple sources.

    Coordinates:
    - Website collection (leadership pages)
    - SEC filing collection (proxy statements, 8-Ks)
    - News/press release collection
    - Data deduplication and storage
    """

    def __init__(self, db_session: Optional[Session] = None):
        """
        Initialize the orchestrator.

        Args:
            db_session: SQLAlchemy session. If not provided, creates new sessions.
        """
        self._provided_session = db_session
        self._website_agent = None
        self._sec_agent = None
        self._news_agent = None

    def _get_session(self) -> Session:
        """Get database session."""
        if self._provided_session:
            return self._provided_session
        SessionLocal = get_session_factory()
        return SessionLocal()

    async def collect_company(
        self,
        company_id: int,
        sources: List[str] = None,
    ) -> CollectionResult:
        """
        Collect leadership data for a single company.

        Args:
            company_id: Database ID of the company
            sources: List of sources to collect from (website, sec, news)
                    If None, uses all applicable sources.

        Returns:
            CollectionResult with extraction results
        """
        if sources is None:
            sources = ["website"]  # Default to website only

        session = self._get_session()
        started_at = datetime.utcnow()

        try:
            # Get company
            company = session.query(IndustrialCompany).filter(
                IndustrialCompany.id == company_id
            ).first()

            if not company:
                logger.error(f"Company {company_id} not found in database")
                return CollectionResult(
                    company_id=company_id,
                    company_name="Unknown",
                    source=",".join(sources),
                    success=False,
                    errors=[f"Company {company_id} not found"],
                    started_at=started_at,
                )

            logger.info(
                f"Starting collection for {company.name} (id={company_id}): "
                f"website={company.website}, cik={company.cik}, sources={sources}"
            )

            result = CollectionResult(
                company_id=company_id,
                company_name=company.name,
                source=",".join(sources),
                started_at=started_at,
            )

            # Create collection job record
            job = PeopleCollectionJob(
                job_type="single_company",
                company_id=company_id,
                config={"sources": sources},
                status="running",
                started_at=started_at,
            )
            session.add(job)
            session.commit()

            # Collect from each source with detailed tracking
            all_people: List[ExtractedPerson] = []
            all_changes: List[LeadershipChange] = []
            agent_diagnostics: Dict[str, Any] = {}

            if "website" in sources:
                agent_start = datetime.utcnow()
                if company.website:
                    logger.info(f"Running WebsiteAgent for {company.name}")
                    website_result = await self._collect_from_website(company, session)
                    people_count = len(website_result.get("people", []))
                    all_people.extend(website_result.get("people", []))
                    result.errors.extend(website_result.get("errors", []))
                    logger.info(f"WebsiteAgent found {people_count} people for {company.name}")
                    agent_diagnostics["website"] = {
                        "ran": True,
                        "people_found": people_count,
                        "errors": website_result.get("errors", []),
                        "duration_ms": int((datetime.utcnow() - agent_start).total_seconds() * 1000),
                    }
                else:
                    logger.warning(f"Skipping WebsiteAgent for {company.name} - no website URL")
                    result.warnings.append("No website URL configured")
                    agent_diagnostics["website"] = {
                        "ran": False,
                        "skip_reason": "No website URL configured",
                    }

            if "sec" in sources:
                agent_start = datetime.utcnow()
                if company.cik:
                    logger.info(f"Running SECAgent for {company.name} (CIK: {company.cik})")
                    sec_result = await self._collect_from_sec(company, session)
                    people_count = len(sec_result.get("people", []))
                    changes_count = len(sec_result.get("changes", []))
                    all_people.extend(sec_result.get("people", []))
                    all_changes.extend(sec_result.get("changes", []))
                    result.errors.extend(sec_result.get("errors", []))
                    logger.info(f"SECAgent found {people_count} people, {changes_count} changes for {company.name}")
                    agent_diagnostics["sec"] = {
                        "ran": True,
                        "people_found": people_count,
                        "changes_found": changes_count,
                        "errors": sec_result.get("errors", []),
                        "duration_ms": int((datetime.utcnow() - agent_start).total_seconds() * 1000),
                    }
                else:
                    logger.warning(f"Skipping SECAgent for {company.name} - no CIK")
                    result.warnings.append("No SEC CIK configured")
                    agent_diagnostics["sec"] = {
                        "ran": False,
                        "skip_reason": "No SEC CIK configured",
                    }

            if "news" in sources:
                agent_start = datetime.utcnow()
                logger.info(f"Running NewsAgent for {company.name}")
                news_result = await self._collect_from_news(company, session)
                changes_count = len(news_result.get("changes", []))
                all_changes.extend(news_result.get("changes", []))
                result.errors.extend(news_result.get("errors", []))
                logger.info(f"NewsAgent found {changes_count} changes for {company.name}")
                agent_diagnostics["news"] = {
                    "ran": True,
                    "changes_found": changes_count,
                    "errors": news_result.get("errors", []),
                    "duration_ms": int((datetime.utcnow() - agent_start).total_seconds() * 1000),
                }

            # Deduplicate and store people
            result.people_found = len(all_people)
            stored = await self._store_people(all_people, company, session)
            result.people_created = stored["created"]
            result.people_updated = stored["updated"]

            # Store changes
            result.changes_detected = len(all_changes)
            await self._store_changes(all_changes, company, session)

            # Update job status
            job.status = "success" if not result.errors else "completed_with_errors"
            job.completed_at = datetime.utcnow()
            job.people_found = result.people_found
            job.people_created = result.people_created
            job.people_updated = result.people_updated
            job.changes_detected = result.changes_detected
            job.errors = result.errors if result.errors else None
            job.warnings = result.warnings if result.warnings else None

            # Store diagnostic info in config for later analysis
            job_config = job.config or {}
            job_config["diagnostics"] = {
                "company_data": {
                    "has_website": bool(company.website),
                    "website_url": company.website,
                    "has_cik": bool(company.cik),
                    "cik": company.cik,
                },
                "agents_run": sources,
                "agent_results": agent_diagnostics,
                "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            }
            job.config = job_config

            # Update company last crawled date
            company.last_crawled_date = date.today()
            company.leadership_last_updated = date.today()

            session.commit()

            result.success = True
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - started_at).total_seconds()

            logger.info(
                f"Collected {company.name}: {result.people_found} found, "
                f"{result.people_created} created, {result.people_updated} updated"
            )

            return result

        except Exception as e:
            logger.exception(f"Error collecting company {company_id}: {e}")
            session.rollback()
            return CollectionResult(
                company_id=company_id,
                company_name=company.name if company else "Unknown",
                source=",".join(sources),
                success=False,
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )

        finally:
            # Close agent sessions to prevent resource leaks
            await self._close_agents()

            if not self._provided_session:
                session.close()

    async def collect_batch(
        self,
        company_ids: List[int],
        sources: List[str] = None,
        max_concurrent: int = None,
    ) -> BatchCollectionResult:
        """
        Collect leadership data for multiple companies.

        Args:
            company_ids: List of company database IDs
            sources: Sources to collect from
            max_concurrent: Max concurrent collections

        Returns:
            BatchCollectionResult with all results
        """
        if max_concurrent is None:
            max_concurrent = COLLECTION_SETTINGS.max_concurrent_companies

        started_at = datetime.utcnow()
        batch_result = BatchCollectionResult(
            total_companies=len(company_ids),
            successful=0,
            failed=0,
            started_at=started_at,
        )

        # Process in batches to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrent)

        async def collect_with_semaphore(company_id: int) -> CollectionResult:
            async with semaphore:
                return await self.collect_company(company_id, sources)

        # Run all collections
        tasks = [collect_with_semaphore(cid) for cid in company_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                batch_result.failed += 1
                batch_result.results.append(CollectionResult(
                    company_id=0,
                    company_name="Unknown",
                    source="",
                    success=False,
                    errors=[str(result)],
                ))
            else:
                batch_result.results.append(result)
                if result.success:
                    batch_result.successful += 1
                else:
                    batch_result.failed += 1
                batch_result.total_people_found += result.people_found
                batch_result.total_people_created += result.people_created
                batch_result.total_changes_detected += result.changes_detected

        batch_result.completed_at = datetime.utcnow()
        return batch_result

    async def _collect_from_website(
        self,
        company: IndustrialCompany,
        session: Session,
    ) -> Dict[str, Any]:
        """Collect leadership data from company website."""
        # Import here to avoid circular imports
        try:
            from app.sources.people_collection.website_agent import WebsiteAgent

            if self._website_agent is None:
                self._website_agent = WebsiteAgent()

            result = await self._website_agent.collect(
                company_id=company.id,
                company_name=company.name,
                website_url=company.website,
            )
            return {
                "people": result.extracted_people,
                "errors": result.errors,
            }
        except ImportError:
            logger.warning("WebsiteAgent not yet implemented")
            return {"people": [], "errors": ["WebsiteAgent not implemented"]}
        except Exception as e:
            logger.error(f"Website collection error: {e}")
            return {"people": [], "errors": [str(e)]}

    async def _collect_from_sec(
        self,
        company: IndustrialCompany,
        session: Session,
    ) -> Dict[str, Any]:
        """Collect leadership data from SEC filings."""
        try:
            from app.sources.people_collection.sec_agent import SECAgent

            if self._sec_agent is None:
                self._sec_agent = SECAgent()

            result = await self._sec_agent.collect(
                company_id=company.id,
                company_name=company.name,
                cik=company.cik,
            )
            return {
                "people": result.extracted_people,
                "changes": result.extracted_changes,
                "errors": result.errors,
            }
        except ImportError:
            logger.warning("SECAgent not yet implemented")
            return {"people": [], "changes": [], "errors": ["SECAgent not implemented"]}
        except Exception as e:
            logger.error(f"SEC collection error: {e}")
            return {"people": [], "changes": [], "errors": [str(e)]}

    async def _collect_from_news(
        self,
        company: IndustrialCompany,
        session: Session,
    ) -> Dict[str, Any]:
        """Collect leadership changes from news/press releases."""
        try:
            from app.sources.people_collection.news_agent import NewsAgent

            if self._news_agent is None:
                self._news_agent = NewsAgent()

            result = await self._news_agent.collect(
                company_id=company.id,
                company_name=company.name,
                website_url=company.website,
            )
            return {
                "changes": result.extracted_changes,
                "errors": result.errors,
            }
        except ImportError:
            logger.warning("NewsAgent not yet implemented")
            return {"changes": [], "errors": ["NewsAgent not implemented"]}
        except Exception as e:
            logger.error(f"News collection error: {e}")
            return {"changes": [], "errors": [str(e)]}

    async def _store_people(
        self,
        people: List[ExtractedPerson],
        company: IndustrialCompany,
        session: Session,
    ) -> Dict[str, int]:
        """
        Store extracted people in the database.

        Handles deduplication by matching on LinkedIn URL or name+company.
        """
        created = 0
        updated = 0

        for extracted in people:
            try:
                # Try to find existing person
                existing_person = None

                # First try LinkedIn URL (most reliable)
                if extracted.linkedin_url:
                    existing_person = session.query(Person).filter(
                        Person.linkedin_url == extracted.linkedin_url
                    ).first()

                # If not found, try name match
                if not existing_person and extracted.full_name:
                    # Check if same person already at this company
                    existing_cp = session.query(CompanyPerson).join(Person).filter(
                        Person.full_name == extracted.full_name,
                        CompanyPerson.company_id == company.id,
                        CompanyPerson.is_current == True,
                    ).first()
                    if existing_cp:
                        existing_person = session.query(Person).get(existing_cp.person_id)

                if existing_person:
                    # Update existing person
                    self._update_person(existing_person, extracted)
                    updated += 1
                    person_id = existing_person.id
                else:
                    # Create new person
                    person = self._create_person(extracted)
                    session.add(person)
                    session.flush()  # Get ID
                    created += 1
                    person_id = person.id

                # Create/update company_person relationship
                await self._store_company_person(
                    person_id, extracted, company, session
                )

            except Exception as e:
                logger.warning(f"Failed to store person {extracted.full_name}: {e}")

        return {"created": created, "updated": updated}

    def _create_person(self, extracted: ExtractedPerson) -> Person:
        """Create a new Person record from extracted data."""
        # Parse name parts
        name_parts = extracted.full_name.split()
        first_name = name_parts[0] if name_parts else None
        last_name = name_parts[-1] if len(name_parts) > 1 else None

        # Convert empty strings to None to avoid unique constraint violations
        linkedin_url = extracted.linkedin_url if extracted.linkedin_url else None
        email = extracted.email if extracted.email else None
        photo_url = extracted.photo_url if extracted.photo_url else None
        bio = extracted.bio if extracted.bio else None

        return Person(
            full_name=extracted.full_name,
            first_name=extracted.first_name or first_name,
            last_name=extracted.last_name or last_name,
            suffix=extracted.suffix,
            linkedin_url=linkedin_url,
            email=email,
            photo_url=photo_url,
            bio=bio,
            bio_source="website" if bio else None,
            data_sources=["website"],
            confidence_score=0.8 if extracted.confidence == ExtractionConfidence.HIGH else 0.6,
            last_verified_date=date.today(),
        )

    def _update_person(self, person: Person, extracted: ExtractedPerson) -> None:
        """Update an existing Person record with new data."""
        # Only update if new data is better/newer
        if extracted.linkedin_url and not person.linkedin_url:
            person.linkedin_url = extracted.linkedin_url

        if extracted.photo_url and not person.photo_url:
            person.photo_url = extracted.photo_url

        if extracted.bio and (not person.bio or len(extracted.bio) > len(person.bio)):
            person.bio = extracted.bio
            person.bio_source = "website"

        # Update data sources
        sources = person.data_sources or []
        if "website" not in sources:
            sources.append("website")
            person.data_sources = sources

        person.last_verified_date = date.today()

    async def _store_company_person(
        self,
        person_id: int,
        extracted: ExtractedPerson,
        company: IndustrialCompany,
        session: Session,
    ) -> None:
        """Store or update the company_person relationship."""
        # Check for existing relationship
        existing = session.query(CompanyPerson).filter(
            CompanyPerson.person_id == person_id,
            CompanyPerson.company_id == company.id,
            CompanyPerson.is_current == True,
        ).first()

        if existing:
            # Update if title changed
            if existing.title != extracted.title:
                existing.title = extracted.title
                existing.title_normalized = extracted.title_normalized
                # Handle both enum and string values (use_enum_values=True in Pydantic)
                title_level = extracted.title_level
                existing.title_level = title_level.value if hasattr(title_level, 'value') else title_level
        else:
            # Handle both enum and string values (use_enum_values=True in Pydantic)
            title_level = extracted.title_level
            title_level_val = title_level.value if hasattr(title_level, 'value') else title_level
            confidence = extracted.confidence
            confidence_val = confidence.value if hasattr(confidence, 'value') else (confidence or "medium")

            # Create new relationship
            cp = CompanyPerson(
                company_id=company.id,
                person_id=person_id,
                title=extracted.title,
                title_normalized=extracted.title_normalized,
                title_level=title_level_val,
                department=extracted.department,
                is_board_member=extracted.is_board_member,
                is_board_chair=extracted.is_board_chair,
                is_current=True,
                source="website",
                source_url=extracted.source_url,
                extraction_date=date.today(),
                confidence=confidence_val,
            )
            session.add(cp)

    async def _store_changes(
        self,
        changes: List[LeadershipChange],
        company: IndustrialCompany,
        session: Session,
    ) -> None:
        """Store detected leadership changes."""
        for change in changes:
            try:
                # Check for duplicate
                existing = session.query(LeadershipChangeModel).filter(
                    LeadershipChangeModel.company_id == company.id,
                    LeadershipChangeModel.person_name == change.person_name,
                    LeadershipChangeModel.change_type == change.change_type.value,
                    LeadershipChangeModel.effective_date == change.effective_date,
                ).first()

                if existing:
                    continue

                # Create change record
                change_record = LeadershipChangeModel(
                    company_id=company.id,
                    person_name=change.person_name,
                    person_id=change.person_id,
                    change_type=change.change_type.value,
                    old_title=change.old_title,
                    new_title=change.new_title,
                    old_company=change.old_company,
                    announced_date=change.announced_date,
                    effective_date=change.effective_date,
                    reason=change.reason,
                    source_type=change.source_type,
                    source_url=change.source_url,
                    source_headline=change.source_headline,
                    is_c_suite=change.is_c_suite,
                    is_board=change.is_board,
                    significance_score=change.significance_score,
                )
                session.add(change_record)

            except Exception as e:
                logger.warning(f"Failed to store change: {e}")

    async def collect_company_with_diagnostics(
        self,
        company_id: int,
        sources: List[str] = None,
    ) -> DiagnosticInfo:
        """
        Collect leadership data with full diagnostic information.

        This method is identical to collect_company but returns detailed
        diagnostic info for debugging why collections may fail.

        Args:
            company_id: Database ID of the company
            sources: List of sources to collect from

        Returns:
            DiagnosticInfo with detailed collection diagnostics
        """
        if sources is None:
            sources = ["website"]

        session = self._get_session()
        started_at = datetime.utcnow()

        # Initialize diagnostics
        diag = DiagnosticInfo(company_id=company_id, company_name="Unknown")

        try:
            # Get company
            company = session.query(IndustrialCompany).filter(
                IndustrialCompany.id == company_id
            ).first()

            if not company:
                diag.failure_reason = f"Company {company_id} not found in database"
                logger.error(f"[DIAG] {diag.failure_reason}")
                return diag

            diag.company_name = company.name
            diag.has_website = bool(company.website)
            diag.website_url = company.website
            diag.has_cik = bool(company.cik)
            diag.cik = company.cik

            logger.info(f"[DIAG] Starting collection for {company.name} (id={company_id})")
            logger.info(f"[DIAG] Company data: website={company.website}, cik={company.cik}")
            logger.info(f"[DIAG] Sources requested: {sources}")

            all_people: List[ExtractedPerson] = []
            all_changes: List[LeadershipChange] = []

            # Website collection
            if "website" in sources:
                if company.website:
                    logger.info(f"[DIAG] Running WebsiteAgent for {company.name}")
                    agent_start = datetime.utcnow()
                    diag.website_agent_ran = True

                    try:
                        website_result = await self._collect_from_website_with_diag(
                            company, session, diag
                        )
                        all_people.extend(website_result.get("people", []))
                        diag.website_people_extracted = len(website_result.get("people", []))
                        diag.website_errors = website_result.get("errors", [])

                        logger.info(
                            f"[DIAG] WebsiteAgent completed: "
                            f"{diag.website_pages_found} pages found, "
                            f"{diag.website_people_extracted} people extracted"
                        )

                    except Exception as e:
                        error_msg = f"WebsiteAgent exception: {str(e)}\n{traceback.format_exc()}"
                        diag.website_errors.append(error_msg)
                        logger.error(f"[DIAG] {error_msg}")

                    diag.website_duration_ms = int(
                        (datetime.utcnow() - agent_start).total_seconds() * 1000
                    )
                else:
                    logger.warning(f"[DIAG] Skipping WebsiteAgent - company has no website URL")
                    diag.website_errors.append("Company has no website URL configured")

            # SEC collection
            if "sec" in sources:
                if company.cik:
                    logger.info(f"[DIAG] Running SECAgent for {company.name} (CIK: {company.cik})")
                    agent_start = datetime.utcnow()
                    diag.sec_agent_ran = True

                    try:
                        sec_result = await self._collect_from_sec(company, session)
                        all_people.extend(sec_result.get("people", []))
                        all_changes.extend(sec_result.get("changes", []))
                        diag.sec_people_extracted = len(sec_result.get("people", []))
                        diag.sec_changes_extracted = len(sec_result.get("changes", []))
                        diag.sec_errors = sec_result.get("errors", [])

                        logger.info(
                            f"[DIAG] SECAgent completed: "
                            f"{diag.sec_people_extracted} people, "
                            f"{diag.sec_changes_extracted} changes"
                        )

                    except Exception as e:
                        error_msg = f"SECAgent exception: {str(e)}\n{traceback.format_exc()}"
                        diag.sec_errors.append(error_msg)
                        logger.error(f"[DIAG] {error_msg}")

                    diag.sec_duration_ms = int(
                        (datetime.utcnow() - agent_start).total_seconds() * 1000
                    )
                else:
                    logger.warning(f"[DIAG] Skipping SECAgent - company has no CIK")
                    diag.sec_errors.append("Company has no SEC CIK configured")

            # News collection
            if "news" in sources:
                logger.info(f"[DIAG] Running NewsAgent for {company.name}")
                agent_start = datetime.utcnow()
                diag.news_agent_ran = True

                try:
                    news_result = await self._collect_from_news(company, session)
                    all_changes.extend(news_result.get("changes", []))
                    diag.news_changes_extracted = len(news_result.get("changes", []))
                    diag.news_errors = news_result.get("errors", [])

                    logger.info(
                        f"[DIAG] NewsAgent completed: "
                        f"{diag.news_changes_extracted} changes"
                    )

                except Exception as e:
                    error_msg = f"NewsAgent exception: {str(e)}\n{traceback.format_exc()}"
                    diag.news_errors.append(error_msg)
                    logger.error(f"[DIAG] {error_msg}")

                diag.news_duration_ms = int(
                    (datetime.utcnow() - agent_start).total_seconds() * 1000
                )

            # Store results
            logger.info(f"[DIAG] Storing {len(all_people)} people, {len(all_changes)} changes")

            try:
                stored = await self._store_people(all_people, company, session)
                diag.people_stored_created = stored["created"]
                diag.people_stored_updated = stored["updated"]
                session.commit()

                logger.info(
                    f"[DIAG] Stored: {diag.people_stored_created} created, "
                    f"{diag.people_stored_updated} updated"
                )

            except Exception as e:
                error_msg = f"Storage exception: {str(e)}\n{traceback.format_exc()}"
                diag.storage_errors.append(error_msg)
                logger.error(f"[DIAG] {error_msg}")
                session.rollback()

            # Determine success
            total_people = diag.website_people_extracted + diag.sec_people_extracted
            diag.success = total_people > 0 or not diag.website_errors

            if not diag.success:
                if not diag.has_website and not diag.has_cik:
                    diag.failure_reason = "Company has neither website nor CIK configured"
                elif diag.website_errors:
                    diag.failure_reason = f"Website collection failed: {diag.website_errors[0]}"
                else:
                    diag.failure_reason = "No people found from any source"

            diag.total_duration_ms = int(
                (datetime.utcnow() - started_at).total_seconds() * 1000
            )

            logger.info(
                f"[DIAG] Collection completed for {company.name}: "
                f"success={diag.success}, people={total_people}, "
                f"duration={diag.total_duration_ms}ms"
            )

            return diag

        except Exception as e:
            diag.failure_reason = f"Unexpected error: {str(e)}"
            logger.exception(f"[DIAG] Unexpected error collecting {company_id}: {e}")
            return diag

        finally:
            # Close agent sessions to prevent resource leaks
            await self._close_agents()

            if not self._provided_session:
                session.close()

    async def _close_agents(self):
        """Close all agent HTTP sessions."""
        if self._website_agent:
            try:
                await self._website_agent.close()
            except Exception:
                pass
            self._website_agent = None
        if hasattr(self, '_sec_agent') and self._sec_agent:
            try:
                await self._sec_agent.close()
            except Exception:
                pass
            self._sec_agent = None
        if hasattr(self, '_news_agent') and self._news_agent:
            try:
                await self._news_agent.close()
            except Exception:
                pass
            self._news_agent = None

    async def _collect_from_website_with_diag(
        self,
        company: IndustrialCompany,
        session: Session,
        diag: DiagnosticInfo,
    ) -> Dict[str, Any]:
        """Collect from website with diagnostic tracking."""
        try:
            from app.sources.people_collection.website_agent import WebsiteAgent

            if self._website_agent is None:
                self._website_agent = WebsiteAgent()

            logger.info(f"[DIAG] WebsiteAgent collecting from {company.website}")

            result = await self._website_agent.collect(
                company_id=company.id,
                company_name=company.name,
                website_url=company.website,
            )

            # Extract diagnostic info from result
            diag.website_pages_found = getattr(result, 'pages_checked', 0)
            if hasattr(result, 'page_urls'):
                diag.website_pages_checked = result.page_urls or []

            logger.info(
                f"[DIAG] WebsiteAgent raw result: "
                f"people={len(result.extracted_people)}, "
                f"errors={result.errors}, "
                f"warnings={result.warnings}"
            )

            return {
                "people": result.extracted_people,
                "errors": result.errors + result.warnings,
            }

        except ImportError as e:
            logger.error(f"[DIAG] WebsiteAgent import error: {e}")
            return {"people": [], "errors": [f"WebsiteAgent not available: {e}"]}

        except Exception as e:
            logger.exception(f"[DIAG] WebsiteAgent collection error: {e}")
            return {"people": [], "errors": [f"WebsiteAgent error: {str(e)}"]}

    async def refresh_company(
        self,
        company_id: int,
        force: bool = False,
    ) -> CollectionResult:
        """
        Refresh leadership data for a company.

        Only re-collects if data is stale (>30 days old) unless force=True.
        """
        session = self._get_session()

        try:
            company = session.query(IndustrialCompany).get(company_id)
            if not company:
                return CollectionResult(
                    company_id=company_id,
                    company_name="Unknown",
                    source="refresh",
                    success=False,
                    errors=["Company not found"],
                )

            # Check if refresh needed
            if not force and company.leadership_last_updated:
                days_old = (date.today() - company.leadership_last_updated).days
                if days_old < 30:
                    return CollectionResult(
                        company_id=company_id,
                        company_name=company.name,
                        source="refresh",
                        success=True,
                        errors=[f"Data is only {days_old} days old, skipping refresh"],
                    )

            # Determine sources
            sources = ["website"]
            if company.cik:
                sources.append("sec")

            return await self.collect_company(company_id, sources)

        finally:
            if not self._provided_session:
                session.close()
