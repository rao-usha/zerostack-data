"""
People Collection Orchestrator - Coordinates all collection agents.

The orchestrator is the main entry point for collecting leadership data.
It coordinates between different collection agents (website, SEC, news)
and handles the full pipeline from collection to database storage.
"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any

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
                return CollectionResult(
                    company_id=company_id,
                    company_name="Unknown",
                    source=",".join(sources),
                    success=False,
                    errors=[f"Company {company_id} not found"],
                    started_at=started_at,
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

            # Collect from each source
            all_people: List[ExtractedPerson] = []
            all_changes: List[LeadershipChange] = []

            if "website" in sources and company.website:
                website_result = await self._collect_from_website(company, session)
                all_people.extend(website_result.get("people", []))
                result.errors.extend(website_result.get("errors", []))

            if "sec" in sources and company.cik:
                sec_result = await self._collect_from_sec(company, session)
                all_people.extend(sec_result.get("people", []))
                all_changes.extend(sec_result.get("changes", []))
                result.errors.extend(sec_result.get("errors", []))

            if "news" in sources:
                news_result = await self._collect_from_news(company, session)
                all_changes.extend(news_result.get("changes", []))
                result.errors.extend(news_result.get("errors", []))

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
                existing.title_level = extracted.title_level.value if extracted.title_level else None
        else:
            # Create new relationship
            cp = CompanyPerson(
                company_id=company.id,
                person_id=person_id,
                title=extracted.title,
                title_normalized=extracted.title_normalized,
                title_level=extracted.title_level.value if extracted.title_level else None,
                department=extracted.department,
                is_board_member=extracted.is_board_member,
                is_board_chair=extracted.is_board_chair,
                is_current=True,
                source="website",
                source_url=extracted.source_url,
                extraction_date=date.today(),
                confidence=extracted.confidence.value if extracted.confidence else "medium",
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
