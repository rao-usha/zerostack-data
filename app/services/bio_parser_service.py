"""
Bio Parser Service.

Parses executive biography text into structured PersonExperience and PersonEducation
records using the existing LLMExtractor.parse_bio() pipeline.
"""

import asyncio
import logging
from typing import Optional
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class BioParserService:
    """
    Orchestrates LLM bio parsing and DB persistence.

    Reuses LLMExtractor.parse_bio() — does not re-implement LLM logic.
    """

    async def parse_person(
        self,
        person_id: int,
        person_name: str,
        company_name: str,
        bio: str,
        db: Session,
    ) -> dict:
        """
        Parse one person's bio and upsert experience + education rows.

        Returns dict with counts of rows created.
        """
        from app.sources.people_collection.llm_extractor import LLMExtractor
        from app.core.people_models import PersonExperience, PersonEducation

        extractor = LLMExtractor()
        try:
            parsed = await extractor.parse_bio(
                bio_text=bio,
                person_name=person_name,
                company_name=company_name,
            )
        except Exception as e:
            logger.warning(f"Bio parse failed for person_id={person_id}: {e}")
            return {"experience_created": 0, "education_created": 0, "error": str(e)}
        finally:
            if hasattr(extractor, 'close'):
                await extractor.close()

        exp_created = 0
        for exp in parsed.experience:
            if not exp.company_name or not exp.title:
                continue
            existing = (
                db.query(PersonExperience)
                .filter(
                    PersonExperience.person_id == person_id,
                    PersonExperience.company_name == exp.company_name,
                    PersonExperience.title == exp.title,
                )
                .first()
            )
            if existing:
                continue
            row = PersonExperience(
                person_id=person_id,
                company_name=exp.company_name,
                title=exp.title,
                start_year=exp.start_year,
                end_year=exp.end_year,
                is_current=exp.is_current,
                description=exp.description,
                source="bio_parse",
            )
            db.add(row)
            exp_created += 1

        edu_created = 0
        for edu in parsed.education:
            if not edu.institution:
                continue
            existing = (
                db.query(PersonEducation)
                .filter(
                    PersonEducation.person_id == person_id,
                    PersonEducation.institution == edu.institution,
                )
                .first()
            )
            if existing:
                continue
            # Map degree string to degree_type enum value
            degree_type = _infer_degree_type(edu.degree)
            row = PersonEducation(
                person_id=person_id,
                institution=edu.institution,
                degree=edu.degree,
                degree_type=degree_type,
                field_of_study=edu.field_of_study,
                graduation_year=edu.graduation_year,
                source="bio_parse",
            )
            db.add(row)
            edu_created += 1

        db.commit()
        return {"experience_created": exp_created, "education_created": edu_created}

    async def parse_all(
        self,
        db: Session,
        limit: Optional[int] = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Parse bios for all people with bio text.

        Skips people already in people_experience unless overwrite=True.
        Uses asyncio.Semaphore(4) for bounded concurrency.

        Returns stats dict.
        """
        from app.core.people_models import Person, PersonExperience, CompanyPerson
        from sqlalchemy import func

        # Find people with bios
        query = (
            db.query(Person, CompanyPerson.company_id)
            .outerjoin(CompanyPerson, (CompanyPerson.person_id == Person.id) & (CompanyPerson.is_current == True))
            .filter(Person.bio.isnot(None), func.length(Person.bio) > 50)
            .order_by(Person.id)
        )
        if limit:
            query = query.limit(limit)

        candidates = query.all()

        if not overwrite:
            # Get person_ids already parsed
            parsed_ids = {
                r[0] for r in db.query(PersonExperience.person_id).distinct().all()
            }
            candidates = [(p, cid) for p, cid in candidates if p.id not in parsed_ids]

        logger.info(f"BioParserService: parsing {len(candidates)} people bios")

        semaphore = asyncio.Semaphore(4)
        stats = {"parsed": 0, "experience_created": 0, "education_created": 0, "errors": 0}

        async def _parse_one(person, company_id):
            company_name = "Unknown"
            if company_id:
                from app.core.people_models import IndustrialCompany
                company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
                if company:
                    company_name = company.name

            async with semaphore:
                result = await self.parse_person(
                    person_id=person.id,
                    person_name=person.full_name,
                    company_name=company_name,
                    bio=person.bio,
                    db=db,
                )
            return result

        tasks = [_parse_one(p, cid) for p, cid in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                stats["errors"] += 1
            else:
                stats["parsed"] += 1
                stats["experience_created"] += r.get("experience_created", 0)
                stats["education_created"] += r.get("education_created", 0)
                if r.get("error"):
                    stats["errors"] += 1

        return stats


def _infer_degree_type(degree: Optional[str]) -> Optional[str]:
    """Map degree string to degree_type value expected by PedigreeScorer."""
    if not degree:
        return None
    d = degree.lower()
    if any(x in d for x in ["phd", "doctorate", "d.phil", "dba", "md", "jd"]):
        return "doctorate"
    if any(x in d for x in ["mba", "master", "ms ", "m.s", "ma ", "m.a", "meng", "mpa", "mpp"]):
        return "masters"
    if any(x in d for x in ["bs", "ba", "b.s", "b.a", "bsc", "bba", "bachelor"]):
        return "bachelors"
    if any(x in d for x in ["certificate", "cert", "diploma"]):
        return "certificate"
    return None
