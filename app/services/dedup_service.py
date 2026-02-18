"""
People Deduplication Service.

Scans for duplicate person records using fuzzy name matching,
auto-merges high-confidence duplicates, and manages a review queue
for ambiguous cases.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from app.core.people_models import (
    Person,
    CompanyPerson,
    PersonExperience,
    PersonEducation,
    PeopleMergeCandidate,
)
from app.sources.people_collection.person_matcher import PersonNameMatcher

logger = logging.getLogger(__name__)


class DedupService:
    """
    Core deduplication logic: scan, auto-merge, manual merge, reject.
    """

    def __init__(self, session: Session):
        self.session = session
        self.matcher = PersonNameMatcher()

    def scan_for_duplicates(
        self,
        company_id: Optional[int] = None,
        limit: int = 1000,
    ) -> Dict[str, int]:
        """
        Scan for duplicate person records.

        Groups people by last_name for efficiency (avoids O(n²) across full table).
        Only compares canonical records (is_canonical=True).
        Skips pairs already in people_merge_candidates.

        Returns:
            Dict with counts: auto_merged, review_queued, skipped, total_compared
        """
        stats = {
            "auto_merged": 0,
            "review_queued": 0,
            "skipped": 0,
            "total_compared": 0,
        }

        # Get canonical people, optionally filtered by company
        query = self.session.query(Person).filter(
            Person.is_canonical == True,
            Person.last_name.isnot(None),
            Person.first_name.isnot(None),
        )

        if company_id:
            person_ids = (
                self.session.query(CompanyPerson.person_id)
                .filter(
                    CompanyPerson.company_id == company_id,
                    CompanyPerson.is_current == True,
                )
                .subquery()
            )
            query = query.filter(Person.id.in_(person_ids))

        people = query.limit(limit).all()

        if len(people) < 2:
            return stats

        # Get existing candidate pairs to skip
        existing_pairs = set()
        existing_rows = self.session.query(
            PeopleMergeCandidate.person_id_a,
            PeopleMergeCandidate.person_id_b,
        ).all()
        for a, b in existing_rows:
            existing_pairs.add((a, b))

        # Group by last name for efficient comparison
        by_last_name: Dict[str, List[Person]] = {}
        for person in people:
            key = person.last_name.lower().strip()
            by_last_name.setdefault(key, []).append(person)

        # Compare within each last-name group
        for last_name, group in by_last_name.items():
            if len(group) < 2:
                continue

            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    p1, p2 = group[i], group[j]

                    # Ensure consistent ordering (a < b)
                    id_a, id_b = min(p1.id, p2.id), max(p1.id, p2.id)

                    if (id_a, id_b) in existing_pairs:
                        stats["skipped"] += 1
                        continue

                    stats["total_compared"] += 1

                    # Compare names
                    result = self.matcher.compare(p1.full_name, p2.full_name)

                    if not result.matched:
                        continue

                    # Check for shared companies
                    shared_companies = self._get_shared_companies(p1.id, p2.id)
                    has_shared = len(shared_companies) > 0

                    classification = self.matcher.classify_match(
                        result.similarity, has_shared,
                    )

                    if classification == "auto_merge":
                        try:
                            self._auto_merge(
                                id_a, id_b, result, shared_companies,
                            )
                            stats["auto_merged"] += 1
                        except Exception as e:
                            logger.warning(
                                f"Auto-merge failed for {id_a}/{id_b}: {e}"
                            )
                            # Fall through to queue for review
                            self._create_candidate(
                                id_a, id_b, result, shared_companies, "pending",
                            )
                            stats["review_queued"] += 1

                    elif classification == "review":
                        self._create_candidate(
                            id_a, id_b, result, shared_companies, "pending",
                        )
                        stats["review_queued"] += 1

                    existing_pairs.add((id_a, id_b))

        self.session.commit()
        logger.info(
            f"Dedup scan complete: {stats['auto_merged']} auto-merged, "
            f"{stats['review_queued']} queued for review, "
            f"{stats['total_compared']} compared"
        )
        return stats

    def _get_shared_companies(self, person_id_1: int, person_id_2: int) -> List[int]:
        """Find company IDs where both people have current roles."""
        companies_1 = set(
            row[0] for row in self.session.query(CompanyPerson.company_id).filter(
                CompanyPerson.person_id == person_id_1,
                CompanyPerson.is_current == True,
            ).all()
        )
        companies_2 = set(
            row[0] for row in self.session.query(CompanyPerson.company_id).filter(
                CompanyPerson.person_id == person_id_2,
                CompanyPerson.is_current == True,
            ).all()
        )
        return list(companies_1 & companies_2)

    def _create_candidate(
        self,
        id_a: int,
        id_b: int,
        match_result,
        shared_companies: List[int],
        status: str,
        canonical_id: Optional[int] = None,
    ) -> PeopleMergeCandidate:
        """Create a merge candidate record."""
        candidate = PeopleMergeCandidate(
            person_id_a=id_a,
            person_id_b=id_b,
            match_type=match_result.match_type,
            similarity_score=match_result.similarity,
            shared_company_ids=shared_companies if shared_companies else None,
            evidence_notes=match_result.notes,
            status=status,
            canonical_person_id=canonical_id,
            reviewed_at=datetime.utcnow() if status != "pending" else None,
        )
        self.session.add(candidate)
        return candidate

    def _auto_merge(
        self,
        id_a: int,
        id_b: int,
        match_result,
        shared_companies: List[int],
    ) -> None:
        """
        Auto-merge two person records.

        Picks the canonical person (the one with more data), transfers
        missing fields from the duplicate, reassigns FK references,
        and marks the duplicate as non-canonical.
        """
        person_a = self.session.get(Person, id_a)
        person_b = self.session.get(Person, id_b)

        if not person_a or not person_b:
            return

        # Pick canonical: the person with more data
        canonical, duplicate = self._pick_canonical(person_a, person_b)

        self._merge_person_data(canonical, duplicate)
        self._reassign_references(canonical.id, duplicate.id)

        # Mark duplicate
        duplicate.canonical_id = canonical.id
        duplicate.is_canonical = False

        # Record in merge candidates
        self._create_candidate(
            id_a, id_b, match_result, shared_companies,
            status="auto_merged",
            canonical_id=canonical.id,
        )

    def _pick_canonical(self, person_a: Person, person_b: Person) -> tuple:
        """
        Pick which person record to keep as canonical.

        Prefers: more CompanyPerson records > has LinkedIn > has email > lower ID.
        """
        def score(p: Person) -> tuple:
            cp_count = self.session.query(CompanyPerson).filter(
                CompanyPerson.person_id == p.id,
            ).count()
            return (
                cp_count,
                1 if p.linkedin_url else 0,
                1 if p.email else 0,
                1 if p.bio else 0,
                -p.id,  # Prefer lower ID as tiebreaker
            )

        score_a = score(person_a)
        score_b = score(person_b)

        if score_a >= score_b:
            return (person_a, person_b)
        return (person_b, person_a)

    def _merge_person_data(self, canonical: Person, duplicate: Person) -> None:
        """Transfer missing fields from duplicate to canonical."""
        if not canonical.email and duplicate.email:
            canonical.email = duplicate.email
            canonical.email_confidence = duplicate.email_confidence

        if not canonical.linkedin_url and duplicate.linkedin_url:
            canonical.linkedin_url = duplicate.linkedin_url
            canonical.linkedin_id = duplicate.linkedin_id

        if not canonical.bio and duplicate.bio:
            canonical.bio = duplicate.bio
            canonical.bio_source = duplicate.bio_source

        if not canonical.photo_url and duplicate.photo_url:
            canonical.photo_url = duplicate.photo_url

        if not canonical.phone and duplicate.phone:
            canonical.phone = duplicate.phone

        if not canonical.twitter_url and duplicate.twitter_url:
            canonical.twitter_url = duplicate.twitter_url

        if not canonical.personal_website and duplicate.personal_website:
            canonical.personal_website = duplicate.personal_website

        if not canonical.city and duplicate.city:
            canonical.city = duplicate.city
            canonical.state = duplicate.state or canonical.state
            canonical.country = duplicate.country or canonical.country

        if not canonical.birth_year and duplicate.birth_year:
            canonical.birth_year = duplicate.birth_year

        # Merge data sources
        sources_a = set(canonical.data_sources or [])
        sources_b = set(duplicate.data_sources or [])
        merged_sources = list(sources_a | sources_b)
        if merged_sources:
            canonical.data_sources = merged_sources

    def _reassign_references(self, canonical_id: int, duplicate_id: int) -> None:
        """
        Reassign FK references from duplicate to canonical.

        Handles unique constraint conflicts by skipping duplicate relationships.
        """
        # Reassign CompanyPerson records
        dup_cps = self.session.query(CompanyPerson).filter(
            CompanyPerson.person_id == duplicate_id,
        ).all()

        for cp in dup_cps:
            # Check if canonical already has this role at this company
            existing = self.session.query(CompanyPerson).filter(
                CompanyPerson.person_id == canonical_id,
                CompanyPerson.company_id == cp.company_id,
                CompanyPerson.title == cp.title,
                CompanyPerson.is_current == cp.is_current,
            ).first()

            if existing:
                # Merge work_email if canonical's CP lacks it
                if not existing.work_email and cp.work_email:
                    existing.work_email = cp.work_email
                self.session.delete(cp)
            else:
                cp.person_id = canonical_id

        # Reassign PersonExperience records
        dup_exps = self.session.query(PersonExperience).filter(
            PersonExperience.person_id == duplicate_id,
        ).all()

        for exp in dup_exps:
            existing = self.session.query(PersonExperience).filter(
                PersonExperience.person_id == canonical_id,
                PersonExperience.company_name == exp.company_name,
                PersonExperience.title == exp.title,
                PersonExperience.start_year == exp.start_year,
            ).first()

            if existing:
                self.session.delete(exp)
            else:
                exp.person_id = canonical_id

        # Reassign PersonEducation records
        dup_edus = self.session.query(PersonEducation).filter(
            PersonEducation.person_id == duplicate_id,
        ).all()

        for edu in dup_edus:
            existing = self.session.query(PersonEducation).filter(
                PersonEducation.person_id == canonical_id,
                PersonEducation.institution == edu.institution,
                PersonEducation.degree == edu.degree,
            ).first()

            if existing:
                self.session.delete(edu)
            else:
                edu.person_id = canonical_id

    def manual_merge(
        self,
        candidate_id: int,
        canonical_person_id: int,
    ) -> Dict[str, Any]:
        """
        Approve and execute a merge from the review queue.

        Args:
            candidate_id: The PeopleMergeCandidate record ID.
            canonical_person_id: Which person to keep.

        Returns:
            Dict with merge result details.
        """
        candidate = self.session.get(PeopleMergeCandidate, candidate_id)
        if not candidate:
            return {"error": "Candidate not found"}

        if candidate.status not in ("pending",):
            return {"error": f"Candidate already {candidate.status}"}

        # Determine canonical and duplicate
        if canonical_person_id == candidate.person_id_a:
            duplicate_id = candidate.person_id_b
        elif canonical_person_id == candidate.person_id_b:
            duplicate_id = candidate.person_id_a
        else:
            return {"error": "canonical_person_id must be one of the candidate pair"}

        canonical = self.session.get(Person, canonical_person_id)
        duplicate = self.session.get(Person, duplicate_id)

        if not canonical or not duplicate:
            return {"error": "Person record not found"}

        # Execute merge
        self._merge_person_data(canonical, duplicate)
        self._reassign_references(canonical.id, duplicate.id)

        duplicate.canonical_id = canonical.id
        duplicate.is_canonical = False

        # Update candidate record
        candidate.status = "approved"
        candidate.canonical_person_id = canonical.id
        candidate.reviewed_at = datetime.utcnow()

        self.session.commit()

        return {
            "status": "merged",
            "canonical_id": canonical.id,
            "duplicate_id": duplicate.id,
            "candidate_id": candidate_id,
        }

    def reject_merge(self, candidate_id: int) -> Dict[str, Any]:
        """Reject a merge candidate."""
        candidate = self.session.get(PeopleMergeCandidate, candidate_id)
        if not candidate:
            return {"error": "Candidate not found"}

        if candidate.status not in ("pending",):
            return {"error": f"Candidate already {candidate.status}"}

        candidate.status = "rejected"
        candidate.reviewed_at = datetime.utcnow()
        self.session.commit()

        return {
            "status": "rejected",
            "candidate_id": candidate_id,
        }

    def get_pending_candidates(
        self,
        limit: int = 50,
        offset: int = 0,
        status: str = "pending",
    ) -> List[Dict[str, Any]]:
        """
        Return enriched candidate list with both persons' details.
        """
        candidates = (
            self.session.query(PeopleMergeCandidate)
            .filter(PeopleMergeCandidate.status == status)
            .order_by(PeopleMergeCandidate.similarity_score.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

        results = []
        for c in candidates:
            person_a = self.session.get(Person, c.person_id_a)
            person_b = self.session.get(Person, c.person_id_b)

            if not person_a or not person_b:
                continue

            results.append({
                "id": c.id,
                "person_a": self._person_summary(person_a),
                "person_b": self._person_summary(person_b),
                "match_type": c.match_type,
                "similarity_score": float(c.similarity_score) if c.similarity_score else None,
                "shared_company_ids": c.shared_company_ids,
                "evidence_notes": c.evidence_notes,
                "status": c.status,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            })

        return results

    def get_merge_history(
        self,
        person_id: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return merge decision history."""
        query = self.session.query(PeopleMergeCandidate).filter(
            PeopleMergeCandidate.status.in_(["auto_merged", "approved", "rejected"]),
        )

        if person_id:
            query = query.filter(
                or_(
                    PeopleMergeCandidate.person_id_a == person_id,
                    PeopleMergeCandidate.person_id_b == person_id,
                )
            )

        candidates = (
            query
            .order_by(PeopleMergeCandidate.reviewed_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

        results = []
        for c in candidates:
            person_a = self.session.get(Person, c.person_id_a)
            person_b = self.session.get(Person, c.person_id_b)

            results.append({
                "id": c.id,
                "person_a": self._person_summary(person_a) if person_a else None,
                "person_b": self._person_summary(person_b) if person_b else None,
                "match_type": c.match_type,
                "similarity_score": float(c.similarity_score) if c.similarity_score else None,
                "status": c.status,
                "canonical_person_id": c.canonical_person_id,
                "reviewed_at": c.reviewed_at.isoformat() if c.reviewed_at else None,
            })

        return results

    def _person_summary(self, person: Person) -> Dict[str, Any]:
        """Build a summary dict for a person record."""
        cp_count = self.session.query(CompanyPerson).filter(
            CompanyPerson.person_id == person.id,
        ).count()

        current_roles = (
            self.session.query(CompanyPerson)
            .filter(
                CompanyPerson.person_id == person.id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        return {
            "id": person.id,
            "full_name": person.full_name,
            "first_name": person.first_name,
            "last_name": person.last_name,
            "email": person.email,
            "linkedin_url": person.linkedin_url,
            "photo_url": person.photo_url,
            "is_canonical": person.is_canonical,
            "total_roles": cp_count,
            "current_roles": [
                {
                    "company_id": cp.company_id,
                    "title": cp.title,
                    "work_email": cp.work_email,
                }
                for cp in current_roles
            ],
        }
