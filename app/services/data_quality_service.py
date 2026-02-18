"""
Data Quality Service for People Intelligence Platform.

Provides data quality scoring, deduplication, and enrichment tracking.
"""

from typing import Optional, List, Dict, Any, Tuple
from datetime import date, timedelta
from collections import defaultdict
import re
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from app.core.people_models import (
    Person,
    CompanyPerson,
    IndustrialCompany,
    PersonExperience,
    PersonEducation,
)


class DataQualityService:
    """Service for managing data quality and enrichment."""

    def __init__(self, db: Session):
        self.db = db

    def get_overall_stats(self) -> Dict[str, Any]:
        """
        Get overall data quality statistics.

        Returns counts, coverage percentages, and quality scores.
        """
        # Total counts
        total_people = self.db.query(Person).count()
        total_companies = self.db.query(IndustrialCompany).count()
        total_positions = (
            self.db.query(CompanyPerson)
            .filter(CompanyPerson.is_current == True)
            .count()
        )

        if total_people == 0:
            return self._empty_stats()

        # LinkedIn coverage
        with_linkedin = (
            self.db.query(Person)
            .filter(
                Person.linkedin_url.isnot(None),
                Person.linkedin_url != "",
            )
            .count()
        )

        # Photo coverage
        with_photo = (
            self.db.query(Person)
            .filter(
                Person.photo_url.isnot(None),
                Person.photo_url != "",
            )
            .count()
        )

        # Email coverage
        with_email = (
            self.db.query(Person)
            .filter(
                Person.email.isnot(None),
                Person.email != "",
            )
            .count()
        )

        # Bio coverage
        with_bio = (
            self.db.query(Person)
            .filter(
                Person.bio.isnot(None),
                Person.bio != "",
            )
            .count()
        )

        # Experience records
        with_experience = (
            self.db.query(
                func.count(func.distinct(PersonExperience.person_id))
            ).scalar()
            or 0
        )

        # Education records
        with_education = (
            self.db.query(func.count(func.distinct(PersonEducation.person_id))).scalar()
            or 0
        )

        # Confidence scores
        confidence_scores = (
            self.db.query(Person.confidence_score)
            .filter(Person.confidence_score.isnot(None))
            .all()
        )
        avg_confidence = (
            sum(float(c[0]) for c in confidence_scores) / len(confidence_scores)
            if confidence_scores
            else None
        )

        # Data freshness
        thirty_days_ago = date.today() - timedelta(days=30)
        recently_verified = (
            self.db.query(Person)
            .filter(Person.last_verified_date >= thirty_days_ago)
            .count()
        )

        # Companies with leadership data
        companies_with_leadership = (
            self.db.query(func.count(func.distinct(CompanyPerson.company_id)))
            .filter(CompanyPerson.is_current == True)
            .scalar()
            or 0
        )

        return {
            "total_people": total_people,
            "total_companies": total_companies,
            "total_active_positions": total_positions,
            "companies_with_leadership": companies_with_leadership,
            "coverage": {
                "linkedin": round(with_linkedin / total_people * 100, 1),
                "photo": round(with_photo / total_people * 100, 1),
                "email": round(with_email / total_people * 100, 1),
                "bio": round(with_bio / total_people * 100, 1),
                "experience": round(with_experience / total_people * 100, 1),
                "education": round(with_education / total_people * 100, 1),
            },
            "counts": {
                "with_linkedin": with_linkedin,
                "with_photo": with_photo,
                "with_email": with_email,
                "with_bio": with_bio,
                "with_experience": with_experience,
                "with_education": with_education,
            },
            "avg_confidence_score": round(avg_confidence, 2)
            if avg_confidence
            else None,
            "recently_verified_count": recently_verified,
            "recently_verified_pct": round(recently_verified / total_people * 100, 1),
        }

    def _empty_stats(self) -> Dict[str, Any]:
        """Return empty stats structure."""
        return {
            "total_people": 0,
            "total_companies": 0,
            "total_active_positions": 0,
            "companies_with_leadership": 0,
            "coverage": {
                "linkedin": 0,
                "photo": 0,
                "email": 0,
                "bio": 0,
                "experience": 0,
                "education": 0,
            },
            "counts": {
                "with_linkedin": 0,
                "with_photo": 0,
                "with_email": 0,
                "with_bio": 0,
                "with_experience": 0,
                "with_education": 0,
            },
            "avg_confidence_score": None,
            "recently_verified_count": 0,
            "recently_verified_pct": 0,
        }

    def get_freshness_stats(self) -> Dict[str, Any]:
        """
        Get data freshness statistics.

        Shows how old the data is across different time buckets.
        """
        total_people = self.db.query(Person).count()
        if total_people == 0:
            return {
                "total_people": 0,
                "freshness_buckets": {
                    "0-7_days": 0,
                    "8-30_days": 0,
                    "31-90_days": 0,
                    "91-180_days": 0,
                    "181-365_days": 0,
                    "over_365_days": 0,
                    "never_verified": 0,
                },
                "median_age_days": None,
                "stale_count": 0,
                "stale_pct": 0.0,
            }

        today = date.today()
        buckets = {
            "0-7_days": 0,
            "8-30_days": 0,
            "31-90_days": 0,
            "91-180_days": 0,
            "181-365_days": 0,
            "over_365_days": 0,
            "never_verified": 0,
        }

        ages = []
        for person in self.db.query(Person).all():
            if person.last_verified_date:
                age = (today - person.last_verified_date).days
                ages.append(age)

                if age <= 7:
                    buckets["0-7_days"] += 1
                elif age <= 30:
                    buckets["8-30_days"] += 1
                elif age <= 90:
                    buckets["31-90_days"] += 1
                elif age <= 180:
                    buckets["91-180_days"] += 1
                elif age <= 365:
                    buckets["181-365_days"] += 1
                else:
                    buckets["over_365_days"] += 1
            else:
                buckets["never_verified"] += 1

        # Calculate median age
        median_age = None
        if ages:
            sorted_ages = sorted(ages)
            mid = len(sorted_ages) // 2
            median_age = sorted_ages[mid]

        # Stale = over 90 days or never verified
        stale_count = (
            buckets["91-180_days"]
            + buckets["181-365_days"]
            + buckets["over_365_days"]
            + buckets["never_verified"]
        )

        return {
            "total_people": total_people,
            "freshness_buckets": buckets,
            "median_age_days": median_age,
            "stale_count": stale_count,
            "stale_pct": round(stale_count / total_people * 100, 1),
        }

    def calculate_person_quality_score(self, person_id: int) -> Dict[str, Any]:
        """
        Calculate data quality score for a single person.

        Returns 0-100 score with breakdown by component.
        """
        person = self.db.get(Person, person_id)
        if not person:
            return {"error": "Person not found"}

        # Component scores (each 0-20, total 100)
        scores = {}

        # 1. Identity completeness (20 pts)
        identity_score = 0
        if person.full_name:
            identity_score += 5
        if person.first_name and person.last_name:
            identity_score += 5
        if person.linkedin_url:
            identity_score += 10
        scores["identity"] = identity_score

        # 2. Contact info (20 pts)
        contact_score = 0
        if person.email:
            contact_score += 10
        if person.phone:
            contact_score += 5
        if person.photo_url:
            contact_score += 5
        scores["contact"] = contact_score

        # 3. Professional info (20 pts)
        professional_score = 0
        if person.bio:
            professional_score += 10

        # Check for current role
        current_role = (
            self.db.query(CompanyPerson)
            .filter(
                CompanyPerson.person_id == person_id,
                CompanyPerson.is_current == True,
            )
            .first()
        )
        if current_role:
            professional_score += 5
            if current_role.title_level:
                professional_score += 5
        scores["professional"] = professional_score

        # 4. Experience/Education (20 pts)
        history_score = 0
        exp_count = (
            self.db.query(PersonExperience)
            .filter(PersonExperience.person_id == person_id)
            .count()
        )
        if exp_count >= 3:
            history_score += 10
        elif exp_count >= 1:
            history_score += 5

        edu_count = (
            self.db.query(PersonEducation)
            .filter(PersonEducation.person_id == person_id)
            .count()
        )
        if edu_count >= 1:
            history_score += 10
        scores["history"] = history_score

        # 5. Data freshness (20 pts)
        freshness_score = 0
        if person.last_verified_date:
            age_days = (date.today() - person.last_verified_date).days
            if age_days <= 7:
                freshness_score = 20
            elif age_days <= 30:
                freshness_score = 15
            elif age_days <= 90:
                freshness_score = 10
            elif age_days <= 180:
                freshness_score = 5
        scores["freshness"] = freshness_score

        total_score = sum(scores.values())

        return {
            "person_id": person_id,
            "person_name": person.full_name,
            "quality_score": total_score,
            "components": scores,
            "issues": self._identify_quality_issues(person, scores),
        }

    def _identify_quality_issues(
        self,
        person: Person,
        scores: Dict[str, int],
    ) -> List[str]:
        """Identify specific quality issues for a person."""
        issues = []

        if not person.linkedin_url:
            issues.append("Missing LinkedIn URL")
        if not person.email:
            issues.append("Missing email")
        if not person.photo_url:
            issues.append("Missing photo")
        if not person.bio:
            issues.append("Missing bio")
        if scores["history"] < 10:
            issues.append("Limited work history")
        if scores["freshness"] < 10:
            issues.append("Data may be stale")

        return issues

    def find_potential_duplicates(
        self,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Find potential duplicate person records.

        Uses name similarity and LinkedIn matching.
        """
        duplicates = []

        # Method 1: Same LinkedIn URL
        linkedin_groups = defaultdict(list)
        for person in (
            self.db.query(Person)
            .filter(
                Person.linkedin_url.isnot(None),
                Person.linkedin_url != "",
            )
            .all()
        ):
            # Normalize LinkedIn URL
            url = person.linkedin_url.lower().rstrip("/")
            linkedin_groups[url].append(person)

        for url, people in linkedin_groups.items():
            if len(people) > 1:
                duplicates.append(
                    {
                        "match_type": "linkedin_url",
                        "match_value": url,
                        "people": [{"id": p.id, "name": p.full_name} for p in people],
                    }
                )

        # Method 2: Exact name match
        name_groups = defaultdict(list)
        for person in self.db.query(Person).all():
            # Normalize name
            name = self._normalize_name(person.full_name)
            if name:
                name_groups[name].append(person)

        for name, people in name_groups.items():
            if len(people) > 1:
                # Check if already captured by LinkedIn match
                already_found = any(
                    d["match_type"] == "linkedin_url"
                    and set(p["id"] for p in d["people"]) == set(p.id for p in people)
                    for d in duplicates
                )
                if not already_found:
                    duplicates.append(
                        {
                            "match_type": "exact_name",
                            "match_value": name,
                            "people": [
                                {
                                    "id": p.id,
                                    "name": p.full_name,
                                    "linkedin": p.linkedin_url,
                                }
                                for p in people
                            ],
                        }
                    )

        return duplicates[:limit]

    def _normalize_name(self, name: str) -> str:
        """Normalize a name for comparison."""
        if not name:
            return ""
        # Remove common suffixes
        name = re.sub(r"\b(jr|sr|ii|iii|iv|phd|md|mba|cpa|esq)\.?\b", "", name.lower())
        # Remove punctuation and extra spaces
        name = re.sub(r"[^\w\s]", "", name)
        name = " ".join(name.split())
        return name.strip()

    def merge_duplicates(
        self,
        canonical_id: int,
        duplicate_ids: List[int],
    ) -> Dict[str, Any]:
        """
        Merge duplicate person records.

        Keeps canonical_id as the master record, marks others as duplicates.
        """
        canonical = self.db.get(Person, canonical_id)
        if not canonical:
            return {"error": "Canonical person not found"}

        merged_count = 0
        for dup_id in duplicate_ids:
            if dup_id == canonical_id:
                continue

            duplicate = self.db.get(Person, dup_id)
            if not duplicate:
                continue

            # Transfer any missing data to canonical
            if not canonical.linkedin_url and duplicate.linkedin_url:
                canonical.linkedin_url = duplicate.linkedin_url
            if not canonical.email and duplicate.email:
                canonical.email = duplicate.email
            if not canonical.phone and duplicate.phone:
                canonical.phone = duplicate.phone
            if not canonical.photo_url and duplicate.photo_url:
                canonical.photo_url = duplicate.photo_url
            if not canonical.bio and duplicate.bio:
                canonical.bio = duplicate.bio

            # Update references
            self.db.query(CompanyPerson).filter(
                CompanyPerson.person_id == dup_id
            ).update({"person_id": canonical_id})

            self.db.query(PersonExperience).filter(
                PersonExperience.person_id == dup_id
            ).update({"person_id": canonical_id})

            self.db.query(PersonEducation).filter(
                PersonEducation.person_id == dup_id
            ).update({"person_id": canonical_id})

            # Mark duplicate
            duplicate.canonical_id = canonical_id
            duplicate.is_canonical = False

            merged_count += 1

        self.db.commit()

        return {
            "canonical_id": canonical_id,
            "merged_count": merged_count,
            "status": "success",
        }

    def get_enrichment_queue(
        self,
        enrichment_type: str = "all",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get people that need enrichment.

        Returns prioritized list based on what's missing.
        """
        query = self.db.query(Person).filter(Person.is_canonical == True)

        if enrichment_type == "linkedin":
            query = query.filter(
                or_(Person.linkedin_url.is_(None), Person.linkedin_url == "")
            )
        elif enrichment_type == "email":
            query = query.filter(or_(Person.email.is_(None), Person.email == ""))
        elif enrichment_type == "photo":
            query = query.filter(
                or_(Person.photo_url.is_(None), Person.photo_url == "")
            )
        elif enrichment_type == "bio":
            query = query.filter(or_(Person.bio.is_(None), Person.bio == ""))

        # Prioritize people with current positions
        people = query.limit(limit * 2).all()

        # Score and sort by priority
        scored = []
        for person in people:
            # Check if has current role
            has_current_role = (
                self.db.query(CompanyPerson)
                .filter(
                    CompanyPerson.person_id == person.id,
                    CompanyPerson.is_current == True,
                )
                .first()
                is not None
            )

            # Priority score (higher = more important to enrich)
            priority = 0
            if has_current_role:
                priority += 50
            if person.linkedin_url:
                priority += 20  # Easier to enrich with LinkedIn
            if not person.email:
                priority += 10
            if not person.photo_url:
                priority += 5

            scored.append(
                {
                    "person_id": person.id,
                    "full_name": person.full_name,
                    "linkedin_url": person.linkedin_url,
                    "has_email": bool(person.email),
                    "has_photo": bool(person.photo_url),
                    "has_bio": bool(person.bio),
                    "has_current_role": has_current_role,
                    "priority_score": priority,
                }
            )

        # Sort by priority descending
        scored.sort(key=lambda x: x["priority_score"], reverse=True)
        return scored[:limit]

    def update_confidence_score(self, person_id: int) -> Dict[str, Any]:
        """
        Recalculate and update confidence score for a person.
        """
        quality = self.calculate_person_quality_score(person_id)
        if "error" in quality:
            return quality

        # Convert quality score (0-100) to confidence (0.00-1.00)
        confidence = quality["quality_score"] / 100.0

        person = self.db.get(Person, person_id)
        person.confidence_score = confidence
        self.db.commit()

        return {
            "person_id": person_id,
            "confidence_score": round(confidence, 2),
            "quality_score": quality["quality_score"],
        }
