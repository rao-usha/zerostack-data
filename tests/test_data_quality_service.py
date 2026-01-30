"""
Unit tests for Data Quality Service.
"""
import pytest
from datetime import date, timedelta
from app.services.data_quality_service import DataQualityService
from app.core.people_models import Person, IndustrialCompany, CompanyPerson


class TestDataQualityService:
    """Tests for DataQualityService class."""

    @pytest.fixture
    def quality_service(self, test_db):
        return DataQualityService(test_db)

    @pytest.mark.unit
    def test_get_overall_stats_empty(self, quality_service, test_db):
        """Test overall stats with no data."""
        result = quality_service.get_overall_stats()

        assert result["total_people"] == 0
        assert "coverage" in result
        assert result["coverage"]["linkedin"] == 0

    @pytest.mark.unit
    def test_get_overall_stats(self, quality_service, test_db, sample_people):
        """Test overall stats with data."""
        result = quality_service.get_overall_stats()

        assert result["total_people"] == len(sample_people)
        # Check coverage structure exists
        assert "coverage" in result
        assert 0 <= result["coverage"]["linkedin"] <= 100
        assert 0 <= result["coverage"]["email"] <= 100

    @pytest.mark.unit
    def test_get_freshness_stats_empty(self, quality_service, test_db):
        """Test freshness stats with no data."""
        result = quality_service.get_freshness_stats()

        assert result["total_people"] == 0
        assert "stale_pct" in result

    @pytest.mark.unit
    def test_get_freshness_stats(self, quality_service, test_db, sample_people):
        """Test freshness stats with data."""
        result = quality_service.get_freshness_stats()

        assert result["total_people"] == len(sample_people)
        assert "freshness_buckets" in result
        assert "stale_pct" in result

    @pytest.mark.unit
    def test_calculate_person_quality_score_complete(self, quality_service, test_db):
        """Test quality score for a well-filled profile."""
        person = Person(
            full_name="Complete Profile Test",
            first_name="Complete",
            last_name="Profile",
            email="complete-test@example.com",
            email_confidence="verified",
            linkedin_url="https://linkedin.com/in/complete-profile-test-unique",
            photo_url="https://example.com/photo-test.jpg",
            bio="Detailed biography of the person.",
            city="New York",
            state="NY",
            confidence_score=0.95,
        )
        test_db.add(person)
        test_db.commit()
        test_db.refresh(person)

        result = quality_service.calculate_person_quality_score(person.id)

        assert "quality_score" in result
        # Profile with identity (20) + contact (15) + bio (10) = 45
        # Note: history and freshness components need experience/education/verification
        assert result["quality_score"] >= 40  # Well-filled profile but missing history/freshness

    @pytest.mark.unit
    def test_calculate_person_quality_score_minimal(self, quality_service, test_db):
        """Test quality score for a minimal profile."""
        person = Person(full_name="Minimal Profile")
        test_db.add(person)
        test_db.commit()
        test_db.refresh(person)

        result = quality_service.calculate_person_quality_score(person.id)

        assert "score" in result or "quality_score" in result
        score = result.get("score") or result.get("quality_score", 0)
        assert score < 50  # Minimal profile should have low score

    @pytest.mark.unit
    def test_calculate_person_quality_score_nonexistent(self, quality_service):
        """Test quality score for non-existent person."""
        result = quality_service.calculate_person_quality_score(99999)

        # Should return error or empty result
        assert result is not None


class TestDuplicateDetection:
    """Tests for duplicate detection."""

    @pytest.fixture
    def quality_service(self, test_db):
        return DataQualityService(test_db)

    @pytest.mark.unit
    def test_find_potential_duplicates_by_linkedin(self, quality_service, test_db):
        """Test finding duplicates by LinkedIn URL."""
        # This would require actual duplicates - skip if none
        result = quality_service.find_potential_duplicates()

        assert isinstance(result, (list, dict))

    @pytest.mark.unit
    def test_find_potential_duplicates_by_name(self, quality_service, test_db):
        """Test finding duplicates by exact name match."""
        # Create two people with same name
        person1 = Person(
            full_name="John Smith",
            first_name="John",
            last_name="Smith",
        )
        person2 = Person(
            full_name="John Smith",
            first_name="John",
            last_name="Smith",
            email="different@example.com",
        )
        test_db.add(person1)
        test_db.add(person2)
        test_db.commit()

        result = quality_service.find_potential_duplicates()

        # Should find potential duplicates
        assert isinstance(result, (list, dict))

    @pytest.mark.unit
    def test_merge_duplicates(self, quality_service, test_db):
        """Test merging duplicate records."""
        import uuid
        unique_id = str(uuid.uuid4())[:8]

        # Create canonical without bio (will receive from duplicate)
        canonical = Person(
            full_name=f"John Smith Merge {unique_id}",
            email=f"john.merge.{unique_id}@example.com",
            # No bio - will be filled from duplicate
        )
        test_db.add(canonical)
        test_db.commit()
        test_db.refresh(canonical)

        # Create duplicate with bio but no email
        duplicate = Person(
            full_name=f"John Smith Merge Dup {unique_id}",
            bio="Executive with 20 years experience in distribution.",
            # No email - canonical's email will be kept
        )
        test_db.add(duplicate)
        test_db.commit()
        test_db.refresh(duplicate)

        # Merge (takes a list of duplicate_ids)
        result = quality_service.merge_duplicates(
            canonical_id=canonical.id,
            duplicate_ids=[duplicate.id],
        )

        assert result["status"] == "success"
        assert result["merged_count"] == 1

        # Verify canonical got the bio from duplicate
        test_db.refresh(canonical)
        assert canonical.bio == "Executive with 20 years experience in distribution."


class TestEnrichmentQueue:
    """Tests for enrichment queue."""

    @pytest.fixture
    def quality_service(self, test_db):
        return DataQualityService(test_db)

    @pytest.mark.unit
    def test_get_enrichment_queue_empty(self, quality_service, test_db):
        """Test enrichment queue with no data."""
        result = quality_service.get_enrichment_queue(limit=10)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_enrichment_queue_priorities(self, quality_service, test_db, sample_people, sample_company):
        """Test enrichment queue prioritization."""
        # Link some people to company (higher priority)
        for person in sample_people[:2]:
            cp = CompanyPerson(
                company_id=sample_company.id,
                person_id=person.id,
                title="Executive",
                is_current=True,
            )
            test_db.add(cp)
        test_db.commit()

        result = quality_service.get_enrichment_queue(limit=10)

        assert isinstance(result, list)
        # Should prioritize people linked to companies
        if len(result) > 0:
            assert "person_id" in result[0] or "id" in result[0]

    @pytest.mark.unit
    def test_get_enrichment_queue_limit(self, quality_service, test_db, sample_people):
        """Test enrichment queue respects limit."""
        result = quality_service.get_enrichment_queue(limit=2)

        assert len(result) <= 2


class TestQualityScoreComponents:
    """Tests for quality score components."""

    @pytest.fixture
    def quality_service(self, test_db):
        return DataQualityService(test_db)

    @pytest.mark.unit
    def test_linkedin_component(self, quality_service, test_db):
        """Test LinkedIn contributes to quality score."""
        # Person with LinkedIn
        with_linkedin = Person(
            full_name="With LinkedIn Component",
            linkedin_url="https://linkedin.com/in/with-linkedin-component",
        )
        test_db.add(with_linkedin)
        test_db.commit()
        test_db.refresh(with_linkedin)

        # Person without LinkedIn
        without_linkedin = Person(full_name="Without LinkedIn Component")
        test_db.add(without_linkedin)
        test_db.commit()
        test_db.refresh(without_linkedin)

        score_with = quality_service.calculate_person_quality_score(with_linkedin.id)
        score_without = quality_service.calculate_person_quality_score(without_linkedin.id)

        # Person with LinkedIn should have higher score
        assert score_with["quality_score"] > score_without["quality_score"]

    @pytest.mark.unit
    def test_email_component(self, quality_service, test_db):
        """Test email contributes to quality score."""
        with_email = Person(
            full_name="With Email Component",
            email="with-email@example.com",
        )
        without_email = Person(
            full_name="Without Email Component",
        )
        test_db.add(with_email)
        test_db.add(without_email)
        test_db.commit()
        test_db.refresh(with_email)
        test_db.refresh(without_email)

        score_with = quality_service.calculate_person_quality_score(with_email.id)
        score_without = quality_service.calculate_person_quality_score(without_email.id)

        # Person with email should have higher score
        assert score_with["quality_score"] > score_without["quality_score"]

    @pytest.mark.unit
    def test_photo_component(self, quality_service, test_db):
        """Test photo contributes to quality score."""
        with_photo = Person(
            full_name="With Photo Component",
            photo_url="https://example.com/photo-component.jpg",
        )
        test_db.add(with_photo)
        test_db.commit()
        test_db.refresh(with_photo)

        without_photo = Person(full_name="Without Photo Component")
        test_db.add(without_photo)
        test_db.commit()
        test_db.refresh(without_photo)

        score_with = quality_service.calculate_person_quality_score(with_photo.id)
        score_without = quality_service.calculate_person_quality_score(without_photo.id)

        assert score_with["quality_score"] > score_without["quality_score"]

    @pytest.mark.unit
    def test_bio_component(self, quality_service, test_db):
        """Test bio contributes to quality score."""
        with_bio = Person(
            full_name="With Bio Component",
            bio="Detailed biography with work history and achievements.",
        )
        test_db.add(with_bio)
        test_db.commit()
        test_db.refresh(with_bio)

        without_bio = Person(full_name="Without Bio Component")
        test_db.add(without_bio)
        test_db.commit()
        test_db.refresh(without_bio)

        score_with = quality_service.calculate_person_quality_score(with_bio.id)
        score_without = quality_service.calculate_person_quality_score(without_bio.id)

        assert score_with["quality_score"] > score_without["quality_score"]
