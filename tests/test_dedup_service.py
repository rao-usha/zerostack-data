"""
Unit tests for dedup service.

Uses mock sessions to test scan/merge/reject logic without a real database.
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Pre-load person_matcher via importlib to avoid aiohttp dependency chain
import importlib.util

_fm_spec = importlib.util.spec_from_file_location(
    "fuzzy_matcher",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "agentic", "fuzzy_matcher.py")
)
_fm_mod = importlib.util.module_from_spec(_fm_spec)
sys.modules["app.agentic.fuzzy_matcher"] = _fm_mod
_fm_spec.loader.exec_module(_fm_mod)

_pm_spec = importlib.util.spec_from_file_location(
    "person_matcher",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "sources", "people_collection", "person_matcher.py")
)
_pm_mod = importlib.util.module_from_spec(_pm_spec)
sys.modules["app.sources.people_collection.person_matcher"] = _pm_mod
_pm_spec.loader.exec_module(_pm_mod)

# Mock out people_models to avoid SQLAlchemy/database dependency
# Save real module first so we can restore it after dedup_service loads
_real_people_models = sys.modules.get("app.core.people_models")
mock_models = MagicMock()
sys.modules["app.core.people_models"] = mock_models

# Eagerly import dedup_service while mock is active
from app.services.dedup_service import DedupService as _DedupService  # noqa: E402

# Restore real people_models so other test files aren't polluted
if _real_people_models is not None:
    sys.modules["app.core.people_models"] = _real_people_models
else:
    del sys.modules["app.core.people_models"]


def _make_mock_person(id, full_name, first_name, last_name, **kwargs):
    """Create a mock Person object."""
    p = MagicMock()
    p.id = id
    p.full_name = full_name
    p.first_name = first_name
    p.last_name = last_name
    p.email = kwargs.get("email")
    p.email_confidence = kwargs.get("email_confidence")
    p.linkedin_url = kwargs.get("linkedin_url")
    p.linkedin_id = kwargs.get("linkedin_id")
    p.bio = kwargs.get("bio")
    p.bio_source = kwargs.get("bio_source")
    p.photo_url = kwargs.get("photo_url")
    p.phone = kwargs.get("phone")
    p.twitter_url = kwargs.get("twitter_url")
    p.personal_website = kwargs.get("personal_website")
    p.city = kwargs.get("city")
    p.state = kwargs.get("state")
    p.country = kwargs.get("country")
    p.birth_year = kwargs.get("birth_year")
    p.data_sources = kwargs.get("data_sources", ["website"])
    p.is_canonical = kwargs.get("is_canonical", True)
    p.canonical_id = kwargs.get("canonical_id")
    return p


def _make_mock_candidate(id, person_id_a, person_id_b, status="pending", **kwargs):
    """Create a mock PeopleMergeCandidate object."""
    c = MagicMock()
    c.id = id
    c.person_id_a = person_id_a
    c.person_id_b = person_id_b
    c.status = status
    c.match_type = kwargs.get("match_type", "name_fuzzy")
    c.similarity_score = kwargs.get("similarity_score", 0.90)
    c.shared_company_ids = kwargs.get("shared_company_ids")
    c.evidence_notes = kwargs.get("evidence_notes")
    c.canonical_person_id = kwargs.get("canonical_person_id")
    c.reviewed_at = kwargs.get("reviewed_at")
    c.created_at = kwargs.get("created_at", datetime.utcnow())
    return c


class TestDedupServiceMergeData:
    """Tests for merge data transfer logic."""

    @pytest.mark.unit
    def test_merge_transfers_missing_email(self):
        """Email is transferred from duplicate to canonical if canonical lacks it."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        canonical = _make_mock_person(1, "John Smith", "John", "Smith", email=None)
        duplicate = _make_mock_person(2, "John Smith", "John", "Smith", email="john@example.com", email_confidence="verified")

        service._merge_person_data(canonical, duplicate)

        assert canonical.email == "john@example.com"
        assert canonical.email_confidence == "verified"

    @pytest.mark.unit
    def test_merge_preserves_existing_email(self):
        """Canonical email is not overwritten by duplicate."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        canonical = _make_mock_person(1, "John Smith", "John", "Smith", email="john@company.com")
        duplicate = _make_mock_person(2, "John Smith", "John", "Smith", email="john@example.com")

        service._merge_person_data(canonical, duplicate)

        assert canonical.email == "john@company.com"

    @pytest.mark.unit
    def test_merge_transfers_linkedin(self):
        """LinkedIn URL is transferred if canonical lacks it."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        canonical = _make_mock_person(1, "John Smith", "John", "Smith")
        duplicate = _make_mock_person(2, "John Smith", "John", "Smith",
                                      linkedin_url="https://linkedin.com/in/jsmith",
                                      linkedin_id="jsmith")

        service._merge_person_data(canonical, duplicate)

        assert canonical.linkedin_url == "https://linkedin.com/in/jsmith"
        assert canonical.linkedin_id == "jsmith"

    @pytest.mark.unit
    def test_merge_combines_data_sources(self):
        """Data sources from both records are combined."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        canonical = _make_mock_person(1, "John Smith", "John", "Smith", data_sources=["website"])
        duplicate = _make_mock_person(2, "John Smith", "John", "Smith", data_sources=["sec"])

        service._merge_person_data(canonical, duplicate)

        assert set(canonical.data_sources) == {"website", "sec"}


class TestDedupServicePickCanonical:
    """Tests for picking the canonical record."""

    @pytest.mark.unit
    def test_picks_person_with_more_roles(self):
        """Person with more company relationships is canonical."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        person_a = _make_mock_person(1, "John Smith", "John", "Smith")
        person_b = _make_mock_person(2, "John Smith", "John", "Smith",
                                     linkedin_url="https://linkedin.com/in/jsmith")

        # person_a has 3 roles, person_b has 1
        count_mock = MagicMock()
        count_mock.count = MagicMock(side_effect=[3, 1])
        session.query.return_value.filter.return_value = count_mock

        canonical, duplicate = service._pick_canonical(person_a, person_b)

        assert canonical.id == 1

    @pytest.mark.unit
    def test_picks_person_with_linkedin_as_tiebreaker(self):
        """When role counts are equal, person with LinkedIn wins."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        person_a = _make_mock_person(1, "John Smith", "John", "Smith")
        person_b = _make_mock_person(2, "John Smith", "John", "Smith",
                                     linkedin_url="https://linkedin.com/in/jsmith")

        # Same role count
        count_mock = MagicMock()
        count_mock.count = MagicMock(side_effect=[1, 1])
        session.query.return_value.filter.return_value = count_mock

        canonical, duplicate = service._pick_canonical(person_a, person_b)

        assert canonical.id == 2  # person_b has linkedin


class TestDedupServiceReject:
    """Tests for rejecting merge candidates."""

    @pytest.mark.unit
    def test_reject_pending_candidate(self):
        """Pending candidate can be rejected."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        candidate = _make_mock_candidate(1, 10, 20, status="pending")
        session.get.return_value = candidate

        result = service.reject_merge(1)

        assert result["status"] == "rejected"
        assert candidate.status == "rejected"
        session.commit.assert_called_once()

    @pytest.mark.unit
    def test_reject_already_merged(self):
        """Already merged candidate cannot be rejected."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)

        candidate = _make_mock_candidate(1, 10, 20, status="auto_merged")
        session.get.return_value = candidate

        result = service.reject_merge(1)

        assert "error" in result

    @pytest.mark.unit
    def test_reject_not_found(self):
        """Non-existent candidate returns error."""
        from app.services.dedup_service import DedupService

        session = MagicMock()
        service = DedupService(session)
        session.get.return_value = None

        result = service.reject_merge(999)

        assert "error" in result
