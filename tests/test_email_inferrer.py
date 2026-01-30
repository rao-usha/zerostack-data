"""
Unit tests for email inference logic.
"""
import sys
import os
import pytest

# Add project root to path to allow direct module import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly from the module file to avoid __init__.py imports
import importlib.util
spec = importlib.util.spec_from_file_location(
    "email_inferrer",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "sources", "people_collection", "email_inferrer.py")
)
email_inferrer_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(email_inferrer_module)

EmailInferrer = email_inferrer_module.EmailInferrer
EmailPattern = email_inferrer_module.EmailPattern
InferredEmail = email_inferrer_module.InferredEmail
CompanyEmailPatternLearner = email_inferrer_module.CompanyEmailPatternLearner


class TestEmailPattern:
    """Tests for EmailPattern enum."""

    @pytest.mark.unit
    def test_pattern_values(self):
        """Test that all expected patterns exist."""
        patterns = [
            EmailPattern.FIRST_LAST,
            EmailPattern.FIRST_L,
            EmailPattern.F_LAST,
            EmailPattern.FIRST,
            EmailPattern.LAST,
            EmailPattern.FIRST_LAST_NO_DOT,
            EmailPattern.F_LAST_NO_DOT,
        ]
        assert len(patterns) >= 7


class TestEmailInferrer:
    """Tests for EmailInferrer class."""

    @pytest.fixture
    def inferrer(self):
        return EmailInferrer()

    @pytest.mark.unit
    def test_infer_email_first_last(self, inferrer):
        """Test inferring email with first.last pattern."""
        results = inferrer.infer_email(
            first_name="John",
            last_name="Smith",
            company_domain="example.com",
        )

        assert len(results) > 0
        emails = [r.email for r in results]
        assert "john.smith@example.com" in emails

    @pytest.mark.unit
    def test_infer_email_known_pattern(self, inferrer):
        """Test inferring email for known company pattern."""
        results = inferrer.infer_email(
            first_name="John",
            last_name="Smith",
            company_domain="fastenal.com",  # Known pattern
        )

        assert len(results) > 0
        # First result should be high confidence
        assert results[0].confidence == "high"

    @pytest.mark.unit
    def test_infer_email_empty_names(self, inferrer):
        """Test handling empty names."""
        results = inferrer.infer_email(
            first_name="",
            last_name="Smith",
            company_domain="example.com",
        )

        assert results == []

    @pytest.mark.unit
    def test_infer_email_empty_domain(self, inferrer):
        """Test handling empty domain."""
        results = inferrer.infer_email(
            first_name="John",
            last_name="Smith",
            company_domain="",
        )

        assert results == []

    @pytest.mark.unit
    def test_infer_email_www_domain(self, inferrer):
        """Test that www prefix is removed from domain."""
        results = inferrer.infer_email(
            first_name="John",
            last_name="Smith",
            company_domain="www.example.com",
        )

        assert len(results) > 0
        for r in results:
            assert "@example.com" in r.email
            assert "www" not in r.email

    @pytest.mark.unit
    def test_infer_email_returns_multiple_candidates(self, inferrer):
        """Test that multiple candidates are returned."""
        results = inferrer.infer_email(
            first_name="John",
            last_name="Smith",
            company_domain="example.com",
        )

        assert len(results) >= 3
        # Check different patterns
        emails = [r.email for r in results]
        assert any("john.smith@" in e for e in emails)
        assert any("jsmith@" in e or "j.smith@" in e for e in emails)


class TestNameNormalization:
    """Tests for name normalization."""

    @pytest.fixture
    def inferrer(self):
        return EmailInferrer()

    @pytest.mark.unit
    def test_normalize_lowercase(self, inferrer):
        """Test names are lowercased."""
        results = inferrer.infer_email(
            first_name="JOHN",
            last_name="SMITH",
            company_domain="example.com",
        )

        assert len(results) > 0
        for r in results:
            # Local part should be lowercase
            local = r.email.split("@")[0]
            assert local == local.lower()

    @pytest.mark.unit
    def test_normalize_suffixes_removed(self, inferrer):
        """Test that suffixes like Jr, III are removed."""
        results = inferrer.infer_email(
            first_name="John",
            last_name="Smith Jr",
            company_domain="example.com",
        )

        assert len(results) > 0
        # Should generate john.smith@ not john.smithjr@
        emails = [r.email for r in results]
        assert any("john.smith@" in e for e in emails)

    @pytest.mark.unit
    def test_normalize_special_chars_removed(self, inferrer):
        """Test that special characters are removed."""
        results = inferrer.infer_email(
            first_name="Mary-Jane",
            last_name="O'Brien",
            company_domain="example.com",
        )

        assert len(results) > 0
        for r in results:
            local = r.email.split("@")[0]
            # Should not contain hyphens or apostrophes
            assert "-" not in local or "." in local
            assert "'" not in local


class TestPatternLearning:
    """Tests for learning email patterns."""

    @pytest.fixture
    def inferrer(self):
        return EmailInferrer()

    @pytest.mark.unit
    def test_learn_pattern_first_last(self, inferrer):
        """Test learning first.last pattern."""
        pattern = inferrer.learn_pattern_from_email(
            email="john.smith@example.com",
            first_name="John",
            last_name="Smith",
        )

        assert pattern == EmailPattern.FIRST_LAST

    @pytest.mark.unit
    def test_learn_pattern_f_last(self, inferrer):
        """Test learning f.last pattern."""
        pattern = inferrer.learn_pattern_from_email(
            email="j.smith@example.com",
            first_name="John",
            last_name="Smith",
        )

        assert pattern == EmailPattern.F_LAST

    @pytest.mark.unit
    def test_learn_pattern_flast(self, inferrer):
        """Test learning flast pattern (no dot)."""
        pattern = inferrer.learn_pattern_from_email(
            email="jsmith@example.com",
            first_name="John",
            last_name="Smith",
        )

        assert pattern == EmailPattern.F_LAST_NO_DOT

    @pytest.mark.unit
    def test_learn_pattern_first_only(self, inferrer):
        """Test learning first-only pattern."""
        pattern = inferrer.learn_pattern_from_email(
            email="john@example.com",
            first_name="John",
            last_name="Smith",
        )

        assert pattern == EmailPattern.FIRST

    @pytest.mark.unit
    def test_learn_pattern_invalid_email(self, inferrer):
        """Test handling invalid email."""
        pattern = inferrer.learn_pattern_from_email(
            email="not-an-email",
            first_name="John",
            last_name="Smith",
        )

        assert pattern is None


class TestEmailValidation:
    """Tests for email validation."""

    @pytest.fixture
    def inferrer(self):
        return EmailInferrer()

    @pytest.mark.unit
    def test_validate_valid_email(self, inferrer):
        """Test validating valid email."""
        assert inferrer.validate_email_format("john.smith@example.com") is True
        assert inferrer.validate_email_format("john@example.co.uk") is True

    @pytest.mark.unit
    def test_validate_invalid_email(self, inferrer):
        """Test validating invalid email."""
        assert inferrer.validate_email_format("not-an-email") is False
        assert inferrer.validate_email_format("@example.com") is False
        assert inferrer.validate_email_format("john@") is False
        assert inferrer.validate_email_format("") is False


class TestDomainExtraction:
    """Tests for domain extraction from website."""

    @pytest.fixture
    def inferrer(self):
        return EmailInferrer()

    @pytest.mark.unit
    def test_extract_domain_https(self, inferrer):
        """Test extracting domain from HTTPS URL."""
        domain = inferrer.extract_domain_from_website("https://www.example.com")
        assert domain == "example.com"

    @pytest.mark.unit
    def test_extract_domain_http(self, inferrer):
        """Test extracting domain from HTTP URL."""
        domain = inferrer.extract_domain_from_website("http://example.com/about")
        assert domain == "example.com"

    @pytest.mark.unit
    def test_extract_domain_with_path(self, inferrer):
        """Test extracting domain from URL with path."""
        domain = inferrer.extract_domain_from_website("https://example.com/about/team")
        assert domain == "example.com"

    @pytest.mark.unit
    def test_extract_domain_with_port(self, inferrer):
        """Test extracting domain from URL with port."""
        domain = inferrer.extract_domain_from_website("https://example.com:8080")
        assert domain == "example.com"

    @pytest.mark.unit
    def test_extract_domain_empty(self, inferrer):
        """Test handling empty website."""
        domain = inferrer.extract_domain_from_website("")
        assert domain is None


class TestCompanyEmailPatternLearner:
    """Tests for CompanyEmailPatternLearner."""

    @pytest.fixture
    def learner(self):
        return CompanyEmailPatternLearner()

    @pytest.mark.unit
    def test_learn_from_known_emails(self, learner):
        """Test learning pattern from multiple emails."""
        emails = [
            ("john.smith@example.com", "John", "Smith"),
            ("jane.doe@example.com", "Jane", "Doe"),
            ("bob.johnson@example.com", "Bob", "Johnson"),
        ]

        pattern = learner.learn_from_known_emails(emails)

        assert pattern == EmailPattern.FIRST_LAST

    @pytest.mark.unit
    def test_learn_from_mixed_patterns(self, learner):
        """Test learning most common pattern from mixed emails."""
        emails = [
            ("john.smith@example.com", "John", "Smith"),
            ("jdoe@example.com", "Jane", "Doe"),
            ("bob.johnson@example.com", "Bob", "Johnson"),
            ("alice.williams@example.com", "Alice", "Williams"),
        ]

        pattern = learner.learn_from_known_emails(emails)

        # Should pick most common (first.last appears 3 times)
        assert pattern == EmailPattern.FIRST_LAST

    @pytest.mark.unit
    def test_learn_from_empty_list(self, learner):
        """Test handling empty email list."""
        pattern = learner.learn_from_known_emails([])
        assert pattern is None

    @pytest.mark.unit
    def test_infer_company_emails(self, learner):
        """Test inferring emails for multiple people."""
        people = [
            ("John", "Smith"),
            ("Jane", "Doe"),
        ]

        results = learner.infer_company_emails(
            company_domain="example.com",
            people=people,
        )

        assert len(results) == 2
        for result in results:
            assert "first_name" in result
            assert "last_name" in result
            assert "candidates" in result
            assert len(result["candidates"]) >= 1

    @pytest.mark.unit
    def test_infer_company_emails_with_known_pattern(self, learner):
        """Test inferring emails with learned pattern."""
        known_emails = [
            ("john.smith@example.com", "John", "Smith"),
        ]
        people = [
            ("Jane", "Doe"),
            ("Bob", "Johnson"),
        ]

        results = learner.infer_company_emails(
            company_domain="example.com",
            people=people,
            known_emails=known_emails,
        )

        assert len(results) == 2
        # First candidate should use learned pattern
        for result in results:
            first_candidate = result["candidates"][0]
            assert first_candidate["pattern"] == "first.last"
