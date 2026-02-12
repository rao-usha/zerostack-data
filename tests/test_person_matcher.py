"""
Unit tests for person name fuzzy matcher.
"""
import sys
import os
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly from file to avoid __init__.py chain (needs aiohttp)
import importlib.util

# First load fuzzy_matcher (dependency)
_fm_spec = importlib.util.spec_from_file_location(
    "fuzzy_matcher",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "agentic", "fuzzy_matcher.py")
)
_fm_mod = importlib.util.module_from_spec(_fm_spec)
sys.modules["app.agentic.fuzzy_matcher"] = _fm_mod
_fm_spec.loader.exec_module(_fm_mod)

# Now load person_matcher
_pm_spec = importlib.util.spec_from_file_location(
    "person_matcher",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "app", "sources", "people_collection", "person_matcher.py")
)
_pm_mod = importlib.util.module_from_spec(_pm_spec)
_pm_spec.loader.exec_module(_pm_mod)

PersonNameMatcher = _pm_mod.PersonNameMatcher


class TestNameNormalization:
    """Tests for name normalization."""

    @pytest.fixture
    def matcher(self):
        return PersonNameMatcher()

    @pytest.mark.unit
    def test_basic_normalization(self, matcher):
        """Names are lowercased and trimmed."""
        assert matcher.normalize_name("  JOHN SMITH  ") == "john smith"

    @pytest.mark.unit
    def test_strip_jr_suffix(self, matcher):
        """Jr suffix is stripped."""
        assert matcher.normalize_name("John Smith Jr.") == "john smith"
        assert matcher.normalize_name("John Smith Jr") == "john smith"

    @pytest.mark.unit
    def test_strip_sr_suffix(self, matcher):
        """Sr suffix is stripped."""
        assert matcher.normalize_name("John Smith Sr") == "john smith"

    @pytest.mark.unit
    def test_strip_iii_suffix(self, matcher):
        """III suffix is stripped."""
        assert matcher.normalize_name("John Smith III") == "john smith"

    @pytest.mark.unit
    def test_strip_phd_suffix(self, matcher):
        """PhD suffix is stripped."""
        assert matcher.normalize_name("John Smith PhD") == "john smith"

    @pytest.mark.unit
    def test_last_first_format(self, matcher):
        """Handle 'Last, First' format."""
        assert matcher.normalize_name("Smith, John") == "john smith"

    @pytest.mark.unit
    def test_last_first_middle_format(self, matcher):
        """Handle 'Last, First Middle' format."""
        assert matcher.normalize_name("Smith, John Robert") == "john robert smith"

    @pytest.mark.unit
    def test_empty_name(self, matcher):
        """Empty name returns empty string."""
        assert matcher.normalize_name("") == ""
        assert matcher.normalize_name(None) == ""

    @pytest.mark.unit
    def test_whitespace_collapse(self, matcher):
        """Multiple spaces are collapsed."""
        assert matcher.normalize_name("John   Robert   Smith") == "john robert smith"


class TestNameComparison:
    """Tests for name comparison."""

    @pytest.fixture
    def matcher(self):
        return PersonNameMatcher()

    @pytest.mark.unit
    def test_exact_match(self, matcher):
        """Identical names match with 1.0 similarity."""
        result = matcher.compare("John Smith", "John Smith")
        assert result.matched is True
        assert result.similarity == 1.0
        assert result.match_type == "name_exact"

    @pytest.mark.unit
    def test_case_insensitive_match(self, matcher):
        """Case differences still match."""
        result = matcher.compare("JOHN SMITH", "john smith")
        assert result.matched is True
        assert result.similarity == 1.0

    @pytest.mark.unit
    def test_middle_name_dropped(self, matcher):
        """Middle name is dropped — first+last still matches."""
        result = matcher.compare("John Robert Smith", "John Smith")
        assert result.matched is True
        assert result.similarity == 1.0

    @pytest.mark.unit
    def test_nickname_bob_robert(self, matcher):
        """Bob/Robert nickname match."""
        result = matcher.compare("Bob Smith", "Robert Smith")
        assert result.matched is True
        assert result.similarity >= 0.90

    @pytest.mark.unit
    def test_nickname_bill_william(self, matcher):
        """Bill/William nickname match."""
        result = matcher.compare("Bill Jones", "William Jones")
        assert result.matched is True
        assert result.similarity >= 0.90

    @pytest.mark.unit
    def test_nickname_jim_james(self, matcher):
        """Jim/James nickname match."""
        result = matcher.compare("Jim Davis", "James Davis")
        assert result.matched is True
        assert result.similarity >= 0.90

    @pytest.mark.unit
    def test_nickname_mike_michael(self, matcher):
        """Mike/Michael nickname match."""
        result = matcher.compare("Mike Johnson", "Michael Johnson")
        assert result.matched is True

    @pytest.mark.unit
    def test_different_names_no_match(self, matcher):
        """Completely different names don't match."""
        result = matcher.compare("John Smith", "Jane Doe")
        assert result.matched is False
        assert result.match_type == "no_match"

    @pytest.mark.unit
    def test_similar_names_fuzzy_match(self, matcher):
        """Similar but not identical names get fuzzy match."""
        result = matcher.compare("Jon Smith", "John Smith")
        assert result.matched is True
        assert result.similarity >= 0.80

    @pytest.mark.unit
    def test_suffix_stripped_before_compare(self, matcher):
        """Suffixes are stripped so Jr/Sr don't affect matching."""
        result = matcher.compare("John Smith Jr.", "John Smith")
        assert result.matched is True
        assert result.similarity == 1.0

    @pytest.mark.unit
    def test_last_first_format_matches(self, matcher):
        """'Smith, John' matches 'John Smith'."""
        result = matcher.compare("Smith, John", "John Smith")
        assert result.matched is True
        assert result.similarity == 1.0

    @pytest.mark.unit
    def test_empty_names(self, matcher):
        """Empty names don't match."""
        result = matcher.compare("", "John Smith")
        assert result.matched is False

        result = matcher.compare("John Smith", "")
        assert result.matched is False


class TestMatchClassification:
    """Tests for match classification."""

    @pytest.fixture
    def matcher(self):
        return PersonNameMatcher()

    @pytest.mark.unit
    def test_auto_merge_high_sim_shared_company(self, matcher):
        """High similarity + shared company = auto merge."""
        assert matcher.classify_match(0.98, True) == "auto_merge"

    @pytest.mark.unit
    def test_review_high_sim_no_company(self, matcher):
        """High similarity without shared company = review."""
        assert matcher.classify_match(0.98, False) == "review"

    @pytest.mark.unit
    def test_review_medium_sim(self, matcher):
        """Medium similarity = review."""
        assert matcher.classify_match(0.85, True) == "review"
        assert matcher.classify_match(0.85, False) == "review"

    @pytest.mark.unit
    def test_no_match_low_sim(self, matcher):
        """Low similarity = no match."""
        assert matcher.classify_match(0.70, True) == "no_match"
        assert matcher.classify_match(0.70, False) == "no_match"

    @pytest.mark.unit
    def test_threshold_boundary(self, matcher):
        """Test exact threshold boundaries."""
        assert matcher.classify_match(0.95, True) == "auto_merge"
        assert matcher.classify_match(0.80, False) == "review"
        assert matcher.classify_match(0.79, True) == "no_match"

    @pytest.mark.unit
    def test_custom_thresholds(self):
        """Custom thresholds are respected."""
        matcher = PersonNameMatcher(auto_merge_threshold=0.90, review_threshold=0.70)
        assert matcher.classify_match(0.92, True) == "auto_merge"
        assert matcher.classify_match(0.75, False) == "review"
        assert matcher.classify_match(0.65, True) == "no_match"
