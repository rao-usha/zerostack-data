"""
Unit tests for MedSpa Ownership Classifier.

Tests phone clustering, name clustering, PE matching, pattern detection,
priority ordering, and scoring penalties.
"""

import pytest
from unittest.mock import MagicMock, patch
from app.sources.medspa_discovery.ownership_classifier import (
    MedSpaOwnershipClassifier,
    Classification,
    OWNERSHIP_PENALTIES,
    STORE_NUMBER_PATTERN,
    BRAND_CITY_PATTERN,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_prospect(yelp_id: str, name: str, phone: str = None) -> dict:
    return {"yelp_id": yelp_id, "name": name, "phone": phone}


# ---------------------------------------------------------------------------
# Phone normalization
# ---------------------------------------------------------------------------

class TestPhoneNormalization:
    def test_strips_formatting(self):
        assert MedSpaOwnershipClassifier._normalize_phone("+1 (310) 555-1234") == "3105551234"

    def test_strips_leading_country_code(self):
        assert MedSpaOwnershipClassifier._normalize_phone("13105551234") == "3105551234"

    def test_returns_none_for_short(self):
        assert MedSpaOwnershipClassifier._normalize_phone("555") is None

    def test_returns_none_for_empty(self):
        assert MedSpaOwnershipClassifier._normalize_phone("") is None
        assert MedSpaOwnershipClassifier._normalize_phone(None) is None


# ---------------------------------------------------------------------------
# Brand core extraction
# ---------------------------------------------------------------------------

class TestBrandCoreExtraction:
    def test_common_prefix(self):
        names = ["Next Health - West Hollywood", "Next Health - Beverly Hills", "Next Health - Austin"]
        result = MedSpaOwnershipClassifier._extract_brand_core(names)
        assert result == "Next Health"

    def test_single_name(self):
        result = MedSpaOwnershipClassifier._extract_brand_core(["My Spa"])
        assert result == "My Spa"

    def test_no_common_prefix_uses_shortest(self):
        names = ["Alpha Spa Downtown", "Beta Wellness Center"]
        result = MedSpaOwnershipClassifier._extract_brand_core(names)
        # No common prefix, uses shortest after stripping suffixes
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Blocking key
# ---------------------------------------------------------------------------

class TestBlockingKey:
    def test_skips_stop_words(self):
        # "The Med Spa of ..." — should skip "the", "med", "spa", "of"
        key = MedSpaOwnershipClassifier._blocking_key("The Med Spa of Glendale")
        assert key == "glendale"

    def test_first_significant_word(self):
        key = MedSpaOwnershipClassifier._blocking_key("Next Health West Hollywood")
        assert key == "next"

    def test_returns_none_for_all_stop_words(self):
        key = MedSpaOwnershipClassifier._blocking_key("The Spa")
        assert key is None


# ---------------------------------------------------------------------------
# Location suffix stripping
# ---------------------------------------------------------------------------

class TestLocationSuffix:
    def test_strips_city(self):
        result = MedSpaOwnershipClassifier._strip_location_suffix("Next Health - West Hollywood")
        assert result == "Next Health"

    def test_no_suffix(self):
        result = MedSpaOwnershipClassifier._strip_location_suffix("Glow Skin Bar")
        assert result == "Glow Skin Bar"


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

class TestPatterns:
    def test_store_number_hash(self):
        assert STORE_NUMBER_PATTERN.search("LaserAway #42")

    def test_store_number_no(self):
        assert STORE_NUMBER_PATTERN.search("Brand No. 5")

    def test_brand_city_pattern(self):
        m = BRAND_CITY_PATTERN.match("Ideal Image - Orlando")
        assert m
        assert m.group(1).strip() == "Ideal Image"

    def test_brand_city_pipe(self):
        m = BRAND_CITY_PATTERN.match("SkinSpirit | Palo Alto")
        assert m
        assert m.group(1).strip() == "SkinSpirit"


# ---------------------------------------------------------------------------
# Phone clustering
# ---------------------------------------------------------------------------

class TestPhoneClustering:
    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_two_same_phone_become_multi_site(self):
        prospects = [
            _make_prospect("a", "Brand A", "+1-310-555-1234"),
            _make_prospect("b", "Brand B", "3105551234"),
            _make_prospect("c", "Solo Spa", "+1-212-555-9999"),
        ]
        out = {}
        remaining = self.classifier._cluster_by_phone(prospects, out)

        assert "a" in out
        assert "b" in out
        assert out["a"].ownership_type == "Multi-Site"
        assert out["b"].ownership_type == "Multi-Site"
        assert out["a"].confidence == 0.90
        assert len(remaining) == 1
        assert remaining[0]["yelp_id"] == "c"

    def test_no_phone_passes_through(self):
        prospects = [
            _make_prospect("a", "No Phone Spa", None),
        ]
        out = {}
        remaining = self.classifier._cluster_by_phone(prospects, out)
        assert len(remaining) == 1
        assert len(out) == 0


# ---------------------------------------------------------------------------
# Name clustering
# ---------------------------------------------------------------------------

class TestNameClustering:
    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_similar_names_cluster(self):
        prospects = [
            _make_prospect("a", "Rejuvenate Medical Spa Austin"),
            _make_prospect("b", "Rejuvenate Medical Spa Dallas"),
            _make_prospect("c", "Completely Different Name"),
        ]
        out = {}
        remaining = self.classifier._cluster_by_name(prospects, out)

        # "a" and "b" should cluster as Multi-Site
        assert "a" in out or "b" in out
        if "a" in out:
            assert out["a"].ownership_type == "Multi-Site"
        # "c" should not be classified
        assert "c" not in out

    def test_dissimilar_names_no_cluster(self):
        prospects = [
            _make_prospect("a", "Alpha Wellness Center"),
            _make_prospect("b", "Zeta Beauty Lounge"),
        ]
        out = {}
        remaining = self.classifier._cluster_by_name(prospects, out)
        assert len(out) == 0
        assert len(remaining) == 2


# ---------------------------------------------------------------------------
# Pattern heuristics
# ---------------------------------------------------------------------------

class TestPatternHeuristics:
    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_store_number_classified(self):
        prospects = [_make_prospect("a", "LaserAway #42")]
        out = {}
        remaining = self.classifier._classify_by_pattern(prospects, out)
        assert "a" in out
        assert out["a"].ownership_type == "Multi-Site"
        assert out["a"].parent_entity == "LaserAway"

    def test_brand_city_classified(self):
        prospects = [_make_prospect("a", "Ideal Image - Orlando")]
        out = {}
        remaining = self.classifier._classify_by_pattern(prospects, out)
        assert "a" in out
        assert out["a"].ownership_type == "Multi-Site"
        assert out["a"].parent_entity == "Ideal Image"

    def test_franchise_keyword(self):
        prospects = [_make_prospect("a", "GlowUp Franchise Location")]
        out = {}
        remaining = self.classifier._classify_by_pattern(prospects, out)
        assert "a" in out
        assert out["a"].ownership_type == "Multi-Site"

    def test_normal_name_not_matched(self):
        prospects = [_make_prospect("a", "Glow Skin Bar")]
        out = {}
        remaining = self.classifier._classify_by_pattern(prospects, out)
        assert len(out) == 0
        assert len(remaining) == 1


# ---------------------------------------------------------------------------
# PE cross-reference
# ---------------------------------------------------------------------------

class TestPECrossRef:
    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_pe_match(self):
        prospects = [_make_prospect("a", "SkinSpirit - Palo Alto")]
        pe_companies = [{"name": "SkinSpirit", "ticker": None, "pe_firm": "Ares Management"}]
        out = {}
        remaining = self.classifier._classify_pe_backed(prospects, pe_companies, out)
        assert "a" in out
        assert out["a"].ownership_type == "PE-Backed"
        assert out["a"].parent_entity == "Ares Management"

    def test_no_pe_match(self):
        prospects = [_make_prospect("a", "Mom's Day Spa")]
        pe_companies = [{"name": "SkinSpirit", "ticker": None, "pe_firm": "Ares"}]
        out = {}
        remaining = self.classifier._classify_pe_backed(prospects, pe_companies, out)
        assert len(out) == 0
        assert len(remaining) == 1


# ---------------------------------------------------------------------------
# Public cross-reference
# ---------------------------------------------------------------------------

class TestPublicCrossRef:
    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_public_match(self):
        prospects = [_make_prospect("a", "Massage Envy Tucson")]
        pe_companies = [{"name": "Massage Envy", "ticker": "MENVY", "pe_firm": None}]
        out = {}
        remaining = self.classifier._classify_public(prospects, pe_companies, out)
        assert "a" in out
        assert out["a"].ownership_type == "Public"
        assert out["a"].parent_entity == "MENVY"


# ---------------------------------------------------------------------------
# Scoring penalties
# ---------------------------------------------------------------------------

class TestScoringPenalties:
    def test_independent_no_penalty(self):
        assert OWNERSHIP_PENALTIES["Independent"] == 0

    def test_multi_site_penalty(self):
        assert OWNERSHIP_PENALTIES["Multi-Site"] == -5

    def test_pe_backed_penalty(self):
        assert OWNERSHIP_PENALTIES["PE-Backed"] == -15

    def test_public_penalty(self):
        assert OWNERSHIP_PENALTIES["Public"] == -20


# ---------------------------------------------------------------------------
# Priority ordering (first match wins)
# ---------------------------------------------------------------------------

class TestPriorityOrdering:
    """Verify that earlier stages take precedence over later ones."""

    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_pe_beats_phone_cluster(self):
        """If a prospect matches PE and also shares a phone, PE wins."""
        prospects = [
            _make_prospect("a", "SkinSpirit - SF", "+1-310-555-1234"),
            _make_prospect("b", "SkinSpirit - LA", "+1-310-555-1234"),
        ]
        pe_companies = [{"name": "SkinSpirit", "ticker": None, "pe_firm": "Ares"}]

        out = {}
        # Stage 1: PE
        remaining = self.classifier._classify_pe_backed(prospects, pe_companies, out)
        # Both should be classified as PE-Backed already
        assert "a" in out
        assert "b" in out
        assert out["a"].ownership_type == "PE-Backed"

        # Stage 3: Phone clustering on remaining (should be empty)
        remaining = self.classifier._cluster_by_phone(remaining, out)
        # PE classification should not be overwritten
        assert out["a"].ownership_type == "PE-Backed"


# ---------------------------------------------------------------------------
# Summary building
# ---------------------------------------------------------------------------

class TestSummary:
    def setup_method(self):
        self.db = MagicMock()
        self.classifier = MedSpaOwnershipClassifier(self.db)

    def test_build_summary(self):
        classifications = {
            "a": Classification("a", "Independent", confidence=0.50, stage="default"),
            "b": Classification("b", "Multi-Site", confidence=0.90, stage="phone_cluster"),
            "c": Classification("c", "PE-Backed", confidence=0.85, stage="pe_crossref"),
        }
        summary = self.classifier._build_summary(classifications, 100)
        assert summary["total"] == 3
        assert summary["classified"] == 3
        assert summary["by_type"]["Independent"] == 1
        assert summary["by_type"]["Multi-Site"] == 1
        assert summary["by_type"]["PE-Backed"] == 1
        assert summary["duration_ms"] == 100
