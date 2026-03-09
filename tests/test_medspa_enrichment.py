"""
Unit tests for MedSpa Enrichment Pipeline.

Tests the enrichment logic (NPPES matching, revenue estimation, density
classification) without requiring a database connection.
"""

import math
import pytest

from app.sources.medspa_discovery.enrichment import (
    MedSpaEnrichmentPipeline,
    _similarity_ratio,
    _tokenize_address,
)
from app.sources.medspa_discovery.metadata import (
    NATIONAL_MEDIAN_AGI,
    REVENUE_BENCHMARKS,
    SATURATION_THRESHOLDS,
)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestSimilarityRatio:
    def test_identical_strings(self):
        assert _similarity_ratio("Glow Med Spa", "Glow Med Spa") == 1.0

    def test_case_insensitive(self):
        assert _similarity_ratio("GLOW MED SPA", "glow med spa") == 1.0

    def test_similar_names(self):
        ratio = _similarity_ratio("Glow Medical Spa", "Glow Med Spa")
        assert ratio > 0.7

    def test_different_names(self):
        ratio = _similarity_ratio("Glow Med Spa", "Elite Fitness Center")
        assert ratio < 0.4


class TestTokenizeAddress:
    def test_basic_address(self):
        tokens = _tokenize_address("123 Main Street")
        assert "main" in tokens
        assert "street" in tokens

    def test_strips_noise_words(self):
        tokens = _tokenize_address("100 Oak Ave Suite 200")
        assert "suite" not in tokens
        assert "oak" in tokens
        assert "ave" in tokens

    def test_empty_input(self):
        assert _tokenize_address("") == set()
        assert _tokenize_address(None) == set()


# ---------------------------------------------------------------------------
# NPPES matching tests
# ---------------------------------------------------------------------------

class TestNPPESMatch:
    """Test the _nppes_match method directly (no DB needed)."""

    @pytest.fixture
    def pipeline(self):
        """Create pipeline with None db (we only test pure methods)."""
        # We can't pass None directly since __init__ stores it, but
        # _nppes_match doesn't use self.db
        p = MedSpaEnrichmentPipeline.__new__(MedSpaEnrichmentPipeline)
        p.db = None
        return p

    def test_tier1_name_match(self, pipeline):
        prospect = {
            "yelp_id": "test-1",
            "name": "Glow Medical Spa",
            "address": "100 Main St",
            "zip_code": "90210",
        }
        providers = [
            {
                "npi": "1234567890",
                "legal_name": "Glow Medical Spa LLC",
                "first_name": "Jane",
                "last_name": "Smith",
                "credential": "MD",
                "dba_name": "Glow Medical Spa",
                "address": "200 Oak Ave",
                "zip": "90210",
                "taxonomy_code": "207N00000X",
                "taxonomy_desc": "Dermatology",
            }
        ]
        result = pipeline._nppes_match(prospect, providers)
        assert result["_tier"] == "tier1_name"
        assert result["has_physician_oversight"] is True
        assert result["medical_director_name"] == "Jane Smith, MD"
        assert result["nppes_match_confidence"] >= 0.85
        assert "MD" in result["nppes_provider_credentials"]

    def test_tier2_address_match(self, pipeline):
        prospect = {
            "yelp_id": "test-2",
            "name": "Totally Different Name",
            "address": "100 Main Street, Beverly Hills",
            "zip_code": "90210",
        }
        providers = [
            {
                "npi": "1234567890",
                "legal_name": "Dr. Jones Dermatology",
                "first_name": "Bob",
                "last_name": "Jones",
                "credential": "DO",
                "dba_name": None,
                "address": "100 Main Street",
                "zip": "90210",
                "taxonomy_code": "207N00000X",
                "taxonomy_desc": "Dermatology",
            }
        ]
        result = pipeline._nppes_match(prospect, providers)
        assert result["_tier"] == "tier2_address"
        assert result["has_physician_oversight"] is True
        assert result["nppes_match_confidence"] >= 0.70
        assert result["nppes_match_confidence"] <= 0.85

    def test_tier3_zip_proximity(self, pipeline):
        prospect = {
            "yelp_id": "test-3",
            "name": "Totally Different Name",
            "address": "999 Other Road",
            "zip_code": "90210",
        }
        providers = [
            {
                "npi": "1234567890",
                "legal_name": "Some Dermatologist",
                "first_name": "Alice",
                "last_name": "Wong",
                "credential": "MD",
                "dba_name": None,
                "address": "500 Different Blvd",
                "zip": "90210",
                "taxonomy_code": "207N00000X",
                "taxonomy_desc": "Dermatology",
            }
        ]
        result = pipeline._nppes_match(prospect, providers)
        assert result["_tier"] == "tier3_zip"
        assert result["has_physician_oversight"] is None  # unknown
        assert result["nppes_provider_count"] == 1
        assert result["nppes_match_confidence"] >= 0.50
        assert result["nppes_match_confidence"] <= 0.65

    def test_no_providers_in_zip(self, pipeline):
        prospect = {
            "yelp_id": "test-4",
            "name": "Remote Spa",
            "address": "1 Rural Rd",
            "zip_code": "99999",
        }
        result = pipeline._nppes_match(prospect, [])
        assert result["_tier"] == "no_match"
        assert result["has_physician_oversight"] is False
        assert result["nppes_provider_count"] == 0

    def test_multiple_providers_credentials_collected(self, pipeline):
        prospect = {
            "yelp_id": "test-5",
            "name": "Glow Aesthetics",
            "address": "100 Main St",
            "zip_code": "90210",
        }
        providers = [
            {
                "npi": "111", "legal_name": "Glow Aesthetics Inc",
                "first_name": "Jane", "last_name": "Smith", "credential": "MD",
                "dba_name": "Glow Aesthetics", "address": "100 Main St",
                "zip": "90210", "taxonomy_code": "207N00000X", "taxonomy_desc": "Dermatology",
            },
            {
                "npi": "222", "legal_name": "NP at Glow",
                "first_name": "Bob", "last_name": "Lee", "credential": "NP",
                "dba_name": None, "address": "100 Main St",
                "zip": "90210", "taxonomy_code": "363L00000X", "taxonomy_desc": "Nurse Practitioner",
            },
        ]
        result = pipeline._nppes_match(prospect, providers)
        assert result["nppes_provider_count"] == 2
        creds = result["nppes_provider_credentials"]
        assert "MD" in creds
        assert "NP" in creds


# ---------------------------------------------------------------------------
# Revenue estimation tests
# ---------------------------------------------------------------------------

class TestRevenueEstimation:
    def test_basic_revenue_calculation(self):
        prospect = {
            "price": "$$",
            "review_count": 50,
            "zip_avg_agi": NATIONAL_MEDIAN_AGI,  # affluence_factor = 1.0
            "competitors": 0,
            "has_physician": True,
        }
        result = MedSpaEnrichmentPipeline._compute_revenue(prospect, median_reviews=50)
        # base=700K, review_factor=1.0, affluence=1.0, competition=1.0, physician=1.15
        expected = 700_000 * 1.0 * 1.0 * 1.0 * 1.15
        assert abs(result["estimated_annual_revenue"] - expected) < 1.0
        assert result["revenue_estimate_low"] == round(expected * 0.65, 2)
        assert result["revenue_estimate_high"] == round(expected * 1.40, 2)

    def test_high_reviews_boost(self):
        low_reviews = {
            "price": "$$", "review_count": 5, "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 0, "has_physician": False,
        }
        high_reviews = {
            "price": "$$", "review_count": 500, "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 0, "has_physician": False,
        }
        low_result = MedSpaEnrichmentPipeline._compute_revenue(low_reviews, median_reviews=50)
        high_result = MedSpaEnrichmentPipeline._compute_revenue(high_reviews, median_reviews=50)
        assert high_result["estimated_annual_revenue"] > low_result["estimated_annual_revenue"]

    def test_affluent_zip_boost(self):
        poor = {
            "price": "$$", "review_count": 50, "zip_avg_agi": 30_000,
            "competitors": 0, "has_physician": False,
        }
        rich = {
            "price": "$$", "review_count": 50, "zip_avg_agi": 100_000,
            "competitors": 0, "has_physician": False,
        }
        poor_result = MedSpaEnrichmentPipeline._compute_revenue(poor, median_reviews=50)
        rich_result = MedSpaEnrichmentPipeline._compute_revenue(rich, median_reviews=50)
        assert rich_result["estimated_annual_revenue"] > poor_result["estimated_annual_revenue"]

    def test_competition_reduces_revenue(self):
        no_comp = {
            "price": "$$", "review_count": 50, "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 0, "has_physician": False,
        }
        high_comp = {
            "price": "$$", "review_count": 50, "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 20, "has_physician": False,
        }
        no_result = MedSpaEnrichmentPipeline._compute_revenue(no_comp, median_reviews=50)
        high_result = MedSpaEnrichmentPipeline._compute_revenue(high_comp, median_reviews=50)
        assert no_result["estimated_annual_revenue"] > high_result["estimated_annual_revenue"]

    def test_physician_oversight_premium(self):
        without = {
            "price": "$$$", "review_count": 50, "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 5, "has_physician": False,
        }
        with_doc = {
            "price": "$$$", "review_count": 50, "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 5, "has_physician": True,
        }
        wo_result = MedSpaEnrichmentPipeline._compute_revenue(without, median_reviews=50)
        wd_result = MedSpaEnrichmentPipeline._compute_revenue(with_doc, median_reviews=50)
        ratio = wd_result["estimated_annual_revenue"] / wo_result["estimated_annual_revenue"]
        assert abs(ratio - 1.15) < 0.01

    def test_review_factor_capped(self):
        extreme = {
            "price": "$$", "review_count": 100_000,
            "zip_avg_agi": NATIONAL_MEDIAN_AGI,
            "competitors": 0, "has_physician": False,
        }
        result = MedSpaEnrichmentPipeline._compute_revenue(extreme, median_reviews=50)
        # review_factor capped at 2.0, so max = 700K * 2.0 * 1.0 * 1.0 * 1.0 = 1.4M
        assert result["estimated_annual_revenue"] <= 700_000 * 2.0 + 1

    def test_price_tier_benchmarks(self):
        for price, expected_base in REVENUE_BENCHMARKS.items():
            prospect = {
                "price": price, "review_count": 50,
                "zip_avg_agi": NATIONAL_MEDIAN_AGI,
                "competitors": 0, "has_physician": False,
            }
            result = MedSpaEnrichmentPipeline._compute_revenue(prospect, median_reviews=50)
            # With all factors at 1.0, revenue should equal base
            assert abs(result["estimated_annual_revenue"] - expected_base) < 1.0

    def test_confidence_levels(self):
        # High: all 4 signals present
        high = {"price": "$$", "review_count": 50, "zip_avg_agi": 50000,
                "competitors": 0, "has_physician": True}
        assert MedSpaEnrichmentPipeline._compute_revenue(high, 50)["revenue_confidence"] == "high"

        # Low: minimal signals
        low = {"price": None, "review_count": 2, "zip_avg_agi": None,
               "competitors": 0, "has_physician": None}
        assert MedSpaEnrichmentPipeline._compute_revenue(low, 50)["revenue_confidence"] == "low"


# ---------------------------------------------------------------------------
# Market saturation tests
# ---------------------------------------------------------------------------

class TestSaturationClassification:
    def test_undersaturated(self):
        assert MedSpaEnrichmentPipeline._classify_saturation(0.5) == "Undersaturated"

    def test_balanced(self):
        assert MedSpaEnrichmentPipeline._classify_saturation(1.5) == "Balanced"

    def test_saturated(self):
        assert MedSpaEnrichmentPipeline._classify_saturation(3.0) == "Saturated"

    def test_oversaturated(self):
        assert MedSpaEnrichmentPipeline._classify_saturation(8.0) == "Oversaturated"

    def test_none_input(self):
        assert MedSpaEnrichmentPipeline._classify_saturation(None) is None

    def test_boundary_values(self):
        assert MedSpaEnrichmentPipeline._classify_saturation(1.0) == "Undersaturated"
        assert MedSpaEnrichmentPipeline._classify_saturation(2.5) == "Balanced"
        assert MedSpaEnrichmentPipeline._classify_saturation(5.0) == "Saturated"


# ---------------------------------------------------------------------------
# Credential collection tests
# ---------------------------------------------------------------------------

class TestCollectCredentials:
    def test_prioritizes_matched_provider(self):
        matched = {"credential": "MD"}
        providers = [
            {"credential": "NP"},
            {"credential": "MD"},
            {"credential": "PA"},
        ]
        creds = MedSpaEnrichmentPipeline._collect_credentials(providers, match_provider=matched)
        assert creds[0] == "MD"  # matched first
        assert "NP" in creds
        assert "PA" in creds

    def test_deduplicates_credentials(self):
        providers = [
            {"credential": "MD"},
            {"credential": "MD"},
            {"credential": "NP"},
        ]
        creds = MedSpaEnrichmentPipeline._collect_credentials(providers)
        assert creds.count("MD") == 1

    def test_empty_credentials(self):
        providers = [{"credential": ""}, {"credential": None}]
        creds = MedSpaEnrichmentPipeline._collect_credentials(providers)
        assert creds == []


class TestFormatProviderName:
    def test_full_name_with_credential(self):
        prov = {"first_name": "Jane", "last_name": "Smith", "credential": "MD", "legal_name": "Jane Smith"}
        assert MedSpaEnrichmentPipeline._format_provider_name(prov) == "Jane Smith, MD"

    def test_name_without_credential(self):
        prov = {"first_name": "Jane", "last_name": "Smith", "credential": "", "legal_name": "Jane Smith"}
        assert MedSpaEnrichmentPipeline._format_provider_name(prov) == "Jane Smith"

    def test_falls_back_to_legal_name(self):
        prov = {"first_name": "", "last_name": "", "credential": "MD", "legal_name": "Smith Medical Group"}
        assert MedSpaEnrichmentPipeline._format_provider_name(prov) == "Smith Medical Group, MD"

    def test_returns_none_when_no_name(self):
        prov = {"first_name": "", "last_name": "", "credential": "", "legal_name": ""}
        assert MedSpaEnrichmentPipeline._format_provider_name(prov) is None


# ---------------------------------------------------------------------------
# Migration DDL tests
# ---------------------------------------------------------------------------

class TestMigrationDDL:
    def test_enrichment_migration_contains_columns(self):
        from app.sources.medspa_discovery.metadata import generate_enrichment_migration_sql
        sql = generate_enrichment_migration_sql()
        assert "has_physician_oversight" in sql
        assert "nppes_provider_count" in sql
        assert "estimated_annual_revenue" in sql
        assert "revenue_estimate_low" in sql
        assert "zip_total_filers" in sql
        assert "medspas_per_10k_filers" in sql
        assert "market_saturation_index" in sql
        assert "website_url" in sql
        assert "review_velocity_30d" in sql

    def test_snapshot_table_ddl(self):
        from app.sources.medspa_discovery.metadata import generate_snapshot_table_sql
        sql = generate_snapshot_table_sql()
        assert "medspa_prospect_snapshots" in sql
        assert "yelp_id" in sql
        assert "snapshot_date" in sql
        assert "UNIQUE(yelp_id, snapshot_date)" in sql
