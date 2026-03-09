"""Unit tests for Multi-Vertical Discovery Engine."""

import pytest
import math
from unittest.mock import MagicMock, patch

from app.sources.vertical_discovery.configs import (
    VERTICAL_REGISTRY,
    VerticalConfig,
    DENTAL,
    VETERINARY,
    HVAC,
    CAR_WASH,
    PHYSICAL_THERAPY,
    PRICE_SCORE_MAP,
    GRADE_THRESHOLDS,
    SATURATION_THRESHOLDS,
)
from app.sources.vertical_discovery.metadata import generate_create_prospects_sql
from app.sources.vertical_discovery.enrichment import (
    VerticalEnrichmentPipeline,
    _similarity_ratio,
    _format_provider_name,
)
from app.sources.vertical_discovery.ownership_classifier import (
    VerticalOwnershipClassifier,
    OWNERSHIP_TYPES,
    OWNERSHIP_PENALTIES,
    _fuzzy_match,
    _cluster_by_name,
    _extract_brand_core,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute.return_value.mappings.return_value.fetchall.return_value = []
    db.execute.return_value.scalar.return_value = 0
    return db


# ---------------------------------------------------------------------------
# VerticalConfig tests
# ---------------------------------------------------------------------------

class TestVerticalConfigs:
    def test_registry_has_5_verticals(self):
        assert len(VERTICAL_REGISTRY) == 5

    def test_all_slugs_present(self):
        expected = {"dental", "veterinary", "hvac", "car_wash", "physical_therapy"}
        assert set(VERTICAL_REGISTRY.keys()) == expected

    def test_weights_sum_to_one(self):
        for slug, config in VERTICAL_REGISTRY.items():
            total = sum(config.prospect_weights.values())
            assert abs(total - 1.0) < 0.01, f"{slug} weights sum to {total}"

    def test_revenue_benchmarks_have_none_key(self):
        for slug, config in VERTICAL_REGISTRY.items():
            assert None in config.revenue_benchmarks, (
                f"{slug} missing None key in revenue_benchmarks"
            )

    def test_healthcare_verticals_have_nppes(self):
        assert DENTAL.has_nppes_enrichment is True
        assert PHYSICAL_THERAPY.has_nppes_enrichment is True
        assert DENTAL.nppes_taxonomy_codes is not None

    def test_non_healthcare_verticals_no_nppes(self):
        assert HVAC.has_nppes_enrichment is False
        assert CAR_WASH.has_nppes_enrichment is False
        assert VETERINARY.has_nppes_enrichment is False
        assert HVAC.nppes_taxonomy_codes is None

    def test_each_vertical_has_search_terms(self):
        for slug, config in VERTICAL_REGISTRY.items():
            assert len(config.search_terms) >= 2, f"{slug} needs >=2 search terms"

    def test_table_names_unique(self):
        tables = [c.table_name for c in VERTICAL_REGISTRY.values()]
        assert len(tables) == len(set(tables))

    def test_config_is_frozen(self):
        with pytest.raises(AttributeError):
            DENTAL.slug = "modified"


# ---------------------------------------------------------------------------
# Metadata DDL tests
# ---------------------------------------------------------------------------

class TestMetadataDDL:
    def test_generates_correct_table_name(self):
        sql = generate_create_prospects_sql(DENTAL)
        assert "dental_prospects" in sql
        assert "UNIQUE(yelp_id)" in sql

    def test_includes_nppes_columns(self):
        sql = generate_create_prospects_sql(DENTAL)
        assert "has_physician_oversight" in sql
        assert "nppes_provider_count" in sql

    def test_includes_revenue_columns(self):
        sql = generate_create_prospects_sql(HVAC)
        assert "estimated_annual_revenue" in sql
        assert "revenue_confidence" in sql

    def test_includes_vertical_default(self):
        sql = generate_create_prospects_sql(CAR_WASH)
        assert "car_wash" in sql

    def test_each_vertical_generates_valid_sql(self):
        for config in VERTICAL_REGISTRY.values():
            sql = generate_create_prospects_sql(config)
            assert "CREATE TABLE IF NOT EXISTS" in sql
            assert config.table_name in sql


# ---------------------------------------------------------------------------
# Enrichment tests
# ---------------------------------------------------------------------------

class TestEnrichment:
    def test_similarity_ratio(self):
        assert _similarity_ratio("hello", "hello") == 1.0
        assert _similarity_ratio("hello", "HELLO") == 1.0
        assert _similarity_ratio("abc", "xyz") < 0.5

    def test_format_provider_name_full(self):
        result = _format_provider_name({
            "provider_first_name": "John",
            "provider_last_name": "Smith",
            "credential": "DDS",
        })
        assert result == "John Smith, DDS"

    def test_format_provider_name_no_cred(self):
        result = _format_provider_name({
            "provider_first_name": "Jane",
            "provider_last_name": "Doe",
        })
        assert result == "Jane Doe"

    def test_format_provider_name_org_only(self):
        result = _format_provider_name({
            "provider_name": "Smith Dental Group",
        })
        assert result == "Smith Dental Group"

    def test_enrich_all_skips_nppes_for_non_healthcare(self, mock_db):
        pipeline = VerticalEnrichmentPipeline(mock_db, HVAC)
        result = pipeline.enrich_all()
        assert result["nppes"]["skipped"] is True

    def test_enrich_nppes_skips_non_healthcare(self, mock_db):
        pipeline = VerticalEnrichmentPipeline(mock_db, CAR_WASH)
        result = pipeline.enrich_nppes()
        assert result["skipped"] is True

    def test_enrich_density_no_prospects(self, mock_db):
        pipeline = VerticalEnrichmentPipeline(mock_db, DENTAL)
        result = pipeline.enrich_competitive_density()
        assert result["enriched"] == 0

    def test_estimate_revenue_no_prospects(self, mock_db):
        pipeline = VerticalEnrichmentPipeline(mock_db, DENTAL)
        result = pipeline.estimate_revenue()
        assert result["estimated"] == 0


# ---------------------------------------------------------------------------
# Ownership classifier tests
# ---------------------------------------------------------------------------

class TestOwnershipClassifier:
    def test_ownership_types(self):
        assert "Independent" in OWNERSHIP_TYPES
        assert "PE-Backed" in OWNERSHIP_TYPES
        assert "Public" in OWNERSHIP_TYPES
        assert "Multi-Site" in OWNERSHIP_TYPES

    def test_penalties_all_types_covered(self):
        for t in OWNERSHIP_TYPES:
            assert t in OWNERSHIP_PENALTIES

    def test_independent_no_penalty(self):
        assert OWNERSHIP_PENALTIES["Independent"] == 0

    def test_pe_backed_has_penalty(self):
        assert OWNERSHIP_PENALTIES["PE-Backed"] < 0

    def test_fuzzy_match_exact(self):
        assert _fuzzy_match("Aspen Dental", "Aspen Dental") is True

    def test_fuzzy_match_close(self):
        assert _fuzzy_match("Aspen Dental", "Aspen Dental Group") is True

    def test_fuzzy_match_different(self):
        assert _fuzzy_match("Aspen Dental", "Pacific Northwest HVAC") is False

    def test_cluster_by_name(self):
        prospects = [
            {"yelp_id": "1", "name": "Bright Smile Dental - Austin"},
            {"yelp_id": "2", "name": "Bright Smile Dental - Dallas"},
            {"yelp_id": "3", "name": "Lone Star Family Dentistry"},
        ]
        clusters = _cluster_by_name(prospects, threshold=0.75)
        # First two should cluster together
        big_cluster = [c for c in clusters if len(c) >= 2]
        assert len(big_cluster) == 1
        assert len(big_cluster[0]) == 2

    def test_extract_brand_core_common_prefix(self):
        names = ["Bright Smile Dental - Austin", "Bright Smile Dental - Dallas"]
        result = _extract_brand_core(names)
        assert "Bright" in result

    def test_extract_brand_core_single(self):
        result = _extract_brand_core(["Solo Dental"])
        assert result == "Solo Dental"

    def test_extract_brand_core_empty(self):
        assert _extract_brand_core([]) == ""

    def test_classify_all_no_prospects(self, mock_db):
        classifier = VerticalOwnershipClassifier(mock_db, DENTAL)
        result = classifier.classify_all()
        assert result["classified"] == 0


# ---------------------------------------------------------------------------
# Collector tests (static methods)
# ---------------------------------------------------------------------------

class TestCollectorHelpers:
    @patch("app.sources.vertical_discovery.collector.VerticalDiscoveryCollector._ensure_tables")
    def test_percentile_rank(self, _):
        from app.sources.vertical_discovery.collector import VerticalDiscoveryCollector
        result = VerticalDiscoveryCollector._percentile_rank([10, 30, 20])
        assert result[0] == 0.0   # smallest
        assert result[1] == 100.0  # largest
        assert result[2] == 50.0   # middle

    @patch("app.sources.vertical_discovery.collector.VerticalDiscoveryCollector._ensure_tables")
    def test_get_grade(self, _):
        from app.sources.vertical_discovery.collector import VerticalDiscoveryCollector
        assert VerticalDiscoveryCollector._get_grade(90) == "A"
        assert VerticalDiscoveryCollector._get_grade(70) == "B"
        assert VerticalDiscoveryCollector._get_grade(55) == "C"
        assert VerticalDiscoveryCollector._get_grade(40) == "D"
        assert VerticalDiscoveryCollector._get_grade(15) == "F"

    def test_price_score_map(self):
        assert PRICE_SCORE_MAP["$$$$"] == 100
        assert PRICE_SCORE_MAP["$"] == 25

    def test_saturation_thresholds_ordered(self):
        thresholds = [t for t, _ in SATURATION_THRESHOLDS]
        assert thresholds == sorted(thresholds)
