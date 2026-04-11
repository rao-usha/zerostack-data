"""
Tests for SPEC 043 — Data Provenance System

Validates: IngestionJob.data_origin column, provenance helpers,
source registry origin field, and scorer provenance output.
"""
import pytest

from app.core.models import IngestionJob, JobStatus
from app.core.source_registry import SOURCE_REGISTRY, get_all_sources
from app.services.provenance import (
    build_scorer_provenance,
    get_origin_for_source,
    get_provenance_for_factors,
)


class TestIngestionJobDataOrigin:
    """Tests for data_origin column on IngestionJob."""

    def test_ingestion_job_data_origin_default(self):
        """T1: New IngestionJob without explicit data_origin gets server default 'real'.

        SQLAlchemy Column defaults apply at INSERT time, not __init__.
        In-memory, the attribute is None until flushed. We verify the
        Column's server_default is configured correctly.
        """
        col = IngestionJob.__table__.c.data_origin
        assert col.server_default is not None
        assert col.server_default.arg == "real"
        # Explicit construction also works
        job = IngestionJob(
            source="fred",
            status=JobStatus.PENDING,
            config={"test": True},
            data_origin="real",
        )
        assert job.data_origin == "real"

    def test_ingestion_job_synthetic_origin(self):
        """T2: Can create job with data_origin='synthetic'."""
        job = IngestionJob(
            source="synthetic_job_postings",
            status=JobStatus.PENDING,
            config={"n_companies": 50},
            data_origin="synthetic",
        )
        assert job.data_origin == "synthetic"

    def test_data_origin_validation(self):
        """T3: data_origin accepts both valid values."""
        for origin in ("real", "synthetic"):
            job = IngestionJob(
                source="test",
                status=JobStatus.PENDING,
                config={},
                data_origin=origin,
            )
            assert job.data_origin == origin


class TestProvenanceHelper:
    """Tests for provenance lookup and summary functions."""

    def test_provenance_helper_real_only(self):
        """T4: Returns 'real' for a known real source."""
        assert get_origin_for_source("fred") == "real"
        assert get_origin_for_source("bls") == "real"
        assert get_origin_for_source("sec") == "real"

    def test_provenance_helper_synthetic_only(self):
        """T5: Returns 'synthetic' for synthetic sources."""
        assert get_origin_for_source("synthetic_macro_scenarios") == "synthetic"
        assert get_origin_for_source("synthetic_private_financials") == "synthetic"
        assert get_origin_for_source("synthetic_job_postings") == "synthetic"
        assert get_origin_for_source("synthetic_lp_gp") == "synthetic"

    def test_provenance_helper_mixed(self):
        """T6: build_scorer_provenance correctly counts mixed origins."""
        result = build_scorer_provenance({
            "factor_a": "real",
            "factor_b": "synthetic",
            "factor_c": "real",
        })
        assert result["real_factors"] == 2
        assert result["synthetic_factors"] == 1
        assert result["total_factors"] == 3
        assert result["real_pct"] == 67
        assert result["detail"]["factor_a"] == "real"
        assert result["detail"]["factor_b"] == "synthetic"

    def test_provenance_helper_no_data(self):
        """T7: Returns 'unknown' when source is not registered."""
        assert get_origin_for_source("nonexistent_source") == "unknown"


class TestSourceRegistryOrigin:
    """Tests for origin field in source registry."""

    def test_source_registry_origin_field(self):
        """T8: All sources have origin field, synthetics marked correctly."""
        all_sources = get_all_sources()
        assert len(all_sources) > 0

        synthetic_keys = {
            "synthetic_macro_scenarios",
            "synthetic_private_financials",
            "synthetic_job_postings",
            "synthetic_lp_gp",
        }

        for src in all_sources:
            assert hasattr(src, "origin"), f"{src.key} missing origin field"
            assert src.origin in ("real", "synthetic"), (
                f"{src.key} has invalid origin: {src.origin}"
            )
            if src.key in synthetic_keys:
                assert src.origin == "synthetic", (
                    f"{src.key} should be synthetic but is {src.origin}"
                )
            else:
                assert src.origin == "real", (
                    f"{src.key} should be real but is {src.origin}"
                )


class TestScorerProvenance:
    """Tests for provenance reporting in scorer responses."""

    def test_scorer_provenance_in_response(self):
        """T9: get_provenance_for_factors builds correct response."""
        result = get_provenance_for_factors([
            "environmental_risk",
            "safety_risk",
            "growth_momentum",
        ])
        assert result["total_factors"] == 3
        assert result["detail"]["environmental_risk"] == "real"
        assert result["detail"]["safety_risk"] == "real"
        # growth_momentum maps to job_postings which is a real source
        # (synthetic_job_postings is separate)
        assert result["detail"]["growth_momentum"] == "real"
        assert "real_pct" in result

    def test_scorer_provenance_all_synthetic(self):
        """Provenance with all-synthetic factors."""
        result = build_scorer_provenance({
            "lp_breadth": "synthetic",
            "tier1_concentration": "synthetic",
            "reup_rate": "synthetic",
        })
        assert result["real_factors"] == 0
        assert result["synthetic_factors"] == 3
        assert result["real_pct"] == 0

    def test_scorer_provenance_empty(self):
        """Provenance with no factors."""
        result = build_scorer_provenance({})
        assert result["total_factors"] == 0
        assert result["real_pct"] == 0
