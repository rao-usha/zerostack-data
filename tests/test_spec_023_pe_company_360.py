"""
Tests for SPEC 023 — PE Company 360.
Unified company intelligence aggregator.
"""
import pytest


class TestCompany360Service:
    """Tests for Company360 aggregator."""

    def test_360_response_structure(self):
        """T1: All sections present in response dataclass."""
        from app.core.pe_company_360 import Company360Result

        result = Company360Result(company_id=1, company_name="TestCo")
        assert hasattr(result, "profile")
        assert hasattr(result, "benchmarks")
        assert hasattr(result, "exit_readiness")
        assert hasattr(result, "deal_score")
        assert hasattr(result, "comparable_transactions")
        assert hasattr(result, "leadership")
        assert hasattr(result, "competitors")
        assert hasattr(result, "recent_alerts")
        assert hasattr(result, "pipeline_deals")
        assert hasattr(result, "thesis")

    def test_360_handles_missing_sections(self):
        """T2: Missing data → None/empty, not error."""
        from app.core.pe_company_360 import Company360Result

        result = Company360Result(company_id=1, company_name="TestCo")
        assert result.benchmarks is None
        assert result.exit_readiness is None
        assert result.deal_score is None
        assert result.comparable_transactions is None
        assert result.leadership == []
        assert result.competitors == []
        assert result.recent_alerts == []
        assert result.thesis is None

    def test_360_profile_fields(self):
        """T3: Profile section has expected fields."""
        from app.core.pe_company_360 import Company360Result

        result = Company360Result(
            company_id=1, company_name="TestCo",
            profile={"industry": "Healthcare", "status": "Active", "employee_count": 200},
        )
        assert result.profile["industry"] == "Healthcare"

    def test_360_scores_type(self):
        """T4: Score sections are dicts when populated."""
        from app.core.pe_company_360 import Company360Result

        result = Company360Result(
            company_id=1, company_name="TestCo",
            exit_readiness={"composite_score": 72.0, "grade": "B"},
            deal_score={"composite_score": 67.0, "grade": "B"},
        )
        assert result.exit_readiness["composite_score"] == 72.0
        assert result.deal_score["grade"] == "B"
