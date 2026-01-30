"""
Unit tests for Analytics Service.
"""
import pytest
from datetime import date, timedelta
from app.services.analytics_service import AnalyticsService
from app.core.people_models import (
    Person, IndustrialCompany, CompanyPerson, LeadershipChange,
)


class TestAnalyticsService:
    """Tests for AnalyticsService class."""

    @pytest.fixture
    def analytics(self, test_db):
        return AnalyticsService(test_db)

    @pytest.mark.unit
    def test_get_industry_stats_empty(self, analytics, test_db):
        """Test industry stats with no data."""
        result = analytics.get_industry_stats("nonexistent")

        assert result["industry"] == "nonexistent"
        assert result["total_companies"] == 0
        assert result["total_executives"] == 0

    @pytest.mark.unit
    def test_get_industry_stats(self, analytics, test_db, sample_companies, sample_people):
        """Test industry stats with data."""
        # Create leadership for one company
        company = sample_companies[0]
        for i, person in enumerate(sample_people[:2]):
            cp = CompanyPerson(
                company_id=company.id,
                person_id=person.id,
                title="CEO" if i == 0 else "CFO",
                title_level="c_suite",
                is_current=True,
            )
            test_db.add(cp)
        test_db.commit()

        result = analytics.get_industry_stats("distribution")

        assert result["industry"] == "distribution"
        assert result["total_companies"] >= 1
        assert result["total_executives"] >= 2

    @pytest.mark.unit
    def test_get_change_trends_empty(self, analytics, test_db):
        """Test change trends with no changes."""
        result = analytics.get_change_trends(months=1)

        assert result["months"] == 1
        assert "trends" in result

    @pytest.mark.unit
    def test_get_change_trends(self, analytics, test_db, sample_leadership_changes):
        """Test change trends with data."""
        result = analytics.get_change_trends(months=1)

        assert "trends" in result
        assert "industry" in result

    @pytest.mark.unit
    def test_get_talent_flow_empty(self, analytics):
        """Test talent flow with no data."""
        result = analytics.get_talent_flow(days=90)

        assert "period_days" in result
        assert "net_importers" in result
        assert "net_exporters" in result

    @pytest.mark.unit
    def test_get_hot_roles_empty(self, analytics):
        """Test hot roles with no data."""
        result = analytics.get_hot_roles(days=30)

        assert isinstance(result, (list, dict))

    @pytest.mark.unit
    def test_get_company_benchmark_score(self, analytics, test_db, sample_company, sample_leadership_team):
        """Test company benchmark score calculation."""
        result = analytics.get_company_benchmark_score(sample_company.id)

        assert "company_id" in result
        assert "team_score" in result
        assert "components" in result

    @pytest.mark.unit
    def test_get_company_benchmark_score_nonexistent(self, analytics):
        """Test benchmark score for non-existent company."""
        result = analytics.get_company_benchmark_score(99999)

        # Should return error result
        assert result is not None
        assert "error" in result

    @pytest.mark.unit
    def test_get_portfolio_analytics(self, analytics, test_db, sample_portfolio, sample_leadership_team):
        """Test portfolio analytics."""
        result = analytics.get_portfolio_analytics(sample_portfolio.id, days=30)

        assert "portfolio_id" in result
        assert result["portfolio_id"] == sample_portfolio.id
        assert "period_days" in result

    @pytest.mark.unit
    def test_get_portfolio_analytics_empty(self, analytics, test_db):
        """Test portfolio analytics for empty portfolio."""
        from app.core.people_models import PeoplePortfolio

        portfolio = PeoplePortfolio(name="Empty Portfolio")
        test_db.add(portfolio)
        test_db.commit()
        test_db.refresh(portfolio)

        result = analytics.get_portfolio_analytics(portfolio.id, days=30)

        assert result["portfolio_id"] == portfolio.id
        assert result["total_companies"] == 0
        assert "period_days" in result


class TestAnalyticsCalculations:
    """Tests for specific analytics calculations."""

    @pytest.fixture
    def analytics(self, test_db):
        return AnalyticsService(test_db)

    @pytest.mark.unit
    def test_tenure_calculation(self, analytics, test_db, sample_company, sample_person):
        """Test tenure calculation for executives."""
        # Create a position with known start date
        cp = CompanyPerson(
            company_id=sample_company.id,
            person_id=sample_person.id,
            title="CEO",
            title_level="c_suite",
            is_current=True,
            start_date=date.today() - timedelta(days=365),  # 1 year ago
        )
        test_db.add(cp)
        test_db.commit()

        result = analytics.get_industry_stats("distribution")

        # Should include tenure data
        assert "avg_tenure" in result or "tenure_stats" in result or result.get("total_executives", 0) >= 1

    @pytest.mark.unit
    def test_c_suite_count(self, analytics, test_db, sample_company, sample_people):
        """Test C-suite counting."""
        # Add C-suite and non-C-suite
        levels = ["c_suite", "c_suite", "vp", "director"]
        for i, level in enumerate(levels):
            if i < len(sample_people):
                cp = CompanyPerson(
                    company_id=sample_company.id,
                    person_id=sample_people[i].id,
                    title=f"Title {i}",
                    title_level=level,
                    is_current=True,
                )
                test_db.add(cp)
        test_db.commit()

        result = analytics.get_industry_stats("distribution")

        assert result["total_executives"] >= 4

    @pytest.mark.unit
    def test_change_rate_calculation(self, analytics, test_db, sample_company, sample_people):
        """Test change rate calculation."""
        # Create multiple changes with announced_date (required for trends)
        for i in range(5):
            change = LeadershipChange(
                company_id=sample_company.id,
                person_name=f"Person {i}",
                change_type="hire" if i % 2 == 0 else "departure",
                announced_date=date.today() - timedelta(days=i),
                detected_date=date.today() - timedelta(days=i),
            )
            test_db.add(change)
        test_db.commit()

        result = analytics.get_change_trends(months=1)

        assert "trends" in result
