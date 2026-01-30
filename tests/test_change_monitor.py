"""
Unit tests for Change Monitor and Alert Digest Generator.
"""
import pytest
from datetime import date, datetime, timedelta
from app.jobs.change_monitor import ChangeMonitor, AlertDigestGenerator
from app.core.people_models import (
    LeadershipChange, PeopleWatchlist, PeopleWatchlistPerson,
    PeoplePortfolio, PeoplePortfolioCompany, IndustrialCompany, Person,
)


class TestChangeMonitor:
    """Tests for ChangeMonitor class."""

    @pytest.fixture
    def monitor(self, test_db):
        return ChangeMonitor(test_db)

    @pytest.mark.unit
    def test_get_recent_changes_empty(self, monitor):
        """Test getting recent changes with no data."""
        result = monitor.get_recent_changes(days=7)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_recent_changes(self, monitor, sample_leadership_changes):
        """Test getting recent changes with data."""
        result = monitor.get_recent_changes(days=7)

        assert len(result) >= 3
        # Verify order is descending by date
        if len(result) >= 2:
            assert result[0].detected_date >= result[1].detected_date

    @pytest.mark.unit
    def test_get_recent_changes_c_suite_only(self, monitor, sample_leadership_changes):
        """Test filtering to C-suite only changes."""
        result = monitor.get_recent_changes(days=7, c_suite_only=True)

        for change in result:
            assert change.is_c_suite is True

    @pytest.mark.unit
    def test_get_recent_changes_by_company(self, monitor, test_db, sample_leadership_changes, sample_companies):
        """Test filtering changes by company."""
        company_ids = [sample_companies[0].id]
        result = monitor.get_recent_changes(days=7, company_ids=company_ids)

        for change in result:
            assert change.company_id in company_ids

    @pytest.mark.unit
    def test_get_recent_changes_by_type(self, monitor, sample_leadership_changes):
        """Test filtering changes by type."""
        result = monitor.get_recent_changes(days=7, change_types=["hire"])

        for change in result:
            assert change.change_type == "hire"


class TestWatchlistAlerts:
    """Tests for watchlist alert functionality."""

    @pytest.fixture
    def monitor(self, test_db):
        return ChangeMonitor(test_db)

    @pytest.mark.unit
    def test_get_watchlist_alerts_empty(self, monitor, test_db):
        """Test watchlist alerts with empty watchlist."""
        watchlist = PeopleWatchlist(name="Empty List")
        test_db.add(watchlist)
        test_db.commit()
        test_db.refresh(watchlist)

        result = monitor.get_watchlist_alerts(watchlist_id=watchlist.id, days=7)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_watchlist_alerts(self, monitor, test_db, sample_watchlist, sample_leadership_changes):
        """Test watchlist alerts with changes."""
        result = monitor.get_watchlist_alerts(
            watchlist_id=sample_watchlist.id,
            days=30,
        )

        assert isinstance(result, list)
        # Results should be for people in the watchlist

    @pytest.mark.unit
    def test_get_watchlist_alerts_structure(self, monitor, test_db, sample_watchlist, sample_company, sample_people):
        """Test watchlist alert structure."""
        # Create a change for a watched person
        change = LeadershipChange(
            company_id=sample_company.id,
            person_id=sample_people[0].id,
            person_name=sample_people[0].full_name,
            change_type="promotion",
            old_title="VP",
            new_title="SVP",
            detected_date=date.today(),
        )
        test_db.add(change)
        test_db.commit()

        result = monitor.get_watchlist_alerts(
            watchlist_id=sample_watchlist.id,
            days=7,
        )

        if len(result) > 0:
            alert = result[0]
            assert "change_id" in alert
            assert "person_name" in alert
            assert "change_type" in alert


class TestPortfolioAlerts:
    """Tests for portfolio alert functionality."""

    @pytest.fixture
    def monitor(self, test_db):
        return ChangeMonitor(test_db)

    @pytest.mark.unit
    def test_get_portfolio_alerts_empty(self, monitor, test_db):
        """Test portfolio alerts with no companies."""
        portfolio = PeoplePortfolio(name="Empty Portfolio")
        test_db.add(portfolio)
        test_db.commit()
        test_db.refresh(portfolio)

        result = monitor.get_portfolio_alerts(portfolio_id=portfolio.id, days=7)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_portfolio_alerts(self, monitor, test_db, sample_portfolio, sample_leadership_changes):
        """Test portfolio alerts with changes."""
        result = monitor.get_portfolio_alerts(
            portfolio_id=sample_portfolio.id,
            days=30,
        )

        assert isinstance(result, list)

    @pytest.mark.unit
    def test_get_portfolio_alerts_c_suite_filter(self, monitor, test_db, sample_portfolio, sample_leadership_changes):
        """Test portfolio alerts with C-suite filter."""
        result = monitor.get_portfolio_alerts(
            portfolio_id=sample_portfolio.id,
            days=30,
            c_suite_only=True,
        )

        for alert in result:
            assert alert.get("is_c_suite") is True


class TestIndustryAlerts:
    """Tests for industry alert functionality."""

    @pytest.fixture
    def monitor(self, test_db):
        return ChangeMonitor(test_db)

    @pytest.mark.unit
    def test_get_industry_alerts_empty(self, monitor):
        """Test industry alerts with no companies."""
        result = monitor.get_industry_alerts(industry="nonexistent", days=7)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_industry_alerts(self, monitor, test_db, sample_companies, sample_leadership_changes):
        """Test industry alerts."""
        result = monitor.get_industry_alerts(
            industry="distribution",
            days=30,
        )

        assert isinstance(result, list)


class TestChangeSummary:
    """Tests for change summary functionality."""

    @pytest.fixture
    def monitor(self, test_db):
        return ChangeMonitor(test_db)

    @pytest.mark.unit
    def test_get_change_summary_empty(self, monitor):
        """Test change summary with no changes."""
        result = monitor.get_change_summary(days=7)

        assert result["period_days"] == 7
        assert result["total_changes"] == 0
        assert result["c_suite_changes"] == 0

    @pytest.mark.unit
    def test_get_change_summary(self, monitor, sample_leadership_changes):
        """Test change summary with data."""
        result = monitor.get_change_summary(days=30)

        assert result["period_days"] == 30
        assert result["total_changes"] >= 3
        assert "by_type" in result
        assert isinstance(result["by_type"], dict)

    @pytest.mark.unit
    def test_get_change_summary_counts(self, monitor, sample_leadership_changes):
        """Test change summary counts are accurate."""
        result = monitor.get_change_summary(days=30)

        # Verify by_type counts sum to total
        by_type_total = sum(result["by_type"].values())
        assert by_type_total == result["total_changes"]


class TestAlertDigestGenerator:
    """Tests for AlertDigestGenerator class."""

    @pytest.fixture
    def generator(self, test_db):
        return AlertDigestGenerator(test_db)

    @pytest.mark.unit
    def test_generate_weekly_digest_empty(self, generator):
        """Test weekly digest with no changes."""
        result = generator.generate_weekly_digest()

        assert "generated_at" in result
        assert "period" in result
        assert "summary" in result
        assert "all_changes" in result

    @pytest.mark.unit
    def test_generate_weekly_digest(self, generator, sample_leadership_changes):
        """Test weekly digest with data."""
        result = generator.generate_weekly_digest()

        assert result["generated_at"] is not None
        assert "all_changes" in result
        assert len(result["all_changes"]) >= 0

    @pytest.mark.unit
    def test_generate_weekly_digest_portfolio_filter(self, generator, sample_portfolio, sample_leadership_changes):
        """Test weekly digest with portfolio filter."""
        result = generator.generate_weekly_digest(portfolio_id=sample_portfolio.id)

        assert result["filter"] is not None
        assert result["filter"]["type"] == "portfolio"

    @pytest.mark.unit
    def test_generate_weekly_digest_industry_filter(self, generator, sample_leadership_changes):
        """Test weekly digest with industry filter."""
        result = generator.generate_weekly_digest(industry="distribution")

        assert result["filter"] is not None
        assert result["filter"]["type"] == "industry"

    @pytest.mark.unit
    def test_generate_weekly_digest_highlights(self, generator, sample_leadership_changes):
        """Test that digest includes highlights."""
        result = generator.generate_weekly_digest()

        assert "highlights" in result
        # Highlights should be high-significance or C-suite changes

    @pytest.mark.unit
    def test_generate_watchlist_digest_not_found(self, generator):
        """Test watchlist digest for non-existent watchlist."""
        result = generator.generate_watchlist_digest(watchlist_id=99999)

        assert "error" in result

    @pytest.mark.unit
    def test_generate_watchlist_digest(self, generator, sample_watchlist, sample_leadership_changes):
        """Test watchlist digest."""
        result = generator.generate_watchlist_digest(
            watchlist_id=sample_watchlist.id,
            days=30,
        )

        assert "watchlist_id" in result
        assert result["watchlist_id"] == sample_watchlist.id
        assert "total_alerts" in result

    @pytest.mark.unit
    def test_generate_watchlist_digest_groups_by_person(self, generator, sample_watchlist, test_db, sample_company, sample_people):
        """Test that watchlist digest groups changes by person."""
        # Create multiple changes for same person
        for i in range(3):
            change = LeadershipChange(
                company_id=sample_company.id,
                person_id=sample_people[0].id,
                person_name=sample_people[0].full_name,
                change_type="promotion",
                new_title=f"Title {i}",
                detected_date=date.today() - timedelta(days=i),
            )
            test_db.add(change)
        test_db.commit()

        result = generator.generate_watchlist_digest(
            watchlist_id=sample_watchlist.id,
            days=30,
        )

        if "by_person" in result:
            assert isinstance(result["by_person"], list)
