"""
Integration tests for People & Org Chart Platform API endpoints.

These tests require the FastAPI app to be running.
"""
import pytest
from fastapi.testclient import TestClient
from datetime import date, datetime

from app.main import app
from app.core.database import get_db
from app.core.people_models import (
    Person, IndustrialCompany, CompanyPerson, LeadershipChange,
    PeoplePortfolio, PeoplePortfolioCompany, PeopleWatchlist, PeopleWatchlistPerson,
    PeoplePeerSet, PeoplePeerSetMember, PeopleCollectionJob,
)


@pytest.fixture
def client(test_db):
    """Create test client with overridden database."""
    def override_get_db():
        try:
            yield test_db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


class TestPeopleEndpoints:
    """Tests for /api/v1/people endpoints."""

    @pytest.mark.integration
    def test_list_people_empty(self, client):
        """Test listing people when database is empty."""
        response = client.get("/api/v1/people/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @pytest.mark.integration
    def test_list_people(self, client, test_db, sample_people):
        """Test listing people with data."""
        response = client.get("/api/v1/people/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) >= len(sample_people)

    @pytest.mark.integration
    def test_get_person(self, client, test_db, sample_person):
        """Test getting a specific person."""
        response = client.get(f"/api/v1/people/{sample_person.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == sample_person.id
        assert data["full_name"] == sample_person.full_name

    @pytest.mark.integration
    def test_get_person_not_found(self, client):
        """Test getting non-existent person."""
        response = client.get("/api/v1/people/99999")

        assert response.status_code == 404

    @pytest.mark.integration
    def test_search_people(self, client, test_db, sample_people):
        """Test searching people by name."""
        response = client.get("/api/v1/people/search", params={"q": "John"})

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestCompaniesLeadershipEndpoints:
    """Tests for /api/v1/companies/{id}/leadership endpoints."""

    @pytest.mark.integration
    def test_get_company_leadership(self, client, test_db, sample_company, sample_leadership_team):
        """Test getting company leadership team."""
        response = client.get(f"/api/v1/companies/{sample_company.id}/leadership")

        assert response.status_code == 200
        data = response.json()
        assert "company_id" in data
        assert "leadership" in data or "people" in data

    @pytest.mark.integration
    def test_get_company_leadership_not_found(self, client):
        """Test getting leadership for non-existent company."""
        response = client.get("/api/v1/companies/99999/leadership")

        assert response.status_code == 404

    @pytest.mark.integration
    def test_get_company_changes(self, client, test_db, sample_company, sample_leadership_changes):
        """Test getting company leadership changes."""
        response = client.get(f"/api/v1/companies/{sample_company.id}/leadership/changes")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, (list, dict))


class TestPortfolioEndpoints:
    """Tests for /api/v1/people-portfolios endpoints."""

    @pytest.mark.integration
    def test_list_portfolios_empty(self, client):
        """Test listing portfolios when empty."""
        response = client.get("/api/v1/people-portfolios/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @pytest.mark.integration
    def test_list_portfolios(self, client, test_db, sample_portfolio):
        """Test listing portfolios with data."""
        response = client.get("/api/v1/people-portfolios/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1

    @pytest.mark.integration
    def test_create_portfolio(self, client):
        """Test creating a new portfolio."""
        response = client.post(
            "/api/v1/people-portfolios/",
            json={
                "name": "Test Portfolio",
                "pe_firm": "Test Capital",
                "description": "A test portfolio",
            },
        )

        assert response.status_code in [200, 201]
        data = response.json()
        assert data["name"] == "Test Portfolio"

    @pytest.mark.integration
    def test_get_portfolio(self, client, test_db, sample_portfolio):
        """Test getting a specific portfolio."""
        response = client.get(f"/api/v1/people-portfolios/{sample_portfolio.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == sample_portfolio.id

    @pytest.mark.integration
    def test_get_portfolio_companies(self, client, test_db, sample_portfolio):
        """Test getting portfolio companies."""
        response = client.get(f"/api/v1/people-portfolios/{sample_portfolio.id}/companies")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestWatchlistEndpoints:
    """Tests for /api/v1/people-watchlists endpoints."""

    @pytest.mark.integration
    def test_list_watchlists(self, client):
        """Test listing watchlists."""
        response = client.get("/api/v1/people-watchlists/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @pytest.mark.integration
    def test_create_watchlist(self, client):
        """Test creating a new watchlist."""
        response = client.post(
            "/api/v1/people-watchlists/",
            json={
                "name": "Key Executives",
                "description": "Important people to track",
            },
        )

        assert response.status_code in [200, 201]
        data = response.json()
        assert data["name"] == "Key Executives"

    @pytest.mark.integration
    def test_get_watchlist(self, client, test_db, sample_watchlist):
        """Test getting a specific watchlist."""
        response = client.get(f"/api/v1/people-watchlists/{sample_watchlist.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == sample_watchlist.id


class TestAnalyticsEndpoints:
    """Tests for /api/v1/people-analytics endpoints."""

    @pytest.mark.integration
    def test_get_industry_stats(self, client, test_db, sample_companies, sample_leadership_team):
        """Test getting industry statistics."""
        response = client.get(
            "/api/v1/people-analytics/industry/distribution"
        )

        assert response.status_code == 200
        data = response.json()
        assert "industry" in data

    @pytest.mark.integration
    def test_get_change_trends(self, client, test_db, sample_leadership_changes):
        """Test getting leadership change trends."""
        response = client.get("/api/v1/people-analytics/trends", params={"days": 30})

        assert response.status_code == 200
        data = response.json()
        assert "total_changes" in data or "period_days" in data

    @pytest.mark.integration
    def test_get_company_benchmark(self, client, test_db, sample_company, sample_leadership_team):
        """Test getting company benchmark score."""
        response = client.get(f"/api/v1/people-analytics/benchmark/{sample_company.id}")

        assert response.status_code == 200
        data = response.json()
        assert "company_id" in data


class TestDataQualityEndpoints:
    """Tests for /api/v1/people-data-quality endpoints."""

    @pytest.mark.integration
    def test_get_overall_stats(self, client, test_db, sample_people):
        """Test getting overall data quality stats."""
        response = client.get("/api/v1/people-data-quality/stats")

        assert response.status_code == 200
        data = response.json()
        assert "total_people" in data

    @pytest.mark.integration
    def test_get_freshness_stats(self, client, test_db, sample_people):
        """Test getting data freshness stats."""
        response = client.get("/api/v1/people-data-quality/freshness")

        assert response.status_code == 200
        data = response.json()
        assert "total_people" in data

    @pytest.mark.integration
    def test_get_enrichment_queue(self, client, test_db, sample_people):
        """Test getting enrichment queue."""
        response = client.get("/api/v1/people-data-quality/enrichment-queue")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestJobsEndpoints:
    """Tests for /api/v1/people-jobs endpoints."""

    @pytest.mark.integration
    def test_list_jobs(self, client):
        """Test listing collection jobs."""
        response = client.get("/api/v1/people-jobs/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @pytest.mark.integration
    def test_get_job_stats(self, client):
        """Test getting job statistics."""
        response = client.get("/api/v1/people-jobs/stats", params={"days": 7})

        assert response.status_code == 200
        data = response.json()
        assert "period_days" in data

    @pytest.mark.integration
    def test_schedule_job(self, client, test_db, sample_companies):
        """Test scheduling a new collection job."""
        company_ids = [c.id for c in sample_companies]
        response = client.post(
            "/api/v1/people-jobs/schedule",
            json={
                "job_type": "website_crawl",
                "company_ids": company_ids,
                "priority": "all",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert data["status"] == "pending"

    @pytest.mark.integration
    def test_schedule_job_invalid_type(self, client):
        """Test scheduling job with invalid type."""
        response = client.post(
            "/api/v1/people-jobs/schedule",
            json={
                "job_type": "invalid_type",
                "company_ids": [1],
            },
        )

        assert response.status_code == 400


class TestAlertsEndpoints:
    """Tests for /api/v1/people-jobs/alerts endpoints."""

    @pytest.mark.integration
    def test_get_recent_alerts(self, client, test_db, sample_leadership_changes):
        """Test getting recent alerts."""
        response = client.get("/api/v1/people-jobs/alerts/recent", params={"days": 30})

        assert response.status_code == 200
        data = response.json()
        assert "total_alerts" in data
        assert "alerts" in data

    @pytest.mark.integration
    def test_get_portfolio_alerts(self, client, test_db, sample_portfolio, sample_leadership_changes):
        """Test getting portfolio alerts."""
        response = client.get(f"/api/v1/people-jobs/alerts/portfolio/{sample_portfolio.id}")

        assert response.status_code == 200
        data = response.json()
        assert "filter_type" in data
        assert data["filter_type"] == "portfolio"


class TestDigestEndpoints:
    """Tests for /api/v1/people-jobs/digest endpoints."""

    @pytest.mark.integration
    def test_get_weekly_digest(self, client, test_db, sample_leadership_changes):
        """Test getting weekly digest."""
        response = client.get("/api/v1/people-jobs/digest/weekly")

        assert response.status_code == 200
        data = response.json()
        assert "generated_at" in data
        assert "summary" in data

    @pytest.mark.integration
    def test_get_weekly_digest_with_portfolio_filter(self, client, test_db, sample_portfolio):
        """Test getting weekly digest filtered by portfolio."""
        response = client.get(
            "/api/v1/people-jobs/digest/weekly",
            params={"portfolio_id": sample_portfolio.id},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["filter"]["type"] == "portfolio"

    @pytest.mark.integration
    def test_get_change_summary(self, client, test_db, sample_leadership_changes):
        """Test getting change summary."""
        response = client.get("/api/v1/people-jobs/digest/summary", params={"days": 30})

        assert response.status_code == 200
        data = response.json()
        assert "total_changes" in data
