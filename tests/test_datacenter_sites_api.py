"""Tests for datacenter sites API endpoints."""

import pytest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.main import app


def get_mock_db():
    """Yield a mock database session."""
    db = MagicMock(spec=Session)
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_result.fetchone.return_value = None
    mock_result.scalar.return_value = 0
    mock_result.keys.return_value = []
    mock_result.scalars.return_value = MagicMock(
        all=MagicMock(return_value=[])
    )
    db.execute.return_value = mock_result
    db.query.return_value = MagicMock(
        filter=MagicMock(return_value=MagicMock(
            all=MagicMock(return_value=[]),
            first=MagicMock(return_value=None),
            count=MagicMock(return_value=0),
        ))
    )
    try:
        yield db
    finally:
        pass


@pytest.fixture
def client():
    """Create a TestClient with mocked DB dependency."""
    from app.core.database import get_db

    app.dependency_overrides[get_db] = get_mock_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.mark.unit
class TestDatacenterSitesAPI:
    """Unit tests for datacenter sites API endpoints."""

    def test_methodology_endpoint(self, client):
        """GET /methodology should return site_suitability and regulatory_speed keys."""
        with patch(
            "app.ml.datacenter_site_scorer.DatacenterSiteScorer"
        ) as MockSiteScorer, patch(
            "app.ml.county_regulatory_scorer.CountyRegulatoryScorer"
        ) as MockRegScorer:
            MockSiteScorer.get_methodology.return_value = {
                "model_version": "1.0",
                "weights": {},
                "domains": [],
            }
            MockRegScorer.get_methodology.return_value = {
                "model_version": "1.0",
                "weights": {},
                "factors": [],
            }

            response = client.get("/api/v1/datacenter-sites/methodology")
            assert response.status_code == 200
            data = response.json()
            assert "site_suitability" in data
            assert "regulatory_speed" in data

    def test_score_counties_returns_status(self, client):
        """POST /score-counties should return a scoring_started status."""
        response = client.post(
            "/api/v1/datacenter-sites/score-counties",
            json={"state": "TX", "force": False},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "scoring_started"
        assert data["state"] == "TX"

    def test_rankings_empty_db(self, client):
        """GET /rankings should return an empty county list when no data exists."""
        response = client.get("/api/v1/datacenter-sites/rankings")
        assert response.status_code == 200
        data = response.json()
        assert data["counties"] == []
        assert data["total"] == 0

    def test_county_detail_not_found(self, client):
        """GET /{fips} should return 404 for a missing county."""
        response = client.get("/api/v1/datacenter-sites/99999")
        assert response.status_code == 404

    def test_data_sources_endpoint(self, client):
        """GET /data-sources should return a sources list."""
        response = client.get("/api/v1/datacenter-sites/data-sources")
        assert response.status_code == 200
        data = response.json()
        assert "sources" in data
        assert isinstance(data["sources"], list)
        assert len(data["sources"]) > 0

    def test_top_states_empty(self, client):
        """GET /top-states should return empty results when no data exists."""
        response = client.get("/api/v1/datacenter-sites/top-states")
        assert response.status_code == 200
        data = response.json()
        assert "states" in data
        assert data["states"] == []
