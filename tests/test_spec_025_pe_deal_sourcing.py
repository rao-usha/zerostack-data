"""
Tests for SPEC 025 — PE Deal Sourcing Service.
"""
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class MockScoreResult:
    """Mock DealScoreResult."""
    company_id: int
    company_name: str
    composite_score: float
    grade: str
    dimensions: list
    strengths: list
    risks: list
    data_gaps: list


def _mock_company(company_id, name, industry="Healthcare"):
    co = MagicMock()
    co.id = company_id
    co.name = name
    co.industry = industry
    return co


@pytest.mark.unit
class TestSpec025PeDealSourcing:
    """Tests for PE deal sourcing automation."""

    def _mock_firm(self, firm_id=1, name="Test Capital"):
        firm = MagicMock()
        firm.id = firm_id
        firm.name = name
        return firm

    def _firm_db(self, firm):
        """Return a mock DB that resolves firm lookup."""
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = firm
        db.flush.return_value = None
        db.commit.return_value = None
        return db

    def test_source_from_signals_creates_deals(self):
        """T1: High-score candidates become pipeline entries."""
        from app.core.pe_deal_sourcing import source_deals_from_signals

        firm = self._mock_firm()
        db = self._firm_db(firm)
        co1 = _mock_company(10, "HealthCo", "Healthcare")

        mock_signals = [{"sector": "Healthcare", "momentum_score": 80}]
        score_result = MockScoreResult(
            company_id=10, company_name="HealthCo",
            composite_score=85.0, grade="A",
            dimensions=[], strengths=["Strong growth"],
            risks=[], data_gaps=[],
        )

        with patch("app.core.pe_deal_sourcing.get_high_momentum_sectors", return_value=mock_signals), \
             patch("app.core.pe_deal_sourcing._find_candidates_by_sector", return_value=[co1]), \
             patch("app.core.pe_deal_sourcing._get_pipeline_company_ids", return_value=set()), \
             patch("app.core.pe_deal_sourcing.score_deal", return_value=score_result):
            report = source_deals_from_signals(db, firm_id=1)

        assert report.deals_created == 1
        assert report.candidates_found == 1
        assert len(report.top_opportunities) == 1
        assert report.top_opportunities[0]["company_name"] == "HealthCo"

    def test_source_from_signals_skips_duplicates(self):
        """T2: Existing pipeline companies not re-added."""
        from app.core.pe_deal_sourcing import source_deals_from_signals

        firm = self._mock_firm()
        db = self._firm_db(firm)
        co1 = _mock_company(10, "HealthCo", "Healthcare")

        mock_signals = [{"sector": "Healthcare", "momentum_score": 80}]

        with patch("app.core.pe_deal_sourcing.get_high_momentum_sectors", return_value=mock_signals), \
             patch("app.core.pe_deal_sourcing._find_candidates_by_sector", return_value=[co1]), \
             patch("app.core.pe_deal_sourcing._get_pipeline_company_ids", return_value={10}):
            report = source_deals_from_signals(db, firm_id=1)

        assert report.deals_created == 0
        assert report.deals_skipped_duplicate == 1

    def test_source_from_signals_score_threshold(self):
        """T3: Only B+ candidates (>=70) get pipeline entries."""
        from app.core.pe_deal_sourcing import source_deals_from_signals

        firm = self._mock_firm()
        db = self._firm_db(firm)
        co1 = _mock_company(10, "LowScore Inc", "Healthcare")

        mock_signals = [{"sector": "Healthcare", "momentum_score": 80}]
        low_score = MockScoreResult(
            company_id=10, company_name="LowScore Inc",
            composite_score=50.0, grade="C",
            dimensions=[], strengths=[], risks=["Weak margins"],
            data_gaps=[],
        )

        with patch("app.core.pe_deal_sourcing.get_high_momentum_sectors", return_value=mock_signals), \
             patch("app.core.pe_deal_sourcing._find_candidates_by_sector", return_value=[co1]), \
             patch("app.core.pe_deal_sourcing._get_pipeline_company_ids", return_value=set()), \
             patch("app.core.pe_deal_sourcing.score_deal", return_value=low_score):
            report = source_deals_from_signals(db, firm_id=1)

        assert report.deals_created == 0
        assert report.deals_skipped_low_score == 1

    def test_get_sourcing_history(self):
        """T4: Returns correct stats for firm."""
        from app.core.pe_deal_sourcing import get_sourcing_history

        db = MagicMock()

        deal1 = MagicMock(id=1, company_id=10, deal_name="D1", status="Screening",
                          data_source="market_scanner", created_at=datetime.utcnow())
        deal2 = MagicMock(id=2, company_id=20, deal_name="D2", status="DD",
                          data_source="market_scanner", created_at=datetime.utcnow())
        deal3 = MagicMock(id=3, company_id=30, deal_name="D3", status="Screening",
                          data_source="acquisition_scorer", created_at=datetime.utcnow())

        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [deal1, deal2, deal3]
        db.execute.return_value = result_mock

        result = get_sourcing_history(db, firm_id=1, days=30)

        assert result["firm_id"] == 1
        assert result["total_sourced"] == 3
        assert result["by_source"]["market_scanner"] == 2
        assert result["by_source"]["acquisition_scorer"] == 1
        assert result["conversion_rate"] > 0

    def test_source_from_targets(self):
        """T5: Scores companies and creates entries."""
        from app.core.pe_deal_sourcing import source_deals_from_targets

        firm = self._mock_firm()
        db = self._firm_db(firm)
        co1 = _mock_company(10, "TargetCo")

        score_result = MockScoreResult(
            company_id=10, company_name="TargetCo",
            composite_score=78.0, grade="B",
            dimensions=[], strengths=["Solid EBITDA"],
            risks=[], data_gaps=[],
        )

        with patch("app.core.pe_deal_sourcing._find_all_scoreable_candidates", return_value=[co1]), \
             patch("app.core.pe_deal_sourcing._get_pipeline_company_ids", return_value=set()), \
             patch("app.core.pe_deal_sourcing.score_deal", return_value=score_result):
            report = source_deals_from_targets(db, firm_id=1)

        assert report.source_type == "acquisition_scorer"
        assert report.deals_created == 1
        assert report.top_opportunities[0]["score"] == 78.0
