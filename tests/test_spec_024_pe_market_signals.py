"""
Tests for SPEC 024 — PE Market Signals Service.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


@pytest.mark.unit
class TestSpec024PeMarketSignals:
    """Tests for PE market signal storage and retrieval."""

    def _sample_signals(self):
        return [
            {
                "industry": "Healthcare",
                "recent_deal_count": 15,
                "prior_deal_count": 10,
                "current_median_ev_ebitda": 12.5,
                "prior_median_ev_ebitda": 11.0,
                "momentum_score": 82,
                "momentum": "bullish",
                "deal_flow_change_pct": 50.0,
                "multiple_change_pct": 13.6,
                "top_companies": ["CompanyA", "CompanyB"],
            },
            {
                "industry": "Software",
                "recent_deal_count": 8,
                "prior_deal_count": 12,
                "current_median_ev_ebitda": 15.0,
                "prior_median_ev_ebitda": 16.0,
                "momentum_score": 45,
                "momentum": "bearish",
                "deal_flow_change_pct": -33.3,
                "multiple_change_pct": -6.3,
            },
            {
                "industry": "Industrials",
                "recent_deal_count": 10,
                "prior_deal_count": 9,
                "current_median_ev_ebitda": 8.5,
                "prior_median_ev_ebitda": 8.0,
                "momentum_score": 65,
                "momentum": "neutral",
                "deal_flow_change_pct": 11.1,
                "multiple_change_pct": 6.3,
            },
        ]

    def test_store_signals(self):
        """T1: Signals are persisted to DB correctly."""
        from app.core.pe_market_signals import store_signals

        db = MagicMock()
        db.commit.return_value = None

        signals = self._sample_signals()
        count = store_signals(db, signals, batch_id="test_batch_1")

        assert count == 3
        assert db.add.call_count == 3
        db.commit.assert_called_once()

    def test_store_signals_empty(self):
        """T1b: Empty signal list returns 0."""
        from app.core.pe_market_signals import store_signals

        db = MagicMock()
        count = store_signals(db, [])
        assert count == 0
        db.add.assert_not_called()

    def test_get_latest_signals(self):
        """T2: Returns most recent signals per sector."""
        from app.core.pe_market_signals import get_latest_signals

        db = MagicMock()

        # Mock: latest batch_id query
        exec_mock = MagicMock()
        exec_mock.scalar.return_value = "batch_123"
        db.execute.return_value = exec_mock

        # Mock: query for signals in that batch
        sig1 = MagicMock()
        sig1.id = 1
        sig1.sector = "Healthcare"
        sig1.momentum_score = 82
        sig1.deal_count = 15
        sig1.avg_multiple = 12.5
        sig1.signal_type = "bullish"
        sig1.top_companies = ["A"]
        sig1.deal_flow_change_pct = 50.0
        sig1.multiple_change_pct = 13.6
        sig1.batch_id = "batch_123"
        sig1.scanned_at = datetime(2026, 3, 13, 7, 0, 0)

        query_mock = MagicMock()
        query_mock.filter.return_value = query_mock
        query_mock.order_by.return_value = query_mock
        query_mock.all.return_value = [sig1]
        db.query.return_value = query_mock

        result = get_latest_signals(db)

        assert len(result) == 1
        assert result[0]["sector"] == "Healthcare"
        assert result[0]["momentum_score"] == 82

    def test_get_high_momentum_sectors(self):
        """T3: Filters by momentum threshold."""
        from app.core.pe_market_signals import get_high_momentum_sectors

        db = MagicMock()

        exec_mock = MagicMock()
        exec_mock.scalar.return_value = "batch_123"
        db.execute.return_value = exec_mock

        # Only Healthcare (82) passes threshold=60
        sig1 = MagicMock()
        sig1.id = 1
        sig1.sector = "Healthcare"
        sig1.momentum_score = 82
        sig1.deal_count = 15
        sig1.avg_multiple = 12.5
        sig1.signal_type = "bullish"
        sig1.top_companies = None
        sig1.deal_flow_change_pct = 50.0
        sig1.multiple_change_pct = 13.6
        sig1.batch_id = "batch_123"
        sig1.scanned_at = datetime(2026, 3, 13, 7, 0, 0)

        query_mock = MagicMock()
        query_mock.filter.return_value = query_mock
        query_mock.order_by.return_value = query_mock
        query_mock.all.return_value = [sig1]
        db.query.return_value = query_mock

        result = get_high_momentum_sectors(db, threshold=60)

        assert len(result) == 1
        assert result[0]["sector"] == "Healthcare"
        assert result[0]["momentum_score"] >= 60

    def test_run_market_scan_orchestration(self):
        """T4: Full scan-store-return flow."""
        from app.core.pe_market_signals import run_market_scan

        db = MagicMock()
        db.commit.return_value = None

        mock_signals = self._sample_signals()

        with patch("app.core.pe_market_scanner.MarketScannerService") as MockScanner:
            instance = MockScanner.return_value
            instance.get_market_signals.return_value = mock_signals

            with patch("app.core.pe_market_signals._fire_opportunity_webhooks"):
                result = run_market_scan(db)

        assert result["status"] == "complete"
        assert result["signals_stored"] == 3
        assert result["total_sectors"] == 3
        # Healthcare has momentum_score 82 > 75
        assert len(result["high_momentum"]) == 1
        assert result["high_momentum"][0]["sector"] == "Healthcare"

    def test_model_exists(self):
        """T5: PEMarketSignal model has expected columns."""
        from app.core.pe_models import PEMarketSignal

        assert PEMarketSignal.__tablename__ == "pe_market_signals"
        assert hasattr(PEMarketSignal, "sector")
        assert hasattr(PEMarketSignal, "momentum_score")
        assert hasattr(PEMarketSignal, "deal_count")
        assert hasattr(PEMarketSignal, "avg_multiple")
        assert hasattr(PEMarketSignal, "signal_type")
        assert hasattr(PEMarketSignal, "top_companies")
        assert hasattr(PEMarketSignal, "batch_id")
        assert hasattr(PEMarketSignal, "scanned_at")
