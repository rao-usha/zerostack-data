"""
Tests for data continuity validation (#3).

Covers:
- Row count drop alert creation
- No alert for row count increase
- No alert when no previous profile exists
- Date gap detection
- Date gap check handles errors gracefully
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from app.core.data_quality_service import check_row_count_delta, check_date_gaps


class TestRowCountDelta:
    """Test check_row_count_delta function."""

    def test_drop_creates_alert(self):
        """Row count drop >20% should create a warning alert."""
        mock_snapshot = MagicMock()
        mock_snapshot.row_count = 1000

        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_snapshot

        job = MagicMock(id=42)

        alert = check_row_count_delta(db, job, "fred_interest_rates", 700)
        assert alert is not None
        assert "dropped" in alert.message.lower() or "drop" in alert.message.lower()
        assert alert.details["previous"] == 1000
        assert alert.details["current"] == 700
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_no_alert_for_increase(self):
        """Row count increase should not create an alert."""
        mock_snapshot = MagicMock()
        mock_snapshot.row_count = 1000

        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_snapshot

        job = MagicMock(id=42)

        alert = check_row_count_delta(db, job, "fred_interest_rates", 1200)
        assert alert is None
        db.add.assert_not_called()

    def test_no_alert_for_small_drop(self):
        """Row count drop <=20% should not create an alert."""
        mock_snapshot = MagicMock()
        mock_snapshot.row_count = 1000

        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_snapshot

        job = MagicMock(id=42)

        alert = check_row_count_delta(db, job, "fred_interest_rates", 850)
        assert alert is None

    def test_no_alert_for_first_profile(self):
        """No previous profile snapshot should return None (no baseline)."""
        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

        job = MagicMock(id=42)

        alert = check_row_count_delta(db, job, "new_table", 500)
        assert alert is None

    def test_no_alert_when_prev_row_count_is_zero(self):
        """Previous snapshot with 0 row count should return None (avoid div by zero)."""
        mock_snapshot = MagicMock()
        mock_snapshot.row_count = 0

        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_snapshot

        job = MagicMock(id=42)

        alert = check_row_count_delta(db, job, "fred_interest_rates", 100)
        assert alert is None


class TestDateGaps:
    """Test check_date_gaps function."""

    def test_gaps_detected(self):
        """Should create alerts for detected date gaps."""
        mock_row = MagicMock()
        mock_row.m = datetime(2025, 1, 1)
        mock_row.next_m = datetime(2025, 5, 1)

        db = MagicMock()
        db.execute.return_value = [mock_row]

        alerts = check_date_gaps(db, "fred_interest_rates", "date", job_id=10)
        assert len(alerts) == 1
        assert "Date gap" in alerts[0].message
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_no_gaps_returns_empty(self):
        """No gaps should return empty list."""
        db = MagicMock()
        db.execute.return_value = []

        alerts = check_date_gaps(db, "fred_interest_rates", "date", job_id=10)
        assert alerts == []
        db.add.assert_not_called()

    def test_exception_returns_empty(self):
        """Database errors should return empty list (advisory, never raises)."""
        db = MagicMock()
        db.execute.side_effect = Exception("column does not exist")

        alerts = check_date_gaps(db, "bad_table", "nonexistent_col", job_id=10)
        assert alerts == []
