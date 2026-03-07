"""
Unit tests for #4: Source Watermark Service.

Tests:
- get_watermark returns None for first run
- advance_watermark creates and updates rows
- advance_watermark never moves backward
- inject_incremental_from_watermark delegates correctly
- First run (no watermark) results in full load
- Batch incremental injection works
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from app.core.models import SourceWatermark


class TestGetWatermark:
    """Tests for get_watermark()."""

    def test_returns_none_for_unknown_source(self):
        """First run with no watermark should return None (full load)."""
        from app.core.watermark_service import get_watermark

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None

        result = get_watermark(db, "fred")
        assert result is None

    def test_returns_timestamp_for_known_source(self):
        """Should return last_success_at from existing watermark."""
        from app.core.watermark_service import get_watermark

        ts = datetime(2026, 3, 1, 12, 0, 0)
        watermark = MagicMock(spec=SourceWatermark)
        watermark.last_success_at = ts

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = watermark

        result = get_watermark(db, "fred")
        assert result == ts


class TestAdvanceWatermark:
    """Tests for advance_watermark()."""

    def test_creates_new_watermark_on_first_success(self):
        """Should create a new SourceWatermark row when none exists."""
        from app.core.watermark_service import advance_watermark

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None

        ts = datetime(2026, 3, 3, 14, 0, 0)
        advance_watermark(db, "fred", ts, job_id=42)

        db.add.assert_called_once()
        added = db.add.call_args[0][0]
        assert added.source == "fred"
        assert added.last_success_at == ts
        assert added.last_job_id == 42
        db.commit.assert_called_once()

    def test_advances_existing_watermark(self):
        """Should update existing watermark when timestamp is newer."""
        from app.core.watermark_service import advance_watermark

        old_ts = datetime(2026, 3, 1, 12, 0, 0)
        new_ts = datetime(2026, 3, 3, 14, 0, 0)

        existing = MagicMock(spec=SourceWatermark)
        existing.last_success_at = old_ts

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = existing

        advance_watermark(db, "fred", new_ts, job_id=43)

        assert existing.last_success_at == new_ts
        assert existing.last_job_id == 43
        db.commit.assert_called_once()

    def test_never_moves_watermark_backward(self):
        """Should NOT update if new timestamp is older."""
        from app.core.watermark_service import advance_watermark

        recent_ts = datetime(2026, 3, 3, 14, 0, 0)
        old_ts = datetime(2026, 3, 1, 12, 0, 0)

        existing = MagicMock(spec=SourceWatermark)
        existing.last_success_at = recent_ts

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = existing

        advance_watermark(db, "fred", old_ts, job_id=44)

        # Should not have changed
        assert existing.last_success_at == recent_ts
        db.commit.assert_not_called()


class TestInjectIncrementalFromWatermark:
    """Tests for inject_incremental_from_watermark()."""

    def test_injects_start_param_from_watermark(self):
        """Should inject source-specific start param from watermark."""
        from app.core.watermark_service import inject_incremental_from_watermark

        ts = datetime(2026, 3, 1, 12, 0, 0)

        db = MagicMock()
        watermark = MagicMock(spec=SourceWatermark)
        watermark.last_success_at = ts
        db.query.return_value.filter.return_value.first.return_value = watermark

        config = {"incremental": True, "category": "rates"}
        result = inject_incremental_from_watermark(config, "fred", db)

        assert result["observation_start"] == "2026-03-01"
        assert result["category"] == "rates"

    def test_full_load_when_no_watermark(self):
        """First run (no watermark) should not inject any start param."""
        from app.core.watermark_service import inject_incremental_from_watermark

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None

        config = {"incremental": True}
        result = inject_incremental_from_watermark(config, "fred", db)

        assert "observation_start" not in result

    def test_handles_composite_source_key(self):
        """Should strip the suffix from composite keys like 'eia:electricity'."""
        from app.core.watermark_service import inject_incremental_from_watermark

        ts = datetime(2026, 2, 15, 10, 0, 0)

        db = MagicMock()
        watermark = MagicMock(spec=SourceWatermark)
        watermark.last_success_at = ts
        db.query.return_value.filter.return_value.first.return_value = watermark

        config = {"incremental": True}
        result = inject_incremental_from_watermark(config, "eia:electricity", db)

        assert result["start"] == "2026-02-15"


class TestBatchIncrementalInjection:
    """Tests for incremental injection in the ingestion executor."""

    def test_watermark_inject_function_is_importable(self):
        """inject_incremental_from_watermark should be importable."""
        from app.core.watermark_service import inject_incremental_from_watermark

        assert callable(inject_incremental_from_watermark)

    def test_inject_passes_through_to_scheduler(self):
        """inject_incremental_from_watermark should delegate to _inject_incremental_params."""
        from app.core.watermark_service import inject_incremental_from_watermark

        ts = datetime(2026, 3, 1, 12, 0, 0)
        db = MagicMock()
        watermark = MagicMock(spec=SourceWatermark)
        watermark.last_success_at = ts
        db.query.return_value.filter.return_value.first.return_value = watermark

        config = {"incremental": True}
        result = inject_incremental_from_watermark(config, "bls", db)

        # BLS uses start_year
        assert result["start_year"] == 2026
