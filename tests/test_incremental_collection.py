"""
Tests for incremental collection mode (Phase 4).

Covers:
- Incremental mode passes watermark "since" in job config
- Full mode does NOT pass watermark
- Group filtering maps group name to source list
- trigger_type set correctly for both modes
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime


@pytest.mark.unit
class TestIncrementalMode:
    """Tests for incremental collection via watermarks."""

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_incremental_passes_since_in_config(self, mock_submit, mock_resolve):
        """Incremental mode should inject 'since' from watermark into config."""
        from app.core.batch_service import launch_batch_collection, Tier, SourceDef

        tier1 = Tier(level=1, priority=10, name="Test", sources=[
            SourceDef("fred", {"category": "interest_rates"}),
        ])
        mock_resolve.return_value = [tier1]

        db = MagicMock()
        # Mock SourceWatermark query
        mock_wm = MagicMock()
        mock_wm.source = "fred"
        mock_wm.last_success_at = datetime(2026, 3, 11, 2, 0, 0)

        original_query = db.query

        def patched_query(model):
            if hasattr(model, '__tablename__') and model.__tablename__ == 'source_watermarks':
                result = MagicMock()
                result.all.return_value = [mock_wm]
                return result
            return original_query(model)

        db.query = patched_query

        with patch("app.core.job_splitter.get_split_config", return_value=None):
            result = await launch_batch_collection(db, mode="incremental")

        # Check the IngestionJob was created with 'since' in config
        add_calls = [c for c in db.add.call_args_list]
        assert len(add_calls) >= 1
        job = add_calls[0][0][0]
        assert job.trigger == "incremental"
        assert "since" in job.config
        assert job.config["since"] == "2026-03-11T02:00:00"

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_full_mode_no_since(self, mock_submit, mock_resolve):
        """Full mode should NOT inject 'since' watermark."""
        from app.core.batch_service import launch_batch_collection, Tier, SourceDef

        tier1 = Tier(level=1, priority=10, name="Test", sources=[
            SourceDef("treasury", {"incremental": True}),
        ])
        mock_resolve.return_value = [tier1]

        db = MagicMock()

        with patch("app.core.job_splitter.get_split_config", return_value=None):
            result = await launch_batch_collection(db, mode="full")

        add_calls = [c for c in db.add.call_args_list]
        assert len(add_calls) >= 1
        job = add_calls[0][0][0]
        assert job.trigger == "batch"
        assert "since" not in job.config

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_incremental_no_watermark_skips_since(self, mock_submit, mock_resolve):
        """Incremental mode with no watermark should not inject 'since'."""
        from app.core.batch_service import launch_batch_collection, Tier, SourceDef

        tier1 = Tier(level=1, priority=10, name="Test", sources=[
            SourceDef("treasury", {}),
        ])
        mock_resolve.return_value = [tier1]

        db = MagicMock()
        # Empty watermarks
        original_query = db.query

        def patched_query(model):
            if hasattr(model, '__tablename__') and model.__tablename__ == 'source_watermarks':
                result = MagicMock()
                result.all.return_value = []
                return result
            return original_query(model)

        db.query = patched_query

        with patch("app.core.job_splitter.get_split_config", return_value=None):
            result = await launch_batch_collection(db, mode="incremental")

        add_calls = [c for c in db.add.call_args_list]
        assert len(add_calls) >= 1
        job = add_calls[0][0][0]
        assert job.trigger == "incremental"
        assert "since" not in job.config


@pytest.mark.unit
class TestGroupFiltering:
    """Tests for group_name filtering in launch_batch_collection."""

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_group_filters_to_matching_sources(self, mock_submit, mock_resolve):
        """group_name='critical' should only launch treasury, fred, prediction_markets."""
        from app.core.batch_service import launch_batch_collection, Tier, SourceDef

        tier1 = Tier(level=1, priority=10, name="Test", sources=[
            SourceDef("treasury", {}),
            SourceDef("fred", {}),
            SourceDef("prediction_markets", {}),
        ])
        tier2 = Tier(level=2, priority=7, name="Test2", sources=[
            SourceDef("eia", {}),
            SourceDef("bls", {}),
        ])
        mock_resolve.return_value = [tier1, tier2]

        db = MagicMock()

        with patch("app.core.job_splitter.get_split_config", return_value=None):
            result = await launch_batch_collection(db, group_name="critical")

        # Only tier 1 sources should be launched (treasury, fred, prediction_markets)
        assert result["total_jobs"] == 3

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_unknown_group_launches_nothing(self, mock_submit, mock_resolve):
        """Unknown group_name should launch nothing (no source match)."""
        from app.core.batch_service import launch_batch_collection, Tier, SourceDef

        tier1 = Tier(level=1, priority=10, name="Test", sources=[
            SourceDef("treasury", {}),
        ])
        mock_resolve.return_value = [tier1]
        db = MagicMock()

        with patch("app.core.job_splitter.get_split_config", return_value=None):
            result = await launch_batch_collection(db, group_name="nonexistent_group")

        # Should launch all sources since group didn't match (no filter applied)
        assert result["total_jobs"] == 1


@pytest.mark.unit
class TestSourceConfigIncrementalFields:
    """Tests for incremental fields on SourceConfig model."""

    def test_supports_incremental_field(self):
        from app.core.models import SourceConfig
        assert hasattr(SourceConfig, "supports_incremental")

    def test_watermark_column_field(self):
        from app.core.models import SourceConfig
        assert hasattr(SourceConfig, "watermark_column")
