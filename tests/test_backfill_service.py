"""
Unit tests for #5: Backfill Framework.

Tests:
- Source-specific param translation (start + end)
- Backfill flag set on job config
- Backfill jobs don't advance watermark
- Multi-source backfill
- BackfillRequest schema validation
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from app.core.models import IngestionJob, JobStatus


class TestCreateBackfillJob:
    """Tests for create_backfill_job()."""

    def test_translates_fred_date_params(self):
        """FRED should get observation_start and observation_end."""
        from app.core.backfill_service import create_backfill_job

        db = MagicMock()
        db.flush = MagicMock()

        # Capture what gets added
        added_jobs = []
        original_add = db.add

        def capture_add(obj):
            added_jobs.append(obj)

        db.add = capture_add

        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)

        job = create_backfill_job(db, "fred", start, end)

        assert job.config["observation_start"] == "2024-01-01"
        assert job.config["observation_end"] == "2024-12-31"
        assert job.config["_backfill"] is True
        assert job.trigger == "backfill"

    def test_translates_bls_year_params(self):
        """BLS should get start_year and end_year."""
        from app.core.backfill_service import create_backfill_job

        db = MagicMock()
        db.flush = MagicMock()
        db.add = MagicMock()

        start = datetime(2020, 6, 15)
        end = datetime(2024, 9, 30)

        job = create_backfill_job(db, "bls", start, end)

        assert job.config["start_year"] == 2020
        assert job.config["end_year"] == 2024

    def test_translates_census_year(self):
        """Census should get year param for both start and end."""
        from app.core.backfill_service import create_backfill_job

        db = MagicMock()
        db.flush = MagicMock()
        db.add = MagicMock()

        start = datetime(2022, 1, 1)
        end = datetime(2023, 12, 31)

        job = create_backfill_job(db, "census", start, end)

        # Census uses 'year' for both start and end (end overrides)
        assert job.config["year"] == 2023

    def test_preserves_extra_config(self):
        """Extra config params should be merged in."""
        from app.core.backfill_service import create_backfill_job

        db = MagicMock()
        db.flush = MagicMock()
        db.add = MagicMock()

        start = datetime(2024, 1, 1)
        end = datetime(2024, 6, 30)

        job = create_backfill_job(
            db, "fred", start, end, extra_config={"category": "interest_rates"}
        )

        assert job.config["category"] == "interest_rates"
        assert job.config["observation_start"] == "2024-01-01"

    def test_backfill_flag_always_set(self):
        """_backfill flag should always be True on backfill jobs."""
        from app.core.backfill_service import create_backfill_job

        db = MagicMock()
        db.flush = MagicMock()
        db.add = MagicMock()

        job = create_backfill_job(
            db, "treasury", datetime(2024, 1, 1), datetime(2024, 12, 31)
        )

        assert job.config["_backfill"] is True
        assert job.trigger == "backfill"


class TestLaunchBackfill:
    """Tests for launch_backfill()."""

    def test_multi_source_backfill(self):
        """Should create jobs for all requested sources."""
        from app.core.backfill_service import launch_backfill

        db = MagicMock()
        db.flush = MagicMock()
        db.commit = MagicMock()

        mock_job = MagicMock()
        mock_job.id = 1
        mock_job.config = {"_backfill": True}

        with patch("app.core.backfill_service.WORKER_MODE", True):
            with patch("app.core.backfill_service.submit_job"):
                with patch(
                    "app.core.backfill_service.IngestionJob",
                    return_value=mock_job,
                ):
                    result = launch_backfill(
                        db,
                        sources=["fred", "treasury", "bls"],
                        start_date=datetime(2024, 1, 1),
                        end_date=datetime(2024, 12, 31),
                    )

        assert result["total_jobs"] == 3
        assert len(result["job_ids"]) == 3
        assert result["sources"] == ["fred", "treasury", "bls"]

    def test_requires_worker_mode(self):
        """Should raise when WORKER_MODE is off."""
        from app.core.backfill_service import launch_backfill

        db = MagicMock()

        with patch("app.core.backfill_service.WORKER_MODE", False):
            with pytest.raises(RuntimeError, match="WORKER_MODE"):
                launch_backfill(
                    db,
                    sources=["fred"],
                    start_date=datetime(2024, 1, 1),
                    end_date=datetime(2024, 12, 31),
                )


class TestBackfillWatermarkGuard:
    """Tests that backfill jobs don't advance the source watermark."""

    @pytest.mark.asyncio
    async def test_backfill_job_skips_watermark_advance(self):
        """_handle_job_completion should skip watermark for backfill trigger."""
        from app.api.v1.jobs import _handle_job_completion

        db = MagicMock()

        job = IngestionJob()
        job.id = 1
        job.source = "fred"
        job.status = JobStatus.SUCCESS
        job.trigger = "backfill"
        job.config = {"_backfill": True, "observation_start": "2024-01-01"}
        job.completed_at = datetime(2026, 3, 3, 14, 0, 0)
        job.schedule_id = None

        with patch("app.core.dependency_service.check_and_unblock_dependent_jobs", return_value=[]):
            with patch("app.core.dependency_service.get_execution_for_job", return_value=None):
                with patch("app.core.watermark_service.advance_watermark") as mock_advance:
                    await _handle_job_completion(db, job)

        # Should NOT have been called because trigger == "backfill"
        mock_advance.assert_not_called()

    @pytest.mark.asyncio
    async def test_normal_job_advances_watermark(self):
        """Non-backfill SUCCESS jobs should advance the watermark."""
        from app.api.v1.jobs import _handle_job_completion

        db = MagicMock()

        job = IngestionJob()
        job.id = 2
        job.source = "fred"
        job.status = JobStatus.SUCCESS
        job.trigger = "batch"
        job.config = {"category": "rates"}
        job.completed_at = datetime(2026, 3, 3, 14, 0, 0)
        job.schedule_id = None

        with patch("app.core.dependency_service.check_and_unblock_dependent_jobs", return_value=[]):
            with patch("app.core.dependency_service.get_execution_for_job", return_value=None):
                with patch("app.core.watermark_service.advance_watermark") as mock_advance:
                    await _handle_job_completion(db, job)

        mock_advance.assert_called_once_with(db, "fred", job.completed_at, 2)


class TestBackfillRequestSchema:
    """Tests for BackfillRequest Pydantic model."""

    def test_valid_request(self):
        """Valid request should parse successfully."""
        from app.core.schemas import BackfillRequest

        req = BackfillRequest(
            sources=["fred", "treasury"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        assert req.sources == ["fred", "treasury"]
        assert req.start_date == "2024-01-01"

    def test_invalid_date_format(self):
        """Invalid date format should raise validation error."""
        from app.core.schemas import BackfillRequest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            BackfillRequest(
                sources=["fred"],
                start_date="01-01-2024",
                end_date="2024-12-31",
            )

    def test_empty_sources_rejected(self):
        """Empty sources list should be rejected."""
        from app.core.schemas import BackfillRequest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            BackfillRequest(
                sources=[],
                start_date="2024-01-01",
                end_date="2024-12-31",
            )

    def test_default_config_is_empty_dict(self):
        """Config should default to empty dict."""
        from app.core.schemas import BackfillRequest

        req = BackfillRequest(
            sources=["fred"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        assert req.config == {}


class TestIncrementalEndParamMap:
    """Tests for INCREMENTAL_END_PARAM_MAP in scheduler_service."""

    def test_end_param_map_has_all_start_sources(self):
        """Every source in INCREMENTAL_PARAM_MAP should have an end param."""
        from app.core.scheduler_service import (
            INCREMENTAL_PARAM_MAP,
            INCREMENTAL_END_PARAM_MAP,
        )

        for source in INCREMENTAL_PARAM_MAP:
            assert source in INCREMENTAL_END_PARAM_MAP, (
                f"Source '{source}' in INCREMENTAL_PARAM_MAP but missing from "
                f"INCREMENTAL_END_PARAM_MAP"
            )

    def test_fred_end_param(self):
        """FRED end param should be observation_end with date format."""
        from app.core.scheduler_service import INCREMENTAL_END_PARAM_MAP

        param_name, formatter = INCREMENTAL_END_PARAM_MAP["fred"]
        assert param_name == "observation_end"
        assert formatter(datetime(2024, 12, 31)) == "2024-12-31"

    def test_bls_end_param(self):
        """BLS end param should be end_year as int."""
        from app.core.scheduler_service import INCREMENTAL_END_PARAM_MAP

        param_name, formatter = INCREMENTAL_END_PARAM_MAP["bls"]
        assert param_name == "end_year"
        assert formatter(datetime(2024, 6, 15)) == 2024
