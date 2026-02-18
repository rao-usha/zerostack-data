"""
Unit tests for app/core/job_helpers.py

Tests the create_and_dispatch_job() helper that eliminates boilerplate
across API routers by creating IngestionJob records, scheduling background
tasks, and returning standard response dicts.

All tests are fully offline (no DB, no network).
"""
import pytest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from app.core.job_helpers import create_and_dispatch_job
from app.core.models import JobStatus

# =============================================================================
# Helpers
# =============================================================================


def _make_mock_db(job_id: int = 42):
    """
    Build a mock SQLAlchemy Session.

    db.add() is a no-op, db.commit() succeeds, and db.refresh()
    stamps the job with the given *job_id*.
    """
    db = MagicMock()

    def _refresh(obj):
        obj.id = job_id

    db.refresh.side_effect = _refresh
    return db


def _make_mock_background_tasks():
    """Return a mock FastAPI BackgroundTasks instance."""
    return MagicMock()


# =============================================================================
# Happy path
# =============================================================================


class TestCreateAndDispatchJobHappyPath:
    """Tests for the normal (non-error) execution path."""

    @patch("app.core.job_helpers.IngestionJob")
    def test_returns_correct_response_dict(self, MockIngestionJob):
        """create_and_dispatch_job returns dict with job_id, status, message, check_status."""
        mock_job = MagicMock()
        mock_job.id = 42
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=42)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db,
            background_tasks=bg,
            source="fred",
            config={"category": "interest_rates"},
        )

        assert result["job_id"] == 42
        assert result["status"] == "pending"
        assert result["check_status"] == "/api/v1/jobs/42"

    @patch("app.core.job_helpers.IngestionJob")
    def test_job_added_to_session_and_committed(self, MockIngestionJob):
        """The job object is added to the DB session and committed."""
        mock_job = MagicMock()
        mock_job.id = 7
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=7)
        bg = _make_mock_background_tasks()

        create_and_dispatch_job(db=db, background_tasks=bg, source="eia", config={})

        db.add.assert_called_once_with(mock_job)
        db.commit.assert_called_once()
        db.refresh.assert_called_once_with(mock_job)

    @patch("app.core.job_helpers.IngestionJob")
    def test_job_created_with_pending_status_and_correct_source(self, MockIngestionJob):
        """IngestionJob is instantiated with PENDING status and the correct source."""
        mock_job = MagicMock()
        mock_job.id = 1
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=1)
        bg = _make_mock_background_tasks()

        create_and_dispatch_job(
            db=db, background_tasks=bg, source="sec", config={"dataset": "filings"}
        )

        MockIngestionJob.assert_called_once_with(
            source="sec",
            status=JobStatus.PENDING,
            config={"dataset": "filings"},
        )

    @patch("app.core.job_helpers.IngestionJob")
    def test_job_config_passed_through(self, MockIngestionJob):
        """The config dict is forwarded to IngestionJob constructor verbatim."""
        mock_job = MagicMock()
        mock_job.id = 10
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=10)
        bg = _make_mock_background_tasks()
        cfg = {"dataset": "pa_projects", "limit": 500, "year": 2024}

        create_and_dispatch_job(db=db, background_tasks=bg, source="fema", config=cfg)

        call_kwargs = MockIngestionJob.call_args[1]
        assert call_kwargs["config"] == cfg


# =============================================================================
# Message handling
# =============================================================================


class TestMessageHandling:
    """Tests for default vs custom message behavior."""

    @patch("app.core.job_helpers.IngestionJob")
    def test_default_message_when_none_provided(self, MockIngestionJob):
        """When message=None, uses default source + ingestion job created."""
        mock_job = MagicMock()
        mock_job.id = 1
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=1)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db, background_tasks=bg, source="fred", config={}
        )

        assert result["message"] == "fred ingestion job created"

    @patch("app.core.job_helpers.IngestionJob")
    def test_default_message_when_message_omitted(self, MockIngestionJob):
        """When message keyword is not passed at all, uses the default."""
        mock_job = MagicMock()
        mock_job.id = 2
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=2)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db, background_tasks=bg, source="bls", config={}
        )

        assert result["message"] == "bls ingestion job created"

    @patch("app.core.job_helpers.IngestionJob")
    def test_custom_message_used_when_provided(self, MockIngestionJob):
        """When a custom message is provided, it overrides the default."""
        mock_job = MagicMock()
        mock_job.id = 3
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=3)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db,
            background_tasks=bg,
            source="eia",
            config={},
            message="Custom: EIA petroleum data loading",
        )

        assert result["message"] == "Custom: EIA petroleum data loading"

    @patch("app.core.job_helpers.IngestionJob")
    def test_empty_string_message_uses_default(self, MockIngestionJob):
        """An empty string is falsy, so the default is used instead."""
        mock_job = MagicMock()
        mock_job.id = 4
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=4)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db,
            background_tasks=bg,
            source="census",
            config={},
            message="",
        )

        # Empty string is falsy, so the or-expression picks the default
        assert result["message"] == "census ingestion job created"


# =============================================================================
# Background task dispatch
# =============================================================================


class TestBackgroundTaskDispatch:
    """Tests that the background task is scheduled correctly."""

    @patch("app.core.job_helpers.IngestionJob")
    def test_background_task_added(self, MockIngestionJob):
        """background_tasks.add_task is called exactly once."""
        mock_job = MagicMock()
        mock_job.id = 99
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=99)
        bg = _make_mock_background_tasks()

        create_and_dispatch_job(db=db, background_tasks=bg, source="fred", config={})

        bg.add_task.assert_called_once()

    @patch("app.api.v1.jobs.run_ingestion_job", new_callable=MagicMock)
    @patch("app.core.job_helpers.IngestionJob")
    def test_background_task_called_with_correct_args(
        self, MockIngestionJob, mock_run_fn
    ):
        """add_task receives run_ingestion_job, job.id, source, and config."""
        mock_job = MagicMock()
        mock_job.id = 55
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=55)
        bg = _make_mock_background_tasks()
        cfg = {"dataset": "gdp", "year": 2024}

        create_and_dispatch_job(db=db, background_tasks=bg, source="bea", config=cfg)

        call_args = bg.add_task.call_args
        # First positional arg should be the run_ingestion_job function
        assert call_args[0][0] is mock_run_fn
        # Remaining positional args: job_id, source, config
        assert call_args[0][1] == 55
        assert call_args[0][2] == "bea"
        assert call_args[0][3] == cfg


# =============================================================================
# Error handling
# =============================================================================


class TestErrorHandling:
    """Tests for database and unexpected errors."""

    @patch("app.core.job_helpers.IngestionJob")
    def test_db_commit_error_raises_http_500(self, MockIngestionJob):
        """When db.commit() raises, an HTTPException with status 500 is raised."""
        mock_job = MagicMock()
        MockIngestionJob.return_value = mock_job

        db = MagicMock()
        db.commit.side_effect = Exception("connection refused")
        bg = _make_mock_background_tasks()

        with pytest.raises(HTTPException) as exc_info:
            create_and_dispatch_job(
                db=db, background_tasks=bg, source="fred", config={}
            )

        assert exc_info.value.status_code == 500
        assert "connection refused" in exc_info.value.detail

    @patch("app.core.job_helpers.IngestionJob")
    def test_db_add_error_raises_http_500(self, MockIngestionJob):
        """When db.add() raises, an HTTPException with status 500 is raised."""
        mock_job = MagicMock()
        MockIngestionJob.return_value = mock_job

        db = MagicMock()
        db.add.side_effect = RuntimeError("session is closed")
        bg = _make_mock_background_tasks()

        with pytest.raises(HTTPException) as exc_info:
            create_and_dispatch_job(
                db=db, background_tasks=bg, source="eia", config={}
            )

        assert exc_info.value.status_code == 500
        assert "session is closed" in exc_info.value.detail

    @patch("app.core.job_helpers.IngestionJob")
    def test_db_refresh_error_raises_http_500(self, MockIngestionJob):
        """When db.refresh() raises, an HTTPException with status 500 is raised."""
        mock_job = MagicMock()
        MockIngestionJob.return_value = mock_job

        db = MagicMock()
        db.refresh.side_effect = Exception("object is not persistent")
        bg = _make_mock_background_tasks()

        with pytest.raises(HTTPException) as exc_info:
            create_and_dispatch_job(
                db=db, background_tasks=bg, source="sec", config={}
            )

        assert exc_info.value.status_code == 500

    @patch("app.core.job_helpers.IngestionJob")
    def test_error_detail_contains_exception_message(self, MockIngestionJob):
        """The HTTPException detail includes the original exception text."""
        mock_job = MagicMock()
        MockIngestionJob.return_value = mock_job

        db = MagicMock()
        db.commit.side_effect = ValueError("unique constraint violated")
        bg = _make_mock_background_tasks()

        with pytest.raises(HTTPException) as exc_info:
            create_and_dispatch_job(
                db=db, background_tasks=bg, source="fred", config={}
            )

        assert exc_info.value.detail == "unique constraint violated"


# =============================================================================
# Response structure
# =============================================================================


class TestResponseStructure:
    """Tests that validate the shape and content of the returned dict."""

    @patch("app.core.job_helpers.IngestionJob")
    def test_response_has_exactly_four_keys(self, MockIngestionJob):
        """Returned dict has exactly job_id, status, message, check_status."""
        mock_job = MagicMock()
        mock_job.id = 1
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=1)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db, background_tasks=bg, source="fred", config={}
        )

        assert set(result.keys()) == {"job_id", "status", "message", "check_status"}

    @patch("app.core.job_helpers.IngestionJob")
    def test_status_is_always_pending_string(self, MockIngestionJob):
        """The status field is always the string pending."""
        mock_job = MagicMock()
        mock_job.id = 1
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=1)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db, background_tasks=bg, source="fred", config={}
        )

        assert result["status"] == "pending"

    @patch("app.core.job_helpers.IngestionJob")
    def test_check_status_url_contains_job_id(self, MockIngestionJob):
        """check_status URL uses the correct job_id path."""
        mock_job = MagicMock()
        mock_job.id = 123
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=123)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db, background_tasks=bg, source="treasury", config={}
        )

        assert result["check_status"] == "/api/v1/jobs/123"

    @patch("app.core.job_helpers.IngestionJob")
    def test_job_id_matches_db_assigned_id(self, MockIngestionJob):
        """job_id in the response matches the id assigned by the database."""
        mock_job = MagicMock()
        mock_job.id = 9999
        MockIngestionJob.return_value = mock_job

        db = _make_mock_db(job_id=9999)
        bg = _make_mock_background_tasks()

        result = create_and_dispatch_job(
            db=db, background_tasks=bg, source="bts", config={}
        )

        assert result["job_id"] == 9999
