"""
Tests for API key pre-flight check (#10) and expanded non-retryable errors (#2).

Covers:
- Pre-flight catches missing required API key
- Pre-flight passes when key is configured
- Pre-flight passes for non-required sources
- Retry service skips retry for missing-key errors
- Retry skipped for 403 / auth / schema / unknown source errors
- Consecutive identical failures stop retries
- Retryable errors (timeout, server error) still retry
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from app.api.v1.jobs import _check_api_key_preflight
from app.core.retry_service import (
    auto_schedule_retry,
    _is_non_retryable_error,
    NON_RETRYABLE_PATTERNS,
    CONSECUTIVE_IDENTICAL_THRESHOLD,
)
from app.core.models import IngestionJob, JobStatus


class TestCheckApiKeyPreflight:
    """Test _check_api_key_preflight helper."""

    def test_required_key_missing_returns_error(self):
        """Missing required API key returns error message."""
        with patch("app.core.config.get_settings") as mock_settings:
            mock_settings.return_value.get_api_key.side_effect = ValueError("missing")
            result = _check_api_key_preflight("eia")
            assert result is not None
            assert "API key required" in result
            assert "eia" in result

    def test_required_key_present_returns_none(self):
        """Configured required API key returns None (no error)."""
        with patch("app.core.config.get_settings") as mock_settings:
            mock_settings.return_value.get_api_key.return_value = "test-key-123"
            result = _check_api_key_preflight("eia")
            assert result is None

    def test_optional_source_returns_none(self):
        """Sources with OPTIONAL key requirement always pass."""
        result = _check_api_key_preflight("treasury")
        assert result is None

    def test_unknown_source_returns_none(self):
        """Sources not in API_REGISTRY pass (handled downstream)."""
        result = _check_api_key_preflight("unknown_source_xyz")
        assert result is None

    def test_dataset_suffix_stripped(self):
        """Source keys like 'job_postings:all' strip the dataset suffix."""
        result = _check_api_key_preflight("job_postings:all")
        assert result is None  # job_postings is OPTIONAL

    def test_recommended_source_returns_none(self):
        """Sources with RECOMMENDED key requirement pass (not REQUIRED)."""
        result = _check_api_key_preflight("fred")
        assert result is None


class TestRetrySkipForMissingKey:
    """Test that auto_schedule_retry skips retry for missing-key errors."""

    def test_retry_skipped_for_api_key_error(self):
        """Jobs that failed due to missing API key should not be retried."""
        job = MagicMock(spec=IngestionJob)
        job.id = 1
        job.status = JobStatus.FAILED
        job.retry_count = 0
        job.max_retries = 3
        job.error_message = "API key required for 'eia' but not configured"
        job.source = "eia"

        db = MagicMock()
        result = auto_schedule_retry(db, job)
        assert result is False

    @patch("app.core.retry_service.calculate_retry_delay")
    def test_retry_allowed_for_other_errors(self, mock_delay):
        """Jobs that failed due to other reasons should still be retried."""
        from datetime import timedelta
        mock_delay.return_value = timedelta(minutes=5)

        job = MagicMock(spec=IngestionJob)
        job.id = 2
        job.status = JobStatus.FAILED
        job.retry_count = 0
        job.max_retries = 3
        job.error_message = "Connection timeout"
        job.source = "eia"

        db = MagicMock()
        result = auto_schedule_retry(db, job)
        assert result is True

    def test_retry_skipped_for_exhausted_retries(self):
        """Jobs with exhausted retries should not be retried."""
        job = MagicMock(spec=IngestionJob)
        job.id = 3
        job.status = JobStatus.FAILED
        job.retry_count = 3
        job.max_retries = 3
        job.error_message = "Connection timeout"
        job.source = "fred"

        db = MagicMock()
        result = auto_schedule_retry(db, job)
        assert result is False


# =============================================================================
# Expanded non-retryable error detection (#2)
# =============================================================================


class TestIsNonRetryableError:
    """Test _is_non_retryable_error helper."""

    def test_none_error_is_retryable(self):
        assert _is_non_retryable_error(None) is False

    def test_empty_error_is_retryable(self):
        assert _is_non_retryable_error("") is False

    def test_api_key_required_is_non_retryable(self):
        assert _is_non_retryable_error("API key required for 'eia' but not configured") is True

    def test_access_forbidden_is_non_retryable(self):
        assert _is_non_retryable_error("Access forbidden: HTTP 403 from census.gov") is True

    def test_authentication_failed_is_non_retryable(self):
        assert _is_non_retryable_error("Authentication failed: invalid credentials") is True

    def test_invalid_request_params_is_non_retryable(self):
        assert _is_non_retryable_error("Invalid request parameters: missing 'dataset'") is True

    def test_missing_required_config_is_non_retryable(self):
        assert _is_non_retryable_error("Missing required config for census source") is True

    def test_unknown_source_is_non_retryable(self):
        assert _is_non_retryable_error("Unknown source: foobar_xyz") is True

    def test_failed_to_load_ingest_is_non_retryable(self):
        assert _is_non_retryable_error("Failed to load ingest function for census") is True

    def test_case_insensitive_matching(self):
        assert _is_non_retryable_error("ACCESS FORBIDDEN") is True
        assert _is_non_retryable_error("api key required") is True

    def test_timeout_is_retryable(self):
        assert _is_non_retryable_error("Connection timeout after 30s") is False

    def test_server_error_is_retryable(self):
        assert _is_non_retryable_error("HTTP 500: Internal Server Error") is False

    def test_rate_limit_is_retryable(self):
        assert _is_non_retryable_error("Rate limited: retry after 60s") is False


class TestExpandedNonRetryableRetryService:
    """Test auto_schedule_retry with expanded non-retryable patterns."""

    def _make_failed_job(self, error_message, job_id=10):
        job = MagicMock(spec=IngestionJob)
        job.id = job_id
        job.status = JobStatus.FAILED
        job.retry_count = 0
        job.max_retries = 3
        job.error_message = error_message
        job.source = "test_source"
        return job

    def test_403_error_not_retried(self):
        """Access forbidden errors should not be retried."""
        job = self._make_failed_job("Access forbidden: HTTP 403")
        db = MagicMock()
        assert auto_schedule_retry(db, job) is False

    def test_auth_failed_not_retried(self):
        """Authentication failed errors should not be retried."""
        job = self._make_failed_job("Authentication failed: bad token")
        db = MagicMock()
        assert auto_schedule_retry(db, job) is False

    def test_unknown_source_not_retried(self):
        """Unknown source errors should not be retried."""
        job = self._make_failed_job("Unknown source: not_a_real_source")
        db = MagicMock()
        assert auto_schedule_retry(db, job) is False

    def test_schema_error_not_retried(self):
        """Invalid request parameters should not be retried."""
        job = self._make_failed_job("Invalid request parameters: bad schema")
        db = MagicMock()
        assert auto_schedule_retry(db, job) is False

    @patch("app.core.retry_service.calculate_retry_delay")
    def test_timeout_error_is_retried(self, mock_delay):
        """Timeout errors should still be retried."""
        from datetime import timedelta
        mock_delay.return_value = timedelta(minutes=5)

        job = self._make_failed_job("Connection timeout after 30s")
        db = MagicMock()
        # Mock the consecutive failure query to return no recent fails
        db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []

        result = auto_schedule_retry(db, job)
        assert result is True

    @patch("app.core.retry_service.calculate_retry_delay")
    def test_server_error_is_retried(self, mock_delay):
        """Server errors should still be retried."""
        from datetime import timedelta
        mock_delay.return_value = timedelta(minutes=5)

        job = self._make_failed_job("HTTP 500: Internal Server Error")
        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []

        result = auto_schedule_retry(db, job)
        assert result is True


class TestConsecutiveIdenticalFailures:
    """Test consecutive identical failure detection in auto_schedule_retry."""

    def _make_failed_job(self, error_message, source="test_source"):
        job = MagicMock(spec=IngestionJob)
        job.id = 20
        job.status = JobStatus.FAILED
        job.retry_count = 0
        job.max_retries = 5
        job.error_message = error_message
        job.source = source
        return job

    def test_consecutive_identical_failures_stops_retry(self):
        """3+ consecutive identical failures should stop retry."""
        error_msg = "Connection refused: port 5432"
        job = self._make_failed_job(error_msg)

        db = MagicMock()
        # Return 3 recent fails with the same error
        fail_rows = [MagicMock(error_message=error_msg) for _ in range(3)]
        db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = fail_rows

        result = auto_schedule_retry(db, job)
        assert result is False

    @patch("app.core.retry_service.calculate_retry_delay")
    def test_mixed_errors_still_retried(self, mock_delay):
        """Different error messages across failures should still retry."""
        from datetime import timedelta
        mock_delay.return_value = timedelta(minutes=5)

        job = self._make_failed_job("Connection timeout")

        db = MagicMock()
        # Return 3 recent fails with DIFFERENT errors
        fail_rows = [
            MagicMock(error_message="Connection timeout"),
            MagicMock(error_message="HTTP 502: Bad Gateway"),
            MagicMock(error_message="Connection timeout"),
        ]
        db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = fail_rows

        result = auto_schedule_retry(db, job)
        assert result is True

    @patch("app.core.retry_service.calculate_retry_delay")
    def test_fewer_than_threshold_still_retried(self, mock_delay):
        """Fewer than CONSECUTIVE_IDENTICAL_THRESHOLD failures should still retry."""
        from datetime import timedelta
        mock_delay.return_value = timedelta(minutes=5)

        error_msg = "Connection refused"
        job = self._make_failed_job(error_msg)

        db = MagicMock()
        # Only 2 recent fails (threshold is 3)
        fail_rows = [MagicMock(error_message=error_msg) for _ in range(2)]
        db.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = fail_rows

        result = auto_schedule_retry(db, job)
        assert result is True
