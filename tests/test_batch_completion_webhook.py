"""
Tests for batch completion summary webhook (#3).

Covers:
- No webhook when jobs still pending
- Webhook sent when all jobs terminal
- Correct counts in payload
- Idempotent on double call
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from app.core.models import IngestionJob, JobStatus


@pytest.mark.asyncio
class TestCheckAndNotifyBatchCompletion:
    """Test check_and_notify_batch_completion()."""

    async def test_no_webhook_when_jobs_pending(self):
        """Should not fire webhook when some jobs are still pending/running."""
        db = MagicMock()
        # COUNT query returns 2 pending/running
        db.query.return_value.select_from.return_value.filter.return_value.scalar.return_value = 2

        from app.core.nightly_batch_service import check_and_notify_batch_completion

        with patch(
            "app.core.webhook_service.notify_batch_completed", new_callable=AsyncMock
        ) as mock_notify:
            await check_and_notify_batch_completion(db, "batch_123")
            mock_notify.assert_not_called()

    async def test_webhook_sent_when_all_terminal(self):
        """Should fire webhook when all jobs are SUCCESS/FAILED."""
        db = MagicMock()
        # COUNT query returns 0 (no pending/running)
        db.query.return_value.select_from.return_value.filter.return_value.scalar.return_value = 0

        mock_status = {
            "batch_run_id": "batch_done",
            "status": "partial_success",
            "total_jobs": 3,
            "successful_jobs": 2,
            "failed_jobs": 1,
            "elapsed_seconds": 120.5,
            "total_rows_inserted": 5000,
            "top_errors": [{"source": "fred", "error": "Timeout"}],
        }

        from app.core.nightly_batch_service import check_and_notify_batch_completion

        with patch(
            "app.core.nightly_batch_service.get_batch_run_status",
            return_value=mock_status,
        ), patch(
            "app.core.webhook_service.notify_batch_completed", new_callable=AsyncMock
        ) as mock_notify:
            await check_and_notify_batch_completion(db, "batch_done")

            mock_notify.assert_called_once_with(
                batch_run_id="batch_done",
                status="partial_success",
                total_jobs=3,
                successful_jobs=2,
                failed_jobs=1,
                elapsed_seconds=120.5,
                total_rows=5000,
                top_errors=[{"source": "fred", "error": "Timeout"}],
            )

    async def test_correct_counts_in_payload(self):
        """Verify the webhook payload has accurate job counts."""
        db = MagicMock()
        db.query.return_value.select_from.return_value.filter.return_value.scalar.return_value = 0

        mock_status = {
            "batch_run_id": "batch_all_ok",
            "status": "completed",
            "total_jobs": 5,
            "successful_jobs": 5,
            "failed_jobs": 0,
            "elapsed_seconds": 60.0,
            "total_rows_inserted": 10000,
            "top_errors": [],
        }

        from app.core.nightly_batch_service import check_and_notify_batch_completion

        with patch(
            "app.core.nightly_batch_service.get_batch_run_status",
            return_value=mock_status,
        ), patch(
            "app.core.webhook_service.notify_batch_completed", new_callable=AsyncMock
        ) as mock_notify:
            await check_and_notify_batch_completion(db, "batch_all_ok")

            call_kwargs = mock_notify.call_args.kwargs
            assert call_kwargs["total_jobs"] == 5
            assert call_kwargs["successful_jobs"] == 5
            assert call_kwargs["failed_jobs"] == 0
            assert call_kwargs["total_rows"] == 10000

    async def test_no_webhook_when_batch_not_found(self):
        """No crash when batch_run_id has no status data."""
        db = MagicMock()
        db.query.return_value.select_from.return_value.filter.return_value.scalar.return_value = 0

        from app.core.nightly_batch_service import check_and_notify_batch_completion

        with patch(
            "app.core.nightly_batch_service.get_batch_run_status",
            return_value=None,
        ), patch(
            "app.core.webhook_service.notify_batch_completed", new_callable=AsyncMock
        ) as mock_notify:
            await check_and_notify_batch_completion(db, "nonexistent")
            mock_notify.assert_not_called()

    async def test_idempotent_double_call(self):
        """Calling twice should fire webhook twice (idempotent — no crash)."""
        db = MagicMock()
        db.query.return_value.select_from.return_value.filter.return_value.scalar.return_value = 0

        mock_status = {
            "batch_run_id": "batch_idem",
            "status": "completed",
            "total_jobs": 1,
            "successful_jobs": 1,
            "failed_jobs": 0,
            "elapsed_seconds": 10.0,
            "total_rows_inserted": 100,
            "top_errors": [],
        }

        from app.core.nightly_batch_service import check_and_notify_batch_completion

        with patch(
            "app.core.nightly_batch_service.get_batch_run_status",
            return_value=mock_status,
        ), patch(
            "app.core.webhook_service.notify_batch_completed", new_callable=AsyncMock
        ) as mock_notify:
            await check_and_notify_batch_completion(db, "batch_idem")
            await check_and_notify_batch_completion(db, "batch_idem")
            assert mock_notify.call_count == 2


class TestNotifyBatchCompleted:
    """Test notify_batch_completed() convenience function."""

    @pytest.mark.asyncio
    async def test_error_summary_formatting(self):
        """Error summary truncates and joins top errors."""
        from app.core.webhook_service import notify_batch_completed

        with patch(
            "app.core.webhook_service.trigger_webhooks", new_callable=AsyncMock
        ) as mock_trigger:
            mock_trigger.return_value = {"webhooks_triggered": 0}

            await notify_batch_completed(
                batch_run_id="batch_err",
                status="partial_success",
                total_jobs=3,
                successful_jobs=1,
                failed_jobs=2,
                top_errors=[
                    {"source": "fred", "error": "Connection timeout"},
                    {"source": "eia", "error": "Rate limited"},
                ],
            )

            call_kwargs = mock_trigger.call_args.kwargs
            event_data = call_kwargs["event_data"]
            assert "fred: Connection timeout" in event_data["error_summary"]
            assert "eia: Rate limited" in event_data["error_summary"]

    @pytest.mark.asyncio
    async def test_no_errors_shows_none(self):
        """When no errors, error_summary is 'None'."""
        from app.core.webhook_service import notify_batch_completed

        with patch(
            "app.core.webhook_service.trigger_webhooks", new_callable=AsyncMock
        ) as mock_trigger:
            mock_trigger.return_value = {"webhooks_triggered": 0}

            await notify_batch_completed(
                batch_run_id="batch_ok",
                status="completed",
                total_jobs=2,
                successful_jobs=2,
                failed_jobs=0,
                top_errors=[],
            )

            event_data = mock_trigger.call_args.kwargs["event_data"]
            assert event_data["error_summary"] == "None"


class TestBatchCompletedEventType:
    """Test BATCH_COMPLETED enum value exists."""

    def test_batch_completed_in_webhook_event_type(self):
        """BATCH_COMPLETED is a valid WebhookEventType."""
        from app.core.models import WebhookEventType

        assert WebhookEventType.BATCH_COMPLETED.value == "batch_completed"

    def test_batch_completed_in_slack_emoji_map(self):
        """BATCH_COMPLETED has Slack emoji mapping."""
        from app.core.webhook_service import format_slack_payload
        from app.core.models import WebhookEventType

        payload = format_slack_payload(
            WebhookEventType.BATCH_COMPLETED,
            {"batch_run_id": "test", "status": "completed"},
        )
        assert "attachments" in payload
        assert payload["attachments"][0]["color"] == "#439FE0"

    def test_batch_completed_in_discord_color_map(self):
        """BATCH_COMPLETED has Discord color mapping."""
        from app.core.webhook_service import format_discord_payload
        from app.core.models import WebhookEventType

        payload = format_discord_payload(
            WebhookEventType.BATCH_COMPLETED,
            {"batch_run_id": "test", "status": "completed"},
        )
        assert "embeds" in payload
        assert payload["embeds"][0]["color"] == 0x0099FF
