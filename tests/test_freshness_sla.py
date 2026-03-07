"""
Tests for configurable freshness SLA (#4).

Covers:
- DB SLA overrides schedule-derived cadence
- CRUD operations (list, upsert, delete)
- Violation detection fires webhook
- No SLA = existing behavior (fallback to schedule cadence)
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, timedelta

from app.core.models import SourceFreshnessSLA


class TestSourceFreshnessSLAModel:
    """Test SourceFreshnessSLA model basics."""

    def test_model_repr(self):
        """Model repr includes source and max_age_hours."""
        sla = SourceFreshnessSLA(source="fred", max_age_hours=24.0)
        repr_str = repr(sla)
        assert "fred" in repr_str
        assert "24.0" in repr_str

    def test_default_alert_on_violation(self):
        """Default alert_on_violation is 1 (True) — column default applied by DB."""
        # Column defaults are applied by the DB, not Python constructors.
        # Verify the column definition has the right default.
        col = SourceFreshnessSLA.__table__.columns["alert_on_violation"]
        assert col.default.arg == 1


class TestFreshnessDashboardWithSLA:
    """Test that DB SLAs override schedule cadence in the dashboard."""

    def test_db_sla_overrides_schedule_cadence(self):
        """When a DB SLA exists, it takes priority over schedule-derived cadence."""
        from app.api.v1.freshness import get_freshness_dashboard, CADENCE_GRACE_HOURS
        from app.core.models import IngestionSchedule, ScheduleFrequency

        now = datetime.utcnow()
        # Source 'fred' had last success 30h ago
        last_success = now - timedelta(hours=30)

        db = MagicMock()

        # Mock the 3 queries in sequence
        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            mock_q = MagicMock()

            if call_count[0] == 1:
                # Last success query
                row = MagicMock()
                row.source = "fred"
                row.last_success = last_success
                mock_q.filter.return_value.group_by.return_value.all.return_value = [row]
            elif call_count[0] == 2:
                # Schedule query: fred is DAILY (grace = 36h, so 30h would be fresh)
                row = MagicMock()
                row.source = "fred"
                row.frequency = ScheduleFrequency.DAILY
                mock_q.filter.return_value.all.return_value = [row]
            elif call_count[0] == 3:
                # SLA query: fred has 24h SLA (so 30h is STALE)
                sla = MagicMock(spec=SourceFreshnessSLA)
                sla.source = "fred"
                sla.max_age_hours = 24.0
                mock_q.all.return_value = [sla]
            return mock_q

        db.query.side_effect = side_effect_query

        result = get_freshness_dashboard(db=db)

        assert result["total_sources"] == 1
        fred_entry = result["sources"][0]
        assert fred_entry["source"] == "fred"
        # Without SLA: schedule cadence = 36h, 30h < 36h → fresh
        # With SLA: max_age = 24h, 30h > 24h → stale
        assert fred_entry["is_stale"] is True
        assert fred_entry["expected_cadence_hours"] == 24.0
        assert fred_entry["sla_source"] == "db"

    def test_no_sla_falls_back_to_schedule(self):
        """Without DB SLA, schedule cadence is used."""
        from app.api.v1.freshness import get_freshness_dashboard
        from app.core.models import ScheduleFrequency

        now = datetime.utcnow()
        last_success = now - timedelta(hours=30)

        db = MagicMock()
        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            mock_q = MagicMock()

            if call_count[0] == 1:
                row = MagicMock()
                row.source = "fred"
                row.last_success = last_success
                mock_q.filter.return_value.group_by.return_value.all.return_value = [row]
            elif call_count[0] == 2:
                row = MagicMock()
                row.source = "fred"
                row.frequency = ScheduleFrequency.DAILY
                mock_q.filter.return_value.all.return_value = [row]
            elif call_count[0] == 3:
                # No SLAs
                mock_q.all.return_value = []
            return mock_q

        db.query.side_effect = side_effect_query

        result = get_freshness_dashboard(db=db)

        fred_entry = result["sources"][0]
        assert fred_entry["expected_cadence_hours"] == 36  # DAILY grace
        assert fred_entry["is_stale"] is False  # 30h < 36h
        assert fred_entry["sla_source"] == "schedule"


class TestFreshnessSLACRUD:
    """Test SLA CRUD endpoints."""

    def test_upsert_creates_new_sla(self):
        """PUT creates a new SLA when none exists."""
        from app.api.v1.freshness import upsert_freshness_sla, FreshnessSLARequest

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None

        # Mock db.add and db.refresh
        added_sla = None

        def capture_add(obj):
            nonlocal added_sla
            added_sla = obj

        db.add.side_effect = capture_add
        db.refresh.side_effect = lambda obj: None

        body = FreshnessSLARequest(max_age_hours=24.0, alert_on_violation=True, description="FRED SLA")

        result = upsert_freshness_sla("fred", body, db=db)

        assert db.add.called
        assert db.commit.called
        assert result["source"] == "fred"
        assert result["max_age_hours"] == 24.0

    def test_upsert_updates_existing_sla(self):
        """PUT updates an existing SLA."""
        from app.api.v1.freshness import upsert_freshness_sla, FreshnessSLARequest

        existing = MagicMock(spec=SourceFreshnessSLA)
        existing.source = "fred"
        existing.max_age_hours = 48.0

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = existing

        body = FreshnessSLARequest(max_age_hours=24.0, alert_on_violation=False)

        result = upsert_freshness_sla("fred", body, db=db)

        assert existing.max_age_hours == 24.0
        assert existing.alert_on_violation == 0
        assert db.commit.called

    def test_delete_existing_sla(self):
        """DELETE removes an existing SLA."""
        from app.api.v1.freshness import delete_freshness_sla

        existing = MagicMock(spec=SourceFreshnessSLA)
        existing.source = "fred"

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = existing

        result = delete_freshness_sla("fred", db=db)
        assert result["deleted"] == "fred"
        db.delete.assert_called_once_with(existing)
        assert db.commit.called

    def test_delete_nonexistent_sla_raises_404(self):
        """DELETE for nonexistent source raises HTTPException."""
        from app.api.v1.freshness import delete_freshness_sla
        from fastapi import HTTPException

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            delete_freshness_sla("nonexistent", db=db)
        assert exc_info.value.status_code == 404

    def test_list_slas(self):
        """GET returns all configured SLAs."""
        from app.api.v1.freshness import list_freshness_slas

        sla1 = MagicMock(spec=SourceFreshnessSLA)
        sla1.source = "fred"
        sla1.max_age_hours = 24.0
        sla1.alert_on_violation = 1
        sla1.description = "FRED SLA"
        sla1.created_at = datetime(2026, 1, 1)
        sla1.updated_at = datetime(2026, 1, 1)

        sla2 = MagicMock(spec=SourceFreshnessSLA)
        sla2.source = "eia"
        sla2.max_age_hours = 168.0
        sla2.alert_on_violation = 0
        sla2.description = None
        sla2.created_at = datetime(2026, 1, 2)
        sla2.updated_at = datetime(2026, 1, 2)

        db = MagicMock()
        db.query.return_value.order_by.return_value.all.return_value = [sla1, sla2]

        result = list_freshness_slas(db=db)
        assert len(result) == 2
        assert result[0]["source"] == "fred"
        assert result[1]["source"] == "eia"
        assert result[0]["alert_on_violation"] is True
        assert result[1]["alert_on_violation"] is False


@pytest.mark.asyncio
class TestFreshnessViolationChecker:
    """Test check_freshness_violations() function."""

    async def test_violation_fires_webhook(self):
        """Stale source with SLA fires ALERT_DATA_STALENESS webhook."""
        from app.api.v1.freshness import check_freshness_violations
        from app.core.models import WebhookEventType

        now = datetime.utcnow()

        sla = MagicMock(spec=SourceFreshnessSLA)
        sla.source = "fred"
        sla.max_age_hours = 24.0
        sla.alert_on_violation = 1

        db = MagicMock()

        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            mock_q = MagicMock()
            if call_count[0] == 1:
                # SLA query
                mock_q.filter.return_value.all.return_value = [sla]
            elif call_count[0] == 2:
                # Last success query
                row = MagicMock()
                row.source = "fred"
                row.last_success = now - timedelta(hours=48)
                mock_q.filter.return_value.group_by.return_value.all.return_value = [row]
            return mock_q

        db.query.side_effect = side_effect_query

        with patch(
            "app.core.webhook_service.trigger_webhooks", new_callable=AsyncMock
        ) as mock_trigger:
            mock_trigger.return_value = {"webhooks_triggered": 1}

            result = await check_freshness_violations(db)

            assert result["violations"] == 1
            assert result["details"][0]["source"] == "fred"
            mock_trigger.assert_called_once()
            call_kwargs = mock_trigger.call_args.kwargs
            assert call_kwargs["event_type"] == WebhookEventType.ALERT_DATA_STALENESS

    async def test_no_violations_when_fresh(self):
        """Fresh source does not trigger webhook."""
        from app.api.v1.freshness import check_freshness_violations

        now = datetime.utcnow()

        sla = MagicMock(spec=SourceFreshnessSLA)
        sla.source = "fred"
        sla.max_age_hours = 48.0
        sla.alert_on_violation = 1

        db = MagicMock()
        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            mock_q = MagicMock()
            if call_count[0] == 1:
                mock_q.filter.return_value.all.return_value = [sla]
            elif call_count[0] == 2:
                row = MagicMock()
                row.source = "fred"
                row.last_success = now - timedelta(hours=12)  # Fresh
                mock_q.filter.return_value.group_by.return_value.all.return_value = [row]
            return mock_q

        db.query.side_effect = side_effect_query

        with patch(
            "app.core.webhook_service.trigger_webhooks", new_callable=AsyncMock
        ) as mock_trigger:
            result = await check_freshness_violations(db)

            assert result["violations"] == 0
            mock_trigger.assert_not_called()

    async def test_no_slas_returns_zero_violations(self):
        """No configured SLAs = no violations to check."""
        from app.api.v1.freshness import check_freshness_violations

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        result = await check_freshness_violations(db)
        assert result["violations"] == 0
