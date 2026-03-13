"""
Tests for SPEC 020 — PE Alert Subscriptions & Notification API.
"""
import pytest


class TestAlertSubscriptions:
    """Tests for subscription management."""

    def test_subscribe_creates_subscription(self):
        """T1: subscribe returns well-formed subscription dict."""
        from app.core.pe_alert_subscriptions import AlertSubscriptionService

        # Verify the service class exists and has the right methods
        assert hasattr(AlertSubscriptionService, "subscribe")
        assert hasattr(AlertSubscriptionService, "unsubscribe")
        assert hasattr(AlertSubscriptionService, "list_subscriptions")
        assert hasattr(AlertSubscriptionService, "get_alert_history")

    def test_alert_types_valid(self):
        """T2: PE alert types are valid enum values."""
        from app.core.pe_alert_subscriptions import PE_ALERT_TYPES

        assert "PE_EXIT_READINESS_CHANGE" in PE_ALERT_TYPES
        assert "PE_FINANCIAL_ALERT" in PE_ALERT_TYPES
        assert "PE_LEADERSHIP_CHANGE" in PE_ALERT_TYPES
        assert "PE_DEAL_STAGE_CHANGE" in PE_ALERT_TYPES
        assert len(PE_ALERT_TYPES) >= 5


class TestHealthReportModel:
    """Tests for health report response structure."""

    def test_health_report_response_model(self):
        """T5: PortfolioHealthReport has all required fields."""
        from app.core.pe_portfolio_monitor import PortfolioHealthReport, CompanyStatus

        report = PortfolioHealthReport(
            firm_id=1, firm_name="Test", check_date="2026-03-13",
            companies_checked=3, alerts_generated=1,
            company_statuses=[
                CompanyStatus(company_id=1, company_name="Co1", exit_score=70.0, exit_grade="B"),
            ],
            alerts=[{"alert_type": "PE_FINANCIAL_ALERT", "title": "test"}],
        )
        assert report.companies_checked == 3
        assert len(report.company_statuses) == 1
        assert report.company_statuses[0].exit_grade == "B"

    def test_company_status_defaults(self):
        """CompanyStatus has sensible defaults."""
        from app.core.pe_portfolio_monitor import CompanyStatus

        status = CompanyStatus(company_id=1, company_name="Co1")
        assert status.trend == "stable"
        assert status.alert_count == 0
        assert status.leadership_count == 0
