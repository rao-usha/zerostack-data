"""
Tests for SPEC 019 — PE Portfolio Monitor.
Detects changes in exit readiness, financials, and leadership.
"""
import pytest
from datetime import date


class TestExitReadinessMonitor:
    """Tests for exit readiness change detection."""

    def test_detect_exit_grade_change(self):
        """T1: Grade boundary crossing detected (e.g. B→A)."""
        from app.core.pe_portfolio_monitor import _detect_exit_changes

        current = {"company_id": 1, "company_name": "TestCo", "exit_score": 82.0, "exit_grade": "A"}
        snapshot = {"exit_score": 64.0, "exit_grade": "B"}
        alerts = _detect_exit_changes(current, snapshot)
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "PE_EXIT_READINESS_CHANGE"
        assert "B" in alerts[0]["title"] and "A" in alerts[0]["title"]

    def test_no_change_no_alert(self):
        """T2: Same grade → no alert fired."""
        from app.core.pe_portfolio_monitor import _detect_exit_changes

        current = {"company_id": 1, "company_name": "TestCo", "exit_score": 72.0, "exit_grade": "B"}
        snapshot = {"exit_score": 68.0, "exit_grade": "B"}
        alerts = _detect_exit_changes(current, snapshot)
        assert len(alerts) == 0


class TestFinancialMonitor:
    """Tests for financial change detection."""

    def test_detect_revenue_decline(self):
        """T3: >10% revenue decline flagged."""
        from app.core.pe_portfolio_monitor import _detect_financial_changes

        current = {"company_id": 1, "company_name": "TestCo",
                    "revenue": 9_000_000, "ebitda_margin": 20.0}
        snapshot = {"revenue": 10_500_000, "ebitda_margin": 22.0}
        alerts = _detect_financial_changes(current, snapshot)
        assert len(alerts) >= 1
        assert any(a["alert_type"] == "PE_FINANCIAL_ALERT" for a in alerts)
        assert any("revenue" in a["title"].lower() for a in alerts)

    def test_detect_margin_compression(self):
        """T4: EBITDA margin drop >5pp flagged."""
        from app.core.pe_portfolio_monitor import _detect_financial_changes

        current = {"company_id": 1, "company_name": "TestCo",
                    "revenue": 10_000_000, "ebitda_margin": 12.0}
        snapshot = {"revenue": 10_000_000, "ebitda_margin": 20.0}
        alerts = _detect_financial_changes(current, snapshot)
        assert len(alerts) >= 1
        assert any("margin" in a["title"].lower() for a in alerts)


class TestLeadershipMonitor:
    """Tests for leadership change detection."""

    def test_detect_leadership_departure(self):
        """T5: CEO/CFO departure detected."""
        from app.core.pe_portfolio_monitor import _detect_leadership_changes

        current = {"company_id": 1, "company_name": "TestCo",
                    "leaders": [{"name": "Jane CFO", "is_ceo": False, "is_cfo": True}]}
        snapshot = {"leaders": [
            {"name": "John CEO", "is_ceo": True, "is_cfo": False},
            {"name": "Jane CFO", "is_ceo": False, "is_cfo": True},
        ]}
        alerts = _detect_leadership_changes(current, snapshot)
        assert len(alerts) >= 1
        assert any("departure" in a["title"].lower() or "left" in a["title"].lower() for a in alerts)

    def test_detect_leadership_addition(self):
        """T6: New C-suite member detected."""
        from app.core.pe_portfolio_monitor import _detect_leadership_changes

        current = {"company_id": 1, "company_name": "TestCo",
                    "leaders": [
                        {"name": "John CEO", "is_ceo": True, "is_cfo": False},
                        {"name": "Jane CFO", "is_ceo": False, "is_cfo": True},
                        {"name": "Bob COO", "is_ceo": False, "is_cfo": False},
                    ]}
        snapshot = {"leaders": [
            {"name": "John CEO", "is_ceo": True, "is_cfo": False},
            {"name": "Jane CFO", "is_ceo": False, "is_cfo": True},
        ]}
        alerts = _detect_leadership_changes(current, snapshot)
        assert len(alerts) >= 1
        assert any("addition" in a["title"].lower() or "joined" in a["title"].lower() for a in alerts)


class TestHealthReport:
    """Tests for portfolio health report."""

    def test_health_report_structure(self):
        """T7: Report has all required fields."""
        from app.core.pe_portfolio_monitor import PortfolioHealthReport

        report = PortfolioHealthReport(
            firm_id=1, firm_name="TestFirm",
            check_date=date.today().isoformat(),
            companies_checked=5, alerts_generated=2,
            company_statuses=[], alerts=[],
        )
        assert report.firm_id == 1
        assert report.companies_checked == 5
        assert report.alerts_generated == 2

    def test_no_snapshot_baseline(self):
        """T8: First run with no prior snapshot → no alerts, creates baseline."""
        from app.core.pe_portfolio_monitor import _detect_exit_changes

        current = {"company_id": 1, "company_name": "TestCo", "exit_score": 72.0, "exit_grade": "B"}
        snapshot = None  # no prior snapshot
        alerts = _detect_exit_changes(current, snapshot)
        assert len(alerts) == 0
