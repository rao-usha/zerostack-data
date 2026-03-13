"""
Tests for SPEC 014 — PE Fund Performance Engine.
IRR, MOIC, TVPI, DPI, RVPI calculations from cash flow data.
"""
import pytest
from datetime import date
from decimal import Decimal


class TestIRRCalculation:
    """Tests for Newton-Raphson IRR computation."""

    def test_irr_simple_cashflows(self):
        """T1: IRR correct for known cash flow series."""
        from app.core.pe_fund_performance import FundPerformanceService

        # Classic example: invest 100, get 10/yr for 5 years + 100 back
        # Should produce ~10% IRR
        cashflows = [
            (date(2020, 1, 1), -100.0),
            (date(2021, 1, 1), 10.0),
            (date(2022, 1, 1), 10.0),
            (date(2023, 1, 1), 10.0),
            (date(2024, 1, 1), 10.0),
            (date(2025, 1, 1), 110.0),
        ]
        irr = FundPerformanceService._calculate_irr(cashflows)
        assert irr is not None
        assert abs(irr - 0.10) < 0.01  # ~10% IRR

    def test_irr_single_cashflow(self):
        """T2: Single cash flow returns None (can't compute IRR)."""
        from app.core.pe_fund_performance import FundPerformanceService

        cashflows = [(date(2020, 1, 1), -100.0)]
        irr = FundPerformanceService._calculate_irr(cashflows)
        assert irr is None

    def test_irr_no_convergence(self):
        """T3: Handles convergence failure gracefully (returns None)."""
        from app.core.pe_fund_performance import FundPerformanceService

        # All same sign — no solution
        cashflows = [
            (date(2020, 1, 1), -100.0),
            (date(2021, 1, 1), -50.0),
            (date(2022, 1, 1), -25.0),
        ]
        irr = FundPerformanceService._calculate_irr(cashflows)
        # Should return None or a negative number, not crash
        # All outflows with no inflows = no valid IRR
        assert irr is None or irr < 0

    def test_all_negative_flows(self):
        """T9: All outflows returns None for IRR."""
        from app.core.pe_fund_performance import FundPerformanceService

        cashflows = [
            (date(2020, 1, 1), -100.0),
            (date(2021, 1, 1), -200.0),
        ]
        irr = FundPerformanceService._calculate_irr(cashflows)
        assert irr is None


class TestFundMetrics:
    """Tests for MOIC, TVPI, DPI, RVPI."""

    def test_moic_calculation(self):
        """T4: MOIC = total distributions / total invested."""
        from app.core.pe_fund_performance import FundPerformanceService

        result = FundPerformanceService._calculate_moic(
            total_invested=100_000_000.0,
            total_distributed=250_000_000.0,
        )
        assert result == 2.5

    def test_tvpi_calculation(self):
        """T5: TVPI = (distributions + NAV) / called capital."""
        from app.core.pe_fund_performance import FundPerformanceService

        result = FundPerformanceService._calculate_tvpi(
            total_distributed=80_000_000.0,
            nav=120_000_000.0,
            called_capital=100_000_000.0,
        )
        assert result == 2.0

    def test_dpi_rvpi_calculation(self):
        """T6: DPI and RVPI computed correctly."""
        from app.core.pe_fund_performance import FundPerformanceService

        dpi = FundPerformanceService._calculate_dpi(
            total_distributed=60_000_000.0,
            called_capital=100_000_000.0,
        )
        assert dpi == 0.6

        rvpi = FundPerformanceService._calculate_rvpi(
            nav=140_000_000.0,
            called_capital=100_000_000.0,
        )
        assert rvpi == 1.4

    def test_zero_called_capital(self):
        """T7: Division by zero returns None."""
        from app.core.pe_fund_performance import FundPerformanceService

        assert FundPerformanceService._calculate_tvpi(50.0, 50.0, 0.0) is None
        assert FundPerformanceService._calculate_dpi(50.0, 0.0) is None
        assert FundPerformanceService._calculate_rvpi(50.0, 0.0) is None
        assert FundPerformanceService._calculate_moic(0.0, 50.0) is None


class TestTimeseries:
    """Tests for quarterly timeseries generation."""

    def test_quarterly_timeseries(self):
        """T8: Timeseries produces quarterly snapshots with metrics."""
        from app.core.pe_fund_performance import FundPerformanceService

        cashflows = [
            (date(2020, 1, 1), -100_000_000.0, "capital_call"),
            (date(2020, 7, 1), -50_000_000.0, "capital_call"),
            (date(2021, 1, 1), 20_000_000.0, "distribution"),
            (date(2021, 7, 1), 30_000_000.0, "distribution"),
            (date(2022, 1, 1), 40_000_000.0, "distribution"),
            (date(2022, 7, 1), 50_000_000.0, "distribution"),
            (date(2023, 1, 1), 80_000_000.0, "distribution"),
        ]
        nav = 60_000_000.0

        timeseries = FundPerformanceService._build_timeseries(cashflows, nav)
        assert len(timeseries) > 0
        # Each entry should have quarter, cumulative metrics
        first = timeseries[0]
        assert "quarter" in first
        assert "called_capital" in first
        assert "distributed" in first
        assert "tvpi" in first
        assert "dpi" in first
