"""
PE Fund Performance Engine.

Calculates IRR, MOIC, TVPI, DPI, RVPI from actual cash flow data
using Newton-Raphson method (no numpy dependency).
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.core.pe_models import PECashFlow, PEFund, PEFundPerformance

logger = logging.getLogger(__name__)


class FundPerformanceService:
    """Calculate fund performance metrics from cash flows."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def calculate_fund_returns(
        self, fund_id: int, as_of_date: Optional[date] = None, nav: float = 0.0,
    ) -> Dict[str, Any]:
        """Calculate all fund metrics from cash flows.

        Args:
            fund_id: Fund to calculate for.
            as_of_date: Cutoff date (default: today).
            nav: Net Asset Value of remaining portfolio (unrealized value).

        Returns:
            Dict with irr, moic, tvpi, dpi, rvpi and supporting data.
        """
        if as_of_date is None:
            as_of_date = date.today()

        fund = self.db.execute(
            select(PEFund).where(PEFund.id == fund_id)
        ).scalar_one_or_none()
        if not fund:
            return {"error": f"Fund {fund_id} not found"}

        flows = self._fetch_cash_flows(fund_id, as_of_date)
        if not flows:
            return {
                "fund_id": fund_id,
                "fund_name": fund.name,
                "as_of_date": as_of_date.isoformat(),
                "irr_pct": None,
                "moic": None,
                "tvpi": None,
                "dpi": None,
                "rvpi": None,
                "called_capital": 0,
                "distributed": 0,
                "nav": nav,
                "cash_flow_count": 0,
            }

        called = sum(abs(f.amount) for f in flows if float(f.amount) < 0)
        distributed = sum(float(f.amount) for f in flows if float(f.amount) > 0)

        # IRR from cash flows (include NAV as terminal value)
        irr_flows = [(f.flow_date, float(f.amount)) for f in flows]
        if nav > 0:
            irr_flows.append((as_of_date, nav))
        irr = self._calculate_irr(irr_flows)

        moic = self._calculate_moic(float(called), distributed)
        tvpi = self._calculate_tvpi(distributed, nav, float(called))
        dpi = self._calculate_dpi(distributed, float(called))
        rvpi = self._calculate_rvpi(nav, float(called))

        return {
            "fund_id": fund_id,
            "fund_name": fund.name,
            "as_of_date": as_of_date.isoformat(),
            "irr_pct": round(irr * 100, 2) if irr is not None else None,
            "moic": round(moic, 3) if moic is not None else None,
            "tvpi": round(tvpi, 3) if tvpi is not None else None,
            "dpi": round(dpi, 3) if dpi is not None else None,
            "rvpi": round(rvpi, 3) if rvpi is not None else None,
            "called_capital": float(called),
            "distributed": distributed,
            "nav": nav,
            "cash_flow_count": len(flows),
        }

    def calculate_fund_timeseries(
        self, fund_id: int, nav: float = 0.0,
    ) -> Dict[str, Any]:
        """Calculate quarterly performance snapshots from inception to present."""
        fund = self.db.execute(
            select(PEFund).where(PEFund.id == fund_id)
        ).scalar_one_or_none()
        if not fund:
            return {"error": f"Fund {fund_id} not found"}

        flows = self._fetch_cash_flows(fund_id)
        if not flows:
            return {"fund_id": fund_id, "fund_name": fund.name, "timeseries": []}

        typed_flows = [
            (f.flow_date, float(f.amount), f.cash_flow_type) for f in flows
        ]
        timeseries = self._build_timeseries(typed_flows, nav)

        return {
            "fund_id": fund_id,
            "fund_name": fund.name,
            "timeseries": timeseries,
        }

    def get_cash_flows(self, fund_id: int) -> Dict[str, Any]:
        """Get raw cash flow ledger for a fund."""
        fund = self.db.execute(
            select(PEFund).where(PEFund.id == fund_id)
        ).scalar_one_or_none()
        if not fund:
            return {"error": f"Fund {fund_id} not found"}

        flows = self._fetch_cash_flows(fund_id)
        return {
            "fund_id": fund_id,
            "fund_name": fund.name,
            "cash_flows": [
                {
                    "id": f.id,
                    "date": f.flow_date.isoformat(),
                    "amount": float(f.amount),
                    "type": f.cash_flow_type,
                    "description": f.description,
                }
                for f in flows
            ],
        }

    def get_j_curve(self, fund_id: int, nav: float = 0.0) -> Dict[str, Any]:
        """J-curve visualization: cumulative NAV over time."""
        fund = self.db.execute(
            select(PEFund).where(PEFund.id == fund_id)
        ).scalar_one_or_none()
        if not fund:
            return {"error": f"Fund {fund_id} not found"}

        flows = self._fetch_cash_flows(fund_id)
        if not flows:
            return {"fund_id": fund_id, "fund_name": fund.name, "j_curve": []}

        # Cumulative net cash position over time
        points = []
        cumulative = 0.0
        for f in flows:
            cumulative += float(f.amount)
            points.append({
                "date": f.flow_date.isoformat(),
                "cumulative_net": round(cumulative, 2),
                "type": f.cash_flow_type,
            })

        # Add current NAV as final point
        if nav > 0:
            points.append({
                "date": date.today().isoformat(),
                "cumulative_net": round(cumulative + nav, 2),
                "type": "nav",
            })

        return {
            "fund_id": fund_id,
            "fund_name": fund.name,
            "j_curve": points,
        }

    # ------------------------------------------------------------------
    # Internal: data fetching
    # ------------------------------------------------------------------

    def _fetch_cash_flows(
        self, fund_id: int, as_of_date: Optional[date] = None,
    ) -> list:
        """Fetch cash flows for a fund, optionally filtered by date."""
        stmt = (
            select(PECashFlow)
            .where(PECashFlow.fund_id == fund_id)
            .order_by(PECashFlow.flow_date)
        )
        if as_of_date:
            stmt = stmt.where(PECashFlow.flow_date <= as_of_date)
        return list(self.db.execute(stmt).scalars().all())

    # ------------------------------------------------------------------
    # Static: pure computation (no DB, fully testable)
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_irr(
        cashflows: List[Tuple[date, float]],
        max_iterations: int = 200,
        tolerance: float = 1e-8,
    ) -> Optional[float]:
        """Calculate IRR using Newton-Raphson method.

        Args:
            cashflows: List of (date, amount) tuples. Negative = outflow.
            max_iterations: Max Newton-Raphson iterations.
            tolerance: Convergence threshold.

        Returns:
            Annual IRR as decimal (e.g., 0.15 = 15%), or None if can't compute.
        """
        if len(cashflows) < 2:
            return None

        # Need at least one positive and one negative flow
        has_positive = any(cf[1] > 0 for cf in cashflows)
        has_negative = any(cf[1] < 0 for cf in cashflows)
        if not has_positive or not has_negative:
            return None

        # Convert dates to year fractions from first date
        base_date = cashflows[0][0]
        year_fracs = []
        amounts = []
        for cf_date, amount in cashflows:
            days = (cf_date - base_date).days
            year_fracs.append(days / 365.25)
            amounts.append(amount)

        # Newton-Raphson: find r where NPV(r) = 0
        # NPV(r) = sum(CF_i / (1 + r)^t_i)
        # NPV'(r) = sum(-t_i * CF_i / (1 + r)^(t_i + 1))
        r = 0.1  # initial guess

        for _ in range(max_iterations):
            npv = 0.0
            dnpv = 0.0

            for i in range(len(amounts)):
                t = year_fracs[i]
                denom = (1.0 + r) ** t

                if abs(denom) < 1e-15:
                    break

                npv += amounts[i] / denom
                if t > 0:
                    dnpv -= t * amounts[i] / ((1.0 + r) ** (t + 1))

            if abs(npv) < tolerance:
                # Sanity check: IRR should be in reasonable range
                if -1.0 < r < 10.0:
                    return round(r, 6)
                return None

            if abs(dnpv) < 1e-15:
                # Derivative too small, try different starting point
                r = r + 0.05
                continue

            r_new = r - npv / dnpv

            # Clamp to prevent wild oscillation
            if r_new < -0.99:
                r_new = -0.99
            elif r_new > 10.0:
                r_new = 10.0

            r = r_new

        # Did not converge
        return None

    @staticmethod
    def _calculate_moic(
        total_invested: float, total_distributed: float,
    ) -> Optional[float]:
        """MOIC = total distributions / total invested."""
        if total_invested <= 0:
            return None
        return total_distributed / total_invested

    @staticmethod
    def _calculate_tvpi(
        total_distributed: float, nav: float, called_capital: float,
    ) -> Optional[float]:
        """TVPI = (distributions + NAV) / called capital."""
        if called_capital <= 0:
            return None
        return (total_distributed + nav) / called_capital

    @staticmethod
    def _calculate_dpi(
        total_distributed: float, called_capital: float,
    ) -> Optional[float]:
        """DPI = distributions / called capital."""
        if called_capital <= 0:
            return None
        return total_distributed / called_capital

    @staticmethod
    def _calculate_rvpi(
        nav: float, called_capital: float,
    ) -> Optional[float]:
        """RVPI = NAV / called capital."""
        if called_capital <= 0:
            return None
        return nav / called_capital

    @staticmethod
    def _build_timeseries(
        cashflows: List[Tuple[date, float, str]],
        nav: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Build quarterly performance snapshots from cash flows.

        Args:
            cashflows: List of (date, amount, type) tuples.
            nav: Current NAV for the most recent quarter.

        Returns:
            List of quarterly snapshots with cumulative metrics.
        """
        if not cashflows:
            return []

        # Sort by date
        sorted_flows = sorted(cashflows, key=lambda x: x[0])

        # Group flows by quarter
        quarters: Dict[str, List[Tuple[date, float, str]]] = {}
        for flow_date, amount, flow_type in sorted_flows:
            q = f"Q{(flow_date.month - 1) // 3 + 1} {flow_date.year}"
            if q not in quarters:
                quarters[q] = []
            quarters[q].append((flow_date, amount, flow_type))

        # Build cumulative snapshots
        cumulative_called = 0.0
        cumulative_distributed = 0.0
        snapshots = []

        quarter_keys = list(quarters.keys())
        for i, q in enumerate(quarter_keys):
            for flow_date, amount, flow_type in quarters[q]:
                if amount < 0:
                    cumulative_called += abs(amount)
                else:
                    cumulative_distributed += amount

            # NAV only applies to latest quarter
            q_nav = nav if i == len(quarter_keys) - 1 else 0.0

            tvpi = (
                round((cumulative_distributed + q_nav) / cumulative_called, 3)
                if cumulative_called > 0 else None
            )
            dpi = (
                round(cumulative_distributed / cumulative_called, 3)
                if cumulative_called > 0 else None
            )

            snapshots.append({
                "quarter": q,
                "called_capital": round(cumulative_called, 2),
                "distributed": round(cumulative_distributed, 2),
                "nav": round(q_nav, 2),
                "tvpi": tvpi,
                "dpi": dpi,
                "net_cash": round(cumulative_distributed - cumulative_called + q_nav, 2),
            })

        return snapshots
