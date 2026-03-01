"""
Deal modeling engine for PE roll-up analysis.

Provides configurable scenario modeling, sensitivity analysis, deployment planning,
and executive summary generation for MedSpa acquisitions.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, Integer, JSON, String, Text, text
from sqlalchemy.orm import Session

from app.core.models import Base

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Persisted scenario model
# ──────────────────────────────────────────────────────────────────────

class DealScenario(Base):
    __tablename__ = "deal_scenarios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    state_filter = Column(String(2), nullable=True)
    benchmarks = Column(JSON, nullable=True)
    assumptions = Column(JSON, nullable=True)
    results = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ──────────────────────────────────────────────────────────────────────
# Default benchmarks and assumptions
# ──────────────────────────────────────────────────────────────────────

# Source: AmSpa State of the Industry, IBISWorld, PE deal comps
MEDSPA_BENCHMARKS = {
    "$":    {"revenue": 400_000, "ebitda_margin": 0.12, "entry_multiple": 3.0},
    "$$":   {"revenue": 700_000, "ebitda_margin": 0.18, "entry_multiple": 3.5},
    "$$$":  {"revenue": 1_200_000, "ebitda_margin": 0.22, "entry_multiple": 4.0},
    "$$$$": {"revenue": 2_000_000, "ebitda_margin": 0.25, "entry_multiple": 4.5},
    None:   {"revenue": 600_000, "ebitda_margin": 0.15, "entry_multiple": 3.5},
}

DEFAULT_ASSUMPTIONS = {
    # Capital structure
    "debt_pct": 0.60,
    "equity_pct": 0.40,
    "transaction_cost_pct": 0.05,
    "working_capital_months": 3,
    # P&L
    "sga_pct": 0.32,
    "cogs_pct": 0.40,
    # Deployment
    "cohort_sizes": [15, 25, 20],
    "cohort_months": [0, 6, 12],
    "integration_cost_per_location": 50_000,
    "management_hires": [
        {"role": "CFO", "month": 1, "annual_cost": 250_000},
        {"role": "COO", "month": 3, "annual_cost": 200_000},
        {"role": "VP Ops", "month": 6, "annual_cost": 175_000},
    ],
    # Scenarios
    "scenarios": {
        "conservative": {"exit_multiple": 7, "margin_improvement": 0.03, "hold_years": 5, "organic_growth": 0.02},
        "base":         {"exit_multiple": 10, "margin_improvement": 0.05, "hold_years": 5, "organic_growth": 0.05},
        "aggressive":   {"exit_multiple": 12, "margin_improvement": 0.07, "hold_years": 4, "organic_growth": 0.08},
    },
}


class DealEngine:
    """Core deal modeling engine for MedSpa PE roll-up analysis."""

    def __init__(self, db: Session):
        self.db = db

    # ── Portfolio assembly ────────────────────────────────────────────

    def get_target_portfolio(
        self,
        state: Optional[str] = None,
        min_grade: str = "A",
        price_tiers: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Query medspa_prospects, return tier counts + geographic breakdown."""
        conditions = ["acquisition_grade = :grade"]
        params: Dict[str, Any] = {"grade": min_grade}

        if state:
            conditions.append("state = :state")
            params["state"] = state.upper()
        if price_tiers:
            placeholders = ", ".join(f":tier_{i}" for i in range(len(price_tiers)))
            conditions.append(f"price IN ({placeholders})")
            for i, t in enumerate(price_tiers):
                params[f"tier_{i}"] = t

        where = " AND ".join(conditions)

        result = self.db.execute(
            text(f"""
                SELECT price, COUNT(*) as cnt
                FROM medspa_prospects
                WHERE {where}
                GROUP BY price ORDER BY cnt DESC
            """),
            params,
        )
        tier_counts: Dict[Optional[str], int] = {}
        for row in result.fetchall():
            tier_counts[row[0]] = row[1]

        result = self.db.execute(
            text(f"""
                SELECT state, COUNT(*) as cnt
                FROM medspa_prospects
                WHERE {where} AND state IS NOT NULL
                GROUP BY state ORDER BY cnt DESC
            """),
            params,
        )
        a_grade_states = [{"state": row[0], "count": row[1]} for row in result.fetchall()]

        return {
            "tier_counts": tier_counts,
            "total_targets": sum(tier_counts.values()),
            "states_covered": len(a_grade_states),
            "a_grade_states": a_grade_states,
        }

    # ── Unit economics ────────────────────────────────────────────────

    def compute_tier_economics(
        self,
        tier_counts: Dict[Optional[str], int],
        benchmarks: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Per-tier revenue, EBITDA, acquisition cost with optional custom benchmarks."""
        bm = benchmarks or MEDSPA_BENCHMARKS
        default_bm = bm.get(None, MEDSPA_BENCHMARKS[None])

        tier_economics = []
        totals = {"locations": 0, "revenue": 0, "ebitda": 0, "acquisition_cost": 0}

        for tier, count in tier_counts.items():
            b = bm.get(tier, default_bm)
            rev = b["revenue"]
            margin = b["ebitda_margin"]
            ebitda = rev * margin
            multiple = b["entry_multiple"]
            acq_cost = ebitda * multiple

            tier_economics.append({
                "tier": tier or "Unknown",
                "count": count,
                "avg_revenue": rev,
                "ebitda_margin": margin,
                "avg_ebitda": ebitda,
                "entry_multiple": multiple,
                "total_revenue": rev * count,
                "total_ebitda": ebitda * count,
                "total_acq_cost": acq_cost * count,
            })
            totals["locations"] += count
            totals["revenue"] += rev * count
            totals["ebitda"] += ebitda * count
            totals["acquisition_cost"] += acq_cost * count

        weighted_margin = totals["ebitda"] / totals["revenue"] if totals["revenue"] > 0 else 0
        avg_multiple = totals["acquisition_cost"] / totals["ebitda"] if totals["ebitda"] > 0 else 0

        return {
            "tier_economics": tier_economics,
            "total_locations": totals["locations"],
            "total_revenue": totals["revenue"],
            "total_ebitda": totals["ebitda"],
            "total_acquisition_cost": totals["acquisition_cost"],
            "weighted_margin": weighted_margin,
            "avg_entry_multiple": avg_multiple,
        }

    # ── Capital stack ─────────────────────────────────────────────────

    def compute_capital_stack(
        self,
        total_acq_cost: float,
        total_ebitda: float = 0,
        total_revenue: float = 0,
        assumptions: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Debt/equity split, transaction costs, working capital, total capital required."""
        a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}

        debt = total_acq_cost * a["debt_pct"]
        equity = total_acq_cost * a["equity_pct"]
        transaction_costs = total_acq_cost * a["transaction_cost_pct"]
        monthly_sga = total_revenue * a["sga_pct"] / 12 if total_revenue > 0 else 0
        working_capital = monthly_sga * a["working_capital_months"]
        total_capital_required = equity + transaction_costs + working_capital

        return {
            "debt": debt,
            "equity": equity,
            "transaction_costs": transaction_costs,
            "working_capital": working_capital,
            "total_capital_required": total_capital_required,
            "leverage_ratio": debt / total_ebitda if total_ebitda > 0 else 0,
        }

    # ── Scenario returns ──────────────────────────────────────────────

    def run_scenario(
        self,
        economics: Dict[str, Any],
        scenario_config: Dict[str, Any],
        assumptions: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Single scenario: entry -> margin improvement + organic growth -> exit."""
        a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}

        total_ebitda = economics["total_ebitda"]
        total_acq_cost = economics["total_acquisition_cost"]
        total_revenue = economics["total_revenue"]

        exit_multiple = scenario_config["exit_multiple"]
        margin_improvement = scenario_config.get("margin_improvement", 0.05)
        hold_years = scenario_config.get("hold_years", 5)
        organic_growth = scenario_config.get("organic_growth", 0)

        # EBITDA compounds annually by margin improvement + organic growth
        annual_growth = margin_improvement + organic_growth
        exit_ebitda = total_ebitda * (1 + annual_growth) ** hold_years

        entry_ev = total_acq_cost
        exit_ev = exit_ebitda * exit_multiple

        gross_moic = exit_ev / entry_ev if entry_ev > 0 else 0
        # Net IRR approximation: (MOIC)^(1/years) - 1
        net_irr = (gross_moic ** (1.0 / hold_years) - 1) if gross_moic > 0 and hold_years > 0 else 0

        capital = self.compute_capital_stack(total_acq_cost, total_ebitda, total_revenue, a)
        equity_invested = capital["total_capital_required"]
        equity_returned = exit_ev - capital["debt"]

        return {
            "entry_ev": entry_ev,
            "exit_ev": exit_ev,
            "exit_multiple": exit_multiple,
            "hold_years": hold_years,
            "entry_ebitda": total_ebitda,
            "exit_ebitda": exit_ebitda,
            "improved_ebitda": exit_ebitda,  # backward-compat alias
            "margin_improvement": margin_improvement,
            "organic_growth": organic_growth,
            "gross_moic": round(gross_moic, 2),
            "net_irr": round(net_irr, 4),
            "equity_invested": round(equity_invested),
            "equity_returned": round(equity_returned),
            "total_capital_required": round(capital["total_capital_required"]),
        }

    # ── Multi-scenario ────────────────────────────────────────────────

    def run_scenarios(
        self,
        economics: Dict[str, Any],
        scenario_configs: Optional[Dict[str, Dict]] = None,
        assumptions: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Run conservative/base/aggressive (or custom named) scenarios."""
        a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}
        configs = scenario_configs or a["scenarios"]

        results = {}
        for name, config in configs.items():
            results[name] = self.run_scenario(economics, config, a)
        return results

    # ── Sensitivity grid ──────────────────────────────────────────────

    def sensitivity_analysis(
        self,
        economics: Dict[str, Any],
        base_assumptions: Optional[Dict],
        row_param: str,
        row_values: List[float],
        col_param: str,
        col_values: List[float],
        metric: str = "net_irr",
    ) -> Dict[str, Any]:
        """2-way sensitivity table. Varies two parameters, returns grid of metric values."""
        a = {**DEFAULT_ASSUMPTIONS, **(base_assumptions or {})}
        base_scenario = a["scenarios"].get("base", {
            "exit_multiple": 10, "margin_improvement": 0.05,
            "hold_years": 5, "organic_growth": 0.05,
        })

        grid = []
        highlight = None

        for ri, rv in enumerate(row_values):
            row = []
            for ci, cv in enumerate(col_values):
                config = {**base_scenario, row_param: rv, col_param: cv}
                result = self.run_scenario(economics, config, a)
                cell_value = result.get(metric, 0)
                row.append(round(cell_value, 4) if isinstance(cell_value, float) else cell_value)

                # Mark base case cell
                if rv == base_scenario.get(row_param) and cv == base_scenario.get(col_param):
                    highlight = {"row": ri, "col": ci}
            grid.append(row)

        return {
            "row_param": row_param,
            "row_values": row_values,
            "col_param": col_param,
            "col_values": col_values,
            "metric": metric,
            "grid": grid,
            "highlight": highlight,
        }

    # ── Deployment timeline ───────────────────────────────────────────

    def deployment_plan(
        self,
        economics: Dict[str, Any],
        cohorts: Optional[List[Dict]] = None,
        management_hires: Optional[List[Dict]] = None,
        integration_cost_per_location: float = 50_000,
        assumptions: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Model phased acquisition: cohort sizes, timing, cumulative capital, milestones."""
        a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}

        if cohorts is None:
            cohorts = [
                {"locations": s, "start_month": m}
                for s, m in zip(a["cohort_sizes"], a["cohort_months"])
            ]
        hires = management_hires or a["management_hires"]

        total_locations = economics["total_locations"]
        avg_acq_per_location = (
            economics["total_acquisition_cost"] / total_locations
            if total_locations > 0 else 500_000
        )

        cumulative_locations = 0
        cumulative_capital = 0
        cohort_results = []

        milestone_templates = [
            ["Close anchor locations", "Deploy shared POS system"],
            ["Consolidate SG&A functions", "Achieve initial margin improvements"],
            ["Reach platform scale", "Prepare for exit or recapitalization"],
        ]

        for i, cohort in enumerate(cohorts):
            loc = cohort["locations"]
            start = cohort["start_month"]
            acq_cost = avg_acq_per_location * loc
            int_cost = integration_cost_per_location * loc
            cumulative_locations += loc
            cumulative_capital += acq_cost + int_cost

            milestones = list(milestone_templates[i]) if i < len(milestone_templates) else []
            for hire in hires:
                if start <= hire["month"] < start + 6:
                    milestones.insert(1, f"Hire {hire['role']}")

            cohort_results.append({
                "phase": i + 1,
                "locations": loc,
                "start_month": start,
                "acquisition_cost": round(acq_cost),
                "integration_cost": round(int_cost),
                "cumulative_locations": cumulative_locations,
                "cumulative_capital": round(cumulative_capital),
                "milestones": milestones,
            })

        # Monthly cash flow (simplified: linear deployment within each cohort window)
        max_start = max((c["start_month"] for c in cohorts), default=0)
        total_months = max(max_start + 6, 24)
        monthly_cash_flow = []
        running_total = 0

        for month in range(total_months):
            month_spend = 0
            for cohort in cohorts:
                start = cohort["start_month"]
                window = 6  # months to deploy each cohort
                if start <= month < start + window:
                    loc_per_month = cohort["locations"] / window
                    month_spend += loc_per_month * (avg_acq_per_location + integration_cost_per_location)
            # Management costs
            for hire in hires:
                if month >= hire["month"]:
                    month_spend += hire["annual_cost"] / 12

            running_total += month_spend
            monthly_cash_flow.append(round(running_total))

        return {
            "cohorts": cohort_results,
            "total_locations": cumulative_locations,
            "total_deployment_months": max_start + 6 if cohorts else 0,
            "total_capital_deployed": round(cumulative_capital),
            "management_build": hires,
            "monthly_cash_flow": monthly_cash_flow,
        }

    # ── Executive narrative ───────────────────────────────────────────

    def executive_summary(
        self,
        portfolio: Dict[str, Any],
        economics: Dict[str, Any],
        scenario_results: Dict[str, Dict],
        deployment: Dict[str, Any],
        fund_size: Optional[float] = None,
        target_close: str = "Q3 2026",
        assumptions: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Generate structured narrative: thesis, the ask, timeline, returns, risks."""
        a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}

        base = scenario_results.get("base", {})
        total_equity = round(deployment.get("total_capital_deployed", 0) * a["equity_pct"])

        moic = base.get("gross_moic", 0)
        irr_pct = round(base.get("net_irr", 0) * 100, 1)

        headline = (
            f"Aesthetics Roll-Up: {economics['total_locations']} Locations, "
            f"${round(total_equity / 1_000_000)}M Equity, {moic}x MOIC"
        )

        thesis = (
            f"The U.S. medical aesthetics market represents a "
            f"${round(economics['total_revenue'] / 1_000_000_000, 1)}B+ addressable opportunity "
            f"with {portfolio['total_targets']:,}+ independently operated med-spas. "
            f"We have identified {portfolio['total_targets']:,} A-grade acquisition targets across "
            f"{portfolio['states_covered']} states at a blended entry multiple of "
            f"{economics['avg_entry_multiple']:.1f}x EBITDA. "
            f"Our thesis: acquire {deployment['total_locations']} locations over "
            f"{deployment['total_deployment_months']} months, consolidate SG&A and purchasing, "
            f"and exit at {base.get('exit_multiple', 10)}x for a {moic}x MOIC "
            f"({irr_pct}% IRR) on ${round(total_equity / 1_000_000)}M of invested equity."
        )

        the_ask = {
            "total_equity": total_equity,
            "deployment_months": deployment["total_deployment_months"],
            "target_close": target_close,
        }
        if fund_size and fund_size > 0:
            the_ask["fund_allocation_pct"] = round(total_equity / fund_size, 4)

        key_metrics = {
            "total_targets": portfolio["total_targets"],
            "a_grade_targets": portfolio["total_targets"],
            "states_covered": portfolio["states_covered"],
            "avg_entry_multiple": round(economics["avg_entry_multiple"], 1),
            "blended_ebitda_margin": round(economics["weighted_margin"], 4),
            "base_moic": moic,
            "base_irr": base.get("net_irr", 0),
        }

        value_creation_levers = [
            {"lever": "SG&A consolidation", "impact_bps": 200,
             "description": "Centralize accounting, HR, marketing across all locations"},
            {"lever": "Purchasing power", "impact_bps": 100,
             "description": "Group purchasing for supplies, equipment, and software licenses"},
            {"lever": "Pricing optimization", "impact_bps": 50,
             "description": "Premium positioning in affluent ZIP codes with data-driven pricing"},
            {"lever": "Revenue synergies", "impact_bps": 50,
             "description": "Cross-selling services, loyalty programs, and referral networks"},
        ]

        key_risks = [
            {"risk": "Integration execution", "severity": "High",
             "mitigation": "Hire COO month 1, pilot with 5 locations before scaling"},
            {"risk": "Labor supply", "severity": "Medium",
             "mitigation": "Competitive compensation, training pipeline, retention bonuses"},
            {"risk": "Multiple compression", "severity": "Medium",
             "mitigation": "Cash-on-cash positive by year 2, conservative leverage"},
            {"risk": "Economic downturn", "severity": "Low",
             "mitigation": "Essential services positioning, debt covenants at 15% cushion"},
        ]

        timeline = [
            {"month": "M0-M3", "action": "Close anchor locations, hire CFO + COO"},
            {"month": "M3-M6", "action": "Deploy shared systems, achieve 100bps margin improvement"},
            {"month": "M6-M12", "action": "Add tuck-in acquisitions, consolidate SG&A"},
            {"month": "M12-M18", "action": "Add remaining locations, reach platform scale"},
            {"month": "M18-M36", "action": "Optimize operations, prepare for exit or recapitalization"},
        ]

        return {
            "headline": headline,
            "thesis": thesis,
            "the_ask": the_ask,
            "key_metrics": key_metrics,
            "value_creation_levers": value_creation_levers,
            "key_risks": key_risks,
            "timeline": timeline,
        }

    # ── Full model run (convenience) ──────────────────────────────────

    def run_full_model(
        self,
        state: Optional[str] = None,
        min_grade: str = "A",
        benchmarks: Optional[Dict] = None,
        assumptions: Optional[Dict] = None,
        scenario_configs: Optional[Dict[str, Dict]] = None,
    ) -> Dict[str, Any]:
        """Run complete deal model: portfolio -> economics -> capital -> scenarios."""
        a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}

        portfolio = self.get_target_portfolio(state=state, min_grade=min_grade)
        economics = self.compute_tier_economics(portfolio["tier_counts"], benchmarks)
        capital = self.compute_capital_stack(
            economics["total_acquisition_cost"],
            economics["total_ebitda"],
            economics["total_revenue"],
            a,
        )
        configs = scenario_configs or a["scenarios"]
        scenarios = self.run_scenarios(economics, configs, a)

        # P&L waterfall per average location
        total_locs = economics["total_locations"]
        avg_rev = economics["total_revenue"] / total_locs if total_locs > 0 else 0
        pnl_waterfall = {
            "revenue": avg_rev,
            "cogs": avg_rev * a["cogs_pct"],
            "gross_profit": avg_rev * (1 - a["cogs_pct"]),
            "sga": avg_rev * a["sga_pct"],
            "ebitda": avg_rev * (1 - a["cogs_pct"]) - avg_rev * a["sga_pct"],
        }

        return {
            "portfolio": portfolio,
            "tier_economics": economics["tier_economics"],
            "total_locations": economics["total_locations"],
            "total_revenue": economics["total_revenue"],
            "total_ebitda": economics["total_ebitda"],
            "total_acquisition_cost": economics["total_acquisition_cost"],
            "weighted_margin": economics["weighted_margin"],
            "avg_entry_multiple": economics["avg_entry_multiple"],
            "capital_stack": capital,
            "leverage_ratio": capital["leverage_ratio"],
            "pnl_waterfall": pnl_waterfall,
            "scenarios": scenarios,
            "a_grade_states": portfolio["a_grade_states"],
        }
