"""
PE Financial Benchmarking Engine.

Compares portfolio company financial metrics against industry peers,
portfolio averages, and top-quartile thresholds.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyFinancials,
    PEFund,
    PEFundInvestment,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)


@dataclass
class MetricBenchmark:
    """Benchmark result for a single metric."""
    metric: str
    label: str
    value: Optional[float]
    industry_median: Optional[float]
    portfolio_avg: Optional[float]
    top_quartile: Optional[float]
    bottom_quartile: Optional[float]
    percentile: Optional[int]  # 0-100
    trend: Optional[str]  # "improving", "declining", "stable"
    peer_count: int = 0


@dataclass
class CompanyBenchmarkResult:
    """Full benchmark result for a company."""
    company_id: int
    company_name: str
    industry: Optional[str]
    fiscal_year: int
    metrics: List[MetricBenchmark] = field(default_factory=list)
    overall_percentile: Optional[int] = None
    data_quality: str = "high"  # high, medium, low


@dataclass
class PortfolioHeatmapRow:
    """One row of the portfolio heatmap."""
    company_id: int
    company_name: str
    industry: Optional[str]
    status: str
    metrics: Dict[str, Optional[int]]  # metric_name -> percentile


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _percentile_rank(value: float, distribution: List[float]) -> int:
    """Compute percentile rank of value within distribution (0-100)."""
    if not distribution:
        return 50
    below = sum(1 for v in distribution if v < value)
    equal = sum(1 for v in distribution if v == value)
    return round((below + 0.5 * equal) / len(distribution) * 100)


def _compute_trend(values: List[Optional[float]]) -> Optional[str]:
    """Determine trend from a time series (most recent 3 values)."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    recent = clean[-2:]
    diff = recent[-1] - recent[-2]
    if abs(diff) < 1.0:
        return "stable"
    return "improving" if diff > 0 else "declining"


def _safe_float(val) -> Optional[float]:
    """Convert Decimal/None to float."""
    if val is None:
        return None
    return float(val)


BENCHMARK_METRICS = [
    ("revenue_growth_pct", "Revenue Growth %"),
    ("ebitda_margin_pct", "EBITDA Margin %"),
    ("gross_margin_pct", "Gross Margin %"),
    ("debt_to_ebitda", "Debt / EBITDA"),
    ("free_cash_flow_usd", "Free Cash Flow ($)"),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def benchmark_company(
    db: Session,
    company_id: int,
    fiscal_year: Optional[int] = None,
) -> Optional[CompanyBenchmarkResult]:
    """
    Benchmark a single company against industry peers.

    Args:
        db: Database session
        company_id: Portfolio company ID
        fiscal_year: Year to benchmark (defaults to most recent)

    Returns:
        CompanyBenchmarkResult or None if company not found
    """
    # Look up the company
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        return None

    # Find the target year
    if fiscal_year is None:
        max_yr = db.execute(
            select(func.max(PECompanyFinancials.fiscal_year)).where(
                PECompanyFinancials.company_id == company_id,
                PECompanyFinancials.fiscal_period == "FY",
            )
        ).scalar_one_or_none()
        if not max_yr:
            return CompanyBenchmarkResult(
                company_id=company_id,
                company_name=company.name,
                industry=company.industry,
                fiscal_year=0,
                metrics=[],
                data_quality="low",
            )
        fiscal_year = max_yr

    # Get company's financials for the target year
    target_fin = db.execute(
        select(PECompanyFinancials).where(
            PECompanyFinancials.company_id == company_id,
            PECompanyFinancials.fiscal_year == fiscal_year,
            PECompanyFinancials.fiscal_period == "FY",
        )
    ).scalar_one_or_none()

    if not target_fin:
        return CompanyBenchmarkResult(
            company_id=company_id,
            company_name=company.name,
            industry=company.industry,
            fiscal_year=fiscal_year,
            metrics=[],
            data_quality="low",
        )

    # Get all peer financials for the same year in the same industry
    peer_query = (
        select(PECompanyFinancials)
        .join(PEPortfolioCompany, PEPortfolioCompany.id == PECompanyFinancials.company_id)
        .where(
            PECompanyFinancials.fiscal_year == fiscal_year,
            PECompanyFinancials.fiscal_period == "FY",
            PECompanyFinancials.company_id != company_id,
        )
    )
    if company.industry:
        peer_query = peer_query.where(PEPortfolioCompany.industry == company.industry)

    peer_rows = db.execute(peer_query).scalars().all()

    # Get the company's own PE owner for portfolio avg
    portfolio_company_ids = _get_portfolio_company_ids(db, company_id)

    portfolio_query = (
        select(PECompanyFinancials).where(
            PECompanyFinancials.fiscal_year == fiscal_year,
            PECompanyFinancials.fiscal_period == "FY",
            PECompanyFinancials.company_id.in_(portfolio_company_ids),
            PECompanyFinancials.company_id != company_id,
        )
    )
    portfolio_rows = db.execute(portfolio_query).scalars().all()

    # Get historical data for trend
    history = db.execute(
        select(PECompanyFinancials).where(
            PECompanyFinancials.company_id == company_id,
            PECompanyFinancials.fiscal_period == "FY",
        ).order_by(PECompanyFinancials.fiscal_year)
    ).scalars().all()

    # Compute revenue per employee
    rev_per_emp = None
    if target_fin.revenue_usd and company.employee_count and company.employee_count > 0:
        rev_per_emp = float(target_fin.revenue_usd) / company.employee_count

    # Build metric benchmarks
    metrics = []
    percentiles = []

    for metric_col, label in BENCHMARK_METRICS:
        target_val = _safe_float(getattr(target_fin, metric_col, None))
        peer_vals = [_safe_float(getattr(p, metric_col)) for p in peer_rows]
        peer_vals = [v for v in peer_vals if v is not None]

        portfolio_vals = [_safe_float(getattr(p, metric_col)) for p in portfolio_rows]
        portfolio_vals = [v for v in portfolio_vals if v is not None]

        history_vals = [_safe_float(getattr(h, metric_col)) for h in history]

        # For debt_to_ebitda, lower is better — invert for percentile
        is_lower_better = metric_col == "debt_to_ebitda"

        pct = None
        if target_val is not None and peer_vals:
            if is_lower_better:
                pct = 100 - _percentile_rank(target_val, peer_vals)
            else:
                pct = _percentile_rank(target_val, peer_vals)
            percentiles.append(pct)

        industry_med = _median(peer_vals) if peer_vals else None
        port_avg = _mean(portfolio_vals) if portfolio_vals else None
        top_q = _quantile(peer_vals, 0.75) if peer_vals else None
        bot_q = _quantile(peer_vals, 0.25) if peer_vals else None

        metrics.append(MetricBenchmark(
            metric=metric_col,
            label=label,
            value=target_val,
            industry_median=industry_med,
            portfolio_avg=port_avg,
            top_quartile=top_q,
            bottom_quartile=bot_q,
            percentile=pct,
            trend=_compute_trend(history_vals),
            peer_count=len(peer_vals),
        ))

    # Add revenue per employee as a special metric
    peer_rev_emp = []
    for p in peer_rows:
        co = db.execute(
            select(PEPortfolioCompany).where(PEPortfolioCompany.id == p.company_id)
        ).scalar_one_or_none()
        if co and co.employee_count and co.employee_count > 0 and p.revenue_usd:
            peer_rev_emp.append(float(p.revenue_usd) / co.employee_count)

    rev_emp_pct = None
    if rev_per_emp is not None and peer_rev_emp:
        rev_emp_pct = _percentile_rank(rev_per_emp, peer_rev_emp)
        percentiles.append(rev_emp_pct)

    metrics.append(MetricBenchmark(
        metric="revenue_per_employee",
        label="Revenue per Employee ($)",
        value=rev_per_emp,
        industry_median=_median(peer_rev_emp) if peer_rev_emp else None,
        portfolio_avg=None,
        top_quartile=_quantile(peer_rev_emp, 0.75) if peer_rev_emp else None,
        bottom_quartile=_quantile(peer_rev_emp, 0.25) if peer_rev_emp else None,
        percentile=rev_emp_pct,
        trend=None,
        peer_count=len(peer_rev_emp),
    ))

    overall = round(_mean(percentiles)) if percentiles else None

    return CompanyBenchmarkResult(
        company_id=company_id,
        company_name=company.name,
        industry=company.industry,
        fiscal_year=fiscal_year,
        metrics=metrics,
        overall_percentile=overall,
        data_quality="high" if len(peer_rows) >= 5 else "medium" if peer_rows else "low",
    )


def benchmark_portfolio(
    db: Session,
    firm_id: int,
    fiscal_year: Optional[int] = None,
) -> List[PortfolioHeatmapRow]:
    """
    Generate portfolio heatmap — percentile rank per metric for each company.

    Args:
        db: Database session
        firm_id: PE firm ID
        fiscal_year: Year to benchmark (defaults to most recent available)

    Returns:
        List of PortfolioHeatmapRow, one per portfolio company
    """
    # Get all companies owned by this firm (via fund investments)
    company_ids = db.execute(
        select(PEFundInvestment.company_id).distinct()
        .join(PEFund, PEFund.id == PEFundInvestment.fund_id)
        .where(
            PEFund.firm_id == firm_id,
            PEFundInvestment.status == "Active",
        )
    ).scalars().all()

    if not company_ids:
        return []

    rows = []
    for cid in company_ids:
        result = benchmark_company(db, cid, fiscal_year)
        if not result:
            continue
        metrics_map = {}
        for m in result.metrics:
            metrics_map[m.metric] = m.percentile
        company = db.execute(
            select(PEPortfolioCompany).where(PEPortfolioCompany.id == cid)
        ).scalar_one_or_none()
        if not company:
            continue
        rows.append(PortfolioHeatmapRow(
            company_id=cid,
            company_name=company.name,
            industry=company.industry,
            status=company.status or "Active",
            metrics=metrics_map,
        ))

    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_portfolio_company_ids(db: Session, company_id: int) -> List[int]:
    """Get all company IDs in the same PE owner's portfolio."""
    # Find which firm owns this company
    firm_id = db.execute(
        select(PEFund.firm_id)
        .join(PEFundInvestment, PEFundInvestment.fund_id == PEFund.id)
        .where(
            PEFundInvestment.company_id == company_id,
            PEFundInvestment.status == "Active",
        )
        .limit(1)
    ).scalar_one_or_none()

    if not firm_id:
        return []

    return list(db.execute(
        select(PEFundInvestment.company_id).distinct()
        .join(PEFund, PEFund.id == PEFundInvestment.fund_id)
        .where(PEFund.firm_id == firm_id)
    ).scalars().all())


def _median(values: List[float]) -> Optional[float]:
    """Compute median of a list."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2
    return s[mid]


def _mean(values: List[float]) -> Optional[float]:
    """Compute mean of a list."""
    if not values:
        return None
    return sum(values) / len(values)


def _quantile(values: List[float], q: float) -> Optional[float]:
    """Compute quantile (0-1) using linear interpolation."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return s[lo] + frac * (s[hi] - s[lo])
