"""
PE Portfolio Analytics — Firm-Level Performance & Risk.

Provides:
- Firm-wide performance aggregation (blended IRR, MOIC, TVPI, DPI, RVPI)
- Vintage cohort analysis
- Sector concentration & HHI risk scoring
- Composite risk dashboard
- Public Market Equivalent (PME) comparison
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyLeadership,
    PEFirm,
    PEFund,
    PEFundInvestment,
    PEFundPerformance,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Pure helper functions (tested directly)
# ============================================================================


def _weighted_avg(values: List[float], weights: List[float]) -> Optional[float]:
    """Compute weighted average. Returns None if total weight is zero."""
    total_weight = sum(weights)
    if total_weight == 0:
        return None
    return sum(v * w for v, w in zip(values, weights)) / total_weight


def _group_by_vintage(
    funds: List[Dict[str, Any]],
) -> Dict[int, Dict[str, Any]]:
    """Group fund records by vintage_year and compute cohort stats."""
    cohorts: Dict[int, Dict[str, Any]] = {}
    buckets: Dict[int, List[Dict]] = defaultdict(list)

    for f in funds:
        vy = f.get("vintage_year")
        if vy:
            buckets[vy].append(f)

    for year, group in sorted(buckets.items()):
        irrs = [f["irr"] for f in group if f.get("irr") is not None]
        moics = [f["moic"] for f in group if f.get("moic") is not None]
        committed = [f["committed"] for f in group if f.get("committed")]

        best = max(group, key=lambda x: x.get("irr") or -999)
        worst = min(group, key=lambda x: x.get("irr") or 999)

        cohorts[year] = {
            "vintage_year": year,
            "fund_count": len(group),
            "total_committed": sum(committed),
            "avg_irr": round(sum(irrs) / len(irrs), 2) if irrs else None,
            "avg_moic": round(sum(moics) / len(moics), 3) if moics else None,
            "best_fund": best.get("name"),
            "best_irr": best.get("irr"),
            "worst_fund": worst.get("name"),
            "worst_irr": worst.get("irr"),
        }

    return cohorts


def _calculate_hhi(shares_pct: List[float]) -> float:
    """Herfindahl-Hirschman Index from percentage shares.

    HHI = sum(s_i^2) where s_i is each sector's share as a percentage (0-100).
    Range: 0 (perfectly diversified) to 10000 (single sector).
    """
    return sum(s * s for s in shares_pct)


def _classify_hhi(hhi: float) -> str:
    """Classify HHI into risk categories."""
    if hhi < 1500:
        return "Diversified"
    elif hhi < 2500:
        return "Moderate"
    else:
        return "Concentrated"


def _calculate_pme_ratio(
    total_value: float,
    called_capital: float,
    hold_years: float,
    benchmark_annual_return: float,
) -> Optional[float]:
    """Simplified Kaplan-Schoar PME.

    PME = (distributions + NAV) / (called_capital × benchmark_growth)
    benchmark_growth = (1 + r)^years
    PME > 1.0 → PE outperformed public markets.
    """
    if called_capital <= 0 or hold_years <= 0:
        return None
    benchmark_growth = (1 + benchmark_annual_return) ** hold_years
    benchmark_value = called_capital * benchmark_growth
    if benchmark_value == 0:
        return None
    return round(total_value / benchmark_value, 3)


# ============================================================================
# Hardcoded benchmarks for demo
# ============================================================================

BENCHMARK_RETURNS = {
    "sp500": {"name": "S&P 500", "annual_return": 0.10},
    "russell2000": {"name": "Russell 2000", "annual_return": 0.08},
}

# Cambridge Associates PE median benchmarks by vintage
CA_PE_MEDIAN = {
    "irr": 14.0,
    "tvpi": 1.65,
    "dpi": 1.20,
}


# ============================================================================
# Service functions (DB-dependent)
# ============================================================================


def _get_firm_fund_data(
    db: Session, firm_id: int,
) -> Tuple[Optional[PEFirm], List[Dict[str, Any]]]:
    """Fetch firm and latest performance data for all its funds."""
    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        return None, []

    funds = db.execute(
        select(PEFund).where(PEFund.firm_id == firm_id)
    ).scalars().all()

    fund_data = []
    for fund in funds:
        # Get latest performance record
        perf = db.execute(
            select(PEFundPerformance)
            .where(PEFundPerformance.fund_id == fund.id)
            .order_by(PEFundPerformance.as_of_date.desc())
            .limit(1)
        ).scalar_one_or_none()

        committed = float(fund.final_close_usd_millions or fund.target_size_usd_millions or 0)

        # Get direct values from performance record
        called = float(perf.called_capital or 0) if perf else 0
        distributed = float(perf.distributed_capital or 0) if perf else 0
        nav = float(perf.remaining_value or 0) if perf else 0

        # Derive from ratios when direct values are missing but ratios exist
        if called == 0 and committed > 0 and perf:
            # Assume ~80% of committed has been called for active funds
            called = committed * 0.8
        if distributed == 0 and called > 0 and perf and perf.dpi:
            distributed = called * float(perf.dpi)
        if nav == 0 and called > 0 and perf and perf.rvpi:
            nav = called * float(perf.rvpi)

        fund_data.append({
            "fund_id": fund.id,
            "name": fund.name,
            "vintage_year": fund.vintage_year,
            "committed": committed,
            "irr": float(perf.net_irr_pct) if perf and perf.net_irr_pct else None,
            "tvpi": float(perf.tvpi) if perf and perf.tvpi else None,
            "dpi": float(perf.dpi) if perf and perf.dpi else None,
            "rvpi": float(perf.rvpi) if perf and perf.rvpi else None,
            "moic": float(perf.tvpi) if perf and perf.tvpi else None,  # TVPI ≈ MOIC
            "called": called,
            "distributed": distributed,
            "nav": nav,
            "status": fund.status,
        })

    return firm, fund_data


def calculate_firm_performance(db: Session, firm_id: int) -> Optional[Dict[str, Any]]:
    """Aggregate performance across all funds for a firm."""
    firm, fund_data = _get_firm_fund_data(db, firm_id)
    if not firm:
        return None

    if not fund_data:
        return {
            "firm_id": firm_id,
            "firm_name": firm.name,
            "fund_count": 0,
            "message": "No funds found",
        }

    # Weighted averages (weight = committed capital)
    irr_vals = [(f["irr"], f["committed"]) for f in fund_data if f["irr"] is not None and f["committed"] > 0]
    tvpi_vals = [(f["tvpi"], f["committed"]) for f in fund_data if f["tvpi"] is not None and f["committed"] > 0]
    dpi_vals = [(f["dpi"], f["committed"]) for f in fund_data if f["dpi"] is not None and f["committed"] > 0]
    rvpi_vals = [(f["rvpi"], f["committed"]) for f in fund_data if f["rvpi"] is not None and f["committed"] > 0]
    moic_vals = [(f["moic"], f["committed"]) for f in fund_data if f["moic"] is not None and f["committed"] > 0]

    blended_irr = _weighted_avg([v for v, _ in irr_vals], [w for _, w in irr_vals]) if irr_vals else None
    blended_tvpi = _weighted_avg([v for v, _ in tvpi_vals], [w for _, w in tvpi_vals]) if tvpi_vals else None
    blended_dpi = _weighted_avg([v for v, _ in dpi_vals], [w for _, w in dpi_vals]) if dpi_vals else None
    blended_rvpi = _weighted_avg([v for v, _ in rvpi_vals], [w for _, w in rvpi_vals]) if rvpi_vals else None
    blended_moic = _weighted_avg([v for v, _ in moic_vals], [w for _, w in moic_vals]) if moic_vals else None

    total_committed = sum(f["committed"] for f in fund_data)
    total_called = sum(f["called"] for f in fund_data)
    total_distributed = sum(f["distributed"] for f in fund_data)
    total_nav = sum(f["nav"] for f in fund_data)

    # AUM breakdown
    aum_breakdown = {
        "total_committed_usd_m": round(total_committed, 2),
        "total_called_usd_m": round(total_called, 2),
        "total_distributed_usd_m": round(total_distributed, 2),
        "total_nav_usd_m": round(total_nav, 2),
        "dry_powder_usd_m": round(total_committed - total_called, 2),
    }

    # Per-fund summary
    fund_summaries = [
        {
            "fund_id": f["fund_id"],
            "name": f["name"],
            "vintage_year": f["vintage_year"],
            "committed_usd_m": f["committed"],
            "irr_pct": round(f["irr"], 2) if f["irr"] is not None else None,
            "tvpi": round(f["tvpi"], 3) if f["tvpi"] is not None else None,
            "dpi": round(f["dpi"], 3) if f["dpi"] is not None else None,
            "status": f["status"],
        }
        for f in fund_data
    ]

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "fund_count": len(fund_data),
        "blended_irr_pct": round(blended_irr, 2) if blended_irr is not None else None,
        "blended_moic": round(blended_moic, 3) if blended_moic is not None else None,
        "blended_tvpi": round(blended_tvpi, 3) if blended_tvpi is not None else None,
        "blended_dpi": round(blended_dpi, 3) if blended_dpi is not None else None,
        "blended_rvpi": round(blended_rvpi, 3) if blended_rvpi is not None else None,
        "aum": aum_breakdown,
        "funds": fund_summaries,
    }


def get_vintage_analysis(db: Session, firm_id: int) -> Optional[Dict[str, Any]]:
    """Vintage cohort analysis for a firm's funds."""
    firm, fund_data = _get_firm_fund_data(db, firm_id)
    if not firm:
        return None

    cohorts = _group_by_vintage(fund_data)

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "cohort_count": len(cohorts),
        "cohorts": list(cohorts.values()),
    }


def get_sector_concentration(db: Session, firm_id: int) -> Optional[Dict[str, Any]]:
    """Sector concentration analysis with HHI risk score."""
    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        return None

    # Get all portfolio companies for this firm (via funds → investments)
    fund_ids = db.execute(
        select(PEFund.id).where(PEFund.firm_id == firm_id)
    ).scalars().all()

    if not fund_ids:
        return {"firm_id": firm_id, "firm_name": firm.name, "sectors": [], "hhi": 0, "risk_level": "N/A"}

    company_ids = db.execute(
        select(PEFundInvestment.company_id)
        .where(PEFundInvestment.fund_id.in_(fund_ids))
        .distinct()
    ).scalars().all()

    companies = db.execute(
        select(PEPortfolioCompany)
        .where(PEPortfolioCompany.id.in_(company_ids))
    ).scalars().all()

    if not companies:
        return {"firm_id": firm_id, "firm_name": firm.name, "sectors": [], "hhi": 0, "risk_level": "N/A"}

    # Count by sector
    sector_counts: Dict[str, int] = defaultdict(int)
    for co in companies:
        sector = co.industry or "Unknown"
        sector_counts[sector] += 1

    total = len(companies)
    sectors = []
    shares = []
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        pct = round(count / total * 100, 1)
        shares.append(pct)
        sectors.append({
            "sector": sector,
            "company_count": count,
            "portfolio_pct": pct,
        })

    hhi = round(_calculate_hhi(shares), 0)
    risk_level = _classify_hhi(hhi)

    # Top-heavy risk
    sorted_pcts = sorted(shares, reverse=True)
    top3_pct = round(sum(sorted_pcts[:3]), 1) if len(sorted_pcts) >= 3 else round(sum(sorted_pcts), 1)
    largest_single = sorted_pcts[0] if sorted_pcts else 0

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "total_companies": total,
        "sector_count": len(sectors),
        "sectors": sectors,
        "hhi": hhi,
        "risk_level": risk_level,
        "top_3_concentration_pct": top3_pct,
        "largest_single_exposure_pct": largest_single,
    }


def get_portfolio_risk_dashboard(db: Session, firm_id: int) -> Optional[Dict[str, Any]]:
    """Composite risk dashboard across multiple dimensions."""
    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        return None

    # Sector concentration
    sector_data = get_sector_concentration(db, firm_id)

    # Get portfolio companies
    fund_ids = db.execute(
        select(PEFund.id).where(PEFund.firm_id == firm_id)
    ).scalars().all()

    company_ids = []
    if fund_ids:
        company_ids = db.execute(
            select(PEFundInvestment.company_id)
            .where(PEFundInvestment.fund_id.in_(fund_ids))
            .distinct()
        ).scalars().all()

    companies = []
    if company_ids:
        companies = db.execute(
            select(PEPortfolioCompany)
            .where(PEPortfolioCompany.id.in_(company_ids))
        ).scalars().all()

    # Geographic concentration
    state_counts: Dict[str, int] = defaultdict(int)
    for co in companies:
        state = co.headquarters_state or "Unknown"
        state_counts[state] += 1
    geo_shares = [count / len(companies) * 100 for count in state_counts.values()] if companies else []
    geo_hhi = round(_calculate_hhi(geo_shares), 0) if geo_shares else 0

    # Vintage concentration
    _, fund_data = _get_firm_fund_data(db, firm_id)
    vintage_counts: Dict[int, float] = defaultdict(float)
    total_committed = sum(f["committed"] for f in fund_data)
    for f in fund_data:
        if f.get("vintage_year") and total_committed > 0:
            vintage_counts[f["vintage_year"]] += f["committed"]
    max_vintage_pct = round(max(vintage_counts.values()) / total_committed * 100, 1) if vintage_counts and total_committed > 0 else 0

    # Exit readiness distribution
    exit_dist = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
    try:
        from app.core.pe_exit_scoring import score_exit_readiness
        for co in companies:
            er = score_exit_readiness(db, co.id)
            if er and er.grade in exit_dist:
                exit_dist[er.grade] += 1
    except Exception:
        pass

    # Management gap risk
    companies_missing_ceo = 0
    companies_missing_cfo = 0
    for co in companies:
        leaders = db.execute(
            select(PECompanyLeadership)
            .where(
                PECompanyLeadership.company_id == co.id,
                PECompanyLeadership.is_current == True,
            )
        ).scalars().all()
        has_ceo = any(l.is_ceo for l in leaders)
        has_cfo = any(l.is_cfo for l in leaders)
        if not has_ceo:
            companies_missing_ceo += 1
        if not has_cfo:
            companies_missing_cfo += 1

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "portfolio_size": len(companies),
        "sector_concentration": {
            "hhi": sector_data.get("hhi", 0) if sector_data else 0,
            "risk_level": sector_data.get("risk_level", "N/A") if sector_data else "N/A",
            "sector_count": sector_data.get("sector_count", 0) if sector_data else 0,
            "top_3_pct": sector_data.get("top_3_concentration_pct", 0) if sector_data else 0,
        },
        "geographic_concentration": {
            "hhi": geo_hhi,
            "risk_level": _classify_hhi(geo_hhi),
            "state_count": len(state_counts),
        },
        "vintage_concentration": {
            "max_single_vintage_pct": max_vintage_pct,
            "vintage_count": len(vintage_counts),
            "risk_level": "Concentrated" if max_vintage_pct > 50 else "Moderate" if max_vintage_pct > 33 else "Diversified",
        },
        "exit_readiness_distribution": exit_dist,
        "management_gaps": {
            "missing_ceo": companies_missing_ceo,
            "missing_cfo": companies_missing_cfo,
            "gap_pct": round((companies_missing_ceo + companies_missing_cfo) / (len(companies) * 2) * 100, 1) if companies else 0,
        },
        "financial_health": {
            "total_companies": len(companies),
        },
    }


def calculate_pme(db: Session, firm_id: int) -> Optional[Dict[str, Any]]:
    """Public Market Equivalent comparison."""
    firm, fund_data = _get_firm_fund_data(db, firm_id)
    if not firm:
        return None

    total_called = sum(f["called"] for f in fund_data)
    total_distributed = sum(f["distributed"] for f in fund_data)
    total_nav = sum(f["nav"] for f in fund_data)
    total_value = total_distributed + total_nav

    # Estimate average hold period from vintage years
    vintages = [f["vintage_year"] for f in fund_data if f.get("vintage_year")]
    if vintages:
        avg_vintage = sum(vintages) / len(vintages)
        hold_years = max(date.today().year - avg_vintage, 1)
    else:
        hold_years = 5  # default

    pme_results = {}
    for key, bench in BENCHMARK_RETURNS.items():
        pme = _calculate_pme_ratio(total_value, total_called, hold_years, bench["annual_return"])
        outperformed = pme > 1.0 if pme else None
        pme_results[key] = {
            "benchmark_name": bench["name"],
            "annual_return_pct": bench["annual_return"] * 100,
            "pme_ratio": pme,
            "outperformed": outperformed,
            "interpretation": (
                f"PE outperformed {bench['name']} by {round((pme - 1) * 100, 1)}%"
                if pme and pme > 1
                else f"PE underperformed {bench['name']} by {round((1 - pme) * 100, 1)}%"
                if pme
                else "Insufficient data"
            ),
        }

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "total_called_usd_m": round(total_called, 2),
        "total_value_usd_m": round(total_value, 2),
        "avg_hold_years": hold_years,
        "benchmarks": pme_results,
    }


def get_benchmark_comparison(db: Session, firm_id: int) -> Optional[Dict[str, Any]]:
    """Compare firm performance vs public market benchmarks."""
    firm, fund_data = _get_firm_fund_data(db, firm_id)
    if not firm:
        return None

    # Firm blended IRR
    irr_vals = [(f["irr"], f["committed"]) for f in fund_data if f["irr"] is not None and f["committed"] > 0]
    blended_irr = _weighted_avg([v for v, _ in irr_vals], [w for _, w in irr_vals]) if irr_vals else None

    # Firm blended TVPI
    tvpi_vals = [(f["tvpi"], f["committed"]) for f in fund_data if f["tvpi"] is not None and f["committed"] > 0]
    blended_tvpi = _weighted_avg([v for v, _ in tvpi_vals], [w for _, w in tvpi_vals]) if tvpi_vals else None

    # Firm blended DPI
    dpi_vals = [(f["dpi"], f["committed"]) for f in fund_data if f["dpi"] is not None and f["committed"] > 0]
    blended_dpi = _weighted_avg([v for v, _ in dpi_vals], [w for _, w in dpi_vals]) if dpi_vals else None

    comparisons = [
        {
            "metric": "Net IRR (%)",
            "firm_value": round(blended_irr, 2) if blended_irr is not None else None,
            "ca_pe_median": CA_PE_MEDIAN["irr"],
            "sp500": BENCHMARK_RETURNS["sp500"]["annual_return"] * 100,
            "russell2000": BENCHMARK_RETURNS["russell2000"]["annual_return"] * 100,
            "vs_median": round(blended_irr - CA_PE_MEDIAN["irr"], 2) if blended_irr is not None else None,
        },
        {
            "metric": "TVPI (x)",
            "firm_value": round(blended_tvpi, 3) if blended_tvpi is not None else None,
            "ca_pe_median": CA_PE_MEDIAN["tvpi"],
            "sp500": None,
            "russell2000": None,
            "vs_median": round(blended_tvpi - CA_PE_MEDIAN["tvpi"], 3) if blended_tvpi is not None else None,
        },
        {
            "metric": "DPI (x)",
            "firm_value": round(blended_dpi, 3) if blended_dpi is not None else None,
            "ca_pe_median": CA_PE_MEDIAN["dpi"],
            "sp500": None,
            "russell2000": None,
            "vs_median": round(blended_dpi - CA_PE_MEDIAN["dpi"], 3) if blended_dpi is not None else None,
        },
    ]

    quartile = "Unknown"
    if blended_irr is not None:
        if blended_irr >= 20:
            quartile = "Top Quartile"
        elif blended_irr >= 14:
            quartile = "Second Quartile"
        elif blended_irr >= 8:
            quartile = "Third Quartile"
        else:
            quartile = "Bottom Quartile"

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "fund_count": len(fund_data),
        "quartile_ranking": quartile,
        "comparisons": comparisons,
    }
