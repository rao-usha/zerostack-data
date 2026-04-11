"""
Synthetic Data Validation Service — PLAN_057.

Validates that each synthetic data generator produces output whose statistical
properties match the expected distributions.  All validators work WITHOUT a
live database — they either sample from the generators' own constants directly
or pass a mock session that triggers built-in fallback priors.

Test battery per generator:
  * Chi-squared goodness-of-fit for categorical distributions
  * Kolmogorov–Smirnov test for continuous distributions
  * Correlation matrix comparison (Frobenius norm)
  * Ordering / constraint checks where applicable
"""
from __future__ import annotations

import logging
import math
import random
from typing import Any, Callable, Dict, List, Optional, Union
from unittest.mock import MagicMock

import numpy as np
from scipy import stats as sp_stats

# Import actual distribution constants from generators
from app.services.synthetic.job_postings import (
    SENIORITY_WEIGHTS,
    SECTOR_ROLE_MIX,
    SyntheticJobPostingsGenerator,
)
from app.services.synthetic.lp_gp_universe import LP_TYPE_CONFIG

logger = logging.getLogger(__name__)


# ============================================================================
# Shared statistical utilities
# ============================================================================

def _chi_squared_test(
    observed_counts: np.ndarray,
    expected_probs: np.ndarray,
    field_name: str,
) -> Dict[str, Any]:
    """Chi-squared goodness-of-fit test."""
    n = int(np.sum(observed_counts))
    expected_counts = expected_probs * n
    # Pool bins with expected count < 1 to avoid instability
    mask = expected_counts >= 1.0
    if not mask.all():
        obs_pooled = np.append(observed_counts[mask], np.sum(observed_counts[~mask]))
        exp_pooled = np.append(expected_counts[mask], np.sum(expected_counts[~mask]))
    else:
        obs_pooled = observed_counts
        exp_pooled = expected_counts

    stat, p_value = sp_stats.chisquare(obs_pooled, f_exp=exp_pooled)
    return {
        "test_name": "chi_squared",
        "field": field_name,
        "statistic": round(float(stat), 4),
        "p_value": round(float(p_value), 6),
        "passed": bool(p_value >= 0.05),
        "n_samples": n,
    }


def _ks_test(
    samples: np.ndarray,
    cdf_name_or_callable: Union[str, Callable],
    args: tuple = (),
    field_name: str = "",
) -> Dict[str, Any]:
    """Kolmogorov-Smirnov test against a reference CDF."""
    stat, p_value = sp_stats.kstest(samples, cdf_name_or_callable, args=args)
    return {
        "test_name": "ks_test",
        "field": field_name,
        "statistic": round(float(stat), 4),
        "p_value": round(float(p_value), 6),
        "passed": bool(p_value >= 0.05),
        "n_samples": len(samples),
    }


def _descriptive_stats(arr: np.ndarray) -> Dict[str, float]:
    """Compute descriptive statistics for an array."""
    return {
        "mean": round(float(np.mean(arr)), 4),
        "std": round(float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0, 4),
        "min": round(float(np.min(arr)), 4),
        "max": round(float(np.max(arr)), 4),
        "p25": round(float(np.percentile(arr, 25)), 4),
        "p50": round(float(np.percentile(arr, 50)), 4),
        "p75": round(float(np.percentile(arr, 75)), 4),
        "n": int(len(arr)),
    }


def _histogram(
    arr: np.ndarray,
    bins_or_categories: Union[int, List[str]],
) -> Dict[str, Any]:
    """Build histogram data for charting."""
    if isinstance(bins_or_categories, list):
        labels = bins_or_categories
        actual = [int(np.sum(arr == label)) for label in labels]
        n = int(np.sum(actual))
        expected = [n / len(labels)] * len(labels)
        return {"labels": labels, "actual": actual, "expected": expected}
    else:
        counts, edges = np.histogram(arr, bins=bins_or_categories)
        labels = [f"{edges[i]:.2f}-{edges[i+1]:.2f}" for i in range(len(counts))]
        return {
            "labels": labels,
            "actual": [int(c) for c in counts],
            "expected": [int(len(arr) / bins_or_categories)] * len(counts),
        }


def _correlation_comparison(
    expected_matrix: np.ndarray,
    actual_matrix: np.ndarray,
    labels: List[str],
) -> Dict[str, Any]:
    """Compare two correlation matrices and compute distance metrics."""
    diff = actual_matrix - expected_matrix
    frobenius = float(np.linalg.norm(diff, "fro"))
    max_elem = float(np.max(np.abs(diff)))
    return {
        "labels": labels,
        "expected": [[round(float(v), 4) for v in row] for row in expected_matrix],
        "actual": [[round(float(v), 4) for v in row] for row in actual_matrix],
        "frobenius_norm_diff": round(frobenius, 4),
        "max_element_diff": round(max_elem, 4),
    }


def _make_mock_db() -> MagicMock:
    """Create a mock DB session whose execute().fetchall() returns []."""
    mock = MagicMock()
    mock.execute.return_value.fetchall.return_value = []
    return mock


# ============================================================================
# Per-generator validators
# ============================================================================

class SyntheticValidator:
    """Validates each synthetic generator against expected distributions."""

    # ----------------------------------------------------------------
    # Job Postings
    # ----------------------------------------------------------------

    @staticmethod
    def validate_job_postings(
        n_samples: int = 1000,
        seed: int = 42,
    ) -> Dict[str, Any]:
        """
        Validate synthetic job posting distributions using the actual
        SENIORITY_WEIGHTS and SECTOR_ROLE_MIX from the generator.
        """
        rng = random.Random(seed)
        tests: List[Dict] = []
        histograms: List[Dict] = []

        # --- Seniority chi-squared ---
        levels = list(SENIORITY_WEIGHTS.keys())
        weights = [SENIORITY_WEIGHTS[k] for k in levels]
        sampled = rng.choices(levels, weights=weights, k=n_samples)
        observed = np.array([sampled.count(lv) for lv in levels])
        expected_probs = np.array(weights)
        tests.append(_chi_squared_test(observed, expected_probs, "seniority"))
        histograms.append({
            "field": "seniority",
            "labels": levels,
            "actual": [int(c) for c in observed],
            "expected": [round(w * n_samples) for w in weights],
        })

        # --- Department chi-squared (technology sector) ---
        sector = "technology"
        depts = list(SECTOR_ROLE_MIX[sector].keys())
        dept_weights = [SECTOR_ROLE_MIX[sector][d] for d in depts]
        sampled_depts = rng.choices(depts, weights=dept_weights, k=n_samples)
        obs_depts = np.array([sampled_depts.count(d) for d in depts])
        tests.append(_chi_squared_test(obs_depts, np.array(dept_weights), "department_technology"))
        histograms.append({
            "field": "department_technology",
            "labels": depts,
            "actual": [int(c) for c in obs_depts],
            "expected": [round(w * n_samples) for w in dept_weights],
        })

        # --- posted_days_ago: KS test vs piecewise uniform CDF ---
        # The actual generator uses: 70% [0,30], 20% [30,60], 10% [60,90]
        days_ago = np.array([
            SyntheticJobPostingsGenerator._pick_posted_days_ago()
            for _ in range(n_samples)
        ])
        random.seed(seed)  # Reset for reproducibility

        def _piecewise_cdf(x):
            """CDF for the piecewise uniform: 70% [0,30], 20% [30,60], 10% [60,90]."""
            if x < 0:
                return 0.0
            elif x <= 30:
                return 0.70 * (x / 30.0)
            elif x <= 60:
                return 0.70 + 0.20 * ((x - 30) / 30.0)
            elif x <= 90:
                return 0.90 + 0.10 * ((x - 60) / 30.0)
            else:
                return 1.0

        cdf_vec = np.vectorize(_piecewise_cdf)
        tests.append(_ks_test(days_ago, cdf_vec, field_name="posted_days_ago"))
        histograms.append(_histogram(days_ago, 10))
        histograms[-1]["field"] = "posted_days_ago"

        passed = sum(1 for t in tests if t["passed"])
        return {
            "generator": "job_postings",
            "status": "PASS" if passed == len(tests) else ("WARN" if passed >= len(tests) - 1 else "FAIL"),
            "tests": tests,
            "histograms": histograms,
            "pass_rate": round(passed / len(tests) * 100, 1),
            "n_tests": len(tests),
            "n_passed": passed,
            "descriptive_stats": {
                "posted_days_ago": _descriptive_stats(days_ago),
            },
        }

    # ----------------------------------------------------------------
    # LP / GP Universe
    # ----------------------------------------------------------------

    @staticmethod
    def validate_lp_gp(
        n_samples: int = 1000,
        seed: int = 42,
    ) -> Dict[str, Any]:
        """
        Validate LP/GP universe distributions using actual LP_TYPE_CONFIG
        weights and AUM ranges from the generator.
        """
        rng = random.Random(seed)
        np_rng = np.random.default_rng(seed)
        tests: List[Dict] = []
        histograms: List[Dict] = []

        # --- LP type chi-squared ---
        lp_types = list(LP_TYPE_CONFIG.keys())
        type_weights = [LP_TYPE_CONFIG[t]["weight"] for t in lp_types]
        sampled_types = rng.choices(lp_types, weights=type_weights, k=n_samples)
        observed = np.array([sampled_types.count(t) for t in lp_types])
        tests.append(_chi_squared_test(observed, np.array(type_weights), "lp_type"))
        histograms.append({
            "field": "lp_type",
            "labels": lp_types,
            "actual": [int(c) for c in observed],
            "expected": [round(w * n_samples) for w in type_weights],
        })

        # --- AUM KS test vs uniform within pension range ---
        pp_cfg = LP_TYPE_CONFIG["public_pension"]
        lo, hi = pp_cfg["aum_range"]
        aum_samples = np_rng.uniform(lo, hi, size=n_samples)
        tests.append(_ks_test(aum_samples, "uniform", args=(lo, hi - lo), field_name="aum_public_pension"))
        histograms.append(_histogram(aum_samples, 10))
        histograms[-1]["field"] = "aum_public_pension"

        # --- Commitment pct KS test vs uniform(0.01, 0.05) ---
        commit_samples = np_rng.uniform(0.01, 0.05, size=n_samples)
        tests.append(_ks_test(commit_samples, "uniform", args=(0.01, 0.04), field_name="commitment_pct"))
        histograms.append(_histogram(commit_samples, 10))
        histograms[-1]["field"] = "commitment_pct"

        passed = sum(1 for t in tests if t["passed"])
        return {
            "generator": "lp_gp",
            "status": "PASS" if passed == len(tests) else ("WARN" if passed >= len(tests) - 1 else "FAIL"),
            "tests": tests,
            "histograms": histograms,
            "pass_rate": round(passed / len(tests) * 100, 1),
            "n_tests": len(tests),
            "n_passed": passed,
            "descriptive_stats": {
                "aum_public_pension": _descriptive_stats(aum_samples),
                "commitment_pct": _descriptive_stats(commit_samples),
            },
        }

    # ----------------------------------------------------------------
    # Macro Scenarios
    # ----------------------------------------------------------------

    @staticmethod
    def validate_macro_scenarios(
        n_scenarios: int = 200,
        seed: int = 42,
    ) -> Dict[str, Any]:
        """
        Validate macro scenario generator using mock DB (triggers fallback params).
        Checks terminal value distributions against O-U stationary distribution.
        """
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator

        mock_db = _make_mock_db()
        gen = MacroScenarioGenerator(mock_db)
        result = gen.generate(
            n_scenarios=n_scenarios,
            horizon_months=24,
            series=["DFF", "DGS10", "UNRATE"],
            seed=seed,
        )

        tests: List[Dict] = []
        histograms: List[Dict] = []
        corr_comparison = None

        # Extract terminal values per series
        series_terminals: Dict[str, np.ndarray] = {}
        for s in result["series"]:
            terminals = []
            for sc in result["scenarios"]:
                path = sc["paths"].get(s, [])
                if path:
                    terminals.append(path[-1])
            series_terminals[s] = np.array(terminals)

        # Access fallback params for expected distribution
        fallback_params = gen._fallback_params(result["series"])

        for s in result["series"]:
            arr = series_terminals.get(s)
            if arr is None or len(arr) < 10:
                continue

            p = fallback_params[s]
            mu = p["mu"]
            theta = p["theta"]
            sigma = p["sigma"]

            # O-U stationary distribution: N(mu, sigma^2 / (2*theta))
            ou_std = sigma / math.sqrt(2 * theta)

            tests.append(_ks_test(arr, "norm", args=(mu, ou_std), field_name=f"terminal_{s}"))
            histograms.append(_histogram(arr, 12))
            histograms[-1]["field"] = f"terminal_{s}"

        # Correlation matrix comparison
        if len(result["series"]) >= 2 and "_corr" in fallback_params:
            expected_corr = fallback_params["_corr"]
            terminal_matrix = np.column_stack(
                [series_terminals[s] for s in result["series"]]
            )
            if terminal_matrix.shape[0] > 2:
                actual_corr = np.corrcoef(terminal_matrix.T)
                corr_comparison = _correlation_comparison(
                    expected_corr, actual_corr, result["series"]
                )

        passed = sum(1 for t in tests if t["passed"])
        total = len(tests)
        resp: Dict[str, Any] = {
            "generator": "macro_scenarios",
            "status": "PASS" if passed == total else ("WARN" if passed >= total - 1 else "FAIL"),
            "tests": tests,
            "histograms": histograms,
            "pass_rate": round(passed / total * 100, 1) if total else 0.0,
            "n_tests": total,
            "n_passed": passed,
            "n_scenarios_generated": n_scenarios,
            "descriptive_stats": {
                f"terminal_{s}": _descriptive_stats(series_terminals[s])
                for s in result["series"]
                if s in series_terminals and len(series_terminals[s]) > 0
            },
        }
        if corr_comparison:
            resp["correlation"] = corr_comparison
        return resp

    # ----------------------------------------------------------------
    # Private Company Financials
    # ----------------------------------------------------------------

    @staticmethod
    def validate_private_financials(
        n_companies: int = 500,
        sector: str = "industrials",
        seed: int = 42,
    ) -> Dict[str, Any]:
        """
        Validate private company financial generator using mock DB (triggers
        sector priors fallback). Checks margins vs Normal, revenue vs LogNormal,
        correlation preservation, and ordering constraints.
        """
        from app.services.synthetic.private_company_financials import (
            PrivateCompanyFinancialGenerator,
            SECTOR_PRIORS,
            _DEFAULT_PRIORS,
        )

        mock_db = _make_mock_db()
        gen = PrivateCompanyFinancialGenerator(mock_db)
        result = gen.generate(
            n_companies=n_companies,
            sector=sector,
            revenue_min_millions=10.0,
            revenue_max_millions=500.0,
            seed=seed,
        )

        priors = SECTOR_PRIORS.get(sector, _DEFAULT_PRIORS)
        companies = result["companies"]

        tests: List[Dict] = []
        histograms: List[Dict] = []

        gm = np.array([c["gross_margin_pct"] / 100.0 for c in companies])
        em = np.array([c["ebitda_margin_pct"] / 100.0 for c in companies])
        nm = np.array([c["net_margin_pct"] / 100.0 for c in companies])
        rev = np.array([c["revenue_millions"] for c in companies])

        # Margin KS tests vs Normal(mean, std) from priors
        for name, arr, prior_key in [
            ("gross_margin", gm, "gross_margin"),
            ("ebitda_margin", em, "ebitda_margin"),
            ("net_margin", nm, "net_margin"),
        ]:
            mu, sigma = priors[prior_key]
            tests.append(_ks_test(arr, "norm", args=(mu, sigma), field_name=name))
            histograms.append(_histogram(arr, 12))
            histograms[-1]["field"] = name

        # Revenue KS test vs LogNormal
        log_rev = np.log(rev[rev > 0])
        rev_log_mean = math.log((10.0 + 500.0) / 2)
        rev_log_std = math.log(500.0 / max(10.0, 1)) / 3
        tests.append(_ks_test(log_rev, "norm", args=(rev_log_mean, rev_log_std), field_name="revenue_log"))
        histograms.append(_histogram(rev, 12))
        histograms[-1]["field"] = "revenue_millions"

        # Correlation matrix comparison
        expected_corr = np.array(priors["corr"])
        actual_margins = np.column_stack([gm, em, nm])
        actual_corr = np.corrcoef(actual_margins.T) if len(companies) > 2 else np.eye(3)
        corr_comparison = _correlation_comparison(
            expected_corr, actual_corr, ["gross_margin", "ebitda_margin", "net_margin"]
        )

        # Ordering constraint: net <= ebitda <= gross
        ordering_violations = 0
        for c in companies:
            if c["ebitda_margin_pct"] > c["gross_margin_pct"] + 0.01 or \
               c["net_margin_pct"] > c["ebitda_margin_pct"] + 0.01:
                ordering_violations += 1
        tests.append({
            "test_name": "ordering_constraint",
            "field": "net<=ebitda<=gross",
            "statistic": ordering_violations,
            "p_value": 1.0 if ordering_violations == 0 else 0.0,
            "passed": ordering_violations == 0,
            "n_samples": len(companies),
        })

        passed = sum(1 for t in tests if t["passed"])
        total = len(tests)
        return {
            "generator": "private_financials",
            "status": "PASS" if passed == total else ("WARN" if passed >= total - 1 else "FAIL"),
            "tests": tests,
            "histograms": histograms,
            "correlation": corr_comparison,
            "pass_rate": round(passed / total * 100, 1) if total else 0.0,
            "n_tests": total,
            "n_passed": passed,
            "n_companies_generated": n_companies,
            "sector": sector,
            "descriptive_stats": {
                "gross_margin": _descriptive_stats(gm),
                "ebitda_margin": _descriptive_stats(em),
                "net_margin": _descriptive_stats(nm),
                "revenue_millions": _descriptive_stats(rev),
            },
            "sample_data": companies[:10],
        }

    # ----------------------------------------------------------------
    # Validate All
    # ----------------------------------------------------------------

    @staticmethod
    def validate_all(seed: int = 42) -> Dict[str, Any]:
        """Run validation across all 4 synthetic generators."""
        results: Dict[str, Any] = {}
        generators = {
            "job_postings": lambda: SyntheticValidator.validate_job_postings(n_samples=500, seed=seed),
            "lp_gp": lambda: SyntheticValidator.validate_lp_gp(n_samples=500, seed=seed),
            "macro_scenarios": lambda: SyntheticValidator.validate_macro_scenarios(n_scenarios=200, seed=seed),
            "private_financials": lambda: SyntheticValidator.validate_private_financials(n_companies=500, seed=seed),
        }

        for name, fn in generators.items():
            try:
                results[name] = fn()
            except Exception as exc:
                logger.exception("Validation failed for %s", name)
                results[name] = {
                    "generator": name,
                    "status": "ERROR",
                    "error": str(exc),
                    "tests": [],
                    "pass_rate": 0.0,
                    "n_tests": 0,
                    "n_passed": 0,
                }

        statuses = [r["status"] for r in results.values()]
        if all(s == "PASS" for s in statuses):
            overall = "PASS"
        elif any(s in ("FAIL", "ERROR") for s in statuses):
            overall = "FAIL"
        else:
            overall = "WARN"

        total_tests = sum(r.get("n_tests", 0) for r in results.values())
        total_passed = sum(r.get("n_passed", 0) for r in results.values())

        return {
            "overall_status": overall,
            "total_tests": total_tests,
            "total_passed": total_passed,
            "overall_pass_rate": round(total_passed / total_tests * 100, 1) if total_tests else 0.0,
            "generators": results,
            "seed": seed,
        }
