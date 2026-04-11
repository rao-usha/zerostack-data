"""
Tests for PLAN_057 — Synthetic Data Validation Service.

All tests run offline (no DB required).  The validators either sample
from in-memory distribution constants or use a mock DB session that
triggers generator fallback priors.
"""
import numpy as np
import pytest

from app.services.synthetic.validation import (
    SyntheticValidator,
    _chi_squared_test,
    _correlation_comparison,
    _descriptive_stats,
    _ks_test,
)


# ---------------------------------------------------------------------------
# Shared utility tests
# ---------------------------------------------------------------------------


class TestChiSquared:
    def test_passes_for_correct_distribution(self):
        """Sampling from the expected distribution should pass chi-squared."""
        rng = np.random.default_rng(42)
        probs = np.array([0.3, 0.5, 0.2])
        n = 5000
        counts = rng.multinomial(n, probs)
        result = _chi_squared_test(counts, probs, "test_field")
        assert result["passed"] is True
        assert result["test_name"] == "chi_squared"
        assert result["field"] == "test_field"
        assert result["n_samples"] == n

    def test_fails_for_wrong_distribution(self):
        """Heavily skewed counts against a uniform expectation should fail."""
        observed = np.array([900, 50, 50])
        expected_probs = np.array([1 / 3, 1 / 3, 1 / 3])
        result = _chi_squared_test(observed, expected_probs, "skewed")
        assert result["passed"] is False
        assert result["p_value"] < 0.001


class TestKS:
    def test_passes_for_normal(self):
        """Samples drawn from N(0,1) should pass a KS test against 'norm'."""
        # seed=123 gives a clear pass; seed=42 is borderline at n=2000
        rng = np.random.default_rng(123)
        samples = rng.standard_normal(2000)
        result = _ks_test(samples, "norm", args=(0, 1), field_name="normal")
        assert result["passed"] is True
        assert result["test_name"] == "ks_test"
        assert result["n_samples"] == 2000

    def test_fails_for_wrong_distribution(self):
        """Uniform samples should fail a normality KS test."""
        rng = np.random.default_rng(42)
        samples = rng.uniform(0, 10, size=2000)
        result = _ks_test(samples, "norm", args=(5, 1), field_name="uniform_vs_norm")
        assert result["passed"] is False


class TestDescriptiveStats:
    def test_shape(self):
        arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        stats = _descriptive_stats(arr)
        assert set(stats.keys()) == {"mean", "std", "min", "max", "p25", "p50", "p75", "n"}
        assert stats["n"] == 5
        assert stats["min"] == 1.0
        assert stats["max"] == 5.0
        assert stats["p50"] == 3.0


class TestCorrelationComparison:
    def test_identical_matrices(self):
        m = np.array([[1.0, 0.5], [0.5, 1.0]])
        result = _correlation_comparison(m, m, ["a", "b"])
        assert result["frobenius_norm_diff"] == 0.0
        assert result["max_element_diff"] == 0.0
        assert len(result["labels"]) == 2

    def test_different_matrices(self):
        expected = np.array([[1.0, 0.5], [0.5, 1.0]])
        actual = np.array([[1.0, 0.2], [0.2, 1.0]])
        result = _correlation_comparison(expected, actual, ["a", "b"])
        assert result["frobenius_norm_diff"] > 0
        assert result["max_element_diff"] == pytest.approx(0.3, abs=0.01)


# ---------------------------------------------------------------------------
# Per-generator validator tests
# ---------------------------------------------------------------------------


class TestValidateJobPostings:
    def test_passes_with_seed(self):
        result = SyntheticValidator.validate_job_postings(n_samples=2000, seed=42)
        assert result["generator"] == "job_postings"
        assert result["status"] in ("PASS", "WARN")
        assert result["n_tests"] >= 3
        assert result["n_passed"] >= 2
        assert len(result["histograms"]) >= 2


class TestValidateLpGp:
    def test_passes(self):
        result = SyntheticValidator.validate_lp_gp(n_samples=2000, seed=42)
        assert result["generator"] == "lp_gp"
        assert result["status"] in ("PASS", "WARN")
        assert result["n_tests"] >= 3
        assert result["n_passed"] >= 2


class TestValidatePrivateFinancials:
    def test_passes(self):
        result = SyntheticValidator.validate_private_financials(
            n_companies=500, sector="industrials", seed=42,
        )
        assert result["generator"] == "private_financials"
        assert result["status"] in ("PASS", "WARN")
        assert result["n_tests"] >= 4
        # Ordering constraint must always pass
        ordering = [t for t in result["tests"] if t["test_name"] == "ordering_constraint"]
        assert len(ordering) == 1
        assert ordering[0]["passed"] is True
        # Sample data present
        assert len(result.get("sample_data", [])) > 0
        # Correlation section present
        assert "correlation" in result


class TestValidateAll:
    def test_returns_summary(self):
        result = SyntheticValidator.validate_all(seed=42)
        assert "overall_status" in result
        assert "total_tests" in result
        assert "total_passed" in result
        assert "overall_pass_rate" in result
        assert "generators" in result
        assert set(result["generators"].keys()) == {
            "job_postings", "lp_gp", "macro_scenarios", "private_financials",
        }
        # Every generator has the required fields
        for gen_data in result["generators"].values():
            assert "status" in gen_data
            assert "tests" in gen_data
            assert "pass_rate" in gen_data
