"""
Macro Scenario Generator — SPEC_042 / PLAN_052 Phase A2.

Generates N forward macro scenarios using a mean-reverting correlated random walk
(discrete Ornstein-Uhlenbeck process) calibrated entirely from FRED data in the DB.

No ML pre-training required. Pure numpy.

Algorithm:
  1. Query historical monthly values for requested FRED series
  2. Compute monthly changes; fit long-run mean + mean-reversion speed per series
  3. Compute cross-series correlation matrix of monthly changes
  4. For each scenario: step forward T months using O-U with correlated Gaussian shocks
     X_{t+1} = X_t + theta*(mu - X_t)*dt + sigma*dW_t
     where dW_t ~ N(0,1) with cross-series correlation
  5. Apply hard clamps per series (rates ≥ 0, etc.)
  6. Return paths + terminal value percentile summary
"""
from __future__ import annotations
import logging
from typing import Dict, List, Optional
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text

log = logging.getLogger(__name__)

# Hard clamps applied at every step to keep simulated values realistic
SERIES_CLAMPS: Dict[str, tuple] = {
    "DFF":      (0.0, 25.0),   # Fed funds rate
    "DGS10":    (0.0, 25.0),   # 10Y Treasury
    "DGS2":     (0.0, 25.0),   # 2Y Treasury
    "UNRATE":   (1.0, 25.0),   # Unemployment
    "CPIAUCSL": (0.0, 30.0),   # CPI (Y/Y pct change)
    "UMCSENT":  (20.0, 120.0), # Consumer sentiment
    "INDPRO":   (-50.0, 50.0), # Industrial production (pct change)
    "DCOILWTICO": (0.0, 500.0),# WTI oil price
}

# Default clamp for unknown series
_DEFAULT_CLAMP = (-1e6, 1e6)

# Mean-reversion speed fallbacks (per month) if too little data to calibrate
_DEFAULT_THETA = 0.05


class MacroScenarioGenerator:
    """Generate N macro scenarios via mean-reverting correlated random walk."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        n_scenarios: int = 100,
        horizon_months: int = 24,
        series: Optional[List[str]] = None,
        seed: Optional[int] = None,
    ) -> Dict:
        if series is None:
            series = ["DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT"]

        rng = np.random.default_rng(seed)

        history = self._query_history(series)
        available = [s for s in series if s in history and len(history[s]) >= 3]

        if not available:
            # Full fallback: no FRED data at all — use hard-coded baseline
            history_months = 0
            params = self._fallback_params(series)
            available = series
        else:
            history_months = max(len(history[s]) for s in available)
            params = self._fit_params(history, available)

        # Simulate
        scenarios = self._simulate(n_scenarios, horizon_months, available, params, rng)

        # Build current_values (last observed value per series)
        current_values = {}
        for s in available:
            vals = history.get(s, [])
            current_values[s] = round(float(vals[-1]), 4) if vals else params[s]["mu"]

        # Summary: terminal value percentiles across all scenarios
        summary = self._compute_summary(scenarios, available)

        return {
            "status": "ok",
            "n_scenarios": n_scenarios,
            "horizon_months": horizon_months,
            "training_history_months": history_months,
            "series": available,
            "current_values": current_values,
            "methodology": "mean_reverting_correlated_random_walk",
            "scenarios": scenarios,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _query_history(self, series: List[str]) -> Dict[str, List[float]]:
        """Query FRED tables for all requested series. Returns {series_id: [vals]} sorted oldest→newest."""
        # FRED data lives in fred_observations (series_id, date, value)
        # and also in individual series tables depending on ingest path.
        # Try fred_observations first, then fallback per-series tables.
        result: Dict[str, List[float]] = {}

        # Query using series_id filter; order by date ascending
        try:
            rows = self.db.execute(
                text("""
                    SELECT date, series_id, value
                    FROM fred_observations
                    WHERE series_id = ANY(:sids)
                    ORDER BY series_id, date ASC
                """),
                {"sids": list(series)},
            ).fetchall()

            from collections import defaultdict
            grouped: Dict[str, List[float]] = defaultdict(list)
            for row in rows:
                try:
                    grouped[row[1]].append(float(row[2]))
                except (TypeError, ValueError):
                    pass
            result.update(dict(grouped))
        except Exception:
            pass

        # For any series not found, try the generic query pattern used by the FRED client
        missing = [s for s in series if s not in result or not result[s]]
        if missing:
            try:
                rows = self.db.execute(
                    text("""
                        SELECT date, series_id, value
                        FROM fred_observations
                        WHERE series_id = ANY(:sids)
                        ORDER BY series_id, date ASC
                    """),
                    {"sids": missing},
                ).fetchall()
                from collections import defaultdict
                grouped2: Dict[str, List[float]] = defaultdict(list)
                for row in rows:
                    try:
                        grouped2[row[1]].append(float(row[2]))
                    except (TypeError, ValueError):
                        pass
                for k, v in grouped2.items():
                    if v:
                        result[k] = v
            except Exception:
                pass

        return result

    # ------------------------------------------------------------------
    # Parameter fitting
    # ------------------------------------------------------------------

    def _fit_params(self, history: Dict[str, List[float]], series: List[str]) -> Dict:
        """Fit O-U parameters per series + cross-series correlation matrix."""
        changes: Dict[str, np.ndarray] = {}
        params: Dict[str, Dict] = {}

        for s in series:
            vals = np.array(history.get(s, []), dtype=float)
            if len(vals) < 3:
                params[s] = self._default_series_params(s)
                changes[s] = np.array([0.0])
                continue

            # Compute monthly changes
            dv = np.diff(vals)
            changes[s] = dv

            mu = float(np.mean(vals))
            sigma = float(np.std(dv)) if len(dv) > 1 else 0.01

            # Mean-reversion speed via OLS: dX = theta*(mu - X_{t-1}) dt
            # dX ~ theta*(mu - X) → regress dv on (mu - vals[:-1])
            X = mu - vals[:-1]
            if np.std(X) > 1e-10 and len(X) > 2:
                theta = float(np.dot(X, dv) / np.dot(X, X))
                theta = max(0.01, min(theta, 0.5))  # keep in sensible range
            else:
                theta = _DEFAULT_THETA

            params[s] = {
                "mu": mu,
                "theta": theta,
                "sigma": max(sigma, 1e-4),
                "last": float(vals[-1]),
            }

        # Build cross-series correlation matrix
        corr_matrix, ordered = self._build_corr_matrix(changes, series)
        params["_corr"] = corr_matrix
        params["_order"] = ordered
        return params

    def _build_corr_matrix(
        self, changes: Dict[str, np.ndarray], series: List[str]
    ) -> tuple:
        """Build correlation matrix from monthly change arrays (common length trimmed)."""
        min_len = min(len(changes.get(s, np.array([]))) for s in series)
        min_len = max(min_len, 2)

        aligned = []
        valid = []
        for s in series:
            arr = changes.get(s, np.zeros(min_len))
            if len(arr) >= min_len:
                aligned.append(arr[-min_len:])
                valid.append(s)
            else:
                aligned.append(np.zeros(min_len))
                valid.append(s)

        mat = np.array(aligned)  # (n_series, T)

        # Compute correlation matrix; handle zero-variance rows
        std = np.std(mat, axis=1, keepdims=True)
        std = np.where(std < 1e-10, 1.0, std)
        normalized = (mat - np.mean(mat, axis=1, keepdims=True)) / std
        corr = normalized @ normalized.T / max(min_len - 1, 1)

        # Ensure positive semi-definite via eigenvalue flooring
        eigvals, eigvecs = np.linalg.eigh(corr)
        eigvals = np.maximum(eigvals, 1e-6)
        corr = eigvecs @ np.diag(eigvals) @ eigvecs.T

        # Re-normalize diagonal to 1
        d = np.sqrt(np.diag(corr))
        corr = corr / np.outer(d, d)
        corr = np.clip(corr, -1.0, 1.0)
        np.fill_diagonal(corr, 1.0)

        return corr, valid

    def _default_series_params(self, s: str) -> Dict:
        fallbacks = {
            "DFF":      {"mu": 3.5,  "theta": 0.05, "sigma": 0.3,  "last": 3.5},
            "DGS10":    {"mu": 4.0,  "theta": 0.04, "sigma": 0.25, "last": 4.0},
            "DGS2":     {"mu": 3.8,  "theta": 0.05, "sigma": 0.3,  "last": 3.8},
            "UNRATE":   {"mu": 4.5,  "theta": 0.03, "sigma": 0.2,  "last": 4.5},
            "CPIAUCSL": {"mu": 2.5,  "theta": 0.04, "sigma": 0.3,  "last": 2.5},
            "UMCSENT":  {"mu": 65.0, "theta": 0.05, "sigma": 4.0,  "last": 65.0},
            "INDPRO":   {"mu": 0.15, "theta": 0.1,  "sigma": 0.8,  "last": 0.15},
            "DCOILWTICO": {"mu": 75.0, "theta": 0.03, "sigma": 5.0, "last": 75.0},
        }
        return fallbacks.get(s, {"mu": 0.0, "theta": 0.05, "sigma": 1.0, "last": 0.0})

    def _fallback_params(self, series: List[str]) -> Dict:
        params = {s: self._default_series_params(s) for s in series}
        n = len(series)
        params["_corr"] = np.eye(n)
        params["_order"] = list(series)
        return params

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------

    def _simulate(
        self,
        n_scenarios: int,
        horizon: int,
        series: List[str],
        params: Dict,
        rng: np.random.Generator,
    ) -> List[Dict]:
        corr: np.ndarray = params["_corr"]
        order: List[str] = params["_order"]

        # Cholesky decomposition for correlated shocks
        try:
            L = np.linalg.cholesky(corr)
        except np.linalg.LinAlgError:
            L = np.eye(len(order))

        n_series = len(order)
        scenarios = []

        for sc_idx in range(n_scenarios):
            # Starting values
            X = np.array([params[s]["last"] for s in order], dtype=float)
            paths: Dict[str, List[float]] = {s: [] for s in order}

            for _ in range(horizon):
                # Correlated standard normal shocks
                z = rng.standard_normal(n_series)
                dW = L @ z

                # O-U step for each series
                new_X = np.empty_like(X)
                for i, s in enumerate(order):
                    p = params[s]
                    drift = p["theta"] * (p["mu"] - X[i])
                    diffusion = p["sigma"] * dW[i]
                    new_X[i] = X[i] + drift + diffusion

                    # Apply hard clamps
                    lo, hi = SERIES_CLAMPS.get(s, _DEFAULT_CLAMP)
                    new_X[i] = float(np.clip(new_X[i], lo, hi))

                X = new_X
                for i, s in enumerate(order):
                    paths[s].append(round(float(X[i]), 4))

            scenarios.append({"scenario_id": sc_idx, "paths": paths})

        return scenarios

    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------

    def _compute_summary(self, scenarios: List[Dict], series: List[str]) -> Dict:
        summary: Dict[str, Dict] = {}
        for s in series:
            terminals = [sc["paths"][s][-1] for sc in scenarios if s in sc["paths"] and sc["paths"][s]]
            if not terminals:
                summary[s] = {"p10_terminal": 0.0, "p50_terminal": 0.0, "p90_terminal": 0.0}
                continue
            arr = np.array(terminals)
            summary[s] = {
                "p10_terminal": round(float(np.percentile(arr, 10)), 4),
                "p50_terminal": round(float(np.percentile(arr, 50)), 4),
                "p90_terminal": round(float(np.percentile(arr, 90)), 4),
            }
        return summary
