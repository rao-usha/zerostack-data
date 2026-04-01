"""
Private Company Financial Generator — SPEC_042 / PLAN_052 Phase A1.

Generates synthetic private company financial profiles on demand by:
  1. Querying public_company_financials for same-sector/size peer companies
  2. Computing financial ratio distributions (gross margin, EBITDA margin, net margin)
  3. Fitting a multivariate Gaussian on those ratios (Cholesky sampling for correlation)
  4. Sampling correlated synthetic ratio sets + log-normal revenue
  5. Returning fully-formed synthetic company profiles

No ML model pre-training required — runs on ingested EDGAR XBRL data.
"""
from __future__ import annotations
import logging
import math
from typing import Dict, List, Optional
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sector keyword filters — used to narrow peer query by company name
# ---------------------------------------------------------------------------

SECTOR_KEYWORDS: Dict[str, List[str]] = {
    "industrials": [
        "manufacturing", "industrial", "machinery", "equipment", "aerospace",
        "defense", "construction", "engineering", "steel", "chemical",
    ],
    "technology": [
        "tech", "software", "semiconductor", "cloud", "digital", "data",
        "systems", "solutions", "computing", "cyber", "analytics",
    ],
    "healthcare": [
        "health", "medical", "pharma", "biotech", "hospital", "care",
        "therapeutics", "diagnostics", "clinical", "life sciences",
    ],
    "consumer": [
        "retail", "consumer", "food", "beverage", "apparel", "restaurant",
        "grocery", "ecommerce", "brand", "goods",
    ],
    "energy": [
        "energy", "oil", "gas", "petroleum", "pipeline", "power",
        "utility", "renewable", "solar", "wind",
    ],
    "financial": [
        "financial", "bank", "insurance", "capital", "investment",
        "asset management", "wealth", "credit", "lending",
    ],
    "real_estate": [
        "realty", "real estate", "properties", "reit", "apartment",
        "commercial", "residential", "development",
    ],
    "logistics": [
        "logistics", "transport", "shipping", "freight", "supply chain",
        "distribution", "warehousing", "delivery",
    ],
}

# ---------------------------------------------------------------------------
# Sector priors — fallback when < 5 real peers found.
# Tuples: (mean, std) for gross_margin, ebitda_margin, net_margin
# Correlation matrix represents typical co-movement of these three ratios.
# ---------------------------------------------------------------------------

SECTOR_PRIORS: Dict[str, Dict] = {
    "industrials": {
        "gross_margin":  (0.28, 0.08),
        "ebitda_margin": (0.13, 0.05),
        "net_margin":    (0.06, 0.04),
        "corr": [[1.0, 0.75, 0.65], [0.75, 1.0, 0.82], [0.65, 0.82, 1.0]],
        "revenue_log_mean": 5.5,   # log($millions)
        "revenue_log_std":  1.2,
    },
    "technology": {
        "gross_margin":  (0.58, 0.14),
        "ebitda_margin": (0.18, 0.12),
        "net_margin":    (0.10, 0.10),
        "corr": [[1.0, 0.70, 0.60], [0.70, 1.0, 0.85], [0.60, 0.85, 1.0]],
        "revenue_log_mean": 5.0,
        "revenue_log_std":  1.3,
    },
    "healthcare": {
        "gross_margin":  (0.52, 0.16),
        "ebitda_margin": (0.16, 0.10),
        "net_margin":    (0.08, 0.08),
        "corr": [[1.0, 0.72, 0.62], [0.72, 1.0, 0.83], [0.62, 0.83, 1.0]],
        "revenue_log_mean": 5.2,
        "revenue_log_std":  1.4,
    },
    "consumer": {
        "gross_margin":  (0.35, 0.10),
        "ebitda_margin": (0.10, 0.05),
        "net_margin":    (0.04, 0.04),
        "corr": [[1.0, 0.73, 0.60], [0.73, 1.0, 0.80], [0.60, 0.80, 1.0]],
        "revenue_log_mean": 5.8,
        "revenue_log_std":  1.1,
    },
    "energy": {
        "gross_margin":  (0.35, 0.12),
        "ebitda_margin": (0.22, 0.10),
        "net_margin":    (0.07, 0.07),
        "corr": [[1.0, 0.78, 0.65], [0.78, 1.0, 0.82], [0.65, 0.82, 1.0]],
        "revenue_log_mean": 6.2,
        "revenue_log_std":  1.3,
    },
    "financial": {
        "gross_margin":  (0.70, 0.12),
        "ebitda_margin": (0.32, 0.14),
        "net_margin":    (0.18, 0.10),
        "corr": [[1.0, 0.80, 0.72], [0.80, 1.0, 0.88], [0.72, 0.88, 1.0]],
        "revenue_log_mean": 5.5,
        "revenue_log_std":  1.4,
    },
    "real_estate": {
        "gross_margin":  (0.55, 0.14),
        "ebitda_margin": (0.35, 0.12),
        "net_margin":    (0.12, 0.10),
        "corr": [[1.0, 0.82, 0.70], [0.82, 1.0, 0.86], [0.70, 0.86, 1.0]],
        "revenue_log_mean": 5.0,
        "revenue_log_std":  1.2,
    },
    "logistics": {
        "gross_margin":  (0.22, 0.07),
        "ebitda_margin": (0.08, 0.04),
        "net_margin":    (0.03, 0.03),
        "corr": [[1.0, 0.72, 0.62], [0.72, 1.0, 0.80], [0.62, 0.80, 1.0]],
        "revenue_log_mean": 6.0,
        "revenue_log_std":  1.1,
    },
}

# Default for unknown sectors
_DEFAULT_PRIORS = {
    "gross_margin":  (0.40, 0.15),
    "ebitda_margin": (0.15, 0.08),
    "net_margin":    (0.07, 0.06),
    "corr": [[1.0, 0.70, 0.60], [0.70, 1.0, 0.82], [0.60, 0.82, 1.0]],
    "revenue_log_mean": 5.3,
    "revenue_log_std":  1.3,
}

# Margin clamp bounds: (min, max) for gross, ebitda, net
_MARGIN_CLAMPS = {
    "gross_margin":  (0.0, 0.98),
    "ebitda_margin": (-0.30, 0.80),
    "net_margin":    (-0.50, 0.70),
}


class PrivateCompanyFinancialGenerator:
    """
    Generates synthetic private company financial profiles conditioned on
    public company peers from EDGAR XBRL data.
    """

    MIN_PEERS_FOR_FITTED_MODEL = 5

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        n_companies: int = 10,
        sector: Optional[str] = None,
        revenue_min_millions: float = 0.0,
        revenue_max_millions: float = 100_000.0,
        seed: Optional[int] = None,
    ) -> Dict:
        """
        Generate n_companies synthetic private company financial profiles.

        Returns a dict with methodology metadata + list of company dicts.
        """
        rng = np.random.default_rng(seed)

        # 1. Query real peers
        peers = self._query_peers(sector, revenue_min_millions, revenue_max_millions)
        peer_count = len(peers)

        # 2. Fit distribution from peers or fall back to sector priors
        if peer_count >= self.MIN_PEERS_FOR_FITTED_MODEL:
            dist, fallback = self._fit_from_peers(peers, revenue_min_millions, revenue_max_millions)
            methodology = "gaussian_copula_from_peers"
        else:
            dist, fallback = self._sector_priors(sector, revenue_min_millions, revenue_max_millions)
            methodology = "gaussian_copula_from_peers"

        # 3. Sample correlated margin sets
        companies = self._sample_companies(n_companies, dist, rng)

        result: Dict = {
            "sector": sector,
            "peer_count": peer_count,
            "synthetic_count": n_companies,
            "methodology": methodology,
            "ratio_stats": {
                "gross_margin": {"mean": round(dist["gm_mean"], 4), "std": round(dist["gm_std"], 4)},
                "ebitda_margin": {"mean": round(dist["em_mean"], 4), "std": round(dist["em_std"], 4)},
                "net_margin": {"mean": round(dist["nm_mean"], 4), "std": round(dist["nm_std"], 4)},
            },
            "companies": companies,
        }
        if fallback:
            result["fallback"] = "sector_priors"
        return result

    # ------------------------------------------------------------------
    # Peer query
    # ------------------------------------------------------------------

    def _query_peers(
        self, sector: Optional[str], rev_min: float, rev_max: float
    ) -> List[Dict]:
        """Query public_company_financials for annual peer records."""
        rev_min_usd = rev_min * 1_000_000
        rev_max_usd = rev_max * 1_000_000

        # Build optional keyword filter
        keyword_clause = ""
        params: Dict = {"rev_min": rev_min_usd, "rev_max": rev_max_usd}

        if sector and sector.lower() in SECTOR_KEYWORDS:
            keywords = SECTOR_KEYWORDS[sector.lower()]
            conditions = " OR ".join(
                f"LOWER(company_name) LIKE :kw{i}" for i, _ in enumerate(keywords)
            )
            keyword_clause = f"AND ({conditions})"
            for i, kw in enumerate(keywords):
                params[f"kw{i}"] = f"%{kw}%"

        sql = f"""
            SELECT
                revenue_usd,
                gross_profit_usd,
                ebitda_usd,
                net_income_usd
            FROM public_company_financials
            WHERE fiscal_period = 'FY'
              AND revenue_usd BETWEEN :rev_min AND :rev_max
              AND revenue_usd > 0
              AND gross_profit_usd IS NOT NULL
              AND ebitda_usd IS NOT NULL
              AND net_income_usd IS NOT NULL
              {keyword_clause}
            ORDER BY period_end_date DESC
            LIMIT 500
        """
        try:
            rows = self.db.execute(text(sql), params).fetchall()
            return [
                {
                    "revenue": float(r[0]),
                    "gross_profit": float(r[1]),
                    "ebitda": float(r[2]),
                    "net_income": float(r[3]),
                }
                for r in rows
                if r[0] and r[0] > 0
            ]
        except Exception as exc:
            logger.debug("Peer query failed: %s", exc)
            try:
                self.db.rollback()
            except Exception:
                pass
            return []

    # ------------------------------------------------------------------
    # Distribution fitting
    # ------------------------------------------------------------------

    def _fit_from_peers(
        self, peers: List[Dict], rev_min: float, rev_max: float
    ) -> tuple:
        """Fit a multivariate Gaussian on financial ratios from real peers."""
        gm = np.array([p["gross_profit"] / p["revenue"] for p in peers])
        em = np.array([p["ebitda"] / p["revenue"] for p in peers])
        nm = np.array([p["net_income"] / p["revenue"] for p in peers])

        # Clamp to valid ranges before fitting
        gm = np.clip(gm, *_MARGIN_CLAMPS["gross_margin"])
        em = np.clip(em, *_MARGIN_CLAMPS["ebitda_margin"])
        nm = np.clip(nm, *_MARGIN_CLAMPS["net_margin"])

        # Revenue log-normal parameters from peers
        rev_millions = np.array([p["revenue"] / 1_000_000 for p in peers])
        log_rev = np.log(rev_millions[rev_millions > 0])
        rev_log_mean = float(np.mean(log_rev))
        rev_log_std = float(np.std(log_rev)) or 1.0

        # Cap revenue sampling within the requested bounds
        if rev_min > 0 and rev_max < 100_000:
            rev_log_mean = math.log((rev_min + rev_max) / 2)
            rev_log_std = math.log(rev_max / max(rev_min, 1)) / 3

        ratios = np.stack([gm, em, nm], axis=1)
        cov = np.cov(ratios.T)
        # Ensure positive semi-definiteness by adding small jitter
        cov += np.eye(3) * 1e-6

        return {
            "gm_mean": float(np.mean(gm)),
            "em_mean": float(np.mean(em)),
            "nm_mean": float(np.mean(nm)),
            "gm_std":  float(np.std(gm)) or 0.05,
            "em_std":  float(np.std(em)) or 0.03,
            "nm_std":  float(np.std(nm)) or 0.03,
            "cov": cov,
            "rev_log_mean": rev_log_mean,
            "rev_log_std":  max(rev_log_std, 0.1),
            "rev_min": rev_min,
            "rev_max": rev_max,
        }, False

    def _sector_priors(self, sector: Optional[str], rev_min: float, rev_max: float) -> tuple:
        """Return sector prior distribution when peers are insufficient."""
        priors = SECTOR_PRIORS.get((sector or "").lower(), _DEFAULT_PRIORS)

        gm_mean, gm_std = priors["gross_margin"]
        em_mean, em_std = priors["ebitda_margin"]
        nm_mean, nm_std = priors["net_margin"]
        corr = np.array(priors["corr"])
        std_vec = np.array([gm_std, em_std, nm_std])
        cov = np.outer(std_vec, std_vec) * corr

        # Revenue bounds: use provided range if specific, else sector defaults
        if rev_min > 0 and rev_max < 100_000:
            rev_log_mean = math.log((rev_min + rev_max) / 2)
            rev_log_std = math.log(rev_max / max(rev_min, 1)) / 3 or 0.5
        else:
            rev_log_mean = priors["revenue_log_mean"]
            rev_log_std = priors["revenue_log_std"]

        return {
            "gm_mean": gm_mean, "em_mean": em_mean, "nm_mean": nm_mean,
            "gm_std": gm_std,   "em_std": em_std,   "nm_std": nm_std,
            "cov": cov,
            "rev_log_mean": rev_log_mean,
            "rev_log_std": rev_log_std,
            "rev_min": rev_min,
            "rev_max": rev_max,
        }, True

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def _sample_companies(
        self, n: int, dist: Dict, rng: np.random.Generator
    ) -> List[Dict]:
        """Sample n synthetic company profiles from the fitted distribution."""
        mean = np.array([dist["gm_mean"], dist["em_mean"], dist["nm_mean"]])
        cov = np.array(dist["cov"])

        # Cholesky decomposition for correlated sampling
        try:
            L = np.linalg.cholesky(cov)
        except np.linalg.LinAlgError:
            # Fall back to diagonal if cov isn't PD
            L = np.diag(np.sqrt(np.diag(cov)))

        # Sample n sets of standard normals, rotate into correlated space
        Z = rng.standard_normal((n, 3))
        samples = Z @ L.T + mean  # shape (n, 3): [gm, em, nm]

        # Clamp margins to valid bounds
        samples[:, 0] = np.clip(samples[:, 0], *_MARGIN_CLAMPS["gross_margin"])
        samples[:, 1] = np.clip(samples[:, 1], *_MARGIN_CLAMPS["ebitda_margin"])
        samples[:, 2] = np.clip(samples[:, 2], *_MARGIN_CLAMPS["net_margin"])

        # Enforce ordering: net_margin ≤ ebitda_margin ≤ gross_margin
        samples[:, 1] = np.minimum(samples[:, 1], samples[:, 0])  # ebitda ≤ gross
        samples[:, 2] = np.minimum(samples[:, 2], samples[:, 1])  # net ≤ ebitda

        # Sample revenues from log-normal, then hard-clip to requested bounds
        log_rev = rng.normal(dist["rev_log_mean"], dist["rev_log_std"], n)
        revenues = np.exp(log_rev)
        rev_min = dist.get("rev_min", 0.0)
        rev_max = dist.get("rev_max", 1e9)
        if rev_min > 0 or rev_max < 1e9:
            revenues = np.clip(revenues, max(rev_min, 1e-3), rev_max)

        companies = []
        for i in range(n):
            rev = round(float(revenues[i]), 2)
            gm, em, nm = float(samples[i, 0]), float(samples[i, 1]), float(samples[i, 2])

            companies.append({
                "company_id": f"synth_{i+1:03d}",
                "revenue_millions":      rev,
                "gross_profit_millions": round(rev * gm, 2),
                "ebitda_millions":       round(rev * em, 2),
                "net_income_millions":   round(rev * nm, 2),
                "gross_margin_pct":      round(gm * 100, 2),
                "ebitda_margin_pct":     round(em * 100, 2),
                "net_margin_pct":        round(nm * 100, 2),
            })

        return companies
