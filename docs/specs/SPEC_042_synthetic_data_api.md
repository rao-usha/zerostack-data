# SPEC_042 — Synthetic Data API (Phase A: Private Financials + Macro Scenarios)

**Type:** api_endpoint
**Status:** Active
**Plan:** PLAN_052 Phase A

---

## Overview

Two on-the-fly synthetic dataset generators exposed as POST endpoints. No pre-trained ML models required — both use parametric statistical methods (Gaussian copula + mean-reverting multivariate random walk) running entirely on ingested data already in the DB.

---

## Endpoint 1: POST /api/v1/synthetic/private-financials

### Request
```json
{
  "sector": "industrials",
  "revenue_min_millions": 10,
  "revenue_max_millions": 500,
  "n_companies": 20,
  "seed": null
}
```

### Response
```json
{
  "status": "ok",
  "sector": "industrials",
  "peer_count": 38,
  "synthetic_count": 20,
  "methodology": "gaussian_copula_from_peers",
  "ratio_stats": {
    "gross_margin": {"mean": 0.29, "std": 0.07},
    "ebitda_margin": {"mean": 0.14, "std": 0.05},
    "net_margin": {"mean": 0.07, "std": 0.04}
  },
  "companies": [
    {
      "company_id": "synth_001",
      "revenue_millions": 125.3,
      "gross_profit_millions": 36.3,
      "ebitda_millions": 17.5,
      "net_income_millions": 8.8,
      "gross_margin_pct": 29.0,
      "ebitda_margin_pct": 14.0,
      "net_margin_pct": 7.0
    }
  ]
}
```

### Behavior
- Query `public_company_financials` for annual (FY) records filtered by revenue range
- Filter by sector using company name keyword matching (fallback: sector priors if < 5 peers)
- Compute financial ratios (gross_margin, ebitda_margin, net_margin) for each peer
- Fit multivariate Gaussian on the ratios using historical correlation structure
- Sample n_companies synthetic ratio sets via Cholesky decomposition
- Apply business constraints: margins clamped to realistic ranges, net_margin ≤ ebitda_margin
- Revenue sampled from log-normal distribution within [revenue_min, revenue_max]

---

## Endpoint 2: POST /api/v1/synthetic/macro-scenarios

### Request
```json
{
  "n_scenarios": 100,
  "horizon_months": 24,
  "series": ["DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT"],
  "seed": null
}
```

### Response
```json
{
  "status": "ok",
  "n_scenarios": 100,
  "horizon_months": 24,
  "training_history_months": 384,
  "series": ["DFF", "DGS10", "UNRATE", "CPIAUCSL", "UMCSENT"],
  "current_values": {"DFF": 3.64, "DGS10": 4.44, "UNRATE": 4.1, "CPIAUCSL": 2.7, "UMCSENT": 57.0},
  "methodology": "mean_reverting_correlated_random_walk",
  "scenarios": [
    {
      "scenario_id": 0,
      "paths": {
        "DFF": [3.64, 3.58, 3.51, ...],
        "DGS10": [4.44, 4.47, 4.43, ...]
      }
    }
  ],
  "summary": {
    "DFF": {"p10_terminal": 1.8, "p50_terminal": 3.1, "p90_terminal": 5.2},
    "DGS10": {"p10_terminal": 3.2, "p50_terminal": 4.1, "p90_terminal": 5.8}
  }
}
```

### Behavior
- Query all FRED tables for requested series (DFF, DGS10, DGS2, UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO)
- Compute monthly mean-reversion speed (theta) and long-run mean per series from historical data
- Compute cross-series correlation matrix of monthly changes
- For each scenario: simulate forward using Ornstein-Uhlenbeck mean-reversion with correlated shocks
- Apply hard constraints: rates clipped to [0, 25], unemployment [0, 25], CPI [0, 30], UMCSENT [20, 120]
- Return full scenario paths + terminal value percentile summary

---

## Acceptance Criteria

- [ ] Both endpoints return 200 with valid JSON on first call
- [ ] `synthetic_count` matches the requested `n_companies`
- [ ] `scenarios` list length matches `n_scenarios`
- [ ] Path length for each series = `horizon_months`
- [ ] No NaN/null values in output
- [ ] Gross margin always ≤ 100%, net margin always ≤ ebitda margin
- [ ] If `peer_count` < 5, response includes `"fallback": "sector_priors"` field
- [ ] Deterministic with same seed
- [ ] n_companies > 100 → 422 validation error
- [ ] n_scenarios > 1000 → 422 validation error
