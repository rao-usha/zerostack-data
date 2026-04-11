# PLAN_057 — Statistical Validation Dashboard

**Status:** CODE COMPLETE (agent crashed before commit — files exist in worktree agent-aa464879)

## Context

Synthetic data needs to be provably representative, not just random numbers. This plan builds a validation service that runs statistical tests (KS, chi-squared) against expected distributions, and a frontend dashboard that visualizes distribution overlays, test results, and correlation preservation. Future-ready for algorithm swapping.

## What Was Built

### Backend: `app/services/synthetic/validation.py` (651 lines)
- `SyntheticValidator` class with per-generator validation methods
- Statistical tests: chi-squared (categorical), KS test (continuous), correlation comparison
- Threshold: p >= 0.05 = PASS, 0.01 <= p < 0.05 = WARN, p < 0.01 = FAIL
- Algorithm registry pattern for future swapping

### API Endpoints: `app/api/v1/synthetic.py` (+83 lines)
- `GET /synthetic/validate` — summary scoreboard (all 4 generators)
- `GET /synthetic/validate/{generator}` — full validation with histograms, tests, correlations
- `GET /synthetic/validate/{generator}/compare` — algorithm comparison (future-ready)

### Frontend: `frontend/synthetic-validation.html` (534 lines)
- Dark theme dashboard with summary scoreboard
- Per-generator tabs with distribution overlays, test results, correlation heatmaps
- Controls for seed and n_samples

### Tests: `tests/test_plan_057_synthetic_validation.py` (150 lines)

### Dependency: `requirements.txt` — added `scipy>=1.11.0,<2.0.0`

## What Gets Validated

| Generator | Field | Test | Expected Distribution |
|-----------|-------|------|----------------------|
| Job Postings | seniority_level | Chi-squared | SENIORITY_WEIGHTS |
| Job Postings | department | Chi-squared | SECTOR_ROLE_MIX |
| Job Postings | posted_days_ago | KS test | Piecewise uniform (70/20/10) |
| LP-GP | lp_type | Chi-squared | LP_TYPE_CONFIG weights |
| LP-GP | AUM per type | KS test | Uniform(aum_range) |
| LP-GP | commitment_pct | KS test | Uniform(0.01, 0.05) |
| Macro Scenarios | terminal values | KS test | O-U stationary N(μ, σ²/2θ) |
| Macro Scenarios | correlation | Frobenius norm | Cross-series correlation matrix |
| Private Financials | margins | KS test | N(mean, std) from SECTOR_PRIORS |
| Private Financials | revenue | KS test | LogNormal |
| Private Financials | correlation | Frobenius norm | SECTOR_PRIORS[sector]["corr"] |

## Worktree

Branch: `worktree-agent-aa464879`
Path: `.claude/worktrees/agent-aa464879/`
Files are untracked (agent crashed before git add). Need to copy into main.
