# PLAN_062 — Phase A1+A2: Learned Synthetic Generators (TabDDPM + Diffusion-TS)

## Context

The v1 synthetic stack (PLAN_052/053/056/057, all shipped) uses parametric models — multivariate Gaussian + log-normal revenue for private company financials, discrete Ornstein-Uhlenbeck for macro scenarios. They work, they have provenance, they unblock dead scorers. But they ceiling out on three structural limits:

1. **Multimodal distributions** — private company margins by sector are multi-modal (profitable scale + unprofitable growth); Gaussian collapses both into one hump.
2. **Stylized facts in financial time series** — real FRED-derived returns have heavy tails (kurtosis 5–15) and volatility clustering; O-U processes produce Gaussian-tailed, uncorrelated-vol paths that materially under-estimate stress scenarios.
3. **Conditional fidelity** — "generate a private $50M-revenue industrial in Texas, vintage 2021" is currently approximated by peer-pool averaging on coarse (sector, size) buckets. Diffusion models support joint conditioning on (NAICS-4, revenue, state, year) natively.

PLAN_052 specified the upgrade path. PLAN_062 executes it.

## Locked-in decisions (from user, 2026-05-10)

1. **Method for A1:** TabDDPM (not GReaT).
2. **GPU:** RTX 5070 (Blackwell, ~12 GB VRAM). Both models train concurrently, ~24 hr wall clock — see research doc §6 for VRAM math.
3. **v1 generators:** kept as `algorithm="parametric"` fallback. v2 is `algorithm="diffusion"` and becomes the default.
4. **Training data for A1:** EDGAR XBRL only (no external benchmarks like Cambridge Associates).
5. **Acceptance criteria:** tight, as drafted — no softening.

## Approach

Both phases train in parallel as separate processes on the 5070, share the validation harness, and ship behind an `algorithm` parameter on existing API routes. Full math, papers, hyperparameters, and failure-mode catalog in `docs/strategy/LEARNED_SYNTHETIC_GENERATORS_RESEARCH.md`.

### Prerequisites (status as of 2026-05-11 verification — see rev_01)

Verified against live DB on 2026-05-10. **Three of the data prerequisites are not met**; addressed in the new Week 1 data-ingest workstream (W1.A, W1.B, W1.C below) rather than as a blocker.

- [x] CUDA 12.4+ installed; `nvidia-smi` shows the RTX 5070 (verifiable; addressed in Week 2)
- [ ] **`public_company_financials` table missing entirely** — `sec_income_statement` and 4 related SEC tables exist but are 0 rows. PLAN_053 Phase C1 specced EDGAR XBRL bulk ingest but it never shipped. → **W1.A**
- [ ] **`fred_observations` table missing** — only `fred_interest_rates` exists with 7 interest-rate series. → **W1.C**
- [ ] **5 of 8 FRED target series absent** (UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO); the 3 we share (DFF, DGS10, DGS2) cover only ~10 years (2016+), not 60. → **W1.B**
- [ ] Python deps to add in Week 2: `torch>=2.4`, `einops`, `flash-attn>=3.0` (Blackwell), `wandb`, `tsgm`.
- [ ] Vendor TabDDPM source from yandex-research/tab-ddpm into `app/services/synthetic/training/tabddpm/` — Week 2.
- [ ] `app/services/synthetic/models/` artifacts directory created with .gitignore for `.pt` files but `.json` configs tracked — Week 2.

Additional finding: v1 generators currently fall back silently to hardcoded priors when their source tables are absent. Once W1 completes they will fit on real data for the first time. The "v1 fallback" we keep around (per Decision 3) becomes a *real* fitted-Gaussian-copula generator only after Week 1, not earlier.

## Phase A1 — Private Company Financials → TabDDPM

### Training
- Features (continuous): revenue, gross_margin, ebitda_margin, net_margin, total_assets, total_debt, employee_count
- Conditioning (categorical): NAICS-4 (~1000 codes, OTHER bucket for <50 occurrences), revenue_bucket (6 buckets), state_fips (50), year
- Preprocessing: QuantileTransformer to normal on continuous; one-hot + group-rare on categoricals; 99.5th-percentile clip
- Architecture: 4×1024 MLP, 1000 diffusion steps (cosine schedule), classifier-free guidance with `p_uncond=0.1`
- Training: bf16 mixed precision, batch=4096, Adam lr=1e-3, 30K epochs, checkpoint at 10K/20K/30K
- Wall clock: ~8–12 hrs on the 5070

### Inference
- DDIM-50 sampling (not 1000-step DDPM) — 10–20× faster, same quality
- Default guidance scale `w=1.5`, tunable per request
- Post-hoc: clamp revenue/total_assets to ≥0; reject samples with EBITDA margin > gross margin (~3% rejection rate)
- Generation latency target: <500ms for 100 samples

### Acceptance criteria
- KS test on (gross_margin, ebitda_margin, net_margin) vs 20% held-out → p > 0.05 in ≥8/10 sectors
- Frobenius norm of covariance difference vs held-out < 0.15
- ML utility (TSTR): classifier trained on synth predicts sector within 5% of classifier on real
- Generation latency < 500ms for 100 samples
- No memorized samples (cosine similarity to nearest training neighbor < 0.95 for ≥99% of generated rows)

### Files
- New: `app/services/synthetic/training/tabddpm_trainer.py`
- New: `app/services/synthetic/training/tabddpm/` (vendored yandex source, modified)
- New: `app/services/synthetic/private_company_financials_v2.py`
- New: `app/services/synthetic/models/` (artifacts directory, .pt + .json)
- New: `tests/test_synthetic_tabddpm.py`
- Modify: `app/api/v1/synthetic.py` — add `algorithm: Literal["parametric","diffusion"]` param to private-financials route
- Modify: `app/services/synthetic/validation.py` — add `ml_utility_test()` and `memorization_check()`

## Phase A2 — Macro Scenarios → Diffusion-TS

### Training
- 8 correlated series, monthly, 60-year history (1965–2025)
- Sequence length L=60 months, forecast horizon 24 months
- Train on **monthly changes**, not levels (critical — see research doc §3.5)
- Splits: 1965–2015 train / 2016–2020 val / 2021–2025 test (walk-forward only)
- Architecture: 4-layer Transformer, hidden_dim=256, 4 attention heads, 8 channels, cross-channel attention
- STL decomposition into trend/seasonal/residual; three parallel reconstruction heads
- Fourier loss `λ=0.01` (tune as secondary hyperparameter)
- 500 diffusion steps cosine schedule
- Training: bf16, batch=256 sequences, Adam lr=1e-4 with cosine warmup, 10K epochs, checkpoint at 3K/6K/10K
- Wall clock: ~16–24 hrs on the 5070

### Inference
- DDIM-50 sampling
- Conditional on current state (latest FRED values) — use Diffusion-TS context-conditioning mode, not unconditional
- Horizon capped at 24 months in v1 (longer horizons drift; revisit if needed)
- Output: 1000 paths × 24 months × 8 series per scenario request

### Acceptance criteria
- Kurtosis of monthly changes within ±0.3 of historical per series
- ACF of squared returns at lag-12 within 0.05 of historical (volatility clustering preserved)
- Cross-series correlation Frobenius vs historical < 0.10
- Discriminative score (TSGM eval) < 0.60 AUROC
- KS test on 24-month-forward terminals vs historical → p > 0.05

### Files
- New: `app/services/synthetic/training/diffusion_ts_trainer.py`
- New: `app/services/synthetic/macro_scenarios_v2.py`
- New: `tests/test_synthetic_diffusion_ts.py`
- Modify: `app/api/v1/synthetic.py` — add `algorithm` param to macro-scenarios route
- Modify: `app/services/synthetic/validation.py` — add `stylized_facts_test()` and `discriminative_score()`

## Shared workstream — validation extensions

Three new tests in `validation.py`, used by both phases:

| Function | What it measures | Used by |
|---|---|---|
| `ml_utility_test(real, synth, task)` | Classifier-on-synth vs classifier-on-real on a downstream task | Both |
| `discriminative_score(real, synth)` | Binary real-vs-synth AUROC (target ≤0.60) | Both (via TSGM for A2) |
| `stylized_facts_test(real, synth, series)` | Kurtosis, ACF of squared returns, gain-loss asymmetry | A2 |
| `memorization_check(synth, training_set)` | Nearest-neighbor similarity threshold | A1 |

## Unified runtime contract

Both v2 generators implement a single `LearnedSyntheticGenerator` ABC (see research doc §7.4). Same interface as future Phase B graph completion. Models loaded as singletons via `@lru_cache` at app startup; both resident on GPU concurrently (~110MB params combined — trivial).

### API surface

```
POST /api/v1/synthetic/private-financials
Body: {"algorithm": "diffusion" | "parametric", "n": 100,
       "naics_4": "5413", "revenue_bucket": "50-250M", ...}

POST /api/v1/synthetic/macro-scenarios
Body: {"algorithm": "diffusion" | "parametric", "n": 1000,
       "horizon_months": 24, "series": [...], ...}

GET /api/v1/synthetic/validate
  Returns side-by-side v1 vs v2 metrics in the unified dashboard.
```

### Provenance extension

Add column to `ingestion_jobs`:
```sql
ALTER TABLE ingestion_jobs
ADD COLUMN synthetic_model VARCHAR(64) NULL;
-- Set on synthetic jobs: 'parametric_v1' | 'tabddpm_v1' | 'diffusion_ts_v1'
```

Every generated sample carries a provenance token. If a training run is later invalidated, we can retract by `synthetic_model` value.

## Sequence (5 weeks — revised in rev_01)

```
Week 1 — DATA PREREQUISITES (added in rev_01)
  W1.A — SEC EDGAR XBRL bulk ingest (2 days, parallel with W1.B)
    Day 1: Build ingest job around existing app/sources/sec/ Company Facts client.
           Target: S&P 500 + Russell 2000 ≈ 2,500 CIKs × 5–10 yrs annual = 12,500–25,000 rows
           XBRL concepts: us-gaap:Revenues, CostOfRevenue, GrossProfit, OperatingIncomeLoss,
                          NetIncomeLoss, Assets, Liabilities, StockholdersEquity, NAICS from
                          submission metadata
    Day 2: Run ingest; populate public_company_financials (created in W1.C); QC for nulls,
           duplicates, anomalous outliers; verify ≥12.5K rows / ≥1,500 distinct CIKs.

  W1.B — FRED expansion + 50-year backfill (2 days, parallel with W1.A)
    Day 1: Add UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO to FRED ingest config.
           Backfill all 12 series (5 new + 7 existing) to 1965 or series-earliest.
           Aggregate daily → monthly (mean for rates, end-of-month for indices).
    Day 2: Verify ≥720 monthly observations on series that historically support it;
           MIN observation_date ≤ 1985 on ≥8 of 12 series.

  W1.C — Schema migration + v1 generator validation (1 day, after W1.A and W1.B land)
    - CREATE TABLE public_company_financials with schema v1 generator queries (see rev_01)
    - CREATE TABLE fred_observations; migrate existing fred_interest_rates rows
    - Keep fred_interest_rates as VIEW over fred_observations for backward compat
    - Verify v1 generators now actually fit on real data:
        curl /synthetic/private-financials → peer_count > 0 (not 0)
        curl /synthetic/macro-scenarios → training_history_months > 60 (not 0)
    - Opportunistic v1 fix: methodology field should report actual mode, not intended mode
      (currently returns "gaussian_copula_from_peers" even when fallback flag is set)

Week 2 — Scaffolding + training infra (was Week 1)
  Day 1-2: Add deps to requirements.txt; install on the 5070 dev machine
           CUDA 12.4 + flash-attn 3 smoke test (Blackwell BF16, torch.compile)
           Vendor TabDDPM source into app/services/synthetic/training/tabddpm/
           Set up app/services/synthetic/models/ artifacts dir with .gitignore
  Day 3-5: Build training data pipelines for both phases (reading from W1 tables)
           Set up W&B project nexdata-synthetic-v2
           Implement LearnedSyntheticGenerator ABC + ingestion_jobs.synthetic_model column

Week 3 — Train both models (CONCURRENT on the 5070) (was Week 2)
  Day 1:   Launch both training jobs as separate processes overnight
           A1 finishes first (~10 hrs), A2 finishes ~24 hrs
  Day 2-5: Validation runs against held-out; iterate hyperparams if criteria miss
           Common iteration: A2 Fourier-loss weight, A1 guidance scale at inference

Week 4 — Validation extensions + integration (was Week 3)
  Day 1-2: Implement ml_utility_test, discriminative_score, stylized_facts_test,
           memorization_check in validation.py
  Day 3-4: Wire v2 generators into API with algorithm-selection param
           Update synthetic-validation.html for v1-vs-v2 side-by-side
  Day 5:   A/B comparison; write up delta in validation_history/

Week 5 — Downstream wiring + demo (was Week 4)
  Day 1-2: Wire v2 macro scenarios into portfolio_stress_scorer (replace O-U path)
           Wire v2 private financials into synthetic seed scripts for PE demo
  Day 3-4: Documentation; demo script showing v1 vs v2 outputs side-by-side
  Day 5:   Final acceptance review, log to memory/logs/, decide on commit grouping
```

## Stop-and-checkpoint triggers

The "build until done" mode pauses for human input when any of:
- Prerequisite blocker: `public_company_financials` empty or unusable → ingest decision needed
- Training doesn't converge in 2 attempts with reasonable hyperparams → write findings, pause
- v2 fails acceptance criteria after 3 hyperparam iterations → pause, decide whether to soften criteria or revisit method (GReaT instead of TabDDPM)
- Memorization check fails (>1% of samples too close to training NN) → pause for ethical/IP review
- Disk fills (model checkpoints + training data caching) → pause for cleanup decision

## Out of scope

- **Phase B** (graph completion via RotatE/CAPER) — separate plan, will reuse the LearnedSyntheticGenerator ABC built here.
- **Phase C** (healthcare revenue MIWAE, deal valuation TabDDPM, geographic kriging) — separate plans.
- **Phase D** (JobGen + ESCO for hiring signals) — separate plan.
- **DP layer** — privacy budget not required for public XBRL/FRED training data. Defer until Phase B touches private peer benchmarks.
- **Frontend overhaul** — only minimal v1-vs-v2 side-by-side addition to existing validation dashboard. No new pages.

## Verification

End of week 4:
1. `pytest tests/test_synthetic_tabddpm.py tests/test_synthetic_diffusion_ts.py -v` — all pass
2. `curl /api/v1/synthetic/private-financials -d '{"algorithm": "diffusion", ...}'` returns valid samples in <500ms
3. `curl /api/v1/synthetic/macro-scenarios -d '{"algorithm": "diffusion", ...}'` returns 1000-path scenario in <30s
4. `curl /api/v1/synthetic/validate` shows v2 meeting acceptance criteria on both phases
5. Portfolio stress scorer with `algorithm="diffusion"` produces wider, fatter-tailed stress distributions than v1
6. v1 still works when `algorithm="parametric"` selected

## Estimated effort

~5 weeks (revised from 4 in rev_01) · ~18–25 new files · ~3,000–4,000 LOC including tests + W1 ingest + schema migrations · 1 overnight training run minimum, expect 2–3 iterations.

## References

- `docs/strategy/LEARNED_SYNTHETIC_GENERATORS_RESEARCH.md` — full math, papers, infrastructure, failure modes (599 lines)
- `docs/plans/completed/PLAN_052_data_interplay_synthetic_extensions.md` — origin research roadmap
- `docs/plans/completed/PLAN_053_synthetic_seed_strategy.md` — v1 scaffolding + EDGAR XBRL Phase C1 (specced, never shipped — see rev_01)
- `docs/plans/PLAN_057_statistical_validation_dashboard.md` — current validation engine

## Revisions

| Rev | Date | Summary | Doc |
|-----|------|---------|-----|
| 01 | 2026-05-11 | Insert Week 1 data prerequisites. Verification surfaced that `public_company_financials` and `fred_observations` tables don't exist; SEC ingest never shipped; FRED has only 7 interest-rate series for ~10 years (5 of 8 target series absent). Plan length 4→5 weeks. | [PLAN_062_rev_01](PLAN_062_learned_synthetic_generators_rev_01.md) |
| 02 | 2026-05-12 | W3 v1 training succeeded mechanically but failed acceptance criteria — TabDDPM 1/4 metrics passed (no sector conditioning), Diffusion-TS 1/8 series passed (vanilla DDPM attenuates fat tails). Adds targeted W3.5 retrain pass: SEC submission-metadata ingest for NAICS conditioning, min-SNR-γ loss weighting, importance sampling on t, larger model. Step 0 commits the 31 uncommitted files before retraining. | [PLAN_062_rev_02](PLAN_062_learned_synthetic_generators_rev_02.md) |

## Addenda

| # | Date | Summary | Doc |
|---|------|---------|-----|
| 1 | 2026-05-11 | Synthetic Consumer Crowd — distilled small model trained on ~2,000 GPT-4o teacher calls; uses PLAN_062 Phase A1 TabDDPM output as input scenarios. 5 outcome variables (purchase intent, WTP, sentiment, WOM, churn). Generic vertical-agnostic v1; PE-firm sellable. Independent ~3-week workstream (Phases 1–3 ship + Phase 4 follow-on calibration). | [PLAN_062_addendum_1_synthetic_consumer_crowd](PLAN_062_addendum_1_synthetic_consumer_crowd.md) |
