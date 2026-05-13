# SPEC 051 — rev_02 Iteration 2: TabDDPM Inference Seeding + SIC-2; Diffusion-TS Student-t Noise

**Status:** Draft
**Task type:** service
**Date:** 2026-05-13
**Test file:** tests/test_spec_051_rev02_iter2.py (skeleton only — TDD waived for time)
**Plan:** [PLAN_062 rev_02](../plans/PLAN_062_learned_synthetic_generators_rev_02.md) Step 3 — iteration 2 of 3

## Goal

Address two specific failure modes from rev_02 iteration 1 validation:

1. **TabDDPM**: pooled correlation Frobenius 0.26 (>0.20 threshold) + inference non-determinism (KS results jitter across runs). Per-sector marginals already pass (6/6).
2. **Diffusion-TS**: 1/8 series passing strict criteria; 1/8 passing with kurt-relaxation. Root cause: Gaussian noise prior cannot produce the heavy-tailed monthly-change distribution observed in 7 of 8 FRED series. Switching to a Student-t prior is the targeted fix.

## Acceptance Criteria

### TabDDPM iteration 2
- [ ] Inference is deterministic given a fixed seed (`torch.manual_seed` set at the top of `sample()` / `validate()`).
- [ ] `sic_2 = LEFT(sic_code, 2)` column added to `public_company_financials` view (or computed in trainer SQL).
- [ ] `TabDDPMConfig.categorical_features` includes `sic_2` (gives ~60–80 buckets, between NAICS-2's 17 and SIC-4's 263).
- [ ] After retrain (5K epochs, seed=42), validation script reports:
  - [ ] Pooled correlation Frobenius < 0.20.
  - [ ] Per-sector ebitda_margin + net_margin KS pass on ≥5 of 6 eligible NAICS-2 sectors.

### Diffusion-TS iteration 2
- [ ] `DiffusionTSConfig` gains `use_student_t_noise: bool = True` + `t_noise_df: float = 4.0`.
- [ ] `q_sample()` draws noise from Student-t(df=t_noise_df) when flag is on, else falls back to standard normal.
- [ ] DDIM sampling loop uses the same noise prior.
- [ ] After retrain (5K epochs, seed=42), validation script reports:
  - [ ] ≥3 of 8 series pass strict criteria (UMCSENT + at least 2 others).
  - [ ] ≥5 of 8 series pass with the rev_02 kurt-relaxation for kurt > 30 series.

## Test Cases (skeleton — TDD waived; numbers asserted by validation script run)

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_tabddpm_sample_is_deterministic | Two `sample()` calls with same seed produce identical output |
| T2 | test_tabddpm_categorical_features_includes_sic2 | `TabDDPMConfig().categorical_features` contains 'sic_2' |
| T3 | test_public_company_financials_has_sic_2 | View column `sic_2` exists and is 2 chars wide |
| T4 | test_diffusion_ts_student_t_noise_has_heavier_tail | Sampled noise from `_sample_noise(...)` with df=4 has kurtosis > 3.5 |
| T5 | test_diffusion_ts_config_student_t_flags | `DiffusionTSConfig().use_student_t_noise` defaults True, `t_noise_df` defaults 4.0 |

(Tests are skeleton-only; spec marked draft. Validation script run is the system-level acceptance gate, per rev_02 Step 3's "validate after each retrain" loop.)

## Rationale

### Why seed inference?
Iteration 1 results jittered (NAICS=31 ebitda KS p flipped 0.056 → 0.024 between runs). For a small held-out test set (893 rows), the diffusion noise sampling adds enough variance to flip pass/fail on the bubble. Deterministic inference makes iteration debugging tractable.

### Why SIC-2 (not deeper NAICS or NAICS-4)?
- NAICS-2 (17 buckets): too coarse; pools heterogeneous industries (steel + pharma + aerospace all in 31).
- SIC-4 (263 buckets): too sparse; ~27 rows per bucket on average, can't learn intra-bucket correlations.
- SIC-2 (~60–80 buckets): middle ground. Already derivable from existing `sic_code` field, no new ingest required.

### Why Student-t noise (df=4)?
Student-t(df=4) has tail behavior heavier than Gaussian but with finite variance. df=4 is the lowest df with finite kurtosis (kurt = 6 + 6/(df-4) → ∞ as df→4 from above; df=5 gives finite kurt=9). df=4 chosen as a balance: heavy enough to teach the model that tails matter, light enough to avoid training instability. The diffusion process's noise prior fundamentally determines what marginal noise statistics the model is exposed to during training — switching the prior changes the implicit "what counts as plausible noise" distribution.

## Stop-and-checkpoint

If iteration 2 doesn't move Frobenius below 0.20 OR Diffusion-TS doesn't reach ≥5/8 with relaxation, iteration 3 of 3 will be: (a) per-sector mixture-of-experts for TabDDPM, (b) explicit shock-event injection in Diffusion-TS post-sampling. After that, surface to user for criteria-relaxation decision per rev_02.
