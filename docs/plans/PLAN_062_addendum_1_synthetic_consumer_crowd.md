# PLAN_062 Addendum 1 — Synthetic Consumer Crowd (Distilled from GPT-4o)

**Status:** Draft
**Date:** 2026-05-11
**Parent plan:** [PLAN_062](PLAN_062_learned_synthetic_generators.md)
**Relationship to parent:** Layered on top — uses PLAN_062 Phase A1 (TabDDPM) outputs as input scenarios for the consumer simulator.

---

## Context

PLAN_062 generates synthetic *companies* (TabDDPM on EDGAR XBRL) and synthetic *macro paths* (Diffusion-TS on FRED). This addendum adds a synthetic *consumer response* layer: given a hypothetical product/brand event affecting a hypothetical company, predict how a representative population of consumers would react.

Conceptually similar to what Aaru does (LLM-generated synthetic populations) but with a key implementation difference: we don't run GPT-4o for every query. Instead we run it once to seed ~2,000 high-quality (persona × scenario) responses, then distill that dataset into a small tabular model that serves identical-shape predictions at <1ms per query and effectively zero variable cost.

Trade-off (from the parent research doc and the SYNTHETIC_CROWD research): distillation cannot make the simulator more accurate than the teacher signal. GPT-4o's consumer-behavior predictions have known biases (stereotyping, ideological skew, hyper-rationality per the Columbia 2026 mega-study). Distillation inherits these. We accept this trade for the cost economics — the goal is a *useful directional simulator for PE underwriting*, not a substitute for real consumer research.

## Decisions locked in (from user, 2026-05-11)

| # | Question | Choice |
|---|---|---|
| 1 | Consumer subdomain | **Generic** consumer crowd (vertical-agnostic; works across CPG, services, retail, hospitality) |
| 2 | Outcome variables | **5 variables:** purchase intent, willingness-to-pay (Δ%), sentiment, word-of-mouth amplitude, churn probability |
| 3 | Validation source | **Hybrid:** qualitative smell-test for v1 ship; real-data calibration (Yelp / Census / CFPB) as a Phase 4 follow-on milestone |
| 4 | Teacher model | **GPT-4o** (OpenAI). API cost ~$0.03/call at our prompt size |

## Sellable positioning

**Vendor:** PE firms underwriting consumer/CPG/services portfolio companies.
**Use case:** Before committing to a price increase, product launch, brand reposition, geographic expansion, or competitive response on a portfolio company, run a synthetic-crowd simulation across 50 consumer archetypes. Get a 5-variable response distribution in seconds; iterate on the proposed change to find the highest-confidence path before spending real money on customer research.
**Economics:** ~$0.001 per query for the buyer (vs. ~$0.03/query if they called GPT-4o directly per request, or thousands of $ for traditional consumer research).
**Differentiation from Aaru/Simile:** They're horizontal market-research replacements; we're vertical-specific to PE portfolio-company decisions. Their populations are general; our personas + scenarios are designed for PE's actual use cases.

---

## Approach — 3 phases, then a follow-on calibration milestone

### Phase 1 — Static dataset generation (~1 week)

**Goal:** Produce `synthetic_consumer_responses_v1.parquet` — a 2,000-row dataset of (persona, scenario) → 5 outcome variables, ready to ship as a one-time data purchase and to serve as distillation training data.

**Personas (50 archetypes).** Defined as combinations of 3 demographic dims × 3 psychographic dims, chosen to span the consumer space rather than full cartesian product:
- Demographics: age bracket (4: 18–29, 30–44, 45–59, 60+), income bracket (4: <$50K, $50–100K, $100–200K, >$200K), geography (3: urban, suburban, rural)
- Psychographics: price sensitivity (low/med/high), brand loyalty (low/med/high), novelty-seeking (low/med/high)
- Hand-curated 50 archetypes that span the space, anchored on Pew typology + Nielsen Generations segmentation (public references)

**Scenarios (40 templates).** Distributed across the categories PE actually decides on:
- Pricing (8): price up 10/20/30%, price down 5/10%, premium tier launch, discount tier launch, dynamic pricing intro
- Product (8): new SKU, line extension, premium add, value add, packaging change, ingredient change, format change, bundle launch
- Brand (8): PE acquisition, rename, repositioning, scandal, recall, sustainability claim, celebrity endorsement, controversy
- Geographic (4): urban expansion, rural expansion, international, new channel
- Competitor (6): new entrant, exit, price cut, product launch, scandal, acquisition
- Operational (6): loyalty program, app, subscription, faster delivery, return policy, service change

50 × 40 = 2,000 cells exactly.

**Teacher prompt.** For each cell, prompt GPT-4o with:
1. Persona context (demographic + psychographic profile, ~150 tokens)
2. Scenario description (what's happening, ~100 tokens)
3. Optional brand context — for some scenarios, inject a TabDDPM-generated synthetic company description from PLAN_062 Phase A1 (links the two systems; ~200 tokens)
4. Structured output schema (JSON with 5 keys + per-variable confidence)

**Cost.** 2,000 calls × ~2K input + 500 output tokens × $0.03/call ≈ **$60 + retries**. Comfortably within budget.

**Output schema (parquet columns):**
- `persona_id` (1–50)
- `scenario_id` (1–40)
- `tabddpm_company_id` (nullable — set when brand context was used)
- Persona descriptive cols: `age_bracket`, `income_bracket`, `geography`, `price_sensitivity`, `brand_loyalty`, `novelty_seeking`
- Scenario descriptive cols: `category`, `event_type`, `magnitude` (nullable, for pricing scenarios)
- Outcome variables (each 0–1 normalized or signed):
  - `purchase_intent` (0–1)
  - `wtp_delta` (-1 to +1, fraction change from baseline)
  - `sentiment` (-1 to +1)
  - `wom_amplitude` (0–1, likelihood of telling others)
  - `churn_probability` (0–1, only meaningful for existing-customer scenarios; null otherwise)
- Confidence cols: `*_confidence` per outcome (0–1)
- Metadata: `teacher_model`, `teacher_call_id`, `generated_at`

### Phase 2 — Distillation (~3–5 days)

**Architecture choice:** Tabular regressor per outcome variable (gradient-boosted trees via LightGBM), NOT a fine-tuned LLM.

**Rationale.** The persona + scenario inputs are well-structured categorical features (already encoded in the parquet schema). A tabular regressor:
- Trains in seconds (no GPU needed)
- Runs in microseconds at inference (CPU-only, no torch dependency)
- Doesn't add transformer baggage to the deployed API
- Captures the (persona × scenario) interaction patterns that drive the response variables — exactly what GBTs do well

If we later find tabular insufficient, we can train a small LLM as Phase 2.1.

**Training pipeline:**
- 80/20 train/test split on the 2,000 rows, stratified by scenario category
- One model per outcome variable (5 models total)
- LightGBM with early stopping; ~100 boosting rounds typical
- Feature importance + SHAP per outcome (interpretability bonus)
- Persisted as `app/services/synthetic/consumer_crowd/models/distilled_v1/{outcome}.lgb`

**Distillation acceptance criteria:**
- Test RMSE ≤ 1.5× "predict outcome mean" baseline for each of the 5 variables
- No outcome has test RMSE > training RMSE × 2 (overfitting check)
- Predictions are deterministic on identical input (no per-call randomness)
- Inference latency < 5ms for 50 personas × 1 scenario (= one full "crowd response")

### Phase 3 — Live API + augmentation (~3–4 days)

**Endpoint surface:**

```
POST /api/v1/synthetic-crowd/consumer-response
Body: {
    "scenario": {
        "category": "pricing",
        "event_type": "price_up",
        "magnitude": 0.10,
        "company_description": "<optional free text or TabDDPM company_id>"
    },
    "personas": "all" | [persona_id, ...] | "sample:N"
}
Response: {
    "responses": [
        {"persona_id": 1, "purchase_intent": 0.42, "wtp_delta": -0.08, ...},
        ...
    ],
    "aggregate": {
        "purchase_intent_mean": 0.51,
        "purchase_intent_std": 0.18,
        ...
    },
    "provenance": {
        "model_version": "distilled_v1",
        "teacher_model": "gpt-4o",
        "teacher_data_size": 2000
    }
}
```

```
POST /api/v1/synthetic-crowd/teacher-passthrough  (admin-only)
Body: same as above
```

The teacher passthrough lets us A/B compare distilled output vs. GPT-4o on demand for ongoing quality auditing. Rate-limited so we don't accidentally burn $$ on it.

```
GET /api/v1/synthetic-crowd/personas
GET /api/v1/synthetic-crowd/scenarios
GET /api/v1/synthetic-crowd/dataset-export?format=parquet
```

The export endpoint is the "static dataset" deliverable shape — buyers download the parquet directly.

**Model loaded as singleton at API startup** (5 LightGBM models, ~5MB total — trivial RAM).

### Phase 4 — Real-data calibration (FOLLOW-ON milestone, ~1 week, not blocking ship)

Before claiming "internally useful" in the vendor pitch, compare distilled predictions against real consumer signal:

- Pull Yelp business reviews for relevant categories (medspa, fitness, casual dining, etc.) — Yelp client already exists in `app/sources/yelp/`
- For each business, build a "scenario" matching its profile (recent menu change, price change, ownership change events from news) and a "persona mix" matching local demographics (Census ACS)
- Compare distilled `sentiment` predictions against actual Yelp review sentiment distribution
- Compare distilled `wom_amplitude` against review-count velocity
- Calibration target: Pearson r ≥ 0.4 on a 1K-business holdout (modest target reflecting the "internally useful" bar)
- If calibration fails: refit the regressors on the union of Phase 1 synthetic + Phase 4 real, or revisit teacher prompts

This phase is the "vendor-grade pitch material" enabler — without it the product is "interesting but unvalidated." Worth shipping as a follow-up rather than blocking the v1 release.

---

## File structure

```
app/services/synthetic/consumer_crowd/
    __init__.py
    personas.py              # 50 archetype definitions
    scenarios.py             # 40 scenario templates
    teacher_runner.py        # GPT-4o orchestrator with retry/cache
    dataset_builder.py       # Phase 1 main: produce parquet
    distiller.py             # Phase 2: train LightGBM models
    model.py                 # Loaded artifact + inference API
    aggregator.py            # Per-crowd aggregation (mean, std, percentiles)
    calibrator.py            # Phase 4: real-data calibration
    models/                  # .gitignored .lgb artifacts
        distilled_v1/
app/api/v1/synthetic_crowd.py    # FastAPI router
data/synthetic_consumer_responses/
    v1.parquet               # Phase 1 output (in repo or external storage TBD)
tests/test_synthetic_consumer_crowd.py
```

**Modified:**
- `app/main.py` — register router; load distilled models at startup
- `requirements.txt` — add `lightgbm>=4.0`, `openai>=1.50` (verify openai isn't already present)
- `app/services/synthetic/validation.py` — add per-outcome RMSE check for the distilled model

## Acceptance criteria

### Phase 1
- [ ] 2,000 rows in `synthetic_consumer_responses_v1.parquet`
- [ ] All 50 personas + 40 scenarios represented (no empty cells)
- [ ] Per-row schema validation: 5 outcomes + 5 confidences all populated where applicable
- [ ] Teacher cost ≤ $100 (allowing 2× buffer for retries/exploration)
- [ ] Spot check: 100 rows pass plausibility review (the qualitative smell test from #3)

### Phase 2
- [ ] All 5 distilled models trained, persisted to `models/distilled_v1/`
- [ ] Each outcome: test RMSE ≤ 1.5× predict-mean baseline
- [ ] No outcome: test RMSE > 2× training RMSE
- [ ] Inference latency for 50-persona crowd: < 5ms

### Phase 3
- [ ] All 5 endpoints respond with valid schemas
- [ ] API p95 latency < 100ms for full 50-persona crowd response
- [ ] Provenance fields populated on every response
- [ ] Teacher passthrough endpoint locked behind admin auth

### Phase 4 (follow-on)
- [ ] Yelp signal pulled for 1K businesses in relevant categories
- [ ] Distilled `sentiment` vs. Yelp sentiment: Pearson r ≥ 0.4
- [ ] Distilled `wom_amplitude` vs. Yelp review-count velocity: Pearson r ≥ 0.4
- [ ] Calibration report written to `docs/strategy/CONSUMER_CROWD_CALIBRATION.md`

## Sequencing

```
Week 1 — Phase 1: Dataset generation
  Day 1: Persona library (50 archetypes from Pew/Nielsen anchors)
  Day 2: Scenario library (40 templates)
  Day 3: Teacher prompt design + GPT-4o orchestrator with retry/cache
  Day 4: Run full 2,000-cell generation; monitor cost
  Day 5: Schema validation; plausibility spot check on 100 rows

Week 2 — Phases 2 & 3: Distill + ship API
  Day 6: LightGBM per-outcome training pipeline; validation
  Day 7: SHAP + interpretability report; model artifact persistence
  Day 8: API router; load-singleton pattern; endpoint tests
  Day 9: Persona/scenario discovery endpoints; dataset export endpoint
  Day 10: End-to-end smoke test; provenance audit; v1 ship

Week 3 (follow-on, optional pre-pitch) — Phase 4: Real-data calibration
  Day 11–14: Yelp pull + business-to-scenario mapping
  Day 15: Calibration metrics; refit if needed; calibration doc
```

## Stop-and-checkpoint triggers

- Teacher cost > $200 (2× budget): pause and re-design prompts for efficiency
- Phase 1 plausibility spot check fails (< 80% plausible): pause — likely a prompt issue, fix before continuing to distill on bad teacher signal
- Phase 2 distillation RMSE > 1.5× baseline on any outcome: pause; either model architecture change (try MLP instead of LGB) or richer feature engineering
- Phase 4 calibration r < 0.2: pause — distilled model may be capturing teacher artifacts more than real consumer signal; consider whether we ship v1 with the "directional only" caveat or refit

## Out of scope for this addendum

- Vertical-specific persona libraries (medspa, fitness, etc.) — generic only per choice #1. Vertical libraries are a separate follow-on plan.
- Fine-tuned small LLM as the distilled artifact (tabular regressor is sufficient for the structured outcome space; LLM is overkill at v1 fidelity bar).
- Multi-step / longitudinal scenarios ("price up 10% in month 1, then product change in month 3, what's churn at month 6"). Single-event only at v1.
- Multi-persona-interaction effects (social network spread). Independent per-persona prediction at v1.
- Anything that depends on PII or de-identified consumer data — all training inputs are synthetic, all validation data is public (Yelp/Census).

## Open questions deferred

- Where does `v1.parquet` live? Repo (bloats git), Postgres (queryable but slow for export), or external object storage (cleanest but adds infra)? Decision deferred to start of Phase 1.
- Pricing model for the sellable product — per-query API key, dataset purchase, both? Deferred to GTM, not blocking the build.

## Estimated effort

~2 weeks for Phases 1–3 (ship-ready) + ~1 week for Phase 4 (vendor-grade calibration). Total ~3 weeks, ~12–18 new files, ~2,000–3,000 LOC.

## References

- `docs/strategy/SYNTHETIC_CROWD_INTELLIGENCE_RESEARCH.md` — Aaru/Simile competitive analysis + academic basis (LLM crowd-wisdom literature)
- `docs/strategy/SYNTHETIC_CROWD_IMPLEMENTATION_RESEARCH.md` — implementation approaches
- `docs/strategy/SYNTHETIC_CROWD_MATHEMATICS.md` — formal math for crowd aggregation
- `docs/plans/PLAN_062_learned_synthetic_generators.md` — parent plan (TabDDPM provides scenario context)
- Hinton, Vinyals, Dean — "Distilling the Knowledge in a Neural Network" — NeurIPS 2014 — [arXiv:1503.02531](https://arxiv.org/abs/1503.02531) (the canonical distillation reference)
