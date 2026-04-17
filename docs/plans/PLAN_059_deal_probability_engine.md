# PLAN_059: Deal Probability Engine

## Context

The platform has 67+ data sources joined under common entity IDs, 10+ scoring engines (all 0-100 weighted signal pattern), a geographic convergence detector (Deal Radar v1), and LLM-powered narrative generation. No competitor has this join. The Deal Probability Engine converts this data platform into an alpha engine by predicting P(transaction within 6-12 months) for every private company in the universe — selling the future instead of history. This is the $15K → $150K/seat price unlock.

**Key insight:** The existing `deal_scorer.py` scores deals already in pipeline. The new engine predicts transactions for companies NOT yet in any pipeline — that's the alpha.

---

## Phase 1: Foundation — Data Model + Company Universe + Signal Taxonomy

### 1A. Database Models (`app/core/probability_models.py`)

6 new tables, following `convergence_models.py` pattern:

| Table | Purpose |
|-------|---------|
| `txn_prob_companies` | Scored company universe (name, sector, NAICS, HQ state, revenue est, employee est, source) |
| `txn_prob_signals` | Per-company per-signal time-series snapshots (score, velocity, acceleration, confidence, details JSON) |
| `txn_prob_scores` | Composite probability per company per run (probability 0-1, raw 0-100, grade, convergence factor, signal chain JSON, narrative) |
| `txn_prob_outcomes` | Ground truth labels (acquired, IPO, recap, no_transaction — with what-we-predicted at announcement/6mo/12mo prior) |
| `txn_prob_alerts` | Threshold-crossing alerts (probability spike, grade change, new convergence, signal acceleration) |
| `sector_signal_weights` | Sector-specific weight overrides with version tracking |

### 1B. Company Universe Builder (`app/services/probability_universe.py`)

Sources:
- `pe_portfolio_companies` where active + private/PE-backed/VC-backed
- `industrial_companies` with employee_count > 50
- Recent Form D filers (12 months)
- Deduplicated via entity resolution

Daily refresh: add new, mark inactive.

### 1C. Signal Taxonomy (`app/ml/probability_signal_taxonomy.py`)

12 signals, each mapped to an existing scorer or new computer:

| Signal | Weight | Source | Reuse? |
|--------|--------|--------|--------|
| financial_health | 0.15 | `CompanyScorer` | Existing |
| exit_readiness | 0.12 | `pe_exit_scoring` | Existing |
| acquisition_attractiveness | 0.12 | `AcquisitionTargetScorer` | Existing |
| exec_transition | 0.10 | `ExecSignalScorer` | Existing |
| sector_momentum | 0.10 | `pe_market_signals` table | Query |
| diligence_health | 0.08 | `CompanyDiligenceScorer` | Existing |
| insider_activity | 0.08 | `insider_transactions` | **New** |
| hiring_velocity | 0.07 | `job_postings` + exec signals | **New** |
| deal_activity_signals | 0.05 | Form D + corp dev job titles | **New** |
| innovation_velocity | 0.05 | USPTO patents + GitHub | **New** |
| founder_risk | 0.05 | `people` + `leadership_changes` | **New** |
| macro_tailwind | 0.03 | `convergence_regions` + macro | **New** |

Sector overrides: Healthcare boosts exec_transition + diligence; Tech boosts innovation + hiring; Industrial boosts macro + exit_readiness.

### Phase 1 Files
- **New:** `app/core/probability_models.py`, `app/services/probability_universe.py`, `app/ml/probability_signal_taxonomy.py`
- **Modify:** `app/main.py` (table creation import)

---

## Phase 2: Signal Engine — Score Every Company

### 2A. New Signal Computers (`app/services/probability_signal_computers.py`)

5 new computers for signals not covered by existing scorers:
- `InsiderActivityComputer` — net buy/sell ratio in trailing 90 days
- `HiringVelocityComputer` — senior hiring intensity + corp dev postings + headcount growth
- `DealActivitySignalComputer` — Form D capital raises + corp dev job titles + pe_deals in sector
- `FounderRiskComputer` — founder age > 60 + co-founder departures + succession indicators (CFO/COO presence)
- `MacroTailwindComputer` — convergence region score for company's state + sector momentum
- `InnovationVelocityComputer` — patent filing rate + GitHub commit velocity

### 2B. Core Engine (`app/services/probability_engine.py`)

Class `TransactionProbabilityEngine` — the central orchestrator:

```
score_company(company_id) → full signal chain + calibrated probability
score_universe(batch_size=100) → batch all active companies
get_rankings(sector, min_prob, limit) → leaderboard
get_company_detail(company_id) → full breakdown
get_signal_history(company_id, signal_type, periods=12) → time-series
```

**Composite formula:**
```
raw = sum(signal.score * sector_weight[signal.type]) 
convergence_bonus = 1 + (signals_above_60_count * 0.08)
composite = min(100, raw * convergence_bonus)
```

**Probability calibration (Phase 2 — sigmoid):**
```
P = 1 / (1 + exp(-k * (composite - x0)))
Default: k=0.08, x0=55 (sector-specific, updated in Phase 4)
```

**Velocity/acceleration:** computed from previous signal snapshot:
```
velocity = (current_score - previous_score) / periods_elapsed
acceleration = (current_velocity - previous_velocity) / periods_elapsed
```

### 2C. API Endpoints (`app/api/v1/transaction_probability.py`)

10 endpoints under `/api/v1/txn-probability/`:

1. `POST /score/{company_id}` — score single company
2. `POST /scan` — batch score universe
3. `GET /rankings` — top companies by probability (filter: sector, min_prob, grade)
4. `GET /company/{id}` — full signal chain detail
5. `GET /company/{id}/history` — signal time-series
6. `GET /company/{id}/signals` — individual signal breakdown
7. `GET /stats` — dashboard (universe size, avg prob, hot count, alert count)
8. `GET /sectors` — per-sector summary
9. `GET /alerts` — recent alerts
10. `GET /methodology` — static methodology doc

### 2D. Synthetic Data (`app/services/synthetic/transaction_probability.py`)

Bootstrap: 200 synthetic companies, signal snapshots at T-12/T-6/T-now, 30 labeled outcomes.

### Phase 2 Files
- **New:** `app/services/probability_signal_computers.py`, `app/services/probability_engine.py`, `app/api/v1/transaction_probability.py`, `app/services/synthetic/transaction_probability.py`
- **Modify:** `app/main.py` (router registration + OpenAPI tag)
- **Reuse (no modification):** `app/ml/company_scorer.py`, `app/ml/acquisition_target_scorer.py`, `app/services/exec_signal_scorer.py`, `app/services/company_diligence_scorer.py`, `app/core/pe_exit_scoring.py`

---

## Phase 3: Intelligence Layer — Narratives, Convergence, Alerts

### 3A. Narrative Generator (`app/services/probability_narrative.py`)

LLM-powered (via existing `LLMClient`):
- `generate_company_narrative(company_id)` — 3-5 sentence explainer
- `generate_deal_memo(company_id)` — 6-section memo (executive summary, signal analysis, comparable transactions, risk factors, recommended action, timing thesis)
- `generate_sector_briefing(sector)` — sector-level top movers summary

### 3B. Convergence Detector (`app/services/probability_convergence.py`)

Named convergence patterns — the product differentiator:

| Pattern | Required Signals | Meaning |
|---------|-----------------|---------|
| classic_exit_setup | exec_transition ≥60, financial_health ≥70, sector_momentum ≥65 | Management buildout + strong fundamentals + hot sector |
| founder_transition | founder_risk ≥70, exec_transition ≥50, deal_activity ≥40 | Aging founder + succession + deal exploration |
| distress_opportunity | diligence_health ≤40, insider_activity selling, hiring restructuring | Weak fundamentals + insider selling + turnaround hiring |
| sector_wave | sector_momentum ≥75, macro_tailwind ≥60, deal_activity ≥40 | Sector momentum + macro tailwinds + deal signals |

### 3C. Alert Engine (`app/services/probability_alerts.py`)

Rules: probability spike (>15% delta), grade upgrade, new convergence pattern, signal acceleration, new universe entry.

### 3D. NLQ (`app/services/probability_nlq.py`)

Following `deal_radar_nlq.py` pattern — Claude-parsed filters with whitelist validation + keyword fallback.

### 3E. Additional Endpoints (added to existing router)

- `POST /company/{id}/narrative` — AI narrative
- `POST /company/{id}/memo` — deal memo
- `GET /convergences` — companies with active convergence patterns
- `POST /query` — natural language query
- `GET /sector/{sector}/briefing` — sector AI briefing

### 3F. Scheduled Jobs (added to `app/main.py` scheduler)

- Daily 1 AM: universe refresh
- Daily 2 AM: score_universe batch
- Weekly: sector briefings

### Phase 3 Files
- **New:** `app/services/probability_narrative.py`, `app/services/probability_convergence.py`, `app/services/probability_alerts.py`, `app/services/probability_nlq.py`
- **Modify:** `app/api/v1/transaction_probability.py` (5 more endpoints), `app/main.py` (scheduled jobs)

---

## Phase 4: Learning Loop — Outcome Tracking + Model Calibration

### 4A. Outcome Tracker (`app/services/probability_outcome_tracker.py`)

- `scan_for_outcomes()` — scan `pe_deals`, SEC filings, news for transactions involving universe companies
- `backfill_predictions()` — for each outcome, record what we predicted at announcement / 6mo / 12mo prior
- `get_labeled_dataset()` — build features + labels DataFrame for training

### 4B. Model Calibrator (`app/ml/probability_calibrator.py`)

- Platt scaling (logistic regression) to map raw composite → calibrated P
- Isotonic regression as alternative
- Sector-specific calibration parameters
- Calibration evaluation (Brier score, reliability diagrams)

### 4C. Weight Optimizer (`app/ml/probability_weight_optimizer.py`)

- `optimize_weights()` — find signal weights maximizing AUC-ROC (requires 50+ labeled outcomes/sector)
- `compute_signal_importance()` — univariate AUC per signal
- `run_backtest()` — walk-forward validation

### 4D. ML Model (`app/ml/probability_model.py`)

Activated when 200+ labeled samples exist:
- LightGBM gradient-boosted model
- Features: 12 scores + 12 velocities + 12 accelerations + convergence count = 37 features
- SHAP values for explainability
- Auto-fallback to rule-based when insufficient data

### 4E. Eval Suite (`tests/integration/test_probability_engine.py`)

- Hard: probability ∈ [0,1], signal chain decomposes correctly, all 12 signals present
- Soft: high-health companies above sector median
- LLM judge: narrative references top contributing signals

### Phase 4 Files
- **New:** `app/services/probability_outcome_tracker.py`, `app/ml/probability_calibrator.py`, `app/ml/probability_weight_optimizer.py`, `app/ml/probability_model.py`, `tests/integration/test_probability_engine.py`
- **Modify:** `app/services/probability_engine.py` (swap in ML calibrator when available), `app/main.py` (monthly/quarterly calibration jobs)

---

## File Summary

| Phase | New Files | Modified |
|-------|-----------|----------|
| 1 | 3 (models, universe, taxonomy) | main.py |
| 2 | 4 (computers, engine, API, synthetic) | main.py |
| 3 | 4 (narrative, convergence, alerts, NLQ) | main.py, API router |
| 4 | 5 (tracker, calibrator, optimizer, model, tests) | engine, main.py |
| **Total** | **16 new files** | **3 modified** |

Zero existing scorers modified — all reused via composition.

---

## Verification Plan

### Phase 1
- Tables created, FK relationships valid
- Universe builder populates 100+ companies from existing PE portfolio data

### Phase 2
- Score 10 known PE companies → all 12 signals populate
- Signal chain JSON decomposes to composite score correctly
- Probability between 0 and 1, varies across companies
- All 10 API endpoints respond with valid schemas

### Phase 3
- Narrative for 5 companies references actual signal data
- Convergence detector finds patterns in synthetic data
- Alerts fire on probability spikes
- NLQ parses "top healthcare companies by probability"

### Phase 4
- Synthetic outcomes → Brier score improves after Platt calibration
- Weight optimizer produces reasonable weights (no signal > 0.4)
- ML model only activates with 200+ samples gate
- Backtest precision > 0.3 at recall 0.5

---

## Implementation Order

Phase 1 → Phase 2 → Phase 3 → Phase 4 (strict dependency chain). Within phases, parallelize where possible (e.g., signal computers can be built independently).

**Estimated scope:** ~16 new files, ~3,500-4,500 lines of code. Phase 1-2 are the core product. Phase 3 is the intelligence wrapper. Phase 4 is the learning flywheel.
