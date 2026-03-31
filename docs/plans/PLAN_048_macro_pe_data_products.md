# PLAN_048 — Macro PE Data Products

**Status:** Draft — awaiting approval
**Date:** 2026-03-30
**Depends on:** PLAN_046 (data bootstrap + econ_snapshot), PLAN_047 (DQ + LAUS)
**Builds on:** Existing `macro_cascade_engine.py`, `investor_intelligence.py`, `pe_models.py`

---

## Goal

Transform the raw economic data layer into **five PE-actionable data products** that create tangible value in three buying scenarios:

1. **GP**: "Help me find the right sector to source deals in right now"
2. **LP**: "How exposed is this fund's portfolio to the current macro environment?"
3. **Operating Partner**: "Which portfolio companies need a macro briefing before the board meeting?"

These are not dashboards — they are **scored, opinionated outputs** that take a position on where capital should go, backed by live macro data.

---

## The Five Data Products

### Product 1 — Deal Environment Score
*"Is now a good time to deploy capital in this sector?"*

A **0–100 score** for each of the 9 sectors in `sector_registry.py`, computed from live FRED + BLS + BEA signals. Updated on every new data ingestion.

**Inputs:**
| Factor | Data Source | Weight | Signal Direction |
|--------|-------------|--------|-----------------|
| Financing cost (DFF) | FRED | 25% | Higher rate → lower score |
| Credit cycle (yield spread) | FRED (DGS10 - DGS2) | 20% | Inverted → lower score |
| Sector labor momentum (CES 12m Δ) | BLS | 20% | Growing → higher score |
| Consumer confidence (UMCSENT) | FRED | 15% | Rising → higher score |
| Exit multiple environment (GDP growth) | FRED / BEA | 10% | Accelerating → higher score |
| Input cost pressure (PPI / CPI) | BLS | 10% | Falling → higher score |

**Output per sector:**
```json
{
  "sector": "industrials",
  "deal_score": 62,
  "grade": "B",
  "signal": "neutral",
  "drivers": [
    {"factor": "Financing cost", "score": 45, "reading": "FFR 5.33% — elevated; LBO cost +180bps vs 2021"},
    {"factor": "Sector momentum", "score": 78, "reading": "Manufacturing +127K jobs (12m); reshoring tailwind"},
    {"factor": "Input costs", "score": 58, "reading": "PPI manufacturing declining — margin relief"}
  ],
  "recommendation": "Selective deployment. Favor asset-light industrials with pricing power. Avoid highly levered targets.",
  "updated_at": "2026-03-28"
}
```

**Grade scale:** A (80–100) → compelling deploy; B (60–80) → selective deploy; C (40–60) → hold; D (<40) → avoid

**New endpoint:** `GET /pe/macro/deal-scores` — all 9 sectors
**New endpoint:** `GET /pe/macro/deal-scores/{sector}` — detail for one sector

---

### Product 2 — Portfolio Macro Sensitivity Report
*"How exposed is our portfolio to the current macro environment?"*

Uses the existing `macro_cascade_engine.py` and `company_macro_linkages` table. For each portfolio company with linkages defined, computes its sensitivity to current macro conditions.

**Inputs:**
- `company_macro_linkages` (existing table) — which macro nodes affect this company
- `macro_nodes` (existing table) — current values for each node (live FRED/BLS values)
- `cascade_scenarios` — run two standard scenarios: `base_case` (current conditions) and `stress_test` (-50bps FFR, +1pp unemployment)

**Output — Portfolio Sensitivity Index:**
```json
{
  "portfolio_id": "fund_2",
  "as_of": "2026-03-28",
  "sensitivity_index": 0.52,   // 0=no macro risk, 1=extreme sensitivity
  "companies": [
    {
      "company_id": 142, "company_name": "Acme Distributors",
      "sector": "industrials",
      "macro_risk_score": 67,
      "top_risks": [
        {"macro_node": "Federal Funds Rate", "linkage": "cost_driver",
         "current_value": 5.33, "impact": "Financing cost elevated — EBITDA -12% stress case"},
        {"macro_node": "PPI Intermediate",   "linkage": "cost_driver",
         "current_value": 2.1, "impact": "Input cost declining — favorable margin trajectory"}
      ],
      "stress_test_ebitda_impact_pct": -8.4
    }
  ],
  "fund_level_summary": {
    "avg_macro_risk": 54,
    "high_risk_companies": 3,
    "rate_sensitive_pct": 0.67,    // % of portfolio revenue exposed to interest rate risk
    "consumer_sensitive_pct": 0.34
  }
}
```

**New endpoint:** `GET /pe/macro/portfolio-sensitivity/{fund_id}`
**New endpoint:** `POST /pe/macro/portfolio-sensitivity/{fund_id}/refresh` — re-run cascade

**Bootstrap step:** `company_macro_linkages` must be seeded. New seeding function: `POST /pe/macro/seed-linkages/{company_id}` — extracts linkages from SEC 10-K risk factors using LLM (extends existing SEC ingest).

---

### Product 3 — Sector Macro Briefing (Auto-Generated Report)
*"Give me a one-page macro brief for the board meeting"*

Auto-generates an HTML report for any sector, combining the deal score, relevant FRED/BLS charts, and a plain-language narrative. Builds on the existing report template system.

**New template:** `app/reports/templates/macro_sector_brief.py`

**Structure:**
- **Title**: "Macro Brief: {Sector} — {Date}"
- **Section 1**: Deal Score card (large number + grade + 3-driver summary)
- **Section 2**: 2 live charts — sector employment trend + relevant FRED indicator
- **Section 3**: "What This Means" — 4 bullets auto-generated from deal score drivers
- **Section 4**: "Watch Points" — 3 conditions that would change the score

**New endpoint:** `POST /reports/generate {"template": "macro_sector_brief", "sector": "industrials"}`

Report stored in existing `reports` table, downloadable via existing `/reports/{id}/download`.

---

### Product 4 — LBO Entry Scoring (Macro-Adjusted)
*"Is this a good entry point in the current cycle?"*

Given a company ticker (or NAICS code) and assumed deal metrics (EV/EBITDA, leverage, sector), score the attractiveness of entry considering macro cycle timing.

**Inputs:**
- Target entry EV/EBITDA multiple
- Sector
- Leverage ratio (assumed Debt/EBITDA)
- Hold period (3, 5, or 7 years)

**Computation:**
```
debt_cost = DFF + credit_spread_est(leverage)   // from FRED + sector credit premium
equity_required_irr = sector_hurdle_rate(sector)
exit_multiple_est = entry_multiple × macro_multiple_adjustment_factor
macro_adjustment = f(GDP_trend, sector_momentum, sentiment)

LBO_IRR = f(entry_ev, hold_period, debt_cost, exit_multiple_est, EBITDA_growth_est)
entry_score = f(LBO_IRR, benchmark_irr, current_deal_environment_score)
```

All intermediate values shown — no black box.

**Output:**
```json
{
  "sector": "industrials",
  "entry_ev_ebitda": 8.5,
  "leverage_debt_ebitda": 4.0,
  "hold_years": 5,
  "macro_inputs": {
    "debt_cost_pct": 8.2,
    "exit_multiple_est": 7.8,   // discounted from entry; macro-adjusted
    "ebitda_cagr_est": 6.5
  },
  "irr_estimate_pct": 18.4,
  "benchmark_irr_pct": 20.0,    // sector median from pe_benchmarks
  "entry_score": 68,
  "grade": "B",
  "verdict": "Attractive — 180bps below sector median IRR but favorable margin expansion.",
  "sensitivity": {
    "+100bps_rates":   {"irr_impact": -1.2, "new_score": 61},
    "exit_7x_vs_8x":  {"irr_impact": -3.8, "new_score": 52}
  }
}
```

**New endpoint:** `POST /pe/macro/lbo-score`

---

### Product 5 — Weekly Macro Intelligence Digest
*"What changed this week that matters for private markets?"*

A lightweight auto-generated HTML digest, delivered weekly (APScheduler), summarizing:
1. Which macro indicators moved materially (>0.5σ from trailing 12-week average)
2. Deal score changes week-over-week for all 9 sectors
3. Top 3 geographic markets by economic momentum
4. One "event of the week" callout (e.g., "Fed held rates — deal environment unchanged")

**Generated by:** New service `app/services/macro_digest_service.py`
**Schedule:** Every Monday 7:00 AM (APScheduler)
**Stored in:** `reports` table, template `macro_weekly_digest`
**Endpoint:** `GET /pe/macro/digest/latest` — most recent digest HTML

---

## Architecture

```
FRED + BLS + BEA + Census
        │ (ingestion)
        ▼
fred_* / bls_* / bea_* / acs5_* tables
        │
        ├── econ_snapshot.py (PLAN_046) ──► economic-intelligence.html
        │
        ├── econ_dq_service.py (PLAN_047) ──► data quality governance
        │
        └── MacroPEDataProducts (this plan)
                │
                ├── DealEnvironmentScorer ──► /pe/macro/deal-scores
                │        └── sector_registry.py (9 sectors × 6 factors)
                │
                ├── PortfolioSensitivityEngine ──► /pe/macro/portfolio-sensitivity
                │        └── macro_cascade_engine.py (existing BFS)
                │        └── company_macro_linkages (existing table)
                │
                ├── MacroSectorBrief ──► /reports/generate (template)
                │        └── reports/templates/macro_sector_brief.py
                │
                ├── LBOEntryScorer ──► /pe/macro/lbo-score
                │        └── pe_benchmarks.py (benchmark IRR)
                │
                └── MacroDigestService ──► /pe/macro/digest/latest
                         └── APScheduler weekly job
```

---

## Files

| File | Action |
|------|--------|
| `app/services/deal_environment_scorer.py` | CREATE — Product 1 scoring logic |
| `app/services/portfolio_sensitivity_service.py` | CREATE — Product 2 cascade wrapper |
| `app/services/lbo_entry_scorer.py` | CREATE — Product 4 IRR + macro adjustment |
| `app/services/macro_digest_service.py` | CREATE — Product 5 weekly digest generator |
| `app/reports/templates/macro_sector_brief.py` | CREATE — Product 3 report template |
| `app/api/v1/pe_macro.py` | CREATE — All Product 1/2/4/5 endpoints |
| `app/main.py` | MODIFY — Register pe_macro router + APScheduler digest job |
| `frontend/economic-intelligence.html` | MODIFY — Wire in deal scores from Product 1 |
| `frontend/index.html` | MODIFY — Add PE Macro gallery section |

---

## Implementation Phases

| Phase | Product | Priority |
|-------|---------|----------|
| 1 | Deal Environment Scorer (Product 1) | P1 — most demo-able |
| 2 | `/pe/macro/deal-scores` endpoint + wire to economic-intelligence.html | P1 |
| 3 | Macro Sector Brief template (Product 3) | P1 — generates PDF for partners |
| 4 | LBO Entry Scorer (Product 4) | P2 — deal committee tool |
| 5 | Portfolio Sensitivity Engine (Product 2) + seed-linkages | P2 |
| 6 | Weekly Macro Digest (Product 5) + APScheduler | P3 |

---

## Demo Script — "The 10-Minute PE Pitch"

Using just Products 1 and 3 (Phases 1–3):

> "Before every deal committee, the team pulls this up. You see the Deal Environment Score — Industrials is a B (62/100). Rates are elevated but sector labor momentum is strong. Reshoring tailwind. Click 'Generate Brief' and you get this one-page PDF that goes in the IC pack. Same data, automatically updated every time we refresh FRED and BLS data. Your analyst doesn't have to build this manually anymore — it's live."

This is the "Bloomberg Terminal for private markets" story, made concrete.

---

## Dependencies on Existing Infrastructure

- `macro_cascade_engine.py` — BFS cascade simulation (Product 2) ✅ exists
- `sector_registry.py` — 9 sectors with FRED/BLS mappings ✅ exists
- `investor_intelligence.py` — per-sector data pull ✅ exists
- `pe_benchmarks.py` — benchmark IRR by sector ✅ exists
- `reports/builder.py` — report generation engine ✅ exists
- `company_macro_linkages` table ✅ exists (needs seed data)
- APScheduler infrastructure ✅ exists (P1/P2/P3 jobs pattern)
