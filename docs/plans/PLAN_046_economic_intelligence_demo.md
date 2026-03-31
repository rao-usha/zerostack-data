# PLAN_046 — US Economic Intelligence Demo + PE Data Products Layer

**Status:** Draft — awaiting approval
**Date:** 2026-03-30 (revised)
**Depends on:** FRED/BLS data already ingested; BEA_API_KEY in .env (regional ready to ingest)
**Feeds into:** PLAN_047 (Economic DQ), PLAN_048 (Macro PE Data Products)

---

## Goal

Build a standalone page and backend aggregation layer that transforms Census + FRED + BLS + BEA data into **PE-actionable intelligence** — a professional data product, not a guided tour.

The page is structured like a morning brief: you open it, you scan it top to bottom, you know what the macro environment is telling you. No hand-holding, no wizard steps. A GP who's been in the business for 20 years should feel like this was built for them.

It answers three questions PE investors ask every morning:
1. **"Is the macro environment favorable for deploying capital?"** → Rate & credit environment
2. **"Which sectors are gaining momentum vs. shedding labor?"** → Sector signals
3. **"Where is economic activity concentrating by geography?"** → Regional divergence

This is the live macro layer that feeds PLAN_048's PE data products (sector conviction scores, portfolio sensitivity, deal entry scoring).

**What this is not:** A guided product tour. Story Mode was considered and rejected — PE investors are sophisticated and don't need step-by-step navigation. The page structure and inline PE insight callouts provide sufficient context.

---

## What's Different From Existing Infrastructure

| Existing | What We're Building |
|---------|-------------------|
| `macro-cascade.html` | Causal network graph (academic) | **Live investor snapshot** (decision-focused) |
| `investor_intelligence.py` | Per-sector FRED/BLS pull | Aggregated cross-sector view for demo |
| Les Schwab report | Company-specific with macro context | Macro-first, company-secondary |
| `macro_cascade.py` | Simulation scenarios | Live current environment summary |

---

## Data Bootstrap (Run Before Demo)

All of these use existing endpoints — no new code needed for the data layer:

```bash
# 1. FRED — interest rates + economic indicators (likely already in DB)
POST /fred/ingest  {"category": "interest_rates"}
POST /fred/ingest  {"category": "economic_indicators"}
POST /fred/ingest  {"category": "housing_market"}
POST /fred/ingest  {"category": "consumer_sentiment"}

# 2. BLS — JOLTS (confirmed 2,290 rows), CES, CPI
POST /bls/ingest/jolts
POST /bls/ingest/ces
POST /bls/ingest/cpi

# 3. BEA Regional — state GDP + personal income (BEA_API_KEY confirmed in .env)
POST /bea/regional/ingest  {"table_name": "SAGDP2N", "geo_fips": "STATE", "year": "2019,2020,2021,2022,2023"}
POST /bea/regional/ingest  {"table_name": "SAINC1",  "geo_fips": "STATE", "year": "2019,2020,2021,2022,2023"}
POST /bea/regional/ingest  {"table_name": "SAINC51", "geo_fips": "STATE", "year": "2019,2020,2021,2022,2023"}

# 4. Census — state demographics
POST /census/state  {"survey": "acs5", "year": 2023, "table_id": "B01003"}   # Total Population
POST /census/state  {"survey": "acs5", "year": 2023, "table_id": "B23025"}   # Employment Status
POST /census/state  {"survey": "acs5", "year": 2023, "table_id": "B19013"}   # Median Household Income
```

All data-bootstrap calls can be triggered via a **"Refresh Data" button** on the demo page itself (calls the existing ingest endpoints, shows job progress).

---

## Backend — New Aggregation Endpoint

### New file: `app/api/v1/econ_snapshot.py`

Thin read layer over existing `fred_*`, `bls_*`, `bea_*`, `acs5_*` tables. No new models.

```
GET /econ-snapshot/macro          — FRED: key series latest values + 24-month history
GET /econ-snapshot/labor          — BLS: JOLTS + CES employment delta by sector (12-month)
GET /econ-snapshot/regional       — BEA regional: state GDP/income growth + Census demographics
GET /econ-snapshot/pe-signals     — Derived PE signals computed from the three above
GET /econ-snapshot/ingest-status  — Which tables are populated; returns ingest hints if missing
```

Each endpoint returns:
```json
{
  "status": "ok" | "partial" | "not_ingested",
  "as_of": "2026-03-28",
  "data": { ... },
  "missing": ["fred_interest_rates table is empty — POST /fred/ingest {category: 'interest_rates'}"]
}
```

#### `/econ-snapshot/macro` — Rate & Credit Environment
```json
{
  "kpis": {
    "fed_funds_rate":   {"value": 5.33, "prev_12m": 5.50, "delta": -0.17, "signal": "easing"},
    "ten_year_yield":   {"value": 4.21, "prev_12m": 4.58, "delta": -0.37},
    "yield_spread_10_2":{"value": -0.44, "inverted": true, "inverted_months": 18},
    "unemployment_rate":{"value": 4.1, "prev_12m": 3.8, "delta": 0.3, "signal": "rising"},
    "cpi_yoy_pct":      {"value": 3.1, "trend": "declining"}
  },
  "history": {
    "DFF": [{"date": "2024-03-01", "value": 5.33}, ...],   // 24 months
    "DGS10": [...],
    "DGS2": [...],
    "UNRATE": [...]
  }
}
```

#### `/econ-snapshot/labor` — Sector Labor Signals
```json
{
  "jolts": {
    "openings_rate": 5.3, "quits_rate": 2.1, "hires_rate": 3.6,
    "history": [{"date": "...", "openings": ..., "quits": ..., "hires": ...}]
  },
  "sector_employment_12m": [
    {"sector": "Leisure & Hospitality", "series_id": "CES7000000001",
     "current": 17200, "prev_12m": 16800, "delta": 400, "delta_pct": 2.4},
    ...
  ],
  "avg_hourly_earnings_yoy_pct": 4.1,
  "total_nonfarm_12m_delta": 2200000
}
```

#### `/econ-snapshot/regional` — Geographic Divergence
```json
{
  "states": [
    {
      "fips": "06", "name": "California",
      "gdp_growth_yoy_pct": 4.2,          // BEA SAGDP2N
      "personal_income_growth_pct": 5.1,  // BEA SAINC1
      "per_capita_income": 78400,          // BEA SAINC51
      "population": 39500000,              // Census B01003
      "median_hh_income": 84097,           // Census B19013
      "signal": "green"                    // green/yellow/red composite
    }, ...
  ],
  "top_5_growth": [...],
  "bottom_5_growth": [...]
}
```

#### `/econ-snapshot/pe-signals` — Derived PE Intelligence
Computed from the three above — no new data fetches:
```json
{
  "deal_environment": {
    "score": 45,           // 0-100; 0=hostile, 100=ideal
    "signal": "neutral",   // green/yellow/red
    "drivers": [
      {"factor": "Rate environment", "reading": "FFR 5.33% — elevated", "impact": "negative"},
      {"factor": "Yield curve",      "reading": "Inverted 18 months",   "impact": "negative"},
      {"factor": "Labor market",     "reading": "Quits rate 2.1% — normalized", "impact": "neutral"},
      {"factor": "Consumer sentiment", "reading": "Rising from 65→68",  "impact": "positive"}
    ]
  },
  "sector_momentum": [
    {"sector": "Healthcare",    "momentum_score": 78, "signal": "green"},
    {"sector": "Manufacturing", "momentum_score": 52, "signal": "yellow"},
    {"sector": "Retail",        "momentum_score": 34, "signal": "red"}
  ],
  "geographic_opportunity": [
    {"state": "Texas",   "opportunity_score": 82, "signal": "green"},
    {"state": "Florida", "opportunity_score": 76, "signal": "green"},
    {"state": "Illinois","opportunity_score": 41, "signal": "yellow"}
  ]
}
```

---

## Frontend — `frontend/economic-intelligence.html`

Dark slate theme, consistent with `pe-demo.html`. Six sections:

### Header Bar
Title: **US Economic Intelligence** — subtitle: "Live macro signals for private markets · FRED · BLS · BEA · Census"
Right: Data as-of date + "Refresh Data" button (triggers ingest jobs, shows spinner)

### Section 1: Macro Environment (Rate & Credit)
*"Is now a good time to deploy capital?"*

**Row: 5 KPI tiles**
- Fed Funds Rate (DFF) — value + 12m delta arrow
- 10Y Treasury Yield (DGS10)
- Yield Curve (10Y–2Y spread) — red chip if inverted, shows "inverted X months"
- Unemployment Rate (UNRATE)
- CPI YoY %

**Chart A:** Rate Timeline (36 months) — 3 lines: DFF / DGS10 / DGS2, red shading in inversion zones
**Chart B:** Yield Spread (36 months) — area chart, red below zero

**PE Insight Callout:**
Auto-generated from live KPIs. If inverted curve + FFR > 4%: "LBO financing costs elevated. Favor lower-leverage deals, seller financing, and equity-light business models."

---

### Section 2: Labor Market — Sector Momentum
*"Which industries are expanding vs. contracting?"*

**Chart C:** Employment Change by Sector (12-month net) — horizontal bar, green/red
Sectors: Leisure & Hospitality / Healthcare / Construction / Manufacturing / Retail / Finance / Professional Services / Education / Govt / Transport

**Chart D:** JOLTS Signal (24 months) — 3-line: openings rate / hires rate / quits rate
Interpretation: Quits rate = worker confidence / wage pressure leading indicator

**Stat Row:** Nonfarm payrolls 12m delta | JOLTS openings latest | Avg hourly earnings YoY %

**PE Insight Callout:** Auto-generated. If quits rate > 2.5%: "Tight labor market. Portfolio companies in labor-intensive sectors face EBITDA compression. Factor 5–8% wage inflation into deal models."

---

### Section 3: Geographic Divergence
*"Where is economic activity concentrating?"*

**Table: State Economic Health Rankings (all 50 states)**
Columns: State | GDP Growth YoY | Personal Income Growth | Per Capita Income | Population | Signal
Sortable. Color-coded signal chips (🟢🟡🔴).

**Chart E:** Top 10 / Bottom 10 States by Personal Income Growth (horizontal bar)

**PE Insight Callout:** Auto-generated from top states. "Fastest-growing markets: [TX, FL, AZ]. Deal sourcing in Sun Belt industrials and consumer services offers valuation discounts vs. coastal comps."

---

### Section 4: Census Demographics Snapshot
*"Population and income by state — sizing the addressable market"*

**Chart F:** Population by state (top 15) — horizontal bar
**Chart G:** Median Household Income distribution — bar chart sorted by income
**Metric row:** Highest income state | Lowest income state | National median

Note: Census ACS data is 1-year lag (2023 data = most recent as of 2025). Shown with as-of date.

---

### Section 5: PE Synthesis Panel
*"What does this mean for deploying capital right now?"*

3-column card grid:
- **Deal Environment Score** (0–100 gauge from `/pe-signals`) + drivers list
- **Sector Momentum** — ranked bar chart from `/pe-signals.sector_momentum`
- **Geographic Opportunity** — top 5 states with opportunity scores

Footer callout: "Signals are computed from live FRED/BLS/BEA data. Full macro → portfolio sensitivity in the [PE Intelligence →] module."

---

## Files

| File | Action |
|------|--------|
| `app/api/v1/econ_snapshot.py` | CREATE — 5 GET routes |
| `app/main.py` | MODIFY — register econ_snapshot router |
| `frontend/economic-intelligence.html` | CREATE — standalone demo |
| `frontend/index.html` | MODIFY — add gallery card |

**No new DB models. No schema changes.**

---

## Implementation Phases

| Phase | What | Notes |
|-------|------|-------|
| 1 | `/econ-snapshot/macro` + `/ingest-status` | FRED tables query, graceful fallback |
| 2 | `/econ-snapshot/labor` | BLS JOLTS + CES |
| 3 | `/econ-snapshot/regional` | BEA regional + Census ACS5 tables |
| 4 | `/econ-snapshot/pe-signals` | Derived signals, threshold logic |
| 5 | Frontend Sections 1–2 (macro + labor) | Chart.js wired to Phase 1–2 APIs |
| 6 | Frontend Sections 3–5 (regional + census + PE synthesis) | Phase 3–4 APIs |
| 7 | Data bootstrap trigger + Refresh button | Calls existing ingest endpoints |
| 8 | Register router + gallery card + restart verify | |

---

## Open Items Resolved

1. **BEA regional**: BEA_API_KEY confirmed in .env → ingest SAGDP2N + SAINC1 + SAINC51 as part of bootstrap
2. **Census integration**: Section 4 (state demographics: population, median HH income) — Census B01003 + B19013 at state level
3. **LAUS**: Add 51 state-level LAUS series (LASST{FIPS}0000000000003) to BLS client as part of PLAN_047 implementation — wire into regional section once available. Show graceful placeholder until then.
4. **PE framing**: Sections 2, 3, 4 all have PE Insight callouts; Section 5 is the full synthesis panel
