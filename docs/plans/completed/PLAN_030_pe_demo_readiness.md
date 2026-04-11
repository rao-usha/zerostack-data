# PLAN 030: PE Demo Readiness

## Goal
Build the backend pieces needed for a compelling PE demo: financial benchmarking endpoint, exit readiness scoring, and seeded demo data so screens aren't empty.

## Context
From GTM strategy:
- Target buyer: mid-market PE firms ($1-5B AUM)
- Two demo stories: (1) finding & winning a deal, (2) preparing & executing an exit
- Critical blocker: most features need seeded demo data to show value
- Current DB: 5.4K portfolio companies, 3.6K fund investments, 2.6K PE people — but no benchmarking or exit scoring

## Steps

### 1. PE Demo Data Seeder
- [ ] Create `app/sources/pe/demo_seeder.py`
- [ ] Seed 3 fictional PE firms with realistic profiles (names, AUM, strategy, vintage years)
- [ ] Seed 8-12 portfolio companies per firm with financial metrics (revenue, EBITDA, margins, growth rates, employee count)
- [ ] Seed fund performance data (IRR, MOIC, DPI) across 2-3 fund vintages
- [ ] Seed 15-20 PE people with titles and firm associations
- [ ] Seed deal pipeline entries (3-5 active deals per firm at different stages)
- [ ] Add API endpoint: `POST /api/v1/pe/seed-demo`
- [ ] Ensure seeder is idempotent (upsert, not duplicate)

### 2. Financial Benchmarking Endpoint
- [ ] Create `app/core/pe_benchmarking.py`
- [ ] `GET /api/v1/pe/benchmarks/{company_id}` — compare a portfolio company against:
  - Industry median (from seeded + real data)
  - Portfolio average (across the firm's holdings)
  - Top quartile threshold
- [ ] Metrics: revenue growth, EBITDA margin, revenue per employee, debt/EBITDA
- [ ] Response includes percentile rank, trend (improving/declining), and peer comparison table
- [ ] Add `GET /api/v1/pe/benchmarks/portfolio/{firm_id}` — full portfolio heatmap data

### 3. Exit Readiness Score
- [ ] Create `app/core/pe_exit_scoring.py`
- [ ] `GET /api/v1/pe/exit-readiness/{company_id}` — composite score (0-100) based on:
  - Financial health (revenue growth, margins, predictability) — 30%
  - Market position (market share proxy, competitive moat) — 20%
  - Management quality (key person risk, org completeness from people data) — 15%
  - Data room readiness (which data sources have coverage) — 15%
  - Market timing (sector multiples trend, M&A volume) — 10%
  - Regulatory risk (from EPA, OSHA, litigation data) — 10%
- [ ] Each sub-score has letter grade + explanation
- [ ] Overall grade: A (80+), B (65-79), C (50-64), D (35-49), F (<35)
- [ ] Response includes recommended actions to improve score

### 4. Deal Scoring Enhancement
- [ ] Enhance existing deal scoring to incorporate benchmarking data
- [ ] Add `acquisition_attractiveness` field that combines: financial benchmarks + exit potential + data coverage

### 5. Wire Up + Register
- [ ] Register all new endpoints in `app/main.py`
- [ ] Add OpenAPI tags for PE Benchmarking
- [ ] Run seeder to populate demo data
- [ ] Verify all endpoints return meaningful data

### 6. Testing
- [ ] Unit tests for benchmarking calculations
- [ ] Unit tests for exit readiness scoring
- [ ] Integration test: seed → benchmark → exit score pipeline

## Files touched
- `app/sources/pe/demo_seeder.py` — NEW
- `app/core/pe_benchmarking.py` — NEW
- `app/core/pe_exit_scoring.py` — NEW
- `app/api/v1/pe_benchmarks.py` — NEW (router)
- `app/main.py` — register router
- `tests/test_pe_benchmarking.py` — NEW
- `tests/test_pe_exit_scoring.py` — NEW

## Parallel work note
This tab only touches backend `app/` files (no frontend, no git ops). Safe to run alongside Tab 1 (commits) and Tab 2 (frontend).
Tab 1 should commit existing `app/main.py` changes BEFORE this tab modifies it, OR this tab should defer `main.py` registration to the end.
