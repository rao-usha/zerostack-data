# PLAN 027 — Company 360 & AI Investment Thesis

## Phase 1: Unified Company 360 Endpoint
- Create `app/core/pe_company_360.py` — aggregates 12 data sections per company
- Calls: benchmark_company, score_exit_readiness, score_deal, ComparableTransactionService.get_comps, buyer analysis, leadership, competitors, alerts, snapshots, pipeline deals
- Add `GET /pe/companies/{company_id}/360` to pe_benchmarks.py

## Phase 2: AI Investment Thesis Generator
- Create `app/core/pe_thesis_generator.py` — structured LLM prompt → thesis
- Add `pe_investment_theses` table (company_id, thesis JSON, generated_at, model_used, cost_usd)
- Cache: return cached thesis if <24h old
- Add `GET /pe/companies/{company_id}/thesis` and `POST .../thesis/refresh`

## Phase 3: Pre-Generate Demo Theses
- Hardcoded realistic thesis content in demo_seeder.py for 5 key companies
- No live LLM call needed for demos

## Phase 4: Verification
- Curl 360 endpoint, verify all sections
- Run full test suite
