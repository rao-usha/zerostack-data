# SPEC 049 — Phase A Data Prerequisites (W1 of PLAN_062)

**Status:** Draft
**Task type:** service
**Date:** 2026-05-11
**Test file:** tests/test_spec_049_phase_a_data_prereqs.py
**Plan:** [PLAN_062](../plans/PLAN_062_learned_synthetic_generators.md) (rev_01) — Week 1

## Goal

Populate the database tables and time-series coverage required for PLAN_062 Phase A1 (TabDDPM private company financials) and A2 (Diffusion-TS macro scenarios). Specifically: trigger the existing-but-never-run SEC EDGAR XBRL bulk ingest at S&P 500 + Russell 2000 scale, expand FRED ingest to 5 new macro series with 50-year backfill, and create the `public_company_financials` view + `fred_observations` consolidated table that the v1 generators reference but that don't exist today.

## Acceptance Criteria

### W1.A — SEC EDGAR XBRL bulk ingest
- [ ] `sec_financial_facts` populated with ≥500K rows across ≥1,500 distinct CIKs (covers S&P 500 + most of Russell 2000)
- [ ] `sec_income_statement` / `sec_balance_sheet` / `sec_cash_flow_statement` each populated with ≥12,500 rows across ≥1,500 CIKs / ≥5 fiscal years
- [ ] EDGAR rate limit (10 req/sec) respected; no 429s in job logs
- [ ] Ingest job records `data_origin='real'` in `ingestion_jobs`

### W1.B — FRED expansion + backfill
- [ ] 5 new series ingested: UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO
- [ ] All 12 series (5 new + 7 existing interest rates) have data back to 1965 or series-earliest historical start
- [ ] ≥720 monthly observations available for series that historically support it (DFF, UNRATE, CPIAUCSL)
- [ ] Daily-frequency series have a daily→monthly aggregation path documented (in the query layer, not stored)

### W1.C — Schema migration + v1 generator validation
- [ ] `public_company_financials` table exists with the schema the v1 generator queries (revenue_usd, gross_profit_usd, ebitda_usd, net_income_usd, fiscal_period, period_end_date, company_name) and is populated by a view or materialized view over `sec_income_statement` + SEC submission metadata
- [ ] `fred_observations` table or view exists, unifying all FRED series across categories (series_id, date, value, ...)
- [ ] Existing `fred_interest_rates` remains queryable (view over `fred_observations` for backward compat with existing scorers)
- [ ] `curl /api/v1/synthetic/private-financials -d '{"sector":"industrials","n":5}'` returns `peer_count > 0` (no longer `0`)
- [ ] `curl /api/v1/synthetic/macro-scenarios -d '{"series":["DFF","UNRATE"]}'` returns `training_history_months > 60` (no longer `0`)
- [ ] v1 generator `methodology` field reports actual mode used (`peer_fitted_gaussian_copula` when fitted, `sector_priors_fallback` when fallback) instead of always lying as `gaussian_copula_from_peers`

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_public_company_financials_view_schema | The view has exactly the columns v1 generator queries; types match |
| T2 | test_public_company_financials_view_returns_data_after_ingest | Sample insert into sec_income_statement + sec_financial_facts NAICS metadata → view returns the expected denormalized row |
| T3 | test_fred_observations_table_includes_existing_series | After migration, all 7 existing interest-rate series + 5 new series are queryable from `fred_observations` |
| T4 | test_fred_interest_rates_backward_compat_view | Old `fred_interest_rates` queries still return the same rows (view over new table) |
| T5 | test_v1_private_financials_reports_actual_mode_when_fitted | When peer query returns rows, methodology = `peer_fitted_gaussian_copula` |
| T6 | test_v1_private_financials_reports_fallback_when_no_peers | When peer query returns empty, methodology = `sector_priors_fallback` |
| T7 | test_v1_macro_scenarios_reports_actual_mode_when_calibrated | When FRED history > 24 months, methodology = `fred_calibrated_ou_walk` |
| T8 | test_v1_macro_scenarios_reports_fallback_when_no_history | When FRED query returns empty, methodology = `default_params_fallback` |
| T9 | test_sec_xbrl_ingest_respects_rate_limit | Mock SECClient verifies sleep/throttle calls keep us ≤ 10 req/sec |
| T10 | test_fred_ingest_handles_new_series_in_config | Adding a new series_id to the FRED config triggers a new table OR adds rows to fred_observations without breaking existing |

## Rubric Checklist (generic — no service rubric exists yet)

- [ ] Follows existing patterns: BaseAPIClient, BaseSourceIngestor for ingest paths
- [ ] All SQL parameterized (`:param` style) — no string concatenation of user input
- [ ] Bounded concurrency for SEC client (already enforced by BaseAPIClient + EDGAR limit)
- [ ] Errors caught and logged; job status updated correctly (PENDING → RUNNING → SUCCESS/FAILED)
- [ ] No PII collection beyond what SEC explicitly publishes (CIK, company name, NAICS, financials — all public)
- [ ] No paywalled or login-required content accessed
- [ ] `data_origin='real'` on all ingestion jobs created by W1.A and W1.B
- [ ] Schema changes use the existing pattern (define in models.py if Base.metadata.create_all is the only migration path)
- [ ] Doesn't break existing scorers that query `fred_interest_rates` or `sec_financial_facts`
- [ ] Documented in W1.C: how the new `methodology` field values map to underlying state (peer count, FRED history months)

## Design Notes

### W1.A — SEC EDGAR XBRL bulk ingest

**Reuse, don't rebuild:** `app/sources/sec/ingest_xbrl.py` + `xbrl_parser.py` + `client.py` + `models.py` already exist. We need to:
1. Build a **CIK list builder** that produces the target list (S&P 500 + Russell 2000 = ~2,500 CIKs). Sources:
   - Hardcoded constant list (simple but stale)
   - Pull from `pe_portfolio_companies` for any company with `ticker` + map ticker → CIK via SEC company tickers JSON
   - Pull from EDGAR's `company_tickers.json` filtered by market cap (preferred — authoritative + fresh)
2. Build a **job orchestrator** that submits N parallel ingest jobs through the existing `BaseSourceIngestor` infrastructure, respecting per-source rate limits
3. Wire as a new endpoint `POST /api/v1/sec/ingest-xbrl-bulk` taking `{cik_list_source, year_range}` params

**No new XBRL parsing code needed.** Concepts already extracted by the existing parser cover what we need.

### W1.B — FRED expansion + backfill

**Reuse, don't rebuild:** `app/sources/fred/client.py` + `ingest.py` + `metadata.py` already exist. The pattern is `fred_{category}` table-per-category. We need to:
1. Add 5 series to `metadata.py` under appropriate categories:
   - UNRATE → new category `fred_employment` (or extend existing labor category if present)
   - CPIAUCSL → new category `fred_inflation`
   - UMCSENT → new category `fred_sentiment`
   - INDPRO → new category `fred_industry`
   - DCOILWTICO → new category `fred_commodities`
2. Trigger ingest for each new category with `observation_start=1965-01-01` (or earlier)
3. Trigger backfill for existing `fred_interest_rates` series with the same early date

**Backfill safe by upsert:** FRED ingest already uses ON CONFLICT (series_id, date) so re-running with an earlier start date just adds older rows.

### W1.C — Schema migration + v1 generator validation

**`public_company_financials` as a VIEW, not a table.** This is the key design choice:

```sql
CREATE OR REPLACE VIEW public_company_financials AS
SELECT
    s.cik,
    s.company_name,
    s.fiscal_period,
    s.period_end_date,
    s.revenue_usd,
    s.gross_profit_usd,
    s.ebitda_usd,
    s.net_income_usd,
    s.total_assets_usd,
    s.total_liabilities_usd,
    s.total_equity_usd,
    sub.naics_code,
    sub.state_fips,
    s.employee_count
FROM sec_income_statement s
LEFT JOIN sec_submission_metadata sub ON sub.cik = s.cik
WHERE s.fiscal_period = 'FY'
  AND s.revenue_usd > 0;
```

A view is preferred over a table because:
- No duplicated storage
- Always in sync with the underlying SEC ingest
- v1 generator code already queries it as if it were a table (PostgreSQL views are transparent to SQL)
- Easier to evolve later

If `sec_income_statement` doesn't have all the columns the view needs (e.g., `ebitda_usd` might be derived not stored), the view does the arithmetic.

**`fred_observations` as a TABLE** (not view), because we want to UNION-ALL across the per-category tables and indexing/perf matters for the macro v1 generator hot path:

```sql
CREATE TABLE fred_observations (
    series_id TEXT NOT NULL,
    date DATE NOT NULL,
    value NUMERIC,
    realtime_start DATE,
    realtime_end DATE,
    ingested_at TIMESTAMP DEFAULT NOW(),
    category TEXT,  -- which fred_{category} table it came from
    PRIMARY KEY (series_id, date)
);
-- Populate from existing fred_interest_rates and new category tables.
-- Long-term: change ingestor to write directly here, deprecate per-category tables.
```

Alternative: `fred_observations` as a VIEW with UNION ALL across all `fred_{category}` tables. Cleaner but UNION ALL queries are slow at scale. Decision: **VIEW for v1**, materialize to a table later if perf demands.

**v1 generator methodology fix:** Change `private_company_financials.py` and `macro_scenarios.py` to track which path produced the output and report it in the response:
- Private: `peer_fitted_gaussian_copula` (peer_count > 0) vs `sector_priors_fallback` (peer_count == 0)
- Macro: `fred_calibrated_ou_walk` (history_months ≥ 24) vs `default_params_fallback` (history_months < 24)

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/sources/sec/cik_list.py` | Create | CIK list builder (S&P 500 + Russell 2000 via EDGAR company_tickers.json) |
| `app/sources/sec/bulk_ingest_orchestrator.py` | Create | Orchestrator that submits parallel XBRL ingest jobs per CIK |
| `app/api/v1/sec.py` | Modify | Add `POST /api/v1/sec/ingest-xbrl-bulk` endpoint |
| `app/sources/fred/metadata.py` | Modify | Add 5 new series IDs under their categories |
| `app/services/synthetic/private_company_financials.py` | Modify | Methodology field reports actual mode |
| `app/services/synthetic/macro_scenarios.py` | Modify | Methodology field reports actual mode |
| `app/core/migrations/049_public_company_financials_view.sql` | Create | SQL migration creating the view |
| `app/core/migrations/049_fred_observations_view.sql` | Create | SQL migration creating fred_observations view and fred_interest_rates back-compat |
| `app/main.py` | Modify | Apply migrations at startup (after `Base.metadata.create_all()`) |
| `tests/test_spec_049_phase_a_data_prereqs.py` | Create | Skeleton tests (10 cases above) |

## Feedback History

_No corrections yet._
