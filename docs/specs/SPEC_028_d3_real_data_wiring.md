# SPEC 028 — Wire Remaining D3 Visualizations to Real Data

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-03-27
**Test file:** tests/test_spec_028_d3_real_data_wiring.py

## Goal

Wire the 5 remaining fake D3 visualizations (orgchart, radar, map, network, sankey) to real database
data by adding new backend endpoints and updating frontends to fetch from those endpoints, following
the same pattern established in PLAN_034 (board-interlocks, pedigree).

## Acceptance Criteria

- [ ] `GET /people-analytics/companies-with-org-charts` returns real companies with org chart snapshots
- [ ] `GET /people-analytics/companies/{id}/org-chart` returns real org chart tree from `org_chart_snapshots`
- [ ] `GET /pe/companies-with-financials` returns real companies from `pe_company_financials`
- [ ] `GET /pe/network-graph` returns real nodes+links from top PE firms + deals
- [ ] `GET /pe/deals/pipeline-summary` returns real deal pipeline Sankey data from `pe_deals`
- [ ] `orgchart.html` shows company selector and loads real org chart data
- [ ] `radar.html` shows company selector and loads real benchmark/financial percentiles
- [ ] `map.html` loads real datacenter state scores from existing `/datacenter-sites/top-states`
- [ ] `network.html` shows real PE firm + portfolio company graph
- [ ] `sankey.html` shows real deal pipeline with dynamic filter buttons
- [ ] Static fallback data remains for each page if API returns empty

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_companies_with_org_charts_returns_list | Returns list with id, name, total_people |
| T2 | test_org_chart_endpoint_returns_chart_data | Returns chart_data with root node for valid company |
| T3 | test_org_chart_missing_company_returns_404 | 404 for company with no snapshot |
| T4 | test_companies_with_financials_returns_list | Returns list with id, name, industry |
| T5 | test_network_graph_returns_nodes_and_links | Returns {nodes, links} structure |
| T6 | test_pipeline_summary_returns_sankey_format | Returns {nodes, links, summary} from pe_deals |
| T7 | test_pipeline_summary_empty_db_returns_empty | Handles empty pe_deals gracefully |

## Rubric Checklist

- [ ] Router created in `app/api/v1/<name>.py`
- [ ] Router registered in `app/main.py` with prefix and OpenAPI tag
- [ ] Uses `BackgroundTasks` for long-running operations (returns job_id immediately)
- [ ] Request/response models defined with Pydantic (typed, validated)
- [ ] Error responses use the error hierarchy from `app/core/api_errors.py`
- [ ] Database session obtained via `get_db()` dependency
- [ ] SQL queries use parameterized style (`:param`), never string concatenation
- [ ] Endpoint docstring describes purpose and parameters
- [ ] Has corresponding test file in `tests/test_<name>.py`
- [ ] Tests mock database and external services
- [ ] Tests cover happy path, validation errors, and edge cases
- [ ] No PII exposure beyond what sources explicitly provide

## Design Notes

### New endpoints:
1. `GET /people-analytics/companies-with-org-charts` → people_analytics.py
   - Query: `SELECT DISTINCT company_id, company_name FROM org_chart_snapshots`
   - Join to `industrial_companies` for id/name
   - Returns: `[{id, name, total_people, departments}]`

2. `GET /people-analytics/companies/{id}/org-chart` → people_analytics.py
   - Query: `SELECT chart_data FROM org_chart_snapshots WHERE company_id=:id ORDER BY snapshot_date DESC LIMIT 1`
   - Returns: `{chart_data, total_people, max_depth, departments}`

3. `GET /pe/companies-with-financials` → pe_benchmarks.py
   - Query: `SELECT DISTINCT company_id, company_name FROM pe_company_financials`
   - Returns: `[{id, name, industry}]`

4. `GET /pe/network-graph` → pe_benchmarks.py
   - Top 20 PE firms by AUM as `pe` nodes
   - pe_deals buyer → deal company as `company` nodes + ownership links
   - Returns: `{nodes: [...], links: [...]}`

5. `GET /pe/deals/pipeline-summary` → pe_deals.py
   - Group pe_deals by deal_type, deal_sub_type, status, COUNT(*)
   - Build Sankey: type_nodes → sub_nodes → status_nodes
   - Returns: `{nodes, links, summary}`

### Frontend pattern (same as PLAN_034):
- `const DATA = [...]` → `let DATA = [...]`
- Add `<select id="co-select">` (hidden initially)
- Add `populateSelector()` + `loadData(id)`
- Replace INIT with async IIFE

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/api/v1/people_analytics.py` | Modify | Add companies-with-org-charts + companies/{id}/org-chart |
| `app/api/v1/pe_benchmarks.py` | Modify | Add companies-with-financials + network-graph |
| `app/api/v1/pe_deals.py` | Modify | Add deals/pipeline-summary |
| `frontend/d3/orgchart.html` | Modify | Wire to org-chart API, add company selector |
| `frontend/d3/radar.html` | Modify | Wire to benchmarks API, remap axes |
| `frontend/d3/map.html` | Modify | Wire to datacenter-sites/top-states, FIPS lookup |
| `frontend/d3/network.html` | Modify | Wire to PE network-graph API |
| `frontend/d3/sankey.html` | Modify | Wire to pipeline-summary API, dynamic filters |

## Feedback History

_No corrections yet._
