# PLAN_036 — Wire Remaining D3 Visualizations to Real Data

## Context

Five D3 prototype visualizations in `frontend/d3/` are using entirely hardcoded static/fake data. Two were wired to real API data in PLAN_034 (board-interlocks, pedigree). This plan wires the remaining five following the same established pattern.

**Audit summary:**
| Viz | File | Status |
|-----|------|--------|
| Board Interlock Network | d3/board-interlocks.html | ✅ Real (PLAN_034) |
| Executive Pedigree | d3/pedigree.html | ✅ Real (PLAN_034) |
| Org Chart | d3/orgchart.html | ❌ Fake ("Acme Industrial") |
| Deal Scoring Radar | d3/radar.html | ❌ Fake (3 fictional companies) |
| Location Intelligence | d3/map.html | ❌ Fake (hardcoded FIPS scores) |
| Investment Network | d3/network.html | ❌ Fake (14 fictional firms) |
| Deal Flow Sankey | d3/sankey.html | ❌ Fake (fictional conversion funnel) |

**Real data available in DB:**
- `org_chart_snapshots`: 7 real org charts (JPMorgan 159 people, Middleby 67, Stanley B&D 46, Prudential 40, Cummins 28)
- `pe_company_financials`: 262 rows with revenue, EBITDA, margins, growth rates
- `pe_deals`: 52 real PE deals with `deal_type`, `deal_sub_type`, `status` (LBO, Growth Equity, Exit, Buyout, Add-on)
- `pe_firms`: 105 real firms with AUM
- Datacenter county scoring: `/api/v1/datacenter-sites/top-states` already live

---

## Implementation Approach (PLAN_034 Pattern)

For each visualization:
1. Change `const DATA = [...]` → `let DATA = [...]` (static data stays as fallback)
2. Add a `<select id="co-select">` company/entity selector (hidden initially)
3. Add `populateSelector()` → fetches a list endpoint → shows dropdown
4. Add `loadData(id)` → fetches data endpoint → transforms → re-renders
5. Replace `INIT` with async IIFE reading `?company_id=` URL param

---

## Visualization 1: orgchart.html — Real Org Charts

**New backend endpoints (people_analytics.py):**
- `GET /people-analytics/companies-with-org-charts` → `[{id, name, total_people, departments}]`
- `GET /people-analytics/companies/{id}/org-chart` → `{chart_data, total_people, max_depth, departments}`

**Query:** `SELECT * FROM org_chart_snapshots WHERE company_id = :id ORDER BY snapshot_date DESC LIMIT 1`

**Frontend transform:**
```js
function transformOrgNode(node) {
  return {
    name: node.name,
    title: node.title,
    dept: node.dept || "Executive",
    tenure: node.tenure || 0,
    children: (node.children || []).map(transformOrgNode)
  };
}
// API returns { chart_data: { root: {...} }, ... }
// Usage: orgData = transformOrgNode(data.chart_data.root || data.chart_data)
```

**Files:** `app/api/v1/people_analytics.py`, `frontend/d3/orgchart.html`

---

## Visualization 2: radar.html — Real Financial Benchmarks

**New backend endpoint (pe_benchmarks.py or pe_firms.py):**
- `GET /pe/companies-with-financials` → `[{id, name, industry}]` (companies in `pe_company_financials`)

**Existing endpoint to reuse:** `GET /pe/benchmarks/{company_id}` already exists and returns percentile scores.

**Frontend axes remapping:** The radar's 8 axes should be remapped to real benchmark metric names. Reduce to 6 axes that map cleanly to `pe_company_financials`:
1. Revenue Growth → `revenue_growth_pct` percentile (0–100)
2. EBITDA Margin → `ebitda_margin_pct` percentile
3. Leverage → inverted `debt_to_ebitda` percentile (lower debt = higher score)
4. Revenue Scale → `revenue_usd` percentile
5. Profitability → `net_income_margin_pct` percentile
6. EV Multiple → `ev_ebitda_multiple` percentile (from pe_deals join)

**Files:** `app/api/v1/pe_benchmarks.py`, `frontend/d3/radar.html`

---

## Visualization 3: map.html — Real Datacenter Site Scores

**Existing endpoints (no new endpoints needed):**
- `GET /api/v1/datacenter-sites/top-states` → state averages (returns `{state: "TX", avg_score: 87.3, ...}`)
- `GET /api/v1/datacenter-sites/rankings?state=TX` → county-level breakdown for click detail

**Frontend changes:**
1. Add `STATE_ABBR_TO_FIPS` lookup (50 entries hardcoded, pure JS)
2. `async loadStateScores()` → fetches `/top-states`, populates `stateScores[fips]` and `stateMetrics[fips]`
3. Wrap `initMap()` in async IIFE: `await loadStateScores(); initMap();`
4. On state click, fetch `/rankings?state={abbr}&limit=1` for county-level detail panel
5. Graceful fallback: if API returns empty, use hardcoded static data

**Files:** `frontend/d3/map.html` only (no backend changes)

---

## Visualization 4: network.html — Real PE Firm Network

**New backend endpoint (pe_benchmarks.py):**
- `GET /pe/network-graph` → pre-built `{nodes: [...], links: [...]}` in D3-compatible format

**Query logic:**
1. Top 20 PE firms by AUM → `pe` type nodes
2. PE deals where `buyer_name ILIKE any firm name` → `company` type nodes from `deal.deal_name`
3. Ownership links: `buyer_name` firm → deal company
4. Fallback to real firm names only (no links) if deal matching is too sparse

**Response format:**
```json
{
  "nodes": [
    {"id": "pf_1", "label": "Blackstone", "type": "pe", "aum": 940000, "strategy": "Buyout"},
    {"id": "co_5", "label": "Hilton Hotels", "type": "company", "sector": "Hospitality"}
  ],
  "links": [
    {"source": "pf_1", "target": "co_5", "type": "ownership"}
  ]
}
```

**Files:** `app/api/v1/pe_benchmarks.py` (or new `pe_network.py`), `frontend/d3/network.html`

---

## Visualization 5: sankey.html — Real Deal Pipeline

**New backend endpoint (pe_deals.py or pe_firms.py):**
- `GET /pe/deals/pipeline-summary` → Sankey nodes/links from real deal data

**Query:**
```sql
SELECT deal_type, COALESCE(deal_sub_type, 'Unspecified') as deal_sub_type,
       status, COUNT(*) as deal_count
FROM pe_deals WHERE deal_type IS NOT NULL AND deal_type != '8-K Event'
GROUP BY deal_type, deal_sub_type, status
```

**Node ID prefix strategy** (avoids D3 collision): `type_LBO`, `sub_Platform`, `status_Closed`

**Frontend changes:**
1. Replace static `SECTOR_DATA` dict with API-driven flow
2. Filter buttons become dynamic `deal_type` values (LBO, Growth Equity, Exit, etc.)
3. `buildDataFromApi(apiResponse)` replaces `buildData(sector)` — accepts `{nodes, links}` directly
4. Stats bar: total deals count, top deal type, pipeline stage breakdown

**Files:** `app/api/v1/pe_deals.py` (or `pe_benchmarks.py`), `frontend/d3/sankey.html`

---

## New Endpoints Summary

| Endpoint | File | Priority |
|----------|------|----------|
| `GET /people-analytics/companies-with-org-charts` | people_analytics.py | 1 |
| `GET /people-analytics/companies/{id}/org-chart` | people_analytics.py | 1 |
| `GET /pe/companies-with-financials` | pe_benchmarks.py | 2 |
| `GET /pe/network-graph` | pe_benchmarks.py | 4 |
| `GET /pe/deals/pipeline-summary` | pe_deals.py | 5 |

**No new endpoints for map.html** (datacenter endpoints already live).

---

## Implementation Order

1. `people_analytics.py` — add org-chart endpoints (2 new endpoints)
2. `orgchart.html` — wire to API, company selector
3. `pe_benchmarks.py` — add companies-with-financials + network-graph endpoints
4. `radar.html` — wire to `/pe/benchmarks/{id}`, axis remapping
5. `map.html` — wire to `/datacenter-sites/top-states`, FIPS lookup (frontend only)
6. `network.html` — wire to `/pe/network-graph`
7. `pe_deals.py` — add pipeline-summary endpoint
8. `sankey.html` — wire to pipeline-summary, dynamic filters
9. Restart API, verify all 5 pages show real data

## Verification

```bash
# Org chart: Prudential Financial (id=142)
curl http://localhost:8001/api/v1/people-analytics/companies/142/org-chart

# Radar: any company from financials list
curl http://localhost:8001/api/v1/pe/companies-with-financials

# Map: datacenter county scores (already live)
curl http://localhost:8001/api/v1/datacenter-sites/top-states

# Network: PE firm graph
curl http://localhost:8001/api/v1/pe/network-graph

# Sankey: deal pipeline
curl http://localhost:8001/api/v1/pe/deals/pipeline-summary
```

After all: open each D3 page, confirm company selector appears, data loads, static fallback works if empty.
