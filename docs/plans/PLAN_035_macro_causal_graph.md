# PLAN 035 — Macro Causal Network Graph + Cascade Simulation

**Date:** 2026-03-27
**Status:** Approved
**Goal:** Build a unified macro causal knowledge graph that models cascading economic effects — interest rates → housing → Sherwin-Williams — and connects them to PE portfolio companies. Extend the existing co-investor network graph to include macro nodes.

---

## Context

The existing network graph (`app/network/graph.py`) shows LP-GP co-investment relationships. The PE conviction system (PLAN_034) shows LP commitment strength. But neither answers: *"If the Fed raises rates 100bps, which portfolio companies get hurt and how badly?"*

The Sherwin-Williams example is the archetype:
```
Fed Funds Rate ↑ →
  30yr Mortgage Rate ↑ (lag: 1-2 mo) →
    Housing Affordability ↓ →
      New Home Sales ↓ (lag: 2-4 mo) +
      Housing Starts ↓ (lag: 3-6 mo) →
        Architectural Coatings Demand ↓ (lag: 1-2 mo) →
          Sherwin-Williams Revenue ↓ (lag: 1-3 mo) →
            SHW EBITDA margin compression →
              PE specialty chemicals portfolio company exit readiness ↓
```

This is a **directed causal graph** where nodes are macro indicators, sectors, and companies, and edges carry lag times, directionality, and elasticity coefficients.

---

## What Already Exists (Do NOT Rebuild)

| Asset | Location |
|---|---|
| FRED ingestion (DFF, DGS10, GDP, UNRATE, CPI, PCE) | `app/sources/fred/client.py`, `ingest.py` |
| BLS ingestion (CPS, CES, JOLTS, CPI, PPI broad) | `app/sources/bls/client.py`, `ingest.py` |
| EIA ingestion (petroleum, gas, electricity) | `app/sources/eia/client.py` |
| NetworkEngine (co-investor graph) | `app/network/graph.py` |
| PEMarketSignal, PECompanyFinancials | `app/core/pe_models.py` |
| MarketScannerAgent | `app/agents/market_scanner.py` |
| LLMClient | `app/agentic/llm_client.py` |
| BaseAPIClient, BaseSourceIngestor | `app/core/http_client.py`, `app/core/ingest_base.py` |
| GraphQL schema (LP, FamilyOffice, Portfolio types) | `app/graphql/` |

---

## New Data Sources Required

### 1. Extend FRED (`app/sources/fred/client.py`) — new series

**Housing Market:**
| Series ID | Name | Frequency |
|---|---|---|
| HOUST | Housing Starts Total | Monthly |
| HSN1F | New Single-Family Home Sales | Monthly |
| EXHOSLUSM495S | Existing Home Sales | Monthly |
| PERMIT | Building Permits | Monthly |
| MORTGAGE30US | 30-Year Fixed Mortgage Rate | Weekly → Monthly |
| CSUSHPINSA | Case-Shiller National HPI | Monthly |
| BSXRNSA | NAHB Housing Market Index (builder confidence) | Monthly |

**Consumer / Commodities:**
| Series ID | Name |
|---|---|
| UMCSENT | U of Michigan Consumer Sentiment |
| DCOILWTICO | WTI Crude Oil Price |
| DHHNGSP | Henry Hub Natural Gas Price |

All go into existing `fred_economic_indicators` table (same schema, just new series IDs).

### 2. Extend BLS PPI (`app/sources/bls/client.py`) — industry-specific series

| Series ID | Name |
|---|---|
| WPU0613 | Paint, Varnish, Lacquers, Coatings PPI |
| WPU132 | Construction Materials PPI |
| WPU081 | Lumber and Wood Products PPI |
| WPU0622 | Plastics Materials PPI |
| WPU1311 | Sand, Gravel, Crushed Stone PPI |

All go into existing `bls_ppi` table.

### 3. New Source: SEC EDGAR Company Facts (`app/sources/edgar_company_facts/`)

**Free public API.** Returns all XBRL-tagged financial data for any SEC-registered company.
- URL: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json`
- No API key required. Rate limit: 10 req/sec.
- Returns: quarterly Revenue, EBITDA, Net Income, EPS, etc. going back 10+ years

**Target companies for initial seed (causal chain anchors):**
| Ticker | Company | CIK | Relevance |
|---|---|---|---|
| SHW | Sherwin-Williams | 0000089089 | Coatings / housing demand |
| DHI | D.R. Horton | 0000045012 | Homebuilder |
| LEN | Lennar | 0000720005 | Homebuilder |
| HD | Home Depot | 0000354950 | Home improvement |
| LOW | Lowe's | 0000060667 | Home improvement |
| XOM | ExxonMobil | 0000034088 | Oil/energy proxy |

**New files:**
- `app/sources/edgar_company_facts/client.py` — fetches company facts JSON from EDGAR
- `app/sources/edgar_company_facts/ingest.py` — persists to new `public_company_financials` table

---

## New Database Model — `app/core/macro_models.py` (new file)

### MacroNode
Every node in the causal graph — a macro indicator, sector, or company anchor.

```python
class MacroNode(Base):
    __tablename__ = "macro_nodes"
    id, node_type  # 'fred_series','bls_series','sector','company','custom'
    name, description, unit
    series_id           # FK for FRED/BLS nodes (e.g. "DFF", "WPU0613")
    company_id          # FK to pe_portfolio_companies (for company nodes)
    ticker              # for public company nodes (SHW, DHI)
    frequency           # 'daily','weekly','monthly','quarterly'
    current_value, current_value_date
    is_leading_indicator, is_coincident, is_lagging  # indicator classification
    sector_tag          # 'housing','consumer','credit','energy','industrial'
    created_at, updated_at
```

### CausalEdge
A directed causal relationship between two nodes.

```python
class CausalEdge(Base):
    __tablename__ = "causal_edges"
    id
    source_node_id (FK → MacroNode)
    target_node_id (FK → MacroNode)
    relationship_direction  # 'positive' or 'negative'
    elasticity              # coefficient: 1% Δ source → elasticity% Δ target
    typical_lag_months      # median lag before effect is felt
    lag_min_months, lag_max_months   # uncertainty range
    confidence              # 0-1 (how well-established is this relationship)
    mechanism_description   # "Higher rates increase mortgage costs, reducing affordability"
    empirical_correlation   # measured correlation from historical data (nullable)
    data_source_refs        # JSON list of academic/empirical sources
    is_active               # bool (can toggle edges off)
    created_at, updated_at
    UniqueConstraint(source_node_id, target_node_id)
```

### CascadeScenario
A named what-if scenario.

```python
class CascadeScenario(Base):
    __tablename__ = "cascade_scenarios"
    id, name, description
    input_node_id (FK → MacroNode)
    input_change_pct        # e.g. 1.0 = +100bps for rate, -20.0 = -20% for oil
    input_change_direction  # 'up' or 'down'
    horizon_months          # how far forward to project (default 24)
    as_of_date, created_at
```

### CascadeResult
Computed impact on each node for a scenario.

```python
class CascadeResult(Base):
    __tablename__ = "cascade_results"
    id
    scenario_id (FK → CascadeScenario)
    node_id (FK → MacroNode)
    estimated_impact_pct    # % change in this node vs baseline
    peak_impact_month       # month (1-24) when impact is largest
    confidence              # 0-1
    impact_path (JSON)      # list of intermediate nodes in causal chain
    distance_from_input     # number of hops from input node
    computed_at
```

### CompanyMacroLinkage
Links companies to the nodes that drive their financials.

```python
class CompanyMacroLinkage(Base):
    __tablename__ = "company_macro_linkages"
    id
    company_id              # FK to pe_portfolio_companies or nullable
    ticker                  # for public companies (SHW, DHI, etc.)
    node_id (FK → MacroNode)
    linkage_type            # 'revenue_driver','cost_driver','demand_driver','competitor_proxy'
    linkage_strength        # 0-1 (how strongly correlated)
    direction               # 'positive' or 'negative'
    evidence_source         # 'sec_10k_risk_factors','empirical','manual'
    evidence_text           # excerpt from 10-K or study
    created_at
    UniqueConstraint(company_id or ticker, node_id)
```

### PublicCompanyFinancials (for EDGAR Company Facts)
```python
class PublicCompanyFinancials(Base):
    __tablename__ = "public_company_financials"
    id, ticker, cik, company_name
    period_end_date, fiscal_period  # 'Q1 2024', 'FY 2023'
    revenue_usd, gross_profit_usd, net_income_usd, ebitda_usd
    eps_basic, eps_diluted
    data_source             # 'edgar_xbrl'
    filed_at
    UniqueConstraint(ticker, period_end_date, fiscal_period)
```

---

## New Services

### `app/services/macro_cascade_engine.py`
The core simulation service.

**Algorithm:**
1. Load input node + input change
2. BFS outward from input node, traversing `CausalEdge`s
3. At each hop, compute: `impact = parent_impact × elasticity × damping_factor`
4. Track cumulative lag: `node_lag = sum(edge lags along path)`
5. Apply **damping factor** per hop (0.7 — effects attenuate with distance)
6. Cap at 6 hops (distant effects become noise below 0.7^6 ≈ 12%)
7. For each reached node, store best (highest confidence) impact path

**Key method:** `simulate(scenario: CascadeScenario) → list[CascadeResult]`

Returns results sorted by abs(estimated_impact_pct) descending — most impacted nodes first.

### `app/agents/macro_sensitivity_agent.py`
LLM agent that reads SEC 10-K Risk Factors and extracts company-macro linkages.

**Flow:**
1. Fetch 10-K from SEC EDGAR for target companies
2. Extract "Risk Factors" section
3. LLM prompt: "Identify all macroeconomic factors mentioned. For each: factor name, direction of impact (positive/negative), strength (high/medium/low), direct quote."
4. Map extracted factors to canonical `MacroNode` names (via fuzzy match)
5. Create `CompanyMacroLinkage` records with `evidence_source='sec_10k_risk_factors'`

### `app/services/macro_node_seeder.py`
Seeds the causal graph with pre-defined economic relationships.

Pre-seeds nodes and edges for the core housing cascade + 3 additional cascades:

**Housing Cascade (Fed Rates → SHW):**
```
DFF → MORTGAGE30US    (elasticity: +0.85, lag: 1-2 mo, confidence: 0.9)
MORTGAGE30US → HOUST  (elasticity: -0.6,  lag: 3-6 mo, confidence: 0.85)
MORTGAGE30US → HSN1F  (elasticity: -0.5,  lag: 2-4 mo, confidence: 0.85)
HOUST → WPU0613       (elasticity: +0.7,  lag: 1-2 mo, confidence: 0.75)  [paint PPI]
WPU0613 → SHW_Revenue (elasticity: +0.8,  lag: 1-3 mo, confidence: 0.7)
```

**Credit/PE Cascade (rates → deal activity):**
```
DFF → DGS10           (elasticity: +0.7, lag: 0-1 mo, confidence: 0.95)
DGS10 → PE_LBO_Cost   (elasticity: +1.2, lag: 1-3 mo, confidence: 0.8)
PE_LBO_Cost → DealActivity (elasticity: -0.5, lag: 3-6 mo, confidence: 0.7)
```

**Energy/Industrial Cascade (oil → industrials):**
```
DCOILWTICO → PPIACO   (elasticity: +0.4, lag: 1-2 mo, confidence: 0.8)
PPIACO → IndustrialMargins (elasticity: -0.6, lag: 0-1 mo, confidence: 0.75)
```

**Consumer/Labor Cascade (employment → spending):**
```
UNRATE → UMCSENT      (elasticity: -0.8, lag: 1-2 mo, confidence: 0.85)
UMCSENT → RSXFS       (elasticity: +0.6, lag: 1-3 mo, confidence: 0.75)  [retail sales]
RSXFS → ConsumerSector (elasticity: +0.7, lag: 1-2 mo, confidence: 0.7)
```

---

## New API Router — `app/api/v1/macro_cascade.py`

```
GET   /macro/graph                        Full causal graph (nodes + edges) for visualization
GET   /macro/nodes                        List all macro nodes
POST  /macro/nodes                        Create a custom node
GET   /macro/nodes/{id}/upstream          All nodes that causally affect this node
GET   /macro/nodes/{id}/downstream        All nodes this node affects
POST  /macro/simulate                     Run cascade: {node_id, change_pct, horizon_months}
GET   /macro/scenarios                    List saved scenarios
GET   /macro/scenarios/{id}/results       Get computed cascade results
GET   /macro/company-impact/{ticker}      What macros affect this company? (from linkages)
GET   /macro/portfolio-impact             For all PE portfolio companies, show macro exposure
GET   /macro/current-environment          Current macro reading (live FRED values for key nodes)
POST  /macro/collect/edgar-facts          Trigger SEC EDGAR company facts ingestion
POST  /macro/collect/seed-relationships   Re-seed known causal relationships
```

---

## New Frontend — `frontend/macro-cascade.html`

D3.js force-directed graph visualization:

**Left panel — Graph:**
- Node types: macro indicator (blue circles), sector (orange squares), company (green diamonds)
- Edge types: positive relationship (green arrows), negative (red arrows), width = confidence
- Click a node: highlight all upstream/downstream paths, show current value in tooltip
- "Scenario Mode" toggle: turns clicked node into input, lets you drag a slider to set % change → animates cascade through graph with color intensity = impact magnitude

**Right panel — Cascade Details:**
- Active scenario name + input change
- Ranked table: "Most Impacted Nodes" with estimated % impact, lag, confidence, causal path
- "Portfolio Exposure" section: PE portfolio companies sorted by macro exposure score
- Time series charts (Chart.js): show historical correlation between selected node pairs

---

## Execution Order

```
Agent A (Data Expansion):
  1. Extend FRED client with 10 new housing/commodity series
  2. Extend BLS PPI with 5 industry series
  3. Create edgar_company_facts/ collector (client + ingest)
  4. Trigger ingestion for new series via API calls

Agent B (Data Model + Seeding):
  1. Create app/core/macro_models.py (5 new tables)
  2. Create app/services/macro_node_seeder.py (pre-seed 15 nodes, 12 edges)
  3. Create app/services/macro_cascade_engine.py (BFS simulation)
  4. Create app/agents/macro_sensitivity_agent.py (10-K Risk Factor extractor)

Agent C (API + Frontend):
  1. Create app/api/v1/macro_cascade.py router (10 endpoints)
  2. Register router in main.py
  3. Create frontend/macro-cascade.html (D3 force-directed graph)

Master:
  1. Restart API, verify all endpoints
  2. Run seed: POST /macro/collect/seed-relationships
  3. Run simulation: POST /macro/simulate {DFF, +1.0%}
  4. Verify SHW shows up in cascade results
  5. Commit
```

---

## Key Files to Modify

| File | Change |
|---|---|
| `app/sources/fred/client.py` | Add 10 new housing/commodity series to COMMON_SERIES |
| `app/sources/bls/client.py` | Add 5 industry PPI series |
| `app/main.py` | Register macro_cascade router |

## New Files to Create

| File | Purpose |
|---|---|
| `app/core/macro_models.py` | MacroNode, CausalEdge, CascadeScenario, CascadeResult, CompanyMacroLinkage, PublicCompanyFinancials |
| `app/sources/edgar_company_facts/client.py` | SEC EDGAR XBRL company facts client |
| `app/sources/edgar_company_facts/ingest.py` | Ingestor for public company financials |
| `app/services/macro_node_seeder.py` | Seeds causal graph with known economic relationships |
| `app/services/macro_cascade_engine.py` | BFS cascade simulation engine |
| `app/agents/macro_sensitivity_agent.py` | LLM agent extracting company-macro linkages from 10-K |
| `app/api/v1/macro_cascade.py` | Full macro cascade API router |
| `frontend/macro-cascade.html` | D3 interactive causal graph visualization |

---

## Verification

1. `pytest tests/ -v --ignore=tests/integration/` — all tests pass
2. `curl http://localhost:8001/api/v1/macro/graph` — returns nodes + edges JSON
3. `curl -X POST http://localhost:8001/api/v1/macro/collect/seed-relationships` — seeds 15+ nodes, 12+ edges
4. `curl -X POST http://localhost:8001/api/v1/macro/simulate -d '{"node_id": <DFF_id>, "change_pct": 1.0}'` — cascade runs
5. `curl http://localhost:8001/api/v1/macro/scenarios/1/results` — SHW appears in results with negative impact
6. `curl http://localhost:8001/api/v1/macro/company-impact/SHW` — shows housing/mortgage nodes as drivers
7. Open `http://localhost:5173/macro-cascade.html` — interactive D3 graph renders
