# PLAN_058 — Cascade Intelligence Platform: Conversational Macro Graph

**Status:** Research Complete — Ready for Implementation Planning  
**Date:** 2026-04-04  
**Scope:** Evolve the Cascade Explorer from a static slider into a dynamic, LLM-powered intelligence platform where users can converse with the macro causal graph, add/remove companies, pull live data, run forecasting models, and explore any macro shock's downstream effects.

---

## Part 1 — What Already Exists (Building Blocks Inventory)

### Infrastructure Ready Today

| Capability | Status | File | Notes |
|-----------|--------|------|-------|
| Causal graph engine (BFS cascade) | **Live** | `app/services/macro_cascade_engine.py` | Elasticity propagation with damping, 6-hop max |
| Macro nodes + edges in DB | **Live** | 20 nodes, 16 edges seeded | Housing, credit, energy, consumer cascades |
| Cascade simulation API | **Live** | `POST /macro/simulate` | Takes node_id + change_pct, returns all downstream impacts |
| Node CRUD API | **Live** | `POST /macro/nodes`, `GET /macro/graph` | Can create/query any node type |
| Company-macro linkage model | **Live** | `CompanyMacroLinkage` table | ticker, node_id, strength, direction, evidence |
| 10-K risk factor extraction | **Live** | `MacroSensitivityAgent` | Parses SEC filings → auto-creates linkages |
| LLM client (OpenAI + Anthropic) | **Live** | `app/agentic/llm_client.py` | gpt-4o, claude-3-5-sonnet, cost tracking |
| Synthetic macro forecasting | **Live** | `MacroScenarioGenerator` | Correlated O-U mean-reverting walk |
| 50+ FRED series in DB | **Live** | 7 FRED tables | Interest rates, housing, sentiment, commodities, etc. |
| PE portfolio companies | **Live** | 1,812 companies | Many with tickers, industry, sector |
| Industrial companies | **Live** | 1,000+ with revenue/employment | Linked to job postings |
| D3 force graph frontend | **Live** | `cascade-explorer.html` | Slider, simulation, node click |
| Deal Environment Scorer | **Live** | 9 sectors × 7 factors | Macro → PE sector attractiveness |
| Portfolio Stress Scorer | **Live** | Per-holding stress | Leverage × rates × sector headwind |

### What's Missing

| Capability | Gap | Effort |
|-----------|-----|--------|
| Chat interface (conversational) | No chat endpoint or message history | Medium |
| LLM tool-use (function calling) | No tool framework for graph operations | Medium |
| Dynamic company addition via LLM | Manual only today | Small (wrap existing APIs) |
| Forecasting integration in graph | Synthetic generator exists but not connected to cascade viz | Small |
| Multi-variable shock (not just FFR) | API supports it, frontend doesn't | Small |
| Auto-discovery of new companies | 10-K agent exists for 5 companies, needs to scale | Medium |

---

## Part 2 — The Four Capabilities to Build

### Capability 1: Add/Remove Companies Dynamically

**What exists:**
- `POST /macro/nodes` — creates any node (company, FRED, custom)
- `CompanyMacroLinkage` model — links companies to macro nodes
- `MacroSensitivityAgent` — extracts linkages from 10-K filings

**What to build:**

**A. Company Search + Add API**
```
POST /macro/companies/add
Body: { "ticker": "HD", "name": "Home Depot" }
```
Steps:
1. Check if company node already exists (by ticker) → return it
2. Create MacroNode with `node_type=company`, `ticker`, `sector_tag` from industry
3. Run MacroSensitivityAgent for this company → auto-create CausalEdges based on 10-K risk factors
4. If no 10-K available: use LLM to infer likely macro linkages from company description + sector
5. Return new node with all edges

**B. Company Removal**
```
DELETE /macro/companies/{ticker}
```
- Soft-delete: set `is_active=false` on node + edges
- Doesn't destroy data, just hides from graph

**C. Bulk Add from Portfolio**
```
POST /macro/companies/add-portfolio/{firm_id}
```
- Pulls all portfolio companies with tickers from `pe_portfolio_companies`
- Runs add flow for each
- Returns count added + skipped

**Data sources for auto-linkage:**
1. SEC 10-K risk factors (existing agent, 23 keyword mappings)
2. NAICS/SIC code → sector → known macro drivers (from SECTOR_CONFIGS)
3. LLM inference: "Given {company_name} in {industry}, which FRED series would most affect its revenue?"

### Capability 2: Pull Live Data from Postgres

**What exists:**
- `GET /macro/current-environment` — fetches current FRED values
- `POST /macro/collect/sync-node-values` — syncs all node current_values from DB
- 7 FRED tables, 3 BLS tables, BEA GDP, EIA energy — all in Postgres

**What to build:**

**A. Node Value Auto-Refresh**
- On graph load: call `sync-node-values` → every node's `current_value` populates from latest DB row
- Display current value on each node in the viz (e.g., "FFR: 3.64%", "HOUST: 1.42M")

**B. Historical Sparklines**
```
GET /macro/nodes/{id}/history?months=24
```
- Queries the underlying FRED/BLS table for this node's series_id
- Returns 24 monthly data points
- Frontend: renders tiny sparkline inside node tooltip

**C. Company Financial Pull**
For company nodes, also pull from `pe_company_financials`:
```
GET /macro/companies/{ticker}/financials
```
- Returns latest revenue, EBITDA, leverage
- Feeds into stress calculation inline with cascade results

**D. Cross-Table Enrichment**
When a company is added, automatically query:
- `pe_company_financials` — revenue, margins, leverage
- `job_postings` — hiring momentum (from Chain 4)
- `epa_echo_facilities` — environmental exposure (from Chain 2)
- `usaspending_awards` — government revenue dependency

### Capability 3: Forecasting Models (Precanned)

**What exists:**
- `MacroScenarioGenerator` — generates N synthetic forward paths for any FRED series
- O-U mean-reverting model calibrated from historical data
- Already returns percentile bands (p10, p50, p90)

**What to build:**

**A. Forecast-Augmented Cascade**
```
POST /macro/forecast-cascade
Body: {
  "node_id": 1,           // FFR
  "scenario": "rate_hike", // precanned
  "horizon_months": 12,
  "n_paths": 100
}
```
Steps:
1. Generate 100 forward paths for the input node using MacroScenarioGenerator
2. For each path's terminal value: compute cascade impact (simulate at each timestep or terminal)
3. Return: p10/p50/p90 cascade impacts for every downstream node
4. Frontend: shows uncertainty bands — "Housing Starts: -0.3% to -0.8% (90% CI)"

**B. Precanned Scenario Library**

| Scenario | Input | Description |
|----------|-------|-------------|
| Rate Hike (+100bps) | DFF +1.0% | Standard tightening cycle |
| Rate Hike (+200bps) | DFF +2.0% | Aggressive tightening |
| Rate Cut (-100bps) | DFF -1.0% | Easing cycle |
| Oil Shock (+50%) | DCOILWTICO +50% | Energy crisis |
| Oil Crash (-40%) | DCOILWTICO -40% | Demand destruction |
| Stagflation | DFF +3%, UNRATE +2% | Multi-variable shock |
| Housing Crash | HOUST -30%, CSUSHPINSA -15% | 2008-style |
| Consumer Recession | UMCSENT -30%, RSXFS -10% | Demand collapse |
| Pandemic Shock | UNRATE +8%, DCOILWTICO -50%, DFF -2% | COVID-style |

**C. Multi-Variable Shocks**
Current simulate API takes one node. Extend to:
```
POST /macro/simulate-multi
Body: {
  "shocks": [
    {"node_id": 1, "change_pct": 3.0},
    {"node_id": 11, "change_pct": 2.0}
  ],
  "horizon_months": 24
}
```
- Runs cascade from each shock independently
- Combines impacts (additive for same target, with interaction dampening)

### Capability 4: LLM Conversational Interface (Chat with the Graph)

**What exists:**
- LLM client with OpenAI + Anthropic support
- Extensive agent patterns (10+ agents)
- All macro APIs are RESTful and well-documented

**What to build:**

**A. Chat Endpoint**
```
POST /macro/chat
Body: {
  "message": "What happens to paint companies if mortgage rates go up 2%?",
  "conversation_id": "uuid",  // for multi-turn context
  "graph_state": { ... }      // current visible nodes/edges
}
```

**B. LLM Tool-Use Framework**

The LLM gets access to these tools (function calling):

| Tool | Description | Maps To |
|------|-------------|---------|
| `simulate_shock` | Run cascade from any node | `POST /macro/simulate` |
| `add_company` | Add a company to the graph | `POST /macro/companies/add` |
| `remove_company` | Remove a company | `DELETE /macro/companies/{ticker}` |
| `get_node_history` | Get historical data for a node | `GET /macro/nodes/{id}/history` |
| `forecast_scenario` | Run forward-looking forecast | `POST /macro/forecast-cascade` |
| `get_company_exposure` | Get macro drivers for a company | `GET /macro/company-impact/{ticker}` |
| `search_companies` | Find companies by industry/sector | `GET /pe/firms/search` or DB query |
| `get_current_macro` | Current macro environment | `GET /macro/current-environment` |
| `explain_edge` | Explain causal mechanism | Returns edge.mechanism_description |
| `add_edge` | Create new causal relationship | `POST /macro/edges` (new) |

**C. Conversation Flow Examples**

**Example 1: "What happens to Sherwin-Williams if rates go up?"**
```
LLM: [calls simulate_shock(node_id=1, change_pct=1.0)]
LLM: "A 1% rate increase would affect Sherwin-Williams through two channels:
      1. Mortgage rates rise +0.85% → Housing Starts fall -0.5% → Paint demand drops → SHW revenue -0.16%
      2. New Home Sales fall -0.4% → SHW revenue -0.13%
      Combined estimated impact: SHW revenue -0.2% with 6-month lag."
```

**Example 2: "Add Home Depot to the graph"**
```
LLM: [calls add_company(ticker="HD", name="Home Depot")]
LLM: [calls get_company_exposure(ticker="HD")]
LLM: "I've added Home Depot to the graph. Based on their 10-K risk factors, 
      HD is linked to:
      - Housing Starts (revenue_driver, positive, strength 0.7)
      - MORTGAGE30US (risk_factor, negative, strength 0.6)
      - Consumer Sentiment (demand_driver, positive, strength 0.5)
      
      At current rates (FFR 3.64%), a +1% hike would reduce HD revenue 
      an estimated -0.3% with a 4-6 month lag."
```

**Example 3: "What if oil hits $150 and rates go to 7%?"**
```
LLM: [calls simulate_multi(shocks=[{DCOILWTICO: +68%}, {DFF: +3.36%}])]
LLM: "Stagflation scenario — combined impacts:
      - Industrial Margins: -24% (oil + rates combined)
      - Housing Starts: -2.8% (rate-driven)
      - PE Deal Activity: -1.8% (financing freeze)
      - Sherwin-Williams: -1.2% (housing + input costs)
      - D.R. Horton: -2.5% (housing crash)
      
      This would be the worst macro environment since 2008 for PE 
      deal-making. Recommend defensive positioning."
```

**Example 4: "Remove PE Deal Activity, it's not relevant to my analysis"**
```
LLM: [calls remove_company(ticker=null, node_id=PE_DEAL_ACTIVITY_ID)]
LLM: "Removed PE Deal Activity from the graph. The housing cascade is 
      now cleaner — FFR → Mortgage → Housing Starts → company impacts."
```

**D. Conversation History Table**
```sql
CREATE TABLE macro_chat_messages (
    id SERIAL PRIMARY KEY,
    conversation_id UUID NOT NULL,
    role VARCHAR(20) NOT NULL,  -- 'user', 'assistant', 'system', 'tool'
    content TEXT NOT NULL,
    tool_calls JSONB,           -- tool invocations
    tool_results JSONB,         -- tool responses
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX ON macro_chat_messages(conversation_id, created_at);
```

**E. Frontend Chat Panel**
- Slide-up chat panel at bottom of cascade-explorer page
- Message input with send button
- Conversation history with tool-call indicators
- Graph updates in real-time as LLM makes tool calls
- "Thinking..." indicator while LLM processes

---

## Part 3 — Architecture

### System Diagram

```
┌─────────────────────────────────────────────────┐
│  FRONTEND: cascade-explorer.html                │
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ D3 Graph │  │ Sliders  │  │ Chat Panel   │  │
│  │ (force)  │  │ + Presets │  │ (messages)   │  │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘  │
│       │             │               │           │
└───────┼─────────────┼───────────────┼───────────┘
        │             │               │
        ▼             ▼               ▼
┌─────────────────────────────────────────────────┐
│  API LAYER: FastAPI                             │
│                                                 │
│  /macro/graph          → graph data             │
│  /macro/simulate       → cascade engine         │
│  /macro/companies/add  → node + edge creation   │
│  /macro/forecast       → O-U scenario generator │
│  /macro/chat           → LLM orchestrator       │
│                                                 │
│  ┌─────────────────────────────────────────┐    │
│  │  LLM Orchestrator (tool-use loop)       │    │
│  │                                         │    │
│  │  System prompt: macro analyst persona   │    │
│  │  Tools: simulate, add_company, forecast │    │
│  │  Context: current graph state + history │    │
│  │                                         │    │
│  │  Loop: user msg → LLM → tool calls →   │    │
│  │        execute tools → LLM → response   │    │
│  └─────────────────────────────────────────┘    │
│                                                 │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│  DATA LAYER: PostgreSQL                         │
│                                                 │
│  macro_nodes, causal_edges, cascade_results     │
│  company_macro_linkages                         │
│  fred_*, bls_*, bea_*, pe_portfolio_companies   │
│  macro_chat_messages (conversation history)     │
│                                                 │
└─────────────────────────────────────────────────┘
```

### LLM Tool-Use Implementation Pattern

Using Anthropic's tool-use (function calling) format:

```python
tools = [
    {
        "name": "simulate_shock",
        "description": "Simulate the cascading effect of a macro shock through the causal graph",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_id": {"type": "integer", "description": "ID of the macro node to shock"},
                "change_pct": {"type": "number", "description": "Percentage change to apply"},
            },
            "required": ["node_id", "change_pct"]
        }
    },
    {
        "name": "add_company",
        "description": "Add a company to the macro causal graph with auto-detected linkages",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": ["name"]
        }
    },
    # ... more tools
]
```

---

## Part 4 — Implementation Phases

### Phase 1: Dynamic Company Management (Backend)
- `POST /macro/companies/add` — create node + auto-link via NAICS/sector
- `DELETE /macro/companies/{ticker}` — soft-delete
- `POST /macro/companies/add-portfolio/{firm_id}` — bulk add from PE portfolio
- LLM-assisted linkage inference for companies without 10-K data
- **Effort:** 2-3 hours

### Phase 2: Data Integration + Forecasting
- `GET /macro/nodes/{id}/history` — sparkline data from FRED/BLS tables
- `POST /macro/forecast-cascade` — forward-looking scenario with uncertainty bands
- `POST /macro/simulate-multi` — multi-variable shock support
- Precanned scenario library (9 scenarios)
- **Effort:** 3-4 hours

### Phase 3: LLM Chat Interface (Backend)
- `POST /macro/chat` — conversational endpoint with tool-use loop
- `macro_chat_messages` table for conversation history
- System prompt with macro analyst persona + graph context
- 10 tools wired to existing APIs
- **Effort:** 4-5 hours

### Phase 4: Enhanced Frontend
- Chat panel (slide-up from bottom of cascade-explorer)
- Multi-node shock UI (shock any node, not just FFR)
- Company search + add button in graph
- Sparklines in node tooltips
- Forecast uncertainty bands on affected nodes
- Conversation history with tool-call indicators
- **Effort:** 4-5 hours

### Phase 5: Polish + Demo Story
- Demo walkthrough: "Show me what happens to homebuilders if we get stagflation"
- Auto-narration mode (LLM generates insight text as you explore)
- Export conversation to report
- **Effort:** 2-3 hours

---

## Part 5 — Key Design Decisions

### LLM Provider for Chat
**Recommendation:** Claude 3.5 Sonnet (Anthropic)
- Best at tool-use with complex multi-step reasoning
- Already integrated via `LLMClient`
- Cost: ~$0.003 per chat turn (1K input + 500 output tokens)
- Fallback: GPT-4o-mini for cost-sensitive deployments

### Graph State Management
- Server-authoritative: all node/edge state in Postgres
- Frontend is a view of server state
- Chat tool calls modify server state → frontend re-fetches
- No client-side graph editing (prevents drift)

### Scaling to 100+ Companies
- Current: 20 nodes, 16 edges
- Target: 100-200 nodes, 300-500 edges
- Force simulation handles up to ~500 nodes smoothly in D3
- Beyond 500: switch to WebGL (d3-force + PixiJS) or cluster by sector

### Forecasting Model Choice
- **Phase 1:** O-U mean-reverting (already built, works, no training)
- **Phase 2:** VAR (Vector Autoregression) — captures cross-series dynamics better
- **Phase 3:** Transformer-based (if needed) — best for nonlinear regime changes
- Start with O-U, upgrade if users need more accuracy

---

## Part 6 — Data Available for Enrichment

### Companies We Can Add Immediately (have tickers + data)

**From PE Portfolio (with 10-K accessible via EDGAR):**
- SHW (Sherwin-Williams) — already in graph
- DHI (D.R. Horton) — already in graph
- HD (Home Depot) — in sensitivity agent targets
- LOW (Lowe's) — in sensitivity agent targets
- LEN (Lennar) — in sensitivity agent targets
- XOM (Exxon Mobil) — in sensitivity agent targets

**From PE Portfolio Companies table (have tickers):**
- ADT, AAON, many others with PE backing

**From Industrial Companies (public, have tickers):**
- Bosch, Visa, Cloudflare, Databricks, Stripe, etc.

### FRED Series We Can Add as Nodes

| Series | Description | Currently in Graph? |
|--------|-------------|-------------------|
| CPIAUCSL | CPI Inflation | No — should add |
| PCE | Personal Consumption Expenditures | No |
| INDPRO | Industrial Production | No — should add |
| TOTALSA | Total Vehicle Sales | No |
| M2SL | M2 Money Supply | No |
| FEDFUNDS | Effective Fed Funds (alt) | Yes (DFF) |
| GDP | Gross Domestic Product | No — should add |
| PAYEMS | Total Nonfarm Payrolls | No — should add |
| DTWEXBGS | Trade-Weighted USD Index | No |
| VIXCLS | VIX Volatility Index | No (not in FRED tables) |

---

## Appendix — Example Chat Session

```
User: "I'm looking at the housing market. What companies should I be watching?"

AI: Based on the current causal graph, the housing cascade flows through:
    - Federal Funds Rate (3.64%) → 30-Year Mortgage (6.73%) → Housing Starts (1.42M)
    
    Currently tracking: Sherwin-Williams (paint/coatings) and D.R. Horton (homebuilder).
    
    I'd recommend adding these companies for a complete view:
    - Home Depot (HD) — building materials retail
    - Lowe's (LOW) — same sector, different footprint  
    - Lennar (LEN) — second-largest homebuilder
    - Toll Brothers (TOL) — luxury segment
    
    Want me to add them?

User: "Yes, add HD, LOW, and LEN"

AI: [adds 3 companies with auto-detected linkages]
    
    Done. I've added all three:
    - Home Depot: linked to Housing Starts (+0.7), Mortgage Rates (-0.5), Consumer Sentiment (+0.6)
    - Lowe's: linked to Housing Starts (+0.65), Consumer Sentiment (+0.55)  
    - Lennar: linked to Housing Starts (+0.85), Mortgage Rates (-0.7), New Home Sales (+0.9)
    
    The graph now has 23 nodes. At current rates, a 1% rate hike would:
    - Housing Starts: -0.5% (5mo lag)
    - D.R. Horton: -0.3% (11mo)
    - Lennar: -0.35% (9mo) — more rate-sensitive
    - Home Depot: -0.2% (7mo)
    - Sherwin-Williams: -0.2% (6mo)

User: "What if we get a 2008-style housing crash?"

AI: [runs Housing Crash scenario: HOUST -30%, CSUSHPINSA -15%]
    
    Simulating a 2008-style scenario (Housing Starts -30%, Home Prices -15%):
    
    Severe impacts:
    - D.R. Horton: -27% revenue (direct homebuilder exposure)
    - Lennar: -25.5% revenue
    - Sherwin-Williams: -18% revenue (paint demand collapse)
    - Home Depot: -15% revenue (renovation + new construction both hit)
    - Lowe's: -13% revenue
    
    This would put all housing-exposed companies in "Critical" stress territory.
    The cascade would take 6-12 months to fully materialize through the supply chain.
    
    Want me to add confidence intervals using our forecasting model?
```

---

## Revisions

| Rev | Date | Summary | Doc |
|-----|------|---------|-----|
| 01 | 2026-04-08 | Replace force graph with Layered DAG + Sankey. Fix slider, recentering, input flexibility. | [PLAN_058_rev_01](PLAN_058_cascade_intelligence_platform_rev_01.md) |
