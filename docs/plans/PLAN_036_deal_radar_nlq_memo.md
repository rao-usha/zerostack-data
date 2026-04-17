# PLAN_036 — NL Query Bar + AI Deal Memo Generator

## Context

Deal Radar is live with real convergence data across 13 US regions. Two AI-powered features will make it dramatically more impressive to investors:

1. **Natural Language Query Bar** — type "show me regions with high EPA violations and population inflow" → map filters in real-time. The "talk to your data" moment.
2. **AI Deal Memo Generator** — click any hot cluster → generates a full investment memo with market sizing, targets, risks, and signal analysis. One-click from signal to thesis.

**Existing infrastructure to reuse:**
- `LLMClient` (`app/agentic/llm_client.py`) — async Claude/OpenAI with `complete()`, `parse_json()`, cost tracking
- `ConvergenceEngine` (`app/services/convergence_engine.py`) — region scoring, DB queries
- Design system (`app/reports/design_system.py`) — `html_document`, `kpi_card`, `data_table`, `chart_container`, etc.
- Report builder (`app/reports/builder.py`) — template registration, persistence, PDF generation
- PE Deal Memo template (`app/reports/templates/pe_deal_memo.py`) — existing memo pattern to follow

---

## Feature 1: Natural Language Query Bar

### How it works
1. User types a query in a search bar overlaid on the Deal Radar map
2. Frontend sends query string to `POST /deal-radar/query`
3. Backend sends query + schema context to Claude → returns structured JSON filter
4. Backend applies filters to convergence_regions table → returns matching regions
5. Frontend highlights matching regions on the map, dims the rest

### Backend: `app/services/deal_radar_nlq.py`

```python
class DealRadarNLQ:
    """Natural language query engine for Deal Radar."""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def query(self, user_query: str) -> NLQResult:
        """Parse natural language → structured filter → query DB."""
        # 1. Call Claude to parse intent
        filters = await self._parse_query(user_query)
        # 2. Build SQL from filters
        regions = self._execute_query(filters)
        # 3. Generate explanation
        return NLQResult(
            query=user_query,
            filters=filters,
            regions=regions,
            explanation=filters.get("explanation", ""),
            region_count=len(regions),
        )
    
    async def _parse_query(self, query: str) -> dict:
        """Use Claude to parse query into structured filters."""
        # System prompt tells Claude about available fields:
        # - signal scores: epa_score, irs_migration_score, trade_score, water_score, macro_score
        # - composite: convergence_score, cluster_status (HOT/ACTIVE/WATCH/LOW)
        # - region metadata: region_id, label, states
        # Returns JSON: {filters: [...], sort_by, explanation}
        
    def _execute_query(self, parsed: dict) -> list:
        """Apply parsed filters to convergence_regions."""
        # Build dynamic WHERE clause from filter conditions
```

**LLM Prompt design** — the system prompt provides:
- Table schema (convergence_regions columns + types)
- Valid values (cluster_status: HOT/ACTIVE/WATCH/LOW, signal types)
- Output format: `{"filters": [{"field": "epa_score", "op": ">=", "value": 60}], "sort_by": "convergence_score", "explanation": "Regions with high EPA violations"}`
- The model NEVER generates raw SQL — only filter objects that the backend validates and constructs safe queries from

### API Endpoint: `app/api/v1/deal_radar.py` (add to existing router)

```
POST /deal-radar/query
Body: {"query": "regions with high EPA and population inflow"}
Returns: {
  "query": "...",
  "explanation": "Showing regions with EPA score >= 60 and IRS migration score >= 60",
  "filters_applied": [...],
  "regions": [...],     // filtered RegionResponse objects
  "total_matched": 5
}
```

### Frontend: Query bar in `deal-radar.html`

- Floating search bar at top of map area with subtle glass effect
- Type query → press Enter → loading spinner → map highlights matching regions
- Non-matching regions dim to 20% opacity
- Explanation text appears below the bar: "Showing 5 regions with EPA score >= 60 and IRS migration..."
- "Clear" button resets to show all regions
- Keyboard shortcut: `/` focuses the search bar

---

## Feature 2: AI Deal Memo Generator

### How it works
1. User clicks a HOT/ACTIVE cluster → thesis panel appears (existing)
2. New button: "Generate Deal Memo →"
3. Backend gathers all signal data for that region + PE market context
4. Claude synthesizes a full investment memo
5. Returns rendered HTML memo (design system styled) displayed in a modal
6. Download as PDF button

### Backend: `app/services/deal_radar_memo.py`

```python
class DealRadarMemoGenerator:
    """AI-powered investment memo generator for convergence clusters."""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def generate(self, region_id: str) -> MemoResult:
        """Generate full investment memo for a convergence cluster."""
        # 1. Gather signal data
        data = self._gather_data(region_id)
        # 2. Generate AI analysis sections
        analysis = await self._generate_analysis(data)
        # 3. Render HTML memo
        html = self._render_memo(data, analysis)
        return MemoResult(region_id=region_id, html=html, ...)
    
    def _gather_data(self, region_id: str) -> dict:
        """Pull all available data for the region."""
        # - Convergence scores (from convergence_regions)
        # - EPA facility details (top violations in region states)
        # - IRS migration flows (inflow/outflow for region states)
        # - Trade data (top exports, volume changes)
        # - Water system stats (violation counts, health-based)
        # - Income distribution (AGI brackets, capital gains)
        # - PE deals in region (from pe_deals if any match states)
        
    async def _generate_analysis(self, data: dict) -> dict:
        """Use Claude to generate memo sections."""
        # Sections:
        # 1. Executive Summary (3-4 sentences)
        # 2. Market Opportunity (sizing, dynamics)
        # 3. Signal Analysis (why these signals converge here)
        # 4. Target Profile (what kind of companies to acquire)
        # 5. Risk Factors (what could go wrong)
        # 6. Recommended Action (entry timing, strategy)
        
    def _render_memo(self, data: dict, analysis: dict) -> str:
        """Render HTML using design system components."""
        # Uses: html_document, page_header, kpi_strip, kpi_card,
        # data_table, section_start/end, chart_container, callout
```

### API Endpoint: `app/api/v1/deal_radar.py` (add to existing router)

```
POST /deal-radar/memo/{region_id}
Returns: {
  "region_id": "appalachia",
  "title": "Appalachia — Convergence Investment Memo",
  "html": "<full HTML document>",
  "sections": ["executive_summary", "market_opportunity", ...],
  "generated_at": "2026-03-30T..."
}
```

### Frontend: Memo modal in `deal-radar.html`

- New "Generate memo →" button in thesis panel action buttons
- Click → loading state → full-screen modal overlays with rendered HTML
- Modal has: close button, "Download PDF" button, "Copy link" button
- The memo itself is a styled HTML document rendered inside the modal via iframe/srcdoc

---

## New Files

| File | Purpose |
|------|---------|
| `app/services/deal_radar_nlq.py` | NL query parsing + execution |
| `app/services/deal_radar_memo.py` | Memo data gathering + AI generation + HTML rendering |

## Modified Files

| File | Change |
|------|--------|
| `app/api/v1/deal_radar.py` | Add 2 endpoints: `POST /query`, `POST /memo/{region_id}` |
| `frontend/deal-radar.html` | Add query bar + memo modal + button |

---

## Execution Order

1. `app/services/deal_radar_nlq.py` — NL query service
2. `app/services/deal_radar_memo.py` — Memo generator service
3. `app/api/v1/deal_radar.py` — Add both endpoints
4. `frontend/deal-radar.html` — Query bar + memo modal UI
5. Tests
6. Restart + verify

## Verification

1. `curl -X POST http://localhost:8001/api/v1/deal-radar/query -d '{"query":"regions with high EPA"}' -H 'Content-Type: application/json'`
2. `curl -X POST http://localhost:8001/api/v1/deal-radar/memo/appalachia`
3. Open http://localhost:3001/ → type query in search bar → verify map filters
4. Click HOT cluster → "Generate memo" → verify memo renders
5. `pytest tests/test_spec_029_deal_radar_nlq_memo.py -v`
