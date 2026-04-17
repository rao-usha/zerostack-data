# PLAN_035 — Deal Radar: Convergence Intelligence

## Context

The user has a static HTML mockup of "Deal Radar — Convergence Intelligence" — a real-time geographic signal dashboard that detects investment opportunities by finding regions where multiple public data signals converge (EPA violations + IRS migration + trade stress + water system issues + macro/income shifts). Currently the mockup uses hardcoded fake data and random signal emission.

**All 5 data sources already exist and are ingested:**
- `epa_echo_facilities` — compliance status, violation counts, penalties by state/county
- `irs_soi_migration` — county-to-county migration flows (population + income movement)
- `us_trade_exports_state` — state-level export values by commodity
- `public_water_systems` + `water_system_violations` — water infrastructure stress
- `irs_soi_zip_income` — income distribution by ZIP/state (macro signal)

**Existing scoring patterns to reuse:**
- `FundConvictionScorer` pattern: weighted multi-signal → 0-100 composite score → letter grade
- `MarketScannerService` pattern: scan across dimensions → produce signals → persist to DB
- `PEMarketSignal` model pattern: signal storage with batch tracking

**Goal:** Build a backend service + API + wired frontend that computes real convergence scores from actual database data, replacing the mockup's fake signals.

---

## Architecture

### New Files

| File | Purpose |
|------|---------|
| `app/services/convergence_engine.py` | Core scoring engine — queries 5 sources, computes regional convergence |
| `app/core/convergence_models.py` | SQLAlchemy models: regions, signals, clusters |
| `app/api/v1/deal_radar.py` | REST endpoints for the dashboard |
| `frontend/deal-radar.html` | Wired version of the mockup (calls real API) |
| `tests/test_convergence_engine.py` | Unit tests |

### Modified Files

| File | Change |
|------|--------|
| `app/main.py` | Register `deal_radar` router + OpenAPI tag |

---

## Step-by-step Plan

### Step 1: Models (`app/core/convergence_models.py`)

Three tables following existing patterns:

```python
class ConvergenceRegion(Base):
    """13 US macro-regions with computed scores."""
    __tablename__ = "convergence_regions"
    id = Column(Integer, primary_key=True)
    region_id = Column(String, unique=True)  # e.g. "southeast"
    label = Column(String)                    # e.g. "Southeast"
    states = Column(JSON)                     # ["AL","GA","SC","NC","TN","MS"]
    center_lat = Column(Float)
    center_lon = Column(Float)
    # Per-signal scores (0-100)
    epa_score = Column(Float, default=0)
    irs_migration_score = Column(Float, default=0)
    trade_score = Column(Float, default=0)
    water_score = Column(Float, default=0)
    macro_score = Column(Float, default=0)
    # Composite
    convergence_score = Column(Float, default=0)
    convergence_grade = Column(String(2))     # A/B/C/D/F
    cluster_status = Column(String(10))       # HOT/ACTIVE/WATCH/LOW
    active_signals = Column(JSON)             # ["EPA","IRS","Water"]
    scored_at = Column(DateTime)
    
class ConvergenceSignal(Base):
    """Individual signal events for the live feed."""
    __tablename__ = "convergence_signals"
    id = Column(Integer, primary_key=True)
    region_id = Column(String, index=True)
    signal_type = Column(String)  # epa|irs|trade|water|macro
    score = Column(Float)
    description = Column(Text)
    raw_data = Column(JSON)
    detected_at = Column(DateTime, server_default=func.now())
    batch_id = Column(String, index=True)

class ConvergenceCluster(Base):
    """Persisted cluster events — when a region crosses threshold."""
    __tablename__ = "convergence_clusters"
    id = Column(Integer, primary_key=True)
    region_id = Column(String, index=True)
    convergence_score = Column(Float)
    signal_count = Column(Integer)
    active_signals = Column(JSON)
    thesis_text = Column(Text)  # AI-generated thesis (cached)
    opportunity_score = Column(Float)
    urgency_score = Column(Float)
    risk_score = Column(Float)
    detected_at = Column(DateTime, server_default=func.now())
```

### Step 2: Region Definitions + Signal Scorers (`app/services/convergence_engine.py`)

**Region→State mapping** (13 regions matching the mockup):
```
Pacific NW: WA, OR
California: CA
Mountain West: MT, WY, CO, UT, ID, NV
Southwest: AZ, NM
Great Plains: ND, SD, NE, KS, OK
Texas: TX
Midwest: MN, IA, MO, WI, IL, IN
Appalachia: WV, KY, VA
Southeast: AL, GA, SC, NC, TN, MS, AR, LA
Great Lakes: MI, OH, PA
Mid-Atlantic: NY, NJ, DE, MD, DC
Northeast: CT, MA, RI, VT, NH, ME
Florida: FL
```

**5 signal scorers** — each queries its source table and produces a 0-100 score per region:

1. **EPA Score** — `_score_epa(region_states)`: Query `epa_echo_facilities` for states in region. Score based on: violation_count density, penalty amounts, non-compliance %. Higher violations → higher signal (distressed = opportunity).

2. **IRS Migration Score** — `_score_irs_migration(region_states)`: Query `irs_soi_migration` for net inflow/outflow. Score high on: large net inflows (growth markets) OR large net outflows (distressed assets). Use absolute magnitude.

3. **Trade Score** — `_score_trade(region_states)`: Query `us_trade_exports_state` for state-level exports. Score on: YoY export changes, trade concentration, commodity diversity.

4. **Water Score** — `_score_water(region_states)`: Query `public_water_systems` + violations. Score on: violation density per capita, health-based violations, repeat offenders, system age.

5. **Macro Score** — `_score_irs_income(region_states)`: Query `irs_soi_zip_income`. Score on: income growth rate, high-AGI bracket growth, capital gains concentration, business income trends.

**Convergence formula** (matches mockup logic):
```python
def convergence_score(scores):
    vals = [scores.epa, scores.irs, scores.trade, scores.water, scores.macro]
    above_60 = sum(1 for v in vals if v >= 60)
    avg = sum(vals) / 5
    return round(avg * (1 + above_60 * 0.1))
```

**Cluster classification:**
- >= 72: HOT (grade A)
- >= 58: ACTIVE (grade B)  
- >= 44: WATCH (grade C)
- < 44: LOW (grade D/F)

**Thesis generation** — `generate_thesis(region, scores)`: Call Claude API (Anthropic SDK already in project) with region signals to produce 3-sentence investment thesis + opportunity/urgency/risk sub-scores. Cache in `convergence_clusters`.

### Step 3: API Endpoints (`app/api/v1/deal_radar.py`)

| Method | Path | Returns |
|--------|------|---------|
| `POST` | `/deal-radar/scan` | Run full convergence scan across all regions, persist results, return summary |
| `GET` | `/deal-radar/regions` | All 13 regions with current scores |
| `GET` | `/deal-radar/regions/{region_id}` | Single region detail with signal breakdown |
| `GET` | `/deal-radar/clusters` | Active clusters (score >= 44), sorted by score desc |
| `GET` | `/deal-radar/signals` | Recent signal feed (last N signals) |
| `GET` | `/deal-radar/stats` | Top-bar stats: total signals today, active clusters, new 24h |
| `POST` | `/deal-radar/thesis/{region_id}` | Generate/return AI thesis for a region |

### Step 4: Wire the Frontend (`frontend/deal-radar.html`)

Take the existing mockup and replace the fake data layer:
- On load: `GET /deal-radar/regions` → render map nodes with real scores
- On load: `GET /deal-radar/stats` → populate top bar
- On load: `GET /deal-radar/clusters` → populate "Top opportunities" sidebar
- On load: `GET /deal-radar/signals?limit=6` → populate live feed
- Poll `/deal-radar/signals` every 10s for new signals (replace the random `emitSignal()` timer)
- Region click: `POST /deal-radar/thesis/{region_id}` → stream into thesis panel
- "Find targets" button: link to existing PE deal sourcing endpoints
- Keep all CSS/animations from the mockup intact

### Step 5: Register Router + Tests

**`app/main.py`**: Add router import + `app.include_router(deal_radar_router, prefix="/api/v1")` + OpenAPI tag.

**Tests** (`tests/test_convergence_engine.py`):
- Test region→state mapping completeness (all 50 states + DC covered)
- Test each scorer returns 0-100 with mock DB data
- Test convergence formula matches expected output
- Test cluster classification thresholds
- Test API endpoints return correct shapes

---

## Verification

1. `docker-compose restart api` → wait for startup
2. `curl http://localhost:8001/api/v1/deal-radar/regions | python -m json.tool` — should return 13 regions with scores
3. `curl -X POST http://localhost:8001/api/v1/deal-radar/scan | python -m json.tool` — should run full scan
4. `curl http://localhost:8001/api/v1/deal-radar/clusters | python -m json.tool` — should show HOT/ACTIVE clusters
5. Open `frontend/deal-radar.html` → verify map renders with real data, click regions for thesis
6. `pytest tests/test_convergence_engine.py -v` — all tests pass

---

## Execution Order

1. Models (convergence_models.py)
2. Engine (convergence_engine.py) — scorers + convergence formula
3. API (deal_radar.py) — endpoints
4. main.py registration
5. Frontend wiring (deal-radar.html)
6. Tests
7. Restart + verify
