# PLAN_055 — Macro Cascade Explorer (Interactive Rate Slider → Company Impact)

## Context

The diligence scorecard view is dry and static. The user wants an interactive visualization where you **slide interest rates** and **watch the cascade flow** through macro indicators down to real companies — paint companies, homebuilders, hardware stores. The full cascade engine + causal graph + simulation API already exist and are live.

**Verified live:**
- 20 macro nodes, 16 causal edges (seeded)
- Housing cascade: DFF → MORTGAGE30US (+0.85x) → HOUST (-0.6x) → SHW, D.R. Horton
- `POST /macro/simulate` works: +2% FFR → Mortgage +1.2% → Housing Starts -0.5% → SHW -0.2%
- Existing `macro-cascade.html` has D3 force graph but is complex/cluttered

## What to Build

A focused, clean D3 page: `frontend/cascade-explorer.html`

### Layout

```
┌─────────────────────────────────────────────────────┐
│  ← Back | Macro Cascade Explorer                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─ RATE SLIDER ──────────────────────────────┐     │
│  │  Federal Funds Rate: ◀━━━━━●━━━━━▶  +2.0%  │     │
│  │  -3%        0%        +3%        +5%        │     │
│  └─────────────────────────────────────────────┘     │
│                                                     │
│  ┌─ D3 FORCE GRAPH ───────────────────────────┐     │
│  │                                             │     │
│  │    [MORTGAGE]                               │     │
│  │       ↓                                     │     │
│  │  [FFR] ──→ [HOUSING STARTS] ──→ [SHW]      │     │
│  │       ↓         ↓                           │     │
│  │  [10Y YIELD]  [D.R.HORTON]  [PAINT PPI]    │     │
│  │       ↓                                     │     │
│  │  [PE LBO COST] → [DEAL ACTIVITY]           │     │
│  │                                             │     │
│  └─────────────────────────────────────────────┘     │
│                                                     │
│  ┌─ IMPACT TABLE ──────────────────────────────┐     │
│  │  Downstream Effects (sorted by |impact|)    │     │
│  │  Mortgage Rate      +1.2%  ▓▓▓▓░░  1mo     │     │
│  │  Housing Starts     -0.5%  ▓▓▓░░░  5mo     │     │
│  │  D.R. Horton Rev    -0.3%  ▓▓░░░░  11mo    │     │
│  │  Sherwin-Williams   -0.2%  ▓░░░░░  6mo     │     │
│  └─────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────┘
```

### How It Works

1. **Page loads:** Fetch `/macro/graph` → render D3 force graph with all nodes/edges
2. **Slider:** Range input from -3% to +5%, step 0.25%. Default = 0%.
3. **On slider move (debounced 300ms):** Call `POST /macro/simulate` with `{node_id: DFF_ID, change_pct: sliderValue}`
4. **On results:** Animate node changes:
   - **Size:** Proportional to |impact_pct| (bigger = more affected)
   - **Color:** Green = positive impact, Red = negative, Gray = no impact. Intensity = magnitude.
   - **Pulse animation:** Nodes ripple outward from source on each update
   - **Edge glow:** Edges on the active cascade path glow/thicken
5. **Impact table below graph:** Sorted by |impact|, shows name, % change, bar chart, peak month, confidence

### Node Visual Design

| Node Type | Shape | Default Color | Affected Color |
|-----------|-------|---------------|----------------|
| Fed Funds Rate (input) | Large circle, gold border | Gold (#f59e0b) | Gold (always) |
| FRED series (macro) | Circle | Slate (#64748b) | Green/Red gradient |
| BLS series | Circle, dashed border | Slate | Green/Red gradient |
| Company | Diamond (rotated square) | Indigo (#6366f1) | Green/Red, size pulses |
| Custom (PE metrics) | Rounded rectangle | Cyan (#06b6d4) | Green/Red |

### Edge Visual Design

- **Default:** Thin gray line (#475569), 1px
- **Active path:** Glow effect, 3px, colored by direction (green=positive, red=negative)
- **Arrow markers** showing direction
- **Label on hover:** elasticity value + mechanism description

### Preset Scenarios (buttons above slider)

- "Rate Hike +1%" → slider to +1
- "Rate Hike +2%" → slider to +2
- "Rate Cut -1%" → slider to -1
- "Stagflation" → slider to +3
- "Reset" → slider to 0

### Right Panel (on node click)

Click any node → slide-in panel showing:
- Node name, current value, series_id
- All upstream causes (what drives this?)
- All downstream effects (what does this affect?)
- If company node: causal path from FFR, link to stress/diligence scores

## Technical Details

### API Calls
- Page load: `GET /macro/graph` (nodes + edges)
- Slider move: `POST /macro/simulate` (debounced 300ms)
- Node click: `GET /macro/nodes/{id}/upstream` + `GET /macro/nodes/{id}/downstream`

### D3 Force Layout
```javascript
simulation = d3.forceSimulation(nodes)
  .force("link", d3.forceLink(edges).id(d => d.id).distance(120))
  .force("charge", d3.forceManyBody().strength(-400))
  .force("center", d3.forceCenter(W/2, H/2))
  .force("x", d3.forceX(W/2).strength(0.05))
  .force("y", d3.forceY(H/2).strength(0.05))
  .force("collision", d3.forceCollide(40));
```

### Color Scale for Impact
```javascript
const impactColor = d3.scaleDiverging(d3.interpolateRdYlGn)
  .domain([-5, 0, 5]);  // -5% = red, 0 = yellow, +5% = green
```

### Debounced Simulation
```javascript
let debounceTimer;
slider.oninput = () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => runSimulation(slider.value), 300);
};
```

## Files

- **NEW:** `frontend/cascade-explorer.html` (~800-1000 lines, self-contained)
- **MODIFY:** `frontend/index.html` (add gallery card)

## Verification

1. Open `http://localhost:3001/cascade-explorer.html`
2. See force graph with 20 nodes, FFR highlighted as input node
3. Drag slider to +2% → nodes animate: Mortgage goes red (+1.2%), Housing Starts red (-0.5%), SHW small red diamond (-0.2%)
4. Drag to -1% → everything reverses: Housing Starts green, SHW green
5. Click on "Housing Starts" node → panel shows upstream (FFR, Mortgage) and downstream (SHW, DHI, Paint PPI)
6. Impact table updates in real time as slider moves
