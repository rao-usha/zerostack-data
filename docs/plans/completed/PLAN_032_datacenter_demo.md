# PLAN 032 — Datacenter Site Selection Demo (3-Agent Parallel)

**Date:** 2026-03-23
**Status:** Completed — commit 1a4b83d (2026-03-23)
**Goal:** Build an interactive web demo and two supporting backend features on top of the existing
datacenter site selection infrastructure. Zero new scoring logic or data collection.

---

## What Already Exists (Do NOT Rebuild)

| Asset | Location |
|---|---|
| 6-dimension scoring engine | `app/ml/datacenter_site_scorer.py` |
| Regulatory scorer | `app/ml/county_regulatory_scorer.py` |
| 13-section HTML report template | `app/reports/templates/datacenter_site.py` |
| 8 REST API endpoints | `app/api/v1/datacenter_sites.py` |
| Pre-generated TX + national reports | `datacenter_report_tx.html`, `datacenter_report_national.html` |
| 18 data sources ingested | power, fiber, zoning, FEMA, NREL, EPA, BLS, etc. |

**`datacenter_site_scores` table — exact schema (from `app/ml/datacenter_site_metadata.py`):**
```
county_fips VARCHAR(5), county_name VARCHAR(255), state VARCHAR(2), score_date DATE
overall_score NUMERIC(5,2), grade VARCHAR(1), national_rank INT, state_rank INT
power_score NUMERIC(5,2), connectivity_score NUMERIC(5,2), regulatory_score NUMERIC(5,2)
labor_score NUMERIC(5,2), risk_score NUMERIC(5,2), cost_incentive_score NUMERIC(5,2)
power_capacity_nearby_mw, substations_count, electricity_price_cents_kwh
ix_count, dc_facility_count, broadband_coverage_pct, regulatory_speed_score
tech_employment, tech_avg_wage, flood_risk_rating, brownfield_sites
incentive_program_count, opportunity_zone BOOLEAN, renewable_ghi
transmission_line_count, mean_elevation_ft, flood_high_risk_zones, wetland_acres
```
No `thesis_text` column yet — Agent 2 adds it.

**Existing API endpoints the demo will call:**
```
GET  /api/v1/datacenter-sites/top-states?limit=15
     → {"states": [{"state":"TX","avg_score":86.7,"county_count":254,"a_grade":12,"max_score":94.2}, ...]}

GET  /api/v1/datacenter-sites/rankings?state=TX&limit=20
     → {"counties": [{"county_fips":"48001","county_name":"Andrews","state":"TX","overall_score":94.2,
                       "grade":"A","power_score":96.1,"connectivity_score":88.4,
                       "regulatory_score":91.2,"labor_score":82.3,"risk_score":95.0,
                       "cost_incentive_score":78.5,"national_rank":1,"state_rank":1}, ...]}

GET  /api/v1/datacenter-sites/{county_fips}
     → full row from datacenter_site_scores including all raw metrics
```

**New endpoints Agents 2+3 will add (frontend must handle 404 gracefully until they exist):**
```
POST /api/v1/datacenter-sites/{county_fips}/thesis
     → {"county_fips":"48001","county_name":"Andrews","state":"TX",
        "overall_score":94.2,"thesis":"...","generated_at":"..."}
     OR {"thesis":null,"error":"LLM not configured"} on 200 if no API key

POST /api/v1/datacenter-sites/pipeline/{county_fips}
     body: {"target_mw":50,"notes":"","status":"Evaluating"}
     → pipeline record

GET  /api/v1/datacenter-sites/pipeline
     → [{"county_fips":"48001","county_name":"Andrews","state":"TX",
          "overall_score":94.2,"status":"Evaluating","added_at":"...","target_mw":50}, ...]
```

---

## The Three Agents

---

### Agent 1 — Interactive Demo Frontend

**Files owned:** `frontend/dc-demo.html` (new file — do NOT touch any .py files)

**CSS variables to copy exactly from `frontend/pe-demo.html` `:root` block:**
```css
:root {
    --primary: #6366f1;
    --bg: #0f172a;
    --bg-card: #1e293b;
    --bg-input: #334155;
    --text: #f1f5f9;
    --text-muted: #94a3b8;
    --border: #334155;
    --success: #22c55e;
    --warning: #f59e0b;
    --error: #ef4444;
    --accent: #06b6d4;
}
```

**CSS classes to copy from `frontend/pe-demo.html` (copy the exact CSS, don't rewrite):**
- `*`, `body` resets
- `.demo-header`, `.demo-logo`, `.demo-logo h1`, `.demo-logo .tag`, `.demo-back`
- `.progress-bar`, `.progress-step`, `.progress-step:hover`, `.progress-step.active`, `.progress-step.completed`, `.step-num`
- `.demo-content`, `.step-panel`, `.step-panel.active`
- `.narration`, `.narration::before`, `.narration-title`, `.narration-text`
- `.demo-nav`
- `.btn`, `.btn-primary`, `.btn-primary:hover`, `.btn-secondary`, `.btn-secondary:hover`, `.btn:disabled`, `.btn-success`
- `.card`, `.card-title`, `.loading`
- `.ptable`, `.ptable th`, `.ptable td`, `.ptable tbody tr`, `.ptable tbody tr:hover`, `.ptable tbody tr.selected`
- `.pct-cell`, `.pct-cell.green`, `.pct-cell.yellow`, `.pct-cell.red`
- `.kpi-grid` (4-col grid), `.kpi`, `.kpi-value`, `.kpi-label`
- `.two-panel` (2-col grid with media query at 1100px)
- `.ss-row`, `.ss-label`, `.ss-bar-bg`, `.ss-bar-fill`, `.ss-grade`
- `.pill`, `.pill-active`

**Additional CSS needed (add after copied classes):**
```css
/* State scorecard grid */
.state-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 12px;
    margin-bottom: 1.5rem;
}
.state-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1rem;
    cursor: pointer;
    transition: border-color 0.15s;
    text-align: center;
}
.state-card:hover { border-color: var(--primary); }
.state-card.selected { border-color: var(--primary); background: rgba(99,102,241,0.08); }
.state-card .sc-state { font-size: 1.25rem; font-weight: 700; }
.state-card .sc-score { font-size: 1.75rem; font-weight: 700; color: var(--primary); }
.state-card .sc-label { font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; margin-top: 2px; }
.state-card .sc-badge { font-size: 0.7rem; padding: 0.1rem 0.5rem; border-radius: 10px; margin-top: 6px; display: inline-block; font-weight: 600; }
.badge-green { background: rgba(34,197,94,0.15); color: var(--success); }
.badge-yellow { background: rgba(245,158,11,0.15); color: var(--warning); }
.badge-red { background: rgba(239,68,68,0.12); color: var(--error); }

/* Thesis card */
.thesis-card {
    background: rgba(99,102,241,0.05);
    border: 1px solid rgba(99,102,241,0.2);
    border-radius: 10px;
    padding: 1.25rem;
    margin-top: 1rem;
    font-size: 0.875rem;
    line-height: 1.7;
    color: var(--text-muted);
}
.thesis-card.loading { font-style: italic; }

/* Toast notification */
.toast {
    position: fixed; bottom: 24px; right: 24px;
    background: var(--success); color: white;
    padding: 0.75rem 1.25rem; border-radius: 8px;
    font-size: 0.875rem; font-weight: 600;
    opacity: 0; transition: opacity 0.3s;
    z-index: 9999;
}
.toast.show { opacity: 1; }
```

**Full HTML structure:**

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nexdata — Datacenter Site Selection Demo</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
    <style>
        /* [all CSS above] */
    </style>
</head>
<body>
    <header class="demo-header">
        <div class="demo-logo">
            <span style="font-size:1.5rem;">🏗️</span>
            <h1>Nexdata</h1>
            <span class="tag">Site Intel Demo</span>
        </div>
        <a href="index.html" class="demo-back">&larr; Back to Dashboard</a>
    </header>

    <nav class="progress-bar">
        <div class="progress-step active" onclick="goStep(0)">
            <span class="step-num">1</span>Market Overview
        </div>
        <div class="progress-step" onclick="goStep(1)">
            <span class="step-num">2</span>County Rankings
        </div>
        <div class="progress-step" onclick="goStep(2)">
            <span class="step-num">3</span>Site Deep Dive
        </div>
        <div class="progress-step" onclick="goStep(3)">
            <span class="step-num">4</span>Site Pipeline
        </div>
    </nav>

    <main class="demo-content">

        <!-- No-data overlay (shown when DB has no scores) -->
        <div id="no-data-screen" style="display:none;">
            <div class="seed-overlay">
                <div class="seed-icon">📊</div>
                <div class="seed-title">No Scoring Data Found</div>
                <div class="seed-text">Run county scoring first: POST /api/v1/datacenter-sites/score-counties</div>
            </div>
        </div>

        <!-- Step 0: National Market Overview -->
        <div id="step-0" class="step-panel active">
            <div class="narration">
                <div class="narration-title">Step 1: National Market — Where's the Power?</div>
                <div class="narration-text">
                    We scored every US county across <strong>18 live data sources</strong> —
                    power capacity, fiber density, regulatory speed, workforce, environmental risk,
                    and tax incentives. Each state card shows its average score and number of
                    A-grade counties. Click any state to drill into county rankings.
                </div>
            </div>
            <div class="kpi-grid" id="overview-kpis"></div>
            <div id="state-grid" class="state-grid"></div>
            <div id="overview-loading" class="loading">Loading national data...</div>
            <div class="demo-nav">
                <span></span>
                <button class="btn btn-primary" onclick="goStep(1)" id="btn-to-rankings" disabled>
                    View County Rankings &rarr;
                </button>
            </div>
        </div>

        <!-- Step 1: County Rankings -->
        <div id="step-1" class="step-panel">
            <div class="narration">
                <div class="narration-title" id="rankings-title">Step 2: County Rankings</div>
                <div class="narration-text">
                    Every county scored 0-100. Power infrastructure alone accounts for
                    <strong>30% of the score</strong> — critical for a 50MW+ campus.
                    Click any county to see its full investment breakdown.
                </div>
            </div>
            <div class="card">
                <div class="card-title" id="rankings-card-title">Top Counties</div>
                <div style="overflow-x:auto;">
                    <table class="ptable">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>County</th>
                                <th>State</th>
                                <th>Overall</th>
                                <th>Power</th>
                                <th>Connectivity</th>
                                <th>Regulatory</th>
                                <th>Labor</th>
                                <th>Risk</th>
                            </tr>
                        </thead>
                        <tbody id="rankings-body"></tbody>
                    </table>
                </div>
                <div id="rankings-loading" class="loading" style="display:none;">Loading rankings...</div>
            </div>
            <div class="demo-nav">
                <button class="btn btn-secondary" onclick="goStep(0)">&larr; Back</button>
                <button class="btn btn-primary" onclick="goStep(2)" id="btn-to-deepdive" disabled>
                    Deep Dive &rarr;
                </button>
            </div>
        </div>

        <!-- Step 2: Site Deep Dive -->
        <div id="step-2" class="step-panel">
            <div class="narration">
                <div class="narration-title" id="deepdive-title">Step 3: Site Deep Dive</div>
                <div class="narration-text">
                    Full score breakdown with raw infrastructure metrics. Use
                    <strong>Generate Investment Thesis</strong> to get an AI-written
                    investment memo for this site.
                </div>
            </div>
            <div class="two-panel">
                <div class="card">
                    <div class="card-title">Score Breakdown</div>
                    <div class="kpi-grid" id="dd-kpis"></div>
                    <div id="dd-bars"></div>
                </div>
                <div class="card">
                    <div class="card-title">Raw Infrastructure Metrics</div>
                    <div id="dd-metrics"></div>
                </div>
            </div>
            <div class="card">
                <div class="card-title">Investment Thesis</div>
                <button class="btn btn-primary" onclick="generateThesis()" id="btn-thesis">
                    Generate Investment Thesis
                </button>
                <div id="thesis-output" style="display:none;" class="thesis-card"></div>
            </div>
            <div class="demo-nav">
                <button class="btn btn-secondary" onclick="goStep(1)">&larr; Back</button>
                <button class="btn btn-success" onclick="addToPipeline()" id="btn-pipeline">
                    Add to Pipeline &rarr;
                </button>
            </div>
        </div>

        <!-- Step 3: Site Pipeline -->
        <div id="step-3" class="step-panel">
            <div class="narration">
                <div class="narration-title">Step 4: Your Site Pipeline</div>
                <div class="narration-text">
                    Shortlisted sites tracked through your selection process.
                    Scores update automatically as new data is ingested.
                </div>
            </div>
            <div class="card">
                <div class="card-title">Active Pipeline</div>
                <div style="overflow-x:auto;">
                    <table class="ptable">
                        <thead>
                            <tr>
                                <th>County</th>
                                <th>State</th>
                                <th>Score</th>
                                <th>Grade</th>
                                <th>Target MW</th>
                                <th>Status</th>
                                <th>Added</th>
                            </tr>
                        </thead>
                        <tbody id="pipeline-body"></tbody>
                    </table>
                </div>
                <div id="pipeline-empty" class="loading" style="display:none;">
                    No sites in pipeline yet. Add one from the Deep Dive step.
                </div>
            </div>
            <div class="demo-nav">
                <button class="btn btn-secondary" onclick="goStep(2)">&larr; Back to Deep Dive</button>
                <span></span>
            </div>
        </div>

    </main>
    <div class="toast" id="toast"></div>

    <script>
        const API = 'http://localhost:8001/api/v1';
        let _selectedState = 'TX';
        let _selectedFips = null;
        let _selectedCountyName = '';
        let _stepsLoaded = {};

        // ===== Navigation =====
        function goStep(n) {
            document.querySelectorAll('.step-panel').forEach((p, i) => {
                p.classList.toggle('active', i === n);
            });
            document.querySelectorAll('.progress-step').forEach((s, i) => {
                s.classList.remove('active', 'completed');
                if (i < n) s.classList.add('completed');
                else if (i === n) s.classList.add('active');
            });
            if (!_stepsLoaded[n]) {
                _stepsLoaded[n] = true;
                loadStepData(n);
            }
        }

        function loadStepData(n) {
            if (n === 0) loadOverview();
            else if (n === 1) loadRankings();
            else if (n === 2) loadDeepDive();
            else if (n === 3) loadPipeline();
        }

        function showToast(msg) {
            const t = document.getElementById('toast');
            t.textContent = msg;
            t.classList.add('show');
            setTimeout(() => t.classList.remove('show'), 3000);
        }

        function scoreColor(s) {
            return s >= 85 ? 'green' : s >= 70 ? 'yellow' : 'red';
        }
        function gradeBadgeClass(s) {
            return s >= 85 ? 'badge-green' : s >= 70 ? 'badge-yellow' : 'badge-red';
        }
        function fmt(v, d = 1) { return v != null ? Number(v).toFixed(d) : 'N/A'; }
        function fmtDate(s) {
            if (!s) return '—';
            return new Date(s).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        }

        // ===== Step 0: Overview =====
        async function loadOverview() {
            const loading = document.getElementById('overview-loading');
            try {
                const r = await fetch(API + '/datacenter-sites/top-states?limit=15');
                if (!r.ok) throw new Error('No data');
                const data = await r.json();
                loading.style.display = 'none';

                if (!data.states || !data.states.length) {
                    document.getElementById('no-data-screen').style.display = '';
                    document.getElementById('step-0').style.display = 'none';
                    return;
                }

                // KPI strip
                const totalCounties = data.states.reduce((s, x) => s + (x.county_count || 0), 0);
                const totalA = data.states.reduce((s, x) => s + (x.a_grade || 0), 0);
                const avgScore = (data.states.reduce((s, x) => s + (x.avg_score || 0), 0) / data.states.length).toFixed(1);
                document.getElementById('overview-kpis').innerHTML = `
                    <div class="kpi"><div class="kpi-value">${data.states.length}</div><div class="kpi-label">States Scored</div></div>
                    <div class="kpi"><div class="kpi-value">${totalCounties.toLocaleString()}</div><div class="kpi-label">Counties Analyzed</div></div>
                    <div class="kpi"><div class="kpi-value">${totalA}</div><div class="kpi-label">A-Grade Counties</div></div>
                    <div class="kpi"><div class="kpi-value">${avgScore}</div><div class="kpi-label">Avg National Score</div></div>
                `;

                // State grid
                document.getElementById('state-grid').innerHTML = data.states.map(s => `
                    <div class="state-card ${s.state === _selectedState ? 'selected' : ''}"
                         onclick="selectState('${s.state}')">
                        <div class="sc-state">${s.state}</div>
                        <div class="sc-score">${fmt(s.avg_score)}</div>
                        <div class="sc-label">avg score</div>
                        <div class="sc-badge ${gradeBadgeClass(s.avg_score)}">${s.a_grade || 0} A-grade</div>
                    </div>
                `).join('');

                document.getElementById('btn-to-rankings').disabled = false;
            } catch(e) {
                loading.textContent = 'No scoring data found. Run POST /api/v1/datacenter-sites/score-counties first.';
            }
        }

        function selectState(state) {
            _selectedState = state;
            document.querySelectorAll('.state-card').forEach(c => {
                c.classList.toggle('selected', c.querySelector('.sc-state').textContent === state);
            });
            document.getElementById('btn-to-rankings').disabled = false;
            // Reset rankings for new state
            _stepsLoaded[1] = false;
        }

        // ===== Step 1: Rankings =====
        async function loadRankings() {
            const loading = document.getElementById('rankings-loading');
            const body = document.getElementById('rankings-body');
            loading.style.display = '';
            body.innerHTML = '';
            document.getElementById('rankings-title').textContent = `Step 2: County Rankings — ${_selectedState}`;
            document.getElementById('rankings-card-title').textContent = `Top Counties in ${_selectedState}`;
            try {
                const r = await fetch(API + `/datacenter-sites/rankings?state=${_selectedState}&limit=20`);
                if (!r.ok) throw new Error('Failed');
                const data = await r.json();
                loading.style.display = 'none';

                if (!data.counties || !data.counties.length) {
                    loading.style.display = '';
                    loading.textContent = `No scored counties found for ${_selectedState}.`;
                    return;
                }

                body.innerHTML = data.counties.map((c, i) => {
                    const sc = scoreColor(c.overall_score);
                    return `<tr class="${c.county_fips === _selectedFips ? 'selected' : ''}"
                                 onclick="selectCounty('${c.county_fips}', '${esc(c.county_name)}')">
                        <td><strong>${c.state_rank || (i + 1)}</strong></td>
                        <td><strong>${esc(c.county_name)}</strong></td>
                        <td>${c.state}</td>
                        <td class="pct-cell ${sc}">${fmt(c.overall_score)} <small style="color:var(--text-muted)">${c.grade || ''}</small></td>
                        <td>${fmt(c.power_score)}</td>
                        <td>${fmt(c.connectivity_score)}</td>
                        <td>${fmt(c.regulatory_score)}</td>
                        <td>${fmt(c.labor_score)}</td>
                        <td>${fmt(c.risk_score)}</td>
                    </tr>`;
                }).join('');
            } catch(e) {
                loading.style.display = '';
                loading.textContent = 'Failed to load rankings.';
            }
        }

        function selectCounty(fips, name) {
            _selectedFips = fips;
            _selectedCountyName = name;
            document.querySelectorAll('#rankings-body tr').forEach(r => r.classList.remove('selected'));
            event.currentTarget.classList.add('selected');
            document.getElementById('btn-to-deepdive').disabled = false;
            // Reset deep dive for new county
            _stepsLoaded[2] = false;
        }

        function esc(s) {
            if (!s) return '';
            return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
        }

        // ===== Step 2: Deep Dive =====
        async function loadDeepDive() {
            if (!_selectedFips) return;
            document.getElementById('deepdive-title').textContent = `Step 3: ${_selectedCountyName} — Deep Dive`;
            document.getElementById('dd-kpis').innerHTML = '<div class="loading">Loading...</div>';
            document.getElementById('thesis-output').style.display = 'none';
            document.getElementById('btn-thesis').disabled = false;
            document.getElementById('btn-thesis').textContent = 'Generate Investment Thesis';

            try {
                const r = await fetch(API + `/datacenter-sites/${_selectedFips}`);
                if (!r.ok) throw new Error('Not found');
                const c = await r.json();

                // KPI strip (6 domain scores)
                document.getElementById('dd-kpis').innerHTML = [
                    ['Overall', c.overall_score, c.grade],
                    ['Power', c.power_score, '30%'],
                    ['Connectivity', c.connectivity_score, '20%'],
                    ['Regulatory', c.regulatory_score, '20%'],
                    ['Labor', c.labor_score, '15%'],
                    ['Risk', c.risk_score, '10%'],
                ].map(([label, val, sub]) => `
                    <div class="kpi">
                        <div class="kpi-value" style="color:${val >= 85 ? 'var(--success)' : val >= 70 ? 'var(--warning)' : 'var(--error)'}">${fmt(val)}</div>
                        <div class="kpi-label">${label} <small>${sub}</small></div>
                    </div>
                `).join('');

                // Score bars
                document.getElementById('dd-bars').innerHTML = [
                    ['Power Infrastructure', c.power_score],
                    ['Connectivity & Fiber', c.connectivity_score],
                    ['Regulatory Speed', c.regulatory_score],
                    ['Labor Workforce', c.labor_score],
                    ['Risk & Environment', c.risk_score],
                    ['Cost & Incentives', c.cost_incentive_score],
                ].map(([label, val]) => {
                    const pct = Math.round(val || 0);
                    const col = pct >= 85 ? 'var(--success)' : pct >= 70 ? 'var(--warning)' : 'var(--error)';
                    return `<div class="ss-row">
                        <div class="ss-label">${label}</div>
                        <div class="ss-bar-bg"><div class="ss-bar-fill" style="width:${pct}%;background:${col}">${pct}</div></div>
                    </div>`;
                }).join('');

                // Raw metrics
                const metrics = [
                    ['Power Capacity Nearby', c.power_capacity_nearby_mw, 'MW'],
                    ['Substations Nearby', c.substations_count, ''],
                    ['Electricity Price', c.electricity_price_cents_kwh, '¢/kWh'],
                    ['Internet Exchanges', c.ix_count, ''],
                    ['DC Facilities Nearby', c.dc_facility_count, ''],
                    ['Broadband Coverage', c.broadband_coverage_pct, '%'],
                    ['Tech Employment', c.tech_employment, ' workers'],
                    ['Tech Avg Wage', c.tech_avg_wage, null],
                    ['Flood High-Risk Zones', c.flood_high_risk_zones, ''],
                    ['Wetland Acres', c.wetland_acres, 'ac'],
                    ['Incentive Programs', c.incentive_program_count, ''],
                    ['Opportunity Zone', c.opportunity_zone ? 'Yes' : 'No', ''],
                ];
                document.getElementById('dd-metrics').innerHTML = `<table class="ptable">
                    <tbody>${metrics.map(([label, val, unit]) => `
                        <tr>
                            <td style="color:var(--text-muted)">${label}</td>
                            <td><strong>${val != null ? (unit === null ? '$' + Number(val).toLocaleString() : Number(val).toLocaleString() + (unit || '')) : 'N/A'}</strong></td>
                        </tr>`).join('')}
                    </tbody>
                </table>`;

            } catch(e) {
                document.getElementById('dd-kpis').innerHTML = '<div class="loading">Failed to load site data.</div>';
            }
        }

        async function generateThesis() {
            if (!_selectedFips) return;
            const btn = document.getElementById('btn-thesis');
            const output = document.getElementById('thesis-output');
            btn.disabled = true;
            btn.textContent = 'Generating...';
            output.className = 'thesis-card loading';
            output.textContent = 'Analyzing 18 data dimensions and generating investment memo...';
            output.style.display = '';

            try {
                const r = await fetch(API + `/datacenter-sites/${_selectedFips}/thesis`, { method: 'POST' });
                if (!r.ok) throw new Error('Endpoint not available');
                const data = await r.json();
                output.className = 'thesis-card';
                if (data.thesis) {
                    output.innerHTML = data.thesis.replace(/\n/g, '<br>');
                } else {
                    output.textContent = data.error || 'Thesis generation requires LLM configuration (OPENAI_API_KEY or ANTHROPIC_API_KEY).';
                }
            } catch(e) {
                output.className = 'thesis-card';
                output.textContent = 'Thesis endpoint not yet available — deploy Agent 2 changes and restart API.';
            }
            btn.disabled = false;
            btn.textContent = 'Regenerate Thesis';
        }

        async function addToPipeline() {
            if (!_selectedFips) return;
            const btn = document.getElementById('btn-pipeline');
            btn.disabled = true;
            btn.textContent = 'Adding...';

            try {
                const r = await fetch(API + `/datacenter-sites/pipeline/${_selectedFips}`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ target_mw: 50, notes: '', status: 'Evaluating' }),
                });
                if (!r.ok) throw new Error('Pipeline endpoint not available');
                showToast(`${_selectedCountyName} added to pipeline`);
                _stepsLoaded[3] = false;  // Force reload pipeline
                goStep(3);
            } catch(e) {
                showToast('Pipeline endpoint not yet available — deploy Agent 3 changes.');
                btn.disabled = false;
                btn.textContent = 'Add to Pipeline &rarr;';
            }
        }

        // ===== Step 3: Pipeline =====
        async function loadPipeline() {
            const body = document.getElementById('pipeline-body');
            const empty = document.getElementById('pipeline-empty');
            body.innerHTML = '';
            empty.style.display = 'none';

            try {
                const r = await fetch(API + '/datacenter-sites/pipeline');
                if (!r.ok) throw new Error('Pipeline endpoint not available');
                const data = await r.json();

                if (!data || !data.length) {
                    empty.style.display = '';
                    return;
                }

                body.innerHTML = data.map(s => `
                    <tr>
                        <td><strong>${esc(s.county_name)}</strong></td>
                        <td>${s.state}</td>
                        <td class="pct-cell ${scoreColor(s.overall_score)}">${fmt(s.overall_score)}</td>
                        <td>${s.grade || '—'}</td>
                        <td>${s.target_mw ? s.target_mw + ' MW' : '—'}</td>
                        <td><span class="pill pill-active">${esc(s.status)}</span></td>
                        <td style="color:var(--text-muted)">${fmtDate(s.added_at)}</td>
                    </tr>
                `).join('');
            } catch(e) {
                empty.style.display = '';
                empty.textContent = 'Pipeline endpoint not yet available — deploy Agent 3 changes and restart API.';
            }
        }

        // ===== Init =====
        loadOverview();
    </script>
</body>
</html>
```

**Acceptance criteria:**
- All 4 steps render without JS errors in browser console
- Step 1 loads state grid from live DB; clicking a state card sets `_selectedState` and enables "View County Rankings" button
- Step 2 loads county table for selected state; clicking a row enables "Deep Dive" button
- Step 3 shows score breakdown + raw metrics for selected county
- "Generate Investment Thesis" button shows graceful error if Agent 2 endpoint not deployed
- "Add to Pipeline" button shows graceful error if Agent 3 endpoint not deployed, then advances to step 4 if it works
- Step 4 shows pipeline table, or "no sites" message if empty

---

### Agent 2 — AI Investment Thesis Endpoint

**Files owned:** `app/api/v1/datacenter_sites.py` — add one endpoint and one helper function only.
Do NOT touch `app/reports/templates/datacenter_site.py` or any frontend file.

**Exact LLM pattern to follow** (copy from `app/core/pe_thesis_generator.py` lines 200-235):
```python
import os
import asyncio
from app.agentic.llm_client import LLMClient

api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
provider = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic"

client = LLMClient(
    provider=provider,
    api_key=api_key,
    max_tokens=600,
    temperature=0.3,
)
response = await client.complete(prompt=prompt, system_prompt=system_prompt)
# response.content  → str
# response.model, response.input_tokens, response.output_tokens, response.cost_usd
```

**The thesis endpoint is async** — FastAPI handles async endpoints natively. Decorate with `@router.post(...)` and define as `async def`.

**Where to insert** in `app/api/v1/datacenter_sites.py`:
- Insert BEFORE the `@router.get("/{county_fips}")` route at line ~275
- Insert AFTER the `/report` endpoint
- This ordering matters — FastAPI matches routes top-to-bottom and `/{county_fips}` is a catch-all

**Add these two ALTER TABLE statements** (run at endpoint call time, not module load):
```python
db.execute(text("ALTER TABLE datacenter_site_scores ADD COLUMN IF NOT EXISTS thesis_text TEXT"))
db.execute(text("ALTER TABLE datacenter_site_scores ADD COLUMN IF NOT EXISTS thesis_generated_at TIMESTAMP"))
db.commit()
```

**Full endpoint to add:**

```python
@router.post("/{county_fips}/thesis")
async def generate_county_thesis(
    county_fips: str,
    db: Session = Depends(get_db),
):
    """Generate AI investment thesis for a scored county. Cached for 24h."""
    import os
    from datetime import date
    from sqlalchemy import text as _text

    # Ensure thesis columns exist
    try:
        db.execute(_text("ALTER TABLE datacenter_site_scores ADD COLUMN IF NOT EXISTS thesis_text TEXT"))
        db.execute(_text("ALTER TABLE datacenter_site_scores ADD COLUMN IF NOT EXISTS thesis_generated_at TIMESTAMP"))
        db.commit()
    except Exception:
        db.rollback()

    # Fetch county score
    result = db.execute(
        _text("""
            SELECT county_fips, county_name, state, overall_score, grade,
                   power_score, connectivity_score, regulatory_score,
                   labor_score, risk_score, cost_incentive_score,
                   electricity_price_cents_kwh, power_capacity_nearby_mw,
                   substations_count, ix_count, tech_employment, tech_avg_wage,
                   incentive_program_count, opportunity_zone,
                   thesis_text, thesis_generated_at
            FROM datacenter_site_scores
            WHERE county_fips = :fips
            ORDER BY score_date DESC LIMIT 1
        """),
        {"fips": county_fips},
    ).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail=f"County {county_fips} not scored")

    row = dict(zip(result.keys(), result))

    # Return cached thesis if generated today
    if row.get("thesis_generated_at") and row.get("thesis_text"):
        if row["thesis_generated_at"].date() == date.today():
            return {
                "county_fips": row["county_fips"],
                "county_name": row["county_name"],
                "state": row["state"],
                "overall_score": float(row["overall_score"]),
                "thesis": row["thesis_text"],
                "generated_at": row["thesis_generated_at"].isoformat(),
                "from_cache": True,
            }

    # Build LLM prompt
    ozone = "Yes" if row.get("opportunity_zone") else "No"
    prompt = f"""You are a real estate investment analyst. Write a concise investment thesis for a
datacenter development site in {row['county_name']} County, {row['state']}.

Site Scores (0-100):
- Overall: {row['overall_score']} (Grade {row['grade']})
- Power Infrastructure (30% weight): {row['power_score']} — {row.get('power_capacity_nearby_mw','N/A')} MW nearby capacity, {row.get('substations_count','N/A')} substations, {row.get('electricity_price_cents_kwh','N/A')}¢/kWh electricity
- Connectivity (20% weight): {row['connectivity_score']} — {row.get('ix_count','N/A')} internet exchanges nearby
- Regulatory Speed (20% weight): {row['regulatory_score']}
- Labor Workforce (15% weight): {row['labor_score']} — {row.get('tech_employment','N/A')} tech workers, avg wage ${row.get('tech_avg_wage','N/A')}
- Risk/Environment (10% weight): {row['risk_score']}
- Cost/Incentives (5% weight): {row['cost_incentive_score']} — {row.get('incentive_program_count','N/A')} incentive programs, Opportunity Zone: {ozone}

Write exactly 3 paragraphs (200 words total):
1. Why this site is compelling for datacenter investment
2. Key risks to underwrite before committing
3. Recommended next steps for due diligence

Be specific — reference the actual scores and metrics above."""

    # Try LLM
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {
            "county_fips": county_fips,
            "county_name": row["county_name"],
            "state": row["state"],
            "overall_score": float(row["overall_score"]),
            "thesis": None,
            "error": "LLM not configured — set OPENAI_API_KEY or ANTHROPIC_API_KEY",
        }

    try:
        from app.agentic.llm_client import LLMClient
        provider = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic"
        client = LLMClient(provider=provider, api_key=api_key, max_tokens=600, temperature=0.3)
        response = await client.complete(prompt=prompt)
        thesis_text = response.content.strip()

        # Cache to DB
        db.execute(
            _text("""
                UPDATE datacenter_site_scores
                SET thesis_text = :thesis, thesis_generated_at = NOW()
                WHERE county_fips = :fips
                AND score_date = (SELECT MAX(score_date) FROM datacenter_site_scores WHERE county_fips = :fips)
            """),
            {"thesis": thesis_text, "fips": county_fips},
        )
        db.commit()

        return {
            "county_fips": county_fips,
            "county_name": row["county_name"],
            "state": row["state"],
            "overall_score": float(row["overall_score"]),
            "thesis": thesis_text,
            "generated_at": "now",
            "from_cache": False,
        }

    except Exception as e:
        logger.warning(f"Thesis generation failed for {county_fips}: {e}")
        return {
            "county_fips": county_fips,
            "county_name": row["county_name"],
            "state": row["state"],
            "overall_score": float(row["overall_score"]),
            "thesis": None,
            "error": f"LLM call failed: {str(e)}",
        }
```

**Acceptance criteria:**
- `POST /api/v1/datacenter-sites/48001/thesis` returns a thesis string for a scored county
- Returns 404 for unknown county_fips
- Returns `{thesis: null, error: "..."}` (not 500) when no LLM key configured
- Second call same day returns `from_cache: true` without another LLM call
- The `ALTER TABLE IF NOT EXISTS` never fails on first call

---

### Agent 3 — Site Pipeline Backend

**Files owned:**
- `app/core/models_site_intel.py` — add `DatacenterSitePipeline` class at end of file
- `app/api/v1/datacenter_sites.py` — add 2 endpoints

**Exact model pattern** (match style of other models in `models_site_intel.py`, e.g. `EpochDatacenter`):

```python
class DatacenterSitePipeline(Base):
    """Shortlisted datacenter sites being tracked through selection process."""

    __tablename__ = "datacenter_site_pipeline"

    id = Column(Integer, primary_key=True, autoincrement=True)
    county_fips = Column(String(5), nullable=False, index=True)
    county_name = Column(String(255))
    state = Column(String(2), index=True)
    overall_score = Column(Numeric(5, 2))
    grade = Column(String(1))
    status = Column(String(50), default="Evaluating")
    # Evaluating | LOI | Under Contract | Passed
    notes = Column(Text, nullable=True)
    target_mw = Column(Integer, nullable=True)
    added_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("county_fips", name="uq_dc_pipeline_county"),
    )
```

Add after the `EpochDatacenter` class (which is the current last model in the file).

**Where to insert the two endpoints** in `app/api/v1/datacenter_sites.py`:
- Both go BEFORE `@router.get("/{county_fips}")` at line ~275 — CRITICAL for correct routing
- Insert after the existing `/report` endpoint
- Order in file: `POST /pipeline/{county_fips}` THEN `GET /pipeline` THEN `GET /{county_fips}`

**Full endpoints to add:**

```python
class PipelineAddRequest(BaseModel):
    target_mw: int = Field(50, ge=1, le=2000)
    notes: str = Field("", max_length=1000)
    status: str = Field("Evaluating")


@router.post("/pipeline/{county_fips}")
def add_to_pipeline(
    county_fips: str,
    request: PipelineAddRequest,
    db: Session = Depends(get_db),
):
    """Add or update a county in the site selection pipeline."""
    from sqlalchemy import text as _text

    # Look up county score
    score_row = db.execute(
        _text("""
            SELECT county_fips, county_name, state, overall_score, grade
            FROM datacenter_site_scores
            WHERE county_fips = :fips
            ORDER BY score_date DESC LIMIT 1
        """),
        {"fips": county_fips},
    ).fetchone()

    if not score_row:
        raise HTTPException(status_code=404, detail=f"County {county_fips} not scored. Run score-counties first.")

    score = dict(zip(score_row.keys(), score_row))

    # Upsert (idempotent on county_fips)
    db.execute(
        _text("""
            INSERT INTO datacenter_site_pipeline
                (county_fips, county_name, state, overall_score, grade, status, notes, target_mw, added_at, updated_at)
            VALUES
                (:fips, :name, :state, :score, :grade, :status, :notes, :mw, NOW(), NOW())
            ON CONFLICT (county_fips) DO UPDATE SET
                status = EXCLUDED.status,
                notes = EXCLUDED.notes,
                target_mw = EXCLUDED.target_mw,
                updated_at = NOW()
        """),
        {
            "fips": county_fips,
            "name": score["county_name"],
            "state": score["state"],
            "score": float(score["overall_score"]),
            "grade": score["grade"],
            "status": request.status,
            "notes": request.notes,
            "mw": request.target_mw,
        },
    )
    db.commit()

    return {
        "county_fips": county_fips,
        "county_name": score["county_name"],
        "state": score["state"],
        "overall_score": float(score["overall_score"]),
        "grade": score["grade"],
        "status": request.status,
        "target_mw": request.target_mw,
        "notes": request.notes,
    }


@router.get("/pipeline")
def get_pipeline(
    status: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get shortlisted sites in the selection pipeline."""
    from sqlalchemy import text as _text

    conditions = ["1=1"]
    params: Dict[str, Any] = {}
    if status:
        conditions.append("status = :status")
        params["status"] = status
    if state:
        conditions.append("state = :state")
        params["state"] = state.upper()

    where = " AND ".join(conditions)
    try:
        result = db.execute(
            _text(f"""
                SELECT county_fips, county_name, state, overall_score, grade,
                       status, notes, target_mw, added_at, updated_at
                FROM datacenter_site_pipeline
                WHERE {where}
                ORDER BY overall_score DESC
            """),
            params,
        )
        rows = [dict(zip(result.keys(), r)) for r in result.fetchall()]
        # Serialize datetime
        for r in rows:
            if r.get("added_at"):
                r["added_at"] = r["added_at"].isoformat()
            if r.get("updated_at"):
                r["updated_at"] = r["updated_at"].isoformat()
        return rows
    except Exception as e:
        logger.warning(f"Pipeline query failed: {e}")
        return []
```

**Table creation:** `DatacenterSitePipeline` is added to `models_site_intel.py` which shares `Base` with `models.py`. FastAPI calls `Base.metadata.create_all()` at startup, so the table will be created automatically on next restart.

**Acceptance criteria:**
- `POST /api/v1/datacenter-sites/pipeline/48001` with `{"target_mw": 100}` returns the pipeline record
- `GET /api/v1/datacenter-sites/pipeline` returns the entry
- Calling POST twice for the same county updates the existing row (no duplicates)
- `GET /pipeline` returns `[]` (not 500) when table is empty or no matches
- `GET /pipeline?state=TX` filters correctly
- Routes are ordered before `/{county_fips}` catch-all in router file

---

## File Ownership Matrix

| File | Agent 1 | Agent 2 | Agent 3 |
|---|---|---|---|
| `frontend/dc-demo.html` | ✅ owns (new) | ❌ | ❌ |
| `app/api/v1/datacenter_sites.py` | ❌ | adds 1 async endpoint + alter table logic | adds 2 endpoints + Pydantic model |
| `app/core/models_site_intel.py` | ❌ | ❌ | adds DatacenterSitePipeline class at EOF |

**Merge note:** Agents 2 and 3 both touch `datacenter_sites.py`. Master agent sequence:
1. Apply Agent 2's endpoint (insert before `/{county_fips}`)
2. Apply Agent 3's two endpoints (insert before `/{county_fips}`, after Agent 2's)
3. The final order in the file: `.../thesis` then `/pipeline/{fips}` then `/pipeline` then `/{fips}`

---

## Master Agent Merge + Verify Checklist

After all 3 agents finish:
1. Merge `datacenter_sites.py`: apply Agent 2 block, then Agent 3 block — both before `/{county_fips}`
2. `docker-compose restart api` — wait 25s
3. Test endpoints:
   ```bash
   curl -s http://localhost:8001/api/v1/datacenter-sites/pipeline | python -m json.tool
   curl -s -X POST http://localhost:8001/api/v1/datacenter-sites/pipeline/48001 \
     -H "Content-Type: application/json" -d '{"target_mw":50}' | python -m json.tool
   curl -s -X POST http://localhost:8001/api/v1/datacenter-sites/48001/thesis | python -m json.tool
   ```
4. Open `frontend/dc-demo.html` in browser, walk all 4 steps
5. Commit

---

## Expected Outcome

- `frontend/dc-demo.html` — live 4-step interactive demo using real scored county data
- AI thesis on any county, cached same-day, graceful degradation without LLM key
- Site pipeline to track shortlisted counties
- Existing 13-section static report `/report` untouched
- Zero new data sources, zero new scoring logic — all built on existing infrastructure
