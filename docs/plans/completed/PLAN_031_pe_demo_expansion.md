# PLAN 031 — PE Demo Expansion (2-Agent Parallel)

**Date:** 2026-03-23 (revised — no new endpoints)
**Status:** Awaiting approval
**Goal:** Expand `frontend/pe-demo.html` from its current 4-step exit-only story into a full
acquisition + exit platform with fund performance. **Zero new backend endpoints — all API calls
use endpoints that already exist.**

---

## Existing Endpoints Used (no new code in app/)

| Agent | Endpoint | What it returns |
|---|---|---|
| 1 | `GET /pe/analytics/{firmId}/benchmarks` | IRR/TVPI/DPI vs Cambridge PE median, quartile ranking |
| 1 | `GET /pe/deal-sourcing/{firmId}/candidates` | `{candidates:[{company_id,company_name,industry,score,grade,strengths,risks}], total}` |
| 1 | `POST /pe/companies/{id}/thesis/refresh` | LLM investment thesis for a company |
| 1 | `POST /deals` | Add company to deal pipeline |
| 2 | `GET /pe/funds/{firmId}` | `{funds:[{fund_id,fund_name,vintage_year,status,metrics:{irr,tvpi,dpi,rvpi}}]}` |
| 2 | `GET /pe/analytics/{firmId}/performance` | Firm-wide blended IRR, MOIC, TVPI, DPI, RVPI |

All URLs are at base `http://localhost:8001/api/v1`.

---

## Agent 1 — Acquisition Story Tab

**File owned:** `frontend/pe-demo.html` (acquisition sections only)

### What to build

Add a story toggle **above** the progress bar — two buttons: `Acquisition` and `Exit`. Clicking
switches which steps are shown. The existing 4-step exit flow stays intact with `id="exit-story"`.
The new acquisition flow has `id="acq-story"` and is hidden by default (`display:none`).

#### Story toggle HTML (insert after `<div class="header">...</div>`, before the progress bar)

```html
<div class="story-toggle" id="story-toggle">
  <button class="toggle-btn active" id="btn-exit" onclick="switchStory('exit')">Exit / Disposition</button>
  <button class="toggle-btn" id="btn-acq" onclick="switchStory('acq')">Acquisition</button>
</div>
```

#### Story toggle CSS (add to existing `<style>` block)

```css
.story-toggle { display:flex; gap:0.5rem; justify-content:center; margin-bottom:1.5rem; }
.toggle-btn { padding:0.45rem 1.2rem; border-radius:6px; border:1px solid var(--border);
  background:transparent; color:var(--text-muted); cursor:pointer; font-size:0.875rem;
  transition:all 0.2s; }
.toggle-btn.active { background:var(--primary); color:#fff; border-color:var(--primary); }
```

#### Story toggle JS (add to existing `<script>` block)

```javascript
function switchStory(mode) {
  document.getElementById('exit-story').style.display = mode === 'exit' ? '' : 'none';
  document.getElementById('acq-story').style.display  = mode === 'acq'  ? '' : 'none';
  document.getElementById('btn-exit').classList.toggle('active', mode === 'exit');
  document.getElementById('btn-acq').classList.toggle('active', mode === 'acq');
}
```

#### Acquisition story container (insert after exit story closing `</div>`, before `</main>`)

Wrap all acquisition content in `<div id="acq-story" style="display:none">`.

4 steps — each step is a `<div class="step-content">` that shows/hides with a next/back pattern
identical to the existing exit steps:

---

**Step A1 — Market Scanner**
Narration: *"Before we source a deal, we scan the market. Which sectors have momentum right now?"*

```javascript
async function loadAcqStep1() {
  const r = await fetch(`${API_BASE}/pe/analytics/${_firmId}/benchmarks`);
  const data = await r.json();
  // data has: firm_irr, firm_tvpi, benchmark_irr (Cambridge median), quartile, funds_summary
  // Show 3 KPI cards: Firm IRR vs. Benchmark, TVPI, Quartile ranking
  document.getElementById('acq-s1-firm-irr').textContent  = (data.firm_irr || '--') + '%';
  document.getElementById('acq-s1-bench-irr').textContent = (data.benchmark_irr || '--') + '%';
  document.getElementById('acq-s1-tvpi').textContent      = (data.firm_tvpi || '--') + 'x';
  document.getElementById('acq-s1-quartile').textContent  = data.quartile || '--';
}
```

KPI card HTML (use existing `.kpi-card`, `.kpi-value`, `.kpi-label` classes):
```html
<div class="kpi-grid">
  <div class="kpi-card"><div class="kpi-value" id="acq-s1-firm-irr">--</div><div class="kpi-label">Firm Net IRR</div></div>
  <div class="kpi-card"><div class="kpi-value" id="acq-s1-bench-irr">--</div><div class="kpi-label">PE Median IRR</div></div>
  <div class="kpi-card"><div class="kpi-value" id="acq-s1-tvpi">--</div><div class="kpi-label">Firm TVPI</div></div>
  <div class="kpi-card"><div class="kpi-value" id="acq-s1-quartile">--</div><div class="kpi-label">Quartile</div></div>
</div>
```

---

**Step A2 — Target Discovery**
Narration: *"Here are today's highest-scored acquisition targets — ranked by AI deal score."*

```javascript
async function loadAcqStep2() {
  const r = await fetch(`${API_BASE}/pe/deal-sourcing/${_firmId}/candidates`);
  const data = await r.json();
  const tbody = document.getElementById('acq-candidates-body');
  tbody.innerHTML = '';
  if (!data.candidates || data.candidates.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-muted)">No candidates — run market scan first</td></tr>';
    return;
  }
  for (const co of data.candidates) {
    const scoreColor = co.score >= 75 ? 'var(--success)' : co.score >= 55 ? 'var(--warning)' : 'var(--danger)';
    tbody.innerHTML += `<tr>
      <td>${co.company_name}</td>
      <td>${co.industry || '--'}</td>
      <td style="font-weight:600;color:${scoreColor}">${co.score} <span style="font-weight:400;color:var(--text-muted)">(${co.grade})</span></td>
      <td style="font-size:0.78rem;color:var(--text-muted)">${(co.strengths || []).slice(0,2).join(' · ')}</td>
      <td><button class="btn btn-sm" onclick="selectAcqTarget(${co.company_id},'${co.company_name.replace(/'/g,"\\'")}')">Deep Dive →</button></td>
    </tr>`;
  }
}

let _acqTargetId = null, _acqTargetName = '';
function selectAcqTarget(id, name) {
  _acqTargetId = id; _acqTargetName = name;
  acqNextStep(3);  // jump to deep dive
}
```

Table HTML:
```html
<table class="data-table">
  <thead><tr><th>Company</th><th>Industry</th><th>Score</th><th>Strengths</th><th></th></tr></thead>
  <tbody id="acq-candidates-body"></tbody>
</table>
```

---

**Step A3 — Company Deep Dive**
Narration: *"AI-generated investment thesis — built from 28 public data sources."*

```javascript
async function loadAcqStep3() {
  if (!_acqTargetId) return;
  document.getElementById('acq-target-name').textContent = _acqTargetName;
  document.getElementById('acq-thesis-text').textContent = 'Generating thesis…';
  try {
    const r = await fetch(`${API_BASE}/pe/companies/${_acqTargetId}/thesis/refresh`, {method:'POST'});
    const data = await r.json();
    const thesis = data.thesis;
    if (thesis && thesis.summary) {
      document.getElementById('acq-thesis-text').textContent = thesis.summary;
    } else if (thesis && thesis.thesis) {
      document.getElementById('acq-thesis-text').textContent = thesis.thesis;
    } else {
      document.getElementById('acq-thesis-text').textContent = 'Thesis generation requires LLM API key.';
    }
  } catch(e) {
    document.getElementById('acq-thesis-text').textContent = 'Unable to generate thesis — check API connection.';
  }
}
```

HTML:
```html
<h3 id="acq-target-name" style="margin-bottom:1rem"></h3>
<div class="narration-box">
  <p id="acq-thesis-text" style="line-height:1.7"></p>
</div>
```

---

**Step A4 — Add to Pipeline**
Narration: *"One click to track this deal."*

```javascript
async function addAcqToPipeline() {
  if (!_acqTargetId) return;
  const btn = document.getElementById('acq-add-btn');
  btn.disabled = true; btn.textContent = 'Adding…';
  try {
    const r = await fetch(`${API_BASE}/deals`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        company_name: _acqTargetName,
        stage: 'prospecting',
        deal_type: 'acquisition',
        notes: 'Added from AI deal sourcing'
      })
    });
    const data = await r.json();
    document.getElementById('acq-pipeline-result').innerHTML =
      `<div class="narration-box" style="border-color:var(--success)">
        ✓ <strong>${_acqTargetName}</strong> added to pipeline — Deal ID: ${data.id || data.deal_id || '—'}<br>
        <span style="color:var(--text-muted);font-size:0.85rem">Win probability scoring will run overnight</span>
      </div>`;
    btn.style.display = 'none';
  } catch(e) {
    btn.disabled = false; btn.textContent = 'Add to Pipeline';
    document.getElementById('acq-pipeline-result').textContent = 'Error — check API connection.';
  }
}
```

HTML:
```html
<button class="btn btn-primary" id="acq-add-btn" onclick="addAcqToPipeline()">
  Add to Pipeline →
</button>
<div id="acq-pipeline-result" style="margin-top:1rem"></div>
```

---

### Navigation JS for acquisition steps

```javascript
let _acqStep = 1;
const ACQ_STEPS = 4;

function acqNextStep(target) {
  _acqStep = target || _acqStep + 1;
  for (let i = 1; i <= ACQ_STEPS; i++) {
    const el = document.getElementById(`acq-step-${i}`);
    if (el) el.style.display = i === _acqStep ? '' : 'none';
  }
  if (_acqStep === 1) loadAcqStep1();
  if (_acqStep === 2) loadAcqStep2();
  if (_acqStep === 3) loadAcqStep3();
}

function acqPrevStep() { if (_acqStep > 1) acqNextStep(_acqStep - 1); }
```

Each step div uses `id="acq-step-1"`, `id="acq-step-2"`, etc. Steps 2–4 start with `display:none`.

---

## Agent 2 — Fund Performance Step

**File owned:** `frontend/pe-demo.html` (fund performance step only, inserted into exit story)

### What to build

Insert a new **Step 4: Fund Performance** between the existing Step 3 (Exit Planning) and the
existing Step 4 (Leadership Network), which becomes Step 5. Update the progress bar to show 5 steps.

#### Progress bar update

Find `step-label` and update the max steps reference:
- Change all `Step X of 4` references to `Step X of 5`
- Add a 5th `.progress-step` element: `<div class="progress-step" data-step="4">LP Reporting</div>`

#### Fund performance step HTML (insert as new step 4 in exit story, before the leadership network step)

```html
<div id="step-4" class="step-content" style="display:none">
  <div class="narration-box">
    <p>When this exit closes, your LP quarterly report is already built. Here's the fund performance your investors see.</p>
  </div>

  <div class="kpi-grid" id="fund-kpi-grid" style="margin-bottom:1.5rem">
    <div class="kpi-card"><div class="kpi-value" id="fund-blended-irr">--</div><div class="kpi-label">Blended Net IRR</div></div>
    <div class="kpi-card"><div class="kpi-value" id="fund-tvpi">--</div><div class="kpi-label">TVPI</div></div>
    <div class="kpi-card"><div class="kpi-value" id="fund-dpi">--</div><div class="kpi-label">DPI (Realized)</div></div>
    <div class="kpi-card"><div class="kpi-value" id="fund-moic">--</div><div class="kpi-label">MOIC</div></div>
  </div>

  <h4 style="color:var(--text-muted);font-size:0.8rem;text-transform:uppercase;letter-spacing:.06em;margin-bottom:0.75rem">Fund Breakdown</h4>
  <table class="data-table" id="fund-perf-table">
    <thead>
      <tr><th>Fund</th><th>Vintage</th><th>IRR</th><th>TVPI</th><th>DPI</th><th>Status</th></tr>
    </thead>
    <tbody id="fund-perf-body"></tbody>
  </table>

  <div style="margin-top:1.5rem;text-align:right">
    <button class="btn" style="background:transparent;border:1px solid var(--border);color:var(--text-muted)"
      onclick="alert('LP memo generation would trigger here')">
      Generate LP Memo →
    </button>
  </div>

  <div class="step-nav">
    <button class="btn" onclick="showStep(3)">← Back</button>
    <button class="btn btn-primary" onclick="showStep(5)">Leadership Network →</button>
  </div>
</div>
```

#### Fund performance JS

```javascript
async function loadFundPerformance() {
  // Firm-wide blended metrics
  try {
    const r = await fetch(`${API_BASE}/pe/analytics/${_firmId}/performance`);
    const d = await r.json();
    document.getElementById('fund-blended-irr').textContent = d.blended_irr != null ? d.blended_irr.toFixed(1) + '%' : '--';
    document.getElementById('fund-tvpi').textContent         = d.blended_tvpi != null ? d.blended_tvpi.toFixed(2) + 'x' : '--';
    document.getElementById('fund-dpi').textContent          = d.blended_dpi  != null ? d.blended_dpi.toFixed(2) + 'x' : '--';
    document.getElementById('fund-moic').textContent         = d.blended_moic != null ? d.blended_moic.toFixed(2) + 'x' : '--';
  } catch(e) {
    console.warn('Performance fetch failed', e);
  }

  // Per-fund breakdown
  try {
    const r2 = await fetch(`${API_BASE}/pe/funds/${_firmId}`);
    const d2 = await r2.json();
    const tbody = document.getElementById('fund-perf-body');
    tbody.innerHTML = '';
    for (const f of (d2.funds || [])) {
      const m = f.metrics || {};
      const irr = m.irr != null ? m.irr.toFixed(1) + '%' : '--';
      const irrColor = m.irr >= 20 ? 'var(--success)' : m.irr >= 10 ? 'var(--warning)' : m.irr != null ? 'var(--danger)' : 'var(--text-muted)';
      tbody.innerHTML += `<tr>
        <td>${f.fund_name}</td>
        <td>${f.vintage_year || '--'}</td>
        <td style="font-weight:600;color:${irrColor}">${irr}</td>
        <td>${m.tvpi != null ? m.tvpi.toFixed(2) + 'x' : '--'}</td>
        <td>${m.dpi  != null ? m.dpi.toFixed(2)  + 'x' : '--'}</td>
        <td><span class="status-pill">${f.status || '--'}</span></td>
      </tr>`;
    }
    if (!d2.funds || d2.funds.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted)">No fund data — seed demo first</td></tr>';
    }
  } catch(e) {
    console.warn('Funds fetch failed', e);
  }
}
```

#### Update `showStep()` to call `loadFundPerformance()` when step 4 is shown

Find the existing `showStep(n)` function and add:
```javascript
if (n === 4) loadFundPerformance();
```

Also update the leadership network step — it must change from `id="step-4"` to `id="step-5"` and its
back/forward buttons updated from `showStep(3)` → `showStep(4)` and `showStep(5)` → `showStep(6)` etc.

---

## Master Agent Role

After both agents complete:

1. Check that Agent 1 didn't touch the exit steps and Agent 2 didn't touch the acquisition steps
2. Verify step numbering in the exit flow is consistent (steps 1–5, leadership network now step 5)
3. Restart API (`docker-compose restart api`, wait 30s)
4. Open `frontend/pe-demo.html` in browser:
   - Toggle to Acquisition → step through A1–A4 — all calls return data or graceful empty states
   - Toggle to Exit → step through 1–5 — Fund Performance step loads, leadership network still works
5. Commit: `feat: add acquisition story and fund performance step to PE demo`

---

## Copy-paste agent instructions

### Agent 1 instruction
```
You are Agent 1 for PLAN_031 (revised). Working directory: this worktree.
Write BYPASS_TRIVIAL to docs/specs/.active_spec before editing.

Edit frontend/pe-demo.html only. Make these additions:

1. Add story-toggle CSS to the existing <style> block
2. Add story toggle HTML (two buttons: "Exit / Disposition" and "Acquisition") directly above the progress bar
3. Add switchStory() JS function to the existing <script> block
4. Wrap the existing exit steps in <div id="exit-story">...</div>
5. Add <div id="acq-story" style="display:none"> containing 4 acquisition steps (A1–A4) with all HTML, JS functions, and nav buttons exactly as written in PLAN_031
6. Add acqNextStep() and acqPrevStep() JS functions

DO NOT change the existing exit steps, progress bar, or step numbering.
DO NOT modify any Python files.
Use the exact endpoint URLs, JS function names, and CSS classes from PLAN_031.
Copy the code exactly — do not paraphrase or abbreviate.
```

### Agent 2 instruction
```
You are Agent 2 for PLAN_031 (revised). Working directory: this worktree.
Write BYPASS_TRIVIAL to docs/specs/.active_spec before editing.

Edit frontend/pe-demo.html only. Make these additions:

1. Insert a new step 4 (Fund Performance) into the exit story between the current step 3 and current step 4
2. Renumber the current step-4 (Leadership Network) to step-5 — update its id, its back/forward button calls
3. Update the progress bar to show 5 steps — add a 5th .progress-step div labeled "LP Reporting"
4. Update the step-label "Step X of 4" references to "Step X of 5"
5. Add loadFundPerformance() JS function using GET /pe/analytics/{_firmId}/performance and GET /pe/funds/{_firmId} exactly as in PLAN_031
6. Add if (n === 4) loadFundPerformance(); to the existing showStep() function

DO NOT change steps 1, 2, or 3. DO NOT touch the acquisition story sections.
DO NOT modify any Python files.
Use the exact endpoint URLs, element IDs, and JS patterns from PLAN_031.
Copy the code exactly — do not paraphrase or abbreviate.
```
