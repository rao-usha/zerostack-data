# PLAN 031 — PE Demo Expansion (3-Agent Parallel)

**Date:** 2026-03-23
**Status:** Awaiting approval
**Goal:** Expand the PE demo from its current 4-step exit-only web UI into a full acquisition + exit platform with job posting intelligence and fund performance storytelling.

---

## Current State (After Deep Dive)

### What's built but NOT yet in the web demo
| Capability | Backend | Web Demo | Terminal Demo |
|---|---|---|---|
| Acquisition story / target discovery | ✅ | ❌ | ✅ |
| Job posting velocity as deal signal | ✅ (67 co, 13K posts) | ❌ | partial |
| Fund performance (IRR/TVPI/DPI curves) | ✅ | ❌ | ❌ |
| AI investment thesis | ✅ | ❌ | ❌ |
| Comparable transactions | schema ✅, logic partial | ❌ | ❌ |
| Market scanner / sector momentum | ✅ | ❌ | ✅ |
| Portfolio analytics (firm IRR, PME) | ✅ | ❌ | partial |

### What the web demo shows today
- Step 1: Portfolio heatmap (companies benchmarked vs peers)
- Step 2: Deep dive — radar chart + exit readiness score
- Step 3: Buyer analysis + data room completeness
- Step 4: Leadership network graph

The demo tells only the **exit/disposition** story. There is no acquisition story in the UI.

---

## The Three Agents

Each agent owns a clean set of files with no overlap.

---

### Agent 1 — Acquisition Story Tab (Frontend + thin backend)

**Files owned:**
- `frontend/pe-demo.html` — add tab/mode switcher and new acquisition steps
- `app/api/v1/pe_benchmarks.py` — add one endpoint: `GET /pe/deal-sourcing/top` (ranked acquisition targets from seeded portfolio companies)

**What to build:**

Add a story switcher to `pe-demo.html` (toggle between "Acquisition" and "Exit"). The acquisition story has 4 steps:

1. **Market Scanner** — call `GET /pe/analytics/{firm_id}/benchmarks` and `GET /market/opportunities` — show sector momentum cards (deal count trend, avg EBITDA multiple, momentum score)

2. **Target Discovery** — call `GET /pe/companies/?ownership_status=Founder-Owned&limit=20` and `GET /pe/deal-sourcing/top` — ranked target list with health score bars, hiring velocity indicator (green/yellow/red), sector pill

3. **Company Deep Dive** — when user clicks a target, show:
   - Health score gauge (reuse exit readiness endpoint, labeled differently)
   - Investment thesis card (call `POST /pe/companies/{id}/thesis/refresh`, show LLM-generated thesis text)
   - Job posting velocity sparkline (call `GET /job-postings/trends/market` filtered by company)

4. **Add to Pipeline** — call `POST /deals` with pre-filled form — show success state with deal ID + win probability

**Acceptance criteria:**
- Toggle between "Acquisition" / "Exit" stories with one click
- All 4 acquisition steps load without errors against seeded data
- Job posting velocity renders even if 0 (shows "No postings tracked")
- Investment thesis card shows either LLM text or a "Generating..." loading state

---

### Agent 2 — Fund Performance & LP Story (New web demo section)

**Files owned:**
- `frontend/pe-demo.html` — add Step 5: Fund Performance (new step in exit story flow)
- `app/api/v1/pe_benchmarks.py` — add `GET /pe/analytics/{firm_id}/lp-summary` endpoint

**What to build:**

Add a **Step 5: Fund Performance & LP Reporting** to the existing exit story:

**LP Summary endpoint** (`GET /pe/analytics/{firm_id}/lp-summary`):
- Pull from `pe_fund_performance` table: latest IRR, TVPI, DPI, RVPI per fund
- Pull from `pe_cash_flows`: total called, total distributed, net cash position
- Return fund name, vintage, performance metrics, % called, % returned

**Web UI — Step 5:**
- Fund performance table: Fund Name | Vintage | IRR | TVPI | DPI | Status
- Color-coded rows: green if IRR > 20%, yellow 10-20%, red < 10%
- Cash flow bar chart: Called vs Distributed per fund (Chart.js horizontal bar)
- Portfolio company status breakdown: Active / Exited / Written Off (donut chart)
- Narration box: "When this deal closes, your LP quarterly report is pre-built."

**Acceptance criteria:**
- Step 5 renders correctly in the exit flow (after Data Room, before end)
- Fund table populates from seeded Summit Ridge data (2 funds, realistic IRRs)
- Charts render without errors even if only 1 fund has performance data
- No hardcoded firm IDs — use `_firmId` variable

---

### Agent 3 — Job Posting Intelligence Integration (Backend + Seeder)

**Files owned:**
- `app/sources/pe/demo_seeder.py` — add job posting data for Summit Ridge portfolio companies
- `app/api/v1/pe_benchmarks.py` — add `GET /pe/companies/{company_id}/hiring-signals` endpoint

**What to build:**

**Hiring signals endpoint** (`GET /pe/companies/{company_id}/hiring-signals`):
- Join `pe_portfolio_companies.name` → fuzzy match `job_postings.company_name`
- Return: total_open_roles, growth_30d_pct, top_departments (list), top_skills (list), signal_label ("Scaling Fast" / "Stable" / "Contracting" / "No Data")
- Include `has_data: bool` so UI can gracefully show "No Data" state

**Demo seeder additions:**
- For each of the 8 Summit Ridge portfolio companies, add `job_postings` rows:
  - MedVantage: 34 open roles, +18% 30d growth, top depts: Engineering, Sales, Clinical
  - CloudShield: 28 open roles, +41% growth (scaling fast), top: Engineering, DevSecOps
  - TrueNorth Behavioral: 12 open roles, -5% growth, top: Clinical, Operations
  - Apex Revenue Solutions: 19 open roles, +8%, top: Account Mgmt, Operations
  - Other 4 companies: seed with realistic mix of growth/stable/contracting signals
- Use existing `job_postings` table schema (company_name, title, department, skills, posted_date, status)

**Update exit readiness scoring** (`app/core/pe_exit_scoring.py`):
- If hiring signal data exists for a company, incorporate `growth_30d_pct` into the Financial Health sub-score (hiring velocity = proxy for revenue growth momentum)
- Weight: 5% of Financial Health dimension (small — this is a signal, not a fundamental)

**Acceptance criteria:**
- `GET /pe/companies/{medvantage_id}/hiring-signals` returns data after seed
- Seeder is idempotent (running twice doesn't duplicate postings)
- Exit readiness score changes slightly when hiring data is present vs absent
- `has_data: false` response when no postings match company name

---

## File Ownership Matrix (No Conflicts)

| File | Agent 1 | Agent 2 | Agent 3 |
|---|---|---|---|
| `frontend/pe-demo.html` | ✅ owns | ✅ owns | ❌ |
| `app/api/v1/pe_benchmarks.py` | adds 1 endpoint | adds 1 endpoint | adds 1 endpoint |
| `app/sources/pe/demo_seeder.py` | ❌ | ❌ | ✅ owns |
| `app/core/pe_exit_scoring.py` | ❌ | ❌ | ✅ owns |

**⚠️ Conflict note:** All three agents touch `pe_benchmarks.py`. Resolve by:
- Agent 1 appends its endpoint at line ~470 (after portfolio heatmap)
- Agent 2 appends its endpoint at line ~520 (after leadership graph)
- Agent 3 appends its endpoint at line ~660 (after buyer analysis)
- Master agent merges all three changes into pe_benchmarks.py at the end

---

## Master Agent Role

The master agent (this session) does NOT write feature code. Responsibilities:
1. Issue the three instructions below
2. Review each agent's output for correctness
3. Merge the three pe_benchmarks.py changes without conflict
4. Run a final integration check (`docker-compose restart api` + test each new endpoint)
5. Commit the combined result

---

## Instructions to Issue (Copy-Paste Ready)

### Instruction for Agent 1
```
You are Agent 1 working on the Nexdata PE demo codebase at /c/Users/awron/projects/Nexdata.

Your task: Add an acquisition story to frontend/pe-demo.html and one backend endpoint.

CONTEXT:
- pe-demo.html currently shows a 4-step exit/disposition story
- _firmId is a dynamic variable (not hardcoded) resolved from the DB
- The demo seeds 3 firms (Summit Ridge Partners, Cascade Growth Equity, Ironforge Industrial Capital)
- Seeded portfolio companies: MedVantage Health Systems, CloudShield Security, TrueNorth Behavioral, Apex Revenue Solutions, Precision Lab Diagnostics, Elevate Staffing Group (under Summit Ridge)
- API base: /api/v1, running on port 8001
- PE benchmarks router prefix: /pe (registered at /api/v1/pe)

BACKEND: Add to app/api/v1/pe_benchmarks.py
Add endpoint: GET /pe/deal-sourcing/top
- Query pe_portfolio_companies WHERE ownership_status NOT IN ('PE-Backed', 'Public') OR ownership_status IS NULL
- If no founder-owned companies exist, fall back to all pe_portfolio_companies (the seeded ones are PE-Backed for demo purposes)
- Join with pe_company_financials for latest revenue_growth_pct
- Return top 10 ranked by revenue_growth_pct DESC as list of: {company_id, name, industry, headquarters_state, employee_count, revenue_growth_pct, ownership_status}

FRONTEND: Add to frontend/pe-demo.html
1. Add a story toggle at the top (below the progress bar): two buttons "Acquisition" / "Exit" that switch which story steps are visible
2. Add 4 acquisition steps (hidden by default, shown when "Acquisition" is selected):
   - Step A1: Market Scanner — call GET /pe/analytics/{_firmId}/benchmarks, show 3 sector cards with deal count, avg multiple, momentum label
   - Step A2: Target Discovery — call GET /pe/deal-sourcing/top, show ranked table: Company | Industry | State | Employees | Rev Growth | Action button
   - Step A3: Company Deep Dive — when action button clicked, call POST /pe/companies/{id}/thesis/refresh and show the returned thesis text in a card; also show a static "Hiring Signal" placeholder (will be wired in Agent 3's work)
   - Step A4: Add to Pipeline — show a success confirmation card with: "Added to pipeline — AI win probability: calculating..."
3. Keep all existing exit steps (steps 0-3) intact and working
4. Use the existing CSS variables and card/button styles already in the file

Do NOT modify app/sources/pe/demo_seeder.py or app/core/pe_exit_scoring.py — those belong to Agent 3.
Do NOT add any LP/fund performance steps — those belong to Agent 2.
```

### Instruction for Agent 2
```
You are Agent 2 working on the Nexdata PE demo codebase at /c/Users/awron/projects/Nexdata.

Your task: Add a Fund Performance step to the exit story in frontend/pe-demo.html and one backend endpoint.

CONTEXT:
- pe-demo.html currently shows a 4-step exit/disposition story ending with the Leadership Network
- _firmId is a dynamic variable resolved by name lookup (not hardcoded)
- Demo firm: Summit Ridge Partners has 2 funds (Fund III: 2019 vintage, ~19% IRR, 2.1x TVPI; Fund IV: 2023 vintage, ~12.5% IRR, 1.28x TVPI)
- pe_fund_performance table has: fund_id, as_of_date, irr, tvpi, dpi, rvpi, nav_usd, called_pct
- pe_funds table has: id, firm_id, name, vintage_year, final_close_usd_millions, status
- pe_cash_flows table has: fund_id, flow_date, amount, cash_flow_type (capital_call/distribution/fee)

BACKEND: Add to app/api/v1/pe_benchmarks.py
Add endpoint: GET /pe/analytics/{firm_id}/lp-summary
- Join pe_funds + pe_fund_performance (latest snapshot per fund) + pe_cash_flows (aggregated totals)
- Return: list of {fund_name, vintage_year, fund_size_millions, irr, tvpi, dpi, rvpi, called_pct, distributed_pct, status}
- If no performance data, return fund record with nulls for metrics (so UI can show "Vintage too early")

FRONTEND: Add to frontend/pe-demo.html
Add Step 4 (renumber current step 4 "Leadership Network" to step 5):
- New Step 4: "Fund Performance" inserted between "Exit Planning" (step 3) and "Leadership Network" (step 5)
- Progress bar should show 5 steps: Your Portfolio | Deep Dive | Exit Planning | Fund Performance | Leadership Network

Fund Performance step UI:
1. Narration box: "When this deal closes, your LP quarterly report is pre-built. Here's the fund performance your investors see."
2. Fund performance table: Fund Name | Vintage | Size | IRR | TVPI | DPI | Status — color rows green (IRR>20%), yellow (10-20%), red (<10% or null)
3. Cash position summary: 2 KPI cards — "Total Called" (sum of capital_calls) and "Total Distributed" (sum of distributions)
4. Narration at bottom: "Auto-generated LP memo available in one click." with a button that shows an alert("LP memo generation would trigger here")

Keep all existing steps intact. Use _firmId for the API call.
Do NOT touch the acquisition story — that belongs to Agent 1.
Do NOT modify app/sources/pe/demo_seeder.py — that belongs to Agent 3.
```

### Instruction for Agent 3
```
You are Agent 3 working on the Nexdata PE demo codebase at /c/Users/awron/projects/Nexdata.

Your task: Add job posting intelligence to the PE demo — seed data + backend endpoint + exit scoring integration.

CONTEXT:
- Summit Ridge Partners has 8 portfolio companies seeded in demo_seeder.py
- Existing job_postings table: company_name (text), title (text), department (text), skills (JSONB or text[]), location_city, location_state, posted_date, status (open/closed), source
- The demo seeder in app/sources/pe/demo_seeder.py is idempotent — check for existing rows before inserting
- app/core/pe_exit_scoring.py contains the exit readiness scoring logic (6 dimensions)
- API router prefix for pe_benchmarks: /pe (full path: /api/v1/pe)

TASK 1 — Seeder: Add to seed_pe_demo_data() in app/sources/pe/demo_seeder.py
Add a new phase at the end: seed job postings for Summit Ridge portfolio companies.
Use these realistic signals:
- MedVantage Health Systems: 34 open roles, Engineering(12), Sales(8), Clinical(9), Ops(5) — skills: Python, AWS, Epic EHR — status: scaling
- CloudShield Security: 28 open roles, Engineering(18), DevSecOps(6), Sales(4) — skills: Kubernetes, SIEM, Go — status: scaling fast
- TrueNorth Behavioral: 12 open roles, Clinical(7), Operations(4), Admin(1) — status: stable
- Apex Revenue Solutions: 19 open roles, Account Mgmt(8), Operations(7), Tech(4) — status: stable
- Precision Lab Diagnostics: 8 open roles, Lab Tech(5), Ops(3) — status: stable
- Elevate Staffing Group: 6 open roles, Recruiting(4), Sales(2) — status: contracting

Seed ~3-5 individual job postings per company (specific titles like "Senior Software Engineer", "Account Executive", etc.)
Check for duplicates by (company_name, title, posted_date) before inserting.
Add counts["job_postings"] to the returned counts dict.

TASK 2 — Endpoint: Add to app/api/v1/pe_benchmarks.py
Add endpoint: GET /pe/companies/{company_id}/hiring-signals
- Look up company name from pe_portfolio_companies
- Query job_postings WHERE company_name ILIKE '%{name}%' AND status = 'open'
- Count total open roles, count by department (top 3), collect top skills (top 5)
- Calculate growth_30d_pct: compare count(posted_date > 30 days ago) vs count(posted_date > 60 days ago)
- Return: {company_id, company_name, has_data: bool, total_open_roles, growth_30d_pct, signal_label, top_departments, top_skills}
- signal_label logic: growth_30d_pct > 20% → "Scaling Fast", 5-20% → "Growing", -5 to 5% → "Stable", < -5% → "Contracting", no data → "No Data"

TASK 3 — Exit scoring: Update app/core/pe_exit_scoring.py
In the Financial Health dimension scoring function:
- Accept an optional hiring_signal dict parameter
- If provided and has_data=True, add a small bonus: growth_30d_pct * 0.05 (capped at +5 points) to the financial health raw score
- Log the adjustment so it's visible in the scoring breakdown

Do NOT modify frontend/pe-demo.html — that belongs to Agents 1 and 2.
```

---

## Merge Order (Master Agent)

After all 3 agents finish:
1. Take pe_benchmarks.py changes from Agent 1 (deal-sourcing/top endpoint)
2. Apply pe_benchmarks.py changes from Agent 2 (lp-summary endpoint) — append after Agent 1's addition
3. Apply pe_benchmarks.py changes from Agent 3 (hiring-signals endpoint) — append after Agent 2's
4. pe-demo.html: Agent 1 adds acquisition story, Agent 2 adds Fund Performance step — these should be additive (no overlap if Agents follow instructions)
5. Restart API, test all 5 new endpoints, verify demo loads

---

## Expected Outcome

After completion:
- Web demo tells **both** acquisition AND exit stories in one UI
- Exit story has 5 steps (adds Fund Performance / LP angle)
- Acquisition story surfaces job posting velocity as a leading indicator
- Exit readiness score incorporates hiring signals
- All powered by seeded demo data (no manual setup beyond `POST /pe/seed-demo`)
