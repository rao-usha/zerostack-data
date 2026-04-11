# PLAN_040 — Les Schwab Report Full Rebuild + Repeatable IC Report Pattern

## Problem

The current `les_schwab_av.py` template (PLAN_039/040) produces a 6-section report with live KPIs
but is missing the richest features of the original `les_schwab_av_deep_dive.html`:
- Business profile with full revenue mix table
- AV/EV adoption scenario fan (3-scenario projection table)
- Revenue vulnerability projections per service line through 2040
- Strategic response options matrix (6 options, prioritized)
- Competitive moat assessment (NPS comparison, moat durability table)
- Investment scenario analysis (bull/base/bear EBITDA + implied EV table)
- Watchpoints & recommended actions (7-row tracking table + 4 concrete actions)

Additionally: no repeatable pattern exists for building *other* IC reports of this type.

---

## What We're Building

### Part 1 — Full Rebuild of `les_schwab_av.py`

Expand from 6 shallow sections → 7 fully-populated IC-grade sections. Every section matches
the original report content, with live Nexdata data overlaid where available.

**Section mapping:**

| # | Section | Live Data | Static Analysis |
|---|---------|-----------|-----------------|
| 1 | Business Profile & Financial Snapshot | EV stations in footprint (AFDC) | Revenue mix table, acquisition context |
| 2 | AV & EV Adoption Trajectory | Vehicle sales SAAR (FRED), consumer sentiment | 3-scenario projection table (2026–2040) |
| 3 | Revenue Vulnerability by Service Line | Gas price (FRED) | Service line projections (base/bear through 2035) |
| 4 | Strategic Response Options | Auto tech wage, employment (BLS) | Options matrix: 7 options, revenue/margin/priority |
| 5 | Competitive Moat Assessment | — | NPS comparison, moat durability table |
| 6 | Investment Scenario Analysis | — | Bull/base/bear EBITDA + EV multiples |
| 7 | Key Watchpoints & Recommended Actions | — | 7-row watchpoint table + 4 action items |

**KPI strip (5 cards):**
1. US Vehicle Sales SAAR — live (FRED TOTALSA)
2. Brake Revenue at Risk — $315M hardcoded (15% of $2.1B rev)
3. EV Stations — Les Schwab States — live (AFDC, summed over Schwab's 10 states)
4. Consumer Sentiment — live (FRED UMCSENT)
5. Auto Tech Median Wage — live (BLS OEUN...04)

**Charts per section:**

| Section | Chart A | Chart B |
|---------|---------|---------|
| 1 | Revenue mix donut (6 service lines) | EV stations by Schwab state (bar, live) |
| 2 | EV/AV fleet penetration 3 scenarios (line) | Vehicle ownership reduction fan (line) |
| 3 | Brake revenue erosion curve (line) | Tire wear rate: ICE vs EV (bar) |
| 4 | Strategic option value vs complexity (bubble/bar) | ADAS calibration revenue opportunity (bar) |
| 5 | Brand NPS comparison (horizontal bar) | Visit frequency: ICE vs EV vs AV (line) |
| 6 | Revenue trajectory by scenario (line) | EBITDA by scenario (line) |
| 7 | (callouts + table only) | — |

All hardcoded data lives as Python constants at the top of the template file so a future
analyst can update estimates in one place.

---

### Part 2 — Repeatable IC Report Pattern

The architecture is: **Python template class → report archive system → trigger script**.

#### Template pattern (one file per company/thesis)
```
app/reports/templates/
├── les_schwab_av.py          ← rebuilt (this plan)
├── discount_tire_av.py       ← future: same pattern, different constants
├── jiffy_lube_ev.py          ← future: same pattern, different thesis
└── _ic_report_base.py        ← NEW: base class with shared helpers
```

`_ic_report_base.py` extracts the boilerplate that every IC report shares:
- `gather_macro(db)` — FRED + BLS + AFDC fetch helpers
- `render_scenario_table(rows)` — standardized scenario table renderer
- `render_watchpoints(rows)` — watchpoint table renderer
- `render_options_matrix(rows)` — strategic options matrix renderer

#### Trigger script
`scripts/generate_report.sh <template_name>` — one-liner to regenerate any registered report.

```bash
#!/bin/bash
# Usage: ./scripts/generate_report.sh les_schwab_av
TEMPLATE=${1:-les_schwab_av}
DATE=$(date +%Y-%m-%d)
curl -s -X POST "http://localhost:8001/api/v1/reports/generate" \
  -H "Content-Type: application/json" \
  -d "{\"template\":\"${TEMPLATE}\",\"format\":\"html\",\"params\":{},\"title\":\"${TEMPLATE} — ${DATE}\"}" \
  | python -m json.tool
```

This is sufficient as the repeatable mechanism — no separate Claude skill needed.
The template IS the source of truth; the script IS the trigger.

A Claude `/generate-report` skill could wrap this later if desired, but the shell
script is simpler, more portable, and does not require Claude to be running.

---

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/reports/templates/les_schwab_av.py` | **Rewrite** | Full 7-section rebuild with all original content |
| `app/reports/templates/_ic_report_base.py` | **Create** | Shared helpers for IC report templates |
| `scripts/generate_report.sh` | **Create** | One-liner trigger script |

No changes to `builder.py`, `main.py`, or `reports.py` — les_schwab_av is already registered
and the weekly scheduler is already wired.

---

## Hardcoded Constants (non-DB, qualitative estimates)

All of these live at the top of `les_schwab_av.py` as module-level constants,
easily auditable and updatable by an analyst:

```python
# Business financials (2024E)
_TOTAL_REVENUE = 2_100  # $M
_REVENUE_MIX = [
    ("Tires (product + install)", 1260, 60, "EV weight → faster wear; AV fleets → fewer owned vehicles", "Medium"),
    ("Brakes (pads, rotors, labor)", 315, 15, "Regenerative braking eliminates 65–70% of mechanical brake wear", "High"),
    ...
]

# AV/EV adoption scenarios (2026–2040)
_ADOPTION_SCENARIOS = [
    (2026, 15, 5, 1, "Negligible", "Minimal"),
    (2028, 22, 9, 3, "-2% to -4%", "Brakes -6%"),
    ...
]

# Strategic options matrix
_STRATEGIC_OPTIONS = [
    ("ADAS / AV Sensor Calibration", "$80–150M incremental", "55–65%", "2–3 years", "Low–Medium", "Priority 1"),
    ...
]

# Competitive moat
_MOAT_DIMENSIONS = [...]

# Investment scenarios (Bull/Base/Bear)
_SCENARIOS = [...]

# Watchpoints
_WATCHPOINTS = [...]
```

---

## Implementation Order

1. Create `_ic_report_base.py` — shared macro fetch helpers + shared table renderers
2. Rewrite `les_schwab_av.py` — 7 sections, all constants, all charts, inherits base
3. Create `scripts/generate_report.sh`
4. Restart API, run script, verify all 7 sections render with live KPI values

---

## Verification

1. All 5 KPI cards show live values (no "—" or "Loading…")
2. Section 1 revenue mix table shows 6 service lines summing to $2,100M
3. Section 2 scenario table has rows for 2026–2040
4. Section 6 investment scenarios table shows bull/base/bear EBITDA + EV multiples
5. Section 7 watchpoints table has 6 rows + 4 action callouts
6. Footer notes all cite data sources with `as of YYYY-MM-DD` dates
7. `scripts/generate_report.sh les_schwab_av` produces a new report_N.html in <5 seconds
8. No `fetch()` calls or `Loading…` in generated HTML
