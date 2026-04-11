# PLAN_049 — Sources Page Redesign + Data Governance

**Status:** Draft — awaiting approval
**Date:** 2026-03-30
**Scope:** `frontend/index.html` (Sources tab only) + `app/api/v1/sources.py`
**Depends on:** Existing DQ infrastructure (anomaly detection, quality scoring, freshness endpoints)
**Feeds into:** PLAN_047 (econ DQ — its outputs surface here)

---

## Problem

The current Sources tab is an operator console that requires 3–4 clicks to answer the most basic question: "is this data ready to use?" Status dots are buried in detail views, quality scores are in a separate DQ tab, freshness info requires timestamp parsing, and the category accordion adds a click layer that serves no one.

There are two distinct user types who need this page to work:
- **Operator** (running collections, monitoring ingestion): needs at-a-glance health + quick actions
- **Analyst** (exploring schemas, previewing data, exporting): needs the current detailed drill-down

Right now the page tries to serve both and serves neither well.

---

## What We're Building

Three concrete changes:

### 1. Health View (new default mode)

Replaces the category accordion with **domain swim lanes** — persistent cards, one per source, with status visible inline. No clicks needed to see health.

### 2. Governance Panel (new section below swim lanes)

A data governance command center: SLA compliance timeline, open anomaly alerts, quality scorecards, and recommendation cards. Pulls from existing DQ endpoints — no new backend logic, just surface what's already computed.

### 3. Explore Mode (existing behavior, preserved behind toggle)

Current detail drill-down (schema, preview, API docs) stays intact — just moved behind a "Explore" toggle so it doesn't crowd the default view. Two targeted fixes: table-level export (currently only source-level), and table search across all schema names.

---

## Header Bar (replaces hero stats)

Current: 4 stats (Total Records / Tables with Data / Sources with Data / Coverage Score)

New: **Status Banner** — a single contextual strip that changes color based on system state:

```
● 28 sources active  ·  3 stale  ·  2 never run  ·  1 anomaly open  ·  Last activity 2h ago  [See Issues ↗]
```

- **All fresh** → green left border
- **Any stale** → amber left border
- **Any failed or anomaly** → red left border
- "See Issues" opens a focused modal showing just the problematic sources with "Run Now" buttons

The 4 hero stats move into the Governance panel as a secondary metrics row — still visible, just not the first thing you see.

---

## Health View — Domain Swim Lanes

Four swim lanes replacing the category accordion. Always visible, no click to expand.

```
┌─ MACRO ECONOMIC ─────────────────────────────────────────────────────────────┐
│  [FRED]    ● 47,231 rows ↑  Last: 2h ago   Score: 94/100  [Run] [Schedule]  │
│  [BLS]     ● 12,890 rows →  Last: 14h ago  Score: 88/100  [Run] [Schedule]  │
│  [BEA]     ○ 8,400 rows →   Last: 8mo ago  Score: 72/100  [Run] (annual)    │
│  [Census]  ● 51 rows  →     Last: 3d ago   Score: 91/100  [Run] [Schedule]  │
│  [FRED Housing] ● 2,100 rows Last: 2h ago  Score: 90/100  [Run] [Schedule]  │
└──────────────────────────────────────────────────────────────────────────────┘

┌─ PE INTELLIGENCE ─────────────────────────────────────────────────────────────┐
│  [SEC EDGAR]  ● 142K rows ↑  Last: 6h ago   Score: 96/100  [Run] [Schedule] │
│  [Form D]     ● 181 rows →   Last: 2d ago   Score: 81/100  [Run] [Schedule] │
│  [PE Firms]   ● 2,840 rows ↑ Last: 1d ago   Score: 85/100  [Run] [Schedule] │
└──────────────────────────────────────────────────────────────────────────────┘

┌─ PEOPLE & ORGS ────────────────────────────────────────────────────────────────┐
│  [People]  ● 3,200 rows ↑  Last: 4h ago   Score: 78/100  [Run] [Schedule]   │
│  [Org Charts] ● 142 snaps  Last: 1d ago   Score: 82/100  [Run] [Schedule]   │
└───────────────────────────────────────────────────────────────────────────────┘

┌─ SITE INTELLIGENCE ────────────────────────────────────────────────────────────┐
│  [AFDC]    ● 68K rows ↑  Last: 3d ago  Score: 95/100  [Run] [Schedule]      │
│  [OSHA]    ● 4,200 rows  Last: 7d ago  Score: 74/100  [Run] [Schedule]      │
│  ...                                                                          │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Per-card fields:**
- Status dot: ● green (fresh) · ● amber (approaching stale) · ● red (stale/failed) · ○ grey (never run)
- Row count with trend arrow (↑ growing / → stable / ↓ shrinking since last run)
- Last run time (relative: "2h ago", "8mo ago")
- Quality score (from existing DQ system — grey "—" if DQ hasn't run yet)
- Run Now button → triggers existing ingest endpoint, shows inline spinner
- Schedule button → opens existing schedule modal

**Hover tooltip** on the source name shows: table names, last job ID, next scheduled run, API key status (set/missing).

**Clicking the source name** → opens detail panel as a **slide-in right drawer** (not a full page navigation). This keeps the swim lane visible while exploring detail.

---

## Mode Toggle

Top-right of the Sources tab:

```
[● Health]  [⊞ Explore]
```

- **Health** (default): swim lanes + governance panel
- **Explore**: current accordion/detail view — schema, preview, API docs, per-endpoint curl examples. Unchanged functionality, just not the default.

Search bar persists across both modes. In Explore mode, search also matches table names (currently only matches source name/key/domain).

---

## Governance Panel

Appears below the swim lanes in Health mode. Three sections:

### A. SLA Compliance Timeline

Horizontal Gantt-style chart. One row per source. X-axis = time (last 30 days). Colored bars show when data was fresh vs. stale vs. missing.

```
FRED        [████████████████████████████░░] 94% compliant
BLS         [██████████████████████████░░░░] 88% compliant
BEA         [████░░░░░░░░░░░░░░░░░░░░░░░░░░] 15% (annual — expected)
Census      [████████████████████████░░░░░░] 82% compliant
SEC EDGAR   [███████████████████████████░░░] 92% compliant
```

- Green = within SLA · Amber = approaching threshold · Red = past SLA · Dark = no data expected (annual release)
- Shows expected cadence next to each source (FRED: daily · BLS: weekly · BEA: annual · etc.)
- Sourced from `/api/v1/datasets/freshness` (already exists) + new `sla_hours` field

### B. Quality Scorecards

2-column grid of source quality cards. Each shows the 4-dimension breakdown as a mini bar:

```
┌─ FRED ────────────────┐    ┌─ BLS ─────────────────┐
│ Overall: 94/100       │    │ Overall: 88/100        │
│ Completeness ████ 97% │    │ Completeness ███░ 85%  │
│ Freshness    ████ 96% │    │ Freshness    ███░ 87%  │
│ Validity     ████ 93% │    │ Validity     ████ 92%  │
│ Consistency  ███░ 88% │    │ Consistency  ███░ 84%  │
│ [View Details →]      │    │ [View Details →]       │
└───────────────────────┘    └────────────────────────┘
```

- Sourced from `/api/v1/data-quality/trends` (already exists)
- "View Details" → opens the DQ detail in the existing DQ tab
- Grey card with "DQ not yet run" for sources without quality scores

### C. Open Anomalies + Recommendations

Two compact lists side by side:

**Open Anomalies** (from `/api/v1/data-quality/anomalies`):
```
● ROW_COUNT_SWING  bls_jolts     +340% vs baseline  [Acknowledge]
● NULL_RATE_SPIKE  bea_regional  geo_fips null 12%  [Investigate]
● DATE_GAP         fred_housing  Missing 2025-11     [View]
```

**Top Recommendations** (from `/api/v1/dq-review/recommendations`):
```
↑ HIGH   BLS CES hasn't run in 8 days — past 7-day SLA   [Run Now]
↑ MEDIUM BEA regional annual release available (Sept)    [Ingest]
→ LOW    OSHA 74/100 quality — 12% null on facility_id  [Review]
```

---

## New Backend Endpoint: `/api/v1/sources/health-summary`

One endpoint that returns everything the Health View needs — eliminates 4+ separate API calls the current view makes on load.

**File:** `app/api/v1/sources.py` — add route to existing router.

**Response shape:**
```json
{
  "banner": {
    "active_count": 28,
    "stale_count": 3,
    "never_run_count": 2,
    "open_anomaly_count": 1,
    "last_activity_at": "2026-03-30T08:15:00",
    "overall_status": "warning"
  },
  "domains": [
    {
      "key": "macro_economic",
      "label": "Macro Economic",
      "sources": [
        {
          "key": "fred",
          "display_name": "FRED",
          "status": "idle",
          "is_stale": false,
          "sla_hours": 24,
          "sla_status": "fresh",
          "total_rows": 47231,
          "row_trend": "stable",
          "last_run_at": "2026-03-30T06:00:00",
          "age_hours": 2.5,
          "quality_score": 94,
          "quality_breakdown": {
            "completeness": 97, "freshness": 96,
            "validity": 93, "consistency": 88
          },
          "next_run_at": "2026-03-31T06:00:00",
          "open_anomalies": 0,
          "tables": ["fred_interest_rates", "fred_economic_indicators", ...]
        }
      ]
    }
  ],
  "governance": {
    "sla_timeline": [
      {
        "source": "fred",
        "compliance_pct": 94,
        "events": [{"date": "2026-03-01", "status": "fresh"}, ...]
      }
    ],
    "open_anomalies": [
      {
        "id": 42, "alert_type": "ROW_COUNT_SWING",
        "table_name": "bls_jolts", "severity": "WARNING",
        "message": "+340% row count vs 30-day baseline",
        "detected_at": "2026-03-29T14:00:00"
      }
    ],
    "top_recommendations": [
      {
        "id": 7, "priority": "HIGH",
        "source": "bls", "category": "ORCHESTRATION",
        "message": "BLS CES hasn't run in 8 days — past 7-day SLA",
        "fix_action": "POST /bls/ingest/ces"
      }
    ]
  }
}
```

**Implementation:** Parallel async calls to existing service methods — `SourceRegistry.get_all()`, `FreshnessService.get_all()`, `QualityTrendingService.get_latest_scores()`, `AnomalyDetectionService.get_open_alerts()`, `DQRecommendationEngine.get_top_recommendations(limit=5)` — assembled into one response. No new DB queries.

---

## Targeted Explore Mode Fixes

Two specific fixes to the existing detail view (while preserving everything else):

**1. Table-level export**
Currently: "Export CSV" button exports the entire source (all tables zipped).
Fix: Add an "Export" button on each individual table row in the DB Tables section.
Implementation: Pass `table_name` param to the existing export endpoint.

**2. Schema search**
Currently: Filter input only matches source name, key, and domain.
Fix: Also search against table names in each source's `tables` array.
Implementation: Extend the existing `currentFilter` JS check to include `source.tables.join(' ')`.

---

## Files

| File | Action | Scope |
|------|--------|-------|
| `frontend/index.html` | MODIFY | Sources tab: status banner, swim lanes, mode toggle, governance panel |
| `app/api/v1/sources.py` | MODIFY | Add `/sources/health-summary` endpoint |

No new models. No schema changes. All governance data comes from existing DQ services.

---

## Implementation Phases

| Phase | What | Notes |
|-------|------|-------|
| 1 | `/sources/health-summary` backend endpoint | Parallel async assembly from existing services |
| 2 | Status banner + mode toggle shell | Replace hero stats; toggle wires Health/Explore |
| 3 | Domain swim lane cards | Replace accordion; wire to health-summary response |
| 4 | Governance panel — SLA timeline | Gantt chart from sla_timeline data |
| 5 | Governance panel — quality scorecards | 4-dimension mini-bars per source |
| 6 | Governance panel — anomalies + recommendations | Two compact lists |
| 7 | Slide-in drawer for source detail | Replace full-page drill-down |
| 8 | Explore mode fixes (table export + schema search) | Small targeted changes |
| 9 | Restart + verify all panels render; check against live DQ data | |

---

## What Doesn't Change

- The Explore mode detail view (schema, preview, API docs, curl examples) — untouched
- The DQ tab — stays as the deep-dive DQ interface; Governance panel is a summary surface
- The Jobs tab, Analytics tab, Reports tab — unaffected
- All existing API endpoints — health-summary is additive

---

## Design Notes

- Same dark slate theme as the rest of index.html
- Swim lane headers: uppercase label, subtle left border accent by domain color
- Source cards: `background: var(--bg-input)`, `border: 1px solid var(--border)`, 10px border-radius
- Status dots: CSS variables already defined in index.html (`--success`, `--warning`, `--danger`)
- Row trend arrows: ↑ green / → muted / ↓ red
- Quality score color: ≥80 green, 60–79 amber, <60 red — matches existing coverage score logic
- Governance panel: separated by a subtle `border-top: 1px solid var(--border)` section divider
