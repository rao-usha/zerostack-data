# SPEC 067 — Atlas Recent Activity feed

**Status:** Draft
**Task type:** report (frontend + small server endpoint)
**Date:** 2026-05-24
**Plan:** PLAN_072 Wave 3
**Test file:** `tests/test_spec_067_atlas_recent.py`
**Builds on:** SPEC_066c (Wave 2 ships first), `disaster_fema_declarations`
table, place-panel infrastructure from SPEC_066b.

## Goal

A "what's new" surface on the Atlas. The single biggest driver of
return-visit behavior is *a reason to check again today*. Recent
Activity is that reason — a header-mounted floating panel listing the
most-recent events across the data we already have. FEMA-only MVP;
SEC + USAspending lanes wire in as PLAN_067 SPEC_074 / SPEC_071-stretch
land.

## Why this hits Phase-2 metrics

| Metric | How the feed moves it |
|---|---|
| **Return rate** | Daily-fresh feed → reason to revisit |
| **Breadth** | Items pull users across domains they wouldn't have searched |
| **Follow / share** | Feed items are the most shareable unit ("Hurricane Beryl declared in N counties"); each item link is a sharable place-deep-link |
| **Wedge discovery** | `recent_item_clicked` telemetry reveals which event types users open most |

## Honest scoping decision

Pre-flight surfaced that the FEMA table is ~3 months stale (most-recent
= 2026-02-20; today = 2026-05-24). A literal "last 7 days" feed would
be empty. The MVP renames the framing to **"most-recent N events"** —
same UX value, honest about what the data actually says. A future
SPEC_067-followup pulls FEMA refreshes on a schedule and SEC + USA
sources arrive via PLAN_067 backfills.

## Acceptance Criteria

### Server
- [ ] `GET /atlas/recent?limit=50&sources=fema` returns the merged
      most-recent event stream, sorted by date desc.
- [ ] Each item: `{source, date, type, title, place_id, place_name,
      event_id}`.
- [ ] `sources` is a comma-separated whitelist (only `fema` for v1;
      `sec`, `usaspending` etc reject with 400 until their backfills land).
- [ ] Limit capped at 200; default 50.

### Frontend
- [ ] Header has a "📰 Recent" button (next to the Compare / Scatter / Share buttons).
- [ ] Click → floating right-aligned overlay panel (matches the existing
      scatter-overlay pattern) opens with a scrollable list of ~50 items.
- [ ] Each item: small card with **date** · type chip · title · place name.
      Color-coded by event type (Hurricane=blue, Fire=orange, etc).
- [ ] Click an item → opens the place panel for `place_id`, fits map
      to county bounds, closes the recent panel.
- [ ] Telemetry: `recent_panel_opened`, `recent_item_clicked: {source, event_id, place_id, type}`.

### Quality
- [ ] No regression — smoke harness stays green
- [ ] Bundle stays in budget (no new external dep — pure HTML/CSS/JS)
- [ ] Empty-source case (e.g. `?sources=sec`) returns 400 with helpful message until SPEC_074 lands

## Test Cases

| ID | Test | Verifies |
|---|---|---|
| T1 | `test_recent_fema_returns_items` | `/atlas/recent?sources=fema` returns ≥10 items |
| T2 | `test_items_have_required_fields` | each item has source, date, title, place_id (5-digit FIPS), place_name |
| T3 | `test_items_sorted_desc_by_date` | dates are non-increasing |
| T4 | `test_unknown_source_400` | `?sources=imaginary` returns 400 |
| T5 | `test_default_limit_50` | default limit is 50; explicit limit honored; 1000 capped to 200 |
| T6 | `smoke harness: recent panel opens` | scenario added to scripts/smoke_atlas.py |

## Design Notes

### `/atlas/recent` endpoint
```python
@router.get("/recent")
def get_recent_events(
    limit: int = 50,
    sources: str = "fema",
    db: Session = Depends(get_db),
):
    src_set = set(s.strip() for s in sources.split(",") if s.strip())
    unknown = src_set - {"fema"}  # extend as SPEC_074 / SPEC_071-stretch land
    if unknown:
        raise HTTPException(400, f"unknown sources: {sorted(unknown)}")
    return {"items": series_mod.fetch_recent_events(db, list(src_set), limit)}
```

### `fetch_recent_events` (series.py)
```python
def fetch_recent_events(db, sources: List[str], limit: int) -> List[dict]:
    items = []
    if "fema" in sources:
        items.extend(_fetch_recent_fema(db, limit))
    items.sort(key=lambda x: x["date"], reverse=True)
    return items[:max(1, min(limit, 200))]
```

### FEMA query
```sql
SELECT disaster_number AS event_id,
       declaration_date::text AS date,
       incident_type AS type,
       declaration_title AS title,
       fips_state_code || lpad(fips_county_code, 3, '0') AS place_id,
       state || ': ' || county AS place_name
FROM fema_disaster_declarations
WHERE declaration_date IS NOT NULL
  AND fips_state_code IS NOT NULL AND fips_county_code IS NOT NULL
ORDER BY declaration_date DESC
LIMIT :lim
```
Returns one row per (declaration × county), so a multi-county disaster
(e.g. Hurricane Beryl) appears multiple times. Acceptable for v1 — each
row links to a distinct place. If de-duplication becomes a UX issue,
a future iteration adds GROUP BY disaster_number with a county count.

### Frontend HTML
```html
<button class="btn-icon" id="recent-toggle">📰 Recent</button>
<div id="recent-overlay" class="floating-overlay">
  <button class="close" id="recent-close">×</button>
  <h4>Recent FEMA declarations</h4>
  <div id="recent-list" class="spinner">Loading…</div>
</div>
```

### Item-card click behavior
Reuses the existing `openPlace(geo_id)` + boundary-bounds fit logic
from the fast-travel search path. No new map code.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_067_atlas_recent_activity.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_067_atlas_recent_activity` |
| `tests/test_spec_067_atlas_recent.py` | Create | T1-T5 server tests |
| `app/services/atlas/series.py` | Modify | Add `fetch_recent_events` + `_fetch_recent_fema` |
| `app/api/v1/atlas.py` | Modify | Add `/atlas/recent` endpoint |
| `frontend/atlas.html` | Modify | Add 📰 Recent button + floating overlay + JS |
| `scripts/smoke_atlas.py` | Modify | Add scenario for recent-panel-content |
| `docs/ATLAS_TOUR.md` | Modify | Document the new feature + SPEC_066c features |

## Verification (manual)

1. `curl 'http://localhost:8001/api/v1/atlas/recent?sources=fema&limit=10' | jq '.items[0]'`
   → shows 2026-02-20 sewer-line-collapse DC.
2. Open `/atlas.html` → 📰 Recent button visible in header.
3. Click it → floating panel opens with ~50 items.
4. Click any item → place panel populates, map fits to that county,
   floating panel closes.
5. `python scripts/smoke_atlas.py` → all scenarios pass + new
   `recent-panel-content` passes.

## Feedback History

_No corrections yet._
