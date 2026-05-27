# SPEC 084 — Atlas Thesis view as Notion-style collapsible blocks

**Status:** Draft
**Task type:** service (frontend-only)
**Date:** 2026-05-27
**Plan:** PLAN_074_atlas_vscode_shell_rev_02
**Test file:** tests/test_spec_084_atlas_thesis_blocks.py
**Builds on:** SPEC_082 · SPEC_083

## Goal

Replace the flat label-then-input thesis form with five Notion-style
collapsible blocks. Each block has a chevron toggle, a summary line
showing populated state, and persists its open/closed state. The Save
action is pinned to the bottom of the pane so it's always visible.

## Acceptance Criteria

- [ ] Thesis view renders 5 `<details class="block">` sections:
      Industry · Demographics · Geography · Notes · Layers/Excludes.
- [ ] Each block has a `<summary>` with a custom chevron (▸ closed,
      ▾ open) and a label + compact preview of populated values.
- [ ] Industry block is `open` by default; the others are closed.
- [ ] Closed summary shows preview ("Industry · Furniture stores",
      "Demographics · HHI ≥ $75K · 25-44"). Empty blocks show
      "(empty)" in a dimmed style.
- [ ] Open-state per block persists to localStorage key
      `atlas_thesis_open_v1` (a `{blockId: bool}` map).
- [ ] Sticky Save bar at the bottom of the view-pane, with the
      same Save button + flash. Always reachable without scroll.
- [ ] "Reset" affordance in the view header — confirms, then clears
      `atlas_thesis_v1`, repaints empty form.
- [ ] Default chevron rotates smoothly on open/close (transform
      transition).
- [ ] No regression to `thesis_context` plumbing: the same field
      keys still flow to the backend.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_thesis_view_has_5_details_blocks | HTML contains 5 `<details class="block">` elements |
| T2 | test_industry_block_open_by_default | Industry block has the `open` attribute |
| T3 | test_demographics_block_closed_by_default | Demographics block has no `open` attribute |
| T4 | test_save_bar_present | Sticky save bar markup present (`#thesis-save-bar`) |
| T5 | test_reset_button_present | Header reset button (`#thesis-reset`) wired |
| T6 | test_open_state_persistence_key | JS references `atlas_thesis_open_v1` |
| T7 | test_no_old_flat_form | Old `<h3>Target demographics</h3>` etc. no longer present |

## Design Notes

### Markup

```html
<div id="view-thesis" class="view-pane">
  <div class="thesis-header">
    <div class="panel-title">Investment thesis</div>
    <button class="link-btn" id="thesis-reset">Reset</button>
  </div>

  <details class="block" data-block="industry" open>
    <summary>
      <svg class="chevron" .../>
      <span class="block-label">Industry</span>
      <span class="block-preview" data-preview="industry"></span>
    </summary>
    <div class="block-body">
      <label>Industry / What you're banking</label>
      <input id="thesis-industry-label">
      <label>NAICS code (optional)</label>
      <input id="thesis-industry-naics">
    </div>
  </details>

  <details class="block" data-block="demographics">
    <summary>
      <svg class="chevron" .../>
      <span class="block-label">Demographics</span>
      <span class="block-preview" data-preview="demographics"></span>
    </summary>
    <div class="block-body">
      <div class="row2">
        <div><label>HHI min ($)</label><input id="thesis-hhi-min"></div>
        <div><label>HHI max ($)</label><input id="thesis-hhi-max"></div>
      </div>
      <label>Target age band</label>
      <select id="thesis-age-band">…</select>
      <label>Min population density (people / km²)</label>
      <input id="thesis-density">
    </div>
  </details>

  <details class="block" data-block="geography">
    <summary>…</summary>
    <div class="block-body">
      <label>Region focus (state, metro, county FIPS)</label>
      <input id="thesis-region">
    </div>
  </details>

  <details class="block" data-block="notes">
    <summary>…</summary>
    <div class="block-body">
      <textarea id="thesis-notes"></textarea>
    </div>
  </details>

  <details class="block" data-block="layers">
    <summary>…</summary>
    <div class="block-body">
      <label>Hide these layers (comma-separated layer ids)</label>
      <input id="thesis-exclude-layers" placeholder="e.g. disaster_nri">
    </div>
  </details>
</div>

<!-- pinned save bar inside view-thesis -->
<div id="thesis-save-bar">
  <button class="save-btn" id="thesis-save">Save thesis</button>
  <span class="save-flash" id="thesis-saved">✓ saved</span>
</div>
```

### Preview rules per block

| Block | Preview when populated | Preview when empty |
|---|---|---|
| Industry | `Furniture stores` (or `NAICS 442110` if no label) | `(empty)` |
| Demographics | `HHI $75K+ · 25-44` (combine populated fields with ·) | `(empty)` |
| Geography | `Texas` (region text) | `(empty)` |
| Notes | First 60 chars of notes + `…` | `(empty)` |
| Layers/Excludes | `hides: disaster_nri, broadband_…` | `(empty)` |

Preview spans use `--dim` color and small font (10.5px).

### CSS sketch

```css
#view-thesis { display:flex; flex-direction:column; padding:0; }
.thesis-header { display:flex; align-items:center; gap:8px;
  padding:12px 16px 8px; }
.thesis-header .link-btn { background:transparent; border:none;
  color:var(--dim); cursor:pointer; font-size:11px; }
.thesis-header .link-btn:hover { color:var(--warn); }

#view-thesis details.block { border-top:1px solid var(--border);
  background:transparent; }
#view-thesis details.block:last-of-type { border-bottom:1px solid var(--border); }
#view-thesis details.block summary {
  display:flex; align-items:center; gap:8px;
  padding:10px 16px; cursor:pointer; list-style:none;
  user-select:none; transition:background .14s ease;
}
#view-thesis details.block summary::-webkit-details-marker { display:none; }
#view-thesis details.block summary:hover { background:rgba(255,255,255,.03); }
#view-thesis details.block .chevron {
  width:12px; height:12px; color:var(--dim); flex-shrink:0;
  transition:transform .18s ease;
}
#view-thesis details.block[open] .chevron { transform:rotate(90deg); }
#view-thesis details.block .block-label { font-size:12.5px;
  font-weight:600; color:var(--text); letter-spacing:.01em; }
#view-thesis details.block .block-preview { font-size:11px;
  color:var(--dim); margin-left:auto;
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
  max-width:55%; }
#view-thesis details.block .block-preview.has-value { color:var(--muted); }
#view-thesis details.block .block-body { padding:6px 16px 14px; }

/* Sticky save bar */
#thesis-save-bar { position:sticky; bottom:0; margin-top:auto;
  display:flex; align-items:center; gap:10px;
  padding:10px 16px; border-top:1px solid var(--border);
  background:rgba(22,29,49,.85); backdrop-filter:blur(8px);
  -webkit-backdrop-filter:blur(8px); }
```

### JS

```js
const THESIS_OPEN_KEY = 'atlas_thesis_open_v1';

function loadOpenState() {
  try { return JSON.parse(localStorage.getItem(THESIS_OPEN_KEY) || '{}'); }
  catch(_) { return {}; }
}
function saveOpenState(state) {
  localStorage.setItem(THESIS_OPEN_KEY, JSON.stringify(state));
}
function initThesisBlocks() {
  const open = loadOpenState();
  document.querySelectorAll('#view-thesis details.block').forEach(d => {
    const id = d.dataset.block;
    if (id in open) d.open = !!open[id];
    d.addEventListener('toggle', () => {
      const s = loadOpenState();
      s[id] = d.open;
      saveOpenState(s);
    });
  });
  refreshThesisPreviews();
  document.querySelectorAll('#view-thesis input, #view-thesis textarea,'
    + ' #view-thesis select').forEach(el =>
    el.addEventListener('input', refreshThesisPreviews));
}
function refreshThesisPreviews() {
  const t = thesisFromForm();
  const set = (block, text, hasValue) => {
    const el = document.querySelector(`#view-thesis [data-preview="${block}"]`);
    if (!el) return;
    el.textContent = text || '(empty)';
    el.classList.toggle('has-value', !!hasValue);
  };
  set('industry',
    t.industry_label || (t.industry_naics ? `NAICS ${t.industry_naics}` : ''),
    !!(t.industry_label || t.industry_naics));
  const demoParts = [];
  if (t.target_hhi_min != null && t.target_hhi_max != null)
    demoParts.push(`HHI $${t.target_hhi_min}K–$${t.target_hhi_max}K`);
  else if (t.target_hhi_min != null) demoParts.push(`HHI ≥ $${t.target_hhi_min}`);
  else if (t.target_hhi_max != null) demoParts.push(`HHI ≤ $${t.target_hhi_max}`);
  if (t.target_age_band) demoParts.push(t.target_age_band);
  if (t.target_pop_density_min != null)
    demoParts.push(`density ≥ ${t.target_pop_density_min}/km²`);
  set('demographics', demoParts.join(' · '), demoParts.length > 0);
  set('geography', t.region || '', !!t.region);
  set('notes',
    t.notes ? (t.notes.slice(0, 60) + (t.notes.length > 60 ? '…' : '')) : '',
    !!t.notes);
  set('layers',
    t.exclude_layers ? `hides: ${t.exclude_layers}` : '',
    !!t.exclude_layers);
}
```

## Rubric Checklist

- [ ] Frontend-only change; no backend modifications.
- [ ] Accessibility: `<details>` is native and keyboard-navigable.
- [ ] No new dependencies.
- [ ] thesis_context plumbing untouched (same field keys).
- [ ] Tests cover markup + persistence-key + regression of old flat form.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `frontend/atlas.html` | Modify | Thesis view markup, CSS for `details.block`, summary preview JS, persistence, sticky save bar, Reset |
| `app/services/atlas/pilot.py` | Optional add | Accept new `region` + `exclude_layers` fields in thesis sanitiser |
| `docs/plans/PLAN_074_atlas_vscode_shell_rev_02.md` | Created | |
| `docs/specs/SPEC_084_atlas_thesis_blocks.md` | Create | this file |
| `tests/test_spec_084_atlas_thesis_blocks.py` | Create | T1-T7 |

## Verification

1. Reload `/atlas.html`. Thesis view shows Industry block expanded
   with input visible; 4 other blocks collapsed with chevrons.
2. Expand Demographics, fill HHI min $75K. Click chevron to collapse.
   Summary line reads "Demographics · HHI ≥ $75 · …".
3. Click Save. Reload. Same blocks are still expanded/collapsed.
4. Click Reset, confirm. All blocks collapse to Industry only, fields empty.
5. Ask Pilot a question. Backend still receives the same thesis_context
   structure (no regression).

## Feedback History

- 2026-05-27 — User feedback driving this spec:
  *"The thesis piece looks like shit with the vertical bars. I think
  it needs to be collapseable components like we were talking with
  notion."* → `memory/feedback/corrections.md` 2026-05-27 entry.
