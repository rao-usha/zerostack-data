# SPEC 085 — Atlas Pilot cleanup + furniture-store demo thesis

**Status:** Draft
**Task type:** bug_fix (+ small feature)
**Date:** 2026-05-28
**Plan:** PLAN_074_atlas_vscode_shell_rev_03
**Test file:** tests/test_spec_085_atlas_pilot_cleanup.py
**Builds on:** SPEC_081 · SPEC_082 · SPEC_083 · SPEC_084

## Goal

Fix the six issues that make the guided tour feel broken, and ship a
one-click furniture-store demo thesis so there's a clean end-to-end
happy path.

## Acceptance Criteria

- [ ] **B1** System prompt example uses the real layer id
      `demo_acs_median_income`; a hard rule forbids guessing layer ids
      ("call list_layers if unsure").
- [ ] **B2** Tool-log entries render real quotes, not `&quot;`
      (escapeHtml removed from the textContent path).
- [ ] **B3** `#pilot-tools`, `#pilot-cites`, `#pilot-meta` are hidden
      inside `#right-col`; the bottom-inspector Tool Log pane streams
      live (per event), not just at `done`.
- [ ] **B4** Final narration contains no options bullet list / "What
      would you like to explore next?". Backend strips a trailing
      options block defensively.
- [ ] **B5** `find_unlinked_claims(narration, citations, question=...)`
      skips numeric tokens present in the question; `$150K` echoed
      from the user is not flagged.
- [ ] **B6** History view reads + writes the same `atlas_pilot_history`
      key the Pilot uses; finishing a question populates the view.
- [ ] **F** A "Load furniture-store demo" button fills the thesis with
      a complete preset and saves it; the subsequent Pilot question
      runs without the `unknown layer_id` error.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_prompt_has_real_layer_id | SYSTEM_PROMPT contains demo_acs_median_income, not the fake id |
| T2 | test_prompt_forbids_guessing_layers | prompt has a "list_layers / don't guess" rule |
| T3 | test_unlinked_skips_question_numbers | `$150K` in question not flagged |
| T4 | test_unlinked_still_flags_uncited | a number NOT in question/citations is still flagged |
| T5 | test_strip_options_block_from_narration | trailing "What would you like…\n- a\n- b" removed |
| T6 | test_strip_keeps_normal_narration | narration without an options block is untouched |
| T7 | test_history_key_unified (frontend) | atlas.html uses atlas_pilot_history in both reader + writer; no `nexdata.pilot.hist` |
| T8 | test_tool_log_not_double_escaped (frontend) | no `escapeHtml(JSON.stringify(ev.args)` in the appendToolLog calls |
| T9 | test_chat_hides_tool_log (frontend) | CSS hides #pilot-tools/#pilot-cites/#pilot-meta within #right-col |
| T10 | test_demo_button_present (frontend) | "Load furniture-store demo" button + loadFurnitureDemo() present |

## Design Notes

### B5 — find_unlinked_claims signature change

```python
def find_unlinked_claims(narration, citations, question=""):
    ...
    q_numbers = set(_re.findall(r"\$?[\d,.]+[KMB%]?", question or ""))
    ...
    if m in cited_text or m in q_numbers:
        continue
```

Both `run_pilot` and `run_pilot_streaming` pass `question=question`.

### B4 — strip trailing options block

```python
_OPTIONS_TAIL = _re.compile(
    r"\n+\s*(what would you like[^\n]*\??|where would you like[^\n]*\??|"
    r"what.s next\??|next steps?\??)\s*(\n\s*[-*•].*)+\s*$",
    _re.IGNORECASE | _re.DOTALL,
)
def _strip_options_block(text):
    return _OPTIONS_TAIL.sub("", text or "").rstrip()
```

Applied to `final_narration` before returning / yielding the narration.

### B3 — live tool log in bottom inspector

In `handlePilotEvent`, every place that calls `appendToolLog(toolsEl, …)`
also appends the same line to `#toollog-pane` (the bottom inspector).
`#pilot-tools` is hidden but still receives content so existing logic +
the at-done mirror keep working.

### F — furniture-store demo preset

```js
const FURNITURE_DEMO = {
  industry_label: 'Furniture stores',
  industry_naics: '442110',
  target_hhi_min: 75000, target_hhi_max: null,
  target_age_band: '25-44',
  target_pop_density_min: 1000,
  region: 'Austin, TX metro',
  exclude_layers: 'disaster_nri',
  notes: 'Mid-to-high-end home furnishings. Trade area ~5 mi / 12-min '
       + 'drive. Want affluent, growing, household-formation-age areas; '
       + 'avoid pure-rural and disaster-risk-dominated tracts.',
};
```

Button in the thesis header, next to Reset: "⚡ Demo". On click: write
to `atlas_thesis_v1`, refill form, refresh previews, save, re-pick
default layer, toast.

## Rubric Checklist (bug_fix)

- [ ] Root cause identified for each of the 6 bugs (see rev_03).
- [ ] Regression test added per fix.
- [ ] No new SQL; no PII.
- [ ] Backend changes keep `thesis_context` + streaming contracts.
- [ ] Frontend changes don't break existing Pilot handlers.

## Files to Create/Modify

| File | Action |
|------|--------|
| `app/services/atlas/pilot.py` | B1, B4, B5 |
| `frontend/atlas.html` | B2, B3, B6, F |
| `docs/specs/SPEC_085_atlas_pilot_cleanup.md` | this file |
| `tests/test_spec_085_atlas_pilot_cleanup.py` | T1-T10 |

## Verification

1. Click "⚡ Demo" in the thesis header → form fills with the furniture
   thesis; map switches to median income.
2. Ask Pilot "walk me through where to open this furniture store".
3. Tool log (bottom inspector) streams live with real quotes; first
   tool call uses a real layer id (no `unknown layer_id`).
4. Chat column shows only narration + clickable option buttons — no
   bullet-text options, no raw tool log.
5. No ⚠ warning for `$150K` echoed from a follow-up question.
6. Open History view (🕘) → the question you just asked is listed.

## Feedback History

- 2026-05-28 — Driving feedback: *"the atlas pilot is very funky…
  weird sliders… can't continue the chat it's not moving to history…
  fix by building a full thesis against the furniture store."* →
  `memory/feedback/corrections.md` 2026-05-28 entry.
