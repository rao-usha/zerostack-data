# PLAN 074 rev_02 — Thesis form → Notion-style collapsible blocks

**Date:** 2026-05-27
**Parent:** PLAN_074_atlas_vscode_shell.md
**Previous revision:** PLAN_074 rev_01 (SPEC_083 — shell polish + default layer)
**Active spec:** SPEC_084_atlas_thesis_blocks (created with this revision)

## Revision 02

The Thesis view in the left panel is a flat top-to-bottom stack of
labelled inputs ("vertical bars"). User reaction: *"looks like shit
with the vertical bars. I think it needs to be collapseable components
like we were talking with notion."*

When SPEC_082 originally surveyed layout patterns (the AskUserQuestion
on workspace shell), the Notion-style stacked-blocks variant was one
of the four shell options. The user picked VS Code faithful for the
*shell* but the *content* of the left panel still needs the Notion
treatment — collapsible sections with chevrons, a small affordance to
expand/collapse, and persistence of open-state.

## What Was Wrong

- The thesis form renders every field at full width, label-then-input,
  stacked vertically — five sections, ~12 controls all visible at once.
- No hierarchy: "Industry" (the most-used field) gets the same visual
  weight as "Min population density" (rarely set).
- The h3 dividers (Target demographics / Notes) act as separators but
  read as decorative — they don't fold their section away.
- "Save" sits at the bottom; user has to scroll past every field to
  reach it.
- The result feels like a 2005 settings page, not a modern app pane.

## What Was Fixed

### Notion-style collapsible blocks

- Each thesis section becomes a `<details>` block with a chevron
  toggle and a compact summary line.
- Sections (5):
  1. **Industry** — open by default (the entry point)
  2. **Target demographics** — closed by default
  3. **Geography** — closed by default (new — was missing)
  4. **Notes** — closed by default
  5. **Layers / Excludes** — closed by default (new — was an array
     field but never wired into the form)
- Summary line of each closed block shows a *preview* of populated
  values, e.g.:
  - `Industry · Furniture stores`
  - `Demographics · HHI ≥ $75K · 25-44`
  - `Notes · (empty)` greyed if not set
- Save state per-section to localStorage `atlas_thesis_open_v1` so
  the layout the user shaped persists.
- Sticky "Save thesis" pill at the bottom of the view-pane (always
  visible without scroll).
- Compact, tight padding inside each block (less padding than the
  full-page form had); rely on chevron + summary for breathing room.
- Subtle hover state on the summary row (background tint).
- Add a tiny "Reset" link in the view header that clears the saved
  thesis after confirmation.

### Lessons Learned

- "Show every field at once" only works for short forms. Once we hit
  5+ inputs, default to collapsed blocks with informative summaries.
- The right hierarchy for a form pane: *primary action visible at all
  times, secondary fields one click away, defaults reasonable*.
- Native `<details>` is the right primitive — accessible, keyboard-
  navigable, no JS required for the toggle itself; we add JS only for
  the summary preview and the open-state persistence.
- When a user references a previously-discussed UX pattern ("like we
  were talking with notion"), echo that language back in the spec
  doc so the intent is preserved across revisions.

## Files Affected

| File | Action |
|---|---|
| `frontend/atlas.html` | Replace thesis-view markup; new CSS for `details.block`; new JS for summary previews + open-state persistence; sticky save bar |
| `docs/plans/PLAN_074_atlas_vscode_shell_rev_02.md` | Created (this file) |
| `docs/plans/PLAN_074_atlas_vscode_shell.md` | `## Revisions` updated to link rev_02 |
| `docs/specs/SPEC_084_atlas_thesis_blocks.md` | Created (companion) |
| `docs/specs/.active_spec` | → SPEC_084 |
| `tests/test_spec_084_atlas_thesis_blocks.py` | Created |

## Acceptance

- [ ] Thesis view renders 5 `<details>` blocks, each with chevron + summary.
- [ ] Industry block expanded by default; others collapsed.
- [ ] Summary text shows populated value(s) when filled, "(empty)"
      greyed when not.
- [ ] Open-state persists to localStorage `atlas_thesis_open_v1`.
- [ ] Sticky Save bar at bottom, always visible.
- [ ] Reset link clears thesis after confirm.
- [ ] No regression to `thesis_context` plumbing — Pilot still gets the
      same field set.
