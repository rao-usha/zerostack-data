# PLAN_056 — Synthetic Data API Console

**Status:** COMPLETE (built by agent in worktree agent-a1a8ca4e)

## Context

We have 4 synthetic generators with API endpoints, but no way for a user to discover, configure, or trigger them from the UI. This plan integrates them into the existing Sources tab as a "Synthetic Data" category with interactive parameter forms and generate buttons.

## Approach

No new HTML file needed. The existing Sources detail view pattern (openSourceDetail → renderDetailView) already handles: description, trigger forms with parameters, job history, table preview, API docs. We add the 4 synthetic sources to the JS `SOURCE_REGISTRY` and `TRIGGER_FORMS`.

## Changes Made (all in frontend/index.html)

1. **SOURCE_REGISTRY** — Added `synthetic` category with 4 source entries (macro scenarios, private financials, job postings, LP-GP universe), each with `origin: 'synthetic'`, correct tablePrefix, POST trigger
2. **TRIGGER_FORMS** — Added 4 form definitions with correct parameters (sliders, dropdowns, text inputs)
3. **collectFormValues** — Added `'series'` to comma-separated array field list
4. **Number parsing** — Changed `parseInt(val)` to `Number(val)` for float support
5. **Origin filter fix** — Explore view now respects `currentOriginFilter` for real/synthetic filtering

## Worktree

Branch: `worktree-agent-a1a8ca4e`
Path: `.claude/worktrees/agent-a1a8ca4e/`
