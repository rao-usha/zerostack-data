# SPEC 097 — Chat-driven nav tools (PLAN_078 Layer 3)

**Status:** Draft
**Date:** 2026-06-01
**Builds on:** SPEC_090 (Pilot tools) · SPEC_095 (session state)
**Test file:** tests/test_spec_097_chat_driven_nav.py

## Goal

Five new Pilot tools so the agent can drive the whole Decision Map
from chat — pick a candidate, retune the recipe, recap the view, edit
the thesis form.

## Acceptance

- [ ] **`select_pin(rank?|geo_id?)`** UI tool: opens trade area for
      that candidate. Rank ∈ [1,10]; if both given, geo_id wins.
- [ ] **`set_fit_weights(weights)`** UI tool: overrides the active
      recipe weights and re-fits. `weights` keys are weight names
      (income / commercial / broadband); values normalised to sum 1.
- [ ] **`describe_session()`** read tool: returns a compact session
      summary the agent can quote without inflating the system prompt.
- [ ] **`reset_thesis()`** UI tool: clears the thesis form.
- [ ] **`set_thesis_field(key, value)`** UI tool: sets one thesis
      field; whitelist of editable keys (industry_label, industry_naics,
      region, target_hhi_min, target_hhi_max, target_age_band,
      target_pop_density_min, notes).
- [ ] Frontend `applyOnePilotAction` handles every new UI action.
- [ ] `compute_fit_score(...)` accepts an optional `weights_override`
      dict; frontend `fetchAndPaintFitScore` forwards CUSTOM_WEIGHTS
      when set; cleared on reset.

## Test cases (T1-T10 backend tool registration + dispatch shape;
T11-T14 frontend dispatcher branches; T15 weights_override happy path)

## Design

- `_VALID_THESIS_KEYS = {industry_label, industry_naics, region,
  target_hhi_min, target_hhi_max, target_age_band,
  target_pop_density_min, notes}`.
- Weights override is renormalised in `compute_fit_score`: if any user
  override is provided, the matching `_RECIPES` recipe's component
  weights are replaced by the override values, then renormalised so
  they sum to 1 over the components that loaded.
- `describe_session` returns the formatted block text the system
  prompt uses, but as a tool response so the agent can quote it.
