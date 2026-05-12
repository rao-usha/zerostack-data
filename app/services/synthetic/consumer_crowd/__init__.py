"""
Synthetic Consumer Crowd — PLAN_062 addendum 1.

Generates synthetic consumer-response data via distillation:
  Phase 1: GPT-4o teacher produces ~2,000 (persona × scenario) → outcome rows
  Phase 2: LightGBM per-outcome regressors distill the teacher signal
  Phase 3: FastAPI router serves crowd responses at <1ms/query
  Phase 4 (follow-on): Real-data calibration vs. Yelp/Census

This package is independent of the v1 parametric synthetic generators
(macro_scenarios, private_company_financials) but uses PLAN_062 Phase A1
TabDDPM-generated company profiles as optional brand context.
"""
