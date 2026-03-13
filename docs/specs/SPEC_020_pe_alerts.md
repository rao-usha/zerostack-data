# SPEC 020 — PE Alert Subscriptions & Notification API

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-03-13
**Test file:** tests/test_spec_020_pe_alerts.py

## Goal

Build alert subscription management and notification API for PE portfolio monitoring. Enables firms to subscribe to alert types, view alert history, and manually trigger portfolio health checks.

## Acceptance Criteria

- [ ] subscribe/unsubscribe/list subscriptions for a firm
- [ ] get_alert_history returns recent alerts with filtering
- [ ] GET /pe/alerts/{firm_id} returns alerts
- [ ] POST /pe/alerts/{firm_id}/subscribe creates subscription
- [ ] DELETE /pe/alerts/{firm_id}/subscribe removes subscription
- [ ] GET /pe/alerts/{firm_id}/subscriptions lists active subscriptions
- [ ] POST /pe/monitor/{firm_id}/run triggers health check and returns report

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_subscribe_creates_subscription | Subscription dict is well-formed |
| T2 | test_unsubscribe_disables | Unsubscribe sets enabled=False |
| T3 | test_list_subscriptions | Returns only enabled subs |
| T4 | test_alert_history_format | Alert dicts have required keys |
| T5 | test_health_report_response_model | Response model has all fields |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_alert_subscriptions.py | Create | Subscription service |
| app/api/v1/pe_benchmarks.py | Modify | Add 5 endpoints |
| tests/test_spec_020_pe_alerts.py | Create | Unit tests |
