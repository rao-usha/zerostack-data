# PLAN 026 — PE Portfolio Monitoring & Autonomous Alerts

## Overview

Build automated PE portfolio monitoring that detects changes and proactively alerts users. Wire PE events into the existing webhook infrastructure and add scheduled health checks.

## Phase 1: PE Webhook Event Types & Portfolio Monitor Service

**New PE event types** (add to `WebhookEventType` in `app/core/models.py`):
- `PE_EXIT_READINESS_CHANGE` — score crosses grade boundary
- `PE_DEAL_STAGE_CHANGE` — deal moves between pipeline stages
- `PE_FINANCIAL_ALERT` — revenue decline >10%, margin drops
- `PE_LEADERSHIP_CHANGE` — C-suite addition/departure
- `PE_NEW_MARKET_OPPORTUNITY` — high-momentum sector detected
- `PE_PORTFOLIO_HEALTH_SUMMARY` — weekly digest

**New model** (`pe_portfolio_snapshots` table in `pe_models.py`):
- company_id, snapshot_date, exit_score, exit_grade, revenue, ebitda_margin, leadership_count, data (JSONB)

**New service** (`app/core/pe_portfolio_monitor.py`):
- `monitor_exit_readiness(firm_id)` — recalc scores, compare snapshots, fire webhooks on grade change
- `monitor_financials(firm_id)` — flag >10% revenue swing, margin compression
- `monitor_leadership(firm_id)` — detect C-suite departures/additions vs snapshot
- `run_full_portfolio_check(firm_id)` — orchestrate all monitors, return PortfolioHealthReport

## Phase 2: Alert Subscriptions & Notification API

**New models**:
- `pe_alert_subscriptions` — firm_id, alert_type, webhook_id, enabled, created_at
- `pe_alerts` — firm_id, company_id, alert_type, severity, title, detail (JSONB), created_at, acknowledged_at

**New service** (`app/core/pe_alert_subscriptions.py`):
- subscribe/unsubscribe/list_subscriptions/get_alert_history

**5 endpoints** in pe_benchmarks.py:
- GET /pe/alerts/{firm_id}
- POST /pe/alerts/{firm_id}/subscribe
- DELETE /pe/alerts/{firm_id}/subscribe
- GET /pe/alerts/{firm_id}/subscriptions
- POST /pe/monitor/{firm_id}/run

## Phase 3: Scheduled Health Checks

- Register `scheduled_pe_portfolio_check()` in main.py — daily at 6 AM UTC via CronTrigger
- Register weekly digest — Mondays 8 AM UTC
- Add `GET /pe/monitor/{firm_id}/health` — portfolio health dashboard data

## Phase 4: Seed Demo Alerts & Verification

- Add 10-15 historical alerts and 2-3 snapshots per firm to demo_seeder.py
- Verify via POST /pe/monitor/{firm_id}/run
- Run full test suite

## File Ownership

| File | Action |
|------|--------|
| app/core/models.py | Add 6 PE event types to WebhookEventType |
| app/core/pe_models.py | Add 3 new tables |
| app/core/pe_portfolio_monitor.py | Create — monitor service |
| app/core/pe_alert_subscriptions.py | Create — alert subscription service |
| app/api/v1/pe_benchmarks.py | Add 6 endpoints |
| app/sources/pe/demo_seeder.py | Add alert/snapshot seed data |
| app/main.py | Register 2 scheduled tasks |
| tests/test_spec_019_pe_portfolio_monitor.py | Create |
| tests/test_spec_020_pe_alerts.py | Create |
