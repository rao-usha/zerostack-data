"""
Seed script — creates all 23 eval suites and their baseline cases.

Run:
    python scripts/seed_eval_suites.py [--host http://localhost:8001] [--dry-run]

Idempotent: skips any suite whose name already exists.
"""

import argparse
import sys
import httpx

DEFAULT_HOST = "http://localhost:8001"

# ---------------------------------------------------------------------------
# Suite + case definitions
# ---------------------------------------------------------------------------
# Each entry: suite dict + "cases" list of case dicts.
# entity_id values reference real DB rows — update if your DB differs.

EVAL_SUITES = [

    # =========================================================================
    # PRIORITY 1 — Core Product (client-facing, demo-critical)
    # =========================================================================

    {
        "name": "P1 — Deep Collect: Industrial Companies",
        "description": "Evaluates DeepCollectionOrchestrator output quality for industrial company org charts.",
        "domain": "people",
        "binding_type": "agent",
        "binding_target": "app.sources.people_collection.deep_collection_orchestrator.DeepCollectionOrchestrator",
        "eval_mode": "agent_output",
        "priority": 1,
        "schedule_cron": "0 6 * * 1",  # Monday 6 AM UTC
        "cases": [
            {"name": "AIT — CEO extracted", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "ceo_exists", "assertion_params": {}, "tier": 1},
            {"name": "AIT — No extraction errors", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "no_extraction_errors", "assertion_params": {}, "tier": 1},
            {"name": "AIT — Headcount 5–300", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "headcount_range", "assertion_params": {"min": 5, "max": 300}, "tier": 2},
            {"name": "AIT — Org depth 1–10", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "org_depth_range", "assertion_params": {"min": 1, "max": 10}, "tier": 2},
            {"name": "AIT — No duplicate CEO", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "no_duplicate_ceo", "assertion_params": {}, "tier": 2},
        ],
    },

    {
        "name": "P1 — PE Collection: Blackstone",
        "description": "Evaluates PE collection pipeline output for Blackstone via DB snapshot.",
        "domain": "pe",
        "binding_type": "api",
        "binding_target": "/api/v1/pe-collection/collect",
        "eval_mode": "db_snapshot",
        "priority": 1,
        "schedule_cron": "0 7 * * 1",  # Monday 7 AM UTC
        "cases": [
            {"name": "Blackstone — Deal count ≥ 1", "entity_type": "pe_firm", "entity_id": 1, "entity_name": "Blackstone",
             "assertion_type": "deal_count_range", "assertion_params": {"min": 1}, "tier": 1},
            {"name": "Blackstone — Has active deal", "entity_type": "pe_firm", "entity_id": 1, "entity_name": "Blackstone",
             "assertion_type": "has_deal_with_status", "assertion_params": {"status": "active"}, "tier": 2},
        ],
    },

    {
        "name": "P1 — AI Report: Management Assessment",
        "description": "Evaluates HTML quality of the management-assessment report generator.",
        "domain": "reports",
        "binding_type": "report",
        "binding_target": "/api/v1/people-reports/management-assessment",
        "eval_mode": "report_output",
        "priority": 1,
        "schedule_cron": "0 8 * * 1",  # Monday 8 AM UTC
        "cases": [
            {"name": "Report — Org Chart section present", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "report_section_present", "assertion_params": {"section": "Org Chart"}, "tier": 1},
            {"name": "Report — Leadership section present", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "report_section_present", "assertion_params": {"section": "Leadership"}, "tier": 1},
            {"name": "Report — Word count 200+", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "report_word_count", "assertion_params": {"min": 200}, "tier": 2},
            {"name": "Report — Data cells populated 50%+", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "report_data_cells_pct", "assertion_params": {"min_pct": 50}, "tier": 2},
            {"name": "Report — No empty tables", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "report_no_empty_tables", "assertion_params": {}, "tier": 2},
        ],
    },

    {
        "name": "P1 — Datacenter Site Thesis",
        "description": "Evaluates AI thesis generation for datacenter county site selection.",
        "domain": "site_intel",
        "binding_type": "api",
        "binding_target": "/api/v1/datacenter-sites/{entity_id}/thesis",
        "eval_mode": "api_response",
        "priority": 1,
        "schedule_cron": "0 8 * * 2",  # Tuesday 8 AM UTC
        "cases": [
            {"name": "Thesis — HTTP 200", "entity_type": "county", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Thesis — Word count 100+", "entity_type": "county", "entity_id": None, "entity_name": None,
             "assertion_type": "response_word_count_range", "assertion_params": {"min": 100}, "tier": 2},
        ],
    },

    # =========================================================================
    # PRIORITY 2 — Demo & Research Workflows
    # =========================================================================

    {
        "name": "P2 — People Test Collect: AIT",
        "description": "Evaluates PeopleCollectionOrchestrator basic collection via test endpoint.",
        "domain": "people",
        "binding_type": "api",
        "binding_target": "/api/v1/people-jobs/test/{entity_id}",
        "eval_mode": "api_response",
        "priority": 2,
        "schedule_cron": "0 9 * * 1",
        "cases": [
            {"name": "Test collect — HTTP 200", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Test collect — No extraction errors key", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "response_no_error_key", "assertion_params": {}, "tier": 2},
        ],
    },

    {
        "name": "P2 — Deep Research Agent",
        "description": "Evaluates DeepResearchAgent output — thesis and risks fields present.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/agents/deep-research",
        "eval_mode": "api_response",
        "priority": 2,
        "schedule_cron": "0 10 * * 1",
        "cases": [
            {"name": "Deep research — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Deep research — thesis field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "thesis"}, "tier": 1},
            {"name": "Deep research — risks field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "risks"}, "tier": 2},
        ],
    },

    {
        "name": "P2 — Company Research Agent",
        "description": "Evaluates CompanyResearchAgent output — summary present, response time.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/agents/research/company",
        "eval_mode": "api_response",
        "priority": 2,
        "schedule_cron": "0 10 * * 2",
        "cases": [
            {"name": "Company research — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Company research — summary field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "summary"}, "tier": 1},
            {"name": "Company research — responds < 30s", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_time_ms", "assertion_params": {"max_ms": 30000}, "tier": 2},
        ],
    },

    {
        "name": "P2 — Due Diligence Agent",
        "description": "Evaluates DueDiligenceAgent output — risk score field and range.",
        "domain": "dd",
        "binding_type": "agent",
        "binding_target": "/api/v1/diligence/start",
        "eval_mode": "api_response",
        "priority": 2,
        "schedule_cron": "0 11 * * 1",
        "cases": [
            {"name": "DD — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "DD — risk_score field present", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "risk_score"}, "tier": 1},
            {"name": "DD — risk_score 0–100", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_range", "assertion_params": {"field": "risk_score", "min": 0, "max": 100}, "tier": 2},
        ],
    },

    {
        "name": "P2 — Fund Conviction Scorer",
        "description": "Evaluates FundConvictionScorer — score and grade fields.",
        "domain": "pe",
        "binding_type": "api",
        "binding_target": "/api/v1/pe/conviction/score/{entity_id}",
        "eval_mode": "api_response",
        "priority": 2,
        "schedule_cron": "0 11 * * 2",
        "cases": [
            {"name": "Conviction — score 0–100", "entity_type": "pe_firm", "entity_id": 1, "entity_name": "Blackstone",
             "assertion_type": "response_field_range", "assertion_params": {"field": "score", "min": 0, "max": 100}, "tier": 1},
            {"name": "Conviction — grade field", "entity_type": "pe_firm", "entity_id": 1, "entity_name": "Blackstone",
             "assertion_type": "response_field_present", "assertion_params": {"field": "grade"}, "tier": 2},
        ],
    },

    {
        "name": "P2 — Proxy Comp Agent",
        "description": "Evaluates ProxyCompAgent — salary and equity fields extracted.",
        "domain": "people",
        "binding_type": "agent",
        "binding_target": "/api/v1/people/companies/{entity_id}/collect-comp",
        "eval_mode": "api_response",
        "priority": 2,
        "schedule_cron": "0 12 * * 1",
        "cases": [
            {"name": "Proxy comp — HTTP 200", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Proxy comp — salary field", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "response_field_present", "assertion_params": {"field": "salary"}, "tier": 2},
            {"name": "Proxy comp — equity field", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "response_field_present", "assertion_params": {"field": "equity"}, "tier": 2},
        ],
    },

    {
        "name": "P2 — Recursive Collect",
        "description": "Evaluates RecursiveCollector output for subsidiary org charts.",
        "domain": "people",
        "binding_type": "agent",
        "binding_target": "/api/v1/people-jobs/recursive-collect/{entity_id}",
        "eval_mode": "agent_output",
        "priority": 2,
        "schedule_cron": "0 9 * * 3",
        "cases": [
            {"name": "Recursive — CEO found", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "ceo_exists", "assertion_params": {}, "tier": 1},
            {"name": "Recursive — Headcount ≥ 3", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "headcount_range", "assertion_params": {"min": 3}, "tier": 2},
        ],
    },

    # =========================================================================
    # PRIORITY 3 — Background Pipelines
    # =========================================================================

    {
        "name": "P3 — Batch Company Research",
        "description": "Evaluates batch variant of CompanyResearchAgent — list response.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/agents/research/batch",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Batch research — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Batch research — list has ≥ 1 result", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_list_length", "assertion_params": {"min": 1}, "tier": 2},
        ],
    },

    {
        "name": "P3 — Competitive Intel Agent",
        "description": "Evaluates CompetitiveIntelAgent output — competitors field + list.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/competitive/analyze",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Competitive — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Competitive — competitors field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "competitors"}, "tier": 1},
            {"name": "Competitive — ≥ 1 competitor", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_list_length", "assertion_params": {"field": "competitors", "min": 1}, "tier": 2},
        ],
    },

    {
        "name": "P3 — Market Scanner Agent",
        "description": "Evaluates MarketScannerAgent — signals field present.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/market/scan/trigger",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Market scan — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Market scan — signals field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "signals"}, "tier": 2},
        ],
    },

    {
        "name": "P3 — Data Hunter Agent",
        "description": "Evaluates DataHunterAgent — filled_count field.",
        "domain": "dd",
        "binding_type": "agent",
        "binding_target": "/api/v1/hunter/start",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Hunter — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Hunter — filled_count field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "filled_count"}, "tier": 2},
        ],
    },

    {
        "name": "P3 — Anomaly Detector Agent",
        "description": "Evaluates AnomalyDetectorAgent — anomalies field present.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/anomalies/scan",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Anomaly scan — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Anomaly scan — anomalies field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "anomalies"}, "tier": 2},
        ],
    },

    {
        "name": "P3 — News Monitor",
        "description": "Evaluates NewsMonitor — matches_created field.",
        "domain": "research",
        "binding_type": "agent",
        "binding_target": "/api/v1/monitors/news/process",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "News monitor — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "News monitor — matches_created field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "matches_created"}, "tier": 2},
        ],
    },

    {
        "name": "P3 — LP Collection Pipeline",
        "description": "Evaluates LP collection via DB snapshot — enrichment coverage and count.",
        "domain": "lp",
        "binding_type": "api",
        "binding_target": "/api/v1/lp-collection/collect",
        "eval_mode": "db_snapshot",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "LP — Count ≥ 1", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "lp_count_range", "assertion_params": {"min": 1}, "tier": 1},
            {"name": "LP — Enrichment coverage ≥ 50%", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "enrichment_coverage_pct", "assertion_params": {"source": "website", "min_pct": 50}, "tier": 2},
        ],
    },

    {
        "name": "P3 — Family Office Collection Pipeline",
        "description": "Evaluates FO collection via DB snapshot — enrichment coverage.",
        "domain": "fo",
        "binding_type": "api",
        "binding_target": "/api/v1/fo-collection/collect",
        "eval_mode": "db_snapshot",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "FO — Enrichment coverage ≥ 50%", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "enrichment_coverage_pct", "assertion_params": {"source": "website", "min_pct": 50}, "tier": 2},
        ],
    },

    {
        "name": "P3 — Datacenter Site Scoring",
        "description": "Evaluates county regulatory scoring via DB snapshot.",
        "domain": "site_intel",
        "binding_type": "api",
        "binding_target": "/api/v1/datacenter-sites/score-counties",
        "eval_mode": "db_snapshot",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Site scores — Composite 0–100", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "score_field_range", "assertion_params": {"field": "composite", "min": 0, "max": 100}, "tier": 1},
        ],
    },

    {
        "name": "P3 — Macro Sensitivity Agent",
        "description": "Evaluates MacroSensitivityAgent — scenarios field present.",
        "domain": "macro",
        "binding_type": "agent",
        "binding_target": "/api/v1/macro/simulate",
        "eval_mode": "api_response",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "Macro sim — HTTP 200", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_status_200", "assertion_params": {}, "tier": 1},
            {"name": "Macro sim — scenarios field", "entity_type": "api_response", "entity_id": None, "entity_name": None,
             "assertion_type": "response_field_present", "assertion_params": {"field": "scenarios"}, "tier": 2},
        ],
    },

    {
        "name": "P3 — DB Snapshot: People Quality (AIT)",
        "description": "Lightweight db_snapshot check on Applied Industrial Technologies org chart.",
        "domain": "people",
        "binding_type": "db",
        "binding_target": "company",
        "eval_mode": "db_snapshot",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "AIT — CEO in DB", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "ceo_exists", "assertion_params": {}, "tier": 1},
            {"name": "AIT — Headcount 3–300 in DB", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "headcount_range", "assertion_params": {"min": 3, "max": 300}, "tier": 2},
            {"name": "AIT — Confidence threshold 0.6", "entity_type": "company", "entity_id": 4, "entity_name": "Applied Industrial Technologies",
             "assertion_type": "confidence_threshold", "assertion_params": {"min_score": 0.6}, "tier": 2},
        ],
    },

    {
        "name": "P3 — DB Snapshot: PE Firms (KKR)",
        "description": "Lightweight db_snapshot on KKR deal and people data quality.",
        "domain": "pe",
        "binding_type": "db",
        "binding_target": "pe_firm",
        "eval_mode": "db_snapshot",
        "priority": 3,
        "schedule_cron": None,
        "cases": [
            {"name": "KKR — Deal count ≥ 1", "entity_type": "pe_firm", "entity_id": 2, "entity_name": "KKR",
             "assertion_type": "deal_count_range", "assertion_params": {"min": 1}, "tier": 1},
            {"name": "KKR — Has active deal", "entity_type": "pe_firm", "entity_id": 2, "entity_name": "KKR",
             "assertion_type": "has_deal_with_status", "assertion_params": {"status": "active"}, "tier": 2},
        ],
    },
]


# ---------------------------------------------------------------------------
# Seed runner
# ---------------------------------------------------------------------------

def seed(host: str, dry_run: bool = False) -> None:
    base = host.rstrip("/")
    client = httpx.Client(timeout=30.0)

    # Fetch existing suite names to avoid duplicates
    resp = client.get(f"{base}/api/v1/evals/suites?active_only=false")
    resp.raise_for_status()
    existing = {s["name"] for s in resp.json()}
    print(f"Found {len(existing)} existing suites.")

    created_suites = 0
    created_cases = 0

    for suite_def in EVAL_SUITES:
        cases = suite_def.pop("cases", [])
        name = suite_def["name"]

        if name in existing:
            print(f"  SKIP  {name}")
            suite_def["cases"] = cases
            continue

        if dry_run:
            print(f"  DRY   {name} ({len(cases)} cases)")
            suite_def["cases"] = cases
            continue

        # Create suite
        resp = client.post(f"{base}/api/v1/evals/suites", json=suite_def)
        if resp.status_code != 201:
            print(f"  ERROR {name}: {resp.status_code} {resp.text}")
            suite_def["cases"] = cases
            continue

        suite_id = resp.json()["id"]
        created_suites += 1
        print(f"  CREATE suite {suite_id}: {name}")

        # Create cases
        for case in cases:
            cresp = client.post(f"{base}/api/v1/evals/suites/{suite_id}/cases", json=case)
            if cresp.status_code == 201:
                created_cases += 1
            else:
                print(f"    CASE ERROR {case['name']}: {cresp.status_code} {cresp.text}")

        print(f"    + {len(cases)} cases added")
        suite_def["cases"] = cases

    print(f"\nDone. Created {created_suites} suites, {created_cases} cases.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed eval suites for all 23 agent endpoints")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    seed(args.host, args.dry_run)
