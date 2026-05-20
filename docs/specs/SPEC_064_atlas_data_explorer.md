# SPEC 064 — Nexdata Atlas Data Explorer

**Status:** Draft
**Task type:** service
**Date:** 2026-05-20
**Plan:** Atlas pivot from PLAN_065 report-first monetization
**Test file:** `tests/test_spec_064_atlas_data_explorer.py`
**Builds on:** SPEC_060 taxonomies, SPEC_061 market-intelligence data foundation,
SPEC_062 diligence orders, SPEC_063 diligence intake page

## Goal

Build the first version of **Nexdata Atlas**: an interactive public-data
exploration engine that exposes cross-dataset connective tissue across sector,
market, geography, and source data. Atlas replaces the paid-report-first user
journey with a viral exploration surface: query -> insight cards -> data
connections -> source trail -> share/fork -> usage telemetry.

Reports, diligence orders, and the Synthetic Playground remain useful, but they
move downstream:

- reports = export/deep-dive,
- orders = concierge/custom-work fallback,
- playground = synthetic proof artifact,
- leads = telemetry/monetization signal after exploration proves value.

## Required Cleanup / Removal

This spec is not only an additive Atlas build. It must also remove or demote the
parts of the current product that send users into a low-value report-purchase
flow before they have seen data value.

### Remove from primary UX

- `frontend/diligence.html` must no longer be the primary CTA from public
  product surfaces. Keep the file for now, but demote it behind Atlas as
  "Request a custom deep dive" after an exploration.
- Existing playground/report CTAs must stop pointing at generic platform or
  report-order language. They should point to Atlas exploration first.
- `$2,500 / $7,500` pricing copy should not appear in first-touch hero sections.
  Pricing can remain in the order API and diligence page for fallback/manual
  sales, but it is not the viral product hook.
- PLAN_064-style lead ops (Slack alerts, daily lead digest, `leads.html`) must
  stay paused. Do not build more lead-management UI until Atlas produces real
  usage.
- Do not present `market_intelligence_pack` as the thing users buy. Present it
  only as "export this exploration" or "request a deeper custom analysis."

### Keep but reposition

- `market_intelligence_pack` remains valuable as a deterministic export/deep
  dive renderer.
- `diligence_orders` and `/diligence-pack/*` remain as a concierge fallback for
  users who request custom work after exploring.
- Synthetic Playground remains a proof/demo surface, but its CTAs should route
  toward Atlas.
- Lead capture remains useful only as telemetry/monetization support, not as the
  main product objective.

### Explicit files to revisit after Atlas works

- `frontend/playground.html` — update CTA copy/links toward `/atlas.html`.
- `app/reports/templates/synthetic_playground.py` — update CTA URL defaults or
  copy so reports lead to Atlas, not generic platform/report purchase.
- `frontend/diligence.html` — change hero from "buy a pack" to "custom deep
  dive after exploring"; remove first-screen price emphasis.
- `app/services/diligence/orders.py` and `app/api/v1/diligence_pack.py` — keep
  functional, but do not expand scope in this spec.
- `docs/plans/PLAN_064_playground_leads_ops.md` — treat as paused/superseded by
  Atlas telemetry; do not implement.
- `docs/specs/SPEC_062_diligence_orders_intake_api.md` and
  `docs/specs/SPEC_063_diligence_pack_intake_page.md` — leave as historical
  shipped specs, but do not use them as the active commercial plan.

Acceptance for cleanup:

- [ ] Public CTAs from playground/report surfaces prefer Atlas exploration over
      report ordering.
- [ ] `frontend/diligence.html` is still reachable but visually demoted to a
      custom-work fallback, not the main product.
- [ ] No new Slack/digest/leads admin work is introduced.
- [ ] No first-touch page leads with `$2,500 / $7,500` pricing.

## Commercial Principle

Do not compete with Anthropic/OpenAI on prose generation. Users increasingly
expect AI reports to be free. Nexdata's differentiated value is:

- governed public-data joins,
- entity and geography resolution,
- cross-dataset insights,
- source provenance,
- coverage honesty,
- learning from how users explore the data.

The product should not merely answer. It should learn which answers are worth
showing first.

## Product Scope — V1

### First path: MSA x NAICS Explorer

Initial test query:

> Houston building equipment contractors

Atlas resolves:

- `msa`: Houston-The Woodlands-Sugar Land, TX (`26420`)
- `naics`: Building Equipment Contractors (`2382`) and related child/parent codes
- `geographies`: constituent counties from SPEC_060 MSA reference data
- `datasets`: only the datasets with actual usable rows for this query

### Initial insight cards

Build 6-8 cards, each skip-on-empty:

1. **Industry Footprint** — Census CBP / business-pattern establishment density
2. **Local Wealth & Demand** — IRS SOI / ACS where geo IDs work
3. **Risk Context** — FEMA disaster frequency and related public risk signals
4. **Connectivity** — FCC broadband coverage / summary
5. **Logistics & Trade** — BTS, port throughput, container freight, trade gateway data
6. **Macro & Rates** — FRED / Treasury context relevant to the sector
7. **Bank & Credit Context** — FDIC/Treasury where relevant
8. **Named Operators** — EPA-ECHO/company/operator data where available

Do not fabricate missing cards. If a card has insufficient data, omit it and
add a coverage note to the exploration summary.

## API Contract

Create:

- `app/services/atlas/__init__.py`
- `app/services/atlas/types.py`
- `app/services/atlas/resolver.py`
- `app/services/atlas/cards.py`
- `app/services/atlas/graph.py`
- `app/services/atlas/telemetry.py`
- `app/api/v1/atlas.py`

### Endpoints

- `POST /api/v1/atlas/explore`
  - Request: `{ "query": "...", "msa": optional, "naics": optional }`
  - Returns an exploration object.
- `GET /api/v1/atlas/explorations/{slug_or_id}`
  - Returns a saved/shareable exploration.
- `POST /api/v1/atlas/events`
  - Records user interaction telemetry.
- `GET /api/v1/atlas/taxonomies`
  - Returns only NAICS/MSA options backed by actual data coverage where possible.

Optional later:

- `POST /api/v1/atlas/export-report`
  - Generates a `market_intelligence_pack` export from an exploration.

### Exploration object

```json
{
  "id": "uuid-or-int",
  "slug": "houston-building-equipment-contractors",
  "query": "Houston building equipment contractors",
  "resolved_entities": {
    "msa": {"code": "26420", "title": "Houston-The Woodlands-Sugar Land, TX"},
    "naics": {"code": "2382", "label": "Building Equipment Contractors"},
    "geographies": [],
    "datasets": []
  },
  "summary": {
    "headline": "...",
    "why_interesting": "...",
    "coverage_notes": []
  },
  "cards": [
    {
      "id": "industry_footprint",
      "title": "Industry Footprint",
      "summary": "...",
      "metrics": [],
      "why_it_matters": "...",
      "datasets_used": ["census_cbp"],
      "confidence": "high",
      "coverage": {},
      "provenance": [],
      "links": []
    }
  ],
  "connections": [
    {
      "source_card": "industry_footprint",
      "target_card": "risk_context",
      "relationship": "same geography",
      "strength": 0.5
    }
  ],
  "related_queries": [],
  "share_url": "/atlas/houston-building-equipment-contractors"
}
```

## Data Model

Create these tables idempotently in an Atlas service. Use explicit typed columns
plus JSONB for structured payload snapshots where appropriate.

```sql
CREATE TABLE IF NOT EXISTS atlas_explorations (
    id SERIAL PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    query TEXT NOT NULL,
    resolved_msa_code TEXT,
    resolved_msa_title TEXT,
    resolved_naics_code TEXT,
    resolved_naics_label TEXT,
    summary JSONB NOT NULL DEFAULT '{}',
    cards JSONB NOT NULL DEFAULT '[]',
    connections JSONB NOT NULL DEFAULT '[]',
    related_queries JSONB NOT NULL DEFAULT '[]',
    coverage_notes JSONB NOT NULL DEFAULT '[]',
    is_public BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS atlas_queries (
    id SERIAL PRIMARY KEY,
    exploration_id INTEGER REFERENCES atlas_explorations(id),
    query TEXT NOT NULL,
    session_id TEXT,
    anon_ip TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS atlas_events (
    id SERIAL PRIMARY KEY,
    exploration_id INTEGER REFERENCES atlas_explorations(id),
    session_id TEXT,
    event_type TEXT NOT NULL,
    card_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS atlas_card_feedback (
    id SERIAL PRIMARY KEY,
    exploration_id INTEGER REFERENCES atlas_explorations(id),
    card_id TEXT NOT NULL,
    session_id TEXT,
    feedback TEXT NOT NULL,
    comment TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS atlas_shared_links (
    id SERIAL PRIMARY KEY,
    exploration_id INTEGER REFERENCES atlas_explorations(id),
    share_code TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    view_count INTEGER NOT NULL DEFAULT 0
);
```

## Telemetry

Track events that train future card ranking and ingestion priorities:

- `query_submitted`
- `entity_resolved`
- `card_viewed`
- `card_expanded`
- `connection_clicked`
- `related_query_clicked`
- `source_opened`
- `share_created`
- `export_clicked`
- `thumbs_up`
- `thumbs_down`
- `followup_question`

V1 only records telemetry. Ranking logic can be simple deterministic ordering.
Future versions can use events to re-rank cards.

## Frontend

Create:

- `frontend/atlas.html`

Static HTML + vanilla JS only. Follow `playground.html` / `diligence.html`
styling patterns.

Sections:

1. Search hero
2. Resolved entity chips
3. Insight card grid
4. Connection graph or relationship list
5. Source/provenance drawer
6. Related queries
7. Share/fork controls
8. Upgrade/export CTAs only after exploration value is visible

Viral mechanics:

- every exploration gets a public URL,
- "Fork this search",
- "Compare another MSA",
- "Open source trail",
- "Copy insight card",
- "Share exploration".

## Acceptance Criteria

- [ ] `POST /api/v1/atlas/explore` returns a valid exploration for
      `"Houston building equipment contractors"` using real available data.
- [ ] The resolver accepts direct `msa` + `naics` parameters and can also infer
      the Houston/NAICS-2382 example from query text.
- [ ] Every card includes `datasets_used`, `confidence`, `coverage`, and
      `provenance`.
- [ ] Missing data produces coverage notes, not fake cards or exceptions.
- [ ] At least 4 cards render for the Houston/2382 smoke test when expected
      source tables are populated.
- [ ] Connections are returned between cards that share geography, source
      family, NAICS, or risk/demand relevance.
- [ ] Exploration slugs are stable and shareable.
- [ ] `POST /api/v1/atlas/events` records telemetry without auth.
- [ ] `frontend/atlas.html` can search, render cards, open provenance, click
      related queries, and create/copy a share link.
- [ ] Atlas router is registered in `app/main.py` under `/api/v1`.
- [ ] Existing SPEC_052-063 tests keep passing.

## Tests

| ID | Test | Verifies |
|---|---|---|
| T1 | test_resolver_direct_msa_naics | Direct `msa=26420`, `naics=2382` resolves expected labels/counties |
| T2 | test_resolver_query_text_houston_2382 | Query text infers Houston + building-equipment contractors |
| T3 | test_explore_returns_cards_with_provenance | Cards include required Atlas fields |
| T4 | test_missing_data_creates_coverage_note | Empty section -> coverage note, no fake card |
| T5 | test_slug_stable | Same query/entities produce same slug |
| T6 | test_connections_reference_existing_cards | Connection card IDs all exist in response cards |
| T7 | test_event_endpoint_records_event | `card_expanded` event persists in `atlas_events` |
| T8 | test_feedback_endpoint_records_feedback | thumbs up/down persists in `atlas_card_feedback` |
| T9 | test_taxonomies_filters_to_available_options | Endpoint returns NAICS/MSA options in expected shape |
| T10 | test_router_smoke_explore | FastAPI TestClient can POST `/atlas/explore` |

## What Not To Build In V1

- No LLM-generated long-form report as the default output.
- No payment-first order form.
- No Slack/digest lead ops.
- No private workspaces.
- No external scraping or new data collection.
- No claims about PE/person/org/family-office coverage unless populated.
- No social media scraping or ToS-hostile collection.

## Future Monetization

Free:

- public explorations,
- limited daily searches,
- shareable links,
- source cards.

Paid:

- private saved explorations,
- exports,
- monitoring,
- bulk comparisons,
- team workspaces,
- API access,
- private-data joins.

## Files To Create/Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_064_atlas_data_explorer.md` | Create | This spec |
| `tests/test_spec_064_atlas_data_explorer.py` | Create | T1-T10 |
| `docs/specs/.active_spec` | Modify | Point to this spec before source edits |
| `app/services/atlas/__init__.py` | Create | Atlas package |
| `app/services/atlas/types.py` | Create | Typed response models/dataclasses |
| `app/services/atlas/resolver.py` | Create | Query/entity resolver |
| `app/services/atlas/cards.py` | Create | Card builders |
| `app/services/atlas/graph.py` | Create | Card connection builder |
| `app/services/atlas/telemetry.py` | Create | Tables + event recording |
| `app/api/v1/atlas.py` | Create | Public Atlas router |
| `app/main.py` | Modify | Register router |
| `frontend/atlas.html` | Create | Public explorer UI |
| `frontend/playground.html` | Modify later | CTA to Atlas, not generic platform |
| `frontend/diligence.html` | Modify later | Demote paid-report CTA behind Atlas |

## Feedback History

- 2026-05-20: User rejected report-first monetization. Static AI reports are
  expected to be free. New direction is viral cross-dataset exploration with
  telemetry that trains the platform.
