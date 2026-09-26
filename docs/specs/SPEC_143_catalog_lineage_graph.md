# SPEC_143 — Catalog lineage graph (PLAN_088 §3 "SPEC_136", §1.8)

**Type:** service + api_endpoint
**Date:** 2026-09-26
**Plan:** `docs/plans/PLAN_088_catalog_improvements.md` (§1.8 lineage, §3 SPEC_136)
**Evidence:** `docs/plans/PLAN_088_evidence.json` `lens:lineage-quality`
**Builds on:** SPEC_123 (catalog), SPEC_124 (`job_keys`), SPEC_126a (mart ledger,
`app/marts/inputs.py`), SPEC_141 (truth pass), SPEC_144 (`usage.json`,
`view_dependencies`, `dataset_key` backfill)
**Test file:** `tests/test_spec_143_catalog_lineage_graph.py`

## Problem (measured 2026-09-25/26, read-only)

- `app/core/lineage_service.py` (693 lines) and the `/lineage` router: no caller
  ever wrote to them (`lineage_nodes` 2 rows, `lineage_edges` 1, a hand test of
  2026-01-14). No frontend reads `/lineage`.
- Three declarations of mart inputs disagreed: `DatasetSpec.inputs`,
  `app/marts/inputs.py` stage maps and the SQL each stage runs.
  `pe_firms_sec` declared `sec_iapd_feed` (never read); `pe_funds_sec` read
  `sec_adv_filings` and `sec_adv_roster_snapshots` undeclared; `pe_people_sec`
  read `sec_adv_roster_snapshots` undeclared; the bridge declared
  `entity_source_records` but reads the 13F, roster and `sec_filers` tables.
- `core.mart_build` has 0 rows (see "Why the ledger is empty").
- 322 `ingestion_jobs` rows (+1 ambiguous) had no resolvable `dataset_key`.

## Design

### `app/catalog/lineage.py` (new) — computed on read, cached 60 s, no tables

Nodes `dataset:`, `producer:`, `source:` (publisher family of a dataset built
from no other dataset), `table:` (`pattern: true` for globs), `view:`,
`consumer:` (opt-in, from `usage.json`).

Edges: `declared` (spec.inputs), `sql` (FROM/JOIN tables in the producer's SQL,
`PRODUCER_MODULES`), `stage_input` (the maps in `app/marts/inputs.py`),
`observed` (latest successful non-dry-run `core.mart_build` per mart: the input
records' release keys, matched to the mart datasets that declare that input),
`produces`, `publishes`, `stores`, `view` (CREATE VIEW code in `VIEW_MODULES`,
plus live `pg_depend` via SPEC_144's `view_dependencies`), `consumes`.

`drift` reconciles them: `declared_not_read`, `read_not_declared`,
`ambiguous_table`, `stage_map_mismatch`, `stage_without_dataset`,
`observed_not_declared`, `undeclared_view_input` (today: `sec_company_metadata`
under `public_company_financials`).

The SQL scan (`scan_source`) parses the module AST: string literals, f-strings
and `.format` templates with placeholders filled from module constants, plus
module constants that hold a relation name (`FILINGS = "public.sec_adv_filings"`).
Names are kept only when a spec owns them (declared table, else a pattern), so
CTE names and prose drop out.

`walk(graph, key, direction=up|down|both, depth)` follows the dataset-level edges
(`declared`, `sql`, `stage_input`, `observed`) and returns the dataset's
producers, tables and the views over them (views over views too) as a subgraph.
`find_cycles` checks the dataset graph is a DAG.

### Inputs match the SQL (datasets.py, inputs only)

| Spec | inputs |
|---|---|
| `pe_firms_sec` | `sec_adv_roster, entity_master` (drop `sec_iapd_feed`) |
| `pe_funds_sec` | `sec_form_d, sec_adv_private_funds, sec_adv_schedule_d, sec_adv_roster, pe_firms_sec` |
| `pe_people_sec` | `sec_form_d, sec_adv_roster, pe_firms_sec, pe_funds_sec` |
| `entity_cik_crd_bridge` | `sec_13f, sec_adv_roster, sec_edgar_submissions` (the SQL decides) |

The evidence dispositions for these four are `amended:SPEC_143 ...`.
`SQL_UNCHECKED` (medspa / vertical / rollup: Python + external APIs) and
`NO_CATALOG_INPUTS` (people / pe / lp / fo collection: web and LLM) explain the
derived/entity specs the scan cannot check.

### `app/marts/inputs.py` derived from the catalog

`PE_MART_STAGE_INPUTS`, `PE_MART_STAGE_UPSTREAM`, `ENTITY_STAGE_INPUTS` and the
new `ENTITY_STAGE_UPSTREAM` come from `lineage.stage_inputs(job_type)`: the
stage spec's inputs produced by `bulk:<name>`, and inputs built by another mart
job (`entity_master` -> `entity_resolve`). Same-job inputs are stages of the same
build. Behaviour change: the funds stage now also asserts `sec_adv_schedule_d`
(it runs with `adv_private_funds` anyway) and the bridge stage asserts
`sec_edgar_submissions` (the feeds stage already did).

### API (`app/api/v1/catalog_lineage.py`, user-level)

- `GET /api/v1/catalog/lineage?live=true&consumers=false` — nodes, edges, drift,
  observed (ledger availability and builds), counts.
- `GET /api/v1/catalog/{key}/lineage?direction=both&depth=3&live=true` — upstream,
  downstream (key, depth, via edge types), producers, tables, views, subgraph,
  drift for the reached datasets. 404 unknown key; 422 bad direction or depth
  outside 1..10.

Included before `catalog.router` in `main.py` (`/catalog/lineage` vs `/catalog/{key}`).

### Legacy lineage retired

`app/core/lineage_service.py` and `app/api/v1/lineage.py` deleted; the router,
its import and its OpenAPI tag are removed from `main.py`; README updated. The
five tables (`lineage_nodes`, `lineage_edges`, `lineage_events`,
`dataset_versions`, `impact_analysis`) stay, with their models, unread (user
decision: stop serving, leave the tables; no migration this spec).

### `dataset_key` aliases (`app/catalog/job_keys.py`)

After the producer lookup (a multi-dataset producer such as `job:pe_mart_build`
stays NULL and is never aliased):

1. `api:<source>` when a spec declares that router producer (`form_d`,
   `form_adv`, `app_rankings`, `web_traffic`, `opencorporates`, `epa_echo`,
   `github`, `census_cbp`);
2. `JOB_SOURCE_DATASETS`: `job_postings`, `job_postings_skills` -> `job_postings`;
   `usda` -> `usda_nass`; `international_econ_{oecd,worldbank,bis,imf}` -> `intl_*`;
3. `config["tables"]`: the one dataset that declares every named table
   (backfilled "synthetic record" rows).

Live, read-only simulation over the 3,719 NULL rows (2026-09-26): **3,676
resolvable** (was 3,396): 280 of the 323 previously unresolved rows. 43 stay
NULL: routers with no catalog dataset (`census_bfs` 14, `epa_ghg` 6,
`dot_grants` 6, `cms_hospitals` 4, `ffiec_banks` 4, `ferc_energy` 1,
`google_trends` 1), rows naming a shared table (`freight_index`,
`pe_collection`, `pe_fund_data`, `pe_people`), `news_items` (no spec),
`test_manual` and `job:pe_mart_build`.

## Why the ledger is empty

`core.mart_build` exists (0013) and `run_guarded` writes it, but no guarded build
has run since SPEC_126a merged (2026-09-23): the last `pe_mart_build` success
was 2026-09-22 15:26 and the last `entity_resolve` 2026-09-18 (live `job_queue`,
read-only). Nothing to fix in code; the next scheduled or manual build records
the first row, and `observed` edges appear then. The graph reports
`observed.available=true` with a note until then.

## Tests (`tests/test_spec_143_catalog_lineage_graph.py`)

- SQL-reference: derived/entity specs declare inputs (or are explained), input
  keys exist, inputs equal the scanned SQL reads with no ambiguity, plan
  corrections, evidence dispositions, scanner handles constants / f-strings /
  `.format`.
- Stage maps equal `stage_inputs`, explicit derived values, stages match the
  executors, every asserted source has a max age, a hand-edited map is drift.
- Graph: shape, `downstream(sec_form_d) ⊇ {pe_funds_sec, pe_people_sec,
  entity_source_records}`, depth limit, upstream of people, no cycles, view
  edges from code and the undeclared `sec_company_metadata`, observed edges from
  a fake ledger, non-PG degrade, consumers opt-in.
- API: user-level GETs, 404/422, not swallowed by `/catalog/{key}`.
- Legacy: files gone, `main.py` registration, no `/api/v1/lineage*` route, tables
  still modelled.
- Aliases: the 323-row fixture resolves ≥ 280 with exactly the expected leftovers;
  per-source resolutions; ambiguous stays NULL; alias table valid.
- PG: `pg_depend` view edge and observed edges from a real `core.mart_build` row.

## Out of scope / follow-ups

- Applying the backfill on live (writes are out of bounds here): run
  `python -m app.catalog.backfill_dataset_key --apply`.
- Catalog specs for the uncatalogued routers above (a truth-pass change).
- Quarantining or dropping the five legacy tables (a later migration).
- Impact analysis joined to the SPEC_124 verdicts (the walk carries each node's
  `status_public` / `data_state`; verdicts are not joined).
