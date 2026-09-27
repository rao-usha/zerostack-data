# SPEC_145 — Catalog browser page, search/facets API, JSON-LD mapper (PLAN_088 §3 "SPEC_139", §1.9)

**Type:** api_endpoint + frontend
**Date:** 2026-09-26
**Plan:** `docs/plans/PLAN_088_catalog_improvements.md` (§1.9 discovery and schema, §2 target entry, §3 SPEC_139)
**Builds on:** SPEC_123 (catalog), SPEC_124 (`/datasets/status`), SPEC_125 (status page conventions),
SPEC_137 (dictionary, `/schema`, masked `/sample`, `/joins`), SPEC_141 (truth fields),
SPEC_142 (rights: storage / commercial_use / gate, review state), SPEC_143 (lineage),
SPEC_144 (quality + usage blocks)
**Test file:** `tests/test_spec_145_catalog_browser.py` (+ `tests/js/catalog_page_smoke.js`)

## Problem

No page renders the catalog. `GET /catalog?q=` is a substring match over key, name and
description only: it cannot find a dataset by a column (`cik`), does not rank, and gives
no facet counts. There is no machine-readable (schema.org / DCAT) description of a dataset.

## Scope (this spec)

1. `GET /api/v1/catalog/search` — ranked full-text search with facet counts.
2. `GET /api/v1/catalog/{key}/jsonld` — schema.org `Dataset` (+ a few DCAT terms), rights-gated.
3. `frontend/catalog.html` — static browser page (search, facets, cards, detail tabs).
4. Nav links from `index.html` and `status.html`.

Out of scope (PLAN_088 SPEC_139 items not requested here): `response_model`s on the
existing catalog routes, `GET /catalog/facets` as a separate route (facets come with
search), `live=estimate|exact` on the detail route, the PE/entity data product page
(`/catalog/products`), public (unauthenticated) JSON-LD emission.

## Design

### Search index — `app/catalog/search.py` (new, pure, in-memory)

Built once per process (`lru_cache`) from `get_catalog()` and `columns.generated.json`
(the committed dictionary; `dataset_tables_offline`). No database access to build or query.

Fields and weights (PLAN_088 §3): key / display_name **5**, tables **3**, column names **2**,
other text **1** (subtitle, description, grain, keywords, source, limitations, column
descriptions). Tokens: lower-case alphanumeric runs (underscores split: `sec_form_d` gives
`sec`, `form`, `d`; the whole key is kept as a token too).

Query: tokenised, **AND** across query tokens, each token matched exactly or as a prefix of an
indexed token (a prefix hit scores half). A dataset's score is the sum over query tokens of the
best weight that token reached, plus a phrase bonus (+5) when the whole query appears in the
display name or key. Ties break on name. Results carry `matched` (field names that matched)
and `highlight` (a short plain-text snippet of the best matching text field; the page escapes it).

Filters (all optional; comma-separated values are OR within a facet, AND across facets):
`kind`, `source`, `status_public`, `data_state` (`unverified` = no verified state),
`redistribution` (declared), `effective_redistribution`, `pii_class`, `origin`,
`identifier` (join-key semantic types present in the dataset's columns), `keyword`.
Values in closed vocabularies are validated (422).

Facets are **disjunctive**: the counts for facet X are taken over the text matches filtered by
every *other* facet, so selecting `kind=filings` still shows the other kinds' counts. For a
single-valued facet with no filter of its own, the counts sum to `count`.

Row estimate: no count on search. `row_estimate` is the SPEC_123 live cache's total when it
holds the dataset (exact counts from an earlier detail call), else the sum of the planner
estimates from `quality.relations(cached=True)` (one `pg_class` read shared for 60 s); a
row-filtered (shared) table makes the estimate unknown. On any failure: `null`.

### JSON-LD — `app/catalog/jsonld.py` (new, pure) + route

`to_jsonld(spec, base_url=...)` maps a spec to schema.org `Dataset`:
`@id`, `identifier`, `name`, `alternateName` (subtitle), `description`, `keywords`,
`creator` / `publisher` (source family / Nexdata), `temporalCoverage`
(`coverage_from/..`), `spatialCoverage` (`US:state` → "United States (state level)"),
`variableMeasured` (dictionary columns: `PropertyValue` with name, description, unitText,
propertyID = semantic type; capped at 250), `distribution` (`DataDownload` per catalog
API endpoint), `isAccessibleForFree: false`, `conditionsOfAccess` (from the rights block),
`license`, `usageInfo` (citation URL), `dct:accrualPeriodicity` (cadence),
`dateModified` (verified_at). `@context` = schema.org with `dcat` / `dct` prefixes.
DCAT-US federal fields (bureauCode, programCode) are skipped (PLAN_088).

Rights rules:
- a dataset with a non-empty `rights_gate` (storage forbidden, commercial use forbidden,
  agreement required) or `status_public == 'retired'` → `JsonLdRefused` → **403** with the reasons;
- `license` is the upstream licence **only** when the rights are reviewed and the effective
  redistribution is `open` or `attribution`; otherwise a `CreativeWork` naming the restriction
  ("Internal use only (rights review pending)" / "Restricted"). Restricted or unreviewed rights
  are never emitted as open;
- `X-JsonLd-Publishable: true` only for a reviewed ga/beta dataset; the route is user-level,
  never public;
- `to_jsonld` raises `ValueError` on a description under 50 characters (the Gold gate).

### Page — `frontend/catalog.html`

Static, no build step; `/js/auth.js` first; palette and classes copied from `status.html`.
A `<script id="catalog-core">` block holds pure helpers (`esc`, `parseHash`/`searchHash`,
`flagReasons`, `freshness`, `fmtRows`, `sparkPoints`, `curlFor`) tested under node. The render
script builds every piece of DB/catalog text through `esc()` — enforced by a test: every
`${...}` interpolation in the page script is `esc(...)`, a numeric formatter, or a vetted helper
that returns escaped HTML.

- Hash routes (nginx `try_files` swallows paths): `#/` search with query and facets in the hash
  (`#/?q=..&kind=..`), `#/dataset/<key>[/<tab>]`.
- Search view: search box, facet sidebar (kind, source, data_state, redistribution, effective
  rights, pii, status_public, identifier), result cards (name, subtitle, kind, data_state badge,
  rights badge, coverage range, freshness from `/datasets/status`, `~rows`).
- Detail view tabs: Overview, Schema, Sample, Lineage, Quality, Rights, Access.
- Warning banner whenever `data_state` is not `ok`, the rights gate is set, or the dataset is
  flagged fabricated / seeded / sample_mixed / demo / placeholder.
- Responsive: facet rail stacks above the results under 900 px.

## Tests (`tests/test_spec_145_catalog_browser.py`)

- Search: `q="form d"` → `sec_form_d` first; `q="cik"` hits via `columns`; AND semantics; prefix;
  empty query returns all with facets; filters and comma-OR; 422 on bad vocab; disjunctive
  facet sums; `unverified`; row estimate from the live cache / relation estimates / null for
  row-filtered tables, and no `count(*)` issued.
- Auth: route mounted with `require_admin_for_writes` in `app.main`; 401 without credentials
  when `REQUIRE_AUTH` is on; 200 for a user principal.
- JSON-LD: required fields; temporalCoverage; variableMeasured; distribution;
  `isAccessibleForFree` false; every gated spec → 403; unreviewed open dataset not emitted as
  open; reviewed open fixture is; short description raises.
- Page (static): auth.js first; every `/api/v1/...` path exists as a GET route; every
  interpolation escaped or vetted; banner logic present; hash routes; nav links.
- Node: `catalog-core` helpers.
- jsdom (when available): detail page with malicious description / name / limitation / column
  text — no script executes, text shows escaped, banner shows, every tab renders.
