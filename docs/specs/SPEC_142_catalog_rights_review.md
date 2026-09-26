# SPEC_142 — Rights proposals with citations, human review workflow and report (PLAN_088 §3 "SPEC_135")

**Type:** service + api_endpoint
**Date:** 2026-09-26
**Plan:** `docs/plans/PLAN_088_catalog_improvements.md` (§1.7 rights findings, §2 target entry, §3 SPEC_135, §4 decisions 3/8)
**Evidence:** `docs/plans/PLAN_088_evidence.json` → `lenses["lens:rights"].data`, copied verbatim to
`app/catalog/evidence/rights_research_2026-09-25.json`.
**Depends on:** SPEC_141 (merged at 1769e81: `spec.py`, `rights.py`), SPEC_137 (`schema_live.storage_forbidden` placeholder).

## Why

`redistribution` can only gate export. It cannot say "the terms forbid holding this at all"
(Yelp, FRED as retrieved, Kaggle M5, Kalshi, NZA, PeeringDB, LoopNet) or "commercial use needs
an agreement" (AMA CPT in CMS utilization, IMF, CourtListener). Rights blocks carry no citation,
so nobody can check them, and `reviewed` is a hand-set boolean with no record of who reviewed
what. 0 of 150 specs are reviewed and nothing can become reviewed safely.

## User decisions applied (all reversible)

- Storage-forbidden data is **flagged only**; nothing is deleted or moved.
- Rights **tightenings** with a citation are applied in `rights.py`; **loosenings stay proposals**
  (`proposed`), shown in the queue and report, never applied.
- `reviewed` stays **False everywhere**. Nothing is ga/beta.
- Decision 8 (reviewer of record / what drives `reviewed`): **code truth plus a committed
  `REVIEWED` hash** (the plan's default). The DB table is the audit trail; it never flips anything.

## Design

### 1. Rights fields (`app/catalog/spec.py`, `app/catalog/rights.py`)

Closed vocabularies, validated at import on both `SourceRights` and `DatasetSpec`:

| Field | Vocabulary / rule |
|---|---|
| `storage` | `allowed` \| `time_limited` \| `forbidden`; `None` = not assessed |
| `storage_max_age_days` | positive int, set exactly when `storage == 'time_limited'` |
| `commercial_use` | `allowed` \| `restricted` \| `agreement_required` \| `forbidden`; `None` = not assessed |
| `share_alike` | bool (ODbL, CC BY-SA) |
| `license_url` | http(s) URL of the licence text |
| `citation_url` | http(s) URL of the terms the block rests on |
| `citation_quote` | the quoted terms; requires `citation_url` |
| `rights_confidence` | `high` \| `medium-high` \| `medium` \| `low-medium` \| `low`; requires `citation_url` |
| `rights_notes` | `SourceRights.notes` (was dropped by `to_dict`) |
| `proposed_rights` | a `RightsProposal`: a candidate block distinct from the effective one |

`RightsProposal(change, reason, citation_url, citation_quote, confidence, license?,
redistribution?, attribution?, storage?, storage_max_age_days?, commercial_use?, share_alike?)`
— `change ∈ {loosen, tighten, restate}`; citation, quote, confidence and reason are required;
unset fields mean "unchanged". `proposed_block(spec)` merges it over the current block.

`rights_hash` (sha256 of the canonical JSON of license, license_url, redistribution, attribution,
storage, storage_max_age_days, commercial_use, share_alike, pii_class, origin) identifies the
exact block a reviewer signed. Citations and notes are not part of it (they document, not decide).

ga/beta additionally requires `storage != 'forbidden'` and `commercial_use ∉ {forbidden,
agreement_required}`.

`to_dict()["rights"]` adds every field above plus `rights_hash` and `proposed`.

### 2. Tightenings applied in `rights.py` (each with `citation_url` / `citation_quote` / confidence)

| Entry | Change |
|---|---|
| `DATASET_RIGHTS['cms_medicare_utilization']` | open → **restricted**, `commercial_use=agreement_required`, licence/attribution "CPT © American Medical Association" |
| `DATASET_RIGHTS['intl_imf']` | attribution → **restricted**, `commercial_use=agreement_required` (email copyright@imf.org) |
| `DATASET_RIGHTS['intl_bis']` | stays attribution; `commercial_use=restricted` (no surcharge to subscribers, no implied endorsement) |
| `courtlistener` | attribution → **restricted**, `commercial_use=agreement_required` (FLP commercial agreement) |
| `yelp`, `medspa_discovery`, `vertical_discovery` | `storage=forbidden` (no storage beyond 24 h, no own database of listings), `commercial_use=restricted` |
| `fred` | `storage=forbidden` (no caching/archiving the FRED Services), `commercial_use=restricted`, mandated FRED non-endorsement attribution |
| `kaggle` (M5) | `storage=forbidden`, `commercial_use=forbidden` (non-commercial only) |
| `prediction_markets` (Kalshi) | `storage=forbidden` (no archived data sets), `commercial_use=forbidden` |
| `national_zoning_atlas` | `storage=forbidden` (no host/store), `commercial_use=forbidden`, NZA credit line |
| `peeringdb` | `storage=forbidden`, `commercial_use=forbidden` (no commercial application, no bulk pass-on) |
| `loopnet` | `storage=forbidden`, `commercial_use=forbidden` (scraping and database creation prohibited) |
| `foot_traffic` (Google Places) | `storage=time_limited`, `storage_max_age_days=30`, `commercial_use=restricted` |
| `afdc`, `COLLECTOR_RIGHTS['nrel_resource']` | licence "DOE/NREL open data (contractor-operated lab; free for any use with credit)" — not §105; open → **attribution** |
| `DATASET_RIGHTS['si_utility_rates']` | OpenEI block + EIA attribution/licence (36% of rows are EIA-sourced) |
| `DATASET_RIGHTS['si_zoning_districts']` | NZA block + note: 164 `nj_sussex_county_gis` rows are outside the NZA terms (not reviewed) |
| `freightos` | `commercial_use=restricted` (no resale, no derived indexes, no AI training), Freightos credit + link |
| mandated notices | OpenFEMA non-endorsement (fema, si fema/fema_nfhl), Census API non-endorsement (census, us_trade), OECD adaptation (`intl_oecd`), World Bank CC BY (`intl_worldbank`), Epoch citation, OSM ODbL (`share_alike=True`), NOAA WMO Res. 40 note |

Monotonicity is tested: every key's `redistribution` is at least as strict (mirror
`REDISTRIBUTION_RANK`) as the pre-SPEC_142 snapshot (`tests/fixtures/spec_142_rights_baseline.json`),
no `storage` / `commercial_use` is looser than before, and `LOOSENED_WITH_REVIEW` is empty.

### 3. Proposals only (never applied)

- `realestate_osm_buildings`: restricted → attribution, `share_alike=True` (ODbL allows commercial use).
- `fred_series`: re-source the 23 public-domain series from the originating agencies → open (an
  ingestion change); UMCSENT stays restricted.
- `dunl`: restricted → attribution if the CC variant is CC BY (low confidence).
- `intl_imf`: back to attribution once copyright@imf.org confirms commercial reuse.

### 4. Review workflow (`app/catalog/rights_review.py`, `app/catalog/rights_reviewed.py`)

- **Audit table** `catalog_rights_review` (Alembic `0015_catalog_rights_review`, down_revision
  `0014_dataset_status`; raw SQL like 0013, no ORM model): `id, dataset_key, decision ∈
  {confirm_current, accept_proposal, reject}, rights_hash, proposal_hash, rights_snapshot JSONB,
  proposal_snapshot JSONB, reviewer, reviewer_user_id, api_key_id, note, reviewed_at`. Append-only:
  a trigger refuses UPDATE and DELETE.
- **`POST /api/v1/catalog/rights/{key}/review`** (admin): body `{decision, rights_hash, note}`.
  `rights_hash` must equal the current block's hash (**409** otherwise: the reviewer saw a
  different block). `accept_proposal` needs a proposal (422) and records its hash. A note of
  ≥ 10 characters is required. The synthetic `REQUIRE_AUTH=false` principal cannot sign off
  (403: a sign-off needs a named reviewer). The row records reviewer (email / key name), user id,
  key id and timestamp. **It never changes `reviewed`.**
- **Effective `reviewed`** = `REVIEWED[key] == spec.rights_hash`, where `REVIEWED` is a checked-in
  dict in `app/catalog/rights_reviewed.py` (empty in this spec). `python -m
  app.catalog.rights_review --emit` prints the file content from the latest `confirm_current`
  decision per key whose hash still matches the code; a human reviews the diff and commits it.
  Any change to a rights field changes the hash and silently re-opens review. `accept_proposal`
  is an instruction to change `rights.py`; after that code change the new block needs its own
  `confirm_current`.
- **`GET /api/v1/catalog/rights/review`** (admin): the queue, sectioned: `proposals` (current vs
  proposed with a field diff, citation, confidence), `candidates` (the §1.7 public-domain /
  CC list, PE/entity pack first), `storage_flags` (storage-forbidden / time-limited /
  agreement-required datasets with live row counts), `pii_blocked` (public-domain but
  personal: waits on the PII policy), `awaiting_commit` (confirmed in the DB, not yet in
  `REVIEWED`), `stale` (a confirmation whose hash no longer matches). Each item carries the
  latest DB decision.
- **`GET /api/v1/catalog/rights/{key}`** (users): the dataset's current block, hash, proposal,
  gate and review history.

### 5. Report — `GET /api/v1/catalog/rights/report?format=json|md&live=true|false` (users)

Counts by redistribution / storage / commercial_use / effective; per source family, each
dataset's current vs proposed block with citations; storage-forbidden holdings with live row
counts (`live.dataset_live`, cached, under statement timeouts; `live=false` or a DB failure
gives `null` counts, never a 500); the gating effect (sample refused for non-admins and flagged
for admins, profile examples withheld, export flagged). `format=md` returns `text/markdown`.

### 6. Enforcement

- `schema_live.storage_forbidden(writers)`: a writer's `storage == 'forbidden'` or
  `commercial_use == 'forbidden'` (SPEC_137's placeholder, now real).
- `schema_live.rights_gate(writers)`: the reasons a table is gated — storage forbidden,
  commercial use forbidden or agreement-required (the last added here).
- `/catalog/{key}/sample`: gated → **403 for non-admins**; **admins get the rows with a
  `rights_gate` block and an `X-Dataset-Rights-Gate` header** (task instruction; this replaces
  SPEC_137 item 5, which refused admins too). `/schema` profile examples stay withheld from
  everyone on a storage-forbidden table (examples are cosmetic; `/sample` is the flagged path).
- `export_policy.catalog_rights_gate(table)` / `export_allowed(table, columns, admin)`: export
  of a gated table is refused for non-admins; the admin-only `/export` preview and table list
  carry `rights_gate` so the admin sees the flag.
- `mirror.merge_rights` carries the strictest `storage` / `commercial_use` and `share_alike`
  of a table's writers.

## Tests (`tests/test_spec_142_catalog_rights.py`)

- T1 vocabularies: bad storage / commercial_use / confidence / max-age / quote-without-url /
  proposal without citation fail at construction (SourceRights, DatasetSpec, RightsProposal).
- T2 tightenings applied (each table row above), each with a citation; evidence file = lens data.
- T3 monotonicity vs the baseline snapshot; `LOOSENED_WITH_REVIEW` empty.
- T4 proposals are loosenings/restatements only, never applied; every proposal has citation,
  quote, confidence.
- T5 `reviewed` False everywhere; `REVIEWED` empty; a matching hash flips a spec to reviewed, a
  changed rights field flips it back; ga/beta still requires reviewed and a non-forbidden gate.
- T6 `to_dict()["rights"]` carries the new fields, hash and proposal.
- T7 queue sections and ordering (PE pack first among candidates; §1.7 candidates present;
  personal-PII public-domain in `pii_blocked`; storage flags list Yelp, FRED, M5, Kalshi, NZA,
  PeeringDB, LoopNet, Google Places).
- T8 report renders offline (json + md) with null counts.
- T9 gate: sample 403 for non-admin, 200 + flag for admin; `export_allowed`.
- T10 migration 0015: revision chain, single head, idempotent, append-only trigger (PG).
- T11 POST review (PG): records actor + timestamp; stale hash → 409; unknown key → 404;
  accept_proposal without proposal → 422; short note → 422; non-admin → 403; local-dev → 403;
  `reviewed` unchanged after a sign-off; `--emit` output includes only matching confirmations.

## Live verification (2026-09-26, read-only on `nexdata-api-1`)

Exact `count(*)` of the gated holdings (flagged, nothing deleted):

| Dataset | Tables → rows |
|---|---|
| `yelp_businesses` | yelp_businesses 400 |
| `medspa_prospects` | medspa_prospects 5,396; zip_medspa_scores 27,604; medspa_prospect_snapshots 0 |
| `fred_series` | 6 base tables, 132,302 (interest_rates 105,283; commodities 17,516; …) |
| `kaggle_m5` | m5_items 30,490; m5_calendar 1,969 (m5_sales / m5_prices absent) |
| `prediction_markets` | prediction_markets 37; market_observations 87 |
| `si_zoning_districts` | zoning_district 3,695 (3,531 national_zoning_atlas + 164 nj_sussex_county_gis) |
| `si_internet_exchanges` | internet_exchange 216; data_center_facility 1,389 |
| `si_warehouse_listings` (LoopNet) | warehouse_listing **0** — settles the PLAN_088 60-vs-0 conflict: nothing to purge |
| `cms_medicare_utilization` | 40,054 (agreement_required; PLAN_088 saw 21,260 — BUG-CMS-IDEMPOTENT duplicates) |
| `courtlistener_dockets`, `intl_imf` | 0 rows |
| `vertical_prospects`, foot traffic | tables absent / 0 rows |

The report endpoint recomputes these live (cached 5 min, 30 s overall deadline).

## Decisions taken here

- **Admins can sample gated data, flagged** (task instruction), replacing SPEC_137 item 5
  ("refuse admins too"). `/schema` profile examples stay withheld from everyone when storage
  or commercial use is forbidden (examples are cosmetic).
- **`SourceRights.reviewed=True` is refused at import**: the only path to reviewed is a
  committed hash. `DatasetSpec(reviewed=True)` still constructs (tests), but `_ds()` builds
  every catalog spec with `reviewed=False` and `apply_review`.
- **`--emit --out`**, not `> file`: shell redirection would truncate `rights_reviewed.py`
  before the catalog imports it.
- **A named reviewer is required**: the `REQUIRE_AUTH=false` principal cannot sign off; API
  keys record `api-key:<name>` / owner email and the key id.
- **GJF not changed**: the plan's "GJF → internal_only" is a *loosening* under the mirror's
  rank (`restricted` is stricter than `internal_only`); it stays `restricted` with a
  low-medium citation and a provenance note.
- **`afdc` / `nrel_resource` open → attribution**: "free with credit" is attribution; a tightening.
- **Candidates** exclude personal-PII datasets (`pii_blocked`) and anything gated; the list
  is `CANDIDATE_KEYS` / `CANDIDATE_SOURCES` / `CANDIDATE_COLLECTORS` in `rights_review.py`.

## Out of scope

Deleting or moving storage-forbidden data (decision 3), re-sourcing FRED, the PII policy,
publishing anything (ga/beta), JSON-LD.
