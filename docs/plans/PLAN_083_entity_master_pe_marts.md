# PLAN_083: Phase 2 — entity master + PE firms/funds from SEC data

## Context

Phase 0 and Phase 1 are done and live on Cloud SQL: the fabricated rows are quarantined, and the SEC bulk data is loaded (985k EDGAR filers, 24k Form ADV advisers, 168k Form D filings including 49k PE/VC fund offerings, 3.8M 13F positions, 1.9M insider trades).

That data is still siloed. Nothing links an adviser (CRD) to its filings (CIK), and the PE tables the app actually reads are nearly empty: **102 firms** (0 with a CRD), **7 funds**, **0 fund investments**. The 2026-09-16 review called this out: the hand-typed 90-firm seed list is the reason PE coverage never grew.

This phase builds the join layer and uses it to populate the PE tables from SEC data.

**Decisions (from the user):**
- PE universe = **PE + VC** fund types (16,941 + 22,248 = ~39k fund vehicles).
- SEC-derived rows are **added into the existing `pe_firms` / `pe_funds`**, tagged by `data_source`, not replacing them.
- Entity master covers the **PE-relevant subset** (~200k organizations), not all 985k filers.

**Grounding facts measured on the live data:**
- 13F cover pages carry a CRD on 60,211 filings → **5,587 advisers bridged to a CIK by identifier**; a unique-name tier would add ~4,600 more (42.5% of advisers total).
- 12,097 advisers report private funds (127,028 fund entries).
- Every PE/VC Form D filing has related persons, but **44% of those "people" are actually entities**, and the high-frequency names are fund administrators (Sydecar, Belltower), not GPs.
- **Trap:** `sec_13f_filings.crd_number` is zero-padded (`000106326`), ADV CRDs are not (`8361`) — a naive join returns nothing. Roster is a 3-snapshot series, so always take the latest per CRD.
- `pe_firms.name` matches **zero** ADV or EDGAR names (brand names vs legal names), so linking must be by identifier, not by name.

---

## SPEC_116 — Entity master and CIK↔CRD bridge

Port the workbench resolver (`C:\Users\awron\projects\wildcard-workbench\ingest\`). It is deliberately **identifier-only**: a union-find over strong keys (EIN, CIK, CRD, LEI, UEI) with no fuzzy name matching, plus a veto table, merge/split history and an idempotent write path. That matches the review's "identifiers before names" rule.

**New `core` schema (alembic 0008):**
- `core.source_record` — one row per source record: `record_key` PK (e.g. `adv:8361`), source, name, name_norm, cik, crd, ein, lei, state, observed_at.
- `core.entity` — entity_id, entity_type, canonical_name, member_count, strong_key_count, resolver_version, `field_conflicts` jsonb, status/superseded_by.
- `core.identifier` — `(id_type, id_value_norm)` PK → entity_id, source, first/last_seen.
- `core.alias`, `core.key_veto`, `core.entity_merge`, `core.resolve_run` (ledger).
- `core.cik_crd_bridge` + `core.cik_crd_bridge_refused`.

**Port map (from the workbench):**
- `ingest/norm.py` → `app/entities/norm.py` **verbatim** (pure stdlib, self-testing): `norm`, `core`, `clean_ein`, `state2`, `zip5`, `phone10`, `domain`.
- `ingest/resolve.py` pure functions → `app/entities/resolve.py` **verbatim**: `_UF`, `_extract_keys`, `plan`, `canonical_row`, `assign_ids`, `parse_veto_target`, and its 67-check `_selftest`. Rewrite only the I/O (psycopg2 → SQLAlchemy, `workbench.` → `core.`, `%(name)s` → `:name`), keeping the `IS DISTINCT FROM` idempotency guards.
- `ingest/cik_crd_bridge.py` tiers → `app/entities/cik_crd_bridge.py`: tier 1 `cover_page_crd` and tier 2 `other_manager_crd` from **our** `sec_13f_filings`; tier 3 `name_state` kept but only for names unique in `sec_filers` (5,096 normalized names are ambiguous). Keep `crd_cik_count` and the refusal table; CIK↔CRD is many-to-many (92 CRDs → >1 CIK), never a FK.

**Feeders — `app/entities/feeds.py`** (PE-relevant subset only):

| Feed | record_key | Keys contributed |
|---|---|---|
| ADV roster, latest snapshot per CRD | `adv:<crd>` | crd, name, state |
| IAPD feed | `iapd:<crd>` | crd (status/freshness only) |
| 13F filings | `f13:<cik>` | **cik + crd together** — this is what merges adviser and filer without the bridge |
| Form D issuers | `formd:<cik>` | cik, name, state |
| EDGAR filers, restricted to CIKs referenced by the feeds above or by `sec_8k_index` / `sec_insider_owners` | `edgar:<cik>` | cik, ein, lei, state |

Normalization at the feed boundary: strip CRD zero-padding, drop all-zero placeholders, keep CIK 10-digit padded, case-fold names.

**Execution:** a `entity_resolve` worker job type + executor (same pattern as `bulk_ingest`), `POST /api/v1/entities/master/resolve`, `GET /api/v1/entities/master/stats`, and a lookup `GET /api/v1/entities/master/by-id/{id_type}/{value}`.

**Note:** `app/core/entity_resolver.py` + `/api/v1/entities` (a separate fuzzy-matching implementation over `canonical_entities`, 4 rows) stays untouched for now. Deleting it is a separate cleanup; it is live-wired in `app/main.py`.

---

## SPEC_117 — PE firms and funds from ADV + Form D

Additive population of the existing tables, keyed by identifier, fully reversible.

**Schema prep (alembic 0009):**
- Partial unique indexes `pe_firms(crd_number) WHERE crd_number IS NOT NULL` and `pe_firms(cik) WHERE cik IS NOT NULL` — needed for upserts and currently absent (the live table has no unique constraint at all, not even on name).
- `pe_funds.cik` column + partial unique index (the Form D fund vehicle's CIK is the natural key).
- **Make `pe_funds.firm_id` nullable.** Most Form D funds cannot be attributed to a GP with confidence, and today's NOT NULL would force either dropping them or inventing a fake parent firm.

**Firms** — from the latest ADV roster snapshot per CRD, advisers reporting ≥1 private fund (12,097):
`name`, `legal_name`, `crd_number`, `sec_file_number` (801-/802-), `cik` (from the bridge when available), `is_sec_registered`, hq city/state/country, `website`, `aum_usd_millions`, `status`, `data_source='SEC ADV'`.
`primary_strategy` is set to Private Equity / Venture Capital only when the firm's linked funds agree; otherwise left NULL. Crawler-only fields (sector_focus, check sizes, LinkedIn, confidence_score) stay NULL by design.

**Funds** — from `form_d_offerings` where `investment_fund_type IN ('Private Equity Fund','Venture Capital Fund')`, deduped to one row per issuer CIK using the latest filing (65k of 168k offerings are amendments; ignoring that would multiply-count):
`name` (issuer entity name), `cik`, `vintage_year` (year of first sale), `target_size_usd_millions` (total offering amount), `final_close_usd_millions` (total amount sold), `first_close_date`, `data_source='SEC Form D'`.

**Firm ↔ fund links**, recorded with the tier that produced them:
1. **Identifier:** fund issuer CIK and adviser CRD resolve to the same entity in the master.
2. **Name-core:** fund legal name starts with the adviser's normalized name core (e.g. "GENSTAR CAPITAL PARTNERS X, L.P." → "GENSTAR CAPITAL") and the match is unique.
3. Otherwise `firm_id` stays NULL.

Coverage of tiers 1 and 2 is unknown until it runs. **I will measure it and report the number before writing any rows**, since a low yield changes whether the fund rows are worth attributing at all.

**Reversibility:** add quarantine rules for `data_source IN ('SEC ADV','SEC Form D')` so this can be undone with the existing tooling.

---

## SPEC_118 — GP people (only if SPEC_117's link yield is decent)

`form_d_related_persons` → `pe_people` + `pe_firm_people`, after filtering: drop entity-suffix names (44% of them), drop the administrator frequency tail (312 names appear on 100+ filings), drop `N/A` first names. Link people to firms only through funds whose `firm_id` is known. Also load ADV `key_personnel`.

---

## Order and verification

1. **SPEC_116** — port norm + resolver (its self-test must pass unchanged), feeds, bridge, migration 0008, worker job. Unit tests on a disposable Postgres, then run against Cloud SQL.
   - Expect: ~200k entities; **≥5,500 CIK↔CRD bridges** at tier 1 (today's workbench bridge has 2,292 and is a year stale); a re-run reports 0 changes (the resolver's idempotency guard).
   - Spot-check: Carlyle, Blackstone and KKR each resolve to one entity carrying both CIK and CRD.
2. **SPEC_117** — migration 0009, build firms then funds, measure link tiers, report, then load.
   - Expect: `pe_firms` 102 → ~12k (existing 102 untouched, verified by row-level diff); `pe_funds` 7 → ~39k.
   - `GET /pe/firms/stats/overview`, `/pe/firms/`, `/pe/firms/{id}` and the fund tearsheet report still return 200 with the new rows present.
   - Re-running the build changes nothing (skip-unchanged merge).
3. **SPEC_118** — only after reviewing the link yield.

Each spec: `/spec` first, tests before code, run in the API image against a disposable Postgres (`TEST_PG_URL`), then apply on Cloud SQL through the worker queue. Database growth is expected to be a few hundred MB, well inside the lean budget (currently 6.9 GB of a 10 GB disk).

**Out of scope / unchanged:** `pe_fund_investments` stays empty — Form D is fundraising, not ownership, and 13F is public equity. The endpoints that depend on it (`/pe/firms/{id}/portfolio`, exit decisions, quarterly diff) already return nothing today; filling them needs a different source (BDC schedules, CMS ownership) and is Phase 3.
