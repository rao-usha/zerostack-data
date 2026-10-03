# SPEC 154 — LEI and UEI identifiers: GLEIF CC0 records, USAspending UEIs, EDGAR LEIs into the gated resolver

**Status:** Active (LIVE 2026-10-03: GLEIF run 2 complete; entity_resolve job 3153)
**Task type:** collector
**Date:** 2026-10-03
**Plan:** `docs/plans/PLAN_098_gleif_lei_identifiers.md`
**Test file:** tests/test_spec_154_gleif_lei_identifiers.py

## Why

The owner asked (2026-10-03) for "the public data S&P are creating" -- reported as an S&P
equivalent of the DUNS number. The verified answer: S&P made its existing Capital IQ company id
(CIQ ID) free to look up on DUNL.org (2025-09-11; ~25.3M rows: id, legal name, LEI, no address /
industry / size / parent), and its own licence statements conflict and do not clearly allow
commercial bulk use:

- dunl.org footer / landing page: CC BY-NC-SA 4.0, "By using this site, you confirm your acceptance";
- `company.parquet-metadata.json` (fetched 2026-10-03): `dc:license` CC BY-SA 4.0;
- the press release: "Creative Commons licensing enables free internal organizational use";
- the S&P Terms of Use the footer links returned an error page (unread).

So the S&P file is NOT loaded. The verified decision's recommended build is:
(1) promote the identifiers NexData already holds (EDGAR `sec_filers.lei`, USAspending
`recipient_uei`) into the resolver; (2) a GLEIF LEI connector (CC0); (3) record the DUNL licence
conflict in the rights catalogue and close the "loosen if CC BY" proposal; (4) the owner emails
dunl@spglobal.com. The owner's request is treated as the exception to the workbench's Sep 23
"NexData-only data" rule for this one CC0 source (reversible: drop `gleif_*`, delete
`core.source_record` rows of source `gleif`, re-run the resolver).

## Scope

1. **Rights.** `rights.py`: new `gleif` entry (CC0 1.0, open, commercial use + storage allowed,
   cited GLEIF terms quote). `dunl` stays `restricted`; notes record the three conflicting licence
   statements; the loosen proposal is removed (neither licence S&P names is CC BY); share_alike,
   commercial_use restricted, confidence high.
2. **Fetch terms registry** (`app/entities/data/site_terms.json`, the `open_web` registry):
   `gleif.org` allowed (CC0 citation); `dunl.org` and `spglobal.com` refused (licence).
3. **Connector** `app/sources/gleif/`: the GLEIF API (`api.gleif.org/api/v1/lei-records`),
   `filter[entity.legalAddress.country]=US`, cursor pagination (`page[cursor]=*`, then the
   `links.next` cursor), sparse fieldset `fields[lei-records]=lei,entity,registration` (drops the
   `spglobal`, `ocid`, `bic` ... mapping fields: never requested, never stored), page size 200.
   Every request goes through `open_web.OpenWebFetcher`: terms review, robots.txt (api.gleif.org
   allows all; goldencopy.gleif.org/robots.txt answers 403 = disallow-all, so the Golden Copy
   bulk zip is NOT used), honest NexdataResearch User-Agent, >= 2 s per request (GLEIF's API
   allows 60/min), Retry-After honoured (wait, then retry the same page, <= 3 tries), exponential
   backoff with jitter on transport / 5xx errors (<= 3 tries). Dry run by default: fetches, parses
   and counts, writes nothing. `--apply` upserts.
   - `gleif_lei_record` (PK `lei`): legal name, legal + HQ address (city, region, postal,
     country), jurisdiction, registration authority id + entity id (`registered_at` /
     `registered_as`), legal form id, category, entity status, registration status, initial /
     last update / next renewal dates, managing LOU, corroboration level,
     `golden_copy_publish_date` of the run that last saw it, `last_seen_run_id`.
   - `gleif_fetch` (append-only run ledger): filter, pages, records, requests, outcome
     (`complete` | `partial` | `refused` | `error`), golden copy publish date, user agent, terms
     citation, error.
   - **Declared clock** (catalog `gleif_lei_records.coverage_sql`): `max(golden_copy_publish_date)`
     of `complete` runs. A partial run never moves the clock.
   - Level 2 (parent) relationships are NOT loaded: the API has no bulk relationship endpoint
     (one call per child LEI) and the Golden Copy relationship file sits behind the 403 robots.
4. **Resolver** (SPEC_150 rules):
   - feeds: `edgar` records carry `sec_filers.lei`; new `gleif` feed (one record per LEI, state =
     the US jurisdiction state, else the legal-address state; registration status DUPLICATE and
     ANNULLED are not fed: they are invalid LEIs); new `usasp` feed (one record per distinct
     recipient UEI, latest name, no state: place of performance is not the recipient's address).
   - `resolve_core.GATED_KEY_TYPES` gains `lei` and `uei`: a shared LEI / UEI joins two different
     CIK groups only when `gate.corroborate` holds for the pair (MEASURED 2026-10-03: 6 LEIs sit on
     more than one `sec_filers` CIK; ungated they would have fused those CIKs). Records with no CIK
     (GLEIF, USAspending) attach by rule 5 and own a contested key by rule 6, exactly as ADV / Form
     5500 records do for EIN / CRD. Nothing merges on a name.
   - weak tier: GLEIF records get name+state candidates (`core.weak_match`, review only), with
     `lei_conflict` when the candidate identity carries LEIs and none is the record's. Weak-source
     records (dol5500, gleif) are never candidates in each other's call, so the dol5500 rows are
     unchanged.
   - metrics: `gleif` and `usasp` source metrics in the run ledger.
5. **Catalog**: dataset `gleif_lei_records` (source `gleif`, dispatch `gleif`), dispatch key `gleif`
   in `jobs.py`, `entity_source_records` inputs gain `gleif_lei_records` and `usaspending_awards`.

## Acceptance Criteria

- [x] `gleif` rights entry CC0 / open / allowed with the GLEIF quote; `dunl` restricted with the
      verified conflict, no proposal.
- [x] Terms registry: gleif.org allowed; dunl.org, spglobal.com refused; the registry still parses.
- [x] Parser: a GLEIF API record -> one row; `spglobal` / `ocid` never stored; "US-DE" -> "DE".
- [x] Fetch loop: robots read first, honest UA, cursor followed with the sparse fieldset kept,
      Retry-After waited then retried, stops on no `next`; a robots-disallowed host gets 0 page
      requests; a partial run is `partial` and does not claim a clock.
- [x] Dry run writes nothing.
- [x] Feeds: edgar carries LEI; gleif feed rows carry LEI + state, DUPLICATE / ANNULLED excluded;
      usasp rows carry UEI.
- [x] Gate: two CIKs sharing only an LEI with unrelated names stay split (refused, recorded);
      with equal names they join (R1); a GLEIF record attaches to the one CIK class holding its
      LEI; two no-CIK records sharing an LEI join; a UEI-only record stays a keyed singleton;
      a contested LEI has one owner.
- [x] Weak tier: GLEIF name+state candidates with `lei_conflict`; dol5500 rows identical with and
      without GLEIF records present.
- [x] Catalog / dispatch registered; SPEC_143 lineage SQL test passes.
- [x] ruff clean; SPEC_147/150 suites still pass.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_gleif_rights_entry | CC0, open, storage/commercial allowed, quote cited |
| T2 | test_dunl_rights_conflict_recorded | restricted, share_alike, no proposal, both licences named |
| T3 | test_terms_registry_entries | gleif.org allowed; dunl.org / spglobal.com refused; parses |
| T4 | test_parse_record | record -> row; mapping fields dropped; region -> state |
| T5 | test_page_urls_keep_fields | first URL + next-cursor URL keep filter, fields, size |
| T6 | test_fetch_loop_follows_cursor | robots first, UA, 2 pages then stop, requests counted |
| T7 | test_fetch_retry_after_respected | 429 Retry-After: 7 -> waits 7 s, retries, completes |
| T8 | test_fetch_robots_disallowed_no_pages | robots disallow -> outcome refused, 0 page requests |
| T9 | test_partial_run_keeps_clock | page error after retries -> partial, no publish date claimed |
| T10 | test_dry_run_writes_nothing | apply=False -> no write executed |
| T11 | test_feed_rows_lei_uei | gleif / usasp / edgar rows carry lei / uei / state |
| T12 | test_feed_sql_excludes_invalid | gleif feed SQL excludes DUPLICATE and ANNULLED |
| T13 | test_lei_shared_by_two_ciks_gated | unrelated names split + refused; equal names R1 join |
| T14 | test_gleif_record_attaches_to_cik | rule 5 only_class; component carries the LEI |
| T15 | test_no_cik_records_share_lei_join | GLEIF + another no-CIK LEI record join freely |
| T16 | test_uei_only_singleton | a usasp record with a UEI nobody else holds materializes nothing |
| T17 | test_contested_lei_single_owner | LEI on two pieces -> one owner, no duplicate identifier |
| T18 | test_weak_gleif_candidates | candidate / lei_conflict; dol5500 rows unchanged |
| T19 | test_catalog_and_dispatch | dataset, dispatch key, entity inputs |
| T20 | test_lei_checksum_enforced | ISO 17442 MOD 97-10 check digits; bad values -> no key (review) |
| T21 | test_gleif_attach_needs_name_when_lei_is_the_only_link | LEI-only rule-5 attach needs a name match; else own piece, LEI withheld from the filer (review) |
| T22 | test_gleif_attach_on_former_name | former EDGAR name attaches; gated_ciks loads that profile (review) |
| T23 | test_gleif_attach_on_squashed_name | names equal once spacing/punctuation is removed attach (review) |

## Rubric Checklist (no collector rubric file exists; generic)

- [x] Tests written and watched fail before source code
- [x] Public data only, robots + terms gate, honest UA, Retry-After, pacing
- [x] Parameterized SQL; idempotent upserts; dry run writes nothing
- [x] Nothing dropped silently (counts reported)
- [x] ruff clean

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/sources/gleif/{__init__,client,ingest}.py | Create | parser, URL builder, fetch loop, upsert, CLI, job entry |
| alembic/versions/0019_gleif.py | Create | gleif_lei_record, gleif_fetch |
| app/catalog/rights.py | Modify | gleif entry; dunl conflict recorded, proposal closed |
| app/entities/data/site_terms.json | Modify | gleif.org allowed; dunl.org, spglobal.com refused |
| app/catalog/datasets.py | Modify | gleif dataset, domains, entity inputs / limitations |
| app/api/v1/jobs.py | Modify | dispatch key gleif |
| app/entities/feeds.py | Modify | edgar lei; gleif + usasp feeds |
| app/entities/resolve_core.py | Modify | GATED_KEY_TYPES += lei, uei; weak tier lei_conflict / exclusions |
| app/entities/resolve.py | Modify | gleif weak call, gleif / usasp metrics |
| tests/test_spec_154_gleif_lei_identifiers.py | Create | T1-T19 |

## Owner calls left open

- Email dunl@spglobal.com: which licence governs company.parquet, and is commercial internal use
  in a PE sourcing tool allowed? Until answered: no CIQ id type, no DUNL company fetch.
- GLEIF Level 2 parents: per-LEI API calls (one per child) or a robots exception for
  goldencopy.gleif.org (403 today). Not done.
- Whether GLEIF's CC0 covers the S&P CIQ-to-LEI pairs (`spglobal` field): unconfirmed; not fetched.

## Results (2026-10-03)

- Tests: 20 in tests/test_spec_154_gleif_lei_identifiers.py (T1-T19 written first: 17 failed, T15 / T16
  passed already as regression guards; T10b, the batched upsert, written with its fix). Related
  suites 547 passed; the only failures import app.main and need `lightgbm` (absent in the image,
  pre-existing). ruff clean on every touched file except 5 pre-existing findings in jobs.py.
- GLEIF: dry run 2 pages (400 records, 3 requests). LIVE run 1 stopped by the operator (row-by-row
  upsert ~10 s/page through the proxy; ledger row 1 = partial, 1,400 rows) -> batched upsert ->
  run 2 complete: 362,284 records, 1,813 pages, 1,814 requests (robots + pages), 0 retries,
  golden copy 2026-10-03T08:00Z. By registration status: LAPSED 204,643, ISSUED 136,935, RETIRED
  19,197, DUPLICATE 1,438, ANNULLED 58, PENDING_ARCHIVAL 10, PENDING_TRANSFER 3.
- Resolver: dry runs job_queue 3151 / 3152 (mart_build 18 / 19) identical stage counts; LIVE 3153
  (mart_build 20) planned exactly what the dry runs did. Fed: gleif 360,788, usasp 2,622, edgar LEIs
  on 470 records (lei assertions 361,258). Entities 156,656 -> 156,672 (+16 new, 0 merged, 0 split,
  0 dissolved); core.identifier lei 0 -> 446 (GLEIF records joined 168 EDGAR components by LEI);
  uei 0 (no other record carries a UEI: 2,622 keyed singletons). Gate: +4 LEI pairs considered, all
  refused no_corroboration; 2 contested LEIs, no owner. Weak tier: 19,189 GLEIF name+state rows
  (15,305 GLEIF records with a candidate: candidate 14,798 / ambiguous 502 / conflict 7 /
  corroborates 147). dol5500 weak rows unchanged in kind.
- Coverage (strong LEI / GLEIF weak candidate only): fin 3 / 149 of 1,140; wealth 4 / 183 of 6,422;
  b2b 1 / 67 of 997; targets 7 / 450 of 10,558. Pilots (Whatnot, GlossGenius, Tecovas, Rockfish,
  Bombas, Carbon Arc, Kimball / Midwest Motor Supply, Talent Source): 0 of 8 -- none has a US LEI.
- Deploy: the shared api + 6 workers still run the old image code; `docker-compose restart api worker`
  is needed BEFORE the next scheduled entity_resolve (monthly, the 10th), or that run's old feeds
  would null the EDGAR LEIs and undo the 446 LEI identifiers.

## Adversarial review (2026-10-03, later)

- **False merges (hand-checked all 168 GLEIF->EDGAR joins of job 3153, 20 sampled against api.gleif.org):**
  6 joined a different legal person on an LEI the filer typed: 22C Capital LLC carried Bloomberg Finance
  L.P.'s LEI; LCP XI (Luxembourg) carried Lexington Partners VI Holdings'; INUSA Capital SPV II, Primary
  Commodity Fund Onshore, Bilbel Capital Fund, North Crossing Equity Partners carried their manager's / GP's.
  Fix: `resolve_core.NAME_GATED_ATTACH_TYPES` -- a no-CIK cluster linked only by LEI / UEI attaches by rule 5
  only when its name matches a current or former EDGAR name of the class (`gated_ciks` now loads those
  profiles), or when the names are equal with spacing / punctuation removed; otherwise
  `own_entity_name_mismatch`, and as the anchor it owns the LEI (rule 6). Cost: 3 true links with name
  variants also detach (two fund-series names, an NFP suffix); TheGoodEarCompany is kept by the squash.
- **LEI normalisation:** `_lei` now enforces the ISO 17442 MOD 97-10 check digits. 16 sec_filers values of
  20 alphanumerics failed ("WUWALLACEFAMILYTRUST", registry numbers, LEI typos); 2 were live identifiers.
  The one GLEIF record failing it is ANNULLED (not fed).
- **Tests the build missed:** the PG suites (TEST_PG_URL) of SPEC_116/147/148/149 failed -- their
  `sec_filers` fixtures lacked `lei` (prod has it) and SPEC_147 did not allow the new skipped feeds;
  `columns.generated.json` and `usage.json` were stale. Fixed / regenerated.
- **Terms re-read:** quote verified verbatim; condition IV(c) (no implied GLEIF endorsement, no logo,
  CHF 100,000 liquidated damages) added to the rights notes. Robots re-checked: api.gleif.org allow-all,
  goldencopy.gleif.org 403, www.gleif.org disallows only query URLs.
- **Live (review):** dry 3154 = live 3155 (name gate + checksum: 10 GLEIF memberships removed, 0 entities
  merged / split / dissolved); dry 3156 after it planned nothing new. Squash rule added: live 3157 (+1:
  TheGoodEarCompany), dry 3158 after it planned nothing new. Now: live entities 156,672; LEI identifiers
  429 (was 446); GLEIF records in an entity 159 (was 168); contested LEIs 11 (2 without an owner).
  Coverage (lei identifier / GLEIF name+state candidate of any status only): fin 3 / 186, wealth 4 / 234,
  b2b 1 / 67, targets 7 / 452 -- none of the 6 false merges sat in a universe. Workbench evals
  eval_core_identity + eval_bridge_core_view PASS; bridge refresh dry run: 8,062 rows, override 37,
  same counts by source (not refreshed).
