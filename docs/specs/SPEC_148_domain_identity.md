# SPEC 148 — Web domains as an identity carrier (two-source rule, redirect aliases)

**Status:** Implemented
**Task type:** service
**Date:** 2026-09-30
**Plan:** `docs/plans/PLAN_092_domain_identity.md`
**Test file:** tests/test_spec_148_domain_identity.py

## Goal

Workbench PE-TARGETS-DEPTH-PLAN phase 2, second half (task B2). `core.entity.canonical_domain`
is filled on 18,479 entities but cannot be trusted (linkedin.com 3,544, facebook.com 752; one
source family only), and ~519+ company domains sit in public NexData tables that are not wired
to core. Make the web domain an identifier the resolver records — normalized to the registrable
domain, generic hosts dropped — with a confirmation rule: a domain ↔ entity link is STRONG only
when two INDEPENDENT source families agree; one family is WEAK (recorded and shown, never used).
A domain never merges anything (plan: "never as a merge key").

## Domain-bearing tables (measured, read-only, 2026-09-30, prototype normalizer)

| Table (feed) | Rows | Registrable ok | Generic dropped | Keys | Family |
|---|---|---|---|---|---|
| `sec_adv_roster_snapshots` (adv, latest per CRD) | 24,017 | 14,337 | 6,777 (social 6,730) | crd | form_adv |
| `sec_adv_feed_firm_state` (iapd) | 23,939 | 13,908 | 7,147 | crd | form_adv |
| `sec_form_adv` (not fed: third copy of Form ADV) | 23,835 | 14,227 | 6,729 | crd | form_adv |
| `sec_filers.website` / `investor_website` (edgar) | 986,698 | **0 filled** | — | cik | edgar |
| `pe_firms` built from "SEC ADV" (not fed: copy of ADV) | 7,452 | 4,525 | 1,942 | crd/cik | form_adv |
| `pe_firms` web-researched (fed: `pefirm`) | 102 | 102 | 0 | cik 29, none 73 | pe_firm_web |
| `industrial_companies` (fed: `industrial`) | 233 | 226 | 7 | cik 59 | industrial_seed |
| `pe_portfolio_companies` (fed: `peportco`) | 361 | 269 | 4 | none (ein 0 filled) | pe_portfolio |
| `portfolio_companies` (fed: `portco`) | 945 | 7 | 0 | none | portfolio_agentic |
| `three_pl_company` (fed: `threepl`) | 116 | 6 | 0 | none | three_pl |
| `family_offices` (fed: `famoffice`) | 308 | 85 | 1 | none (crd 0 filled) | family_office |
| `lp_fund` (fed: `lpfund`) | 564 | 563 | 0 | none (crd 0 filled) | lp_fund |
| `company_ats_config.careers_url` (not fed) | 233 | 138 | 17 | company_id | same company as industrial |
| `job_postings.source_url` (not fed) | 30,949 | 11,495 | 17,937 (ats 17,789) | company_id | ATS / same company |
| `github_repositories.homepage` (not fed) | 305 | 53 | 7 | none | project, not company |
| `data_center_facility.website` (not fed) | 1,389 | 1,373 | 0 | none | operator site of a facility |

Distinct-domain overlaps with the form_adv family: pe_firm_web 55, lp_fund 13, pe_portfolio 8,
industrial 7, family_office 7. 524 ADV domains are named by more than one CRD
(focusfinancialpartners.com 15, man.com 14, apple.com 12 …).

## Acceptance Criteria

- [x] `app/entities/domains.py` (pure): `classify(value) -> (registrable|None, reason)` with reasons
      `ok | empty | invalid | generic:<category>`; registrable = eTLD+1 from a vendored, version-pinned
      Public Suffix List (ICANN section); `www.`/scheme/path/port/userinfo stripped, lowercased, IDN
      to punycode; IPs and bare suffixes refused; social / builder (and tenant subdomains) / ATS /
      webmail / directory / parking / shortener hosts dropped as generic.
- [x] Feeds store the classified registrable domain in `core.source_record.domain` (generic → NULL)
      and report per feed `{raw, kept, empty, invalid, generic:<cat>}`.
- [x] New ATTACH-ONLY feeds (`pefirm`, `industrial`, `peportco`, `portco`, `threepl`, `famoffice`,
      `lpfund`) write one source record per row with a kept domain. Their keys (cik) are used only to
      ATTACH the claim to an existing identity; they never enter the union-find, never become members,
      never feed the weak name+state tier, so entity composition is unchanged.
- [x] Source families: adv/iapd = `form_adv`; edgar = `edgar`; each attach-only feed its own family;
      a homepage probe naming the entity's legal name = `own_site`. Two records of one family are
      ONE source.
- [x] `domain_links()` (pure) per (domain, subject): `strong` iff one subject claims the domain and
      ≥2 distinct families agree; `weak` iff one family; `conflict` (reason `domain_shared`, other
      subjects listed) when >1 subject claims it; a domain claimed by more than
      `DOMAIN_SUBJECT_CAP` subjects is refused as `shared_host` (counted, examples capped).
- [x] Attach rules, in order: (1) member of a component → that entity; (2) attach-only record whose
      keys all point at one identity → it (`via: key`), keys pointing at different identities → its
      own subject with conflict `attach_key_disagreement`; (3) keyless attach-only record whose
      `name_norm` equals a name of exactly one identity claiming the same domain → that identity
      (`via: name`); (4) otherwise the record is its own subject.
- [x] Redirect alias: probe evidence `A → B` (B a different registrable domain) writes an `alias`
      row for every subject claiming A (`alias_of = B`, the redirect evidence in `evidence`) and
      counts those claims toward B (`via: redirect`); the redirect itself is never a family.
- [x] Strong links never merge: `plan()` components are identical with and without domains.
- [x] Strong entity links are written to `core.identifier` (`id_type = 'domain'`); `canonical_domain`
      is the entity's single strong domain, else NULL (several strong → NULL + `field_conflicts`).
- [x] `core.domain_link` (migration 0017) holds every strong/weak/conflict/alias row with its claims
      (record_key, source, family, via) — provenance on every link; idempotent re-run writes 0 rows.
- [x] `core.domain_probe` (migration 0017) holds probe evidence; the probe collector
      (`app/entities/domain_probe.py` over `app/core/open_web.py`) sends the honest NexdataResearch UA,
      refuses any host without a recorded terms review (`app/entities/data/site_terms.json`),
      obeys robots.txt (RFC 9309: 4xx = allow, 5xx/unreachable = disallow) for every hop, honours
      Retry-After (host backed off, never retried in the run), paces ≥ max(crawl-delay, 2 s) per host,
      refuses non-public addresses, never follows a redirect off the registrable domain (records it).
- [x] Resolve metrics carry a `domains` block (strong / weak / conflict / alias / shared_host,
      claims by family, subjects by status); dry run reports the same numbers and writes nothing
      but the ledger row.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_registrable_psl | eTLD+1 incl. co.uk / com.au / wildcard + exception rules, IDN, bare suffix refused |
| T2 | test_normalize_strips_and_lowercases | scheme, www, path, port, userinfo, e-mail, case |
| T3 | test_invalid_values | empty / placeholder / IP / no dot / spaces |
| T4 | test_generic_hosts_dropped | social, builder tenant subdomain, ATS board, webmail, parking, shortener |
| T5 | test_feed_row_stores_registrable_domain | `_row` stores classify() output, generic → NULL |
| T6 | test_feed_domain_stats | per-feed raw/kept/generic counts |
| T7 | test_two_source_rule_strong | adv + pefirm (via cik) on one entity → strong |
| T8 | test_same_family_is_weak | adv + iapd only → weak |
| T9 | test_shared_domain_conflict | two entities claim one domain → both conflict, never strong |
| T10 | test_shared_host_cap | > cap subjects → refused, counted |
| T11 | test_attach_by_name | keyless lpfund with same name + domain → corroborates → strong |
| T12 | test_attach_key_disagreement | attach-only cik pointing elsewhere than domain owner |
| T13 | test_redirect_alias | A→B probe: alias row with evidence; A's claims count toward B |
| T14 | test_own_site_name_is_a_family | probe name match upgrades weak to strong |
| T15 | test_domains_never_merge | plan() identical with/without domains and attach-only records |
| T16 | test_canonical_domain_strong_only | canonical_domain only from a strong link |
| T17 | test_robots_rfc9309 | robots 200 disallow / 404 allow / 503 disallow / unreachable disallow |
| T18 | test_terms_gate_refuses_unreviewed | no review → 0 requests |
| T19 | test_retry_after_backs_off_host | 429 + Retry-After recorded, host not retried |
| T20 | test_probe_records_cross_domain_redirect | same-site hops followed (gated), off-site hop recorded not fetched |
| T21 | test_private_address_refused | host resolving to 10.x / 127.x refused |
| T22 | test_resolve_pg_domains | PG: feeds + resolve write domain_link, identifier domain rows, canonical_domain; re-run 0 |
| T23 | test_dry_run_pg_writes_nothing | PG: dry run keeps no domain_link rows |
| T14b | test_own_site_on_redirect_target | redirected claims meet the target's own-site evidence (found live) |
| T17c | test_robots_redirect_followed_through_the_gates | RFC 9309 robots redirects followed, each hop gated (found live) |
| T22b | test_probe_evidence_pg | PG: a recorded redirect probe becomes an alias at resolve time |

## Rubric Checklist (generic — no service rubric file exists)

- [x] Tests written and watched fail before source code
- [x] Parameterized SQL only
- [x] No new dependency (PSL vendored, stdlib robots parsing)
- [x] Idempotent writes (IS DISTINCT FROM merge), nothing dropped silently (counts + capped detail)
- [x] Public data only; robots + terms + honest UA + Retry-After for any web request
- [x] ruff clean; catalog columns regenerated

## Design Notes

- `resolve.resolve()`: records split into resolvable vs attach-only (`ATTACH_ONLY_SOURCES`);
  `plan()`, `weak_name_state()`, `source_metrics()` see only resolvable records (B1 behaviour
  unchanged). `domain_links(records, rec_keys, comps, probe)` sees all.
- Probe evidence is read from `core.domain_probe` (latest per domain) at resolve time.
- `core.domain_link` PK `(domain, subject)`, subject = `ent:<component first member>` mapped to
  `entity_id` at write, or the record_key.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/entities/domains.py | Create | normalization, PSL, generic hosts |
| app/entities/data/public_suffix_list_icann.dat | Create | vendored PSL (ICANN), MPL-2.0 header kept |
| app/entities/data/site_terms.json | Create | per-site terms reviews (probe gate) |
| app/entities/feeds.py | Modify | classify domains, domain stats, attach-only feeds |
| app/entities/resolve_core.py | Modify | `domain_links()`, attach-only constants |
| app/entities/resolve.py | Modify | split records, write domain_link + identifier + canonical_domain |
| app/core/open_web.py | Create | robots/terms/UA/Retry-After/pacing/SSRF-gated fetch |
| app/entities/domain_probe.py | Create | redirect + own-site name collector (CLI, dry run default) |
| alembic/versions/0017_domain_link.py | Create | core.domain_link, core.domain_probe |
| app/api/v1/entity_master.py | Modify | `domain` lookup id_type |
| app/catalog/datasets.py, columns.generated.json | Modify | tables + limitations |
| tests/test_spec_148_domain_identity.py | Create | T1–T23 |

## Results (2026-09-30/10-01, live)

- Dry run job 3053 (mart_build #4), live jobs 3054-3057 (#5-#8), one-off worker on a pre-claimed row.
- Generic dropped: adv 6,777 / iapd 7,147 of ~24k each; industrial 7, peportco 4, famoffice 1.
- domain_link: strong 11 (8 entities), weak 14,855, conflict 1,704, alias 1; shared_host refused 0.
- canonical_domain 18,401 -> 8 (strong only); core.identifier +8 `domain` rows; 0 merges, 0 new entities.
- Probe: 6 terms reviews (2 allowed, 4 refused); gryphoninvestors.com -> gryphon-inv.com (301) alias,
  gryphon-inv.com homepage names "Gryphon Investors" -> strong (pe_firm_web + own_site).
- Two defects found live and fixed test-first: robots redirect to another registrable domain was
  treated as disallow-all (T17c); own-site evidence ran before redirect transfer (T14b).

## Feedback History

_No corrections yet._
