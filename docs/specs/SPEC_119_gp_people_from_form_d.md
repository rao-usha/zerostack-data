# SPEC 119 — GP people from Form D related persons

**Status:** Draft
**Task type:** service (mart)
**Date:** 2026-09-20
**Test file:** tests/test_spec_119_gp_people.py
**Plan:** [PLAN_085](../plans/PLAN_085_gp_people.md)

## Goal

Populate `pe_people` and `pe_firm_people` with the individuals behind PE/VC
funds, from SEC Form D related persons, now that SPEC_118 gave 20,490 fund
vehicles a `firm_id`. `pe_firm_people` goes from 2,527 rows over **78 firms**
to roughly 12,500 over **3,200 firms**.

This is only possible now. Before SPEC_118 only 5,101 funds had a firm, and
before SPEC_117 only 7 did — the related-person rows had nowhere to attach.

## What the source actually is

`form_d_related_persons` holds 583,963 rows over 168,592 filings. Joined
through `form_d_issuers (is_primary)` to `pe_funds`, **87,179 rows land on a
fund with a `firm_id`**, covering 3,417 firms.

It is not a people table. The SEC form lets filers put anything in the name
fields and they do: ~40% of distinct name tuples are **entities**, mis-split
across the fields — `first_name='LLC'`, `last_name='Fund GP,'` — and 23,597
rows carry a placeholder first name (`N/A`, `-`, `--`). Cleaning that is most
of this spec.

## Acceptance Criteria

- [ ] Every input row is either kept or counted by exactly one named refusal,
      asserted in code: the counters **must sum to the input row count**.
- [ ] Identity is `(name_norm, firm_id)`, never name alone.
- [ ] The 2,551 existing `pe_people` rows are not merged into or modified.
- [ ] `pe_firm_people.title` is never `''`.
- [ ] A second run reports `inserted == 0` and `updated == 0` on both tables.
- [ ] Reversible through `app/core/quarantine.py`, proven by running it.
- [ ] Ship gate measured and reported before writing.

## Identity — the decision most likely to cause silent damage

**Key: `(name_norm, firm_id)`**, stored as
`pe_people.source_key = 'secformd:<firm_id>:<name_norm>'`.

`name_norm` is `norm._fold()` over `first_name` + `last_name` only. It does
**not** use `norm.core()`, which strips `co`/`ltd`/`the` as legal suffixes and
would mangle human surnames, and it does **not** use `middle_name`, which is
corrupt: 319 name+firm groups carry two or more conflicting non-empty middle
values for one person at one firm, with initials running in blocks down the
filing sequence.

Measured head-to-head:

| key | rows | failure mode |
|---|---|---|
| name alone | 9,211 | **fuses 139 distinct humans** — `david miller` spans 5 firm families, 5 zips, 4 states |
| name + firm | 9,933 | same human appears twice (722 rows), almost all one GP at affiliated registrations |

A visible, labelled duplicate is recoverable; a silent fusion of two people is
not. Name+firm costs 7.8% duplication and buys that.

**Existing rows are not touched.** 162 incoming names collide with a legacy
`pe_people` row by normalized name and **zero collide at the same `firm_id`** —
the overlap is a duplicated *firm* master (49 of the 78 legacy firms have an
SEC twin: legacy `KKR` vs `KOHLBERG KRAVIS ROBERTS & CO. L.P.`). That is a firm
dedup problem, not this load's. Both counts ship as stats.

## Refusals — evaluated in this fixed order

Order matters: the same rules in a different order produce different counters
for the same kept set (measured: placeholder-first gives 23,748/11,486,
entity-first gives 34,692/542). The order is part of the contract.

| # | counter | rule | removes |
|---|---|---|---|
| 1 | `refused_placeholder_name` | first or last folds empty, or first is one of `n a`, `na`, `none`, `.`, `-`, `--`, `*` | 23,550 |
| 2 | `refused_see_clarification` | either field folds to `see clarification` | 47 |
| 3 | `refused_entity_name` | any **whole token** of first or last is in the entity vocabulary | 11,560 |
| 4 | `refused_entity_name_dotted` | as above, after gluing each run of single-letter tokens **within one field** (`l.l.c.` → `llc`) | ~339 |
| 5 | `refused_name_too_long` | first+last folds to more than 4 tokens or fewer than 2 | 180 |
| | **kept** | | **51,842** |

Three constraints on rule 3, each a bug if violated, measured:

1. **Whole-token matching only.** `LIKE '%co%'` destroys 600 real people
   including Michael Collins (311 rows) and Scott Voss (157); `%lp%` costs 27
   (Volpert, Alpern); `%inc%` costs 32 (Vincent, Reyna).
2. **Never test `middle_name`.** All 8 tuples flagged by a middle-field token
   alone are real people with a corrupted middle field (`Anthony | GP | Cusano`).
3. **Glue single-letter runs per field, never across fields** — otherwise
   `Brian | C. | O'Connor` glues to `co`.

Surnames that merely resemble the vocabulary are safe: Marks, Gross, Rich,
Price, Banks, Bond, Young are not tokens in the list.

## Tiers

### `pe_firm_people.person_link_method` — how much to trust the link

Weakest label wins, so `WHERE person_link_method = 'form_d_signer'` is the
clean set.

| tier | meaning | pairs |
|---|---|---|
| `form_d_signer` | named on a Form D of a fund attributed by a non-platform tier | ~9,570 |
| `cross_brand` | the name appears at ≥3 distinct firm **brands** (brand = first token of `pe_firms.name`) | ~140 |
| `platform_fund` | **every** fund behind the pair was linked by `adv_platform`, so the firm is the filer, not the sponsor | ~25 |
| `fund_admin` | `relationship_clarification` matches agent/administrator/authorized-signatory on ≥50% of the person's filings | ~200 |

Tier 1 is **not** called `related_person`: `pe_funds.firm_link_method` already
uses that spelling for its second-weakest tier, and both columns sit on the
same row.

`fund_admin`'s 50% threshold is a cliff, not a knob: of 9,211 person-shaped
names, 9,162 sit at exactly 0%, 30 at 90–100%, and only 8 names fall anywhere
between 0.25 and 0.90. The ≥50% list reads as a roster of fund administrators
(Brett Sagan 97.8% across 89 firms, Taylor Hughes 100% across 44), while every
genuine GP in the top 45 by fund count scores exactly 0.0% — including all
sixteen HarbourVest Managing Directors.

`cross_brand` collapses on the **first** token, not two: Blackstone's four
registrations are four distinct two-token stems, so a two-token collapse would
convict John Finley and Christopher Striano of promiscuity. Threshold 3 rather
than 2 because the two-brand band is dominated by real GPs at affiliated
registrations (Oaktree/Duration, TPG's three entities).

### `pe_firm_people.firm_link_method`

The fund tier, propagated verbatim from `pe_funds` — strongest across the
pair's filings by SPEC_118's existing `TIER_RANK`. A person is never more
certain than the fund→firm link beneath them, and the vocabulary is not
re-spelled.

## Evidence, not judgement

`fund_count`, `filing_count`, `first_seen`, `last_seen` (from
`form_d_filings.filed_at`, **not** `loaded_at`, which is `NOW()` and identical
across a whole release), and:

`address_confirmation` ∈ `zip5 | state | none | unknown` — does any filing's
zip agree with the adviser's ADV main office? 68% of pairs confirm. Michael
Collins confirms 313/313; Brett Sagan confirms **0** of 64 across three brands.

It is recorded and **never gated on**: Orlando Bravo (Thoma Bravo) and Gabriel
Caillaux (General Atlantic) both score `none`. Source is
`sec_adv_roster_snapshots.main_office_postal_code` — `form_adv_advisers` has
zero rows.

`start_date`, `end_date`, `seniority`, `department` and `role_type` stay NULL
with a counter. The corpus spans 2023-07 to 2026-06, every pair is
left-censored, and 40.9% appear on exactly one filing — a filing date is when
someone was *observed*, never when they joined.

`title` is the **sorted union** of `relationships` across the pair's filings,
comma-joined in the fixed order `Director, Executive Officer, Promoter`. The
union, not the latest filing, is what keeps 4% of pairs stable across reruns.
`relationships` is non-empty on 100% of the 87,179 rows, so `title` is never
`''`. `relationship_clarification` must **not** become the title: Adam
Schwartz at Angelo Gordon carries 42 distinct clarification strings.

## `pe_firms.is_spv_platform` — and what it cannot do

Access platforms (EquityBee, Alumni Ventures, OurCrowd, Vauban, Echo) file
hundreds of tiny SPVs. Their staff are indistinguishable from founders by
anything in Form D: Adam Ingram (EquityBee, 203 funds) and Michael Collins
(Alumni Ventures, 276 funds, a genuine GP) are both at exactly one firm, both
0.0% admin prose, both `Executive Officer`.

So the flag goes on the **firm**, not the person. Two candidate rules were
measured and **rejected**:

- **SPEC_118's `find_platform_advisers`.** Detects *filing* platforms by
  borrowed names. EquityBee's funds carry EquityBee's own name (affinity
  0.978), so it scores near zero. Wrong instrument.
- **Fund size alone.** Madison Dearborn, Vista Equity and Andreessen Horowitz
  all have a 90th-percentile fund size of **$0.00M**, because Form D reports
  nothing sold at launch. This would classify three major GPs as platforms.

What survives: **≥60 attributed fund vehicles AND no fund ever reporting more
than $50M raised.** Among the 17 firms with ≥60 funds this separates cleanly —
platforms top out at $5–14M (Vauban 6.0, Alumni 8.1, Echo 8.8, OurCrowd 13.0,
EquityBee 14.2), while the next firm up is Brown Advisory at $190M and real
GPs run to $18,118M (Apollo).

**Known miss, recorded not hidden: Forge Global Advisors** is a secondary
marketplace, unquestionably a platform, but one $397M vehicle puts it over the
cap. The ≥60 floor also means nothing is claimed about smaller firms. This
flag is a floor on what we can prove, not a complete census; ADV Schedule A
titles (SPEC_120) are the real fix.

## Ship gate

Run `build(conn, dry_run=True)` and report before writing. Any failure stops.

1. **Refusal closure** — the counters sum exactly to the input row count.
2. **Volume** — pairs 9,700–10,200; people 9,000–9,450; firms ≥ 3,100.
3. **Tier shape** — `form_d_signer` 94–98% of pairs; `fund_admin` ≤ 400;
   `platform_fund` ≤ 100.
4. **The admin threshold is still a cliff** — ≤150 filings in the 0.25–0.90
   band.
5. **Title** — 100% non-empty, exactly 7 distinct values, scoped to rows this
   mart wrote (1,090 legacy rows hold `''` and are not ours to fix).
6. **No silent merge** — `collides_same_firm == 0`.
7. **Idempotency** — second run inserts 0, updates 0, both tables.
8. **Reversibility** — the quarantine rule resolves to exactly the inserted
   rows and pulls the links through the FK walk.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | `test_refusal_counters_close_exactly` | kept + refusals == input, the house rule |
| T2 | `test_entity_tokens_match_whole_words_only` | Collins/Voss/Volpert/Vincent survive; `LLC`/`Fund GP` do not |
| T3 | `test_dotted_legal_forms_are_glued_per_field` | `L.L.C.` caught; `Brian C. O'Connor` not |
| T4 | `test_middle_name_never_refuses` | `Anthony \| GP \| Cusano` kept |
| T5 | `test_placeholder_names_refused_and_rescued` | `N/A \| \| Belltower…` refused; `- \| \| Brandon Green` rescued |
| T6 | `test_identity_is_name_plus_firm` | one name at two firms → two rows, not one |
| T7 | `test_title_is_sorted_union_not_latest` | stable across filing order |
| T8 | `test_fund_admin_tier_by_clarification_share` | ≥50% share tiers, below does not |
| T9 | `test_cross_brand_uses_first_token` | Blackstone's four registrations stay one brand |
| T10 | `test_platform_fund_tier_when_all_funds_are_platform` | mixed funds do not tier |
| T11 | `test_address_confirmation_never_gates` | `none` rows are still written |
| T12 | `test_dates_come_from_filed_at_not_loaded_at` | idempotence under re-load |
| T13 | (pg) `test_build_is_idempotent` | second run 0/0 |
| T14 | (pg) `test_existing_people_untouched` | the 2,551 legacy rows unchanged |
| T15 | (pg) `test_two_phase_merge_resolves_person_ids` | links point at the right people |
| T16 | `test_spv_platform_rule_rejects_zero_size_gps` | Madison Dearborn not flagged |

## Files to Create/Modify

| File | Action |
|------|--------|
| alembic/versions/0011_pe_people_sec.py | Create |
| app/marts/pe_people_sec.py | Create |
| app/marts/names.py | Create (folding + entity vocabulary) |
| app/core/pe_models.py | Modify (new columns) |
| app/core/quarantine.py | Modify (SEC_MART_RULES + fix `MAX_TOTAL_ROWS`) |
| app/worker/executors/pe_marts.py | Modify (run the people build) |
| tests/test_spec_119_gp_people.py | Create |

## Feedback History

_No corrections yet._
