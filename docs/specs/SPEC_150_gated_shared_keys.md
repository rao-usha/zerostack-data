# SPEC 150 — Gated shared keys: an EIN or CRD alone never joins two EDGAR filers

**Status:** Active (LIVE 2026-10-01 job 3065; workbench bridge --apply held for the D2 owner call)
**Task type:** service
**Date:** 2026-10-01
**Plan:** `docs/plans/PLAN_094_gated_shared_keys.md`
**Test file:** tests/test_spec_150_gated_shared_keys.py
**Owner call:** the owner asked to fix the shared-EIN multi-CIK entities by changing the merge
rules, and approved the rule set below (2026-10-01, "do that"). The first build was code, tests,
dry run and measurements only. **Amended 2026-10-01 (review fixes, gate_v2):** a review of the
dry run (job 3062) found a de-SPAC join, a run-date-dependent rule, a truncated first-filing
date and a silent EIN+CRD split; fixed below (V4, data-relative sequential, heavy-filer first
date, A2 / A6), then the owner's "1. do that, 2 keep local, 3 do that" took it LIVE through the
job path. NexData stays local (no push).

## Goal

471 live core entities hold more than one CIK (measured 2026-10-01 after the 05:06 run): 397
joined by an EIN only, 52 by a shared 13F cover-page CRD, 3 by both, 19 by a bridge edge.
Before SPEC_147, EDGAR filers shared EINs: subsidiaries on a parent EIN, insiders filed under
a company EIN, insurance separate accounts, ESOPs/plans/trusts, old `/ADV` duplicate filer
accounts, renames and successor CIKs. A shared EIN or CRD says "related", not "the same legal
person". Make the resolver require corroboration before an EIN or CRD joins two CIKs, split
what fails, keep stable ids for the main company, and record why.

## Rules (deterministic; `app/entities/gate.py`, used by `resolve_core.plan`)

1. **Gated keys.** CIK, LEI, UEI and state registry ids union records as today (ungated).
   EIN and CRD, including the CIK<->CRD bridge edges, union two different CIK groups only when
   a corroboration below holds for that pair. Records without a CIK that share an EIN/CRD with
   each other (ADV + IAPD, Form 5500) still union freely: there is no CIK to protect.
2. **EDGAR-aware name normalization.** Strip tail tags `/[A-Z0-9.&]{1,6}/?` (`/ADV`, `/NY/`,
   `\DE\`) and apostrophes; fold "limited partnership" / "limited liability company" /
   `L.P.` / `L.L.C.` to lp / llc; strip legal suffixes at the tail (norm's list); keep every
   designator token; Roman numerals I-X map to digits. Equal = exact normalized name, the
   space-free form, or (non-vehicles only) the sorted token set.
3. **Vetoes, checked first.** V1: a person filer never joins an organization (a name with no
   organization token, 2-4 alphabetic tokens, no digits, filing as an insider owner or a 13F
   manager, with no Form D, 10-K or tickers). V4 (gate_v2): equal names are two companies when
   one CIK was renamed INTO the name (its last differing former name ended at date R) while the
   other filed under that name before R, their filing windows overlap, and the other never
   carried the first one's old names -- a de-SPAC (Plum Acquisition -> VEEA INC. beside the
   private Veea Inc.; GigCapital7 -> Hadron Energy) or a holding-company namesake (BGC);
   refused as `V4_name_adopted_concurrent`. V3: a vehicle joins only through R1, R1c or R2
   (fund / series / separate account / variable / SPV / co-invest / feeder / master / QP / DST /
   plan / ESOP / 401(k) / portfolio / funding / SPC / SICAV / SCSp / GP ... but not "Fund
   Management"; a Form D pooled-fund filer; SIC 6189; a numbered name with no SIC and no 13F).
4. **Corroborations** (pairwise, in order): R1 equal names with compatible legal forms; R1c
   equal names, conflicting forms, sequential filings (a conversion); V2 equal names,
   conflicting forms, side by side = two persons; R2 a former name of one equals a current or
   former name of the other AND sequential (concurrent = a holding-company reorg, refused as
   `R2_concurrent`); R3 non-vehicles sharing a CRD whose names are a >=2-token prefix of each
   other and the extra tokens are neither structural nor a jurisdiction; R4 non-vehicles sharing
   a CRD with sequential filings. **Sequential (data-relative, gate_v2):** the old CIK filed
   nothing for >= 120 days while the new one kept filing (the new CIK's last filing is >= 120
   days after the old CIK's last), the new CIK's first filing is <= 200 days after the old CIK's
   last, and the windows overlap by <= 31 days. No rule reads the run date: the same data gives
   the same plan on any day (gate_v1 measured dormancy against the run date, so N10 WEALTH / N10
   ASSETS -- one 13F each, three days apart -- would have merged on 2026-12-12 with no new data).
5. **Attaching records with no CIK** (an ADV/IAPD cluster by CRD, a Form 5500 sponsor by EIN):
   the only class holding the key; else the one class whose names match the record's name
   (legal form breaks a tie); else the single non-vehicle, non-person class; else the records
   stay together as their own entity.
6. **Contested keys.** `core.identifier` is keyed `(id_type, id_value)`. A key value carried by
   two or more resulting pieces has ONE owner: the piece holding its anchor record (ADV/IAPD,
   Form 5500), else the single operating piece, else nobody. Non-owners get no identifier row,
   NULL in the canonical column, and a `contested` note in `field_conflicts`.
7. **Ambiguity flags** (review only, never a merge) on split pairs: A1 EIN-only, sequential,
   non-vehicles, not both Form-D-only; A3 equal base name with conflicting legal forms
   overlapping < 180 days; A4 EIN and CRD both shared but the names share no token; A2 (gate_v2)
   EIN and CRD both shared, names related (Orion Resource Partners (USA) LP / Orion Resource
   Partners LP; "shared" CRD includes a review-only hint: an identifier-tier 13F bridge edge on
   a CRD that maps to several CIKs, which `resolve._load_crd_hints` loads and plan() never
   unions on); A5 former names overlap while both are active and neither is an operating
   issuer; A6 (gate_v2) the V4 name-adopted pairs.

**Filer profiles.** The rules need, per CIK: current and former names, SIC, the recent-filing
window (first/last), latest form, tickers, insider flags, and Form D / 13F / insider-owner
counts, the former names' from/to dates and `recent_filing_count`. EDGAR's recent list holds
1,000 filings, so for a heavy filer (`recent_filing_count` >= 1,000) the earliest recent date is
only a floor: the earliest former-name date is the first filing when it is older, else the first
filing is unknown -- never "recent" (it cannot be the new side of a succession, and it ranks as
the oldest filer). `resolve.py` loads them from the NexData SEC tables (`sec_filers`,
`sec_filer_former_names`, `sec_insider_owners`, `form_d_filings`, `sec_13f_filings`)
only for CIKs that share a gated key with another CIK group, and passes them to the pure
`plan()`. A missing table or column means no profile (the rule then sees only the record's
name), never an error. The 10-K count of the analysis is dropped: `sec_10k` holds 789 rows for
~95 curated companies (an ad-hoc dispatch table, not a bulk source), and SIC / tickers already
mark those as operating issuers. The catalog's entity_master inputs gain the four bulk sources
(the resolve stage now reads them; the SPEC_141 evidence disposition is `amended:SPEC_150`).

## Main piece and stable ids

When an entity splits, the piece that keeps its `entity_id` is the **main** piece, ranked:
(1) it owns a CRD; (2) it is operating (a non-vehicle, non-person CIK, or an anchor-only piece);
(3) it is an active filer (last filing within 120 days of the newest filing any loaded profile
shows -- the data's own date, not the clock); (4) it is the oldest filer
(earliest first filing); then the existing rules (largest, smallest record_key). Every other
piece gets a new id and a `split_from` note.

## Provenance

- `field_conflicts.gate` on every component a gated decision touched: rules that joined CIKs,
  links refused (with the veto / missing-corroboration reason and the shared key), ambiguity
  flags, how no-CIK records attached, contested keys and their owner. Lists capped
  (`DETAIL_CAP`) with exact counts alongside; `gate_version` named.
- `field_conflicts.split_from` on a piece that left an entity (the lost entity ids).
- An entity whose records all became single-record pieces is dissolved by the orphan sweep and
  gets `field_conflicts.dissolved_by_gate` (the refused links).
- `core.resolve_run.metrics.gate`: pairs considered / joined by rule / refused by reason,
  ambiguity flags, attachment outcomes, contested keys and owner-less keys, multi-CIK components.

## Acceptance Criteria

- [x] Rename kept: a dormant CIK and its successor whose former name matches stay one entity.
- [x] `/ADV` duplicate kept: two CIKs whose names differ only by an EDGAR tag stay one entity.
- [x] Subsidiary split: a parent and a subsidiary sharing an EIN, both filing, are two entities.
- [x] Insider split: a person filed under a company EIN or CRD never joins the company.
- [x] Separate account split: insurer separate accounts sharing the insurer's EIN are not joined
      to the insurer or to each other.
- [x] ESOP split: a company's ESOP/plan sharing its EIN is not joined to it.
- [x] Ambiguous handled per design: split, flagged (A1/A3/A4/A5) in `field_conflicts.gate` and
      metrics, never merged.
- [x] No-CIK records attach per rule 5; Form 5500 sponsors still join their single EDGAR filer.
- [x] Contested keys: one owner, no duplicate identifier rows, non-owners NULL + noted.
- [x] Main piece keeps the id; other pieces carry `split_from`; dissolved entities carry
      `dissolved_by_gate`.
- [x] Never a new merge: every new component is a subset of a component the old rules made.
- [x] Records that share no gated key across CIKs resolve exactly as before.
- [x] Re-running over an unchanged corpus changes 0 entities / memberships.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_edgar_name_norm | tail tags, apostrophes, LP/LLC folding, roman numerals, designators kept |
| T2 | test_adv_duplicate_kept | `X INC /ADV` + `X INC` sharing an EIN -> one entity, rule R1 |
| T3 | test_rename_kept | dormant CIK + successor with matching former name -> one entity, R2 |
| T4 | test_form_conversion_kept_and_side_by_side_split | LLC->Inc sequential kept (R1c); Ltd + LP concurrent split (V2) |
| T5 | test_subsidiary_split | parent + subsidiary, shared EIN, both active -> two; concurrent former-name reorg refused |
| T6 | test_insider_split | person filer with the issuer's EIN -> split, V1 |
| T7 | test_separate_account_split | insurer + 2 separate accounts -> no join (V3) |
| T8 | test_esop_split | company + its ESOP -> split (V3); the 5500 sponsor attaches to the company |
| T9 | test_shared_crd_rules | R3 strategy-labelled CIK kept; R4 dormant->successor kept; distinct advisers split; bridge edge gated too |
| T10 | test_ambiguous_flagged_not_merged | A3 / A1 / A4 / A5 flagged, split |
| T11 | test_attach_no_cik_records | only class / name match / single operating class / own entity |
| T12 | test_contested_key_owner | anchor owner, operating owner, no owner; keys unique across comps; canonical NULL + note |
| T13 | test_main_piece_keeps_id | CRD owner keeps the id over a larger vehicle piece; others new + split reported |
| T14 | test_never_a_new_merge | randomized corpora: every new component is a subset of an old one |
| T15 | test_ungated_unchanged | corpus with no cross-CIK gated key -> identical components to the ungated rules |
| T16 | test_profiles_only_for_shared_keys | `gated_ciks()` names exactly the CIKs needing a profile |
| T17 | test_gated_split_writes_provenance_pg | PG: split, ids, identifier uniqueness, split_from / gate / dissolved_by_gate, idempotent rerun |
| T18 | test_despac_namesake_split | VEEA-shaped pair: V4 refusal + A6 flag, split |
| T18b | test_name_adopted_controls_still_kept | renamed together (Osaic) and renamed long before (University Bancorp) stay R1 |
| T19 | test_sequential_is_data_relative | plan() takes no run date; N10 never a succession on unchanged data; Zega always one |
| T20 | test_heavy_filer_first_date | truncated recent list: former-name date or unknown; never the new side; ranks oldest |
| T21 | test_ein_and_crd_shared_flagged | EIN + CRD shared, related names -> A2 flag, split |
| T21b | test_crd_hint_flags_never_joins | a multi-CIK-CRD bridge edge flags A2, never changes a component |

## Rubric Checklist (generic — no service rubric file exists)

- [x] Tests written and watched fail before source code
- [x] Pure rules (no DB, no clock, no run date: data-relative since gate_v2)
- [x] Parameterized SQL only; missing profile tables tolerated
- [x] Idempotent; nothing dropped silently (counts reported)
- [x] ruff clean

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/entities/gate.py | Create | pure rules: name norm, filer kind, corroborate, ambiguity |
| app/entities/resolve_core.py | Modify | gated plan(), contested keys, main rank, canonical_row withheld keys |
| app/entities/resolve.py | Modify | profile loader, provenance writes, gate metrics |
| tests/test_spec_150_gated_shared_keys.py | Create | T1-T21b |
| tests (older specs) | Modify only where a test asserted an EIN joining two CIKs | |

## Owner calls left open (recorded 2026-10-01, never decided by the code)

- **Ambiguous pairs kept split and flagged** (a split loses no record and a later rule can
  rejoin it; a wrong merge would fuse two companies' identifiers). The review judged three of
  them the same company; each needs an owner ruling before any rule joins it:
  - 140401 RP Management, LLC (CIK 1507673) / Royalty Pharma Manager, LLC (CIK 2077508): same
    EIN 611450238, 4-day handover 2025-08-29 -> 2025-09-02 (A1).
  - 59988 Kagin's Digital, Inc. (CIK 2065818, former name "Kagin's Digital, LLC") / Kagins
    Digital LLC (CIK 2122940, one Form D 2026-03-23): same EIN 334640086 (A3).
  - 141204 Mesirow Financial Investment Management, Inc. (CIK 1469475) / CIK 846788 (former name
    "MESIROW FINANCIAL INVESTMENT MANAGEMENT"): same EIN 363429599, both filing (A5).
  - Also flagged: 71692 Hand Technologies, 67501 Donum Charitable Lending Fund (A3); A2
    Orion Resource Partners (132557), Knighthead (140229), ValueAct (143259); A6 VEEA (124223),
    Hadron Energy (84597), BGC Group (145067).
- **D2 override set.** With the gate live, override CRDs 141488 (Peak, CIK 1710524) and 313265
  (Smith Group, CIK 1893134) are served by core.entity (same CRD), so the workbench eval's
  recomputed 39-CRD set becomes 37: re-baseline or give overrides precedence (workbench owner).
- **Override CIK 2079080** (Brian Low Financial Group) is set to CRD 306346 in the override
  table while core serves CRD 306246 (entity 52860); pre-existing, not caused by this spec.
