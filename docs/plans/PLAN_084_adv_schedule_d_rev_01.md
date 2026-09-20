# PLAN 084 — Revision 01

**Date:** 2026-09-20
**Trigger:** adversarial review of the SPEC_118 platform-adviser rule and the
`pe_funds` write path, run before the first production write. 32 findings
raised across four lenses, 6 survived refutation, 4 of those confirmed by
direct measurement against the live database.

## Revision 01

The plan reached its ship gate, failed it at 94.56%, and the investigation
that followed produced a new rule (filing platforms) that moved it to 96.25%.
That rule was written and measured in one pass and shipped nothing — the
review caught two defects in it before any row was written, plus two
pre-existing weaknesses in the SPEC_117 name tier that the new rule *promotes*
by routing displaced funds through it.

## What Was Wrong

**1. The collision metric did not compute what its docstring claimed.**
`find_platform_advisers` maps a 1-or-2-token stem to a *set* of CRDs, so a
single fund row credits the filer with every adviser sharing that stem, not
one. The docstring says "count how many other advisers' names appear on a
filer's fund list"; the code counted "how many adviser CRDs share a stem with
any of my funds".

*Measured:* the review's synthetic 30-entity Goldman scenario does not occur —
the largest real stem holds 13 CRDs (`pearl diver`), and no stem reaches 25,
so no live adviser can be misclassified by one row today. But the margin is a
factor of two, not a factor of ten, and the quantity was the wrong one.

**2. Displacement dropped links instead of moving them.** When a platform's
claim was displaced, `attribute()` picked the sponsor from `core_index` — every
adviser on the ADV roster — while `pe_funds_sec` can only write a link when the
CRD has a `pe_firms` row, which only PE/VC-reporting advisers get. The sponsor
was taken on faith and the platform link discarded with `continue`.

*Measured:* **234 of the 320 displacements resolved to nothing**, so those
funds ended with no manager at all — worse than the platform link they
replaced. 127 went to CRD 122816 and 41 to CRD 301819, neither in `pe_firms`.
`platform_displaced` still counted all 320 as successes, so the loss was
silent.

**3. `match_name_core` cut the fund name at a character offset.**
`fund_core[:cut]` can land inside a token, so an adviser core that is a
character prefix of a longer word matches it.

*Measured:* **89 of 1,939** `name_core` links come from a mid-token cut.
Some are harmless plurals (`ascent venture` ← "ASCENT VENTURES LP", the right
firm), but others are plainly wrong entities: "CARMELINA CAPITAL PARTNERS" →
`carmel`, "HarbourView Royalties Fund" → HARBOUR GROUP INDUSTRIES (a Missouri
industrial holding company), "Counteract One LP" → `counter`.

**4. `MIN_CORE_LEN = 6` admits single-token adviser cores.** 1,437 of 25,223
cores are one token, 549 of them ≤7 characters — `boston`, `harris`, `summit`.
**1,079 of 1,939** `name_core` links rest on a single-token core. This is why
"Summit Global All Cap Equity Fund LP" was attributed to SUMMIT PARTNERS.
Pre-existing in SPEC_117; SPEC_118 makes it load-bearing.

**5. (minor) `SOURCE_SQL`'s `DISTINCT ON` had no deterministic tiebreak.**
`loaded_at` is `NOW()`, identical for every row of a release, and an amendment
usually shares its original's `date_of_first_sale`, so which filing wins could
change between runs — taking `entity_name`, and therefore the attribution, with
it.

**6. (minor) A human-verified `manual` link was relabelled.** The keep-prior
branch was gated on `firm_id != prior_id`, so when the mart *agreed* with a
human it overwrote `firm_link_method` with its own tier, destroying the
provenance that T17 exists to protect.

## What Was Fixed

1. Count distinct adviser *names* (stems), not CRDs, and require a minimum
   fund count so one row cannot carry the verdict.
2. `attribute()` takes a `can_resolve` predicate; a displacement that would not
   produce a link keeps the platform's claim under `adv_platform` instead, and
   the unresolvable case is counted rather than silent.
3. `match_name_core` requires the cut to land on a token boundary, allowing
   only a trailing plural `s` — which keeps "ASCENT VENTURES" → ASCENT VENTURE
   PARTNERS while rejecting "CARMELINA" → `carmel`.
4. **Not fixed — the proposed remedy is wrong, and measuring it showed why.**
   Requiring single-token cores to be >=8 characters would drop 309 of 1,643
   `name_core` links, and the 6- and 7-character cores it removes are mostly
   *correct*: ARDIAN, TINICUM, EQUIAM, GRESHAM, LUMINA are all distinctive
   names that happen to be short. Length is not the defect; *commonness* is
   (`boston`, `harris`, `summit`). A stopword or corpus-frequency test would
   be the right instrument, and that is new design work, not a threshold
   change. Left as measured with the reasoning recorded.
   The motivating example is already handled by tier order: "Summit Global
   All Cap Equity Fund LP" now resolves to PATHSTONE via `adv_exact`, which
   outranks `name_core`.
5. `accession_number DESC` added as the final sort key.
6. A foreign method is preserved whenever it outranks, agreement or not.

## Lessons Learned

- **A metric's docstring is not a test.** The collision rule was validated by
  eyeballing a ranked table of the four advisers it caught. Nothing checked
  that the number meant what the comment said, and it did not. A measurement
  that confirms the answer you expect is not evidence the method is right.
- **Validate the analysis script against the shipped code path.** The 21→64
  "cliff" was produced by a throwaway script that ranked only advisers with
  ≥50 funds and built its index from a different table than the mart uses. It
  described a different function than the one that shipped.
- **A rule that re-points data needs a resolvability check at the point of
  decision.** `attribute()` decided; `pe_funds_sec` discovered the decision was
  unwritable. Splitting the two made a 234-row loss invisible.
- **The gate worked exactly as designed.** It was written to catch a resolver
  bug, the shortfall was a resolver bug, and refusing to treat disagreement as
  a correction is what surfaced the platform problem. Keep gates that can fail.
