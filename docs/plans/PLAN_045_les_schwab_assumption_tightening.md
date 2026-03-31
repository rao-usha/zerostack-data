# PLAN_045 — Les Schwab Report: Assumption Tightening (IC-Defensible Pass)

## Source
ChatGPT third-pass review of report #212. Four remaining credibility gaps flagged.

---

## Four Changes

### 1. Alignment Volume (~1.4M) → Label as "modeled estimate"

**Problem:** `~1.4M alignments annually` is cited as a fact in two places (Section 4
callout, Key Assumptions table). It is derived: $168M revenue ÷ ~$90 ASP — an estimate,
not sourced data.

**Fix:**
- Anywhere `~1.4M alignments` appears, add `(modeled est.)` inline or change to
  `an estimated ~1.4M alignments annually (modeled: $168M rev ÷ ~$90 blended ASP)`
- Key Assumptions table already notes the basis — add `(modeled estimate)` to the
  value cell: change `~1.4M jobs/yr` → `~1.4M jobs/yr (modeled est.)`
- Section 4 ADAS callout: `With an estimated ~1.4M alignments annually` is already
  qualified — tighten to `With an estimated ~1.4M alignments annually (modeled: rev ÷ ASP)`

---

### 2. ADAS Attach Rate Assumption → Add OEM + Insurance Dependency Note

**Problem:** The ADAS revenue opportunity implicitly assumes that a meaningful % of
alignments will result in a calibration upsell. This depends on:
- Whether the OEM *requires* post-alignment recalibration (not all do)
- Whether insurance workflows cover calibration as part of repair/service
- Whether technicians are trained to offer it proactively

**Fix:** Add one sentence to the ADAS callout in Section 4:
> "Adoption pace depends on OEM calibration requirements (currently mandatory on ~65%
> of MY2023+ ADAS-equipped vehicles per NHTSA) and whether regional insurance workflows
> begin covering calibration as standard — a trend accelerating in collision repair
> but not yet standard in tire/alignment shops."

Also add a row to the Key Assumptions table:
- Assumption: `ADAS attach rate (full rollout)`
- Value: `~50% base / 100% upside`
- Source: `NHTSA ADAS vehicle share; Hunter Engineering market data`
- Sensitivity: `50% penetration = $56–105M; 25% = $28–53M`

---

### 3. Fleet Thesis → Add AV Commercialization Timing Caveat

**Problem:** The fleet / robo-taxi thesis in Sections 4 and 5 is presented as a logical
extension but the timeline is genuinely uncertain — it depends on AV regulatory approval,
insurance frameworks, and actual fleet operator purchasing behavior.

**Fix:** In the fleet callout in Section 4, add one sentence:
> "Timing is uncertain and depends on the pace of AV commercial deployment — currently
> accelerating in select metros (Phoenix, San Francisco) but still a 5–10 year horizon
> for scale relevant to Les Schwab's footprint."

In the Section 5 moat callout about fleet, add a similar qualifier.

---

### 4. AI Upside Ranges ($78–132M) → Emphasize Base Case, De-emphasize High End

**Problem:** The combined EBITDA range ($78–132M) leads with the upside number. The
"The math" callout also mentions $78–132M first. For IC scrutiny, the base case
($40–60M) should be the anchor; the upside should be the stretch.

**Fix:**
- Reorder the "The math" callout: lead with base case, show upside as secondary
  > "The base case, informed by comparable operational AI deployments in multi-site
  > retail, suggests ~$40–60M in EBITDA contribution is achievable with disciplined
  > execution. The upside scenario — all 3 initiatives at the high end — represents
  > an estimated $78–132M by 2028 (+29–48% of current EBITDA). These are directional."
- In the IC Summary Initiative cards, add `(base: ~$40–60M combined)` as a sub-note
  below the individual initiative EBITDA ranges, or add it to the Recommendation row
- Consider whether the $78–132M in the summary table header should be footnoted more
  prominently as "upside scenario"

---

## Files to Modify

| File | Changes |
|------|---------|
| `app/reports/templates/les_schwab_av.py` | Constants: `_KEY_ASSUMPTIONS` (add ADAS attach row, update alignment value label); Rendering: Section 4 ADAS callout (alignment label + ADAS dependency note), Section 4 fleet callout (AV timing caveat), Section 5 fleet moat callout (timing qualifier), Section 8 "The math" callout (reorder base/upside), IC Summary (base case anchor) |

---

## Implementation Order

1. Update `_KEY_ASSUMPTIONS` (add ADAS attach row, update alignment label)
2. Section 4 ADAS callout — alignment `(modeled est.)` + ADAS dependency note
3. Section 4 fleet callout — AV timing caveat
4. Section 5 moat fleet callout — timing qualifier
5. Section 8 "The math" — reorder to base case first
6. IC Summary Recommendation row — add base case anchor
7. Restart + regenerate + verify
