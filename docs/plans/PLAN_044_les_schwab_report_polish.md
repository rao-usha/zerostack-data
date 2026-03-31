# PLAN_044 — Les Schwab Report: Investment-Grade Polish Pass

## Additional Note from User
Initiative 1 data infrastructure investment ($500K–$1M) should be listed as **TBD** —
agentic development tools (e.g., AI-assisted data warehouse build) may compress costs
materially below traditional consulting-led timelines. Do not preview specific numbers.
The $0.6–1.2M investment figure and the combined total should both show TBD in the report.

---

## Source
ChatGPT peer review of the current report (report #208). Flagged as "already high quality"
but identified 6 structural improvements to make it "defensible in partner discussion."

---

## What We're Fixing (in priority order)

### 1. TLDR / Executive Summary (new Section 0)
**Problem:** Report dives straight into the business profile. A partner reviewing before
the IC meeting needs a 30-second read.

**Fix:** Add a compact executive summary section **above the KPI strip** (or as the
first section before the TOC), containing exactly 5 bullets:
- **Risk:** The one-sentence threat
- **Opportunity:** The one-sentence upside
- **Action:** What Meritage should do now
- **Timing:** Exit window guidance
- **Recommendation:** Buy / Hold / Sell with conditions

Design: a dark-background banner card below the page header, visually distinct from the
numbered sections. Not a full section — no section number, no TOC entry.

---

### 2. Key Assumptions Box (new, early in report)
**Problem:** Critical assumptions — EV brake reduction (40–60%), tire wear increase
(20–30%), 1.4M alignments/yr — are buried in prose across multiple sections.
If a partner pushes back on the numbers, the reader can't quickly locate the basis.

**Fix:** Add a "Key Modeling Assumptions" callout or table **at the end of Section 1**
(Business Profile), before the narrative dives into scenarios. Format:

| Assumption | Value | Basis | Sensitivity |
|-----------|-------|-------|-------------|
| EV brake wear reduction | 40–60% | Real-world EV fleet data (Bolt, Model 3) | ±10pp changes brake GP by ~$10M |
| EV tire wear premium | +20–30% vs ICE | Continental AG / Michelin fleet data | Low sensitivity to total rev |
| Annual alignments (est.) | ~1.4M | $168M rev ÷ ~$90 ASP | ±200K changes ADAS upside by ±$16–30M |
| ADAS calibration rate (full rollout) | 100% | Upper bound; base case ~50% | 50% penetration = $56–$105M |
| AV fleet impact horizon | 2033–2040 | Wood Mackenzie / BloombergNEF | Key swing factor in bear case |

---

### 3. Directional Language Tightening (Sections 3, 6, 8)
**Problem:** Specific numbers ($78–132M EBITDA, $112–210M ADAS opportunity) are
presented without consistent "illustrative / order-of-magnitude" framing. An IC partner
will treat these as commitments unless they're clearly flagged as directional.

**Fix:** Audit every financial figure in Sections 3, 6, and 8 and apply consistent
qualifier language:
- Section 3 revenue projections table: add footnote "Directional model; ±20% range"
- Section 6 scenario table: add "illustrative" to the header row
- Section 8 initiative tables: already use "Directional Range" column — add a single
  disclaimer sentence above the combined summary table
- KPI strip: "Brake Revenue at Risk" card delta text — add "est." prefix

Do NOT soften Section 1 or 2 — those are grounded in public data.

---

### 4. Execution Risks Subsection (in Section 7)
**Problem:** Section 7 (Watchpoints) covers market risk signals but says almost
nothing about execution risk. A partner will ask: "What can management screw up?"

**Fix:** Add a short "Execution Risks" block at the **end of Section 7**, after the
action callouts. Four risks, each 2 sentences:

1. **ADAS Rollout Complexity** — calibration requires dedicated equipment ($15–25K/bay),
   technician training, and liability exposure if calibration is done incorrectly.
   A botched rollout creates reputational risk that outweighs the revenue upside.

2. **Fleet Sales Capability Gap** — fleet B2B requires a different sales motion
   (contracts, invoicing, SLAs) that Les Schwab's retail-trained team doesn't have today.
   Hiring 3–5 fleet reps is table stakes; building the back-office is the harder part.

3. **Store Manager Adoption Risk (AI)** — AI store briefings only create value if
   store managers read and act on them. Rollout requires change management, not just
   a software deployment. Org culture at Les Schwab is strong — that's an asset, but
   also means change takes longer.

4. **Data Quality Bootstrap Problem** — Initiative 1 depends on connecting POS data
   to a structured database. If Les Schwab's POS systems are fragmented (common in
   regional chains), Track A takes 6–12 months longer than planned, delaying Track B
   GenAI value by the same amount.

---

### 5. Prose Reduction (Sections 4 & 8)
**Problem:** Both sections have ~20–30% more words than needed. The tables carry the
analytical weight; the prose just needs to set them up.

**Fix:**
- **Section 4** (Strategic Response): Cut the two long callout paragraphs at the bottom
  by ~40%. Keep the ADAS callout; trim the fleet callout to 2 sentences.
- **Section 8** (AI Initiatives): Each initiative narrative is currently 4–6 sentences.
  Cut to 3–4. The financial impact tables do the work.

---

### 6. Reduce EV/AV Timing Repetition
**Problem:** The EV-near-term / AV-long-term framing appears in the intro of Sections
2, 3, and 6. The third occurrence is redundant.

**Fix:** Keep the full framing in Section 2 (where it belongs). In Section 3 and
Section 6, replace the repeated framing with a single back-reference: "As established
in Section 2, the EV headwind is a 5–10 year story while AV ownership impacts are
a 2033+ horizon." One sentence, not a paragraph.

---

## Files to Modify

| File | Sections Changed |
|------|-----------------|
| `app/reports/templates/les_schwab_av.py` | Section 1 (assumptions box), Section 3 (directional qualifier), Section 4 (prose trim), Section 6 (directional qualifier), Section 7 (execution risks), Section 8 (prose trim + AI credibility note) |

Also need:
- New constants for the TLDR bullets and assumptions table
- TLDR rendered as a styled banner between `page_header()` and `kpi_strip()`

---

## Implementation Order

1. TLDR banner (new code, no risk to existing sections)
2. Key Assumptions box (add to end of Section 1)
3. Directional language pass (Sections 3, 6, 8 — targeted string replacements)
4. Execution Risks block (add to end of Section 7)
5. Prose trim (Sections 4 & 8)
6. EV/AV repetition reduction (Sections 3 & 6)
7. Restart + regenerate + verify

---

## Design Notes

**TLDR banner** — sits between page header and KPI strip. Dark slate background
(`#1a202c`), white text, 5 labeled bullet rows. Not a `section_start()` card —
custom HTML block. Width matches `.container`.

**Key Assumptions table** — rendered via `data_table()` with a 4-column layout.
Preceded by a neutral callout: "The financial projections in this report are
directional estimates built on the following key assumptions."

**Tone standard** — all prose should read as: analyst presenting a framework,
not consultant selling a conclusion. "May represent" not "represents".
"Could deliver" not "will deliver."
