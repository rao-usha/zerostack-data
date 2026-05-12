# PRD — Litigation Finance Intelligence (LitInt)

**Status:** Draft v1
**Date:** 2026-04-30
**Owner:** Product
**Companion docs:** `NEXDATA_SELLABLE_CAPABILITIES_2026.md`, sister PRDs (DCII / IRADev / Underwrite)

> **See also:** [`PLATFORM_POSITIONING_2026.md`](./PLATFORM_POSITIONING_2026.md) — canonical roll-up across the four vertical PRDs (DCII, IRADev, Underwrite, LitInt). Owns cross-product sequencing (Underwrite → DCII → IRADev → LitInt), the shared 12-month Phase 0 program, bundle pricing, the single MCP investment, and the platform-reconciled Y2 ARR aggregate. **LitInt is sequenced last** because court-access partnerships (PACER via Lex Machina / Bloomberg Law, with RECAP / CourtListener fallback) require 6–12 months of negotiation that cannot be allowed to gate the other three products' timelines.

---

## 1. Executive Summary

Litigation finance is a **$15B AUM and growing**[^1] asset class — funds buy claims and case portfolios from law firms, plaintiffs, and corporates in exchange for a share of recovery. The industry's two structural problems are (a) **adverse selection at intake** (cherry-picking strong cases is hard without data) and (b) **portfolio underwriting / monitoring** (cases are illiquid, multi-year, with opaque defendant solvency and exposure data).

Mass-tort and environmental-claims litigation is particularly data-rich: EPA enforcement records, ACRES contamination, EPA TRI emissions, demographic exposure, OSHA filings, FDA adverse-event reports, USPTO patents, SEC litigation disclosures, and corporate-entity graphs. **Nexdata already collects most of this.** What's missing is the lens — packaging it for litigation-finance underwriters, plaintiff-side firms doing case selection, and mass-tort aggregators.

**Target outcome:** Smaller TAM than DCII/Underwrite but high willingness-to-pay and zero meaningful incumbents. $50K–$300K ACV. 5–8 design partners. $2–4M ARR by end of 2027 *(standalone target — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled Y2 aggregate of $10.5–16.5M across all four products)*. Quickest of the four PRDs to a paying customer once court-access partnership lands; that partnership is the gate (see Phase 0).

[^1]: $15B+ AUM figure: triangulated across Westfleet Insider 2024–25 LF reports, Burford and Omni Bridgeway annual disclosures, and the ABA Section of Litigation 2024 LF survey. Ranges in industry estimates run $13–18B; we use $15B as a conservative midpoint. Re-validate against Westfleet 2026 release before GA marketing.

---

## 2. Target Buyers (ICP)

| Tier | Segment | Examples | Pain | ACV |
|---|---|---|---|---|
| **A** | Litigation finance funds | Burford, Parabellum, Longford, Omni Bridgeway, Therium, Curiam, Validity, Westfleet | Case underwriting + portfolio monitoring + defendant-solvency | $150–300K |
| **A** | Mass-tort aggregators | Tort Network, Mass Tort Marketing firms, settlement administrators | Class-pool sizing + exposure modeling + lead validation | $100–250K |
| **B** | Plaintiff-side environmental / mass-tort firms | Motley Rice, Levin Papantonio, Beasley Allen, Weitz & Luxenberg, Lieff Cabraser | Case-source intel, defendant exposure mapping | $75–200K |
| **B** | E&O / D&O insurance funds | Coverage units inside Nationwide, Chubb, Berkshire, AIG | Corporate exposure underwriting | $200–500K |
| **C** | Class-action plaintiff PI / referral firms | Morgan & Morgan referral teams, Sokolove, Goldwater | Lead intelligence | $50–150K |
| **C** | Corporate defense advisory | Kroll, FTI, AlixPartners, Charles River Associates | Defending against claims; reverse-mapping | $150–400K |

**Beachhead:** Litigation finance funds (Tier A). Smaller buyers (~30 funds globally)[^2], all sophisticated, all willing to pay six figures for any incremental edge. Direct sales reach.

LF funds evaluate both plaintiff-side and defense-side angles when underwriting cases — that's why **the seller-neutral framing is intentional**, not a strategic ambivalence. The product is intel that supports either side's underwriting.

**Governance commitment — direct sales to plaintiff firms (Tier B) and to insurers / defense advisory (Tier B/C) deferred to Phase 3+** until a written governance plan ratifies the dual-side sales motion. The risk is reputational: if a plaintiff firm and a defendant's insurer both buy LitInt and discover the other did, trust erodes on both sides. LF funds as the buyer is the seller-neutral lane that doesn't trigger this conflict.

[^2]: ~30 funds globally: Westfleet Insider tracks ~25 active US LF funds + ~5–10 active international funds (Omni Bridgeway, Therium, Harbour, etc.). The narrow buyer count is *why* LitInt is the smallest-TAM product in the platform but also the highest-conviction-per-logo motion.

---

## 3. Problem Statement

A litigation-finance underwriter evaluating a proposed environmental mass-tort claim today does the following:

1. **Defendant exposure mapping.** Reads EPA ACRES + EPA TRI manually. Cross-references with corporate filings. Pulls historic enforcement actions from EPA Echo. 30+ hours.
2. **Class-pool sizing.** Gets demographics from Census ACS. Maps exposure radius manually. Estimates affected population. Often guesses.
3. **Defendant solvency / asset coverage.** Reads SEC filings (if public). Calls credit analysts. Tracks corporate transactions. Frequently 10-Ks are a year stale.
4. **Parallel-litigation history.** Reads PACER. Calls colleagues. Tracks prior settlements. Industry has no centralized signal layer.
5. **Recovery comp.** "What's a hexavalent-chromium contamination case worth?" — institutional knowledge in 5 partners' heads.
6. **Co-defendant / chain-of-causation graph.** Multi-party tort cases require mapping insurers, subsidiaries, predecessors. Fragmented.

There is no platform. PACER is the closest thing and PACER is barely a database. The market spends $250M+ in analyst hours on workflows that should take minutes.

---

## 4. Product Overview

```
┌─────────────────────────────────────────────────────┐
│ Layer 4 — Litigation-finance workflows              │
│ Case intake · Defendant intel · Portfolio monitor   │
│ Recovery comps · Co-defendant graph · Settlement    │
├─────────────────────────────────────────────────────┤
│ Layer 3 — Litigation-specific intel                 │
│ Enforcement · Exposure · Solvency · PACER signals   │
├─────────────────────────────────────────────────────┤
│ Layer 2 — Public-data fusion (shared with others)   │
│ EPA · OSHA · FDA · SEC · USPTO · Census · BLS       │
├─────────────────────────────────────────────────────┤
│ Layer 1 — Existing Nexdata + governance             │
└─────────────────────────────────────────────────────┘
```

### 4.1 The five workflows the product supports

| # | Workflow | Persona | What they do today | What LitInt gives them |
|---|---|---|---|---|
| 1 | **Case intake / underwriting** | LF analyst | 30+ hours per case | Defendant exposure, class size, solvency, parallel-litigation in one report |
| 2 | **Defendant intel** | LF analyst / firm partner | Manual EPA / SEC / PACER scrap | Live defendant graph: subsidiaries, insurers, contamination, enforcement |
| 3 | **Portfolio monitor** | LF asset mgr | Manual annual review | Continuous re-scoring on defendant solvency, parallel litigation, settlement comps |
| 4 | **Recovery comp engine** | LF underwriter | Partner gut + spreadsheets | Recovery comps by defendant type, peril, jurisdiction, vintage |
| 5 | **Mass-tort class sizing** | Aggregator / firm | Census + back-of-envelope | EPA exposure radius × ACS demographics × NIBRS / OSHA / FDA exposure |

### 4.2 Output formats

- Web app (case-by-case interactive analysis)
- Case report (PDF + XLSX, branded — for fund IC memos)
- API + GraphQL (for LF funds with internal models)
- Quarterly Mass-Tort Activity Report (HTML/PDF, lead-gen)
- Watchlist alerts (email/Slack/webhook)

---

## 5. Feature Requirements

### 5.1 Defendant Intelligence

| ID | Feature | Priority | Notes |
|---|---|---|---|
| DI-1 | Corporate entity graph: parent/sub/affiliate/predecessor | M | Reuse from DCII shell-LLC work |
| DI-2 | Insurer mapping (D&O, E&O, GL, environmental impairment) | M | New work — disclosure mining |
| DI-3 | EPA enforcement history (Echo) | M | Existing |
| DI-4 | EPA contamination history (ACRES, NPL, Superfund) | M | Existing |
| DI-5 | EPA toxic releases (TRI) | M | Existing |
| DI-6 | OSHA enforcement + violation history | M | New ingest |
| DI-7 | FDA adverse-event reports (FAERS, MAUDE) | M | New ingest, drug + device |
| DI-8 | NHTSA recall + complaint database | S | New ingest |
| DI-9 | CFPB complaint database (financial defendants) | S | New ingest |
| DI-10 | SEC litigation disclosures (10-K, 10-Q, 8-K legal proceedings sections) | M | NLP extraction on existing SEC data |
| DI-11 | Defendant solvency score: SEC + bank covenants + news + bond spreads | M | Composite |
| DI-12 | Defendant insurance program inference | S | **Governance-load-bearing.** Outputs scored with explicit confidence bands (low/medium/high) and provenance. Restricted by license and product-architecture to litigation-finance underwriting use cases; **NOT exposed to insurer customers as competitive intelligence** (avoids the dual-sale conflict — selling defense-side insurer profiles to plaintiffs while also selling LitInt to insurers). Inference layer is also the most ethically gray feature in the entire LitInt surface; ship behind a feature flag and revisit if any LP / regulator raises concerns. |

### 5.2 Case Intake / Underwriting

| ID | Feature | Priority | Notes |
|---|---|---|---|
| CI-1 | Case-type intake form: tort type, jurisdiction, defendants, exposure period | M | |
| CI-2 | Auto-pull defendant graph + exposure data | M | |
| CI-3 | Class-size estimator: exposure radius × ACS demographics | M | |
| CI-4 | Parallel-litigation lookup (PACER ingest) | M | New ingestion |
| CI-5 | Settlement-comp suggestions (recovery range by tort type, jurisdiction) | M | Hand-seeded comp set |
| CI-6 | Statute-of-limitations check by jurisdiction × tort | S | Legal-data partner |
| CI-7 | Generate one-click intake memo (PDF) | M | |

### 5.3 Portfolio Monitor

| ID | Feature | Priority | Notes |
|---|---|---|---|
| PM-1 | Per-case continuous re-score on new defendant data | M | |
| PM-2 | Solvency-deterioration alerts (defendant rating downgrade, bond widening) | M | |
| PM-3 | Parallel-litigation alerts (new filings against same defendant) | M | |
| PM-4 | Settlement-event alerts (defendant settles a related case) | M | |
| PM-5 | Portfolio aggregate dashboards (by defendant, by jurisdiction, by tort type) | M | |
| PM-6 | Reserve-recommendation modeling | C | Out of scope Phase 1 |

### 5.4 Recovery Comp Engine

| ID | Feature | Priority | Notes |
|---|---|---|---|
| RC-1 | Settlement comp database: tort type × jurisdiction × class size × era | M | Hand-seeded, expand over time |
| RC-2 | Comp filtering (by defendant industry, peril, multi-defendant flag) | M | |
| RC-3 | Verdict comp database (going-to-trial cases) | M | |
| RC-4 | Insurance-coverage influence on settlement | S | |
| RC-5 | Time-to-resolution distribution by tort type | M | |

### 5.5 Mass-Tort Class Sizing

| ID | Feature | Priority | Notes |
|---|---|---|---|
| MT-1 | Exposure-radius modeling (chemical plumes, contamination footprint, drug-prescription density) | M | |
| MT-2 | Population-at-risk estimator (ACS demographics × exposure radius) | M | |
| MT-3 | Disease-prevalence overlay (CDC, BRFSS, NHANES) | S | |
| MT-4 | Lead-validation tools (for tort-aggregator clients) | S | |

### 5.6 Co-Defendant / Chain-of-Causation Graph

| ID | Feature | Priority | Notes |
|---|---|---|---|
| CD-1 | Multi-party tort graph: defendants, suppliers, distributors, contractors | M | |
| CD-2 | Asbestos / chemical / pharmaceutical chain-of-distribution graph | M | Per-tort domain |
| CD-3 | Insurer / reinsurer attachment graph | S | Difficult — coverage litigation territory |

---

## 6. Data Sources

### Already built (reuse)
- EPA Echo, EPA ACRES, EPA TRI, EPA NPL/Superfund
- SEC EDGAR (10-K legal proceedings, 8-K)
- Census ACS (demographics)
- USPTO (patents — for IP-litigation cases)
- BLS QCEW
- Org chart + people stack
- Reports / audit / lineage / governance stack

### New to build for LitInt
- **PACER scraper** (federal court filings) — high priority; many 3rd-party APIs available (Bloomberg Law, Lex Machina) — license rather than scrape
- **OSHA** ingestor (enforcement actions, violations)
- **FDA FAERS / MAUDE** (adverse events for drug + device cases)
- **NHTSA** (auto recall + complaint)
- **CFPB** (consumer-finance complaint)
- **State court records**: trickier — state-by-state, license via Lex Machina / Bloomberg Law / Trellis
- **Settlement comp database** — hand-curated from case law + news + filings
- **Verdict comp database** — partner with Verdictsearch / Bloomberg Law
- **Defendant solvency model** — combine SEC + bond spreads + news
- **Insurer mapping** — disclosure mining (D&O policies disclosed in proxy filings; E&O harder)

### Required partner / license
- **Lex Machina (LexisNexis) or Bloomberg Law** — PACER + state court access. Preferred path.
- **Fallback if Lex Machina / Bloomberg Law partnership terms are unworkable: RECAP archive + CourtListener (Free Law Project)** for federal coverage. RECAP is a community-archived PACER mirror; CourtListener provides a queryable API. Coverage gaps are real (state courts almost entirely missing; federal coverage is partial and lags live PACER) but the fallback is **viable enough to launch Phase 1** without a paid partnership. Phase 0 negotiation runs in parallel with the RECAP/CourtListener integration so we don't gate launch on a deal we don't control.
- **Verdictsearch** or equivalent — verdict comp data. Highly recommended.
- **CCH IntelliConnect / Westlaw** — possible legal-data partner.

---

## 7. Why Perplexity / Computer Cannot Do This

1. **PACER and state court data** are licensed, structured, and not "browseable" in any reliable way. Requires partnership.
2. **Defendant solvency models** require multi-source historical signal. Not a one-shot agent run.
3. **Settlement comp accuracy** matters enormously — wrong comps = mispriced cases = LP losses. Computer averages press-release noise; we curate.
4. **Insurer mapping** requires disclosure mining + inference layer. Not a search.
5. **Confidentiality / professional responsibility.** LF firms have ethics-rule constraints on tools that hallucinate. Audit trail + provenance is non-negotiable.

---

## 8. Differentiation vs. Existing Players

| Competitor | What they do | Where LitInt wins |
|---|---|---|
| **Lex Machina (Lexis)** | Court analytics + outcomes data | We focus on defendant-side intel + LF underwriting; partnership candidate for PACER access |
| **Bloomberg Law** | Legal research + analytics | Same as above; possibly competitive at the analytics layer |
| **Premonition / Justia** | Court analytics | Smaller; we'd compete by depth on defendants |
| **PinPoint (Burford-internal)** | Burford's own platform | Internal; we serve smaller LF funds without dev resources |
| **Verdictsearch** | Verdict + settlement comp database | Partnership / data feed candidate |
| **In-house LF teams** | Hand-rolled spreadsheets | Replace |

**Strategic posture:** Position as **the LF underwriter's all-in-one intake + monitor product**, layered on top of legal-data partners (Lex Machina / Bloomberg Law for court access) plus our governed public-data warehouse. We are the **missing tier-1 product for litigation finance** — Lex Machina is too academic, Bloomberg Law is too generalist, internal tools are bespoke.

---

## 9. GTM

### Beachhead motion
- 8 named-account outbound at LF funds (Burford, Parabellum, Longford, Omni, Therium, Curiam, Validity, Westfleet). Direct, partner-level.
- Demo: take a real environmental case, show defendant graph + class size + comps + solvency in 60 seconds. The demo is the close.

### Channel plays
- **LF Journal / PR Newswire LF beat** — small, focused trade press. Get cited.
- **AALA (American Litigation Funders Association)** annual conference. Sponsor a session.
- **AILAS (Association of International Litigation & Arbitration Specialists)** — international LF reach.
- **Plaintiff-firm partnerships** — Motley Rice, Levin Papantonio etc. Co-marketed product to their LF partners.
- **Verdictsearch / Lex Machina partnerships** — they bring legal data, we bring exposure data. Joint sale.

### Pricing
| Tier | ACV | Includes |
|---|---|---|
| **Plaintiff Firm** | $75K | 5 seats, web app, intake reports (50/yr), defendant intel |
| **LF Fund** | $150–250K | 15 seats, all of Plaintiff + portfolio monitor + comps + alerts + API |
| **Enterprise** | $300K+ | Unlimited seats, all of LF Fund + governed warehouse, lineage/audit, SSO |

### Success metrics
- 5 design partners by 2026-Q4 (target: 2 LF funds + 2 plaintiff firms + 1 mass-tort aggregator)
- $1M ARR by 2027-Q1
- $3–4M ARR by 2027-Q4 *(standalone — see `PLATFORM_POSITIONING_2026.md` §7 for the platform-reconciled aggregate)*
- 1 published joint case study with an LF fund (will accelerate sales 10x in this clubby market)

### 9.5 Mass-tort PI lead-aggregation (under-weighted in Tier C — re-elevate)

The original Tier C placement of "class-action plaintiff PI / referral firms" (Morgan & Morgan referral teams, Sokolove, Goldwater) understates the actual buying volume in this segment. Mass-tort lead-aggregation is a quietly large business:

- **Single-event volume:** 3M Combat Arms earplugs MDL — ~250,000+ filed claims; Camp Lejeune CLJA — ~1.5M+ potential class members; Roundup litigation — ~125,000+ settled claims to date. A handful of firms intermediate the pipeline of leads from media buy → intake → settlement administration.
- **Top-5 mass-tort firms** (Morgan & Morgan, Goldwater, Sokolove, Kirsch & Volpe, Sweetwater) collectively run nine-figure annual marketing budgets and operate intake-velocity workflows that benefit directly from LitInt's exposure-radius modeling, defendant solvency layer, and settlement-comp database.
- **Buying behavior** differs from Tier A LF funds: mass-tort firms care about validating leads at intake (does this claimant's exposure actually fit the class?), pricing media buys against expected per-lead value, and forecasting settlement-administration timing. Per-firm ACV is lower ($50–150K) but the buyer pool is larger (~30–50 active firms US) and decision cycles are short.

**Action:** Mass-tort PI lead-aggregation moves from Tier C to **Tier B** in the next ICP revision. Phase 1 demos should include a mass-tort exposure-radius case alongside the LF-fund underwriting case. The lead-validation feature MT-4 is promoted from S to M priority.

### 9.6 Cross-sell to other Nexdata products

LitInt is sequenced last; by GA most accounts will already be Nexdata customers via Underwrite or DCII. Per `PLATFORM_POSITIONING_2026.md` §6, multi-product MSAs price at ~25% off for two products and ~30% for three+. LitInt cross-sell is largely "the customer already exists, add the SKU."

| LitInt account | Also fits | Why |
|---|---|---|
| D&O / E&O coverage units inside Nationwide, Chubb, Berkshire, AIG (Tier B) | **Underwrite** | Same insurer parents already buying Underwrite for property + cyber; D&O / E&O is an adjacent line. LitInt's defendant-exposure modeling feeds the D&O underwriting workflow. **(Cross-sell gated on the §2 governance plan — see beachhead commitment.)** |
| Burford, Parabellum, Longford (LF funds with infra exposure) | **DCII**, **IRADev** | Some LF funds run portfolios that include infra-litigation cases (queue-position disputes, IRA credit-clawback fights). DCII / IRADev data is directly useful for those underwriting workflows. Lower-probability bundle but real motion. |
| Mass-tort firms underwriting environmental claims | **Underwrite** | Mass-tort plaintiff firms and Underwrite's parametric / commercial property buyers don't overlap as buyers, but the underlying defendant-graph data does. Single-database, two-product motion possible. |
| Reinsurers (Munich Re, Swiss Re — already Underwrite Phase 3 targets) | **Underwrite** | Reinsurers carry mass-tort accumulation exposure; LitInt's cross-defendant graph is directly useful for treaty-cycle exposure modeling. Strong overlap with the reinsurer Phase 3 motion in Underwrite. |

**Action:** Tag every LitInt design-partner contract with provenance from which sister product (Underwrite / DCII / IRADev) the relationship originated, so the platform-attribution data feeds back into the bundle-pricing motion. LitInt is unlikely to be the first product sold to any account; it's the platform-completion product.

---

## 10. Phasing

### Phase 0 — Foundation (now → 2027-Q1, 12 months)

Phase 0 length extended from the original 6 months to 12 months to align with `PLATFORM_POSITIONING_2026.md` Phase 0 program. The driver is court-access partnership: 6 months is unrealistic for a Lex Machina / Bloomberg Law deal. Two parallel workstreams so launch is not gated on a single negotiation:

**Workstream A — Court-access partnership (long lead, 6–12 months, cannot gate Phase 1)**
- Lex Machina (LexisNexis) or Bloomberg Law partnership negotiation for PACER + state court access
- Decision gate at 2026-Q4: if either partnership is on track to close, defer the RECAP/CourtListener integration. If neither, accelerate Workstream B.

**Workstream B — Build-it-ourselves fallback (do this in parallel, do not wait)**
- RECAP archive integration (federal coverage, community-mirrored PACER docket scraping)
- CourtListener API integration (Free Law Project; queryable federal docket index)
- Acknowledged coverage gaps: state courts largely absent in fallback path; federal coverage lags live PACER by hours-to-days. Acceptable for Phase 1.

**Other Phase 0 workstreams (independent of court access)**
- OSHA ingestor
- FDA FAERS/MAUDE ingestor
- NHTSA ingestor
- SEC legal-proceedings NLP extractor
- Defendant entity-graph extension (can leverage DCII's hyperscaler shell-LLC entity-resolution work since DCII's Phase 0 ships the underlying graph layer)

### Phase 1 — LitInt MVP (2026-Q4)
- Defendant Intel + Case Intake workflows
- Mass-tort class sizing
- 3 design partners

### Phase 2 — Underwriting depth (2027-Q1 → Q2)
- Recovery comp database (hand-seeded)
- Defendant solvency model
- Portfolio monitor
- 5+ design partners

### Phase 3 — Enterprise + adjacent (2027-Q3+)
- Governed warehouse SKU
- Verdict comp database
- D&O / E&O insurer SKU **(only after governance plan ratifies dual-side sales motion — see §2 beachhead governance commitment)**
- MCP server: shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11. Ship the LitInt MCP surface (defendant graph, EPA enforcement, mass-tort exposure modeling, settlement comps) on the unified MCP at the same time as the other three product surfaces; do not stand up a LitInt-specific MCP.

---

## 11. Risks & Open Questions

| # | Risk / Question | Mitigation |
|---|---|---|
| R1 | PACER / court-access partnerships expensive | Negotiate hard; consider revenue-share with Lex Machina |
| R2 | Smaller TAM than other PRDs (~30 LF funds) | High ACV per logo + adjacent expansion (plaintiff firms, mass-tort, D&O) |
| R3 | Ethical / professional-responsibility concerns about predictive tools in legal work | Lean into provenance + audit; don't make causal claims, only signal aggregations |
| R4 | Defendant push-back: "you're helping plaintiffs" PR | Sell to defense-side too (Phase 4); product is intel, not advocacy |
| R5 | Comp database quality is hand-built and labor-intensive | Allocate 1 ops + 0.5 attorney FTE; partner with Verdictsearch |
| Q1 | Should we sell to defense-side firms (Kroll, FTI)? | Yes, post-MVP — they have budget |
| Q2 | International expansion (UK, Australia LF markets large) | Phase 4 |
| Q3 | Should we publish public mass-tort indexes for marketing? | Yes — quarterly free reports = lead-gen |

---

## 12. Shared Infra with Sister PRDs

LitInt shares:
- EPA + ACRES + TRI + Echo ingestors (same as Underwrite)
- SEC ingestor
- Census ACS (demographics)
- Org chart / people stack (defendant leadership)
- Reports / audit / lineage / governance stack
- Unified MCP server (shared cross-product investment — see `PLATFORM_POSITIONING_2026.md` §11; LitInt exposes its defendant-graph / mass-tort / settlement-comp surface on the same MCP, not a product-specific one)

**Net new for LitInt:**
- PACER + state court partnership (largest item)
- OSHA, FDA, NHTSA, CFPB ingestors
- Settlement + verdict comp databases (curated)
- Insurer-mapping module
- Defendant solvency composite

**~50% of the engineering is shared.** The expensive new piece is court-data access (partnership cost, not engineering).

---

## 13. What Success Looks Like

A LF underwriter at Burford evaluates a proposed mass-tort case against a chemical-manufacturing defendant. She opens LitInt, types the defendant name. Up returns: full corporate-entity graph (3 subsidiaries + 1 predecessor + 2 D&O insurers identified), 14 EPA Echo enforcement actions across 6 facilities, 3 ACRES contamination sites within the alleged exposure period, exposed population estimate (47,000 within 5 miles of the 6 facilities, 23% of which match the demographic profile alleged), 4 parallel cases in 3 jurisdictions, comps by tort type ($85M–$280M), and a defendant solvency composite of 6.2/10 (declining; bonds widening). The IC memo writes itself — sourced, audit-trailed, defensible. 4 hours of analyst work in 6 minutes.

That is the product.

---

## 14. Sources / Internal References

- `app/sources/epa/` — EPA ingestors (Echo, ACRES, TRI)
- `app/sources/sec/` — SEC EDGAR
- `app/sources/census/` — ACS demographics
- `app/core/people_models.py` — entity / org-chart graph
- `docs/strategy/PRD_INSURANCE_UNDERWRITING.md` — sister PRD (shares EPA / ACRES / governance stack)
- `docs/strategy/NEXDATA_SELLABLE_CAPABILITIES_2026.md` — origin
