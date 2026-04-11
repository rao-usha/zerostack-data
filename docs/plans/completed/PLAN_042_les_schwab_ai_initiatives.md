# PLAN_042 — Les Schwab Report: AI Initiatives Section (3 Initiatives, Rev/Cost/EBITDA)

## Problem

Section 8 of the Les Schwab report currently presents a Crawl/Walk/Run AI roadmap but:
- Year 1 (Crawl) reads as pure data/BI with almost no GenAI — not compelling enough
- No cost savings dimension — MD is specifically asking about Y1 cost impact
- Language is too definitive for what is inherently a directional analysis
- Initiatives lack a clean, comparable financial summary table per initiative

---

## What We're Building

Replace the current Section 8 with **three succinct, parallel AI initiatives** — each framed
as a directional opportunity (not a commitment) with a consistent financial impact structure:

```
Initiative | Horizon | Revenue Impact | Cost Savings | EBITDA Contribution
```

The key design insight from the conversation:
- **Year 1 runs two tracks in parallel**: data infrastructure AND GenAI analysis on existing data
- Claude-powered weekly store briefings deploy in ~30 days on imperfect data — GenAI from Day 1
- Data quality improves continuously; AI quality follows automatically
- Language stays directional ("may represent", "could deliver", "we estimate") — not a business case commitment

---

## The Three Initiatives

### Initiative 1 — Operational Intelligence & AI-Assisted Store Briefings (2026)

**Two parallel tracks:**

**Track A — Data Foundation** ($500K–$1M, 3–6 months)
- Connect POS/service management to structured database
- 6 core KPIs tracked weekly per store with market overlays (AFDC EV density, FRED signals)
- The prerequisite that sharpens everything downstream

**Track B — Claude Weekly Store Analysis** ($100–200K build, ~$3K/yr running)
- Deploy immediately on existing POS data — no perfect data required
- Claude generates a weekly one-page briefing per store: key observations, 3 prioritized actions, market context
- As Track A delivers better data, briefings automatically improve
- By Month 12: Claude is comparing this week vs. same week LY with full KPI + macro context

**Financial impact:**
- Revenue: $5–12M (faster issue identification → reduced leakage; managers act in days not quarters)
- Cost savings: $18–25M (labor scheduling efficiency ~$12–16M, parts waste ~$3–5M, rework ~$2M, management overhead ~$1–2M)
- EBITDA contribution: **$23–37M**
- Investment: $600K–$1.2M
- Payback: <12 months

---

### Initiative 2 — Agentic Cross-Store Learning (2026–2027)

Takes the structured data from Initiative 1 and runs an agent that:
- Identifies top-quartile patterns and propagates them to lagging stores
- Generates specific, store-level next best actions grounded in Nexdata signals
- Monitors 500 stores simultaneously — something no regional manager can do
- Learns from outcomes: which recommendations moved the metrics?

**Financial impact:**
- Revenue: $15–42M (closing top/bottom store performance gap on ADAS attach, EV tire mix)
- Cost savings: $7–10M (warranty/returns ~$4–6M, reduced field supervision ~$1–2M, training efficiency ~$1–2M)
- EBITDA contribution: **$22–52M incremental**
- Investment: $1–3M
- Note: This initiative is only possible because Initiative 1 built the data foundation

---

### Initiative 3 — Passive Data Collection & Predictive Intelligence (2027–2028)

Once reporting + agentic layers are proven, instrument stores for passive capture:
- License plate recognition at bay entry → VIN lookup → auto-populated check-in
- Telematics API integration for fleet customers → predictive tire replacement windows
- Competitive price feed, NOAA weather overlay, per-VIN ML predictions
- Fleet B2B portal: auto-scheduling, fleet manager dashboards

**Financial impact:**
- Revenue: $18–140M (fleet contracts, predictive outreach, ADAS capture at scale)
- Cost savings: $15–18M (LPR automation ~$3M, planned vs. rush parts ordering ~$8–9M, OT reduction ~$4–6M)
- EBITDA contribution: **$33–43M incremental**
- Investment: $4–8M

---

## Combined Financial Summary (2028)

| Initiative | Revenue | Cost Savings | EBITDA | Investment |
|------------|---------|-------------|--------|-----------|
| 1 — Store Intelligence | $5–12M | $18–25M | $23–37M | $0.6–1.2M |
| 2 — Agentic Learning | $15–42M | $7–10M | $22–52M | $1–3M |
| 3 — Passive + Predictive | $18–140M | $15–18M | $33–43M | $4–8M |
| **Total** | **$38–194M** | **$40–53M** | **$78–132M** | **$5.6–12.2M** |

As % of current EBITDA ($273M): **+29–48% potential uplift by 2028**

The Initiative 1 Year 1 cost savings ($23–37M) alone cover the entire 3-year program investment.

---

## Files to Modify

| File | Action | Description |
|------|--------|-------------|
| `app/reports/templates/les_schwab_av.py` | Modify | Replace Section 8 constants + rendering with 3-initiative structure |

No other files need changing. Section 8 is self-contained in the template.

---

## Design Principles for the Section

1. **Directional language throughout** — use "may", "could", "we estimate" for financial projections
2. **Consistent per-initiative structure** — narrative → financial impact table → callout
3. **Track A/Track B framing for Initiative 1** — makes the parallel deployment explicit
4. **Keep it succinct** — 3 initiatives, one page each, no sub-sub-sections
5. **Live Nexdata signals cited inline** — FRED, AFDC, BLS as before
6. **Summary table at section end** — single view of Rev/Cost/EBITDA across all 3

---

## Implementation Order

1. Replace `_CWR_*` constants with new `_AI1_*`, `_AI2_*`, `_AI3_*` constants
2. Replace Section 8 rendering with 3-initiative structure
3. Restart API + regenerate + verify

---

## Tone Notes

The current section reads like a consulting deliverable with high conviction.
The MD will read this and push back if numbers are too specific.
Target tone: "Here is a directional framework for how AI could create value —
the specific numbers will depend on implementation choices and execution quality."
