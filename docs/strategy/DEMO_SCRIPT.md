# Nexdata Demo Scripts
**Last updated:** 2026-03-23
**Target runtime:** ~5 minutes per story

---

## Pre-flight checklist
- [ ] API running: `curl http://localhost:8001/health`
- [ ] Seed PE data: open pe-demo.html → click "Seed Demo Data" if portfolio is empty
- [ ] Seed DC data: open dc-demo.html → click "Seed Demo Data" if county scores are empty
- [ ] Browser: open both demo files from the Reports tab in index.html

---

## Story 1 — Acquisition (PE Deal Flow)
**Open:** `pe-demo.html` → click **Acquisition** toggle

### Step A1: Market Scanner (~45s)
> "We start by scanning the market. This isn't a static database — it's a live benchmark pull.
> The firm's IRR is [X]% versus Cambridge's PE median of [Y]%. We're in the [quartile] quartile.
> That means we're performing above/below the median, which shapes how aggressive we can be
> on deal pricing right now."

**Click:** Scan Targets →

### Step A2: Target Discovery (~1m)
> "Here are today's highest-scored acquisition candidates — ranked by our AI deal score,
> not a sales rep's pitch. Each one is scored across [28] public data sources: financials,
> job postings, leadership, competitive signals. A score of 75+ is our buy threshold.
> [Point to top result] — [Company Name] scores [X] with strengths in [Y] and [Z].
> Notice risks are right there too. No surprises in diligence."

**Click:** Deep Dive → on the top-scoring company

### Step A3: Company Deep Dive (~1m 30s)
> "Now we're looking at an AI-generated investment thesis for [Company].
> This was built in real-time from 28 public data sources — not a template, not a cached summary.
> [Read first sentence of thesis].
> This is the kind of one-pager that would take a junior analyst two days to produce.
> We're generating it on demand, for every target in our pipeline."

**Click:** Add to Pipeline →

### Step A4: Add to Pipeline (~30s)
> "One click. [Company] is now tracked in our deal pipeline with stage 'prospecting'
> and deal type 'acquisition'. Win probability scoring runs overnight.
> Tomorrow morning, the team sees a probability estimate and a recommended next action —
> call management, request financials, or pass."

---

## Story 2 — Exit / Disposition (Default flow)
**Open:** `pe-demo.html` → **Exit / Disposition** is active by default

### Step 1: Portfolio Overview (~45s)
> "This is Summit Ridge Partners' current portfolio. [X] companies, [Y] total deals.
> The health indicators update daily from our collection pipeline.
> We look at growth rate, margin trend, employee signal, and deal velocity together —
> not just EBITDA."

**Click:** Scan for Exit Candidates →

### Step 2: Exit Candidates (~1m)
> "Our exit scoring model ranks every portfolio company across 40+ signals.
> [Company] scores [X] — a grade [A/B]. Strong thesis: [read strength].
> The watch item is [risk] — we know that before the buyer does.
> That's not a surprise in the data room; that's a managed narrative."

**Click:** Deep Dive → on top-ranked company

### Step 3: Exit Planning (~1m 15s)
> "This is the exit prep view. Three things buyers care about:
> team quality, business momentum, and the story.
> [Point to company thesis] — AI-generated from the same 28 sources a buyer will look at.
> We see what they see before they see it."

**Click:** Fund Performance →

### Step 4: Fund Performance / LP Reporting (~45s)
> "When this exit closes, the LP report is already written.
> Blended IRR: [X]%. TVPI: [Y]x. Here's the per-fund breakdown —
> [Fund name] vintage [year] at [Z]% IRR.
> Nexdata doesn't just help you find and exit deals.
> It automates the reporting that comes after."

**Click:** Leadership Network →

### Step 5: Leadership Network (~30s)
> "Finally — the org chart and network graph for [Company].
> This shows you who's connected to whom, and where the key-person risk lives.
> Pre-exit, we flag the 2-3 people whose departure would materially affect the deal price."

---

## Story 3 — Datacenter Site Selection
**Open:** `dc-demo.html`

### Step 1: Market Overview (~1m)
> "We start with a state-level scan. Select Texas.
> [KPI cards load] — [X] counties scored, [Y] with power capacity above threshold.
> Our scoring model pulls from 18 data sources: grid capacity, fiber density,
> permit speed, labor market, cost incentives. One composite score per county.
> No consultant hours. No RFP process. Just data."

**Click:** Select Texas → View Rankings →

### Step 2: County Rankings (~1m)
> "Here are Texas counties ranked by overall site score.
> [Andrews County] — score [X], grade [Y]. Strong on [regulatory / risk scores].
> You can sort by any dimension.
> A typical site selection process takes 6-12 months and $500K in consulting fees.
> We get you to a shortlist in 30 seconds."

**Click:** [Top county] → Deep Dive →

### Step 3: Site Deep Dive (~1m 30s)
> "Now we're inside [County]. Score breakdown: power [X], connectivity [Y], regulatory [Z].
> Raw metrics: [power capacity MW], [fiber providers], [permit days].
> [Click Generate Thesis] — this calls our LLM with the full site profile.
> [Thesis loads] This is a three-paragraph investment thesis for this specific location.
> That's what goes in the board memo."

**Click:** Add to Pipeline →

### Step 4: Site Pipeline (~30s)
> "One click to track this site.
> The pipeline shows every site we're evaluating, with status and target MW.
> Filter by state, sort by score. This is your real-time shortlist —
> not a spreadsheet someone updates once a quarter."

---

## Closing line (all stories)
> "This is what we mean by Bloomberg Terminal for private markets.
> Not a database you query. An AI analyst team that works while you sleep."

---

## Recording tips
- Use OBS or Loom; 1080p minimum
- Speak at 60% of your normal speed — the data needs time to load visually
- Pause 1-2 seconds after each API response loads before narrating
- Keep mouse movement slow and deliberate
- Crop to the demo window only (hide browser chrome if possible)
