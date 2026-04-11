# Synthetic Crowd Intelligence: Research, Competitors, and Nexdata Application

**Date:** April 11, 2026
**Purpose:** Deep research on wisdom of crowds + synthetic survey intelligence — what Simile/Aaru are doing, what the academic literature says, and what Nexdata can do differently with its 28+ real data sources.

---

## Executive Summary

A new category of AI companies is replacing traditional surveys and focus groups with LLM-powered synthetic populations. **Simile** (Stanford spinout, $100M Series A) builds digital twins from real human interviews. **Aaru** (teenage founders, $1B headline valuation) generates pure synthetic populations from public data. Both target general-purpose market research.

**Neither operates in PE intelligence.** Both simulate *consumer behavior*. Neither has SEC filings, job posting data, FRED macro data, or any structured investment data.

**Nexdata's opportunity:** Build domain-specific synthetic crowd intelligence for PE deal evaluation — grounded in 28+ real data sources, not pure LLM hallucination. A "Synthetic IC Committee" that evaluates a deal from 12 expert perspectives, each grounded in actual SEC filings, EPA violations, job posting trends, and macro data. This is fundamentally different from what Simile/Aaru offer.

**The academic consensus:** LLM crowds partially satisfy the "wisdom of crowds" conditions. Ensembles of 12+ LLMs across diverse models achieve forecasting accuracy statistically indistinguishable from human crowds (Schoenegger et al., Science Advances 2025). But independence and diversity conditions are only partially met — multi-model ensembles (GPT-4 + Claude) are strictly better than single-model.

---

## Part 1: The Competitors

### Simile AI (formerly Simuli)

| Attribute | Detail |
|-----------|--------|
| **What they do** | "The Simulation Company" — AI simulation of society using generative agents based on real humans |
| **Founded** | Stanford spinout. Joon Sung Park (CEO), Percy Liang (Chief Scientist), Michael Bernstein (CPO) |
| **Funding** | $100M Series A (Feb 2026), led by Index Ventures. Angels: Andrej Karpathy, Fei-Fei Li, Adam D'Angelo |
| **Technology** | Digital twins built from 2-hour audio interviews with real humans. Agent architecture: interview transcript + memory + reflection + planning |
| **Validation** | 85% of human self-replication accuracy on General Social Survey. Reduces political bias 36-62%, racial bias 7-38% vs. demographic-only agents |
| **Customers** | CVS Health, Telstra, Suntory, Wealthfront, Banco Itau |
| **Gallup partnership** | Building probability-based synthetic panels (waitlist mode) |
| **Consumer product** | "MiniMe" — create your own AI agent from a 10-minute interview |
| **Pricing** | Enterprise-only, estimated $100K+/year |

**Key research papers:**
1. *"Generative Agents: Interactive Simulacra of Human Behavior"* (Park et al., UIST 2023) — 25 autonomous AI characters in simulated town "Smallville." Best Paper award.
2. *"Generative Agent Simulations of 1,000 People"* (Park et al., 2024) — 1,052 stratified US individuals, 2-hour interviews each. Direct precursor to Simile commercial product.

**CVS Health case study:** Testing medication adherence reminders, education content, benefit designs. Built on 2.9M consented responses from 400K+ participants. "Condensed weeks of research into hours."

**Critical weakness:** The Columbia Business School mega-study ("Digital Twins as Funhouse Mirrors," 2025-2026) found much weaker results than Simile's own papers: average correlation between twin and human responses was only r=0.20 (weak), with five systematic distortions: stereotyping, insufficient individuation, representation bias, ideological biases, and hyper-rationality.

---

### Aaru

| Attribute | Detail |
|-----------|--------|
| **What they do** | "Rethinking the science of prediction" — pure synthetic population simulation |
| **Founded** | March 2024, NYC. Cameron Fink (CEO, 18), Ned Koh (President, 19), John Kessler (CTO, 15 at founding) |
| **Funding** | $50M+ Series A (Dec 2025), led by Redpoint Ventures. Headline $1B valuation (blended ~$450M). Also backed by General Catalyst, Accenture Ventures, A* |
| **Technology** | Multi-agent system — synthetic populations from census, behavioral, sentiment data. No real human interviews. Agents browse internet to mimic media diets. 500K+ profiles, 3M+ simulations/month |
| **Validation** | EY Wealth Management survey: 0.90 median Spearman correlation across 53 questions. Predicted NY Democratic primary within 371 votes. **Missed 2024 presidential election.** |
| **Customers** | EY, Accenture (strategic investor), IPG, Spindrift Beverage, Coca-Cola (testing), political campaigns |
| **Products** | Lumen (corporate), Dynamo (political), Seraph (public sector), GeoPulse API (by ZIP code) |
| **Pricing** | Enterprise-only, ~$0.08/simulation, estimated $100-250K+/year contracts |
| **ARR** | Still below $10M despite $1B headline valuation |

**EY case study (flagship):** Replicated 3,600-investor global wealth survey in 1 day vs. 6 months. Two critical findings where Aaru outperformed the real survey:
- Inheritance: Real survey said 82% retain parents' advisor. Aaru predicted 43%. Actual retention is 20-30%. **Aaru was closer to reality.**
- Consolidation: Survey said 69% want single provider. Aaru predicted 37%. Actual is 33%. **Aaru was closer again.**

**The insight:** When synthetic responses diverge from what real people *say*, the synthetic version may better predict what people *do*. Surveys capture stated preference; synthetic agents model revealed preference.

**Key weakness:** No peer-reviewed publications. 2024 presidential election miss. ARR <$10M at $1B valuation is extreme. The "pure synthetic" approach (no real human grounding) is philosophically opposed to Simile's interview-based approach.

---

### Competitive Landscape (Full Map)

| Company | Approach | Funding | Best For |
|---------|----------|---------|----------|
| **Simile** | Interview-based digital twins | $100M | Consumer brands, behavioral prediction |
| **Aaru** | Pure synthetic populations | $50M+ ($1B val) | Financial services, political polling |
| **Synthetic Users** | LLM personas for UX | YC W23 | Product teams, UX testing |
| **Ditto** | Population-data-grounded | Undisclosed | Multi-country research, $50-75K/yr |
| **Conjoint.ly** | Hybrid real + synthetic | Established | Conjoint analysis, survey augmentation |
| **Qualtrics Edge** | Real panels + AI augmentation | Public co | Enterprise survey + AI analysis |
| **SYMAR (ex-OpinioAI)** | Synthetic surveys/focus groups | Undisclosed | 90-95% cost savings claim |
| **Vox Populi AI** | Political opinion simulation | Unknown | Campaigns, policy orgs |

**Market size:** Traditional market research is $120B. AI simulation currently <$1B but growing 60%+ annually. Synthetic data generators projected $0.3B (2023) to $2.1B by 2028.

---

## Part 2: Academic Literature

### Key Papers

**1. "Out of One, Many: Using Language Models to Simulate Human Samples"**
(Argyle et al., Political Analysis 2023)
- Conditioned GPT-3 on demographic personas, compared to ANES survey data
- Reproduced partisan gaps on gun control, immigration, abortion with surprising fidelity
- Coined "silicon sampling." Performance degraded for niche subpopulations

**2. "Using Large Language Models to Simulate Multiple Humans"**
(Aher, Arriaga, Kalai — ICML 2023, Microsoft Research)
- Replicated Milgram, ultimatum games, wisdom of crowds jar estimation
- LLM crowd's aggregate converged toward correct answer, mimicking classic wisdom of crowds
- Caveat: LLMs may have memorized famous experiment results

**3. "Whose Opinions Do Language Models Reflect?"**
(Santurkar, Durmus et al. — Stanford, ICML 2023)
- Default LLM opinions most closely match **college-educated Democrats under 50**
- Significant misalignment with older adults, rural populations, non-college, strong conservatives
- This is the core "WEIRD bias" finding

**4. "Large Language Models as Simulated Economic Agents"**
(Horton — MIT Sloan/NBER 2023)
- "Homo silicus" — LLMs reproduced classic economic results (downward-sloping demand, reservation wages)
- Generated plausible conjoint preference data
- Argues LLMs are "digital wind tunnels" for testing experimental designs

**5. "Wisdom of the Silicon Crowd"**
(Schoenegger et al. — Science Advances, Feb 2025)
- **The most important paper.** Ensemble of 12 LLMs achieves forecasting accuracy statistically indistinguishable from human crowds
- Diversity across models matters more than prompt variation within one model
- This is the scientific foundation for multi-model synthetic crowd approaches

**6. "Generative Agents: Interactive Simulacra of Human Behavior"**
(Park et al. — Stanford, UIST 2023, Best Paper)
- 25 autonomous agents in simulated town with emergent social behavior
- Established viability of persona-conditioned LLM simulation at scale

### What Works vs. What Fails

| Works Well | Fails |
|-----------|-------|
| High-salience political opinions (75-85% directional accuracy) | Niche subpopulations (rural, elderly, marginalized) |
| Consumer preferences for common products | Lived experience questions ("how has chronic pain affected you?") |
| Classic economic experimental effects | Post-training-cutoff events |
| Directional/ordinal comparisons | Precise quantitative estimates |
| Pilot study / hypothesis generation | Extreme or taboo opinions (RLHF suppresses) |
| Replicating known survey results | Behavioral predictions (stated vs. revealed preference) |

### Techniques That Improve Accuracy

1. **Rich multi-dimensional personas** — not just demographics but psychographics, media consumption, values
2. **Temperature variation** (0.7-1.0) — introduces response diversity
3. **Multi-model ensembles** (GPT-4 + Claude + Gemini) — genuine architectural independence
4. **Calibration against known benchmarks** — run calibration questions with known ground truth
5. **Adversarial persona pairs** — deliberately prompt opposing viewpoints to counteract sycophancy
6. **Chain-of-thought reasoning** — reduces surface-level agreement bias
7. **Ensemble of 50-100+ respondents** minimum per question

### The Wisdom of Crowds Problem

Surowiecki's 4 conditions for crowd wisdom:

| Condition | Real Crowds | LLM Crowds |
|-----------|------------|------------|
| **Diversity of opinion** | Each person has unique experience | Partially met — persona prompting creates surface diversity but same underlying distribution |
| **Independence** | Errors are uncorrelated | **Poorly met** — all from same model weights. Multi-model ensembles partially fix this |
| **Decentralization** | People draw on local knowledge | Poorly met — LLMs have broad but shallow knowledge |
| **Aggregation** | Mechanism to combine judgments | Well met — computational aggregation is trivial |

**Bottom line:** LLM crowds are better than single LLM responses, worse than equivalent-sized real crowds, and most useful as complement to (not replacement for) human judgment.

---

## Part 3: What Nexdata Can Do Differently

### The Fundamental Insight

**Simile and Aaru simulate *consumer behavior* from demographic data. Nexdata would simulate *expert judgment* grounded in actual deal data.**

No survey respondent has read the target's 10-K, checked their EPA violation history, analyzed their job postings for leadership churn, and cross-referenced FRED macro data for their sector. But a Nexdata synthetic persona can do all of that simultaneously.

This is not "replacing surveys with AI." This is using AI crowds to synthesize structured intelligence that no survey could ever capture.

### Why Nexdata's Approach Is Structurally Different

| Dimension | Simile/Aaru | Nexdata |
|-----------|-------------|---------|
| **Data grounding** | Census demographics, social media sentiment | 28+ structured sources: SEC, FRED, BLS, EPA, OSHA, job postings, org charts |
| **Domain** | Consumer behavior, general market research | PE deal evaluation, LP intelligence, competitive dynamics |
| **Personas** | Generic demographic profiles | PE-specific archetypes grounded in real deal data |
| **Validation** | Correlation with survey responses | Cross-validation against 8 live scoring engines + real economic data |
| **Provenance** | Unclear | Built-in `data_origin` tracking at database level |
| **Integration** | Standalone platform | Embedded in existing PE intelligence stack |
| **Cost per run** | Enterprise platform ($100K+/yr) | Per-run ($2-80 in LLM API costs) |

---

### Seven Products for PE Firms

#### 1. Synthetic IC Committee (Highest Impact)

Simulate a 12-member Investment Committee evaluating a deal. Each persona has a distinct archetype and is grounded in real Nexdata outputs:

| Persona | Grounding Data | Role |
|---------|---------------|------|
| Deal Lead (Bullish) | Diligence score, job posting growth momentum | Advocate |
| Skeptical Partner | EPA/OSHA violations, legal exposure, stress score | Find what's wrong |
| Macro Strategist | FRED rates, BLS employment, deal environment score | Macro context |
| Operating Partner | Exec signals, org chart depth, pedigree scores | Management quality |
| Sector Specialist | Industry FRED/BLS series, peer comparisons | Domain judgment |
| LP Relations | GP pipeline score, LP conviction, fund performance | LP perspective |
| Regulatory Expert | EPA ECHO, OSHA, government contracts | Compliance risk |
| Customer Voice | Customer-facing job postings, revenue concentration | Customer retention |
| Exit Strategist | Comparable exits, stress score, macro headwinds | Exit viability |
| Quant Analyst | All 8 scorer outputs, statistical distributions | Data-driven calibration |
| Devil's Advocate | Inverts consensus | Stress-test groupthink |
| Synthesis Chair | All persona outputs | Aggregates recommendation |

**Why PE firms pay:** IC prep takes 40+ hours. This gives a pre-IC simulation surfacing arguments, counterarguments, and data gaps *before* the real meeting.

**Pricing:** $5-15K per deal evaluation. A mid-market PE firm evaluates 200+ deals/year.

#### 2. Synthetic Commercial DD ("McKinsey Killer")

Instead of $300K for 6-week McKinsey commercial DD, generate a synthetic market study in hours. 500 "customer personas" grounded in:
- Census demographics for target's geographic markets
- BLS employment/wage data for customer industries
- Job posting intelligence (hiring trends = demand signals)
- SEC revenue concentration data
- FRED consumer sentiment, retail sales

**Pricing:** $25-50K per report vs. $300K from McKinsey. Even as a "pre-screen" before commissioning full DD, $25K on 50+ deals/year.

**Gross margin:** $25K revenue vs. $30-80 in LLM API costs = extraordinary.

#### 3. Synthetic LP Sentiment Monitor

Simulate how your LP base reacts to portfolio changes, market shifts, or strategy pivots. Each synthetic LP grounded in:
- Real LP-GP relationship data (from `lp_fund`, `lp_gp_relationships`)
- LP type characteristics (pension vs. endowment vs. family office)
- Macro conditions (FRED rates, equity markets)
- Fund performance relative to benchmarks

**Use cases:** "If we extend Fund IV's investment period 12 months, how do our LPs react?" / "Interest rates jump 75bps — which LPs reduce re-ups?"

**Whitespace product:** Nobody else is simulating institutional LP behavior grounded in actual commitment data.

#### 4. Synthetic War Room (Competitive Intelligence)

Simulate competitor responses to strategic moves. Each competitor persona grounded in their actual SEC filings, job postings, exec team, and macro conditions.

**Example:** "We acquire TargetCo and cut prices 15% in the Southeast. How do Competitors A, B, C respond?"

#### 5. Synthetic Risk Tribunal

For any deal, simulate 7 stakeholder groups reacting to top risks: Regulators (EPA/OSHA data), Employees (org chart/turnover), Customers (revenue concentration), Competitors (peer filings), Creditors (stress score), Community (site intel), Media (news signals).

#### 6. Synthetic Exit Readiness Test

Before marketing a portfolio company for exit, simulate 20 buyer personas evaluating it. Each buyer built from real PE firm profiles, sector preferences, deal history, and current macro conditions.

#### 7. Synthetic Board Advisory

For portfolio company board meetings, simulate domain expert panels grounded in real data. European expansion decision gets FX expert (FRED exchange rates), regulatory specialist (compliance data), operations expert (site intel, logistics), customer perspective (market signals).

---

### Technical Architecture

**Key design principle:** Asymmetric information injection. Give each persona *different subsets* of grounding data. The skeptical partner sees EPA violations and legal exposure but NOT growth momentum. The bullish analyst sees job postings and revenue growth but NOT environmental risk. This forces genuine perspective diversity.

**Multi-model execution:** Run 60% of personas on GPT-4o, 40% on Claude. The Science Advances paper proved ensemble accuracy requires model diversity — same-model ensembles have correlated biases.

**Aggregation:** Trimmed mean (drop top/bottom 10%) for quantitative outputs. Synthesis persona for qualitative outputs that reads all responses and produces coherent summary flagging consensus vs. disagreement.

**Validation pipeline:** Cross-validate synthetic crowd outputs against real scorer outputs. If the crowd says "proceed" but diligence score is D-tier, flag divergence. Display data freshness timestamps. Block runs if key data sources are stale.

**Cost per run:**
- IC Committee (12 personas): ~$1.50-3.00 in LLM API
- Commercial DD (100 respondents): ~$5-15
- Full "McKinsey replacement" (500 respondents, 20 questions): ~$30-80

**Implementation phases:**
1. **Phase 1 (2-3 weeks):** Synthetic IC Committee MVP — 6 persona archetypes, single endpoint
2. **Phase 2 (3-4 weeks):** Synthetic Commercial DD — Census/BLS persona generator
3. **Phase 3 (2-3 weeks):** Synthetic LP Sentiment — LP persona generator from existing tables
4. **Phase 4 (ongoing):** War Room, Risk Tribunal, Exit Test — variations of same engine

---

### Risks and Guardrails

**Where synthetic PE intelligence must NEVER replace human judgment:**
- Final investment decisions (fiduciary obligations)
- Legal/regulatory compliance assessments
- Management quality assessment (character, integrity)
- Specific valuation figures (DCF with audited financials is the standard)

**Required guardrails in every output:**
- Watermark: "SYNTHETIC INTELLIGENCE BRIEFING — NOT INVESTMENT ADVICE"
- Data provenance table (real vs. synthetic sources)
- Data freshness timestamps
- Confidence score based on data coverage
- Explicit "what this analysis cannot tell you" section
- LLM cost of the analysis

**The biggest risk:** Overreliance creep. Junior analysts treating synthetic IC output as gospel. Mitigation: every output includes "Questions for the real IC" — things the synthetic crowd couldn't answer.

---

### The Pitch

> "Before you spend $300K and 6 weeks on McKinsey, spend $25K and 6 hours with Nexdata's Synthetic IC. If the synthetic IC kills the deal, you just saved $275K. If it greenlights the deal, you walk into McKinsey's kickoff with the right questions already in hand."

> "Your current deal scoring gives you one number. Our synthetic IC gives you 12 numbers from 12 expert perspectives, shows you where they agree and disagree, surfaces data gaps you didn't know existed, and produces a debate transcript you can hand to your actual IC partners before the meeting."

---

## Key Academic Sources

- Argyle et al. (2023) "Out of One, Many" — *Political Analysis* — silicon sampling foundation
- Aher, Arriaga, Kalai (2023) "Using LLMs to Simulate Multiple Humans" — *ICML* — experimental replication
- Santurkar, Durmus et al. (2023) "Whose Opinions Do Language Models Reflect?" — *ICML* — WEIRD bias
- Horton (2023) "Homo Silicus" — *NBER Working Paper* — economic agent simulation
- Schoenegger et al. (2025) "Wisdom of the Silicon Crowd" — *Science Advances* — ensemble accuracy = human crowds
- Park et al. (2023) "Generative Agents" — *UIST Best Paper* — Smallville emergent behavior
- Park et al. (2024) "Generative Agent Simulations of 1,000 People" — *arXiv 2411.10109* — Simile foundation
- Brand et al. (2025) "Simulating Human-like Survey Responses" — *AION Institute* — 90% of human test-retest reliability
- Columbia Business School (2025-2026) "Digital Twins as Funhouse Mirrors" — *arXiv 2509.19088* — critical rebuttal (r=0.20)

## Company Sources

- [Simile AI](https://simile.ai) — Stanford spinout, $100M raise
- [Aaru](https://aaru.com) — $1B valuation, teenage founders
- [EY x Aaru Wealth Study](https://www.ey.com/en_us/insights/wealth-asset-management/how-ai-simulation-accelerates-growth-in-wealth-and-asset-management)
- [Simile x CVS Health Case Study](https://www.simile.ai/blog/simile-cvs-health)
- [Bloomberg: Simile $100M](https://www.bloomberg.com/news/articles/2026-02-12/ai-startup-nabs-100-million-to-help-firms-predict-human-behavior)
- [TechCrunch: Aaru $1B](https://techcrunch.com/2025/12/05/ai-synthetic-research-startup-aaru-raised-a-series-a-at-a-1b-headline-valuation/)
- [Accenture invests in Aaru](https://newsroom.accenture.com/news/2025/accenture-invests-in-and-collaborates-with-ai-powered-agentic-prediction-engine-aaru)
