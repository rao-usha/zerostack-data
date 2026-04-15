# Synthetic Crowd Intelligence: Implementation Research

**Date:** April 12, 2026
**Purpose:** Practical architecture, prompt engineering, cost modeling, and validation approaches for building a synthetic crowd intelligence system on Nexdata
**Companion doc:** `SYNTHETIC_CROWD_MATHEMATICS.md` (mathematical foundations)

---

## 1. Prompt Engineering for Synthetic Survey Personas

### State-of-the-Art Persona Construction

The most validated approach comes from the **Polypersona** framework (arXiv 2512.14562) and German GSS Personas (arXiv 2511.21722). Four-stage "Anthology approach":

1. Generate backstories using an unrestrictive prompt
2. Perform demographic surveys on each backstory-conditioned persona
3. Methodologically select a representative set matching a desired distribution
4. Administer the actual survey to the calibrated persona set

For PE context, persona axes are **role** (operating partner, LP allocator, sector banker, regulatory analyst), **risk tolerance** (conservative/moderate/aggressive), **sector expertise** (healthcare, industrials, tech), and **deal experience** (first-time buyer, serial acquirer, distressed specialist).

### Optimal Prompt Structure

**System prompt** (persona identity, ~300-500 tokens):
```
You are [Name], a [Role] at [Firm Type] with [X] years in [Sector].
Your investment philosophy: [2-3 sentences grounded in the role].
Your risk framework: [Conservative/Moderate/Aggressive with specific thresholds].
You have seen [specific deal archetypes] succeed and fail.
When evaluating deals, you prioritize: [ordered list of 3-5 factors].
You are skeptical of: [specific red flags based on role].
```

**User prompt** (question + grounding data, 1,000-4,000 tokens):
```
COMPANY BRIEF: [name, sector, revenue, EBITDA, employee count]
KEY DATA POINTS: [structured subset relevant to THIS persona's expertise]
SCORING CONTEXT: [relevant scorer outputs - diligence grade, stress score, etc.]
QUESTION: [specific structured question with output format instructions]
```

**Few-shot examples**: 1-2 examples in system prompt. More than 3 wastes tokens without improving quality.

### Preventing Mode Collapse

**The core problem:** Post-training alignment causes LLMs to converge on "typical" responses, reducing diversity by 40-60%.

**Techniques ranked by impact:**

1. **Information asymmetry** (HIGHEST IMPACT): Give each persona *different data subsets*. The operating partner sees OSHA/EPA/BLS. The LP allocator sees FRED/macro. The sector banker sees SEC filings. This creates genuine perspective diversity because the inputs differ, not just the personality framing.

2. **Cross-model ensemble**: GPT-4o and Claude have fundamentally different training data distributions. Running the same persona on both produces genuinely different viewpoints.

3. **Verbalized Sampling** (arXiv 2510.01171): Training-free prompting strategy that asks the model to "generate N responses with their corresponding probabilities" then samples from that distribution. 2-3x diversity improvement. GitHub: `CHATS-lab/verbalized-sampling`.

4. **Temperature stratification**: Run the same persona at temperatures 0.3, 0.7, and 1.0.

5. **Explicit disagreement instruction**: Add to 2-3 personas: "You are known for contrarian views. When the data is ambiguous, you tend to find the risk others miss."

6. **Anchoring variation**: Different personas see different data points prominently positioned at the start of their context.

### Data Budget Per Persona

Research is clear that **more context degrades quality** past a threshold (GPT-4 performance drops after 64K tokens, Databricks study). Budget:

- Scorer output summaries: ~200-400 tokens per scorer
- Role-specific raw data: ~500-1,500 tokens
- Total context per persona: **~4,600 tokens input**

**Per-persona data routing:**

| Persona Role | Primary Data Sources | Scorer Outputs |
|---|---|---|
| Operating Partner | OSHA, EPA ECHO, BLS, job postings | Diligence score, safety factors |
| LP Allocator | FRED, BEA, macro scenarios | Deal environment, stress score |
| Sector Banker | SEC EDGAR, public comps | Fund conviction, pedigree score |
| Regulatory Analyst | EPA, OSHA, CourtListener, SAM.gov | Legal exposure factor |
| Growth Investor | Job postings, Census, foot traffic | Growth momentum, innovation |
| Debt Analyst | FRED rates, leverage, stress scenarios | Portfolio stress, rate sensitivity |

**Compression technique:** Pre-compute 3-5 sentence narrative summaries from each source rather than raw numbers. Existing scorers already produce `reading` fields — use those directly.

---

## 2. Multi-Model Orchestration

### Architecture

Nexdata's existing infrastructure is well-suited: `LLMClient` supports both OpenAI and Anthropic, `asyncio.Semaphore` handles concurrency, worker queue handles async jobs.

**Orchestration pattern:**
- Split personas across models (60% GPT-4o, 40% Claude Sonnet) for ensemble diversity
- Per-provider semaphores (5 concurrent for OpenAI, 3 for Anthropic)
- `asyncio.gather(return_exceptions=True)` — don't fail all if one fails
- Require quorum: 8 of 12 successful responses minimum

### Model Tiering

| Task | Model | Rationale |
|---|---|---|
| Persona survey responses (core) | GPT-4o + Claude Sonnet 4.6 | Split for ensemble diversity |
| Synthesis narrative | GPT-4o or Claude Opus | Needs strong reasoning |
| Data summarization (pre-processing) | GPT-4o-mini | Cheap, sufficient quality |
| Validation/consistency checks | GPT-4o-mini | Binary checks, cheap |

### Cost Optimization Levers (70-85% reduction possible)

1. **Prompt caching**: OpenAI auto-caches prompts >1,024 tokens at 90% discount. Static system prompt goes first.
2. **Model routing**: GPT-4o-mini ($0.15/1M input) for data summarization step.
3. **Batch API**: 50% discount for 24h turnaround on non-urgent panels.

### Structured Output

**Recommended:** Adopt the `instructor` library (`pip install instructor`). Enforces Pydantic model outputs with automatic retry on validation failure. 3M+ monthly downloads, supports OpenAI + Anthropic.

```python
class PersonaSurveyResponse(BaseModel):
    investment_recommendation: Literal["strong_buy", "buy", "hold", "sell", "strong_sell"]
    confidence_pct: int = Field(ge=0, le=100)
    fair_value_multiple: float = Field(ge=0, le=50)
    key_risk: str = Field(max_length=200)
    key_opportunity: str = Field(max_length=200)
    reasoning: str = Field(max_length=500)
    management_quality: int = Field(ge=1, le=5)
    market_position: int = Field(ge=1, le=5)
    regulatory_risk: int = Field(ge=1, le=5)
    growth_outlook: int = Field(ge=1, le=5)
    deal_timing: int = Field(ge=1, le=5)
```

---

## 3. Aggregation Methods

### Quantitative Outputs

**Likert scales (1-5):** Median (robust to outliers), IQR (disagreement measure), full distribution.

**Categorical recommendations:** Modal recommendation + vote split. "8 of 12 panelists recommend Buy; 3 Hold; 1 Sell."

**Continuous estimates (multiples):** Median, trimmed mean (drop top/bottom 10%), range. Flag if range >2x median.

### Qualitative Synthesis

Pass all persona reasoning texts to a synthesis LLM call: "Synthesize these 12 analyst perspectives into a single coherent narrative. Highlight agreements, key disagreements, and most cited risks/opportunities. Do not average — preserve the tension."

Cost: ~$0.03-0.05 per synthesis.

### Outlier Detection

Using existing `scipy.stats`:
- Likert: Flag responses >1.5 IQR from panel median across all items
- Continuous: Modified Z-score (median absolute deviation)
- Categorical: Flag disagreement with >75% supermajority

---

## 4. Validation Pipeline

### Internal Consistency

1. **Intra-response**: If persona rates management=5 but recommends "sell", flag
2. **Intra-panel**: 11/12 agree but one wildly different — contrarian or hallucination?
3. **Data-response**: Grounding data shows revenue -20% but persona says "strong growth trajectory" — flag

### Cross-Validation Against Existing Scorers

Nexdata's unique advantage — 8 independent scoring engines as ground truth anchors:
- Crowd says "buy" but diligence grade is D → flag CONFLICT
- Crowd bullish on growth but job posting momentum negative → flag CONFLICT
- Crowd flags regulatory risk but EPA/OSHA data shows clean record → flag CONFLICT

### Backtesting

1. Seed 20-50 historical PE deals with known outcomes
2. Reconstruct data environment at entry date using historical FRED/BLS/SEC data
3. Run synthetic panel with data available at that time
4. Compare recommendation to actual outcome (>2x MOIC? Lost money?)
5. Track calibration: confidence-when-right vs. confidence-when-wrong

### Red-Teaming

- **Fraudulent company**: Theranos-like profile. Panel should flag concerns.
- **Obvious winner**: A-grade everything. Panel should converge on buy.
- **Ambiguous case**: Great growth + terrible regulatory. Panel should show high disagreement.
- **Data poisoning**: One obviously wrong data point. Check if any persona accepts uncritically.

---

## 5. Output Design for PE Buyers

### What PE Professionals Want

Standard IC memo format (2-4 pages):
1. Executive Summary (1 paragraph: thesis, price, structure)
2. Key Investment Highlights (3-5 bullets)
3. Key Risks and Mitigants (3-5 bullets)
4. Financial Summary
5. Valuation (entry multiple, comp range)
6. Management Assessment
7. Market/Sector Context

### Presenting Synthetic Crowd Output

**Do NOT show:** Raw Likert scores, individual persona responses, JSON, statistical tests.

**DO show:** Translated into IC memo language:

> "**Panel Consensus: BUY** (8 of 12 analysts, 67% avg confidence). Two analysts recommend Hold, citing regulatory risk. One contrarian sell driven by leverage concerns at current rate environment."

> "**Valuation Range**: 7.5-9.2x EV/EBITDA (median 8.3x). Operating-focused panelists anchor lower due to margin compression risk; growth-oriented panelists price in expanding addressable market."

> "**Key Risk (unanimously cited)**: Environmental remediation liability at Tennessee facility. EPA ECHO data shows 3 active enforcement actions. Regulatory analyst estimates $2-5M remediation costs."

> "**Disagreement Alert**: Panelists split on management quality (median 3.5/5, range 2-5). Operating partners who reviewed OSHA data more skeptical than those focused on financial performance."

### Output Formats

1. **HTML memo** (primary): Use existing report template system with Chart.js for confidence distribution and disagreement heatmap
2. **API JSON**: Full structured `SyntheticPanelResult` for programmatic consumption
3. **Slide deck** (stretch): `python-pptx` already in requirements. 5-slide IC deck.

---

## 6. Frameworks and Tools

### Framework Recommendation: Don't Adopt a Heavy Framework

Nexdata already has: `LLMClient` with multi-provider support, `asyncio.Semaphore`, worker queue, cost tracking, provenance tracking, structured validation. Adding CrewAI/LangGraph would introduce overhead for capabilities you already have.

**Build a thin orchestration layer** using existing infrastructure.

### Tools to Adopt

| Tool | Purpose | Effort | Value |
|---|---|---|---|
| `instructor` | Pydantic-enforced structured outputs | 2 hours | Eliminates JSON parsing failures |
| Verbalized Sampling (prompting) | Anti-mode-collapse | Zero code | 2-3x diversity improvement |
| `deepeval` | Automated quality checks | 4 hours | CI/CD quality gates |

### Key GitHub Repos

- `jacob-bd/llm-council-plus` — Three-stage LLM council with synthesis. Best architecture reference.
- `CHATS-lab/verbalized-sampling` — Mode collapse mitigation.
- `567-labs/instructor` — Structured output enforcement. Production-grade.
- `confident-ai/deepeval` — LLM evaluation framework.
- `pengr/LLM-Synthetic-Data` — Living reading list of LLM data synthesis papers.

---

## 7. Cost Modeling

### Per-Panel Cost (12 Personas)

| Phase | What | Cost |
|-------|------|------|
| Data summarization | 6 sources via GPT-4o-mini | $0.01 |
| Persona calls | 6 GPT-4o + 6 Claude Sonnet | $0.27 |
| Synthesis | 1 GPT-4o call | $0.04 |
| Validation | 3 GPT-4o-mini calls | ~$0.00 |
| **Total** | | **$0.32** |

With prompt caching: **$0.29**. With Batch API: **$0.19**.

### Scaling Economics

| Volume | Cost/Panel | Monthly Cost |
|---|---|---|
| 10 panels/day | $0.32 | ~$96 |
| 50 panels/day | $0.29 (cached) | ~$435 |
| 200 panels/day | $0.19 (batch) | ~$1,140 |

### Break-Even

At $5K/month feature pricing:
- Can run **15,625 panels/month** before LLM costs equal revenue
- Realistic usage: 50-200 panels/month = $15-60 LLM cost
- **99%+ gross margin**

Even at $2K/month: >95% gross margin.

---

## 8. The Critical Math Result

From the companion math document (`SYNTHETIC_CROWD_MATHEMATICS.md`), the single most important formula:

**Effective sample size:**
```
N_eff = N / (1 + (N-1) * rho)
```

At rho = 0.80 (typical same-model LLM responses), **100 LLM responses = 1.2 independent respondents**. Naive scaling is useless.

**Nexdata's structural advantage:** Asymmetric information injection (different data subsets per persona) reduces ICC from ~0.7-0.9 down to ~0.2-0.5, changing N_eff from ~1-2 to **~10-45** for a 100-response ensemble.

This is why data grounding matters more than prompt engineering. The math proves it.

---

## 9. Implementation Sequence (When Ready)

| Week | What |
|------|------|
| 1 | Define 6 persona templates. Build `PersonaSurveyResponse` Pydantic model. Add `instructor`. Write prompts with information asymmetry. |
| 2 | Build `orchestrator.py` using existing `LLMClient`. Parallel calls with `asyncio.gather`. Wire to worker queue. |
| 3 | Build aggregation (median/IQR for Likert, vote counting, trimmed mean). Cross-validation against scorers. Synthesis call. |
| 4 | HTML memo output using existing templates. API endpoint. Provenance tracking. |
| 5 | Red-team testing. Calibrate prompts. DeepEval quality gates. |

**Key files:**
- `app/services/synthetic_crowd/personas.py`
- `app/services/synthetic_crowd/orchestrator.py`
- `app/services/synthetic_crowd/synthesis.py`
- `app/services/synthetic_crowd/validation.py`
- `app/api/v1/synthetic_crowd.py`

---

## Sources

### Persona Engineering
- [Polypersona (arXiv 2512.14562)](https://arxiv.org/pdf/2512.14562) — 3,568 simulated responses, 433 personas
- [German GSS Personas (arXiv 2511.21722)](https://arxiv.org/html/2511.21722v2)
- [Quantifying Persona Effect (arXiv 2402.10811)](https://arxiv.org/html/2402.10811v2) — demographics alone = ~1.5% behavioral variance

### Mode Collapse
- [Verbalized Sampling (arXiv 2510.01171)](https://arxiv.org/abs/2510.01171) — 2-3x diversity improvement
- [Verbalized Sampling GitHub](https://github.com/CHATS-lab/verbalized-sampling)

### RAG and Context
- [Long-Context LLMs Meet RAG (ICLR 2025)](https://proceedings.iclr.cc/paper_files/paper/2025/file/5df5b1f121c915d8bdd00db6aac20827-Paper-Conference.pdf)
- [Long Context RAG Performance (Databricks)](https://www.databricks.com/blog/long-context-rag-performance-llms)

### Validation
- [Synthetic Replacements for Human Survey Data — Perils of LLMs (Cambridge)](https://www.cambridge.org/core/journals/political-analysis/article/synthetic-replacements-for-human-survey-data-the-perils-of-large-language-models/B92267DC26195C7F36E63EA04A47D2FE)
- [Assessing Reliability of Persona-Conditioned LLMs (arXiv 2602.18462)](https://arxiv.org/html/2602.18462)

### Architecture
- [LLM Council Plus GitHub](https://github.com/jacob-bd/llm-council-plus)
- [Instructor Library](https://python.useinstructor.com/)
- [DeepEval GitHub](https://github.com/confident-ai/deepeval)
- [7 Ensemble AI Patterns](https://dev.to/atanasster/7-ensemble-ai-patterns-for-reliable-llm-systems-200l)

### Cost
- [LLM Cost Optimization: 5 Levers (MorphLLM)](https://www.morphllm.com/llm-cost-optimization)
- [Token Optimization Strategies](https://www.glukhov.org/post/2025/11/cost-effective-llm-applications)
