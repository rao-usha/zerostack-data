# Agentic Portfolio Research - Quick Summary

## What We Built

A comprehensive plan for an **agentic system** that automatically discovers LP/FO portfolio companies and investment patterns by intelligently combining data from 5+ sources.

---

## Why Agentic?

Traditional API-based approaches don't work because:
- ❌ No single API has portfolio data for LPs/FOs
- ❌ Data scattered across websites, PDFs, SEC filings, news articles
- ❌ Each investor has different disclosure patterns
- ✅ **Agent can reason about where to look and adapt strategy**

---

## What the Agent Does

### 1. **Plans Strategy** (Smart Decision Making)
```
Agent analyzes investor:
- CalPERS: Large pension, public filings
- Strategy: Check 13F → Parse annual report → Scrape website
```

### 2. **Executes Multi-Source Collection**
Agent tries 5 strategies in priority order:

| Strategy | Data Source | Coverage |
|----------|------------|----------|
| 🏛️ **SEC 13F** | API | 40-60 large investors |
| 🌐 **Website** | HTML parsing | 60-80 investors |
| 📄 **Annual Reports** | PDF parsing | 50-70 public pensions |
| 📰 **News Search** | LLM extraction | 30-50 active investors |
| 🔄 **Reverse Search** | Google → company sites | 20-40 per investor |

### 3. **Synthesizes & Validates**
- Deduplicates across sources
- Prioritizes high-confidence data (SEC > Annual Report > News)
- Combines partial information from multiple sources
- Flags low-confidence findings for review

### 4. **Logs Reasoning**
```json
{
  "investor": "CalPERS",
  "strategy_used": ["sec_13f", "annual_report", "website"],
  "reasoning": [
    "SEC 13F found 150 public equity holdings",
    "Annual report listed 50 PE/VC investments",
    "Website confirmed 20 additional holdings",
    "Total: 220 companies, 3 sources, high confidence"
  ]
}
```

---

## Database Schema

### Core Tables:
1. **`portfolio_companies`** - All investments found
2. **`co_investments`** - Who invests together (network mapping)
3. **`investor_themes`** - Investment patterns (climate tech, healthcare, etc.)
4. **`agentic_collection_jobs`** - Agent decision trails

---

## Expected Results

### Coverage:
- **80-100 LPs** (60-75%) with 5+ portfolio companies each
- **40-60 Family Offices** (40-60%) with investment history
- **Average 3+ sources** per investor (validation)

### Quality:
- ✅ High confidence: 50%+ (multiple sources agree)
- ✅ Medium confidence: 30% (single reliable source)
- ✅ Low confidence: 20% (needs human review)

### Network Insights:
- Identify co-investor pairs (warm introductions)
- Map investment themes by investor type
- Track deal flow trends over time

---

## Business Value

### What You Can Do With This Data:

**1. Warm Introductions**
```
Query: "Who has CalPERS co-invested with?"
Result: "CalPERS + Ontario Teachers' = 15 deals together"
→ Warm intro opportunity
```

**2. Investment Themes**
```
Query: "What does Yale Endowment invest in?"
Result: "Climate tech (30%), Healthcare (25%), Emerging markets (20%)"
→ Pitch relevant to their interests
```

**3. Competitive Intelligence**
```
Query: "Who else invested in Company X?"
Result: "Harvard, MIT, Stanford endowments"
→ Validate investment thesis
```

**4. Portfolio Tracking**
```
Query: "Recent LP investments in fintech"
Result: "CalPERS → Stripe, Yale → Plaid, Harvard → Robinhood"
→ Identify trends
```

---

## Implementation Phases

### Quick Win (Week 1): SEC 13F Only
**Effort:** 2-3 days
**Coverage:** 40-60 large investors
**Value:** Immediate portfolio data for biggest LPs

### Phase 1 (Week 1-2): Core Infrastructure
- Database schema
- Agent orchestrator
- SEC 13F + Website strategies
- Basic synthesis

### Phase 2 (Week 3): PDF & News
- Annual report parsing
- News article extraction with LLM
- Improved deduplication

### Phase 3 (Week 4): Network Analysis
- Co-investor mapping
- Investment theme classification
- Reverse search strategy

### Phase 4 (Ongoing): Refinement
- Improve accuracy
- Add more sources
- Quarterly updates

**Total Timeline:** 4-6 weeks for full implementation

---

## Technical Highlights

### Agentic Decision Making:
```python
class PortfolioAgent:
    def decide_next_step(self, results_so_far):
        if len(results) >= 10:
            return "STOP - sufficient coverage"
        elif len(results) == 0:
            return "TRY - alternative strategy"
        elif unique_sources == 1:
            return "CONTINUE - need validation"
```

### LLM Entity Extraction:
```python
# Extract structured investment data from news articles
extracted = llm_extract("""
    CalPERS invests $50M in climate tech startup Acme Corp
""")
# → {company: "Acme Corp", amount: 50000000, theme: "climate_tech"}
```

### Multi-Source Synthesis:
```python
# Combine data from 3 sources
13F: "Apple Inc, $5M, 2024-12-31"
Website: "Apple (Technology), 2024"
News: "Invested in AAPL Q4 2024"
# → Single record with high confidence
```

---

## Key Safeguards

### Rate Limiting:
- 0.5-1 request/second per domain
- Max 3 concurrent domains
- 10-minute timeout per investor

### Quality Control:
- Confidence scoring (high/medium/low)
- Source attribution (provenance tracking)
- Deduplication logic
- Human review for low-confidence findings

### Ethical:
- Publicly disclosed data only
- No authentication bypass
- Respect robots.txt
- Opt-out mechanism

---

## Cost Estimates

### Per Investor:
- **API Calls:** 20-50 requests
- **LLM Tokens:** ~10,000 tokens (news extraction)
- **Cost:** $0.05-0.15 per investor
- **Time:** 3-5 minutes

### For 100 Investors:
- **Total Cost:** $5-15
- **Total Time:** 5-8 hours (automated)
- **Coverage:** 60-75 with good data

---

## Next Steps

### To Start Implementation:

1. **Review the detailed plan:**
   - `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md`

2. **Choose quick win or full approach:**
   - Quick: Implement SEC 13F only (2-3 days)
   - Full: Follow 4-week phased plan

3. **Set up infrastructure:**
   - Create database tables
   - Set up LLM API keys (OpenAI/Anthropic)
   - Install dependencies

4. **Test with 5 sample investors:**
   - CalPERS, Yale Endowment, Harvard Management Co
   - Verify agent decision logic
   - Refine strategies

5. **Scale to full dataset:**
   - Batch process all 131 LPs
   - Batch process all 100 FOs
   - Monitor quality and costs

---

## Success Criteria

### Minimum Viable:
- ✅ 50+ LPs with portfolio data
- ✅ Average 2+ sources per LP
- ✅ Agent reasoning logs are clear

### Full Success:
- ✅ 80+ LPs with 5+ companies each
- ✅ 40+ FOs with investment history
- ✅ 20+ co-investor relationships identified
- ✅ Investment themes classified
- ✅ <10% false positives

---

## Why This Is Valuable

Traditional approaches require:
- ❌ Manual research: 30-60 min per investor
- ❌ Single source: limited coverage
- ❌ Stale data: no automated updates

Agentic approach provides:
- ✅ Automated research: 3-5 min per investor
- ✅ Multi-source validation: higher quality
- ✅ Scalable updates: quarterly refreshes
- ✅ Network insights: co-investor relationships
- ✅ Pattern detection: investment themes

**ROI:** 10-20x time savings + higher data quality + unique insights

---

## Questions?

The detailed plan includes:
- Complete Python code examples
- Database schemas with indexes
- Error handling strategies
- Agent decision logic
- LLM prompts
- Testing procedures

Ready to implement when you are! 🚀
