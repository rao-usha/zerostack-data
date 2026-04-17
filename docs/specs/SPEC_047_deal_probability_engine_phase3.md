# SPEC 047 — Deal Probability Engine: Phase 3 Intelligence Layer

**Status:** Draft
**Task type:** service
**Date:** 2026-04-14
**Test file:** tests/test_spec_047_deal_probability_engine_phase3.py

## Goal

Make Phase 2's raw probabilities actionable. Phase 3 adds four intelligence services on top of the scoring engine:
1. **Convergence detector** — named multi-signal patterns (classic_exit_setup, founder_transition, etc.) that are the product's alpha differentiator
2. **Alert engine** — threshold-crossing alerts on probability spikes, grade upgrades, new convergence events, signal acceleration
3. **NLQ** — natural language query to companies/rankings (Claude-parsed, whitelist-validated, keyword fallback)
4. **Narrative generator** — LLM-generated explainers (short) and deal memos (6 sections, HTML) per company

## Acceptance Criteria

- [ ] `app/services/probability_convergence.py` defines ≥4 named patterns and a `ConvergenceDetector.detect_company(company_id)` that returns matched patterns
- [ ] `app/services/probability_alerts.py` has `AlertEngine.evaluate(company_id, old_score, new_score, convergences)` that writes `TxnProbAlert` rows for qualifying events
- [ ] `app/services/probability_nlq.py` whitelist-validates filters, parses with Claude, falls back to keyword matching
- [ ] `app/services/probability_narrative.py` produces narrative + deal memo via `LLMClient` (Anthropic) with a graceful fallback when LLM unavailable
- [ ] 5 new endpoints added to `app/api/v1/transaction_probability.py`: narrative, memo, convergences, query, sector briefing
- [ ] `TransactionProbabilityEngine.score_company()` calls the alert engine + convergence detector after persisting a score
- [ ] No modifications to existing scorers or Phase 2 core signatures (add-only)
- [ ] Pure-function convergence math is testable without DB

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_convergence_patterns_registry_has_four | ≥4 patterns registered |
| T2 | test_convergence_classic_exit_setup | Match when exec_transition≥60, financial_health≥70, sector_momentum≥65 |
| T3 | test_convergence_founder_transition | Match when founder_risk≥70, exec_transition≥50, deal_activity≥40 |
| T4 | test_convergence_distress_opportunity | Inverted diligence (≤40) + selling insider + restructuring hiring |
| T5 | test_convergence_no_match | Returns empty list when no pattern qualifies |
| T6 | test_alert_probability_spike | delta > 0.15 → high-severity alert |
| T7 | test_alert_grade_upgrade | Grade B → A generates medium alert |
| T8 | test_alert_new_convergence | Pattern match generates high-severity alert |
| T9 | test_alert_no_spike | delta < threshold → no alert |
| T10 | test_nlq_whitelist_rejects_bad_field | Unknown field filter dropped |
| T11 | test_nlq_keyword_fallback | Keyword-only fallback works without LLM |
| T12 | test_nlq_executes_filters | Valid filters return correctly-filtered companies |
| T13 | test_narrative_graceful_when_no_llm | Falls back to template when LLM fails |
| T14 | test_engine_integrates_alerts | Scoring a company twice fires alerts on second run |
| T15 | test_api_convergences_endpoint | GET /convergences returns JSON shape |
| T16 | test_api_query_endpoint | POST /query with a keyword returns filtered results |

## Rubric Checklist

- [ ] All LLM calls use `LLMClient` with Anthropic provider
- [ ] Whitelist validation on NLQ (no raw SQL from LLM)
- [ ] Scorers unchanged; phase 3 is purely additive
- [ ] Alert dedup: don't fire the same alert twice for the same condition
- [ ] Graceful degradation when LLM unavailable

## Design Notes

### Convergence patterns (dataclass registry)

```python
@dataclass
class ConvergencePattern:
    key: str  # e.g. "classic_exit_setup"
    label: str
    description: str
    required_signals: Dict[str, Dict]  # {"exec_transition": {"min": 60}, ...}
    severity: str  # high|medium|low

CONVERGENCE_PATTERNS = {
    "classic_exit_setup": ...  # exec_transition≥60, financial_health≥70, sector_momentum≥65
    "founder_transition": ...  # founder_risk≥70, exec_transition≥50, deal_activity_signals≥40
    "distress_opportunity": ... # diligence_health≤40, insider_activity≤40, hiring_velocity≥50
    "sector_wave": ...          # sector_momentum≥75, macro_tailwind≥60, deal_activity_signals≥40
}
```

### AlertEngine interface

```python
class AlertEngine:
    def evaluate(self, company_id: int, prev_score: Optional[float],
                 new_score: float, new_grade: str, prev_grade: Optional[str],
                 convergences: List[Dict]) -> List[TxnProbAlert]:
        """Write alert rows and return them."""
```

### NLQ whitelist

```python
ALLOWED_FIELDS = {
    "probability": "float",
    "raw_composite_score": "float",
    "grade": "string",
    "sector": "string",
    "hq_state": "string",
    "active_signal_count": "int",
}
```

### Narrative generator

```python
class ProbabilityNarrativeGenerator:
    async def generate_narrative(self, company_id: int) -> str  # 3-5 sentences
    async def generate_memo(self, company_id: int) -> Dict  # HTML + sections
    async def generate_sector_briefing(self, sector: str) -> str
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/services/probability_convergence.py | Create | 4+ named patterns + detector |
| app/services/probability_alerts.py | Create | AlertEngine |
| app/services/probability_nlq.py | Create | Claude + keyword fallback |
| app/services/probability_narrative.py | Create | LLM narrative + memo |
| app/services/probability_engine.py | Modify | Call alerts + convergence after score persist |
| app/api/v1/transaction_probability.py | Modify | 5 new endpoints |
| tests/test_spec_047_deal_probability_engine_phase3.py | Create | 16 tests |

## Feedback History

_No corrections yet._
