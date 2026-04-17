"""
Deal Probability Engine — Signal Taxonomy (SPEC 045, PLAN_059 Phase 1).

The "wiring diagram" that maps 12 probability signals to either existing
scorers or new computers (built in Phase 2). Default weights sum to 1.0;
sector-specific overrides (Healthcare, Technology, Industrial) adjust
weights while preserving the sum-to-one invariant.

This module is pure data — no DB access, no imports of scorers. The
probability_engine orchestrator dispatches based on scorer_source.
"""

from typing import Dict


# ---------------------------------------------------------------------------
# Signal taxonomy
# ---------------------------------------------------------------------------
#
# Each signal is mapped to one of three scorer_source kinds:
#   - "existing_scorer": reuses a class in app.ml.* or app.services.*
#   - "query": direct DB query (e.g., pe_market_signals table)
#   - "new_computer": built in Phase 2 (app.services.probability_signal_computers)
#
# default_weight: base weight (must sum to 1.0 across all signals)
# refresh_cadence: how often the signal should be re-scored
#   - "daily" for fast-moving signals (insider activity, job postings, exec transitions)
#   - "weekly" for slower signals (financial health, exit readiness)
# ---------------------------------------------------------------------------

SIGNAL_TAXONOMY: Dict[str, Dict] = {
    "financial_health": {
        "default_weight": 0.15,
        "scorer_source": "existing_scorer",
        "scorer_class": "app.ml.company_scorer.CompanyScorer",
        "scorer_method": "score_company",
        "score_field": "composite_score",
        "description": "Revenue, profitability, growth trajectory",
        "refresh_cadence": "weekly",
    },
    "exit_readiness": {
        "default_weight": 0.12,
        "scorer_source": "existing_scorer",
        "scorer_class": "app.ml.exit_readiness_scorer.ExitReadinessScorer",
        "scorer_method": "score_company",
        "score_field": "overall_score",
        "description": "Financial health, ops maturity, market timing for exit",
        "refresh_cadence": "weekly",
    },
    "acquisition_attractiveness": {
        "default_weight": 0.12,
        "scorer_source": "existing_scorer",
        "scorer_class": "app.ml.acquisition_target_scorer.AcquisitionTargetScorer",
        "scorer_method": "score_company",
        "score_field": "overall_score",
        "description": "Growth + market + management gap + deal activity + sector momentum",
        "refresh_cadence": "weekly",
    },
    "exec_transition": {
        "default_weight": 0.10,
        "scorer_source": "existing_scorer",
        "scorer_class": "app.services.exec_signal_scorer.ExecSignalScorer",
        "scorer_method": "score_company",
        "score_field": "transition_score",
        "description": "C-suite/VP hiring patterns, management buildout",
        "refresh_cadence": "daily",
    },
    "sector_momentum": {
        "default_weight": 0.10,
        "scorer_source": "query",
        "query_target": "pe_market_signals",
        "score_field": "momentum_score",
        "description": "Sector deal flow, multiples trends, 0-100 momentum",
        "refresh_cadence": "weekly",
    },
    "diligence_health": {
        "default_weight": 0.08,
        "scorer_source": "existing_scorer",
        "scorer_class": "app.services.company_diligence_scorer.CompanyDiligenceScorer",
        "scorer_method": "score_company",
        "score_field": "score",
        "description": "6-factor health: revenue concentration, env, safety, legal, innovation, growth",
        "refresh_cadence": "weekly",
    },
    "insider_activity": {
        "default_weight": 0.08,
        "scorer_source": "new_computer",
        "computer_class": "app.services.probability_signal_computers.InsiderActivityComputer",
        "description": "Net insider buying/selling in trailing 90 days (Form 4)",
        "refresh_cadence": "daily",
    },
    "hiring_velocity": {
        "default_weight": 0.07,
        "scorer_source": "new_computer",
        "computer_class": "app.services.probability_signal_computers.HiringVelocityComputer",
        "description": "Senior hiring intensity + corp dev postings + headcount growth",
        "refresh_cadence": "daily",
    },
    "deal_activity_signals": {
        "default_weight": 0.05,
        "scorer_source": "new_computer",
        "computer_class": "app.services.probability_signal_computers.DealActivitySignalComputer",
        "description": "Form D capital raises + corp dev titles + pe_deals in same sector",
        "refresh_cadence": "daily",
    },
    "innovation_velocity": {
        "default_weight": 0.05,
        "scorer_source": "new_computer",
        "computer_class": "app.services.probability_signal_computers.InnovationVelocityComputer",
        "description": "USPTO patent filing rate + GitHub commit velocity",
        "refresh_cadence": "weekly",
    },
    "founder_risk": {
        "default_weight": 0.05,
        "scorer_source": "new_computer",
        "computer_class": "app.services.probability_signal_computers.FounderRiskComputer",
        "description": "Founder age, co-founder departures, succession indicators",
        "refresh_cadence": "weekly",
    },
    "macro_tailwind": {
        "default_weight": 0.03,
        "scorer_source": "new_computer",
        "computer_class": "app.services.probability_signal_computers.MacroTailwindComputer",
        "description": "Convergence region score for HQ state + sector macro momentum",
        "refresh_cadence": "weekly",
    },
}


# ---------------------------------------------------------------------------
# Sector weight overrides
# ---------------------------------------------------------------------------
#
# Each sector's overrides produce a complete weight set (must sum to 1.0).
# We store sparse overrides (only the deltas) and fill in remaining signals
# with proportional adjustment in get_weights_for_sector().
#
# Rationale:
#   - Healthcare: heavier on exec_transition (CMO/CFO turnover precedes PE exits)
#     and diligence_health (regulatory/compliance weight matters more)
#   - Technology: heavier on innovation + hiring (product velocity is the signal)
#   - Industrial: heavier on macro + exit_readiness (cyclicality dominates)
# ---------------------------------------------------------------------------

SECTOR_WEIGHT_OVERRIDES: Dict[str, Dict[str, float]] = {
    "Healthcare": {
        "exec_transition": 0.15,
        "diligence_health": 0.12,
        "innovation_velocity": 0.03,
        "founder_risk": 0.03,
    },
    "Technology": {
        "innovation_velocity": 0.10,
        "hiring_velocity": 0.10,
        "founder_risk": 0.08,
        "macro_tailwind": 0.02,
    },
    "Industrial": {
        "macro_tailwind": 0.08,
        "exit_readiness": 0.15,
        "innovation_velocity": 0.02,
        "hiring_velocity": 0.04,
    },
}


def get_weights_for_sector(sector: str) -> Dict[str, float]:
    """
    Return the full weight mapping for a sector.

    Applies sector-specific overrides to the default weights, then rescales
    the non-overridden signals proportionally so the final sum is exactly 1.0.
    If the sector has no overrides (or None is passed), returns defaults.
    """
    defaults = {k: v["default_weight"] for k, v in SIGNAL_TAXONOMY.items()}

    if not sector or sector not in SECTOR_WEIGHT_OVERRIDES:
        return defaults

    overrides = SECTOR_WEIGHT_OVERRIDES[sector]
    result = dict(defaults)

    # Apply overrides
    for signal_key, override_weight in overrides.items():
        if signal_key in result:
            result[signal_key] = override_weight

    # Rescale non-overridden signals so total sums to 1.0
    overridden_total = sum(overrides.get(k, 0) for k in overrides)
    non_overridden_keys = [k for k in result if k not in overrides]
    non_overridden_default_total = sum(defaults[k] for k in non_overridden_keys)
    target_non_overridden_total = 1.0 - overridden_total

    if non_overridden_default_total > 0 and non_overridden_keys:
        scale = target_non_overridden_total / non_overridden_default_total
        for k in non_overridden_keys:
            result[k] = defaults[k] * scale

    return result


def get_signal_metadata(signal_key: str) -> Dict:
    """Return the full metadata dict for a signal."""
    return SIGNAL_TAXONOMY.get(signal_key, {})


def list_signals_by_refresh_cadence(cadence: str) -> list:
    """Return signal keys with the given refresh cadence (daily, weekly)."""
    return [k for k, v in SIGNAL_TAXONOMY.items() if v.get("refresh_cadence") == cadence]
