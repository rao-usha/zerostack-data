"""
40 business-event scenarios for the synthetic crowd (SPEC_050 / PLAN_062 addendum 1).

Scenarios distributed across the categories PE firms actually decide on:
- pricing (8): price-up/down at various magnitudes, premium/discount tier launches, dynamic pricing
- product (8): new SKU, line extensions, packaging/ingredient/format changes, bundles
- brand (8): PE acquisition, rename, repositioning, scandal, recall, sustainability, endorsement, controversy
- geographic (4): urban/rural/international expansion, new channel
- competitor (6): new entrant, exit, price cut, product launch, scandal, acquisition
- operational (6): loyalty program, app launch, subscription, faster delivery, return policy, service change

Each scenario carries an `outcomes_applicable` list — for example, churn_probability
is only meaningful for scenarios where the persona has an existing relationship
with the brand to churn from (pricing changes, brand events, operational changes
for current customers), not for new-product or geographic-expansion scenarios.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional


ALL_OUTCOMES = [
    "purchase_intent",
    "wtp_delta",
    "sentiment",
    "wom_amplitude",
    "churn_probability",
]

# For new-relationship scenarios (no existing customer to churn), exclude churn.
NO_CHURN = ["purchase_intent", "wtp_delta", "sentiment", "wom_amplitude"]


@dataclass(frozen=True)
class Scenario:
    id: int
    category: str
    event_type: str
    magnitude: Optional[float]
    description: str
    outcomes_applicable: List[str] = field(default_factory=lambda: list(ALL_OUTCOMES))

    def to_dict(self) -> Dict:
        return asdict(self)


SCENARIOS: List[Scenario] = [
    # -------------------------------------------------------------------
    # Pricing (8) — existing-customer events, all 5 outcomes
    # -------------------------------------------------------------------
    Scenario(1,  "pricing", "price_up_10pct",  0.10,
             "The brand you regularly buy raises its price by 10%, with no other changes.", ALL_OUTCOMES),
    Scenario(2,  "pricing", "price_up_20pct",  0.20,
             "The brand you regularly buy raises its price by 20%, with no other changes.", ALL_OUTCOMES),
    Scenario(3,  "pricing", "price_up_30pct",  0.30,
             "The brand you regularly buy raises its price by 30%, with no other changes.", ALL_OUTCOMES),
    Scenario(4,  "pricing", "price_down_5pct", -0.05,
             "The brand you regularly buy lowers its price by 5%, with no other changes.", ALL_OUTCOMES),
    Scenario(5,  "pricing", "price_down_10pct", -0.10,
             "The brand you regularly buy lowers its price by 10%, with no other changes.", ALL_OUTCOMES),
    Scenario(6,  "pricing", "premium_tier_launch", None,
             "The brand you regularly buy introduces a premium tier at 2× the price with added features and quality.", ALL_OUTCOMES),
    Scenario(7,  "pricing", "discount_tier_launch", None,
             "The brand you regularly buy introduces a discount tier at 60% of the price with reduced features.", ALL_OUTCOMES),
    Scenario(8,  "pricing", "dynamic_pricing_intro", None,
             "The brand you regularly buy moves to dynamic/surge pricing — prices vary by time of day and demand.", ALL_OUTCOMES),

    # -------------------------------------------------------------------
    # Product (8) — mix of new-relationship and existing-customer
    # -------------------------------------------------------------------
    Scenario(9,  "product", "new_sku_launch", None,
             "A brand you've heard of launches a new product in a category you currently buy elsewhere.", NO_CHURN),
    Scenario(10, "product", "line_extension", None,
             "The brand you regularly buy extends its line with a related new variant (e.g., new flavor, color, size).", ALL_OUTCOMES),
    Scenario(11, "product", "premium_add", None,
             "The brand you regularly buy adds a premium version (better materials, higher quality, premium price).", ALL_OUTCOMES),
    Scenario(12, "product", "value_add", None,
             "The brand you regularly buy adds a value version (good-enough quality, lower price).", ALL_OUTCOMES),
    Scenario(13, "product", "packaging_change", None,
             "The brand you regularly buy redesigns its packaging — new look, same product inside.", ALL_OUTCOMES),
    Scenario(14, "product", "ingredient_change", None,
             "The brand you regularly buy reformulates with a different key ingredient (claimed similar or better quality).", ALL_OUTCOMES),
    Scenario(15, "product", "format_change", None,
             "The brand you regularly buy changes format (e.g., bottle → pouch, in-store → digital-first delivery).", ALL_OUTCOMES),
    Scenario(16, "product", "bundle_launch", None,
             "The brand you regularly buy starts selling its products bundled together at a small discount vs. individual.", ALL_OUTCOMES),

    # -------------------------------------------------------------------
    # Brand (8) — existing relationship; reputation/identity events
    # -------------------------------------------------------------------
    Scenario(17, "brand", "pe_acquisition", None,
             "The brand you regularly buy is acquired by a large private equity firm; news is in the trade press.", ALL_OUTCOMES),
    Scenario(18, "brand", "rename", None,
             "The brand you regularly buy renames itself (same product, new name and visual identity).", ALL_OUTCOMES),
    Scenario(19, "brand", "repositioning", None,
             "The brand you regularly buy repositions itself upmarket with new advertising aimed at a younger affluent audience.", ALL_OUTCOMES),
    Scenario(20, "brand", "scandal", None,
             "The brand you regularly buy is hit by a public scandal — executive misconduct allegations in mainstream press.", ALL_OUTCOMES),
    Scenario(21, "brand", "product_recall", None,
             "The brand you regularly buy issues a product recall for a safety issue affecting a recent batch.", ALL_OUTCOMES),
    Scenario(22, "brand", "sustainability_claim", None,
             "The brand you regularly buy launches a high-profile sustainability initiative (carbon-neutral, ethical sourcing).", ALL_OUTCOMES),
    Scenario(23, "brand", "celebrity_endorsement", None,
             "The brand you regularly buy signs a major celebrity endorsement (think Taylor Swift / LeBron James tier).", ALL_OUTCOMES),
    Scenario(24, "brand", "political_controversy", None,
             "The brand you regularly buy takes a public stance on a polarizing political issue, generating controversy.", ALL_OUTCOMES),

    # -------------------------------------------------------------------
    # Geographic (4) — mostly new-relationship (no churn)
    # -------------------------------------------------------------------
    Scenario(25, "geographic", "urban_expansion", None,
             "A brand you've heard of opens its first location/availability in a major nearby urban area.", NO_CHURN),
    Scenario(26, "geographic", "rural_expansion", None,
             "A brand you've heard of expands to small towns and rural markets near you (previously urban-only).", NO_CHURN),
    Scenario(27, "geographic", "international_arrival", None,
             "A well-known international brand (European or Asian) launches in the US in your area for the first time.", NO_CHURN),
    Scenario(28, "geographic", "new_channel", None,
             "The brand you regularly buy adds a new sales channel — they were retail-only, now they're available on Amazon/DTC too.", ALL_OUTCOMES),

    # -------------------------------------------------------------------
    # Competitor (6) — mix of new-relationship and switching scenarios
    # -------------------------------------------------------------------
    Scenario(29, "competitor", "new_entrant", None,
             "A new competitor enters the category you currently buy, with a similar product at a similar price.", ALL_OUTCOMES),
    Scenario(30, "competitor", "competitor_exit", None,
             "A long-standing competing brand in this category exits the market — only your usual brand and one other remain.", ALL_OUTCOMES),
    Scenario(31, "competitor", "competitor_price_cut", None,
             "A competing brand permanently cuts its price by 15%, while your usual brand stays the same.", ALL_OUTCOMES),
    Scenario(32, "competitor", "competitor_product_launch", None,
             "A competing brand launches a clearly improved version of the product you buy — better features at similar price.", ALL_OUTCOMES),
    Scenario(33, "competitor", "competitor_scandal", None,
             "A competing brand in this category is hit by a public scandal (recall, ethics issue, lawsuit).", ALL_OUTCOMES),
    Scenario(34, "competitor", "competitor_acquired", None,
             "A competing brand is acquired by a major conglomerate; press speculates the product will change.", ALL_OUTCOMES),

    # -------------------------------------------------------------------
    # Operational (6) — existing-customer experience changes
    # -------------------------------------------------------------------
    Scenario(35, "operational", "loyalty_program_launch", None,
             "The brand you regularly buy launches a loyalty/rewards program with points and member-only perks.", ALL_OUTCOMES),
    Scenario(36, "operational", "app_launch", None,
             "The brand you regularly buy launches a mobile app for ordering, tracking, and customer service.", ALL_OUTCOMES),
    Scenario(37, "operational", "subscription_model", None,
             "The brand you regularly buy introduces a subscription option — auto-replenishment at a 10% discount.", ALL_OUTCOMES),
    Scenario(38, "operational", "faster_delivery", None,
             "The brand you regularly buy upgrades delivery — was 3-5 days, now next-day at no extra charge.", ALL_OUTCOMES),
    Scenario(39, "operational", "return_policy_tighter", None,
             "The brand you regularly buy tightens its return policy — was 30-day no-questions, now 14-day with restocking fee.", ALL_OUTCOMES),
    Scenario(40, "operational", "service_change", None,
             "The brand you regularly buy outsources customer service overseas to cut costs; wait times increase.", ALL_OUTCOMES),
]


# -----------------------------------------------------------------------
# Accessors
# -----------------------------------------------------------------------

def get_scenario_by_id(scenario_id: int) -> Scenario:
    for s in SCENARIOS:
        if s.id == scenario_id:
            return s
    raise KeyError(f"No scenario with id={scenario_id}")


def get_all_scenarios() -> List[Scenario]:
    return list(SCENARIOS)


def assert_library_valid() -> None:
    """Acceptance-criteria check (T3, T4): 40 scenarios across 6 categories."""
    assert len(SCENARIOS) == 40, f"Expected 40 scenarios, got {len(SCENARIOS)}"
    ids = {s.id for s in SCENARIOS}
    assert ids == set(range(1, 41)), f"Scenario IDs must be 1..40, got {sorted(ids)}"

    expected_counts = {
        "pricing": 8,
        "product": 8,
        "brand": 8,
        "geographic": 4,
        "competitor": 6,
        "operational": 6,
    }
    actual_counts = {}
    for s in SCENARIOS:
        actual_counts[s.category] = actual_counts.get(s.category, 0) + 1
    assert actual_counts == expected_counts, (
        f"Category counts mismatch. Expected {expected_counts}, got {actual_counts}"
    )

    # T4: required fields all present
    for s in SCENARIOS:
        assert s.category, f"Scenario {s.id} missing category"
        assert s.event_type, f"Scenario {s.id} missing event_type"
        assert s.description, f"Scenario {s.id} missing description"
        assert s.outcomes_applicable, f"Scenario {s.id} missing outcomes_applicable"
        for outcome in s.outcomes_applicable:
            assert outcome in ALL_OUTCOMES, (
                f"Scenario {s.id} outcome '{outcome}' not in ALL_OUTCOMES"
            )
