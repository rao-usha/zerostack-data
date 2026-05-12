"""
50 consumer persona archetypes for the synthetic crowd (SPEC_050 / PLAN_062 addendum 1).

Personas hand-curated to span the demographic × psychographic space rather than
full cartesian product. Anchored on Pew American Trends Panel typology, Nielsen
PRIZM segments, and VALS framework (public references) — none of which we
license, but their dimensions are the well-validated public scaffolding.

Each persona carries:
- Categorical dims (age × income × geography × price_sensitivity × brand_loyalty
  × novelty_seeking) — feed into the distilled tabular regressor
- A ~30-word prompt_summary — feeds the GPT-4o teacher's persona context

Dimensions:
- age_bracket: '18-29' | '30-44' | '45-59' | '60+'
- income_bracket: '<$50K' | '$50K-$100K' | '$100K-$200K' | '>$200K'
- geography: 'urban' | 'suburban' | 'rural'
- price_sensitivity, brand_loyalty, novelty_seeking: 'low' | 'medium' | 'high'
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Dict


@dataclass(frozen=True)
class Persona:
    id: int
    archetype: str
    age_bracket: str
    income_bracket: str
    geography: str
    price_sensitivity: str
    brand_loyalty: str
    novelty_seeking: str
    prompt_summary: str

    def to_dict(self) -> Dict:
        return asdict(self)


PERSONAS: List[Persona] = [
    Persona(1,  "Suburban Mid-Career Family Spender",   "30-44", "$50K-$100K",  "suburban", "medium", "medium", "low",
            "A 35-year-old suburban parent earning ~$75K, balancing brand familiarity against value, slow to adopt new products until peers do."),
    Persona(2,  "Urban Gen-Z Novelty Seeker",            "18-29", "$50K-$100K",  "urban",    "high",   "low",    "high",
            "A 24-year-old urban professional earning $65K, eagerly tries new brands, switches readily, very price-aware on essentials."),
    Persona(3,  "Rural Boomer Brand Loyalist",           "60+",   "$50K-$100K",  "rural",    "low",    "high",   "low",
            "A 67-year-old rural retiree with steady income, sticks with brands used for decades, suspicious of trendy alternatives."),
    Persona(4,  "Wealthy Suburban Empty-Nester",         "60+",   "$100K-$200K", "suburban", "low",    "high",   "low",
            "A 64-year-old suburban empty-nester earning $150K, comfortable buying premium, loyal to established brands, prefers proven."),
    Persona(5,  "Urban Affluent DINK",                   "30-44", ">$200K",      "urban",    "low",    "low",    "high",
            "A 38-year-old urban dual-income-no-kids earning $300K, willing to spend on premium, samples new brands frequently."),
    Persona(6,  "Suburban Gen-X Pragmatist",             "45-59", "$100K-$200K", "suburban", "medium", "high",   "low",
            "A 52-year-old suburban Gen-X parent earning $130K, brand-loyal once trusted, deliberate about purchases, skeptical of marketing."),
    Persona(7,  "Urban Millennial Renter",               "30-44", "$50K-$100K",  "urban",    "high",   "low",    "high",
            "A 33-year-old urban renter earning $70K, price-sensitive on rent-driven budget, open to new brands if cost-effective."),
    Persona(8,  "Rural Working-Class Boomer",            "60+",   "<$50K",       "rural",    "high",   "high",   "low",
            "A 68-year-old rural retiree on $35K fixed income, intensely price-sensitive, loyal to lifetime brands, resistant to change."),
    Persona(9,  "Suburban Millennial Parent",            "30-44", "$100K-$200K", "suburban", "medium", "medium", "low",
            "A 36-year-old suburban parent earning $125K, balances quality and value, slower to switch brands now that kids are involved."),
    Persona(10, "Urban Gen-X Professional",              "45-59", ">$200K",      "urban",    "low",    "medium", "medium",
            "A 50-year-old urban senior professional earning $250K, premium-comfortable, willing to try new brands for clear value-adds."),
    Persona(11, "Suburban Gen-Z Student",                "18-29", "<$50K",       "suburban", "high",   "low",    "high",
            "A 21-year-old suburban college student on $20K budget, ultra-price-sensitive, tries cheap new brands often, no loyalty."),
    Persona(12, "Rural Gen-X Working Family",            "45-59", "$50K-$100K",  "rural",    "high",   "high",   "low",
            "A 49-year-old rural blue-collar worker earning $65K, intensely brand-loyal to value brands, distrustful of urban trends."),
    Persona(13, "Urban Affluent Boomer",                 "60+",   "$100K-$200K", "urban",    "low",    "high",   "low",
            "A 65-year-old urban retired professional earning $150K, prefers established premium brands, loyal but engaged with quality."),
    Persona(14, "Suburban Wealthy Pre-Retiree",          "45-59", ">$200K",      "suburban", "low",    "medium", "medium",
            "A 56-year-old suburban executive earning $280K, premium-comfortable, occasionally tries new brands recommended by peers."),
    Persona(15, "Urban Millennial Eco-Conscious",        "30-44", "$100K-$200K", "urban",    "medium", "medium", "high",
            "A 34-year-old urban professional earning $120K, prioritizes sustainability claims, will pay premium for verified eco brands."),
    Persona(16, "Rural Millennial",                      "30-44", "$50K-$100K",  "rural",    "high",   "high",   "low",
            "A 32-year-old rural worker earning $55K, deeply value-focused, brand-loyal to local/regional names, slow to adopt urban brands."),
    Persona(17, "Suburban Gen-Z Trend-Adopter",          "18-29", "$50K-$100K",  "suburban", "medium", "low",    "high",
            "A 23-year-old suburban early-career professional earning $58K, follows social-media trends, switches brands as trends shift."),
    Persona(18, "Urban Gen-X DIY",                       "45-59", "$50K-$100K",  "urban",    "high",   "low",    "medium",
            "A 51-year-old urban Gen-X DIY enthusiast earning $70K, price-sensitive, switches readily based on reviews and forums."),
    Persona(19, "Suburban Boomer Volume Buyer",          "60+",   "$50K-$100K",  "suburban", "high",   "high",   "low",
            "A 66-year-old suburban Boomer earning $70K, shops warehouse clubs, brand-loyal to value brands, fiercely price-sensitive."),
    Persona(20, "Urban Affluent Millennial Creative",    "30-44", "$100K-$200K", "urban",    "low",    "low",    "high",
            "A 34-year-old urban creative professional earning $140K, samples emerging brands constantly, drops them quickly, premium-tolerant."),
    Persona(21, "Rural Gen-Z Limited-Means",             "18-29", "<$50K",       "rural",    "high",   "medium", "low",
            "A 22-year-old rural Gen-Z worker on $28K, extreme price sensitivity, sticks with affordable familiar brands, low novelty appetite."),
    Persona(22, "Suburban Gen-X Coach/Volunteer Parent", "45-59", "$50K-$100K",  "suburban", "medium", "high",   "low",
            "A 48-year-old suburban Gen-X parent earning $80K, brand-loyal once a brand earns trust in the household, deliberate buyer."),
    Persona(23, "Urban Boomer Cultural Connoisseur",     "60+",   ">$200K",      "urban",    "low",    "medium", "medium",
            "A 68-year-old urban Boomer earning $300K, comfortable with premium, samples niche brands per recommendations, refined."),
    Persona(24, "Suburban Millennial Striver",           "30-44", "$50K-$100K",  "suburban", "high",   "medium", "medium",
            "A 36-year-old suburban Millennial earning $80K, aspirational but budget-constrained, tries premium tiers selectively."),
    Persona(25, "Rural Boomer Skeptic",                  "60+",   "$50K-$100K",  "rural",    "high",   "high",   "low",
            "A 70-year-old rural Boomer earning $60K, distrustful of new brands and marketing claims, stays with longtime favorites."),
    Persona(26, "Urban Gen-Z Side-Hustler",              "18-29", "<$50K",       "urban",    "high",   "low",    "high",
            "A 25-year-old urban Gen-Z gig worker earning $40K, price-sensitive but trend-aware, switches brands constantly."),
    Persona(27, "Suburban Affluent Gen-X",               "45-59", ">$200K",      "suburban", "low",    "high",   "low",
            "A 54-year-old suburban Gen-X earning $250K, brand-loyal to established premium names, sees switching as risk."),
    Persona(28, "Urban Millennial Bargain Hunter",       "30-44", "$50K-$100K",  "urban",    "high",   "low",    "medium",
            "A 37-year-old urban Millennial earning $75K, hunts deals systematically, switches whenever a competitor undercuts price."),
    Persona(29, "Rural Gen-X Outdoors Enthusiast",       "45-59", "$50K-$100K",  "rural",    "medium", "medium", "medium",
            "A 50-year-old rural Gen-X outdoorsperson earning $70K, brand-loyal to gear that earns trust, open to new product categories."),
    Persona(30, "Suburban Gen-Z Influenced",             "18-29", "$50K-$100K",  "suburban", "medium", "low",    "high",
            "A 22-year-old suburban Gen-Z earning $52K, heavily influenced by social media creators, brand allegiance shifts weekly."),
    Persona(31, "Urban Affluent Gen-X Foodie",           "45-59", "$100K-$200K", "urban",    "low",    "low",    "high",
            "A 49-year-old urban Gen-X foodie earning $160K, premium-tolerant, constantly samples new restaurants and CPG brands."),
    Persona(32, "Rural Affluent Landowner",              "45-59", "$100K-$200K", "rural",    "low",    "high",   "low",
            "A 56-year-old rural affluent landowner earning $140K, loyal to regional brands and trusted suppliers, premium-tolerant when needed."),
    Persona(33, "Suburban Working-Class Millennial",     "30-44", "$50K-$100K",  "suburban", "high",   "high",   "low",
            "A 33-year-old suburban Millennial blue-collar earning $60K, brand-loyal to value names that have worked over time."),
    Persona(34, "Urban Gen-X Single",                    "45-59", "$50K-$100K",  "urban",    "medium", "low",    "medium",
            "A 47-year-old urban Gen-X single professional earning $90K, samples brands selectively, low household constraints."),
    Persona(35, "Suburban Gen-Z Aspiring",               "18-29", "<$50K",       "suburban", "high",   "medium", "high",
            "A 24-year-old suburban Gen-Z early-career earning $42K, aspirational about premium brands but priced into entry tiers."),
    Persona(36, "Rural Boomer Frugal Saver",             "60+",   "<$50K",       "rural",    "high",   "high",   "low",
            "A 72-year-old rural Boomer on $32K fixed income, intensely frugal, sticks to lifetime brands, hostile to price increases."),
    Persona(37, "Urban Millennial Wellness Devotee",     "30-44", "$100K-$200K", "urban",    "low",    "high",   "high",
            "A 34-year-old urban Millennial earning $130K, premium-spends on wellness brands aligned with values, brand-loyal in that category."),
    Persona(38, "Suburban Boomer Comfortable Retiree",   "60+",   "$100K-$200K", "suburban", "low",    "high",   "low",
            "A 67-year-old suburban Boomer earning $130K from retirement income, prefers familiar premium brands, resistant to change."),
    Persona(39, "Urban Gen-Z Tech-Native",               "18-29", "$50K-$100K",  "urban",    "medium", "low",    "high",
            "A 26-year-old urban Gen-Z tech worker earning $90K, comfortable trying digital-native brands, low patience for friction."),
    Persona(40, "Rural Millennial Tradesperson",         "30-44", "$50K-$100K",  "rural",    "medium", "medium", "low",
            "A 35-year-old rural Millennial tradesperson earning $72K, value-quality-balance buyer, brand-loyal to tools and supplies."),
    Persona(41, "Suburban Affluent Millennial Tech-Worker", "30-44", ">$200K",   "suburban", "low",    "medium", "high",
            "A 38-year-old suburban Millennial tech worker earning $280K, premium-comfortable, samples emerging digital brands eagerly."),
    Persona(42, "Urban Boomer Empty-Nester",             "60+",   "$50K-$100K",  "urban",    "medium", "high",   "low",
            "A 65-year-old urban Boomer empty-nester earning $80K, downsized budget, brand-loyal to familiar names, selective splurging."),
    Persona(43, "Suburban Gen-X Sandwich-Generation",    "45-59", "$50K-$100K",  "suburban", "high",   "high",   "low",
            "A 52-year-old suburban Gen-X caring for aging parents and teens, earning $85K, price-conscious and brand-loyal to reliable names."),
    Persona(44, "Rural Gen-Z Vocational",                "18-29", "<$50K",       "rural",    "high",   "low",    "medium",
            "A 20-year-old rural Gen-Z vocational worker earning $35K, switches brands on price, moderate openness to new categories."),
    Persona(45, "Urban Affluent Boomer Traveler",        "60+",   ">$200K",      "urban",    "low",    "low",    "high",
            "A 66-year-old urban Boomer earning $350K, retired traveler, samples global brands eagerly, premium-comfortable everywhere."),
    Persona(46, "Suburban Millennial Eco-Family",        "30-44", "$100K-$200K", "suburban", "medium", "medium", "high",
            "A 37-year-old suburban Millennial parent earning $140K, prioritizes sustainable brands for family, willing to switch for values."),
    Persona(47, "Rural Boomer Religious",                "60+",   "$50K-$100K",  "rural",    "low",    "high",   "low",
            "A 71-year-old rural Boomer earning $65K, brand-loyal especially to brands aligned with community values, resistant to change."),
    Persona(48, "Urban Gen-Z Activist",                  "18-29", "$50K-$100K",  "urban",    "medium", "low",    "high",
            "A 25-year-old urban Gen-Z activist earning $60K, boycotts brands over values mismatches, samples mission-aligned alternatives."),
    Persona(49, "Suburban Boomer Health-Conscious",      "60+",   "$50K-$100K",  "suburban", "medium", "high",   "medium",
            "A 64-year-old suburban Boomer earning $75K, health-driven brand loyalty, samples new wellness/health products selectively."),
    Persona(50, "Urban Wealthy Boomer Philanthropist",   "60+",   ">$200K",      "urban",    "low",    "medium", "low",
            "A 70-year-old urban Boomer earning $400K, philanthropic, premium-comfortable, loyal but moves on if brand values shift."),
]


# -----------------------------------------------------------------------
# Quick accessors
# -----------------------------------------------------------------------

def get_persona_by_id(persona_id: int) -> Persona:
    for p in PERSONAS:
        if p.id == persona_id:
            return p
    raise KeyError(f"No persona with id={persona_id}")


def get_all_personas() -> List[Persona]:
    return list(PERSONAS)


def assert_library_valid() -> None:
    """Acceptance-criteria check (T1, T2): 50 personas spanning the space."""
    assert len(PERSONAS) == 50, f"Expected 50 personas, got {len(PERSONAS)}"
    ids = {p.id for p in PERSONAS}
    assert ids == set(range(1, 51)), f"Persona IDs must be 1..50, got {sorted(ids)}"

    # Span check: every (age × geography) cell has ≥1 persona
    cells = {(p.age_bracket, p.geography) for p in PERSONAS}
    expected = {
        (age, geo)
        for age in ["18-29", "30-44", "45-59", "60+"]
        for geo in ["urban", "suburban", "rural"]
    }
    missing = expected - cells
    assert not missing, f"Missing (age × geography) cells: {missing}"
