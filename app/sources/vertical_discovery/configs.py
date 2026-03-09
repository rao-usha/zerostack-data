"""
Vertical Discovery — configuration registry for all supported verticals.

Each VerticalConfig defines the search terms, scoring weights, revenue
benchmarks, and NPPES taxonomy codes for a PE roll-up vertical.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class VerticalConfig:
    """Immutable configuration for a discovery vertical."""

    slug: str                                    # URL-safe identifier
    display_name: str                            # "Dental Practices"
    table_name: str                              # "dental_prospects"
    search_terms: List[str]                      # Yelp search queries
    yelp_categories: str                         # Yelp category filter
    prospect_weights: Dict[str, float]           # scoring weights (sum to 1.0)
    revenue_benchmarks: Dict[Optional[str], int] # price tier -> base revenue
    nppes_taxonomy_codes: Optional[List[str]]    # None for non-healthcare
    has_nppes_enrichment: bool
    model_version: str = "v1.0"
    national_median_agi: float = 45_000.0        # IRS SOI baseline


# ---------------------------------------------------------------------------
# Vertical definitions
# ---------------------------------------------------------------------------

DENTAL = VerticalConfig(
    slug="dental",
    display_name="Dental Practices",
    table_name="dental_prospects",
    search_terms=["dentist", "dental office", "dental clinic"],
    yelp_categories="dentists,cosmeticdentists,generaldentistry,pediatricdentists,orthodontists",
    prospect_weights={
        "zip_affluence": 0.25,
        "yelp_rating": 0.25,
        "review_volume": 0.20,
        "low_competition": 0.15,
        "price_tier": 0.15,
    },
    revenue_benchmarks={
        "$": 300_000,
        "$$": 600_000,
        "$$$": 1_200_000,
        "$$$$": 2_000_000,
        None: 500_000,
    },
    nppes_taxonomy_codes=[
        "1223G0001X",  # General Practice Dentistry
        "1223D0001X",  # Dental Public Health
        "1223E0200X",  # Endodontics
        "1223X0400X",  # Orthodontics
        "1223P0221X",  # Pediatric Dentistry
        "1223P0300X",  # Periodontics
        "1223S0112X",  # Oral & Maxillofacial Surgery
    ],
    has_nppes_enrichment=True,
)

VETERINARY = VerticalConfig(
    slug="veterinary",
    display_name="Veterinary Clinics",
    table_name="veterinary_prospects",
    search_terms=["veterinarian", "vet clinic", "animal hospital"],
    yelp_categories="vet,animalhospitals,emergencypethospitals",
    prospect_weights={
        "zip_affluence": 0.25,
        "yelp_rating": 0.30,
        "review_volume": 0.20,
        "low_competition": 0.15,
        "price_tier": 0.10,
    },
    revenue_benchmarks={
        "$": 400_000,
        "$$": 800_000,
        "$$$": 1_500_000,
        "$$$$": 2_500_000,
        None: 600_000,
    },
    nppes_taxonomy_codes=None,
    has_nppes_enrichment=False,
)

HVAC = VerticalConfig(
    slug="hvac",
    display_name="HVAC Contractors",
    table_name="hvac_prospects",
    search_terms=["hvac", "air conditioning repair", "heating repair"],
    yelp_categories="hvac,heating,airconditioning",
    prospect_weights={
        "zip_affluence": 0.20,
        "yelp_rating": 0.25,
        "review_volume": 0.25,
        "low_competition": 0.20,
        "price_tier": 0.10,
    },
    revenue_benchmarks={
        "$": 500_000,
        "$$": 1_000_000,
        "$$$": 2_500_000,
        "$$$$": 4_000_000,
        None: 800_000,
    },
    nppes_taxonomy_codes=None,
    has_nppes_enrichment=False,
)

CAR_WASH = VerticalConfig(
    slug="car_wash",
    display_name="Car Washes",
    table_name="car_wash_prospects",
    search_terms=["car wash", "auto detailing"],
    yelp_categories="carwash,autodetailing",
    prospect_weights={
        "zip_affluence": 0.30,
        "yelp_rating": 0.20,
        "review_volume": 0.20,
        "low_competition": 0.20,
        "price_tier": 0.10,
    },
    revenue_benchmarks={
        "$": 200_000,
        "$$": 500_000,
        "$$$": 1_200_000,
        "$$$$": 2_000_000,
        None: 400_000,
    },
    nppes_taxonomy_codes=None,
    has_nppes_enrichment=False,
)

PHYSICAL_THERAPY = VerticalConfig(
    slug="physical_therapy",
    display_name="Physical Therapy Clinics",
    table_name="physical_therapy_prospects",
    search_terms=["physical therapy", "physical therapist", "PT clinic"],
    yelp_categories="physicaltherapy,sportsmedicine,rehabilitation",
    prospect_weights={
        "zip_affluence": 0.25,
        "yelp_rating": 0.25,
        "review_volume": 0.20,
        "low_competition": 0.15,
        "price_tier": 0.15,
    },
    revenue_benchmarks={
        "$": 300_000,
        "$$": 700_000,
        "$$$": 1_400_000,
        "$$$$": 2_200_000,
        None: 550_000,
    },
    nppes_taxonomy_codes=[
        "225100000X",  # Physical Therapist
        "2251C2600X",  # Cardiopulmonary PT
        "2251E1300X",  # Electrophysiology PT
        "2251G0304X",  # Geriatric PT
        "2251H1200X",  # Hand PT
        "2251N0400X",  # Neurology PT
        "2251S0007X",  # Sports PT
    ],
    has_nppes_enrichment=True,
)


# ---------------------------------------------------------------------------
# Registry — keyed by slug for URL routing
# ---------------------------------------------------------------------------

VERTICAL_REGISTRY: Dict[str, VerticalConfig] = {
    v.slug: v
    for v in [DENTAL, VETERINARY, HVAC, CAR_WASH, PHYSICAL_THERAPY]
}

# Common constants reused from medspa pattern
PRICE_SCORE_MAP = {
    "$$$$": 100,
    "$$$": 75,
    "$$": 50,
    "$": 25,
}
DEFAULT_PRICE_SCORE = 37.5

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

SATURATION_THRESHOLDS = [
    (1.0, "Undersaturated"),
    (2.5, "Balanced"),
    (5.0, "Saturated"),
    (float("inf"), "Oversaturated"),
]
