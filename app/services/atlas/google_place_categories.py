"""SPEC_101 — NAICS → Google Place Type mapping.

Used by ratings.fetch_ratings() to translate a thesis NAICS into the
Place Type the Google Places (New) searchNearby endpoint expects.

The Place Type list is documented at:
  https://developers.google.com/maps/documentation/places/web-service/place-types

Strategy:
  1. Try an exact-match NAICS lookup.
  2. Walk the NAICS hierarchy upward (442110 → 4421 → 442 → 44)
     until a match is found.
  3. Return None if nothing matches. Caller skips the Google call
     entirely — no quota burned on a NAICS we can't categorise.
"""
from __future__ import annotations

from typing import Optional, Dict


# Hand-curated. NAICS → single Place Type string. We prefer the
# most-specific Place Type per industry. When Google offers multiple
# valid types (e.g. "restaurant" vs "fast_food_restaurant"), we pick
# the broader one so the rating aggregate covers more locations.
NAICS_TO_GOOGLE_CATEGORY: Dict[str, str] = {
    # ── Retail ────────────────────────────────────────────────────
    "441110": "car_dealer",
    "4411":   "car_dealer",
    "441":    "car_dealer",
    "442110": "furniture_store",
    "442":    "furniture_store",
    "443":    "electronics_store",
    "4451":   "supermarket",
    "445110": "supermarket",
    "44512":  "convenience_store",
    "445120": "convenience_store",
    "4453":   "liquor_store",
    "4471":   "gas_station",
    "447110": "gas_station",
    "448":    "clothing_store",
    "4482":   "shoe_store",
    "4483":   "jewelry_store",
    "451211": "book_store",
    "4521":   "department_store",
    "452":    "department_store",
    "4531":   "florist",
    "45391":  "pet_store",
    "446110": "drugstore",
    # ── Food service ──────────────────────────────────────────────
    "722511": "restaurant",
    "722513": "fast_food_restaurant",
    "722515": "cafe",
    "7225":   "restaurant",
    "7224":   "bar",
    "722320": "catering_service",
    "311811": "bakery",
    # ── Hospitality ───────────────────────────────────────────────
    "7211":   "hotel",
    "721":    "lodging",
    # ── Health care ───────────────────────────────────────────────
    "622":    "hospital",
    "6211":   "doctor",
    "6212":   "dentist",
    "621498": "medical_lab",
    "6231":   "nursing_home",
    # ── Personal / fitness ────────────────────────────────────────
    "713940": "gym",
    "812111": "barber_shop",
    "812112": "hair_salon",
    "812199": "spa",
    "8111":   "car_repair",
    "811192": "car_wash",
    # ── Real estate / pro services ───────────────────────────────
    "5411":   "lawyer",
    "5412":   "accounting",
    "531":    "real_estate_agency",
    "5311":   "real_estate_agency",
    # ── Entertainment ────────────────────────────────────────────
    "512131": "movie_theater",
    "7132":   "casino",
    # ── Education / child care ───────────────────────────────────
    "6111":   "school",
    "6113":   "university",
    "6244":   "child_care_agency",
}


def naics_to_google_category(naics: Optional[str]) -> Optional[str]:
    """Resolve a NAICS code (any depth) to a Google Place Type.
    Walks the NAICS hierarchy upward until a match is found.

    Returns None when nothing matches. The caller must skip the
    Google API call in that case so we don't burn quota on a NAICS
    that has no sensible category mapping.
    """
    if not naics:
        return None
    s = str(naics).strip()
    if not s:
        return None
    # 1. Exact match
    if s in NAICS_TO_GOOGLE_CATEGORY:
        return NAICS_TO_GOOGLE_CATEGORY[s]
    # 2. Walk upward: 442110 → 44211 → 4421 → 442 → 44
    while len(s) > 1:
        s = s[:-1]
        if s in NAICS_TO_GOOGLE_CATEGORY:
            return NAICS_TO_GOOGLE_CATEGORY[s]
    return None
