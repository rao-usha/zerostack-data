"""SPEC_100 — Resolve a thesis industry label to a NAICS code prefix.

Used by the CBP-backed competition lookup to translate a free-text
industry (e.g. "Furniture stores", "coffee shop", "warehouse") into a
NAICS prefix the SQL aggregator can `LIKE`-match.

Resolution order:
  1. If the caller passes `naics_hint` and it looks like a valid 2-6
     digit NAICS code, use it verbatim.
  2. Else lowercase + strip the label and look up each keyword in
     INDUSTRY_NAICS_MAP (longest keyword wins, so "coffee shop" beats
     "shop").
  3. Else return None — the caller falls back to NAICS='00' (the
     focal+neighbours total). Never raise.

The keyword table covers ~30 industries that show up most often in
Decision Map theses. It is hand-curated — accuracy over coverage. For
anything not in the table, the caller still gets a useful number
(county-total establishments) instead of nothing.

NAICS codes follow the 2017 vintage (NAICS2017), which is what the
Census CBP API still uses through the 2022 reporting year.
"""
from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

# Keywords → NAICS prefix. Longer keys are tried first so that
# "coffee shop" matches before "shop" alone. Keys must be lowercase.
INDUSTRY_NAICS_MAP: Dict[str, str] = {
    # ── Retail (NAICS 44-45) ───────────────────────────────────────
    "furniture store":            "442",
    "home furnishings":           "442",
    "furniture":                  "442",
    "electronics store":          "443",
    "appliance store":            "443",
    "electronics":                "443",
    "building material":          "444",
    "garden center":              "4441",
    "grocery store":              "4451",
    "grocery":                    "4451",
    "convenience store":          "44512",
    "supermarket":                "4451",
    "specialty food":             "4452",
    "liquor store":               "4453",
    "clothing store":             "448",
    "apparel":                    "448",
    "clothing":                   "448",
    "shoe store":                 "4482",
    "jewelry":                    "4483",
    "sporting goods":             "45111",
    "book store":                 "451211",
    "bookstore":                  "451211",
    "department store":           "4521",
    "general merchandise":        "452",
    "florist":                    "4531",
    "office supplies":            "4532",
    "gift shop":                  "45322",
    "pet store":                  "45391",
    "auto dealer":                "4411",
    "car dealer":                 "4411",
    "auto parts":                 "4413",
    "gas station":                "4471",
    # ── Food service (NAICS 722) ───────────────────────────────────
    "coffee shop":                "722515",
    "coffee":                     "722515",
    "cafe":                       "722513",
    "café":                       "722513",
    "fast food":                  "722513",
    "restaurant":                 "7225",
    "full service restaurant":    "722511",
    "limited service":            "722513",
    "bar":                        "7224",
    "drinking place":             "7224",
    "bakery":                     "311811",
    "catering":                   "722320",
    # ── Lodging / hospitality (NAICS 721) ─────────────────────────
    "hotel":                      "7211",
    "motel":                      "7211",
    "lodging":                    "721",
    "bed and breakfast":          "721191",
    "resort":                     "7211",
    # ── Manufacturing (NAICS 31-33) ───────────────────────────────
    "manufacturing":              "31-33",  # CBP combined sector code
    "food processing":            "311",
    "beverage manufacturing":     "3121",
    "brewery":                    "31212",
    "winery":                     "31213",
    "distillery":                 "31214",
    "textile":                    "313",
    "apparel manufacturing":      "315",
    "wood product":               "321",
    "paper":                      "322",
    "printing":                   "323",
    "chemical":                   "325",
    "plastic":                    "326",
    "metal manufacturing":        "332",
    "machinery":                  "333",
    "semiconductor":              "334413",
    "electronics manufacturing":  "334",
    "auto manufacturing":         "3361",
    "aerospace":                  "3364",
    # ── Logistics / warehousing (NAICS 48-49) ─────────────────────
    "warehouse":                  "493",
    "warehousing":                "493",
    "distribution":               "493",
    "logistics":                  "493",
    "fulfillment":                "493",
    "trucking":                   "484",
    "freight":                    "484",
    "courier":                    "492",
    # ── Health care (NAICS 62) ────────────────────────────────────
    "hospital":                   "622",
    "clinic":                     "6211",
    "doctor":                     "6211",
    "physician":                  "6211",
    "dentist":                    "6212",
    "urgent care":                "621498",
    "nursing home":               "6231",
    "assisted living":            "6233",
    "pharmacy":                   "446110",
    "drug store":                 "446110",
    # ── Professional services (NAICS 54) ──────────────────────────
    "law firm":                   "5411",
    "legal":                      "5411",
    "accounting":                 "5412",
    "architecture":               "5413",
    "engineering":                "541330",
    "consulting":                 "5416",
    "advertising":                "5418",
    "design":                     "5414",
    # ── Tech / data (NAICS 51, 518) ──────────────────────────────
    "data center":                "518210",
    "cloud":                      "518210",
    "software":                   "5112",
    "telecom":                    "517",
    # ── Real estate (NAICS 53) ────────────────────────────────────
    "real estate":                "531",
    "office building":            "5311",
    "apartment":                  "5311",
    "self storage":               "53113",
    # ── Education (NAICS 61) ──────────────────────────────────────
    "school":                     "6111",
    "college":                    "6113",
    "university":                 "6113",
    "child care":                 "6244",
    "daycare":                    "6244",
    # ── Arts / recreation (NAICS 71) ──────────────────────────────
    "gym":                        "713940",
    "fitness":                    "713940",
    "movie theater":              "512131",
    "amusement":                  "7131",
    "casino":                     "7132",
    # ── Construction (NAICS 23) ───────────────────────────────────
    "construction":               "23",
    "general contractor":         "236",
    "homebuilder":                "2361",
    # ── Personal services (NAICS 81) ──────────────────────────────
    "barber":                     "812111",
    "salon":                      "812112",
    "spa":                        "812199",
    "auto repair":                "8111",
    "car wash":                   "811192",
    "dry cleaning":               "8123",
}

# Cached sorted keys (longest first) so multi-word matches win.
_SORTED_KEYS = sorted(INDUSTRY_NAICS_MAP.keys(), key=len, reverse=True)

# NAICS prefix shape: 2-6 digits. The "31-33" range is the only legal
# multi-sector code; we normalize it to "31" for LIKE prefix matching
# since CBP stores 31, 32, 33 as separate sector rows.
_NAICS_RE = re.compile(r"^\d{2,6}$")


def _normalize(s: Optional[str]) -> str:
    """Lowercase + strip + collapse internal whitespace + drop a few
    accent marks. Cheap, no external deps."""
    if not s:
        return ""
    s = s.lower().strip()
    # Light accent folding so "café" matches the "café" key (and also
    # "cafe" which is in the table separately).
    folds = (("é", "e"), ("è", "e"), ("ñ", "n"), ("ü", "u"), ("ö", "o"))
    for a, b in folds:
        s = s.replace(a, b)
    s = re.sub(r"\s+", " ", s)
    return s


def industry_to_naics(label: Optional[str],
                       naics_hint: Optional[str] = None) -> Optional[str]:
    """Resolve a thesis industry label to a NAICS prefix.

    Returns None when nothing matches — the caller falls back to
    NAICS='00' (county total establishments).

    `naics_hint`: if the caller already has a clean NAICS code (e.g.
    from the thesis's `industry_naics` field), use it verbatim.
    """
    # 1. Honour an explicit NAICS code.
    if naics_hint:
        h = str(naics_hint).strip()
        # CBP stores 2-6 digit codes; collapse a passed-in "31-33"
        # range to "31" for prefix matching.
        if h == "31-33":
            return "31"
        if _NAICS_RE.match(h):
            return h
    # 2. Keyword match on a normalized label.
    norm = _normalize(label)
    if not norm:
        return None
    for key in _SORTED_KEYS:
        # Word-boundary match so "bar" doesn't match "barber".
        if re.search(rf"\b{re.escape(key)}\b", norm):
            return INDUSTRY_NAICS_MAP[key]
    return None


def naics_label_short(naics: str) -> str:
    """Quick fallback label for the UI when the full taxonomies lookup
    isn't worth the cost. Returns a generic family name for a 2-digit
    sector, or the raw code for deeper levels."""
    if not naics:
        return ""
    sector = naics[:2]
    return {
        "11": "Agriculture",
        "21": "Mining & extraction",
        "22": "Utilities",
        "23": "Construction",
        "31": "Manufacturing",
        "32": "Manufacturing",
        "33": "Manufacturing",
        "42": "Wholesale trade",
        "44": "Retail trade",
        "45": "Retail trade",
        "48": "Transportation",
        "49": "Warehousing & courier",
        "51": "Information",
        "52": "Finance & insurance",
        "53": "Real estate",
        "54": "Professional services",
        "55": "Management of companies",
        "56": "Admin & support services",
        "61": "Educational services",
        "62": "Health care",
        "71": "Arts & recreation",
        "72": "Accommodation & food",
        "81": "Other services",
        "92": "Public administration",
    }.get(sector, naics)
