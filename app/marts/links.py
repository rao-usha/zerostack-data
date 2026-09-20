"""
Fund -> manager attribution (SPEC_117).

Measured on the live data: **no** PE/VC Form D fund shares a strong identifier
with its manager — the fund vehicle is its own legal entity with its own CIK.
So attribution falls back to names, and only where a name is unambiguous:

  tier 1 `name_core`      the fund name begins with an adviser's normalized
                          name core ("GENSTAR CAPITAL PARTNERS X, L.P." ->
                          "genstar capital"), and that core belongs to exactly
                          one adviser
  tier 2 `related_person` a related person named on the filing matches exactly
                          one adviser's name core (the GP entity is often
                          listed there)

A fund that two advisers could claim is left unlinked. Coverage is ~15%; the
rest of the funds are loaded with firm_id NULL rather than guessed.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, Iterable, Optional, Tuple

from app.entities import norm

logger = logging.getLogger(__name__)

# A core shorter than this matches far too much ("capital", "north")
MIN_CORE_LEN = 6


def build_core_index(advisers: Iterable[Tuple[str, Optional[str], Optional[str]]]) -> Dict[str, str]:
    """(crd, legal_name, business_name) rows -> {name_core: crd}, ambiguous cores dropped."""
    by_core: Dict[str, set] = defaultdict(set)
    for crd, legal_name, business_name in advisers:
        for raw in (legal_name, business_name):
            core = norm.core(raw) if raw else None
            if core and len(core) >= MIN_CORE_LEN:
                by_core[core].add(crd)
    index = {core: next(iter(crds)) for core, crds in by_core.items() if len(crds) == 1}
    logger.info(f"[marts:links] adviser cores: {len(by_core)} seen, {len(index)} unambiguous")
    return index


def match_name_core(fund_name: Optional[str], core_index: Dict[str, str]) -> Optional[str]:
    """Longest unambiguous adviser core that prefixes the fund name core."""
    fund_core = norm.core(fund_name) if fund_name else None
    if not fund_core:
        return None
    for cut in range(len(fund_core), MIN_CORE_LEN - 1, -1):
        crd = core_index.get(fund_core[:cut])
        if crd:
            return crd
    return None


def match_related_persons(names: Iterable[str], core_index: Dict[str, str]) -> Optional[str]:
    """Adviser matched by a related-person name, or None when 0 or >1 match."""
    hits = {core_index[c] for c in (norm.core(n) for n in names if n) if c and c in core_index}
    return next(iter(hits)) if len(hits) == 1 else None


def attribute(
    funds: Iterable[Tuple[str, Optional[str]]],
    related_persons: Dict[str, Iterable[str]],
    core_index: Dict[str, str],
) -> Dict[str, Tuple[str, str]]:
    """{fund cik: (crd, tier)} for the funds that can be attributed."""
    out: Dict[str, Tuple[str, str]] = {}
    for cik, name in funds:
        crd = match_name_core(name, core_index)
        if crd:
            out[cik] = (crd, "name_core")
            continue
        crd = match_related_persons(related_persons.get(cik, ()), core_index)
        if crd:
            out[cik] = (crd, "related_person")
    tiers = defaultdict(int)
    for _crd, tier in out.values():
        tiers[tier] += 1
    logger.info(f"[marts:links] attributed {len(out)} funds: {dict(tiers)}")
    return out
