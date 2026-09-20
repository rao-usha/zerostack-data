"""
Fund -> manager attribution (SPEC_117, ADV tiers added by SPEC_118).

Measured on the live data: **no** PE/VC Form D fund shares a strong identifier
with its manager — the fund vehicle is its own legal entity with its own CIK.
That is still true of Form D, but it is no longer the whole story: on Form ADV
Schedule D 7.B.(1) the adviser lists every private fund it manages, by name,
under its own CRD. That is a claimed list rather than an identifier join, so
the name still has to be unambiguous before it counts:

  tier 1 `adv_exact`      the fund's whole name core is exactly one adviser's
                          Schedule D fund
  tier 2 `adv_family`     several Schedule D rows carry that core, but every
                          claimant belongs to one adviser family (same CRD, or
                          the same adviser name at the same address — the
                          series-LLC / relying-adviser pattern)
  tier 3 `name_core`      the fund name begins with an adviser's normalized
                          name core ("GENSTAR CAPITAL PARTNERS X, L.P." ->
                          "genstar capital"), and that core belongs to exactly
                          one adviser
  tier 4 `related_person` a related person named on the filing matches exactly
                          one adviser's name core (the GP entity is often
                          listed there)
  tier 5 `adv_platform`   the only claimant is a filing platform, and nothing
                          in the fund's name points at the sponsor behind it

A filing platform is an adviser of record for funds it does not sponsor —
AngelList's Platform Advisor LLC files 22,329 of them. Its Schedule D claim is
true and useless: "Singh Capital Rolling Fund - D1" is Singh Capital's fund.
So a platform's claim yields to a name that identifies a different adviser,
and survives only where nothing better exists, under its own tier.

Refusals are counted, never silent. Names are compared whole and normalized,
never fuzzily: a fund that two adviser families could claim is left unlinked.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from app.entities import norm

logger = logging.getLogger(__name__)

# A core shorter than this matches far too much ("capital", "north")
MIN_CORE_LEN = 6
# An exact ADV hit on a two-token core ("summit capital") is a name collision,
# not a fund. The prefix tiers below have their own ambiguity check and keep
# their own threshold.
MIN_ADV_CORE_TOKENS = 3

# (fund_name_core, crd_number, adviser_name_core, address_key)
AdvFundRow = Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]


# How many *other advisers' names* a filer's fund list may carry before it is
# one, and how many funds it must report before the count is allowed to decide.
# Measured on the live ADV data there is a cliff, not a gradient, and real GPs
# score in the single digits (Carlyle 0, KKR 3, Apollo 6, Ares 7) however many
# funds they report — which is why fund count alone cannot be the test either.
PLATFORM_MIN_COLLISIONS = 25
PLATFORM_MIN_FUNDS = 50


def find_platform_advisers(
    rows: Iterable[AdvFundRow],
    core_index: Dict[str, str],
    min_collisions: int = PLATFORM_MIN_COLLISIONS,
    min_funds: int = PLATFORM_MIN_FUNDS,
) -> Set[str]:
    """CRDs whose Schedule D fund names belong to many other advisers.

    Name *affinity* — the share of an adviser's funds carrying its own name —
    looks like the obvious test and does not work: KKR's funds are named "KKR"
    while its adviser record reads "Kohlberg Kravis Roberts & Co. L.P.", so it
    scores 0.000, exactly like a platform. Counting whose *else's* names show
    up separates them, because a GP's list never names its competitors.

    A collision is one borrowed **name**, not one borrowed registration. Big
    brands register many adviser entities — "goldman sachs" is one name across
    a dozen CRDs — so counting CRDs let a single fund row named after a bank
    partner convict a small GP of being a platform. `min_funds` is the second
    guard: a filer with a short list has not demonstrated anything either way.
    """
    stems: Dict[str, Set[str]] = defaultdict(set)
    for core, crd in core_index.items():
        toks = core.split()
        stems[" ".join(toks[:2]) if len(toks) > 1 else toks[0]].add(crd)

    borrowed: Dict[str, Set[str]] = defaultdict(set)
    filed: Counter = Counter()
    for fund_core, crd, _adviser_core, _addr in rows:
        if not fund_core or not crd:
            continue
        filed[crd] += 1
        toks = fund_core.split()
        for k in (2, 1):
            stem = " ".join(toks[:k])
            # the stem is somebody else's name when any CRD holding it is not us
            if stems.get(stem, set()) - {crd}:
                borrowed[crd].add(stem)

    found = {
        crd
        for crd, names in borrowed.items()
        if len(names) >= min_collisions and filed[crd] >= min_funds
    }
    if found:
        logger.info(
            f"[marts:links] filing platforms: "
            + ", ".join(f"{crd} ({len(borrowed[crd])} names over {filed[crd]} funds)"
                        for crd in sorted(found))
        )
    return found


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


# ---------------------------------------------------------------------------
# ADV Schedule D fund index
# ---------------------------------------------------------------------------

class AdvFundIndex(dict):
    """{fund_name_core: crd} for the cores that resolve, plus its own bookkeeping.

    A plain dict so lookups stay `index[core]`; `tiers` says which tier a hit
    earns and `multi_family_cores` remembers what was thrown away, so a refusal
    can be counted instead of looking like a fund nobody claimed.
    """

    def __init__(self, mapping: Optional[Dict[str, str]] = None):
        super().__init__(mapping or {})
        self.tiers: Dict[str, str] = {}
        self.multi_family_cores: Set[str] = set()


def _families(claims: Sequence[Tuple[str, Optional[str], Optional[str]]]) -> List[List[str]]:
    """Group the CRDs claiming one fund name into adviser families.

    One CRD is one adviser. Two CRDs are siblings when they file under the same
    adviser name at the same business address — series LLCs and relying
    advisers, which is what most collisions on a fund name actually are. A
    missing name or address proves nothing, so it never merges families.
    """
    parent = {crd: crd for crd, _name, _addr in claims}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    by_sibling_key: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for crd, adviser_core, address_key in claims:
        if adviser_core and address_key:
            by_sibling_key[(adviser_core, address_key)].append(crd)
    for crds in by_sibling_key.values():
        root = find(crds[0])
        for crd in crds[1:]:
            parent[find(crd)] = root

    groups: Dict[str, List[str]] = defaultdict(list)
    for crd in parent:
        groups[find(crd)].append(crd)
    return list(groups.values())


def _family_crd(family: Sequence[str], funds_filed: Counter) -> str:
    """The CRD a family link is recorded against: the sibling that files the
    most private funds overall — the main filer, not a one-fund relying
    adviser — lowest CRD on a tie so the choice is stable between runs."""
    return min(family, key=lambda crd: (-funds_filed[crd], crd))


def build_adv_fund_index(rows: Iterable[AdvFundRow]) -> AdvFundIndex:
    """ADV Schedule D fund rows -> {fund_name_core: crd}.

    A core claimed by more than one adviser family is dropped; a core claimed
    several times inside one family is kept and marked `adv_family`.
    """
    claims: Dict[str, List[Tuple[str, Optional[str], Optional[str]]]] = defaultdict(list)
    funds_filed: Counter = Counter()
    for fund_core, crd, adviser_core, address_key in rows:
        if fund_core and crd:
            claims[fund_core].append((crd, adviser_core, address_key))
            funds_filed[crd] += 1

    index = AdvFundIndex()
    for core, rows_for_core in claims.items():
        families = _families(rows_for_core)
        if len(families) > 1:
            index.multi_family_cores.add(core)
            continue
        index[core] = _family_crd(families[0], funds_filed)
        index.tiers[core] = "adv_exact" if len(rows_for_core) == 1 else "adv_family"

    by_tier = Counter(index.tiers.values())
    logger.info(
        f"[marts:links] adv fund cores: {len(claims)} seen, {dict(by_tier)}, "
        f"{len(index.multi_family_cores)} dropped as multi-family"
    )
    return index


def match_adv_fund(
    fund_name: Optional[str],
    adv_index: AdvFundIndex,
    master_cores: Iterable[str] = (),
) -> Tuple[Optional[str], Optional[str]]:
    """(crd, tier) for a whole-name Schedule D match, else (None, refusal reason).

    `(None, None)` means the fund simply is not on any adviser's list; a named
    reason means we could have guessed and chose not to.
    """
    core = norm.core(fund_name) if fund_name else None
    if not core or len(core) < MIN_CORE_LEN or len(core.split()) < MIN_ADV_CORE_TOKENS:
        return None, "short_core"
    if core in adv_index:
        return adv_index[core], adv_index.tiers[core]
    if core in adv_index.multi_family_cores:
        return None, "multi_family"
    # A master fund is named by its feeders, so its name on its own attributes
    # the feeder to whoever manages the master — often a different adviser.
    if core in master_cores:
        return None, "master_name_only"
    return None, None


# ---------------------------------------------------------------------------
# name tiers (SPEC_117)
# ---------------------------------------------------------------------------

def _is_token_boundary(fund_core: str, cut: int) -> bool:
    """Does slicing at `cut` end on a whole word (bar a trailing plural)?

    A character cut matches an adviser core that is merely the first letters of
    a longer word: "CARMELINA CAPITAL" -> `carmel` -> Carmel Partners, and
    "HarbourView Royalties Fund" -> `harbour` -> Harbour Group Industries, a
    Missouri industrial holding company. The plural is kept because
    "ASCENT VENTURES LP" really is Ascent Venture Partners.
    """
    if cut == len(fund_core):
        return True
    if fund_core[cut] == " ":
        return True
    return fund_core[cut] == "s" and (cut + 1 == len(fund_core)
                                      or fund_core[cut + 1] == " ")


def match_name_core(fund_name: Optional[str], core_index: Dict[str, str]) -> Optional[str]:
    """Longest unambiguous adviser core that prefixes the fund name core."""
    fund_core = norm.core(fund_name) if fund_name else None
    if not fund_core:
        return None
    for cut in range(len(fund_core), MIN_CORE_LEN - 1, -1):
        if not _is_token_boundary(fund_core, cut):
            continue
        crd = core_index.get(fund_core[:cut])
        if crd:
            return crd
    return None


def match_related_persons(names: Iterable[str], core_index: Dict[str, str]) -> Optional[str]:
    """Adviser matched by a related-person name, or None when 0 or >1 match."""
    hits = {core_index[c] for c in (norm.core(n) for n in names if n) if c and c in core_index}
    return next(iter(hits)) if len(hits) == 1 else None


class Attribution(dict):
    """{fund cik: (crd, tier)} plus the tier and refusal counts behind it."""

    def __init__(self):
        super().__init__()
        self.tiers: Counter = Counter()
        self.refusals: Counter = Counter()


def attribute(
    funds: Iterable[Tuple[str, Optional[str]]],
    related_persons: Dict[str, Iterable[str]],
    core_index: Dict[str, str],
    adv_index: Optional[AdvFundIndex] = None,
    adv_master_cores: Iterable[str] = (),
    platform_crds: Iterable[str] = (),
    can_resolve: Optional[Callable[[str], bool]] = None,
) -> Attribution:
    """{fund cik: (crd, tier)} for the funds that can be attributed."""
    adv_index = adv_index if adv_index is not None else AdvFundIndex()
    master_cores = set(adv_master_cores)
    platforms = set(platform_crds)
    out = Attribution()
    for cik, name in funds:
        crd, reason = match_adv_fund(name, adv_index, master_cores)
        if crd and crd in platforms:
            # The claim is true but says who filed, not who sponsors. Prefer
            # the adviser the fund is named after -- but only if that adviser
            # can actually carry the link. The sponsor comes from the whole
            # ADV roster while only PE/VC-reporting advisers have a pe_firms
            # row, so taking it on faith dropped 234 of 320 funds to no link
            # at all, which is worse than the platform claim it replaced.
            sponsor = match_name_core(name, core_index)
            if sponsor and sponsor != crd:
                if can_resolve is None or can_resolve(sponsor):
                    out[cik] = (sponsor, "name_core")
                    out.tiers["name_core"] += 1
                    out.refusals["platform_displaced"] += 1
                    continue
                out.refusals["platform_sponsor_unresolvable"] += 1
            out[cik] = (crd, "adv_platform")
            out.tiers["adv_platform"] += 1
            continue
        if crd:
            out[cik] = (crd, reason)
            out.tiers[reason] += 1
            continue
        if reason:
            # The refusal is of the ADV tier only: the name tiers below carry
            # their own ambiguity check and are allowed to try.
            out.refusals[reason] += 1
        crd = match_name_core(name, core_index)
        if crd:
            out[cik] = (crd, "name_core")
            out.tiers["name_core"] += 1
            continue
        crd = match_related_persons(related_persons.get(cik, ()), core_index)
        if crd:
            out[cik] = (crd, "related_person")
            out.tiers["related_person"] += 1
    logger.info(
        f"[marts:links] attributed {len(out)} funds: {dict(out.tiers)}, "
        f"refused {dict(out.refusals)}"
    )
    return out
