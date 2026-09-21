"""
pe_people / pe_firm_people from SEC Form D related persons (SPEC_119).

The third mart in the SPEC_117/118 family, and the one that only became
possible once funds had managers: before SPEC_118 just 5,101 of 39,148 fund
vehicles carried a `firm_id`, so the related-person rows had nowhere to attach.

This module is the **sole writer** of `pe_people.source_key`, and of
`pe_firm_people`'s `person_link_method`, `firm_link_method`,
`address_confirmation`, `fund_count`, `filing_count`, `first_seen`,
`last_seen` and `data_source`.

Identity is `(name_norm, firm_id)`, never name alone. Measured head to head,
name alone fuses 139 distinct humans -- `david miller` spans five firm
families, five zips and four states -- while name+firm costs 7.8% duplication,
almost all one GP at two affiliated registrations. A visible duplicate is
recoverable; a silent fusion of two people is not.

The 2,551 pre-existing `pe_people` rows are never merged into: 162 collide by
normalized name and **zero** collide at the same `firm_id`, because the overlap
is a duplicated *firm* master (legacy `KKR` versus `KOHLBERG KRAVIS ROBERTS &
CO. L.P.`). That is a firm-dedup problem, not this load's.
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import date
from typing import Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.entities import norm
from app.marts import names
from app.marts.pe_funds_sec import TIER_RANK

logger = logging.getLogger(__name__)

DATA_SOURCE = "SEC Form D"
PEOPLE_TARGET = "public.pe_people"
LINKS_TARGET = "public.pe_firm_people"
STG_PEOPLE = "mart_pe_people"
STG_LINKS = "mart_pe_firm_people"

# `relationships` is a small closed vocabulary; the title is their sorted union
# so it does not flap when a later filing lists them in another order.
RELATIONSHIP_ORDER = ("Director", "Executive Officer", "Promoter")

# A clarification naming the filer as somebody's agent rather than a principal.
ADMIN_PROSE = re.compile(r"\b(agent|administrat|authorized signator|signatory)", re.I)
# Share of a person's filings that must carry it. Measured: of 9,211
# person-shaped names, 9,162 sit at exactly 0% and 30 at 90-100%; only 8 names
# fall anywhere between 0.25 and 0.90, so this is a cliff, not a knob.
ADMIN_SHARE_MIN = 0.5
# Distinct firm BRANDS before a name is cross-brand. First token, not two:
# Blackstone's four registrations are four distinct two-token stems, so a
# two-token collapse convicts its real GPs of promiscuity.
CROSS_BRAND_MIN = 3

# Weakest label wins, so `person_link_method = 'form_d_signer'` is the clean
# set. NOT called `related_person`: pe_funds.firm_link_method already spells a
# different meaning that way, and both columns sit on the same row.
PERSON_TIERS = ("form_d_signer", "cross_brand", "platform_fund", "fund_admin")

# A firm with this many attributed vehicles that has never reported raising
# more than the cap is running SPVs, not funds. Measured across the 17 firms
# with >=60 funds: platforms top out at $5-14M (Vauban 6.0, Alumni 8.1, Echo
# 8.8, OurCrowd 13.0, EquityBee 14.2) and the next firm up is Brown Advisory at
# $190M, with real GPs running to $18,118M.
#
# Neither half works alone. Fund size by itself classifies Madison Dearborn,
# Vista Equity and Andreessen Horowitz as platforms, because Form D reports
# nothing sold at launch and their 90th percentile is $0.00M. SPEC_118's
# `find_platform_advisers` is the wrong instrument entirely: it detects
# *filing* platforms by borrowed names, and EquityBee's funds carry EquityBee's
# own name.
#
# Known miss, recorded rather than hidden: FORGE GLOBAL ADVISORS is a secondary
# marketplace, but one $397M vehicle puts it over the cap. The >=60 floor also
# means nothing is claimed about smaller firms. This is a floor on what can be
# proven from Form D; ADV Schedule A titles are the real fix.
SPV_MIN_FUNDS = 60
SPV_MAX_RAISE_USD_M = 50

PEOPLE_COLUMNS: List[Tuple[str, str]] = [
    ("source_key", "TEXT"),
    ("full_name", "TEXT"),
    ("first_name", "TEXT"),
    ("last_name", "TEXT"),
    ("city", "TEXT"),
    ("state", "TEXT"),
    ("country", "TEXT"),
    ("is_active", "BOOLEAN"),
    ("data_sources", "JSON"),
    ("last_verified", "DATE"),
    ("updated_at", "TIMESTAMP"),
]
PEOPLE_NAMES = [c for c, _ in PEOPLE_COLUMNS]

LINK_COLUMNS: List[Tuple[str, str]] = [
    ("firm_id", "INTEGER"),
    ("person_id", "INTEGER"),
    ("title", "TEXT"),
    ("is_current", "BOOLEAN"),
    ("person_link_method", "TEXT"),
    ("firm_link_method", "TEXT"),
    ("address_confirmation", "TEXT"),
    ("fund_count", "INTEGER"),
    ("filing_count", "INTEGER"),
    ("first_seen", "DATE"),
    ("last_seen", "DATE"),
    ("data_source", "TEXT"),
    ("updated_at", "TIMESTAMP"),
]
LINK_NAMES = [c for c, _ in LINK_COLUMNS]

# One row per (related person, filing). `filed_at` comes from form_d_filings,
# never `loaded_at`, which is NOW() and identical across a whole release -- it
# would make first_seen/last_seen change on every reload and break idempotence.
SOURCE_SQL = """
SELECT rp.accession_number, rp.first_name, rp.middle_name, rp.last_name,
       rp.city, rp.state_or_country, rp.zip_code, rp.relationships,
       rp.relationship_clarification,
       f.firm_id, f.cik AS fund_cik, f.firm_link_method,
       fil.filed_at
FROM form_d_related_persons rp
JOIN form_d_issuers i
  ON i.accession_number = rp.accession_number AND i.is_primary
JOIN pe_funds f ON f.cik = i.cik
LEFT JOIN form_d_filings fil ON fil.accession_number = rp.accession_number
WHERE f.firm_id IS NOT NULL
-- Ordered so the per-pair picks below are reproducible. Without it Postgres
-- returns whichever row the plan emits first, so the spelling of a name and
-- its city/state could change between runs on identical data and the merge
-- would report updates forever.
ORDER BY rp.accession_number, rp.related_person_seq
"""

FIRM_SQL = """
SELECT p.id, p.name, p.crd_number,
       (SELECT r.main_office_postal_code
          FROM sec_adv_roster_snapshots r
         WHERE ltrim(r.crd_number, '0') = ltrim(p.crd_number, '0')
         ORDER BY r.roster_date DESC LIMIT 1) AS postal_code,
       (SELECT r.main_office_state
          FROM sec_adv_roster_snapshots r
         WHERE ltrim(r.crd_number, '0') = ltrim(p.crd_number, '0')
         ORDER BY r.roster_date DESC LIMIT 1) AS adv_state
FROM pe_firms p
"""

# The SPV-platform measurement, over non-platform-linked funds only so an
# AngelList filer's 6,370 vehicles cannot drag a sponsor's numbers around.
SPV_SQL = f"""
SELECT firm_id,
       count(*) AS n,
       max(COALESCE(final_close_usd_millions, 0)) AS max_raise
FROM pe_funds
WHERE firm_id IS NOT NULL
  AND COALESCE(firm_link_method, 'x') <> 'adv_platform'
GROUP BY firm_id
HAVING count(*) >= {SPV_MIN_FUNDS}
"""


def _brand(firm_name: Optional[str]) -> str:
    """First token of the firm name -- the only family signal available.

    `pe_firms` has no `parent_firm_id`, so one brand is many rows. When that
    column exists this should be recomputed against it and will shrink.
    """
    folded = norm._fold(firm_name or "")
    return folded.split()[0] if folded else ""


def _title(relationship_sets: Sequence[Sequence[str]]) -> str:
    """Sorted union of `relationships` across the pair's filings.

    The union, not the latest filing: 4% of pairs change their relationship set
    between filings, and "latest" would flap. `relationships` is non-empty on
    100% of rows, so this never returns '' -- which matters because
    `pe_firm_people.title` is NOT NULL and 1,090 legacy rows satisfy it with an
    empty string, a latent bug this mart does not copy.
    """
    seen: Set[str] = set()
    for rels in relationship_sets:
        for r in rels or ():
            if r:
                seen.add(r.strip())
    ordered = [r for r in RELATIONSHIP_ORDER if r in seen]
    ordered += sorted(r for r in seen if r not in RELATIONSHIP_ORDER)
    return ", ".join(ordered)


def find_spv_platform_firms(conn) -> Set[int]:
    """Firm ids that file many vehicles and never raise real money."""
    found = {
        r["firm_id"]
        for r in conn.execute(text(SPV_SQL)).mappings()
        if float(r["max_raise"] or 0) < SPV_MAX_RAISE_USD_M
    }
    logger.info(f"[marts:pe_people] spv platform firms: {len(found)}")
    return found


class _Pair:
    """One (person, firm) pair, accumulated across its filings."""

    __slots__ = ("first", "middle", "last", "display", "city", "state", "zips",
                 "rels", "admin_hits", "filings", "funds", "fund_tiers",
                 "first_seen", "last_seen", "best_acc")

    def __init__(self):
        self.first = self.middle = self.last = self.display = None
        self.city = self.state = None
        # the accession whose spelling currently wins; see _observe
        self.best_acc = ""
        self.zips: Set[str] = set()
        self.rels: List[Sequence[str]] = []
        self.admin_hits = 0
        self.filings = 0
        self.funds: Set[str] = set()
        self.fund_tiers: List[Optional[str]] = []
        self.first_seen: Optional[date] = None
        self.last_seen: Optional[date] = None


def _classify_refusal(first, middle, last) -> Tuple[Optional[str], Optional[Tuple[str, str]]]:
    """(refusal counter name, rescued (first, last)) -- order is the contract.

    The same rules in a different order produce different counters for the same
    kept set (measured: placeholder-first 23,748/11,486, entity-first
    34,692/542), so this order is part of the spec, not an implementation
    detail.
    """
    if names.is_see_clarification(first, last):
        return "refused_see_clarification", None
    if names.is_placeholder(first, last):
        rescued = names.rescue_name_in_last_field(first, last)
        if rescued:
            return None, rescued
        return "refused_placeholder_name", None
    is_entity, only_glue = names.looks_like_entity(first, last)
    if is_entity:
        return ("refused_entity_name_dotted" if only_glue else "refused_entity_name"), None
    if not names.token_count_ok(names.name_norm(first, last)):
        return "refused_name_too_long", None
    return None, None


def build(conn, today: Optional[date] = None, dry_run: bool = False) -> Dict[str, int]:
    """Upsert SEC-derived people and firm links. Counts every refusal.

    `dry_run` runs the whole pipeline and reports without writing, so the ship
    gate can read the numbers first.
    """
    today = today or date.today()

    firms = {r["id"]: dict(r) for r in conn.execute(text(FIRM_SQL)).mappings()}
    spv_firms = find_spv_platform_firms(conn)

    stats: Counter = Counter()
    pairs: Dict[Tuple[str, int], _Pair] = {}
    name_brands: Dict[str, Set[str]] = defaultdict(set)
    name_filings: Counter = Counter()
    name_admin: Counter = Counter()

    base_rows = 0
    for r in conn.execute(text(SOURCE_SQL)).mappings():
        base_rows += 1
        first, middle, last = r["first_name"], r["middle_name"], r["last_name"]
        refusal, rescued = _classify_refusal(first, middle, last)
        if refusal:
            stats[refusal] += 1
            continue
        if rescued:
            stats["rescued_name_in_last_field"] += 1
            first, last = rescued
            middle = None

        key_name = names.name_norm(first, last)
        firm_id = r["firm_id"]
        pair = pairs.get((key_name, firm_id))
        if pair is None:
            pair = pairs[(key_name, firm_id)] = _Pair()
        # One person is spelled several ways across their filings ("John M."
        # vs "John"), and their address changes. Take the latest filing's
        # version, chosen by accession rather than by arrival order, so a
        # rerun on unchanged data produces byte-identical rows.
        acc = r["accession_number"] or ""
        if acc >= pair.best_acc:
            pair.best_acc = acc
            pair.first, pair.middle, pair.last = first, middle, last
            pair.display = names.display_name(first, middle, last)
            pair.city = (r["city"] or None) or pair.city
            pair.state = norm.state2(r["state_or_country"]) or pair.state
        pair.filings += 1
        pair.rels.append(r["relationships"] or ())
        if r["fund_cik"]:
            pair.funds.add(r["fund_cik"])
        pair.fund_tiers.append(r["firm_link_method"])
        z = norm.zip5(r["zip_code"])
        if z:
            pair.zips.add(z)
        filed = r["filed_at"]
        filed = filed.date() if hasattr(filed, "date") else filed
        if filed:
            pair.first_seen = min(pair.first_seen or filed, filed)
            pair.last_seen = max(pair.last_seen or filed, filed)

        name_filings[key_name] += 1
        if ADMIN_PROSE.search(r["relationship_clarification"] or ""):
            name_admin[key_name] += 1
        brand = _brand((firms.get(firm_id) or {}).get("name"))
        if brand:
            name_brands[key_name].add(brand)

    stats["kept"] = sum(p.filings for p in pairs.values())

    # The house rule, asserted rather than trusted: a counter that does not
    # close the sum means a class is being dropped silently.
    counted = (stats["kept"] + stats["refused_placeholder_name"]
               + stats["refused_see_clarification"] + stats["refused_entity_name"]
               + stats["refused_entity_name_dotted"] + stats["refused_name_too_long"])
    if counted != base_rows:
        raise RuntimeError(
            f"refusal ledger does not close: counted {counted} of {base_rows} rows"
        )

    people_rows, link_rows = [], []
    tiers: Counter = Counter()
    for (key_name, firm_id), pair in sorted(pairs.items()):
        admin_share = (name_admin[key_name] / name_filings[key_name]
                       if name_filings[key_name] else 0.0)
        all_platform = bool(pair.fund_tiers) and all(
            t == "adv_platform" for t in pair.fund_tiers)
        if admin_share >= ADMIN_SHARE_MIN:
            method = "fund_admin"
        elif len(name_brands[key_name]) >= CROSS_BRAND_MIN:
            method = "cross_brand"
        elif all_platform:
            method = "platform_fund"
        else:
            method = "form_d_signer"
        tiers[method] += 1

        ranked = [t for t in pair.fund_tiers if t]
        fund_tier = min(ranked, key=lambda t: TIER_RANK.get(t, 99)) if ranked else None

        firm = firms.get(firm_id) or {}
        firm_zip = norm.zip5(firm.get("postal_code"))
        firm_state = norm.state2(firm.get("adv_state"))
        if firm_zip and pair.zips:
            confirmation = "zip5" if firm_zip in pair.zips else (
                "state" if firm_state and firm_state == pair.state else "none")
        elif firm_state and pair.state:
            confirmation = "state" if firm_state == pair.state else "none"
        else:
            confirmation = "unknown"
        stats[f"address_{confirmation}"] += 1

        source_key = f"secformd:{firm_id}:{key_name}"
        people_rows.append((
            source_key, pair.display, (pair.first or "").strip() or None,
            (pair.last or "").strip() or None, pair.city, pair.state, None, True,
            '["SEC Form D"]',
            # from the filings, never today(): a rerun must not rewrite the row
            pair.last_seen, today,
        ))
        link_rows.append((
            firm_id, None, _title(pair.rels), True, method, fund_tier,
            confirmation, len(pair.funds), pair.filings,
            pair.first_seen, pair.last_seen, DATA_SOURCE, today,
        ))

    stats.update({
        "base_rows": base_rows,
        "candidate_pairs": len(people_rows),
        "distinct_people": len({r[0].rsplit(":", 1)[-1] for r in people_rows}),
        "firms_covered": len({r[0] for r in link_rows}),
        "spv_platform_firms": len(spv_firms),
        "role_type_underivable": len(link_rows),
        **{f"tier_{t}": tiers[t] for t in PERSON_TIERS},
    })

    # Fold the legacy names the same way the key is folded. Comparing raw
    # lowercased names against folded keys silently reports zero: "O'Brien"
    # folds to "o brien" and would never match itself.
    incoming = {r[0].rsplit(":", 1)[-1] for r in people_rows}
    legacy = conn.execute(text(
        "SELECT first_name, last_name, full_name FROM pe_people WHERE source_key IS NULL"
    )).fetchall()
    stats["name_collides_existing_person"] = sum(
        1 for f, l, full in legacy
        if (names.name_norm(f, l) if (f or l) else norm._fold(full)) in incoming
    )
    stats["collides_same_firm"] = 0  # the key carries firm_id, so this cannot be non-zero

    inserted_people = updated_people = inserted_links = updated_links = 0
    if not dry_run and people_rows:
        create_staging(conn, STG_PEOPLE, PEOPLE_COLUMNS)
        copy_rows(conn, STG_PEOPLE, PEOPLE_NAMES, people_rows)
        inserted_people, updated_people = merge_staging(
            conn, STG_PEOPLE, PEOPLE_TARGET, PEOPLE_NAMES, ["source_key"],
            compare_columns=[c for c in PEOPLE_NAMES
                             if c not in ("data_sources", "updated_at")],
            conflict_where="source_key IS NOT NULL",
        )
        drop_staging(conn, STG_PEOPLE)

        # person_id is a serial the first merge generates, so the ids have to
        # be read back by source_key -- they cannot be staged.
        ids = {r["source_key"]: r["id"] for r in conn.execute(text(
            "SELECT id, source_key FROM pe_people WHERE source_key LIKE 'secformd:%'"
        )).mappings()}
        resolved = []
        for people_row, link_row in zip(people_rows, link_rows):
            person_id = ids.get(people_row[0])
            if person_id is None:
                stats["link_person_missing"] += 1
                continue
            resolved.append((link_row[0], person_id) + tuple(link_row[2:]))

        create_staging(conn, STG_LINKS, LINK_COLUMNS)
        copy_rows(conn, STG_LINKS, LINK_NAMES, resolved)
        inserted_links, updated_links = merge_staging(
            conn, STG_LINKS, LINKS_TARGET, LINK_NAMES, ["firm_id", "person_id"],
            compare_columns=[c for c in LINK_NAMES if c != "updated_at"],
        )
        drop_staging(conn, STG_LINKS)

        conn.execute(text("UPDATE pe_firms SET is_spv_platform = TRUE WHERE id = ANY(:ids)"),
                     {"ids": sorted(spv_firms)})
        conn.execute(text(
            "UPDATE pe_firms SET is_spv_platform = FALSE "
            "WHERE is_spv_platform IS DISTINCT FROM FALSE AND NOT (id = ANY(:ids))"),
            {"ids": sorted(spv_firms)})

    stats.update({
        "inserted_people": inserted_people, "updated_people": updated_people,
        "inserted_links": inserted_links, "updated_links": updated_links,
        "dry_run": int(dry_run),
    })
    logger.info(f"[marts:pe_people] {dict(stats)}")
    return dict(stats)
