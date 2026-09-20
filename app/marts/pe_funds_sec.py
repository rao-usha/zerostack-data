"""
pe_funds from Form D (SPEC_117), attributed with Form ADV Schedule D (SPEC_118).

One row per PE/VC fund vehicle (the Form D issuer CIK), from its most recent
filing — 65k of 168k offerings are amendments, so counting filings instead of
deduping to the issuer would multiply-count the same fund.

This build is the **sole writer** of `pe_funds.firm_id`: the merge overwrites
the column from the staged row, so a second mart writing links here would have
them reset to NULL on the next Form D run. It is also the only writer of
`firm_link_method`, which records the tier each link came from. An existing
link to a different firm is left alone and counted, never downgraded.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.entities import norm
from app.marts import links

logger = logging.getLogger(__name__)

DATA_SOURCE = "SEC Form D"
STAGING = "mart_pe_funds"
TARGET = "public.pe_funds"
ADV_MART = "public.sec_adv_private_funds"
FUND_TYPES = ("Private Equity Fund", "Venture Capital Fund")

COLUMNS: List[Tuple[str, str]] = [
    ("cik", "TEXT"),
    ("firm_id", "INTEGER"),
    ("name", "TEXT"),
    ("vintage_year", "INTEGER"),
    ("target_size_usd_millions", "NUMERIC"),
    ("final_close_usd_millions", "NUMERIC"),
    ("strategy", "TEXT"),
    ("status", "TEXT"),
    ("first_close_date", "DATE"),
    ("sec_file_number", "TEXT"),
    ("data_source", "TEXT"),
    ("updated_at", "TIMESTAMP"),
    ("firm_link_method", "TEXT"),
]
COLUMN_NAMES = [c for c, _ in COLUMNS]

# newest filing per fund vehicle
SOURCE_SQL = f"""
SELECT DISTINCT ON (i.cik)
       i.cik, i.entity_name, o.investment_fund_type, o.date_of_first_sale,
       o.total_offering_amount, o.total_amount_sold, o.is_indefinite, o.file_num,
       o.accession_number
FROM form_d_offerings o
JOIN form_d_issuers i ON i.accession_number = o.accession_number AND i.is_primary
WHERE o.investment_fund_type IN {FUND_TYPES!r}
-- accession_number last so the pick is deterministic: `loaded_at` is NOW(),
-- identical for every row of one release, and an amendment usually repeats
-- its original's date_of_first_sale, so without it the winning filing -- and
-- with it entity_name, and so firm_id -- could change between runs.
ORDER BY i.cik, o.date_of_first_sale DESC NULLS LAST, o.loaded_at DESC,
         o.accession_number DESC
"""

ADVISER_SQL = """
SELECT DISTINCT ON (crd_number) crd_number, legal_name, business_name
FROM sec_adv_roster_snapshots
WHERE crd_number IS NOT NULL
ORDER BY crd_number, roster_date DESC
"""

RELATED_SQL = f"""
SELECT i.cik, rp.first_name, rp.middle_name, rp.last_name
FROM form_d_offerings o
JOIN form_d_issuers i ON i.accession_number = o.accession_number AND i.is_primary
JOIN form_d_related_persons rp ON rp.accession_number = o.accession_number
WHERE o.investment_fund_type IN {FUND_TYPES!r}
"""

# The adviser's address comes from the roster, not Schedule D — 7.B.(1) has no
# address at all. CRDs are matched unpadded but reported in the roster's
# spelling, which is what pe_firms.crd_number carries. fund_name_core is
# already norm.core() of the fund name, computed by the loader.
ADV_INDEX_SQL = f"""
WITH latest_adviser AS (
    SELECT DISTINCT ON (crd_number) crd_number, legal_name, business_name,
           main_office_street1, main_office_street2, main_office_postal_code
    FROM sec_adv_roster_snapshots
    WHERE crd_number IS NOT NULL
    ORDER BY crd_number, roster_date DESC
)
SELECT m.fund_name_core, m.master_fund_name,
       COALESCE(a.crd_number, m.crd_number) AS crd_number,
       COALESCE(a.business_name, a.legal_name, fil.legal_name) AS adviser_name,
       a.main_office_street1, a.main_office_street2, a.main_office_postal_code
FROM {ADV_MART} m
LEFT JOIN latest_adviser a ON ltrim(a.crd_number, '0') = ltrim(m.crd_number, '0')
LEFT JOIN sec_adv_filings fil ON fil.filing_id = m.filing_id
"""

# Link strength, lowest wins. The adviser's own fund list beats any name
# guess -- except a filing platform's, which is the weakest thing we record.
TIER_RANK = {"adv_exact": 1, "adv_family": 2, "name_core": 3,
             "related_person": 4, "adv_platform": 5}
# A link stamped with a method this mart does not own came from somewhere else
# (a human, another pipeline) and outranks everything we can infer.
FOREIGN_METHOD_RANK = 0
# A link with no method at all is this mart's own pre-SPEC_118 name guess, so
# the ADV tiers are allowed to correct it.
LEGACY_METHOD_RANK = 98


def _rank(method: Optional[str]) -> int:
    if method is None:
        return LEGACY_METHOD_RANK
    return TIER_RANK.get(method, FOREIGN_METHOD_RANK)

EXISTING_LINKS_SQL = """
SELECT cik, firm_id, firm_link_method
FROM pe_funds
WHERE cik IS NOT NULL AND firm_id IS NOT NULL
"""


def _millions(value) -> Optional[float]:
    return float(value) / 1_000_000 if value is not None else None


def _load_related(conn) -> Dict[str, List[str]]:
    related: Dict[str, List[str]] = defaultdict(list)
    for r in conn.execute(text(RELATED_SQL)).mappings():
        name = " ".join(x for x in (r["first_name"], r["middle_name"], r["last_name"]) if x).strip()
        if name:
            related[r["cik"]].append(name)
    return related


def _firm_ids_by_crd(conn) -> Dict[str, int]:
    return {
        r["crd_number"]: r["id"]
        for r in conn.execute(
            text("SELECT id, crd_number FROM pe_firms WHERE crd_number IS NOT NULL")
        ).mappings()
    }


def _load_adv_index(conn, core_index: Optional[Dict[str, str]] = None
                    ) -> Tuple[links.AdvFundIndex, Set[str], int, Set[str]]:
    """(index, master-fund cores, rows read, platform CRDs) from the Schedule D mart."""
    if conn.execute(text(f"SELECT to_regclass('{ADV_MART}')")).scalar() is None:
        # Before migration 0010 there is nothing to read; say so rather than
        # reporting the ADV tiers as having found nothing.
        logger.warning(f"[marts:pe_funds] {ADV_MART} missing — ADV tiers skipped")
        return links.AdvFundIndex(), set(), 0, set()

    rows: List[links.AdvFundRow] = []
    masters: Set[str] = set()
    read = 0
    for r in conn.execute(text(ADV_INDEX_SQL)).mappings():
        read += 1
        street, _unit = norm.street_norm(r["main_office_street1"], r["main_office_street2"])
        postal = norm.zip5(r["main_office_postal_code"])
        address_key = f"{street}|{postal}" if street and postal else None
        rows.append(
            (r["fund_name_core"], r["crd_number"], norm.core(r["adviser_name"]), address_key)
        )
        master_core = norm.core(r["master_fund_name"]) if r["master_fund_name"] else None
        if master_core:
            masters.add(master_core)
    platforms = links.find_platform_advisers(rows, core_index) if core_index else set()
    return links.build_adv_fund_index(rows), masters, read, platforms


def _existing_links(conn) -> Dict[str, Tuple[int, Optional[str]]]:
    return {
        r["cik"]: (r["firm_id"], r["firm_link_method"])
        for r in conn.execute(text(EXISTING_LINKS_SQL)).mappings()
    }


def build(conn, today: Optional[date] = None, dry_run: bool = False) -> Dict[str, int]:
    """Upsert SEC-derived funds, attributing the ones we can. Counts by tier.

    `dry_run` runs the whole attribution and reports the tier and agreement
    counts without writing — the ship gate reads those before anything lands.
    """
    today = today or date.today()
    funds = [dict(r) for r in conn.execute(text(SOURCE_SQL)).mappings()]
    advisers = [
        (r["crd_number"], r["legal_name"], r["business_name"])
        for r in conn.execute(text(ADVISER_SQL)).mappings()
    ]

    core_index = links.build_core_index(advisers)
    adv_index, master_cores, adv_rows, platforms = _load_adv_index(conn, core_index)
    firm_ids = _firm_ids_by_crd(conn)
    attributed = links.attribute(
        [(f["cik"], f["entity_name"]) for f in funds],
        _load_related(conn),
        core_index,
        adv_index,
        master_cores,
        platforms,
        can_resolve=firm_ids.__contains__,
    )
    existing = _existing_links(conn)

    rows, tiers, agree = [], defaultdict(int), defaultdict(int)
    for f in funds:
        cik = f["cik"]
        crd_tier = attributed.get(cik)
        firm_id = firm_ids.get(crd_tier[0]) if crd_tier else None
        method = crd_tier[1] if (crd_tier and firm_id) else None

        prior = existing.get(cik)
        if prior:
            prior_id, prior_method = prior
            if firm_id is None:
                agree["kept_existing"] += 1
            elif firm_id != prior_id:
                agree["disagreed"] += 1
            else:
                agree["agreed"] += 1
            # Keep whichever link came from the stronger evidence. Every
            # pre-existing link here was written by this same mart (the query
            # filters on cik), so a stale name-prefix guess must not outrank
            # the adviser's own Schedule D claim. Equal rank -> keep what is
            # already stored, so reruns stay stable. The rank comparison runs
            # whether or not the firms agree: gating it on disagreement meant
            # that agreeing with a human relabelled their `manual` link as the
            # mart's own tier and destroyed the provenance.
            if _rank(method) >= _rank(prior_method):
                firm_id, method = prior_id, prior_method
        elif firm_id is not None:
            agree["new_links"] += 1

        if method:
            tiers[method] += 1
        first_sale = f.get("date_of_first_sale")
        strategy = "Private Equity" if f["investment_fund_type"] == "Private Equity Fund" else "Venture Capital"
        rows.append(
            (
                cik,
                firm_id,
                (f.get("entity_name") or "").strip() or None,
                first_sale.year if first_sale else None,
                None if f.get("is_indefinite") else _millions(f.get("total_offering_amount")),
                _millions(f.get("total_amount_sold")),
                strategy,
                "Active",
                first_sale,
                (f.get("file_num") or "").strip() or None,
                DATA_SOURCE,
                today,
                method,
            )
        )
    rows = [r for r in rows if r[2]]
    linked = sum(1 for r in rows if r[1] is not None)

    inserted = updated = 0
    if not dry_run:
        create_staging(conn, STAGING, COLUMNS)
        copy_rows(conn, STAGING, COLUMN_NAMES, rows)
        inserted, updated = merge_staging(
            conn, STAGING, TARGET, COLUMN_NAMES, ["cik"], conflict_where="cik IS NOT NULL"
        )
        drop_staging(conn, STAGING)

    stats = {
        "candidates": len(rows),
        "inserted": inserted,
        "updated": updated,
        "adv_index_rows": adv_rows,
        "linked_adv_exact": tiers["adv_exact"],
        "linked_adv_family": tiers["adv_family"],
        "linked_name_core": tiers["name_core"],
        "linked_related_person": tiers["related_person"],
        "linked_adv_platform": tiers["adv_platform"],
        "platform_advisers": len(platforms),
        "unlinked": len(rows) - linked,
        "new_links": agree["new_links"],
        "agreed": agree["agreed"],
        "disagreed": agree["disagreed"],
        "kept_existing": agree["kept_existing"],
        "refused_short_core": attributed.refusals["short_core"],
        "refused_multi_family": attributed.refusals["multi_family"],
        "refused_master_name_only": attributed.refusals["master_name_only"],
        "platform_displaced": attributed.refusals["platform_displaced"],
        "platform_sponsor_unresolvable":
            attributed.refusals["platform_sponsor_unresolvable"],
        "dry_run": int(dry_run),
    }
    logger.info(f"[marts:pe_funds] {stats}")
    return stats
