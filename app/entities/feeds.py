"""
Feeds: SEC bulk tables (and Form 5500 sponsors) -> core.source_record (SPEC_116, SPEC_147).

SPEC_154: EDGAR records carry `sec_filers.lei`; GLEIF LEI records (`gleif_lei_record`, CC0) and
USAspending recipient UEIs (`usaspending_awards`) are fed one record per LEI / UEI. LEI and UEI
are gated keys in the resolver (resolve_core.GATED_KEY_TYPES): they join two CIKs only when the
SPEC_150 gate corroborates the pair. Either table absent -> that feed is skipped and reported.

One row per (source, native id). Only identifiers travel; names ride along for
the canonical row and aliases but never merge anything.

Scope is the PE-relevant subset, not all 985k EDGAR filers: advisers, 13F
filers, Form D issuers, and the filers those reference (plus 8-K filers and
insider-reporting owners).

Identifier traps handled here, measured on the live data:
- `sec_13f_filings.crd_number` is zero-padded ('000106326'), ADV CRDs are not
  ('8361'). Both are stored canonically (digits, no leading zeros).
- All-zero / repeated-digit placeholders are dropped rather than stored.
- The ADV roster is a snapshot series, so only the latest row per CRD is fed.

Form 5500 sponsors (SPEC_147) are fed from `workbench.dol5500_sponsor`, the DOL
public bulk files the workbench's `dol_form_5500` connector already loaded into
this database (no new fetch). One record per EIN (`dol5500:<9-digit EIN>`), the
latest form_year winning. EIN is their only strong key; EDGAR submissions is the
only other NexData source carrying EINs, so the edgar feed scope also takes
every CIK whose sponsor EIN no other sec_filers CIK carries (SPEC_149). When the table is absent (a database the
workbench never loaded) the feed is skipped and reported, never an error.

Web domains (SPEC_148): every feed stores `domains.domain(website)` -- the
registrable domain, with social / builder / ATS / webmail / parking hosts
dropped -- and reports per feed how many raw values were kept, empty, invalid
or generic. Eight ATTACH-ONLY feeds read company tables that carry a website
but no trustworthy identifier (`ATTACH_FEEDS`): a row is fed only when its
domain survives, and the resolver uses its keys solely to attach the domain
claim, never to resolve (`resolve_core.ATTACH_ONLY_SOURCES`). The pe_firms rows
built from "SEC ADV" are left out: their website IS the ADV website, so they
would be a second copy of one source, not a second source.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.entities import domains, norm
from app.entities.resolve_core import _cik, _crd, _lei, _uei, ein_is_placeholder

logger = logging.getLogger(__name__)

STAGING = "core_source_record"
TARGET = "core.source_record"

COLUMNS: List[Tuple[str, str]] = [
    ("record_key", "TEXT"),
    ("source", "VARCHAR(32)"),
    ("native_id", "TEXT"),
    ("legal_name", "TEXT"),
    ("name_norm", "TEXT"),
    ("name_norm_version", "VARCHAR(8)"),
    ("ein", "TEXT"),
    ("cik", "TEXT"),
    ("crd", "TEXT"),
    ("lei", "TEXT"),
    ("uei", "TEXT"),
    ("state_entity_id", "TEXT"),
    ("state", "VARCHAR(2)"),
    ("zip5", "VARCHAR(5)"),
    ("domain", "TEXT"),
    ("observed_at", "TIMESTAMP"),
]
COLUMN_NAMES = [c for c, _ in COLUMNS]


@dataclass
class Feed:
    name: str
    sql: str
    key_prefix: str
    # a relation that must exist for this feed to run (None: always runs)
    requires: Optional[str] = None
    # native_id IS the EIN: normalized to 9 digits, rows without one dropped
    native_ein: bool = False
    # SPEC_148: keys only attach a domain claim; rows without a kept domain dropped
    attach_only: bool = False
    # SPEC_154: native_id IS this identifier ('lei' | 'uei'): canonicalized, rows without one dropped
    native_key: Optional[str] = None
    # SPEC_155: a column the `requires` relation must carry (a table predating its migration is skipped)
    requires_column: Optional[str] = None


# The CIK universe we care about: everything referenced by an adviser-side or
# PE-side source. `sec_filers` alone is 985k rows and mostly irrelevant here.
_RELEVANT_CIKS = """
    SELECT cik FROM form_d_issuers
    UNION SELECT cik FROM sec_13f_filings
    UNION SELECT cik FROM sec_8k_index
    UNION SELECT rptowner_cik AS cik FROM sec_insider_owners
    UNION SELECT issuer_cik AS cik FROM sec_insider_filings
"""

DOL5500_TABLE = "workbench.dol5500_sponsor"

# SPEC_147: EDGAR filers sharing an EIN with a Form 5500 sponsor. Measured
# 2026-09-30: 3,283 sponsor EINs appear in sec_filers, 2,166 of those CIKs were
# outside the scope above.
# SPEC_149: only an EIN carried by exactly ONE sec_filers CIK is expanded.
# MEASURED 2026-10-01: 231 sponsor EINs sit on 693 CIKs -- a parent's EIN on its
# subsidiary co-registrants (Century Communities: 134 LLCs), an issuer's EIN on
# its insiders' reporting CIKs, ESOPs, old '/ADV' and '/TA' duplicates. Pulling
# those in fused distinct filers (153 new multi-CIK entities) and nulled the CIK
# of 7 adviser entities. A shared EIN cannot say WHICH filer is the sponsor.
_SPONSOR_EINS = "SELECT DISTINCT ein FROM workbench.dol5500_sponsor WHERE ein IS NOT NULL"
_SPONSOR_EIN_UNIQUE = f"""
    SELECT f.ein FROM sec_filers f
    WHERE f.ein IN ({_SPONSOR_EINS})
    GROUP BY f.ein HAVING COUNT(*) = 1
"""
_SPONSOR_EIN_CIKS = f"""
    UNION SELECT sf.cik FROM sec_filers sf
    JOIN ({_SPONSOR_EIN_UNIQUE}) sp ON sp.ein = sf.ein
"""
# sponsor EINs on more than one sec_filers CIK: reported, never expanded
_SPONSOR_EIN_SHARED_COUNT = f"""
    SELECT COUNT(*) FROM (
        SELECT f.ein FROM sec_filers f
        WHERE f.ein IN ({_SPONSOR_EINS})
        GROUP BY f.ein HAVING COUNT(*) > 1
    ) shared
"""
# SPEC_149: edgar records the current scope no longer emits AND whose CIK is in no
# PE-relevant table -- only the (wider, pre-SPEC_149) sponsor expansion could have
# fed them. Other stale records are left alone: pruning them is a separate call.
_EDGAR_PRUNE = f"""
    DELETE FROM core.source_record sr
    WHERE sr.source = 'edgar'
      AND NOT EXISTS (SELECT 1 FROM stg.{{stg}} s WHERE s.record_key = sr.record_key)
      AND NOT EXISTS (SELECT 1 FROM ({_RELEVANT_CIKS}) rel WHERE rel.cik = sr.native_id)
"""

FEEDS: List[Feed] = [
    Feed(
        "adv",
        # latest snapshot per CRD: the roster is a monthly series
        """
        SELECT DISTINCT ON (crd_number)
               crd_number AS native_id,
               COALESCE(legal_name, business_name) AS legal_name,
               crd_number AS crd,
               NULL::TEXT AS cik,
               NULL::TEXT AS ein,
               main_office_state AS state,
               website AS domain,
               roster_date::TIMESTAMP AS observed_at
        FROM sec_adv_roster_snapshots
        WHERE crd_number IS NOT NULL
        ORDER BY crd_number, roster_date DESC
        """,
        "adv",
    ),
    Feed(
        "iapd",
        """
        SELECT DISTINCT ON (crd_number)
               crd_number AS native_id,
               COALESCE(legal_name, business_name) AS legal_name,
               crd_number AS crd,
               NULL::TEXT AS cik,
               NULL::TEXT AS ein,
               main_office_state AS state,
               website AS domain,
               edition_date::TIMESTAMP AS observed_at
        FROM sec_adv_feed_firm_state
        WHERE crd_number IS NOT NULL
        ORDER BY crd_number, edition_date DESC
        """,
        "iapd",
    ),
    Feed(
        "f13",
        # The important one: a 13F cover page carries CIK *and* CRD on the same
        # record, so the union-find merges adviser and filer without a bridge.
        """
        SELECT DISTINCT ON (cik)
               cik AS native_id,
               filing_manager_name AS legal_name,
               crd_number AS crd,
               cik,
               NULL::TEXT AS ein,
               filing_manager_state_or_country AS state,
               NULL::TEXT AS domain,
               filing_date::TIMESTAMP AS observed_at
        FROM sec_13f_filings
        WHERE cik IS NOT NULL
        ORDER BY cik, filing_date DESC
        """,
        "f13",
    ),
    Feed(
        "formd",
        """
        SELECT DISTINCT ON (cik)
               cik AS native_id,
               entity_name AS legal_name,
               NULL::TEXT AS crd,
               cik,
               NULL::TEXT AS ein,
               state_or_country AS state,
               NULL::TEXT AS domain,
               loaded_at AS observed_at
        FROM form_d_issuers
        WHERE cik IS NOT NULL
        ORDER BY cik, loaded_at DESC
        """,
        "formd",
    ),
]


def _edgar_feed(relevant_ciks: str) -> Feed:
    return Feed(
        "edgar",
        f"""
        SELECT f.cik AS native_id,
               f.name AS legal_name,
               NULL::TEXT AS crd,
               f.cik,
               f.ein,
               f.lei,
               COALESCE(f.state_of_incorporation, f.biz_state2, f.biz_state_or_country) AS state,
               f.website AS domain,
               f.loaded_at AS observed_at
        FROM sec_filers f
        JOIN ({relevant_ciks}) rel ON rel.cik = f.cik
        """,
        "edgar",
    )


EDGAR = _edgar_feed(_RELEVANT_CIKS)
EDGAR_WITH_SPONSORS = _edgar_feed(_RELEVANT_CIKS + _SPONSOR_EIN_CIKS)
FEEDS.append(EDGAR)

DOL5500 = Feed(
    "dol5500",
    # one record per sponsor EIN: the latest plan year names it
    """
    SELECT DISTINCT ON (ein)
           ein AS native_id,
           sponsor_name AS legal_name,
           NULL::TEXT AS crd,
           NULL::TEXT AS cik,
           ein,
           state,
           zip5,
           NULL::TEXT AS domain,
           loaded_at::TIMESTAMP AS observed_at
    FROM workbench.dol5500_sponsor
    WHERE ein IS NOT NULL
    ORDER BY ein, form_year DESC, loaded_at DESC
    """,
    "dol5500",
    requires=DOL5500_TABLE,
    native_ein=True,
)


GLEIF_TABLE = "public.gleif_lei_record"
USASP_TABLE = "public.usaspending_awards"

# SPEC_154: one record per LEI from the GLEIF API load (CC0). DUPLICATE / ANNULLED registrations
# are invalid LEIs (GLEIF: another LEI is the real one, or the LEI was issued in error): not fed.
# `state` is the US jurisdiction state, else the legal-address state (app/sources/gleif/client.py).
GLEIF = Feed(
    "gleif",
    """
    SELECT lei AS native_id,
           legal_name,
           NULL::TEXT AS crd,
           NULL::TEXT AS cik,
           NULL::TEXT AS ein,
           lei,
           state,
           NULL::TEXT AS domain,
           golden_copy_publish_date::TIMESTAMP AS observed_at
    FROM gleif_lei_record
    WHERE COALESCE(registration_status, '') NOT IN ('DUPLICATE', 'ANNULLED')
    """,
    "gleif",
    requires=GLEIF_TABLE,
    native_key="lei",
)

# SPEC_154: one record per distinct USAspending recipient UEI (the latest award names it). No
# state: the table holds the place of performance, not the recipient's address.
USASP = Feed(
    "usasp",
    """
    SELECT DISTINCT ON (upper(trim(recipient_uei)))
           upper(trim(recipient_uei)) AS native_id,
           recipient_name AS legal_name,
           NULL::TEXT AS crd,
           NULL::TEXT AS cik,
           NULL::TEXT AS ein,
           recipient_uei AS uei,
           NULL::TEXT AS state,
           NULL::TEXT AS domain,
           ingested_at::TIMESTAMP AS observed_at
    FROM usaspending_awards
    WHERE recipient_uei IS NOT NULL
    ORDER BY upper(trim(recipient_uei)), ingested_at DESC, award_id
    """,
    "usasp",
    requires=USASP_TABLE,
    native_key="uei",
)

_NATIVE_CANON = {"lei": _lei, "uei": _uei}


def _ein9(raw) -> Optional[str]:
    """9-digit EIN with its leading zeros, or None.

    An EIN that went through an integer column loses its leading zero
    ('012345678' -> 12345678), so an int, or a plain digit string of 7-8
    digits, is left-padded back to 9. Hyphens and spaces are stripped.
    Placeholders (all one digit, '000000000') and anything else that is not
    9 digits are refused, never guessed.
    """
    if raw is None or isinstance(raw, bool):
        return None
    s = str(raw).strip()
    if not re.fullmatch(r"[0-9][0-9\- ]*", s):
        return None
    digits = re.sub(r"\D", "", s)
    if s.isdigit() and 7 <= len(digits) < 9:   # an int, or its string, lost the zeros
        digits = digits.zfill(9)
    if len(digits) != 9 or ein_is_placeholder(digits):   # SPEC_149: 123456789, '00' prefix
        return None
    return digits


def _zip5(raw) -> Optional[str]:
    s = str(raw or "").strip()[:5]
    return s if len(s) == 5 and s.isdigit() else None


def _row(feed: Feed, raw: dict) -> Optional[tuple]:
    """Source row -> core.source_record tuple, or None when it carries no key."""
    cik = _cik(raw.get("cik"))
    crd = _crd(raw.get("crd"))
    ein = _ein9(raw.get("ein"))
    lei = _lei(raw.get("lei"))
    uei = _uei(raw.get("uei"))
    if feed.native_ein:
        native_id = _ein9(raw.get("native_id")) or ""
    elif feed.native_key:
        native_id = _NATIVE_CANON[feed.native_key](raw.get("native_id")) or ""
    else:
        native_id = str(raw.get("native_id") or "").strip()
    if not native_id:
        return None
    legal_name = (raw.get("legal_name") or "").strip() or None
    domain = domains.domain(raw.get("domain"))
    if feed.attach_only and not domain:
        return None
    return (
        f"{feed.key_prefix}:{native_id}",
        feed.name,
        native_id,
        legal_name,
        norm.norm(legal_name) if legal_name else None,
        norm.NAME_NORM_VERSION,
        ein,
        cik,
        crd,
        lei,   # SPEC_154: EDGAR sec_filers.lei, GLEIF
        uei,   # SPEC_154: USAspending recipient UEI
        None,  # state_entity_id
        norm.state2(raw.get("state")),
        _zip5(raw.get("zip5")),
        domain,
        raw.get("observed_at"),
    )


FETCH_BATCH = 20_000


def domain_stats(values) -> Dict[str, int]:
    """How a feed's raw website values fared: raw / kept / empty / invalid /
    generic (and generic:<category>). Counts only, never the values."""
    out: Dict[str, int] = {"raw": 0, "kept": 0, "empty": 0, "invalid": 0, "generic": 0}
    for v in values:
        out["raw"] += 1
        dom, reason = domains.classify(v)
        if dom:
            out["kept"] += 1
        elif reason.startswith("generic:"):
            out["generic"] += 1
            out[reason] = out.get(reason, 0) + 1
        else:
            out[reason] += 1
    return out


def _fetch_rows(conn, feed: Feed, raw_domains: Optional[list] = None) -> List[tuple]:
    """Read the feed fully before COPY starts.

    psycopg2 allows only one operation at a time per connection: streaming a
    SELECT straight into COPY FROM STDIN on the same connection aborts the
    transaction ("no COPY in progress"). Batched fetch keeps memory bounded.
    """
    rows: List[tuple] = []
    result = conn.execute(text(feed.sql)).mappings()
    while True:
        batch = result.fetchmany(FETCH_BATCH)
        if not batch:
            break
        for raw in batch:
            if raw_domains is not None:
                raw_domains.append(raw.get("domain"))
            row = _row(feed, dict(raw))
            if row is not None:
                rows.append(row)
    return rows


def _relation_exists(conn, name: str) -> bool:
    return bool(conn.execute(text("SELECT to_regclass(:n) IS NOT NULL"), {"n": name}).scalar())


def _column_exists(conn, relation: str, column: str) -> bool:
    schema, _, table = relation.rpartition(".")
    return bool(conn.execute(text(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = :s AND table_name = :t AND column_name = :c"),
        {"s": schema or "public", "t": table, "c": column}).scalar())


def feeds_for(conn) -> Tuple[List[Feed], List[str]]:
    """(feeds to run, names skipped) for this database.

    A feed whose `requires` relation is absent is skipped. The edgar scope
    expansion needs the Form 5500 table too; without it the SEC feeds run
    exactly as before."""
    run, skipped = [], []
    for feed in FEEDS + [DOL5500, GLEIF, USASP] + ATTACH_FEEDS:
        if feed.requires and not _relation_exists(conn, feed.requires):
            logger.warning(f"[entities:feeds] {feed.requires} not found: {feed.name} feed skipped")
            skipped.append(feed.name)
        elif feed.requires_column and not _column_exists(conn, feed.requires, feed.requires_column):
            logger.warning(f"[entities:feeds] {feed.requires}.{feed.requires_column} not found: {feed.name} feed skipped")
            skipped.append(feed.name)
        else:
            run.append(feed)
    if DOL5500.name not in skipped:
        run = [EDGAR_WITH_SPONSORS if f is EDGAR else f for f in run]
    return run, skipped


def run_feeds(conn, feeds: Optional[Sequence[Feed]] = None,
              progress: Optional[Callable[[str, float], None]] = None) -> Dict[str, Any]:
    """Refresh core.source_record. Returns rows per feed (0 for a skipped feed)
    and `skipped`, the feeds whose source table is absent."""
    skipped: List[str] = []
    if feeds is None:
        feeds, skipped = feeds_for(conn)
    feeds = list(feeds)
    counts: Dict[str, Any] = {name: 0 for name in skipped}
    counts["skipped"] = skipped
    counts["domains"] = {}
    for i, feed in enumerate(feeds):
        if progress:
            progress(f"feed {feed.name}", 100.0 * i / max(1, len(feeds)))
        raw_domains: list = []
        rows = _fetch_rows(conn, feed, raw_domains)
        stats = counts["domains"][feed.name] = domain_stats(raw_domains)
        # one-level scalar maps survive the build ledger's compact_summary
        counts.setdefault("domain_kept", {})[feed.name] = stats["kept"]
        counts.setdefault("domain_generic", {})[feed.name] = stats["generic"]
        create_staging(conn, STAGING, COLUMNS)
        copied = copy_rows(conn, STAGING, COLUMN_NAMES, rows)
        inserted, updated = merge_staging(conn, STAGING, TARGET, COLUMN_NAMES, ["record_key"])
        if feed is EDGAR_WITH_SPONSORS:
            counts["sponsor_ein_shared"] = int(conn.execute(text(_SPONSOR_EIN_SHARED_COUNT)).scalar() or 0)
            counts["edgar_pruned"] = conn.execute(text(_EDGAR_PRUNE.format(stg=STAGING))).rowcount or 0
        drop_staging(conn, STAGING)
        counts[feed.name] = copied
        logger.info(
            f"[entities:feeds] {feed.name}: staged {copied}, inserted {inserted}, updated {updated}"
        )
    return counts


# ---------------------------------------------------------------------------
# SPEC_148: attach-only company tables (a website, no trustworthy identifier)
# ---------------------------------------------------------------------------

def _attach_feed(name: str, table: str, sql: str, requires_column: Optional[str] = None) -> Feed:
    return Feed(name, sql, name, requires=table, attach_only=True, requires_column=requires_column)


ATTACH_FEEDS: List[Feed] = [
    _attach_feed("pefirm", "public.pe_firms", """
        SELECT id::TEXT AS native_id, COALESCE(legal_name, name) AS legal_name,
               crd_number AS crd, cik, NULL::TEXT AS ein, headquarters_state AS state,
               website AS domain, updated_at::TIMESTAMP AS observed_at
        FROM pe_firms
        WHERE website IS NOT NULL
          AND (data_sources IS NULL OR CAST(data_sources AS TEXT) NOT LIKE '%SEC ADV%')
    """),
    _attach_feed("industrial", "public.industrial_companies", """
        SELECT id::TEXT AS native_id, COALESCE(legal_name, name) AS legal_name,
               NULL::TEXT AS crd, cik, NULL::TEXT AS ein, headquarters_state AS state,
               website AS domain, updated_at::TIMESTAMP AS observed_at
        FROM industrial_companies WHERE website IS NOT NULL
    """),
    _attach_feed("peportco", "public.pe_portfolio_companies", """
        SELECT id::TEXT AS native_id, COALESCE(legal_name, name) AS legal_name,
               NULL::TEXT AS crd, NULL::TEXT AS cik, ein, headquarters_state AS state,
               website AS domain, updated_at::TIMESTAMP AS observed_at
        FROM pe_portfolio_companies WHERE website IS NOT NULL
    """),
    _attach_feed("portco", "public.portfolio_companies", """
        SELECT id::TEXT AS native_id, company_name AS legal_name,
               NULL::TEXT AS crd, NULL::TEXT AS cik, NULL::TEXT AS ein, NULL::TEXT AS state,
               company_website AS domain, updated_at::TIMESTAMP AS observed_at
        FROM portfolio_companies WHERE company_website IS NOT NULL
    """),
    _attach_feed("threepl", "public.three_pl_company", """
        SELECT id::TEXT AS native_id, company_name AS legal_name,
               NULL::TEXT AS crd, NULL::TEXT AS cik, NULL::TEXT AS ein, headquarters_state AS state,
               website AS domain, collected_at::TIMESTAMP AS observed_at
        FROM three_pl_company WHERE website IS NOT NULL
    """),
    _attach_feed("famoffice", "public.family_offices", """
        SELECT id::TEXT AS native_id, COALESCE(legal_name, name) AS legal_name,
               sec_crd_number AS crd, NULL::TEXT AS cik, NULL::TEXT AS ein, state_province AS state,
               website AS domain, updated_at::TIMESTAMP AS observed_at
        FROM family_offices WHERE website IS NOT NULL
    """),
    # SPEC_155: the careers domain a VERIFIED job board shows (posting URLs on the firm's own site,
    # or a domain in the postings that matches the firm's name). Board URLs and posting text are
    # written by one company in one system: ONE family (ats_board), so alone it is a weak link.
    # SPEC_156: the linked target's EIN rides along (EIN-only firms attach by EIN, not by name).
    _attach_feed("atsboard", "public.ats_board", """
        SELECT id::TEXT AS native_id, company_name AS legal_name,
               NULL::TEXT AS crd, cik, ein AS ein, NULL::TEXT AS state,
               careers_domain AS domain, COALESCE(verified_at, updated_at)::TIMESTAMP AS observed_at
        FROM ats_board
        WHERE status = 'active' AND verification = 'verified' AND careers_domain IS NOT NULL
    """, requires_column="careers_domain"),
    _attach_feed("lpfund", "public.lp_fund", """
        SELECT id::TEXT AS native_id, COALESCE(formal_name, name) AS legal_name,
               sec_crd_number AS crd, NULL::TEXT AS cik, NULL::TEXT AS ein, NULL::TEXT AS state,
               website_url AS domain, updated_at::TIMESTAMP AS observed_at
        FROM lp_fund WHERE website_url IS NOT NULL
    """),
]
