"""
``sec_adv_roster``: the SEC's monthly RIA and ERA adviser rosters (SPEC_112).

Source page: "Information About Registered Investment Advisers and Exempt
Reporting Advisers" on www.sec.gov. There is one zip per month per
population. Ported from wildcard-workbench ``adv_monthly_roster.py`` without
the universe filter or the differ, and with the ``-exempt`` ERA files
included.

- Filenames are scraped from the page, never templated. They drift a lot:
  ``ia09012026-registered.zip``, ``ia060126_0.zip``,
  ``ia020226-exemptzip.zip``, ``ia122025.zip``.
- Only December 2025 and later zips hold CSV. Jun 2023 - Sep 2025 are bare
  .xlsx, Oct and Nov 2025 are "no data" PDFs, and 2006 - May 2023 zips wrap
  an .xlsx. All of those are skipped in discover.
- The default window is the last 36 calendar months per type.
- Every release loads into ``public.sec_adv_roster_snapshots``.
- The newest roster of each type is also upserted into ``public.sec_form_adv``.
  The update is NULL-preserving, ``col = COALESCE(EXCLUDED.col, existing)``,
  so values enriched by other writers survive.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.core.safe_sql import qi
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_form_adv.parse import (
    LISTING_URL,
    ROSTER_COLUMNS,
    classify_listing,
    iter_roster_rows,
)

logger = logging.getLogger(__name__)

TARGET = "public.sec_adv_roster_snapshots"
KEY_COLUMNS = ["crd_number", "roster_date", "adviser_type"]
STAGING = "sec_adv_roster"
DEFAULT_WINDOW = 3           # calendar months per adviser type when since is None (Lean budget)
CSV_ERA_START = date(2025, 12, 1)


def _col_defs() -> str:
    return ",\n    ".join(f"{qi(c)} {t}" for c, t in ROSTER_COLUMNS)


SNAPSHOT_DDL = [
    f"""CREATE TABLE IF NOT EXISTS public.sec_adv_roster_snapshots (
    {_col_defs()},
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (crd_number, roster_date, adviser_type)
)""",
    "CREATE INDEX IF NOT EXISTS idx_sec_adv_roster_crd ON public.sec_adv_roster_snapshots (crd_number)",
    "CREATE INDEX IF NOT EXISTS idx_sec_adv_roster_date ON public.sec_adv_roster_snapshots (roster_date, adviser_type)",
]

# Same shape as app/sources/sec/formadv_metadata.generate_create_table_sql().
# CREATE IF NOT EXISTS only, so an existing table is never altered.
SEC_FORM_ADV_DDL = [
    """CREATE TABLE IF NOT EXISTS sec_form_adv (
    id SERIAL PRIMARY KEY,
    crd_number TEXT NOT NULL UNIQUE,
    sec_number TEXT,
    firm_name TEXT NOT NULL,
    business_address_street1 TEXT,
    business_address_street2 TEXT,
    business_address_city TEXT,
    business_address_state TEXT,
    business_address_zip TEXT,
    business_address_country TEXT,
    business_phone TEXT,
    business_fax TEXT,
    business_email TEXT,
    website TEXT,
    mailing_address_street1 TEXT,
    mailing_address_street2 TEXT,
    mailing_address_city TEXT,
    mailing_address_state TEXT,
    mailing_address_zip TEXT,
    mailing_address_country TEXT,
    legal_name TEXT,
    doing_business_as TEXT,
    registration_status TEXT,
    registration_date DATE,
    state_registrations TEXT[],
    assets_under_management NUMERIC,
    aum_date DATE,
    aum_currency TEXT DEFAULT 'USD',
    total_client_count INT,
    individual_client_count INT,
    high_net_worth_client_count INT,
    pooled_investment_vehicle_count INT,
    is_family_office BOOLEAN DEFAULT FALSE,
    is_registered_with_sec BOOLEAN DEFAULT TRUE,
    is_registered_with_state BOOLEAN DEFAULT FALSE,
    key_personnel JSONB,
    form_adv_url TEXT,
    filing_date DATE,
    last_amended_date DATE,
    ingested_at TIMESTAMP DEFAULT NOW(),
    last_updated_at TIMESTAMP DEFAULT NOW()
)""",
    "CREATE INDEX IF NOT EXISTS idx_formadv_crd ON sec_form_adv(crd_number)",
    "CREATE INDEX IF NOT EXISTS idx_formadv_sec_number ON sec_form_adv(sec_number)",
    "CREATE INDEX IF NOT EXISTS idx_formadv_firm_name ON sec_form_adv(firm_name)",
    "CREATE INDEX IF NOT EXISTS idx_formadv_state ON sec_form_adv(business_address_state)",
    "CREATE INDEX IF NOT EXISTS idx_formadv_family_office ON sec_form_adv(is_family_office)",
]

# sec_form_adv column -> SQL expression over snapshot alias ``s``. All expressions
# are fixed code (no external input). The int casts guard INT overflow on
# sec_form_adv's INT client columns.
_INT_MAX = 2147483647


def _int_expr(col: str) -> str:
    return f"CASE WHEN s.{qi(col)} BETWEEN 0 AND {_INT_MAX} THEN CAST(s.{qi(col)} AS INT) END"


def _raw(key: str) -> str:
    # Keys are fixed header names from this module; single quotes are not present.
    assert "'" not in key
    return f"NULLIF(BTRIM(s.raw ->> '{key}'), '')"


FORM_ADV_MAP: List[tuple] = [
    ("crd_number", "s.crd_number"),
    ("sec_number", "s.sec_number"),
    ("firm_name", "COALESCE(s.business_name, s.legal_name)"),
    ("business_address_street1", "s.main_office_street1"),
    ("business_address_street2", "s.main_office_street2"),
    ("business_address_city", "s.main_office_city"),
    ("business_address_state", "s.main_office_state"),
    ("business_address_zip", "s.main_office_postal_code"),
    ("business_address_country", "s.main_office_country"),
    ("business_phone", "s.main_office_phone"),
    ("business_fax", "s.main_office_fax"),
    ("website", "s.website"),
    ("mailing_address_street1", _raw("Mail Office Street Address 1")),
    ("mailing_address_street2", _raw("Mail Office Street Address 2")),
    ("mailing_address_city", _raw("Mail Office City")),
    ("mailing_address_state", _raw("Mail Office State")),
    ("mailing_address_zip", _raw("Mail Office Postal Code")),
    ("mailing_address_country", _raw("Mail Office Country")),
    ("legal_name", "s.legal_name"),
    ("doing_business_as", "s.business_name"),
    ("registration_status", "s.sec_status"),
    ("registration_date", "s.sec_status_effective_date"),
    ("state_registrations", "s.notice_filed_states"),
    ("assets_under_management", "s.aum_total"),
    ("total_client_count", _int_expr("clients_count")),
    ("individual_client_count", _int_expr("clients_individuals")),
    ("high_net_worth_client_count", _int_expr("clients_hnw")),
    ("pooled_investment_vehicle_count", _int_expr("clients_pooled_vehicles")),
    ("is_registered_with_sec", "(s.adviser_type = 'ria')"),
    ("filing_date", "s.latest_filing_date"),
]


def build_form_adv_upsert_sql() -> str:
    """INSERT … SELECT … ON CONFLICT (crd_number) with NULL-preserving COALESCE updates."""
    cols = [c for c, _ in FORM_ADV_MAP]
    col_sql = ", ".join(qi(c) for c in cols)
    select_sql = ",\n               ".join(f"{expr} AS {qi(c)}" for c, expr in FORM_ADV_MAP)
    sets = ",\n            ".join(
        f"{qi(c)} = COALESCE(EXCLUDED.{qi(c)}, sec_form_adv.{qi(c)})" for c in cols if c != "crd_number"
    )
    return f"""
        WITH src AS (
            SELECT DISTINCT ON (s.crd_number)
               {select_sql}
            FROM public.sec_adv_roster_snapshots s
            WHERE s.roster_date = :roster_date
              AND s.adviser_type = :adviser_type
              AND COALESCE(s.business_name, s.legal_name) IS NOT NULL
            ORDER BY s.crd_number
        ),
        upserted AS (
            INSERT INTO public.sec_form_adv ({col_sql}, "last_updated_at")
            SELECT {col_sql}, NOW() FROM src
            ON CONFLICT ("crd_number") DO UPDATE SET
            {sets},
            "last_updated_at" = NOW()
            RETURNING (xmax = 0) AS inserted
        )
        SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted) FROM upserted
    """


@register_bulk_source
class SecAdvRoster(BulkSource):
    name = "sec_adv_roster"
    parser_version = "1"
    window_months = DEFAULT_WINDOW
    # Zips before this date wrap an .xlsx (checked: ia040423.zip -> ia040423.xlsx),
    # and Jun 2023 - Sep 2025 is published as bare .xlsx. Only Dec 2025+ zips hold CSV.
    csv_era_start: Optional[date] = CSV_ERA_START

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        page = http.get_text(LISTING_URL)
        files, skipped = classify_listing(page)
        if not files:
            raise RuntimeError(f"no roster zips found on {LISTING_URL}; listing layout or naming drifted "
                               f"(skipped: {dict(skipped)})")
        if skipped:
            logger.info(f"[bulk:{self.name}] skipped non-zip roster files (xlsx era / no-data PDFs): {dict(skipped)}")

        releases: List[Release] = []
        for adviser_type in ("ria", "era"):
            typed = [f for f in files if f.adviser_type == adviser_type]
            if not typed:
                logger.warning(f"[bulk:{self.name}] no {adviser_type} zips on listing page")
                continue
            newest = max(f.roster_date for f in typed)
            pre_csv = [f for f in typed if self.csv_era_start and f.roster_date < self.csv_era_start]
            if pre_csv:
                logger.info(f"[bulk:{self.name}] skipping {len(pre_csv)} pre-{self.csv_era_start} {adviser_type} "
                            f"zips (they wrap xlsx, not CSV)")
            typed = [f for f in typed if f not in pre_csv]
            if since is None:
                newest_idx = newest.year * 12 + newest.month - 1
                typed = [f for f in typed
                         if f.roster_date.year * 12 + f.roster_date.month - 1 > newest_idx - self.window_months]
            else:
                typed = [f for f in typed if f.roster_date >= since]
            for f in typed:
                releases.append(Release(
                    release_key=f"{adviser_type}:{f.roster_date.isoformat()}",
                    url=f.url,
                    meta={
                        "adviser_type": adviser_type,
                        "roster_date": f.roster_date.isoformat(),
                        "filename": f.filename,
                        "suffix": f.suffix,
                        "newest_roster_date": newest.isoformat(),
                    },
                ))
        releases.sort(key=lambda r: (r.meta["roster_date"], r.meta["adviser_type"]))
        return releases

    def ddl(self) -> List[str]:
        return SNAPSHOT_DDL + SEC_FORM_ADV_DDL

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        adviser_type, _, date_s = release.release_key.partition(":")
        adviser_type = release.meta.get("adviser_type", adviser_type)
        roster_date = date.fromisoformat(release.meta.get("roster_date", date_s))

        names = [c for c, _ in ROSTER_COLUMNS]
        create_staging(conn, STAGING, ROSTER_COLUMNS)
        copied = copy_rows(conn, STAGING, names,
                           iter_roster_rows(path, adviser_type, roster_date, release.release_key))
        if copied == 0:
            raise RuntimeError(f"{release.release_key}: roster parsed 0 rows from {Path(path).name}")
        inserted, updated = merge_staging(conn, STAGING, TARGET, names, KEY_COLUMNS)
        drop_staging(conn, STAGING)
        out = {"sec_adv_roster_snapshots": inserted + updated}

        if self._is_newest(conn, release, adviser_type, roster_date):
            ins, upd = conn.execute(
                text(build_form_adv_upsert_sql()),
                {"roster_date": roster_date, "adviser_type": adviser_type},
            ).one()
            out["sec_form_adv"] = int(ins or 0) + int(upd or 0)
        logger.info(f"[bulk:{self.name}] {release.release_key}: copied {copied}, {out}")
        return out

    @staticmethod
    def _is_newest(conn, release: Release, adviser_type: str, roster_date: date) -> bool:
        db_max = conn.execute(
            text("SELECT MAX(roster_date) FROM public.sec_adv_roster_snapshots WHERE adviser_type = :t"),
            {"t": adviser_type},
        ).scalar()
        if db_max is not None and db_max > roster_date:
            return False
        listed = release.meta.get("newest_roster_date")
        if listed and date.fromisoformat(listed) > roster_date:
            return False
        return True
